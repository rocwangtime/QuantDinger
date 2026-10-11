"""Real Gamma shapes, fail-closed metadata, bounded rollover and research prices."""
import copy
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import requests
from marshmallow import ValidationError

from app.openapi.schemas.polymarket import ObserverQuery
from app.services.polymarket import observer, updown
from app.services.polymarket.engine import fee, market_from_gamma

FIXTURES = json.loads((Path(__file__).parent / 'fixtures/polymarket/updown_gamma.json').read_text())
BTC = next(row for row in FIXTURES if row.get('cryptoMarketConfig', {}).get('id') == 'btc-15m-twap-60')
NOW = updown.timestamp(BTC['eventStartTime']) + timedelta(minutes=1)


@pytest.mark.parametrize('row', [r for r in FIXTURES if (r.get('cryptoMarketConfig') or {}).get('asset', '').upper() in updown.ASSETS or r.get('events')])
def test_recorded_gamma_acceptance(row):
    config = row.get('cryptoMarketConfig') or {}
    series = row.get('events', [{}])[0].get('seriesSlug', '')
    asset = config.get('asset', series.split('-')[0]).upper()
    interval = config.get('duration') or ('1h' if series.endswith('hourly') else '1d')
    market = updown.market_from_updown(row, asset=asset, interval=interval, now=updown.timestamp(row['eventStartTime']))
    assert market['market_type'] == 'up_down'
    assert market['feeRate'] == '0.07'
    assert market['feeExponent'] == 1
    assert market['asset'] == asset
    assert market['price_to_beat'] is None
    with pytest.raises(ValueError, match='standardBinaryRequired'):
        market_from_gamma(row)


@pytest.mark.parametrize('changes,reason', [
    ({'outcomes': '["Up","Down","Other"]'}, 'upDownBinaryRequired'),
    ({'outcomes': '["Yes","No"]'}, 'upDownBinaryRequired'),
    ({'negRisk': True}, 'negativeRiskExcluded'),
    ({'clobTokenIds': '["1"]'}, 'outcomeIdsMissing'),
    ({'clobTokenIds': '["1","1"]'}, 'outcomeIdsMissing'),
    ({'enableOrderBook': False}, 'ordersUnavailable'),
    ({'acceptingOrders': False}, 'ordersUnavailable'),
    ({'closed': True}, 'marketClosed'),
    ({'feesEnabled': False}, 'feeUnknown'),
    ({'feeSchedule': None}, 'feeUnknown'),
    ({'feeSchedule': {'rate': 0.07, 'exponent': 2, 'takerOnly': True}}, 'feeUnknown'),
    ({'feeSchedule': {'rate': 0.25, 'exponent': 1, 'takerOnly': True}}, 'feeUnknown'),
    ({'eventStartTime': None}, 'windowUnknown'),
    ({'cryptoMarketConfig': {'asset': 'eth', 'duration': '15m'}}, 'windowMismatch'),
])
def test_updown_rejection(changes, reason):
    with pytest.raises(ValueError, match=reason):
        updown.market_from_updown({**BTC, **changes}, asset='BTC', interval='15m', now=NOW)


def test_reverse_outcomes_preserve_token_identity():
    row = {**BTC, 'outcomes': '["Down","Up"]'}
    market = updown.market_from_updown(row, asset='BTC', interval='15m', now=NOW)
    assert market['yesAssetId'] == json.loads(BTC['clobTokenIds'])[1]


@pytest.mark.parametrize('price,expected', [('0.01', '0.06930'), ('0.10', '0.63000'), ('0.30', '1.47000'), ('0.50', '1.75000'), ('0.70', '1.47000'), ('0.99', '0.06930')])
def test_official_crypto_fee_curve(price, expected):
    assert fee(Decimal(100), Decimal(price), Decimal('0.07')) == Decimal(expected)


@pytest.mark.parametrize('seconds,bucket', [(601, '>10m'), (600, '>10m'), (599, '5-10m'), (300, '5-10m'), (299, '2-5m'), (120, '2-5m'), (119, '1-2m'), (60, '1-2m'), (59, '30-60s'), (30, '30-60s'), (29, '<30s')])
def test_expiry_buckets_and_bounded_near_keys(seconds, bucket):
    market = {'market_type': 'up_down', 'asset': 'BTC', 'interval': '15m', 'endDate': (NOW + timedelta(seconds=seconds)).isoformat()}
    tags = observer.sample_tags(market, NOW)
    assert tags == ('up_down', 'BTC', '15m', bucket)
    assert observer.near_miss_key(tags) == f'up_down|BTC|15m|{bucket}'
    assert observer.sample_tags({}, NOW) == ('yes_no', None, None, '')
    assert observer.near_miss_key(observer.sample_tags({}, NOW)) == 'yes_no|||'


def test_rollover_drops_expired_adds_new_tokens_and_caps(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ASSETS', 'BTC')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_INTERVALS', '15m')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_PER_PAIR_LIMIT', '1')
    old = copy.deepcopy(BTC)
    new = {**old, 'id': 'new', 'conditionId': '0x' + 'a' * 64, 'clobTokenIds': '["123","456"]', 'eventStartTime': old['endDate'], 'endDate': (updown.timestamp(old['endDate']) + timedelta(minutes=15)).isoformat()}

    class Client:
        def _json(self, method, url, **kwargs):
            assert method == 'GET'
            if url.endswith('/series'):
                return [{'id': '10192', 'slug': 'btc-up-or-down-15m'}]
            return [{'seriesSlug': 'btc-up-or-down-15m', 'markets': [old, new]}]

    first, _ = updown.discover(Client(), {}, now=NOW)
    second, _ = updown.discover(Client(), {}, now=updown.timestamp(old['endDate']))
    assert [m['id'] for m in first] == [old['id']]
    assert [m['id'] for m in second] == ['new']
    assert observer.subscription_assets(first) != observer.subscription_assets(second)
    assert observer.subscription_assets(second) == ['123', '456']


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', raising=False)
    monkeypatch.delenv('POLYMARKET_OBSERVER_ENABLED', raising=False)
    assert not updown.enabled()
    assert not observer.enabled()


def test_reference_cache_failure_fallback_and_expiry(monkeypatch):
    now = [100.0]
    calls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {'symbol': 'BTCUSDT', 'price': '60000.5'}

    class Session:
        fail = False

        def get(self, url, **kwargs):
            calls.append(url)
            if self.fail or 'api.binance.com' in url:
                raise requests.HTTPError('451')
            return Response()

    session = Session()
    cache = updown.ReferencePrices(session, clock=lambda: now[0])
    assert cache.fetch('BTC')['source'] == 'data-api.binance.vision'
    assert cache.fetch('BTC')['price'] == '60000.5'
    assert len(calls) == 2
    now[0] += 6
    session.fail = True
    assert cache.fetch('BTC')['price'] is None
    assert cache.fetch('BTC')['source'] == 'unavailable'
    assert len(calls) == 4
    now[0] += 100
    assert cache.get('BTC')['price'] is None


@pytest.mark.parametrize('field,value', [('market_type', 'live'), ('asset', 'DOGE'), ('interval', '2m'), ('expiry_bucket', 'invalid')])
def test_api_filters_validated(field, value):
    with pytest.raises(ValidationError):
        ObserverQuery().load({field: value})


def test_api_filters_parameterized():
    from app.routes.polymarket import observer_filters
    where, params = observer_filters({'market_type': 'up_down', 'asset': 'BTC', 'interval': '15m', 'expiry_bucket': '<30s'})
    assert where == ' AND market_type=%s AND asset=%s AND interval=%s AND expiry_bucket=%s'
    assert params == ('up_down', 'BTC', '15m', '<30s')


def test_loop_reconnects_with_new_subscription_at_rediscovery(monkeypatch):
    """Exercise the loop, public subscription message and close-before-refresh path."""
    import sys
    from types import SimpleNamespace
    mono = [100.0]
    sockets, subscriptions = [], []
    base = {'id': 'old', 'asset': 'BTC', 'interval': '15m', 'endDate': (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(), 'yesAssetId': '1', 'noAssetId': '2'}
    discovered = [[base], [{**base, 'id': 'new', 'yesAssetId': '3', 'noAssetId': '4'}]]
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'true')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_REFRESH_SECONDS', '10')

    class Stop:
        stopped = False

        def is_set(self):
            return self.stopped

        def wait(self, seconds):
            mono[0] += seconds
            if len(subscriptions) >= 2:
                self.stopped = True
            return self.stopped

    class Socket:
        def __init__(self, url, on_open, **kwargs):
            assert url.endswith('/ws/market')
            self.opened = on_open
            self.closed = False
            sockets.append(self)

        def run_forever(self, **kwargs):
            self.opened(self)

        def send(self, raw):
            if raw != 'PING':
                subscriptions.append(json.loads(raw))

        def close(self):
            self.closed = True

    class Thread:
        def __init__(self, target, **kwargs):
            self.target = target
            self.socket = sockets[-1]

        def start(self):
            self.target()

        def is_alive(self):
            return not self.socket.closed

        def join(self, timeout):
            assert self.socket.closed

    class Client:
        def discover(self, **kwargs):
            return [], []

        def close(self):
            pass

    class Reference:
        def start(self):
            pass

        def close(self):
            pass

    def discovery(*args):
        if len(subscriptions) == 1:
            assert sockets[0].closed
        return discovered.pop(0), []

    monkeypatch.setattr(observer, '_STOP', Stop())
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: mono[0]))
    monkeypatch.setattr(observer, 'threading', SimpleNamespace(Thread=Thread))
    monkeypatch.setattr(observer, 'PublicClient', Client)
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS))
    monkeypatch.setattr(observer, '_run_retention', lambda *_: None)
    monkeypatch.setattr(observer, '_close_stale_episodes', lambda *_: None)
    monkeypatch.setattr(observer, '_persist_status', lambda: None)
    monkeypatch.setattr(updown, 'ReferencePrices', Reference)
    monkeypatch.setattr(updown, 'discover', discovery)
    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    observer._market_loop()
    assert [s['assets_ids'] for s in subscriptions] == [['1', '2'], ['3', '4']]
    assert all(s['type'] == 'market' for s in subscriptions)
    assert all(sock.closed for sock in sockets)


@pytest.mark.parametrize('changes', [{'feeRate': 0.25}, {'feeExponent': 2}, {'feeSchedule': {'rate': 0.07, 'exponent': True, 'takerOnly': True}}])
def test_inconsistent_fee_metadata_rejected(changes):
    with pytest.raises(ValueError, match='feeUnknown'):
        updown.market_from_updown({**BTC, **changes}, asset='BTC', interval='15m', now=NOW)


def test_updown_crossed_and_expired_books_rejected():
    end = datetime.now(timezone.utc) + timedelta(minutes=15)
    market = {'market_type': 'up_down', 'yesAssetId': 'u', 'noAssetId': 'd', 'feeRate': '0.07', 'endDate': end.isoformat()}
    books = {'u': {'asks': {'0.4': '10'}, 'bids': {'0.5': '10'}}, 'd': {'asks': {'0.5': '10'}, 'bids': {}}}
    assert observer.opportunity(market, books, 5) is None
    books['u']['bids'] = {'0.39': '10'}
    assert observer.opportunity(market, books, 5) is not None
    market['endDate'] = (end - timedelta(hours=1)).isoformat()
    assert observer.opportunity(market, books, 5) is None

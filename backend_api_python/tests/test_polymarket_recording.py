"""Recording parsers, independent throttles, bounded receive staging and finality."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from app.services.polymarket import observer, recording

NOW = datetime(2026, 10, 14, 0, 0, tzinfo=timezone.utc)


def market(**extra):
    return dict(id='123', conditionId='condition', yesAssetId='u', noAssetId='d',
                slug='test', question='test', feeRate='0', market_type='up_down', asset='BTC',
                interval='5m', eventStartTime=NOW.isoformat(), endDate=(NOW + timedelta(minutes=5)).isoformat(), **extra)


def book(token='u', size='10', timestamp=None):
    return dict(event_type='book', asset_id=token, bids=[dict(price='0.4', size=size)],
                asks=[dict(price='0.5', size='10')], timestamp=timestamp)


def twap(at=NOW, **extra):
    return dict(channel='price.crypto.twap', payload=dict(symbol='btcusd', window_seconds=60,
                source='chainlink', full_accuracy_value='100.12345678', timestamp=int(at.timestamp() * 1000), **extra))


@pytest.mark.parametrize('value', [None, '', 'nope', '2026-10-14T00:00:00', float('nan'), float('inf')])
def test_exchange_time_missing_or_bad_is_unknown(value):
    assert recording.exchange_time(value) is None


def test_timestamp_parser_and_top_five_sort_size_change():
    assert recording.exchange_time('1791936000000') == NOW
    assert recording.exchange_time(NOW.isoformat()) == NOW
    raw = {side: {str(i / 10): str(i) for i in range(1, 9)} for side in ('bids', 'asks')}
    bids, asks = recording.top_five(raw)
    assert [r['price'] for r in bids] == ['0.8', '0.7', '0.6', '0.5', '0.4']
    assert [r['price'] for r in asks] == ['0.1', '0.2', '0.3', '0.4', '0.5']
    raw['bids']['0.8'] = '99'
    assert recording.top_five(raw)[0][0]['size'] == '99'
    raw['asks']['bad'] = '1'
    assert recording.top_five(raw) is None


def test_trade_parser_raw_and_enveloped_missing_size_timestamp():
    payload = dict(asset_id='u', price='.42', size='3.5', side='SELL', timestamp=str(int(NOW.timestamp() * 1000)))
    assert recording.parse_trade(payload, NOW)[:6] == ('u', '0.42', '3.5', 'SELL', NOW, NOW)
    missing = dict(assetId='u', price='.42', side='BUY')
    assert recording.parse_trade(missing, NOW)[2:5] == (None, 'BUY', None)
    p = observer.Pipeline()
    p.set_markets([market()])
    p.receive(json.dumps(dict(type='last_trade_price', payload=missing)))
    assert p.recording.pending[-1][2][0][0] == recording.TRADE_SQL


@pytest.mark.parametrize('change', [dict(price='NaN'), dict(price='1.2'), dict(size='-1'), dict(side='bad')])
def test_invalid_trade_is_rejected(change):
    assert recording.parse_trade(dict(asset_id='u', price='.5', size='1', side='BUY', **{}) | change, NOW) is None


def test_twap_precision_snapshot_and_provenance():
    assert recording.parse_twap(twap(), NOW) == [('BTC', 60, NOW, NOW, '100.12345678', 'chainlink')]
    snapshot = dict(channel='price.crypto.twap', snapshot=True, payload=dict(symbol='ethusd', source='chainlink', window_seconds=60,
        data=[dict(timestamp=int(NOW.timestamp() * 1000), value=2000)]))
    assert recording.parse_twap(snapshot, NOW)[0][0] == 'ETH'
    for key, value in [('source', 'pyth'), ('window_seconds', 30), ('symbol', 'solusd'), ('full_accuracy_value', 'NaN')]:
        event = twap()
        event['payload'][key] = value
        assert recording.parse_twap(event, NOW) == []


def test_twap_downsampling_preserves_exact_boundaries_and_cache_order():
    r = recording.Recorder()
    r.twap(twap(NOW + timedelta(seconds=1)), NOW, 10)
    r.twap(twap(NOW + timedelta(seconds=2)), NOW, 10.1)
    assert len(r.pending) == 1
    # A backfilled exact boundary survives the downsampling throttle.
    r.twap(twap(NOW), NOW, 10.2)
    assert len(r.pending) == 2
    assert r.twap_cache['BTC'][2] == NOW + timedelta(seconds=2)


def test_receive_stays_nonblocking_with_full_writer_and_bounds_trade_buffer(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_BUFFER_MAX', '2')
    p = observer.Pipeline()
    p.set_markets([market()])
    p.queue = observer.WriteQueue(1)
    p.queue.put('x', 'observation', [('protected', ())])
    monkeypatch.setattr(p.queue, 'put', lambda *args: pytest.fail('receive enqueued SQL'))
    monkeypatch.setattr(observer, 'get_db_connection', lambda: pytest.fail('receive opened DB'))
    monkeypatch.setattr(recording, 'get_db_connection', lambda: pytest.fail('receive opened DB'))
    for i in range(10):
        p.receive(json.dumps(dict(event_type='last_trade_price', asset_id='u', price='.5', size=str(i), side='BUY')))
    assert len(p.recording.pending) == 2
    assert p.recording.dropped == 8
    assert p.recording.pending[-1][2][0][1][4] == '9'


def test_book_change_throttle_retains_latest_and_independent_of_quote(monkeypatch):
    p = observer.Pipeline()
    p.set_markets([market()])
    p.queue.take(500)  # Window metadata.
    monkeypatch.setattr(observer, 'opportunity', lambda *args: None)
    p.receive(json.dumps(book(timestamp=str(int(NOW.timestamp() * 1000)))))
    p.evaluate(10)
    first = [item for item in p.queue.take(500) if item[1] == 'book']
    assert len(first) == 1
    assert first[0][2][0][1][3] == NOW
    p.receive(json.dumps(book(size='20')))
    p.evaluate(10.5)
    assert not [item for item in p.queue.take(500) if item[1] == 'book']
    p.evaluate(11)
    latest = [item for item in p.queue.take(500) if item[1] == 'book']
    assert json.loads(latest[0][2][0][1][6])[0]['size'] == '20'
    p.evaluate(12)
    assert not [item for item in p.queue.take(500) if item[1] == 'book']


def test_handover_only_current_owner_records_trades_and_prune():
    p = observer.Pipeline()
    p.set_markets([market()])
    trade = json.dumps(dict(event_type='last_trade_price', asset_id='u', price='.5', side='BUY'))
    p.receive(trade, owned={'d'})
    assert not p.recording.pending
    p.receive(trade, owned={'u'})
    assert len(p.recording.pending) == 1
    p.receive(json.dumps(book()), owned={'u'})
    p.set_markets([])
    assert not p.recording.metadata


def test_queue_overflow_never_coalesces_distinct_trades_and_exposes_loss():
    q = observer.WriteQueue(2)
    q.put('u', 'trade', [('trade', (1,))])
    q.put('u', 'trade', [('trade', (2,))])
    assert len(q.items) == 2
    q.put('u', 'trade', [('trade', (3,))])
    assert q.metrics['dropped'] == 1
    assert [item[2][0][1] for item in q.items] == [(2,), (3,)]


@pytest.mark.parametrize(('start', 'end', 'outcome'), [('100', '101', 'Up'), ('100', '100', 'Up'), ('100', '99', 'Down'), (None, '100', None), ('NaN', '100', None)])
def test_settlement_matching(start, end, outcome):
    assert recording.computed_outcome(start, end) == outcome


def test_official_finality_identity_binary_prices_and_label_order():
    row = dict(conditionId='c', closed=True, umaResolutionStatus='resolved', outcomes='["Down","Up"]', outcomePrices='["0","1"]')
    assert recording.official_outcome(row, 'c') == 'Up'
    for key, value in [('closed', False), ('umaResolutionStatus', 'proposed'), ('conditionId', 'other'), ('outcomePrices', '["0.1","0.9"]'), ('outcomePrices', '["NaN","1"]')]:
        assert recording.official_outcome(row | {key: value}, 'c') is None


def test_anonymous_auth_rejection_stops_without_auth_frame():
    p = observer.Pipeline()
    sock = Mock()
    p.references.receive(sock, json.dumps(dict(op='error', code='auth_required')))
    assert p.references.state == 'auth_required'
    sock.close.assert_called_once()
    sock.send.assert_not_called()
    assert p.recording.twap_cache == {}


def test_anonymous_subscription_has_only_btc_eth_no_credentials(monkeypatch):
    p = observer.Pipeline()
    frames = []
    class Socket:
        def __init__(self, url, **callbacks):
            assert url == 'wss://ws-live-v2.polymarket.com/ws'
            self.callbacks = callbacks
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
        def send(self, raw):
            frames.append(json.loads(raw))
    import websocket
    monkeypatch.setattr(websocket, 'WebSocketApp', Socket)
    p.references.connect()
    p.references.socket_thread.join(1)
    assert len(frames) == 1
    assert frames[0]['op'] == 'subscribe'
    assert [s['filter']['symbol'] for s in frames[0]['subscriptions']] == ['btcusd', 'ethusd']
    assert 'auth' not in json.dumps(frames)


def test_recording_flag_disables_all_staging(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ENABLED', 'false')
    p = observer.Pipeline()
    p.set_markets([market()])
    p.receive(json.dumps(book()))
    p.evaluate(10)
    assert not p.recording.metadata
    assert all(item[1] not in {'book', 'trade', 'twap', 'window'} for item in p.queue.items)

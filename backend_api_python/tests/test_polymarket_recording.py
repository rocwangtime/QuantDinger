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
    assert p.recording.metadata['u'] == (None, p.recording.metadata['u'][1], 'ws')
    assert not p.recording.pending
    assert all(item[1] not in {'book', 'trade', 'twap', 'window'} for item in p.queue.items)


@pytest.mark.parametrize(('books', 'trades', 'expected'), [
    (None, None, (7, 7)), ('', '', (7, 7)),
    ('3', None, (3, 3)), ('3', '', (3, 3)), ('3', '14', (3, 14)),
    (None, '14', (7, 14)), ('0', None, (1, 1)), ('366', '', (365, 365)),
    ('3', '0', (3, 1)), ('3', '366', (3, 365)),
    ('1', '365', (1, 365)),
])
def test_recording_retention_fallback_override_and_bounds(monkeypatch, books, trades, expected):
    for suffix, value in [('RECORDING', books), ('TRADES', trades)]:
        name = f'POLYMARKET_OBSERVER_{suffix}_RETENTION_DAYS'
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    days = recording.retention_days()
    assert (days['books'], days['trades']) == expected
    assert recording.Recorder().metrics({})['recording_retention_days'] == days


def test_cleanup_uses_distinct_retention_per_table(monkeypatch):
    for suffix, value in [('RECORDING', '3'), ('TRADES', '14'), ('TWAP', '5'), ('WINDOW', '30')]:
        monkeypatch.setenv(f'POLYMARKET_OBSERVER_{suffix}_RETENTION_DAYS', value)
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_CLEANUP_BATCH', '1000')
    cur = Mock()
    recording.cleanup(cur)
    assert cur.execute.call_count == 4
    for call, table, column, days in zip(cur.execute.call_args_list,
            ['books', 'trades', 'twap', 'windows'],
            ['received_at', 'received_at', 'exchange_at', 'end_time'], [3, 14, 5, 30]):
        sql, params = call.args
        assert sql.startswith(f'DELETE FROM qd_polymarket_observer_{table} ')
        assert f'WHERE {column} < NOW()' in sql
        assert params == (days, 1000)



def test_freshness_metrics_separate_idle_age_from_arrival_lag():
    now = datetime.now(timezone.utc)
    recorder = recording.Recorder()
    recorder.metadata = {'idle': (now - timedelta(seconds=15), now - timedelta(seconds=14), 'ws'),
                         'late': (now - timedelta(seconds=10), now, 'ws')}
    metrics = recorder.metrics({'idle': {}, 'late': {}})
    assert 15000 <= metrics['book_age_p95_ms'] < 16000
    assert 14000 <= metrics['book_receipt_age_p95_ms'] < 15000
    assert metrics['book_arrival_lag_p95_ms'] == 10000


CHAINLINK = 'https://data.chain.link/streams/btc-usd-twap-60s-streams'
BINANCE = 'https://www.binance.com/en/trade/BTC_USDT'


def cell(asset, interval, source=CHAINLINK, identity=None):
    seconds = {'5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}[interval]
    tag = identity or f'{asset}{interval}'
    return dict(market(resolutionSource=source), id=tag, conditionId='c' + tag, yesAssetId=tag + 'u',
                noAssetId=tag + 'd', asset=asset, interval=interval,
                endDate=(NOW + timedelta(seconds=seconds)).isoformat())


def scope_env(monkeypatch, assets=None, intervals=None):
    for name, value in (('ASSETS', assets), ('INTERVALS', intervals)):
        if value is None:
            monkeypatch.delenv(f'POLYMARKET_OBSERVER_RECORDING_{name}', raising=False)
        else:
            monkeypatch.setenv(f'POLYMARKET_OBSERVER_RECORDING_{name}', value)


@pytest.mark.parametrize(('assets', 'intervals'), [(None, None), ('', ''), (' ', ' , ')])
def test_default_recording_scope_is_unchanged(monkeypatch, assets, intervals):
    scope_env(monkeypatch, assets, intervals)
    assert recording.recording_scope() == (('BTC', 'ETH'), ('5m', '15m'))
    recorded = {(a, i) for a in ('BTC', 'ETH', 'SOL', 'XRP') for i in ('5m', '15m', '1h', '4h', '1d')
                if recording.scoped(cell(a, i))}
    assert recorded == {('BTC', '5m'), ('BTC', '15m'), ('ETH', '5m'), ('ETH', '15m')}
    assert recording.scoped(dict(id='y', market_type='yes_no')) and recording.scoped(dict(id='y'))
    assert recording.Recorder().metrics({})['recording_scope'] == dict(assets=['BTC', 'ETH'], intervals=['5m', '15m'])


def test_recording_scope_normalizes_and_drops_unknown_names(monkeypatch):
    scope_env(monkeypatch, 'sol, doge,BTC,sol', '1H,2h, 1d ,5m')
    assert recording.recording_scope() == (('SOL', 'BTC'), ('1h', '1d', '5m'))
    scope_env(monkeypatch, 'DOGE', '5m')
    assert recording.recording_scope() == ((), ('5m',))  # Visible in status, never widened silently.
    assert not recording.scoped(cell('BTC', '5m'))
    assert recording.scoped(dict(id='y', market_type='yes_no'))


def test_default_scope_skips_hourly_books_trades_and_windows(monkeypatch):
    scope_env(monkeypatch)
    monkeypatch.setattr(observer, 'opportunity', lambda *args: None)
    p = observer.Pipeline()
    p.set_markets([cell('BTC', '1h', BINANCE), cell('SOL', '5m')])
    assert not [item for item in p.queue.take(500) if item[1] == 'window']
    for token in ('BTC1hu', 'SOL5mu'):
        p.receive(json.dumps(book(token)))
        p.receive(json.dumps(dict(event_type='last_trade_price', asset_id=token, price='.5', size='1', side='BUY')))
    p.evaluate(10)
    assert not p.recording.pending
    assert not [item for item in p.queue.take(500) if item[1] in {'book', 'trade'}]


def test_configured_scope_records_every_cell_with_rule_settlement_window(monkeypatch):
    scope_env(monkeypatch, 'BTC,ETH,SOL,XRP', '5m,15m,1h,4h,1d')
    monkeypatch.setattr(observer, 'opportunity', lambda *args: None)
    markets = [cell('BTC', '5m'), cell('SOL', '15m'), cell('BTC', '1h', BINANCE), cell('XRP', '4h'),
               cell('ETH', '1d', BINANCE), cell('ETH', '1h', ''), cell('XRP', '1d', '')]
    p = observer.Pipeline()
    p.set_markets(markets)
    windows = {item[0]: item[2][0][1] for item in p.queue.take(500) if item[1] == 'window'}
    assert set(windows) == {m['id'] for m in markets}
    # Chainlink TWAP rules 60 s, Binance candles 0; unreadable rules fall back by label.
    assert {k: v[-1] for k, v in windows.items()} == {
        'BTC5m': 60, 'SOL15m': 60, 'BTC1h': 0, 'XRP4h': 60, 'ETH1d': 0, 'ETH1h': 0, 'XRP1d': 0}
    assert windows['BTC1h'][:9] == ('BTC1h', 'cBTC1h', 'BTC', '1h', NOW.isoformat(),
                                    (NOW + timedelta(hours=1)).isoformat(), None, 'BTC1hu', 'BTC1hd')
    for m in markets:
        p.receive(json.dumps(book(m['yesAssetId'], timestamp=str(int(NOW.timestamp() * 1000)))))
        p.receive(json.dumps(dict(event_type='last_trade_price', asset_id=m['noAssetId'], price='.5', size='1', side='SELL')))
    p.evaluate(10)
    queued = p.queue.take(5000)
    assert {item[0] for item in queued if item[1] == 'book'} == {m['yesAssetId'] for m in markets}
    assert {item[0] for item in queued if item[1] == 'trade'} == {m['noAssetId'] for m in markets}
    p.set_markets(markets)  # Unchanged catalogue: no duplicate window registration.
    assert not [item for item in p.queue.take(500) if item[1] == 'window']


@pytest.mark.parametrize(('source', 'interval', 'expected'), [
    (CHAINLINK, '5m', 60), (CHAINLINK, '4h', 60), (BINANCE, '1h', 0), (BINANCE, '1d', 0),
    (BINANCE, '15m', 0), ('', '5m', 60), ('', '15m', 60), ('', '4h', 60), ('', '1h', 0), ('', '1d', 0),
])
def test_settlement_window_follows_rules_then_label(source, interval, expected):
    assert recording.settlement_window(cell('BTC', interval, source)) == expected

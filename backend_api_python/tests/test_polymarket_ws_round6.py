"""Public-feed first frames, staggered fan-in, failover and safe diagnostics."""
import importlib.util
import json
import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import websocket

from app.services.polymarket import observer, ws_rotation, ws_transport
from app.services.polymarket.ws_balance import RateBalancer
from app.services.polymarket.ws_redundancy import FeedMerge
from app.services.polymarket.ws_subscriptions import SubscriptionPacer
from tests.test_polymarket_ws_admission import state
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for


def test_large_initial_subscriptions_then_round_robin_without_shard_order_starvation():
    pacer = SubscriptionPacer()
    feeds = [state([f'{s}-{i}' for i in range(16)]) for s in range(3)]
    pacer.next_at = 1000
    for s in feeds:
        pacer.enqueue(s, s['assets'], initial=True)
        pacer.drain(s, 0)
        assert len(s['sent'][0]['assets_ids']) == 12
        assert s['sent'][0]['type'] == 'market'
    for now in (1000, 1000.5, 1001, 1001.5, 1002, 1002.5):
        before = sum(len(s['sent']) for s in feeds)
        for s in feeds:
            pacer.drain(s, now)
        assert sum(len(s['sent']) for s in feeds) == before + 1
        expected = int((now - 1000) * 2) % 3
        assert feeds[expected]['sent'][-1]['operation'] == 'subscribe'
    assert not pacer.waiting and all(not s['subscribe_pending'] for s in feeds)


def test_initial_subscription_is_immediate_even_with_shared_pressure():
    pacer, s = SubscriptionPacer(), state(['a', 'b'])
    s['socket'].subscription_ready = observer.Event()
    s['socket'].transport.last = {'tcp_pending_bytes': 100000}
    s['subscribe_pause_until'] = 100
    pacer.next_at = 100
    pacer.enqueue(s, s['assets'], initial=True)
    pacer.drain(s, 0)
    assert s['sent'] == [dict(type='market', assets_ids=['a', 'b'])]
    assert s['socket'].subscription_ready.is_set()


def test_heartbeat_waits_for_first_successful_subscription(monkeypatch):
    clock, sent = [0], []
    app = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
    app.keep_running, app.ping_interval = True, 10
    app.sock = SimpleNamespace(send=sent.append)
    def wait(_):
        clock[0] += 1
        if clock[0] == 15:
            assert not sent
            sent.append('subscription')
            app.subscription_ready.set()
        return clock[0] > 21
    app.stop_ping = SimpleNamespace(wait=wait)
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    app._send_ping()
    assert sent == ['subscription', 'PING']


def backup(m, sockets, monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ROTATE_BUSY_WARM_SECONDS', '0')
    monkeypatch.setattr(m, 'hot_shard', lambda index: True)
    m.balancer.rates = {'market:a': 500}
    old = m.states[0]
    old['opened_at'] -= 121
    m.tick()
    assert len(sockets) == 2 and sockets[1].opened.wait(1)
    return old, m.backups[0]


def test_replacement_does_not_wait_for_primary_age_and_hot_rotation_defaults_off(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_HOT_ROTATE_SECONDS', raising=False)
    monkeypatch.setattr(m, 'hot_shard', lambda index: True)
    m.balancer.rates = {'market:a': 500}
    old = m.states[0]
    old['backup_started'] = True  # First startup backup is now eager (round 8).
    old['opened_at'] -= 89
    m.tick()
    assert m.standby is None
    assert len(sockets) == 2 and sockets[1].opened.wait(1)
    candidate = m.backups[0]
    assert candidate['opened_at'] - old['opened_at'] < 90
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: candidate.get('ready'))
    old['opened_at'] -= 2100
    m.tick()
    assert m.states[0] is old and m.rotations == 0
    assert observer.STATUS['ws_hot_rotate_seconds'] == 0


def test_ready_backup_merges_duplicates_preserves_trade_multiplicity_and_fails_over(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, candidate = backup(m, sockets, monkeypatch)
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: candidate.get('ready'))
    trade = dict(event_type='last_trade_price', asset_id='ay', price='.4', size='1', side='BUY',
                 timestamp=int(datetime.now(timezone.utc).timestamp() * 1000))
    before = p.messages
    sockets[0].emit([trade, trade])
    wait_for(lambda: p.messages == before + 1)
    sockets[1].emit([trade, trade])
    wait_for(lambda: m.merges[0].duplicates >= 2)
    assert p.messages == before + 1
    live_book = p.books['ay']
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer: send buffer full')
    wait_for(lambda: m.states[0] is candidate)
    assert p.books['ay'] is live_book and m.backup_promotions == 1
    assert not m.backups and old['retired']
    m.tick()
    assert len(sockets) == 2  # New backup also waits for the stagger.
    sockets[0].callbacks['on_error'](sockets[0], OSError('late error'))
    assert candidate['connected'] and not candidate['inbox'].invalid.is_set()


def test_hot_rotation_opt_in_promotes_ready_backup(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    old, candidate = backup(m, sockets, monkeypatch)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_ROTATE_SECONDS', '150')
    old['opened_at'] -= 31
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: m.states[0] is candidate)
    assert m.backup_promotions == 1 and old['retired']


def test_warming_backup_survives_parallel_primary_recovery(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, candidate = backup(m, sockets, monkeypatch)
    sockets[0].close()
    old['thread'].join(1)
    old['retry_at'] = 0
    m.tick()
    assert len(sockets) == 3 and sockets[2].opened.wait(1)
    replacement = m.states[0]
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: candidate.get('ready'))
    assert m.states[0] is replacement and len(p.books) == 2


def test_shadow_stale_snapshot_cannot_hide_behind_newer_metadata():
    shadow = ws_rotation.ShadowBooks(['a'])
    now = datetime.now(timezone.utc)
    shadow.receive(book('a', '5', 1780000000005), now)
    shadow.receive(book('a', '1', 1780000000001), now)
    assert shadow.books['a']['asks']['.4'] == '5'
    assert shadow.recorder.metadata['a'][0].timestamp() * 1000 == 1780000000005


def test_missing_live_books_wait_for_backup_to_reach_retained_watermark(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, candidate = backup(m, sockets, monkeypatch)
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + 1
    sockets[0].emit([book('ay', '3', at), book('an', '3', at)])
    wait_for(lambda: p.books['ay']['asks']['.4'] == '3')
    sockets[0].close()
    old['thread'].join(1)
    old['worker'].join(1)
    assert not p.books
    sockets[1].emit([book('ay', '1', at - 1), book('an', '1', at - 1)])
    wait_for(lambda: len(candidate['shadow'].books) == 2)
    assert not p.books and m.states[0] is old
    sockets[1].emit([book('ay', '4', at), book('an', '4', at)])
    wait_for(lambda: m.states[0] is candidate)
    assert p.books['ay']['asks']['.4'] == '4'


def test_backup_normal_shutdown_is_separate_from_preparation_failure(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, _ = backup(m, sockets, monkeypatch)
    sockets[1].callbacks['on_close'](sockets[1], 1001, 'server shutting down')
    m.tick()
    assert m.rotation_failures == 0 and old['connected'] and len(p.books) == 2
    assert observer.STATUS['close_reason_counts'] == {'server_shutdown': 1}


def test_backup_failure_keeps_primary_and_backoff_records_stage(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, candidate = backup(m, sockets, monkeypatch)
    sockets[1].callbacks['on_error'](sockets[1], OSError('backup wire failure'))
    m.tick()
    assert not m.backups and old['connected'] and len(p.books) == 2
    assert m.rotation_failures == 1
    assert 0 <= m.rotation_last_failure['retry_seconds'] <= .5
    for delay in (1, 2, 4, 8):
        m.rotation_failed(0, old, candidate, 'test')
        assert 0 <= m.rotation_last_failure['retry_seconds'] <= delay
    assert m.rotation_last_failure['stage']


def test_normal_1001_counted_once_without_error_or_rotation_failure(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    errors = []
    monkeypatch.setattr(observer, '_ws_error', errors.append)
    app = sockets[0]
    frame = websocket.ABNF(fin=1, opcode=8, data=struct.pack('!H', 1001) + b'server shutting down')
    app.callbacks['on_error'](app, frame)
    app.callbacks['on_close'](app, None, None)
    assert observer.STATUS['close_reason_counts'] == {'server_shutdown': 1}
    assert not errors and not m.rotation_failures
    remaining = m.states[0]['retry_at'] - observer.time.monotonic()
    assert -.01 <= remaining <= .5


def test_invalid_subscription_has_own_category(feed_manager):
    m, _, sockets = feed_manager
    sockets[0].callbacks['on_close'](sockets[0], 1008, 'invalid subscription payload')
    assert observer.STATUS['close_reason_counts'] == {'invalid_subscription': 1}


@pytest.mark.parametrize('code', [1001, 1008, 1013])
def test_typed_close_exception_preserves_status_and_reason(code):
    error = websocket.WebSocketConnectionClosedException('closed')
    error.status_code, error.reason = code, 'public close reason'
    assert ws_transport.remote_close(error) == (code, 'public close reason')


def test_merge_stale_books_and_levels_cannot_roll_back_newer_data_but_late_trades_survive():
    merge = FeedMerge({})
    assert merge.filter(book('a', at=1780000000000), 'main', {'a'})
    delta = dict(event_type='price_change', timestamp=1780000000002,
                 price_changes=[dict(asset_id='a', side='SELL', price='.4', size='5')])
    assert merge.filter(delta, 'main', {'a'})
    assert not merge.filter(delta, 'backup', {'a'}, backup=True)
    assert not merge.filter(book('a', at=1780000000001), 'backup', {'a'}, backup=True)
    stale = {**delta, 'timestamp': 1780000000001}
    assert not merge.filter(stale, 'backup', {'a'}, backup=True)
    assert merge.filter(dict(event_type='last_trade_price', asset_id='a', timestamp=1780000000000), 'backup', {'a'})
    assert not merge.filter(book('other'), 'backup', {'a'}, backup=True)


def test_merge_cache_is_bounded_and_expires(monkeypatch):
    merge = FeedMerge({})
    monkeypatch.setattr(observer.time, 'monotonic', lambda: 0)
    trade = dict(event_type='last_trade_price', asset_id='a', timestamp=1)
    assert merge.filter([trade, trade], 'main', {'a'}) == [trade, trade]
    assert merge.filter([trade, trade], 'backup', {'a'}) == []
    monkeypatch.setattr(observer.time, 'monotonic', lambda: 61)
    assert merge.filter(trade, 'backup', {'a'})
    assert merge.evictions == 1 and len(merge.seen) == 1


def test_byte_budget_drives_balancing_without_message_overload():
    b = RateBalancer(2)
    b.set_groups({k: k for k in 'abcd'}, now=0)
    b.rates = {k: 1 for k in 'abcd'}
    b.byte_rates = {k: 150000 for k in 'abcd'}
    b.measured = set('abcd')
    target = b.place(set('abcd'), dict(a=0, b=0, c=0, d=1))
    assert max(b.byte_loads(target)) == 300000
    assert b.capacity_cost(target) == (0, 0, 2 * (300000 - b.max_bytes))
    assert max(b.asset_counts(target)) == 2


def test_frame_bytes_attributed_once_across_assets_and_ewma():
    b = RateBalancer(1)
    b.set_groups(dict(a='a', b='b'), now=0)
    b.assignment = dict(a=0, b=0)
    b.sample_at = 0
    b.receive([book('a'), book('b')], 0, 100000)
    b.sample(1)
    assert b.byte_rates == dict(a=50000, b=50000)


def test_shard_byte_peak_survives_a_later_quiet_window(feed_manager, monkeypatch):
    m, _, _ = feed_manager
    clock = [observer.time.monotonic()]
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    inbox = m.states[0]['inbox']
    inbox.peak_at = clock[0]
    inbox.peak_bytes = inbox.received_bytes
    inbox.received_bytes += 20000
    clock[0] += 2
    m.tick()
    assert observer.STATUS['ws_receive_queues'][0]['peak_receive_bytes_per_second'] == 10000
    clock[0] += 2
    m.tick()
    assert observer.STATUS['ws_receive_queues'][0]['peak_receive_bytes_per_second'] == 10000


def test_merge_cache_capacity_bounds_memory_under_unique_traffic(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_MERGE_CACHE_MAX', '65536')
    merge = FeedMerge({})
    now = observer.time.monotonic()
    for i in range(65536):
        merge.seen[i] = (now, {'old': 1})
    assert merge.unique({'event_type': 'last_trade_price', 'asset_id': 'a'}, 'new')
    assert len(merge.seen) == 65536 and merge.evictions == 1


def test_transport_byte_window_and_handshake_allowlist_survive_socket_teardown():
    transport = ws_transport.TransportDiagnostics()
    app = SimpleNamespace(sock=SimpleNamespace(getheaders=lambda: {
        'CF-Ray': 'public-ray', 'Server': 'cloudflare', 'Sec-WebSocket-Extensions': 'permessage-deflate',
        'Authorization': 'DO-NOT-LOG', 'Set-Cookie': 'DO-NOT-LOG', 'X-Api-Key': 'DO-NOT-LOG'}))
    transport.capture_headers(app)
    for second in range(100):
        transport.read(10, second)
        transport.read(5, second + .5)
    assert len(transport.read_buckets) == 61
    app.sock = None
    assert transport.sample(app, 100)['read_bytes_60s'] == 59 * 15
    assert transport.headers == {'cf-ray': 'public-ray', 'server': 'cloudflare',
                                 'sec-websocket-extensions': 'permessage-deflate'}
    assert 'DO-NOT-LOG' not in json.dumps(transport.last) + json.dumps(transport.headers)


def test_close_only_path_records_reason_age_and_handshake_before_teardown(feed_manager):
    m, _, sockets = feed_manager
    old = m.states[0]
    old['opened_at'] -= 205
    app = sockets[0]
    app.transport.headers = {'cf-ray': 'public-ray'}
    app.transport.read(1234)
    app.callbacks['on_close'](app, 1013, 'slow consumer: send buffer full')
    evidence = old['last_transport_failure']
    assert evidence['reason'] == 'slow consumer: send buffer full'
    assert evidence['connection_age_seconds'] >= 205
    assert evidence['sample']['read_bytes_60s'] == 1234
    assert evidence['handshake_headers'] == {'cf-ray': 'public-ray'}


def test_hot_oldest_tokens_separate_exchange_age_receipt_age_and_arrival_lag(monkeypatch):
    p = observer.Pipeline()
    p.set_markets([dict(id='a', yesAssetId='ay', noAssetId='an')])
    monkeypatch.setattr(observer, 'STATUS', dict(ws_group_rates={'market:a': 500}))
    now = datetime.now(timezone.utc)
    p.receive(book('ay', at=int((now - timedelta(seconds=5)).timestamp() * 1000)), received_at=now)
    rows = p.metrics()['book_hot_oldest_tokens']
    row = next(r for r in rows if r['token'] == 'ay')
    assert row['book_age_ms'] >= 5000 and row['receipt_age_ms'] < 1000
    assert row['arrival_lag_ms'] >= 5000

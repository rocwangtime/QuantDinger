"""Round14: independent ingress work, honest warm spares and bounded paper evidence."""
import json
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import maker, observer, recording, ws_payloads
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_redundancy import FeedContinuity, FeedMerge
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import make_hot, ready_backup
from tests.test_polymarket_maker_optimization import Account, NOW, P, summaries


def test_initial_spare_dial_is_staggered_and_subscriptions_have_different_order(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS')
    make_hot(m)
    old = m.states[0]
    old['opened_at'] = time.monotonic()
    m.tick()
    assert not m.backups and len(sockets) == 1
    assert 7 <= old['backup_phase_seconds'] <= 11
    old['opened_at'] -= 12
    m.tick()
    assert sockets[1].opened.wait(1)
    initial = [json.loads(s.sent[0]) for s in sockets]
    assert initial[0]['assets_ids'] == list(reversed(initial[1]['assets_ids']))
    assert all(s['type'] == 'market' for s in initial)
    assert sockets[0].heartbeat_phase == 10
    assert 3 <= sockets[1].heartbeat_phase <= 5
    assert not any(s == 'PING' for sock in sockets for s in sock.sent)


def test_pair_traffic_evidence_counts_physical_streams_once(feed_manager):
    m, _, sockets = feed_manager
    backup = ready_backup(m, sockets)
    for sock, rate, total in [(sockets[0], 400000, 18000000), (sockets[1], 420000, 19000000)]:
        sock.transport = SimpleNamespace(last=dict(read_bytes_per_second=rate, read_bytes_60s=total))
    load = m.pair_load(0, backup)
    assert load['peer_connections'] == 1
    assert load['pair_receive_bytes_per_second'] == 820000
    assert load['pair_read_bytes_60s'] == 37000000
    assert load['heartbeat_phase_seconds'] == sockets[1].heartbeat_phase


def test_large_decode_does_not_hold_shared_parser_lock(monkeypatch):
    cache = ws_payloads.ParsedFrames()
    first, second = json.dumps(book('a')), json.dumps(book('b'))
    entered, release = threading.Event(), threading.Event()
    original = json.loads
    def blocked(raw):
        if raw == first:
            entered.set()
            assert release.wait(2)
        return original(raw)
    monkeypatch.setattr(json, 'loads', blocked)
    worker = threading.Thread(target=cache.parse, args=(first,))
    worker.start()
    try:
        assert entered.wait(1)
        assert cache.parse(second)['asset_id'] == 'b'
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive() and cache.metrics()['misses'] == 2


def test_prepared_row_identity_is_shared_by_continuity_and_merge(monkeypatch):
    raw = json.dumps(dict(event_type='price_change', timestamp=1000,
        price_changes=[dict(asset_id='a', side='SELL', price='.4', size='2')]))
    cache = ws_payloads.ParsedFrames()
    event = cache.parse(raw)
    def forbidden(*args, **kwargs):
        pytest.fail('JSON fingerprint work reached publication')
    monkeypatch.setattr(ws_payloads.json, 'dumps', forbidden)
    merge, continuity = FeedMerge({}), FeedContinuity()
    at = datetime.now(timezone.utc)
    for source in ('primary', 'backup'):
        continuity.receive(event, source, at)
        assert bool(merge.filter(event, source, {'a'})) == (source == 'primary')
    assert len(continuity.history['a']) == 1 and merge.duplicates == 1


def test_materialized_full_book_never_aliases_mutable_consumers():
    cache = ws_payloads.ParsedFrames()
    event = cache.parse(json.dumps(book('a', '3')))
    levels, charge = ws_payloads.materialized_book(event)
    books = {}
    observer.ws_apply_event(event, books)
    history = BookHistory()
    history.record(event, 1000)
    books['a']['asks']['.4'] = '99'
    assert levels['asks']['.4'] == '3'
    assert history.assets['a']['events'][0][1]['asks']['.4'] == '3'
    assert cache.metrics()['charged_bytes'] >= charge


@pytest.mark.parametrize('resync', [False, True])
def test_private_shadow_build_does_not_block_primary_and_resync_fences_publication(feed_manager, monkeypatch, resync):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    shadow = candidate['shadow']
    entered, release = threading.Event(), threading.Event()
    original = shadow.receive
    def blocked(event, received):
        if event:
            entered.set()
            assert release.wait(2)
        return original(event, received)
    monkeypatch.setattr(shadow, 'receive', blocked)
    at = int(time.time() * 1000)
    sockets[1].emit(book('ay', '8', at))
    try:
        assert entered.wait(1)
        # This must drain while the other connection is building its private book.
        sockets[0].emit(book('ay', '9', at + 1))
        wait_for(lambda: p.books['ay']['asks']['.4'] == '9', .5)
        if resync:
            candidate['inbox'].epoch += 1
            candidate['inbox'].resync.set()
    finally:
        release.set()
    if resync:
        wait_for(lambda: candidate['shadow'] is not shadow)
        assert not candidate.get('ready')
    else:
        wait_for(lambda: candidate['inbox'].queue.empty())
    assert p.books['ay']['asks']['.4'] == '9'


@pytest.mark.parametrize('group', sorted(observer.HOT_SERIES))
def test_hot_replay_stays_95_percent_ready_and_preserves_book_under_one_second_failover(feed_manager, group):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    m.balancer.set_groups({a: group for a in m.states[0]['assets']})
    start = time.monotonic()
    m.update_hot_coverage(start)
    for batch in range(12):
        for i in range(40):
            at = int(time.time() * 1000)
            event = dict(event_type='price_change', timestamp=at, price_changes=[
                dict(asset_id=a, side='SELL', price=str((400 + j) / 1000), size=str(batch * 40 + i + 1))
                for a in ('ay', 'an') for j in range(10)])
            for sock in sockets[:2]:
                sock.emit(event)
        wait_for(lambda: p.books['ay']['asks'].get('0.4') == str(batch * 40 + 40))
        wait_for(lambda: candidate['inbox'].queue.empty() and m.backup_ready(candidate))
        m.update_hot_coverage(time.monotonic())
    assert m.update_hot_coverage(time.monotonic())[group]['hot_backup_ready_ratio'] >= .95
    before = {a: {side: dict(rows) for side, rows in b.items()} for a, b in p.books.items()}
    kicked = time.monotonic()
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer: send buffer full')
    wait_for(lambda: m.states[0] is candidate, .9)
    assert time.monotonic() - kicked < 1 and p.books == before
    assert not candidate['inbox'].dropped and not candidate['inbox'].resyncs


def test_history_byte_budget_counts_full_ladders_and_retains_timestamped_anchor():
    h = BookHistory(maximum=2000, byte_limit=24000)
    for at in range(1000, 1100):
        h.record(dict(event_type='book', asset_id='a', timestamp=at, bids=[],
            asks=[dict(price=str(i / 100), size=str(at)) for i in range(1, 51)]), at)
        assert 0 <= h.charged_bytes <= h.byte_limit
    assert h.byte_evictions > 0 and len(h.assets['a']['events']) < 100
    assert h.lookup(h.copy_assets(['a']), 'a', 1000, 2000) is None
    assert h.lookup(h.copy_assets(['a']), 'a', 1099, 2000)['asks'][0]['size'] == '1099'


def test_history_global_budget_covers_multiple_assets_and_oversized_anchor():
    h = BookHistory(byte_limit=9000)
    for i in range(100):
        h.record(book(str(i), at=1000 + i), 1000 + i)
        assert h.charged_bytes <= 9000
    assert len(h.assets) < 100
    h.record(dict(event_type='book', asset_id='huge', timestamp=2000, bids=[],
        asks=[dict(price='.4', size='9' * 10000)]), 2000)
    assert h.charged_bytes <= 9000 and 'huge' not in h.assets


def test_history_idle_expiry_and_forget_keep_budget_exact():
    h = BookHistory(seconds=10)
    h.record(book('quiet', at=1000), 1000)
    h.record(book('live', at=11001), 11001)
    h.prune(11001)
    assert 'quiet' not in h.assets and h.idle_evictions == 1
    assert h.charged_bytes == sum(s['charged_bytes'] for s in h.assets.values())
    h.retain([])
    assert not h.assets and h.charged_bytes == 0


def test_history_byte_eviction_replays_deltas_and_deletions_without_inventing_past():
    h = BookHistory(byte_limit=4000)
    h.record(book('a', at=1000), 1000)
    for i in range(20):
        h.record(dict(event_type='price_change', timestamp=1001 + i, price_changes=[
            dict(asset_id='a', side='SELL', price='.4', size='0' if i == 19 else str(i + 1))]), 1001 + i)
        assert h.charged_bytes == sum(s['charged_bytes'] for s in h.assets.values()) <= h.byte_limit
    assert h.lookup(h.copy_assets(['a']), 'a', 1020, 2000)['asks'] == []
    assert h.lookup(h.copy_assets(['a']), 'a', 999, 2000) is None


def test_pipeline_evaluator_expires_history_even_without_further_market_frames(feed_manager, monkeypatch):
    _, p, _ = feed_manager
    h = BookHistory(seconds=10)
    h.record(book('ay', at=1000), 1000)
    p.paper_account = SimpleNamespace(history=h, submit=lambda *args: None)
    p.dirty.clear()
    monkeypatch.setattr(observer, '_end_episode', lambda *args: None)
    p.evaluate()
    assert not h.assets and h.charged_bytes == 0
    p.paper_account = None


def test_review_skips_profitable_unrecorded_cells_before_capacity_and_warns(monkeypatch, caplog):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ASSETS', 'BTC,ETH')
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_INTERVALS', '5m,15m')
    rows = summaries('BTC:1h', 7, 30, lambda d, k: .4 + k % 3 * .01)
    rows += summaries('ETH:1h', 7, 30, lambda d, k: .5 + k % 3 * .01)
    previous = dict(active={'BTC:1h': dict(source='admitted', since='', mode='full')})
    m = maker.Maker(Account(), {})
    m.state['selection'] = previous
    original_config, original_hash = dict(m.config), m.hash
    m.review(rows, NOW)
    selection = m.state['selection']
    assert set(selection['active']) == set(P.cold_start)
    assert 'BTC:1h' in selection['unavailable_cells'] and 'ETH:1h' not in selection['scores']
    assert m.config == original_config and m.hash == original_hash
    assert sum('skipped unrecorded cells' in r.message for r in caplog.records) == 1
    assert any(e['event'] == 'selection_scope_warning' for e in m.account.maker_changed)


def test_scope_intersects_discovery_and_allows_explicitly_recorded_hourly(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ASSETS', 'BTC,ETH')
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_INTERVALS', '5m,1h')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ASSETS', 'BTC')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_INTERVALS', '1h')
    assert maker.recorded_cells() == {'BTC:1h'}
    selected = maker.select([], {}, maker.params(dict(maker.DEFAULTS, cold_start='BTC:1h,ETH:1h')), NOW,
                            maker.recorded_cells())
    assert set(selected['active']) == {'BTC:1h'}


def test_empty_recording_scope_does_not_admit_cells_or_raise(monkeypatch, caplog):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ASSETS', 'UNKNOWN')
    m = maker.Maker(Account(), {})
    m.begin(NOW)
    m.review([], NOW)
    assert m.state['selection']['active'] == {} and 'skipped unrecorded cells' in caplog.text


@pytest.mark.parametrize('phase,expected', [(10, [110, 120]), (4, [104, 114, 124])])
def test_heartbeat_phase_changes_only_first_deadline(monkeypatch, phase, expected):
    import websocket
    app = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
    clock, sent = [100], []
    app.keep_running, app.ping_interval, app.heartbeat_phase = True, 10, phase
    app.subscription_ready.set()
    app.sock = SimpleNamespace(send=lambda raw: sent.append((clock[0], raw)))
    def wait(seconds):
        clock[0] += seconds
        return clock[0] > 124
    app.stop_ping = SimpleNamespace(wait=wait)
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(app.transport, 'sample', lambda *args: None)
    app._send_ping()
    assert sent == [(at, 'PING') for at in expected]


def test_old_popped_epoch_never_builds_or_publishes_backup_book(feed_manager):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    inbox = candidate['inbox']
    with m.lifecycle_lock:
        before = dict(candidate['shadow'].books['ay']['asks'])
        inbox.epoch += 1
        m.redundant_receive(0, candidate, book('ay', '99'), datetime.now(timezone.utc), inbox, inbox.epoch - 1)
        assert candidate['shadow'].books['ay']['asks'] == before
        assert p.books['ay']['asks']['.4'] != '99'


def test_reactivated_subscription_fences_detached_shadow(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    entered, release = threading.Event(), threading.Event()
    original = candidate['shadow'].receive
    def blocked(event, received):
        if event:
            entered.set()
            assert release.wait(2)
        return original(event, received)
    monkeypatch.setattr(candidate['shadow'], 'receive', blocked)
    sockets[1].emit(book('ay', '99'))
    try:
        assert entered.wait(1)
        with m.lifecycle_lock, candidate['inbox'].lock:
            candidate['inbox'].activated['ay'] = datetime.now(timezone.utc)
    finally:
        release.set()
    wait_for(lambda: candidate['inbox'].resyncs > 0)
    assert p.books['ay']['asks']['.4'] != '99'
    assert not candidate.get('ready')


def test_malformed_book_parse_fails_as_recoverable_frame_error():
    with pytest.raises(ValueError, match='invalid public book'):
        ws_payloads.ParsedFrames().parse(json.dumps(dict(event_type='book', asset_id='a', timestamp=1000,
                                                       bids=[], asks=[dict(price='.4')])) )


def test_hot_age_sample_distinguishes_receipt_silence_from_network_delivery_lag():
    from datetime import timedelta
    r = recording.Recorder()
    at = datetime.now(timezone.utc) - timedelta(seconds=10.059)
    received = at + timedelta(milliseconds=83.5)
    r.receive(book('a', at=int(at.timestamp() * 1000)), ['a'], {}, {'a': 'm'}, received)
    metrics = r.metrics({'a': {'bids': {}, 'asks': {'.4': '1'}}}, {'a': 'up_down:BTC:5m'},
                        {'up_down:BTC:5m': 500}, 100)
    hot = metrics['book_freshness_by_activity']['hot']
    assert hot['book_age_p95_ms'] >= 10059
    assert hot['book_receipt_age_p95_ms'] >= 9975
    assert hot['book_arrival_lag_p95_ms'] < 85


def test_already_reviewed_unrecorded_selection_is_repaired_without_double_counting_today(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ASSETS', 'BTC,ETH')
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_INTERVALS', '5m,15m')
    m = maker.Maker(Account(), {})
    today = maker.day_key(NOW)
    m.state['selection'] = dict(reviewed=today, active={'BTC:1h': dict(source='admitted')},
        lcb_history={'BTC:15m': [.1, .2]})
    assert m.review_due(NOW)
    m.review([], NOW)
    assert not m.review_due(NOW)
    assert m.state['selection']['lcb_history']['BTC:15m'] == [.1, None]


@pytest.mark.parametrize('where', ['queued', 'processing'])
def test_readiness_uses_live_age_of_outstanding_work_not_last_dequeued_sample(feed_manager, where):
    from datetime import timedelta
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    inbox = candidate['inbox']
    old = datetime.now(timezone.utc) - timedelta(seconds=1.1)
    with m.lifecycle_lock:
        inbox.queue_wait_seconds = 0
        if where == 'queued':
            # Keep the worker out while installing the synthetic oldest frame.
            inbox.queue.put_nowait(('{}', old, inbox.epoch))
        else:
            inbox.processing_received_at = old
        assert not m.backup_ready(candidate)
        assert 'queue_wait' in m.readiness_blockers(candidate)
        if where == 'queued':
            try:
                inbox.queue.get_nowait()
            except observer.Empty:
                pass
        inbox.processing_received_at = None

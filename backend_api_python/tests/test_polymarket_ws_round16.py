"""Round16 recovery margin, physical roles, bounded evidence and 100ms diagnostics."""
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, ws_recovery
from app.services.polymarket.ws_redundancy import DoubleKicks, FeedContinuity, FeedMerge
from app.services.polymarket.ws_transport import TransportDiagnostics
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import make_hot, ready_backup


def http_pool(monkeypatch, handler):
    sessions, requests = [], []
    class Session:
        trust_env = True
        def __init__(self):
            self.closed = False
            sessions.append(self)
        def send(self, prepared, **options):
            assets = [r['token_id'] for r in json.loads(prepared.body)]
            requests.append((self, assets, prepared, options))
            rows = handler(assets)
            return SimpleNamespace(content=b'[]', json=lambda: rows, raise_for_status=lambda: None, close=lambda: None)
        def close(self):
            self.closed = True
    monkeypatch.setattr(ws_recovery.requests, 'Session', Session)
    return sessions, requests


def test_prepared_warm_pool_reuses_session_without_publishing_cached_quotes(monkeypatch):
    sessions, calls = http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a'}, parallel=True)
    prepared = state['_rest_prepared'][('a',)]
    assert not sessions and not calls
    assert r.request(state, {'a'}, None, warm=True)
    wait_for(lambda: not r.metrics()['inflight'])
    assert not published and r.metrics()['warmups'] == 1
    state['rest_retry_at'] = 0
    r.request(state, {'a'}, lambda rows, started: published.extend(rows))
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(sessions) == 1 and len(published) == 1
    assert calls[0][2] is calls[1][2] is prepared
    assert sessions[0].trust_env is False
    assert not any(k.lower() in ('authorization', 'cookie') for k in prepared.headers)
    assert calls[0][3] == dict(timeout=(.4, 1), allow_redirects=False)
    r.close()
    assert sessions[0].closed and not r.request({}, {'a'}, lambda *args: None)


def test_parallel_tokens_publish_fast_leg_without_waiting_for_slow_leg(monkeypatch):
    entered, release = threading.Barrier(2), threading.Event()
    def handler(assets):
        entered.wait(1)  # Both HTTP operations must run concurrently.
        if assets == ['b']:
            assert release.wait(2)
        return [book(a) for a in assets]
    _, calls = http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a', 'b'}, parallel=True)
    r.request(state, {'a', 'b'}, lambda rows, started: published.extend(rows))
    try:
        wait_for(lambda: len(published) == 1)
        assert published[0]['asset_id'] == 'a' and r.metrics()['inflight'] == 1
        assert len(calls) == 2
    finally:
        release.set()
    wait_for(lambda: not r.metrics()['inflight'])
    assert {e['asset_id'] for e in published} == {'a', 'b'}
    r.close()


def test_parallel_failure_isolated_and_budget_counts_http_operations(monkeypatch):
    def handler(assets):
        if assets == ['b']:
            raise ws_recovery.requests.Timeout()
        return [book('a')]
    http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a', 'b'}, parallel=True)
    r.request(state, {'a', 'b'}, lambda rows, started: published.extend(rows))
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(published) == 1 and r.metrics()['failures'] == 1 and len(r.starts) == 2
    with r.lock:
        r.starts = ws_recovery.deque([time.monotonic()]*39)
    state['rest_retry_at'] = 0
    assert not r.request(state, {'a', 'b'}, lambda *args: None)
    assert r.metrics()['throttled'] == 1
    r.close()


def test_changed_assets_replace_prepared_bodies_and_inflight_uses_original(monkeypatch):
    arrived, release = threading.Event(), threading.Event()
    def handler(assets):
        arrived.set();assert release.wait(2)
        return [book(a) for a in assets]
    http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a'})
    r.request(state, {'a'}, lambda rows, started: published.extend(rows))
    assert arrived.wait(1)
    r.prepare(state, {'new'})
    release.set();wait_for(lambda: not r.metrics()['inflight'])
    assert published[0]['asset_id'] == 'a' and set(state['_rest_prepared']) == {('new',)}
    r.close()


def test_close_during_http_closes_checked_out_session(monkeypatch):
    arrived, release = threading.Event(), threading.Event()
    def handler(assets):
        arrived.set();assert release.wait(2)
        return [book('a')]
    sessions, _ = http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery()
    r.request({}, {'a'}, lambda *args: None)
    assert arrived.wait(1)
    r.close();release.set()
    wait_for(lambda: not r.metrics()['inflight'])
    assert sessions[0].closed


def test_rest_dispatch_precedes_waiting_for_pipeline_lock(feed_manager, monkeypatch):
    m, p, _ = feed_manager;state = m.states[0]
    make_hot(m)
    dispatched = threading.Event()
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args: dispatched.set())
    thread = threading.Thread(target=lambda: m.record_primary_failure(0, state, 1013))
    with p.lock:
        thread.start()
        assert dispatched.wait(1), 'HTTP launch must precede expensive publication lock'
        assert thread.is_alive()
    thread.join(1)
    assert not thread.is_alive() and not p.books


def test_no_backup_rest_publication_under_one_second_with_400ms_old_feed(feed_manager, monkeypatch):
    m, p, sockets = feed_manager;state = m.states[0];make_hot(m)
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args: callbacks.append(args[2]))
    now = datetime.now(timezone.utc)
    with p.lock:
        for asset in state['assets']:
            at, _, kind = p.recording.metadata[asset]
            p.recording.metadata[asset] = (at, now-timedelta(milliseconds=400), kind)
            m.continuity.published[asset]['received'] = now-timedelta(milliseconds=400)
            m.continuity.published[asset]['published_at'] = now-timedelta(milliseconds=400)
    began = time.monotonic()
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    # Inject a bounded 120ms HTTP response without live WS or network access.
    threading.Event().wait(.12)
    callbacks[0]([book('ay')], now)
    assert 'ay' in p.books and 'an' not in p.books
    callbacks[0]([book('an')], now)
    event = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert event['merged_gap_final'] and 500 <= event['merged_gap_ms'] < 1000
    assert time.monotonic()-began < .6


@pytest.mark.parametrize('lag_ms,ready', [(0, True), (700, True), (750, True), (751, False)])
def test_backup_exchange_grace_is_bounded_and_preserves_newer_live_book(feed_manager, lag_ms, ready):
    m, p, sockets = feed_manager;c = ready_backup(m, sockets)
    at = int(datetime.now(timezone.utc).timestamp()*1000)
    with m.lifecycle_lock, p.lock, c['shadow'].lock:
        m.merges[0].latest.update({a: datetime.fromtimestamp(at/1000, timezone.utc) for a in c['assets']})
        for a in c['assets']:
            c['shadow'].recorder.metadata[a] = (datetime.fromtimestamp((at-lag_ms)/1000, timezone.utc),
                                                datetime.now(timezone.utc), 'ws')
        before = dict(p.books)
        assert m.backup_ready(c) is ready
        assert p.books == before
    if ready:
        sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
        wait_for(lambda: m.states[0] is c, .7)
        assert p.books == before


@pytest.mark.parametrize('failure', ['invalid', 'resync', 'delivery_gap', 'queue', 'missing', 'repair'])
def test_grace_never_admits_invalid_backup(feed_manager, failure):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    with m.lifecycle_lock, c['shadow'].lock:
        inbox = c['inbox']
        if failure in ('invalid', 'resync'):
            getattr(inbox, failure).set()
        elif failure == 'delivery_gap':
            inbox.delivery_gap = True
        elif failure == 'queue':
            c.pop('merged_supply_at', None)
            inbox.processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=1.1)
        elif failure == 'missing':
            c['shadow'].books.pop('ay')
        else:
            c['recover_assets'] = {'ay'}
        assert not m.backup_ready(c)
        # Restore so the worker and fixture can stop normally.
        inbox.processing_received_at = None


def test_double_kick_survives_mutable_roles_and_deduplicates_callbacks():
    kicks = DoubleKicks()
    assert kicks.record('btc', 'p', 'primary', 1013, 10, 3600) is None
    e = kicks.record('btc', 'b', 'primary', 1013, 10.039, 3600.039)
    assert e['sources'] == ['p', 'b'] and e['roles'] == ['primary', 'primary']
    assert kicks.record('btc', 'b', 'primary', 1013, 10.040, 3600.040) is None
    kicks.record('eth', 'other', 'backup', 1013, 10.05, 3600.05)
    kicks.record('btc', 'third', 'backup', 1013, 12, 3602)
    assert kicks.metrics(3602)['total'] == 1
    assert kicks.metrics(3602)['count_current_hour'] == 1
    assert kicks.metrics(7201)['count_rolling_hour'] == 0
    json.dumps(kicks.metrics(3602))


def test_promoted_backup_second_kick_counted_with_final_business_gap(feed_manager, monkeypatch):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args: callbacks.append(args[2]))
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is c, .7)
    assert c['physical_role'] == 'backup' and c['promoted_at']
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer')
    assert m.double_kicks.total == 1
    callbacks[0]([book('ay'), book('an')], datetime.now(timezone.utc))
    m.update_double_kicks()
    report = m.double_kicks.metrics(time.time())
    assert report['events'][0]['merged_gap_final'] and report['business_gaps_rolling_hour'] == 1
    json.dumps(report)
    m.tick()
    assert observer.STATUS['ws_double_kicks']['total'] == 1
    assert all(r['role'] in ('primary', 'backup') for r in observer.STATUS['ws_connections_by_source'])


def test_continuity_global_limit_ttl_and_uncertainty_after_eviction(monkeypatch):
    clock = [10.]
    monkeypatch.setattr('app.services.polymarket.ws_redundancy.time.monotonic', lambda: clock[0])
    c = FeedContinuity();now = datetime.now(timezone.utc)
    for i in range(5000):
        c.receive(book(f'a{i%30}', at=1000+i), 'p', now)
    assert sum(map(len, c.history.values())) == len(c.history_order) == 4096
    assert c.history_evictions == 904
    c.prune({'a0'})
    assert all(a == 'a0' for a, k in c.history_order)
    clock[0] += 31;c.expire()
    assert not c.history and not c.history_order
    assert c.switch('p', 'b', {'a0'}, 'expired') == {'a0'}
    assert not c.records[1]['historical_trades_verified']


def test_dedup_idle_ttl_does_not_remove_stale_level_fences(monkeypatch):
    clock = [10.]
    monkeypatch.setattr('app.services.polymarket.ws_redundancy.time.monotonic', lambda: clock[0])
    m = FeedMerge({})
    newer = dict(event_type='price_change', timestamp=2000, price_changes=[
        dict(asset_id='a', side='SELL', price='.4', size='2')])
    older = dict(newer, timestamp=1000)
    assert m.filter(newer, 'p', {'a'})
    assert not m.filter(newer, 'b', {'a'})
    clock[0] += 16;m.expire()
    assert not m.seen and m.levels
    assert not m.filter(older, 'b', {'a'}, backup=True)


def test_global_dedup_budget_applies_to_all_shards(feed_manager):
    m, _, _ = feed_manager
    for i in range(8):
        merge = m.merges.setdefault(i, FeedMerge({}))
        for n in range(1500):
            merge.unique(dict(id=n), 'p')
    m.cache_maintenance_at = 0;m.tick()
    assert sum(len(merge.seen) for merge in m.merges.values()) <= 8192
    assert sum(merge.maximum for merge in m.merges.values()) <= 8192


def test_100ms_samples_bound_memory_and_preserve_preclose_backlog():
    t = TransportDiagnostics()
    for i in range(1000):
        t.dispatch(.03)
        t.sample_fast(382000 if i == 990 else 0, 10, .4, .2, now=i/10)
    result = t.fast_metrics(True)
    assert result['samples'] == len(result['samples_100ms']) == 100
    assert result['tcp_pending_max_bytes'] == 382000
    assert result['scheduler_lag_max_ms'] == 200 and result['queue_age_max_ms'] == 400
    assert result['reader_dispatch_max_ms'] == 30
    assert t.dispatch_max_seconds == 0
    json.dumps(result)


def test_sampler_collects_while_supervisor_blocked_and_stops(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    sockets[0].transport = t = TransportDiagnostics()
    block = threading.Event()
    monkeypatch.setattr(m, 'tick', lambda: block.wait(1))
    monkeypatch.setattr(observer, 'socket_pending_bytes', lambda sock: 382000)
    m.start_supervisor()
    try:
        wait_for(lambda: t.fast_metrics()['samples'] >= 3)
        assert t.fast_metrics()['tcp_pending_max_bytes'] == 382000
    finally:
        block.set();m.close()
    assert not m.sampler.is_alive()


def test_1013_captures_fast_local_and_peer_evidence_with_roles(feed_manager):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    for sock in sockets[:2]:
        sock.transport = t = TransportDiagnostics()
        t.sample_fast(12345, 3, .2, .15)
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    evidence = m.retiring[-1]['last_transport_failure'] if m.retiring else m.states[0]['last_transport_failure']
    assert evidence['source'] and evidence['role'] == 'primary'
    assert evidence['local_100ms']['tcp_pending_max_bytes'] == 12345
    assert evidence['peer_local_100ms'][0]['queue_age_max_ms'] == 200
    assert c['physical_role'] == 'backup'


@pytest.mark.parametrize('age,supply,ready', [(1.2, True, True), (1.2, False, False), (1.51, True, False)])
def test_recent_actual_merged_supply_gets_bounded_queue_grace(feed_manager, age, supply, ready):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    with m.lifecycle_lock, c['shadow'].lock:
        c['merged_supply_at'] = time.monotonic() if supply else time.monotonic()-1
        c['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=age)
        assert m.backup_ready(c) is ready
        c['inbox'].processing_received_at = None


def test_pressure_refresh_starts_before_close_and_retains_live_watermarks(feed_manager, monkeypatch):
    m, p, _ = feed_manager;state = m.states[0];make_hot(m)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kwargs: calls.append((args, kwargs)) or True)
    with m.lifecycle_lock:
        before = dict(p.books)
        state['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=.5)
        for row in m.continuity.published.values():
            row['received'] = datetime.now(timezone.utc)-timedelta(seconds=.8)
        m.tick()
        state['inbox'].processing_received_at = None
    pressure = [(a, k) for a, k in calls if k.get('pressure')]
    assert len(pressure) == 1 and state['connected'] and p.books == before
    pressure[0][0][2]([book('ay', '8'), book('an', '8')], datetime.now(timezone.utc))
    assert p.books['ay']['asks']['.4'] == '8' and p.books['an']['asks']['.4'] == '8'
    assert not state.get('outage_events')


def test_warm_schedule_is_off_reader_and_never_publishes(feed_manager, monkeypatch):
    m, _, _ = feed_manager;make_hot(m);state = m.states[0]
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kwargs: calls.append((args, kwargs)) or True)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REST_WARM_SECONDS', '60')
    state['rest_warm_at'] = 0;m.tick()
    assert len(calls) == 1 and calls[0][1] == dict(warm=True) and calls[0][0][2] is None
    assert state['rest_warm_at'] > time.monotonic()


def test_pressure_and_warm_leave_emergency_http_budget(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery()
    with r.lock:
        r.starts = ws_recovery.deque([time.monotonic()]*32)
    assert not r.request({}, {'a'}, None, warm=True)
    assert not r.request({}, {'a'}, lambda *args: None, pressure=True)
    assert r.request({}, {'a'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert r.metrics()['requests'] == 1 and r.metrics()['throttled'] == 2
    r.close()


def test_physical_double_kick_with_surviving_supply_is_alert_only(feed_manager):
    m, _, _ = feed_manager
    m.double_kicks.record('btc', 'p', 'primary', 1013, 10, 3600)
    event = m.double_kicks.record('btc', 'b', 'backup', 1013, 10.05, 3600.05)
    before = dict(m.continuity.published['ay'])
    event.update(supply_interrupted=False)
    m.continuity.watch_gap(event, {'ay'})
    m.continuity.publication({'ay': (before['exchange_at'], before['received'] + timedelta(milliseconds=10), 'ws')}, {'ay'})
    m.update_double_kicks()
    result = m.double_kicks.metrics(3600.1)
    assert result['count_current_hour'] == 1 and result['business_gaps_current_hour'] == 0
    assert result['events'][0]['merged_gap_final'] and 0 <= result['events'][0]['merged_gap_ms'] < 100


def test_100ms_sampler_does_not_take_pipeline_or_lifecycle_lock(feed_manager):
    m, p, sockets = feed_manager
    sockets[0].transport = TransportDiagnostics()
    completed = threading.Event()
    with m.lifecycle_lock, p.lock:
        thread = threading.Thread(target=lambda: (m.sample_fast(10, 9.8), completed.set()))
        thread.start()
        assert completed.wait(.5)
    thread.join(1)
    assert sockets[0].transport.fast_metrics()['samples'] == 1


def test_hot_warmup_prepares_both_http_sessions_without_publishing(monkeypatch):
    entered = threading.Barrier(2)
    def handler(assets):
        entered.wait(1)
        return [book(a) for a in assets]
    sessions, calls = http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {}
    r.prepare(state, {'a', 'b'}, parallel=True)
    assert r.request(state, {'a', 'b'}, None, warm=True)
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(sessions) == 2 and {tuple(c[1]) for c in calls} == {('a',), ('b',)}
    assert r.metrics()['completed'] == 2 and r.metrics()['warmups'] == 1
    r.close()



def test_inflight_warmup_cannot_suppress_or_delay_same_state_fault_request(monkeypatch):
    warm_entered, release = threading.Barrier(3), threading.Event()
    sequence, lock = [0], threading.Lock()
    def handler(assets):
        with lock:
            sequence[0] += 1
            warm = sequence[0] <= 2
        if warm:
            warm_entered.wait(1)
            assert release.wait(2)
        return [book(a) for a in assets]
    http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a', 'b'}, parallel=True)
    assert r.request(state, {'a', 'b'}, None, warm=True)
    warm_entered.wait(1)
    try:
        assert r.request(state, {'a', 'b'}, lambda rows, started: published.extend(rows))
        wait_for(lambda: len(published) == 2)
        wait_for(lambda: r.metrics()['inflight'] == 1)  # Only the blocked warmup remains.
    finally:
        release.set()
    wait_for(lambda: not r.metrics()['inflight'])
    assert r.metrics()['requests'] == 4
    r.close()



@pytest.mark.parametrize('kind,ready', [('last_trade_price', False), ('book', True)])
def test_queue_grace_requires_actual_recent_merged_book_publication(feed_manager, kind, ready):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    now = datetime.now(timezone.utc)
    event = book('ay', '8') if kind == 'book' else dict(event_type=kind, asset_id='ay',
        price='.4', size='1', side='BUY', timestamp=int(now.timestamp()*1000))
    with m.lifecycle_lock:
        c.pop('merged_supply_at', None)
        c['inbox'].processing_received_at = now-timedelta(seconds=1.2)
        m.redundant_receive(0, c, event, now, c['inbox'])
        assert m.backup_ready(c) is ready
        c['inbox'].processing_received_at = None

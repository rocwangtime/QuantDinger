"""Round17: bounded background REST, real recovery priority and spare stability."""
import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import pytest

from app.services.polymarket import ws_recovery
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_redundancy import HotBackupCoverage
from tests.test_polymarket_ws_round16 import http_pool
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import make_hot, ready_backup


@pytest.mark.parametrize('kind', ['recovery', 'warmup', 'pressure'])
def test_repeated_trigger_is_single_flight_and_cooldown(monkeypatch, kind):
    entered, release = threading.Event(), threading.Event()
    def handler(assets):
        entered.set()
        assert release.wait(2)
        return [book(a) for a in assets]
    _, calls = http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {}
    options = dict(warm=kind == 'warmup', pressure=kind == 'pressure')
    try:
        assert r.request(state, {'a'}, lambda *args: None, **options)
        assert entered.wait(1)
        for _ in range(100):
            assert not r.request(state, {'a'}, lambda *args: None, **options)
        assert len(calls) == 1
    finally:
        release.set()
    wait_for(lambda: not r.metrics()['inflight'])
    assert not r.request(state, {'a'}, lambda *args: None, **options)
    assert r.metrics()['categories'][kind]['total']['attempts'] == 1
    r.close()


@pytest.mark.parametrize('kind', ['warmup', 'pressure'])
def test_background_keeps_parallel_emergency_slots_and_independent_retry(monkeypatch, kind):
    entered, release = threading.Event(), threading.Event()
    first = [True]
    def handler(assets):
        if first[0]:
            first[0] = False
            entered.set()
            assert release.wait(2)
        return [book(a) for a in assets]
    http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a', 'b'}, parallel=True)
    try:
        assert r.request(state, {'a', 'b'}, lambda *args: None,
                         warm=kind == 'warmup', pressure=kind == 'pressure')
        assert entered.wait(1)
        assert len(r.slots) >= 2
        assert r.request(state, {'a', 'b'}, lambda rows, started: published.extend(rows))
        wait_for(lambda: len(published) == 2)
        assert not r.request({}, {'c'}, None, warm=True)
        assert r.metrics()['categories']['recovery']['total']['requests'] == 2
        assert r.metrics()['throttled'] == 0
    finally:
        release.set()
    wait_for(lambda: not r.metrics()['inflight'])
    r.close()


def test_pressure_batches_two_tokens_into_one_http_operation(monkeypatch):
    _, calls = http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {};published = []
    r.prepare(state, {'a', 'b'}, parallel=True)
    assert r.request(state, {'a', 'b'}, lambda rows, at: published.extend(rows), pressure=True)
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(calls) == 1 and calls[0][1] == ['a', 'b']
    assert len(published) == 2 and r.metrics()['requests'] == 1
    r.close()


def test_budget_denial_backs_off_without_consuming_emergency_credits(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {}
    r.starts.extend([time.monotonic()] * 32)
    assert not r.request(state, {'a'}, lambda *args: None, pressure=True)
    for _ in range(100):
        assert not r.request(state, {'a'}, lambda *args: None, pressure=True)
    assert r.metrics()['throttled'] == 1
    assert state['_rest_pressure_retry_at'] > time.monotonic() + 9
    r.prepare(state, {'a', 'b'}, parallel=True)
    assert r.request(state, {'a', 'b'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(r.starts) == 34 and r.metrics()['requests'] == 2
    r.close()


def test_background_hour_budget_cannot_starve_real_recovery(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {}
    r.background_starts = deque([time.monotonic()] * 120)
    assert not r.request(state, {'a'}, None, warm=True)
    assert r.request(state, {'a'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert r.metrics()['categories']['warmup']['rolling_hour']['throttled'] == 1
    assert r.metrics()['categories']['recovery']['rolling_hour']['requests'] == 1
    r.close()


def test_warmup_uses_global_http_idle_time_across_states(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery()
    assert r.request({}, {'a'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert not r.request({}, {'b'}, None, warm=True)
    assert r.metrics()['categories']['warmup']['total']['deferred'] == 1
    assert r.metrics()['throttled'] == 0
    r.close()


def test_sliding_http_budget_exactly_forty_and_expired_credits_reusable(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {}
    r.starts.extend([time.monotonic()] * 39)
    assert r.request(state, {'a'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    denied = {}
    assert not r.request(denied, {'b'}, lambda *args: None)
    assert len(r.starts) == 40
    r.starts = deque([time.monotonic()-11] * 40)
    denied['rest_retry_at'] = 0
    assert r.request(denied, {'b'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert len(r.starts) == 1
    r.close()


def test_hour_metrics_are_classified_bounded_and_expire_without_traffic(monkeypatch):
    r = ws_recovery.SnapshotRecovery();clock = [10.1]
    monkeypatch.setattr(ws_recovery.time, 'monotonic', lambda: clock[0])
    for second in range(10, 7211):
        clock[0] = second + .1
        with r.lock:
            r.count('pressure', 'requests', clock[0])
            r.count('recovery', 'throttled', clock[0], 2)
    metrics = r.metrics()
    assert metrics['hour_buckets'] == 3600
    assert metrics['categories']['pressure']['total']['requests'] == 7201
    assert metrics['categories']['pressure']['rolling_hour']['requests'] == 3600
    assert metrics['categories']['recovery']['rolling_hour']['throttled'] == 7200
    clock[0] += 3601
    assert r.metrics()['hour_buckets'] == 0
    assert r.metrics()['categories']['pressure']['rolling_hour']['requests'] == 0
    r.close()


@pytest.mark.parametrize('stale', [False, True])
def test_pressure_requires_stale_business_and_only_once_per_backlog_episode(feed_manager, monkeypatch, stale):
    m, _, _ = feed_manager;make_hot(m);state = m.states[0]
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kw: calls.append(kw) or True)
    with m.lifecycle_lock:
        state['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=.6)
        if stale:
            for row in m.continuity.published.values():
                row['received'] -= timedelta(seconds=1)
        now = time.monotonic()
        for tick in range(50):
            m.background_snapshot_tick(now + tick)
        assert len(calls) == int(stale)
        state['inbox'].processing_received_at = None
        m.background_snapshot_tick(now + 51)
        assert not state.get('rest_pressure_episode')
        if stale:
            state['inbox'].processing_received_at = datetime.now(timezone.utc)-timedelta(seconds=.6)
            m.background_snapshot_tick(now + 52)
            assert len(calls) == 2
        state['inbox'].processing_received_at = None


@pytest.mark.parametrize('warm', [0, 60])
def test_default_warmup_off_and_outage_precedes_optional_background(feed_manager, monkeypatch, warm):
    m, _, _ = feed_manager;make_hot(m);state = m.states[0]
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REST_WARM_SECONDS', str(warm))
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kw: calls.append(kw) or True)
    state['rest_warm_at'] = 0
    state['outage_at'] = time.monotonic()
    m.background_snapshot_tick(time.monotonic())
    assert not calls
    del state['outage_at']
    m.background_snapshot_tick(time.monotonic())
    assert len(calls) == bool(warm)


def test_queued_repair_cannot_erase_ws_snapshot_or_make_ready_spare_unready(feed_manager, monkeypatch):
    m, p, sockets = feed_manager;c = ready_backup(m, sockets)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kw: calls.append(args) or True)
    with m.lifecycle_lock:
        c['recover_assets'] = set(c['assets'])
        c['_rest_missing_assets'] = set(c['assets'])
        before = dict(c['shadow'].books)
        live = dict(p.books)
        m.tick()
        assert c['shadow'].books == before and p.books == live
        assert c.get('ready') and m.backup_ready(c)
        assert not calls


def test_rejected_missing_snapshot_repair_is_retained_until_ws_arrives(feed_manager, monkeypatch):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    with m.lifecycle_lock, c['shadow'].lock:
        c['shadow'].books.pop('ay')
        c['shadow'].recorder.metadata.pop('ay')
        c['recover_assets'] = {'ay'}
        c['ready'] = False
        m.tick()  # Fixture refuses HTTP admission.
        assert c['recover_assets'] == {'ay'}
    sockets[1].emit(book('ay', '7'))
    wait_for(lambda: m.backup_ready(c))
    assert not c['recover_assets']


def test_unready_reason_distribution_integrates_time_and_overlaps():
    coverage = HotBackupCoverage({'btc'})
    coverage.at = 0
    coverage.update({'btc': {0}}, set(), 0, blockers={'btc': {'catchup', 'queue_wait'}})
    rows = coverage.update({'btc': {0}}, set(), 2, blockers={'btc': {'catchup'}})
    assert rows['btc']['unready_reason_seconds'] == {'catchup': 2, 'queue_wait': 2}
    rows = coverage.update({'btc': {0}}, {0}, 5, blockers={'btc': set()})
    assert rows['btc']['unready_reason_seconds'] == {'catchup': 5, 'queue_wait': 2}
    rows = coverage.update({'btc': {0}}, {0}, 10, blockers={'btc': set()})
    assert rows['btc']['unready_reason_seconds']['catchup'] == 5
    assert rows['btc']['unready_reason_entries'] == {'catchup': 1, 'queue_wait': 1}


def test_group_status_contains_missing_spare_and_catchup_reasons(feed_manager):
    m, _, sockets = feed_manager;make_hot(m)
    rows = m.update_hot_coverage(time.monotonic())
    assert rows['up_down:BTC:5m']['unready_reasons'] == ['no_backup']
    c = ready_backup(m, sockets)
    with m.lifecycle_lock:
        c['ready'] = False
        rows = m.update_hot_coverage(time.monotonic())
        assert 'catchup' in rows['up_down:BTC:5m']['unready_reasons']
        c['ready'] = True
        rows = m.update_hot_coverage(time.monotonic())
        assert not rows['up_down:BTC:5m']['unready_reasons']


def test_history_sustained_churn_keeps_byte_cap_and_expires_idle_anchors():
    history = BookHistory(byte_limit=256 * 1024)
    peaks = []
    for epoch in range(8):
        for i in range(1000):
            at = (epoch * 1000 + i) * 10
            event = book(str(i % 8), str(i + 1), at=at)
            history.record(event, at)
            assert 0 <= history.charged_bytes <= history.byte_limit
        peaks.append(history.metrics()['history_events'])
        history.prune(at)
    assert max(peaks) <= 1000 and history.byte_evictions > 0
    history.prune(at + 11000)
    assert history.charged_bytes == 0 and not history.assets


def test_burst_fan_in_keeps_spare_ready_without_rest_or_second_long_queue(feed_manager, monkeypatch):
    m, p, sockets = feed_manager;c = ready_backup(m, sockets)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kw: calls.append(kw) or True)
    # 500 frames/s on each physical feed, interleaved in 25-frame bursts.
    # This is a controlled local scheduling test, not a public-network SLA.
    peak = 0
    for burst in range(10):
        for i in range(25):
            event = json.dumps([book('ay', str(2+burst*25+i)), book('an', str(2+burst*25+i))])
            sockets[0].emit(event)
            sockets[1].emit(event)
        peak = max(peak, c['inbox'].pending_wait_seconds())
        threading.Event().wait(.05)
    wait_for(lambda: c['inbox'].queue.empty() and m.backup_ready(c))
    assert peak < 1 and c['inbox'].resyncs == 0 and not calls
    assert set(p.books) == {'ay', 'an'}


def test_empty_or_trade_frame_cannot_cancel_real_gap_repair(feed_manager):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    with m.lifecycle_lock, c['shadow'].lock:
        c['inbox'].delivery_gap = True
        c['ready'] = False
        c['recover_assets'] = set(c['assets'])
        m.redundant_receive(0, c, [], datetime.now(timezone.utc), c['inbox'])
        assert c['recover_assets'] == set(c['assets'])
        assert not m.backup_ready(c)


def test_failed_background_snapshot_has_classified_failure_and_does_not_block_fault(monkeypatch):
    def handler(assets):
        raise ws_recovery.requests.Timeout()
    http_pool(monkeypatch, handler)
    r = ws_recovery.SnapshotRecovery();state = {}
    assert r.request(state, {'a'}, None, pressure=True)
    wait_for(lambda: not r.metrics()['inflight'])
    assert r.request(state, {'a'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    categories = r.metrics()['categories']
    assert categories['pressure']['rolling_hour']['failures'] == 1
    assert categories['recovery']['rolling_hour']['failures'] == 1
    r.close()


def test_fresh_snapshot_clears_automatic_intent_without_weakening_later_explicit_repair(feed_manager):
    m, _, sockets = feed_manager;c = ready_backup(m, sockets)
    with m.lifecycle_lock:
        c['recover_assets'] = {'ay'}
        c['_rest_missing_assets'] = {'ay'}
        received = datetime.now(timezone.utc)
        m.redundant_receive(0, c, book('ay', '7'), received, c['inbox'])
        assert not c['recover_assets'] and not c['_rest_missing_assets']
        c['recover_assets'] = {'ay'}
        m.tick()
        assert not c['ready'] and 'ay' not in c['shadow'].books


def test_hour_of_healthy_ticks_adds_no_background_http_traffic(feed_manager, monkeypatch):
    m, _, _ = feed_manager;make_hot(m)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args, **kw: calls.append(kw) or True)
    now = time.monotonic()
    with m.lifecycle_lock:
        for tick in range(18000):
            m.background_snapshot_tick(now + tick * .2)
    assert not calls and m.snapshot_recovery.metrics()['throttled'] == 0


def test_parallel_warmup_must_fit_entire_remaining_hour_budget(monkeypatch):
    http_pool(monkeypatch, lambda assets: [book(a) for a in assets])
    r = ws_recovery.SnapshotRecovery();state = {}
    r.prepare(state, {'a', 'b'}, parallel=True)
    r.background_starts = deque([time.monotonic()] * 119)
    assert not r.request(state, {'a', 'b'}, None, warm=True)
    assert r.metrics()['requests'] == 0 and len(r.background_starts) == 119
    assert r.request(state, {'a', 'b'}, lambda *args: None)
    wait_for(lambda: not r.metrics()['inflight'])
    assert r.metrics()['categories']['recovery']['total']['requests'] == 2
    r.close()

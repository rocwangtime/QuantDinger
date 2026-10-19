"""Fix A: status/metrics aggregation off the hot locks, O(1) rolling hour.

Control semantics (failover, merge, REST, heartbeat, close codes) are not
touched; these tests pin the observability contracts that changed.
"""
import random
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from app.services.polymarket import observer, recording, ws_recovery
from tests.test_polymarket_ws_rotation import feed_manager  # noqa: F401  (fixture)


def test_incremental_rolling_hour_equals_full_resum_through_expiry(monkeypatch):
    r = ws_recovery.SnapshotRecovery()
    clock = [100.0]
    monkeypatch.setattr(ws_recovery.time, 'monotonic', lambda: clock[0])
    rng = random.Random(7)
    kinds, fields = ('recovery', 'warmup', 'pressure'), ('attempts', 'requests', 'completed',
                                                         'failures', 'throttled', 'deferred')
    for step in range(9000):
        clock[0] += rng.choice((0, .3, 1, 1, 2, 17))
        with r.lock:
            r.count(rng.choice(kinds), rng.choice(fields), clock[0], rng.randint(1, 3))
        if step % 997 == 0:
            m = r.metrics()
            full = sum(r.hour.values(), Counter())
            for kind in kinds:
                for field in fields:
                    assert m['categories'][kind]['rolling_hour'][field] == full[kind, field]
            assert all(v > 0 for v in r.hour_totals.values())
    clock[0] += 3601
    m = r.metrics()
    assert m['hour_buckets'] == 0 and not r.hour_totals
    assert all(v == 0 for c in m['categories'].values() for v in c['rolling_hour'].values())
    assert sum(sum(c['total'].values()) for c in m['categories'].values()) > 0
    r.close()


def test_metrics_cost_is_independent_of_retained_buckets():
    r = ws_recovery.SnapshotRecovery()
    base = time.monotonic()
    for k in range(3599, 0, -1):
        r.count('recovery', 'requests', base - k)
    began = time.perf_counter()
    for _ in range(200):
        r.metrics()
    # The re-sum took ~4ms/call at 3600 buckets on the dev box.
    assert (time.perf_counter() - began) / 200 < .001
    r.close()


def test_supervisor_tick_rebuilds_status_only_when_stale(feed_manager):  # noqa: F811
    m, p, sockets = feed_manager
    builds = []
    original = m.publish_status
    m.publish_status = lambda: (builds.append(time.monotonic()), original())
    m.tick()
    assert len(builds) == 1  # explicit callers (market loop, tests) always rebuild
    for _ in range(5):
        m.tick(status=False)
    assert len(builds) == 1
    m.status_at -= m.STATUS_REFRESH_SECONDS
    m.tick(status=False)
    assert len(builds) == 2
    assert observer.STATUS['ws_shards'] == m.count and 'ws_rest_recovery' in observer.STATUS


def test_supervisor_thread_still_refreshes_status(feed_manager):  # noqa: F811
    m, p, sockets = feed_manager
    m.status_at = -float('inf')
    observer.STATUS.pop('ws_backups', None)
    m.start_supervisor()
    deadline = time.monotonic() + 3
    while 'ws_backups' not in observer.STATUS and time.monotonic() < deadline:
        time.sleep(.02)
    assert 'ws_backups' in observer.STATUS


def test_pipeline_metrics_sort_outside_publication_lock(feed_manager, monkeypatch):  # noqa: F811
    m, p, sockets = feed_manager
    seen = []
    original = recording.Recorder.metrics
    def metrics(self, *args, **kwargs):
        seen.append(p.lock._is_owned())
        return original(self, *args, **kwargs)
    monkeypatch.setattr(recording.Recorder, 'metrics', metrics)
    result = p.metrics()
    assert seen == [False]
    assert 'book_freshness_by_activity' in result and 'ws_delivery_latency_by_activity' in result


def test_metrics_view_matches_live_metrics():
    r = recording.Recorder()
    now = datetime.now(timezone.utc)
    r.metadata.update({'a': (now - timedelta(seconds=1), now, 'ws'), 'b': (now, now, 'ws'), 'c': (None, now, 'ws')})
    for i in range(50):
        r.delivery_samples.append((time.monotonic(), 'a', float(i), float(i + 5)))
    books = {'a': {}, 'b': {}}
    groups = {'a': 'g1', 'b': 'g2'}
    live = r.metrics(books, groups, {'g1': 200, 'g2': 1}, 100)
    viewed = r.metrics(books, groups, {'g1': 200, 'g2': 1}, 100, view=r.metrics_view(books))
    for key in ('ws_delivery_latency_by_activity', 'book_freshness_by_activity', 'book_age_tokens'):
        assert type(live[key]) is type(viewed[key])
    assert live['ws_delivery_latency_by_activity'] == viewed['ws_delivery_latency_by_activity']
    assert live['book_age_tokens'] == viewed['book_age_tokens'] == 2


def test_publish_latency_includes_application_queue_wait():
    r = recording.Recorder()
    market = dict(id='m', yesAssetId='a', noAssetId='b')
    now = datetime.now(timezone.utc)
    received = now - timedelta(seconds=8)          # sat 8s in the local receive queue
    exchange = received - timedelta(milliseconds=120)
    event = dict(event_type='price_change', timestamp=int(exchange.timestamp() * 1000),
                 price_changes=[dict(asset_id='a', side='BUY', price='.5', size='1')])
    r.receive(event, ['a'], {'m': market}, {'a': 'm'}, received)
    row = r.metrics({'a': {}}, {'a': 'g'}, {'g': 500}, 100)['ws_delivery_latency_by_activity']['hot']
    assert 100 <= row['p95_ms'] <= 140                 # unchanged meaning: exchange -> socket receipt
    assert 8000 <= row['publish_p95_ms'] <= 9000       # exchange -> merged publication
    fresh = r.metrics({'a': {}}, {'a': 'g'}, {'g': 500}, 100)['book_freshness_by_activity']['hot']
    assert fresh['delivery_latency_p95_ms'] == row['p95_ms']
    assert fresh['publish_latency_p95_ms'] == row['publish_p95_ms']


def test_legacy_three_field_samples_still_report():
    r = recording.Recorder()
    r.delivery_samples.append((time.monotonic(), 'a', 12.0))
    row = r.metrics({'a': {}}, {'a': 'g'}, {'g': 500}, 100)['ws_delivery_latency_by_activity']['hot']
    assert row['p95_ms'] == 12.0 and row['publish_p95_ms'] is None


def test_concurrent_count_and_metrics_stay_consistent():
    r = ws_recovery.SnapshotRecovery()
    stop = threading.Event()
    def writer():
        while not stop.is_set():
            with r.lock:
                r.count('recovery', 'attempts', time.monotonic())
    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(200):
            m = r.metrics()
            assert m['categories']['recovery']['rolling_hour']['attempts'] <= m['categories']['recovery']['total']['attempts']
    finally:
        stop.set()
        t.join()
    with r.lock:
        assert r.hour_totals == sum(r.hour.values(), Counter())
    r.close()

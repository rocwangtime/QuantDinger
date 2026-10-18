"""Make-before-break publication, isolation, recovery and bounded monitoring."""
import importlib.util
import io
import json
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, ws_rotation


def wait_for(predicate, seconds=2):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            assert predicate(), 'condition not met before deadline'
            return
        threading.Event().wait(.005)


def book(asset, size='1', at=None):
    return dict(event_type='book', asset_id=asset,
                timestamp=at or int(datetime.now(timezone.utc).timestamp() * 1000),
                bids=[], asks=[dict(price='.4', size=size)])


@pytest.fixture
def feed_manager(monkeypatch):
    # Publication contracts below use immediate warmup; admission/soak are
    # covered separately with the production defaults.
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ROTATE_WARM_SECONDS', '0')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ROTATE_SECONDS', '2100')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED', '1')
    # Historical two-spare contracts remain covered with the explicit A/B option.
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '2')
    # Individual admission tests cover the production bootstrap phase.
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS', '0')
    sockets = []
    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks, self.sent = callbacks, []
            self.stopped, self.opened = threading.Event(), threading.Event()
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            self.opened.set()
            self.stopped.wait(30)
            self.callbacks['on_close'](self, None, None)
        def send(self, raw):
            self.sent.append(raw)
        def close(self):
            self.stopped.set()
        def emit(self, event):
            raw = event if isinstance(event, str) else json.dumps(event)
            self.callbacks['on_message'](self, raw)
    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, 'STATUS', dict(reconnect_count=0, slow_consumer_count=0))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    market = dict(id='a', yesAssetId='ay', noAssetId='an')
    p = observer.Pipeline()
    p.set_markets([market])
    monkeypatch.setattr(observer, '_PIPELINE', p)
    m = observer.MarketConnections()
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *args: None)
    m.sync([market])
    assert sockets[0].opened.wait(1)
    sockets[0].emit([book('ay'), book('an')])
    wait_for(lambda: len(p.books) == 2)
    try:
        yield m, p, sockets
    finally:
        m.close()
        assert all(s.stopped.is_set() for s in sockets)
        assert all(not s.get(key) or not s[key].is_alive()
                   for s in list(m.states.values()) + m.retiring for key in ('thread', 'worker'))


def prepare(m, sockets):
    old = m.states[0]
    old['opened_at'] -= 2101
    m.tick()
    assert len(sockets) == 2
    assert sockets[1].opened.wait(1)
    m.subscriptions.next_at = 0
    m.tick()  # Admit the replacement's first batch before testing publication.
    return old, m.standby[1]


def test_rotation_keeps_old_until_complete_and_rejects_late_callbacks(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    old_book = p.books['ay']
    sockets[1].emit(book('ay', '2'))
    wait_for(lambda: 'ay' in standby['shadow'].books)
    assert m.states[0] is old and p.books['ay'] is old_book
    assert not sockets[0].stopped.is_set()
    # Old feed continues to publish while shadow warms; timestamps must catch up.
    sockets[0].emit(book('ay', '3'))
    wait_for(lambda: p.books['ay']['asks']['.4'] == '3')
    sockets[1].emit([book('ay', '4'), book('an', '4')])
    wait_for(lambda: m.states[0] is standby)
    assert sockets[0].stopped.wait(1)
    assert p.books['ay']['asks']['.4'] == p.books['an']['asks']['.4'] == '4'
    assert m.rotations == 1 and observer.STATUS['reconnect_count'] == 0
    # An old worker/close callback must never invalidate the promoted generation.
    old['worker'].join(1)
    sockets[0].callbacks['on_error'](sockets[0], RuntimeError('late old failure'))
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow_consumer')
    sockets[0].emit(book('ay', '99'))
    assert standby['connected'] and len(p.books) == 2
    assert observer.STATUS['slow_consumer_count'] == 0
    sockets[1].emit(dict(event_type='price_change', timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
                         price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='5')]))
    wait_for(lambda: p.books['ay']['asks']['.4'] == '5')


def test_stale_shadow_cannot_roll_back_live_book(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    sockets[1].emit([book('ay', '2', 1780000000000), book('an', '2', 1780000000000)])
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old and p.books['ay']['asks']['.4'] == '1'
    standby['created_at'] -= 91
    m.tick()
    assert m.standby is None and m.rotation_failures == 1
    assert not sockets[0].stopped.is_set() and old['connected']


def test_missing_snapshot_times_out_even_with_deltas_and_trades(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    sockets[1].emit([book('ay', '2'), dict(event_type='price_change', timestamp=1780000000000,
        price_changes=[dict(asset_id='an', side='SELL', price='.4', size='99')]),
        dict(event_type='last_trade_price', asset_id='ay', price='.4', size='1', side='BUY')])
    wait_for(lambda: 'ay' in standby['shadow'].books)
    assert 'an' not in standby['shadow'].books
    assert not p.recording.pending and not standby['shadow'].recorder.pending
    standby['created_at'] -= 91
    m.tick()
    assert m.standby is None and m.states[0] is old
    sockets[0].emit(book('an', '3'))
    wait_for(lambda: p.books['an']['asks']['.4'] == '3')


def test_each_shadow_timestamp_must_catch_up_to_latest_live_book(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    at = int(datetime.now(timezone.utc).timestamp() * 1000)
    sockets[0].emit([book('ay', '3', at), book('an', '3', at)])
    wait_for(lambda: p.books['an']['asks']['.4'] == '3')
    sockets[1].emit([book('ay', '4', at), book('an', '4', at - 1)])
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old
    sockets[0].emit(book('ay', '5', at + 1))
    wait_for(lambda: p.books['ay']['asks']['.4'] == '5')
    sockets[1].emit(book('an', '4', at))
    wait_for(lambda: standby['shadow'].recorder.metadata['an'][0].timestamp() * 1000 == at)
    assert m.states[0] is old
    sockets[1].emit(book('ay', '6', at + 1))
    wait_for(lambda: m.states[0] is standby)
    assert p.books['ay']['asks']['.4'] == '6'


def test_failed_shadow_and_assignment_change_preserve_live_feed(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    sockets[1].callbacks['on_error'](sockets[1], OSError('standby failure'))
    m.tick()
    assert m.standby is None and old['connected'] and len(p.books) == 2
    assert not sockets[0].stopped.is_set()
    old['rotate_retry_at'] = 0
    m.tick()
    assert m.standby
    m.apply_assignment(dict(ay=0))
    assert not m.standby and not sockets[0].stopped.is_set()
    assert m.states[0] is old


def test_standby_slow_consumer_during_soak_preserves_active(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ROTATE_WARM_SECONDS', '30')
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old and m.rotations == 0
    sockets[1].callbacks['on_error'](sockets[1], RuntimeError('slow consumer: send buffer full'))
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer: send buffer full')
    m.tick()
    assert m.standby is None and m.rotation_failures == 1 and m.standby_slow_consumers == 1
    assert m.states[0] is old and old['connected'] and not sockets[0].stopped.is_set()
    assert p.books['ay']['asks']['.4'] == '1'


def test_idle_shadow_promotes_after_soak_on_worker_without_another_data_frame(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ROTATE_WARM_SECONDS', '30')
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old
    standby['warm_since'] -= 31
    wait_for(lambda: m.states[0] is standby)
    assert p.books['ay']['asks']['.4'] == '2' and sockets[0].stopped.is_set()


def test_overflowing_shadow_never_invalidates_active_books(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    inbox = standby['inbox']
    inbox.queue.maxsize = inbox.ceiling = 1
    entered, release = threading.Event(), threading.Event()
    original = m.shadow_receive
    def blocked(*args):
        entered.set()
        assert release.wait(2)
        original(*args)
    monkeypatch.setattr(m, 'shadow_receive', blocked)
    sockets[1].emit(book('ay', '2'))
    assert entered.wait(1)
    try:
        sockets[1].emit(book('an', '2'))
        sockets[1].emit(book('ay', '3'))
        assert inbox.dropped == 1 and inbox.resync.is_set()
    finally:
        release.set()
    wait_for(lambda: standby['inbox'].invalid.is_set())
    m.tick()
    assert m.standby is None and m.states[0] is old and len(p.books) == 2


def test_subscription_refresh_cancels_shadow_and_late_callbacks(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    markets = m.markets + [dict(id='b', yesAssetId='by', noAssetId='bn')]
    p.set_markets(markets)
    m.sync(markets)
    assert m.standby is None and m.states[0] is old and standby['retired']
    sockets[1].emit([book('ay', '99'), book('an', '99')])
    sockets[1].callbacks['on_error'](sockets[1], OSError('late standby error'))
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow_consumer')
    assert old['connected'] and m.rotations == 0 and len(p.books) == 2
    assert observer.STATUS['slow_consumer_count'] == 0
    m.subscriptions.next_at = 0
    m.tick()
    sockets[0].emit([book('by'), book('bn')])
    wait_for(lambda: len(p.books) == 4)


def test_reconnect_generation_ignores_all_old_callbacks(feed_manager):
    m, p, sockets = feed_manager
    state = m.states[0]
    sockets[0].close()
    state['thread'].join(1)
    state['retry_at'] = 0  # Generation isolation is independent of jitter delay.
    m.tick()
    assert sockets[1].opened.wait(1)
    m.subscriptions.next_at = 0
    m.tick()
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: len(p.books) == 2 and p.books['ay']['asks']['.4'] == '2')
    assert state['retired'] and m.states[0] is not state
    state = m.states[0]
    health = state['data_health']
    before = health.total
    sockets[0].emit(book('ay', '99'))
    sockets[0].callbacks['on_error'](sockets[0], RuntimeError('slow_consumer'))
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow_consumer')
    sockets[0].callbacks['on_open'](sockets[0])
    assert health.total == before and state['connected'] and not state['inbox'].invalid.is_set()
    assert p.books['ay']['asks']['.4'] == '2'
    assert observer.STATUS['slow_consumer_count'] == 0


def test_old_feed_failure_during_prepare_can_promote_ready_shadow(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    sockets[0].close()
    old['thread'].join(1)
    m.tick()
    assert len(sockets) == 2  # No third connection alongside the warming one.
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: m.states[0] is standby)
    assert standby['connected'] and len(p.books) == 2


def test_failed_active_feed_keeps_latest_watermarks_for_shadow(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + 1
    sockets[0].emit([book('ay', '3', at), book('an', '3', at)])
    wait_for(lambda: p.books['an']['asks']['.4'] == '3')
    sockets[0].close()
    old['thread'].join(1)
    old['worker'].join(1)
    assert not p.books and not p.recording.metadata
    sockets[1].emit([book('ay', '2', at - 1), book('an', '2', at - 1)])
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old and m.rotations == 0
    sockets[1].emit([book('ay', '4', at), book('an', '4', at)])
    wait_for(lambda: m.states[0] is standby)
    assert p.books['ay']['asks']['.4'] == '4'


def test_shadow_without_exchange_timestamp_never_promotes(feed_manager):
    m, _, sockets = feed_manager
    old, standby = prepare(m, sockets)
    snapshots = [book('ay'), book('an')]
    for snapshot in snapshots:
        snapshot.pop('timestamp')
    sockets[1].emit(snapshots)
    wait_for(lambda: len(standby['shadow'].books) == 2)
    assert m.states[0] is old and m.standby and m.rotations == 0


def test_shadow_waiting_on_publication_lock_cannot_promote_stale_delivery(feed_manager):
    m, p, sockets = feed_manager
    old, standby = prepare(m, sockets)
    m.shadow_receive(0, standby, [book('ay', '2'), book('an', '2')],
                     datetime.now(timezone.utc) - timedelta(seconds=2), standby['inbox'])
    assert m.states[0] is old and p.books['ay']['asks']['.4'] == '1'


def test_overflow_cannot_interleave_with_shadow_publication(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    _, standby = prepare(m, sockets)
    inbox = standby['inbox']
    attempted, finished = threading.Event(), threading.Event()
    original = p.receive
    observed = []
    def overflow():
        attempted.set()
        with inbox.lock:
            inbox.resync.set()
        finished.set()
    producer = threading.Thread(target=overflow)
    def publish(*args, **kwargs):
        if not observed:
            producer.start()
            assert attempted.wait(1)
        assert not finished.wait(.02)
        original(*args, **kwargs)
        observed.append(args[0]['asset_id'])
    monkeypatch.setattr(p, 'receive', publish)
    try:
        m.shadow_receive(0, standby, [book('ay', '2'), book('an', '2')],
                         datetime.now(timezone.utc), inbox)
    finally:
        producer.join(1)
    assert finished.is_set() and set(observed) == {'ay', 'an'}
    assert m.states[0] is standby and m.rotations == 1


def test_supervisor_rotates_without_market_tick(feed_manager):
    m, p, sockets = feed_manager
    m.states[0]['opened_at'] -= 2101
    m.start_supervisor()
    wait_for(lambda: len(sockets) == 2)
    assert sockets[1].opened.wait(1)
    sockets[1].emit([book('ay', '2'), book('an', '2')])
    wait_for(lambda: m.rotations == 1)
    assert len(p.books) == 2


@pytest.mark.parametrize('established', [False, True])
def test_supervisor_reconnect_uses_full_jitter_and_resets_after_healthy_lifetime(feed_manager, established, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.setattr(ws_rotation.random, 'uniform', lambda low, high: high / 2)
    if established:
        m.states[0]['opened_at'] -= 61
        m.states[0]['attempts'] = 7
    m.start_supervisor()
    sockets[0].close()
    wait_for(lambda: len(sockets) == 2, .9)
    assert sockets[1].opened.wait(1)
    sockets[1].close()  # Second consecutive short-lived attempt backs off.
    wait_for(lambda: not m.states[0]['thread'].is_alive())
    remaining = m.states[0]['retry_at'] - time.monotonic()
    assert 0 < remaining <= 1.2
    assert [ws_rotation.reconnect_delay(i) for i in range(8)] == [.25, .25, .5, 1, 2, 4, 8, 15]
    assert ws_rotation.reconnect_delay(999) == 15


def test_data_collapse_requires_busy_baseline_consecutive_seconds_and_healthy_pong():
    health = ws_rotation.DataHealth(0)
    pong = dict(pings=1, pongs=1, pong_age_seconds=1)
    for now in range(1, 121):
        health.total += 100000
        assert not health.sample(now, pong, wall_time=100)
    for now in range(121, 140):
        health.total += 100
        assert not health.sample(now, pong, wall_time=100)
    assert health.sample(140, pong, wall_time=100)
    health.total += 100000
    assert not health.sample(141, pong, wall_time=100)
    assert not health.sample(147, pong, wall_time=100)  # Scheduling gap resets consecutiveness.
    for now in range(148, 170):
        assert not health.sample(now, dict(pong, pong_age_seconds=30), wall_time=100)
    cold = ws_rotation.DataHealth(0)
    for now in range(1, 200):
        assert not cold.sample(now, pong, wall_time=100)


def test_control_pong_does_not_count_as_data(feed_manager):
    m, _, sockets = feed_manager
    health = m.states[0]['data_health']
    total = health.total
    sockets[0].emit('PONG')
    assert health.total == total


def test_freshness_p95_by_activity_includes_missing_and_unknown(monkeypatch):
    p = observer.Pipeline()
    markets = [dict(id=k, yesAssetId=k+'y', noAssetId=k+'n') for k in ['hot', 'cold', 'new']]
    p.set_markets(markets)
    monkeypatch.setattr(observer, 'STATUS', dict(ws_group_rates={'market:hot': 200, 'market:cold': 1}))
    received = datetime.now(timezone.utc)
    for key, age in [('hot', 1), ('cold', 100)]:
        for side in ['y', 'n']:
            p.receive(book(key+side, at=int((received - timedelta(seconds=age)).timestamp() * 1000)),
                      received_at=received)
    groups = p.metrics()['book_freshness_by_activity']
    assert 1000 <= groups['hot']['book_age_p95_ms'] < 2000
    assert 100000 <= groups['cold']['book_age_p95_ms'] < 101000
    assert groups['hot']['tokens'] == groups['hot']['measured_tokens'] == 2
    assert groups['unmeasured']['missing_tokens'] == 2
    assert groups['unmeasured']['book_age_p95_ms'] is None


def monitor_module():
    spec = importlib.util.spec_from_file_location('p34_monitor', Path(__file__).resolve().parents[2] / 'deploy/p34_monitor.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resident_monitor_log_rotation_is_strictly_bounded(tmp_path):
    monitor = monitor_module()
    path = tmp_path / 'tcp.log'
    with monitor.RotatingLog(path, 1024, 2) as log:
        log.write('x' * 10000)
        log.write('end')
    files = list(tmp_path.iterdir())
    assert len(files) == 3 and all(f.stat().st_size <= 1024 for f in files)
    assert path.read_text().endswith('end')


def test_sampler_ignores_pipe_holders_after_parent_exit_and_is_resident(tmp_path):
    monitor = monitor_module()
    script = tmp_path / 'sample.sh'
    marker = tmp_path / 'leak'
    script.write_text(f"(sleep .4; touch '{marker}') &\necho sample\nexit 0\n")
    output, metadata = io.StringIO(), io.StringIO()
    began = time.monotonic()
    assert monitor.run_sample(['bash'], script, output, .2, metadata) == 0
    assert time.monotonic() - began < .4 and 'sample' in output.getvalue()
    threading.Event().wait(.5)
    assert not marker.exists()
    stop = threading.Event()
    thread = threading.Thread(target=monitor.lane, args=('tcp', ['bash'], script, tmp_path, .02, .2, 0, stop, 1024, 2))
    thread.start()
    wait_for(lambda: (tmp_path / 'tcp.log').exists() and (tmp_path / 'tcp.log').stat().st_size > 0)
    stop.set()
    thread.join(1)
    assert not thread.is_alive()
    assert 'done' in (tmp_path / 'tcp_heartbeat.jsonl').read_text()

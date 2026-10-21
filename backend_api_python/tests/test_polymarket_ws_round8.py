"""Eager paced hot redundancy, replenishment and time-based coverage."""
import json
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_redundancy import HotBackupCoverage
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup


@pytest.mark.parametrize('key', sorted(observer.HOT_SERIES))
def test_first_hot_backup_is_eager_and_ready_before_42s(feed_manager, key):
    m, _, sockets = feed_manager
    old = m.states[0]
    m.balancer.set_groups({a: key for a in old['assets']})
    m.tick()
    assert sockets[1].opened.wait(1)
    candidate = m.backups[0]
    assert 0 <= candidate['opened_at'] - old['opened_at'] < 2
    assert json.loads(sockets[1].sent[0]) == dict(type='market', assets_ids=old['assets'][::-1])
    sockets[1].emit([book(a) for a in old['assets']])
    wait_for(lambda: candidate.get('ready'))
    m.tick()
    row = observer.snapshot()['ws_hot_backup_by_group'][key]
    assert row['active'] and row['ready'] and row['required_shards'] == [0]
    assert row['time_without_backup_seconds'] < 2
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is candidate, .5)
    # Replenish even a young survivor as soon as the global dial slot clears.
    m.backup_next_connect_at = 0
    m.tick()
    assert sockets[2].opened.wait(1)
    assert m.backups[0]['opened_at'] - candidate['opened_at'] < 2


def test_all_four_hot_groups_start_with_paced_initial_subscriptions(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED')
    # Make the first batches tiny so large shards also exercise shared batches.
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SUBSCRIBE_INITIAL_ASSETS', '2')
    p.recording.enabled = False
    markets = []
    for i, key in enumerate(sorted(observer.HOT_SERIES)):
        _, asset, interval = key.split(':')
        for window in range(2):
            markets.append(dict(id=f'{i}-{window}', market_type='up_down', asset=asset, interval=interval,
                                yesAssetId=f'{i}-{window}-y', noAssetId=f'{i}-{window}-n'))
    p.set_markets(markets)
    m.sync(markets)
    m.start_supervisor()
    wait_for(lambda: len(m.backups) == 4 and all(s.get('opened_at') for s in m.backups.values()), 8)
    with m.lifecycle_lock:
        candidates = list(m.backups.values())
    opened = sorted(s['opened_at'] for s in candidates)
    assert all(b - a >= .49 for a, b in zip(opened, opened[1:]))
    wait_for(lambda: all(not s.get('subscribe_pending') for s in candidates), 8)
    for s in candidates:
        commands = [json.loads(raw) for raw in s['socket'].sent if raw != 'PING']
        assert commands[0]['type'] == 'market' and len(commands[0]['assets_ids']) == 2
        assert all(len(c['assets_ids']) <= 2 for c in commands)
        assert {a for c in commands for a in c['assets_ids']} == set(s['assets'])
        events = [book(a) for a in s['assets']]
        # This test measures paced admission on healthy primaries. Subscription
        # pacing can exceed the half-open detector's four-second threshold.
        m.states[s['primary_index']]['socket'].emit(events)
        s['socket'].emit(events)
    wait_for(lambda: all(s.get('ready') for s in candidates))
    wait_for(lambda: all(row['ready'] for row in observer.STATUS['ws_hot_backup_by_group'].values()))


@pytest.mark.parametrize('failure', ['1013', 'error', '1001', 'dead_reader', 'dead_worker', 'handshake_timeout'])
def test_failed_backup_replenishes_without_waiting_for_discovery(feed_manager, monkeypatch, failure):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    old = m.states[0]
    books = dict(p.books)
    monkeypatch.setattr(observer.random, 'uniform', lambda low, high: high)
    # Expire only the previous global dial slot, not the primary age guard.
    candidate['opened_at'] -= 2
    candidate['initial_sent_at'] -= 2
    m.backup_next_connect_at = 0
    if failure in ('1013', '1001'):
        code = int(failure)
        sockets[1].callbacks['on_close'](sockets[1], code, 'slow consumer' if code == 1013 else 'server shutting down')
    elif failure == 'error':
        sockets[1].callbacks['on_error'](sockets[1], OSError('wire failure'))
    elif failure.startswith('dead_'):
        key = 'thread' if failure == 'dead_reader' else 'worker'
        monkeypatch.setitem(candidate, key, SimpleNamespace(is_alive=lambda: False))
    else:
        candidate.pop('opened_at')
        candidate.pop('initial_sent_at')
        candidate['connected'] = False
        candidate['created_at'] -= 16
    m.tick()
    assert candidate['retired'] and not m.backups
    assert 0 < old['backup_retry_at'] - time.monotonic() <= 3.3
    assert old['connected'] and p.books == books
    # Production supervisor polls at 200ms; no discovery/sync call is needed.
    m.start_supervisor()
    wait_for(lambda: len(sockets) == 3, 4)
    assert sockets[2].opened.wait(1)
    replacement = m.backups[0]
    assert replacement['opened_at'] - old['opened_at'] >= 90
    sockets[2].emit([book(a) for a in old['assets']])
    wait_for(lambda: replacement.get('ready'))
    assert old['backup_failures'] == 0
    if failure == '1001':
        assert m.rotation_failures == 0
    else:
        assert m.rotation_failures == 1


def test_young_primary_replenishes_without_age_guard(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    old = m.states[0]
    m.balancer.set_groups({a: 'up_down:BTC:5m' for a in old['assets']})
    m.tick()
    assert sockets[1].opened.wait(1)
    monkeypatch.setattr(observer.random, 'uniform', lambda low, high: high)
    sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer')
    m.tick()
    old['backup_retry_at'] = 0
    old['rotate_retry_at'] = time.monotonic() + 480
    m.backup_next_connect_at = 0
    m.tick()
    assert sockets[2].opened.wait(1)
    assert m.backups[0]['opened_at'] - old['opened_at'] < 2


@pytest.mark.parametrize('failure', ['handshake_error', 'return_without_callback', 'handshake_hangs'])
def test_backup_never_opened_is_retired_and_retried(feed_manager, monkeypatch, failure):
    m, _, sockets = feed_manager
    old = m.states[0]
    old['opened_at'] -= 121
    m.balancer.set_groups({a: 'up_down:ETH:15m' for a in old['assets']})
    app_class = sys.modules['websocket'].WebSocketApp
    original_run = app_class.run_forever
    attempted = threading.Event()
    def cannot_open(self, **kwargs):
        if failure == 'handshake_error':
            self.callbacks['on_error'](self, OSError('public socket handshake failed'))
        attempted.set()
        if failure == 'handshake_hangs':
            self.stopped.wait(3)
    monkeypatch.setattr(app_class, 'run_forever', cannot_open)
    m.tick()
    assert attempted.wait(1)
    candidate = m.backups[0]
    if failure == 'handshake_hangs':
        candidate['created_at'] -= 16
    else:
        candidate['thread'].join(1)
    m.tick()
    assert candidate['retired'] and not m.backups and old['connected']
    assert old['backup_failures'] == 1
    monkeypatch.setattr(app_class, 'run_forever', original_run)
    m.start_supervisor()
    wait_for(lambda: len(sockets) == 3, 4)
    assert sockets[2].opened.wait(1)


def test_hot_repair_jitter_staggers_backups_and_caps_at_thirty_seconds(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    old = m.states[0]
    monkeypatch.setattr(observer.random, 'uniform', lambda low, high: high)
    for expected in (2.7, 3.2, 4.2, 6.2, 10.2, 18.2, 30, 30):
        m.rotation_failed(0, old, candidate, 'connect_failure')
        assert m.rotation_last_failure['retry_seconds'] == expected
    monkeypatch.setattr(observer.random, 'uniform', lambda low, high: low)
    m.rotation_failed(0, old, candidate, 'connect_failure')
    assert m.rotation_last_failure['retry_seconds'] == 2.2


def test_coverage_accounts_duration_split_groups_rollover_and_inactive_periods():
    key = 'up_down:BTC:5m'
    coverage = HotBackupCoverage({key, 'up_down:ETH:5m'})
    coverage.at = 0
    coverage.update({key: {0, 1}}, set(), 0)
    coverage.update({key: {0, 1}}, {0}, 5)
    coverage.update({key: {0, 1}}, {0, 1}, 10)
    row = coverage.update({key: {0, 1}}, {0, 1}, 40)[key]
    assert row['hot_backup_ready_ratio'] == .75
    assert row['time_without_backup_seconds'] == 10
    assert row['current_time_without_backup_seconds'] == 0
    coverage.unready(1, 40)  # A close callback updates even between status ticks.
    row = coverage.update({key: {0, 1}}, {0}, 50)[key]
    assert row['hot_backup_ready_ratio'] == .6
    assert row['time_without_backup_seconds'] == 20
    assert row['current_time_without_backup_seconds'] == 10
    coverage.update({}, set(), 60)
    row = coverage.update({}, set(), 100)[key]
    assert row['observed_seconds'] == 60 and row['time_without_backup_seconds'] == 30
    # Stable series identity retains cumulative history across window changes.
    rows = coverage.update({key: {2}}, {2}, 100)
    assert rows[key]['hot_backup_ready_ratio'] == .5
    assert rows['up_down:ETH:5m']['hot_backup_ready_ratio'] is None


def test_snapshot_recovery_does_not_claim_ready_from_old_shadow(feed_manager):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    candidate['recover_assets'] = {'ay'}
    m.tick()
    assert not candidate['ready'] and 'ay' not in candidate['shadow'].books
    m.redundant_receive(0, candidate, [], datetime.now(timezone.utc), candidate['inbox'])
    assert not candidate['ready']
    m.subscriptions.next_at = 0
    m.tick()
    sockets[1].emit(book('ay'))
    wait_for(lambda: candidate.get('ready'))


def test_backup_unsubscribe_failure_does_not_break_rollover_or_main(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    old = m.states[0]
    p.recording.enabled = False
    market = dict(id='b', market_type='up_down', asset='BTC', interval='5m', yesAssetId='by', noAssetId='bn')
    p.set_markets([market])
    def broken_send(raw):
        raise OSError('backup wire closed during rollover')
    monkeypatch.setattr(sockets[1], 'send', broken_send)
    m.sync([market])
    assert candidate['retired'] and old['connected'] and not m.backups
    assert m.rotation_last_failure['reason'] == 'backup_wire_failure'
    m.start_supervisor()
    wait_for(lambda: len(sockets) == 3, 4)
    assert sockets[2].opened.wait(1)
    assert set(m.backups[0]['assets']) == {'by', 'bn'}


def test_status_cold_reports_both_p95s_and_unknown_without_samples(feed_manager):
    _, p, _ = feed_manager
    now = datetime.now(timezone.utc)
    p.recording.metadata.clear()
    p.recording.delivery_samples.clear()
    p.receive(book('ay', at=int((now - timedelta(seconds=180)).timestamp() * 1000)),
              received_at=now - timedelta(seconds=179.8))
    p.receive(book('an', at=int((now - timedelta(milliseconds=200)).timestamp() * 1000)), received_at=now)
    observer.STATUS['ws_group_rates'] = {'market:a': 1}
    report = observer.snapshot()
    cold = report['book_freshness_by_activity']['cold']
    assert 199 <= cold['delivery_latency_p95_ms'] < 202
    assert cold['delivery_latency_p95_ms'] == report['ws_delivery_latency_by_activity']['cold']['p95_ms']
    assert cold['update_silence_p95_ms'] >= 179800
    p.recording.delivery_samples.clear()
    cold = observer.snapshot()['book_freshness_by_activity']['cold']
    assert cold['delivery_latency_p95_ms'] is None and cold['update_silence_p95_ms'] >= 179800

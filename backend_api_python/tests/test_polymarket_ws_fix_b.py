"""Fix B: one feed process per hot group. In-process engine stays the default."""
import json
import os
import re
import signal
import threading
import time
from datetime import datetime, timezone

from app.services.polymarket import observer
from app.services.polymarket.ws_group_process import (
    HOT_ORDER, PROD_FRAME_WEIGHTS, GroupFeeds, even_plan, merge_feed_status,
    partition, planned_events, process_per_group_enabled, queue_age_series,
    weighted_counts, without_stale_books)

_FEED_ID = re.compile(r'^feed-\d+$')


def _paths(value, prefix=''):
    if isinstance(value, dict):
        found = set()
        for key, item in value.items():
            raw = str(key)
            # Shard indexes and feed generation counters are instance ids.
            name = '#' if isinstance(key, int) or raw.isdigit() or _FEED_ID.match(raw) else raw
            child = f'{prefix}.{name}'
            found.add(child)
            found |= _paths(item, child)
        return found
    if isinstance(value, (list, tuple)) and value and isinstance(value[0], dict):
        found = set()
        for item in value:
            found |= _paths(item, prefix)
        return found
    return set()


def _markets():
    return [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n', market_type='up_down',
                 asset=asset, interval=interval)
            for i, (asset, interval) in enumerate((('BTC', '15m'), ('BTC', '5m'), ('ETH', '15m'), ('ETH', '5m')))]


def test_process_flag_defaults_off(monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', raising=False)
    assert process_per_group_enabled() is False
    monkeypatch.setenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', '1')
    assert process_per_group_enabled() is True


def test_observer_command_defaults_group_processes_on(monkeypatch):
    from app.commands.polymarket_observer import enable_group_processes
    monkeypatch.delenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', raising=False)
    enable_group_processes()
    assert os.environ['POLYMARKET_OBSERVER_PROCESS_PER_GROUP'] == '1'
    # setdefault writes the variable with no monkeypatch undo. Drop it before
    # setenv so the fixture records "absent" and later tests stay in-process.
    os.environ.pop('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', None)
    monkeypatch.setenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', '0')
    enable_group_processes()
    assert os.environ['POLYMARKET_OBSERVER_PROCESS_PER_GROUP'] == '0'


def test_queue_age_series_covers_the_window_not_only_kicks():
    t0 = 1_000_000.0
    samples = [
        dict(role='primary', t=t0 + 0.1, wait=0.002, depth=1),
        dict(role='primary', t=t0 + 0.4, wait=7.2, depth=40),
        dict(role='backup', t=t0 + 0.4, wait=9, depth=3),
        dict(role='primary', t=t0 + 1.2, wait=0.01, depth=0),
        dict(role='primary', t=t0 - 1, wait=5, depth=9),
    ]
    assert queue_age_series(samples, t0) == [
        dict(second=0, queue_age_ms=7200.0, depth_max=40, samples=2),
        dict(second=1, queue_age_ms=10.0, depth_max=0, samples=1),
    ]


class _LagInbox:
    def __init__(self, wait, depth, invalid=False):
        self.wait = wait
        self.queue = type('Q', (), {'qsize': lambda self: depth})()
        self.invalid = threading.Event()
        if invalid:
            self.invalid.set()

    def pending_wait_seconds(self):
        return self.wait


def test_primary_first_yields_only_for_a_live_backlogged_primary():
    manager = observer.MarketConnections()
    assert manager.primary_first is False
    backup = dict(redundant=True, connected=True)
    primary = dict(connected=True, inbox=_LagInbox(0.2, 4))
    manager.states[0] = primary
    assert manager._primary_blocks_backup(0, backup) is False
    manager.primary_first = True
    assert manager._primary_blocks_backup(0, backup) is True
    primary['inbox'] = _LagInbox(0.01, 4)
    assert manager._primary_blocks_backup(0, backup) is False
    primary['inbox'] = _LagInbox(7.2, 0)
    assert manager._primary_blocks_backup(0, backup) is False
    primary['inbox'] = _LagInbox(0.2, 4, invalid=True)
    assert manager._primary_blocks_backup(0, backup) is False
    primary['inbox'] = _LagInbox(0.2, 4)
    primary['connected'] = False
    assert manager._primary_blocks_backup(0, backup) is False
    primary['connected'] = True
    assert manager._primary_blocks_backup(0, dict(redundant=False)) is False


def test_partition_puts_each_hot_series_alone():
    specs = partition(_markets() + [dict(id='c', market_type='yes_no')])
    assert [row['key'] for row in specs] == [*HOT_ORDER, 'cold']
    assert all(row['shard_count'] == 1 and row['hot'] for row in specs if row['hot'])
    assert specs[-1]['shard_base'] == 4 and specs[-1]['hot'] is False


def test_weighted_counts_match_the_measured_mix():
    weights = [PROD_FRAME_WEIGHTS[key] for key in HOT_ORDER]
    counts = weighted_counts(3254, weights)
    assert sum(counts) == 3254
    assert counts[1] > counts[0] > counts[3] > counts[2]


def test_even_schedule_matches_the_inprocess_harness():
    seconds, rate, groups = 2, 400, 4
    total, primary, _backup = even_plan(seconds, rate, groups)
    assert total == int(seconds * rate / 2)
    epoch = 1_700_000_000_000
    emitted = [row for row in planned_events(dict(
        seconds=seconds, rate=rate, levels=4, epoch=epoch, assets=('0y', '0n'),
        group=1, groups=groups, mode='even')) if row[1] == 'primary']
    expected = [index for index in range(total) if index % groups == 1]
    assert len(emitted) == len(expected)
    for index, (offset, _role, event) in zip(expected, emitted):
        assert offset == primary[index]
        at = epoch + round(index * 2000 / rate)
        if (index // groups) % 5 == 0:
            assert event[0]['timestamp'] == at and event[0]['hash'] == f'{index}0y'
        else:
            assert event['timestamp'] == at and event['price_changes'][0]['size'] == str(index)


def test_merge_keeps_parent_lifecycle_and_remaps_shards():
    def continuity(checks, p50):
        return dict(checks=checks, unverified=0, snapshot_restored=0, history_assets=0, history_entries=0,
                    history_evictions=0, merged_gap_ms=dict(samples=1, p50=p50, p95=p50, max=p50),
                    published={}, events=[], pending_assets=[])
    parts = [
        dict(shard_base=0, status=dict(
            mode='stopped', enabled=False, running=False, connected=True, ws_shards=1, reconnect_count=2,
            ws_failover_ms=10, ws_receive_queues={0: dict(depth=1)}, ws_backups={'0': dict(ready=True)},
            close_reason_counts={'slow_consumer': 1}, ws_continuity=continuity(1, 3), ws_capacity_excess=(1, 0))),
        dict(shard_base=1, status=dict(
            mode='stopped', enabled=False, running=False, connected=True, ws_shards=1, reconnect_count=3,
            ws_failover_ms=40, ws_receive_queues={0: dict(depth=4)}, ws_backups={0: dict(ready=False)},
            close_reason_counts={'slow_consumer': 2}, ws_continuity=continuity(4, 9), ws_capacity_excess=(0, 2))),
    ]
    merged = merge_feed_status(parts)
    assert 'mode' not in merged and 'enabled' not in merged and 'running' not in merged
    assert set(merged['ws_receive_queues']) == {0, 1}
    assert merged['ws_receive_queues'][1]['depth'] == 4
    assert set(merged['ws_backups']) == {0, 1}
    assert merged['reconnect_count'] == 5 and merged['ws_failover_ms'] == 40 and merged['ws_shards'] == 2
    assert merged['close_reason_counts']['slow_consumer'] == 3
    assert merged['ws_continuity']['checks'] == 5 and merged['ws_continuity']['merged_gap_ms']['p50'] == 9
    assert merged['ws_capacity_excess'] == (1, 2) and merged['connected'] is True


def test_parent_skips_books_older_than_its_watermark():
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    metadata = {'ay': (stamp, stamp, 'ws')}
    older = int(stamp.timestamp() * 1000) - 1000
    newer = int(stamp.timestamp() * 1000) + 1000
    old = dict(event_type='book', asset_id='ay', timestamp=older, bids=[], asks=[])
    new = dict(event_type='book', asset_id='ay', timestamp=newer, bids=[], asks=[])
    change = dict(event_type='price_change', timestamp=older, price_changes=[])
    assert without_stale_books(old, metadata) is None
    assert without_stale_books(new, metadata) is new
    assert without_stale_books(change, metadata) is change
    assert without_stale_books([old, new, change], metadata) == [new, change]


def _inprocess_paths(monkeypatch):
    import sys
    from types import SimpleNamespace
    sockets = []

    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks, self.assets, self.stopped = callbacks, [], threading.Event()
            sockets.append(self)

        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            self.stopped.wait(30)

        def send(self, raw):
            if raw == 'PING':
                return
            message = json.loads(raw)
            if 'assets_ids' in message:
                self.assets = list(message['assets_ids'])

        def close(self):
            self.stopped.set()

        def emit(self, raw):
            self.callbacks['on_message'](self, raw)

    monkeypatch.setitem(sys.modules, 'websocket', SimpleNamespace(WebSocketApp=Socket))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '4')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS', '0')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED', '0')
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, 'STATUS', dict(mode='running', enabled=True, running=True,
                                                  reconnect_count=0, slow_consumer_count=0))
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    markets = _markets()
    pipeline.set_markets(markets)
    manager = observer.MarketConnections()
    monkeypatch.setattr(manager.snapshot_recovery, 'request', lambda *args, **kwargs: False)
    try:
        manager.sync(markets)
        deadline = time.monotonic() + 8
        while len(manager.backups) < 4 or not all(row.get('ready') for row in manager.backups.values()):
            for sock in list(sockets):
                if sock.assets:
                    sock.emit(json.dumps([dict(event_type='book', asset_id=asset, timestamp=int(time.time() * 1000),
                                               bids=[dict(price='.4', size='1')], asks=[dict(price='.6', size='1')])
                                          for asset in sock.assets]))
            manager.backup_next_connect_at = 0
            manager.tick(status=False)
            if time.monotonic() > deadline:
                raise RuntimeError('in-process bootstrap failed')
            time.sleep(0.01)
        manager.publish_status()
        assert observer.STATUS['mode'] == 'ws'
        return _paths(observer.snapshot())
    finally:
        manager.close(3)


def test_group_processes_keep_status_shape_and_reap(monkeypatch):
    inprocess = _inprocess_paths(monkeypatch)
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '4')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS', '0')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED', '0')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REPLAY', '1')
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, 'STATUS', dict(mode='running', enabled=True, running=True,
                                                  reconnect_count=0, slow_consumer_count=0))
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    markets = _markets()
    pipeline.set_markets(markets)
    feeds = GroupFeeds(replay=True)
    monkeypatch.setattr(observer, '_WS', feeds)
    try:
        feeds.sync(markets)
        feeds.wait_ready(20)
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'ws'
        assert observer.STATUS['enabled'] is True and observer.STATUS['running'] is True
        observer.STATUS['mode'] = 'stopping'
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'stopping'
        observer.STATUS['mode'] = 'starting'
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'ws'
        missing = inprocess - _paths(observer.snapshot())
        assert not missing, sorted(missing)[:50]
        assert 'ws_group_processes' in observer.snapshot()
        key = feeds.order[0]
        pid = feeds.children[key].proc.pid
        os.kill(pid, signal.SIGKILL)
        deadline = time.monotonic() + 15
        replacement = None
        while time.monotonic() < deadline:
            current = feeds.children.get(key)
            if current is not None and current.proc is not None and current.proc.pid != pid and current.hello.is_set():
                replacement = current.proc.pid
                break
            time.sleep(0.1)
        assert replacement, 'child was not restarted'
        feeds.wait_ready(15)
        at = int(time.time() * 1000)
        feeds.emit(key, 'primary', json.dumps([dict(
            event_type='book', asset_id='0y', timestamp=at, hash='restarted',
            bids=[dict(price='.41', size='9')], asks=[dict(price='.61', size='9')])]))
        seen = time.monotonic() + 5
        while time.monotonic() < seen and pipeline.books.get('0y', {}).get('bids', {}).get('.41') != '9':
            time.sleep(0.05)
        assert pipeline.books.get('0y', {}).get('bids', {}).get('.41') == '9'
        pids = [child.proc.pid for child in feeds.children.values() if child.proc is not None]
        feeds.close(5)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and any(os.path.exists(f'/proc/{pid}') for pid in pids):
            time.sleep(0.05)
        assert not any(os.path.exists(f'/proc/{pid}') for pid in pids)
        assert feeds.alive_workers() is False
    finally:
        feeds.close(3)

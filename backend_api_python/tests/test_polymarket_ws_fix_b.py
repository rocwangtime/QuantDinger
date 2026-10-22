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
    HOT_ORDER, PROD_FRAME_WEIGHTS, GroupFeeds, _arm_replay_echo, _as_price_change,
    _echo_books_enabled, even_plan, merge_feed_status,
    partition, planned_events, primary_first_enabled, process_per_group_enabled,
    queue_age_series, ready_drops_while_receiving, restart_backoff_seconds, suppress_backup_books,
    weighted_counts, wire_event, without_stale_books)
from app.services.polymarket.ws_payloads import cheap_envelope, hold_until_snapshot, snapshot_keeps_change

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


def test_primary_first_switch_defaults_off(monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_PRIMARY_FIRST', raising=False)
    assert primary_first_enabled() is False
    assert observer.MarketConnections().primary_first is False
    monkeypatch.setenv('POLYMARKET_OBSERVER_PRIMARY_FIRST', '1')
    assert primary_first_enabled() is True


def test_restart_backoff_caps_and_alerts_on_the_third_death():
    assert [restart_backoff_seconds(n) for n in range(1, 9)] == [0.5, 1, 2, 4, 8, 16, 30, 30]
    feeds = GroupFeeds(replay=True)
    spawned = []
    feeds._spawn_locked = lambda spec: spawned.append(spec['key'])
    child = type('Child', (), {})()
    child.spec = dict(key='g')
    child.stop = lambda timeout: None
    child.proc = type('Proc', (), {'is_alive': lambda self: False, 'exitcode': -9})()
    assert feeds._consider_restart('g', child, 100.0) is True
    assert feeds.restarts['g'] == 1 and feeds.restart_after['g'] == 100.5
    assert 'g' not in feeds.restart_alert and spawned == ['g']
    assert feeds._consider_restart('g', child, 100.2) is False
    assert feeds._consider_restart('g', child, 100.5) is True
    assert feeds.restarts['g'] == 2 and 'g' not in feeds.restart_alert
    assert feeds._consider_restart('g', child, 102.0) is True
    assert feeds.restart_alert['g']['restarts'] == 3
    assert feeds.restart_alert['g']['backoff_s'] == 2
    alive = type('Proc', (), {'is_alive': lambda self: True, 'exitcode': None})()
    child.proc = alive
    assert feeds._consider_restart('g', child, 200.0) is False
    feeds.restart_stable_since['g'] = 200.0
    assert feeds._consider_restart('g', child, 200.0 + 599) is False
    assert feeds.restarts['g'] == 3 and 'g' in feeds.restart_alert
    assert feeds._consider_restart('g', child, 200.0 + 600) is False
    assert feeds.restarts['g'] == 0 and 'g' not in feeds.restart_alert


def test_cheap_envelope_skips_the_ladder():
    book = wire_event(7, 0, ('ay', 'an'), 140, 1_700_000_000_000)
    tips = cheap_envelope(json.dumps(book, separators=(',', ':')))
    assert tips == [
        ('book', 'ay', '1700000000000', '7ay'),
        ('book', 'an', '1700000000000', '7an'),
    ]
    assert '0.33' not in json.dumps(tips)
    change = wire_event(8, 1, ('ay', 'an'), 140, 1_700_000_000_001)
    tips = cheap_envelope(json.dumps(change, separators=(',', ':')))
    assert {row[1] for row in tips} == {'ay', 'an'}
    assert {row[2] for row in tips} == {'1700000000001'}
    assert all(row[2] for row in tips)


def _exchange_price_change(*assets, stamp='1757908892351'):
    """Current market channel: both outcome tokens, timestamp and event_type last."""
    return json.dumps(dict(
        market='0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1',
        price_changes=[dict(asset_id=asset, price='0.5', size='200', side='BUY', hash=f'h{asset}',
                            best_bid='0.5', best_ask='1') for asset in assets],
        timestamp=stamp, event_type='price_change'), separators=(',', ':'))


def _exchange_book(asset, stamp='1757908892351', digest=None):
    """Book with event_type last. A `}` inside a size must not end the object."""
    return json.dumps(dict(
        market='0xabc', asset_id=asset, timestamp=stamp, hash=digest or f'h{asset}',
        bids=[dict(price='.48', size='30')], asks=[dict(price='.52', size='1}2')],
        event_type='book'), separators=(',', ':'))


def test_cheap_envelope_reads_exchange_field_order():
    """The order Polymarket publishes. Lab order (timestamp before the rows) is not enough."""
    tips = cheap_envelope(_exchange_price_change('71321045679252212594626385532706912750332728571942532289631379312455583992563',
                                                 '52114319501245915516055106046884209969926127482827954674443846427813813222426'))
    assert [row[1] for row in tips] == [
        '71321045679252212594626385532706912750332728571942532289631379312455583992563',
        '52114319501245915516055106046884209969926127482827954674443846427813813222426']
    assert {row[0] for row in tips} == {'price_change'}
    assert {row[2] for row in tips} == {'1757908892351'}
    assert [row[3] for row in tips] == [
        'h71321045679252212594626385532706912750332728571942532289631379312455583992563',
        'h52114319501245915516055106046884209969926127482827954674443846427813813222426']
    books = cheap_envelope('[' + _exchange_book('ay') + ',' + _exchange_book('an') + ']')
    assert books == [
        ('book', 'ay', '1757908892351', 'hay'),
        ('book', 'an', '1757908892351', 'han'),
    ]
    # Older schema: one top-level asset_id, changes without a nested asset, event_type not first.
    old = json.dumps(dict(
        asset_id='ay', changes=[dict(price='.4', size='1', side='BUY')],
        event_type='price_change', timestamp='1600000000000', hash='oldhash'), separators=(',', ':'))
    assert cheap_envelope(old) == [('price_change', 'ay', '1600000000000', 'oldhash')]
    trade = json.dumps(dict(
        asset_id='ay', fee_rate_bps='0', market='0xabc', price='0.5', side='BUY', size='10',
        timestamp='1757908892351', event_type='last_trade_price'), separators=(',', ':'))
    assert cheap_envelope(trade) == [('last_trade_price', 'ay', '1757908892351', None)]


def test_ready_drop_while_receiving_fails_the_gate():
    held = [
        dict(role='backup', ready=False, connected=True, received=1),
        dict(role='backup', ready=True, connected=True, received=2),
        dict(role='backup', ready=True, connected=True, received=9),
    ]
    assert ready_drops_while_receiving(held) == 0
    quiet = [
        dict(role='backup', ready=True, connected=True, received=5),
        dict(role='backup', ready=False, connected=True, received=5),
    ]
    assert ready_drops_while_receiving(quiet) == 0
    dropped = [
        dict(role='backup', ready=False, connected=True, received=1),
        dict(role='backup', ready=True, connected=True, received=2),
        dict(role='backup', ready=False, connected=True, received=3),
        dict(role='backup', ready=False, connected=False, received=4),
    ]
    assert ready_drops_while_receiving(dropped) == 1


def test_child_ready_probe_is_copied_not_summed():
    """A second child must not add its number into the spare that owns the group."""
    btc = dict(group='up_down:BTC:5m', feed='up_down:BTC:5m', liveness_backup=True,
               ready_count=1, spare_count=1,
               spares=[dict(ready=True, blockers=[], received_frames=40, envelope_age_s=0.2)])
    eth = dict(group='up_down:ETH:5m', feed='up_down:ETH:5m', liveness_backup=True,
               ready_count=0, spare_count=1,
               spares=[dict(ready=False, blockers=['stale_envelope'], received_frames=12)])
    other = dict(group='up_down:BTC:5m', feed='up_down:ETH:5m', liveness_backup=True,
                 ready_count=4, spare_count=1,
                 spares=[dict(ready=False, blockers=['missing_envelope'], received_frames=0)])
    parts = [
        dict(key='up_down:BTC:5m', status={
            'ws_child_ready': {'up_down:BTC:5m': btc},
            'ws_hot_backup_by_group': {'up_down:BTC:5m': {'ready_backup_count': 1},
                                       'up_down:ETH:5m': {'ready_backup_count': 0}}}),
        dict(key='up_down:ETH:5m', status={
            'ws_child_ready': {'up_down:ETH:5m': eth, 'up_down:BTC:5m': other},
            'ws_hot_backup_by_group': {'up_down:BTC:5m': {'ready_backup_count': 0},
                                       'up_down:ETH:5m': {'ready_backup_count': 0}}}),
    ]
    merged = merge_feed_status(parts)
    probe = merged['ws_child_ready']['up_down:BTC:5m']
    assert probe['ready_count'] == 1 and probe['feed'] == 'up_down:BTC:5m'
    assert probe['spares'][0]['received_frames'] == 40
    assert probe['also'][0]['ready_count'] == 4 and probe['also'][0]['feed'] == 'up_down:ETH:5m'
    assert merged['ws_child_ready']['up_down:ETH:5m']['spares'][0]['blockers'] == ['stale_envelope']
    assert merged['ws_hot_backup_by_group']['up_down:BTC:5m']['ready_backup_count'] == 1
    assert merged['ws_hot_backup_by_group']['up_down:ETH:5m']['ready_backup_count'] == 0
    flipped = merge_feed_status(list(reversed(parts)))
    owned = flipped['ws_child_ready']['up_down:BTC:5m']
    assert owned['ready_count'] == 1 and owned['feed'] == 'up_down:BTC:5m'
    assert owned['also'][0]['feed'] == 'up_down:ETH:5m'


class _Inbox:
    def __init__(self):
        self.epoch = 0
        self.invalid = threading.Event()
        self.resync = threading.Event()
        self.delivery_gap = False

    def pending_wait_seconds(self):
        return 0.0


def test_liveness_backup_promotes_and_holds_until_a_book():
    manager = observer.MarketConnections()
    manager.liveness_backup = True
    manager.update_hot_coverage = lambda now: None
    retired, snaps = [], []
    manager.retire = lambda state: retired.append(state)
    manager.request_snapshot = lambda index, state, assets, pressure=False: snaps.append(list(assets))
    manager.redundancy_tick = lambda now: None
    manager._arm_book_fallback = lambda index, state: None
    inbox = _Inbox()
    backup = dict(redundant=True, connected=True, standby=True, assets=['ay', 'an'],
                  worker=threading.current_thread(), inbox=inbox, source='backup',
                  subscribe_pending=[], recover_assets=[])
    primary = dict(connected=True, assets=['ay', 'an'], inbox=_Inbox(), source='primary')
    from app.services.polymarket.ws_transport import TransportDiagnostics
    if not backup.get('socket'):
        backup['socket'] = type('Socket', (), {'transport': TransportDiagnostics()})()
        backup['socket'].transport.pong()
    manager.states[0] = primary
    manager.backups[0] = backup
    raw = json.dumps([dict(event_type='book', asset_id=asset, timestamp=5, hash=f'h{asset}',
                           bids=[dict(price='.4', size='1')], asks=[]) for asset in ('ay', 'an')])
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), inbox)
    assert backup['ready'] is True and manager.backup_ready(backup)
    assert not backup.get('shadow') and manager.backup_promotions == 0 and not retired
    primary['connected'] = False
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), inbox)
    assert manager.states[0] is backup and backup['redundant'] is False
    assert backup['need_book'] == {'ay', 'an'} and snaps == [['ay', 'an']] and retired == [primary]
    change = dict(event_type='price_change', price_changes=[dict(asset_id='ay', price='.4', size='1')])
    assert manager._release_resync(backup, change) is None
    assert 'ay' in backup['need_book']
    book = dict(event_type='book', asset_id='ay', timestamp=6, hash='next', bids=[], asks=[])
    assert manager._release_resync(backup, book) is book
    assert backup['need_book'] == {'an'}


def _price_change(*assets, stamp=1_700_000_000_000):
    """Production key order. An old exchange timestamp must still count: freshness is receive time."""
    return _exchange_price_change(*assets, stamp=stamp)


def _liveness_manager():
    """Primary stays up. The spare has a deep queue and an empty ladder."""
    manager = observer.MarketConnections()
    manager.liveness_backup = True
    manager.balancer.assignment = {'ay': 0, 'an': 0}
    manager.balancer.groups = {'ay': 'up_down:BTC:5m', 'an': 'up_down:BTC:5m'}
    now = time.monotonic()
    primary_inbox = observer.ReceiveQueue(16, 64)
    backup_inbox = observer.ReceiveQueue(16, 64)
    backup_inbox.queue_wait_seconds = 5
    primary = dict(connected=True, assets=['ay', 'an'], inbox=primary_inbox, source='primary',
                   thread=threading.current_thread(), subscribe_initial=True, socket=None,
                   ping_at=now + 100, lock=threading.RLock())
    shadow = observer._LivenessShadow(['ay', 'an'])
    backup = dict(redundant=True, connected=True, standby=True, assets=['ay', 'an'],
                  worker=threading.current_thread(), thread=threading.current_thread(),
                  inbox=backup_inbox, source='backup', subscribe_pending=[], subscribe_initial=False,
                  recover_assets={'ay', 'an'}, primary_index=0, shadow=shadow, lock=threading.RLock(),
                  initial_sent_at=now, created_at=now, snapshot_retry_at=0, required_assets={'ay', 'an'},
                  admission_observed=True)
    from app.services.polymarket.ws_transport import TransportDiagnostics
    if not backup.get('socket'):
        backup['socket'] = type('Socket', (), {'transport': TransportDiagnostics()})()
        backup['socket'].transport.pong()
    manager.states[0] = primary
    manager.backups[0] = backup
    return manager, primary, backup, shadow


def test_liveness_backup_reaches_ready_without_a_shadow_book():
    manager, primary, backup, shadow = _liveness_manager()
    assert manager._queue_lag_breaks_backup(backup, backup['inbox']) is False
    plain = observer.MarketConnections()
    assert plain._queue_lag_breaks_backup(dict(redundant=True), backup['inbox']) is True
    assert backup['inbox'].pending_wait_seconds() == 5
    manager.liveness_receive(0, backup, _price_change('ay', 'an'), datetime.now(timezone.utc), backup['inbox'])
    assert backup['ready'] is True and manager.backup_ready(backup)
    assert shadow.books == {} and not backup.get('liveness_hash')
    assert backup['liveness_assets'] == {'ay', 'an'}
    blockers = set(manager.readiness_blockers(backup))
    assert blockers.isdisjoint({'queue_wait', 'snapshot_repair', 'missing_snapshot', 'catchup', 'missing_envelope'})
    manager.status_at = time.monotonic()
    manager._tick()
    assert backup['ready'] is True and manager.backup_ready(backup)
    assert backup['liveness_assets'] == {'ay', 'an'} and shadow.books == {}
    assert manager.states[0] is primary and manager.backup_promotions == 0


def test_primary_1013_promotes_a_ready_liveness_backup_without_a_ws_book():
    manager, primary, backup, shadow = _liveness_manager()
    snaps, retired = [], []
    manager.request_snapshot = lambda index, state, assets, pressure=False: snaps.append(list(assets)) or True
    manager.retire = lambda state: retired.append(state)
    manager.redundancy_tick = lambda now: None
    manager.update_hot_coverage = lambda now: None
    manager._arm_book_fallback = lambda index, state: None
    manager.liveness_receive(0, backup, _price_change('ay', 'an'), datetime.now(timezone.utc), backup['inbox'])
    assert manager.backup_ready(backup) and shadow.books == {}
    # closed() on 1013: the primary is down and its generation is invalid.
    primary['connected'] = False
    primary['inbox'].invalid.set()
    primary['failed_at'] = time.monotonic()
    started = time.monotonic()
    assert manager._promote_liveness_backup(0, backup) is True
    assert time.monotonic() - started <= 1.5
    assert manager.states[0] is backup and manager.backup_promotions == 1
    assert backup['need_book'] == {'ay', 'an'} and snaps == [['ay', 'an']] and retired == [primary]
    assert shadow.books == {} and not backup.get('liveness_hash')


def test_ready_backup_count_includes_a_liveness_spare():
    manager, primary, backup, shadow = _liveness_manager()
    manager.liveness_receive(0, backup, _price_change('ay', 'an'), datetime.now(timezone.utc), backup['inbox'])
    assert primary['connected'] and not primary['inbox'].invalid.is_set()
    rows = manager.update_hot_coverage(time.monotonic())
    assert rows['up_down:BTC:5m']['ready_backup_count'] == 1
    manager.hot_coverage_rows = rows
    manager.publish_status()
    assert observer.STATUS['ws_hot_backup_by_group']['up_down:BTC:5m']['ready_backup_count'] == 1
    assert observer.STATUS['ws_backups'][0]['ready'] is True
    assert shadow.books == {}
    probe = manager.child_ready_probe()['up_down:BTC:5m']
    assert probe['ready_count'] == 1 and probe['spare_count'] == 1
    spare = probe['spares'][0]
    assert spare['ready'] is True and spare['blockers'] == []
    assert spare['seen_assets'] == 2 and spare['required_assets'] == 2
    assert spare['envelope_fresh'] is True and spare['missing_assets'] == []


def test_exchange_frames_keep_a_receiving_spare_ready(monkeypatch):
    """A >15s gap between receives must not clear ready while the next frame arrives.

    The exchange timestamp is years in the past. Comparing it to the wall clock
    would mark every frame stale. Receive time is monotonic.
    """
    clock = {'now': 1_000.0}
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock['now'])
    manager, _primary, backup, shadow = _liveness_manager()
    raw = _exchange_price_change('ay', 'an', stamp=1_600_000_000_000)
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), backup['inbox'])
    assert backup['ready'] is True and manager.backup_ready(backup)
    assert backup['liveness_assets'] == {'ay', 'an'} and shadow.books == {}
    assert 'stale_envelope' not in manager.readiness_blockers(backup)
    assert manager.update_hot_coverage(clock['now'])['up_down:BTC:5m']['ready_backup_count'] == 1
    clock['now'] += 20
    assert manager.backup_ready(backup) is False
    assert 'stale_envelope' in manager.readiness_blockers(backup)
    backup['socket'].transport.pong()
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), backup['inbox'])
    assert backup['ready'] is True and manager.backup_ready(backup)
    assert manager.update_hot_coverage(clock['now'])['up_down:BTC:5m']['ready_backup_count'] == 1
    books = '[' + _exchange_book('ay') + ',' + _exchange_book('an') + ']'
    manager.liveness_receive(0, backup, books, datetime.now(timezone.utc), backup['inbox'])
    assert backup['liveness_hash'] == {'ay': 'hay', 'an': 'han'}
    assert manager.backup_ready(backup) is True


def test_fresh_pong_holds_ready_across_a_quiet_gap(monkeypatch):
    """BTC5m shape: both assets seen, then 20s with no envelope. A live PONG holds ready."""
    clock = {'now': 1_000.0}
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock['now'])
    manager, _primary, backup, _shadow = _liveness_manager()
    manager.liveness_receive(0, backup, _exchange_price_change('ay', 'an'), datetime.now(timezone.utc), backup['inbox'])
    assert backup['ready'] is True
    clock['now'] += 20
    assert manager.backup_ready(backup) is False
    backup['socket'] = type('Sock', (), {})()
    backup['socket'].transport = type('Transport', (), {'pongs': 3, 'pong_at': clock['now'] - 1, 'last': {}})()
    assert manager.backup_ready(backup) is True
    assert 'stale_envelope' not in manager.readiness_blockers(backup)
    assert manager.update_hot_coverage(clock['now'])['up_down:BTC:5m']['ready_backup_count'] == 1
    probe = manager.child_ready_probe(clock['now'])['up_down:BTC:5m']['spares'][0]
    assert probe['ready'] is True and probe['envelope_fresh'] is False and probe['link_fresh'] is True
    assert probe['pong_age_s'] == 1 and 'stale_envelope' not in probe['blockers']
    backup['liveness_assets'] = {'ay'}
    assert manager.backup_ready(backup) is False
    assert 'missing_envelope' in manager.readiness_blockers(backup)
    backup['liveness_assets'] = {'ay', 'an'}
    backup['socket'].transport.pong_at = clock['now'] - 16
    assert manager.backup_ready(backup) is False
    assert 'stale_envelope' in manager.readiness_blockers(backup)
    backup['socket'].transport = type('Transport', (), {
        'pongs': 0, 'pong_at': None, 'last': dict(pongs=2, pong_age_seconds=1)})()
    assert manager.backup_ready(backup) is True


def test_image_makes_the_app_tree_readable():
    text = open('Dockerfile', encoding='utf-8').read()
    copied = text.find('COPY . .')
    mode = text.find('chmod -R a+rX /app')
    assert copied != -1 and mode > copied


def test_liveness_replay_suppresses_backup_books_until_the_kick(monkeypatch):
    from app.services.polymarket import ws_group_process as feeds
    monkeypatch.delenv('QD_REPLAY_SUPPRESS_BOOKS', raising=False)
    feeds._REPLAY_ECHO_BOOKS = False
    assert suppress_backup_books() is False and _echo_books_enabled() is True
    monkeypatch.setenv('QD_REPLAY_SUPPRESS_BOOKS', '1')
    feeds._REPLAY_ECHO_BOOKS = False
    assert suppress_backup_books() is True and _echo_books_enabled() is False
    book = json.dumps([dict(event_type='book', asset_id='ay', timestamp=5, hash='h', bids=[], asks=[])])
    change = json.loads(_as_price_change(book))
    assert change['event_type'] == 'price_change'
    assert {row['asset_id'] for row in change['price_changes']} == {'ay'}
    _arm_replay_echo()
    assert _echo_books_enabled() is True
    feeds._REPLAY_ECHO_BOOKS = False


def test_snapshot_orders_changes_by_timestamp_and_hash():
    book_at = datetime(2026, 10, 19, tzinfo=timezone.utc)
    older = book_at.timestamp() * 1000 - 5
    same = int(book_at.timestamp() * 1000)
    newer = same + 5
    assert snapshot_keeps_change(older, 'h', book_at, 'book') is False
    assert snapshot_keeps_change(same, 'book', book_at, 'book') is False
    assert snapshot_keeps_change(same, 'other', book_at, 'book') is True
    assert snapshot_keeps_change(newer, 'h', book_at, 'book') is True
    assert snapshot_keeps_change(None, 'h', book_at, 'book') is False


def test_rest_snapshot_releases_price_changes_within_1_5s(monkeypatch):
    from app.services.polymarket.ws_redundancy import FeedMerge
    manager = observer.MarketConnections()
    manager.liveness_backup = True
    manager.update_hot_coverage = lambda now: None
    manager.retire = lambda state: None
    manager.redundancy_tick = lambda now: None
    sent = []

    class Sock:
        def send(self, raw):
            sent.append(json.loads(raw))

    inbox = _Inbox()
    inbox.epoch = 1
    backup = dict(redundant=True, connected=True, standby=True, assets=['ay', 'an'],
                  worker=threading.current_thread(), inbox=inbox, source='backup',
                  subscribe_pending=[], recover_assets=[], lock=threading.RLock(), socket=Sock())
    primary = dict(connected=False, assets=['ay', 'an'], inbox=_Inbox(), source='primary')
    from app.services.polymarket.ws_transport import TransportDiagnostics
    if not backup.get('socket'):
        backup['socket'] = type('Socket', (), {'transport': TransportDiagnostics()})()
        backup['socket'].transport.pong()
    manager.states[0] = primary
    manager.backups[0] = backup
    manager.balancer.assignment = {'ay': 0, 'an': 0}
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    pipeline.set_markets([dict(id='0', yesAssetId='ay', noAssetId='an')])
    manager.merges[0] = FeedMerge({})
    stamp = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
    backup['book_marks'] = {
        'ay': (datetime.fromtimestamp(stamp / 1000, timezone.utc), 'hay', stamp),
        'an': (datetime.fromtimestamp(stamp / 1000, timezone.utc), 'han', stamp),
    }
    backup['need_book'] = {'ay', 'an'}
    older = dict(event_type='price_change', timestamp=stamp - 5, price_changes=[
        dict(asset_id='ay', price='.4', size='1', hash='old')])
    newer = dict(event_type='price_change', timestamp=stamp + 5, price_changes=[
        dict(asset_id='ay', price='.41', size='2', hash='new'),
        dict(asset_id='an', price='.59', size='2', hash='new2')])
    assert manager._release_resync(backup, older) is None
    assert manager._release_resync(backup, newer) is None
    backup['redundant'] = False
    manager.states[0] = backup
    arrived = time.monotonic()

    def publish_rest(state, assets, publish, warm=False, pressure=False):
        publish([
            dict(event_type='book', asset_id='ay', timestamp=stamp, hash='hay', bids=[], asks=[]),
            dict(event_type='book', asset_id='an', timestamp=stamp, hash='han', bids=[], asks=[]),
        ], datetime.now(timezone.utc))
        return True

    manager.snapshot_recovery.request = publish_rest
    assert manager.request_snapshot(0, backup, ['ay', 'an']) is True
    assert 'need_book' not in backup
    assert time.monotonic() - arrived < 1.5
    released = manager._release_resync(backup, dict(
        event_type='price_change', timestamp=stamp + 9, price_changes=[
            dict(asset_id='ay', price='.42', size='3', hash='live')]))
    assert released['price_changes'][0]['hash'] == 'live'
    applied = json.dumps(pipeline.books)
    assert 'old' not in applied
    assert pipeline.books['ay']['asks']['.41'] == '2'
    monkeypatch.setattr(observer, 'book_fallback_seconds', lambda: 0.05)
    backup['need_book'] = {'ay'}
    manager._arm_book_fallback(0, backup)
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline and not any(row.get('operation') == 'subscribe' for row in sent):
        time.sleep(0.01)
    assert any(row.get('operation') == 'subscribe' and row.get('assets_ids') == ['ay'] for row in sent)


def _failover_backup(manager, pipeline, monkeypatch):
    inbox = _Inbox()
    inbox.epoch = 1
    sent = []

    class Sock:
        def send(self, raw):
            sent.append(json.loads(raw))

    backup = dict(redundant=False, connected=True, standby=False, assets=['ay', 'an'],
                  worker=threading.current_thread(), inbox=inbox, source='backup',
                  subscribe_pending=[], recover_assets=[], lock=threading.RLock(), socket=Sock())
    manager.states[0] = backup
    manager.balancer.assignment = {'ay': 0, 'an': 0}
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    pipeline.set_markets([dict(id='0', yesAssetId='ay', noAssetId='an')])
    return backup, sent


def test_rest_book_survives_a_change_inserted_before_the_write(monkeypatch):
    """The old clear happened before the pipeline write. A newer price_change in
    that window made FeedMerge drop the REST book after the hold was already gone.
    """
    from app.services.polymarket.ws_redundancy import FeedMerge
    manager = observer.MarketConnections()
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    backup, _sent = _failover_backup(manager, pipeline, monkeypatch)
    stamp = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
    book_at = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    merge = FeedMerge({})
    merge.latest['ay'] = merge.snapshots['ay'] = book_at
    merge.latest['an'] = merge.snapshots['an'] = book_at
    manager.merges[0] = merge
    backup['book_marks'] = {
        'ay': (book_at, 'hay', stamp),
        'an': (book_at, 'han', stamp),
    }
    backup['need_book'] = {'ay', 'an'}
    held = backup
    real_filter = FeedMerge.filter

    def filtering(self, event, source, owned, backup=False):
        # Reader behavior at the old clear-point: need_book is already empty,
        # so the newer change is applied before the REST book is judged.
        if source == 'rest' and not held.get('need_book'):
            newer = dict(event_type='price_change', timestamp=stamp + 50, price_changes=[
                dict(asset_id='ay', price='.9', size='9', side='BUY', hash='race')])
            real_filter(self, newer, 'primary', owned, False)
        return real_filter(self, event, source, owned, backup)

    monkeypatch.setattr(FeedMerge, 'filter', filtering)
    book_holds = []
    original = pipeline.receive

    def receive(raw, owned=None, received_at=None, inbox=None, epoch=None):
        items = raw if isinstance(raw, list) else [raw]
        if any(isinstance(item, dict) and (item.get('event_type') or item.get('type')) == 'book' for item in items):
            book_holds.append(set(backup.get('need_book') or ()))
        return original(raw, owned, received_at, inbox, epoch)

    pipeline.receive = receive

    def publish_rest(state, assets, publish, warm=False, pressure=False):
        publish([
            dict(event_type='book', asset_id='ay', timestamp=stamp + 10, hash='rest-ay',
                 bids=[dict(price='.4', size='7')], asks=[dict(price='.6', size='1')]),
            dict(event_type='book', asset_id='an', timestamp=stamp + 10, hash='rest-an',
                 bids=[dict(price='.4', size='7')], asks=[dict(price='.6', size='1')]),
        ], datetime.now(timezone.utc))
        return True

    manager.snapshot_recovery.request = publish_rest
    assert manager.request_snapshot(0, backup, ['ay', 'an']) is True
    assert book_holds and book_holds[0] == {'ay', 'an'}
    assert 'need_book' not in backup
    assert pipeline.books['ay']['bids']['.4'] == '7'
    assert pipeline.books['ay']['bids'].get('.9') != '9'


def test_filtered_rest_book_stays_held_and_resubscribes_now(monkeypatch):
    from app.services.polymarket.ws_redundancy import FeedMerge
    manager = observer.MarketConnections()
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    backup, sent = _failover_backup(manager, pipeline, monkeypatch)
    stamp = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
    newer = datetime.fromtimestamp((stamp + 100) / 1000, timezone.utc)
    merge = FeedMerge({})
    merge.latest['ay'] = merge.snapshots['ay'] = newer
    manager.merges[0] = merge
    backup['book_marks'] = {'ay': (newer, 'live', stamp + 100)}
    backup['need_book'] = {'ay'}
    began = time.monotonic()

    def publish_rest(state, assets, publish, warm=False, pressure=False):
        publish([dict(event_type='book', asset_id='ay', timestamp=stamp, hash='old',
                      bids=[dict(price='.4', size='1')], asks=[dict(price='.6', size='1')])],
                datetime.now(timezone.utc))
        return True

    manager.snapshot_recovery.request = publish_rest
    assert manager.request_snapshot(0, backup, ['ay']) is True
    assert backup.get('need_book') == {'ay'}
    assert time.monotonic() - began < 0.3
    assert any(row.get('operation') == 'subscribe' and row.get('assets_ids') == ['ay'] for row in sent)
    assert 'ay' not in pipeline.books


def test_rest_request_false_resubscribes_within_1_5s(monkeypatch):
    manager = observer.MarketConnections()
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    backup, sent = _failover_backup(manager, pipeline, monkeypatch)
    stamp = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
    book_at = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    backup['book_marks'] = {'ay': (book_at, 'hay', stamp), 'an': (book_at, 'han', stamp)}
    backup['need_book'] = {'ay', 'an'}
    held = dict(event_type='price_change', timestamp=stamp + 5, price_changes=[
        dict(asset_id='ay', price='.41', size='2', hash='new')])
    assert manager._release_resync(backup, held) is None
    manager.snapshot_recovery.request = lambda *args, **kwargs: False
    began = time.monotonic()
    assert manager.request_snapshot(0, backup, ['ay', 'an']) is False
    assert time.monotonic() - began <= 1.5
    assert time.monotonic() - began < 0.3
    assert any(row.get('operation') == 'subscribe' and row.get('assets_ids') == ['an', 'ay'] for row in sent)
    assert backup.get('need_book') == {'ay', 'an'}
    book = dict(event_type='book', asset_id='ay', timestamp=stamp + 10, hash='pushed',
                bids=[dict(price='.4', size='7')], asks=[dict(price='.6', size='1')])
    manager._release_resync(backup, book)
    live = manager._release_resync(backup, dict(
        event_type='price_change', timestamp=stamp + 20, price_changes=[
            dict(asset_id='ay', price='.42', size='3', hash='live')]))
    assert time.monotonic() - began <= 1.5
    assert 'ay' not in (backup.get('need_book') or set())
    assert live['price_changes'][0]['hash'] == 'live'


def test_book_fallback_retries_and_held_changes_are_bounded(monkeypatch):
    assert observer.book_fallback_seconds() == 0.8
    assert observer.book_fallback_repeat_seconds() == 1.5
    manager = observer.MarketConnections()
    sent = []

    class Sock:
        def send(self, raw):
            sent.append(json.loads(raw))

    backup = dict(connected=True, assets=['ay'], socket=Sock(), need_book={'ay'})
    manager.states[0] = backup
    monkeypatch.setattr(observer, 'book_fallback_seconds', lambda: 0.02)
    monkeypatch.setattr(observer, 'book_fallback_repeat_seconds', lambda: 0.02)
    manager._arm_book_fallback(0, backup)
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline and sum(row.get('operation') == 'subscribe' for row in sent) < 2:
        time.sleep(0.01)
    assert sum(row.get('operation') == 'subscribe' for row in sent) >= 2
    manager._cancel_book_fallback(backup)
    state = {'need_book': {'ay'}}
    last = observer.HELD_CHANGES_MAX + 40
    for i in range(last):
        manager._release_resync(state, dict(event_type='price_change', timestamp=i, price_changes=[
            dict(asset_id='ay', price='.4', size=str(i), hash=str(i))]))
    assert len(state['held_changes']) == observer.HELD_CHANGES_MAX
    assert state['held_changes'][-1]['price_changes'][0]['hash'] == str(last - 1)
    assert state['held_changes_dropped'] == 40


def test_env_file_zero_selects_the_in_process_engine(monkeypatch):
    from app.commands.polymarket_observer import enable_group_processes
    monkeypatch.delenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', raising=False)

    def fake_load(path, override=False):
        assert override is False
        if 'POLYMARKET_OBSERVER_PROCESS_PER_GROUP' not in os.environ:
            os.environ['POLYMARKET_OBSERVER_PROCESS_PER_GROUP'] = '0'
        return True

    monkeypatch.setattr('dotenv.load_dotenv', fake_load)
    enable_group_processes()
    assert os.environ['POLYMARKET_OBSERVER_PROCESS_PER_GROUP'] == '0'
    assert process_per_group_enabled() is False
    os.environ.pop('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', None)


def test_compose_passes_process_per_group_into_the_observer():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    expected = '${POLYMARKET_OBSERVER_PROCESS_PER_GROUP:-1}'
    for name in ('docker-compose.yml', 'docker-compose.ghcr.yml'):
        text = open(os.path.join(root, name), encoding='utf-8').read()
        assert expected in text


def test_promotion_replays_the_primary_book_within_1_5s(monkeypatch):
    """The promoted socket has no ladder. REST must echo the primary's last book."""
    from app.services.polymarket.ws_group_process import _replay_snapshot
    from app.services.polymarket.ws_redundancy import FeedMerge
    manager = observer.MarketConnections()
    manager.liveness_backup = True
    manager.update_hot_coverage = lambda now: None
    manager.retire = lambda state: None
    manager.redundancy_tick = lambda now: None
    inbox = _Inbox()
    inbox.epoch = 1
    backup = dict(redundant=True, connected=True, standby=True, assets=['ay', 'an'],
                  worker=threading.current_thread(), inbox=inbox, source='backup',
                  subscribe_pending=[], recover_assets=[], lock=threading.RLock())
    stamp = int(datetime(2026, 10, 19, tzinfo=timezone.utc).timestamp() * 1000)
    primary = dict(connected=True, assets=['ay', 'an'], inbox=_Inbox(), source='primary',
                   book_marks={
                       'ay': (datetime.fromtimestamp(stamp / 1000, timezone.utc), 'hay', stamp),
                       'an': (datetime.fromtimestamp(stamp / 1000, timezone.utc), 'han', stamp),
                   })
    from app.services.polymarket.ws_transport import TransportDiagnostics
    if not backup.get('socket'):
        backup['socket'] = type('Socket', (), {'transport': TransportDiagnostics()})()
        backup['socket'].transport.pong()
    manager.states[0] = primary
    manager.backups[0] = backup
    manager.balancer.assignment = {'ay': 0, 'an': 0}
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    pipeline.set_markets([dict(id='0', yesAssetId='ay', noAssetId='an')])
    manager.merges[0] = FeedMerge({})
    manager.snapshot_recovery.request = _replay_snapshot
    raw = json.dumps([dict(event_type='book', asset_id=asset, timestamp=stamp, hash=f'h{asset}',
                           bids=[dict(price='.4', size='1')], asks=[]) for asset in ('ay', 'an')])
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), inbox)
    primary['connected'] = False
    arrived = time.monotonic()
    manager.liveness_receive(0, backup, raw, datetime.now(timezone.utc), inbox)
    assert manager.states[0] is backup
    assert 'need_book' not in backup
    assert time.monotonic() - arrived < 1.5
    assert backup['book_marks']['ay'][1] == 'hay'
    released = manager._release_resync(backup, dict(
        event_type='price_change', timestamp=stamp + 5, price_changes=[
            dict(asset_id='ay', price='.41', size='2', hash='live')]))
    assert released['price_changes'][0]['hash'] == 'live'


def test_liveness_books_count_as_snapshots_for_repair():
    manager = observer.MarketConnections()
    manager.liveness_backup = True
    state = dict(redundant=True, assets=['ay', 'an'], liveness_assets={'ay'}, shadow=type('S', (), {'books': {}})())
    assert manager.redundant_seen(state) == {'ay'}
    inbox = _Inbox()
    state.update(connected=True, worker=threading.current_thread(), inbox=inbox,
                 subscribe_pending=[], recover_assets={'ay', 'an'})
    manager.states[0] = dict(connected=True, assets=['ay', 'an'], inbox=_Inbox())
    raw = json.dumps([dict(event_type='book', asset_id='ay', timestamp=5, hash='hay', bids=[], asks=[])])
    manager.liveness_receive(0, state, raw, datetime.now(timezone.utc), inbox)
    assert state['recover_assets'] == {'an'}
    assert manager.redundant_seen(state) == {'ay'}


def test_dropped_deltas_are_fenced_until_a_book():
    feeds = GroupFeeds(replay=True)
    feeds.note_drop('up_down:BTC:5m', ['ay', ''], 4)
    assert feeds.drop_totals['up_down:BTC:5m'] == 4
    assert feeds.resync_assets == {'ay'}
    change = dict(event_type='price_change', price_changes=[dict(asset_id='ay', price='.4', size='1')])
    assert hold_until_snapshot(change, feeds.resync_assets) is None
    book = dict(event_type='book', asset_id='ay', timestamp=9, hash='fresh', bids=[], asks=[])
    assert hold_until_snapshot(book, feeds.resync_assets) is book
    assert feeds.resync_assets == set()
    assert hold_until_snapshot(change, feeds.resync_assets) is change


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
                    history_evictions=0, receive_cadence_ms=dict(hot=__import__('app.services.polymarket.ws_redundancy', fromlist=['cadence_metrics']).cadence_metrics([(p50, 'a', 't')])),
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
    assert merged['ws_continuity']['checks'] == 5 and merged['ws_continuity']['receive_cadence_ms']['hot']['p50'] == 5
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
        # Cadence has no maximum after only one initial snapshot. Publish a
        # second real update before comparing populated diagnostic schemas.
        for child in feeds.children.values():
            assets = [a for m in child.spec['markets'] for a in (m['yesAssetId'], m['noAssetId'])]
            child.send(dict(op='emit', role='primary',
                raw=json.dumps(wire_event(99, 1, assets, 140, int(time.time()*1000)))))
        deadline = time.monotonic()+3
        while not all((((c.latest or {}).get('status') or {}).get('ws_continuity') or {}).get(
                'receive_cadence_ms', {}).get('hot', {}).get('samples', 0) for c in feeds.children.values()):
            assert time.monotonic() < deadline, 'cadence publication did not reach status'
            for child in feeds.children.values():
                child.send(dict(op='status'))
            time.sleep(.01)
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'ws'
        assert observer.STATUS['enabled'] is True and observer.STATUS['running'] is True
        observer.STATUS['mode'] = 'stopping'
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'stopping'
        observer.STATUS['mode'] = 'starting'
        feeds.publish_status()
        assert observer.STATUS['mode'] == 'ws'
        # Reason names are samples. Liveness does not emit shadow-book
        # blockers (missing_snapshot, catchup); the document shape still matches.
        def _schema(paths):
            skip = ('unready_reason_entries.', 'unready_reason_seconds.')
            return {path for path in paths if not any(part in path for part in skip)}
        missing = _schema(inprocess) - _schema(_paths(observer.snapshot()))
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

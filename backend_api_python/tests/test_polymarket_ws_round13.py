"""Half-open hot feeds, honest gaps, bounded working sets and SQL admission."""
import json
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, observer_db, recording, ws_payloads, ws_transport
from app.services.polymarket.ws_redundancy import FeedMerge
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup


def delta(at, size='2'):
    return dict(event_type='price_change', timestamp=at,
                price_changes=[dict(asset_id='ay', side='SELL', price='.4', size=size)])


@pytest.mark.parametrize('seconds', [3, 4, 5])
def test_wire_silence_promotes_ready_backup_without_waiting_for_pong_timeout(feed_manager, monkeypatch, seconds):
    m, p, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BYTE_SILENCE_SECONDS', str(seconds))
    backup = ready_backup(m, sockets)
    old = m.states[0]
    old['inbox'].last_byte_at = time.monotonic() - seconds - .1
    sockets[0].transport = SimpleNamespace(last={}, last_read_at=old['inbox'].last_byte_at)
    # Source history agrees with wire evidence; fan-in metadata stays current.
    for asset, row in m.continuity.latest.items():
        at, key, _ = row[old['source']]
        row[old['source']] = (at, key, datetime.now(timezone.utc) - timedelta(seconds=seconds))
    sockets[1].emit([book('ay', '7'), book('an', '7')])
    wait_for(lambda: m.states[0] is backup, .7)
    assert old['retired'] and p.books['ay']['asks']['.4'] == '7'
    event = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert event['reason'] == 'byte_silence' and event['ready_backup_at_kick']
    assert event['takeover_gap_ms'] >= seconds * 1000
    assert event['detection_to_takeover_ms'] < 700
    assert observer.STATUS.get('slow_consumer_count', 0) == 0
    assert not any(raw == 'PING' for socket in sockets for raw in socket.sent)


def test_hot_fanin_publishes_backup_deltas_and_deduplicates_before_detection(feed_manager):
    m, p, sockets = feed_manager
    ready_backup(m, sockets)
    old = m.states[0]
    old['inbox'].last_byte_at = time.monotonic() - 2
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + 1
    event = delta(at, '8')
    before = p.messages
    sockets[1].emit(event)
    wait_for(lambda: p.books['ay']['asks']['.4'] == '8')
    assert m.states[0] is old and old['connected']
    sockets[0].emit(event)
    wait_for(lambda: m.merges[0].duplicates > 0 and old['inbox'].queue.empty())
    assert p.messages == before + 1
    published = m.continuity.metrics()['published']['ay']
    assert published['receipt_age_ms'] < 1000
    # Older full snapshots from either copy cannot roll the merged book back.
    for socket in sockets[:2]:
        socket.emit(book('ay', '99', at - 1))
    wait_for(lambda: m.merges[0].stale >= 2)
    assert p.books['ay']['asks']['.4'] == '8'


@pytest.mark.parametrize('condition', ['both_quiet', 'recent_wire_bytes', 'pong', 'backup_lag', 'backup_resync'])
def test_silence_switch_requires_recent_caught_up_backup_and_no_primary_bytes(feed_manager, condition):
    m, _, sockets = feed_manager
    backup = ready_backup(m, sockets)
    old = m.states[0]
    with m.lifecycle_lock:
        old['inbox'].last_byte_at = time.monotonic() - 6
        backup['inbox'].last_data_at = time.monotonic()
        if condition == 'both_quiet':
            backup['inbox'].last_data_at -= 6
        elif condition == 'recent_wire_bytes':
            sockets[0].transport = SimpleNamespace(last={}, last_read_at=time.monotonic())
        elif condition == 'pong':
            sockets[0].emit('PONG')
        elif condition == 'backup_lag':
            backup['inbox'].queue_wait_seconds = 2
        else:
            backup['inbox'].resync.set()
        m.redundant_receive(0, backup, [], datetime.now(timezone.utc), backup['inbox'])
        assert not old.get('data_suspect') and m.states[0] is old
        # Clear synthetic evidence before releasing workers.
        old['inbox'].last_byte_at = time.monotonic()
        backup['inbox'].resync.clear()


@pytest.mark.parametrize('gap_seconds', [5.1, 18.7])
def test_failure_gap_uses_source_receipt_even_when_merged_receipt_is_fresh(feed_manager, gap_seconds):
    m, p, sockets = feed_manager
    backup = ready_backup(m, sockets)
    old = m.states[0]
    last = datetime.now(timezone.utc) - timedelta(seconds=gap_seconds)
    with m.lifecycle_lock:
        for asset, row in m.continuity.latest.items():
            at, _, _ = row[old['source']]
            row[old['source']] = (at, 'missing-overlap', last)
        # The normal live pipeline receipt would give the old, false ~22ms result.
        for asset, (at, _, source) in list(p.recording.metadata.items()):
            p.recording.metadata[asset] = (at, datetime.now(timezone.utc), source)
        sockets[0].callbacks['on_close'](sockets[0], None, 'lost')
    wait_for(lambda: m.states[0] is backup)
    event = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert event['takeover_gap_ms'] >= gap_seconds * 1000
    assert event['receipt_gap_ms'] == event['takeover_gap_ms']
    assert event['merged_receipt_gap_ms'] < 1000
    assert event['detection_to_takeover_ms'] < 1000
    assert m.continuity.pending_assets() == {'ay', 'an'}
    assert not event['gap_final']
    assert all(r['book_preserved'] for r in m.continuity.metrics()['events'] if r['status'] == 'unverified')
    # Final metric includes the wait for both restoring snapshots.
    sockets[1].emit([book('ay', '3'), book('an', '3')])
    wait_for(lambda: not m.continuity.pending_assets() and '_last_receipt' not in event)
    assert event['takeover_gap_ms'] >= gap_seconds * 1000
    assert event['recovery'] == 'backup'
    assert event['gap_final']


def test_real_backlog_remains_unready_independent_of_admission_budget(feed_manager):
    m, _, sockets = feed_manager
    backup = ready_backup(m, sockets)
    with m.lifecycle_lock:
        inbox = backup['inbox']
        inbox.queue_wait_seconds = 1.01
        assert not m.backup_ready(backup)
        assert 'queue_wait' in m.readiness_blockers(backup)
        m.redundant_receive(0, backup, [], datetime.now(timezone.utc), inbox)
        assert not backup['ready']
        inbox.queue_wait_seconds = .01
        # A large instantaneous TCP pending sample gates adding assets, not a
        # fully caught-up, current complete backup.
        sockets[1].transport = SimpleNamespace(last=dict(tcp_pending_bytes=100000))
        m.redundant_receive(0, backup, [], datetime.now(timezone.utc), inbox)
        assert m.backup_ready(backup) and m.prepare_stage(backup) == 'ready'
        assert 'tcp_pending' in m.subscriptions.pressure_reasons(backup)


def test_wire_queue_has_byte_budget_even_with_few_large_frames(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RECEIVE_QUEUE_BYTES', '1024')
    inbox = observer.ReceiveQueue(2048, 16384)
    assert inbox.put('x' * 100)
    assert inbox.queue.charged_bytes == 656
    assert not inbox.put('x' * 100)
    assert inbox.resync.is_set() and inbox.delivery_gap
    assert inbox.drop_reasons['overflow'] == 1 and inbox.queue.qsize() == 1
    inbox.discard()
    assert inbox.queue.charged_bytes == 0 and inbox.queue.empty()
    inbox.resync.clear()
    assert inbox.put('x' * 100)
    inbox.queue.get_nowait()
    assert inbox.queue.charged_bytes == 0


def test_merge_bounds_both_unique_events_and_repeated_snapshot_generations(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_MERGE_CACHE_MAX', '1024')
    merge = FeedMerge({})
    for i in range(5000):
        merge.unique(dict(event_type='last_trade_price', timestamp=i), 'primary')
    assert len(merge.seen) == 1024
    frozen = book('ay', at=1)
    for i in range(10000):
        merge.unique(frozen, 'feed-' + str(i))
    assert max(len(counts) for _, counts in merge.seen.values()) <= 4
    assert len(merge.seen) <= merge.maximum
    # Same-source trade multiplicity survives the smaller cache.
    trade = dict(event_type='last_trade_price', timestamp=99999, price='.4', size='1')
    for _ in range(3):
        assert merge.unique(trade, 'primary')
        assert not merge.unique(trade, 'backup')


def test_full_cache_does_not_evict_current_duplicate_or_miss_other_copy(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_MERGE_CACHE_MAX', '1024')
    m = FeedMerge({})
    events = [dict(timestamp=i) for i in range(1024)]
    for event in events:
        m.unique(event, 'primary')
    assert not m.unique(events[0], 'backup')
    assert not m.evictions and len(m.seen) == 1024


@pytest.mark.parametrize('envelope', [False, True])
def test_owned_price_changes_keep_shared_parse_and_fingerprints_when_unfiltered(envelope):
    event = delta(1000)
    if envelope:
        event = dict(type='price_change', payload={k: v for k, v in event.items() if k != 'event_type'})
    parsed = ws_payloads.public_event(event)
    assert recording.owned_events(parsed, {'ay'}) is parsed
    assert recording.owned_events(parsed, {'unowned'}) is not parsed


def test_exchange_book_hash_identity_avoids_serializing_level_payloads(monkeypatch):
    a = book('ay', at=1000)
    a['hash'] = 'exchange-hash'
    b = json.loads(json.dumps(a))
    seen = []
    original = ws_payloads.json.dumps
    monkeypatch.setattr(ws_payloads.json, 'dumps', lambda value, **kw: (seen.append(value), original(value, **kw))[1])
    assert ws_payloads.fingerprint(a) == ws_payloads.fingerprint(b)
    assert all(isinstance(value, tuple) for value in seen)
    for key, value in [('asset_id', 'other'), ('timestamp', 1001), ('hash', 'different')]:
        assert ws_payloads.fingerprint(dict(a, **{key: value})) != ws_payloads.fingerprint(a)
    del a['hash']
    assert ws_payloads.fingerprint(a) != ws_payloads.fingerprint(b)


def test_transport_wire_silence_updates_only_on_received_bytes():
    t = ws_transport.TransportDiagnostics()
    t.read(100, now=10)
    t.read(0, now=12)
    assert t.last_read_at == 10
    t.read(1, now=13)
    assert t.last_read_at == 13


def test_observer_sql_serialized_and_released_on_failure(monkeypatch):
    lock = threading.Lock()
    entered = threading.Event()
    release = threading.Event()
    active = peak = calls = 0
    @contextmanager
    def database():
        nonlocal active, peak, calls
        with lock:
            active += 1
            calls += 1
            peak = max(peak, active)
        try:
            entered.set()
            assert release.wait(2)
            yield object()
        finally:
            with lock:
                active -= 1
    monkeypatch.setattr(observer_db, 'get_db_connection', database)
    errors = []
    def work(fail=False):
        try:
            with observer_db.connection():
                if fail:
                    raise ValueError('retryable write failure')
        except ValueError:
            errors.append(1)
    threads = [threading.Thread(target=work, args=(i == 0,)) for i in range(6)]
    for thread in threads:
        thread.start()
    try:
        assert entered.wait(1)
        wait_for(lambda: observer_db.metrics()['waiting'] == 5)
        assert calls == 1 and observer_db.metrics()['active'] == 1
    finally:
        release.set()
        for thread in threads:
            thread.join(2)
    assert calls == 6 and peak == 1 and active == 0 and errors == [1]
    assert observer_db.metrics()['waiting'] == observer_db.metrics()['active'] == 0


def test_recovery_fanin_never_opens_database_on_workers(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    backup = ready_backup(m, sockets)
    def forbidden(*args, **kwargs):
        raise AssertionError('WS worker acquired database connection')
    monkeypatch.setattr(observer_db, 'get_db_connection', forbidden)
    sockets[0].callbacks['on_close'](sockets[0], None, 'lost')
    wait_for(lambda: m.states[0] is backup)
    sockets[1].emit([book('ay', '5'), book('an', '5')])
    wait_for(lambda: p.books['ay']['asks']['.4'] == '5')
    assert backup['worker'].is_alive()

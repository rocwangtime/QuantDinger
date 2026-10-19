"""Bounded spare backlog, independent snapshot compaction and retained bytes."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from app.services.polymarket import observer
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup


def detached(m, sockets):
    state = ready_backup(m, sockets)
    # Replace this generation so its old consumer cannot race the queue fixture.
    old = state['inbox']
    old.invalid.set()
    wait_for(lambda: not state['worker'].is_alive())
    state['inbox'] = inbox = observer.ReceiveQueue(1024)
    inbox.activated = {}
    return state, inbox


def enqueue(inbox, event, seconds=0):
    item = (json.dumps(event), datetime.now(timezone.utc)-timedelta(seconds=seconds), inbox.epoch)
    inbox.queue.put_nowait(item)
    return item


def test_compaction_replays_independent_full_books_and_suffix_without_http(feed_manager, monkeypatch, caplog):
    m, p, sockets = feed_manager
    state, inbox = detached(m, sockets)
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *a, **kw: calls.append(kw))
    at = int(datetime.now(timezone.utc).timestamp()*1000)+10
    obsolete = enqueue(inbox, dict(event_type='price_change', timestamp=at-10,
        price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='99')]), 1)
    anchor = enqueue(inbox, book('ay', '2', at=at), .1)
    enqueue(inbox, book('an', '3', at=at), .09)
    enqueue(inbox, dict(event_type='price_change', timestamp=at+1,
        price_changes=[dict(asset_id='ay', side='SELL', price='.4', size='4')]), .08)
    before = inbox.queue.charged_bytes
    assert m.compact_backup(0, state, inbox)
    assert inbox.epoch == 1 and inbox.compactions == 1 and inbox.resyncs == 0
    assert inbox.dropped == inbox.drop_reasons['superseded'] == 1
    assert inbox.queue.charged_bytes == before-observer.WireQueue.charge(obsolete)
    assert 'scope=historical_alert_only' in caplog.text
    assert not m.backup_ready(state) and not state['shadow'].books
    assert not m.hot_backup_coverage.rows['up_down:BTC:5m']['ready']
    assert inbox.queue.queue[0][1] == anchor[1]  # Original receipt survives.
    while not inbox.queue.empty():
        raw, received, epoch = inbox.queue.get_nowait()
        assert epoch == inbox.epoch
        m.redundant_receive(0, state, m.parsed_frames.parse(raw), received, inbox, epoch)
    assert m.backup_ready(state)
    assert state['shadow'].books['ay']['asks']['.4'] == p.books['ay']['asks']['.4'] == '4'
    m.tick()
    assert not calls


@pytest.mark.parametrize('blocker', ['missing', 'old', 'timestamp', 'older_watermark',
    'invalid', 'resync', 'loss', 'primary', 'retired', 'no_prefix', 'malformed'])
def test_compaction_never_discards_without_valid_independent_anchors(feed_manager, blocker):
    m, _, sockets = feed_manager
    state, inbox = detached(m, sockets)
    at = int(datetime.now(timezone.utc).timestamp()*1000)+20
    if blocker != 'no_prefix':
        enqueue(inbox, {}, 1)
    books = [book('ay', at=at), book('an', at=at)]
    if blocker == 'missing':
        books.pop()
    if blocker == 'timestamp':
        books[0].pop('timestamp')
    if blocker == 'older_watermark':
        books[0]['timestamp'] = 1000
    enqueue(inbox, books, .6 if blocker == 'old' else .1)
    if blocker == 'malformed':
        inbox.queue.put_nowait(('{', datetime.now(timezone.utc), inbox.epoch))
    if blocker in ('invalid', 'resync'):
        getattr(inbox, blocker).set()
    if blocker == 'loss':
        inbox.delivery_gap = True
    if blocker == 'primary':
        state['standby'] = False
    if blocker == 'retired':
        state['retired'] = True
    before = list(inbox.queue.queue), inbox.queue.charged_bytes, state['shadow']
    assert not m.compact_backup(0, state, inbox)
    assert (list(inbox.queue.queue), inbox.queue.charged_bytes, state['shadow']) == before
    assert inbox.epoch == inbox.compactions == inbox.dropped == 0


def test_lag_without_recent_snapshot_is_bounded_and_requires_real_repair(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    state = ready_backup(m, sockets)
    inbox = state['inbox']
    calls = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda *a, **kw: calls.append(kw) or False)
    with m.lifecycle_lock, inbox.lock:
        for _ in range(30):
            enqueue(inbox, {}, 1.6)
    wait_for(lambda: inbox.resyncs > 0)
    assert not m.backup_ready(state) and not state['shadow'].books
    assert inbox.dropped and inbox.queue.charged_bytes == 0
    m.tick()
    assert calls
    sockets[1].emit([book('ay', '7'), book('an', '7')])
    wait_for(lambda: m.backup_ready(state))


def test_backup_raw_byte_cap_and_release_across_repeated_pressure(feed_manager):
    m, _, sockets = feed_manager
    state = ready_backup(m, sockets)
    assert state['inbox'].queue.byte_limit == 2*1024*1024
    assert m.states[0]['inbox'].queue.byte_limit == 16*1024*1024
    for _ in range(30):
        inbox = observer.ReceiveQueue(10000)
        inbox.queue.byte_limit = 2*1024*1024
        while inbox.put('x'*10000):
            assert inbox.queue.charged_bytes <= 2*1024*1024
        assert inbox.resync.is_set() and inbox.drop_reasons['overflow'] == 1
        inbox.discard()
        assert not inbox.queue.queue and inbox.queue.charged_bytes == 0


def test_promoted_backup_restores_primary_byte_budget(feed_manager):
    m, _, sockets = feed_manager
    state = ready_backup(m, sockets)
    assert state['inbox'].queue.byte_limit == 2*1024*1024
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is state)
    assert state['inbox'].queue.byte_limit == state['inbox'].primary_byte_limit == 16*1024*1024


def test_popped_frame_from_before_compaction_cannot_rebuild_or_publish(feed_manager):
    m, p, sockets = feed_manager
    state, inbox = detached(m, sockets)
    before = dict(p.books['ay']['asks'])
    at = int(datetime.now(timezone.utc).timestamp()*1000)+20
    enqueue(inbox, {}, 1)
    enqueue(inbox, [book('ay', at=at), book('an', at=at)], .1)
    assert m.compact_backup(0, state, inbox)
    m.redundant_receive(0, state, book('ay', '999', at=at+1), datetime.now(timezone.utc), inbox, 0)
    assert not state['shadow'].books and p.books['ay']['asks'] == before


def test_http_snapshot_started_before_compaction_is_fenced(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    state, inbox = detached(m, sockets)
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish:
                        callbacks.append(publish) or True)
    assert m.request_snapshot(0, state, {'ay', 'an'})
    at = int(datetime.now(timezone.utc).timestamp()*1000)+20
    enqueue(inbox, {}, 1)
    enqueue(inbox, [book('ay', at=at), book('an', at=at)], .1)
    assert m.compact_backup(0, state, inbox)
    before = dict(p.books['ay']['asks'])
    callbacks[0]([book('ay', '999', at=at+1)], datetime.now(timezone.utc))
    assert not state['shadow'].books and p.books['ay']['asks'] == before


def test_worker_replay_under_backup_pressure_keeps_books_coverage_and_memory_bounds():
    from scripts.polymarket_ws_worker_replay import run
    result = run(seconds=2, rate=2400, backup_service_ms=4)
    assert result['valid'] and result['compactions'] > 0
    assert result['resyncs'] == result['http_attempts'] == 0
    assert min(result['ready_by_shard'].values()) >= .9
    assert result['peak_backup_queue_ms'] <= 2000
    assert max(row['queue_charged_bytes'] for row in result['memory_samples']) <= 8*1024*1024

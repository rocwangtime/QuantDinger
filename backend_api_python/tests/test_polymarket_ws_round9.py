"""Round 9: rollover ownership, pair locality and snapshot-gated migrations."""
import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.services.polymarket import observer
from tests.test_polymarket_ws_balance import balancer, connected_manager, finish_migration
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for


def commands(socket, operation):
    return [c for raw in socket.sent if raw != 'PING'
            if (c := json.loads(raw)).get('operation') == operation]


def start_move(m, sockets):
    m.count = m.balancer.count = 2
    before = dict(m.balancer.assignment)
    m.apply_assignment(dict(ay=1, an=1))
    assert sockets[1].opened.wait(1)
    assert m.migration and m.balancer.assignment == before
    return m.states[0], m.states[1]


def test_rollover_assigns_missing_group_and_unsubscribes_old_episode(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old = {**m.markets[0], 'market_type': 'up_down', 'asset': 'BTC', 'interval': '5m'}
    m.sync([old])
    key = observer.balance_key(old)
    m.balancer.owners.clear()  # No applied assets survive the episode transition.
    new = {**old, 'id': 'new', 'yesAssetId': 'ny', 'noAssetId': 'nn'}
    # Reproduce the stale/incomplete planning result at assignment_for: both
    # assets of the newly published group are missing from the proposed owners.
    monkeypatch.setattr(m, 'capacity_owners', lambda: {})
    p.recording.enabled = False
    p.set_markets([new])
    m.sync([new])
    assert set(m.balancer.current_owners()) == {key}
    assert m.balancer.assignment == dict(ny=0, nn=0)
    assert any(set(c['assets_ids']) == {'ay', 'an'} for c in commands(sockets[0], 'unsubscribe'))
    m.subscriptions.next_at = 0
    m.tick()
    assert any(set(c['assets_ids']) == {'ny', 'nn'} for c in commands(sockets[0], 'subscribe'))
    assert not m.states[0]['inbox'].invalid.is_set()


def test_assignment_completion_keeps_known_owners_and_covers_every_group(monkeypatch):
    m = observer.MarketConnections()
    m.balancer.set_groups(dict(a='old', y='up_down:BTC:5m:outcome:yes',
                               n='up_down:BTC:5m:outcome:no'))
    proposed = {'old': 2}
    result = m.assignment_for(proposed)
    assert set(result) == set(m.balancer.groups)
    assert result['a'] == 2 and proposed == {'old': 2}


def test_migration_waits_for_both_snapshots_and_catches_up_without_clearing_books(feed_manager):
    m, p, sockets = feed_manager
    old, target = start_move(m, sockets)
    original = dict(p.books)
    sockets[1].emit(book('ay', '2'))
    wait_for(lambda: 'ay' in m.migration['shadows'][1].books)
    assert p.books == original and m.balancer.assignment == dict(ay=0, an=0)
    assert not commands(sockets[0], 'unsubscribe')
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + 20
    sockets[0].emit([book('ay', '3', at), book('an', '3', at)])
    wait_for(lambda: p.books['an']['asks']['.4'] == '3')
    sockets[1].emit(book('an', '2', at - 1))
    wait_for(lambda: len(m.migration['shadows'][1].books) == 2)
    assert m.migration and m.balancer.assignment == dict(ay=0, an=0)
    # Observe the instant the old unsubscribe is sent: ownership and both full
    # books must already be published; neither asset is ever removed to warm up.
    send = sockets[0].send
    seen = []
    def inspect(raw):
        if json.loads(raw).get('operation') == 'unsubscribe':
            seen.append((dict(m.balancer.assignment), set(p.books)))
        send(raw)
    sockets[0].send = inspect
    sockets[1].emit([book('ay', '4', at), book('an', '4', at)])
    wait_for(lambda: m.migration is None)
    assert seen == [(dict(ay=1, an=1), {'ay', 'an'})]
    assert p.books['ay']['asks']['.4'] == p.books['an']['asks']['.4'] == '4'
    assert old['connected'] and target['connected']
    assert not m.continuity.pending_assets()
    # Late former-owner frames cannot overwrite the new books.
    sockets[0].emit([book('ay', '99', at + 1), book('an', '99', at + 1)])
    sockets[1].emit(book('ay', '5', at + 2))
    wait_for(lambda: p.books['ay']['asks']['.4'] == '5')
    assert p.books['an']['asks']['.4'] == '4'


@pytest.mark.parametrize('failure', ['timeout', 'disconnect', 'resync', 'generation'])
def test_failed_migration_keeps_old_owners_and_live_books(feed_manager, failure):
    m, p, sockets = feed_manager
    old, target = start_move(m, sockets)
    before = dict(p.books)
    if failure == 'timeout':
        m.migration['created_at'] -= 91
    elif failure == 'disconnect':
        sockets[1].callbacks['on_close'](sockets[1], 1013, 'slow consumer: send buffer full')
    elif failure == 'resync':
        target['inbox'].resync.set()
    else:
        m.migration['generations'][1] = object()
    m.migration_tick(observer.time.monotonic())
    assert m.migration is None and m.migration_failures == 1
    assert m.balancer.assignment == dict(ay=0, an=0)
    assert p.books == before and old['connected']
    assert not commands(sockets[0], 'unsubscribe')
    assert not m.apply_assignment(dict(ay=1, an=1))  # Failed prepare also cools down.


def test_pre_subscription_books_cannot_complete_migration(feed_manager):
    m, _, sockets = feed_manager
    _, target = start_move(m, sockets)
    target['inbox'].activated = {a: datetime.now(timezone.utc) for a in target['assets']}
    m.migration_receive(1, target, [book('ay'), book('an')],
                        datetime.now(timezone.utc) - timedelta(seconds=.5), target['inbox'])
    assert m.migration and not m.migration['shadows'][1].books
    assert not commands(sockets[0], 'unsubscribe')


def test_multi_destination_swap_commits_only_after_every_pair_is_warm(monkeypatch):
    markets = [dict(id=k, yesAssetId=k+'y', noAssetId=k+'n') for k in 'ab']
    m, sent = connected_manager(monkeypatch, markets, {'market:a': 0, 'market:b': 1}, {})
    m.apply_assignment(dict(ay=1, an=1, by=0, bn=0))
    first = m.states[0]
    m.subscriptions.drain(first, observer.time.monotonic())
    m.migration_receive(0, first, [book('by'), book('bn')], datetime.now(timezone.utc), first['inbox'])
    assert m.balancer.assignment == dict(ay=0, an=0, by=1, bn=1)
    assert all(c['operation'] != 'unsubscribe' for _, c in sent)
    finish_migration(m)
    assert m.balancer.assignment == dict(ay=1, an=1, by=0, bn=0)
    assert [c['operation'] for _, c in sent] == ['subscribe', 'subscribe', 'unsubscribe', 'unsubscribe']


def test_rollover_does_not_reoptimize_overloaded_retained_groups(monkeypatch):
    markets = [dict(id=k, yesAssetId=k+'y', noAssetId=k+'n') for k in 'abcd']
    owners = {f'market:{k}': 0 for k in 'abcd'}
    m, _ = connected_manager(monkeypatch, markets, owners, {k: 150 for k in owners})
    m.balancer.owners = dict(owners)
    for _ in range(10):
        assert m.capacity_owners() == owners
    assert m.balancer.last_rebalance == float('-inf')


def test_overload_does_not_bypass_group_or_global_cooldown(monkeypatch):
    owners = dict(a=0, b=0, c=1, d=1)
    b = balancer(monkeypatch, owners, dict(a=300, b=200, c=100, d=80), count=2)
    b.group_cooldown, b.minimum = 300, 120
    b.applied(dict(a=1, b=1, c=0, d=0), owners, 0)
    assert b.overloaded(owners)
    assert b.plan(owners, 119) is None
    assert b.plan(owners, 120) is None
    b.set_groups({k+'new': k for k in owners}, 200)
    assert b.plan(owners, 299) is None
    assert b.plan(owners, 300) is not None


def test_primary_1013_counts_once_per_generation_and_survives_shard_reconnect(monkeypatch):
    m = observer.MarketConnections()
    clock = [0]
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    for at in (0, 411, 559):  # Inspection's three kicks within nine minutes.
        clock[0] = at
        state = dict(assets=['y', 'n'], connected=False, lock=threading.RLock())
        m.record_primary_1013(2, state, 1013)
        m.record_primary_1013(2, state, 1013)  # on_error then on_close.
    row = m.primary_1013_metrics(559)[2]
    assert row['total'] == row['count_10m'] == 3
    assert row['assets'] == 2 and row['measured_bytes_per_s'] == 0
    assert m.primary_1013_metrics(601)[2]['count_10m'] == 2
    m.record_primary_1013(2, dict(standby=True), 1013)
    m.record_primary_1013(2, dict(retired=True), 1013)
    m.record_primary_1013(2, {}, 1001)
    assert m.primary_1013_metrics(1200)[2]['count_10m'] == 0
    assert m.primary_1013_metrics(1200)[2]['total'] == 3


def test_last_single_leg_snapshot_finishes_without_wire_resnapshot(feed_manager):
    m, _, sockets = feed_manager
    start_move(m, sockets)
    sockets[1].emit(book('ay', '2'))
    wait_for(lambda: 'ay' in m.migration['shadows'][1].books)
    sockets[1].emit(book('an', '2'))
    wait_for(lambda: m.migration is None)
    assert not m.continuity.pending_assets()
    m.tick()
    assert not commands(sockets[1], 'unsubscribe')


def test_live_watermark_survives_source_book_invalidation(feed_manager):
    m, p, sockets = feed_manager
    start_move(m, sockets)
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + 10
    sockets[0].emit([book('ay', '3', at), book('an', '3', at)])
    wait_for(lambda: p.books['an']['asks']['.4'] == '3')
    # Emulate source failure clearing published metadata after its final update.
    with p.lock:
        p.books.clear()
        p.recording.metadata.clear()
    sockets[1].emit([book('ay', '2', at - 1), book('an', '2', at - 1)])
    wait_for(lambda: len(m.migration['shadows'][1].books) == 2)
    assert m.balancer.assignment == dict(ay=0, an=0)
    sockets[1].emit([book('ay', '4', at), book('an', '4', at)])
    wait_for(lambda: m.migration is None)
    assert p.books['an']['asks']['.4'] == '4'


def test_small_excess_on_several_shards_does_not_accumulate_into_a_trigger(monkeypatch):
    owners = {k: i for i, k in enumerate('abcd')}
    b = balancer(monkeypatch, owners, {k: 310 for k in owners})
    assert b.capacity_cost(owners)[1] == 40
    assert not b.overloaded(owners)
    assert b.plan(owners, 60) is None
    assert b.plan(owners, 120) is None

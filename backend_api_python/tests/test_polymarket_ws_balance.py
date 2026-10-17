"""Measured group ownership, bounded churn, and subscription handover contracts."""
import json
import threading
import time

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_balance import DailyStats, RateBalancer


def balancer(monkeypatch, owners, rates, count=4):
    """Groups ``k`` with assets ``ky``/``kn`` on ``owners[k]`` and measured ``rates``."""
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_SECONDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_MIN_SECONDS', '10')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_CONFIRMATIONS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_MIN_MSG_PER_S', '.1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_GROUP_COOLDOWN_SECONDS', '0')
    b = RateBalancer(count)
    b.set_groups({k + side: k for k in owners for side in 'yn'})
    b.assignment = {a: owners[k] for a, k in b.groups.items()}
    b.rates, b.measured = dict(rates), set(rates)
    b.sample_at = b.last_check = 0
    return b


def settle(b, owners, rounds=10):
    for _ in range(rounds):
        target = b.plan(owners)
        if not target:
            return owners
        assert sum(target[k] != owners[k] for k in target) <= b.max_moves
        owners = target
    raise AssertionError('planner did not converge')


def test_plan_moves_whole_groups_within_budget(monkeypatch):
    owners = {str(i): 0 for i in range(8)}
    b = balancer(monkeypatch, owners, {k: 10 for k in owners})
    final = settle(b, owners)
    assert set(final) == set(owners)
    assert b.loads(final) == [20] * 4
    assert b.metrics()['ws_load_imbalance'] == 4  # Applied owners are unchanged.


def test_plan_acts_only_outside_band(monkeypatch):
    # Within REBALANCE_RATIO nothing moves.
    b = balancer(monkeypatch, dict(a=0, b=1, c=2, d=3), dict(a=10, b=10, c=10, d=14))
    assert b.plan(dict(a=0, b=1, c=2, d=3)) is None
    # 2.4 is the best possible layout: above the target, but nothing to gain.
    owners = dict(a=0, b=1, c=2, d=3)
    b = balancer(monkeypatch, owners, dict(a=12, b=5, c=5, d=5))
    assert b.ideal(owners) == pytest.approx(2.4)
    assert b.plan(owners) is None
    # A tiny gain in max/min is not a reason to churn near the peak lower bound.
    owners = dict(a=0, b=1, c=2, d=3, e=3, f=0)
    b = balancer(monkeypatch, owners, dict(a=10, b=10, c=10, d=4, e=1, f=1))
    assert max(b.loads(owners)) / min(b.loads(owners)) > b.target
    final = settle(b, owners)
    assert final == owners


def test_unavoidable_hot_group_does_not_churn(monkeypatch):
    owners = dict(a=0, b=1, c=2, d=3)
    b = balancer(monkeypatch, owners, dict(a=100, b=1, c=1, d=1))
    assert b.plan(owners) is None


def test_ingress_cost_split_and_series_rate_survives_rollover(monkeypatch):
    key = 'up_down:BTC:5m'
    b = RateBalancer(2)
    b.set_groups({'w0y': key, 'w0n': key, 'm1y': 'market:1', 'm1n': 'market:1'})
    b.assignment = dict(w0y=0, w0n=0, m1y=1, m1n=1)
    b.sample_at = 0
    b.since = {key: 0, 'market:1': 0}
    frame = dict(event_type='price_change', price_changes=[
        dict(asset_id='w0y'), dict(asset_id='w0y'), dict(asset_id='w0n')])
    for _ in range(30):
        b.receive(frame, 0)
    b.receive(json.dumps(frame), 1)  # A former owner's duplicate is ignored.
    assert b.pending == {key: 30}
    # The window expires before the sample; its frames still count for the series.
    b.set_groups({'w1y': key, 'w1n': key, 'm1y': 'market:1', 'm1n': 'market:1'})
    b.assignment = dict(w1y=0, w1n=0, m1y=1, m1n=1)
    b.receive(dict(event_type='book', asset_id='w1y'), 0)
    assert b.sample(1)
    assert b.rates == {key: 31, 'market:1': 0}
    assert b.sample(2) and 0 < b.rates[key] < 31


@pytest.mark.parametrize('raw', ['oops', 'null', '[]', '{}', b'\xff\xfe'])
def test_non_token_payloads_do_not_affect_rates(monkeypatch, raw):
    b = balancer(monkeypatch, {'a': 0}, {'a': 0})
    b.receive(raw, 0)
    assert b.pending == {}


def manager(monkeypatch):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_DAILY_STATS', DailyStats())
    m = observer.MarketConnections()
    sent = []
    class Socket:
        def __init__(self, index):
            self.index = index
        def send(self, raw):
            sent.append((self.index, json.loads(raw)))
        def close(self):
            pass
    m.states = {i: dict(assets=['a', 'b'] if i == 0 else [], socket=Socket(i),
                       connected=True, lock=threading.RLock(), inbox=observer.ReceiveQueue(20),
                       source=f'fake-{i}') for i in range(4)}
    m.balancer.assignment = dict(a=0, b=0)
    return m, sent


def finish_migration(m):
    """Drive paced fake destination subscriptions and deliver full snapshots."""
    from datetime import datetime, timezone
    from tests.test_polymarket_ws_rotation import book
    if not m.migration:
        return
    for index, shadow in list(m.migration['shadows'].items()):
        state = m.states[index]
        while state.get('subscribe_pending'):
            m.subscriptions.next_at = 0
            m.subscriptions.drain(state, time.monotonic())
        m.migration_receive(index, state, [book(a) for a in shadow.assets],
                            datetime.now(timezone.utc), state['inbox'])
    assert m.migration is None


def test_handover_sends_deltas_without_invalidating_generations(monkeypatch):
    m, sent = manager(monkeypatch)
    monkeypatch.setattr(observer, '_PIPELINE', None)
    m.states[0]['inbox'] = observer.ReceiveQueue(2)
    m.states[2]['inbox'] = observer.ReceiveQueue(2)
    assert m.apply_assignment(dict(a=0, b=2))
    assert m.balancer.assignment == dict(a=0, b=0) and sent == []
    finish_migration(m)
    assert sent == [(2, dict(assets_ids=['b'], operation='subscribe')),
                    (0, dict(assets_ids=['b'], operation='unsubscribe'))]
    assert not m.states[0]['inbox'].invalid.is_set()
    assert not m.states[2]['inbox'].invalid.is_set()
    assert m.states[0]['connected'] and m.states[2]['connected']
    assert m.balancer.assignment == dict(a=0, b=2)


def test_utc_counter_rollover_and_checkpoints(monkeypatch):
    stats = DailyStats()
    stats.increment('reconnect_count', day='2026-10-12')
    stats.increment('rebalance_count', day='2026-10-13')
    stats.increment('message', shard=3, day='2026-10-13')
    assert stats.days['2026-10-12']['reconnect_count'] == 1
    assert stats.days['2026-10-13']['shard_msg_totals'] == {'3': 1}


def connected_manager(monkeypatch, markets, owners, rates):
    """MarketConnections with live fake sockets and measured group rates."""
    from types import SimpleNamespace
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_SECONDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_MIN_SECONDS', '10')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_CONFIRMATIONS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_MIN_MSG_PER_S', '.1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_GROUP_COOLDOWN_SECONDS', '0')
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_DAILY_STATS', DailyStats())
    monkeypatch.setattr(observer, '_PIPELINE', None)
    m = observer.MarketConnections()
    sent = []

    class Socket:
        def __init__(self, index):
            self.index = index

        def send(self, raw):
            sent.append((self.index, json.loads(raw)))

        def close(self):
            pytest.fail('balancing closed a socket')
    m.markets = list(markets)
    m.balancer.set_groups({a: observer.balance_key(x) for x in markets for a in observer.subscription_assets([x])})
    assignment = {a: owners[observer.balance_key(x)] for x in markets for a in observer.subscription_assets([x])}
    m.states = {i: dict(assets=[a for a in assignment if assignment[a] == i], socket=Socket(i), connected=True,
                        lock=threading.RLock(), inbox=observer.ReceiveQueue(20), source=f'fake-{i}',
                        thread=SimpleNamespace(is_alive=lambda: True), ping_at=float('inf'))
                for i in range(4)}
    m.balancer.assignment = assignment
    m.balancer.rates, m.balancer.measured = dict(rates), set(rates)
    m.balancer.sample_at = m.balancer.last_check = 0
    m.balancer.pending = {k: v for k, v in rates.items()}  # One-second sample keeps the rates.
    return m, sent


def test_reconnect_does_not_force_short_biased_sample(monkeypatch):
    from types import SimpleNamespace
    markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n') for i in range(8)]
    m, _ = connected_manager(monkeypatch, markets, {f'market:{i}': 0 for i in range(8)},
                             {f'market:{i}': 10 for i in range(8)})
    m.balancer.interval = 60
    m.balancer.last_check = 1
    monkeypatch.setattr(observer, 'time', SimpleNamespace(process_time=time.process_time, monotonic=lambda: 1))
    m.tick()
    assert m.balancer.last_rebalance == float('-inf')
    m.reconnect_pending = True
    m.tick()
    assert m.balancer.last_rebalance == float('-inf')
    assert m.balancer.last_check == 1


def test_manager_retains_market_locality_on_connected_tick(monkeypatch):
    from types import SimpleNamespace
    m, _ = manager(monkeypatch)
    m.markets = [dict(id='m', yesAssetId='a', noAssetId='b')]
    m.balancer.set_groups(dict(a='market:m', b='market:m'))
    m.reconnect_pending = True
    for state in m.states.values():
        state.update(thread=SimpleNamespace(is_alive=lambda: True), ping_at=float('inf'))
    monkeypatch.setattr(observer, 'time', SimpleNamespace(process_time=time.process_time, monotonic=lambda: 1))
    m.tick()
    assert m.balancer.assignment['a'] == m.balancer.assignment['b']
    assert not m.stats.days


def test_manager_applies_weighted_whole_group_rebalance_and_cooldown(monkeypatch):
    from types import SimpleNamespace
    markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n') for i in range(16)]
    owners = {f'market:{i}': i % 4 for i in range(16)}
    rates = {k: (7 if owner == 3 else 1) for k, owner in owners.items()}
    m, sent = connected_manager(monkeypatch, markets, owners, rates)
    b = m.balancer
    clock = [1]
    monkeypatch.setattr(observer, 'time', SimpleNamespace(process_time=time.process_time, monotonic=lambda: clock[0]))
    before = b.loads()
    m.tick()
    finish_migration(m)
    assert max(b.loads()) / min(b.loads()) < max(before) / min(before)
    assert b.last_rebalance == 1
    assert next(iter(m.stats.days.values()))['rebalance_count'] == 1
    moved = {a for _, command in sent for a in command['assets_ids']}
    assert 0 < len(moved) <= 2 * b.max_moves
    for market in markets:
        assert b.assignment[market['yesAssetId']] == b.assignment[market['noAssetId']]
    applied = dict(b.assignment)
    # A repeated catalogue refresh must not undo the assignment or bypass cooldown.
    m.sync(markets)
    clock[0] = 5
    m.tick()
    assert b.assignment == applied
    # After the cooldown the next bounded round continues towards the target.
    for second in range(12, 60, 11):
        clock[0] = second
        b.pending = dict(b.rates)
        b.sample_at = second - 1
        m.tick()
        finish_migration(m)
    assert max(b.loads()) / min(b.loads()) <= b.target


def test_swaps_escape_single_move_local_minimum(monkeypatch):
    # Two hot markets per shard require exchanges, not high-to-low moves.
    weights = [8, 7, 6, 5, 4, 3, 2, 1]
    owners = {str(i): i // 2 for i in range(8)}
    b = balancer(monkeypatch, owners, {str(i): w for i, w in enumerate(weights)})
    b.trigger = b.target = 1
    b.threshold = 0
    b.min_gain = 0
    final = settle(b, owners)
    assert b.loads(final) == [9] * 4


def test_checkpoint_failure_keeps_dirty_day_for_retry(monkeypatch):
    from app.services.polymarket import ws_balance
    stats = DailyStats()
    stats.increment('reconnect_count', day='2026-10-13')
    class Broken:
        def __enter__(self):
            raise RuntimeError('database unavailable')
        def __exit__(self, *args):
            pass
    monkeypatch.setattr(ws_balance, 'get_db_connection', Broken)
    with pytest.raises(RuntimeError, match='database unavailable'):
        stats.persist()
    assert stats.dirty == {'2026-10-13'}
    assert stats.days['2026-10-13']['reconnect_count'] == 1



def test_rollover_keeps_retained_books_and_queue(monkeypatch):
    from datetime import datetime, timezone, timedelta
    m, sent = manager(monkeypatch)
    p = observer.Pipeline()
    monkeypatch.setattr(observer, '_PIPELINE', p)
    p.set_markets([dict(id='keep', yesAssetId='a', noAssetId='b')])
    p.receive(dict(event_type='book', asset_id='a', bids=[], asks=[]))
    retained = p.books['a']
    for state in m.states.values():
        state['inbox'] = observer.ReceiveQueue(4)
        state['socket'].close = lambda: pytest.fail('rollover closed socket')
    inbox = m.states[0]['inbox']
    inbox.put('queued retained delta')
    for i in range(3):
        m.apply_assignment(dict(a=0, b=0, **{f'new{i}': 1}))
        assert p.books['a'] is retained
        assert inbox.queue.qsize() == 1
        assert not inbox.invalid.is_set()
    moved = m.states[1]['inbox']
    old = datetime.now(timezone.utc) - timedelta(seconds=1)
    p.receive(dict(event_type='book', asset_id='new2', bids=[], asks=[]),
              {'new2'}, old, moved, moved.epoch)
    assert 'new2' not in p.books
    assert not any(command['assets_ids'] == ['a', 'b'] for _, command in sent)


def test_catalogue_owner_moves_count_once_and_wire_failure_is_local(monkeypatch):
    m, _ = manager(monkeypatch)
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS, rebalance_count=0))
    closed = []
    m.states[0]['inbox'] = observer.ReceiveQueue(4)
    m.states[0]['socket'].send = lambda raw: (_ for _ in ()).throw(OSError('fake wire failure'))
    m.states[0]['socket'].close = lambda: closed.append(0)
    for i in (1, 2, 3):
        m.states[i]['socket'].close = lambda: pytest.fail('unrelated socket closed')
    m.apply_assignment(dict(a=0, b=2))
    finish_migration(m)
    assert closed == [0]
    assert m.states[0]['inbox'].invalid.is_set()
    assert m.states[2]['connected']
    assert observer.STATUS['rebalance_count'] == 1
    assert next(iter(m.stats.days.values()))['rebalance_count'] == 1
    m.apply_assignment(dict(a=0, b=2))
    assert observer.STATUS['rebalance_count'] == 1


def test_hot_connections_are_isolated_without_chasing_max_min(monkeypatch):
    owners = dict(a=0, b=0, c=1, d=2, e=3)
    b = balancer(monkeypatch, owners, dict(a=367.434, b=148.030, c=114.782, d=30, e=22))
    final = settle(b, owners)
    assert max(b.loads(final)) == pytest.approx(367.434)
    assert b.hot_groups(final) == {'a', 'b', 'c'}
    assert all(sum(shard == final[k] for shard in final.values()) == 1 for k in ('a', 'b', 'c'))
    assert max(b.loads(final)) / min(b.loads(final)) > 7
    assert b.plan(final) is None


def test_rebalance_needs_sustained_pressure_and_real_gain(monkeypatch):
    owners = {str(i): 0 for i in range(8)}
    b = balancer(monkeypatch, owners, {k: 20 for k in owners})
    b.confirmations = 2
    assert b.plan(owners, 60) is None
    assert b.plan(owners, 120) is not None
    # A single spike that disappears resets the confirmation streak.
    balanced = {k: int(k) % 4 for k in owners}
    assert b.plan(balanced, 180) is None
    assert b.plan(owners, 240) is None
    # A 1% peak reduction does not pass the 10% actual-improvement band.
    b.confirmations = 1
    b.max_moves = 1
    b.rates = {k: 100 if k == '0' else 1 for k in owners}
    b.hot_rate = 1000
    owners = {k: 0 if int(k) < 3 else int(k) % 3 + 1 for k in owners}
    assert b.plan(owners, 300) is None


def test_global_and_per_group_cooldowns_survive_rollover(monkeypatch):
    owners = dict(a=0, b=0, c=1, d=1)
    b = balancer(monkeypatch, owners, dict(a=300, b=200, c=100, d=80), count=2)
    b.group_cooldown, b.minimum = 300, 120
    b.max_load = 1000  # Exercise ordinary cooldown, outside capacity recovery.
    b.applied(dict(a=1, b=1, c=0, d=0), owners, 0)
    assert b.plan(owners, 119) is None
    assert b.plan(owners, 120) is None  # All groups are still in their cooldown.
    b.set_groups({k + 'new': k for k in owners}, 200)
    b.assignment = {k + 'new': shard for k, shard in owners.items()}
    assert b.plan(owners, 299) is None
    assert b.plan(owners, 300) is not None


def test_missing_delivery_and_partial_recovery_do_not_decay_hot_rate(monkeypatch):
    b = balancer(monkeypatch, dict(hot=0, quiet=1), dict(hot=367, quiet=10), count=2)
    b.since = dict(hot=0, quiet=0)
    b.pending = {'quiet': 600}
    b.sample(60, {'hot'})
    assert b.rates['hot'] == 367
    b.pending = dict(hot=100, quiet=600)  # Only part of a recovery window arrived.
    b.sample(120)
    assert b.rates['hot'] == 367
    b.pending = dict(hot=60 * 367, quiet=600)
    b.sample(180)
    assert b.rates['hot'] == 367


@pytest.mark.parametrize('count', [1, 4, 6, 8])
def test_idle_connections_and_indivisible_hot_group_are_stable(monkeypatch, count):
    owners = {'hot': 0, 'cold': min(1, count - 1)}
    b = balancer(monkeypatch, owners, dict(hot=367, cold=1), count=count)
    assert b.plan(owners) is None
    assert b.metrics()['ws_peak_lower_bound'] == (368 if count == 1 else 367)


def test_hot_classification_has_an_exit_band(monkeypatch):
    owners = dict(a=0, b=1, c=2, d=3)
    b = balancer(monkeypatch, owners, dict(a=101, b=99, c=99, d=99))
    assert b.hot_groups(owners) == {'a'}
    b.rates['a'] = 99
    assert b.hot_groups(owners) == {'a'}
    b.rates['a'] = 89
    assert b.hot_groups(owners) == set()


def test_retired_groups_do_not_accumulate_or_bias_unknown_weights(monkeypatch):
    b = balancer(monkeypatch, dict(old=0, series=1), dict(old=10000, series=10))
    b.pending = dict(old=30, series=20)
    b.moved_at = dict(old=0, series=1)
    b.set_groups({'new-window-y': 'series', 'new-market-y': 'new'})
    assert b.rates == {'series': 10}
    assert b.pending == {'series': 20}
    assert b.moved_at == {'series': 1}
    assert b.weight('new') == 10


def test_slow_subscription_send_does_not_lock_retained_books_or_accept_early_target(monkeypatch):
    from datetime import datetime, timezone
    m, _ = manager(monkeypatch)
    p = observer.Pipeline()
    monkeypatch.setattr(observer, '_PIPELINE', p)
    for i in (0, 1):
        m.states[i]['inbox'] = observer.ReceiveQueue(20)
    p.books['a'] = {'bids': {}, 'asks': {}}
    entered, release = threading.Event(), threading.Event()

    def slow_send(raw):
        entered.set()
        assert release.wait(3)
    m.states[1]['socket'].send = slow_send
    m.apply_assignment(dict(a=0, b=1))
    thread = threading.Thread(target=lambda: m.subscriptions.drain(m.states[1], time.monotonic()), daemon=True)
    thread.start()
    try:
        assert entered.wait(1)
        assert p.lock.acquire(timeout=.3), 'Network send holds the shared book lock'
        try:
            inbox = m.states[1]['inbox']
            p.receive(dict(event_type='book', asset_id='b', bids=[], asks=[]),
                      {a for a, owner in m.balancer.assignment.items() if owner == 1},
                      datetime.now(timezone.utc), inbox, inbox.epoch)
            assert 'b' not in p.books  # Target has not obtained a full snapshot.
            assert 'a' in p.books
        finally:
            p.lock.release()
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive()
    m.states[1]['socket'].send = lambda raw: None
    finish_migration(m)
    inbox = m.states[1]['inbox']
    p.receive(dict(event_type='book', asset_id='b', bids=[], asks=[]),
              {'b'}, datetime.now(timezone.utc), inbox, inbox.epoch)
    assert 'b' in p.books


def test_manager_freezes_plans_through_outage_and_recovery_window(monkeypatch):
    from types import SimpleNamespace
    markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n') for i in range(4)]
    owners = {f'market:{i}': i for i in range(4)}
    m, _ = connected_manager(monkeypatch, markets, owners, {k: 100 for k in owners})
    b = m.balancer
    b.since = {k: 0 for k in owners}
    clock, calls = [1], []
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(b, 'plan', lambda owners, now: calls.append(now))
    m.states[0]['connected'] = False
    m.tick()
    assert b.rates['market:0'] == 100
    assert calls == []
    m.states[0]['connected'] = True
    clock[0] = 2
    m.tick()
    assert b.rates['market:0'] == 100
    assert calls == []
    clock[0] = 3
    m.tick()
    assert calls == [3]


def test_manager_sample_expands_sockets_for_confirmed_capacity_pressure(monkeypatch):
    from types import SimpleNamespace
    markets = [dict(id=k, yesAssetId=k+'y', noAssetId=k+'n') for k in 'abcde']
    owners = {f'market:{k}': i % 4 for i, k in enumerate('abcde')}
    m, _ = connected_manager(monkeypatch, markets, owners, {k: 200 for k in owners})
    m.balancer.owners = dict(owners)
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: 1))
    # New physical connection is deliberately stubbed; this test is admission
    # planning against healthy measured traffic, never a network smoke test.
    opened = []
    def connect(index, state):
        opened.append(index)
        state.update(connected=True, inbox=observer.ReceiveQueue(20),
                     thread=SimpleNamespace(is_alive=lambda: True), source=f'fake-{index}',
                     socket=m.states[1]['socket'], ping_at=float('inf'))
    monkeypatch.setattr(m, 'connect', connect)
    m.tick()
    assert m.count == m.balancer.count == 5 and opened == [4]
    assert m.migration is not None
    finish_migration(m)
    assert max(m.balancer.loads()) <= 300
    assert not any(m.balancer.capacity_cost(m.balancer.current_owners()))

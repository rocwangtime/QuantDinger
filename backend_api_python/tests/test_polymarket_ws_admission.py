"""Production pacing, rollover rejection and whole-group socket capacity."""
import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_balance import RateBalancer
from app.services.polymarket.ws_rotation import DataHealth
from app.services.polymarket.ws_subscriptions import SubscriptionPacer


def state(assets):
    sent = []
    inbox = observer.ReceiveQueue(128)
    inbox.activated = {a: datetime.max.replace(tzinfo=timezone.utc) for a in assets}
    return dict(assets=assets, connected=True, lock=threading.RLock(), inbox=inbox,
                socket=SimpleNamespace(send=lambda raw: sent.append(json.loads(raw)),
                                       transport=SimpleNamespace(last={})), sent=sent)


def test_cold_start_bypasses_global_budget_and_small_shards_subscribe_in_one_frame():
    pacer = SubscriptionPacer()
    active, standby = state(['a', 'b', 'c', 'd']), state(['x', 'y'])
    standby['standby'] = True
    pacer.next_at = 10000
    for s in (active, standby):
        pacer.enqueue(s, s['assets'], initial=True)
        pacer.drain(s, 0)
    assert active['sent'] == [dict(type='market', assets_ids=['a', 'b', 'c', 'd'])]
    assert standby['sent'] == [dict(type='market', assets_ids=['x', 'y'])]
    assert active['subscriptions_complete_at'] == 0
    assert pacer.next_at == 10000
    assert active['inbox'].activated['c'] < datetime.max.replace(tzinfo=timezone.utc)


@pytest.mark.parametrize('pressure', ['queue', 'resync', 'invalid', 'gap', 'tcp', 'scheduler', 'retired'])
def test_new_subscription_pauses_under_pressure(pressure):
    pacer, s = SubscriptionPacer(), state(['a', 'b'])
    pacer.enqueue(s, s['assets'])
    if pressure == 'queue':
        for _ in range(33):
            s['inbox'].put('frame')
    elif pressure in ('resync', 'invalid'):
        getattr(s['inbox'], pressure).set()
    elif pressure == 'gap':
        s['inbox'].delivery_gap = True
    elif pressure == 'tcp':
        s['socket'].transport.last['tcp_pending_bytes'] = 65536
    elif pressure == 'scheduler':
        s['socket'].transport.last['scheduler_lag_seconds'] = .2
        s['inbox'].processing_received_at = datetime.now(timezone.utc) - timedelta(seconds=.3)
    else:
        s['retired'] = True
    pacer.drain(s, 100)
    assert not s['sent'] and len(s['subscribe_pending']) == 2


def test_standby_must_soak_after_last_batch_and_pressure_restarts_soak(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SUBSCRIBE_INITIAL_ASSETS', '2')
    pacer, s = SubscriptionPacer(), state(['a', 'b', 'c'])
    pacer.enqueue(s, s['assets'], initial=True)
    pacer.drain(s, 0)
    assert not pacer.settled(s, 100)
    pacer.drain(s, 100)
    assert not pacer.settled(s, 125)  # First p36 failure happened after 25 seconds.
    s['inbox'].resync.set()
    assert not pacer.settled(s, 129)
    s['inbox'].resync.clear()
    assert not pacer.settled(s, 130)
    assert not pacer.settled(s, 159.99)
    assert pacer.settled(s, 160)


def test_real_transport_requires_fresh_pong_before_promotion():
    pacer, s = SubscriptionPacer(), state(['a', 'b'])
    pacer.enqueue(s, s['assets'], initial=True)
    pacer.drain(s, 0)
    s['socket'].transport.last = dict(pongs=0, pong_age_seconds=None)
    assert not pacer.settled(s, 40)
    s['socket'].transport.last = dict(pongs=2, pong_age_seconds=16)
    assert not pacer.settled(s, 41)
    s['socket'].transport.last['pong_age_seconds'] = 1
    assert pacer.settled(s, 42)


def test_catalogue_removes_unsent_assets_without_subscribing_them(monkeypatch):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SUBSCRIBE_INITIAL_ASSETS', '2')
    pacer, s = SubscriptionPacer(), state(['a', 'b', 'old', 'oldn'])
    pacer.enqueue(s, s['assets'], initial=True)
    pacer.drain(s, 0)
    s['assets'] = ['a', 'b', 'new', 'newn']
    pacer.enqueue(s, ['new', 'newn'])
    pacer.drain(s, .5)
    assert s['sent'][-1] == dict(operation='subscribe', assets_ids=['new', 'newn'])
    assert all('old' not in message['assets_ids'] for message in s['sent'])


def test_recent_delivery_gap_remains_a_pause_after_metrics_consumes_the_flag(monkeypatch):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_PIPELINE', None)
    m, s = observer.MarketConnections(), state(['a', 'b'])
    s.update(thread=SimpleNamespace(is_alive=lambda: True), ping_at=float('inf'))
    m.states = {0: s}
    m.balancer.set_groups(dict(a='market:a', b='market:a'))
    m.balancer.assignment = dict(a=0, b=0)
    m.subscriptions.enqueue(s, s['assets'])
    s['inbox'].delivery_gap = True
    clock = [10]
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    m.tick()
    assert not s['inbox'].delivery_gap
    assert not s['sent']
    clock[0] = 11
    m.tick()
    assert len(s['sent']) == 1


@pytest.mark.parametrize('phase', [0, 7, 15, 44.9, 285, 299.9])
def test_rollover_resets_existing_collapse_streak(phase):
    health = DataHealth(0)
    health.baseline = 100000
    health.low_seconds = 19
    health.at = 130
    pong = dict(pings=10, pongs=10, pong_age_seconds=1)
    assert not health.sample(131, pong, wall_time=phase)
    assert health.low_seconds == 0
    for now in range(132, 151):
        assert not health.sample(now, pong, wall_time=100)
    assert health.sample(151, pong, wall_time=100)


@pytest.mark.parametrize('baseline,rate', [(20000, 0), (100000, 1500), (1000000, 2000)])
def test_quiet_cutoff_requires_busy_baseline_and_absolute_near_silence(baseline, rate):
    health = DataHealth(0)
    health.baseline = baseline
    for now in range(1, 200):
        health.total += rate
        assert not health.sample(now, dict(pings=1, pongs=1, pong_age_seconds=1), wall_time=100)


def test_capacity_precedes_hot_isolation_so_cold_assets_cannot_pile_onto_shard():
    b = RateBalancer(4)
    owners = {str(i): (i if i < 3 else 3) for i in range(24)}
    b.set_groups({str(i) + side: str(i) for i in range(24) for side in 'yn'})
    b.rates = {str(i): (250 if i < 3 else 1) for i in range(24)}
    b.measured = set(b.rates)
    target = b.place(set(owners), owners)
    assert max(b.asset_counts(target)) <= 12
    assert max(b.loads(target)) <= 300
    assert set(target) == set(owners)


def test_small_measured_overload_stays_inside_hysteresis_band():
    b = RateBalancer(4)
    b.confirmations = 2
    owners = dict(a=0, b=0, c=1, d=2, e=3)
    b.set_groups({k + side: k for k in owners for side in 'yn'})
    b.rates = dict(a=299, b=2, c=298, d=298, e=298)
    b.measured = set(owners)
    assert b.plan(owners, 60) is None
    target = b.plan(owners, 120)
    assert target is None  # 301 vs 300 is inside the 10% entry band.


def test_manager_grows_when_measured_groups_need_more_sockets(monkeypatch):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    m = observer.MarketConnections()
    m.balancer.set_groups({k + side: k for k in 'abcde' for side in 'yn'})
    m.balancer.rates = {k: 200 for k in 'abcde'}
    owners = m.capacity_owners()
    assert m.count == m.balancer.count == 5
    assert not any(m.balancer.capacity_cost(owners))


def test_indivisible_series_excess_is_reported_without_idle_growth(monkeypatch):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    m = observer.MarketConnections()
    m.balancer.set_groups({k: 'hot' for k in 'yn'})
    m.balancer.rates = dict(hot=690)
    owners = m.capacity_owners()
    assert m.count == 4
    assert m.balancer.capacity_cost(owners) == (0, 390, 0)

"""Round12: isolate hot ingress, contain failures, and explain local loss."""
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_balance import RateBalancer
from app.services.polymarket.ws_subscriptions import SubscriptionPacer
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup
from tests.test_polymarket_ws_round11 import rollover_setup


@pytest.mark.parametrize('seed', [0, 1, 1000])
def test_startup_isolates_all_hot_pairs_even_with_equal_or_missing_priors(monkeypatch, seed):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '4')
    m = observer.MarketConnections()
    markets = [dict(id=key, market_type='up_down', asset=key.split(':')[1], interval=key.split(':')[2],
                    yesAssetId=key+'y', noAssetId=key+'n') for key in sorted(observer.HOT_SERIES)]
    markets += [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n') for i in range(17)]
    # Exercise real sync placement without starting any public socket.
    monkeypatch.setattr(m, 'connect', lambda *_: None)
    monkeypatch.setattr(m, 'tick', lambda: None)
    if seed:
        m.seed_rates({observer.balance_key(x): seed for x in markets})
    m.sync(markets)
    owners = m.balancer.current_owners()
    hot_shards = {owners[k] for k in observer.HOT_SERIES}
    assert len(hot_shards) == 4 and 7 <= m.count <= 8
    assert m.balancer.forced_collisions(owners) == 0
    for market in markets:
        y, n = market['yesAssetId'], market['noAssetId']
        assert m.balancer.assignment[y] == m.balancer.assignment[n]
        if observer.balance_key(market) in observer.HOT_SERIES:
            assert len(m.states[m.balancer.assignment[y]]['assets']) == 2


def test_search_repairs_forced_collision_before_improving_byte_capacity():
    b = RateBalancer(3)
    b.set_groups(dict(y='hot', n='hot', cy='cold', cn='cold', dy='other', dn='other'))
    b.forced_hot = {'hot'}
    b.byte_rates = dict(hot=1200000, cold=2000, other=1000)
    owners, _, _ = b.search(dict(hot=0, cold=0, other=1), 8)
    assert b.forced_collisions(owners) == 0
    detail = next(row for row in b.capacity_details(owners) if 'hot' in row['groups'])
    assert detail['indivisible_excess'][2] == 1200000 - b.max_bytes
    assert detail['co_tenant_excess'][2] == 0


def test_actual_ingress_cannot_be_hidden_by_slow_worker_rates():
    b = RateBalancer(2)
    b.set_groups(dict(y='hot', n='hot'))
    b.assignment = dict(y=0, n=0)
    b.byte_rates = {'hot': 55000}
    b.ingress_byte_rates = {'hot': 1200000}
    assert b.byte_loads()[0] == 1200000
    assert b.overloaded(b.current_owners())
    assert b.metrics()['ws_group_ingress_byte_rates'] == {'hot': 1200000}
    b.set_groups(dict(y='next', n='next'))
    assert not b.ingress_byte_rates


def test_default_one_spare_and_explicit_two_spare_option(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT')
    first = ready_backup(m, sockets)
    first['initial_sent_at'] -= 60
    m.backup_next_connect_at = 0
    m.tick()
    assert not m.reserves and len(sockets) == 2
    assert m.update_hot_coverage(time.monotonic())['up_down:BTC:5m']['target_backup_count'] == 1
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '2')
    m.tick()
    assert m.reserves[0]['socket'].opened.wait(1) and len(sockets) == 3
    assert m.update_hot_coverage(time.monotonic())['up_down:BTC:5m']['target_backup_count'] == 2


@pytest.mark.parametrize('source', ['primary', 'backup'])
@pytest.mark.parametrize('rate', [650000, 1200000])
def test_byte_saturated_pair_stops_prewarm_without_subscription_churn(feed_manager, monkeypatch, source, rate):
    m, _, _, first, _, current, upcoming, end = rollover_setup(feed_manager, monkeypatch)
    state = m.states[0] if source == 'primary' else first
    state['inbox'].byte_rate = rate
    assert set(m.backup_assets(0)) == {'ay', 'an'}
    state['inbox'].byte_rate = 0
    # Once this window is saturated, a quiet tick cannot keep re-subscribing
    # and removing its next snapshot. A new current pair gets a new decision.
    assert set(m.backup_assets(0)) == {'ay', 'an'}
    m.states[0]['assets'] = ['ny', 'nn']
    m.balancer.set_groups(dict(ny='up_down:BTC:5m', nn='up_down:BTC:5m'))
    m.prewarm_markets = [dict(upcoming, id='later', yesAssetId='ly', noAssetId='ln',
                              eventStartTime=(end+timedelta(minutes=5)).isoformat())]
    assert set(m.backup_assets(0, end+timedelta(minutes=4))) == {'ny', 'nn', 'ly', 'ln'}


def test_drop_reasons_distinguish_overflow_recovery_and_close():
    inbox = observer.ReceiveQueue(2, 4)
    for _ in range(4):
        assert inbox.put('raw')
    assert not inbox.put('overflow') and inbox.epoch == 1
    assert not inbox.put('recovery')
    inbox.discard()
    assert inbox.queue.empty() and inbox.queue.maxsize == 4
    inbox.resync.clear()
    assert inbox.put('new')
    inbox.invalid.set()
    assert not inbox.put('invalid')
    inbox.discard('shutdown_discarded')
    assert inbox.drop_reasons == dict(overflow=1, resync_rejected=1, invalid_rejected=1,
                                     resync_discarded=4, shutdown_discarded=1)
    assert inbox.dropped == sum(inbox.drop_reasons.values()) == 8
    assert inbox.discard_max_seconds >= 0


def test_large_generation_drop_count_is_cumulative_across_bounded_overflows():
    inbox = observer.ReceiveQueue(16384)
    for _ in range(7):
        for _ in range(16384):
            inbox.put('raw')
        inbox.put('overflow')
        inbox.discard()
        inbox.resync.clear()
    assert inbox.dropped == 7*16385 > 100000
    assert inbox.drop_reasons['overflow'] == 7
    assert inbox.high_water == 16384 and inbox.queue.empty()


@pytest.mark.parametrize('reason', ['queue_depth', 'queue_lag', 'tcp_pending', 'scheduler_lag', 'resync'])
def test_ingress_pressure_explains_admission_blocker(reason):
    s = dict(inbox=observer.ReceiveQueue(64), socket=SimpleNamespace(transport=SimpleNamespace(last={})))
    inbox = s['inbox']
    if reason == 'queue_depth':
        for _ in range(33):
            inbox.put('raw')
    elif reason == 'queue_lag':
        inbox.put('raw')
        inbox.lag_seconds = 2
    elif reason == 'resync':
        inbox.resync.set()
    else:
        s['socket'].transport.last[reason+'_seconds' if reason == 'scheduler_lag' else 'tcp_pending_bytes'] = .2 if reason == 'scheduler_lag' else 65536
    if reason == 'scheduler_lag':
        inbox.processing_received_at = datetime.now(timezone.utc) - timedelta(seconds=.3)
    assert SubscriptionPacer.pressure_reasons(s) == [reason]
    assert SubscriptionPacer.pressured(s)


def test_primary_kicks_survive_redial_without_resetting_group_totals_or_coverage(feed_manager):
    m, _, sockets = feed_manager
    key = 'up_down:BTC:5m'
    m.balancer.set_groups({a: key for a in m.states[0]['assets']})
    at = time.monotonic()
    m.update_hot_coverage(at)
    # Advance the accumulator's origin instead of sleeping in a network test.
    m.hot_backup_coverage.at -= 100
    for n in range(4):
        old = m.states[0]
        old['socket'].callbacks['on_close'](old['socket'], 1013, 'slow consumer: send buffer full')
        old['retry_at'] = 0
        m.tick()
        assert m.states[0]['socket'].opened.wait(1)
        m.record_primary_failure(0, old, 1013)  # Late callback cannot count twice.
        assert m.primary_failures[key]['total'] == n+1
    row = m.update_hot_coverage(time.monotonic())[key]
    assert row['zero_ready_backup_seconds'] >= 100
    assert row['observed_seconds'] >= 100
    assert observer.STATUS['ws_hot_primary_failures'][key]['total'] == 4
    assert observer.STATUS['ws_primary_1013_by_shard'][0]['total'] == 4


def test_intentional_manager_shutdown_is_not_remote_failure(feed_manager, caplog):
    m, _, sockets = feed_manager
    caplog.clear()
    with caplog.at_level('INFO'):
        m.close()
    assert 'manager local shutdown' in caplog.text
    assert 'transport failure' not in caplog.text and 'WS close shard=' not in caplog.text
    assert not m.primary_failures


def test_real_backlog_still_vetoes_ready_and_exposes_pressure_details(feed_manager):
    m, _, sockets = feed_manager
    first = ready_backup(m, sockets)
    with m.lifecycle_lock:
        first['inbox'].queue_wait_seconds = 12
        assert not m.backup_ready(first)
        row = m.backup_metrics(0, first, time.monotonic())
        assert not row['ready'] and row['queue_wait_seconds'] == 12
        assert 'drop_reasons' in row and 'process_max_seconds' in row


def test_close_evidence_keeps_local_queue_and_heartbeat_separate(feed_manager):
    m, _, sockets = feed_manager
    old = m.states[0]
    old['inbox'].drop_reasons['overflow'] = 7
    old['inbox'].dropped = 7
    sockets[0].transport = observer.TransportDiagnostics()
    sockets[0].transport.pings = sockets[0].transport.pongs = 7
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    e = old['last_transport_failure']
    assert e['code'] == 1013 and e['drop_reasons']['overflow'] == 7
    assert e['groups'] == ['market:a']
    assert e['sample']['pings'] == e['sample']['pongs'] == 7
    assert all(k in e for k in ('queue_depth', 'queue_wait_seconds', 'resyncs', 'discard_max_seconds'))


def test_market_loop_keeps_existing_manager_when_one_shard_has_many_retries(monkeypatch):
    stop = threading.Event()
    market = dict(id='a', yesAssetId='y', noAssetId='n')
    managers = []
    class Manager:
        def __init__(self):
            self.ticks = self.closes = 0
            managers.append(self)
        def sync(self, *args, **kwargs):
            pass
        def seed_rates(self, rates):
            pass
        def start_supervisor(self):
            pass
        def tick(self):
            self.ticks += 1
            if self.ticks == 4:
                stop.set()
        def failures(self):
            return 12  # Prior code fell back and destroyed every healthy shard.
        def close(self):
            self.closes += 1
    class Client:
        def discover(self, **kwargs):
            return [market], []
        def capture(self, *_):
            pytest.fail('One failing shard triggered global REST fallback')
        def close(self):
            pass
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_ENABLED', 'true')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'false')
    monkeypatch.setattr(observer, '_STOP', stop)
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, 'MarketConnections', Manager)
    monkeypatch.setattr(observer, 'PublicClient', Client)
    monkeypatch.setattr(observer, '_prior_group_rates', lambda: {})
    monkeypatch.setattr(observer, '_run_retention', lambda *_: None)
    monkeypatch.setattr(observer, '_close_stale_episodes', lambda *_: None)
    monkeypatch.setattr(observer, '_persist_status', lambda: None)
    observer._market_loop()
    assert len(managers) == 1 and managers[0].ticks == 4 and managers[0].closes == 1

"""Round 10: independent hot spares, parallel recovery and honest capacity."""
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer, ws_recovery
from app.services.polymarket.ws_balance import RateBalancer
from tests.test_polymarket_ws_rotation import book, feed_manager, wait_for
from tests.test_polymarket_ws_round7 import ready_backup


def second_backup(m, sockets):
    first = ready_backup(m, sockets)
    first['initial_sent_at'] -= 16
    m.backup_next_connect_at = 0
    m.tick()
    reserve = m.reserves[0]
    assert reserve['socket'].opened.wait(1)
    reserve['socket'].emit([book(a) for a in reserve['assets']])
    wait_for(lambda: reserve.get('ready'))
    return first, reserve


def test_two_backups_same_assets_no_extra_per_connection_load(feed_manager):
    m, _, sockets = feed_manager
    first = ready_backup(m, sockets)
    m.backup_next_connect_at = 0
    m.tick()
    assert not m.reserves  # Extra copy is staggered, first is eager.
    first['initial_sent_at'] -= 16
    m.backup_next_connect_at = 0  # Advance the independent global admission slot too.
    m.tick()
    reserve = m.reserves[0]
    assert reserve['socket'].opened.wait(1)
    for c in (first, reserve):
        assert set(c['assets']) == set(m.states[0]['assets'])
        assert json.loads(c['socket'].sent[0])['assets_ids'] == c['assets']
    assert len(m.backup_items()) == 2


@pytest.mark.parametrize('lost', ['first', 'reserve'])
def test_one_backup_kick_preserves_coverage_and_other_can_take_over(feed_manager, lost):
    m, p, sockets = feed_manager
    first, reserve = second_backup(m, sockets)
    victim, survivor = (first, reserve) if lost == 'first' else (reserve, first)
    before = dict(p.books)
    victim['socket'].callbacks['on_close'](victim['socket'], 1013, 'slow consumer')
    row = m.update_hot_coverage(time.monotonic())['up_down:BTC:5m']
    assert row['ready'] and p.books == before
    m.tick()
    assert victim['retired']
    old = m.states[0]
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    wait_for(lambda: m.states[0] is survivor and old.get('retired'), .5)
    event = m.primary_failures['up_down:BTC:5m']['events'][-1]
    assert event['ready_backup_at_kick']
    assert event['takeover_gap_ms'] < 500 and event['recovery'] == 'backup'
    assert len(p.books) == 2 and old['retired']


def test_two_backups_survive_rollover_and_shutdown(feed_manager):
    m, p, sockets = feed_manager
    second_backup(m, sockets)
    p.recording.enabled = False
    market = dict(id='next', market_type='up_down', asset='BTC', interval='5m', yesAssetId='ny', noAssetId='nn')
    p.set_markets([market])
    m.sync([market])
    assert len(m.backup_items()) == 2
    for _, candidate in m.backup_items():
        assert candidate['assets'] == ['ny', 'nn']
        assert not candidate['ready']
        commands = [json.loads(raw) for raw in candidate['socket'].sent if raw != 'PING']
        assert any(c.get('operation') == 'unsubscribe' and set(c['assets_ids']) == {'ay', 'an'} for c in commands)
    m.close()
    assert not m.reserves and not m.backups
    assert all(s.stopped.is_set() for s in sockets)


def test_warming_backup_cannot_block_immediate_primary_redial_or_rest(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    old = m.states[0]
    m.balancer.set_groups({a: 'up_down:BTC:5m' for a in old['assets']})
    m.tick()
    assert sockets[1].opened.wait(1)
    warm = m.backups[0]
    requests = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: requests.append((state, assets)))
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    assert requests and requests[0][0] is old
    # The old reader deliberately stays alive; redial must not wait for join.
    old['retry_at'] = 0
    began = time.monotonic()
    m.tick()
    assert time.monotonic() - began < .5
    assert sockets[2].opened.wait(1)
    assert m.states[0] is not old and m.backups[0] is warm
    assert not warm.get('ready') and m.states[0]['connected']
    assert requests[-1][0] is m.states[0]
    assert len(m.primary_failures['up_down:BTC:5m']['events']) == 1
    # Retired callbacks cannot count another primary kick.
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    assert m.primary_failures['up_down:BTC:5m']['total'] == 1


def test_backup_ingress_gap_repairs_without_unsubscribe_or_pacer_deadlock(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    candidate['inbox'].delivery_gap = True
    candidate['inbox'].queue_wait_seconds = 0
    candidate['recover_assets'] = {'ay', 'an'}
    requests = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: requests.append((assets, publish)))
    m.tick()
    assert requests and not candidate['ready']
    assert not any(json.loads(raw).get('operation') == 'unsubscribe' for raw in sockets[1].sent if raw != 'PING')
    m.redundant_receive(0, candidate, [], datetime.now(timezone.utc), candidate['inbox'])
    assert not candidate['inbox'].delivery_gap
    requests[0][1]([book('ay'), book('an')], datetime.now(timezone.utc))
    wait_for(lambda: candidate.get('ready'))
    assert m.prepare_stage(candidate) == 'ready'


def test_rest_wins_while_ws_handshake_stays_offline_and_late_rest_cannot_roll_back(feed_manager, monkeypatch):
    m, p, sockets = feed_manager
    old = m.states[0]
    key = 'up_down:BTC:5m'
    m.balancer.set_groups({a: key for a in old['assets']})
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: callbacks.append(publish))
    sockets[0].callbacks['on_close'](sockets[0], 1013, 'slow consumer')
    old['worker'].join(1)
    assert not old['connected']
    at = int(datetime.now(timezone.utc).timestamp() * 1000)
    callbacks[0]([book('ay', '4', at), book('an', '4', at)], datetime.now(timezone.utc))
    assert p.books['ay']['asks']['.4'] == '4'
    event = m.primary_failures[key]['events'][-1]
    assert not event['ready_backup_at_kick'] and event['recovery'] == 'rest_snapshot'
    assert event['takeover_gap_ms'] < 2000
    callbacks[0]([book('ay', '99', at-1), book('an', '99', at-1)], datetime.now(timezone.utc))
    assert p.books['ay']['asks']['.4'] == '4'
    assert len(m.primary_failures[key]['events']) == 1


def test_retired_rest_result_and_removed_asset_never_publish(feed_manager, monkeypatch):
    m, p, _ = feed_manager
    state = m.states[0]
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: callbacks.append(publish))
    m.request_snapshot(0, state, {'ay', 'an'})
    before = dict(p.books)
    state['retired'] = True
    callbacks[0]([book('ay', '99'), book('an', '99')], datetime.now(timezone.utc))
    assert p.books == before
    state['retired'] = False
    m.balancer.assignment.pop('an')
    callbacks[0]([book('ay', '3'), book('an', '99')], datetime.now(timezone.utc))
    assert p.books['an'] == before['an']


def fake_http(monkeypatch, rows, block=None, error=None):
    calls = []
    class Session:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def post(self, url, **kwargs):
            calls.append((url, kwargs, self.trust_env))
            if block:
                block.wait(2)
            if error:
                raise error
            return SimpleNamespace(content=b'[]', raise_for_status=lambda: None, json=lambda: rows)
    monkeypatch.setattr(ws_recovery.requests, 'Session', Session)
    return calls


def test_public_snapshot_timeout_identity_and_no_auth(monkeypatch):
    recovery = ws_recovery.SnapshotRecovery()
    calls = fake_http(monkeypatch, [book('a')])
    published = []
    recovery.request({}, {'a'}, lambda rows, started: published.extend(rows))
    wait_for(lambda: recovery.metrics()['inflight'] == 0)
    assert published[0]['asset_id'] == 'a'
    assert calls[0][0] == 'https://clob.polymarket.com/books'
    assert calls[0][1] == dict(json=[dict(token_id='a')], timeout=(.4, 1), allow_redirects=False)
    assert calls[0][2] is False
    assert recovery.metrics()['completed'] == 1


@pytest.mark.parametrize('kind', ['missing', 'duplicate', 'wrong_asset', 'missing_time', 'stale', 'bad_levels', 'http'])
def test_invalid_rest_never_restores_or_marks_ready(monkeypatch, kind):
    rows = [book('a')]
    error = None
    if kind == 'missing':
        rows = []
    elif kind == 'duplicate':
        rows = [book('a'), book('a')]
    elif kind == 'wrong_asset':
        rows = [book('wrong')]
    elif kind == 'missing_time':
        rows[0].pop('timestamp')
    elif kind == 'stale':
        rows = [book('a', at=int((datetime.now(timezone.utc)-timedelta(seconds=10)).timestamp()*1000))]
    elif kind == 'bad_levels':
        rows[0]['bids'] = [{}]
    else:
        error = ws_recovery.requests.Timeout()
    fake_http(monkeypatch, rows, error=error)
    recovery = ws_recovery.SnapshotRecovery()
    published = []
    recovery.request({}, {'a', 'b'} if kind == 'duplicate' else {'a'}, lambda *args: published.append(args))
    wait_for(lambda: not recovery.metrics()['inflight'])
    assert not published and recovery.metrics()['failures'] == 1


def test_rest_global_sliding_budget_inflight_and_per_state_dedup(monkeypatch):
    block = threading.Event()
    calls = fake_http(monkeypatch, [book('a')], block=block)
    recovery = ws_recovery.SnapshotRecovery()
    states = [{} for _ in range(5)]
    for state in states:
        recovery.request(state, {'a'}, lambda *args: None)
    recovery.request(states[0], {'a'}, lambda *args: None)
    assert recovery.metrics()['inflight'] == 4 and len(calls) == 4
    block.set()
    wait_for(lambda: not recovery.metrics()['inflight'])
    with recovery.lock:
        recovery.starts = ws_recovery.deque([time.monotonic()]*40)
    recovery.request({}, {'a'}, lambda *args: None)
    assert len(calls) == 4 and recovery.metrics()['throttled'] == 2
    with recovery.lock:
        recovery.starts = ws_recovery.deque([time.monotonic()-11]*40)
    recovery.request({}, {'a'}, lambda *args: None)
    wait_for(lambda: len(calls) == 5 and not recovery.metrics()['inflight'])


def test_capacity_does_not_count_backups_and_explains_indivisible_excess(monkeypatch):
    b = RateBalancer(2)
    b.set_groups(dict(ay='hot', an='hot', by='cold', bn='cold'))
    b.assignment = dict(ay=0, an=0, by=0, bn=0)
    b.rates = dict(hot=500, cold=50)
    b.byte_rates = dict(hot=400000, cold=10000)
    b.measured = {'hot', 'cold'}
    report = b.metrics()
    assert report['ws_capacity_excess'] == (0, 250, 147856)
    row = report['ws_capacity_by_shard'][0]
    assert row['indivisible_excess'] == (0, 200, 137856)
    assert row['co_tenant_excess'] == (0, 50, 10000)
    assert report['ws_capacity_scope'] == 'logical_primary_planning_budget'
    # Removing co-tenants can fix 50 msg/s; a pair's 200 msg/s remains honest.
    b.assignment.update(by=1, bn=1)
    assert b.metrics()['ws_capacity_excess'] == (0, 200, 137856)


def test_idle_worker_clears_historical_lag_instead_of_latching_ingress_pressure(feed_manager):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    inbox = candidate['inbox']
    inbox.queue_wait_seconds = inbox.lag_seconds = 1.5
    inbox.delivery_gap = True
    candidate['ready'] = False
    wait_for(lambda: candidate.get('ready'), .8)
    assert not inbox.delivery_gap and inbox.queue_wait_seconds == 0
    assert m.prepare_stage(candidate) == 'ready'


def test_rest_response_from_before_resync_epoch_is_discarded(feed_manager, monkeypatch):
    m, p, _ = feed_manager
    state = m.states[0]
    callbacks = []
    monkeypatch.setattr(m.snapshot_recovery, 'request', lambda state, assets, publish: callbacks.append(publish))
    m.request_snapshot(0, state, {'ay', 'an'})
    before = dict(p.books)
    with state['inbox'].lock:
        state['inbox'].epoch += 1
    callbacks[0]([book('ay', '99'), book('an', '99')], datetime.now(timezone.utc))
    assert p.books == before


def test_uncovered_groups_rebuild_with_global_stagger_not_primary_age(feed_manager, monkeypatch):
    m, p, _ = feed_manager
    markets = []
    for i, key in enumerate(sorted(observer.HOT_SERIES)):
        _, asset, interval = key.split(':')
        markets.append(dict(id=str(i), market_type='up_down', asset=asset, interval=interval,
                            yesAssetId=f'{i}y', noAssetId=f'{i}n'))
    p.recording.enabled = False
    p.set_markets(markets)
    m.sync(markets)
    m.start_supervisor()
    wait_for(lambda: len(m.backups) == 4 and all(c.get('opened_at') for c in m.backups.values()), 5)
    candidates = list(m.backups.values())
    for candidate in candidates:
        candidate['socket'].callbacks['on_close'](candidate['socket'], 1013, 'slow consumer')
    wait_for(lambda: len(m.backups) == 4 and all(c.get('opened_at') and c not in candidates
        for c in m.backups.values()), 5)
    opened = sorted(c['opened_at'] for c in m.backups.values())
    assert all(b - a >= .49 for a, b in zip(opened, opened[1:]))
    assert all(c['opened_at'] - m.states[i]['opened_at'] < 10 for i, c in m.backups.items())


@pytest.mark.parametrize('problem', ['gap', 'lag', 'resync'])
def test_readiness_metrics_reject_current_pressure_even_if_old_ready_flag_is_true(feed_manager, problem):
    m, _, sockets = feed_manager
    candidate = ready_backup(m, sockets)
    inbox = candidate['inbox']
    with m.lifecycle_lock:
        if problem == 'gap':
            inbox.delivery_gap = True
        elif problem == 'lag':
            inbox.queue_wait_seconds = 1.1
        else:
            inbox.resync.set()
        assert not m.backup_ready(candidate)

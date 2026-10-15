"""Measured token ownership, bounded churn, and subscription handover contracts."""
import json
import threading

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_balance import DailyStats, RateBalancer


def balancer(monkeypatch, assignment, rates):
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_SECONDS', '1')
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REBALANCE_MIN_SECONDS', '10')
    b = RateBalancer(4)
    b.set_assets(assignment)
    b.assignment = assignment.copy()
    b.rates = rates.copy()
    b.pending = rates.copy()  # One-second sample retains the supplied rates.
    b.sample_at = b.last_check = 0
    return b


def test_largest_first_preserves_set_and_reduces_moves(monkeypatch):
    assignment = {str(i): 0 for i in range(8)}
    b = balancer(monkeypatch, assignment, {a: 10 for a in assignment})
    target = b.propose(1)
    assert set(target) == set(assignment)
    assert b.loads(target) == [20] * 4
    assert sum(target[a] != assignment[a] for a in target) == 6
    assert b.metrics()['ws_load_imbalance'] == 4


def test_hysteresis_and_minimum_interval(monkeypatch):
    assignment = {'a': 0, 'b': 1, 'c': 2, 'd': 3}
    b = balancer(monkeypatch, assignment, dict(a=10, b=10, c=10, d=11))
    assert b.propose(1) is None
    b.assignment = {a: 0 for a in assignment}
    b.last_rebalance = 1
    assert b.propose(2, reconnect=True) is None
    assert b.propose(11, reconnect=True)


def test_unavoidable_hot_token_does_not_churn(monkeypatch):
    b = balancer(monkeypatch, {'a': 0}, {'a': 100})
    assert b.propose(1) is None


def test_ewma_token_frames_include_unknown_book_and_decay(monkeypatch):
    b = balancer(monkeypatch, {'a': 0, 'b': 1}, dict(a=0, b=0))
    frame = json.dumps(dict(event_type='price_change', price_changes=[
        dict(asset_id='a'), dict(asset_id='a'), dict(asset_id='b')]))
    b.receive(frame, 0)
    b.receive(frame, 1)
    assert b.pending == dict(a=1, b=1)
    b.propose(1)
    first = b.rates['a']
    assert 0 < first < 1 and first == b.rates['b']
    b.propose(2)
    assert b.rates['a'] < first
    b.set_assets({'b': 1})
    assert set(b.rates) == {'b'}


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
                       connected=True, lock=threading.RLock()) for i in range(4)}
    m.balancer.assignment = dict(a=0, b=0)
    return m, sent


def test_handover_invalidates_generations_before_publishing_owners(monkeypatch):
    m, sent = manager(monkeypatch)
    monkeypatch.setattr(observer, '_PIPELINE', None)
    m.states[0]['inbox'] = observer.ReceiveQueue(2)
    m.states[2]['inbox'] = observer.ReceiveQueue(2)
    assert m.apply_assignment(dict(a=0, b=2))
    assert not sent
    assert m.states[0]['inbox'].invalid.is_set()
    assert m.states[2]['inbox'].invalid.is_set()
    assert not m.states[0]['connected'] and not m.states[2]['connected']
    assert m.balancer.assignment == dict(a=0, b=2)


def test_utc_counter_rollover_and_checkpoints(monkeypatch):
    stats = DailyStats()
    stats.increment('reconnect_count', day='2026-10-12')
    stats.increment('rebalance_count', day='2026-10-13')
    stats.increment('message', shard=3, day='2026-10-13')
    assert stats.days['2026-10-12']['reconnect_count'] == 1
    assert stats.days['2026-10-13']['shard_msg_totals'] == {'3': 1}


@pytest.mark.parametrize('raw', ['oops', 'null', '[]', '{}'])
def test_non_token_payloads_do_not_affect_rates(monkeypatch, raw):
    b = balancer(monkeypatch, {'a': 0}, {'a': 0})
    b.receive(raw, 0)
    assert b.pending == {'a': 0}


def test_reconnect_checks_before_periodic_deadline(monkeypatch):
    b = balancer(monkeypatch, {str(i): 0 for i in range(8)}, {str(i): 10 for i in range(8)})
    b.interval = 60
    b.last_check = 1
    assert b.propose(1) is None
    assert b.propose(1, reconnect=True)


def test_manager_retains_market_locality_on_connected_tick(monkeypatch):
    from types import SimpleNamespace
    m, _ = manager(monkeypatch)
    b = balancer(monkeypatch, dict(a=0, b=0), dict(a=10, b=10))
    m.balancer = b
    m.reconnect_pending = True
    for state in m.states.values():
        state.update(thread=SimpleNamespace(is_alive=lambda: True), ping_at=float('inf'))
    monkeypatch.setattr(observer, 'time', SimpleNamespace(monotonic=lambda: 1))
    m.tick()
    assert m.balancer.assignment['a'] == m.balancer.assignment['b']
    assert not m.stats.days


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

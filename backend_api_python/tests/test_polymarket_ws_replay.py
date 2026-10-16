"""Two simulated hours of the measured asset-rate distribution through real shard balancing.

Uses ``POLYMARKET_WS_ASSET_RATES`` or ``deploy/pm_ws_asset_rates_20261016.json``
when present, otherwise the synthetic distribution in scripts/pm_ws_replay.py
(hottest market ~367 msg/s, three hottest ~80%). Up/Down windows roll over on
the real 5m/15m clock and the catalogue refreshes every 60 s, as in production.
Frame counts are Poisson per second and enter the balancer's ingress counters;
MarketConnections.sync/tick decide every placement and move. The network
replay (slow consumer, freshness) is scripts/pm_ws_replay.py.
"""
import importlib.util
import json
import os
import random
import threading
import time
from types import SimpleNamespace

import pytest

from app.services.polymarket import observer
from app.services.polymarket.ws_balance import DailyStats

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location('pm_ws_replay', os.path.join(ROOT, 'scripts', 'pm_ws_replay.py'))
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)


def distribution():
    path = os.getenv('POLYMARKET_WS_ASSET_RATES') or os.path.join(
        os.path.dirname(ROOT), 'deploy', 'pm_ws_asset_rates_20261016.json')
    return replay.load_rates(path) if os.path.exists(path) else replay.synthetic_rates()


def simulate(monkeypatch, seconds=7200, prior=False, seed=3, synthetic=False):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_DAILY_STATS', DailyStats())
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, 'STATUS', dict(observer.STATUS, rebalance_count=0))
    clock = [0.0]
    monkeypatch.setattr(observer, 'time', SimpleNamespace(process_time=time.process_time, monotonic=lambda: clock[0]))
    rates = replay.synthetic_rates() if synthetic else distribution()
    schedule = replay.Schedule(rates, 0.0, 1.0)
    rng = random.Random(seed)
    m = observer.MarketConnections()
    m.balancer.sample_at = m.balancer.last_check = 0.0
    closed = []

    class Socket:
        def __init__(self, index):
            self.index = index

        def send(self, raw):
            json.loads(raw)

        def close(self):
            closed.append(self.index)
    for i in range(m.count):
        m.states[i] = dict(assets=[], socket=Socket(i), connected=True, lock=threading.RLock(),
                           thread=SimpleNamespace(is_alive=lambda: True), ping_at=float('inf'), attempts=0)
    if prior:
        m.seed_rates(replay.prior_rates(schedule))
    markets = schedule.markets(0.0)
    m.sync(markets)
    ratios, moves = [], []
    while clock[0] < seconds:
        clock[0] += 1
        if clock[0] % 60 == 0:
            markets = schedule.markets(clock[0])
            m.sync(markets)
        before = dict(m.balancer.assignment)
        flows = schedule.traffic(clock[0])
        groups = m.balancer.groups
        for (yes, no), (yes_rate, no_rate) in flows.items():
            for asset, rate in ((yes, yes_rate), (no, no_rate)):
                if asset in m.balancer.assignment and groups.get(asset):
                    key = groups[asset]
                    m.balancer.pending[key] = m.balancer.pending.get(key, 0) + replay.poisson(rng, rate)
        m.tick()
        moved = {a for a in before if a in m.balancer.assignment and before[a] != m.balancer.assignment[a]}
        if moved:
            moves.append((clock[0], len(moved)))
        for market in markets:
            assert m.balancer.assignment[market['yesAssetId']] == m.balancer.assignment[market['noAssetId']]
        if clock[0] % 30 == 0:
            loads = [0.0] * m.count
            for (yes, no), (yes_rate, no_rate) in flows.items():
                if yes in m.balancer.assignment:
                    loads[m.balancer.assignment[yes]] += yes_rate + no_rate
            bound = max(sum(loads) / m.count, max(sum(v) for v in flows.values()))
            ratios.append((clock[0], max(loads) / bound, loads))
    assert not closed
    return ratios, moves, m


def test_measured_cold_start_reaches_physical_peak_bound(monkeypatch):
    ratios, moves, m = simulate(monkeypatch)
    steady = [r for t, r, _ in ratios if t >= 600]
    assert max(steady) <= 1.1, [x for x in ratios if x[0] >= 600 and x[1] > 1.1][:3]
    # Rollovers never move a series; later rounds only absorb rate noise.
    assert sum(1 for t, _ in moves if t >= 600) <= 6
    assert all(count <= 4 * m.balancer.max_moves for _, count in moves)


def test_restart_with_persisted_group_rates_is_balanced_immediately(monkeypatch):
    ratios, moves, _ = simulate(monkeypatch, seconds=1800, prior=True)
    assert max(r for _, r, _ in ratios) <= 1.1
    assert len(moves) <= 2


@pytest.mark.parametrize('seed', [11, 12])
def test_noise_does_not_cause_rebalance_churn(monkeypatch, seed):
    _, moves, _ = simulate(monkeypatch, seconds=3600, prior=True, seed=seed)
    assert len(moves) <= 3


def test_real_file_uses_msg_per_s_and_does_not_invent_market_names():
    rates = distribution()
    assert rates['source'] == 'pm_ws_asset_rates_20261016.json'
    assert len(rates['markets']) == 28
    ordered = sorted((m['rate'] for m in rates['markets']), reverse=True)
    assert ordered[:3] == pytest.approx([367.434, 148.030, 114.782])
    assert sum(ordered) == pytest.approx(681.792, abs=.02)
    assert all('series' not in m for m in rates['markets'])


def test_series_rollovers_and_noise_keep_peak_and_churn_bounded(monkeypatch):
    peaks, moves, _ = simulate(monkeypatch, seconds=7200, synthetic=True)
    assert max(p for t, p, _ in peaks if t >= 600) <= 1.15
    assert sum(t >= 600 for t, _ in moves) <= 4

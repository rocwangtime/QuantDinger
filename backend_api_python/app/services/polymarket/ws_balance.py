"""Public-feed load measurement and UTC session checkpoints (no external I/O on receive)."""
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from app.utils.db import get_db_connection


def setting(name, default, minimum):
    try:
        value = float(os.getenv('POLYMARKET_OBSERVER_WS_' + name, default))
        return max(minimum, value) if math.isfinite(value) else default
    except ValueError:
        return default


def event_assets(event):
    if isinstance(event, list):
        return set().union(*(event_assets(item) for item in event))
    if not isinstance(event, dict):
        return set()
    payload = event.get('payload') or event
    assets = {str(payload.get('asset_id') or payload.get('assetId') or '')}
    for row in payload.get('price_changes', payload.get('priceChanges', [])):
        assets.add(str(row.get('asset_id') or row.get('assetId') or ''))
    return assets - {''}


def balance_key(market):
    """Unit that shares one socket.

    Every window of an Up/Down series (current and next) uses one key, so a
    rollover replaces assets on the series' shard instead of creating an
    unmeasured market elsewhere. Measured series rates survive rollovers.
    """
    if market.get('market_type') == 'up_down' and market.get('asset') and market.get('interval'):
        return f"up_down:{market['asset']}:{market['interval']}"
    return f"market:{market['id']}"


def ratio(loads):
    """Heaviest over lightest shard; an idle shard next to a busy one is infinite."""
    if not loads or max(loads) <= 0:
        return 1.0
    return max(loads) / min(loads) if min(loads) > 0 else math.inf


class RateBalancer:
    """Ingress frame rates per balance group and bounded whole-group moves.

    Receive callbacks count every frame before it is queued, so a backlog cannot
    hide load. A frame's cost of one message is split between the owned assets it
    carries, so shard loads add up to frames per second.
    """
    def __init__(self, count):
        self.count = count
        self.lock = threading.Lock()
        self.assignment, self.groups, self.pending = {}, {}, {}
        self.rates, self.owners, self.measured, self.since = {}, {}, set(), {}
        self.sample_at = time.monotonic()
        self.last_check = self.sample_at
        self.last_rebalance = float('-inf')
        self.interval = setting('REBALANCE_SECONDS', 60, 1)
        self.minimum = setting('REBALANCE_MIN_SECONDS', 60, 1)
        self.threshold = setting('REBALANCE_HYSTERESIS', .1, 0)
        self.trigger = setting('REBALANCE_RATIO', 1.5, 1)
        self.target = setting('REBALANCE_TARGET_RATIO', 2, 1)
        self.max_moves = int(setting('REBALANCE_MAX_GROUPS', 4, 1))
        self.half_life = setting('RATE_HALF_LIFE_SECONDS', 30, 1)

    def set_groups(self, groups, now=None):
        """Publish asset -> balance key; keys keep their rate and owner."""
        with self.lock:
            now = time.monotonic() if now is None else now
            self.groups = dict(groups)
            for key in set(groups.values()):
                self.since.setdefault(key, now)

    def receive(self, event, shard):
        """Count one frame at ingress. ``event`` is parsed JSON or raw text."""
        if isinstance(event, (str, bytes, bytearray)):
            try:
                event = json.loads(event)
            except (ValueError, TypeError):
                return
        try:
            assets = [a for a in event_assets(event) if self.assignment.get(a) == shard]
        except (TypeError, AttributeError):
            return
        if not assets:
            return
        share = 1 / len(assets)
        groups = self.groups
        with self.lock:
            # Counted per group, so a window that expires before the next sample
            # still contributes its frames to the series rate.
            for asset in assets:
                key = groups.get(asset)
                if key is not None:
                    self.pending[key] = self.pending.get(key, 0) + share

    def sample(self, now):
        """Fold counts since the last sample into per-group EWMA msg/s."""
        with self.lock:
            elapsed = now - self.sample_at
            if elapsed <= 0:
                return False
            counts, self.pending = self.pending, {}
            for key in set(self.groups.values()):
                # A group that appeared mid-window is measured from its start.
                window = min(elapsed, now - self.since.get(key, self.sample_at))
                if window < min(elapsed, self.interval) / 2:
                    continue
                rate = counts.get(key, 0) / window
                if key in self.measured:
                    alpha = 1 - math.exp(-math.log(2) * window / self.half_life)
                    self.rates[key] += alpha * (rate - self.rates[key])
                else:
                    self.rates[key] = rate
                    self.measured.add(key)
            self.sample_at = now
            return True

    def weight(self, key):
        if key in self.rates:
            return self.rates[key]
        known = sorted(v for v in self.rates.values() if v > 0)
        return known[len(known) // 2] if known else 1.0

    def loads(self, owners=None):
        """Per-shard msg/s for key -> shard owners (default: applied owners)."""
        owners = self.current_owners() if owners is None else owners
        loads = [0.0] * self.count
        for key, shard in owners.items():
            if shard < self.count:
                loads[shard] += self.weight(key)
        return loads

    def current_owners(self):
        owners = {}
        for asset, shard in self.assignment.items():
            key = self.groups.get(asset)
            if key is not None:
                owners.setdefault(key, shard)
        return owners

    @staticmethod
    def imbalance(loads):
        mean = sum(loads) / len(loads) if loads else 0
        return (max(loads) - min(loads)) / mean if mean else 0

    def place(self, keys, previous=None):
        """Owners for ``keys``: retain known owners, put new groups on the lightest shard."""
        previous = dict(self.owners if previous is None else previous)
        owners = {k: previous[k] for k in keys if k in previous and previous[k] < self.count}
        loads = self.loads(owners)
        for key in sorted((k for k in keys if k not in owners), key=lambda k: (-self.weight(k), k)):
            shard = min(range(self.count), key=lambda i: (loads[i], i))
            owners[key] = shard
            loads[shard] += self.weight(key)
        return owners

    def search(self, owners, budget, movable=None):
        """Greedy whole-group moves or swaps, each lowering (max/min, sum of squares).

        Only steps touching the heaviest or lightest shard can lower the ratio,
        so candidates are moves off the heaviest, moves onto the lightest and
        swaps with either; the squared term breaks ties toward even loads.
        Returns (owners, loads, moved keys); at most ``budget`` groups move.
        """
        owners = dict(owners)
        weights = {k: self.weight(k) for k in owners}
        loads = [0.0] * self.count
        for key, shard in owners.items():
            loads[shard] += weights[key]
        movable = sorted(owners if movable is None else movable)
        moved = set()

        def score(values):
            return min(ratio(values), 1e6), round(sum(v * v for v in values), 6)

        def shifted(a, b, delta):
            trial = list(loads)
            trial[a] -= delta
            trial[b] += delta
            return trial

        while len(moved) < budget:
            hi = max(range(self.count), key=lambda i: (loads[i], -i))
            lo = min(range(self.count), key=lambda i: (loads[i], i))
            best, best_score = None, score(loads)
            members = {i: [k for k in movable if owners[k] == i] for i in range(self.count)}
            for key in movable:
                a = owners[key]
                for b in (range(self.count) if a == hi else (lo,)):
                    if b != a and score(trial := shifted(a, b, weights[key])) < best_score:
                        best, best_score = ((key, b),), score(trial)
            if len(moved) + 2 <= budget:
                for a in {hi, lo}:
                    for key in members[a]:
                        for b in range(self.count):
                            if b == a:
                                continue
                            for other in members[b]:
                                trial = shifted(a, b, weights[key] - weights[other])
                                if score(trial) < best_score:
                                    best, best_score = ((key, b), (other, a)), score(trial)
            if best is None:
                break
            for key, shard in best:
                loads[owners[key]] -= weights[key]
                loads[shard] += weights[key]
                owners[key] = shard
                moved.add(key)
        return owners, loads, moved

    def ideal(self, keys):
        """Max/min ratio of an unconstrained layout: the yardstick for acting."""
        owners, loads = {}, [0.0] * self.count
        weights = {k: self.weight(k) for k in keys}
        for key in sorted(keys, key=lambda k: (-weights[k], k)):
            shard = min(range(self.count), key=lambda i: (loads[i], i))
            owners[key] = shard
            loads[shard] += weights[key]
        return ratio(self.search(owners, len(owners))[1])

    def plan(self, owners):
        """Up to REBALANCE_MAX_GROUPS whole-group moves, or None.

        Acts when the heaviest/lightest ratio exceeds REBALANCE_RATIO and is
        worse than the achievable ratio by REBALANCE_HYSTERESIS, or above
        REBALANCE_TARGET_RATIO while the target is achievable. An indivisible
        hot group therefore never causes churn. Unmeasured groups stay put.
        """
        before = self.loads(owners)
        if not any(before) or ratio(before) <= self.trigger:
            return None
        ideal = self.ideal(owners)
        if ratio(before) <= min(ideal * (1 + self.threshold), max(self.target, ideal * 1.02)):
            return None
        target, loads, moved = self.search(owners, self.max_moves, [k for k in owners if k in self.measured])
        if not moved or ratio(loads) >= min(ratio(before), 1e6) * .99:
            return None
        return target

    def metrics(self):
        with self.lock:
            loads = self.loads()
            live = set(self.groups.values())
            return dict(ws_shard_loads=loads, ws_load_imbalance=self.imbalance(loads),
                        ws_load_ratio=ratio(loads) if math.isfinite(ratio(loads)) else None,
                        ws_rebalance_hysteresis=self.threshold, ws_rebalance_ratio=self.trigger,
                        ws_rebalance_target_ratio=self.target,
                        ws_group_rates={k: round(v, 3) for k, v in sorted(
                            self.rates.items(), key=lambda kv: -kv[1]) if k in live},
                        ws_unmeasured_groups=len(live - self.measured),
                        ws_last_rebalance_monotonic=(
                            self.last_rebalance if math.isfinite(self.last_rebalance) else None))


class DailyStats:
    """Cumulative per-session rows make retries after ambiguous commits idempotent."""
    def __init__(self):
        self.session = uuid4().hex
        self.lock = threading.RLock()
        self.flush_lock = threading.Lock()
        self.days = {}
        self.dirty = set()

    def increment(self, name, shard=None, day=None):
        day = day or datetime.now(timezone.utc).date().isoformat()
        with self.lock:
            row = self.days.setdefault(day, dict(reconnect_count=0, slow_consumer_count=0,
                                                rebalance_count=0, shard_msg_totals={}))
            if shard is None:
                row[name] += 1
            else:
                key = str(shard)
                row['shard_msg_totals'][key] = row['shard_msg_totals'].get(key, 0) + 1
            self.dirty.add(day)

    def message(self, shard):
        self.increment('message', shard)

    def persist(self):
        with self.flush_lock:
            with self.lock:
                rows = {day: json.loads(json.dumps(self.days[day])) for day in self.dirty}
            if not rows:
                return
            with get_db_connection() as db:
                cur = db.cursor()
                try:
                    for day, row in rows.items():
                        cur.execute('INSERT INTO qd_polymarket_ws_daily(day,session_id,reconnect_count,slow_consumer_count,rebalance_count,shard_msg_totals) VALUES (%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(day,session_id) DO UPDATE SET reconnect_count=EXCLUDED.reconnect_count,slow_consumer_count=EXCLUDED.slow_consumer_count,rebalance_count=EXCLUDED.rebalance_count,shard_msg_totals=EXCLUDED.shard_msg_totals,updated_at=NOW()',
                                    (day, self.session, row['reconnect_count'], row['slow_consumer_count'], row['rebalance_count'], json.dumps(row['shard_msg_totals'])))
                    db.commit()
                finally:
                    cur.close()
            with self.lock:
                for day, row in rows.items():
                    if self.days[day] == row:
                        self.dirty.discard(day)

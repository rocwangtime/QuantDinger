"""Public-feed load measurement and UTC session checkpoints (no external I/O on receive)."""
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from .observer_db import connection as get_db_connection


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


def metric_ratio(loads):
    """Expose undefined ratios as null; keep ratio() for planning decisions."""
    value = ratio(loads)
    return value if math.isfinite(value) else None


class RateBalancer:
    """Ingress frame rates per balance group and bounded whole-group moves.

    Workers attribute parsed frames to groups; socket callbacks independently
    count raw frames/bytes. Samples spanning backlog, loss or recovery are frozen
    by the manager. One frame's cost is split between its owned assets.
    """
    def __init__(self, count):
        self.count = count
        self.lock = threading.Lock()
        self.assignment, self.groups, self.pending = {}, {}, {}
        self.byte_rates, self.pending_bytes = {}, {}
        self.ingress_byte_rates = {}
        self.forced_hot = set()
        self.rates, self.owners, self.measured, self.since = {}, {}, set(), {}
        self.sample_at = time.monotonic()
        self.last_check = self.sample_at
        self.last_rebalance = float('-inf')
        self.interval = setting('REBALANCE_SECONDS', 60, 1)
        self.minimum = setting('REBALANCE_MIN_SECONDS', 120, 1)
        self.threshold = setting('REBALANCE_HYSTERESIS', .1, 0)
        self.group_cooldown = setting('GROUP_COOLDOWN_SECONDS', 300, 0)
        self.confirmations = int(setting('REBALANCE_CONFIRMATIONS', 2, 1))
        self.min_gain = setting('REBALANCE_MIN_MSG_PER_S', 10, 0)
        self.hot_rate = setting('HOT_GROUP_MSG_PER_S', 100, 0)
        self.hot_share = setting('HOT_GROUP_SHARE', .15, 0)
        self.moved_at, self.unavailable, self.dedicated = {}, set(), set()
        self.pressure, self.pressure_samples = None, 0
        self.trigger = setting('REBALANCE_RATIO', 1.5, 1)
        self.target = setting('REBALANCE_TARGET_RATIO', 2, 1)
        self.max_moves = int(setting('REBALANCE_MAX_GROUPS', 8, 1))
        self.half_life = setting('RATE_HALF_LIFE_SECONDS', 30, 1)
        self.max_assets = int(setting('ASSETS_PER_SHARD', 12, 2))
        self.max_load = setting('MAX_MSG_PER_S', 300, 1)
        self.max_bytes = setting('MAX_BYTES_PER_S', 262144, 1)

    def set_groups(self, groups, now=None):
        """Publish asset -> balance key; keys keep their rate and owner."""
        with self.lock:
            now = time.monotonic() if now is None else now
            self.groups = dict(groups)
            live = set(groups.values())
            # Bound catalogue history; series keys survive replacement of windows.
            for values in (self.rates, self.byte_rates, self.ingress_byte_rates, self.pending_bytes, self.owners, self.since, self.moved_at, self.pending):
                for key in values.keys() - live:
                    del values[key]
            self.measured.intersection_update(live)
            self.dedicated.intersection_update(live)
            for key in set(groups.values()):
                self.since.setdefault(key, now)

    def receive(self, event, shard, raw_bytes=0):
        """Attribute one drained frame. ``event`` is parsed JSON or raw text."""
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
                    self.pending_bytes[key] = self.pending_bytes.get(key, 0) + share * raw_bytes

    def sample(self, now, unavailable=()):
        """Fold counts since the last sample into per-group EWMA msg/s."""
        with self.lock:
            elapsed = now - self.sample_at
            if elapsed <= 0:
                return False
            counts, self.pending = self.pending, {}
            byte_counts, self.pending_bytes = self.pending_bytes, {}
            blocked = self.unavailable | set(unavailable)
            self.unavailable = set(unavailable)
            for key in set(self.groups.values()):
                # Never interpret an outage/backlog (or partial recovery window) as quiet traffic.
                if key in blocked:
                    continue
                # A group that appeared mid-window is measured from its start.
                window = min(elapsed, now - self.since.get(key, self.sample_at))
                if window < min(elapsed, self.interval) / 2:
                    continue
                rate = counts.get(key, 0) / window
                byte_rate = byte_counts.get(key, 0) / window
                if key in self.measured:
                    alpha = 1 - math.exp(-math.log(2) * window / self.half_life)
                    self.rates[key] += alpha * (rate - self.rates[key])
                    self.byte_rates[key] = self.byte_rates.get(key, byte_rate) + alpha * (
                        byte_rate - self.byte_rates.get(key, byte_rate))
                else:
                    self.rates[key] = rate
                    self.byte_rates[key] = byte_rate
                    self.measured.add(key)
            self.sample_at = now
            return True

    def weight(self, key):
        if key in self.rates:
            return self.rates[key]
        known = sorted(v for v in self.rates.values() if v > 0)
        return known[len(known) // 2] if known else 1.0

    def byte_weight(self, key):
        if key in self.ingress_byte_rates:
            return max(self.ingress_byte_rates[key], self.byte_rates.get(key, 0))
        if key in self.byte_rates:
            return self.byte_rates[key]
        known = sorted(v for v in self.byte_rates.values() if v > 0)
        return known[len(known) // 2] if known else 0

    def planning_weight(self, key):
        return max(self.weight(key), self.byte_weight(key) / self.max_bytes * self.max_load)

    def planning_loads(self, owners):
        values = [0.] * self.count
        for key, shard in owners.items():
            values[shard] += self.planning_weight(key)
        return values

    def byte_loads(self, owners=None):
        values = [0.] * self.count
        for key, shard in (self.current_owners() if owners is None else owners).items():
            values[shard] += self.byte_weight(key)
        return values

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

    def place(self, keys, previous=None, optimize=True):
        """Owners for ``keys``: retain known owners, put new groups on the lightest shard."""
        previous = dict(self.owners if previous is None else previous)
        owners = {k: previous[k] for k in keys if k in previous and previous[k] < self.count}
        loads = self.planning_loads(owners)
        for key in sorted((k for k in keys if k not in owners), key=lambda k: (k not in self.forced_hot, -self.planning_weight(k), k)):
            shard = min(range(self.count), key=lambda i: (
                self.forced_collisions(dict(owners, **{key: i})),
                self.capacity_cost(dict(owners, **{key: i})), loads[i], i))
            owners[key] = shard
            loads[shard] += self.planning_weight(key)
        if optimize and previous and any(self.capacity_cost(owners)):
            owners, _, _ = self.search(owners, min(32, len(owners)))
        return owners

    def asset_counts(self, owners):
        counts = [0] * self.count
        for key in self.groups.values():
            if key in owners:
                counts[owners[key]] += 1
        return counts

    def capacity_cost(self, owners):
        """Excess is explicit when an indivisible group or eight sockets cannot fit."""
        return (sum(max(0, n - self.max_assets) for n in self.asset_counts(owners)),
                round(sum(max(0, n - self.max_load) for n in self.loads(owners)), 6),
                round(sum(max(0, n - self.max_bytes) for n in self.byte_loads(owners)), 6))

    def capacity_details(self, owners):
        """Planning budgets, not a documented upstream connection limit.

        Show indivisible-series floors separately from co-tenant excess; sums
        across logical primary shards exclude redundant and migration copies.
        """
        counts, loads, byte_loads = self.asset_counts(owners), self.loads(owners), self.byte_loads(owners)
        rows = []
        for index in range(self.count):
            keys = [k for k, i in owners.items() if i == index]
            raw = (max(0, counts[index] - self.max_assets), max(0, loads[index] - self.max_load),
                   max(0, byte_loads[index] - self.max_bytes))
            floor = (sum(max(0, sum(v == k for v in self.groups.values()) - self.max_assets) for k in keys),
                     sum(max(0, self.weight(k) - self.max_load) for k in keys),
                     sum(max(0, self.byte_weight(k) - self.max_bytes) for k in keys))
            rows.append(dict(shard=index, groups=keys, excess=raw, indivisible_excess=floor,
                             co_tenant_excess=tuple(max(0, a - b) for a, b in zip(raw, floor)),
                             estimated_groups=[k for k in keys if k not in self.measured]))
        return rows

    def hot_groups(self, owners):
        """Reserve at least one shared shard; isolate dominant measured groups."""
        total = sum(self.planning_weight(k) for k in owners)
        threshold = max(self.hot_rate, total * self.hot_share)
        candidates = [k for k in owners if k in self.measured and
                      self.planning_weight(k) >= threshold * (1 - self.threshold if k in self.dedicated else 1)]
        candidates = set(candidates) | (self.forced_hot & owners.keys())
        self.dedicated = set(sorted(candidates, key=lambda k: (k not in self.forced_hot, -self.planning_weight(k), k))[:max(0, self.count - 1)])
        return self.dedicated

    def isolation_cost(self, owners, hot):
        # The load sharing a dominant group's connection. Counting a hot/hot
        # collision twice makes separating two dominant groups the first move.
        return sum(self.planning_weight(other) for key in hot for other in owners
                   if other != key and owners[other] == owners[key])

    def forced_collisions(self, owners):
        # Structural isolation must work before rates exist, and cannot lose to
        # small capacity improvements or a frozen/unavailable measurement window.
        return sum(other != key and owners[other] == owners[key]
                   for key in self.forced_hot & owners.keys() for other in owners)

    def search(self, owners, budget, movable=None):
        """Bounded moves/swaps minimizing hot co-tenancy, peak, then squares.

        Idle connections are valid. Raising the minimum by moving cold traffic
        onto a saturated connection is never a goal. Each group moves at most
        once per plan, and at most ``budget`` distinct groups change owner.
        """
        owners = dict(owners)
        movable = sorted(owners if movable is None else movable)
        # Keep the quadratic swap search bounded even for large catalogues.
        movable = sorted(movable, key=lambda k: (-self.planning_weight(k), k))[:32]
        hot = self.hot_groups(owners)
        moved = set()

        def score(layout):
            values = self.planning_loads(layout)
            return (self.forced_collisions(layout), *self.capacity_cost(layout), round(self.isolation_cost(layout, hot), 6), round(max(values), 6),
                    round(sum(v * v for v in values), 6))

        while len(moved) < budget:
            best, best_score = None, score(owners)
            available = [k for k in movable if k not in moved]
            for key in available:
                for shard in range(self.count):
                    if shard == owners[key]:
                        continue
                    trial = dict(owners, **{key: shard})
                    value = score(trial)
                    if value < best_score:
                        best, best_score = {key: shard}, value
            if len(moved) + 2 <= budget:
                for i, key in enumerate(available):
                    for other in available[i + 1:]:
                        if owners[key] == owners[other]:
                            continue
                        change = {key: owners[other], other: owners[key]}
                        value = score({**owners, **change})
                        if value < best_score:
                            best, best_score = change, value
            if best is None:
                break
            owners.update(best)
            moved.update(best)
        return owners, self.planning_loads(owners), moved

    def ideal(self, keys):
        """Legacy ratio diagnostic; a large value need not mean overload."""
        return ratio(self.loads(self.place(keys, {})))

    def applied(self, old, new, now):
        moved = {k for k in new if k in old and old[k] != new[k]}
        if moved:
            self.last_rebalance = now
            self.moved_at.update({k: now for k in moved})
            self.pressure, self.pressure_samples = None, 0

    def overloaded(self, owners):
        """Rate caps have an entry band; asset capacity is an exact bound."""
        return (max(self.asset_counts(owners)) > self.max_assets
                or max(self.loads(owners)) > self.max_load * (1 + self.threshold)
                or max(self.byte_loads(owners)) > self.max_bytes * (1 + self.threshold))

    def plan(self, owners, now=None):
        """Require sustained, meaningful improvement and global/per-group cooldown.

        The physical lower bound is max(largest indivisible group, total/N).
        Once the peak is within hysteresis of that bound and hot connections
        are isolated, a high max/min ratio provides no reason to migrate.
        """
        now = time.monotonic() if now is None else now
        before = self.planning_loads(owners)
        hot = self.hot_groups(owners)
        bound = max([sum(before) / self.count, *(self.planning_weight(k) for k in owners)])
        isolation = self.isolation_cost(owners, hot)
        overloaded = self.overloaded(owners)
        if (not overloaded and (not any(before) or (isolation <= max(1e-6, self.min_gain) and
                max(before) <= bound * (1 + self.threshold)))):
            self.pressure, self.pressure_samples = None, 0
            return None
        pressure = (max(range(self.count), key=lambda i: before[i]), frozenset(hot))
        self.pressure_samples = self.pressure_samples + 1 if pressure == self.pressure else 1
        self.pressure = pressure
        if self.pressure_samples < self.confirmations or now - self.last_rebalance < self.minimum:
            return None
        movable = [k for k in owners if k in self.measured and k not in self.unavailable
                   and now - self.moved_at.get(k, float('-inf')) >= self.group_cooldown]
        target, loads, moved = self.search(owners, self.max_moves, movable)
        peak_gain = max(before) - max(loads)
        isolation_gain = isolation - self.isolation_cost(target, hot)
        capacity_gain = overloaded and self.capacity_cost(target) < self.capacity_cost(owners)
        if not moved or (not capacity_gain and peak_gain < max(self.min_gain, max(before) * self.threshold)
                         and (isolation_gain <= 1e-6 or
                              isolation_gain < max(self.min_gain, isolation * self.threshold))):
            return None
        return target

    def metrics(self):
        with self.lock:
            loads = self.loads()
            live = set(self.groups.values())
            return dict(ws_shard_loads=loads, ws_load_imbalance=self.imbalance(loads),
                        ws_load_ratio=metric_ratio(loads),
                        ws_rebalance_hysteresis=self.threshold, ws_rebalance_ratio=self.trigger,
                        ws_rebalance_target_ratio=self.target,
                        ws_peak_lower_bound=max([sum(loads) / self.count,
                                                 *(self.weight(k) for k in self.current_owners())]),
                        ws_hot_groups=sorted(self.hot_groups(self.current_owners())),
                        ws_hot_co_tenant_msg_per_s=self.isolation_cost(
                            self.current_owners(), self.hot_groups(self.current_owners())),
                        ws_rebalance_pressure_samples=self.pressure_samples,
                        ws_group_cooldown_seconds=self.group_cooldown,
                        ws_max_assets_per_shard=self.max_assets, ws_max_msg_per_s=self.max_load,
                        ws_max_bytes_per_s=self.max_bytes, ws_shard_byte_loads=self.byte_loads(),
                        ws_group_byte_rates={k: round(v, 3) for k, v in self.byte_rates.items() if k in live},
                        ws_group_ingress_byte_rates={k: round(v, 3) for k, v in self.ingress_byte_rates.items() if k in live},
                        ws_capacity_excess=self.capacity_cost(self.current_owners()),
                        ws_capacity_scope='logical_primary_planning_budget',
                        ws_capacity_by_shard=self.capacity_details(self.current_owners()),
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

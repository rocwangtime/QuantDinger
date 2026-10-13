"""Public-feed load measurement and UTC session checkpoints (no external I/O on receive)."""
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
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


class RateBalancer:
    def __init__(self, count):
        self.count = count
        self.lock = threading.RLock()
        self.assignment, self.rates, self.pending = {}, {}, {}
        self.sample_at = time.monotonic()
        self.last_check = self.sample_at
        self.last_rebalance = float('-inf')
        self.interval = setting('REBALANCE_SECONDS', 60, 1)
        self.minimum = setting('REBALANCE_MIN_SECONDS', 120, 1)
        self.threshold = setting('REBALANCE_HYSTERESIS', .25, 0)
        self.half_life = setting('RATE_HALF_LIFE_SECONDS', 30, 1)

    def set_assets(self, assignment):
        with self.lock:
            # Applied ownership is changed only after wire updates succeed.
            self.rates = {a: self.rates.get(a, 0) for a in assignment}
            self.pending = {a: self.pending.get(a, 0) for a in assignment}

    def receive(self, raw, shard):
        try:
            assets = event_assets(json.loads(raw))
        except (ValueError, TypeError, AttributeError):
            return
        with self.lock:
            for asset in assets:
                # Count a frame once per token and ignore handover duplicates.
                if self.assignment.get(asset) == shard:
                    self.pending[asset] = self.pending.get(asset, 0) + 1

    def loads(self, assignment):
        loads = [0.0] * self.count
        for asset, shard in assignment.items():
            loads[shard] += self.rates.get(asset, 0)
        return loads

    @staticmethod
    def imbalance(loads):
        mean = sum(loads) / len(loads)
        return (max(loads) - min(loads)) / mean if mean else 0

    def propose(self, now, reconnect=False):
        with self.lock:
            if not reconnect and now - self.last_check < self.interval:
                return None
            self.last_check = now
            elapsed = now - self.sample_at
            if elapsed > 0:
                alpha = 1 - math.exp(-math.log(2) * elapsed / self.half_life)
                for asset in self.rates:
                    self.rates[asset] += alpha * (self.pending.get(asset, 0) / elapsed - self.rates[asset])
                self.pending = {}
                self.sample_at = now
            if now - self.last_rebalance < self.minimum:
                return None
            old = self.imbalance(self.loads(self.assignment))
            if old <= self.threshold:
                return None
            # Largest-first bins; remap equivalent bins to retain the most owners.
            bins, loads = {}, [0.0] * self.count
            for asset in sorted(self.assignment, key=lambda a: (-self.rates.get(a, 0), a)):
                shard = min(range(self.count), key=lambda i: (loads[i], i))
                bins[asset] = shard
                loads[shard] += self.rates.get(asset, 0)
            overlap = [[0] * self.count for _ in range(self.count)]
            for asset, index in bins.items():
                overlap[index][self.assignment[asset]] += 1

            @lru_cache(None)
            def match(index, used):
                if index == self.count:
                    return 0, ()
                candidates = []
                for shard in range(self.count):
                    if not used & (1 << shard):
                        score, tail = match(index + 1, used | (1 << shard))
                        candidates.append((score + overlap[index][shard], (shard, *tail)))
                return max(candidates, key=lambda item: item[0])

            mapping = match(0, 0)[1]
            target = {a: mapping[i] for a, i in bins.items()}
            if old - self.imbalance(self.loads(target)) <= self.threshold:
                return None
            # Apply only the smallest prefix of beneficial LPT moves that reaches
            # the hysteresis band. This avoids remapping already balanced tokens.
            result = dict(self.assignment)
            remaining = {a for a in target if target[a] != result[a]}
            loads = self.loads(result)
            while remaining:
                def moved_loads(asset):
                    moved = list(loads)
                    moved[result[asset]] -= self.rates.get(asset, 0)
                    moved[target[asset]] += self.rates.get(asset, 0)
                    return moved
                asset = min(remaining, key=lambda a: (self.imbalance(moved_loads(a)), a))
                loads = moved_loads(asset)
                result[asset] = target[asset]
                remaining.remove(asset)
                if self.imbalance(loads) <= self.threshold:
                    break
            return result if old - self.imbalance(loads) > self.threshold else None

    def metrics(self):
        with self.lock:
            loads = self.loads(self.assignment)
            return dict(ws_shard_loads=loads, ws_load_imbalance=self.imbalance(loads),
                        ws_rebalance_hysteresis=self.threshold, ws_last_rebalance_monotonic=(
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

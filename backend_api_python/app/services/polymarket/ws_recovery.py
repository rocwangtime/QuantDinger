"""Prepared, pooled unauthenticated /books repairs, off the reader/supervisor."""
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests

from .client import CLOB
from . import recording


class SnapshotRecovery:
    def __init__(self):
        self.lock = threading.Lock()
        self.starts = deque()
        self.active = set()
        self.slots = [dict(session=None) for _ in range(4)]
        self.closed = False
        self.requests = self.failures = self.completed = self.throttled = self.warmups = self.pressure_requests = 0
        self.latencies = deque(maxlen=128)

    def prepare(self, state, assets, parallel=False):
        """Prepare current token bodies before a fault; never cache old quotes."""
        assets = tuple(sorted(set(assets)))
        if state.get('_rest_assets') == assets and state.get('_rest_parallel') == parallel:
            return
        state['_rest_assets'], state['_rest_parallel'] = assets, parallel
        groups = [assets] + ([(a,) for a in assets] if parallel and len(assets) == 2 else [])
        state['_rest_prepared'] = {group: requests.Request('POST', f'{CLOB}/books',
            json=[dict(token_id=a) for a in group]).prepare() for group in groups}

    def request(self, state, assets, publish, warm=False, pressure=False):
        assets = tuple(sorted(set(assets)))
        if not assets:
            return False
        if state.get('_rest_assets') != assets:
            self.prepare(state, assets, state.get('_rest_parallel', False))
        prepared = state['_rest_prepared']
        now = time.monotonic()
        with self.lock:
            while self.starts and self.starts[0] <= now - 10:
                self.starts.popleft()
            identity = id(state)
            if self.closed or identity in self.active or now < state.get('rest_retry_at', 0):
                return False
            # Warm only an idle pool; leave slots available for actual outages.
            if warm and self.active:
                return False
            parallel = state.get('_rest_parallel') and len(assets) == 2 and len(self.slots) >= 2
            groups = [(a,) for a in assets] if parallel else [assets]
            budget = 24 if warm else 32 if pressure else 40
            if len(self.slots) < len(groups) or len(self.starts) + len(groups) > budget:
                self.throttled += 1
                return False
            self.active.add(identity)
            remaining = [len(groups)]
            state['rest_retry_at'] = now + .5
            jobs = [(group, self.slots.pop()) for group in groups]
            self.starts.extend([now] * len(jobs))
            self.requests += len(jobs)
            self.warmups += int(warm)
            self.pressure_requests += int(pressure)

        def run(group, slot):
            began = time.monotonic()
            try:
                started = datetime.now(timezone.utc)
                session = slot['session']
                if session is None:
                    session = slot['session'] = requests.Session()
                    session.trust_env = False
                # Explicit prepared body has no env auth, cookies or redirects.
                response = session.send(prepared[group], timeout=(.4, 1), allow_redirects=False)
                try:
                    response.raise_for_status()
                    if len(response.content) > 8 * 1024 * 1024:
                        raise ValueError('oversize public snapshot')
                    rows = response.json()
                finally:
                    response.close()
                if not isinstance(rows, list) or len(rows) != len(group):
                    raise ValueError('incomplete public snapshot')
                events, seen = [], set()
                for row in rows:
                    asset = str(row.get('asset_id', ''))
                    at = recording.exchange_time(row.get('timestamp'))
                    if asset not in group or asset in seen or at is None:
                        raise ValueError('invalid public snapshot identity/time')
                    if abs((datetime.now(timezone.utc) - at).total_seconds()) > 2:
                        raise ValueError('stale public snapshot')
                    seen.add(asset)
                    for side in ('bids', 'asks'):
                        if not isinstance(row.get(side), list) or any(
                                not isinstance(level, dict) or 'price' not in level or 'size' not in level
                                for level in row[side]):
                            raise ValueError('invalid public snapshot levels')
                    events.append(dict(event_type='book', asset_id=asset, timestamp=row['timestamp'],
                                       bids=row['bids'], asks=row['asks']))
                if not warm:
                    publish(events, started)
                with self.lock:
                    self.completed += 1
            except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
                with self.lock:
                    self.failures += 1
            finally:
                with self.lock:
                    self.latencies.append((time.monotonic() - began) * 1000)
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        self.active.discard(identity)
                    if self.closed:
                        if slot['session']:
                            slot['session'].close()
                    else:
                        self.slots.append(slot)
        with self.lock:
            if self.closed:
                self.active.discard(identity)
                for _, slot in jobs:
                    if slot['session']:
                        slot['session'].close()
                return False
            for group, slot in jobs:
                threading.Thread(target=run, args=(group, slot),
                    name='polymarket-public-book', daemon=True).start()
        return True

    def close(self):
        with self.lock:
            self.closed = True
            for slot in self.slots:
                if slot['session']:
                    slot['session'].close()
            self.slots.clear()

    def metrics(self):
        with self.lock:
            samples = sorted(self.latencies)
            return dict(requests=self.requests, completed=self.completed, failures=self.failures,
                        inflight=len(self.active), inflight_http=4-len(self.slots) if not self.closed else 0,
                        throttled=self.throttled, limit_per_10s=40, warmups=self.warmups, pressure_requests=self.pressure_requests,
                        warm_limit_per_10s=24, pressure_limit_per_10s=32,
                        latency_ms=dict(samples=len(samples), max=max(samples, default=0),
                            p95=samples[min(len(samples)-1, int(len(samples)*.95))] if samples else None))

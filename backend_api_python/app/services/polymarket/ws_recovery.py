"""Prepared, pooled unauthenticated /books repairs, off the reader/supervisor."""
import logging
import threading
import time
from collections import deque, Counter
from datetime import datetime, timezone

import requests

from .client import CLOB
from . import recording

_LOG = logging.getLogger(__name__)
# A /books answer whose book timestamp is this far from local time is stale.
STALE_SECONDS = 2
# Retry delay after stale answers. The server keeps serving the same cached
# book for a while (1.6-4.4s seen on 2026-10-20), so 0.5s retries only fail.
# The first stale answer of a streak waits 1s (a live book is rarely stale
# twice), the next ones 5s. A completed repair ends the streak.
STALE_RETRY_SECONDS = 5
FAILURE_LOG_SECONDS = 30


class SnapshotRejected(ValueError):
    """A /books answer that cannot be used. `reason` is the counter key."""

    def __init__(self, reason, message, age_s=None):
        super().__init__(message)
        self.reason = reason
        self.age_s = age_s


def failure_reason(exc):
    """Stable counter key for a failed /books repair."""
    if isinstance(exc, SnapshotRejected):
        return exc.reason
    if isinstance(exc, requests.ConnectTimeout):
        return 'connect_timeout'
    if isinstance(exc, requests.ReadTimeout):
        return 'read_timeout'
    if isinstance(exc, requests.HTTPError):
        status = getattr(getattr(exc, 'response', None), 'status_code', None)
        return f'http_{status}' if status else 'http_error'
    if isinstance(exc, requests.ConnectionError):
        return 'connection_error'
    if isinstance(exc, requests.RequestException):
        return 'request_error'
    if isinstance(exc, ValueError):
        return 'bad_json'
    return 'error_' + type(exc).__name__


class SnapshotRecovery:
    def __init__(self):
        self.lock = threading.Lock()
        self.starts = deque()
        self.active = set()
        self.slots = [dict(session=None) for _ in range(4)]
        self.closed = False
        self.requests = self.failures = self.completed = self.throttled = self.warmups = self.pressure_requests = 0
        self.latencies = deque(maxlen=128)
        self.counts = {kind: Counter() for kind in ('recovery', 'warmup', 'pressure')}
        # Rolling hour at one-second resolution; fixed 3600 bucket bound.
        # hour_totals is the running sum of every retained bucket: added on
        # count, subtracted on expiry, so metrics() never re-sums the hour.
        self.hour = {}
        self.hour_totals = Counter()
        self.background_starts = deque()
        self.background_active = 0
        self.last_http_at = -float('inf')
        self.failure_reasons = {kind: Counter() for kind in self.counts}
        self.last_failure = None
        self.logged_at = -float('inf')
        self.unlogged = Counter()

    def count(self, kind, field, now, amount=1):
        self.counts[kind][field] += amount
        bucket = self.hour.setdefault(int(now), Counter())
        bucket[kind, field] += amount
        self.hour_totals[kind, field] += amount
        self.expire_hour(now)

    def expire_hour(self, now):
        while self.hour and next(iter(self.hour)) <= int(now) - 3600:
            for key, amount in self.hour.pop(next(iter(self.hour))).items():
                left = self.hour_totals[key] - amount
                if left:
                    self.hour_totals[key] = left
                else:
                    del self.hour_totals[key]

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
        with self.lock:
            now = time.monotonic()
            while self.starts and self.starts[0] <= now - 10:
                self.starts.popleft()
            kind = 'warmup' if warm else 'pressure' if pressure else 'recovery'
            background = warm or pressure
            identity = (kind, id(state))
            retry_key = {'warmup': '_rest_warm_retry_at', 'pressure': '_rest_pressure_retry_at',
                         'recovery': 'rest_retry_at'}[kind]
            if self.closed or identity in self.active or now < state.get(retry_key, 0):
                return False
            # A background job never owns the recovery identity or cooldown.
            # Keep two HTTP slots and at least eight sliding-window credits
            # available for actual outages. No background task queue.
            self.count(kind, 'attempts', now)
            while self.background_starts and self.background_starts[0] <= now - 3600:
                self.background_starts.popleft()
            if background and (self.background_active or len(self.slots) < 3 or self.active
                               or warm and now - self.last_http_at < 60):
                state[retry_key] = now + 1
                self.count(kind, 'deferred', now)
                return False
            parallel = (not pressure and state.get('_rest_parallel')
                        and len(assets) == 2 and len(self.slots) >= 2)
            groups = [(a,) for a in assets] if parallel else [assets]
            budget = 24 if warm else 32 if pressure else 40
            if len(self.slots) < len(groups) + (2 if background else 0):
                state[retry_key] = now + .05
                self.throttled += 1
                self.count(kind, 'throttled', now)
                self.count(kind, 'deferred', now)
                return False
            if (len(self.starts) + len(groups) > budget
                    or background and len(self.background_starts) + len(groups) > 120):
                self.throttled += 1
                self.count(kind, 'throttled', now)
                state[retry_key] = max(now + .5, self.starts[0] + 10 if self.starts else now)
                if background and len(self.background_starts) + len(groups) > 120:
                    state[retry_key] = max(state[retry_key], self.background_starts[0] + 3600)
                return False
            self.active.add(identity)
            remaining = [len(groups)]
            state[retry_key] = now + (60 if warm else 30 if pressure else .5)
            jobs = [(group, self.slots.pop()) for group in groups]
            self.starts.extend([now] * len(jobs))
            self.last_http_at = now
            if background:
                self.background_active += 1
                self.background_starts.extend([now] * len(jobs))
            self.requests += len(jobs)
            self.count(kind, 'requests', now, len(jobs))
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
                        raise SnapshotRejected('oversize', 'oversize public snapshot')
                    rows = response.json()
                finally:
                    response.close()
                if not isinstance(rows, list) or len(rows) != len(group):
                    raise SnapshotRejected('incomplete', 'incomplete public snapshot')
                events, seen = [], set()
                for row in rows:
                    asset = str(row.get('asset_id', ''))
                    at = recording.exchange_time(row.get('timestamp'))
                    if asset not in group or asset in seen or at is None:
                        raise SnapshotRejected('identity', 'invalid public snapshot identity/time')
                    age = (datetime.now(timezone.utc) - at).total_seconds()
                    if abs(age) > STALE_SECONDS:
                        raise SnapshotRejected('stale', 'stale public snapshot', age)
                    seen.add(asset)
                    for side in ('bids', 'asks'):
                        if not isinstance(row.get(side), list) or any(
                                not isinstance(level, dict) or 'price' not in level or 'size' not in level
                                for level in row[side]):
                            raise SnapshotRejected('levels', 'invalid public snapshot levels')
                    events.append(dict(event_type='book', asset_id=asset, timestamp=row['timestamp'],
                                       bids=row['bids'], asks=row['asks']))
                if not warm:
                    try:
                        publish(events, started)
                    except (ValueError, TypeError, KeyError, AttributeError) as exc:
                        raise SnapshotRejected('publish_' + type(exc).__name__, str(exc)[:200]) from exc
                with self.lock:
                    self.completed += 1
                    self.count(kind, "completed", time.monotonic())
                    state.pop('_rest_stale_streak', None)
            except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError) as exc:
                self._failed(kind, state, retry_key, group, exc, began)
            finally:
                with self.lock:
                    self.latencies.append((time.monotonic() - began) * 1000)
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        self.active.discard(identity)
                        if background:
                            self.background_active -= 1
                    if self.closed:
                        if slot['session']:
                            slot['session'].close()
                    else:
                        self.slots.append(slot)
        with self.lock:
            if self.closed:
                self.active.discard(identity)
                if background:
                    self.background_active -= 1
                for _, slot in jobs:
                    if slot['session']:
                        slot['session'].close()
                return False
            for group, slot in jobs:
                threading.Thread(target=run, args=(group, slot),
                    name='polymarket-public-book', daemon=True).start()
        return True

    def _failed(self, kind, state, retry_key, group, exc, began):
        reason = failure_reason(exc)
        now = time.monotonic()
        detail = dict(at=datetime.now(timezone.utc).isoformat(), kind=kind, reason=reason,
                      assets=len(group), latency_ms=round((now - began) * 1000, 1),
                      age_s=round(exc.age_s, 3) if getattr(exc, 'age_s', None) is not None else None,
                      message=str(exc)[:160])
        with self.lock:
            self.failures += 1
            self.count(kind, "failures", now)
            self.failure_reasons[kind][reason] += 1
            self.last_failure = detail
            if reason == 'stale':
                # Same cached book for seconds: do not burn the 10s budget.
                streak = state['_rest_stale_streak'] = state.get('_rest_stale_streak', 0) + 1
                if streak > 1:
                    # First stale answer keeps the normal retry set at request start.
                    state[retry_key] = max(state.get(retry_key, 0), now + STALE_RETRY_SECONDS)
                    self.count(kind, 'stale_backoff', now)
            self.unlogged[reason] += 1
            emit = now - self.logged_at >= FAILURE_LOG_SECONDS
            if emit:
                summary, self.unlogged = dict(self.unlogged), Counter()
                self.logged_at = now
        if emit:
            _LOG.warning('Polymarket REST book repair failed kind=%s reason=%s assets=%s latency_ms=%s age_s=%s '
                         'since_last_log=%s', kind, reason, len(group), detail['latency_ms'], detail['age_s'], summary)

    def skipped_ended(self, kind, count):
        """Assets of a market past its end time: left to the WS push or retirement."""
        with self.lock:
            self.count(kind, 'skipped_ended', time.monotonic(), count)

    def close(self):
        with self.lock:
            self.closed = True
            for slot in self.slots:
                if slot['session']:
                    slot['session'].close()
            self.slots.clear()

    def metrics(self):
        with self.lock:
            self.expire_hour(time.monotonic())
            hourly = self.hour_totals  # O(1); equals sum(self.hour.values(), Counter())
            fields = ('attempts', 'requests', 'completed', 'failures', 'throttled', 'deferred',
                      'stale_backoff', 'skipped_ended')
            categories = {kind: dict(total={f: counts[f] for f in fields},
                rolling_hour={f: hourly[kind, f] for f in fields}) for kind, counts in self.counts.items()}
            samples = sorted(self.latencies)
            return dict(categories=categories, rolling_hour_seconds=3600, hour_buckets=len(self.hour),
                        background_limit_per_hour=120, reserved_recovery_slots=2,
                        requests=self.requests, completed=self.completed, failures=self.failures,
                        inflight=len(self.active), inflight_http=4-len(self.slots) if not self.closed else 0,
                        failure_reasons={kind: dict(reasons) for kind, reasons in self.failure_reasons.items()},
                        last_failure=dict(self.last_failure) if self.last_failure else None,
                        throttled=self.throttled, limit_per_10s=40, warmups=self.warmups, pressure_requests=self.pressure_requests,
                        warm_limit_per_10s=24, pressure_limit_per_10s=32,
                        latency_ms=dict(samples=len(samples), max=max(samples, default=0),
                            p95=samples[min(len(samples)-1, int(len(samples)*.95))] if samples else None))

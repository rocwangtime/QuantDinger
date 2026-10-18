"""Bounded shadow books and non-control traffic health for public feed rotation."""
import math
import random
import time
import threading
from functools import wraps

from . import recording
from .ws_balance import setting
from .ws_redundancy import FeedMerge


def reconnect_delay(attempts):
    # Full jitter, including the first retry; no synchronized immediate retries.
    return random.uniform(0, min(30, .5 * 2 ** min(6, max(0, attempts - 1))))


class DataHealth:
    def __init__(self, now):
        self.at = self.started = now
        self.total = self.previous = 0
        self.baseline = 0
        self.low_seconds = 0

    def sample(self, now, transport, wall_time=None):
        elapsed = now - self.at
        if elapsed < .8:
            return False
        rate = (self.total - self.previous) / elapsed
        self.at, self.previous = now, self.total
        # 5m markets naturally stop/start together. Do not learn that silence
        # as transport failure, including when discovery has not refreshed yet.
        phase = (time.time() if wall_time is None else wall_time) % 300
        excluded = (phase < setting('ROTATE_ROLLOVER_AFTER_SECONDS', 45, 0)
                    or phase >= 300 - setting('ROTATE_ROLLOVER_BEFORE_SECONDS', 15, 0))
        if excluded:
            self.low_seconds = 0
            return False
        healthy_pong = (transport.get('pong_age_seconds') is not None
                        and transport['pong_age_seconds'] <= 15
                        and transport.get('pongs', 0) > 0
                        and transport.get('pings', 0) - transport.get('pongs', 0) <= 1)
        low = (self.baseline >= setting('ROTATE_MIN_BYTES_PER_S', 65536, 1)
               and rate < min(self.baseline * .01, setting('ROTATE_MAX_QUIET_BYTES_PER_S', 1024, 1)))
        # Discovery/scheduler pauses are unknown samples, never consecutive quiet seconds.
        self.low_seconds = self.low_seconds + elapsed if low and healthy_pong and elapsed <= 2 else 0
        if not low:
            alpha = 1 - math.exp(-elapsed / 60)
            self.baseline += alpha * (rate - self.baseline)
        return now - self.started >= 120 and self.low_seconds >= setting('ROTATE_QUIET_SECONDS', 20, 1)


def shadow_locked(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return wrapped


class ShadowBooks:
    """Keep only current subscribed books, never a replay buffer or trade writer."""
    def __init__(self, assets):
        self.lock = threading.RLock()
        self.assets = frozenset(assets)
        self.books = {}
        self.minimum_times = {}
        self.recorder = recording.Recorder()
        self.recorder.enabled = False
        self.merge = FeedMerge({}, deduplicate=False)

    @shadow_locked
    def receive(self, event, received):
        from .observer import ws_apply_event
        event = self.merge.filter(event, 'shadow', self.assets)
        changed = ws_apply_event(event, self.books)
        self.recorder.receive(event, changed, {}, {}, received, self.assets)

    @shadow_locked
    def prune(self, assets):
        self.assets = frozenset(assets)
        self.books = {a: b for a, b in self.books.items() if a in self.assets}
        self.minimum_times = {a: at for a, at in self.minimum_times.items() if a in self.assets}
        self.recorder.prune(self.assets)
        self.merge.prune(self.assets)

    @shadow_locked
    def observe_active(self, metadata):
        # Keep the live watermark even if its failed worker clears live books.
        for asset in self.assets:
            at = metadata.get(asset, (None,))[0]
            previous = self.minimum_times.get(asset)
            if at is not None and (previous is None or at > previous):
                self.minimum_times[asset] = at

    @shadow_locked
    def ready(self, active_metadata):
        self.observe_active(active_metadata)
        if not self.assets or self.books.keys() != self.assets:
            return False
        for asset in self.assets:
            at = self.recorder.metadata.get(asset, (None,))[0]
            previous = self.minimum_times.get(asset)
            if at is None or previous is not None and at < previous:
                return False
        return True

"""Bounded shadow books and non-control traffic health for public feed rotation."""
import math

from . import recording
from .ws_balance import setting


def reconnect_delay(attempts):
    # Initial failure (including a long-lived connection) retries immediately.
    return 0 if attempts <= 1 else min(120, 2 ** min(7, attempts - 2))


class DataHealth:
    def __init__(self, now):
        self.at = self.started = now
        self.total = self.previous = 0
        self.baseline = 0
        self.low_seconds = 0

    def sample(self, now, transport):
        elapsed = now - self.at
        if elapsed < .8:
            return False
        rate = (self.total - self.previous) / elapsed
        self.at, self.previous = now, self.total
        healthy_pong = (transport.get('pong_age_seconds') is not None
                        and transport['pong_age_seconds'] <= 15
                        and transport.get('pongs', 0) > 0
                        and transport.get('pings', 0) - transport.get('pongs', 0) <= 1)
        low = self.baseline >= setting('ROTATE_MIN_BYTES_PER_S', 16384, 1) and rate < self.baseline * .05
        # Discovery/scheduler pauses are unknown samples, never consecutive quiet seconds.
        self.low_seconds = self.low_seconds + elapsed if low and healthy_pong and elapsed <= 2 else 0
        if not low:
            alpha = 1 - math.exp(-elapsed / 60)
            self.baseline += alpha * (rate - self.baseline)
        return now - self.started >= 60 and self.low_seconds >= setting('ROTATE_QUIET_SECONDS', 5, 1)


class ShadowBooks:
    """Keep only current subscribed books, never a replay buffer or trade writer."""
    def __init__(self, assets):
        self.assets = frozenset(assets)
        self.books = {}
        self.minimum_times = {}
        self.recorder = recording.Recorder()
        self.recorder.enabled = False

    def receive(self, event, received):
        from .observer import ws_apply_event
        event = recording.owned_events(event, self.assets)
        changed = ws_apply_event(event, self.books)
        self.recorder.receive(event, changed, {}, {}, received, self.assets)

    def observe_active(self, metadata):
        # Keep the live watermark even if its failed worker clears live books.
        for asset in self.assets:
            at = metadata.get(asset, (None,))[0]
            previous = self.minimum_times.get(asset)
            if at is not None and (previous is None or at > previous):
                self.minimum_times[asset] = at

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

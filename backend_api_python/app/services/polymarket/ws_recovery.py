"""Bounded, unauthenticated /books repairs, off the reader and supervisor."""
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
        self.requests = self.failures = self.completed = self.throttled = 0

    def request(self, state, assets, publish):
        now = time.monotonic()
        with self.lock:
            while self.starts and self.starts[0] <= now - 10:
                self.starts.popleft()
            # Deliberately reserve ample headroom below /books' 500/10s IP cap.
            # At most one in flight per socket, four overall, forty per window.
            identity = id(state)
            if identity in self.active or now < state.get('rest_retry_at', 0):
                return
            if len(self.active) >= 4 or len(self.starts) >= 40:
                self.throttled += 1
                return
            self.active.add(identity)
            self.starts.append(now)
            self.requests += 1
            state['rest_retry_at'] = now + .5
        assets = set(assets)
        def run():
            try:
                started = datetime.now(timezone.utc)
                # Small requests only, no credentials, redirects or default env auth.
                with requests.Session() as session:
                    session.trust_env = False
                    response = session.post(f'{CLOB}/books', json=[dict(token_id=a) for a in sorted(assets)],
                                            timeout=(.4, 1), allow_redirects=False)
                    response.raise_for_status()
                    if len(response.content) > 8 * 1024 * 1024:
                        raise ValueError('oversize public snapshot')
                    rows = response.json()
                if not isinstance(rows, list) or len(rows) != len(assets):
                    raise ValueError('incomplete public snapshot')
                events, seen = [], set()
                for row in rows:
                    asset = str(row.get('asset_id', ''))
                    at = recording.exchange_time(row.get('timestamp'))
                    if asset not in assets or asset in seen or at is None:
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
                publish(events, started)
                with self.lock:
                    self.completed += 1
            except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
                with self.lock:
                    self.failures += 1
            finally:
                with self.lock:
                    self.active.discard(identity)
        threading.Thread(target=run, name='polymarket-public-book-repair', daemon=True).start()

    def metrics(self):
        with self.lock:
            return dict(requests=self.requests, completed=self.completed, failures=self.failures,
                        inflight=len(self.active), throttled=self.throttled, limit_per_10s=40)

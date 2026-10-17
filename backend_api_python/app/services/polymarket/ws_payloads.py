"""Short, bounded sharing of immutable public frames across redundant readers."""
import hashlib
import json
import threading
import time
from collections import OrderedDict


class PublicEvent(dict):
    """Workers only read these dictionaries; fingerprints are not wire fields."""
    digest = None


def fingerprint(event):
    if isinstance(event, PublicEvent) and event.digest is not None:
        return event.digest
    digest = hashlib.blake2b(json.dumps(event, sort_keys=True, separators=(',', ':')).encode(),
                             digest_size=16).digest()
    if isinstance(event, PublicEvent):
        event.digest = digest
    return digest


def public_event(event):
    if isinstance(event, list):
        return [public_event(item) for item in event]
    if not isinstance(event, dict):
        return event
    event = PublicEvent(event)
    if isinstance(event.get('payload'), dict):
        event['payload'] = PublicEvent(event['payload'])
    payload = event.get('payload') or event
    for field in ('price_changes', 'priceChanges'):
        if isinstance(payload.get(field), list):
            if payload is not event:
                payload = PublicEvent(payload)
                event['payload'] = payload
            payload[field] = [PublicEvent(row) if isinstance(row, dict) else row for row in payload[field]]
    return event


class ParsedFrames:
    def __init__(self, maximum=512, byte_limit=1048576, ttl=2):
        self.lock = threading.Lock()
        self.items = OrderedDict()
        self.maximum, self.byte_limit, self.ttl = maximum, byte_limit, ttl
        self.bytes = self.hits = self.misses = 0

    def prune(self, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            self._prune(now)

    def _prune(self, now):
        while self.items and (next(iter(self.items.values()))[0] < now - self.ttl
                              or len(self.items) > self.maximum or self.bytes > self.byte_limit):
            _, (_, _, size) = self.items.popitem(last=False)
            self.bytes -= size

    def parse(self, raw):
        if not isinstance(raw, (str, bytes, bytearray)):
            return raw
        if isinstance(raw, bytearray):
            raw = bytes(raw)
        # Charge UTF-8's worst case for text, independent of frame contents.
        size = len(raw) * (4 if isinstance(raw, str) else 1)
        with self.lock:
            now = time.monotonic()
            self._prune(now)
            cached = self.items.get(raw)
            if cached:
                self.hits += 1
                return cached[1]
            event = public_event(json.loads(raw))
            self.misses += 1
            if size <= self.byte_limit:
                self.items[raw] = (now, event, size)
                self.bytes += size
                self._prune(now)
            return event

    def metrics(self):
        with self.lock:
            return dict(entries=len(self.items), charged_bytes=self.bytes, hits=self.hits, misses=self.misses)

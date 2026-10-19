"""Short, bounded sharing of immutable public frames across redundant readers."""
import hashlib
import json
import threading
import time
from collections import OrderedDict

try:
    import orjson
except ImportError:
    orjson = None


def loads(raw):
    return orjson.loads(raw) if orjson is not None else json.loads(raw)


def canonical(value):
    return (orjson.dumps(value, option=orjson.OPT_SORT_KEYS) if orjson is not None
            else json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


class PublicEvent(dict):
    """Workers only read these dictionaries; fingerprints are not wire fields."""
    digest = None
    identity = None
    book = None
    changes = None
    fanin = None


def materialized_book(payload):
    """Immutable parsed levels; each mutable live/shadow/history book copies them."""
    cached = payload.book if isinstance(payload, PublicEvent) else None
    if cached is not None:
        return cached
    levels = {side: {str(x['price']): str(x['size']) for x in payload.get(side, [])}
              for side in ('bids', 'asks')}
    charge = book_bytes(levels)
    result = (levels, charge)
    if isinstance(payload, PublicEvent):
        payload.book = result
    return result


def book_bytes(levels):
    return 1024 + sum(256 * len(side) + 4 * (sum(map(len, side)) + sum(map(len, side.values())))
                      for side in levels.values())


def event_key(kind, at, row):
    """Share the source-independent overlap/dedup work across both consumers."""
    cached = row.identity if isinstance(row, PublicEvent) else None
    if cached is not None and cached[:2] == (kind, at):
        return cached[2]
    stamp = at.isoformat() if at is not None else ''
    key = hashlib.blake2b(kind.encode() + stamp.encode() + fingerprint(row), digest_size=16).digest()
    if isinstance(row, PublicEvent):
        row.identity = (kind, at, key)
    return key


def prepare(event):
    # Compute JSON fingerprints off the lifecycle/Pipeline locks. In particular,
    # continuity and merge must not each hash every price-change row again.
    from .recording import events, exchange_time
    for kind, payload in events(event):
        at = exchange_time(payload.get('timestamp'))
        if kind == 'price_change':
            price_rows(payload, at)
        else:
            fingerprint(event if isinstance(event, dict) else payload)
            if kind == 'book':
                try:
                    materialized_book(payload)
                except KeyError as exc:
                    raise ValueError('invalid public book level') from exc
                event_key(kind, at, payload)
    return event


def price_rows(payload, at):
    if isinstance(payload, PublicEvent) and payload.changes is not None:
        return payload.changes
    result = []
    prefix = b'price_change' + (at.isoformat().encode() if at is not None else b'')
    for row in payload.get('price_changes', payload.get('priceChanges', [])):
        asset = str(row.get('asset_id') or row.get('assetId') or '')
        key = hashlib.blake2b(prefix + fingerprint(row), digest_size=16).digest()
        if isinstance(row, PublicEvent):
            row.identity = ('price_change', at, key)
        result.append((row, asset, (asset, row.get('side'), row.get('price')), key))
    if isinstance(payload, PublicEvent):
        payload.changes = result
    return result


def fingerprint(event):
    if isinstance(event, PublicEvent) and event.digest is not None:
        return event.digest
    payload = (event.get('payload') or event) if isinstance(event, dict) else {}
    book_hash = payload.get('hash')
    # Exchange full-book hashes are shared across copies. Avoid serializing
    # thousands of levels again merely to compare two identical snapshots.
    identity = event
    if (isinstance(book_hash, str) and 0 < len(book_hash) <= 128
            and 'bids' in payload and 'asks' in payload):
        identity = ('book', payload.get('asset_id') or payload.get('assetId'),
                    payload.get('timestamp'), book_hash)
    digest = hashlib.blake2b(canonical(identity),
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
    def __init__(self, maximum=512, byte_limit=4 * 1048576, ttl=2):
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
        size = 256 + len(raw) * 12  # Raw + decoded Python tree, conservative charge.
        with self.lock:
            now = time.monotonic()
            self._prune(now)
            cached = self.items.get(raw)
            if cached:
                self.hits += 1
                return cached[1]
        # A large decode must not serialize every other connection's parser.
        event = prepare(public_event(loads(raw)))
        from .recording import events
        size += sum(payload.book[1] for kind, payload in events(event)
                    if kind == 'book' and isinstance(payload, PublicEvent) and payload.book is not None)
        size += sum(len(payload.changes) * 2048 for kind, payload in events(event)
                    if kind == 'price_change' and isinstance(payload, PublicEvent) and payload.changes is not None)
        with self.lock:
            cached = self.items.get(raw)
            if cached:
                self.hits += 1
                return cached[1]
            self.misses += 1
            if size <= self.byte_limit:
                self.items[raw] = (now, event, size)
                self.bytes += size
                self._prune(now)
            return event

    def metrics(self):
        with self.lock:
            return dict(entries=len(self.items), charged_bytes=self.bytes, hits=self.hits, misses=self.misses)

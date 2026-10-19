"""Short, bounded sharing of immutable public frames across redundant readers."""
import hashlib
import json
import re
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


_ENVELOPE_FIELD = re.compile(r'"(event_type|asset_id|timestamp|hash)"\s*:\s*"?([^"\\,}\s]+)"?')
# Strings are opaque so a brace inside a size cannot split an object. The
# matcher stays in C; the Python loop only sees braces and whole strings.
_JSON_ATOM = re.compile(r'"(?:\\.|[^"\\])*"|[{}]')


def _json_objects(raw):
    """Top-level JSON objects. Nested bid rows stay inside their parent."""
    depth = 0
    start = None
    for match in _JSON_ATOM.finditer(raw):
        token = match.group(0)
        if token[0] == '"':
            continue
        if token == '{':
            if depth == 0:
                start = match.start()
            depth += 1
            continue
        if depth:
            depth -= 1
        if depth == 0 and start is not None:
            yield raw[start:match.end()]
            start = None


def _tips_for_object(blob):
    """One event. Field order inside the object does not matter.

    A live price_change puts `timestamp` and often `event_type` after every
    `price_changes` row. Flushing on the next asset_id, or on event_type
    before the kind is known, drops the first outcome or the whole frame.
    """
    kind = stamp = None
    pending_hash = None
    rows = []
    asset = digest = None

    def close():
        nonlocal asset, digest, pending_hash
        if asset:
            rows.append((asset, digest if digest is not None else pending_hash))
        asset = digest = None

    for match in _ENVELOPE_FIELD.finditer(blob):
        key, value = match.group(1), match.group(2)
        if key == 'event_type':
            if kind is None:
                kind = value
        elif key == 'asset_id':
            close()
            asset = value
        elif key == 'timestamp' and stamp is None:
            stamp = value
        elif key == 'hash':
            if asset and digest is None:
                digest = value
            elif asset is None and pending_hash is None:
                pending_hash = value
    close()
    if not kind:
        return []
    return [(kind, row_asset, stamp, row_digest) for row_asset, row_digest in rows]


def _cheap_envelope_regex(raw):
    """Reference implementation. Also the fallback for frames the fast path cannot prove."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode('utf-8', 'replace')
    if not isinstance(raw, str) or not raw:
        return []
    tips = []
    for blob in _json_objects(raw):
        tips.extend(_tips_for_object(blob))
    return tips


_ENVELOPE_KEYS = ('event_type', 'asset_id', 'timestamp', 'hash')
_ENVELOPE_QUOTED = tuple('"%s"' % key for key in _ENVELOPE_KEYS)
_ENVELOPE_QUOTED_BYTES = tuple(key.encode() for key in _ENVELOPE_QUOTED)
# The regex captures `"?([^"\\,}\s]+)"?`. A string made only of these
# characters is captured whole; anything else falls back to the regex.
_PLAIN_VALUE = re.compile(r'[^"\\,}\s]+\Z')
# Ladder rows carry price/size only. Their keys are counted, not walked;
# the raw-count check below sends any surprise back to the regex.
_LADDER_KEYS = frozenset(('bids', 'asks', 'buys', 'sells'))


class _Fallback(Exception):
    pass


def _envelope_value(value):
    if value is None:
        return 'null'
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if type(value) is str and _PLAIN_VALUE.match(value):
        return value
    # Numbers (-0, 1e5), empty or spaced strings and containers: the regex
    # result depends on the source text, so only the regex can answer.
    raise _Fallback


def _envelope_walk(node, out, seen):
    """(key, value) pairs in document order; seen counts matched keys."""
    if type(node) is dict:
        for key, value in node.items():
            if key in _ENVELOPE_SET:
                seen[0] += 1
                out.append((key, _envelope_value(value)))
            elif key in _LADDER_KEYS and type(value) is list:
                continue
            elif type(value) is dict or type(value) is list:
                _envelope_walk(value, out, seen)
    elif type(node) is list:
        for item in node:
            if type(item) is dict or type(item) is list:
                _envelope_walk(item, out, seen)


_ENVELOPE_SET = frozenset(_ENVELOPE_KEYS)


def _tips_for_pairs(pairs):
    """Same state machine as _tips_for_object, fed by parsed pairs."""
    kind = stamp = None
    pending_hash = None
    rows = []
    asset = digest = None
    for key, value in pairs:
        if key == 'event_type':
            if kind is None:
                kind = value
        elif key == 'asset_id':
            if asset:
                rows.append((asset, digest if digest is not None else pending_hash))
            asset, digest = value, None
        elif key == 'timestamp':
            if stamp is None:
                stamp = value
        elif asset and digest is None:
            digest = value
        elif asset is None and pending_hash is None:
            pending_hash = value
    if asset:
        rows.append((asset, digest if digest is not None else pending_hash))
    if not kind:
        return []
    return [(kind, row_asset, stamp, row_digest) for row_asset, row_digest in rows]


def _top_objects(node, out):
    if type(node) is dict:
        out.append(node)
    elif type(node) is list:
        for item in node:
            _top_objects(item, out)


def cheap_envelope(raw):
    """Timestamp, asset and hash only. Does not build bid or ask ladders.

    Each top-level object is one event. The parent timestamp applies to every
    asset in that object, including assets that appear before the timestamp
    and when `event_type` is the last key.

    Fast path: orjson parse plus a walk over the four envelope keys in
    document order, so field order never matters. The output is identical to
    _cheap_envelope_regex. Any frame the fast path cannot prove identical
    (parse error, key text inside strings, keys inside ladder rows, numeric or
    spaced values) is handled by the regex reference instead.
    """
    if not raw or not isinstance(raw, (str, bytes, bytearray)):
        return []
    # An escape changes the text the regex sees (it stops at a backslash),
    # so any escaped frame goes to the reference implementation.
    if orjson is None or (b'\\' if not isinstance(raw, str) else '\\') in raw:
        return _cheap_envelope_regex(raw)
    try:
        tree = orjson.loads(raw)
        objects = []
        _top_objects(tree, objects)
        tips = []
        seen = [0]
        for item in objects:
            pairs = []
            _envelope_walk(item, pairs, seen)
            tips.extend(_tips_for_pairs(pairs))
        quoted = _ENVELOPE_QUOTED if isinstance(raw, str) else _ENVELOPE_QUOTED_BYTES
        if seen[0] != sum(raw.count(key) for key in quoted):
            raise _Fallback
        return tips
    except (_Fallback, ValueError, TypeError):
        return _cheap_envelope_regex(raw)


def _as_exchange_time(value):
    if value is None or hasattr(value, 'timestamp'):
        return value
    from .recording import exchange_time
    return exchange_time(value)


def snapshot_keeps_change(change_stamp, change_hash, book_stamp, book_hash):
    """Whether a price change still has to be applied after a snapshot.

    Older than the snapshot is already in the book. The same timestamp and
    hash is the snapshot itself. A missing change timestamp cannot be ordered,
    so the snapshot wins.
    """
    book_at = _as_exchange_time(book_stamp)
    change_at = _as_exchange_time(change_stamp)
    if book_at is None:
        return True
    if change_at is None:
        return False
    if change_at < book_at:
        return False
    if change_at == book_at and change_hash and book_hash and change_hash == book_hash:
        return False
    return True


def hold_until_snapshot(event, pending):
    """Keep books. Hold price changes that still touch an asset in `pending`.

    A book removes its asset from `pending`. `pending` is mutated.
    """
    if not pending or not event:
        return event
    items = event if isinstance(event, list) else [event]
    kept = []
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = item.get('event_type') or item.get('type')
        payload = item.get('payload') or item
        if kind == 'book':
            asset = str(payload.get('asset_id') or payload.get('assetId') or '')
            pending.discard(asset)
            kept.append(item)
            continue
        if kind == 'price_change':
            field = 'price_changes' if 'price_changes' in payload else 'priceChanges' if 'priceChanges' in payload else None
            rows = payload.get(field) or [] if field else []
            assets = {str(row.get('asset_id') or row.get('assetId') or '') for row in rows if isinstance(row, dict)}
            if assets & pending:
                continue
        kept.append(item)
    if not kept:
        return None
    if isinstance(event, list):
        return kept
    return kept[0]

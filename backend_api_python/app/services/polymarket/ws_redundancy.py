"""Bounded public-feed fan-in. Caller serializes with the Pipeline lock.

There is no exchange sequence number. Match copies by canonical event content
and timestamp, retaining same-source multiplicity for identical trade prints.
Older snapshots/levels cannot overwrite newer state. Trades may arrive late.
"""
import hashlib
import json
import time
from collections import OrderedDict

from . import recording


class FeedMerge:
    def __init__(self, metadata, deduplicate=True):
        self.deduplicate = deduplicate
        self.latest = {a: row[0] for a, row in metadata.items() if row[0] is not None}
        self.snapshots = dict(self.latest)
        self.levels = OrderedDict()
        self.seen = OrderedDict()
        self.duplicates = self.stale = self.evictions = 0

    def unique(self, event, source):
        if not self.deduplicate:
            return True
        now = time.monotonic()
        while self.seen and (len(self.seen) >= 65536 or next(iter(self.seen.values()))[0] < now - 60):
            self.seen.popitem(last=False)
            self.evictions += 1
        key = hashlib.blake2b(json.dumps(event, sort_keys=True, separators=(',', ':')).encode(),
                              digest_size=16).digest()
        _, counts = self.seen.get(key, (now, {}))
        previous = max(counts.values(), default=0)
        counts[source] = counts.get(source, 0) + 1
        self.seen[key] = (now, counts)
        self.seen.move_to_end(key)
        if counts[source] <= previous:
            self.duplicates += 1
            return False
        return True

    def filter(self, event, source, owned, backup=False):
        if isinstance(event, list):
            return [e for item in event if (e := self.filter(item, source, owned, backup))]
        event = recording.owned_events(event, owned)
        if not isinstance(event, dict) or not event:
            return {}
        kind = event.get('event_type') or event.get('type')
        payload = event.get('payload') or event
        at = recording.exchange_time(payload.get('timestamp'))
        if kind == 'price_change':
            field = 'price_changes' if 'price_changes' in payload else 'priceChanges'
            rows = []
            for row in payload.get(field, []):
                asset = str(row.get('asset_id') or row.get('assetId') or '')
                key = (asset, row.get('side'), row.get('price'))
                floor = max((t for t in (self.snapshots.get(asset), self.levels.get(key)) if t is not None),
                            default=None)
                if (backup and at is None) or (at is not None and floor is not None and at < floor):
                    self.stale += 1
                    continue
                unit = dict(event_type=kind, timestamp=payload.get('timestamp'), row=row)
                if not self.unique(unit, source):
                    continue
                if at is not None:
                    self.levels[key] = at
                    self.levels.move_to_end(key)
                    self.latest[asset] = max(at, self.latest.get(asset, at))
                rows.append(row)
            while len(self.levels) > 65536:
                self.levels.popitem(last=False)
                self.evictions += 1
            if not rows:
                return {}
            filtered = {**payload, field: rows}
            return {**event, 'payload': filtered} if 'payload' in event else filtered
        if kind == 'book':
            asset = str(payload.get('asset_id') or payload.get('assetId') or '')
            floor = self.latest.get(asset)
            if (backup and at is None) or (at is not None and floor is not None and at < floor):
                self.stale += 1
                return {}
            if not self.unique(event, source):
                return {}
            if at is not None:
                self.snapshots[asset] = self.latest[asset] = at
                for key in list(self.levels):
                    if key[0] == asset:
                        del self.levels[key]
            return event
        return event if self.unique(event, source) else {}

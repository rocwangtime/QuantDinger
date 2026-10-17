"""Bounded public-feed fan-in. Caller serializes with the Pipeline lock.

There is no exchange sequence number. Match copies by canonical event content
and timestamp, retaining same-source multiplicity for identical trade prints.
Older snapshots/levels cannot overwrite newer state. Trades may arrive late.
"""
import hashlib
import json
import time
import threading
from functools import wraps
from datetime import datetime, timezone
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


def serialized(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return wrapped


class FeedContinuity:
    """Per-asset overlap evidence, independent of book invalidation.

    The public channel has timestamps/hashes but no sequence or predecessor
    hash. A missing overlap is uncertainty, never proof of lost trades. New
    full snapshots restore book state, not historical trade prints.
    Caller serializes mutation with the Pipeline/lifecycle locks.
    """
    def __init__(self):
        self.lock = threading.RLock()
        self.history = {}
        self.latest = {}
        self.pending = {}
        self.records = OrderedDict()
        self.serial = self.checks = self.uncertain = self.recovered = 0

    @serialized
    def prune(self, assets):
        for cache in (self.history, self.latest, self.pending):
            for asset in cache.keys() - assets:
                del cache[asset]

    @serialized
    def log(self, row):
        import logging
        self.serial += 1
        self.records[self.serial] = row
        while len(self.records) > 128:
            self.records.popitem(last=False)
        logging.getLogger(__name__).info('Polymarket WS continuity %s', json.dumps(row))

    @serialized
    def receive(self, event, source, received):
        for kind, payload in recording.events(event):
            if kind not in ('book', 'price_change'):
                continue
            at = recording.exchange_time(payload.get('timestamp'))
            if at is None:
                continue
            rows = (payload.get('price_changes', payload.get('priceChanges', []))
                    if kind == 'price_change' else [payload])
            for row in rows:
                asset = str(row.get('asset_id') or row.get('assetId') or '')
                if not asset:
                    continue
                key = hashlib.blake2b(json.dumps([kind, at.isoformat(), row], sort_keys=True,
                                                separators=(',', ':')).encode(), digest_size=16).hexdigest()
                history = self.history.setdefault(asset, OrderedDict())
                sources = history.setdefault(key, set())
                sources.add(source)
                while len(sources) > 4:
                    sources.remove(next(s for s in sources if s != source))
                history.move_to_end(key)
                while len(history) > 512:
                    history.popitem(last=False)
                latest = self.latest.setdefault(asset, {})
                if source not in latest or at >= latest[source][0]:
                    latest[source] = (at, key, received)
                # Only two live feeds plus a few old generations are useful.
                while len(latest) > 4:
                    del latest[next(iter(latest))]
                pending = self.pending.get(asset)
                if (pending and source != pending['old'] and kind == 'book'
                        and (pending['at'] is None or at >= pending['at'])):
                    self.recovered += 1
                    self.log(dict(asset=asset, reason=pending['reason'], status='snapshot_restored',
                                  gap_ms=max(0, (at - pending['at']).total_seconds() * 1000)
                                      if pending['at'] is not None else None,
                                  receipt_gap_ms=max(0, (received - pending['received']).total_seconds() * 1000),
                                  exchange_at=at.isoformat(), hash=row.get('hash'),
                                  historical_trades_recovered=False))
                    del self.pending[asset]

    @serialized
    def switch(self, old, new, assets, reason):
        uncertain = set()
        for asset in assets:
            self.checks += 1
            latest = self.latest.get(asset, {})
            before, after = latest.get(old), latest.get(new)
            overlap_sources = self.history.get(asset, {}).get(before[1], set()) if before else set()
            overlap = bool(before and after and after[0] >= before[0]
                           and old in overlap_sources and new in overlap_sources)
            if overlap:
                self.log(dict(asset=asset, reason=reason, status='overlap', gap_ms=0,
                              exchange_at=after[0].isoformat(), fingerprint=after[1]))
            else:
                uncertain.add(asset)
                self.uncertain += 1
                previous = self.pending.get(asset)
                self.pending[asset] = previous or dict(old=old, at=before[0] if before else None,
                    received=before[2] if before else datetime.now(timezone.utc), reason=reason)
                self.log(dict(asset=asset, reason=reason, status='unverified', gap_ms=None,
                              receipt_gap_ms=max(0, (datetime.now(timezone.utc) - before[2]).total_seconds() * 1000)
                                  if before else None,
                              exchange_at=before[0].isoformat() if before else None,
                              fingerprint=before[1] if before else None))
        return uncertain

    @serialized
    def pending_assets(self):
        return set(self.pending)

    @serialized
    def metrics(self):
        return dict(checks=self.checks, unverified=self.uncertain, snapshot_restored=self.recovered,
                    pending_assets=sorted(self.pending), events=list(self.records.values()),
                    sequence_available=False)

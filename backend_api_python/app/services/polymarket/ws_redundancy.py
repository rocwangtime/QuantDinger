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
from .ws_payloads import fingerprint


class HotBackupCoverage:
    """Session cumulative coverage; a split series needs every shard ready.

    Account at transitions, including close callbacks, rather than counting
    status samples. Unsubscribed periods do not enter the denominator.
    """
    def __init__(self, groups):
        self.lock = threading.RLock()
        self.rows = {key: dict(active=False, ready=False, observed_seconds=0., ready_seconds=0.,
                              time_without_backup_seconds=0., without_since=None,
                              zero_ready_backup_seconds=0., zero_ready=False, zero_active=False,
                              zero_ready_by_phase_seconds=dict(before=0., after=0., steady=0.)) for key in groups}
        self.required, self.ready_shards = {}, set()
        self.at = time.monotonic()

    def update(self, required, ready_shards, now, backup_counts=None, wall_now=None, zero_required=None):
        with self.lock:
            now = max(now, self.at)
            elapsed = now - self.at
            wall_now = time.time() if wall_now is None else wall_now
            zero_required = required if zero_required is None else zero_required
            for key, row in self.rows.items():
                if row['active']:
                    row['observed_seconds'] += elapsed
                    row['ready_seconds' if row['ready'] else 'time_without_backup_seconds'] += elapsed
                if row['zero_active'] and row['zero_ready']:
                    row['zero_ready_backup_seconds'] += elapsed
                    # Integrate across actual window boundaries, even when
                    # there was no status sample at that instant.
                    period = 900 if key.endswith(':15m') else 300
                    start = wall_now - elapsed
                    while start < wall_now:
                        phase = start % period
                        label, edge = (('after', 45) if phase < 45 else
                                       ('steady', period - 15) if phase < period - 15 else ('before', period))
                        stop = min(wall_now, start + edge - phase)
                        row['zero_ready_by_phase_seconds'][label] += stop - start
                        start = stop
                active = bool(required.get(key))
                ready = active and required[key] <= ready_shards
                count = (min((backup_counts.get(i, 0) for i in zero_required.get(key, set())), default=0)
                         if backup_counts is not None else int(ready))
                if active and not ready:
                    if not row['active'] or row['ready']:
                        row['without_since'] = now
                else:
                    row['without_since'] = None
                row.update(active=active, ready=ready, zero_active=bool(zero_required.get(key)), zero_ready=count == 0)
            self.required = {k: set(v) for k, v in required.items()}
            self.ready_shards = set(ready_shards)
            self.at = now
            return {key: dict(active=row['active'], ready=row['ready'],
                        required_shards=sorted(required.get(key, set())),
                        ready_shards=sorted(required.get(key, set()) & ready_shards),
                        observed_seconds=row['observed_seconds'], ready_seconds=row['ready_seconds'],
                        hot_backup_ready_ratio=(row['ready_seconds'] / row['observed_seconds']
                                                if row['observed_seconds'] else None),
                        time_without_backup_seconds=row['time_without_backup_seconds'],
                        zero_ready_backup_seconds=row['zero_ready_backup_seconds'],
                        zero_ready_backup_active=row['zero_active'],
                        zero_ready_by_phase_seconds=dict(row['zero_ready_by_phase_seconds']),
                        current_time_without_backup_seconds=(now - row['without_since']
                                                            if row['without_since'] is not None else 0.))
                    for key, row in self.rows.items()}

    def unready(self, index, now):
        with self.lock:
            self.update(self.required, self.ready_shards - {index}, now)


class FeedMerge:
    def __init__(self, metadata, deduplicate=True):
        self.deduplicate = deduplicate
        self.latest = {a: row[0] for a, row in metadata.items() if row[0] is not None}
        self.snapshots = dict(self.latest)
        self.levels = OrderedDict()
        self.seen = OrderedDict()
        self.duplicates = self.stale = self.evictions = 0

    def prune(self, assets):
        self.latest = {a: at for a, at in self.latest.items() if a in assets}
        self.snapshots = {a: at for a, at in self.snapshots.items() if a in assets}
        self.levels = OrderedDict((k, at) for k, at in self.levels.items() if k[0] in assets)
        self.expire()

    def expire(self):
        now = time.monotonic()
        while self.seen and next(iter(self.seen.values()))[0] < now - 60:
            self.seen.popitem(last=False)
            self.evictions += 1

    def unique(self, event, source, key=None):
        if not self.deduplicate:
            return True
        now = time.monotonic()
        while self.seen and (len(self.seen) >= 65536 or next(iter(self.seen.values()))[0] < now - 60):
            self.seen.popitem(last=False)
            self.evictions += 1
        key = fingerprint(event) if key is None else key
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
                if self.deduplicate:
                    duplicate_key = hashlib.blake2b(b'price_change' + str(payload.get('timestamp')).encode()
                                                    + fingerprint(row), digest_size=16).digest()
                    if not self.unique(row, source, duplicate_key):
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
                key = hashlib.blake2b(kind.encode() + at.isoformat().encode() + fingerprint(row),
                                      digest_size=16).hexdigest()
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
    def switch(self, old, new, assets, reason, book_preserved=False):
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
                now = datetime.now(timezone.utc)
                caught_up = bool(before and after and after[0] >= before[0])
                self.log(dict(asset=asset, reason=reason, status='unverified', gap_ms=None,
                              receipt_gap_ms=max(0, (datetime.now(timezone.utc) - before[2]).total_seconds() * 1000)
                                  if before else None,
                              source_silence_ms=max(0, (now - before[2]).total_seconds() * 1000) if before else None,
                              destination_silence_ms=max(0, (now - after[2]).total_seconds() * 1000) if after else None,
                              destination_exchange_gap_ms=(after[0] - before[0]).total_seconds() * 1000
                                  if before and after else None,
                              book_preserved=book_preserved and caught_up,
                              book_unavailable_ms=0 if book_preserved and caught_up else None,
                              market_silence_compatible=bool(caught_up and after[0] == before[0]),
                              historical_trades_verified=False,
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

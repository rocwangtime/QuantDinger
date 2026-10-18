"""Bounded public-feed fan-in. Caller serializes with the Pipeline lock.

There is no exchange sequence number. Match copies by canonical event content
and timestamp, retaining same-source multiplicity for identical trade prints.
Older snapshots/levels cannot overwrite newer state. Trades may arrive late.
"""
import json
import time
import threading
from functools import wraps
from datetime import datetime, timezone
from collections import OrderedDict, deque

from . import recording
from .ws_payloads import fingerprint, event_key, price_rows, PublicEvent
from .ws_balance import setting


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
                              zero_ready_by_phase_seconds=dict(before=0., after=0., steady=0.),
                              unready_reasons=set(), unready_reason_seconds={}, unready_reason_entries={}) for key in groups}
        self.required, self.ready_shards = {}, set()
        self.at = time.monotonic()

    def update(self, required, ready_shards, now, backup_counts=None, wall_now=None, zero_required=None, blockers=None):
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
                for reason in row['unready_reasons']:
                    seconds = row['unready_reason_seconds']
                    seconds[reason] = seconds.get(reason, 0.) + elapsed
                if blockers is not None:
                    reasons = set(blockers.get(key, ()))
                    for reason in reasons - row['unready_reasons']:
                        counts = row['unready_reason_entries']
                        counts[reason] = counts.get(reason, 0) + 1
                    row['unready_reasons'] = reasons
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
                        unready_reasons=sorted(row['unready_reasons']),
                        unready_reason_seconds=dict(row['unready_reason_seconds']),
                        unready_reason_entries=dict(row['unready_reason_entries']),
                        unready_reason_scope='group_without_ready_backup_overlapping_reasons',
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
        self.maximum = min(65536, int(setting('MERGE_CACHE_MAX', 4096, 1024)))
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
        while self.seen and (len(self.seen) > self.maximum or next(iter(self.seen.values()))[0] < now - 15):
            self.seen.popitem(last=False)
            self.evictions += 1

    def unique(self, event, source, key=None):
        if not self.deduplicate:
            return True
        now = time.monotonic()
        key = fingerprint(event) if key is None else key
        while self.seen and (len(self.seen) >= self.maximum and key not in self.seen or next(iter(self.seen.values()))[0] < now - 15):
            self.seen.popitem(last=False)
            self.evictions += 1
        _, counts = self.seen.get(key, (now, {}))
        previous = max(counts.values(), default=0)
        counts[source] = counts.get(source, 0) + 1
        while len(counts) > 4:
            del counts[next(s for s in counts if s != source)]
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
            hint = payload.fanin if isinstance(payload, PublicEvent) else None
            now = time.monotonic()
            if (self.deduplicate and hint and hint[0] == id(self) and source != hint[1]
                    and all(source not in counts and len(counts) < 4 and self.seen.get(key, (0, None))[1] is counts
                            and self.seen[key][0] >= now - 15 for key, counts in hint[2])):
                # First exact copy on another source. Advance the SAME row
                # counters, preserving split-frame and repeated-source semantics.
                for key, counts in hint[2]:
                    counts[source] = counts.get(source, 0) + 1
                    self.seen[key] = (now, counts)
                    self.seen.move_to_end(key)
                self.duplicates += len(hint[2])
                return {}
            rows = []
            counters = []
            for row, asset, key, duplicate_key in price_rows(payload, at):
                floor = self.snapshots.get(asset)
                level_at = self.levels.get(key)
                if level_at is not None and (floor is None or level_at > floor):
                    floor = level_at
                if (backup and at is None) or (at is not None and floor is not None and at < floor):
                    self.stale += 1
                    continue
                if self.deduplicate:
                    if not self.unique(row, source, duplicate_key):
                        continue
                    counters.append((duplicate_key, self.seen[duplicate_key][1]))
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
            if isinstance(payload, PublicEvent) and self.deduplicate and len(rows) == len(payload.get(field, [])):
                payload.fanin = (id(self), source, counters)
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
        self.history_order = OrderedDict()
        self.history_limit = 4096
        self.history_ttl = 30
        self.history_evictions = 0
        self.pending = {}
        self.records = OrderedDict()
        self.published = {}
        self.gap_waiting = {}
        self.merged_gaps = deque(maxlen=4096)
        self.serial = self.checks = self.uncertain = self.recovered = 0

    @serialized
    def prune(self, assets):
        for cache in (self.history, self.latest, self.pending, self.published, self.gap_waiting):
            for asset in cache.keys() - assets:
                del cache[asset]
        for key in list(self.history_order):
            if key[0] not in assets:
                del self.history_order[key]

    @serialized
    def expire(self, now=None):
        now = time.monotonic() if now is None else now
        while self.history_order and (len(self.history_order) > self.history_limit
                or next(iter(self.history_order.values())) < now - self.history_ttl):
            (asset, key), _ = self.history_order.popitem(last=False)
            history = self.history.get(asset, {})
            history.pop(key, None)
            if not history:
                self.history.pop(asset, None)
            self.history_evictions += 1

    @serialized
    def log(self, row):
        import logging
        self.serial += 1
        row.setdefault('merged_gap_ms', self.published.get(row.get('asset'), {}).get('receipt_gap_ms'))
        row['event_id'] = self.serial
        row['merged_gap_final'] = False
        self.gap_waiting.setdefault(row.get('asset'), {})[self.serial] = row
        row['physical_evidence_alert_only'] = True
        self.records[self.serial] = row
        while len(self.records) > 128:
            serial, removed = self.records.popitem(last=False)
            waiting = self.gap_waiting.get(removed.get('asset'), {})
            waiting.pop(serial, None)
        logging.getLogger(__name__).info('Polymarket WS continuity %s', json.dumps(row))

    @serialized
    def publication(self, metadata, assets):
        """Actual merged book receipt cadence, independent of physical sources."""
        for asset in assets:
            row = metadata.get(asset)
            if not row or row[0] is None:
                continue
            previous = self.published.get(asset)
            if previous and row[1] <= previous['received']:
                continue
            gap = max(0, (row[1] - previous['received']).total_seconds() * 1000) if previous else 0
            self.published[asset] = dict(received=row[1], exchange_at=row[0], receipt_gap_ms=gap,
                max_receipt_gap_ms=max(gap, previous['max_receipt_gap_ms'] if previous else 0))
            if previous:
                self.merged_gaps.append(gap)
            for event in self.gap_waiting.pop(asset, {}).values():
                event.update(merged_gap_ms=gap if previous else None, merged_gap_final=bool(previous))
                import logging
                logging.getLogger(__name__).info('Polymarket WS business gap %s', json.dumps(event))

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
            # Only the final row per asset/frame is needed for overlap evidence.
            # Book merging still processes every row; absent overlap is uncertain.
            final = {str(row.get('asset_id') or row.get('assetId') or ''): row for row in rows}
            for asset, row in final.items():
                if not asset:
                    continue
                key = event_key(kind, at, row).hex()
                history = self.history.setdefault(asset, OrderedDict())
                sources = history.setdefault(key, set())
                sources.add(source)
                while len(sources) > 4:
                    sources.remove(next(s for s in sources if s != source))
                history.move_to_end(key)
                self.history_order[asset, key] = time.monotonic()
                self.history_order.move_to_end((asset, key))
                while len(history) > 512:
                    removed, _ = history.popitem(last=False)
                    self.history_order.pop((asset, removed), None)
                    self.history_evictions += 1
                self.expire()
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
                              merged_receipt_age_ms=max(0, (now - self.published[asset]['received']).total_seconds() * 1000)
                                  if asset in self.published else None,
                              merged_max_receipt_gap_ms=self.published.get(asset, {}).get('max_receipt_gap_ms'),
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
        gaps = sorted(self.merged_gaps)
        return dict(checks=self.checks, unverified=self.uncertain, snapshot_restored=self.recovered,
                    merged_gap_ms=dict(samples=len(gaps), p50=gaps[len(gaps)//2] if gaps else None,
                        p95=gaps[min(len(gaps)-1, int(len(gaps)*.95))] if gaps else None,
                        max=max(gaps) if gaps else None),
                    pending_assets=sorted(self.pending), events=list(self.records.values()),
                    history_assets=len(self.history), history_entries=sum(map(len, self.history.values())),
                    history_limit=self.history_limit, history_ttl_seconds=self.history_ttl,
                    history_evictions=self.history_evictions,
                    published={a: dict(receipt_gap_ms=r['receipt_gap_ms'], max_receipt_gap_ms=r['max_receipt_gap_ms'],
                        receipt_age_ms=max(0, (datetime.now(timezone.utc) - r['received']).total_seconds() * 1000))
                        for a, r in self.published.items()},
                    sequence_available=False)


class DoubleKicks:
    """Pair physical generations, including a backup promoted before its close."""
    def __init__(self):
        self.recent = {}
        self.events = deque(maxlen=256)
        self.total = 0
        self.hourly = OrderedDict()

    def record(self, group, source, role, code, now, wall):
        recent = self.recent.setdefault(group, deque(maxlen=8))
        if any(row['source'] == source for row in recent):
            return
        peer = next((r for r in reversed(recent) if 0 <= now-r['monotonic'] <= 1 and not r['paired']), None)
        row = dict(source=source, role=role, code=code, monotonic=now, paired=peer is not None)
        recent.append(row)
        if peer is None:
            return
        peer['paired'] = True
        hour = int(wall // 3600) * 3600
        self.hourly[hour] = self.hourly.get(hour, 0) + 1
        while len(self.hourly) > 48:
            self.hourly.popitem(last=False)
        self.total += 1
        event = dict(group=group, at=wall, sources=[peer['source'], source],
                     roles=[peer['role'], role], codes=[peer['code'], code],
                     separation_ms=(now-peer['monotonic'])*1000, merged_gap_ms=None,
                     merged_gap_final=False, business_gap=False)
        self.events.append(event)
        return event

    def metrics(self, wall):
        hour = int(wall // 3600) * 3600
        events = [e for e in self.events if e['at'] > wall-3600]
        return dict(total=self.total, count_current_hour=self.hourly.get(hour, 0),
                    count_rolling_hour=len(events),
                    rolling_hour_count_capped=bool(self.total > len(self.events) and self.events and self.events[0]['at'] > wall-3600),
                    business_gaps_rolling_hour=sum(e['business_gap'] for e in events),
                    business_gaps_current_hour=sum(e['business_gap'] for e in events if e['at'] >= hour),
                    pending_rolling_hour=sum(not e['merged_gap_final'] for e in events),
                    hourly=[dict(hour_start_utc=h, count=n) for h, n in self.hourly.items()],
                    events=[{k: v for k, v in e.items() if not k.startswith('_')} for e in self.events], pair_window_seconds=1)

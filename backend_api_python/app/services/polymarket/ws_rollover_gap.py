"""Measure first valid market availability even without a same-token baseline."""
from collections import deque
from datetime import datetime, timezone

from . import recording

ROLLOVER_GAP_DEFINITION = (
    'New market scheduled eventStartTime to first valid merged publication for each outcome; '
    'maximum across outcomes, including discovery/subscription delay. '
    'Separate cross-market gap: last complete old outcome pair to first complete new pair. '
    'Startup is labelled separately; missing boundary remains unmeasured.')


class RolloverGaps:
    def __init__(self):
        self.current = {}
        self.events = deque(maxlen=128)
        self.serial = 0

    def observe(self, markets, published, now=None):
        now = now or datetime.now(timezone.utc)
        for prior in self.current.values():
            for asset in prior['assets']:
                if asset in published:
                    prior['last_valid_publication'][asset] = published[asset]['published_at'].isoformat()
        for market in markets:
            if market.get('market_type') != 'up_down':
                continue
            group = f"up_down:{market.get('asset')}:{market.get('interval')}"
            assets = [str(market.get('yesAssetId')), str(market.get('noAssetId'))]
            if 'None' in assets:
                continue
            prior = self.current.get(group)
            if prior and prior['market_id'] == str(market['id']) and prior['assets'] == assets:
                continue
            boundary = recording.exchange_time(market.get('eventStartTime'))
            old = [datetime.fromisoformat(prior['last_valid_publication'][a])
                   for a in prior['assets'] if a in prior['last_valid_publication']] if prior else []
            # Discovery can pass through an empty catalogue; keep the previous
            # group reference until its successor appears, but never its books.
            self.serial += 1
            row = dict(event_id=self.serial, group=group, market_id=str(market['id']), assets=assets,
                phase='rollover' if prior else 'startup',
                scheduled_start=boundary.isoformat() if boundary else None,
                catalogue_seen_at=now.isoformat(),
                discovery_lateness_ms=max(0, (now-boundary).total_seconds()*1000) if boundary else None,
                old_last_complete_publication_at=min(old).isoformat() if prior and len(old) == len(prior['assets']) else None,
                first_valid_publication={}, last_valid_publication={}, start_to_first_valid_ms=None, cross_market_gap_ms=None,
                complete=False, measurement='pending_publication' if boundary else 'unmeasured_missing_boundary')
            self.current[group] = row
            self.events.append(row)

    def baselines(self, assets):
        result = {}
        for row in self.current.values():
            if row['phase'] != 'rollover' or not row['scheduled_start']:
                continue
            at = datetime.fromisoformat(row['scheduled_start'])
            for asset in set(row['assets']) & set(assets):
                result[asset] = dict(received=at, published_at=at, rollover_event_id=row['event_id'])
        return result

    def publication(self, asset, at):
        for row in self.current.values():
            if asset not in row['assets']:
                continue
            row['last_valid_publication'][asset] = at.isoformat()
            if asset in row['first_valid_publication']:
                continue
            row['first_valid_publication'][asset] = at.isoformat()
            if not set(row['assets']) <= row['first_valid_publication'].keys():
                return
            first_complete = max(datetime.fromisoformat(t) for t in row['first_valid_publication'].values())
            boundary = datetime.fromisoformat(row['scheduled_start']) if row['scheduled_start'] else None
            old = datetime.fromisoformat(row['old_last_complete_publication_at']) if row['old_last_complete_publication_at'] else None
            row.update(complete=True,
                start_to_first_valid_ms=max(0, (first_complete-boundary).total_seconds()*1000) if boundary else None,
                cross_market_gap_ms=max(0, (first_complete-old).total_seconds()*1000) if old else None,
                measurement='measured' if boundary else 'unmeasured_missing_boundary')

    def metrics(self):
        return {group: dict(definition=ROLLOVER_GAP_DEFINITION,
                            events=[dict(e) for e in self.events if e['group'] == group])
                for group in self.current}

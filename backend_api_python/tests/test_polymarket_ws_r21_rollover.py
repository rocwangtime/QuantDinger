from datetime import datetime, timezone, timedelta

from app.services.polymarket.ws_rollover_gap import RolloverGaps
from app.services.polymarket.ws_redundancy import FeedContinuity
from app.services.polymarket.ws_group_process import merge_feed_status

T = datetime(2026, 10, 21, 23, 40, tzinfo=timezone.utc)


def market(identity, start=T):
    return dict(id=identity, market_type='up_down', asset='BTC', interval='5m',
                yesAssetId=identity+'y', noAssetId=identity+'n',
                eventStartTime=start.isoformat() if start else None)


def test_rollover_measures_boundary_delay_without_new_token_baseline():
    gaps = RolloverGaps()
    gaps.observe([market('old', T-timedelta(minutes=5))], {}, T-timedelta(minutes=5))
    old = {a: dict(published_at=T-timedelta(milliseconds=100)) for a in ['oldy', 'oldn']}
    gaps.observe([], old, T)
    gaps.observe([market('new')], {}, T+timedelta(milliseconds=200))
    gaps.publication('newy', T+timedelta(milliseconds=700))
    assert not gaps.current['up_down:BTC:5m']['complete']
    gaps.publication('newn', T+timedelta(milliseconds=900))
    row = gaps.current['up_down:BTC:5m']
    assert row['phase'] == 'rollover' and row['start_to_first_valid_ms'] == 900
    assert row['cross_market_gap_ms'] == 1000 and row['discovery_lateness_ms'] == 200
    gaps.publication('newn', T+timedelta(seconds=20))
    assert row['start_to_first_valid_ms'] == 900


def test_interruption_at_new_market_start_uses_explicit_named_boundary(monkeypatch):
    from app.services.polymarket import ws_redundancy
    clock = type('Clock', (), {'now': staticmethod(lambda tz: T+timedelta(seconds=1))})
    monkeypatch.setattr(ws_redundancy, 'datetime', clock)
    continuity = FeedContinuity()
    continuity.rollover.observe([market('new')], {}, T)
    event = {}
    continuity.watch_gap(event, {'newy', 'newn'})
    assert event['baseline_kind'] == 'scheduled_market_start' and not event['unmeasured_assets']
    continuity.publication({a: (T, T+timedelta(milliseconds=900), 'ws') for a in ['newy', 'newn']}, {'newy', 'newn'})
    assert event['merged_gap_final'] and event['merged_gap_ms'] == 1000
    assert event['gap_measurement'] == 'measured_rollover_start'


def test_missing_boundary_is_unknown_and_startup_is_separate():
    gaps = RolloverGaps()
    gaps.observe([market('first', None)], {}, T)
    for a in ['firsty', 'firstn']:
        gaps.publication(a, T)
    row = gaps.current['up_down:BTC:5m']
    assert row['phase'] == 'startup' and row['complete']
    assert row['start_to_first_valid_ms'] is None and not gaps.baselines({'firsty'})


def test_parent_status_keeps_both_groups_rollover_measurements():
    parts = []
    for i, asset in enumerate(['BTC', 'ETH']):
        group = f'up_down:{asset}:5m'
        parts.append(dict(key=group, shard_base=i, status={'ws_rollover_gaps': {group: dict(events=[dict(event_id=1)])}}))
    assert len(merge_feed_status(parts)['ws_rollover_gaps']) == 2

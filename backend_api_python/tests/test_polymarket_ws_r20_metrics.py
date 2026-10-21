from datetime import datetime, timedelta, timezone
from app.services.polymarket.ws_redundancy import FeedContinuity, DoubleKicks, HotBackupCoverage, cadence_metrics
from app.services.polymarket.ws_group_process import _merge_continuity, merge_feed_status


def test_interruption_gap_closes_once_from_publication_time_with_shared_definition(monkeypatch):
    import app.services.polymarket.ws_redundancy as module
    at = datetime.now(timezone.utc)
    clock = [at]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    monkeypatch.setattr(module, 'datetime', Clock)
    c = FeedContinuity()
    c.publication({'a': (at, at, None)}, {'a'})
    primary, paired = {}, dict(supply_interrupted=True)
    c.watch_gap(primary, {'a'})
    c.watch_gap(paired, {'a'})
    clock[0] += timedelta(seconds=2)
    c.publication({'a': (at+timedelta(milliseconds=10), at+timedelta(milliseconds=10), None)}, {'a'})
    assert primary['merged_gap_ms'] == paired['merged_gap_ms'] == 2000
    assert primary['merged_gap_final'] and paired['business_gap']
    clock[0] += timedelta(seconds=50)
    c.publication({'a': (clock[0], clock[0], None)}, {'a'})
    assert primary['merged_gap_ms'] == 2000
    missing = {}
    c.watch_gap(missing, {'missing'})
    assert missing['merged_gap_ms'] is None and missing['unmeasured_assets'] == ['missing']


def test_cold_natural_quiet_is_subtracted_and_histograms_pool_quantiles():
    at = datetime.now(timezone.utc)
    c = FeedContinuity()
    c.publication({'cold': (at-timedelta(seconds=300), at-timedelta(seconds=300), None)}, {'cold'})
    c.publication({'cold': (at, at, None)}, {'cold'})
    row = c.metrics()['receive_cadence_ms']['cold']
    assert row['max'] == 0 and row['samples'] == 1
    left = dict(receive_cadence_ms=dict(hot=cadence_metrics([(1,'a','t')]*100)))
    right = dict(receive_cadence_ms=dict(hot=cadence_metrics([(1000,'b','u')])))
    merged = _merge_continuity(left,right)['receive_cadence_ms']['hot']
    assert merged['p95'] == 1 and merged['max'] == 1000 and merged['maximum']['asset'] == 'b'
    c.cadence_seconds = {0:c.cadence_seconds[next(iter(c.cadence_seconds))]}
    assert c.metrics()['receive_cadence_ms']['cold']['samples'] == 0


def test_seven_second_near_double_kick_is_paired():
    d = DoubleKicks()
    d.record('g','primary','primary',1013,0,1000)
    event = d.record('g','promoted','primary',None,7,1007)
    assert event and event['separation_ms'] == 7000
    assert d.metrics(1008)['pair_window_seconds'] == 15


def test_parent_uses_owner_coverage_and_zero_ready_means_zero():
    c = HotBackupCoverage(['g'])
    row = c.update({'g': {0}}, {0}, c.at+1, backup_counts={0:1})['g']
    assert not row['zero_ready_backup_active']
    owner = dict(row,target_backup_count=2,ready_backup_count=1)
    parts = [dict(key='cold', status=dict(ws_hot_backup_by_group={'g':dict(active=False,ready=False)})),
             dict(key='g', status=dict(ws_hot_backup_by_group={'g':owner})),
             dict(key='other', status=dict(ws_hot_backup_by_group={'g':dict(active=False,ready=False)}))]
    assert merge_feed_status(parts)['ws_hot_backup_by_group']['g'] == owner


def test_multiple_reserves_from_each_child_have_distinct_global_status_keys():
    parts = [dict(shard_base=0, status=dict(ws_reserves={'0:2': {'source':'a'}})),
             dict(shard_base=2, status=dict(ws_reserves={'0:2': {'source':'b'}}))]
    assert set(merge_feed_status(parts)['ws_reserves']) == {'0:2','2:2'}


def test_late_physical_switch_references_original_interruption_even_after_publication():
    c = FeedContinuity()
    at = datetime.now(timezone.utc)
    c.publication({'a': (at,at,'ws')}, {'a'})
    primary = {}
    c.watch_gap(primary, {'a'})
    c.publication({'a':(at,at+timedelta(milliseconds=20),'ws')}, {'a'})
    measured = primary['merged_gap_ms']
    c.switch('physical-old',None,{'a'},'failover')
    # The switch happens later; it must not start a second gap from the new baseline.
    c.publication({'a':(at,at+timedelta(milliseconds=100),'ws')}, {'a'})
    physical = c.metrics()['events'][0]
    assert physical['business_event_id'] == primary['business_event_id']
    assert physical['merged_gap_ms'] == measured and physical['merged_gap_final']

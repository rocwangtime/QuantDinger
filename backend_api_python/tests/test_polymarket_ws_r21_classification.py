"""Approved business acceptance depends on publication, not predicted supply."""
from datetime import datetime, timedelta, timezone

import pytest
from app.services.polymarket import ws_redundancy as module


@pytest.mark.parametrize('ms', [0, 1499.999, 1500, 1500.001, 1554.872, 1628.046, 4582.5, 4265.7, 30657.5, 4178.8])
@pytest.mark.parametrize('supply', [None, False, True])
def test_actual_merged_gap_classifies_every_production_failure(monkeypatch, ms, supply):
    clock = [datetime(2026,10,22,tzinfo=timezone.utc)]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    monkeypatch.setattr(module, 'datetime', Clock)
    c = module.FeedContinuity()
    c.publication({'a': (clock[0],clock[0],'ws')}, {'a'})
    event = dict(ready_backups=2, physical_source_silence_alert=True)
    if supply is not None:
        event['supply_interrupted'] = supply
    c.watch_gap(event, {'a'})
    assert event['business_gap'] is None and not event['merged_gap_final']
    clock[0] += timedelta(milliseconds=ms)
    c.publication({'a': (clock[0]+timedelta(microseconds=1),clock[0]+timedelta(microseconds=1),'ws')}, {'a'})
    assert event['merged_gap_final'] and event['business_gap'] is (ms > 1500)
    assert event['merged_gap_ms'] == pytest.approx(ms)


def test_missing_asset_does_not_misreport_partial_measurement_as_pass(monkeypatch):
    c = module.FeedContinuity()
    at = datetime.now(timezone.utc)-timedelta(seconds=2)
    c.publication({'a': (at,at,'ws')},{'a'})
    event={}
    c.watch_gap(event, {'a','missing'})
    c.publication({'a': (datetime.now(timezone.utc),datetime.now(timezone.utc),'ws')},{'a'})
    assert not event['merged_gap_final'] and event['business_gap'] is None
    assert event['unmeasured_assets']==['missing']


def test_double_kick_counts_only_final_gaps_over_1500ms(monkeypatch):
    clock=[datetime.fromtimestamp(3600,timezone.utc)]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock[0]
    monkeypatch.setattr(module,'datetime',Clock)
    kicks=module.DoubleKicks()
    continuity=module.FeedContinuity()
    for group,ms in [('ok',1500),('bad',1501)]:
        continuity.publication({group:(clock[0],clock[0],'ws')},{group})
        kicks.record(group,'p','primary',1013,10,3600)
        event=kicks.record(group,'b','backup',1013,11,3601)
        event['supply_interrupted']=False
        continuity.watch_gap(event,{group})
        assert kicks.metrics(3602)['business_gaps_current_hour']==0
        clock[0]+=timedelta(milliseconds=ms)
        continuity.publication({group:(clock[0],clock[0],'ws')},{group})
    report=kicks.metrics(3605)
    assert report['business_gaps_current_hour']==1 and report['count_current_hour']==2

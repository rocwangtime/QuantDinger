from datetime import datetime, timezone, timedelta
from app.services.polymarket.ws_redundancy import FeedMerge, DoubleKicks, business_gap_counts
from app.services.polymarket.recording import Recorder


def book(at):
    return dict(event_type='book',asset_id='a',timestamp=at,hash=str(at),bids=[],asks=[])


def test_stale_counts_attribute_primary_and_each_backup():
    merge=FeedMerge({})
    merge.filter(book(2000),'main',{'a'})
    for source in ('main','backup1','backup2'):
        assert not merge.filter(book(1000),source,{'a'})
    assert merge.stale_by_source==dict(main=1,backup1=1,backup2=1)
    recorder=Recorder();recorder.enabled=False
    recorder.metadata['a']=(datetime.fromtimestamp(2,timezone.utc),datetime.now(timezone.utc),'ws')
    recorder.receive(book(1000),{'a'},{},{},datetime.now(timezone.utc),source='backup2')
    assert recorder.stale_by_source=={'backup2':1}


def test_double_kick_reports_first_interruption_max_and_pending():
    kicks=DoubleKicks()
    first=dict(at=100,merged_gap_ms=7259.6,merged_gap_final=True,business_event_id=26)
    kicks.watch_primary('BTC','main',first,100)
    kicks.record('BTC','main','primary',None,100,100)
    second=kicks.record('BTC','backup','primary',None,111,111)
    second.update(merged_gap_ms=6.967,merged_gap_final=True,business_event_id=27)
    metrics=kicks.metrics(112)
    assert metrics['events'][0]['merged_gap_ms']==7259.6
    assert metrics['business_gaps_rolling_hour']==1
    assert business_gap_counts([first,first],112)['business_gaps_rolling_hour']==1
    first['merged_gap_final']=False
    assert kicks.metrics(112)['pending_rolling_hour']==1
    assert kicks.metrics(112)['business_gaps_rolling_hour']==0
    assert kicks.metrics(4000)['business_gaps_rolling_hour']==0


def test_threshold_is_same_for_double_and_primary_failures():
    for gap,count in ((1500,0),(1500.001,1)):
        kicks=DoubleKicks();kicks.record('x','a','primary',None,1,1)
        e=kicks.record('x','b','backup',None,2,2)
        e.update(merged_gap_ms=gap,merged_gap_final=True)
        assert kicks.metrics(3)['business_gaps_rolling_hour']==count
        assert business_gap_counts([e],3)['business_gaps_rolling_hour']==count


def test_child_to_parent_preserves_source_for_metadata_rejection(monkeypatch):
    from types import SimpleNamespace
    from app.services.polymarket import observer
    from app.services.polymarket.ws_group_process import _Outbox, _Feed
    from app.services.polymarket.ws_group_process import _recv
    import multiprocessing
    pipeline=observer.Pipeline();pipeline.recording.enabled=False
    pipeline.set_markets([dict(id='m',yesAssetId='a',noAssetId='b')])
    pipeline.receive(book(2000))
    monkeypatch.setattr(observer,'_PIPELINE',pipeline)
    incoming,outgoing=multiprocessing.Pipe(duplex=False)
    outbox=_Outbox(outgoing)
    event=dict(event_type='price_change',timestamp=1000,price_changes=[dict(asset_id='a',side='BUY',price='.4',size='3')])
    outbox.submit(event,{'a'},datetime.now(timezone.utc),source='feed-10081')
    outbox.close();outbox.run()
    message=_recv(incoming)
    feed=object.__new__(_Feed)
    feed.feeds=SimpleNamespace(resync_assets=set(),published=0)
    feed.apply_s=0;feed.gap_pending={};feed.spec={'key':'BTC'}
    feed._apply(message['items'])
    incoming.close();outgoing.close()
    assert pipeline.recording.stale_by_source=={'feed-10081':1}


def test_retained_hour_counts_are_distinct_from_short_diagnostic_tail():
    from collections import deque
    events=deque([dict(at=100+i,merged_gap_ms=1600,merged_gap_final=True,business_event_id=i) for i in range(100)],maxlen=4096)
    assert business_gap_counts(events,200)['business_gaps_rolling_hour']==100
    assert business_gap_counts(events,200)['rolling_hour_count_capped'] is False
    assert business_gap_counts(events,3800)['business_gaps_rolling_hour']==0

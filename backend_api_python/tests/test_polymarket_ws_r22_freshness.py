"""Freshness decisions use advancing siblings and distinguish local pressure."""
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
import time

from app.services.polymarket import observer
from tests.test_polymarket_ws_r21_supply import setup_supply, emit


def prime(primary, spare, lag=3, rtt=2):
    now = datetime.now(timezone.utc)
    primary['feed_freshness'] = {a:(now-timedelta(seconds=lag), now) for a in primary['assets']}
    primary['socket'] = SimpleNamespace(transport=SimpleNamespace(pong_rtt_seconds=rtt))
    spare['socket'] = SimpleNamespace(transport=SimpleNamespace(pong_rtt_seconds=.19))
    return now


def test_busy_primary_with_slow_pong_uses_verified_backup(monkeypatch):
    m, primary, spare, pipeline, requests, _ = setup_supply(monkeypatch)
    now = prime(primary, spare)
    primary['inbox'].last_data_at = primary['inbox'].last_byte_at = time.monotonic()
    emit(m, spare, round(now.timestamp()*1000), '19')
    assert pipeline.books['ay']['bids']['.4'] == '19'
    assert m.states[0] is primary and not requests


def test_sustained_relative_age_retires_busy_primary_without_rest(monkeypatch):
    m, primary, spare, pipeline, requests, _ = setup_supply(monkeypatch)
    now = prime(primary, spare, rtt=.2)
    base = time.monotonic()
    for offset in (0, .4, .8, 1.2, 1.6, 2.1):
        monkeypatch.setattr(observer.time, 'monotonic', lambda: base+offset)
        primary['inbox'].last_byte_at = base+offset
        emit(m, spare, round(now.timestamp()*1000)+int(offset), '21')
    assert primary['failure_reason'] == 'upstream_lag'
    assert m.states[0] is spare and not requests and not spare.get('need_book')
    assert pipeline.books['ay']['bids']['.4'] == '21'


def test_quiet_or_stale_backup_never_retires_primary(monkeypatch):
    m, primary, spare, _, _, retired = setup_supply(monkeypatch)
    prime(primary, spare)
    # Old exchange data received just now is not fresher positive evidence.
    for n in range(30):
        emit(m, spare, 1000)
    assert m.states[0] is primary and not retired
    assert not primary.get('data_suspect')


def test_local_queue_pressure_cannot_be_upstream_lag(monkeypatch):
    m, primary, spare, _, _, retired = setup_supply(monkeypatch)
    now = prime(primary, spare)
    primary['inbox'].queue_wait_seconds = 1
    base = time.monotonic()
    for n in range(12):
        monkeypatch.setattr(observer.time, 'monotonic', lambda: base+n*.3)
        emit(m, spare, round(now.timestamp()*1000)+n)
    assert primary['freshness_classification'] == 'ingress_pressure'
    assert not primary.get('data_suspect') and not retired


def test_unverified_warm_book_cannot_retire_busy_lagging_primary(monkeypatch):
    m, primary, spare, _, requests, retired = setup_supply(monkeypatch)
    now = prime(primary, spare)
    primary['book_checkpoints'].prune(set())
    base = time.monotonic()
    for offset in (0, .4, .8, 1.2, 1.6, 2.1, 2.4):
        monkeypatch.setattr(observer.time, 'monotonic', lambda: base+offset)
        emit(m, spare, round(now.timestamp()*1000)+int(offset))
    assert m.states[0] is primary and not retired and not requests
    assert not primary.get('data_suspect')


def test_fanin_stops_after_primary_recovers_instead_of_staying_on(monkeypatch):
    from app.services.polymarket.ws_payloads import feed_envelope
    m, primary, spare, pipeline, _, _ = setup_supply(monkeypatch)
    for row in m.continuity.published.values():
        row['published_at']-=timedelta(seconds=.3)
    emit(m,spare,1100,'3')
    assert pipeline.books['ay']['bids']['.4']=='3'
    event=dict(event_type='price_change',timestamp=1200,price_changes=[dict(asset_id=a,side='BUY',price='.4',size='4',hash='1200'+a) for a in primary['assets']])
    pipeline.receive(event)
    m.continuity.publication(pipeline.recording.metadata,set(primary['assets']))
    m._feed_freshness(primary,feed_envelope(event),datetime.now(timezone.utc))
    later=time.monotonic()+1
    monkeypatch.setattr(observer.time,'monotonic',lambda:later)
    emit(m,spare,1300,'5')
    assert pipeline.books['ay']['bids']['.4']=='4'
    assert m.states[0] is primary


def test_close_during_raw_processing_keeps_verified_handoff_without_rest(monkeypatch):
    import json
    import threading
    from app.services.polymarket.ws_group_process import ReplaySocket
    from app.services.polymarket.ws_redundancy import FeedMerge
    from app.services.polymarket.ws_warm_book import WarmBook
    monkeypatch.setitem(__import__('sys').modules,'websocket',SimpleNamespace(WebSocketApp=ReplaySocket))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_REPLAY','1')
    monkeypatch.setattr(observer,'_STOP',threading.Event())
    monkeypatch.setattr(observer,'STATUS',dict(reconnect_count=0,slow_consumer_count=0))
    pipeline=observer.Pipeline();pipeline.recording.enabled=False
    pipeline.set_markets([dict(id='a',yesAssetId='ay',noAssetId='an')])
    monkeypatch.setattr(observer,'_PIPELINE',pipeline)
    manager=observer.MarketConnections();manager.count=manager.balancer.count=1;manager.liveness_backup=True
    manager.balancer.assignment={a:0 for a in ['ay','an']}
    manager.balancer.groups={a:'up_down:BTC:5m' for a in ['ay','an']}
    manager.merges[0]=FeedMerge({});manager.redundancy_tick=lambda now:None
    primary=dict(assets=['ay','an'],socket=None,thread=None,connected=False,lock=threading.RLock(),attempts=0,retry_at=0,ping_at=0)
    manager.states[0]=primary
    rest=[];manager.snapshot_recovery.request=lambda *a,**kw:rest.append(a) or False
    entered=threading.Event();release=threading.Event()
    def wait(predicate):
        deadline=time.monotonic()+3
        while not predicate():
            assert time.monotonic()<deadline,'race handoff did not complete'
            time.sleep(.001)
    try:
        manager.connect(0,primary)
        for i in range(2):
            spare=dict(assets=['ay','an'],required_assets={'ay','an'},primary_index=0,socket=None,thread=None,connected=False,
                lock=threading.RLock(),attempts=0,retry_at=0,ping_at=0,standby=True,redundant=True,busy=True,
                shadow=WarmBook(['ay','an']),created_at=time.monotonic())
            (manager.backups if i==0 else manager.reserves)[0]=spare;manager.connect(0,spare)
        states=[primary,*[s for _,s in manager.backup_items()]]
        wait(lambda:all(s.get('connected') and not s.get('subscribe_initial',True) for s in states))
        stamp=round(time.time()*1000)
        initial=[dict(event_type='book',asset_id=a,timestamp=stamp,hash='initial'+a,bids=[dict(price='.4',size='2')],asks=[]) for a in ['ay','an']]
        for s in states:s['socket'].emit(json.dumps(initial))
        wait(lambda:len(pipeline.books)==2 and all(manager.backup_ready(s) for _,s in manager.backup_items()))
        wait(lambda:all(s['inbox'].queue.unfinished_tasks==0 for s in states))
        remember=manager._remember_books
        def paused(state,event,*args,**kwargs):
            remember(state,event,*args,**kwargs)
            if state is primary and threading.current_thread() is primary.get('worker'):
                entered.set();assert release.wait(3)
        monkeypatch.setattr(manager,'_remember_books',paused)
        delta=dict(event_type='price_change',timestamp=stamp+20,price_changes=[dict(asset_id=a,side='BUY',price='.4',size='3',hash='next'+a) for a in ['ay','an']])
        primary['socket'].emit(json.dumps(delta));assert entered.wait(3)
        for _,s in manager.backup_items():s['socket'].emit(json.dumps(delta))
        wait(lambda:all(s['inbox'].queue.unfinished_tasks==0 for _,s in manager.backup_items()))
        primary['socket'].callbacks['on_error'](primary['socket'],RuntimeError('Connection to remote host was lost.'))
        primary['socket'].kick(None,None);release.set()
        wait(lambda:manager.states[0] is not primary and all(pipeline.books[a]['bids']['.4']=='3' for a in ['ay','an']))
        assert not rest and not manager.states[0].get('need_book')
        assert manager.primary_failures['up_down:BTC:5m']['events'][-1]['merged_gap_ms']<1500
    finally:
        release.set();manager.close(2)

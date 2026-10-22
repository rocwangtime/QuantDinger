"""Offline repeated physical generations; separates Python retention from RSS.

Fake public sockets exercise real readers/workers/retirement and warm fan-in.
This cannot exclude production TLS/native allocator retention.
"""
import gc
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
import tracemalloc
from types import SimpleNamespace
from unittest.mock import patch
import weakref
from datetime import datetime, timezone

ROOT=Path('/workspace/quantdinger/wt_r21')
sys.path.insert(0,str(ROOT/'backend_api_python'))
from app.services.polymarket import observer
from app.services.polymarket.ws_group_process import ReplaySocket
from app.services.polymarket.ws_redundancy import FeedMerge
from app.services.polymarket.ws_warm_book import WarmBook


def wait_for(f):
    until=time.monotonic()+3
    while not f():
        if time.monotonic()>until:raise TimeoutError('memory fixture did not settle')
        time.sleep(.001)


def generation(index, frames=500):
    pipeline=observer.Pipeline();pipeline.recording.enabled=False
    assets=[f'{index}y',f'{index}n']
    pipeline.set_markets([dict(id=str(index),yesAssetId=assets[0],noAssetId=assets[1])])
    with patch.object(observer,'_PIPELINE',pipeline):
        manager=observer.MarketConnections();manager.liveness_backup=True
        manager.balancer.assignment={a:0 for a in assets}
        manager.balancer.groups={a:'up_down:BTC:5m' for a in assets}
        manager.merges[0]=FeedMerge({});manager.redundancy_tick=lambda now:None
        primary=dict(assets=assets,socket=None,thread=None,connected=False,lock=threading.RLock(),attempts=0,retry_at=0,ping_at=0)
        manager.states[0]=primary;manager.connect(0,primary)
        spare=dict(primary,assets=list(assets),required_assets=set(assets),primary_index=0,
            standby=True,redundant=True,shadow=WarmBook(assets),created_at=time.monotonic(),socket=None,thread=None)
        manager.backups[0]=spare;manager.connect(0,spare)
        try:
            wait_for(lambda:all(s.get('connected') and not s.get('subscribe_initial',True) for s in [primary,spare]))
            epoch=round(time.time()*1000)
            book=[dict(event_type='book',asset_id=a,timestamp=epoch,hash='initial'+a,
                bids=[dict(price='.4',size='1')],asks=[dict(price='.6',size='1')]) for a in assets]
            for state in [primary,spare]:state['socket'].emit(json.dumps(book))
            wait_for(lambda:manager.backup_ready(spare) and len(pipeline.books)==2)
            primary['inbox'].last_data_at=time.monotonic()-.3
            for n in range(frames):
                raw=json.dumps(dict(event_type='price_change',timestamp=epoch+n+1,price_changes=[
                    dict(asset_id=a,side='BUY',price='.4',size=str(n+1),hash=f'{index}/{n}/{a}') for a in assets]))
                spare['socket'].emit(raw)
                if n%50==0:wait_for(lambda:spare['inbox'].queue.unfinished_tasks==0)
            wait_for(lambda:spare['inbox'].queue.unfinished_tasks==0)
            assert all(pipeline.books[a]['bids']['.4']==str(frames) for a in assets)
            primary['socket'].kick(None,None)
            wait_for(lambda:manager.states[0] is spare)
            assert not spare.get('need_book')
            return weakref.ref(manager)
        finally:
            manager.close(2)


def run():
    rows=[];refs=[];start=time.monotonic();tracemalloc.start()
    with patch.dict('sys.modules',{'websocket':SimpleNamespace(WebSocketApp=ReplaySocket)}),patch.dict(os.environ,{'POLYMARKET_OBSERVER_WS_REPLAY':'1'}),patch.object(observer,'_STOP',threading.Event()),patch.object(observer,'STATUS',dict(reconnect_count=0,slow_consumer_count=0)):
        for n in range(60):
            refs.append(generation(n))
            if (n+1)%10==0:
                gc.collect();current,peak=tracemalloc.get_traced_memory()
                rows.append(dict(generations=n+1,current_python_bytes=current,peak_python_bytes=peak,
                    rss_bytes=observer.process_rss_bytes(),live_retired_managers=sum(ref() is not None for ref in refs),
                    threads=threading.active_count()))
                print(rows[-1],flush=True)
    result=dict(generations=60,frames_per_generation=500,physical_sockets=120,
        elapsed_seconds=time.monotonic()-start,samples=rows,
        python_growth_after_warmup=rows[-1]['current_python_bytes']-rows[1]['current_python_bytes'],
        limitation='Synthetic sockets; cannot exclude production TLS/native retention. RSS is not a Python heap size.',
        valid=all(r['live_retired_managers']==0 for r in rows))
    (ROOT/'deploy/r21/memory-check.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__=='__main__':
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    raise SystemExit(0 if run()['valid'] else 1)

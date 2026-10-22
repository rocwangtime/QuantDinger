"""Controlled busy-but-late TCP feed, actual observer readers/workers, no network.

The final seven seconds deliberately replay old rows to model the observed
stale fence surge. Production did not retain individual sibling frames; this
is a causal regression fixture, not a reconstruction of unrecorded wire data.
"""
import argparse
from collections import deque
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from polymarket_ws_handoff_replay import wait_for
from app.services.polymarket import observer
from app.services.polymarket.ws_group_process import ReplaySocket
from app.services.polymarket.ws_redundancy import FeedMerge


def replay(seconds=60, pressure=False, quiet=False):
    with ExitStack() as stack:
        stack.enter_context(patch.dict('sys.modules', {'websocket':SimpleNamespace(WebSocketApp=ReplaySocket)}))
        stack.enter_context(patch.dict('os.environ', {'POLYMARKET_OBSERVER_WS_REPLAY':'1'}))
        stack.enter_context(patch.object(observer,'_STOP',threading.Event()))
        stack.enter_context(patch.object(observer,'STATUS',dict(reconnect_count=0,slow_consumer_count=0)))
        pipeline=observer.Pipeline(); pipeline.recording.enabled=False
        assets=['ay','an']
        pipeline.set_markets([dict(id='a',yesAssetId='ay',noAssetId='an')])
        stack.enter_context(patch.object(observer,'_PIPELINE',pipeline))
        m=observer.MarketConnections(); m.count=m.balancer.count=1; m.liveness_backup=True
        m.balancer.assignment={a:0 for a in assets}; m.balancer.groups={a:'up_down:BTC:5m' for a in assets}
        m.merges[0]=FeedMerge({}); m.redundancy_tick=lambda now:None
        stack.callback(m.close,2)
        def state(backup=False):
            row=dict(assets=list(assets),socket=None,thread=None,connected=False,lock=threading.RLock(),attempts=0,retry_at=0,ping_at=0)
            if backup: row.update(required_assets=set(assets),primary_index=0,standby=True,redundant=True,busy=True,shadow=observer.WarmBook(assets),created_at=time.monotonic())
            return row
        primary=state();m.states[0]=primary;m.connect(0,primary)
        for i in range(2):
            spare=state(True);(m.backups if i==0 else m.reserves)[0]=spare;m.connect(0,spare)
        states=[primary,*[s for _,s in m.backup_items()]]
        wait_for(lambda:all(s.get('connected') and not s.get('subscribe_initial',True) for s in states))
        rest=[]
        def request(*a,**kw): rest.append(time.monotonic()); return False
        m.snapshot_recovery.request=request
        initial=[dict(event_type='book',asset_id=a,timestamp=round(time.time()*1000),hash='initial'+a,bids=[dict(price='.4',size='2')],asks=[dict(price='.6',size='1')]) for a in assets]
        oracle={};observer.ws_apply_event(initial,oracle)
        for s in states:s['socket'].emit(json.dumps(initial))
        wait_for(lambda:all(m.backup_ready(s) for _,s in m.backup_items()) and len(pipeline.books)==2)
        wait_for(lambda:all(s['inbox'].queue.unfinished_tasks==0 for s in states))
        burners=[]
        def stop_burners():
            for process in burners:
                if process.poll() is None:process.terminate()
            for process in burners:process.wait(timeout=3)
        if pressure:
            # Real OS scheduling pressure confined to the old primary worker.
            # Readers and spares retain normal priority. Four normal-priority
            # burners occupy the same four CPUs until the healthy handoff.
            parse=m.parsed_frames.parse
            def cpu_work(raw):
                if threading.current_thread() is primary['worker']:
                    until=time.thread_time()+.01
                    while time.thread_time()<until:pass
                return parse(raw)
            m.parsed_frames.parse=cpu_work
            os.sched_setscheduler(primary['worker'].native_id,os.SCHED_IDLE,os.sched_param(0))
            burners=[subprocess.Popen([sys.executable,'-c','while True: pass'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL) for _ in range(4)]
            stack.callback(stop_burners)
        publications={a:[time.monotonic()] for a in assets}
        original=pipeline.receive
        def receive(*a,**kw):
            before=dict(pipeline.recording.metadata);original(*a,**kw)
            for asset,row in pipeline.recording.metadata.items():
                if row!=before.get(asset):publications[asset].append(time.monotonic())
        pipeline.receive=receive
        pre_kicks=[]
        def diagnose():
            old=m.states[0]
            rows=[]
            for _,candidate in m.backup_items():
                shadow=candidate.get('shadow')
                if not isinstance(shadow,observer.WarmBook):continue
                peer=old.get('book_checkpoints')
                for asset in assets:
                    mark=shadow.checkpoints.latest.get(asset)
                    other=peer.latest.get(asset) if peer else None
                    baseline=shadow.baseline(asset,peer,pipeline.books.get(asset),m.merges[0].latest.get(asset))
                    rows.append(dict(source=candidate.get('source'),ready=m.backup_ready(candidate),asset=asset,
                        shadow_at=str(shadow.recorder.metadata.get(asset,(None,))[0]),mark=str(mark),peer=str(other),
                        live_at=str(m.merges[0].latest.get(asset)),book_equal=shadow.books.get(asset)==pipeline.books.get(asset),
                        peer_in_history=bool(other in shadow.checkpoints.history.get(asset,{}) if other else False),
                        baseline='book' if isinstance(baseline,dict) else baseline))
            return dict(primary=old.get('source'),candidates=rows)
        history=deque(maxlen=6000);switches=[];rates=[];next_rate=1;local_wait_max=0
        cpu_start=time.process_time()
        start=time.monotonic();epoch=round(time.time()*1000);seq=0;second=None;first_closed=None;replenishments=0
        counters=primary['inbox'].received_bytes
        # 50Hz × >8KB = at least 400KB/s even with delayed timestamps.
        padding='x'*9000
        deadline=start+(3 if quiet else seconds+5)
        while time.monotonic()<deadline:
            elapsed=time.monotonic()-start;seq+=1
            stamp=epoch+round(elapsed*1000)
            event=dict(event_type='price_change',timestamp=stamp,price_changes=[dict(asset_id=a,side='BUY',price='.4',size=str(seq+2),hash=str(seq)+a) for a in assets],padding=padding)
            history.append((elapsed,event))
            current=m.states[0]
            if current is not primary and (not switches or switches[-1]['source']!=current['source']):
                switches.append(dict(source=current['source'],seconds=elapsed))
                if first_closed is not None and second is None:second=current
            if not quiet:
                observer.ws_apply_event(event,oracle)
                delayed=event
                if elapsed<=seconds:
                    tail=min(7,seconds*.7)
                    # End at about 20s of exchange age. The final stale-only
                    # tail models the measured stale-fence surge, without
                    # silently turning a requested 20s ramp into a 30s delay.
                    delay=(20-tail)*elapsed/(seconds-tail)
                    due=elapsed-delay
                    if elapsed>=seconds-tail:
                        due=seconds-20-.1
                    delayed=next((ev for t,ev in reversed(history) if t<=due),initial)
                for s in states:
                    if s['socket'].stopped.is_set():continue
                    if s is primary and elapsed>seconds:continue
                    payload=delayed if s is primary and not pressure else event
                    # Padding maintains primary byte rate even during initial history gap.
                    if isinstance(payload,list): payload=[{**e,'padding':padding} for e in initial]
                    s['socket'].transport.pong_rtt_seconds=(min(20,20*elapsed/seconds) if s is primary and not pressure else .19)
                    s['socket'].emit(json.dumps(payload,separators=(',',':')))
            if not primary.get('retired'):
                local_wait_max=max(local_wait_max,primary['inbox'].pending_wait_seconds())
            if pressure:
                m.primary_pressure(0,time.monotonic())
                if m.states[0] is not primary:stop_burners()
            # The live manager replenishes retired generations. Keep two spares
            # after the preventive switch, so the later double kick has the
            # same protection budget as the original baseline.
            active=[s for s in states if not s.get('retired') and not s['socket'].stopped.is_set()]
            current=m.states[0]
            if (not quiet and len(active)<3 and replenishments<4
                    and current.get('connected') and not current.get('data_suspect')
                    and not current.get('need_book')):
                spare=state(True)
                m.reserves[(0, replenishments+1)]=spare
                m.connect(0,spare);states.append(spare);replenishments+=1
                wait_for(lambda:spare.get('connected') and not spare.get('subscribe_initial',True))
                snapshot=[dict(event_type='book',asset_id=a,timestamp=stamp,hash=str(seq)+a,
                    bids=[dict(price=p,size=q) for p,q in oracle[a]['bids'].items()],
                    asks=[dict(price=p,size=q) for p,q in oracle[a]['asks'].items()]) for a in assets]
                spare['socket'].emit(json.dumps(snapshot))
            if elapsed>=seconds and not quiet and first_closed is None:
                wait_for(lambda:all(s['inbox'].queue.unfinished_tasks==0 for s in states if not s.get('retired')))
                pre_kicks.append(diagnose())
                first_closed=m.states[0]
                first_closed['socket'].callbacks['on_error'](first_closed['socket'],RuntimeError('Connection to remote host was lost.'))
                first_closed['socket'].kick(None,None)
            if elapsed>=seconds+3 and second and not second['socket'].stopped.is_set():
                wait_for(lambda:all(s['inbox'].queue.unfinished_tasks==0 for s in states if not s.get('retired')))
                pre_kicks.append(diagnose())
                second['socket'].callbacks['on_error'](second['socket'],RuntimeError('Connection to remote host was lost.'))
                second['socket'].kick(None,None)
            if elapsed>=next_rate:
                if not primary.get('retired'):
                    rates.append((primary['inbox'].received_bytes-counters)/(elapsed-(next_rate-1)))
                counters=primary['inbox'].received_bytes;next_rate=int(elapsed)+1
            time.sleep(max(0,start+seq/50-time.monotonic()))
        wait_for(lambda:all(s['inbox'].queue.unfinished_tasks==0 for s in states if not s.get('retired')))
        sha=lambda b:hashlib.sha256(json.dumps(b,sort_keys=True).encode()).hexdigest()
        events=[{k:v for k,v in e.items() if not k.startswith('_')} for e in m.primary_failures.get('up_down:BTC:5m',{}).get('events',[])]
        return dict(pressure_model='SCHED_IDLE old primary worker plus four normal-priority burners on four CPUs' if pressure else None,scenario='quiet' if quiet else 'pressure' if pressure else 'upstream',seconds=seconds,
            max_publication_gap_ms=max(((b-a)*1000 for rows in publications.values() for a,b in zip(rows,rows[1:])),default=0),
            primary_min_bytes_per_second=min(rates[1:max(2,int(seconds)-1)],default=0),switches=switches,
            reason=primary.get('failure_reason'),classification=primary.get('freshness_classification'),
            primary_local_wait_max_seconds=local_wait_max,cpu_seconds=time.process_time()-cpu_start,measured_cpu_cores=(time.process_time()-cpu_start)/(time.monotonic()-start),
            stage_cpu_seconds=m.stage_cpu,pre_kicks=pre_kicks,
            rest_requests=len(rest),replenishments=replenishments,failures=events,double_kicks=m.double_kicks.metrics(time.time()),
            sha256=sha(pipeline.books),expected_sha256=sha(oracle),valid=(sha(pipeline.books)==sha(oracle) and all(e.get('merged_gap_final') for e in events) if not quiet else not switches))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--seconds',type=float,default=60);p.add_argument('--pressure',action='store_true');p.add_argument('--quiet',action='store_true');a=p.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    result=replay(a.seconds,a.pressure,a.quiet);print(json.dumps(result));raise SystemExit(0 if result['valid'] else 1)

"""Offline production-worker replay of r20 4s and promoted-primary 30.7s outages.

Only public synthetic WS data and a delayed fake REST response; no network/SQL.
Both versions use the identical fixture. Wall time is real, including heartbeat.
"""
import argparse
import hashlib
import json
import logging
import threading
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.services.polymarket import observer
from app.services.polymarket.ws_group_process import ReplaySocket
from app.services.polymarket.ws_redundancy import FeedMerge


def wait_for(predicate, timeout=3):
    until = time.monotonic()+timeout
    while not predicate():
        if time.monotonic() > until:
            raise TimeoutError('silence replay did not settle')
        time.sleep(.002)


def replay(scenario='four-second'):
    with ExitStack() as stack:
        stack.enter_context(patch.dict('sys.modules', {'websocket': SimpleNamespace(WebSocketApp=ReplaySocket)}))
        stack.enter_context(patch.dict('os.environ', {'POLYMARKET_OBSERVER_WS_REPLAY': '1'}))
        stack.enter_context(patch.object(observer, '_STOP', threading.Event()))
        stack.enter_context(patch.object(observer, 'STATUS', dict(reconnect_count=0, slow_consumer_count=0)))
        pipeline = observer.Pipeline()
        pipeline.recording.enabled = False
        assets = ['ay','an']
        pipeline.set_markets([dict(id='a', yesAssetId='ay', noAssetId='an')])
        stack.enter_context(patch.object(observer, '_PIPELINE', pipeline))
        manager = observer.MarketConnections()
        manager.count = manager.balancer.count = 1
        manager.liveness_backup = True
        manager.balancer.assignment = {a:0 for a in assets}
        manager.balancer.groups = {a:'up_down:ETH:5m' for a in assets}
        manager.merges[0] = FeedMerge({})
        manager.redundancy_tick = lambda now: None
        stack.callback(manager.close, 2)
        primary = dict(assets=assets, socket=None, thread=None, connected=False,
                       lock=threading.RLock(), attempts=0, retry_at=0, ping_at=0)
        manager.states[0] = primary
        manager.connect(0, primary)
        shadow_type = getattr(observer, 'WarmBook', observer._LivenessShadow)
        for i in range(2):
            candidate = dict(assets=list(assets), required_assets=set(assets), primary_index=0,
                socket=None, thread=None, connected=False, lock=threading.RLock(), attempts=0, retry_at=0, ping_at=0,
                standby=True, redundant=True, busy=True, shadow=shadow_type(assets), created_at=time.monotonic())
            (manager.backups if i == 0 else manager.reserves)[0] = candidate
            manager.connect(0, candidate)
        states = [primary, *[s for _,s in manager.backup_items()]]
        wait_for(lambda: all(s.get('connected') and not s.get('subscribe_initial',True) for s in states))
        oracle, rest_calls, rest_threads = {}, [], []
        def rest(state, requested, publish, **kwargs):
            rest_calls.append(time.monotonic())
            def complete():
                time.sleep(.33)
                stamp = round(time.time()*1000)
                books = [dict(event_type='book', asset_id=a, timestamp=stamp, hash='rest'+str(stamp)+a,
                    bids=[dict(price=p,size=q) for p,q in oracle[a]['bids'].items()],
                    asks=[dict(price=p,size=q) for p,q in oracle[a]['asks'].items()]) for a in requested]
                publish(books, datetime.now(timezone.utc))
            t = threading.Thread(target=complete,daemon=True)
            rest_threads.append(t)
            t.start()
            return True
        manager.snapshot_recovery.request = rest
        initial = [dict(event_type='book',asset_id=a,timestamp=round(time.time()*1000),hash='initial'+a,
            bids=[dict(price='.4',size='2')],asks=[dict(price='.6',size='1')]) for a in assets]
        observer.ws_apply_event(initial,oracle)
        for s in states:
            s['socket'].emit(json.dumps(initial))
        wait_for(lambda: all(manager.backup_ready(s) for _,s in manager.backup_items()) and len(pipeline.books)==2)
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks==0 for s in states))
        publications, switches = [], []
        original_receive = pipeline.receive
        def receive(*args,**kwargs):
            original_receive(*args,**kwargs)
            publications.append(time.monotonic())
        pipeline.receive = receive
        start = time.monotonic()
        publications.append(start)
        primary['socket'].stall(90)
        stalled = primary
        second_stall = None
        seq = 0
        until = start+45
        while time.monotonic() < until:
            now = time.monotonic()
            current = manager.states[0]
            if current is not stalled:
                if not switches or switches[-1]['source'] != current['source']:
                    switches.append(dict(source=current['source'], seconds=now-start))
                if scenario == 'four-second':
                    if manager.primary_failures['up_down:ETH:5m']['events'][-1].get('merged_gap_final'):
                        break
                elif second_stall is None:
                    # Match the production ~3s healthy interval after first handoff.
                    if now-start >= switches[0]['seconds']+3:
                        stalled = current
                        second_stall = now
                        current['socket'].stall(90)
                        # Last received PONG was just before the stall; next PING
                        # occurs 8.5s later, then the production 22s timeout.
                        current['socket'].transport.pending_ping_at = now+8.5
                elif len(switches) >= 2:
                    if manager.primary_failures['up_down:ETH:5m']['events'][-1].get('merged_gap_final'):
                        break
            stamp = round(time.time()*1000)
            seq += 1
            event = dict(event_type='price_change',timestamp=stamp,price_changes=[
                dict(asset_id=a,side='BUY',price='.4',size=str(seq+2),hash=str(seq)+a) for a in assets])
            observer.ws_apply_event(event,oracle)
            for s in states:
                if not s.get('retired'):
                    s['socket'].emit(json.dumps(event))
            manager.pong_timeout_tick(now)
            time.sleep(.025)
        else:
            raise TimeoutError('physical recovery not completed')
        for t in rest_threads:
            t.join(2)
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks==0 for s in states if not s.get('retired')))
        sha = lambda b: hashlib.sha256(json.dumps(b,sort_keys=True).encode()).hexdigest()
        events = [{k:v for k,v in e.items() if not k.startswith('_')}
            for e in manager.primary_failures['up_down:ETH:5m']['events']]
        return dict(scenario=scenario, wall_seconds=time.monotonic()-start, switches=switches,
            second_stall_seconds=second_stall-start if second_stall else None,
            max_publication_gap_ms=max((b-a)*1000 for a,b in zip(publications,publications[1:])),
            rest_requests=len(rest_calls), failures=events, silence=manager.liveness_silence,
            final_book_sha256=sha(pipeline.books),expected_book_sha256=sha(oracle),
            valid=sha(pipeline.books)==sha(oracle) and all(e.get('merged_gap_final') for e in events))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario',choices=['four-second','promoted-primary'],default='four-second')
    args=parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    result=replay(args.scenario)
    print(json.dumps(result))
    raise SystemExit(0 if result['valid'] else 1)

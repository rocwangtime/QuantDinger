"""Offline no-code close -> actual worker promotion -> snapshot hold regression.

Uses the selected PYTHONPATH's production observer, fake public transport and
an explicitly delayed REST response. No live socket, SQL or credentials.
"""
import argparse
import copy
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
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.001)
    raise TimeoutError('handoff replay did not settle')


def replay(silence=1.15, rest_delay=.33):
    with ExitStack() as stack:
        stack.enter_context(patch.dict('sys.modules', {'websocket': SimpleNamespace(WebSocketApp=ReplaySocket)}))
        stack.enter_context(patch.dict('os.environ', {'POLYMARKET_OBSERVER_WS_REPLAY': '1'}))
        stack.enter_context(patch.object(observer, '_STOP', threading.Event()))
        stack.enter_context(patch.object(observer, 'STATUS', dict(reconnect_count=0, slow_consumer_count=0)))
        pipeline = observer.Pipeline()
        pipeline.recording.enabled = False
        assets = ['ay', 'an']
        pipeline.set_markets([dict(id='a', yesAssetId='ay', noAssetId='an')])
        stack.enter_context(patch.object(observer, '_PIPELINE', pipeline))
        manager = observer.MarketConnections()
        manager.count = manager.balancer.count = 1
        manager.liveness_backup = True
        manager.balancer.assignment = {a: 0 for a in assets}
        manager.balancer.groups = {a: 'up_down:BTC:5m' for a in assets}
        manager.merges[0] = FeedMerge({})
        manager.redundancy_tick = lambda now: None
        primary = dict(assets=assets, socket=None, thread=None, connected=False, lock=threading.RLock(),
                       attempts=0, retry_at=0, ping_at=0)
        manager.states[0] = primary
        manager.connect(0, primary)
        shadow_type = getattr(observer, 'WarmBook', observer._LivenessShadow)
        for i in range(2):
            candidate = dict(assets=assets, required_assets=set(assets), primary_index=0,
                socket=None, thread=None, connected=False, lock=threading.RLock(), attempts=0,
                retry_at=0, ping_at=0, standby=True, redundant=True, busy=True,
                shadow=shadow_type(assets), created_at=time.monotonic())
            (manager.backups if i == 0 else manager.reserves)[0] = candidate
            manager.connect(0, candidate)
        stack.callback(manager.close, 2)
        all_states = [primary, *[s for _, s in manager.backup_items()]]
        wait_for(lambda: all(s.get('connected') and not s.get('subscribe_initial', True) for s in all_states))
        oracle = {}
        rest_requests = []
        times = {}
        rest_threads = []

        def rest(state, requested, publish, warm=False, pressure=False, **kwargs):
            if warm or pressure:
                return False
            rest_requests.append(dict(at=time.monotonic(), assets=list(requested)))

            def complete():
                time.sleep(rest_delay)
                stamp = round(time.time()*1000)
                books = [dict(event_type='book', asset_id=a, timestamp=stamp, hash=f'rest{stamp}{a}',
                    bids=[dict(price=p, size=q) for p, q in oracle[a]['bids'].items()],
                    asks=[dict(price=p, size=q) for p, q in oracle[a]['asks'].items()]) for a in requested]
                times['rest_return'] = time.monotonic()
                publish(books, datetime.now(timezone.utc))
            thread = threading.Thread(target=complete, daemon=True)
            rest_threads.append(thread)
            thread.start()
            return True

        manager.snapshot_recovery.request = rest
        epoch = round(time.time()*1000)
        initial = [dict(event_type='book', asset_id=a, timestamp=epoch, hash=f'init{a}',
                        bids=[dict(price='.4', size='2')], asks=[dict(price='.6', size='1')]) for a in assets]
        observer.ws_apply_event(initial, oracle)
        for state in all_states:
            state['socket'].emit(json.dumps(initial))
        wait_for(lambda: all(manager.backup_ready(s) for _, s in manager.backup_items())
                 and set(pipeline.books) == set(assets))
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks == 0 for s in all_states))
        times['last_primary_publication'] = time.monotonic()

        def emit_delta(seq, include_primary=False):
            event = dict(event_type='price_change', timestamp=round(time.time()*1000),
                price_changes=[dict(asset_id=a, side='BUY', price='.4', size=str(seq+2), hash=f'{seq}{a}') for a in assets])
            observer.ws_apply_event(event, oracle)
            raw = json.dumps(event)
            for state in all_states:
                if (state is not primary or include_primary) and not state['socket'].stopped.is_set():
                    state['socket'].emit(raw)

        # Verify a common price_change watermark, then only the primary is quiet.
        emit_delta(1, True)
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks == 0 for s in all_states))
        start_silence = time.monotonic()
        seq = 2
        while time.monotonic()-start_silence < silence:
            emit_delta(seq)
            seq += 1
            time.sleep(.025)
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks == 0 for _, s in manager.backup_items()))
        times['close_callback'] = time.monotonic()
        primary['socket'].callbacks['on_error'](primary['socket'], RuntimeError('Connection to remote host was lost.'))
        primary['socket'].kick(None, None)
        seen_need = False
        need_start = need_end = None
        # No new frame for 250ms: promotion must also work on an idle spare.
        deadline = time.monotonic()+3
        first_publication = None
        while time.monotonic() < deadline:
            state = manager.states[0]
            now = time.monotonic()
            if state is not primary and 'promotion' not in times:
                times['promotion'] = now
            if state.get('need_book'):
                seen_need = True
                need_start = need_start or now
            elif need_start and need_end is None:
                need_end = now
            events = manager.primary_failures.get('up_down:BTC:5m', {}).get('events', [])
            if events and events[-1].get('merged_gap_final'):
                first_publication = now
                break
            if now-times['close_callback'] > .25:
                emit_delta(seq)
                seq += 1
            time.sleep(.001)
        if first_publication is None:
            raise TimeoutError('no valid merged recovery')
        wait_for(lambda: all(s['inbox'].queue.unfinished_tasks == 0 for s in all_states))
        for thread in rest_threads:
            thread.join(2)
        event = manager.primary_failures['up_down:BTC:5m']['events'][-1]
        sha = lambda books: hashlib.sha256(json.dumps(books, sort_keys=True).encode()).hexdigest()
        return dict(scenario='no-code-close-warm-book', silence_s=silence, rest_delay_s=rest_delay,
            ready_backups=event['ready_backups'], code=event['code'],
            need_book_seen=seen_need, need_book_wait_ms=((need_end or first_publication)-need_start)*1000 if need_start else 0,
            rest_requests=len(rest_requests), promotion_ms=(times['promotion']-times['close_callback'])*1000,
            detection_to_first_valid_publication_ms=(first_publication-times['close_callback'])*1000,
            event=copy.deepcopy({k:v for k,v in event.items() if not k.startswith('_')}),
            final_book_sha256=sha(pipeline.books), expected_book_sha256=sha(oracle),
            valid=sha(pipeline.books) == sha(oracle) and bool(event.get('merged_gap_final')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silence', type=float, default=1.15)
    parser.add_argument('--rest-delay', type=float, default=.33)
    args = parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    result = replay(args.silence, args.rest_delay)
    print(json.dumps(result))
    raise SystemExit(0 if result['valid'] else 1)

"""Offline production-shaped worker replay. No network, SQL, credentials or orders.

Run with PYTHONPATH=. from backend_api_python. Synthetic public books/deltas:
four hot pairs, eight workers, 1896 physical / 948 published frames/s,
50ms bursts, 300ms skew. --stage-profile measures inclusive thread CPU per stage.
Includes real supervisor/status, recording metadata and bounded book history.
TLS, SQL/evaluation and upstream 1013 are deliberately outside this experiment.
Use the identical script and interpreter against both code trees.
"""
import argparse
import hashlib
import json
import logging
import threading
import time
from collections import deque
from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.services.polymarket import observer, ws_payloads, ws_redundancy, ws_rotation
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_transport import TransportDiagnostics


def run(seconds=15, rate=1896, levels=140, cold_pairs=19, stage_profile=False):
    sockets, http = [], []
    stages = {}
    stage_lock = threading.Lock()
    class Socket:
        def __init__(self, url, **callbacks):
            self.callbacks, self.assets = callbacks, []
            self.stopped = threading.Event()
            self.transport = TransportDiagnostics()
            sockets.append(self)
        def run_forever(self, **kwargs):
            self.callbacks['on_open'](self)
            self.stopped.wait()
        def send(self, raw):
            if raw == 'PING':
                self.transport.pong()
                return
            message = json.loads(raw)
            self.assets = message['assets_ids']
        def close(self):
            self.stopped.set()
        def emit(self, raw):
            self.transport.read(len(raw))
            self.callbacks['on_message'](self, raw)

    with ExitStack() as stack:
        def measured(name, original):
            def call(*args, **kwargs):
                start = time.thread_time()
                try:
                    return original(*args, **kwargs)
                finally:
                    elapsed = time.thread_time()-start
                    with stage_lock:
                        row = stages.setdefault(name, dict(calls=0, cpu_s=0))
                        row['calls'] += 1
                        row['cpu_s'] += elapsed
            return call
        if stage_profile:
            for cls, method in ((observer.Pipeline, 'receive'), (BookHistory, 'record'),
                    (BookHistory, 'trim_bytes'), (ws_payloads.ParsedFrames, 'parse'),
                    (ws_rotation.ShadowBooks, 'receive'), (ws_redundancy.FeedMerge, 'filter'),
                    (ws_redundancy.FeedContinuity, 'receive'), (observer.MarketConnections, 'tick')):
                name = cls.__name__+'.'+method
                stack.enter_context(patch.object(cls, method, measured(name, getattr(cls, method))))
        stack.enter_context(patch.dict('os.environ', {
            'POLYMARKET_OBSERVER_WS_SHARDS': '4',
            'POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT': '1',
            'POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS': '0',
            'POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED': '0',
        }))
        stack.enter_context(patch.dict('sys.modules', {'websocket': SimpleNamespace(WebSocketApp=Socket)}))
        stack.enter_context(patch.object(observer, '_STOP', threading.Event()))
        stack.enter_context(patch.object(observer, 'STATUS', dict(reconnect_count=0, slow_consumer_count=0)))
        p = observer.Pipeline()
        p.recording.enabled = False  # metadata still runs; no database writer
        p.paper_account = SimpleNamespace(history=BookHistory())
        stack.enter_context(patch.object(observer, '_PIPELINE', p))
        m = observer.MarketConnections()
        stack.enter_context(patch.object(m.snapshot_recovery, 'request',
                                        lambda *a, **kw: http.append(kw) or False))
        markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n',
            market_type='up_down', asset=asset, interval=interval)
            for i, (asset, interval) in enumerate((('BTC', '15m'), ('BTC', '5m'), ('ETH', '15m'), ('ETH', '5m')))]
        m.sync(markets)
        # Retained cold books make history eviction cost match the 46-asset
        # production catalogue; cold updates are sparse and included in timing.
        cold = [dict(event_type='book', asset_id=f'cold{i}', timestamp=int(time.time()*1000),
            bids=[], asks=[dict(price='.6', size='1')]) for i in range(cold_pairs*2)]
        deadline = time.monotonic() + 5
        # Establish full books before measuring; the bootstrap itself is tested separately.
        while len(m.backups) < 4 or not all(c.get('ready') for c in m.backups.values()):
            for sock in list(sockets):
                if sock.assets:
                    sock.emit(json.dumps([dict(event_type='book', asset_id=a, timestamp=int(time.time()*1000),
                        bids=[], asks=[dict(price='.6', size='1')]) for a in sock.assets]))
            m.backup_next_connect_at = 0
            m.tick()
            if time.monotonic() > deadline:
                raise RuntimeError('offline bootstrap failed')
            time.sleep(.01)
        m.start_supervisor()
        owners = [m.balancer.assignment[market['yesAssetId']] for market in markets]
        primary = [m.states[i]['socket'] for i in owners]
        backups = [m.backups[i]['socket'] for i in owners]
        # Generation excluded from the measurement; production-sized 76-digit IDs
        # are not required for book work but payload bytes are reported explicitly.
        raws = []
        epoch = int(time.time()*1000)
        for i in range(int(seconds*rate/2)):
            group, sequence = i % 4, i // 4
            at = epoch + round(i*2000/rate)
            assets = markets[group]['yesAssetId'], markets[group]['noAssetId']
            if sequence % 5 == 0:
                event = [dict(event_type='book', asset_id=a, timestamp=at, hash=f'{i}{a}',
                    bids=[dict(price=str(x/1000), size='3') for x in range(400-levels//2, 400)],
                    asks=[dict(price=str(x/1000), size='4') for x in range(600, 600+levels//2)]) for a in assets]
            else:
                event = dict(event_type='price_change', timestamp=at, price_changes=[
                    dict(asset_id=a, side='SELL', price=str(x/1000), size=str(i), hash=f'{i}{a}{x}',
                        best_bid='.399', best_ask='.600') for a in assets for x in range(600, 604)])
            raws.append((group, json.dumps(event, separators=(',', ':'))))
        pending = deque()
        frames = total_bytes = ready = samples = 0
        clock_drift = 0
        published_before = p.messages
        peak = 0
        began, cpu = time.monotonic(), time.process_time()
        wall_began = time.time()
        try:
            for burst in range(int(seconds/.05)+7):
                time.sleep(max(0, began + burst*.05-time.monotonic()))
                clock_drift = max(clock_drift, abs(time.time()-wall_began-(time.monotonic()-began)))
                if burst % 20 == 0:
                    with p.lock:
                        for event in cold:
                            p.paper_account.history.record(event, epoch + burst*50)
                end = min(len(raws), round((burst+1)*.05*rate/2))
                start = min(len(raws), round(burst*.05*rate/2))
                for group, raw in raws[start:end]:
                    primary[group].emit(raw)
                    pending.append((burst+6, group, raw))
                while pending and pending[0][0] <= burst:
                    _, group, raw = pending.popleft()
                    backups[group].emit(raw)
                    frames += 2
                    total_bytes += len(raw)*2
                for candidate in m.backups.values():
                    peak = max(peak, candidate['inbox'].pending_wait_seconds())
                    ready += m.backup_ready(candidate)
                    samples += 1
            while any(s['inbox'].queue.qsize() or s['inbox'].processing_received_at
                      for s in list(m.states.values()) + list(m.backups.values())):
                if time.monotonic()-began > seconds+10:
                    raise RuntimeError('workers did not drain')
                time.sleep(.01)
            used = time.process_time()-cpu
            result = dict(seconds=time.monotonic()-began, frames=frames, bytes=total_bytes,
                cpu_s=used, ms_per_wire_message=used/frames*1000, peak_backup_queue_ms=peak*1000,
                published=p.messages-published_before,
                ms_per_published_message=used/max(1, p.messages-published_before)*1000,
                ready_ratio=ready/samples, resyncs=sum(s['inbox'].resyncs for s in m.backups.values()),
                http_attempts=len(http), history=p.paper_account.history.metrics(),
                clock_drift_s=clock_drift,
                drops={i: dict(s['inbox'].drop_reasons) for i, s in m.backups.items()},
                assignment={str(i): m.balancer.assignment[market['yesAssetId']] for i, market in enumerate(markets)},
                final_book_sha256=hashlib.sha256(json.dumps(p.books, sort_keys=True).encode()).hexdigest())
        finally:
            m.close()
        expected = {}
        for _, raw in raws:
            observer.ws_apply_event(json.loads(raw), expected)
        result['expected_book_sha256'] = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
        result['valid'] = (result['published'] == len(raws)
            and result['final_book_sha256'] == result['expected_book_sha256']
            and result['resyncs'] == 0)
        if stage_profile:
            result['stage_profile_inclusive'] = stages
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, default=15)
    parser.add_argument('--rate', type=float, default=1896)
    parser.add_argument('--levels', type=int, default=140)
    parser.add_argument('--cold-pairs', type=int, default=19)
    parser.add_argument('--stage-profile', action='store_true')
    args = parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    if args.seconds < 1 or args.rate < 1 or not 2 <= args.levels <= 600 or args.cold_pairs < 0:
        parser.error('seconds/rate must be positive, levels 2..600, cold-pairs nonnegative')
    result = run(args.seconds, args.rate, args.levels, args.cold_pairs, args.stage_profile)
    print(json.dumps(result))
    raise SystemExit(0 if result['valid'] else 1)

"""Offline production-shaped worker replay. No network, SQL, credentials or orders.

Run with PYTHONPATH=. from backend_api_python. Synthetic public books/deltas:
four hot pairs, eight workers, 1896 physical / 948 published frames/s,
50ms bursts, 300ms skew. --stage-profile measures inclusive thread CPU per stage.
Includes real supervisor/status, recording metadata and bounded book history.
TLS, SQL/evaluation and upstream 1013 are deliberately outside this experiment.
Use the identical script and interpreter against both code trees.

Fix-A options (all default off, so earlier invocations are unchanged):
--lock-timing wraps lifecycle_lock and Pipeline.lock with an outermost
wait/hold recorder; --market-loop adds the production 1s market-loop tick and
a 10s snapshot()+json.dumps (no SQL); --rest-hour-buckets N pre-fills the
REST rolling hour like a long-running process; --prod-gil applies the
production 0.5ms GIL switch interval; --gil-load F adds a pure-Python thread
holding the GIL for fraction F (evaluator/paper stand-in). Queue wait is sampled
every 50ms for hot primaries and backups.
"""
import argparse
import hashlib
import json
import logging
import threading
import time
from collections import deque
from contextlib import ExitStack
import math
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.services.polymarket import observer, ws_payloads, ws_redundancy, ws_rotation
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_transport import TransportDiagnostics


def pct(values, q):
    values = sorted(values)
    return values[min(len(values)-1, max(0, math.ceil(len(values)*q)-1))] if values else None


class TimedLock:
    """Outermost wait/hold recorder around an RLock; reentrant entries are free."""
    def __init__(self, inner, name, sink):
        self.inner, self.name, self.sink, self.local = inner, name, sink, threading.local()

    def acquire(self, blocking=True, timeout=-1):
        depth = getattr(self.local, 'depth', 0)
        if depth:
            ok = self.inner.acquire(blocking, timeout)
        else:
            began = time.perf_counter()
            ok = self.inner.acquire(blocking, timeout)
            self.local.held_at = time.perf_counter()
            if ok:
                self.sink.append((self.name, threading.get_ident(), 'wait', self.local.held_at-began))
        if ok:
            self.local.depth = depth+1
        return ok

    def release(self):
        self.local.depth -= 1
        if not self.local.depth:
            self.sink.append((self.name, threading.get_ident(), 'hold', time.perf_counter()-self.local.held_at))
        self.inner.release()

    __enter__ = acquire

    def __exit__(self, *exc):
        self.release()


def run(seconds=15, rate=1896, levels=140, cold_pairs=19, stage_profile=False, lock_timing=False,
        market_loop=False, rest_hour_buckets=0, prod_gil=False, gil_load=0.0):
    sockets, http = [], []
    lock_events = []
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
                    (ws_redundancy.FeedContinuity, 'receive'), (observer.MarketConnections, 'tick'),
                    (observer.SnapshotRecovery, 'metrics'), (observer.MarketConnections, 'update_hot_coverage'),
                    (observer.Pipeline, 'metrics'), (observer.MarketConnections, 'publish_status')):
                if not hasattr(cls, method):
                    continue  # Older trees lack newer stages; same script runs on both.
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
        if prod_gil:
            previous_switch = observer.sys.getswitchinterval()
            stack.callback(observer.sys.setswitchinterval, previous_switch)
            observer.tune_switch_interval()
        p = observer.Pipeline()
        if lock_timing:
            p.lock = TimedLock(p.lock, 'pipeline', lock_events)
        p.recording.enabled = False  # metadata still runs; no database writer
        p.paper_account = SimpleNamespace(history=BookHistory(), ready=True, dropped=0, error=None)
        stack.enter_context(patch.object(observer, '_PIPELINE', p))
        m = observer.MarketConnections()
        if lock_timing:
            m.lifecycle_lock = TimedLock(m.lifecycle_lock, 'lifecycle', lock_events)
        if rest_hour_buckets:
            # A long-running process: one populated second per bucket, oldest first.
            base = time.monotonic()
            for k in range(min(3599, rest_hour_buckets), 0, -1):
                for field in ('attempts', 'requests', 'completed'):
                    m.snapshot_recovery.count('recovery', field, base-k)
                m.snapshot_recovery.count('pressure', 'deferred', base-k)
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
        hot_primary = [m.states[i] for i in owners]
        primary_waits, backup_waits, backup_lifecycle, primary_lock = [], [], [], []
        background_stop = threading.Event()
        background = []
        snapshot_errors = []
        if market_loop:
            def loop():
                persist_at = time.monotonic() + 10
                while not background_stop.wait(1):
                    m.tick()
                    if time.monotonic() >= persist_at:
                        persist_at = time.monotonic() + 10
                        try:
                            json.dumps(observer.snapshot(), default=str)
                        except Exception as exc:  # report, never hide
                            snapshot_errors.append(repr(exc)[:200])
            background.append(threading.Thread(target=loop, name='replay-market-loop', daemon=True))
        if gil_load > 0:
            def burn():
                while not background_stop.is_set():
                    end = time.perf_counter() + .01*gil_load
                    while time.perf_counter() < end:
                        sum(range(200))
                    background_stop.wait(.01*(1-gil_load))
            background.append(threading.Thread(target=burn, name='replay-gil-load', daemon=True))
        for thread in background:
            thread.start()
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
                for candidate in list(m.backups.values()):
                    wait = candidate['inbox'].pending_wait_seconds()
                    peak = max(peak, wait)
                    backup_waits.append(wait)
                    backup_lifecycle.append(candidate['inbox'].lifecycle_wait_seconds)
                    ready += m.backup_ready(candidate)
                    samples += 1
                for state in hot_primary:
                    if state.get('inbox'):
                        primary_waits.append(state['inbox'].pending_wait_seconds())
                        primary_lock.append(state['inbox'].lock_wait_seconds)
            while any(s['inbox'].queue.qsize() or s['inbox'].processing_received_at
                      for s in list(m.states.values()) + list(m.backups.values())):
                if time.monotonic()-began > seconds+10:
                    raise RuntimeError('workers did not drain')
                time.sleep(.01)
            used = time.process_time()-cpu
            background_stop.set()
            for thread in background:
                thread.join(5)
            result = dict(seconds=time.monotonic()-began, frames=frames, bytes=total_bytes,
                cpu_s=used, ms_per_wire_message=used/frames*1000, peak_backup_queue_ms=peak*1000,
                published=p.messages-published_before,
                ms_per_published_message=used/max(1, p.messages-published_before)*1000,
                ready_ratio=ready/samples, resyncs=sum(s['inbox'].resyncs for s in m.backups.values()),
                http_attempts=len(http), history=p.paper_account.history.metrics(),
                clock_drift_s=clock_drift, snapshot_errors=snapshot_errors[:3], cpu_cores=used/max(.001, time.monotonic()-began),
                primary_queue_wait_ms=dict(p50=pct(primary_waits, .5)*1000, p99=pct(primary_waits, .99)*1000,
                                           max=max(primary_waits)*1000, samples=len(primary_waits)),
                backup_queue_wait_ms=dict(p50=pct(backup_waits, .5)*1000, p99=pct(backup_waits, .99)*1000,
                                          max=max(backup_waits)*1000, samples=len(backup_waits)),
                backup_lifecycle_wait_sampled_ms=dict(p99=pct(backup_lifecycle, .99)*1000,
                                                      max=max(backup_lifecycle)*1000),
                primary_pipeline_wait_sampled_ms=dict(p99=pct(primary_lock, .99)*1000,
                                                      max=max(primary_lock)*1000),
                drops={i: dict(s['inbox'].drop_reasons) for i, s in m.backups.items()},
                assignment={str(i): m.balancer.assignment[market['yesAssetId']] for i, market in enumerate(markets)},
                final_book_sha256=hashlib.sha256(json.dumps(p.books, sort_keys=True).encode()).hexdigest())
            if lock_timing:
                roles = {}
                for kind, collection in (('primary', m.states), ('backup', m.backups)):
                    for state in collection.values():
                        if state.get('worker'):
                            roles[state['worker'].ident] = kind
                if m.supervisor:
                    roles[m.supervisor.ident] = 'supervisor'
                for thread in background:
                    roles[thread.ident] = thread.name.replace('replay-', '')
                summary = {}
                for name, ident, kind, value in list(lock_events):
                    row = summary.setdefault(name, {}).setdefault(roles.get(ident, 'other'), {}).setdefault(kind, [])
                    row.append(value)
                result['lock_timing'] = {name: {role: {kind: dict(count=len(v), total_s=sum(v),
                    p99_ms=pct(v, .99)*1000, max_ms=max(v)*1000) for kind, v in kinds.items()}
                    for role, kinds in roles_.items()} for name, roles_ in summary.items()}
        finally:
            background_stop.set()
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
    parser.add_argument('--lock-timing', action='store_true')
    parser.add_argument('--market-loop', action='store_true')
    parser.add_argument('--rest-hour-buckets', type=int, default=0)
    parser.add_argument('--prod-gil', action='store_true')
    parser.add_argument('--gil-load', type=float, default=0.0)
    args = parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    if (args.seconds < 1 or args.rate < 1 or not 2 <= args.levels <= 600 or args.cold_pairs < 0
            or not 0 <= args.gil_load < 1 or args.rest_hour_buckets < 0):
        parser.error('seconds/rate must be positive, levels 2..600, cold-pairs nonnegative')
    result = run(args.seconds, args.rate, args.levels, args.cold_pairs, args.stage_profile, args.lock_timing,
                 args.market_loop, args.rest_hour_buckets, args.prod_gil, args.gil_load)
    print(json.dumps(result))
    raise SystemExit(0 if result['valid'] else 1)

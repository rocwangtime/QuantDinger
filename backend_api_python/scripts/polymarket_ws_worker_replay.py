"""Offline production-shaped worker replay. No network, SQL, credentials or orders.

Run with PYTHONPATH=. from backend_api_python. Synthetic public books/deltas:
four hot pairs, eight workers, 1896 physical / 948 published frames/s,
50ms bursts, 300ms skew. --stage-profile measures inclusive thread CPU per stage.
Includes real supervisor/status, recording metadata and bounded book history.
TLS, SQL/evaluation and upstream 1013 are deliberately outside the in-process
experiment. Use the identical script and interpreter against both code trees.

--process-per-group drives the fix-B feed processes (one hot group each) with
the same frames. --even keeps the legacy round-robin; the default for that
mode is the production frame mix. --background-load F is a second process
burning about F cores. --kicks N injects one 1013 per hot group. --confine-cpus
N pins this process, the feeds and the burner to N CPUs.

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
import os
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
        # Business latency: synthetic exchange timestamp -> merged publication
        # (Pipeline.receive returns), whichever physical copy delivered it.
        merged_lag, schedule = [], {}
        original_receive = observer.Pipeline.receive
        def receive(self, raw, *args, **kwargs):
            result = original_receive(self, raw, *args, **kwargs)
            stamp = raw[0].get('timestamp') if isinstance(raw, list) and raw and isinstance(raw[0], dict) else \
                raw.get('timestamp') if isinstance(raw, dict) else None
            if isinstance(stamp, int) and schedule and stamp >= schedule['epoch']:
                # Lag behind the replay's own emission schedule (burst start).
                merged_lag.append(time.time()*1000 - schedule['wall_ms'] - (stamp - schedule['epoch'])//50*50)
            return result
        stack.enter_context(patch.object(observer.Pipeline, 'receive', receive))
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
        schedule.update(epoch=epoch, wall_ms=wall_began*1000)
        rss_began = observer.process_rss_bytes()
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
                rss_bytes=dict(began=rss_began, drained=observer.process_rss_bytes()),
                merged_publication_lag_ms=dict(p50=pct(merged_lag, .5), p99=pct(merged_lag, .99),
                                               max=max(merged_lag, default=None), samples=len(merged_lag)),
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


def _proc_cpu_seconds(pid):
    with open(f'/proc/{pid}/stat', encoding='ascii') as source:
        text = source.read()
    fields = text[text.rfind(')') + 2:].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def _burn(fraction, stop):
    while not stop.is_set():
        end = time.perf_counter() + 0.02 * fraction
        while time.perf_counter() < end and not stop.is_set():
            sum(range(80))
        stop.wait(0.02 * (1 - fraction))


def run_processes(seconds=20, rate=3300, levels=140, cold_pairs=19, market_loop=True,
                  background_load=0.0, weighted=True, kicks=1, confine_cpus=0):
    """Multi-process replay. Children parse; the parent only applies merged events."""
    import multiprocessing
    import os

    if confine_cpus:
        allowed = sorted(os.sched_getaffinity(0))
        os.sched_setaffinity(0, set(allowed[:confine_cpus]))
    ctx = multiprocessing.get_context('spawn')
    stop_burn = ctx.Event()
    burner = None
    if background_load > 0:
        burner = ctx.Process(target=_burn, args=(background_load, stop_burn), name='replay-background', daemon=True)
        burner.start()
    try:
        return _run_processes_inner(seconds, rate, levels, cold_pairs, market_loop, weighted, kicks, burner, stop_burn)
    finally:
        if burner is not None:
            stop_burn.set()
            burner.join(2)
            if burner.is_alive():
                burner.kill()
                burner.join(1)


def _run_processes_inner(seconds, rate, levels, cold_pairs, market_loop, weighted, kicks, burner, stop_burn):
    from app.services.polymarket.ws_group_process import (
        PROD_FRAME_WEIGHTS, HOT_ORDER, GroupFeeds, planned_events)
    with ExitStack() as stack:
        stack.enter_context(patch.dict('os.environ', {
            'POLYMARKET_OBSERVER_WS_SHARDS': '4',
            'POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT': '1',
            'POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS': '0',
            'POLYMARKET_OBSERVER_WS_COLD_ROTATION_ENABLED': '0',
            'POLYMARKET_OBSERVER_WS_REPLAY': '1',
            'POLYMARKET_OBSERVER_PROCESS_PER_GROUP': '1',
        }))
        stack.enter_context(patch.object(observer, '_STOP', threading.Event()))
        stack.enter_context(patch.object(observer, 'STATUS', dict(mode='running', enabled=True, running=True,
                                                                   reconnect_count=0, slow_consumer_count=0)))
        previous_switch = observer.sys.getswitchinterval()
        stack.callback(observer.sys.setswitchinterval, previous_switch)
        observer.tune_switch_interval()
        pipeline = observer.Pipeline()
        pipeline.recording.enabled = False
        pipeline.paper_account = SimpleNamespace(history=BookHistory(), ready=True, dropped=0, error=None)
        stack.enter_context(patch.object(observer, '_PIPELINE', pipeline))
        stack.enter_context(patch.object(observer, '_WS', None))
        feeds = GroupFeeds(replay=True)
        observer._WS = feeds
        stack.callback(feeds.close, 5)
        markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n',
                        market_type='up_down', asset=asset, interval=interval)
                   for i, (asset, interval) in enumerate((('BTC', '15m'), ('BTC', '5m'), ('ETH', '15m'), ('ETH', '5m')))]
        pipeline.set_markets(markets)
        cold = [dict(event_type='book', asset_id=f'cold{i}', timestamp=int(time.time() * 1000),
                     bids=[], asks=[dict(price='.6', size='1')]) for i in range(cold_pairs * 2)]
        feeds.sync(markets)
        feeds.start_supervisor()
        feeds.wait_ready(20)
        feeds.publish_status()
        snapshot_errors = []
        background_stop = threading.Event()
        threads = []
        if market_loop:
            def loop():
                while not background_stop.wait(1):
                    feeds.tick()
                    try:
                        json.dumps(observer.snapshot(), default=str)
                    except Exception as exc:
                        snapshot_errors.append(repr(exc)[:200])
            threads.append(threading.Thread(target=loop, name='replay-market-loop', daemon=True))
        for thread in threads:
            thread.start()
        weights = [PROD_FRAME_WEIGHTS[key] for key in HOT_ORDER] if weighted else None
        epoch = int(time.time() * 1000)
        start = feeds.drive(dict(seconds=seconds, rate=rate, levels=levels, epoch=epoch,
                                 mode='weighted' if weighted else 'even', weights=weights, lead=1.0))
        hot_keys = [key for key in feeds.order if feeds.children[key].spec.get('hot')]
        kick_at = []
        if kicks and hot_keys and seconds >= 4:
            span = max(1.0, seconds - 3)
            kick_at = [1.5 + span * (i + 0.5) / len(hot_keys) for i in range(len(hot_keys))]
            def punch():
                for key, at in zip(hot_keys, kick_at):
                    delay = start + at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    feeds.kick(key, 1013, 'slow consumer')
            threads.append(threading.Thread(target=punch, name='replay-kicks', daemon=True))
            threads[-1].start()
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        for child in feeds.children.values():
            child.samples.clear()
        feeds.gaps.clear()
        published_before = pipeline.messages
        rss_began = observer.process_rss_bytes()
        began = time.monotonic()
        cpu = time.process_time()
        burner_at = _proc_cpu_seconds(burner.pid) if burner is not None else None
        # Cold history only; it is not a websocket group.
        next_cold = began
        end = start + seconds + 0.45
        while time.monotonic() < end:
            if time.monotonic() >= next_cold:
                with pipeline.lock:
                    for event in cold:
                        pipeline.paper_account.history.record(event, epoch)
                next_cold += 1
            time.sleep(0.05)
        # Drain the 300 ms backup skew plus one batch.
        time.sleep(0.6)
        parent_cpu = time.process_time() - cpu
        elapsed = time.monotonic() - began
        background_stop.set()
        for thread in threads:
            thread.join(5)
        feeds.publish_status()
        samples = [row for child in feeds.children.values() for row in child.samples]
        primary = [row['wait'] for row in samples if row.get('role') == 'primary' and row.get('wait') is not None]
        backup = [row['wait'] for row in samples if row.get('role') == 'backup' and row.get('wait') is not None]
        ready_rows = [row for row in samples if row.get('role') == 'backup' and 'ready' in row]
        status = observer.snapshot()
        codes = []
        for row in (status.get('ws_last_transport_failures') or {}).values():
            codes.append(row.get('code'))
        for row in (status.get('ws_hot_primary_failures') or {}).values():
            for event in row.get('events') or []:
                codes.append(event.get('code'))
        for row in (status.get('ws_primary_1013_by_shard') or {}).values():
            codes.append(1013 if row.get('total') else None)
        expected = {}
        logical = 0
        for index, child in enumerate(child for child in (feeds.children[key] for key in hot_keys)):
            assets = [asset for market in child.spec['markets'] for asset in (market['yesAssetId'], market['noAssetId'])]
            spec = dict(seconds=seconds, rate=rate, levels=levels, epoch=epoch, assets=assets, group=index,
                        groups=len(hot_keys), mode='weighted' if weighted else 'even', weights=weights)
            for _offset, role, event in planned_events(spec):
                if role != 'primary':
                    continue
                logical += 1
                observer.ws_apply_event(event, expected)
        final = hashlib.sha256(json.dumps(pipeline.books, sort_keys=True).encode()).hexdigest()
        expect = hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()
        gaps = [row['seconds'] for row in feeds.gaps if isinstance(row.get('seconds'), (int, float))]
        if burner is not None and burner_at is not None:
            burner_cores = (_proc_cpu_seconds(burner.pid) - burner_at) / max(elapsed, 0.001)
        else:
            burner_cores = 0
        resyncs = 0
        for row in (status.get('ws_backups') or {}).values():
            resyncs += row.get('resyncs') or 0
        published = pipeline.messages - published_before
        ready_ratio = (sum(bool(row.get('ready')) for row in ready_rows) / len(ready_rows)) if ready_rows else None
        primary_p99 = pct(primary, .99) * 1000 if primary else None
        gap_max = max(gaps) if gaps else None
        codes_1008 = sum(code == 1008 for code in codes)
        gap_ok = (not kick_at) or (len(gaps) >= len(kick_at) and gap_max is not None and gap_max <= 1.5)
        result = dict(mode='process-per-group', weighted=weighted, seconds=elapsed, rate=rate,
                      logical=logical, published=published,
                      ready_ratio=ready_ratio, ready_samples=len(ready_rows),
                      resyncs=resyncs,
                      primary_queue_wait_ms=dict(p50=pct(primary, .5) * 1000 if primary else None,
                                                 p99=primary_p99,
                                                 max=max(primary) * 1000 if primary else None, samples=len(primary)),
                      backup_queue_wait_ms=dict(p50=pct(backup, .5) * 1000 if backup else None,
                                                p99=pct(backup, .99) * 1000 if backup else None,
                                                max=max(backup) * 1000 if backup else None, samples=len(backup)),
                      failover_gaps_s=gaps, failover_gap_max_s=gap_max,
                      kicks=len(kick_at), codes_1008=codes_1008,
                      close_reasons=status.get('close_reason_counts'),
                      parent_mode=status.get('mode'),
                      cpu_cores=status.get('ws_process_cpu_cores'), parent_cpu_s=parent_cpu,
                      burner_cores=burner_cores, restarts=dict(feeds.restarts),
                      stale_skipped=sum(child.stale_skipped for child in feeds.children.values()),
                      rss_bytes=dict(began=rss_began, drained=observer.process_rss_bytes(),
                                     status=status.get('ws_process_rss_bytes')),
                      snapshot_errors=snapshot_errors[:3],
                      final_book_sha256=final, expected_book_sha256=expect,
                      valid=(final == expect and not snapshot_errors
                             and codes_1008 == 0 and not feeds.restarts and status.get('mode') == 'ws'
                             and ready_ratio is not None and ready_ratio >= 0.95
                             and primary_p99 is not None and primary_p99 < 250 and gap_ok))
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
    parser.add_argument('--process-per-group', action='store_true')
    parser.add_argument('--background-load', type=float, default=0.0)
    parser.add_argument('--even', action='store_true')
    parser.add_argument('--kicks', type=int, default=1)
    parser.add_argument('--confine-cpus', type=int, default=0)
    args = parser.parse_args()
    logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    if (args.seconds < 1 or args.rate < 1 or not 2 <= args.levels <= 600 or args.cold_pairs < 0
            or not 0 <= args.gil_load < 1 or args.rest_hour_buckets < 0
            or not 0 <= args.background_load < 1 or args.kicks < 0 or args.confine_cpus < 0):
        parser.error('seconds/rate must be positive, levels 2..600, loads in [0, 1), counts nonnegative')
    if args.process_per_group:
        result = run_processes(args.seconds, args.rate, args.levels, args.cold_pairs, market_loop=True,
                               background_load=args.background_load, weighted=not args.even, kicks=args.kicks,
                               confine_cpus=args.confine_cpus)
    else:
        result = run(args.seconds, args.rate, args.levels, args.cold_pairs, args.stage_profile, args.lock_timing,
                     args.market_loop, args.rest_hour_buckets, args.prod_gil, args.gil_load)
    print(json.dumps(result))
    raise SystemExit(0 if result['valid'] else 1)

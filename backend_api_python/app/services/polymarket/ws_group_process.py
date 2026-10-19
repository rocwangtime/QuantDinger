"""One feed process per hot Up/Down group, plus one process for every cold market.

The parent keeps the public book store, recording, paper account, evaluator and
the STATUS document. Each child runs the existing MarketConnections path
(socket, parse, shadow book, merge, 1013 promotion) for its own markets and
forwards only events that already passed FeedMerge. Raw frames never cross
the process boundary.

IPC is a unidirectional pipe of pickled batches, flushed every 8 ms or 32
events. At the published rate (about 1k merged events/s, not 3.3k physical
frames) that is a few dozen kilobytes and well under a millisecond of CPU per
second per group. A shared-memory ring would save a copy and add crash and
restart failure modes this rate does not need. The pipe is widened to 1 MiB so
a book burst does not block the flusher; the book worker never waits on the
parent. If the parent falls behind, the child drops the oldest batched events
past 8k and counts them — queue_wait on the socket stays the backlog signal.

Inside a feed process the hot backup does not rebuild a book. It checks
liveness, asset, timestamp and hash. On failover the socket is promoted and
the book is resynced from the next WS snapshot or REST; price changes wait
for that snapshot. The in-process engine (PROCESS_PER_GROUP=0) still builds
the shadow book.

POLYMARKET_OBSERVER_PRIMARY_FIRST defaults off. Set it to 1 and a feed-process
backup sleeps 1ms while its live primary queue holds a frame older than 50ms.
The replay gate's failover_gaps_s is kick-to-next-forwarded event (see
FAILOVER_GAP_DEFINITION). It is not merged_gap_ms and not the
hot-primary-failure business_gap flag.

Rollback: POLYMARKET_OBSERVER_PROCESS_PER_GROUP=0 keeps the in-process engine.
The observer command defaults the variable to 1. Tests leave it unset.
"""
import copy
import json
import logging
import math
import os
import pickle
import signal
import threading
import time
from datetime import datetime, timezone

from .ws_balance import balance_key, ratio
from .ws_balance import RateBalancer

_LOG = logging.getLogger(__name__)

HOT_ORDER = (
    'up_down:BTC:15m', 'up_down:BTC:5m', 'up_down:ETH:15m', 'up_down:ETH:5m')
# Physical-frame mix measured 2026-10-19 (primary+backup). Cold sockets are
# negligible and stay in the cold process, so the hot weights sum to 1.
PROD_FRAME_WEIGHTS = {
    'up_down:BTC:15m': 778 / 3254,
    'up_down:BTC:5m': 1460 / 3254,
    'up_down:ETH:15m': 382 / 3254,
    'up_down:ETH:5m': 634 / 3254,
}
PARENT_OWNED = frozenset({
    'enabled', 'running', 'mode', 'markets_subscribed', 'started_at',
    'last_message_at', 'flush_complete', 'updown_subscriptions', 'updown_rejections',
})
SHARD_DICTS = frozenset({
    'ws_receive_queues', 'ws_backups', 'ws_reserves', 'ws_merge_metrics',
    'ws_connection_ages_seconds', 'ws_subscription_pending',
    'ws_last_transport_failures', 'ws_primary_1013_by_shard',
})
SHARD_LISTS = frozenset({
    'ws_shard_assets', 'ws_shard_loads', 'ws_shard_byte_loads', 'ws_capacity_by_shard',
})
SUM_KEYS = frozenset({
    'ws_process_cpu_cores', 'ws_process_rss_bytes', 'ws_retiring_connections',
    'ws_rotation_count', 'ws_rotation_failures', 'ws_backup_promotions',
    'ws_standby_slow_consumer_count', 'ws_connected', 'ws_connections',
    'reconnect_count', 'slow_consumer_count', 'rebalance_count', 'ws_unmeasured_groups',
})
MAX_KEYS = frozenset({
    'ws_failover_ms', 'ws_tick_gap_seconds', 'ws_tick_gap_max_seconds',
    'ws_rebalance_pressure_samples',
})
MERGE_DICTS = frozenset({
    'ws_local_100ms', 'ws_hot_backup_by_group', 'ws_hot_primary_failures',
    'ws_group_rates', 'ws_group_byte_rates', 'ws_group_ingress_byte_rates',
    'close_reason_counts',
})
_STOP_CHILD = threading.Event()


def process_per_group_enabled():
    return os.getenv('POLYMARKET_OBSERVER_PROCESS_PER_GROUP', '0').lower() in {'1', 'true', 'yes'}


def primary_first_enabled():
    """Yield a backup while its live primary queue is old. Default off."""
    return os.getenv('POLYMARKET_OBSERVER_PRIMARY_FIRST', '0').lower() in {'1', 'true', 'yes'}


def restart_backoff_seconds(attempt):
    """1-based restart count. 0.5s, then doubles, capped at 30s."""
    if attempt < 1:
        return 0.5
    return min(30.0, 0.5 * (2 ** min(attempt - 1, 6)))


# The gate reports this interval. Do not confuse it with the two production
# signals that disagreed at 16:26 SGT on 2026-10-19 (business-gap log 7.7ms,
# ws_continuity.merged_gap_ms 0.695s). Those formulas are unchanged.
FAILOVER_GAP_DEFINITION = (
    'failover_gaps_s is seconds from mark_kick, recorded before the 1013 '
    'on_close, until that group forwards the next merged event with a higher '
    'outbox sequence. It is not ws_continuity.merged_gap_ms and not the '
    'hot-primary-failure business_gap flag.'
)
PRIMARY_QUEUE_AGE_DEFINITION = (
    'pending_wait_seconds of ReceiveQueue (oldest frame queued or in parse), '
    'sampled every 50ms for the whole window on each group while the socket '
    'stays open, not only at a kick or disconnect. series_1s is the max of '
    'those samples in each whole second after the window start. '
    'primary_queue_wait_ms is the pooled distribution; its p99 is the pass/fail check.'
)


def _send(conn, obj):
    conn.send_bytes(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))


def _recv(conn):
    return pickle.loads(conn.recv_bytes())


def _widen(conn):
    try:
        import fcntl
        fcntl.fcntl(conn.fileno(), 1031, 1 << 20)  # F_SETPIPE_SZ
    except (OSError, AttributeError, ValueError):
        pass


def _finite(value):
    return value if isinstance(value, (int, float)) and math.isfinite(value) else None


def partition(markets, prewarm=None):
    """Hot series each become one feed. Every other market shares the cold feed."""
    hot, cold = {}, []
    hot_pre, cold_pre = {}, []
    for market in markets or []:
        key = balance_key(market)
        if key in HOT_ORDER or (isinstance(key, str) and key.startswith('up_down:') and key.count(':') == 2
                                and key.split(':')[1] in {'BTC', 'ETH'} and key.endswith(('5m', '15m'))):
            hot.setdefault(key, []).append(market)
        else:
            cold.append(market)
    for market in prewarm or []:
        key = balance_key(market)
        if key in hot or key in HOT_ORDER:
            hot_pre.setdefault(key, []).append(market)
        else:
            cold_pre.append(market)
    order = [key for key in HOT_ORDER if key in hot or key in hot_pre]
    for key in sorted(set(hot) | set(hot_pre)):
        if key not in order:
            order.append(key)
    specs = []
    for index, key in enumerate(order):
        specs.append(dict(key=key, hot=True, shard_base=index, shard_count=1,
                          markets=hot.get(key, []), prewarm=hot_pre.get(key, [])))
    if cold or cold_pre:
        specs.append(dict(key='cold', hot=False, shard_base=len(order), shard_count=max(1, min(4, (len(cold) + 3) // 4 or 1)),
                          markets=cold, prewarm=cold_pre))
    return specs


def weighted_counts(total, weights):
    raw = [total * weight for weight in weights]
    counts = [int(value) for value in raw]
    leftover = total - sum(counts)
    for index in sorted(range(len(weights)), key=lambda i: raw[i] - counts[i], reverse=True)[:max(0, leftover)]:
        counts[index] += 1
    return counts


def wire_event(identity, sequence, assets, levels, at):
    """Prod-shaped public book / price_change. `identity` keeps hashes unique."""
    if sequence % 5 == 0:
        return [dict(event_type='book', asset_id=asset, timestamp=at, hash=f'{identity}{asset}',
                     bids=[dict(price=str(level / 1000), size='3') for level in range(400 - levels // 2, 400)],
                     asks=[dict(price=str(level / 1000), size='4') for level in range(600, 600 + levels // 2)])
                for asset in assets]
    return dict(event_type='price_change', timestamp=at, price_changes=[
        dict(asset_id=asset, side='SELL', price=str(level / 1000), size=str(identity), hash=f'{identity}{asset}{level}',
             best_bid='.399', best_ask='.600') for asset in assets for level in range(600, 604)])


def even_plan(seconds, rate, groups):
    """Same burst clock as the in-process harness, including the 300 ms backup skew."""
    total = int(seconds * rate / 2)
    primary, backup = {}, {}
    for burst in range(int(seconds / 0.05) + 7):
        start = min(total, round(burst * 0.05 * rate / 2))
        end = min(total, round((burst + 1) * 0.05 * rate / 2))
        for index in range(start, end):
            primary.setdefault(index, burst * 0.05)
            backup.setdefault(index, (burst + 6) * 0.05)
    return total, primary, backup


def planned_events(spec):
    """Yield (offset_seconds, role, event) for one feed. Shared by child and parent."""
    seconds, rate, levels = spec['seconds'], spec['rate'], spec['levels']
    epoch, assets = spec['epoch'], tuple(spec['assets'])
    if spec.get('mode') == 'weighted':
        weights = spec['weights']
        counts = weighted_counts(int(seconds * rate / 2), weights)
        count = counts[spec['group']]
        for seq in range(count):
            at = epoch + seq
            event = wire_event(seq, seq, assets, levels, at)
            primary_t = math.floor((seq / max(count, 1)) * seconds / 0.05) * 0.05
            yield primary_t, 'primary', event
            yield primary_t + 0.3, 'backup', event
        return
    groups = spec['groups']
    total, primary, backup = even_plan(seconds, rate, groups)
    group = spec['group']
    for index in range(total):
        if index % groups != group:
            continue
        event = wire_event(index, index // groups, assets, levels, epoch + round(index * 2000 / rate))
        yield primary.get(index, 0), 'primary', event
        yield backup.get(index, 0.3), 'backup', event


def _as_int_key(key):
    text = str(key)
    return int(text) if text.isdigit() else key


def _remap_shard_dict(value, base):
    merged = {}
    for key, row in (value or {}).items():
        shard = _as_int_key(key)
        merged[shard + base if isinstance(shard, int) else shard] = row
    return merged


def _sum_numbers(left, right):
    if isinstance(left, bool) or isinstance(right, bool):
        return left
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left + right
    if isinstance(left, dict) and isinstance(right, dict):
        keys = set(left) | set(right)
        return {key: _sum_numbers(left.get(key, 0), right.get(key, 0)) if isinstance(left.get(key, right.get(key)), (int, float, dict))
                else left.get(key, right.get(key)) for key in keys}
    return left if left not in (None, 0) else right


def merge_feed_status(parts):
    """Combine per-feed STATUS documents. Parent-owned lifecycle keys are omitted.

    Shard-keyed maps are shifted by each feed's shard_base. Counters are sums.
    Gap percentiles keep the worst child (an envelope, not a pooled percentile).
    """
    present = [part for part in parts if part.get('status')]
    if not present:
        return {}
    merged = {}
    for part in present:
        base = int(part.get('shard_base') or 0)
        status = part['status']
        for key, value in status.items():
            if key in PARENT_OWNED:
                continue
            if key in SHARD_DICTS and isinstance(value, dict):
                bucket = merged.setdefault(key, {})
                bucket.update(_remap_shard_dict(value, base))
            elif key in SHARD_LISTS and isinstance(value, list):
                rows = merged.setdefault(key, [])
                for row in value:
                    if isinstance(row, dict) and 'shard' in row and isinstance(row.get('shard'), int):
                        row = dict(row, shard=row['shard'] + base)
                    rows.append(row)
            elif key in MERGE_DICTS and isinstance(value, dict):
                bucket = merged.setdefault(key, {})
                for sub, item in value.items():
                    if isinstance(item, (int, float)) and not isinstance(item, bool):
                        bucket[sub] = bucket.get(sub, 0) + item
                    elif isinstance(item, dict) and isinstance(bucket.get(sub), dict):
                        bucket[sub] = _sum_numbers(bucket[sub], item)
                    else:
                        bucket[sub] = item
            elif key in SUM_KEYS and isinstance(value, (int, float)) and not isinstance(value, bool):
                merged[key] = merged.get(key, 0) + value
            elif key in MAX_KEYS and isinstance(value, (int, float)) and not isinstance(value, bool):
                merged[key] = max(merged.get(key, value), value)
            elif key == 'connected':
                merged[key] = bool(value) and merged.get(key, True)
            elif key == 'ws_shards' and isinstance(value, int):
                merged[key] = merged.get(key, 0) + value
            elif key == 'ws_connections_by_source' and isinstance(value, list):
                rows = merged.setdefault(key, [])
                for row in value:
                    row = dict(row)
                    if isinstance(row.get('shard'), int):
                        row['shard'] = row['shard'] + base
                    rows.append(row)
            elif key == 'ws_rotation_pending':
                if value is not None:
                    merged[key] = value + base if isinstance(value, int) else value
                else:
                    merged.setdefault(key, None)
            elif key in ('ws_parsed_frames', 'ws_aux_memory', 'ws_rest_recovery', 'ws_continuity',
                         'ws_double_kicks', 'ws_migration', 'ws_capacity_excess'):
                merged[key] = _merge_special(key, merged.get(key), value)
            elif key == 'ws_hot_groups' and isinstance(value, list):
                merged[key] = sorted(set(merged.get(key, [])) | set(value))
            elif key == 'last_error' or key in ('last_close_reason', 'last_close_code'):
                if value is not None:
                    merged[key] = value
            elif isinstance(value, dict):
                bucket = merged.setdefault(key, {})
                if isinstance(bucket, dict):
                    bucket.update(value)
                else:
                    merged[key] = value
            else:
                merged.setdefault(key, value)
    loads = merged.get('ws_shard_loads') or []
    if loads:
        finite = [value for value in loads if isinstance(value, (int, float))]
        if finite:
            mean = sum(finite) / len(finite)
            merged['ws_load_imbalance'] = (max(finite) - min(finite)) / mean if mean else 0
            merged['ws_load_ratio'] = ratio(finite)
            merged['ws_peak_lower_bound'] = max(mean, max(finite))
    return merged


def _merge_special(key, previous, value):
    if previous is None:
        return copy.deepcopy(value)
    if key == 'ws_capacity_excess':
        left = list(previous) if isinstance(previous, (list, tuple)) else [previous]
        right = list(value) if isinstance(value, (list, tuple)) else [value]
        width = max(len(left), len(right))
        total = [(left[i] if i < len(left) else 0) + (right[i] if i < len(right) else 0) for i in range(width)]
        return tuple(total)
    if key == 'ws_migration':
        chosen = dict(value if value.get('pending') else previous)
        chosen['failures'] = (previous.get('failures') or 0) + (value.get('failures') or 0)
        chosen['pending'] = bool(previous.get('pending') or value.get('pending'))
        return chosen
    if key == 'ws_double_kicks':
        return _merge_kicks(previous, value)
    if key == 'ws_continuity':
        return _merge_continuity(previous, value)
    if key == 'ws_rest_recovery':
        return _merge_recovery(previous, value)
    if isinstance(previous, dict) and isinstance(value, dict):
        return _sum_numbers(previous, value)
    return previous


def _merge_kicks(left, right):
    events = list(left.get('events') or []) + list(right.get('events') or [])
    hourly = {}
    for row in list(left.get('hourly') or []) + list(right.get('hourly') or []):
        hour = row.get('hour_start_utc')
        hourly[hour] = hourly.get(hour, 0) + row.get('count', 0)
    return dict(left, total=(left.get('total') or 0) + (right.get('total') or 0),
                count_current_hour=(left.get('count_current_hour') or 0) + (right.get('count_current_hour') or 0),
                count_rolling_hour=(left.get('count_rolling_hour') or 0) + (right.get('count_rolling_hour') or 0),
                business_gaps_rolling_hour=(left.get('business_gaps_rolling_hour') or 0) + (right.get('business_gaps_rolling_hour') or 0),
                business_gaps_current_hour=(left.get('business_gaps_current_hour') or 0) + (right.get('business_gaps_current_hour') or 0),
                pending_rolling_hour=(left.get('pending_rolling_hour') or 0) + (right.get('pending_rolling_hour') or 0),
                hourly=[dict(hour_start_utc=hour, count=count) for hour, count in hourly.items()],
                events=events, rolling_hour_count_capped=bool(left.get('rolling_hour_count_capped') or right.get('rolling_hour_count_capped')))


def _merge_continuity(left, right):
    merged = dict(left)
    for key in ('checks', 'unverified', 'snapshot_restored', 'history_assets', 'history_entries', 'history_evictions'):
        merged[key] = (left.get(key) or 0) + (right.get(key) or 0)
    gaps = [row.get('merged_gap_ms') or {} for row in (left, right)]
    merged['merged_gap_ms'] = dict(
        samples=sum((row.get('samples') or 0) for row in gaps),
        p50=max((row.get('p50') for row in gaps if row.get('p50') is not None), default=None),
        p95=max((row.get('p95') for row in gaps if row.get('p95') is not None), default=None),
        max=max((row.get('max') for row in gaps if row.get('max') is not None), default=None))
    published = dict(left.get('published') or {})
    published.update(right.get('published') or {})
    merged['published'] = published
    merged['events'] = list(left.get('events') or []) + list(right.get('events') or [])
    merged['pending_assets'] = sorted(set(left.get('pending_assets') or []) | set(right.get('pending_assets') or []))
    return merged


def _merge_recovery(left, right):
    merged = copy.deepcopy(left)
    for key in ('requests', 'completed', 'failures', 'inflight', 'throttled', 'warmups', 'pressure_requests', 'hour_buckets'):
        merged[key] = (left.get(key) or 0) + (right.get(key) or 0)
    categories = merged.setdefault('categories', {})
    for name, row in (right.get('categories') or {}).items():
        categories[name] = _sum_numbers(categories.get(name, {}), row)
    latency = dict(left.get('latency_ms') or {})
    other = right.get('latency_ms') or {}
    latency['samples'] = (latency.get('samples') or 0) + (other.get('samples') or 0)
    latency['max'] = max(latency.get('max') or 0, other.get('max') or 0)
    latency['p95'] = max((value for value in (latency.get('p95'), other.get('p95')) if value is not None), default=None)
    merged['latency_ms'] = latency
    return merged


class ReplaySocket:
    """In-process stand-in for websocket-client. The child emits frames the parent schedules."""

    def __init__(self, url, **callbacks):
        self.callbacks = callbacks
        self.assets = []
        self.stopped = threading.Event()
        self.keep_running = True
        self.url = url
        self.sock = None
        from .ws_transport import TransportDiagnostics
        self.transport = TransportDiagnostics()

    def run_forever(self, **kwargs):
        self.callbacks['on_open'](self)
        self.stopped.wait()

    def send(self, raw):
        if raw == 'PING':
            self.transport.pong()
            return
        message = json.loads(raw)
        if message.get('operation') == 'unsubscribe':
            drop = set(message.get('assets_ids') or [])
            self.assets = [asset for asset in self.assets if asset not in drop]
            return
        if 'assets_ids' in message:
            if message.get('operation') == 'subscribe':
                self.assets = list(dict.fromkeys([*self.assets, *message['assets_ids']]))
            else:
                self.assets = list(message['assets_ids'])

    def close(self):
        self.keep_running = False
        self.stopped.set()

    def emit(self, raw):
        self.transport.read(len(raw) if isinstance(raw, str) else len(raw))
        self.callbacks['on_message'](self, raw)

    def kick(self, code, reason):
        self.keep_running = False
        try:
            self.callbacks['on_close'](self, code, reason)
        finally:
            self.stopped.set()


def install_replay_transport():
    import sys
    import types
    module = types.ModuleType('websocket')
    module.WebSocketApp = ReplaySocket
    # Hide the real package so read_ahead_app cannot subclass a live client.
    sys.modules['websocket'] = module


class _Outbox:
    def __init__(self, conn):
        self.conn = conn
        self.cv = threading.Condition()
        self.urgent = []
        self.pubs = []
        self.seq = 0
        self.kick_seq = None
        self.kick_at = None
        self.closed = False
        self.high = 0
        self.dropped = 0
        self.batches = 0
        self.pickle_s = 0.0
        self.on_drop = None

    def submit(self, event, owned, received_at):
        dropped_assets = None
        with self.cv:
            self.seq += 1
            seq = self.seq
            gap = None
            if self.kick_seq is not None and seq > self.kick_seq:
                gap = time.monotonic() - self.kick_at
                self.kick_seq = None
            self.pubs.append((event, None if owned is None else list(owned),
                              int(received_at.timestamp() * 1000) if received_at else 0, seq))
            self.high = max(self.high, len(self.pubs))
            if len(self.pubs) > 8000:
                drop = len(self.pubs) - 4000
                lost = self.pubs[:drop]
                del self.pubs[:drop]
                self.dropped += drop
                assets = sorted({asset for _event, owned_ids, _stamp, _seq in lost if owned_ids for asset in owned_ids})
                self.urgent.append(dict(op='dropped', n=drop, total=self.dropped, assets=assets))
                dropped_assets = assets
            if gap is not None:
                self.urgent.append(dict(op='gap', seconds=gap, seq=seq))
            if len(self.pubs) >= 32 or dropped_assets:
                self.cv.notify()
        if dropped_assets and self.on_drop:
            try:
                self.on_drop(dropped_assets)
            except Exception:
                _LOG.exception('drop resync failed')

    def mark_kick(self):
        with self.cv:
            self.kick_at = time.monotonic()
            self.kick_seq = self.seq
            self.cv.notify()

    def add(self, message):
        with self.cv:
            self.urgent.append(message)
            self.cv.notify()

    def close(self):
        with self.cv:
            self.closed = True
            self.cv.notify()

    def run(self):
        while True:
            with self.cv:
                if not self.urgent and not self.pubs and not self.closed:
                    self.cv.wait(0.008)
                urgent, self.urgent = self.urgent, []
                pubs, self.pubs = self.pubs, []
                closed = self.closed
            try:
                for message in urgent:
                    started = time.process_time()
                    _send(self.conn, message)
                    self.pickle_s += time.process_time() - started
                if pubs:
                    started = time.process_time()
                    items = [dict(e=event, o=owned, t=stamp, seq=seq) for event, owned, stamp, seq in pubs]
                    _send(self.conn, dict(op='pub', items=items))
                    self.pickle_s += time.process_time() - started
                    self.batches += 1
                if closed and not pubs:
                    return
            except (EOFError, BrokenPipeError, OSError):
                return


def _arm_death_signal():
    """Die with the parent. Docker stop signals PID 1; a killed parent must not leave feed processes."""
    try:
        import ctypes
        import ctypes.util
        library = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
        library.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong]
        library.prctl.restype = ctypes.c_int
        if library.prctl(1, signal.SIGTERM) != 0:  # PR_SET_PDEATHSIG
            return
    except (OSError, AttributeError):
        return
    if os.getppid() == 1:
        os._exit(0)


def without_stale_books(event, metadata):
    """Drop full books older than the parent watermark. Price changes always stay.

    Failover can hand the parent a restored snapshot that the child already
    superseded. Applying it would rewind the published book. The check lives
    only on this parent path so the in-process engine is unchanged.
    """
    from .recording import exchange_time
    if isinstance(event, list):
        return [item for item in (without_stale_books(part, metadata) for part in event) if item]
    if not isinstance(event, dict):
        return event
    kind = event.get('event_type') or event.get('type')
    if kind != 'book':
        return event
    payload = event.get('payload') or event
    asset = str(payload.get('asset_id') or payload.get('assetId') or '')
    at = exchange_time(payload.get('timestamp'))
    previous = metadata.get(asset, (None,))[0] if asset else None
    if previous is not None and at is not None and at < previous:
        return None
    return event


def _resync_dropped(manager, assets):
    """Ask the live primary for a fresh book after an IPC drop."""
    wanted = set(assets or [])
    if not wanted:
        return
    for index, state in list(manager.states.items()):
        owned = [asset for asset in state.get('assets', []) if asset in wanted]
        if owned and not state.get('redundant'):
            try:
                manager.request_snapshot(index, state, owned)
            except Exception:
                _LOG.exception('snapshot after drop failed')


def _socket_for(manager, role):
    if role == 'primary':
        state = next(iter(manager.states.values()), None)
    else:
        items = manager.backup_items()
        state = items[0][1] if items else None
    return state.get('socket') if state else None


def _bootstrap(manager):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and not _STOP_CHILD.is_set():
        manager.backup_next_connect_at = 0
        for state in list(manager.states.values()) + [row for _, row in manager.backup_items()]:
            sock = state.get('socket')
            assets = list(getattr(sock, 'assets', None) or [])
            if not sock or not assets or not hasattr(sock, 'emit'):
                continue
            at = int(time.time() * 1000)
            event = [dict(event_type='book', asset_id=asset, timestamp=at, hash=f'boot{asset}',
                          bids=[dict(price='.4', size='1')], asks=[dict(price='.6', size='1')]) for asset in assets]
            sock.emit(json.dumps(event, separators=(',', ':')))
        try:
            manager.tick(status=False)
        except Exception:
            _LOG.exception('feed bootstrap tick failed')
        if manager.backups and all(manager.backup_ready(row) for _, row in manager.backup_items()):
            return True
        time.sleep(0.01)
    return False


def _schedule(spec):
    """Small (offset, role, wire_event args) rows. Books are built at emit time."""
    rows = []
    if spec.get('mode') == 'weighted':
        counts = weighted_counts(int(spec['seconds'] * spec['rate'] / 2), spec['weights'])
        count = counts[spec['group']]
        for seq in range(count):
            args = (seq, seq, spec['assets'], spec['levels'], spec['epoch'] + seq)
            primary_t = math.floor((seq / max(count, 1)) * spec['seconds'] / 0.05) * 0.05
            rows.append((primary_t, 'primary', args))
            rows.append((primary_t + 0.3, 'backup', args))
    else:
        groups, group = spec['groups'], spec['group']
        total, primary, backup = even_plan(spec['seconds'], spec['rate'], groups)
        for index in range(total):
            if index % groups != group:
                continue
            args = (index, index // groups, spec['assets'], spec['levels'],
                    spec['epoch'] + round(index * 2000 / spec['rate']))
            rows.append((primary.get(index, 0), 'primary', args))
            rows.append((backup.get(index, 0), 'backup', args))
    rows.sort(key=lambda row: (row[0], row[1] != 'primary'))
    return rows


def _drive(manager, spec):
    start = spec['start_mono']
    for offset, role, args in _schedule(spec):
        delay = start + offset - time.monotonic()
        if delay > 0 and _STOP_CHILD.wait(delay):
            return
        sock = _socket_for(manager, role)
        if sock is None or not hasattr(sock, 'emit'):
            continue
        stopped = getattr(sock, 'stopped', None)
        if stopped is not None and stopped.is_set():
            continue
        try:
            sock.emit(json.dumps(wire_event(*args), separators=(',', ':')))
        except Exception:
            _LOG.exception('replay emit failed role=%s', role)


def queue_age_series(samples, t0):
    """Max primary queue age and depth in each whole second at or after t0."""
    buckets = {}
    for row in samples or []:
        if row.get('role') != 'primary':
            continue
        stamp = row.get('t')
        wait = row.get('wait')
        if not isinstance(stamp, (int, float)) or not isinstance(wait, (int, float)):
            continue
        if stamp < t0:
            continue
        second = int(stamp - t0)
        bucket = buckets.setdefault(second, dict(wait=0.0, depth=0, n=0))
        bucket['wait'] = max(bucket['wait'], float(wait))
        depth = row.get('depth')
        if isinstance(depth, (int, float)):
            bucket['depth'] = max(bucket['depth'], int(depth))
        bucket['n'] += 1
    return [dict(second=second, queue_age_ms=round(buckets[second]['wait'] * 1000, 3),
                 depth_max=buckets[second]['depth'], samples=buckets[second]['n'])
            for second in sorted(buckets)]


def _sample_loop(manager, outbox):
    while not _STOP_CHILD.wait(0.05):
        rows = []
        try:
            now = time.time()
            for state in list(manager.states.values()):
                inbox = state.get('inbox')
                if inbox:
                    rows.append(dict(role='primary', t=now, depth=inbox.queue.qsize(),
                                     wait=inbox.pending_wait_seconds(),
                                     lock=getattr(inbox, 'lock_wait_seconds', 0) or 0, resyncs=inbox.resyncs))
            for _, state in manager.backup_items():
                inbox = state.get('inbox')
                if not inbox:
                    continue
                rows.append(dict(role='backup', t=now, depth=inbox.queue.qsize(),
                                 wait=inbox.pending_wait_seconds(),
                                 life=getattr(inbox, 'lifecycle_wait_seconds', 0) or 0,
                                 ready=bool(manager.backup_ready(state)), resyncs=inbox.resyncs))
        except Exception:
            continue
        if rows:
            outbox.add(dict(op='sample', rows=rows))


def _control_loop(manager, outbox, control, replay):
    drive_thread = None
    while not _STOP_CHILD.is_set():
        try:
            if not control.poll(0.05):
                continue
            message = _recv(control)
        except (EOFError, OSError):
            _STOP_CHILD.set()
            return
        op = message.get('op')
        if op == 'stop':
            _STOP_CHILD.set()
            return
        if op == 'sync':
            try:
                manager.sync(message.get('markets') or [], prewarm=message.get('prewarm') or [])
            except Exception:
                _LOG.exception('feed sync failed')
        elif op == 'rates':
            manager.seed_rates(message.get('rates') or {})
        elif op == 'kick':
            outbox.mark_kick()
            sock = _socket_for(manager, 'primary')
            if sock is not None and hasattr(sock, 'kick'):
                sock.kick(message.get('code', 1013), message.get('reason') or 'slow consumer')
        elif op == 'emit' and replay:
            sock = _socket_for(manager, message.get('role') or 'primary')
            if sock is not None and hasattr(sock, 'emit'):
                sock.emit(message['raw'])
        elif op == 'drive' and replay:
            drive_thread = threading.Thread(target=_drive, args=(manager, message), name='polymarket-replay-drive', daemon=True)
            drive_thread.start()
        elif op == 'status':
            try:
                manager.publish_status()
            except Exception:
                _LOG.exception('feed status failed')
        elif op == 'stages':
            with manager.stage_lock:
                stages = dict(manager.stage_cpu)
            outbox.add(dict(op='stages', token=message.get('token'),
                            stages=stages, pickle_s=outbox.pickle_s, cpu_s=time.process_time()))


def _install_forwarder(outbox):
    from . import observer

    class ForwardingPipeline(observer.Pipeline):
        """Local books for failover. Publication SQL, paper and evaluator stay in the parent."""

        def __init__(self):
            super().__init__()
            self.recording.enabled = False
            self.paper_account = None

        def start(self):
            return None

        def capture(self, market, kind, callback):
            return None

        def set_markets(self, markets):
            with self.lock:
                self.markets = {market['id']: market for market in markets}
                self.asset_markets = {asset: market['id'] for market in markets
                                      for asset in (market['yesAssetId'], market['noAssetId'])}
                self.books = {asset: book for asset, book in self.books.items() if asset in self.asset_markets}
                self.recording.prune(self.asset_markets)
                self.dirty.intersection_update(self.markets)
                self.last_eval = {key: value for key, value in self.last_eval.items() if key in self.markets}

        def receive(self, raw, owned=None, received_at=None, inbox=None, epoch=None):
            before = self.messages
            super().receive(raw, owned, received_at, inbox, epoch)
            if self.messages != before and raw != 'PONG':
                outbox.submit(raw, owned, received_at or datetime.now(timezone.utc))

    pipeline = ForwardingPipeline()
    observer._PIPELINE = pipeline
    return pipeline


def _publish_hook(manager, outbox, key, shard_base):
    from . import observer
    original = manager.publish_status

    def publish_status():
        with manager.lifecycle_lock:
            original()
            with observer._LOCK:
                snap = copy.deepcopy(observer.STATUS)
            with manager.stats.lock:
                daily = copy.deepcopy(manager.stats.days)
        outbox.add(dict(op='status', key=key, shard_base=shard_base, shard_count=manager.count,
                        status=snap, rss=observer.process_rss_bytes(), daily=daily,
                        cpu=snap.get('ws_process_cpu_cores')))

    manager.publish_status = publish_status


def child_main(spec, data_conn, control_conn):
    """Spawn target. Fresh interpreter: no parent threads, locks or mocks."""
    _STOP_CHILD.clear()
    _arm_death_signal()
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, lambda *_: _STOP_CHILD.set())
    _widen(data_conn)
    replay = bool(spec.get('replay'))
    if replay:
        logging.getLogger('app.services.polymarket').setLevel(logging.ERROR)
    try:
        os.environ['POLYMARKET_OBSERVER_WS_SHARDS'] = str(spec.get('shard_count') or 1)
        if replay:
            os.environ['POLYMARKET_OBSERVER_WS_REPLAY'] = '1'
            install_replay_transport()
        from . import observer
        observer.tune_switch_interval()
        observer._STOP = threading.Event()
        outbox = _Outbox(data_conn)
        flusher = threading.Thread(target=outbox.run, name='polymarket-feed-flush', daemon=True)
        flusher.start()
        _install_forwarder(outbox)
        manager = observer.MarketConnections()
        manager.primary_first = primary_first_enabled()
        manager.liveness_backup = True
        manager.backup_serial = int(spec.get('shard_base') or 0) * 10000
        outbox.on_drop = lambda assets, manager=manager: _resync_dropped(manager, assets)
        _publish_hook(manager, outbox, spec['key'], int(spec.get('shard_base') or 0))
        if spec.get('rates'):
            manager.seed_rates(spec['rates'])
        if replay:
            manager.snapshot_recovery.request = lambda *args, **kwargs: False
        manager.sync(spec.get('markets') or [], prewarm=spec.get('prewarm') or [])
        manager.start_supervisor()
        outbox.add(dict(op='hello', pid=os.getpid(), key=spec['key']))
        if replay:
            _bootstrap(manager)
            try:
                manager.publish_status()
            except Exception:
                _LOG.exception('feed status after bootstrap failed')
            threading.Thread(target=_sample_loop, args=(manager, outbox), name='polymarket-feed-sample', daemon=True).start()
        threading.Thread(target=_control_loop, args=(manager, outbox, control_conn, replay),
                         name='polymarket-feed-control', daemon=True).start()
        _STOP_CHILD.wait()
        try:
            manager.close(timeout=3)
        except Exception:
            _LOG.exception('feed close failed')
        with manager.stage_lock:
            stages = dict(manager.stage_cpu)
        outbox.add(dict(op='bye', pid=os.getpid(), cpu_s=time.process_time(), rss=observer.process_rss_bytes(),
                        dropped=outbox.dropped, batches=outbox.batches, stages=stages, pickle_s=outbox.pickle_s))
        outbox.close()
        flusher.join(2)
    except Exception:
        _LOG.exception('feed process failed key=%s', spec.get('key'))
        try:
            import traceback
            _send(data_conn, dict(op='fatal', key=spec.get('key'), error=traceback.format_exc()[-2000:]))
        except Exception:
            pass
        raise
    finally:
        for conn in (data_conn, control_conn):
            try:
                conn.close()
            except Exception:
                pass


class _Feed:
    def __init__(self, feeds, spec):
        self.feeds = feeds
        self.spec = spec
        self.proc = None
        self.data = None
        self.control = None
        self.reader = None
        self.hello = threading.Event()
        self.latest = None
        self.fatal = None
        self.samples = []
        self.cpu_s = None
        self.rss = 0
        self.dropped = 0
        self.batches = 0
        self.stale_skipped = 0
        self.stages = {}
        self.pickle_s = 0.0
        self.apply_s = 0.0
        self.dead = False

    def send(self, message):
        conn = self.control
        if conn is None or self.dead:
            return
        try:
            _send(conn, message)
        except (EOFError, BrokenPipeError, OSError):
            self.dead = True

    def read_loop(self):
        conn = self.data
        while not self.feeds.stopped and not self.dead:
            try:
                if not conn.poll(0.05):
                    continue
                message = _recv(conn)
            except (EOFError, OSError):
                self.dead = True
                return
            self._handle(message)

    def _handle(self, message):
        op = message.get('op')
        if op == 'hello':
            self.hello.set()
        elif op == 'fatal':
            self.fatal = message.get('error')
            _LOG.error('feed %s failed: %s', self.spec['key'], self.fatal)
            self.dead = True
        elif op == 'status':
            self.latest = message
            self.rss = message.get('rss') or 0
        elif op == 'sample':
            if len(self.samples) < 200000:
                self.samples.extend(message.get('rows') or [])
        elif op == 'stages' and message.get('token') == getattr(self, 'stage_token', None):
            self.stage_latest = dict(stages=message.get('stages') or {},
                                     pickle_s=message.get('pickle_s') or 0.0,
                                     cpu_s=message.get('cpu_s') or 0.0)
            ready = getattr(self, 'stage_ready', None)
            if ready is not None:
                ready.set()
        elif op == 'gap':
            self.feeds.gaps.append(dict(key=self.spec['key'], seconds=message.get('seconds'), seq=message.get('seq')))
        elif op == 'dropped':
            self.dropped = message.get('total') or self.dropped
            self.feeds.note_drop(self.spec['key'], message.get('assets') or [], self.dropped)
        elif op == 'pub':
            self._apply(message.get('items') or [])
        elif op == 'bye':
            self.cpu_s = message.get('cpu_s')
            self.rss = message.get('rss') or self.rss
            self.dropped = message.get('dropped') or 0
            self.batches = message.get('batches') or self.batches
            self.stages = message.get('stages') or {}
            self.pickle_s = message.get('pickle_s') or 0.0
            self.dead = True

    def _apply(self, items):
        from . import observer
        from .ws_payloads import hold_until_snapshot
        pipeline = observer._PIPELINE
        if pipeline is None:
            return
        for item in items:
            with pipeline.lock:
                event = without_stale_books(item.get('e'), pipeline.recording.metadata)
                event = hold_until_snapshot(event, self.feeds.resync_assets) if event else None
            if not event:
                self.stale_skipped += 1
                continue
            received = datetime.fromtimestamp((item.get('t') or 0) / 1000, timezone.utc)
            owned = None if item.get('o') is None else frozenset(item['o'])
            try:
                started = time.process_time()
                pipeline.receive(event, owned, received)
                self.apply_s += time.process_time() - started
            except Exception:
                _LOG.exception('parent apply failed key=%s', self.spec['key'])
            self.feeds.published += 1

    def stop(self, timeout):
        self.send(dict(op='stop'))
        proc = self.proc
        if proc is not None and proc.is_alive():
            proc.join(timeout)
        if proc is not None and proc.is_alive():
            proc.terminate()
            proc.join(1)
        if proc is not None and proc.is_alive():
            proc.kill()
            proc.join(1)
        self.dead = True
        for conn in (self.data, self.control):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


class GroupFeeds:
    """Parent façade with the MarketConnections methods the market loop calls."""

    def __init__(self, replay=False):
        import multiprocessing
        self.ctx = multiprocessing.get_context('spawn')
        self.replay = replay or os.getenv('POLYMARKET_OBSERVER_WS_REPLAY', '0').lower() in {'1', 'true', 'yes'}
        self.children = {}
        self.order = []
        self.lock = threading.Lock()
        self.stopped = False
        self.states = {}
        self.restarts = {}
        self.rates = {}
        self.gaps = []
        self.published = 0
        self.restart_after = {}
        self.restart_alert = {}
        self.drop_totals = {}
        self.resync_assets = set()
        self.watchdog = None
        self._cpu_at = time.monotonic()
        self._cpu = time.process_time()
        self._parent_cpu = 0

    def seed_rates(self, rates):
        self.rates = dict(rates or {})
        for child in list(self.children.values()):
            child.spec['rates'] = self.rates
            child.send(dict(op='rates', rates=self.rates))

    def start_supervisor(self):
        if self.watchdog is not None:
            return
        self.watchdog = threading.Thread(target=self._watch, name='polymarket-group-watchdog', daemon=True)
        self.watchdog.start()

    def sync(self, markets, prewarm=None):
        specs = partition(markets, prewarm)
        keys = [spec['key'] for spec in specs]
        spawned = []
        with self.lock:
            if self.stopped:
                return
            for key in list(self.children):
                if key not in keys:
                    self.children.pop(key).stop(2)
            self.order = keys
            for spec in specs:
                spec = dict(spec, replay=self.replay, rates=self.rates)
                current = self.children.get(spec['key'])
                if current is not None and not current.dead and current.proc is not None and current.proc.is_alive():
                    current.spec.update(markets=spec['markets'], prewarm=spec['prewarm'], rates=self.rates)
                    current.send(dict(op='sync', markets=spec['markets'], prewarm=spec['prewarm']))
                else:
                    spawned.append(self._spawn_locked(spec))
        self.start_supervisor()
        for child in spawned:
            key = child.spec['key']
            deadline = time.monotonic() + 30
            current = child
            while time.monotonic() < deadline:
                current = self.children.get(key) or current
                if current.hello.wait(0.2):
                    break
            else:
                current = self.children.get(key) or current
                raise RuntimeError(f"feed {key} did not start: {current.fatal or 'timeout'}")

    def _spawn_locked(self, spec):
        parent_data, child_data = self.ctx.Pipe(duplex=False)
        child_control, parent_control = self.ctx.Pipe(duplex=False)
        _widen(parent_data)
        _widen(parent_control)
        proc = self.ctx.Process(target=child_main, name=f"polymarket-feed-{spec['key']}",
                                args=(spec, child_data, child_control), daemon=False)
        proc.start()
        child_data.close()
        child_control.close()
        feed = _Feed(self, spec)
        feed.proc = proc
        feed.data = parent_data
        feed.control = parent_control
        feed.reader = threading.Thread(target=feed.read_loop, name=f"polymarket-feed-read-{spec['key']}", daemon=True)
        feed.reader.start()
        self.children[spec['key']] = feed
        return feed

    def _watch(self):
        while not self.stopped:
            time.sleep(0.5)
            if self.stopped:
                return
            with self.lock:
                if self.stopped:
                    return
                for key, child in list(self.children.items()):
                    if self.stopped:
                        return
                    self._consider_restart(key, child, time.monotonic())

    def _consider_restart(self, key, child, now):
        proc = child.proc
        if proc is not None and proc.is_alive():
            return False
        if now < self.restart_after.get(key, 0):
            return False
        attempt = self.restarts.get(key, 0) + 1
        self.restarts[key] = attempt
        delay = restart_backoff_seconds(attempt)
        self.restart_after[key] = now + delay
        if attempt >= 3:
            self.restart_alert[key] = dict(restarts=attempt, backoff_s=delay)
            _LOG.error('feed %s restart alert restarts=%s backoff_s=%.1f code=%s',
                       key, attempt, delay, None if proc is None else proc.exitcode)
        else:
            _LOG.warning('restarting feed %s after exit code=%s restarts=%s backoff_s=%.1f',
                         key, None if proc is None else proc.exitcode, attempt, delay)
        try:
            child.stop(0.2)
        except Exception:
            _LOG.exception('stopping dead feed %s', key)
        self._spawn_locked(dict(child.spec))
        return True

    def note_drop(self, key, assets, total):
        """Live IPC loss. Price changes for these assets wait for the next book."""
        self.drop_totals[key] = int(total or 0)
        self.resync_assets.update(str(asset) for asset in (assets or []) if asset)

    def kick(self, key, code=1013, reason='slow consumer'):
        child = self.children.get(key)
        if child is not None:
            child.send(dict(op='kick', code=code, reason=reason))

    def drive(self, spec):
        """Start a prod-shaped replay clock in every hot child. `spec` is the shared schedule."""
        start = time.monotonic() + float(spec.get('lead') or 0.5)
        hot = [self.children[key] for key in self.order if key in self.children and self.children[key].spec.get('hot')]
        self.gaps.clear()
        for child in hot:
            child.samples.clear()
        weights = spec.get('weights')
        for index, child in enumerate(hot):
            markets = child.spec.get('markets') or []
            assets = [asset for market in markets for asset in (market['yesAssetId'], market['noAssetId'])]
            message = dict(spec, op='drive', group=index, groups=len(hot), assets=assets, start_mono=start,
                           weights=weights, key=child.spec['key'])
            child.send(message)
        return start

    def emit(self, key, role, raw):
        child = self.children.get(key)
        if child is not None:
            child.send(dict(op='emit', role=role, raw=raw))

    def hot_ready(self):
        ready = False
        for key in self.order:
            child = self.children.get(key)
            if child is None or not child.spec.get('hot'):
                continue
            ready = True
            latest = child.latest or {}
            backups = (latest.get('status') or {}).get('ws_backups') or {}
            if not backups or not all(row.get('ready') for row in backups.values()):
                return False
        return ready

    def wait_ready(self, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.hot_ready():
                return True
            fatal = {key: child.fatal for key, child in self.children.items() if child.fatal}
            if fatal:
                raise RuntimeError(f'feed failed during bootstrap: {fatal}')
            time.sleep(0.05)
        detail = {key: ((child.latest or {}).get('status') or {}).get('ws_backups')
                  for key, child in self.children.items()}
        raise TimeoutError(f'backups not ready: {detail}')

    def tick(self, status=True):
        if status:
            self.publish_status()

    def publish_status(self):
        from . import observer
        now = time.monotonic()
        parent_cpu = (time.process_time() - self._cpu) / (now - self._cpu_at) if now > self._cpu_at else 0
        self._cpu_at, self._cpu = now, time.process_time()
        self._parent_cpu = parent_cpu
        parts = []
        base = 0
        group_cpu = {}
        with self.lock:
            order = list(self.order)
        for key in order:
            child = self.children.get(key)
            if child is None:
                continue
            count = int((child.latest or {}).get('shard_count') or child.spec.get('shard_count') or 1)
            if child.latest:
                part = dict(child.latest)
                part['shard_base'] = base
                parts.append(part)
                group_cpu[key] = _finite((child.latest.get('status') or {}).get('ws_process_cpu_cores')) or 0
            base += count
        merged = merge_feed_status(parts)
        child_cpu = sum(group_cpu.values())
        merged['ws_process_cpu_cores'] = child_cpu + parent_cpu
        merged['ws_parent_cpu_cores'] = parent_cpu
        merged['ws_group_process_cpu_cores'] = group_cpu
        merged['ws_feed_restarts'] = dict(self.restarts)
        merged['ws_feed_restart_alert'] = {key: dict(row) for key, row in self.restart_alert.items()}
        merged['ws_ipc_dropped'] = sum(self.drop_totals.values())
        merged['ws_ipc_resync_assets'] = sorted(self.resync_assets)
        now = time.monotonic()
        merged['ws_group_processes'] = {
            key: dict(pid=None if child.proc is None else child.proc.pid,
                      alive=bool(child.proc and child.proc.is_alive()),
                      restarts=self.restarts.get(key, 0),
                      backoff_s=max(0.0, self.restart_after.get(key, 0) - now),
                      restart_alert=key in self.restart_alert,
                      hot=bool(child.spec.get('hot')),
                      dropped=child.dropped, batches=child.batches)
            for key, child in self.children.items()}
        merged['ws_shards'] = base
        # Socket open sets mode=ws inside the child. Keep the market loop's
        # rest/disabled/shutdown modes; otherwise a live feed is ws.
        clear_error = False
        if any((part.get('status') or {}).get('mode') == 'ws' for part in parts):
            with observer._LOCK:
                parent_mode = observer.STATUS.get('mode')
            if parent_mode not in {'rest', 'disabled', 'stopping', 'stopped'}:
                merged['mode'] = 'ws'
                # _ws_connected clears last_error when a socket opens. A child
                # that only has the default None must not leave the key absent.
                clear_error = not any((part.get('status') or {}).get('last_error') for part in parts)
        merged['ws_observer_database'] = observer.database_metrics()
        merged['ws_metrics_session_id'] = observer._DAILY_STATS.session
        merged['ws_metrics_scope'] = 'observer_process_session'
        self._absorb_daily(parts)
        with observer._LOCK:
            for key, value in merged.items():
                if key in ('last_error', 'last_close_reason', 'last_close_code') and value is None:
                    continue
                observer.STATUS[key] = value
            if clear_error:
                observer.STATUS['last_error'] = None

    def _absorb_daily(self, parts):
        from . import observer
        totals = {}
        for part in parts:
            base = int(part.get('shard_base') or 0)
            for day, row in (part.get('daily') or {}).items():
                dest = totals.setdefault(day, dict(reconnect_count=0, slow_consumer_count=0,
                                                   rebalance_count=0, shard_msg_totals={}))
                for name in ('reconnect_count', 'slow_consumer_count', 'rebalance_count'):
                    dest[name] += int(row.get(name) or 0)
                for shard, count in (row.get('shard_msg_totals') or {}).items():
                    try:
                        mapped = str(int(shard) + base)
                    except (TypeError, ValueError):
                        mapped = str(shard)
                    dest['shard_msg_totals'][mapped] = dest['shard_msg_totals'].get(mapped, 0) + int(count)
        stats = observer._DAILY_STATS
        with stats.lock:
            stats.days = totals
            stats.dirty = set(totals)

    def child_rss_bytes(self):
        return sum((child.rss or 0) for child in self.children.values())

    def alive_workers(self):
        return any(child.proc is not None and child.proc.is_alive() for child in self.children.values())

    def close(self, timeout=5):
        with self.lock:
            if self.stopped and not self.children:
                return
            self.stopped = True
            children = list(self.children.values())
            self.children.clear()
        deadline = time.monotonic() + timeout
        for child in children:
            child.stop(max(0.2, deadline - time.monotonic()))
        if self.watchdog is not None and self.watchdog.is_alive() and threading.current_thread() is not self.watchdog:
            self.watchdog.join(1)
        self.watchdog = None

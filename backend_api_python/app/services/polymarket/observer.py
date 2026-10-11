"""Scheduler-owned public market observer; it never places or signs orders."""
import json
import os
import threading
import time
from contextlib import contextmanager
from collections import deque
from itertools import groupby
from zlib import crc32
from threading import RLock
from datetime import datetime, timezone

from app.utils.db import get_db_connection
from .client import PublicClient
from .engine import number, sweep
from . import updown

STATUS = {"enabled": False, "running": False, "mode": "stopped", "connected": False, "markets_subscribed": 0,
          "last_message_at": None, "reconnect_count": 0, "ws_connections": 0, "slow_consumer_count": 0, "last_error": None, "started_at": None}
_LOCK = threading.RLock()
_STOP = threading.Event()
_THREAD = None
_LAST_SAMPLE = {}
_LAST_RESEARCH = {}
_REFERENCE = None
_PIPELINE = None
_WS = None


def enabled():
    return os.getenv("POLYMARKET_OBSERVER_ENABLED", "false").lower() in {"1", "true", "yes"}


def snapshot():
    with _LOCK:
        result = dict(STATUS)
        if "close_reason_counts" in result:
            result["close_reason_counts"] = dict(result["close_reason_counts"])
    if _PIPELINE:
        metrics = _PIPELINE.metrics()
        if metrics["last_message_at"] is None:
            metrics.pop("last_message_at")
        result.update(metrics)
    return result


def _ws_connected():
    with _LOCK:
        STATUS.update(mode="ws", connected=True, last_error=None)


def _ws_error(error):
    if 'slow consumer' in str(error).lower() or 'slow_consumer' in str(error).lower():
        close_reason(None, error)
    with _LOCK:
        STATUS.update(connected=False, last_error=str(error).splitlines()[0][:160])


def _ws_disconnected():
    with _LOCK:
        STATUS.update(connected=False)


def ws_apply_event(event, books):
    """Apply full book snapshots and price-change deltas; return changed asset ids."""
    if isinstance(event, list):
        changed = []
        for item in event:
            changed.extend(ws_apply_event(item, books))
        return changed
    if not isinstance(event, dict):
        return []
    kind = event.get("event_type") or event.get("type")
    if kind == "book":
        event = event.get("payload") or event
        asset = str(event.get("asset_id") or event.get("assetId") or "")
        if asset:
            books[asset] = {"bids": {str(x["price"]): str(x["size"]) for x in event.get("bids", [])},
                            "asks": {str(x["price"]): str(x["size"]) for x in event.get("asks", [])}}
            return [asset]
    if kind == "price_change":
        changes = event.get("price_changes") or (event.get("payload") or {}).get("priceChanges") or []
        changed = []
        for row in changes:
            asset = str(row.get("asset_id") or row.get("assetId") or "")
            if asset not in books:
                continue
            side = "bids" if str(row.get("side", "")).upper() == "BUY" else "asks"
            price, size = str(row.get("price")), str(row.get("size", 0))
            if float(size) <= 0:
                books[asset][side].pop(price, None)
            else:
                books[asset][side][price] = size
            changed.append(asset)
        return changed
    return []


def _persist_status():
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO qd_polymarket_observer_status (singleton,status,updated_at) VALUES (true,%s::jsonb,NOW()) ON CONFLICT (singleton) DO UPDATE SET status=EXCLUDED.status,updated_at=NOW()", (json.dumps(snapshot()),))
            db.commit()
        finally:
            cur.close()


def get_status():
    try:
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute("SELECT status FROM qd_polymarket_observer_status WHERE singleton=true")
                row = cur.fetchone()
                value = row.get("status") if isinstance(row, dict) else row[0] if row else None
                return value if value else snapshot()
            finally:
                cur.close()
    except Exception:
        return snapshot()


def opportunity(market, books, quantity, settlement_cost=0):
    """Depth-weighted ask cost using the engine's fee and sweep model."""
    if market.get('market_type') == 'up_down':
        if updown.timestamp(market['endDate']) <= datetime.now(timezone.utc):
            return None
        for token in (market['yesAssetId'], market['noAssetId']):
            book = books.get(token) or {}
            asks, bids = book.get('asks', {}), book.get('bids', {})
            if not asks or any(not 0 < number(p) < 1 or number(size) < 0 for side in (asks, bids) for p, size in side.items()):
                return None
            if bids and max(map(number, bids)) >= min(map(number, asks)):
                return None
    qty, rate = number(quantity), number(market["feeRate"])
    legs = []
    for asset in (market["yesAssetId"], market["noAssetId"]):
        raw = books.get(asset)
        if not raw or not raw.get("asks"):
            return None
        levels = sorted(({"price": p, "size": s} for p, s in raw["asks"].items()), key=lambda x: number(x["price"]))
        legs.append(sweep({"asks": levels, "bids": []}, "BUY", qty, rate))
    filled = min(number(x["filled"]) for x in legs)
    if filled <= 0:
        return None
    # Compare equal complete-set quantities even when one token has less depth.
    legs = [sweep({"asks": sorted(({"price": p, "size": s} for p, s in books[asset]["asks"].items()), key=lambda x: number(x["price"])), "bids": []}, "BUY", filled, rate) for asset in (market["yesAssetId"], market["noAssetId"])]
    cost = sum((number(x["notional"]) + number(x["fee"]) for x in legs), number(settlement_cost))
    net = filled - cost
    candidates = {filled}
    for asset in (market["yesAssetId"], market["noAssetId"]):
        cumulative = number(0)
        for price, size in sorted(books[asset]["asks"].items(), key=lambda row: number(row[0])):
            cumulative += number(size)
            if number(0) < cumulative <= filled:
                candidates.add(cumulative)
    positive_size = number(0)
    for candidate in candidates:
        candidate_legs = [sweep({"asks": sorted(({"price": p, "size": s} for p, s in books[asset]["asks"].items()), key=lambda x: number(x["price"])), "bids": []}, "BUY", candidate, rate) for asset in (market["yesAssetId"], market["noAssetId"])]
        candidate_cost = sum((number(x["notional"]) + number(x["fee"]) for x in candidate_legs), number(settlement_cost))
        if candidate - candidate_cost > 0:
            positive_size = max(positive_size, candidate)
    return {"net_edge": net, "net_edge_bps": net / filled * 10000, "size_available": positive_size,
            "cost": cost, "yes_ask": legs[0]["averagePrice"], "no_ask": legs[1]["averagePrice"]}


def sample_tags(market, now):
    kind = market.get('market_type', 'yes_no')
    asset, interval = market.get('asset'), market.get('interval')
    bucket = updown.expiry_bucket(market, now)
    return kind, asset, interval, bucket


def near_miss_key(tags):
    return '|'.join(x or '' for x in tags)


def record_research(market, books, source, quote=None, now=None):
    if market.get('market_type') != 'up_down':
        return
    now = now or datetime.now(timezone.utc)
    if updown.timestamp(market['endDate']) <= now:
        return
    mono = time.monotonic()
    if mono - _LAST_RESEARCH.get(market['id'], float('-inf')) < updown.setting('UPDOWN_SAMPLE_SECONDS', 5, 1, 60):
        return
    prices = []
    for token in (market['yesAssetId'], market['noAssetId']):
        book = books.get(token) or {}
        asks, bids = book.get('asks', {}), book.get('bids', {})
        ask = min((number(p) for p in asks), default=None)
        bid = max((number(p) for p in bids), default=None)
        if ask is None or bid is not None and bid >= ask:
            return
        prices.extend((ask, (ask + bid) / 2 if bid is not None else None))
    provider = _REFERENCE
    reference = provider.get(market['asset']) if provider else {'price': None, 'source': 'unavailable', 'observed_at': None}
    tags = sample_tags(market, now)
    with _observation_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,observed_at,market_type,asset,interval,expiry_bucket,end_time,up_ask,up_mid,down_ask,down_mid,price_to_beat,reference_price,reference_source,reference_observed_at,source,net_edge_bps) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (market['id'], now, *tags, market['endDate'], *prices, market.get('price_to_beat'), reference['price'], reference['source'], reference['observed_at'], source, quote.get('net_edge_bps') if quote else None))
            db.commit()
            _LAST_RESEARCH[market['id']] = mono
        finally:
            cur.close()


def record_sample(market, quote, source, now=None):
    now = now or datetime.now(timezone.utc)
    if market.get('market_type') == 'up_down' and updown.timestamp(market['endDate']) <= now:
        _end_episode(market['id'], now)
        return
    tags = sample_tags(market, now)
    threshold = number(os.getenv("POLYMARKET_OBSERVER_MIN_EDGE", "0"))
    near = number(os.getenv("POLYMARKET_OBSERVER_NEAR_MISS_BPS", "50"))
    net = number(quote["net_edge"])
    kind = "episode" if net > threshold else "near_miss" if net <= 0 and number(quote["net_edge_bps"]) >= -near else None
    if kind != "episode":
        _end_episode(market["id"], now)
    if not kind:
        return
    with _observation_connection() as db:
        cur = db.cursor()
        try:
            if kind == "episode":
                if tags[0] == 'up_down':
                    cur.execute("UPDATE qd_polymarket_observer_episodes SET ended_at=last_seen_at,duration_ms=(EXTRACT(EPOCH FROM (last_seen_at-started_at))*1000)::bigint WHERE market_id=%s AND ended_at IS NULL AND expiry_bucket<>%s", (market['id'], tags[3]))
                cur.execute("INSERT INTO qd_polymarket_observer_episodes(market_id,slug,question,started_at,last_seen_at,max_net_edge,max_net_edge_bps,best_size,sample_count,source,market_type,asset,interval,expiry_bucket) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s) ON CONFLICT (market_id) WHERE ended_at IS NULL DO UPDATE SET last_seen_at=EXCLUDED.last_seen_at,max_net_edge=GREATEST(qd_polymarket_observer_episodes.max_net_edge,EXCLUDED.max_net_edge),max_net_edge_bps=GREATEST(qd_polymarket_observer_episodes.max_net_edge_bps,EXCLUDED.max_net_edge_bps),best_size=GREATEST(qd_polymarket_observer_episodes.best_size,EXCLUDED.best_size),sample_count=qd_polymarket_observer_episodes.sample_count+1,source=EXCLUDED.source,expiry_bucket=EXCLUDED.expiry_bucket", (market["id"], market["slug"], market["question"], now, now, net, quote["net_edge_bps"], quote["size_available"], source, *tags))
            else:
                cur.execute("INSERT INTO qd_polymarket_observer_near_misses(market_id,hour_bucket,slug,question,observed_at,net_edge,net_edge_bps,best_edge_bps,market_type,asset,interval,expiry_bucket,tag_key) VALUES (%s,date_trunc('hour',%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (market_id,hour_bucket,tag_key) DO UPDATE SET samples=qd_polymarket_observer_near_misses.samples+1, best_edge_bps=GREATEST(qd_polymarket_observer_near_misses.best_edge_bps,EXCLUDED.best_edge_bps)", (market["id"], now, market["slug"], market["question"], now, net, quote["net_edge_bps"], quote["net_edge_bps"], *tags, near_miss_key(tags)))
            db.commit()
        finally:
            cur.close()


def _end_episode(market_id, now):
    with _observation_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("UPDATE qd_polymarket_observer_episodes SET ended_at=last_seen_at,duration_ms=(EXTRACT(EPOCH FROM (last_seen_at-started_at))*1000)::bigint WHERE market_id=%s AND ended_at IS NULL", (market_id,))
            db.commit()
        finally:
            cur.close()


def _close_stale_episodes(market_ids, grace_seconds=60):
    """Close episodes unseen beyond the grace period or outside the current set."""
    with _observation_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("UPDATE qd_polymarket_observer_episodes SET ended_at=last_seen_at,duration_ms=(EXTRACT(EPOCH FROM (last_seen_at-started_at))*1000)::bigint WHERE ended_at IS NULL AND (last_seen_at < NOW() - (%s * INTERVAL '1 second') OR NOT (market_id = ANY(%s)))", (grace_seconds, list(market_ids)))
            db.commit()
        finally:
            cur.close()


def _run_retention(now_mono):
    """Run bounded history cleanup no more often than every ten minutes."""
    if now_mono - _RETENTION_LAST[0] < 600:
        return
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("DELETE FROM qd_polymarket_observer_episodes WHERE started_at < NOW() - (%s * INTERVAL '1 day')", (max(1, min(365, int(os.getenv("POLYMARKET_OBSERVER_RETENTION_DAYS", "30")))),))
            cur.execute("DELETE FROM qd_polymarket_observer_near_misses WHERE hour_bucket < NOW() - (%s * INTERVAL '1 day')", (max(1, min(365, int(os.getenv("POLYMARKET_OBSERVER_NEAR_MISS_RETENTION_DAYS", "7")))),))
            if updown.enabled():
                cur.execute("DELETE FROM qd_polymarket_observer_samples WHERE observed_at < NOW() - (%s * INTERVAL '1 day')", (updown.setting('UPDOWN_SAMPLE_RETENTION_DAYS', 7, 1, 365),))
            db.commit()
        finally:
            cur.close()
    _RETENTION_LAST[0] = now_mono


_RETENTION_LAST = [0.0]


def subscription_assets(markets):
    return [a for m in markets for a in (m['yesAssetId'], m['noAssetId'])]


def _market_loop():
    global _REFERENCE, _WS
    client = PublicClient()
    series_cache = {}
    yes_markets = []
    yes_refresh = 0
    if updown.enabled():
        _REFERENCE = updown.ReferencePrices()
        _REFERENCE.start()
    backoff = 5
    ws_failures = 0
    refresh = 0
    markets = []
    connections = None
    heartbeat = 0
    try:
        _close_stale_episodes((), max(1, int(os.getenv("POLYMARKET_OBSERVER_STALE_GRACE_SECONDS", "60"))))
        while not _STOP.is_set():
            try:
                now_mono = time.monotonic()
                _run_retention(now_mono)
                if not markets or now_mono >= refresh:
                    count = min(50, max(1, int(os.getenv("POLYMARKET_OBSERVER_MARKET_COUNT", "20"))))
                    if not updown.enabled() or not yes_markets or now_mono >= yes_refresh:
                        yes_markets, _ = client.discover(limit=count)
                        yes_refresh = now_mono + max(60, int(os.getenv('POLYMARKET_OBSERVER_REFRESH_SECONDS', '1800')))
                    markets = list(yes_markets)
                    if updown.enabled():
                        crypto, rejected = updown.discover(client, series_cache)
                        markets.extend(crypto)
                        with _LOCK:
                            STATUS['updown_subscriptions'] = [{k: m[k] for k in ('id', 'asset', 'interval', 'endDate')} for m in crypto[:20]]
                            STATUS['updown_rejections'] = rejected[:20]
                    if _STOP.is_set():
                        break
                    active_ids = {m['id'] for m in markets}
                    for cache in (_LAST_SAMPLE, _LAST_RESEARCH):
                        for identity in list(cache):
                            if identity not in active_ids:
                                cache.pop(identity, None)
                    if _PIPELINE:
                        _PIPELINE.set_markets(markets)
                    if connections:
                        connections.sync(markets)
                    if _PIPELINE:
                        _PIPELINE.capture('*', 'observation', lambda: _close_stale_episodes(
                            [m['id'] for m in markets], max(1, int(os.getenv('POLYMARKET_OBSERVER_STALE_GRACE_SECONDS', '60')))))
                    else:
                        _close_stale_episodes([m['id'] for m in markets])
                    refresh = time.monotonic() + (updown.setting("UPDOWN_REFRESH_SECONDS", 60, 10, 300) if updown.enabled() else max(60, int(os.getenv("POLYMARKET_OBSERVER_REFRESH_SECONDS", "1800"))))
                    with _LOCK:
                        STATUS["markets_subscribed"] = len(markets)
                if not markets:
                    with _LOCK:
                        STATUS.update(mode="rest", connected=False, last_error="no_supported_markets")
                    _STOP.wait(max(1, int(os.getenv("POLYMARKET_OBSERVER_REST_POLL_SECONDS", "10"))))
                    continue
                use_ws = os.getenv("POLYMARKET_OBSERVER_WS_ENABLED", "true").lower() in {"1", "true", "yes"} and ws_failures < 3
                if use_ws:
                    if connections is None:
                        connections = MarketConnections()
                        _WS = connections
                        connections.sync(markets)
                    connections.tick()
                    ws_failures = connections.failures()
                    if now_mono >= heartbeat:
                        _persist_status()
                        heartbeat = now_mono + 10
                    _STOP.wait(1)
                    continue
                else:
                    if connections:
                        connections.close()
                        connections = None
                        _WS = None
                        if _PIPELINE:
                            with _PIPELINE.evaluation_lock:
                                with _PIPELINE.lock:
                                    _PIPELINE.books.clear()
                                    _PIPELINE.dirty.clear()
                    with _LOCK:
                        STATUS.update(mode="rest", connected=True)
                    frames = client.capture(markets)
                    for market in markets:
                        frame = frames.get(market["id"], {})
                        if frame.get("books") and not frame.get("error"):
                            mapped = {a: {side: {str(x["price"]): str(x["size"]) for x in frame["books"][a][side]} for side in ("asks", "bids")} for a in (market["yesAssetId"], market["noAssetId"])}
                            quote = opportunity(market, mapped, os.getenv("POLYMARKET_OBSERVER_SIZE_SHARES", "100"), os.getenv("POLYMARKET_OBSERVER_MERGE_COST", "0.02"))
                            if _PIPELINE:
                                with _PIPELINE.evaluation_lock:
                                    _PIPELINE.capture(market['id'], 'research', lambda: record_research(market, mapped, 'rest', quote))
                                    if quote:
                                        _PIPELINE.capture(market['id'], 'observation', lambda: record_sample(market, quote, 'rest'))
                            else:
                                record_research(market, mapped, 'rest', quote)
                                if quote:
                                    record_sample(market, quote, 'rest')
                    with _LOCK:
                        STATUS["last_message_at"] = datetime.now(timezone.utc).isoformat()
                    _STOP.wait(max(1, int(os.getenv("POLYMARKET_OBSERVER_REST_POLL_SECONDS", "10"))))
                    if ws_failures >= 3:
                        ws_failures = 0
                _persist_status()
                _run_retention(time.monotonic())
            except Exception as exc:
                if os.getenv("POLYMARKET_OBSERVER_WS_ENABLED", "true").lower() in {"1", "true", "yes"} and STATUS.get("mode") in {"starting", "ws"}:
                    ws_failures += 1
                with _LOCK:
                    STATUS.update(connected=False, last_error=str(exc).splitlines()[0][:160], reconnect_count=STATUS["reconnect_count"] + 1)
                try:
                    _persist_status()
                except Exception:
                    pass
                _STOP.wait(backoff)
                backoff = min(backoff * 2, 120)
                if _PIPELINE:
                    _PIPELINE.capture('*', 'observation', lambda: _close_stale_episodes(
                        [m['id'] for m in markets], max(1, int(os.getenv('POLYMARKET_OBSERVER_STALE_GRACE_SECONDS', '60')))))
                else:
                    _close_stale_episodes([m['id'] for m in markets])
    finally:
        if connections:
            connections.close()
        _WS = None
        if _REFERENCE:
            _REFERENCE.close()
            _REFERENCE = None
        client.close()
        if _PIPELINE:
            _PIPELINE.market_done.set()


def start():
    global _THREAD, _PIPELINE
    if not enabled():
        with _LOCK:
            STATUS.update(enabled=False, running=False, mode="disabled", connected=False)
        try:
            _persist_status()
        except Exception:
            pass
        return False
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return True
        if _PIPELINE and (_PIPELINE.writer.is_alive() or _PIPELINE.evaluator.is_alive()):
            return False
        _STOP.clear()
        _LAST_RESEARCH.clear()
        _LAST_SAMPLE.clear()
        STATUS.update(enabled=True, running=True, mode="starting", connected=False, started_at=datetime.now(timezone.utc).isoformat(), last_error=None)
        _PIPELINE = Pipeline()
        _PIPELINE.market_done.clear()
        _PIPELINE.start()
        _THREAD = threading.Thread(target=_market_loop, name="polymarket-observer", daemon=True)
        _THREAD.start()
    try:
        _persist_status()
    except Exception:
        pass
    return True


def stop(timeout=15):
    global _THREAD
    deadline = time.monotonic() + timeout
    _STOP.set()
    receivers = _WS
    if receivers:
        receivers.close(timeout=max(0, deadline - time.monotonic()))
    with _LOCK:
        thread = _THREAD
    if thread:
        thread.join(max(0, deadline - time.monotonic()))
    if _PIPELINE:
        _PIPELINE.close(max(0, deadline - time.monotonic()))
    with _LOCK:
        pending = bool(thread and thread.is_alive()) or bool(_PIPELINE and (
            _PIPELINE.writer.is_alive() or _PIPELINE.evaluator.is_alive()))
        pending = pending or bool(receivers and any(
            state['thread'] and state['thread'].is_alive() for state in receivers.states.values()))
        STATUS.update(running=pending, connected=False, mode="stopping" if pending else "stopped")
        if pending:
            STATUS['flush_complete'] = False
    try:
        _persist_status()
    except Exception:
        pass


_CAPTURE = threading.local()


@contextmanager
def _observation_connection():
    """Record evaluator SQL without opening a database connection."""
    records = getattr(_CAPTURE, 'records', None)
    if records is None:
        with get_db_connection() as db:
            yield db
        return

    class Recorder:
        def cursor(self):
            return self

        def execute(self, sql, params):
            records.append((sql, params))

        def commit(self):
            pass

        def close(self):
            pass

    yield Recorder()


def subscription_changes(previous, current):
    old, new = set(previous), set(current)
    return [{'assets_ids': sorted(tokens), 'operation': operation}
            for tokens, operation in ((old - new, 'unsubscribe'), (new - old, 'subscribe')) if tokens]


def close_reason(code, reason):
    reason = str(reason or code or 'connection_closed')
    if 'slow consumer' in reason.lower():
        reason = reason[reason.lower().index('slow consumer'):].rstrip("'\"")
    reason = reason[:160]
    with _LOCK:
        STATUS['last_close_reason'] = reason
        category = 'slow_consumer' if 'slow consumer' in reason.lower() or 'slow_consumer' in reason.lower() else 'remote_close'
        reasons = STATUS.setdefault('close_reason_counts', {})
        reasons[category] = reasons.get(category, 0) + 1
        if 'slow consumer' in reason.lower() or 'slow_consumer' in reason.lower():
            STATUS['slow_consumer_count'] = STATUS.get('slow_consumer_count', 0) + 1


class WriteQueue:
    """Bounded FIFO. Only the evaluator can wait; receives never enqueue.

    Research is replaceable. Observation groups containing episode transitions
    are indivisible and preserved, including their relative market ordering.
    """
    def __init__(self, maximum):
        self.maximum = maximum
        self.items = deque()
        self.condition = threading.Condition()
        self.metrics = dict(enqueued=0, written=0, dropped=0, coalesced=0,
                            queue_high_water=0, batch_latency_ms=0, last_write_error=None)

    def put(self, market, kind, records):
        if not records:
            return
        with self.condition:
            if kind == 'research':
                for item in self.items:
                    if item[0:2] == (market, kind):
                        item[2][:] = records
                        self.metrics['coalesced'] += 1
                        return
            while len(self.items) >= self.maximum:
                disposable = next((i for i, item in enumerate(self.items) if item[1] == 'research'), None)
                if disposable is not None:
                    del self.items[disposable]
                    self.metrics['dropped'] += 1
                elif kind == 'research':
                    self.metrics['dropped'] += 1
                    return
                else:
                    self.condition.wait(.1)
            self.items.append((market, kind, records, time.monotonic()))
            self.metrics['enqueued'] += len(records)
            self.metrics['queue_high_water'] = max(self.metrics['queue_high_water'], len(self.items))
            self.condition.notify_all()

    def take(self, maximum):
        with self.condition:
            batch = []
            rows = 0
            while self.items:
                count = len(self.items[0][2])
                if batch and rows + count > maximum:
                    break
                batch.append(self.items.popleft())
                rows += count
            self.condition.notify_all()
            return batch

    def snapshot(self):
        with self.condition:
            return {**self.metrics, 'queue_depth': len(self.items),
                    'writer_lag_seconds': time.monotonic() - self.items[0][3] if self.items else 0}


class Pipeline:
    def __init__(self):
        self.lock = threading.Lock()
        self.evaluation_lock = threading.Lock()
        self.books = {}
        self.markets = {}
        self.asset_markets = {}
        self.dirty = set()
        self.last_eval = {}
        self.queue = WriteQueue(updown.setting('WRITE_QUEUE_MAX', 10000, 1, 100000))
        self.evaluator_stop = threading.Event()
        self.writer_stop = threading.Event()
        self.market_done = threading.Event()
        self.market_done.set()
        self.messages = 0
        self.started = time.monotonic()
        self.last_message = None
        self.last_message_at = None
        self.inflight = []
        self.cycle_ms = 0
        self.evaluator = threading.Thread(target=self.evaluate_loop, name='polymarket-evaluator', daemon=True)
        self.writer = threading.Thread(target=self.write_loop, name='polymarket-writer', daemon=True)

    def start(self):
        self.writer.start()
        self.evaluator.start()

    def set_markets(self, markets):
        # Serialize removed-market ends with evaluations already holding a
        # snapshot, so an old snapshot cannot reopen a removed episode.
        with self.evaluation_lock:
            self._set_markets(markets)

    def _set_markets(self, markets):
        with self.lock:
            previous = set(self.markets)
            self.markets = {m['id']: m for m in markets}
            self.asset_markets = {a: m['id'] for m in markets for a in subscription_assets([m])}
            self.books = {a: b for a, b in self.books.items() if a in self.asset_markets}
            self.dirty.intersection_update(self.markets)
            self.last_eval = {k: v for k, v in self.last_eval.items() if k in self.markets}
        for identity in previous - self.markets.keys():
            self.capture(identity, 'observation', lambda: _end_episode(identity, datetime.now(timezone.utc)))

    def receive(self, raw):
        if raw == 'PONG':
            return
        try:
            event = json.loads(raw)
            with self.lock:
                changed = ws_apply_event(event, self.books)
                self.dirty.update(self.asset_markets[a] for a in changed if a in self.asset_markets)
                self.messages += 1
                self.last_message = time.monotonic()
                self.last_message_at = datetime.now(timezone.utc).isoformat()
        except (ValueError, TypeError, KeyError) as exc:
            with _LOCK:
                STATUS["last_error"] = str(exc)[:160]

    def capture(self, market, kind, callback):
        _CAPTURE.records = []
        try:
            callback()
            records = _CAPTURE.records
        finally:
            del _CAPTURE.records
        self.queue.put(market, kind, records)

    def evaluate(self, now=None):
        with self.evaluation_lock:
            self._evaluate(now)

    def _evaluate(self, now=None):
        began = time.monotonic()
        now = began if now is None else now
        with self.lock:
            ready = [identity for identity in self.dirty if now - self.last_eval.get(identity, float('-inf')) >= 1]
            frames = []
            for identity in ready:
                market = self.markets.get(identity)
                if not market:
                    continue
                books = {a: {side: dict(levels) for side, levels in self.books[a].items()}
                         for a in subscription_assets([market]) if a in self.books}
                frames.append((market, books))
                self.dirty.discard(identity)
                self.last_eval[identity] = now
        for market, books in frames:
            quote = opportunity(market, books, os.getenv('POLYMARKET_OBSERVER_SIZE_SHARES', '100'),
                                os.getenv('POLYMARKET_OBSERVER_MERGE_COST', '0.02'))
            self.capture(market['id'], 'research', lambda: record_research(market, books, 'ws', quote))
            self.capture(market['id'], 'observation', lambda: record_sample(market, quote, 'ws') if quote
                         else _end_episode(market['id'], datetime.now(timezone.utc)))
        self.cycle_ms = (time.monotonic() - began) * 1000

    def evaluate_loop(self):
        while not self.evaluator_stop.wait(updown.setting('EVALUATE_MS', 250, 50, 5000) / 1000):
            try:
                self.evaluate()
            except Exception as exc:
                with _LOCK:
                    STATUS['last_error'] = str(exc)[:160]

    def write_batch(self, batch):
        began = time.monotonic()
        records = [record for item in batch for record in item[2]]
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                # Group adjacent identical statements only: grouping across end/start
                # boundaries would reorder episode transitions for the same market.
                for sql, group in groupby(records, key=lambda row: row[0]):
                    cur.executemany(sql, [params for _, params in group])
                db.commit()
            finally:
                cur.close()
        with self.queue.condition:
            self.queue.metrics['written'] += len(records)
            self.queue.metrics['batch_latency_ms'] = (time.monotonic() - began) * 1000
            self.queue.metrics['last_write_error'] = None

    def write_loop(self):
        batch = []
        interval = updown.setting('WRITE_BATCH_MS', 1000, 10, 10000) / 1000
        while True:
            if not batch:
                batch = self.queue.take(updown.setting('WRITE_BATCH_SIZE', 500, 1, 5000))
            self.inflight = batch
            if batch:
                try:
                    self.write_batch(batch)
                    batch = []
                    self.inflight = []
                except Exception as exc:
                    with self.queue.condition:
                        self.queue.metrics['last_write_error'] = str(exc)[:160]
                    # Retain failed transitions ahead of later records. Retry without
                    # re-enqueuing into the bounded queue (which could deadlock).
                    with self.queue.condition:
                        self.queue.condition.wait(interval)
                    continue
            if (self.writer_stop.is_set() and not self.evaluator.is_alive()
                    and self.market_done.is_set() and not self.queue.snapshot()['queue_depth']):
                return
            with self.queue.condition:
                self.queue.condition.wait(interval)

    def metrics(self):
        with self.lock:
            result = {'messages_received': self.messages,
                      'last_message_at': self.last_message_at,
                      'messages_per_second': self.messages / max(.001, time.monotonic() - self.started),
                      'last_message_age_seconds': time.monotonic() - self.last_message if self.last_message else None,
                      'evaluator_cycle_ms': self.cycle_ms}
        metrics = self.queue.snapshot()
        if self.inflight:
            metrics['writer_lag_seconds'] = max(metrics['writer_lag_seconds'], time.monotonic() - self.inflight[0][3])
        return {**result, **metrics, 'writer_inflight': len(self.inflight)}

    def close(self, timeout):
        deadline = time.monotonic() + timeout
        self.evaluator_stop.set()
        self.evaluator.join(max(0, deadline - time.monotonic()))
        self.writer_stop.set()
        with self.queue.condition:
            self.queue.condition.notify_all()
        self.writer.join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['flush_complete'] = not self.writer.is_alive() and not self.evaluator.is_alive()


def shard_markets(markets, count):
    """Stable whole-market assignment; two shards separate catalogue types."""
    result = {}
    for market in markets:
        index = (0 if market.get('market_type') != 'up_down' else 1) if count == 2 else crc32(str(market['id']).encode()) % count
        result.setdefault(index, []).append(market)
    return result


class MarketConnections:
    """Own independent public receive threads and per-socket subscription sets."""
    def __init__(self):
        self.states = {}
        self.count = updown.setting('WS_SHARDS', 2, 1, 8)
        self.closed = False

    def sync(self, markets):
        assignments = shard_markets(markets, self.count)
        for index in list(self.states):
            if index not in assignments:
                state = self.states.pop(index)
                state['socket'].close()
                state['thread'].join(5)
        for index, group in assignments.items():
            assets = subscription_assets(group)
            state = self.states.get(index)
            if state:
                with state['lock']:
                    previous = state['assets']
                    state['assets'] = assets
                    if state['connected']:
                        try:
                            for message in subscription_changes(previous, assets):
                                state['socket'].send(json.dumps(message))
                        except Exception as exc:
                            state['connected'] = False
                            state['socket'].close()
                            _ws_error(exc)
            else:
                self.states[index] = dict(assets=assets, socket=None, thread=None, connected=False, lock=RLock(),
                                          attempts=0, retry_at=0, ping_at=0)
        self.tick()

    def connect(self, index, state):
        import websocket
        if state['socket']:
            state['socket'].close()
            state['thread'].join(5)
            with _LOCK:
                STATUS['reconnect_count'] += 1
            if _PIPELINE:
                with _PIPELINE.lock:
                    for asset in state['assets']:
                        _PIPELINE.books.pop(asset, None)
        state['attempts'] += 1
        state['retry_at'] = time.monotonic() + min(120, 5 * 2 ** min(5, state['attempts'] - 1))

        def opened(sock):
            if self.closed or _STOP.is_set():
                sock.close()
                return
            with state['lock']:
                sock.send(json.dumps({'assets_ids': state['assets'], 'type': 'market'}))
                state['connected'] = True
                state['attempts'] = 0
                state['ping_at'] = time.monotonic() + 10
            _ws_connected()
            with _LOCK:
                STATUS['ws_connections'] = STATUS.get('ws_connections', 0) + 1

        def received(_sock, raw):
            if _PIPELINE:
                _PIPELINE.receive(raw)

        def failed(_sock, error):
            state['connected'] = False
            _ws_error(error)

        def closed(_sock, code=None, reason=None):
            state['connected'] = False
            if code is not None or reason is not None:
                close_reason(code, reason)
            elif not self.closed and 'slow consumer' not in str(STATUS.get('last_error', '')).lower():
                close_reason(None, STATUS.get('last_error'))
            state['retry_at'] = time.monotonic() + 5

        sock = websocket.WebSocketApp('wss://ws-subscriptions-clob.polymarket.com/ws/market',
                                     on_open=opened, on_message=received, on_error=failed, on_close=closed)
        state['socket'] = sock
        state['thread'] = threading.Thread(target=lambda: sock.run_forever(ping_interval=0),
                                           name=f'polymarket-receive-{index}', daemon=True)
        state['thread'].start()

    def tick(self):
        if self.closed or _STOP.is_set():
            return
        now = time.monotonic()
        for index, state in self.states.items():
            if not state['thread'] or not state['thread'].is_alive():
                if now >= state['retry_at']:
                    self.connect(index, state)
            elif state['connected'] and now >= state['ping_at']:
                try:
                    state['socket'].send('PING')
                    state['ping_at'] = now + 10
                except Exception as exc:
                    state['connected'] = False
                    state['socket'].close()
                    _ws_error(exc)
        with _LOCK:
            STATUS['ws_shards'] = self.count
            STATUS['ws_connected'] = sum(bool(s['connected']) for s in self.states.values())
            STATUS['connected'] = bool(self.states) and all(s['connected'] for s in self.states.values())

    def failures(self):
        return max((state['attempts'] for state in self.states.values()), default=0)

    def close(self, timeout=5):
        deadline = time.monotonic() + timeout
        self.closed = True
        for state in list(self.states.values()):
            if state['socket']:
                state['socket'].close()
        for state in list(self.states.values()):
            if state['thread']:
                state['thread'].join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['ws_connected'] = 0
            STATUS['connected'] = False

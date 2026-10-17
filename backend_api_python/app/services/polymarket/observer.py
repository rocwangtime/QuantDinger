"""Dedicated public market observer; it never places or signs orders."""
import json
import logging
import os
import sys
import threading
import time
from time import process_time
from contextlib import contextmanager
from collections import deque
from itertools import groupby
import random
from queue import Queue, Full, Empty
from threading import RLock, Event
from datetime import datetime, timezone

from app.utils.db import get_db_connection
from .account_history import max_pair_skew_ms, pair_rejection
from .client import PublicClient
from .engine import number, sweep
from . import updown, recording
from .ws_balance import RateBalancer, DailyStats, balance_key
from .ws_transport import TransportDiagnostics, receive_buffer_options, remote_close

STATUS = {"enabled": False, "running": False, "mode": "stopped", "connected": False, "markets_subscribed": 0,
          "last_message_at": None, "rebalance_count": 0, "reconnect_count": 0, "ws_connections": 0, "slow_consumer_count": 0, "last_error": None, "started_at": None}
_LOCK = threading.RLock()
_STOP = threading.Event()
_THREAD = None
_LAST_SAMPLE = {}
_LAST_RESEARCH = {}
_REFERENCE = None
_PIPELINE = None
_WS = None
_DAILY_STATS = DailyStats()
MARKET_WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
_LOG = logging.getLogger(__name__)


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
    provider = _REFERENCE
    if hasattr(provider, 'metrics'):  # Spot WebSocket composite only.
        result.update(provider.metrics())
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
    _DAILY_STATS.persist()
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
    chainlink = None
    if _PIPELINE:
        with _PIPELINE.lock:
            tick = _PIPELINE.recording.twap_cache.get(market['asset'])
        if tick and 0 <= (now - tick[2]).total_seconds() <= 5:
            chainlink = tick
    tags = sample_tags(market, now)
    with _observation_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,observed_at,market_type,asset,interval,expiry_bucket,end_time,up_ask,up_mid,down_ask,down_mid,price_to_beat,reference_price,reference_source,reference_observed_at,source,net_edge_bps,chainlink_price,chainlink_observed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (market['id'], now, *tags, market['endDate'], *prices, market.get('price_to_beat'), reference['price'], reference['source'], reference['observed_at'], source, quote.get('net_edge_bps') if quote else None, chainlink[4] if chainlink else None, chainlink[2] if chainlink else None))
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
            recording.cleanup(cur)
            if updown.enabled():
                cur.execute("DELETE FROM qd_polymarket_observer_samples WHERE observed_at < NOW() - (%s * INTERVAL '1 day')", (updown.setting('UPDOWN_SAMPLE_RETENTION_DAYS', 7, 1, 365),))
            db.commit()
        finally:
            cur.close()
    _RETENTION_LAST[0] = now_mono


_RETENTION_LAST = [0.0]


def subscription_assets(markets):
    return [a for m in markets for a in (m['yesAssetId'], m['noAssetId'])]


def tune_switch_interval():
    """Shorten the GIL hand-off in the dedicated observer process.

    Socket readers release the GIL on every recv; with the default 5 ms switch
    interval a CPU-bound thread (evaluator, workers, paper account) can make
    each reacquire wait that long. POLYMARKET_OBSERVER_GIL_SWITCH_MS, default
    0.5; 0 keeps the interpreter default.
    """
    try:
        value = float(os.getenv('POLYMARKET_OBSERVER_GIL_SWITCH_MS', '0.5'))
    except ValueError:
        return
    if value > 0:
        sys.setswitchinterval(min(5.0, max(.1, value)) / 1000)


def _prior_group_rates():
    """Group msg/s from the last persisted status, used only for first placement."""
    try:
        rates = (get_status() or {}).get('ws_group_rates') or {}
        return {str(k): float(v) for k, v in rates.items() if float(v) >= 0}
    except (TypeError, ValueError, AttributeError):
        return {}


def _market_loop():
    global _REFERENCE, _WS
    client = PublicClient()
    series_cache = {}
    yes_markets = []
    yes_refresh = 0
    if updown.enabled():
        _REFERENCE = updown.reference_provider()
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
                        tune_switch_interval()
                        connections = MarketConnections()
                        _WS = connections
                        connections.seed_rates(_prior_group_rates())
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
                            if _PIPELINE and _PIPELINE.paper_account:
                                at = frame['observedMs']
                                with _PIPELINE.lock:
                                    for asset in mapped:
                                        _PIPELINE.paper_account.history.record_rest(asset, frame['books'][asset], at)
                                _PIPELINE.paper_account.submit(market, mapped, at)
                            if _PIPELINE and _PIPELINE.recording.enabled:
                                with _PIPELINE.evaluation_lock:
                                    metadata = {a: (recording.exchange_time(frame['books'][a].get('exchangeTimestamp')),
                                                   datetime.fromtimestamp(frame['observedMs'] / 1000, timezone.utc), 'rest')
                                                for a in mapped}
                                    with _PIPELINE.lock:
                                        _PIPELINE.books.update(mapped)
                                        _PIPELINE.recording.metadata.update(metadata)
                                    for key, kind, records in _PIPELINE.recording.books(
                                            mapped, {market['id']: market}, {a: market['id'] for a in mapped}, metadata, time.monotonic()):
                                        _PIPELINE.queue.put(key, kind, records)
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
        if _PIPELINE and (_PIPELINE.writer.is_alive() or _PIPELINE.evaluator.is_alive()
                          or _PIPELINE.references.thread.is_alive()
                          or (_PIPELINE.references.socket_thread and _PIPELINE.references.socket_thread.is_alive())):
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
            _PIPELINE.writer.is_alive() or _PIPELINE.evaluator.is_alive()
            or _PIPELINE.references.thread.is_alive()
            or (_PIPELINE.references.socket_thread and _PIPELINE.references.socket_thread.is_alive())))
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
        STATUS['last_close_code'] = code
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
            if kind in {'research', 'book'}:
                for item in self.items:
                    if item[0:2] == (market, kind):
                        item[2][:] = records
                        self.metrics['coalesced'] += 1
                        return
            while len(self.items) >= self.maximum:
                disposable = next((i for i, item in enumerate(self.items) if item[1] in {'research', 'book', 'trade', 'twap', 'reference', 'resolution'}), None)
                if disposable is not None:
                    del self.items[disposable]
                    self.metrics['dropped'] += 1
                elif kind in {'research', 'book', 'trade', 'twap', 'reference', 'resolution'}:
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
        self.lock = threading.RLock()
        self.evaluation_lock = threading.Lock()
        self.books = {}
        self.recording = recording.Recorder()
        self.references = recording.References(self)
        from .paper_account import Runtime, enabled as account_enabled
        self.paper_account = Runtime(self) if account_enabled() else None
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
        self.pair_rejections = dict(stale=0, skew=0)
        self.evaluator = threading.Thread(target=self.evaluate_loop, name='polymarket-evaluator', daemon=True)
        self.writer = threading.Thread(target=self.write_loop, name='polymarket-writer', daemon=True)

    def start(self):
        self.writer.start()
        if self.recording.enabled:
            self.references.thread.start()
        if self.paper_account:
            self.paper_account.thread.start()
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
            self.recording.prune(self.asset_markets)
            if self.paper_account:
                self.paper_account.history.assets = {a: b for a, b in self.paper_account.history.assets.items() if a in self.asset_markets}
            self.dirty.intersection_update(self.markets)
            self.last_eval = {k: v for k, v in self.last_eval.items() if k in self.markets}
        if self.recording.enabled:
            for market in markets:
                if (market['id'] not in previous and market.get('market_type') == 'up_down'
                        and recording.scoped(market, self.recording.scope)):
                    self.queue.put(market['id'], 'window', [(recording.WINDOW_SQL, (
                        market['id'], market['conditionId'], market['asset'], market['interval'],
                        market['eventStartTime'], market['endDate'], market.get('price_to_beat'),
                        market['yesAssetId'], market['noAssetId'], recording.settlement_window(market)))])
        for identity in previous - self.markets.keys():
            self.capture(identity, 'observation', lambda: _end_episode(identity, datetime.now(timezone.utc)))

    def receive(self, raw, owned=None, received_at=None, inbox=None, epoch=None):
        if raw == 'PONG':
            return
        try:
            received = received_at or datetime.now(timezone.utc)
            event = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            waiting_at = time.monotonic()
            with self.lock:
                if inbox:
                    inbox.lock_wait_seconds = time.monotonic() - waiting_at
                    if owned is not None and hasattr(inbox, "activated"):
                        owned = {a for a in owned if received >= inbox.activated.get(a, received)}
                if inbox and (inbox.invalid.is_set() or inbox.resync.is_set() or
                              (epoch is not None and epoch != inbox.epoch)):
                    return
                if inbox:
                    inbox.lag_seconds = (datetime.now(timezone.utc) - received).total_seconds()
                event = recording.owned_events(event, owned)
                changed = ws_apply_event(event, self.books)
                self.recording.receive(event, changed, self.markets, self.asset_markets, received, owned)
                if self.paper_account:
                    self.paper_account.history.record(event, int(received.timestamp() * 1000))
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
            pending = list(self.recording.pending)
            self.recording.pending.clear()
            # Copy while locked; sorting/throttling and queue work occur outside.
            recorded_books = {a: {side: dict(levels) for side, levels in book.items()}
                              for a, book in self.books.items()} if self.recording.enabled else {}
            metadata = dict(self.recording.metadata)
            markets, asset_markets = dict(self.markets), dict(self.asset_markets)
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
        for key, kind, records in pending + self.recording.books(recorded_books, markets, asset_markets, metadata, now):
            self.queue.put(key, kind, records)
        for market, books in frames:
            evaluated_at = datetime.now(timezone.utc)
            exchange_ms = [metadata[a][0].timestamp() * 1000
                           if a in metadata and metadata[a][0] is not None else None
                           for a in subscription_assets([market])]
            reason = pair_rejection(exchange_ms, evaluated_at.timestamp() * 1000,
                                    max_skew=max_pair_skew_ms())
            if reason:
                with self.lock:
                    self.pair_rejections[reason] += 1
                self.capture(market['id'], 'observation',
                             lambda: _end_episode(market['id'], evaluated_at))
                continue
            if self.paper_account:
                self.paper_account.submit(market, books, int(time.time() * 1000))
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
        counts = {}
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                # Group adjacent identical statements only: grouping across end/start
                # boundaries would reorder episode transitions for the same market.
                for sql, group in groupby(records, key=lambda row: row[0]):
                    params = [params for _, params in group]
                    table = next((table for table in recording.TABLES
                                  if sql.startswith('INSERT INTO ' + recording.PREFIX + table + '(')), None)
                    if table:
                        # execute_batch exposes only its last statement rowcount.
                        # One multi-VALUES insert counts actual rows, even on retry.
                        header, values = sql.split(' VALUES ', 1)
                        placeholders, _, tail = values.partition(')')
                        statement = header + ' VALUES ' + ','.join([placeholders + ')'] * len(params)) + tail
                        cur.execute('WITH recorded AS (' + statement + ' RETURNING 1) SELECT count(*) count FROM recorded',
                                    tuple(value for row in params for value in row))
                        counts[table] = counts.get(table, 0) + cur.fetchone()['count']
                    else:
                        cur.executemany(sql, params)
                db.commit()
            finally:
                cur.close()
        self.recording.committed(counts)
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
                    and self.market_done.is_set() and not self.references.thread.is_alive() and not self.queue.snapshot()['queue_depth']):
                return
            with self.queue.condition:
                self.queue.condition.wait(interval)

    def metrics(self):
        with self.lock:
            result = {'messages_received': self.messages,
                      'last_message_at': self.last_message_at,
                      'messages_per_second': self.messages / max(.001, time.monotonic() - self.started),
                      'last_message_age_seconds': time.monotonic() - self.last_message if self.last_message else None,
                      'evaluator_cycle_ms': self.cycle_ms,
                      'pair_rejections': dict(self.pair_rejections),
                      **self.recording.metrics(self.books), **self.references.metrics()}
        metrics = self.queue.snapshot()
        if self.inflight:
            metrics['writer_lag_seconds'] = max(metrics['writer_lag_seconds'], time.monotonic() - self.inflight[0][3])
        if self.paper_account:
            result['paper_account'] = dict(running=self.paper_account.ready, dropped=self.paper_account.dropped, error=self.paper_account.error)
        return {**result, **metrics, 'writer_inflight': len(self.inflight)}

    def close(self, timeout):
        deadline = time.monotonic() + timeout
        self.references.close(max(0, deadline - time.monotonic()))
        self.evaluator_stop.set()
        self.evaluator.join(max(0, deadline - time.monotonic()))
        if not self.evaluator.is_alive():
            self.evaluate()
        if self.paper_account:
            self.paper_account.close(max(0, deadline - time.monotonic()))
        self.writer_stop.set()
        with self.queue.condition:
            self.queue.condition.notify_all()
        self.writer.join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['flush_complete'] = (not self.writer.is_alive() and not self.evaluator.is_alive()
                                        and not self.references.thread.is_alive()
                                        and not (self.references.socket_thread and self.references.socket_thread.is_alive()))


def shard_markets(markets, count, previous=None, rates=None, owners=None):
    """Place whole balance groups (an Up/Down series or one YES/NO market).

    ``previous`` maps assets to their applied shard, ``owners`` remembers group
    shards across rollovers and ``rates`` holds group msg/s. Known groups keep
    their shard; new groups go largest-first to the lightest shard. Periodic
    rebalancing is RateBalancer.plan, never a catalogue refresh.
    """
    balancer = RateBalancer(count)
    balancer.rates = dict(rates or {})
    groups = {}
    for market in markets:
        groups.setdefault(balance_key(market), []).append(market)
    known = {k: v for k, v in (owners or {}).items() if v < count}
    previous = previous or {}
    for key, group in groups.items():
        votes = [previous[a] for a in subscription_assets(group) if previous.get(a, count) < count]
        if key not in known and votes:
            known[key] = min(set(votes), key=lambda shard: (-votes.count(shard), shard))
    placed = balancer.place(list(groups), known)
    result = {}
    for key, group in groups.items():
        result.setdefault(placed[key], []).extend(group)
    return {i: result[i] for i in sorted(result) if result[i]}


class ReceiveQueue:
    """Elastic bounded FIFO; a gap requests snapshots on the existing socket."""
    def __init__(self, maximum, ceiling=None):
        self.queue = Queue(maximum)
        self.ceiling = max(maximum, ceiling or maximum)
        self.lock = RLock()
        self.resync = Event()
        self.epoch = 0
        self.resyncs = 0
        self.delivery_gap = False
        self.lag_seconds = 0
        self.lock_wait_seconds = 0
        self.queue_wait_seconds = 0
        self.dropped = 0
        self.high_water = 0
        self.received_frames = self.received_bytes = 0
        self.callback_max_seconds = self.parse_max_seconds = 0
        self.metric_at = time.monotonic()
        self.metric_frames = self.metric_bytes = 0
        self.receive_rate = self.byte_rate = 0
        self.invalid = Event()

    def put(self, raw):
        with self.lock:
            if self.invalid.is_set() or self.resync.is_set():
                self.dropped += 1
                return False
            if self.queue.full() and self.queue.maxsize < self.ceiling:
                with self.queue.mutex:
                    self.queue.maxsize = min(self.ceiling, self.queue.maxsize * 2)
            try:
                self.queue.put_nowait((raw, datetime.now(timezone.utc), self.epoch))
                self.high_water = max(self.high_water, self.queue.qsize())
                return True
            except Full:
                self.epoch += 1
                self.delivery_gap = True
                self.resync.set()
                self.dropped += 1
                return False

    def discard(self):
        while True:
            try:
                self.queue.get_nowait()
                self.dropped += 1
            except Empty:
                return


READ_AHEAD_BYTES = 65536


def read_ahead_app(base):
    """WebSocketApp whose reader drains buffered frames with few syscalls.

    websocket-client reads a frame as select + 2-byte header + payload, each a
    GIL release; under contention every reacquire can wait a full switch
    interval, which capped a socket near 100 frames/s. This reader takes up to
    64 KiB per recv (frame_buffer keeps the surplus) and parses every buffered
    frame before selecting again. Unknown library internals fall back to the
    stock dispatcher.
    """
    try:
        import selectors
        from websocket._dispatcher import DispatcherBase
    except ImportError:
        return base
    if not isinstance(base, type):
        return base

    class Dispatcher(DispatcherBase):
        def read(self, sock, read_callback, check_callback):
            app = self.app
            if app.sock is None or app.sock.sock is None:
                return
            frames = getattr(app.sock, 'frame_buffer', None)
            raw = getattr(app.sock, '_recv', None)
            sock = app.sock.sock
            pending = getattr(sock, 'pending', None)
            app.read_ahead_active = False
            if not (callable(raw) and hasattr(frames, 'recv_buffer')):
                # Use the library's SSL-aware dispatcher if its internal API
                # changes. Do not silently keep half of the custom read path.
                return base.create_dispatcher(app, self.ping_timeout, is_ssl=callable(pending)).read(
                    sock, read_callback, check_callback)
            app.read_ahead_active = True
            app.read_calls = app.read_bytes = 0

            def buffered_recv(_size):
                data = raw(READ_AHEAD_BYTES)
                app.read_calls += 1
                app.read_bytes += len(data)
                return data
            frames.recv = buffered_recv
            sel = selectors.DefaultSelector()
            sel.register(sock, selectors.EVENT_READ)
            try:
                while app.keep_running:
                    if ((frames is not None and any(frames.recv_buffer))
                            or (callable(pending) and pending()) or sel.select(self.ping_timeout)):
                        if not read_callback():
                            break
                    check_callback()
            finally:
                sel.close()

    class App(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.transport = TransportDiagnostics()
            self.text_heartbeat_active = False

        def _send_ping(self):
            # Use the library's per-connection lifecycle, but the exchange expects
            # TEXT PING, not an RFC control ping. Discovery/retention/SQL on the
            # market loop must not delay it. Sample even when no frames arrive.
            stop_ping = self.stop_ping
            self.text_heartbeat_active = True
            deadline = time.monotonic() + self.ping_interval
            try:
                while not stop_ping.wait(1) and self.keep_running:
                    now = time.monotonic()
                    self.transport.sample(self, now)
                    if now >= deadline and self.sock:
                        self.transport.ping_at = now
                        self.sock.send('PING')
                        self.transport.pings += 1
                        deadline = now + self.ping_interval
            except Exception as exc:
                self._callback(self.on_error, exc)
                # shutdown wakes a blocked reader without a close handshake.
                if self.sock:
                    self.sock.shutdown()
            finally:
                self.text_heartbeat_active = False

        def create_dispatcher(self, ping_timeout, dispatcher=None, is_ssl=False, handleDisconnect=None):
            if dispatcher:
                return super().create_dispatcher(ping_timeout, dispatcher, is_ssl, handleDisconnect)
            return Dispatcher(self, ping_timeout or 10)
    return App


def socket_pending_bytes(app):
    """Linux encrypted TCP receive backlog; None when unsupported/disconnected."""
    try:
        import array
        import fcntl
        import termios
        pending = array.array('i', [0])
        fcntl.ioctl(app.sock.sock.fileno(), termios.FIONREAD, pending, True)
        return pending[0]
    except (ImportError, AttributeError, OSError, ValueError):
        return None


class MarketConnections:
    """Own independent public receive threads and per-socket subscription sets."""
    def __init__(self):
        self.states = {}
        self.count = updown.setting('WS_SHARDS', 4, 1, 8)
        self.balancer = RateBalancer(self.count)
        self.stats = _DAILY_STATS
        self.reconnect_pending = False
        self.sample_unavailable = set()
        self.cpu_at, self.cpu_seconds = time.monotonic(), process_time()
        self.tick_at = self.cpu_at
        self.tick_gap_max = 0
        self.closed = False
        self.markets = []

    def sync(self, markets):
        self.markets = list(markets)
        target = updown.setting('WS_ASSETS_PER_SHARD', 600, 1, 100000)
        self.count = max(self.count, min(8, (len(subscription_assets(markets)) + target - 1) // target))
        self.balancer.count = self.count
        self.balancer.set_groups({a: balance_key(m) for m in markets for a in subscription_assets([m])},
                                 time.monotonic())
        if set(subscription_assets(markets)) == set(self.balancer.assignment):
            self.tick()
            return
        # Rollover keeps each series on its shard: only changed assets move.
        assignments = shard_markets(markets, self.count, self.balancer.assignment,
                                    self.balancer.rates, self.balancer.owners)
        assignment = {a: i for i, group in assignments.items() for a in subscription_assets(group)}
        self.apply_assignment(assignment)
        self.tick()

    def seed_rates(self, rates):
        """Prior weights place a cold start by load; they never trigger moves."""
        with self.balancer.lock:
            for key, rate in rates.items():
                self.balancer.rates.setdefault(key, rate)

    def assignment_for(self, owners):
        return {a: owners[balance_key(m)] for m in self.markets for a in subscription_assets([m])}

    def apply_assignment(self, assignment):
        assignments = {i: [a for a, shard in assignment.items() if shard == i] for i in range(self.count)}
        # Serialize publication with consumers. Retained assets keep both books
        # and queued deltas; moved assets require a new snapshot on their owner.
        pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
        with pipeline_lock:
            old = self.balancer.assignment
            old_owners = self.balancer.current_owners()
            moved = any(a in assignment and assignment[a] != i for a, i in old.items())
            changed = {a for a in old.keys() | assignment.keys() if old.get(a) != assignment.get(a)}
            if _PIPELINE:
                for asset in changed:
                    _PIPELINE.books.pop(asset, None)
                    _PIPELINE.recording.metadata.pop(asset, None)
                    if _PIPELINE.paper_account:
                        _PIPELINE.paper_account.history.assets.pop(asset, None)
            # Remove on every source before subscribing on any destination.
            changes = {}
            for index, assets in assignments.items():
                state = self.states.setdefault(index, dict(assets=[], socket=None, thread=None,
                    connected=False, lock=RLock(), attempts=0, retry_at=0, ping_at=0))
                changes[index] = subscription_changes(state['assets'], assets)
                with state['lock']:
                    state['assets'] = assets
                    if state.get('inbox'):
                        activated = getattr(state['inbox'], 'activated', {})
                        state['inbox'].activated = {a: (datetime.max.replace(tzinfo=timezone.utc) if a in changed else activated[a])
                                                   for a in assets if a in changed or a in activated}
            with self.balancer.lock:
                self.balancer.assignment = dict(assignment)
                new_owners = self.balancer.current_owners()
                self.balancer.owners.update(new_owners)
                self.balancer.applied(old_owners, new_owners, time.monotonic())
        # Subscription sends/close handshakes may block on the network. Ownership
        # is already published; retained books must not wait behind that I/O.
        for operation in ('unsubscribe', 'subscribe'):
            for index, commands in changes.items():
                state = self.states[index]
                with state['lock']:
                    if state['connected'] and state['socket']:
                        for command in commands:
                            if command['operation'] == operation:
                                try:
                                    if operation == 'subscribe' and state.get('inbox'):
                                        for asset in command['assets_ids']:
                                            state['inbox'].activated[asset] = datetime.now(timezone.utc)
                                    state['socket'].send(json.dumps(command))
                                except Exception as exc:
                                    # A real wire failure affects this socket only.
                                    state['connected'] = False
                                    if state.get('inbox'):
                                        state['inbox'].invalid.set()
                                    state['socket'].close()
                                    _ws_error(exc)
                                    break

        if moved:
            self.balancer.last_rebalance = time.monotonic()
            self.stats.increment('rebalance_count')
            with _LOCK:
                STATUS['rebalance_count'] = STATUS.get('rebalance_count', 0) + 1
        return True

    def connect(self, index, state):
        import websocket
        if state['socket']:
            if state.get('inbox'):
                state['inbox'].invalid.set()
            state['socket'].close()
            state['thread'].join(5)
            if state.get('worker'):
                state['worker'].join(5)
            with _LOCK:
                STATUS['reconnect_count'] += 1
            self.stats.increment('reconnect_count')
            _LOG.info('Polymarket WS reconnect shard=%s attempt=%s', index, state['attempts'] + 1)
            self.reconnect_pending = True
            if _PIPELINE:
                with _PIPELINE.lock:
                    for asset in state['assets']:
                        _PIPELINE.books.pop(asset, None)
                        _PIPELINE.recording.metadata.pop(asset, None)
                        if _PIPELINE.paper_account:
                            _PIPELINE.paper_account.history.assets.pop(asset, None)
        state['attempts'] += 1
        inbox = ReceiveQueue(updown.setting('WS_RECEIVE_QUEUE_MAX', 2048, 1, 100000),
                             updown.setting('WS_RECEIVE_QUEUE_CEILING', 16384, 1, 100000))
        state['receive_drops'] = state.get('receive_drops', 0) + (state['inbox'].dropped if state.get('inbox') else 0)
        state['inbox'] = inbox
        state['queue_logged'] = False
        state['retry_at'] = time.monotonic() + min(120, 5 * 2 ** min(5, state['attempts'] - 1))

        def opened(sock):
            if self.closed or _STOP.is_set():
                sock.close()
                return
            with state['lock']:
                sock.send(json.dumps({'assets_ids': state['assets'], 'type': 'market'}))
                state['connected'] = True
                state['opened_at'] = time.monotonic()
                state['ping_at'] = time.monotonic() + 10
            _ws_connected()
            with _LOCK:
                STATUS['ws_connections'] = STATUS.get('ws_connections', 0) + 1

        def process_frames():
            while not self.closed and state.get('inbox') is inbox and not inbox.invalid.is_set():
                if inbox.resync.is_set():
                    # Serialize gap recovery with publication. Never reuse a book
                    # assembled across a missing delta; request fresh full books.
                    pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
                    with pipeline_lock, inbox.lock:
                        if inbox.invalid.is_set() or state.get('inbox') is not inbox:
                            break
                        inbox.discard()
                        if _PIPELINE:
                            for asset in state['assets']:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.assets.pop(asset, None)
                        inbox.resyncs += 1
                    with state['lock']:
                        sock.send(json.dumps(dict(assets_ids=state['assets'], operation='unsubscribe')))
                        with inbox.lock:
                            inbox.resync.clear()
                        sock.send(json.dumps(dict(assets_ids=state['assets'], operation='subscribe')))
                    _LOG.warning('Polymarket WS snapshot recovery shard=%s drops=%s', index, inbox.dropped)
                try:
                    raw, received_at, epoch = inbox.queue.get(timeout=.2)
                    inbox.queue_wait_seconds = (datetime.now(timezone.utc) - received_at).total_seconds()
                    inbox.lag_seconds = inbox.queue_wait_seconds
                    if inbox.queue_wait_seconds > 1:
                        inbox.delivery_gap = True
                except Empty:
                    continue
                try:
                    parsing_at = time.monotonic()
                    event = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
                    inbox.parse_max_seconds = max(inbox.parse_max_seconds, time.monotonic() - parsing_at)
                    self.balancer.receive(event, index)
                    self.stats.message(index)
                    if _PIPELINE:
                        # Recheck generation under the same lock used for invalidation.
                        waiting_at = time.monotonic()
                        with _PIPELINE.lock:
                            lock_wait = time.monotonic() - waiting_at
                            valid = not inbox.invalid.is_set() and state.get('inbox') is inbox
                            if valid:
                                _PIPELINE.receive(event, frozenset(state['assets']), received_at, inbox, epoch)
                                inbox.lock_wait_seconds = lock_wait
                except (ValueError, TypeError, AttributeError):
                    continue

        def consume():
            try:
                process_frames()
            except Exception:
                _LOG.exception('Polymarket book worker failed shard=%s', index)
                inbox.invalid.set()
            finally:
                while True:
                    try:
                        inbox.queue.get_nowait()
                        inbox.dropped += 1
                    except Empty:
                        break
                if _PIPELINE and inbox.invalid.is_set():
                    with _PIPELINE.lock:
                        if state.get('inbox') is inbox:
                            for asset in state['assets']:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.assets.pop(asset, None)

        def received(_sock, raw):
            if raw in ('PONG', b'PONG'):
                if hasattr(_sock, 'transport'):
                    _sock.transport.pong()
                return
            # No JSON, rate-balancer lock or Pipeline lock on the socket reader.
            # Raw ingress totals remain accurate even when the FIFO overflows;
            # group rates are sampled only across healthy drain windows below.
            began = time.monotonic()
            inbox.received_frames += 1
            inbox.received_bytes += len(raw)
            inbox.put(raw)
            inbox.callback_max_seconds = max(inbox.callback_max_seconds, time.monotonic() - began)

        def failed(_sock, error):
            inbox.delivery_gap = True
            state['connected'] = False
            code, reason = remote_close(error)
            if code is not None:
                state['remote_close'] = (code, reason)
                error = reason
            if hasattr(_sock, 'transport'):
                # on_error is called before the library clears app.sock. Capture
                # the last readable TCP state here, plus the preceding 60s.
                _sock.transport.sample(_sock)
                state['last_transport_failure'] = dict(
                    code=code, reason=str(error)[:160], assets=len(state['assets']),
                    sample=dict(_sock.transport.last))
                _LOG.warning('Polymarket WS transport failure shard=%s evidence=%s', index,
                             json.dumps(dict(state['last_transport_failure'], samples=list(_sock.transport.samples))))
            slow = 'slow consumer' in str(error).lower() or 'slow_consumer' in str(error).lower()
            if not slow or not state.get('slow_error'):
                _ws_error(error)
            if slow:
                if not state.get('slow_error'):
                    self.stats.increment('slow_consumer_count')
                state['slow_error'] = True

        def closed(_sock, code=None, reason=None):
            inbox.delivery_gap = True
            state['connected'] = False
            counted = state.pop('slow_error', False)
            remote_code, remote_reason = state.pop('remote_close', (None, None))
            code = code if code is not None else remote_code
            reason = reason if reason is not None else remote_reason
            with _LOCK:
                STATUS['last_close_code'] = code
            _LOG.info('Polymarket WS close shard=%s code=%s slow_consumer=%s', index, code, 'slow consumer' in str(reason).lower() or 'slow_consumer' in str(reason).lower())
            slow = 'slow consumer' in str(reason).lower() or 'slow_consumer' in str(reason).lower()
            if slow and not counted:
                self.stats.increment('slow_consumer_count')
            if (code is not None or reason is not None) and not (slow and counted):
                close_reason(code, reason)
            elif not counted and not self.closed and 'slow consumer' not in str(STATUS.get('last_error', '')).lower():
                close_reason(None, STATUS.get('last_error'))
            inbox.invalid.set()
            if time.monotonic() - state.get('opened_at', time.monotonic()) >= 60:
                state['attempts'] = 0
            state['retry_at'] = time.monotonic() + random.uniform(.5, 1.5) * min(120, 5 * 2 ** min(5, max(0, state['attempts'] - 1)))

        sock = read_ahead_app(websocket.WebSocketApp)(os.getenv('POLYMARKET_OBSERVER_WS_URL', MARKET_WS_URL),
                                     on_open=opened, on_message=received, on_error=failed, on_close=closed)
        state['socket'] = sock
        # websocket-client validates UTF-8 per byte in Python without wsaccel
        # (~50 us per price_change, ~0.5 ms per book) on the reading thread;
        # json.loads rejects invalid UTF-8 in C instead.
        state['thread'] = threading.Thread(target=lambda: sock.run_forever(ping_interval=10, skip_utf8_validation=True,
                                                                               sockopt=receive_buffer_options()),
                                           name=f'polymarket-receive-{index}', daemon=True)
        state['worker'] = threading.Thread(target=consume, name=f'polymarket-book-{index}', daemon=True)
        state['worker'].start()
        state['thread'].start()

    def tick(self):
        if self.closed or _STOP.is_set():
            return
        now = time.monotonic()
        tick_gap = now - self.tick_at
        self.tick_at = now
        self.tick_gap_max = max(self.tick_gap_max, tick_gap)
        # Group attribution runs off-reader. Freeze measurements/plans during
        # outages, overflow or backlog, plus one full recovery window; never
        # move a hot group because incomplete delivery made its rate look low.
        balancer = self.balancer
        first = not balancer.measured and not balancer.rates
        unhealthy = set()
        for state in self.states.values():
            inbox = state.get('inbox')
            bad = (bool(state.get('socket')) and not state['connected']) or (inbox and (
                inbox.delivery_gap or inbox.invalid.is_set() or inbox.resync.is_set() or
                (inbox.queue.qsize() and inbox.lag_seconds > 1)))
            if inbox:
                inbox.delivery_gap = False
            if bad:
                unhealthy.update(balancer.groups[a] for a in state['assets'] if a in balancer.groups)
        self.sample_unavailable.update(unhealthy)
        if (now - balancer.last_check >= balancer.interval
                or first and now - balancer.last_check >= min(20, balancer.interval)):
            balancer.last_check = now
            previous_unavailable = set(balancer.unavailable)
            balancer.sample(now, self.sample_unavailable)
            owners = balancer.current_owners()
            if (not self.sample_unavailable and not previous_unavailable and self.markets and balancer.measured
                    and set(owners) >= {balance_key(m) for m in self.markets}):
                target = balancer.plan(owners, now)
                if target:
                    self.apply_assignment(self.assignment_for(target))
            else:
                balancer.pressure, balancer.pressure_samples = None, 0
            self.sample_unavailable.clear()
        self.reconnect_pending = False
        for index, state in self.states.items():
            if state.get('inbox') and state['inbox'].invalid.is_set() and state['socket']:
                if not state.get('queue_logged'):
                    _LOG.warning('Polymarket WS generation invalidated shard=%s drops=%s', index, state['inbox'].dropped)
                    state['queue_logged'] = True
                state['socket'].close()
            if not state['thread'] or not state['thread'].is_alive():
                if now >= state['retry_at']:
                    self.connect(index, state)
            elif (state['connected'] and now >= state['ping_at']
                  and not getattr(state['socket'], 'text_heartbeat_active', False)):
                try:
                    state['socket'].send('PING')
                    state['ping_at'] = now + 10
                except Exception as exc:
                    state['connected'] = False
                    state['socket'].close()
                    _ws_error(exc)
        for state in self.states.values():
            inbox = state.get('inbox')
            if inbox and now > inbox.metric_at:
                elapsed = now - inbox.metric_at
                inbox.receive_rate = (inbox.received_frames - inbox.metric_frames) / elapsed
                inbox.byte_rate = (inbox.received_bytes - inbox.metric_bytes) / elapsed
                inbox.metric_frames, inbox.metric_bytes = inbox.received_frames, inbox.received_bytes
                inbox.metric_at = now
        cpu_now = process_time()
        cpu_cores = (cpu_now - self.cpu_seconds) / (now - self.cpu_at) if now > self.cpu_at else 0
        self.cpu_at, self.cpu_seconds = now, cpu_now
        with _LOCK:
            STATUS.setdefault('rebalance_count', 0)
            STATUS.update(self.balancer.metrics())
            STATUS.update(ws_process_cpu_cores=cpu_cores, ws_gil_switch_ms=sys.getswitchinterval() * 1000,
                          ws_tick_gap_seconds=tick_gap, ws_tick_gap_max_seconds=self.tick_gap_max)
            STATUS['ws_last_transport_failures'] = {i: s['last_transport_failure']
                for i, s in self.states.items() if 'last_transport_failure' in s}
            STATUS['ws_shard_assets'] = [len(self.states.get(i, {}).get('assets', [])) for i in range(self.count)]
            STATUS['ws_receive_queues'] = {
                i: dict(depth=s['inbox'].queue.qsize(), drops=s.get('receive_drops', 0) + s['inbox'].dropped,
                        high_water=s['inbox'].high_water, capacity=s['inbox'].queue.maxsize,
                        resyncs=s['inbox'].resyncs, lag_seconds=s['inbox'].lag_seconds,
                        queue_wait_seconds=s['inbox'].queue_wait_seconds,
                        lock_wait_seconds=s['inbox'].lock_wait_seconds,
                        receive_rate=s['inbox'].receive_rate, receive_bytes_per_second=s['inbox'].byte_rate,
                        received_frames=s['inbox'].received_frames, received_bytes=s['inbox'].received_bytes,
                        callback_max_seconds=s['inbox'].callback_max_seconds,
                        parse_max_seconds=s['inbox'].parse_max_seconds,
                        tcp_pending_bytes=socket_pending_bytes(s.get('socket')),
                        read_ahead_active=getattr(s.get('socket'), 'read_ahead_active', False),
                        read_calls=getattr(s.get('socket'), 'read_calls', 0),
                        transport=dict(getattr(getattr(s.get('socket'), 'transport', None), 'last', {})))
                for i, s in self.states.items() if 'inbox' in s}
            STATUS['ws_shards'] = self.count
            STATUS['ws_connected'] = sum(bool(s['connected']) for s in self.states.values())
            STATUS['connected'] = bool(self.states) and all(s['connected'] for s in self.states.values())

    def failures(self):
        return max((state['attempts'] for state in self.states.values() if not state['connected']), default=0)

    def close(self, timeout=5):
        deadline = time.monotonic() + timeout
        self.closed = True
        for state in list(self.states.values()):
            if state.get('inbox'):
                state['inbox'].invalid.set()
            if state['socket']:
                state['socket'].close()
        for state in list(self.states.values()):
            if state['thread']:
                state['thread'].join(max(0, deadline - time.monotonic()))
            if state.get('worker'):
                state['worker'].join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['ws_connected'] = 0
            STATUS['connected'] = False

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
from .ws_rotation import DataHealth, ShadowBooks, reconnect_delay
from .ws_subscriptions import SubscriptionPacer
from .ws_redundancy import FeedMerge, FeedContinuity, HotBackupCoverage
from .ws_balance import setting as ws_setting

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


HOT_SERIES = frozenset(f'up_down:{asset}:{interval}' for asset in ('BTC', 'ETH') for interval in ('5m', '15m'))


def activity_rates(rates):
    result = {}
    for key, rate in rates.items():
        base = key.split(':outcome:')[0]
        result[base] = result.get(base, 0) + rate
    return result


def live_markets(markets, now=None):
    now = now or datetime.now(timezone.utc)
    def live(market):
        if not market.get('endDate'):
            return True
        try:
            return updown.timestamp(market['endDate']) > now
        except ValueError:
            return False
    return [m for m in markets if live(m)]


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
                    markets = live_markets(markets)
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
                        connections.start_supervisor()
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
        category = ('slow_consumer' if 'slow consumer' in reason.lower() or 'slow_consumer' in reason.lower()
                    else 'invalid_subscription' if code == 1008
                    else 'server_shutdown' if code == 1001 else 'remote_close')
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
                      **self.recording.metrics(self.books,
                          {a: balance_key(self.markets[m]) for a, m in self.asset_markets.items()},
                          activity_rates(STATUS.get('ws_group_rates', {})), ws_setting('HOT_GROUP_MSG_PER_S', 100, 0)),
                      **self.references.metrics()}
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
    balancer.groups = {a: k for k, group in groups.items() for a in subscription_assets(group)}
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
        self.peak_byte_rate = 0
        self.peak_at, self.peak_bytes = self.metric_at, 0
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
                app.transport.read(len(data))
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
            self.subscription_ready = Event()

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
                    if now >= deadline and self.sock and self.subscription_ready.is_set():
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
        self.lifecycle_lock = RLock()
        self.standby = None
        self.backups = {}
        self.backup_next_connect_at = 0
        self.hot_backup_coverage = HotBackupCoverage(HOT_SERIES)
        self.merges = {}
        self.continuity = FeedContinuity()
        self.split_series = set()
        self.failover_ms = 0
        self.backup_serial = 0
        self.backup_promotions = 0
        self.rotation_last_failure = None
        self.retiring = []
        self.rotations = self.rotation_failures = 0
        self.standby_slow_consumers = 0
        self.supervisor_stop = Event()
        self.supervisor = None
        self.count = updown.setting('WS_SHARDS', 4, 1, 8)
        self.balancer = RateBalancer(self.count)
        self.subscriptions = SubscriptionPacer()
        self.stats = _DAILY_STATS
        self.reconnect_pending = False
        self.sample_unavailable = set()
        self.cpu_at, self.cpu_seconds = time.monotonic(), process_time()
        self.tick_at = self.cpu_at
        self.tick_gap_max = 0
        self.closed = False
        self.markets = []

    def sync(self, markets):
        with self.lifecycle_lock:
            self._sync(markets)

    def _sync(self, markets):
        self.markets = markets = live_markets(markets)
        target = self.balancer.max_assets
        hot = {balance_key(m) for m in markets} & HOT_SERIES
        cold_assets = sum(2 for m in markets if balance_key(m) not in hot)
        self.count = max(self.count, min(8, len(hot) + (cold_assets + target - 1) // target),
                         min(8, (len(subscription_assets(markets)) + target - 1) // target))
        self.balancer.count = self.count
        self.balancer.set_groups({a: self.asset_key(m, side) for m in markets for side, a in
                                  (('yes', m['yesAssetId']), ('no', m['noAssetId']))},
                                 time.monotonic())
        self.balancer.forced_hot = {k for k in self.balancer.groups.values() if k.split(':outcome:')[0] in HOT_SERIES}
        self.continuity.prune(set(subscription_assets(markets)))
        self.split_series.intersection_update({balance_key(m) for m in markets})
        if set(subscription_assets(markets)) == set(self.balancer.assignment):
            self.tick()
            return
        # Rollover keeps each series on its shard: only changed assets move.
        owners = self.capacity_owners()
        assignment = self.assignment_for(owners)
        self.apply_assignment(assignment)
        self.tick()

    def start_supervisor(self):
        """Keep recovery/rotation independent of discovery, retention and status SQL."""
        if self.supervisor is not None:
            return
        def supervise():
            while not self.supervisor_stop.wait(.2) and not _STOP.is_set():
                try:
                    self.tick()
                except Exception:
                    _LOG.exception('Polymarket WS supervisor failed')
        self.supervisor = threading.Thread(target=supervise, name='polymarket-supervisor', daemon=True)
        self.supervisor.start()

    def seed_rates(self, rates):
        """Prior weights place a cold start by load; they never trigger moves."""
        with self.balancer.lock:
            for key, rate in rates.items():
                self.balancer.rates.setdefault(key, rate)

    def asset_key(self, market, side):
        key = balance_key(market)
        return f'{key}:outcome:{side}' if key in self.split_series else key

    def hot_shard(self, index):
        return any(self.balancer.groups.get(a, '').split(':outcome:')[0] in HOT_SERIES
                   for a in self.states[index]['assets'])

    def assignment_for(self, owners):
        return {a: owners[k] for a, k in self.balancer.groups.items()}

    def split_byte_overloads(self):
        # Outcome legs can use different sockets; pair evaluation still uses
        # the shared Pipeline. Preserve side keys/rates across window rollovers.
        b = self.balancer
        oversized = {k for k in b.groups.values() if ':outcome:' not in k and b.byte_weight(k) > b.max_bytes
                     and sum(v == k for v in b.groups.values()) > 1}
        if not oversized:
            return
        self.split_series.update(oversized)
        old_groups = dict(b.groups)
        groups = {a: self.asset_key(m, side) for m in self.markets for side, a in
                  (('yes', m['yesAssetId']), ('no', m['noAssetId']))}
        for key in oversized:
            parts = {v for a, v in groups.items() if old_groups.get(a) == key}
            for part in parts:
                b.rates[part] = b.weight(key) / len(parts)
                b.byte_rates[part] = b.byte_weight(key) / len(parts)
                if key in b.measured:
                    b.measured.add(part)
        b.set_groups(groups)
        hot_keys = {k for k in groups.values() if k.split(':outcome:')[0] in HOT_SERIES}
        cold_assets = sum(k.split(':outcome:')[0] not in HOT_SERIES for k in groups.values())
        self.count = b.count = max(self.count, min(8, len(hot_keys) +
                                      (cold_assets + b.max_assets - 1) // b.max_assets))
        b.forced_hot = {k for k in groups.values() if k.split(':outcome:')[0] in HOT_SERIES}

    def capacity_owners(self):
        """Grow within the eight-socket bound when whole groups do not fit."""
        balancer = self.balancer
        self.split_byte_overloads()
        keys = set(balancer.groups.values())
        owners = balancer.place(keys)
        owners, _, _ = balancer.search(owners, min(32, len(keys)))
        while any(balancer.capacity_cost(owners)) and self.count < 8:
            before = balancer.capacity_cost(owners)
            self.count += 1
            balancer.count = self.count
            trial = balancer.place(keys, owners)
            if balancer.capacity_cost(trial) >= before:
                # One indivisible series can exceed a cap; extra idle sockets
                # cannot help. Report the excess instead of growing forever.
                self.count -= 1
                balancer.count = self.count
                break
            owners = trial
        return owners

    def apply_assignment(self, assignment):
        with self.lifecycle_lock:
            if self.standby and assignment != self.balancer.assignment:
                self.cancel_rotation('assignment_changed')
            return self._apply_assignment(assignment)

    def _apply_assignment(self, assignment):
        self.update_hot_coverage(time.monotonic())
        assignments = {i: [a for a, shard in assignment.items() if shard == i] for i in range(self.count)}
        # Serialize publication with consumers. Retained assets keep both books
        # and queued deltas; moved assets require a new snapshot on their owner.
        pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
        with pipeline_lock:
            old = self.balancer.assignment
            old_owners = self.balancer.current_owners()
            moved = any(a in assignment and assignment[a] != i for a, i in old.items())
            changed = {a for a in old.keys() | assignment.keys() if old.get(a) != assignment.get(a)}
            for asset in changed & old.keys() & assignment.keys():
                self.continuity.switch(self.states[old[asset]].get('source'), None, [asset], 'reassignment')
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
                    if 'subscribe_pending' in state:
                        state['subscribe_pending'][:] = [a for a in state['subscribe_pending'] if a in assets]
                    if changes[index] and state.get('data_health'):
                        state['data_health'] = DataHealth(time.monotonic())
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
                                    if operation == 'subscribe':
                                        self.subscriptions.enqueue(state, command['assets_ids'])
                                        self.subscriptions.drain(state, time.monotonic())
                                    else:
                                        if not state.get('subscribe_initial'):
                                            state['socket'].send(json.dumps(command))
                                except Exception as exc:
                                    # A real wire failure affects this socket only.
                                    state['connected'] = False
                                    if state.get('inbox'):
                                        state['inbox'].invalid.set()
                                    state['socket'].close()
                                    _ws_error(exc)
                                    break

        for index, candidate in list(self.backups.items()):
            assets = self.states[index]['assets']
            if not assets or not self.hot_shard(index):
                self.backups.pop(index)
                self.retire(candidate)
                continue
            commands = subscription_changes(candidate['assets'], assets)
            with pipeline_lock, candidate['inbox'].lock:
                candidate['assets'] = list(assets)
                if 'subscribe_pending' in candidate:
                    candidate['subscribe_pending'][:] = [a for a in candidate['subscribe_pending'] if a in assets]
                shadow = candidate['shadow']
                shadow.assets = frozenset(assets)
                shadow.books = {a: b for a, b in shadow.books.items() if a in assets}
                shadow.recorder.prune(assets)
                candidate['ready'] = shadow.books.keys() == shadow.assets
            for command in commands:
                try:
                    if command['operation'] == 'unsubscribe':
                        candidate['socket'].send(json.dumps(command))
                    else:
                        self.subscriptions.enqueue(candidate, command['assets_ids'])
                except Exception as exc:
                    candidate['connected'] = False
                    candidate['inbox'].invalid.set()
                    self.hot_backup_coverage.unready(index, time.monotonic())
                    _LOG.warning('Polymarket WS backup subscription failed shard=%s reason=%s', index, str(exc)[:160])
                    break
            with pipeline_lock:
                merge = self.merges.get(index)
                if merge:
                    merge.latest = {a: at for a, at in merge.latest.items() if a in assets}
                    merge.snapshots = {a: at for a, at in merge.snapshots.items() if a in assets}

        if moved:
            self.balancer.last_rebalance = time.monotonic()
            self.stats.increment('rebalance_count')
            with _LOCK:
                STATUS['rebalance_count'] = STATUS.get('rebalance_count', 0) + 1
        self.update_hot_coverage(time.monotonic())
        return True

    def retire(self, state):
        """Wake readers without waiting for a network close handshake on the book worker."""
        state['retired'] = True
        self.subscriptions.forget(state)
        if state.get('inbox'):
            state['inbox'].invalid.set()
        sock = state.get('socket')
        if sock:
            sock.keep_running = False
            wire = getattr(sock, 'sock', None)
            if wire and hasattr(wire, 'shutdown'):
                wire.shutdown()
            else:
                sock.close()
        self.retiring.append(state)

    def cancel_rotation(self, reason):
        pending, self.standby = self.standby, None
        if pending:
            index, candidate = pending
            self.retire(candidate)
            active = self.states.get(index)
            if active and reason == 'assignment_changed':
                # Let the refreshed subscriptions settle before another age
                # rotation. Cancellation is routine, not a preparation failure.
                active['rotate_retry_at'] = time.monotonic() + 30
            if active and candidate.get('normal_shutdown'):
                active['rotate_retry_at'] = time.monotonic() + reconnect_delay(1)
            if reason not in {'shutdown', 'assignment_changed'} and not candidate.get('normal_shutdown'):
                self.rotation_failed(index, active, candidate, reason)

    def rotation_failed(self, index, active, candidate, reason):
        prefix = 'backup' if candidate.get('redundant') else 'rotate'
        failures = active.get(f'{prefix}_failures', 0) + 1 if active else 1
        delay = reconnect_delay(failures) if candidate.get('redundant') else min(480, 60 * 2 ** min(3, failures - 1))
        if active:
            active.update({f'{prefix}_failures': failures, f'{prefix}_retry_at': time.monotonic() + delay})
        self.rotation_failures += 1
        self.rotation_last_failure = dict(shard=index, reason=reason, stage=self.prepare_stage(candidate),
                                          retry_seconds=delay, consecutive_failures=failures)
        _LOG.info('Polymarket WS rotation aborted details=%s', json.dumps(self.rotation_last_failure))

    def prepare_stage(self, state):
        if state.get('failure_stage'):
            return state['failure_stage']
        if not state.get('opened_at'):
            return 'connect'
        if state.get('subscribe_pending'):
            return 'subscribe'
        if self.subscriptions.pressured(state):
            return 'ingress_pressure'
        shadow = state.get('shadow')
        if shadow and shadow.books.keys() != shadow.assets:
            return 'snapshots'
        if not self.subscriptions.settled(state, time.monotonic()):
            return 'pong_or_warmup'
        return 'catchup_or_publication'

    def busy(self, index, state):
        inbox = state.get('inbox')
        return (sum(self.balancer.weight(k) for k, i in self.balancer.current_owners().items() if i == index)
                >= ws_setting('HOT_SHARD_MSG_PER_S', 300, 1)
                or self.balancer.byte_loads()[index] >= ws_setting('HOT_SHARD_BYTES_PER_S', 262144, 1)
                or inbox is not None and (inbox.receive_rate >= ws_setting('HOT_SHARD_MSG_PER_S', 300, 1)
                    or inbox.byte_rate >= ws_setting('HOT_SHARD_BYTES_PER_S', 262144, 1)))

    def redundancy_tick(self, now):
        for index, candidate in list(self.backups.items()):
            old = self.states.get(index)
            # Traffic pressure, missing PONG and elapsed warmup are not wire
            # failures. A hot standby remains online and keeps consuming.
            # A handshake can fail/return without an on_close callback. Also
            # bound an unfinished handshake; hot snapshot warmup stays online.
            failed = (candidate['inbox'].invalid.is_set() or candidate.get('failed_at') is not None
                      or not candidate['thread'].is_alive() or not candidate['worker'].is_alive()
                      or candidate.get('initial_sent_at') is None and now - candidate['created_at'] >= 15)
            if not old or failed:
                self.hot_backup_coverage.unready(index, now)
                self.backups.pop(index)
                if not candidate.get('normal_shutdown'):
                    self.rotation_failed(index, old, candidate, 'backup_wire_failure')
                elif old:
                    old['backup_retry_at'] = now + reconnect_delay(1)
                self.retire(candidate)
            elif candidate.get('initial_sent_at') is not None:
                # Handshake completion can lag the dial. Pace from the actual
                # first subscription, so delayed handshakes cannot bunch up.
                self.backup_next_connect_at = max(self.backup_next_connect_at,
                    candidate['initial_sent_at'] + candidate['admission_seconds'])
        dialing = any(s.get('initial_sent_at') is None for s in self.backups.values())
        # Give uncovered new groups a turn before retrying an older pair.
        for index, old in sorted(self.states.items(), key=lambda item: (bool(item[1].get('backup_started')), item[0])):
            if not self.hot_shard(index):
                continue
            health = old.get('data_health')
            transport = getattr(getattr(old.get('socket'), 'transport', None), 'last', {})
            if health and health.sample(now, transport):
                old['data_suspect'] = True
                old.setdefault('failed_at', now)
                old.setdefault('recover_assets', set()).update(old['assets'])
            candidate = self.backups.get(index)
            if candidate:
                health = candidate.get('data_health')
                transport = getattr(getattr(candidate.get('socket'), 'transport', None), 'last', {})
                if health and health.sample(now, transport):
                    candidate.setdefault('recover_assets', set()).update(candidate['assets'])
            stagger = old.setdefault('backup_stagger_seconds', random.uniform(90, 120))
            if (dialing or index in self.backups or not old['connected'] or not old['assets']
                    or old.get('subscribe_initial', True) or not old.get('inbox') or old['inbox'].invalid.is_set()
                    or now < old.get('backup_retry_at', 0) or now < self.backup_next_connect_at
                    or old.get('backup_started') and now - old.get('opened_at', now) < stagger):
                continue
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock:
                self.merges.setdefault(index, FeedMerge({a: v for a, v in
                    (_PIPELINE.recording.metadata.items() if _PIPELINE else []) if a in old['assets']}))
            self.backup_serial += 1
            candidate = dict(assets=list(old['assets']), socket=None, thread=None, connected=False,
                lock=RLock(), attempts=0, retry_at=0, ping_at=0, standby=True, redundant=True,
                busy=True, source=f'backup-{self.backup_serial}', shadow=ShadowBooks(old['assets']), created_at=now)
            self.backups[index] = candidate
            # First redundancy is eager, including newly allocated hot shards.
            # Later attempts preserve the survivor's 90–120s target age. Pace
            # initial batches too, which necessarily bypass the shared queue.
            old['backup_started'] = True
            initial = max(int(ws_setting('SUBSCRIBE_BATCH_ASSETS', 2, 1)),
                          int(ws_setting('SUBSCRIBE_INITIAL_ASSETS', 12, 1)))
            candidate['admission_seconds'] = max(ws_setting('SUBSCRIBE_BATCH_SECONDS', .5, .05),
                min(initial, len(candidate['assets'])) / ws_setting('SUBSCRIBE_ASSETS_PER_S', 4, .1))
            self.backup_next_connect_at = now + candidate['admission_seconds']
            _LOG.info('Polymarket WS redundant prepare shard=%s assets=%s', index, len(old['assets']))
            self.connect(index, candidate)
            dialing = True

    def update_hot_coverage(self, now):
        required = {}
        for asset, index in self.balancer.assignment.items():
            key = self.balancer.groups.get(asset, '').split(':outcome:')[0]
            if key in HOT_SERIES:
                required.setdefault(key, set()).add(index)
        ready = set()
        for index, candidate in self.backups.items():
            old = self.states.get(index, {})
            if (candidate.get('ready') and candidate['connected'] and old.get('connected')
                    and not old.get('data_suspect') and not old['inbox'].invalid.is_set()
                    and not candidate['inbox'].invalid.is_set() and not candidate['inbox'].resync.is_set()
                    and not candidate.get('subscribe_pending') and not candidate.get('recover_assets')):
                ready.add(index)
        return self.hot_backup_coverage.update(required, ready, now)

    def redundant_receive(self, index, state, event, received_at, inbox):
        with self.lifecycle_lock:
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock, inbox.lock:
                if (self.backups.get(index) is not state or inbox.invalid.is_set() or inbox.resync.is_set()
                        or state.get('retired')):
                    return
                event = recording.owned_events(event, {a for a in state['assets']
                    if received_at >= getattr(inbox, 'activated', {}).get(a, received_at)})
                shadow = state['shadow']
                shadow.receive(event, received_at)
                self.continuity.receive(recording.owned_events(event, set(state['assets'])), state['source'], received_at)
                if _PIPELINE:
                    merged = self.merges[index].filter(event, state['source'], shadow.assets, backup=True)
                    if merged:
                        _PIPELINE.receive(merged, shadow.assets, received_at, inbox, inbox.epoch)
                old = self.states[index]
                # No simultaneous all-asset timestamp equality barrier on busy
                # feeds. Each asset has its own full snapshot; the merge guards
                # stale snapshots/levels independently, while retaining trades.
                ready = (state['connected'] and shadow.books.keys() == shadow.assets
                         and all(row[0] is not None for row in shadow.recorder.metadata.values())
                         and not state.get('subscribe_pending')
                         and inbox.queue_wait_seconds <= 1 and not inbox.delivery_gap
                         and (datetime.now(timezone.utc) - received_at).total_seconds() <= 1)
                if _PIPELINE:
                    # After live invalidation, do not restore an older shadow
                    # snapshot over the retained merge watermark.
                    ready = ready and all(
                        self.merges[index].latest.get(a) is None
                        or shadow.recorder.metadata.get(a, (None,))[0] is not None
                        and shadow.recorder.metadata[a][0] >= self.merges[index].latest[a]
                        for a in shadow.assets - _PIPELINE.books.keys())
                if not ready:
                    if inbox.queue_wait_seconds <= 1 and inbox.queue.empty():
                        inbox.delivery_gap = False
                    if state.get('ready'):
                        state['ready'] = False
                        self.update_hot_coverage(time.monotonic())
                    return
                # Reset retry escalation only on usable full snapshots.
                old['backup_failures'] = 0
                if not state.get('ready'):
                    state['ready'] = True
                    self.update_hot_coverage(time.monotonic())
                maximum = ws_setting('HOT_ROTATE_SECONDS', 0, 0)
                due = maximum > 0 and time.monotonic() - old.get('opened_at', time.monotonic()) >= maximum
                if old['connected'] and not old['inbox'].invalid.is_set() and not old.get('data_suspect') and not due:
                    return
                # Publish usable backup snapshots only for missing live assets;
                # existing merged books continue without reset or replay.
                if _PIPELINE:
                    missing = shadow.assets - _PIPELINE.books.keys()
                    for asset in missing:
                        at, receipt, _ = shadow.recorder.metadata[asset]
                        book = shadow.books[asset]
                        _PIPELINE.receive(dict(event_type='book', asset_id=asset, timestamp=int(at.timestamp() * 1000),
                            bids=[dict(price=p, size=q) for p, q in book['bids'].items()],
                            asks=[dict(price=p, size=q) for p, q in book['asks'].items()]),
                            shadow.assets, receipt, inbox, inbox.epoch)
                self.check_continuity(index, old, state, 'failover' if not old['connected'] else 'data_collapse' if old.get('data_suspect') else 'hot_age')
                self.failover_ms = max(0, (time.monotonic() - old.get('failed_at', time.monotonic())) * 1000)
                state.update(standby=False, redundant=False, rotate_failures=0, rotate_retry_at=0,
                             backup_started=True, backup_failures=0, backup_retry_at=0)
                state.pop('shadow')
                self.states[index] = state
                del self.backups[index]
                self.update_hot_coverage(time.monotonic())
                self.backup_promotions += 1
                self.rotations += 1
                _LOG.info('Polymarket WS redundant promoted shard=%s reason=%s', index, 'hot_age' if due else 'data_collapse' if old.get('data_suspect') else 'failover')
                self.retire(old)
                # Replenish when the survivor reaches its target age, including
                # an eager startup backup that has just been promoted.
                self.redundancy_tick(time.monotonic())

    def check_continuity(self, index, old, new, reason):
        assets = self.continuity.switch(old.get('source'), new.get('source'), new['assets'], reason)
        if assets:
            new.setdefault('recover_assets', set()).update(assets)

    def rotation_tick(self, now):
        self.retiring = [s for s in self.retiring if any(
            s.get(key) and s[key].is_alive() for key in ('thread', 'worker'))]
        if self.standby:
            index, candidate = self.standby
            if (candidate.get('retired') or candidate.get('inbox') and candidate['inbox'].invalid.is_set()
                    or now - candidate['created_at'] >= ws_setting('ROTATE_PREPARE_SECONDS', 90, 1)):
                self.cancel_rotation('failed_or_timeout')
            elif candidate.get('inbox'):
                self.subscriptions.settled(candidate, now)
            return
        # One rotating candidate in addition to bounded hot backups. Bound
        # stuck teardown threads too.
        if len(self.retiring) >= self.count * 2:
            return
        for index, state in list(self.states.items()):
            if not state['connected'] or not state['assets'] or now < state.get('rotate_retry_at', 0):
                continue
            if self.hot_shard(index) or not ws_setting('COLD_ROTATION_ENABLED', 0, 0):
                continue  # Permanent hot fan-in; no cold standby in normal operation.
            health = state.get('data_health')
            transport = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
            collapsed = health.sample(now, transport) if health else False
            if index in self.backups:
                continue
            busy = self.hot_shard(index)
            maximum = ws_setting('HOT_ROTATE_SECONDS', 0, 0) if busy else ws_setting('ROTATE_SECONDS', 0, 0)
            due = maximum > 0 and now - state.get('opened_at', now) >= maximum + (0 if busy else index * 10)
            if not due and not (collapsed and busy):
                continue
            candidate = dict(assets=list(state['assets']), socket=None, thread=None,
                             connected=False, lock=RLock(), attempts=0, retry_at=0, ping_at=0,
                             standby=True, shadow=ShadowBooks(state['assets']), created_at=now,
                             rotation_reason='age' if due else 'data_collapse')
            candidate['busy'] = self.busy(index, state) or len(state['assets']) >= self.balancer.max_assets
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock:
                if _PIPELINE:
                    candidate['shadow'].observe_active(_PIPELINE.recording.metadata)
                self.standby = (index, candidate)
            _LOG.info('Polymarket WS rotation prepare shard=%s reason=%s age_seconds=%.3f',
                      index, candidate['rotation_reason'], now - state.get('opened_at', now))
            self.connect(index, candidate)
            break

    def shadow_receive(self, index, state, event, received_at, inbox):
        if state.get('redundant'):
            return self.redundant_receive(index, state, event, received_at, inbox)
        # Same order as assignment/tick: lifecycle -> pipeline. No live deltas
        # can cross publication, and the old generation cannot clear new books.
        with self.lifecycle_lock:
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock, inbox.lock:
                if state.get('retired') or self.standby != (index, state) or inbox.invalid.is_set():
                    return
                event = recording.owned_events(event, {a for a in state['assets']
                    if received_at >= getattr(inbox, 'activated', {}).get(a, received_at)})
                shadow = state['shadow']
                shadow.receive(event, received_at)
                self.continuity.receive(recording.owned_events(event, set(state['assets'])), state['source'], received_at)
                old = self.states[index]
                metadata = _PIPELINE.recording.metadata if _PIPELINE else {}
                if (not state['connected'] or not self.subscriptions.settled(state, time.monotonic())
                        or inbox.resync.is_set() or inbox.delivery_gap
                        or inbox.queue_wait_seconds > 1
                        or (datetime.now(timezone.utc) - received_at).total_seconds() > 1
                        or not shadow.ready(metadata)
                        or set(old['assets']) != shadow.assets
                        or _PIPELINE and not shadow.assets <= _PIPELINE.asset_markets.keys()):
                    return
                # Publish fully warmed books and their original exchange/receipt
                # times together. Standby trades never enter persistence twice.
                if _PIPELINE:
                    for asset, book in shadow.books.items():
                        at, receipt, _ = shadow.recorder.metadata[asset]
                        _PIPELINE.receive(dict(event_type='book', asset_id=asset,
                            timestamp=int(at.timestamp() * 1000),
                            bids=[dict(price=p, size=q) for p, q in book['bids'].items()],
                            asks=[dict(price=p, size=q) for p, q in book['asks'].items()]),
                            shadow.assets, receipt, inbox, inbox.epoch)
                state['standby'] = False
                self.check_continuity(index, old, state, 'rotation')
                state.pop('shadow')
                state['rotate_retry_at'] = time.monotonic() + 60
                state['rotate_failures'] = 0
                old['inbox'].invalid.set()
                old['retired'] = True
                self.states[index] = state
                self.standby = None
                self.rotations += 1
                _LOG.info('Polymarket WS rotation promoted shard=%s reason=%s old_age_seconds=%.3f',
                          index, state['rotation_reason'], time.monotonic() - old.get('opened_at', time.monotonic()))
            self.retire(old)

    def connect(self, index, state):
        import websocket
        if state['socket']:
            self.continuity.switch(state.get('source'), None, state['assets'], 'reconnect')
            if state.get('inbox'):
                state['inbox'].invalid.set()
            state['socket'].close()
            if state['thread'] and state['thread'].is_alive():
                state['thread'].join(5)
            if state.get('worker') and state['worker'].is_alive():
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
        state.pop('opened_at', None)
        state.pop('initial_sent_at', None)
        state.pop('failed_at', None)
        state.pop('data_suspect', None)
        state.pop('subscribe_pause_until', None)
        state.pop('normal_shutdown', None)
        state.pop('failure_stage', None)
        state.pop('backup_stagger_seconds', None)
        state.pop('slow_error', None)
        state.pop('close_evidence_recorded', None)
        if not state.get('standby') and index in self.merges:
            # This generation needs fresh snapshots; a previous generation's
            # duplicate cache must not suppress them after its books were reset.
            self.merges[index] = FeedMerge({})
        self.subscriptions.forget(state)
        self.backup_serial += 1
        state['source'] = f'feed-{self.backup_serial}'
        state['attempts'] += 1
        inbox = ReceiveQueue(updown.setting('WS_RECEIVE_QUEUE_MAX', 2048, 1, 100000),
                             updown.setting('WS_RECEIVE_QUEUE_CEILING', 16384, 1, 100000))
        state['receive_drops'] = state.get('receive_drops', 0) + (state['inbox'].dropped if state.get('inbox') else 0)
        state['inbox'] = inbox
        inbox.activated = {a: datetime.max.replace(tzinfo=timezone.utc) for a in state['assets']}
        state['queue_logged'] = False
        state['retry_at'] = time.monotonic() + reconnect_delay(state['attempts'])
        state['data_health'] = DataHealth(time.monotonic())

        def opened(sock):
            if self.closed or state.get('retired') or state.get('inbox') is not inbox or _STOP.is_set():
                sock.close()
                return
            with state['lock']:
                state['connected'] = True
                state['opened_at'] = time.monotonic()
                state['ping_at'] = time.monotonic() + 10
                state['warm_since'] = None
                state['snapshot_retry_at'] = time.monotonic() + 10
                self.subscriptions.enqueue(state, state['assets'], initial=True)
                self.subscriptions.drain(state, time.monotonic())
                state['initial_sent_at'] = time.monotonic()
            if hasattr(sock, 'transport'):
                sock.transport.capture_headers(sock)
            _LOG.info('Polymarket WS open shard=%s standby=%s assets=%s', index,
                      bool(state.get('standby')), len(state['assets']))
            if not state.get('standby'):
                _ws_connected()
            with _LOCK:
                STATUS['ws_connections'] = STATUS.get('ws_connections', 0) + 1

        def process_frames():
            while not self.closed and state.get('inbox') is inbox and not inbox.invalid.is_set():
                if inbox.resync.is_set():
                    if state.get('standby'):
                        if not state.get('redundant'):
                            inbox.invalid.set()
                            break
                        with self.lifecycle_lock, inbox.lock:
                            self.hot_backup_coverage.unready(index, time.monotonic())
                            inbox.discard()
                            state['shadow'] = ShadowBooks(state['assets'])
                            state['ready'] = False
                            inbox.resync.clear()
                            inbox.delivery_gap = False
                            inbox.resyncs += 1
                            state.setdefault('recover_assets', set()).update(state['assets'])
                        continue
                    # Serialize gap recovery with publication. Never reuse a book
                    # assembled across a missing delta; request fresh full books.
                    pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
                    with pipeline_lock, inbox.lock:
                        if (inbox.invalid.is_set() or state.get('inbox') is not inbox
                                or self.states.get(index) is not state or state.get('retired')):
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
                        inbox.activated = {a: datetime.max.replace(tzinfo=timezone.utc) for a in state['assets']}
                        self.subscriptions.enqueue(state, state['assets'])
                    _LOG.warning('Polymarket WS snapshot recovery shard=%s drops=%s', index, inbox.dropped)
                try:
                    raw, received_at, epoch = inbox.queue.get(timeout=.2)
                    inbox.queue_wait_seconds = (datetime.now(timezone.utc) - received_at).total_seconds()
                    inbox.lag_seconds = inbox.queue_wait_seconds
                    if inbox.queue_wait_seconds > 1:
                        inbox.delivery_gap = True
                except Empty:
                    if state.get('standby'):
                        # Only this worker promotes. A supervisor promotion
                        # could otherwise strand an already-popped shadow delta.
                        self.shadow_receive(index, state, [], datetime.now(timezone.utc), inbox)
                    continue
                try:
                    parsing_at = time.monotonic()
                    event = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
                    inbox.parse_max_seconds = max(inbox.parse_max_seconds, time.monotonic() - parsing_at)
                    if state.get('standby'):
                        self.shadow_receive(index, state, event, received_at, inbox)
                        continue
                    self.balancer.receive(event, index, len(raw.encode('utf-8')) if isinstance(raw, str) else len(raw) if isinstance(raw, (bytes, bytearray)) else 0)
                    self.stats.message(index)
                    if _PIPELINE:
                        # Recheck generation under the same lock used for invalidation.
                        waiting_at = time.monotonic()
                        with _PIPELINE.lock:
                            lock_wait = time.monotonic() - waiting_at
                            valid = (not inbox.invalid.is_set() and state.get('inbox') is inbox
                                     and self.states.get(index) is state and not state.get('retired'))
                            if valid:
                                continuity_assets = {a for a in state['assets']
                                    if received_at >= inbox.activated.get(a, received_at)}
                                self.continuity.receive(recording.owned_events(event, continuity_assets), state['source'], received_at)
                                owned = frozenset(state['assets'])
                                merge = self.merges.get(index)
                                merged = merge.filter(event, state['source'], owned) if merge else event
                                if merged:
                                    _PIPELINE.receive(merged, owned, received_at, inbox, epoch)
                                pending = self.standby
                                if pending and pending[0] == index:
                                    pending[1]['shadow'].observe_active(_PIPELINE.recording.metadata)
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
                        backup = self.backups.get(index)
                        if (state.get('inbox') is inbox and self.states.get(index) is state
                                and not (backup and backup.get('ready') and backup['connected']
                                         and not backup['inbox'].invalid.is_set())):
                            for asset in state['assets']:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.assets.pop(asset, None)

        def received(_sock, raw):
            if state.get('retired') or state.get('inbox') is not inbox:
                return
            if raw in ('PONG', b'PONG'):
                if hasattr(_sock, 'transport'):
                    _sock.transport.pong()
                return
            # No JSON, rate-balancer lock or Pipeline lock on the socket reader.
            # Raw ingress totals remain accurate even when the FIFO overflows;
            # group rates are sampled only across healthy drain windows below.
            raw_bytes = len(raw.encode('utf-8')) if isinstance(raw, str) else len(raw)
            state['data_health'].total += raw_bytes
            began = time.monotonic()
            inbox.received_frames += 1
            inbox.received_bytes += raw_bytes
            inbox.put(raw)
            inbox.callback_max_seconds = max(inbox.callback_max_seconds, time.monotonic() - began)

        def evidence(_sock, code, reason):
            if state.get('close_evidence_recorded'):
                return
            state['close_evidence_recorded'] = True
            transport = getattr(_sock, 'transport', None)
            if transport:
                transport.sample(_sock)
            state['last_transport_failure'] = dict(
                code=code, reason=str(reason or '')[:160], assets=len(state['assets']),
                connection_age_seconds=max(0, time.monotonic() - state.get('opened_at', time.monotonic())),
                standby=bool(state.get('standby')), subscribe_pending=len(state.get('subscribe_pending', [])),
                subscribe_batches=state.get('subscribe_batches', 0),
                handshake_headers=dict(transport.headers) if transport else {},
                sample=dict(transport.last) if transport else {})
            _LOG.log(logging.INFO if code == 1001 else logging.WARNING,
                     'Polymarket WS transport failure shard=%s evidence=%s', index,
                     json.dumps(dict(state['last_transport_failure'], samples=list(transport.samples) if transport else [])))

        def failed(_sock, error):
            if state.get('retired') or state.get('inbox') is not inbox:
                return
            inbox.delivery_gap = True
            state['connected'] = False
            state.setdefault('failed_at', time.monotonic())
            self.hot_backup_coverage.unready(index, time.monotonic())
            code, reason = remote_close(error)
            if code is not None:
                state['remote_close'] = (code, reason)
                error = reason
            state['failure_stage'] = self.prepare_stage(state)
            evidence(_sock, code, error)
            if code == 1001:
                state['normal_shutdown'] = True
                return
            if state.get('standby'):
                if ('slow consumer' in str(error).lower() or 'slow_consumer' in str(error).lower()) and not state.get('slow_error'):
                    self.standby_slow_consumers += 1
                    state['slow_error'] = True
                _LOG.warning('Polymarket WS standby failure shard=%s reason=%s', index, str(error)[:160])
                inbox.invalid.set()
                return
            slow = 'slow consumer' in str(error).lower() or 'slow_consumer' in str(error).lower()
            if not slow or not state.get('slow_error'):
                _ws_error(error)
            if slow:
                if not state.get('slow_error'):
                    self.stats.increment('slow_consumer_count')
                state['slow_error'] = True

        def closed(_sock, code=None, reason=None):
            if state.get('inbox') is not inbox:
                return
            remote_code, remote_reason = state.pop('remote_close', (None, None))
            code = code if code is not None else remote_code
            reason = reason if reason is not None else remote_reason
            if not state.get('retired'):
                evidence(_sock, code, reason)
            if code == 1001 and not state.get('retired'):
                state['normal_shutdown'] = True
                close_reason(code, reason)
            if state.get('standby') and not state.get('retired'):
                slow = 'slow consumer' in str(reason).lower() or 'slow_consumer' in str(reason).lower()
                if slow and not state.get('slow_error'):
                    self.standby_slow_consumers += 1
                    state['slow_error'] = True
                _LOG.info('Polymarket WS standby close shard=%s code=%s slow_consumer=%s', index, code, slow)
            if state.get('retired') or state.get('standby'):
                state['connected'] = False
                inbox.invalid.set()
                if not state.get('retired'):
                    state.setdefault('failed_at', time.monotonic())
                    self.hot_backup_coverage.unready(index, time.monotonic())
                return
            inbox.delivery_gap = True
            state['connected'] = False
            state.setdefault('failed_at', time.monotonic())
            self.hot_backup_coverage.unready(index, time.monotonic())
            counted = state.pop('slow_error', False)
            with _LOCK:
                STATUS['last_close_code'] = code
            _LOG.info('Polymarket WS close shard=%s code=%s slow_consumer=%s age_seconds=%.3f',
                      index, code, 'slow consumer' in str(reason).lower() or 'slow_consumer' in str(reason).lower(),
                      time.monotonic() - state.get('opened_at', time.monotonic()))
            slow = 'slow consumer' in str(reason).lower() or 'slow_consumer' in str(reason).lower()
            if slow and not counted:
                self.stats.increment('slow_consumer_count')
            if code == 1001:
                pass
            elif (code is not None or reason is not None) and not (slow and counted):
                close_reason(code, reason)
            elif not counted and not self.closed and 'slow consumer' not in str(STATUS.get('last_error', '')).lower():
                close_reason(None, STATUS.get('last_error'))
            inbox.invalid.set()
            if time.monotonic() - state.get('opened_at', time.monotonic()) >= 60:
                state['attempts'] = 1
            if code == 1001:
                state['attempts'] = 1
            state['retry_at'] = time.monotonic() + reconnect_delay(state['attempts'])

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
        with self.lifecycle_lock:
            self._tick()

    def _tick(self):
        if self.closed or _STOP.is_set():
            return
        live = live_markets(self.markets)
        if len(live) != len(self.markets):
            if _PIPELINE:
                _PIPELINE.set_markets(live)
            self._sync(live)
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
            bad = (bool(state.get('socket')) and not state['connected']) or bool(state.get('subscribe_pending')) or (inbox and (
                inbox.delivery_gap or inbox.invalid.is_set() or inbox.resync.is_set() or
                (inbox.queue.qsize() and inbox.lag_seconds > 1)))
            if inbox:
                if inbox.delivery_gap:
                    state['subscribe_pause_until'] = now + 1
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
                if (any(balancer.capacity_cost(owners)) and balancer.pressure_samples >= balancer.confirmations
                        and now - balancer.last_rebalance >= balancer.minimum):
                    expanded = self.capacity_owners()
                    if balancer.capacity_cost(expanded) < balancer.capacity_cost(owners):
                        target = expanded
                if target:
                    self.apply_assignment(self.assignment_for(target))
            else:
                balancer.pressure, balancer.pressure_samples = None, 0
            self.sample_unavailable.clear()
        self.reconnect_pending = False
        for state in list(self.states.values()) + list(self.backups.values()) + ([self.standby[1]] if self.standby else []):
            try:
                if state['connected'] and now >= state.get('snapshot_retry_at', now + 10):
                    missing = (set(state['assets']) - state['shadow'].books.keys() if state.get('redundant')
                               else set(state['assets']) & self.continuity.pending_assets())
                    state.setdefault('recover_assets', set()).update(missing)
                    state['snapshot_retry_at'] = now + 10
                recover = state.pop('recover_assets', set()) & set(state['assets'])
                if recover and state['connected'] and not state.get('subscribe_initial'):
                    if state.get('redundant'):
                        state['ready'] = False
                        self.hot_backup_coverage.unready(next(i for i, s in self.backups.items() if s is state), now)
                        for asset in recover:
                            state['shadow'].books.pop(asset, None)
                            state['shadow'].recorder.metadata.pop(asset, None)
                    state['socket'].send(json.dumps(dict(operation='unsubscribe', assets_ids=sorted(recover))))
                    self.subscriptions.enqueue(state, sorted(recover))
                self.subscriptions.drain(state, now)
            except Exception as exc:
                state['connected'] = False
                if state.get('inbox'):
                    state['inbox'].invalid.set()
                state['socket'].close()
                _ws_error(exc)
        self.redundancy_tick(now)
        self.rotation_tick(now)
        for index, state in list(self.states.items()):
            if state.get('inbox') and state['inbox'].invalid.is_set() and state['socket']:
                if not state.get('queue_logged'):
                    _LOG.warning('Polymarket WS generation invalidated shard=%s drops=%s', index, state['inbox'].dropped)
                    state['queue_logged'] = True
                state['socket'].close()
            if not state['thread'] or not state['thread'].is_alive():
                if now >= state['retry_at']:
                    if self.standby and self.standby[0] == index:
                        # Keep a warming replacement while the old feed fails.
                        continue
                    backup = self.backups.get(index)
                    if backup and backup['connected'] and not backup['inbox'].invalid.is_set():
                        # Give a still-warming backup its bounded prepare window,
                        # instead of opening a third, nearly same-age connection.
                        continue
                    self.connect(index, state)
            elif (state['connected'] and not state.get('subscribe_initial') and now >= state['ping_at']
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
                peak_window = now - inbox.peak_at
                if peak_window >= 1:
                    inbox.peak_byte_rate = max(inbox.peak_byte_rate,
                        (inbox.received_bytes - inbox.peak_bytes) / peak_window)
                    inbox.peak_at, inbox.peak_bytes = now, inbox.received_bytes
                    state['peak_byte_rate'] = max(state.get('peak_byte_rate', 0), inbox.peak_byte_rate)
        cpu_now = process_time()
        cpu_cores = (cpu_now - self.cpu_seconds) / (now - self.cpu_at) if now > self.cpu_at else 0
        self.cpu_at, self.cpu_seconds = now, cpu_now
        hot_coverage = self.update_hot_coverage(time.monotonic())
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
                        peak_receive_bytes_per_second=max(s.get('peak_byte_rate', 0), s['inbox'].peak_byte_rate),
                        received_frames=s['inbox'].received_frames, received_bytes=s['inbox'].received_bytes,
                        callback_max_seconds=s['inbox'].callback_max_seconds,
                        parse_max_seconds=s['inbox'].parse_max_seconds,
                        tcp_pending_bytes=socket_pending_bytes(s.get('socket')),
                        read_ahead_active=getattr(s.get('socket'), 'read_ahead_active', False),
                        read_calls=getattr(s.get('socket'), 'read_calls', 0),
                        transport=dict(getattr(getattr(s.get('socket'), 'transport', None), 'last', {})))
                for i, s in self.states.items() if 'inbox' in s}
            STATUS.update(ws_continuity=self.continuity.metrics(), ws_failover_ms=self.failover_ms,
                          ws_hot_backup_by_group=hot_coverage,
                          ws_required_hot_groups=sorted(HOT_SERIES),
                          ws_split_byte_series=sorted(self.split_series),
                          ws_rotation_count=self.rotations, ws_rotation_failures=self.rotation_failures,
                          ws_rotation_last_failure=self.rotation_last_failure,
                          ws_hot_rotate_seconds=ws_setting('HOT_ROTATE_SECONDS', 0, 0),
                          ws_backup_promotions=self.backup_promotions,
                          ws_backups={i: dict(ready=s.get('ready', False), connected=s['connected'],
                              stage=self.prepare_stage(s), pending=len(s.get('subscribe_pending', [])),
                              age_seconds=max(0, now - s.get('opened_at', now)),
                              primary_age_offset_seconds=(s['opened_at'] - self.states[i].get('opened_at', s['opened_at'])
                                                          if 'opened_at' in s else None),
                              received_bytes=s['inbox'].received_bytes,
                              transport=dict(getattr(getattr(s.get('socket'), 'transport', None), 'last', {})))
                              for i, s in self.backups.items()},
                          ws_merge_metrics={i: dict(duplicates=m.duplicates, stale=m.stale, evictions=m.evictions,
                              entries=len(m.seen)) for i, m in self.merges.items()},
                          ws_standby_slow_consumer_count=self.standby_slow_consumers,
                          ws_rotation_pending=self.standby[0] if self.standby else None,
                          ws_connection_ages_seconds={i: max(0, now - s.get('opened_at', now))
                                                      for i, s in self.states.items()})
            STATUS['ws_subscription_pending'] = {i: len(s.get('subscribe_pending', [])) for i, s in self.states.items()}
            STATUS['ws_standby_subscription_pending'] = (len(self.standby[1].get('subscribe_pending', []))
                                                        if self.standby else None)
            STATUS['ws_shards'] = self.count
            STATUS['ws_connected'] = sum(bool(s['connected']) for s in self.states.values())
            STATUS['connected'] = bool(self.states) and all(s['connected'] for s in self.states.values())

    def failures(self):
        return max((state['attempts'] for state in self.states.values() if not state['connected']), default=0)

    def close(self, timeout=5):
        deadline = time.monotonic() + timeout
        self.supervisor_stop.set()
        with self.lifecycle_lock:
            self.closed = True
            self.cancel_rotation('shutdown')
            for state in self.backups.values():
                self.retire(state)
            self.backups.clear()
        closing = list(self.states.values()) + self.retiring
        for state in closing:
            if state.get('inbox'):
                state['inbox'].invalid.set()
            if state['socket']:
                state['socket'].close()
        for state in closing:
            if state['thread'] and state['thread'].is_alive():
                state['thread'].join(max(0, deadline - time.monotonic()))
            if state.get('worker') and state['worker'].is_alive():
                state['worker'].join(max(0, deadline - time.monotonic()))
        if self.supervisor and self.supervisor.is_alive():
            self.supervisor.join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['ws_connected'] = 0
            STATUS['connected'] = False

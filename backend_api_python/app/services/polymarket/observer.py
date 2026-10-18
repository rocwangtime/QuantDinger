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

from .observer_db import connection as get_db_connection
from .account_history import max_pair_skew_ms, pair_rejection
from .client import PublicClient
from .engine import number, sweep
from . import updown, recording
from .ws_balance import RateBalancer, DailyStats, balance_key
from .ws_transport import TransportDiagnostics, receive_buffer_options, remote_close
from .ws_rotation import DataHealth, ShadowBooks, reconnect_delay
from .ws_subscriptions import SubscriptionPacer
from .ws_recovery import SnapshotRecovery
from .ws_payloads import ParsedFrames, materialized_book, book_bytes
from .ws_redundancy import FeedMerge, FeedContinuity, HotBackupCoverage
from .ws_balance import setting as ws_setting
from .observer_db import metrics as database_metrics

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
        result['ws_book_memory'].update(result.get('ws_aux_memory', {}))
    provider = _REFERENCE
    if hasattr(provider, 'metrics'):  # Spot WebSocket composite only.
        result.update(provider.metrics())
    result['ws_process_rss_bytes'] = process_rss_bytes()
    return result


def process_rss_bytes():
    """Current resident set, not ru_maxrss's lifetime high-water mark."""
    try:
        with open('/proc/self/statm', encoding='ascii') as source:
            return int(source.read().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError, IndexError):
        return None


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
            levels, _ = materialized_book(event)
            books[asset] = {side: dict(rows) for side, rows in levels.items()}
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
    status = json.dumps(snapshot())
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO qd_polymarket_observer_status (singleton,status,updated_at) VALUES (true,%s::jsonb,NOW()) ON CONFLICT (singleton) DO UPDATE SET status=EXCLUDED.status,updated_at=NOW()", (status,))
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
    warm_markets = []
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
                        warm_markets = []
                        crypto, rejected = updown.discover(client, series_cache, prewarm=warm_markets)
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
                        connections.sync(markets, prewarm=warm_markets)
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
                # Once running, socket failures belong to the per-shard supervisor.
                # A single hot shard must never tear down healthy sockets or reset
                # session coverage/failure counters through global REST fallback.
                use_ws = os.getenv("POLYMARKET_OBSERVER_WS_ENABLED", "true").lower() in {"1", "true", "yes"} and (connections is not None or ws_failures < 3)
                if use_ws:
                    if connections is None:
                        tune_switch_interval()
                        connections = MarketConnections()
                        _WS = connections
                        connections.seed_rates(_prior_group_rates())
                        connections.sync(markets, prewarm=warm_markets)
                        connections.start_supervisor()
                    connections.tick()
                    ws_failures = 0
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
                self.paper_account.history.retain(self.asset_markets)
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
            if self.paper_account:
                self.paper_account.history.prune(int(time.time() * 1000))
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
                      'ws_book_memory': dict(assets=len(self.books),
                          levels=sum(len(levels) for b in self.books.values() for levels in b.values()),
                          pending_records=len(self.recording.pending),
                          delivery_samples=len(self.recording.delivery_samples),
                          **(self.paper_account.history.metrics() if self.paper_account else dict(history_events=0))),
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


class WireQueue(Queue):
    """Bound raw-frame storage by bytes as well as frame count."""
    def __init__(self, maximum, byte_limit):
        super().__init__(maximum)
        self.byte_limit = byte_limit
        self.charged_bytes = self.byte_high_water = 0

    @staticmethod
    def charge(item):
        raw = item[0]
        # Include receipt/tuple overhead and text's worst-case storage width.
        return 256 + (len(raw) * (4 if isinstance(raw, str) else 1)
                      if isinstance(raw, (str, bytes, bytearray)) else sys.getsizeof(raw))

    def _put(self, item):
        size = self.charge(item)
        if self.charged_bytes + size > self.byte_limit:
            raise Full
        super()._put(item)
        self.charged_bytes += size
        self.byte_high_water = max(self.byte_high_water, self.charged_bytes)

    def _get(self):
        item = super()._get()
        self.charged_bytes -= self.charge(item)
        return item


class ReceiveQueue:
    """Elastic bounded FIFO; a gap requests snapshots on the existing socket."""
    def __init__(self, maximum, ceiling=None):
        self.queue = WireQueue(maximum, int(ws_setting('RECEIVE_QUEUE_BYTES', 16 * 1024 * 1024, 1024)))
        self.ceiling = max(maximum, ceiling or maximum)
        self.lock = RLock()
        self.resync = Event()
        self.epoch = 0
        self.resyncs = 0
        self.delivery_gap = False
        self.lag_seconds = 0
        self.lock_wait_seconds = 0
        self.queue_wait_seconds = 0
        self.processing_received_at = None
        self.processing_charged_bytes = 0
        self.dropped = 0
        self.drop_reasons = dict(overflow=0, resync_rejected=0, invalid_rejected=0,
                                 resync_discarded=0, shutdown_discarded=0)
        self.high_water = 0
        self.received_frames = self.received_bytes = 0
        self.callback_max_seconds = self.parse_max_seconds = 0
        self.process_max_seconds = self.lifecycle_wait_seconds = self.discard_max_seconds = 0
        self.metric_at = time.monotonic()
        self.metric_frames = self.metric_bytes = 0
        self.receive_rate = self.byte_rate = 0
        self.peak_byte_rate = 0
        self.peak_at, self.peak_bytes = self.metric_at, 0
        self.invalid = Event()
        self.last_byte_at = self.last_data_at = None

    def pending_wait_seconds(self):
        with self.queue.mutex:
            oldest = self.queue.queue[0][1] if self.queue.queue else None
        now = datetime.now(timezone.utc)
        return max([self.queue_wait_seconds, *(max(0, (now - at).total_seconds())
            for at in (oldest, self.processing_received_at) if at is not None)])

    def put(self, raw):
        with self.lock:
            if self.invalid.is_set() or self.resync.is_set():
                self.dropped += 1
                self.drop_reasons['invalid_rejected' if self.invalid.is_set() else 'resync_rejected'] += 1
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
                self.drop_reasons['overflow'] += 1
                return False

    def discard(self, reason='resync_discarded'):
        # Detach in constant time under the queue mutex. Release raw frames after
        # unlocking; producers cannot extend a drain loop indefinitely.
        began = time.monotonic()
        with self.lock, self.queue.mutex:
            discarded = self.queue.queue
            self.queue.queue = deque()
            self.queue.charged_bytes = 0
            count = len(discarded)
            self.queue.not_full.notify_all()
            self.dropped += count
            self.drop_reasons[reason] += count
        discarded.clear()
        self.discard_max_seconds = max(self.discard_max_seconds, time.monotonic() - began)


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
            deadline = time.monotonic() + getattr(self, 'heartbeat_phase', self.ping_interval)
            try:
                while not stop_ping.wait(1) and self.keep_running:
                    now = time.monotonic()
                    self.transport.sample(self, now)
                    if now >= deadline and self.sock and self.subscription_ready.is_set():
                        self.transport.ping(now)
                        self.sock.send('PING')
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
        self.reserves = {}
        self.snapshot_recovery = SnapshotRecovery()
        self.parsed_frames = ParsedFrames()
        self.primary_failures = {}
        self.backup_next_connect_at = 0
        self.hot_backup_coverage = HotBackupCoverage(HOT_SERIES)
        self.observed_hot_groups = set()
        self.merges = {}
        self.continuity = FeedContinuity()
        self.migration = None
        self.migration_retry_at = 0
        self.migration_failures = 0
        self.primary_1013 = {}
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
        self.pong_checked_at = 0
        self.memory_at = 0
        self.aux_memory = {}
        self.markets = []
        self.prewarm_markets = []

    def backup_items(self):
        return list(self.backups.items()) + list(self.reserves.items())

    def backup_ready(self, candidate):
        inbox = candidate.get('inbox') if candidate else None
        required = set(candidate.get('required_assets', candidate.get('assets', []))) if candidate else set()
        if candidate and candidate.get('primary_index') in self.states:
            required = set(self.states[candidate['primary_index']]['assets'])
        shadow = candidate.get('shadow') if candidate else None
        return bool(candidate and candidate.get('ready') and candidate.get('connected') and inbox
                    and required
                    and (not shadow or required <= shadow.books.keys()
                         and all(shadow.recorder.metadata.get(a, (None,))[0] is not None for a in required))
                    and not inbox.invalid.is_set() and not inbox.resync.is_set()
                    and not inbox.delivery_gap and inbox.pending_wait_seconds() <= 1
                    and not required.intersection(candidate.get('subscribe_pending', []))
                    and not required.intersection(candidate.get('recover_assets', [])))

    def readiness_blockers(self, state):
        """Explain publication readiness separately from adding subscriptions."""
        inbox = state['inbox']
        required = set(self.states[state['primary_index']]['assets'])
        shadow = state['shadow']
        return [name for name, blocked in (
            ('disconnected', not state.get('connected')),
            ('invalid', inbox.invalid.is_set()), ('resync', inbox.resync.is_set()),
            ('delivery_gap', inbox.delivery_gap), ('queue_wait', inbox.pending_wait_seconds() > 1),
            ('current_subscription', bool(required.intersection(state.get('subscribe_pending', [])))),
            ('snapshot_repair', bool(required.intersection(state.get('recover_assets', [])))),
            ('missing_snapshot', not required <= shadow.books.keys()),
            ('missing_timestamp', any(shadow.recorder.metadata.get(a, (None,))[0] is None for a in required)),
            ('catchup', not state.get('ready'))) if blocked]

    def record_primary_failure(self, index, state, code):
        if self.closed or state.get('standby') or state.get('retired') or self.states.get(index) is not state:
            return
        with self.lifecycle_lock:
            if state.get('failure_recorded'):
                if code is not None:
                    for event in state.get('outage_events', []):
                        event['code'] = code
                return
            state['failure_recorded'] = True
            now = time.monotonic()
            state['outage_at'] = now
            state['outage_wall'] = datetime.now(timezone.utc)
            with (_PIPELINE.lock if _PIPELINE else RLock()):
                receipts = [v[1] for a, v in (_PIPELINE.recording.metadata.items() if _PIPELINE else [])
                            if a in state['assets']]
            groups = {self.balancer.groups.get(a, '').split(':outcome:')[0] for a in state['assets']} & HOT_SERIES
            ready = sum(self.backup_ready(c) for i, c in self.backup_items() if i == index)
            for key in groups:
                row = self.primary_failures.setdefault(key, dict(total=0, without_ready_backup=0, events=deque(maxlen=64)))
                row['total'] += 1
                row['without_ready_backup'] += not ready
                event = dict(code=code, ready_backup_at_kick=bool(ready), ready_backups=ready,
                             takeover_gap_ms=None, receipt_gap_ms=None, recovery=None, gap_final=False)
                source_receipts = [self.continuity.latest.get(a, {}).get(state.get('source'), (None, None, None))[2]
                                   for a in state['assets']]
                source_receipts = [at for at in source_receipts if at is not None]
                event['_last_receipt'] = min(source_receipts or receipts, default=state['outage_wall'])
                event['source_silence_ms'] = max(0, (state['outage_wall'] - event['_last_receipt']).total_seconds() * 1000)
                event['merged_receipt_gap_ms'] = max(0, (state['outage_wall'] - min(receipts, default=state['outage_wall'])).total_seconds() * 1000)
                event['merged_gap_ms'] = event['merged_receipt_gap_ms']
                event['merged_gap_final'] = False
                event['_merged_previous'] = {a: v[1] for a, v in (_PIPELINE.recording.metadata.items() if _PIPELINE else [])
                                             if a in state['assets']}
                event['reason'] = state.get('failure_reason', 'transport')
                row['events'].append(event)
                _LOG.warning('Polymarket WS hot primary failure group=%s shard=%s code=%s ready_backups=%s',
                             key, index, code, ready)
                state.setdefault('outage_events', []).append(event)
                state['outage_events'] = state['outage_events'][-64:]
            # Public REST runs independently of WS handshake/supervisor cadence.
            if not ready and not (state.get('failure_reason') == 'pong_timeout' and any(
                    i == index and self.promotable_backup(c) for i, c in self.backup_items())):
                if _PIPELINE:
                    with _PIPELINE.lock:
                        self.merges.setdefault(index, FeedMerge(_PIPELINE.recording.metadata))
                        for asset in state['assets']:
                            if self.balancer.assignment.get(asset) == index:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.forget(asset)
                self.request_snapshot(index, state, set(state['assets']))

    def complete_recovery(self, index, state, method):
        if 'outage_at' not in state or not _PIPELINE:
            return
        owned = {a for a in state['assets'] if self.balancer.assignment.get(a) == index}
        wall = state['outage_wall']
        self.update_merged_gap(state)
        if owned & self.continuity.pending_assets():
            return
        if not owned or not all(a in _PIPELINE.books and
                _PIPELINE.recording.metadata.get(a, (None, datetime.min.replace(tzinfo=timezone.utc)))[1] >= wall
                for a in owned):
            return
        gap = max(0, (time.monotonic() - state['outage_at']) * 1000)
        for event in state.pop('outage_events', []):
            if event.get('_last_receipt') is not None:
                last = event.pop('_last_receipt', wall)
                receipt_gap = max(0, (datetime.now(timezone.utc) - last).total_seconds() * 1000)
                event.update(takeover_gap_ms=receipt_gap, detection_to_takeover_ms=event.get('detection_to_takeover_ms', gap),
                             recovery=event.get('recovery', method) or method,
                             receipt_gap_ms=receipt_gap, gap_final=True)
        state.pop('outage_at', None)

    def update_merged_gap(self, state):
        wall = state.get('outage_wall')
        if wall is None:
            return
        owned = set(state['assets'])
        for event in state.get('outage_events', []):
            previous = event.get('_merged_previous', {})
            for asset, last in list(previous.items()):
                current = self.continuity.published.get(asset)
                if current and current['received'] > last:
                    event['merged_gap_ms'] = max(event['merged_gap_ms'], current['receipt_gap_ms'])
                    previous[asset] = current['received']
            event['merged_gap_final'] = bool(owned) and all(previous.get(a, wall) > wall for a in owned)

    @staticmethod
    def promotable_backup(state):
        inbox = state.get('inbox')
        return bool(state.get('connected') and not state.get('retired') and inbox
                    and state.get('initial_sent_at') is not None and not state.get('subscribe_initial')
                    and all(state.get(key) and state[key].is_alive() for key in ('thread', 'worker'))
                    and not inbox.invalid.is_set() and not inbox.resync.is_set()
                    and not inbox.delivery_gap)

    def backup_protects(self, index, primary):
        return any(i == index and (self.backup_ready(c) or primary.get('pong_dead') and self.promotable_backup(c))
                   for i, c in self.backup_items())

    def pong_timeout_tick(self, now):
        # Supervisor owns detection; the backup worker owns promotion so an
        # already-popped delta cannot be stranded by a control-thread switch.
        if now - self.pong_checked_at < 1:
            return
        self.pong_checked_at = now
        for index, state in list(self.states.items()) + self.backup_items():
            transport = getattr(state.get('socket'), 'transport', None)
            inbox = state.get('inbox')
            if (not state.get('connected') or not inbox or not isinstance(transport, TransportDiagnostics)
                    or not transport.pong_timed_out(now, inbox.last_byte_at)):
                continue
            state.update(connected=False, failure_reason='pong_timeout', failed_at=now, retry_at=now)
            if state.get('redundant'):
                inbox.invalid.set()
                (self.backups if self.backups.get(index) is state else self.reserves).pop(index, None)
                self.retire(state)
                continue
            self.record_primary_failure(index, state, None)
            state['pong_dead'] = True
            if not any(i == index and self.promotable_backup(c) for i, c in self.backup_items()):
                inbox.invalid.set()
            # Wake the old reader, without waiting for a network close handshake.
            sock = state['socket']
            sock.keep_running = False
            wire = getattr(sock, 'sock', None)
            if wire and hasattr(wire, 'shutdown'):
                wire.shutdown()
            else:
                sock.close()
            _LOG.warning('Polymarket WS PONG timeout shard=%s timeout_seconds=22', index)

    def memory_metrics(self, now):
        if now - self.memory_at < 1:
            return self.aux_memory
        self.memory_at = now
        states = list(self.states.values()) + [s for _, s in self.backup_items()] + self.retiring
        if self.standby:
            states.append(self.standby[1])
        states = list({id(s): s for s in states}.values())
        shadows = {id(s['shadow']): s['shadow'] for s in states if s.get('shadow')}
        if self.migration:
            shadows.update({id(s): s for s in self.migration['shadows'].values()})
        shadow_bytes = 0
        shadow_merges = []
        for shadow in shadows.values():
            with shadow.lock:
                shadow_bytes += sum(book_bytes(book) for book in shadow.books.values())
                shadow_merges.append(shadow.merge)
        merges = list(self.merges.values()) + shadow_merges
        # Conservative budgets, not RSS. Fixed per-entry charges include up to
        # four source names/counters; expensive object walks stay off workers.
        dedup = sum(1536 * len(m.seen) + 1024 * len(m.levels) + 1024 * len(m.latest) for m in merges)
        with self.continuity.lock:
            continuity = (1536 * sum(map(len, self.continuity.history.values()))
                          + 1024 * sum(map(len, self.continuity.latest.values()))
                          + 256 * len(self.continuity.merged_gaps))
        queues = processing = 0
        for state in states:
            inbox = state.get('inbox')
            if inbox:
                with inbox.queue.mutex:
                    queues += inbox.queue.charged_bytes
                processing += inbox.processing_charged_bytes
        frames = self.parsed_frames.metrics()['charged_bytes']
        self.aux_memory = dict(shadow_charged_bytes=shadow_bytes, dedup_charged_bytes=dedup,
            continuity_charged_bytes=continuity, parsed_frames_charged_bytes=frames,
            queue_charged_bytes=queues, processing_frame_charged_bytes=processing,
            auxiliary_charged_bytes=shadow_bytes + dedup + continuity + frames + queues + processing,
            accounting='conservative_retained_cache_estimates_not_rss')
        return self.aux_memory

    def request_snapshot(self, index, state, assets):
        generation = state.get('inbox')
        epoch = generation.epoch if generation else None
        def publish(events, started):
            with self.lifecycle_lock:
                if (self.closed or state.get('retired') or state.get('inbox') is not generation
                        or generation and (generation.epoch != epoch or generation.resync.is_set())):
                    return
                if state.get('redundant'):
                    if not any(i == index and c is state for i, c in self.backup_items()):
                        return
                    inbox = state['inbox']
                    if inbox.invalid.is_set() or inbox.resync.is_set():
                        return
                    self.redundant_receive(index, state, events, started, inbox)
                elif self.states.get(index) is state and _PIPELINE:
                    with _PIPELINE.lock:
                        owned = {a for a in assets if self.balancer.assignment.get(a) == index}
                        # Seed from live watermarks: late REST may never roll back WS.
                        merge = self.merges.setdefault(index, FeedMerge(_PIPELINE.recording.metadata))
                        merged = merge.filter(events, 'rest', owned, backup=True)
                        if merged:
                            self.continuity.receive(merged, 'rest', datetime.now(timezone.utc))
                            _PIPELINE.receive(merged, owned, datetime.now(timezone.utc))
                            self.continuity.publication(_PIPELINE.recording.metadata, owned)
                        self.complete_recovery(index, state, 'rest_snapshot')
        self.snapshot_recovery.request(state, assets, publish)

    def sync(self, markets, prewarm=None):
        with self.lifecycle_lock:
            if prewarm is not None:
                self.prewarm_markets = live_markets(prewarm)
            self._sync(markets)

    def active_catalogue(self, markets, now=None):
        now = now or datetime.now(timezone.utc)
        active = live_markets(markets, now)
        known = {m['id'] for m in active}
        for market in live_markets(self.prewarm_markets, now):
            if (market['id'] not in known and balance_key(market) in HOT_SERIES
                    and updown.timestamp(market['eventStartTime']) <= now):
                active.append(market)
                known.add(market['id'])
        return active

    def backup_assets(self, index, now=None):
        """At most the nearest next window per hot series, only on spares."""
        now = now or datetime.now(timezone.utc)
        assets = list(self.states[index]['assets'])
        state = self.states[index]
        if max([self.ingress_rate(state), self.balancer.byte_loads()[index],
                *(self.ingress_rate(c) for i, c in self.backup_items() if i == index)]) >= self.balancer.max_bytes:
            state['prewarm_blocked_assets'] = frozenset(assets)
        if state.get('prewarm_blocked_assets') == frozenset(assets):
            return assets  # Do not add next-window traffic to a saturated pair.
        groups = {self.balancer.groups.get(a) for a in assets}
        nearest = {}
        for market in live_markets(self.prewarm_markets, now):
            key = balance_key(market)
            start = updown.timestamp(market['eventStartTime'])
            if key in groups and key in HOT_SERIES and 0 < (start - now).total_seconds() <= 90:
                if key not in nearest or start < updown.timestamp(nearest[key]['eventStartTime']):
                    nearest[key] = market
        for market in nearest.values():
            assets.extend(a for a in subscription_assets([market]) if a not in assets)
        return assets

    def sync_backup(self, index, candidate):
        required = set(self.states[index]['assets'])
        assets = self.backup_assets(index)
        now = time.monotonic()
        if required and self.hot_shard(index):
            candidate.pop('idle_since', None)
        elif not required and now - candidate.setdefault('idle_since', now) < 90:
            # Discovery can arrive just after expiry. Keep the physical socket,
            # but never count its expired books as a ready backup.
            assets = []
        else:
            (self.backups if self.backups.get(index) is candidate else self.reserves).pop(index)
            self.retire(candidate)
            return
        if assets == candidate['assets'] and required == set(candidate.get('required_assets', required)):
            return
        commands = subscription_changes(candidate['assets'], assets)
        with (_PIPELINE.lock if _PIPELINE else RLock()), candidate['inbox'].lock:
            candidate['assets'] = assets
            candidate['required_assets'] = required
            candidate['subscribe_pending'] = [a for a in candidate.get('subscribe_pending', []) if a in assets]
            candidate['recover_assets'] = set(candidate.get('recover_assets', set())) & set(assets)
            activated = getattr(candidate['inbox'], 'activated', {})
            candidate['inbox'].activated = {a: activated.get(a, datetime.max.replace(tzinfo=timezone.utc)) for a in assets}
            candidate['shadow'].prune(assets)
            candidate['ready'] = False
        for command in commands:
            try:
                if command['operation'] == 'unsubscribe':
                    candidate['socket'].send(json.dumps(command))
                else:
                    self.subscriptions.enqueue(candidate, command['assets_ids'])
            except Exception as exc:
                candidate['connected'] = False
                candidate['inbox'].invalid.set()
                _LOG.warning('Polymarket WS backup subscription failed shard=%s reason=%s', index, str(exc)[:160])
                break
        self.redundant_receive(index, candidate, [], datetime.now(timezone.utc), candidate['inbox'])

    def _sync(self, markets):
        self.markets = markets = self.active_catalogue(markets)
        if _PIPELINE and set(_PIPELINE.markets) != {m['id'] for m in markets}:
            _PIPELINE.set_markets(markets)
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
        # A YES/NO pair is indivisible, including when its byte rate exceeds a cap.
        return balance_key(market)

    def hot_shard(self, index):
        return any(self.balancer.groups.get(a, '').split(':outcome:')[0] in HOT_SERIES
                   for a, owner in self.balancer.assignment.items() if owner == index)

    def assignment_for(self, owners):
        # A rollover can replace every asset of a group before current_owners()
        # sees it. Complete the catalogue, retaining the proposed known owners.
        complete = self.balancer.place(set(self.balancer.groups.values()), owners, optimize=False)
        return {a: complete[k] for a, k in self.balancer.groups.items()}

    def capacity_owners(self, rebalance=False, now=None):
        """Place new groups without silently migrating retained ones at rollover."""
        b = self.balancer
        now = time.monotonic() if now is None else now
        keys = set(b.groups.values())
        previous = {**b.owners, **b.current_owners()}
        owners = b.place(keys, previous, optimize=False)
        initial = not b.assignment
        movable = keys if initial else {
            k for k in keys if k not in previous or rebalance and k in b.measured
            and k not in b.unavailable and now - b.moved_at.get(k, float('-inf')) >= b.group_cooldown}
        if initial or rebalance:
            owners, _, _ = b.search(owners, b.max_moves, movable)
        while any(b.capacity_cost(owners)) and self.count < 8:
            before = b.capacity_cost(owners)
            self.count += 1
            b.count = self.count
            trial = b.place(keys, owners, optimize=False)
            trial, _, _ = b.search(trial, b.max_moves, movable)
            if b.capacity_cost(trial) >= before:
                self.count -= 1
                b.count = self.count
                break
            owners = trial
        return owners

    def apply_assignment(self, assignment):
        with self.lifecycle_lock:
            if self.standby and assignment != self.balancer.assignment:
                self.cancel_rotation('assignment_changed')
            if self.migration:
                if assignment == self.migration['target']:
                    return True
                self.cancel_migration('assignment_changed')
            old = self.balancer.assignment
            incoming = {a for a in old.keys() & assignment.keys() if old[a] != assignment[a]}
            if incoming:
                return self.prepare_migration(assignment, incoming)
            return self._apply_assignment(assignment)

    def _apply_assignment(self, assignment, warmed=None):
        warmed = warmed or {}
        warm_assets = {a for shadow in warmed.values() for a in shadow.assets}
        self.update_hot_coverage(time.monotonic())
        assignments = {i: [a for a, shard in assignment.items() if shard == i] for i in range(self.count)}
        # Serialize publication with consumers. Warmed assets keep live books
        # until caught-up destination snapshots replace them atomically.
        pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
        with pipeline_lock:
            if warmed:
                metadata = _PIPELINE.recording.metadata if _PIPELINE else {}
                for index, shadow in warmed.items():
                    state = self.states[index]
                    inbox = state.get('inbox')
                    if (not state['connected'] or state.get('subscribe_pending')
                            or self.migration and inbox is not self.migration['generations'][index]
                            or inbox and (inbox.invalid.is_set() or inbox.resync.is_set()
                                or inbox.delivery_gap or inbox.queue_wait_seconds > 1)
                            or not shadow.ready(metadata)):
                        return False
            old = self.balancer.assignment
            old_owners = self.balancer.current_owners()
            moved = any(a in assignment and assignment[a] != i for a, i in old.items())
            changed = {a for a in old.keys() | assignment.keys() if old.get(a) != assignment.get(a)}
            for asset in changed & old.keys() & assignment.keys():
                self.continuity.switch(self.states[old[asset]].get('source'),
                    self.states[assignment[asset]].get('source'), [asset], 'reassignment',
                    book_preserved=asset in warm_assets)
            if _PIPELINE:
                for asset in changed - warm_assets:
                    _PIPELINE.books.pop(asset, None)
                    _PIPELINE.recording.metadata.pop(asset, None)
                    if _PIPELINE.paper_account:
                        _PIPELINE.paper_account.history.forget(asset)
            # Warmed destinations already have their physical subscriptions.
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
                        state['inbox'].activated = {a: (datetime.max.replace(tzinfo=timezone.utc) if a in changed - warm_assets else activated.get(a, datetime.now(timezone.utc)))
                                                   for a in assets if a in changed or a in activated}
            with self.balancer.lock:
                self.balancer.assignment = dict(assignment)
                new_owners = self.balancer.current_owners()
                self.balancer.owners.update(new_owners)
                self.balancer.applied(old_owners, new_owners, time.monotonic())
            # Both paths are still subscribed. Publish caught-up full books under
            # the same lock as ownership, then remove the former subscriptions.
            for index, shadow in warmed.items():
                state = self.states[index]
                merge = self.merges.setdefault(index, FeedMerge({}))
                for asset in shadow.assets:
                    merge.latest[asset] = merge.snapshots[asset] = shadow.recorder.metadata[asset][0]
                merge.levels = type(merge.levels)((k, v) for k, v in merge.levels.items()
                                                   if k[0] not in shadow.assets)
                # Retain hot fan-in duplicate counts across the shard boundary.
                for origin in {old[a] for a in shadow.assets}:
                    source_merge = self.merges.get(origin)
                    if source_merge and source_merge is not merge:
                        merge.seen.update(source_merge.seen)
                merge.seen = type(merge.seen)(sorted(merge.seen.items(), key=lambda item: item[1][0])[-merge.maximum:])
                for asset, book in shadow.books.items():
                    at, receipt, _ = shadow.recorder.metadata[asset]
                    event = dict(event_type='book', asset_id=asset, timestamp=int(at.timestamp() * 1000),
                        bids=[dict(price=p, size=q) for p, q in book['bids'].items()],
                        asks=[dict(price=p, size=q) for p, q in book['asks'].items()])
                    # A prepared full book satisfies an uncertain continuity
                    # switch locally, without a second wire resubscription.
                    self.continuity.receive(event, state.get('source'), receipt)
                    if _PIPELINE:
                        _PIPELINE.receive(event, shadow.assets, receipt, state['inbox'], state['inbox'].epoch)
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

        for index, candidate in self.backup_items():
            self.sync_backup(index, candidate)
        with pipeline_lock:
            for index, merge in self.merges.items():
                merge.prune(set(self.states.get(index, {}).get('assets', [])))

        if moved:
            self.balancer.last_rebalance = time.monotonic()
            self.stats.increment('rebalance_count')
            with _LOCK:
                STATUS['rebalance_count'] = STATUS.get('rebalance_count', 0) + 1
        self.update_hot_coverage(time.monotonic())
        return True

    def prepare_migration(self, assignment, incoming):
        """Warm incoming pairs on destination sockets while the old owners publish."""
        if time.monotonic() < self.migration_retry_at:
            return False
        shadows = {i: ShadowBooks({a for a in incoming if assignment[a] == i})
                   for i in set(assignment[a] for a in incoming)}
        pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
        with pipeline_lock:
            for shadow in shadows.values():
                shadow.observe_active(_PIPELINE.recording.metadata if _PIPELINE else {})
        self.migration = dict(target=dict(assignment), shadows=shadows,
                              created_at=time.monotonic(), generations={})
        for index, shadow in shadows.items():
            state = self.states.setdefault(index, dict(assets=[], socket=None, thread=None,
                connected=False, lock=RLock(), attempts=0, retry_at=0, ping_at=0))
            with state['lock']:
                state['assets'] = sorted(set(state['assets']) | shadow.assets)
                if state.get('inbox'):
                    activated = getattr(state['inbox'], 'activated', {})
                    state['inbox'].activated = {**activated, **{a: datetime.max.replace(tzinfo=timezone.utc)
                                                               for a in shadow.assets}}
                if state['connected'] and state['socket']:
                    self.subscriptions.enqueue(state, sorted(shadow.assets))
                elif not state.get('thread') or not state['thread'].is_alive():
                    self.connect(index, state)
            # connect() has synchronously installed the new inbox/source before
            # its reader opens. A later reconnect/promotion cancels this prepare.
            self.migration['generations'][index] = state.get('inbox')
        _LOG.info('Polymarket WS migration prepare assets=%s destinations=%s', len(incoming), sorted(shadows))
        return True

    def cancel_migration(self, reason):
        pending, self.migration = self.migration, None
        if not pending:
            return
        if reason != 'shutdown':
            self.migration_failures += 1
            self.migration_retry_at = time.monotonic() + self.balancer.minimum
            # Reconcile only the extra destination subscriptions; the applied
            # owners and their live books have never changed.
            self._apply_assignment(dict(self.balancer.assignment))
        _LOG.info('Polymarket WS migration cancelled reason=%s', reason)

    def migration_tick(self, now):
        pending = self.migration
        if not pending:
            return
        failed = now - pending['created_at'] >= ws_setting('MIGRATION_PREPARE_SECONDS', 90, 1)
        for index, generation in pending['generations'].items():
            state = self.states[index]
            inbox = state.get('inbox')
            failed |= (inbox is not generation or (not state['connected']
                       and ('opened_at' in state or 'failed_at' in state)) or state.get('retired', False)
                       or bool(inbox and (inbox.invalid.is_set() or inbox.resync.is_set() or inbox.delivery_gap)))
        if failed:
            self.cancel_migration('failed_or_timeout')

    def migration_receive(self, index, state, event, received, inbox):
        if not self.migration:
            return event
        with self.lifecycle_lock:
            pending = self.migration
            if not pending or index not in pending['shadows']:
                return event
            if (self.states.get(index) is not state or inbox is not pending['generations'][index]
                    or inbox.invalid.is_set() or inbox.resync.is_set()
                    or (datetime.now(timezone.utc) - received).total_seconds() > 1):
                return event
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock:
                shadow = pending['shadows'][index]
                assets = {a for a in shadow.assets if received >= getattr(inbox, 'activated', {}).get(a, received)}
                shadow_event = recording.owned_events(event, assets)
                shadow.receive(shadow_event, received)
                self.continuity.receive(shadow_event, state.get('source'), received)
            # Recheck every watermark/inbox under the publication lock. Network
            # sends happen after that lock exits.
            if self._apply_assignment(pending['target'], warmed=pending['shadows']):
                self.migration = None
                _LOG.info('Polymarket WS migration committed destinations=%s', sorted(pending['shadows']))
            # Another destination can commit while this worker waits to publish.
            # Books/deltas already applied to its shadow must not replay over the
            # published snapshot. Trades still follow normal owner/merge rules.
            def live_event(row):
                if isinstance(row, list):
                    return [live_event(item) for item in row]
                if isinstance(row, dict) and (row.get('event_type') or row.get('type')) in ('book', 'price_change'):
                    return recording.owned_events(row, set(state['assets']) - shadow.assets)
                return row
            return live_event(event)

    def migration_metrics(self, now):
        pending = self.migration
        return dict(pending=bool(pending), failures=self.migration_failures,
                    age_seconds=max(0, now - pending['created_at']) if pending else 0,
                    assets=sorted(a for s in pending['shadows'].values() for a in s.assets) if pending else [],
                    retry_in_seconds=max(0, self.migration_retry_at - now))

    def record_primary_1013(self, index, state, code):
        if code != 1013 or state.get('standby') or state.get('retired') or state.get('primary_1013_recorded'):
            return
        with self.lifecycle_lock:
            if state.get('primary_1013_recorded'):
                return
            state['primary_1013_recorded'] = True
            row = self.primary_1013.setdefault(index, dict(total=0, recent=deque(maxlen=64)))
            row['total'] += 1
            inbox = state.get('inbox')
            owners = self.balancer.current_owners()
            row['last'] = dict(assets=len(state['assets']),
                groups=sorted(k for k, i in owners.items() if i == index),
                measured_msg_per_s=self.balancer.loads(owners)[index],
                measured_bytes_per_s=self.balancer.byte_loads(owners)[index],
                receive_bytes_per_second=inbox.byte_rate if inbox else None)
            row['recent'].append(time.monotonic())

    def primary_1013_metrics(self, now):
        return {i: dict(total=row['total'], count_10m=sum(t >= now - 600 for t in row['recent']),
                        window_seconds=600, window_count_capped=(len(row['recent']) == 64
                            and row['recent'][0] >= now - 600), **row['last']) for i, row in self.primary_1013.items()}

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
        if not any(s is state for s in self.retiring):
            self.retiring.append(state)

    def reap_retired(self):
        # A dead socket retains callbacks -> state -> shadow/books/transport.
        # Keep live threads joinable, but release completed generations each tick.
        self.retiring[:] = [s for s in self.retiring if any(
            s.get(k) and s[k].is_alive() for k in ('thread', 'worker'))]

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
        delay = min(2, reconnect_delay(failures)) if candidate.get('redundant') else min(480, 60 * 2 ** min(3, failures - 1))
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
        if state.get('redundant') and self.backup_ready(state):
            return 'ready'
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

    @staticmethod
    def ingress_rate(state):
        inbox = state.get('inbox')
        transport = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
        return max(inbox.byte_rate if inbox else 0, transport.get('read_bytes_per_second') or 0)

    @staticmethod
    def backup_target():
        # One full spare cuts hot stream copies from three to two. Keep an
        # explicit two-spare option for measured A/B comparisons.
        return min(2, int(ws_setting('HOT_BACKUP_COUNT', 1, 1)))

    def pair_load(self, index, state):
        peers = {id(s): s for i, s in [(index, self.states.get(index, {})), *self.backup_items()]
                 if i == index and s is not state and s.get('inbox')}
        own = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
        peer_samples = [getattr(getattr(s.get('socket'), 'transport', None), 'last', {}) for s in peers.values()]
        return dict(peer_connections=len(peers),
            pair_receive_bytes_per_second=self.ingress_rate(state) + sum(self.ingress_rate(s) for s in peers.values()),
            pair_read_bytes_60s=(own.get('read_bytes_60s') or 0) + sum(s.get('read_bytes_60s') or 0 for s in peer_samples),
            peer_queue_wait_seconds=max((s['inbox'].queue_wait_seconds for s in peers.values()), default=0),
            heartbeat_phase_seconds=getattr(state.get('socket'), 'heartbeat_phase', None))

    def backup_metrics(self, index, state, now):
        inbox = state['inbox']
        required = set(self.states[index]['assets'])
        pending = set(state.get('subscribe_pending', []))
        return dict(ready=self.backup_ready(state), connected=state['connected'], stage=self.prepare_stage(state),
                    pending=len(pending), pending_current=len(pending & required),
                    prewarm_assets=len(set(state['assets']) - required),
                    shadow_assets=len(state['shadow'].merge.latest), shadow_levels=len(state['shadow'].merge.levels),
                    queue_depth=inbox.queue.qsize(), queue_wait_seconds=inbox.pending_wait_seconds(),
                    queue_capacity=inbox.queue.maxsize, queue_high_water=inbox.high_water, drops=inbox.dropped,
                    queue_charged_bytes=inbox.queue.charged_bytes, queue_byte_limit=inbox.queue.byte_limit,
                    drop_reasons=dict(inbox.drop_reasons), resyncs=inbox.resyncs,
                    receive_bytes_per_second=self.ingress_rate(state),
                    admission_pressure_reasons=self.subscriptions.pressure_reasons(state),
                    readiness_blockers=self.readiness_blockers(state),
                    lifecycle_wait_seconds=inbox.lifecycle_wait_seconds,
                    process_max_seconds=inbox.process_max_seconds, discard_max_seconds=inbox.discard_max_seconds,
                    delivery_gap=inbox.delivery_gap, resync=inbox.resync.is_set(),
                    age_seconds=max(0, now - state.get('opened_at', now)),
                    primary_age_offset_seconds=(state['opened_at'] - self.states[index].get('opened_at', state['opened_at'])
                                                if 'opened_at' in state else None),
                    received_bytes=inbox.received_bytes,
                    transport=dict(getattr(getattr(state.get('socket'), 'transport', None), 'last', {})))

    def redundancy_tick(self, now):
        for index, candidate in self.backup_items():
            self.sync_backup(index, candidate)
            if candidate.get('retired'):
                continue
            old = self.states.get(index)
            failed = (candidate['inbox'].invalid.is_set() or candidate.get('failed_at') is not None
                      or not candidate['thread'].is_alive() or not candidate['worker'].is_alive()
                      or candidate.get('initial_sent_at') is None and now - candidate['created_at'] >= 15)
            if not old or failed:
                (self.backups if self.backups.get(index) is candidate else self.reserves).pop(index)
                if not candidate.get('normal_shutdown'):
                    self.rotation_failed(index, old, candidate, 'backup_wire_failure')
                elif old:
                    old['backup_retry_at'] = now + reconnect_delay(1)
                self.retire(candidate)
            elif candidate.get('initial_sent_at') is not None and not candidate.get('admission_observed'):
                self.backup_next_connect_at = max(self.backup_next_connect_at,
                    candidate['initial_sent_at'] + candidate['admission_seconds'])
                candidate['admission_observed'] = True
        for index in list(self.reserves):
            if index not in self.backups:
                self.backups[index] = self.reserves.pop(index)
        self.update_hot_coverage(now)
        dialing = any(s.get('initial_sent_at') is None for _, s in self.backup_items())
        # Uncovered groups go first. Extra redundancy is staggered, never at
        # the expense of the only backup or by adding load to another group.
        for index, old in sorted(self.states.items(), key=lambda item: (item[0] in self.backups, item[0])):
            if not self.hot_shard(index):
                continue
            health = old.get('data_health')
            transport = getattr(getattr(old.get('socket'), 'transport', None), 'last', {})
            if health and health.sample(now, transport):
                old['data_suspect'] = True
                old.setdefault('failed_at', now)
                old.setdefault('recover_assets', set()).update(old['assets'])
            for i, candidate in self.backup_items():
                if i != index:
                    continue
                health = candidate.get('data_health')
                transport = getattr(getattr(candidate.get('socket'), 'transport', None), 'last', {})
                if health and health.sample(now, transport):
                    candidate.setdefault('recover_assets', set()).update(candidate['assets'])
            first = self.backups.get(index)
            if (dialing or index in self.reserves or first and self.backup_target() < 2
                    or not old['connected'] or not old['assets']
                    or old.get('subscribe_initial', True) or not old.get('inbox') or old['inbox'].invalid.is_set()
                    or now < old.get('backup_retry_at', 0) or now < self.backup_next_connect_at):
                continue
            # Stagger only initial protection, never delay replenishment after
            # failover. A long 90s bootstrap would itself consume the 95% budget.
            stagger = ws_setting('HOT_BACKUP_STAGGER_SECONDS', 7, 0)
            if not old.get('backup_started') and now < old.get('opened_at', now) + old.setdefault(
                    'backup_phase_seconds', random.uniform(stagger, stagger + 4) if stagger else 0):
                continue
            if first and now < first.get('initial_sent_at', now) + first.setdefault('reserve_delay', random.uniform(10, 15)):
                continue
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock:
                self.merges.setdefault(index, FeedMerge({a: v for a, v in
                    (_PIPELINE.recording.metadata.items() if _PIPELINE else []) if a in old['assets']}))
            self.backup_serial += 1
            owned_assets = self.backup_assets(index)
            candidate = dict(assets=owned_assets, socket=None, thread=None, connected=False,
                required_assets=set(old['assets']), primary_index=index,
                lock=RLock(), attempts=0, retry_at=0, ping_at=0, standby=True, redundant=True,
                busy=True, source=f'backup-{self.backup_serial}', shadow=ShadowBooks(owned_assets), created_at=now)
            (self.reserves if first else self.backups)[index] = candidate
            old['backup_started'] = True
            initial = max(int(ws_setting('SUBSCRIBE_BATCH_ASSETS', 2, 1)),
                          int(ws_setting('SUBSCRIBE_INITIAL_ASSETS', 12, 1)))
            candidate['admission_seconds'] = max(ws_setting('SUBSCRIBE_BATCH_SECONDS', .5, .05),
                min(initial, len(candidate['assets'])) / ws_setting('SUBSCRIBE_ASSETS_PER_S', 4, .1))
            self.backup_next_connect_at = now + candidate['admission_seconds']
            _LOG.info('Polymarket WS redundant prepare shard=%s assets=%s reserve=%s', index, len(owned_assets), bool(first))
            self.connect(index, candidate)
            dialing = True

    def update_hot_coverage(self, now):
        required = {}
        for asset, index in self.balancer.assignment.items():
            key = self.balancer.groups.get(asset, '').split(':outcome:')[0]
            if key in HOT_SERIES:
                required.setdefault(key, set()).add(index)
        ready = set()
        for index, candidate in self.backup_items():
            old = self.states.get(index, {})
            if (self.backup_ready(candidate) and old.get('connected')
                    and not old.get('data_suspect') and not old['inbox'].invalid.is_set()
                    and not candidate['inbox'].invalid.is_set() and not candidate['inbox'].resync.is_set()):
                ready.add(index)
        counts = {index: sum(i == index and self.backup_ready(c) for i, c in self.backup_items())
                  for shards in required.values() for index in shards}
        self.observed_hot_groups.update(required)
        # Catalogue absence during rollover must not hide zero-spare time.
        # The legacy coverage denominator still excludes unsubscribed periods.
        zero_required = {key: required.get(key, {None}) for key in self.observed_hot_groups}
        rows = self.hot_backup_coverage.update(required, ready, now, backup_counts=counts,
                                              zero_required=zero_required)
        for key, row in rows.items():
            row['ready_backup_count'] = min((sum(i == index and self.backup_ready(c)
                for i, c in self.backup_items()) for index in required.get(key, set())), default=0)
            row['target_backup_count'] = self.backup_target()
        return rows

    def redundant_receive(self, index, state, event, received_at, inbox, epoch=None):
        # Building a private ladder must not hold every feed's publication lock.
        # ShadowBooks has its own short lock; release it before lifecycle entry.
        shadow = state.get('shadow')
        if shadow is None or inbox.invalid.is_set() or inbox.resync.is_set() or state.get('retired'):
            return
        epoch = inbox.epoch if epoch is None else epoch
        if epoch != inbox.epoch:
            return
        activated = dict(getattr(inbox, 'activated', {}))
        event = recording.owned_events(event, {a for a in list(state['assets'])
            if received_at >= activated.get(a, received_at)})
        shadow.receive(event, received_at)
        waiting_at = time.monotonic()
        with self.lifecycle_lock:
            pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
            with pipeline_lock, inbox.lock, shadow.lock:
                inbox.lifecycle_wait_seconds = time.monotonic() - waiting_at
                if (not any(i == index and c is state for i, c in self.backup_items()) or inbox.invalid.is_set() or inbox.resync.is_set()
                        or state.get('retired') or state.get('inbox') is not inbox
                        or state.get('shadow') is not shadow or inbox.epoch != epoch):
                    return
                if any(a in state['assets'] and at != getattr(inbox, 'activated', {}).get(a)
                       for a, at in activated.items()):
                    # An unsubscribe/re-subscribe can cross detached work.
                    # Fence its shadow too, not only the shared publication.
                    inbox.resync.set()
                    state['ready'] = False
                    self.update_hot_coverage(time.monotonic())
                    return
                event = recording.owned_events(event, {a for a in state['assets']
                    if received_at >= getattr(inbox, 'activated', {}).get(a, received_at)})
                required = set(self.states[index]['assets'])
                live_event = recording.owned_events(event, required)
                self.continuity.receive(live_event, state['source'], received_at)
                if _PIPELINE:
                    merged = self.merges[index].filter(live_event, state['source'], required, backup=True)
                    if merged:
                        _PIPELINE.receive(merged, required, received_at, inbox, inbox.epoch)
                        self.continuity.publication(_PIPELINE.recording.metadata, required)
                        self.update_merged_gap(self.states[index])
                old = self.states[index]
                # No simultaneous all-asset timestamp equality barrier on busy
                # feeds. Each asset has its own full snapshot; the merge guards
                # stale snapshots/levels independently, while retaining trades.
                ready = (state['connected'] and bool(required) and required <= shadow.books.keys()
                         and all(shadow.recorder.metadata.get(a, (None,))[0] is not None for a in required)
                         and not required.intersection(state.get('subscribe_pending', []))
                         and not required.intersection(state.get('recover_assets', []))
                         and inbox.pending_wait_seconds() <= 1 and not inbox.delivery_gap
                         and (datetime.now(timezone.utc) - received_at).total_seconds() <= 1)
                if _PIPELINE:
                    # After live invalidation, do not restore an older shadow
                    # snapshot over the retained merge watermark.
                    ready = ready and all(
                        self.merges[index].latest.get(a) is None
                        or shadow.recorder.metadata.get(a, (None,))[0] is not None
                        and shadow.recorder.metadata[a][0] >= self.merges[index].latest[a]
                        for a in required - _PIPELINE.books.keys())
                force = bool(old.get('pong_dead') and self.promotable_backup(state))
                if not ready and not force:
                    if inbox.queue_wait_seconds <= 1 and inbox.queue.empty():
                        inbox.delivery_gap = False
                    if state.get('ready'):
                        state['ready'] = False
                        self.update_hot_coverage(time.monotonic())
                    return
                # Reset retry escalation only on usable full snapshots.
                old['backup_failures'] = 0
                if ready and not state.get('ready'):
                    state['ready'] = True
                    self.update_hot_coverage(time.monotonic())
                # A prewarmed window can activate while its market is quiet;
                # publish its assembled books without waiting for another trade.
                if _PIPELINE and ready:
                    for asset in required - _PIPELINE.books.keys():
                        at, receipt, _ = shadow.recorder.metadata[asset]
                        levels = shadow.books[asset]
                        self.merges[index].latest[asset] = self.merges[index].snapshots[asset] = at
                        restored = dict(event_type='book', asset_id=asset, timestamp=int(at.timestamp() * 1000),
                            bids=[dict(price=p, size=q) for p, q in levels['bids'].items()],
                            asks=[dict(price=p, size=q) for p, q in levels['asks'].items()])
                        self.continuity.receive(restored, state['source'], receipt)
                        _PIPELINE.receive(restored, required, receipt, inbox, inbox.epoch)
                now = time.monotonic()
                old_transport = getattr(old.get('socket'), 'transport', None)
                last_byte = max((at for at in (old['inbox'].last_byte_at,
                    getattr(old_transport, 'last_read_at', None)) if at is not None), default=None)
                quiet = min(5, ws_setting('HOT_BYTE_SILENCE_SECONDS', 4, 3))
                if (old['connected'] and last_byte is not None and now - last_byte >= quiet
                        and inbox.last_data_at is not None and inbox.last_data_at > last_byte
                        and now - inbox.last_data_at <= 1
                        and self.backup_ready(state)):
                    old.update(data_suspect=True, failure_reason='byte_silence')
                    old.setdefault('failed_at', now)
                    self.record_primary_failure(index, old, None)
                maximum = ws_setting('HOT_ROTATE_SECONDS', 0, 0)
                due = maximum > 0 and time.monotonic() - old.get('opened_at', time.monotonic()) >= maximum
                if old['connected'] and not old['inbox'].invalid.is_set() and not old.get('data_suspect') and not due:
                    return
                # REST/supervisor may refresh books/readiness, but only this
                # socket's worker can promote: it may already hold a popped delta.
                if threading.current_thread() is not state.get('worker'):
                    return
                extra = set(state['assets']) - required - set(state.get('subscribe_pending', []))
                state['assets'] = list(old['assets'])
                state['subscribe_pending'] = [a for a in state.get('subscribe_pending', []) if a in required]
                state['recover_assets'] = set(state.get('recover_assets', [])) & required
                self.subscriptions.forget(state)
                self.check_continuity(index, old, state, 'failover' if not old['connected'] else 'data_collapse' if old.get('data_suspect') else 'hot_age')
                self.failover_ms = max(0, (time.monotonic() - old.get('failed_at', time.monotonic())) * 1000)
                if 'outage_at' in old:
                    state.update(outage_at=old.pop('outage_at'), outage_wall=old['outage_wall'],
                                 outage_events=old.pop('outage_events', []))
                    for event in state['outage_events']:
                        last = event['_last_receipt']
                        receipt_gap = max(0, (datetime.now(timezone.utc) - last).total_seconds() * 1000)
                        event.update(takeover_gap_ms=receipt_gap, detection_to_takeover_ms=self.failover_ms,
                                     recovery='backup', receipt_gap_ms=receipt_gap)
                    self.complete_recovery(index, state, 'backup')
                state.update(standby=False, redundant=False, rotate_failures=0, rotate_retry_at=0,
                             backup_started=True, backup_failures=0, backup_retry_at=0)
                state['unsubscribe_pending'] = extra
                state.pop('shadow')
                self.states[index] = state
                (self.backups if self.backups.get(index) is state else self.reserves).pop(index)
                if index in self.reserves and index not in self.backups:
                    self.backups[index] = self.reserves.pop(index)
                self.update_hot_coverage(time.monotonic())
                self.backup_promotions += 1
                self.rotations += 1
                _LOG.info('Polymarket WS redundant promoted shard=%s reason=%s', index, 'hot_age' if due else 'data_collapse' if old.get('data_suspect') else 'failover')
                self.retire(old)
                if force and not ready:
                    self.request_snapshot(index, state, required)
                # Replenish the missing copy as soon as the shared dial slot permits.
                self.redundancy_tick(time.monotonic())

    def check_continuity(self, index, old, new, reason):
        preserved = bool(_PIPELINE and self.backup_ready(new) and set(new['assets']) <= _PIPELINE.books.keys())
        assets = self.continuity.switch(old.get('source'), new.get('source'), new['assets'], reason,
                                        book_preserved=preserved)
        if assets:
            new.setdefault('recover_assets', set()).update(assets)

    def rotation_tick(self, now):
        self.reap_retired()
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

    def shadow_receive(self, index, state, event, received_at, inbox, epoch=None):
        if state.get('redundant'):
            return self.redundant_receive(index, state, event, received_at, inbox, epoch)
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
                    for asset in [a for a in state['assets'] if self.balancer.assignment.get(a) == index]:
                        _PIPELINE.books.pop(asset, None)
                        _PIPELINE.recording.metadata.pop(asset, None)
                        if _PIPELINE.paper_account:
                            _PIPELINE.paper_account.history.forget(asset)
        state.pop('opened_at', None)
        state.pop('initial_sent_at', None)
        state.pop('failed_at', None)
        state.pop('data_suspect', None)
        state.pop('subscribe_pause_until', None)
        state.pop('normal_shutdown', None)
        state.pop('failure_stage', None)
        state.pop('slow_error', None)
        state.pop('close_evidence_recorded', None)
        state.pop('primary_1013_recorded', None)
        state.pop('failure_recorded', None)
        if not state.get('standby') and index in self.merges:
            # This generation needs fresh snapshots; a previous generation's
            # duplicate cache must not suppress them after its books were reset.
            previous = self.merges[index]
            self.merges[index] = FeedMerge({a: (at,) for a, at in previous.latest.items()})
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
                state['snapshot_retry_at'] = time.monotonic() + (.5 if state.get('redundant') else 10)
                assets = list(state['assets'])
                if state.get('redundant'):
                    required = state.get('required_assets', set(assets))
                    assets = [a for a in reversed(assets) if a in required] + [a for a in reversed(assets) if a not in required]
                self.subscriptions.enqueue(state, assets, initial=True)
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
                if (state.get('redundant') and not inbox.resync.is_set()
                        and inbox.pending_wait_seconds() > ws_setting('BACKUP_MAX_LAG_SECONDS', 2, 1)):
                    with inbox.lock:
                        inbox.epoch += 1
                        inbox.delivery_gap = True
                        inbox.resync.set()
                        state['ready'] = False
                if inbox.resync.is_set():
                    if state.get('standby'):
                        if not state.get('redundant'):
                            inbox.invalid.set()
                            break
                        with self.lifecycle_lock, inbox.lock:
                            self.update_hot_coverage(time.monotonic())
                            inbox.discard()
                            inbox.queue_wait_seconds = inbox.lag_seconds = 0
                            state['shadow'] = ShadowBooks(state['assets'])
                            state['ready'] = False
                            inbox.resync.clear()
                            inbox.delivery_gap = False
                            inbox.resyncs += 1
                            state.setdefault('recover_assets', set()).update(state['assets'])
                        continue
                    with self.lifecycle_lock:
                        if self.migration and index in self.migration['shadows']:
                            self.cancel_migration('destination_resync')
                    # Serialize gap recovery with publication. Never reuse a book
                    # assembled across a missing delta; request fresh full books.
                    pipeline_lock = _PIPELINE.lock if _PIPELINE else RLock()
                    with pipeline_lock, inbox.lock:
                        if (inbox.invalid.is_set() or state.get('inbox') is not inbox
                                or self.states.get(index) is not state or state.get('retired')):
                            break
                        inbox.discard()
                        if _PIPELINE:
                            for asset in [a for a in state['assets'] if self.balancer.assignment.get(a) == index]:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.forget(asset)
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
                    inbox.processing_received_at = received_at
                    inbox.processing_charged_bytes = WireQueue.charge((raw, received_at, epoch))
                    inbox.queue_wait_seconds = (datetime.now(timezone.utc) - received_at).total_seconds()
                    inbox.lag_seconds = inbox.queue_wait_seconds
                    # Queueing is latency, not loss. Only overflow/resync or a
                    # transport failure sets delivery_gap. A busy FIFO may never
                    # produce Empty, so recovery cannot depend on that timeout.
                except Empty:
                    if state.get('redundant') and inbox.queue.empty():
                        # Lag describes an outstanding frame, not the last busy
                        # frame forever. Real loss has a separate resync fence.
                        inbox.queue_wait_seconds = inbox.lag_seconds = 0
                        if not inbox.resync.is_set():
                            inbox.delivery_gap = False
                    if state.get('standby'):
                        # Only this worker promotes. A supervisor promotion
                        # could otherwise strand an already-popped shadow delta.
                        self.shadow_receive(index, state, [], datetime.now(timezone.utc), inbox)
                    continue
                try:
                    parsing_at = time.monotonic()
                    event = self.parsed_frames.parse(raw)
                    inbox.parse_max_seconds = max(inbox.parse_max_seconds, time.monotonic() - parsing_at)
                    if state.get('standby'):
                        self.shadow_receive(index, state, event, received_at, inbox, epoch)
                        continue
                    event = self.migration_receive(index, state, event, received_at, inbox)
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
                                    if self.balancer.assignment.get(a) == index
                                    and received_at >= inbox.activated.get(a, received_at)}
                                self.continuity.receive(recording.owned_events(event, continuity_assets), state['source'], received_at)
                                owned = frozenset(a for a in state['assets'] if self.balancer.assignment.get(a) == index)
                                merge = self.merges.get(index)
                                merged = merge.filter(event, state['source'], owned) if merge else event
                                if merged:
                                    _PIPELINE.receive(merged, owned, received_at, inbox, epoch)
                                    self.continuity.publication(_PIPELINE.recording.metadata, owned)
                                    self.complete_recovery(index, state, 'ws')
                                migration = self.migration
                                if migration:
                                    for shadow in migration['shadows'].values():
                                        shadow.observe_active(_PIPELINE.recording.metadata)
                                pending = self.standby
                                if pending and pending[0] == index:
                                    pending[1]['shadow'].observe_active(_PIPELINE.recording.metadata)
                                inbox.lock_wait_seconds = lock_wait
                except (ValueError, TypeError, AttributeError):
                    continue
                finally:
                    inbox.process_max_seconds = max(inbox.process_max_seconds, time.monotonic() - parsing_at)
                    inbox.processing_received_at = None
                    inbox.processing_charged_bytes = 0
                    if inbox.queue.empty():
                        inbox.queue_wait_seconds = 0

        def consume():
            try:
                process_frames()
            except Exception:
                _LOG.exception('Polymarket book worker failed shard=%s', index)
                inbox.invalid.set()
            finally:
                inbox.discard('shutdown_discarded')
                if _PIPELINE and inbox.invalid.is_set():
                    with _PIPELINE.lock:
                        if (state.get('inbox') is inbox and self.states.get(index) is state
                                and not self.backup_protects(index, state)):
                            for asset in [a for a in state['assets'] if self.balancer.assignment.get(a) == index]:
                                restored = _PIPELINE.recording.metadata.get(asset)
                                if restored and state.get('outage_wall') and restored[1] >= state['outage_wall']:
                                    continue
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.forget(asset)

        def received(_sock, raw):
            if state.get('retired') or state.get('inbox') is not inbox:
                return
            inbox.last_byte_at = time.monotonic()
            if raw in ('PONG', b'PONG'):
                if hasattr(_sock, 'transport'):
                    _sock.transport.pong()
                return
            # No JSON, rate-balancer lock or Pipeline lock on the socket reader.
            # Raw ingress totals remain accurate even when the FIFO overflows;
            # group rates are sampled only across healthy drain windows below.
            raw_bytes = len(raw.encode('utf-8')) if isinstance(raw, str) else len(raw)
            inbox.last_data_at = inbox.last_byte_at
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
                groups=sorted({self.balancer.groups[a] for a in state['assets'] if a in self.balancer.groups}),
                queue_depth=inbox.queue.qsize(), queue_high_water=inbox.high_water,
                queue_wait_seconds=inbox.queue_wait_seconds, lock_wait_seconds=inbox.lock_wait_seconds,
                drops=inbox.dropped, drop_reasons=dict(inbox.drop_reasons), resyncs=inbox.resyncs,
                callback_max_seconds=inbox.callback_max_seconds, parse_max_seconds=inbox.parse_max_seconds,
                process_max_seconds=inbox.process_max_seconds, lifecycle_wait_seconds=inbox.lifecycle_wait_seconds,
                discard_max_seconds=inbox.discard_max_seconds,
                subscribe_batches=state.get('subscribe_batches', 0),
                pair_load=self.pair_load(index, state),
                handshake_headers=dict(transport.headers) if transport else {},
                sample=dict(transport.last) if transport else {})
            if code == 1013:
                state['last_transport_failure']['byte_burst'] = transport.burst() if isinstance(transport, TransportDiagnostics) else {}
                state['last_transport_failure']['peer_byte_bursts'] = [
                    dict(source=peer.get('source'), **t.burst())
                    for i, peer in [(index, self.states.get(index, {})), *self.backup_items()]
                    if i == index and peer is not state
                    and isinstance(t := getattr(peer.get('socket'), 'transport', None), TransportDiagnostics)]
            _LOG.log(logging.INFO if code == 1001 else logging.WARNING,
                     'Polymarket WS transport failure shard=%s evidence=%s', index,
                     json.dumps(dict(state['last_transport_failure'], samples=list(transport.samples) if transport else [])))

        def failed(_sock, error):
            if self.closed or state.get('retired') or state.get('inbox') is not inbox:
                return
            self.record_primary_failure(index, state, remote_close(error)[0])
            inbox.delivery_gap = True
            state['connected'] = False
            state.setdefault('failed_at', time.monotonic())
            self.update_hot_coverage(time.monotonic())
            code, reason = remote_close(error)
            self.record_primary_1013(index, state, code)
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
            if self.closed or state.get('retired'):
                state['connected'] = False
                inbox.invalid.set()
                return
            remote_code, remote_reason = state.pop('remote_close', (None, None))
            code = code if code is not None else remote_code
            reason = reason if reason is not None else remote_reason
            self.record_primary_failure(index, state, code)
            self.record_primary_1013(index, state, code)
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
                    self.update_hot_coverage(time.monotonic())
                return
            inbox.delivery_gap = True
            state['connected'] = False
            state.setdefault('failed_at', time.monotonic())
            self.update_hot_coverage(time.monotonic())
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
            state['retry_at'] = time.monotonic() + (0 if state.get('pong_dead') else reconnect_delay(state['attempts']))

        sock = read_ahead_app(websocket.WebSocketApp)(os.getenv('POLYMARKET_OBSERVER_WS_URL', MARKET_WS_URL),
                                     on_open=opened, on_message=received, on_error=failed, on_close=closed)
        state['socket'] = sock
        # The first heartbeat differs by role; subsequent heartbeats remain 10s.
        sock.heartbeat_phase = random.uniform(3, 5) if state.get('redundant') else 10
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
        live = self.active_catalogue(self.markets)
        if {m['id'] for m in live} != {m['id'] for m in self.markets}:
            if _PIPELINE:
                _PIPELINE.set_markets(live)
            self._sync(live)
            return
        now = time.monotonic()
        self.parsed_frames.prune(now)
        self.pong_timeout_tick(now)
        tick_gap = now - self.tick_at
        self.tick_at = now
        self.tick_gap_max = max(self.tick_gap_max, tick_gap)
        self.migration_tick(now)
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
                    and set(owners) >= set(balancer.groups.values())
                    and not self.migration and now >= self.migration_retry_at):
                target = balancer.plan(owners, now)
                if (balancer.overloaded(owners) and balancer.pressure_samples >= balancer.confirmations
                        and now - balancer.last_rebalance >= balancer.minimum):
                    expanded = self.capacity_owners(rebalance=True, now=now)
                    if balancer.capacity_cost(expanded) < balancer.capacity_cost(owners):
                        target = expanded
                if target:
                    self.apply_assignment(self.assignment_for(target))
            else:
                balancer.pressure, balancer.pressure_samples = None, 0
            self.sample_unavailable.clear()
        self.reconnect_pending = False
        for index, state in list(self.states.items()) + self.backup_items() + ([self.standby] if self.standby else []):
            try:
                if state['connected'] and state.get('unsubscribe_pending'):
                    state['socket'].send(json.dumps(dict(operation='unsubscribe', assets_ids=sorted(state['unsubscribe_pending']))))
                    state.pop('unsubscribe_pending')
                if state['connected'] and now >= state.get('snapshot_retry_at', now + 10):
                    missing = (set(state['assets']) - state['shadow'].books.keys() - set(state.get('subscribe_pending', [])) if state.get('redundant')
                               else set(state['assets']) & self.continuity.pending_assets())
                    state.setdefault('recover_assets', set()).update(missing)
                    state['snapshot_retry_at'] = now + (.5 if state.get('redundant') else 10)
                recover = state.pop('recover_assets', set()) & set(state['assets'])
                if recover and state['connected'] and not state.get('subscribe_initial'):
                    if state.get('redundant'):
                        # Repair full snapshots without tearing down the live stream.
                        state['ready'] = False
                        for asset in recover:
                            state['shadow'].books.pop(asset, None)
                            state['shadow'].recorder.metadata.pop(asset, None)
                        self.update_hot_coverage(now)
                        self.request_snapshot(index, state, recover)
                    elif self.hot_shard(index):
                        self.request_snapshot(index, state, recover)
                    else:
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
        with (_PIPELINE.lock if _PIPELINE else RLock()):
            for merge in self.merges.values():
                merge.expire()
        self.rotation_tick(now)
        for index, state in self.states.items():
            if 'outage_at' in state:
                self.request_snapshot(index, state, set(state['assets']))
        for index, state in list(self.states.items()):
            if state.get('inbox') and state['inbox'].invalid.is_set() and state['socket']:
                if not state.get('queue_logged'):
                    _LOG.warning('Polymarket WS generation invalidated shard=%s drops=%s reasons=%s resyncs=%s',
                                 index, state['inbox'].dropped, state['inbox'].drop_reasons, state['inbox'].resyncs)
                    state['queue_logged'] = True
                state['socket'].close()
            if not state['connected'] and state.get('failed_at') is not None:
                if now >= state['retry_at']:
                    if any(i == index and (self.backup_ready(c) or state.get('pong_dead') and self.promotable_backup(c))
                           for i, c in self.backup_items()):
                        continue
                    # Fresh state fences late callbacks and avoids join/close
                    # handshakes on the supervisor. Retain outage accounting.
                    replacement = dict(assets=list(state['assets']), socket=None, thread=None,
                        connected=False, lock=RLock(), attempts=state['attempts'], retry_at=0, ping_at=0)
                    for key in ('outage_at', 'outage_wall', 'outage_events'):
                        if key in state:
                            replacement[key] = state[key]
                    self.continuity.switch(state.get('source'), None, state['assets'], 'reconnect')
                    self.states[index] = replacement
                    self.retire(state)
                    self.stats.increment('reconnect_count')
                    with _LOCK:
                        STATUS['reconnect_count'] += 1
                    self.connect(index, replacement)
                    self.request_snapshot(index, replacement, set(replacement['assets']))
            elif not state['thread'] or not state['thread'].is_alive():
                if now >= state['retry_at']:
                    self.connect(index, state)
            elif (state['connected'] and not state.get('subscribe_initial') and now >= state['ping_at']
                  and not getattr(state['socket'], 'text_heartbeat_active', False)):
                try:
                    transport = getattr(state['socket'], 'transport', None)
                    if isinstance(transport, TransportDiagnostics):
                        transport.ping(now)
                    state['socket'].send('PING')
                    state['ping_at'] = now + 10
                except Exception as exc:
                    state['connected'] = False
                    state['socket'].close()
                    _ws_error(exc)
        for state in list(self.states.values()) + [c for _, c in self.backup_items()]:
            inbox = state.get('inbox')
            if inbox and now - inbox.metric_at >= 1:
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
                keys = {balancer.groups[a] for a in state['assets'] if a in balancer.groups}
                if not state.get('standby') and len(keys) == 1:
                    # Dedicated sockets give unambiguous group bytes even when
                    # workers lag. Never learn backlog as low ingress demand.
                    with balancer.lock:
                        balancer.ingress_byte_rates[next(iter(keys))] = inbox.byte_rate
        cpu_now = process_time()
        cpu_cores = (cpu_now - self.cpu_seconds) / (now - self.cpu_at) if now > self.cpu_at else 0
        self.cpu_at, self.cpu_seconds = now, cpu_now
        hot_coverage = self.update_hot_coverage(time.monotonic())
        aux_memory = self.memory_metrics(now)
        with _LOCK:
            STATUS.setdefault('rebalance_count', 0)
            STATUS.update(self.balancer.metrics())
            STATUS.update(ws_process_cpu_cores=cpu_cores, ws_process_rss_bytes=process_rss_bytes(),
                          ws_aux_memory=aux_memory,
                          ws_observer_database=database_metrics(),
                          ws_metrics_session_id=self.stats.session,
                          ws_metrics_scope='observer_process_session',
                          ws_retiring_connections=len(self.retiring), ws_parsed_frames=self.parsed_frames.metrics(),
                          ws_gil_switch_ms=sys.getswitchinterval() * 1000,
                          ws_tick_gap_seconds=tick_gap, ws_tick_gap_max_seconds=self.tick_gap_max)
            STATUS['ws_last_transport_failures'] = {i: s['last_transport_failure']
                for i, s in self.states.items() if 'last_transport_failure' in s}
            STATUS['ws_shard_assets'] = [len(self.states.get(i, {}).get('assets', [])) for i in range(self.count)]
            STATUS['ws_receive_queues'] = {
                i: dict(depth=s['inbox'].queue.qsize(), drops=s.get('receive_drops', 0) + s['inbox'].dropped,
                        high_water=s['inbox'].high_water, capacity=s['inbox'].queue.maxsize,
                        charged_bytes=s['inbox'].queue.charged_bytes, byte_limit=s['inbox'].queue.byte_limit,
                        resyncs=s['inbox'].resyncs, lag_seconds=s['inbox'].lag_seconds,
                        drop_reasons=dict(s['inbox'].drop_reasons),
                        queue_wait_seconds=s['inbox'].queue_wait_seconds,
                        lock_wait_seconds=s['inbox'].lock_wait_seconds,
                        receive_rate=s['inbox'].receive_rate, receive_bytes_per_second=s['inbox'].byte_rate,
                        peak_receive_bytes_per_second=max(s.get('peak_byte_rate', 0), s['inbox'].peak_byte_rate),
                        received_frames=s['inbox'].received_frames, received_bytes=s['inbox'].received_bytes,
                        callback_max_seconds=s['inbox'].callback_max_seconds,
                        parse_max_seconds=s['inbox'].parse_max_seconds,
                        process_max_seconds=s['inbox'].process_max_seconds,
                        discard_max_seconds=s['inbox'].discard_max_seconds,
                        tcp_pending_bytes=socket_pending_bytes(s.get('socket')),
                        read_ahead_active=getattr(s.get('socket'), 'read_ahead_active', False),
                        read_calls=getattr(s.get('socket'), 'read_calls', 0),
                        transport=dict(getattr(getattr(s.get('socket'), 'transport', None), 'last', {})))
                for i, s in self.states.items() if 'inbox' in s}
            STATUS.update(ws_continuity=self.continuity.metrics(), ws_failover_ms=self.failover_ms,
                          ws_hot_backup_by_group=hot_coverage,
                          ws_hot_primary_failures={k: dict(total=v['total'], without_ready_backup=v['without_ready_backup'],
                              events=[{field: value for field, value in e.items() if not field.startswith('_')}
                                  for e in v['events']]) for k, v in self.primary_failures.items()},
                          ws_takeover_gap_scope='failed_source_receipt_to_recovery; detection_to_takeover_ms_is_control_time',
                          ws_acceptance_gap_scope='merged_gap_ms; physical_source_silence_and_historical_trades_are_alert_only',
                          ws_pong_timeout_seconds=22,
                          ws_backup_max_lag_seconds=ws_setting('BACKUP_MAX_LAG_SECONDS', 2, 1),
                          ws_rest_recovery=self.snapshot_recovery.metrics(),
                          ws_reserves={i: self.backup_metrics(i, c, now) for i, c in self.reserves.items()},
                          ws_required_hot_groups=sorted(HOT_SERIES),
                          ws_split_byte_series=[],
                          ws_migration=self.migration_metrics(now),
                          ws_primary_1013_by_shard=self.primary_1013_metrics(now),
                          ws_rotation_count=self.rotations, ws_rotation_failures=self.rotation_failures,
                          ws_rotation_last_failure=self.rotation_last_failure,
                          ws_hot_rotate_seconds=ws_setting('HOT_ROTATE_SECONDS', 0, 0),
                          ws_hot_byte_silence_seconds=min(5, ws_setting('HOT_BYTE_SILENCE_SECONDS', 4, 3)),
                          ws_backup_promotions=self.backup_promotions,
                          ws_backups={i: self.backup_metrics(i, s, now) for i, s in self.backups.items()},
                          ws_merge_metrics={i: dict(duplicates=m.duplicates, stale=m.stale, evictions=m.evictions,
                              entries=len(m.seen), capacity=m.maximum, sources_per_entry_limit=4,
                              assets=len(m.latest), levels=len(m.levels)) for i, m in self.merges.items()},
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
        _LOG.info('Polymarket WS manager local shutdown primary_shards=%s', len(self.states))
        deadline = time.monotonic() + timeout
        self.supervisor_stop.set()
        with self.lifecycle_lock:
            self.closed = True
            self.cancel_migration('shutdown')
            self.cancel_rotation('shutdown')
            for _, state in self.backup_items():
                self.retire(state)
            self.backups.clear()
            self.reserves.clear()
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

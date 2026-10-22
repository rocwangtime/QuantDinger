"""Dedicated public market observer; it never places or signs orders."""
import json
import logging
import os
import socket
import sys
import threading
import time
from time import process_time, thread_time
from .ws_warm_book import BookCheckpoints, WarmBook
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
from .ws_redundancy import DoubleKicks, FeedMerge, FeedContinuity, HotBackupCoverage, DisconnectClusters
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
    rss = process_rss_bytes()
    child_rss = _WS.child_rss_bytes() if _WS is not None and hasattr(_WS, 'child_rss_bytes') else 0
    result['ws_process_rss_bytes'] = None if rss is None and not child_rss else (rss or 0) + (child_rss or 0)
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


class DiscoveryHealth:
    """Keep only unexpired hot catalogue rows on a failed series refresh."""
    def __init__(self):
        self.previous = []
        self.all_rejected_at = None

    def update(self, markets, rejected, now=None, monotonic=None):
        now = now or datetime.now(timezone.utc)
        monotonic = time.monotonic() if monotonic is None else monotonic
        failed = {key for key in HOT_SERIES if any(
            row.get('series') == updown.series_slug(*key.split(':')[1:]) for row in rejected)}
        known = {m['id'] for m in markets}
        retained = [m for m in live_markets(self.previous, now)
                    if balance_key(m) in failed and m['id'] not in known]
        self.previous = list(markets) + retained
        if failed == HOT_SERIES:
            if self.all_rejected_at is None:
                self.all_rejected_at = monotonic
        else:
            self.all_rejected_at = None
        elapsed = monotonic - self.all_rejected_at if self.all_rejected_at is not None else 0
        return self.previous, dict(rejected_hot_groups=sorted(failed), retained_markets=len(retained),
            all_hot_rejected_seconds=elapsed, level1_alert=elapsed > 120,
            alert='all_hot_discovery_rejected' if elapsed > 120 else None)


def _market_loop():
    global _REFERENCE, _WS
    client = PublicClient()
    series_cache = {}
    discovery_health = DiscoveryHealth()
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
                        crypto, discovery_status = discovery_health.update(crypto, rejected)
                        markets.extend(crypto)
                        with _LOCK:
                            STATUS['updown_subscriptions'] = [{k: m[k] for k in ('id', 'asset', 'interval', 'endDate')} for m in crypto[:20]]
                            STATUS['updown_rejections'] = rejected[:20]
                            STATUS['discovery_health'] = dict(discovery_status, last_http_error=getattr(client, 'last_error', None))
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
                        from .ws_group_process import GroupFeeds, process_per_group_enabled
                        connections = GroupFeeds() if process_per_group_enabled() else MarketConnections()
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
        if receivers and hasattr(receivers, 'alive_workers'):
            pending = pending or receivers.alive_workers()
        elif receivers:
            pending = pending or any(
                state['thread'] and state['thread'].is_alive() for state in receivers.states.values())
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
                # One wall-clock read per publication: queue lag, publish
                # latency and last_message_at all describe this same moment.
                published = datetime.now(timezone.utc)
                if inbox:
                    inbox.lag_seconds = (published - received).total_seconds()
                event = recording.owned_events(event, owned)
                changed = ws_apply_event(event, self.books)
                self.recording.receive(event, changed, self.markets, self.asset_markets, received, owned,
                                       published=published)
                if self.paper_account:
                    self.paper_account.history.record(event, int(received.timestamp() * 1000))
                self.dirty.update(self.asset_markets[a] for a in changed if a in self.asset_markets)
                self.messages += 1
                self.last_message = time.monotonic()
                self.last_message_at = published.isoformat()
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
        # Copy under the publication lock; sort/percentile work happens after
        # release so a status read never stalls every primary worker.
        with self.lock:
            view = self.recording.metrics_view(self.books)
            groups = {a: balance_key(self.markets[m]) for a, m in self.asset_markets.items()}
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
                      'pair_rejections': dict(self.pair_rejections)}
        result.update(self.recording.metrics(view['books'], groups,
                          activity_rates(STATUS.get('ws_group_rates', {})), ws_setting('HOT_GROUP_MSG_PER_S', 100, 0),
                          view=view))
        result.update(self.references.metrics())
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
        self.wake_pending = False

    def wake(self):
        # Out-of-band notification: consumes no FIFO capacity or task counter.
        with self.queue.not_empty:
            self.wake_pending = True
            self.queue.not_empty.notify_all()

    def next_frame(self, timeout=.2):
        deadline = time.monotonic() + timeout
        with self.queue.not_empty:
            while not self.queue._qsize():
                if self.wake_pending:
                    self.wake_pending = False
                    raise Empty
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Empty
                self.queue.not_empty.wait(remaining)
            self.wake_pending = False
            item = self.queue._get()
            self.queue.not_full.notify()
            return item

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
            self.queue.unfinished_tasks -= count
            if not self.queue.unfinished_tasks:
                self.queue.all_tasks_done.notify_all()
            self.queue.not_full.notify_all()
            self.dropped += count
            self.drop_reasons[reason] += count
        discarded.clear()
        self.discard_max_seconds = max(self.discard_max_seconds, time.monotonic() - began)


READ_AHEAD_BYTES = 65536


def wake_reader(app):
    """Unblock the reader from another thread without freeing its fd.

    websocket-client's ``shutdown()`` / ``close()`` close the SSL socket. The
    next handshake can reuse that number while this reader, or its heartbeat,
    is still on the old SSL object (WRONG_VERSION_NUMBER,
    DECRYPTION_FAILED_OR_BAD_RECORD_MAC). Callers outside the reader only
    ``shutdown(SHUT_RDWR)``. ``run_forever`` teardown, on the reader, closes.
    """
    if app is None:
        return
    app.generation_retired = True
    try:
        app.keep_running = False
    except Exception:
        pass
    stop = getattr(app, 'stop_ping', None)
    if stop is not None and hasattr(stop, 'set'):
        stop.set()
    wire = getattr(app, 'sock', None)
    raw = getattr(wire, 'sock', None) if wire is not None else None
    if raw is not None and hasattr(raw, 'shutdown'):
        try:
            raw.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        return
    # Doubles and replay sockets have no SSL fd. close() only sets their stop
    # event. A live WebSocket object is left for the reader.
    if wire is None:
        closer = getattr(app, 'close', None)
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


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
                        began = time.monotonic()
                        keep_reading = read_callback()
                        app.transport.dispatch(time.monotonic() - began)
                        if not keep_reading:
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
                while (not stop_ping.wait(1) and self.keep_running
                       and not getattr(self, 'generation_retired', False)):
                    now = time.monotonic()
                    self.transport.sample(self, now)
                    if (now >= deadline and self.sock and self.subscription_ready.is_set()
                            and self.keep_running and not getattr(self, 'generation_retired', False)):
                        self.transport.ping(now)
                        self.sock.send('PING')
                        deadline = now + self.ping_interval
            except Exception as exc:
                self._callback(self.on_error, exc)
                # Wake the reader. This thread must not close the SSL fd.
                wake_reader(self)
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


def book_fallback_seconds():
    """First resubscribe wait when a REST attempt did not release the hold.

    Production REST p95 is 285ms and the max is 378ms. 0.8s still covers a slow
    success. A rate-limited request returns False and resubscribes immediately
    instead of waiting out this delay.
    """
    return 0.8


def book_fallback_repeat_seconds():
    """Retry interval after the first fallback, while the hold is still up."""
    return 1.5


# A long REST miss must not flush an unbounded burst into the primary queue.
HELD_CHANGES_MAX = 256
# A liveness backup is healthy while envelopes are still arriving, or while
# the socket has answered PING inside this same window. 15s is shorter than
# the 22s PONG timeout. Exchange timestamps are not compared to the wall clock:
# liveness_at is the monotonic time the frame was received.
LIVENESS_ENVELOPE_SECONDS = 15
LIVENESS_SILENCE_COOLDOWN_SECONDS = 60


class _LivenessShadow:
    """Placeholder so status can read a backup that does not rebuild a ladder."""

    def __init__(self, assets):
        self.assets = set(assets)
        self.books = {}
        self.lock = RLock()
        self.merge = FeedMerge({}, deduplicate=False)
        self.recorder = recording.Recorder()
        self.recorder.enabled = False

    def prune(self, assets):
        self.assets = set(assets)

    def observe_active(self, metadata):
        return None

    def receive(self, event, received):
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
        self.double_kicks = DoubleKicks()
        self.remote_closes = deque(maxlen=128)
        self.disconnect_clusters = DisconnectClusters()
        self.cache_maintenance_at = 0
        self.sampler = None
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
        self.rotations = self.rotation_failures = self.backup_wire_failures = 0
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
        self.tick_gap_last = 0
        self.status_at = -float('inf')
        self.hot_coverage_rows = {}
        self.closed = False
        self.pong_checked_at = 0
        self.memory_at = 0
        self.aux_memory = {}
        self.markets = []
        self.prewarm_markets = []
        # Per-group processes set this so a backup parse cannot hold the GIL
        # while its own primary queue is old. In-process callers leave it off.
        self.primary_first = False
        self.primary_first_lag_seconds = 0.05
        # Feed processes set this. Backups then check liveness and hash only.
        self.liveness_backup = False
        # Primary byte silence seen from a receiving liveness spare (per group).
        self.liveness_silence = {}
        self.stage_lock = RLock()
        self.stage_cpu = dict(primary_parse=0.0, primary_apply=0.0, backup_parse=0.0, backup_apply=0.0)

    def backup_items(self):
        return list(self.backups.items()) + [(key[0] if isinstance(key, tuple) else key, row)
                                           for key, row in self.reserves.items()]

    def remove_backup(self, index, state):
        for holder in (self.backups, self.reserves):
            for key, row in list(holder.items()):
                if row is state:
                    holder.pop(key, None)
        if index not in self.backups:
            for key, row in list(self.reserves.items()):
                shard = key[0] if isinstance(key, tuple) else key
                if shard == index:
                    self.backups[index] = self.reserves.pop(key)
                    break

    def backup_pong_fresh(self, state, now=None):
        now = time.monotonic() if now is None else now
        age = self._liveness_pong_age(state, now)
        # First PING is sent within 10s. Fresh envelopes permit bootstrap only.
        if age is None:
            return now - state.get('opened_at', state.get('created_at', -float('inf'))) <= 10
        return age <= 15

    def best_backup(self, index):
        candidates = [c for i, c in self.backup_items() if i == index and (self.backup_ready(c)
            or self.states.get(index, {}).get('pong_dead') and self.promotable_backup(c) and self.backup_pong_fresh(c))]
        return min(candidates, key=lambda c: (c['inbox'].pending_wait_seconds(),
            -c.get('opened_at', 0)), default=None)

    def primary_pressure(self, index, now):
        old = self.states.get(index)
        if not old or not old.get('connected') or old.get('data_suspect'):
            return False
        candidate = self.best_backup(index)
        inbox = old.get('inbox')
        lag = inbox.pending_wait_seconds() if inbox else 0
        if (not candidate or lag < .5 or lag - candidate['inbox'].pending_wait_seconds() < .25
                or now - old.get('last_pressure_promotion', -float('inf')) < 30):
            old.pop('pressure_since', None)
            return False
        since = old.setdefault('pressure_since', now)
        if now - since < 1:
            return False
        old.update(data_suspect=True, failure_reason='ingress_pressure', failed_at=now)
        self.record_primary_failure(index, old, None)
        return True

    def _primary_blocks_backup(self, index, state):
        """Pause this backup worker while its primary queue is actually old.

        Only a process that sets primary_first uses the rule. A disconnected
        or invalidated primary is never paused, otherwise promotion cannot run.
        An empty queue is not a backlog: pending_wait_seconds keeps the previous
        frame's receive time after the worker blocks in queue.get, and that age
        would climb on an idle socket.
        """
        if not self.primary_first or not state.get('redundant'):
            return False
        primary = self.states.get(index)
        if primary is None or primary is state or not primary.get('connected'):
            return False
        inbox = primary.get('inbox')
        if inbox is None or inbox.invalid.is_set():
            return False
        try:
            if inbox.queue.qsize() <= 0:
                return False
            return inbox.pending_wait_seconds() > self.primary_first_lag_seconds
        except Exception:
            return False

    def redundant_seen(self, state):
        """Assets whose backup book is current enough to skip a snapshot repair."""
        if self.liveness_backup:
            return set(state.get('liveness_assets') or ())
        shadow = state.get('shadow')
        return set(shadow.books.keys()) if shadow is not None else set()

    def _charge(self, name, started, state=None):
        if started is None:
            return
        spent = thread_time() - started
        with self.stage_lock:
            self.stage_cpu[name] = self.stage_cpu.get(name, 0.0) + spent
            if state is not None:
                stages = state.setdefault('stage_cpu', {})
                stages[name] = stages.get(name, 0.0) + spent

    def _liveness_required(self, index, state):
        primary = self.states.get(index) if index is not None else None
        if primary is not None and primary is not state:
            return {str(asset) for asset in (primary.get('assets') or [])}
        return {str(asset) for asset in (state.get('required_assets') or state.get('assets') or [])}

    def _liveness_envelope_fresh(self, state, now=None):
        last = state.get('liveness_at')
        if last is None:
            return False
        now = time.monotonic() if now is None else now
        return now - last <= LIVENESS_ENVELOPE_SECONDS

    def _liveness_pong_age(self, state, now):
        """Age of the last TEXT PONG. None when this socket has never answered."""
        socket = state.get('socket')
        transport = getattr(socket, 'transport', None) if socket is not None else None
        if transport is None:
            return None
        pongs = getattr(transport, 'pongs', None)
        pong_at = getattr(transport, 'pong_at', None)
        if pongs and pong_at is not None:
            return now - pong_at
        last = getattr(transport, 'last', None)
        if not isinstance(last, dict):
            return None
        try:
            count = int(last.get('pongs') or 0)
        except (TypeError, ValueError):
            return None
        if count <= 0:
            return None
        age = last.get('pong_age_seconds')
        return age if isinstance(age, (int, float)) else None

    def _liveness_link_fresh(self, state, now=None):
        """Envelope receive time, or a PONG no older than the envelope window.

        A quiet print does not drop a spare that has already seen every
        required asset and is still answering PING. A socket that stops
        answering falls out of this window before the 22s PONG timeout.
        """
        now = time.monotonic() if now is None else now
        if self._liveness_envelope_fresh(state, now):
            return True
        age = self._liveness_pong_age(state, now)
        return age is not None and age <= LIVENESS_ENVELOPE_SECONDS

    def _queue_lag_breaks_backup(self, state, inbox):
        """A deep backup queue rewinds a shadow ladder. Liveness has none.

        Production sat at 1.5–1.9 cores with the spare queue past the 1s ready
        limit and often past the 2s resync. That is when the spare has to stay
        promotable. Overflow still raises resync through ReceiveQueue.put.
        """
        if self.liveness_backup and state.get('redundant'):
            return False
        return bool(state.get('redundant') and inbox and not inbox.resync.is_set()
                    and inbox.pending_wait_seconds() > ws_setting('BACKUP_MAX_LAG_SECONDS', 2, 1))

    def liveness_healthy(self, index, state):
        """Ready without a shadow ladder.

        Connection is up, the generation is intact (not invalid, resync, or a
        delivery gap), the required assets are subscribed, and each has a
        recent timestamped envelope. A book hash is kept when one arrives.
        Queue depth, catch-up against the primary ladder, and "no full book
        yet" are shadow-book gates. They stay off this path: production books
        are sparse and a loaded backup queue is exactly when promotion is needed.
        """
        inbox = state.get('inbox')
        if not state.get('redundant') or not state.get('connected') or state.get('retired') or state.get('data_suspect'):
            return False
        if not inbox or inbox.invalid.is_set() or inbox.resync.is_set() or inbox.delivery_gap:
            return False
        required = self._liveness_required(index, state)
        if not required or state.get('subscribe_initial'):
            return False
        if required.intersection(state.get('subscribe_pending') or []):
            return False
        if not required <= {str(asset) for asset in (state.get('liveness_assets') or ())}:
            return False
        hashes = state.get('liveness_hash') or {}
        if any(asset in hashes and not hashes[asset] for asset in required):
            return False
        return self._liveness_link_fresh(state) and self.backup_pong_fresh(state)

    def liveness_receive(self, index, state, raw, received_at, inbox, epoch=None):
        """Track link health and independently maintain a bounded warm ladder."""
        from .ws_payloads import cheap_envelope, decoded_envelope, loads
        epoch = inbox.epoch if epoch is None else epoch
        activated = dict(getattr(inbox, 'activated', {}))
        if inbox.invalid.is_set() or inbox.resync.is_set() or epoch != inbox.epoch or state.get('retired'):
            return
        shadow = state.get('shadow')
        tips = None
        if isinstance(shadow, WarmBook):
            parse_at = thread_time()
            event = loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            tips = decoded_envelope(event)
            self._feed_freshness(state, tips, received_at)
            self._charge('backup_parse', parse_at, state)
            apply_at = thread_time()
            event = recording.owned_events(event, {a for a in state['assets']
                if received_at >= activated.get(a, received_at)})
            shadow.receive(event, received_at)
            self._charge('backup_apply', apply_at, state)
            with self.lifecycle_lock, inbox.lock:
                if (state.get('shadow') is not shadow or state.get('inbox') is not inbox
                        or epoch != inbox.epoch or activated != getattr(inbox, 'activated', {})
                        or inbox.invalid.is_set() or inbox.resync.is_set() or state.get('retired')):
                    # A detached parse crossed a subscription/queue generation.
                    # Never prove continuity with the contaminated private book.
                    if state.get('shadow') is shadow:
                        inbox.resync.set()
                    return
        assets = {str(asset) for asset in (state.get('assets') or [])}
        now = time.monotonic()
        for kind, asset, stamp, digest in tips if tips is not None else cheap_envelope(raw):
            if asset not in assets or not stamp:
                continue
            state.setdefault('liveness_assets', set()).add(asset)
            state['liveness_at'] = now
            if kind == 'book' and digest:
                state.setdefault('liveness_hash', {})[asset] = digest
                pending = state.get('recover_assets')
                if pending:
                    pending.discard(asset)
        healthy = self.liveness_healthy(index, state)
        if healthy != bool(state.get('ready')):
            state['ready'] = healthy
            self.update_hot_coverage(now)
        if healthy:
            state['ever_ready'] = True
            if isinstance(shadow, WarmBook):
                supply_at = thread_time()
                self._supply_liveness_backup(index, state, event, received_at, inbox, shadow)
                self._charge('backup_fanin', supply_at, state)
            self._liveness_byte_silence(index, state, now)
        self._promote_liveness_backup(index, state)

    def _supply_liveness_backup(self, index, state, event, received_at, inbox, shadow):
        """Publish a verified spare without predicting a physical link failure.

        Natural quiet periods cause no promotion. Once a common book baseline
        is verified, the normal merge fences duplicate and out-of-order deltas.
        Verification is local to both subscription/queue generations.
        """
        old = self.states.get(index)
        now = time.monotonic()
        if (not _PIPELINE or old is None or old is state
                or (datetime.now(timezone.utc)-received_at).total_seconds() > .5):
            return
        supply, lagging = self._freshness_decision(index, old, state, now)
        if not supply:
            return
        with self.lifecycle_lock, _PIPELINE.lock, inbox.lock:
            old = self.states.get(index)
            if (old is None or old is state or state.get('shadow') is not shadow
                    or state.get('inbox') is not inbox or not self.backup_ready(state)
                    or not any(i == index and c is state for i, c in self.backup_items())):
                return
            primary_inbox = old.get('inbox')
            if primary_inbox is None or primary_inbox.resync.is_set():
                return
            required = {a for a in old['assets'] if self.balancer.assignment.get(a) == index}
            generation = (old.get('source'), id(primary_inbox), primary_inbox.epoch, inbox.epoch,
                          frozenset(getattr(primary_inbox, 'activated', {}).items()),
                          frozenset(getattr(inbox, 'activated', {}).items()), frozenset(required))
            if state.get('_supply_generation') != generation:
                state['_supply_generation'] = generation
                state['_supply_verified'] = set()
            verified = state['_supply_verified']
            merge = self.merges.get(index)
            if merge is None:
                return
            baselines = []
            for asset in required-verified:
                baseline = shadow.baseline(asset, old.get('book_checkpoints'),
                    _PIPELINE.books.get(asset), merge.latest.get(asset))
                if baseline is not None:
                    verified.add(asset)
                    if isinstance(baseline, dict):
                        baselines.append(baseline)
            if not verified:
                return
            if lagging and required <= verified and required <= _PIPELINE.books.keys():
                self._liveness_upstream_lag(index, old, state, now)
            live_event = recording.owned_events(event, required & verified)
            # Preserve the real source receive clock, including before failure.
            self.continuity.receive(live_event, state['source'], received_at)
            if baselines:
                restored = merge.filter(baselines, state['source'], required, backup=True)
                if restored:
                    shadow.handoff_watermarks(merge, {str(payload.get('asset_id'))
                        for kind, payload in recording.events(restored) if kind == 'book'})
                    _PIPELINE.receive(restored, required, received_at, inbox, inbox.epoch)
            merged = merge.filter(live_event, state['source'], required & verified, backup=True)
            if merged:
                _PIPELINE.receive(merged, required & verified, received_at, inbox, inbox.epoch)
            if baselines or merged:
                # Checkpoints describe the shared book, so a later promotion
                # can verify overlap after the old physical source fell behind.
                checkpoints = old.setdefault('book_checkpoints', BookCheckpoints())
                checkpoints.receive(restored if baselines else [])
                checkpoints.receive(merged)
                self.continuity.publication(_PIPELINE.recording.metadata, required)
                state['merged_supply_at'] = now
                state['_freshness_supply_primary'] = old.get('source')
                state['early_supply_publications'] = state.get('early_supply_publications', 0)+1
                self.complete_recovery(index, old, 'warm_backup_fanin')

    def _feed_freshness(self, state, tips, received):
        """Exchange age at ingress, before any local apply/merge stale fence."""
        marks = state.setdefault('feed_freshness', {})
        for asset in list(marks):
            if asset not in state.get('assets', ()):
                marks.pop(asset, None)
        for kind, asset, stamp, digest in tips or ():
            if kind not in ('book', 'price_change') or asset not in state.get('assets', ()):
                continue
            at = recording.exchange_time(stamp)
            if at is not None:
                marks[asset] = (at, received)

    @staticmethod
    def _pong_rtt(state):
        transport = getattr(state.get('socket'), 'transport', None)
        return getattr(transport, 'pong_rtt_seconds', None)

    def _freshness_decision(self, index, old, spare, now):
        """Fresh sibling book evidence is mandatory; quiet alone never retires."""
        wall = datetime.now(timezone.utc)
        mine = spare.get('feed_freshness', {})
        peer = old.get('feed_freshness', {})
        newer = []
        for asset, (at, received) in list(mine.items()):
            if self.balancer.assignment.get(asset) != index or (wall-received).total_seconds() > .5:
                continue
            previous = peer.get(asset)
            published = self.continuity.published.get(asset)
            floor = previous[0] if previous else (published or {}).get('exchange_at')
            if floor is not None and at > floor:
                newer.append((asset, at, received, previous))
        if not newer:
            old.pop('_upstream_lag_evidence', None)
            return False, False
        quiet = any((row := self.continuity.published.get(a)) is not None
                    and (wall-row['published_at']).total_seconds() >= .25 for a, *_ in newer)
        active = (spare.get('_freshness_supply_primary') == old.get('source')
                  and now < spare.get('_freshness_supply_until', 0))
        primary_rtt, backup_rtt = self._pong_rtt(old), self._pong_rtt(spare)
        rtt_bad = (primary_rtt is not None and primary_rtt > 1.5
                   and backup_rtt is not None and backup_rtt < .5)
        age_bad = any(previous is not None and (wall-previous[1]).total_seconds() <= .5
            and (previous[1]-previous[0]).total_seconds() > 1.5
            and (previous[1]-previous[0] - (received-at)).total_seconds() > 1
            for _, at, received, previous in newer)
        inbox = old.get('inbox')
        # A queued/processing frame is local pressure, even if PONG is delayed.
        local_wait = max(inbox.pending_wait_seconds(), inbox.queue_wait_seconds) if inbox else 0
        if local_wait >= .25:
            old.pop('_upstream_lag_evidence', None)
            old['freshness_classification'] = 'ingress_pressure'
            if quiet:
                spare['_freshness_supply_until'] = now+.5
            return quiet or active, False
        if not (rtt_bad or age_bad):
            old.pop('_upstream_lag_evidence', None)
            if quiet:
                spare['_freshness_supply_until'] = now+.5
            return quiet or active, False
        since, last = old.get('_upstream_lag_evidence', (now, now))
        if now-last > .5:
            since = now
        old['_upstream_lag_evidence'] = (since, now)
        old['freshness_classification'] = 'upstream_lag'
        supply = quiet or rtt_bad or now-since >= 1
        if supply:
            spare['_freshness_supply_until'] = now+.5
        return supply or active, now-since >= 2

    def _liveness_upstream_lag(self, index, old, spare, now):
        with self.lifecycle_lock:
            if (self.states.get(index) is not old or old.get('data_suspect')
                    or not old.get('connected') or not self.backup_ready(spare)):
                return False
            if not self._freshness_decision(index, old, spare, time.monotonic())[1]:
                return False
            # TCP backlog is local ingress pressure, never upstream evidence.
            if (socket_pending_bytes(old.get('socket')) or 0) > 0:
                old.pop('_upstream_lag_evidence', None)
                old['freshness_classification'] = 'ingress_pressure'
                return False
            old.update(data_suspect=True, failure_reason='upstream_lag', failed_at=now)
            self.record_primary_failure(index, old, None)
            return True

    def silence_reason(self, state):
        pending = socket_pending_bytes(state.get('socket'))
        inbox = state.get('inbox')
        lag = inbox.pending_wait_seconds() if inbox else 0
        reason = 'ingress_pressure' if (pending or 0) > 0 or lag >= 4 else 'byte_silence'
        state['silence_evidence'] = dict(reason=reason, tcp_pending_bytes=pending,
            queue_wait_seconds=lag, remote_silence_confirmed=pending == 0 and lag < 4)
        return reason

    def transfer_outage(self, index, old, new):
        # Every promotion gets an event; final gap is closed by publication.
        if not old.get('failure_recorded'):
            old.setdefault('failure_reason', 'planned_rotation')
            self.record_primary_failure(index, old, None)
        if 'outage_at' in old:
            new.update(outage_at=old.pop('outage_at'), outage_wall=old['outage_wall'],
                       outage_events=old.pop('outage_events', []))

    def _liveness_byte_silence(self, index, state, now=None):
        """Mark a silent primary suspect while this spare still receives.

        The shadow path does this in redundant_receive; a liveness spare never
        runs that, so without this a dead link waits for the 22s PONG timeout
        (09:36 ETH15m on 2026-10-20: 24s gap while the spare read 205 KB/s).
        Requires the primary to be byte-quiet (data and PONG) for the hot
        silence window and at least 1s of continuing newer spare envelopes.
        A single early copy after natural quiet cannot retire the primary.
        Any primary byte resets this evidence. Idempotence belongs to the failed physical generation;
        a newly promoted socket must never inherit another socket's cooldown.
        """
        now = time.monotonic() if now is None else now
        old = self.states.get(index)
        if (old is None or old is state or not old.get('connected') or old.get('data_suspect')
                or old.get('pong_dead') or old.get('retired') or old.get('standby')):
            return False
        old_inbox = old.get('inbox')
        if old_inbox is None or old_inbox.invalid.is_set():
            return False
        transport = getattr(old.get('socket'), 'transport', None)
        last_byte = max((at for at in (getattr(old_inbox, 'last_byte_at', None), getattr(transport, 'last_read_at', None))
                         if at is not None), default=None)
        if last_byte is None:
            return False
        quiet = min(5, ws_setting('HOT_BYTE_SILENCE_SECONDS', 4, 3))
        mine = state.get('liveness_at')
        if now - last_byte < .25 or mine is None or mine <= last_byte or now - mine > 1:
            return False
        received = getattr(state.get('inbox'), 'last_data_at', None)
        if received is not None and received <= last_byte:
            return False
        prior_byte, since, previous = old.get('_peer_silence_evidence', (None, now, now))
        if prior_byte != last_byte or now-previous > 1:
            since = now
        old['_peer_silence_evidence'] = (last_byte, since, now)
        if now-last_byte < quiet or now-since < 1:
            return False
        with self.lifecycle_lock:
            current_byte = max((at for at in (getattr(old_inbox, 'last_byte_at', None),
                getattr(transport, 'last_read_at', None)) if at is not None), default=None)
            if (self.states.get(index) is not old or old.get('inbox') is not old_inbox
                    or not old.get('connected') or old.get('data_suspect') or old.get('retired')
                    or current_byte != last_byte or not self.backup_ready(state)):
                return False
            # Mark before evidence I/O releases the GIL. Another spare must
            # not count the same physical generation a second time.
            old['data_suspect'] = True
            old['failure_reason'] = self.silence_reason(old)
            groups = sorted({self.balancer.groups.get(a, '').split(':outcome:')[0]
                             for a in old.get('assets') or []} - {''})
            key = groups[0] if groups else str(index)
            row = self.liveness_silence.setdefault(key, dict(promotions=0, suppressed=0, last_at=None, last_silence_s=None))
            silence = round(now - last_byte, 3)
            row.update(promotions=row['promotions'] + 1, last_at=datetime.now(timezone.utc).isoformat(),
                       last_silence_s=silence, _last_monotonic=now)
            row.update(old['silence_evidence'])
            old.setdefault('failed_at', now)
            _LOG.warning('Polymarket WS primary byte silence group=%s shard=%s silence_s=%s spare=%s',
                         key, index, silence, state.get('source'))
            self.record_primary_failure(index, old, None)
            return True

    def _remember_books(self, state, event, checkpoint=True):
        if checkpoint:
            checkpoints = state.setdefault('book_checkpoints', BookCheckpoints())
            if state.get('assets'):
                checkpoints.prune(set(state['assets']))
            checkpoints.receive(event)
        marks = state.setdefault('book_marks', {})
        for kind, payload in recording.events(event):
            if kind != 'book':
                continue
            asset = str(payload.get('asset_id') or payload.get('assetId') or '')
            if asset:
                marks[asset] = (recording.exchange_time(payload.get('timestamp')), payload.get('hash'),
                                payload.get('timestamp'))

    def _flush_held(self, state):
        from .ws_payloads import snapshot_keeps_change
        pending = set(state.get('need_book') or ())
        marks = state.get('book_marks') or {}
        waiting, ready = [], []
        for item in state.get('held_changes') or []:
            payload = item.get('payload') or item
            field = 'price_changes' if 'price_changes' in payload else 'priceChanges' if 'priceChanges' in payload else None
            rows = [row for row in (payload.get(field) or []) if isinstance(row, dict)] if field else []
            stamp = recording.exchange_time(payload.get('timestamp'))
            keep, hold = [], []
            for row in rows:
                asset = str(row.get('asset_id') or row.get('assetId') or '')
                if asset in pending:
                    hold.append(row)
                    continue
                mark = marks.get(asset)
                if mark and not snapshot_keeps_change(stamp, row.get('hash'), mark[0], mark[1]):
                    continue
                keep.append(row)
            if hold and field:
                body = dict(payload)
                body[field] = hold
                waiting.append(dict(item, payload=body) if 'payload' in item else body)
            if keep and field:
                body = dict(payload)
                body[field] = keep
                ready.append(body)
        state['held_changes'] = waiting
        return ready

    def _release_resync(self, state, event):
        """Apply books now. Buffer price changes until that asset's snapshot."""
        pending = state.get('need_book')
        if not pending and not state.get('held_changes'):
            self._remember_books(state, event, checkpoint=False)
            return event
        apply = []
        items = event if isinstance(event, list) else [event]
        for item in items:
            if not isinstance(item, dict):
                continue
            kind = item.get('event_type') or item.get('type')
            payload = item.get('payload') or item
            if kind == 'book':
                asset = str(payload.get('asset_id') or payload.get('assetId') or '')
                if asset:
                    state.setdefault('book_marks', {})[asset] = (
                        recording.exchange_time(payload.get('timestamp')), payload.get('hash'),
                        payload.get('timestamp'))
                    if pending and asset in pending:
                        pending.discard(asset)
                apply.append(item)
                continue
            if kind == 'price_change' and pending:
                field = 'price_changes' if 'price_changes' in payload else 'priceChanges' if 'priceChanges' in payload else None
                rows = payload.get(field) or [] if field else []
                assets = {str(row.get('asset_id') or row.get('assetId') or '') for row in rows if isinstance(row, dict)}
                if assets & set(pending):
                    self._hold_change(state, item)
                    continue
            apply.append(item)
        apply.extend(self._flush_held(state))
        if pending is not None and not pending:
            state.pop('need_book', None)
            self._cancel_book_fallback(state)
        if not apply:
            return None
        if len(apply) == 1 and not isinstance(event, list):
            return apply[0]
        return apply

    def _hold_change(self, state, item):
        held = state.setdefault('held_changes', [])
        held.append(item)
        overflow = len(held) - HELD_CHANGES_MAX
        if overflow > 0:
            del held[:overflow]
            state['held_changes_dropped'] = state.get('held_changes_dropped', 0) + overflow

    def _accept_rest_books(self, state, events):
        """Release assets whose book is in `events` and not older than the last one.

        `events` must be the books FeedMerge actually wrote. A stale or duplicate
        REST book is absent, so that asset stays held.
        """
        pending = state.get('need_book')
        if not pending:
            return []
        for kind, payload in recording.events(events):
            if kind != 'book':
                continue
            asset = str(payload.get('asset_id') or payload.get('assetId') or '')
            if asset not in pending:
                continue
            at = recording.exchange_time(payload.get('timestamp'))
            previous = (state.get('book_marks') or {}).get(asset)
            if previous and previous[0] is not None and at is not None and at < previous[0]:
                continue
            state.setdefault('book_marks', {})[asset] = (at, payload.get('hash'), payload.get('timestamp'))
            pending.discard(asset)
        extra = self._flush_held(state)
        if not pending:
            state.pop('need_book', None)
            self._cancel_book_fallback(state)
        return extra

    def _cancel_book_fallback(self, state):
        state['book_fallback_token'] = state.get('book_fallback_token', 0) + 1

    def _resubscribe_books(self, index, state):
        """Ask the server to push a book for assets still held."""
        if self.states.get(index) is not state or state.get('retired') or not state.get('connected'):
            return False
        sock = state.get('socket')
        pending = sorted(state.get('need_book') or ())
        if sock is None or not pending:
            return False
        try:
            sock.send(json.dumps(dict(assets_ids=pending, operation='unsubscribe')))
            sock.send(json.dumps(dict(assets_ids=pending, type='market', operation='subscribe')))
        except Exception:
            _LOG.exception('Polymarket WS book resubscribe failed shard=%s', index)
            return False
        return True

    def _arm_book_fallback(self, index, state):
        """Resubscribe assets still waiting. First wait, then repeat until release."""
        if not state.get('need_book'):
            return
        token = state.get('book_fallback_token', 0) + 1
        state['book_fallback_token'] = token
        first = book_fallback_seconds()
        repeat = book_fallback_repeat_seconds()

        def fire():
            delay = first
            while True:
                if delay > 0 and self.supervisor_stop.wait(delay):
                    return
                with self.lifecycle_lock:
                    if state.get('book_fallback_token') != token or not state.get('need_book'):
                        return
                    if self.states.get(index) is not state or state.get('retired') or not state.get('connected'):
                        return
                    self._resubscribe_books(index, state)
                delay = repeat

        threading.Thread(target=fire, name='polymarket-book-fallback', daemon=True).start()

    def wake_backups(self, index, failed):
        # Only a confirmed failed current generation can send the hint.
        if self.states.get(index) is not failed or failed.get('connected') or failed.get('standby'):
            return
        for i, candidate in self.backup_items():
            inbox = candidate.get('inbox')
            if i == index and inbox is not None:
                inbox.wake()

    def _promote_liveness_backup(self, index, state):
        """Hand off a checked warm baseline; repair only unverified assets."""
        if threading.current_thread() is not state.get('worker'):
            return False
        old = self.states.get(index)
        if old is None or old is state or not self.backup_ready(state) or self.best_backup(index) is not state:
            return False
        if old.get('connected') and not old.get('inbox').invalid.is_set() and not old.get('data_suspect') and not old.get('pong_dead'):
            return False
        with self.lifecycle_lock:
            old = self.states.get(index)
            if old is None or old is state or not state.get('connected'):
                return False
            inbox = old.get('inbox')
            if old.get('connected') and inbox is not None and not inbox.invalid.is_set() and not old.get('data_suspect') and not old.get('pong_dead'):
                return False
            if self.best_backup(index) is not state:
                return False
            required = list(old.get('assets') or [])
            preserved = bool(_PIPELINE and set(required) <= set(getattr(_PIPELINE, 'books', {})))
            self.continuity.switch(old.get('source'), state.get('source'), required, 'failover', book_preserved=preserved)
            extra = set(state.get('assets') or []) - set(required)
            state['assets'] = list(required)
            # A ready link can still lack a verified baseline (startup/rollover).
            pending, baselines = set(required), []
            shadow = state.get('shadow')
            if isinstance(shadow, WarmBook) and _PIPELINE:
                with _PIPELINE.lock:
                    for asset in required:
                        baseline = shadow.baseline(asset, old.get('book_checkpoints'),
                            _PIPELINE.books.get(asset), self.merges.get(index).latest.get(asset)
                            if self.merges.get(index) else None)
                        if baseline is not None:
                            pending.discard(asset)
                            if isinstance(baseline, dict):
                                baselines.append(baseline)
            state['need_book'] = pending
            state['baseline_handoff'] = dict(verified=sorted(set(required)-pending), rest=sorted(pending))
            if isinstance(shadow, WarmBook):
                state['book_checkpoints'] = shadow.checkpoints
            self.continuity.verified_baseline(set(required)-pending)
            # The promoted socket never parsed a ladder, so it has no hash to echo.
            # Carry the primary's last book so REST can release each asset immediately.
            required_ids = {str(asset) for asset in required}
            carried = {str(asset): mark for asset, mark in (old.get('book_marks') or {}).items()
                       if str(asset) in required_ids}
            if carried:
                state['book_marks'] = {**dict(state.get('book_marks') or {}), **carried}
            self.transfer_outage(index, old, state)
            state.update(standby=False, redundant=False,
                         promoted_at=datetime.now(timezone.utc).isoformat(),
                         rotate_failures=0, rotate_retry_at=0, backup_started=True,
                         backup_failures=0, backup_retry_at=0, last_pressure_promotion=time.monotonic())
            state['unsubscribe_pending'] = extra
            promoted = time.monotonic()
            for event in state.get('outage_events', []):
                event.update(promotion_at=state['promoted_at'],
                    detection_to_promotion_ms=max(0, (promoted-old.get('outage_at', old.get('failed_at', promoted)))*1000),
                    baseline_verified=state['baseline_handoff']['verified'],
                    need_book_assets=state['baseline_handoff']['rest'])
            self.states[index] = state
            self.remove_backup(index, state)
            self.backup_promotions += 1
            self.rotations += 1
            self.failover_ms = max(0, (time.monotonic() - old.get('failed_at', time.monotonic())) * 1000)
            self.update_hot_coverage(time.monotonic())
            self.retire(old)
            if baselines:
                now_wall = datetime.now(timezone.utc)
                with _PIPELINE.lock:
                    merged = self.merges[index].filter(baselines, state['source'], set(required))
                    if merged:
                        shadow.handoff_watermarks(self.merges[index],
                            {str(payload.get('asset_id')) for kind, payload in recording.events(merged) if kind == 'book'})
                        _PIPELINE.receive(merged, set(required), now_wall, state['inbox'], state['inbox'].epoch)
                        self.continuity.publication(_PIPELINE.recording.metadata, set(required))
                        self.complete_recovery(index, state, 'warm_backup')
            state.pop('shadow', None)
            if pending:
                state['need_book_at'] = datetime.now(timezone.utc).isoformat()
                self.request_snapshot(index, state, [a for a in required if a in pending])
            else:
                state.pop('need_book', None)
            if state.get('need_book'):
                self._arm_book_fallback(index, state)
            self.redundancy_tick(time.monotonic())
        return True

    @staticmethod
    def backup_queue_limit(candidate):
        # A feed actively publishing unique merged updates gets 500ms of
        # scheduling grace. Missing/invalid generations never pass this alone.
        return 1.5 if time.monotonic() - candidate.get('merged_supply_at', -float('inf')) <= .25 else 1

    def backup_caught_up(self, index, candidate):
        shadow = candidate.get('shadow')
        if not shadow or index not in self.states:
            return True
        merge = self.merges.get(index)
        limit = .75
        return all(shadow.recorder.metadata.get(a, (None,))[0] is not None
            and (not merge or merge.latest.get(a) is None
                 or (merge.latest[a] - shadow.recorder.metadata[a][0]).total_seconds() <= limit)
            for a in self.states[index]['assets'])

    def backup_ready(self, candidate):
        # Readers may hold a backup_items() snapshot across worker promotion.
        # The same physical object is now primary and is never its own spare.
        if candidate and self.states.get(candidate.get('primary_index')) is candidate:
            return False
        inbox = candidate.get('inbox') if candidate else None
        required = set(candidate.get('required_assets', candidate.get('assets', []))) if candidate else set()
        if candidate and candidate.get('primary_index') in self.states:
            required = set(self.states[candidate['primary_index']]['assets'])
        if self.liveness_backup and candidate and candidate.get('redundant'):
            shard = candidate.get('primary_index')
            if shard is None:
                shard = next((i for i, item in self.backup_items() if item is candidate), None)
            return self.liveness_healthy(shard, candidate)
        shadow = candidate.get('shadow') if candidate else None
        return bool(candidate and candidate.get('ready') and candidate.get('connected') and inbox
                    and required
                    and (not shadow or required <= shadow.books.keys()
                         and all(shadow.recorder.metadata.get(a, (None,))[0] is not None for a in required))
                    and not inbox.invalid.is_set() and not inbox.resync.is_set()
                    and not inbox.delivery_gap and inbox.pending_wait_seconds() <= self.backup_queue_limit(candidate)
                    and not required.intersection(candidate.get('subscribe_pending', []))
                    and not required.intersection(candidate.get('recover_assets', []))
                    and self.backup_caught_up(candidate.get('primary_index'), candidate)
                    and self.backup_pong_fresh(candidate))

    def readiness_blockers(self, state):
        """Explain publication readiness separately from adding subscriptions."""
        if self.states.get(state.get('primary_index')) is state:
            return ['not_backup']
        inbox = state.get('inbox')
        if inbox is None:
            return ['disconnected']
        primary = self.states.get(state.get('primary_index'))
        required = set(primary['assets'] if primary else state.get('required_assets', state.get('assets', [])))
        if self.liveness_backup and state.get('redundant'):
            seen = {str(asset) for asset in (state.get('liveness_assets') or ())}
            return [name for name, blocked in (
                ('disconnected', not state.get('connected')),
                ('invalid', inbox.invalid.is_set()), ('resync', inbox.resync.is_set()),
                ('delivery_gap', inbox.delivery_gap),
                ('data_suspect', bool(state.get('data_suspect'))),
                ('current_subscription', bool(state.get('subscribe_initial')
                    or required.intersection(state.get('subscribe_pending') or []))),
                ('missing_envelope', not required <= seen),
                ('stale_envelope', not self._liveness_link_fresh(state)),
                ('stale_pong', not self.backup_pong_fresh(state)),
) if blocked]
        shadow = state.get('shadow')
        if shadow is None:
            return ['not_backup'] if not state.get('redundant') else ['missing_snapshot']
        return [name for name, blocked in (
            ('disconnected', not state.get('connected')),
            ('stale_pong', not self.backup_pong_fresh(state)),
            ('invalid', inbox.invalid.is_set()), ('resync', inbox.resync.is_set()),
            ('delivery_gap', inbox.delivery_gap), ('queue_wait', inbox.pending_wait_seconds() > self.backup_queue_limit(state)),
            ('current_subscription', bool(required.intersection(state.get('subscribe_pending', [])))),
            ('snapshot_repair', bool(required.intersection(state.get('recover_assets', [])))),
            ('missing_snapshot', not required <= shadow.books.keys()),
            ('missing_timestamp', any(shadow.recorder.metadata.get(a, (None,))[0] is None for a in required)),
            ('catchup', not state.get('ready') or not self.backup_caught_up(state['primary_index'], state))) if blocked]

    def note_physical_close(self, index, state, code):
        if state.get('remote_failure_at') is not None:
            return
        state['remote_failure_at'] = time.monotonic()
        state['remote_failure_wall'] = datetime.now(timezone.utc).timestamp()
        state['role_at_failure'] = 'backup' if state.get('standby') else 'primary'
        self.remote_closes.append(dict(at=state['remote_failure_wall'], source=state.get('source'), code=code,
            groups=sorted({self.balancer.groups.get(a, '') for a in state['assets']} - {''})))
        with self.lifecycle_lock:
            groups = {self.balancer.groups.get(a, '').split(':outcome:')[0] for a in state['assets']} & HOT_SERIES
            for group in groups:
                paired = self.double_kicks.record(group, state.get('source'),
                    state['role_at_failure'], code, state['remote_failure_at'], state['remote_failure_wall'])
                if paired is not None:
                    paired['_assets'] = set(state['assets'])
                    self.continuity.watch_gap(paired, set(state['assets']))
                    paired['supply_interrupted'] = not any(i == index and c is not state
                        and c.get('connected') and c.get('remote_failure_at') is None
                        and not c.get('inbox').invalid.is_set()
                        and (c is self.states.get(index) or self.backup_ready(c))
                        for i, c in [(index, self.states.get(index, {})), *self.backup_items()])

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
            ready = sum(self.backup_ready(c) for i, c in self.backup_items() if i == index)
            use_rest = not ready and not (state.get('failure_reason') == 'pong_timeout' and any(
                i == index and self.promotable_backup(c) and self.backup_pong_fresh(c) for i, c in self.backup_items()))
            groups = {self.balancer.groups.get(a, '').split(':outcome:')[0] for a in state['assets']} & HOT_SERIES
            gap_events = {key: dict(code=code, ready_backup_at_kick=bool(ready), ready_backups=ready,
                         takeover_gap_ms=None, receipt_gap_ms=None, recovery=None, gap_final=False) for key in groups}
            for event in gap_events.values():
                self.continuity.watch_gap(event, set(state['assets']))
            if use_rest:
                self.request_snapshot(index, state, set(state['assets']))
            with (_PIPELINE.lock if _PIPELINE else RLock()):
                receipts = [v[1] for a, v in (_PIPELINE.recording.metadata.items() if _PIPELINE else [])
                            if a in state['assets']]
            groups = {self.balancer.groups.get(a, '').split(':outcome:')[0] for a in state['assets']} & HOT_SERIES
            ready = sum(self.backup_ready(c) for i, c in self.backup_items() if i == index)
            for key in groups:
                row = self.primary_failures.setdefault(key, dict(total=0, without_ready_backup=0, events=deque(maxlen=64)))
                row['total'] += 1
                row['without_ready_backup'] += not ready
                event = gap_events[key]
                source_receipts = [self.continuity.latest.get(a, {}).get(state.get('source'), (None, None, None))[2]
                                   for a in state['assets']]
                source_receipts = [at for at in source_receipts if at is not None]
                event['_last_receipt'] = min(source_receipts or receipts, default=state['outage_wall'])
                event['source_silence_ms'] = max(0, (state['outage_wall'] - event['_last_receipt']).total_seconds() * 1000)
                event['physical_source_silence_alert'] = event['source_silence_ms'] > 20000
                event['physical_source_silence_alert_only'] = True
                event['merged_receipt_gap_ms'] = max(0, (state['outage_wall'] - min(receipts, default=state['outage_wall'])).total_seconds() * 1000)
                event['reason'] = state.get('failure_reason', 'transport')
                row['events'].append(event)
                _LOG.warning('Polymarket WS hot primary failure group=%s shard=%s code=%s ready_backups=%s',
                             key, index, code, ready)
                state.setdefault('outage_events', []).append(event)
                state['outage_events'] = state['outage_events'][-64:]
            # Public REST runs independently of WS handshake/supervisor cadence.
            if use_rest:
                if _PIPELINE:
                    with _PIPELINE.lock:
                        self.merges.setdefault(index, FeedMerge(_PIPELINE.recording.metadata))
                        for asset in state['assets']:
                            if self.balancer.assignment.get(asset) == index:
                                _PIPELINE.books.pop(asset, None)
                                _PIPELINE.recording.metadata.pop(asset, None)
                                if _PIPELINE.paper_account:
                                    _PIPELINE.paper_account.history.forget(asset)

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
                event.update(takeover_gap_ms=receipt_gap, detection_to_first_valid_publication_ms=gap,
                             first_valid_publication_at=datetime.now(timezone.utc).isoformat(),
                             detection_to_takeover_ms=event.get('detection_to_takeover_ms', gap),
                             recovery=event.get('recovery', method) or method,
                             receipt_gap_ms=receipt_gap, gap_final=True)
        state.pop('outage_at', None)

    def update_merged_gap(self, state):
        # Finalized only by FeedContinuity.publication; control ticks must not
        # move event anchors or inflate a completed gap with later traffic.
        return None

    @staticmethod
    def promotable_backup(state):
        inbox = state.get('inbox')
        return bool(state.get('connected') and not state.get('retired') and inbox
                    and state.get('initial_sent_at') is not None and not state.get('subscribe_initial')
                    and all(state.get(key) and state[key].is_alive() for key in ('thread', 'worker'))
                    and not inbox.invalid.is_set() and not inbox.resync.is_set()
                    and not inbox.delivery_gap)

    def backup_protects(self, index, primary):
        return any(i == index and (self.backup_ready(c) or primary.get('pong_dead') and self.promotable_backup(c) and self.backup_pong_fresh(c))
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
            state.update(connected=False, failure_reason='pong_timeout', failed_at=now,
                         retry_at=now + self.retry_delay(index, state, state.get('attempts', 1)))
            if state.get('redundant'):
                inbox.invalid.set()
                self.remove_backup(index, state)
                self.retire(state)
                continue
            self.record_primary_failure(index, state, None)
            state['pong_dead'] = True
            if not any(i == index and self.promotable_backup(c) and self.backup_pong_fresh(c) for i, c in self.backup_items()):
                inbox.invalid.set()
            wake_reader(state.get('socket'))
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
                          + 1024 * len(self.continuity.cadence_seconds))
        queues = processing = 0
        for state in states:
            inbox = state.get('inbox')
            if inbox:
                with inbox.queue.mutex:
                    queues += inbox.queue.charged_bytes
                processing += inbox.processing_charged_bytes
        checkpoints = {id(c): c for state in states if (c := state.get('book_checkpoints')) is not None}
        checkpoints.update({id(shadow.checkpoints): shadow.checkpoints for shadow in shadows.values()
                            if isinstance(shadow, WarmBook)})
        checkpoint_bytes = sum(c.charged_bytes() for c in checkpoints.values())
        frames = self.parsed_frames.metrics()['charged_bytes']
        self.aux_memory = dict(book_checkpoint_charged_bytes=checkpoint_bytes, shadow_charged_bytes=shadow_bytes, dedup_charged_bytes=dedup,
            continuity_charged_bytes=continuity, parsed_frames_charged_bytes=frames,
            queue_charged_bytes=queues, processing_frame_charged_bytes=processing,
            auxiliary_charged_bytes=checkpoint_bytes + shadow_bytes + dedup + continuity + frames + queues + processing,
            accounting='conservative_retained_cache_estimates_not_rss')
        return self.aux_memory

    def ended_assets(self, assets, now=None):
        """Assets whose market is past its end time.

        /books keeps serving a cached book for a market that stopped trading,
        seconds old (1.6-4.4s seen on 2026-10-20), so a repair fails the stale
        check and retries. The WS push or retirement handles these assets.
        An asset of no known market is not ended.
        """
        markets = self.markets
        if getattr(self, '_end_by_asset_for', None) is not markets:
            ends = {}
            for market in markets or []:
                end = market.get('endDate')
                if not end:
                    continue
                try:
                    at = updown.timestamp(end)
                except (TypeError, ValueError):
                    continue
                for asset in (market.get('yesAssetId'), market.get('noAssetId')):
                    if asset is not None:
                        ends[str(asset)] = at
            self._end_by_asset, self._end_by_asset_for = ends, markets
        now = now or datetime.now(timezone.utc)
        ends = self._end_by_asset
        return {a for a in assets if (at := ends.get(str(a))) is not None and at <= now}

    def _rest_watermarks(self, index, state, assets):
        """asset -> (latest WS exchange time, WS book hash when that book is the latest change)."""
        merge = self.merges.get(index)
        latest = merge.latest if merge is not None else {}
        marks = state.get('book_marks') or {}
        rows = {}
        for asset in assets:
            key = str(asset)
            at = latest.get(key, latest.get(asset))
            mark = marks.get(key) or marks.get(asset)
            mark_at = mark[0] if mark else None
            if mark_at is not None and (at is None or mark_at >= at):
                rows[key] = (mark_at, mark[1] if len(mark) > 1 else None)
            elif at is not None:
                rows[key] = (at, None)
        return rows

    def request_snapshot(self, index, state, assets, pressure=False):
        ended = self.ended_assets(assets)
        if ended:
            self.snapshot_recovery.skipped_ended('pressure' if pressure else 'recovery', len(ended))
            assets = [a for a in assets if a not in ended]
            if not assets:
                if (state.get('need_book') and self.states.get(index) is state
                        and not state.get('retired')):
                    self._resubscribe_books(index, state)
                return False
        generation = state.get('inbox')
        epoch = generation.epoch if generation else None
        def publish(events, started):
            with self.lifecycle_lock:
                if (self.closed or state.get('retired') or state.get('inbox') is not generation
                        or generation and (generation.epoch != epoch or generation.resync.is_set())):
                    return
                if state.get('redundant'):
                    if self.liveness_backup:
                        return
                    if not any(i == index and c is state for i, c in self.backup_items()):
                        return
                    inbox = state['inbox']
                    if inbox.invalid.is_set() or inbox.resync.is_set():
                        return
                    self.redundant_receive(index, state, events, started, inbox)
                elif self.states.get(index) is state and _PIPELINE:
                    # lifecycle → pipeline → state. The reader releases the state
                    # lock before taking the pipeline lock, so a newer price_change
                    # cannot land between the REST write and the hold release.
                    with _PIPELINE.lock:
                        owned = {a for a in assets if self.balancer.assignment.get(a) == index}
                        # Seed from live watermarks: late REST may never roll back WS.
                        merge = self.merges.setdefault(index, FeedMerge(_PIPELINE.recording.metadata))
                        merged = merge.filter(events, 'rest', owned, backup=True)
                        if merged:
                            self.continuity.receive(merged, 'rest', datetime.now(timezone.utc))
                            _PIPELINE.receive(merged, owned, datetime.now(timezone.utc))
                            self.continuity.publication(_PIPELINE.recording.metadata, owned)
                        with state['lock']:
                            extra = self._accept_rest_books(state, merged) if state.get('need_book') else []
                        if extra:
                            source = state.get('source') or 'rest'
                            kept = merge.filter(extra, source, owned)
                            if kept:
                                self.continuity.receive(kept, source, datetime.now(timezone.utc))
                                _PIPELINE.receive(kept, owned, datetime.now(timezone.utc))
                                self.continuity.publication(_PIPELINE.recording.metadata, owned)
                        self.complete_recovery(index, state, 'rest_snapshot')
                    if state.get('need_book'):
                        # The book was filtered. Stay held and ask for a push now;
                        # the timer covers a push that never arrives.
                        self._resubscribe_books(index, state)
                        self._arm_book_fallback(index, state)
        state['_rest_watermarks'] = self._rest_watermarks(index, state, assets)
        state['_rest_hot'] = bool(self.hot_shard(index))
        if pressure:
            accepted = self.snapshot_recovery.request(state, assets, publish, pressure=True)
        else:
            accepted = self.snapshot_recovery.request(state, assets, publish)
        if (accepted is False and state.get('need_book') and self.states.get(index) is state
                and not state.get('retired')):
            self._resubscribe_books(index, state)
        return accepted

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
            return assets  # Stable per-window safety latch avoids subscription churn.
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
            self.remove_backup(index, candidate)
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
        if self.liveness_backup:
            candidate['liveness_assets'] = set(candidate.get('liveness_assets') or ()) & set(assets)
        else:
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
        self.continuity.hot_assets = {a for a, key in self.balancer.groups.items()
                                      if key.split(':outcome:')[0] in HOT_SERIES}
        with self.continuity.lock:
            self.continuity.rollover.observe(markets, self.continuity.published)
            self.continuity.prune(set(subscription_assets(markets)))
        if set(subscription_assets(markets)) == set(self.balancer.assignment):
            self.tick()
            return
        # Rollover keeps each series on its shard: only changed assets move.
        owners = self.capacity_owners()
        assignment = self.assignment_for(owners)
        self.apply_assignment(assignment)
        self.tick()

    def update_double_kicks(self):
        # The exact same publication callback closes primary and paired events.
        return None

    def sample_fast(self, now, deadline):
        # No Pipeline/lifecycle lock, JSON parsing, TCP_INFO or DB work here.
        states = list(self.states.items()) + self.backup_items()
        for _, state in states:
            sock, inbox = state.get('socket'), state.get('inbox')
            transport = getattr(sock, 'transport', None)
            if inbox and state.get('connected') and isinstance(transport, TransportDiagnostics):
                transport.sample_fast(socket_pending_bytes(sock), inbox.queue.qsize(),
                    inbox.pending_wait_seconds(), now-deadline, now)

    def start_supervisor(self):
        """Keep recovery/rotation independent of discovery, retention and status SQL."""
        if self.supervisor is not None:
            return
        def supervise():
            while not self.supervisor_stop.wait(.2) and not _STOP.is_set():
                try:
                    self.tick(status=False)
                except Exception:
                    _LOG.exception('Polymarket WS supervisor failed')
        def sample():
            deadline = time.monotonic() + .1
            while not self.supervisor_stop.wait(max(0, deadline-time.monotonic())) and not _STOP.is_set():
                now = time.monotonic()
                self.sample_fast(now, deadline)
                deadline = time.monotonic() + .1
        self.sampler = threading.Thread(target=sample, name='polymarket-100ms-sampler', daemon=True)
        self.sampler.start()
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
                                    wake_reader(state.get('socket'))
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
        wake_reader(state.get('socket'))
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

    def retry_delay(self, index, state, attempts):
        groups = {self.balancer.groups.get(a, '').split(':outcome:')[0] for a in state.get('assets', [])}
        hot = sorted(groups & HOT_SERIES)
        lane = sorted(HOT_SERIES).index(hot[0]) if hot else index % 8
        role = 'backup' if state.get('redundant') else 'primary' if hot else 'cold'
        return reconnect_delay(attempts, role, lane)

    def rotation_failed(self, index, active, candidate, reason):
        prefix = 'backup' if candidate.get('redundant') else 'rotate'
        failures = active.get(f'{prefix}_failures', 0) + 1 if active else 1
        delay = self.retry_delay(index, candidate, failures) if candidate.get('redundant') else min(480, 60 * 2 ** min(3, failures - 1))
        if active:
            active.update({f'{prefix}_failures': failures, f'{prefix}_retry_at': time.monotonic() + delay})
        established = candidate.get('redundant') and candidate.get('ever_ready')
        if established:
            self.backup_wire_failures += 1
        else:
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
        # Market-channel IP limit is unpublished. Bound the explicit budget.
        return min(5, int(ws_setting('HOT_BACKUP_COUNT', 2, 1)))

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

    @staticmethod
    def connection_role(state):
        return dict(role='backup' if state.get('standby') else 'primary',
                    physical_role=state.get('physical_role'), source=state.get('source'),
                    promoted_at=state.get('promoted_at'))

    def _hot_groups_for(self, assets):
        found = set()
        for asset in assets or []:
            group = self.balancer.groups.get(asset, '').split(':outcome:')[0]
            if group in HOT_SERIES:
                found.add(group)
        return found

    def _spare_probe(self, index, state, now):
        """One spare, as this process sees it. Does not change ready or coverage."""
        inbox = state.get('inbox')
        if state.get('primary_index') in self.states:
            required = {str(asset) for asset in (self.states[state['primary_index']].get('assets') or [])}
        elif index in self.states:
            required = {str(asset) for asset in (self.states[index].get('assets') or [])}
        else:
            required = {str(asset) for asset in (state.get('required_assets') or state.get('assets') or [])}
        if self.liveness_backup and state.get('redundant'):
            seen = {str(asset) for asset in (state.get('liveness_assets') or ())}
        else:
            shadow = state.get('shadow')
            seen = {str(asset) for asset in (getattr(shadow, 'books', {}) or {})}
        try:
            ready = bool(self.backup_ready(state))
            blockers = list(self.readiness_blockers(state))
        except Exception:
            ready = False
            blockers = ['probe_error']
        last = state.get('liveness_at')
        pong_age = self._liveness_pong_age(state, now)
        missing = sorted(required - seen)
        return dict(
            shard=index, source=state.get('source'), ready=ready,
            connected=bool(state.get('connected')),
            received_frames=int(getattr(inbox, 'received_frames', 0) or 0) if inbox is not None else 0,
            blockers=blockers, required_assets=len(required), seen_assets=len(required & seen),
            missing_assets=missing[:8],
            envelope_age_s=None if last is None else round(max(0.0, now - last), 3),
            pong_age_s=None if pong_age is None else round(pong_age, 3),
            envelope_fresh=bool(self._liveness_envelope_fresh(state, now)),
            link_fresh=bool(self._liveness_link_fresh(state, now)),
            subscribe_initial=bool(state.get('subscribe_initial')),
            data_suspect=bool(state.get('data_suspect')),
            queue_wait_seconds=round(inbox.pending_wait_seconds(), 3) if inbox is not None else None)

    def child_ready_probe(self, now=None):
        """Per hot group, this process's spares. The parent copies it; it does not sum it.

        `ws_hot_backup_by_group.ready_backup_count` is a sum across children, and
        every child publishes 0 for the groups it does not own. This probe is the
        spare's own ready flag, blockers, envelope age and PONG age.
        """
        now = time.monotonic() if now is None else now
        groups = {}

        def row(group):
            return groups.setdefault(group, dict(
                group=group, feed=group, liveness_backup=bool(self.liveness_backup),
                ready_count=0, spare_count=0, spares=[]))

        for asset in self.balancer.assignment:
            for group in self._hot_groups_for([asset]):
                row(group)
        for index, state in self.backup_items():
            if 'inbox' not in state:
                continue
            owned = self._hot_groups_for(state.get('assets'))
            if not owned and index in self.states:
                owned = self._hot_groups_for(self.states[index].get('assets'))
            if not owned:
                continue
            spare = self._spare_probe(index, state, now)
            for group in owned:
                item = row(group)
                item['spares'].append(spare)
                item['spare_count'] += 1
                item['ready_count'] += int(bool(spare['ready']))
        return groups

    def backup_metrics(self, index, state, now):
        inbox = state['inbox']
        required = set(self.states[index]['assets'])
        pending = set(state.get('subscribe_pending', []))
        return dict(**self.connection_role(state), ready=self.backup_ready(state), connected=state['connected'], stage=self.prepare_stage(state),
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
                self.remove_backup(index, candidate)
                if not candidate.get('normal_shutdown'):
                    self.rotation_failed(index, old, candidate, 'backup_wire_failure')
                elif old:
                    old['backup_retry_at'] = now + self.retry_delay(index, candidate, 1)
                self.retire(candidate)
            elif candidate.get('initial_sent_at') is not None and not candidate.get('admission_observed'):
                self.backup_next_connect_at = max(self.backup_next_connect_at,
                    candidate['initial_sent_at'] + candidate['admission_seconds'])
                candidate['admission_observed'] = True
        self.update_hot_coverage(now)
        dialing = any(s.get('initial_sent_at') is None for _, s in self.backup_items())
        # Uncovered groups go first. Extra redundancy is staggered, never at
        # the expense of the only backup or by adding load to another group.
        for index, old in sorted(self.states.items(), key=lambda item: (sum(i == item[0] for i, _ in self.backup_items()), item[0])):
            if not self.hot_shard(index):
                continue
            self.primary_pressure(index, now)
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
                    # A missing full book is not a liveness failure. Queueing
                    # every asset for REST repair cleared ready on every tick.
                    if not (self.liveness_backup and candidate.get('redundant')):
                        candidate.setdefault('recover_assets', set()).update(candidate['assets'])
            first = self.backups.get(index)
            copies = [c for i, c in self.backup_items() if i == index]
            if (dialing or len(copies) >= self.backup_target()
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
            if copies and now < max(c.get('initial_sent_at', now) for c in copies) + 10:
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
                busy=True, source=f'backup-{self.backup_serial}',
                shadow=WarmBook(owned_assets) if self.liveness_backup else ShadowBooks(owned_assets),
                created_at=now)
            if first:
                self.reserves[index if index not in self.reserves else (index, self.backup_serial)] = candidate
            else:
                self.backups[index] = candidate
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
        blockers = {}
        for key, indices in zero_required.items():
            reasons = set()
            for index in indices - ready:
                candidates = [c for i, c in self.backup_items() if i == index]
                if not candidates:
                    reasons.add('no_backup')
                for candidate in candidates:
                    reasons.update(self.readiness_blockers(candidate))
                    if not self.backup_ready(candidate) and self.subscriptions.pressured(candidate):
                        reasons.add('ingress_pressure')
                if candidates and not reasons:
                    reasons.add('primary_unhealthy')
            blockers[key] = reasons
        rows = self.hot_backup_coverage.update(required, ready, now, backup_counts=counts,
                                              zero_required=zero_required, blockers=blockers)
        for key, row in rows.items():
            row['ready_backup_count'] = min((sum(i == index and self.backup_ready(c)
                for i, c in self.backup_items()) for index in required.get(key, set())), default=0)
            row['target_backup_count'] = self.backup_target()
        return rows

    def redundant_receive(self, index, state, event, received_at, inbox, epoch=None):
        if self.liveness_backup and state.get('redundant'):
            return
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
                repaired = set()
                for kind, payload in recording.events(event):
                    if kind == 'book':
                        asset = str(payload.get('asset_id') or payload.get('assetId') or '')
                        metadata = shadow.recorder.metadata.get(asset)
                        if (asset in shadow.books and metadata and metadata[1] == received_at
                                and shadow.merge.snapshots.get(asset) == recording.exchange_time(payload.get('timestamp'))):
                            repaired.add(asset)
                if not inbox.delivery_gap:
                    state.setdefault('recover_assets', set()).difference_update(repaired)
                    state.setdefault('_rest_missing_assets', set()).difference_update(repaired)
                required = set(self.states[index]['assets'])
                live_event = recording.owned_events(event, required)
                self.continuity.receive(live_event, state['source'], received_at)
                if _PIPELINE:
                    merged = self.merges[index].filter(live_event, state['source'], required, backup=True)
                    if merged:
                        before = {a: self.continuity.published.get(a, {}).get('received') for a in required}
                        _PIPELINE.receive(merged, required, received_at, inbox, inbox.epoch)
                        self.continuity.publication(_PIPELINE.recording.metadata, required)
                        if any((after := self.continuity.published.get(a, {}).get('received')) is not None
                                and (before[a] is None or after > before[a])
                                and (datetime.now(timezone.utc)-after).total_seconds() <= 1 for a in required):
                            state['merged_supply_at'] = time.monotonic()
                        self.update_merged_gap(self.states[index])
                old = self.states[index]
                # No simultaneous all-asset timestamp equality barrier on busy
                # feeds. Each asset has its own full snapshot; the merge guards
                # stale snapshots/levels independently, while retaining trades.
                ready = (state['connected'] and bool(required) and required <= shadow.books.keys()
                         and all(shadow.recorder.metadata.get(a, (None,))[0] is not None for a in required)
                         and not required.intersection(state.get('subscribe_pending', []))
                         and not required.intersection(state.get('recover_assets', []))
                         and inbox.pending_wait_seconds() <= self.backup_queue_limit(state) and not inbox.delivery_gap
                         and self.backup_caught_up(index, state)
                         and (datetime.now(timezone.utc) - received_at).total_seconds() <= self.backup_queue_limit(state))
                if _PIPELINE:
                    # After live invalidation, do not restore an older shadow
                    # snapshot over the retained merge watermark.
                    ready = ready and all(
                        self.merges[index].latest.get(a) is None
                        or shadow.recorder.metadata.get(a, (None,))[0] is not None
                        and shadow.recorder.metadata[a][0] >= self.merges[index].latest[a]
                        for a in required - _PIPELINE.books.keys())
                force = bool(old.get('pong_dead') and self.promotable_backup(state) and self.backup_pong_fresh(state))
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
                    state['ever_ready'] = True
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
                    old.update(data_suspect=True, failure_reason=self.silence_reason(old))
                    old.setdefault('failed_at', now)
                    self.record_primary_failure(index, old, None)
                maximum = ws_setting('HOT_ROTATE_SECONDS', 0, 0)
                due = maximum > 0 and time.monotonic() - old.get('opened_at', time.monotonic()) >= maximum
                if old['connected'] and not old['inbox'].invalid.is_set() and not old.get('data_suspect') and not due:
                    return
                # REST/supervisor may refresh books/readiness, but only this
                # socket's worker can promote: it may already hold a popped delta.
                if threading.current_thread() is not state.get('worker') or self.best_backup(index) is not state:
                    return
                extra = set(state['assets']) - required - set(state.get('subscribe_pending', []))
                state['assets'] = list(old['assets'])
                state['subscribe_pending'] = [a for a in state.get('subscribe_pending', []) if a in required]
                state['recover_assets'] = set(state.get('recover_assets', [])) & required
                self.subscriptions.forget(state)
                self.check_continuity(index, old, state, 'failover' if not old['connected'] else 'data_collapse' if old.get('data_suspect') else 'hot_age')
                self.failover_ms = max(0, (time.monotonic() - old.get('failed_at', time.monotonic())) * 1000)
                self.transfer_outage(index, old, state)
                if 'outage_at' in state:
                    for event in state['outage_events']:
                        last = event['_last_receipt']
                        receipt_gap = max(0, (datetime.now(timezone.utc) - last).total_seconds() * 1000)
                        event.update(takeover_gap_ms=receipt_gap, detection_to_takeover_ms=self.failover_ms,
                                     recovery='backup', receipt_gap_ms=receipt_gap)
                    self.complete_recovery(index, state, 'backup')
                state.update(standby=False, redundant=False, promoted_at=datetime.now(timezone.utc).isoformat(), rotate_failures=0, rotate_retry_at=0,
                             backup_started=True, backup_failures=0, backup_retry_at=0, last_pressure_promotion=time.monotonic())
                state['unsubscribe_pending'] = extra
                state.pop('shadow')
                self.states[index] = state
                self.remove_backup(index, state)

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
                state['promoted_at'] = datetime.now(timezone.utc).isoformat()
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
        state['physical_role'] = 'backup' if state.get('standby') else 'primary'
        self.snapshot_recovery.prepare(state, state['assets'], parallel=self.hot_shard(index))
        state.setdefault('rest_warm_at', time.monotonic() + 15 + index*.5)
        if state['socket']:
            self.continuity.switch(state.get('source'), None, state['assets'], 'reconnect')
            if state.get('inbox'):
                state['inbox'].invalid.set()
            wake_reader(state.get('socket'))
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
        state.pop('book_checkpoints', None)
        state.pop('remote_failure_at', None)
        state.pop('remote_failure_wall', None)
        state.pop('role_at_failure', None)
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
        state['retry_at'] = time.monotonic() + self.retry_delay(index, state, state['attempts'])
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
                if self._primary_blocks_backup(index, state):
                    time.sleep(0.001)
                    continue
                if self._queue_lag_breaks_backup(state, inbox):
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
                            if self.liveness_backup:
                                state['shadow'] = WarmBook(state['assets'])
                                state['liveness_assets'] = set()
                                state['liveness_hash'] = {}
                            else:
                                state['shadow'] = ShadowBooks(state['assets'])
                            state['ready'] = False
                            inbox.resync.clear()
                            inbox.delivery_gap = False
                            inbox.resyncs += 1
                            state.setdefault('recover_assets', set()).update(state['assets'])
                            state.setdefault('_rest_missing_assets', set()).update(state['assets'])
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
                    raw, received_at, epoch = inbox.next_frame(timeout=.2)
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
                        if self.liveness_backup and state.get('redundant'):
                            self._promote_liveness_backup(index, state)
                        else:
                            self.shadow_receive(index, state, [], datetime.now(timezone.utc), inbox)
                    continue
                try:
                    parsing_at = time.monotonic()
                    standby = bool(state.get('standby'))
                    if self.liveness_backup and state.get('redundant'):
                        self.liveness_receive(index, state, raw, received_at, inbox, epoch)
                        inbox.processing_received_at = None
                        inbox.processing_charged_bytes = 0
                        continue
                    parse_cpu = thread_time()
                    event = self.parsed_frames.parse(raw)
                    from .ws_payloads import feed_envelope
                    self._feed_freshness(state, feed_envelope(event), received_at)
                    self._charge('backup_parse' if standby else 'primary_parse', parse_cpu)
                    inbox.parse_max_seconds = max(inbox.parse_max_seconds, time.monotonic() - parsing_at)
                    apply_cpu = thread_time()
                    if standby:
                        self.shadow_receive(index, state, event, received_at, inbox, epoch)
                        self._charge('backup_apply', apply_cpu)
                        continue
                    if state.get('need_book') or state.get('held_changes'):
                        with state['lock']:
                            event = self._release_resync(state, event)
                    else:
                        self._remember_books(state, event, checkpoint=False)
                    if not event:
                        self._charge('primary_apply', apply_cpu)
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
                                    published_assets = {a for a in owned
                                        if _PIPELINE.recording.metadata.get(a, (None, None))[1] == received_at}
                                    self._remember_books(state, recording.owned_events(merged, published_assets))
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
                    self._charge('primary_apply', apply_cpu)
                except (ValueError, TypeError, AttributeError):
                    if state.get('redundant') and isinstance(state.get('shadow'), WarmBook):
                        inbox.delivery_gap = True
                        inbox.resync.set()
                        state['ready'] = False
                    continue
                finally:
                    inbox.queue.task_done()
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
                role=state.get('role_at_failure', 'backup' if state.get('standby') else 'primary'),
                physical_role=state.get('physical_role'), source=state.get('source'),
                promoted_at=state.get('promoted_at'),
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
                state['last_transport_failure']['local_100ms'] = transport.fast_metrics(True) if isinstance(transport, TransportDiagnostics) else {}
                state['last_transport_failure']['peer_local_100ms'] = [
                    dict(source=peer.get('source'), **t.fast_metrics(True))
                    for i, peer in [(index, self.states.get(index, {})), *self.backup_items()]
                    if i == index and peer is not state
                    and isinstance(t := getattr(peer.get('socket'), 'transport', None), TransportDiagnostics)]
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
            self.note_physical_close(index, state, remote_close(error)[0])
            self.record_primary_failure(index, state, remote_close(error)[0])
            inbox.delivery_gap = True
            state['connected'] = False
            state.setdefault('failed_at', time.monotonic())
            self.wake_backups(index, state)
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
            self.note_physical_close(index, state, code)
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
            self.wake_backups(index, state)
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
            state['retry_at'] = time.monotonic() + self.retry_delay(index, state, state['attempts'])

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

    # Status is observation, not control. The supervisor runs recovery every
    # 0.2s; rebuilding the full STATUS that often held lifecycle_lock for the
    # whole build (r17 root cause: ~6 builds/s). Explicit callers (market loop,
    # tests) still rebuild every call; the supervisor only when it is stale.
    STATUS_REFRESH_SECONDS = 1.0

    def tick(self, status=True):
        with self.lifecycle_lock:
            if self._tick() is not False and (
                    status or time.monotonic() - self.status_at >= self.STATUS_REFRESH_SECONDS):
                self.publish_status()

    def background_snapshot_tick(self, now):
        # Outage dispatch always precedes optional background work.
        if any('outage_at' in s for s in self.states.values()):
            return
        for index, state in self.states.items():
            inbox = state.get('inbox')
            if not state.get('connected') or not inbox:
                continue
            pending = inbox.pending_wait_seconds()
            if pending < .35:
                state.pop('rest_pressure_episode', None)
            btc = any(self.balancer.groups.get(a, '').startswith('up_down:BTC:') for a in state['assets'])
            # Backlog alone is not a business outage: either physical feed may
            # still be advancing the merged books. Measure actual publication.
            receipts = [self.continuity.published.get(a, {}).get('received') for a in state['assets']]
            stale = bool(receipts) and any(at is None or
                (datetime.now(timezone.utc)-at).total_seconds() >= .7 for at in receipts)
            if (btc and pending >= .35 and stale and not state.get('rest_pressure_episode')
                    and now >= state.get('rest_pressure_at', 0)
                    and not any(i == index and self.backup_ready(c) for i, c in self.backup_items())):
                state['rest_pressure_at'] = now + 1  # Includes rejected attempts.
                self.snapshot_recovery.prepare(state, state['assets'], parallel=True)
                if self.request_snapshot(index, state, set(state['assets']), pressure=True):
                    state['rest_pressure_episode'] = True
                    state['rest_pressure_at'] = now + 30
        # Default off: round15 did not require polling to keep its HTTP pool
        # warm. Optional warmup only runs after pool idleness.
        interval = ws_setting('REST_WARM_SECONDS', 0, 0)
        if interval <= 0:
            return
        for index, state in self.states.items():
            if (self.hot_shard(index) and state.get('connected') and
                    not self.subscriptions.pressured(state) and now >= state.get('rest_warm_at', 0)):
                state['rest_warm_at'] = now + max(60, interval)
                self.snapshot_recovery.prepare(state, state['assets'], parallel=True)
                if self.snapshot_recovery.request(state, state['assets'], None, warm=True):
                    break

    def _tick(self):
        """Control only: recovery, rotation, rates. Returns False when skipped."""
        if self.closed or _STOP.is_set():
            return False
        live = self.active_catalogue(self.markets)
        if {m['id'] for m in live} != {m['id'] for m in self.markets}:
            if _PIPELINE:
                _PIPELINE.set_markets(live)
            self._sync(live)
            return False
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
                    if state.get('redundant') and self.liveness_backup:
                        # Do not wipe envelope health or force ready false while
                        # waiting for a sparse WS book. Promotion resyncs by REST.
                        state['snapshot_retry_at'] = now + 10
                        state.pop('recover_assets', None)
                        state.pop('_rest_missing_assets', None)
                    else:
                        if state.get('redundant'):
                            missing = set(state['assets']) - self.redundant_seen(state) - set(state.get('subscribe_pending', []))
                        else:
                            missing = set(state['assets']) & self.continuity.pending_assets()
                        state.setdefault('recover_assets', set()).update(missing)
                        if state.get('redundant'):
                            state.setdefault('_rest_missing_assets', set()).update(missing)
                        state['snapshot_retry_at'] = now + (.5 if state.get('redundant') else 10)
                recover = state.pop('recover_assets', set()) & set(state['assets'])
                if self.liveness_backup and state.get('redundant'):
                    # Queued snapshot repair is the shadow-book path. Applying
                    # it here cleared ready on every 0.5s pass in production.
                    recover = set()
                if recover and state['connected'] and not state.get('subscribe_initial'):
                    if state.get('redundant'):
                        # Repair full snapshots without tearing down the live stream.
                        # Recheck after dequeueing the repair intent: WS may
                        # already have delivered the missing full snapshot.
                        # Never erase a usable ladder to start an HTTP repair.
                        automatic = state.pop('_rest_missing_assets', set()) & recover
                        if not state['inbox'].delivery_gap:
                            recover -= automatic & self.redundant_seen(state)
                        if recover:
                            # Explicit gap/continuity repairs remain mandatory.
                            # Only obsolete automatic missing-book work cancels.
                            state['ready'] = False
                            if self.liveness_backup:
                                live = state.get('liveness_assets') or set()
                                hashes = state.get('liveness_hash') or {}
                                for asset in recover:
                                    live.discard(asset)
                                    hashes.pop(asset, None)
                            else:
                                for asset in recover:
                                    state['shadow'].books.pop(asset, None)
                                    state['shadow'].recorder.metadata.pop(asset, None)
                            self.update_hot_coverage(now)
                            if not self.request_snapshot(index, state, recover):
                                state.setdefault('recover_assets', set()).update(recover)
                                state.setdefault('_rest_missing_assets', set()).update(automatic & recover)
                    elif self.hot_shard(index):
                        if not self.request_snapshot(index, state, recover):
                            state.setdefault('recover_assets', set()).update(recover)
                    else:
                        state['socket'].send(json.dumps(dict(operation='unsubscribe', assets_ids=sorted(recover))))
                        self.subscriptions.enqueue(state, sorted(recover))
                self.subscriptions.drain(state, now)
            except Exception as exc:
                state['connected'] = False
                if state.get('inbox'):
                    state['inbox'].invalid.set()
                wake_reader(state.get('socket'))
                _ws_error(exc)
        self.redundancy_tick(now)
        if now - self.cache_maintenance_at >= 1:
            self.cache_maintenance_at = now
            with (_PIPELINE.lock if _PIPELINE else RLock()):
                budget = max(1, 8192 // max(1, len(self.merges)))
                for merge in self.merges.values():
                    merge.maximum = min(merge.maximum, budget)
                    merge.expire()
            self.continuity.expire(now)
        self.update_double_kicks()
        self.background_snapshot_tick(now)
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
                wake_reader(state.get('socket'))
            if not state['connected'] and state.get('failed_at') is not None:
                if now >= state['retry_at']:
                    if any(i == index and (self.backup_ready(c) or state.get('pong_dead') and self.promotable_backup(c) and self.backup_pong_fresh(c))
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
                    wake_reader(state.get('socket'))
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
        # Zero-spare integration keeps its 0.2s supervisor resolution.
        self.hot_coverage_rows = self.update_hot_coverage(time.monotonic())
        self.tick_gap_last = tick_gap

    def publish_status(self):
        """Rebuild STATUS from the current control state (caller holds lifecycle_lock)."""
        now = time.monotonic()
        self.status_at = now
        tick_gap = self.tick_gap_last
        cpu_now = process_time()
        cpu_cores = (cpu_now - self.cpu_seconds) / (now - self.cpu_at) if now > self.cpu_at else 0
        self.cpu_at, self.cpu_seconds = now, cpu_now
        hot_coverage = self.hot_coverage_rows
        aux_memory = self.memory_metrics(now)
        with _LOCK:
            STATUS.setdefault('rebalance_count', 0)
            STATUS.update(self.balancer.metrics())
            STATUS.update(ws_process_cpu_cores=cpu_cores,
                          ws_local_100ms={s.get('source'): t.fast_metrics()
                              for _, s in [*self.states.items(), *self.backup_items()]
                              if isinstance(t := getattr(s.get('socket'), 'transport', None), TransportDiagnostics)}, ws_process_rss_bytes=process_rss_bytes(),
                          ws_aux_memory=aux_memory,
                          ws_observer_database=database_metrics(),
                          ws_metrics_session_id=self.stats.session,
                          ws_metrics_scope='observer_process_session',
                          ws_retiring_connections=len(self.retiring), ws_parsed_frames=self.parsed_frames.metrics(),
                          ws_gil_switch_ms=sys.getswitchinterval() * 1000,
                          ws_tick_gap_seconds=tick_gap, ws_tick_gap_max_seconds=self.tick_gap_max)
            STATUS['ws_remote_closes'] = list(self.remote_closes)
            STATUS['ws_server_disconnects'] = self.disconnect_clusters.update(self.remote_closes)
            STATUS['ws_rollover_preparation'] = {i: dict(
                prewarm_pressure_blocked=s.get('prewarm_blocked_assets') == frozenset(s.get('assets', [])),
                spares=[dict(source=c.get('source'),
                    future_assets=sorted(set(c['assets']) - set(s['assets'])),
                    seen_future_assets=sorted(set(c.get('liveness_assets', [])) - set(s['assets'])),
                    current_subscription_pending=sorted(set(s['assets']) & set(c.get('subscribe_pending', []))))
                    for shard, c in self.backup_items() if shard == i]) for i, s in self.states.items() if self.hot_shard(i)}
            STATUS['ws_last_transport_failures'] = {i: s['last_transport_failure']
                for i, s in self.states.items() if 'last_transport_failure' in s}
            STATUS['ws_shard_assets'] = [len(self.states.get(i, {}).get('assets', [])) for i in range(self.count)]
            STATUS['ws_receive_queues'] = {
                i: dict(**self.connection_role(s), depth=s['inbox'].queue.qsize(), drops=s.get('receive_drops', 0) + s['inbox'].dropped,
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
            STATUS.update(ws_rollover_gaps=self.continuity.rollover.metrics(),
                          ws_stage_cpu_seconds=dict(self.stage_cpu),
                          ws_backup_book_cpu_seconds={s.get('source'): dict(s.get('stage_cpu') or {})
                              for _, s in self.backup_items()},
                          ws_warm_backup_supply={s.get('source'): dict(
                              publications=s.get('early_supply_publications', 0),
                              verified_assets=sorted(s.get('_supply_verified') or []))
                              for _, s in self.backup_items()},
                          ws_warm_backup_supply_primary_quiet_seconds=.25,
                          ws_continuity=self.continuity.metrics(), ws_failover_ms=self.failover_ms,
                          ws_hot_backup_by_group=hot_coverage,
                          ws_child_ready=self.child_ready_probe(now),
                          ws_double_kicks=self.double_kicks.metrics(datetime.now(timezone.utc).timestamp()),
                          ws_connections_by_source=[dict(shard=i, **self.connection_role(s))
                              for i, s in [*self.states.items(), *self.backup_items()]],
                          ws_backup_exchange_lag_grace_seconds=.75,
                          ws_backup_supply_queue_grace_seconds=1.5,
                          ws_hot_primary_failures={k: dict(total=v['total'], without_ready_backup=v['without_ready_backup'],
                              events=[{field: value for field, value in e.items() if not field.startswith('_')}
                                  for e in v['events']]) for k, v in self.primary_failures.items()},
                          ws_takeover_gap_scope='failed_source_receipt_to_recovery; detection_to_takeover_ms_is_control_time',
                          ws_acceptance_gap_scope='ws_hot_primary_failures.<group>.events[].merged_gap_ms with merged_gap_final=true <=1500; ws_double_kicks.business_gaps_current_hour <=1; physical_source_silence>20s and historical_trade_loss alert_only; cold delivery latency',
                          ws_pong_timeout_seconds=22,
                          ws_backup_max_lag_seconds=ws_setting('BACKUP_MAX_LAG_SECONDS', 2, 1),
                          ws_rest_recovery=self.snapshot_recovery.metrics(),
                          ws_reserves={(f'{key[0]}:{key[1]}' if isinstance(key, tuple) else str(key)): self.backup_metrics(key[0] if isinstance(key, tuple) else key, c, now)
                                       for key, c in self.reserves.items()},
                          ws_required_hot_groups=sorted(HOT_SERIES),
                          ws_split_byte_series=[],
                          ws_migration=self.migration_metrics(now),
                          ws_primary_1013_by_shard=self.primary_1013_metrics(now),
                          ws_rotation_count=self.rotations, ws_rotation_failures=self.rotation_failures,
                          ws_backup_wire_failures=self.backup_wire_failures,
                          ws_rotation_last_failure=self.rotation_last_failure,
                          ws_hot_rotate_seconds=ws_setting('HOT_ROTATE_SECONDS', 0, 0),
                          ws_hot_byte_silence_seconds=min(5, ws_setting('HOT_BYTE_SILENCE_SECONDS', 4, 3)),
                          ws_hot_byte_silence_peer_evidence_seconds=1.0,
                          ws_backup_promotions=self.backup_promotions,
                          ws_liveness_silence={k: {f: v for f, v in row.items() if not f.startswith('_')} for k, row in self.liveness_silence.items()},
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
            self.snapshot_recovery.close()
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
            if state.get('socket'):
                wake_reader(state.get('socket'))
        for state in closing:
            if state['thread'] and state['thread'].is_alive():
                state['thread'].join(max(0, deadline - time.monotonic()))
            if state.get('worker') and state['worker'].is_alive():
                state['worker'].join(max(0, deadline - time.monotonic()))
        if self.sampler and self.sampler.is_alive():
            self.sampler.join(max(0, deadline - time.monotonic()))
        if self.supervisor and self.supervisor.is_alive():
            self.supervisor.join(max(0, deadline - time.monotonic()))
        with _LOCK:
            STATUS['ws_connected'] = 0
            STATUS['connected'] = False

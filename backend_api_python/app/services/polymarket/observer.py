"""Scheduler-owned public market observer; it never places or signs orders."""
import json
import os
import threading
import time
from datetime import datetime, timezone

from app.utils.db import get_db_connection
from .client import PublicClient
from .engine import number, sweep
from . import updown

STATUS = {"enabled": False, "running": False, "mode": "stopped", "connected": False, "markets_subscribed": 0,
          "last_message_at": None, "reconnect_count": 0, "last_error": None, "started_at": None}
_LOCK = threading.RLock()
_STOP = threading.Event()
_THREAD = None
_LAST_SAMPLE = {}
_LAST_RESEARCH = {}
_REFERENCE = None


def enabled():
    return os.getenv("POLYMARKET_OBSERVER_ENABLED", "false").lower() in {"1", "true", "yes"}


def snapshot():
    with _LOCK:
        return dict(STATUS)


def _ws_connected():
    with _LOCK:
        STATUS.update(mode="ws", connected=True, last_error=None)


def _ws_error(error):
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
        asset = str(event.get("asset_id") or "")
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
            price, size = str(row.get("price")), number(row.get("size", 0))
            if size <= 0:
                books[asset][side].pop(price, None)
            else:
                books[asset][side][price] = str(size)
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
    reference = _REFERENCE.get(market['asset']) if _REFERENCE else {'price': None, 'source': 'unavailable', 'observed_at': None}
    tags = sample_tags(market, now)
    with get_db_connection() as db:
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
    with get_db_connection() as db:
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
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("UPDATE qd_polymarket_observer_episodes SET ended_at=last_seen_at,duration_ms=(EXTRACT(EPOCH FROM (last_seen_at-started_at))*1000)::bigint WHERE market_id=%s AND ended_at IS NULL", (market_id,))
            db.commit()
        finally:
            cur.close()


def _close_stale_episodes(market_ids, grace_seconds=60):
    """Close episodes unseen beyond the grace period or outside the current set."""
    with get_db_connection() as db:
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
    global _REFERENCE
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
    ws = None
    books = {}
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
                            STATUS['updown_subscriptions'] = [{k: m[k] for k in ('id', 'asset', 'interval', 'endDate')} for m in crypto]
                            STATUS['updown_rejections'] = rejected
                    active_ids = {m['id'] for m in markets}
                    for cache in (_LAST_SAMPLE, _LAST_RESEARCH):
                        for identity in list(cache):
                            if identity not in active_ids:
                                cache.pop(identity, None)
                    books = {}
                    _close_stale_episodes([m["id"] for m in markets], max(1, int(os.getenv("POLYMARKET_OBSERVER_STALE_GRACE_SECONDS", "60"))))
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
                    import websocket
                    assets = subscription_assets(markets)
                    def opened(sock, assets=tuple(assets)):
                        nonlocal ws_failures
                        ws_failures = 0
                        nonlocal backoff
                        backoff = 5
                        sock.send(json.dumps({"assets_ids": assets, "type": "market", "custom_feature_enabled": True}))
                        _ws_connected()
                    def received(sock, raw, markets=tuple(markets), books=books):
                        if raw == "PONG":
                            return
                        try:
                            event = json.loads(raw)
                            changed = ws_apply_event(event, books)
                            now = int(time.time() * 1000)
                            for market in markets:
                                if market["yesAssetId"] in changed or market["noAssetId"] in changed:
                                    now_mono = time.monotonic()
                                    if now_mono - _LAST_SAMPLE.get(market["id"], 0) < 1.0:
                                        continue
                                    _LAST_SAMPLE[market["id"]] = now_mono
                                    quote = opportunity(market, books, os.getenv("POLYMARKET_OBSERVER_SIZE_SHARES", "100"), os.getenv("POLYMARKET_OBSERVER_MERGE_COST", "0.02"))
                                    record_research(market, books, "ws", quote)
                                    if quote:
                                        record_sample(market, quote, "ws")
                                    else:
                                        _end_episode(market["id"], datetime.now(timezone.utc))
                            with _LOCK:
                                STATUS["last_message_at"] = datetime.now(timezone.utc).isoformat()
                        except Exception as exc:
                            with _LOCK:
                                STATUS["last_error"] = str(exc).splitlines()[0][:160]
                    def failed(_sock, err):
                        _ws_error(err)
                    def closed(*_args, markets=tuple(markets)):
                        _ws_disconnected()
                        _close_stale_episodes([m["id"] for m in markets], max(1, int(os.getenv("POLYMARKET_OBSERVER_STALE_GRACE_SECONDS", "60"))))
                    ws = websocket.WebSocketApp("wss://ws-subscriptions-clob.polymarket.com/ws/market", on_open=opened, on_message=received,
                          on_error=failed, on_close=closed)
                    stopper = threading.Thread(target=lambda: ws.run_forever(ping_interval=0), daemon=True)
                    stopper.start()
                    while stopper.is_alive() and time.monotonic() < refresh and not _STOP.wait(10):
                        try:
                            ws.send("PING")
                            _persist_status()
                        except Exception:
                            break
                    if _STOP.is_set():
                        ws.close()
                        stopper.join(5)
                        break
                    if time.monotonic() >= refresh:
                        ws.close()
                        stopper.join(5)
                        continue
                    ws.close()
                    stopper.join(5)
                    books = {}
                    with _LOCK:
                        STATUS["reconnect_count"] += 1
                    ws_failures += 1
                    _STOP.wait(backoff)
                    backoff = min(backoff * 2, 120)
                else:
                    with _LOCK:
                        STATUS.update(mode="rest", connected=True)
                    frames = client.capture(markets)
                    for market in markets:
                        frame = frames.get(market["id"], {})
                        if frame.get("books") and not frame.get("error"):
                            mapped = {a: {side: {str(x["price"]): str(x["size"]) for x in frame["books"][a][side]} for side in ("asks", "bids")} for a in (market["yesAssetId"], market["noAssetId"])}
                            quote = opportunity(market, mapped, os.getenv("POLYMARKET_OBSERVER_SIZE_SHARES", "100"), os.getenv("POLYMARKET_OBSERVER_MERGE_COST", "0.02"))
                            record_research(market, mapped, "rest", quote)
                            if quote:
                                record_sample(market, quote, "rest")
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
                _close_stale_episodes([m["id"] for m in markets], max(1, int(os.getenv("POLYMARKET_OBSERVER_STALE_GRACE_SECONDS", "60"))))
    finally:
        if _REFERENCE:
            _REFERENCE.close()
            _REFERENCE = None
        client.close()


def start():
    global _THREAD
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
        _STOP.clear()
        STATUS.update(enabled=True, running=True, mode="starting", connected=False, started_at=datetime.now(timezone.utc).isoformat(), last_error=None)
        _THREAD = threading.Thread(target=_market_loop, name="polymarket-observer", daemon=True)
        _THREAD.start()
    try:
        _persist_status()
    except Exception:
        pass
    return True


def stop(timeout=15):
    global _THREAD
    _STOP.set()
    with _LOCK:
        thread = _THREAD
    if thread:
        thread.join(timeout)
    with _LOCK:
        STATUS.update(running=False, connected=False, mode="stopped")
    try:
        _persist_status()
    except Exception:
        pass

"""Bounded public-feed evidence. Receive callbacks touch memory only.

PolyBolt currently gates TWAP behind credentials. The optional anonymous probe
never authenticates; a rejection disables it for this process. Missing boundary
prices remain unknown: neither spot nor a nearby tick substitutes for a TWAP.
"""
import json
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from app.utils.db import get_db_connection
from .client import PublicClient, GAMMA
from .maker_model import parse_rules
from .updown import ASSETS, INTERVALS, setting

PREFIX = 'qd_polymarket_observer_'
TABLES = ('books', 'trades', 'twap', 'windows', 'samples')
BOOK_SQL = f"INSERT INTO {PREFIX}books(recording_id,market_id,token_id,exchange_at,received_at,source,bids,asks) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT DO NOTHING"
TRADE_SQL = f"INSERT INTO {PREFIX}trades(recording_id,market_id,token_id,price,size,side,exchange_at,received_at,transaction_hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
TWAP_SQL = f"INSERT INTO {PREFIX}twap(asset,window_seconds,exchange_at,received_at,price,source) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
WINDOW_SQL = f"INSERT INTO {PREFIX}windows(market_id,condition_id,asset,interval,start_time,end_time,price_to_beat,up_token_id,down_token_id,window_seconds) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (market_id) DO UPDATE SET up_token_id=EXCLUDED.up_token_id,down_token_id=EXCLUDED.down_token_id WHERE (qd_polymarket_observer_windows.up_token_id,qd_polymarket_observer_windows.down_token_id) IS DISTINCT FROM (EXCLUDED.up_token_id,EXCLUDED.down_token_id)"


def flag(name, default=False):
    return os.getenv('POLYMARKET_OBSERVER_' + name, str(default)).lower() in {'true', 'yes', '1'}


def exchange_time(value):
    """Accept documented epoch milliseconds or ISO timestamps; never invent one."""
    if value is None or value == '':
        return None
    try:
        if isinstance(value, str) and ('T' in value or ' ' in value):
            result = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if result.tzinfo is None:
                return None
            return result.astimezone(timezone.utc)
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def decimal(value, *, low=0, high=None):
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result < low or high is not None and result > high:
            return None
        return str(result)
    except (ValueError, TypeError, InvalidOperation):
        return None


def events(event):
    if isinstance(event, list):
        for item in event:
            yield from events(item)
    elif isinstance(event, dict):
        yield event.get('event_type') or event.get('type'), event.get('payload') or event


def owned_events(event, owned):
    """Discard handover overlap from nonowners, preserving event envelopes."""
    if owned is None:
        return event
    if isinstance(event, list):
        return [owned_events(item, owned) for item in event]
    if not isinstance(event, dict):
        return event
    kind = event.get('event_type') or event.get('type')
    payload = event.get('payload') or event
    if kind in {'book', 'last_trade_price'}:
        token = str(payload.get('asset_id') or payload.get('assetId') or '')
        return event if token in owned else {}
    if kind == 'price_change':
        field = 'price_changes' if 'price_changes' in payload else 'priceChanges'
        filtered = {**payload, field: [p for p in payload.get(field, [])
                    if str(p.get('asset_id') or p.get('assetId') or '') in owned]}
        return {**event, 'payload': filtered} if 'payload' in event else filtered
    return event


def recording_scope():
    """Up/Down assets and intervals whose books, trades and windows are recorded.

    YES/NO markets are always recorded. Unset or empty keeps the original BTC/ETH
    5m/15m scope; unknown names are dropped. Discovery (UPDOWN_*) still decides
    which markets exist, so recording only narrows it.
    """
    def pick(name, default, allowed, case):
        names = [case(x.strip()) for x in os.getenv('POLYMARKET_OBSERVER_RECORDING_' + name, '').split(',') if x.strip()]
        return tuple(dict.fromkeys(v for v in names or default if v in allowed))
    return pick('ASSETS', ('BTC', 'ETH'), ASSETS, str.upper), pick('INTERVALS', ('5m', '15m'), INTERVALS, str.lower)


def scoped(market, scope=None):
    assets, intervals = scope or recording_scope()
    return market.get('market_type', 'yes_no') == 'yes_no' or (
        market.get('asset') in assets and market.get('interval') in intervals)


def settlement_window(market):
    """Settlement averaging window from the rule text: Chainlink TWAP 60 s, Binance print 0.

    Unreadable rules keep the label convention (1h/1d Binance candles), so a Binance
    window is never joined to Chainlink TWAP ticks.
    """
    spec = parse_rules(market)
    if spec:
        return int(spec['w'])
    return 0 if market.get('interval') in ('1h', '1d') else 60


def top_five(book):
    result = []
    for side in ('bids', 'asks'):
        levels = []
        for price, size in book.get(side, {}).items():
            p, z = decimal(price, high=1), decimal(size)
            if p is None or z is None:
                return None  # A malformed level invalidates evidence, not silently truncated.
            if Decimal(z) > 0:
                levels.append({'price': p, 'size': z})
        levels.sort(key=lambda x: Decimal(x['price']), reverse=side == 'bids')
        result.append(levels[:5])
    return result


def parse_trade(payload, received):
    token = str(payload.get('asset_id') or payload.get('assetId') or '')
    price = decimal(payload.get('price'), high=1)
    raw_size = payload.get('size')
    size = decimal(raw_size) if raw_size is not None else None
    side = str(payload.get('side') or '').upper()
    if not token or price is None or raw_size is not None and size is None or side not in {'BUY', 'SELL'}:
        return None
    return (token, price, size, side, exchange_time(payload.get('timestamp')), received,
            payload.get('transaction_hash') or payload.get('transactionHash'))


def parse_twap(event, received):
    if not isinstance(event, dict) or event.get('channel') != 'price.crypto.twap':
        return []
    payload = event.get('payload') or {}
    asset = {'btcusd': 'BTC', 'ethusd': 'ETH'}.get(payload.get('symbol'))
    if not asset or payload.get('source') != 'chainlink':
        return []
    result = []
    for row in payload.get('data', [payload]):
        window = row.get('window_seconds', payload.get('window_seconds'))
        price = decimal(row.get('full_accuracy_value', row.get('value')))
        at = exchange_time(row.get('timestamp'))
        if window == 60 and at is not None and price is not None and Decimal(price) > 0:
            result.append((asset, 60, at, received, price, 'chainlink'))
    return result


def computed_outcome(start, end):
    a, b = decimal(start), decimal(end)
    if a is None or b is None or Decimal(a) <= 0 or Decimal(b) <= 0:
        return None
    return 'Up' if Decimal(b) >= Decimal(a) else 'Down'


def official_outcome(row, condition):
    if not isinstance(row, dict) or row.get('conditionId') != condition or row.get('closed') is not True or row.get('umaResolutionStatus') != 'resolved':
        return None
    try:
        labels, prices = row.get('outcomes'), row.get('outcomePrices')
        labels = json.loads(labels) if isinstance(labels, str) else labels
        prices = json.loads(prices) if isinstance(prices, str) else prices
        if labels not in (['Up', 'Down'], ['Down', 'Up']) or not isinstance(prices, list) or len(prices) != 2:
            return None
        values = [decimal(p, high=1) for p in prices]
        if any(p is None for p in values) or sorted(Decimal(p) for p in values) != [0, 1]:
            return None
        return labels[next(i for i, p in enumerate(values) if Decimal(p) == 1)]
    except (ValueError, TypeError):
        return None


class Recorder:
    """Protected by Pipeline.lock; a bounded staging buffer precedes its writer."""
    def __init__(self):
        self.enabled = flag('RECORDING_ENABLED', True)
        self.scope = recording_scope()
        self.metadata = {}
        self.delivery_samples = deque(maxlen=8192)
        self.last_books = {}
        self.pending = deque()
        self.maximum = setting('RECORDING_BUFFER_MAX', 10000, 1, 100000)
        self.dropped = 0
        self.invalid = 0
        self.last_twap = {}
        self.twap_cache = {}
        self.health_lock = threading.Lock()
        self.writes = {table: deque(maxlen=61) for table in TABLES}

    def prune(self, tokens):
        for cache in (self.metadata, self.last_books):
            for token in list(cache):
                if token not in tokens:
                    cache.pop(token, None)

    def stage(self, key, kind, sql, params):
        if len(self.pending) >= self.maximum:
            self.pending.popleft()
            self.dropped += 1
        self.pending.append((key, kind, [(sql, params)]))

    def receive(self, event, changed, markets, asset_markets, received, owned=None):
        changed = set(changed)
        for kind, payload in events(event):
            if kind == 'book':
                tokens = [str(payload.get('asset_id') or payload.get('assetId') or '')]
            elif kind == 'price_change':
                tokens = [str(p.get('asset_id') or p.get('assetId') or '')
                          for p in payload.get('price_changes', payload.get('priceChanges', []))]
            else:
                tokens = []
            at = exchange_time(payload.get('timestamp'))
            if at is not None:
                from .ws_balance import event_assets
                lag = (received - at).total_seconds() * 1000
                for asset in event_assets(payload):
                    if asset in asset_markets and (owned is None or asset in owned):
                        self.delivery_samples.append((time.monotonic(), asset, lag))
            for token in set(tokens):
                if token in changed and (owned is None or token in owned):
                    previous = self.metadata.get(token, (None,))[0]
                    if previous is None or at is None or at >= previous:
                        self.metadata[token] = (at, received, 'ws')
            if self.enabled and kind == 'last_trade_price':
                trade = parse_trade(payload, received)
                if trade is None:
                    self.invalid += 1
                    continue
                market = markets.get(asset_markets.get(trade[0]))
                if market and scoped(market, self.scope) and (owned is None or trade[0] in owned):
                    self.stage(trade[0], 'trade', TRADE_SQL, (str(uuid4()), market['id'], *trade))

    def books(self, books, markets, asset_markets, metadata, now):
        if not self.enabled:
            return []
        groups = []
        interval = setting('BOOK_MIN_INTERVAL_MS', 1000, 100, 60000) / 1000
        for token, book in books.items():
            market = markets.get(asset_markets.get(token))
            meta = metadata.get(token)
            if not market or not scoped(market, self.scope) or not meta:
                continue
            previous = self.last_books.get(token)
            if previous and now - previous[0] < interval:
                continue
            levels = top_five(book)
            if levels is None or previous and levels == previous[1]:
                continue
            self.last_books[token] = (now, levels)
            at, received, source = meta
            groups.append((token, 'book', [(BOOK_SQL, (str(uuid4()), market['id'], token, at, received, source,
                                               json.dumps(levels[0]), json.dumps(levels[1])))]))
        return groups

    def twap(self, event, received, now):
        for tick in parse_twap(event, received):
            asset, _, at, _, _, _ = tick
            # Reject replayed/out-of-order ticks from the live cache. Snapshot
            # history may still be recorded for exact start/end boundary joins.
            cached = self.twap_cache.get(asset)
            if not cached or at > cached[2]:
                self.twap_cache[asset] = tick
            boundary = at.microsecond == 0 and int(at.timestamp()) % 300 == 0
            last = self.last_twap.get(asset, float('-inf'))
            if now - last < setting('TWAP_MIN_INTERVAL_MS', 1000, 100, 60000) / 1000 and not boundary:
                continue
            self.last_twap[asset] = now
            self.stage(asset, 'twap', TWAP_SQL, tick)

    def committed(self, counts):
        second = int(time.monotonic())
        with self.health_lock:
            for table, count in counts.items():
                history = self.writes[table]
                if history and history[-1][0] == second:
                    history[-1] = (second, history[-1][1] + count)
                else:
                    history.append((second, count))

    def metrics(self, books, groups=None, rates=None, hot_threshold=100):
        group_rates = rates or {}
        now = datetime.now(timezone.utc)
        ages = sorted((now - at).total_seconds() * 1000 for token, (at, _, _) in self.metadata.items()
                      if at is not None and token in books and at <= now)
        second = int(time.monotonic())
        with self.health_lock:
            rates = {table: sum(count for at, count in history if at > second - 60)
                     for table, history in self.writes.items()}
        receipt_ages = sorted((now - received).total_seconds() * 1000
                              for token, (_, received, _) in self.metadata.items()
                              if token in books and received is not None and received <= now)
        arrival_lags = sorted((received - at).total_seconds() * 1000
                              for token, (at, received, _) in self.metadata.items()
                              if token in books and at is not None and received is not None and at <= received)

        def percentile(q, values=ages):
            return values[min(len(values) - 1, math.ceil(len(values) * q) - 1)] if values else None
        freshness = {}
        hot_details = []
        delivery = {}
        if groups is not None:
            # Snapshot cross-section, not message-weighted: every subscribed token
            # counts once; missing and unknown timestamps cannot look fresh.
            for name in ('hot', 'cold', 'unmeasured'):
                tokens = [a for a, key in groups.items() if
                          ('unmeasured' if key not in group_rates else
                           'hot' if group_rates[key] >= hot_threshold else 'cold') == name]
                values, receipts, lags = [], [], []
                for token in tokens:
                    at, received, _ = self.metadata.get(token, (None, None, None))
                    if token not in books:
                        continue
                    if at is not None and at <= now:
                        values.append((now - at).total_seconds() * 1000)
                    if received is not None and received <= now:
                        receipts.append((now - received).total_seconds() * 1000)
                    if at is not None and received is not None and at <= received:
                        lags.append((received - at).total_seconds() * 1000)
                    if name == 'hot':
                        hot_details.append(dict(token=token, group=groups[token],
                            group_msg_per_s=group_rates[groups[token]],
                            book_age_ms=(now - at).total_seconds() * 1000 if at is not None else None,
                            receipt_age_ms=(now - received).total_seconds() * 1000 if received is not None else None,
                            arrival_lag_ms=(received - at).total_seconds() * 1000
                                if at is not None and received is not None else None))
                token_set = set(tokens)
                sample_floor = time.monotonic() - 60
                samples = sorted(lag for at, token, lag in self.delivery_samples
                                 if at > sample_floor and token in token_set)
                delivery[name] = dict(samples=len(samples), p50_ms=percentile(.5, samples),
                    p95_ms=percentile(.95, samples), max_ms=max(samples) if samples else None,
                    negative_clock_samples=sum(v < 0 for v in samples),
                    within_1s=(max(samples) <= 1000 and min(samples) >= 0) if samples else None)
                freshness[name] = dict(tokens=len(tokens), measured_tokens=len(values),
                    missing_tokens=len(tokens) - len(values),
                    book_age_p95_ms=percentile(.95, sorted(values)),
                    update_silence_p95_ms=percentile(.95, sorted(receipts)),
                    updated_within_30s_tokens=sum(v <= 30000 for v in receipts),
                    book_receipt_age_p95_ms=percentile(.95, sorted(receipts)),
                    book_arrival_lag_p95_ms=percentile(.95, sorted(lags)))
        return dict(ws_delivery_latency_by_activity=delivery, ws_delivery_latency_window_seconds=60,
                    ws_delivery_latency_max_samples=8192, book_freshness_by_activity=freshness, book_freshness_hot_threshold_msg_per_s=hot_threshold,
                    book_hot_oldest_tokens=sorted(hot_details,
                        key=lambda row: -(row['book_age_ms'] if row['book_age_ms'] is not None else float('inf')))[:8],
                    recording_enabled=self.enabled, recording_rows_per_minute=rates,
                    recording_scope=dict(assets=list(self.scope[0]), intervals=list(self.scope[1])),
                    recording_retention_days=retention_days(),
                    recording_buffer_depth=len(self.pending), recording_dropped=self.dropped,
                    recording_invalid=self.invalid, book_age_p50_ms=percentile(.5),
                    book_age_p95_ms=percentile(.95), book_age_tokens=len(ages),
                    book_receipt_age_p95_ms=percentile(.95, receipt_ages),
                    book_arrival_lag_p95_ms=percentile(.95, arrival_lags))


def retention_days():
    books = max(1, min(365, int(os.getenv('POLYMARKET_OBSERVER_RECORDING_RETENTION_DAYS') or 7)))
    trades = max(1, min(365, int(os.getenv('POLYMARKET_OBSERVER_TRADES_RETENTION_DAYS') or books)))
    return dict(books=books, trades=trades,
                twap=setting('TWAP_RETENTION_DAYS', 7, 1, 365),
                windows=setting('WINDOW_RETENTION_DAYS', 30, 1, 365))


def cleanup(cur):
    # Chunk indexed deletes, keeping cleanup transactions bounded. Repeated
    # ten-minute passes remove a backlog without holding a huge transaction.
    days = retention_days()
    for table, column in (
        ('books', 'received_at'),
        ('trades', 'received_at'),
        ('twap', 'exchange_at'),
        ('windows', 'end_time'),
    ):
        cur.execute(f"DELETE FROM {PREFIX}{table} WHERE ctid IN (SELECT ctid FROM {PREFIX}{table} WHERE {column} < NOW() - (%s * INTERVAL '1 day') ORDER BY {column} LIMIT %s)",
                    (days[table], setting('RECORDING_CLEANUP_BATCH', 100000, 100, 1000000)))


class References:
    """Public resolution worker plus optional anonymous TWAP socket."""
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.stop_event = threading.Event()
        self.state = 'disabled'
        self.error = None
        self.resolution_error = None
        self.last_tick_at = None
        self.socket = None
        self.socket_thread = None
        self.connected_monotonic = None
        self.thread = threading.Thread(target=self.run, name='polymarket-resolution', daemon=True)

    def metrics(self):
        state = self.state
        if state == 'streaming' and self.last_tick_at and (datetime.now(timezone.utc) - datetime.fromisoformat(self.last_tick_at)).total_seconds() > 30:
            state = 'stale'
        return dict(twap_stream_state=state, twap_stream_error=self.error,
                    twap_last_tick_at=self.last_tick_at, resolution_error=self.resolution_error)

    def receive(self, sock, raw):
        if self.stop_event.is_set():
            return
        try:
            event = json.loads(raw)
            if event.get('op') == 'error':
                code = event.get('code')
                if code in {'auth_required', 'unauthorized', 'forbidden'}:
                    self.state, self.error = 'auth_required', 'TWAP requires credentials; anonymous recording unavailable'
                    sock.close()
                else:
                    self.state, self.error = 'unavailable', 'TWAP subscription rejected'
                    sock.close()
                return
            ticks = parse_twap(event, datetime.now(timezone.utc))
            if ticks:
                with self.pipeline.lock:
                    self.pipeline.recording.twap(event, datetime.now(timezone.utc), time.monotonic())
                self.state, self.error = 'streaming', None
                self.last_tick_at = datetime.now(timezone.utc).isoformat()
        except (ValueError, TypeError, KeyError):
            self.error = 'Invalid TWAP frame'

    def connect(self):
        import websocket
        self.state = 'connecting'
        self.connected_monotonic = time.monotonic()
        def opened(sock):
            # This is deliberately the only outgoing subscription: no auth,
            # wallet, signing, challenge response or environment credentials.
            if self.stop_event.is_set():
                sock.close()
                return
            sock.send(json.dumps({'op': 'subscribe', 'rid': 'observer-twap', 'subscriptions': [
                {'channel': 'price.crypto.twap', 'filter': {'symbol': symbol, 'window_seconds': 60}}
                for symbol in ('btcusd', 'ethusd')]}))
            self.state = 'subscribing'
        def failed(*_):
            if self.state != 'auth_required':
                self.state, self.error = 'unavailable', 'Anonymous TWAP connection unavailable'
        def closed(*_):
            if not self.stop_event.is_set() and self.state != 'auth_required':
                failed()
        self.socket = websocket.WebSocketApp('wss://ws-live-v2.polymarket.com/ws',
            on_open=opened, on_message=self.receive, on_error=failed, on_close=closed)
        self.socket_thread = threading.Thread(target=lambda: self.socket.run_forever(ping_interval=20, ping_timeout=10),
                                              name='polymarket-twap-receive', daemon=True)
        self.socket_thread.start()

    def poll(self, client):
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute(f"SELECT * FROM {PREFIX}windows WHERE end_time <= NOW() AND end_time >= NOW() - (%s * INTERVAL '1 day') AND official_outcome IS NULL ORDER BY resolution_checked_at NULLS FIRST,end_time LIMIT 10",
                            (setting('WINDOW_RETENTION_DAYS', 30, 1, 365),))
                windows = cur.fetchall()
            finally:
                cur.close()
        self.resolution_error = None
        for window in windows:
            if self.stop_event.is_set():
                break
            identity = window['market_id']
            if not identity.isdigit() or len(identity) > 20:
                continue
            try:
                response = client._json('GET', f'{GAMMA}/markets/{identity}')
            except ValueError:
                self.pipeline.queue.put(identity, 'resolution', [(
                    f'UPDATE {PREFIX}windows SET resolution_checked_at=NOW() WHERE market_id=%s', (identity,))])
                self.resolution_error = 'Public resolution read unavailable'
                continue
            outcome = official_outcome(response, window['condition_id'])
            evidence = {k: response.get(k) for k in ('id', 'conditionId', 'closed', 'umaResolutionStatus', 'outcomes', 'outcomePrices')}
            self.pipeline.queue.put(identity, 'resolution', [(
                f"UPDATE {PREFIX}windows SET official_outcome=COALESCE(%s,official_outcome),resolution=%s::jsonb,resolution_checked_at=NOW(),updated_at=NOW() WHERE market_id=%s",
                (outcome, json.dumps(evidence), identity))])
        # Exact exchange boundary only. Missing observations stay null, including
        # after restart; persisted ticks, not a spot reconstruction, fill gaps.
        self.pipeline.queue.put('*', 'reference', reference_updates())

    def run(self):
        client = PublicClient()
        retry = 0
        poll_at = time.monotonic() + 10
        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                if flag('TWAP_ENABLED') and self.state != 'auth_required' and now >= retry:
                    if not self.socket_thread or not self.socket_thread.is_alive():
                        self.connect()
                        retry = now + 300
                if (self.state in {'connecting', 'subscribing'} and self.connected_monotonic is not None
                        and now - self.connected_monotonic > 30 and self.socket):
                    self.state, self.error = 'unavailable', 'Anonymous TWAP subscription timed out'
                    self.socket.close()
                if now >= poll_at:
                    try:
                        self.poll(client)
                    except Exception:
                        self.resolution_error = 'Public resolution read unavailable'
                    poll_at = now + setting('RESOLUTION_POLL_SECONDS', 60, 10, 3600)
                self.stop_event.wait(1)
        finally:
            client.close()

    def close(self, timeout):
        self.stop_event.set()
        if self.socket:
            self.socket.close()
        deadline = time.monotonic() + timeout
        if self.socket_thread:
            self.socket_thread.join(max(0, deadline - time.monotonic()))
        if self.thread.is_alive():
            self.thread.join(max(0, deadline - time.monotonic()))
        self.state = 'stopped'


def reference_updates():
    return [
        (f"UPDATE {PREFIX}windows w SET price_to_beat=t.price,start_twap_at=t.exchange_at,updated_at=NOW() FROM {PREFIX}twap t WHERE w.asset=t.asset AND w.window_seconds=t.window_seconds AND w.start_time=t.exchange_at AND w.start_twap_at IS NULL", ()),
        (f"UPDATE {PREFIX}windows w SET settlement_twap=t.price,settlement_twap_at=t.exchange_at,updated_at=NOW() FROM {PREFIX}twap t WHERE w.asset=t.asset AND w.window_seconds=t.window_seconds AND w.end_time=t.exchange_at AND w.settlement_twap_at IS NULL", ()),
        (f"UPDATE {PREFIX}windows SET computed_outcome=CASE WHEN settlement_twap>=price_to_beat THEN 'Up' ELSE 'Down' END,outcome_matches=CASE WHEN official_outcome IS NULL THEN NULL ELSE official_outcome=CASE WHEN settlement_twap>=price_to_beat THEN 'Up' ELSE 'Down' END END WHERE start_twap_at IS NOT NULL AND settlement_twap_at IS NOT NULL AND (computed_outcome IS NULL OR outcome_matches IS NULL AND official_outcome IS NOT NULL)", ()),
        (f"UPDATE {PREFIX}samples s SET price_to_beat=w.price_to_beat FROM {PREFIX}windows w WHERE s.market_id=w.market_id AND w.price_to_beat IS NOT NULL AND s.price_to_beat IS DISTINCT FROM w.price_to_beat", ()),
    ]

"""Bounded public crypto catalogue and research reference prices. No credentials."""
import json
import logging
import os
import random
import threading
import time
from collections import Counter, deque
from datetime import datetime, timedelta, timezone

import requests

from .engine import _array, market_from_gamma, number

ASSETS = ('BTC', 'ETH', 'SOL', 'XRP')
INTERVALS = {'5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
EXPIRY_BUCKETS = ('>10m', '5-10m', '2-5m', '1-2m', '30-60s', '<30s')
# Binance spot reference sources written to observer samples; settlement proxies accept all.
REST_SOURCES = ('api.binance.com', 'data-api.binance.vision')
STREAM_URLS = (('stream.binance.com', 'wss://stream.binance.com:9443/stream'),
               ('data-stream.binance.vision', 'wss://data-stream.binance.vision/stream'))
BINANCE_SOURCES = REST_SOURCES + tuple(host for host, _ in STREAM_URLS)
UNAVAILABLE = {'price': None, 'source': 'unavailable', 'observed_at': None}
_LOG = logging.getLogger(__name__)


def enabled():
    return os.getenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'false').lower() in {'1', 'true', 'yes'}


def flag(name, default='false'):
    return os.getenv('POLYMARKET_OBSERVER_' + name, default).lower() in {'1', 'true', 'yes'}


def setting(name, default, low, high):
    return max(low, min(high, int(os.getenv('POLYMARKET_OBSERVER_' + name, str(default)))))


def selections(name, defaults, allowed):
    return tuple(dict.fromkeys(x.strip() for x in os.getenv('POLYMARKET_OBSERVER_UPDOWN_' + name, ','.join(defaults)).split(',') if x.strip() in allowed))


def series_slug(asset, interval):
    """Gamma series slug; SOL hourly/daily series are published as "solana-"."""
    name = 'solana' if asset == 'SOL' and interval in ('1h', '1d') else asset.lower()
    return f"{name}-up-or-down-{ {'1h': 'hourly', '1d': 'daily'}.get(interval, interval)}"


def timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if result.tzinfo is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError):
        raise ValueError('polymarket.windowUnknown') from None


def expiry_bucket(market, now):
    if market.get('market_type') != 'up_down':
        return ''
    seconds = (timestamp(market['endDate']) - now).total_seconds()
    for boundary, label in ((600, '>10m'), (300, '5-10m'), (120, '2-5m'), (60, '1-2m'), (30, '30-60s')):
        if seconds >= boundary:
            return label
    return '<30s'


def market_from_updown(row, *, asset, interval, now=None):
    """Reuse standard binary constraints, explicitly verify Up/Down fee semantics."""
    now = now or datetime.now(timezone.utc)
    outcomes = _array(row.get('outcomes'))
    if outcomes not in (['Up', 'Down'], ['Down', 'Up']):
        raise ValueError('polymarket.upDownBinaryRequired')
    if asset not in ASSETS or interval not in INTERVALS:
        raise ValueError('polymarket.windowUnknown')
    config = row.get('cryptoMarketConfig')
    if config and (config.get('asset', '').upper() != asset or config.get('duration') != interval):
        raise ValueError('polymarket.windowMismatch')
    start, end = timestamp(row.get('eventStartTime')), timestamp(row.get('endDate'))
    if end <= now:
        raise ValueError('polymarket.marketClosed')
    # Daily windows can span 23/25 hours across US daylight-saving changes.
    duration = (end - start).total_seconds()
    if duration != INTERVALS[interval] and not (interval == '1d' and duration in (82800, 90000)):
        raise ValueError('polymarket.windowMismatch')
    schedule = row.get('feeSchedule')
    if row.get('feesEnabled') is not True or not isinstance(schedule, dict) or type(schedule.get('exponent')) is not int or schedule.get('exponent') != 1 or number(schedule.get('rate')) != number('0.07') or schedule.get('takerOnly') is not True:
        raise ValueError('polymarket.feeUnknown')
    if row.get('feeRate') is not None and number(row['feeRate']) != number(schedule['rate']):
        raise ValueError('polymarket.feeUnknown')
    if row.get('feeExponent') is not None and row['feeExponent'] != 1:
        raise ValueError('polymarket.feeUnknown')
    converted = {**row, 'outcomes': ['Yes' if x == 'Up' else 'No' for x in outcomes]}
    market = market_from_gamma(converted)
    strike = row.get('priceToBeat')
    if strike is not None:
        strike = number(strike)
        if strike <= 0:
            raise ValueError('polymarket.referenceInvalid')
        strike = str(strike)
    return {**market, 'market_type': 'up_down', 'asset': asset, 'interval': interval,
            'eventStartTime': start.isoformat(), 'resolutionSource': str(row.get('resolutionSource') or '')[:500],
            'price_to_beat': strike}


def discover(client, series_cache, *, now=None, prewarm=None):
    now = now or datetime.now(timezone.utc)
    assets = selections('ASSETS', ASSETS, ASSETS)
    intervals = selections('INTERVALS', INTERVALS, INTERVALS)
    cap = setting('UPDOWN_MAX_MARKETS', 40, 1, 100)
    per_pair = setting('UPDOWN_PER_PAIR_LIMIT', 2, 1, 10)
    ahead = setting('UPDOWN_LOOKAHEAD_SECONDS', 1800, 0, 86400)
    selected, excluded, conditions = [], [], set()
    next_hot = {}
    for asset in assets:
        for interval in intervals:
            if len(selected) >= cap:
                continue
            slug = series_slug(asset, interval)
            try:
                if slug not in series_cache:
                    series = client._json('GET', 'https://gamma-api.polymarket.com/series', params={'slug': slug})
                    series_cache[slug] = next((str(s['id']) for s in series if s.get('slug') == slug), None)
                series_id = series_cache[slug]
                if not series_id:
                    series_cache.pop(slug, None)
                    raise ValueError('polymarket.seriesUnavailable')
                events = client._json('GET', 'https://gamma-api.polymarket.com/events', params={
                    'series_id': series_id, 'active': 'true', 'closed': 'false', 'limit': min(100, per_pair + 5),
                    'order': 'endDate', 'ascending': 'true', 'end_date_min': now.isoformat(),
                    'end_date_max': (now + timedelta(seconds=ahead + INTERVALS[interval] + 3600)).isoformat()})
                pair = 0
                for event in events:
                    if event.get('seriesSlug') != slug:
                        continue
                    for row in event.get('markets', []):
                        try:
                            market = market_from_updown({**row, 'events': [event]}, asset=asset, interval=interval, now=now)
                            start = timestamp(market['eventStartTime'])
                            if (prewarm is not None and asset in ('BTC', 'ETH') and interval in ('5m', '15m')
                                    and now < start <= now + timedelta(seconds=INTERVALS[interval])):
                                key = (asset, interval)
                                if key not in next_hot or start < timestamp(next_hot[key]['eventStartTime']):
                                    next_hot[key] = market
                            if pair >= per_pair or len(selected) >= cap:
                                continue
                            if timestamp(market['eventStartTime']) > now + timedelta(seconds=ahead):
                                continue
                            if market['conditionId'] in conditions:
                                continue
                            selected.append(market)
                            conditions.add(market['conditionId'])
                            pair += 1
                            if len(selected) >= cap and prewarm is None:
                                return selected, excluded
                            if pair >= per_pair and prewarm is None:
                                break
                        except (ValueError, KeyError, TypeError) as exc:
                            excluded.append({'marketId': str(row.get('id', '')), 'reason': str(exc)})
                    if pair >= per_pair and prewarm is None:
                        break
            except (ValueError, TypeError, KeyError) as exc:
                excluded.append({'series': slug, 'reason': str(exc)})
    if prewarm is not None:
        selected_pairs = {(m['asset'], m['interval']) for m in selected}
        prewarm.extend(market for key, market in next_hot.items() if key in selected_pairs)
    return selected, excluded[:100]


class ReferencePrices:
    """Shared per-asset cache; failed reads are cached too. Worker keeps HTTP off book callbacks."""
    def __init__(self, session=None, clock=time.monotonic):
        self.session = session or requests.Session()
        self.clock = clock
        self.cache = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None

    def fetch(self, asset):
        ttl = setting('REFERENCE_CACHE_SECONDS', 5, 2, 60)
        with self.lock:
            cached = self.cache.get(asset)
            if cached and self.clock() - cached[0] < ttl:
                return dict(cached[1])
        result = {'price': None, 'source': 'unavailable', 'observed_at': None}
        for host in REST_SOURCES:
            try:
                response = self.session.get(f'https://{host}/api/v3/ticker/price', params={'symbol': asset + 'USDT'}, timeout=(1, 2), allow_redirects=False)
                response.raise_for_status()
                data = response.json()
                price = number(data['price'])
                if data.get('symbol') != asset + 'USDT' or price <= 0:
                    continue
                result = {'price': str(price), 'source': host, 'observed_at': datetime.now(timezone.utc).isoformat()}
                break
            except (requests.RequestException, ValueError, TypeError, KeyError):
                continue
        with self.lock:
            self.cache[asset] = (self.clock(), result)
        return dict(result)

    def get(self, asset):
        with self.lock:
            entry = self.cache.get(asset)
            if entry and self.clock() - entry[0] <= setting('REFERENCE_CACHE_SECONDS', 5, 2, 60) * 3:
                return dict(entry[1])
        return {'price': None, 'source': 'unavailable', 'observed_at': None}

    def start(self):
        def run():
            while not self.stop_event.is_set():
                for asset in selections('ASSETS', ASSETS, ASSETS):
                    if self.stop_event.is_set():
                        break
                    self.fetch(asset)
                self.stop_event.wait(setting('REFERENCE_CACHE_SECONDS', 5, 2, 60))
        self.thread = threading.Thread(target=run, name='polymarket-reference', daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(15)
        self.session.close()


def parse_stream(raw):
    """Binance combined-stream frame -> ('book', asset, mid) | ('trade', asset, event_ms) | None."""
    try:
        frame = json.loads(raw)
        data = frame['data']
        symbol, kind = frame['stream'].split('@', 1)
        asset = symbol.upper()[:-4]
        if not symbol.endswith('usdt') or asset not in ASSETS or data.get('s') != asset + 'USDT':
            return None
        if kind == 'bookTicker':
            bid, ask = number(data['b']), number(data['a'])
            return ('book', asset, (bid + ask) / 2) if 0 < bid < ask else None
        if kind == 'aggTrade':
            at = data.get('E', data.get('T'))
            return ('trade', asset, at) if type(at) is int and at > 0 else None
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    return None


class StreamingReferencePrices:
    """Binance public bookTicker mid per asset over one combined stream. No credentials, no orders.

    The socket callback only parses a frame and stores it under the lock. A supervisor thread owns
    connects, host fallback, backoff, a silence watchdog and rotation before the 24 h server cut.
    bookTicker pushes only on change, so while the connection keeps delivering frames an unchanged
    asset is confirmed as of the latest frame (for up to STALE_S after its own last update).
    """
    STALE_S = 10  # An open socket with no frame this long is treated as dead.
    MAX_AGE_S = 23.5 * 3600  # Binance drops every connection at 24 h; rotate first.
    HEALTHY_S = 60  # A connection that streamed this long resets the backoff.
    BACKOFF_S = (1, 60)
    LAG_SAMPLES = 2000
    OFFSET_EVERY_S = 600  # Re-estimate the local clock against Binance server time.

    def __init__(self, assets=None, *, aggtrade=None, socket_factory=None, session=None,
                 clock=time.monotonic, wall=time.time, jitter=random.uniform):
        self.assets = tuple(assets or selections('ASSETS', ASSETS, ASSETS))
        self.aggtrade = flag('SPOT_WS_AGGTRADE', 'true') if aggtrade is None else aggtrade
        self.socket_factory = socket_factory
        self.session = session
        self.offset = None  # (server - local ms, round trip ms) of the tightest /api/v3/time probe
        self.offset_at = None
        self.clock, self.wall, self.jitter = clock, wall, jitter
        self.lock = threading.Lock()
        self.prices = {}  # asset -> (received monotonic, received epoch s, mid, host, generation)
        self.lags = deque(maxlen=self.LAG_SAMPLES)  # receive time - exchange event time, ms
        self.generation = 0
        self.frames = 0
        self.last_frame = self.last_wall = self.opened_at = None
        self.stop_event = threading.Event()
        self.thread = self.socket = self.socket_thread = None
        self.host_index = self.attempts = 0
        self.retry_at = 0.0
        self.planned = False
        self.state, self.error = 'stopped', None
        self.connects = self.reconnects = self.rotations = 0

    def url(self):
        streams = [f'{a.lower()}usdt@bookTicker' for a in self.assets]
        if self.aggtrade:
            streams += [f'{a.lower()}usdt@aggTrade' for a in self.assets]
        return STREAM_URLS[self.host_index][1] + '?streams=' + '/'.join(streams)

    def receive(self, raw, host, generation):
        mono, wall = self.clock(), self.wall()
        parsed = parse_stream(raw)
        with self.lock:
            if generation != self.generation:
                return  # Late frame from a socket that is already being replaced.
            self.frames += 1
            self.last_frame, self.last_wall = mono, wall
            if parsed is None:
                return
            kind, asset, value = parsed
            if kind == 'book':
                self.prices[asset] = (mono, wall, value, host, generation)
            else:
                self.lags.append(wall * 1000 - value)

    def connect(self):
        factory = self.socket_factory
        if factory is None:
            import websocket
            factory = websocket.WebSocketApp
        host = STREAM_URLS[self.host_index][0]
        with self.lock:
            self.generation += 1
            generation = self.generation
            self.last_frame = self.last_wall = None
            self.opened_at = self.clock()
        self.state = 'connecting'
        self.connects += 1

        def opened(sock):
            if self.stop_event.is_set():
                sock.close()
            elif generation == self.generation:
                self.state = 'connected'

        def failed(_sock, error):
            if generation == self.generation:
                self.error = str(error)[:160]

        # Market-data streams only: nothing is ever sent, so no subscription or auth frames.
        sock = factory(self.url(), on_open=opened, on_message=lambda _s, raw: self.receive(raw, host, generation),
                       on_error=failed)
        self.socket = sock
        self.socket_thread = threading.Thread(
            target=lambda: sock.run_forever(ping_interval=0, skip_utf8_validation=True, reconnect=0),
            name='polymarket-spot-receive', daemon=True)
        self.socket_thread.start()

    def ended(self, now):
        """The socket thread exited: pick the host and the delay of the next attempt."""
        with self.lock:
            self.generation += 1
            streamed, lived = self.last_frame is not None, now - self.opened_at
        self.socket = self.socket_thread = None
        if self.planned:  # Rotation ahead of the server cut: reconnect at once, same host.
            self.planned, self.attempts, self.retry_at, self.state = False, 0, now, 'rotating'
            self.rotations += 1
            return
        self.reconnects += 1
        if streamed and lived >= self.HEALTHY_S:
            self.attempts = 0  # Includes the 24 h cut if rotation was missed.
        elif not streamed:
            self.host_index = (self.host_index + 1) % len(STREAM_URLS)  # e.g. HTTP 451 on stream.binance.com
        self.attempts += 1
        low, high = self.BACKOFF_S
        self.retry_at = now + min(high, low * 2 ** (self.attempts - 1)) * self.jitter(.5, 1.5)
        self.state = 'backoff'
        _LOG.info('Binance spot WS reconnect host=%s attempt=%s', STREAM_URLS[self.host_index][0], self.attempts)

    def clock_offset(self):
        """Binance server minus local clock, from the lowest-RTT of three public /api/v3/time reads.

        Raw aggTrade lag mixes transport delay with local clock error (often 10–200 ms without NTP).
        The estimate is good to about half the round-trip asymmetry. Runs on the supervisor thread.
        """
        if self.session is None:
            self.session = requests.Session()
        best = None
        for host in REST_SOURCES:
            for _ in range(3):
                try:
                    sent = self.wall()
                    response = self.session.get(f'https://{host}/api/v3/time', timeout=(1, 2), allow_redirects=False)
                    response.raise_for_status()
                    server, done = response.json()['serverTime'], self.wall()
                    if type(server) is not int:
                        break
                    rtt = (done - sent) * 1000
                    if best is None or rtt < best[1]:
                        best = (server - (sent + done) * 500, rtt)
                except (requests.RequestException, ValueError, TypeError, KeyError):
                    break  # Next host; a dead network costs at most one timeout per host.
            if best:
                break
        if best:
            self.offset = (round(best[0], 1), round(best[1], 1))

    def tick(self):
        self.supervise(self.clock())
        now = self.clock()
        if self.aggtrade and (self.offset_at is None or now - self.offset_at >= self.OFFSET_EVERY_S):
            self.offset_at = now
            self.clock_offset()

    def supervise(self, now):
        if self.socket_thread is not None:
            if self.socket_thread.is_alive():
                with self.lock:
                    since = self.last_frame if self.last_frame is not None else self.opened_at
                if now - self.opened_at >= self.MAX_AGE_S:
                    self.planned = True
                    self.socket.close()
                elif now - since > self.STALE_S:
                    self.error = 'no frames for %ss' % self.STALE_S
                    self.socket.close()
                return
            self.ended(now)
        if not self.stop_event.is_set() and now >= self.retry_at:
            self.connect()

    def get(self, asset):
        with self.lock:
            entry = self.prices.get(asset)
            if not entry:
                return dict(UNAVAILABLE, age_ms=None)
            mono, wall, mid, host, generation = entry
            # Same connection still streaming: no push means no change. Never across a reconnect.
            if generation == self.generation and self.last_frame is not None and self.last_frame - mono <= self.STALE_S:
                mono, wall = self.last_frame, self.last_wall
        return {'price': str(mid), 'source': host, 'observed_at': datetime.fromtimestamp(wall, timezone.utc).isoformat(),
                'age_ms': max(0, round((self.clock() - mono) * 1000))}

    def metrics(self):
        now = self.clock()
        with self.lock:
            ages = {a: round((now - e[0]) * 1000) for a, e in self.prices.items()}  # Since each own update.
            lags, frames, last = sorted(self.lags), self.frames, self.last_frame
        state = self.state
        if state == 'connected' and last is not None and now - last <= self.STALE_S:
            state = 'streaming'
        lag, offset = None, self.offset
        if lags:
            # Exchange event -> local receive, shifted onto the server clock when an offset is known.
            shift = offset[0] if offset else 0
            lag = {k: round(lags[min(len(lags) - 1, int(q * len(lags)))] + shift, 1)
                   for k, q in (('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99), ('max', 1))}
            lag.update(n=len(lags), clock_corrected=offset is not None)
        return dict(spot_ws_state=state, spot_ws_host=STREAM_URLS[self.host_index][0], spot_ws_error=self.error,
                    spot_ws_frames=frames, spot_ws_frame_age_ms=round((now - last) * 1000) if last is not None else None,
                    spot_ws_connects=self.connects, spot_ws_reconnects=self.reconnects,
                    spot_ws_rotations=self.rotations, spot_ws_age_ms=ages, spot_ws_lag_ms=lag,
                    spot_ws_clock_offset_ms=offset[0] if offset else None,
                    spot_ws_clock_rtt_ms=offset[1] if offset else None)

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.tick()
            except Exception as exc:  # Keep supervising; the composite falls back meanwhile.
                self.error = str(exc)[:160]
                _LOG.exception('Binance spot WS supervisor failed')
            self.stop_event.wait(.5)

    def start(self):
        self.thread = threading.Thread(target=self.run, name='polymarket-spot-ws', daemon=True)
        self.thread.start()

    def close(self, timeout=5):
        deadline = time.monotonic() + timeout
        self.stop_event.set()
        if self.thread:  # Supervisor first, so it cannot open a socket after this point.
            self.thread.join(max(0, deadline - time.monotonic()))
        if self.socket:
            self.socket.close()
        if self.socket_thread:
            self.socket_thread.join(max(0, deadline - time.monotonic()))
        if self.session is not None:
            self.session.close()
        self.state = 'stopped'


class CompositeReferencePrices:
    """WebSocket mid while it is fresh, else the REST reference. Same get(asset) shape, plus age_ms."""
    def __init__(self, stream=None, rest=None, fresh_ms=None, wall=time.time):
        self.stream = stream or StreamingReferencePrices()
        self.rest = rest or ReferencePrices()
        self.fresh_ms = setting('SPOT_WS_FRESH_MS', 1000, 100, 10000) if fresh_ms is None else fresh_ms
        self.wall = wall
        self.lock = threading.Lock()
        self.served = Counter()

    def serve(self, kind, value):
        with self.lock:
            self.served[kind] += 1
        return value

    def get(self, asset):
        live = self.stream.get(asset)
        if live['price'] is not None and live['age_ms'] <= self.fresh_ms:
            return self.serve('ws', live)
        rest = self.rest.get(asset)
        if rest['price'] is not None:
            age = (self.wall() - datetime.fromisoformat(rest['observed_at']).timestamp()) * 1000
            return self.serve('rest', dict(rest, age_ms=max(0, round(age))))
        # REST down too: a stale socket value still beats nothing, within REST's own expiry.
        if live['price'] is not None and live['age_ms'] <= setting('REFERENCE_CACHE_SECONDS', 5, 2, 60) * 3000:
            return self.serve('ws_stale', live)
        return self.serve('unavailable', dict(UNAVAILABLE, age_ms=None))

    def metrics(self):
        with self.lock:
            served = dict(self.served)
        return dict(self.stream.metrics(), spot_provider='binance_ws+rest', spot_ws_fresh_ms=self.fresh_ms, spot_served=served)

    def start(self):
        self.rest.start()
        self.stream.start()

    def close(self):
        self.stream.close()
        self.rest.close()


def reference_provider():
    """REST reference by default; the WebSocket composite only behind SPOT_WS_ENABLED."""
    if flag('SPOT_WS_ENABLED'):
        return CompositeReferencePrices(StreamingReferencePrices(), ReferencePrices())
    return ReferencePrices()

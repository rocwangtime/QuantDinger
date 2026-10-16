"""Binance spot WebSocket reference: parsing, freshness fallback, reconnects. No network."""
import copy
import json
import math
import re
import threading
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
import requests

from app.services.polymarket import backtest, maker, observer, updown
from tests.test_polymarket_maker import NOW, books, market, setup

EPOCH = 1780000000.0


def frame(stream, **data):
    return json.dumps({'stream': stream, 'data': data})


def book_frame(asset='BTC', bid='60000.01', ask='60000.03'):
    return frame(f'{asset.lower()}usdt@bookTicker', u=1, s=asset + 'USDT', b=bid, B='1.5', a=ask, A='2')


def trade_frame(asset='BTC', event_ms=1780000000000, trade_ms=1779999999990):
    return frame(f'{asset.lower()}usdt@aggTrade', e='aggTrade', E=event_ms, s=asset + 'USDT', p='60000.02', q='.1', T=trade_ms, m=True)


def test_parse_stream_book_mid_and_trade_event_time():
    assert updown.parse_stream(book_frame()) == ('book', 'BTC', Decimal('60000.02'))
    assert updown.parse_stream(book_frame('XRP', '0.5123', '0.5124')) == ('book', 'XRP', Decimal('0.51235'))
    assert updown.parse_stream(trade_frame('ETH')) == ('trade', 'ETH', 1780000000000)
    no_event = json.loads(trade_frame())
    del no_event['data']['E']
    assert updown.parse_stream(json.dumps(no_event)) == ('trade', 'BTC', 1779999999990)


@pytest.mark.parametrize('raw', [
    'not json', '{}', '[]', json.dumps({'stream': 'btcusdt@bookTicker'}),
    frame('btcusdt@bookTicker', s='ETHUSDT', b='1', a='2'),  # Payload does not match the stream.
    frame('dogeusdt@bookTicker', s='DOGEUSDT', b='1', a='2'),
    frame('btcusdc@bookTicker', s='BTCUSDC', b='1', a='2'),
    book_frame(bid='60000.03', ask='60000.03'), book_frame(bid='60000.05', ask='60000.03'), book_frame(bid='0'),
    book_frame(bid='NaN'), frame('btcusdt@bookTicker', s='BTCUSDT', b=None, a='2'),
    frame('btcusdt@aggTrade', s='BTCUSDT', E=True), frame('btcusdt@aggTrade', s='BTCUSDT', E=1.5e12),
    frame('btcusdt@depth5', s='BTCUSDT', b='1', a='2'),
])
def test_parse_stream_rejects_malformed_frames(raw):
    assert updown.parse_stream(raw) is None


class Socket:
    """websocket.WebSocketApp stand-in: run_forever returns at once, the 'thread' lives until close."""
    def __init__(self, url, on_open=None, on_message=None, on_error=None):
        self.url, self.on_open, self.on_message, self.on_error = url, on_open, on_message, on_error
        self.running = self.closed = False
        self.kwargs = None

    def run_forever(self, **kwargs):
        self.kwargs, self.running = kwargs, True

    def close(self):
        self.closed, self.running = True, False

    def send(self, _):
        raise AssertionError('market-data streams never send')


class Thread:
    def __init__(self, target, name=None, daemon=None):
        self.target, self.name, self.socket = target, name, None

    def start(self):
        self.target()

    def is_alive(self):
        return bool(self.socket and self.socket.running)

    def join(self, timeout=None):
        pass


@pytest.fixture
def harness(monkeypatch):
    sockets = []

    def factory(url, **callbacks):
        sockets.append(Socket(url, **callbacks))
        return sockets[-1]

    class Bound(Thread):  # Each socket thread is bound to the socket created just before it.
        def __init__(self, target, name=None, daemon=None):
            super().__init__(target, name, daemon)
            self.socket = sockets[-1] if sockets else None

    monkeypatch.setattr(updown, 'threading', SimpleNamespace(Thread=Bound, Lock=threading.Lock, Event=threading.Event))
    clock = [1000.0]
    session = Session()
    stream = updown.StreamingReferencePrices(('BTC', 'ETH'), aggtrade=True, socket_factory=factory, session=session,
                                             clock=lambda: clock[0], wall=lambda: EPOCH + clock[0] - 1000,
                                             jitter=lambda low, high: 1.0)
    return SimpleNamespace(stream=stream, sockets=sockets, clock=clock, session=session)


class Session:
    """Binance /api/v3/time stand-in; offline unless `replies` is filled."""
    def __init__(self):
        self.replies, self.calls, self.closed = [], [], False

    def get(self, url, **kwargs):
        self.calls.append(url)
        if not self.replies:
            raise requests.ConnectionError('offline')
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: reply)

    def close(self):
        self.closed = True


def deliver(h, raw, socket=None):
    socket = socket or h.sockets[-1]
    socket.on_message(socket, raw)


def test_connect_subscribes_by_url_and_streams_mid_with_age(harness):
    h = harness
    h.stream.tick()
    sock = h.sockets[0]
    assert sock.url == ('wss://stream.binance.com:9443/stream?streams='
                        'btcusdt@bookTicker/ethusdt@bookTicker/btcusdt@aggTrade/ethusdt@aggTrade')
    assert sock.kwargs == dict(ping_interval=0, skip_utf8_validation=True, reconnect=0)
    sock.on_open(sock)
    assert h.stream.get('BTC') == dict(updown.UNAVAILABLE, age_ms=None)
    deliver(h, book_frame())
    h.clock[0] += .25
    assert h.stream.get('BTC') == {'price': '60000.02', 'source': 'stream.binance.com',
                                   'observed_at': datetime.fromtimestamp(EPOCH, timezone.utc).isoformat(), 'age_ms': 250}
    for lag in (10, 20, 30, 40, 400):
        deliver(h, trade_frame(event_ms=round((EPOCH + .25) * 1000) - lag))
    metrics = h.stream.metrics()
    assert metrics['spot_ws_state'] == 'streaming' and metrics['spot_ws_frames'] == 6
    assert metrics['spot_ws_age_ms'] == {'BTC': 250}
    assert metrics['spot_ws_lag_ms'] == dict(p50=30, p90=400, p95=400, p99=400, max=400, n=5, clock_corrected=False)
    assert metrics['spot_ws_clock_offset_ms'] is None
    assert h.stream.get('ETH')['price'] is None


def test_unchanged_asset_is_confirmed_by_later_frames_on_the_same_connection_only(harness):
    h = harness
    h.stream.tick()
    deliver(h, book_frame('BTC'))
    h.clock[0] += .6
    deliver(h, book_frame('ETH', '2400.1', '2400.2'))  # BTC quiet: its mid still holds as of this frame.
    h.clock[0] += .1
    btc = h.stream.get('BTC')
    assert btc['price'] == '60000.02' and btc['age_ms'] == 100
    assert btc['observed_at'] == datetime.fromtimestamp(EPOCH + .6, timezone.utc).isoformat()
    assert h.stream.metrics()['spot_ws_age_ms'] == {'BTC': 700, 'ETH': 100}
    h.clock[0] = 1000 + h.stream.STALE_S
    deliver(h, trade_frame('ETH'))
    assert h.stream.get('BTC')['age_ms'] == 0  # Own update exactly STALE_S old: still confirmed.
    h.clock[0] += .1
    deliver(h, trade_frame('ETH'))
    assert h.stream.get('BTC')['age_ms'] == round((h.stream.STALE_S + .1) * 1000)  # Too quiet: own age.
    # A reconnect never vouches for a price received before it.
    h.sockets[0].close()
    h.stream.tick()
    h.clock[0] = h.stream.retry_at
    h.stream.tick()
    deliver(h, book_frame('ETH', '2400.3', '2400.4'))
    assert h.stream.get('ETH')['age_ms'] == 0
    assert h.stream.get('BTC')['age_ms'] > h.stream.STALE_S * 1000


def test_clock_offset_corrects_lag_using_the_tightest_server_time_probe(harness):
    h = harness
    walls = iter([9.0, 10.0, 10.2, 11.0, 11.05, 12.0, 12.3])  # Refused, then round trips of 200, 50, 300 ms.
    h.stream.wall = lambda: next(walls)
    # Offsets 200, 183 and 250 ms: only the tightest round trip is trusted.
    h.session.replies = [requests.HTTPError('451'), dict(serverTime=10300), dict(serverTime=11208), dict(serverTime=12400)]
    h.stream.clock_offset()
    # api.binance.com refused, so the three probes went to data-api.binance.vision; 50 ms RTT wins.
    assert [c.split('/')[2] for c in h.session.calls] == ['api.binance.com'] + ['data-api.binance.vision'] * 3
    assert h.stream.offset == (183.0, 50.0)
    h.stream.lags.extend([-40, -30, 10])
    metrics = h.stream.metrics()
    assert metrics['spot_ws_lag_ms'] == dict(p50=153, p90=193, p95=193, p99=193, max=193, n=3, clock_corrected=True)
    assert (metrics['spot_ws_clock_offset_ms'], metrics['spot_ws_clock_rtt_ms']) == (183.0, 50.0)
    h.session.replies, h.session.calls, h.stream.wall = [], [], lambda: 20.0
    h.stream.clock_offset()  # Network down: one attempt per host, previous estimate kept.
    assert len(h.session.calls) == 2 and h.stream.offset == (183.0, 50.0)


def test_supervisor_probes_the_clock_after_socket_work_and_every_ten_minutes(harness):
    h = harness
    h.stream.tick()
    assert len(h.sockets) == 1 and len(h.session.calls) == 2
    h.clock[0] += h.stream.OFFSET_EVERY_S - 1
    deliver(h, book_frame())
    h.stream.tick()
    assert len(h.session.calls) == 2
    h.clock[0] += 1
    h.stream.tick()
    assert len(h.session.calls) == 4
    h.stream.aggtrade = False
    h.clock[0] += h.stream.OFFSET_EVERY_S
    deliver(h, book_frame())
    h.stream.tick()
    assert len(h.session.calls) == 4  # Lag metric off: no clock probes.


def test_failed_handshake_switches_host_with_backoff_and_ignores_old_frames(harness):
    h = harness
    h.stream.tick()
    first = h.sockets[0]
    first.on_error(first, 'Handshake status 451')
    first.close()  # run_forever returns after the failed handshake.
    h.stream.tick()
    assert h.stream.state == 'backoff' and h.stream.host_index == 1 and h.stream.retry_at == 1001
    assert h.stream.metrics()['spot_ws_error'] == 'Handshake status 451'
    h.stream.tick()
    assert len(h.sockets) == 1  # Still backing off.
    h.clock[0] = 1001
    h.stream.tick()
    second = h.sockets[1]
    assert second.url.startswith('wss://data-stream.binance.vision/stream?streams=btcusdt@bookTicker/')
    deliver(h, book_frame(bid='1', ask='3'), first)  # Late frame from the dead socket.
    assert h.stream.get('BTC')['price'] is None
    deliver(h, book_frame())
    assert h.stream.get('BTC')['source'] == 'data-stream.binance.vision'
    # Short-lived after streaming: same host, growing backoff.
    second.close()
    h.clock[0] += 5
    h.stream.tick()
    assert h.stream.host_index == 1 and h.stream.attempts == 2 and h.stream.retry_at == h.clock[0] + 2
    for _ in range(10):  # Repeated failures never wait more than the cap.
        h.clock[0] = h.stream.retry_at
        h.stream.tick()
        h.sockets[-1].close()
        h.stream.tick()
    assert h.stream.retry_at - h.clock[0] == 60
    assert h.stream.metrics()['spot_ws_reconnects'] == 12


def test_healthy_drop_and_server_cut_reconnect_quickly_on_same_host(harness):
    h = harness
    h.stream.tick()
    for _ in range(130):
        h.clock[0] += .5
        deliver(h, book_frame())
        h.stream.tick()
    h.sockets[0].close()  # e.g. the 24 h server disconnect when rotation is disabled.
    h.stream.tick()
    assert h.stream.host_index == 0 and h.stream.attempts == 1 and h.stream.retry_at == h.clock[0] + 1


def test_silent_socket_is_closed_and_replaced(harness):
    h = harness
    h.stream.tick()
    deliver(h, book_frame())
    h.clock[0] += h.stream.STALE_S
    h.stream.tick()
    assert not h.sockets[0].closed
    h.clock[0] += .5
    h.stream.tick()
    assert h.sockets[0].closed and 'no frames' in h.stream.error
    h.stream.tick()
    h.clock[0] += 1
    h.stream.tick()
    assert len(h.sockets) == 2 and h.sockets[1].url.startswith('wss://stream.binance.com:9443/')


def test_planned_rotation_before_the_24h_cut_reconnects_immediately(harness):
    h = harness
    h.stream.tick()
    deliver(h, book_frame())
    h.clock[0] += h.stream.MAX_AGE_S - 1
    deliver(h, book_frame())
    h.stream.tick()
    assert not h.sockets[0].closed
    h.clock[0] += 1
    deliver(h, book_frame())
    h.stream.tick()
    assert h.sockets[0].closed
    h.stream.tick()
    assert len(h.sockets) == 2 and h.sockets[1].url == h.sockets[0].url
    metrics = h.stream.metrics()
    assert (metrics['spot_ws_rotations'], metrics['spot_ws_reconnects'], metrics['spot_ws_connects']) == (1, 0, 2)


def test_close_stops_supervision_and_late_open_closes(harness):
    h = harness
    h.stream.tick()
    h.stream.close()
    assert h.sockets[0].closed and h.stream.state == 'stopped' and h.session.closed
    h.clock[0] += 100
    h.stream.tick()
    assert len(h.sockets) == 1
    late = Socket('x')
    h.sockets[0].on_open(late)
    assert late.closed


class Rest:
    def __init__(self):
        self.value = dict(updown.UNAVAILABLE)
        self.started = self.closed = False

    def get(self, asset):
        return dict(self.value)

    def start(self):
        self.started = True

    def close(self):
        self.closed = True


def test_composite_serves_fresh_ws_then_rest_then_bounded_stale_ws(harness, monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_REFERENCE_CACHE_SECONDS', raising=False)
    h, rest = harness, Rest()
    composite = updown.CompositeReferencePrices(h.stream, rest, fresh_ms=1000, wall=lambda: EPOCH + 10)
    assert composite.get('BTC') == dict(updown.UNAVAILABLE, age_ms=None)
    h.stream.tick()
    deliver(h, book_frame())
    h.clock[0] += 1
    assert composite.get('BTC')['source'] == 'stream.binance.com'  # Exactly 1000 ms is still fresh.
    h.clock[0] += .001
    rest.value = {'price': '60001', 'source': 'data-api.binance.vision',
                  'observed_at': datetime.fromtimestamp(EPOCH + 7.5, timezone.utc).isoformat()}
    assert composite.get('BTC') == dict(rest.value, age_ms=2500)
    rest.value = dict(updown.UNAVAILABLE)
    h.clock[0] = 1014
    assert composite.get('BTC')['source'] == 'stream.binance.com'  # REST down: stale WS, within 15 s.
    h.clock[0] = 1015.5
    assert composite.get('BTC')['price'] is None
    metrics = composite.metrics()
    assert metrics['spot_served'] == dict(ws=1, rest=1, ws_stale=1, unavailable=2)
    assert metrics['spot_provider'] == 'binance_ws+rest' and metrics['spot_ws_fresh_ms'] == 1000
    composite.close()
    assert rest.closed and h.stream.state == 'stopped'


def test_flag_selects_provider_and_observer_reports_metrics(monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_SPOT_WS_ENABLED', raising=False)
    assert type(updown.reference_provider()) is updown.ReferencePrices
    monkeypatch.setenv('POLYMARKET_OBSERVER_SPOT_WS_ENABLED', 'true')
    monkeypatch.setenv('POLYMARKET_OBSERVER_UPDOWN_ASSETS', 'ETH,BTC')
    monkeypatch.setenv('POLYMARKET_OBSERVER_SPOT_WS_AGGTRADE', 'false')
    provider = updown.reference_provider()
    assert type(provider) is updown.CompositeReferencePrices and provider.fresh_ms == 1000
    assert provider.stream.url() == 'wss://stream.binance.com:9443/stream?streams=ethusdt@bookTicker/btcusdt@bookTicker'
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, '_REFERENCE', provider)
    assert observer.snapshot()['spot_ws_state'] == 'stopped'
    monkeypatch.setattr(observer, '_REFERENCE', updown.ReferencePrices())
    assert 'spot_ws_state' not in observer.snapshot()


def test_new_sources_fit_the_sample_column_and_every_proxy_accepts_them():
    assert updown.BINANCE_SOURCES == backtest.BINANCE_SOURCES
    assert all(len(source) <= 40 for source in updown.BINANCE_SOURCES)  # reference_source VARCHAR(40).
    import inspect
    sql = re.search(r"reference_source IN \(([^)]*)\)", inspect.getsource(maker.RecordedFeed.outcome)).group(1)
    assert tuple(x.strip(" '") for x in sql.split(',')) == updown.BINANCE_SOURCES


class Cursor:
    """Applies the outcome query's market and source filters to in-memory samples."""
    def __init__(self, samples):
        self.samples, self.result = samples, None

    def execute(self, sql, params):
        if 'observer_windows' in sql:
            self.result = None
            return
        allowed = re.search(r"reference_source IN \(([^)]*)\)", sql).group(1)
        allowed = {x.strip(" '") for x in allowed.split(',')}
        identity, boundary = params[0], datetime.fromisoformat(params[1])
        rows = [s for s in self.samples if s['market_id'] == identity and s['reference_source'] in allowed
                and 0 <= (boundary - s['reference_observed_at']).total_seconds() <= 10]
        self.result = max(rows, key=lambda s: s['reference_observed_at']) if rows else None

    def fetchone(self):
        return self.result


@pytest.mark.parametrize('source', ['stream.binance.com', 'data-stream.binance.vision'])
def test_proxy_settlement_accepts_websocket_samples(source):
    mk = market()
    start, end = (datetime.fromisoformat(mk[k]) for k in ('eventStartTime', 'endDate'))
    row = dict(market=mk, spec=dict(tie=0))
    samples = [dict(market_id='42', reference_source=source, reference_observed_at=start, reference_price='60000'),
               dict(market_id='42', reference_source='api.binance.com', reference_observed_at=end, reference_price='59999')]
    assert maker.RecordedFeed.outcome(Cursor(samples), row) == ('Down', True)
    samples[0]['reference_source'] = 'unavailable'
    assert maker.RecordedFeed.outcome(Cursor(samples), row) == (None, False)
    at = start.replace(tzinfo=timezone.utc)
    rows = [dict(observed_at=at, reference_observed_at=at, reference_price='60000.02', reference_source=source)]
    assert backtest.proxy(rows, at) == Decimal('60000.02')


def test_spot_samples_are_time_bounded_so_4hz_still_covers_the_strike_window():
    """15m Chainlink market 110 s after open, first stepped now: the start window needs 180 s of 4 Hz spot."""
    start = NOW - 110000
    mk = market('15', start=start, duration=900000, interval='15m')
    _, ws = setup(dict(spot_stale_ms='1000'))
    for at in range(start - 70000, NOW - 2500 + 1, 250):
        ws.observe_spot('BTC', '60000', at, at)
    rest = copy.deepcopy(ws)
    assert len(ws.state['assets']['BTC']['samples']) > 400
    assert ws.state['assets']['BTC']['samples'][0][0] >= (NOW - 2500) / 1000 - 180
    ws.observe_spot('BTC', '60000', NOW - 100, NOW - 100)
    ws.step(mk, books(NOW), NOW)
    fresh = ws.state['markets']['15']['decision']
    assert ws.state['markets']['15']['phase'] == 'OPEN'
    assert ws.state['markets']['15']['a_start'] == pytest.approx(math.log(60000))
    # Same history, spot 2.5 s old (the REST cadence): calm at the 1 s guard, larger Z at the 6 s one.
    rest.step(mk, books(NOW), NOW)
    assert rest.state['markets']['15']['decision']['reason'] == 'spot_stale'
    rest.p.spot_stale_ms = 6000
    rest.step(mk, books(NOW + 1), NOW + 1)
    aged = rest.state['markets']['15']['decision']
    assert aged['Z'] / fresh['Z'] == pytest.approx(math.sqrt((.3 + 2.501) / (.3 + .1)), rel=.01)

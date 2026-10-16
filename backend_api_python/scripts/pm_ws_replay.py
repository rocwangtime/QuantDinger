"""Offline end-to-end replay of the Polymarket market channel against the observer.

A separate process serves a minimal WebSocket feed on 127.0.0.1 that mimics the
public CLOB market channel: subscribe/unsubscribe by asset id, a book snapshot per
newly subscribed asset, PING/PONG, and Poisson price_change/book/last_trade_price
frames at per-asset rates taken from a measured distribution file. Up/Down series
roll over on an accelerated clock: the current window expires, the next window
becomes hot and a new next window appears, so asset ids change exactly as in
production. The server closes a connection with "slow consumer" when its unsent
backlog exceeds --slow-bytes, a model of a bounded publisher queue. The actual public server
queue capacity and implementation are not documented.

The client side is the real observer: MarketConnections, websocket-client,
receive queues, shard workers, Pipeline (recording on, database stubbed) and the
evaluator, all in one process so they share one GIL as in production. Nothing
connects to Polymarket, a wallet or a database.

    .venv/bin/python scripts/pm_ws_replay.py --rates deploy/pm_ws_asset_rates_20261016.json
    .venv/bin/python scripts/pm_ws_replay.py --synthetic --seconds 180 --scale 2

Prints one JSON report: per-shard server-side msg/s, max/min ratio, slow-consumer
closes, reconnects, rebalances and book freshness p95 sampled once per second.
"""
import argparse
from collections import deque
import asyncio
import base64
import hashlib
import json
import math
import multiprocessing
import os
import random
import socket
import ssl
import struct
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERVAL_SECONDS = {'5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}


# --------------------------------------------------------------------------- input

def synthetic_rates():
    """Distribution matching the 2026-10-16 production description.

    28 markets / 56 assets, about 1,400 asset-msg/s in total. The hottest market
    carries ~367 msg/s over both sides and the three hottest ~80% of the total.
    Up/Down series have a current and a next window; YES/NO markets are static.
    """
    series = {('BTC', '5m'): (367, 9), ('BTC', '15m'): (362, 6), ('ETH', '5m'): (352, 5),
              ('ETH', '15m'): (70, 3), ('SOL', '5m'): (38, 2), ('SOL', '15m'): (22, 2),
              ('XRP', '5m'): (26, 2), ('XRP', '15m'): (14, 1)}
    markets = [dict(series=list(key), role=role, rate=rate)
               for key, (current, upcoming) in series.items()
               for role, rate in (('current', current), ('next', upcoming))]
    yes_no = [24, 16, 12, 9, 7, 6, 5, 4, 3, 2, 1.5, 1]
    markets += [dict(id=f'yn{i}', rate=rate) for i, rate in enumerate(yes_no)]
    return dict(source='synthetic', markets=markets)


def load_rates(path):
    """Accept the measured file in the shapes the box tooling produces.

    Supported: {"markets": [{id|slug|series, role?, rate | yes_rate/no_rate}]},
    {"assets": [{asset_id, rate|msg_per_sec, market|market_id|condition_id, slug?}]},
    a bare list of either, or {asset_id: rate}. Up/Down slugs such as
    "btc-updown-5m-1760600400" become rolling series; ordering by the slug's
    window timestamp marks the earliest window current and later ones next.
    """
    with open(path) as handle:
        data = json.load(handle)
    rows = data.get('markets') or data.get('assets') or data.get('rates') if isinstance(data, dict) else data
    if isinstance(data, dict) and rows is None:
        rows = [dict(asset_id=k, rate=v) for k, v in data.items() if isinstance(v, (int, float))]
    if isinstance(rows, dict):
        rows = [dict(asset_id=k, rate=v) if isinstance(v, (int, float)) else dict(asset_id=k, **v)
                for k, v in rows.items()]

    def rate(row):
        for name in ('rate', 'msg_per_s', 'msg_per_sec', 'msgs_per_sec', 'msg_s', 'rate_per_sec', 'per_second', 'mps'):
            if row.get(name) is not None:
                return float(row[name])
        if row.get('count') is not None and row.get('seconds'):
            return float(row['count']) / float(row['seconds'])
        raise ValueError(f'no rate field in {row}')

    def series(slug):
        parts = str(slug or '').lower().split('-')
        if len(parts) >= 4 and parts[1] in ('updown', 'up') and parts[-2] in INTERVAL_SECONDS:
            return parts[0].upper(), parts[-2], int(parts[-1]) if parts[-1].isdigit() else 0
        return None

    grouped = {}
    for row in rows:
        if 'yes_rate' in row or 'no_rate' in row or row.get('asset_id') is None and 'rate' in row:
            key = row.get('id') or row.get('slug') or row.get('market') or str(len(grouped))
            grouped[key] = dict(slug=row.get('slug'), series=row.get('series'), role=row.get('role'),
                                rates=[float(row.get('yes_rate', row.get('rate', 0)) or 0),
                                       float(row.get('no_rate', 0) or 0)])
            continue
        key = (row.get('market') or row.get('market_id') or row.get('condition_id')
               or row.get('conditionId') or row.get('slug') or row.get('asset_id'))
        entry = grouped.setdefault(key, dict(slug=row.get('slug'), series=row.get('series'),
                                             role=row.get('role'), rates=[]))
        entry['rates'].append(rate(row))
    markets, windows = [], {}
    for key, entry in grouped.items():
        if not all(math.isfinite(v) and v >= 0 for v in entry['rates']):
            raise ValueError(f'invalid rate for market {key}')
        total = sum(entry['rates'])
        parsed = series(entry['slug'])
        if entry['series']:
            markets.append(dict(series=list(entry['series'])[:2], role=entry['role'] or 'current', rate=total))
        elif parsed:
            windows.setdefault(parsed[:2], []).append((parsed[2], total))
        else:
            markets.append(dict(id=str(key)[:40], rate=total))
    for key, items in windows.items():
        for index, (_, total) in enumerate(sorted(items)):
            markets.append(dict(series=list(key), role='current' if index == 0 else 'next', rate=total))
    return dict(source=os.path.basename(path), markets=markets)


def token(*parts):
    digest = hashlib.sha256('|'.join(map(str, parts)).encode()).hexdigest()
    return str(int(digest, 16))[:77]


class Schedule:
    """Shared, deterministic market/asset layout for server and client."""
    def __init__(self, spec, t0, scale):
        self.t0, self.scale = t0, scale
        self.static = [m for m in spec['markets'] if 'series' not in m]
        self.series = {}
        for m in spec['markets']:
            if 'series' in m:
                self.series.setdefault(tuple(m['series']), {})[m.get('role', 'current')] = m['rate']

    def period(self, interval):
        return INTERVAL_SECONDS[interval] / self.scale

    def window(self, interval, now):
        return math.floor((now - self.t0) / self.period(interval))

    def traffic(self, now):
        """{(yes, no): (yes_rate, no_rate)} for every asset receiving flow now."""
        result = {}
        for m in self.static:
            result[(token('yn', m['id'], 'y'), token('yn', m['id'], 'n'))] = (m['rate'] / 2, m['rate'] / 2)
        for (asset, interval), roles in self.series.items():
            w = self.window(interval, now)
            for offset, role in ((0, 'current'), (1, 'next')):
                rate = roles.get(role, 0)
                result[(token(asset, interval, w + offset, 'y'),
                        token(asset, interval, w + offset, 'n'))] = (rate / 2, rate / 2)
        return result

    def markets(self, now):
        """Catalogue the observer would discover at monotonic time ``now``."""
        wall = datetime.now(timezone.utc)
        result = []
        for m in self.static:
            result.append(dict(id=f"yn-{m['id']}", conditionId=f"0x{m['id']}", slug=m['id'], question=m['id'],
                               yesAssetId=token('yn', m['id'], 'y'), noAssetId=token('yn', m['id'], 'n'),
                               feeRate='0', endDate=(wall + timedelta(days=30)).isoformat(),
                               market_type='yes_no'))
        for (asset, interval) in self.series:
            w = self.window(interval, now)
            for offset in (0, 1):
                start = self.t0 + (w + offset) * self.period(interval)
                begins = wall + timedelta(seconds=(start - now) * self.scale)
                ends = begins + timedelta(seconds=INTERVAL_SECONDS[interval])
                ident = f'{asset}-{interval}-{w + offset}'
                result.append(dict(id=ident, conditionId='0x' + ident, slug=ident, question=ident,
                                   yesAssetId=token(asset, interval, w + offset, 'y'),
                                   noAssetId=token(asset, interval, w + offset, 'n'),
                                   feeRate='0.07', endDate=ends.isoformat(), eventStartTime=begins.isoformat(),
                                   market_type='up_down', asset=asset, interval=interval, price_to_beat=None))
        return result


def prior_rates(schedule):
    """Group rates a previous run would have persisted (series and markets)."""
    rates = {f"market:yn-{m['id']}": m['rate'] for m in schedule.static}
    for (asset, interval), roles in schedule.series.items():
        rates[f'up_down:{asset}:{interval}'] = sum(roles.values())
    return rates


# --------------------------------------------------------------------------- server

def frame(payload):
    data = payload.encode() if isinstance(payload, str) else payload
    size = len(data)
    if size < 126:
        head = struct.pack('!BB', 0x81, size)
    elif size < 65536:
        head = struct.pack('!BBH', 0x81, 126, size)
    else:
        head = struct.pack('!BBQ', 0x81, 127, size)
    return head + data


def close_frame(code, reason):
    data = struct.pack('!H', code) + reason.encode()
    return struct.pack('!BB', 0x88, len(data)) + data


async def read_frame(reader):
    first, second = await reader.readexactly(2)
    size = second & 0x7f
    if size == 126:
        size = struct.unpack('!H', await reader.readexactly(2))[0]
    elif size == 127:
        size = struct.unpack('!Q', await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if second & 0x80 else b'\0\0\0\0'
    data = bytearray(await reader.readexactly(size))
    for i in range(size):
        data[i] ^= mask[i % 4]
    return first & 0x0f, bytes(data)


def book_payload(asset, now_ms, rng, levels=40):
    mid = rng.randint(20, 80)
    bids = [dict(price=f'0.{max(1, mid - i):02d}', size=f'{rng.uniform(5, 900):.2f}') for i in range(1, levels)]
    asks = [dict(price=f'0.{min(99, mid + i):02d}', size=f'{rng.uniform(5, 900):.2f}') for i in range(0, levels - 1)]
    return dict(event_type='book', asset_id=asset, market='0x' + asset[:64], bids=bids, asks=asks,
                timestamp=str(now_ms), hash=hashlib.sha1(f'{asset}{now_ms}'.encode()).hexdigest())


def serve(port, spec, t0, scale, slow_bytes, book_share, pair_share, seed, report, stop_at,
          bandwidth_kib=0, slow_frames=256, tls_cert=None, tls_key=None):
    rng = random.Random(seed)
    schedule = Schedule(spec, t0, scale)
    clients = {}
    stats = dict(slow_consumer=0, closes=0, connections=0, sent={}, offered={}, wire_bytes={},
                 max_buffer={}, max_queue_frames=0, subscribed={})

    async def finish_close(writer):
        try:
            await asyncio.wait_for(writer.drain(), .2)
        except (TimeoutError, ConnectionError, OSError):
            pass
        finally:
            writer.close()

    def enqueue(state, data, counted=True):
        if state['closed']:
            return
        key = state['shard']
        if counted:
            stats['offered'][key] = stats['offered'].get(key, 0) + 1
        buffered = state['queued_bytes'] + state['writer'].transport.get_write_buffer_size()
        if buffered + len(data) > slow_bytes or len(state['outbox']) >= slow_frames:
            stats['slow_consumer'] += 1
            print(f'close at={time.monotonic() - t0:.3f}s shard={key} queued={len(state["outbox"])} bytes={buffered}', file=sys.stderr)
            state['closed'] = True
            state['outbox'].clear()
            state['writer'].write(close_frame(1013, 'slow consumer: send buffer full'))
            asyncio.create_task(finish_close(state['writer']))
            return
        if not bandwidth_kib:
            state['writer'].write(data)
            stats['wire_bytes'][key] = stats['wire_bytes'].get(key, 0) + len(data)
            if counted:
                stats['sent'][key] = stats['sent'].get(key, 0) + 1
            stats['max_buffer'][key] = max(stats['max_buffer'].get(key, 0), buffered + len(data))
            return
        state['outbox'].append((data, counted))
        state['queued_bytes'] += len(data)
        stats['max_queue_frames'] = max(stats['max_queue_frames'], len(state['outbox']))
        stats['max_buffer'][key] = max(stats['max_buffer'].get(key, 0), buffered + len(data))

    async def handle(reader, writer):
        sock = writer.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
        request = await reader.readuntil(b'\r\n\r\n')
        key = next(line.split(b':', 1)[1].strip() for line in request.split(b'\r\n')
                   if line.lower().startswith(b'sec-websocket-key'))
        accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
        writer.write(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                     b'Sec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
        ident = stats['connections']
        stats['connections'] += 1
        path = request.split(b' ', 2)[1].decode()
        shard = path.split('shard=', 1)[1].split('&')[0] if 'shard=' in path else str(ident)
        state = dict(writer=writer, assets=set(), id=ident, shard=shard, closed=False,
                     outbox=deque(), queued_bytes=0, credit=0)
        clients[ident] = state
        stats['sent'].setdefault(shard, 0)
        stats['wire_bytes'].setdefault(shard, 0)
        try:
            while True:
                opcode, data = await read_frame(reader)
                if opcode == 8:
                    break
                if opcode != 1:
                    continue
                if data == b'PING':
                    enqueue(state, frame('PONG'), False)
                    continue
                command = json.loads(data)
                assets = [str(a) for a in command.get('assets_ids', [])]
                if command.get('operation') == 'unsubscribe':
                    state['assets'].difference_update(assets)
                    continue
                now_ms = int(time.time() * 1000)
                for asset in assets:
                    if asset not in state['assets']:
                        state['assets'].add(asset)
                        enqueue(state, frame(json.dumps(book_payload(asset, now_ms, rng))), False)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            state['closed'] = True
            clients.pop(ident, None)
            writer.close()

    async def pump():
        tick = .01
        last = time.monotonic()
        while time.monotonic() < stop_at:
            await asyncio.sleep(tick)
            now = time.monotonic()
            dt, last = now - last, now
            traffic = schedule.traffic(now)
            owners = {}
            for state in list(clients.values()):
                for asset in state['assets']:
                    owners.setdefault(asset, []).append(state)
            now_ms = int(time.time() * 1000)
            for (yes, no), rates in traffic.items():
                for asset, other, rate in ((yes, no, rates[0]), (no, yes, rates[1])):
                    targets = owners.get(asset)
                    if not targets or rate <= 0:
                        continue
                    count = poisson(rng, rate * dt)
                    for _ in range(count):
                        roll = rng.random()
                        if roll < book_share:
                            payload = book_payload(asset, now_ms, rng)
                        elif roll < book_share + .04:
                            payload = dict(event_type='last_trade_price', asset_id=asset, market='0x' + asset[:64],
                                           price=f'0.{rng.randint(1, 99):02d}', size=f'{rng.uniform(1, 500):.2f}',
                                           side=rng.choice(('BUY', 'SELL')), fee_rate_bps='0', timestamp=str(now_ms))
                        else:
                            price = rng.randint(1, 99)
                            changes = [dict(asset_id=asset, price=f'0.{price:02d}', size=f'{rng.uniform(0, 900):.2f}',
                                            side=rng.choice(('BUY', 'SELL')),
                                            hash=hashlib.sha1(os.urandom(8)).hexdigest(),
                                            best_bid=f'0.{max(1, price - 1):02d}', best_ask=f'0.{price:02d}')]
                            if rng.random() < pair_share:
                                changes.append(dict(changes[0], asset_id=other, price=f'0.{100 - price:02d}'))
                            payload = dict(market='0x' + asset[:64], price_changes=changes,
                                           timestamp=str(now_ms), event_type='price_change')
                        data = frame(json.dumps(payload))
                        for state in targets:
                            if state['closed']:
                                continue
                            enqueue(state, data)
            for state in list(clients.values()):
                if state['closed']:
                    continue
                key = state['shard']
                state['credit'] = (min(65536, state['credit'] + bandwidth_kib * 1024 * dt)
                                   if bandwidth_kib else float('inf'))
                while state['outbox'] and state['credit'] >= len(state['outbox'][0][0]):
                    data, counted = state['outbox'].popleft()
                    state['queued_bytes'] -= len(data)
                    state['credit'] -= len(data)
                    state['writer'].write(data)
                    stats['wire_bytes'][key] = stats['wire_bytes'].get(key, 0) + len(data)
                    if counted:
                        stats['sent'][key] = stats['sent'].get(key, 0) + 1
                buffered = state['queued_bytes'] + state['writer'].transport.get_write_buffer_size()
                stats['max_buffer'][key] = max(stats['max_buffer'].get(key, 0), buffered)
                stats['subscribed'][key] = len(state['assets'])
            if now - stats.get('reported', 0) < .5:
                continue
            stats['reported'] = now
            report.put(('tick', dict(sent=dict(stats['sent']), slow_consumer=stats['slow_consumer'],
                                     max_buffer=dict(stats['max_buffer']), connections=stats['connections'],
                                     wire_bytes=dict(stats['wire_bytes']), offered=dict(stats['offered']),
                                     max_queue_frames=stats['max_queue_frames'])))

    async def main():
        context = None
        if tls_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(tls_cert, tls_key)
        server = await asyncio.start_server(handle, '127.0.0.1', port, ssl=context)
        report.put(('ready', port))
        async with server:
            await pump()

    asyncio.run(main())


def poisson(rng, mean):
    if mean <= 0:
        return 0
    if mean > 30:
        return max(0, int(rng.gauss(mean, math.sqrt(mean)) + .5))
    limit, k, p = math.exp(-mean), 0, rng.random()
    while p > limit:
        k += 1
        p *= rng.random()
    return k


# --------------------------------------------------------------------------- client

class NullDb:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.count = len(params or ()) and 1

    def executemany(self, sql, rows):
        pass

    def fetchone(self):
        return {'count': 1}

    def commit(self):
        pass

    def close(self):
        pass


def run(args):
    sys.path.insert(0, args.code or ROOT)
    os.environ.setdefault('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'true')
    os.environ.setdefault('POLYMARKET_OBSERVER_RECORDING_ENABLED', 'true')
    os.environ['POLYMARKET_PAPER_ACCOUNT_ENABLED'] = 'false'
    os.environ['POLYMARKET_OBSERVER_WS_SHARDS'] = str(args.shards)
    scheme = 'wss' if args.tls_cert else 'ws'
    os.environ['POLYMARKET_OBSERVER_WS_URL'] = f'{scheme}://127.0.0.1:{args.port}'
    spec = load_rates(args.rates) if args.rates else synthetic_rates()
    for m in spec['markets']:
        m['rate'] *= args.scale
    import websocket
    from app.services.polymarket import observer, recording, ws_balance
    original = websocket.WebSocketApp

    class Local(original):
        def __init__(self, url, **kwargs):
            # Tag the socket with its shard so the server can attribute traffic;
            # the baseline code hard-codes the public URL, so it is replaced here.
            index = sys._getframe(1).f_locals.get('index')
            super().__init__(f"{os.environ['POLYMARKET_OBSERVER_WS_URL']}/?shard={index}", **kwargs)

        def run_forever(self, **kwargs):
            if args.tls_cert:
                kwargs['sslopt'] = {'cert_reqs': ssl.CERT_NONE, 'check_hostname': False}
            return super().run_forever(**kwargs)
    websocket.WebSocketApp = Local
    observer.get_db_connection = NullDb
    ws_balance.get_db_connection = recording.get_db_connection = NullDb
    recording.References.run = lambda self: None

    def offline_http(*args, **kwargs):
        raise AssertionError('HTTP is disabled during offline replay')
    observer.PublicClient._json = offline_http
    def busy():
        # Other CPU-bound threads (paper account, references, evaluator spikes)
        # contend for the same GIL as the socket readers in production.
        while True:
            sum(i * i for i in range(2000))
    if args.switch_interval:
        sys.setswitchinterval(args.switch_interval)
    elif hasattr(observer, 'tune_switch_interval'):
        observer.tune_switch_interval()  # As the production observer process does.
    for _ in range(args.gil_threads):
        threading.Thread(target=busy, daemon=True).start()
    t0 = time.monotonic()
    stop_at = t0 + args.seconds + 5
    ctx = multiprocessing.get_context('spawn')
    report = ctx.Queue()
    server = ctx.Process(target=serve, args=(args.port, spec, t0, args.time_scale, args.slow_bytes,
                                             args.book_share, args.pair_share, args.seed, report, stop_at,
                                             args.bandwidth_kib, args.slow_frames, args.tls_cert, args.tls_key),
                         daemon=True)
    server.start()
    assert report.get(timeout=20)[0] == 'ready'
    schedule = Schedule(spec, t0, args.time_scale)
    pipeline = observer.Pipeline()
    observer._PIPELINE = pipeline
    pipeline.start()
    markets = schedule.markets(time.monotonic())
    pipeline.set_markets(markets)
    connections = observer.MarketConnections()
    observer._WS = connections
    if args.prior and hasattr(connections, 'seed_rates'):
        connections.seed_rates(prior_rates(schedule))
    connections.sync(markets)
    refresh = time.monotonic() + args.refresh / args.time_scale
    ages, lags, samples, last = [], [], [], None
    peak_drops = peak_cpu = peak_callback = peak_tcp = 0
    peaks, moves = [], []
    warm = t0 + args.warmup
    while time.monotonic() < t0 + args.seconds:
        time.sleep(1)
        now = time.monotonic()
        if now >= refresh:
            markets = schedule.markets(now)
            pipeline.set_markets(markets)
            connections.sync(markets)
            refresh = now + args.refresh / args.time_scale
        connections.tick()
        while not report.empty():
            kind, payload = report.get_nowait()
            if kind == 'tick':
                last = payload
        if now < warm:
            continue
        metrics = pipeline.metrics()
        if metrics.get('book_age_p95_ms') is not None:
            ages.append(metrics['book_age_p95_ms'])
        queues = observer.STATUS.get('ws_receive_queues', {})
        peak_drops = max(peak_drops, sum(q['drops'] for q in queues.values()))
        peak_cpu = max(peak_cpu, observer.STATUS.get('ws_process_cpu_cores', 0))
        peak_callback = max(peak_callback, max((q.get('callback_max_seconds', 0) for q in queues.values()), default=0))
        peak_tcp = max(peak_tcp, max((q.get('tcp_pending_bytes') or 0 for q in queues.values()), default=0))
        if len(moves) == 0 or moves[-1][1] != observer.STATUS.get('rebalance_count', 0):
            moves.append((round(now - t0, 1), observer.STATUS.get('rebalance_count', 0)))
        lags.append(max((q['lag_seconds'] for q in queues.values()), default=0))
        if last:
            samples.append((now, last['sent']))
    status = dict(observer.STATUS)
    connections.close()
    pipeline.close(5)
    server.join(10)
    while not report.empty():
        kind, payload = report.get_nowait()
        if kind == 'tick':
            last = payload
    # Server-side msg/s per shard: whole window and every 30 s window.
    first, final = samples[0], samples[-1]
    span = final[0] - first[0]
    shards = sorted(final[1], key=int)
    shard_rates = [(final[1][k] - first[1].get(k, 0)) / span for k in shards]
    window_ratios, steady, begin = [], [], first
    for sample in samples[1:]:
        if sample[0] - begin[0] >= 30:
            loads = [(sample[1].get(k, 0) - begin[1].get(k, 0)) / (sample[0] - begin[0]) for k in shards]
            window_ratios.append(round(max(loads) / max(min(loads), 1e-9), 2))
            peaks.append(round(max(loads), 1))
            if begin[0] >= t0 + args.settle:
                steady.append(window_ratios[-1])
            begin = sample
    ages.sort()
    lags.sort()

    def pct(values, q):
        return values[min(len(values) - 1, math.ceil(len(values) * q) - 1)] if values else None
    total = sum(m['rate'] for m in spec['markets'])
    result = dict(
        source=spec['source'], code=args.code or ROOT, scale=args.scale, seconds=args.seconds,
        shards=args.shards, bandwidth_kib=args.bandwidth_kib, slow_frames=args.slow_frames,
        gil_threads=args.gil_threads, switch_interval=sys.getswitchinterval(), tls=bool(args.tls_cert),
        input_msg_per_sec=round(total, 1),
        shard_msg_per_sec=[round(r, 1) for r in shard_rates],
        shard_max_min_ratio=round(max(shard_rates) / max(min(shard_rates), 1e-9), 3) if shard_rates else None,
        shard_ratio_30s=window_ratios,
        shard_ratio_30s_max_after_settle=max(steady) if steady else None,
        slow_consumer_server=last['slow_consumer'] if last else None,
        slow_consumer_client=status.get('slow_consumer_count', 0),
        reconnects=status.get('reconnect_count', 0), connections_opened=last['connections'] if last else None,
        rebalances=status.get('rebalance_count', 0),
        max_server_backlog_bytes=max(last['max_buffer'].values()) if last and last['max_buffer'] else None,
        book_age_p95_ms_p95=pct(ages, .95), book_age_p95_ms_max=ages[-1] if ages else None,
        receive_lag_s_p95=pct(lags, .95), receive_lag_s_max=lags[-1] if lags else None,
        input_hottest_market_msg_per_s=max(m['rate'] for m in spec['markets']),
        shard_peak_msg_per_s_30s=peaks, rebalance_timeline=moves,
        server_wire_bytes=last.get('wire_bytes') if last else None,
        server_offered_frames=last.get('offered') if last else None,
        max_server_queue_frames=last.get('max_queue_frames') if last else None,
        receive_drops=peak_drops, process_cpu_cores_max=peak_cpu,
        callback_seconds_max=peak_callback, tcp_pending_bytes_max=peak_tcp,
        reader_metrics=status.get('ws_receive_queues'),
        group_rates=status.get('ws_group_rates'),
        shard_assets=status.get('ws_shard_assets'), estimated_shard_loads=[
            round(x, 1) for x in status.get('ws_shard_loads', [])])
    print(json.dumps(result, indent=2))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--rates', help='measured per-asset rate JSON')
    parser.add_argument('--synthetic', action='store_true', help='use the built-in distribution')
    parser.add_argument('--code', help='repository backend_api_python to test (default: this tree)')
    parser.add_argument('--seconds', type=float, default=180)
    parser.add_argument('--warmup', type=float, default=30)
    parser.add_argument('--settle', type=float, default=240, help='seconds before steady-state ratio')
    parser.add_argument('--prior', action='store_true', help='seed group rates as after a restart')
    parser.add_argument('--switch-interval', type=float, default=0, help='override sys.setswitchinterval')
    parser.add_argument('--gil-threads', type=int, default=0,
                        help='pure-Python busy threads standing in for other observer work')
    parser.add_argument('--scale', type=float, default=1.0, help='multiply every rate')
    parser.add_argument('--time-scale', type=float, default=5.0, help='Up/Down clock speed-up')
    parser.add_argument('--refresh', type=float, default=60.0, help='catalogue refresh seconds (real time)')
    parser.add_argument('--slow-bytes', type=int, default=1 << 20)
    parser.add_argument('--book-share', type=float, default=.06)
    parser.add_argument('--pair-share', type=float, default=.5)
    parser.add_argument('--tls-cert', help='local TLS replay certificate')
    parser.add_argument('--tls-key', help='local TLS replay private key')
    parser.add_argument('--shards', type=int, choices=range(1, 9), default=4)
    parser.add_argument('--bandwidth-kib', type=float, default=0, help='per-connection modeled send capacity; 0 unlimited')
    parser.add_argument('--slow-frames', type=int, default=256, help='modeled application send queue capacity')
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--seed', type=int, default=7)
    args = parser.parse_args(argv)
    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error('tls-cert and tls-key must be given together')
    if args.seconds <= args.warmup + 2 or args.bandwidth_kib < 0 or args.slow_frames < 1:
        parser.error('seconds must exceed warmup + 2; capacities must be nonnegative')
    if not args.port:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            args.port = probe.getsockname()[1]
    return run(args)


if __name__ == '__main__':
    main()

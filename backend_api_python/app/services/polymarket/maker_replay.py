"""Offline replay of the v3 paper maker over recorded observer data. No network, no SQL.

Input is a directory of gzip CSV exports (see EXPORT_SQL), read-only copies of the
observer tables. The real Maker runs every `step_ms` exactly as RecordedFeed does:
latest recorded book per token received by then, the latest Binance reference
observation, and every public print received since the previous poll. Markets
settle on their recorded official outcome. Per-poll gate inputs (|F - M|, Z,
data faults, phases) are tallied so thresholds can be read off the data.

Recorded samples carry the Binance reference only while a market is listed, so
they have holes at every roll; optional klines_<ASSET>USDT.csv.gz (public 1 s
klines: open_ms,close) rebuild the live 5 s REST poller without gaps.
"""
import csv
import gzip
import json
import math
import pickle
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from . import maker_model as mm
from .maker import OUTCOMES, Maker, day_template
from .paper_account import Account, day_key

CHAINLINK = 'https://data.chain.link/streams/{}-usd'
EXPORT_SQL = dict(
    windows="SELECT market_id,asset,interval,start_time,end_time,window_seconds,official_outcome,up_token_id,down_token_id "
            "FROM qd_polymarket_observer_windows",
    spot="SELECT DISTINCT asset,reference_observed_at,reference_price FROM qd_polymarket_observer_samples "
         "WHERE reference_source LIKE '%binance%' AND observed_at>=:start",
    books="SELECT market_id,token_id,exchange_at,received_at,bids,asks FROM qd_polymarket_observer_books "
          "WHERE received_at>=:a AND received_at<:b",
    trades="SELECT recording_id,market_id,token_id,price,size,side,exchange_at,received_at,transaction_hash "
           "FROM qd_polymarket_observer_trades WHERE received_at>=:a AND received_at<:b")
GAP_EDGES = (.01, .02, .03, .04, .06, .08, .10, .15, .20)
Z_EDGES = (.005, .01, .015, .02, .03, .045, .06, .1)
SUMMED = ('quotes', 'cancels', 'placed_up', 'placed_down', 'filled_up', 'filled_down', 'fill_events_up', 'fill_events_down',
          'paired_sets', 'set_pnl', 'net_pnl', 'settlement_pnl', 'markets_settled', 'markets_with_fills', 'decomposed_shares',
          'fill_edge', 'residual', 'decomposed_pnl', 'as_1', 'as_5', 'as_60', 'as_300', 'unpaired_at_flat',
          'quote_seconds_improve', 'quote_seconds_join', 'filled_improve', 'filled_join', 'brier_model', 'brier_book', 'brier_n')


def ms(text):
    return datetime.fromisoformat(text).timestamp() * 1000 if text else None


def rows(path):
    with gzip.open(path, 'rt', newline='') as handle:
        yield from csv.reader(handle)


def prepare(directory):
    """Split the chunked exports into one pickle per market (books + prints), once."""
    directory = Path(directory)
    cache = directory / 'markets'
    if cache.is_dir() and any(cache.iterdir()):
        return cache
    cache.mkdir(exist_ok=True)
    tokens = {}
    for r in rows(directory / 'windows.csv.gz'):
        tokens[r[7]], tokens[r[8]] = (r[0], 0), (r[0], 1)
    books, prints = defaultdict(list), defaultdict(list)
    for path in sorted(directory.glob('books_*.csv.gz')):
        for market_id, token, exchange, received, bids, asks in rows(path):
            if token in tokens:
                books[market_id].append((ms(received), ms(exchange), tokens[token][1], bids, asks))
    for path in sorted(directory.glob('trades_*.csv.gz')):
        for identity, market_id, token, price, size, side, exchange, received, tx in rows(path):
            if token in tokens:
                prints[market_id].append((ms(received), tokens[token][1], dict(
                    recording_id=identity, price=price, size=size or None, side=side, exchange_ms=ms(exchange),
                    received_ms=ms(received), transaction_hash=tx or None)))
    for market_id in set(books) | set(prints):
        with open(cache / f'{market_id}.pkl', 'wb') as handle:
            pickle.dump(dict(books=sorted(books[market_id], key=lambda b: b[0]),
                             prints=sorted(prints[market_id], key=lambda p: (p[0], p[2]['recording_id']))), handle)
    return cache


def klines(directory, asset, period_ms):
    """Binance REST poller rebuilt from public 1 s klines: one observation every period_ms.

    The live reference thread polls ticker/price about every 5 s regardless of markets;
    recorded samples are written only while a market is listed, so they have holes at
    every roll. The last complete second's close stands in for the ticker price.
    """
    path = Path(directory) / f'klines_{asset}USDT.csv.gz'
    if not path.exists():
        return None
    closes = {}
    for opened, close in rows(path):
        closes[int(opened)] = close
    if not closes:
        return None
    out, t, last, price = [], min(closes) + 1000, max(closes) + 1000, None
    seconds = sorted(closes)
    i = 0
    while t <= last:
        while i < len(seconds) and seconds[i] <= (t // 1000) * 1000 - 1000:
            price, i = closes[seconds[i]], i + 1  # A second without trades keeps the last close.
        if price is not None:
            out.append((float(t), price))
        t += period_ms
    return out


def load(directory, period_ms=5160):
    directory = Path(directory)
    windows = []
    for market_id, asset, interval, start, end, w, outcome, up, down in rows(directory / 'windows.csv.gz'):
        windows.append(dict(id=market_id, asset=asset, interval=interval, start=ms(start), end=ms(end), w=int(w),
                            outcome=outcome or None, up=up, down=down))
    spot = defaultdict(list)
    for asset, observed, price in rows(directory / 'spot.csv.gz'):
        if observed:
            spot[asset].append((ms(observed), price))
    spot = {a: sorted(v) for a, v in spot.items()}
    for asset in list(spot):
        rebuilt = klines(directory, asset, period_ms)
        if rebuilt:
            spot[asset] = rebuilt
    return dict(windows=sorted(windows, key=lambda w: w['start']), spot=spot, cache=prepare(directory))


def market(window):
    """Market dict as the observer pipeline lists it (BTC/ETH 5m/15m are Chainlink-stream markets)."""
    start, end = (datetime.fromtimestamp(x / 1000).astimezone().isoformat() for x in (window['start'], window['end']))
    return dict(id=window['id'], market_type='up_down', asset=window['asset'], interval=window['interval'],
                yesAssetId=window['up'], noAssetId=window['down'], tickSize='0.01', feeRate='0.07', minOrderNotional='5',
                resolutionSource=CHAINLINK.format(window['asset'].lower()), eventStartTime=start, endDate=end)


class Probe(Maker):
    """Maker that tallies its gate inputs and fill attribution; quoting is unchanged.

    queue='proportional' is the sensitivity run: depth lost ahead of an order advances it.
    """
    def __init__(self, account, config, queue='pessimistic'):
        super().__init__(account, config)
        self.queue = queue
        self.tally = defaultdict(Counter)
        self.attribution = defaultdict(lambda: defaultdict(Decimal))
        self.calibration = defaultdict(lambda: [0, 0.0, 0.0])

    def context(self, row, books, now):
        ctx = super().context(row, books, now)
        t = self.tally[row['cell']]
        listed = any(b for b in books) or row.get('probe_listed')
        row['probe_listed'] = bool(listed)
        if ctx['time_phase'] == mm.OPEN and listed:
            t['open_polls'] += 1
            t['reason:' + str(ctx['reason'])] += 1
            if ctx.get('ok'):
                t['gap_bucket:%d' % mm.bucket(ctx['gap'], GAP_EDGES)] += 1
                t['z_bucket:%d' % mm.bucket(ctx['Z'], Z_EDGES)] += 1
                spread = float(ctx['best'][0][1] - ctx['best'][0][0])
                t['spread_ticks:%d' % min(round(spread / float(row['market']['tickSize'])), 6)] += 1
                if now - row.get('probe_cal_at', 0) >= 5000:
                    row['probe_cal_at'] = now
                    row.setdefault('probe_cal', []).append((ctx['F'], ctx['M'], mm.bucket(ctx['gap'], GAP_EDGES),
                                                            mm.bucket(ctx['tau'], (60, 120, 300, 600))))
        self.memory.setdefault('probe', {})[row['market']['id']] = ctx
        return ctx

    def observe_ring(self, row, books):
        super().observe_ring(row, books)
        if self.queue == 'proportional':  # Sensitivity: unexplained depth loss ahead of us advances the queue.
            for o in row['orders']:
                if o.get('ahead') is not None and o.get('ahead_prop') is not None:
                    o['ahead'] = str(min(Decimal(o['ahead']), Decimal(o['ahead_prop'])))

    def normal(self, row, ctx, now):
        """Polls where both sides may open: OPEN, data ok, no gate, both fairs in range, no inventory."""
        p = self.p
        return (ctx.get('ok') and ctx['phase'] == mm.OPEN and not self.gates(row, ctx, now)[0]
                and all(p.fair_min <= f <= p.fair_max for f in ctx['fair']) and not ctx.get('spread_spike')
                and Decimal(row['qty'][0]) == Decimal(row['qty'][1]))

    def place(self, row, side, price, size, ctx, now, kind='live', level=None, eligible=False):
        order = super().place(row, side, price, size, ctx, now, kind, level, eligible)
        if order is not None and kind == 'live':
            behind = int((ctx['best'][side][0] - price) / Decimal(row['market']['tickSize']))
            order['probe'] = (mm.bucket(ctx['gap'], GAP_EDGES), mm.bucket(ctx['Z'], Z_EDGES), behind,
                              mm.bucket(ctx['tau'], (120, 300, 600)))
        return order

    def fill(self, row, order, qty, pr, how, now):
        super().fill(row, order, qty, pr, how, now)
        if order['kind'] == 'live':
            row['fills'][-1]['probe'] = order.get('probe')

    def settle(self, market_id, outcome, now, estimated=False):
        row = self.state['markets'].get(market_id)
        y = OUTCOMES.get(outcome)
        if row is not None and not row['settled'] and y is not None and now >= row['spec']['t1'] * 1000:
            for record in row['fills']:
                parts = self.decompose(record, y, row['spec']['t1'] * 1000)
                gap, z, behind, tau = record.get('probe') or (None, None, None, None)
                for key in ('all', f'gap:{gap}', f'z:{z}', f'behind:{behind}', f'tau:{tau}', 'cell:' + row['cell']):
                    bucket = self.attribution[key]
                    for name in ('qty', 'edge', 'as_5', 'as_60', 'pnl'):
                        bucket[name] += parts[name]
            for f, m, gap, tau in row.get('probe_cal', []):
                for key in (f'gap:{gap}', f'tau:{tau}', 'all'):
                    c = self.calibration[key]
                    c[0] += 1
                    c[1] += (f - float(y)) ** 2
                    c[2] += (m - float(y)) ** 2
        super().settle(market_id, outcome, now, estimated)


def replay(data, config=None, cells=('BTC:5m', 'ETH:5m', 'BTC:15m', 'ETH:15m'), step_ms=250, start=None, end=None,
           mode='full', queue='pessimistic', source='admitted', before_settle=None):
    """Run one Maker over the recorded period; returns aggregate metrics per cell and overall."""
    account = Account(dict(initial_capital='5000', trade_cap='1000'))
    m = Probe(account, config or {}, queue)
    windows = [w for w in data['windows'] if w['asset'] + ':' + w['interval'] in cells
               and (start is None or w['end'] > start) and (end is None or w['start'] < end)]
    loaded = {}
    first = min((w['start'] for w in windows), default=0)
    now = int(max(first - 600000, start or 0))
    stop = int(min(max((w['end'] for w in windows), default=0) + 120000, end or math.inf))
    spot_at = {a: 0 for a in data['spot']}
    pending = list(windows)
    quoting = Counter()

    def admit(day):
        m.state['selection'] = dict(reviewed=day, at=now, active={c: dict(source=source, since='', mode=mode) for c in cells},
                                    cooloff={}, lcb_history={}, scores={}, breakers={})
    admit(day_key(now))
    while now < stop:
        if m.state['selection'].get('reviewed') != day_key(now):
            admit(day_key(now))
        while pending and pending[0]['start'] - 1800000 <= now:
            w = pending.pop(0)
            path = data['cache'] / f"{w['id']}.pkl"
            blob = pickle.loads(path.read_bytes()) if path.exists() else dict(books=[], prints=[])
            loaded[w['id']] = dict(w=w, market=market(w), books=blob['books'], prints=blob['prints'], b=0, p=0,
                                   latest=[None, None])
        for asset, series in data['spot'].items():
            i = spot_at[asset]
            while i + 1 < len(series) and series[i + 1][0] <= now:
                i += 1
            spot_at[asset] = i
            if series and series[i][0] <= now:
                m.observe_spot(asset, series[i][1], series[i][0], now)
        m.begin(now)
        for identity, x in list(loaded.items()):
            while x['b'] < len(x['books']) and x['books'][x['b']][0] <= now:
                received, exchange, side, bids, asks = x['books'][x['b']]
                x['latest'][side] = dict(bids=json.loads(bids), asks=json.loads(asks), exchange_at=exchange)
                x['b'] += 1
                m.feed_at = max(m.feed_at or 0, received)
            if now < x['w']['end'] + 60000 or identity in m.state['markets']:
                m.step(x['market'], list(x['latest']), now)
            row = m.state['markets'].get(identity)
            batch = [[], []]
            while x['p'] < len(x['prints']) and x['prints'][x['p']][0] <= now:
                batch[x['prints'][x['p']][1]].append(x['prints'][x['p']][2])
                x['p'] += 1
            if row is not None and not row['settled']:
                for side in (0, 1):
                    m.prints(identity, side, batch[side], now)
                ctx = m.memory.get('probe', {}).get(identity)
                if ctx is not None and ctx['time_phase'] == mm.OPEN and row.get('probe_listed'):
                    sides = {o['side'] for o in row['orders'] if o['kind'] == 'live' and o['cancel_at'] is None
                             and o['live_from'] <= now}
                    quoting[row['cell'] + ':open_polls'] += 1
                    quoting[row['cell'] + ':any'] += bool(sides)
                    quoting[row['cell'] + ':both'] += len(sides) == 2
                    if m.normal(row, ctx, now):
                        quoting[row['cell'] + ':normal'] += 1
                        quoting[row['cell'] + ':normal_any'] += bool(sides)
                        quoting[row['cell'] + ':normal_both'] += len(sides) == 2
                        for side in (0, 1):
                            if side not in sides:
                                d = (row.get('decision') or {}).get('sides', {}).get(side, {})
                                placing = any(o['kind'] == 'live' and o['side'] == side and o['cancel_at'] is None and o['live_from'] > now
                                              for o in row['orders'])
                                why = 'placing' if placing else d.get('reason') or ('cooldown' if now < row['cooldown'][side] else 'no_order')
                                quoting['missing:' + str(why)] += 1
                if now >= x['w']['end'] + 5000:
                    outcome = x['w']['outcome'] or proxy(data['spot'].get(x['w']['asset'], []), x['w'])
                    if before_settle is not None and row is not None and not row['settled']:
                        before_settle(row, outcome)
                    m.settle(identity, outcome, now)
            if identity not in m.state['markets'] and now >= x['w']['end'] + 60000 or (row is not None and row['settled']):
                del loaded[identity]
        m.cleanup()
        now += step_ms
    return summarize(m, quoting, cells)


def proxy(series, w):
    """Binance-sample fallback when no official outcome was recorded (rare)."""
    def at(t):
        before = [p for s, p in series if t - 10000 <= s <= t]
        return Decimal(before[-1]) if before else None
    a, b = at(w['start']), at(w['end'])
    return None if a is None or b is None else 'Up' if b >= a else 'Down'


def summarize(m, quoting, cells):
    total = {k: Decimal(0) for k in SUMMED}
    skips, phases = Counter(), Counter()
    by_cell = defaultdict(lambda: dict(pnl=Decimal(0), filled=Decimal(0)))
    for day in m.state['days'].values():
        day = {**day_template(), **day}
        for key in SUMMED:
            total[key] += Decimal(str(day.get(key, 0)))
        skips.update(day['skip_reasons'])
        phases.update(day['phase_counts'])
        for cell, cs in day['cells'].items():
            by_cell[cell]['pnl'] += Decimal(cs['pnl'])
            by_cell[cell]['filled'] += Decimal(cs['filled'])
    out = {k: float(v) for k, v in total.items()}
    out['skip_reasons'], out['phase_counts'] = dict(skips), dict(phases)
    guards = Counter()
    for day in m.state['days'].values():
        guards.update(day.get('guard_cancels', {}))
    out['guard_cancels'] = dict(guards)
    out['missing'] = {k[8:]: v for k, v in quoting.items() if k.startswith('missing:')}
    out['cells'] = {}
    for cell in cells:
        t, open_polls = m.tally[cell], quoting[cell + ':open_polls']
        out['cells'][cell] = dict(
            pnl=float(by_cell[cell]['pnl']), filled=float(by_cell[cell]['filled']), open_polls=open_polls,
            quoting_any=quoting[cell + ':any'] / open_polls if open_polls else None,
            quoting_both=quoting[cell + ':both'] / open_polls if open_polls else None,
            normal_share=quoting[cell + ':normal'] / open_polls if open_polls else None,
            normal_any=quoting[cell + ':normal_any'] / quoting[cell + ':normal'] if quoting[cell + ':normal'] else None,
            normal_both=quoting[cell + ':normal_both'] / quoting[cell + ':normal'] if quoting[cell + ':normal'] else None,
            tally=dict(t))
    placed = out['placed_up'] + out['placed_down']
    filled = out['filled_up'] + out['filled_down']
    out['fill_rate'] = filled / placed if placed else None
    shares = out['decomposed_shares']
    for key in ('fill_edge', 'as_1', 'as_5', 'as_60', 'as_300', 'residual', 'decomposed_pnl'):
        out[key + '_per_share'] = out[key] / shares if shares else None
    shadow = defaultdict(lambda: defaultdict(Decimal))
    for day in m.state['days'].values():
        for level, bucket in day.get('shadow', {}).items():
            for name, value in bucket.items():
                shadow[level][name] += Decimal(str(value))
    out['shadow'] = {k: {n: float(v) for n, v in b.items()} for k, b in sorted(shadow.items())}
    out['attribution'] = {k: {n: float(v) for n, v in b.items()} for k, b in sorted(m.attribution.items())}
    out['calibration'] = {k: dict(n=c[0], brier_model=c[1] / c[0], brier_book=c[2] / c[0]) for k, c in sorted(m.calibration.items()) if c[0]}
    out['brier_model_mean'] = out['brier_model'] / out['brier_n'] if out['brier_n'] else None
    out['brier_book_mean'] = out['brier_book'] / out['brier_n'] if out['brier_n'] else None
    return out

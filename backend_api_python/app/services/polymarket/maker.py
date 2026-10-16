"""Paper-only v3 fair-value maker owned by the account worker. No execution integrations.

Edge comes from where fills sit relative to a conservative fair value, minus
adverse selection; pairing Up+Down only removes variance and frees capital.
Every virtual order obeys place/cancel latency and pessimistic queueing, and
rebates are scenarios only: they never enter cash or PnL.
"""
import copy
import hashlib
import json
import math
import os
import statistics
import time
from datetime import timedelta
from decimal import Decimal, ROUND_DOWN
from types import SimpleNamespace
from uuid import uuid4

from . import maker_model as mm
from .engine import ZERO, number
from .paper_account import day_key, utc
from .updown import timestamp

TAG = 'maker_fair_value_v3'
VERSION = 3
HORIZONS = (1, 5, 15, 60, 300)
MAIN_HORIZON = '60'
LEVELS = (-1, 0, 1, 2, 3)
ASSETS = ('BTC', 'ETH', 'SOL', 'XRP')
INTERVALS = ('5m', '15m', '1h', '4h', '1d')
SHORT = ('5m', '15m')
Z_EDGES, TAU_EDGES, QUEUE_EDGES = (.0025, .005, .01, .02, .03), (60, 300, 900, 3600, 14400), (0, 50, 200)
Q2, Q8 = Decimal('.01'), Decimal('.00000001')
DATA_FAULTS = ('book_missing', 'book_stale', 'updown_skew', 'crossed', 'spot_stale')
FEE_SOURCE = 'https://docs.polymarket.com/trading/fees'
REBATE_SOURCE = 'https://docs.polymarket.com/programs/maker-rebates'
TEXT = ('universe_assets', 'universe_intervals', 'cold_start', 'review_utc', 'preopen_windows', 'phases',
        'event_times', 'complementary_prints')
DEFAULTS = dict(
    # selection (shadow layer covers the whole universe; active layer <= max_active cells)
    universe_assets='BTC,ETH,SOL,XRP', universe_intervals='5m,15m,1h,4h,1d',
    cold_start='BTC:1h,ETH:1h,BTC:15m,ETH:15m,BTC:5m:preopen,ETH:5m:preopen',
    review_utc='00:05', lookback_days='7', halflife_days='3', min_markets='150', min_filled_shares='1000',
    as_edge_ratio_max='.7', max_active='6', max_active_per_coin='3', probation_days='3', probation_size_mult='.5',
    exit_cooloff_days='3', exit_min_markets='50', min_quotable_share='.20', min_taker_volume='2000',
    tight_spread_ticks='2', min_tight_share='.5', max_stale_share='.02', bootstrap_iters='2000', lcb_alpha='.10',
    # fair value / toxicity
    sigma_prior_annual='.5', vol_halflife_short_s='300', vol_halflife_long_s='14400', vol_mix='.5', vol_scale='1',
    gap_max='.04', latency_eff_s='.30', sigma_short_window_s='60', z_hard_max='.03',
    # quote
    h_min='.01', k_tox='3', lam_fee='1', c0='0', k_hedge='1.5', e_neg='0', round_tol_ticks='.2',
    max_improve_ticks='1', hedge_max_behind_ticks='3', fair_min='.10', fair_max='.90', spread_spike_mult='2',
    base_size='10', max_order='25', kappa_inv='.02', n_cap='20', time_mult_cap='3',
    flow_ticks='1', flow_hold_s='15', flow_window_s='10', flow_median_s='1800', flow_mult='3',
    sweep_levels='2', sweep_window_s='5', fill_ticks='1', fill_hold_s='10', fill_size_mult='.5',
    momentum_sigma='2', soft_size_mult='.5', soft_ticks='1',
    preopen_windows='5m:120,15m:300,1h:900,4h:1800,1d:3600', preopen_pull_s='5', strike_wait_s='2',
    phases='5m:120:60:20,15m:180:90:30,1h:300:120:30,4h:600:180:60,1d:900:300:60',
    # order management / guards
    min_rest_ms='500', up_persist_ms='300', resize_ratio='.5', max_actions_per_market_s='5',
    book_stale_ms='2000', book_stale_ms_short='2000', updown_sync_ms='250', spot_stale_ms='6000',
    jump_sigma='4', jump_cooldown_ms='3000', fv_adverse_frac='.5',
    depth_pull_frac='.5', depth_pull_window_ms='1500', depth_pull_cooldown_ms='2000',
    event_blackout_min='10', event_times='',
    # risk (stops are loss magnitudes in USD)
    n_cap_coin_dir='60', n_cap_global_dir='100', worst_loss_market='12', worst_loss_global='75',
    lock_market='100', lock_global='1000', daily_soft_stop='50', daily_hard_stop='100',
    cell_daily_stop='25', weekly_stop='200', as_alert_sigma='2',
    # simulator
    place_latency_ms='250', cancel_latency_ms='250', print_lag_ms='2000', complementary_prints='verify',
    settle_timeout_s='3600',
    # scenario only
    rebate_fee_rate='.07', rebate_pool_fraction='.20')


def enabled():
    return os.getenv('POLYMARKET_PAPER_MAKER_ENABLED', 'false').lower() in {'1', 'true', 'yes'}


def cells(text, assets=ASSETS, intervals=INTERVALS):
    """'BTC:1h,BTC:5m:preopen' -> {'BTC:1h': 'full', 'BTC:5m': 'preopen'}."""
    result = {}
    for item in filter(None, (x.strip() for x in text.split(','))):
        parts = item.split(':')
        if len(parts) not in (2, 3) or parts[0] not in assets or parts[1] not in intervals or parts[2:] not in ([], ['preopen']):
            raise ValueError(item)
        result[parts[0] + ':' + parts[1]] = parts[2] if len(parts) == 3 else 'full'
    return result


def table(text, width):
    result = {}
    for item in text.split(','):
        key, *values = item.strip().split(':')
        if key not in INTERVALS or len(values) != width or key in result:
            raise ValueError(item)
        result[key] = tuple(float(number(v)) for v in values)
    if set(result) != set(INTERVALS) or any(v < 0 for row in result.values() for v in row):
        raise ValueError(text)
    return result


def params(c):
    """Typed view of a validated string config; raises ValueError on inconsistency."""
    p = SimpleNamespace(**{k: float(v) for k, v in c.items() if k not in TEXT})
    p.assets = [a for a in c['universe_assets'].split(',') if a]
    p.intervals = [i for i in c['universe_intervals'].split(',') if i]
    if not p.assets or not p.intervals or not set(p.assets) <= set(ASSETS) or not set(p.intervals) <= set(INTERVALS):
        raise ValueError('universe')
    p.cold_start = cells(c['cold_start'], p.assets, p.intervals)
    p.phases = table(c['phases'], 3)
    p.preopen = {k: v[0] for k, v in table(c['preopen_windows'], 1).items()}
    if any(not r >= h >= f > 0 for r, h, f in p.phases.values()):
        raise ValueError('phases')
    hours, minutes = (int(x) for x in c['review_utc'].split(':'))
    if not (0 <= hours < 24 and 0 <= minutes < 60):
        raise ValueError('review_utc')
    p.review_minutes = hours * 60 + minutes
    p.events = [timestamp(x.strip()).timestamp() for x in c['event_times'].split(',') if x.strip()]
    p.complementary = c['complementary_prints']
    if p.complementary not in ('verify', 'count'):
        raise ValueError('complementary_prints')
    for key in ('max_improve_ticks', 'hedge_max_behind_ticks', 'base_size', 'max_order', 'n_cap', 'n_cap_coin_dir',
                'n_cap_global_dir', 'worst_loss_market', 'worst_loss_global', 'lock_market', 'lock_global'):
        setattr(p, key + '_d', number(c[key]))
    p.max_improve_ticks, p.hedge_max_behind_ticks = p.max_improve_ticks_d, p.hedge_max_behind_ticks_d
    positive = ('h_min', 'base_size', 'max_order', 'n_cap', 'sigma_prior_annual', 'vol_halflife_short_s',
                'vol_halflife_long_s', 'vol_scale', 'sigma_short_window_s', 'flow_window_s', 'sweep_window_s',
                'lookback_days', 'halflife_days', 'probation_size_mult', 'fill_size_mult', 'soft_size_mult',
                'max_actions_per_market_s', 'book_stale_ms', 'book_stale_ms_short', 'spot_stale_ms', 'jump_sigma',
                'momentum_sigma', 'time_mult_cap', 'flow_mult', 'max_active', 'max_active_per_coin', 'lcb_alpha')
    fractions = ('vol_mix', 'probation_size_mult', 'fill_size_mult', 'soft_size_mult', 'fv_adverse_frac',
                 'depth_pull_frac', 'resize_ratio', 'min_quotable_share', 'min_tight_share', 'max_stale_share',
                 'rebate_fee_rate', 'rebate_pool_fraction', 'fair_min', 'fair_max')
    integers = ('bootstrap_iters', 'max_active', 'max_active_per_coin', 'min_markets', 'exit_min_markets',
                'lookback_days', 'probation_days', 'exit_cooloff_days', 'sweep_levels')
    if (any(getattr(p, k) <= 0 for k in positive) or any(getattr(p, k) > 1 for k in fractions)
            or any(getattr(p, k) != int(getattr(p, k)) for k in integers) or p.bootstrap_iters < 100
            or not p.lcb_alpha < 1 or not p.fair_min < p.fair_max or not p.round_tol_ticks < 1
            or p.time_mult_cap < 1):
        raise ValueError('bounds')
    p.sigma_prior = p.sigma_prior_annual / math.sqrt(mm.SECONDS_PER_YEAR)
    return p


def validate(config):
    config = {**DEFAULTS, **config}
    if set(config) != set(DEFAULTS):
        raise ValueError('invalid_maker_configuration')
    try:
        for key in DEFAULTS:
            if key in TEXT:
                config[key] = str(config[key]).strip()
                continue
            value = number(config[key])
            if value < 0 or value > 10000000:
                raise ValueError(key)
            config[key] = str(value)
        params(config)
    except (ValueError, TypeError, KeyError, ArithmeticError):
        raise ValueError('invalid_maker_configuration') from None
    return config


def configuration():
    return validate({k: os.getenv('POLYMARKET_PAPER_MAKER_' + k.upper(), v) for k, v in DEFAULTS.items()})


def rebate(quantity, price, fee_rate='.07', pool_fraction='.20'):
    # Scenario only: our pool share ~= 20% of our own fills' fee equivalent.
    # Actual daily share requires the market pool and total maker weight.
    # https://docs.polymarket.com/programs/maker-rebates
    return number(quantity) * number(fee_rate) * number(price) * (1 - number(price)) * number(pool_fraction)


def day_template():
    return dict(strategy=TAG, version=VERSION, quotes=0, cancels=0, replaces=0, shadow_quotes=0,
                placed_up='0', placed_down='0', filled_up='0', filled_down='0', fill_events_up=0, fill_events_down=0,
                filled_improve='0', filled_join='0', quote_seconds_improve='0', quote_seconds_join='0',
                notional_filled='0', paired_sets='0', set_pnl='0', net_pnl='0', settlement_pnl='0',
                unpaired_settlement_loss='0', markets_settled=0, markets_with_fills=0, unresolved_markets=0,
                decomposed_shares='0', fill_edge='0', residual='0', decomposed_pnl='0',
                **{f'as_{h}': '0' for h in HORIZONS}, mark_samples=0, mark_missing=0, estimated_settlements=0,
                paired_before_flat='0', unpaired_at_flat='0', flat_settlement_pnl='0', flat_red_flags=[],
                hedge_durations=[], inventory_peak='0', locked_sum='0', locked_count=0, locked_max='0',
                rebate_basis='0', prop_filled='0', skip_reasons={}, phase_counts={}, guard_cancels={},
                mirror_checked=0, mirror_mismatch=0, complementary_checked=0, complementary_sum_one=0,
                complementary_fills='0', brier_model='0', brier_book='0', brier_n=0,
                reliability=[[0, '0', '0'] for _ in range(10)], shadow={}, cells={},
                soft_stop=False, hard_stop=False, alerts=[])


def fresh():
    return dict(strategy=TAG, version=VERSION, seq=0, markets={}, days={}, assets={}, selection={}, halt=None)


def upgrade(state, account):
    """Adopt v1/v2 state in place: keep inventory and history, release old reservations."""
    if state.get('version') == VERSION:
        return
    for key, value in fresh().items():
        state.setdefault(key, value)
    if 'history' in state:
        state['legacy_history'] = state.pop('history')
    for identity, row in list(state['markets'].items()):
        if 'orders' in row:
            continue
        reserved = number(row.get('reserved', '0'))
        if reserved:
            account.transfer('locked', -reserved)
            account.transfer('free', reserved)
        market, now = row['market'], int(time.time() * 1000)
        lots = [[[0, q]] if number(q) > 0 else [] for q in row['qty']]
        spec = mm.parse_rules(market) or dict(source='legacy', w=0.0 if market.get('interval') in ('1h', '1d') else 60.0,
                                              t0=timestamp(market['eventStartTime']).timestamp(),
                                              t1=timestamp(market['endDate']).timestamp(), tie=1.0)
        state['markets'][identity] = new_row(market, spec, now, qty=list(row['qty']), cost=list(row['cost']),
                                             lots=lots, settled=row.get('settled', False))
    state.update(strategy=TAG, version=VERSION)


def new_row(market, spec, now, **values):
    row = dict(market=market, spec=spec, cell=market.get('asset', '') + ':' + market.get('interval', ''),
               created=now, qty=['0', '0'], cost=['0', '0'], lots=[[], []], orders=[], fills=[], shadow_fills=[],
               a_start=None, cursor=[now, now], print_ids=[[], []], caught_up=[True, True], ring=[[], []],
               recent=[[], []], txs=[], persist=[None, None], cooldown=[0, 0], fill_until=[0, 0],
               flow_until=[0, 0], actions=[], flat=None, settled=False, pnl='0', filled='0', calib=[],
               summary=dict(polls=0, stale=0, tight=0, open=0, quotable=0, taker_volume='0'),
               decision=None, fair=None, phase=None)
    row.update(values)
    return row


def displayed(book, price):
    """Visible bid size at `price`; 0 above best bid, None below the recorded depth."""
    levels = [(number(level['price']), number(level['size'])) for level in book['bids']]
    if not levels or price > levels[0][0]:
        return ZERO
    if price < min(p for p, _ in levels):
        return None
    return sum((z for p, z in levels if p == price), ZERO)


def select(summaries, previous, p, now):
    """Daily active-cell review from settled per-market summaries (no lookahead).

    Score: half-life weighted mean per-market PnL of live-eligible target-level
    shadow fills (rebates excluded), one-sided LCB by cluster bootstrap.
    """
    today = utc(now).date()
    by_cell = {}
    for s in summaries:
        end = utc(s['end']).date()
        if today - timedelta(days=int(p.lookback_days)) <= end < today and s.get('outcome') is not None:
            by_cell.setdefault(s['cell'], []).append((s, (today - end).days))
    scores = {}
    for asset in p.assets:
        for interval in p.intervals:
            cell = asset + ':' + interval
            rows = by_cell.get(cell, [])
            traded = [(s, age) for s, age in rows if number(s['shadow']['eligible']['shares']) > 0]
            samples = [(s['end'], float(s['shadow']['eligible']['pnl']), .5 ** (age / p.halflife_days)) for s, age in traded]
            values = [(v, w) for _, v, w in samples]
            mean = mm.weighted_mean(values)
            std = math.sqrt(mm.weighted_mean([((v - mean) ** 2, w) for v, w in values])) if values else 0.0
            seed = int(hashlib.sha256((cell + today.isoformat()).encode()).hexdigest()[:8], 16)
            lcb = mm.cluster_bootstrap(samples, int(p.bootstrap_iters), p.lcb_alpha, seed) if samples else None
            shares = sum((number(s['shadow']['eligible']['shares']) for s, _ in traded), ZERO)
            edge = sum(float(s['shadow']['eligible']['edge']) for s, _ in traded)
            as60 = sum(float(s['shadow']['eligible']['as60']) for s, _ in traded)
            polls = sum(s['polls'] for s, _ in rows)
            opened = sum(s['open'] for s, _ in rows)
            recent = [float(s['shadow']['eligible']['pnl']) for s, age in traded if age < 3]
            score = dict(markets=len(traded), shares=str(shares), mean=mean, std=std, lcb=lcb,
                         as_edge_ratio=-as60 / edge if edge > 0 else None,
                         median_taker_volume=statistics.median([float(s['taker_volume']) for s, _ in rows]) if rows else 0.0,
                         tight_share=sum(s['tight'] for s, _ in rows) / polls if polls else 0.0,
                         stale_share=sum(s['stale'] for s, _ in rows) / polls if polls else 1.0,
                         quotable_share=sum(s['quotable'] for s, _ in rows) / opened if opened else 0.0,
                         recent_markets=len(recent), recent_mean=sum(recent) / len(recent) if recent else None)
            score['filters'] = (score['median_taker_volume'] >= p.min_taker_volume and score['tight_share'] >= p.min_tight_share
                                and score['stale_share'] < p.max_stale_share and score['quotable_share'] >= p.min_quotable_share)
            score['admissible'] = bool(score['filters'] and lcb is not None and lcb > 0 and len(traded) >= p.min_markets
                                       and shares >= number(p.min_filled_shares) and score['as_edge_ratio'] is not None
                                       and score['as_edge_ratio'] <= p.as_edge_ratio_max)
            scores[cell] = score
    key = today.isoformat()
    cooloff = {c: d for c, d in previous.get('cooloff', {}).items() if d > key}
    history = {c: (previous.get('lcb_history', {}).get(c, []) + [s['lcb']])[-2:] for c, s in scores.items()}
    breakers = previous.get('breakers', {})
    until = lambda days: (today + timedelta(days=int(days))).isoformat()
    active = {}
    for cell, entry in previous.get('active', {}).items():
        s = scores.get(cell)
        if s is None:
            continue
        if entry.get('source') == 'admitted':
            leave = (all(x is not None and x < 0 for x in history[cell]) and len(history[cell]) == 2
                     or s['recent_markets'] >= p.exit_min_markets and s['recent_mean'] < 0)
        else:  # Cold-start prior: once the sample is complete, evidence decides.
            leave = s['markets'] >= p.min_markets and not s['admissible']
        if leave or cell in breakers:
            cooloff[cell] = until(p.exit_cooloff_days)
            continue
        if entry.get('source') != 'admitted' and s['admissible']:
            entry = dict(source='admitted', since=key, probation_until=until(p.probation_days), mode='full')
        active[cell] = entry
    def fits(cell):
        coin = cell.split(':')[0]
        return len(active) < p.max_active and sum(c.split(':')[0] == coin for c in active) < p.max_active_per_coin
    ranked = sorted((c for c, s in scores.items() if s['admissible'] and c not in active and c not in cooloff),
                    key=lambda c: (-(scores[c]['mean'] / scores[c]['std'] if scores[c]['std'] else math.inf), c))
    for cell in ranked:
        if fits(cell):
            active[cell] = dict(source='admitted', since=key, probation_until=until(p.probation_days), mode='full')
    for cell, mode in p.cold_start.items():
        if cell not in active and cell not in cooloff and scores[cell]['markets'] < p.min_markets and fits(cell):
            active[cell] = dict(source='cold_start', since=key, mode=mode)
    return dict(reviewed=key, at=now, active=active, cooloff=cooloff, lcb_history=history, scores=scores, breakers={})


class Maker:
    def __init__(self, account, config=None):
        self.account = account
        self.config = validate(config) if config is not None else configuration()
        self.p = params(self.config)
        self.hash = hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest()[:16]
        self.state = account.state.setdefault('maker', fresh())
        upgrade(self.state, account)
        self.memory = {}  # Ephemeral flow/spread history; rebuilt after restart.
        self.risk = None

    # ----- bookkeeping -------------------------------------------------------------
    def audit(self, event, market_id, now, detail):
        if not hasattr(self.account, 'maker_changed'):
            self.account.maker_changed = []
        self.account.maker_changed.append(dict(id=str(uuid4()), event=event, market_id=market_id, at=now, strategy=TAG,
            detail=copy.deepcopy(dict(detail, strategy=TAG, version=VERSION, config_hash=self.hash))))

    def stats(self, now):
        key = day_key(now)
        day = self.state['days'].get(key)
        if day is None or day.get('version') != VERSION:
            # A v1/v2 day rolling into this release keeps its numbers apart; v3 stops never see them.
            legacy = day
            day = self.state['days'][key] = day_template()
            if legacy is not None:
                day['legacy'] = legacy
        return day

    def add(self, now, key, value):
        s = self.stats(now)
        s[key] = str(number(s[key]) + value)

    def count(self, now, group, key, amount=1):
        bucket = self.stats(now)[group]
        bucket[key] = bucket.get(key, 0) + amount

    def cell_stats(self, now, cell):
        return self.stats(now)['cells'].setdefault(cell, dict(pnl='0', filled='0', as60='0', as60_shares='0', stopped=None))

    def next_id(self, market_id):
        self.state['seq'] = self.state.get('seq', 0) + 1
        return f"{market_id}:{self.state['seq']}"

    def in_universe(self, market):
        return (market.get('market_type') == 'up_down' and market.get('asset') in self.p.assets
                and market.get('interval') in self.p.intervals)

    # ----- reference price and volatility -----------------------------------------
    def asset(self, name):
        prior = self.p.sigma_prior ** 2
        return self.state['assets'].setdefault(name, dict(at=None, z=None, var_s=prior, var_l=prior, samples=[],
                                                          cooldown_until=0, jump_at=0, momentum=0.0, momentum_at=0))

    def sigma(self, a):
        p = self.p
        return p.vol_scale * math.sqrt(p.vol_mix * a['var_s'] + (1 - p.vol_mix) * a['var_l'])

    def sigma_short(self, a, now):
        start = now / 1000 - self.p.sigma_short_window_s
        points = [s for s in a['samples'] if s[0] >= start]
        pairs = list(zip(points, points[1:]))
        realized = math.sqrt(sum((z2 - z1) ** 2 for (_, z1), (_, z2) in pairs) / sum(t2 - t1 for (t1, _), (t2, _) in pairs)) if len(pairs) >= 3 else 0.0
        return max(realized, self.sigma(a))  # Never below the blended estimate on a thin sample.

    def observe_spot(self, name, price, observed_ms, now):
        """New settlement-source proxy observation (Binance); duplicates are ignored."""
        if price is None or observed_ms is None or number(price) <= 0:
            return
        a, p = self.asset(name), self.p
        if a['at'] is not None and observed_ms <= a['at']:
            return
        z = math.log(float(price))
        if a['at'] is not None:
            dt, r = (observed_ms - a['at']) / 1000, z - a['z']
            if dt <= 60:
                score = r / (self.sigma(a) * math.sqrt(dt))
                a['momentum'], a['momentum_at'] = score, observed_ms
                if abs(score) > p.jump_sigma:  # Whole-coin pull; resume only on newer books.
                    a['cooldown_until'], a['jump_at'] = now + p.jump_cooldown_ms, observed_ms
                for key, halflife in (('var_s', p.vol_halflife_short_s), ('var_l', p.vol_halflife_long_s)):
                    alpha = 1 - .5 ** (dt / halflife)
                    a[key] = (1 - alpha) * a[key] + alpha * r * r / dt
        a['at'], a['z'] = observed_ms, z
        horizon = observed_ms / 1000 - max(180.0, p.sigma_short_window_s + 10)
        a['samples'] = [s for s in a['samples'] if s[0] >= horizon][-400:] + [[observed_ms / 1000, z]]

    def windows(self, row, t, a):
        """Realized (A_start, A_end) of the settlement averages, or None if uncovered."""
        # Window averages tolerate two missed REST observations before declaring no coverage.
        spec, gap = row['spec'], 2 * self.p.spot_stale_ms / 1000
        t0, t1, w = spec['t0'], spec['t1'], spec['w']
        if t < t0 - w:
            return 0.0, 0.0
        a_start = row['a_start']
        if a_start is None:
            if w == 0:
                strike = row['market'].get('price_to_beat')
                if strike and number(strike) > 0:
                    a_start = math.log(float(strike))
                else:  # Binance candle open: first observation at/after T0, else last before.
                    after = [z for s, z in a['samples'] if t0 <= s <= t0 + gap]
                    before = [z for s, z in a['samples'] if t0 - gap <= s < t0]
                    a_start = after[0] if after else before[-1] if before else None
                if a_start is None:
                    return None
                row['a_start'] = a_start
            else:
                integral = mm.window_integral(a['samples'], t0 - w, min(t, t0), gap)
                if integral is None:
                    return None
                a_start = integral / w
                if t >= t0:
                    row['a_start'] = a_start
        a_end = 0.0
        if w and t >= t1 - w:
            integral = mm.window_integral(a['samples'], t1 - w, t, gap)
            if integral is None:
                return None
            a_end = integral / w
        return a_start, a_end

    # ----- per-poll risk ----------------------------------------------------------
    def unrealized(self):
        total, by_cell = ZERO, {}
        for row in self.state['markets'].values():
            if row['settled'] or not row['fair']:
                continue
            value = sum((number(q) * Decimal(repr(f)) for q, f in zip(row['qty'], row['fair'])), ZERO) - sum(map(number, row['cost']))
            total += value
            by_cell[row['cell']] = by_cell.get(row['cell'], ZERO) + value
        return total, by_cell

    def begin(self, now):
        """Once per poll: selection bootstrap, daily/weekly stops, cell breakers, capital samples."""
        p, day, today = self.p, self.stats(now), day_key(now)
        self.risk = dict(at=now, soft=False, hard=False, stopped={})
        self.contrib = {}  # Landed cancels drop out of worst-case exposure each poll.
        if not self.state['selection']:
            self.state['selection'] = select([], {}, p, now)
            self.state['selection']['reviewed'] = None  # Priors only; the dated review still runs.
        unreal, by_cell = self.unrealized()
        daily = number(day['net_pnl']) + unreal
        week = sum((number(d['net_pnl']) for k, d in self.state['days'].items()
                    if k >= day_key(now - 6 * 86400000) and d.get('strategy') == TAG), ZERO) + unreal
        if week <= -number(self.config['weekly_stop']) and not self.state['halt']:
            self.state['halt'] = dict(reason='weekly_stop', at=now, rolling_pnl=str(week))
            day['alerts'] = (day['alerts'] + ['weekly_stop'])[-20:]
            self.audit('halt', 'account', now, dict(self.state['halt']))
        hard = daily <= -number(self.config['daily_hard_stop']) or bool(self.account.daily(now).get('loss_stop'))
        soft = daily <= -number(self.config['daily_soft_stop'])
        day['soft_stop'] = day['soft_stop'] or soft
        day['hard_stop'] = day['hard_stop'] or hard
        stopped = {}
        for cell, cs in day['cells'].items():
            reason = cs.get('stopped')
            if not reason and number(cs['pnl']) + by_cell.get(cell, ZERO) <= -number(self.config['cell_daily_stop']):
                reason = 'cell_daily_stop'
            if not reason and number(cs['as60_shares']) >= p.base_size * 2:
                history = [number(d['cells'][cell]['as60']) / number(d['cells'][cell]['as60_shares'])
                           for k, d in self.state['days'].items() if day_key(now - 7 * 86400000) <= k < today
                           and number(d.get('cells', {}).get(cell, {}).get('as60_shares', '0')) > 0]
                if len(history) >= 3:
                    mean, std = statistics.mean(history), statistics.pstdev(history)
                    if std and number(cs['as60']) / number(cs['as60_shares']) < mean - Decimal(repr(p.as_alert_sigma)) * std:
                        reason = 'as60_anomaly'
            if reason:
                if not cs.get('stopped'):
                    cs['stopped'] = reason
                    day['alerts'] = (day['alerts'] + [cell + ':' + reason])[-20:]
                    self.state['selection'].setdefault('breakers', {})[cell] = today
                    self.audit('cell_stop', cell, now, dict(reason=reason))
                stopped[cell] = reason
        exposure = self.exposures()
        day['locked_sum'] = str(number(day['locked_sum']) + exposure['locked'])
        day['locked_count'] += 1
        day['locked_max'] = str(max(number(day['locked_max']), exposure['locked']))
        day['inventory_peak'] = str(max(number(day['inventory_peak']), exposure['unpaired']))
        self.risk = dict(at=now, soft=soft, hard=hard, daily=daily, week=week, stopped=stopped)

    def contribution(self, row, skip_side=None):
        """One market's worst-case exposure if every resting order fills.

        skip_side excludes that side's live (not cancel-pending) orders, which the
        caller is about to replace. A cancel in flight still counts until it lands.
        Up shares count positive, Down negative.
        """
        n = number(row['qty'][0]) - number(row['qty'][1])
        rem, notional = [ZERO, ZERO], [ZERO, ZERO]
        now = self.risk['at'] if self.risk else 0
        for o in row['orders']:
            if (o['kind'] != 'live' or o['side'] == skip_side and o['cancel_at'] is None
                    or o['cancel_at'] is not None and o['cancel_at'] <= now):
                continue
            left = number(o['remaining'])
            rem[o['side']] += left
            notional[o['side']] += left * number(o['price'])
        residual = sum(map(number, row['cost']))

        def worst(direction, side):
            if direction >= 0:
                return (residual if direction > 0 else ZERO) + notional[side]
            return notional[side] / rem[side] * max(ZERO, rem[side] + direction) if rem[side] else ZERO
        return dict(asset=row['market']['asset'], n=n, rem=rem, losses=(worst(n, 0), worst(-n, 1)),
                    locked=sum(notional, ZERO) + residual, unpaired=abs(n))

    def exposures(self, exclude=None):
        """Totals over open markets from a per-market cache invalidated on every change."""
        cache = self.__dict__.setdefault('contrib', {})
        result = dict(up={}, down={}, worst=ZERO, locked=ZERO, unpaired=ZERO)
        for identity, row in self.state['markets'].items():
            if row['settled'] or identity == exclude:
                continue
            c = cache.get(identity)
            if c is None:
                c = cache[identity] = self.contribution(row)
            result['up'][c['asset']] = result['up'].get(c['asset'], ZERO) + c['n'] + c['rem'][0]
            result['down'][c['asset']] = result['down'].get(c['asset'], ZERO) + c['n'] - c['rem'][1]
            result['worst'] += max(c['losses'])
            result['locked'] += c['locked']
            result['unpaired'] += c['unpaired']
        return result

    def changed(self, row):
        self.__dict__.setdefault('contrib', {}).pop(row['market']['id'], None)

    def room(self, row, side, price):
        """Largest extra size on `side` the caps allow (replacing that side's live orders)."""
        p, others, mine = self.p, self.exposures(row['market']['id']), self.contribution(row, side)
        asset, sign = mine['asset'], 1 if side == 0 else -1
        up = others['up'].get(asset, ZERO) + mine['n'] + mine['rem'][0]
        down = others['down'].get(asset, ZERO) + mine['n'] - mine['rem'][1]
        overall = (sum(others['up'].values(), ZERO) + mine['n'] + mine['rem'][0] if side == 0
                   else -(sum(others['down'].values(), ZERO) + mine['n'] - mine['rem'][1]))
        limits = [p.n_cap_coin_dir_d - (up if side == 0 else -down), p.n_cap_global_dir_d - overall,
                  p.n_cap_d - max(ZERO, sign * mine['n'] + mine['rem'][side])]
        for cap, used in ((p.worst_loss_market_d, mine['losses'][side]), (p.worst_loss_global_d, others['worst'] + max(mine['losses'])),
                          (p.lock_market_d, mine['locked']), (p.lock_global_d, others['locked'] + mine['locked']),
                          (min(number(self.account.state['free']), number(self.account.config['trade_cap'])), ZERO)):
            limits.append((cap - used) / price)
        return max(ZERO, min(limits))

    # ----- one market, one poll -----------------------------------------------------
    def row(self, market, now):
        rows = self.state['markets']
        if market['id'] in rows:
            return rows[market['id']]
        spec = mm.parse_rules(market)
        if spec is None:
            self.count(now, 'skip_reasons', 'rules_unparsed')
            return None
        p, t = self.p, now / 1000
        ph = mm.phase(spec, t, p.phases[market['interval']], p.preopen[market['interval']], p.preopen_pull_s, p.strike_wait_s)
        if ph in (mm.WAIT, mm.FLAT) or len(rows) >= 100:
            return None
        rows[market['id']] = new_row(market, spec, now)
        return rows[market['id']]

    def step(self, market, books, now, quoting=True):
        """Recompute targets for one market and reconcile live + shadow virtual orders."""
        if not self.in_universe(market):
            return
        if self.risk is None or self.risk['at'] != now:
            self.begin(now)
        row = self.row(market, now)
        if row is None or row['settled']:
            return
        row['market'] = market
        self.observe_ring(row, books)
        ctx = self.context(row, books, now)
        if ctx.get('ok'):
            self.observe_books(row, ctx, now)
        self.finalize(row, now)
        self.account_quote_time(row, now)
        self.resolve_marks(row, now)
        if ctx['time_phase'] == mm.FLAT and row['flat'] is None:
            self.enter_flat(row, now)
        targets = self.targets(row, ctx, now) if quoting and ctx.get('ok') else {}
        if not ctx.get('ok') or not quoting:
            row['decision'] = dict(phase=ctx['phase'], reason=ctx['reason'] if quoting else 'quoting_disabled', sides={})
        self.reconcile(row, targets, ctx, now)
        self.shadow(row, ctx if quoting else dict(ctx, ok=False), now)
        self.sample(row, ctx, now)
        row['phase'] = ctx['phase']
        self.count(now, 'phase_counts', ctx['phase'])

    def context(self, row, books, now):
        p, spec, market = self.p, row['spec'], row['market']
        t, interval = now / 1000, market['interval']
        ph = mm.phase(spec, t, p.phases[interval], p.preopen[interval], p.preopen_pull_s, p.strike_wait_s)
        ctx = dict(phase=ph, time_phase=ph, t=t, tau=spec['t1'] - t, ok=False, reason=None, Z=0.0)

        def calm(reason):  # Data problems move any phase to a temporary calm.
            ctx.update(phase=mm.FLAT, reason=reason)
            return ctx
        if ph in (mm.WAIT, mm.FLAT):
            return calm('phase')
        if len(books) != 2 or not all(b and b.get('bids') and b.get('asks') and b.get('exchange_at') is not None for b in books):
            return calm('book_missing')
        stale = p.book_stale_ms_short if interval in SHORT else p.book_stale_ms
        if any(not -100 <= now - b['exchange_at'] <= stale for b in books):
            return calm('book_stale')
        if abs(books[0]['exchange_at'] - books[1]['exchange_at']) > p.updown_sync_ms:
            return calm('updown_skew')
        best = [(number(b['bids'][0]['price']), number(b['asks'][0]['price'])) for b in books]
        if any(bid >= ask for bid, ask in best):
            return calm('crossed')
        a = self.asset(market['asset'])
        if now < a['cooldown_until'] or min(b['exchange_at'] for b in books) <= a['jump_at']:
            return calm('coin_jump')
        mid = mm.microprice(books[0])
        f, d, v, z, sigma = .5, 0.0, 0.0, 0.0, self.sigma(a)
        if ph != mm.PREOPEN:  # Before the strike window the outcome ignores spot: F = 0.5, Z = 0.
            if a['at'] is None or now - a['at'] > p.spot_stale_ms:
                return calm('spot_stale')
            parts = self.windows(row, t, a)
            if parts is None:
                return calm('strike_unknown')
            f, d, v = mm.fair(spec, t, a['z'], *parts, sigma)
            # Effective exposure: reaction latency plus the age of our spot observation.
            z = mm.toxicity(spec, t, d, v, self.sigma_short(a, now), p.latency_eff_s + (now - a['at']) / 1000)
        fair = [min(f, mid), min(1 - f, 1 - mid)]
        ctx.update(ok=True, books=books, best=best, M=mid, F=f, d=d, V=v, Z=z, sigma=sigma, fair=fair,
                   gap=abs(f - mid), g=mm.weight(spec, t), asset=a)
        row['fair'] = fair
        if ph in mm.OPENING and (ctx['gap'] > p.gap_max or z > p.z_hard_max):
            ctx.update(phase=mm.REDUCE, reason='gap' if ctx['gap'] > p.gap_max else 'z_hard_max')
        return ctx

    def observe_ring(self, row, books):
        """Ring of recent distinct snapshots per token, used for marks, M- and depth pulls."""
        for i, book in enumerate(books[:2]):
            if not book or not book.get('bids') or not book.get('asks') or book.get('exchange_at') is None:
                continue
            ring = row['ring'][i]
            if ring and book['exchange_at'] <= ring[-1][0]:
                continue
            ring.append([book['exchange_at'], mm.microprice(book), book['bids'][0]['price'], book['bids'][0]['size']])
            del ring[:-16]
            for o in row['orders']:  # Proportional queue comparison: unexplained depth loss advances it.
                if o['side'] == i and o.get('ahead_prop') is not None and o.get('level_size') is not None:
                    shown = displayed(book, number(o['price']))
                    base = number(o['level_size']) - number(o['traded'])
                    if shown is not None and base > 0 and shown < base:
                        o['ahead_prop'] = str(number(o['ahead_prop']) * shown / base)
                    o['level_size'], o['traded'] = (str(shown) if shown is not None else None), '0'

    def observe_books(self, row, ctx, now):
        """Mirror-book check, spread history and spike flag for a coherent pair."""
        day = self.stats(now)
        (bid_up, ask_up), (bid_down, ask_down) = ctx['best']
        day['mirror_checked'] += 1
        day['mirror_mismatch'] += int(bid_up + ask_down != 1 or bid_down + ask_up != 1)
        tick = number(row['market']['tickSize'])
        spread = float((ask_up - bid_up) / tick)
        memory = self.memory.setdefault('spread:' + row['cell'], dict(bucket=None, values=[], medians=[]))
        start = int(now // 10000)
        if memory['bucket'] != start:
            if memory['values']:
                memory['medians'] = (memory['medians'] + [statistics.median(memory['values'])])[-360:]
            memory.update(bucket=start, values=[])
        memory['values'].append(spread)
        ctx['spread_spike'] = len(memory['medians']) >= 6 and spread > self.p.spread_spike_mult * statistics.median(memory['medians'])
        ctx['tight'] = spread <= self.p.tight_spread_ticks

    def targets(self, row, ctx, now):
        p, market, ph = self.p, row['market'], ctx['phase']
        if ph not in mm.OPENING + mm.HEDGE_ONLY:
            return {}
        tick, rate = number(market['tickSize']), float(market.get('feeRate') or p.rebate_fee_rate)
        minimum = number(market.get('minOrderNotional') or 0)
        net = number(row['qty'][0]) - number(row['qty'][1])
        gates, probation = self.gates(row, ctx, now)
        duration = row['spec']['t1'] - row['spec']['t0']
        out, decision = {}, dict(phase=ph, F=ctx['F'], M=ctx['M'], Z=ctx['Z'], d=ctx['d'], sigma=ctx['sigma'],
                                 tau=ctx['tau'], n=str(net), gates=gates, sides={})
        for side in (0, 1):
            hedge = net > 0 and side == 1 or net < 0 and side == 0
            fair_k = ctx['fair'][side]
            reason = None
            if not hedge and (ph not in mm.OPENING or gates):
                reason = gates[0] if gates and ph in mm.OPENING else 'phase_' + ph.lower()
            elif not hedge and not p.fair_min <= fair_k <= p.fair_max:
                reason = 'fair_range'
            elif not hedge and ctx.get('spread_spike'):
                reason = 'spread_spike'
            if reason:
                decision['sides'][side] = dict(reason=reason)
                self.count(now, 'skip_reasons', reason)
                continue
            widen = 0.0 if hedge else self.widen(row, ctx, side, now)
            h = mm.half_spread(p, fair_k, ctx['Z'], float(net), hedge, ph, ctx['tau'], duration, rate, widen)
            raw = mm.bid_price(fair_k, h, tick, p.round_tol_ticks)
            px, reason = mm.constrain(raw, hedge, *ctx['best'][side], tick, p)
            size, limit = (ZERO, ZERO) if px is None else self.size(row, side, net, hedge, ph, px, gates, probation, now)
            if px is not None and (size <= 0 or size < minimum):
                reason = 'size'
            decision['sides'][side] = dict(hedge=hedge, h=h, widen=widen, raw=str(raw), price=str(px) if px is not None else None,
                                           size=str(size), limit=str(limit), reason=reason)
            if reason:
                self.count(now, 'skip_reasons', reason)
                continue
            out[side] = (px, size, limit, hedge)
        row['decision'] = decision
        return out

    def gates(self, row, ctx, now):
        """Reasons that block opening (risk-increasing) quotes; hedges are always allowed."""
        p, reasons = self.p, []
        entry = self.state['selection'].get('active', {}).get(row['cell'])
        if entry is None:
            reasons.append('inactive')
        elif entry.get('mode') == 'preopen' and ctx['phase'] != mm.PREOPEN:
            reasons.append('preopen_only')
        probation = bool(entry) and (entry.get('source') != 'admitted' or day_key(now) < entry.get('probation_until', ''))
        if self.state['halt']:
            reasons.append('weekly_stop')
        if self.risk['hard']:
            reasons.append('hard_stop')
        if row['cell'] in self.risk['stopped']:
            reasons.append('cell_stop')
        if any(abs(ctx['t'] - event) <= p.event_blackout_min * 60 for event in p.events):
            reasons.append('event_blackout')
        return reasons, probation

    def widen(self, row, ctx, side, now):
        """Widening only (never narrowing) for an opening side, in price units."""
        p, tick = self.p, float(row['market']['tickSize'])
        extra = 0.0
        if now < row['flow_until'][side]:
            extra += p.flow_ticks * tick
        if now < row['fill_until'][side]:
            extra += p.fill_ticks * tick
        a = ctx['asset']
        adverse = a['momentum'] < -p.momentum_sigma if side == 0 else a['momentum'] > p.momentum_sigma
        if ctx['g'] > 0 and adverse and now - a['momentum_at'] <= p.spot_stale_ms:
            extra += tick
        if self.risk['soft']:
            extra += p.soft_ticks * tick
        return extra

    def size(self, row, side, net, hedge, ph, price, gates, probation, now):
        """(target size, risk limit) in shares; the hedge leg's |n| is never capped."""
        p = self.p
        mult = min([1.0] + [p.probation_size_mult] * probation + [p.soft_size_mult] * self.risk['soft']
                   + [p.fill_size_mult] * (now < row['fill_until'][side]))
        cap = p.n_cap_d * (Decimal(repr(p.probation_size_mult)) if probation else 1)
        base, absn = p.base_size_d * Decimal(repr(mult)), abs(net)
        if hedge:
            extra = ZERO
            if ph in mm.OPENING and not gates:  # Over-hedge only while openings are allowed.
                extra = min(max(ZERO, base - absn), cap, self.room(row, side, price))
            limit = absn + extra
            size = min(limit, p.max_order_d, number(self.account.state['free']) / price)
        else:
            limit = min(cap - absn, self.room(row, side, price))
            size = min(base, limit, p.max_order_d)
        return max(ZERO, size).quantize(Q2, rounding=ROUND_DOWN), max(ZERO, limit)

    # ----- order management --------------------------------------------------------
    def budget(self, row, now):
        row['actions'] = [x for x in row['actions'] if x > now - 1000]
        return len(row['actions']) < self.p.max_actions_per_market_s

    def place(self, row, side, price, size, ctx, now, kind='live', level=None, eligible=False):
        p, book = self.p, ctx['books'][side]
        if kind == 'live':
            size = min(size, (number(self.account.state['free']) / price).quantize(Q2, rounding=ROUND_DOWN))
            if size <= 0 or size < number(row['market'].get('minOrderNotional') or 0):
                return None
        shown = displayed(book, price)
        ahead = shown
        if kind == 'live' and ahead is not None:  # Our own earlier same-price orders queue ahead.
            ahead += sum((number(o['remaining']) for o in row['orders'] if o['kind'] == 'live' and o['side'] == side
                          and o['cancel_at'] is None and number(o['price']) == price), ZERO)
        reserve = price * size if kind == 'live' else ZERO
        if reserve:
            self.account.transfer('free', -reserve)
            self.account.transfer('locked', reserve)
        order = dict(id=self.next_id(row['market']['id']), kind=kind, side=side, level=level, price=str(price),
                     size=str(size), remaining=str(size), reserved=str(reserve), filled='0',
                     ahead=str(ahead) if ahead is not None else None, ahead_prop=str(ahead) if ahead is not None else None,
                     level_size=str(shown) if shown is not None else None, traded='0', prop_remaining=str(size),
                     queue0=str(ahead) if ahead is not None else None,
                     decided=now, live_from=now + p.place_latency_ms, cancel_at=None, accounted=None,
                     fair=ctx['fair'][side], improve=price > ctx['best'][side][0], eligible=eligible,
                     z=ctx['Z'], tau=ctx['tau'])
        row['orders'].append(order)
        self.changed(row)
        if kind == 'shadow':
            self.stats(now)['shadow_quotes'] += 1
            return order
        row['actions'].append(now)
        self.stats(now)['quotes'] += 1
        self.add(now, 'placed_' + ('up' if side == 0 else 'down'), size)
        self.account.event('maker_quote', now)
        self.audit('place', row['market']['id'], now, dict(order={k: order[k] for k in ('id', 'side', 'price', 'size', 'ahead', 'live_from', 'improve')},
                                                          decision=row['decision'], books=ctx['books']))
        return order

    def cancel(self, row, order, now, reason):
        """Cancel takes effect after cancel latency; prints before then still fill it."""
        if order['cancel_at'] is not None:
            return
        order.update(cancel_at=now + self.p.cancel_latency_ms, cancel_reason=reason)
        if order['kind'] == 'live':
            row['actions'].append(now)
            self.stats(now)['cancels'] += 1
            self.count(now, 'guard_cancels', reason)
            self.account.event('maker_cancel', now)
            self.audit('cancel', row['market']['id'], now, dict(order=order['id'], reason=reason, effective=order['cancel_at']))

    def release(self, order):
        if order['kind'] == 'live' and number(order['reserved']):
            self.account.transfer('locked', -number(order['reserved']))
            self.account.transfer('free', order['reserved'])
            order['reserved'] = '0'

    def finalize(self, row, now, force=False):
        """Drop filled orders and cancels whose effective time and print lag have passed."""
        keep = []
        settled = all(row['caught_up'])
        for o in row['orders']:
            done = number(o['remaining']) <= 0 or force or (o['cancel_at'] is not None and settled
                                                            and now >= o['cancel_at'] + self.p.print_lag_ms)
            if done:
                self.release(o)
            else:
                keep.append(o)
        if len(keep) != len(row['orders']):
            self.changed(row)
        row['orders'] = keep

    def account_quote_time(self, row, now):
        day = self.stats(now)
        for o in row['orders']:
            if o['kind'] != 'live' or now < o['live_from']:
                continue
            end = min(now, o['cancel_at']) if o['cancel_at'] is not None else now
            start = o['accounted'] or o['live_from']
            if end > start:
                key = 'quote_seconds_' + ('improve' if o['improve'] else 'join')
                day[key] = str(number(day[key]) + Decimal(end - start) / 1000)
                o['accounted'] = end

    def reconcile(self, row, targets, ctx, now):
        p, tick = self.p, number(row['market']['tickSize'])
        minimum = number(row['market'].get('minOrderNotional') or 0)
        for side in (0, 1):
            live = [o for o in row['orders'] if o['kind'] == 'live' and o['side'] == side
                    and o['cancel_at'] is None and number(o['remaining']) > 0]
            target = targets.get(side)
            if target is None:
                for o in live:
                    self.cancel(row, o, now, 'no_target' if ctx.get('ok') else ctx['reason'] or 'no_target')
                row['persist'][side] = None
                continue
            price, size, limit, hedge = target
            if not live:
                if now >= row['cooldown'][side] and self.budget(row, now):
                    self.place(row, side, price, self.fit(row, side, price, size, hedge), ctx, now)
                continue
            head, total = live[0], sum((number(o['remaining']) for o in live), ZERO)
            current = number(head['price'])
            guard = self.guard(row, head, ctx, side, now)
            if guard:
                for o in live:
                    self.cancel(row, o, now, guard)
                if guard == 'depth_pull':
                    row['cooldown'][side] = now + p.depth_pull_cooldown_ms
                continue
            if price > current:
                if not row['persist'][side] or number(row['persist'][side][0]) != price:
                    row['persist'][side] = [str(price), now]
            else:
                row['persist'][side] = None
            action = None
            if price < current:
                action = 'protect'  # Lower target: replace at once, ignoring minimum rest.
            elif (price >= current + tick and now - row['persist'][side][1] >= p.up_persist_ms
                  and now - head['live_from'] >= p.min_rest_ms):
                action = 'improve'
            elif size < Decimal(repr(p.resize_ratio)) * total or total > limit:
                action = 'resize'
            if action:
                if action != 'protect' and not self.budget(row, now):
                    continue
                for o in live:
                    self.cancel(row, o, now, action)
                self.stats(now)['replaces'] += 1
                if now >= row['cooldown'][side]:
                    self.place(row, side, price, self.fit(row, side, price, size, hedge), ctx, now)
            elif size >= total + max(minimum, Q2) and self.budget(row, now):
                extra = size - total if hedge else min(size, self.room(row, side, current)) - total
                if extra >= max(minimum, Q2):
                    self.place(row, side, current, extra, ctx, now)  # Same-price add-on keeps the queue.

    def fit(self, row, side, price, size, hedge):
        """Re-check risk room at placement: orders placed earlier this poll now count."""
        if hedge:
            return size
        size = min(size, self.room(row, side, price)).quantize(Q2, rounding=ROUND_DOWN)
        return size if size >= max(number(row['market'].get('minOrderNotional') or 0), Q2) else ZERO

    def guard(self, row, order, ctx, side, now):
        p = self.p
        edge = order['fair'] - float(order['price'])
        drop = order['fair'] - ctx['fair'][side]
        if drop > 0 and drop >= p.fv_adverse_frac * max(edge, float(row['market']['tickSize'])):
            return 'fv_adverse'
        ring = [r for r in row['ring'][side] if r[0] >= ctx['books'][side]['exchange_at'] - p.depth_pull_window_ms]
        if len(ring) >= 2:
            old_price, old_size = number(ring[0][2]), number(ring[0][3])
            shown = displayed(ctx['books'][side], old_price)  # None: level now outside visible depth, no evidence.
            traded = ZERO if shown is None else sum((number(x[2]) for x in row['recent'][side] if x[3] == 'SELL' and number(x[1]) <= old_price
                          and ring[0][0] <= x[0] <= ctx['books'][side]['exchange_at']), ZERO)
            if shown is not None and old_size > 0 and old_size - shown - traded >= Decimal(repr(p.depth_pull_frac)) * old_size:
                return 'depth_pull'
        return None

    def shadow(self, row, ctx, now):
        """Five-level zero-capital ladder around the n=0 target; independent queues."""
        p = self.p
        live = ctx.get('ok') and ctx['phase'] in mm.OPENING + mm.HEDGE_ONLY
        tick = number(row['market']['tickSize'])
        rate = float(row['market'].get('feeRate') or p.rebate_fee_rate)
        duration = row['spec']['t1'] - row['spec']['t0']
        quotable = False
        for side in (0, 1):
            wanted, eligible = {}, False
            if live:
                fair_k = ctx['fair'][side]
                h = mm.half_spread(p, fair_k, ctx['Z'], 0.0, False, mm.OPEN, ctx['tau'], duration, rate,
                                   self.widen(row, ctx, side, now))
                base = mm.bid_price(fair_k, h, tick, p.round_tol_ticks)
                bid, ask = ctx['best'][side]
                eligible = (ctx['phase'] in mm.OPENING and base >= bid and p.fair_min <= fair_k <= p.fair_max
                            and not ctx.get('spread_spike'))
                quotable = quotable or eligible
                for level in LEVELS:
                    price = base + level * tick
                    if 0 < price < ask:
                        wanted[level] = price
            existing = {o['level']: o for o in row['orders'] if o['kind'] == 'shadow' and o['side'] == side and o['cancel_at'] is None}
            for level, o in existing.items():
                if wanted.get(level) != number(o['price']):
                    self.cancel(row, o, now, 'shadow_move')
            for level, price in wanted.items():
                o = existing.get(level)
                if o is None or o['cancel_at'] is not None:
                    self.place(row, side, price, p.base_size_d, ctx, now, kind='shadow', level=level,
                               eligible=eligible and level == 0)
        ctx['quotable'] = quotable

    def sample(self, row, ctx, now):
        s = row['summary']
        if ctx['time_phase'] in (mm.WAIT, mm.FLAT):
            return
        s['polls'] += 1
        s['stale'] += int(ctx['reason'] in DATA_FAULTS)
        s['tight'] += int(bool(ctx.get('tight')))
        s['open'] += int(bool(ctx.get('ok')))
        s['quotable'] += int(bool(ctx.get('quotable')))
        spec = row['spec']
        every = max(30.0, (spec['t1'] - spec['t0']) / 50) * 1000
        if ctx.get('ok') and ctx['time_phase'] != mm.PREOPEN and (not row['calib'] or now - row['calib'][-1][0] >= every):
            row['calib'] = (row['calib'] + [[now, ctx['F'], ctx['M']]])[-60:]

    # ----- public prints, fills, ledger -------------------------------------------
    def prints(self, market_id, index, prints, now, complete=True):
        """Process recorded public prints for one token in receive order, exactly once."""
        row = self.state['markets'].get(market_id)
        if not row or row['settled']:
            return
        seen = dict(row['print_ids'][index])
        for pr in prints:
            identity = str(pr['recording_id'])
            if identity in seen:
                continue
            seen[identity] = pr['received_ms']
            row['cursor'][index] = max(row['cursor'][index], pr['received_ms'])
            self.observe_print(row, index, pr, now)
            self.match(row, index, pr, now)
        horizon = row['cursor'][index] - self.p.print_lag_ms - 1000
        row['print_ids'][index] = [[k, v] for k, v in seen.items() if v >= horizon]
        row['caught_up'][index] = complete

    def observe_print(self, row, index, pr, now):
        p, exchange = self.p, pr.get('exchange_ms')
        tx = pr.get('transaction_hash')
        row['txs'] = [x for x in row['txs'] if x[2] >= now - 15000]
        other = [x for x in row['txs'] if tx and x[0] == tx and x[1] != index]
        if other and pr.get('size') is not None:
            day = self.stats(now)
            day['complementary_checked'] += 1
            day['complementary_sum_one'] += int(number(other[0][3]) + number(pr['price']) == 1)
        elif pr.get('size') is not None:
            s = row['summary']
            s['taker_volume'] = str(number(s['taker_volume']) + number(pr['size']))
        if tx:
            row['txs'].append([tx, index, now, str(pr['price'])])
        if exchange is None or pr.get('side') != 'SELL':
            return
        recent = row['recent'][index]
        recent.append([exchange, str(pr['price']), str(pr.get('size') or 0), 'SELL', tx])
        recent[:] = [x for x in recent if x[0] >= exchange - max(p.flow_window_s, p.sweep_window_s, p.depth_pull_window_ms / 1000) * 1000][-200:]
        # Flow: last window's taker-sell volume vs the median of past same-length buckets.
        memory = self.memory.setdefault(f"flow:{row['market']['id']}:{index}", dict(bucket=None, volume=0.0, history=[]))
        start = int(exchange // (p.flow_window_s * 1000))
        if memory['bucket'] != start:
            if memory['bucket'] is not None:
                gaps = min(start - memory['bucket'] - 1, 180)
                memory['history'] = (memory['history'] + [memory['volume']] + [0.0] * gaps)[-int(p.flow_median_s / p.flow_window_s):]
            memory.update(bucket=start, volume=0.0)
        memory['volume'] += float(pr.get('size') or 0)
        window = sum(float(x[2]) for x in recent if x[0] >= exchange - p.flow_window_s * 1000)
        heavy = len(memory['history']) >= 6 and window > p.flow_mult * max(statistics.median(memory['history']), 1e-9)
        groups = {}
        for x in recent:
            if x[0] >= exchange - p.sweep_window_s * 1000:
                groups.setdefault(x[4] or x[0], set()).add(x[1])
        if heavy or any(len(prices) >= p.sweep_levels for prices in groups.values()):
            row['flow_until'][index] = max(row['flow_until'][index], now + p.flow_hold_s * 1000)

    def match(self, row, index, pr, now):
        """Pessimistic queue: at-price prints consume visible depth first; any print
        below our bid means the level was swept and fills our whole remainder."""
        exchange, size = pr.get('exchange_ms'), pr.get('size')
        if exchange is None or exchange > pr['received_ms'] or exchange >= row['spec']['t1'] * 1000:
            return
        size = number(size) if size is not None else None
        if size is not None and size <= 0:
            return
        if pr.get('side') == 'SELL':
            side, price, how = index, number(pr['price']), 'direct'
        elif pr.get('side') == 'BUY' and self.p.complementary == 'count':
            side, price, how = 1 - index, 1 - number(pr['price']), 'complementary'
        else:
            return
        tx = pr.get('transaction_hash')
        used = row.setdefault('used_txs', [])
        if tx and [tx, side] in [x[:2] for x in used]:
            return  # One taker transaction fills a given side once (complementary dedupe).
        hit = False
        for o in row['orders']:
            if (o['side'] != side or exchange < o['live_from'] or o['cancel_at'] is not None and exchange >= o['cancel_at']
                    or number(o['remaining']) <= 0):
                continue
            bid = number(o['price'])
            if price > bid:
                continue
            if price == bid:
                if size is None or o['ahead'] is None:
                    continue
                consumed = min(number(o['ahead']), size)
                o['ahead'] = str(number(o['ahead']) - consumed)
                o['traded'] = str(number(o['traded']) + size)
                available = size - consumed
                if o['ahead_prop'] is not None:
                    prop_used = min(number(o['ahead_prop']), size)
                    o['ahead_prop'] = str(number(o['ahead_prop']) - prop_used)
                    prop = min(size - prop_used, number(o['prop_remaining']))
                else:
                    prop = ZERO
            else:
                available, prop = number(o['remaining']), number(o['prop_remaining'])
            o['prop_remaining'] = str(max(ZERO, number(o['prop_remaining']) - prop))
            if o['kind'] == 'live' and prop:
                self.add(now, 'prop_filled', prop)  # Fills had cancels advanced the queue pro rata.
            qty = min(available, number(o['remaining'])).quantize(Q8, rounding=ROUND_DOWN)
            if qty > 0:
                self.fill(row, o, qty, pr, how, now)
                hit = True
        if hit and tx:
            row['used_txs'] = [x for x in used if x[2] >= now - 15000] + [[tx, side, now]]

    def pre_mid(self, row, side, exchange):
        before = [r for r in row['ring'][side] if r[0] < exchange]
        return before[-1][1] if before else None

    def fill(self, row, order, qty, pr, how, now):
        side, price, exchange = order['side'], number(order['price']), pr['exchange_ms']
        order['remaining'] = str(number(order['remaining']) - qty)
        order['filled'] = str(number(order['filled']) + qty)
        pre = self.pre_mid(row, side, exchange)
        record = dict(side=side, qty=str(qty), price=str(price), at=exchange, pre=pre if pre is not None else float(price), marks={})
        if order['kind'] == 'shadow':
            ahead = order['queue0']
            record.update(level=order['level'], eligible=order['eligible'], tau=mm.bucket(order['tau'], TAU_EDGES),
                          z=mm.bucket(order['z'], Z_EDGES), queue=mm.bucket(float(ahead), QUEUE_EDGES) if ahead is not None else None)
            row['shadow_fills'] = (row['shadow_fills'] + [record])[-300:]
            return
        self.changed(row)
        cost = qty * price
        order['reserved'] = str(number(order['reserved']) - cost)
        self.account.transfer('locked', -cost)
        self.account.transfer('positions', cost)
        row['qty'][side] = str(number(row['qty'][side]) + qty)
        row['cost'][side] = str(number(row['cost'][side]) + cost)
        row['lots'][side].append([exchange, str(qty)])
        row['filled'] = str(number(row['filled']) + qty)
        row['fills'] = (row['fills'] + [dict(record, how=how, improve=order['improve'])])[-500:]
        row['fill_until'][side] = now + self.p.fill_hold_s * 1000
        leg = 'up' if side == 0 else 'down'
        rate = number(row['market'].get('feeRate') or self.config['rebate_fee_rate'])
        self.add(now, 'filled_' + leg, qty)
        self.stats(now)['fill_events_' + leg] += 1
        self.add(now, 'filled_improve' if order['improve'] else 'filled_join', qty)
        self.add(now, 'notional_filled', cost)
        self.add(now, 'rebate_basis', qty * rate * price * (1 - price))
        if how == 'complementary':
            self.add(now, 'complementary_fills', qty)
        cs = self.cell_stats(now, row['cell'])
        cs['filled'] = str(number(cs['filled']) + qty)
        self.merge(row, exchange, now)
        self.account.event('maker_fill', now)
        self.audit('fill', row['market']['id'], now, dict(print=pr, order=order['id'], side=side, quantity=str(qty),
                   price=str(price), how=how, pre_mid=pre, maker_fee='0'))

    def merge(self, row, exchange, now):
        """Merge complete sets at once: each Up+Down pair redeems for 1."""
        paired = min(map(number, row['qty']))
        if not paired:
            return
        costs = [number(c) if paired == number(z) else
                 (number(c) * paired / number(z)).quantize(Decimal('.000000000000000001'), rounding=ROUND_DOWN)
                 for c, z in zip(row['cost'], row['qty'])]
        for i in range(2):
            row['qty'][i] = str(number(row['qty'][i]) - paired)
            row['cost'][i] = str(number(row['cost'][i]) - costs[i])
        self.account.transfer('positions', -sum(costs))
        self.account.transfer('free', paired)
        gain = paired - sum(costs)
        day = self.stats(now)
        left, durations = paired, []
        while left > 0 and row['lots'][0] and row['lots'][1]:  # FIFO lots: time from fill to hedge.
            a, b = row['lots'][0][0], row['lots'][1][0]
            take = min(number(a[1]), number(b[1]), left)
            if min(a[0], b[0]) > 0:
                durations.append([round((exchange - min(a[0], b[0])) / 1000, 3), str(take)])
            for lot, lots in ((a, row['lots'][0]), (b, row['lots'][1])):
                lot[1] = str(number(lot[1]) - take)
                if number(lot[1]) <= 0:
                    lots.pop(0)
            left -= take
        day['hedge_durations'] = (day['hedge_durations'] + durations)[-500:]
        self.add(now, 'paired_sets', paired)
        self.add(now, 'set_pnl', gain)
        self.add(now, 'net_pnl', gain)
        if row['flat'] is None:
            self.add(now, 'paired_before_flat', paired)
        row['pnl'] = str(number(row['pnl']) + gain)
        cs = self.cell_stats(now, row['cell'])
        cs['pnl'] = str(number(cs['pnl']) + gain)

    # ----- markouts, flat, settlement ----------------------------------------------
    def ring_mid(self, row, side, due):
        ring = row['ring'][side]
        before = [r for r in ring if r[0] <= due]
        if not before:
            return None
        stale = self.p.book_stale_ms_short if row['market']['interval'] in SHORT else self.p.book_stale_ms
        # Valid if fresh at due, or a later snapshot proves the book was unchanged until then.
        return before[-1][1] if due - before[-1][0] <= stale or len(before) < len(ring) else None

    def resolve_marks(self, row, now, final=False):
        end, lag, day = row['spec']['t1'] * 1000, self.p.print_lag_ms, self.stats(now)
        for live, records in ((True, row['fills']), (False, row['shadow_fills'])):
            for record in records:
                if record.get('done'):
                    continue
                for h in HORIZONS:
                    key, due = str(h), record['at'] + h * 1000
                    if key in record['marks'] or due >= end:
                        continue
                    if now < due + lag and not final:
                        break
                    record['marks'][key] = self.ring_mid(row, record['side'], due)
                    if live:
                        day['mark_samples' if record['marks'][key] is not None else 'mark_missing'] += 1
                else:
                    record['done'] = True

    def enter_flat(self, row, now):
        worst = sum(map(number, row['cost']))
        row['flat'] = dict(at=now, qty=list(row['qty']), fair=row['fair'] or [.5, .5], worst=str(worst))
        self.add(now, 'unpaired_at_flat', abs(number(row['qty'][0]) - number(row['qty'][1])))
        if worst > self.p.worst_loss_market_d / 2:
            day = self.stats(now)
            day['flat_red_flags'] = (day['flat_red_flags'] + [row['market']['id']])[-50:]

    def decompose(self, record, y_up, end):
        """q(Y-p) = q(M- - p) + q(M_h - M-) + q(Y - M_h) for the filled token; missing marks carry forward."""
        y = y_up if record['side'] == 0 else 1 - y_up
        q, price, pre = number(record['qty']), number(record['price']), Decimal(repr(record['pre']))
        marks, previous = {}, pre
        for h in HORIZONS:
            if record['at'] + h * 1000 >= end:
                value = y
            else:
                mark = record['marks'].get(str(h))
                value = Decimal(repr(mark)) if mark is not None else previous
            marks[h], previous = value, value
        return dict(qty=q, edge=q * (pre - price), **{f'as_{h}': q * (marks[h] - pre) for h in HORIZONS},
                    residual=q * (y - marks[int(MAIN_HORIZON)]), pnl=q * (y - price))

    def settle(self, market_id, outcome, now, estimated=False):
        row = self.state['markets'][market_id]
        spec = row['spec']
        if row['settled'] or now < spec['t1'] * 1000:
            return
        holding = any(number(q) for q in row['qty'])
        if outcome not in ('Up', 'Down', 'Tie') and (holding or now < spec['t1'] * 1000 + self.p.settle_timeout_s * 1000):
            return  # Unknown winner is never inferred; inventory waits for resolution.
        self.resolve_marks(row, now, final=True)
        self.finalize(row, now, force=True)
        y = {'Up': Decimal(1), 'Down': ZERO, 'Tie': Decimal('.5')}.get(outcome)
        day, end = self.stats(now), spec['t1'] * 1000
        cost = sum(map(number, row['cost']))
        payout = (number(row['qty'][0]) * y + number(row['qty'][1]) * (1 - y)) if y is not None else ZERO
        self.account.transfer('positions', -cost)
        self.account.transfer('free', payout)
        self.add(now, 'net_pnl', payout - cost)
        self.add(now, 'settlement_pnl', payout - cost)
        self.add(now, 'unpaired_settlement_loss', max(ZERO, cost - payout))
        row['pnl'] = str(number(row['pnl']) + payout - cost)
        cs = self.cell_stats(now, row['cell'])
        cs['pnl'] = str(number(cs['pnl']) + payout - cost)
        day['estimated_settlements'] += int(estimated)
        day['markets_settled'] += 1
        day['markets_with_fills'] += int(number(row['filled']) > 0)
        shadow = {str(level): dict(shares='0', pnl='0', edge='0', as60='0') for level in LEVELS}
        shadow['eligible'] = dict(shares='0', pnl='0', edge='0', as60='0')
        totals = dict(edge=ZERO, as60=ZERO)
        if y is None:
            day['unresolved_markets'] += 1
        else:
            for record in row['fills']:
                parts = self.decompose(record, y, end)
                for key in ('edge', 'residual', 'pnl') + tuple(f'as_{h}' for h in HORIZONS):
                    self.add(now, {'edge': 'fill_edge', 'pnl': 'decomposed_pnl'}.get(key, key), parts[key])
                self.add(now, 'decomposed_shares', parts['qty'])
                totals['edge'] += parts['edge']
                totals['as60'] += parts['as_60']
            cs['as60'] = str(number(cs['as60']) + totals['as60'])
            cs['as60_shares'] = str(number(cs['as60_shares']) + sum((number(r['qty']) for r in row['fills']), ZERO))
            for record in row['shadow_fills']:
                parts = self.decompose(record, y, end)
                for key in [str(record['level'])] + ['eligible'] * bool(record['eligible']):
                    bucket = shadow[key]
                    for name, value in (('shares', parts['qty']), ('pnl', parts['pnl']), ('edge', parts['edge']), ('as60', parts['as_60'])):
                        bucket[name] = str(number(bucket[name]) + value)
            for key, bucket in shadow.items():
                total = day['shadow'].setdefault(key, dict(shares='0', pnl='0', edge='0', as60='0', markets=0))
                for name in ('shares', 'pnl', 'edge', 'as60'):
                    total[name] = str(number(total[name]) + number(bucket[name]))
                total['markets'] += int(number(bucket['shares']) > 0)
            if row['flat']:
                fair = row['flat']['fair']
                flat = sum((number(q) * (value - Decimal(repr(f))) for q, f, value in
                            zip(row['flat']['qty'], fair, (y, 1 - y))), ZERO)
                self.add(now, 'flat_settlement_pnl', flat)
            for _, f, m in row['calib']:
                day['brier_model'] = str(number(day['brier_model']) + Decimal(repr((f - float(y)) ** 2)))
                day['brier_book'] = str(number(day['brier_book']) + Decimal(repr((m - float(y)) ** 2)))
                day['brier_n'] += 1
                cell = day['reliability'][min(9, int(f * 10))]
                cell[0] += 1
                cell[1] = str(number(cell[1]) + Decimal(repr(f)))
                cell[2] = str(number(cell[2]) + y)
        summary = dict(strategy=TAG, market_id=market_id, cell=row['cell'], end=end, outcome=outcome, estimated=estimated,
                       filled=row['filled'], pnl=row['pnl'], edge=str(totals['edge']), as60=str(totals['as60']),
                       shadow=shadow, unpaired_at_flat=str(abs(number(row['flat']['qty'][0]) - number(row['flat']['qty'][1]))) if row['flat'] else '0',
                       **row['summary'])
        self.audit('market_summary', market_id, now, summary)
        if row['shadow_fills'] and y is not None:
            self.audit('shadow_fills', market_id, now, dict(fills=[
                [r['level'], r['side'], r['qty'], r['price'], r['pre'], *[r['marks'].get(str(h)) for h in HORIZONS],
                 str(y if r['side'] == 0 else 1 - y), r['tau'], r['z'], r['queue'], r['eligible']] for r in row['shadow_fills']]))
        self.changed(row)
        row.update(settled=True, outcome=outcome, estimated=estimated, qty=['0', '0'], cost=['0', '0'], lots=[[], []],
                   fills=[], shadow_fills=[], orders=[], ring=[[], []], recent=[[], []], print_ids=[[], []], calib=[])
        self.account.event('maker_settlement', now)
        self.audit('settlement', market_id, now, dict(outcome=outcome, estimated=estimated, cost=str(cost), payout=str(payout)))

    # ----- lifecycle, selection, reporting -----------------------------------------
    def restart(self, now):
        """Orders resting when the worker died count as live until a cancel could land."""
        for row in self.state['markets'].values():
            for o in row['orders']:
                self.cancel(row, o, now, 'restart')

    def shutdown(self, now):
        for row in self.state['markets'].values():
            for o in row['orders']:
                self.cancel(row, o, now, 'shutdown')

    def cleanup(self):
        for identity, row in list(self.state['markets'].items()):
            if row['settled']:
                del self.state['markets'][identity]
                self.changed(row)
                for key in [k for k in self.memory if k.startswith(f'flow:{identity}:')]:
                    del self.memory[key]

    def review_due(self, now):
        moment = utc(now)
        return (self.state['selection'].get('reviewed') != day_key(now)
                and moment.hour * 60 + moment.minute >= self.p.review_minutes)

    def review(self, summaries, now):
        previous = self.state['selection']
        self.state['selection'] = select(summaries, previous, self.p, now)
        self.audit('selection', 'selection', now, dict(active=self.state['selection']['active'],
                                                       cooloff=self.state['selection']['cooloff'],
                                                       scores=self.state['selection']['scores']))

    def report(self, now, current=True):
        stored = self.stats(now) if current else self.state['days'].get(day_key(now), {})
        if stored.get('strategy', TAG) != TAG:
            return None  # v1/v2 days keep the report written by their own release.
        s = {**day_template(), **copy.deepcopy(stored)}
        n = lambda key: number(s.get(key, '0'))
        ratio = lambda a, b: str(a / b) if b else None
        filled = n('filled_up') + n('filled_down')
        shares = n('decomposed_shares')
        s.update(config=self.config, config_hash=self.hash, inventory_units='unpaired_shares', maker_fee='0')
        for leg in ('up', 'down'):
            s['fill_rate_' + leg] = ratio(n('filled_' + leg), n('placed_' + leg))
        for kind in ('improve', 'join'):
            s['fills_per_quote_hour_' + kind] = ratio(n('filled_' + kind), n('quote_seconds_' + kind) / 3600)
        s['net_pnl_per_market'] = ratio(n('net_pnl'), Decimal(s['markets_with_fills']))
        s['net_pnl_per_completed_set'] = ratio(n('set_pnl'), n('paired_sets'))
        for key in ('fill_edge', 'residual', 'decomposed_pnl') + tuple(f'as_{h}' for h in HORIZONS):
            s[key + '_per_share'] = ratio(n(key), shares)
        s['as60_edge_ratio'] = str(-n('as_60') / n('fill_edge')) if n('fill_edge') > 0 else None
        s['neutralization_rate'] = ratio(2 * n('paired_before_flat'), filled)
        durations, acc = sorted((float(d), float(q)) for d, q in s['hedge_durations']), 0.0
        s['inventory_half_life_s'] = None  # Share-weighted median time from fill to hedge.
        for duration, quantity in durations:
            acc += quantity
            if acc >= sum(q for _, q in durations) / 2:
                s['inventory_half_life_s'] = duration
                break
        locked = n('locked_sum') / s['locked_count'] if s['locked_count'] else ZERO
        s['locked_avg'] = str(locked)
        s['capital_return'] = ratio(n('net_pnl'), locked)
        s['turnover'] = ratio(n('notional_filled'), locked)
        s['queue_sensitivity'] = dict(pessimistic_filled=str(filled), proportional_extra_filled=s['prop_filled'])
        s['brier_model_mean'] = ratio(n('brier_model'), Decimal(s['brier_n']))
        s['brier_book_mean'] = ratio(n('brier_book'), Decimal(s['brier_n']))
        full = n('rebate_basis') * number(self.config['rebate_pool_fraction'])
        s['rebate_scenarios'] = {'0%': '0', '50%': str(full / 2), '100%': str(full)}
        s['rebate_scenario_only'] = str(full)
        s['estimated_rebate'] = None  # Unknown daily pool/share; never fabricate earned income.
        s['net_pnl_including_estimated_rebate'] = s['net_pnl']
        s['rebate_status'] = 'unknown_pool_not_credited'
        s['fee_source'], s['rebate_source'] = FEE_SOURCE, REBATE_SOURCE
        if current:
            s['selection'] = copy.deepcopy(self.state['selection'])
            s['halt'] = self.state['halt']
        return s


class RecordedFeed:
    """Bounded SQL polling in the account worker; readers never wait for this."""
    LIMIT = 500

    def __init__(self, account, account_id='global'):
        self.maker = Maker(account)
        self.account_id = account_id
        self.maker.restart(int(time.time() * 1000))

    def poll(self, pipeline, now, quoting=True):
        from app.utils.db import get_db_connection
        from . import observer
        m = self.maker
        with pipeline.lock:
            listed = [x for x in pipeline.markets.values() if m.in_universe(x)][:200]
        known = m.state['markets']
        markets = {x['id']: x for x in listed} if quoting else {}
        markets.update({k: r['market'] for k, r in known.items() if not r['settled']})
        provider = observer._REFERENCE
        for asset in m.p.assets:
            ref = provider.get(asset) if provider else {}
            observed = timestamp(ref['observed_at']).timestamp() * 1000 if ref.get('observed_at') else None
            m.observe_spot(asset, ref.get('price'), observed, now)
        m.begin(now)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                tokens = [x[k] for x in markets.values() for k in ('yesAssetId', 'noAssetId')]
                cur.execute('SELECT b.* FROM unnest(%s::text[]) AS t(token) CROSS JOIN LATERAL (SELECT token_id,bids,asks,exchange_at,received_at FROM qd_polymarket_observer_books WHERE token_id=t.token ORDER BY received_at DESC LIMIT 1) b', (tokens,))
                latest = {}
                for b in cur.fetchall():
                    b = dict(b)
                    b['exchange_at'] = b['exchange_at'].timestamp() * 1000 if b['exchange_at'] else None
                    latest[b['token_id']] = b
                for market in markets.values():
                    m.step(market, [latest.get(market[k]) for k in ('yesAssetId', 'noAssetId')], now, quoting)
                active = {k: r for k, r in known.items() if not r['settled']}
                pairs = [(k, i, r['market'][key], r['cursor'][i]) for k, r in active.items()
                         for i, key in enumerate(('yesAssetId', 'noAssetId'))]
                grouped = {}
                if pairs:
                    cur.execute('SELECT p.* FROM unnest(%s::text[],%s::timestamptz[]) AS c(token,since) CROSS JOIN LATERAL (SELECT recording_id,token_id,price,size,side,exchange_at,received_at,transaction_hash FROM qd_polymarket_observer_trades WHERE token_id=c.token AND received_at>c.since ORDER BY received_at,recording_id LIMIT %s) p',
                                ([x[2] for x in pairs], [utc(x[3] - m.p.print_lag_ms) for x in pairs], self.LIMIT + 1))
                    for trade in cur.fetchall():
                        grouped.setdefault(trade['token_id'], []).append(dict(trade, received_ms=trade['received_at'].timestamp() * 1000,
                            exchange_ms=trade['exchange_at'].timestamp() * 1000 if trade['exchange_at'] else None))
                for identity, index, token, _ in pairs:
                    batch = grouped.get(token, [])
                    m.prints(identity, index, batch[:self.LIMIT], now, complete=len(batch) <= self.LIMIT)
                for identity, row in active.items():
                    if now >= row['spec']['t1'] * 1000 and all(row['caught_up']):
                        outcome, estimated = self.outcome(cur, row, now)
                        m.settle(identity, outcome, now, estimated)
                if m.review_due(now):
                    cur.execute("SELECT detail FROM qd_polymarket_maker_events WHERE account_id=%s AND strategy=%s AND event='market_summary' AND observed_at>=%s",
                                (self.account_id, TAG, utc(now - (int(m.p.lookback_days) + 1) * 86400000)))
                    m.review([r['detail'] for r in cur.fetchall()], now)
            finally:
                cur.close()
        m.cleanup()

    @staticmethod
    def outcome(cur, row, now=None):
        market, identity = row['market'], row['market']['id']
        cur.execute('SELECT official_outcome FROM qd_polymarket_observer_windows WHERE market_id=%s', (identity,))
        resolution = cur.fetchone()
        if resolution and resolution['official_outcome']:
            return resolution['official_outcome'], False
        # Recorded Binance observations nearest each settlement print, no lookahead.
        # Daily rules print a minute after each boundary, after this market's own
        # samples stop: wait for the print, then read any same-asset sample.
        lag = mm.print_lag(market) if (row['spec'] or {}).get('source') == 'binance' else 0
        if lag and (now is None or now < (row['spec']['t1'] + lag) * 1000):
            return None, False
        values = []
        for boundary in (market['eventStartTime'], market['endDate']):
            if lag:
                boundary = utc(timestamp(boundary).timestamp() * 1000 + lag * 1000)
                where = "asset=%s AND observed_at>=%s::timestamptz - INTERVAL '10 seconds' AND observed_at<%s::timestamptz + INTERVAL '5 minutes'"
                params = (market['asset'], boundary, boundary)
            else:
                where, params = 'market_id=%s', (identity,)
            cur.execute(f"SELECT reference_price FROM qd_polymarket_observer_samples WHERE {where} AND reference_source IN ('api.binance.com','data-api.binance.vision') AND reference_observed_at<=%s AND reference_observed_at>=%s::timestamptz - INTERVAL '10 seconds' ORDER BY reference_observed_at DESC LIMIT 1", (*params, boundary, boundary))
            sample = cur.fetchone()
            values.append(number(sample['reference_price']) if sample else None)
        if any(v is None for v in values):
            return None, False
        if values[1] == values[0] and row['spec'] and row['spec']['tie'] != 1:
            return 'Tie', True
        return ('Up' if values[1] >= values[0] else 'Down'), True

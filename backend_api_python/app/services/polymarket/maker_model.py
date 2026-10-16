"""Pure v3 maker math: settlement spec, fair value, toxicity, phases and quote prices.

No I/O and no clock. Probability math uses floats; callers keep the ledger in Decimal.
Times are epoch seconds unless a name ends in _ms.
"""
import math
import random
import re
from decimal import Decimal, ROUND_FLOOR

from .updown import timestamp

UP, DOWN = 0, 1
WAIT, PREOPEN, STRIKE, OPEN, REDUCE, HEDGE, FLAT = 'WAIT', 'PREOPEN', 'STRIKE', 'OPEN', 'REDUCE', 'HEDGE', 'FLAT'
OPENING = (PREOPEN, OPEN)
HEDGE_ONLY = (STRIKE, REDUCE, HEDGE)
SECONDS_PER_YEAR = 365 * 86400


def parse_rules(market):
    """SettlementSpec from the market's own rule text, never from its interval label.

    Chainlink-stream markets average the settlement source over a 60 s window
    (w=60); Binance candle markets use single prints (w=0). t0/t1 are the strike
    and final prints: the window boundaries, or `lag` seconds after them when the
    rules compare 1-minute candle closes (daily). The book keeps trading past
    endDate until resolution, so every time-based step keys off the prints.
    Ambiguous text -> None.
    """
    def source(text):
        text = text.lower()
        chainlink, binance = 'chain.link' in text or 'chainlink' in text, 'binance' in text
        return None if chainlink == binance else 'chainlink' if chainlink else 'binance'
    kind = source(str(market.get('resolutionSource') or '')) or source(str(market.get('description') or ''))
    try:
        t0, t1 = timestamp(market['eventStartTime']).timestamp(), timestamp(market['endDate']).timestamp()
    except (KeyError, ValueError):
        return None
    if not kind or t1 <= t0 + 60:
        return None
    text = (str(market.get('description') or '') + ' ' + str(market.get('question') or '')).lower()
    tie = .5 if '50-50' in text or '50/50' in text else 1.0  # value of Up when X == K
    lag = print_lag(market) if kind == 'binance' else 0
    return dict(source=kind, w=60.0 if kind == 'chainlink' else 0.0, t0=t0 + lag, t1=t1 + lag, lag=lag, tie=tie)


def print_lag(market):
    """Seconds from a window boundary to its settlement print, from the rule text.

    Daily Binance rules compare 1-minute candle closes at noon ET, a minute after
    each boundary; hourly candle open/close and Chainlink streams print at them.
    """
    text = (str(market.get('description') or '') + ' ' + str(market.get('question') or '')).lower()
    return 60 if re.search(r'\b1[ -]?minute candle', text) else 0


def weight(spec, t):
    """g(t): share of a price shock at t that reaches D = ln X - ln K."""
    t0, t1, w = spec['t0'], spec['t1'], spec['w']
    if t < t0 - w or t >= t1:
        return 0.0
    if t < t0:
        return (t - t0 + w) / w
    if t < t1 - w:
        return 1.0
    return (t1 - t) / w


def variance(spec, t):
    """V_D / sigma^2 = integral of g(s)^2 over [t, T1], closed form."""
    t0, t1, w = spec['t0'], spec['t1'], spec['w']
    if t >= t1:
        return 0.0
    if w == 0:
        return t1 - max(t, t0)
    if t < t0:
        u = max(0.0, t - t0 + w)
        return (w ** 3 - u ** 3) / (3 * w * w) + (t1 - t0 - w) + w / 3
    if t < t1 - w:
        return (t1 - w - t) + w / 3
    return (t1 - t) ** 3 / (3 * w * w)


def cdf(x):
    return .5 * (1 + math.erf(x / math.sqrt(2)))


def pdf(x):
    return math.exp(-x * x / 2) / math.sqrt(2 * math.pi)


def fair(spec, t, z, a_start, a_end, sigma):
    """(F_model, d, V_D). a_start/a_end: realized window parts (integral / w, or ln K)."""
    v = sigma * sigma * variance(spec, t)
    mu = a_end - a_start + weight(spec, t) * z
    if v <= 0:
        return (1.0 if mu > 0 else 0.0 if mu < 0 else spec['tie']), 0.0, 0.0
    d = mu / math.sqrt(v)
    return cdf(d), d, v


def toxicity(spec, t, d, v, sigma_short, latency):
    """Z: std of the fair-value move inside our effective exposure latency."""
    g = weight(spec, t)
    if not g:
        return 0.0
    if v <= 0:
        return math.inf
    return pdf(d) * abs(g) * sigma_short * math.sqrt(latency) / math.sqrt(v)


def window_integral(samples, a, b, gap):
    """Trapezoid integral of z over [a, b] from sorted (t, z); None without coverage.

    Coverage: no two observations (including the window edges) more than `gap`
    seconds apart. The last value is held to `b`; future prices are never used.
    """
    if b <= a:
        return 0.0
    points = [(t, z) for t, z in samples if t <= b]
    before = [p for p in points if p[0] <= a]
    inside = [p for p in points if a < p[0]]
    if before and a - before[-1][0] <= gap:
        start = before[-1]
        if inside:  # Interpolate the value at a.
            (t1, z1) = inside[0]
            start = (a, start[1] + (z1 - start[1]) * (a - start[0]) / (t1 - start[0]))
        else:
            start = (a, start[1])
    elif inside and inside[0][0] - a <= gap:
        start = (a, inside[0][1])
    else:
        return None
    path = [start] + inside + [(b, (inside or [start])[-1][1])]
    if any(t2 - t1 > gap for (t1, _), (t2, _) in zip(path, path[1:])):
        return None
    return sum((t2 - t1) * (z1 + z2) / 2 for (t1, z1), (t2, z2) in zip(path, path[1:]))


def fee(price, rate):
    return rate * price * (1 - price)


def microprice(book):
    """Top-of-book size-weighted mid of one outcome token."""
    bid, ask = book['bids'][0], book['asks'][0]
    pb, qb, pa, qa = float(bid['price']), float(bid['size']), float(ask['price']), float(ask['size'])
    return (pb * qa + pa * qb) / (qa + qb) if qa + qb > 0 else (pb + pa) / 2


def phase(spec, t, thresholds, preopen_window, pull, strike_wait):
    """Time-only phase; data, Z and risk gates are layered on by the caller."""
    tau = spec['t1'] - t
    reduce, hedge, flat = thresholds
    if tau <= flat:
        return FLAT
    if tau <= hedge:
        return HEDGE
    if tau <= reduce:
        return REDUCE
    start = spec['t0'] - spec['w']
    if t < start - preopen_window:
        return WAIT
    if t < start - pull:
        return PREOPEN
    if t < spec['t0'] + strike_wait:
        return STRIKE
    return OPEN


def time_mult(tau, duration, cap):
    """g_tau = clip(sqrt(tau_ref / tau), 1, cap), tau_ref = half the period."""
    return cap if tau <= 0 else min(cap, max(1.0, math.sqrt(duration / 2 / tau)))


def half_spread(p, fair_k, z, n, hedge, ph, tau, duration, rate, widen=0.0):
    """h_k per v3: toxicity floor, fee moat credit, inventory skew, widening."""
    moat = p.lam_fee * fee(fair_k, rate)
    base = max(p.h_min, p.k_tox * z - moat + p.c0)
    s = p.kappa_inv * min(abs(n) / p.n_cap, 1.0) * time_mult(tau, duration, p.time_mult_cap)
    if hedge:
        floor = -p.e_neg if ph == HEDGE else max(0.0, p.k_hedge * z - moat)
        return max(floor, base - s)
    return base + s + widen


def bid_price(fair_k, h, tick, tolerance):
    """floor_tick(fair - h + tolerance * tick) as an exact Decimal tick multiple."""
    steps = Decimal(repr((fair_k - h) / float(tick) + tolerance)).to_integral_value(rounding=ROUND_FLOOR)
    return steps * tick


def constrain(px, hedge, best_bid, best_ask, tick, p):
    """Post-only, bounded lag behind the best bid (hedge and opening sides). Returns (px, reason)."""
    px = min(px, best_ask - tick)
    if hedge:
        if px < best_bid - p.hedge_max_behind_ticks * tick:
            return None, 'hedge_too_far'
    elif px < best_bid - p.open_max_behind_ticks * tick:
        return None, 'behind_best'
    px = min(px, best_bid + p.max_improve_ticks * tick)
    return (px, None) if px > 0 else (None, 'nonpositive')


def bucket(value, edges):
    """Index of the first edge >= value (len(edges) when above all)."""
    return next((i for i, edge in enumerate(edges) if value <= edge), len(edges))


def weighted_mean(samples):
    den = sum(w for _, w in samples)
    return sum(v * w for v, w in samples) / den if den else 0.0


def cluster_bootstrap(samples, iters, alpha, seed):
    """One-sided lower confidence bound of a weighted mean, resampling whole clusters.

    samples: iterable of (cluster, value, weight). Deterministic for a seed.
    """
    clusters = {}
    for cluster, value, w in samples:
        num, den = clusters.get(cluster, (0.0, 0.0))
        clusters[cluster] = (num + value * w, den + w)
    keys = sorted(clusters)
    if not keys:
        return None
    nums = [clusters[k][0] for k in keys]
    dens = [clusters[k][1] for k in keys]
    try:
        import numpy as np
        rng = np.random.default_rng(seed)
        picks = rng.integers(0, len(keys), size=(iters, len(keys)))
        num, den = np.asarray(nums)[picks].sum(axis=1), np.asarray(dens)[picks].sum(axis=1)
        means = sorted(float(x) for x in np.divide(num, den, out=np.zeros_like(num), where=den > 0))
    except ImportError:  # pragma: no cover - numpy is a locked dependency
        rng, means = random.Random(seed), []
        for _ in range(iters):
            picks = [rng.randrange(len(keys)) for _ in keys]
            den = sum(dens[i] for i in picks)
            means.append(sum(nums[i] for i in picks) / den if den else 0.0)
        means.sort()
    return means[max(0, math.ceil(alpha * iters) - 1)]

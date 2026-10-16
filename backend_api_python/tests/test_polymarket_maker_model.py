"""v3 pure maker math: settlement spec, fair value, toxicity, quote bounds."""
import math
import random
from decimal import Decimal as D

import pytest

from app.services.polymarket import maker_model as mm
from app.services.polymarket.maker import DEFAULTS, params

P = params(dict(DEFAULTS))
P_DOC = params(dict(DEFAULTS, k_tox='3', open_max_behind_ticks='0'))  # Original v3 table.
SIGMA = .5 / math.sqrt(mm.SECONDS_PER_YEAR)


def spec(w=60.0, t0=1000.0, t1=1300.0, tie=1.0):
    return dict(source='chainlink' if w else 'binance', w=w, t0=t0, t1=t1, tie=tie)


def numeric_variance(s, t, steps=200000):
    """Midpoint rule on each smooth piece of g^2; exact for these polynomials to ~1e-12."""
    edges = sorted({t, max(t, s['t0'] - s['w']), max(t, s['t0']), max(t, s['t1'] - s['w']), s['t1']})
    total = 0.0
    for a, b in zip(edges, edges[1:]):
        h = (b - a) / steps
        total += sum(mm.weight(s, a + (i + .5) * h) ** 2 for i in range(steps)) * h
    return total


def test_rules_text_decides_source_window_and_tie():
    base = dict(eventStartTime='2026-10-16T10:00:00Z', endDate='2026-10-16T10:15:00Z', interval='15m')
    chain = mm.parse_rules(dict(base, resolutionSource='https://data.chain.link/streams/btc-usd'))
    assert chain['source'] == 'chainlink' and chain['w'] == 60 and chain['t1'] - chain['t0'] == 900
    hourly = mm.parse_rules(dict(base, resolutionSource='', description='Resolves with the Binance BTC/USDT 1H candle.'))
    assert hourly['source'] == 'binance' and hourly['w'] == 0 and hourly['tie'] == 1
    daily = mm.parse_rules(dict(base, resolutionSource='https://www.binance.com/en/trade/BTC_USDT',
                                description='If the prices are equal the market resolves 50-50.'))
    assert daily['tie'] == .5
    # The interval label never substitutes for unreadable rules.
    assert mm.parse_rules(dict(base, resolutionSource='', description='')) is None
    assert mm.parse_rules(dict(base, resolutionSource='', description='chainlink or binance')) is None



def test_print_lag_only_for_one_minute_candle_closes():
    daily = ('This market will resolve to "Up" if the "Close" price for the Binance 1 minute candle for '
             'BTC/USDT Oct 15 \'26 12:00 in the ET timezone (noon) is lower than the final "Close" price '
             'for the Oct 16 \'26 12:00 ET candle. If the final "Close" price for both of these candles is '
             'exactly equal on Binance, this market will resolve 50-50.')
    hourly = ('This market will resolve to "Up" if the close price is greater than or equal to the open price '
              'for the BTC/USDT 1 hour candle that begins on the time and date specified in the title.')
    assert mm.print_lag(dict(description=daily)) == 60
    assert mm.print_lag(dict(description='Uses the Binance 1-minute candle close.')) == 60
    assert mm.print_lag(dict(description=hourly)) == 0
    assert mm.print_lag(dict(description='Chainlink BTC/USD TWAP data stream.')) == 0
    assert mm.print_lag(dict(description='the 11 minute candle')) == 0
    assert mm.print_lag({}) == 0

def test_daily_candle_close_rules_key_strike_variance_and_phases_to_the_prints():
    from app.services.polymarket.updown import timestamp
    base = dict(eventStartTime='2026-10-15T16:00:00Z', endDate='2026-10-16T16:00:00Z', interval='1d',
                resolutionSource='https://www.binance.com/en/trade/BTC_USDT',
                description='The "Close" price for the Binance 1 minute candle for BTC/USDT 12:00 ET. '
                            'If both candles are equal this market will resolve 50-50.')
    t0, t1 = (timestamp(base[k]).timestamp() for k in ('eventStartTime', 'endDate'))
    s = mm.parse_rules(base)
    assert (s['source'], s['w'], s['t0'], s['t1'], s['lag'], s['tie']) == ('binance', 0, t0 + 60, t1 + 60, 60, .5)
    # Hourly candles and Chainlink streams print at the boundaries, whatever else the text says.
    hourly = mm.parse_rules(dict(base, endDate='2026-10-15T17:00:00Z', description='BTC/USDT 1 hour candle.'))
    chain = mm.parse_rules(dict(base, resolutionSource='https://data.chain.link/streams/btc-usd'))
    assert all((x['lag'], x['t0']) == (0, t0) for x in (hourly, chain)) and chain['t1'] == t1
    # Spot moves before the strike prints shift K and X alike.
    assert mm.weight(s, t0 + 59) == 0 and mm.weight(s, t0 + 60) == 1
    assert mm.fair(s, t0 + 30, 5.0, 0.0, 0.0, SIGMA)[0] == .5
    # The minute after endDate is still price risk.
    assert mm.variance(s, t1 - 100) == 160 and mm.variance(s, t1) == 60 and mm.variance(s, t1 + 60) == 0
    th = P.phases['1d']
    at = lambda t: mm.phase(s, t, th, P.preopen['1d'], P.preopen_pull_s, P.strike_wait_s)
    assert [at(t) for t in (t0 - 3541, t0 + 30, t0 + 56, t0 + 63, t1 - 241, t1 - 240, t1 - 1, t1)] == [
        mm.WAIT, mm.PREOPEN, mm.STRIKE, mm.OPEN, mm.REDUCE, mm.HEDGE, mm.HEDGE, mm.FLAT]


@pytest.mark.parametrize('z', [-3.0, 0.0, 11.0, 50.0])
def test_before_strike_window_fair_half_and_zero_toxicity_for_any_spot(z):
    s = spec()
    for t in (0.0, 500.0, 939.999):
        f, d, v = mm.fair(s, t, z, 0.0, 0.0, SIGMA)
        assert f == .5 and d == 0
        assert mm.toxicity(s, t, d, v, SIGMA * 10, .3) == 0
    zero = spec(w=0)
    assert mm.fair(zero, 999.0, z, 0.0, 0.0, SIGMA)[0] == .5


def test_variance_closed_form_matches_integral_and_binance_limit():
    s = spec()
    for t in (900.0, 945.0, 990.0, 1000.0, 1100.0, 1240.0, 1250.0, 1299.0):
        assert abs(mm.variance(s, t) - numeric_variance(s, t)) < 1e-9
    zero = spec(w=0)
    for t in (1000.0, 1100.0, 1299.5):
        assert mm.variance(zero, t) == pytest.approx(1300 - t)
        f, d, v = mm.fair(zero, t, math.log(60010), math.log(60000), 0.0, SIGMA)
        assert v == pytest.approx(SIGMA ** 2 * (1300 - t))
        z = mm.toxicity(zero, t, d, v, SIGMA, .3)
        assert z == pytest.approx(mm.pdf(d) * math.sqrt(.3 / (1300 - t)))


def test_fair_monotone_in_spot_and_symmetric_about_strike():
    s, k = spec(w=0), math.log(60000)
    values = [mm.fair(s, 1100.0, k + x, k, 0.0, SIGMA)[0] for x in (-.003, -.001, 0, .0005, .002)]
    assert values == sorted(values) and values[2] == .5
    for x in (.0001, .001, .004):
        up = mm.fair(s, 1100.0, k + x, k, 0.0, SIGMA)[0]
        down = mm.fair(s, 1100.0, k - x, k, 0.0, SIGMA)[0]
        assert up + down == pytest.approx(1, abs=1e-12)
    # Inside the settlement window realized averages shift the mean.
    w = spec()
    assert mm.fair(w, 1270.0, k, k, .5 * (k + .001), SIGMA)[0] > .5


def test_window_integral_trapezoid_and_coverage():
    samples = [(0.0, 1.0), (10.0, 2.0), (20.0, 2.0)]
    assert mm.window_integral(samples, 0, 20, 12) == pytest.approx(35.0)
    assert mm.window_integral(samples, 5, 15, 12) == pytest.approx(1.75 * 5 + 2 * 5, rel=1e-9)
    assert mm.window_integral(samples, 0, 30, 12) == pytest.approx(55.0)  # Last value held, never the future.
    assert mm.window_integral(samples, 0, 40, 12) is None
    assert mm.window_integral([(30.0, 1.0)], 0, 40, 12) is None


@pytest.mark.parametrize('seed', range(5))
def test_zero_inventory_bids_sum_at_most_one_minus_two_hmin(seed):
    rng, tick = random.Random(seed), D('.01')
    for _ in range(300):
        f, m = rng.uniform(.05, .95), rng.uniform(.05, .95)
        fair = [min(f, m), min(1 - f, 1 - m)]
        z = rng.uniform(0, .03)
        bids = [mm.bid_price(x, mm.half_spread(P, x, z, 0.0, False, mm.OPEN, 300, 900, .07), tick, P.round_tol_ticks)
                for x in fair]
        assert sum(bids) <= 1 - 2 * D(DEFAULTS['h_min'])


def test_rounding_tolerance_and_post_only_improve_bounds():
    tick = D('.01')
    assert mm.bid_price(.4999, .01, tick, .2) == D('.49')  # 0.4899 is not floored to 0.48.
    rng = random.Random(3)
    for _ in range(500):
        bid = D(rng.randrange(5, 90)) / 100
        ask = bid + D(rng.randrange(1, 6)) / 100
        px = D(rng.randrange(1, 99)) / 100
        out, why = mm.constrain(px, False, bid, ask, tick, P_DOC)
        if out is not None:
            assert bid <= out <= min(ask - tick, bid + tick)
        else:
            assert why == 'behind_best' and min(px, ask - tick) < bid
        hedge, _ = mm.constrain(px, True, bid, ask, tick, P_DOC)
        if hedge is not None:
            assert bid - 3 * tick <= hedge <= ask - tick


def test_inventory_skew_monotone_risk_widens_hedge_narrows():
    risk = [mm.half_spread(P_DOC, .5, .002, n, False, mm.OPEN, 600, 900, .07) for n in (0, 5, 10, 20, 40)]
    hedge = [mm.half_spread(P_DOC, .5, .002, n, True, mm.OPEN, 600, 900, .07) for n in (0, 5, 10, 20, 40)]
    assert risk == sorted(risk) and risk[3] == risk[4]
    assert hedge == sorted(hedge, reverse=True)
    # Hedge floor: max(0, k_hedge Z - fee) normally; -e_neg (at fair) in the hedge phase.
    # base = max(.01, 3*.02 - .0175) = .0425; skew = .02 * g_tau.
    assert mm.half_spread(P_DOC, .5, .02, 20, True, mm.REDUCE, 600, 900, .07) == pytest.approx(.0425 - .02)
    assert mm.half_spread(P_DOC, .5, .02, 20, True, mm.REDUCE, 100, 900, .07) == pytest.approx(1.5 * .02 - .07 * .25)
    assert mm.half_spread(P_DOC, .5, .02, 20, True, mm.HEDGE, 60, 900, .07) == 0
    assert mm.time_mult(450, 900, 3) == 1 and mm.time_mult(50, 900, 3) == 3


def test_phase_thresholds_by_interval():
    s, th = spec(t0=1000.0, t1=1300.0), P.phases['5m']
    at = lambda t: mm.phase(s, t, th, P.preopen['5m'], P.preopen_pull_s, P.strike_wait_s)
    assert [at(t) for t in (700, 830, 936, 1001, 1100, 1185, 1245, 1285)] == [
        mm.WAIT, mm.PREOPEN, mm.STRIKE, mm.STRIKE, mm.OPEN, mm.REDUCE, mm.HEDGE, mm.FLAT]


def test_cluster_bootstrap_deterministic_lower_bound():
    rng = random.Random(1)
    samples = [(i // 2, rng.gauss(.15, 1.5), 1.0) for i in range(600)]
    a = mm.cluster_bootstrap(samples, 2000, .1, 7)
    assert a == mm.cluster_bootstrap(samples, 2000, .1, 7)
    mean = sum(v for _, v, _ in samples) / len(samples)
    assert a < mean
    assert mm.cluster_bootstrap([(1, 1.0, 1.0), (2, 3.0, 1.0)], 500, .1, 1) >= 1
    assert mm.cluster_bootstrap([], 100, .1, 1) is None

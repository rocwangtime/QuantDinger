"""HK equities (spread table 01), Phase 2 effective 2026-08-03.

Source: https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Mechanism/Reduction-of-Minimum-Spreads
Excluded products (ETPs, structured products, debt) require their own table.
Percent settings use percentage points; slippage is measured from quote midpoint.
"""
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import time

from app.services.automation.domain import number

BANDS = tuple((Decimal(high), Decimal(tick)) for high, tick in (
    ('0.25', '.001'), ('10', '.005'), ('20', '.01'), ('50', '.02'),
    ('100', '.05'), ('200', '.1'), ('500', '.2'), ('1000', '.5'),
    ('2000', '1'), ('5000', '2'), ('9995', '5')))
DEFAULTS = dict(max_slippage_pct=.5, spread_limit_pct=1, limit_offset_ticks=None)


def settings(raw):
    p = {key: raw.get(key, value) for key, value in DEFAULTS.items()}
    p['max_slippage_pct'] = number(p['max_slippage_pct'], 0, 5)
    p['spread_limit_pct'] = number(p['spread_limit_pct'], 0, 10)
    if p['limit_offset_ticks'] is not None:
        ticks = number(p['limit_offset_ticks'], 0, 100)
        if not ticks.is_integer():
            raise ValueError('limit_offset_ticks must be an integer')
        p['limit_offset_ticks'] = int(ticks)
    return p


def round_tick(price, side):
    """Directional rounding reselects the band after crossing its upper edge."""
    p = Decimal(str(price))
    if not p.is_finite() or not Decimal('.01') <= p <= BANDS[-1][0] or side not in {'buy', 'sell'}:
        raise ValueError('HK_PRICE_UNSUPPORTED')
    rounding = ROUND_FLOOR if side == 'buy' else ROUND_CEILING
    for upper, tick in BANDS:
        if p <= upper:
            result = (p / tick).to_integral_value(rounding=rounding) * tick
            if result > upper:
                continue
            return result
    raise ValueError('HK_PRICE_UNSUPPORTED')


def limit_price(quote, side, params=None, *, now=None):
    """Return (price, reason code); unsafe data never falls back to last trade."""
    if not isinstance(quote, dict):
        return None, 'HK_QUOTE_MISSING'
    if side not in {'buy', 'sell'}:
        raise ValueError('Invalid order side')
    now = time.time() if now is None else now
    try:
        stamp = number(quote.get('as_of'))
    except (ValueError, TypeError):
        return None, 'HK_QUOTE_MISSING'
    if quote.get('is_stale') or not 0 <= now-stamp <= 5:
        return None, 'HK_QUOTE_STALE'
    try:
        number(quote.get('price'), .01, 9995)
        bid = Decimal(str(number(quote.get('bid'), .01, 9995)))
        ask = Decimal(str(number(quote.get('ask'), .01, 9995)))
    except (ValueError, TypeError):
        return None, 'HK_QUOTE_MISSING'
    if bid > ask:
        return None, 'HK_QUOTE_INVALID'
    if not quote.get('simulate_execution_eligible'):
        return None, 'HK_QUOTE_INELIGIBLE'
    p = settings(params or {})
    mid = (bid + ask) / 2
    if (ask-bid)/mid*100 > Decimal(str(p['spread_limit_pct'])):
        return None, 'HK_SPREAD_TOO_WIDE'
    slip = Decimal(str(p['max_slippage_pct']))/100
    cap = mid*(1+slip if side == 'buy' else 1-slip)
    target = ask if side == 'buy' else bid
    if p['limit_offset_ticks'] is not None:
        tick = next(tick for upper, tick in BANDS if mid <= upper)
        offset = tick*p['limit_offset_ticks']
        target = mid+offset if side == 'buy' else mid-offset
    target = min(target, cap) if side == 'buy' else max(target, cap)
    try:
        result = round_tick(target, side)
    except ValueError:
        return None, 'HK_PRICE_UNSUPPORTED'
    return float(result), None

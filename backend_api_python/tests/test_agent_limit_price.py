from decimal import Decimal

import pytest

from app.services.automation.limit_price import limit_price, round_tick, settings
from app.services.automation.symbol_config import DEFAULTS, validate


@pytest.mark.parametrize('boundary,below_tick,above_tick', [
    ('.25', '.001', '.005'), ('.5', '.005', '.005'),
    ('10', '.005', '.01'), ('20', '.01', '.02'), ('50', '.02', '.05'),
    ('100', '.05', '.1'), ('200', '.1', '.2'), ('500', '.2', '.5'),
    ('1000', '.5', '1'), ('2000', '1', '2'), ('5000', '2', '5')])
def test_band_boundaries(boundary, below_tick, above_tick):
    p, low, high = map(Decimal, (boundary, below_tick, above_tick))
    assert round_tick(p, 'buy') == round_tick(p, 'sell') == p
    assert round_tick(p-Decimal('.0001'), 'buy') == p-low
    assert round_tick(p-Decimal('.0001'), 'sell') == p
    assert round_tick(p+Decimal('.0001'), 'buy') == p
    assert round_tick(p+Decimal('.0001'), 'sell') == p+high


def quote(**extra):
    return dict({'price': 25, 'bid': 24.98, 'ask': 25.02, 'as_of': 100,
                 'simulate_execution_eligible': True}, **extra)


@pytest.mark.parametrize('side,expected', [('buy', 25.02), ('sell', 24.98)])
def test_xiaomi_best_quote(side, expected):
    assert limit_price(quote(), side, now=100) == (expected, None)


@pytest.mark.parametrize('side,expected', [('buy', 25), ('sell', 25)])
def test_cap_rounds_inward(side, expected):
    result, reason = limit_price(quote(), side, {'max_slippage_pct': .03}, now=100)
    assert (result, reason) == (expected, None)
    assert abs(result-25) <= 25*.0003


@pytest.mark.parametrize('side,expected', [('buy', 25.04), ('sell', 24.96)])
def test_mid_offset(side, expected):
    assert limit_price(quote(), side, {'limit_offset_ticks': 2}, now=100) == (expected, None)
    assert limit_price(quote(), side, {'limit_offset_ticks': 100, 'max_slippage_pct': .1}, now=100) == (
        25.02 if side == 'buy' else 24.98, None)


@pytest.mark.parametrize('extra,now,reason', [
    ({'as_of': None}, 100, 'HK_QUOTE_MISSING'),
    ({}, 106, 'HK_QUOTE_STALE'), ({}, 99, 'HK_QUOTE_STALE'),
    ({'is_stale': True}, 100, 'HK_QUOTE_STALE'),
    ({'bid': None}, 100, 'HK_QUOTE_MISSING'),
    ({'ask': float('nan')}, 100, 'HK_QUOTE_MISSING'),
    ({'bid': 26}, 100, 'HK_QUOTE_INVALID'),
    ({'ask': 26}, 100, 'HK_SPREAD_TOO_WIDE'),
    ({'simulate_execution_eligible': False}, 100, 'HK_QUOTE_INELIGIBLE')])
def test_unsafe_quotes_skip(extra, now, reason):
    assert limit_price(quote(**extra), 'buy', now=now) == (None, reason)


@pytest.mark.parametrize('key,value', [('max_slippage_pct', -1), ('max_slippage_pct', 6),
    ('spread_limit_pct', float('inf')), ('spread_limit_pct', True),
    ('limit_offset_ticks', 1.5), ('limit_offset_ticks', -1)])
def test_settings_validated(key, value):
    with pytest.raises(ValueError):
        settings({key: value})
    with pytest.raises(ValueError):
        validate({**DEFAULTS, 'reference_price': 25, key: value})


def test_existing_symbol_payload_accepts_optional_defaults():
    payload = {key: value for key, value in DEFAULTS.items()
               if key not in {'max_slippage_pct', 'spread_limit_pct', 'limit_offset_ticks'}}
    result = validate({**payload, 'reference_price': 25})
    assert result['max_slippage_pct'] == .5
    assert result['spread_limit_pct'] == 1
    assert result['limit_offset_ticks'] is None


def test_absent_quote_is_safe():
    assert limit_price(None, 'buy', now=100) == (None, 'HK_QUOTE_MISSING')

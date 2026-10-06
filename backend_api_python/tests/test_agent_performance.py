"""Accounting and protection invariants; no real broker or model calls."""
from datetime import datetime, timezone

import pytest

from app.services.automation.performance import book, evaluate, purchase_allowed
from tests.test_agent_automations import config


def fill(i, side, quantity, price, status='FILLED'):
    return {'id': i, 'order_spec': {'symbol': 'AAPL', 'side': side},
            'filled_qty': quantity, 'avg_fill_price': price, 'status': status}


def account(qty):
    return {'positions': [{'symbol': 'AAPL', 'quantity': qty, 'side': 'long'}]}


def test_gross_book_partial_cancelled_fills_and_round_trip():
    fills = [fill(1, 'buy', 5, 100, 'CANCELLED'), fill(2, 'buy', 5, 120), fill(3, 'sell', 4, 130)]
    result = book(config(), {}, fills, {'AAPL': 125}, account(6))
    assert result['equity'] == pytest.approx(1170)
    assert result['realized_pnl'] == pytest.approx(80)
    assert result['unrealized_pnl'] == pytest.approx(90)
    assert result['return_pct'] == pytest.approx(17)
    assert result['positions'][0]['average_cost'] == 110
    assert result['filled_order_count'] == 3 and result['fees'] is None
    # Repeated receipt reconciliation is not an additional fill.
    assert book(config(), {}, fills, {'AAPL': 125}, account(6)) == result


def test_adopted_holdings_consume_budget_and_external_holdings_are_excluded():
    state = {'baseline': {'AAPL': 3}, 'performance': {'baseline_prices': {'AAPL': 100}, 'capital': 1000}}
    result = book(config(), state, [], {'AAPL': 110}, account(40))
    assert result['virtual_cash'] == 700
    assert result['equity'] == 1030 and result['return_pct'] == pytest.approx(3)


def test_known_external_inventory_changes_or_splits_invalidate_attribution():
    cfg = config()
    state = {'performance': {'external_quantities': {'AAPL': 8}}}
    receipts = [fill(1, 'buy', 2, 100)]
    assert book(cfg, state, receipts, {'AAPL': 110}, account(10))['equity'] == 1020
    assert book(cfg, state, receipts, {'AAPL': 55}, account(20))['equity'] is None
    assert book(cfg, state, receipts, {'AAPL': 110}, account(8))['equity'] is None


@pytest.mark.parametrize('state,fills,prices,qty,reason', [
    ({'baseline': {'AAPL': 1}}, [], {'AAPL': 100}, 1, 'baseline_price_missing'),
    ({}, [fill(1, 'buy', 2, None)], {'AAPL': 100}, 2, 'fill_price_missing'),
    ({}, [fill(1, 'buy', 2, 100)], {}, 2, 'mark_missing'),
    ({}, [fill(1, 'buy', 2, 100)], {'AAPL': 100}, 1, 'broker_quantity_mismatch'),
    ({}, [fill(1, 'sell', 2, 100)], {'AAPL': 100}, 0, 'oversold'),
])
def test_incomplete_book_never_claims_full_returns(state, fills, prices, qty, reason):
    result = book(config(), state, fills, prices, account(qty))
    assert result['equity'] is None and result['return_pct'] is None
    assert any(e.startswith(reason) for e in result['errors'])


def test_stop_loss_does_not_depend_on_model_or_other_symbols_quotes():
    cfg = config(risk={'enabled': True})
    report = book(cfg, {}, [fill(1, 'buy', 5, 100)], {'AAPL': 90}, account(5))
    report['equity'] = None  # A missing unrelated mark must not disable this stop.
    _, risk, exits = evaluate(cfg, {}, report, {'AAPL': 90}, 1000)
    assert risk['stopped_symbols'] == ['AAPL'] and exits[0]['action'] == 'EXIT'
    assert not purchase_allowed({'config': cfg, 'state': {'risk': risk}}, 'AAPL', 1001)


def test_portfolio_loss_latches_until_explicit_reset_and_day_uses_exchange_zone():
    cfg = config(risk={'enabled': True})
    now = datetime(2026, 10, 6, 14, tzinfo=timezone.utc).timestamp()
    state = {'performance': {'high_water': 1100, 'day': '2026-10-06', 'day_equity': 1000}}
    report = book(cfg, {}, [fill(1, 'buy', 10, 100)], {'AAPL': 96}, account(10))
    tracking, risk, exits = evaluate(cfg, state, report, {'AAPL': 96}, now)
    assert risk['halted'] and set(risk['reasons']) == {'daily_loss_limit', 'drawdown_limit'}
    assert exits and tracking['day'] == '2026-10-06'
    report = book(cfg, {}, [], {'AAPL': 110}, account(0))
    _, recovered, _ = evaluate(cfg, {'performance': tracking, 'risk': risk}, report, {'AAPL': 110}, now + 86400)
    assert recovered['halted']  # A new session/recovered price is not permission.


def test_buy_gate_requires_healthy_recent_monitor_and_symbol_scope():
    cfg = config(risk={'enabled': True})
    row = {'config': cfg, 'state': {'risk': {'healthy': True, 'checked_at': 100, 'stopped_symbols': ['TSLA']}}}
    assert purchase_allowed(row, 'AAPL', 110)
    assert not purchase_allowed(row, 'TSLA', 110)
    assert not purchase_allowed(row, 'AAPL', 191)
    assert not purchase_allowed(row, 'AAPL', 99)
    assert not purchase_allowed({'config': cfg, 'state': {}}, 'AAPL', 100)


@pytest.mark.parametrize('risk', [{'enabled': 'yes'}, {'enabled': True, 'stop_loss_pct': True},
                                {'max_daily_loss_pct': 3}, {'max_drawdown_pct': float('nan')}, []])
def test_risk_configuration_rejects_invalid_units_and_types(risk):
    # Empty lists must not silently become a disabled risk object.
    with pytest.raises((ValueError, TypeError)):
        config(risk=risk)


def test_drawdown_fires_at_exact_threshold():
    cfg = config(risk={'enabled': True})
    report = book(cfg, {}, [fill(1, 'buy', 10, 100)], {'AAPL': 90}, account(10))
    _, risk, _ = evaluate(cfg, {'performance': {'high_water': 1000}}, report, {'AAPL': 90}, 1000)
    assert 'drawdown_limit' in risk['reasons']


def test_disabling_checks_does_not_implicitly_clear_a_latched_halt():
    row = {'config': config(risk={'enabled': False}), 'state': {'risk': {'halted': True}}}
    assert not purchase_allowed(row, 'AAPL', 100)


def test_benchmark_freezes_initial_universe_and_reserve():
    cfg = config(symbols=['AAPL', 'TSLA'], reserve_ratio=.2)
    report = book(cfg, {}, [], {}, account(0))
    tracking, _, _ = evaluate(cfg, {}, report, {'AAPL': 100, 'TSLA': 200}, 1000)
    assert report['benchmark_return_pct'] == pytest.approx(0)
    changed = config(symbols=['QQQ'], reserve_ratio=.9)
    report = book(changed, {'performance': tracking}, [], {}, account(0))
    evaluate(changed, {'performance': tracking}, report, {'AAPL': 110, 'TSLA': 240, 'QQQ': 10}, 2000)
    assert report['benchmark_return_pct'] == pytest.approx(12)

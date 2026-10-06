"""Task-owned, fill-based gross accounting. Broker account equity is separate."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def positive(value):
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError):
        return None


def purchase_allowed(row, symbol, now=None):
    """Fail closed for new exposure when the independent monitor is unavailable."""
    risk = (row.get('state') or {}).get('risk') or {}
    if risk.get('halted') or symbol in risk.get('stopped_symbols', []):
        return False
    if not row['config'].get('risk', {}).get('enabled'):
        return True
    now = now if now is not None else datetime.now(timezone.utc).timestamp()
    return risk.get('healthy') is True and 0 <= now - float(risk.get('checked_at') or 0) <= 90


def book(config, state, receipts, prices, account):
    """Replay cumulative fills once per intent, including partial/cancelled fills.

    Same-symbol outstanding orders cannot overlap in the executor. Intent order
    therefore gives the acquisition/disposal order used by average-cost books.
    Missing fill prices and broker ownership discrepancies suppress total P&L.
    Fees are unavailable from this gateway; figures are explicitly gross.
    """
    tracking = state.get('performance') or {}
    capital = float(tracking.get('capital', config['budget']))
    cash, realized, turnover, count = capital, 0., 0., 0
    holdings, errors = {}, []
    for code, qty in (state.get('baseline') or {}).items():
        qty = float(qty)
        if qty <= 0:
            continue
        price = positive((tracking.get('baseline_prices') or {}).get(code))
        if price is None:
            errors.append('baseline_price_missing:' + code)
            holdings[code] = {'quantity': qty, 'cost': None}
        else:
            holdings[code] = {'quantity': qty, 'cost': qty * price}
            cash -= qty * price
    for receipt in sorted(receipts, key=lambda r: r['id']):
        qty = float(receipt.get('filled_qty') or 0)
        if qty <= 0:
            continue
        order = receipt['order_spec']
        code = order['symbol']
        price = positive(receipt.get('avg_fill_price'))
        position = holdings.setdefault(code, {'quantity': 0., 'cost': 0.})
        count += 1
        if price is None:
            errors.append('fill_price_missing:' + str(receipt['id']))
            position['cost'] = None
            position['quantity'] += qty if order['side'] == 'buy' else -qty
            continue
        gross = qty * price
        turnover += gross
        if order['side'] == 'buy':
            cash -= gross
            if position['cost'] is not None:
                position['cost'] += gross
            position['quantity'] += qty
        else:
            if position['quantity'] < qty - 1e-8:
                errors.append('oversold:' + code)
            if position['cost'] is not None and position['quantity'] > 0:
                disposed = position['cost'] * qty / position['quantity']
                position['cost'] -= disposed
                realized += gross - disposed
            cash += gross
            position['quantity'] -= qty
    broker = {}
    for position in account['positions']:
        if position.get('side') == 'long':
            code = position['symbol']
            broker[code] = broker.get(code, 0.) + float(position['quantity'])
    market_value, unrealized, positions = 0., 0., []
    for code, position in holdings.items():
        qty = position['quantity']
        if qty <= 1e-8:
            continue
        price = positive(prices.get(code))
        cost = position['cost']
        if price is None:
            errors.append('mark_missing:' + code)
        external = tracking.get('external_quantities')
        expected = qty + float((external or {}).get(code, 0))
        if (external is not None and abs(broker.get(code, 0) - expected) > 1e-8) or broker.get(code, 0) < qty - 1e-8:
            errors.append('broker_quantity_mismatch:' + code)
        value = qty * price if price is not None else None
        pnl = value - cost if value is not None and cost is not None else None
        market_value += value or 0
        unrealized += pnl or 0
        positions.append({'symbol': code, 'quantity': qty,
                          'average_cost': cost / qty if cost is not None else None,
                          'price': price, 'market_value': value, 'unrealized_pnl': pnl})
    equity = cash + market_value if not errors else None
    return {'capital': capital, 'virtual_cash': cash if not errors else None,
            'equity': equity, 'return_pct': (equity / capital - 1) * 100 if equity is not None else None,
            'realized_pnl': realized if not errors else None,
            'unrealized_pnl': unrealized if not errors else None,
            'positions': positions, 'filled_order_count': count, 'turnover': turnover,
            'fees': None, 'pnl_basis': 'gross_before_fees_and_dividends', 'errors': errors}


def evaluate(config, state, report, prices, now):
    """Return durable monitoring state and deterministic exit candidates."""
    tracking = dict(state.get('performance') or {})
    tracking.setdefault('capital', config['budget'])
    tracking.setdefault('symbols', list(config['symbols']))
    tracking.setdefault('reserve_ratio', config['reserve_ratio'])
    tracking.setdefault('started_at', now)
    anchors = tracking.get('benchmark_prices') or {}
    # Freeze one simultaneous initial mark; never cherry-pick later start dates.
    if not anchors and all(positive(prices.get(s)) for s in tracking['symbols']):
        anchors = {s: prices[s] for s in tracking['symbols']}
        tracking['benchmark_prices'] = anchors
        tracking['benchmark_started_at'] = now
    benchmark = None
    if anchors and all(positive(prices.get(s)) for s in anchors):
        benchmark = ((1 - tracking['reserve_ratio']) * sum(prices[s] / p for s, p in anchors.items())
                     / len(anchors) + tracking['reserve_ratio'] - 1) * 100
    report['benchmark_return_pct'] = benchmark
    report['benchmark_started_at'] = tracking.get('benchmark_started_at')
    report['as_of'] = now
    equity = report['equity']
    risk = dict(state.get('risk') or {})
    risk.update(checked_at=now, healthy=equity is not None)
    risk.pop('monitor_error', None)
    risk.setdefault('halted', False)
    risk.setdefault('stopped_symbols', [])
    exits = []
    if equity is not None:
        high = max(float(tracking.get('high_water', report['capital'])), equity)
        tracking['high_water'] = high
        day = datetime.fromtimestamp(now, ZoneInfo('America/New_York' if config['market'] == 'USStock'
                                                 else 'Asia/Hong_Kong')).date().isoformat()
        if tracking.get('day') != day:
            tracking.update(day=day, day_equity=equity)
        day_equity = float(tracking['day_equity'])
        loss = max(0., 1 - equity / day_equity) if day_equity > 0 else 0.
        drawdown = max(0., 1 - equity / high) if high > 0 else 0.
        tracking['max_drawdown_pct'] = max(float(tracking.get('max_drawdown_pct', 0)), drawdown * 100)
        report.update(drawdown_pct=drawdown * 100, max_drawdown_pct=tracking['max_drawdown_pct'],
                      daily_loss_pct=loss * 100)
        limits = config.get('risk') or {}
        if limits.get('enabled'):
            reasons = []
            if equity <= day_equity * (1 - limits['max_daily_loss_pct']):
                reasons.append('daily_loss_limit')
            if equity <= high * (1 - limits['max_drawdown_pct']):
                reasons.append('drawdown_limit')
            if reasons:
                risk.update(halted=True, reasons=sorted(set(risk.get('reasons', []) + reasons)))
    limits = config.get('risk') or {}
    if limits.get('enabled'):
        for position in report['positions']:
            code, price, cost = position['symbol'], position['price'], position['average_cost']
            if price is not None and cost is not None and price <= cost * (1 - limits['stop_loss_pct']):
                risk['stopped_symbols'] = sorted(set(risk['stopped_symbols'] + [code]))
            if price is not None and (risk['halted'] or code in risk['stopped_symbols']):
                exits.append({'symbol': code, 'action': 'EXIT', 'target_weight': 0,
                              'min_price': price * .99, 'max_price': price * 1.01,
                              'reason': '独立持仓保护：止损或组合亏损阈值已触发',
                              'invalidation': '由程序校验，不依赖模型判断'})
    if equity is not None:
        tracking['latest_valid'] = report
    tracking['latest'] = report
    return tracking, risk, exits

"""Pure event selection from eligible portfolio observations."""
from __future__ import annotations

from app.services.automation.performance import positive


def review_event(config, revision, previous, prices, receipts, now):
    previous = previous or {}
    if not all(positive(prices.get(code)) for code in config['symbols']):
        return None, previous
    if now - float(previous.get('reviewed_at', 0)) < config['cooldown_seconds']:
        return None, previous
    current = {code: prices[code] for code in config['symbols']}
    fills = [[r['id'], float(r.get('filled_qty') or 0), float(r['avg_fill_price']) if r.get('avg_fill_price') is not None else None]
             for r in receipts if float(r.get('filled_qty') or 0) > 0]
    fills.sort(key=lambda r: r[0])
    cause = None
    if previous.get('revision') != revision or not previous.get('prices'):
        cause = {'type': 'initial_observation'}
    elif config['events']['on_fill'] and fills != previous.get('fills', []):
        cause = {'type': 'fills_changed', 'fills': fills}
    else:
        threshold = config['events']['price_move_pct']
        moved = {code: {'from': previous['prices'].get(code), 'to': price,
                        'move_pct': price / previous['prices'][code] - 1}
                 for code, price in current.items() if positive(previous['prices'].get(code))
                 and abs(price - previous['prices'][code]) >= previous['prices'][code] * threshold}
        if moved:
            cause = {'type': 'price_movement', 'symbols': moved}
    if not cause:
        return None, previous
    cause['observed_at'] = now
    next_state = {'revision': revision, 'reviewed_at': now, 'prices': current, 'fills': fills}
    return cause, next_state

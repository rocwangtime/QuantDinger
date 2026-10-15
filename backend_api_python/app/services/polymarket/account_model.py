"""Deterministic complete-set paper sizing and fills using the verified engine fees."""
import copy
import hashlib
from pathlib import Path
from decimal import ROUND_DOWN

from .engine import EPS, IMPLEMENTATION_HASH, ONE, ZERO, digest, fee, number, public, sweep

VERSION = 'paper-account-2'
SOURCE_HASH = hashlib.sha256(Path(__file__).read_bytes() + Path(__file__).with_name('account_history.py').read_bytes()
                             + IMPLEMENTATION_HASH.encode()).hexdigest()
DEFAULTS = dict(taker_enabled=False, initial_capital='5000', trade_cap='1000', free_fraction='0.20', locked_fraction='0.60',
                loss_fraction='0.02', min_edge_bps='10', full_edge_bps='100', min_ticket='10',
                size_step='0.01', slippage='0', competitor_top='0.50', competitor_other='0',
                merge_cost='0.02', merge_enabled=True, settlement_seconds=60, cooldown_seconds=60,
                default_latency_ms=150, latency_multiplier='1', latency_refresh_seconds=300,
                max_book_age_ms=2000, history_seconds=10, history_events=2000, retention_days=90)


def validate(config):
    result = {**DEFAULTS, **config}
    if set(result) != set(DEFAULTS):
        raise ValueError('polymarket.unknownSetting')
    for key, default in DEFAULTS.items():
        if isinstance(default, bool):
            if type(result[key]) is not bool:
                raise ValueError('polymarket.invalidSetting')
        else:
            value = number(result[key])
            if value < 0 or value > 10000000:
                raise ValueError('polymarket.invalidSetting')
            if isinstance(default, int):
                if value != int(value):
                    raise ValueError('polymarket.invalidSetting')
                result[key] = int(value)
            else:
                result[key] = str(value)
    for key in ('free_fraction', 'locked_fraction', 'loss_fraction', 'competitor_top', 'competitor_other'):
        if number(result[key]) > ONE:
            raise ValueError('polymarket.invalidSetting')
    if (number(result['full_edge_bps']) <= number(result['min_edge_bps'])
            or number(result['min_ticket']) > number(result['trade_cap'])
            or any(number(result[k]) <= 0 for k in ('initial_capital', 'trade_cap', 'size_step', 'history_seconds', 'history_events', 'retention_days'))):
        raise ValueError('polymarket.invalidSetting')
    return result


def competed(book, config):
    result = copy.deepcopy(book)
    for side in ('asks', 'bids'):
        for i, level in enumerate(result[side]):
            level['size'] = str(number(level['size']) * (ONE - number(config['competitor_top' if i == 0 else 'competitor_other'])))
    return result


def cost(books, qty, rate, merge):
    legs = [sweep(b, 'BUY', qty, rate, order_type='FOK') for b in books]
    return sum((x['notional'] + x['fee'] for x in legs), ZERO) + merge, legs


def size(market, books, config, *, free, locked, equity, stopped=False, cooldown=False):
    """Walk matched marginal depth; cash caps include fees and the merge assumption."""
    c = validate(config)
    decision = {'quantity': '0', 'reason': None, 'inputs': public(dict(free=free, locked=locked, equity=equity,
                stopped=stopped, cooldown=cooldown)), 'limits': [], 'caps': {}}
    def skip(reason):
        decision['reason'] = reason
        return decision
    if stopped or cooldown:
        return skip('daily_loss_stop' if stopped else 'cooldown')
    if len(books) != 2 or any(not b.get('asks') for b in books):
        return skip('missing_book')
    rate, threshold, merge = number(market['feeRate']), number(c['min_edge_bps']), number(c['merge_cost'])
    tick = number(market['tickSize'])
    # Bounds are the union of cumulative depth boundaries on BOTH outcomes.
    bounds = set()
    for b in books:
        cumulative = ZERO
        for level in b['asks']:
            cumulative += number(level['size'])
            bounds.add(cumulative)
    previous = ZERO
    available = ZERO
    for boundary in sorted(bounds):
        qty = boundary - previous
        prices = []
        for b in books:
            cumulative = ZERO
            price = None
            for level in b['asks']:
                cumulative += number(level['size'])
                if cumulative >= boundary:
                    price = number(level['price'])
                    break
            prices.append(price)
        if any(p is None for p in prices):
            break
        # Allocate fixed merge cost over cumulative sets and require the marginal
        # tranche (including conservative per-level fees) to retain the threshold.
        marginal = sum((qty * p + fee(qty, p, rate) for p in prices), ZERO)
        marginal_edge = (qty - marginal - merge * qty / boundary) / qty * 10000
        if marginal_edge < threshold:
            break
        available, previous = boundary, boundary
    if not available:
        return skip('below_edge')
    depth_cost, _ = cost(books, available, rate, merge)
    edge = (available - depth_cost) / available * 10000
    scaled = number(c['min_ticket']) + (number(c['trade_cap']) - number(c['min_ticket'])) * max(ZERO, min(ONE,
             (edge - threshold) / (number(c['full_edge_bps']) - threshold)))
    caps = dict(depth=depth_cost, trade=number(c['trade_cap']), free=number(free) * number(c['free_fraction']),
                edge=scaled, portfolio=max(ZERO, number(equity) * number(c['locked_fraction']) - number(locked)))
    cap = min(caps.values())
    step = number(market.get('sizeStep') or c['size_step'])
    low, high = 0, int(available / step)
    # Search in discrete size steps, accounting for each consumed level's fees.
    while low < high:
        mid = (low + high + 1) // 2
        spent, _ = cost(books, step * mid, rate, merge)
        if spent <= cap:
            low = mid
        else:
            high = mid - 1
    qty = step * low
    spent, legs = cost(books, qty, rate, merge)
    decision.update(caps=public(caps), binding=min(caps, key=caps.get), net_edge_bps=str(edge), notional=str(spent))
    if not qty or any(x['notional'] < number(market['minOrderNotional']) for x in legs):
        return skip('below_minimum')
    limits = [(min(ONE - tick, x['worstPrice'] + number(c['slippage'])) / tick).to_integral_value(rounding=ROUND_DOWN) * tick for x in legs]
    # Reserve worst permitted spend, including a per-level rounding allowance.
    reserve = sum((qty * p + fee(qty, p, rate) for p in limits), ZERO) + merge + EPS * 1000
    if reserve > cap or (qty - reserve) / qty * 10000 < threshold:
        # Zero buffer still needs rounding protection; reduce a size step rather
        # than silently exceed a cash/portfolio cap.
        low, high = 0, int(qty / step)
        while low < high:
            mid = (low + high + 1) // 2
            candidate = step * mid
            upper = sum((candidate * p + fee(candidate, p, rate) for p in limits), ZERO) + merge + EPS * 1000
            if upper <= cap:
                low = mid
            else:
                high = mid - 1
        qty = step * low
        reserve = sum((qty * p + fee(qty, p, rate) for p in limits), ZERO) + merge + EPS * 1000
        if qty <= 0 or (qty - reserve) / qty * 10000 < threshold:
            return skip('price_limit_edge')
    if any(qty * p < number(market['minOrderNotional']) for p in limits):
        return skip('below_minimum')
    decision.update(quantity=str(qty), limits=public(limits), reserve=str(reserve))
    return decision


def fill(market, book, qty, config, limit=None, side='BUY'):
    if not book:
        book = {'asks': [], 'bids': []}
    return public(sweep(competed(book, config), side, number(qty), number(market['feeRate']),
                        limit=number(limit) if limit is not None else None,
                        order_type='FOK' if side == 'BUY' else 'FAK'))


def outcome(evidence):
    """Recompute fills and financial deltas; account reservations are separate."""
    if evidence['version'] != VERSION or evidence.get('source_hash') != SOURCE_HASH:
        raise ValueError('polymarket.engineVersionMismatch')
    m, c, d = evidence['market'], evidence['config'], evidence['decision']
    qty = number(d['quantity'])
    result = dict(status='cancelled_by_limit' if d['reason'] else 'not_filled', legs=[], cost='0',
                  payout='0', realized_pnl='0', residual_quantity='0', residual_cost='0', fees='0')
    if d['reason']:
        return result
    first = fill(m, evidence['steps'].get('leg1', {}).get('book'), qty, c, d['limits'][0])
    result['legs'].append(dict(role='leg1', **first))
    if not number(first['filled']):
        return result
    second = fill(m, evidence['steps'].get('leg2', {}).get('book'), qty, c, d['limits'][1])
    result['legs'].append(dict(role='leg2', **second))
    entry = number(first['notional']) + number(first['fee'])
    if number(second['filled']):
        spent = entry + number(second['notional']) + number(second['fee']) + number(c['merge_cost'])
        result.update(status='filled_merged', cost=str(spent), payout=str(qty))
    else:
        unwind = fill(m, evidence['steps'].get('unwind', {}).get('book'), qty, c, side='SELL')
        result['legs'].append(dict(role='unwind', **unwind))
        residual = qty - number(unwind['filled'])
        basis = entry * residual / qty
        proceeds = number(unwind['notional']) - number(unwind['fee'])
        result.update(status='single_leg_residual' if residual else 'single_leg_unwound', cost=str(entry),
                      payout=str(proceeds), realized_pnl=str(proceeds - (entry - basis)),
                      residual_quantity=str(residual), residual_cost=str(basis))
    result['fees'] = str(sum((number(x['fee']) for x in result['legs']), ZERO))
    if evidence.get('release'):
        release = evidence['release']
        if result['status'] == 'filled_merged':
            result['realized_pnl'] = str(number(result['payout']) - number(result['cost']))
        elif result['status'] == 'single_leg_residual':
            result['resolution_payout'] = str(number(result['residual_quantity']) * number(release['outcome_price']))
            result['realized_pnl'] = str(number(result['realized_pnl']) + number(result['resolution_payout']) - number(result['residual_cost']))
    return result


def replay(evidence, expected, evidence_hash):
    if digest(evidence) != evidence_hash:
        raise ValueError('polymarket.evidenceHashMismatch')
    result = outcome(evidence)
    d = evidence['decision']
    recomputed = size(evidence['market'], evidence['signal_books'], evidence['config'], **d['inputs'])
    same_decision = recomputed == d
    if d.get('reason') == 'restart_cancelled':
        same_decision = evidence.get('restart_cancelled') is True
    return dict(replayMatches=result == expected and same_decision, sizingMatches=same_decision, result=result, evidenceHash=evidence_hash)

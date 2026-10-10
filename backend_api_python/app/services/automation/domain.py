"""Pure scheduling, validation and sizing rules shared by replay and live tasks."""
from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone


def number(value, low=0, high=1e12):
    if isinstance(value, bool):
        raise ValueError('Expected a finite number')
    value = float(value)
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f'Number must be between {low} and {high}')
    return value


def symbol(value, market):
    from app.services.futu_trading.symbols import from_futu_code, to_futu_code
    code = to_futu_code(str(value).strip().upper(), market)
    display, actual_market = from_futu_code(code)
    pattern = r'HK\.[0-9]{5}' if market == 'HKStock' else r'US\.[A-Z][A-Z0-9.-]{0,14}'
    if actual_market != market or not re.fullmatch(pattern, code):
        raise ValueError('Symbol does not match the selected market')
    return display


def normalize_config(raw):
    if not isinstance(raw, dict):
        raise ValueError('Task config must be an object')
    market = raw.get('market', 'USStock')
    kind = raw.get('kind', 'daily_portfolio')
    if market not in {'USStock', 'HKStock'} or kind not in {'daily_portfolio', 'price_trigger', 'event_portfolio'}:
        raise ValueError('Unsupported market or task template')
    if raw.get('execution_mode', 'plan_only') not in {'plan_only', 'paper_auto'}:
        raise ValueError('Only research and SIMULATE execution are supported')
    symbols = raw.get('symbols')
    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 20:
        raise ValueError('Choose 1–20 symbols')
    symbols = list(dict.fromkeys(symbol(s, market) for s in symbols))
    credential_id = number(raw.get('credential_id'), 1)
    if not credential_id.is_integer():
        raise ValueError('Invalid account identifier')
    if kind == 'price_trigger' and len(symbols) != 1:
        raise ValueError('A fast trigger task manages one symbol')
    config = {
        'kind': kind, 'market': market, 'symbols': symbols,
        'credential_id': int(credential_id),
        'execution_mode': raw.get('execution_mode', 'plan_only'),
        'budget': number(raw.get('budget', 1000), 1),
        'max_weight': number(raw.get('max_weight', .25), .01, 1),
        'reserve_ratio': number(raw.get('reserve_ratio', .1), 0, .95),
        'max_order_notional': number(raw.get('max_order_notional', 500), 1),
        'max_daily_notional': number(raw.get('max_daily_notional', 2000), 1),
        'before_open_minutes': int(number(raw.get('before_open_minutes', 60), 5, 180)),
        'execute_after_open_minutes': int(number(raw.get('execute_after_open_minutes', 5), 1, 30)),
        'cooldown_seconds': int(number(raw.get('cooldown_seconds', 300), 30, 86400)),
        'brief': str(raw.get('brief') or '').strip()[:4000],
        'manage_existing': raw.get('manage_existing') is True,
        'decision_timeout_seconds': 15 if kind == 'price_trigger' else 180,
    }
    if config['max_daily_notional'] < config['max_order_notional']:
        raise ValueError('Daily limit must be at least the per-order limit')
    risk = raw.get('risk', {})
    if not isinstance(risk, dict) or not isinstance(risk.get('enabled', False), bool):
        raise ValueError('Risk settings must be an object with a boolean enabled field')
    config['risk'] = {
        'enabled': risk.get('enabled', False),
        'stop_loss_pct': number(risk.get('stop_loss_pct', .08), .001, .5),
        'max_daily_loss_pct': number(risk.get('max_daily_loss_pct', .03), .001, .5),
        'max_drawdown_pct': number(risk.get('max_drawdown_pct', .1), .001, .9),
    }
    research = raw.get('research', {})
    if not isinstance(research, dict) or research.get('mode', 'snapshot') not in {'snapshot', 'tool_loop'}:
        raise ValueError('Unsupported research mode')
    if kind == 'price_trigger' and research.get('mode', 'snapshot') != 'snapshot':
        raise ValueError('Fast price triggers require snapshot research')

    def integer(value, low, high):
        value = number(value, low, high)
        if not value.is_integer():
            raise ValueError('Limit must be an integer')
        return int(value)

    config['research'] = {
        'mode': research.get('mode', 'snapshot'),
        'max_model_calls': integer(research.get('max_model_calls', 3), 1, 4),
        'max_tool_requests': integer(research.get('max_tool_requests', 6), 1, 8),
        'max_output_tokens': integer(research.get('max_output_tokens', 7000), 700, 14000),
        'max_decisions_per_day': integer(research.get('max_decisions_per_day', 8), 1, 100),
    }
    if kind == 'event_portfolio':
        events = raw.get('events', {})
        if not isinstance(events, dict) or not isinstance(events.get('on_fill', True), bool):
            raise ValueError('Invalid portfolio event settings')
        config['events'] = {'price_move_pct': number(events.get('price_move_pct', .02), .001, .5),
                            'on_fill': events.get('on_fill', True)}
    if kind == 'price_trigger':
        trigger = raw.get('trigger') or {}
        if trigger.get('type') not in {'price_above', 'price_below'}:
            raise ValueError('Select an above/below price trigger')
        config['trigger'] = {'type': trigger['type'], 'price': number(trigger.get('price'), .0001)}
    from app.services.llm_selection import validate_selection
    config['llm_selection'] = validate_selection(raw.get('llm_selection'))
    return config


def session_schedule(config, now=None):
    import exchange_calendars as xcals
    import pandas as pd
    now = now or datetime.now(timezone.utc)
    cal = xcals.get_calendar('XNYS' if config['market'] == 'USStock' else 'XHKG')
    zone = 'America/New_York' if config['market'] == 'USStock' else 'Asia/Hong_Kong'
    day = pd.Timestamp(now).tz_convert(zone).date()
    session = cal.date_to_session(str(day), direction='next')
    opening = cal.session_open(session).to_pydatetime()
    closing = cal.session_close(session).to_pydatetime()
    if now >= closing:
        session = cal.next_session(session)
        opening = cal.session_open(session).to_pydatetime()
        closing = cal.session_close(session).to_pydatetime()
    run_at = opening - timedelta(minutes=config['before_open_minutes'])
    execution_at = opening + timedelta(minutes=config['execute_after_open_minutes'])
    return {'session': str(session.date()), 'run_at': run_at, 'open_at': opening,
            'execute_at': execution_at, 'expires_at': execution_at + timedelta(minutes=10),
            'close_at': closing, 'due': run_at <= now < opening}


def trigger_state(config, quote, previous, now):
    """One event per false→true edge, with re-arm and durable cooldown."""
    previous = dict(previous or {})
    price = number(quote.get('price'), .0001)
    if not quote.get('is_realtime') or not 0 <= now - float(quote.get('as_of', 0)) <= 5:
        return False, previous
    threshold = config['trigger']['price']
    above = config['trigger']['type'] == 'price_above'
    met = price >= threshold if above else price <= threshold
    # Hysteresis prevents repeated edge crossings around a single tick.
    reset = price < threshold * .999 if above else price > threshold * 1.001
    latched = bool(previous.get('latched'))
    if reset:
        latched = False
    fire = met and not latched and now >= float(previous.get('next_allowed_at', 0))
    if fire:
        previous.update(latched=True, next_allowed_at=now + config['cooldown_seconds'])
    else:
        previous['latched'] = latched
    previous['observed_price'] = price
    return fire, previous


def parse_decision(text, evidence, config):
    """Accept complete machine decisions; streamed partial text never executes."""
    raw = text.strip()
    if raw.startswith('```'):
        raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    decision = json.loads(raw)
    items = decision.get('items') if isinstance(decision, dict) else None
    if not isinstance(items, list) or len(items) > 40:
        raise ValueError('Model did not return a complete portfolio decision')
    allowed = set(evidence['instruments'])
    seen, total = set(), 0.0
    for item in items:
        if not isinstance(item, dict):
            raise ValueError('Invalid decision item')
        code = item.get('symbol')
        if code not in allowed or code in seen:
            raise ValueError('Unknown or duplicate decision symbol')
        seen.add(code)
        action = item.get('action')
        if action not in {'BUY', 'REDUCE', 'EXIT', 'HOLD', 'WAIT'}:
            raise ValueError('Invalid decision action')
        weight = number(item.get('target_weight'), 0, config['max_weight'])
        if action in {'EXIT', 'WAIT'} and weight != 0:
            raise ValueError('EXIT and WAIT require zero target weight')
        if action == 'BUY' and code not in config['symbols']:
            raise ValueError('New purchases must stay inside the selected universe')
        if action in {'BUY', 'REDUCE', 'EXIT'}:
            lo = number(item.get('min_price'), .0001)
            hi = number(item.get('max_price'), lo)
            reference = number(evidence['instruments'][code]['price'], .0001)
            if lo < reference * .8 or hi > reference * 1.2:
                raise ValueError('Execution price range is too broad')
        item['reason'] = str(item.get('reason') or '')[:1000]
        item['invalidation'] = str(item.get('invalidation') or '')[:500]
        item['target_weight'] = weight
        total += weight
    if total > 1 - config['reserve_ratio'] + 1e-9:
        raise ValueError('Target weights exceed the cash reserve constraint')
    # Omissions mean HOLD, never liquidate an unmentioned holding.
    return {'items': items, 'summary': str(decision.get('summary') or '')[:2000]}


def available_buy_cash(funds, market):
    """HK SIMULATE may omit generic power; exact stock limits are checked separately."""
    cash = float(funds.get('cash') or 0)
    power = float(funds.get('power') or 0)
    if market == 'HKStock' and power == 0:
        return cash
    return min(cash, power)


def size_order(item, config, *, price, lot, owned_qty, broker_qty, power, open_symbols,
               max_cash_buy=None):
    code = item['symbol']
    if code in open_symbols or item['action'] in {'HOLD', 'WAIT'}:
        return None
    price, lot = number(price, .0001), int(number(lot, 1))
    if not item['min_price'] <= price <= item['max_price']:
        return None
    target = math.floor(config['budget'] * item['target_weight'] / price / lot) * lot
    owned = min(number(owned_qty), number(broker_qty))
    if item['action'] == 'BUY':
        # Existing external positions contribute to exposure, but are never sold.
        delta = max(0, target - broker_qty)
        cap = min(config['max_order_notional'], max(0, power), delta * price)
        if config['market'] == 'HKStock' and max_cash_buy is not None:
            # Futu max_cash_buy is a quantity, not a monetary balance.
            cap = min(cap, number(max_cash_buy) * price)
        qty, side = math.floor(cap / price / lot) * lot, 'buy'
    else:
        delta = owned if item['action'] == 'EXIT' else min(owned, max(0, broker_qty - target))
        qty = math.floor(min(delta, config['max_order_notional'] / price) / lot) * lot
        side = 'sell'
    if qty <= 0:
        return None
    return {'broker': 'futu', 'credential_id': config['credential_id'], 'market': config['market'],
            'symbol': code, 'side': side, 'qty': qty, 'order_type': 'limit',
            'limit_price': price, 'reason': item['reason']}

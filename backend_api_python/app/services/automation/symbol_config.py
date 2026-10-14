"""Market-specific single-symbol settings, materialized atomically into the task runtime config."""
import copy
import math
import re

from app.services.automation import store
from app.services.automation.domain import number
from app.services.automation.limit_price import DEFAULTS as PRICE_DEFAULTS, settings

DEFAULTS = dict(budget=100000, per_order_cap=20000, daily_buy_cap=40000,
                cash_reserve_pct=20, max_position=60000, prev_close_pct=2,
                short_move_pct=1, window_minutes=5, cooldown_minutes=30,
                max_triggers_day=8, blackout_auction=True, blackout_lunch=True,
                blackout_close_minutes=5, stop_loss_pct=5, daily_loss_pct=2,
                max_drawdown_pct=8, model='deepseek-v4-1-flash', mode='plan_only', lot_size=200, reference_price=None)


DEFAULTS.update(PRICE_DEFAULTS)
US_DEFAULTS = {**DEFAULTS, 'budget': 15000, 'per_order_cap': 3000,
               'daily_buy_cap': 6000, 'max_position': 9000, 'lot_size': 1,
               'blackout_lunch': False, 'blackout_open_minutes': 5, 'extended_hours': False}
PROFILES = {'HKStock': DEFAULTS, 'USStock': US_DEFAULTS}


def eligible(row):
    c = row['config']
    if c['market'] not in PROFILES or c['kind'] != 'price_trigger' or len(c['symbols']) != 1:
        raise ValueError('Symbol settings require a single-symbol US/HK SIMULATE price task')
    return c['symbols'][0]


def validate(raw, market='HKStock'):
    defaults = PROFILES[market]
    currency = 'USD' if market == 'USStock' else 'HKD'
    if not isinstance(raw, dict) or (set(raw) - set(defaults) - {'blackout_open_minutes', 'extended_hours'}) or (set(defaults) - set(raw) - set(PRICE_DEFAULTS) - {'blackout_open_minutes', 'extended_hours'}):
        raise ValueError('Provide all symbol settings; unknown fields are forbidden')
    p = {**PRICE_DEFAULTS, **({'blackout_open_minutes': 5, 'extended_hours': False} if market == 'USStock' else {}), **raw}
    p.update(settings(p))
    for key in tuple(k for k in ('blackout_auction', 'blackout_lunch', 'extended_hours') if k in p):
        if not isinstance(p[key], bool):
            raise ValueError('Blackout switches must be boolean')
    bounds = dict(budget=(1, 1e9), per_order_cap=(1, 1e9), daily_buy_cap=(1, 1e9),
                  max_position=(1, 1e9), cash_reserve_pct=(0, 95), prev_close_pct=(.01, 50),
                  short_move_pct=(.01, 50), window_minutes=(1, 120), cooldown_minutes=(1, 1440),
                  max_triggers_day=(1, 100), blackout_close_minutes=(0, 60),
                  stop_loss_pct=(.1, 50), daily_loss_pct=(.1, 50), max_drawdown_pct=(.1, 90),
                  lot_size=(1, 100000), reference_price=(.0001, 1e9))
    if 'blackout_open_minutes' in p:
        bounds['blackout_open_minutes'] = (0, 60)
    if market == 'USStock' and p['blackout_lunch']:
        raise ValueError('USStock has no lunch break')
    if p['reference_price'] is None:
        raise ValueError('REFERENCE_PRICE_REQUIRED: enter a reference price to validate one lot cost')
    for key, limits in bounds.items():
        p[key] = number(p[key], *limits)
    for key in ('window_minutes', 'cooldown_minutes', 'max_triggers_day', 'blackout_close_minutes', 'lot_size', 'blackout_open_minutes'):
        if key not in p:
            continue
        if not p[key].is_integer():
            raise ValueError(f'{key} must be an integer')
        p[key] = int(p[key])
    lot_cost = p['lot_size'] * p['reference_price']
    spendable = p['budget'] * (1-p['cash_reserve_pct']/100)
    for key in ('per_order_cap', 'daily_buy_cap', 'max_position'):
        if p[key] < lot_cost:
            raise ValueError(f'CAP_BELOW_ONE_LOT: {key} must cover one lot ({lot_cost:g} {currency})')
        if p[key] > spendable:
            raise ValueError(f'CAP_UNREACHABLE: {key} exceeds budget after cash reserve ({spendable:g} {currency})')
    if not p['per_order_cap'] <= p['daily_buy_cap'] <= p['budget'] or p['max_position'] > p['budget'] * (1-p['cash_reserve_pct']/100):
        raise ValueError('Caps must fit budget and reserve; daily cap must cover each order')
    if p['mode'] not in {'plan_only', 'paper_auto'}:
        raise ValueError('Only plan_only / paper_auto are supported')
    if not isinstance(p['model'], str) or not re.fullmatch(r'[a-zA-Z0-9._:-]{1,200}', p['model']):
        raise ValueError('Invalid model identifier')
    return p


def read(row):
    code = eligible(row)
    saved = store.query('SELECT params FROM qd_agent_symbol_configs WHERE task_id=%s AND symbol=%s', (row['id'], code), one=True)
    return {'symbol': 'HK.'+code.removesuffix('.HK') if row['config']['market'] == 'HKStock' else 'US.'+code.removeprefix('US.'), 'params': {**PROFILES[row['config']['market']], **saved['params']} if saved else defaults(row),
            'applied': bool(saved), 'revision': row['revision'], 'active': row['active'],
            'effective_config': row['config'], 'lot_source': 'known_symbol' if code == '01810.HK' else 'configured; broker revalidated before submission'}


def defaults(row):
    if eligible(row) == '01810.HK':
        return dict(DEFAULTS)
    c = row['config']
    return {**PROFILES[c['market']], 'budget': c['budget'], 'per_order_cap': c['max_order_notional'],
            'daily_buy_cap': c['max_daily_notional'], 'cash_reserve_pct': c['reserve_ratio']*100,
            'max_position': c['budget']*c['max_weight'], 'mode': c['execution_mode'],
            'lot_size': 1 if c['market'] == 'USStock' else None, 'model': (c.get('llm_selection') or {}).get('model', DEFAULTS['model'])}


def materialize(config, p):
    c = copy.deepcopy(config)
    c.update(budget=p['budget'], max_order_notional=p['per_order_cap'], max_daily_notional=p['daily_buy_cap'],
             reserve_ratio=p['cash_reserve_pct']/100, max_weight=p['max_position']/p['budget'],
             execution_mode=p['mode'], cooldown_seconds=p['cooldown_minutes']*60, symbol_params=p)
    from app.services.automation.domain import validate_universe_source
    validate_universe_source(c)
    c['risk'] = dict(enabled=True, stop_loss_pct=p['stop_loss_pct']/100,
                     max_daily_loss_pct=p['daily_loss_pct']/100, max_drawdown_pct=p['max_drawdown_pct']/100,
                     daily_loss_budget=True)
    c['research']['max_decisions_per_day'] = p['max_triggers_day']
    # Explicit selection retains provider and fails closed if the model is unavailable.
    c['llm_selection'] = dict(provider=(c.get('llm_selection') or {}).get('provider', 'deepseek'),
                              model=p['model'], reasoning_effort='default')
    return c


def update(user_id, task_id, raw):
    row = store.task(user_id, task_id)
    eligible(row)
    p = validate(raw, row['config']['market'])
    config = materialize(row['config'], p)
    from app.services.llm_selection import validate_selection
    config['llm_selection'] = validate_selection(config['llm_selection'])
    updated = store.edit(user_id, task_id, row['name'], config, symbol_params=p, expected_revision=row['revision'])
    return read(updated)


def persist(cur, row, user_id, params):
    code = eligible(row)
    cur.execute('SELECT params FROM qd_agent_symbol_configs WHERE task_id=%s AND symbol=%s', (row['id'], code))
    saved = cur.fetchone()
    old = saved['params'] if saved else {}
    cur.execute('''INSERT INTO qd_agent_symbol_configs(task_id,symbol,params) VALUES(%s,%s,%s::jsonb)
        ON CONFLICT(task_id,symbol) DO UPDATE SET params=EXCLUDED.params,updated_at=NOW()''',
                (row['id'], code, store.dumps(params)))
    for field, value in params.items():
        if field not in old or old.get(field) != value:
            cur.execute('''INSERT INTO qd_agent_symbol_config_audit(task_id,symbol,user_id,field,old_value,new_value)
                VALUES(%s,%s,%s,%s,%s::jsonb,%s::jsonb)''',
                        (row['id'], code, user_id, field, store.dumps(old.get(field)), store.dumps(value)))


def history(row, before=None):
    code = eligible(row)
    before = int(before or 9223372036854775807)
    return store.query('''SELECT * FROM qd_agent_symbol_config_audit WHERE task_id=%s AND symbol=%s
        AND id<%s ORDER BY id DESC LIMIT 100''', (row['id'], code, before))


def check_daily_buy(cur, row, order, intent_id):
    """Reserve requested buy notional, including uncertain orders, on the market-local day."""
    cur.execute('''SELECT COALESCE(SUM((order_spec->>'qty')::numeric *
        (order_spec->>'limit_price')::numeric),0) AS used FROM qd_agent_trade_intents
        WHERE agent_token_id=%s AND id<>%s AND order_spec->>'side'='buy'
        AND status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED','FILLED','CANCELLED','FAILED')
        AND created_at >= (date_trunc('day', NOW() AT TIME ZONE %s) AT TIME ZONE %s)''',
                (row['token_id'], intent_id, *(['America/New_York' if row['config']['market'] == 'USStock' else 'Asia/Hong_Kong'] * 2)))
    if float(cur.fetchone()['used']) + order['qty'] * order['limit_price'] > row['config']['symbol_params']['daily_buy_cap']:
        raise ValueError('DAILY_BUY_CAP: symbol daily buy cap exceeded')


def validate_order(order, config, *, lot, broker_qty, cash):
    """Final sizing invariant check using the broker lot and execution snapshot."""
    lot = number(lot, 1)
    if not lot.is_integer():
        raise ValueError('INVALID_LOT: broker lot must be a positive integer')
    qty = number(order['qty'])
    qty = math.floor(qty / lot) * int(lot)
    order['qty'] = qty
    if qty < lot:
        raise ValueError('BELOW_ONE_LOT: order quantity is below one lot')
    notional = qty * number(order['limit_price'], .0001)
    if notional > config['max_order_notional']:
        raise ValueError('PER_ORDER_CAP: order exceeds per-order cap')
    if order['side'] == 'buy':
        if (number(broker_qty) + qty) * order['limit_price'] > config['budget'] * config['max_weight']:
            raise ValueError('MAX_POSITION: purchase exceeds maximum position')
        if notional > number(cash):
            raise ValueError('CASH_RESERVE: purchase exceeds cash after budget reserve')

from datetime import datetime
from decimal import Decimal

import pytest

from app.services.automation.domain import available_buy_cash, trigger_state
from app.services.automation.limit_price import round_tick, limit_price
from app.services.automation.symbol_config import US_DEFAULTS, validate, materialize, eligible
from tests.test_agent_automations import config
from tests.test_agent_automations_db import db  # noqa: F401


@pytest.mark.parametrize('price,side,expected', [
    ('1.009', 'buy', '1.00'), ('1.009', 'sell', '1.01'),
    ('.12345', 'buy', '.1234'), ('.12345', 'sell', '.1235'),
    ('.99999', 'sell', '1.0000'),
])
def test_us_tick(price, side, expected):
    assert round_tick(price, side, 'USStock') == Decimal(expected)


def test_cash_never_uses_margin():
    assert available_buy_cash({'cash': 1000000, 'power': 2000000}, 'USStock') == 1000000
    assert available_buy_cash({'cash': 1000000, 'power': 500000}, 'USStock') == 500000


@pytest.mark.parametrize('stamp,expected', [
    ('2026-10-12T13:34:59+00:00', False),
    ('2026-10-12T13:35:00+00:00', True),
    ('2026-11-02T14:34:59+00:00', False),
    ('2026-11-02T14:35:00+00:00', True),
    ('2026-11-02T17:30:00+00:00', True),  # ET noon, no lunch
    ('2026-11-02T20:55:00+00:00', False),
    ('2026-11-02T21:01:00+00:00', False),
    ('2026-11-01T16:00:00+00:00', False),
])
def test_us_dst_blackouts(stamp, expected):
    c = materialize(config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100}), US_DEFAULTS)
    now = datetime.fromisoformat(stamp).timestamp()
    assert trigger_state(c, dict(price=103, prev_close=100, is_realtime=True, as_of=now), {}, now)[0] is expected


def test_us_validation_and_quote():
    assert eligible({'config': config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100})}) == 'AAPL'
    assert validate({**US_DEFAULTS, 'reference_price': 250}, 'USStock')['lot_size'] == 1
    for changes in [{'lot_size': .5}, {'reference_price': None}, {'blackout_lunch': True}, {'blackout_open_minutes': 1.5}]:
        with pytest.raises(ValueError):
            validate({**US_DEFAULTS, 'reference_price': 250, **changes}, 'USStock')
    q = dict(price=250, bid=249.99, ask=250.019, as_of=100, simulate_execution_eligible=True)
    assert limit_price(q, 'buy', now=100, market='USStock') == (250.01, None)


def test_seed_is_inactive_plan_only_and_idempotent(db):
    from pathlib import Path
    from app.services.automation import store
    source = store.create(900001, 'US daily', config())
    sql = (Path(__file__).parents[1] / 'migrations/20261014_us_agent_symbol_seed.sql').read_text()
    for _ in range(2):
        with store.get_db_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(sql)
            connection.commit()
    rows = store.query("SELECT * FROM qd_agent_automations WHERE config->>'kind'='price_trigger'")
    assert len(rows) == 1
    row = rows[0]
    assert row['active'] is False
    assert row['state'] == {}
    assert row['config']['credential_id'] == source['config']['credential_id']
    assert row['config']['symbol_params'] == US_DEFAULTS
    assert row['config']['execution_mode'] == 'plan_only'
    assert store.query('SELECT status FROM qd_agent_tokens WHERE id=%s', (row['token_id'],), one=True)['status'] == 'internal'


def test_daily_buy_cap_uses_et_day(db):
    from app.services.automation import store
    from app.services.automation.symbol_config import check_daily_buy
    row = store.create(900001, 'US price', config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100}))
    with store.get_db_connection() as connection:
        with connection.cursor() as cur:
            for key, amount, offset in [('previous', 7000, '-1 second'), ('current', 5000, '1 second')]:
                cur.execute('''INSERT INTO qd_agent_trade_intents(user_id,agent_token_id,broker,account_ref,
                    idempotency_key,intent_hash,order_spec,status,created_at)
                    VALUES(900001,%s,'futu','credential:7',%s,'test',%s::jsonb,'UNCERTAIN',
                    (date_trunc('day',NOW() AT TIME ZONE 'America/New_York') AT TIME ZONE 'America/New_York') + %s::interval)''',
                    (row['token_id'], key, store.dumps(dict(side='buy', qty=1, limit_price=amount)), offset))
            check_daily_buy(cur, row, dict(qty=1, limit_price=1000), -1)
            with pytest.raises(ValueError, match='DAILY_BUY_CAP'):
                check_daily_buy(cur, row, dict(qty=1, limit_price=1001), -1)


@pytest.mark.parametrize('stamp,extended,expected', [
    ('2026-03-06T14:35:00+00:00', False, True),
    ('2026-03-09T13:35:00+00:00', False, True),
    ('2026-11-02T13:00:00+00:00', False, False),
    ('2026-11-02T13:00:00+00:00', True, True),
    ('2026-11-02T22:00:00+00:00', True, True),
    ('2026-11-03T01:00:00+00:00', True, False),
    ('2026-11-01T17:00:00+00:00', True, False),
    ('2026-11-27T17:55:00+00:00', False, False),  # early close 13:00 ET
])
def test_extended_hours_and_spring_dst(stamp, extended, expected):
    from app.services.automation.symbol_trigger import session_allowed
    c = materialize(config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100}),
                    {**US_DEFAULTS, 'extended_hours': extended})
    assert session_allowed(c, datetime.fromisoformat(stamp).timestamp()) is expected


def test_broker_lot_whole_shares():
    from app.services.automation.symbol_config import validate_order
    c = materialize(config(), US_DEFAULTS)
    order = dict(qty=3.9, limit_price=250, side='buy')
    validate_order(order, c, lot=1, broker_qty=0, cash=1000)
    assert order['qty'] == 3
    with pytest.raises(ValueError):
        validate_order(order, c, lot=.5, broker_qty=0, cash=1000)


def test_trigger_day_resets_at_et_midnight():
    c = materialize(config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100}),
                    {**US_DEFAULTS, 'extended_hours': True})
    previous = {'day': '2026-11-02', 'count': 8}
    now = datetime.fromisoformat('2026-11-03T00:30:00+00:00').timestamp()
    quote = dict(price=103, prev_close=100, is_realtime=True, as_of=now)
    fired, state = trigger_state(c, quote, previous, now)
    assert not fired and state['day'] == '2026-11-02' and state['count'] == 8
    now = datetime.fromisoformat('2026-11-03T14:35:00+00:00').timestamp()
    fired, state = trigger_state(c, {**quote, 'as_of': now}, state, now)
    assert fired and state['day'] == '2026-11-03' and state['count'] == 1


def test_us_runner_uses_broker_lot_and_final_tick(db, monkeypatch):
    from datetime import timedelta
    from unittest.mock import MagicMock
    from app.services.automation import runner, store
    now = datetime.fromisoformat('2026-10-12T14:00:00+00:00')
    row = store.create(900001, 'US price', config(kind='price_trigger', trigger={'type': 'price_above', 'price': 100}))
    c = row['config']
    c['execution_mode'] = 'paper_auto'
    c['risk']['enabled'] = False
    store.query('UPDATE qd_agent_automations SET config=%s::jsonb WHERE id=%s RETURNING id',
                (store.dumps(c), row['id']), one=True)
    row = store.set_active(900001, row['id'], True)
    run = store.create_run(row, 'protection:test', now + timedelta(seconds=60))
    run['result'] = {'items': [dict(symbol='AAPL', action='BUY', target_weight=.6,
                                   min_price=240, max_price=260, reason='fixture')]}
    store.update_run(run['id'], status='planned', result=run['result'])
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = dict(simulate_execution_eligible=True,
        price=250, bid=249.99, ask=250.019, as_of=now.timestamp())
    client.get_lot_size.return_value = 1
    client.get_max_cash_buy.return_value = None
    monkeypatch.setattr(runner, 'utcnow', lambda: now)
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', lambda *_: client)
    monkeypatch.setattr(runner, 'account_snapshot', lambda *_: dict(
        positions=[], open_orders=[], funds=dict(cash=1000000, power=2000000)))
    orders = []
    def submit(_user, _token, order, _key):
        orders.append(dict(order))
        return {'id': 1}
    def execute(*_args, **kwargs):
        with store.get_db_connection() as connection:
            with connection.cursor() as cur:
                kwargs['before_submit'](cur)
        return {'status': 'SUBMITTED'}
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', submit)
    monkeypatch.setattr('app.services.futu_agent_execution.execute_simulate_intent', execute)
    runner.execute(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'completed'
    assert len(orders) == 1 and orders[0]['limit_price'] == 250.01
    assert orders[0]['qty'] == 11
    client.get_lot_size.assert_called_once_with('AAPL')
    client.place_order.assert_not_called()

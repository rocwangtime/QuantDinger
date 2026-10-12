from pathlib import Path
from datetime import datetime, timezone

import pytest

from tests.test_agent_automations_db import db  # noqa: F401
from tests.test_agent_automations import config
from app.services.automation import store
from app.services.automation.symbol_config import DEFAULTS, read, update, history, validate
from app.services.automation.domain import trigger_state


def create():
    with store.get_db_connection() as connection:
        with connection.cursor() as cur:
            cur.execute((Path(__file__).parents[1]/'migrations/20261012_agent_symbol_config.sql').read_text())
        connection.commit()
    return store.create(900001, 'Xiaomi', config(market='HKStock', symbols=['01810'], kind='price_trigger', trigger={'type': 'price_above', 'price': 30}))


def test_atomic_settings_audit_and_ownership(db, monkeypatch):
    monkeypatch.setattr('app.services.llm_selection.model_catalog', lambda: {'items': [
        {'provider': 'deepseek', 'model': DEFAULTS['model'], 'reasoning_options': ['default']}
    ]})
    row = create()
    assert read(row)['params'] == DEFAULTS
    assert row['config']['budget'] == 100000
    assert len(history(row)) == len(DEFAULTS)
    changed = {**DEFAULTS, 'reference_price': 30, 'per_order_cap': 15000}
    result = update(900001, row['id'], changed)
    assert result['effective_config']['max_order_notional'] == 15000
    assert result['revision'] == row['revision']+1
    assert any(a['field'] == 'per_order_cap' and a['old_value'] == 20000 and a['new_value'] == 15000 for a in history(row))
    with pytest.raises(ValueError, match='not found'):
        update(900002, row['id'], changed)
    store.set_active(900001, row['id'], True)
    with pytest.raises(ValueError):
        update(900001, row['id'], DEFAULTS)
    assert read(store.task(900001, row['id']))['params'] == changed


@pytest.mark.parametrize('key,value', [('budget', float('nan')), ('lot_size', 1.5), ('lot_size', True), ('mode', 'live'), ('cash_reserve_pct', 99), ('max_position', 90000), ('window_minutes', 0), ('blackout_lunch', 'yes'), ('daily_buy_cap', 1000)])
def test_validation(key, value):
    with pytest.raises((ValueError, TypeError)):
        validate({**DEFAULTS, 'reference_price': 30, key: value})


def test_percent_edges_cooldown_daily_cap_blackout_and_stale(db):
    row = create()
    c = row['config']
    now = datetime(2026, 10, 12, 2, 0, tzinfo=timezone.utc).timestamp()
    def quote(price, t):
        return dict(price=price, prev_close=100, is_realtime=True, as_of=t)
    fire, state = trigger_state(c, quote(102.1, now), {}, now)
    assert fire and state['count'] == 1
    fire, state = trigger_state(c, quote(103, now+1), state, now+1)
    assert not fire
    _, state = trigger_state(c, quote(100, now+2), state, now+2)
    assert not trigger_state(c, quote(103, now+3), state, now+3)[0]
    assert trigger_state(c, quote(97, now+4), state, now+4)[0]
    lunch = datetime(2026, 10, 12, 4, 30, tzinfo=timezone.utc).timestamp()
    assert not trigger_state(c, quote(103, lunch), {}, lunch)[0]
    assert not trigger_state(c, quote(103, now-6), {}, now)[0]
    state = {'day': '2026-10-12', 'count': 8}
    assert not trigger_state(c, quote(103, now), state, now)[0]
    state = {'day': '2026-10-12', 'count': 0, 'samples': [[now-300, 100]]}
    q = quote(101.1, now)
    q['prev_close'] = None
    assert trigger_state(c, q, state, now)[0]


def test_symbol_endpoints_require_login(client):
    for method, path in [('get', '/api/agent-automations/1/symbol-config'),
                         ('put', '/api/agent-automations/1/symbol-config'),
                         ('get', '/api/agent-automations/1/symbol-config/audit')]:
        assert getattr(client, method)(path).status_code == 401


def test_daily_buy_reservation_and_manual_risk_confirm(db):
    from app.services.automation.symbol_config import check_daily_buy
    row = create()
    with store.get_db_connection() as connection:
        with connection.cursor() as cur:
            cur.execute('''INSERT INTO qd_agent_trade_intents(user_id,agent_token_id,broker,account_ref,
                idempotency_key,intent_hash,order_spec,status) VALUES(%s,%s,'futu','credential:7',
                'cap-fixture','test',%s::jsonb,'UNCERTAIN')''',
                        (900001, row['token_id'], store.dumps(dict(side='buy',qty=200,limit_price=150))))
            check_daily_buy(cur, row, dict(qty=200, limit_price=50), -1)
            with pytest.raises(ValueError, match='daily buy cap'):
                check_daily_buy(cur, row, dict(qty=200, limit_price=51), -1)
        connection.commit()
    store.query("UPDATE qd_agent_automations SET state=%s::jsonb WHERE id=%s RETURNING id",
                (store.dumps({'risk': {'halted': True}}), row['id']), one=True)
    with pytest.raises(ValueError, match='Manual risk reset'):
        store.set_active(900001, row['id'], True)


def test_unavailable_model_cannot_change_settings(db, monkeypatch):
    row = create()
    monkeypatch.setattr('app.services.llm_selection.model_catalog', lambda: {'items': []})
    with pytest.raises(ValueError, match='Selected model is not configured'):
        update(900001, row['id'], {**DEFAULTS, 'reference_price': 30, 'model': 'missing-model'})
    assert len(history(row)) == len(DEFAULTS)
    assert store.task(900001, row['id'])['revision'] == row['revision']


def test_drawdown_monitor_pauses_and_preserves_positions(db, monkeypatch):
    import time
    from unittest.mock import MagicMock
    from app.services.automation import monitor
    row = create()
    state = {'baseline': {'01810.HK': 1000}, 'performance': {
        'capital': 100000, 'baseline_prices': {'01810.HK': 100}, 'high_water': 100000}}
    store.query('UPDATE qd_agent_automations SET active=TRUE,state=%s::jsonb WHERE id=%s RETURNING id',
                (store.dumps(state), row['id']), one=True)
    row = store.task(900001, row['id'])
    account = {'as_of': time.time(), 'positions': [{'symbol': '01810.HK', 'quantity': 1000, 'side': 'long'}], 'open_orders': []}
    monkeypatch.setattr(monitor, 'account_snapshot', lambda *_: account)
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = {'simulate_execution_eligible': True, 'price': 90}
    monkeypatch.setattr(monitor, '_load_client', lambda *_: client)
    monitor.observe(row)
    current = store.task(900001, row['id'])
    assert not current['active'] and current['state']['risk']['halted']
    assert 'drawdown_limit' in current['state']['risk']['reasons']
    assert current['state']['baseline'] == state['baseline']
    assert store.query('SELECT id FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],)) == []


def test_daily_loss_uses_budget_not_daily_equity():
    from app.services.automation.symbol_config import materialize
    from app.services.automation.performance import evaluate
    from zoneinfo import ZoneInfo
    c = materialize(config(), DEFAULTS)
    now = datetime(2026, 10, 12, 2, 0, tzinfo=timezone.utc).timestamp()
    day = datetime.fromtimestamp(now, ZoneInfo('Asia/Hong_Kong')).date().isoformat()
    c['market'] = 'HKStock'
    state = {'performance': {'day': day, 'day_equity': 200000, 'high_water': 200000}}
    report = {'equity': 197500, 'capital': 100000, 'positions': []}
    _, risk, _ = evaluate(c, state, report, {}, now)
    assert 'daily_loss_limit' in risk['reasons']


@pytest.mark.parametrize('key', ['per_order_cap', 'daily_buy_cap', 'max_position'])
def test_cap_must_cover_one_lot(key):
    with pytest.raises(ValueError, match='CAP_BELOW_ONE_LOT'):
        validate({**DEFAULTS, 'reference_price': 101, key: 20000})


@pytest.mark.parametrize('key', ['per_order_cap', 'daily_buy_cap', 'max_position'])
def test_caps_reachable_after_reserve(key):
    with pytest.raises(ValueError, match='CAP_UNREACHABLE'):
        validate({**DEFAULTS, 'reference_price': 30, key: 90000})


def test_exact_lot_cost_and_missing_reference():
    assert validate({**DEFAULTS, 'reference_price': 100})['per_order_cap'] == 20000
    with pytest.raises(ValueError, match='REFERENCE_PRICE_REQUIRED'):
        validate(DEFAULTS)


@pytest.mark.parametrize('qty,cash,broker_qty,code', [
    (199, 80000, 0, 'BELOW_ONE_LOT'), (600, 80000, 0, 'PER_ORDER_CAP'),
    (200, 9999, 0, 'CASH_RESERVE'), (200, 80000, 1100, 'MAX_POSITION')])
def test_final_order_limits(qty, cash, broker_qty, code):
    from app.services.automation.symbol_config import materialize, validate_order
    c = materialize(config(), DEFAULTS)
    with pytest.raises(ValueError, match=code):
        validate_order(dict(qty=qty, limit_price=50, side='buy'), c,
                       lot=200, broker_qty=broker_qty, cash=cash)


def test_final_order_rounds_down_and_sell_ignores_buy_constraints():
    from app.services.automation.symbol_config import materialize, validate_order
    order = dict(qty=399, limit_price=50, side='sell')
    validate_order(order, materialize(config(), DEFAULTS), lot=200, broker_qty=10000, cash=0)
    assert order['qty'] == 200


@pytest.mark.parametrize('cash,price,position,expected', [
    (25000, 50, 0, 'CASH_RESERVE'),
    (100000, 101, 0, 'PER_ORDER_CAP'),
    (100000, 50, 1100, 'MAX_POSITION'),
])
def test_hk_runner_rejects_unaffordable_lot_before_intent(db, monkeypatch, cash, price, position, expected):
    from unittest.mock import MagicMock
    from datetime import timedelta
    from app.services.automation import runner
    row = create()
    c = row['config']
    c['execution_mode'] = 'paper_auto'
    c['risk']['enabled'] = False
    store.query('UPDATE qd_agent_automations SET config=%s::jsonb WHERE id=%s RETURNING id',
                (store.dumps(c), row['id']), one=True)
    row = store.set_active(900001, row['id'], True)
    run = store.create_run(row, 'protection:test', datetime.now(timezone.utc) + timedelta(seconds=60))
    item = dict(symbol='01810.HK', action='BUY', target_weight=.6, min_price=1, max_price=200, reason='fixture')
    run['result'] = {'items': [item]}
    store.update_run(run['id'], status='planned', result=run['result'])
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = dict(simulate_execution_eligible=True, price=price)
    client.get_lot_size.return_value = 200
    client.get_max_cash_buy.return_value = 10000
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', lambda *_: client)
    monkeypatch.setattr(runner, 'account_snapshot', lambda *_: dict(
        positions=[dict(symbol='01810.HK', quantity=position, side='long', marketValue=position*price)],
        open_orders=[], funds=dict(cash=cash, power=0)))
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent', lambda *_: pytest.fail('Must reject before intent'))
    runner.execute(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['result']['execution_checks'][0]['reason_code'] == expected
    client.place_order.assert_not_called()

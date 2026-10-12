"""Disposable local PG18 replay with offline market observations."""
from datetime import datetime, timedelta, timezone

import pytest

from app.services.automation import scoring, store
from tests.test_agent_automations_db import db, create  # noqa: F401

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('mode', ['plan_only', 'paper_auto'])
def test_persist_recover_score_and_refresh_daily_report(db, monkeypatch, mode):
    row = create(market='HKStock', symbols=['01810.HK'], execution_mode=mode,
                 kind='price_trigger', trigger={'type': 'price_above', 'price': 100})
    now = datetime(2026, 10, 12, 10, tzinfo=timezone.utc)
    decision_at = datetime(2026, 10, 9, 2, tzinfo=timezone.utc)
    config = row['config']
    config['execution_mode'] = mode
    config['symbol_params'] = {**config.get('symbol_params', {}), 'lot_size': 200}
    store.query('UPDATE qd_agent_automations SET config=%s::jsonb WHERE id=%s RETURNING id', (store.dumps(config), row['id']))
    run = store.create_run(row, 'price:1:fixture', now+timedelta(seconds=30), preview=True)
    store.query('UPDATE qd_agent_automation_runs SET preview=FALSE,created_at=%s WHERE id=%s RETURNING id',
                (decision_at, run['id']))
    evidence = {'task_config': config, 'instruments': {'01810.HK': {'price': 100, 'quote_as_of': decision_at.timestamp()}},
                'account': {'funds': {'cash': 100000, 'power': 0}, 'positions': [], 'open_orders': []},
                'managed_quantities': {}}
    result = {'decision_at': decision_at.timestamp(), 'items': [{'symbol': '01810.HK', 'action': 'BUY',
        'target_weight': .5, 'min_price': 99, 'max_price': 101, 'reason': 'fixture'}],
        'fast_latency': {'elapsed_ms': {'trigger_detected': 0, 'context_built': 10, 'model_call': 20,
                                     'decision': 30, 'submitted': 40, 'finished': 50}}}
    if mode == 'paper_auto':
        result.pop('decision_at')
        result['fast_latency']['timestamps'] = {'decision': decision_at.timestamp()}
    store.update_run(run['id'], evidence=evidence, result=result, status='completed', finished_at=decision_at)
    def bars(row, score):
        return [{'time': due.timestamp()-60, 'close': 102} for due in scoring.horizons(decision_at).values()
                if due.astimezone(scoring.HK).date() == score['due_at'].astimezone(scoring.HK).date()]
    monkeypatch.setattr(scoring, 'fetch_bars', bars)
    scoring.tick(now)
    scores = store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))
    assert len(scores) == 4 and all(s['outcome']['direction_hit'] for s in scores)
    assert all(s['inputs']['quantity'] == 200 and s['inputs']['execution_mode'] == mode for s in scores)
    assert all(s['outcome']['would_be_pnl_hkd'] == 400 for s in scores)
    # Partial registration after a crash gets repaired without duplicate outcomes.
    store.query("DELETE FROM qd_agent_decision_scores WHERE run_id=%s AND horizon='30m' RETURNING run_id", (run['id'],))
    scoring.tick(now+timedelta(minutes=1))
    assert len(store.query('SELECT * FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))) == 4
    report = scoring.read_report(row, '2026-10-09')['report']
    assert report['decisions'] == 1 and report['triggers'] == {'price': 1}
    assert report['symbols']['01810.HK']['horizons']['next_close']['hit_rate'] == 1
    assert report['latency']['stages_ms']['total']['p95'] == 50
    # Missing history remains pending; later successful observation fills it.
    store.query("UPDATE qd_agent_decision_scores SET outcome=NULL,checked_at=NULL WHERE run_id=%s AND horizon='5m' RETURNING run_id", (run['id'],))
    monkeypatch.setattr(scoring, 'fetch_bars', lambda *_: [])
    scoring.tick(now+timedelta(minutes=2))
    assert scoring.read_report(row, '2026-10-09')['report']['symbols']['01810.HK']['horizons']['5m']['pending_or_missing'] == 1
    monkeypatch.setattr(scoring, 'fetch_bars', bars)
    scoring.tick(now+timedelta(minutes=13))
    assert scoring.read_report(row, '2026-10-09')['report']['symbols']['01810.HK']['horizons']['5m']['hit_rate'] == 1
    # A delayed cumulative fill updates slippage without replacing horizon prices.
    spec = {'symbol': '01810.HK', 'side': 'buy', 'strategy_version': f"automation:{row['id']}:{run['id']}"}
    receipt = store.query('''INSERT INTO qd_agent_trade_intents
        (user_id,agent_token_id,broker,account_ref,idempotency_key,intent_hash,order_spec,status,filled_qty,avg_fill_price,updated_at)
        VALUES(%s,%s,'futu','credential:7','late-fill','fixture',%s::jsonb,'PARTIALLY_FILLED',100,101,%s) RETURNING id''',
        (row['user_id'], row['token_id'], store.dumps(spec), now+timedelta(minutes=14)), one=True)
    monkeypatch.setattr(scoring, 'fetch_bars', lambda *_: pytest.fail('Scored historical mark must be immutable'))
    scoring.tick(now+timedelta(minutes=15))
    filled = store.query('SELECT outcome FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))
    assert all(s['outcome']['filled_qty'] == 100 and s['outcome']['price'] == 102 for s in filled)
    store.query('UPDATE qd_agent_trade_intents SET filled_qty=200,avg_fill_price=100.5,status=%s,updated_at=%s WHERE id=%s RETURNING id',
                ('FILLED', now+timedelta(minutes=16), receipt['id']))
    scoring.tick(now+timedelta(minutes=17))
    scoring.tick(now+timedelta(minutes=18))
    filled = store.query('SELECT outcome FROM qd_agent_decision_scores WHERE run_id=%s', (run['id'],))
    assert all(s['outcome']['filled_qty'] == 200 and s['outcome']['slippage_bps'] == pytest.approx(50) for s in filled)
    report = scoring.read_report(row, '2026-10-09')['report']
    assert report['symbols']['01810.HK']['horizons']['close']['avg_slippage_bps'] == pytest.approx(50)
    with pytest.raises(ValueError):
        store.task(900002, row['id'])


def test_risk_transition_survives_reset_and_does_not_repeat(db, monkeypatch):
    import time
    from unittest.mock import MagicMock
    from app.services.automation import monitor
    row = create(market='HKStock', symbols=['00700.HK'], execution_mode='paper_auto',
                 risk={'enabled': True, 'max_drawdown_pct': .03})
    row = store.set_active(row['user_id'], row['id'], True)
    spec = {'symbol': '00700.HK', 'side': 'buy'}
    store.query('''INSERT INTO qd_agent_trade_intents
        (user_id,agent_token_id,broker,account_ref,idempotency_key,intent_hash,order_spec,status,filled_qty,avg_fill_price)
        VALUES(%s,%s,'futu','credential:7','risk-fill','fixture',%s::jsonb,'FILLED',5,100) RETURNING id''',
        (row['user_id'], row['token_id'], store.dumps(spec)), one=True)
    account = {'as_of': time.time(), 'positions': [{'symbol': '00700.HK', 'quantity': 5, 'side': 'long'}],
               'open_orders': [], 'funds': {'cash': 500, 'power': 0}}
    monkeypatch.setattr(monitor, 'account_snapshot', lambda *_: account)
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = {'simulate_execution_eligible': True, 'price': 100}
    monkeypatch.setattr(monitor, '_load_client', lambda *_: client)
    monitor.observe(row)
    row = store.set_active(row['user_id'], row['id'], False)
    client.get_simulate_execution_quote.return_value['price'] = 90
    monitor.observe(row)
    monitor.observe(store.task(row['user_id'], row['id']))
    events = store.query('SELECT * FROM qd_agent_risk_events WHERE task_id=%s', (row['id'],))
    assert len(events) == 1
    assert events[0]['event']['reasons'] == ['daily_loss_limit', 'drawdown_limit']
    assert events[0]['event']['stop_loss_symbols'] == ['00700.HK']
    store.reset_risk(row['user_id'], row['id'])
    assert len(store.query('SELECT id FROM qd_agent_risk_events WHERE task_id=%s', (row['id'],))) == 1
    start = events[0]['occurred_at'].astimezone(scoring.HK).replace(hour=0, minute=0, second=0, microsecond=0)
    assert len(scoring.daily(row, start, start+timedelta(days=1))['risk_events']) == 1


def test_daily_generation_waits_until_after_close_and_includes_zero_decisions(db, monkeypatch):
    row = create(market='HKStock', symbols=['00700.HK'])
    local = datetime.now(scoring.HK)
    before = local.replace(hour=16, minute=59, second=0, microsecond=0)
    after = local.replace(hour=17, minute=1, second=0, microsecond=0)
    monkeypatch.setattr(scoring, 'fetch_bars', lambda *_: pytest.fail('No score may fetch market data'))
    scoring.tick(before)
    assert scoring.read_report(row, local.date().isoformat())['report'] is None
    scoring.tick(after)
    report = scoring.read_report(row, local.date().isoformat())['report']
    assert report['decisions'] == 0 and report['triggered_runs'] == 0
    assert report['symbols']['00700.HK']['horizons']['close']['hit_rate'] is None
    assert report['observed_equity_change_hkd'] is None

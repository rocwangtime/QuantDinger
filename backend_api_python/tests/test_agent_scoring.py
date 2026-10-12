"""Deterministic HK horizon semantics and owner-scoped report contracts."""
from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

from app.routes import agent_automations as routes
from app.services.automation import scoring

AT = datetime(2026, 10, 9, 7, 45, 12, tzinfo=timezone.utc)


def test_horizons_skip_weekend_and_hk_holiday():
    times = scoring.horizons(AT)
    assert times['5m'] == AT + timedelta(minutes=5)
    assert times['close'] == datetime(2026, 10, 9, 8, tzinfo=timezone.utc)
    assert times['next_close'] == datetime(2026, 10, 12, 8, tzinfo=timezone.utc)
    holiday = datetime(2026, 10, 1, 2, tzinfo=timezone.utc)
    assert scoring.horizons(holiday)['close'].day == 2


@pytest.mark.parametrize('action,direction', [('BUY', 1), ('HOLD', 1), ('REDUCE', -1), ('EXIT', -1), ('WAIT', 0)])
def test_direction_lots_and_fill_slippage(action, direction):
    due = AT + timedelta(minutes=5)
    score = {'symbol': '01810.HK', 'horizon': '5m', 'due_at': due,
             'inputs': {'action': action, 'price': 100, 'quantity': 200}}
    bars = [{'time': due.replace(second=0).timestamp(), 'close': 102}]
    receipts = [{'order_spec': {'symbol': '01810.HK', 'side': 'buy' if direction == 1 else 'sell'},
                 'filled_qty': 100, 'avg_fill_price': 101}]
    value = scoring.outcome(score, bars, receipts)
    assert value['would_be_pnl_hkd'] == 400 * direction
    assert value['direction_hit'] == (direction > 0 if direction else None)
    assert value['slippage_bps'] == pytest.approx(100 * direction) if direction else value['slippage_bps'] is None
    assert value['return_pct'] == pytest.approx(2)


def test_no_later_session_or_adjusted_price_substitution():
    score = {'symbol': '01810.HK', 'horizon': 'close', 'due_at': AT,
             'inputs': {'action': 'BUY', 'price': 100, 'quantity': None}}
    assert scoring.outcome(score, [{'time': AT.timestamp(), 'close': 102}], []) is None
    value = scoring.outcome(score, [{'time': AT.timestamp()-60, 'close': 102}], [])
    assert value['would_be_pnl_hkd'] is None
    assert value['slippage_bps'] is None


def test_report_and_score_endpoints_require_owner(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(routes.blp, url_prefix='/api/agent-automations')
    def task(user, task_id):
        if user != 42 or task_id != 7:
            raise ValueError('Task not found')
        return {'id': task_id}
    monkeypatch.setattr(routes.store, 'task', task)
    monkeypatch.setattr(scoring, 'read_report', lambda row, day: {'report': {'day': day}})
    monkeypatch.setattr(routes.store, 'query', lambda sql, args, **kw: None)
    client = app.test_client()
    assert client.get('/api/agent-automations/7/daily-report?day=2026-10-09').status_code == 401
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 42, '_verified_user_role': 'user'})
    headers = {'Authorization': 'Bearer fixture-only'}
    assert client.get('/api/agent-automations/7/daily-report?day=2026-10-09', headers=headers).status_code == 200
    assert client.get('/api/agent-automations/8/daily-report?day=2026-10-09', headers=headers).status_code == 400
    assert client.get('/api/agent-automations/runs/8/scores', headers=headers).status_code == 400


def test_whole_lot_hypothetical_quantities_preserve_owned_and_cash_limits():
    from tests.test_agent_automations import config
    cfg = config(market='HKStock', symbols=['00700.HK'], budget=100000,
                 max_weight=.6, max_order_notional=40000, max_daily_notional=80000)
    item = {'symbol': '00700.HK', 'action': 'BUY', 'target_weight': .6,
            'min_price': 90, 'max_price': 110, 'reason': 'fixture'}
    evidence = {'account': {'funds': {'cash': 45000, 'power': 0},
        'positions': [{'symbol': '00700.HK', 'quantity': 200, 'side': 'long', 'marketValue': 20000}],
        'open_orders': []}, 'managed_quantities': {'00700.HK': 200}}
    assert scoring.hypothetical_quantity(item, cfg, evidence, 100, 200) == 400
    assert scoring.hypothetical_quantity({**item, 'action': 'HOLD'}, cfg, evidence, 100, 200) == 200
    assert scoring.hypothetical_quantity({**item, 'action': 'WAIT'}, cfg, evidence, 100, None) == 0
    assert scoring.hypothetical_quantity(item, cfg, evidence, 100, None) is None
    assert scoring.hypothetical_quantity({**item, 'action': 'EXIT', 'target_weight': 0}, cfg, evidence, 100, 200) == 200
    evidence['account']['funds']['cash'] = 19999
    assert scoring.hypothetical_quantity(item, cfg, evidence, 100, 200) == 0


def test_registration_excludes_preview_and_legacy_decisions(monkeypatch):
    monkeypatch.setattr(scoring.store, 'query', lambda *_args, **_kw: pytest.fail('Not an eligible decision'))
    for preview, result in [(True, {'decision_at': AT.timestamp()}), (False, {})]:
        scoring.register({'preview': preview, 'result': result, 'evidence': {'task_config': {'market': 'HKStock'}}})


def test_horizon_summary_excludes_wait_and_preserves_unknown_data():
    base = {'return_pct': 2, 'directional_return_pct': 2, 'direction_hit': True,
            'would_be_pnl_hkd': 400, 'slippage_bps': 50}
    rows = [{'horizon': '5m', 'outcome': base},
            {'horizon': '5m', 'outcome': {**base, 'direction_hit': None,
                'directional_return_pct': None, 'would_be_pnl_hkd': 0, 'slippage_bps': None}},
            {'horizon': '5m', 'outcome': None}]
    summary = scoring.summarize(rows)
    assert summary['5m']['hit_rate'] == 1 and summary['5m']['hit_samples'] == 1
    assert summary['5m']['scored'] == 2 and summary['5m']['pending_or_missing'] == 1
    assert summary['5m']['would_be_pnl_hkd'] == 400
    assert summary['close']['hit_rate'] is None and summary['close']['would_be_pnl_hkd'] is None


def test_finite_job_is_on_maintenance_schedule():
    from app.celery_app import celery_app
    from app.tasks.maintenance import hk_decision_scoring
    assert celery_app.conf.beat_schedule['hk-decision-scoring']['schedule'] == 60
    assert celery_app.conf.task_routes[hk_decision_scoring.name]['queue'] == 'maintenance'
    assert hk_decision_scoring.time_limit == 300


def test_existing_fast_path_decisions_use_reliable_persisted_timestamp():
    assert scoring.decision_timestamp({'fast_latency': {'timestamps': {'decision': AT.timestamp()}}}) == AT.timestamp()
    assert scoring.decision_timestamp({'items': [], 'finished_at': AT.timestamp()}) is None

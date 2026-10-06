"""Independent paper-portfolio observations and protective execution plans."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from app.services.automation import store
from app.services.automation.market import account_snapshot
from app.services.automation.performance import book, evaluate, positive
from app.services.futu_agent_execution import _load_client


def observe(row):
    """No LLM calls. Quotes must satisfy the existing execution eligibility gate."""
    from app.services.automation.worker import STOP
    account = account_snapshot(row['user_id'], row['config'])
    owned = [code for code, qty in store.owned_quantities(row).items() if qty > 0]
    codes = list(dict.fromkeys(row['config']['symbols'] + owned
                              + (row['state'].get('performance') or {}).get('symbols', [])))
    if len(codes) > 60:
        raise ValueError('Too many tracked symbols')
    prices = {}
    client = _load_client(row['user_id'], f"credential:{row['config']['credential_id']}")
    try:
        if not client.connect():
            raise ValueError('OpenD unavailable')
        for code in codes:
            if STOP.is_set():
                return
            quote = client.get_simulate_execution_quote(code)
            if quote.get('simulate_execution_eligible') and positive(quote.get('price')):
                prices[code] = float(quote['price'])
    finally:
        client.disconnect()
    # Read the current receipts/state under the same lock as pause and broker
    # submission. The remote read happened before taking this database lock.
    now = time.time()
    with store.get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (row['user_id'],))
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s FOR UPDATE', (row['id'],))
            current = dict(cur.fetchone())
            if current['revision'] != row['revision'] or STOP.is_set():
                return
            state = dict(current['state'] or {})
            cur.execute('SELECT id,status,order_spec,filled_qty,avg_fill_price FROM qd_agent_trade_intents WHERE agent_token_id=%s ORDER BY id',
                        (current['token_id'],))
            receipts = [dict(r) for r in cur.fetchall()]
            report = book(current['config'], state, receipts, prices, account)
            # With no holdings, stale/missing quotes would still produce cash
            # equity. Do not authorize purchases without a marked universe.
            if not all(s in prices for s in current['config']['symbols']):
                report['errors'].append('universe_quotes_unavailable')
                report['equity'] = report['return_pct'] = None
            if now - account['as_of'] > 30:
                report['errors'].append('observation_stale')
                report['equity'] = report['return_pct'] = None
                prices = {}
                for position in report['positions']:
                    position['price'] = None
            report['account_as_of'] = account['as_of']
            tracking, risk, exits = evaluate(current['config'], state, report, prices, now)
            if current['config']['execution_mode'] != 'paper_auto':
                exits = []
            previous_risk = state.get('risk') or {}
            state.update(performance=tracking, risk=risk)
            if report['equity'] is not None and now - float(tracking.get('last_sample_at', 0)) >= 60:
                cur.execute('INSERT INTO qd_agent_automation_samples(task_id,sampled_at,report) VALUES(%s,%s,%s::jsonb)',
                            (current['id'], datetime.fromtimestamp(now, timezone.utc), store.dumps(report)))
                tracking['last_sample_at'] = now
            newly_tripped = (risk.get('halted') and not previous_risk.get('halted')) or (
                set(risk.get('stopped_symbols', [])) - set(previous_risk.get('stopped_symbols', [])))
            if newly_tripped:
                # In-flight model runs observe cancellation on their next poll.
                # Reductions already accepted by the broker remain reconciled.
                cur.execute("""UPDATE qd_agent_automation_runs SET cancel_requested=TRUE,
                    status=CASE WHEN status IN ('queued','planned') THEN 'cancelled' ELSE status END
                    WHERE task_id=%s AND event_key NOT LIKE 'protection:%%'
                    AND status IN ('queued','researching','planned','executing')""", (current['id'],))
            if current['active'] and exits and not STOP.is_set():
                cur.execute("""SELECT id FROM qd_agent_automation_runs WHERE task_id=%s
                    AND status IN ('queued','researching','planned','executing') LIMIT 1""", (current['id'],))
                busy = cur.fetchone()
                cur.execute("SELECT id FROM qd_agent_trade_intents WHERE agent_token_id=%s AND status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED') LIMIT 1",
                            (current['token_id'],))
                outstanding = cur.fetchone()
                if not busy and not outstanding:
                    expiry = datetime.fromtimestamp(now, timezone.utc) + timedelta(seconds=45)
                    result = {'summary': '独立持仓保护已触发，准备减仓', 'items': exits,
                              'source': 'deterministic_protection', 'prompt_version': None}
                    cur.execute("""INSERT INTO qd_agent_automation_runs
                        (task_id,user_id,revision,event_key,status,phase,result,evidence,expires_at)
                        VALUES(%s,%s,%s,%s,'planned','独立保护等待执行',%s::jsonb,%s::jsonb,%s)
                        ON CONFLICT DO NOTHING""",
                        (current['id'], current['user_id'], current['revision'],
                         f"protection:{current['revision']}:{int(now // 60)}", store.dumps(result),
                         store.dumps({'as_of': now, 'account': account, 'prices': prices, 'risk': risk,
                                      'task_config': current['config']}), expiry))
            cur.execute('UPDATE qd_agent_automations SET state=%s::jsonb WHERE id=%s', (store.dumps(state), current['id']))
            db.commit()
        finally:
            cur.close()


def unavailable(row):
    """Persist an observable failure without leaking provider/credential errors."""
    store.query("""UPDATE qd_agent_automations SET state=jsonb_set(state,'{risk}',
        COALESCE(state->'risk','{}'::jsonb) || %s::jsonb) WHERE id=%s AND revision=%s RETURNING id""",
                (store.dumps({'healthy': False, 'checked_at': time.time(), 'monitor_error': '账户或报价不可用，暂停新增买入'}),
                 row['id'], row['revision']), one=True)


def dashboard(row):
    """Cached dashboard never invokes a model or submits broker orders."""
    runs = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s ORDER BY id DESC LIMIT 30', (row['id'],))
    usage_rows = store.query("SELECT result->'usage' AS usage FROM qd_agent_automation_runs WHERE task_id=%s AND result ? 'usage'",
                             (row['id'],))
    usage = [r['usage'] for r in usage_rows]
    costs = {}
    unknown = 0
    for item in usage:
        if item.get('estimated_cost') is None or not item.get('currency'):
            unknown += 1
        else:
            currency = item['currency']
            costs[currency] = costs.get(currency, 0.) + item['estimated_cost']
    samples = store.query('''SELECT sampled_at,report FROM (SELECT sampled_at,report
        FROM qd_agent_automation_samples WHERE task_id=%s ORDER BY sampled_at DESC LIMIT 1440) s ORDER BY sampled_at''', (row['id'],))
    return {'performance': (row['state'] or {}).get('performance') or {},
            'risk': (row['state'] or {}).get('risk') or {}, 'series': samples,
            'orders': store.receipts(row), 'runs': runs,
            'model_usage': {'recorded_calls': len(usage), 'total_tokens': sum(u.get('total_tokens', 0) for u in usage),
                            'estimated_cost_by_currency': costs, 'unpriced_calls': unknown,
                            'coverage': 'completed_decisions_only'}}

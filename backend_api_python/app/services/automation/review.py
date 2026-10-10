"""Bounded repeatable-read reports for actual forward paper observations."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.services.automation import store

RUN_LIMIT, SAMPLE_LIMIT, ORDER_LIMIT = 1000, 10000, 1000


def timestamp(value):
    if isinstance(value, datetime):
        return (value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value).timestamp()
    return float(value)


def assemble(row, runs, samples, orders, since, now, truncated):
    zone = ZoneInfo('America/New_York' if row['config']['market'] == 'USStock' else 'Asia/Hong_Kong')
    statuses, actions, causes = Counter(), Counter(), Counter()
    model_calls = input_tokens = output_tokens = unpriced = estimates = 0
    costs, sessions = {}, {}
    previews = decisions = protective = 0
    for run in runs:
        statuses[run['status']] += 1
        protective += int(run.get('source') == 'deterministic_protection')
        if run.get('has_decision') and run.get('source') != 'deterministic_protection':
            decisions += 1
            previews += int(bool(run.get('preview')))
            actions.update(i['action'] for i in run.get('items') or [])
            causes[(run.get('event') or {}).get('type', 'preview' if run.get('preview') else 'scheduled_or_price_trigger')] += 1
        usages = [c['usage'] for c in run.get('calls') or []] or ([run['usage']] if run.get('usage') else [])
        for usage in usages:
            model_calls += 1
            input_tokens += usage.get('input_tokens', 0)
            output_tokens += usage.get('output_tokens', 0)
            estimates += int(usage.get('token_source') != 'provider')
            unit, cost = usage.get('currency'), usage.get('estimated_cost')
            if unit and cost is not None:
                costs[unit] = costs.get(unit, 0) + cost
            else:
                unpriced += 1
    for sample in sorted(samples, key=lambda s: timestamp(s['sampled_at'])):
        report = sample['report']
        if report.get('equity') is None:
            continue
        stamp = timestamp(sample['sampled_at'])
        day = datetime.fromtimestamp(stamp, zone).date().isoformat()
        if day not in sessions:
            sessions[day] = {'day': day, 'first_mark_at': stamp, 'first_equity': report['equity'], 'samples': 0}
        session = sessions[day]
        session.update(last_mark_at=stamp, last_equity=report['equity'])
        session['samples'] += 1
        session['observed_change_pct'] = (session['last_equity']/session['first_equity']-1)*100 if session['first_equity'] > 0 and session['samples'] > 1 else None
    state = row.get('state') or {}
    latest = (state.get('performance') or {}).get('latest_valid') or None
    return {'schema_version': 'agent-paper-review-v1', 'generated_at': now,
            'task': {k: row[k] for k in ('id', 'name', 'revision', 'active')},
            'configuration': {k: row['config'].get(k) for k in ('market', 'symbols', 'kind', 'execution_mode', 'budget', 'research', 'risk', 'llm_selection')},
            'window': {'since': since, 'until': now, 'timezone': str(zone)},
            'coverage': {'partial': any(truncated.values()), 'truncated': truncated,
                         'run_rows': len(runs), 'sample_rows': len(samples), 'order_rows': len(orders),
                         'model_usage': 'completed_snapshot_decisions_and_recorded_tool_loop_attempts',
                         'order_scope': 'task_orders_updated_in_window_cumulative_fills'},
            'decisions': {'valid_model_decisions': decisions, 'previews': previews, 'protective_runs': protective,
                          'statuses': dict(statuses), 'proposed_actions': dict(actions), 'causes': dict(causes)},
            'model_usage': {'recorded_calls': model_calls, 'input_tokens': input_tokens, 'output_tokens': output_tokens,
                            'estimated_token_calls': estimates, 'unpriced_calls': unpriced, 'estimated_cost_by_currency': costs},
            'observed_sessions': list(sessions.values()), 'latest_valid_task_performance': latest,
            'current_risk': state.get('risk') or {}, 'orders': orders,
            'limitations': ['gross_before_fees_and_dividends', 'observed_marks_not_full_session_returns',
                            'no_inferred_trading_accuracy', 'cumulative_fills_not_fill_event_times',
                            'failed_snapshot_usage_unavailable', 'cached_data_no_broker_reconciliation']}


def report(user_id, task_id, days=14):
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= 90:
        raise ValueError('Review days must be an integer between 1 and 90')
    store.ensure_schema()
    now = datetime.now(timezone.utc)
    since = now-timedelta(days=days)
    with store.get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s AND user_id=%s', (task_id, user_id))
            row = cur.fetchone()
            if not row:
                raise ValueError('Task not found')
            cur.execute("""SELECT id,status,preview,result->>'source' AS source,
                result->'usage' AS usage,result->'research_calls' AS calls,result->'event' AS event,
                result ? 'items' AS has_decision,
                (SELECT jsonb_agg(jsonb_build_object('symbol',i->>'symbol','action',i->>'action'))
                 FROM jsonb_array_elements(COALESCE(r.result->'items','[]'::jsonb)) i) AS items
                FROM qd_agent_automation_runs r WHERE task_id=%s AND created_at >= %s AND created_at <= %s
                ORDER BY id DESC LIMIT %s""", (task_id, since, now, RUN_LIMIT+1))
            runs = [dict(r) for r in cur.fetchall()]
            cur.execute('''SELECT sampled_at,report FROM qd_agent_automation_samples
                WHERE task_id=%s AND sampled_at >= %s AND sampled_at <= %s ORDER BY sampled_at DESC LIMIT %s''',
                (task_id, since, now, SAMPLE_LIMIT+1))
            samples = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT id,status,filled_qty,avg_fill_price,updated_at,
                order_spec->>'symbol' AS symbol,order_spec->>'side' AS side
                FROM qd_agent_trade_intents WHERE user_id=%s AND agent_token_id=%s
                AND updated_at >= %s AND updated_at <= %s ORDER BY id DESC LIMIT %s""",
                (user_id, row['token_id'], since, now, ORDER_LIMIT+1))
            orders = [dict(r) for r in cur.fetchall()]
            partial = {'runs': len(runs)>RUN_LIMIT, 'samples': len(samples)>SAMPLE_LIMIT, 'orders': len(orders)>ORDER_LIMIT}
            return assemble(dict(row), runs[:RUN_LIMIT], samples[:SAMPLE_LIMIT], orders[:ORDER_LIMIT],
                            since.timestamp(), now.timestamp(), partial)
        finally:
            cur.close()

"""Owner-scoped diagnostics; external probes are explicit and read-only."""
from __future__ import annotations

import time
from app.services.automation.review import timestamp

from app.services.automation import store
from app.services.automation.market import account_snapshot
from app.services.automation.performance import positive
from app.services.agent_trade_intents import get_policy
from app.services.futu_agent_execution import _load_client
from app.services.futu_trading.operator_gate import hard_switch_enabled, state_for_user
from app.services.llm import LLMService
from app.services.llm_selection import validate_selection
from app.services.research_workflow import market_clock


def model_state(config):
    try:
        selection = validate_selection(config.get('llm_selection'))
        service = LLMService(selection=selection)
        provider = service.provider.value
        model = selection.get('model') or service.get_default_model()
        ready = provider in {'openai', 'deepseek', 'volcengine'} and bool(model) and service.is_configured()
        return {'configured': bool(ready), 'provider': provider, 'model': model,
                'code': 'model_configured' if ready else 'model_unconfigured'}
    except Exception:
        return {'configured': False, 'code': 'model_unavailable'}


def scheduler_state(now):
    rows = store.query("""SELECT metadata_json,EXTRACT(EPOCH FROM (NOW()-heartbeat_at)) AS age
        FROM qd_worker_heartbeats WHERE role='scheduler' AND status='running'
        AND metadata_json->>'leader'='true' ORDER BY heartbeat_at DESC LIMIT 1""")
    if not rows:
        return {'status': 'unknown', 'code': 'scheduler_unknown'}
    row = rows[0]
    component = (row['metadata_json'] or {}).get('agent_automation') or {}
    age = float(row['age'])
    stamp = now-age
    last = component.get('last_tick_at') or component.get('started_at') or 0
    if not 0 <= age <= 45 or not 0 <= now-float(last) <= 15:
        status = 'stalled' if component else 'unknown'
    else:
        status = component.get('status', 'unknown')
    return {'status': status, 'code': 'scheduler_' + status, 'heartbeat_at': stamp,
            'last_tick_at': component.get('last_tick_at'), 'last_success_at': component.get('last_success_at')}


def read(row):
    now = time.time()
    config, state = row['config'], row.get('state') or {}
    checks = []
    def add(key, status, code, **details):
        checks.append({'key': key, 'status': status, 'code': code, **details})
    model = model_state(config)
    add('model', 'pass' if model['configured'] else 'blocked', model['code'])
    actor = store.query('''SELECT status,paper_only,scopes,markets,instruments,expires_at,max_order_notional,max_daily_notional
        FROM qd_agent_tokens WHERE id=%s AND user_id=%s''', (row['token_id'], row['user_id']), one=True) or {}
    owned = store.owned_quantities(row)
    symbols = sorted(set(config['symbols']) | {s for s, qty in owned.items() if qty > 0})
    expiration = actor.get('expires_at')
    actor_ready = (actor.get('status') == 'internal' and actor.get('paper_only') is True and
                   positive(actor.get('max_order_notional')) and positive(actor.get('max_daily_notional')) and
                   'T' in str(actor.get('scopes', '')).split(',') and config['market'].upper() in str(actor.get('markets', '')).upper().split(',') and
                   set(symbols) <= set(str(actor.get('instruments', '')).split(',')) and
                   (expiration is None or timestamp(expiration) > now))
    add('actor', 'pass' if actor_ready else 'blocked', 'actor_ready' if actor_ready else 'actor_invalid')
    policy = None
    account_limits = None
    if config['execution_mode'] == 'paper_auto':
        policy = get_policy(row['user_id'], 'futu', f"credential:{config['credential_id']}")
        active = policy['mode'] == 'PAPER_AUTO' and policy.get('enabled_until') is not None and timestamp(policy['enabled_until']) > now
        add('policy', 'pass' if active else 'blocked', 'policy_ready' if active else 'policy_required',
            expires_at=timestamp(policy['enabled_until']) if policy.get('enabled_until') else None)
        missing = sorted(set(s.upper() for s in symbols) - set(policy['allowed_symbols']))
        allowed = config['market'].upper() in policy['allowed_markets'] and not missing
        add('allowlist', 'pass' if allowed else 'blocked', 'allowlist_ready' if allowed else 'allowlist_missing', missing_symbols=missing)
        armed = hard_switch_enabled() and any(s['enabled'] and s['state'] == 'armed' and
            int(s['credential_id']) == config['credential_id'] for s in state_for_user(row['user_id']))
        add('operator', 'pass' if armed else 'blocked', 'operator_ready' if armed else 'operator_required')
        daily = store.query("""SELECT COALESCE(SUM(notional),0) AS used,COUNT(*) AS orders
            FROM qd_agent_trade_intents WHERE user_id=%s AND broker='futu' AND account_ref=%s
            AND created_at >= date_trunc('day',NOW()) AND status IN
            ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED','FILLED','CANCELLED','FAILED')""",
            (row['user_id'], f"credential:{config['credential_id']}"), one=True)
        account_limits = {'day_basis': 'database_day', 'used_notional': float(daily['used']), 'order_count': daily['orders'],
            'remaining_notional': max(0, float(policy['max_daily_notional'])-float(daily['used'])),
            'remaining_orders': max(0, int(policy['max_orders_per_day'])-int(daily['orders'])),
            'effective_order_limit': min(config['max_order_notional'], float(actor.get('max_order_notional') or 0), float(policy['max_order_notional']))}
        available = account_limits['remaining_notional'] > 0 and account_limits['remaining_orders'] > 0
        add('trading_limits', 'pass' if available else 'wait', 'trading_limits_available' if available else 'trading_limits_exhausted')
    configuration_ready = all(c['status'] != 'blocked' for c in checks)
    quota = store.decision_budget(row)
    add('decision_quota', 'pass' if quota['remaining'] else 'wait', 'quota_available' if quota['remaining'] else 'quota_exhausted')
    scheduler = scheduler_state(now)
    add('scheduler', 'pass' if scheduler['status'] == 'running' else 'unknown' if scheduler['status'] == 'unknown' else 'wait', scheduler['code'])
    probe = dict(state.get('connection_check') or {})
    fresh = probe.get('revision') == row['revision'] and 0 <= now-float(probe.get('checked_at', 0)) <= 90
    add('connection', 'pass' if fresh and probe.get('account_ok') else 'unknown',
        'connection_ready' if fresh and probe.get('account_ok') else 'connection_check_required')
    if fresh:
        add('quotes', probe.get('quote_status', 'unknown'), probe.get('quote_code', 'quotes_unavailable'))
    risk = state.get('risk') or {}
    if risk.get('halted') or risk.get('stopped_symbols'):
        add('risk', 'wait', 'risk_latched')
    elif config['execution_mode'] == 'paper_auto' and config.get('risk', {}).get('enabled'):
        healthy = risk.get('healthy') and 0 <= now-float(risk.get('checked_at', 0)) <= 90
        add('risk', 'pass' if healthy else 'wait', 'risk_healthy' if healthy else 'risk_waiting')
    return {'checked_at': now, 'task_revision': row['revision'], 'configuration_ready': configuration_ready,
            'model': model, 'checks': checks, 'decision_budget': quota, 'account_limits': account_limits,
            'scheduler': scheduler, 'connection_check': probe if fresh else None}


def require_configuration(row):
    result = read(row)
    if not result['configuration_ready']:
        failures = [c['code'] for c in result['checks'] if c['status'] == 'blocked']
        raise ValueError('Task configuration is not ready: ' + ', '.join(failures))
    return result


def probe(row):
    """Never read credentials into output, arm accounts or invoke model/order APIs."""
    started = time.time()
    result = {'revision': row['revision'], 'checked_at': started, 'account_ok': False,
              'quote_status': 'unknown', 'quote_code': 'quotes_unavailable'}
    client = None
    try:
        account = account_snapshot(row['user_id'], row['config'])
        if not 0 <= time.time()-float(account['as_of']) <= 30:
            raise ValueError('Stale account')
        result.update(account_ok=True, account_as_of=account['as_of'], open_order_count=len(account['open_orders']))
        clock = market_clock(row['config']['market'])
        if not clock.get('is_open'):
            result.update(quote_status='wait', quote_code='market_closed')
        else:
            client = _load_client(row['user_id'], f"credential:{row['config']['credential_id']}")
            if not client.connect():
                raise ValueError('Quote connection unavailable')
            quotes = []
            for code in row['config']['symbols']:
                if time.time()-started > 30:
                    raise TimeoutError('Probe expired')
                quote = client.get_simulate_execution_quote(code)
                quotes.append({'symbol': code, 'eligible': bool(quote.get('simulate_execution_eligible') and positive(quote.get('price'))),
                               'as_of': quote.get('as_of'), 'market_status': quote.get('market_status')})
            result['quotes'] = quotes
            ready = bool(quotes) and all(q['eligible'] for q in quotes)
            result.update(quote_status='pass' if ready else 'wait', quote_code='quotes_ready' if ready else 'quotes_unavailable')
        if time.time()-started > 30:
            raise TimeoutError('Probe expired')
    except Exception:
        result.update(quote_status='unknown', quote_code='probe_failed')
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                result.update(quote_status='unknown', quote_code='probe_failed')
    # Keep start time: slow observations cannot appear freshly authorized.
    saved = store.query("""UPDATE qd_agent_automations SET state=jsonb_set(state,'{connection_check}',%s::jsonb)
        WHERE id=%s AND user_id=%s AND revision=%s RETURNING id""",
        (store.dumps(result), row['id'], row['user_id'], row['revision']), one=True)
    if not saved:
        raise ValueError('Task changed during the connection check; retry')
    return result

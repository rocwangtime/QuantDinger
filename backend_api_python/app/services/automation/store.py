from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from pathlib import Path

from app.utils.db import get_db_connection

_ready = False
_lock = threading.Lock()


def dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str, allow_nan=False)


def ensure_schema():
    global _ready
    if _ready:
        return
    with _lock:
        if _ready:
            return
        from app.utils.agent_auth import ensure_agent_gateway_schema
        ensure_agent_gateway_schema()
        sql = (Path(__file__).resolve().parents[3] / 'migrations/20261004_agent_automation.sql').read_text()
        sql += (Path(__file__).resolve().parents[3] / 'migrations/20261006_agent_performance.sql').read_text()
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(sql)
            db.commit()
            cur.close()
        _ready = True


def query(sql, args=(), *, one=False):
    ensure_schema()
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(sql, args)
            data = cur.fetchone() if one else cur.fetchall()
            db.commit()
            return dict(data) if one and data else (None if one else [dict(row) for row in data])
        finally:
            cur.close()


def task(user_id, task_id):
    result = query('SELECT * FROM qd_agent_automations WHERE user_id=%s AND id=%s', (user_id, task_id), one=True)
    if not result:
        raise ValueError('Task not found')
    return result


def create(user_id, name, config):
    from app.services.agent_trade_intents import account_scope
    from app.utils.agent_auth import generate_token
    account_scope(user_id, {'broker': 'futu', 'credential_id': config['credential_id'], 'market': config['market']})
    ensure_schema()
    # No bearer credential is retained or returned. Internal actors cannot log in
    # through Agent Gateway (only status=active tokens authenticate there).
    _, prefix, digest = generate_token()
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('''INSERT INTO qd_agent_tokens
                (user_id,name,token_prefix,token_hash,scopes,markets,instruments,paper_only,
                 max_order_notional,max_daily_notional,status)
                VALUES (%s,%s,%s,%s,'T',%s,%s,TRUE,%s,%s,'internal') RETURNING id''',
                (user_id, 'automation-task', prefix, digest, config['market'], ','.join(config['symbols']),
                 config['max_order_notional'], config['max_daily_notional']))
            token_id = cur.fetchone()['id']
            cur.execute('''INSERT INTO qd_agent_automations(user_id,name,config,token_id)
                VALUES (%s,%s,%s::jsonb,%s) RETURNING *''', (user_id, name[:120], dumps(config), token_id))
            result = dict(cur.fetchone())
            db.commit()
            return result
        finally:
            cur.close()


def set_active(user_id, task_id, active, baseline=None, baseline_prices=None, external_quantities=None):
    ensure_schema()
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            # Serialize activation for this user's account; one executing task
            # owns the account at a time. Read-only tasks may coexist.
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (user_id,))
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s AND user_id=%s FOR UPDATE', (task_id,user_id))
            row = dict(cur.fetchone() or {})
            if not row:
                raise ValueError('Task not found')
            if row['active'] == active:
                db.commit()
                return row
            config = row['config']
            state = row['state'] or {}
            if active and config['execution_mode'] == 'paper_auto':
                cur.execute('''SELECT id FROM qd_agent_automations WHERE user_id=%s AND id<>%s
                    AND active=TRUE AND config->>'execution_mode'='paper_auto'
                    AND config->>'credential_id'=%s''', (user_id,task_id,str(config['credential_id'])))
                if cur.fetchone():
                    raise ValueError('Another active trading task already manages this account')
                cur.execute('''SELECT * FROM qd_agent_automations WHERE user_id=%s AND id<>%s
                    AND config->>'credential_id'=%s''',
                    (user_id,task_id,str(config['credential_id'])))
                for other in cur.fetchall():
                    owned = dict((other['state'] or {}).get('baseline') or {})
                    cur.execute('SELECT status,order_spec,filled_qty FROM qd_agent_trade_intents WHERE agent_token_id=%s', (other['token_id'],))
                    for receipt in cur.fetchall():
                        if receipt['status'] in {'EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED'}:
                            raise ValueError('Another task has outstanding orders on this account')
                        order = receipt['order_spec']
                        owned[order['symbol']] = float(owned.get(order['symbol'],0)) + float(receipt['filled_qty'] or 0)*(1 if order['side']=='buy' else -1)
                    if any(float(qty)>0 for qty in owned.values()):
                        raise ValueError('Another task still owns positions; continue managing them in that task')
                if 'baseline' not in state:
                    state['baseline'] = baseline or {}
                    state['performance'] = {'capital': config['budget'], 'baseline_prices': baseline_prices or {}}
                    if external_quantities is not None:
                        state['performance']['external_quantities'] = external_quantities
            cur.execute('''UPDATE qd_agent_automations SET active=%s,revision=revision+1,
                state=%s::jsonb,updated_at=NOW() WHERE id=%s RETURNING *''', (active,dumps(state),task_id))
            result = dict(cur.fetchone())
            cur.execute('''UPDATE qd_agent_automation_runs SET cancel_requested=TRUE,
                status=CASE WHEN status IN ('queued','planned') THEN 'cancelled' ELSE status END
                WHERE task_id=%s AND status IN ('queued','researching','planned','executing')''', (task_id,))
            db.commit()
            return result
        finally:
            cur.close()


def edit(user_id, task_id, name, config):
    from app.services.agent_trade_intents import account_scope
    account_scope(user_id, {'broker':'futu','credential_id':config['credential_id'],'market':config['market']})
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (user_id,))
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s AND user_id=%s FOR UPDATE', (task_id,user_id))
            row = cur.fetchone()
            if not row or row['active']:
                raise ValueError('请先暂停任务再修改配置')
            cur.execute('SELECT status,order_spec,filled_qty FROM qd_agent_trade_intents WHERE agent_token_id=%s ORDER BY id', (row['token_id'],))
            orders = cur.fetchall()
            if any(r['status'] in {'EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED'} for r in orders):
                raise ValueError('任务仍有未完成订单，请先处理并对账')
            if orders or (row['state'] or {}).get('baseline'):
                if any(config[k]!=row['config'][k] for k in ('market','credential_id')):
                    raise ValueError('已有交易历史的任务不能更换账户或市场')
            if (row['state'] or {}).get('performance') and config['budget'] != row['config']['budget']:
                raise ValueError('收益跟踪已开始，不能修改任务本金；请创建新任务')
            owned = dict((row['state'] or {}).get('baseline') or {})
            for receipt in orders:
                order = receipt['order_spec']
                code = order['symbol']
                owned[code] = float(owned.get(code, 0)) + float(receipt['filled_qty'] or 0) * (1 if order['side'] == 'buy' else -1)
            instruments = list(dict.fromkeys(config['symbols'] + [code for code, qty in owned.items() if qty > 0]))
            cur.execute('''UPDATE qd_agent_automations SET name=%s,config=%s::jsonb,revision=revision+1,
                state=state-'trigger',updated_at=NOW() WHERE id=%s RETURNING *''', (name[:120],dumps(config),task_id))
            result = dict(cur.fetchone())
            cur.execute('''UPDATE qd_agent_tokens SET markets=%s,instruments=%s,max_order_notional=%s,
                max_daily_notional=%s WHERE id=%s''', (config['market'],','.join(instruments),
                config['max_order_notional'],config['max_daily_notional'],row['token_id']))
            cur.execute("UPDATE qd_agent_automation_runs SET cancel_requested=TRUE WHERE task_id=%s AND status IN ('queued','researching','planned','executing')", (task_id,))
            db.commit()
            return result
        finally:
            cur.close()


def decision_budget(row, now=None, cur=None):
    """Created runs reserve daily slots, including failed/cancelled previews."""
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo
    zone = ZoneInfo('America/New_York' if row['config']['market'] == 'USStock' else 'Asia/Hong_Kong')
    now = now or datetime.now(timezone.utc)
    local = now.astimezone(zone)
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    sql = """SELECT COUNT(*) AS used FROM qd_agent_automation_runs WHERE task_id=%s
        AND event_key NOT LIKE 'protection:%%' AND created_at >= %s AND created_at < %s"""
    args = (row['id'], start, end)
    if cur is None:
        used = query(sql, args, one=True)['used']
    else:
        cur.execute(sql, args)
        used = cur.fetchone()['used']
    limit = (row['config'].get('research') or {}).get('max_decisions_per_day', 8)
    return {'day': local.date().isoformat(), 'timezone': str(zone), 'used': used,
            'limit': limit, 'remaining': max(0, limit-used), 'resets_at': end.timestamp()}


def admit_run(cur, row, event_key, expires_at, execute_at=None, preview=False, evidence=None):
    """Caller holds the task row lock. Protection uses its separate model-free path."""
    if not preview and not row['active']:
        return None
    if not decision_budget(row, cur=cur)['remaining']:
        return None
    cur.execute("""SELECT id FROM qd_agent_automation_runs WHERE task_id=%s
        AND status IN ('queued','researching','planned','executing') LIMIT 1""", (row['id'],))
    if cur.fetchone():
        return None
    cur.execute('''INSERT INTO qd_agent_automation_runs
        (task_id,user_id,revision,event_key,expires_at,execute_at,preview,evidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING *''',
        (row['id'],row['user_id'],row['revision'],event_key,expires_at,execute_at,preview,dumps(evidence or {})))
    created = cur.fetchone()
    return dict(created) if created else None


def create_run(row, event_key, expires_at, execute_at=None, preview=False, state=None):
    ensure_schema()
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s FOR UPDATE', (row['id'],))
            current = cur.fetchone()
            if not current or current['revision'] != row['revision']:
                return None
            created = admit_run(cur, current, event_key, expires_at, execute_at, preview)
            if created and state is not None:
                merged = {**(current['state'] or {}), 'trigger': state['trigger']}
                cur.execute('UPDATE qd_agent_automations SET state=%s::jsonb WHERE id=%s', (dumps(merged),row['id']))
            db.commit()
            return created
        finally:
            cur.close()


def update_run(run_id, **fields):
    allowed = {'status','phase','draft','evidence','result','finished_at'}
    if not fields or set(fields)-allowed:
        raise ValueError('Invalid run update')
    clauses, values = [], []
    for key, value in fields.items():
        clauses.append(f'{key}=%s' + ('::jsonb' if key in {'evidence','result'} else ''))
        values.append(dumps(value) if key in {'evidence','result'} else value)
    return query(f"UPDATE qd_agent_automation_runs SET {','.join(clauses)} WHERE id=%s RETURNING id", (*values,run_id), one=True)


def cancelled(run):
    row = query('''SELECT r.cancel_requested,r.status,t.active,t.revision FROM qd_agent_automation_runs r
        JOIN qd_agent_automations t ON t.id=r.task_id WHERE r.id=%s''', (run['id'],), one=True)
    return not row or row['status'] not in {'queued','researching','planned','executing'} or row['cancel_requested'] or row['revision'] != run['revision'] or (not run['preview'] and not row['active'])


@contextmanager
def submission_guard(row, run, order=None):
    """Serialize pause/cancel against the final broker call, not just its preflight."""
    from datetime import datetime, timezone
    from app.services.automation.worker import STOP
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (row['user_id'],))
            cur.execute('''SELECT t.active,t.revision,t.config,t.state,r.cancel_requested,r.status,r.expires_at
                FROM qd_agent_automations t JOIN qd_agent_automation_runs r ON r.task_id=t.id
                WHERE t.id=%s AND r.id=%s FOR UPDATE OF t,r''', (row['id'],run['id']))
            live = cur.fetchone()
            if STOP.is_set() or not live or not live['active'] or live['revision'] != run['revision'] or live['cancel_requested'] or live['status'] != 'executing' or live['expires_at'] <= datetime.now(timezone.utc):
                raise ValueError('Task paused, cancelled or expired before broker submission')
            from app.services.automation.performance import purchase_allowed
            if order and order['side'] == 'buy' and not purchase_allowed(live, order['symbol']):
                raise ValueError('独立保护已暂停买入，或账户行情监测已过期')
            yield
            db.commit()
        finally:
            cur.close()


def cancel_run(user_id, run_id):
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (user_id,))
            cur.execute('''UPDATE qd_agent_automation_runs SET cancel_requested=TRUE,
                status=CASE WHEN status IN ('queued','planned') THEN 'cancelled' ELSE status END
                WHERE user_id=%s AND id=%s RETURNING id''', (user_id,run_id))
            result = cur.fetchone()
            db.commit()
            return result
        finally:
            cur.close()


def receipts(row, run_id=None):
    suffix = ' AND order_spec->>\'strategy_version\'=%s' if run_id else ''
    args = (row['token_id'], f"automation:{row['id']}:{run_id}") if run_id else (row['token_id'],)
    return query('''SELECT id,status,order_spec,filled_qty,avg_fill_price,broker_order_id,updated_at
        FROM qd_agent_trade_intents WHERE agent_token_id=%s''' + suffix + ' ORDER BY id', args)


def owned_quantities(row):
    owned = dict((row.get('state') or {}).get('baseline') or {})
    for receipt in receipts(row):
        order = receipt['order_spec']
        code = order['symbol']
        owned[code] = float(owned.get(code,0)) + float(receipt.get('filled_qty') or 0) * (1 if order['side']=='buy' else -1)
    return {code:max(0,qty) for code,qty in owned.items()}


def reset_risk(user_id, task_id):
    """An explicit paused-task reset starts new risk anchors, preserving returns."""
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute('SELECT pg_advisory_xact_lock(824112,%s)', (user_id,))
            cur.execute('SELECT * FROM qd_agent_automations WHERE user_id=%s AND id=%s FOR UPDATE', (user_id, task_id))
            row = cur.fetchone()
            if not row or row['active']:
                raise ValueError('请先暂停任务再重置保护状态')
            state = dict(row['state'] or {})
            tracking = dict(state.get('performance') or {})
            equity = (tracking.get('latest_valid') or tracking.get('latest') or {}).get('equity')
            if equity is None:
                raise ValueError('缺少有效净值，请先恢复账户与行情监测')
            tracking.update(high_water=equity, day_equity=equity)
            state.update(performance=tracking, risk={'halted': False, 'stopped_symbols': [], 'healthy': False})
            cur.execute('UPDATE qd_agent_automations SET state=%s::jsonb,revision=revision+1,updated_at=NOW() WHERE id=%s RETURNING *',
                        (dumps(state), task_id))
            result = dict(cur.fetchone())
            db.commit()
            return result
        finally:
            cur.close()

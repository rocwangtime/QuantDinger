"""Human-session management of persistent paper Agent tasks."""
from datetime import datetime, timedelta, timezone
from functools import wraps
import json
import math
import re
import time
import uuid

from flask import Response, g, jsonify, request, stream_with_context
from app.openapi.blueprint import HumanBlueprint
from app.openapi.schemas.automations import AutomationDashboardEnvelopeSchema, AutomationReadinessEnvelopeSchema, AutomationReviewEnvelopeSchema
from app.utils.auth import login_required
from app.services.automation import store
from app.services.automation.domain import normalize_config, session_schedule
from app.services.automation.market import account_snapshot
from app.services.agent_trade_intents import IntentError, get_policy

blp = HumanBlueprint('agent_automations',__name__)


def envelope(fn):
    @wraps(fn)
    def wrapped(*args,**kwargs):
        try:
            return jsonify({'code':1,'msg':'success','data':fn(*args,**kwargs)})
        except (ValueError,TypeError,KeyError,IntentError) as exc:
            return jsonify({'code':0,'msg':str(exc)[:250]}),400
        except Exception:
            return jsonify({'code':0,'msg':'任务服务暂不可用，请检查服务连接'}),503
    return wrapped


def public_task(row):
    result = {k:row[k] for k in ('id','name','active','revision','config','created_at','updated_at')}
    result['managed_quantities'] = store.owned_quantities(row)
    result['monitor_status'] = (row['state'] or {}).get('monitor_status','等待调度器启动') if row['active'] else '已暂停'
    result['latest_run'] = store.query('SELECT id,status,phase,created_at FROM qd_agent_automation_runs WHERE task_id=%s ORDER BY id DESC LIMIT 1',(row['id'],),one=True)
    result['decision_budget'] = store.decision_budget(row)
    result['event_review'] = (row['state'] or {}).get('event_review') or {}
    result['risk'] = (row['state'] or {}).get('risk') or {}
    if row['config']['kind']=='daily_portfolio':
        result['schedule'] = session_schedule(row['config'])
    return result


def public_run(run, *, evidence=False):
    result = dict(run)
    result.pop('user_id',None)
    if not evidence:
        result.pop('evidence',None)
        result['result'] = dict(result.get('result') or {})
        if 'tool_trace' in result['result']:
            result['result']['tool_request_count'] = len(result['result'].pop('tool_trace'))
    # Show the streamed summary/reasons, never raw machine JSON.
    text = run.get('draft') or ''
    parts = re.findall(r'"(?:summary|reason)"\s*:\s*"((?:\\.|[^"\\])*)',text)
    visible = []
    for part in parts:
        try:
            visible.append(json.loads('"'+part+'"'))
        except ValueError:
            visible.append(part)
    result['draft'] = '\n'.join(visible)
    return result


@blp.route('',methods=['GET'])
@login_required
@envelope
def list_tasks():
    return [public_task(row) for row in store.query('SELECT * FROM qd_agent_automations WHERE user_id=%s ORDER BY id DESC',(g.user_id,))]


@blp.route('',methods=['POST'])
@login_required
@envelope
def create_task():
    data = request.get_json() or {}
    config = normalize_config(data.get('config'))
    name = str(data.get('name') or '').strip()
    if not name:
        raise ValueError('请输入任务名称')
    return public_task(store.create(g.user_id,name,config))


@blp.route('/<int:task_id>',methods=['PUT'])
@login_required
@envelope
def edit_task(task_id):
    data = request.get_json() or {}
    name = str(data.get('name') or '').strip()
    if not name:
        raise ValueError('请输入任务名称')
    return public_task(store.edit(g.user_id,task_id,name,normalize_config(data.get('config'))))


@blp.route('/<int:task_id>/state',methods=['POST'])
@login_required
@envelope
def set_task_state(task_id):
    data = request.get_json() or {}
    if not isinstance(data.get('active'),bool):
        raise ValueError('active must be a boolean')
    row = store.task(g.user_id,task_id)
    config = row['config']
    baseline = None
    baseline_prices = None
    external_quantities = None
    if data['active']:
        from app.services.automation.readiness import require_configuration
        require_configuration(row)
        snapshot = account_snapshot(g.user_id,config)
        if config['execution_mode']=='paper_auto':
            from app.services.futu_trading.operator_gate import hard_switch_enabled, state_for_user
            policy = get_policy(g.user_id,'futu',f"credential:{config['credential_id']}")
            if not hard_switch_enabled() or policy['mode']!='PAPER_AUTO':
                raise ValueError('请先在账户页面完成此模拟账户的自动交易授权；任务不会替你修改账户授权')
            if not any(s['enabled'] and int(s['credential_id'])==config['credential_id'] for s in state_for_user(g.user_id)):
                raise ValueError('请在账户页面亲自启用此模拟账户的自动交易，再启用交易任务')
            if snapshot['open_orders']:
                raise ValueError('账户存在挂单，请先处理后再启用交易任务')
            if config['manage_existing']:
                baseline = {p['symbol']:p['quantity'] for p in snapshot['positions'] if p['side']=='long' and p['symbol'] in config['symbols']}
                baseline_prices = {p['symbol']:float(p['marketValue']) / float(p['quantity'])
                                   for p in snapshot['positions'] if p['symbol'] in baseline and float(p['quantity']) > 0}
                if any(not math.isfinite(p) or p <= 0 for p in baseline_prices.values()) or sum(baseline[s] * baseline_prices[s] for s in baseline) > config['budget']:
                    raise ValueError('接管持仓缺少有效市值，或市值超过任务预算')
            totals = {}
            for p in snapshot['positions']:
                if p['side'] == 'long':
                    totals[p['symbol']] = totals.get(p['symbol'], 0) + float(p['quantity'])
            external_quantities = {s: max(0, qty - (baseline or {}).get(s, 0)) for s, qty in totals.items()}
    return public_task(store.set_active(g.user_id,task_id,data['active'],baseline,baseline_prices,external_quantities))


@blp.route('/<int:task_id>/preview',methods=['POST'])
@login_required
@envelope
def preview_task(task_id):
    row = store.task(g.user_id,task_id)
    # Explicit preview ignores the trigger/time window, but can never execute.
    expiry = datetime.now(timezone.utc)+timedelta(seconds=180)
    run = store.create_run(row,'preview:'+str(uuid.uuid4()),expiry,preview=True)
    if not run:
        raise ValueError('此任务已有待完成的分析，或每日决策额度已用完')
    return public_run(run)


@blp.route('/<int:task_id>/runs',methods=['GET'])
@login_required
@envelope
def task_runs(task_id):
    row = store.task(g.user_id,task_id)
    runs = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s AND user_id=%s ORDER BY id DESC LIMIT 30',(task_id,g.user_id))
    return [{**public_run(run),'orders':store.receipts(row,run['id'])} for run in runs]


@blp.route('/<int:task_id>/snapshot',methods=['GET'])
@login_required
@envelope
def snapshot(task_id):
    row = store.task(g.user_id,task_id)
    return account_snapshot(g.user_id,row['config'])


@blp.route('/<int:task_id>/dashboard', methods=['GET'])
@blp.doc(summary='Read cached forward performance, independent protection and decision ledger')
@blp.response(200, AutomationDashboardEnvelopeSchema)
@login_required
@envelope
def task_dashboard(task_id):
    from app.services.automation.monitor import dashboard
    row = store.task(g.user_id, task_id)
    result = dashboard(row)
    result['task'] = public_task(row)
    result['runs'] = [public_run(run) for run in result['runs']]
    return result


@blp.route('/<int:task_id>/readiness', methods=['GET'])
@blp.doc(summary='Read cached task prerequisites and scheduler diagnostics without remote calls')
@blp.response(200, AutomationReadinessEnvelopeSchema)
@login_required
@envelope
def task_readiness(task_id):
    from app.services.automation.readiness import read
    return read(store.task(g.user_id, task_id))


@blp.route('/<int:task_id>/check-connection', methods=['POST'])
@blp.doc(summary='Explicitly probe SIMULATE account and session quotes without submitting orders or calling a model')
@login_required
@envelope
def task_connection_check(task_id):
    from app.services.automation.readiness import probe
    return probe(store.task(g.user_id, task_id))


@blp.route('/<int:task_id>/review', methods=['GET'])
@blp.doc(summary='Export a bounded forward paper review from cached database records')
@blp.response(200, AutomationReviewEnvelopeSchema)
@blp.doc(parameters=[{'name': 'days', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 90, 'default': 14}, 'description': 'Rolling UTC time window; observations grouped by exchange-local date'}])
@login_required
@envelope
def task_review(task_id):
    from app.services.automation.review import report
    return report(g.user_id, task_id, int(request.args.get('days', '14')))


@blp.route('/<int:task_id>/risk/reset', methods=['POST'])
@blp.doc(summary='Explicitly reset a paused task protection latch without erasing performance')
@login_required
@envelope
def reset_task_risk(task_id):
    return public_task(store.reset_risk(g.user_id, task_id))


@blp.route('/runs/<int:run_id>/cancel',methods=['POST'])
@login_required
@envelope
def cancel_run(run_id):
    result = store.cancel_run(g.user_id,run_id)
    if not result:
        raise ValueError('Run not found')
    return result


@blp.route('/runs/<int:run_id>',methods=['GET'])
@login_required
@envelope
def run_detail(run_id):
    run = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s AND user_id=%s',(run_id,g.user_id),one=True)
    if not run:
        raise ValueError('Run not found')
    return public_run(run,evidence=True)


@blp.route('/runs/<int:run_id>/stream',methods=['GET'])
@blp.response(200, description='Stream persisted run progress until completion', content_type='text/event-stream')
@login_required
def run_stream(run_id):
    user_id = g.user_id
    found = store.query('SELECT id FROM qd_agent_automation_runs WHERE id=%s AND user_id=%s',(run_id,user_id),one=True)
    if not found:
        return jsonify({'code':0,'msg':'Run not found'}),404

    @stream_with_context
    def events():
        previous = ''
        for _ in range(380):
            run = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s AND user_id=%s',(run_id,user_id),one=True)
            if not run:
                return
            payload = store.dumps(public_run(run))
            if payload!=previous:
                yield f'event: progress\ndata: {payload}\n\n'
                previous = payload
            else:
                yield ': heartbeat\n\n'
            if run['status'] not in {'queued','researching','executing'}:
                yield 'event: done\ndata: {}\n\n'
                return
            time.sleep(.5)
    return Response(events(),mimetype='text/event-stream',headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no'})

"""Human-session management of persistent paper Agent tasks."""
from datetime import datetime, timedelta, timezone
from functools import wraps
import json
import re
import time
import uuid

from flask import Response, g, jsonify, request, stream_with_context
from app.openapi.blueprint import HumanBlueprint
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
    if row['config']['kind']=='daily_portfolio':
        result['schedule'] = session_schedule(row['config'])
    return result


def public_run(run, *, evidence=False):
    result = dict(run)
    result.pop('user_id',None)
    if not evidence:
        result.pop('evidence',None)
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
    if data['active']:
        snapshot = account_snapshot(g.user_id,config)
        if config['execution_mode']=='paper_auto':
            from app.services.futu_trading.operator_gate import hard_switch_enabled
            policy = get_policy(g.user_id,'futu',f"credential:{config['credential_id']}")
            if not hard_switch_enabled() or policy['mode']!='PAPER_AUTO':
                raise ValueError('请先在账户页面完成此模拟账户的自动交易授权；任务不会替你修改账户授权')
            if snapshot['open_orders']:
                raise ValueError('账户存在挂单，请先处理后再启用交易任务')
            if config['manage_existing']:
                baseline = {p['symbol']:p['quantity'] for p in snapshot['positions'] if p['side']=='long' and p['symbol'] in config['symbols']}
    return public_task(store.set_active(g.user_id,task_id,data['active'],baseline))


@blp.route('/<int:task_id>/preview',methods=['POST'])
@login_required
@envelope
def preview_task(task_id):
    row = store.task(g.user_id,task_id)
    # Explicit preview ignores the trigger/time window, but can never execute.
    expiry = datetime.now(timezone.utc)+timedelta(seconds=180)
    run = store.create_run(row,'preview:'+str(uuid.uuid4()),expiry,preview=True)
    if not run:
        raise ValueError('此任务已有正在运行的分析，请先等待或停止')
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

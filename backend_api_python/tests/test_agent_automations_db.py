"""Opt-in isolated PostgreSQL replay. All model/broker interfaces are stubbed.

AUTOMATION_TEST_DATABASE_URL must name an explicitly disposable local database.
Each test uses a fresh schema, removed afterwards; never use a deployment DSN.
"""
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.automation import store, runner, worker
from tests.test_agent_automations import config

pytestmark = pytest.mark.integration


@pytest.fixture
def db(monkeypatch):
    import psycopg2
    from psycopg2.extras import RealDictCursor
    url=os.getenv('AUTOMATION_TEST_DATABASE_URL','')
    if not url:
        pytest.skip('Disposable PostgreSQL not supplied')
    from urllib.parse import urlparse
    parsed=urlparse(url)
    assert parsed.hostname in {'127.0.0.1','localhost'} and parsed.path=='/qd_automation_test'
    schema='automation_test_'+uuid.uuid4().hex
    conn=psycopg2.connect(url)
    conn.autocommit=True
    with conn.cursor() as cur:
        cur.execute('SHOW server_version_num')
        assert 180000 <= int(cur.fetchone()[0]) < 190000, 'Tests require local PostgreSQL 18'
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET search_path TO {schema}')
        cur.execute((Path(__file__).parents[1]/'migrations/init.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261004_agent_automation.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261006_agent_performance.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261012_agent_symbol_config.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261012_agent_scoring.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261015_us_screener.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261015_us_forward_scores.sql').read_text())
        cur.execute("INSERT INTO qd_users(id,username,password_hash) VALUES(900001,'automation-fixture','not-a-login')")

    @contextmanager
    def connection():
        c=psycopg2.connect(url,cursor_factory=RealDictCursor)
        try:
            with c.cursor() as cur:
                cur.execute(f'SET search_path TO {schema}')
            yield c
        finally:
            c.close()

    monkeypatch.setattr(store,'get_db_connection',connection)
    monkeypatch.setattr(store,'_ready',True)
    monkeypatch.setattr('app.services.agent_trade_intents.account_scope',lambda *_:('futu','credential:7'))
    worker.STOP.clear()
    yield
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA {schema} CASCADE')
    conn.close()


def create(**changes):
    return store.create(900001,'replay fixture',config(**changes))


def run_for(row, preview=True):
    return store.create_run(row,'test-event',datetime.now(timezone.utc)+timedelta(seconds=180),preview=preview)


def test_migrate_bootstrap_seeds_inactive_aapl_once(db, monkeypatch):
    from app.commands.migrate import main
    from app.services.automation.symbol_config import US_DEFAULTS
    from app.utils import db as db_module

    # Two existing tasks on one US account must still produce just one seed.
    existing = [create(symbols=['MSFT']), create(symbols=['TSLA'])]
    monkeypatch.setattr(db_module, 'get_db_connection', store.get_db_connection)
    monkeypatch.setenv('SKIP_AUTO_MIGRATE', 'false')
    policies = store.query('SELECT * FROM qd_agent_trading_policies')

    def snapshot():
        tasks = store.query("""SELECT * FROM qd_agent_automations
            WHERE user_id=900001 AND config->>'market'='USStock'
              AND config->>'kind'='price_trigger' AND config->'symbols'='["AAPL"]'::jsonb""")
        assert len(tasks) == 1
        row = tasks[0]
        assert row['active'] is False
        assert row['config']['execution_mode'] == 'plan_only'
        assert row['config']['credential_id'] == 7
        assert row['config']['symbol_params'] == US_DEFAULTS
        saved = store.query('SELECT * FROM qd_agent_symbol_configs WHERE task_id=%s', (row['id'],))
        assert len(saved) == 1 and saved[0]['symbol'] == 'AAPL'
        assert saved[0]['params'] == US_DEFAULTS
        audit = store.query('SELECT * FROM qd_agent_symbol_config_audit WHERE task_id=%s ORDER BY id', (row['id'],))
        assert {a['field']: a['new_value'] for a in audit} == US_DEFAULTS
        assert len(audit) == len(US_DEFAULTS)
        actor = store.query('SELECT * FROM qd_agent_tokens WHERE id=%s', (row['token_id'],), one=True)
        assert actor['status'] == 'internal' and actor['paper_only'] is True
        assert (actor['scopes'], actor['markets'], actor['instruments']) == ('T', 'USStock', 'AAPL')
        assert actor['max_order_notional'] == 3000 and actor['max_daily_notional'] == 6000
        assert store.query('SELECT * FROM qd_agent_trading_policies') == policies
        assert [store.task(900001, r['id']) for r in existing] == existing
        assert len(store.query('SELECT id FROM qd_agent_tokens WHERE user_id=900001')) == 3
        return tasks, saved, audit, actor

    main()
    first = snapshot()
    assert store.query("SELECT name FROM qd_bootstrap_migrations WHERE name='us-agent-symbol-seed-20261014'")
    main()
    assert snapshot() == first
    # Also exercise the SQL guard independently of the bootstrap ledger.
    with store.get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute((Path(__file__).parents[1] / 'migrations/20261014_us_agent_symbol_seed.sql').read_text())
        conn.commit()
    assert snapshot() == first


def test_durable_event_dedup_revision_and_user_isolation(db):
    row=create()
    assert row['active'] is False
    run=run_for(row)
    assert run and run_for(row) is None
    assert store.create_run(row,'another-event',run['expires_at'],preview=True) is None
    with pytest.raises(ValueError):
        store.task(900002,row['id'])
    assert not store.cancelled(run)
    store.cancel_run(900001,run['id'])
    assert store.cancelled(run)
    assert run_for(row) is None  # Same event stays deduplicated after cancellation.
    updated=store.edit(900001,row['id'],'changed',row['config'])
    assert updated['revision']==row['revision']+1
    assert store.create_run(row,'stale-revision',run['expires_at'],preview=True) is None


def test_only_one_trading_owner_and_final_pause_guard(db):
    row=store.set_active(900001,create(execution_mode='paper_auto')['id'],True,{'AAPL':5})
    other=create(execution_mode='paper_auto')
    with pytest.raises(ValueError,match='active trading task'):
        store.set_active(900001,other['id'],True)
    run=run_for(row,preview=False)
    store.update_run(run['id'],status='executing')
    with store.submission_guard(row,run):
        pass
    store.set_active(900001,row['id'],False)
    with pytest.raises(ValueError,match='paused'):
        with store.submission_guard(row,run):
            pytest.fail('Paused task must not submit')
    with pytest.raises(ValueError,match='owns positions'):
        store.set_active(900001,other['id'],True)


def test_complete_preview_streams_persists_and_never_executes(db,monkeypatch):
    row=create(kind='price_trigger',trigger={'type':'price_above','price':100})
    run=run_for(row)
    evidence={'as_of':time.time(),'account':{'as_of':time.time()},'instruments':{'AAPL':{'price':100}}}
    monkeypatch.setattr(runner,'build_evidence',lambda *_a,**_k:evidence)
    closed=[]

    class Model:
        provider=SimpleNamespace(value='openai')
        last_provider='openai'
        last_model='fixture'
        last_usage={'prompt_tokens':5,'completion_tokens':5}
        def __init__(self,**_): pass
        def stream_llm_api_cancellable(self,messages,cancelled,**_):
            try:
                yield '{"summary":"保持观察",'
                assert store.query('SELECT draft FROM qd_agent_automation_runs WHERE id=%s',(run['id'],),one=True)['draft']
                yield '"items":[{"symbol":"AAPL","action":"WAIT","target_weight":0}]}'
            finally:
                closed.append(True)
    monkeypatch.setattr('app.services.llm.LLMService',Model)
    monkeypatch.setattr(runner,'execute',lambda *_:pytest.fail('Preview must never execute'))
    runner.analyze(row,run)
    result=store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s',(run['id'],),one=True)
    assert result['status']=='completed'
    assert result['result']['summary']=='保持观察' and closed
    assert result['result']['usage']['total_tokens']==10
    assert store.receipts(row)==[]


def test_cancel_closes_model_stream_without_decision(db,monkeypatch):
    row=create()
    run=run_for(row)
    closed=[]
    evidence={'as_of':time.time(),'account':{'as_of':time.time()},'instruments':{}}
    monkeypatch.setattr(runner,'build_evidence',lambda *_a,**_k:evidence)
    class Model:
        provider=SimpleNamespace(value='openai')
        def __init__(self,**_): pass
        def stream_llm_api_cancellable(self,messages,cancelled,**_):
            try:
                yield '{"summary":"partial'
                store.cancel_run(900001,run['id'])
                time.sleep(.32)
                assert cancelled()
                return
            finally:
                closed.append(True)
    monkeypatch.setattr('app.services.llm.LLMService',Model)
    runner.analyze(row,run)
    result=store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s',(run['id'],),one=True)
    assert result['status']=='cancelled' and closed
    assert result['result']=={} and store.receipts(row)==[]


def test_expired_fast_context_does_not_call_model(db,monkeypatch):
    row=create(kind='price_trigger',trigger={'type':'price_above','price':100})
    row=store.set_active(900001,row['id'],True)
    run=run_for(row,preview=False)
    evidence={'as_of':time.time()-150,'account':{'as_of':time.time()},'instruments':{'AAPL':{'price':100}}}
    monkeypatch.setattr('app.services.llm.LLMService',lambda **_:pytest.fail('Stale context must not invoke model'))
    runner.analyze(row,run,evidence,{'price':100,'as_of':time.time()})
    result=store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s',(run['id'],),one=True)
    assert result['status']=='failed' and store.receipts(row)==[]


def test_http_auth_ownership_and_sse_response(db,monkeypatch):
    from flask import Flask
    from app.routes.agent_automations import blp
    app=Flask(__name__)
    app.register_blueprint(blp,url_prefix='/api/agent-automations')
    client=app.test_client()
    assert client.get('/api/agent-automations').status_code==401
    monkeypatch.setattr('app.utils.auth.verify_token',lambda _: {'user_id':900001,'_verified_user_role':'user'})
    headers={'Authorization':'Bearer test-fixture-only'}
    response=client.post('/api/agent-automations',headers=headers,json={'name':'HTTP test','config':config()})
    assert response.status_code==200, response.get_json()
    row=response.get_json()['data']
    assert 'token_id' not in row and row['active'] is False
    response=client.post(f"/api/agent-automations/{row['id']}/preview",headers=headers)
    run=response.get_json()['data']
    store.update_run(run['id'],status='completed',draft='{"summary":"visible","items":[]}')
    response=client.get(f"/api/agent-automations/runs/{run['id']}/stream",headers=headers)
    assert response.mimetype=='text/event-stream'
    assert 'event: progress' in response.text and 'event: done' in response.text
    assert 'visible' in response.text
    monkeypatch.setattr('app.utils.auth.verify_token',lambda _: {'user_id':900002,'_verified_user_role':'user'})
    assert client.get(f"/api/agent-automations/runs/{run['id']}/stream",headers=headers).status_code==404


def test_buy_sell_replay_uses_owned_fills_and_does_not_repeat(db,monkeypatch):
    """Gateway is a fake broker here; real gateway guardrails have separate tests."""
    from unittest.mock import MagicMock
    from tests.test_agent_automations import decision
    row=store.set_active(900001,create(execution_mode='paper_auto')['id'],True)
    holdings=[]
    client=MagicMock()
    client.get_lot_size.return_value=1
    client.get_max_cash_buy.return_value=None
    monkeypatch.setattr('app.services.automation.symbol_trigger.session_allowed', lambda *_: True)
    client.get_simulate_execution_quote.return_value={'simulate_execution_eligible':True,'price':100}
    monkeypatch.setattr('app.services.futu_agent_execution._load_client',lambda *_:client)
    monkeypatch.setattr(runner,'account_snapshot',lambda *_:{'positions':holdings,'open_orders':[],
        'funds':{'cash':1000,'power':1000}})
    submitted=[]
    def submit(user,token,order,key):
        return store.query('''INSERT INTO qd_agent_trade_intents
            (user_id,agent_token_id,broker,account_ref,idempotency_key,intent_hash,order_spec)
            VALUES(%s,%s,'futu','credential:7',%s,'test',%s::jsonb)
            ON CONFLICT(agent_token_id,idempotency_key) DO UPDATE SET updated_at=NOW() RETURNING *''',
            (user,token['id'],key,store.dumps(order)),one=True)
    def fill(user,token,intent_id,**kwargs):
        with kwargs['submit_guard']():
            intent=store.query("SELECT * FROM qd_agent_trade_intents WHERE id=%s",(intent_id,),one=True)
            order=intent['order_spec']
            submitted.append((order['side'],order['qty']))
            holdings.clear()
            if order['side']=='buy':
                holdings.append({'symbol':'AAPL','quantity':order['qty'],'side':'long','marketValue':100*order['qty']})
            return store.query("UPDATE qd_agent_trade_intents SET status='FILLED',filled_qty=%s WHERE id=%s RETURNING *",(order['qty'],intent_id),one=True)
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent',submit)
    monkeypatch.setattr('app.services.futu_agent_execution.execute_simulate_intent',fill)
    for index,item in enumerate([decision(),decision(action='EXIT',target_weight=0)]):
        run=store.create_run(row,f'replay:{index}',datetime.now(timezone.utc)+timedelta(seconds=60))
        run['result']={'items':[item]}
        store.update_run(run['id'],status='planned',result=run['result'])
        runner.execute(row,run)
        runner.execute(row,run)  # Restart/replay does not resubmit completed work.
        result=store.query('SELECT status,phase FROM qd_agent_automation_runs WHERE id=%s',(run['id'],),one=True)
        assert result['status']=='completed',result
    assert submitted==[('buy',2),('sell',2)]
    assert store.owned_quantities(row)=={'AAPL':0} and holdings==[]


def monitored_task(monkeypatch, **changes):
    from unittest.mock import MagicMock
    from app.services.automation import monitor
    row = store.set_active(900001, create(execution_mode='paper_auto', risk={'enabled': True}, **changes)['id'], True)
    store.query('''INSERT INTO qd_agent_trade_intents
        (user_id,agent_token_id,broker,account_ref,idempotency_key,intent_hash,order_spec,status,filled_qty,avg_fill_price)
        VALUES(%s,%s,'futu','credential:7','fill-fixture','fixture',%s::jsonb,'FILLED',5,100) RETURNING id''',
        (row['user_id'], row['token_id'], store.dumps({'symbol': 'AAPL', 'side': 'buy'})), one=True)
    account = {'as_of': time.time(), 'positions': [{'symbol': 'AAPL', 'quantity': 5, 'side': 'long'}],
               'open_orders': [], 'funds': {'cash': 500, 'power': 500}}
    monkeypatch.setattr(monitor, 'account_snapshot', lambda *_: account)
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = {'simulate_execution_eligible': True, 'price': 100}
    monkeypatch.setattr(monitor, '_load_client', lambda *_: client)
    return row, client


def test_independent_monitor_persists_curve_and_protection_without_model(db, monkeypatch):
    from app.services.automation import monitor
    row, client = monitored_task(monkeypatch)
    monkeypatch.setattr('app.services.llm.LLMService', lambda **_: pytest.fail('Protection must not invoke a model'))
    monitor.observe(row)
    current = store.task(row['user_id'], row['id'])
    assert current['state']['risk']['healthy']
    assert current['state']['performance']['latest']['equity'] == 1000
    assert len(monitor.dashboard(current)['series']) == 1
    # A cumulative fill replay creates neither duplicate profit nor minute samples.
    monitor.observe(current)
    assert len(monitor.dashboard(store.task(row['user_id'], row['id']))['series']) == 1
    client.get_simulate_execution_quote.return_value['price'] = 90
    monitor.observe(current)
    current = store.task(row['user_id'], row['id'])
    assert current['state']['risk']['halted']
    assert current['state']['risk']['stopped_symbols'] == ['AAPL']
    run = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],), one=True)
    assert run['event_key'].startswith('protection:') and run['status'] == 'planned'
    assert run['result']['items'][0]['action'] == 'EXIT'
    monitor.observe(current)
    assert len(store.query('SELECT id FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],))) == 1


def test_paused_task_is_observed_but_never_protectively_submits_and_reset_preserves_history(db, monkeypatch):
    from app.services.automation import monitor
    row, client = monitored_task(monkeypatch)
    monitor.observe(row)
    row = store.set_active(row['user_id'], row['id'], False)
    client.get_simulate_execution_quote.return_value['price'] = 90
    monitor.observe(row)
    row = store.task(row['user_id'], row['id'])
    assert row['state']['risk']['halted']
    assert not store.query('SELECT id FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],))
    samples = monitor.dashboard(row)['series']
    # Closing/stale quotes do not erase the last valid mark needed for reset.
    client.get_simulate_execution_quote.return_value['simulate_execution_eligible'] = False
    monitor.observe(row)
    row = store.reset_risk(row['user_id'], row['id'])
    assert not row['state']['risk']['halted'] and not row['state']['risk']['healthy']
    assert row['state']['performance']['high_water'] == 950
    assert monitor.dashboard(row)['series'] == samples
    assert len(store.receipts(row)) == 1


def test_final_buy_guard_reads_current_protection_state(db, monkeypatch):
    row, _ = monitored_task(monkeypatch)
    run = run_for(row, preview=False)
    store.update_run(run['id'], status='executing')
    order = {'side': 'buy', 'symbol': 'AAPL'}
    # The model's old snapshot cannot authorize a purchase before monitoring.
    with pytest.raises(ValueError, match='独立保护'):
        with store.submission_guard(row, run, order):
            pytest.fail('Unmonitored buy must be blocked')
    store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{risk}',%s::jsonb) WHERE id=%s RETURNING id",
                (store.dumps({'healthy': True, 'checked_at': time.time(), 'halted': False}), row['id']), one=True)
    with store.submission_guard(row, run, order):
        pass
    store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{risk,halted}','true') WHERE id=%s RETURNING id", (row['id'],), one=True)
    with pytest.raises(ValueError, match='独立保护'):
        with store.submission_guard(row, run, order):
            pytest.fail('Protection tripped after planning must block the buy')
    with store.submission_guard(row, run, {'side': 'sell', 'symbol': 'AAPL'}):
        pass


def test_dashboard_http_is_user_scoped_and_cached_without_broker_io(db, monkeypatch):
    from flask import Flask
    from app.routes.agent_automations import blp
    row = create()
    app = Flask(__name__)
    app.register_blueprint(blp, url_prefix='/api/agent-automations')
    client = app.test_client()
    assert client.get(f"/api/agent-automations/{row['id']}/dashboard").status_code == 401
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 900001, '_verified_user_role': 'user'})
    monkeypatch.setattr('app.routes.agent_automations.account_snapshot', lambda *_: pytest.fail('Dashboard must be cached'))
    headers = {'Authorization': 'Bearer fixture-only'}
    result = client.get(f"/api/agent-automations/{row['id']}/dashboard", headers=headers).get_json()
    assert result['code'] == 1 and result['data']['series'] == []
    assert 'token_id' not in result['data']['task']
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 900002, '_verified_user_role': 'user'})
    assert client.get(f"/api/agent-automations/{row['id']}/dashboard", headers=headers).status_code == 400


def test_universe_edit_retains_owned_instruments_and_cannot_rewrite_capital(db, monkeypatch):
    row, _ = monitored_task(monkeypatch)
    with pytest.raises(ValueError, match='先暂停'):
        store.reset_risk(row['user_id'], row['id'])
    row = store.set_active(row['user_id'], row['id'], False)
    changed = {**row['config'], 'symbols': ['TSLA']}
    row = store.edit(row['user_id'], row['id'], 'new universe', changed)
    token = store.query('SELECT instruments FROM qd_agent_tokens WHERE id=%s', (row['token_id'],), one=True)
    assert set(token['instruments'].split(',')) == {'AAPL', 'TSLA'}
    with pytest.raises(ValueError, match='本金'):
        store.edit(row['user_id'], row['id'], 'changed capital', {**changed, 'budget': 2000})


def test_daily_budget_survives_failure_cancel_revision_and_exempts_protection(db):
    row = create(research={'max_decisions_per_day': 2})
    first = run_for(row)
    store.update_run(first['id'], status='failed')
    second = store.create_run(row, 'preview:second', first['expires_at'], preview=True)
    store.cancel_run(row['user_id'], second['id'])
    row = store.edit(row['user_id'], row['id'], 'edited', row['config'])
    assert store.create_run(row, 'preview:third', first['expires_at'], preview=True) is None
    quota = store.decision_budget(row)
    assert quota['used'] == 2 and quota['remaining'] == 0 and quota['timezone'] == 'America/New_York'
    store.query("INSERT INTO qd_agent_automation_runs(task_id,user_id,revision,event_key,status,expires_at) VALUES(%s,%s,%s,'protection:fixture','completed',%s) RETURNING id",
        (row['id'], row['user_id'], row['revision'], first['expires_at']), one=True)
    assert store.decision_budget(row)['used'] == 2


def test_tool_loop_preview_persists_each_call_and_cannot_submit(db, monkeypatch):
    from tests.test_agent_research import Model, evidence
    row = create(research={'mode': 'tool_loop'})
    run = run_for(row)
    model = Model([{'tool_requests': [{'tool': 'quote', 'arguments': {'symbol': 'AAPL'}}]},
                   {'summary': 'wait', 'items': [{'symbol': 'AAPL', 'action': 'WAIT', 'target_weight': 0}]}])
    monkeypatch.setattr(runner, 'build_evidence', lambda *_a, **_k: evidence())
    monkeypatch.setattr('app.services.llm.LLMService', lambda **_: model)
    monkeypatch.setattr(runner, 'execute', lambda *_: pytest.fail('Preview cannot execute'))
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'completed', saved['phase']
    assert saved['result']['prompt_version'] == 'portfolio-tools-v1'
    assert len(saved['result']['tool_trace']) == 1 and saved['result']['usage']['request_count'] == 2
    from app.routes.agent_automations import public_run
    brief = public_run(saved)
    assert 'tool_trace' not in brief['result'] and brief['result']['tool_request_count'] == 1
    assert public_run(saved, evidence=True)['result']['tool_trace'] == saved['result']['tool_trace']
    from app.services.automation.monitor import dashboard
    assert dashboard(row)['model_usage']['recorded_calls'] == 2
    assert store.receipts(row) == []


def test_tool_loop_failure_keeps_usage_without_executable_items(db, monkeypatch):
    from tests.test_agent_research import Model, evidence
    row = create(research={'mode': 'tool_loop', 'max_model_calls': 1})
    run = run_for(row)
    model = Model([{'tool_requests': [{'tool': 'account', 'arguments': {}}]}])
    monkeypatch.setattr(runner, 'build_evidence', lambda *_a, **_k: evidence())
    monkeypatch.setattr('app.services.llm.LLMService', lambda **_: model)
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'failed' and 'items' not in saved['result']
    assert saved['result']['usage']['request_count'] == 1 and store.receipts(row) == []


def test_event_review_dedup_busy_cooldown_and_pause(db, monkeypatch):
    from app.services.automation import monitor
    row, client = monitored_task(monkeypatch, kind='event_portfolio', cooldown_seconds=30)
    monitor.observe(row)
    current = store.task(row['user_id'], row['id'])
    first = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],), one=True)
    assert first['evidence']['event']['type'] == 'initial_observation' and first['status'] == 'queued'
    monitor.observe(current)
    assert store.decision_budget(current)['used'] == 1
    store.update_run(first['id'], status='completed')
    # Expire the cooldown without changing remote timestamps.
    state = current['state']['event_review']
    state['reviewed_at'] -= 31
    store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{event_review}',%s::jsonb) WHERE id=%s RETURNING id", (store.dumps(state), row['id']), one=True)
    client.get_simulate_execution_quote.return_value['price'] = 103
    monitor.observe(store.task(row['user_id'], row['id']))
    latest = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s ORDER BY id DESC LIMIT 1', (row['id'],), one=True)
    assert latest['evidence']['event']['type'] == 'price_movement' and latest['id'] != first['id']
    row = store.set_active(row['user_id'], row['id'], False)
    client.get_simulate_execution_quote.return_value['price'] = 106
    monitor.observe(row)
    assert store.decision_budget(row)['used'] == 2


def test_event_review_defers_for_orders_and_protection_has_priority(db, monkeypatch):
    from app.services.automation import monitor
    row, client = monitored_task(monkeypatch, kind='event_portfolio')
    store.query("UPDATE qd_agent_trade_intents SET status='PARTIALLY_FILLED' WHERE agent_token_id=%s RETURNING id", (row['token_id'],), one=True)
    monitor.observe(row)
    assert store.decision_budget(row)['used'] == 0
    assert not store.task(row['user_id'], row['id'])['state'].get('event_review')
    store.query("UPDATE qd_agent_trade_intents SET status='FILLED' WHERE agent_token_id=%s RETURNING id", (row['token_id'],), one=True)
    client.get_simulate_execution_quote.return_value['price'] = 90
    monitor.observe(store.task(row['user_id'], row['id']))
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],), one=True)
    assert saved['event_key'].startswith('protection:') and store.decision_budget(row)['used'] == 0


def test_fill_event_uses_durable_cumulative_changes(db, monkeypatch):
    from app.services.automation import monitor
    row, _ = monitored_task(monkeypatch, kind='event_portfolio')
    monitor.observe(row)
    first = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s', (row['id'],), one=True)
    store.update_run(first['id'], status='completed')
    state = store.task(row['user_id'], row['id'])['state']['event_review']
    state['reviewed_at'] -= 301
    store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{event_review}',%s::jsonb) WHERE id=%s RETURNING id", (store.dumps(state), row['id']), one=True)
    store.query('UPDATE qd_agent_trade_intents SET filled_qty=6 WHERE agent_token_id=%s RETURNING id', (row['token_id'],), one=True)
    monkeypatch.setattr(monitor, 'account_snapshot', lambda *_: {'as_of': time.time(),
        'positions': [{'symbol': 'AAPL', 'quantity': 6, 'side': 'long'}], 'open_orders': [], 'funds': {'cash': 400}})
    monitor.observe(store.task(row['user_id'], row['id']))
    latest = store.query('SELECT * FROM qd_agent_automation_runs WHERE task_id=%s ORDER BY id DESC LIMIT 1', (row['id'],), one=True)
    assert latest['evidence']['event']['type'] == 'fills_changed'
    assert latest['evidence']['event']['fills'][0][1:] == [6.0, 100.0]


def test_event_research_preserves_trigger_and_invokes_existing_executor(db, monkeypatch):
    from tests.test_agent_research import Model, evidence
    row = store.set_active(900001, create(kind='event_portfolio', execution_mode='paper_auto', research={'mode': 'tool_loop'})['id'], True)
    run = run_for(row, preview=False)
    run['evidence'] = {'event': {'type': 'price_movement', 'observed_at': time.time()}}
    model = Model([{'summary': 'wait', 'items': [{'symbol': 'AAPL', 'action': 'WAIT', 'target_weight': 0}]}])
    monkeypatch.setattr(runner, 'build_evidence', lambda *_a, **_k: evidence())
    monkeypatch.setattr('app.services.llm.LLMService', lambda **_: model)
    invoked = []
    monkeypatch.setattr(runner, 'execute', lambda row, run: invoked.append(run['result']))
    runner.analyze(row, run)
    assert len(invoked) == 1 and invoked[0]['event']['type'] == 'price_movement'
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'planned' and saved['evidence']['event']['type'] == 'price_movement'


def test_quota_reservations_are_serialized_across_connections(db):
    from concurrent.futures import ThreadPoolExecutor
    row = create(research={'max_decisions_per_day': 1})
    def reserve(index):
        with store.get_db_connection() as connection:
            cur = connection.cursor()
            cur.execute('SELECT * FROM qd_agent_automations WHERE id=%s FOR UPDATE', (row['id'],))
            current = dict(cur.fetchone())
            result = store.admit_run(cur, current, f'parallel:{index}', datetime.now(timezone.utc)+timedelta(seconds=180), preview=True)
            if result:
                # Complete before releasing the lock: the next contender is
                # rejected by quota, not merely by the busy-run check.
                cur.execute("UPDATE qd_agent_automation_runs SET status='completed' WHERE id=%s", (result['id'],))
            connection.commit()
            return result
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, range(2)))
    assert sum(result is not None for result in results) == 1
    assert store.decision_budget(row)['used'] == 1


def ready_services(monkeypatch):
    from app.services.automation import readiness
    monkeypatch.setattr(readiness, 'model_state', lambda _: {'configured': True, 'provider': 'openai', 'model': 'fixture', 'code': 'model_configured'})
    monkeypatch.setattr(readiness, 'get_policy', lambda *_: {'mode': 'PAPER_AUTO', 'enabled_until': datetime.now(timezone.utc)+timedelta(hours=1),
        'allowed_symbols': ['AAPL'], 'allowed_markets': ['USSTOCK'], 'max_order_notional': 500, 'max_daily_notional': 2000, 'max_orders_per_day': 10})
    monkeypatch.setattr(readiness, 'hard_switch_enabled', lambda: True)
    monkeypatch.setattr(readiness, 'state_for_user', lambda _: [{'credential_id': 7, 'enabled': True, 'state': 'armed'}])
    return readiness


def test_readiness_requires_model_exact_policy_actor_and_operator_without_remote_io(db, monkeypatch):
    readiness = ready_services(monkeypatch)
    row = create(execution_mode='paper_auto')
    monkeypatch.setattr(readiness, '_load_client', lambda *_: pytest.fail('Cached readiness cannot use broker'))
    result = readiness.read(row)
    assert result['configuration_ready'] and result['connection_check'] is None
    assert result['account_limits']['remaining_orders'] == 10
    monkeypatch.setattr(readiness, 'state_for_user', lambda _: [{'credential_id': 7, 'enabled': True, 'state': 'stopping'}])
    assert not readiness.read(row)['configuration_ready']
    with pytest.raises(ValueError, match='operator_required'):
        readiness.require_configuration(row)
    monkeypatch.setattr(readiness, 'state_for_user', lambda _: [{'credential_id': 7, 'enabled': True, 'state': 'armed'}])
    store.query("UPDATE qd_agent_tokens SET instruments='TSLA' WHERE id=%s RETURNING id", (row['token_id'],), one=True)
    assert not readiness.read(row)['configuration_ready']


def test_readiness_expired_policy_and_retained_holding_scope(db, monkeypatch):
    readiness = ready_services(monkeypatch)
    row, _ = monitored_task(monkeypatch)
    policy = readiness.get_policy(None)
    policy['allowed_symbols'] = ['TSLA']
    monkeypatch.setattr(readiness, 'get_policy', lambda *_: policy)
    result = readiness.read(row)
    assert not result['configuration_ready']
    assert next(c for c in result['checks'] if c['key'] == 'allowlist')['missing_symbols'] == ['AAPL']
    policy['allowed_symbols'] = ['AAPL']
    policy['enabled_until'] = datetime.now(timezone.utc)-timedelta(seconds=1)
    assert not readiness.read(row)['configuration_ready']


def test_readiness_published_component_requires_fresh_successful_loop(db, monkeypatch):
    readiness = ready_services(monkeypatch)
    row = create()
    state = {'leader': True, 'agent_automation': {'status': 'running', 'last_tick_at': time.time(), 'last_success_at': time.time()}}
    store.query("INSERT INTO qd_worker_heartbeats(worker_id,role,status,metadata_json) VALUES('fixture','scheduler','running',%s::jsonb) RETURNING worker_id", (store.dumps(state),), one=True)
    assert readiness.read(row)['scheduler']['status'] == 'running'
    state['agent_automation']['last_tick_at'] -= 16
    store.query('UPDATE qd_worker_heartbeats SET metadata_json=%s::jsonb RETURNING worker_id', (store.dumps(state),), one=True)
    assert readiness.read(row)['scheduler']['status'] == 'stalled'


def test_manual_connection_probe_is_revision_scoped_and_never_submits(db, monkeypatch):
    from unittest.mock import MagicMock
    readiness = ready_services(monkeypatch)
    row = create()
    account = {'as_of': time.time(), 'open_orders': []}
    monkeypatch.setattr(readiness, 'account_snapshot', lambda *_: account)
    monkeypatch.setattr(readiness, 'market_clock', lambda _: {'is_open': True})
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = {'simulate_execution_eligible': True, 'price': 100, 'as_of': time.time(), 'market_status': 'AFTERNOON'}
    monkeypatch.setattr(readiness, '_load_client', lambda *_: client)
    result = readiness.probe(row)
    assert result['account_ok'] and result['quote_status'] == 'pass'
    client.place_limit_order.assert_not_called()
    client.cancel_order.assert_not_called()
    client.disconnect.assert_called_once()
    current = store.task(row['user_id'], row['id'])
    assert readiness.read(current)['connection_check']['revision'] == row['revision']
    row = store.edit(row['user_id'], row['id'], 'changed', row['config'])
    assert readiness.read(row)['connection_check'] is None
    with pytest.raises(ValueError, match='changed'):
        readiness.probe(current)


def test_probe_sanitizes_failure_and_closed_session_is_waiting(db, monkeypatch):
    readiness = ready_services(monkeypatch)
    row = create()
    monkeypatch.setattr(readiness, 'account_snapshot', lambda *_: (_ for _ in ()).throw(RuntimeError('secret-fixture-token')))
    result = readiness.probe(row)
    assert not result['account_ok'] and 'secret-fixture' not in store.dumps(result)
    monkeypatch.setattr(readiness, 'account_snapshot', lambda *_: {'as_of': time.time(), 'open_orders': []})
    monkeypatch.setattr(readiness, 'market_clock', lambda _: {'is_open': False})
    monkeypatch.setattr(readiness, '_load_client', lambda *_: pytest.fail('Closed session should defer quote probe'))
    result = readiness.probe(row)
    assert result['account_ok'] and result['quote_code'] == 'market_closed' and result['quote_status'] == 'wait'


def test_review_http_is_owner_scoped_read_only_and_explicitly_partial(db, monkeypatch):
    from flask import Flask
    from app.routes.agent_automations import blp
    from app.services.automation import review
    row, _ = monitored_task(monkeypatch)
    first = run_for(row)
    store.update_run(first['id'], status='completed', result={'items': [], 'summary': 'fixture'})
    second = store.create_run(row, 'preview:review', first['expires_at'], preview=True)
    store.update_run(second['id'], status='failed')
    monkeypatch.setattr(review, 'RUN_LIMIT', 1)
    app = Flask(__name__)
    app.register_blueprint(blp, url_prefix='/api/agent-automations')
    client = app.test_client()
    assert client.get(f"/api/agent-automations/{row['id']}/review").status_code == 401
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 900001, '_verified_user_role': 'user'})
    headers = {'Authorization': 'Bearer fixture-only'}
    response = client.get(f"/api/agent-automations/{row['id']}/review?days=14", headers=headers)
    assert response.status_code == 200, response.get_json()
    report = response.get_json()['data']
    assert report['schema_version'] == 'agent-paper-review-v1' and report['coverage']['partial'] is True
    assert len(report['orders']) == 1 and 'broker_order_id' not in report['orders'][0]
    assert 'token_id' not in report['task'] and 'brief' not in report['configuration']
    assert client.get(f"/api/agent-automations/{row['id']}/review?days=0", headers=headers).status_code == 400
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 900002, '_verified_user_role': 'user'})
    for path, method in [('readiness', 'get'), ('check-connection', 'post'), ('review', 'get')]:
        assert getattr(client, method)(f"/api/agent-automations/{row['id']}/{path}", headers=headers).status_code == 400


def test_restart_cannot_revive_an_old_in_memory_submission(db):
    from app.services.automation import health
    row = store.set_active(900001, create(execution_mode='paper_auto')['id'], True)
    run = run_for(row, preview=False)
    store.update_run(run['id'], status='executing')
    run['_worker_generation'] = health.generation()
    with store.submission_guard(row, run):
        pass
    health.stopped()
    worker.STOP.clear()
    health.started()
    assert store.cancelled(run)
    with pytest.raises(ValueError, match='cancelled'):
        with store.submission_guard(row, run):
            pytest.fail('An old callback cannot submit after restart')
    # Durable work can be claimed afresh, with the current process generation.
    run['_worker_generation'] = health.generation()
    with store.submission_guard(row, run):
        pass
    health.stopped()


def test_restart_cancels_model_result_before_validation(db, monkeypatch):
    from app.services.automation import health
    from tests.test_agent_research import evidence
    row = create()
    run = run_for(row)
    class Model:
        provider = SimpleNamespace(value='openai')
        last_provider, last_model, last_usage = 'openai', 'fixture', None
        def __init__(self, **_): pass
        def stream_llm_api_cancellable(self, messages, cancelled, **_):
            yield '{"summary":"wait",'
            health.stopped()
            worker.STOP.clear()
            health.started()
            assert cancelled()
            yield '"items":[]}'
    monkeypatch.setattr(runner, 'build_evidence', lambda *_a, **_k: evidence())
    monkeypatch.setattr('app.services.llm.LLMService', Model)
    runner.analyze(row, run)
    saved = store.query('SELECT * FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'cancelled' and 'items' not in saved['result']
    health.stopped()


def test_missing_model_blocks_http_start_before_broker_io_but_pause_is_available(db, monkeypatch):
    from flask import Flask
    from app.routes import agent_automations as routes
    readiness = ready_services(monkeypatch)
    row = store.set_active(900001, create(execution_mode='paper_auto')['id'], True)
    monkeypatch.setattr(readiness, 'model_state', lambda _: {'configured': False, 'code': 'model_unconfigured'})
    monkeypatch.setattr(routes, 'account_snapshot', lambda *_: pytest.fail('Missing configuration must fail before broker I/O'))
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 900001, '_verified_user_role': 'user'})
    app = Flask(__name__)
    app.register_blueprint(routes.blp, url_prefix='/api/agent-automations')
    client = app.test_client()
    headers = {'Authorization': 'Bearer fixture-only'}
    url = f"/api/agent-automations/{row['id']}/state"
    response = client.post(url, headers=headers, json={'active': True})
    assert response.status_code == 400 and 'model_unconfigured' in response.get_json()['msg']
    assert store.task(row['user_id'], row['id'])['active']
    response = client.post(url, headers=headers, json={'active': False})
    assert response.status_code == 200 and response.get_json()['data']['active'] is False


def test_old_queued_callbacks_cannot_claim_durable_work_after_restart(db, monkeypatch):
    from app.services.automation import health
    analysis_row = create()
    analysis = run_for(analysis_row)
    execution_row = store.set_active(900001, create(execution_mode='paper_auto')['id'], True)
    execution = run_for(execution_row, preview=False)
    store.update_run(execution['id'], status='planned')
    health.started()
    for run in (analysis, execution):
        run['_worker_generation'] = health.generation()
    health.stopped()
    worker.STOP.clear()
    health.started()
    monkeypatch.setattr(runner, 'build_evidence', lambda *_: pytest.fail('Obsolete callbacks must not start research'))
    runner.analyze(analysis_row, analysis)
    runner.execute(execution_row, execution)
    states = store.query('SELECT id,status FROM qd_agent_automation_runs ORDER BY id')
    assert [r['status'] for r in states] == ['queued', 'planned']
    health.stopped()


@pytest.mark.parametrize('market,symbol', [('HKStock', '01810'), ('USStock', 'AAPL')])
def test_fast_admission_not_hidden_by_bulk_backlog(db, market, symbol):
    bulk_ids = []
    for _ in range(10):
        row = create()
        bulk_ids.append(run_for(row)['id'])
    fast_row = create(kind='price_trigger', market=market, symbols=[symbol], trigger={'type': 'price_above', 'price': 10})
    store.query('UPDATE qd_agent_automations SET active=TRUE WHERE id=%s RETURNING id', (fast_row['id'],))
    fast_row = store.task(900001, fast_row['id'])
    fast_run = run_for(fast_row, preview=False)
    assert [r['id'] for r in worker.ready_runs(True)] == [fast_run['id']]
    assert [r['id'] for r in worker.ready_runs(False)] == bulk_ids[:8]
    store.update_run(fast_run['id'], status='planned')
    assert [r['id'] for r in worker.ready_runs(True)] == [fast_run['id']]

    preview_row = create(kind='price_trigger', market='HKStock', symbols=['00700'],
                         trigger={'type': 'price_above', 'price': 10})
    preview_run = run_for(preview_row)
    assert preview_run['id'] not in {r['id'] for r in worker.ready_runs(True)}
    store.update_run(fast_run['id'], status='expired')
    assert worker.ready_runs(True) == []


def test_hk_fast_latency_concurrent_triggers_not_starved(db, monkeypatch):
    """Real PG ledger and runner, offline model/broker, saturated bulk pool."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from app.services.automation import latency

    count = int(os.environ.get('HK_LATENCY_STRESS_TRIGGERS', '32'))
    bound = float(os.environ.get('HK_LATENCY_STRESS_P95_MS', '10000'))
    assert count >= 20 and bound > 0
    release = threading.Event()
    barrier = threading.Barrier(3)

    class Model:
        provider = SimpleNamespace(value='openai')
        last_provider, last_model, last_usage = 'openai', 'offline', {}
        def __init__(self, **_): pass
        def stream_llm_api_cancellable(self, messages, cancelled, **_):
            assert not cancelled()
            time.sleep(.002)
            yield '{"summary":"观察","items":[{"symbol":"01810.HK","action":"WAIT","target_weight":0}]}'

    monkeypatch.setattr('app.services.llm.LLMService', Model)
    monkeypatch.setattr(runner, 'execute', lambda *_: pytest.fail('Advisory plan cannot execute'))
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', lambda *_: pytest.fail('No network'))
    def trigger(i):
        row = create(market='HKStock', symbols=['01810.HK'], kind='price_trigger',
                     trigger={'type': 'price_above', 'price': 20})
        row = store.set_active(900001, row['id'], True)
        run = store.create_run(row, f'stress:{i}', datetime.now(timezone.utc)+timedelta(seconds=15))
        latency.mark(run, 'trigger_detected')
        latency.persist(run)
        evidence = {'as_of': time.time(), 'account': {'as_of': time.time()},
                    'instruments': {'01810.HK': {'price': 21}}}
        quote = {'price': 21, 'as_of': time.time(), 'received_at': time.time()}
        return run, (evidence, quote)

    def slow():
        barrier.wait(timeout=5)
        release.wait(timeout=30)

    with ThreadPoolExecutor(max_workers=2) as bulk, ThreadPoolExecutor(max_workers=1) as fast:
        try:
            blocked = [bulk.submit(slow) for _ in range(2)]
            barrier.wait(timeout=5)
            with ThreadPoolExecutor(max_workers=8) as producers:
                events = list(producers.map(trigger, range(count)))
            runs = [run for run, _ in events]
            contexts = {run['id']: context for run, context in events}
            pending = {}
            deadline = time.monotonic()+20
            while runs or pending:
                assert time.monotonic() < deadline, 'Fast slot starved'
                for key, future in list(pending.items()):
                    if future.done():
                        future.result()
                        pending.pop(key)
                        runs = [run for run in runs if run['id'] != key]
                worker.dispatch_runs(fast, pending, 1, runs, contexts)
                assert len(pending) <= 1
                time.sleep(.001)
            assert all(not future.done() for future in blocked)
            rows = store.query('SELECT result,status FROM qd_agent_automation_runs ORDER BY id')
            assert len(rows) == count and all(row['status'] == 'completed' for row in rows)
            metrics = latency.summarize(rows)
            assert metrics['stages_ms']['total']['count'] == count
            assert metrics['stages_ms']['total']['p95'] <= bound, metrics
            for stage in latency.STAGES[1:]:
                assert metrics['stages_ms'][stage]['count'] == count
            assert latency.read(store.task(900001, events[0][0]['task_id']))['sampled_runs'] == 1
            print(f'HK stress: {count} plans, P95={metrics["stages_ms"]["total"]["p95"]} ms, bound={bound} ms')
        finally:
            release.set()


@pytest.mark.parametrize('market,symbol', [('HKStock', '01810.HK'), ('USStock', 'AAPL')])
def test_hk_latency_failed_model_has_no_submission(db, monkeypatch, market, symbol):
    from app.services.automation import latency
    row = create(market=market, symbols=[symbol], kind='price_trigger',
                 trigger={'type': 'price_above', 'price': 20})
    row = store.set_active(900001, row['id'], True)
    run = run_for(row, preview=False)
    latency.mark(run, 'trigger_detected')
    latency.persist(run)
    evidence = {'as_of': time.time(), 'account': {'as_of': time.time()},
                'instruments': {symbol: {'price': 21}}}
    class Model:
        provider = SimpleNamespace(value='openai')
        def __init__(self, **_): pass
        def stream_llm_api_cancellable(self, *_a, **_k):
            raise RuntimeError('provider body must not be logged')
    monkeypatch.setattr('app.services.llm.LLMService', Model)
    runner.analyze(row, run, evidence, {'price': 21, 'as_of': time.time()})
    saved = store.query('SELECT result,status FROM qd_agent_automation_runs WHERE id=%s', (run['id'],), one=True)
    assert saved['status'] == 'failed'
    assert set(saved['result']['fast_latency']['timestamps']) == {'trigger_detected', 'context_built', 'model_call', 'finished'}
    metrics = latency.read(row)
    assert metrics['stages_ms']['total']['count'] == 1
    assert metrics['stages_ms']['decision'] == {'count': 0, 'p50': None, 'p95': None}
    assert metrics['stages_ms']['submitted']['count'] == 0
    assert latency.read({**row, 'user_id': 900002})['sampled_runs'] == 0

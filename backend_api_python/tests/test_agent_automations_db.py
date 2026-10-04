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
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET search_path TO {schema}')
        cur.execute((Path(__file__).parents[1]/'migrations/init.sql').read_text())
        cur.execute((Path(__file__).parents[1]/'migrations/20261004_agent_automation.sql').read_text())
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

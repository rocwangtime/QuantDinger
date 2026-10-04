"""Scheduler-owned task dispatcher. DB claims protect work across restarts."""
from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from app.services.automation import store
from app.services.automation.domain import session_schedule, trigger_state
from app.services.automation.market import PushFeed, build_evidence, account_snapshot
from app.services.automation.runner import analyze, execute
from app.services.research_workflow import market_clock
from app.utils.logger import get_logger

logger = get_logger(__name__)
STOP = threading.Event()
_thread = None


def start():
    global _thread
    if _thread and _thread.is_alive():
        return
    store.ensure_schema()
    STOP.clear()
    _thread = threading.Thread(target=loop,name='AgentAutomation',daemon=True)
    _thread.start()


def stop():
    STOP.set()
    if _thread:
        _thread.join(timeout=3)


def reconcile_orders():
    """Reconcile accepted orders and request cancellation after plan validity.

    Cancellation is not a fill guarantee: a fill can race the broker's cancel
    acknowledgement. Always reconcile the final broker result afterwards.
    """
    from app.services.futu_agent_execution import _load_client, reconcile_simulate_intent
    rows = store.query("""SELECT i.user_id,i.id,i.account_ref,r.expires_at
        FROM qd_agent_trade_intents i JOIN qd_agent_automations t ON t.token_id=i.agent_token_id
        JOIN qd_agent_automation_runs r ON i.order_spec->>'strategy_version'=
            'automation:'||t.id::text||':'||r.id::text
        WHERE i.status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED') LIMIT 30""")
    for row in rows:
        if STOP.is_set():
            return
        try:
            receipt = reconcile_simulate_intent(row['user_id'],row['id'])
            if row['expires_at'] >= datetime.now(timezone.utc) or receipt['status'] not in {'SUBMITTED','PARTIALLY_FILLED'} or not receipt.get('broker_order_id'):
                continue
            client = _load_client(row['user_id'],row['account_ref'])
            try:
                if client.connect(need_quote=False):
                    client.cancel_order(receipt['broker_order_id'])
            finally:
                client.disconnect()
            reconcile_simulate_intent(row['user_id'],row['id'])
        except Exception:
            logger.warning('Task order reconciliation/cancellation pending; will retry')


def loop():
    pool = ThreadPoolExecutor(max_workers=2,thread_name_prefix='AgentDecision')
    preparation = ThreadPoolExecutor(max_workers=2,thread_name_prefix='AgentContext')
    pending, warm, feeds, cache, fast_events = {}, {}, {}, {}, {}
    retry_after = {}
    last_reconcile = 0.
    try:
        while not STOP.is_set():
            try:
                now = datetime.now(timezone.utc)
                tasks = store.query('SELECT * FROM qd_agent_automations WHERE active=TRUE ORDER BY id')
                active_ids = {r['id'] for r in tasks}
                for key in list(feeds):
                    if key not in active_ids:
                        feeds.pop(key).stop()
                        cache.pop(key,None)
                for key, future in list(pending.items()):
                    if future.done():
                        try:
                            future.result()
                        except Exception:
                            logger.warning('Automation job failed outside run handler')
                        pending.pop(key)
                for key, future in list(warm.items()):
                    if future.done():
                        try:
                            value = future.result()
                            if key[0] not in active_ids:
                                if key[1] == 'feed':
                                    value.stop()
                                warm.pop(key)
                                continue
                            if key[1]=='context':
                                cache[key[0]] = value
                            elif key[1]=='feed':
                                feeds[key[0]] = value
                            elif key[1]=='account' and key[0] in cache:
                                cache[key[0]]['account'] = value
                        except Exception:
                            retry_after[key] = time.time()+30
                        warm.pop(key)
                for row in tasks:
                    task_id, config = row['id'], row['config']
                    def report(message):
                        if (row['state'] or {}).get('monitor_status') != message:
                            store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{monitor_status}',%s::jsonb) WHERE id=%s AND revision=%s RETURNING id",
                                        (store.dumps(message),task_id,row['revision']),one=True)
                    if config['kind']=='daily_portfolio':
                        schedule = session_schedule(config,now)
                        if schedule['due']:
                            store.create_run(row,'session:'+schedule['session'],schedule['expires_at'],schedule['execute_at'])
                        report('等待盘前分析窗口' if not schedule['due'] else '盘前分析窗口已到')
                        continue
                    # No regular-session signal or execution during HK lunch/closures.
                    clock = market_clock(config['market'],now)
                    if not clock.get('is_open'):
                        report('当前非连续交易时段，等待开市')
                        continue
                    if task_id not in feeds and (task_id,'feed') not in warm and time.time() >= retry_after.get((task_id,'feed'),0):
                        def connect_feed(r=row):
                            feed = PushFeed(r)
                            feed.start()
                            return feed
                        warm[(task_id,'feed')] = preparation.submit(connect_feed)
                    evidence = cache.get(task_id)
                    if (not evidence or time.time()-evidence['as_of']>60) and (task_id,'context') not in warm and time.time() >= retry_after.get((task_id,'context'),0):
                        warm[(task_id,'context')] = preparation.submit(build_evidence,row,cancelled=STOP.is_set)
                    if evidence and time.time()-evidence['account']['as_of']>5 and (task_id,'account') not in warm and time.time() >= retry_after.get((task_id,'account'),0):
                        warm[(task_id,'account')] = preparation.submit(account_snapshot,row['user_id'],config)
                    feed = feeds.get(task_id)
                    if not feed or not evidence or time.time()-evidence['as_of']>120 or time.time()-evidence['account']['as_of']>15:
                        report('正在准备实时订阅和账户上下文；失败时每 30 秒重试')
                        continue
                    quote = feed.snapshot()
                    if not quote or time.time()-quote['received_at']>30:
                        # A silent connection is rebuilt; stale quotes never trigger.
                        if quote or time.time()-feed.created_at>30:
                            feeds.pop(task_id).stop()
                        report('等待新的富途推送报价')
                        continue
                    report('实时行情监听中；仅新鲜报价触发分析')
                    fire, state = trigger_state(config,quote,(row['state'] or {}).get('trigger'),time.time())
                    if fire:
                        expiry = datetime.fromtimestamp(quote['received_at'],timezone.utc)+timedelta(seconds=15)
                        run = store.create_run(row,f"price:{row['revision']}:{quote['as_of']}",expiry,
                                               state={**row['state'],'trigger':state})
                        if run:
                            fast_events[run['id']] = (copy.deepcopy(evidence),quote)
                    elif state != (row['state'] or {}).get('trigger'):
                        store.query("UPDATE qd_agent_automations SET state=jsonb_set(state,'{trigger}',%s::jsonb) WHERE id=%s AND revision=%s RETURNING id",(store.dumps(state),task_id,row['revision']),one=True)
                # Expired work is terminal. A crash after broker submission is
                # reconciled from its existing intent; it is never resubmitted.
                store.query("""UPDATE qd_agent_automation_runs SET status='expired',finished_at=NOW(),phase='任务已超时'
                    WHERE status IN ('queued','planned','researching','executing') AND expires_at<NOW() RETURNING id""")
                runs = store.query("""SELECT * FROM qd_agent_automation_runs WHERE status='queued'
                    OR (status='planned' AND (execute_at IS NULL OR execute_at<=NOW())) ORDER BY id LIMIT 8""")
                for run in runs:
                    if len(pending)>=2:
                        break
                    if run['id'] in pending:
                        continue
                    row = store.task(run['user_id'],run['task_id'])
                    if run['status']=='planned':
                        pending[run['id']] = pool.submit(execute,row,run)
                    elif row['config']['kind']=='price_trigger' and not run['preview']:
                        context = fast_events.pop(run['id'],None)
                        if context:
                            pending[run['id']] = pool.submit(analyze,row,run,*context)
                        else:
                            store.update_run(run['id'],status='expired',phase='触发上下文未保留，等待下一次新事件')
                    else:
                        pending[run['id']] = pool.submit(analyze,row,run)
                if time.monotonic()-last_reconcile>5 and ('reconcile',0) not in warm:
                    warm[('reconcile',0)] = preparation.submit(reconcile_orders)
                    last_reconcile = time.monotonic()
            except Exception:
                logger.warning('Agent automation tick failed; retrying without submitting new work',exc_info=False)
            STOP.wait(1)
    finally:
        for feed in feeds.values():
            feed.stop()
        pool.shutdown(wait=False,cancel_futures=True)
        preparation.shutdown(wait=False,cancel_futures=True)

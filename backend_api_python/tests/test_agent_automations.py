"""Deterministic rules and broker-boundary contracts; no provider or broker I/O."""
import json
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime
from datetime import timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.services.automation import domain, market, store


def config(**changes):
    return domain.normalize_config({'credential_id':7,'symbols':['AAPL'],**changes})


@pytest.mark.parametrize('changes', [
    {'market':'crypto'}, {'execution_mode':'real'}, {'symbols':['HK.01810']},
    {'symbols':['']}, {'symbols':['AAPL;DROP']}, {'budget':float('nan')},
    {'budget':True}, {'credential_id':1.5}, {'max_daily_notional':1},
    {'kind':'price_trigger','symbols':['AAPL','TSLA']},
    {'market':'HKStock','symbols':['US.AAPL']},
])
def test_invalid_config_is_rejected(changes):
    with pytest.raises((ValueError,TypeError)):
        config(**changes)


def test_hk_symbol_normalization():
    assert config(market='HKStock',symbols=['1810','HK.01810'])['symbols']==['01810.HK']


@pytest.mark.parametrize('stamp,session,hour,due', [
    ('2026-10-04T11:00:00+00:00','2026-10-05',13,False),
    ('2026-10-05T12:40:00+00:00','2026-10-05',13,True),
    ('2026-11-02T13:40:00+00:00','2026-11-02',14,True),
    ('2026-12-25T13:00:00+00:00','2026-12-28',14,False),
])
def test_schedule_uses_exchange_sessions_and_dst(stamp,session,hour,due):
    result = domain.session_schedule(config(),datetime.fromisoformat(stamp))
    assert result['session']==session
    assert result['open_at'].hour==hour
    assert result['due'] is due


def test_trigger_requires_fresh_edge_cooldown_and_rearm():
    cfg = config(kind='price_trigger',trigger={'type':'price_above','price':100})
    quote = {'price':101,'as_of':100,'is_realtime':True}
    assert not domain.trigger_state(cfg,quote,{},106)[0]
    assert not domain.trigger_state(cfg,{**quote,'as_of':110},{},100)[0]
    fired, state = domain.trigger_state(cfg,quote,{},100)
    assert fired
    assert not domain.trigger_state(cfg,{**quote,'as_of':500},state,500)[0]
    _, state = domain.trigger_state(cfg,{**quote,'price':99,'as_of':101},state,101)
    assert not domain.trigger_state(cfg,{**quote,'as_of':102},state,102)[0]
    assert domain.trigger_state(cfg,{**quote,'as_of':500},state,500)[0]


def decision(**changes):
    return {'symbol':'AAPL','action':'BUY','target_weight':.2,'min_price':99,'max_price':101,
            'reason':'test','invalidation':'below support',**changes}


@pytest.mark.parametrize('item', [decision(symbol='TSLA'), decision(target_weight=float('nan')),
    decision(action='SHORT'), decision(min_price=1), decision(action='EXIT'), 'buy'])
def test_malformed_or_outside_universe_decisions_rejected(item):
    with pytest.raises(ValueError):
        domain.parse_decision(json.dumps({'items':[item]}),{'instruments':{'AAPL':{'price':100}}},config())


def test_partial_json_never_becomes_a_decision():
    with pytest.raises(ValueError):
        domain.parse_decision('{"summary":"Buy now","items":[',{'instruments':{}},config())


def test_whole_lots_exposure_and_external_positions():
    cfg = config(budget=10000,max_order_notional=5000,max_daily_notional=10000,max_weight=.5)
    args = dict(price=100,lot=1,owned_qty=5,broker_qty=10,power=5000,open_symbols=set())
    assert domain.size_order(decision(),cfg,**args)['qty']==10
    assert domain.size_order(decision(action='EXIT',target_weight=0),cfg,**args)['qty']==5
    assert domain.size_order(decision(),cfg,**{**args,'lot':100}) is None
    assert domain.size_order(decision(),cfg,**{**args,'open_symbols':{'AAPL'}}) is None
    assert domain.size_order(decision(),cfg,**{**args,'price':102}) is None
    assert domain.size_order(decision(action='EXIT',target_weight=0),cfg,**{**args,'owned_qty':0}) is None


def test_snapshot_translates_actual_client_open_order_contract(monkeypatch):
    client = MagicMock()
    client.config.trade_market='US'
    client.get_account_summary.return_value={'success':True,'summary':{'currency':'USD','power':1000,'cash':1000}}
    client.get_positions.return_value=[]
    client.get_open_orders.return_value=[{'symbol':'US.AAPL','orderId':'test','action':'buy','quantity':10,'limitPrice':100,'filled':2,'status':'submitted'}]
    monkeypatch.setattr(market,'_load_client',lambda *_:client)
    data=market.account_snapshot(1,config())
    assert data['open_orders'][0]=={'symbol':'AAPL','order_id':'test','side':'buy','qty':10,'price':100,'filled':2,'status':'submitted'}
    client.disconnect.assert_called_once()


def test_owned_positions_use_cumulative_fill_not_event_count(monkeypatch):
    row={'state':{'baseline':{'AAPL':5}}}
    monkeypatch.setattr(store,'receipts',lambda _: [
        {'order_spec':{'symbol':'AAPL','side':'buy'},'filled_qty':3},
        {'order_spec':{'symbol':'AAPL','side':'sell'},'filled_qty':2},
    ])
    assert store.owned_quantities(row)=={'AAPL':6}
    assert store.owned_quantities(row)=={'AAPL':6}


@pytest.mark.parametrize('guarded', [False,True])
def test_final_broker_boundary_rejects_expiry_and_pause(guarded):
    from tests.test_futu_client_contract import _client_with_mocks, _FakeFT
    client, quote, trade = _client_with_mocks()
    client._is_regular_us_session_now = lambda: True
    quote.get_market_snapshot.return_value=(0,[{'lot_size':1,'last_price':100}])

    @contextmanager
    def cancelled_guard():
        raise ValueError('Task paused')
        yield

    with patch('app.services.futu_trading.client._ensure_futu',return_value=_FakeFT), patch(
        'app.services.futu_trading.operator_gate.submission_permit',return_value=nullcontext()):
        result=client.place_limit_order('AAPL','buy',1,100,remark='qd_1_2',
            deadline_at=time.time()+60 if guarded else time.time()-1,
            submit_guard=cancelled_guard if guarded else None)
    assert not result.success and not result.submission_attempted
    assert ('Task paused' in result.message) if guarded else (result.message=='AGENT_DECISION_EXPIRED')
    trade.place_order.assert_not_called()


@pytest.mark.parametrize('status,expired,cancel_count',[
    ('SUBMITTED',True,1),('PARTIALLY_FILLED',True,1),('SUBMITTED',False,0),
    ('FILLED',True,0),('UNCERTAIN',True,0),
])
def test_expired_pending_orders_request_cancel_then_reconcile(monkeypatch,status,expired,cancel_count):
    from app.services.automation import worker
    worker.STOP.clear()
    monkeypatch.setattr(store,'query',lambda *_:[{'id':1,'user_id':1,'account_ref':'credential:7',
        'expires_at':datetime.now(timezone.utc)+timedelta(seconds=-5 if expired else 60)}])
    reconcile=MagicMock(return_value={'status':status,'broker_order_id':'fixture-order'})
    monkeypatch.setattr('app.services.futu_agent_execution.reconcile_simulate_intent',reconcile)
    client=MagicMock()
    monkeypatch.setattr('app.services.futu_agent_execution._load_client',lambda *_:client)
    worker.reconcile_orders()
    assert client.cancel_order.call_count==cancel_count
    assert reconcile.call_count==1+cancel_count


@pytest.mark.parametrize('changes,cancels', [
    ({'active': False}, 1), ({'cancel_requested': True}, 1),
    ({'risk': {'halted': True}}, 1), ({'risk': {'stopped_symbols': ['AAPL']}}, 1),
    ({'risk': {'stopped_symbols': ['TSLA']}}, 0),
    ({'side': 'sell', 'risk': {'halted': True}}, 0),
])
def test_pause_cancel_and_protection_cancel_pending_orders_before_expiry(monkeypatch, changes, cancels):
    from app.services.automation import worker
    worker.STOP.clear()
    row = {'id': 1, 'user_id': 1, 'account_ref': 'credential:7', 'active': True,
           'cancel_requested': False, 'side': 'buy', 'symbol': 'AAPL',
           'expires_at': datetime.now(timezone.utc) + timedelta(seconds=60), **changes}
    monkeypatch.setattr(store, 'query', lambda *_: [row])
    reconcile = MagicMock(return_value={'status': 'SUBMITTED', 'broker_order_id': 'fixture-order'})
    monkeypatch.setattr('app.services.futu_agent_execution.reconcile_simulate_intent', reconcile)
    client = MagicMock()
    monkeypatch.setattr('app.services.futu_agent_execution._load_client', lambda *_: client)
    worker.reconcile_orders()
    assert client.cancel_order.call_count == cancels
    assert reconcile.call_count == 1 + cancels


def test_multi_symbol_run_cannot_reuse_cash_when_broker_snapshot_lags(monkeypatch):
    from app.services.automation import runner
    cfg=config(symbols=['AAPL','TSLA'],execution_mode='paper_auto',max_weight=.5)
    row={'id':1,'token_id':4,'user_id':1,'config':cfg,'revision':1,'state':{}}
    run={'id':3,'preview':False,'revision':1,'expires_at':datetime.now(timezone.utc)+timedelta(seconds=60),
         'result':{'items':[decision(target_weight=.4),decision(symbol='TSLA',target_weight=.4)]}}
    monkeypatch.setattr(store,'query',lambda *_a,**_k:{'id':4,'paper_only':True})
    monkeypatch.setattr(store,'cancelled',lambda _:False)
    monkeypatch.setattr(store,'receipts',lambda _:[])
    monkeypatch.setattr(store,'task',lambda *_:row)
    monkeypatch.setattr(store,'owned_quantities',lambda _:{})
    updates=[]
    monkeypatch.setattr(store,'update_run',lambda _id,**fields:updates.append(fields))
    # Simulate a lagging broker snapshot: it never includes either new fill.
    monkeypatch.setattr(runner,'account_snapshot',lambda *_:{'funds':{'cash':1000,'power':1000},
        'positions':[{'symbol':'QQQ','side':'long','quantity':4,'marketValue':400}], 'open_orders':[]})
    client=MagicMock()
    client.get_simulate_execution_quote.return_value={'simulate_execution_eligible':True,'price':100}
    monkeypatch.setattr('app.services.futu_agent_execution._load_client',lambda *_:client)
    orders=[]
    def submit(_user,_token,order,_key):
        orders.append(order)
        return {'id':len(orders)}
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent',submit)
    monkeypatch.setattr('app.services.futu_agent_execution.execute_simulate_intent',lambda *_a,**_k:{'status':'FILLED'})
    runner.execute(row,run)
    assert [o['qty'] for o in orders]==[4,1]
    assert sum(o['qty']*o['limit_price'] for o in orders)+400 <= 900
    assert updates[-1]['status']=='completed'


@pytest.mark.parametrize('cash,power,maximum,expected', [
    (10000, 0, 1000, 200), (10000, None, 1000, 200),
    (999, 0, 1000, 0), (10000, 0, 150, 100),
    (10000, 0, 0, 0), (10000, 1000, 1000, 100),
])
def test_hk_zero_power_sizing_preserves_cash_notional_and_broker_limits(cash, power, maximum, expected):
    cfg = config(market='HKStock', symbols=['01810'], budget=10000, max_order_notional=2000)
    item = decision(symbol='01810.HK', target_weight=.5, min_price=1, max_price=100)
    order = domain.size_order(item, cfg, price=10, lot=100, owned_qty=0, broker_qty=0,
        power=domain.available_buy_cash({'cash': cash, 'power': power}, 'HKStock'),
        open_symbols=set(), max_cash_buy=maximum)
    assert (order['qty'] if order else 0) == expected


@pytest.mark.parametrize('power,expected', [(0, 0), (None, 0), (500, 500), (2000, 1000)])
def test_us_cash_power_behavior_unchanged(power, expected):
    assert domain.available_buy_cash({'cash': 1000, 'power': power}, 'USStock') == expected


@pytest.mark.parametrize('maximum,pending,expected', [
    (1000, 0, [400, 100]), (150, 0, [100, 100]), (0, 0, []),
    (1000, 100, [400]),
])
def test_hk_run_preserves_reserve_exposure_and_lagging_cash(monkeypatch, maximum, pending, expected):
    from app.services.automation import runner
    cfg=config(market='HKStock',symbols=['01810','00700'],budget=100000,max_order_notional=50000,max_daily_notional=100000,execution_mode='paper_auto',max_weight=.5)
    row={'id':1,'token_id':4,'user_id':1,'config':cfg,'revision':1,'state':{}}
    run={'id':3,'preview':False,'revision':1,'expires_at':datetime.now(timezone.utc)+timedelta(seconds=60),
         'result':{'items':[decision(symbol='01810.HK',target_weight=.4),decision(symbol='00700.HK',target_weight=.4)]}}
    monkeypatch.setattr(store,'query',lambda *_a,**_k:{'id':4,'paper_only':True})
    monkeypatch.setattr(store,'cancelled',lambda _:False)
    monkeypatch.setattr(store,'receipts',lambda _:[])
    monkeypatch.setattr(store,'task',lambda *_:row)
    monkeypatch.setattr(store,'owned_quantities',lambda _:{})
    updates=[]
    monkeypatch.setattr(store,'update_run',lambda _id,**fields:updates.append(fields))
    # Simulate a lagging broker snapshot: it never includes either new fill.
    monkeypatch.setattr(runner,'account_snapshot',lambda *_:{'funds':{'cash':100000,'power':0},
        'positions':[{'symbol':'QQQ','side':'long','quantity':4,'marketValue':40000}], 'open_orders':[{'symbol':'00883.HK','order_id':'pending','side':'buy',
            'qty':pending,'filled':0,'price':100}] if pending else []})
    client=MagicMock()
    client.get_lot_size.return_value=100
    client.get_max_cash_buy.return_value=maximum
    client.get_simulate_execution_quote.return_value={'simulate_execution_eligible':True,'price':100}
    monkeypatch.setattr('app.services.futu_agent_execution._load_client',lambda *_:client)
    orders=[]
    def submit(_user,_token,order,_key):
        orders.append(order)
        return {'id':len(orders)}
    monkeypatch.setattr('app.services.agent_trade_intents.submit_intent',submit)
    monkeypatch.setattr('app.services.futu_agent_execution.execute_simulate_intent',lambda *_a,**_k:{'status':'FILLED'})
    runner.execute(row,run)
    assert [o['qty'] for o in orders]==expected
    assert sum(o['qty']*o['limit_price'] for o in orders)+40000+pending*100 <= 90000
    assert updates[-1]['status']=='completed'

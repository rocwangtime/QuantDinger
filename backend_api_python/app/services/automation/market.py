"""Broker snapshots and a dedicated push subscription, separate from execution."""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime, timedelta, timezone

from app.services.automation.domain import symbol
from app.services.futu_agent_execution import _load_client


def account_snapshot(user_id, config):
    client = _load_client(user_id, f"credential:{config['credential_id']}")
    try:
        if not client.connect(need_quote=False):
            raise ValueError('OpenD is not connected')
        expected = 'US' if config['market'] == 'USStock' else 'HK'
        if client.config.trade_market != expected:
            raise ValueError('Selected SIMULATE account does not match the task market')
        account = client.get_account_summary()
        if not account.get('success'):
            raise ValueError('Unable to read broker buying power')
        positions = client.get_positions()
        orders = client.get_open_orders(strict=True)
        return {'as_of':time.time(), 'currency':account['summary']['currency'],
                'funds':account['summary'],
                'positions':[{k:p.get(k) for k in ('symbol','quantity','side','avg_cost','marketValue','unrealized_pl')}
                             for p in positions],
                'open_orders':[{'symbol':symbol(o['symbol'],config['market']),
                                'order_id':o.get('orderId'),'side':o.get('action'),
                                'qty':o.get('quantity'),'price':o.get('limitPrice'),
                                'filled':o.get('filled'),'status':o.get('status')} for o in orders]}
    finally:
        client.disconnect()


def build_evidence(row, *, news=True, cancelled=lambda:False):
    from app.services.futu_trading.quote_client import FutuQuoteClient
    from app.services.futu_trading.execution_quote import describe_futu_quote
    from app.services.futu_trading.config import config_from_exchange_config
    from app.services.exchange_execution import resolve_exchange_config
    from app.services.research_workflow import market_clock
    config = row['config']
    account = account_snapshot(row['user_id'], config)
    codes = list(dict.fromkeys(config['symbols'] + ([] if config['kind']=='price_trigger' else [p['symbol'] for p in account['positions'] if p['quantity']>0])))
    if len(codes)>40:
        raise ValueError('Account has too many holdings for this task; use a dedicated simulated account')
    cfg = resolve_exchange_config({'credential_id':config['credential_id']},user_id=row['user_id'])
    client = FutuQuoteClient(config_from_exchange_config(cfg))
    instruments = {}
    try:
        if not client.connect():
            raise ValueError('OpenD quote connection unavailable')
        for code in codes:
            if cancelled():
                raise TimeoutError('Research cancelled or expired')
            quote = client.get_quote(code, config['market'])
            provenance = describe_futu_quote(code,quote,market_type=config['market'])
            price = float(provenance.get('price') or 0)
            if not math.isfinite(price) or price<=0 or provenance.get('as_of') is None:
                raise ValueError('Broker quote has no usable price or timestamp')
            start = (datetime.now(timezone.utc)-timedelta(days=120)).date().isoformat()
            bars = client.get_history_kline(code,market_type=config['market'],start=start,max_count=120)
            # Prior calendar days only: avoid using a partially formed daily bar.
            from zoneinfo import ZoneInfo
            zone = ZoneInfo('America/New_York' if config['market']=='USStock' else 'Asia/Hong_Kong')
            today = datetime.now(zone).date()
            bars = [b for b in bars if datetime.fromtimestamp(b['time'],zone).date()<today][-60:]
            item = {'price':price,'quote_as_of':provenance['as_of'],'source':'futu',
                    'daily_bars':bars,'news':[],'news_status':'not_requested'}
            if config['kind']=='price_trigger':
                recent = client.get_history_kline(code,market_type=config['market'],ktype='K_1M',
                    start=(today-timedelta(days=3)).isoformat(),max_count=3000)
                item['minute_bars'] = [b for b in recent if b['time'] < time.time()-60][-60:]
            if news:
                from app.services.search import get_search_service
                response = get_search_service().search_stock_news(code,code,config['market'],max_results=3)
                item['news'] = response.to_list() if response.success else []
                item['news_status'] = 'available' if item['news'] else 'unavailable'
                item['news_checked_at'] = time.time()
            instruments[code] = item
    finally:
        client.disconnect()
    return {'as_of':time.time(),'account':account,'instruments':instruments,
            'market_clock':market_clock(config['market']), 'brief':config['brief']}


class PushFeed:
    """Own the context so installing handlers cannot replace another consumer."""
    def __init__(self, row):
        self.row = row
        self.quote = None
        self.context = None
        self.lock = threading.Lock()
        self.closed = False
        self.created_at = time.time()

    def start(self):
        from app.services.exchange_execution import resolve_exchange_config
        from app.services.futu_trading.config import config_from_exchange_config
        from app.services.futu_trading.client import _ensure_futu
        from app.services.futu_trading.symbols import to_futu_code
        from app.services.futu_trading.timezones import futu_time_key_to_timestamp
        ft = _ensure_futu()
        config = self.row['config']
        cfg = config_from_exchange_config(resolve_exchange_config(
            {'credential_id':config['credential_id']},user_id=self.row['user_id']))
        code = to_futu_code(config['symbols'][0], config['market'])
        feed = self

        class Handler(ft.StockQuoteHandlerBase):
            def on_recv_rsp(self, response):
                ret, data = super().on_recv_rsp(response)
                if ret == ft.RET_OK:
                    for row in data.to_dict('records'):
                        if row.get('code') != code or feed.closed:
                            continue
                        try:
                            stamp = futu_time_key_to_timestamp(
                                str(row['data_date'])+' '+str(row['data_time']),config['market'])
                            price = float(row['last_price'])
                            if not math.isfinite(price) or price<=0:
                                continue
                            with feed.lock:
                                feed.quote = {'price':price,'as_of':stamp,'received_at':time.time(),
                                              'is_realtime':True,'source':'futu_subscribed_push',
                                              'execution_eligible':False}
                        except (KeyError,ValueError,TypeError):
                            continue
                return ret, data

        self.context = ft.OpenQuoteContext(host=cfg.host,port=cfg.port)
        try:
            self.context.set_handler(Handler())
            ret, _ = self.context.subscribe([code],[ft.SubType.QUOTE],subscribe_push=True)
            if ret != ft.RET_OK:
                raise ValueError('Futu real-time quote subscription failed')
        except Exception:
            self.stop()
            raise

    def snapshot(self):
        with self.lock:
            return dict(self.quote or {})

    def stop(self):
        self.closed = True
        if self.context:
            self.context.close()
            self.context = None

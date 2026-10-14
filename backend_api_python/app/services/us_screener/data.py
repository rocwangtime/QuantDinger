"""Allowlisted Futu quote context only. No trading context or credentials."""
import os
import json
import time
from datetime import timedelta
from app.services.us_screener.core import completed_bars
from app.services.us_screener import store


class FutuData:
    def __init__(self, deadline):
        from futu import OpenQuoteContext
        from app.services.futu_trading.config import config_from_exchange_config, validate_opend_host
        config = config_from_exchange_config({
            "futu_host": os.getenv("FUTU_OPEND_HOST"),
            "futu_port": os.getenv("FUTU_OPEND_PORT"),
            "market_category": "USStock",
        })
        self.deadline = deadline
        self.last_call = 0.0
        self.context = OpenQuoteContext(host=validate_opend_host(config.host), port=config.port)

        self.context.set_sync_query_connect_timeout(10)

    def close(self):
        self.context.close()

    def call(self, method, *args, **kwargs):
        if method not in {'get_market_snapshot', 'request_history_kline'}:
            raise ValueError('read_only_api_required')
        wait = max(0, 2.1-(time.monotonic()-self.last_call))
        if time.monotonic()+wait >= self.deadline:
            raise TimeoutError('runtime_budget')
        time.sleep(wait)
        self.last_call = time.monotonic()
        result = getattr(self.context, method)(*args, **kwargs)
        if result[0] != 0:
            raise ValueError('futu_market_data_unavailable')
        return result

    def quotes(self, symbols):
        cached = store.query("SELECT symbol,quote FROM qd_us_screener_quote_cache WHERE symbol=ANY(%s) AND observed_at>NOW()-INTERVAL '60 seconds'", (symbols,))
        result = {r['symbol']: r['quote'] for r in cached}
        errors = {}
        missing = [s for s in symbols if s not in result]
        for i in range(0, len(missing), 50):
            batch = missing[i:i+50]
            try:
                raw = self.call('get_market_snapshot', ['US.'+s for s in batch])[1]
                for r in raw.to_dict('records'):
                    symbol = r['code'].removeprefix('US.')
                    # Avoid NaN/numpy values in JSON persistence.
                    quote = {key: str(r.get(key, '')) for key in ('last_price', 'bid_price', 'ask_price', 'update_time')}
                    result[symbol] = quote
                    store.query('INSERT INTO qd_us_screener_quote_cache(symbol,quote) VALUES (%s,%s) ON CONFLICT(symbol) DO UPDATE SET quote=EXCLUDED.quote,observed_at=NOW()', (symbol, json.dumps(quote)))
            except Exception:
                errors.update({s: 'quote_batch_failed' for s in batch})
        return result, errors

    def bars(self, symbol, as_of):
        cached = store.query('SELECT bars FROM qd_us_screener_bar_cache WHERE symbol=%s AND as_of=%s', (symbol, as_of), one=True)
        if cached:
            return cached['bars']
        from futu import KLType, AuType
        raw = self.call('request_history_kline', 'US.'+symbol, start=str(as_of-timedelta(days=420)),
                        end=str(as_of), ktype=KLType.K_DAY, autype=AuType.QFQ, max_count=300)[1]
        bars = completed_bars(raw.to_dict('records'), as_of)
        # Store only JSON-native fields needed by the engine.
        bars = [{k: b[k] for k in ('time_key', 'close', 'high', 'low', 'volume')} for b in bars]
        store.query('INSERT INTO qd_us_screener_bar_cache(symbol,as_of,bars) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',
                    (symbol, as_of, json.dumps(bars)))
        return bars

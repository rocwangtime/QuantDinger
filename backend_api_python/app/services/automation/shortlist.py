"""Resolve a run-local US research universe; never mutate a task."""
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.services.automation import store
from app.services.automation.domain import symbol, validate_universe_source


def resolve(config, now):
    validate_universe_source(config)
    day = now.astimezone(ZoneInfo('America/New_York')).date()
    run = store.query("""SELECT id,as_of FROM qd_us_screener_runs
        WHERE status='completed' AND as_of<=%s AND as_of>=%s
        AND finished_at<=%s ORDER BY as_of DESC,finished_at DESC,id DESC LIMIT 1""",
        (day, day-timedelta(days=config.get('universe_max_age_days', 4)), now), one=True)
    if not run:
        return None
    rows = store.query("""SELECT symbol FROM qd_us_screener_symbols WHERE run_id=%s
        AND included=TRUE AND exclusion_reasons='[]'::jsonb ORDER BY rank NULLS LAST,symbol LIMIT %s""",
        (run['id'], config.get('top_n', 10)))
    symbols = list(dict.fromkeys(symbol(r['symbol'], 'USStock') for r in rows))
    return {'run_id': run['id'], 'as_of': str(run['as_of']), 'symbols': symbols} if symbols else None


def build_evidence(row, *, news=True, cancelled=lambda: False):
    """Quote-only research with a hypothetical cash budget, never a trade context."""
    import math
    import time
    from datetime import datetime
    from app.services.us_screener.data import FutuData
    from app.services.us_screener.service import latest_closed_day
    from app.services.research_workflow import market_clock
    config = row['config']
    validate_universe_source(config)
    provider = FutuData(time.monotonic()+100)
    instruments = {}
    try:
        quotes, errors = provider.quotes(config['symbols'])
        day = latest_closed_day(datetime.now(ZoneInfo('America/New_York')))
        for code in config['symbols']:
            if cancelled():
                raise TimeoutError('Research cancelled or expired')
            if code in errors or code not in quotes:
                raise ValueError('Screener research quote unavailable')
            quote = quotes[code]
            price = float(quote['last_price'])
            stamp = datetime.fromisoformat(quote['update_time']).replace(tzinfo=ZoneInfo('America/New_York')).timestamp()
            if not math.isfinite(price) or price <= 0 or stamp > time.time()+5:
                raise ValueError('Screener research quote invalid')
            item = {'price': price, 'quote_as_of': stamp, 'source': 'futu_read_only_snapshot',
                    'lot_size': 1, 'daily_bars': provider.bars(code, day)[-60:],
                    'news': [], 'news_status': 'not_requested'}
            if news:
                from app.services.search import get_search_service
                response = get_search_service().search_stock_news(code, code, 'USStock', max_results=3)
                item['news'] = response.to_list() if response.success else []
                item['news_status'] = 'available' if item['news'] else 'unavailable'
                item['news_checked_at'] = time.time()
            instruments[code] = item
    finally:
        provider.close()
    return {'as_of': time.time(), 'account': {'as_of': time.time(), 'currency': 'USD',
            'funds': {'cash': config['budget'], 'power': config['budget']},
            'positions': [], 'open_orders': [],
            'basis': 'Hypothetical task cash budget; broker account/holdings are not read; no executable permission'},
            'instruments': instruments, 'market_clock': market_clock('USStock'), 'brief': config['brief']}

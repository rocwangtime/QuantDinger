"""Bounded regular-session read-only quote capture, once per exchange session."""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from app.services.us_screener import store
from app.services.us_screener.core import capture_session, regular_spread


def enabled():
    return any(os.getenv(key, 'false').lower() == 'true' for key in
               ('US_SCREENER_ENABLED', 'US_SCREENER_RTH_CAPTURE_ENABLED'))


def capture(now=None, scheduled=True, provider_factory=None):
    if not enabled():
        return {'reason': 'capture_disabled'}
    now = now or datetime.now(timezone.utc)
    day = capture_session(now, scheduled=scheduled)
    if day is None:
        return {'reason': 'outside_capture_window'}
    claim = store.query("INSERT INTO qd_us_screener_quote_captures(session_date,status) VALUES (%s,'running') ON CONFLICT DO NOTHING RETURNING session_date", (day,), one=True)
    if not claim:
        return {'reason': 'already_captured'}
    provider = None
    started = time.monotonic()
    try:
        from app.services.universe import UniverseService
        from app.services.us_screener.service import parameters
        from app.services.us_screener.data import FutuData
        universe = store.query("SELECT id FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE", one=True)
        members = UniverseService().resolve_members(0, universe['id'], as_of=day) if universe else []
        symbols = sorted(set(m['symbol'] for m in members)) or parameters()['manual_fallback']
        if not symbols or len(symbols) > 200:
            raise ValueError('empty_or_oversized_universe')
        provider = (provider_factory or FutuData)(time.monotonic()+75)
        quotes, errors = provider.quotes(symbols)
        count = 0
        for symbol in symbols:
            quote = quotes.get(symbol, {})
            try:
                # Reject extended-hours, previous-session, crossed and invalid quotes.
                _, source = regular_spread([], quote, day)
                stamp = datetime.fromisoformat(str(quote['update_time']))
                stamp = stamp.replace(tzinfo=ZoneInfo('America/New_York')) if stamp.tzinfo is None else stamp
                if source != 'rth_snapshot' or not 0 <= (now+timedelta(seconds=time.monotonic()-started)-stamp).total_seconds() <= 120:
                    continue
            except (ValueError, KeyError, TypeError):
                continue
            store.query('INSERT INTO qd_us_screener_quote_cache(symbol,session_date,quote) VALUES (%s,%s,%s) ON CONFLICT(symbol,session_date) DO UPDATE SET quote=EXCLUDED.quote,observed_at=NOW()', (symbol, day, json.dumps(quote)))
            count += 1
        status = 'completed' if count == len(symbols) and not errors else 'partial'
        store.query('UPDATE qd_us_screener_quote_captures SET status=%s,finished_at=NOW() WHERE session_date=%s', (status, day))
        return {'session_date': str(day), 'captured': count, 'status': status}
    except Exception:
        store.query("UPDATE qd_us_screener_quote_captures SET status='failed',error='capture_failed',finished_at=NOW() WHERE session_date=%s", (day,))
        raise
    finally:
        if provider:
            provider.close()

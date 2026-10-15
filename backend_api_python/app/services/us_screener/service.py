"""Independent daily research shortlist; no agent/task/execution integration."""
import os
import re
import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from app.services.us_screener import store
from app.services.us_screener.core import calendar, evaluate, rank, regime, scheduled_day


def parameters():
    spread_policy = os.getenv('US_SCREENER_UNVERIFIED_SPREAD', 'flag')
    if spread_policy not in {'flag', 'exclude'}:
        raise ValueError('invalid_unverified_spread_policy')
    policy = os.getenv('US_SCREENER_EARNINGS_UNKNOWN', 'flag')
    if policy not in {'flag', 'exclude', 'open', 'closed'}:
        raise ValueError('invalid_earnings_policy')
    min_coverage = float(os.getenv('US_SCREENER_MIN_COVERAGE', '0.9'))
    if not 0 < min_coverage <= 1:
        raise ValueError('invalid_min_coverage')
    symbols = sorted(set(s.strip().upper() for s in os.getenv('US_SCREENER_MANUAL_SYMBOLS', '').split(',') if s.strip()))
    if len(symbols) > 200 or any(not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', s) for s in symbols):
        raise ValueError('invalid_manual_symbols')
    return {'unverified_spread': spread_policy, 'min_coverage': min_coverage, 'universe': 'nasdaq100', 'manual_fallback': symbols, 'top_n': 10,
            'min_price': 5, 'min_dollar_volume': 50_000_000, 'max_spread_bps': 10,
            'earnings_window': 3, 'earnings_unknown': policy, 'quote_max_age_sec': 3600,
            'runtime_seconds': 480, 'score_version': '0.4*z(m20)+0.4*z(m60)-0.2*z(vol20)',
            'mode': 'research_only'}


def latest_closed_day(now):
    cal = calendar()
    session = cal.date_to_session(str(now.astimezone(ZoneInfo('America/New_York')).date()), direction='previous')
    if now < cal.session_close(session).to_pydatetime():
        session = cal.previous_session(session)
    return session.date()


def enqueue(scheduled=False, now=None):
    now = now or datetime.now(timezone.utc)
    day = scheduled_day(now) if scheduled else latest_closed_day(now)
    if day is None:
        return None
    params = parameters()
    key = 'daily:'+str(day) if scheduled else 'manual:'+str(uuid.uuid4())
    row = store.create(day, params, key)
    if row:
        from app.tasks.us_screener import run_screener
        try:
            run_screener.delay(row['id'])
        except Exception:
            store.finish(row['id'], [], {'flag': 'unknown'}, 'failed', 'queue_unavailable')
            raise
    return row


def execute(run_id, provider_factory=None):
    run = store.read(run_id)
    if not run or run['status'] != 'queued':
        return
    claimed = store.query("UPDATE qd_us_screener_runs SET status='running' WHERE id=%s AND status='queued' RETURNING id", (run_id,), one=True)
    if not claimed:
        return
    from app.services.universe import UniverseService
    from app.services.fundamental_data import screener_earnings_dates
    from app.services.us_screener.data import FutuData
    rows, provider = [], None
    market_regime = {'flag': 'unknown'}
    params, as_of = run['params'], run['as_of']
    deadline = time.monotonic()+params['runtime_seconds']
    try:
        universe = store.query("SELECT id,metadata_json FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE", one=True)
        members = UniverseService().resolve_members(0, universe['id'], as_of=as_of) if universe else []
        symbols = sorted(set(m['symbol'] for m in members))
        market_regime['universe_metadata'] = universe['metadata_json'] if universe else {}
        if not symbols and universe:
            coverage = store.query(
                'SELECT MIN(valid_from) AS first_valid_from FROM qd_universe_members WHERE universe_id=%s',
                (universe['id'],), one=True)
            first_valid_from = coverage['first_valid_from'] if coverage else None
            if first_valid_from and str(as_of) < str(first_valid_from):
                market_regime['universe_first_valid_from'] = str(first_valid_from)
                market_regime['universe_resolution_reason'] = 'as_of_precedes_universe_members'
                if not params['manual_fallback']:
                    raise ValueError('as_of_precedes_universe_members')
        if not symbols:
            symbols = params['manual_fallback']
            market_regime['universe_fallback'] = True
        if not symbols or len(symbols) > 200:
            raise ValueError('empty_or_oversized_universe')
        rows = [{'symbol': s, 'features': {'earnings_status': 'unknown'}, 'score': None, 'rank': None, 'included': False,
                 'exclusion_reasons': ['not_processed'], 'flags': ['earnings_unknown']} for s in symbols]
        # Persist pending symbol coverage before any network call.
        store.finish(run_id, rows, market_regime, 'running')
        provider = (provider_factory or FutuData)(deadline)
        quotes, errors = provider.quotes(symbols, as_of=as_of)
        try:
            market_regime.update(regime(provider.bars('QQQ', as_of)))
        except Exception:
            market_regime.update(flag='unknown', reason='qqq_data_failed')
        for i, symbol in enumerate(symbols):
            try:
                if time.monotonic() >= deadline:
                    raise TimeoutError('runtime_budget')
                if symbol in errors:
                    raise ValueError(errors[symbol])
                bars = provider.bars(symbol, as_of)
                dates = screener_earnings_dates(symbol, as_of)
                rows[i] = evaluate(symbol, bars, quotes.get(symbol, {}), dates, as_of, params)
            except Exception as exc:
                rows[i]['features'] = {'earnings_status': 'unknown'}
                rows[i]['flags'] = ['earnings_unknown']
                rows[i]['features']['failure_detail'] = str(exc)[:160]
                rows[i]['exclusion_reasons'] = ['runtime_budget' if isinstance(exc, TimeoutError) else 'market_data_failed']
        rank(rows, params['top_n'])
        failed = sum(any(r in {'market_data_failed', 'runtime_budget', 'invalid_or_missing_market_data', 'invalid_bars', 'insufficient_bars', 'invalid_quote', 'stale_quote'} for r in row['exclusion_reasons']) for row in rows)
        coverage = (len(rows)-failed)/len(rows)
        market_regime.update(coverage_ratio=coverage, failed_count=failed)
        status = 'partial' if coverage < params.get('min_coverage', 0.9) else 'completed'
        error = ''
        if not any(row['included'] for row in rows):
            status, error = 'partial', 'no_eligible_symbols'
            market_regime['reason'] = error
        store.finish(run_id, rows, market_regime, status, error)
    except Exception as exc:
        store.finish(run_id, rows, market_regime, 'failed', str(exc)[:160] if isinstance(exc, ValueError) else 'screener_failed')
    finally:
        if provider:
            provider.close()

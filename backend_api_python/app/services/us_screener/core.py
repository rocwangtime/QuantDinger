"""Pure research mathematics. No execution dependencies."""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import math
import statistics

import exchange_calendars as xcals
import pandas as pd


def calendar():
    return xcals.get_calendar('XNYS')


def scheduled_day(now):
    local = now.astimezone(ZoneInfo('America/New_York'))
    if local.time() < time(16, 30) or not calendar().is_session(str(local.date())):
        return None
    return local.date()


def earnings_reasons(dates, as_of, window=3, policy='flag'):
    cal = calendar()
    start = cal.date_to_session(str(as_of), direction='previous')
    beginning = cal.session_offset(start, -window).date()
    end = cal.session_offset(start, window).date()
    if any(beginning <= d <= end for d in dates):
        return (['earnings_blackout'], [])
    upcoming = [d for d in dates if d >= as_of]
    if not upcoming:
        return (['earnings_unknown'] if policy in {'closed', 'exclude'} else [], ['earnings_unknown'])
    return ([], [])


def features(bars):
    if len(bars) < 61:
        raise ValueError('insufficient_bars')
    c = [float(b['close']) for b in bars]
    for b in bars:
        vals = [float(b[k]) for k in ('close', 'high', 'low', 'volume')]
        if not all(math.isfinite(v) for v in vals) or min(vals[:3]) <= 0 or vals[3] < 0 or vals[1] < vals[2]:
            raise ValueError('invalid_bars')
    returns = [math.log(c[i] / c[i-1]) for i in range(len(c)-20, len(c))]
    tr = [max(float(bars[i]['high'])-float(bars[i]['low']),
              abs(float(bars[i]['high'])-c[i-1]), abs(float(bars[i]['low'])-c[i-1]))
          for i in range(len(c)-14, len(c))]
    return {'momentum20': c[-1]/c[-21]-1, 'momentum60': c[-1]/c[-61]-1,
            'volatility20': statistics.stdev(returns)*math.sqrt(252),
            'atr14_pct': statistics.mean(tr)/c[-1]*100,
            'distance_ma50': (c[-1]/statistics.mean(c[-50:])-1)*100,
            'avg_dollar_volume20': statistics.mean(float(b['close'])*float(b['volume']) for b in bars[-20:])}


def regime(bars):
    if len(bars) < 200:
        return {'flag': 'unknown', 'reason': 'insufficient_qqq_bars'}
    c = [float(b['close']) for b in bars]
    if not all(math.isfinite(v) and v > 0 for v in c):
        return {'flag': 'unknown', 'reason': 'invalid_qqq_bars'}
    ma50, ma200 = statistics.mean(c[-50:]), statistics.mean(c[-200:])
    return {'flag': 'risk_on' if c[-1] > ma50 > ma200 else 'risk_off',
            'close': c[-1], 'ma50': ma50, 'ma200': ma200}


def regular_spread(bars, quote, as_of):
    """RTH snapshot, otherwise Corwin-Schultz (2012) two-day high/low estimate.

    Average the last 20 adjacent daily estimates, floor negative alpha at zero.
    Daily ranges contain volatility; the estimator removes its two-day component.
    This is an estimate, not an observed executable quote.
    """
    try:
        stamp = datetime.fromisoformat(str(quote['update_time']))
        stamp = stamp.replace(tzinfo=ZoneInfo('America/New_York')) if stamp.tzinfo is None else stamp
        session = str(as_of)
        cal = calendar()
        bid, ask = float(quote['bid_price']), float(quote['ask_price'])
        if (stamp.date() == as_of and cal.is_session(session)
                and cal.session_open(session).to_pydatetime() <= stamp <= cal.session_close(session).to_pydatetime()
                and all(math.isfinite(v) and v > 0 for v in (bid, ask)) and ask >= bid):
            return (ask-bid)/((ask+bid)/2)*10000, 'rth_snapshot'
    except (ValueError, KeyError, TypeError):
        pass
    estimates = []
    denominator = 3-2*math.sqrt(2)
    for left, right in zip(bars[-21:-1], bars[-20:]):
        h1, l1, h2, l2 = (float(left['high']), float(left['low']), float(right['high']), float(right['low']))
        beta = math.log(h1/l1)**2 + math.log(h2/l2)**2
        gamma = math.log(max(h1,h2)/min(l1,l2))**2
        alpha = max(0, (math.sqrt(2*beta)-math.sqrt(beta))/denominator-math.sqrt(gamma/denominator))
        estimates.append(2*math.tanh(alpha/2)*10000)
    return statistics.mean(estimates), 'corwin_schultz_daily'


def evaluate(symbol, bars, quote, dates, as_of, params):
    row = {'symbol': symbol, 'features': {}, 'score': None, 'rank': None,
           'included': False, 'exclusion_reasons': [], 'flags': []}
    row['features']['earnings_status'] = 'known' if any(d >= as_of for d in dates) else 'unknown'
    if row['features']['earnings_status'] == 'unknown':
        row['flags'].append('earnings_unknown')
    try:
        row['features'].update(features(bars))
        price = float(bars[-1]['close'])
        spread, source = regular_spread(bars, quote, as_of)
        observed = source == 'rth_snapshot'
        row['features'].update(price=price, spread_bps=spread if observed else None, spread_source=source,
                               spread_estimate_bps=None if observed else spread)
        if not observed:
            row['flags'].append('spread_unverified')
            if params.get('unverified_spread', 'flag') == 'exclude':
                row['exclusion_reasons'].append('spread_unverified')
        for condition, reason in ((price < params['min_price'], 'price_below_min'),
                                  (row['features']['avg_dollar_volume20'] < params['min_dollar_volume'], 'illiquid'),
                                  (observed and spread > params['max_spread_bps'], 'spread_too_wide')):
            if condition:
                row['exclusion_reasons'].append(reason)
        reasons, flags = earnings_reasons(dates, as_of, params['earnings_window'], params['earnings_unknown'])
        row['exclusion_reasons'].extend(reasons)
        row['flags'] = sorted(set(row['flags'] + flags))
        row['features']['earnings_dates'] = [d.isoformat() for d in dates]
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        reason = str(exc)
        row['exclusion_reasons'].append(reason if reason in {'insufficient_bars', 'invalid_bars', 'invalid_quote', 'stale_quote'} else 'invalid_or_missing_market_data')
    return row


def rank(rows, top_n=10):
    eligible = [r for r in rows if not r['exclusion_reasons']]
    for r in eligible:
        r['score'] = 0.0
    for key, weight in [('momentum20', .4), ('momentum60', .4), ('volatility20', -.2)]:
        values = [r['features'][key] for r in eligible]
        mean = statistics.mean(values) if values else 0
        sd = statistics.pstdev(values) if values else 0
        for r in eligible:
            r['score'] += weight*((r['features'][key]-mean)/sd if sd else 0)
    for i, r in enumerate(sorted(eligible, key=lambda r: (-r['score'], r['symbol'])), 1):
        r['rank'] = i
        r['included'] = i <= top_n
        if not r['included']:
            r['exclusion_reasons'].append('below_top_n')
    return rows


def completed_bars(raw, as_of):
    frame = pd.DataFrame(raw)
    if frame.empty or 'time_key' not in frame:
        return []
    frame['day'] = pd.to_datetime(frame['time_key']).dt.date
    frame = frame[frame.day <= as_of].sort_values('day').drop_duplicates('day')
    if frame.empty or frame.iloc[-1]['day'] != as_of:
        raise ValueError('stale_daily_bars')
    if len(frame) >= 61:
        expected = set(calendar().sessions_in_range(str(frame.iloc[-61]['day']), str(as_of)).date)
        if set(frame.tail(61).day) != expected:
            raise ValueError('daily_bar_gaps')
    return frame.to_dict('records')


def capture_session(now, scheduled=True):
    local = now.astimezone(ZoneInfo('America/New_York'))
    cal = calendar()
    session = str(local.date())
    if not cal.is_session(session):
        return None
    opening = cal.session_open(session).to_pydatetime()
    closing = cal.session_close(session).to_pydatetime()
    start = closing-timedelta(minutes=30) if scheduled else opening
    end = closing-timedelta(minutes=2) if scheduled else closing
    return local.date() if start <= now < end else None

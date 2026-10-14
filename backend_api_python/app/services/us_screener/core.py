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
        return (['earnings_unknown'] if policy == 'closed' else [], ['earnings_unknown'])
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


def evaluate(symbol, bars, quote, dates, as_of, params):
    row = {'symbol': symbol, 'features': {}, 'score': None, 'rank': None,
           'included': False, 'exclusion_reasons': [], 'flags': []}
    try:
        row['features'] = features(bars)
        price, bid, ask = [float(quote[k]) for k in ('last_price', 'bid_price', 'ask_price')]
        if not all(math.isfinite(v) and v > 0 for v in (price, bid, ask)) or ask < bid:
            raise ValueError('invalid_quote')
        stamp = datetime.fromisoformat(str(quote['update_time'])).replace(tzinfo=ZoneInfo('America/New_York'))
        age = (datetime.now(ZoneInfo('America/New_York'))-stamp).total_seconds()
        if stamp.date() != as_of or age < -5 or age > params['quote_max_age_sec']:
            raise ValueError('stale_quote')
        spread = (ask-bid)/((ask+bid)/2)*10000
        row['features'].update(price=price, spread_bps=spread, quote_time=stamp.isoformat())
        for condition, reason in ((price < params['min_price'], 'price_below_min'),
                                  (row['features']['avg_dollar_volume20'] < params['min_dollar_volume'], 'illiquid'),
                                  (spread > params['max_spread_bps'], 'spread_too_wide')):
            if condition:
                row['exclusion_reasons'].append(reason)
        reasons, flags = earnings_reasons(dates, as_of, params['earnings_window'], params['earnings_unknown'])
        row['exclusion_reasons'].extend(reasons)
        row['flags'].extend(flags)
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

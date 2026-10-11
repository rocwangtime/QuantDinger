"""Bounded public crypto catalogue and research reference prices. No credentials."""
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

from .engine import _array, market_from_gamma, number

ASSETS = ('BTC', 'ETH', 'SOL', 'XRP')
INTERVALS = {'5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
EXPIRY_BUCKETS = ('>10m', '5-10m', '2-5m', '1-2m', '30-60s', '<30s')


def enabled():
    return os.getenv('POLYMARKET_OBSERVER_UPDOWN_ENABLED', 'false').lower() in {'1', 'true', 'yes'}


def setting(name, default, low, high):
    return max(low, min(high, int(os.getenv('POLYMARKET_OBSERVER_' + name, str(default)))))


def selections(name, defaults, allowed):
    return tuple(dict.fromkeys(x.strip() for x in os.getenv('POLYMARKET_OBSERVER_UPDOWN_' + name, ','.join(defaults)).split(',') if x.strip() in allowed))


def timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if result.tzinfo is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError):
        raise ValueError('polymarket.windowUnknown') from None


def expiry_bucket(market, now):
    if market.get('market_type') != 'up_down':
        return ''
    seconds = (timestamp(market['endDate']) - now).total_seconds()
    for boundary, label in ((600, '>10m'), (300, '5-10m'), (120, '2-5m'), (60, '1-2m'), (30, '30-60s')):
        if seconds >= boundary:
            return label
    return '<30s'


def market_from_updown(row, *, asset, interval, now=None):
    """Reuse standard binary constraints, explicitly verify Up/Down fee semantics."""
    now = now or datetime.now(timezone.utc)
    outcomes = _array(row.get('outcomes'))
    if outcomes not in (['Up', 'Down'], ['Down', 'Up']):
        raise ValueError('polymarket.upDownBinaryRequired')
    if asset not in ASSETS or interval not in INTERVALS:
        raise ValueError('polymarket.windowUnknown')
    config = row.get('cryptoMarketConfig')
    if config and (config.get('asset', '').upper() != asset or config.get('duration') != interval):
        raise ValueError('polymarket.windowMismatch')
    start, end = timestamp(row.get('eventStartTime')), timestamp(row.get('endDate'))
    if end <= now:
        raise ValueError('polymarket.marketClosed')
    # Daily windows can span 23/25 hours across US daylight-saving changes.
    duration = (end - start).total_seconds()
    if duration != INTERVALS[interval] and not (interval == '1d' and duration in (82800, 90000)):
        raise ValueError('polymarket.windowMismatch')
    schedule = row.get('feeSchedule')
    if row.get('feesEnabled') is not True or not isinstance(schedule, dict) or type(schedule.get('exponent')) is not int or schedule.get('exponent') != 1 or number(schedule.get('rate')) != number('0.07') or schedule.get('takerOnly') is not True:
        raise ValueError('polymarket.feeUnknown')
    if row.get('feeRate') is not None and number(row['feeRate']) != number(schedule['rate']):
        raise ValueError('polymarket.feeUnknown')
    if row.get('feeExponent') is not None and row['feeExponent'] != 1:
        raise ValueError('polymarket.feeUnknown')
    converted = {**row, 'outcomes': ['Yes' if x == 'Up' else 'No' for x in outcomes]}
    market = market_from_gamma(converted)
    strike = row.get('priceToBeat')
    if strike is not None:
        strike = number(strike)
        if strike <= 0:
            raise ValueError('polymarket.referenceInvalid')
        strike = str(strike)
    return {**market, 'market_type': 'up_down', 'asset': asset, 'interval': interval,
            'eventStartTime': start.isoformat(), 'resolutionSource': str(row.get('resolutionSource') or '')[:500],
            'price_to_beat': strike}


def discover(client, series_cache, *, now=None):
    now = now or datetime.now(timezone.utc)
    assets = selections('ASSETS', ASSETS, ASSETS)
    intervals = selections('INTERVALS', INTERVALS, INTERVALS)
    cap = setting('UPDOWN_MAX_MARKETS', 40, 1, 100)
    per_pair = setting('UPDOWN_PER_PAIR_LIMIT', 2, 1, 10)
    ahead = setting('UPDOWN_LOOKAHEAD_SECONDS', 1800, 0, 86400)
    selected, excluded, conditions = [], [], set()
    for asset in assets:
        for interval in intervals:
            slug = f"{asset.lower()}-up-or-down-{ {'1h': 'hourly', '1d': 'daily'}.get(interval, interval)}"
            try:
                if slug not in series_cache:
                    series = client._json('GET', 'https://gamma-api.polymarket.com/series', params={'slug': slug})
                    series_cache[slug] = next((str(s['id']) for s in series if s.get('slug') == slug), None)
                series_id = series_cache[slug]
                if not series_id:
                    continue
                events = client._json('GET', 'https://gamma-api.polymarket.com/events', params={
                    'series_id': series_id, 'active': 'true', 'closed': 'false', 'limit': min(100, per_pair + 5),
                    'order': 'endDate', 'ascending': 'true', 'end_date_min': now.isoformat(),
                    'end_date_max': (now + timedelta(seconds=ahead + INTERVALS[interval] + 3600)).isoformat()})
                pair = 0
                for event in events:
                    if event.get('seriesSlug') != slug:
                        continue
                    for row in event.get('markets', []):
                        try:
                            market = market_from_updown({**row, 'events': [event]}, asset=asset, interval=interval, now=now)
                            if timestamp(market['eventStartTime']) > now + timedelta(seconds=ahead):
                                continue
                            if market['conditionId'] in conditions:
                                continue
                            selected.append(market)
                            conditions.add(market['conditionId'])
                            pair += 1
                            if len(selected) >= cap:
                                return selected, excluded
                            if pair >= per_pair:
                                break
                        except (ValueError, KeyError, TypeError) as exc:
                            excluded.append({'marketId': str(row.get('id', '')), 'reason': str(exc)})
                    if pair >= per_pair:
                        break
            except (ValueError, TypeError, KeyError) as exc:
                excluded.append({'series': slug, 'reason': str(exc)})
    return selected, excluded[:100]


class ReferencePrices:
    """Shared per-asset cache; failed reads are cached too. Worker keeps HTTP off book callbacks."""
    def __init__(self, session=None, clock=time.monotonic):
        self.session = session or requests.Session()
        self.clock = clock
        self.cache = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None

    def fetch(self, asset):
        ttl = setting('REFERENCE_CACHE_SECONDS', 5, 2, 60)
        with self.lock:
            cached = self.cache.get(asset)
            if cached and self.clock() - cached[0] < ttl:
                return dict(cached[1])
        result = {'price': None, 'source': 'unavailable', 'observed_at': None}
        for host in ('api.binance.com', 'data-api.binance.vision'):
            try:
                response = self.session.get(f'https://{host}/api/v3/ticker/price', params={'symbol': asset + 'USDT'}, timeout=(1, 2), allow_redirects=False)
                response.raise_for_status()
                data = response.json()
                price = number(data['price'])
                if data.get('symbol') != asset + 'USDT' or price <= 0:
                    continue
                result = {'price': str(price), 'source': host, 'observed_at': datetime.now(timezone.utc).isoformat()}
                break
            except (requests.RequestException, ValueError, TypeError, KeyError):
                continue
        with self.lock:
            self.cache[asset] = (self.clock(), result)
        return dict(result)

    def get(self, asset):
        with self.lock:
            entry = self.cache.get(asset)
            if entry and self.clock() - entry[0] <= setting('REFERENCE_CACHE_SECONDS', 5, 2, 60) * 3:
                return dict(entry[1])
        return {'price': None, 'source': 'unavailable', 'observed_at': None}

    def start(self):
        def run():
            while not self.stop_event.is_set():
                for asset in selections('ASSETS', ASSETS, ASSETS):
                    if self.stop_event.is_set():
                        break
                    self.fetch(asset)
                self.stop_event.wait(setting('REFERENCE_CACHE_SECONDS', 5, 2, 60))
        self.thread = threading.Thread(target=run, name='polymarket-reference', daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(15)
        self.session.close()

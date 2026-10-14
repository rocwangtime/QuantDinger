"""Bounded timestamped book events. Receive work remains dictionary updates only."""
import copy
import os
from collections import deque


def max_pair_skew_ms():
    value = int(os.getenv('POLYMARKET_OBSERVER_MAX_PAIR_SKEW_MS', '250'))
    if value < 0:
        raise ValueError('POLYMARKET_OBSERVER_MAX_PAIR_SKEW_MS must be nonnegative')
    return value


def pair_rejection(exchange_ms, at, max_age=2000, max_skew=250):
    if len(exchange_ms) != 2 or any(x is None or not 0 <= at - x <= max_age for x in exchange_ms):
        return 'stale'
    if max(exchange_ms) - min(exchange_ms) > max_skew:
        return 'skew'
    return None


def coherent_pair(exchange_ms, at, max_age=2000, max_skew=250):
    return pair_rejection(exchange_ms, at, max_age, max_skew) is None


def apply(book, event):
    if event['kind'] == 'book':
        return {side: dict(event[side]) for side in ('asks', 'bids')}
    side, price, size = event['side'], event['price'], event['size']
    if float(size) <= 0:
        book[side].pop(price, None)
    else:
        book[side][price] = size
    return book


class BookHistory:
    def __init__(self, seconds=10, maximum=2000):
        self.seconds, self.maximum = seconds, maximum
        self.assets = {}

    def record(self, event, at):
        if isinstance(event, list):
            for item in event:
                self.record(item, at)
            return
        if not isinstance(event, dict):
            return
        from .recording import exchange_time
        payload = event.get('payload') or event
        exchange = exchange_time(payload.get('timestamp'))
        exchange_ms = exchange.timestamp() * 1000 if exchange else None
        records = []
        kind = event.get('event_type') or event.get('type')
        if kind == 'book':
            event = event.get('payload') or event
            records.append((str(event.get('asset_id') or event.get('assetId') or ''), dict(kind='book', **{
                side: {str(x['price']): str(x['size']) for x in event.get(side, [])} for side in ('asks', 'bids')})))
        elif kind == 'price_change':
            for item in event.get('price_changes') or (event.get('payload') or {}).get('priceChanges') or []:
                records.append((str(item.get('asset_id') or item.get('assetId') or ''), dict(kind='delta',
                    side='bids' if str(item.get('side', '')).upper() == 'BUY' else 'asks', price=str(item['price']), size=str(item['size']))))
        for asset, record in records:
            record['exchange_ms'] = exchange_ms
            state = self.assets.setdefault(asset, {'base': {'asks': {}, 'bids': {}}, 'base_at': None, 'events': deque()})
            if record['kind'] == 'delta' and state['base_at'] is None and not state['events']:
                continue
            state['events'].append((at, record))
            while len(state['events']) > self.maximum or state['events'][0][0] < at - self.seconds * 1000:
                popped_at, popped = state['events'].popleft()
                state['base'] = apply(state['base'], popped)
                state['base_at'] = popped_at
                state['base_exchange_ms'] = popped.get('exchange_ms')

    def record_rest(self, asset, book, at):
        """Adapt a public REST snapshot without replacing its exchange time."""
        self.record(dict(event_type='book', asset_id=asset,
                         timestamp=book.get('exchangeTimestamp'),
                         asks=book['asks'], bids=book['bids']), at)

    def copy_assets(self, assets):
        return {a: {'base': copy.deepcopy(self.assets[a]['base']),
                    'base_exchange_ms': self.assets[a].get('base_exchange_ms'),
                    'base_at': self.assets[a]['base_at'], 'events': list(self.assets[a]['events'])}
                for a in assets if a in self.assets}

    @staticmethod
    def lookup(states, asset, at, max_age):
        state = states.get(asset)
        if not state or state['base_at'] is not None and state['base_at'] > at:
            return None
        book, last = copy.deepcopy(state['base']), state['base_at']
        exchange_ms = state.get('base_exchange_ms')
        for observed, event in state['events']:
            if observed > at:
                break
            book, last = apply(book, event), observed
            exchange_ms = event.get('exchange_ms')
        if last is None or at - last > max_age:
            return None
        if exchange_ms is None or not 0 <= at - exchange_ms <= max_age:
            return None
        from .engine import number
        sides = {}
        try:
            for side in ('asks', 'bids'):
                levels = {}
                for raw_price, raw_size in book[side].items():
                    price, size = number(raw_price), number(raw_size)
                    if not 0 < price < 1 or size < 0 or price in levels:
                        return None
                    if size:
                        levels[price] = size
                sides[side] = [{'price': str(p), 'size': str(levels[p])}
                               for p in sorted(levels, reverse=side == 'bids')[:50]]
            if sides['asks'] and sides['bids'] and number(sides['bids'][0]['price']) >= number(sides['asks'][0]['price']):
                return None
        except ValueError:
            return None
        return dict(assetId=asset, observedMs=last, exchangeMs=exchange_ms, **sides)

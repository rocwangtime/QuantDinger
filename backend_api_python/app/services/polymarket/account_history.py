"""Bounded timestamped book events. Receive work remains dictionary updates only."""
import copy
import os
from collections import deque

from .ws_payloads import materialized_book


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
    def __init__(self, seconds=10, maximum=2000, byte_limit=16 * 1024 * 1024):
        self.seconds, self.maximum = seconds, maximum
        self.byte_limit = max(1024, byte_limit)
        self.charged_bytes = self.byte_evictions = self.idle_evictions = 0
        self.assets = {}

    @staticmethod
    def level_bytes(price, size):
        # Conservative charge for strings, dict capacity and allocator overhead.
        return 256 + 4 * (len(price) + len(size))

    def forget(self, asset):
        state = self.assets.pop(asset, None)
        if state:
            self.charged_bytes -= state['charged_bytes']

    def retain(self, assets):
        for asset in self.assets.keys() - set(assets):
            self.forget(asset)

    def pop(self, state):
        at, record = state['events'].popleft()
        before = state['base_bytes']
        if record['kind'] == 'book':
            state['base_bytes'] = record['_bytes'] - 512
        else:
            levels = state['base'][record['side']]
            price, size = record['price'], record['size']
            if price in levels:
                state['base_bytes'] -= self.level_bytes(price, levels[price])
            if float(size) > 0:
                state['base_bytes'] += self.level_bytes(price, size)
        state['base'] = apply(state['base'], record)
        state['base_at'], state['base_exchange_ms'] = at, record.get('exchange_ms')
        change = state['base_bytes'] - before - record['_bytes']
        state['charged_bytes'] += change
        self.charged_bytes += change

    def trim_bytes(self):
        while self.charged_bytes > self.byte_limit and self.assets:
            # Fold the oldest event into its timestamped base. Requests older
            # than that base fail closed rather than replaying incomplete data.
            asset, state = min(self.assets.items(), key=lambda item:
                item[1]['events'][0][0] if item[1]['events'] else item[1]['base_at'])
            if state['events']:
                self.pop(state)
            else:
                self.forget(asset)  # A single oversized anchor cannot evade the cap.
            self.byte_evictions += 1

    def prune(self, at):
        cutoff = at - self.seconds * 1000
        for asset, state in list(self.assets.items()):
            last = state['events'][-1][0] if state['events'] else state['base_at']
            if last is not None and last < cutoff:
                self.forget(asset)
                self.idle_evictions += 1
                continue
            while state['events'] and state['events'][0][0] < cutoff:
                self.pop(state)
        self.trim_bytes()

    def metrics(self):
        return dict(history_events=sum(len(s['events']) for s in self.assets.values()),
                    history_charged_bytes=self.charged_bytes, history_byte_limit=self.byte_limit,
                    history_byte_evictions=self.byte_evictions, history_idle_evictions=self.idle_evictions)

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
            levels, charge = materialized_book(event)
            records.append((str(event.get('asset_id') or event.get('assetId') or ''), dict(kind='book',
                _bytes=512 + charge, **{side: dict(rows) for side, rows in levels.items()})))
        elif kind == 'price_change':
            for item in event.get('price_changes') or (event.get('payload') or {}).get('priceChanges') or []:
                records.append((str(item.get('asset_id') or item.get('assetId') or ''), dict(kind='delta',
                    side='bids' if str(item.get('side', '')).upper() == 'BUY' else 'asks', price=str(item['price']), size=str(item['size']))))
        for asset, record in records:
            record['exchange_ms'] = exchange_ms
            if record['kind'] == 'delta' and asset not in self.assets:
                continue
            state = self.assets.setdefault(asset, {'base': {'asks': {}, 'bids': {}}, 'base_at': None,
                'base_bytes': 0, 'charged_bytes': 0, 'events': deque()})
            if record['kind'] == 'delta' and state['base_at'] is None and not state['events']:
                continue
            if record['kind'] != 'book':
                record['_bytes'] = 512 + self.level_bytes(record['price'], record['size'])
            state['events'].append((at, record))
            state['charged_bytes'] += record['_bytes']
            self.charged_bytes += record['_bytes']
            while state['events'] and (len(state['events']) > self.maximum
                    or state['events'][0][0] < at - self.seconds * 1000):
                self.pop(state)
            self.trim_bytes()

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

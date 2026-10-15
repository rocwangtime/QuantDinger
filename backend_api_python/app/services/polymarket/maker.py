"""Paper maker, owned by the existing account worker. No execution integrations."""
import copy
import os
import time
from uuid import uuid4
from decimal import Decimal, ROUND_DOWN

from .engine import ZERO, number
from .paper_account import day_key, utc
from .updown import timestamp

TAG = 'maker_complete_set_v1'
DEFAULTS = dict(combined_bid='.99', market_cap='20', global_cap='100', quote_shares='10',
                spot_bps='3', stop_seconds='30', mark_seconds='5', max_age_ms='2000')


def enabled():
    return os.getenv('POLYMARKET_PAPER_MAKER_ENABLED', 'false').lower() in {'1', 'true', 'yes'}


def configuration():
    config = {k: str(number(os.getenv('POLYMARKET_PAPER_MAKER_' + k.upper(), v))) for k, v in DEFAULTS.items()}
    if any(number(v) <= 0 for v in config.values()) or number(config['combined_bid']) >= 1:
        raise ValueError('invalid_maker_configuration')
    return config


def rebate(quantity, price):
    # Estimated only, never credited to spendable cash.
    return number(quantity) * Decimal('.07') * number(price) * (1 - number(price)) * Decimal('.20')


class Maker:
    def __init__(self, account, config=None):
        self.account = account
        self.config = config or configuration()
        self.state = account.state.setdefault('maker', dict(strategy=TAG, markets={}, days={}))

    def audit(self, event, market_id, now, detail):
        if not hasattr(self.account, 'maker_changed'):
            self.account.maker_changed = []
        self.account.maker_changed.append(dict(id=str(uuid4()), event=event, market_id=market_id,
            at=now, detail=copy.deepcopy(dict(detail, config=self.config, strategy=TAG))))

    def stats(self, now):
        return self.state['days'].setdefault(day_key(now), dict(strategy=TAG, quotes=0, cancels=0,
            quoted_up='0', quoted_down='0', filled_up='0', filled_down='0', paired_sets='0',
            set_pnl='0', net_pnl='0', adverse_selection_loss='0', unpaired_settlement_loss='0',
            estimated_rebate='0', rebate_estimated=True, inventory_peak='0', mark_samples=0,
            estimated_settlements=0, mark_missing=0, fill_events_up=0, fill_events_down=0))

    def add(self, now, key, value):
        s = self.stats(now)
        s[key] = str(number(s[key]) + value)

    def exposure(self, market=None):
        rows = [market] if market else self.state['markets'].values()
        return sum((abs(number(r['qty'][0]) - number(r['qty'][1])) for r in rows), ZERO)

    def cancel(self, row, now):
        if row.get('quotes'):
            self.account.transfer('locked', -number(row['reserved']))
            self.account.transfer('free', row['reserved'])
            row.update(quotes=[], reserved='0')
            self.stats(now)['cancels'] += 1
            self.account.event('maker_cancel', now)
            self.audit('cancel', row['market']['id'], now, {})

    def quote(self, market, books, spot, now):
        if market.get('market_type') != 'up_down' or market.get('asset') not in {'BTC', 'ETH'} or market.get('interval') not in {'5m', '15m'}:
            return
        if market['id'] not in self.state['markets'] and (len(self.state['markets']) >= 100 or
                now < timestamp(market['eventStartTime']).timestamp() * 1000 or
                now >= timestamp(market['endDate']).timestamp() * 1000 - float(self.config['stop_seconds']) * 1000):
            return
        row = self.state['markets'].setdefault(market['id'], dict(market=market, qty=['0', '0'], cost=['0', '0'],
            quotes=[], reserved='0', marks=[], settled=False))
        if row['settled']:
            return
        if len(row['marks']) >= 2000:
            self.cancel(row, now)
            return
        valid = (spot is not None and number(spot) > 0 and len(books) == 2 and all(
            b and b.get('bids') and b.get('asks') and b.get('exchange_at') is not None and
            0 <= now - b['exchange_at'] <= number(self.config['max_age_ms']) for b in books))
        if now < timestamp(market['eventStartTime']).timestamp() * 1000 or now >= timestamp(market['endDate']).timestamp() * 1000 - float(self.config['stop_seconds']) * 1000 or not valid or self.account.daily(now)['loss_stop']:
            self.cancel(row, now)
            return
        fingerprint = [[b['bids'][0]['price'], b['asks'][0]['price']] for b in books]
        if any(number(b['bids'][0]['price']) >= number(b['asks'][0]['price']) for b in books):
            self.cancel(row, now)
            return
        if row['quotes']:
            moved = abs(number(spot) / number(row['spot']) - 1) * 10000 >= number(self.config['spot_bps'])
            if fingerprint == row['book'] and not moved and not self.exposure(row) and all(number(q['remaining']) > 0 for q in row['quotes']):
                return
            self.cancel(row, now)
        tick = number(market['tickSize'])
        prices = [(number(b['bids'][0]['price']) / tick).to_integral_value(rounding=ROUND_DOWN) * tick for b in books]
        excess = sum(prices) - number(self.config['combined_bid'])
        if excess > 0:
            prices[prices.index(max(prices))] -= (excess / tick).to_integral_value(rounding='ROUND_UP') * tick
        if min(prices) <= 0 or any(p >= number(b['asks'][0]['price']) for p, b in zip(prices, books)):
            return
        # Reserve worst-case one-sided fills for BOTH caps, across every live quote.
        others = [r for r in self.state['markets'].values() if r is not row]
        outstanding = sum((max((number(q['remaining']) for q in r['quotes']), default=ZERO) for r in others), ZERO)
        imbalance = number(row['qty'][0]) - number(row['qty'][1])
        sizes = []
        for i in range(2):
            delta = imbalance if i == 0 else -imbalance
            # Pause the overfilled leg immediately; keep the pairing leg open,
            # including at/above the cap after a configuration reduction.
            room = (-delta if delta < 0 else
                    min(number(self.config['market_cap']),
                        number(self.config['global_cap']) - self.exposure() - outstanding))
            sizes.append(ZERO if delta > 0 else max(ZERO, min(number(self.config['quote_shares']), room)))
        reserve = sum(p * z for p, z in zip(prices, sizes))
        budget = min(Decimal('1000'), number(self.account.config['trade_cap']), number(self.account.state['free']))
        factor = min(Decimal(1), budget / reserve) if reserve else ZERO
        sizes = [(z * factor).quantize(Decimal('.01'), rounding=ROUND_DOWN) for z in sizes]
        reserve = sum(p * z for p, z in zip(prices, sizes))
        if reserve <= 0:
            return
        self.account.transfer('free', -reserve)
        self.account.transfer('locked', reserve)
        row.update(reserved=str(reserve), spot=str(spot), book=fingerprint, joined=now,
            quotes=[dict(price=str(p), remaining=str(size), ahead=str(sum((number(l['size']) for l in b['bids'] if number(l['price']) == p), ZERO)) if p >= min(number(l['price']) for l in b['bids']) else None, seen=[])
                    for p, size, b in zip(prices, sizes, books)])
        self.stats(now)['quotes'] += sum(z > 0 for z in sizes)
        for leg, size in zip(('up', 'down'), sizes):
            self.add(now, 'quoted_' + leg, size)
        self.account.event('maker_quote', now)
        self.audit('quote', market['id'], now, dict(quotes=row['quotes'], spot=str(spot), books=books))

    def trade(self, market_id, index, print_, now):
        row = self.state['markets'][market_id]
        if not row['quotes'] or row['settled'] or print_['side'] != 'SELL' or print_.get('size') is None:
            return ZERO
        q = row['quotes'][index]
        at = print_['received_ms']
        if at > now or at <= row['joined'] or print_.get('exchange_ms') is None or print_['exchange_ms'] <= row['joined'] or print_['exchange_ms'] > at or at >= timestamp(row['market']['endDate']).timestamp() * 1000 - float(self.config['stop_seconds']) * 1000:
            return ZERO
        identity = str(print_['recording_id'])
        if identity in q['seen']:
            return ZERO
        if len(q['seen']) >= 500 or len(row['marks']) >= 2000:
            self.cancel(row, now)
            return ZERO
        q['seen'].append(identity)
        p, bid, available = number(print_['price']), number(q['price']), number(print_['size'])
        if p > bid:
            return ZERO
        if p == bid:
            if q['ahead'] is None:
                return ZERO
            consumed = min(number(q['ahead']), available)
            q['ahead'] = str(number(q['ahead']) - consumed)
            available -= consumed
        # Bound ledger precision; never round a partial print up.
        delta = number(row['qty'][index]) - number(row['qty'][1 - index])
        outstanding = sum((max((number(q['remaining']) for q in r['quotes']), default=ZERO)
                           for r in self.state['markets'].values() if r is not row), ZERO)
        room = ZERO if delta > 0 else (-delta if delta < 0 else max(ZERO, min(
            number(self.config['market_cap']), number(self.config['global_cap']) - self.exposure() - outstanding)))
        qty = min(available, number(q['remaining']), room).quantize(Decimal('.00000001'), rounding=ROUND_DOWN).normalize()
        if qty <= 0:
            return ZERO
        cost = qty * bid
        q['remaining'] = str(number(q['remaining']) - qty)
        row['reserved'] = str(number(row['reserved']) - cost)
        self.account.transfer('locked', -cost)
        self.account.transfer('positions', cost)
        row['qty'][index] = str(number(row['qty'][index]) + qty)
        row['cost'][index] = str(number(row['cost'][index]) + cost)
        row['marks'].append(dict(index=index, qty=str(qty), price=str(bid), due=at + float(self.config['mark_seconds']) * 1000))
        leg = 'up' if index == 0 else 'down'
        self.add(now, 'filled_' + leg, qty)
        self.stats(now)['fill_events_' + leg] += 1
        self.add(now, 'estimated_rebate', rebate(qty, bid))
        peak = self.exposure()
        self.stats(now)['inventory_peak'] = str(max(number(self.stats(now)['inventory_peak']), peak))
        paired = min(map(number, row['qty']))
        if paired:
            costs = [number(c) if paired == number(z) else
                     (number(c) * paired / number(z)).quantize(Decimal('.000000000000000001'), rounding=ROUND_DOWN)
                     for c, z in zip(row['cost'], row['qty'])]
            for i in range(2):
                row['qty'][i] = str(number(row['qty'][i]) - paired)
                row['cost'][i] = str(number(row['cost'][i]) - costs[i])
            self.account.transfer('positions', -sum(costs))
            self.account.transfer('free', paired)
            self.add(now, 'paired_sets', paired)
            self.add(now, 'set_pnl', paired - sum(costs))
            self.add(now, 'net_pnl', paired - sum(costs))
        self.account.event('maker_fill', now)
        self.audit('fill', market_id, now, dict(print=print_, index=index, quantity=str(qty), price=str(bid), maker_fee='0', estimated_rebate=str(rebate(qty, bid)), strategy=TAG))
        return qty

    def mark(self, market_id, index, mid, at, now):
        row = self.state['markets'][market_id]
        for mark in list(row['marks']):
            if mark['index'] == index and at >= mark['due']:
                # First recorded mid after horizon, at most max_age_ms late.
                if at - mark['due'] <= float(self.config['max_age_ms']):
                    self.add(now, 'adverse_selection_loss', max(ZERO, (number(mark['price']) - number(mid)) * number(mark['qty'])))
                    self.stats(now)['mark_samples'] += 1
                else:
                    self.stats(now)['mark_missing'] += 1
                row['marks'].remove(mark)

    def settle(self, market_id, outcome, now, estimated=False):
        row = self.state['markets'][market_id]
        if row['settled'] or (outcome not in {'Up', 'Down'} and any(number(q) for q in row['qty'])) or now < timestamp(row['market']['endDate']).timestamp() * 1000:
            return
        self.cancel(row, now)
        cost = sum(map(number, row['cost']))
        payout = number(row['qty'][0 if outcome == 'Up' else 1]) if outcome else ZERO
        self.account.transfer('positions', -cost)
        self.account.transfer('free', payout)
        self.add(now, 'net_pnl', payout - cost)
        self.add(now, 'unpaired_settlement_loss', max(ZERO, cost - payout))
        self.stats(now)['estimated_settlements'] += int(estimated)
        row.update(settled=True, outcome=outcome, estimated=estimated, qty=['0', '0'], cost=['0', '0'])
        self.account.event('maker_settlement', now)
        self.audit('settlement', market_id, now, dict(outcome=outcome, estimated=estimated, cost=str(cost), payout=str(payout)))

    def report(self, now):
        s = dict(self.stats(now), config=self.config, inventory_units='unpaired_shares', maker_fee='0')
        for leg in ('up', 'down'):
            s['fill_rate_' + leg] = str(number(s['filled_' + leg]) / number(s['quoted_' + leg])) if number(s['quoted_' + leg]) else None
        s['net_pnl_including_estimated_rebate'] = str(number(s['net_pnl']) + number(s['estimated_rebate']))
        s['net_pnl_per_completed_set'] = str(number(s['set_pnl']) / number(s['paired_sets'])) if number(s['paired_sets']) else None
        return s


class RecordedFeed:
    """Bounded SQL polling in the account worker; readers never wait for this."""
    def __init__(self, account):
        self.maker = Maker(account)
        # Restart cancels virtual orders; inventory remains in the shared ledger.
        for row in self.maker.state['markets'].values():
            self.maker.cancel(row, int(time.time() * 1000))

    def poll(self, pipeline, now, quoting=True):
        from app.utils.db import get_db_connection
        from . import observer
        with pipeline.lock:
            markets = [m for m in pipeline.markets.values() if m.get('market_type') == 'up_down'
                       and m.get('asset') in {'BTC', 'ETH'} and m.get('interval') in {'5m', '15m'}][:100]
        known = self.maker.state['markets']
        markets = {m['id']: m for m in markets if m.get('market_type') == 'up_down' and m.get('asset') in {'BTC', 'ETH'} and m.get('interval') in {'5m', '15m'}}
        if not quoting:
            markets = {}
        markets.update({k: r['market'] for k, r in known.items() if not r['settled'] or r['marks']})
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                tokens = [m[k] for m in markets.values() for k in ('yesAssetId', 'noAssetId')]
                cur.execute('SELECT b.* FROM unnest(%s::text[]) AS t(token) CROSS JOIN LATERAL (SELECT token_id,bids,asks,exchange_at,received_at FROM qd_polymarket_observer_books WHERE token_id=t.token ORDER BY received_at DESC LIMIT 1) b', (tokens,))
                latest = {}
                for b in cur.fetchall():
                    b = dict(b)
                    b['exchange_at'] = b['exchange_at'].timestamp() * 1000 if b['exchange_at'] else None
                    latest[b['token_id']] = b
                for identity, market in markets.items():
                    books = [latest.get(market[k]) for k in ('yesAssetId', 'noAssetId')]
                    ref = observer._REFERENCE.get(market['asset']) if observer._REFERENCE else {}
                    fresh = ref.get('observed_at') and 0 <= (utc(now) - timestamp(ref['observed_at'])).total_seconds() <= 5
                    self.maker.quote(market, books, ref.get('price') if fresh and quoting else None, now)
                    if identity not in known:
                        continue
                    row = known[identity]
                    expired_marks = [m for m in row['marks'] if now > m['due'] + 60000]
                    self.maker.stats(now)['mark_missing'] += len(expired_marks)
                    row['marks'] = [m for m in row['marks'] if m not in expired_marks]
                    for index, token in enumerate((market['yesAssetId'], market['noAssetId'])):
                        if row['quotes']:
                            cur.execute('SELECT * FROM qd_polymarket_observer_trades WHERE token_id=%s AND received_at>%s ORDER BY received_at,recording_id LIMIT 501', (token, utc(row['joined'])))
                            prints = cur.fetchall()
                            if len(prints) > 500:
                                self.maker.cancel(row, now)
                            for trade in prints[:500]:
                                self.maker.trade(identity, index, dict(trade, received_ms=trade['received_at'].timestamp() * 1000,
                                    exchange_ms=trade['exchange_at'].timestamp() * 1000 if trade['exchange_at'] else None), now)
                        if not any(m['index'] == index for m in row['marks']):
                            continue
                        cur.execute('SELECT bids,asks,received_at,exchange_at FROM qd_polymarket_observer_books WHERE token_id=%s AND received_at>=%s ORDER BY received_at LIMIT 100',
                                    (token, utc(min((m['due'] for m in row['marks'] if m['index'] == index), default=now))))
                        for b in cur.fetchall():
                            if b['bids'] and b['asks'] and b['exchange_at'] and 0 <= (b['received_at'] - b['exchange_at']).total_seconds() * 1000 <= float(self.maker.config['max_age_ms']):
                                mid = (number(b['bids'][0]['price']) + number(b['asks'][0]['price'])) / 2
                                self.maker.mark(identity, index, mid, b['received_at'].timestamp() * 1000, now)
                    if now >= timestamp(market['endDate']).timestamp() * 1000 and not row['settled']:
                        cur.execute('SELECT official_outcome FROM qd_polymarket_observer_windows WHERE market_id=%s', (identity,))
                        resolution = cur.fetchone()
                        outcome = resolution['official_outcome'] if resolution else None
                        estimated = False
                        if not outcome:
                            # Recorded Binance observations nearest each boundary, no lookahead.
                            values = []
                            for boundary in (market['eventStartTime'], market['endDate']):
                                cur.execute("SELECT reference_price FROM qd_polymarket_observer_samples WHERE market_id=%s AND reference_source IN ('api.binance.com','data-api.binance.vision') AND reference_observed_at<=%s AND reference_observed_at>=%s::timestamptz - INTERVAL '10 seconds' ORDER BY reference_observed_at DESC LIMIT 1", (identity, boundary, boundary))
                                sample = cur.fetchone()
                                values.append(sample['reference_price'] if sample else None)
                            if all(v is not None for v in values):
                                outcome = 'Up' if values[1] >= values[0] else 'Down'
                                estimated = True
                        self.maker.settle(identity, outcome, now, estimated)
            finally:
                cur.close()
        # Closed zero-inventory markets need no growing live state.
        for identity, row in list(known.items()):
            if row['settled'] and not row['marks']:
                del known[identity]

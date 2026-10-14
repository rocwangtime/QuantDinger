"""Continuous public-book PAPER account. Sole worker owns accounting and DB I/O."""
import copy
import json
import logging
import os
import queue
import statistics
import threading
import time
import uuid
import zlib
from collections import deque
from datetime import datetime, timezone

from app.utils.db import get_db_connection
from . import account_model as model
from .account_history import BookHistory, coherent_pair, max_pair_skew_ms
from .client import CLOB, PublicClient
from .engine import ZERO, _array, digest, number, public
from .updown import timestamp

LOG = logging.getLogger(__name__)
PREFIX = 'POLYMARKET_PAPER_ACCOUNT_'


def enabled():
    return os.getenv(PREFIX + 'ENABLED', 'false').lower() in {'1', 'true', 'yes'}


def configuration():
    values = {}
    for key, default in model.DEFAULTS.items():
        raw = os.getenv(PREFIX + key.upper())
        if raw is not None:
            values[key] = raw.lower() in {'1', 'true', 'yes'} if isinstance(default, bool) else raw
    return model.validate(values)


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def day_key(ms):
    return utc(ms).date().isoformat()


def resolution_price(row, market):
    """Require explicit finality; closure or spot prices alone never resolve inventory."""
    prices, labels = _array(row.get('outcomePrices')), _array(row.get('outcomes'))
    if (row.get('closed') is not True or row.get('umaResolutionStatus') != 'resolved'
            or row.get('conditionId') != market['conditionId']
            or not isinstance(prices, list) or len(prices) != 2
            or labels not in (['Yes', 'No'], ['No', 'Yes'], ['Up', 'Down'], ['Down', 'Up'])):
        return None
    index = next(i for i, label in enumerate(labels) if label.lower() in ('yes', 'up'))
    price = number(prices[index])
    return str(price) if price in (0, 1) and number(prices[1-index]) == 1 - price else None


class Latency:
    def __init__(self, config):
        self.config = config
        self.samples = deque(maxlen=60)
        self.checked_ms = None
        self.error = None

    def measure(self, client, now):
        began = time.monotonic()
        try:
            client._json('GET', CLOB + '/time')
            self.samples.append((time.monotonic() - began) * 1000)
            self.error = None
        except ValueError as exc:
            self.error = str(exc)
        self.checked_ms = now

    def status(self, now):
        fresh = self.checked_ms is not None and now - self.checked_ms <= self.config['latency_refresh_seconds'] * 2000
        measured = bool(self.samples) and fresh and self.error is None
        median = statistics.median(self.samples) if measured else self.config['default_latency_ms']
        p90 = sorted(self.samples)[max(0, int(len(self.samples) * .9 + .999) - 1)] if measured else None
        return dict(median_rtt_ms=median, p90_rtt_ms=p90, samples=len(self.samples), fallback=not measured,
                    checked_ms=self.checked_ms, error=self.error,
                    one_way_ms=int(median * float(self.config['latency_multiplier']) + .999))


class Account:
    """State machine; only tick/signal callers mutate it, no network or SQL here."""
    def __init__(self, config=None, state=None):
        self.config = model.validate(config or {})
        self.state = state or dict(free=self.config['initial_capital'], locked='0', positions='0',
                                  pending={}, holdings={}, cooldowns={}, days={}, latency={}, events=[])
        self.changed = []
        self.rollover(int(time.time() * 1000))

    def equity(self):
        return sum((number(self.state[k]) for k in ('free', 'locked', 'positions')), ZERO)

    def daily(self, now):
        key = day_key(now)
        if key not in self.state['days']:
            self.state['days'][key] = dict(start_equity=str(self.equity()), net_pnl='0', fees='0', attempted=0,
                filled=0, single_leg_failures=0, unwinds=0, opportunities=0, loss_stop=False,
                peak_equity=str(self.equity()), max_drawdown='0', utilization_sum='0', utilization_count=0,
                utilization_max='0', by_market_type={})
        return self.state['days'][key]

    def rollover(self, now):
        self.daily(now)
        cutoff = day_key(now - self.config['retention_days'] * 86400000)
        self.state['days'] = {k: v for k, v in self.state['days'].items() if k >= cutoff}
        self.state['cooldowns'] = {k: v for k, v in self.state['cooldowns'].items() if v > now}

    def risk(self, now):
        stats = self.daily(now)
        equity = self.equity()
        peak = max(number(stats['peak_equity']), equity)
        stats['peak_equity'] = str(peak)
        stats['max_drawdown'] = str(max(number(stats['max_drawdown']), peak - equity))
        utilization = number(self.state['locked']) / equity if equity else ZERO
        stats['utilization_sum'] = str(number(stats['utilization_sum']) + utilization)
        stats['utilization_count'] += 1
        stats['utilization_max'] = str(max(number(stats['utilization_max']), utilization))
        if equity <= number(stats['start_equity']) * (1 - number(self.config['loss_fraction'])) and not stats['loss_stop']:
            stats['loss_stop'] = True
            self.event('daily_loss_stop', now)

    def event(self, name, now, trade_id=None):
        self.state['events'].append(dict(event=name, at=now, trade_id=trade_id))
        self.state['events'] = self.state['events'][-100:]

    def transfer(self, key, amount):
        self.state[key] = str(number(self.state[key]) + number(amount))
        if number(self.state[key]) < 0:
            raise ValueError('paper_account_negative_capital')

    def signal(self, market, books, now, latency):
        self.rollover(now)
        if market.get('endDate') and timestamp(market['endDate']) <= utc(now):
            return None
        self.risk(now)
        stats = self.daily(now)
        probe = model.size(market, books, self.config, free=self.config['initial_capital'], locked='0', equity=self.config['initial_capital'])
        if probe['reason'] in ('below_edge', 'missing_book'):
            return None
        stats['opportunities'] += 1
        kind = market.get('market_type', 'yes_no')
        group = stats['by_market_type'].setdefault(kind, dict(opportunities=0, attempted=0, filled=0, net_pnl='0', fees='0'))
        group['opportunities'] += 1
        cooldown = self.state['cooldowns'].get(market['id'], 0) > now
        if cooldown or any(t['market_id'] == market['id'] for t in self.state['pending'].values()):
            return None
        # Cap memory and outstanding capital reservations even during persistent
        # failures. Cooldown skips are counted as signals, not new attempts.
        if len(self.state['pending']) + len(self.state['holdings']) >= 100:
            return None
        stopped = stats['loss_stop']
        deployed = number(self.state['locked']) + sum((number(h['cost']) for h in self.state['holdings'].values() if h['kind'] == 'residual'), ZERO)
        decision = model.size(market, books, self.config, free=self.state['free'], locked=deployed,
                              equity=self.equity(), stopped=stopped, cooldown=cooldown)
        identity = uuid.uuid4().hex
        self.state['cooldowns'][market['id']] = now + self.config['cooldown_seconds'] * 1000
        stats['attempted'] += 1
        group['attempted'] += 1
        evidence = dict(version=model.VERSION, source_hash=model.SOURCE_HASH, market=copy.deepcopy(market), config=self.config.copy(),
            decision=decision, signal_ms=now, signal_books=books, latency=latency, steps={})
        trade = dict(id=identity, market_id=market['id'], market_type=kind, signal_ms=now, status='pending',
                     evidence=evidence, result={}, release_ms=None)
        if decision['reason']:
            self.finish(trade, now)
        else:
            reserve = number(decision['reserve'])
            self.transfer('free', -reserve)
            self.transfer('locked', reserve)
            trade.update(reserve=str(reserve), stage='leg1', due_ms=now + latency['one_way_ms'])
            self.state['pending'][identity] = trade
            self.changed.append(copy.deepcopy(trade))
        self.event('attempt', now, identity)
        return trade

    def finish(self, trade, now):
        result = model.outcome(trade['evidence'])
        self.state['cooldowns'][trade['market_id']] = now + self.config['cooldown_seconds'] * 1000
        trade.update(result=result, status=result['status'])
        reserve = number(trade.get('reserve', '0'))
        self.transfer('locked', -reserve)
        self.transfer('free', reserve)
        stats = self.daily(now)
        group = stats['by_market_type'].setdefault(trade['market_type'], dict(opportunities=0, attempted=0, filled=0, net_pnl='0', fees='0'))
        stats['fees'] = str(number(stats['fees']) + number(result['fees']))
        group['fees'] = str(number(group['fees']) + number(result['fees']))
        spent, proceeds = number(result['cost']), number(result['payout'])
        if trade['status'] == 'filled_merged':
            self.transfer('free', -spent)
            self.transfer('locked', spent)
            if self.config['merge_enabled']:
                due = now + self.config['settlement_seconds'] * 1000
            else:
                due = int(timestamp(trade['evidence']['market']['endDate']).timestamp() * 1000)
            trade['release_ms'] = due
            self.state['holdings'][trade['id']] = dict(cost=str(spent), payout=str(proceeds), due_ms=due,
                kind='complete', trade=copy.deepcopy(trade))
            stats['filled'] += 1
            group['filled'] += 1
        elif trade['status'] in ('single_leg_unwound', 'single_leg_residual'):
            self.transfer('free', proceeds - spent)
            stats['single_leg_failures'] += 1
            if any(x['role'] == 'unwind' and number(x['filled']) for x in result['legs']):
                stats['unwinds'] += 1
            self.pnl(number(result['realized_pnl']), stats, group)
            if number(result['residual_quantity']):
                # Zero is a conservative mark; retain cost basis separately.
                self.state['holdings'][trade['id']] = dict(cost=result['residual_cost'], payout='0', kind='residual',
                    due_ms=int(timestamp(trade['evidence']['market']['endDate']).timestamp() * 1000) if trade['evidence']['market'].get('endDate') else 0, trade=copy.deepcopy(trade))
                self.pnl(-number(result['residual_cost']), stats, group)
        self.state['pending'].pop(trade['id'], None)
        self.changed.append(copy.deepcopy(trade))
        self.event(trade['status'], now, trade['id'])
        self.risk(now)

    @staticmethod
    def pnl(amount, stats, group):
        stats['net_pnl'] = str(number(stats['net_pnl']) + amount)
        group['net_pnl'] = str(number(group['net_pnl']) + amount)

    def tick(self, now, lookup):
        self.rollover(now)
        for identity, holding in list(self.state['holdings'].items()):
            if holding['due_ms'] > now:
                continue
            trade = holding['trade']
            stats = self.daily(now)
            group = stats['by_market_type'].setdefault(trade['market_type'], dict(opportunities=0, attempted=0, filled=0, net_pnl='0', fees='0'))
            if holding['kind'] == 'complete':
                self.transfer('locked', -number(holding['cost']))
                self.transfer('free', holding['payout'])
                self.pnl(number(holding['payout']) - number(holding['cost']), stats, group)
            else:
                # Unknown winner is never inferred from spot or expired depth.
                # Keep the zero-mark residual until explicit public resolution.
                continue
            trade['released_at_ms'] = now
            trade['evidence']['release'] = dict(at_ms=now, kind='complete')
            trade['result'] = model.outcome(trade['evidence'])
            self.changed.append(copy.deepcopy(trade))
            self.state['holdings'].pop(identity)
            self.event('release', now, identity)
        for trade in list(self.state['pending'].values()):
            if trade['due_ms'] > now:
                continue
            evidence = trade['evidence']
            market = evidence['market']
            stage, due = trade['stage'], trade['due_ms']
            books = lookup(market, due)
            index = 1 if stage == 'leg2' else 0
            book = books[index] if books else None
            evidence['steps'][stage] = dict(at_ms=due, books=books, book=book,
                                           latency_ms=evidence['latency']['one_way_ms'])
            fill = model.fill(market, book, evidence['decision']['quantity'], evidence['config'],
                    evidence['decision']['limits'][index] if stage != 'unwind' else None,
                    side='SELL' if stage == 'unwind' else 'BUY')
            if stage == 'leg1' and number(fill['filled']):
                trade.update(stage='leg2', due_ms=due + evidence['latency']['one_way_ms'])
            elif stage == 'leg2' and not number(fill['filled']):
                trade.update(stage='unwind', due_ms=due + evidence['latency']['one_way_ms'])
            else:
                self.finish(trade, now)
                continue
            self.changed.append(copy.deepcopy(trade))
        self.risk(now)

    def resolve(self, identity, price, now, response=None):
        holding = self.state['holdings'].get(identity)
        if not holding or holding['kind'] != 'residual':
            return
        trade = holding['trade']
        payout = number(trade['result']['residual_quantity']) * number(price)
        self.transfer('free', payout)
        stats = self.daily(now)
        group = stats['by_market_type'].setdefault(trade['market_type'], dict(opportunities=0, attempted=0, filled=0, net_pnl='0', fees='0'))
        self.pnl(payout, stats, group)
        trade['released_at_ms'] = now
        trade['evidence']['release'] = dict(at_ms=now, kind='residual', outcome_price=str(price), public_resolution=response)
        trade['result'] = model.outcome(trade['evidence'])
        self.changed.append(copy.deepcopy(trade))
        self.state['holdings'].pop(identity)
        self.event('residual_resolution', now, identity)

    def status(self, now):
        stats = self.daily(now)
        return dict(mode='paper', equity=str(self.equity()), free_capital=self.state['free'], locked_capital=self.state['locked'],
                    positions_value=self.state['positions'], open_positions=len(self.state['holdings']),
                    residual_cost_basis=str(sum((number(h['cost']) for h in self.state['holdings'].values() if h['kind'] == 'residual'), ZERO)),
                    today_pnl=stats['net_pnl'], loss_stop=stats['loss_stop'], latency=self.state['latency'],
                    pending=len(self.state['pending']), config=self.config)


class Store:
    def __init__(self, account_id='global'):
        self.account_id = account_id

    def load(self, config):
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute('SELECT state FROM qd_polymarket_paper_accounts WHERE account_id=%s', (self.account_id,))
                row = cur.fetchone()
                return Account(config, row['state'] if row else None)
            finally:
                cur.close()

    def save(self, account, now):
        if 'maker' in account.state:
            from .maker import Maker
            maker = Maker(account)
            for day in list(maker.state['days']):
                if day < day_key(now - account.config['retention_days'] * 86400000):
                    del maker.state['days'][day]
                    continue
                at = int(timestamp(day + 'T12:00:00Z').timestamp() * 1000)
                account.daily(at)['maker'] = maker.report(at)
        state = public(account.state)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute('INSERT INTO qd_polymarket_paper_accounts(account_id,state) VALUES (%s,%s::jsonb) ON CONFLICT(account_id) DO UPDATE SET state=EXCLUDED.state,updated_at=NOW()', (self.account_id, json.dumps(state)))
                for trade in account.changed:
                    evidence = public(trade['evidence'])
                    frozen = zlib.compress(json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode())
                    result = dict(trade['result'], release_ms=trade.get('release_ms'))
                    cur.execute('INSERT INTO qd_polymarket_paper_trades(id,account_id,market_id,market_type,status,signal_at,released_at,result,evidence,evidence_hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s) ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,released_at=EXCLUDED.released_at,result=EXCLUDED.result,evidence=EXCLUDED.evidence,evidence_hash=EXCLUDED.evidence_hash',
                                (trade['id'], self.account_id, trade['market_id'], trade['market_type'], trade['status'], utc(trade['signal_ms']), utc(trade['released_at_ms']) if trade.get('released_at_ms') else None, json.dumps(result), frozen, digest(evidence)))
                    for leg in trade['result'].get('legs', []):
                        detail = dict(leg, **evidence['steps'][leg['role']])
                        detail.pop('books', None)
                        detail.pop('book', None)
                        cur.execute('INSERT INTO qd_polymarket_paper_legs(trade_id,role,detail) VALUES (%s,%s,%s::jsonb) ON CONFLICT(trade_id,role) DO UPDATE SET detail=EXCLUDED.detail', (trade['id'], leg['role'], json.dumps(detail)))
                for event in getattr(account, 'maker_changed', []):
                    cur.execute('INSERT INTO qd_polymarket_maker_events(id,account_id,strategy,market_id,observed_at,event,detail) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(id) DO NOTHING',
                        (event['id'], self.account_id, 'maker_complete_set_v1', event['market_id'], utc(event['at']), event['event'], json.dumps(public(event['detail']), default=str)))
                if 'maker' in state:
                    cur.execute('DELETE FROM qd_polymarket_maker_events WHERE account_id=%s AND observed_at<%s', (self.account_id, utc(now - account.config['retention_days'] * 86400000)))
                for day, stats in state['days'].items():
                    cur.execute('INSERT INTO qd_polymarket_paper_daily(account_id,day,stats) VALUES (%s,%s,%s::jsonb) ON CONFLICT(account_id,day) DO UPDATE SET stats=EXCLUDED.stats', (self.account_id, day, json.dumps(stats)))
                cur.execute('INSERT INTO qd_polymarket_paper_ledger(account_id,observed_at,event,free,locked,positions,equity,detail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)',
                            (self.account_id, utc(now), state['events'][-1]['event'] if state['events'] else 'snapshot', state['free'], state['locked'], state['positions'], str(account.equity()), json.dumps(state['events'])))
                for table, column in (('trades', 'signal_at'), ('ledger', 'observed_at'), ('daily', 'day')):
                    cur.execute(f'DELETE FROM qd_polymarket_paper_{table} WHERE account_id=%s AND {column}<%s' + (" AND id NOT IN (SELECT jsonb_object_keys(state->'holdings') FROM qd_polymarket_paper_accounts WHERE account_id=%s UNION SELECT jsonb_object_keys(state->'pending') FROM qd_polymarket_paper_accounts WHERE account_id=%s)" if table == 'trades' else ''), (self.account_id, utc(now - account.config['retention_days'] * 86400000), *((self.account_id, self.account_id) if table == 'trades' else ())))
                db.commit()
                account.changed.clear()
                if hasattr(account, 'maker_changed'):
                    account.maker_changed.clear()
            finally:
                cur.close()


class Runtime:
    """Bounded evaluator inbox; worker alone performs HTTP, simulation and SQL."""
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.config = configuration()
        self.history = BookHistory(self.config['history_seconds'], self.config['history_events'])
        self.inbox = queue.Queue(maxsize=256)
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name='polymarket-paper-account', daemon=True)
        self.dropped = 0
        self.error = None
        self.ready = False
        self.latency = Latency(self.config)
        self.resolutions = queue.Queue(maxsize=100)
        self.residual_markets = {}
        self.public_thread = threading.Thread(target=self.public_loop, name='polymarket-paper-public', daemon=True)

    def public_loop(self):
        client = PublicClient()
        try:
            while not self.stop_event.is_set():
                self.latency.measure(client, int(time.time() * 1000))
                for identity, market in list(self.residual_markets.items()):
                    try:
                        row = client._json('GET', 'https://gamma-api.polymarket.com/markets/' + market['id'])
                        price = resolution_price(row, market)
                        if price is not None:
                            self.resolutions.put_nowait((identity, price, {k: row.get(k) for k in ('id', 'conditionId', 'closed', 'umaResolutionStatus', 'outcomes', 'outcomePrices')}))
                    except (ValueError, TypeError, KeyError, queue.Full):
                        pass
                self.stop_event.wait(self.config['latency_refresh_seconds'])
        finally:
            client.close()

    def submit(self, market, books, now):
        try:
            self.inbox.put_nowait((market, books, now))
        except queue.Full:
            self.dropped += 1

    def lookup(self, market, at):
        assets = (market['yesAssetId'], market['noAssetId'])
        with self.pipeline.lock:
            states = self.history.copy_assets(assets)
        return [BookHistory.lookup(states, a, at, self.config['max_book_age_ms']) for a in assets]

    def admit(self, account, market, observed, now):
        """Pair coherence applies only to new signals, never delayed leg lookup."""
        if now - observed > self.config['max_book_age_ms']:
            return
        signal_books = self.lookup(market, observed)
        if any(book is None for book in signal_books) or not coherent_pair(
                [book.get('exchangeMs') for book in signal_books], observed,
                self.config['max_book_age_ms'], max_pair_skew_ms()):
            return
        if not self.config['merge_enabled'] and not market.get('endDate'):
            return
        account.signal(market, signal_books, observed, self.latency.status(now))

    def run(self):
        store = Store()
        from app.utils.db import get_db_connection_sync
        lease = get_db_connection_sync()
        cur = lease.cursor()
        try:
            cur.execute('SELECT pg_try_advisory_lock(%s) AS acquired', (202610115001,))
            if not cur.fetchone()['acquired']:
                raise ValueError('paper_account_already_running')
            lease.commit()
            account = store.load(self.config)
            # Restart cannot execute a missed entry against invented books.
            for trade in list(account.state['pending'].values()):
                if 'leg1' not in trade['evidence']['steps']:
                    trade['evidence']['decision']['reason'] = 'restart_cancelled'
                    trade['evidence']['restart_cancelled'] = True
                account.finish(trade, int(time.time() * 1000))
            from .maker import RecordedFeed, enabled as maker_enabled
            maker_feed = RecordedFeed(account) if maker_enabled() or 'maker' in account.state else None
            self.ready = True
            self.public_thread.start()
            last_save = 0
            last_maker = 0
            while not self.stop_event.is_set():
                now = int(time.time() * 1000)
                account.state['latency'] = self.latency.status(now)
                self.residual_markets = {k: v['trade']['evidence']['market'] for k, v in account.state['holdings'].items() if v['kind'] == 'residual'}
                while not self.resolutions.empty():
                    identity, price, response = self.resolutions.get_nowait()
                    account.resolve(identity, price, now, response)
                # Prior failed saves are retried before accepting more signals.
                account.tick(now, self.lookup)
                if maker_feed and now - last_maker >= 250:
                    maker_feed.poll(self.pipeline, now, quoting=maker_enabled())
                    last_maker = now
                for _ in range(32):
                    try:
                        market, books, observed = self.inbox.get_nowait()
                    except queue.Empty:
                        break
                    self.admit(account, market, observed, now)
                if account.changed or now - last_save >= 10000:
                    while not self.stop_event.is_set():
                        try:
                            store.save(account, now)
                            self.error = None
                            break
                        except Exception as exc:
                            self.error = str(exc)[:160]
                            LOG.exception('Paper account persistence failed; admissions paused')
                            self.stop_event.wait(1)
                    last_save = now
                self.stop_event.wait(.025)
            if maker_feed:
                for row in maker_feed.maker.state['markets'].values():
                    maker_feed.maker.cancel(row, int(time.time() * 1000))
            store.save(account, int(time.time() * 1000))
        except Exception as exc:
            self.error = str(exc)[:160]
            LOG.exception('Paper account worker stopped')
        finally:
            self.ready = False
            self.stop_event.set()
            if self.public_thread.is_alive():
                self.public_thread.join(15)
            try:
                lease.rollback()
                cur.execute('SELECT pg_advisory_unlock(%s)', (202610115001,))
                lease.commit()
            finally:
                cur.close()
                lease.close()

    def close(self, timeout):
        self.stop_event.set()
        self.thread.join(timeout)

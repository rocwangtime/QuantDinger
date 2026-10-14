"""Read-only authenticated account queries and deterministic frozen replay."""
import json
import os
import time
import zlib

from app.utils.db import get_db_connection
from . import account_model as model
from .paper_account import Account, configuration, enabled


def taker_verification_status():
    value = os.getenv('POLYMARKET_PAPER_ACCOUNT_TAKER_STATUS', '').strip().lower()
    return 'unverified' if value == 'unverified' else 'unspecified'


def query(sql, params=()):
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall() or []
        finally:
            cur.close()


def status():
    rows = query('SELECT state,updated_at FROM qd_polymarket_paper_accounts WHERE account_id=%s', ('global',))
    account = Account(configuration(), rows[0]['state'] if rows else None)
    result = account.status(int(time.time() * 1000))
    result['taker_verification_status'] = taker_verification_status()
    fresh = bool(rows) and time.time() - rows[0]['updated_at'].timestamp() < 30
    result.update(enabled=enabled(), running=enabled() and fresh, updated_at=rows[0]['updated_at'] if rows else None,
                  holdings=[dict(id=k, kind=v['kind'], cost_basis=v['cost'], payout=v['payout'],
                                 due_ms=v['due_ms']) for k, v in account.state['holdings'].items()])
    if 'maker' in account.state:
        from .maker import Maker
        result['maker'] = Maker(account).report(int(time.time() * 1000))
    return result


def trades(limit=50, offset=0, status=None, market_type=None, day=None):
    terms, params = ['account_id=%s'], ['global']
    for column, value in (('status', status), ('market_type', market_type)):
        if value:
            terms.append(column + '=%s')
            params.append(value)
    if day:
        terms.append("signal_at >= %s::date AT TIME ZONE 'UTC' AND signal_at < (%s::date + 1) AT TIME ZONE 'UTC'")
        params.extend([str(day), str(day)])
    where = ' AND '.join(terms)
    total = query('SELECT count(*) AS total FROM qd_polymarket_paper_trades WHERE ' + where, tuple(params))[0]['total']
    rows = query('SELECT id,market_id,market_type,status,signal_at,released_at,result,evidence_hash FROM qd_polymarket_paper_trades WHERE ' + where + ' ORDER BY signal_at DESC,id LIMIT %s OFFSET %s', (*params, limit, offset))
    return dict(items=rows, total=total, limit=limit, offset=offset)


def detail(identity):
    rows = query('SELECT * FROM qd_polymarket_paper_trades WHERE account_id=%s AND id=%s', ('global', identity))
    if not rows:
        raise ValueError('polymarket.notFound')
    row = rows[0]
    row['evidence'] = json.loads(zlib.decompress(bytes(row['evidence'])))
    if model.digest(row['evidence']) != row['evidence_hash']:
        raise ValueError('polymarket.evidenceHashMismatch')
    return row


def replay(identity):
    row = detail(identity)
    if row['status'] == 'pending':
        raise ValueError('polymarket.evidenceUnavailable')
    expected = {k: v for k, v in row['result'].items() if k != 'release_ms'}
    return model.replay(row['evidence'], expected, row['evidence_hash'])


def reports(days=30, day=None):
    rows = query('SELECT day,stats FROM qd_polymarket_paper_daily WHERE account_id=%s AND ' +
        ('day=%s::date' if day else "day >= (NOW() AT TIME ZONE 'UTC')::date - %s") + ' ORDER BY day DESC',
        ('global', str(day) if day else days - 1))
    result = []
    for row in rows:
        s = row['stats']
        result.append(dict(day=str(row['day']), **s, fill_rate=s['filled'] / s['attempted'] if s['attempted'] else 0,
            capital_utilization_avg=float(s['utilization_sum']) / s['utilization_count'] if s['utilization_count'] else 0,
            capital_utilization_max=s['utilization_max']))
    ws_rows = query('SELECT day,reconnect_count,slow_consumer_count,rebalance_count,shard_msg_totals FROM qd_polymarket_ws_daily WHERE ' +
        ('day=%s::date' if day else "day >= (NOW() AT TIME ZONE 'UTC')::date - %s"),
        (str(day) if day else days - 1,))
    by_day = {row['day']: row for row in result}
    for row in ws_rows:
        key = str(row['day'])
        report = by_day.setdefault(key, dict(day=key))
        stats = report.setdefault('ws_stats', dict(reconnect_count=0, slow_consumer_count=0,
                                                 rebalance_count=0, shard_msg_totals={}))
        for name in ('reconnect_count', 'slow_consumer_count', 'rebalance_count'):
            stats[name] += row[name]
        for shard, total in row['shard_msg_totals'].items():
            stats['shard_msg_totals'][shard] = stats['shard_msg_totals'].get(shard, 0) + total
    for report in by_day.values():
        report['taker_verification_status'] = taker_verification_status()
        report.setdefault('ws_stats', dict(reconnect_count=0, slow_consumer_count=0,
                                          rebalance_count=0, shard_msg_totals={}))
    return sorted(by_day.values(), key=lambda row: row['day'], reverse=True)

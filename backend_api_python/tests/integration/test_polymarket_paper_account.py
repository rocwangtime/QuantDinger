"""Additive account migration, atomic persistence and report queries on local PG18."""
import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from app.services.polymarket import account_api, paper_account as p
from app.services.polymarket.engine import number
from app.utils.db import get_db_connection

pytestmark = pytest.mark.integration


@pytest.fixture
def isolated(monkeypatch):
    dsn = os.getenv('QD_TEST_POSTGRES_DSN', '')
    if not dsn:
        pytest.skip('QD_TEST_POSTGRES_DSN required')
    assert '@127.0.0.1:5432/qd_automation_test' in dsn
    monkeypatch.setenv('DATABASE_URL', dsn)
    schema = 'paper_test_' + uuid4().hex
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET LOCAL search_path TO {schema}')
        sql = (Path(__file__).parents[2] / 'migrations/20261011_polymarket_paper_account.sql').read_text()
        cur.execute(sql)
        cur.execute(sql)
        cur.execute((Path(__file__).parents[2] / 'migrations/20261013_polymarket_ws_daily.sql').read_text())
        maker_sql = (Path(__file__).parents[2] / 'migrations/20261014_polymarket_maker.sql').read_text()
        cur.execute(maker_sql)
        cur.execute(maker_sql)
        db.commit()
        cur.close()
    @contextmanager
    def scoped():
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(f'SET search_path TO {schema}')
            cur.close()
            try:
                yield db
            finally:
                db.rollback()
                cur = db.cursor()
                cur.execute('SET search_path TO public')
                db.commit()
                cur.close()
    monkeypatch.setattr(p, 'get_db_connection', scoped)
    monkeypatch.setattr(account_api, 'get_db_connection', scoped)
    try:
        yield scoped
    finally:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(f'DROP SCHEMA {schema} CASCADE')
            db.commit()
            cur.close()


def market():
    return dict(id='42', yesAssetId='11', noAssetId='22', feeRate='.07', tickSize='.01',
                minOrderNotional='1', conditionId='0x' + 'a' * 64, endDate='2030-01-01T00:00:00Z')


def books():
    return [dict(assetId=a, observedMs=1000, asks=[dict(price='.4', size='10000')],
                 bids=[dict(price='.39', size='10000')]) for a in ('11', '22')]


def test_persistence_report_evidence_replay_and_restart(isolated):
    store = p.Store()
    a = store.load({'retention_days': 90, 'taker_enabled': True})
    now = 1780000000000
    t = a.signal(market(), books(), now, dict(one_way_ms=100, median_rtt_ms=100, fallback=False))
    store.save(a, now)
    restored = store.load(a.config)
    assert restored.state['pending'][t['id']]['stage'] == 'leg1'
    assert number(restored.state['free']) + number(restored.state['locked']) == 5000
    for delta in (100, 200):
        a.tick(now + delta, lambda *args: books())
        store.save(a, now + delta)
    assert account_api.trades(status='filled_merged')['total'] == 1
    detail = account_api.detail(t['id'])
    assert detail['evidence']['steps']['leg2']['at_ms'] == now + 200
    assert account_api.replay(t['id'])['replayMatches']
    reports = account_api.reports(day=p.day_key(now))
    assert reports[0]['attempted'] == reports[0]['filled'] == 1
    assert reports[0]['fill_rate'] == 1 and reports[0]['opportunities'] == 1
    assert float(reports[0]['fees']) > 0
    a.tick(now + 60200, lambda *args: None)
    store.save(a, now + 60200)
    assert account_api.replay(t['id'])['replayMatches']
    assert float(account_api.reports(day=p.day_key(now))[0]['net_pnl']) > 0
    assert account_api.detail(t['id'])['released_at'] is not None
    with isolated() as db:
        cur = db.cursor()
        cur.execute('SELECT count(*) AS count FROM qd_polymarket_paper_legs')
        assert cur.fetchone()['count'] == 2
        cur.execute('SELECT count(*) AS count FROM qd_polymarket_paper_ledger WHERE abs(free+locked+positions-equity) > 0.000001')
        assert cur.fetchone()['count'] == 0
        cur.close()


def test_retention_bounded_preserves_open_capital_evidence(isolated):
    store = p.Store()
    a = p.Account({'taker_enabled': True, 'retention_days': 1, 'merge_enabled': False})
    now = 1780000000000
    t = a.signal(market(), books(), now, dict(one_way_ms=1))
    a.tick(now + 1, lambda *args: books())
    a.tick(now + 2, lambda *args: books())
    store.save(a, now)
    a.rollover(now + 2 * 86400000)
    store.save(a, now + 2 * 86400000)
    assert account_api.detail(t['id'])['status'] == 'filled_merged'
    assert len(account_api.reports(day=p.day_key(now))) == 0


def test_maker_atomic_shared_ledger_reports_restart_and_retry(isolated):
    from app.services.polymarket.maker import Maker, RecordedFeed, TAG
    from tests.test_polymarket_maker import NOW, admit, market as maker_market, books as maker_books, print_
    a = p.Account({'taker_enabled': True})
    m = Maker(a)
    admit(m, 'BTC:5m')
    m.observe_spot('BTC', '60000', NOW, NOW)
    m.step(maker_market(), maker_books(), NOW)
    m.prints('42', 0, [print_('up', price='.48', size='1')], NOW + 400)  # Through print: all 10 Up at .49.
    m.prints('42', 1, [print_('down', price='.49', size='104', at=NOW + 450)], NOW + 500)  # 100 queued ahead.
    events = list(a.maker_changed)
    store = p.Store()
    store.save(a, NOW + 500)
    # Ambiguous commit retry uses stable event IDs and overwrites daily state.
    a.maker_changed = events
    store.save(a, NOW + 500)
    restored = store.load(a.config)
    assert restored.equity() == number('5000.08')
    assert number(restored.state['positions']) == number('2.94')
    assert number(restored.state['locked']) == number('2.94')  # Down remainder still resting.
    feed = RecordedFeed(restored)  # Restart: virtual orders stay fillable until the cancel lands.
    row = feed.maker.state['markets']['42']
    assert row['orders'] and all(o['cancel_at'] is not None for o in row['orders'])
    feed.maker.finalize(row, max(o['cancel_at'] for o in row['orders']) + 2000)
    assert number(restored.state['locked']) == 0
    assert number(restored.state['positions']) == number('2.94')
    feed.maker.settle('42', 'Down', NOW + 420000)
    store.save(restored, NOW + 420000)
    report = account_api.reports(day=p.day_key(NOW))[0]
    assert report['attempted'] == report['filled'] == 0  # taker counters isolated
    maker = report['maker']
    assert maker['strategy'] == TAG and maker['paired_sets'] == '4.00000000'
    assert number(maker['net_pnl']) == number('-2.86') == number(maker['decomposed_pnl'])
    assert maker['estimated_rebate'] is None and number(maker['rebate_scenario_only']) > 0
    assert maker['net_pnl_including_estimated_rebate'] == maker['net_pnl']
    assert number(maker['net_pnl_per_completed_set']) == number('.02')
    with isolated() as db:
        cur = db.cursor()
        cur.execute("SELECT count(*) AS n FROM qd_polymarket_maker_events WHERE event='fill' AND strategy=%s", (TAG,))
        assert cur.fetchone()['n'] == 2
        cur.execute("SELECT count(*) AS n FROM qd_polymarket_maker_events WHERE event='market_summary'")
        assert cur.fetchone()['n'] == 1
        cur.execute('SELECT count(*) AS n FROM qd_polymarket_paper_ledger WHERE abs(free+locked+positions-equity) > 0.000001')
        assert cur.fetchone()['n'] == 0
        cur.execute('SELECT state FROM qd_polymarket_paper_accounts')
        assert number(cur.fetchone()['state']['free']) == number('4997.14')
        cur.close()


def test_recorded_maker_feed_prints_official_and_proxy_settlement(isolated, monkeypatch):
    import json
    import threading
    from types import SimpleNamespace
    from app.services.polymarket import maker, observer
    from tests.test_polymarket_maker import NOW, market as maker_market
    monkeypatch.setattr('app.utils.db.get_db_connection', isolated)
    clock = dict(now=NOW)
    reference = SimpleNamespace(get=lambda asset: dict(price='60000', observed_at=p.utc(clock['now']).isoformat()))
    monkeypatch.setattr(observer, '_REFERENCE', reference)
    market_ = maker_market()
    pipeline = SimpleNamespace(lock=threading.Lock(), markets={'42': market_})
    with isolated() as db:
        cur = db.cursor()
        cur.execute("CREATE TABLE qd_polymarket_observer_samples (market_id TEXT, reference_price NUMERIC, reference_source TEXT, reference_observed_at TIMESTAMPTZ, observed_at TIMESTAMPTZ)")
        cur.execute((Path(__file__).parents[2] / 'migrations/20261014_polymarket_recording.sql').read_text())
        for token in ('42u', '42d'):
            cur.execute("INSERT INTO qd_polymarket_observer_books VALUES (%s,'42',%s,%s,%s,'ws',%s::jsonb,%s::jsonb)",
                (str(uuid4()), token, p.utc(NOW), p.utc(NOW), json.dumps([dict(price='.49', size='100')]), json.dumps([dict(price='.51', size='100')])))
        cur.execute("INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time,official_outcome) VALUES ('42','c','BTC','5m',%s,%s,'Down')", (market_['eventStartTime'], market_['endDate']))
        db.commit()
        cur.close()
    a = p.Account({'taker_enabled': True})
    feed = maker.RecordedFeed(a)
    feed.poll(pipeline, NOW)
    row = a.state['maker']['markets']['42']
    # Cold-start priors: BTC 5m is pre-open only, at probation size.
    assert sorted((o['side'], o['price'], o['size']) for o in row['orders'] if o['kind'] == 'live') == [(0, '0.49', '5.00'), (1, '0.49', '5.00')]
    assert a.state['maker']['selection']['reviewed'] == p.day_key(NOW)
    with isolated() as db:
        cur = db.cursor()
        cur.execute("INSERT INTO qd_polymarket_observer_trades VALUES (%s,'42','42u',.48,3,'SELL',%s,%s,NULL)", (str(uuid4()), p.utc(NOW + 300), p.utc(NOW + 300)))
        db.commit()
        cur.close()
    clock['now'] = NOW + 400
    feed.poll(pipeline, NOW + 400)
    feed.poll(pipeline, NOW + 500)  # Re-reading the lookback window never double fills.
    assert row['qty'][0] == '5.00000000'
    p.Store().save(a, NOW + 500)
    clock['now'] = NOW + 420001
    feed.poll(pipeline, NOW + 420001)
    assert feed.maker.stats(NOW)['estimated_settlements'] == 0
    assert a.equity() == number('4997.55')
    assert '42' not in feed.maker.state['markets']
    # A second run with no official outcome can use recorded Binance boundaries.
    with isolated() as db:
        cur = db.cursor()
        cur.execute("UPDATE qd_polymarket_observer_windows SET official_outcome=NULL")
        for at, price in ((market_['eventStartTime'], '60000'), (market_['endDate'], '60001')):
            cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,reference_price,reference_source,reference_observed_at) VALUES ('42',%s,'api.binance.com',%s)", (price, at))
        db.commit()
        cur.close()
    b = p.Account({'taker_enabled': True})
    clock['now'] = NOW
    mf = maker.RecordedFeed(b)
    mf.poll(pipeline, NOW)
    clock['now'] = NOW + 400
    mf.poll(pipeline, NOW + 400)
    assert b.state['maker']['markets']['42']['qty'][0] == '5.00000000'
    clock['now'] = NOW + 420001
    mf.poll(pipeline, NOW + 420001)
    assert mf.maker.stats(NOW)['estimated_settlements'] == 1
    assert b.equity() == number('5002.55')


def test_maker_evidence_failure_rolls_back_account_and_ledger(isolated):
    from app.services.polymarket.maker import Maker
    from tests.test_polymarket_maker import NOW, market as maker_market, books as maker_books
    a = p.Account({'taker_enabled': True})
    store = p.Store()
    store.save(a, NOW)
    m = Maker(a)
    m.observe_spot('BTC', '60000', NOW, NOW)
    m.step(maker_market(), maker_books(), NOW)
    assert number(a.state['locked']) > 0
    with isolated() as db:
        cur = db.cursor()
        cur.execute('DROP TABLE qd_polymarket_maker_events')
        db.commit()
        cur.close()
    from psycopg2.errors import UndefinedTable
    with pytest.raises(UndefinedTable, match='qd_polymarket_maker_events'):
        store.save(a, NOW + 100)
    restored = store.load(a.config)
    assert number(restored.state['free']) == 5000
    assert number(restored.state['locked']) == 0
    assert 'maker' not in restored.state
    assert a.maker_changed  # Can retry the same stable IDs after repair.
    with isolated() as db:
        cur = db.cursor()
        cur.execute('SELECT count(*) AS n FROM qd_polymarket_paper_ledger')
        assert cur.fetchone()['n'] == 1
        cur.close()


def test_verification_annotation_survives_repeated_saves(isolated, monkeypatch):
    monkeypatch.setenv('POLYMARKET_PAPER_ACCOUNT_TAKER_STATUS', 'unverified')
    a = p.Account({'taker_enabled': True})
    for at in (1000, 2000):
        a.daily(at)
        p.Store().save(a, at)
        assert account_api.status()['taker_verification_status'] == 'unverified'
        assert account_api.reports(day='1970-01-01')[0]['taker_verification_status'] == 'unverified'
    assert 'taker_verification_status' not in a.state

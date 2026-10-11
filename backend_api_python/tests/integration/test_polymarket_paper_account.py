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
    a = store.load({'retention_days': 90})
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
    a = p.Account({'retention_days': 1, 'merge_enabled': False})
    now = 1780000000000
    t = a.signal(market(), books(), now, dict(one_way_ms=1))
    a.tick(now + 1, lambda *args: books())
    a.tick(now + 2, lambda *args: books())
    store.save(a, now)
    a.rollover(now + 2 * 86400000)
    store.save(a, now + 2 * 86400000)
    assert account_api.detail(t['id'])['status'] == 'filled_merged'
    assert len(account_api.reports(day=p.day_key(now))) == 0

"""Evidence schema/writes against local PG18, with isolated schemas only."""
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from app.services.polymarket import observer, recording
from app.utils.db import get_db_connection

pytestmark = pytest.mark.integration


@pytest.fixture
def database(monkeypatch):
    dsn = os.getenv('QD_TEST_POSTGRES_DSN', '')
    if not dsn:
        pytest.skip('Local PG18 DSN required')
    assert dsn == 'postgresql://qd_test:qd_test@127.0.0.1:5432/qd_automation_test'
    monkeypatch.setenv('DATABASE_URL', dsn)
    schema = 'pm_recording_' + uuid4().hex
    migrations = Path(__file__).parents[2] / 'migrations'
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute('SHOW server_version_num')
        assert int(next(iter(cur.fetchone().values()))) // 10000 == 18
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET search_path TO {schema}')
        for file in ('20261011_polymarket_observer.sql', '20261011_polymarket_updown.sql',
                     '20261014_polymarket_recording.sql', '20261014_polymarket_recording.sql',
                     '20261014_polymarket_backtest.sql'):
            cur.execute((migrations / file).read_text())
        db.commit()
        @contextmanager
        def connect():
            try:
                yield db
            except Exception:
                db.rollback()
                raise
        monkeypatch.setattr(observer, 'get_db_connection', connect)
        monkeypatch.setattr(recording, 'get_db_connection', connect)
        try:
            yield db, cur
        finally:
            db.rollback()
            cur.execute('SET search_path TO public')
            cur.execute(f'DROP SCHEMA {schema} CASCADE')
            db.commit()
            cur.close()


def market(start=None):
    start = start or datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=30)
    return dict(id='123', conditionId='condition', yesAssetId='u', noAssetId='d', slug='test', question='test',
                feeRate='0', market_type='up_down', asset='BTC', interval='15m',
                eventStartTime=start.isoformat(), endDate=(start + timedelta(minutes=15)).isoformat())


def flush(p):
    batch = p.queue.take(5000)
    if batch:
        p.write_batch(batch)
    return batch


def test_repeatable_migration_preserves_legacy_sample_and_indexes(database):
    db, cur = database
    cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,observed_at,asset,interval,expiry_bucket,end_time,source) VALUES ('legacy',NOW(),'BTC','5m','<30s',NOW(),'ws')")
    sql = (Path(__file__).parents[2] / 'migrations' / '20261014_polymarket_recording.sql').read_text()
    cur.execute(sql)
    cur.execute(sql)
    cur.execute("SELECT market_id,chainlink_price,chainlink_observed_at FROM qd_polymarket_observer_samples")
    assert list(cur.fetchone().values()) == ['legacy', None, None]
    cur.execute("SELECT count(*) count FROM pg_indexes WHERE schemaname=current_schema() AND indexname LIKE 'qd_pm_%'")
    assert cur.fetchone()['count'] == 11
    db.commit()


def test_batched_recordings_are_retry_idempotent_and_health_counts_actual_rows(database, monkeypatch):
    db, cur = database
    p = observer.Pipeline()
    p.set_markets([market()])
    monkeypatch.setattr(observer, 'opportunity', lambda *args: None)
    at = datetime.now(timezone.utc)
    stamp = str(int(at.timestamp() * 1000))
    for token in ('u', 'd'):
        p.receive(json.dumps(dict(event_type='book', asset_id=token, timestamp=stamp,
                                  bids=[dict(price='.4', size='2')], asks=[dict(price='.5', size='3')])) )
    p.receive(json.dumps(dict(event_type='last_trade_price', asset_id='u', price='.5', size='4', side='SELL', timestamp=stamp)))
    p.evaluate(10)
    batch = flush(p)
    p.write_batch(batch)  # Ambiguous-commit retry uses stable recording UUIDs.
    for table, expected in [('books', 2), ('trades', 1), ('windows', 1)]:
        cur.execute(f'SELECT count(*) count FROM {recording.PREFIX}{table}')
        assert cur.fetchone()['count'] == expected
        assert p.metrics()['recording_rows_per_minute'][table] == expected
    cur.execute('SELECT * FROM qd_polymarket_observer_books WHERE token_id=%s', ('u',))
    row = cur.fetchone()
    assert row['exchange_at'] == recording.exchange_time(stamp)
    assert row['received_at'] >= row['exchange_at']
    assert row['bids'] == [{'price': '0.4', 'size': '2'}]
    assert p.metrics()['book_age_p95_ms'] >= 0
    db.commit()


def test_exact_twap_boundaries_official_resolution_and_sample_backfill(database):
    db, cur = database
    m = market()
    p = observer.Pipeline()
    p.set_markets([m])
    start = datetime.fromisoformat(m['eventStartTime'])
    end = datetime.fromisoformat(m['endDate'])
    for at, price in [(start - timedelta(seconds=1), '99'), (end - timedelta(seconds=1), '102')]:
        p.queue.put('BTC', 'twap', [(recording.TWAP_SQL, ('BTC', 60, at, at, price, 'chainlink'))])
    p.queue.put('*', 'reference', recording.reference_updates())
    flush(p)
    cur.execute('SELECT price_to_beat,settlement_twap,computed_outcome,outcome_matches FROM qd_polymarket_observer_windows')
    assert list(cur.fetchone().values()) == [None, None, None, None]
    for at, price in [(start, '100.12345678'), (end, '100.12345678')]:
        p.queue.put('BTC', 'twap', [(recording.TWAP_SQL, ('BTC', 60, at, at, price, 'chainlink'))])
    cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,observed_at,asset,interval,expiry_bucket,end_time,source) VALUES ('123',%s,'BTC','15m','<30s',%s,'ws')", (end - timedelta(seconds=1), end))
    db.commit()
    flush(p)
    class Client:
        def _json(self, method, url):
            assert method == 'GET'
            assert url == 'https://gamma-api.polymarket.com/markets/123'
            return dict(id='123', conditionId='condition', closed=True, umaResolutionStatus='resolved',
                        outcomes=['Up', 'Down'], outcomePrices=['1', '0'])
    p.references.poll(Client())
    flush(p)
    cur.execute('SELECT * FROM qd_polymarket_observer_windows')
    row = cur.fetchone()
    assert str(row['price_to_beat']) == '100.12345678'
    assert row['start_twap_at'] == start
    assert row['settlement_twap_at'] == end
    assert row['computed_outcome'] == row['official_outcome'] == 'Up'
    assert row['outcome_matches'] is True
    assert row['resolution']['conditionId'] == 'condition'
    cur.execute('SELECT price_to_beat,chainlink_price FROM qd_polymarket_observer_samples')
    row = cur.fetchone()
    assert str(row['price_to_beat']) == '100.12345678'
    assert row['chainlink_price'] is None  # No contemporaneous observation fabricated.
    cur.execute("UPDATE qd_polymarket_observer_windows SET official_outcome='Down',outcome_matches=NULL")
    db.commit()
    p.queue.put('*', 'reference', recording.reference_updates())
    flush(p)
    cur.execute('SELECT outcome_matches FROM qd_polymarket_observer_windows')
    assert cur.fetchone()['outcome_matches'] is False


def test_sample_current_chainlink_cache_populates_only_fresh_ticks(database, monkeypatch):
    db, cur = database
    now = datetime.now(timezone.utc)
    m = market(now - timedelta(seconds=10))
    p = observer.Pipeline()
    monkeypatch.setattr(observer, '_PIPELINE', p)
    p.recording.twap_cache['BTC'] = ('BTC', 60, now, now, '100.25', 'chainlink')
    books = {token: dict(asks={'.5': '3'}, bids={'.4': '3'}) for token in ('u', 'd')}
    observer._LAST_RESEARCH.pop(m['id'], None)
    p.capture('123', 'research', lambda: observer.record_research(m, books, 'ws', now=now))
    flush(p)
    cur.execute('SELECT chainlink_price,chainlink_observed_at FROM qd_polymarket_observer_samples')
    row = cur.fetchone()
    assert str(row['chainlink_price']) == '100.25'
    assert row['chainlink_observed_at'] == now
    observer._LAST_RESEARCH.pop(m['id'], None)
    p.capture('123', 'research', lambda: observer.record_research(m, books, 'ws', now=now + timedelta(seconds=6)))
    flush(p)
    cur.execute('SELECT chainlink_price FROM qd_polymarket_observer_samples ORDER BY id DESC LIMIT 1')
    assert cur.fetchone()['chainlink_price'] is None
    observer._LAST_RESEARCH.pop(m['id'], None)
    db.commit()


def test_retention_chunks_delete_old_rows_preserve_recent(database, monkeypatch):
    db, cur = database
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_RETENTION_DAYS', '1')
    cur.execute("INSERT INTO qd_polymarket_observer_books SELECT gen_random_uuid(),'123','u',NOW(),NOW()-INTERVAL '2 days','ws','[]'::jsonb,'[]'::jsonb FROM generate_series(1,105)")
    cur.execute("INSERT INTO qd_polymarket_observer_books VALUES (gen_random_uuid(),'123','u',NOW(),NOW(),'ws','[]'::jsonb,'[]'::jsonb)")
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_CLEANUP_BATCH', '100')
    recording.cleanup(cur)
    cur.execute('SELECT count(*) count FROM qd_polymarket_observer_books')
    assert cur.fetchone()['count'] == 6
    recording.cleanup(cur)
    cur.execute('SELECT count(*) count FROM qd_polymarket_observer_books')
    assert cur.fetchone()['count'] == 1
    db.commit()

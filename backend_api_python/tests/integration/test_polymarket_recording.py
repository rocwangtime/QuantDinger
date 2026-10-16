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
                     '20261014_polymarket_backtest.sql', '20261016_polymarket_recording_scope.sql',
                     '20261016_polymarket_recording_scope.sql'):
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


def test_scope_migration_is_repeatable_and_admits_binance_print_windows(database):
    db, cur = database
    cur.execute("INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time) VALUES ('legacy','c','BTC','5m',NOW(),NOW())")
    sql = (Path(__file__).parents[2] / 'migrations' / '20261016_polymarket_recording_scope.sql').read_text()
    cur.execute(sql)
    cur.execute(sql)
    cur.execute("SELECT conname,pg_get_constraintdef(oid) definition FROM pg_constraint WHERE conrelid='qd_polymarket_observer_windows'::regclass AND contype='c'")
    rows = cur.fetchall()
    assert [r['conname'] for r in rows] == ['qd_pm_windows_settlement_window']
    assert '0' in rows[0]['definition'] and '60' in rows[0]['definition']
    cur.execute("SELECT window_seconds FROM qd_polymarket_observer_windows WHERE market_id='legacy'")
    assert cur.fetchone()['window_seconds'] == 60
    cur.execute("INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time,window_seconds) VALUES ('daily','c','SOL','1d',NOW(),NOW(),0)")
    db.commit()
    with pytest.raises(Exception, match='qd_pm_windows_settlement_window'):
        cur.execute("INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time,window_seconds) VALUES ('bad','c','BTC','5m',NOW(),NOW(),30)")
    db.rollback()


def test_wider_scope_records_binance_windows_outside_chainlink_twap_joins(database, monkeypatch):
    db, cur = database
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_ASSETS', 'BTC,SOL')
    monkeypatch.setenv('POLYMARKET_OBSERVER_RECORDING_INTERVALS', '15m,1h')
    monkeypatch.setattr(observer, 'opportunity', lambda *args: None)
    start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    hour = dict(market(start), id='777', conditionId='hour', yesAssetId='hu', noAssetId='hd', interval='1h',
                endDate=(start + timedelta(hours=1)).isoformat(),
                resolutionSource='https://www.binance.com/en/trade/BTC_USDT')
    sol = dict(market(start), id='778', conditionId='sol', yesAssetId='su', noAssetId='sd', asset='SOL',
               resolutionSource='https://data.chain.link/streams/sol-usd-twap-60s-streams')
    p = observer.Pipeline()
    p.set_markets([market(start), hour, sol])
    stamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    for token in ('hu', 'su'):
        p.receive(json.dumps(dict(event_type='book', asset_id=token, timestamp=stamp,
                                  bids=[dict(price='.4', size='2')], asks=[dict(price='.5', size='3')])))
        p.receive(json.dumps(dict(event_type='last_trade_price', asset_id=token, price='.5', size='1', side='BUY', timestamp=stamp)))
    p.evaluate(10)
    for boundary in (start, start + timedelta(minutes=15), start + timedelta(hours=1)):
        p.queue.put('BTC', 'twap', [(recording.TWAP_SQL, ('BTC', 60, boundary, boundary, '100', 'chainlink'))])
    p.queue.put('*', 'reference', recording.reference_updates())
    flush(p)
    cur.execute('SELECT market_id,asset,interval,window_seconds,price_to_beat,settlement_twap FROM qd_polymarket_observer_windows ORDER BY market_id')
    rows = {r['market_id']: r for r in cur.fetchall()}
    assert {k: (r['asset'], r['interval'], r['window_seconds']) for k, r in rows.items()} == {
        '123': ('BTC', '15m', 60), '777': ('BTC', '1h', 0), '778': ('SOL', '15m', 60)}
    # Chainlink TWAP ticks at the hour boundaries never price a Binance-candle window.
    assert (rows['777']['price_to_beat'], rows['777']['settlement_twap']) == (None, None)
    assert str(rows['123']['price_to_beat']) == str(rows['123']['settlement_twap']) == '100'
    for table in ('books', 'trades'):
        cur.execute(f'SELECT market_id FROM {recording.PREFIX}{table} ORDER BY market_id')
        assert [r['market_id'] for r in cur.fetchall()] == ['777', '778']
    db.commit()


def test_daily_proxy_settlement_reads_same_asset_samples_at_candle_closes(database):
    from app.services.polymarket import maker_model as mm
    from app.services.polymarket.maker import RecordedFeed
    db, cur = database
    t0 = datetime(2026, 10, 15, 16, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)
    daily = dict(id='900', asset='BTC', interval='1d', eventStartTime=t0.isoformat(), endDate=t1.isoformat(),
                 resolutionSource='https://www.binance.com/en/trade/BTC_USDT',
                 description='The "Close" price for the Binance 1 minute candle for BTC/USDT 12:00 ET. '
                             'If both candles are equal this market will resolve 50-50.')
    row = dict(market=daily, spec=mm.parse_rules(daily))
    cur.execute("INSERT INTO qd_polymarket_observer_windows(market_id,condition_id,asset,interval,start_time,end_time,window_seconds) VALUES ('900','c','BTC','1d',%s,%s,0)", (t0, t1))
    def sample(market_id, asset, at, price, observed=None):
        cur.execute("INSERT INTO qd_polymarket_observer_samples(market_id,observed_at,asset,interval,expiry_bucket,end_time,source,reference_price,reference_source,reference_observed_at) VALUES (%s,%s,%s,'1h','>10m',%s,'ws',%s,'api.binance.com',%s)",
                    (market_id, observed or at + timedelta(seconds=2), asset, t1, price, at))
    sample('900', 'BTC', t0 - timedelta(seconds=3), '1')          # Boundary itself: a minute early.
    sample('900', 'BTC', t0 + timedelta(seconds=55), '60000')     # Own sample at the opening close.
    sample('900', 'BTC', t1 - timedelta(seconds=3), '1')          # Own last sample before end.
    sample('901', 'BTC', t1 + timedelta(seconds=56), '60050')     # Next daily market, closing candle.
    sample('901', 'BTC', t1 + timedelta(seconds=61), '1')         # After the closing print.
    sample('950', 'ETH', t1 + timedelta(seconds=58), '1')         # Other asset.
    db.commit()
    end = t1.timestamp() * 1000
    assert RecordedFeed.outcome(cur, row, end + 30000) == (None, False)
    assert RecordedFeed.outcome(cur, row, end + 60000) == ('Up', True)
    assert RecordedFeed.outcome(cur, row, end + 60000, 1800) == (None, False)  # Official result may still land.
    assert RecordedFeed.outcome(cur, row, end + 60000 + 1800000, 1800) == ('Up', True)
    sample('902', 'BTC', t1 + timedelta(seconds=59), '60000')
    assert RecordedFeed.outcome(cur, row, end + 60000) == ('Tie', True)
    cur.execute("UPDATE qd_polymarket_observer_windows SET official_outcome='Down'")
    assert RecordedFeed.outcome(cur, row, end + 60000) == ('Down', False)
    db.commit()

"""Observer persistence against the explicitly disposable PostgreSQL database."""
import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.services.polymarket import observer
from app.utils.db import get_db_connection


def _test_dsn(monkeypatch):
    dsn = os.getenv("QD_TEST_POSTGRES_DSN", "")
    if not dsn:
        pytest.skip("QD_TEST_POSTGRES_DSN is required")
    if ("127.0.0.1" not in dsn and "localhost" not in dsn) or "qd_automation_test" not in dsn:
        pytest.fail("observer integration requires localhost qd_automation_test PostgreSQL")
    monkeypatch.setenv("DATABASE_URL", dsn)


def _delete_episode(market_id):
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("DELETE FROM qd_polymarket_observer_episodes WHERE market_id=%s", (market_id,))
            db.commit()
        finally:
            cur.close()


@pytest.mark.integration
def test_episode_persists_and_closes_on_edge_drop(monkeypatch):
    _test_dsn(monkeypatch)
    market_id = "test-" + uuid4().hex
    market = {"id": market_id, "slug": "test", "question": "Observer persistence test"}
    try:
        observer.record_sample(market, {"net_edge": 1, "net_edge_bps": 100, "size_available": 5}, "rest", datetime.now(timezone.utc))
        observer.record_sample(market, {"net_edge": -1, "net_edge_bps": -10, "size_available": 5}, "rest", datetime.now(timezone.utc))
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute("SELECT ended_at,duration_ms FROM qd_polymarket_observer_episodes WHERE market_id=%s", (market_id,))
                row = cur.fetchone()
                ended = row.get("ended_at") if isinstance(row, dict) else row[0]
                assert ended is not None
            finally:
                cur.close()
    finally:
        _delete_episode(market_id)


@pytest.mark.integration
def test_stale_open_episode_is_closed_with_duration(monkeypatch):
    _test_dsn(monkeypatch)
    market_id = "test-" + uuid4().hex
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO qd_polymarket_observer_episodes(market_id,slug,question,started_at,last_seen_at,max_net_edge,max_net_edge_bps,best_size,sample_count,source) VALUES (%s,'test','stale test',NOW()-INTERVAL '5 minutes',NOW()-INTERVAL '2 minutes',1,100,1,1,'rest')", (market_id,))
            db.commit()
        finally:
            cur.close()
    try:
        observer._close_stale_episodes([market_id], 60)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute("SELECT ended_at,last_seen_at,duration_ms FROM qd_polymarket_observer_episodes WHERE market_id=%s", (market_id,))
                row = cur.fetchone()
                values = list(row.values()) if isinstance(row, dict) else row
                assert values[0] == values[1]
                assert values[2] == 180000
            finally:
                cur.close()
    finally:
        _delete_episode(market_id)


@pytest.mark.integration
def test_updown_upgrade_preserves_existing_rows_and_is_repeatable(monkeypatch):
    """Apply both SQL files in an isolated schema containing real legacy rows."""
    from pathlib import Path
    _test_dsn(monkeypatch)
    schema = 'updown_upgrade_' + uuid4().hex
    migrations = Path(__file__).parents[2] / 'migrations'
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(f'CREATE SCHEMA {schema}')
            cur.execute(f'SET LOCAL search_path TO {schema}')
            cur.execute((migrations / '20261011_polymarket_observer.sql').read_text())
            cur.execute("INSERT INTO qd_polymarket_observer_near_misses(market_id,hour_bucket,observed_at,net_edge,net_edge_bps,best_edge_bps,samples) VALUES ('legacy',NOW(),NOW(),-1,-10,-10,7)")
            cur.execute("INSERT INTO qd_polymarket_observer_episodes(market_id,started_at,last_seen_at,max_net_edge,max_net_edge_bps,best_size,source) VALUES ('legacy',NOW(),NOW(),1,100,1,'rest')")
            sql = (migrations / '20261011_polymarket_updown.sql').read_text()
            cur.execute(sql)
            cur.execute(sql)
            cur.execute("SELECT samples,market_type,asset,interval,expiry_bucket,tag_key FROM qd_polymarket_observer_near_misses")
            row = cur.fetchone()
            assert list(row.values()) == [7, 'yes_no', None, None, '', 'yes_no|||']
            cur.execute("SELECT market_type,asset FROM qd_polymarket_observer_episodes")
            assert list(cur.fetchone().values()) == ['yes_no', None]
            cur.execute("INSERT INTO qd_polymarket_observer_near_misses SELECT market_id,hour_bucket,slug,question,observed_at,net_edge,net_edge_bps,best_edge_bps,1,'up_down','BTC','15m','<30s','up_down|BTC|15m|<30s' FROM qd_polymarket_observer_near_misses WHERE market_id='legacy'")
            cur.execute('SELECT count(*) count FROM qd_polymarket_observer_near_misses')
            assert cur.fetchone()['count'] == 2
        finally:
            db.rollback()  # Includes schema and every fixture row.
            cur.close()


@pytest.mark.integration
def test_updown_persistence_bucket_keys_samples_and_api(monkeypatch, app):
    import inspect
    from datetime import timedelta
    from app.routes import polymarket as routes
    from app.services.polymarket import updown
    _test_dsn(monkeypatch)
    market_id = 'test-' + uuid4().hex
    now = datetime.now(timezone.utc)
    market = {'id': market_id, 'slug': 'test', 'question': 'Up/down test', 'market_type': 'up_down', 'asset': 'BTC', 'interval': '15m', 'endDate': (now + timedelta(minutes=3)).isoformat(), 'yesAssetId': 'u', 'noAssetId': 'd'}
    books = {'u': {'asks': {'0.4': '100'}, 'bids': {'0.39': '100'}}, 'd': {'asks': {'0.59': '100'}, 'bids': {'0.58': '100'}}}
    positive = {'net_edge': 1, 'net_edge_bps': 100, 'size_available': 5}
    negative = {'net_edge': -0.01, 'net_edge_bps': -1, 'size_available': 0}
    try:
        observer.record_sample(market, positive, 'ws', now)
        observer.record_sample(market, positive, 'ws', now + timedelta(seconds=70))
        observer.record_sample(market, negative, 'ws', now + timedelta(seconds=71))
        observer.record_sample(market, negative, 'ws', now + timedelta(seconds=130))
        observer.record_research(market, books, 'ws', positive, now)
        observer.record_research(market, books, 'ws', positive, now)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute('SELECT expiry_bucket,ended_at FROM qd_polymarket_observer_episodes WHERE market_id=%s ORDER BY id', (market_id,))
                episodes = cur.fetchall()
                assert [e['expiry_bucket'] for e in episodes] == ['2-5m', '1-2m']
                assert all(e['ended_at'] for e in episodes)
                cur.execute('SELECT tag_key FROM qd_polymarket_observer_near_misses WHERE market_id=%s', (market_id,))
                assert {r['tag_key'] for r in cur.fetchall()} == {'up_down|BTC|15m|1-2m', 'up_down|BTC|15m|30-60s'}
                cur.execute('SELECT * FROM qd_polymarket_observer_samples WHERE market_id=%s', (market_id,))
                samples = cur.fetchall()
                assert len(samples) == 1
                assert samples[0]['up_ask'] == updown.number('0.4')
                assert samples[0]['up_mid'] == updown.number('0.395')
                assert samples[0]['reference_source'] == 'unavailable'
            finally:
                cur.close()
        query = {'days': 1, 'limit': 100, 'offset': 0, 'market_type': 'up_down', 'asset': 'BTC', 'interval': '15m'}
        with app.test_request_context():
            result = inspect.unwrap(routes.observer_summary)(query)[0].get_json()
            assert result['code'] == 1
            assert any(r['expiry_bucket'] == '2-5m' and int(r['episodes']) >= 1 for r in result['data']['groups'])
            result = inspect.unwrap(routes.observer_episodes)({**query, 'expiry_bucket': '2-5m'})[0].get_json()
            assert any(r['market_id'] == market_id for r in result['data'])
    finally:
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                for table in ('episodes', 'near_misses', 'samples'):
                    cur.execute(f'DELETE FROM qd_polymarket_observer_{table} WHERE market_id=%s', (market_id,))
                db.commit()
            finally:
                cur.close()
        observer._LAST_RESEARCH.pop(market_id, None)


@pytest.mark.integration
def test_batched_pipeline_preserves_start_end_start_and_near_max(monkeypatch):
    _test_dsn(monkeypatch)
    identity = 'test-' + uuid4().hex
    market = {'id': identity, 'slug': 'test', 'question': 'writer transaction order'}
    pipeline = observer.Pipeline()
    positive = dict(net_edge=1, net_edge_bps=100, size_available=5)
    negative = dict(net_edge=-.01, net_edge_bps=-1, size_available=0)
    try:
        for quote in (positive, positive, negative, positive, negative, {**negative, 'net_edge_bps': -10}):
            pipeline.capture(identity, 'observation', lambda: observer.record_sample(market, quote, 'ws'))
        batch = pipeline.queue.take(500)
        pipeline.write_batch(batch)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute('SELECT sample_count,ended_at FROM qd_polymarket_observer_episodes WHERE market_id=%s ORDER BY id', (identity,))
                rows = cur.fetchall()
                assert [r['sample_count'] for r in rows] == [2, 1]
                assert all(r['ended_at'] for r in rows)
                cur.execute('SELECT samples,best_edge_bps FROM qd_polymarket_observer_near_misses WHERE market_id=%s', (identity,))
                row = cur.fetchone()
                assert row['samples'] == 3
                assert row['best_edge_bps'] == -1
                assert pipeline.metrics()['written'] == 9
            finally:
                cur.close()
    finally:
        _delete_episode(identity)
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute('DELETE FROM qd_polymarket_observer_near_misses WHERE market_id=%s', (identity,))
            db.commit()
            cur.close()


@pytest.mark.integration
def test_ws_daily_migration_and_incremental_idempotent_upsert(monkeypatch):
    from pathlib import Path
    from app.services.polymarket import ws_balance, account_api
    _test_dsn(monkeypatch)
    schema = 'ws_daily_' + uuid4().hex
    sql = (Path(__file__).parents[2] / 'migrations' / '20261013_polymarket_ws_daily.sql').read_text()
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute('SHOW server_version_num')
        assert int(next(iter(cur.fetchone().values()))) // 10000 == 18
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET search_path TO {schema}')
        cur.execute(sql)
        cur.execute(sql)
        cur.execute('CREATE TABLE qd_polymarket_paper_daily(account_id text, day date, stats jsonb)')
        db.commit()
        class Connection:
            def __enter__(self):
                return db
            def __exit__(self, *args):
                if args[0]:
                    db.rollback()
        monkeypatch.setattr(ws_balance, 'get_db_connection', Connection)
        monkeypatch.setattr(account_api, 'get_db_connection', Connection)
        first, second = ws_balance.DailyStats(), ws_balance.DailyStats()
        day = '2026-10-13'
        try:
            first.increment('reconnect_count', day=day)
            first.increment('message', shard=0, day=day)
            first.persist()
            first.dirty.add(day)  # Simulate retry after an ambiguous commit.
            first.persist()
            first.increment('message', shard=0, day=day)
            first.increment('slow_consumer_count', day=day)
            first.persist()
            second.increment('rebalance_count', day=day)
            second.increment('message', shard=1, day=day)
            second.persist()
            report = account_api.reports(day=day)[0]
            assert report['ws_stats'] == dict(reconnect_count=1, slow_consumer_count=1,
                                             rebalance_count=1, shard_msg_totals={'0': 2, '1': 1})
            assert not first.dirty
        finally:
            db.rollback()
            cur.execute('SET search_path TO public')
            cur.execute(f'DROP SCHEMA {schema} CASCADE')
            db.commit()
            cur.close()

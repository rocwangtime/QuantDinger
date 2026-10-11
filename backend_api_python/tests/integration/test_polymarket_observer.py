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

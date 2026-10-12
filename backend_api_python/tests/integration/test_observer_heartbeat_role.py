"""Upgrade the production heartbeat constraint in an isolated local PG18 schema."""
import os
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import psycopg2
import pytest
from flask import Flask
from psycopg2.extras import RealDictCursor

from app.openapi.routes import health


@pytest.mark.integration
def test_observer_heartbeat_migration_and_worker_health(monkeypatch):
    dsn = os.getenv("QD_TEST_POSTGRES_DSN", "")
    if not dsn:
        pytest.skip("QD_TEST_POSTGRES_DSN is required")
    parsed = urlparse(dsn)
    assert parsed.hostname in {"localhost", "127.0.0.1"}
    assert parsed.port == 5432 and parsed.path == "/qd_automation_test"
    migrations = Path(__file__).resolve().parents[2] / "migrations"
    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version_num")
            assert 180000 <= int(cur.fetchone()["server_version_num"]) < 190000
            schema = "heartbeat_test_" + uuid4().hex
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET LOCAL search_path TO {schema}")
            cur.execute((migrations / "20260713_process_roles_and_strategy_commands.sql").read_text())
            cur.execute("INSERT INTO qd_worker_heartbeats(worker_id, role) VALUES ('existing', 'scheduler')")
            sql = (migrations / "20261013_polymarket_observer_heartbeat_role.sql").read_text()
            cur.execute(sql)
            cur.execute(sql)
            cur.execute("INSERT INTO qd_worker_heartbeats(worker_id, role) VALUES ('observer', 'polymarket-observer')")

        @contextmanager
        def connection():
            yield conn

        monkeypatch.setattr(health, "get_db_connection", connection)
        app = Flask(__name__)
        app.register_blueprint(health.blp)
        response = app.test_client().get("/api/health/workers")
        assert response.status_code == 200
        workers = {row["role"]: row for row in response.get_json()["workers"]}
        assert workers["polymarket-observer"]["healthy"] == 1
        assert workers["polymarket-observer"]["total"] == 1
        assert workers["scheduler"]["healthy"] == 1
    finally:
        conn.rollback()
        conn.close()

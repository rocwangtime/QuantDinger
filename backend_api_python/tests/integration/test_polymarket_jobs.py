"""PostgreSQL tenant isolation, concurrent admission, retries and evidence.

QD_TEST_POSTGRES_DSN must point to a migrated disposable database. Tests create
and drop only their own schema and never use configured application accounts.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.services.billing_service import BillingError
from app.services.polymarket import jobs
from app.services.polymarket.engine import digest
from app.utils import agent_jobs, db_postgres as pg


@pytest.fixture
def store(monkeypatch):
    dsn = os.getenv("QD_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("QD_TEST_POSTGRES_DSN is required")
    import psycopg2
    from psycopg2 import sql
    from psycopg2.pool import ThreadedConnectionPool
    schema = "qd_polymarket_test_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("CREATE TABLE {}.qd_agent_jobs (LIKE public.qd_agent_jobs INCLUDING DEFAULTS INCLUDING INDEXES)").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("CREATE SEQUENCE {}.job_ids").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("ALTER TABLE {}.qd_agent_jobs ALTER COLUMN id SET DEFAULT nextval(%s)").format(sql.Identifier(schema)), (schema + ".job_ids",))
    pool = ThreadedConnectionPool(1, 8, dsn, options=f"-c search_path={schema}")
    monkeypatch.setattr(pg, "_get_connection_pool", lambda: pool)
    monkeypatch.setattr(pg, "_acquire_conn_with_wait", lambda p: p.getconn())
    monkeypatch.setenv("CELERY_TASKS_ENABLED", "false")
    dispatched = []
    monkeypatch.setattr(agent_jobs, "_get_executor", lambda: SimpleNamespace(submit=lambda runner: dispatched.append(runner)))
    try:
        yield dispatched
    finally:
        pool.closeall()
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        admin.close()


def submit(key="same", user=7, **payload):
    return agent_jobs.submit_job(user_id=user, agent_token_id=None, kind="polymarket_scan",
                                request_payload={"settings": {}, **payload}, runner=lambda data: {}, idempotency_key=key)


def test_concurrent_human_retries_dispatch_exactly_one_job(store):
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: submit(), range(4)))
    assert len({row["job_id"] for row in results}) == 1
    assert len(store) == 1
    assert sum(row.get("duplicate", False) for row in results) == 3


def test_idempotency_conflict_and_tenant_keys_are_distinct(store):
    first = submit()
    with pytest.raises(BillingError, match="IDEMPOTENCY_CONFLICT"):
        submit(durationSeconds=30)
    other = submit(user=8)
    assert first["job_id"] != other["job_id"]
    with pytest.raises(ValueError, match="notFound"):
        jobs.owned_job(first["job_id"], 8)
    assert len(jobs.list_jobs(7, "polymarket_scan")) == 1


def test_concurrent_admission_cannot_exceed_two_jobs_and_duplicate_still_recovers(store):
    def admit(index):
        try:
            return submit(str(index))
        except BillingError as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(admit, range(4)))
    admitted = [row for row in results if isinstance(row, dict)]
    assert len(admitted) == len(store) == 2
    assert results.count("polymarket.tooManyActiveJobs") == 2
    rows = jobs.list_jobs(7, "polymarket_scan")
    matching = next(index for index, result in enumerate(results) if isinstance(result, dict))
    assert submit(str(matching))["duplicate"]
    agent_jobs.cancel_job(rows[0]["job_id"], user_id=7)
    assert submit("after-cancel")["status"] == "queued"


def test_cancelled_job_cannot_be_started_or_overwritten_with_profit(store):
    row = submit()
    identifier = row["job_id"]
    assert agent_jobs.cancel_job(identifier, user_id=8) is None
    assert agent_jobs.cancel_job(identifier, user_id=7)["status"] == "cancelled"
    assert not agent_jobs._set_status(identifier, "running")
    assert not agent_jobs._set_result(identifier, {"realizedPnl": 100})
    assert jobs.owned_job(identifier, 7)["status"] == "cancelled"


def test_evidence_is_durable_hash_verified_and_omitted_from_history(store):
    bundle = {"engineVersion": "test", "frames": {"books": ["frozen"]}}
    receipt = agent_jobs.record_completed_job(user_id=7, agent_token_id=None, kind="polymarket_paper", request_payload={},
                                              result={"bundle": bundle, "bundleHash": digest(bundle), "status": "merged"})
    assert jobs.bundle_from_job(receipt["job_id"], 7, "polymarket_paper") == bundle
    row = jobs.list_jobs(7, "polymarket_paper")[0]
    assert "bundle" not in row["result"] and row["result"]["bundleHash"] == digest(bundle)
    with pytest.raises(ValueError, match="notFound"):
        jobs.bundle_from_job(receipt["job_id"], 8, "polymarket_paper")

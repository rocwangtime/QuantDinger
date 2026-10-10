"""Real PostgreSQL isolation, concurrency and failure recovery; no broker calls.

Set QD_TEST_POSTGRES_DSN to a migrated disposable PostgreSQL instance. Each test
uses its own schema; neither DATABASE_URL nor existing application rows are used.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.services import order_groups as groups
from app.services.fundamental_data import FundamentalDataService
from app.services.strategy_evolution.history import ResearchHistory
from app.utils import db_postgres as pg


@pytest.fixture
def ledger(monkeypatch):
    dsn = os.environ.get("QD_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("QD_TEST_POSTGRES_DSN is required")
    import psycopg2
    from psycopg2 import sql
    from psycopg2.pool import ThreadedConnectionPool
    schema = "qd_research_test_" + uuid4().hex
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    tables = ["qd_users", "qd_strategies_trading", "qd_strategy_positions", "strategy_runs", "strategy_order_intents", "strategy_order_fills",
              "pending_orders", "strategy_runtime_events", "qd_order_groups", "qd_strategy_virtual_accounts", "qd_strategy_virtual_orders",
              "qd_strategy_virtual_positions", "qd_strategy_virtual_trades", "qd_strategy_runtime_leases",
              "qd_strategy_commands", "qd_research_studies", "qd_fundamental_snapshots", "qd_fundamental_revisions", "qd_ai_decisions"]
    with admin.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        for table in tables:
            cur.execute(sql.SQL("CREATE TABLE {}.{} (LIKE public.{} INCLUDING DEFAULTS INCLUDING INDEXES)").format(
                sql.Identifier(schema), sql.Identifier(table), sql.Identifier(table)))
            cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name='id'",
                        (schema, table))
            if cur.fetchone():
                cur.execute(sql.SQL("CREATE SEQUENCE {}.{}").format(sql.Identifier(schema), sql.Identifier(table + "_ids")))
                cur.execute(sql.SQL("ALTER TABLE {}.{} ALTER COLUMN id SET DEFAULT nextval(%s)").format(
                    sql.Identifier(schema), sql.Identifier(table)), (schema + "." + table + "_ids",))
        cur.execute(sql.SQL("CREATE TRIGGER revisions AFTER INSERT OR UPDATE ON {}.qd_fundamental_snapshots "
                            "FOR EACH ROW EXECUTE FUNCTION public.qd_archive_fundamental_revision()").format(sql.Identifier(schema)))
    pool = ThreadedConnectionPool(1, 8, dsn, options=f"-c search_path={schema}")
    monkeypatch.setattr(pg, "_get_connection_pool", lambda: pool)
    monkeypatch.setattr(pg, "_acquire_conn_with_wait", lambda p: p.getconn())

    def query(statement, params=()):
        with pg.get_pg_connection() as db:
            cur = db.cursor()
            cur.execute(statement, params)
            rows = cur.fetchall() if cur._cursor.description else []
            db.commit()
            cur.close()
            return rows
    query("INSERT INTO qd_users(id,username,password_hash) VALUES(1,'research','test'),(2,'other','test') RETURNING id")
    manifest = {"universe": {"instruments": [{"symbol": "BTC/USDT"}, {"symbol": "ETH/USDT"}]}}
    query("INSERT INTO qd_strategies_trading(id,user_id,strategy_name,symbol,status,execution_mode,market_type," 
          "initial_capital,trading_config) VALUES(1,1,'group-test','basket:test','stopped','signal','swap',10000,%s::jsonb) RETURNING id",
          (json.dumps({"strategy_manifest": manifest}),))
    query("INSERT INTO strategy_runs(id,strategy_id,user_id,runtime_status) VALUES(1,1,1,'stopped') RETURNING id")
    query("SELECT setval(%s,1)", (schema + ".strategy_runs_ids",))
    yield query
    pool.closeall()
    with admin.cursor() as cur:
        cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    admin.close()


def payload():
    return {"strategyId": 1, "strategyRunId": 1, "maxGrossNotional": 500, "timeoutSeconds": 300,
            "legs": [{"symbol": "BTC/USDT", "action": "open_long", "quantity": 1, "referencePrice": 100},
                     {"symbol": "ETH/USDT", "action": "open_short", "quantity": 2, "referencePrice": 50}]}


def create(**overrides):
    return groups.create_group(user_id=1, idempotency_key="test-key", payload={**payload(), **overrides})


def advance(group):
    return groups.advance_group(user_id=1, group_id=group["group_id"])


def test_concurrent_duplicate_submission_queues_exactly_two_legs(ledger):
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: create(), range(4)))
    assert len({x["group_id"] for x in results}) == 1
    assert ledger("SELECT count(*) AS count FROM pending_orders")[0]["count"] == 2
    assert {x["status"] for x in ledger("SELECT status FROM pending_orders")} == {"group_waiting"}
    with pytest.raises(ValueError, match="idempotencyConflict"):
        create(maxGrossNotional=600)


def test_fresh_virtual_strategy_creates_a_group_run_without_starting_executor(ledger):
    group = create(strategyRunId=0)
    assert group["config"]["strategyRunId"] > 0
    assert ledger("SELECT runtime_status FROM strategy_runs WHERE id=%s", (group["config"]["strategyRunId"],))[0]["runtime_status"] == "order_group"
    assert ledger("SELECT status FROM qd_strategies_trading WHERE id=1")[0]["status"] == "stopped"
    assert create(strategyRunId=0)["group_id"] == group["group_id"]


def test_restart_and_concurrent_ticks_do_not_duplicate_fills(ledger):
    group = create()
    first = advance(group)
    assert first["state"]["status"] == "executing" and len(first["state"]["residuals"]) == 1
    # Functions have no in-memory state. A fresh caller resumes the durable group.
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda _: advance(group), range(3)))
    assert all(x["state"]["status"] == "completed" for x in results)
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_trades")[0]["count"] == 2
    assert ledger("SELECT count(*) AS count FROM strategy_order_fills")[0]["count"] == 2
    assert create()["state"]["status"] == "completed"


def test_cancel_after_first_fill_requires_review_and_unwind_is_idempotent(ledger):
    group = create()
    advance(group)
    cancelled = groups.cancel_group(user_id=1, group_id=group["group_id"])
    assert cancelled["state"]["status"] == "needs_review"
    assert cancelled["state"]["legs"][1]["status"] == "cancelled"
    prices = {"BTC/USDT": 90}
    unwind = groups.unwind_group(user_id=1, group_id=group["group_id"], reference_prices=prices)
    duplicate = groups.unwind_group(user_id=1, group_id=group["group_id"], reference_prices=prices)
    assert duplicate["state"] == unwind["state"]
    finished = advance(group)
    assert finished["state"]["status"] == "unwound" and not finished["state"]["residuals"]
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_positions")[0]["count"] == 0
    account = ledger("SELECT * FROM qd_strategy_virtual_accounts")[0]
    assert float(account["realized_pnl"]) < -10 and float(account["total_commission"]) > 0
    assert advance(group)["state"]["status"] == "unwound"


def test_failed_leg_rolls_back_accounting_and_stops_remaining_work(ledger, monkeypatch):
    group = create()
    advance(group)
    real_fill = groups.execute_virtual_signal_order
    def interrupted(row, data):
        real_fill(row, data)
        raise RuntimeError("crash after ledger writes")
    monkeypatch.setattr(groups, "execute_virtual_signal_order", interrupted)
    result = advance(group)
    assert result["state"]["status"] == "needs_review"
    assert result["state"]["legs"][1]["filled"] == 0
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_trades")[0]["count"] == 1
    assert ledger("SELECT count(*) AS count FROM strategy_order_fills")[0]["count"] == 1
    assert result["state"]["legs"][1]["status"] == "cancelled"


def test_deadline_never_claims_next_leg_and_retains_exposure(ledger):
    group = create()
    advance(group)
    ledger("UPDATE qd_order_groups SET state=jsonb_set(state,'{deadline}',to_jsonb(%s::text)) WHERE group_id=%s",
           ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), group["group_id"]))
    result = advance(group)
    assert result["state"]["reason"] == "deadlineExceeded" and result["state"]["status"] == "needs_review"
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_trades")[0]["count"] == 1


def test_terminal_partial_fill_preserves_actual_residual_quantity(ledger):
    group = create()
    advance(group)
    entry = group["state"]["legs"][0]
    ledger("UPDATE qd_strategy_virtual_orders SET fill_qty=.4,status='filled' WHERE pending_order_id=%s", (entry["pendingId"],))
    result = groups.cancel_group(user_id=1, group_id=group["group_id"])
    assert result["state"]["residuals"][0]["quantity"] == .4


def test_access_account_mode_and_symbol_boundaries(ledger):
    with pytest.raises(ValueError, match="virtualOnly"):
        create(executionMode="live")
    bad = payload()["legs"]
    bad[0]["symbol"] = "DOGE/USDT"
    with pytest.raises(ValueError, match="symbolOutsideStrategy"):
        create(legs=bad)
    group = create()
    for call in [groups.get_group, groups.cancel_group]:
        with pytest.raises(ValueError, match="notFound"):
            call(user_id=2, group_id=group["group_id"])
    ledger("UPDATE qd_strategies_trading SET execution_mode='live' WHERE id=1")
    assert advance(group)["state"]["status"] == "cancelled"
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_trades")[0]["count"] == 0


def test_compensation_failure_can_resume_without_reducing_twice(ledger, monkeypatch):
    group = create()
    advance(group)
    groups.cancel_group(user_id=1, group_id=group["group_id"])
    prices = {"BTC/USDT": 100}
    groups.unwind_group(user_id=1, group_id=group["group_id"], reference_prices=prices)
    with monkeypatch.context() as patch:
        patch.setattr(groups, "execute_virtual_signal_order", lambda *a: (_ for _ in ()).throw(RuntimeError("temporary")))
        assert advance(group)["state"]["status"] == "needs_review"
    groups.unwind_group(user_id=1, group_id=group["group_id"], reference_prices=prices)
    assert advance(group)["state"]["status"] == "unwound"
    assert ledger("SELECT count(*) AS count FROM qd_strategy_virtual_trades")[0]["count"] == 2


def test_research_history_counts_concurrent_runs_and_overlapping_holdouts(ledger):
    def reserve(index):
        history = ResearchHistory(user_id=1, source_id=9, study_id=f"study-{index}")
        return history.reserve(code_hash="a" * 64, trials=10)
    with ThreadPoolExecutor(max_workers=4) as executor:
        previous = list(executor.map(reserve, range(4)))
    assert sorted(previous) == [0, 10, 20, 30]
    history = ResearchHistory(user_id=1, source_id=9, study_id="study-0")
    assert history.reserve(code_hash="a" * 64, trials=10) == 30  # retry doesn't double count
    assert history.expose_holdout(datetime(2025, 1, 1), datetime(2025, 3, 1)) == 0
    other = ResearchHistory(user_id=1, source_id=9, study_id="study-1")
    assert other.expose_holdout(datetime(2025, 2, 1), datetime(2025, 4, 1)) == 1
    isolated = ResearchHistory(user_id=2, source_id=9, study_id="isolated")
    assert isolated.reserve(code_hash="a" * 64, trials=10) == 0
    assert isolated.expose_holdout(datetime(2025, 2, 1), datetime(2025, 4, 1)) == 0


def test_group_reservation_blocks_executor_and_other_orders_until_flat(ledger):
    from app.services.strategy_command_repository import StrategyCommandRepository
    from app.services.strategy_v2.live_execution import LiveOrderRequest, StrategyV2OrderGateway
    group = create()
    from app.services.strategy import StrategyService
    with pytest.raises(ValueError, match="strategyReserved"):
        StrategyService().update_strategy_status(1, "running", user_id=1)
    with pytest.raises(ValueError, match="strategyReserved"):
        StrategyService().delete_strategy(1, user_id=1)
    repository = StrategyCommandRepository()
    assert repository.acquire_strategy_lease(strategy_id=1, owner_id="test", lease_seconds=30) is None
    request = LiveOrderRequest(strategy_id=1, strategy_run_id=1, user_id=1, symbol="BTC/USDT", action="open_long",
                               quantity=1, reference_price=100, signal_timestamp=1, market_type="swap", execution_mode="signal")
    with pytest.raises(ValueError, match="strategyReserved"):
        StrategyV2OrderGateway().submit(request)
    advance(group)
    with pytest.raises(ValueError, match="residualPositionRemains"):
        groups.resolve_group(user_id=1, group_id=group["group_id"], reason="closed manually")
    # Simulate an operator's independently reconciled closure, not an automatic group order.
    ledger("DELETE FROM qd_strategy_virtual_positions WHERE strategy_id=1")
    resolved = groups.resolve_group(user_id=1, group_id=group["group_id"], reason="reconciled manual closure")
    assert resolved["state"]["status"] == "resolved" and not resolved["state"]["residuals"]
    assert resolved["state"]["exposureBeforeResolution"]
    assert groups.cancel_group(user_id=1, group_id=group["group_id"])["state"]["status"] == "resolved"
    assert repository.acquire_strategy_lease(strategy_id=1, owner_id="test", lease_seconds=30) == 1


def test_covariance_admission_serializes_and_reserves_queued_risk(ledger):
    from app.services.portfolio.execution_risk import enforce_portfolio_entry
    model = {"symbols": ["BTC/USDT"], "covariance": [[.0004]], "as_of": datetime.now(timezone.utc).isoformat(), "period": "daily"}
    policy = {"portfolio_model": model, "max_portfolio_daily_volatility": .03}
    ledger("UPDATE qd_strategies_trading SET execution_mode='live',exchange_config=%s::jsonb,trading_config=%s::jsonb WHERE id=1",
           (json.dumps({"credential_id": 5}), json.dumps({"portfolio_risk": policy, "quote_currency": "USDT"})))
    def admit(index):
        try:
            with pg.get_pg_transaction() as db:
                enforce_portfolio_entry(user_id=1, strategy_id=1, action="open_long", symbol="BTC/USDT", quantity=100, price=100)
                cur = db.cursor()
                cur.execute("INSERT INTO pending_orders(user_id,strategy_id,symbol,signal_type,amount,price,execution_mode,status,idempotency_key) "
                            "VALUES(1,1,'BTC/USDT','open_long',100,100,'live','pending',%s) RETURNING id", (f"risk-{index}",))
                cur.close()
            return "admitted"
        except ValueError as exc:
            return str(exc)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(admit, range(2)))
    assert sorted(results) == ["admitted", "portfolioRisk.volatilityExceeded"]
    assert ledger("SELECT count(*) AS count FROM pending_orders")[0]["count"] == 1
    # Reductions bypass model availability and never need an admission reservation.
    enforce_portfolio_entry(user_id=1, strategy_id=1, action="reduce_long", symbol="BTC/USDT", quantity=100, price=100)


def test_gateway_risk_retry_returns_existing_pending_without_double_reservation(ledger):
    from dataclasses import replace
    from app.services.strategy_v2.live_execution import LiveOrderRequest, StrategyV2OrderGateway
    model = {"symbols": ["BTC/USDT"], "covariance": [[.0004]], "as_of": datetime.now(timezone.utc).isoformat(), "period": "daily"}
    policy = {"portfolio_model": model, "max_portfolio_daily_volatility": .03}
    ledger("UPDATE qd_strategies_trading SET execution_mode='live',exchange_config=%s::jsonb,trading_config=%s::jsonb WHERE id=1",
           (json.dumps({"credential_id": 5}), json.dumps({"portfolio_risk": policy, "quote_currency": "USDT"})))
    request = LiveOrderRequest(strategy_id=1, strategy_run_id=1, user_id=1, symbol="BTC/USDT", action="open_long",
        quantity=100, reference_price=100, signal_timestamp=1, market_type="swap", execution_mode="live",
        portfolio_risk=policy, client_order_id="risk-retry")
    gateway = StrategyV2OrderGateway()
    pending = gateway.submit(request)
    assert pending and gateway.submit(request) == pending
    with pytest.raises(ValueError, match="volatilityExceeded"):
        gateway.submit(replace(request, client_order_id="new-risk"))
    assert ledger("SELECT count(*) AS count FROM pending_orders")[0]["count"] == 1
    assert ledger("SELECT count(*) AS count FROM strategy_order_intents")[0]["count"] == 1


def test_account_guard_rejects_mixed_or_unknown_currency_without_fx_valuation(ledger):
    from app.services.portfolio.execution_risk import enforce_portfolio_entry
    model = {"symbols": ["BTC/USDT"], "covariance": [[.0004]], "as_of": datetime.now(timezone.utc).isoformat(), "period": "daily"}
    policy = {"portfolio_model": model, "max_portfolio_daily_volatility": .03}
    ledger("UPDATE qd_strategies_trading SET execution_mode='live',exchange_config=%s::jsonb,trading_config=%s::jsonb WHERE id=1",
           (json.dumps({"credential_id": 5}), json.dumps({"portfolio_risk": policy})))
    kwargs = dict(user_id=1, strategy_id=1, action="open_long", symbol="BTC/USDT", quantity=1, price=100)
    with pytest.raises(ValueError, match="valuationCurrencyMissing"), pg.get_pg_transaction():
        enforce_portfolio_entry(**kwargs)
    ledger("UPDATE qd_strategies_trading SET trading_config=trading_config || '{\"quote_currency\":\"USDT\"}'::jsonb WHERE id=1")
    ledger("INSERT INTO qd_strategies_trading(id,user_id,strategy_name,status,execution_mode,market_type,initial_capital," 
           "exchange_config,trading_config) VALUES(2,1,'other currency','running','live','swap',10000," 
           "'{\"credential_id\":5}'::jsonb,'{\"quote_currency\":\"USD\"}'::jsonb) RETURNING id")
    with pytest.raises(ValueError, match="mixedValuationCurrencyUnsupported"), pg.get_pg_transaction():
        enforce_portfolio_entry(**kwargs)
    # A currency/model problem must not block a reduction.
    enforce_portfolio_entry(**{**kwargs, "action": "reduce_long"})


def test_fundamental_corrections_preserve_recorded_knowledge_time(ledger):
    ledger("INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,net_income) "
           "VALUES('USStock','AAPL','2025-01-01','2025-02-01',5) RETURNING id")
    cutoff = ledger("SELECT clock_timestamp() AS time")[0]["time"]
    ledger("UPDATE qd_fundamental_snapshots SET net_income=10 WHERE symbol='AAPL'")
    old = FundamentalDataService.load_recorded_revision(market="USStock", symbol="AAPL", recorded_as_of=cutoff,
                                                       available_until=date(2025, 2, 2))
    current = FundamentalDataService.load_recorded_revision(market="USStock", symbol="AAPL",
        recorded_as_of=ledger("SELECT clock_timestamp() AS time")[0]["time"], available_until=date(2025, 2, 2))
    assert old[0]["net_income"] == 5 and current[0]["net_income"] == 10
    assert FundamentalDataService.load_recorded_revision(market="USStock", symbol="AAPL", recorded_as_of=cutoff,
                                                       available_until=date(2025, 1, 2)) == []


@pytest.mark.parametrize("mode,expected", [("shadow", True), ("required", False)])
def test_audit_database_failure_does_not_poison_shadow_order_transaction(ledger, monkeypatch, mode, expected):
    from app.services.ai_decision_filter import AIDecisionFilter, AIDecisionRequest, AIDecisionResult
    service = AIDecisionFilter()
    monkeypatch.setattr(service, "_evaluate", lambda *a, **k: AIDecisionResult(
        allowed=True, decision="pass", provider="llm", reason="test", decision_id="audit-test"))
    ledger("DROP TABLE qd_ai_decisions")
    with pg.get_pg_transaction() as db:
        result = service.evaluate(AIDecisionRequest(user_id=1, source_type="strategy", symbol="BTC/USDT",
                                                   action="open_long", mode=mode), enabled=True)
        assert result.allowed is expected
        cur = db.cursor()
        cur.execute("UPDATE qd_strategies_trading SET strategy_name='transaction survived' WHERE id=1")
        cur.close()
    assert ledger("SELECT strategy_name FROM qd_strategies_trading WHERE id=1")[0]["strategy_name"] == "transaction survived"

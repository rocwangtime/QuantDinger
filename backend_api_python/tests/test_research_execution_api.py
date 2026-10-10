import inspect
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
from flask import g

from app.routes import portfolio_research, strategy_ai_decision_routes, strategy_evolution
from app.services import ai_evaluation
from app.services.strategy_v2 import deployment
from app.services.strategy_evolution.bundles import content_hash, runtime_identity


def connection(rows):
    @contextmanager
    def connect():
        cursor = SimpleNamespace(execute=lambda *a: None, fetchone=lambda: rows[0] if rows else None,
                                 fetchall=lambda: rows, close=lambda: None)
        yield SimpleNamespace(cursor=lambda: cursor)
    return connect


def test_openapi_contains_new_inputs_and_authenticated_routes(app):
    from app.openapi import get_openapi_api
    paths = get_openapi_api(app).spec.to_dict()["paths"]
    for path in ["/api/portfolio/risk-analysis", "/api/portfolio/order-groups",
                 "/api/portfolio/order-groups/{group_id}/unwind", "/api/portfolio/order-groups/{group_id}/resolve",
                 "/api/strategies/{strategy_id}/ai-evaluation", "/api/strategy-evolution/jobs/{job_id}/replay"]:
        assert path in paths
    assert "requestBody" in paths["/api/portfolio/order-groups"]["post"]
    for path, method in [("/api/strategies", "post"), ("/api/strategies/{strategy_id}", "put")]:
        fields = paths[path][method]["requestBody"]["content"]["application/json"]["schema"]["properties"]
        assert {"aiDecisionMode", "portfolioRisk", "researchEvidenceJobId"} <= fields.keys()


def test_order_group_route_uses_authenticated_owner_and_idempotency_header(app, monkeypatch):
    received = {}
    monkeypatch.setattr(portfolio_research.order_groups, "create_group", lambda **kw: received.update(kw) or {"group_id": "test"})
    with app.test_request_context(headers={"Idempotency-Key": "client-1"}):
        g.user_id = 23
        response = inspect.unwrap(portfolio_research.create_virtual_order_group)({"strategyId": 1, "userId": 999})
    assert received["user_id"] == 23 and received["idempotency_key"] == "client-1"
    assert response.get_json()["code"] == 1


def test_public_deployment_accepts_modes_evidence_and_covariance_policy():
    from app.services.strategy import StrategyService
    payload = {"sourceId": 9, "aiDecisionMode": "shadow", "researchEvidenceJobId": "study",
               "portfolioRisk": {"max_portfolio_daily_volatility": .03}}
    assert StrategyService._deployment_payload(payload) == payload


def test_shadow_job_and_replay_are_owner_and_kind_scoped(app, monkeypatch):
    monkeypatch.setattr(strategy_ai_decision_routes.agent_jobs, "get_job", lambda job_id, user_id: None)
    with app.test_request_context():
        g.user_id = 23
        assert inspect.unwrap(strategy_ai_decision_routes.get_strategy_ai_evaluation)("other-job")[1] == 404
        assert inspect.unwrap(strategy_evolution.replay_strategy_evolution)("other-job")[1] == 404
    monkeypatch.setattr(strategy_ai_decision_routes.agent_jobs, "get_job", lambda *a, **k: {"kind": "backtest"})
    with app.test_request_context():
        g.user_id = 23
        assert inspect.unwrap(strategy_ai_decision_routes.get_strategy_ai_evaluation)("wrong-kind")[1] == 404
    monkeypatch.setattr(strategy_ai_decision_routes, "get_strategy_service", lambda: SimpleNamespace(get_strategy=lambda *a, **k: None))
    with app.test_request_context():
        g.user_id = 23
        assert inspect.unwrap(strategy_ai_decision_routes.submit_strategy_ai_evaluation)({}, 1)[1] == 404


def test_replay_ignores_body_override_and_keeps_frozen_request(app, monkeypatch):
    original = {"sourceId": 9, "__frozenSource": {"code": "original"}, "config": {"trials": 10}}
    received = {}
    monkeypatch.setattr(strategy_evolution.agent_jobs, "get_job", lambda *a, **k: {
        "kind": "strategy_evolution", "request": original, "result": {"reproducibility": {"bundleId": "a" * 64}}})
    monkeypatch.setattr(strategy_evolution.agent_jobs, "submit_job", lambda **kw: received.update(kw) or {"job_id": "replay", "status": "queued"})
    with app.test_request_context(method="POST", json={"sourceId": 999, "config": {"trials": 100}}):
        g.user_id = 23
        assert inspect.unwrap(strategy_evolution.replay_strategy_evolution)("owned")[1] == 202
    request = received["request_payload"]
    assert received["user_id"] == request["__userId"] == 23
    assert request["__frozenSource"] == original["__frozenSource"] and request["config"] == original["config"]


def test_linked_deployment_checks_source_parameters_runtime_and_history(monkeypatch):
    from app.utils import agent_jobs
    source = {"id": 9, "code": "frozen"}
    evidence = {"historyTracked": True, "sourceFrozenAtSubmission": True, "bundleId": "a" * 64,
                "sourceHash": content_hash("frozen"), "runtime": runtime_identity(), "studyId": "study"}
    assumptions = {"initialCapital": 10000, "leverage": 1, "commission": .001, "slippage": .001}
    result = {"promotion": {"eligible": True}, "source": {"id": 9}, "bestParams": {"p": 1},
              "reproducibility": evidence, "economicAssumptions": assumptions}
    monkeypatch.setattr(agent_jobs, "get_job", lambda job_id, user_id: {"kind": "strategy_evolution", "status": "succeeded", "result": result}
                        if user_id == 23 else None)
    monkeypatch.setattr(deployment, "get_db_connection", connection([{"stale": False}]))
    validate = deployment.StrategyV2DeploymentService._validate_research_evidence
    kwargs = dict(user_id=23, source=source, params={"p": 1}, job_id="owned")
    assert validate(**kwargs) == assumptions
    for override in [{"params": {"p": True}}, {"source": {"id": 9, "code": "edited"}}, {"user_id": 999}]:
        with pytest.raises(ValueError):
            validate(**{**kwargs, **override})
    monkeypatch.setattr(deployment, "get_db_connection", connection([{"stale": True}]))
    with pytest.raises(ValueError, match="researchEvidenceStale"):
        validate(**kwargs)
    result["promotion"]["eligible"] = False
    with pytest.raises(ValueError, match="researchEvidenceInsufficient"):
        validate(**kwargs)


def test_correlation_limit_checks_projected_account_not_only_single_order(monkeypatch):
    from app.services.live_trading import account_risk
    rows = [{"strategy_id": i + 1, "status": "running", "initial_capital": 1000, "leverage": 1,
             "strategy_market_type": "swap", "credential_id": 5, "symbol": symbol, "side": "long",
             "size": 5, "current_price": 100, "trading_config": {}}
            for i, symbol in enumerate(["BTC/USDT", "ETH/USDT"])]
    monkeypatch.setattr(account_risk, "_load_account_rows", lambda **kw: rows)
    model = {"symbols": ["BTC/USDT", "ETH/USDT"], "covariance": [[.0004, .000399], [.000399, .0004]],
             "period": "daily", "as_of": datetime.now(timezone.utc).isoformat()}
    kwargs = dict(user_id=1, credential_id=5, market_type="swap", strategy_id=1,
                  limits={"portfolio_model": model, "max_portfolio_daily_volatility": .005})
    result = account_risk.account_risk_snapshot(**kwargs)
    assert "accountRisk.portfolioVolatilityExceeded" in result["violations"]
    rows[1]["side"] = "short"
    assert account_risk.account_risk_snapshot(**kwargs)["allowed"]
    model["as_of"] = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    assert "accountRisk.portfolioModelUnavailable" in account_risk.account_risk_snapshot(**kwargs)["violations"]


def test_shadow_observations_use_closed_horizon_bars_and_frozen_costs(tmp_path, monkeypatch):
    from app.routes import strategy_services
    from app.services.strategy_v2.snapshot import MarketDataSnapshotStore
    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=24)
    state = {"reference_price": 100, "context": {"evaluation_costs": {"commission": .01, "slippage": .01}}}
    record = {"created_at": now - timedelta(hours=48), "decision_uid": "observed", "symbol": "USStock:AAPL",
              "market_type": "spot", "action": "open_long", "request_snapshot": state,
              "checks_json": [{"name": "entry_policy", "raw_allowed": False, "valid_decision": True}]}
    future = {**record, "decision_uid": "future", "created_at": now}
    unavailable = {**record, "decision_uid": "unavailable", "checks_json": []}
    monkeypatch.setattr(ai_evaluation, "get_db_connection", connection([record, future, unavailable]))
    monkeypatch.setattr(strategy_services, "get_strategy_service", lambda: SimpleNamespace(
        get_strategy=lambda *a, **kw: {"market_category": "USStock", "trading_config": {"commission": .99}}))
    monkeypatch.setenv("BACKTEST_SNAPSHOT_DIR", str(tmp_path))
    frame = pd.DataFrame({"open": [100, 100], "high": [110, 9999], "low": [100, 100], "close": [110, 9999], "volume": [1000, 1000]},
                         index=[target - timedelta(hours=1), now + timedelta(hours=1)])
    result = ai_evaluation.evaluate_strategy_shadow(user_id=23, strategy_id=1, frame_loader=lambda *a, **kw: frame)
    assert result["observed"] == 1 and result["missing"] == 2
    row = result["rows"][0]
    assert row["net_return"] == pytest.approx(.05801) and row["filtered_return"] == 0
    assert row["observed_price"] == 110 and result["missed_winners"] == 1
    assert MarketDataSnapshotStore(tmp_path).load(row["data_snapshot"]["snapshotId"]).iloc[0]["close"] == 110
    assert result["rows"][1]["missing_reason"] == "horizonNotReached"
    assert result["rows"][2]["missing_reason"] == "validShadowDecisionUnavailable"

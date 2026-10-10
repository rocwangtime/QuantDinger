from dataclasses import replace
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from app.services import ai_decision_filter as ai
from app.services.ai_evaluation import summarize_shadow
from app.services.portfolio.risk import analyze_portfolio, projected_model_risk
from app.services.strategy_evolution import EvolutionConfig, SearchParameter, StrategyEvolutionEngine
from app.services.strategy_evolution.bundles import EvolutionBundleStore, FrozenUniverse, content_hash
from app.services.strategy_evolution.evaluator import PreparedEvolutionEvaluator
from app.services.strategy_evolution import service as evolution
from app.services.strategy_v2.service import StrategyV2BacktestService
from app.services.strategy_v2.deployment import StrategyV2DeploymentService


def decision_request(**values):
    return ai.AIDecisionRequest(user_id=1, source_type="strategy", symbol="BTC/USDT", action=values.pop("action", "open_long"),
                                market_type="swap", quantity=1, reference_price=100, **values)


@pytest.mark.parametrize("mode,allowed,decision", [("advisory", True, "skipped"),
                                                 ("required", False, "unavailable_rejected"),
                                                 ("shadow", True, "shadow_unavailable")])
@pytest.mark.parametrize("failure", ["no_provider", "billing", "provider_error"])
def test_ai_modes_preserve_failure_and_billing_audit(monkeypatch, mode, allowed, decision, failure):
    service = ai.AIDecisionFilter()
    audits = []
    monkeypatch.setattr(service, "_persist", lambda request, result: audits.append(result))
    monkeypatch.setattr(service, "_jev_config", lambda: {"api_key": ""})
    monkeypatch.setattr(ai, "LLMService", lambda: SimpleNamespace(is_configured=lambda: failure != "no_provider",
        get_default_model=lambda: "test", call_llm_api=lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("timeout"))))
    monkeypatch.setattr(service, "_consume_credits", lambda *a: {"accepted": failure != "billing", "charged": 1,
        "refunded": 0, "message": "insufficient_credits" if failure == "billing" else "ok"})
    refunds = []
    monkeypatch.setattr(service, "_refund_credits", lambda user_id, billing: refunds.append(billing) or {**billing, "refunded": 1})
    result = service.evaluate(decision_request(mode=mode), enabled=True)
    assert result.allowed is allowed
    assert result.decision == decision or (failure == "provider_error" and mode == "advisory" and result.allowed)
    assert audits == [result]
    assert result.checks[-1]["mode"] == mode
    if failure == "provider_error":
        assert refunds and result.billing["refunded"] == 1


@pytest.mark.parametrize("mode", ["advisory", "required", "shadow"])
def test_ai_exits_do_not_call_provider_or_bill(monkeypatch, mode):
    service = ai.AIDecisionFilter()
    monkeypatch.setattr(service, "_jev_config", lambda: pytest.fail("exit called provider"))
    monkeypatch.setattr(service, "_persist", lambda *a: None)
    result = service.evaluate(decision_request(mode=mode, action="reduce_short"), enabled=True)
    assert result.allowed and result.decision == "skipped"
    assert not result.checks[-1]["enforced"]


def test_shadow_keeps_rejection_and_reserved_policy_cannot_be_spoofed(monkeypatch):
    service = ai.AIDecisionFilter()
    result = ai.AIDecisionResult(allowed=False, decision="reject", provider="llm", reason="risk",
        checks=[{"name": "entry_policy", "raw_allowed": True}], decision_id="test")
    monkeypatch.setattr(service, "_evaluate", lambda *a, **k: result)
    monkeypatch.setattr(service, "_persist", lambda *a: None)
    shadow = service.evaluate(decision_request(mode="shadow"), enabled=True)
    assert shadow.allowed and shadow.decision == "shadow_reject"
    assert len(shadow.checks) == 1 and shadow.checks[0]["raw_allowed"] is False


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.1, 1.1])
def test_ai_invalid_confidence_is_not_probability(value):
    assert ai.AIDecisionFilter._bounded_float(value) is None
    with pytest.raises(ValueError):
        ai.AIDecisionFilter._validate_choice_answer({"choice": "pass", "confidence": .8,
            "probabilities": {"pass": value, "reject": 1 - value}}, question="test", options={"pass", "reject"})


@pytest.mark.parametrize("scenario", ["rejected_entry", "exit", "broker_recovery"])
def test_dispatch_risk_rechecks_before_client_creation_and_preserves_recovery(monkeypatch, scenario):
    from app.services import pending_order_worker as worker_module
    from app.services.pending_orders import live_order_support
    from app.services.portfolio import execution_risk
    from app.utils import db

    class ReachedClient(BaseException):
        pass

    action = "reduce_long" if scenario == "exit" else "open_long"
    context = SimpleNamespace(strategy_id=1, signal_type=action, symbol="BTC/USDT", amount=1,
        cfg={"portfolio_risk": {}, "trading_config": {"portfolio_risk": {"enabled": True}}},
        exchange_config={}, safe_exchange_config={}, exchange_id="simulated", market_category="Crypto", market_type="swap")
    monkeypatch.setattr(live_order_support, "build_live_order_context", lambda **kw: context)
    monkeypatch.setattr(worker_module, "append_strategy_log", lambda *a: None)
    monkeypatch.setattr(db, "get_db_transaction", lambda: nullcontext())
    checks = []
    actual_guard = execution_risk.enforce_portfolio_entry

    def guard(**kwargs):
        checks.append(kwargs)
        if scenario == "exit":
            return actual_guard(**kwargs)  # Exit returns before any database/model access.
        raise ValueError("portfolioRisk.modelUnavailable")

    monkeypatch.setattr(execution_risk, "enforce_portfolio_entry", guard)
    monkeypatch.setattr(worker_module, "LiveOrderNotifier", lambda **kw: SimpleNamespace(notify=lambda **kw: None))
    monkeypatch.setattr(worker_module, "create_client", lambda *a, **kw: (_ for _ in ()).throw(ReachedClient()))
    worker = object.__new__(worker_module.PendingOrderWorker)
    worker._notifier = None
    worker._load_notification_config = worker._load_strategy_name = lambda *a: None
    failed = []
    worker._mark_failed = lambda **kw: failed.append(kw)
    row = {"user_id": 1, "price": 100, "client_order_id": "known-broker-id" if scenario == "broker_recovery" else ""}
    if scenario == "rejected_entry":
        worker._execute_live_order(order_id=42, order_row=row, payload={})
        assert failed == [{"order_id": 42, "error": "portfolioRisk.modelUnavailable"}]
        assert checks[0]["pending_id"] == 42
    else:
        with pytest.raises(ReachedClient):
            worker._execute_live_order(order_id=42, order_row=row, payload={})
        assert not failed
        assert len(checks) == (1 if scenario == "exit" else 0)


def dated_panel():
    index = pd.date_range("2025-01-01", periods=100)
    values = np.sin(np.arange(100)) * .02
    return {symbol: dict(zip(index.strftime("%Y-%m-%d"), values)) for symbol in ["BTC/USDT", "ETH/USDT"]}


def test_correlated_longs_have_more_risk_than_hedged_pair():
    common = dict(returns=dated_panel(), shrinkage=.2, annual_periods=365)
    longs = analyze_portfolio(weights={"BTC/USDT": .5, "ETH/USDT": .5}, **common)
    hedge = analyze_portfolio(weights={"BTC/USDT": .5, "ETH/USDT": -.5}, **common)
    assert longs["daily_volatility"] > hedge["daily_volatility"] * 2
    assert longs["gross_weight"] == hedge["gross_weight"] == 1
    assert hedge["net_weight"] == 0 and hedge["stress_return"] == 0
    assert sum(longs["risk_contributions"].values()) == pytest.approx(longs["daily_volatility"] ** 2)
    assert longs["empirical_expected_shortfall_95"] >= longs["empirical_var_95"]


def test_risk_does_not_impute_missing_dates_or_accept_nonfinite_inputs():
    panel = dated_panel()
    panel["ETH/USDT"] = dict(list(panel["ETH/USDT"].items())[-20:])
    report = analyze_portfolio(returns=panel, weights={x: .5 for x in panel})
    assert not report["available"] and report["observations"] == 20
    assert analyze_portfolio(returns={}, weights={"BTC/USDT": 1})["missing"] == ["BTC/USDT"]
    panel["BTC/USDT"]["2025-01-01"] = float("nan")
    with pytest.raises(ValueError):
        analyze_portfolio(returns=panel, weights={"BTC/USDT": 1})


def test_order_risk_model_fails_closed_when_stale_uncovered_or_not_psd():
    now = datetime.now(timezone.utc)
    model = {"symbols": ["BTC/USDT"], "covariance": [[.0004]], "as_of": now.isoformat(), "period": "daily"}
    assert projected_model_risk(model, notionals={"BTC/USDT": 100}, capital=100)["daily_volatility"] == .02
    assert not projected_model_risk(model, notionals={"ETH/USDT": 1}, capital=100)["available"]
    assert not projected_model_risk({**model, "as_of": (now - timedelta(days=5)).isoformat()},
                                   notionals={}, capital=100)["available"]
    with pytest.raises(ValueError):
        projected_model_risk({**model, "covariance": [[-.1]]}, notionals={}, capital=100)
    with pytest.raises(ValueError):
        StrategyV2DeploymentService._validate_portfolio_risk({"max_portfolio_daily_volatility": float("nan")})


def test_shadow_report_counts_blocked_losses_and_missed_winners_without_fake_gain():
    report = summarize_shadow([
        {"decision_at": "2025-01-01", "net_return": -.1, "filtered_return": 0, "raw_allowed": False},
        {"decision_at": "2025-01-02", "net_return": .2, "filtered_return": 0, "raw_allowed": False},
        {"decision_at": "2025-01-03", "net_return": None, "raw_allowed": True},
    ], horizon_hours=24)
    assert report["observed"] == 2 and report["missing"] == 1
    assert report["blocked_losses"] == report["missed_winners"] == 1
    assert report["mean_paired_delta"] == pytest.approx(-.05)
    assert report["evidence_status"] == "insufficient_evidence"


def test_source_is_frozen_before_queue_and_client_cannot_supply_internal_state(monkeypatch):
    source = {"id": 9, "code": "original", "param_schema": {"params": []}}
    monkeypatch.setattr(evolution, "get_script_source_service", lambda: SimpleNamespace(get_source=lambda *a, **k: source))
    prepared = evolution.StrategyEvolutionService().prepare_submission(user_id=1,
        payload={"sourceId": 9, "__frozenSource": {"code": "injected"}, "__bundleId": "injected", "__userId": 99})
    source["code"] = "changed"
    assert prepared["__frozenSource"]["code"] == "original"
    assert "__bundleId" not in prepared and "__userId" not in prepared
    assert prepared["__codeHash"] == content_hash("original")


def test_full_input_replay_uses_frozen_membership_and_fundamentals(tmp_path, monkeypatch):
    from app.services.strategy_v2 import service as v2
    from app.services.strategy_v2.snapshot import MarketDataSnapshotStore
    code = '''
def initialize(context):
    context.set_universe(pool="test_pool")
    context.subscribe(frequency="1d")
    context.set_warmup(2)
def handle_data(context, data):
    fundamentals = get_fundamentals(["PE"])
    for symbol in get_universe_stocks():
        order_target_percent(symbol, 0.4 if fundamentals.loc[symbol, "PE"] < 20 else 0)
'''
    universe = {"id": 7, "name": "test_pool", "kind": "static", "reference": "POOL:test_pool"}
    candidates = [{"market": "USStock", "symbol": "AAPL", "membership_periods": [{"valid_from": "2024-01-01", "valid_to": "2024-03-01"}]},
                  {"market": "USStock", "symbol": "MSFT", "membership_periods": [{"valid_from": "2024-03-01", "valid_to": None}]}]
    calls = []
    index = pd.date_range("2023-12-01", "2024-06-30")
    frame = pd.DataFrame({"open": 100 + np.arange(len(index)), "high": 102 + np.arange(len(index)),
                         "low": 99 + np.arange(len(index)), "close": 101 + np.arange(len(index)), "volume": 10000}, index=index)
    def fetch(*a, **k):
        calls.append("prices")
        return frame.copy()
    def enrich(frames, members):
        calls.append("fundamentals")
        return {key: data.assign(pe_ratio=10.0) for key, data in frames.items()}
    service = StrategyV2BacktestService(universe_service=FrozenUniverse(candidates, universe),
        frame_fetcher=fetch, fundamental_enricher=enrich, snapshot_store=MarketDataSnapshotStore(root=tmp_path / "snapshots"))
    store = EvolutionBundleStore(tmp_path / "bundles")
    kwargs = dict(user_id=1, code=code, start_date=datetime(2024, 1, 1), end_date=datetime(2024, 6, 30),
                  initial_capital=10000, leverage_enabled=False, leverage=1, source_id=9, strategy_name="frozen", bundle_store=store)
    prepared = PreparedEvolutionEvaluator(backtest_service=service, **kwargs)
    result = prepared({}, kwargs["start_date"], kwargs["end_date"], .0005, .0005)
    assert result["totalExecutions"] > 0
    candidates[0]["membership_periods"][0]["valid_to"] = None
    frame["close"] *= 3
    def forbidden(*a, **k):
        pytest.fail("replay touched mutable data or product provider")
    service.frame_fetcher = forbidden
    service.fundamental_enricher = forbidden
    service.universe_service = SimpleNamespace(list_universes=forbidden)
    monkeypatch.setattr(v2, "get_catalog_product", forbidden)
    replay = PreparedEvolutionEvaluator(backtest_service=service, bundle_id=prepared.bundle_id, **kwargs)
    repeated = replay({}, kwargs["start_date"], kwargs["end_date"], .0005, .0005)
    for key in ["totalReturn", "totalExecutions", "equityCurve", "trades"]:
        assert repeated.get(key) == result.get(key)
    assert replay.bundle_metadata["replay"] and calls.count("fundamentals") == 1
    with pytest.raises(ValueError, match="bundleNotFound"):
        store.load(prepared.bundle_id, user_id=2)
    with pytest.raises(ValueError, match="replaySourceChanged"):
        PreparedEvolutionEvaluator(backtest_service=service, bundle_id=prepared.bundle_id, **{**kwargs, "code": code + "\n# changed"})


def test_engine_discloses_fixed_parameters_and_blocks_reused_holdout():
    def evaluate(params, start, end, *cost):
        count = max(3, (end - start).days)
        curve = [{"time": (start + timedelta(days=i)).isoformat(), "value": 10000 * (1 + .001 * i + .0001 * (i % 3))}
                 for i in range(count)]
        return {"totalReturn": 5, "sharpeRatio": 1, "maxDrawdown": -1, "winRate": 60, "totalTrades": 10, "equityCurve": curve}
    result = StrategyEvolutionEngine(evaluate).run(parameters=[SearchParameter("p", "integer", 1, 3, 1, default=1)],
        config=EvolutionConfig.from_payload({"trials": 3, "folds": 4, "autoPrune": False, "monteCarloPaths": 100}),
        start_date=datetime(2024, 1, 1), end_date=datetime(2024, 12, 31), commission=.001, slippage=.001,
        previous_trials=50, holdout_tracker=lambda *a: 2)
    dsr = result["validation"]["deflatedSharpe"]
    assert dsr["effectiveTrials"] == 50 + dsr["attemptedTrials"]
    assert result["plan"]["rollingRefit"] is False
    assert "holdoutPreviouslyExposed" in result["promotion"]["reasons"]
    assert not result["promotion"]["eligible"]


@pytest.mark.parametrize("catalog", [None, {"product_type": "crypto", "api_family": "swap", "instrument_id": "BTC-USDT-SWAP"}])
def test_crypto_replay_freezes_instrument_rules_and_missing_catalog(tmp_path, monkeypatch, catalog):
    from app.services.instrument_rules import InstrumentRules, InstrumentRulesSnapshot, instrument_rules_key
    from app.services.strategy_v2 import service as v2
    from app.services.strategy_v2.snapshot import MarketDataSnapshotStore
    rule_key = instrument_rules_key("BTC/USDT", exchange_id="okx", market_type="swap")
    rules = InstrumentRulesSnapshot.build([InstrumentRules(key=rule_key, exchange_id="okx", market_type="swap",
        symbol="BTC/USDT", amount_step=.001, min_amount=.001, min_notional=1, captured_at="2024-01-01T00:00:00Z")])
    calls = []
    def rule_provider(*a, **kw):
        calls.append(kw)
        return rules
    monkeypatch.setattr(v2, "get_catalog_product", lambda **kw: catalog)
    index = pd.date_range("2023-12-01", "2024-06-30")
    frame = pd.DataFrame({"open": 100, "high": 105, "low": 95, "close": 100 + np.sin(np.arange(len(index))), "volume": 10000}, index=index)
    service = StrategyV2BacktestService(frame_fetcher=lambda *a, **kw: frame,
        instrument_rules_provider=SimpleNamespace(historical_snapshot=rule_provider),
        snapshot_store=MarketDataSnapshotStore(tmp_path / "snapshots"))
    code = '''
def initialize(context):
    context.set_universe(["Crypto:BTC/USDT@okx:swap"])
    context.subscribe(frequency="1d")
    context.set_warmup(2)
def handle_data(context, data):
    order_target_percent("Crypto:BTC/USDT@okx:swap", 0.2)
'''
    kwargs = dict(user_id=1, code=code, start_date=datetime(2024, 1, 1), end_date=datetime(2024, 6, 30),
        initial_capital=10000, leverage_enabled=False, leverage=1, source_id=1, strategy_name="crypto",
        bundle_store=EvolutionBundleStore(tmp_path / "bundles"))
    first = PreparedEvolutionEvaluator(backtest_service=service, **kwargs)
    result = first({}, kwargs["start_date"], kwargs["end_date"], .0005, .0005)
    def forbidden(*a, **kw):
        pytest.fail("frozen crypto inputs were fetched again")
    monkeypatch.setattr(v2, "get_catalog_product", forbidden)
    service.frame_fetcher = forbidden
    service.instrument_rules_provider = SimpleNamespace(historical_snapshot=forbidden)
    replay = PreparedEvolutionEvaluator(backtest_service=service, bundle_id=first.bundle_id, **kwargs)
    repeated = replay({}, kwargs["start_date"], kwargs["end_date"], .0005, .0005)
    assert repeated["equityCurve"] == result["equityCurve"] and result["totalExecutions"] > 0
    assert len(calls) == 1


def test_pbo_aligns_full_daily_returns_including_pruned_candidates():
    from app.services.strategy_evolution.statistics import return_matrix_pbo
    index = pd.date_range("2024-01-01", periods=100)
    candidates = []
    for scale in [.1, .2, -.1]:
        curve = [{"time": stamp.isoformat(), "value": 1000 + scale * i + .2 * (i % 3)} for i, stamp in enumerate(index)]
        candidates.append([{"equityCurve": curve}])
    # A candidate pruned after 60 dates still participates; missing dates aren't filled.
    candidates.append([{"equityCurve": candidates[0][0]["equityCurve"][:60]}])
    result = return_matrix_pbo(candidates)
    assert result["available"] and result["candidates"] == 4 and result["observations"] == 59
    assert result["samples"] == 70 and result["selectedCandidatesOnly"] is False
    assert not return_matrix_pbo([[{"equityCurve": []}], candidates[0]])["available"]

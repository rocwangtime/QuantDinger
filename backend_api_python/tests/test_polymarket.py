"""Financial invariants, public API contracts, delayed fills, and frozen replay."""

import copy
import inspect
from types import SimpleNamespace

import pytest
from flask import g

from app.services.polymarket import engine as e, jobs
from app.services.polymarket.client import PublicClient


def gamma(**override):
    return {"id": "42", "question": "Will event happen?", "slug": "will-event-happen", "active": True,
            "closed": False, "acceptingOrders": True, "enableOrderBook": True, "negRisk": False,
            "conditionId": "0x" + "a" * 64, "outcomes": '["Yes","No"]', "clobTokenIds": '["11","22"]',
            "feesEnabled": True, "feeSchedule": {"rate": ".04", "exponent": 1, "takerOnly": True},
            "orderPriceMinTickSize": ".01", "orderMinSize": "5", "version": "v1", **override}


def market(**override):
    return e.market_from_gamma(gamma(**override))


def frame(at=1000, *, yes=".46", no=".51", yes_size="100", no_size="100", bid=".45", age=0):
    m = market()
    return {"observedMs": at, "books": {
        aid: {"assetId": aid, "conditionId": m["conditionId"], "observedMs": at - age,
              "asks": [{"price": price, "size": size}], "bids": [{"price": bid, "size": "100"}]}
        for aid, price, size in [("11", yes, yes_size), ("22", no, no_size)]}}


def bundle(**settings):
    return {"engineVersion": e.ENGINE_VERSION, "implementationHash": e.IMPLEMENTATION_HASH, "market": market(),
            "settings": e.validate_settings({"slippageBps": 0, **settings}),
            "frames": {role: frame(at) for role, at in [("signal", 1000), ("leg1", 1150), ("leg2", 1400), ("unwind", 1550)]}}


@pytest.mark.parametrize("value", [True, None, "NaN", "Infinity", float("inf"), "-Infinity", "1000000001"])
def test_financial_numbers_reject_boolean_nonfinite_and_unbounded(value):
    with pytest.raises(ValueError):
        e.number(value)


@pytest.mark.parametrize("override,reason", [
    ({"negRisk": True}, "negativeRiskExcluded"), ({"negRisk": None}, "negativeRiskExcluded"),
    ({"outcomes": '["Team A","Team B"]'}, "standardBinaryRequired"),
    ({"feesEnabled": None}, "feeUnknown"), ({"feeSchedule": {"rate": .04, "exponent": 2}}, "feeUnknown"),
    ({"clobTokenIds": '["11","11"]'}, "outcomeIdsMissing"), ({"closed": True}, "marketClosed"),
    ({"acceptingOrders": False}, "ordersUnavailable"), ({"conditionId": "invalid"}, "conditionMissing"),
    ({"orderPriceMinTickSize": None}, "invalidNumber"), ({"version": "v3"}, "unsupportedVersion"),
])
def test_unknown_market_identity_rules_and_fees_fail_closed(override, reason):
    with pytest.raises(ValueError, match=reason):
        market(**override)


def test_protocol_v2_uses_position_ids_and_maps_outcome_order():
    m = market(version="v2", positionIds=["33", "44"], outcomes='["No","Yes"]')
    assert m["yesAssetId"] == "44" and m["noAssetId"] == "33"
    assert market(feesEnabled=False)["feeRate"] == "0"


def test_books_sort_unsorted_depth_and_reject_wrong_identity_or_crossed_book():
    m = market()
    raw = {"asset_id": "11", "market": m["conditionId"], "asks": [{"price": ".7", "size": "100"}, {"price": ".46", "size": "50"}],
           "bids": [{"price": ".3", "size": "50"}, {"price": ".4", "size": "100"}]}
    normalized = e.normalize_book(raw, m, "11", 1000)
    assert normalized["asks"][0]["price"] == "0.46"
    assert normalized["bids"][0]["price"] == "0.4"
    with pytest.raises(ValueError, match="IdentityMismatch"):
        e.normalize_book({**raw, "asset_id": "22"}, m, "11", 1000)
    with pytest.raises(ValueError, match="crossedBook"):
        e.normalize_book({**raw, "bids": [{"price": ".8", "size": "10"}]}, m, "11", 1000)


def test_fee_and_depth_can_erase_an_apparent_three_percent_edge():
    settings = e.validate_settings({})
    q = e.quote(market(), frame(), settings)
    assert q["eligible"] and e.number(q["netProfit"]) == e.number(".93680")
    costly = e.quote(market(feeSchedule={"rate": ".07", "exponent": 1}), frame(), settings)
    assert not costly["eligible"] and e.number(costly["netProfit"]) < 0
    deep = frame()
    deep["books"]["22"]["asks"] = [{"price": ".51", "size": "10"}, {"price": ".6", "size": "90"}]
    assert not e.quote(market(), deep, settings)["eligible"]
    assert e.quote(market(), frame(no_size="99"), settings)["reason"] == "polymarket.insufficientDepth"


def test_balanced_merge_conserves_cash_and_records_fees():
    b = bundle()
    frozen = copy.deepcopy(b)
    result = e.simulate(b)
    assert result["status"] == "merged" and result["mergedQuantity"] == "100"
    assert e.number(result["realizedPnl"]) == e.number(".98680")
    assert e.number(result["cash"]) == 200 + e.number(result["realizedPnl"])
    assert not result["residuals"] and len(result["fills"]) == 2
    assert b == frozen and e.simulate(b) == result


def test_fok_is_per_leg_and_second_leg_failure_is_not_reported_as_arbitrage_profit():
    b = bundle()
    b["frames"]["leg2"] = frame(1400, no=".7")
    result = e.simulate(b)
    assert result["mergedQuantity"] == "0" and result["status"] == "unwound"
    assert result["reason"] == "polymarket.secondLegUnfilled"
    assert e.number(result["realizedPnl"]) < 0 and result["cash"] == str(200 + e.number(result["realizedPnl"]))
    assert [x["filled"] for x in result["fills"]] == ["100", "0", "100"]


def test_compensation_cost_limit_preserves_and_accounts_for_residual_exposure():
    b = bundle(maxUnwindLoss=0)
    b["frames"]["leg2"] = frame(1400, no=".7")
    result = e.simulate(b)
    assert result["status"] == "needs_review" and result["residuals"][0]["quantity"] == "100"
    assert e.number(result["cashChange"]) < 0 and e.number(result["realizedPnl"]) == 0
    assert e.number(result["cash"]) + e.number(result["residualCostBasis"]) == 200


def test_partial_fills_merge_only_balanced_quantity_then_attempt_one_exit():
    b = bundle(orderType="FAK")
    b["frames"]["leg1"] = frame(1150, yes_size="50")
    b["frames"]["leg2"] = frame(1400, no_size="30")
    result = e.simulate(b)
    assert result["status"] == "partial_merged" and result["mergedQuantity"] == "30"
    assert [x["filled"] for x in result["fills"]] == ["50", "30", "20"]
    assert result["residuals"] == []


@pytest.mark.parametrize("role", ["leg1", "leg2"])
def test_missing_delayed_observation_never_falls_back_to_signal_price(role):
    b = bundle(maxUnwindLoss=0)
    del b["frames"][role]
    result = e.simulate(b)
    assert result["reason"] == "polymarket.observationMissing"
    assert result["mergedQuantity"] == "0"
    assert result["status"] == ("unfilled" if role == "leg1" else "needs_review")


def test_stale_books_timeout_and_budget_fail_before_unbounded_execution():
    b = bundle(maxBookAgeMs=500)
    b["frames"]["signal"] = frame(age=501)
    assert e.simulate(b)["reason"] == "polymarket.staleBook"
    b = bundle(maxUnhedgedMs=1000, maxUnwindLoss=0)
    b["frames"]["leg2"] = frame(3000)
    b["frames"]["unwind"] = frame(3500)
    assert e.simulate(b)["reason"] == "polymarket.exposureTimeout"
    b = bundle(budget=100, slippageBps=1000)
    result = e.simulate(b)
    assert result["reason"] == "polymarket.priceBufferConsumesEdge" and not result["fills"]


def test_entry_delay_and_unknown_model_settings_are_enforced():
    b = bundle()
    b["frames"]["leg1"] = frame(1100)
    assert e.simulate(b)["reason"] == "polymarket.observationTooEarly"
    for settings in [{"quantity": True}, {"quantity": float("nan")}, {"orderType": "MARKET"}, {"privateKey": "no"}, {"latencyMs": 1.5}]:
        with pytest.raises(ValueError):
            e.validate_settings(settings)


def test_tick_rounding_never_exceeds_configured_price_buffer():
    q = e.quote(market(), frame(), e.validate_settings({"slippageBps": 10}))
    assert q["entryPriceLimits"] == ["0.46", "0.51"]
    q = e.quote(market(), frame(), e.validate_settings({"slippageBps": 1000}))
    assert q["reason"] == "polymarket.priceBufferConsumesEdge"


def test_public_batch_capture_rejects_missing_duplicate_and_wrong_market_books():
    m = market()
    row = {"asset_id": "11", "market": m["conditionId"], "bids": [], "asks": [{"price": ".46", "size": "100"}]}
    calls = []
    client = PublicClient(session=SimpleNamespace(request=lambda *a, **kw: calls.append((a, kw)) or SimpleNamespace(
        content=b"[]", raise_for_status=lambda: None, json=lambda: [row]), close=lambda: None))
    captured = client.capture([m])[m["id"]]
    assert captured["error"] == "polymarket.observationMissing"
    args, options = calls[0]
    assert args == ("POST", "https://clob.polymarket.com/books")
    assert options["allow_redirects"] is False
    client._json = lambda *a, **kw: [row, row]
    assert client.capture([m])[m["id"]]["error"] == "polymarket.bookIdentityMismatch"


def test_owned_frozen_replay_hash_checks_and_never_reads_public_api(monkeypatch):
    b = bundle()
    result = {**e.simulate(b), "bundle": b, "bundleHash": e.digest(b)}
    row = {"kind": "polymarket_paper", "status": "succeeded", "result": result}
    monkeypatch.setattr(jobs.agent_jobs, "get_job", lambda job_id, user_id: row if user_id == 7 else None)
    monkeypatch.setattr(jobs, "PublicClient", lambda: pytest.fail("replay must not read the network"))
    replay = jobs.run_replay({"sourceJobId": "source", "__userId": 7})
    assert replay["replayMatches"] and replay["bundleHash"] == result["bundleHash"]
    with pytest.raises(ValueError, match="notFound"):
        jobs.bundle_from_job("source", 8, "polymarket_paper")
    b["frames"]["signal"]["observedMs"] = 999
    with pytest.raises(ValueError, match="HashMismatch"):
        jobs.bundle_from_job("source", 7, "polymarket_paper")


def test_http_routes_require_auth_and_register_strict_schemas(client, app):
    from app.openapi import get_openapi_api
    paths = get_openapi_api(app).spec.to_dict()["paths"]
    for path in ["/api/polymarket/scans", "/api/polymarket/paper-runs", "/api/polymarket/jobs/{job_id}/evidence"]:
        assert path in paths
    for url, method in [("/api/polymarket/jobs", "get"), ("/api/polymarket/scans", "post"), ("/api/polymarket/paper-runs", "post")]:
        assert getattr(client, method)(url).status_code == 401


def test_route_uses_session_owner_and_requires_idempotency(app, monkeypatch):
    from app.routes import polymarket as routes
    captured = {}
    monkeypatch.setattr(routes.agent_jobs, "submit_job", lambda **kw: captured.update(kw) or {"job_id": "saved", "status": "queued"})
    with app.test_request_context(headers={"Idempotency-Key": "same-key"}):
        g.user_id = 7
        response, status = inspect.unwrap(routes.scan)({"settings": {}, "__userId": 999})
        assert status == 202 and response.get_json()["code"] == 1
    assert captured["user_id"] == captured["request_payload"]["__userId"] == 7
    assert captured["idempotency_key"] == "same-key"
    with app.test_request_context():
        g.user_id = 7
        assert inspect.unwrap(routes.scan)({"settings": {}})[1] == 400


def test_workers_support_all_three_polymarket_kinds():
    from app.tasks.agent_jobs import supports_kind
    assert all(supports_kind(kind) for kind in jobs.KINDS)


@pytest.mark.parametrize("payload", [
    {"settings": {"privateKey": "not-accepted"}}, {"executionMode": "live"},
    {"settings": {"quantity": "NaN"}}, {"marketIds": ["https://example.com"]},
    {"durationSeconds": 31}, {"marketIds": [str(x) for x in range(9)]},
])
def test_http_schema_rejects_credentials_live_mode_nonfinite_and_unbounded_input(client, monkeypatch, payload):
    from app.utils import auth
    monkeypatch.setattr(auth, "verify_token", lambda _: {"user_id": 7, "_verified_username": "test", "_verified_user_role": "user"})
    response = client.post("/api/polymarket/scans", json=payload, headers={"Authorization": "Bearer test", "Idempotency-Key": "key"})
    assert response.status_code == 400


def test_paper_refetches_signal_instead_of_executing_an_old_profitable_scan(monkeypatch):
    saved = market()
    monkeypatch.setattr(jobs, "bundle_from_job", lambda *a: {"markets": [saved]})
    captures = []
    client = SimpleNamespace(market=lambda identity: saved,
                             capture=lambda markets: captures.append(markets) or {"42": frame(1000, no=".7")},
                             close=lambda: None)
    result = jobs.run_paper({"settings": {}, "scanJobId": "old", "marketId": "42", "__userId": 7}, client=client)
    assert result["status"] == "rejected" and not result["fills"]
    assert len(captures) == 1 and result["reason"] == "polymarket.noNetEdge"


def test_paper_rejects_changed_outcome_identity_and_always_closes_public_client(monkeypatch):
    saved = market()
    monkeypatch.setattr(jobs, "bundle_from_job", lambda *a: {"markets": [saved]})
    closed = []
    client = SimpleNamespace(market=lambda identity: {**saved, "yesAssetId": "999"},
                             capture=lambda _: pytest.fail("identity mismatch must stop before reading books"),
                             close=lambda: closed.append(True))
    with pytest.raises(ValueError, match="IdentityChanged"):
        jobs.run_paper({"settings": {}, "scanJobId": "old", "marketId": "42", "__userId": 7}, client=client)
    assert closed

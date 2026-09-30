"""No provider network or broker orders: verify model tier isolation and receipts."""

from __future__ import annotations

import pytest

from app.services import model_trading_agent as runner
from app.utils import agent_auth

REAL_AUDIT = agent_auth._audit


def _token(scopes="R,T"):
    return {
        "id": 51, "user_id": 7, "name": "model-test", "scopes": scopes,
        "markets": "*", "instruments": "*", "paper_only": True,
        "rate_limit_per_min": 100, "status": "active", "expires_at": None,
    }


@pytest.fixture(autouse=True)
def _setup(monkeypatch):
    agent_auth._schema_ready = True
    agent_auth._rate_state.clear()
    monkeypatch.setattr(agent_auth, "_lookup_token", lambda _raw: _token())
    monkeypatch.setattr(agent_auth, "_touch_token_last_used", lambda *_: None)
    monkeypatch.setattr(agent_auth, "_audit", lambda *a, **kw: None)
    monkeypatch.setenv("AGENT_MODEL_ENABLED", "true")
    monkeypatch.setattr(runner, "_select_provider", lambda *_: (
        runner.LLMService("deepseek"), "test-model", "test-key"))
    yield
    agent_auth._rate_state.clear()


def _headers(key=None):
    headers = {"Authorization": "Bearer qd_agent_MODELTEST12345"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _tool(name, arguments):
    return {"tool_calls": [{"id": "call-1", "type": "function", "function": {
        "name": name, "arguments": arguments}}]}


def test_observe_cannot_invoke_order_tool(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    monkeypatch.setattr(runner, "_completion", lambda *_: _tool("place_futu_simulate_order", "{}"))
    monkeypatch.setattr(runner, "_execute_tool", lambda *_: pytest.fail("unauthorized tool executed"))
    response = client.post("/api/agent/v1/model-agent/observe", headers=_headers("observe-deny"),
                           json={"goal": "buy now", "provider": "deepseek"})
    assert response.status_code == 403
    assert "outside the selected tier" in response.get_json()["message"]


def test_plan_cannot_invoke_direct_order(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    monkeypatch.setattr(runner, "_completion", lambda *_: _tool("place_futu_simulate_order", "{}"))
    monkeypatch.setattr(runner, "_execute_tool", lambda *_: pytest.fail("unauthorized tool executed"))
    response = client.post("/api/agent/v1/model-agent/plan", headers=_headers("plan-1"),
                           json={"goal": "buy now"})
    assert response.status_code == 403


def test_observe_requires_idempotency_to_avoid_repeat_provider_billing(client, monkeypatch):
    monkeypatch.setattr(runner, "_completion", lambda *_: pytest.fail("model called"))
    response = client.post("/api/agent/v1/model-agent/observe", headers=_headers(),
                           json={"goal": "read positions"})
    assert response.status_code == 400
    assert "Idempotency-Key" in response.get_json()["message"]


def test_paper_requires_idempotency_key_before_model_call(client, monkeypatch):
    monkeypatch.setattr(runner, "_completion", lambda *_: pytest.fail("model called"))
    response = client.post("/api/agent/v1/model-agent/paper", headers=_headers(),
                           json={"goal": "buy now"})
    assert response.status_code == 400
    assert "Idempotency-Key" in response.get_json()["message"]


def test_paper_idempotent_replay_never_calls_provider_or_broker(client, monkeypatch):
    cached = {"code": 0, "message": "model-agent-run", "data": {
        "run_id": "cached", "provider": "deepseek", "model": "test-model",
        "tier": "paper", "answer": "completed", "tools": [{"tool": "place_futu_simulate_order",
        "result": {"id": 44, "status": "SUBMITTED"}}], "completed": True}}
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: (
        "completed", {"response_body": cached, "response_status": 200}))
    monkeypatch.setattr(runner, "_completion", lambda *_: pytest.fail("provider called on replay"))
    response = client.post("/api/agent/v1/model-agent/paper", headers=_headers("paper-44"),
                           json={"goal": "place one paper order"})
    assert response.status_code == 200
    assert response.headers["Idempotent-Replayed"] == "true"
    assert response.get_json()["data"]["run_id"] == "cached"


def test_paper_tool_routes_through_durable_gateway(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    args = ('{"credential_id": 2, "market": "USStock", "symbol": "AAPL", '
            '"side": "buy", "qty": 1, "limit_price": 100, "reason": "test"}')
    monkeypatch.setattr(runner, "_completion", lambda *_: _tool("place_futu_simulate_order", args))
    seen = []
    monkeypatch.setattr(runner, "submit_intent", lambda user_id, token, order, key:
                        seen.append((user_id, token["id"], order["symbol"], key)) or
                        {"id": 44, "status": "PROPOSED"})
    from app.services import futu_agent_execution
    monkeypatch.setattr(futu_agent_execution, "execute_simulate_intent",
                        lambda user_id, token, intent_id: seen.append((user_id, intent_id)) or
                        {"id": intent_id, "status": "SUBMITTED"})
    response = client.post("/api/agent/v1/model-agent/paper", headers=_headers("paper-44"),
                           json={"goal": "place one paper order"})
    assert response.status_code == 200
    receipt = response.get_json()["data"]["tools"][0]
    assert receipt["result"] == {"id": 44, "status": "SUBMITTED", "broker": "futu"}
    assert seen[0][:3] == (7, 51, "AAPL")
    assert seen[1] == (7, 44)


def test_provider_request_never_accepts_api_key(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    monkeypatch.setattr(runner, "_completion", lambda *_: pytest.fail("model called"))
    response = client.post("/api/agent/v1/model-agent/observe", headers=_headers("observe-extra"),
                           json={"goal": "hello", "api_key": "must-not-be-accepted"})
    assert response.status_code == 400


def test_provider_status_never_returns_api_keys(client, monkeypatch):
    monkeypatch.setattr(runner.LLMService, "get_api_key", lambda *_: "super-secret-provider-key")
    response = client.get("/api/agent/v1/model-agent/providers", headers=_headers())
    assert response.status_code == 200
    assert "super-secret-provider-key" not in response.get_data(as_text=True)
    assert {item["provider"] for item in response.get_json()["data"]["providers"]} == {
        "deepseek", "openai"}


def test_disabled_model_agent_denies_before_provider_call(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    monkeypatch.setenv("AGENT_MODEL_ENABLED", "false")
    monkeypatch.setattr(runner, "_completion", lambda *_: pytest.fail("model called"))
    response = client.post("/api/agent/v1/model-agent/observe", headers=_headers("observe-disabled"),
                           json={"goal": "hello"})
    assert response.status_code == 403


def test_invalid_tool_json_and_parallel_calls_fail_closed(monkeypatch):
    monkeypatch.setattr(runner, "_completion", lambda *_: _tool("get_trade_intent", "{"))
    with pytest.raises(runner.ModelAgentError, match="invalid tool JSON"):
        runner.run_model_agent(goal="read", tier="observe", provider="deepseek",
                               user_id=7, token=_token(), idempotency_key="")
    monkeypatch.setattr(runner, "_completion", lambda *_: {
        "tool_calls": _tool("get_trade_intent", "{}")["tool_calls"] * 2})
    with pytest.raises(runner.ModelAgentError, match="multiple"):
        runner.run_model_agent(goal="read", tier="observe", provider="deepseek",
                               user_id=7, token=_token(), idempotency_key="")


def test_provider_adapters_use_documented_chat_tool_shapes(monkeypatch):
    class Response:
        ok = True
        status_code = 200

        def json(self):
            return {"choices": [{"finish_reason": "stop", "message": {"content": "done"}}]}

    seen = []
    def fake_post(self, url, **kwargs):
        seen.append((url, kwargs["json_payload"]))
        return Response()

    monkeypatch.setattr(runner.LLMService, "_llm_post", fake_post)
    for provider in ("deepseek", "openai"):
        service = runner.LLMService(provider)
        assert runner._completion(service, "test-model", "test-key", [{"role": "user", "content": "x"}],
                                  runner._tools_for("observe"))["content"] == "done"
    assert seen[0][0].endswith("/chat/completions")
    assert "parallel_tool_calls" not in seen[0][1]
    assert seen[0][1]["max_tokens"] == 2048
    assert seen[1][1]["parallel_tool_calls"] is False
    assert seen[1][1]["max_completion_tokens"] == 2048


def test_hk_quote_tool_is_read_only_and_tagged_not_execution_eligible(client, monkeypatch):
    monkeypatch.setattr(agent_auth, "_reserve_idempotency", lambda *_: ("reserved", None))
    monkeypatch.setattr(agent_auth, "_complete_idempotency", lambda *_: None)
    from app.services import exchange_execution
    from app.services.futu_trading import session_pool

    monkeypatch.setattr(runner, "account_scope", lambda *_a, **_kw: ("futu", "credential:3"))
    monkeypatch.setattr(exchange_execution, "resolve_exchange_config",
                        lambda *_a, **_kw: {"exchange_id": "futu"})
    class Client:
        def get_quote(self, symbol, market):
            assert (symbol, market) == ("HK.00700", "HKStock")
            return {"success": True, "last": 600, "bid": 599, "ask": 601, "raw": {}}

        def close(self):
            pass

    class Pool:
        def acquire(self, _cfg, *, mode):
            assert mode == "quote"
            return Client()

    monkeypatch.setattr(session_pool, "get_futu_session_pool", lambda: Pool())
    replies = iter([_tool("get_futu_quote", '{"credential_id":3,"market":"HKStock","symbol":"HK.00700"}'),
                    {"content": "Quote observed; not an execution permit."}])
    monkeypatch.setattr(runner, "_completion", lambda *_: next(replies))
    response = client.post("/api/agent/v1/model-agent/observe", headers=_headers("observe-hk"),
                           json={"goal": "HK quote"})
    assert response.status_code == 200
    receipt = response.get_json()["data"]["tools"][0]["result"]
    assert receipt["market"] == "HK"
    assert receipt["execution_eligible"] is False


def test_model_audit_omits_prompt_and_answer(app, monkeypatch):
    from contextlib import contextmanager
    from flask import g

    recorded = []
    class Cursor:
        def execute(self, _sql, params):
            recorded.append(params)

        def close(self):
            pass

    class DB:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

    @contextmanager
    def fake_db():
        yield DB()

    monkeypatch.setattr(agent_auth, "get_db_connection", fake_db)
    with app.test_request_context("/api/agent/v1/model-agent/observe", method="POST",
                                  json={"goal": "secret-goal-should-not-be-stored", "provider": "deepseek"}):
        g.agent_token = _token()
        REAL_AUDIT("R", 200, {"code": 0, "message": "ok", "data": {
            "run_id": "run-1", "answer": "secret-answer-should-not-be-stored",
            "tools": [{"tool": "get_trade_intent", "result": {"secret": "private"}}],
        }}, 12)
    assert len(recorded) == 1
    assert "secret-goal" not in recorded[0][8]
    assert "secret-answer" not in recorded[0][9]
    assert "private" not in recorded[0][9]
    assert "get_trade_intent" in recorded[0][9]

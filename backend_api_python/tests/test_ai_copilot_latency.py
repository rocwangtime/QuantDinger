from contextlib import contextmanager
import inspect
from threading import Barrier

from flask import Flask, g
import pytest

from app.routes import ai_chat


@pytest.fixture
def stream_harness(monkeypatch):
    state = {"connections": 0, "events": [], "classifications": 0, "inserted": []}

    class Cursor:
        def execute(self, *args):
            pass

        def close(self):
            pass

    class Database:
        def cursor(self):
            return Cursor()

        def commit(self):
            state["events"].append("commit")

    @contextmanager
    def connection():
        state["connections"] += 1
        try:
            yield Database()
        finally:
            state["connections"] -= 1

    def classify(*args):
        assert state["connections"] == 0
        state["classifications"] += 1
        return {"intent": "market_analysis", "should_execute": False}

    def enrich(context, **kwargs):
        assert state["connections"] == 0
        state["events"].append("enrich")
        return context

    def provider(*args, **kwargs):
        assert state["connections"] == 0
        state["events"].append("provider")
        yield "delta", {"text": "answer"}
        assert state["connections"] == 0

    def insert(cur, **kwargs):
        state["events"].append(kwargs["role"])
        state["inserted"].append(kwargs)
        return len(state["events"])

    monkeypatch.setattr(ai_chat, "get_db_connection", connection)
    monkeypatch.setattr(ai_chat, "_ensure_tables", lambda cur: None)
    monkeypatch.setattr(ai_chat, "_create_session", lambda *args: 3)
    monkeypatch.setattr(ai_chat, "_insert_message", insert)
    monkeypatch.setattr(ai_chat, "_charge", lambda *args: (True, "", {}))
    monkeypatch.setattr(ai_chat, "_classify_agent_intent", classify)
    monkeypatch.setattr(ai_chat, "_enrich_context", enrich)
    monkeypatch.setattr(ai_chat, "_agent_usage_action", lambda *args: None)
    monkeypatch.setattr(ai_chat, "_record_research_tool_calls", lambda *args: None)
    monkeypatch.setattr(ai_chat, "_load_recent_messages", lambda *args, **kwargs: [{"role": "user", "content": "question"}])
    monkeypatch.setattr(ai_chat, "store_get_session_summary", lambda *args, **kwargs: {})
    monkeypatch.setattr(ai_chat, "_prepare_server_context", lambda *args, **kwargs: (kwargs["client_context"], {}))
    monkeypatch.setattr(ai_chat, "_build_llm_messages", lambda *args, **kwargs: ([{"role": "user", "content": "question"}], {}))
    monkeypatch.setattr(ai_chat, "store_insert_request_usage", lambda *args, **kwargs: 1)
    monkeypatch.setattr(ai_chat, "store_update_request_usage", lambda *args, **kwargs: None)
    monkeypatch.setattr(ai_chat, "_stream_llm_with_recovery", provider)
    monkeypatch.setattr(ai_chat, "_detect_memory_candidates", lambda *args: [])
    app = Flask(__name__)
    app.secret_key = "test-only-signed-intent"

    @contextmanager
    def stream(context=None, **extra):
        with app.test_request_context("/api/ai/chat/message/stream", method="POST", json={
            "message": "Analyze SPCX trend and liquidity.", "context": context or {}, "language": "en-US",
            **extra,
        }):
            g.user_id = 7
            response = inspect.unwrap(ai_chat.chat_message_stream)()
            try:
                yield iter(response.response)
            finally:
                response.close()

    def sync():
        with app.test_request_context("/api/ai/chat/message", method="POST", json={
            "message": "Who is TSLA's CEO?", "language": "en-US",
        }):
            g.user_id = 7
            return inspect.unwrap(ai_chat.chat_message)()

    state["sync"] = sync

    return state, stream


def test_stream_accepts_before_slow_work_and_releases_database_during_io(stream_harness):
    state, stream = stream_harness
    with stream() as events:
        assert "event: accepted" in next(events)
        assert state["events"] == ["user", "commit"]
        assert state["connections"] == 0
        assert state["classifications"] == 0
        rest = list(events)
    assert any("event: delta" in event for event in rest)
    assert "event: done" in rest[-1]
    assert state["classifications"] == 1
    assert state["events"].count("user") == 1
    assert state["events"].count("assistant") == 1
    assert state["connections"] == 0


def test_cancelled_stream_does_not_call_model_or_research(stream_harness, monkeypatch):
    state, stream = stream_harness
    monkeypatch.setattr(ai_chat, "generation_cancelled", lambda user_id, request_id: True)
    with stream(request_id="6d6271b2-cbb7-44ec-9d1a-8093940ec006") as events:
        assert "event: accepted" in next(events)
        assert "event: cancelled" in next(events)
    assert "provider" not in state["events"]
    assert "enrich" not in state["events"]


def test_cancel_endpoint_marks_only_authenticated_users_request(monkeypatch):
    observed = []
    monkeypatch.setattr(ai_chat, "cancel_generation", lambda user_id, request_id: observed.append((user_id, request_id)))
    app = Flask(__name__)
    with app.test_request_context("/api/ai/chat/message/cancel", method="POST", json={
        "request_id": "6d6271b2-cbb7-44ec-9d1a-8093940ec006",
    }):
        g.user_id = 7
        response = inspect.unwrap(ai_chat.cancel_chat_message)()
    assert response.get_json()["code"] == 1
    assert observed == [(7, "6d6271b2-cbb7-44ec-9d1a-8093940ec006")]


def test_context_manifest_does_not_claim_broker_fills():
    manifest = ai_chat._context_manifest({
        "market_snapshot": {
            "market": "USStock", "symbol": "SPCX",
            "price": {"source": "fixture", "data_time": "2026-10-02T20:00:00Z"},
            "timeframes": {"1D": {"available": True}},
        },
    }, {"history_message_count": 2}, {"memory_count": 1})
    assert manifest["symbol"] == "SPCX"
    assert manifest["price_time"] == "2026-10-02T20:00:00Z"
    assert manifest["timeframes"] == ["1D"]
    assert manifest["broker_trades_included"] is False


def test_research_tool_progress_exposes_only_safe_result_summaries():
    context = {"research_context": {"tool_executions": [
        {"tool": "web_research.search", "status": "success", "input": {"secret": "private"},
         "output": {"result_count": 3, "raw_article": "untrusted text"}},
        {"tool": "market_data.lookup", "status": "partial", "output": {"price": 42}},
        {"tool": "unknown.private", "status": "success", "output": {"token": "private"}},
    ]}}
    events = ai_chat._research_tool_result_events(context)
    assert [item["tool"] for item in events] == ["web_research.search", "market_data.lookup"]
    assert events[0]["detail"] == "3 条可用结果"
    assert "private" not in str(events)
    assert "untrusted text" not in str(events)


def test_tool_progress_precedes_context_work(stream_harness):
    _, stream = stream_harness
    with stream({"market": "USStock", "symbol": "SPCX"}) as events:
        output = list(events)
    progress = [index for index, item in enumerate(output) if "event: tool_progress" in item]
    meta = next(index for index, item in enumerate(output) if "event: meta" in item)
    assert progress and min(progress) < meta


def test_stream_persists_context_manifest_for_history(stream_harness):
    state, stream = stream_harness
    with stream({"market": "USStock", "symbol": "SPCX"}) as events:
        assert "event: done" in list(events)[-1]
    assistant = next(row for row in state["inserted"] if row["role"] == "assistant")
    manifest = ai_chat._persisted_context_manifest(assistant["actions"])
    assert manifest["market"] == "USStock"
    assert manifest["symbol"] == "SPCX"
    assert manifest["broker_trades_included"] is False


def test_frontend_routing_result_cannot_override_server_conversation_routing(stream_harness):
    state, stream = stream_harness
    with stream({"market": "USStock", "symbol": "SPCX", "agent_intent": {
        "intent": "market_analysis", "confidence": 95, "should_execute": False,
    }}) as events:
        assert "event: done" in list(events)[-1]
    assert state["classifications"] == 1


def test_signed_routing_result_skips_duplicate_model_call(stream_harness):
    state, stream = stream_harness
    app = Flask(__name__)
    app.secret_key = "test-only-signed-intent"
    message = "Analyze SPCX trend and liquidity."
    with app.app_context():
        token = ai_chat._signed_intent(
            {"intent": "market_analysis", "should_execute": False}, 7, message, [], "en-US",
        )
    with stream(routing_token=token) as events:
        assert "event: done" in list(events)[-1]
    assert state["classifications"] == 0


def test_signed_routing_uses_configured_secret_when_flask_secret_is_unset(monkeypatch):
    """Production does not populate Flask's session key from the app config."""
    monkeypatch.setenv("SECRET_KEY", "test-only-persistent-routing-secret")
    app_a = Flask("routing-preflight")
    app_b = Flask("routing-stream")
    assert app_a.secret_key is None
    assert app_b.secret_key is None
    context = {"market": "USStock", "symbol": "SPCX"}
    message = "梳理 SPCX 当前最重要的下行风险。"
    with app_a.app_context():
        token = ai_chat._signed_intent(
            {"intent": "market_analysis", "should_execute": False},
            7, message, [], "zh-CN", None, context,
        )
    assert token
    with app_b.app_context():
        plan = ai_chat._verified_intent(
            token, 7, message, [], "zh-CN", None,
            {**context, "agent_intent": {"intent": "market_analysis"}},
        )
    assert plan == {"intent": "market_analysis", "should_execute": False}


def test_signed_routing_result_is_bound_to_message_and_user(stream_harness):
    state, stream = stream_harness
    app = Flask(__name__)
    app.secret_key = "test-only-signed-intent"
    with app.app_context():
        token = ai_chat._signed_intent(
            {"intent": "strategy_build", "should_execute": True}, 8,
            "Analyze SPCX trend and liquidity.", [], "en-US",
        )
    with stream(routing_token=token) as events:
        assert "event: done" in list(events)[-1]
    assert state["classifications"] == 1


def test_grounded_chart_correction_replaces_stream_instead_of_appending_broken_suffix(stream_harness, monkeypatch):
    _, stream = stream_harness
    monkeypatch.setattr(ai_chat, "_ensure_grounded_research_chart", lambda answer, context: "corrected chart")
    with stream() as events:
        result = list(events)
    assert any('event: replace' in event and 'corrected chart' in event for event in result)
    assert 'event: done' in result[-1]


def test_sync_research_and_generation_release_database_connections(stream_harness, monkeypatch):
    from types import SimpleNamespace
    state, _ = stream_harness
    def respond(*args, **kwargs):
        assert state["connections"] == 0
        return '{"answer":"Grounded answer"}'
    monkeypatch.setattr(ai_chat, "LLMService", lambda: SimpleNamespace(call_llm_api=respond))
    response = state["sync"]()
    assert response.get_json()["data"]["reply"] == "Grounded answer"
    assert state["classifications"] == 1
    assert state["connections"] == 0


def test_sync_billing_rejection_prevents_model_and_research_calls(stream_harness, monkeypatch):
    state, _ = stream_harness
    monkeypatch.setattr(ai_chat, "_charge", lambda *args: (False, "insufficient_credits", {}))
    _, code = state["sync"]()
    assert code == 402
    assert state["classifications"] == 0
    assert "enrich" not in state["events"]


def test_billing_rejection_stops_before_research_or_model_work(stream_harness, monkeypatch):
    state, stream = stream_harness
    monkeypatch.setattr(ai_chat, "_charge", lambda *args: (False, "insufficient_credits", {}))
    with stream() as events:
        result = list(events)
    assert "event: accepted" in result[0]
    assert "event: error" in result[1]
    assert "enrich" not in state["events"]
    assert "provider" not in state["events"]
    assert state["classifications"] == 0


def test_research_failure_is_reported_after_acceptance_without_resubmission(stream_harness, monkeypatch):
    state, stream = stream_harness

    def fail(*args, **kwargs):
        raise RuntimeError("data lookup failed")

    monkeypatch.setattr(ai_chat, "_enrich_context", fail)
    with stream() as events:
        result = list(events)
    assert "event: accepted" in result[0]
    assert "event: error" in result[-1]
    assert state["events"].count("user") == 1
    assert "provider" not in state["events"]
    assert state["connections"] == 0


def test_snapshot_fetches_price_and_requested_timeframes_concurrently(monkeypatch):
    rendezvous = Barrier(3, timeout=3)
    calls = []

    class MarketData:
        def get_realtime_price(self, market, symbol, **kwargs):
            calls.append((market, symbol, "price"))
            rendezvous.wait()
            return {"price": 100, "source": "fixture"}

        def get_kline(self, market, symbol, timeframe, limit, **kwargs):
            calls.append((market, symbol, timeframe))
            rendezvous.wait()
            return [{"time": 1700000000 + i * 3600, "open": 100, "high": 102,
                     "low": 99, "close": 101, "volume": 1000} for i in range(20)]

    monkeypatch.setattr(ai_chat, "KlineService", MarketData)
    snapshot = ai_chat._build_market_snapshot({"market": "USStock", "symbol": "SPCX", "snapshot_timeframes": ["1H", "1D"]})
    assert snapshot["price"]["last"] == 100
    assert list(snapshot["timeframes"]) == ["1H", "1D"]
    assert all(frame["available"] for frame in snapshot["timeframes"].values())
    assert sorted(calls) == [("USStock", "SPCX", item) for item in ["1D", "1H", "price"]]


def test_snapshot_partial_failure_preserves_other_evidence(monkeypatch):
    class MarketData:
        def get_realtime_price(self, *args, **kwargs):
            return {"price": 100}

        def get_kline(self, *args, **kwargs):
            raise RuntimeError("history unavailable")

    monkeypatch.setattr(ai_chat, "KlineService", MarketData)
    snapshot = ai_chat._build_market_snapshot({"market": "USStock", "symbol": "SPCX", "snapshot_timeframes": ["1D"]})
    assert snapshot["price"]["last"] == 100
    assert snapshot["timeframes"]["1D"]["available"] is False
    assert snapshot["timeframes"]["1D"]["error"] == "history unavailable"

"""The interactive strategy path must stream and honour cancellation."""

import json

from app.routes import strategy as route
from app.services.strategy_validation_isolated import validate_strategy_candidate_isolated


def _consume(generator):
    events = []
    while True:
        try:
            events.append(next(generator))
        except StopIteration as complete:
            return events, complete.value


def test_strategy_completion_streams_each_delta(monkeypatch):
    monkeypatch.setattr(route, "generation_cancelled", lambda *_: False)

    class LLM:
        def get_code_generation_model(self):
            return "test-model"

        def stream_llm_api(self, *_args, **_kwargs):
            yield "def init"
            yield "ialize(): pass"

    events, result = _consume(route._stream_strategy_completion(
        LLM(), [{"role": "user", "content": "test"}], temperature=0.2,
        user_id=7, request_id="test", phase="generation",
    ))
    assert result == "def initialize(): pass"
    assert [json.loads(event.split("data: ", 1)[1])["text"] for event in events] == [
        "def init", "ialize(): pass",
    ]


def test_strategy_completion_stops_and_closes_provider(monkeypatch):
    checks = iter([False, True])
    monkeypatch.setattr(route, "generation_cancelled", lambda *_: next(checks, True))
    closed = []

    class LLM:
        def get_code_generation_model(self):
            return "test-model"

        def stream_llm_api(self, *_args, **_kwargs):
            try:
                yield "first"
                yield "second"
            finally:
                closed.append(True)

    events, result = _consume(route._stream_strategy_completion(
        LLM(), [], temperature=0.2, user_id=7, request_id="test",
        phase="generation",
    ))
    assert result is None
    assert len(events) == 1
    assert closed == [True]


def test_strategy_repair_is_streamed_then_validated(monkeypatch):
    monkeypatch.setattr(route, "generation_cancelled", lambda *_: False)
    monkeypatch.setattr(route, "render_strategy_capability_repairs", lambda *_: "")
    monkeypatch.setattr(route, "resolve_strategy_validation_intent", lambda **_: object())
    monkeypatch.setattr(route, "validate_strategy_candidate_isolated", lambda code, **_: (
        {"success": False, "error": "invalid source"} if code == "bad" else
        {"success": True, "manifest": {"strategyType": "cta"}, "behavior": {"executed": True}}
    ))

    class LLM:
        def get_code_generation_model(self):
            return "test-model"

        def stream_llm_api(self, *_args, **_kwargs):
            yield "```python\n"
            yield "good\n```"

    events, result = _consume(route._stream_validate_strategy(
        LLM(), "prompt", "bad", asset_type="script", generation_mode="authoring",
        context={}, system_prompt="system", existing_code="", user_id=7,
        request_id="test",
    ))
    assert result[0] == "good"
    assert result[2] == {"executed": True}
    assert events[0].startswith("event: progress")
    assert sum(event.startswith("event: delta") for event in events) == 2


def test_strategy_validation_runs_in_clean_child_without_loading_deployment_env():
    code = '''
def initialize(context):
    context.set_universe(["USStock:SPCX"])
    context.subscribe(frequency="1d")

def handle_data(context, data):
    return None
'''
    result = validate_strategy_candidate_isolated(
        code, prompt="Create a daily SPCX strategy", existing_code="",
        asset_type="script", generation_mode="authoring",
        context={"market": "USStock", "symbol": "SPCX"},
    )
    assert result["success"] is True
    assert result["manifest"]["strategyType"] == "cta"
    assert result["behavior"]["executed"] is False

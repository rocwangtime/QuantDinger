"""The interactive strategy path must stream and honour cancellation."""

import json

from app.routes import strategy_stream_routes as route


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

    events, result = _consume(route._stream_completion(
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

    events, result = _consume(route._stream_completion(
        LLM(), [], temperature=0.2, user_id=7, request_id="test",
        phase="generation",
    ))
    assert result is None
    assert len(events) == 1
    assert closed == [True]

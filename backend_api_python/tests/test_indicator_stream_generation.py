"""The indicator authoring model must emit real provider deltas and stop safely."""

from app.services import indicator_ai_generation as generation


class _Provider:
    value = "test"


class _FakeLLM:
    provider = _Provider()

    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    def get_code_generation_model(self):
        return "test-model"

    def get_api_key(self):
        return "test-key"

    def get_base_url(self):
        return "https://example.invalid"

    def stream_llm_api(self, **kwargs):
        def stream():
            try:
                yield from self.chunks
            finally:
                self.closed = True

        return stream()


class _Logger:
    def info(self, *args):
        pass

    def warning(self, *args):
        pass


def _drain(source):
    deltas = []
    while True:
        try:
            deltas.append(next(source))
        except StopIteration as done:
            return deltas, done.value


def _request(monkeypatch, llm, **kwargs):
    from app.services import llm as llm_module

    monkeypatch.setattr(llm_module, "LLMService", lambda: llm)
    return generation.generate_indicator_code_candidate(
        prompt="show moving average", existing="", context={},
        system_prompt="Only indicator code", workspace_context=None,
        template_factory=lambda: "template", logger=_Logger(), **kwargs,
    )


def test_indicator_generation_yields_provider_chunks_before_completion(monkeypatch):
    llm = _FakeLLM(["my_indicator_name = ", "'SMA'\n"])
    source = _request(monkeypatch, llm)
    assert next(source) == ("generation", "my_indicator_name = ")
    rest, result = _drain(source)
    assert rest == [("generation", "'SMA'\n")]
    assert result[0] == "my_indicator_name = 'SMA'"
    assert llm.closed


def test_cancelled_indicator_generation_returns_no_candidate(monkeypatch):
    llm = _FakeLLM(["partial", " must not persist"])
    stopped = False
    source = _request(monkeypatch, llm, cancel_check=lambda: stopped)
    assert next(source) == ("generation", "partial")
    stopped = True
    rest, result = _drain(source)
    assert rest == []
    assert result is None
    assert llm.closed


def test_disconnect_closes_indicator_provider_stream(monkeypatch):
    llm = _FakeLLM(["first", "second"])
    source = _request(monkeypatch, llm)
    assert next(source) == ("generation", "first")
    source.close()
    assert llm.closed

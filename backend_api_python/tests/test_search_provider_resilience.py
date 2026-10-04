from app.services.search import SearchService
from app.services.search_models import SearchResponse, SearchResult


class _Provider:
    is_available = True

    def __init__(self, name, responses):
        self.name = name
        self.responses = iter(responses)
        self.calls = 0

    def search(self, query, max_results, days):
        self.calls += 1
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value


def _result(provider):
    return SearchResponse(query="SPCX news", provider=provider, success=True, results=[
        SearchResult("SPCX filing", "", "https://example.test/story", "example.test")
    ])


def test_rate_limited_provider_cools_down_without_blocking_fallback():
    limited = _Provider("GDELT", [SearchResponse(
        query="SPCX news", provider="GDELT", success=False, results=[], error_message="HTTP 429"
    )])
    fallback = _Provider("DuckDuckGo", [_result("DuckDuckGo"), _result("DuckDuckGo")])
    service = SearchService.__new__(SearchService)
    service._providers = [limited, fallback]

    assert service.search_with_fallback("SPCX news").provider == "DuckDuckGo"
    assert service.search_with_fallback("SPCX news").provider == "DuckDuckGo"
    assert limited.calls == 1
    assert fallback.calls == 2
    assert service._cooldown_until["GDELT"] > 0


def test_provider_is_retried_after_cooldown_expires(monkeypatch):
    from app.services import search

    clock = [100.0]
    monkeypatch.setattr(search.time, "monotonic", lambda: clock[0])
    limited = _Provider("GDELT", [SearchResponse(
        query="SPCX news", provider="GDELT", success=False, results=[], error_message="HTTP 429"
    ), _result("GDELT")])
    fallback = _Provider("DuckDuckGo", [_result("DuckDuckGo")])
    service = SearchService.__new__(SearchService)
    service._providers = [limited, fallback]

    assert service.search_with_fallback("SPCX news").provider == "DuckDuckGo"
    assert service._provider_cooling_down(limited)
    clock[0] += 301
    assert service.search_with_fallback("SPCX news").provider == "GDELT"
    assert limited.calls == 2


def test_provider_exception_falls_back_and_empty_result_is_not_cooled_down():
    broken = _Provider("GDELT", [RuntimeError("socket disconnected")])
    empty = _Provider("DuckDuckGo", [SearchResponse(
        query="SPCX news", provider="DuckDuckGo", success=False, results=[], error_message=None
    )])
    good = _Provider("Tavily", [_result("Tavily")])
    service = SearchService.__new__(SearchService)
    service._providers = [broken, empty, good]

    assert service.search_with_fallback("SPCX news").provider == "Tavily"
    assert "DuckDuckGo" not in service.__dict__.get("_cooldown_until", {})

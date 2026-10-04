from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pytest

from app.services.market_data_collector import MarketDataCollector
from app.services.search_models import SearchResponse, SearchResult


class _FakeFinnhubClient:
    def __init__(self, timestamp: int) -> None:
        self.timestamp = timestamp

    def company_news(self, *_args, **_kwargs):
        return [{
            "datetime": self.timestamp,
            "headline": "Confirmed company update",
            "summary": "A timestamp conversion fixture.",
            "source": "Finnhub",
            "url": "https://example.test/news",
        }]


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="requires POSIX timezone control")
def test_finnhub_unix_timestamp_is_utc_when_server_uses_asia_shanghai(monkeypatch):
    expected = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
    collector = MarketDataCollector.__new__(MarketDataCollector)
    collector._finnhub_client = _FakeFinnhubClient(int(expected.timestamp()))
    collector._get_news_from_search = lambda *_args, **_kwargs: []
    monkeypatch.setenv("FINNHUB_FREE_ONLY", "true")

    previous_timezone = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "Asia/Shanghai"
        time.tzset()
        result = collector._get_news("USStock", "TSLA")
    finally:
        if previous_timezone is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_timezone
        time.tzset()

    assert result["news"][0]["datetime"] == "2026-09-08T12:00:00Z"


def test_undated_search_result_does_not_inherit_retrieval_date():
    collector = MarketDataCollector.__new__(MarketDataCollector)
    collector._finnhub_client = None
    collector._get_news_from_search = lambda *_args, **_kwargs: [{
        "datetime": "", "headline": "SPCX update", "source": "search",
        "url": "https://example.test/story",
    }]
    result = collector._get_news("USStock", "SPCX")
    assert result["news"][0]["datetime"] == ""


def test_search_news_preserves_unknown_publication_time(monkeypatch):
    from app.services import search

    collector = MarketDataCollector.__new__(MarketDataCollector)
    fake_service = type("Search", (), {
        "is_available": True,
        "search_stock_news": lambda self, **kwargs: SearchResponse(
            query="SPCX", provider="DuckDuckGo", results=[SearchResult(
                title="SPCX update", snippet="Report", url="https://example.test/story",
                source="example.test", published_date=None,
            )],
        ),
    })()
    monkeypatch.setattr(search, "get_search_service", lambda: fake_service)
    assert collector._get_news_from_search("USStock", "SPCX")[0]["datetime"] == ""

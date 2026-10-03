from datetime import datetime, timedelta, timezone
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from app.data_providers import sentiment
from app.services.market_data_collector import MarketDataCollector, _macro_observation


def observation(**changes):
    return {"value": 18.4, "source": "yfinance", "data_time": datetime.now(timezone.utc).isoformat(), **changes}


@pytest.mark.parametrize("changes", [
    {"source": ""}, {"source": "N/A"}, {"is_fallback": True}, {"is_estimate": True},
    {"data_time": None}, {"value": float("nan")}, {"value": 0},
    {"data_time": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()},
])
def test_unverified_macro_values_are_not_agent_evidence(changes):
    assert _macro_observation(observation(**changes), "VIX", "USStock") is None


def test_valid_macro_preserves_source_and_date():
    result = _macro_observation(observation(), "VIX", "USStock")
    assert result["price"] == 18.4
    assert result["source"] == "yfinance"
    assert result["data_time"]


def test_crypto_fear_greed_is_not_equity_sentiment():
    data = observation(value=67, source="alternative.me")
    assert _macro_observation(data, "FEAR_GREED", "USStock") is None
    assert _macro_observation(data, "FEAR_GREED", "Crypto")["scope"] == "crypto"


def test_cached_defaults_and_fetched_estimates_are_excluded(monkeypatch):
    monkeypatch.setattr("app.data_providers.get_cached", lambda *args: {"vix": {"value": 18}})
    monkeypatch.setattr(sentiment, "fetch_vix", lambda: observation())
    monkeypatch.setattr(sentiment, "fetch_dollar_index", lambda: observation(is_estimate=True))
    monkeypatch.setattr(sentiment, "fetch_yield_curve", lambda: {"yield_10y": 4.2})
    monkeypatch.setattr(sentiment, "fetch_fear_greed_index", lambda: pytest.fail("Must not fetch crypto sentiment for stocks"))
    result = MarketDataCollector.__new__(MarketDataCollector)._get_macro_data("USStock")
    assert set(result) == {"VIX"}
    assert result["VIX"]["source"] == "yfinance"


def test_provider_defaults_and_fx_proxy_are_explicitly_untrusted(monkeypatch):
    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(Ticker=lambda *args: SimpleNamespace(history=lambda **kwargs: pd.DataFrame())))
    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(
        index_vix=lambda: pd.DataFrame(),
        currency_boc_sina=lambda **kwargs: pd.DataFrame({"中行汇买价": [728.0]}),
    ))
    assert sentiment.fetch_vix()["is_fallback"] is True
    assert sentiment.fetch_dollar_index()["is_estimate"] is True
    assert sentiment.fetch_yield_curve()["is_fallback"] is True

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.futu_trading.execution_quote import describe_futu_quote


def test_futu_quote_reports_source_time_but_never_execution_eligible():
    stamp = "2026-09-29 09:30:01"
    now = datetime(2026, 9, 29, 9, 30, 2, tzinfo=ZoneInfo("America/New_York")).timestamp()
    result = describe_futu_quote("spy", {
        "last": 650.5, "bid": 650.4, "ask": 650.6,
        "raw": {"update_time": stamp},
    }, now=now)
    assert result["provider"] == "futu_opend_snapshot"
    assert result["is_stale"] is False
    assert result["delay_ms"] == 1000
    assert result["is_realtime"] is False
    assert result["execution_eligible"] is False
    assert result["quote_level"] == "UNVERIFIED"


def test_missing_or_old_timestamp_is_stale():
    missing = describe_futu_quote("SPY", {"last": 650}, now=100)
    assert missing["is_stale"] is True
    assert missing["as_of"] is None
    old = describe_futu_quote("SPY", {
        "last": 650, "raw": {"update_time": "2026-09-29 09:30:01"},
    }, now=datetime(2026, 9, 29, 9, 30, 20, tzinfo=ZoneInfo("America/New_York")).timestamp())
    assert old["is_stale"] is True


def test_future_timestamp_is_stale_instead_of_appearing_instantaneous():
    now = datetime(2026, 9, 29, 9, 30, 1, tzinfo=ZoneInfo("America/New_York")).timestamp()
    result = describe_futu_quote("SPY", {
        "last": 650, "raw": {"update_time": "2026-09-29 09:31:01"},
    }, now=now)
    assert result["is_stale"] is True
    assert result["delay_ms"] == -60000
    assert result["execution_eligible"] is False

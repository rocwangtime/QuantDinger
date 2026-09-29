from datetime import datetime

import pytest

from app.services.backtest_limits import (
    BacktestRangeLimitError,
    backtest_range_policy_metadata,
    validate_backtest_range,
)
from app.services.strategy_v2.service import (
    StrategyV2BacktestService,
    _backtest_member_data_kwargs,
)


def test_forex_intraday_range_error_includes_actionable_recommendation():
    err = validate_backtest_range(
        market="Forex",
        symbol="EURUSD",
        timeframe="15m",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 4, 1, 23, 59, 59),
    )

    assert err is not None
    assert err["error_type"] == "BACKTEST_RANGE_LIMIT"
    assert err["max_days"] == 60
    assert err["recommendation_available"] is True
    assert err["recommended_start"] == "2024-02-02"
    assert err["recommended_end"] == "2024-02-29"
    assert "Suggested fix: use 2024-02-02 to 2024-04-01" in err["msg"]
    assert "set end date to 2024-02-29" in err["msg"]


def test_recommendation_accounts_for_indicator_warmup_bars():
    err = validate_backtest_range(
        market="Forex",
        symbol="EURUSD",
        timeframe="15m",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 4, 1, 23, 59, 59),
        warmup_bars=96,
    )

    assert err is not None
    assert err["warmup_bars"] == 96
    assert err["warmup_days"] == 1
    assert err["fetch_start"] == "2023-12-31"
    assert err["recommended_start"] == "2024-02-03"
    assert err["recommended_end"] == "2024-02-28"
    assert "including 96 warmup bars" in err["msg"]


def test_range_equal_to_limit_is_allowed():
    err = validate_backtest_range(
        market="Forex",
        symbol="EURUSD",
        timeframe="15m",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 3, 1, 0, 0, 0),
    )

    assert err is None


def test_warmup_larger_than_policy_has_no_fake_date_recommendation():
    err = validate_backtest_range(
        market="USStock",
        symbol="TSLA",
        timeframe="1m",
        start_date=datetime(2024, 1, 10),
        end_date=datetime(2024, 1, 10, 23, 59, 59),
        warmup_bars=60 * 24 * 31,
    )

    assert err is not None
    assert err["max_days"] == 30
    assert err["warmup_days"] == 31
    assert err["recommendation_available"] is False
    assert err["recommended_start"] is None
    assert err["recommended_end"] is None
    assert "warmup alone exceeds" in err["msg"]


def test_policy_metadata_uses_strictest_market_and_normalizes_timeframe():
    policy = backtest_range_policy_metadata(
        markets=["Crypto", "USStock"],
        timeframe="1h",
        warmup_bars=24,
    )

    assert policy["timeframe"] == "1H"
    assert policy["market"] == "USStock"
    assert policy["maxDays"] == 700
    assert policy["warmupDays"] == 13
    assert policy["maxSelectedDays"] == 687


def test_us_stock_minute_policy_includes_equity_warmup_calendar_buffer():
    policy = backtest_range_policy_metadata(
        markets=["USStock"], timeframe="1m", warmup_bars=3
    )

    assert policy["warmupDays"] == 7
    assert policy["maxSelectedDays"] == 23


def test_futu_backtest_data_source_requires_explicit_opt_in(monkeypatch):
    member = {"market": "USStock", "symbol": "SPY"}
    monkeypatch.delenv("STRATEGY_V2_BACKTEST_STOCK_DATA_SOURCE", raising=False)
    assert _backtest_member_data_kwargs(member, None, False)["exchange_id"] == ""

    monkeypatch.setenv("STRATEGY_V2_BACKTEST_STOCK_DATA_SOURCE", "futu")
    selected = _backtest_member_data_kwargs(member, None, False)
    assert selected["exchange_id"] == "futu"
    assert selected["strict_data_source"] is True
    assert _backtest_member_data_kwargs({"market": "Crypto", "symbol": "BTC/USDT"}, None, False)["exchange_id"] == ""
    assert _backtest_member_data_kwargs({**member, "exchange_id": "ibkr"}, None, False)["exchange_id"] == "ibkr"
    assert _backtest_member_data_kwargs(member, {"exchange_id": "alpaca"}, False)["exchange_id"] == ""


def test_service_rejects_one_year_of_one_minute_data_before_fetching():
    code = '''
def initialize(context):
    context.set_universe(["Crypto:BTC/USDT"])
    context.subscribe(frequency="1m")

def handle_data(context, data):
    pass
'''

    def unexpected_fetch(*_args, **_kwargs):
        raise AssertionError("market data must not be fetched for an oversized request")

    service = StrategyV2BacktestService(
        repository=object(),
        universe_service=object(),
        frame_fetcher=unexpected_fetch,
        snapshot_store=object(),
    )

    with pytest.raises(BacktestRangeLimitError) as caught:
        service.run(
            user_id=1,
            code=code,
            start_date=datetime(2025, 7, 19),
            end_date=datetime(2026, 7, 19, 23, 59, 59),
            initial_capital=10_000,
            persist=False,
        )

    assert caught.value.details["error_type"] == "BACKTEST_RANGE_LIMIT"
    assert caught.value.details["timeframe"] == "1m"
    assert caught.value.details["max_days"] == 30

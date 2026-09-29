"""Shared multi-timeframe loading for live Strategy API V2 sessions."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Callable

import pandas as pd

from app.data_sources.errors import (
    MarketDataFailure,
    MarketDataUnavailableError,
    classify_market_data_failure,
)
from app.services.fundamental_data import get_fundamental_data_service
from app.services.strategy_v2.frequencies import frequency_seconds
from app.services.strategy_v2.models import StrategyManifest
from app.services.strategy_v2.service import StrategyV2BacktestService


_STOCK_INTRADAY_HISTORY_DAY_CAPS = {
    "1m": 7,
    "3m": 7,
    "5m": 59,
    "15m": 59,
    "30m": 59,
}


def live_history_days(
    frequency: str,
    warmup_bars: int,
    candidates: list[dict[str, object]] | tuple = (),
) -> int:
    """Return a frequency-aware live lookback with a startup buffer."""
    bars = max(10, max(1, int(warmup_bars or 0)) * 3)
    seconds = frequency_seconds(frequency) * bars
    days = max(1, int(math.ceil(seconds / 86_400)))
    normalized = str(frequency or "").strip().lower()
    stock_session = any(
        str(item.get("market") or "") in {"USStock", "HKStock", "CNStock", "AStock"}
        or str(item.get("underlying_market") or "") in {"USStock", "HKStock", "CNStock", "AStock"}
        or str(item.get("api_family") or "").strip().lower() == "stock"
        for item in candidates
    )
    if stock_session and normalized.endswith(("m", "h")):
        hours = float(normalized[:-1]) / (60 if normalized.endswith("m") else 1)
        session_days = math.ceil(max(1, warmup_bars) * 3 * hours / 4 * 7 / 5 * 1.5)
        days = max(days, 7, session_days)
        provider_cap = _STOCK_INTRADAY_HISTORY_DAY_CAPS.get(normalized)
        if provider_cap is not None:
            days = min(days, provider_cap)
    return days


def completed_bar_token(frequency: str, now: datetime | None = None) -> int:
    """Return a stable token for the latest fully completed UTC candle."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    seconds = max(1, frequency_seconds(frequency))
    return int(current.timestamp()) // seconds - 1


def daily_equity_execution_policy(frequency: str, candidates, *, execution_mode: str, schedules=()):
    from app.services.market_schedule import equity_data_market

    if execution_mode != "live" or frequency != "1d" or schedules or not candidates:
        return None
    markets = {equity_data_market(str(member.get("market") or ""), member) for member in candidates}
    if len(markets) != 1 or not markets.issubset({"USStock", "HKStock"}):
        return None
    return markets.pop(), any(member.get("market") != "Crypto" for member in candidates)


def equity_daily_frames_ready(frames, candidates, signal_session: datetime, market: str) -> bool:
    """Do not consume a signal while any constituent still has yesterday's data."""
    from app.services.market_schedule import equity_bar_session_date

    expected = signal_session.date()
    for member in candidates:
        frame = frames.get(str(member["key"]))
        if frame is None or frame.empty or equity_bar_session_date(frame.index[-1], market) != expected:
            return False
    return True


def load_live_frequency_frames(
    *,
    service: StrategyV2BacktestService,
    candidates: list[dict[str, object]],
    manifest: StrategyManifest,
    end_date: datetime,
    exchange_config: dict[str, object] | None = None,
    strict_data_source: bool = False,
    warn: Callable[[str], None] | None = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Load a complete live frame bundle for all declared strategy timeframes."""
    start_dates = {
        frequency: end_date
        - timedelta(days=live_history_days(frequency, manifest.warmup_bars, candidates))
        for frequency in manifest.frequencies
    }
    fetch_options = {}
    if exchange_config is not None:
        fetch_options["exchange_config"] = exchange_config
    if strict_data_source:
        fetch_options["strict_data_source"] = True
    bundles, skipped = service.fetch_frequency_frames(
        candidates,
        manifest.frequencies,
        start_dates,
        end_date,
        **fetch_options,
    )
    driving_frequency = manifest.driving_frequency
    driving_frames = bundles.get(driving_frequency, {})
    if not driving_frames:
        structured = [
            item.get("market_data_error")
            for item in skipped
            if isinstance(item.get("market_data_error"), dict)
        ]
        priority = {
            "region_restricted": 0,
            "proxy_failure": 1,
            "symbol_not_found": 2,
            "unsupported_timeframe": 3,
            "rate_limited": 4,
            "exchange_unavailable": 5,
            "no_market_data": 6,
        }
        if structured:
            selected = min(
                structured,
                key=lambda value: priority.get(str(value.get("code") or ""), 99),
            )
            raise MarketDataUnavailableError(MarketDataFailure.from_mapping(selected))
        detail = "; ".join(str(item.get("reason") or "") for item in skipped[:3])
        raise MarketDataUnavailableError(
            classify_market_data_failure(
                detail or "No usable market data",
                symbol=str(candidates[0].get("symbol") or "") if candidates else "",
                timeframe=driving_frequency,
            )
        )

    if skipped and warn:
        details = ", ".join(
            f"{item.get('symbol') or '?'}@{item.get('frequency') or '?'}:"
            f"{item.get('reason') or 'unavailable'}"
            for item in skipped[:5]
        )
        suffix = f" ({details})" if details else ""
        warn(
            f"Skipped {len(skipped)} instrument/timeframe data source(s)"
            f" without usable market data{suffix}"
        )

    if manifest.fundamental_dependencies:
        driving_frames = get_fundamental_data_service().enrich_panel(
            driving_frames,
            candidates,
        )
        bundles[driving_frequency] = driving_frames
        service.validate_fundamental_dependencies(driving_frames, manifest)
    return bundles


__all__ = ["completed_bar_token", "live_history_days", "load_live_frequency_frames"]

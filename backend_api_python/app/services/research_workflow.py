"""Research workflow controls. None of these helpers authorize or place orders."""
from __future__ import annotations

import math
import os
import re
from datetime import datetime, timezone


def research_data_options(market: str) -> dict:
    """Prefer the configured quote-only OpenD path; factory retains public fallback."""
    if market in {'USStock', 'HKStock'} and os.getenv('FUTU_OPEND_HOST'):
        return {'exchange_id': 'futu'}
    return {}


def explicit_strategy_creation(message: str) -> bool:
    """Route artifact requests deterministically, not strategy advice/questions."""
    text = str(message or "").lower()
    if re.search(r"^\s*(?:如何|怎么|为什么|what\b|how\b|why\b)", text):
        return False
    if re.search(r"(?:不要|先不|暂不|别)\s*(?:生成|写|创建|开发)|(?:do not|don't)\s+(?:generate|create|build|write)", text):
        return False
    artifact = re.search(r"策略|strategy|python|脚本", text)
    verb = re.search(r"生成|编写|写一个|写出|创建|开发|generate|create|build|write", text)
    executable = re.search(r"回测|保存|代码|脚本|python|固定规则|backtest|runnable|code", text)
    return bool(artifact and verb and executable)


def market_clock(market: str, now: datetime | None = None) -> dict:
    """Exchange calendar includes holidays, DST and HK lunch breaks; fail closed."""
    now = now or datetime.now(timezone.utc)
    if market == "Crypto":
        return {"available": True, "timezone": "UTC", "is_open": True,
                "as_of_utc": now.isoformat(), "session": "continuous"}
    calendars = {"USStock": ("XNYS", "America/New_York"), "HKStock": ("XHKG", "Asia/Hong_Kong")}
    if market not in calendars:
        return {"available": False, "reason": "Calendar not supported for this market"}
    try:
        import exchange_calendars as xcals
        import pandas as pd
        name, tz = calendars[market]
        cal = xcals.get_calendar(name)
        minute = pd.Timestamp(now).floor("min")
        opened = bool(cal.is_open_on_minute(minute, ignore_breaks=False))
        next_open = cal.next_open(minute)
        previous_close = cal.previous_close(minute)
        return {"available": True, "calendar": name, "timezone": tz,
                "as_of_utc": now.isoformat(), "is_open": opened,
                "session": "regular" if opened else "closed",
                "next_open": next_open.tz_convert(tz).isoformat(),
                "previous_close": previous_close.tz_convert(tz).isoformat(),
                "minutes_since_close": (minute - previous_close).total_seconds() / 60}
    except Exception:
        return {"available": False, "reason": "Exchange calendar unavailable"}


def normalize_research_config(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("Research config must be an object")
    config = dict(raw)
    if 'llm_selection' in config:
        from app.services.llm_selection import validate_selection
        config['llm_selection'] = validate_selection(config['llm_selection'])
    interval = int(config.get("run_interval_minutes") or config.get("interval_minutes") or 60)
    if interval < 5 or interval > 10080:
        raise ValueError("Research interval must be between 5 and 10080 minutes")
    config["run_interval_minutes"] = interval
    prompt = str(config.get("prompt") or config.get("focus_conditions") or "").strip()
    if len(prompt) > 12000:
        raise ValueError("Research brief must be at most 12000 characters")
    config["prompt"] = prompt
    window = config.get("session_window") or "always"
    if window not in {"always", "regular", "after_close"}:
        raise ValueError("Invalid research session window")
    if window != "always" and config.get("market") not in {"USStock", "HKStock"}:
        raise ValueError("Session windows currently support US and HK stocks only")
    config["session_window"] = window
    trigger = config.get("trigger") or {"type": "scheduled"}
    if not isinstance(trigger, dict) or trigger.get("type") not in {"scheduled", "price_above", "price_below"}:
        raise ValueError("Invalid research trigger")
    kind = trigger["type"]
    if kind != "scheduled":
        value = float(trigger.get("price") or 0)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Trigger price must be positive and finite")
        if not config.get("symbol") or not config.get("market"):
            raise ValueError("Price triggers require one market and symbol")
        config["trigger"] = {"type": kind, "price": value}
    else:
        config["trigger"] = {"type": "scheduled"}
    return config


def research_gate(config: dict, *, now: datetime | None = None, candles=None) -> dict:
    """Only cheap market checks before any billing/LLM call; not an order gate."""
    now = now or datetime.now(timezone.utc)
    window = config.get("session_window") or "always"
    clock = market_clock(config.get("market"), now)
    if window != "always":
        if not clock.get("available"):
            return {"allowed": False, "reason": "Exchange calendar unavailable", "market_clock": clock}
        if window == "regular" and not clock["is_open"]:
            return {"allowed": False, "reason": "Outside regular trading session", "market_clock": clock}
        if window == "after_close" and (clock["is_open"] or not 0 <= clock.get("minutes_since_close", -1) < 60):
            return {"allowed": False, "reason": "Outside first hour after close", "market_clock": clock}
    trigger = config.get("trigger") or {"type": "scheduled"}
    if trigger["type"] == "scheduled":
        return {"allowed": True, "market_clock": clock}
    # Never treat a stale/unfinished candle as a currently satisfied price event.
    valid = []
    for bar in candles or []:
        try:
            ts, price = float(bar["time"]), float(bar["close"])
            if math.isfinite(price) and price > 0 and 60 <= now.timestamp() - ts <= 300:
                valid.append((ts, price))
        except (KeyError, TypeError, ValueError):
            continue
    if not valid:
        return {"allowed": False, "needs_candles": candles is None,
                "reason": "No fresh closed 1-minute candle for price trigger", "market_clock": clock}
    ts, price = max(valid)
    satisfied = price >= trigger["price"] if trigger["type"] == "price_above" else price <= trigger["price"]
    return {"allowed": satisfied, "reason": "Price condition met" if satisfied else "Price condition not met",
            "observed_price": price, "candle_time": ts, "market_clock": clock}

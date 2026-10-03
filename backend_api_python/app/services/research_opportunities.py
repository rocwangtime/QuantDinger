"""Research-only opportunity inbox derived from completed monitor runs.

An AI BUY/SELL outlook is a review lead, never a trade signal or authorization.
The source run remains immutable evidence; this table stores only workflow state.
"""

from __future__ import annotations

import re
import math
from typing import Any

_MARKETS = {"USStock", "HKStock"}
_SYMBOL = re.compile(r"[A-Z0-9.]{1,20}\Z")
_ANALYSIS_FIELDS = (
    "market", "symbol", "name", "final_decision", "confidence", "reasoning",
    "risk_report", "suggested_entry", "suggested_stop_loss", "suggested_take_profit",
)


def eligible_opportunities(result: Any) -> list[tuple[str, str]]:
    """Extract stock entry/exit leads without trusting report prose."""
    if not isinstance(result, dict) or result.get("success") is not True:
        return []
    analyses = result.get("position_analyses")
    if not isinstance(analyses, list):
        return []
    seen: set[tuple[str, str]] = set()
    for item in analyses:
        if not isinstance(item, dict) or item.get("error"):
            continue
        market = str(item.get("market") or "").strip()
        symbol = str(item.get("symbol") or "").strip().upper()
        decision = str(item.get("final_decision") or "").strip().upper()
        if market in _MARKETS and _SYMBOL.fullmatch(symbol) and decision in {"BUY", "SELL"}:
            seen.add((market, symbol))
    return sorted(seen)


def public_analysis(result: Any, market: str, symbol: str) -> dict:
    """Return only the matching bounded snapshot, never the full HTML report."""
    analyses = result.get("position_analyses") if isinstance(result, dict) else None
    if not isinstance(analyses, list):
        return {}
    for item in analyses:
        if not isinstance(item, dict):
            continue
        if str(item.get("market") or "").strip() != market:
            continue
        if str(item.get("symbol") or "").strip().upper() != symbol:
            continue
        out = {key: item.get(key) for key in _ANALYSIS_FIELDS}
        out["name"] = str(out.get("name") or "")[:100]
        for key in ("reasoning", "risk_report"):
            out[key] = str(out.get(key) or "")[:1200]
        for key in ("confidence", "suggested_entry", "suggested_stop_loss", "suggested_take_profit"):
            try:
                value = float(out[key])
                out[key] = value if math.isfinite(value) else None
            except (TypeError, ValueError):
                out[key] = None
        return out
    return {}


def insert_opportunities(cur, *, monitor_id: int, user_id: int, run_id: int, result: Any) -> int:
    """Queue each eligible symbol once in the same transaction as its run."""
    leads = eligible_opportunities(result)
    for market, symbol in leads:
        cur.execute(
            """INSERT INTO qd_research_opportunities
               (monitor_id, user_id, run_id, market, symbol, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'new', NOW(), NOW())
               ON CONFLICT (run_id, market, symbol) DO NOTHING""",
            (int(monitor_id), int(user_id), int(run_id), market, symbol),
        )
    return len(leads)

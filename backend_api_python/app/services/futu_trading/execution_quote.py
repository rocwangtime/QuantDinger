"""Provenance envelope for read-only Futu quote diagnostics.

No execution code may treat this envelope as authorization: Futu market-data
permissions and real-time entitlement have not been independently verified.
"""

from __future__ import annotations

import math
import time
from typing import Any

from app.services.futu_trading.timezones import futu_time_key_to_timestamp


def describe_futu_quote(symbol: str, snapshot: dict[str, Any], *, now: float | None = None) -> dict:
    received = float(now if now is not None else time.time())
    raw = snapshot.get("raw") if isinstance(snapshot.get("raw"), dict) else {}
    stamp = raw.get("update_time") or raw.get("time_key") or raw.get("time")
    try:
        as_of = futu_time_key_to_timestamp(stamp, "USStock") if stamp else None
    except (TypeError, ValueError):
        as_of = None
    try:
        price = float(snapshot.get("last") or 0)
        bid = float(snapshot.get("bid") or 0)
        ask = float(snapshot.get("ask") or 0)
    except (TypeError, ValueError):
        price = bid = ask = 0.0
    if not all(math.isfinite(value) and value >= 0 for value in (price, bid, ask)):
        price = bid = ask = 0.0
    age = received - as_of if as_of is not None else None
    # A clock or timezone error must not turn a future-dated quote into a
    # seemingly fresh one. Two seconds of skew is tolerated for transport.
    stale = age is None or age < -2 or age > 10 or price <= 0
    return {
        "symbol": symbol.upper(),
        "market": "US",
        "price": price or None,
        "bid": bid or None,
        "ask": ask or None,
        "provider": "futu_opend_snapshot",
        "broker": "futu",
        "as_of": as_of,
        "received_at": received,
        "delay_ms": int(age * 1000) if age is not None else None,
        "is_stale": stale,
        "is_realtime": False,
        "market_status": "UNKNOWN",
        "quote_level": "UNVERIFIED",
        "execution_eligible": False,
        "reason": "realtime_entitlement_not_verified",
    }

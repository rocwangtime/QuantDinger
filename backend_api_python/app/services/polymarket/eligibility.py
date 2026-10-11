"""Read-only Polymarket geoblock probe. A positive result never grants trading authority."""
import ipaddress
import os
import threading
import time
from datetime import datetime, timezone

import requests

URL = "https://polymarket.com/api/geoblock"
CLOSE_ONLY = {"AU", "BY", "BE", "BI", "BR", "CA-BC", "CA-ON", "CA-AB", "CA-QC", "CF", "CD", "ET", "FR", "DE", "IQ", "IT", "LB", "LY", "MM", "NZ", "NI", "PL", "RU", "SG", "SO", "SK", "SS", "SD", "TW", "TH", "GB", "US", "UM", "VE", "YE", "ZW"}
_CACHE = None
_LOCK = threading.Lock()
_LAST_REFRESH = 0.0


def derive(data, *, error=None, cached=False, now=None):
    now = now or datetime.now(timezone.utc)
    if error or not isinstance(data, dict) or not isinstance(data.get("blocked"), bool):
        return {"status": "unknown", "blocked": None, "country": None, "region": None, "ip": None,
                "checked_at": now.isoformat(), "cached": cached, "error": "probe_unavailable" if error else "invalid_response"}
    country, region = data.get("country"), data.get("region")
    try:
        country = country.upper() if isinstance(country, str) and len(country) == 2 and country.isalpha() else None
        ipaddress.ip_address(data.get("ip", ""))
    except (ValueError, TypeError):
        country = None
    if country is None:
        return {"status": "unknown", "blocked": None, "country": None, "region": None, "ip": None,
                "checked_at": now.isoformat(), "cached": cached, "error": "invalid_response"}
    region = region.upper() if isinstance(region, str) and len(region) <= 8 else None
    jurisdiction = f"{country}-{region}" if country and region else country
    blocked = data["blocked"]
    status = "blocked" if blocked else "close_only" if jurisdiction in CLOSE_ONLY or country in CLOSE_ONLY else "eligible"
    ip = None
    try:
        parsed = ipaddress.ip_address(data.get("ip", ""))
        octets = str(parsed).split(".") if parsed.version == 4 else None
        ip = ".".join(octets[:2] + ["x", "x"]) if octets else ":".join(str(parsed).split(":")[:3]) + ":…"
    except ValueError:
        pass
    return {"status": status, "blocked": blocked, "country": country, "region": region, "ip": ip,
            "checked_at": now.isoformat(), "cached": cached, "error": None}


def check(*, refresh=False, session=requests, clock=time.monotonic):
    global _CACHE, _LAST_REFRESH
    ttl = max(1, int(os.getenv("POLYMARKET_GEOBLOCK_TTL_SECONDS", "900")))
    with _LOCK:
        if _CACHE and not refresh and clock() - _LAST_REFRESH < ttl:
            return {**_CACHE, "cached": True}
        min_interval = max(5, int(os.getenv("POLYMARKET_GEOBLOCK_REFRESH_MIN_SECONDS", "30")))
        if refresh and _CACHE and clock() - _LAST_REFRESH < min_interval:
            return {**_CACHE, "cached": True}
        try:
            response = session.get(URL, timeout=(2, 4))
            if response.status_code != 200:
                result = derive(None, error="http_error")
            else:
                result = derive(response.json())
        except (requests.RequestException, ValueError, TypeError):
            result = derive(None, error="network_error")
        _CACHE, _LAST_REFRESH = result, clock()
        return dict(result)


def require_live_eligibility():
    """Future live entry points must call this; all non-eligible states fail closed."""
    return check().get("status") == "eligible"

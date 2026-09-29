"""Bounded Futu quote cache for strategy risk ticks.

Subscribes (best-effort) to OpenD QUOTE for the strategy's symbols and
refreshes a local last-price cache. Falls back to snapshot polling when
push is unavailable.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Iterable, Mapping, Optional

from app.utils.logger import get_logger

logger = get_logger(__name__)


def fresh_futu_quote_prices(snapshot: Mapping[str, Any]) -> Dict[str, float]:
    """Never refresh the strategy's freshness clock from an old cache."""
    if not snapshot.get("connected") or snapshot.get("source") != "futu_quote":
        return {}
    return dict(snapshot.get("prices") or {})


class FutuQuoteFeed:
    """Per-runtime quote cache backed by FutuClient.get_quote / subscribe."""

    def __init__(
        self,
        *,
        exchange_config: Dict[str, Any],
        instruments: Iterable[Mapping[str, Any]],
        poll_interval_sec: float = 2.0,
        max_symbols: int = 50,
    ) -> None:
        self.exchange_config = dict(exchange_config or {})
        self.instruments = [dict(item) for item in instruments][: max(1, int(max_symbols))]
        self.poll_interval_sec = max(0.5, float(poll_interval_sec))
        self._prices: Dict[str, float] = {}
        self._price_updated_at: Dict[str, float] = {}
        self._updated_at = 0.0
        self._connected = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._client = None
        self._last_error = ""

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def last_error(self) -> str:
        return self._last_error

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="FutuQuoteFeed", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        if self._client is not None:
            try:
                self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False

    def snapshot(self, max_age_seconds: float = 10.0) -> Dict[str, Any]:
        now = time.time()
        age_ms = int(max(0.0, (now - self._updated_at) * 1000))
        fresh_prices = {
            key: price for key, price in self._prices.items()
            if now - self._price_updated_at.get(key, 0.0) <= float(max_age_seconds)
        }
        return {
            "prices": fresh_prices,
            "source": "futu_quote" if self._connected and fresh_prices else "futu_stale",
            "age_ms": age_ms,
            "connected": self._connected,
            "error": self._last_error,
        }

    def _run(self) -> None:
        from app.services.futu_trading.config import config_from_exchange_config
        from app.services.futu_trading.quote_client import FutuQuoteClient

        config = config_from_exchange_config(self.exchange_config)
        retry_delay = self.poll_interval_sec
        while not self._stop.is_set():
            client = None
            try:
                client = FutuQuoteClient(config)
                self._client = client
                if not client.connect():
                    raise ConnectionError("FutuOpenD quote connection failed")
                for item in self.instruments:
                    symbol = str(item.get("symbol") or "")
                    if symbol:
                        try:
                            client.subscribe_quote([symbol], str(item.get("market") or "USStock"))
                        except Exception:
                            pass
                self._connected = True
                self._last_error = ""
                retry_delay = self.poll_interval_sec
                while not self._stop.is_set():
                    self._poll_once()
                    self._stop.wait(self.poll_interval_sec)
            except Exception as exc:
                self._last_error = str(exc)
                self._connected = False
                logger.warning("FutuQuoteFeed reconnecting: %s", exc)
            finally:
                self._connected = False
                if client is not None:
                    try:
                        client.disconnect()
                    except Exception:
                        pass
                if self._client is client:
                    self._client = None
            if not self._stop.is_set():
                self._stop.wait(retry_delay)
                retry_delay = min(30.0, retry_delay * 2)

    def _poll_once(self) -> None:
        if self._client is None:
            return
        from app.services.futu_trading.execution_quote import describe_futu_quote

        updated = False
        updated_times: list[float] = []
        attempted = 0
        failed = 0
        for item in self.instruments:
            key = str(item.get("key") or "")
            symbol = str(item.get("symbol") or "")
            market = str(item.get("market") or "HKStock")
            if not key or not symbol:
                continue
            attempted += 1
            try:
                quote = self._client.get_quote(symbol, market)
                if isinstance(quote, dict) and quote.get("success"):
                    provenance = describe_futu_quote(symbol, quote, now=time.time(),
                                                     market_type=market)
                    if not provenance["is_stale"]:
                        self._prices[key] = float(provenance["price"])
                        self._price_updated_at[key] = float(provenance["as_of"])
                        updated_times.append(float(provenance["as_of"]))
                        updated = True
                    else:
                        self._last_error = "FUTU_QUOTE_STALE_OR_UNTIMED"
                        failed += 1
                else:
                    failed += 1
            except Exception as exc:
                self._last_error = str(exc)
                failed += 1
        if attempted and failed == attempted:
            raise ConnectionError(self._last_error or "Futu quotes unavailable")
        if updated:
            self._updated_at = max(updated_times)
            self._connected = True
            self._last_error = ""

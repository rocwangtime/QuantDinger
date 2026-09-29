"""Futu live strategies must stop on stale quotes and recover after OpenD reconnects."""

from datetime import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from app.services.futu_trading.quote_feed import FutuQuoteFeed, fresh_futu_quote_prices


def _feed(instruments=None):
    return FutuQuoteFeed(
        exchange_config={"futu_host": "127.0.0.1", "trade_env": "demo", "trade_market": "US"},
        instruments=instruments or [{"key": "SPY", "symbol": "SPY", "market": "USStock"}],
        poll_interval_sec=0.5,
    )


def test_futu_quote_snapshot_drops_stale_symbols_individually(monkeypatch):
    monkeypatch.setattr("app.services.futu_trading.quote_feed.time.time", lambda: 100.0)
    feed = _feed()
    feed._connected = True
    feed._prices = {"SPY": 700.0, "AAPL": 200.0}
    feed._price_updated_at = {"SPY": 99.0, "AAPL": 80.0}
    feed._updated_at = 99.0

    snapshot = feed.snapshot(max_age_seconds=10)

    assert fresh_futu_quote_prices(snapshot) == {"SPY": 700.0}
    feed._connected = False
    assert fresh_futu_quote_prices(feed.snapshot(max_age_seconds=10)) == {}


def test_futu_quote_feed_reconnects_after_poll_failure(monkeypatch):
    created = []

    class FakeClient:
        def __init__(self, _config):
            self.disconnected = False
            created.append(self)

        def connect(self):
            return True

        def subscribe_quote(self, *_args):
            return None

        def get_quote(self, *_args):
            if len(created) == 1:
                raise ConnectionError("OpenD disconnected")
            return {"success": True, "last": 701.0, "raw": {
                "update_time": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S"),
            }}

        def disconnect(self):
            self.disconnected = True

    monkeypatch.setattr("app.services.futu_trading.quote_client.FutuQuoteClient", FakeClient)
    feed = _feed()
    stop = MagicMock()
    stop.is_set.side_effect = lambda: stop.wait.call_count >= 2
    feed._stop = stop

    feed._run()

    assert len(created) == 2
    assert all(client.disconnected for client in created)
    assert feed._prices == {"SPY": 701.0}
    assert feed.last_error == ""
    assert feed._client is None


def test_repeated_old_snapshot_never_refreshes_strategy_quote_clock(monkeypatch):
    now = datetime(2026, 9, 29, 9, 30, 30, tzinfo=ZoneInfo("America/New_York")).timestamp()
    monkeypatch.setattr("app.services.futu_trading.quote_feed.time.time", lambda: now)
    feed = _feed()
    feed._connected = True
    feed._prices = {"SPY": 700.0}
    feed._price_updated_at = {"SPY": now - 20}
    feed._updated_at = now - 20
    feed._client = MagicMock()
    feed._client.get_quote.return_value = {
        "success": True, "last": 701.0,
        "raw": {"update_time": "2026-09-29 09:30:00"},
    }

    with pytest.raises(ConnectionError, match="FUTU_QUOTE_STALE_OR_UNTIMED"):
        feed._poll_once()

    assert fresh_futu_quote_prices(feed.snapshot()) == {}
    assert feed._prices["SPY"] == 700.0
    assert feed._price_updated_at["SPY"] == now - 20

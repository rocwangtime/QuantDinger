"""No test in this module connects to OpenD or places a real broker order."""

from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from app.services import futu_agent_execution as gateway
from app.services.agent_trade_intents import IntentError
from app.services.futu_trading.client import OrderResult
from app.services.futu_trading.client import FutuClient
from app.services.futu_trading.config import FutuConfig


def _order(side="buy"):
    return {"broker": "futu", "credential_id": 7, "market": "USStock",
            "symbol": "AAPL", "side": side, "qty": 1.0, "order_type": "limit",
            "limit_price": 100.0}


def _quote(**overrides):
    return {"simulate_execution_eligible": True, "price": 100.0,
            "received_at": time.time(), **overrides}


def test_execution_quote_requires_subscription_and_exact_regular_market_state(monkeypatch):
    from app.services.futu_trading import client as client_module

    ft = SimpleNamespace(RET_OK=0, SubType=SimpleNamespace(QUOTE="QUOTE"))
    monkeypatch.setattr(client_module, "_ensure_futu", lambda: ft)
    client = FutuClient(FutuConfig(acc_id=123))
    client._connected = True
    client._trade_ctx = MagicMock()
    client._quote_ctx = MagicMock()
    client._quote_ctx.subscribe.return_value = (0, None)
    client._quote_ctx.get_market_state.return_value = (0, [{"market_state": "AFTERNOON"}])
    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    client._quote_ctx.get_market_snapshot.return_value = (0, [{
        "last_price": 100, "bid_price": 99.9, "ask_price": 100.1,
        "update_time": stamp,
    }])
    quote = client.get_simulate_execution_quote("AAPL")
    assert quote["simulate_execution_eligible"] is True
    assert quote["execution_eligible"] is False  # never claim REAL entitlement
    client._quote_ctx.subscribe.assert_called_once_with(["US.AAPL"], ["QUOTE"], subscribe_push=False)
    client._quote_ctx.get_market_state.return_value = (0, [{"market_state": "PRE_MARKET_BEGIN"}])
    assert client.get_simulate_execution_quote("AAPL")["simulate_execution_eligible"] is False


def test_us_simulate_funds_explicitly_request_usd(monkeypatch):
    from app.services.futu_trading import client as client_module

    ft = SimpleNamespace(RET_OK=0, Currency=SimpleNamespace(USD="USD"),
                         TrdEnv=SimpleNamespace(SIMULATE="SIMULATE"))
    monkeypatch.setattr(client_module, "_ensure_futu", lambda: ft)
    client = FutuClient(FutuConfig(acc_id=123))
    client._connected = True
    client._trade_ctx = MagicMock()
    client._quote_ctx = MagicMock()
    client._accounts = [{"acc_id": 123, "trd_env": "SIMULATE", "trdmarket_auth": "US"}]
    client._trade_ctx.accinfo_query.return_value = (0, [{"power": 1000, "currency": "N/A"}])
    result = client.get_account_summary()
    assert result["summary"]["currency"] == "USD"
    assert result["summary"]["currency_basis"] == "futu_usd_query"
    assert client._trade_ctx.accinfo_query.call_args.kwargs["currency"] == "USD"


def test_preflight_requires_fresh_eligible_quote_and_usd_power():
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = _quote(simulate_execution_eligible=False)
    with pytest.raises(IntentError, match="Fresh subscribed"):
        gateway._preflight(client, _order())
    client.get_simulate_execution_quote.return_value = _quote()
    client.get_account_summary.return_value = {
        "success": True, "summary": {"power": 1000, "currency": "N/A"},
    }
    with pytest.raises(IntentError, match="buying power"):
        gateway._preflight(client, _order())
    client.get_account_summary.return_value["summary"]["currency"] = "USD"
    assert gateway._preflight(client, _order())[1] == 100.0


def test_preflight_rejects_fractional_or_far_limit_before_broker_call():
    client = MagicMock()
    client.get_simulate_execution_quote.return_value = _quote()
    with pytest.raises(IntentError, match="whole shares"):
        gateway._preflight(client, {**_order(), "qty": 0.5})
    with pytest.raises(IntentError, match="deviates"):
        gateway._preflight(client, {**_order(), "limit_price": 104.0})
    client.place_limit_order.assert_not_called()


def test_non_proposed_intent_is_read_only_even_if_called_twice(monkeypatch):
    row = {"id": 9, "status": "UNCERTAIN", "broker": "futu", "agent_token_id": 4}
    monkeypatch.setattr(gateway, "_read_owned_intent", lambda *_args: row)
    monkeypatch.setattr(gateway, "_load_client", lambda *_args: pytest.fail("must not reconnect or resubmit"))
    assert gateway.execute_simulate_intent(1, {"id": 4, "paper_only": True}, 9) == row
    assert gateway.execute_simulate_intent(1, {"id": 4, "paper_only": True}, 9) == row


def test_ambiguous_submit_is_durable_and_never_retried(monkeypatch):
    row = {"id": 9, "status": "PROPOSED", "broker": "futu", "agent_token_id": 4,
           "account_ref": "credential:7", "order_spec": _order()}
    calls = []

    class Cursor:
        def __init__(self):
            self.result = None

        def execute(self, sql, params=None):
            calls.append((sql, params))
            if "SELECT * FROM qd_agent_trade_intents" in sql:
                self.result = row.copy()
            elif "SELECT 1 FROM qd_futu_automation_state" in sql:
                self.result = {"?column?": 1}
            elif "COALESCE(SUM(notional)" in sql:
                self.result = {"used": 0}
            elif "SET status='EXECUTING'" in sql:
                row["status"] = "EXECUTING"
            elif "SET status=%s" in sql:
                row["status"] = params[0]
            else:
                self.result = None

        def fetchone(self):
            return self.result

        def close(self):
            pass

    class Db:
        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("COMMIT", None))

        def rollback(self):
            pass

    @contextmanager
    def fake_db():
        yield Db()

    monkeypatch.setattr(gateway, "get_db_connection", fake_db)
    monkeypatch.setattr(gateway, "hard_switch_enabled", lambda: True)
    monkeypatch.setattr(gateway, "_policy_row", lambda *_a, **_kw: {"mode": "PAPER_AUTO"})
    monkeypatch.setattr(gateway, "_risk", lambda *_a: (True, {"notional": 100.0}))
    monkeypatch.setattr(gateway, "_preflight", lambda *_a: (_quote(), 100.0))
    client = MagicMock()
    client.config = SimpleNamespace(acc_id=123)
    client.connect.return_value = True
    client.place_limit_order.return_value = OrderResult(success=False, message="timeout")
    monkeypatch.setattr(gateway, "_load_client", lambda *_a: client)
    token = {"id": 4, "paper_only": True,
             "max_order_notional": 1000, "max_daily_notional": 5000}

    assert gateway.execute_simulate_intent(1, token, 9)["status"] == "UNCERTAIN"
    assert gateway.execute_simulate_intent(1, token, 9)["status"] == "UNCERTAIN"
    client.place_limit_order.assert_called_once()
    assert client.place_limit_order.call_args.kwargs["remark"] == "qd_agent_9"
    assert [sql for sql, _ in calls].index("COMMIT") < next(
        i for i, (sql, _) in enumerate(calls) if "SET status=%s" in sql
    )


def test_reconcile_absence_never_means_safe_to_retry(monkeypatch):
    row = {"id": 9, "status": "UNCERTAIN", "broker": "futu",
           "account_ref": "credential:7", "broker_remark": "qd_agent_9"}
    monkeypatch.setattr(gateway, "_read_owned_intent", lambda *_a: row)
    client = MagicMock()
    client.connect.return_value = True
    client.find_order_by_remark.return_value = None
    monkeypatch.setattr(gateway, "_load_client", lambda *_a: client)
    db = MagicMock()

    @contextmanager
    def fake_db():
        yield db

    monkeypatch.setattr(gateway, "get_db_connection", fake_db)
    assert gateway.reconcile_simulate_intent(1, 9) == row
    client.find_order_by_remark.assert_called_once_with("qd_agent_9")
    client.place_limit_order.assert_not_called()
    assert "last_reconciled_at=NOW()" in db.cursor.return_value.execute.call_args.args[0]

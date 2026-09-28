"""Read-only acceptance checks against FutuOpenD and a US paper account.

Skipped unless FUTU_INTEGRATION=1. Set FUTU_SIM_ACC_ID only to the trading
account ID returned by the OpenD probe, never the Futu login ID. These checks
do not place or cancel orders.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


def _enabled() -> bool:
    return str(os.getenv("FUTU_INTEGRATION") or "").strip().lower() in ("1", "true", "yes", "on")


@pytest.mark.skipif(not _enabled(), reason="Set FUTU_INTEGRATION=1 with a running FutuOpenD")
def test_futu_opend_us_paper_probe_and_quote():
    from app.services.futu_trading import FutuClient, FutuConfig

    account_id = int(os.getenv("FUTU_SIM_ACC_ID") or 0)
    client = FutuClient(
        FutuConfig(
            host=os.getenv("FUTU_OPEND_HOST", "127.0.0.1"),
            port=int(os.getenv("FUTU_OPEND_PORT", "11111")),
            trade_env="demo",
            trade_market="US",
            acc_id=account_id,
        )
    )
    assert client.connect(), "FutuOpenD connect failed"
    try:
        status = client.get_connection_status()
        assert status.get("opend_connected") is True
        assert status.get("trade_env") == "demo"
        assert status.get("trade_market") == "US"
        probe = client.probe_permissions()
        paper_accounts = [
            account for account in probe.get("accounts", [])
            if "SIMULATE" in str(account.get("trd_env") or "").upper()
            and "US" in str(account.get("trdmarket_auth") or "").upper()
        ]
        assert paper_accounts, "OpenD did not list a US SIMULATE trading account"
        quote = client.get_quote("AAPL", "USStock")
        assert quote.get("success") is True
        assert float(quote.get("last") or 0) > 0
        if account_id:
            assert status.get("connected") is True
            assert any(int(account["acc_id"]) == account_id for account in paper_accounts)
            assert client.get_account_summary().get("success") is True
            assert isinstance(client.get_positions(), list)
            assert isinstance(client.get_recent_orders(), list)
    finally:
        client.disconnect()

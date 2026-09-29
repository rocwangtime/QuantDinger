from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from app.services import agent_trade_intents as intents


def _order(**overrides):
    return {
        "market": "Crypto", "symbol": "BTC/USDT", "side": "buy",
        "qty": 0.01, "order_type": "limit", "limit_price": 100,
        **overrides,
    }


def test_order_hash_input_is_canonical_and_rejects_unreviewed_fields():
    order = intents.normalize_order(_order(symbol="btc/usdt"))
    assert order["symbol"] == "BTC/USDT"
    assert order["broker"] == "platform"
    with pytest.raises(intents.IntentError, match="Unsupported"):
        intents.normalize_order(_order(unlock_password="secret"))
    with pytest.raises(intents.IntentError, match="limit_price"):
        intents.normalize_order(_order(limit_price=float("nan")))


def test_futu_is_only_a_us_paper_limit_proposal(monkeypatch):
    with pytest.raises(intents.IntentError, match="USStock limit"):
        intents.normalize_order(_order(broker="futu", credential_id=2))
    with pytest.raises(intents.IntentError, match="credential_id"):
        intents.normalize_order(_order(broker="futu"))


def test_real_modes_cannot_be_configured_even_with_confirmation():
    for mode in ("LIVE_APPROVAL", "LIVE_AUTO"):
        with pytest.raises(intents.IntentError) as exc:
            intents.set_policy(1, "platform", "default", {
                "mode": mode, "confirm_mode": mode,
            })
        assert exc.value.status == 501


def test_platform_paper_requires_human_expiry_and_exact_allowlist():
    with pytest.raises(intents.IntentError, match="internal platform paper"):
        intents.set_policy(1, "futu", "credential:2", {
            "mode": "PAPER_AUTO", "confirm_mode": "PAPER_AUTO",
        })
    with pytest.raises(intents.IntentError, match="allowlists"):
        intents.set_policy(1, "platform", "default", {
            "mode": "PAPER_AUTO", "confirm_mode": "PAPER_AUTO",
            "enabled_until": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        })
    with pytest.raises(intents.IntentError, match="within 24 hours"):
        intents.set_policy(1, "platform", "default", {
            "mode": "PAPER_AUTO", "confirm_mode": "PAPER_AUTO",
            "allowed_markets": ["Crypto"], "allowed_symbols": ["BTC/USDT"],
            "enabled_until": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
        })


class _Cursor:
    def __init__(self):
        self.statements = []
        self.result = None

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if "INSERT INTO qd_agent_trade_intents" in statement:
            self.result = {"id": 7}
        elif "UPDATE qd_agent_trade_intents SET status=" in statement and "RETURNING *" in statement:
            self.result = {"id": 7, "status": params[0], "paper_order_uid": params[4]}
        else:
            self.result = None

    def fetchone(self):
        return self.result

    def close(self):
        pass


class _Db:
    def __init__(self):
        self.cur = _Cursor()
        self.commits = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1


def _fake_db(monkeypatch):
    db = _Db()

    @contextmanager
    def connection():
        yield db

    monkeypatch.setattr(intents, "get_db_connection", connection)
    return db


def test_default_plan_only_records_intent_without_quote_or_order(monkeypatch):
    db = _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PLAN_ONLY"})
    result = intents.submit_intent(
        1, {"id": 3, "paper_only": True}, _order(), "key-1",
        quote_provider=lambda *_: pytest.fail("plan-only must not fetch an execution quote"),
    )
    assert result["status"] == "PROPOSED"
    assert result["paper_order_uid"] is None
    assert not any("INSERT INTO qd_agent_paper_orders" in sql for sql, _ in db.cur.statements)
    assert db.commits == 1


def test_create_intent_remains_plan_only_even_when_paper_auto_is_armed(monkeypatch):
    db = _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PAPER_AUTO"})
    result = intents.submit_intent(
        1, {"id": 3, "paper_only": True}, _order(), "plan-key",
        quote_provider=lambda *_: pytest.fail("proposal must not fetch an execution quote"),
    )
    assert result["status"] == "PROPOSED"
    assert not any("INSERT INTO qd_agent_paper_orders" in sql for sql, _ in db.cur.statements)


def test_direct_paper_order_requires_explicit_policy(monkeypatch):
    _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PLAN_ONLY"})
    with pytest.raises(intents.IntentError, match="PAPER_AUTO"):
        intents.submit_intent(1, {"id": 3, "paper_only": True}, _order(), "direct-key",
                              require_paper_execution=True)


def test_explicit_internal_paper_is_atomic_and_never_calls_broker(monkeypatch):
    db = _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PAPER_AUTO"})
    monkeypatch.setattr(intents, "_risk", lambda *_a, **_k: (True, {"reason": "accepted", "notional": 1.0}))
    result = intents.submit_intent(
        1, {"id": 3, "paper_only": True}, _order(limit_price=110), "key-2",
        quote_provider=lambda *_: 100.0, require_paper_execution=True,
    )
    assert result["status"] == "FILLED"
    assert result["paper_order_uid"] == "intent-7"
    assert any("INSERT INTO qd_agent_paper_orders" in sql for sql, _ in db.cur.statements)
    assert db.commits == 1


def test_live_capable_token_cannot_use_internal_paper(monkeypatch):
    _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PAPER_AUTO"})
    with pytest.raises(intents.IntentError, match="Live-capable"):
        intents.submit_intent(1, {"id": 3, "paper_only": False}, _order(), "key-3",
                              require_paper_execution=True)


def test_internal_paper_quote_failure_records_rejection(monkeypatch):
    db = _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {"mode": "PAPER_AUTO"})

    def failed_quote(*_args):
        raise RuntimeError("price service unavailable")

    result = intents.submit_intent(
        1, {"id": 3, "paper_only": True}, _order(), "key-4",
        quote_provider=failed_quote, require_paper_execution=True,
    )
    assert result["status"] == "REJECTED"
    assert db.commits == 1
    assert not any("INSERT INTO qd_agent_paper_orders" in sql for sql, _ in db.cur.statements)


def test_risk_uses_fill_price_when_sell_limit_understates_notional():
    class DailyCursor:
        def execute(self, *_args):
            pass

        def fetchone(self):
            return {"used": 0, "orders": 0}

    policy = {
        "allowed_markets": ["CRYPTO"], "allowed_symbols": ["BTC/USDT"],
        "allow_market_order": False, "max_order_notional": 500,
        "max_daily_notional": 5000, "max_orders_per_day": 10,
        "broker": "platform", "account_ref": "default", "allow_short": False,
    }
    accepted, result = intents._risk(
        DailyCursor(), 1, policy,
        intents.normalize_order(_order(side="sell", qty=1, limit_price=1)), 1000,
    )
    assert accepted is False
    assert result["reason"] == "max_order_notional"


def test_global_emergency_stop_downgrades_active_account_modes(monkeypatch):
    db = _fake_db(monkeypatch)
    monkeypatch.setattr(intents, "_policy_row", lambda *_a, **_k: {
        "mode": "PLAN_ONLY", "configured_mode": "PLAN_ONLY",
    })
    monkeypatch.setattr(intents, "get_policy", lambda *_a: {"mode": "EMERGENCY_STOP"})
    result = intents.set_policy(1, "*", "*", {
        "mode": "EMERGENCY_STOP", "confirm_mode": "EMERGENCY_STOP",
    })
    statements = [sql for sql, _ in db.cur.statements]
    assert result["mode"] == "EMERGENCY_STOP"
    assert any("SET mode='PLAN_ONLY', enabled_until=NULL" in sql for sql in statements)
    assert any("reason', 'global_emergency_stop'" in sql for sql in statements)

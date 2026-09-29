"""Cancellation acknowledgements must not masquerade as terminal fills/stops."""

from __future__ import annotations

from contextlib import contextmanager

from app.routes.agent_v1 import quick_trade


class _Cursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.statements = []
        self.rowcount = 0

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        self.rowcount = 1 if statement.lstrip().startswith("UPDATE") else 0

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _Db:
    def __init__(self, rows=None):
        self.cur = _Cursor(rows)

    def cursor(self):
        return self.cur

    def commit(self):
        pass


def test_legacy_exchange_cancel_ack_requires_manual_review(monkeypatch):
    row = {
        "id": 3, "credential_id": 8, "symbol": "BTCUSDT",
        "market_type": "spot", "exchange_order_id": "ex-17", "raw_result": {},
    }
    dbs = [_Db([row]), _Db()]
    seen = list(dbs)

    @contextmanager
    def connection():
        yield dbs.pop(0)

    monkeypatch.setattr(quick_trade, "get_db_connection", connection)
    from app.services.pending_orders import live_order_phases
    from app.services.quick_trade import credentials

    monkeypatch.setattr(credentials, "build_exchange_config", lambda *_a, **_k: {"exchange_id": "test"})
    monkeypatch.setattr(credentials, "create_exchange_client", lambda *_a, **_k: object())
    monkeypatch.setattr(live_order_phases, "cancel_live_limit_order", lambda **_k: {"success": True})

    result = quick_trade.cancel_agent_orders(1)
    assert result["live_cancel_requests_accepted"] == 1
    assert result["manual_review_required"] is True
    assert not any("UPDATE qd_quick_trades" in sql for db in seen for sql, _ in db.cur.statements)


def test_paper_cancel_updates_intent_without_revoking_or_liquidating(monkeypatch):
    seen = []

    @contextmanager
    def connection():
        db = _Db()
        seen.append(db)
        yield db

    monkeypatch.setattr(quick_trade, "get_db_connection", connection)
    result = quick_trade.cancel_agent_orders(2)
    assert result["cancelled_open_paper_orders"] == 1
    assert result["cancelled_paper_intents"] == 1
    assert result["manual_review_required"] is False
    writes = [sql for db in seen for sql, _ in db.cur.statements if sql.lstrip().startswith("UPDATE")]
    assert any("UPDATE qd_agent_trade_intents" in sql for sql in writes)
    assert not any("qd_agent_tokens" in sql or "position" in sql for sql in writes)

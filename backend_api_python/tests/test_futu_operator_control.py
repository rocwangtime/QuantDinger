"""Paper automation must not confuse a Web disconnect with a trading stop."""

from unittest.mock import MagicMock
from contextlib import contextmanager

import pytest

from app.services.futu_trading import operator_control, operator_gate


def test_submission_permit_checks_saved_credential_and_pending_order_under_lock(monkeypatch):
    monkeypatch.setenv("FUTU_PAPER_AUTOTRADE_ALLOWED", "true")
    cursor = MagicMock()
    cursor.fetchone.return_value = {"?column?": 1}
    db = MagicMock()
    db.cursor.return_value = cursor

    @contextmanager
    def fake_connection():
        yield db

    monkeypatch.setattr(operator_gate, "get_db_connection", fake_connection)
    with operator_gate.submission_permit(
        user_id=1, credential_id=2, acc_id=3, remark="qd_4_5",
    ):
        assert db.rollback.call_count == 0  # lock held through broker submission
    assert db.rollback.call_count == 1
    query = cursor.execute.call_args_list[-1]
    assert "pending.strategy_id = %s" in query.args[0]
    assert "LOWER(pending.exchange_id) = 'futu'" in query.args[0]
    assert "pending.execution_mode = 'live'" in query.args[0]
    assert query.args[1] == (5, 3, 1, 2, 4)


def test_agent_submit_permit_binds_immutable_order_and_human_policy(monkeypatch):
    monkeypatch.setenv("FUTU_PAPER_AUTOTRADE_ALLOWED", "true")
    cursor = MagicMock()
    cursor.fetchone.return_value = {"?column?": 1}
    db = MagicMock()
    db.cursor.return_value = cursor

    @contextmanager
    def fake_connection():
        yield db

    monkeypatch.setattr(operator_gate, "get_db_connection", fake_connection)
    with operator_gate.submission_permit(
        user_id=1, credential_id=2, acc_id=3, remark="qd_agent_9",
        symbol="AAPL", side="buy", qty=1, limit_price=100,
    ):
        assert db.rollback.call_count == 0
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "pg_advisory_xact_lock(824111" in statements[0]
    assert "intent.order_spec->>'symbol'" in statements[-1]
    assert "policy.enabled_until > NOW()" in statements[-1]
    assert "min_agent_intent_id" in statements[-1]
    assert cursor.execute.call_args.args[1] == (9, 3, 1, 2, "qd_agent_9", "AAPL", "buy", 1.0, 100.0)


def test_submission_permit_does_not_send_expected_denial_or_broker_error_to_db_logger(monkeypatch):
    monkeypatch.setenv("FUTU_PAPER_AUTOTRADE_ALLOWED", "true")
    cursor = MagicMock()
    db = MagicMock()
    db.cursor.return_value = cursor
    exits = []

    @contextmanager
    def fake_connection():
        try:
            yield db
        except BaseException:
            exits.append("error")
            raise
        else:
            exits.append("normal")

    monkeypatch.setattr(operator_gate, "get_db_connection", fake_connection)
    cursor.fetchone.return_value = None
    with pytest.raises(ValueError, match="FUTU_OPERATOR_NOT_ARMED"):
        with operator_gate.submission_permit(
            user_id=1, credential_id=2, acc_id=3, remark="qd_4_5",
        ):
            pytest.fail("broker submission must not run")
    cursor.fetchone.return_value = {"?column?": 1}
    with pytest.raises(RuntimeError, match="broker failed"):
        with operator_gate.submission_permit(
            user_id=1, credential_id=2, acc_id=3, remark="qd_4_5",
        ):
            raise RuntimeError("broker failed")
    assert exits == ["normal", "normal"]


def test_submission_permit_defaults_to_denied_without_database(monkeypatch):
    monkeypatch.delenv("FUTU_PAPER_AUTOTRADE_ALLOWED", raising=False)
    with pytest.raises(ValueError, match="FUTU_PAPER_AUTOTRADE_HARD_DISABLED"):
        with operator_gate.submission_permit(
            user_id=1, credential_id=2, acc_id=3, remark="qd_4_5",
        ):
            pytest.fail("broker submission must not run")


def test_submission_permit_rejects_untracked_order_identity(monkeypatch):
    monkeypatch.setenv("FUTU_PAPER_AUTOTRADE_ALLOWED", "true")
    with pytest.raises(ValueError, match="FUTU_PLATFORM_ORDER_ID_REQUIRED"):
        with operator_gate.submission_permit(
            user_id=1, credential_id=2, acc_id=3, remark="manual-order",
        ):
            pytest.fail("broker submission must not run")


def test_duplicate_saved_account_uses_newest_credential_unless_pinned(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        {"id": 8, "encrypted_config": "new"},
        {"id": 7, "encrypted_config": "old"},
    ]
    db = MagicMock()
    db.cursor.return_value = cursor

    @contextmanager
    def fake_connection():
        yield db

    monkeypatch.setattr(operator_control, "get_db_connection", fake_connection)
    monkeypatch.setattr(operator_control, "decrypt_credential_blob", lambda _: '{"acc_id":3,"trade_env":"demo","trade_market":"US"}')
    assert operator_control.saved_account_credential(1, 3)[0] == 8
    assert operator_control.saved_account_credential(1, 3, credential_id=7)[0] == 7


def test_connect_reuses_matching_saved_paper_account(monkeypatch):
    from app.services.futu_trading.config import FutuConfig

    db = MagicMock()
    cur = db.cursor.return_value
    cur.fetchall.return_value = [{"id": 8, "encrypted_config": "opaque"}]
    context = MagicMock()
    context.__enter__.return_value = db
    monkeypatch.setattr(operator_control, "get_db_connection", lambda: context)
    monkeypatch.setattr(operator_control, "decrypt_credential_blob", lambda _: (
        '{"futu_host":"host.docker.internal","futu_port":11112,'
        '"trade_env":"demo","trade_market":"HK","market_category":"HKStock",'
        '"security_firm":"FUTUSECURITIES","acc_id":99}'
    ))
    selected = FutuConfig(host="host.docker.internal", port=11112, trade_market="HK",
                          market_category="HKStock", acc_id=99)
    assert operator_control.ensure_saved_account_credential(1, selected) == 8
    assert operator_control.ensure_saved_account_credential(1, selected) == 8
    assert db.commit.call_count == 2
    assert not any("INSERT" in str(call.args[0]) for call in cur.execute.call_args_list)


def test_pause_can_identify_order_accepted_before_binding_was_written(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [{
        "exchange_order_id": "", "client_order_id": "",
        "pending_order_id": 22, "strategy_id": 11,
    }]
    db = MagicMock()
    db.cursor.return_value = cursor

    @contextmanager
    def fake_connection():
        yield db

    monkeypatch.setattr(operator_control, "get_db_connection", fake_connection)
    ids, remarks = operator_control._owned_order_identities(1, 2)
    assert ids == set()
    assert remarks == {"qd_11_22"}


def test_pause_disarms_before_cancellation_and_keeps_failure_unconfirmed(monkeypatch):
    events = []
    monkeypatch.setattr(operator_control, "begin_pause", lambda *args: events.append("disarm"))
    monkeypatch.setattr(operator_control, "wait_submission_barrier", lambda *args: events.append("barrier"))
    monkeypatch.setattr(operator_control, "_cancel_queued_orders", lambda *args: events.append("queue") or 1)

    def cancel(*_args):
        events.append("cancel")
        raise RuntimeError("OpenD unavailable")

    monkeypatch.setattr(operator_control, "_cancel_owned_open_orders", cancel)
    monkeypatch.setattr(
        operator_control, "finish_pause",
        lambda *args, **kwargs: events.append(("finish", kwargs["confirmed"])),
    )
    result = operator_control.pause_account(1, 2, 3, {})
    assert events == ["disarm", "barrier", "queue", "cancel", ("finish", False)]
    assert result["state"] == "unconfirmed"
    assert result["enabled"] is False


def test_pause_keeps_stale_processing_submit_unconfirmed(monkeypatch):
    monkeypatch.setattr(operator_control, "begin_pause", lambda *_: None)
    monkeypatch.setattr(operator_control, "wait_submission_barrier", lambda *_: None)
    monkeypatch.setattr(operator_control, "_cancel_queued_orders", lambda *_: 0)
    monkeypatch.setattr(operator_control, "_cancel_owned_open_orders", lambda *_: 0)
    monkeypatch.setattr(operator_control, "_unresolved_submit_outcomes", lambda *_: 1)
    finished = []
    monkeypatch.setattr(operator_control, "finish_pause", lambda *_, **kw: finished.append(kw["confirmed"]))
    result = operator_control.pause_account(1, 2, 3, {})
    assert result["state"] == "unconfirmed"
    assert result["error"] == "FUTU_SUBMISSION_OUTCOME_REVIEW_REQUIRED"
    assert finished == [False]


def test_pause_cancels_only_platform_owned_open_orders(monkeypatch):
    owned = {"owned-1"}
    rows = [
        {"orderId": "owned-1", "remark": "qd_1_2"},
        {"orderId": "manual-1", "remark": "manual"},
    ]
    cancelled = []

    class FakeClient:
        def __init__(self, _config):
            pass

        def connect(self, need_quote=False):
            assert need_quote is False
            return True

        def get_open_orders(self, *, strict=False):
            assert strict
            return [row for row in rows if row["orderId"] not in cancelled]

        def cancel_order(self, order_id):
            cancelled.append(order_id)
            return True

        def disconnect(self):
            pass

    monkeypatch.setattr("app.services.futu_trading.client.FutuClient", FakeClient)
    monkeypatch.setattr(operator_control, "_owned_order_identities", lambda *_: (owned, {"qd_1_2"}))
    result = operator_control._cancel_owned_open_orders(
        1, 2, {"acc_id": 3, "trade_env": "demo", "trade_market": "US"},
    )
    assert result == 1
    assert cancelled == ["owned-1"]


def test_pause_catches_broker_order_visible_only_on_second_refresh(monkeypatch):
    calls = []
    cancelled = []

    class DelayedClient:
        def __init__(self, _config):
            pass

        def connect(self, need_quote=False):
            return True

        def get_open_orders(self, *, strict=False):
            calls.append("query")
            if len(calls) == 1 or cancelled:
                return []
            return [{"orderId": "late-1", "remark": "qd_1_2"}]

        def cancel_order(self, order_id):
            cancelled.append(order_id)
            return True

        def disconnect(self):
            pass

    monkeypatch.setattr("app.services.futu_trading.client.FutuClient", DelayedClient)
    monkeypatch.setattr(operator_control, "_owned_order_identities", lambda *_: (set(), {"qd_1_2"}))
    monkeypatch.setattr(operator_control.time, "sleep", lambda *_: None)
    result = operator_control._cancel_owned_open_orders(
        1, 2, {"acc_id": 3, "trade_env": "demo", "trade_market": "US"},
    )
    assert result == 1
    assert cancelled == ["late-1"]
    assert len(calls) >= 4


def test_pause_rejects_untracked_platform_order(monkeypatch):
    client = MagicMock()
    client.connect.return_value = True
    client.get_open_orders.return_value = [{"orderId": "unknown", "remark": "qd_1_99"}]
    monkeypatch.setattr("app.services.futu_trading.client.FutuClient", lambda _: client)
    monkeypatch.setattr(operator_control, "_owned_order_identities", lambda *_: (set(), set()))
    with pytest.raises(RuntimeError, match="FUTU_UNTRACKED_PLATFORM_ORDER_REVIEW_REQUIRED"):
        operator_control._cancel_owned_open_orders(
            1, 2, {"acc_id": 3, "trade_env": "demo", "trade_market": "US"},
        )
    client.cancel_order.assert_not_called()

from __future__ import annotations

from contextlib import AbstractContextManager

import pytest

from app.services.pending_orders import submission_recovery as recovery_module
from app.services.pending_orders.submission_recovery import SubmissionRecoveryMixin


class FakeCursor:
    def __init__(self, calls, *, rowcount=1):
        self.calls = calls
        self.rowcount = rowcount

    def execute(self, sql, params=()):
        self.calls.append((sql, params))

    def close(self):
        return None


class FakeConnection(AbstractContextManager):
    def __init__(self, calls, *, rowcount=1):
        self.calls = calls
        self.rowcount = rowcount

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return FakeCursor(self.calls, rowcount=self.rowcount)

    def commit(self):
        return None


class RecoveryHarness(SubmissionRecoveryMixin):
    def __init__(self):
        self.bindings = []

    def _register_pending_order_binding(self, **kwargs):
        self.bindings.append(kwargs)


def test_submission_identity_is_committed_before_binding(monkeypatch):
    calls = []
    monkeypatch.setattr(
        recovery_module,
        "get_db_connection",
        lambda: FakeConnection(calls),
    )
    recovery = RecoveryHarness()

    recovery._prepare_submission(
        order_id=17,
        exchange_id="alpaca",
        market_type="USStock",
        client_order_id="qd_2_17",
    )

    assert "status = 'processing'" in calls[0][0]
    assert calls[0][1] == ("alpaca", None, "qd_2_17", 17)
    assert recovery.bindings[0]["client_order_id"] == "qd_2_17"


def test_futu_submission_credential_is_committed_before_broker_submit(monkeypatch):
    calls = []
    monkeypatch.setattr(recovery_module, "get_db_connection", lambda: FakeConnection(calls))

    RecoveryHarness()._prepare_submission(
        order_id=19,
        exchange_id="futu",
        market_type="USStock",
        client_order_id="qd_2_19",
        credential_id=7,
    )

    assert "credential_id = COALESCE" in calls[0][0]
    assert calls[0][1] == ("futu", 7, "qd_2_19", 19)


def test_submission_is_not_sent_when_processing_lease_was_lost(monkeypatch):
    monkeypatch.setattr(
        recovery_module,
        "get_db_connection",
        lambda: FakeConnection([], rowcount=0),
    )
    recovery = RecoveryHarness()

    with pytest.raises(RuntimeError, match="submission_lease_lost"):
        recovery._prepare_submission(
            order_id=18,
            exchange_id="ibkr",
            market_type="USStock",
            client_order_id="qd_2_18",
        )

    assert recovery.bindings == []


def test_stale_precommitted_submission_moves_to_reconciliation(monkeypatch):
    calls = []
    monkeypatch.setattr(
        recovery_module,
        "get_db_connection",
        lambda: FakeConnection(calls),
    )

    RecoveryHarness()._recover_stale_submissions(90)

    assert "SET status = 'sent'" in calls[0][0]
    assert "client_order_id" in calls[0][0]
    assert calls[0][1] == (90,)

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import app.services.pending_order_worker as worker_module
from app.services.pending_order_worker import PendingOrderWorker


@pytest.mark.parametrize(
    ("order_type", "limit_price", "market_category", "expected_error"),
    [
        ("market", 0, "USStock", "futu_explicit_limit_price_required"),
        ("limit", 0, "USStock", "futu_explicit_limit_price_required"),
        ("limit", 100, "HKStock", "futu_credential_market_mismatch"),
    ],
)
def test_futu_worker_rejects_wrong_market_or_non_explicit_limit_orders(
    order_type, limit_price, market_category, expected_error,
):
    worker = PendingOrderWorker.__new__(PendingOrderWorker)
    worker._mark_failed = MagicMock()
    client = MagicMock()
    notify = MagicMock()

    worker._execute_futu_order(
        order_id=10,
        order_row={"symbol": "US.SPY", "signal_type": "open_long", "amount": 1},
        payload={"order_type": order_type, "limit_price": limit_price, "ref_price": 100},
        client=client,
        strategy_id=1,
        exchange_config={"exchange_id": "futu"},
        market_category=market_category,
        _notify_live_best_effort=notify,
        _console_print=MagicMock(),
    )

    worker._mark_failed.assert_called_once_with(order_id=10, error=expected_error)
    notify.assert_called_once_with(status="failed", error=expected_error)
    client.find_order_by_remark.assert_not_called()
    client.place_limit_order.assert_not_called()
    client.place_market_order.assert_not_called()


class _FakeFutuClient:
    def __init__(self, result):
        self.result = result
        self.disconnected = False

    def get_order_status(self, _order_id):
        return self.result

    def disconnect(self):
        self.disconnected = True


def _worker_with_claim(row):
    worker = PendingOrderWorker.__new__(PendingOrderWorker)
    worker._claim_futu_sent_order = MagicMock(return_value=dict(row))
    worker._release_futu_sync_claim = MagicMock()
    worker._update_futu_sent_order_snapshot = MagicMock()
    worker._unrecorded_pending_fill = MagicMock(return_value=0.0)
    return worker


def _configure_futu_strategy(monkeypatch, client):
    monkeypatch.setattr(
        worker_module,
        "load_strategy_configs",
        lambda _strategy_id: {
            "user_id": 1,
            "exchange_config": {"exchange_id": "futu"},
        },
    )
    monkeypatch.setattr(
        worker_module,
        "resolve_exchange_config",
        lambda config, user_id: config,
    )
    monkeypatch.setattr(worker_module, "create_client", lambda *_args, **_kwargs: client)
    monkeypatch.setattr(worker_module, "FutuClient", _FakeFutuClient)


def test_failed_status_query_requeues_without_overwriting_fill(monkeypatch):
    row = {
        "id": 17,
        "exchange_order_id": "OID-17",
        "strategy_id": 9,
        "filled": 5,
        "avg_price": 100,
    }
    result = SimpleNamespace(
        success=False,
        status="",
        filled=0,
        avg_price=0,
        raw={},
        message="OpenD unavailable",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    _configure_futu_strategy(monkeypatch, client)

    worker._sync_one_futu_sent_order(row)

    worker._release_futu_sync_claim.assert_called_once_with(17, "not_finalized")
    worker._update_futu_sent_order_snapshot.assert_not_called()
    assert client.disconnected


def test_invalid_claimed_order_is_requeued_immediately():
    row = {
        "id": 18,
        "exchange_order_id": "OID-18",
        "strategy_id": 0,
    }
    worker = _worker_with_claim(row)

    worker._sync_one_futu_sent_order(row)

    worker._release_futu_sync_claim.assert_called_once_with(18, "not_finalized")
    worker._update_futu_sent_order_snapshot.assert_not_called()


def test_futu_sync_rejects_non_us_market_before_querying_broker(monkeypatch):
    row = {"id": 22, "exchange_order_id": "OID-22", "strategy_id": 9}
    client = MagicMock()
    worker = _worker_with_claim(row)
    _configure_futu_strategy(monkeypatch, client)
    monkeypatch.setattr(
        worker_module,
        "load_strategy_configs",
        lambda _strategy_id: {
            "user_id": 1,
            "market_category": "HKStock",
            "exchange_config": {"exchange_id": "futu"},
        },
    )

    worker._sync_one_futu_sent_order(row)

    client.get_order_status.assert_not_called()
    worker._update_futu_sent_order_snapshot.assert_not_called()
    worker._release_futu_sync_claim.assert_called_once_with(22, "not_finalized")


def test_successful_regressive_snapshot_preserves_recorded_fill(monkeypatch):
    row = {
        "id": 19,
        "exchange_order_id": "OID-19",
        "strategy_id": 9,
        "filled": 5,
        "avg_price": 100,
    }
    result = SimpleNamespace(
        success=True,
        status="submitted",
        filled=0,
        avg_price=0,
        raw={},
        message="OK",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    _configure_futu_strategy(monkeypatch, client)

    worker._sync_one_futu_sent_order(row)

    worker._release_futu_sync_claim.assert_not_called()
    update = worker._update_futu_sent_order_snapshot.call_args.kwargs
    assert update["filled"] == 5
    assert update["avg_price"] == 100
    assert update["status"] == "sent"
    assert client.disconnected


def test_terminal_snapshot_discards_cached_client(monkeypatch):
    row = {
        "id": 21,
        "exchange_order_id": "OID-21",
        "strategy_id": 9,
        "filled": 0,
        "avg_price": 0,
    }
    result = SimpleNamespace(
        success=True,
        status="filled",
        filled=0,
        avg_price=0,
        raw={},
        message="OK",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    _configure_futu_strategy(monkeypatch, client)

    worker._sync_one_futu_sent_order(row)

    assert client.disconnected


def test_filled_snapshot_without_broker_price_stays_retryable(monkeypatch):
    row = {
        "id": 23,
        "exchange_order_id": "OID-23",
        "strategy_id": 9,
        "filled": 0,
        "avg_price": 0,
    }
    result = SimpleNamespace(
        success=True,
        status="filled",
        filled=1,
        avg_price=0,
        raw={},
        message="OK",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    worker._unrecorded_pending_fill.return_value = 1
    _configure_futu_strategy(monkeypatch, client)
    persist = MagicMock()
    monkeypatch.setattr(worker_module, "persist_strategy_fill", persist)

    worker._sync_one_futu_sent_order(row)

    persist.assert_not_called()
    worker._update_futu_sent_order_snapshot.assert_not_called()
    worker._release_futu_sync_claim.assert_called_once_with(23, "not_finalized")
    assert client.disconnected


def test_immediate_futu_fill_waits_for_durable_reconciliation(monkeypatch):
    worker = PendingOrderWorker.__new__(PendingOrderWorker)
    worker._prepare_submission = MagicMock()
    worker._mark_sent = MagicMock()
    worker._mark_failed = MagicMock()
    worker._unrecorded_pending_fill = MagicMock(return_value=1)
    client = MagicMock()
    client.find_order_by_remark.return_value = None
    client.place_limit_order.return_value = SimpleNamespace(
        success=True,
        status="filled",
        filled=1,
        avg_price=0,
        order_id="OID-24",
        raw={},
    )
    persist = MagicMock()
    monkeypatch.setattr(worker_module, "persist_strategy_fill", persist)
    monkeypatch.setattr(worker_module, "append_strategy_log", MagicMock())

    worker._execute_futu_order(
        order_id=24,
        order_row={"symbol": "US.SPY", "signal_type": "open_long", "amount": 1},
        payload={
            "signal_type": "open_long",
            "symbol": "US.SPY",
            "amount": 1,
            "order_type": "limit",
            "limit_price": 100,
            "ref_price": 99,
        },
        client=client,
        strategy_id=1,
        exchange_config={"exchange_id": "futu"},
        market_category="USStock",
        _notify_live_best_effort=MagicMock(),
        _console_print=MagicMock(),
    )

    worker._mark_failed.assert_not_called()
    sent = worker._mark_sent.call_args.kwargs
    assert sent["filled"] == 0
    assert sent["avg_price"] == 0
    assert sent["final_filled"] is False
    persist.assert_not_called()


def test_immediate_futu_fill_records_broker_price_without_advancing_pending_snapshot(monkeypatch):
    worker = PendingOrderWorker.__new__(PendingOrderWorker)
    worker._prepare_submission = MagicMock()
    worker._mark_sent = MagicMock()
    worker._mark_failed = MagicMock()
    worker._unrecorded_pending_fill = MagicMock(return_value=1)
    client = MagicMock()
    client.find_order_by_remark.return_value = None
    client.place_limit_order.return_value = SimpleNamespace(
        success=True,
        status="filled",
        filled=1,
        avg_price=99.5,
        order_id="OID-25",
        raw={},
    )
    persist = MagicMock(return_value=(None, None))
    monkeypatch.setattr(worker_module, "persist_strategy_fill", persist)
    monkeypatch.setattr(worker_module, "append_strategy_log", MagicMock())

    worker._execute_futu_order(
        order_id=25,
        order_row={"symbol": "US.SPY", "signal_type": "open_long", "amount": 1},
        payload={"signal_type": "open_long", "symbol": "US.SPY", "amount": 1, "order_type": "limit", "limit_price": 100},
        client=client,
        strategy_id=1,
        exchange_config={"exchange_id": "futu"},
        market_category="USStock",
        _notify_live_best_effort=MagicMock(),
        _console_print=MagicMock(),
    )

    sent = worker._mark_sent.call_args.kwargs
    assert (sent["filled"], sent["avg_price"], sent["final_filled"]) == (0, 0, False)
    assert persist.call_args.kwargs["filled"] == 1
    assert persist.call_args.kwargs["avg_price"] == 99.5
    assert persist.call_args.kwargs["market_type"] == "USStock"
    assert persist.call_args.kwargs["cumulative_filled"] == 1
    worker._unrecorded_pending_fill.assert_called_once_with(25, 1.0, fail_closed=True)


def test_ambiguous_futu_submit_keeps_durable_remark_for_reconciliation(monkeypatch):
    worker = PendingOrderWorker.__new__(PendingOrderWorker)
    worker._prepare_submission = MagicMock()
    worker._mark_submit_unknown = MagicMock()
    worker._mark_failed = MagicMock()
    client = MagicMock()
    client.find_order_by_remark.return_value = None
    client.place_limit_order.return_value = SimpleNamespace(
        success=False, message="connection timed out",
    )
    monkeypatch.setattr(worker_module, "append_strategy_log", MagicMock())

    worker._execute_futu_order(
        order_id=27,
        order_row={"symbol": "US.SPY", "signal_type": "open_long", "amount": 1},
        payload={"signal_type": "open_long", "symbol": "US.SPY", "amount": 1,
                 "order_type": "limit", "limit_price": 100},
        client=client,
        strategy_id=1,
        exchange_config={"exchange_id": "futu"},
        market_category="USStock",
        _notify_live_best_effort=MagicMock(),
        _console_print=MagicMock(),
    )

    worker._prepare_submission.assert_called_once_with(
        order_id=27, exchange_id="futu", market_type="USStock",
        client_order_id="qd_1_27",
    )
    worker._mark_submit_unknown.assert_called_once()
    worker._mark_failed.assert_not_called()


def test_futu_sync_recovers_broker_id_from_precommitted_remark(monkeypatch):
    row = {"id": 28, "exchange_order_id": "", "client_order_id": "qd_1_28", "strategy_id": 9,
           "filled": 0, "avg_price": 0}
    result = SimpleNamespace(success=True, order_id="OID-28", status="submitted",
                             filled=0, avg_price=0, raw={}, message="matched_by_remark")

    class RemarkClient(_FakeFutuClient):
        def find_order_by_remark(self, remark):
            assert remark == "qd_1_28"
            return result

    client = RemarkClient(result)
    worker = _worker_with_claim(row)
    worker._bind_reconciled_exchange_order_id = MagicMock()
    _configure_futu_strategy(monkeypatch, client)
    monkeypatch.setattr(worker_module, "FutuClient", RemarkClient)

    worker._sync_one_futu_sent_order(row)

    worker._bind_reconciled_exchange_order_id.assert_called_once_with(
        order_id=28, exchange_id="futu", market_type="USStock",
        client_order_id="qd_1_28", exchange_order_id="OID-28", observed_filled=0.0,
    )
    worker._update_futu_sent_order_snapshot.assert_called_once()


def test_retry_uses_durable_trade_ledger_to_avoid_duplicate_fill(monkeypatch):
    row = {
        "id": 20,
        "exchange_order_id": "OID-20",
        "strategy_id": 9,
        "filled": 0,
        "avg_price": 0,
    }
    result = SimpleNamespace(
        success=True,
        status="partially_filled",
        filled=5,
        avg_price=101,
        raw={},
        message="OK",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    _configure_futu_strategy(monkeypatch, client)
    persist = MagicMock()
    monkeypatch.setattr(worker_module, "persist_strategy_fill", persist)

    worker._sync_one_futu_sent_order(row)

    worker._unrecorded_pending_fill.assert_called_once_with(
        20,
        5.0,
        fail_closed=True,
    )
    persist.assert_not_called()
    update = worker._update_futu_sent_order_snapshot.call_args.kwargs
    assert update["filled"] == 5


def test_futu_rest_sync_uses_atomic_cumulative_fill_persistence(monkeypatch):
    row = {"id": 26, "exchange_order_id": "OID-26", "strategy_id": 9, "filled": 0, "avg_price": 0}
    result = SimpleNamespace(
        success=True, status="filled", filled=1, avg_price=99.5, raw={}, message="OK",
    )
    client = _FakeFutuClient(result)
    worker = _worker_with_claim(row)
    worker._unrecorded_pending_fill.return_value = 1
    _configure_futu_strategy(monkeypatch, client)
    persist = MagicMock(return_value=(None, None))
    monkeypatch.setattr(worker_module, "persist_strategy_fill", persist)
    monkeypatch.setattr(worker_module, "append_strategy_log", MagicMock())

    worker._sync_one_futu_sent_order(row)

    saved = persist.call_args.kwargs
    assert saved["cumulative_filled"] == 1
    assert saved["cumulative_average_price"] == 99.5
    assert saved["market_type"] == "USStock"
    assert worker._update_futu_sent_order_snapshot.call_args.kwargs["status"] == "filled"


def test_futu_rest_sync_accounts_for_ingested_stream_events(monkeypatch):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"recorded": 2},
        {"cumulative": 5, "incremental": 3},
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    context = MagicMock()
    context.__enter__.return_value = connection
    context.__exit__.return_value = False
    monkeypatch.setattr(worker_module, "get_db_connection", lambda: context)

    delta = PendingOrderWorker._unrecorded_pending_fill(
        20,
        6,
        fail_closed=True,
        include_stream_events=True,
    )

    assert delta == 1
    assert cursor.execute.call_count == 2

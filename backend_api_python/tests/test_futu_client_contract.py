"""Contract tests for FutuClient with a mocked futu-api SDK."""

from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from app.services.futu_trading.client import FutuClient
from app.services.futu_trading.config import FutuConfig


class _FakeFT:
    RET_OK = 0
    RET_ERROR = -1

    class TrdEnv:
        SIMULATE = "SIMULATE"
        REAL = "REAL"

    class TrdMarket:
        HK = "HK"
        US = "US"
        NONE = "NONE"

    class SecurityFirm:
        FUTUSECURITIES = "FUTUSECURITIES"

    class TrdSide:
        BUY = "BUY"
        SELL = "SELL"

    class OrderType:
        MARKET = "MARKET"
        NORMAL = "NORMAL"

    class OrderStatus:
        SUBMITTED = "SUBMITTED"
        FILLED_PART = "FILLED_PART"
        FILLED_ALL = "FILLED_ALL"
        WAITING_SUBMIT = "WAITING_SUBMIT"
        SUBMITTING = "SUBMITTING"

    class ModifyOrderOp:
        CANCEL = "CANCEL"

    class KLType:
        K_DAY = "K_DAY"

    class AuType:
        QFQ = "QFQ"

    class SubType:
        QUOTE = "QUOTE"

    class TradeOrderHandlerBase:
        def on_recv_rsp(self, rsp_pb):
            return rsp_pb

    class TradeDealHandlerBase:
        def on_recv_rsp(self, rsp_pb):
            return rsp_pb


def _client_with_mocks():
    cfg = FutuConfig(host="127.0.0.1", port=11111, trade_env="demo", trade_market="US", acc_id=99)
    client = FutuClient(cfg)
    quote = MagicMock()
    trade = MagicMock()
    quote.get_global_state.return_value = (_FakeFT.RET_OK, {"trd_logined": True, "server_ver": 1})
    trade.get_acc_list.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{"acc_id": 99, "trd_env": "SIMULATE", "acc_type": "STOCK"}]),
    )
    client._quote_ctx = quote
    client._trade_ctx = trade
    client._connected = True
    client._acc_id = 99
    client._accounts = [{"acc_id": 99, "trd_env": "SIMULATE", "trdmarket_auth": ["US"],
                         "sim_acc_type": "STOCK_AND_OPTION"}]
    return client, quote, trade


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_start_push_registers_paper_order_handler_only(_ensure):
    client, _quote, trade = _client_with_mocks()

    assert client.start_push()

    handlers = [call.args[0] for call in trade.set_handler.call_args_list]
    assert len(handlers) == 1
    assert any(isinstance(handler, _FakeFT.TradeOrderHandlerBase) for handler in handlers)


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_place_limit_order_success(_ensure):
    client, quote, trade = _client_with_mocks()
    client._is_regular_us_session_now = lambda: True
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{"lot_size": 100, "last_price": 350.0}]),
    )
    trade.place_order.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{
            "order_id": "OID-1",
            "order_status": "SUBMITTED",
            "dealt_qty": 0,
            "dealt_avg_price": 0,
            "qty": 100,
            "price": 350.0,
            "code": "US.AAPL",
            "trd_side": "BUY",
            "remark": "r1",
        }]),
    )
    with patch("app.services.futu_trading.operator_gate.submission_permit", return_value=nullcontext()):
        result = client.place_limit_order("AAPL", "buy", 100, 350.0, "USStock", remark="qd_1_2")
    assert result.success
    assert result.order_id == "OID-1"
    assert result.status == "submitted"
    kwargs = trade.place_order.call_args.kwargs
    assert kwargs["code"] == "US.AAPL"
    assert kwargs["qty"] == 100
    assert kwargs["remark"] == "qd_1_2"


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_hk_stock_simulate_requires_exact_lot_and_hk_session(_ensure):
    client, quote, trade = _client_with_mocks()
    client.config = FutuConfig(trade_market="HK", market_category="HKStock", acc_id=99)
    client._accounts = [{"acc_id": 99, "trd_env": "SIMULATE", "trdmarket_auth": ["HK"],
                         "sim_acc_type": "STOCK"}]
    client._is_regular_hk_session_now = lambda: True
    client.get_simulate_execution_quote = lambda _: {"simulate_execution_eligible": True, "price": 420.0}
    quote.get_market_snapshot.return_value = (_FakeFT.RET_OK, pd.DataFrame([{"lot_size": 100}]))
    trade.acctradinginfo_query.return_value = (_FakeFT.RET_OK, pd.DataFrame([{"max_cash_buy": 1000}]))
    trade.place_order.return_value = (_FakeFT.RET_OK, pd.DataFrame([{
        "order_id": "HK-1", "order_status": "SUBMITTED", "qty": 100,
        "code": "HK.00700", "price": 420.0, "trd_side": "BUY",
    }]))
    with patch("app.services.futu_trading.operator_gate.submission_permit", return_value=nullcontext()):
        bad = client.place_limit_order("00700.HK", "buy", 50, 420.0, "HKStock", remark="qd_1_2")
        good = client.place_limit_order("00700.HK", "buy", 100, 420.0, "HKStock", remark="qd_1_2")
    assert "FUTU_INVALID_LOT_SIZE" in bad.message
    assert good.success and trade.place_order.call_args.kwargs["code"] == "HK.00700"
    assert trade.acctradinginfo_query.call_args.kwargs["acc_id"] == 99
    client._is_regular_hk_session_now = lambda: False
    closed = client.place_limit_order("00700.HK", "buy", 100, 420.0, "HKStock", remark="qd_1_3")
    assert closed.message == "FUTU_REGULAR_SESSION_ONLY"


def test_hk_option_simulate_account_cannot_be_used_for_stock_orders():
    client = FutuClient(FutuConfig(trade_market="HK", market_category="HKStock", acc_id=99))
    client._acc_id = 99
    client._accounts = [{"acc_id": 99, "trd_env": "SIMULATE", "trdmarket_auth": ["HK"],
                         "sim_acc_type": "OPTION"}]
    with pytest.raises(ValueError, match="FUTU_HK_STOCK_SIM_ACCOUNT_REQUIRED"):
        client._acc_id_arg()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_place_limit_order_is_default_denied_without_operator_arm(_ensure, monkeypatch):
    client, quote, trade = _client_with_mocks()
    monkeypatch.delenv("FUTU_PAPER_AUTOTRADE_ALLOWED", raising=False)
    client._is_regular_us_session_now = lambda: True
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK, pd.DataFrame([{"lot_size": 1, "last_price": 100.0}]),
    )

    result = client.place_limit_order("AAPL", "buy", 1, 100.0, "USStock", remark="qd_1_2")

    assert not result.success
    assert result.submission_attempted is False
    assert "FUTU_PAPER_AUTOTRADE_HARD_DISABLED" in result.message
    trade.place_order.assert_not_called()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_place_order_rejects_bad_lot(_ensure):
    client, quote, trade = _client_with_mocks()
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{"lot_size": 100, "last_price": 350.0}]),
    )
    result = client.place_limit_order("AAPL", "buy", 50, 350.0, "USStock")
    assert not result.success
    assert "FUTU_INVALID_LOT_SIZE" in result.message
    trade.place_order.assert_not_called()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_orders_fail_closed_without_explicit_simulate_us_account(_ensure):
    client, quote, trade = _client_with_mocks()
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK, pd.DataFrame([{"lot_size": 1, "last_price": 100.0}]),
    )
    client._acc_id = 0
    assert "FUTU_SIM_ACCOUNT_SELECTION_REQUIRED" in client.place_limit_order("AAPL", "buy", 1, 100).message
    client._acc_id = 99
    client._accounts = [{"acc_id": 99, "trd_env": "REAL", "trdmarket_auth": ["US"]}]
    assert "FUTU_SELECTED_ACCOUNT_NOT_SIMULATE" in client.place_limit_order("AAPL", "buy", 1, 100).message
    trade.place_order.assert_not_called()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_position_query_error_cannot_masquerade_as_flat_account(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.position_list_query.return_value = (_FakeFT.RET_ERROR, "OpenD unavailable")

    with pytest.raises(RuntimeError, match="FUTU_POSITION_QUERY_FAILED"):
        client.get_positions()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_opend_status_fails_closed_after_disconnect(_ensure):
    client, quote, _trade = _client_with_mocks()
    assert client.get_connection_status()["connected"] is True

    quote.get_global_state.return_value = (_FakeFT.RET_ERROR, "connection lost")
    failed = client.get_connection_status()
    assert failed["connected"] is False
    assert failed["opend_connected"] is False
    assert failed["opend_error"] == "FUTU_OPEND_STATUS_UNAVAILABLE"

    quote.get_global_state.return_value = (_FakeFT.RET_OK, {"trd_logined": False})
    logged_out = client.get_connection_status()
    assert logged_out["connected"] is False
    assert logged_out["opend_connected"] is False


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_quote_snapshot_and_probe_are_json_serializable(_ensure):
    client, quote, _trade = _client_with_mocks()
    client._acc_id = 0  # A probe must work before the operator selects an account.
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{
            "code": "US.AAPL",
            "last_price": 100.0,
            "lot_size": np.int64(1),
            "suspension": np.bool_(False),
            "optional_price": np.nan,
            "update_time": pd.Timestamp("2026-09-29 09:30:00"),
        }]),
    )

    quote_result = json.loads(json.dumps(client.get_quote("AAPL", "USStock"), allow_nan=False))
    assert quote_result["success"] is True
    assert quote_result["raw"]["lot_size"] == 1
    assert quote_result["raw"]["suspension"] is False
    assert quote_result["raw"]["optional_price"] is None
    json.dumps(client.probe_permissions(), allow_nan=False)


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_orders_reject_market_and_hk(_ensure):
    client, _quote, trade = _client_with_mocks()
    assert "FUTU_LIMIT_ORDERS_ONLY" in client.place_market_order("AAPL", "buy", 1).message
    assert "Unsupported market_type" in client.place_limit_order("00700.HK", "buy", 1, 1, "HKStock").message
    trade.place_order.assert_not_called()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_paper_order_rejects_outside_regular_session_and_short_sell(_ensure):
    client, quote, trade = _client_with_mocks()
    quote.get_market_snapshot.return_value = (
        _FakeFT.RET_OK, pd.DataFrame([{"lot_size": 1, "last_price": 100.0}]),
    )
    client._is_regular_us_session_now = lambda: False
    assert "FUTU_REGULAR_SESSION_ONLY" in client.place_limit_order("AAPL", "buy", 1, 100).message
    client._is_regular_us_session_now = lambda: True
    trade.position_list_query.return_value = (
        _FakeFT.RET_OK, pd.DataFrame([{"code": "US.AAPL", "qty": 1, "can_sell_qty": 1}]),
    )
    assert "FUTU_LONG_ONLY_INSUFFICIENT_POSITION" in client.place_limit_order("AAPL", "sell", 2, 100).message
    trade.place_order.assert_not_called()


@pytest.mark.parametrize(
    ("market", "code", "other_code", "display", "quantity"),
    [
        ("US", "US.AAPL", "HK.00700", "AAPL", 1),
        ("HK", "HK.00700", "US.AAPL", "00700.HK", 100),
    ],
)
@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_recent_paper_orders_include_terminal_states_and_deduplicate(
    _ensure, market, code, other_code, display, quantity,
):
    client, _quote, trade = _client_with_mocks()
    client.config = FutuConfig(
        trade_market=market, market_category=f"{market}Stock", acc_id=99,
    )
    client._accounts = [{
        "acc_id": 99, "trd_env": "SIMULATE", "trdmarket_auth": [market],
        "sim_acc_type": "STOCK",
    }]
    history = pd.DataFrame([{
        "order_id": "OID-1", "code": code, "order_status": "SUBMITTED",
        "qty": quantity, "dealt_qty": 0, "price": 100,
        "updated_time": "2026-09-28 09:30:00",
    }])
    current = pd.DataFrame([{
        "order_id": "OID-1", "code": code, "order_status": "FILLED_ALL",
        "qty": quantity, "dealt_qty": quantity, "dealt_avg_price": 100, "price": 100,
        "updated_time": "2026-09-28 09:31:00",
    }, {
        "order_id": "OTHER-MARKET", "code": other_code, "order_status": "FILLED_ALL",
        "qty": 1, "dealt_qty": 1, "dealt_avg_price": 100, "price": 100,
        "updated_time": "2026-09-28 09:32:00",
    }])
    trade.history_order_list_query.return_value = (_FakeFT.RET_OK, history)
    trade.order_list_query.return_value = (_FakeFT.RET_OK, current)

    orders = client.get_recent_orders()

    assert len(orders) == 1
    assert orders[0]["symbol"] == display
    assert orders[0]["status"] == "filled"
    assert orders[0]["filled"] == quantity
    assert orders[0]["avgFillPrice"] == 100
    assert trade.history_order_list_query.call_args.kwargs["trd_env"] == _FakeFT.TrdEnv.SIMULATE
    assert trade.history_order_list_query.call_args.kwargs["acc_id"] == 99


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_get_order_status_and_find_by_remark(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.order_list_query.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{
            "order_id": "OID-9",
            "order_status": "FILLED_ALL",
            "dealt_qty": 100,
            "dealt_avg_price": 351.2,
            "qty": 100,
            "code": "HK.00700",
            "trd_side": "BUY",
            "remark": "futu-remark",
        }]),
    )
    status = client.get_order_status("OID-9")
    assert status.success
    assert status.status == "filled"
    assert status.filled == 100
    found = client.find_order_by_remark("futu-remark")
    assert found is not None
    assert found.order_id == "OID-9"


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_duplicate_broker_remark_cannot_be_used_as_unique_order_identity(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.order_list_query.return_value = (_FakeFT.RET_OK, pd.DataFrame([
        {"order_id": "OID-1", "remark": "qd_agent_9", "order_status": "SUBMITTED"},
        {"order_id": "OID-2", "remark": "qd_agent_9", "order_status": "SUBMITTED"},
    ]))
    assert client.find_order_by_remark("qd_agent_9", refresh_cache=True) is None


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_account_summary_rejects_empty_broker_response(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.accinfo_query.return_value = (_FakeFT.RET_OK, pd.DataFrame())

    summary = client.get_account_summary()

    assert summary == {"success": False, "error": "FUTU_ACCOUNT_QUERY_FAILED"}


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_recent_orders_distinguishes_empty_history_from_query_failure(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.history_order_list_query.return_value = (-1, "OpenD disconnected")
    trade.order_list_query.return_value = (-1, "OpenD disconnected")

    with pytest.raises(RuntimeError, match="FUTU_ORDER_QUERY_FAILED"):
        client.get_recent_orders()

    trade.order_list_query.return_value = (_FakeFT.RET_OK, pd.DataFrame())
    assert client.get_recent_orders() == []


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_get_order_status_treats_empty_query_as_failure(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.order_list_query.return_value = (_FakeFT.RET_OK, pd.DataFrame())

    status = client.get_order_status("OID-MISSING")

    assert not status.success
    assert status.filled == 0
    assert "not found" in status.message.lower()


@patch("app.services.futu_trading.client._ensure_futu")
def test_trading_client_rejects_remote_host_before_loading_sdk(ensure_futu, monkeypatch):
    monkeypatch.delenv("FUTU_ALLOW_REMOTE_OPEND", raising=False)
    client = FutuClient(FutuConfig(host="169.254.169.254"))

    assert client.connect() is False
    ensure_futu.assert_not_called()


@patch("app.services.futu_trading.client._ensure_futu", return_value=_FakeFT)
def test_cancel_order(_ensure):
    client, _quote, trade = _client_with_mocks()
    trade.modify_order.return_value = (_FakeFT.RET_OK, pd.DataFrame([{"order_id": "OID-1"}]))
    assert client.cancel_order("OID-1") is True


def test_parse_futu_deal_normalizer():
    try:
        from app.services.execution_streams.normalizers import parse_futu_deal
    except ModuleNotFoundError:
        pytest.skip("optional deps missing for execution_streams import")

    events = parse_futu_deal({
        "code": "HK.00700",
        "order_id": "OID-1",
        "deal_id": "D-1",
        "qty": 100,
        "price": 350.0,
        "trd_side": "BUY",
        "remark": "r1",
        "dealt_qty": 100,
        "order_status": "FILLED_ALL",
        "create_time": "2026-08-10 10:00:00",
    })
    assert len(events) == 1
    assert events[0].exchange_id == "futu"
    assert events[0].symbol == "00700.HK"
    assert events[0].quantity == 100
    assert events[0].client_order_id == "r1"
    assert events[0].occurred_at == datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)


def test_parse_futu_order_snapshot_uses_cumulative_fill_and_average_price():
    from app.services.execution_streams.normalizers import parse_futu_deal

    base = {
        "code": "HK.00700",
        "order_id": "OID-2",
        "qty": 100,
        "price": 350.0,
        "dealt_avg_price": 349.2,
        "trd_side": "BUY",
        "order_status": "FILLED_PART",
        "updated_time": "2026-08-10 10:01:00",
    }
    first = parse_futu_deal({**base, "dealt_qty": 20})[0]
    second = parse_futu_deal({**base, "dealt_qty": 40})[0]

    assert first.exchange_fill_id == ""
    assert first.price == 349.2
    assert first.cumulative_average_price == 349.2
    assert first.quantity == 0
    assert first.cumulative_quantity == 20
    assert first.is_cumulative
    assert first.event_key() != second.event_key()


def test_futu_order_limit_price_is_not_execution_evidence():
    from app.services.execution_streams.normalizers import parse_futu_deal

    event = parse_futu_deal({
        "code": "US.SPY",
        "order_id": "OID-3",
        "price": 700,
        "dealt_qty": 1,
        "dealt_avg_price": 0,
        "order_status": "FILLED_ALL",
    })[0]

    assert event.price == 0
    assert event.cumulative_average_price == 0
    assert event.cumulative_quantity == 1


def test_futu_trade_deal_with_executed_price_needs_no_rest_snapshot(monkeypatch):
    from app.services.execution_streams.fill_snapshot import complete_snapshot
    from app.services.execution_streams.normalizers import parse_futu_deal

    event = parse_futu_deal({
        "code": "US.SPY",
        "order_id": "OID-4",
        "deal_id": "DEAL-4",
        "qty": 1,
        "price": 699.5,
        "dealt_qty": 1,
        "order_status": "FILLED_ALL",
    })[0]
    def unexpected_query(*_args, **_kwargs):
        raise AssertionError("Futu deal must not require a generic REST fill query")

    monkeypatch.setattr(
        "app.services.grid.exchange_orders.wait_grid_market_fill",
        unexpected_query,
    )
    snapshot = complete_snapshot(event.__dict__)

    assert snapshot["quantity"] == 1
    assert snapshot["price"] == 699.5


@patch("app.services.futu_trading.client._ensure_futu")
def test_connect_trade_only_skips_quote_context(ensure):
    ft = _FakeFT()
    quote_ctx = MagicMock()
    trade_ctx = MagicMock()
    trade_ctx.get_acc_list.return_value = (
        _FakeFT.RET_OK,
        pd.DataFrame([{"acc_id": 7, "trd_env": "SIMULATE", "acc_type": "STOCK"}]),
    )
    ft.OpenQuoteContext = MagicMock(return_value=quote_ctx)
    ft.OpenSecTradeContext = MagicMock(return_value=trade_ctx)
    ensure.return_value = ft

    client = FutuClient(FutuConfig(host="127.0.0.1", port=11111, trade_env="demo"))
    assert client.connect(need_quote=False)
    ft.OpenQuoteContext.assert_not_called()
    ft.OpenSecTradeContext.assert_called_once()
    assert client.connected
    assert client._quote_ctx is None
    status = client.get_connection_status()
    assert status["quote_ctx"] is False
    assert status["trade_ctx"] is True

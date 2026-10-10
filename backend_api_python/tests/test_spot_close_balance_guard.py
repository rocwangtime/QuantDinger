from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.services.live_trading.base import LiveTradingError
from app.services.live_trading.binance_spot import BinanceSpotClient
from app.services.live_trading.bitget_spot import BitgetSpotClient
from app.services.live_trading.bybit import BybitClient
from app.services.live_trading.gate import GateSpotClient, GateStockClient
from app.services.live_trading.htx import HtxClient
from app.services.live_trading.okx import OkxClient
from app.services.live_trading.spot_sizing import clamp_spot_close_quantity, get_spot_base_holding


def spot_client(exchange, free):
    clients = {
        "binance": (BinanceSpotClient, "get_account", {"balances": [{"asset": "BTC", "free": free, "locked": "1"}]}),
        "bitget": (BitgetSpotClient, "get_assets", {"data": [{"coin": "BTC", "available": free, "frozen": "1"}]}),
        "bybit": (BybitClient, "get_wallet_balance", {"result": {"list": [{"coin": [{"coin": "BTC", "free": free, "walletBalance": "1"}]}]}}),
        "gate": (GateSpotClient, "get_accounts", [{"currency": "BTC", "available": free, "locked": "1"}]),
        "gate_stock": (GateStockClient, "get_positions", {"data": {"list": [{"symbol": "BTC", "available": free, "volume": "1"}]}}),
        "htx": (HtxClient, "get_balance", {"data": {"list": [{"currency": "btc", "type": "trade", "balance": free}, {"currency": "btc", "type": "frozen", "balance": "1"}]}}),
        "okx": (OkxClient, "get_balance", {"data": [{"details": [{"ccy": "BTC", "availBal": free, "cashBal": "1"}]}]}),
    }
    cls, method, response = clients[exchange]
    client = MagicMock(spec=cls)
    client.market_type = "spot"
    client.category = "spot"
    getattr(client, method).return_value = response
    if hasattr(client, "_normalize_quantity"):
        client._normalize_quantity.side_effect = lambda **kw: (Decimal(str(kw["quantity"])), 8)
    if hasattr(client, "_normalize_base_size"):
        client._normalize_base_size.side_effect = lambda **kw: (Decimal(str(kw["base_size"])), 8)
    return client, getattr(client, method)


EXCHANGES = ["binance", "bitget", "bybit", "gate", "gate_stock", "htx", "okx"]


@pytest.mark.parametrize("exchange", EXCHANGES)
@pytest.mark.parametrize("free", ["0", "0.25"])
def test_spot_close_caps_against_confirmed_free_balance(exchange, free):
    client, _ = spot_client(exchange, free)
    quantity, meta = clamp_spot_close_quantity(client, symbol="BTC/USDT", requested_qty=1, safety_ratio=1)
    assert quantity == float(free)
    assert meta["exchange_free"] == float(free)
    assert meta["adjusted"] is True


@pytest.mark.parametrize("exchange", EXCHANGES)
@pytest.mark.parametrize("free", [None, "", "bad", "NaN", "Infinity", "-1"])
def test_spot_close_rejects_unconfirmed_available_balance(exchange, free):
    client, _ = spot_client(exchange, free)
    with pytest.raises(LiveTradingError, match="strategyRuntime.spotBalanceUnavailable"):
        clamp_spot_close_quantity(client, symbol="BTC/USDT", requested_qty=1)


@pytest.mark.parametrize("exchange", EXCHANGES)
@pytest.mark.parametrize("failure", ["timeout", "invalid_response"])
def test_spot_close_rejects_failed_balance_query(exchange, failure):
    client, fetch = spot_client(exchange, "1")
    if failure == "timeout":
        fetch.side_effect = TimeoutError("balance timeout")
    else:
        fetch.return_value = {"error": "balance unavailable"}
    with pytest.raises(LiveTradingError, match="strategyRuntime.spotBalanceUnavailable"):
        clamp_spot_close_quantity(client, symbol="BTC/USDT", requested_qty=1)


def test_spot_close_missing_asset_in_valid_snapshot_is_zero():
    client, fetch = spot_client("binance", "1")
    fetch.return_value = {"balances": []}
    assert clamp_spot_close_quantity(client, symbol="BTC/USDT", requested_qty=1)[0] == 0


def test_bybit_spot_balance_prefers_unified_account():
    client = BybitClient(api_key="key", secret_key="secret", category="spot")
    account_types = []

    def wallet_balance(*, account_type):
        account_types.append(account_type)
        return {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "BTC",
                                "walletBalance": "1.25",
                                "locked": "0.05",
                            }
                        ]
                    }
                ]
            }
        }

    client.get_wallet_balance = wallet_balance

    holding = get_spot_base_holding(
        client,
        symbol="BTC/USDT",
        strict=True,
        require_available=True,
    )

    assert account_types == ["UNIFIED"]
    assert holding["total"] == 1.25
    assert holding["available"] == 1.2


def test_spot_close_unsupported_client_is_not_treated_as_a_valid_snapshot():
    with pytest.raises(LiveTradingError, match="strategyRuntime.spotBalanceUnavailable"):
        clamp_spot_close_quantity(object(), symbol="BTC/USDT", requested_qty=1)


def test_spot_close_rejects_upward_quantity_normalization():
    client, _ = spot_client("binance", "0.25")
    client._normalize_quantity.side_effect = None
    client._normalize_quantity.return_value = (Decimal("1"), 0)
    with pytest.raises(LiveTradingError, match="strategyRuntime.spotCloseQuantityInvalid"):
        clamp_spot_close_quantity(client, symbol="BTC/USDT", requested_qty=1)


@pytest.mark.parametrize("failure", ["zero", "timeout"])
def test_direct_execution_does_not_submit_unavailable_spot_balance(failure):
    from app.services.live_trading.execution import place_order_from_signal

    client, fetch = spot_client("binance", "0")
    if failure == "timeout":
        fetch.side_effect = TimeoutError("balance timeout")
    with pytest.raises(LiveTradingError, match="strategyRuntime.spotBalance"):
        place_order_from_signal(client, signal_type="close_long", symbol="BTC/USDT", amount=1, market_type="spot")
    client.place_market_order.assert_not_called()


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("failure", ["zero", "timeout", "available"])
def test_worker_never_restores_blocked_spot_sell_quantity(monkeypatch, retry, failure):
    from app.services import pending_order_worker as module
    from app.services.live_trading import position_query

    client, fetch = spot_client("htx", "0.25" if failure == "available" else "0")
    if failure == "timeout":
        fetch.side_effect = TimeoutError("balance timeout")
    ctx = SimpleNamespace(
        strategy_id=1, signal_type="close_long", symbol="BTC/USDT", amount=1,
        cfg={"user_id": 1}, exchange_config={"exchange_id": "htx"},
        safe_exchange_config={}, exchange_id="htx", market_category="Crypto", market_type="spot",
    )
    monkeypatch.setattr(module, "prepare_live_order_context", lambda **kw: ctx)
    monkeypatch.setattr(module, "create_client", lambda *args, **kw: client)
    monkeypatch.setattr(module, "append_strategy_log", MagicMock())
    monkeypatch.setattr(module, "LiveOrderNotifier", MagicMock())
    monkeypatch.setattr(position_query, "query_exchange_position_size", lambda **kw: 1)
    resolve = MagicMock(side_effect=[(0, {}), (1, {})] if retry else [(1, {})])
    monkeypatch.setattr(module, "resolve_reduce_only_quantity", resolve)
    monkeypatch.setattr(module, "exchange_quantity_snapshot", lambda *args, **kw: (kw["requested"], {}))
    adapter = MagicMock()
    monkeypatch.setattr(module, "LiveOrderPhaseAdapter", adapter)
    execute = MagicMock(return_value=SimpleNamespace(raw={}, success=False, error="test_execution_finished"))
    monkeypatch.setattr(module, "MarketOrderExecutor", lambda *args, **kw: SimpleNamespace(execute=execute))
    monkeypatch.setenv("SPOT_CLOSE_SAFETY_RATIO", "1")
    worker = object.__new__(module.PendingOrderWorker)
    for name in ["_notifier", "_load_notification_config", "_load_strategy_name", "_register_pending_order_binding", "_sync_positions_best_effort", "_mark_failed", "_log_live_order_sizing", "_friendly_order_error"]:
        setattr(worker, name, MagicMock())

    worker._execute_live_order(order_id=1, order_row={"amount": 1, "user_id": 1}, payload={"amount": 1})

    assert resolve.call_count == (2 if retry else 1)
    if failure == "available":
        adapter.assert_called_once()
        assert execute.call_args.args[0].quantity == 0.25
        return
    adapter.assert_not_called()
    execute.assert_not_called()
    error = "strategyRuntime.spotBalanceInsufficient" if failure == "zero" else "strategyRuntime.spotBalanceUnavailable"
    worker._mark_failed.assert_called_once_with(order_id=1, error=error)


@pytest.mark.parametrize("failure", ["zero", "timeout", "available"])
def test_grid_exit_uses_only_confirmed_sellable_quantity(monkeypatch, failure):
    from app.services.grid.engine import GridEngine

    monkeypatch.setattr("app.services.grid.engine.GridRestingOrderRepository", MagicMock())
    monkeypatch.setattr("app.services.grid.engine.GridCellRepository", MagicMock())
    client, fetch = spot_client("htx", "0.25" if failure == "available" else "0")
    if failure == "timeout":
        fetch.side_effect = TimeoutError("balance timeout")
    engine = GridEngine(
        42, "BTC/USDT", {"market_type": "spot", "bot_params": {"gridCount": 5}},
        {"exchange_id": "htx", "credential_id": 7},
        create_client_fn=lambda: client, enqueue_market=lambda *args, **kw: False,
    )
    monkeypatch.setattr("app.services.live_trading.position_query.resolve_reduce_only_quantity", lambda **kw: (1, {"db_size": 1, "exchange_size": 1}))
    monkeypatch.setattr("app.services.grid.engine.append_strategy_log", MagicMock())
    monkeypatch.setattr(engine._orders, "list_open", lambda *args: [])
    monkeypatch.setenv("SPOT_CLOSE_SAFETY_RATIO", "1")

    quantity = engine._resolve_grid_exit_quantity(client, pos_side="long", requested_qty=1)
    assert quantity == (0.25 if failure == "available" else 0)

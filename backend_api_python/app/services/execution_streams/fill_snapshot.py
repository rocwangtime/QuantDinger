"""Convert private executions into comparable order snapshots before posting."""

import json
import os
import time
import threading
from collections import deque

from app.services.live_trading.fill_accounting import base_quantity, contract_multiplier
from app.services.live_trading.base import LiveTradingError
from app.services.live_trading.fill_evidence import require_execution

_snapshot_lock = threading.Lock()
_snapshots = {}
_request_times = {}


def prepare_event(event, client, exchange_config):
    result = dict(event)
    raw = event.get("raw_json") or event.get("raw") or {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    meta = raw.get("_qd_execution") or {}
    exchange = str(event.get("exchange_id") or "").lower()
    market = str(event.get("market_type") or "spot").lower()
    symbol = str(event.get("symbol") or "")
    multiplier = 1.0
    if market in {"swap", "future", "futures", "perp", "perpetual"} and exchange in {"okx", "gate", "htx"}:
        multiplier = contract_multiplier(client, exchange, symbol)
    for field in ("quantity", "cumulative_quantity"):
        result[field] = base_quantity(float(event.get(field) or 0), multiplier)
    result["cumulative_average_price"] = float(
        event.get("cumulative_average_price") or meta.get("cumulative_average_price") or 0
    )
    result["fees_cumulative"] = bool(event.get("fees_cumulative") or meta.get("fees_cumulative"))
    # Old durable events have no canonical metadata; recover documented fields.
    if not result["cumulative_average_price"]:
        if exchange == "okx":
            result["cumulative_average_price"] = float(raw.get("avgPx") or 0)
        elif exchange == "alpaca":
            result["cumulative_average_price"] = float((raw.get("order") or {}).get("filled_avg_price") or 0)
        elif exchange == "binance":
            item = raw.get("o") or raw
            cumulative = float(item.get("z") or 0)
            result["cumulative_average_price"] = (
                float(item.get("Z") or 0) / cumulative
                if market == "spot" and cumulative > 0
                else float(item.get("ap") or 0)
            )
    if exchange == "okx" and raw.get("fee") not in (None, ""):
        result["fees_cumulative"] = True
    result["_client"] = client
    result["_exchange_config"] = exchange_config
    return result


def complete_snapshot(event):
    """Use REST when a stream cannot establish an order's cumulative value."""
    from app.services.grid.exchange_orders import wait_grid_market_fill
    from app.services.pending_orders.fee_reconciliation import fee_breakdown_snapshot
    from app.services.execution_streams.events import normalize_status

    if float(event.get("cumulative_quantity") or 0) > 0 and float(event.get("cumulative_average_price") or 0) > 0:
        return event
    if (
        str(event.get("exchange_id") or "").lower() == "futu"
        and str(event.get("exchange_fill_id") or "")
        and float(event.get("quantity") or 0) > 0
        and float(event.get("price") or 0) > 0
    ):
        # TradeDealHandler supplies the executed quantity and price directly.
        # FutuClient has no generic wait_for_fill implementation to consult.
        return event
    owner = (event.get("exchange_id"), event.get("credential_id"), event.get("market_type"))
    key = (*owner, event.get("symbol"), event.get("exchange_order_id"))
    now = time.monotonic()
    interval = max(1.0, float(os.getenv("EXECUTION_FILL_SNAPSHOT_MIN_SEC", "2")))
    budget = max(1, int(os.getenv("EXECUTION_FILL_SNAPSHOT_MAX_PER_MIN", "30")))
    with _snapshot_lock:
        cached = _snapshots.get(key)
        if cached and now - cached[0] < interval:
            if cached[1]["cumulative_quantity"] + 1e-12 < float(event.get("cumulative_quantity") or 0):
                raise LiveTradingError("strategyRuntime.fillSnapshotNotReady")
            return dict(event, **cached[1])
        requests = _request_times.setdefault(owner, deque())
        while requests and now - requests[0] >= 60:
            requests.popleft()
        if len(requests) >= budget:
            raise LiveTradingError("strategyRuntime.fillSnapshotNotReady")
        requests.append(now)
    details = {}
    quantity, average = wait_grid_market_fill(
        event["_client"],
        symbol=event["symbol"],
        market_type=event["market_type"],
        exchange_config=event["_exchange_config"],
        exchange_order_id=str(event.get("exchange_order_id") or ""),
        client_order_id=str(event.get("client_order_id") or ""),
        max_wait_sec=0.0,
        details=details,
    )
    if quantity <= 0 or average <= 0 or quantity + 1e-12 < float(event.get("cumulative_quantity") or 0):
        raise LiveTradingError("strategyRuntime.fillSnapshotNotReady")
    order = details.get("order") or {}
    state = (
        details.get("status")
        or details.get("state")
        or order.get("orderStatus")
        or order.get("status")
        or order.get("state")
        or "partial"
    )
    snapshot = dict(
        cumulative_quantity=quantity,
        cumulative_average_price=average,
        order_status=normalize_status(state),
        is_cumulative=True,
        fees_cumulative=True,
        _snapshot_fees=fee_breakdown_snapshot(details),
        fee_status=str(details.get("fee_status") or ("actual" if fee_breakdown_snapshot(details) else "pending")),
    )
    with _snapshot_lock:
        if len(_snapshots) >= 1024:
            _snapshots.clear()
        _snapshots[key] = (now, snapshot)
    return dict(event, **snapshot)


def combine_pending_snapshot(event, pending):
    """Translate one exchange leg into the pending order's aggregate scope."""
    response = pending.get("exchange_response_json") or {}
    if isinstance(response, str):
        response = json.loads(response)
    executor = (response.get("phases") or {}).get("executor") or {}
    legs = [executor.get(name) for name in ("limit_summary", "market_summary")]
    legs = [leg for leg in legs if isinstance(leg, dict) and leg.get("exchange_order_id")]
    if len(legs) < 2:
        return event, response
    matching = next((leg for leg in legs if str(leg["exchange_order_id"]) == str(event.get("exchange_order_id"))), None)
    if matching is None:
        raise LiveTradingError("strategyRuntime.inconsistentFillSnapshot")
    if float(event.get("cumulative_quantity") or 0) < float(matching.get("filled_qty") or 0):
        return dict(event, cumulative_quantity=0, quantity=0, fees_cumulative=False), response
    matching.update(filled_qty=event["cumulative_quantity"], avg_price=event["cumulative_average_price"])
    if "_snapshot_fees" in event and event.get("fee_status") != "pending":
        matching["fees_by_ccy"] = event["_snapshot_fees"]
    for leg in legs:
        if float(leg.get("filled_qty") or 0) > 0:
            require_execution(leg["filled_qty"], leg.get("avg_price"))
    quantity = sum(float(leg.get("filled_qty") or 0) for leg in legs)
    value = sum(float(leg.get("filled_qty") or 0) * float(leg.get("avg_price") or 0) for leg in legs)
    other_fees = {}
    for leg in legs:
        if leg is not matching:
            for currency, amount in (leg.get("fees_by_ccy") or {}).items():
                other_fees[currency] = other_fees.get(currency, 0.0) + float(amount)
    status = event.get("order_status")
    if matching is not legs[-1] and status in {"filled", "cancelled"}:
        status = "partial"
    return dict(
        event,
        cumulative_quantity=quantity,
        cumulative_average_price=value / quantity if quantity else 0,
        order_status=status,
        _other_fees=other_fees,
    ), response

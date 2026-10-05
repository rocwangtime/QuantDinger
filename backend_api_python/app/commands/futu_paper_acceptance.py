"""Read-only reconciliation gate for bounded Futu US/HK paper roundtrips.

Run only after the operator has finished the test strategy and inspected both
orders in Futu. This command never submits or cancels an order. It obtains the
SIMULATE account from the strategy's saved, encrypted credential; no account
password, token, or account ID is accepted on the command line.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal, InvalidOperation
from typing import Any


class AcceptanceError(RuntimeError):
    """A bounded, non-secret mismatch code suitable for operator output."""


def _number(value: Any) -> Decimal:
    try:
        number = Decimal(str(value or 0))
        if not number.is_finite():
            raise InvalidOperation
        return number
    except (InvalidOperation, ValueError):
        raise AcceptanceError("INVALID_QUANTITY_OR_PRICE") from None


def _symbol(value: Any) -> str:
    raw = str(value or "").strip().upper()
    raw = raw.split(":")[-1].removeprefix("US.").removeprefix("HK.")
    raw = raw.removesuffix(".HK")
    return raw.zfill(5) if raw.isdigit() else raw


def _same_money(left: Any, right: Any) -> bool:
    # Futu order averages can be rounded to cents while the local ledger
    # stores eight decimal places. Anything beyond one cent needs review.
    return abs(_number(left) - _number(right)) <= Decimal("0.01")


def verify_roundtrip(
    *,
    pending_orders: list[dict],
    trades: list[dict],
    broker_orders: list[dict],
    platform_positions: list[dict],
    broker_positions: list[dict],
    expected_symbol: str = "SPY",
    expected_quantity: int = 1,
) -> dict:
    """Prove exactly one bounded buy and sell, with a flat final account."""
    symbol = _symbol(expected_symbol)
    quantity_expected = _number(expected_quantity)
    if not symbol or quantity_expected <= 0:
        raise AcceptanceError("INVALID_ACCEPTANCE_TARGET")
    if len(pending_orders) != 2:
        raise AcceptanceError("EXPECTED_EXACTLY_TWO_PLATFORM_ORDERS")
    by_signal = {str(row.get("signal_type") or ""): row for row in pending_orders}
    if set(by_signal) != {"open_long", "close_long"}:
        raise AcceptanceError("EXPECTED_ONE_BUY_AND_ONE_SELL")
    if any(_symbol(row.get("symbol")) != symbol for row in pending_orders):
        raise AcceptanceError("UNEXPECTED_PLATFORM_SYMBOL")

    broker_by_id = {str(row.get("id") or row.get("orderId") or ""): row for row in broker_orders}
    seen_trade_ids: set[int] = set()
    for signal, side in (("open_long", "buy"), ("close_long", "sell")):
        order = by_signal[signal]
        order_id = str(order.get("exchange_order_id") or "").strip()
        if not order_id or order_id not in broker_by_id:
            raise AcceptanceError("BROKER_ORDER_MISSING")
        broker = broker_by_id[order_id]
        if str(order.get("status") or "").lower() != "filled":
            raise AcceptanceError("PLATFORM_ORDER_NOT_FILLED")
        if str(broker.get("status") or "").lower() != "filled":
            raise AcceptanceError("BROKER_ORDER_NOT_FILLED")
        if str(broker.get("side") or "").lower() != side:
            raise AcceptanceError("BROKER_SIDE_MISMATCH")
        if _symbol(broker.get("symbol")) != symbol:
            raise AcceptanceError("BROKER_SYMBOL_MISMATCH")
        if any(_number(value) != quantity_expected for value in (
            order.get("amount"), order.get("filled"),
            broker.get("quantity"), broker.get("filled"),
        )):
            raise AcceptanceError("ORDER_QUANTITY_MISMATCH")
        broker_average = _number(broker.get("avgFillPrice"))
        if broker_average <= 0 or not _same_money(order.get("avg_price"), broker_average):
            raise AcceptanceError("ORDER_AVERAGE_PRICE_MISMATCH")

        own_trades = [row for row in trades if int(row.get("pending_order_id") or 0) == int(order["id"])]
        if not own_trades or any(str(row.get("type") or "") != signal for row in own_trades):
            raise AcceptanceError("PLATFORM_FILL_MISSING_OR_WRONG_SIDE")
        if any(_number(row.get("amount")) <= 0 or _number(row.get("price")) <= 0 for row in own_trades):
            raise AcceptanceError("PLATFORM_FILL_INVALID")
        if any(str(row.get("exchange_order_id") or "") != order_id for row in own_trades):
            raise AcceptanceError("PLATFORM_FILL_ORDER_ID_MISMATCH")
        quantity = sum((_number(row.get("amount")) for row in own_trades), Decimal(0))
        if quantity != quantity_expected:
            raise AcceptanceError("PLATFORM_FILL_QUANTITY_MISMATCH")
        weighted = sum(
            (_number(row.get("amount")) * _number(row.get("price")) for row in own_trades),
            Decimal(0),
        ) / quantity
        if not _same_money(weighted, broker_average):
            raise AcceptanceError("PLATFORM_FILL_PRICE_MISMATCH")
        seen_trade_ids.update(int(row["id"]) for row in own_trades)

    if len(seen_trade_ids) != len(trades):
        raise AcceptanceError("UNMATCHED_PLATFORM_TRADES")
    for row in platform_positions:
        if _symbol(row.get("symbol")) == symbol and _number(row.get("size")) != 0:
            raise AcceptanceError("PLATFORM_POSITION_NOT_FLAT")
    for row in broker_positions:
        if _symbol(row.get("symbol")) == symbol and _number(row.get("quantity")) != 0:
            raise AcceptanceError("BROKER_POSITION_NOT_FLAT")
    final_key = "final_spy_quantity" if symbol == "SPY" else "final_hk_quantity"
    return {"passed": True, "orders": 2, "fill_rows": len(trades), final_key: 0}


def _database_rows(strategy_id: int) -> tuple[dict, list[dict], list[dict], list[dict]]:
    from app.utils.db import get_db_connection

    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """SELECT id, user_id, symbol, market_category, execution_mode, exchange_config
                   FROM qd_strategies_trading WHERE id = %s""",
                (strategy_id,),
            )
            strategy = dict(cur.fetchone() or {})
            cur.execute(
                "SELECT id FROM strategy_runs WHERE strategy_id = %s ORDER BY id DESC LIMIT 1",
                (strategy_id,),
            )
            run_id = int((cur.fetchone() or {}).get("id") or 0)
            cur.execute(
                """SELECT p.id, p.symbol, p.signal_type, p.amount, p.filled,
                          p.avg_price, p.status, p.exchange_id, p.exchange_order_id, p.credential_id
                   FROM pending_orders p
                   WHERE p.strategy_id = %s AND p.strategy_run_id = %s
                   ORDER BY p.id""",
                (strategy_id, run_id),
            )
            orders = [dict(row) for row in (cur.fetchall() or [])]
            cur.execute(
                """SELECT id, pending_order_id, exchange_order_id, type, amount, price
                   FROM qd_strategy_trades
                   WHERE strategy_id = %s AND pending_order_id IN
                       (SELECT id FROM pending_orders WHERE strategy_id = %s AND strategy_run_id = %s)
                   ORDER BY id""",
                (strategy_id, strategy_id, run_id),
            )
            trades = [dict(row) for row in (cur.fetchall() or [])]
            cur.execute(
                """SELECT symbol, side, size FROM qd_strategy_positions
                   WHERE strategy_id = %s""",
                (strategy_id,),
            )
            positions = [dict(row) for row in (cur.fetchall() or [])]
        finally:
            cur.close()
    return strategy, orders, trades, positions


def check_strategy(strategy_id: int) -> dict:
    """Read the saved paper account and compare live broker and platform data."""
    from app.services.exchange_execution import resolve_exchange_config
    from app.services.futu_trading import FutuClient
    from app.services.futu_trading.config import config_from_exchange_config

    strategy, orders, trades, positions = _database_rows(strategy_id)
    if not strategy:
        raise AcceptanceError("STRATEGY_NOT_FOUND")
    target = {
        ("USStock", "SPY"): ("US", 1),
        ("HKStock", "00700"): ("HK", 100),
    }.get((strategy.get("market_category"), _symbol(strategy.get("symbol"))))
    if strategy.get("execution_mode") != "live" or target is None:
        raise AcceptanceError("STRATEGY_NOT_BOUNDED_FUTU_ROUNDTRIP")
    expected_market, expected_quantity = target
    if len(orders) != 2:
        raise AcceptanceError("EXPECTED_EXACTLY_TWO_PLATFORM_ORDERS")
    if any(str(row.get("exchange_id") or "").lower() != "futu" for row in orders):
        raise AcceptanceError("PLATFORM_ORDER_NOT_FUTU")
    credential_ids = {int(row.get("credential_id") or 0) for row in orders}
    if len(credential_ids) != 1 or min(credential_ids, default=0) <= 0:
        raise AcceptanceError("ORDER_CREDENTIAL_MISMATCH")
    exchange_config = resolve_exchange_config(
        strategy.get("exchange_config") or {}, user_id=int(strategy["user_id"])
    )
    if str(exchange_config.get("exchange_id") or "").lower() != "futu":
        raise AcceptanceError("STRATEGY_NOT_FUTU")
    if int(exchange_config.get("credential_id") or 0) not in credential_ids:
        raise AcceptanceError("STRATEGY_CREDENTIAL_MISMATCH")
    config = config_from_exchange_config(exchange_config)
    if config.acc_id <= 0 or config.trade_market != expected_market:
        raise AcceptanceError("SIMULATE_ACCOUNT_NOT_SELECTED")

    client = FutuClient(config)
    if not client.connect(need_quote=False):
        raise AcceptanceError("OPEND_NOT_CONNECTED")
    try:
        result = verify_roundtrip(
            pending_orders=orders,
            trades=trades,
            broker_orders=client.get_recent_orders(limit=500),
            platform_positions=positions,
            broker_positions=client.get_positions(),
            expected_symbol=_symbol(strategy.get("symbol")),
            expected_quantity=expected_quantity,
        )
    finally:
        client.disconnect()
    return {"strategy_id": strategy_id, **result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy-id", type=int, required=True)
    args = parser.parse_args()
    try:
        result = check_strategy(args.strategy_id)
    except AcceptanceError as exc:
        print(f"futu_paper_acceptance=FAIL code={exc}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        # Broker and DB exceptions may contain sensitive connection details.
        print("futu_paper_acceptance=FAIL code=CHECK_UNAVAILABLE", file=sys.stderr)
        raise SystemExit(1) from None
    print("futu_paper_acceptance=PASS " + " ".join(f"{k}={v}" for k, v in result.items()))


if __name__ == "__main__":
    main()

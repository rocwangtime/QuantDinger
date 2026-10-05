"""The final paper-trade gate must compare broker data, not just local status."""

from copy import deepcopy

import pytest

from app.commands.futu_paper_acceptance import AcceptanceError, _database_rows, verify_roundtrip


@pytest.fixture
def evidence():
    return {
        "pending_orders": [
            {"id": 11, "symbol": "USStock:SPY", "signal_type": "open_long", "amount": 1,
             "filled": 1, "avg_price": 600, "status": "filled", "exchange_order_id": "B-11"},
            {"id": 12, "symbol": "USStock:SPY", "signal_type": "close_long", "amount": 1,
             "filled": 1, "avg_price": 601, "status": "filled", "exchange_order_id": "S-12"},
        ],
        "trades": [
            {"id": 21, "pending_order_id": 11, "exchange_order_id": "B-11",
             "type": "open_long", "amount": 1, "price": 600},
            {"id": 22, "pending_order_id": 12, "exchange_order_id": "S-12",
             "type": "close_long", "amount": 1, "price": 601},
        ],
        "broker_orders": [
            {"id": "B-11", "symbol": "SPY", "side": "buy", "quantity": 1,
             "filled": 1, "avgFillPrice": 600, "status": "filled"},
            {"id": "S-12", "symbol": "SPY", "side": "sell", "quantity": 1,
             "filled": 1, "avgFillPrice": 601, "status": "filled"},
        ],
        "platform_positions": [],
        "broker_positions": [],
    }


def test_completed_one_share_roundtrip_matches_broker(evidence):
    assert verify_roundtrip(**evidence) == {
        "passed": True, "orders": 2, "fill_rows": 2, "final_spy_quantity": 0,
    }


def test_completed_hk_board_lot_roundtrip_matches_broker(evidence):
    for group in ("pending_orders", "broker_orders"):
        for row in evidence[group]:
            row["symbol"] = "HK.00700" if group == "broker_orders" else "HKStock:00700"
            row["filled"] = 100
            if group == "broker_orders":
                row["quantity"] = 100
            else:
                row["amount"] = 100
    for row in evidence["trades"]:
        row["amount"] = 100
    assert verify_roundtrip(**evidence, expected_symbol="00700.HK", expected_quantity=100) == {
        "passed": True, "orders": 2, "fill_rows": 2, "final_hk_quantity": 0,
    }


@pytest.mark.parametrize("path,key,value,code", [
    ("pending_orders", "filled", 0, "ORDER_QUANTITY_MISMATCH"),
    ("pending_orders", "avg_price", 599, "ORDER_AVERAGE_PRICE_MISMATCH"),
    ("trades", "amount", 2, "PLATFORM_FILL_QUANTITY_MISMATCH"),
    ("trades", "price", 599, "PLATFORM_FILL_PRICE_MISMATCH"),
    ("broker_orders", "status", "submitted", "BROKER_ORDER_NOT_FILLED"),
    ("broker_orders", "side", "sell", "BROKER_SIDE_MISMATCH"),
    ("broker_orders", "filled", 0, "ORDER_QUANTITY_MISMATCH"),
])
def test_mismatch_fails_closed(evidence, path, key, value, code):
    changed = deepcopy(evidence)
    changed[path][0][key] = value
    with pytest.raises(AcceptanceError, match=code):
        verify_roundtrip(**changed)


def test_extra_duplicate_trade_is_not_accepted(evidence):
    evidence["trades"].append({
        "id": 23, "pending_order_id": 11, "exchange_order_id": "B-11",
        "type": "open_long", "amount": 1, "price": 600,
    })
    with pytest.raises(AcceptanceError, match="PLATFORM_FILL_QUANTITY_MISMATCH"):
        verify_roundtrip(**evidence)


def test_extra_unmatched_trade_is_not_accepted(evidence):
    evidence["trades"].append({
        "id": 23, "pending_order_id": 999, "exchange_order_id": "unknown",
        "type": "open_long", "amount": 1, "price": 600,
    })
    with pytest.raises(AcceptanceError, match="UNMATCHED_PLATFORM_TRADES"):
        verify_roundtrip(**evidence)


def test_nonflat_broker_position_is_not_accepted(evidence):
    evidence["broker_positions"].append({"symbol": "SPY", "quantity": 1})
    with pytest.raises(AcceptanceError, match="BROKER_POSITION_NOT_FLAT"):
        verify_roundtrip(**evidence)


def test_nonflat_platform_position_is_not_accepted(evidence):
    evidence["platform_positions"].append({"symbol": "SPY", "size": 1})
    with pytest.raises(AcceptanceError, match="PLATFORM_POSITION_NOT_FLAT"):
        verify_roundtrip(**evidence)


def test_database_acceptance_scopes_orders_and_fills_to_latest_run(monkeypatch):
    from app.utils import db as db_module

    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))

        def fetchone(self):
            return {"id": 7} if len(self.calls) == 2 else {"id": 3}

        def fetchall(self):
            return []

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cur = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return self.cur

    connection = Connection()
    monkeypatch.setattr(db_module, "get_db_connection", lambda: connection)

    _database_rows(3)

    assert connection.cur.calls[2][1] == (3, 7)
    assert connection.cur.calls[3][1] == (3, 3, 7)

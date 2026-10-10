"""Durable sequential coordination in an isolated virtual strategy account.

No broker calls. Each ledger fill and group transition commit together. The
ordinary pending-order dispatcher never claims group_waiting orders.
"""

import json
import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone

from app.services.portfolio.risk import finite_number
from app.services.strategy_evolution.bundles import content_hash
from app.services.strategy_v2.live_execution import LiveOrderRequest, StrategyV2OrderGateway
from app.services.virtual_trading import canonical_symbol, execute_virtual_signal_order
from app.utils.db import get_db_connection, get_db_transaction
from app.utils.logger import get_logger

logger = get_logger(__name__)
ACTIVE = {"executing", "unwinding"}


def obj(value):
    return json.loads(value) if isinstance(value, str) else value


def validate_group(payload):
    if payload.get("executionMode", "signal") != "signal":
        raise ValueError("orderGroup.virtualOnly")
    legs = payload.get("legs")
    if not isinstance(legs, list) or not 2 <= len(legs) <= 8:
        raise ValueError("orderGroup.twoToEightLegsRequired")
    clean, identities = [], set()
    for leg in legs:
        if not isinstance(leg, dict):
            raise ValueError("orderGroup.invalidLeg")
        symbol = str(leg.get("symbol") or "").strip()
        action = leg.get("action")
        quantity, price = finite_number(leg.get("quantity")), finite_number(leg.get("referencePrice"))
        if not symbol or len(symbol) > 80 or action not in {"open_long", "open_short"} or quantity <= 0 or price <= 0:
            raise ValueError("orderGroup.invalidLeg")
        identity = (canonical_symbol(symbol), action)
        if identity in identities:
            raise ValueError("orderGroup.duplicateLeg")
        identities.add(identity)
        clean.append({"symbol": symbol, "action": action, "quantity": quantity, "referencePrice": price})
    timeout = int(payload.get("timeoutSeconds", 300))
    budget = finite_number(payload.get("maxGrossNotional"))
    if not 5 <= timeout <= 3600 or budget <= 0 or sum(x["quantity"] * x["referencePrice"] for x in clean) > budget:
        raise ValueError("orderGroup.invalidBudgetOrDeadline")
    return {"strategyId": int(payload["strategyId"]), "strategyRunId": int(payload.get("strategyRunId") or 0),
            "executionMode": "signal", "legs": clean, "timeoutSeconds": timeout, "maxGrossNotional": budget}


def _read(cur, group_id, user_id, *, lock=False):
    cur.execute("SELECT * FROM qd_order_groups WHERE group_id=%s AND user_id=%s" + (" FOR UPDATE" if lock else ""),
                (group_id, user_id))
    row = cur.fetchone()
    if not row:
        raise ValueError("orderGroup.notFound")
    return {**row, "config": obj(row["config"]), "state": obj(row["state"])}


def _save(cur, group):
    cur.execute("UPDATE qd_order_groups SET state=%s::jsonb,updated_at=NOW() WHERE group_id=%s",
                (json.dumps(group["state"], allow_nan=False), group["group_id"]))


def _strategy(cur, group):
    config = group["config"]
    cur.execute("SELECT * FROM qd_strategies_trading WHERE id=%s AND user_id=%s FOR UPDATE",
                (config["strategyId"], group["user_id"]))
    row = cur.fetchone() or {}
    if row.get("execution_mode") != "signal" or row.get("status") != "stopped":
        raise ValueError("orderGroup.stoppedVirtualStrategyRequired")
    cur.execute("SELECT strategy_id FROM qd_strategy_runtime_leases WHERE strategy_id=%s AND lease_expires_at>NOW() LIMIT 1",
                (config["strategyId"],))
    if cur.fetchone():
        raise ValueError("orderGroup.activeExecutorRejected")
    cur.execute("SELECT id FROM qd_strategy_commands WHERE strategy_id=%s AND command_type='start' "
                "AND status IN ('pending','processing') LIMIT 1", (config["strategyId"],))
    if cur.fetchone():
        raise ValueError("orderGroup.startPending")
    return row


def _queue(cur, group, leg, index, *, unwind=False):
    config = group["config"]
    pending_id = StrategyV2OrderGateway().submit(LiveOrderRequest(
        strategy_id=config["strategyId"], strategy_run_id=config["strategyRunId"], user_id=group["user_id"],
        symbol=leg["symbol"], action=leg["action"], quantity=leg["quantity"], reference_price=leg["referencePrice"],
        signal_timestamp=int(time.time()), market_type=config["marketType"], execution_mode="signal",
        client_order_id=f"group:{group['group_id']}:{'unwind' if unwind else 'entry'}:{index}",
        order_group_id=group["group_id"],
        reason=f"virtual_order_group:{group['group_id']}",
    ))
    if not pending_id:
        raise RuntimeError("orderGroup.intentCreationFailed")
    cur.execute("UPDATE pending_orders SET status='group_waiting',payload_json="
                "(payload_json::jsonb || %s::jsonb)::text WHERE id=%s",
                (json.dumps({"order_group_id": group["group_id"]}), pending_id))
    return {**leg, "pendingId": pending_id, "status": "group_waiting", "filled": 0.0}


def create_group(*, user_id, idempotency_key, payload):
    key = str(idempotency_key or "").strip()
    if not key or len(key) > 120:
        raise ValueError("orderGroup.idempotencyKeyRequired")
    config = validate_group(payload)
    digest = content_hash(config)
    with get_db_transaction() as db:
        with closing(db.cursor()) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"order-group:{user_id}:{key}",))
            cur.execute("SELECT group_id,request_hash FROM qd_order_groups WHERE user_id=%s AND idempotency_key=%s",
                        (user_id, key))
            existing = cur.fetchone()
            if existing:
                if existing["request_hash"] != digest:
                    raise ValueError("orderGroup.idempotencyConflict")
                return _read(cur, existing["group_id"], user_id)
            group = {"group_id": uuid.uuid4().hex, "user_id": int(user_id), "config": config,
                     "state": {"status": "executing", "legs": [], "compensations": [], "residuals": [],
                               "deadline": (datetime.now(timezone.utc) + timedelta(seconds=config["timeoutSeconds"])).isoformat()}}
            strategy = _strategy(cur, group)
            config["marketType"] = str(strategy.get("market_type") or "spot")
            if config["marketType"] == "spot" and any(x["action"] == "open_short" for x in config["legs"]):
                raise ValueError("orderGroup.spotShortUnsupported")
            if config["maxGrossNotional"] > finite_number(strategy.get("initial_capital")):
                raise ValueError("orderGroup.budgetExceedsVirtualCapital")
            from app.services.live_trading.records import strategy_allowed_symbols
            allowed = strategy_allowed_symbols({**strategy, "trading_config": obj(strategy["trading_config"]) or {}})
            if any(canonical_symbol(x["symbol"]) not in allowed for x in config["legs"]):
                raise ValueError("orderGroup.symbolOutsideStrategy")
            if config["strategyRunId"]:
                cur.execute("SELECT id FROM strategy_runs WHERE id=%s AND strategy_id=%s AND user_id=%s",
                            (config["strategyRunId"], config["strategyId"], user_id))
                if not cur.fetchone():
                    raise ValueError("orderGroup.runNotFound")
            else:
                runtime = obj(strategy["trading_config"]) or {}
                cur.execute("INSERT INTO strategy_runs(user_id,strategy_id,source_version_id,code_hash,parameter_snapshot_json," 
                            "market_type,runtime_status) VALUES(%s,%s,%s,%s,%s::jsonb,%s,'order_group') RETURNING id",
                            (user_id, config["strategyId"], str(strategy.get("source_version_id") or ""),
                             str((runtime.get("strategy_manifest") or {}).get("codeHash") or ""),
                             json.dumps({"order_group": config}), config["marketType"]))
                config["strategyRunId"] = int(cur.fetchone()["id"])
            cur.execute("SELECT id FROM qd_strategy_virtual_positions WHERE strategy_id=%s AND size>0 LIMIT 1",
                        (config["strategyId"],))
            if cur.fetchone():
                raise ValueError("orderGroup.emptyVirtualAccountRequired")
            cur.execute("SELECT id FROM pending_orders WHERE strategy_id=%s AND status IN "
                        "('pending','processing','sent','syncing','reconciling','group_waiting') LIMIT 1",
                        (config["strategyId"],))
            if cur.fetchone():
                raise ValueError("orderGroup.inflightOrdersRejected")
            cur.execute("SELECT cash_balance FROM qd_strategy_virtual_accounts WHERE strategy_id=%s FOR UPDATE",
                        (config["strategyId"],))
            account = cur.fetchone()
            available = float(account["cash_balance"]) if account else float(strategy["initial_capital"])
            from app.services.virtual_execution_costs import VIRTUAL_COMMISSION_RATE, VIRTUAL_SLIPPAGE_RATE
            required = sum(x["quantity"] * x["referencePrice"] for x in config["legs"])
            if required * (1 + VIRTUAL_SLIPPAGE_RATE) * (1 + VIRTUAL_COMMISSION_RATE) > available:
                raise ValueError("orderGroup.insufficientVirtualCash")
            cur.execute("INSERT INTO qd_order_groups(group_id,user_id,idempotency_key,request_hash,config,state) "
                        "VALUES(%s,%s,%s,%s,%s::jsonb,%s::jsonb) RETURNING group_id",
                        (group["group_id"], user_id, key, digest, json.dumps(config), json.dumps(group["state"])))
            group["state"]["legs"] = [_queue(cur, group, leg, index) for index, leg in enumerate(config["legs"])]
            _save(cur, group)
            return group


def get_group(*, user_id, group_id):
    with get_db_connection() as db:
        with closing(db.cursor()) as cur:
            return _read(cur, group_id, user_id)


def _refresh(cur, group):
    state = group["state"]
    for leg in state["legs"] + state["compensations"]:
        cur.execute("SELECT status,filled,avg_price FROM pending_orders WHERE id=%s FOR UPDATE", (leg["pendingId"],))
        row = cur.fetchone() or {}
        # Virtual fills are the execution truth, even after an interrupted dispatcher.
        cur.execute("SELECT status,fill_qty,fill_price FROM qd_strategy_virtual_orders WHERE pending_order_id=%s",
                    (leg["pendingId"],))
        virtual = cur.fetchone() or {}
        leg.update(status=virtual.get("status") or row.get("status", "missing"),
                   filled=float(virtual.get("fill_qty", row.get("filled")) or 0),
                   fillPrice=float(virtual.get("fill_price", row.get("avg_price")) or 0))
    residuals = []
    for index, leg in enumerate(state["legs"]):
        reduced = sum(x["filled"] for x in state["compensations"] if x["entryIndex"] == index)
        quantity = max(0.0, leg["filled"] - reduced)
        if quantity > 1e-9:
            residuals.append({"entryIndex": index, "symbol": leg["symbol"], "side": leg["action"].split("_")[-1],
                              "quantity": quantity, "entryFillPrice": leg.get("fillPrice", 0)})
    state["residuals"] = residuals
    state["grossResidualNotional"] = sum(x["quantity"] * x["entryFillPrice"] for x in residuals)
    state["netResidualNotional"] = sum(x["quantity"] * x["entryFillPrice"] * (-1 if x["side"] == "short" else 1)
                                        for x in residuals)


def _halt(cur, group, reason):
    _refresh(cur, group)
    for leg in group["state"]["legs"] + group["state"]["compensations"]:
        cur.execute("UPDATE pending_orders SET status='cancelled',last_error=%s,updated_at=NOW() "
                    "WHERE id=%s AND status='group_waiting' RETURNING order_intent_id", (reason, leg["pendingId"]))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE strategy_order_intents SET status='cancelled',updated_at=NOW() WHERE id=%s",
                        (row["order_intent_id"],))
    _refresh(cur, group)
    group["state"].update(status="needs_review" if group["state"]["residuals"] else "cancelled", reason=reason)
    _save(cur, group)


def cancel_group(*, user_id, group_id):
    with get_db_transaction() as db:
        with closing(db.cursor()) as cur:
            group = _read(cur, group_id, user_id, lock=True)
            if group["state"]["status"] not in {"cancelled", "unwound", "resolved"}:
                _halt(cur, group, "userCancelled")
            return group


def unwind_group(*, user_id, group_id, reference_prices):
    """Explicit compensation at caller supplied marks; costs remain in the ledger."""
    if not isinstance(reference_prices, dict):
        raise ValueError("orderGroup.unwindReferencePriceRequired")
    with get_db_transaction() as db:
        with closing(db.cursor()) as cur:
            group = _read(cur, group_id, user_id, lock=True)
            if group["state"]["status"] in {"unwinding", "unwound", "resolved"}:
                return group
            _strategy(cur, group)
            _halt(cur, group, "unwindRequested")
            ids = [x["pendingId"] for x in group["state"]["legs"] + group["state"]["compensations"]]
            cur.execute("SELECT id FROM qd_strategy_virtual_trades WHERE strategy_id=%s "
                        "AND pending_order_id != ALL(%s) AND created_at >= "
                        "(SELECT created_at FROM qd_order_groups WHERE group_id=%s) LIMIT 1",
                        (group["config"]["strategyId"], ids, group_id))
            if cur.fetchone():
                raise ValueError("orderGroup.foreignFillsRequireManualReview")
            for residual in group["state"]["residuals"]:
                price = finite_number(reference_prices.get(residual["symbol"]))
                if price <= 0:
                    raise ValueError("orderGroup.unwindReferencePriceRequired")
                index = len(group["state"]["compensations"])
                compensation = _queue(cur, group, {"symbol": residual["symbol"], "action": f"reduce_{residual['side']}",
                                                 "quantity": residual["quantity"], "referencePrice": price}, index, unwind=True)
                compensation["entryIndex"] = residual["entryIndex"]
                group["state"]["compensations"].append(compensation)
            group["state"].update(status="unwinding" if group["state"]["residuals"] else "unwound",
                                  deadline=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
            _save(cur, group)
            return group


def resolve_group(*, user_id, group_id, reason):
    """Acknowledge external closure only after the isolated account is flat."""
    reason = str(reason or "").strip()
    if not reason or len(reason) > 500:
        raise ValueError("orderGroup.resolutionReasonRequired")
    with get_db_transaction() as db:
        with closing(db.cursor()) as cur:
            group = _read(cur, group_id, user_id, lock=True)
            if group["state"]["status"] == "resolved":
                return group
            _strategy(cur, group)
            _halt(cur, group, "manualResolutionRequested")
            cur.execute("SELECT id FROM qd_strategy_virtual_positions WHERE strategy_id=%s AND size>0 LIMIT 1",
                        (group["config"]["strategyId"],))
            if cur.fetchone():
                raise ValueError("orderGroup.residualPositionRemains")
            cur.execute("SELECT id FROM pending_orders WHERE strategy_id=%s AND status IN "
                        "('pending','processing','sent','syncing','reconciling','group_waiting') LIMIT 1",
                        (group["config"]["strategyId"],))
            if cur.fetchone():
                raise ValueError("orderGroup.inflightOrdersRejected")
            group["state"].update(status="resolved", resolution={"reason": reason, "flatAccountVerified": True,
                                   "at": datetime.now(timezone.utc).isoformat()})
            group["state"]["exposureBeforeResolution"] = group["state"]["residuals"]
            group["state"].update(residuals=[], grossResidualNotional=0.0, netResidualNotional=0.0)
            _save(cur, group)
            return group


def advance_group(*, user_id, group_id):
    """One leg per tick, with durable failure handling outside the rolled-back fill."""
    try:
        with get_db_transaction() as db:
            with closing(db.cursor()) as cur:
                group = _read(cur, group_id, user_id, lock=True)
                state = group["state"]
                if state["status"] not in ACTIVE:
                    return group
                _strategy(cur, group)
                _refresh(cur, group)
                if datetime.fromisoformat(state["deadline"]) <= datetime.now(timezone.utc):
                    _halt(cur, group, "deadlineExceeded")
                    return group
                work = ([x for x in state["compensations"] if x["status"] != "cancelled"]
                        if state["status"] == "unwinding" else state["legs"])
                if any(x["status"] == "filled" and abs(x["filled"] - x["quantity"]) > 1e-8 for x in work):
                    _halt(cur, group, "legIncomplete")
                    return group
                if any(x["status"] not in {"filled", "group_waiting"} for x in work):
                    _halt(cur, group, "legFailedOrUnknown")
                    return group
                leg = next((x for x in work if x["status"] == "group_waiting"), None)
                if leg:
                    cur.execute("SELECT * FROM pending_orders WHERE id=%s FOR UPDATE", (leg["pendingId"],))
                    row = cur.fetchone()
                    fill = execute_virtual_signal_order(row, obj(row["payload_json"]))
                    _refresh(cur, group)
                    if fill["status"] != "filled" or abs(fill["fill_quantity"] - leg["quantity"]) > 1e-8:
                        _halt(cur, group, "legIncomplete")
                        return group
                if all(x["status"] == "filled" for x in work):
                    if state["status"] == "unwinding" and state["residuals"]:
                        _halt(cur, group, "unwindResidualRequiresReview")
                        return group
                    state["status"] = "unwound" if state["status"] == "unwinding" else "completed"
                _save(cur, group)
                return group
    except Exception as exc:
        logger.warning("Virtual order group halted: %s", group_id, exc_info=True)
        with get_db_transaction() as db:
            with closing(db.cursor()) as cur:
                group = _read(cur, group_id, user_id, lock=True)
                if group["state"]["status"] in ACTIVE:
                    _halt(cur, group, f"executionFailed:{type(exc).__name__}")
                return group


def reconcile_order_groups():
    with get_db_connection() as db:
        with closing(db.cursor()) as cur:
            cur.execute("SELECT group_id,user_id FROM qd_order_groups WHERE state->>'status' "
                        "IN ('executing','unwinding') ORDER BY updated_at LIMIT 20")
            rows = cur.fetchall() or []
    for row in rows:
        advance_group(user_id=row["user_id"], group_id=row["group_id"])

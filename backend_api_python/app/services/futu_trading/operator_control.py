"""Operator-facing Futu paper arm and stop orchestration.

Only order identities recorded by this platform may be cancelled. A failed
broker query or unconfirmed cancel leaves the durable gate disabled and the
state `unconfirmed`; it must never be reported as a successful stop.
"""

from __future__ import annotations

import json
import time
from typing import Any

from app.services.futu_trading.config import FutuConfig, config_from_exchange_config
from app.services.futu_trading.operator_gate import (
    begin_pause, finish_pause, wait_submission_barrier,
)
from app.utils.credential_crypto import decrypt_credential_blob
from app.utils.db import get_db_connection


def saved_account_credential(
    user_id: int, acc_id: int, *, connected: FutuConfig | None = None,
    credential_id: int = 0,
) -> tuple[int, dict]:
    """Use the newest matching saved SIMULATE/US credential unless pinned.

    Older duplicate rows remain available for existing strategies and for
    stopping an account that was armed against one of those rows.
    """
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """SELECT id, encrypted_config FROM qd_exchange_credentials
               WHERE user_id = %s AND LOWER(exchange_id) = 'futu' ORDER BY id DESC""",
            (int(user_id),),
        )
        rows = cur.fetchall() or []
        db.rollback()
        cur.close()
    matches: list[tuple[int, dict]] = []
    for row in rows:
        if credential_id and int(row["id"]) != int(credential_id):
            continue
        try:
            cfg = json.loads(decrypt_credential_blob(row["encrypted_config"]))
            parsed = config_from_exchange_config(cfg)
            if parsed.acc_id != int(acc_id):
                continue
            if connected and (
                parsed.host != connected.host or parsed.port != connected.port
                or parsed.security_firm != connected.security_firm
            ):
                continue
            matches.append((int(row["id"]), cfg))
        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
            continue
    if not matches:
        raise ValueError("FUTU_SAVE_SIMULATE_ACCOUNT_FIRST")
    return matches[0]


def _owned_order_identities(user_id: int, credential_id: int) -> tuple[set[str], set[str]]:
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """SELECT exchange_order_id, client_order_id, pending_order_id, strategy_id
               FROM qd_live_order_bindings
               WHERE user_id = %s AND credential_id = %s AND LOWER(exchange_id) = 'futu'
               UNION ALL
               SELECT exchange_order_id, client_order_id, id AS pending_order_id, strategy_id
               FROM pending_orders
               WHERE user_id = %s AND credential_id = %s AND LOWER(exchange_id) = 'futu'""",
            (int(user_id), int(credential_id), int(user_id), int(credential_id)),
        )
        rows = cur.fetchall() or []
        db.rollback()
        cur.close()
    remarks = {str(row["client_order_id"]) for row in rows if row.get("client_order_id")}
    for row in rows:
        strategy_id = int(row.get("strategy_id") or 0)
        pending_order_id = int(row.get("pending_order_id") or 0)
        if strategy_id > 0 and pending_order_id > 0:
            # The worker writes this remark after a successful submit. Derive
            # it from its pending row so a crash between broker accept and DB
            # update can still be identified and cancelled safely.
            remarks.add(f"qd_{strategy_id}_{pending_order_id}")
    return (
        {str(row["exchange_order_id"]) for row in rows if row.get("exchange_order_id")},
        remarks,
    )


def _cancel_queued_orders(user_id: int, credential_id: int) -> int:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """UPDATE pending_orders SET status = 'cancelled',
                     last_error = 'futu_operator_paused', updated_at = NOW()
                   WHERE user_id = %s AND credential_id = %s
                     AND LOWER(exchange_id) = 'futu'
                     AND execution_mode = 'live' AND status = 'pending'
                     AND COALESCE(exchange_order_id, '') = ''""",
                (int(user_id), int(credential_id)),
            )
            count = int(cur.rowcount or 0)
            db.commit()
            return count
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


def _cancel_owned_open_orders(user_id: int, credential_id: int, config: dict) -> int:
    from app.services.futu_trading.client import FutuClient

    client = FutuClient(config_from_exchange_config(config))
    if not client.connect(need_quote=False):
        raise RuntimeError("FUTU_OPEND_UNAVAILABLE_CANCEL_UNCONFIRMED")
    try:
        owned_ids, owned_remarks = _owned_order_identities(user_id, credential_id)
        cancelled = 0
        for attempt in range(3):
            open_orders = client.get_open_orders(strict=True)
            owned_open = [
                order for order in open_orders
                if str(order.get("orderId") or "") in owned_ids
                or str(order.get("remark") or "") in owned_remarks
            ]
            # An untracked qd_ remark could be a submit that crashed before
            # its binding was persisted. Do not cancel an unidentified order;
            # require operator review instead of claiming a safe stop.
            unknown_platform_orders = [
                order for order in open_orders
                if str(order.get("remark") or "").startswith("qd_")
                and order not in owned_open
            ]
            if unknown_platform_orders:
                raise RuntimeError("FUTU_UNTRACKED_PLATFORM_ORDER_REVIEW_REQUIRED")
            if not owned_open:
                return cancelled
            for order in owned_open:
                if client.cancel_order(str(order["orderId"])):
                    cancelled += 1
            if attempt < 2:
                time.sleep(0.5)
        raise RuntimeError("FUTU_ORDER_CANCEL_NOT_CONFIRMED")
    finally:
        client.disconnect()


def pause_account(user_id: int, credential_id: int, acc_id: int, config: dict) -> dict[str, Any]:
    """Disarm immediately, drain in-flight submit, cancel our queued/open orders."""
    begin_pause(user_id, credential_id, acc_id)
    queued = 0
    cancelled = 0
    try:
        wait_submission_barrier(acc_id)
        queued = _cancel_queued_orders(user_id, credential_id)
        cancelled = _cancel_owned_open_orders(user_id, credential_id, config)
    except Exception as exc:
        finish_pause(user_id, acc_id, confirmed=False, error=str(exc))
        return {
            "state": "unconfirmed", "enabled": False, "queued_cancelled": queued,
            "broker_cancelled": cancelled, "error": str(exc),
        }
    finish_pause(user_id, acc_id, confirmed=True)
    return {
        "state": "paused", "enabled": False, "queued_cancelled": queued,
        "broker_cancelled": cancelled,
    }

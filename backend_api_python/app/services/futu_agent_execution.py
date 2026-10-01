"""One-shot, fail-closed Agent execution against Futu US/HK SIMULATE.

The intent ID is the broker identity. A committed EXECUTING record precedes
the external call, so an ambiguous response or crash can only be reconciled,
never retried as a new order.
"""

from __future__ import annotations

import math
import time
from typing import Any

from app.services.agent_trade_intents import IntentError, _policy_row, _risk
from app.services.exchange_execution import resolve_exchange_config
from app.services.futu_trading.client import FutuClient, OrderResult
from app.services.futu_trading.config import config_from_exchange_config
from app.services.futu_trading.operator_gate import hard_switch_enabled
from app.utils.db import get_db_connection


def _broker_status(result: OrderResult) -> str:
    return {
        "submitted": "SUBMITTED",
        "partially_filled": "PARTIALLY_FILLED",
        "filled": "FILLED",
        "cancelled": "CANCELLED",
        "rejected": "FAILED",
    }.get(str(result.status).lower(), "UNCERTAIN")


def _submission_status(result: OrderResult) -> str:
    if result.success and result.order_id:
        return _broker_status(result)
    return "UNCERTAIN" if result.submission_attempted else "FAILED"


def _preflight(client: FutuClient, order: dict[str, Any]) -> tuple[dict, float]:
    market = "USStock" if client.config.trade_market == "US" else "HKStock"
    if order["broker"] != "futu" or order["market"] != market or order["order_type"] != "limit":
        raise IntentError("Only matching Futu stock SIMULATE limit orders can execute", 403)
    qty = float(order["qty"])
    if not qty.is_integer() or qty <= 0:
        raise IntentError("Futu execution requires whole shares", 400)
    if market == "HKStock":
        lot = client.get_lot_size(order["symbol"])
        if lot <= 0 or qty % lot != 0:
            raise IntentError("Futu HK quantity must be an exact broker lot multiple", 400)
    quote = client.get_simulate_execution_quote(order["symbol"])
    if not quote.get("simulate_execution_eligible"):
        raise IntentError("Fresh subscribed regular-session Futu quote is required", 409)
    price = float(quote["price"])
    limit = float(order["limit_price"])
    if not all(math.isfinite(x) and x > 0 for x in (price, limit)):
        raise IntentError("Invalid quote or limit price", 409)
    if abs(limit / price - 1) > 0.02:
        raise IntentError("Limit price deviates more than 2% from Futu quote", 409)
    if order["side"] == "buy":
        account = client.get_account_summary()
        summary = account.get("summary") or {}
        currency = "USD" if market == "USStock" else "HKD"
        if not account.get("success") or str(summary.get("currency") or "").upper() != currency:
            raise IntentError("Futu buying power could not be verified", 409)
        if market == "HKStock":
            maximum = client.get_max_cash_buy(order["symbol"], limit)
            if not math.isfinite(maximum) or maximum < qty:
                raise IntentError("Futu HK max cash buy is insufficient", 409)
        else:
            power = float(summary.get("power") or 0)
            if not math.isfinite(power) or power < qty * limit:
                raise IntentError("Futu buying power could not be verified", 409)
    else:
        # The client repeats a broker-side can_sell_qty check immediately
        # before submission; this preflight is only a preliminary guard.
        positions = client.get_positions()
        held = sum(float(p.get("quantity") or 0) for p in positions
                   if str(p.get("symbol") or "").upper() == order["symbol"]
                   and p.get("side") == "long")
        if held + 1e-9 < qty:
            raise IntentError("Futu position could not be verified for a long-only sale", 409)
    return quote, max(price, limit)


def _load_client(user_id: int, account_ref: str) -> FutuClient:
    if not account_ref.startswith("credential:"):
        raise IntentError("Invalid saved Futu account", 403)
    try:
        credential_id = int(account_ref.split(":", 1)[1])
        cfg = resolve_exchange_config({"credential_id": credential_id}, user_id=user_id)
        if str(cfg.get("exchange_id") or "").lower() != "futu":
            raise ValueError("wrong broker")
        config = config_from_exchange_config(cfg)
        if config.acc_id <= 0 or config.trade_market not in {"US", "HK"} or config.trade_env != "demo":
            raise ValueError("wrong trading account")
        return FutuClient(config)
    except (TypeError, ValueError, KeyError) as exc:
        raise IntentError("A saved stock SIMULATE account is required", 403) from exc


def _read_owned_intent(user_id: int, intent_id: int, token_id: int | None = None) -> dict:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("SELECT * FROM qd_agent_trade_intents WHERE user_id=%s AND id=%s",
                        (user_id, intent_id))
            row = dict(cur.fetchone() or {})
        finally:
            cur.close()
    if not row or row.get("broker") != "futu":
        raise IntentError("Futu intent not found", 404)
    if token_id is not None and int(row.get("agent_token_id") or 0) != token_id:
        raise IntentError("Intent belongs to another Agent token", 403)
    return row


def execute_simulate_intent(user_id: int, token: dict, intent_id: int) -> dict:
    if not token.get("paper_only", True):
        raise IntentError("SIMULATE execution requires a paper-only Agent token", 403)
    token_id = int(token["id"])
    row = _read_owned_intent(user_id, intent_id, token_id)
    if row["status"] != "PROPOSED":
        return row  # Idempotent read; never submit a second broker order.
    if not hard_switch_enabled():
        raise IntentError("Futu paper automation is disabled on this server", 403)
    account_ref = row["account_ref"]
    client = _load_client(user_id, account_ref)
    try:
        if not client.connect():
            raise IntentError("Futu OpenD is unavailable", 503)
        order = row["order_spec"]
        quote, risk_price = _preflight(client, order)
        credential_id = int(account_ref.split(":", 1)[1])
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute("SELECT pg_advisory_xact_lock(824111, %s)", (user_id,))
                cur.execute("SELECT * FROM qd_agent_trade_intents WHERE user_id=%s AND id=%s FOR UPDATE",
                            (user_id, intent_id))
                latest = dict(cur.fetchone() or {})
                if latest.get("status") != "PROPOSED":
                    db.rollback()
                    return latest
                policy = _policy_row(cur, user_id, "futu", account_ref, lock=True)
                if policy["mode"] != "PAPER_AUTO":
                    raise IntentError("Human Futu PAPER_AUTO authorization is required", 403)
                cur.execute(
                    """SELECT 1 FROM qd_futu_automation_state WHERE acc_id=%s AND user_id=%s
                       AND credential_id=%s AND enabled=TRUE AND state='armed'
                       AND min_agent_intent_id < %s""",
                    (client.config.acc_id, user_id, credential_id, intent_id),
                )
                if cur.fetchone() is None:
                    raise IntentError("Futu operator must arm this account after the intent was created", 403)
                if time.time() - float(quote["received_at"]) > 10:
                    raise IntentError("Futu quote expired while waiting for authorization", 409)
                accepted, risk = _risk(cur, user_id, policy, order, risk_price)
                if not accepted:
                    raise IntentError(f"Risk check rejected: {risk['reason']}", 403)
                notional = float(risk["notional"])
                if notional > float(token.get("max_order_notional") or 0):
                    raise IntentError("Agent token per-order notional limit", 403)
                # The token's daily cap is evaluated against all broker intents
                # accepted today by this token, including uncertain outcomes.
                cur.execute(
                    """SELECT COALESCE(SUM(notional),0) AS used FROM qd_agent_trade_intents
                       WHERE agent_token_id=%s AND created_at >= date_trunc('day', NOW())
                         AND status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED',
                                        'FILLED','CANCELLED','FAILED')""",
                    (token_id,),
                )
                if float((cur.fetchone() or {}).get("used") or 0) + notional > float(
                    token.get("max_daily_notional") or 0
                ):
                    raise IntentError("Agent token daily notional limit", 403)
                from app.services.agent_trade_intents import _json

                remark = f"qd_agent_{intent_id}"
                cur.execute(
                    """UPDATE qd_agent_trade_intents SET status='EXECUTING',
                           broker_remark=%s, quote_snapshot=%s::jsonb,
                           risk_result=%s::jsonb, notional=%s, updated_at=NOW()
                       WHERE id=%s""",
                    (remark, _json(quote), _json(risk), notional, intent_id),
                )
                db.commit()  # durable before the first possible broker submit
            except Exception:
                db.rollback()
                raise
            finally:
                cur.close()
        try:
            result = client.place_limit_order(
                order["symbol"], order["side"], order["qty"], order["limit_price"],
                order["market"], remark=remark,
            )
        except Exception:
            result = OrderResult(success=False, message="FUTU_SUBMISSION_OUTCOME_UNKNOWN",
                                 submission_attempted=True)
        status = _submission_status(result)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute(
                    """UPDATE qd_agent_trade_intents SET status=%s,
                           broker_order_id=COALESCE(NULLIF(%s,''),broker_order_id),
                           filled_qty=GREATEST(filled_qty,%s),
                           avg_fill_price=CASE WHEN %s > filled_qty THEN %s ELSE avg_fill_price END,
                           last_reconciled_at=NOW(), updated_at=NOW()
                       WHERE id=%s AND user_id=%s AND status='EXECUTING'""",
                    (status, result.order_id, result.filled, result.filled,
                     result.avg_price if result.avg_price > 0 else None, intent_id, user_id),
                )
                db.commit()
            finally:
                cur.close()
        return _read_owned_intent(user_id, intent_id, token_id)
    finally:
        client.disconnect()


def reconcile_simulate_intent(user_id: int, intent_id: int) -> dict:
    row = _read_owned_intent(user_id, intent_id)
    if row["status"] not in {"EXECUTING", "UNCERTAIN", "SUBMITTED", "PARTIALLY_FILLED"}:
        return row
    client = _load_client(user_id, row["account_ref"])
    try:
        if not client.connect(need_quote=False):
            raise IntentError("Futu OpenD is unavailable for reconciliation", 503)
        result = (client.get_order_status(row["broker_order_id"], refresh_cache=True)
                  if row.get("broker_order_id") else None)
        if result is None or not result.success:
            result = client.find_order_by_remark(
                row.get("broker_remark") or f"qd_agent_{intent_id}", refresh_cache=True,
            )
        if result is None or not result.success or not result.order_id:
            # Current-day order query may be incomplete. Absence is never
            # proof that a submission failed or safe to resubmit.
            with get_db_connection() as db:
                cur = db.cursor()
                try:
                    cur.execute(
                        """UPDATE qd_agent_trade_intents SET last_reconciled_at=NOW(),
                           status=CASE WHEN status='EXECUTING'
                                            AND updated_at < NOW() - INTERVAL '60 seconds'
                                       THEN 'UNCERTAIN' ELSE status END
                           WHERE user_id=%s AND id=%s""",
                        (user_id, intent_id),
                    )
                    db.commit()
                finally:
                    cur.close()
            return row
        status = _broker_status(result)
        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute(
                    """UPDATE qd_agent_trade_intents SET
                           status=CASE WHEN status IN ('FILLED','CANCELLED','FAILED') THEN status
                                       WHEN status='PARTIALLY_FILLED' AND %s IN ('SUBMITTED','UNCERTAIN')
                                       THEN status
                                       ELSE %s END,
                           broker_order_id=COALESCE(NULLIF(broker_order_id,''),%s),
                           filled_qty=GREATEST(filled_qty,%s),
                           avg_fill_price=CASE WHEN %s > filled_qty THEN %s ELSE avg_fill_price END,
                           last_reconciled_at=NOW(), updated_at=NOW()
                       WHERE user_id=%s AND id=%s""",
                    (status, status, result.order_id, result.filled, result.filled,
                     result.avg_price if result.avg_price > 0 else None, user_id, intent_id),
                )
                db.commit()
            finally:
                cur.close()
        return _read_owned_intent(user_id, intent_id)
    finally:
        client.disconnect()


def cancel_open_simulate_agent_orders(user_id: int) -> dict:
    """Request cancellation only for platform-owned Agent order identities."""
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """SELECT id, account_ref, broker_order_id, broker_remark
                   FROM qd_agent_trade_intents WHERE user_id=%s AND broker='futu'
                     AND status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED')
                   ORDER BY id""",
                (user_id,),
            )
            rows = [dict(row) for row in (cur.fetchall() or [])]
        finally:
            cur.close()
    accepted = 0
    unresolved = []
    for row in rows:
        intent_id = int(row["id"])
        client = None
        try:
            client = _load_client(user_id, row["account_ref"])
            if not client.connect(need_quote=False):
                raise RuntimeError("FUTU_OPEND_UNAVAILABLE")
            result = (client.get_order_status(row["broker_order_id"], refresh_cache=True)
                      if row.get("broker_order_id") else None)
            if result is None or not result.success:
                result = client.find_order_by_remark(
                    row.get("broker_remark") or f"qd_agent_{intent_id}", refresh_cache=True,
                )
            if result is None or not result.success or not result.order_id:
                raise RuntimeError("FUTU_ORDER_OUTCOME_UNKNOWN")
            if _broker_status(result) in {"FILLED", "CANCELLED", "FAILED"}:
                continue
            if not client.cancel_order(result.order_id):
                raise RuntimeError("FUTU_CANCEL_NOT_ACKNOWLEDGED")
            accepted += 1
        except Exception:
            unresolved.append(intent_id)
        finally:
            if client is not None:
                client.disconnect()
    return {
        "futu_cancel_requests_accepted": accepted,
        "futu_unresolved_intent_ids": unresolved,
        "futu_manual_review_required": bool(rows),
    }

"""Durable, default-deny permission for Futu US SIMULATE order submission.

The Web diagnostic connection is intentionally unrelated to this gate. An
account must be explicitly armed after each worker start. Both a submit and
pause use the same PostgreSQL advisory lock, so pause can wait for an already
in-flight broker submission before reporting that submissions have stopped.
"""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Iterator

from app.utils.db import get_db_connection

_LOCK_NAMESPACE = 20260929


def hard_switch_enabled() -> bool:
    return os.getenv("FUTU_PAPER_AUTOTRADE_ALLOWED", "false").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _lock_account(cur, acc_id: int, *, timeout: str = "15s") -> None:
    cur.execute("SELECT set_config('lock_timeout', %s, true)", (timeout,))
    cur.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
        (_LOCK_NAMESPACE, str(int(acc_id))),
    )


def state_for_user(user_id: int) -> list[dict]:
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """SELECT acc_id, credential_id, enabled, state, last_error,
                      armed_at, paused_at, updated_at
               FROM qd_futu_automation_state WHERE user_id = %s
               ORDER BY updated_at DESC""",
            (int(user_id),),
        )
        rows = [dict(row) for row in (cur.fetchall() or [])]
        db.rollback()
        cur.close()
    for row in rows:
        if not hard_switch_enabled() and row["state"] == "armed":
            row["state"] = "unconfirmed"
            row["last_error"] = "server_hard_switch_off_cancel_verification_required"
        row["enabled"] = bool(row["enabled"] and hard_switch_enabled())
    return rows


def arm(user_id: int, credential_id: int, acc_id: int) -> None:
    if not hard_switch_enabled():
        raise ValueError("FUTU_PAPER_AUTOTRADE_HARD_DISABLED")
    if min(int(user_id), int(credential_id), int(acc_id)) <= 0:
        raise ValueError("FUTU_ACCOUNT_AND_CREDENTIAL_REQUIRED")
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            _lock_account(cur, acc_id)
            cur.execute(
                "SELECT user_id, state FROM qd_futu_automation_state WHERE acc_id = %s FOR UPDATE",
                (int(acc_id),),
            )
            previous = cur.fetchone()
            if previous and int(previous["user_id"]) != int(user_id):
                raise ValueError("FUTU_ACCOUNT_OWNED_BY_ANOTHER_USER")
            if previous and previous["state"] in {"stopping", "unconfirmed"}:
                raise ValueError("FUTU_STOP_NOT_CONFIRMED")
            # A fresh arm never releases signals queued while the account was
            # paused. The final submit gate also checks this high-water mark.
            cur.execute(
                "SELECT COALESCE(MAX(id), 0) AS last_id FROM pending_orders WHERE user_id = %s AND credential_id = %s",
                (int(user_id), int(credential_id)),
            )
            min_pending_order_id = int((cur.fetchone() or {}).get("last_id") or 0)
            cur.execute(
                """SELECT COALESCE(MAX(id), 0) AS last_id FROM qd_agent_trade_intents
                   WHERE user_id = %s AND broker = 'futu' AND account_ref = %s""",
                (int(user_id), f"credential:{int(credential_id)}"),
            )
            min_agent_intent_id = int((cur.fetchone() or {}).get("last_id") or 0)
            cur.execute(
                """INSERT INTO qd_futu_automation_state
                   (acc_id, user_id, credential_id, min_pending_order_id, min_agent_intent_id,
                    enabled, state, last_error, armed_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, TRUE, 'armed', '', NOW(), NOW())
                   ON CONFLICT (acc_id) DO UPDATE SET
                     credential_id = EXCLUDED.credential_id,
                     min_pending_order_id = EXCLUDED.min_pending_order_id,
                     min_agent_intent_id = EXCLUDED.min_agent_intent_id,
                     enabled = TRUE, state = 'armed', last_error = '',
                     armed_at = NOW(), updated_at = NOW()""",
                (int(acc_id), int(user_id), int(credential_id), min_pending_order_id,
                 min_agent_intent_id),
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


def begin_pause(user_id: int, credential_id: int, acc_id: int) -> None:
    """Block every new submission immediately, before waiting for in-flight calls."""
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                "SELECT user_id FROM qd_futu_automation_state WHERE acc_id = %s FOR UPDATE",
                (int(acc_id),),
            )
            previous = cur.fetchone()
            if previous and int(previous["user_id"]) != int(user_id):
                raise ValueError("FUTU_ACCOUNT_OWNED_BY_ANOTHER_USER")
            cur.execute(
                """INSERT INTO qd_futu_automation_state
                   (acc_id, user_id, credential_id, enabled, state, paused_at, updated_at)
                   VALUES (%s, %s, %s, FALSE, 'stopping', NOW(), NOW())
                   ON CONFLICT (acc_id) DO UPDATE SET
                     enabled = FALSE, state = 'stopping',
                     paused_at = NOW(), updated_at = NOW()""",
                (int(acc_id), int(user_id), int(credential_id)),
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


def wait_submission_barrier(acc_id: int) -> None:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            _lock_account(cur, acc_id, timeout="30s")
        finally:
            db.rollback()
            cur.close()


def finish_pause(user_id: int, acc_id: int, *, confirmed: bool, error: str = "") -> None:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """UPDATE qd_futu_automation_state SET
                     enabled = FALSE, state = %s, last_error = %s, updated_at = NOW()
                   WHERE acc_id = %s AND user_id = %s""",
                ("paused" if confirmed else "unconfirmed", str(error or "")[:500],
                 int(acc_id), int(user_id)),
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


def disarm_all_on_worker_start() -> None:
    """A restarted trading worker never inherits yesterday's permission."""
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """UPDATE qd_futu_automation_state SET
                     enabled = FALSE,
                     state = CASE WHEN state = 'paused' THEN 'paused' ELSE 'unconfirmed' END,
                     last_error = CASE WHEN state = 'paused' THEN last_error
                                       ELSE 'worker_restarted_cancel_verification_required' END,
                     paused_at = NOW(), updated_at = NOW()
                   WHERE enabled = TRUE OR state IN ('armed', 'stopping')"""
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()


@contextmanager
def submission_permit(*, user_id: int, credential_id: int, acc_id: int, remark: str,
                      symbol: str = "", side: str = "", qty: float = 0,
                      limit_price: float = 0) -> Iterator[None]:
    """Hold the account lock through the *broker* submit; DB errors fail closed."""
    if not hard_switch_enabled():
        raise ValueError("FUTU_PAPER_AUTOTRADE_HARD_DISABLED")
    if min(int(user_id), int(credential_id), int(acc_id)) <= 0:
        raise ValueError("FUTU_OPERATOR_NOT_ARMED")
    strategy_match = re.fullmatch(r"qd_(\d+)_(\d+)(?:_[a-z0-9]+)?", str(remark or ""))
    agent_match = re.fullmatch(r"qd_agent_(\d+)", str(remark or ""))
    if strategy_match is None and agent_match is None:
        raise ValueError("FUTU_PLATFORM_ORDER_ID_REQUIRED")
    if agent_match is not None and (not symbol or side not in {"buy", "sell"}
                                    or qty <= 0 or limit_price <= 0):
        raise ValueError("FUTU_AGENT_ORDER_SPEC_REQUIRED")
    allowed = False
    broker_error: BaseException | None = None
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            if agent_match is not None:
                # Serialize human policy changes through the broker call.
                # EMERGENCY_STOP cannot commit midway through submission.
                cur.execute("SELECT pg_advisory_xact_lock(824111, %s)", (int(user_id),))
            _lock_account(cur, acc_id)
            if strategy_match is not None:
                cur.execute(
                    """SELECT 1 FROM qd_futu_automation_state gate
                       JOIN pending_orders pending
                         ON pending.id = %s AND pending.user_id = gate.user_id
                        AND pending.credential_id = gate.credential_id
                       WHERE gate.acc_id = %s AND gate.user_id = %s
                         AND gate.credential_id = %s
                         AND gate.enabled = TRUE AND gate.state = 'armed'
                         AND pending.id > gate.min_pending_order_id
                         AND pending.strategy_id = %s
                         AND LOWER(pending.exchange_id) = 'futu'
                         AND pending.execution_mode = 'live'
                         AND pending.status = 'processing'""",
                    (int(strategy_match.group(2)), int(acc_id), int(user_id),
                     int(credential_id), int(strategy_match.group(1))),
                )
            else:
                cur.execute(
                    """SELECT 1 FROM qd_futu_automation_state gate
                       JOIN qd_agent_trade_intents intent
                         ON intent.id = %s AND intent.user_id = gate.user_id
                        AND intent.account_ref = 'credential:' || gate.credential_id::text
                       JOIN qd_agent_trading_policies policy
                         ON policy.user_id = gate.user_id AND policy.broker = 'futu'
                        AND policy.account_ref = intent.account_ref
                       JOIN qd_agent_tokens token
                         ON token.id = intent.agent_token_id AND token.user_id = gate.user_id
                       LEFT JOIN qd_agent_trading_policies global_stop
                         ON global_stop.user_id = gate.user_id AND global_stop.broker = '*'
                        AND global_stop.account_ref = '*'
                       WHERE gate.acc_id = %s AND gate.user_id = %s
                         AND gate.credential_id = %s
                         AND gate.enabled = TRUE AND gate.state = 'armed'
                         AND intent.id > gate.min_agent_intent_id
                         AND intent.broker = 'futu' AND intent.status = 'EXECUTING'
                         AND intent.broker_remark = %s
                         AND intent.order_spec->>'symbol' = %s
                         AND intent.order_spec->>'side' = %s
                         AND (intent.order_spec->>'qty')::numeric = %s
                         AND (intent.order_spec->>'limit_price')::numeric = %s
                         AND intent.order_spec->>'order_type' = 'limit'
                         AND intent.order_spec->>'market' = 'USStock'
                         AND token.status = 'active' AND token.paper_only = TRUE
                         AND (token.expires_at IS NULL OR token.expires_at > NOW())
                         AND policy.mode = 'PAPER_AUTO' AND policy.enabled_until > NOW()
                         AND COALESCE(global_stop.mode, '') <> 'EMERGENCY_STOP'""",
                    (int(agent_match.group(1)), int(acc_id), int(user_id),
                     int(credential_id), str(remark), str(symbol).upper(), str(side).lower(),
                     float(qty), float(limit_price)),
                )
            allowed = cur.fetchone() is not None
            if allowed:
                try:
                    yield
                except BaseException as exc:
                    # Broker exceptions must not flow through the DB pool's
                    # generic error logger; an SDK message can contain data
                    # that does not belong in database logs.
                    broker_error = exc
        finally:
            db.rollback()  # releases the advisory transaction lock
            cur.close()
    if not allowed:
        raise ValueError("FUTU_OPERATOR_NOT_ARMED")
    if broker_error is not None:
        raise broker_error

"""Legacy Agent quick-trade alias and emergency cancellation.

Every new quick order is an immutable trade intent. A token, confirm flag or
environment variable can no longer call a broker directly from this route.
"""

from __future__ import annotations

from app.services.kline import KlineService
from app.utils.agent_auth import SCOPE_T, agent_required, current_user_id
from app.utils.db import get_db_connection
from app.utils.logger import get_logger
from flask import request

from . import agent_v1_bp
from ._helpers import envelope, error, get_json_or_400

logger = get_logger(__name__)
_kline = KlineService()


def _last_price(market: str, symbol: str) -> float | None:
    """Research K-line price, permitted only for internal paper simulation."""
    try:
        rows = _kline.get_kline(market=market, symbol=symbol, timeframe="1m", limit=1) or []
        if not rows:
            return None
        last = rows[-1]
        if isinstance(last, dict):
            for key in ("close", "c", "Close"):
                if last.get(key) is not None:
                    return float(last[key])
    except Exception as exc:
        logger.warning("Agent internal paper quote failed: %s", exc)
    return None


def _paper_fill_outcome(body: dict, last_price: float | None) -> tuple[float | None, str, str]:
    """Retained for existing paper simulations and regression tests."""
    if last_price is None:
        return None, "rejected", "no last price available; recorded without fill"
    order_type = str(body.get("order_type") or "market").strip().lower()
    if order_type == "market":
        return float(last_price), "filled", ""
    side = str(body.get("side") or "").strip().lower()
    limit_price = float(body.get("limit_price") or 0)
    marketable = (side == "buy" and last_price <= limit_price) or (
        side == "sell" and last_price >= limit_price
    )
    if marketable:
        return float(last_price), "filled", ""
    return None, "submitted", "paper limit order is waiting for its trigger price"


@agent_v1_bp.route("/quick-trade/orders", methods=["POST"])
@agent_required(SCOPE_T)
def place_order():
    """Compatibility alias for POST /trade-intents; no direct broker route."""
    body, err = get_json_or_400()
    if err:
        return err
    body = dict(body)
    body.setdefault("order_type", "market")
    from .trade_intents import submit_trade_intent_request

    return submit_trade_intent_request(body)


def cancel_agent_orders(user_id: int) -> dict:
    """Best-effort cancellation only; never liquidates a position."""
    live_cancel_requests_accepted = 0
    live_failures: list[dict] = []
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """SELECT id, credential_id, symbol, market_type, exchange_order_id, raw_result
               FROM qd_quick_trades
               WHERE user_id=%s AND source='agent_mcp'
                 AND status IN ('submitted','partial','partially_filled')
                 AND COALESCE(exchange_order_id,'') <> ''
               ORDER BY id DESC""",
            (user_id,),
        )
        live_rows = cur.fetchall() or []
        cur.close()

    for row in live_rows:
        try:
            from app.services.pending_orders.live_order_phases import cancel_live_limit_order
            from app.services.quick_trade.credentials import build_exchange_config, create_exchange_client

            market_type = str(row.get("market_type") or "swap")
            config = build_exchange_config(int(row.get("credential_id") or 0), user_id, {
                "market_type": market_type,
            })
            client = create_exchange_client(config, market_type=market_type)
            raw = row.get("raw_result") or {}
            if not isinstance(raw, dict):
                raw = {}
            metadata = raw.get("_quick_trade") or {}
            outcome = cancel_live_limit_order(
                client=client,
                symbol=str(row.get("symbol") or ""),
                order_id=str(row.get("exchange_order_id") or ""),
                client_order_id=str(metadata.get("client_order_id") or ""),
                market_type=market_type,
                exchange_config=config,
            )
            if outcome is None or outcome is False or (
                isinstance(outcome, dict) and outcome.get("success") is False
            ):
                raise ValueError("exchange did not accept cancellation request")
            # An exchange acknowledgement is not proof of a terminal order
            # state. Keep the local order open until normal reconciliation
            # confirms cancellation or a fill; never claim it was cancelled.
            live_cancel_requests_accepted += 1
        except Exception as exc:
            live_failures.append({
                "trade_id": row.get("id"),
                "exchange_order_id": row.get("exchange_order_id"),
                "error": str(exc)[:300],
            })

    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """UPDATE qd_agent_paper_orders
               SET status='cancelled', note=COALESCE(note,'') || ' [agent_cancel]'
               WHERE user_id=%s AND status NOT IN ('filled','cancelled','rejected')""",
            (user_id,),
        )
        paper_affected = cur.rowcount
        cur.execute(
            """UPDATE qd_agent_trade_intents SET status='CANCELLED', updated_at=NOW()
               WHERE user_id=%s AND status='SUBMITTED'
                 AND paper_order_uid IN (
                   SELECT order_uid FROM qd_agent_paper_orders
                   WHERE user_id=%s AND status='cancelled'
                 )""",
            (user_id, user_id),
        )
        intent_affected = cur.rowcount
        db.commit()
        cur.close()
    return {
        "cancelled_open_paper_orders": int(paper_affected or 0),
        "cancelled_paper_intents": int(intent_affected or 0),
        "live_cancel_requests_accepted": live_cancel_requests_accepted,
        "live_cancel_failures": live_failures,
        "manual_review_required": bool(live_rows),
    }


@agent_v1_bp.route("/quick-trade/kill-switch", methods=["POST"])
@agent_required(SCOPE_T)
def kill_switch():
    """Persist stop first, cancel open Agent orders, revoke T tokens."""
    body = request.get_json(silent=True) or {}
    if body.get("confirm") is not True:
        return error(400, "confirm=true is required for the emergency kill switch")
    user_id = current_user_id()
    from app.services.agent_trade_intents import emergency_stop_user

    emergency_stop_user(user_id)
    cancellation = cancel_agent_orders(user_id)
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(
            """UPDATE qd_agent_tokens SET status='revoked'
               WHERE user_id=%s AND status='active'
                 AND (',' || UPPER(scopes) || ',') LIKE '%%,T,%%'""",
            (user_id,),
        )
        revoked = cur.rowcount
        db.commit()
        cur.close()
    return envelope({**cancellation, "revoked_t_tokens": int(revoked or 0)}, message="emergency-stop")

"""Server-owned Agent trading policies, proposals and paper execution.

REAL trading is deliberately absent.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.utils.db import get_db_connection


class IntentError(ValueError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


_DEFAULT = {
    "mode": "PLAN_ONLY",
    "allowed_markets": [],
    "allowed_symbols": [],
    "max_order_notional": 1000.0,
    "max_daily_notional": 5000.0,
    "max_orders_per_day": 10,
    "allow_market_order": False,
    "allow_short": False,
    "enabled_until": None,
}
def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _positive(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise IntentError(f"{name} must be a positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise IntentError(f"{name} must be a positive number")
    return number


def _list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or len(value) > 100:
        raise IntentError(f"{name} must be a list of at most 100 values")
    items = [str(item).strip().upper() for item in value]
    if any(not item or len(item) > 80 for item in items):
        raise IntentError(f"{name} contains an invalid value")
    return sorted(set(items))


def normalize_order(body: dict[str, Any]) -> dict[str, Any]:
    """Only canonical, allowlisted fields become part of the immutable hash."""
    allowed = {
        "broker", "credential_id", "market", "symbol", "side", "qty",
        "order_type", "limit_price", "reason", "strategy_version",
    }
    extra = sorted(set(body) - allowed)
    if extra:
        raise IntentError(f"Unsupported trade intent fields: {', '.join(extra)}")
    broker = str(body.get("broker") or ("futu" if body.get("credential_id") else "platform")).strip().lower()
    if broker not in {"platform", "futu"}:
        raise IntentError("Only platform paper or Futu proposals are supported")
    market = str(body.get("market") or "").strip()
    symbol = str(body.get("symbol") or "").strip().upper()
    side = str(body.get("side") or "").strip().lower()
    order_type = str(body.get("order_type") or "limit").strip().lower()
    if not market or not symbol or len(market) > 40 or len(symbol) > 60:
        raise IntentError("market and symbol are required")
    if side not in {"buy", "sell"}:
        raise IntentError("side must be buy or sell")
    if order_type not in {"market", "limit"}:
        raise IntentError("order_type must be market or limit")
    qty = _positive(body.get("qty"), "qty")
    limit = None
    if order_type == "limit":
        limit = _positive(body.get("limit_price"), "limit_price")
    elif body.get("limit_price") is not None:
        raise IntentError("limit_price is only valid for limit orders")
    try:
        credential_id = int(body.get("credential_id") or 0)
    except (TypeError, ValueError) as exc:
        raise IntentError("credential_id must be an integer") from exc
    if credential_id < 0 or (broker == "futu" and not credential_id):
        raise IntentError("Futu proposals require a saved credential_id")
    if broker == "platform" and credential_id:
        raise IntentError("Platform paper proposals cannot select a broker credential")
    if broker == "futu" and (market not in {"USStock", "HKStock"} or order_type != "limit"):
        raise IntentError("Futu proposals are stock SIMULATE limit orders only")
    return {
        "broker": broker,
        "credential_id": credential_id or None,
        "market": market,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "order_type": order_type,
        "limit_price": limit,
        "reason": str(body.get("reason") or "").strip()[:2000],
        "strategy_version": str(body.get("strategy_version") or "").strip()[:120],
    }


def account_scope(user_id: int, order: dict[str, Any]) -> tuple[str, str]:
    if order["broker"] == "platform":
        return "platform", "default"
    from app.services.exchange_execution import resolve_exchange_config
    from app.services.futu_trading.config import config_from_exchange_config

    credential_id = int(order["credential_id"])
    cfg = resolve_exchange_config({"credential_id": credential_id}, user_id=user_id)
    if str(cfg.get("exchange_id") or "").lower() != "futu":
        raise IntentError("Futu credential is not available to this user", 403)
    config = config_from_exchange_config(cfg)
    if config.acc_id <= 0 or config.trade_env != "demo" or config.trade_market not in {"US", "HK"}:
        raise IntentError("An explicitly selected stock SIMULATE account is required", 403)
    expected = "USStock" if config.trade_market == "US" else "HKStock"
    # Policy and account-data reads identify the saved account without an order.
    # Actual order proposals always carry a market and must still match it.
    if order.get("market") is not None and order["market"] != expected:
        raise IntentError("Futu proposal market does not match the saved account", 403)
    return "futu", f"credential:{credential_id}"


def _policy_row(cur, user_id: int, broker: str, account_ref: str, *, lock: bool = False) -> dict:
    # A global emergency stop dominates every account, including absent rows.
    cur.execute(
        "SELECT mode FROM qd_agent_trading_policies WHERE user_id=%s AND broker='*' AND account_ref='*'",
        (user_id,),
    )
    global_row = cur.fetchone()
    cur.execute(
        """SELECT mode, allowed_markets, allowed_symbols, max_order_notional,
                  max_daily_notional, max_orders_per_day, allow_market_order,
                  allow_short, enabled_until, updated_at
           FROM qd_agent_trading_policies
           WHERE user_id=%s AND broker=%s AND account_ref=%s"""
        + (" FOR UPDATE" if lock else ""),
        (user_id, broker, account_ref),
    )
    row = dict(cur.fetchone() or {})
    policy = {**_DEFAULT, **row, "broker": broker, "account_ref": account_ref}
    configured = policy["mode"]
    expires = policy.get("enabled_until")
    if expires is not None:
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if expires <= datetime.now(timezone.utc):
            policy["mode"] = "PLAN_ONLY"
    if global_row and global_row.get("mode") == "EMERGENCY_STOP":
        policy["mode"] = "EMERGENCY_STOP"
    policy["configured_mode"] = configured
    return policy


def get_policy(user_id: int, broker: str = "platform", account_ref: str = "default") -> dict:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            return _policy_row(cur, user_id, broker, account_ref)
        finally:
            cur.close()


def set_policy(user_id: int, broker: str, account_ref: str, body: dict[str, Any]) -> dict:
    """Human-session only; the caller route must enforce JWT, never Agent T."""
    if broker not in {"platform", "futu", "*"} or not account_ref or len(account_ref) > 80:
        raise IntentError("Unsupported policy account")
    if broker == "futu" and not account_ref.startswith("credential:"):
        raise IntentError("Futu policy requires a saved credential reference")
    if broker == "*" and account_ref != "*":
        raise IntentError("Global policy requires account_ref='*'")
    mode = str(body.get("mode") or "").strip().upper()
    if mode in {"LIVE_APPROVAL", "LIVE_AUTO"}:
        raise IntentError("REAL trading modes are not implemented or enabled", 501)
    if mode not in {"PLAN_ONLY", "PAPER_AUTO", "EMERGENCY_STOP"}:
        raise IntentError("Invalid trading policy mode")
    if body.get("confirm_mode") != mode:
        raise IntentError("Type the exact mode in confirm_mode")
    if mode == "PAPER_AUTO" and broker not in {"platform", "futu"}:
        raise IntentError("PAPER_AUTO requires a platform or saved Futu account")
    markets = _list(body.get("allowed_markets", []), "allowed_markets")
    symbols = _list(body.get("allowed_symbols", []), "allowed_symbols")
    per_order = _positive(body.get("max_order_notional", 1000), "max_order_notional")
    daily = _positive(body.get("max_daily_notional", 5000), "max_daily_notional")
    try:
        count = int(body.get("max_orders_per_day", 10))
    except (TypeError, ValueError) as exc:
        raise IntentError("max_orders_per_day must be an integer") from exc
    if count < 1 or count > 100 or daily < per_order:
        raise IntentError("Invalid daily order/notional limits")
    if body.get("allow_short") is True:
        raise IntentError("Short selling is not supported")
    allow_market = body.get("allow_market_order") is True
    enabled_until = None
    if mode == "PAPER_AUTO":
        if not markets or not symbols or "*" in markets or "*" in symbols:
            raise IntentError("PAPER_AUTO requires exact market and symbol allowlists")
        if broker == "futu" and (markets not in (["USSTOCK"], ["HKSTOCK"]) or allow_market):
            raise IntentError("Futu PAPER_AUTO permits one stock market and limit orders only")
        raw = str(body.get("enabled_until") or "")
        try:
            enabled_until = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntentError("enabled_until must be an ISO-8601 timestamp") from exc
        if enabled_until.tzinfo is None:
            raise IntentError("enabled_until requires a timezone")
        now = datetime.now(timezone.utc)
        if not now < enabled_until <= now + timedelta(hours=24):
            raise IntentError("PAPER_AUTO must expire within 24 hours")
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            # Serialize human policy changes and all Agent submissions for the
            # tenant. Emergency stop cannot race past a just-submitting order.
            cur.execute("SELECT pg_advisory_xact_lock(824111, %s)", (user_id,))
            previous = _policy_row(cur, user_id, broker, account_ref, lock=True)
            cur.execute(
                """INSERT INTO qd_agent_trading_policies
                     (user_id, broker, account_ref, mode, allowed_markets, allowed_symbols,
                      max_order_notional, max_daily_notional, max_orders_per_day,
                      allow_market_order, allow_short, enabled_until)
                   VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,FALSE,%s)
                   ON CONFLICT (user_id,broker,account_ref) DO UPDATE SET
                     mode=EXCLUDED.mode, allowed_markets=EXCLUDED.allowed_markets,
                     allowed_symbols=EXCLUDED.allowed_symbols,
                     max_order_notional=EXCLUDED.max_order_notional,
                     max_daily_notional=EXCLUDED.max_daily_notional,
                     max_orders_per_day=EXCLUDED.max_orders_per_day,
                     allow_market_order=EXCLUDED.allow_market_order,
                     allow_short=FALSE, enabled_until=EXCLUDED.enabled_until,
                     updated_at=NOW()""",
                (user_id, broker, account_ref, mode, _json(markets), _json(symbols),
                 per_order, daily, count, allow_market, enabled_until),
            )
            cur.execute(
                """INSERT INTO qd_agent_policy_audit
                     (user_id, broker, account_ref, actor_user_id, previous_mode, new_mode,
                      previous_policy, new_policy)
                   VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                (user_id, broker, account_ref, user_id, previous["configured_mode"], mode,
                 _json(previous), _json({
                     "mode": mode, "allowed_markets": markets, "allowed_symbols": symbols,
                     "max_order_notional": per_order, "max_daily_notional": daily,
                     "max_orders_per_day": count, "allow_market_order": allow_market,
                     "allow_short": False,
                     "enabled_until": enabled_until.isoformat() if enabled_until else None,
                 })),
            )
            if mode == "EMERGENCY_STOP":
                if broker == "*":
                    # Clearing the global stop must never resurrect a still-
                    # unexpired PAPER_AUTO account policy. Downgrade every
                    # account first, with a separate audit row for each.
                    cur.execute(
                        """INSERT INTO qd_agent_policy_audit
                             (user_id, broker, account_ref, actor_user_id,
                              previous_mode, new_mode, previous_policy, new_policy)
                           SELECT user_id, broker, account_ref, %s, mode,
                                  'PLAN_ONLY', to_jsonb(p),
                                  jsonb_build_object('mode', 'PLAN_ONLY',
                                                     'reason', 'global_emergency_stop')
                           FROM qd_agent_trading_policies p
                           WHERE user_id=%s AND broker <> '*' AND mode <> 'PLAN_ONLY'""",
                        (user_id, user_id),
                    )
                    cur.execute(
                        """UPDATE qd_agent_trading_policies
                           SET mode='PLAN_ONLY', enabled_until=NULL, updated_at=NOW()
                           WHERE user_id=%s AND broker <> '*'""",
                        (user_id,),
                    )
                cur.execute(
                    """UPDATE qd_agent_trade_intents SET status='EXPIRED', updated_at=NOW()
                       WHERE user_id=%s AND status='PROPOSED'
                         AND (%s='*' OR (broker=%s AND account_ref=%s))""",
                    (user_id, broker, broker, account_ref),
                )
            db.commit()
        finally:
            cur.close()
    return get_policy(user_id, broker, account_ref)


def _risk(cur, user_id: int, policy: dict, order: dict, price: float) -> tuple[bool, dict]:
    market = order["market"].upper()
    symbol = order["symbol"].upper()
    if market not in policy["allowed_markets"] or symbol not in policy["allowed_symbols"]:
        return False, {"reason": "not_allowlisted"}
    if order["order_type"] == "market" and not policy["allow_market_order"]:
        return False, {"reason": "market_order_disabled"}
    # Internal paper fills at the observed research price. In particular, a
    # marketable sell may fill *above* its limit, so the limit alone can
    # understate exposure and bypass the per-order/daily caps.
    notional = order["qty"] * max(order["limit_price"] or 0, price)
    if not math.isfinite(notional) or notional <= 0:
        return False, {"reason": "price_unavailable"}
    if notional > float(policy["max_order_notional"]):
        return False, {"reason": "max_order_notional", "notional": notional}
    cur.execute(
        """SELECT COALESCE(SUM(notional),0) AS used, COUNT(*) AS orders
           FROM qd_agent_trade_intents WHERE user_id=%s AND broker=%s AND account_ref=%s
             AND created_at >= date_trunc('day', NOW())
             AND status IN ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED',
                            'FILLED','CANCELLED','FAILED')""",
        (user_id, policy["broker"], policy["account_ref"]),
    )
    daily = cur.fetchone() or {}
    if float(daily.get("used") or 0) + notional > float(policy["max_daily_notional"]):
        return False, {"reason": "max_daily_notional"}
    if int(daily.get("orders") or 0) >= int(policy["max_orders_per_day"]):
        return False, {"reason": "max_orders_per_day"}
    if order["side"] == "sell" and not policy["allow_short"] and order["broker"] == "platform":
        cur.execute(
            """SELECT COALESCE(SUM(CASE WHEN side='buy' THEN qty ELSE -qty END),0) AS held
               FROM qd_agent_paper_orders
               WHERE user_id=%s AND market=%s AND symbol=%s AND status='filled'""",
            (user_id, order["market"], order["symbol"]),
        )
        if float((cur.fetchone() or {}).get("held") or 0) + 1e-9 < order["qty"]:
            return False, {"reason": "short_sale_disabled"}
    return True, {"reason": "accepted", "notional": notional}


def _paper_outcome(order: dict, price: float) -> tuple[str, float | None]:
    if order["order_type"] == "market":
        return "filled", price
    marketable = (order["side"] == "buy" and price <= order["limit_price"]) or (
        order["side"] == "sell" and price >= order["limit_price"]
    )
    return ("filled", price) if marketable else ("submitted", None)


def submit_intent(
    user_id: int,
    token: dict,
    body: dict[str, Any],
    idempotency_key: str,
    *,
    quote_provider: Callable[[str, str], float | None] | None = None,
    require_paper_execution: bool = False,
) -> dict:
    if not 1 <= len(idempotency_key) <= 120:
        raise IntentError("Idempotency-Key is required (1–120 characters)")
    order = normalize_order(body)
    broker, account_ref = account_scope(user_id, order)
    digest = hashlib.sha256(_json(order).encode("utf-8")).hexdigest()
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("SELECT pg_advisory_xact_lock(824111, %s)", (user_id,))
            policy = _policy_row(cur, user_id, broker, account_ref, lock=True)
            if policy["mode"] == "EMERGENCY_STOP":
                raise IntentError("Agent trading is stopped for this account", 403)
            if require_paper_execution and (broker != "platform" or policy["mode"] != "PAPER_AUTO"):
                raise IntentError("Direct paper orders require an active platform PAPER_AUTO policy", 403)
            if require_paper_execution and not token.get("paper_only", True):
                raise IntentError("Live-capable tokens and Futu accounts cannot use platform paper execution", 403)
            cur.execute(
                """INSERT INTO qd_agent_trade_intents
                     (user_id,agent_token_id,broker,account_ref,idempotency_key,intent_hash,order_spec)
                   VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)
                   ON CONFLICT (agent_token_id,idempotency_key) DO NOTHING RETURNING id""",
                (user_id, int(token["id"]), broker, account_ref, idempotency_key, digest, _json(order)),
            )
            created = cur.fetchone()
            if not created:
                cur.execute(
                    """SELECT * FROM qd_agent_trade_intents
                       WHERE agent_token_id=%s AND idempotency_key=%s""",
                    (int(token["id"]), idempotency_key),
                )
                existing = dict(cur.fetchone() or {})
                if existing.get("intent_hash") != digest:
                    raise IntentError("Idempotency-Key was used with different order content", 409)
                db.commit()
                return existing
            intent_id = int(created["id"])
            status = "PROPOSED"
            quote = None
            risk = {"reason": "plan_only", "broker_execution": False}
            notional = None
            order_uid = None
            if require_paper_execution:
                # Research K-lines are acceptable for *internal simulation only*.
                # This quote can never authorize a Futu or REAL broker order.
                try:
                    price = quote_provider(order["market"], order["symbol"]) if quote_provider else None
                except Exception:
                    price = None
                price = float(price or 0)
                quote = {
                    "price": price, "provider": "research_kline", "broker": None,
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "is_realtime": False, "execution_eligible": False,
                }
                if price <= 0 or not math.isfinite(price):
                    status, risk = "REJECTED", {"reason": "price_unavailable"}
                else:
                    accepted, risk = _risk(cur, user_id, policy, order, price)
                    if accepted:
                        notional = risk["notional"]
                        paper_status, fill_price = _paper_outcome(order, price)
                        status = "FILLED" if paper_status == "filled" else "SUBMITTED"
                        order_uid = f"intent-{intent_id}"
                        cur.execute(
                            """INSERT INTO qd_agent_paper_orders
                                 (order_uid,user_id,agent_token_id,market,symbol,side,order_type,
                                  qty,limit_price,fill_price,fill_value,status,note)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (order_uid, user_id, int(token["id"]), order["market"], order["symbol"],
                             order["side"], order["order_type"], order["qty"], order["limit_price"],
                             fill_price, (fill_price * order["qty"]) if fill_price else None,
                             paper_status, "internal paper simulation; not Futu SIMULATE"),
                        )
                    else:
                        status = "REJECTED"
            cur.execute(
                """UPDATE qd_agent_trade_intents SET status=%s, quote_snapshot=%s::jsonb,
                     risk_result=%s::jsonb, notional=%s, paper_order_uid=%s, updated_at=NOW()
                   WHERE id=%s RETURNING *""",
                (status, _json(quote) if quote else None, _json(risk), notional, order_uid, intent_id),
            )
            result = dict(cur.fetchone())
            db.commit()
            return result
        finally:
            cur.close()


def list_intents(user_id: int, *, limit: int = 100) -> list[dict]:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """SELECT * FROM qd_agent_trade_intents WHERE user_id=%s
                   ORDER BY id DESC LIMIT %s""",
                (user_id, max(1, min(limit, 200))),
            )
            return [dict(row) for row in (cur.fetchall() or [])]
        finally:
            cur.close()


def get_intent(user_id: int, intent_id: int) -> dict | None:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute("SELECT * FROM qd_agent_trade_intents WHERE user_id=%s AND id=%s", (user_id, intent_id))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            cur.close()


def cancel_proposal(user_id: int, intent_id: int) -> dict | None:
    with get_db_connection() as db:
        cur = db.cursor()
        try:
            cur.execute(
                """UPDATE qd_agent_trade_intents SET status='CANCELLED', updated_at=NOW()
                   WHERE user_id=%s AND id=%s AND status='PROPOSED' RETURNING *""",
                (user_id, intent_id),
            )
            row = cur.fetchone()
            db.commit()
            return dict(row) if row else None
        finally:
            cur.close()


def emergency_stop_user(user_id: int) -> None:
    set_policy(user_id, "*", "*", {"mode": "EMERGENCY_STOP", "confirm_mode": "EMERGENCY_STOP"})

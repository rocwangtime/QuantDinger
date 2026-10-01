"""Immutable Agent proposals and human-only, fail-closed trading policies."""

from __future__ import annotations

from flask import request

from app.services.agent_trade_intents import (
    IntentError, account_scope, cancel_proposal, get_intent, get_policy,
    list_intents, set_policy, submit_intent,
)
from app.utils.agent_auth import (
    SCOPE_R, SCOPE_T, agent_required, current_token, current_user_id,
    ensure_agent_gateway_schema, instrument_allowed, market_allowed,
)
from app.utils.auth import admin_required, get_current_user_id, login_required

from . import agent_v1_bp
from ._helpers import envelope, error, get_json_or_400


def submit_trade_intent_request(body: dict, *, require_paper_execution: bool = False):
    """Shared implementation for the new endpoint and legacy quick-trade alias."""
    try:
        from app.services.agent_trade_intents import normalize_order
        from .quick_trade import _last_price

        order = normalize_order(body)
        if not market_allowed(order["market"]) or not instrument_allowed(order["symbol"]):
            return error(403, "Market or instrument is not allowed for this token", http=403)
        result = submit_intent(
            current_user_id(), current_token(), body,
            (request.headers.get("Idempotency-Key") or "").strip(),
            quote_provider=_last_price,
            require_paper_execution=require_paper_execution,
        )
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)
    return envelope(result, message="trade-intent", status=201)


@agent_v1_bp.route("/trade-intents", methods=["POST"])
@agent_required(SCOPE_T)
def create_trade_intent():
    body, err = get_json_or_400()
    return err if err else submit_trade_intent_request(body)


@agent_v1_bp.route("/paper-orders/place", methods=["POST"])
@agent_required(SCOPE_T)
def place_platform_paper_order():
    """Explicit direct-order tool for internal paper; never reaches OpenD."""
    body, err = get_json_or_400()
    if err:
        return err
    if body.get("broker", "platform") != "platform" or body.get("credential_id"):
        return error(403, "This endpoint is for internal platform paper only", http=403)
    return submit_trade_intent_request(body, require_paper_execution=True)


@agent_v1_bp.route("/simulate-orders/place", methods=["POST"])
@agent_required(SCOPE_T)
def place_futu_simulate_order():
    """Explicit one-call direct order, under human policy and operator arm."""
    body, err = get_json_or_400()
    if err:
        return err
    try:
        from app.services.agent_trade_intents import normalize_order
        from app.services.futu_agent_execution import execute_simulate_intent

        order = normalize_order(body)
        if order["broker"] != "futu":
            return error(403, "This endpoint is for Futu SIMULATE only", http=403)
        if not market_allowed(order["market"]) or not instrument_allowed(order["symbol"]):
            return error(403, "Market or instrument is not allowed for this token", http=403)
        proposal = submit_intent(
            current_user_id(), current_token(), body,
            (request.headers.get("Idempotency-Key") or "").strip(),
        )
        result = execute_simulate_intent(current_user_id(), current_token(), int(proposal["id"]))
        return envelope(result, message="futu-simulate-order", status=201)
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)


@agent_v1_bp.route("/trade-intents/<int:intent_id>/execute-simulate", methods=["POST"])
@agent_required(SCOPE_T)
def agent_execute_futu_intent(intent_id: int):
    try:
        from app.services.futu_agent_execution import execute_simulate_intent

        row = get_intent(current_user_id(), intent_id)
        if row is None:
            return error(404, "Trade intent not found", http=404)
        order = row["order_spec"]
        if not market_allowed(order["market"]) or not instrument_allowed(order["symbol"]):
            return error(403, "Market or instrument is not allowed for this token", http=403)
        return envelope(execute_simulate_intent(current_user_id(), current_token(), intent_id))
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)


@agent_v1_bp.route("/trade-intents/<int:intent_id>/reconcile-simulate", methods=["POST"])
@agent_required(SCOPE_T)
def agent_reconcile_futu_intent(intent_id: int):
    try:
        from app.services.futu_agent_execution import reconcile_simulate_intent

        row = get_intent(current_user_id(), intent_id)
        if row is None or int(row.get("agent_token_id") or 0) != int(current_token()["id"]):
            return error(404, "Trade intent not found", http=404)
        return envelope(reconcile_simulate_intent(current_user_id(), intent_id))
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)


@agent_v1_bp.route("/trade-intents", methods=["GET"])
@agent_required(SCOPE_R)
def agent_list_trade_intents():
    return envelope(list_intents(current_user_id()))


@agent_v1_bp.route("/trade-intents/<int:intent_id>", methods=["GET"])
@agent_required(SCOPE_R)
def agent_get_trade_intent(intent_id: int):
    row = get_intent(current_user_id(), intent_id)
    return envelope(row) if row else error(404, "Trade intent not found", http=404)


@agent_v1_bp.route("/trade-intents/<int:intent_id>/cancel", methods=["POST"])
@agent_required(SCOPE_T)
def agent_cancel_trade_intent(intent_id: int):
    row = cancel_proposal(current_user_id(), intent_id)
    return envelope(row) if row else error(409, "Only a proposed intent can be cancelled", http=409)


@agent_v1_bp.route("/agent-orders/cancel", methods=["POST"])
@agent_required(SCOPE_T)
def agent_cancel_open_orders():
    body = request.get_json(silent=True) or {}
    if body.get("confirm") is not True:
        return error(400, "confirm=true is required")
    from .quick_trade import cancel_agent_orders

    return envelope(cancel_agent_orders(current_user_id()), message="cancel-agent-orders")


@agent_v1_bp.route("/trading-policy", methods=["GET"])
@agent_required(SCOPE_R)
def agent_read_trading_policy():
    broker = (request.args.get("broker") or "platform").strip().lower()
    account_ref = (request.args.get("account_ref") or "default").strip()
    if broker not in {"platform", "futu"}:
        return error(400, "Unsupported broker")
    if broker == "futu":
        try:
            credential_id = int(account_ref.removeprefix("credential:"))
            _, account_ref = account_scope(current_user_id(), {
                "broker": "futu", "credential_id": credential_id,
            })
        except (ValueError, IntentError):
            return error(403, "Futu credential is not available", http=403)
    return envelope(get_policy(current_user_id(), broker, account_ref))


def _human_scope(user_id: int, broker: str, account_ref: str) -> tuple[str, str]:
    if broker == "platform" and account_ref == "default":
        return broker, account_ref
    if broker == "futu":
        try:
            credential_id = int(account_ref.removeprefix("credential:"))
        except (TypeError, ValueError) as exc:
            raise IntentError("Invalid Futu credential reference") from exc
        return account_scope(user_id, {"broker": "futu", "credential_id": credential_id})
    if broker == "*" and account_ref == "*":
        return broker, account_ref
    raise IntentError("Unsupported policy account")


@agent_v1_bp.route("/admin/trading-policy", methods=["GET"])
@login_required
@admin_required
def human_read_trading_policy():
    ensure_agent_gateway_schema()
    user_id = int(get_current_user_id())
    try:
        broker, account_ref = _human_scope(
            user_id,
            str(request.args.get("broker") or "platform").strip().lower(),
            str(request.args.get("account_ref") or "default").strip(),
        )
        return envelope(get_policy(user_id, broker, account_ref))
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)


@agent_v1_bp.route("/admin/trading-policy", methods=["PUT"])
@login_required
@admin_required
def human_set_trading_policy():
    ensure_agent_gateway_schema()
    body, err = get_json_or_400()
    if err:
        return err
    user_id = int(get_current_user_id())
    try:
        broker, account_ref = _human_scope(
            user_id, str(body.get("broker") or "platform").strip().lower(),
            str(body.get("account_ref") or "default").strip(),
        )
        return envelope(set_policy(user_id, broker, account_ref, body), message="policy-updated")
    except IntentError as exc:
        return error(exc.status, str(exc), http=exc.status)


@agent_v1_bp.route("/admin/trade-intents", methods=["GET"])
@login_required
@admin_required
def human_list_trade_intents():
    ensure_agent_gateway_schema()
    return envelope(list_intents(int(get_current_user_id())))


@agent_v1_bp.route("/admin/agent-orders/cancel", methods=["POST"])
@login_required
@admin_required
def human_cancel_open_agent_orders():
    body, err = get_json_or_400()
    if err:
        return err
    if body.get("confirm") != "CANCEL_AGENT_ORDERS":
        return error(400, "Type CANCEL_AGENT_ORDERS in confirm")
    from .quick_trade import cancel_agent_orders

    return envelope(cancel_agent_orders(int(get_current_user_id())), message="cancel-agent-orders")

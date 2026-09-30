"""Opt-in, tiered first-party DeepSeek/OpenAI Agent API."""

from __future__ import annotations

import os

from flask import request

from app.services.model_trading_agent import (
    ModelAgentError, provider_status, run_model_agent,
)
from app.utils.agent_auth import SCOPE_R, SCOPE_T, agent_required, current_token, current_user_id

from . import agent_v1_bp
from ._helpers import envelope, error, get_json_or_400


@agent_v1_bp.route("/model-agent/providers", methods=["GET"])
@agent_required(SCOPE_R)
def model_agent_providers():
    return envelope({
        "enabled": os.getenv("AGENT_MODEL_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
        "providers": provider_status(),
        "tiers": ["observe", "plan", "paper"],
    })


def _run(tier: str):
    body, err = get_json_or_400()
    if err:
        return err
    if set(body) - {"goal", "provider"}:
        return error(400, "Only goal and provider are accepted")
    if not isinstance(body.get("goal"), str):
        return error(400, "goal must be a string")
    if body.get("provider") is not None and not isinstance(body["provider"], str):
        return error(400, "provider must be a string")
    try:
        result = run_model_agent(
            goal=body["goal"].strip(), tier=tier, provider=body.get("provider"),
            user_id=current_user_id(), token=current_token(),
            idempotency_key=(request.headers.get("Idempotency-Key") or "").strip(),
        )
        return envelope(result, message="model-agent-run")
    except ModelAgentError as exc:
        return error(exc.status, str(exc), details=exc.receipt, http=exc.status)


@agent_v1_bp.route("/model-agent/observe", methods=["POST"])
@agent_required(SCOPE_R)
def model_agent_observe():
    return _run("observe")


@agent_v1_bp.route("/model-agent/plan", methods=["POST"])
@agent_required(SCOPE_T)
def model_agent_plan():
    return _run("plan")


@agent_v1_bp.route("/model-agent/paper", methods=["POST"])
@agent_required(SCOPE_T)
def model_agent_paper():
    return _run("paper")

"""Canonical strategy deployment and lifecycle routes."""

from __future__ import annotations

import os
import json
import re
import time
from typing import Any

from flask import Response, g, jsonify, request, stream_with_context
from app.services.ai_generation_control import generation_cancelled, valid_request_id
from app.services.llm_selection import agent_model_selection

from app import get_trading_executor
from app.routes.strategy_blueprint import strategy_blp
from app.routes.strategy_services import get_strategy_service
from app.services.ai_generation_contracts import (
    SCRIPT_STRATEGY_REPAIR_REQUIREMENTS,
)
from app.services.ai_copilot_context import fit_messages_to_budget
from app.services.llm_cost import aggregate_usage_display
from app.services.ai_authoring_intent import resolve_authoring_intent
from app.services.ai_code_edits import (
    CODE_EDIT_SYSTEM_SUFFIX,
    CodeEditError,
    apply_model_code_edits,
)
from app.services.strategy_ai_generation import (
    apply_deterministic_strategy_edit,
    build_strategy_generation_request,
    build_strategy_system_prompt,
    resolve_strategy_validation_intent,
    select_strategy_system_prompt,
    validate_generated_strategy,
)
from app.services.strategy_ai_capabilities import (
    StrategyAIGenerationIntent,
    render_strategy_capability_contract,
    render_strategy_capability_repairs,
    resolve_strategy_generation_intent,
)
from app.services.strategy_ai_behavior import validate_strategy_ai_behavior
from app.services.strategy_ai_workspace import (
    begin_strategy_ai_turn,
    classify_strategy_ai_intent,
    clear_strategy_ai_workspace,
    complete_strategy_candidate_turn,
    complete_strategy_discussion_turn,
    get_strategy_ai_workspace,
    normalize_asset_type,
    set_strategy_ai_change_status,
)
from app.services.strategy import redact_strategy_row
from app.services.strategy_daily_pnl import load_strategy_daily_metrics
from app.services.strategy_runtime.bot_type import resolve_bot_type
from app.services.strategy_runtime.health import load_runtime_health
from app.services.strategy_v2 import compile_strategy_v2
from app.utils.auth import login_required
from app.utils.logger import get_logger


logger = get_logger(__name__)


STRATEGY_CANDIDATE_MESSAGE_KEY = "candidate_generated_validated"


def _request_lang(default: str = "zh-CN") -> str:
    raw = request.headers.get("X-App-Lang") or request.headers.get("Accept-Language") or default
    lang = str(raw or default).split(",", 1)[0].strip()
    return lang or default


def _strategy_ai_text(key: str, lang: str = "zh-CN") -> str:
    texts = {
        STRATEGY_CANDIDATE_MESSAGE_KEY: (
            "Candidate generated and validated against the current Strategy API V2 workspace contract."
        ),
    }
    if str(lang or "zh-CN").strip().lower().startswith("zh"):
        zh_texts = {
            STRATEGY_CANDIDATE_MESSAGE_KEY: "策略候选已生成，并已通过当前 Strategy API V2 工作区契约检查。",
        }
        return zh_texts.get(key, texts.get(key, key))
    return texts.get(key, key)

# Split route modules share this blueprint.
from app.routes import script_source_routes  # noqa: E402,F401
from app.routes import strategy_account_routes  # noqa: E402,F401
from app.routes import strategy_ai_decision_routes  # noqa: E402,F401
from app.routes import strategy_asset_routes  # noqa: E402,F401
from app.routes import strategy_deviation_routes  # noqa: E402,F401
from app.routes import strategy_executor_routes  # noqa: E402,F401
from app.routes import strategy_grid_routes  # noqa: E402,F401
from app.routes import strategy_ledger_routes  # noqa: E402,F401
from app.routes import strategy_logs_routes  # noqa: E402,F401
from app.routes import strategy_notifications  # noqa: E402,F401
from app.routes import strategy_positions_routes  # noqa: E402,F401
from app.routes import strategy_position_ownership_routes  # noqa: E402,F401
from app.routes import strategy_review_routes  # noqa: E402,F401


def _ok(data: Any = None, message: str = "common.success"):
    return jsonify({"code": 1, "msg": message, "data": data})


def _error(message: str, status: int = 400, data: Any = None):
    return jsonify({"code": 0, "msg": message, "data": data}), status


def _strategy(strategy_id: int):
    return get_strategy_service().get_strategy(int(strategy_id), user_id=int(g.user_id))


def _attach_runtime_health(rows, *, user_id: int | None = None, client_timezone: str = ""):
    items = [dict(row) for row in (rows or [])]
    statuses = {
        int(row.get("id") or 0): str(row.get("status") or "")
        for row in items
        if int(row.get("id") or 0) > 0
    }
    health = load_runtime_health(statuses, strategy_statuses=statuses)
    for row in items:
        runtime_health = health.get(int(row.get("id") or 0), {})
        row["runtime_health"] = runtime_health
        resolved_bot_type = resolve_bot_type(row)
        if not resolved_bot_type and str(runtime_health.get("trigger_mode") or "").strip().lower() == "exchange_resting_orders":
            resolved_bot_type = "grid"
        if resolved_bot_type:
            row["resolved_bot_type"] = resolved_bot_type
    if user_id is not None:
        metrics = load_strategy_daily_metrics(
            items,
            user_id=int(user_id),
            client_timezone=str(client_timezone or ""),
        )
        for row in items:
            row.update(metrics.get(int(row.get("id") or 0), {}))
    return items


@strategy_blp.route("/strategies", methods=["GET"])
@login_required
def list_strategies():
    user_id = int(g.user_id)
    rows = get_strategy_service().list_strategies(user_id=user_id)
    enriched = _attach_runtime_health(
        rows,
        user_id=user_id,
        client_timezone=request.headers.get("X-App-Timezone", ""),
    )
    return _ok([redact_strategy_row(row) for row in enriched])


@strategy_blp.route("/strategies/<int:strategy_id>", methods=["GET"])
@login_required
def get_strategy(strategy_id: int):
    row = _strategy(strategy_id)
    if not row:
        return _error("strategyV2.strategyNotFound", 404)
    return _ok(redact_strategy_row(_attach_runtime_health(
        [row],
        user_id=int(g.user_id),
        client_timezone=request.headers.get("X-App-Timezone", ""),
    )[0]))


@strategy_blp.route("/strategies", methods=["POST"])
@login_required
def create_strategy():
    try:
        payload = dict(request.get_json() or {})
        payload["user_id"] = int(g.user_id)
        strategy_id = get_strategy_service().create_strategy(payload)
        return _ok({"id": strategy_id}, "strategyV2.created")
    except Exception as exc:
        logger.warning("strategy create failed: %s", exc)
        return _error(str(exc))


@strategy_blp.route("/strategies/<int:strategy_id>", methods=["PUT"])
@login_required
def update_strategy(strategy_id: int):
    try:
        changed = get_strategy_service().update_strategy(
            strategy_id,
            dict(request.get_json() or {}),
            user_id=int(g.user_id),
        )
        if not changed:
            return _error("strategyV2.strategyNotFound", 404)
        return _ok({"id": strategy_id}, "strategyV2.updated")
    except Exception as exc:
        logger.warning("strategy update failed: %s", exc)
        return _error(str(exc))


@strategy_blp.route("/strategies/<int:strategy_id>", methods=["DELETE"])
@login_required
def delete_strategy(strategy_id: int):
    if get_trading_executor().is_running(strategy_id):
        return _error("strategyV2.stopBeforeDelete", 409)
    if not get_strategy_service().delete_strategy(strategy_id, user_id=int(g.user_id)):
        return _error("strategyV2.strategyNotFound", 404)
    return _ok({"id": strategy_id}, "strategyV2.deleted")


@strategy_blp.route("/strategies/<int:strategy_id>/start", methods=["POST"])
@login_required
def start_strategy(strategy_id: int):
    row = _strategy(strategy_id)
    if not row:
        return _error("strategyV2.strategyNotFound", 404)
    service = get_strategy_service()
    if not service.update_strategy_status(strategy_id, "running", user_id=int(g.user_id)):
        return _error("strategyV2.strategyNotFound", 404)
    executor = get_trading_executor()
    if executor.start_strategy(strategy_id):
        timeout = max(0.0, float(os.getenv("STRATEGY_COMMAND_START_WAIT_SEC", "8")))
        running, detail = executor.wait_strategy_running(strategy_id, timeout=timeout)
        if running and detail == "strategyV2.startQueued":
            return _ok({"id": strategy_id, "status": "starting"}, detail), 202
        if running:
            return _ok({"id": strategy_id, "status": "running"}, "strategyV2.started")
        service.update_strategy_status(strategy_id, "stopped", user_id=int(g.user_id))
        return _error(detail or "strategyV2.startFailed", 409)
    service.update_strategy_status(strategy_id, "stopped", user_id=int(g.user_id))
    detail = str(getattr(executor, "_last_start_failure", "") or "")
    return _error(detail or "strategyV2.startFailed", 409)


@strategy_blp.route("/strategies/<int:strategy_id>/stop", methods=["POST"])
@login_required
def stop_strategy(strategy_id: int):
    row = _strategy(strategy_id)
    if not row:
        return _error("strategyV2.strategyNotFound", 404)
    payload = dict(request.get_json(silent=True) or {})
    close_positions = bool(
        payload.get("close_positions")
        or payload.get("closePositions")
        or str(payload.get("mode") or "").strip().lower() in {"close", "flatten", "stop_and_close"}
    )
    result = get_trading_executor().stop_strategy_with_policy(
        strategy_id,
        close_positions=close_positions,
    )
    status = str(result.get("status") or "")
    if status == "stopped":
        get_strategy_service().update_strategy_status(strategy_id, "stopped", user_id=int(g.user_id))
    data = {"id": strategy_id, **result}
    if not result.get("success"):
        message = "strategyV2.stopClosePartialFailure" if close_positions and status == "stopped" else "strategyV2.stopFailed"
        return _error(message, 409, data=data)
    if status == "stopping":
        return _ok(data, "strategyV2.stopQueued"), 202
    completed = int(result.get("close_orders_completed") or 0)
    queued = int(result.get("close_orders_queued") or 0)
    if close_positions and completed > 0 and completed == queued:
        message = "strategyV2.stoppedAndVirtualCloseCompleted"
    else:
        message = "strategyV2.stoppedAndCloseQueued" if close_positions else "strategyV2.paused"
    return _ok(data, message)


@strategy_blp.route("/strategies/exchange/test", methods=["POST"])
@login_required
def test_exchange_connection():
    result = get_strategy_service().test_exchange_connection(
        dict(request.get_json() or {}),
        user_id=int(g.user_id),
    )
    if result.get("success"):
        return _ok(result.get("data"), str(result.get("message") or "strategyV2.connectionOk"))
    return _error(str(result.get("message") or "strategyV2.connectionFailed"))


@strategy_blp.route("/strategies/verify", methods=["POST"])
@login_required
def verify_strategy():
    code = str((request.get_json() or {}).get("code") or "").strip()
    if not code:
        return _error("strategyV2.codeRequired")
    try:
        program = compile_strategy_v2(code)
        from app.services.strategy_marketplace_contract import derive_strategy_contract
        return _ok({
            "valid": True,
            "manifest": program.manifest.metadata(),
            "marketplace_contract": derive_strategy_contract(code, source="draft_verification"),
        })
    except Exception as exc:
        return _error("strategyV2.contractInvalid", data={"valid": False, "error": str(exc)})


@strategy_blp.route("/strategies/generate", methods=["POST"])
@login_required
@agent_model_selection
def generate_strategy():
    payload = dict(request.get_json() or {})
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        return _error("strategyV2.promptRequired")
    try:
        from app.services.billing_service import get_billing_service
        from app.services.llm import LLMService

        llm = LLMService()
        if not llm.is_configured():
            return _error("strategyV2.llmNotConfigured")
        asset_type = normalize_asset_type(payload.get("assetType") or payload.get("asset_type"))
        generation_mode = str(payload.get("generationMode") or "authoring").strip().lower()
        context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        existing_code = str(payload.get("existingCode") or "")
        system_prompt, generation_intent = build_strategy_system_prompt(
            prompt=prompt,
            asset_type=asset_type,
            existing_code=existing_code,
            generation_mode=generation_mode,
            context=context,
        )
        use_patch_response = bool(existing_code.strip())
        user_prompt = build_strategy_generation_request(
            prompt=prompt,
            asset_type=asset_type,
            existing_code=existing_code,
            generation_mode=generation_mode,
            context=context,
            response_mode="patch" if use_patch_response else "full",
        )
        full_system_prompt = system_prompt
        if use_patch_response:
            system_prompt = f"{full_system_prompt}\n\n{CODE_EDIT_SYSTEM_SUFFIX}"
        accepted, message = get_billing_service().check_and_consume(
            user_id=int(g.user_id),
            feature="ai_code_gen",
            reference_id=f"strategy_generate_{int(g.user_id)}_{int(time.time())}",
        )
        if not accepted:
            return _error(message or "strategyV2.insufficientCredits", 402)
        content = llm.call_llm_api(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            model=llm.get_code_generation_model(),
            temperature=0.2 if use_patch_response else 0.4,
            use_json_mode=use_patch_response,
        )
        if use_patch_response:
            try:
                code, edit_plan = apply_model_code_edits(existing_code, content)
            except CodeEditError as exc:
                logger.warning("strategy model patch rejected, retrying full candidate: %s", exc)
                full_request = build_strategy_generation_request(
                    prompt=prompt,
                    asset_type=asset_type,
                    existing_code=existing_code,
                    generation_mode=generation_mode,
                    context=context,
                    response_mode="full",
                )
                fallback = llm.call_llm_api(
                    messages=[
                        {"role": "system", "content": full_system_prompt},
                        {"role": "user", "content": full_request},
                    ],
                    model=llm.get_code_generation_model(),
                    temperature=0.25,
                    use_json_mode=False,
                )
                code = _strip_code_fence(str(fallback or ""))
                edit_plan = {
                    "executor": "model_full_fallback",
                    "operation": "generate_candidate",
                    "patch_error": str(exc),
                }
        else:
            code = _strip_code_fence(str(content or ""))
            edit_plan = {"executor": "model", "operation": "generate_candidate"}
        candidate_before_validation = code
        validation_intent = resolve_strategy_validation_intent(
            prompt=prompt,
            existing_code=existing_code,
            context=context,
        )
        code, program, behavior_validation = _compile_or_repair_generated_strategy(
            llm,
            user_prompt,
            code,
            asset_type=asset_type,
            generation_mode=generation_mode,
            context=context,
            system_prompt=full_system_prompt,
            intent=validation_intent,
        )
        if code != candidate_before_validation and edit_plan.get("executor") == "model_patch":
            edit_plan = {
                "executor": "model_patch_repaired",
                "operation": "generate_candidate",
                "patch_operation_count": edit_plan.get("operation_count", 0),
            }
        return _ok({
            "code": code,
            "llm_usage": aggregate_usage_display(getattr(llm, "usage_events", [])),
            "manifest": program.manifest.metadata(),
            "validation": {
                "success": True,
                "behavior": behavior_validation,
                "edit_plan": edit_plan,
            },
        })
    except Exception as exc:
        logger.warning("strategy generation failed: %s", exc)
        return _error("strategyV2.generationInvalid", data={"error": str(exc), "llm_usage": aggregate_usage_display(getattr(locals().get("llm"), "usage_events", []))})


def _strategy_sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _stream_strategy_completion(llm, messages: list[dict], *, temperature: float,
                                user_id: int, request_id: str, phase: str):
    """Yield source as it arrives; never continue a cancelled provider call."""
    parts: list[str] = []
    provider_stream = llm.stream_llm_api(
        messages, model=llm.get_code_generation_model(), temperature=temperature,
    )
    try:
        for chunk in provider_stream:
            if generation_cancelled(user_id, request_id):
                return None
            if not chunk:
                continue
            parts.append(str(chunk))
            yield _strategy_sse("delta", {"phase": phase, "text": str(chunk)})
    finally:
        close = getattr(provider_stream, "close", None)
        if close:
            close()
    return None if generation_cancelled(user_id, request_id) else "".join(parts)


def _stream_validate_strategy(llm, prompt: str, code: str, *, asset_type: str,
                              generation_mode: str, context: dict, system_prompt: str,
                              intent: StrategyAIGenerationIntent, user_id: int,
                              request_id: str, max_repair_attempts: int = 2):
    candidate = code
    for attempt in range(max(0, min(int(max_repair_attempts), 3)) + 1):
        if generation_cancelled(user_id, request_id):
            return None
        try:
            program = validate_generated_strategy(
                candidate, asset_type=asset_type, generation_mode=generation_mode,
                context=context, prompt=prompt, intent=intent, compiler=compile_strategy_v2,
            )
            behavior = validate_strategy_ai_behavior(candidate, program.manifest, intent)
            return candidate, program, behavior
        except Exception as validation_error:
            if attempt >= max_repair_attempts:
                raise
            yield _strategy_sse("progress", {"phase": "repair", "attempt": attempt + 1})
            repair_prompt = "\n\n".join([
                SCRIPT_STRATEGY_REPAIR_REQUIREMENTS,
                render_strategy_capability_repairs(intent),
                f"Original user request:\n{prompt}",
                f"Validation error to repair now:\n{validation_error}",
                f"Invalid generated source:\n{candidate}",
                "Repair the source and return the complete Python source only.",
            ])
            repaired = yield from _stream_strategy_completion(
                llm, [{"role": "system", "content": system_prompt},
                      {"role": "user", "content": repair_prompt}],
                temperature=0.15, user_id=user_id, request_id=request_id,
                phase="repair",
            )
            if repaired is None:
                return None
            candidate = _strip_code_fence(repaired)
    raise RuntimeError("strategyV2.generationRepairExhausted")


@strategy_blp.route("/strategies/generate/stream", methods=["POST"])
@login_required
@agent_model_selection
def generate_strategy_stream():
    """SSE strategy draft, validation, and explicit cancellation for interactive use."""
    payload = dict(request.get_json(silent=True) or {})
    prompt = str(payload.get("prompt") or "").strip()
    request_id = valid_request_id(payload.get("request_id"))
    if not prompt:
        return _error("strategyV2.promptRequired")
    if not request_id:
        return _error("strategyV2.requestIdRequired", 400)
    user_id = int(g.user_id)

    @stream_with_context
    def generate():
        llm = None
        try:
            from app.services.billing_service import get_billing_service
            from app.services.llm import LLMService

            yield _strategy_sse("accepted", {"request_id": request_id})
            if generation_cancelled(user_id, request_id):
                yield _strategy_sse("cancelled", {})
                return
            llm = LLMService()
            if not llm.is_configured():
                yield _strategy_sse("error", {"msg": "strategyV2.llmNotConfigured"})
                return
            asset_type = normalize_asset_type(payload.get("assetType") or payload.get("asset_type"))
            generation_mode = str(payload.get("generationMode") or "authoring").strip().lower()
            context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
            existing_code = str(payload.get("existingCode") or "")
            system_prompt, _ = build_strategy_system_prompt(
                prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                generation_mode=generation_mode, context=context,
            )
            full_system_prompt = system_prompt
            use_patch_response = bool(existing_code.strip())
            if use_patch_response:
                system_prompt = f"{full_system_prompt}\n\n{CODE_EDIT_SYSTEM_SUFFIX}"
            user_prompt = build_strategy_generation_request(
                prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                generation_mode=generation_mode, context=context,
                response_mode="patch" if use_patch_response else "full",
            )
            accepted, message = get_billing_service().check_and_consume(
                user_id=user_id, feature="ai_code_gen",
                reference_id=f"strategy_generate_{user_id}_{request_id}",
            )
            if not accepted:
                yield _strategy_sse("error", {"msg": message or "strategyV2.insufficientCredits"})
                return
            yield _strategy_sse("progress", {"phase": "generation"})
            content = yield from _stream_strategy_completion(
                llm, [{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt}],
                temperature=0.2 if use_patch_response else 0.4,
                user_id=user_id, request_id=request_id, phase="generation",
            )
            if content is None:
                yield _strategy_sse("cancelled", {})
                return
            if use_patch_response:
                try:
                    code, edit_plan = apply_model_code_edits(existing_code, content)
                except CodeEditError:
                    yield _strategy_sse("progress", {"phase": "full_fallback"})
                    full_request = build_strategy_generation_request(
                        prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                        generation_mode=generation_mode, context=context, response_mode="full",
                    )
                    fallback = yield from _stream_strategy_completion(
                        llm, [{"role": "system", "content": full_system_prompt},
                              {"role": "user", "content": full_request}],
                        temperature=0.25, user_id=user_id, request_id=request_id,
                        phase="full_fallback",
                    )
                    if fallback is None:
                        yield _strategy_sse("cancelled", {})
                        return
                    code = _strip_code_fence(fallback)
                    edit_plan = {"executor": "model_full_fallback", "operation": "generate_candidate"}
            else:
                code = _strip_code_fence(content)
                edit_plan = {"executor": "model", "operation": "generate_candidate"}
            candidate_before_validation = code
            validation_intent = resolve_strategy_validation_intent(
                prompt=prompt, existing_code=existing_code, context=context,
            )
            yield _strategy_sse("progress", {"phase": "validation"})
            validated = yield from _stream_validate_strategy(
                llm, user_prompt, code, asset_type=asset_type,
                generation_mode=generation_mode, context=context,
                system_prompt=full_system_prompt, intent=validation_intent,
                user_id=user_id, request_id=request_id,
            )
            if validated is None:
                yield _strategy_sse("cancelled", {})
                return
            code, program, behavior_validation = validated
            if code != candidate_before_validation and edit_plan.get("executor") == "model_patch":
                edit_plan = {"executor": "model_patch_repaired", "operation": "generate_candidate"}
            yield _strategy_sse("done", {"data": {
                "code": code,
                "llm_usage": aggregate_usage_display(getattr(llm, "usage_events", [])),
                "manifest": program.manifest.metadata(),
                "validation": {"success": True, "behavior": behavior_validation,
                               "edit_plan": edit_plan},
            }})
        except Exception as exc:
            logger.warning("streamed strategy generation failed: %s", type(exc).__name__, exc_info=True)
            yield _strategy_sse("error", {"msg": "strategyV2.generationInvalid"})

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no",
    })


def _strip_code_fence(value: str) -> str:
    text = str(value or "").strip()
    blocks = re.findall(r"```(?:python|py)\s*\n(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if len(blocks) == 1:
        return blocks[0].strip()
    text = re.sub(r"^```(?:python)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _compile_or_repair_generated_strategy(
    llm,
    prompt: str,
    code: str,
    *,
    asset_type: str = "script",
    generation_mode: str = "authoring",
    context: dict | None = None,
    system_prompt: str | None = None,
    intent: StrategyAIGenerationIntent | None = None,
    max_repair_attempts: int = 2,
):
    selected_system_prompt = system_prompt or select_strategy_system_prompt(asset_type, generation_mode)
    resolved_intent = intent or resolve_strategy_generation_intent(
        prompt=prompt,
        context=context,
    )
    candidate = code
    attempts = max(0, min(int(max_repair_attempts), 3))
    for attempt in range(attempts + 1):
        try:
            program = validate_generated_strategy(
                candidate,
                asset_type=asset_type,
                generation_mode=generation_mode,
                context=context,
                prompt=prompt,
                intent=resolved_intent,
                compiler=compile_strategy_v2,
            )
            behavior_validation = validate_strategy_ai_behavior(
                candidate, program.manifest, resolved_intent
            )
            return candidate, program, behavior_validation
        except Exception as validation_error:
            if attempt >= attempts:
                raise
            logger.info(
                "repairing invalid generated strategy attempt=%s/%s: %s",
                attempt + 1,
                attempts,
                validation_error,
            )
            capability_repairs = render_strategy_capability_repairs(resolved_intent)
            repair_prompt = "\n\n".join(
                [
                    SCRIPT_STRATEGY_REPAIR_REQUIREMENTS,
                    capability_repairs,
                    f"Original user request:\n{prompt}",
                    f"Validation error to repair now:\n{validation_error}",
                    f"Invalid generated source:\n{candidate}",
                    "Repair the source and return the complete Python source only.",
                ]
            )
            repaired_content = llm.call_llm_api(
                messages=[
                    {"role": "system", "content": selected_system_prompt},
                    {"role": "user", "content": repair_prompt},
                ],
                model=llm.get_code_generation_model(),
                temperature=0.15,
                use_json_mode=False,
            )
            candidate = _strip_code_fence(str(repaired_content or ""))

    raise RuntimeError("strategyV2.generationRepairExhausted")


def _strategy_ai_billing_feature(intent: str) -> str:
    """Match the indicator IDE tariff: chat is cheap, code changes use code-gen."""
    return "ai_copilot_chat" if str(intent or "").strip().lower() == "discussion" else "ai_code_gen"


def _consume_strategy_ai_credit(user_id: int, reference: str, feature: str):
    from app.services.billing_service import get_billing_service

    return get_billing_service().check_and_consume(
        user_id=int(user_id),
        feature=feature,
        reference_id=reference,
    )


def _strategy_ai_billing_meta(user_id: int, feature: str, consume_status: str) -> dict:
    """Return enough billing state for the header balance to refresh immediately."""
    from app.services.billing_service import get_billing_service

    billing = get_billing_service()
    charged = billing.get_feature_cost(feature) if consume_status == "consumed" else 0
    return {
        "feature": feature,
        "credits_charged": int(charged or 0),
        "remaining_credits": float(billing.get_user_credits(int(user_id))),
    }


@strategy_blp.route("/strategies/ai-workspace/<int:source_id>", methods=["GET"])
@login_required
def get_strategy_workspace(source_id: int):
    try:
        asset_type = request.args.get("assetType") or request.args.get("asset_type")
        return _ok(get_strategy_ai_workspace(g.user_id, source_id, asset_type))
    except LookupError as exc:
        return _error(str(exc), 404)
    except PermissionError as exc:
        return _error(str(exc), 403)
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.error("get strategy AI workspace failed: %s", exc, exc_info=True)
        return _error(str(exc), 500)


@strategy_blp.route("/strategies/ai-workspace/<int:source_id>", methods=["DELETE"])
@login_required
def delete_strategy_workspace(source_id: int):
    try:
        asset_type = request.args.get("assetType") or request.args.get("asset_type")
        return _ok(clear_strategy_ai_workspace(g.user_id, source_id, asset_type))
    except LookupError as exc:
        return _error(str(exc), 404)
    except PermissionError as exc:
        return _error(str(exc), 403)
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.error("clear strategy AI workspace failed: %s", exc, exc_info=True)
        return _error(str(exc), 500)


@strategy_blp.route("/strategies/ai-workspace/changes/<int:change_id>/status", methods=["POST"])
@login_required
def update_strategy_workspace_change(change_id: int):
    try:
        status = str((request.get_json() or {}).get("status") or "").strip().lower()
        return _ok(set_strategy_ai_change_status(g.user_id, change_id, status))
    except LookupError as exc:
        return _error(str(exc), 404)
    except PermissionError as exc:
        return _error(str(exc), 403)
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.error("update strategy AI candidate failed: %s", exc, exc_info=True)
        return _error(str(exc), 500)


@strategy_blp.route("/strategies/ai-workspace/turn", methods=["POST"])
@login_required
@agent_model_selection
def run_strategy_workspace_turn():
    payload = dict(request.get_json() or {})
    lang = _request_lang()
    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        return _error("strategyV2.promptRequired")
    try:
        from app.services.llm import LLMService

        user_id = int(g.user_id)
        asset_type = normalize_asset_type(payload.get("assetType") or payload.get("asset_type"))
        source_id = int(payload.get("sourceId") or payload.get("source_id") or 0)
        existing_code = str(payload.get("existingCode") or "")
        context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        requested_mode = str(payload.get("interactionMode") or "auto")
        generation_mode = str(payload.get("generationMode") or "authoring").strip().lower()
        llm = LLMService()
        if not llm.is_configured():
            return _error("strategyV2.llmNotConfigured")
        intent_workspace = None
        if source_id:
            # Validate ownership and source visibility before charging. Creating
            # an empty thread is harmless; a failed billing check must not add
            # a dangling user message to the conversation.
            intent_workspace = get_strategy_ai_workspace(user_id, source_id, asset_type)
        intent_decision = resolve_authoring_intent(
            prompt=prompt,
            requested_mode=requested_mode,
            asset_kind="portfolio_strategy" if asset_type == "portfolio_strategy" else "cta_strategy",
            existing_code=existing_code,
            recent_messages=(intent_workspace or {}).get("messages") or [],
            fallback_classifier=classify_strategy_ai_intent,
            llm=llm,
        )
        intent = str(intent_decision["intent"])
        logger.info(
            "strategy authoring intent=%s source=%s confidence=%.2f",
            intent,
            intent_decision.get("source"),
            float(intent_decision.get("confidence") or 0.0),
        )
        billing_feature = _strategy_ai_billing_feature(intent)
        accepted, message = _consume_strategy_ai_credit(
            user_id,
            f"strategy_ai_turn_{user_id}_{source_id}_{int(time.time())}",
            billing_feature,
        )
        if not accepted:
            return _error(message or "strategyV2.insufficientCredits", 402)
        billing_meta = _strategy_ai_billing_meta(user_id, billing_feature, message)

        workspace = None
        if source_id:
            workspace = begin_strategy_ai_turn(
                user_id,
                source_id,
                prompt,
                asset_type=asset_type,
                intent=intent,
            )
            if not existing_code:
                existing_code = str(workspace["source"].get("code") or "")

        if intent == "discussion":
            discussion_intent = resolve_strategy_generation_intent(
                prompt=prompt,
                existing_code=existing_code,
                context=context,
            )
            discussion_contract = render_strategy_capability_contract(discussion_intent)
            discussion_system = (
                "You are QuantDinger's Strategy API V2 code reviewer. Answer in the user's language. "
                "Explain the current strategy's universe, market type, subscriptions, signals, sizing, risk, and limitations. "
                "Never claim code was changed and never return replacement source. Be concise and concrete. "
                "Treat the active capability contract below as authoritative platform behavior and distinguish it from behavior actually implemented by the source."
            )
            if discussion_contract:
                discussion_system = f"{discussion_system}\n\n{discussion_contract}"
            messages = [{"role": "system", "content": discussion_system}]
            if workspace:
                messages.append({
                    "role": "system",
                    "content": "# Bounded strategy memory\n" + json.dumps(workspace.get("summary") or {}, ensure_ascii=False)[:5000],
                })
                for item in workspace.get("recent_messages") or []:
                    role = str(item.get("role") or "")
                    content = str(item.get("content") or "").strip()
                    if role in {"user", "assistant"} and content:
                        messages.append({"role": role, "content": content[:2400]})
            messages.append({
                "role": "user",
                "content": f"# Current source\n{existing_code[:40000]}\n\n# Question\n{prompt}",
            })
            messages, budget = fit_messages_to_budget(messages, max_tokens=32000)
            logger.info("strategy discussion context budget=%s", budget)
            answer = str(llm.call_llm_api(
                messages=messages,
                model=llm.get_default_model(),
                temperature=0.2,
                use_json_mode=False,
            ) or "").strip()
            if workspace:
                result = complete_strategy_discussion_turn(
                    user_id=user_id,
                    workspace=workspace,
                    answer=answer,
                )
                result["billing"] = billing_meta
                result.update(billing_meta)
                return _ok(result)
            return _ok({
                "reply_type": "discussion",
                "assistant_message": {"role": "assistant", "content": answer, "message_type": "discussion"},
                "billing": billing_meta,
                **billing_meta,
            })

        system_prompt, generation_intent = build_strategy_system_prompt(
            prompt=prompt,
            asset_type=asset_type,
            existing_code=existing_code,
            generation_mode=generation_mode,
            context=context,
        )
        use_patch_response = bool(existing_code.strip())
        full_system_prompt = system_prompt
        if use_patch_response:
            system_prompt = f"{full_system_prompt}\n\n{CODE_EDIT_SYSTEM_SUFFIX}"
        user_prompt = build_strategy_generation_request(
            prompt=prompt,
            asset_type=asset_type,
            existing_code=existing_code,
            generation_mode=generation_mode,
            context=context,
            response_mode="patch" if use_patch_response else "full",
        )
        messages = [{"role": "system", "content": system_prompt}]
        if workspace:
            messages.append({
                "role": "system",
                "content": (
                    "# Bounded strategy authoring memory\n"
                    "Use memory for intent continuity only. The current source and structured constraints remain authoritative.\n"
                    + json.dumps(workspace.get("summary") or {}, ensure_ascii=False)[:5000]
                ),
            })
            for item in workspace.get("recent_messages") or []:
                role = str(item.get("role") or "")
                if role == "assistant" and str(item.get("message_type") or "") == "discussion":
                    continue
                content = str(item.get("content") or "").strip()
                if role in {"user", "assistant"} and content:
                    messages.append({"role": role, "content": content[:2400]})
        messages.append({"role": "user", "content": user_prompt})
        messages, budget = fit_messages_to_budget(messages, max_tokens=48000)
        logger.info("strategy authoring context budget=%s", budget)
        deterministic_edit = apply_deterministic_strategy_edit(
            existing_code,
            prompt,
            summary=(workspace or {}).get("summary") or {},
            recent_messages=(workspace or {}).get("recent_messages") or [],
        )
        if deterministic_edit:
            candidate_code, edit_plan = deterministic_edit
        else:
            generated = llm.call_llm_api(
                messages=messages,
                model=llm.get_code_generation_model(),
                temperature=0.2 if use_patch_response else 0.35,
                use_json_mode=use_patch_response,
            )
            if use_patch_response:
                try:
                    candidate_code, edit_plan = apply_model_code_edits(existing_code, generated)
                except CodeEditError as exc:
                    logger.warning("strategy model patch rejected, retrying full candidate: %s", exc)
                    full_request = build_strategy_generation_request(
                        prompt=prompt,
                        asset_type=asset_type,
                        existing_code=existing_code,
                        generation_mode=generation_mode,
                        context=context,
                        response_mode="full",
                    )
                    full_messages = [dict(item) for item in messages]
                    full_messages[0] = {"role": "system", "content": full_system_prompt}
                    full_messages[-1] = {"role": "user", "content": full_request}
                    fallback = llm.call_llm_api(
                        messages=full_messages,
                        model=llm.get_code_generation_model(),
                        temperature=0.25,
                        use_json_mode=False,
                    )
                    candidate_code = _strip_code_fence(str(fallback or ""))
                    edit_plan = {
                        "executor": "model_full_fallback",
                        "operation": "generate_candidate",
                        "patch_error": str(exc),
                    }
            else:
                candidate_code = _strip_code_fence(str(generated or ""))
                edit_plan = {"executor": "model", "operation": "generate_candidate"}
        candidate_before_validation = candidate_code
        validation_intent = resolve_strategy_validation_intent(
            prompt=prompt,
            existing_code=existing_code,
            context=context,
        )
        candidate_code, program, behavior_validation = _compile_or_repair_generated_strategy(
            llm,
            user_prompt,
            candidate_code,
            asset_type=asset_type,
            generation_mode=generation_mode,
            context=context,
            system_prompt=full_system_prompt,
            intent=validation_intent,
        )
        if candidate_code != candidate_before_validation and edit_plan.get("executor") == "model_patch":
            edit_plan = {
                "executor": "model_patch_repaired",
                "operation": "generate_candidate",
                "patch_operation_count": edit_plan.get("operation_count", 0),
            }
        manifest = program.manifest.metadata()
        validation = {
            "success": True,
            "manifest": manifest,
            "behavior": behavior_validation,
            "edit_plan": edit_plan,
        }
        assistant_text = _strategy_ai_text(STRATEGY_CANDIDATE_MESSAGE_KEY, lang)
        if workspace:
            result = complete_strategy_candidate_turn(
                user_id=user_id,
                workspace=workspace,
                prompt=prompt,
                base_code=existing_code,
                candidate_code=candidate_code,
                validation=validation,
                assistant_text=assistant_text,
                assistant_message_key=STRATEGY_CANDIDATE_MESSAGE_KEY,
            )
            result.update({"code": candidate_code, "manifest": manifest})
            result["billing"] = billing_meta
            result.update(billing_meta)
            return _ok(result)
        return _ok({
            "reply_type": "candidate",
            "assistant_message": {
                "role": "assistant",
                "content": assistant_text,
                "message_key": STRATEGY_CANDIDATE_MESSAGE_KEY,
                "message_type": "candidate",
            },
            "code": candidate_code,
            "manifest": manifest,
            "validation": validation,
            "billing": billing_meta,
            **billing_meta,
        })
    except LookupError as exc:
        return _error(str(exc), 404)
    except PermissionError as exc:
        return _error(str(exc), 403)
    except ValueError as exc:
        return _error("strategyV2.generationInvalid", data={"error": str(exc)})
    except Exception as exc:
        logger.warning("strategy AI workspace turn failed: %s", exc, exc_info=True)
        return _error("strategyV2.generationInvalid", data={"error": str(exc)})

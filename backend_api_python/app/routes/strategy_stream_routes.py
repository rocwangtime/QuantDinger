"""Cancellable streaming drafts for interactive Strategy API V2 authoring.

These are untrusted text until the existing /strategies/verify endpoint accepts
them. This route never compiles or executes the generated source.
"""

from __future__ import annotations

import json
import re

from flask import Response, g, jsonify, request, stream_with_context

from app.routes.strategy_blueprint import strategy_blp
from app.services.ai_code_edits import CODE_EDIT_SYSTEM_SUFFIX, CodeEditError, apply_model_code_edits
from app.services.ai_generation_control import generation_cancelled, valid_request_id
from app.services.llm_cost import aggregate_usage_display
from app.services.llm_selection import agent_model_selection
from app.services.strategy_ai_generation import build_strategy_generation_request, build_strategy_system_prompt
from app.services.strategy_ai_workspace import normalize_asset_type
from app.utils.auth import login_required
from app.utils.logger import get_logger


logger = get_logger(__name__)


def _sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


def _strip_code_fence(value: str) -> str:
    source = str(value or "").strip()
    blocks = re.findall(r"```(?:python|py)\s*\n(.*?)```", source, flags=re.IGNORECASE | re.DOTALL)
    if len(blocks) == 1:
        return blocks[0].strip()
    source = re.sub(r"^```(?:python)?\s*", "", source, flags=re.IGNORECASE)
    return re.sub(r"\s*```$", "", source).strip()


def _stream_completion(llm, messages: list[dict], *, temperature: float,
                       user_id: int, request_id: str, phase: str, model: str | None = None):
    parts: list[str] = []
    provider_stream = llm.stream_llm_api(
        messages, model=model or llm.get_code_generation_model(), temperature=temperature,
    )
    try:
        for chunk in provider_stream:
            if generation_cancelled(user_id, request_id):
                return None
            if not chunk:
                continue
            parts.append(str(chunk))
            yield _sse("delta", {"phase": phase, "text": str(chunk)})
    finally:
        close = getattr(provider_stream, "close", None)
        if close:
            close()
    return None if generation_cancelled(user_id, request_id) else "".join(parts)


@strategy_blp.route("/strategies/generate/stream", methods=["POST"])
@login_required
@agent_model_selection
def generate_strategy_stream():
    payload = dict(request.get_json(silent=True) or {})
    prompt = str(payload.get("prompt") or "").strip()
    request_id = valid_request_id(payload.get("request_id"))
    if not prompt:
        return jsonify({"code": 0, "msg": "strategyV2.promptRequired", "data": None}), 400
    if not request_id:
        return jsonify({"code": 0, "msg": "strategyV2.requestIdRequired", "data": None}), 400
    user_id = int(g.user_id)

    @stream_with_context
    def generate():
        try:
            from app.services.billing_service import get_billing_service
            from app.services.llm import LLMService

            yield _sse("accepted", {"request_id": request_id})
            if generation_cancelled(user_id, request_id):
                yield _sse("cancelled", {})
                return
            llm = LLMService()
            if not llm.is_configured():
                yield _sse("error", {"msg": "strategyV2.llmNotConfigured"})
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
                yield _sse("error", {"msg": message or "strategyV2.insufficientCredits"})
                return
            yield _sse("progress", {"phase": "generation"})
            content = yield from _stream_completion(
                llm, [{"role": "system", "content": system_prompt},
                      {"role": "user", "content": user_prompt}],
                temperature=0.2 if use_patch_response else 0.4,
                user_id=user_id, request_id=request_id, phase="generation",
            )
            if content is None:
                yield _sse("cancelled", {})
                return
            if use_patch_response:
                try:
                    code, edit_plan = apply_model_code_edits(existing_code, content)
                except CodeEditError:
                    yield _sse("progress", {"phase": "full_fallback"})
                    full_request = build_strategy_generation_request(
                        prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                        generation_mode=generation_mode, context=context, response_mode="full",
                    )
                    fallback = yield from _stream_completion(
                        llm, [{"role": "system", "content": full_system_prompt},
                              {"role": "user", "content": full_request}],
                        temperature=0.25, user_id=user_id, request_id=request_id,
                        phase="full_fallback",
                    )
                    if fallback is None:
                        yield _sse("cancelled", {})
                        return
                    code = _strip_code_fence(fallback)
                    edit_plan = {"executor": "model_full_fallback", "operation": "generate_candidate"}
            else:
                code = _strip_code_fence(content)
                edit_plan = {"executor": "model", "operation": "generate_candidate"}
            if generation_cancelled(user_id, request_id):
                yield _sse("cancelled", {})
                return
            yield _sse("done", {"data": {
                "code": code,
                "llm_usage": aggregate_usage_display(getattr(llm, "usage_events", [])),
                "validation": {"success": False, "status": "pending", "edit_plan": edit_plan},
            }})
        except Exception as exc:
            logger.warning("streamed strategy draft failed: %s", type(exc).__name__)
            yield _sse("error", {"msg": "strategyV2.generationInvalid"})

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no",
    })


@strategy_blp.route("/strategies/ai-workspace/turn/stream", methods=["POST"])
@login_required
@agent_model_selection
def stream_strategy_workspace_turn():
    """Stream the IDE conversation; publish a candidate only after validation.

    The older JSON endpoint remains available to existing clients. Streaming
    clients receive untrusted draft deltas and must never apply those deltas.
    """
    payload = dict(request.get_json(silent=True) or {})
    prompt = str(payload.get("prompt") or "").strip()
    request_id = valid_request_id(payload.get("request_id"))
    if not prompt or not request_id:
        return jsonify({"code": 0, "msg": "strategyV2.promptRequired" if not prompt else "strategyV2.requestIdRequired"}), 400
    user_id = int(g.user_id)

    def cancelled() -> bool:
        return generation_cancelled(user_id, request_id)

    @stream_with_context
    def generate():
        try:
            from app.routes.strategy import (
                STRATEGY_CANDIDATE_MESSAGE_KEY, _strategy_ai_billing_feature,
                _strategy_ai_billing_meta, _strategy_ai_text,
            )
            from app.services.ai_authoring_intent import resolve_authoring_intent
            from app.services.ai_copilot_context import fit_messages_to_budget
            from app.services.ai_generation_contracts import SCRIPT_STRATEGY_REPAIR_REQUIREMENTS
            from app.services.strategy_ai_behavior import validate_strategy_ai_behavior
            from app.services.strategy_ai_capabilities import (
                render_strategy_capability_contract, render_strategy_capability_repairs,
                resolve_strategy_generation_intent,
            )
            from app.services.strategy_ai_generation import (
                apply_deterministic_strategy_edit, resolve_strategy_validation_intent,
                select_strategy_system_prompt, validate_generated_strategy,
            )
            from app.services.strategy_ai_workspace import (
                begin_strategy_ai_turn, classify_strategy_ai_intent,
                complete_strategy_candidate_turn, complete_strategy_discussion_turn,
                get_strategy_ai_workspace,
            )
            from app.services.llm import LLMService
            from app.services.strategy_v2 import compile_strategy_v2
            from app.services.billing_service import get_billing_service

            yield _sse("accepted", {"request_id": request_id})
            yield _sse("progress", {"phase": "routing"})
            asset_type = normalize_asset_type(payload.get("assetType") or payload.get("asset_type"))
            source_id = int(payload.get("sourceId") or payload.get("source_id") or 0)
            existing_code = str(payload.get("existingCode") or "")
            context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
            generation_mode = str(payload.get("generationMode") or "authoring").strip().lower()
            llm = LLMService()
            if not llm.is_configured():
                yield _sse("error", {"msg": "strategyV2.llmNotConfigured"})
                return
            previous = get_strategy_ai_workspace(user_id, source_id, asset_type) if source_id else None
            intent_decision = resolve_authoring_intent(
                prompt=prompt,
                requested_mode=str(payload.get("interactionMode") or "auto"),
                asset_kind="portfolio_strategy" if asset_type == "portfolio_strategy" else "cta_strategy",
                existing_code=existing_code,
                recent_messages=(previous or {}).get("messages") or [],
                fallback_classifier=classify_strategy_ai_intent,
                llm=llm,
            )
            if cancelled():
                yield _sse("cancelled", {})
                return
            intent = str(intent_decision["intent"])
            feature = _strategy_ai_billing_feature(intent)
            accepted, billing_status = get_billing_service().check_and_consume(
                user_id=user_id, feature=feature,
                reference_id=f"strategy_ai_turn_{user_id}_{request_id}",
            )
            if not accepted:
                yield _sse("error", {"msg": billing_status or "strategyV2.insufficientCredits"})
                return
            billing_meta = _strategy_ai_billing_meta(user_id, feature, billing_status)
            workspace = begin_strategy_ai_turn(
                user_id, source_id, prompt, asset_type=asset_type, intent=intent,
            ) if source_id else None
            if workspace and not existing_code:
                existing_code = str(workspace["source"].get("code") or "")
            if cancelled():
                yield _sse("cancelled", {})
                return

            if intent == "discussion":
                discussion_intent = resolve_strategy_generation_intent(
                    prompt=prompt, existing_code=existing_code, context=context,
                )
                contract = render_strategy_capability_contract(discussion_intent)
                system = (
                    "You are QuantDinger's Strategy API V2 code reviewer. Answer in the user's language. "
                    "Explain the current strategy's universe, market type, subscriptions, signals, sizing, risk, and limitations. "
                    "Never claim code was changed and never return replacement source. Be concise and concrete. "
                    "Treat the active capability contract below as authoritative platform behavior and distinguish it from behavior actually implemented by the source."
                )
                if contract:
                    system = f"{system}\n\n{contract}"
                messages = [{"role": "system", "content": system}]
                if workspace:
                    messages.append({"role": "system", "content": "# Bounded strategy memory\n" + json.dumps(workspace.get("summary") or {}, ensure_ascii=False)[:5000]})
                    for item in workspace.get("recent_messages") or []:
                        role = str(item.get("role") or "")
                        content = str(item.get("content") or "").strip()
                        if role in {"user", "assistant"} and content:
                            messages.append({"role": role, "content": content[:2400]})
                messages.append({"role": "user", "content": f"# Current source\n{existing_code[:40000]}\n\n# Question\n{prompt}"})
                messages, _ = fit_messages_to_budget(messages, max_tokens=32000)
                yield _sse("progress", {"phase": "generation"})
                answer = yield from _stream_completion(
                    llm, messages, model=llm.get_default_model(), temperature=0.2,
                    user_id=user_id, request_id=request_id, phase="generation",
                )
                if answer is None:
                    yield _sse("cancelled", {})
                    return
                if workspace:
                    result = complete_strategy_discussion_turn(user_id=user_id, workspace=workspace, answer=answer.strip())
                else:
                    result = {"reply_type": "discussion", "assistant_message": {"role": "assistant", "content": answer.strip(), "message_type": "discussion"}}
                result.update(billing_meta)
                result["billing"] = billing_meta
                result["llm_usage"] = aggregate_usage_display(getattr(llm, "usage_events", []))
                yield _sse("done", {"data": result})
                return

            system, _ = build_strategy_system_prompt(
                prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                generation_mode=generation_mode, context=context,
            )
            full_system = system
            use_patch = bool(existing_code.strip())
            if use_patch:
                system = f"{system}\n\n{CODE_EDIT_SYSTEM_SUFFIX}"
            user_prompt = build_strategy_generation_request(
                prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                generation_mode=generation_mode, context=context,
                response_mode="patch" if use_patch else "full",
            )
            messages = [{"role": "system", "content": system}]
            if workspace:
                messages.append({"role": "system", "content": "# Bounded strategy authoring memory\nUse memory for intent continuity only. The current source and structured constraints remain authoritative.\n" + json.dumps(workspace.get("summary") or {}, ensure_ascii=False)[:5000]})
                for item in workspace.get("recent_messages") or []:
                    role = str(item.get("role") or "")
                    if role == "assistant" and str(item.get("message_type") or "") == "discussion":
                        continue
                    content = str(item.get("content") or "").strip()
                    if role in {"user", "assistant"} and content:
                        messages.append({"role": role, "content": content[:2400]})
            messages.append({"role": "user", "content": user_prompt})
            messages, _ = fit_messages_to_budget(messages, max_tokens=48000)
            deterministic = apply_deterministic_strategy_edit(
                existing_code, prompt,
                summary=(workspace or {}).get("summary") or {},
                recent_messages=(workspace or {}).get("recent_messages") or [],
            )
            if deterministic:
                code, edit_plan = deterministic
            else:
                yield _sse("progress", {"phase": "generation"})
                generated = yield from _stream_completion(
                    llm, messages, temperature=0.2 if use_patch else 0.35,
                    user_id=user_id, request_id=request_id, phase="generation",
                )
                if generated is None:
                    yield _sse("cancelled", {})
                    return
                if use_patch:
                    try:
                        code, edit_plan = apply_model_code_edits(existing_code, generated)
                    except CodeEditError:
                        yield _sse("progress", {"phase": "full_fallback"})
                        full_request = build_strategy_generation_request(
                            prompt=prompt, asset_type=asset_type, existing_code=existing_code,
                            generation_mode=generation_mode, context=context, response_mode="full",
                        )
                        full_messages = [dict(item) for item in messages]
                        full_messages[0] = {"role": "system", "content": full_system}
                        full_messages[-1] = {"role": "user", "content": full_request}
                        fallback = yield from _stream_completion(
                            llm, full_messages, temperature=0.25,
                            user_id=user_id, request_id=request_id, phase="full_fallback",
                        )
                        if fallback is None:
                            yield _sse("cancelled", {})
                            return
                        code = _strip_code_fence(fallback)
                        edit_plan = {"executor": "model_full_fallback", "operation": "generate_candidate"}
                else:
                    code = _strip_code_fence(generated)
                    edit_plan = {"executor": "model", "operation": "generate_candidate"}

            validation_intent = resolve_strategy_validation_intent(
                prompt=prompt, existing_code=existing_code, context=context,
            )
            selected_system = full_system or select_strategy_system_prompt(asset_type, generation_mode)
            for attempt in range(3):
                if cancelled():
                    yield _sse("cancelled", {})
                    return
                yield _sse("progress", {"phase": "validation"})
                try:
                    program = validate_generated_strategy(
                        code, asset_type=asset_type, generation_mode=generation_mode,
                        context=context, prompt=user_prompt, intent=validation_intent,
                        compiler=compile_strategy_v2,
                    )
                    behavior = validate_strategy_ai_behavior(code, program.manifest, validation_intent)
                    break
                except Exception as validation_error:
                    if attempt >= 2:
                        raise
                    repair_prompt = "\n\n".join([
                        SCRIPT_STRATEGY_REPAIR_REQUIREMENTS,
                        render_strategy_capability_repairs(validation_intent),
                        f"Original user request:\n{user_prompt}",
                        f"Validation error to repair now:\n{validation_error}",
                        f"Invalid generated source:\n{code}",
                        "Repair the source and return the complete Python source only.",
                    ])
                    yield _sse("progress", {"phase": "repair"})
                    repaired = yield from _stream_completion(
                        llm, [{"role": "system", "content": selected_system},
                              {"role": "user", "content": repair_prompt}],
                        temperature=0.15, user_id=user_id, request_id=request_id,
                        phase="repair",
                    )
                    if repaired is None:
                        yield _sse("cancelled", {})
                        return
                    code = _strip_code_fence(repaired)
            if cancelled():
                yield _sse("cancelled", {})
                return
            manifest = program.manifest.metadata()
            validation = {"success": True, "manifest": manifest, "behavior": behavior, "edit_plan": edit_plan}
            assistant_text = _strategy_ai_text(STRATEGY_CANDIDATE_MESSAGE_KEY, request.headers.get("X-App-Lang") or "zh-CN")
            if workspace:
                result = complete_strategy_candidate_turn(
                    user_id=user_id, workspace=workspace, prompt=prompt,
                    base_code=existing_code, candidate_code=code, validation=validation,
                    assistant_text=assistant_text,
                    assistant_message_key=STRATEGY_CANDIDATE_MESSAGE_KEY,
                )
                result.update({"code": code, "manifest": manifest})
            else:
                result = {"reply_type": "candidate", "assistant_message": {
                    "role": "assistant", "content": assistant_text,
                    "message_key": STRATEGY_CANDIDATE_MESSAGE_KEY,
                    "message_type": "candidate",
                }, "code": code, "manifest": manifest, "validation": validation}
            result.update(billing_meta)
            result["billing"] = billing_meta
            result["llm_usage"] = aggregate_usage_display(getattr(llm, "usage_events", []))
            yield _sse("done", {"data": result})
        except (LookupError, PermissionError, ValueError):
            yield _sse("error", {"msg": "strategyV2.generationInvalid"})
        except Exception as exc:
            logger.warning("streamed strategy workspace turn failed: %s", type(exc).__name__)
            yield _sse("error", {"msg": "strategyV2.generationInvalid"})

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no",
    })

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
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _strip_code_fence(value: str) -> str:
    source = str(value or "").strip()
    blocks = re.findall(r"```(?:python|py)\s*\n(.*?)```", source, flags=re.IGNORECASE | re.DOTALL)
    if len(blocks) == 1:
        return blocks[0].strip()
    source = re.sub(r"^```(?:python)?\s*", "", source, flags=re.IGNORECASE)
    return re.sub(r"\s*```$", "", source).strip()


def _stream_completion(llm, messages: list[dict], *, temperature: float,
                       user_id: int, request_id: str, phase: str):
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

"""Clean-environment worker for AI-generated Strategy API V2 validation.

Started with ``python -I`` in a temporary working directory. Importing the
real ``app`` package would load deployment .env files, so install a namespace
stub before importing only the validation modules.
"""

from __future__ import annotations

import json
import io
import os
from pathlib import Path
import sys
import types


class _NullWriter(io.TextIOBase):
    def write(self, value: str) -> int:
        return len(value)


def _prepare_isolation() -> None:
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (25, 26))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024 * 1024, 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_AS, (1536 * 1024 * 1024, 1536 * 1024 * 1024))
    app_root = Path(__file__).resolve().parents[1]
    app_stub = types.ModuleType("app")
    app_stub.__path__ = [str(app_root)]
    sys.modules["app"] = app_stub


def _deny_network() -> None:
    import socket

    def denied(*_args, **_kwargs):
        raise PermissionError("network is disabled in strategy validation")

    socket.socket = denied
    socket.create_connection = denied
    socket.socketpair = denied


def main() -> None:
    _prepare_isolation()
    request = json.loads(sys.stdin.buffer.read(256 * 1024 + 1).decode("utf-8"))
    if not isinstance(request, dict):
        raise ValueError("invalid validation request")
    from app.services.strategy_ai_generation import (
        resolve_strategy_validation_intent, validate_generated_strategy,
    )
    from app.services.strategy_ai_behavior import validate_strategy_ai_behavior

    _deny_network()
    code = str(request.get("code") or "")
    prompt = str(request.get("prompt") or "")
    context = request.get("context") if isinstance(request.get("context"), dict) else {}
    real_stdout = sys.stdout
    sys.stdout = _NullWriter()
    try:
        try:
            intent = resolve_strategy_validation_intent(
                prompt=prompt, existing_code=str(request.get("existing_code") or ""),
                context=context,
            )
            program = validate_generated_strategy(
                code, asset_type=str(request.get("asset_type") or "script"),
                generation_mode=str(request.get("generation_mode") or "authoring"),
                context=context, prompt=prompt, intent=intent,
            )
            behavior = validate_strategy_ai_behavior(code, program.manifest, intent)
            result = {"success": True, "manifest": program.manifest.metadata(),
                      "behavior": behavior}
        except Exception as exc:
            result = {"success": False, "error": str(exc)[:500] or type(exc).__name__}
    finally:
        sys.stdout = real_stdout
    sys.stdout.write(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()

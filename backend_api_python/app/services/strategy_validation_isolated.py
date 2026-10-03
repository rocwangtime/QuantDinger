"""Validate an AI strategy in a clean, bounded child process.

The model's source is never executed in the HTTP worker. The child receives
JSON, has no parent credentials, and is killed on a wall-clock deadline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def validate_strategy_candidate_isolated(
    code: str, *, prompt: str, existing_code: str, asset_type: str,
    generation_mode: str, context: dict, timeout: int = 30,
) -> dict:
    payload = json.dumps({
        "code": code,
        "prompt": prompt,
        "existing_code": existing_code,
        "asset_type": asset_type,
        "generation_mode": generation_mode,
        "context": context,
    }, ensure_ascii=False, default=str).encode("utf-8")
    if len(payload) > 256 * 1024:
        return {"success": False, "error": "strategyV2.aiCandidateTooLarge"}
    worker = Path(__file__).resolve().parents[1] / "utils" / "strategy_validation_worker.py"
    clean_env = {
        "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC",
        "PYTHONNOUSERSITE": "1", "QD_SANDBOX_WORKER": "1",
        "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
    }
    try:
        with tempfile.TemporaryDirectory(prefix="quantdinger-strategy-check-") as workdir:
            completed = subprocess.run(
                [sys.executable, "-I", str(worker)], input=payload,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                cwd=workdir, env=clean_env, close_fds=True,
                timeout=max(1, min(int(timeout), 60)), check=False,
            )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "strategyV2.aiValidationTimedOut"}
    except (OSError, ValueError):
        return {"success": False, "error": "strategyV2.aiValidationUnavailable"}
    if completed.returncode != 0 or len(completed.stdout) > 1024 * 1024:
        return {"success": False, "error": "strategyV2.aiValidationUnavailable"}
    try:
        result = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"success": False, "error": "strategyV2.aiValidationUnavailable"}
    return result if isinstance(result, dict) else {
        "success": False, "error": "strategyV2.aiValidationUnavailable",
    }

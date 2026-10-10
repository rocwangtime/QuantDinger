"""Immutable full-input bundles, including fundamental values and membership."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import tempfile
import platform
from pathlib import Path

import pandas as pd


def content_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, allow_nan=False).encode()).hexdigest()


def runtime_identity():
    import numpy as np
    root = Path(__file__).resolve().parent.parent
    files = [path for folder in ("strategy_v2", "strategy_evolution") for path in (root / folder).glob("*.py")]
    files += [root / name for name in ("instrument_rules.py", "fundamental_data.py", "backtest_limits.py")]
    return {
        "pandas": pd.__version__, "numpy": np.__version__, "python": platform.python_version(),
        "modules": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
    }


def pack_frame(frame):
    def scalar(value):
        if hasattr(value, "item"):
            value = value.item()
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    return {
        "columns": list(frame.columns),
        "index": [int(pd.Timestamp(value).value) for value in frame.index],
        "rows": [[scalar(value) for value in row] for row in frame.itertuples(index=False, name=None)],
    }


def unpack_frame(value):
    return pd.DataFrame(value["rows"], columns=value["columns"], index=pd.to_datetime(value["index"], unit="ns"))


class EvolutionBundleStore:
    def __init__(self, root=None):
        self.root = Path(root or os.getenv("EVOLUTION_BUNDLE_DIR") or "data/evolution_bundles")

    def save(self, document):
        payload = json.dumps(document, sort_keys=True, default=str, allow_nan=False).encode()
        identity = hashlib.sha256(payload).hexdigest()
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / f"{identity}.json.gz"
        if not target.exists():
            fd, temporary = tempfile.mkstemp(dir=self.root, suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as raw:
                    with gzip.GzipFile(fileobj=raw, mode="wb") as handle:
                        handle.write(payload)
                os.replace(temporary, target)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        return identity

    def load(self, identity, *, user_id):
        identity = str(identity)
        if len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity):
            raise ValueError("strategyEvolution.bundleIdInvalid")
        with gzip.open(self.root / f"{identity}.json.gz", "rb") as handle:
            payload = handle.read()
        if hashlib.sha256(payload).hexdigest() != identity:
            raise ValueError("strategyEvolution.bundleHashMismatch")
        document = json.loads(payload)
        if document["userId"] != int(user_id):
            raise ValueError("strategyEvolution.bundleNotFound")
        if document["runtime"] != runtime_identity():
            raise ValueError("strategyEvolution.replayRuntimeChanged")
        return document


class FrozenUniverse:
    def __init__(self, candidates, universe):
        self.candidates, self.universe = candidates, universe

    def list_universes(self, user_id):
        return [self.universe] if self.universe else []

    def candidate_members(self, user_id, universe_id, **kwargs):
        return self.candidates

    def resolve_members(self, user_id, universe_id, *, as_of):
        day = str(as_of)[:10]
        return [item for item in self.candidates if any(
            (not period.get("valid_from") or str(period["valid_from"])[:10] <= day)
            and (not period.get("valid_to") or str(period["valid_to"])[:10] > day)
            for period in item.get("membership_periods", [item])
        )]

"""JSON-compatible telemetry: unavailable numeric values are null."""
import math


def sanitize_json(value):
    """Copy JSON containers, replacing non-finite floats without mutating inputs."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: sanitize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_json(item) for item in value]
    return value

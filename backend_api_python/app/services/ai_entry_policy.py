"""Entry-only AI policy; deterministic risk and exits remain independent."""

from dataclasses import replace


ENTRY_MODES = frozenset({"shadow", "advisory", "required"})


def normalize_entry_mode(value):
    mode = str(value or "advisory").strip().lower()
    if mode not in ENTRY_MODES:
        raise ValueError("aiDecision.invalidMode")
    return mode


def apply_entry_policy(result, *, mode, entry, enabled):
    raw_allowed = result.allowed
    valid = result.provider in {"jev", "llm"} and result.decision in {"pass", "reject"}
    allowed = raw_allowed
    decision, reason = result.decision, result.reason
    if enabled and entry:
        if mode == "shadow":
            allowed = True
            decision = "shadow_pass" if valid and raw_allowed else "shadow_reject" if valid else "shadow_unavailable"
        elif mode == "required" and not valid:
            allowed, decision, reason = False, "unavailable_rejected", "ai_required_decision_unavailable"
    policy = {
        "name": "entry_policy", "mode": mode, "raw_allowed": raw_allowed,
        "raw_decision": result.decision, "raw_reason": result.reason,
        "valid_decision": valid, "enforced": bool(enabled and entry and mode != "shadow"),
    }
    checks = [item for item in result.checks if item.get("name") != "entry_policy"]
    return replace(result, allowed=allowed, decision=decision, reason=reason, checks=[*checks, policy])

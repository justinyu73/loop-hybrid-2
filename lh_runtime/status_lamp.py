#!/usr/bin/env python3
"""Status lamp: the one place a snapshot is judged degraded.

``lamp(snapshot)`` is a pure function: it reads the snapshot, changes nothing,
and gives the same answer for the same input.  It returns ``ok`` or
``degraded`` together with every rule it evaluated and the rules that fired, so
a reader sees why.  No other field of the snapshot is a health verdict; a
reader that needs one asks this function.

Unknown is not healthy: a missing or unreadable input fires its rule.
"""
from __future__ import annotations

from typing import Any, Mapping

SCHEMA = "lh-status-lamp/v1"
OK = "ok"
DEGRADED = "degraded"
RULES = (
    ("heartbeat_stale", "the driver heartbeat is missing, unreadable, or older than the staleness threshold"),
    ("code_identity_stale", "the engine code on disk differs from the code the driver loaded, or that is unknown"),
    ("needs_human", "a goal or event is parked in human_required, or the count is unknown"),
    ("dispatch_stopped", "the last dispatch gate decision is stop"),
    ("integrity_check_red", "the scheduled integrity checks are enabled and red, unknown, missing, or older than their maximum age"),
)


def _count(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _fires(rule_id: str, snapshot: Mapping[str, Any]) -> bool:
    if rule_id == "heartbeat_stale":
        return snapshot.get("stale") is not False
    if rule_id == "code_identity_stale":
        identity = snapshot.get("code_identity")
        return not isinstance(identity, Mapping) or identity.get("stale") is not False
    if rule_id == "needs_human":
        status = snapshot.get("status")
        headline = status.get("headline") if isinstance(status, Mapping) else None
        if not isinstance(headline, Mapping):
            return True
        counts = [_count(headline.get("needs_human")), _count(headline.get("needs_human_events"))]
        return any(count is None or count > 0 for count in counts)
    if rule_id == "integrity_check_red":
        checks = snapshot.get("scheduled_checks")
        if checks is None:
            return False  # not enabled: the rule does not apply
        return not isinstance(checks, Mapping) or checks.get("verdict") != "green" or checks.get("stale") is not False
    if rule_id == "dispatch_stopped":
        gate = snapshot.get("dispatch_gate")
        return isinstance(gate, Mapping) and gate.get("action") == "stop"
    raise ValueError(f"unknown lamp rule: {rule_id}")


def lamp(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    fired = [{"id": rule_id, "rule": text} for rule_id, text in RULES if _fires(rule_id, snapshot)]
    return {
        "schema": SCHEMA,
        "lamp": DEGRADED if fired else OK,
        "fired": fired,
        "evaluated": [rule_id for rule_id, _text in RULES],
    }

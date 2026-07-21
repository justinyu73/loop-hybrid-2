#!/usr/bin/env python3
"""S1 part 2: improvement findings -> improvement goal commands.

`gate-pack/improvement/improvement_loop.py` produces report-only findings;
this bridge turns each finding into a standard ``manual_intent`` command so
improvement work enters the SAME pipeline as any other goal (intent ->
derive -> admission -> dispatch -> lamp -> value gate -> draft PR -> human
merge). There is no apply shortcut anywhere: the bridge is pure deterministic
mapping — every finding maps 1:1 to a command, no model, no filtering
judgment; downstream admission decides (an unknown campaign or stage routes
to human_required by the existing intent-derivation path).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from command_ingress import submit_command

SOURCE = "improvement_gate"
REQUIRED_FIELDS = ("finding_id", "campaign_id", "stage_id", "summary")


def load_findings(path: str | Path) -> list[dict[str, str]]:
    """Read and validate the findings artifact.

    Shape: a JSON list, or an object with a ``findings`` list. Each finding
    needs ``finding_id``, ``campaign_id``, ``stage_id``, ``summary`` (all
    non-empty strings); ``suggested_goal`` is an optional string. Anything
    else is a clear error and nothing is submitted."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items = raw.get("findings") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        raise ValueError("findings artifact must be a list or an object with a 'findings' list")
    findings: list[dict[str, str]] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"finding #{index} must be an object")
        entry: dict[str, str] = {}
        for field in REQUIRED_FIELDS:
            value = item.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"finding #{index} missing non-empty field: {field}")
            entry[field] = value.strip()
        suggested = item.get("suggested_goal")
        if suggested is not None:
            if not isinstance(suggested, str):
                raise ValueError(f"finding {entry['finding_id']!r}: suggested_goal must be a string")
            if suggested.strip():
                entry["suggested_goal"] = suggested.strip()
        findings.append(entry)
    return findings


def finding_to_command(finding: dict[str, str]) -> dict[str, Any]:
    """One finding -> one bounded manual_intent command (deterministic 1:1)."""
    return {
        "event_id": f"evt-improvement:{finding['finding_id']}",
        "idempotency_key": f"improvement:{finding['finding_id']}",
        "source": SOURCE,
        "payload": {
            "campaign_id": finding["campaign_id"],
            "stage_id": finding["stage_id"],
            "intent": finding.get("suggested_goal") or finding["summary"],
        },
    }


def submit_findings(goal_store: Any, path: str | Path) -> list[dict[str, Any]]:
    """Submit every finding as one manual_intent command.

    Idempotency keys are ``improvement:{finding_id}``, so a re-run of the
    same findings file replays cleanly (``reused``) instead of duplicating
    commands. Returns one row per finding with the recorded status."""
    submitted: list[dict[str, Any]] = []
    for finding in load_findings(path):
        command = finding_to_command(finding)
        result = submit_command(
            goal_store,
            source=command["source"],
            event_type="manual_intent",
            event_id=command["event_id"],
            payload=command["payload"],
            idempotency_key=command["idempotency_key"],
        )
        submitted.append({
            "finding_id": finding["finding_id"],
            "event_key": result["event_key"],
            "status": result["status"],
            "idempotency_key": command["idempotency_key"],
        })
    return submitted

#!/usr/bin/env python3
"""LH-owned normalization of async external verdicts (goal-lifecycle-v1).

The contract's asynchronous boundary says value reduction may run only after
an LH-owned `lh-normalized-verifier-result/v1` record and its
`value_reduction_ready` event are durable and bound to the same Goal
revision, Run, Attempt, check and receipt. This module is the only writer of
that record: an external adapter supplies evidence, and nothing outside LH
assigns the normalized outcome.

"Late" is not a clock. Evidence is late when its binding has been superseded,
decided by re-reading the durable stores at normalization time: the verdict
row must still carry the op_key the evidence answered, the run must still sit
at its parked transition point, the goal must still be on the same revision,
and the receipt digest must match the parked attempt. Anything else is
discarded with a typed reason and zero state change -- the loop has already
moved on, which is the entire content of lateness.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

NORMALIZATION_VERSION = "lh-normalized-verifier-result/v1"


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def normalize_resolved_run(
    *,
    goal_store: Any,
    run_store: Any,
    verdict_store: Any,
    run_id: str,
    op_key: str,
    conclusion: str,
) -> dict[str, Any]:
    """Normalize one landed conclusion; returns a typed outcome, never raises
    for evidence problems. Ready is emitted for success conclusions only."""
    record = verdict_store.full_record(run_id)
    if record is None:
        return {"status": "discarded", "reason": "verdict_record_missing"}
    if record["op_key"] != op_key:
        # A retry re-parked this run under a new op_key; the old conclusion
        # answers a question nobody is asking anymore.
        return {"status": "discarded", "reason": "op_key_superseded"}
    try:
        run = run_store.get_run(run_id)
    except (KeyError, ValueError):
        return {"status": "discarded", "reason": "run_missing"}
    if run["state"] not in {"awaiting_external_verdict", "verified"}:
        # verified is admitted for crash recovery: the run may have crossed
        # before a crash while the normalized record did not land; the
        # binding re-checks below still hold it to the same attempt.
        return {"status": "discarded", "reason": f"run_not_parked: {run['state']}"}
    goal_data = run.get("goal") if isinstance(run.get("goal"), dict) else {}
    goal_id = goal_data.get("goal_id")
    if not isinstance(goal_id, str) or not goal_id:
        return {"status": "discarded", "reason": "run_carries_no_goal"}
    try:
        goal = goal_store.get_goal(goal_id)
    except KeyError:
        return {"status": "discarded", "reason": "goal_retired"}
    revision_id = goal.get("current_revision_id")
    if not revision_id:
        return {"status": "discarded", "reason": "goal_has_no_revision"}
    receipt_meta = run_store.latest_receipt(run_id)
    if not receipt_meta or not receipt_meta.get("receipt_digest"):
        return {"status": "discarded", "reason": "receipt_missing"}
    action = verdict_store.action_for_run(run_id)
    request = action.get("request") if isinstance(action, dict) and isinstance(action.get("request"), dict) else None
    if not request or not request.get("action_id"):
        return {"status": "discarded", "reason": "parked_action_missing"}
    dispatched_at = record.get("dispatched_at")
    resolved_at = record.get("resolved_at")
    if not isinstance(dispatched_at, (int, float)) or not isinstance(resolved_at, (int, float)):
        return {"status": "discarded", "reason": "verdict_timestamps_missing"}
    binding = {
        "goal_id": goal_id,
        "revision_id": revision_id,
        "run_id": run_id,
        "attempt": int(run["attempts"]),
        "check_id": str(request["action_id"]),
        "check_definition_digest": _digest(request),
        "receipt_digest": receipt_meta["receipt_digest"],
    }
    outcome = "verified" if conclusion == "success" else "failed"
    result = goal_store.record_normalized_verifier_result(
        binding=binding,
        outcome=outcome,
        source_digest=_digest({"conclusion": conclusion}),
        authority_check_result_digest=_digest({
            "op_key": record["op_key"],
            "state": record["state"],
            "conclusion": record["conclusion"],
            "dispatched_at": dispatched_at,
            "resolved_at": resolved_at,
        }),
        measured_duration=max(0.0, float(resolved_at) - float(dispatched_at)),
        normalization_version=NORMALIZATION_VERSION,
    )
    if result["status"] in {"conflict", "conflict_replay"}:
        # The binding is quarantined for good; the goal routes through the
        # existing conflict state to a human. Both transitions are recorded;
        # a goal that already left active is left where its own machine put it.
        try:
            goal_store.transition_goal(goal_id, "conflict", expected_state="active")
            goal_store.transition_goal(goal_id, "human_required", expected_state="conflict")
        except ValueError:
            pass
    return result

#!/usr/bin/env python3
"""Deterministic canary for the verified-completion GitHub PR request route."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "goal_bind"))
sys.path.insert(0, str(HERE.parent / "execution_receipt"))
import execution_receipt as er
import goal_bind as gb
import value_route as vr


def goal() -> dict:
    return {
        "schema": gb.GOAL_SCHEMA, "goal_id": "goal-a",
        "feature_contract": {"must_have": ["deliver status"], "must_not": ["send telemetry"]},
        "approval_mode": gb.APPROVAL_MODE,
        "plan": {"grill_ref": "discussion://fixture", "assumptions": ["LH owns GitHub"]},
        "acceptance": {"static": ["lint"], "targeted": ["targeted"], "core_regression": ["core"], "negative": ["negative"]},
        "run_policy": {"max_attempts": 4, "on_verified_success": gb.SUCCESS_ROUTE},
    }


def receipt(goal_value: dict, *, passing: bool, attempt: int = 1) -> dict:
    h = lambda char: "sha256:" + char * 64
    verification = {name: {"exit_code": 0, "evidence_ref": f"remote://run/{name}"} for name in er.CHECKS}
    if not passing:
        verification["core_regression"]["exit_code"] = 1
    return {
        "schema": er.RECEIPT_SCHEMA, "run_id": "run-001", "goal_digest": gb.digest(goal_value), "attempt": attempt,
        "workspace": {"ref": "lh://workspace/run-001", "base_revision": h("b"), "disposable": True},
        "provider": {"profile": "lh-provider", "response_digest": h("c"), "duration_ms": 1},
        "trajectory": {"ref": "remote://run/trajectory", "digest": h("d")}, "diff_digest": h("e"),
        "commands": [{"id": "test", "argv_digest": h("f"), "exit_code": 0 if passing else 1, "duration_ms": 1, "output_ref": "remote://run/test"}],
        "verification": verification,
        "failure": None if passing else {"classification": "retryable", "fingerprint": "core:exit-1", "excerpt_ref": "remote://run/stderr"},
    }


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    valid_goal = goal()
    complete_receipt = receipt(valid_goal, passing=True)
    complete = vr.route(valid_goal, complete_receipt)
    retry = vr.route(valid_goal, receipt(valid_goal, passing=False))
    exhausted = vr.route(valid_goal, receipt(valid_goal, passing=False, attempt=4))
    bad_goal = goal(); bad_goal["run_policy"]["on_verified_success"] = "merge_pull_request"
    invalid = vr.route(bad_goal, receipt(bad_goal, passing=True))
    other_goal = goal(); other_goal["goal_id"] = "goal-b"
    mismatched = vr.route(valid_goal, receipt(other_goal, passing=True))
    request = complete.get("pr_request") or {}
    cases = [
        case("verified-completion-requests-draft-pr", complete.get("status") == "pr_create_requested" and request.get("draft") is True, complete.get("status", "")),
        case("request-binds-goal-diff-and-evidence", request.get("goal_digest") == gb.digest(valid_goal) and request.get("diff_digest") == complete_receipt["diff_digest"] and set(request.get("verification", {})) == er.CHECKS, "bound"),
        case("failed-verification-does-not-request-pr", retry.get("route") == "retry" and retry.get("pr_request") is None, retry.get("status", "")),
        case("exhausted-attempt-does-not-request-pr", exhausted.get("route") == "stop" and exhausted.get("pr_request") is None, exhausted.get("status", "")),
        case("unsupported-success-route-is-rejected", invalid.get("verdict") == "ng", invalid.get("status", "")),
        case("receipt-must-bind-current-goal", mismatched.get("status") == "goal_receipt_mismatch", mismatched.get("status", "")),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "value-route-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["LH's existing GitHub adapter consumes the draft PR request; this contract intentionally does not hold GitHub credentials or perform a merge."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

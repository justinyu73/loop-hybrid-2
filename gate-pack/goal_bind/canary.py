#!/usr/bin/env python3
"""Deterministic canary for the additive Goal Discovery / Goal Bind contract."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import goal_bind as gb


def goal(goal_id: str = "goal-a") -> dict:
    return {
        "schema": gb.GOAL_SCHEMA,
        "goal_id": goal_id,
        "feature_contract": {"must_have": ["deliver status"], "must_not": ["send telemetry"]},
        "approval_mode": gb.APPROVAL_MODE,
        "plan": {"grill_ref": "discussion://fixture", "assumptions": ["LH owns execution"]},
        "acceptance": {"static": ["lint"], "targeted": ["targeted"], "core_regression": ["core"], "negative": ["negative"]},
        "run_policy": {"max_attempts": 4, "on_verified_success": gb.SUCCESS_ROUTE},
    }


def request(status: str, active: list[dict], *, ids: list[str], question: str = "", proposed: dict | None = None) -> dict:
    return {
        "schema": gb.REQUEST_SCHEMA,
        "trigger": {"kind": "keyword", "value": "繼續"},
        "assessment": {"status": status, "reason": "fixture assessment", "goal_ids": ids, "question": question},
        "active_goals": active,
        "proposed_goal": proposed,
    }


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    active = goal()
    found = gb.resolve(request("goal_found", [active], ids=[active["goal_id"]]))
    missing = gb.resolve(request("goal_missing", [], ids=[], question="Which observable feature should this loop complete?"))
    revision = copy.deepcopy(active); revision["goal_id"] = "goal-a-r2"; revision["revision_of"] = active["goal_id"]; revision["plan"]["grill_ref"] = "discussion://fixture/revision"
    stale = gb.resolve(request("goal_stale", [active], ids=[active["goal_id"]], proposed=revision))
    other = goal("goal-b")
    conflict = gb.resolve(request("goal_conflict", [active, other], ids=[active["goal_id"], other["goal_id"]], question="Which goal should continue?"))
    widened = copy.deepcopy(revision); widened["feature_contract"]["must_not"] = []
    widened_result = gb.resolve(request("goal_stale", [active], ids=[active["goal_id"]], proposed=widened))
    unknown = gb.resolve(request("goal_found", [active], ids=["missing-id"]))
    cases = [
        case("existing-goal-binds", found["verdict"] == "pass" and found["binding"]["route"] == "bind", found["status"]),
        case("missing-goal-asks-one-question", missing["verdict"] == "pass" and missing["binding"]["route"] == "ask_user", missing["status"]),
        case("stale-goal-grills-and-binds", stale["verdict"] == "pass" and stale["binding"]["route"] == "grill_and_bind", stale["status"]),
        case("conflict-is-surfaced", conflict["verdict"] == "pass" and conflict["binding"]["route"] == "surface_conflict", conflict["status"]),
        case("revision-cannot-silently-drop-feature-boundary", widened_result["verdict"] == "ng", widened_result["status"]),
        case("assessment-cannot-name-an-unknown-goal", unknown["verdict"] == "ng", unknown["status"]),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "goal-bind-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["Natural-language assessment and execution remain LH engine responsibilities; this additive contract validates only their structured handoff."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

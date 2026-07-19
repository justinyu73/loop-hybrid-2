#!/usr/bin/env python3
"""Route verified LH value evidence to an injected GitHub PR adapter request."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "goal_bind"))
sys.path.insert(0, str(HERE.parent / "execution_receipt"))
sys.path.insert(0, str(HERE.parent / "verification_reducer"))
import execution_receipt as er
import goal_bind as gb
import verification_reducer as vr

PR_REQUEST_SCHEMA = "lh-github-pr-create-request/v1"


def route(goal: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    problems = [*(f"goal: {problem}" for problem in gb.validate_goal(goal)), *(f"receipt: {problem}" for problem in er.validate_receipt(receipt))]
    if problems:
        return {"verdict": "ng", "status": "invalid_value_route_input", "problems": problems}
    if goal["run_policy"]["on_verified_success"] != gb.SUCCESS_ROUTE:
        return {"verdict": "ng", "status": "unsupported_success_route", "problems": ["goal success route is not create_pull_request"]}
    if receipt["goal_digest"] != gb.digest(goal):
        return {"verdict": "ng", "status": "goal_receipt_mismatch", "problems": ["receipt does not bind the current GoalSpec"]}
    reduced = vr.reduce(receipt)
    if reduced["verdict"] != "pass":
        return {"verdict": "ng", "status": "verification_reduction_invalid", "problems": reduced.get("problems", [])}
    if reduced["route"] != "pass":
        return {"verdict": "pass", "status": reduced["status"], "route": reduced["route"], "pr_request": None, "problems": []}
    request = {
        "schema": PR_REQUEST_SCHEMA,
        "goal_id": goal["goal_id"],
        "goal_digest": gb.digest(goal),
        "run_id": receipt["run_id"],
        "attempt": receipt["attempt"],
        "draft": True,
        "base_revision": receipt["workspace"]["base_revision"],
        "diff_digest": receipt["diff_digest"],
        "verification": receipt["verification"],
        "execution_receipt_digest": er.digest(receipt),
        "trajectory": receipt["trajectory"],
    }
    return {"verdict": "pass", "status": "pr_create_requested", "route": "complete", "pr_request": request, "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Route verified value evidence to an LH GitHub PR-create request")
    parser.add_argument("goal")
    parser.add_argument("receipt")
    args = parser.parse_args()
    try:
        goal = json.loads(Path(args.goal).read_text(encoding="utf-8"))
        receipt = json.loads(Path(args.receipt).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        result = {"verdict": "ng", "status": "invalid_value_route_input", "problems": [str(exc)]}
    else:
        result = route(goal, receipt)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())

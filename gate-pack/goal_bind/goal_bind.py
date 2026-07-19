#!/usr/bin/env python3
"""Validate the additive LH Goal Discovery / Goal Bind interface.

LH owns natural-language assessment and execution.  This module accepts that
assessment as structured input, binds a canonical feature contract to a run,
and refuses a grill-derived revision that silently widens the contract.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

GOAL_SCHEMA = "lh-goal-spec/v1"
REQUEST_SCHEMA = "lh-goal-discovery-request/v1"
BINDING_SCHEMA = "lh-goal-binding/v1"
STATUSES = {"goal_found", "goal_missing", "goal_stale", "goal_conflict"}
TRIGGER_KINDS = {"keyword", "lifecycle"}
APPROVAL_MODE = "feature_contract_is_approval"
SUCCESS_ROUTE = "create_pull_request"


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) and item.strip() for item in value)


def validate_goal(goal: Any) -> list[str]:
    if not isinstance(goal, dict):
        return ["goal must be an object"]
    allowed = {"schema", "goal_id", "feature_contract", "approval_mode", "plan", "acceptance", "run_policy", "revision_of"}
    problems = [] if set(goal) <= allowed else ["goal contains unsupported fields"]
    if goal.get("schema") != GOAL_SCHEMA:
        problems.append(f"goal.schema must be {GOAL_SCHEMA}")
    if not isinstance(goal.get("goal_id"), str) or not goal["goal_id"].strip():
        problems.append("goal_id must be non-empty")
    contract = goal.get("feature_contract")
    if not isinstance(contract, dict) or set(contract) != {"must_have", "must_not"}:
        problems.append("feature_contract must contain only must_have and must_not")
    elif not _string_list(contract.get("must_have")) or not contract["must_have"] or not _string_list(contract.get("must_not")):
        problems.append("feature contract lists are invalid")
    if goal.get("approval_mode") != APPROVAL_MODE:
        problems.append(f"approval_mode must be {APPROVAL_MODE}")
    plan = goal.get("plan")
    if not isinstance(plan, dict) or set(plan) != {"grill_ref", "assumptions"} or not isinstance(plan.get("grill_ref"), str) or not plan["grill_ref"] or not _string_list(plan.get("assumptions")):
        problems.append("plan must contain grill_ref and assumptions")
    acceptance = goal.get("acceptance")
    required_checks = {"static", "targeted", "core_regression", "negative"}
    if not isinstance(acceptance, dict) or set(acceptance) != required_checks or any(not _string_list(acceptance.get(name)) or not acceptance[name] for name in required_checks):
        problems.append("acceptance must contain non-empty static, targeted, core_regression, and negative lists")
    policy = goal.get("run_policy")
    if not isinstance(policy, dict) or policy != {"max_attempts": 4, "on_verified_success": SUCCESS_ROUTE}:
        problems.append("run_policy must set max_attempts 4 and create_pull_request")
    if "revision_of" in goal and (not isinstance(goal["revision_of"], str) or not goal["revision_of"].strip()):
        problems.append("revision_of must be a non-empty string when present")
    return problems


def _preserves_contract(parent: dict[str, Any], revision: dict[str, Any]) -> bool:
    before, after = parent["feature_contract"], revision["feature_contract"]
    return set(before["must_have"]).issubset(after["must_have"]) and set(before["must_not"]).issubset(after["must_not"])


def validate_request(request: Any) -> list[str]:
    if not isinstance(request, dict):
        return ["request must be an object"]
    allowed = {"schema", "trigger", "assessment", "active_goals", "proposed_goal"}
    problems = [] if set(request) <= allowed else ["request contains unsupported fields"]
    if request.get("schema") != REQUEST_SCHEMA:
        problems.append(f"request.schema must be {REQUEST_SCHEMA}")
    trigger = request.get("trigger")
    if not isinstance(trigger, dict) or set(trigger) != {"kind", "value"} or trigger.get("kind") not in TRIGGER_KINDS or not isinstance(trigger.get("value"), str) or not trigger["value"].strip():
        problems.append("trigger must contain a supported kind and non-empty value")
    assessment = request.get("assessment")
    required_assessment = {"status", "reason", "goal_ids", "question"}
    if not isinstance(assessment, dict) or set(assessment) != required_assessment:
        problems.append("assessment fields are invalid")
        assessment = {}
    elif assessment.get("status") not in STATUSES or not isinstance(assessment.get("reason"), str) or not assessment["reason"].strip() or not _string_list(assessment.get("goal_ids")) or not isinstance(assessment.get("question"), str):
        problems.append("assessment values are invalid")
    active = request.get("active_goals")
    if not isinstance(active, list):
        problems.append("active_goals must be an array")
        active = []
    else:
        for index, goal in enumerate(active):
            problems.extend(f"active_goals[{index}]: {problem}" for problem in validate_goal(goal))
    proposed = request.get("proposed_goal")
    if proposed is not None:
        problems.extend(f"proposed_goal: {problem}" for problem in validate_goal(proposed))
    if problems:
        return problems
    by_id = {goal["goal_id"]: goal for goal in active}
    status, goal_ids, question = assessment["status"], assessment["goal_ids"], assessment["question"]
    if len(goal_ids) != len(set(goal_ids)) or any(goal_id not in by_id for goal_id in goal_ids):
        problems.append("assessment goal_ids must reference active goals exactly")
    if status == "goal_found" and (len(goal_ids) != 1 or proposed is not None or question):
        problems.append("goal_found requires one active goal, no proposal, and no question")
    if status == "goal_missing" and (goal_ids or proposed is not None or not question.strip()):
        problems.append("goal_missing requires a focused question and no goal")
    if status == "goal_stale":
        if len(goal_ids) != 1 or not isinstance(proposed, dict) or proposed.get("revision_of") != goal_ids[0] or not _preserves_contract(by_id[goal_ids[0]], proposed):
            problems.append("goal_stale requires a contract-preserving proposed revision of one active goal")
    if status == "goal_conflict" and (len(goal_ids) < 2 or proposed is not None or not question.strip()):
        problems.append("goal_conflict requires two or more goals and a focused question")
    return problems


def resolve(request: dict[str, Any]) -> dict[str, Any]:
    problems = validate_request(request)
    if problems:
        return {"verdict": "ng", "status": "invalid_goal_discovery_request", "problems": problems}
    assessment = request["assessment"]
    goals = {goal["goal_id"]: goal for goal in request["active_goals"]}
    status = assessment["status"]
    if status == "goal_found":
        goal = goals[assessment["goal_ids"][0]]
        route, binding_goal = "bind", goal
    elif status == "goal_stale":
        route, binding_goal = "grill_and_bind", request["proposed_goal"]
    elif status == "goal_missing":
        route, binding_goal = "ask_user", None
    else:
        route, binding_goal = "surface_conflict", None
    binding = {
        "schema": BINDING_SCHEMA,
        "trigger": request["trigger"],
        "assessment": assessment,
        "route": route,
        "goal_id": binding_goal["goal_id"] if binding_goal else None,
        "goal_digest": digest(binding_goal) if binding_goal else None,
        "problems": [],
    }
    return {"verdict": "pass", "status": "goal_binding_ready", "binding": binding, "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an LH Goal Discovery / Goal Bind request")
    parser.add_argument("request")
    args = parser.parse_args()
    try:
        request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"verdict": "ng", "status": "invalid_goal_discovery_request", "problems": [str(exc)]}, ensure_ascii=False, indent=2))
        return 2
    result = resolve(request)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())

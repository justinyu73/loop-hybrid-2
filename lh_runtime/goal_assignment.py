"""Immutable project and criterion assignment carried by an LH Goal revision.

The assignment is authored before execution and stored inside the Goal payload.
It binds one project to the exact Goal revision and binds stable criterion ids
to target-owned check definitions. Consumers receive a digest-bearing
projection; they do not infer a check from prose or from a later receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any


ASSIGNMENT_SCHEMA = "loop-hybrid-goal-assignment/v2"
PROJECTION_SCHEMA = "loop-hybrid-project-goal-assignment/v2"
VERIFICATION_BUDGET_SCHEMA = "loop-hybrid-verification-budget/v1"
COMMIT_RE = re.compile(r"^[0-9a-f]{40,64}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def _digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _verification_budget(value: Any) -> dict[str, Any]:
    """Normalize the LH-owned total verifier-time ceiling."""
    if not isinstance(value, dict):
        raise ValueError("goal_assignment.verification_budget must be an object")
    if value.get("schema") != VERIFICATION_BUDGET_SCHEMA:
        raise ValueError(
            "goal_assignment.verification_budget.schema must be "
            f"{VERIFICATION_BUDGET_SCHEMA}"
        )
    maximum = value.get("max_seconds")
    if not isinstance(maximum, (int, float)) or isinstance(maximum, bool):
        raise ValueError(
            "goal_assignment.verification_budget.max_seconds must be a "
            "positive finite number"
        )
    try:
        normalized_maximum = float(maximum)
    except (OverflowError, ValueError) as exc:
        raise ValueError(
            "goal_assignment.verification_budget.max_seconds must be a "
            "positive finite number"
        ) from exc
    if not math.isfinite(normalized_maximum) or normalized_maximum <= 0:
        raise ValueError(
            "goal_assignment.verification_budget.max_seconds must be a "
            "positive finite number"
        )
    return {
        "schema": VERIFICATION_BUDGET_SCHEMA,
        "max_seconds": normalized_maximum,
    }


def normalize_assignment(value: Any) -> dict[str, Any]:
    """Return the canonical assignment or reject an ambiguous binding."""
    if not isinstance(value, dict):
        raise ValueError("goal_assignment must be an object")
    if value.get("schema") != ASSIGNMENT_SCHEMA:
        raise ValueError(f"goal_assignment.schema must be {ASSIGNMENT_SCHEMA}")
    project_id = _text("goal_assignment.project_id", value.get("project_id"))
    assigner_ref = _text("goal_assignment.assigner_ref", value.get("assigner_ref"))
    base_revision = _text(
        "goal_assignment.base_revision",
        value.get("base_revision"),
    )
    if not COMMIT_RE.fullmatch(base_revision):
        raise ValueError(
            "goal_assignment.base_revision must be an exact 40-64 hex commit"
        )
    verification_budget = _verification_budget(
        value.get("verification_budget")
    )
    raw_criteria = value.get("criteria")
    if not isinstance(raw_criteria, list) or not raw_criteria:
        raise ValueError("goal_assignment.criteria must be a non-empty list")

    criteria: list[dict[str, str]] = []
    seen_criteria: set[str] = set()
    seen_checks: set[str] = set()
    for index, raw in enumerate(raw_criteria):
        if not isinstance(raw, dict):
            raise ValueError(f"goal_assignment.criteria[{index}] must be an object")
        criterion_id = _text(
            f"goal_assignment.criteria[{index}].criterion_id",
            raw.get("criterion_id"),
        )
        check_id = _text(
            f"goal_assignment.criteria[{index}].check_id",
            raw.get("check_id"),
        )
        if criterion_id in seen_criteria:
            raise ValueError(f"criterion_id is duplicated: {criterion_id}")
        if check_id in seen_checks:
            raise ValueError(f"check_id is bound more than once: {check_id}")
        seen_criteria.add(criterion_id)
        seen_checks.add(check_id)
        criterion_authority_digest = _text(
            f"goal_assignment.criteria[{index}].criterion_authority_digest",
            raw.get("criterion_authority_digest"),
        )
        check_definition_digest = _text(
            f"goal_assignment.criteria[{index}].check_definition_digest",
            raw.get("check_definition_digest"),
        )
        if not DIGEST_RE.fullmatch(criterion_authority_digest):
            raise ValueError(
                f"goal_assignment.criteria[{index}].criterion_authority_digest "
                "must be sha256:<64 lowercase hex>"
            )
        if not DIGEST_RE.fullmatch(check_definition_digest):
            raise ValueError(
                f"goal_assignment.criteria[{index}].check_definition_digest "
                "must be sha256:<64 lowercase hex>"
            )
        criteria.append({
            "criterion_id": criterion_id,
            "criterion_authority_ref": _text(
                f"goal_assignment.criteria[{index}].criterion_authority_ref",
                raw.get("criterion_authority_ref"),
            ),
            "criterion_authority_digest": criterion_authority_digest,
            "check_id": check_id,
            "check_definition_digest": check_definition_digest,
        })

    return {
        "schema": ASSIGNMENT_SCHEMA,
        "project_id": project_id,
        "assigner_ref": assigner_ref,
        "base_revision": base_revision,
        "verification_budget": verification_budget,
        "criteria": sorted(criteria, key=lambda item: item["criterion_id"]),
    }


def assignment_from_goal(goal: Any) -> dict[str, Any] | None:
    """Read the assignment from the immutable Goal payload or its envelope."""
    if not isinstance(goal, dict):
        return None
    value = goal.get("goal_assignment")
    if value is None:
        envelope = goal.get("admission_envelope")
        if isinstance(envelope, dict):
            value = envelope.get("goal_assignment")
    if value is None:
        return None
    return normalize_assignment(value)


def projection_for_goal(goal_record: dict[str, Any]) -> dict[str, Any] | None:
    revision = goal_record.get("current_revision")
    if not isinstance(revision, dict):
        return None
    assignment = assignment_from_goal(revision.get("goal"))
    if assignment is None:
        return None
    return {
        "schema": PROJECTION_SCHEMA,
        "status": "admitted",
        "project_id": assignment["project_id"],
        "goal_id": goal_record.get("goal_id"),
        "goal_state": goal_record.get("state"),
        "revision_id": revision.get("revision_id"),
        "revision": revision.get("revision"),
        "goal_digest": revision.get("goal_digest"),
        "assignment": assignment,
        "assignment_digest": _digest(assignment),
    }


def resolve_project(store: Any, project_id: str) -> dict[str, Any]:
    """Resolve one active LH Goal assignment for a project."""
    project_id = _text("project_id", project_id)
    matches: list[dict[str, Any]] = []
    invalid: list[dict[str, str]] = []
    for goal in store.goals_in_state("active"):
        try:
            projection = projection_for_goal(goal)
        except ValueError as exc:
            invalid.append({
                "goal_id": str(goal.get("goal_id") or ""),
                "reason": str(exc),
            })
            continue
        if projection is not None and projection["project_id"] == project_id:
            matches.append(projection)
    if invalid:
        return {
            "schema": PROJECTION_SCHEMA,
            "status": "rejected",
            "project_id": project_id,
            "reason": "invalid_active_goal_assignment",
            "invalid": invalid,
        }
    if not matches:
        return {
            "schema": PROJECTION_SCHEMA,
            "status": "unavailable",
            "project_id": project_id,
            "reason": "no_active_goal_assignment",
        }
    if len(matches) != 1:
        return {
            "schema": PROJECTION_SCHEMA,
            "status": "conflict",
            "project_id": project_id,
            "reason": "multiple_active_goal_assignments",
            "goal_ids": sorted(str(item["goal_id"]) for item in matches),
        }
    return matches[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True)
    parser.add_argument("--project", required=True)
    args = parser.parse_args(argv)

    try:
        from .goal_store import GoalStore
    except ImportError:  # direct script execution
        from goal_store import GoalStore

    result = resolve_project(GoalStore(Path(args.store)), args.project)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "admitted" else 1


if __name__ == "__main__":
    raise SystemExit(main())

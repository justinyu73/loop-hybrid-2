#!/usr/bin/env python3
"""Evaluate a bounded, shadow-only improvement proposal without applying it."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
PROPOSAL_SCHEMA = "loop-hybrid-improvement-proposal/v1"
SHADOW_SCHEMA = "loop-hybrid-improvement-shadow-results/v1"
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SURFACES = {"hook", "skill", "prompt", "routing_policy"}
REQUIRED_CANNOT_CLAIM = {
    "automatic promotion authority", "authenticated maintainer identity",
    "product acceptance", "permanent superiority",
}


def load_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("JSON root must be an object")
    return value


def _safe_file(value: Any, label: str, repo_root: Path) -> tuple[Path | None, str | None]:
    if not isinstance(value, str) or not value.strip():
        return None, f"{label} must be a non-empty repo-relative string"
    path = Path(value)
    if path.is_absolute():
        return None, f"{label} must be repo-relative"
    resolved = (repo_root / path).resolve()
    try:
        resolved.relative_to(repo_root.resolve())
    except ValueError:
        return None, f"{label} must remain inside the repository"
    if not resolved.is_file():
        return None, f"{label} must reference an existing file"
    return resolved, None


def validate_proposal(proposal: dict[str, Any], *, repo_root: Path = REPO_ROOT) -> list[str]:
    problems: list[str] = []
    if proposal.get("schema") != PROPOSAL_SCHEMA:
        problems.append(f"schema must be {PROPOSAL_SCHEMA}")
    if not isinstance(proposal.get("proposal_id"), str) or not SAFE_ID.fullmatch(proposal["proposal_id"]):
        problems.append("proposal_id must be a safe identifier")
    if proposal.get("status") != "proposed":
        problems.append("status must be proposed")
    if proposal.get("surface") not in SURFACES:
        problems.append(f"surface must be one of {sorted(SURFACES)}")
    if not isinstance(proposal.get("claim"), str) or not proposal["claim"].strip():
        problems.append("claim must be non-empty")
    if not isinstance(proposal.get("independent_falsifier_required"), bool):
        problems.append("independent_falsifier_required must be boolean")
    for field in ("target_ref", "baseline_ref", "candidate_ref", "rollback_artifact_ref"):
        _, error = _safe_file(proposal.get(field), field, repo_root)
        if error:
            problems.append(error)
    tasks = proposal.get("frozen_tasks")
    ids: list[str] = []
    if not isinstance(tasks, list) or not tasks:
        problems.append("frozen_tasks must be a non-empty array")
        tasks = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict) or not isinstance(task.get("task_id"), str):
            problems.append(f"frozen_tasks[{index}] must contain task_id")
            continue
        ids.append(task["task_id"])
        _, error = _safe_file(task.get("fixture_ref"), f"frozen_tasks[{index}].fixture_ref", repo_root)
        if error:
            problems.append(error)
    if len(ids) != len(set(ids)):
        problems.append("frozen task IDs must be unique")
    budget = proposal.get("shadow_budget")
    if not isinstance(budget, dict):
        problems.append("shadow_budget must be an object")
    else:
        for field, minimum in (("max_shadow_runs", 1), ("max_regressions", 0), ("min_improvements", 1)):
            if not isinstance(budget.get(field), int) or isinstance(budget.get(field), bool) or budget[field] < minimum:
                problems.append(f"shadow_budget.{field} must be an integer >= {minimum}")
        if not isinstance(budget.get("max_cost_increase_pct"), (int, float)) or isinstance(budget.get("max_cost_increase_pct"), bool):
            problems.append("shadow_budget.max_cost_increase_pct must be numeric")
        if isinstance(budget.get("max_shadow_runs"), int) and len(ids) > budget["max_shadow_runs"]:
            problems.append("frozen task count exceeds shadow budget")
    assertions = proposal.get("boundary_assertions")
    if not isinstance(assertions, dict) or any(assertions.get(key) is not False for key in ("changes_authority", "changes_acceptance", "changes_protected_core")):
        problems.append("boundary assertions must all be false")
    cannot_claim = proposal.get("cannot_claim")
    if not isinstance(cannot_claim, list) or not REQUIRED_CANNOT_CLAIM.issubset(set(cannot_claim)):
        problems.append(f"cannot_claim must include {sorted(REQUIRED_CANNOT_CLAIM)}")
    return problems


def run_local_shadow(proposal: dict[str, Any], *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    problems = validate_proposal(proposal, repo_root=repo_root)
    if problems:
        return {"verdict": "ng", "status": "invalid_proposal", "problems": problems}
    baseline = (repo_root / proposal["baseline_ref"]).read_text(encoding="utf-8").lower()
    candidate = (repo_root / proposal["candidate_ref"]).read_text(encoding="utf-8").lower()
    runs: list[dict[str, Any]] = []
    for task in proposal["frozen_tasks"]:
        fixture = load_json(repo_root / task["fixture_ref"])
        expected = fixture.get("expected_token")
        if not isinstance(expected, str) or not expected:
            return {"verdict": "ng", "status": "invalid_fixture", "problems": [f"{task['task_id']}: expected_token missing"]}
        baseline_pass, candidate_pass = expected.lower() in baseline, expected.lower() in candidate
        runs.append({
            "task_id": task["task_id"], "baseline_pass": baseline_pass, "candidate_pass": candidate_pass,
            "baseline_cost_units": fixture.get("baseline_cost_units"), "candidate_cost_units": fixture.get("candidate_cost_units"),
            "falsifier_verdict": "pass" if candidate_pass else "fail", "evidence_refs": [task["fixture_ref"]],
        })
    return {"verdict": "pass", "status": "executed", "shadow": {"schema": SHADOW_SCHEMA, "proposal_id": proposal["proposal_id"], "runs": runs}, "problems": []}


def evaluate_shadow(proposal: dict[str, Any], shadow: dict[str, Any], *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    problems = [*(f"proposal: {p}" for p in validate_proposal(proposal, repo_root=repo_root))]
    if shadow.get("schema") != SHADOW_SCHEMA or shadow.get("proposal_id") != proposal.get("proposal_id"):
        problems.append("shadow schema or proposal_id is invalid")
    runs = shadow.get("runs")
    expected = [task.get("task_id") for task in proposal.get("frozen_tasks", []) if isinstance(task, dict)]
    if not isinstance(runs, list) or sorted(run.get("task_id") for run in runs if isinstance(run, dict)) != sorted(expected):
        problems.append("shadow runs must cover each frozen task exactly once")
        runs = []
    for index, run in enumerate(runs):
        if not isinstance(run, dict):
            problems.append(f"runs[{index}] must be an object")
            continue
        for field in ("baseline_pass", "candidate_pass"):
            if not isinstance(run.get(field), bool):
                problems.append(f"runs[{index}].{field} must be boolean")
        for field in ("baseline_cost_units", "candidate_cost_units"):
            if not isinstance(run.get(field), (int, float)) or isinstance(run.get(field), bool) or run[field] < 0:
                problems.append(f"runs[{index}].{field} must be numeric >= 0")
        if run.get("falsifier_verdict") not in {"pass", "fail"}:
            problems.append(f"runs[{index}].falsifier_verdict must be pass or fail")
    if problems:
        return {"verdict": "ng", "status": "invalid", "problems": problems}
    regressions = sum(run["baseline_pass"] and not run["candidate_pass"] for run in runs)
    improvements = sum(not run["baseline_pass"] and run["candidate_pass"] and run["falsifier_verdict"] == "pass" for run in runs)
    falsifier_failures = sum(run["falsifier_verdict"] != "pass" for run in runs)
    base_cost = sum(float(run["baseline_cost_units"]) for run in runs)
    candidate_cost = sum(float(run["candidate_cost_units"]) for run in runs)
    delta = 0.0 if base_cost == candidate_cost == 0 else float("inf") if base_cost == 0 else round((candidate_cost - base_cost) * 100 / base_cost, 4)
    budget = proposal["shadow_budget"]
    reasons = []
    if regressions > budget["max_regressions"]: reasons.append("regression budget exceeded")
    if falsifier_failures: reasons.append("falsifier failed")
    if delta > budget["max_cost_increase_pct"]: reasons.append("cost increase budget exceeded")
    status = "shadow_rejected" if reasons else "shadow_null" if improvements < budget["min_improvements"] else "shadow_passed"
    evidence = json.dumps({"proposal_id": proposal["proposal_id"], "runs": runs}, sort_keys=True).encode()
    return {"verdict": "pass", "status": status, "proposal_id": proposal["proposal_id"], "improvements": improvements,
            "regressions": regressions, "falsifier_failures": falsifier_failures, "cost_delta_pct": delta,
            "rejection_reasons": reasons, "evidence_ref": "shadow-evaluation:sha256:" + hashlib.sha256(evidence).hexdigest(),
            "mutation_performed": False, "problems": []}


def is_constitutional_surface(target_ref: Any) -> bool:
    return target_ref == "AGENTS.md" or isinstance(target_ref, str) and target_ref.startswith((
        "gate-pack/boundary_seal/", "gate-pack/quota/", "gate-pack/ceremony_grader.py", "gate-pack/verify.sh",
    ))


def decide(proposal: dict[str, Any], evaluation: dict[str, Any]) -> dict[str, Any]:
    if evaluation.get("verdict") != "pass":
        return {"verdict": "ng", "status": "invalid_evaluation", "mutation_performed": False, "problems": evaluation.get("problems", [])}
    if evaluation["status"] == "shadow_rejected":
        status = "automatic_reject_recorded"
    elif evaluation["status"] == "shadow_null":
        status = "null_result_recorded"
    elif is_constitutional_surface(proposal.get("target_ref")):
        status = "promotion_waiting_human"
    elif proposal.get("independent_falsifier_required") and evaluation["regressions"] == 0 and evaluation["falsifier_failures"] == 0:
        status = "evidence_authorized"
    else:
        status = "promotion_waiting_human"
    return {"verdict": "pass", "status": status, "proposal_id": proposal.get("proposal_id"),
            "target_ref": proposal.get("target_ref"), "evidence_ref": evaluation.get("evidence_ref"),
            "evidence_authorization": {"shadow_pass": evaluation["status"] == "shadow_passed", "independent_falsifier_pass": evaluation["falsifier_failures"] == 0,
                                       "zero_regression": evaluation["regressions"] == 0, "provider_calls": 0, "fake_evidence": False} if status == "evidence_authorized" else None,
            "mutation_performed": False,
            "cannot_claim": ["candidate applied", "automatic promotion authority", "product acceptance"], "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate a shadow-only improvement proposal")
    parser.add_argument("proposal")
    parser.add_argument("--shadow", help="use supplied shadow JSON instead of local fixture execution")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    proposal = load_json(args.proposal)
    shadow_result = {"verdict": "pass", "shadow": load_json(args.shadow)} if args.shadow else run_local_shadow(proposal)
    evaluation = evaluate_shadow(proposal, shadow_result.get("shadow", {})) if shadow_result["verdict"] == "pass" else shadow_result
    decision = decide(proposal, evaluation)
    print(json.dumps({"evaluation": evaluation, "decision": decision}, ensure_ascii=False, indent=2))
    return 0 if evaluation.get("verdict") == decision.get("verdict") == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())

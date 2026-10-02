#!/usr/bin/env python3
"""Bounded live acceptance for the durable FailureCase escalation chain.

The target repository is read at one exact commit and every attempt runs in a
disposable clone.  The only durable project write is to the selected RunStore;
the acceptance GoalStore, workspaces, and final evidence live under an
operator-selected state directory.

Dry-run is the default.  ``--execute`` is required before any store is opened.
"""
from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
import delivery_contract as contract_engine
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore


EVIDENCE_SCHEMA = "lh-failure-case-live-acceptance/v1"
STAGE_ID = "repeated-failure-repair"
DIAGNOSIS = "Replace the repeated wrong marker with the exact fixed marker required by the approved lamp."
EXPECTED_EVENTS = [
    "failure_case_opened",
    "grill_result",
    "resolution_checked",
    "grill_result",
    "resolution_checked",
    "failure_case_closed",
]
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _validated_id(name: str, value: str) -> str:
    if not _ID_RE.fullmatch(value):
        raise ValueError(f"{name} must match {_ID_RE.pattern}")
    return value


def _validated_marker(marker: str, allowed_path: str) -> tuple[str, str]:
    marker_path = Path(marker)
    allowed = allowed_path.strip().strip("/") + "/"
    if (
        not allowed_path.strip()
        or marker_path.is_absolute()
        or ".." in marker_path.parts
        or marker.endswith("/")
        or not marker.startswith(allowed)
    ):
        raise ValueError("marker-path must be a file below allowed-path")
    return marker, allowed


def _campaign(campaign_id: str, marker: str, allowed_path: str) -> dict[str, Any]:
    quoted = shlex.quote(marker)
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": campaign_id,
        "stages": [
            {
                "stage_id": STAGE_ID,
                "goal": {
                    "feature_contract": "exercise repeated FailureCase escalation and deterministic repair",
                },
                "allowed_paths": [allowed_path],
                "allowed_side_effects": ["workspace", "artifact"],
                "acceptance_lamp": {
                    "id": "fc-p0-live-marker",
                    "smoke": f"{marker} contains exactly fixed",
                    "verification_argv": [
                        "sh",
                        "-c",
                        f"test -f {quoted} && grep -qx fixed {quoted}",
                    ],
                },
                "max_attempts": 4,
                "next_stage_id": None,
            }
        ],
    }


def _failure_case_delivery_binding(
    _repo: Path,
    _base: str,
    goal_id: str,
    node_id: str,
    checks: list[dict[str, Any]],
    verifier_argv: list[str],
    allowed_paths: list[str],
    max_attempts: int,
    *,
    goal: dict[str, Any],
) -> dict[str, Any]:
    """Build this command's explicit RunStore binding through LH's engine."""
    forbidden_paths = [".git/", "secrets/", "credentials/", "cookies/"]
    contract = contract_engine.seal_contract({
        "schema": contract_engine.SCHEMA,
        "contract_version": 1,
        "contract_id": f"failure-case-contract-{goal_id}-{node_id}",
        "unit_id": f"failure-case-unit-{goal_id}",
        "goal": {"id": goal_id, "revision": 1},
        "node": {"id": node_id, "kind": "failure-case-live-acceptance"},
        "planner": {"principal": "failure-case-live-acceptance", "source": "approved acceptance definition"},
        "independent_verifier": {
            "principal": "failure-case-live-verifier",
            "read_only": True,
            "source_write": False,
            "capability": "failure-case-bounded-marker-verifier",
            "argv": list(verifier_argv),
            "cwd": "${WORKTREE}",
            "timeout_seconds": 30,
        },
        "outcome": {
            "observable": "RunStore reaches verified after deterministic FailureCase repair",
            "start_state": "queued",
            "success_state": "verified",
            "terminal_states": ["verified", "human_required", "exhausted"],
        },
        "scope": {
            "ownership": "failure-case-live-acceptance",
            "allowed_paths": list(allowed_paths),
            "forbidden_paths": forbidden_paths,
            "identity": [
                "unit_id", "goal_id", "goal_revision", "node_id", "dispatch_key",
                "run_id", "attempt", "fence", "base_sha", "diff_digest",
            ],
        },
        "obligations": checks,
        "required_receipts": [
            "plan_verdict", "packet_admission", "dispatch", "executor",
            "delivery_verifier", "completion",
        ],
        "source_required_receipts": [
            "plan_verdict", "packet_admission", "dispatch", "executor",
            "delivery_verifier",
        ],
        "source_obligation_ids": [item["id"] for item in checks],
        "source_vs_live": {
            "source_must_not_claim_live": True,
            "live_required_for_source_delivery": False,
        },
        "repair_same_unit": {
            "enabled": True,
            "route": "same_work_unit_new_attempt",
            "identity_fields": ["unit_id", "dispatch_key", "goal_id", "node_id"],
            "max_attempts": int(max_attempts),
            "scope_drift_route": "planner_required",
            "unknown_outcome_route": "reconcile_before_retry",
        },
        "authority_store": "run",
        "managed_scope": "failure-case-live-acceptance",
    })
    plan = contract_engine.plan_delivery_unit(contract)
    packet = contract_engine.bind_packet(
        {
            "schema": "host-delivery-unit-packet/v1",
            "packet_id": f"failure-case-packet-{goal_id}-{node_id}",
            "goal_id": goal_id,
            "goal_revision": 1,
            "node_id": node_id,
            "write_set": list(allowed_paths),
            "forbidden_paths": forbidden_paths,
        },
        plan,
        contract,
    )
    admission = dict(goal.get("admission_envelope") or {})
    admission.update({
        "delivery_contract": contract,
        "delivery_plan": plan,
        "delivery_packet": packet,
        "allowed_paths": list(allowed_paths),
        "forbidden_paths": forbidden_paths,
    })
    return {
        "goal": {
            **goal,
            "goal_id": goal_id,
            "goal_revision": 1,
            "node_id": node_id,
            "unit_id": contract["unit_id"],
            "delivery_required": True,
            "delivery_phase": "sync",
            "delivery_contract": contract,
            "delivery_plan": plan,
            "delivery_packet": packet,
            "admission_envelope": admission,
        },
        "contract": contract,
        "plan": plan,
        "packet": packet,
    }


def _seed(
    worker: GoalLoopWorker,
    *,
    event_key: str,
    campaign_id: str,
    goal_id: str,
    marker_path: str,
    allowed_path: str,
) -> dict[str, Any]:
    envelope = worker.compilers[campaign_id].compile()["stages"][STAGE_ID]
    context = worker.execution_context[campaign_id]
    binding = _failure_case_delivery_binding(
        context["source_repo"],
        context["base_revision"],
        goal_id,
        STAGE_ID,
        [{
            "id": "failure-case-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        [sys.executable, "-B", "-c", "from pathlib import Path; import sys; p=Path(sys.argv[1]); raise SystemExit(0 if p.is_file() and p.read_text(encoding='utf-8') == 'fixed\\n' else 1)", marker_path],
        [allowed_path],
        int(envelope["max_attempts"]),
        goal={"feature_contract": envelope["goal"], "admission_envelope": envelope},
    )
    return worker.goal_store.record_event(
        event_id=f"evt-{event_key}",
        idempotency_key=event_key,
        source="fc_p0_live_acceptance",
        event_type="goal_candidate",
        payload={
            "candidate": {
                "goal_id": goal_id,
                "campaign_id": campaign_id,
                "stage_id": STAGE_ID,
                "goal": {
                    **binding["goal"],
                },
            }
        },
    )


def _model(marker: str, capsules: list[dict[str, Any]]):
    def execute(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        capsules.append(dict(capsule))
        target = workspace / marker
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("fixed\n" if int(capsule["attempt"]) >= 4 else "same wrong\n", encoding="utf-8", newline="")
        return {"summary": "bounded FailureCase live-acceptance fixture"}

    return execute


def _grill(snapshots: list[dict[str, Any]]):
    def judge(snapshot: dict[str, Any]) -> dict[str, str]:
        snapshots.append(snapshot)
        return {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}

    return judge


def _tick_projection(result: dict[str, Any]) -> dict[str, Any]:
    run = result.get("run") if isinstance(result.get("run"), dict) else {}
    terminal = result.get("terminal_after") if isinstance(result.get("terminal_after"), dict) else {}
    case = run.get("failure_case") if isinstance(run.get("failure_case"), dict) else {}
    terminal_case = terminal.get("failure_case") if isinstance(terminal.get("failure_case"), dict) else {}
    return {
        "status": result.get("status"),
        "run_status": run.get("status"),
        "attempt": run.get("attempt"),
        "failure_state": case.get("state"),
        "terminal_status": terminal.get("status"),
        "terminal_next_node": terminal_case.get("next_node_id"),
    }


def _existing_goal(store: GoalStore, goal_id: str) -> dict[str, Any] | None:
    try:
        return store.get_goal(goal_id)
    except KeyError:
        return None


def run_acceptance(
    *,
    project_id: str,
    acceptance_id: str,
    source_repo: Path,
    base_revision: str,
    goal_store_root: Path,
    run_store_root: Path,
    workspace_root: Path,
    evidence_out: Path,
    marker_path: str,
    allowed_path: str,
    max_cycles: int = 8,
) -> dict[str, Any]:
    project_id = _validated_id("project-id", project_id)
    acceptance_id = _validated_id("acceptance-id", acceptance_id)
    marker_path, allowed_path = _validated_marker(marker_path, allowed_path)
    source_repo = source_repo.resolve()
    if not (source_repo / ".git").exists():
        raise ValueError("source-repo must be a Git working tree")
    resolved_base = _git(source_repo, "rev-parse", "--verify", f"{base_revision}^{{commit}}").stdout.strip()
    if _git(source_repo, "cat-file", "-e", f"{resolved_base}:{marker_path}", check=False).returncode == 0:
        raise ValueError("marker-path already exists at base-revision")

    campaign_id = f"fc-p0-{project_id}-{acceptance_id}"
    goal_id = f"{campaign_id}:{STAGE_ID}"
    event_key = f"fc-p0-live:{project_id}:{acceptance_id}"
    compiler = CampaignCompiler(_campaign(campaign_id, marker_path, allowed_path))
    goals = GoalStore(goal_store_root)
    runs = RunStore(run_store_root)
    controller = LoopController(runs, workspace_root)
    capsules: list[dict[str, Any]] = []
    grill_snapshots: list[dict[str, Any]] = []
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=controller,
        compilers={campaign_id: compiler},
        execution_context={
            campaign_id: {
                "source_repo": source_repo,
                "base_revision": resolved_base,
            }
        },
        grill_runner=_grill(grill_snapshots),
    )

    before_goal = _existing_goal(goals, goal_id)
    before_attempts = 0
    if before_goal is not None and isinstance(before_goal.get("run_id"), str):
        before_attempts = int(runs.get_run(before_goal["run_id"])["attempts"])
    seed = _seed(
        worker,
        event_key=event_key,
        campaign_id=campaign_id,
        goal_id=goal_id,
        marker_path=marker_path,
        allowed_path=allowed_path,
    )
    ticks: list[dict[str, Any]] = []
    for _ in range(max_cycles):
        goal = _existing_goal(goals, goal_id)
        if goal is not None and goal["state"] == "completed":
            break
        ticks.append(_tick_projection(worker.tick(
            holder=f"fc-p0-{project_id}-{acceptance_id}",
            model=_model(marker_path, capsules),
        )))

    goal = goals.get_goal(goal_id)
    run_id = goal.get("run_id")
    if not isinstance(run_id, str):
        raise RuntimeError("acceptance goal did not produce a run")
    run = runs.get_run(run_id)
    case = runs.failure_case_for_run(run_id)
    outbox = [] if case is None else [
        item for item in runs.failure_outbox()
        if item["failure_case_id"] == case["failure_case_id"]
    ]
    event_types = [item["event_type"] for item in outbox]
    invariants = {
        "goal_completed": goal["state"] == "completed",
        "run_verified": run["state"] == "verified",
        "four_attempts": int(run["attempts"]) == 4,
        "failure_case_resolved": case is not None and case["state"] == "resolved",
        "second_grill_generation": case is not None and int(case["grill_generation"]) == 2,
        "next_node_reported": case is not None and case["next_node_id"] == "campaign_completed",
        "full_outbox_chain": event_types == EXPECTED_EVENTS,
        "two_runner_fixable_results": [
            item["payload"].get("decision")
            for item in outbox
            if item["event_type"] == "grill_result"
        ] == ["runner-fixable", "runner-fixable"],
        "checker_failed_then_passed": [
            item["payload"].get("machine_resolved")
            for item in outbox
            if item["event_type"] == "resolution_checked"
        ] == [False, True],
    }
    result = {
        "schema": EVIDENCE_SCHEMA,
        "status": "pass" if all(invariants.values()) else "fail",
        "mode": "replay" if seed["status"] == "reused" and before_goal is not None else "execute",
        "project_id": project_id,
        "acceptance_id": acceptance_id,
        "source": {
            "repo": str(source_repo),
            "base_revision": resolved_base,
            "working_tree_modified": False,
        },
        "campaign_id": campaign_id,
        "goal": {"goal_id": goal_id, "state": goal["state"]},
        "run": {
            "run_id": run_id,
            "state": run["state"],
            "attempts": int(run["attempts"]),
            "new_attempts": int(run["attempts"]) - before_attempts,
        },
        "failure_case": case,
        "outbox": [
            {
                "sequence": item["sequence"],
                "event_id": item["event_id"],
                "event_type": item["event_type"],
                "payload": item["payload"],
            }
            for item in outbox
        ],
        "event_types": event_types,
        "invocation": {
            "cycles": len(ticks),
            "ticks": ticks,
            "model_calls": len(capsules),
            "grill_calls": len(grill_snapshots),
        },
        "invariants": invariants,
        "side_effect_boundary": {
            "source_repo": "read_only",
            "attempt_workspaces": "disposable_clones",
            "durable_project_write": str(run_store_root.resolve()),
            "network_operations": "none",
        },
    }
    evidence_out.parent.mkdir(parents=True, exist_ok=True)
    temporary = evidence_out.with_suffix(evidence_out.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="")
    temporary.replace(evidence_out)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--acceptance-id", required=True)
    parser.add_argument("--source-repo", type=Path, required=True)
    parser.add_argument("--base-revision", required=True)
    parser.add_argument("--goal-store", type=Path, required=True)
    parser.add_argument("--run-store", type=Path, required=True)
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--evidence-out", type=Path, required=True)
    parser.add_argument("--allowed-path", required=True)
    parser.add_argument("--marker-path", required=True)
    parser.add_argument("--max-cycles", type=int, default=8)
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute:
        print(json.dumps({
            "schema": EVIDENCE_SCHEMA,
            "status": "dry_run",
            "project_id": args.project_id,
            "acceptance_id": args.acceptance_id,
            "source_repo": str(args.source_repo.resolve()),
            "base_revision": args.base_revision,
            "run_store": str(args.run_store.resolve()),
            "evidence_out": str(args.evidence_out.resolve()),
            "execute_required": True,
        }, ensure_ascii=False, indent=2))
        return 0
    if not 4 <= args.max_cycles <= 20:
        raise SystemExit("--max-cycles must be between 4 and 20")
    result = run_acceptance(
        project_id=args.project_id,
        acceptance_id=args.acceptance_id,
        source_repo=args.source_repo,
        base_revision=args.base_revision,
        goal_store_root=args.goal_store,
        run_store_root=args.run_store,
        workspace_root=args.workspace_root,
        evidence_out=args.evidence_out,
        marker_path=args.marker_path,
        allowed_path=args.allowed_path,
        max_cycles=args.max_cycles,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

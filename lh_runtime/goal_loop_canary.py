#!/usr/bin/env python3
"""Committed G5 smoke: one serial worker, restart, retry, verdict poll, lease."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from external_verdict import VerdictStore
from admission_bridge import GoalAdmissionBridge
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign() -> dict:
    def stage(stage_id: str, next_stage_id: str | None) -> dict:
        return {
            "stage_id": stage_id,
            "goal": {"feature_contract": stage_id},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": stage_id + "-lamp", "smoke": "a staged change exists", "verification_argv": ["sh", "-c", "! git diff --cached --quiet"]},
            "max_attempts": 4,
            "next_stage_id": next_stage_id,
        }
    return {"schema": CAMPAIGN_SCHEMA, "campaign_id": "campaign-g5", "stages": [stage("stage-1", "stage-2"), stage("stage-2", None)]}


def seed_candidate(store: GoalStore, compiler: CampaignCompiler, *, goal_id: str, stage_id: str, event_key: str) -> dict:
    envelope = compiler.compile()["stages"][stage_id]
    event = store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": "campaign-g5", "stage_id": stage_id, "goal": {"feature_contract": stage_id, "admission_envelope": envelope}}
    })
    return event


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "g5 bounded model fixture"}


def failing_model(workspace: Path, capsule: dict) -> dict:
    # Vary the output per attempt: the W6b no-progress line stops a run after
    # two consecutive identical failure signatures, and this fixture exercises
    # the retry path itself, so its failures must differ each attempt.
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "g5 retry fixture"}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source = root / "source"
        source.mkdir()
        git("init", "-q", str(source))
        git("-C", str(source), "config", "user.email", "g5@example.invalid")
        git("-C", str(source), "config", "user.name", "G5 Canary")
        (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
        git("-C", str(source), "add", "baseline.txt")
        git("-C", str(source), "commit", "-qm", "baseline")
        base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        goals = GoalStore(root / "goals")
        runs = RunStore(root / "runs")
        controller = LoopController(runs, root / "workspaces")
        compiler = CampaignCompiler(campaign())
        worker = GoalLoopWorker(goal_store=goals, run_store=runs, controller=controller, compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        seed_candidate(goals, compiler, goal_id="campaign-g5:stage-1", stage_id="stage-1", event_key="g5-seed-1")
        first = worker.tick(holder="worker-a", model=model)
        restarted = GoalLoopWorker(goal_store=GoalStore(root / "goals"), run_store=RunStore(root / "runs"), controller=LoopController(RunStore(root / "runs"), root / "workspaces-restart"), compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        second = restarted.tick(holder="worker-b", model=model)
        goals_after = GoalStore(root / "goals")
        runs_after = RunStore(root / "runs")

        retry_goals = GoalStore(root / "retry-goals")
        retry_runs = RunStore(root / "retry-runs")
        retry_controller = LoopController(retry_runs, root / "retry-workspaces")
        retry_worker = GoalLoopWorker(goal_store=retry_goals, run_store=retry_runs, controller=retry_controller, compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        retry_policy = compiler.compile()["stages"]["stage-2"]
        retry_policy["acceptance_lamp"] = {"id": "retry", "smoke": "always fail", "verification_argv": [sys.executable, "-c", "raise SystemExit(1)"]}
        retry_event = retry_goals.record_event(event_id="g5-retry-1", idempotency_key="g5-retry-1", source="manual_intent", event_type="goal_candidate", payload={"candidate": {"goal_id": "campaign-g5:retry", "campaign_id": "campaign-g5", "stage_id": "stage-2", "goal": {"feature_contract": "stage-2", "admission_envelope": retry_policy}}})
        retry_goals.create_candidate(retry_event["event_key"], goal_id="campaign-g5:retry", campaign_id="campaign-g5", stage_id="stage-2", goal={"feature_contract": "stage-2", "admission_envelope": retry_policy})
        retry_first = retry_worker.tick(holder="retry-a", model=failing_model)
        retry_second = GoalLoopWorker(goal_store=GoalStore(root / "retry-goals"), run_store=RunStore(root / "retry-runs"), controller=LoopController(RunStore(root / "retry-runs"), root / "retry-workspaces-2"), compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}}).tick(holder="retry-b", model=failing_model)

        verdict_goals = GoalStore(root / "verdict-goals")
        verdict_runs = RunStore(root / "verdict-runs")
        verdict_controller = LoopController(verdict_runs, root / "verdict-workspaces")
        verdict_policy = compiler.compile()["stages"]["stage-2"]
        seed_candidate(verdict_goals, compiler, goal_id="campaign-g5:verdict", stage_id="stage-2", event_key="g5-verdict-1")
        verdict_goals.create_candidate("g5-verdict-1", goal_id="campaign-g5:verdict", campaign_id="campaign-g5", stage_id="stage-2", goal={"feature_contract": "stage-2", "admission_envelope": verdict_policy})
        GoalAdmissionBridge(verdict_goals, verdict_runs).admit("campaign-g5:verdict", source_repo=source, base_revision=base, envelope=verdict_policy)
        verdict_goal = verdict_goals.get_goal("campaign-g5:verdict")
        verdict_run_id = verdict_goal["run_id"]
        verdict_runs.begin_attempt(verdict_run_id, "workspace://verdict/1")
        verdict_store = VerdictStore(root / "verdicts")
        verdict_store.park(verdict_run_id, "op-g5-verdict", {"request": {}}, at=1.0)
        verdict_runs.park_external_verdict(verdict_run_id, 1, receipt_ref="missing-receipt", receipt_digest="sha256:g5")
        polled = GoalLoopWorker(goal_store=GoalStore(root / "verdict-goals"), run_store=RunStore(root / "verdict-runs"), controller=LoopController(RunStore(root / "verdict-runs"), root / "verdict-workspaces-2"), compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}}).tick(holder="verdict-b", model=model, verdict_store=verdict_store, conclusion_source=lambda op_key: {"conclusion": "success"} if op_key == "op-g5-verdict" else None)

        lease_goals = GoalStore(root / "lease-goals")
        lease_event = lease_goals.record_event(event_id="lease-1", idempotency_key="lease-1", source="scheduled_tick", event_type="wake", payload={})
        lease_a = lease_goals.claim_event(lease_event["event_key"], "worker-a", seconds=60)
        lease_b = lease_goals.claim_event(lease_event["event_key"], "worker-b", seconds=60)
        lease_goals.release_event(lease_event["event_key"], "worker-a")
        cases = [
            case("serial-worker-runs-seed-and-emits-next-event", first["status"] == "progress" and first["run"]["status"] == "verified" and first["terminal_after"]["status"] == "completed_with_next_event" and goals_after.get_event(first["terminal_after"]["derived_event_key"])["state"] == "completed", str(first)),
            case("restart-claims-next-event-and-runs-it-once", second["status"] == "progress" and second["run"]["status"] == "verified" and goals_after.get_goal("campaign-g5:stage-2")["state"] == "completed" and runs_after.summary()["runs_by_state"].get("verified") == 2, str(second)),
            case("retry-pending-is_reused_by_restart", retry_first["run"]["status"] == "retry_pending" and retry_second["run"]["status"] == "retry_pending" and retry_first["run"]["run_id"] == retry_second["run"]["run_id"], str(retry_second)),
            case("startup-polls-external-verdict", polled["external_resumed"] == [{"run_id": verdict_run_id, "op_key": "op-g5-verdict", "conclusion": "success", "state": "verified"}], "external verdict resumed"),
            case("event-lease-excludes-second-worker", lease_a is True and lease_b is False, str({"worker_a": lease_a, "worker_b": lease_b})),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-loop-g5",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/goal_loop_canary.py", "worker_mode": "single_serial_tick", "temporary_state": "isolated temp directories"},
        "known_gaps_open": ["G5 remains provider-free; G6 external adapters and promotion are not part of this worker."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

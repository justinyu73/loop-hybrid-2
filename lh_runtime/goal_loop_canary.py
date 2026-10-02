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
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
import external_action_port as eap
from external_verdict import VerdictStore
from admission_bridge import GoalAdmissionBridge
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from native_delivery_fixture import make_native_bundle
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign(source: Path, base: str) -> dict:
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
    campaign = {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": "campaign-g5",
        "stages": [stage("stage-1", "stage-2"), stage("stage-2", None)],
    }
    # Stage completion is an authorized producer boundary.  The compiler may
    # derive the next Goal, but it must carry a binding supplied by the
    # persisted stage definition; it may not invent one from a stage name.
    # Build those bindings from this canary's explicit source/check/verifier
    # declaration so the successor path exercises the same real admission
    # bridge as a directly submitted candidate.
    compiled = CampaignCompiler(campaign).compile()["stages"]
    for stage_row in campaign["stages"]:
        stage_id = stage_row["stage_id"]
        binding = make_native_bundle(
            source,
            base,
            f"campaign-g5:{stage_id}",
            stage_id,
            [{
                "id": "goal-loop-source-check",
                "commands": [{
                    "id": "diff-check",
                    "argv": ["git", "diff", "--cached", "--check"],
                    "cwd": "${WORKTREE}",
                    "expect_exit": 0,
                    "timeout_seconds": 10,
                }],
                "required_receipts": ["executor"],
            }],
            [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if any(Path('src').glob('attempt-*.txt')) else 1)"],
            ["src/"],
            int(stage_row["max_attempts"]),
            phase="sync",
            goal={"feature_contract": stage_row["goal"], "admission_envelope": compiled[stage_id]},
        )
        stage_row["goal"] = {
            **stage_row["goal"],
            "delivery_required": True,
            "delivery_contract": binding["contract"],
            "delivery_plan": binding["plan"],
            "delivery_packet": binding["packet"],
        }
    return campaign


def seed_candidate(
    store: GoalStore,
    compiler: CampaignCompiler,
    *,
    goal_id: str,
    stage_id: str,
    event_key: str,
    source: Path,
    base: str,
    envelope: dict | None = None,
) -> dict:
    envelope = envelope if envelope is not None else compiler.compile()["stages"][stage_id]
    binding = make_native_bundle(
        source,
        base,
        goal_id,
        stage_id,
        [{
            "id": "goal-loop-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if any(Path('src').glob('attempt-*.txt')) else 1)"],
        ["src/"],
        int(envelope.get("max_attempts", 4)),
        phase="async" if isinstance(envelope.get("external_verdict"), dict) else "sync",
        goal={"feature_contract": stage_id, "admission_envelope": envelope},
    )
    event = store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": "campaign-g5", "stage_id": stage_id, "goal": binding["goal"]}
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
        (source / "AGENTS.md").write_text("Canonical acceptance boundaries and restart receipts are authoritative.\n", encoding="utf-8")
        (source / "docs" / "contracts").mkdir(parents=True)
        (source / "docs" / "contracts" / "context.md").write_text("Use the durable receipt failure corpus when preparing a retry.\n", encoding="utf-8")
        goals = GoalStore(root / "goals")
        runs = RunStore(root / "runs", command_runner=fixture_command_runner)
        controller = LoopController(runs, root / "workspaces")
        compiler = CampaignCompiler(campaign(source, base))
        worker = GoalLoopWorker(goal_store=goals, run_store=runs, controller=controller, compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        seed_candidate(goals, compiler, goal_id="campaign-g5:stage-1", stage_id="stage-1", event_key="g5-seed-1", source=source, base=base)
        first = worker.tick(holder="worker-a", model=model)
        restarted = GoalLoopWorker(goal_store=GoalStore(root / "goals"), run_store=RunStore(root / "runs"), controller=LoopController(RunStore(root / "runs", command_runner=fixture_command_runner), root / "workspaces-restart"), compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        second = restarted.tick(holder="worker-b", model=model)
        goals_after = GoalStore(root / "goals")
        runs_after = RunStore(root / "runs")

        retry_goals = GoalStore(root / "retry-goals")
        retry_runs = RunStore(root / "retry-runs")
        retry_controller = LoopController(retry_runs, root / "retry-workspaces")
        retry_worker = GoalLoopWorker(goal_store=retry_goals, run_store=retry_runs, controller=retry_controller, compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}})
        retry_policy = compiler.compile()["stages"]["stage-2"]
        retry_policy["acceptance_lamp"] = {"id": "retry", "smoke": "always fail", "verification_argv": [sys.executable, "-c", "raise SystemExit(1)"]}
        seed_candidate(
            retry_goals,
            compiler,
            goal_id="campaign-g5:retry",
            stage_id="stage-2",
            event_key="g5-retry-1",
            source=source,
            base=base,
            envelope=retry_policy,
        )
        retry_first = retry_worker.tick(holder="retry-a", model=failing_model)
        retry_second = GoalLoopWorker(goal_store=GoalStore(root / "retry-goals"), run_store=RunStore(root / "retry-runs"), controller=LoopController(RunStore(root / "retry-runs"), root / "retry-workspaces-2"), compilers={"campaign-g5": compiler}, execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}}).tick(holder="retry-b", model=failing_model)

        verdict_goals = GoalStore(root / "verdict-goals")
        verdict_runs = RunStore(root / "verdict-runs", command_runner=fixture_command_runner)
        verdict_controller = LoopController(verdict_runs, root / "verdict-workspaces")
        verdict_policy = compiler.compile()["stages"]["stage-2"]
        verdict_policy.pop("acceptance_lamp", None)
        verdict_policy["external_verdict"] = {"action_id": "g5-external"}
        seed_candidate(
            verdict_goals,
            compiler,
            goal_id="campaign-g5:verdict",
            stage_id="stage-2",
            event_key="g5-verdict-1",
            source=source,
            base=base,
            envelope=verdict_policy,
        )
        verdict_store = VerdictStore(root / "verdicts")

        class Adapter:
            def perform(self, _op_key: str, _request: dict[str, object]) -> dict[str, object]:
                return {"head_sha": base, "pr_url": "https://github.invalid/pr/g5", "pr_number": 5}

        verdict_ledger = eap.ActionLedger(root / "verdict-actions")
        verdict_worker = GoalLoopWorker(
            goal_store=verdict_goals,
            run_store=verdict_runs,
            controller=verdict_controller,
            compilers={"campaign-g5": compiler},
            execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}},
            action_ledger=verdict_ledger,
            external_adapter=Adapter(),
        )
        dispatched = verdict_worker.tick(
            holder="verdict-a",
            model=model,
            verdict_store=verdict_store,
            conclusion_source=lambda _op_key: None,
        )
        dispatched_op_key = (
            dispatched.get("run", {}).get("op_key")
            if isinstance(dispatched.get("run"), dict)
            else None
        )
        verdict_goal = verdict_goals.get_goal("campaign-g5:verdict")
        verdict_run_id = verdict_goal["run_id"]
        polled = GoalLoopWorker(
            goal_store=GoalStore(root / "verdict-goals"),
            run_store=RunStore(root / "verdict-runs"),
            controller=LoopController(RunStore(root / "verdict-runs", command_runner=fixture_command_runner), root / "verdict-workspaces-2"),
            compilers={"campaign-g5": compiler},
            execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}},
            action_ledger=verdict_ledger,
            external_adapter=Adapter(),
        ).tick(
            holder="verdict-b",
            model=model,
            verdict_store=verdict_store,
            conclusion_source=lambda op_key: {"conclusion": "success"} if op_key else None,
        )

        lease_goals = GoalStore(root / "lease-goals")
        lease_event = lease_goals.record_event(event_id="lease-1", idempotency_key="lease-1", source="scheduled_tick", event_type="wake", payload={})
        lease_a = lease_goals.claim_event(lease_event["event_key"], "worker-a", seconds=60)
        lease_b = lease_goals.claim_event(lease_event["event_key"], "worker-b", seconds=60)
        lease_goals.release_event(lease_event["event_key"], "worker-a")

        context_store = KnowledgeStore(root / "context-knowledge")
        context_goals = GoalStore(root / "context-goals")
        context_runs = RunStore(root / "context-runs", command_runner=fixture_command_runner)
        context_worker = GoalLoopWorker(
            goal_store=context_goals,
            run_store=context_runs,
            controller=LoopController(context_runs, root / "context-workspaces"),
            compilers={"campaign-g5": compiler},
            execution_context={"campaign-g5": {"source_repo": source, "base_revision": base}},
            knowledge_store=context_store,
            knowledge_repo_roots=(source,),
        )
        seed_candidate(context_goals, compiler, goal_id="campaign-g5:context", stage_id="stage-1", event_key="g5-context-1", source=source, base=base)
        seen_context: dict[str, object] = {}

        def context_model(workspace: Path, capsule: dict) -> dict:
            seen_context["projection"] = capsule.get("provider_context_projection")
            seen_context["texts"] = capsule.get("provider_context_texts")
            return model(workspace, capsule)

        context_result = context_worker.tick(holder="context-worker", model=context_model)
        context_projection = seen_context.get("projection") if isinstance(seen_context.get("projection"), dict) else {}
        context_texts = seen_context.get("texts") if isinstance(seen_context.get("texts"), dict) else {}
        context_packet = (
            json.loads(context_texts["knowledge_context"])
            if isinstance(context_texts.get("knowledge_context"), str) else {}
        )
        cases = [
            case("serial-worker-runs-seed-and-emits-next-event", first["status"] == "progress" and first["run"]["status"] == "verified" and first["terminal_after"]["status"] == "completed_with_next_event" and goals_after.get_event(first["terminal_after"]["derived_event_key"])["state"] == "completed", str(first)),
            case("restart-claims-next-event-and-runs-it-once", second["status"] == "progress" and second["run"]["status"] == "verified" and goals_after.get_goal("campaign-g5:stage-2")["state"] == "completed" and runs_after.summary()["runs_by_state"].get("verified") == 2, str(second)),
            case("retry-pending-is_reused_by_restart", retry_first["run"]["status"] == "retry_pending" and retry_second["run"]["status"] == "retry_pending" and retry_first["run"]["run_id"] == retry_second["run"]["run_id"], str(retry_second)),
            case("startup-polls-external-verdict",
                 len(polled["external_resumed"]) == 1
                 and polled["external_resumed"][0].get("run_id") == verdict_run_id
                 and polled["external_resumed"][0].get("op_key") == dispatched_op_key
                 and isinstance(dispatched_op_key, str)
                 and dispatched_op_key.startswith("op-")
                 and polled["external_resumed"][0].get("conclusion") == "success"
                 and polled["external_resumed"][0].get("state") == "verified"
                 and polled["external_resumed"][0]["normalized"]["status"] == "ready"
                 and RunStore(root / "verdict-runs").get_run(verdict_run_id)["state"] == "verified"
                 and GoalStore(root / "verdict-goals").normalized_result_for(verdict_run_id, 1)["ready_event_key"] is not None,
                 str(polled["external_resumed"])),
            case("event-lease-excludes-second-worker", lease_a is True and lease_b is False, str({"worker_a": lease_a, "worker_b": lease_b})),
            case("next-goal-receives-bounded-provenanced-context", context_result["run"]["status"] == "verified" and context_packet.get("schema") == "loop-hybrid-goal-context/v1" and context_packet.get("authority") == "advisory_only" and context_packet.get("gate_mutation") == "forbidden" and 0 < int(context_packet.get("chars", 0)) <= 2400 and context_packet.get("hits") and context_projection.get("schema") == "lh-provider-context-projection/v1" and "knowledge_context" in context_projection.get("field_names", []) and context_projection.get("total_bytes", 0) <= 5120, json.dumps({"result": context_result, "projection": context_projection}, ensure_ascii=False)),
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

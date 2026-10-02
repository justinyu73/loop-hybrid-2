#!/usr/bin/env python3
"""Committed W9g/W9h smoke: terminal-run revival starts a fresh cycle.

Live evidence (2026-07-21, W9f day 1): a revived stopped goal whose linked
run was VERIFIED got the old run re-linked — admission only revision-bumped
on stopped runs — so the next tick consumed the stale receipt instead of
re-running the lamp. Day 2 then showed the twin gap (W9h): a COMPLETED goal
hit the worker's re-admission guard before admission could bump at all.
Proves, offline, that admission bumps the revision and creates a NEW run for
a verified terminal run (never re-linking it), that success-cycle bumps skip
the fail-loop revision cap, that stopped-run bumps keep the cap, that an
active goal with a queued run is untouched, that the revived run really
dispatches and re-runs its lamp, and that a completed goal revives the same
way (daily recurrence after a success), and that a new explicit candidate can
revive a human_required goal only after the normal admission policy is
re-evaluated.
"""
from __future__ import annotations

import json
import hashlib
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import make_source_repo
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from command_ingress import command_status
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore, MAX_GOAL_REVISIONS
from native_delivery_fixture import make_native_bundle
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner

CAMPAIGN_ID = "campaign-w9g"
STAGE_ID = "health"
GOAL_ID = f"{CAMPAIGN_ID}:{STAGE_ID}"


def _campaign() -> dict:
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "stages": [{
            "stage_id": STAGE_ID,
            "goal": {"feature_contract": "marker must read fixed"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": "health-lamp", "smoke": "src/out.txt reads fixed",
                                "verification_argv": ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"]},
            "max_attempts": 1,
            "next_stage_id": None,
        }],
    }


def _envelope(source: Path, base: str) -> dict:
    compiled = CampaignCompiler(_campaign()).compile()["stages"][STAGE_ID]
    binding = make_native_bundle(
        source,
        base,
        GOAL_ID,
        STAGE_ID,
        [{
            "id": "revival-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"],
        ["src/"],
        1,
        goal={"feature_contract": STAGE_ID, "admission_envelope": compiled},
    )
    return binding["goal"]["admission_envelope"]


def _seed_candidate(goals: GoalStore, event_key: str, envelope: dict) -> None:
    goals.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                      "goal": {"feature_contract": STAGE_ID, "admission_envelope": envelope}}
    })
    goals.create_candidate(event_key, goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                           goal={"feature_contract": STAGE_ID, "admission_envelope": envelope})
    goals.transition_event(event_key, "completed")


def _finish_verified(runs: RunStore, run_id: str) -> None:
    """Create a real final delivery before exercising revival semantics.

    Revival is a Run/Goal identity canary, but its terminal predecessor must
    still be a genuine delivery-bound verified Run.  The old fixture wrote a
    bare receipt and relied on ``finish_attempt``; mandatory delivery correctly
    leaves that attempt running, so later cycles were testing a stale fixture
    rather than revival.
    """
    ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
    run = runs.get_run(run_id)
    source = Path(run["source_repo"])
    with tempfile.TemporaryDirectory(prefix=f"revival-{run_id}-") as raw:
        candidate = Path(raw) / "candidate"
        subprocess.run(["git", "clone", "-q", str(source), str(candidate)], check=True, capture_output=True, text=True)
        target = candidate / "src" / "out.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("fixed\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(candidate), "add", "-A"], check=True, capture_output=True, text=True)
        diff = subprocess.run(
            ["git", "-C", str(candidate), "diff", "--cached", "--binary", "--relative"],
            check=True, capture_output=True, text=True,
        ).stdout
        diff_digest = "sha256:" + hashlib.sha256(diff.encode()).hexdigest()
        controller = LoopController(runs, runs.root.parent / "revival-workspaces")
        delivery = controller._record_delivery_from_provider(
            run_id=run_id,
            ordinal=ordinal,
            fence=runs.attempt_fence(run_id, ordinal),
            phase="final",
            provider={"summary": "revival terminal fixture"},
            workspace=candidate,
            diff_digest=diff_digest,
            changed_paths=["src/out.txt"],
            checker={
                "argv": ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"],
                "exit_code": 0,
                "stdout": "fixed\n",
                "stderr": "",
            },
            dispatch_key=f"revival:{run_id}:{ordinal}:{diff_digest}",
            terminal_state="verified",
        )
        if delivery.get("verdict") != "GREEN" or not isinstance(delivery.get("evidence"), dict):
            raise AssertionError(f"revival final delivery fixture was not GREEN: {delivery}")
        diff_ref = runs.write_artifact(run_id, ordinal, "diff.patch", diff)
        stdout_ref = runs.write_artifact(run_id, ordinal, "verifier.stdout", "fixed\n")
        stderr_ref = runs.write_artifact(run_id, ordinal, "verifier.stderr", "")
        receipt = {
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": run_id,
            "attempt": ordinal,
            "diff": diff_ref,
            "verification": {
                "argv": ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"],
                "exit_code": 0,
                "stdout": stdout_ref,
                "stderr": stderr_ref,
            },
        }
        ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
        if not runs.finish_attempt_with_delivery(
            run_id,
            ordinal,
            state="verified",
            receipt_ref=ref["ref"],
            receipt_digest=ref["digest"],
            evidence=delivery["evidence"],
            fence=runs.attempt_fence(run_id, ordinal),
        ):
            raise AssertionError("revival final delivery fixture could not become verified")


def _finish_stopped(runs: RunStore, run_id: str) -> None:
    ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "verification": {"argv": ["true"], "exit_code": 1}}
    ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    runs.finish_attempt(run_id, ordinal, state="stopped", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _finish_human_required(runs: RunStore, run_id: str) -> None:
    ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "routing": {"route": "human_required", "reason": "canary human gate"}}
    ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    runs.finish_attempt(run_id, ordinal, state="human_required", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _revive(goals: GoalStore) -> None:
    """The _process_event revival path: stopped -> candidate."""
    goals.transition_goal(GOAL_ID, "stopped", expected_state="active")
    goals.transition_goal(GOAL_ID, "candidate", expected_state="stopped")


def _fixed_model(calls: list[dict]):
    def model(workspace: Path, _capsule: dict) -> dict:
        calls.append({})
        src = workspace / "src"
        src.mkdir(exist_ok=True)
        (src / "out.txt").write_text("fixed\n", encoding="utf-8")
        return {"summary": "w9g revival fixture"}
    return model


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # Unit: verified old run -> admission bumps revision and creates a NEW run.
        goals = GoalStore(root / "goals")
        runs = RunStore(root / "runs", command_runner=fixture_command_runner)
        bridge = GoalAdmissionBridge(goals, runs)
        env = _envelope(source, base)
        _seed_candidate(goals, "w9g-seed-1", env)
        first = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        old_run_id = first["run_id"]
        _finish_verified(runs, old_run_id)
        _revive(goals)
        revived = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        old_run_after = runs.get_run(old_run_id)
        revived_revision = goals.get_goal(GOAL_ID)["current_revision"]["revision"]

        # Verified-terminal bump beyond the cap still admits.
        over_cap = revived
        while goals.get_goal(GOAL_ID)["current_revision"]["revision"] < MAX_GOAL_REVISIONS + 1:
            _finish_verified(runs, over_cap["run_id"])
            _revive(goals)
            over_cap = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        final_revision = goals.get_goal(GOAL_ID)["current_revision"]["revision"]

        # Stopped-terminal bump beyond the cap keeps revision_cap_reached.
        goals_s = GoalStore(root / "goals-s")
        runs_s = RunStore(root / "runs-s")
        bridge_s = GoalAdmissionBridge(goals_s, runs_s)
        _seed_candidate(goals_s, "w9g-seed-s", env)
        stop_cycle = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        while goals_s.get_goal(GOAL_ID)["current_revision"]["revision"] < MAX_GOAL_REVISIONS:
            _finish_stopped(runs_s, stop_cycle["run_id"])
            goals_s.transition_goal(GOAL_ID, "stopped", expected_state="active")
            goals_s.transition_goal(GOAL_ID, "candidate", expected_state="stopped")
            stop_cycle = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        _finish_stopped(runs_s, stop_cycle["run_id"])
        goals_s.transition_goal(GOAL_ID, "stopped", expected_state="active")
        goals_s.transition_goal(GOAL_ID, "candidate", expected_state="stopped")
        capped = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)

        # Active goal with a queued run: replay is unchanged.
        goals_q = GoalStore(root / "goals-q")
        runs_q = RunStore(root / "runs-q")
        bridge_q = GoalAdmissionBridge(goals_q, runs_q)
        _seed_candidate(goals_q, "w9g-seed-q", env)
        queued_first = bridge_q.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)
        queued_replay = bridge_q.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env)

        # End to end: the revived run dispatches and the lamp really re-runs.
        runs_e = RunStore(root / "runs-e", command_runner=fixture_command_runner)
        compiler = CampaignCompiler(_campaign())
        env_e = _envelope(source, base)
        compiler.stages[STAGE_ID] = env_e
        worker = GoalLoopWorker(
            goal_store=GoalStore(root / "goals-e"),
            run_store=runs_e,
            controller=LoopController(runs_e, root / "workspaces-e"),
            compilers={CAMPAIGN_ID: compiler},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        # Day 1: run finishes verified at the store level, then the goal stops
        # with the verified run still linked (the live day-1 setup).
        goals_e = worker.goal_store
        goals_e.record_event(event_id="w9g-e2e", idempotency_key="w9g-e2e", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": env_e}}
        })
        goals_e.create_candidate("w9g-e2e", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                 goal={"feature_contract": STAGE_ID, "admission_envelope": env_e})
        goals_e.transition_event("w9g-e2e", "completed")
        bridge_e = GoalAdmissionBridge(goals_e, runs_e)
        day1 = bridge_e.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env_e)
        run_e1 = day1["run_id"]
        _finish_verified(runs_e, run_e1)
        goals_e.transition_goal(GOAL_ID, "stopped", expected_state="active")
        goals_e.transition_goal(GOAL_ID, "candidate", expected_state="stopped")
        # Day 2: a standing-style command re-issues the work; the revived goal
        # must get a FRESH run that dispatches and re-runs the lamp.
        goals_e.record_event(event_id="w9g-e2e-day2", idempotency_key="w9g-e2e-day2", source="standing_intent", event_type="manual_intent",
                             payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID, "intent": "daily check"})
        calls: list[dict] = []
        worker.tick(holder="w9g", model=_fixed_model(calls))  # derives the candidate event
        tick_c = worker.tick(holder="w9g", model=_fixed_model(calls))  # admits revival + dispatches the fresh run
        run_e2 = tick_c.get("run", {}).get("run_id")
        run_e2_state = runs_e.get_run(run_e2)["state"] if run_e2 else None
        goal_e = goals_e.get_goal(GOAL_ID)["state"]

        # Completed-goal revival (W9h, day-2 recurrence): a COMPLETED goal
        # re-issued by a new command revives as candidate and gets a fresh run
        # (the worker guard accepts completed, admission bumps via W9g).
        runs_f = RunStore(root / "runs-f", command_runner=fixture_command_runner)
        compiler_f = CampaignCompiler(_campaign())
        env_f = _envelope(source, base)
        compiler_f.stages[STAGE_ID] = env_f
        worker_f = GoalLoopWorker(
            goal_store=GoalStore(root / "goals-f"),
            run_store=runs_f,
            controller=LoopController(runs_f, root / "workspaces-f"),
            compilers={CAMPAIGN_ID: compiler_f},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        goals_f = worker_f.goal_store
        goals_f.record_event(event_id="w9h-e2e", idempotency_key="w9h-e2e", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": env_f}}
        })
        goals_f.create_candidate("w9h-e2e", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                 goal={"feature_contract": STAGE_ID, "admission_envelope": env_f})
        goals_f.transition_event("w9h-e2e", "completed")
        bridge_f = GoalAdmissionBridge(goals_f, runs_f)
        day1_f = bridge_f.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env_f)
        run_f1 = day1_f["run_id"]
        _finish_verified(runs_f, run_f1)
        goals_f.transition_goal(GOAL_ID, "completed", expected_state="active")
        goals_f.record_event(event_id="w9h-e2e-day2", idempotency_key="w9h-e2e-day2", source="standing_intent", event_type="manual_intent",
                             payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID, "intent": "daily check"})
        calls_f: list[dict] = []
        worker_f.tick(holder="w9h", model=_fixed_model(calls_f))
        tick_f = worker_f.tick(holder="w9h", model=_fixed_model(calls_f))
        run_f2 = tick_f.get("run", {}).get("run_id")
        run_f2_state = runs_f.get_run(run_f2)["state"] if run_f2 else None
        goal_f = goals_f.get_goal(GOAL_ID)["state"]

        # Human-required revival: a new command may retry a parked goal, but
        # the same admission envelope/context checks still gate the fresh run.
        runs_h = RunStore(root / "runs-h", command_runner=fixture_command_runner)
        compiler_h = CampaignCompiler(_campaign())
        env_h = _envelope(source, base)
        compiler_h.stages[STAGE_ID] = env_h
        worker_h = GoalLoopWorker(
            goal_store=GoalStore(root / "goals-h"),
            run_store=runs_h,
            controller=LoopController(runs_h, root / "workspaces-h"),
            compilers={CAMPAIGN_ID: compiler_h},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        goals_h = worker_h.goal_store
        goals_h.record_event(event_id="w9i-seed", idempotency_key="w9i-seed", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": env_h}}
        })
        goals_h.create_candidate("w9i-seed", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                 goal={"feature_contract": STAGE_ID, "admission_envelope": env_h})
        goals_h.transition_event("w9i-seed", "completed")
        bridge_h = GoalAdmissionBridge(goals_h, runs_h)
        day1_h = bridge_h.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=env_h)
        run_h1 = day1_h["run_id"]
        _finish_verified(runs_h, run_h1)
        goals_h.transition_goal(GOAL_ID, "human_required", expected_state="active")
        goals_h.record_event(event_id="w9i-e2e", idempotency_key="w9i-e2e", source="manual_intent", event_type="manual_intent",
                             payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID, "intent": "retry daily check"})
        calls_h: list[dict] = []
        worker_h.tick(holder="w9i", model=_fixed_model(calls_h))
        tick_h = worker_h.tick(holder="w9i", model=_fixed_model(calls_h))
        run_h2 = tick_h.get("run", {}).get("run_id")
        run_h2_state = runs_h.get_run(run_h2)["state"] if run_h2 else None
        goal_h = goals_h.get_goal(GOAL_ID)
        event_h = goals_h.get_event("intent-derived:w9i-e2e")

        # Re-entry never bypasses an envelope refusal: a human-only candidate
        # returns to human_required without creating a Run.
        runs_n = RunStore(root / "runs-n")
        worker_n = GoalLoopWorker(
            goal_store=GoalStore(root / "goals-n"),
            run_store=runs_n,
            controller=LoopController(runs_n, root / "workspaces-n"),
            compilers={CAMPAIGN_ID: compiler_h},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        goals_n = worker_n.goal_store
        goals_n.record_event(event_id="w9j-seed", idempotency_key="w9j-seed", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": env_h}}
        })
        goals_n.create_candidate("w9j-seed", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                 goal={"feature_contract": STAGE_ID, "admission_envelope": env_h})
        goals_n.transition_event("w9j-seed", "completed")
        goals_n.transition_goal(GOAL_ID, "human_required", expected_state="candidate")
        refused_env = {**env_h, "human_only": True}
        goals_n.record_event(event_id="w9j-e2e", idempotency_key="w9j-e2e", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": refused_env}}
        })
        tick_n = worker_n.tick(holder="w9j", model=_fixed_model([]))
        event_n = goals_n.get_event("w9j-e2e")
        goal_n = goals_n.get_goal(GOAL_ID)

        # An unresolved human-required Run is a live human gate, not a
        # terminal cycle.  Re-entry must preserve that gate and command status
        # must not project the old Run/receipt onto the new event.
        runs_r = RunStore(root / "runs-r")
        goals_r = GoalStore(root / "goals-r")
        worker_r = GoalLoopWorker(
            goal_store=goals_r,
            run_store=runs_r,
            controller=LoopController(runs_r, root / "workspaces-r"),
            compilers={CAMPAIGN_ID: compiler_h},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        _seed_candidate(goals_r, "w9k-seed", env_h)
        parked_admit = GoalAdmissionBridge(goals_r, runs_r).admit(
            GOAL_ID, source_repo=source, base_revision=base, envelope=env_h,
        )
        parked_run = parked_admit["run_id"]
        _finish_human_required(runs_r, parked_run)
        goals_r.transition_goal(GOAL_ID, "human_required", expected_state="active")
        goals_r.record_event(event_id="w9k-e2e", idempotency_key="w9k-e2e", source="manual_intent", event_type="goal_candidate",
                             payload={"candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                                                     "goal": {"feature_contract": STAGE_ID, "admission_envelope": env_h}}})
        tick_r = worker_r.tick(holder="w9k", model=_fixed_model([]))
        event_r = goals_r.get_event("w9k-e2e")
        goal_r = goals_r.get_goal(GOAL_ID)
        status_r = command_status(goals_r, "w9k-e2e", runs_r)

        cases = [
            {"id": "verified-old-run-is-never-re-linked",
             "ok": revived["status"] == "active" and revived["run_id"] != old_run_id
             and revived["run_state"] == "queued"
             and old_run_after["state"] == "verified"
             and revived_revision == 2,
             "detail": json.dumps({"old": old_run_id[:20], "new": revived["run_id"][:20], "old_state": old_run_after["state"]})},
            {"id": "verified-bump-beyond-cap-still-admits",
             "ok": over_cap["status"] == "active" and final_revision == MAX_GOAL_REVISIONS + 1,
             "detail": json.dumps({"revision": final_revision, "status": over_cap["status"]})},
            {"id": "stopped-bump-beyond-cap-keeps-human-required",
             "ok": capped["status"] == "human_required" and "revision_cap_reached" in capped.get("reasons", []),
             "detail": json.dumps(capped)},
            {"id": "active-goal-with-queued-run-unchanged",
             "ok": queued_first["status"] == "active" and queued_replay["status"] == "reused"
             and queued_replay["run_id"] == queued_first["run_id"]
             and goals_q.get_goal(GOAL_ID)["current_revision"]["revision"] == 1,
             "detail": json.dumps({"first": queued_first["run_id"][:20], "replay": queued_replay["run_id"][:20]})},
            {"id": "revived-run-really-dispatches-and-reruns-lamp",
             "ok": run_e2 is not None and run_e2 != run_e1 and run_e2_state == "verified"
             and goal_e == "completed" and len(calls) == 1
             and runs_e.get_run(run_e1)["state"] == "verified",
             "detail": json.dumps({"day1_run": run_e1[:20], "day2_run": (run_e2 or "")[:20],
                                   "day2_state": run_e2_state, "goal": goal_e, "model_calls": len(calls)})},
            {"id": "completed-goal-revives-and-reruns",
             "ok": run_f2 is not None and run_f2 != run_f1 and run_f2_state == "verified"
             and goal_f == "completed" and len(calls_f) == 1,
             "detail": json.dumps({"day1_run": run_f1[:20], "day2_run": (run_f2 or "")[:20],
                                   "day2_state": run_f2_state, "goal": goal_f, "model_calls": len(calls_f)})},
            {"id": "human-required-goal-revives-and-reruns",
             "ok": run_h2 is not None and run_h2 != run_h1 and run_h2_state == "verified"
             and goal_h["state"] == "completed" and goal_h["current_revision"]["revision"] == 2
             and event_h["state"] == "completed" and event_h["goal_id"] == GOAL_ID
             and len(calls_h) == 1 and runs_h.get_run(run_h1)["state"] == "verified",
             "detail": json.dumps({"parked_run": run_h1[:20], "fresh_run": (run_h2 or "")[:20],
                                   "fresh_state": run_h2_state, "goal": goal_h["state"],
                                   "revision": goal_h["current_revision"]["revision"],
                                   "event_goal_id": event_h["goal_id"], "model_calls": len(calls_h)})},
            {"id": "human-required-revival-rechecks-human-only-policy",
             "ok": tick_n.get("event", {}).get("status") == "human_required"
             and "human_only_stage" in tick_n.get("event", {}).get("admission", {}).get("reasons", [])
             and event_n["state"] == "human_required" and event_n["goal_id"] is None
             and goal_n["state"] == "human_required" and runs_n.summary()["runs_by_state"] == {},
             "detail": json.dumps({"event_state": event_n["state"], "goal_state": goal_n["state"],
                                   "admission": tick_n.get("event", {}).get("admission"),
                                   "runs": runs_n.summary()["runs_by_state"]})},
            {"id": "human-required-run-is-not-reused",
             "ok": tick_r.get("event", {}).get("status") == "human_required"
             and "existing_run_human_required" in tick_r.get("event", {}).get("admission", {}).get("reasons", [])
             and event_r["state"] == "human_required" and event_r["goal_id"] is None
             and goal_r["state"] == "human_required" and goal_r["run_id"] == parked_run
             and runs_r.summary()["runs_by_state"] == {"human_required": 1},
             "detail": json.dumps({"parked_run": parked_run[:20], "event_state": event_r["state"],
                                   "goal_state": goal_r["state"], "goal_run_id": goal_r["run_id"],
                                   "admission": tick_r.get("event", {}).get("admission"),
                                   "runs": runs_r.summary()["runs_by_state"]})},
            {"id": "human-required-refusal-does-not-project-old-run",
             "ok": status_r["goal_id"] is None
             and status_r["execution"]["status"] == "not_started"
             and status_r["execution"]["run_id"] is None
             and status_r["execution"]["receipt"] is None,
             "detail": json.dumps({"goal_id": status_r["goal_id"], "execution": status_r["execution"]})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-run-revival",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/revival_canary.py",
                         "fixtures": "seeded stores and fixture models only; no provider"},
        "known_gaps_open": [],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

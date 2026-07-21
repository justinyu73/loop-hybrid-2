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
way (daily recurrence after a success).
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_source_repo
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore, MAX_GOAL_REVISIONS
from run_store import RunStore

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


def _envelope() -> dict:
    return CampaignCompiler(_campaign()).compile()["stages"][STAGE_ID]


def _seed_candidate(goals: GoalStore, event_key: str) -> None:
    goals.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                      "goal": {"feature_contract": STAGE_ID, "admission_envelope": _envelope()}}
    })
    goals.create_candidate(event_key, goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                           goal={"feature_contract": STAGE_ID, "admission_envelope": _envelope()})
    goals.transition_event(event_key, "completed")


def _finish_verified(runs: RunStore, run_id: str) -> None:
    ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "verification": {"argv": ["true"], "exit_code": 0}}
    ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    runs.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _finish_stopped(runs: RunStore, run_id: str) -> None:
    ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "verification": {"argv": ["true"], "exit_code": 1}}
    ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    runs.finish_attempt(run_id, ordinal, state="stopped", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


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
        runs = RunStore(root / "runs")
        bridge = GoalAdmissionBridge(goals, runs)
        _seed_candidate(goals, "w9g-seed-1")
        first = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        old_run_id = first["run_id"]
        _finish_verified(runs, old_run_id)
        _revive(goals)
        revived = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        old_run_after = runs.get_run(old_run_id)
        revived_revision = goals.get_goal(GOAL_ID)["current_revision"]["revision"]

        # Verified-terminal bump beyond the cap still admits.
        over_cap = revived
        while goals.get_goal(GOAL_ID)["current_revision"]["revision"] < MAX_GOAL_REVISIONS + 1:
            _finish_verified(runs, over_cap["run_id"])
            _revive(goals)
            over_cap = bridge.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        final_revision = goals.get_goal(GOAL_ID)["current_revision"]["revision"]

        # Stopped-terminal bump beyond the cap keeps revision_cap_reached.
        goals_s = GoalStore(root / "goals-s")
        runs_s = RunStore(root / "runs-s")
        bridge_s = GoalAdmissionBridge(goals_s, runs_s)
        _seed_candidate(goals_s, "w9g-seed-s")
        stop_cycle = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        while goals_s.get_goal(GOAL_ID)["current_revision"]["revision"] < MAX_GOAL_REVISIONS:
            _finish_stopped(runs_s, stop_cycle["run_id"])
            goals_s.transition_goal(GOAL_ID, "stopped", expected_state="active")
            goals_s.transition_goal(GOAL_ID, "candidate", expected_state="stopped")
            stop_cycle = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        _finish_stopped(runs_s, stop_cycle["run_id"])
        goals_s.transition_goal(GOAL_ID, "stopped", expected_state="active")
        goals_s.transition_goal(GOAL_ID, "candidate", expected_state="stopped")
        capped = bridge_s.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())

        # Active goal with a queued run: replay is unchanged.
        goals_q = GoalStore(root / "goals-q")
        runs_q = RunStore(root / "runs-q")
        bridge_q = GoalAdmissionBridge(goals_q, runs_q)
        _seed_candidate(goals_q, "w9g-seed-q")
        queued_first = bridge_q.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())
        queued_replay = bridge_q.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=_envelope())

        # End to end: the revived run dispatches and the lamp really re-runs.
        runs_e = RunStore(root / "runs-e")
        compiler = CampaignCompiler(_campaign())
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
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": compiler.compile()["stages"][STAGE_ID]}}
        })
        goals_e.create_candidate("w9g-e2e", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                 goal={"feature_contract": STAGE_ID, "admission_envelope": compiler.compile()["stages"][STAGE_ID]})
        goals_e.transition_event("w9g-e2e", "completed")
        bridge_e = GoalAdmissionBridge(goals_e, runs_e)
        day1 = bridge_e.admit(GOAL_ID, source_repo=source, base_revision=base, envelope=compiler.compile()["stages"][STAGE_ID])
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
        runs_f = RunStore(root / "runs-f")
        compiler_f = CampaignCompiler(_campaign())
        worker_f = GoalLoopWorker(
            goal_store=GoalStore(root / "goals-f"),
            run_store=runs_f,
            controller=LoopController(runs_f, root / "workspaces-f"),
            compilers={CAMPAIGN_ID: compiler_f},
            execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        )
        goals_f = worker_f.goal_store
        env_f = compiler_f.compile()["stages"][STAGE_ID]
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

#!/usr/bin/env python3
"""Committed W6b smoke: deterministic no-progress and campaign failure lines.

Proves, offline with fixture models, that two consecutive identical failure
signatures (same verifier exit, same diff digest) stop a run early with a
durable no_progress_stop event; that varying signatures retry to normal
exhaustion; that a verified attempt never triggers the line; that a campaign
routes to a human after N consecutive failed goals (declared
failure_stop_threshold, default 3, range 3-5, invalid values rejected); that
a completed goal resets the count; and that the count is derived from
durable goal states so a new worker instance on the same stores trips the
line at the right count. All deterministic — no model judgment anywhere.
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
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

MARKER_LAMP = ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"]
DEFAULT_LAMP = ["sh", "-c", "! git diff --cached --quiet"]


def _campaign(campaign_id: str, stage_ids: list[str], *, lamp: list[str], threshold: Any = None) -> dict:
    campaign = {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": campaign_id,
        "stages": [
            {
                "stage_id": sid,
                "goal": {"feature_contract": sid},
                "allowed_paths": ["src/"],
                "allowed_side_effects": ["workspace", "artifact"],
                "acceptance_lamp": {"id": f"{sid}-lamp", "smoke": "fixture lamp", "verification_argv": lamp},
                "max_attempts": 4,
                "next_stage_id": None,
            }
            for sid in stage_ids
        ],
    }
    if threshold is not None:
        campaign["failure_stop_threshold"] = threshold
    return campaign


def _worker(root: Path, tag: str, source: Path, base: str, campaign: dict) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    compiler = CampaignCompiler(campaign)
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={compiler.campaign_id: compiler},
        execution_context={compiler.campaign_id: {"source_repo": source, "base_revision": base}},
    )


def _seed(worker: GoalLoopWorker, campaign_id: str, stage_id: str, event_key: str) -> None:
    envelope = worker.compilers[campaign_id].compile()["stages"][stage_id]
    worker.goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": f"{campaign_id}:{stage_id}", "campaign_id": campaign_id, "stage_id": stage_id,
                      "goal": {"feature_contract": stage_id, "admission_envelope": envelope}}
    })


def _seed_candidate_only(worker: GoalLoopWorker, campaign_id: str, stage_id: str, event_key: str) -> None:
    envelope = worker.compilers[campaign_id].compile()["stages"][stage_id]
    worker.goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": f"{campaign_id}:{stage_id}", "campaign_id": campaign_id, "stage_id": stage_id,
                      "goal": {"feature_contract": stage_id, "admission_envelope": envelope}}
    })
    worker.goal_store.create_candidate(event_key, goal_id=f"{campaign_id}:{stage_id}", campaign_id=campaign_id,
                                       stage_id=stage_id, goal={"feature_contract": stage_id, "admission_envelope": envelope})
    # Retire the seed event so later ticks do not re-admit this goal: the
    # scenario needs it to sit in candidate state until the line fires.
    worker.goal_store.transition_event(event_key, "completed")


def _empty_model(_workspace: Path, _capsule: dict) -> dict:
    """Fails the default lamp with an identical empty diff every attempt."""
    return {"summary": "w6b identical failure fixture"}


def _wrong_model(workspace: Path, capsule: dict) -> dict:
    """Fails the marker lamp with a varying diff every attempt."""
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    (src / "out.txt").write_text(f"wrong {capsule['attempt']}\n", encoding="utf-8")
    return {"summary": "w6b varying failure fixture"}


def _fixed_model(workspace: Path, _capsule: dict) -> dict:
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    (src / "out.txt").write_text("fixed\n", encoding="utf-8")
    return {"summary": "w6b success fixture"}


def _ticks(worker: GoalLoopWorker, holder: str, model, count: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for _ in range(count):
        result = worker.tick(holder=holder, model=model)
    return result


def _only_run(worker: GoalLoopWorker) -> dict[str, Any]:
    runs = worker.run_store.runnable_runs() or worker.run_store.terminal_runs()
    return worker.run_store.get_run(runs[0]["run_id"])


def _fail_goal(worker: GoalLoopWorker, campaign_id: str, stage_id: str, event_key: str) -> dict[str, Any]:
    """Admit one goal and drive its run to a stopped failure (2 ticks: the
    identical empty-diff failures trip the no-progress line at attempt 2)."""
    _seed(worker, campaign_id, stage_id, event_key)
    return _ticks(worker, f"w6b-{event_key}", _empty_model, 2)


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # 1) Identical signatures: the run stops early, below max_attempts.
        w1 = _worker(root, "same", source, base, _campaign("c1", ["s1"], lamp=DEFAULT_LAMP))
        _seed(w1, "c1", "s1", "same-1")
        _ticks(w1, "w6b-same", _empty_model, 2)
        run1 = _only_run(w1)
        events1 = [event for event in w1.run_store.events(run1["run_id"]) if event["event_type"] == "no_progress_stop"]

        # 2) Varying signatures: retries burn to normal exhaustion.
        w2 = _worker(root, "vary", source, base, _campaign("c2", ["s1"], lamp=MARKER_LAMP))
        _seed(w2, "c2", "s1", "vary-1")
        _ticks(w2, "w6b-vary", _wrong_model, 4)
        run2 = _only_run(w2)
        events2 = [event for event in w2.run_store.events(run2["run_id"]) if event["event_type"] == "no_progress_stop"]

        # 3) A verified attempt never trips the line.
        w3 = _worker(root, "pass", source, base, _campaign("c3", ["s1"], lamp=MARKER_LAMP))
        _seed(w3, "c3", "s1", "pass-1")
        w3.tick(holder="w6b-pass", model=_wrong_model)
        w3.tick(holder="w6b-pass", model=_fixed_model)
        run3 = _only_run(w3)
        events3 = [event for event in w3.run_store.events(run3["run_id"]) if event["event_type"] == "no_progress_stop"]

        # 4) Default threshold 3: three consecutive failed goals stop the campaign.
        w4 = _worker(root, "t3", source, base, _campaign("c4", ["s1", "s2", "s3", "s4"], lamp=DEFAULT_LAMP))
        _fail_goal(w4, "c4", "s1", "t3-1")
        _fail_goal(w4, "c4", "s2", "t3-2")
        _seed_candidate_only(w4, "c4", "s4", "t3-4")
        tick4 = _fail_goal(w4, "c4", "s3", "t3-3")
        goal4_s4 = w4.goal_store.get_goal("c4:s4")["state"]
        try:
            event4 = w4.goal_store.get_event("campaign-failure-line:c4")
        except KeyError:
            event4 = None

        # 5) Declared threshold 5: four consecutive failures do not stop the campaign.
        w5 = _worker(root, "t5", source, base, _campaign("c5", ["s1", "s2", "s3", "s4", "s5"], lamp=DEFAULT_LAMP, threshold=5))
        for index in range(1, 5):
            _fail_goal(w5, "c5", f"s{index}", f"t5-{index}")
        _seed_candidate_only(w5, "c5", "s5", "t5-5")
        tick5 = w5.tick(holder="w6b-t5-idle", model=_empty_model)
        goal5_s5 = w5.goal_store.get_goal("c5:s5")["state"]

        # 6) A completed goal resets the count: fail, fail, complete, fail, fail.
        w6 = _worker(root, "reset", source, base, _campaign("c6", ["s1", "s2", "s3", "s4", "s5"], lamp=DEFAULT_LAMP))
        _fail_goal(w6, "c6", "s1", "rst-1")
        _fail_goal(w6, "c6", "s2", "rst-2")
        _seed(w6, "c6", "s3", "rst-3")
        w6.tick(holder="w6b-rst", model=_fixed_model)
        goal6_s3 = w6.goal_store.get_goal("c6:s3")["state"]
        _fail_goal(w6, "c6", "s4", "rst-4")
        tick6 = _fail_goal(w6, "c6", "s5", "rst-5")

        # 7) Restart: a new worker on the same stores trips the line at the right count.
        w7a = _worker(root, "boot-a", source, base, _campaign("c7", ["s1", "s2", "s3"], lamp=DEFAULT_LAMP))
        _fail_goal(w7a, "c7", "s1", "boot-1")
        _fail_goal(w7a, "c7", "s2", "boot-2")
        w7b = GoalLoopWorker(
            goal_store=GoalStore(root / "boot-a-goals"),
            run_store=RunStore(root / "boot-a-runs"),
            controller=LoopController(RunStore(root / "boot-a-runs"), root / "boot-a-workspaces-2"),
            compilers=w7a.compilers,
            execution_context=w7a.execution_context,
        )
        tick7 = _fail_goal(w7b, "c7", "s3", "boot-3")

        # 8) Invalid thresholds are rejected at compile time.
        invalid = 0
        for bad in (2, 6, "3", 3.5):
            try:
                CampaignCompiler(_campaign("cbad", ["s1"], lamp=DEFAULT_LAMP, threshold=bad))
            except ValueError:
                invalid += 1
        valid = all(CampaignCompiler(_campaign("cok", ["s1"], lamp=DEFAULT_LAMP, threshold=good)).failure_stop_threshold == good for good in (3, 4, 5))
        default_threshold = CampaignCompiler(_campaign("cdef", ["s1"], lamp=DEFAULT_LAMP)).failure_stop_threshold == 3

        cases = [
            {"id": "identical-signatures-stop-run-early",
             "ok": run1["state"] == "stopped" and run1["attempts"] == 2 and run1["max_attempts"] == 4
             and len(events1) == 1 and events1[0]["payload"]["signature"]["exit_code"] == 1,
             "detail": json.dumps({"state": run1["state"], "attempts": run1["attempts"],
                                   "event": events1[0]["payload"] if events1 else None})},
            {"id": "varying-signatures-reach-normal-exhaustion",
             "ok": run2["state"] == "stopped" and run2["attempts"] == 4 and events2 == [],
             "detail": json.dumps({"state": run2["state"], "attempts": run2["attempts"], "events": len(events2)})},
            {"id": "verified-attempt-never-trips-the-line",
             "ok": run3["state"] == "verified" and events3 == [],
             "detail": json.dumps({"state": run3["state"], "attempts": run3["attempts"]})},
            {"id": "threshold-3-routes-campaign-to-human",
             "ok": goal4_s4 == "human_required" and event4 is not None
             and event4["state"] == "human_required"
             and event4["payload"]["consecutive_failures"] == 3
             and event4["payload"]["routed_goal_ids"] == ["c4:s4"]
             and tick4.get("campaign_stops", []) != [],
             "detail": json.dumps({"s4": goal4_s4, "event": event4["payload"] if event4 else None,
                                   "stops": tick4.get("campaign_stops")})},
            {"id": "threshold-5-tolerates-four-failures",
             "ok": goal5_s5 == "candidate" and tick5.get("campaign_stops") == [],
             "detail": json.dumps({"s5": goal5_s5, "stops": tick5.get("campaign_stops")})},
            {"id": "completed-goal-resets-the-counter",
             "ok": goal6_s3 == "completed" and tick6.get("campaign_stops") == [],
             "detail": json.dumps({"s3": goal6_s3, "stops": tick6.get("campaign_stops")})},
            {"id": "counter-survives-worker-restart",
             "ok": tick7.get("campaign_stops", []) != []
             and tick7["campaign_stops"][0]["consecutive_failures"] == 3,
             "detail": json.dumps({"stops": tick7.get("campaign_stops")})},
            {"id": "invalid-threshold-rejected-at-compile",
             "ok": invalid == 4 and valid and default_threshold,
             "detail": json.dumps({"invalid_rejected": invalid, "valid": valid, "default": default_threshold})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-stop-lines",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/stop_lines_canary.py",
                         "fixtures": "injected models only; fully deterministic, no network"},
        "known_gaps_open": [
            "campaign failure line fires once per campaign (idempotent event key); re-arming after human review is a later slice",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

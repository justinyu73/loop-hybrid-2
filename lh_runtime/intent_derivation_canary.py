#!/usr/bin/env python3
"""Committed MVP-W1 smoke: manual_intent commands derive candidates and walk the loop."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_campaign, make_source_repo
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN = "intent-campaign"


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "w1 intent fixture"}


def make_worker(root: Path, name: str, source: Path, base: str, campaign: dict) -> tuple[GoalLoopWorker, GoalStore, RunStore]:
    goals = GoalStore(root / f"{name}-goals")
    runs = RunStore(root / f"{name}-runs")
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=LoopController(runs, root / f"{name}-ws"),
        compilers={CAMPAIGN: CampaignCompiler(campaign)},
        execution_context={CAMPAIGN: {"source_repo": source, "base_revision": base}},
    )
    return worker, goals, runs


def seed_intent(goals: GoalStore, *, stage_id: str, key: str, campaign_id: str = CAMPAIGN) -> None:
    goals.record_event(
        event_id=key,
        idempotency_key=key,
        source="example-commander",
        event_type="manual_intent",
        payload={"campaign_id": campaign_id, "stage_id": stage_id, "intent": "do the stage", "correlation_id": key},
    )


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        campaign = make_campaign(CAMPAIGN, stage_id="stage-work")

        # Full chain: intent -> derived candidate -> admit -> dispatch -> completed.
        worker, goals, runs = make_worker(root, "chain", source, base, campaign)
        seed_intent(goals, stage_id="stage-work", key="cmd-w1-1")
        first = worker.tick(holder="w1", model=model)
        derived_key = first.get("event", {}).get("derived_event_key") if isinstance(first.get("event"), dict) else None
        second = worker.tick(holder="w1", model=model)
        goal = goals.get_goal("intent-campaign:stage-work")
        cases.append(case(
            "intent-derives-candidate-event",
            first.get("event", {}).get("status") == "derived_candidate_event"
            and goals.get_event("cmd-w1-1")["state"] == "completed"
            and isinstance(derived_key, str),
            json.dumps({"first_event": first.get("event")}, ensure_ascii=False),
        ))
        cases.append(case(
            "derived-candidate-admits-dispatches-completes",
            goal["state"] == "completed" and runs.summary()["runs_by_state"].get("verified") == 1,
            json.dumps({"goal": goal["state"], "runs": runs.summary()["runs_by_state"]}),
        ))

        # Replay: the same command key never double-writes.
        replay_worker, replay_goals, replay_runs = make_worker(root, "replay", source, base, campaign)
        seed_intent(replay_goals, stage_id="stage-work", key="cmd-w1-2")
        seed_intent(replay_goals, stage_id="stage-work", key="cmd-w1-2")
        replay_worker.tick(holder="w1", model=model)
        summary = replay_goals.summary()
        cases.append(case(
            "replay-never-double-writes",
            summary["event_count"] == 2,  # original + one derived, not two originals
            json.dumps(summary),
        ))

        # Unknown stage / unknown campaign route to human_required, nothing derived.
        bad_worker, bad_goals, _ = make_worker(root, "bad", source, base, campaign)
        seed_intent(bad_goals, stage_id="no-such-stage", key="cmd-w1-bad-stage")
        bad_stage = bad_worker.tick(holder="w1", model=model)
        seed_intent(bad_goals, stage_id="stage-work", key="cmd-w1-bad-campaign", campaign_id="ghost-campaign")
        bad_campaign = bad_worker.tick(holder="w1", model=model)
        cases.append(case(
            "unknown-stage-and-campaign-are-human-required",
            bad_stage.get("event", {}).get("status") == "human_required"
            and bad_campaign.get("event", {}).get("status") == "human_required"
            and bad_goals.get_event("cmd-w1-bad-stage")["state"] == "human_required"
            and bad_goals.get_event("cmd-w1-bad-campaign")["state"] == "human_required",
            json.dumps({"stage": bad_stage.get("event"), "campaign": bad_campaign.get("event")}, ensure_ascii=False),
        ))

        # Non-auto-admissible stage (forbidden side effect) stays human_required.
        wild = make_campaign(CAMPAIGN, stage_id="stage-wild")
        wild["stages"][0]["allowed_side_effects"] = ["workspace", "push"]
        wild_worker, wild_goals, wild_runs = make_worker(root, "wild", source, base, wild)
        seed_intent(wild_goals, stage_id="stage-wild", key="cmd-w1-wild")
        wild_result = wild_worker.tick(holder="w1", model=model)
        cases.append(case(
            "non-auto-admissible-stage-stays-human-required",
            wild_result.get("event", {}).get("status") == "human_required"
            and wild_runs.summary()["event_count"] == 0,
            json.dumps({"event": wild_result.get("event")}, ensure_ascii=False),
        ))

        # Re-issued command for an existing goal: a goal that went stopped
        # (e.g. retry budget exhausted) and was reset to candidate by a human
        # must be re-admitted by a new command — the worker must not crash on
        # the ownership conflict, it proceeds with the existing candidate.
        retry_worker, retry_goals, retry_runs = make_worker(root, "retry", source, base, campaign)
        seed_intent(retry_goals, stage_id="stage-work", key="cmd-w1-r1")
        retry_worker.tick(holder="w1", model=model)  # derives + admits + dispatches? one event per tick
        retry_worker.tick(holder="w1", model=model)
        # goal is now completed; build a second pre-stopped goal directly
        from _fixture import make_goal
        make_goal(retry_goals, "intent-campaign:stage-retry", campaign_id=CAMPAIGN, stage_id="stage-work")
        # consume the fixture seed event so it does not pollute pending_events
        retry_goals.transition_event("fixture-event:intent-campaign:stage-retry", "completed")
        retry_goals.transition_goal("intent-campaign:stage-retry", "stopped", expected_state="candidate")
        retry_goals.transition_goal("intent-campaign:stage-retry", "candidate", expected_state="stopped")
        seed_intent(retry_goals, stage_id="stage-work", key="cmd-w1-r2")
        retry_first = retry_worker.tick(holder="w1", model=model)
        retry_second = retry_worker.tick(holder="w1", model=model)
        cases.append(case(
            "reissued-command-after-human-reset-does-not-crash",
            retry_first.get("event", {}).get("status") == "derived_candidate_event"
            and retry_second.get("status") == "progress",
            json.dumps({"first": retry_first.get("event"), "second_status": retry_second.get("status")}, ensure_ascii=False),
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-intent-derivation-w1",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/intent_derivation_canary.py",
            "note": "production consumer: the resident driver tick picks up command-down events (see docs/active/lh-big-node-approval-package.md)",
        },
        "known_gaps_open": [
            "Derivation covers manual_intent with campaign_id+stage_id; richer intent phrasing still routes human_required by design.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Committed MVP-W1 smoke: manual_intent commands derive candidates and walk the loop."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import make_campaign, make_source_repo
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from native_delivery_fixture import make_native_bundle
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner

CAMPAIGN = "intent-campaign"


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "w1 intent fixture"}


def bind_campaign(campaign: dict, source: Path, base: str) -> dict:
    """Attach an explicit producer-owned binding to each authorized stage.

    Manual intent only names the campaign/stage.  The real derivation path
    copies the persisted stage definition; this fixture therefore supplies
    the contract from explicit scope/check/verifier inputs instead of letting
    the worker synthesize one from an intent string.
    """
    compiled = CampaignCompiler(campaign).compile()["stages"]
    for stage in campaign["stages"]:
        stage_id = stage["stage_id"]
        binding = make_native_bundle(
            source,
            base,
            f"{campaign['campaign_id']}:{stage_id}",
            stage_id,
            [{
                "id": "intent-source-check",
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
            int(stage["max_attempts"]),
            goal={"feature_contract": stage["goal"], "admission_envelope": compiled[stage_id]},
        )
        stage["goal"] = {
            **stage["goal"],
            "delivery_required": True,
            "delivery_contract": binding["contract"],
            "delivery_plan": binding["plan"],
            "delivery_packet": binding["packet"],
        }
    return campaign


def make_worker(root: Path, name: str, source: Path, base: str, campaign: dict) -> tuple[GoalLoopWorker, GoalStore, RunStore]:
    goals = GoalStore(root / f"{name}-goals")
    runs = RunStore(root / f"{name}-runs", command_runner=fixture_command_runner)
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
        campaign = bind_campaign(make_campaign(CAMPAIGN, stage_id="stage-work"), source, base)

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

        # The App Gateway bounds an execution to one driver cycle.  The
        # command and its derived candidate must therefore reach one
        # Goal/Run/Attempt in that same cycle.
        bounded_worker, bounded_goals, bounded_runs = make_worker(root, "bounded", source, base, campaign)
        seed_intent(bounded_goals, stage_id="stage-work", key="cmd-w1-bounded")
        bounded = run_driver(
            bounded_worker,
            holder="w1-bounded",
            model=model,
            max_cycles=1,
            sleep_fn=lambda _seconds: None,
        )
        bounded_goal = bounded_goals.get_goal("intent-campaign:stage-work")
        cases.append(case(
            "one-cycle-intent-admits-and-dispatches",
            bounded["cycles"] == 1
            and bounded["runs_dispatched"] == 1
            and bounded_goal["state"] == "completed"
            and bounded_runs.summary()["runs_by_state"].get("verified") == 1,
            json.dumps(
                {
                    "driver": bounded,
                    "goal": bounded_goal["state"],
                    "runs": bounded_runs.summary()["runs_by_state"],
                },
                ensure_ascii=False,
            ),
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
        wild = bind_campaign(wild, source, base)
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
        # Revision-bump 自跑復活：goal 的 run 耗盡（stopped）後，重發 intent
        # 不需人復位——worker 自動把 stopped goal 轉回 candidate，admission
        # bump revision（新 run_id），新 run 跑完，舊 run 留作歷史。
        retry_campaign = bind_campaign(make_campaign(CAMPAIGN, stage_id="stage-retry"), source, base)
        rworker, rgoals, rruns = make_worker(root, "retry2", source, base, retry_campaign)
        retry_goal_id = "intent-campaign:stage-retry"
        retry_event_key = f"fixture-event:{retry_goal_id}"
        rgoals.record_event(
            event_id=f"evt-{retry_goal_id}",
            idempotency_key=retry_event_key,
            source="manual",
            event_type="manual_intent",
            payload={"campaign_id": CAMPAIGN, "goal_id": retry_goal_id},
        )
        rgoals.create_candidate(
            retry_event_key,
            goal_id=retry_goal_id,
            campaign_id=CAMPAIGN,
            stage_id="stage-retry",
            goal=retry_campaign["stages"][0]["goal"],
        )
        rgoals.transition_event(retry_event_key, "completed")
        from admission_bridge import GoalAdmissionBridge
        envelope = CampaignCompiler(retry_campaign).compile()["stages"]["stage-retry"]
        first_admit = GoalAdmissionBridge(rgoals, rruns).admit(
            "intent-campaign:stage-retry", source_repo=source, base_revision=base, envelope=envelope
        )
        first_run_id = first_admit["run_id"]
        rruns.begin_attempt(first_run_id, "workspace://retry2/1")
        receipt = {
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": first_run_id,
            "attempt": 1,
            "verification": {"exit_code": 1},
            "usage": {"state": "unknown", "reason": "intent revision-bump fixture"},
        }
        receipt_ref = rruns.write_artifact(first_run_id, 1, "receipt.json", json.dumps(receipt, sort_keys=True))
        if not rruns.finish_attempt(first_run_id, 1, state="stopped", receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"]):
            raise AssertionError("stopped intent fixture attempt did not finish")
        rgoals.transition_goal("intent-campaign:stage-retry", "stopped", expected_state="active")
        seed_intent(rgoals, stage_id="stage-retry", key="cmd-w1-rev")
        rworker.tick(holder="w1", model=model)  # derive candidate
        rworker.tick(holder="w1", model=model)  # revive + bump + dispatch
        rworker.tick(holder="w1", model=model)  # reduce to completed
        revived = rgoals.get_goal("intent-campaign:stage-retry")
        cases.append(case(
            "stopped-goal-revives-via-revision-bump",
            revived["state"] == "completed"
            and revived["current_revision"]["revision"] == 2
            and revived["run_id"] != first_run_id
            and rruns.get_run(first_run_id)["state"] == "stopped",
            json.dumps({"state": revived["state"], "revision": revived["current_revision"]["revision"], "new_run": revived["run_id"][:24], "old_run": first_run_id[:24]}),
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

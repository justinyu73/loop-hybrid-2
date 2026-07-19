#!/usr/bin/env python3
"""Committed G1 smoke: durable Goal event, candidate, claim, and lookup."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore


EVENT = {
    "event_id": "stage-complete-evt-1",
    "idempotency_key": "stage-complete:campaign-fixture:stage-1:receipt-1",
    "source": "stage_completion",
    "event_type": "verified_stage",
    "payload": {"campaign_id": "campaign-fixture", "stage_id": "stage-1", "receipt": "receipt-1"},
}
CANDIDATE = {
    "goal_id": "campaign-fixture:stage-2",
    "campaign_id": "campaign-fixture",
    "stage_id": "stage-2",
    "goal": {"must_have": ["bounded stage two"], "must_not": ["publish externally"]},
}


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def record_event(store: GoalStore) -> dict[str, object]:
    return store.record_event(**EVENT)


def create_candidate(store: GoalStore) -> dict[str, object]:
    return store.create_candidate(EVENT["idempotency_key"], **CANDIDATE)


def crash_child(root: str, phase: str) -> int:
    store = GoalStore(root)
    record_event(store)
    if phase == "after_candidate":
        create_candidate(store)
    os._exit(17)


def run_crash(root: Path, phase: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(Path(__file__)), "--crash-child", str(root), phase], capture_output=True, text=True)


def main() -> int:
    if len(sys.argv) == 4 and sys.argv[1] == "--crash-child":
        crash_child(sys.argv[2], sys.argv[3])
        return 17

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        direct = GoalStore(root / "direct")
        first = record_event(direct)
        replay = record_event(direct)
        duplicate_summary = direct.summary()
        duplicate_candidate = create_candidate(direct)
        duplicate_candidate_replay = create_candidate(direct)
        direct.transition_goal(CANDIDATE["goal_id"], "active", expected_state="candidate", event_key=EVENT["idempotency_key"])
        active_lookup = GoalStore(root / "direct").active_goals(campaign_id="campaign-fixture", stage_id="stage-2")

        before_root = root / "crash-before"
        before_process = run_crash(before_root, "before_candidate")
        before_restart = GoalStore(before_root)
        before_event = before_restart.get_event(EVENT["idempotency_key"])
        before_candidate = create_candidate(before_restart)

        after_root = root / "crash-after"
        after_process = run_crash(after_root, "after_candidate")
        after_restart = GoalStore(after_root)
        after_event = after_restart.get_event(EVENT["idempotency_key"])
        after_candidate = create_candidate(after_restart)
        after_summary = after_restart.summary()

        cases = [
            case("duplicate-event-is-reused", first["status"] == "received" and replay["status"] == "reused" and duplicate_summary["event_count"] == 1, str(replay)),
            case("duplicate-event-creates-one-candidate-claim", duplicate_candidate["status"] == "created" and duplicate_candidate_replay["status"] == "reused" and direct.summary()["goal_count"] == 1 and direct.summary()["claim_count"] == 1, str(duplicate_candidate_replay)),
            case("crash-before-candidate-replays-event", before_process.returncode == 17 and before_event["state"] == "event_received" and before_candidate["status"] == "created", str(before_event)),
            case("crash-after-candidate-keeps-candidate", after_process.returncode == 17 and after_event["state"] == "candidate" and after_candidate["status"] == "reused" and after_summary["goal_count"] == 1, str(after_event)),
            case("active-goal-lookup-is-durable", len(active_lookup) == 1 and active_lookup[0]["goal_id"] == CANDIDATE["goal_id"] and active_lookup[0]["current_revision"]["revision"] == 1, str(active_lookup)),
            case("event-key-conflict-fails-closed", _conflict_is_rejected(direct), "conflicting payload rejected"),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-store-g1",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/goal_canary.py",
            "crash_exit_code": 17,
            "required_tables": ["goal_events", "goals", "goal_revisions", "goal_claims"],
        },
        "known_gaps_open": [
            "G1 does not match events, compile campaign policy, auto-admit candidates, or create RunStore runs; those remain G2-G4.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _conflict_is_rejected(store: GoalStore) -> bool:
    try:
        store.record_event(
            event_id="stage-complete-evt-conflict",
            idempotency_key=EVENT["idempotency_key"],
            source=EVENT["source"],
            event_type=EVENT["event_type"],
            payload={"different": True},
        )
    except ValueError:
        return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())

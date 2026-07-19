#!/usr/bin/env python3
"""Committed G3 smoke: deterministic bind, stale, conflict, and candidate reduction."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_matcher import GoalMatcher
from goal_store import GoalStore


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def add_active(store: GoalStore, goal_id: str, event_key: str, revision_id: str, fingerprints: list[str]) -> dict:
    event = store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_created", payload={"goal_id": goal_id})
    store.create_candidate(event_key, goal_id=goal_id, campaign_id="campaign-g3", stage_id=goal_id, revision_id=revision_id, goal={"failure_fingerprints": fingerprints})
    store.transition_goal(goal_id, "active", expected_state="candidate", event_key=event_key)
    return store.get_goal(goal_id)


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        store = GoalStore(Path(raw))
        active_a = add_active(store, "goal-a", "goal-created-a", "rev-a", ["fp-a"])
        active_b = add_active(store, "goal-b", "goal-created-b", "rev-b", ["fp-b"])
        matcher = GoalMatcher([active_a, active_b])
        found = matcher.reduce({"event_key": "continue-a", "payload": {"goal_id": "goal-a", "revision_id": "rev-a"}})
        stale = matcher.reduce({"event_key": "stale-a", "payload": {"goal_id": "goal-a", "revision_id": "rev-old"}})
        conflict = GoalMatcher([active_a, active_a]).reduce({"event_key": "conflict-a", "payload": {"goal_id": "goal-a"}})
        failure = matcher.reduce({"event_key": "failure-a", "payload": {"failure_fingerprint": "fp-a"}})
        unscoped = matcher.reduce({"event_key": "unscoped", "payload": {"message": "continue"}})
        candidate_spec = {"goal_id": "goal-c", "campaign_id": "campaign-g3", "stage_id": "stage-c", "goal": {"feature_contract": "bounded"}}
        candidate_event = {"event_key": "stage-completion-c", "payload": {"candidate": candidate_spec}}
        candidate_first = matcher.reduce(candidate_event)
        candidate_second = matcher.reduce(candidate_event)
        stored_event = store.record_event(event_id="stage-completion-c", idempotency_key="stage-completion-c", source="stage_completion", event_type="verified_stage", payload=candidate_event["payload"])
        stored_candidate_first = store.create_candidate(stored_event["event_key"], goal_id="goal-c", campaign_id="campaign-g3", stage_id="stage-c", goal=candidate_spec["goal"])
        stored_candidate_second = store.create_candidate(stored_event["event_key"], goal_id="goal-c", campaign_id="campaign-g3", stage_id="stage-c", goal=candidate_spec["goal"])
        widened = matcher.reduce({"event_key": "widened", "payload": {"scope_widening": True}})
        cases = [
            case("existing-goal-binds", found["route"] == "bind" and found["goal_id"] == "goal-a", str(found)),
            case("stale-revision-is-not-bound", stale["route"] == "stale" and stale["goal_id"] == "goal-a", str(stale)),
            case("ambiguous-match-is-conflict", conflict["route"] == "conflict", str(conflict)),
            case("failure-fingerprint-binds-deterministically", failure["route"] == "bind" and failure["goal_id"] == "goal-a", str(failure)),
            case("unscoped-event-requires-human", unscoped["route"] == "human_required", str(unscoped)),
            case("candidate-replay-has-one-key-and-one-claim", candidate_first["route"] == "candidate" and candidate_second["candidate_key"] == candidate_first["candidate_key"] and stored_candidate_first["status"] == "created" and stored_candidate_second["status"] == "reused" and store.summary()["goal_count"] == 3, str(store.summary())),
            case("scope-widening-stops", widened["route"] == "human_required", str(widened)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-matcher-g3",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/matcher_canary.py", "routes": ["bind", "candidate", "stale", "conflict", "human_required"]},
        "known_gaps_open": ["G3 is deterministic only; G4 owns admission and Goal-to-Run creation."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

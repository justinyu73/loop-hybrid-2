#!/usr/bin/env python3
"""Committed G4 smoke: one bounded admission, replay, and human stop."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from admission_bridge import GoalAdmissionBridge
from goal_store import GoalStore
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def envelope(*, side_effects: list[str] | None = None, human_only: bool = False) -> dict:
    return {
        "schema": "lh-campaign-admission-envelope/v1",
        "campaign_id": "campaign-g4",
        "stage_id": "stage-2",
        "goal": {"feature_contract": "bounded stage"},
        "allowed_paths": ["src/"],
        "allowed_side_effects": side_effects if side_effects is not None else ["workspace", "artifact"],
        "acceptance_lamp": {"id": "stage-2-smoke", "smoke": "python3 -B tests/stage-2-smoke.py", "verification_argv": ["python3", "-B", "tests/stage-2-smoke.py"]},
        "human_only": human_only,
        "max_attempts": 4,
        "next_stage_id": None,
        "auto_admission": {"eligible": not human_only and not (side_effects and "push" in side_effects), "reasons": [] if not human_only and not (side_effects and "push" in side_effects) else ["forbidden_side_effect:push"]},
    }


def add_candidate(store: GoalStore, goal_id: str, event_key: str, policy: dict) -> None:
    store.record_event(event_id=event_key, idempotency_key=event_key, source="stage_completion", event_type="verified_stage", payload={"candidate": goal_id})
    store.create_candidate(event_key, goal_id=goal_id, campaign_id=policy["campaign_id"], stage_id=policy["stage_id"], goal={"feature_contract": "bounded stage", "admission_envelope": policy})


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source = root / "source"
        source.mkdir()
        git("init", "-q", str(source))
        git("-C", str(source), "config", "user.email", "g4@example.invalid")
        git("-C", str(source), "config", "user.name", "G4 Canary")
        (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
        git("-C", str(source), "add", "baseline.txt")
        git("-C", str(source), "commit", "-qm", "baseline")
        base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        goals = GoalStore(root / "goals")
        runs = RunStore(root / "runs")
        bridge = GoalAdmissionBridge(goals, runs)
        good_policy = envelope()
        add_candidate(goals, "campaign-g4:stage-2", "g4-event-1", good_policy)
        first = bridge.admit("campaign-g4:stage-2", source_repo=source, base_revision=base, envelope=good_policy)
        replay = bridge.admit("campaign-g4:stage-2", source_repo=source, base_revision=base, envelope=good_policy)
        linked_goal = goals.get_goal("campaign-g4:stage-2")
        events = runs.events(first["run_id"])

        blocked_goals = GoalStore(root / "blocked-goals")
        blocked_runs = RunStore(root / "blocked-runs")
        blocked_bridge = GoalAdmissionBridge(blocked_goals, blocked_runs)
        blocked_policy = envelope(side_effects=["workspace", "push"])
        add_candidate(blocked_goals, "campaign-g4:blocked", "g4-event-blocked", blocked_policy)
        blocked = blocked_bridge.admit("campaign-g4:blocked", source_repo=source, base_revision=base, envelope=blocked_policy)
        human_goals = GoalStore(root / "human-goals")
        human_runs = RunStore(root / "human-runs")
        human_bridge = GoalAdmissionBridge(human_goals, human_runs)
        human_policy = envelope(human_only=True)
        add_candidate(human_goals, "campaign-g4:human", "g4-event-human", human_policy)
        human = human_bridge.admit("campaign-g4:human", source_repo=source, base_revision=base, envelope=human_policy)

        # U4: a moving ref name is pinned to its SHA at admission; the original
        # ref is kept as base_ref. Unresolvable refs route to human_required.
        git("-C", str(source), "branch", "feature-x")
        pinned_goals = GoalStore(root / "pinned-goals")
        pinned_runs = RunStore(root / "pinned-runs")
        pinned_bridge = GoalAdmissionBridge(pinned_goals, pinned_runs)
        add_candidate(pinned_goals, "campaign-g4:pinned", "g4-event-pinned", good_policy)
        pinned = pinned_bridge.admit("campaign-g4:pinned", source_repo=source, base_revision="feature-x", envelope=good_policy)
        pinned_run = pinned_runs.get_run(pinned["run_id"]) if pinned["run_id"] else None
        ghost = pinned_bridge.admit("campaign-g4:pinned", source_repo=source, base_revision="no-such-ref", envelope=good_policy)
        cases = [
            case("candidate-admits-one-queued-run", first["status"] == "active" and first["run_state"] == "queued" and linked_goal["state"] == "active" and linked_goal["run_id"] == first["run_id"], str(first)),
            case("admission-replay-reuses-run", replay["status"] == "reused" and replay["run_id"] == first["run_id"] and runs.summary()["runs_by_state"] == {"queued": 1} and len(events) == 1, str(replay)),
            case("goal-run-link-is-durable", runs.get_run(first["run_id"])["goal"]["goal_id"] == linked_goal["goal_id"] and runs.get_run(first["run_id"])["goal"]["revision_id"] == linked_goal["current_revision"]["revision_id"], str(runs.get_run(first["run_id"])["goal"])),
            case("scope-widening-does-not-create-run", blocked["status"] == "human_required" and blocked["run_id"] is None and blocked_goals.get_goal("campaign-g4:blocked")["state"] == "human_required" and blocked_runs.summary()["event_count"] == 0, str(blocked)),
            case("human-only-does-not-create-run", human["status"] == "human_required" and human["run_id"] is None and human_runs.summary()["event_count"] == 0, str(human)),
            case("invalid_source_is_human_required", _invalid_source_is_stopped(source, base, root), "unavailable source rejected to human_required"),
            case(
                "moving-ref-pinned-to-sha-at-admission",
                pinned["status"] == "active" and pinned_run is not None and pinned_run["base_revision"] == base and pinned_run["goal"].get("base_ref") == "feature-x",
                json.dumps({"base_revision": None if pinned_run is None else pinned_run["base_revision"], "base_ref": None if pinned_run is None else pinned_run["goal"].get("base_ref")}),
            ),
            case(
                "unresolvable-ref-is-human-required",
                ghost["status"] == "human_required" and ghost["run_id"] is None and "unresolvable_base_revision" in ghost.get("reasons", []),
                str(ghost),
            ),
        ]
        # Re-admission of a goal whose run is exhausted (stopped) must not
        # re-link the dead run — it routes to human_required for a revision
        # decision instead of silently re-stopping the goal.
        exhausted_goals = GoalStore(root / "exhausted-goals")
        exhausted_runs = RunStore(root / "exhausted-runs")
        exhausted_bridge = GoalAdmissionBridge(exhausted_goals, exhausted_runs)
        add_candidate(exhausted_goals, "campaign-g4:exhausted", "g4-event-exhausted", good_policy)
        first_admit = exhausted_bridge.admit("campaign-g4:exhausted", source_repo=source, base_revision=base, envelope=good_policy)
        first_run = exhausted_runs.get_run(first_admit["run_id"])
        exhausted_runs.begin_attempt(first_run["run_id"], "workspace://exhausted/1")
        exhausted_runs.finish_attempt(first_run["run_id"], 1, state="stopped", receipt_ref="artifacts/exhausted/1/receipt.json", receipt_digest="sha256:exhausted")
        exhausted_goals.transition_goal("campaign-g4:exhausted", "stopped", expected_state="active")
        exhausted_goals.transition_goal("campaign-g4:exhausted", "candidate", expected_state="stopped")
        zombie = exhausted_bridge.admit("campaign-g4:exhausted", source_repo=source, base_revision=base, envelope=good_policy)
        cases.append(case(
            "exhausted-run-readmission-routes-human-required",
            zombie["status"] == "human_required"
            and "run_exhausted_needs_new_revision" in zombie.get("reasons", [])
            and exhausted_goals.get_goal("campaign-g4:exhausted")["state"] == "candidate",
            json.dumps(zombie),
        ))
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-admission-bridge-g4",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/admission_canary.py", "run_state_required": "queued", "retry_semantics": "unchanged"},
        "known_gaps_open": ["G4 creates a local queued RunStore run; controller execution, providers, GitHub, and promotion remain outside this node."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _invalid_source_is_stopped(source: Path, base: str, root: Path) -> bool:
    goals = GoalStore(root / "invalid-goals")
    runs = RunStore(root / "invalid-runs")
    policy = envelope()
    add_candidate(goals, "campaign-g4:invalid", "g4-event-invalid", policy)
    result = GoalAdmissionBridge(goals, runs).admit("campaign-g4:invalid", source_repo=root / "missing", base_revision=base, envelope=policy)
    return result["status"] == "human_required" and goals.get_goal("campaign-g4:invalid")["state"] == "human_required" and runs.summary()["event_count"] == 0


if __name__ == "__main__":
    raise SystemExit(main())

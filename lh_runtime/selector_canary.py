#!/usr/bin/env python3
"""Committed H2 smoke: dependency/priority-aware runnable selection (GoalHierarchy v1 §3)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_campaign, make_goal as _make_goal, make_source_repo
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker, select_next_runnable
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN = "selector-fixture"


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def make_goal(
    store: GoalStore,
    goal_id: str,
    *,
    parent_goal_id: str | None = None,
    depends_on: list[str] | None = None,
    priority: int = 0,
) -> None:
    _make_goal(
        store,
        goal_id,
        campaign_id=CAMPAIGN,
        stage_id="stage-s",
        parent_goal_id=parent_goal_id,
        depends_on=depends_on,
        priority=priority,
    )


def queue_run(goals: GoalStore, runs: RunStore, goal_id: str, run_id: str) -> None:
    runs.create_run(goal={"goal_id": goal_id}, source_repo=Path("fixture-repo"), base_revision="fixture-rev", run_id=run_id)
    goals.activate_with_run(goal_id, run_id)


def lookup_of(goals: GoalStore):
    def lookup(goal_id: str) -> dict[str, object] | None:
        try:
            return goals.get_goal(goal_id)
        except KeyError:
            return None

    return lookup


def selected_run_id(goals: GoalStore, runs: RunStore) -> str | None:
    picked = select_next_runnable(runs.runnable_runs(), lookup_of(goals))
    return None if picked is None else picked["run_id"]


def campaign() -> dict:
    return make_campaign(CAMPAIGN, stage_id="stage-s")


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "h2 selector fixture"}


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # Priority beats earlier FIFO position.
        prio = GoalStore(root / "prio-goals")
        prio_runs = RunStore(root / "prio-runs")
        make_goal(prio, "low", priority=1)
        queue_run(prio, prio_runs, "low", "run-a-low")
        make_goal(prio, "high", priority=5)
        queue_run(prio, prio_runs, "high", "run-b-high")
        cases.append(case(
            "priority-beats-earlier-created-fifo",
            selected_run_id(prio, prio_runs) == "run-b-high",
            f"selected={selected_run_id(prio, prio_runs)}",
        ))

        # No priority / no depends_on: selection is byte-identical to old FIFO.
        flat = GoalStore(root / "flat-goals")
        flat_runs = RunStore(root / "flat-runs")
        make_goal(flat, "flat-1")
        queue_run(flat, flat_runs, "flat-1", "run-a-first")
        make_goal(flat, "flat-2")
        queue_run(flat, flat_runs, "flat-2", "run-b-second")
        fifo_first = flat_runs.runnable_runs()[0]["run_id"]
        cases.append(case(
            "no-priority-no-deps-equals-old-fifo",
            fifo_first == "run-a-first" and selected_run_id(flat, flat_runs) == fifo_first,
            f"fifo={fifo_first} selected={selected_run_id(flat, flat_runs)}",
        ))

        # Priority tie falls back to created_at/run_id (deterministic).
        tie = GoalStore(root / "tie-goals")
        tie_runs = RunStore(root / "tie-runs")
        make_goal(tie, "tie-1", priority=3)
        queue_run(tie, tie_runs, "tie-1", "run-a-tie")
        make_goal(tie, "tie-2", priority=3)
        queue_run(tie, tie_runs, "tie-2", "run-b-tie")
        cases.append(case(
            "priority-tie-falls-back-to-fifo",
            selected_run_id(tie, tie_runs) == "run-a-tie",
            f"selected={selected_run_id(tie, tie_runs)}",
        ))

        # Unsatisfied depends_on is never selected, even with higher priority
        # and earlier creation; only completed releases the dependency.
        deps = GoalStore(root / "deps-goals")
        deps_runs = RunStore(root / "deps-runs")
        make_goal(deps, "parent")
        deps.transition_goal("parent", "active", expected_state="candidate")
        make_goal(deps, "base", parent_goal_id="parent")
        make_goal(deps, "follower", parent_goal_id="parent", depends_on=["base"], priority=9)
        queue_run(deps, deps_runs, "follower", "run-a-follower")
        queue_run(deps, deps_runs, "base", "run-b-base")
        blocked_pick = selected_run_id(deps, deps_runs)
        cases.append(case(
            "unsatisfied-depends-on-never-selected",
            blocked_pick == "run-b-base",
            f"selected={blocked_pick}",
        ))
        deps.transition_goal("base", "completed", expected_state="active")
        released_pick = selected_run_id(deps, deps_runs)
        cases.append(case(
            "completed-dependency-releases-follower",
            released_pick == "run-a-follower",
            f"selected={released_pick}",
        ))

        # stopped does not release: H1 routes the dependent to human_required,
        # and the selector then has nothing to dispatch (serial idle, no drift).
        broke = GoalStore(root / "broke-goals")
        broke_runs = RunStore(root / "broke-runs")
        make_goal(broke, "parent")
        broke.transition_goal("parent", "active", expected_state="candidate")
        make_goal(broke, "base", parent_goal_id="parent")
        make_goal(broke, "follower", parent_goal_id="parent", depends_on=["base"], priority=99)
        queue_run(broke, broke_runs, "base", "run-a-base")
        queue_run(broke, broke_runs, "follower", "run-b-follower")
        broke.transition_goal("base", "stopped", expected_state="active")
        cases.append(case(
            "stopped-dependency-leaves-nothing-runnable",
            broke.get_goal("follower")["state"] == "human_required" and selected_run_id(broke, broke_runs) is None,
            f"follower={broke.get_goal('follower')['state']} selected={selected_run_id(broke, broke_runs)}",
        ))

        # End-to-end wiring: a real worker tick dispatches the higher-priority
        # run first, still one run per tick (serial single holder unchanged).
        source, base = make_source_repo(root)

        wire = GoalStore(root / "wire-goals")
        wire_runs = RunStore(root / "wire-runs")
        envelope = CampaignCompiler(campaign()).compile()["stages"]["stage-s"]
        bridge = GoalAdmissionBridge(wire, wire_runs)
        for goal_id, priority in (("wire-low", 1), ("wire-high", 5)):
            make_goal(wire, goal_id, priority=priority)
            bridge.admit(goal_id, source_repo=source, base_revision=base, envelope=envelope)
        worker = GoalLoopWorker(
            goal_store=wire,
            run_store=wire_runs,
            controller=LoopController(wire_runs, root / "wire-workspaces"),
            compilers={CAMPAIGN: CampaignCompiler(campaign())},
            execution_context={CAMPAIGN: {"source_repo": source, "base_revision": base}},
        )
        first_tick = worker.tick(holder="h2-worker", model=model)
        high_run_id = wire.get_goal("wire-high")["run_id"]
        first_run_id = first_tick.get("run", {}).get("run_id") if isinstance(first_tick.get("run"), dict) else None
        cases.append(case(
            "worker-dispatches-highest-priority-first",
            first_tick.get("status") == "progress" and first_run_id == high_run_id and first_tick.get("run", {}).get("status") == "verified",
            json.dumps({"tick_status": first_tick.get("status"), "first_run": first_run_id, "high_run": high_run_id}),
        ))
        second_tick = worker.tick(holder="h2-worker", model=model)
        low_run_id = wire.get_goal("wire-low")["run_id"]
        second_run_id = second_tick.get("run", {}).get("run_id") if isinstance(second_tick.get("run"), dict) else None
        cases.append(case(
            "worker-then-dispatches-remaining-fifo",
            second_tick.get("status") == "progress" and second_run_id == low_run_id and second_tick.get("run", {}).get("status") == "verified",
            json.dumps({"tick_status": second_tick.get("status"), "second_run": second_run_id, "low_run": low_run_id}),
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-hierarchy-h2",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/selector_canary.py",
            "contract": "goal-hierarchy v1 contract (project documentation)",
            "approval_package": "h2/h3 goal-intake approval package (project documentation)",
        },
        "known_gaps_open": [
            "H2 selection is deterministic only; the optional model turning-point node landed in H3 (turning_point_canary.py) and defaults to this deterministic path when disabled.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

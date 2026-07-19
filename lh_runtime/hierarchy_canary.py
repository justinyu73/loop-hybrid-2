#!/usr/bin/env python3
"""Committed H1 smoke: GoalHierarchy v1 rollup, dependency break, broken-spec routing."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_goal as _make_goal
from goal_store import GoalStore

CAMPAIGN = "hierarchy-fixture"


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def make_goal(
    store: GoalStore,
    goal_id: str,
    *,
    parent_goal_id: str | None = None,
    depends_on: list[str] | None = None,
    priority: int = 0,
) -> dict[str, object]:
    return _make_goal(
        store,
        goal_id,
        campaign_id=CAMPAIGN,
        stage_id="stage-h",
        parent_goal_id=parent_goal_id,
        depends_on=depends_on,
        priority=priority,
    )


def activate(store: GoalStore, goal_id: str) -> None:
    store.transition_goal(goal_id, "active", expected_state="candidate")


def build_parent_with_children(store: GoalStore, children: tuple[str, ...] = ("child-a", "child-b")) -> None:
    make_goal(store, "parent")
    activate(store, "parent")
    for child in children:
        make_goal(store, child, parent_goal_id="parent")
        activate(store, child)


def crash_child(root: str) -> int:
    store = GoalStore(root)
    build_parent_with_children(store)
    store.transition_goal("child-a", "completed", expected_state="active")
    os._exit(17)


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--crash-child":
        crash_child(sys.argv[2])
        return 17

    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # Rollup: parent completes only when every child is terminal.
        direct = GoalStore(root / "rollup")
        build_parent_with_children(direct)
        direct.transition_goal("child-a", "completed", expected_state="active")
        mid_parent = direct.get_goal("parent")
        direct.transition_goal("child-b", "stopped", expected_state="active")
        done_parent = direct.get_goal("parent")
        cases.append(case(
            "parent-completes-only-when-all-children-terminal",
            mid_parent["state"] == "active" and done_parent["state"] == "completed",
            f"mid={mid_parent['state']} done={done_parent['state']}",
        ))

        # human_required propagates to the parent immediately.
        blocked = GoalStore(root / "blocked")
        build_parent_with_children(blocked)
        blocked.transition_goal("child-a", "human_required", expected_state="active")
        blocked_parent = blocked.get_goal("parent")
        blocked.transition_goal("child-b", "completed", expected_state="active")
        still_blocked = blocked.get_goal("parent")
        cases.append(case(
            "one-child-human-required-blocks-parent-auto-complete",
            blocked_parent["state"] == "human_required" and still_blocked["state"] == "human_required",
            f"on-hr={blocked_parent['state']} after-other-terminal={still_blocked['state']}",
        ))

        # Reset clears the block; parent re-enters via active, then completes.
        blocked.transition_goal("child-a", "active", expected_state="human_required")
        recovered = blocked.get_goal("parent")
        blocked.transition_goal("child-a", "completed", expected_state="active")
        finished = blocked.get_goal("parent")
        cases.append(case(
            "parent-recovers-via-active-after-child-reset",
            recovered["state"] == "active" and finished["state"] == "completed",
            f"recovered={recovered['state']} finished={finished['state']}",
        ))

        # Dependency break: only completed releases depends_on; stopped routes human_required.
        deps = GoalStore(root / "deps")
        make_goal(deps, "parent")
        activate(deps, "parent")
        make_goal(deps, "base", parent_goal_id="parent")
        activate(deps, "base")
        make_goal(deps, "follower", parent_goal_id="parent", depends_on=["base"], priority=5)
        follower_row = deps.get_goal("follower")
        deps.transition_goal("base", "stopped", expected_state="active")
        broken = deps.get_goal("follower")
        broken_parent = deps.get_goal("parent")
        cases.append(case(
            "stopped-dependency-routes-dependents-human-required",
            broken["state"] == "human_required" and broken_parent["state"] == "human_required",
            f"follower={broken['state']} parent={broken_parent['state']}",
        ))
        cases.append(case(
            "depends-on-and-priority-are-stored-verbatim",
            follower_row["depends_on"] == ["base"] and follower_row["priority"] == 5 and follower_row["parent_goal_id"] == "parent",
            json.dumps({"depends_on": follower_row["depends_on"], "priority": follower_row["priority"]}),
        ))

        # Completed dependency does not break dependents.
        released = GoalStore(root / "released")
        make_goal(released, "parent")
        activate(released, "parent")
        make_goal(released, "base", parent_goal_id="parent")
        activate(released, "base")
        make_goal(released, "follower", parent_goal_id="parent", depends_on=["base"])
        activate(released, "follower")
        released.transition_goal("base", "completed", expected_state="active")
        untouched = released.get_goal("follower")
        cases.append(case(
            "completed-dependency-leaves-dependents-alone",
            untouched["state"] == "active",
            f"follower={untouched['state']}",
        ))

        # Broken hierarchy specs: stored, then routed to human_required.
        specs = GoalStore(root / "specs")
        make_goal(specs, "parent")
        activate(specs, "parent")
        make_goal(specs, "outsider")  # top-level goal, not a sibling
        cross = make_goal(specs, "cross", parent_goal_id="parent", depends_on=["outsider"])
        missing = make_goal(specs, "missing", parent_goal_id="parent", depends_on=["ghost"])
        no_parent = make_goal(specs, "orphan", parent_goal_id="ghost-parent")
        self_dep = make_goal(specs, "selfish", parent_goal_id="parent", depends_on=["selfish"])
        cases.append(case(
            "cross-parent-dependency-fails-closed",
            cross["state"] == "human_required" and "not a sibling" in str(cross.get("hierarchy_violation")),
            str(cross),
        ))
        cases.append(case(
            "missing-dependency-target-fails-closed",
            missing["state"] == "human_required" and "does not exist" in str(missing.get("hierarchy_violation")),
            str(missing),
        ))
        cases.append(case(
            "missing-parent-fails-closed",
            no_parent["state"] == "human_required" and "parent goal does not exist" in str(no_parent.get("hierarchy_violation")),
            str(no_parent),
        ))
        cases.append(case(
            "self-dependency-fails-closed",
            self_dep["state"] == "human_required" and "depend on itself" in str(self_dep.get("hierarchy_violation")),
            str(self_dep),
        ))

        # Terminal parent is not extendable.
        sealed = GoalStore(root / "sealed")
        build_parent_with_children(sealed, children=("only-child",))
        sealed.transition_goal("only-child", "completed", expected_state="active")
        late = make_goal(sealed, "late-child", parent_goal_id="parent")
        cases.append(case(
            "child-under-terminal-parent-fails-closed",
            late["state"] == "human_required" and "not extendable" in str(late.get("hierarchy_violation")),
            str(late),
        ))

        # Crash after a child completes: reopen, replay is consistent, rollup resumes.
        crash_root = root / "crash"
        crash_process = subprocess.run(
            [sys.executable, str(Path(__file__)), "--crash-child", str(crash_root)], capture_output=True, text=True
        )
        reopened = GoalStore(crash_root)
        crash_parent = reopened.get_goal("parent")
        crash_child_a = reopened.get_goal("child-a")
        reopened.transition_goal("child-b", "completed", expected_state="active")
        crash_done = reopened.get_goal("parent")
        cases.append(case(
            "crash-after-child-complete-rollup-stays-consistent",
            crash_process.returncode == 17
            and crash_parent["state"] == "active"
            and crash_child_a["state"] == "completed"
            and crash_done["state"] == "completed",
            f"rc={crash_process.returncode} parent={crash_parent['state']} done={crash_done['state']}",
        ))

        # Flat goals (no hierarchy fields) keep the exact G1 behavior.
        flat = GoalStore(root / "flat")
        make_goal(flat, "flat-goal")
        flat.transition_goal("flat-goal", "active", expected_state="candidate")
        flat.transition_goal("flat-goal", "completed", expected_state="active")
        flat_row = flat.get_goal("flat-goal")
        cases.append(case(
            "flat-goal-lifecycle-unchanged",
            flat_row["state"] == "completed" and flat_row["parent_goal_id"] is None and flat_row["depends_on"] == [] and flat_row["priority"] == 0,
            json.dumps({"state": flat_row["state"], "parent_goal_id": flat_row["parent_goal_id"]}),
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-hierarchy-h1",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/hierarchy_canary.py",
            "crash_exit_code": 17,
            "contract": "goal-hierarchy v1 contract (project documentation)",
        },
        "known_gaps_open": [
            "H1 covers store + rollup only; runnable selection is H2 (selector_canary.py), the optional model turning-point node is H3 (turning_point_canary.py) — both landed, see those canaries.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

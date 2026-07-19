#!/usr/bin/env python3
"""Committed H3 smoke: bounded turning-point judgment node (GoalHierarchy v1 §5)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import turning_point as tp
from _fixture import make_campaign, make_goal as _make_goal, make_source_repo
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN = "turning-point-fixture"


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
        stage_id="stage-t",
        parent_goal_id=parent_goal_id,
        depends_on=depends_on,
        priority=priority,
    )


def campaign() -> dict:
    return make_campaign(CAMPAIGN, stage_id="stage-t")


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "h3 turning-point fixture"}


def make_source(root: Path) -> tuple[Path, str]:
    return make_source_repo(root)


def make_worker(root: Path, name: str, source: Path, base: str, children: tuple[tuple[str, int], ...]) -> tuple[GoalLoopWorker, GoalStore, RunStore]:
    """Parent + admitted children (priority per tuple), ready to dispatch."""
    goals = GoalStore(root / f"{name}-goals")
    runs = RunStore(root / f"{name}-runs")
    make_goal(goals, "parent")
    goals.transition_goal("parent", "active", expected_state="candidate")
    envelope = CampaignCompiler(campaign()).compile()["stages"]["stage-t"]
    bridge = GoalAdmissionBridge(goals, runs)
    for goal_id, priority in children:
        make_goal(goals, goal_id, parent_goal_id="parent", priority=priority)
        bridge.admit(goal_id, source_repo=source, base_revision=base, envelope=envelope)
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=LoopController(runs, root / f"{name}-workspaces"),
        compilers={CAMPAIGN: CampaignCompiler(campaign())},
        execution_context={CAMPAIGN: {"source_repo": source, "base_revision": base}},
    )
    return worker, goals, runs


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # --- validate_decision: the closed decision space -------------------
        ok = tp.validate_decision({"decision": "select:child-b"}, runnable_goal_ids=["child-a", "child-b"], rollup_satisfied=False)
        cases.append(case(
            "select-in-set-accepted",
            ok == {"type": "select", "goal_id": "child-b"},
            str(ok),
        ))
        rejected = [
            tp.validate_decision(raw, runnable_goal_ids=["child-a"], rollup_satisfied=False)["type"]
            for raw in (
                {"decision": "select:ghost"},
                {"decision": "select:"},
                {"decision": "admit:new-goal"},
                {"decision": "expand-scope"},
                {"decision": "skip-budget-gate"},
                {"decision": "priority:99"},
            )
        ]
        cases.append(case(
            "out-of-set-and-admission-scope-gate-attempts-rejected",
            all(kind == "reject" for kind in rejected),
            str(rejected),
        ))
        early = tp.validate_decision({"decision": "parent_done"}, runnable_goal_ids=[], rollup_satisfied=False)
        done = tp.validate_decision({"decision": "parent_done"}, runnable_goal_ids=[], rollup_satisfied=True)
        cases.append(case(
            "parent-done-requires-satisfied-rollup",
            early["type"] == "reject" and done == {"type": "parent_done"},
            json.dumps({"early": early, "done": done}),
        ))
        malformed = [
            tp.validate_decision(raw, runnable_goal_ids=["child-a"], rollup_satisfied=False)["type"]
            for raw in ("select:child-a", None, 42, {}, {"decision": 7})
        ]
        cases.append(case(
            "malformed-output-rejected",
            all(kind == "reject" for kind in malformed),
            str(malformed),
        ))
        hr = tp.validate_decision({"decision": "human_required"}, runnable_goal_ids=["child-a"], rollup_satisfied=False)
        cases.append(case(
            "human-required-passes-through",
            hr == {"type": "human_required"},
            str(hr),
        ))

        # --- worker wiring ---------------------------------------------------
        source, base = make_source(root)

        # The model may reorder within the legal set: it picks the lower-
        # priority child and that is the run dispatched.
        chooser, chooser_goals, _ = make_worker(root, "choose", source, base, (("child-high", 5), ("child-low", 1)))
        picked = chooser.tick(holder="h3", model=model, turning_point=lambda snap: {"decision": "select:child-low"})
        low_run = chooser_goals.get_goal("child-low")["run_id"]
        cases.append(case(
            "model-selects-within-set-and-wins",
            picked.get("run", {}).get("run_id") == low_run and picked.get("run", {}).get("status") == "verified",
            json.dumps({"run": picked.get("run"), "low_run": low_run}),
        ))

        # Out-of-set select: rejected, deterministic H2 pick is dispatched.
        fallback, fallback_goals, _ = make_worker(root, "fallback", source, base, (("child-high", 5), ("child-low", 1)))
        fell = fallback.tick(holder="h3", model=model, turning_point=lambda snap: {"decision": "select:ghost"})
        high_run = fallback_goals.get_goal("child-high")["run_id"]
        cases.append(case(
            "out-of-set-select-falls-back-to-h2",
            fell.get("run", {}).get("run_id") == high_run,
            json.dumps({"run": fell.get("run"), "high_run": high_run}),
        ))

        # A raising model is a reject: deterministic pick, loop keeps going.
        raiser, raiser_goals, _ = make_worker(root, "raiser", source, base, (("child-high", 5), ("child-low", 1)))

        def boom(snapshot: dict) -> dict:
            raise RuntimeError("model unavailable")

        raised = raiser.tick(holder="h3", model=model, turning_point=boom)
        cases.append(case(
            "model-failure-falls-back-to-h2",
            raised.get("run", {}).get("run_id") == raiser_goals.get_goal("child-high")["run_id"],
            json.dumps({"run": raised.get("run")}),
        ))

        # The model cannot waive the lamp: it selects a child whose run has no
        # verification plan, and the run still routes to human_required
        # instead of dispatching (hard gates ignore the model).
        gated = GoalStore(root / "gated-goals")
        gated_runs = RunStore(root / "gated-runs")
        make_goal(gated, "lamp-ok", priority=5)
        envelope = CampaignCompiler(campaign()).compile()["stages"]["stage-t"]
        GoalAdmissionBridge(gated, gated_runs).admit("lamp-ok", source_repo=source, base_revision=base, envelope=envelope)
        make_goal(gated, "lamp-less", priority=1)
        gated_runs.create_run(goal={"goal_id": "lamp-less"}, source_repo=source, base_revision=base, run_id="run-lamp-less")
        gated.activate_with_run("lamp-less", "run-lamp-less")
        gated_worker = GoalLoopWorker(
            goal_store=gated,
            run_store=gated_runs,
            controller=LoopController(gated_runs, root / "gated-workspaces"),
            compilers={CAMPAIGN: CampaignCompiler(campaign())},
            execution_context={CAMPAIGN: {"source_repo": source, "base_revision": base}},
        )
        gate_result = gated_worker.tick(holder="h3", model=model, turning_point=lambda snap: {"decision": "select:lamp-less"})
        cases.append(case(
            "model-cannot-waive-missing-lamp",
            gate_result.get("run", {}).get("status") == "human_required"
            and gated.get_goal("lamp-less")["state"] == "human_required"
            and gated.get_goal("lamp-ok")["state"] == "active",
            json.dumps({"run": gate_result.get("run"), "lamp_ok": gated.get_goal("lamp-ok")["state"]}),
        ))

        # human_required decision routes the shared parent, nothing dispatches.
        routed, routed_goals, routed_runs = make_worker(root, "routed", source, base, (("child-a", 3), ("child-b", 2)))
        route_result = routed.tick(holder="h3", model=model, turning_point=lambda snap: {"decision": "human_required"})
        cases.append(case(
            "human-required-decision-routes-parent-no-dispatch",
            route_result.get("run", {}).get("status") == "human_required"
            and routed_goals.get_goal("parent")["state"] == "human_required"
            and routed_runs.summary()["runs_by_state"].get("queued") == 2,
            json.dumps({"run": route_result.get("run"), "parent": routed_goals.get_goal("parent")["state"]}),
        ))

        # Node disabled: identical to H2 — deterministic order, and the whole
        # hierarchy still runs to completion (parent rollup included).
        plain, plain_goals, _ = make_worker(root, "plain", source, base, (("child-high", 5), ("child-low", 1)))
        first = plain.tick(holder="h3", model=model)
        second = plain.tick(holder="h3", model=model)
        cases.append(case(
            "disabled-node-runs-hierarchy-to-completion",
            first.get("run", {}).get("run_id") == plain_goals.get_goal("child-high")["run_id"]
            and plain_goals.get_goal("child-high")["state"] == "completed"
            and plain_goals.get_goal("child-low")["state"] == "completed"
            and plain_goals.get_goal("parent")["state"] == "completed",
            json.dumps({
                "first_run": first.get("run", {}).get("run_id"),
                "high": plain_goals.get_goal("child-high")["state"],
                "low": plain_goals.get_goal("child-low")["state"],
                "parent": plain_goals.get_goal("parent")["state"],
            }),
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-goal-hierarchy-h3",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/turning_point_canary.py",
            "contract": "goal-hierarchy v1 contract (project documentation)",
            "approval_package": "h2/h3 goal-intake approval package (project documentation)",
        },
        "known_gaps_open": [
            "H3 node is wired at the worker dispatch point only; feeding a real provider-backed model is a host configuration, not part of this canary.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

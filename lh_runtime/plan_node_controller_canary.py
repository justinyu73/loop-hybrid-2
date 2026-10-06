#!/usr/bin/env python3
"""Offline acceptance canary for the provider-neutral plan-node controller."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent

from plan_node_controller import PlanNodeController, PlanNodeControllerError  # noqa: E402
from work_unit_store import WorkUnitStore  # noqa: E402


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _plan(char: str = "1") -> dict[str, Any]:
    return {"sealed_plan": {"plan_digest": _digest(char)}, "planner_receipt": {"identity": "deterministic"}}


def _project(_: dict[str, Any], __: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "projected",
        "first_actionable": {"node_id": "first"},
        "runs_created": 0,
        "attempts_created": 0,
        "provider_invocations": 0,
        "queue_digest": _digest("2"),
    }


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def run_cases(root: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    successful_calls = {"planner": 0, "verifier": 0, "queue": 0}

    def planner(_: dict[str, Any]) -> dict[str, Any]:
        successful_calls["planner"] += 1
        return _plan()

    def verifier(_: dict[str, Any], __: dict[str, Any]) -> dict[str, Any]:
        successful_calls["verifier"] += 1
        return {"verdict": "GREEN", "identity": "independent", "read_only": True, "source_write": False}

    def project(_: dict[str, Any], __: dict[str, Any]) -> dict[str, Any]:
        successful_calls["queue"] += 1
        return _project({}, {})

    first = PlanNodeController(root / "success", goal_id="fixture-goal").dispatch(
        input_digest=_digest("3"),
        planner=planner,
        verifier=verifier,
        queue_projector=project,
        context={"route": "deterministic"},
    )
    cases.append(_case(
        "single-plan-dispatch-does-not-create-lh-execution",
        first["status"] == "completed"
        and first["runs_created"] == first["attempts_created"] == first["provider_invocations"] == 0
        and successful_calls == {"planner": 1, "verifier": 1, "queue": 1},
        {"result": first, "calls": successful_calls},
    ))

    replay = PlanNodeController(root / "success", goal_id="fixture-goal").dispatch(
        input_digest=_digest("3"),
        planner=planner,
        verifier=verifier,
        queue_projector=project,
    )
    cases.append(_case(
        "restart-replays-one-plan-receipt",
        replay.get("replayed") is True and successful_calls == {"planner": 1, "verifier": 1, "queue": 1},
        {"replay": replay, "calls": successful_calls},
    ))

    try:
        PlanNodeController(root / "success", goal_id="fixture-goal").dispatch(
            input_digest=_digest("4"), planner=planner, verifier=verifier, queue_projector=project,
        )
    except PlanNodeControllerError as exc:
        cases.append(_case("different-input-digest-fails-closed", exc.reason == "plan_input_digest_drift", exc.reason))
    else:
        cases.append(_case("different-input-digest-fails-closed", False, "unexpected success"))

    queue_calls = {"count": 0}

    def red_verifier(_: dict[str, Any], __: dict[str, Any]) -> dict[str, Any]:
        return {"verdict": "RED", "identity": "independent", "read_only": True, "source_write": False}

    def unexpected_queue(_: dict[str, Any], __: dict[str, Any]) -> dict[str, Any]:
        queue_calls["count"] += 1
        return _project({}, {})

    try:
        PlanNodeController(root / "red", goal_id="fixture-goal").dispatch(
            input_digest=_digest("5"), planner=lambda _: _plan("5"), verifier=red_verifier,
            queue_projector=unexpected_queue,
        )
    except PlanNodeControllerError as exc:
        cases.append(_case("red-verifier-routes-without-queue-or-run", exc.reason == "plan_verifier_red" and queue_calls["count"] == 0, exc.reason))
    else:
        cases.append(_case("red-verifier-routes-without-queue-or-run", False, "unexpected success"))

    try:
        PlanNodeController(root / "self-accept", goal_id="fixture-goal").dispatch(
            input_digest=_digest("6"),
            planner=lambda _: {"sealed_plan": {"plan_digest": _digest("6")}, "planner_receipt": {"identity": "same"}},
            verifier=lambda _, __: {"verdict": "GREEN", "identity": "same", "read_only": True, "source_write": False},
            queue_projector=_project,
        )
    except PlanNodeControllerError as exc:
        cases.append(_case("planner-cannot-self-accept", exc.reason == "verifier_identity_not_independent", exc.reason))
    else:
        cases.append(_case("planner-cannot-self-accept", False, "unexpected success"))

    store = WorkUnitStore(root / "legacy-store")
    store.create_parent_goal(
        "parent-plan",
        goal_id="LH-EXAMPLE-GOAL-001",
        goal_revision=4,
        base_sha="a" * 40,
    )
    store.create_work_unit(
        "work-p3b",
        parent_goal_id="parent-plan",
        node_id="plan",
        node_kind="planning",
        producer="PlanNodeController",
        worker_id="runtime-planner",
        base_sha="a" * 40,
    )
    legacy_controller = PlanNodeController(store, node_id="plan")
    legacy_first = legacy_controller.dispatch("parent-plan", "planner-holder", workspace_ref="task-worktree")
    legacy_replay = PlanNodeController(store, node_id="plan").dispatch("parent-plan", "planner-holder")
    legacy_busy = legacy_controller.dispatch("parent-plan", "different-holder")
    runs = store.list_runs("parent-plan")
    attempts = store.attempts_for_run(runs[0]["run_id"]) if runs else []
    cases.extend([
        _case("legacy-store-single-node-admitted", legacy_first["status"] == "dispatched" and legacy_first["parallel_minimum"] == 1, legacy_first),
        _case("legacy-store-restart-reuses-attempt", legacy_replay["status"] == "replayed" and legacy_replay["reused"] and legacy_replay["run_id"] == legacy_first["run_id"], legacy_replay),
        _case("legacy-store-busy-holder-contained", legacy_busy["status"] == "waiting" and legacy_busy["reason"] == "lease_busy", legacy_busy),
        _case("legacy-store-no-duplicate-run-or-attempt", len(runs) == 1 and len(attempts) == 1, {"runs": runs, "attempts": attempts}),
        _case("legacy-store-no-provider-invocation", legacy_first["provider_invocations"] == legacy_replay["provider_invocations"] == 0, {"first": legacy_first, "replay": legacy_replay}),
    ])

    return cases


def main() -> int:
    configured = os.environ.get("LH_HOST_TMP_ROOT")
    if configured:
        base = Path(configured).resolve()
        base.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="plan-node-controller-", dir=base)
    else:
        temporary = tempfile.TemporaryDirectory(prefix="plan-node-controller-")
    try:
        cases = run_cases(Path(temporary.name).resolve())
    finally:
        temporary.cleanup()
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "p3b0-plan-node-controller", "status": "pass" if not failures else "fail", "cases": cases, "failures": failures}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

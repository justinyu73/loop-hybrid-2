#!/usr/bin/env python3
"""Offline acceptance canary for the P2 parent-Goal scheduler."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from parallel_scheduler import ParallelScheduler  # noqa: E402
from work_unit_store import DAGCycleError, WorkUnitStore  # noqa: E402


BASE = "a" * 40


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def unit(
    root: Path,
    work_unit_id: str,
    *,
    base_sha: str = BASE,
    dependencies: tuple[str, ...] = (),
    write_path: str | None = None,
    read_path: str | None = None,
) -> dict[str, Any]:
    return {
        "work_unit_id": work_unit_id,
        "node_id": work_unit_id,
        "worker_id": f"worker-{work_unit_id}",
        "base_sha": base_sha,
        "dependencies": list(dependencies),
        "read_set": [read_path or f"docs/{work_unit_id}.md"],
        "write_set": [write_path or f"src/{work_unit_id}.py"],
        "worktree": str(root / f"worktree-{work_unit_id}"),
        "branch": f"agent/{work_unit_id}",
        "state_root": str(root / f"state-{work_unit_id}"),
    }


def cycle_case(root: Path) -> dict[str, Any]:
    store = WorkUnitStore(root / "cycle")
    store.create_parent_goal("parent-cycle", base_sha=BASE, goal_revision=2)
    rejected = False
    try:
        store.register_work_units("parent-cycle", [
            unit(root, "cycle-a", dependencies=("cycle-b",)),
            unit(root, "cycle-b", dependencies=("cycle-a",)),
        ])
    except DAGCycleError as exc:
        rejected = True
        detail: Any = {"error": str(exc), "cycle": list(exc.cycle)}
    else:
        detail = "cycle was accepted"
    return case("dag-cycle-rejected-atomically", rejected and store.list_work_units("parent-cycle") == [], detail)


def overlap_case(root: Path) -> dict[str, Any]:
    store = WorkUnitStore(root / "overlap")
    store.create_parent_goal("parent-overlap", base_sha=BASE, goal_revision=2)
    store.register_work_units("parent-overlap", [
        unit(root, "overlap-a", write_path="src/shared.py"),
        unit(root, "overlap-b", read_path="src/shared.py", write_path="src/other.py"),
    ])
    result = ParallelScheduler(store).dispatch("parent-overlap", holder="overlap")
    ok = result["status"] == "rejected" and result["reason"] == "read_write_overlap" and store.run_count("parent-overlap") == 0
    return case("read-write-overlap-rejected-before-run", ok, {"result": result, "run_count": store.run_count("parent-overlap")})


def parallel_case(root: Path) -> dict[str, Any]:
    store = WorkUnitStore(root / "parallel")
    store.create_parent_goal("parent-parallel", base_sha=BASE, goal_revision=2)
    store.register_work_units("parent-parallel", [
        unit(root, "parallel-a"),
        unit(root, "parallel-b"),
        unit(root, "parallel-c"),
    ])
    scheduler = ParallelScheduler(store)
    first = scheduler.dispatch("parent-parallel", holder="parallel")
    run_ids = [row["run_id"] for row in first["dispatched"]]
    states = [store.get_work_unit(row["work_unit_id"])["state"] for row in first["dispatched"]]
    replay = scheduler.dispatch("parent-parallel", holder="parallel")
    replay_ids = sorted(row["run_id"] for row in replay["workers"])
    ok = (
        first["status"] == "dispatched"
        and len(first["dispatched"]) == 3
        and len(set(run_ids)) == 3
        and all(state == "running" for state in states)
        and replay["status"] == "already_running"
        and replay_ids == sorted(run_ids)
        and store.run_count("parent-parallel") == 3
        and all(row["parent_goal_id"] == "parent-parallel" for row in first["workers"])
        and all(int(row["fence"]) > 0 for row in first["workers"])
    )
    return case("three-disjoint-workers-and-idempotent-replay", ok, {"first": first, "replay": replay, "run_count": store.run_count("parent-parallel")})


def dependency_and_fence_case(root: Path) -> dict[str, Any]:
    store = WorkUnitStore(root / "dependency")
    store.create_parent_goal("parent-dependency", base_sha=BASE, goal_revision=2)
    store.register_work_units("parent-dependency", [
        unit(root, "dep-a"),
        unit(root, "dep-b", dependencies=("dep-a",)),
        unit(root, "dep-c"),
        unit(root, "dep-d"),
    ])
    scheduler = ParallelScheduler(store)
    first = scheduler.dispatch("parent-dependency", holder="dependency")
    first_ids = {row["work_unit_id"] for row in first["dispatched"]}
    dep_a = next(row for row in first["dispatched"] if row["work_unit_id"] == "dep-a")
    stale = scheduler.integrate("dep-a", holder=dep_a["holder"], fence=int(dep_a["fence"]) - 1)
    integrated = scheduler.integrate("dep-a", holder=dep_a["holder"], fence=int(dep_a["fence"]))
    second = scheduler.dispatch("parent-dependency", holder="dependency")
    second_ids = {row["work_unit_id"] for row in second["dispatched"]}
    ok = (
        first["status"] == "dispatched"
        and "dep-b" not in first_ids
        and stale is False
        and integrated is True
        and "dep-b" in second_ids
        and store.get_work_unit("dep-a")["state"] == "integrated"
        and store.get_work_unit("dep-b")["state"] == "running"
    )
    return case("integrated-dependency-and-current-fence-required", ok, {"first": first, "stale_integrate": stale, "integrated": integrated, "second": second})


def restart_case(root: Path) -> dict[str, Any]:
    store = WorkUnitStore(root / "restart")
    store.create_parent_goal("parent-restart", base_sha=BASE, goal_revision=2)
    store.register_work_units("parent-restart", [unit(root, "restart-a"), unit(root, "restart-b")])
    first = ParallelScheduler(store, lease_seconds=0).dispatch("parent-restart", holder="restart")
    first_ids = sorted(row["run_id"] for row in first["dispatched"])
    restarted_store = WorkUnitStore(store.root)
    restarted = ParallelScheduler(restarted_store, lease_seconds=0).dispatch("parent-restart", holder="restart")
    second_ids = sorted(row["run_id"] for row in restarted["dispatched"])
    attempt_counts = [len(restarted_store.attempts_for_run(run_id)) for run_id in second_ids]
    ok = (
        first["status"] == "dispatched"
        and restarted["status"] == "dispatched"
        and second_ids == first_ids
        and restarted_store.run_count("parent-restart") == 2
        and attempt_counts == [2, 2]
        and all(row["reused"] is False for row in restarted["dispatched"])
        and len(restarted["reconciled"]) == 2
    )
    return case("restart-reuses-run-and-advances-attempt", ok, {"first": first, "restarted": restarted, "attempt_counts": attempt_counts})


def main() -> int:
    injected = os.environ.get("LH_HOST_TMP_ROOT")
    if injected:
        task_root = Path(injected).resolve()
        task_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="parallel-scheduler-", dir=task_root)
    else:
        temporary = tempfile.TemporaryDirectory(prefix="lh-w1-p2-scheduler-")
        task_root = Path(temporary.name).resolve()
    try:
        root = Path(temporary.name).resolve()
        cases = [
            cycle_case(root),
            overlap_case(root),
            parallel_case(root),
            dependency_and_fence_case(root),
            restart_case(root),
        ]
    finally:
        temporary.cleanup()
    failures = [item for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-parallel-scheduler-p2",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "state_root": str(task_root),
        "host_state_root_used": False,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

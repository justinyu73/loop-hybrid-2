"""Offline acceptance canary for the durable cumulative budget gate."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from goal_loop_driver import run_driver
from run_store import RunStore


class _IdleGoalStore:
    def goals_in_state(self, _state: str) -> list[Any]:
        return []


class _IdleWorker:
    def __init__(self, root: Path) -> None:
        self.run_store = RunStore(root / "runs")
        self.goal_store = _IdleGoalStore()
        self.tick_calls = 0

    def tick(self, **_kwargs: Any) -> dict[str, Any]:
        self.tick_calls += 1
        raise AssertionError("budget gate dispatched a run")


def _receipt(store: RunStore, run_id: str, usage: dict[str, Any]) -> None:
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1",
        "run_id": run_id,
        "attempt": ordinal,
        "usage": usage,
        "verification": {"argv": ["true"], "exit_code": 0},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _seed(root: Path, usage: dict[str, Any]) -> None:
    store = RunStore(root / "runs")
    run_id = "run-canary"
    store.create_run(
        run_id=run_id,
        goal={"goal_id": "goal-canary"},
        source_repo=HERE,
        base_revision="base",
    )
    _receipt(store, run_id, usage)


def _run(root: Path) -> dict[str, Any]:
    worker = _IdleWorker(root)
    result = run_driver(
        worker,
        holder="canary",
        model=lambda _workspace, _capsule: {},
        budget_ceiling_tokens=10,
        budget_scope="campaign/day-canary",
        max_cycles=3,
    )
    return {
        "stop_reason": result["stop_reason"],
        "cycles": result["cycles"],
        "runs_dispatched": result["runs_dispatched"],
        "tick_calls": worker.tick_calls,
        "total_tokens": result["budget"]["total_tokens"],
        "unknown_records": result["budget"]["unknown_records"],
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-durable-budget-") as temp_dir:
        root = Path(temp_dir)
        measured_root = root / "measured"
        _seed(measured_root, {"state": "measured", "model": "canary", "input_tokens": 7, "output_tokens": 3})
        exhausted = _run(measured_root)
        restarted = _run(measured_root)

        unknown_root = root / "unknown"
        _seed(unknown_root, {"state": "unknown", "model": "canary"})
        unknown = _run(unknown_root)

    cases = {
        "exhausted_on_startup": exhausted,
        "same_store_after_restart": restarted,
        "unknown_usage_stops": unknown,
    }
    failures: list[str] = []
    expected_exhausted = {
        "stop_reason": "budget_exhausted",
        "cycles": 0,
        "runs_dispatched": 0,
        "tick_calls": 0,
        "total_tokens": 10,
        "unknown_records": 0,
    }
    if exhausted != expected_exhausted:
        failures.append("exhausted_on_startup")
    if restarted != exhausted:
        failures.append("same_store_after_restart")
    if unknown != {
        "stop_reason": "budget_unknown",
        "cycles": 0,
        "runs_dispatched": 0,
        "tick_calls": 0,
        "total_tokens": 0,
        "unknown_records": 1,
    }:
        failures.append("unknown_usage_stops")

    output = {
        "check_id": "lh-durable-budget",
        "status": "PASS" if not failures else "FAIL",
        "blocking_failures": failures,
        "cases": cases,
    }
    print(json.dumps(output, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

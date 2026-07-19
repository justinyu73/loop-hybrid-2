"""Offline acceptance canary for the supervisor singleton and wake event."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from command_ingress import submit_command
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore


class _EmptyGoalStore:
    def goals_in_state(self, _state: str) -> list[dict[str, Any]]:
        return []


class _BlockingWorker:
    def __init__(self, root: Path) -> None:
        self.run_store = RunStore(root / "runs")
        self.goal_store = _EmptyGoalStore()

    def tick(self, **_kwargs: Any) -> dict[str, Any]:
        marker = self.run_store.root.parent / "holder-entered"
        marker.write_text("entered\n", encoding="utf-8")
        release = self.run_store.root.parent / "release-holder"
        while not release.exists():
            time.sleep(0.01)
        return {"status": "idle", "run": None, "terminal_after": None}


class _ProbeWorker:
    def __init__(self, root: Path) -> None:
        self.run_store = RunStore(root / "runs")
        self.goal_store = _EmptyGoalStore()

    def tick(self, **_kwargs: Any) -> dict[str, Any]:
        marker = self.run_store.root.parent / "contender-ticked"
        marker.write_text("unexpected\n", encoding="utf-8")
        return {"status": "idle", "run": None, "terminal_after": None}


def _model(_workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
    return {"summary": "supervisor canary model"}


def _child(mode: str, root: Path) -> int:
    worker = _BlockingWorker(root) if mode == "holder" else _ProbeWorker(root)
    result = run_driver(
        worker,
        holder=mode,
        model=_model,
        max_cycles=2 if mode == "holder" else 1,
        idle_limit=100,
        backoff_seconds=0.01,
    )
    (root / f"{mode}-result.json").write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
    return 0


def _singleton_case(root: Path) -> dict[str, Any]:
    script = str(Path(__file__).resolve())
    holder = subprocess.Popen([sys.executable, "-B", script, "--child", "holder", str(root)])
    try:
        entered = root / "holder-entered"
        deadline = time.monotonic() + 5.0
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        contender = subprocess.run(
            [sys.executable, "-B", script, "--child", "contender", str(root)],
            capture_output=True,
            text=True,
            check=False,
        )
        (root / "release-holder").write_text("release\n", encoding="utf-8")
        holder_exit = holder.wait(timeout=5.0)
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=5.0)

    contender_result_path = root / "contender-result.json"
    contender_result = json.loads(contender_result_path.read_text(encoding="utf-8")) if contender_result_path.exists() else {}
    return {
        "ok": (
            entered.exists()
            and contender.returncode == 0
            and contender_result.get("stop_reason") == "not_holder"
            and contender_result.get("cycles") == 0
            and not (root / "contender-ticked").exists()
            and holder_exit == 0
        ),
        "detail": {
            "holder_exit": holder_exit,
            "contender_exit": contender.returncode,
            "contender_stdout": contender.stdout,
            "contender": contender_result,
            "contender_ticked": (root / "contender-ticked").exists(),
        },
    }


def _scheduled_tick_case(root: Path) -> dict[str, Any]:
    goals = GoalStore(root / "scheduled-goals")
    runs = RunStore(root / "scheduled-runs")
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=LoopController(runs, root / "scheduled-workspaces"),
        compilers={},
        execution_context={},
    )
    first = submit_command(
        goals,
        source="scheduler",
        event_type="scheduled_tick",
        event_id="wake-1",
        idempotency_key="wake-1",
        payload={"campaign_id": "campaign-supervisor", "wake_key": "wake-1"},
    )
    consumed = worker.tick(holder="scheduled-worker", model=_model)
    event_after = goals.get_event("wake-1")
    replay = submit_command(
        goals,
        source="scheduler",
        event_type="scheduled_tick",
        event_id="wake-1",
        idempotency_key="wake-1",
        payload={"campaign_id": "campaign-supervisor", "wake_key": "wake-1"},
    )
    after_replay = worker.tick(holder="scheduled-worker", model=_model)
    summary = goals.summary()
    ok = (
        first["status"] == "received"
        and consumed["event"]["status"] == "scheduled_tick_consumed"
        and event_after["state"] == "completed"
        and replay["status"] == "reused"
        and replay["state"] == "completed"
        and after_replay["status"] == "idle"
        and summary["event_count"] == 1
        and summary["goal_count"] == 0
    )
    return {
        "ok": ok,
        "detail": {
            "first": first,
            "consumed": consumed["event"],
            "event_after": {"state": event_after["state"], "result": event_after["result"]},
            "replay": replay,
            "after_replay_status": after_replay["status"],
            "summary": summary,
        },
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-supervisor-") as raw:
        root = Path(raw)
        singleton = _singleton_case(root / "singleton")
        scheduled = _scheduled_tick_case(root)

    cases = [
        {"id": "singleton-second-process-not-holder", **singleton},
        {"id": "scheduled-tick-consumes-idempotently", **scheduled},
    ]
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-supervisor",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--child":
        raise SystemExit(_child(sys.argv[2], Path(sys.argv[3])))
    raise SystemExit(main())

"""Offline acceptance canary for the supervisor singleton and wake event."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
# Upper bounds on child start/exit waits; no case asserts speed, so a loaded host must not fail them.
CHILD_WAIT_SECONDS = 30.0
sys.path.insert(0, str(HERE))

from command_ingress import submit_command
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore
from status_snapshot import build_snapshot


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
    if mode == "crash":
        marker = root / "crash-entered"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("crash-before-release\n", encoding="utf-8")
        os._exit(17)
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
        deadline = time.monotonic() + CHILD_WAIT_SECONDS
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        contender = subprocess.run(
            [sys.executable, "-B", script, "--child", "contender", str(root)],
            capture_output=True,
            text=True,
            check=False,
        )
        (root / "release-holder").write_text("release\n", encoding="utf-8")
        holder_exit = holder.wait(timeout=CHILD_WAIT_SECONDS)
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=CHILD_WAIT_SECONDS)

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


def _crash_restart_case(root: Path) -> dict[str, Any]:
    """Prove the host can re-arm after a real process exit, not just return idle."""
    script = str(Path(__file__).resolve())
    crashed = subprocess.Popen([sys.executable, "-B", script, "--child", "crash", str(root)])
    marker = root / "crash-entered"
    deadline = time.monotonic() + CHILD_WAIT_SECONDS
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    crash_exit = crashed.wait(timeout=CHILD_WAIT_SECONDS)
    restarted = subprocess.run(
        [sys.executable, "-B", script, "--child", "restart", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    result_path = root / "restart-result.json"
    result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
    return {
        "ok": (
            marker.exists()
            and crash_exit == 17
            and restarted.returncode == 0
            and result.get("cycles") == 1
            and result.get("stop_reason") == "max_cycles"
            and (root / "contender-ticked").exists()
        ),
        "detail": {
            "crash_exit": crash_exit,
            "restart_exit": restarted.returncode,
            "restart_result": result,
            "restart_stdout": restarted.stdout,
        },
    }


def _stale_and_human_stop_case(root: Path) -> dict[str, Any]:
    runs = RunStore(root / "runs")
    goals = GoalStore(root / "goals")
    old_heartbeat = {
        "schema": "loop-hybrid-driver-heartbeat/v1",
        "holder": "driver:stale",
        "monotonic_ts": 1.0,
        "wall_ts": (datetime.now(timezone.utc) - timedelta(seconds=100)).isoformat(),
        "phase": "progress",
        "cycles": 1,
    }
    snapshot = build_snapshot(
        runs,
        goals,
        generated_at=datetime.now(timezone.utc).isoformat(),
        heartbeat=old_heartbeat,
        attempt_timeout_seconds=1.0,
    )
    pause = root / "pause"
    pause.write_text("operator-stop\n", encoding="utf-8")
    stopped = run_driver(
        _ProbeWorker(root / "stopped"),
        holder="operator-stop",
        model=_model,
        pause_flag=pause,
        max_cycles=1,
        backoff_seconds=0,
    )
    return {
        "ok": (
            snapshot["stale"] is True
            and stopped["stop_reason"] == "paused"
            and stopped["cycles"] == 0
            and not (root / "stopped" / "contender-ticked").exists()
        ),
        "detail": {"snapshot": {"stale": snapshot["stale"], "heartbeat_age_seconds": snapshot["heartbeat_age_seconds"], "threshold": snapshot["staleness_threshold_seconds"]}, "stop": stopped},
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-supervisor-") as raw:
        root = Path(raw)
        singleton = _singleton_case(root / "singleton")
        scheduled = _scheduled_tick_case(root)
        crashed = _crash_restart_case(root / "crash-restart")
        stale_stop = _stale_and_human_stop_case(root / "stale-stop")

    cases = [
        {"id": "singleton-second-process-not-holder", **singleton},
        {"id": "scheduled-tick-consumes-idempotently", **scheduled},
        {"id": "crash-releases-owner-and-restart-reacquires", **crashed},
        {"id": "stale-heartbeat-and-stop-gate-observed", **stale_stop},
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

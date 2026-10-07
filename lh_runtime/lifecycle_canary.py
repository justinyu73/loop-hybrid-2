#!/usr/bin/env python3
"""Offline acceptance canary for the P3 foreground lifecycle seam."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

from controller import LoopController  # noqa: E402
from _fixture import make_source_repo  # noqa: E402
from goal_loop_worker import GoalLoopWorker  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from goal_loop_driver import run_driver  # noqa: E402
from lifecycle import (  # noqa: E402
    ForegroundLifecycle,
    NativeProcessIdentityPort,
    ProcessIdentity,
    build_foreground_descriptor,
    read_owner_record,
)
from run_store import RunStore  # noqa: E402
from p7_fence_fixture import fixture_command_runner
from native_delivery_fixture import make_native_run  # noqa: E402
import token_cost  # noqa: E402


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": ok, "detail": detail}


class _FixtureIdentityPort:
    def current(self) -> ProcessIdentity:
        return ProcessIdentity(pid=9001, start_token="new-start", source="fixture")

    def observe(self, pid: int) -> ProcessIdentity | None:
        if pid == 7001:
            return ProcessIdentity(pid=pid, start_token="replacement-start", source="fixture")
        return None


class _EmptyGoalStore:
    def goals_in_state(self, _state: str) -> list[dict[str, Any]]:
        return []


class _BlockingWorker:
    def __init__(self, root: Path) -> None:
        self.run_store = RunStore(root / "runs")
        self.goal_store = _EmptyGoalStore()

    def tick(self, **_kwargs: Any) -> dict[str, Any]:
        entered = self.run_store.root.parent / "entered"
        entered.write_text("tick\n", encoding="utf-8")
        release = self.run_store.root.parent / "release"
        while not release.exists():
            time.sleep(0.01)
        return {"status": "idle", "run": None, "terminal_after": None}


def _model(_workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
    return {"summary": "lifecycle canary model"}


def _child(mode: str, root: Path) -> int:
    root.mkdir(parents=True, exist_ok=True)
    if mode == "holder":
        lease = ForegroundLifecycle().acquire(root, "holder")
        if lease is None:
            return 3
        (root / "entered").write_text("holder\n", encoding="utf-8")
        while not (root / "release").exists():
            time.sleep(0.01)
        lease.close("test_complete")
        return 0
    if mode == "contender":
        lease = ForegroundLifecycle().acquire(root, "contender")
        (root / "contender-result.json").write_text(
            json.dumps({"acquired": lease is not None}), encoding="utf-8"
        )
        if lease is not None:
            lease.close("unexpected_acquisition")
        return 0
    if mode == "crash":
        lease = ForegroundLifecycle().acquire(root, "crash")
        if lease is None:
            return 4
        (root / "entered").write_text("crash\n", encoding="utf-8")
        os._exit(17)
    if mode == "shutdown":
        result = run_driver(
            _BlockingWorker(root),
            holder="shutdown",
            model=_model,
            max_cycles=10,
            idle_limit=100,
            backoff_seconds=0.01,
        )
        (root / "result.json").write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        return 0
    return 2


def _wait_for(path: Path, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    return path.exists()


def _single_holder_case(root: Path) -> dict[str, Any]:
    script = str(Path(__file__).resolve())
    holder = subprocess.Popen([sys.executable, "-B", script, "--child", "holder", str(root)])
    contender: subprocess.CompletedProcess[str] | None = None
    holder_exit: int | None = None
    try:
        entered = _wait_for(root / "entered")
        contender = subprocess.run(
            [sys.executable, "-B", script, "--child", "contender", str(root)],
            capture_output=True,
            text=True,
            check=False,
        )
        (root / "release").write_text("release\n", encoding="utf-8")
        holder_exit = holder.wait(timeout=5.0)
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=5.0)
    contender_record = {}
    result_path = root / "contender-result.json"
    if result_path.exists():
        contender_record = json.loads(result_path.read_text(encoding="utf-8"))
    owner = read_owner_record(root / "driver.owner.json") or {}
    return {
        "ok": (
            entered
            and contender is not None
            and contender.returncode == 0
            and contender_record.get("acquired") is False
            and holder_exit == 0
            and owner.get("state") == "stopped"
        ),
        "detail": {
            "entered": entered,
            "holder_exit": holder_exit,
            "contender_exit": contender.returncode if contender is not None else None,
            "contender": contender_record,
            "owner": owner,
        },
    }


def _crash_restart_case(root: Path) -> dict[str, Any]:
    script = str(Path(__file__).resolve())
    crashed = subprocess.Popen([sys.executable, "-B", script, "--child", "crash", str(root)])
    entered = _wait_for(root / "entered")
    crash_exit = crashed.wait(timeout=5.0)
    previous = read_owner_record(root / "driver.owner.json") or {}
    restarted = ForegroundLifecycle().acquire(root, "restart")
    recovery = restarted.recovery if restarted is not None else None
    if restarted is not None:
        restarted.close("restart_complete")
    final = read_owner_record(root / "driver.owner.json") or {}
    return {
        "ok": (
            entered
            and crash_exit == 17
            and previous.get("state") == "running"
            and recovery == "previous_process_exited"
            and final.get("state") == "stopped"
        ),
        "detail": {
            "entered": entered,
            "crash_exit": crash_exit,
            "previous": previous,
            "recovery": recovery,
            "final": final,
        },
    }


def _graceful_shutdown_case(root: Path) -> dict[str, Any]:
    script = str(Path(__file__).resolve())
    # Windows has no SIGTERM delivery to another process; its graceful stop is
    # CTRL_BREAK_EVENT to the child's own process group (SIGBREAK in the child).
    windows = sys.platform == "win32"
    child = subprocess.Popen([sys.executable, "-B", script, "--child", "shutdown", str(root)],
                             creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if windows else 0)
    entered = _wait_for(root / "entered")
    if entered:
        child.send_signal(signal.CTRL_BREAK_EVENT if windows else signal.SIGTERM)
    (root / "release").write_text("release\n", encoding="utf-8")
    exit_code = child.wait(timeout=5.0)
    result_path = root / "result.json"
    result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
    owner = read_owner_record(root / "runs" / "driver.owner.json") or {}
    return {
        "ok": (
            entered
            and exit_code == 0
            and result.get("stop_reason") == "shutdown_requested"
            and result.get("cycles") == 1
            and result.get("runs_dispatched") == 0
            and owner.get("state") == "shutdown_complete"
        ),
        "detail": {"entered": entered, "exit_code": exit_code, "result": result, "owner": owner},
    }


def _pid_reuse_case(root: Path) -> dict[str, Any]:
    owner_path = root / "driver.owner.json"
    owner_path.parent.mkdir(parents=True, exist_ok=True)
    owner_path.write_text(json.dumps({
        "schema": "lh-runtime-owner-lease/v1",
        "owner_id": "old-owner",
        "process_identity": {"pid": 7001, "start_token": "old-start", "source": "fixture"},
        "lease_id": "old-lease",
        "state": "running",
        "phase": "tick",
    }), encoding="utf-8")
    lifecycle = ForegroundLifecycle(identity_port=_FixtureIdentityPort())
    lease = lifecycle.acquire(root, "new-owner")
    recovery = lease.recovery if lease is not None else None
    if lease is not None:
        lease.close("pid_reuse_test")
    record = read_owner_record(owner_path) or {}
    return {
        "ok": recovery == "pid_reused" and record.get("state") == "stopped",
        "detail": {"recovery": recovery, "record": record},
    }


def _durable_completed_attempt_case(root: Path) -> dict[str, Any]:
    runs = RunStore(root / "runs", command_runner=fixture_command_runner)
    source, base = make_source_repo(root / "source")
    run_id = make_native_run(
        runs,
        source,
        base,
        "run-completed",
        "lifecycle",
        [{
            "id": "lifecycle-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["git", "rev-parse", "HEAD"],
        ["src/"],
        4,
        goal={"case": "completed-attempt"},
        run_id="run-completed",
    )["run_id"]
    completed = LoopController(runs, root / "workspaces").tick(
        run_id,
        holder="lifecycle-complete",
        model=_model,
        verifier_argv=["git", "diff", "--check"],
    )
    worker = GoalLoopWorker(
        goal_store=GoalStore(root / "goals"),
        run_store=runs,
        controller=LoopController(runs, root / "workspaces"),
        compilers={},
        execution_context={},
    )
    first = run_driver(worker, holder="restart-one", model=_model, idle_limit=1, max_cycles=3)
    second = run_driver(worker, holder="restart-two", model=_model, idle_limit=1, max_cycles=3)
    latest = runs.latest_receipt(run_id) or {}
    receipt_readable = json.loads((runs.root / latest["receipt_ref"]).read_text(encoding="utf-8"))
    summary = runs.summary()
    return {
        "ok": (
            completed.get("status") == "verified"
            and first["runs_dispatched"] == 0
            and second["runs_dispatched"] == 0
            and summary["runs_by_state"].get("verified") == 1
            and latest["ordinal"] == 1
            and receipt_readable["run_id"] == run_id
        ),
        "detail": {"completed": completed, "first": first, "second": second, "summary": summary, "latest": latest},
    }


def _adapter_case() -> dict[str, Any]:
    foreground = build_foreground_descriptor(("python", "-m", "lh_runtime.goal_loop_run"))
    ok = (foreground["optional"] is False and foreground["platform"] == "any"
          and foreground["foreground"]["shell"] is False
          and foreground["foreground"]["bounded_session"] is True
          and isinstance(foreground["foreground"]["argv"], list))
    return {"ok": ok, "detail": {"foreground": foreground}}


def main() -> int:
    cases: list[dict[str, Any]] = []
    identity = NativeProcessIdentityPort()
    current = identity.current()
    observed = identity.observe(os.getpid())
    cases.append(_case(
        "native-process-identity-has-birth-token",
        current is not None and current.matches(observed),
        {"current": current.as_dict() if current else None, "observed": observed.as_dict() if observed else None},
    ))
    adapter = _adapter_case()
    cases.append(_case("foreground-lifecycle-is-argv-bound", adapter["ok"], adapter["detail"]))
    with tempfile.TemporaryDirectory(prefix="lh-p3-") as raw:
        root = Path(raw)
        holder = _single_holder_case(root / "holder")
        crash = _crash_restart_case(root / "crash")
        shutdown = _graceful_shutdown_case(root / "shutdown")
        reuse = _pid_reuse_case(root / "reuse")
        durable = _durable_completed_attempt_case(root / "durable")
    cases.extend([
        _case("single-holder-records-owner", holder["ok"], holder["detail"]),
        _case("crash-restart-reacquires-after-exit", crash["ok"], crash["detail"]),
        _case("signal-shutdown-is-graceful", shutdown["ok"], shutdown["detail"]),
        _case("pid-reuse-does-not-match-old-owner", reuse["ok"], reuse["detail"]),
        _case("restart-does-not-redispatch-completed-attempt", durable["ok"], durable["detail"]),
    ])
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-lifecycle-p3",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "host service/task installation and reboot readback remain platform acceptance work",
            "macOS and Windows live host proof require their native runners",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--child":
        raise SystemExit(_child(sys.argv[2], Path(sys.argv[3])))
    raise SystemExit(main())

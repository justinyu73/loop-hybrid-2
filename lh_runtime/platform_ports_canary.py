#!/usr/bin/env python3
"""Provider-free P1/P5 acceptance checks for the portable platform seams."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "tools"))
sys.path.insert(0, str(HERE))

from controller import LoopController, _TimeoutBudget  # noqa: E402
from run_store import RunStore  # noqa: E402
from portable_runtime_contract import core_contract_issues  # noqa: E402
from platform_ports import (  # noqa: E402
    CapabilityUnavailable,
    DeadlineExpired,
    EnvironmentSecretStore,
    FileLockSchedulerPort,
    LocalProcessPort,
    OptionalCapabilityAdapter,
    OptionalCapabilityRegistry,
    MonotonicDeadlinePort,
    PlatformPaths,
    PortableFileLock,
    ProcessResult,
    ProcessTimeout,
    UnsupportedPlatformError,
    build_process_port,
    build_shell_port,
)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _temporary_directory(prefix: str) -> tempfile.TemporaryDirectory[str]:
    configured = os.environ.get("LH_TASK_TMP_ROOT", "").strip()
    if not configured:
        return tempfile.TemporaryDirectory(prefix=prefix)
    parent = Path(configured).expanduser() / "platform-ports-canary"
    parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=str(parent))


def main() -> int:
    with _temporary_directory("lh-p5-") as raw:
        root = Path(raw)
        process = LocalProcessPort()
        result = process.run(
            [sys.executable, "-c", "print('argv-port')"],
            cwd=root,
            timeout=2,
        )
        try:
            process.run(
                [sys.executable, "-c", "import time; time.sleep(0.2)"],
                cwd=root,
                timeout=0.01,
            )
        except ProcessTimeout:
            timed_out = True
        else:
            timed_out = False

        now = [100.0]
        deadline = MonotonicDeadlinePort(clock=lambda: now[0]).start(1)
        now[0] = 102.0
        try:
            deadline.check()
        except DeadlineExpired:
            deadline_expired = True
        else:
            deadline_expired = False

        lock_port = PortableFileLock()
        first = lock_port.acquire(root / "locks" / "driver.lock")
        second = lock_port.acquire(root / "locks" / "driver.lock")
        if first is not None:
            lock_port.release(first)
        third = lock_port.acquire(root / "locks" / "driver.lock")
        if third is not None:
            lock_port.release(third)

        scheduler = FileLockSchedulerPort()
        scheduled = scheduler.acquire(root / "scheduled")
        scheduled_busy = scheduler.acquire(root / "scheduled")
        if scheduled is not None:
            scheduler.release(scheduled)
        custom_scheduler = FileLockSchedulerPort(filename="custom.lock")

        paths = PlatformPaths.from_environment(
            {"HOME": "/clean-user", "XDG_STATE_HOME": "/clean-state"},
            platform_name="linux",
        )
        windows_paths = PlatformPaths.from_environment(
            {"LH_INSTANCE_ROOT": str(root / "D" / "LoopHybrid")},
            platform_name="win32",
        )
        unsupported = build_process_port(platform_name="freebsd")
        unsupported_marker = root / "unsupported-child-ran"
        try:
            unsupported.run([
                sys.executable,
                "-B",
                "-c",
                f"from pathlib import Path; Path({str(unsupported_marker)!r}).write_text('ran')",
            ], cwd=root, timeout=2)
        except UnsupportedPlatformError as exc:
            unsupported_process = (
                exc.code == "unsupported_platform"
                and exc.platform_name == "freebsd"
                and not unsupported_marker.exists()
            )
        else:
            unsupported_process = False

        unsupported_shell = build_shell_port(platform_name="freebsd")
        try:
            unsupported_shell.run_shell("echo unsupported", cwd=root, timeout=2)
        except UnsupportedPlatformError:
            unsupported_shell_blocked = True
        else:
            unsupported_shell_blocked = False

        adapter_calls: list[str] = []
        adapter = OptionalCapabilityAdapter(
            "fixture.host-feature",
            ("freebsd",),
            lambda: adapter_calls.append("built") or {"adapter": "fixture"},
            "fixture-host-adapter",
        )
        adapters = OptionalCapabilityRegistry([adapter])
        try:
            adapters.resolve("fixture.host-feature", platform_name="linux")
        except CapabilityUnavailable:
            capability_missing_is_closed = True
        else:
            capability_missing_is_closed = False
        capability_value = adapters.resolve("fixture.host-feature", platform_name="freebsd")

        try:
            PlatformPaths.from_environment({"HOME": str(root / "home")}, platform_name="freebsd")
        except UnsupportedPlatformError:
            unsupported_path_default = True
        else:
            unsupported_path_default = False
        explicit_unknown_paths = PlatformPaths.from_environment(
            {"LH_INSTANCE_ROOT": str(root / "explicit-root")},
            platform_name="freebsd",
        )
        secrets = EnvironmentSecretStore({"LH_TEST_SECRET": "fixture-only"})

        class RecordingProcessPort:
            def __init__(self) -> None:
                self.calls: list[tuple[str, ...]] = []

            def run(self, argv, *, cwd=None, timeout=None, env=None):
                del cwd, timeout, env
                normalized = tuple(str(item) for item in argv)
                self.calls.append(normalized)
                return ProcessResult(normalized, 0, "recorded", "")

        recording = RecordingProcessPort()
        controller = LoopController(
            RunStore(root / "store"),
            root / "workspaces",
            process_port=recording,
        )
        controller_result = controller._run(
            ["fixture", "argv"],
            cwd=None,
            budget=_TimeoutBudget(2, controller.deadline_port),
        )

        forbidden = ("import signal", "import fcntl", "SIGALRM", "setitimer", "wsl.localhost", "systemd")
        boundary_ok = all(
            not any(token in (HERE / filename).read_text(encoding="utf-8") for token in forbidden)
            for filename in ("controller.py", "goal_loop_driver.py")
        )
        static_contract_issues = core_contract_issues()

        cases = [
            case("argv-process-is-captured-without-shell", result.returncode == 0 and result.stdout.strip() == "argv-port", repr(result)),
            case("process-timeout-is-portable", timed_out, str(timed_out)),
            case("deadline-is-monotonic-and-signal-free", deadline_expired, str(deadline_expired)),
            case("file-lock-rejects-second-holder", first is not None and second is None, f"first={first} second={second}"),
            case("file-lock-can-be-reacquired-after-release", third is not None, str(third)),
            case("scheduler-port-enforces-single-holder", scheduled is not None and scheduled_busy is None, f"scheduled={scheduled} busy={scheduled_busy}"),
            case("scheduler-reports-its-resolved-lock-path", custom_scheduler.lock_path(root / "scheduled") == root / "scheduled" / "custom.lock", str(custom_scheduler.lock_path(root / "scheduled"))),
            case("platform-paths-have-clean-user-overrides", paths.state_root.as_posix() == "/clean-state/loop-hybrid/state" and "/home/user" not in paths.state_root.as_posix(), str(paths)),
            case("windows-paths-are-instance-relative", windows_paths.instance_root == root / "D" / "LoopHybrid" and windows_paths.workspace_root == root / "D" / "LoopHybrid" / "workspaces", str(windows_paths)),
            case("unsupported-platform-process-fails-before-child", unsupported_process, f"port={unsupported} marker={unsupported_marker.exists()}"),
            case("unsupported-platform-shell-fails-before-child", unsupported_shell_blocked, str(unsupported_shell)),
            case("optional-capability-is-explicit-and-lazy", capability_missing_is_closed and capability_value == {"adapter": "fixture"} and adapter_calls == ["built"], f"value={capability_value} calls={adapter_calls}"),
            case("unsupported-platform-has-no-implicit-path-default", unsupported_path_default and explicit_unknown_paths.instance_root == root / "explicit-root", str(explicit_unknown_paths)),
            case("secret-store-does-not-write-files", secrets.get("LH_TEST_SECRET") == "fixture-only" and not (root / "secret").exists(), str(secrets.get("LH_TEST_SECRET"))),
            case("controller-uses-injected-process-port", controller_result.stdout == "recorded" and recording.calls == [("fixture", "argv")], f"{controller_result} calls={recording.calls}"),
            case("core-has-no-host-lock-or-signal-imports", boundary_ok, str(boundary_ok)),
            case("portable-runtime-contract-has-no-fixed-host-defaults", not static_contract_issues, str(static_contract_issues)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-platform-ports-p1",
        "acceptance_id": "lh-platform-ports-p5",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "provider adapter subprocess seams remain outside this P1 controller/driver slice",
            "PlatformPaths is a P1 contract seam; live instance-root wiring is P2 scope",
            "Windows msvcrt locking requires a Windows-hosted acceptance run",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

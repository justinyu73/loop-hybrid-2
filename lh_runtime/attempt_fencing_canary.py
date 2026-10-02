#!/usr/bin/env python3
"""Live two-process proof for lease exclusivity and late-finish fencing."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from controller import LoopController
from native_delivery_fixture import make_native_run
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _wait(path: Path, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            raw = path.read_text(encoding="utf-8")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"raw": raw}
        time.sleep(0.01)
    raise TimeoutError(f"timed out waiting for {path}")


def _repo(root: Path) -> tuple[Path, str]:
    source = root / "repo"
    source.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "config", "user.email", "fence@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(source), "config", "user.name", "Fence Canary"], check=True)
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source), "add", "baseline.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "baseline"], check=True)
    base = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    return source, base


def _checks() -> list[dict[str, object]]:
    return [{
        "id": "fence-diff-check",
        "commands": [{
            "id": "fence-diff",
            "argv": ["git", "diff", "--check"],
            "cwd": "${WORKTREE}",
            "expect_exit": 0,
            "timeout_seconds": 10,
        }],
        "required_receipts": ["executor"],
    }]


def _verifier(flag: Path) -> list[str]:
    program = (
        "from pathlib import Path; import subprocess; "
        "assert Path('baseline.txt').read_text(encoding='utf-8') == 'baseline\\n'; "
        f"assert Path({str(flag)!r}).is_file(); "
        "assert Path(subprocess.check_output(['git','rev-parse','--show-toplevel'], text=True).strip()).resolve() == Path.cwd().resolve()"
    )
    return [sys.executable, "-B", "-c", program]


def _child(mode: str, root: Path, run_id: str, marker: Path, release: Path) -> int:
    store = RunStore(root, command_runner=fixture_command_runner)
    if mode not in {"lease", "late-old", "late-new"}:
        raise ValueError(f"unknown child mode: {mode}")
    controller = LoopController(
        store,
        root / "workspaces",
        timeout_seconds=20,
        lease_seconds=0.2 if mode == "late-old" else 5.0,
    )
    flag = root.parent / ("lease-flag" if mode == "lease" else "late-flag")

    recovered = False
    if mode == "late-new":
        recovered = store.recover_stale_run(run_id)

    def model(workspace: Path, capsule: dict[str, object]) -> dict[str, object]:
        flag.write_text("model started\n", encoding="utf-8")
        _write(marker, {
            "acquired": True,
            "recovered": recovered,
            "attempt": capsule["attempt"],
            "fence": capsule["fence"],
            "model_started": True,
        })
        _wait(release)
        return {"summary": f"real controller fence fixture {mode}"}

    result = controller.tick(
        run_id,
        holder=f"canary-{mode}",
        model=model,
        verifier_argv=_verifier(flag),
    )
    if result.get("status") == "lease_busy":
        _write(marker, {"acquired": False, "status": "lease_busy"})
        return 0
    _write(marker, {
        "acquired": True,
        "recovered": recovered,
        "attempt": result.get("attempt"),
        "fence": result.get("fence"),
        "finished": result.get("status") == "verified",
        "controller": result,
    })
    return 0


def _spawn(mode: str, root: Path, run_id: str, marker: Path, release: Path) -> subprocess.Popen:
    return subprocess.Popen([
        sys.executable,
        "-B",
        str(Path(__file__).resolve()),
        "--child",
        mode,
        str(root),
        run_id,
        str(marker),
        str(release),
    ])


def _run_live_lease_case(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    source, base = _repo(root)
    state_root = root / "state"
    store = RunStore(state_root)
    bundle = make_native_run(
        store,
        source,
        base,
        "fence-live-lease",
        "fence",
        _checks(),
        _verifier(root / "lease-flag"),
        ["src/"],
        3,
        goal={"case": "live-lease"},
        run_id="run-live-lease",
    )
    run_id = bundle["run_id"]
    first_marker = root / "first.json"
    second_marker = root / "second.json"
    release = root / "release-first"
    first = _spawn("lease", state_root, run_id, first_marker, release)
    try:
        first_state = _wait(first_marker)
        second = _spawn("lease", state_root, run_id, second_marker, release)
        second_state = _wait(second_marker)
        release.write_text("release\n", encoding="utf-8")
        first_exit = first.wait(timeout=10)
        second_exit = second.wait(timeout=10)
    finally:
        if first.poll() is None:
            first.terminate()
            first.wait(timeout=5)
        if "second" in locals() and second.poll() is None:
            second.terminate()
            second.wait(timeout=5)
    final = store.get_run(run_id)
    attempts = [row for row in store.events(run_id) if row["event_type"] == "attempt_started"]
    ok = (
        first_state.get("acquired") is True
        and second_state.get("acquired") is False
        and first_exit == 0
        and second_exit == 0
        and final["attempts"] == 1
        and final["state"] == "verified"
        and len(attempts) == 1
        and first_state.get("model_started") is True
        and first_state.get("attempt") == 1
    )
    return {"id": "two-process-single-attempt", "ok": ok, "detail": {"first": first_state, "second": second_state, "final": {"attempts": final["attempts"], "state": final["state"]}}}


def _run_live_late_finish_case(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    source, base = _repo(root)
    state_root = root / "state"
    store = RunStore(state_root)
    bundle = make_native_run(
        store,
        source,
        base,
        "fence-late-finish",
        "fence",
        _checks(),
        _verifier(root / "late-flag"),
        ["src/"],
        3,
        goal={"case": "late-finish"},
        run_id="run-late-finish",
    )
    run_id = bundle["run_id"]
    old_marker = root / "old.json"
    new_marker = root / "new.json"
    allow_old = root / "allow-old"
    allow_new = root / "allow-new"
    old = _spawn("late-old", state_root, run_id, old_marker, allow_old)
    try:
        old_state = _wait(old_marker)
        time.sleep(0.35)
        (root / "late-flag").unlink(missing_ok=True)
        new = _spawn("late-new", state_root, run_id, new_marker, allow_new)
        new_state = _wait(new_marker)
        allow_old.write_text("finish\n", encoding="utf-8")
        old_exit = old.wait(timeout=10)
        after_old = store.get_run(run_id)
        after_old_attempts = [row for row in store.events(run_id) if row["event_type"] == "attempt_started"]
        allow_new.write_text("finish\n", encoding="utf-8")
        new_exit = new.wait(timeout=10)
    finally:
        if old.poll() is None:
            old.terminate()
            old.wait(timeout=5)
        if "new" in locals() and new.poll() is None:
            new.terminate()
            new.wait(timeout=5)
    final = store.get_run(run_id)
    rejected = [row for row in store.events(run_id) if row["event_type"] == "fence_rejected"]
    ok = (
        old_state.get("attempt") == 1
        and old_state.get("model_started") is True
        and new_state.get("recovered") is True
        and new_state.get("attempt") == 2
        and new_state.get("model_started") is True
        and old_exit == 0
        and new_exit == 0
        and after_old["attempts"] == 2
        and after_old["state"] == "running"
        and after_old_attempts[-1]["payload"]["attempt"] == 2
        and rejected
        and final["attempts"] == 2
        and final["state"] == "verified"
    )
    return {"id": "late-finish-is-fenced", "ok": ok, "detail": {"old": old_state, "new": new_state, "after_old": {"attempts": after_old["attempts"], "state": after_old["state"]}, "final": {"attempts": final["attempts"], "state": final["state"]}, "rejections": len(rejected)}}


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        return _child(sys.argv[2], Path(sys.argv[3]), sys.argv[4], Path(sys.argv[5]), Path(sys.argv[6]))
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        cases = [_run_live_lease_case(root / "lease"), _run_live_late_finish_case(root / "late")]
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "lh-attempt-fencing", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "cases": cases}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

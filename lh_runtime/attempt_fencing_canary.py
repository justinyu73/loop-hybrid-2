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
from run_store import RunStore


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


def _child(mode: str, root: Path, run_id: str, marker: Path, release: Path) -> int:
    store = RunStore(root)
    if mode == "lease":
        acquired = store.acquire_lease(run_id, "canary", seconds=5)
        if not acquired:
            _write(marker, {"acquired": False})
            return 0
        ordinal = store.begin_attempt(run_id, "workspace://canary/1")
        fence = store.attempt_fence(run_id, ordinal)
        _write(marker, {"acquired": True, "attempt": ordinal, "fence": fence})
        _wait(release)
        finished = store.finish_attempt(
            run_id,
            ordinal,
            state="verified",
            receipt_ref=f"artifacts/{run_id}/1/receipt.json",
            receipt_digest="sha256:canary",
            fence=fence,
        )
        _write(marker, {"acquired": True, "attempt": ordinal, "fence": fence, "finished": finished})
        store.release_lease(run_id, "canary")
        return 0

    if mode == "late-old":
        if not store.acquire_lease(run_id, "canary-old", seconds=0.2):
            _write(marker, {"error": "old process could not acquire lease"})
            return 1
        ordinal = store.begin_attempt(run_id, "workspace://canary/old")
        fence = store.attempt_fence(run_id, ordinal)
        _write(marker, {"attempt": ordinal, "fence": fence})
        _wait(release)
        finished = store.finish_attempt(
            run_id,
            ordinal,
            state="verified",
            receipt_ref=f"artifacts/{run_id}/1/receipt.json",
            receipt_digest="sha256:late-old",
            fence=fence,
        )
        _write(marker, {"attempt": ordinal, "fence": fence, "finished": finished})
        store.release_lease(run_id, "canary-old")
        return 0

    if mode == "late-new":
        recovered = store.recover_stale_run(run_id)
        if not recovered or not store.acquire_lease(run_id, "canary-new", seconds=5):
            _write(marker, {"error": "new process did not recover and acquire"})
            return 1
        ordinal = store.begin_attempt(run_id, "workspace://canary/new")
        fence = store.attempt_fence(run_id, ordinal)
        _write(marker, {"recovered": recovered, "attempt": ordinal, "fence": fence})
        _wait(release)
        finished = store.finish_attempt(
            run_id,
            ordinal,
            state="verified",
            receipt_ref=f"artifacts/{run_id}/2/receipt.json",
            receipt_digest="sha256:late-new",
            fence=fence,
        )
        _write(marker, {"recovered": recovered, "attempt": ordinal, "fence": fence, "finished": finished})
        store.release_lease(run_id, "canary-new")
        return 0

    raise ValueError(f"unknown child mode: {mode}")


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
    store = RunStore(root)
    run_id = store.create_run(goal={"case": "live-lease"}, source_repo=root, base_revision="base", run_id="run-live-lease")
    first_marker = root / "first.json"
    second_marker = root / "second.json"
    release = root / "release-first"
    first = _spawn("lease", root, run_id, first_marker, release)
    try:
        first_state = _wait(first_marker)
        second = _spawn("lease", root, run_id, second_marker, release)
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
    )
    return {"id": "two-process-single-attempt", "ok": ok, "detail": {"first": first_state, "second": second_state, "final": {"attempts": final["attempts"], "state": final["state"]}}}


def _run_live_late_finish_case(root: Path) -> dict:
    store = RunStore(root)
    run_id = store.create_run(goal={"case": "late-finish"}, source_repo=root, base_revision="base", run_id="run-late-finish")
    old_marker = root / "old.json"
    new_marker = root / "new.json"
    allow_old = root / "allow-old"
    allow_new = root / "allow-new"
    old = _spawn("late-old", root, run_id, old_marker, allow_old)
    try:
        old_state = _wait(old_marker)
        time.sleep(0.35)
        new = _spawn("late-new", root, run_id, new_marker, allow_new)
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
        and new_state.get("recovered") is True
        and new_state.get("attempt") == 2
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

#!/usr/bin/env python3
"""Acceptance canary for the C5 non-empty bounded-repo-edit requirement."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from controller import LoopController
from native_delivery_fixture import make_native_run
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def _source(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    _git("init", "-q", str(source))
    _git("-C", str(source), "config", "user.email", "c5-canary@example.invalid")
    _git("-C", str(source), "config", "user.name", "C5 bounded-edit canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    _git("-C", str(source), "add", "baseline.txt")
    _git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return source, base


def _receipt(store: RunStore, result: dict[str, object]) -> dict[str, object]:
    ref = result.get("receipt_ref")
    if not isinstance(ref, str):
        raise AssertionError(f"missing receipt_ref: {result}")
    return json.loads((store.root / ref).read_text(encoding="utf-8"))


def _goal() -> dict[str, object]:
    return {
        "feature_contract": "C5 bounded repo edit",
        "admission_envelope": {
            "allowed_paths": [".lh-pilot/"],
            "requires_non_empty_diff": True,
        },
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-c5-bounded-edit-") as raw:
        root = Path(raw)
        source, base = _source(root)

        empty_store = RunStore(root / "empty-runs")
        empty_controller = LoopController(empty_store, root / "empty-workspaces")
        empty_run = make_native_run(
            empty_store, source, base, "c5-bounded-empty", "bounded-edit",
            [{"id": "bounded-marker", "commands": [{"id": "marker", "argv": ["test", "-s", ".lh-pilot/bounded-marker.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
            ["test", "-s", ".lh-pilot/bounded-marker.txt"], [".lh-pilot/"], 2,
            goal=_goal(), run_id="run-c5-empty",
        )["run_id"]
        empty_result = empty_controller.tick(
            empty_run,
            holder="c5-canary",
            model=lambda _workspace, _capsule: {"summary": "deliberately empty edit"},
            verifier_argv=["true"],
        )
        empty_receipt = _receipt(empty_store, empty_result)
        empty_edit = empty_receipt["verification"]["bounded_repo_edit"]

        nonempty_store = RunStore(root / "nonempty-runs", command_runner=fixture_command_runner)
        nonempty_controller = LoopController(nonempty_store, root / "nonempty-workspaces")
        nonempty_run = make_native_run(
            nonempty_store, source, base, "c5-bounded-nonempty", "bounded-edit",
            [{"id": "bounded-marker", "commands": [{"id": "marker", "argv": ["test", "-s", ".lh-pilot/bounded-marker.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
            ["test", "-s", ".lh-pilot/bounded-marker.txt"], [".lh-pilot/"], 2,
            goal=_goal(), run_id="run-c5-nonempty",
        )["run_id"]

        def bounded_model(workspace: Path, _capsule: dict[str, object]) -> dict[str, str]:
            marker = workspace / ".lh-pilot" / "bounded-marker.txt"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("c5 bounded edit\n", encoding="utf-8")
            return {"summary": "created one bounded pilot marker"}

        nonempty_result = nonempty_controller.tick(
            nonempty_run,
            holder="c5-canary",
            model=bounded_model,
            verifier_argv=["test", "-s", ".lh-pilot/bounded-marker.txt"],
        )
        nonempty_receipt = _receipt(nonempty_store, nonempty_result)
        nonempty_edit = nonempty_receipt["verification"]["bounded_repo_edit"]

        cases = [
            {
                "id": "empty-diff-cannot-pass",
                "ok": (
                    empty_result.get("status") == "retry_pending"
                    and "precheck" not in empty_result
                    and empty_receipt["verification"]["exit_code"] == -1
                    and empty_edit["required"] is True
                    and empty_edit["observed"] is False
                    and empty_edit["diff_bytes"] == 0
                    and empty_edit["files_touched"] == []
                    and empty_edit["reason"] == "bounded_repo_edit_required_but_diff_empty"
                ),
            },
            {
                "id": "nonempty-bounded-edit-can-pass",
                "ok": (
                    nonempty_result.get("status") == "verified"
                    and nonempty_receipt["verification"]["exit_code"] == 0
                    and nonempty_edit["required"] is True
                    and nonempty_edit["observed"] is True
                    and nonempty_edit["diff_bytes"] > 0
                    and nonempty_edit["files_touched"] == [".lh-pilot/bounded-marker.txt"]
                    and nonempty_edit["reason"] is None
                ),
            },
        ]
        failures = [item for item in cases if not item["ok"]]
        print(json.dumps({
            "check_id": "lh-c5-bounded-edit",
            "status": "pass" if not failures else "fail",
            "total": len(cases),
            "blocking_failures": failures,
        }, ensure_ascii=False, indent=2))
        return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

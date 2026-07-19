#!/usr/bin/env python3
"""Reduce LH verification evidence and reset a marked disposable workspace."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(HERE.parent / "execution_receipt"))
import execution_receipt as er

MARKER_NAME = "lh-disposable-workspace.json"
MARKER_SCHEMA = "lh-disposable-workspace/v1"


def digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def reduce(receipt: dict[str, Any]) -> dict[str, Any]:
    problems = er.validate_receipt(receipt)
    if problems:
        return {"verdict": "ng", "status": "invalid_execution_receipt", "problems": problems}
    failed = sorted(name for name, result in receipt["verification"].items() if result["exit_code"] != 0)
    if not failed:
        return {"verdict": "pass", "status": "verification_passed", "route": "pass", "failed_checks": [], "cleanup_required": False, "problems": []}
    if receipt["failure"] is None:
        return {"verdict": "ng", "status": "failure_summary_required", "problems": ["failed verification requires a bounded failure summary"]}
    if receipt["attempt"] >= er.MAX_ATTEMPTS:
        return {"verdict": "pass", "status": "attempt_budget_exhausted", "route": "stop", "failed_checks": failed, "cleanup_required": False, "failure": receipt["failure"], "problems": []}
    return {"verdict": "pass", "status": "verification_retry", "route": "retry", "failed_checks": failed, "cleanup_required": True, "failure": receipt["failure"], "problems": []}


def _marker_path(workspace: Path) -> Path:
    return workspace / ".git" / MARKER_NAME


def _load_marker(workspace: Path, receipt: dict[str, Any]) -> list[str]:
    marker_path = _marker_path(workspace)
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"disposable workspace marker is unavailable: {exc}"]
    expected = {"schema": MARKER_SCHEMA, "run_id": receipt["run_id"], "workspace_ref": receipt["workspace"]["ref"], "base_revision": receipt["workspace"]["base_revision"]}
    return [f"workspace marker {key} does not match receipt" for key, value in expected.items() if marker.get(key) != value]


def reset_disposable_workspace(receipt: dict[str, Any], workspace: Path) -> dict[str, Any]:
    problems = er.validate_receipt(receipt)
    if problems:
        return {"verdict": "ng", "status": "invalid_execution_receipt", "problems": problems}
    if receipt["workspace"]["disposable"] is not True:
        return {"verdict": "ng", "status": "workspace_not_disposable", "problems": ["receipt does not authorize workspace cleanup"]}
    problems = _load_marker(workspace, receipt)
    if problems:
        return {"verdict": "ng", "status": "workspace_marker_invalid", "problems": problems}
    before = subprocess.run(["git", "-C", str(workspace), "status", "--porcelain"], capture_output=True, text=True)
    if before.returncode != 0:
        return {"verdict": "ng", "status": "workspace_not_git", "problems": [before.stderr.strip() or "workspace is not a Git worktree"]}
    checkout = subprocess.run(["git", "-C", str(workspace), "checkout", "--", "."], capture_output=True, text=True)
    clean = subprocess.run(["git", "-C", str(workspace), "clean", "-fd"], capture_output=True, text=True)
    after = subprocess.run(["git", "-C", str(workspace), "status", "--porcelain"], capture_output=True, text=True)
    if checkout.returncode != 0 or clean.returncode != 0 or after.returncode != 0:
        return {"verdict": "ng", "status": "workspace_reset_failed", "problems": [checkout.stderr.strip(), clean.stderr.strip(), after.stderr.strip()]}
    if after.stdout:
        return {"verdict": "ng", "status": "workspace_not_clean_after_reset", "problems": [after.stdout]}
    return {"verdict": "pass", "status": "workspace_reset", "before_digest": digest(before.stdout), "after_digest": digest(after.stdout), "commands": [["git", "checkout", "--", "."], ["git", "clean", "-fd"]], "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Reduce LH verification evidence or reset a marked disposable workspace")
    parser.add_argument("mode", choices=("reduce", "reset"))
    parser.add_argument("receipt")
    parser.add_argument("--workspace")
    args = parser.parse_args()
    try:
        receipt = json.loads(Path(args.receipt).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        result = {"verdict": "ng", "status": "invalid_execution_receipt", "problems": [str(exc)]}
    else:
        result = reduce(receipt) if args.mode == "reduce" else reset_disposable_workspace(receipt, Path(args.workspace)) if args.workspace else {"verdict": "ng", "status": "workspace_required", "problems": ["--workspace is required for reset"]}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())

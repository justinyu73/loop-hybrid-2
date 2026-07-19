#!/usr/bin/env python3
"""Deterministic reducer and disposable-workspace cleanup canary."""
from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "execution_receipt"))
import execution_receipt as er
import verification_reducer as vr


def receipt(attempt: int = 1, *, passing: bool = False) -> dict:
    h = lambda char: "sha256:" + char * 64
    verification = {name: {"exit_code": 0, "evidence_ref": f"remote://run/{name}"} for name in er.CHECKS}
    if not passing:
        verification["targeted"]["exit_code"] = 1
    return {
        "schema": er.RECEIPT_SCHEMA, "run_id": "run-001", "goal_digest": h("a"), "attempt": attempt,
        "workspace": {"ref": "lh://workspace/run-001", "base_revision": h("b"), "disposable": True},
        "provider": {"profile": "lh-provider", "response_digest": h("c"), "duration_ms": 1},
        "trajectory": {"ref": "remote://run/trajectory", "digest": h("d")}, "diff_digest": h("e"),
        "commands": [{"id": "test", "argv_digest": h("f"), "exit_code": verification["targeted"]["exit_code"], "duration_ms": 1, "output_ref": "remote://run/test"}],
        "verification": verification,
        "failure": None if passing else {"classification": "unattributed", "fingerprint": "targeted:exit-1", "excerpt_ref": "remote://run/stderr"},
    }


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    retry = vr.reduce(receipt())
    passed = vr.reduce(receipt(passing=True))
    stopped = vr.reduce(receipt(4))
    no_summary = receipt(); no_summary["failure"] = None
    missing_summary = vr.reduce(no_summary)
    with tempfile.TemporaryDirectory() as raw:
        workspace = Path(raw) / "workspace"; workspace.mkdir()
        subprocess.run(["git", "init", "-q", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "config", "user.email", "canary@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(workspace), "config", "user.name", "Canary"], check=True)
        (workspace / "tracked.txt").write_text("baseline\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(workspace), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(workspace), "commit", "-qm", "baseline"], check=True)
        cleanup_receipt = receipt()
        marker = {"schema": vr.MARKER_SCHEMA, "run_id": cleanup_receipt["run_id"], "workspace_ref": cleanup_receipt["workspace"]["ref"], "base_revision": cleanup_receipt["workspace"]["base_revision"]}
        (workspace / ".git" / vr.MARKER_NAME).write_text(json.dumps(marker), encoding="utf-8")
        (workspace / "tracked.txt").write_text("changed\n", encoding="utf-8")
        (workspace / "untracked.txt").write_text("remove\n", encoding="utf-8")
        reset = vr.reset_disposable_workspace(cleanup_receipt, workspace)
        restored = (workspace / "tracked.txt").read_text(encoding="utf-8") == "baseline\n" and not (workspace / "untracked.txt").exists()
        no_marker_workspace = Path(raw) / "unmarked"; no_marker_workspace.mkdir()
        subprocess.run(["git", "init", "-q", str(no_marker_workspace)], check=True)
        no_marker = vr.reset_disposable_workspace(cleanup_receipt, no_marker_workspace)
    cases = [
        case("failed-verifier-retries-before-budget", retry.get("route") == "retry" and retry.get("cleanup_required") is True, retry.get("status", "")),
        case("all-verifiers-pass", passed.get("route") == "pass", passed.get("status", "")),
        case("fourth-failure-stops", stopped.get("route") == "stop", stopped.get("status", "")),
        case("failed-verifier-needs-summary", missing_summary.get("verdict") == "ng", missing_summary.get("status", "")),
        case("marked-disposable-workspace-is-restored", reset.get("status") == "workspace_reset" and restored, reset.get("status", "")),
        case("unmarked-workspace-is-refused", no_marker.get("status") == "workspace_marker_invalid", no_marker.get("status", "")),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "verification-reducer-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["LH supplies the disposable-workspace marker and executes the next model attempt; this reducer only derives the route and verifies local cleanup." ]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

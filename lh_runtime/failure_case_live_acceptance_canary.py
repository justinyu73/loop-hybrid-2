#!/usr/bin/env python3
"""Offline proof for the bounded FailureCase live-acceptance command."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

from _fixture import make_source_repo
from failure_case_live_acceptance import EXPECTED_EVENTS, run_acceptance
import failure_case_live_acceptance as live_module
from p7_native_runstore_fixture import explicit_runstore_factory


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        args = {
            "project_id": "fixture",
            "acceptance_id": "fc-p0-canary",
            "source_repo": source,
            "base_revision": base,
            "goal_store_root": root / "acceptance" / "goals",
            "run_store_root": root / "project" / "runs",
            "workspace_root": root / "acceptance" / "workspaces",
            "evidence_out": root / "acceptance" / "evidence.json",
            "marker_path": "src/.fc-p0-marker",
            "allowed_path": "src/",
        }
        with explicit_runstore_factory(live_module):
            first = run_acceptance(**args)
        first_bytes = args["evidence_out"].read_bytes()
        second = run_acceptance(**args)
        second_bytes = args["evidence_out"].read_bytes()
        source_untouched = not (source / "src" / ".fc-p0-marker").exists()
        cases = [
            {
                "id": "live-shape-opens-grills-checks-closes",
                "ok": first["status"] == "pass"
                and first["mode"] == "execute"
                and first["run"]["attempts"] == 4
                and first["run"]["new_attempts"] == 4
                and first["event_types"] == EXPECTED_EVENTS
                and first["failure_case"]["state"] == "resolved"
                and first["failure_case"]["grill_generation"] == 2
                and first["failure_case"]["next_node_id"] == "campaign_completed",
                "detail": json.dumps({
                    "status": first["status"],
                    "run": first["run"],
                    "events": first["event_types"],
                    "case": first["failure_case"],
                }, sort_keys=True),
            },
            {
                "id": "replay-reuses-the-same-durable-chain",
                "ok": second["status"] == "pass"
                and second["mode"] == "replay"
                and second["run"]["run_id"] == first["run"]["run_id"]
                and second["failure_case"]["failure_case_id"] == first["failure_case"]["failure_case_id"]
                and second["run"]["new_attempts"] == 0
                and second["invocation"]["model_calls"] == 0
                and len(second["outbox"]) == len(first["outbox"])
                and first_bytes != second_bytes,
                "detail": json.dumps({
                    "mode": second["mode"],
                    "run": second["run"],
                    "model_calls": second["invocation"]["model_calls"],
                    "outbox_count": len(second["outbox"]),
                    "evidence_refreshed": first_bytes != second_bytes,
                }, sort_keys=True),
            },
            {
                "id": "source-repo-remains-read-only",
                "ok": source_untouched,
                "detail": json.dumps({"source_marker_exists": not source_untouched}),
            },
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-failure-case-live-acceptance",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {
            "command": "python3 -B lh_runtime/failure_case_live_acceptance_canary.py",
            "fixtures": "temporary local Git repository and SQLite stores only; no network",
        },
        "known_gaps_open": [],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

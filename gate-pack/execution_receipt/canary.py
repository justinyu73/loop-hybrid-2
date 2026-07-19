#!/usr/bin/env python3
"""Deterministic canary for compact execution receipts and retry capsules."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import execution_receipt as er


def receipt(attempt: int = 1) -> dict:
    h = lambda char: "sha256:" + char * 64
    return {
        "schema": er.RECEIPT_SCHEMA, "run_id": "run-001", "goal_digest": h("a"), "attempt": attempt,
        "workspace": {"ref": "lh://workspace/run-001", "base_revision": h("b"), "disposable": True},
        "provider": {"profile": "lh-provider", "response_digest": h("c"), "duration_ms": 42},
        "trajectory": {"ref": "remote://runs/run-001/attempt", "digest": h("d")}, "diff_digest": h("e"),
        "commands": [{"id": "test", "argv_digest": h("f"), "exit_code": 1, "duration_ms": 10, "output_ref": "remote://runs/run-001/test"}],
        "verification": {name: {"exit_code": 0 if name != "targeted" else 1, "evidence_ref": f"remote://runs/run-001/{name}"} for name in er.CHECKS},
        "failure": {"classification": "retryable", "fingerprint": "targeted:exit-1", "excerpt_ref": "remote://runs/run-001/stderr-excerpt"},
    }


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    valid = er.next_attempt_capsule(receipt())
    raw_stderr = receipt(); raw_stderr["stderr"] = "large transcript"
    raw_result = er.next_attempt_capsule(raw_stderr)
    persistent = receipt(); persistent["workspace"]["disposable"] = False
    persistent_result = er.next_attempt_capsule(persistent)
    exhausted = er.next_attempt_capsule(receipt(4))
    no_failure = receipt(); no_failure["failure"] = None
    no_failure_result = er.next_attempt_capsule(no_failure)
    capsule = valid.get("capsule") or {}
    cases = [
        case("compact-retry-capsule", valid["status"] == "next_attempt_ready" and capsule.get("next_attempt") == 2, valid["status"]),
        case("raw-stderr-is-not-a-receipt-field", raw_result["verdict"] == "ng", raw_result["status"]),
        case("workspace-must-be-disposable", persistent_result["verdict"] == "ng", persistent_result["status"]),
        case("fourth-failure-exhausts-budget", exhausted["status"] == "attempt_budget_exhausted" and exhausted["capsule"] is None, exhausted["status"]),
        case("retry-needs-bounded-failure-summary", no_failure_result["status"] == "failure_summary_required", no_failure_result["status"]),
        case("capsule-excludes-command-and-provider-details", "commands" not in capsule and "provider" not in capsule and "verification" not in capsule, "compact"),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "execution-receipt-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["Receipt references require an LH or remote run store; this contract does not operate that store."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

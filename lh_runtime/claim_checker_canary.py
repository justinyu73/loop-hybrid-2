#!/usr/bin/env python3
"""Flip-test the compact optimization claim checker."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
from claim_checker import check


def main() -> int:
    registry = json.loads(
        (ROOT / "docs" / "active" / "optimization-claims.json").read_text(
            encoding="utf-8"
        )
    )
    actual = check(ROOT, copy.deepcopy(registry))
    downgraded = copy.deepcopy(registry)
    next(
        item for item in downgraded["claims"] if item["id"] == "OPT-ROUTE-2"
    )["observed_proof"] = "offline_canary"
    duplicate = copy.deepcopy(registry)
    duplicate["claims"][1]["id"] = duplicate["claims"][0]["id"]
    missing = copy.deepcopy(registry)
    missing["claims"][0]["evidence_refs"] = ["missing-evidence"]
    cases = [
        {
            "id": "current-claims-pass",
            "ok": actual["status"] == "pass",
            "detail": json.dumps(actual["blocking_failures"], ensure_ascii=False),
        },
        {
            "id": "proof-downgrade-fails",
            "ok": check(ROOT, downgraded)["status"] == "fail",
            "detail": "bounded-live claim cannot close with an offline canary",
        },
        {
            "id": "duplicate-claim-fails",
            "ok": check(ROOT, duplicate)["status"] == "fail",
            "detail": "claim ids are unique",
        },
        {
            "id": "missing-evidence-fails",
            "ok": check(ROOT, missing)["status"] == "fail",
            "detail": "complete claim evidence must exist",
        },
    ]
    failures = [
        {"id": case["id"], "detail": case["detail"]}
        for case in cases
        if not case["ok"]
    ]
    print(json.dumps({
        "check_id": "lh-optimization-claim-checker",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/claim_checker_canary.py",
            "provider_invocations": 0,
        },
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Advancement verdict: an already-green check is not advancement.

Asking only whether an assigned check passes lets a successor that changed
nothing earn a genuine receipt and be counted as progress.  Here the
assignment fixes, before the work is judged, which receipts it will compare:
for every criterion a check id, a baseline receipt, and a closing receipt from
the progress-receipts ledger.  A criterion advances only when its baseline was
red and its closing receipt is green.  Receipt order elsewhere in the ledger
and checks the worker chose for itself are irrelevant.

Usage:
  advancement.py evaluate --ledger L --assignment assignment.json

assignment.json:
  {"schema": "lh-advancement-assignment/v1", "task": "T",
   "criteria": [{"id": "A", "check": "<check-id>",
                 "baseline_receipt": "sha256:...", "closing_receipt": "sha256:..."}]}
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "progress_receipts"))
import receipts  # noqa: E402

ASSIGNMENT_SCHEMA = "lh-advancement-assignment/v1"
CRITERION_FIELDS = {"id", "check", "baseline_receipt", "closing_receipt"}


def _refused(reason: str, **detail: Any) -> dict[str, Any]:
    return {"status": "refused", "reason": reason, **detail}


def evaluate(ledger: Path, assignment: dict[str, Any]) -> dict[str, Any]:
    criteria = assignment.get("criteria")
    if (assignment.get("schema") != ASSIGNMENT_SCHEMA or not isinstance(assignment.get("task"), str)
            or not isinstance(criteria, list) or not criteria
            or any(not isinstance(row, dict) or set(row) != CRITERION_FIELDS for row in criteria)):
        return _refused("assignment_invalid")
    rows = receipts._ledger(ledger)
    if not receipts._chain_intact(rows):
        return _refused("chain_broken")
    position = {row["receipt_digest"]: index for index, row in enumerate(rows)}
    results = []
    for criterion in criteria:
        found = []
        for key in ("baseline_receipt", "closing_receipt"):
            index = position.get(criterion[key])
            if index is None:
                return _refused("receipt_unknown", criterion=criterion["id"], receipt=criterion[key])
            row = rows[index]
            if row.get("task") != assignment["task"] or row.get("check") != criterion["check"]:
                return _refused("receipt_check_mismatch", criterion=criterion["id"], receipt=criterion[key])
            found.append((index, row))
        (base_index, baseline), (close_index, closing) = found
        if base_index >= close_index:
            return _refused("receipt_order_invalid", criterion=criterion["id"])
        if baseline.get("ok") is True:
            verdict = "already_green" if closing.get("ok") is True else "regressed"
        else:
            verdict = "advanced" if closing.get("ok") is True else "still_red"
        results.append({"id": criterion["id"], "check": criterion["check"], "verdict": verdict,
                        "baseline_commit": baseline.get("commit"), "closing_commit": closing.get("commit")})
    advanced = all(row["verdict"] == "advanced" for row in results)
    return {"status": "advanced" if advanced else "not_advanced", "task": assignment["task"], "criteria": results}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["evaluate"])
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--assignment", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        assignment = json.loads(args.assignment.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        assignment = {}
    result = evaluate(args.ledger, assignment if isinstance(assignment, dict) else {})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "advanced" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

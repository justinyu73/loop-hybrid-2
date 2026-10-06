#!/usr/bin/env python3
"""Independent, read-only verifier for bounded executor retries.

The proof that an executor failed, was retried, and stopped within its launch
budget should not be read back by the same code that wrote it.  This module
imports nothing from the engine's executor or store: it reads the
executor's digest-bound receipts as JSON and the work-unit store through a
read-only SQLite connection, and checks that the two tell one consistent story.

Usage:
  verify_retry.py --executor-root DIR --queue-db work-units.sqlite3 \
                  --dispatch-key KEY --expected retry_success|exhausted|unknown
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Mapping

INVOCATION_SCHEMA = "lh-command-invocation/v1"
FAILURE_SCHEMA = "lh-successor-executor-failure/v1"
RECOVERY_SCHEMA = "lh-successor-executor-recovery/v1"
MAX_LAUNCHES = 3


class VerificationError(ValueError):
    pass


def digest_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"unreadable:{path.name}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"not_object:{path.name}")
    return value


def _read_bound(path: Path, field: str) -> dict[str, Any]:
    value = _read_json(path)
    body = copy.deepcopy(value)
    supplied = body.pop(field, None)
    if not isinstance(supplied, str) or supplied != digest_json(body):
        raise VerificationError(f"digest_mismatch:{path.name}")
    return value


def _matching(directory: Path, suffix: str, dispatch_key: str) -> list[Path]:
    return sorted(path for path in directory.glob(f"*{suffix}")
                  if _read_json(path).get("dispatch_key") == dispatch_key)


def _positive(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _verify_invocation(path: Path, dispatch_key: str) -> dict[str, Any]:
    value = _read_bound(path, "invocation_digest")
    if (value.get("schema") != INVOCATION_SCHEMA or value.get("status") != "reserved"
            or value.get("dispatch_key") != dispatch_key
            or not all(_positive(value.get(field)) for field in ("attempt", "fence", "goal_revision"))):
        raise VerificationError("invocation_binding_invalid")
    return value


def _verify_linked(path: Path, field: str, invocations: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    value = _read_bound(path, field)
    invocation = invocations.get(value.get("invocation_digest"))
    if invocation is None:
        raise VerificationError(f"{field.split('_')[0]}_invocation_missing")
    linked = value.get("invocation_path")
    if not isinstance(linked, str) or _read_bound(Path(linked), "invocation_digest").get(
            "invocation_digest") != value.get("invocation_digest"):
        raise VerificationError(f"{field.split('_')[0]}_invocation_path_mismatch")
    for name in ("attempt", "fence", "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "packet_digest"):
        if name in value and value.get(name) != invocation.get(name):
            raise VerificationError(f"{field.split('_')[0]}_{name}_mismatch")
    return value


def _verify_failure(path: Path, invocations: Mapping[str, Mapping[str, Any]], dispatch_key: str) -> dict[str, Any]:
    value = _verify_linked(path, "failure_receipt_digest", invocations)
    if (value.get("schema") != FAILURE_SCHEMA or value.get("status") != "failed"
            or value.get("outcome") != "known_failure" or value.get("retryable") is not True
            or value.get("dispatch_key") != dispatch_key):
        raise VerificationError("failure_receipt_binding_invalid")
    proof = value.get("timeout_and_termination_proof")
    if not isinstance(proof, Mapping) or proof.get("process_terminated") is not True:
        raise VerificationError("failure_termination_unproven")
    if value.get("workspace_effects_reconciled") is not True or not value.get("workspace_diff_digest"):
        raise VerificationError("failure_workspace_reconciliation_missing")
    return value


def _verify_recovery(path: Path, invocations: Mapping[str, Mapping[str, Any]], dispatch_key: str) -> dict[str, Any]:
    value = _verify_linked(path, "recovery_receipt_digest", invocations)
    if (value.get("schema") != RECOVERY_SCHEMA or value.get("status") != "reconciled"
            or value.get("outcome") != "unknown" or value.get("dispatch_key") != dispatch_key):
        raise VerificationError("recovery_receipt_binding_invalid")
    return value


def _row_json(row: sqlite3.Row, field: str) -> dict[str, Any]:
    try:
        value = json.loads(row[field])
    except (TypeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"queue_{field}_invalid") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"queue_{field}_not_object")
    return value


def verify_retry(*, executor_root: Path, queue_db: Path, dispatch_key: str, expected: str) -> dict[str, Any]:
    directory = executor_root.resolve()
    if directory.name != "successor-executor":
        directory = directory / "successor-executor"
    if not directory.is_dir():
        raise VerificationError("executor_evidence_directory_missing")
    invocations = [_verify_invocation(path, dispatch_key) for path in _matching(directory, ".invocation.json", dispatch_key)]
    if not invocations:
        raise VerificationError("invocation_missing")
    if len({value["attempt"] for value in invocations}) != len(invocations):
        raise VerificationError("duplicate_invocation_attempt")
    if len(invocations) > MAX_LAUNCHES:
        raise VerificationError("launch_budget_exceeded")
    by_digest = {value["invocation_digest"]: value for value in invocations}
    failures = [_verify_failure(path, by_digest, dispatch_key)
                for path in _matching(directory, ".failure-receipt.json", dispatch_key)]
    recoveries = [_verify_recovery(path, by_digest, dispatch_key)
                  for path in _matching(directory, ".recovery-receipt.json", dispatch_key)]
    accepted = [_read_bound(path, "receipt_digest")
                for path in _matching(directory, ".executor-receipt.json", dispatch_key)]
    if not queue_db.is_file():
        raise VerificationError("queue_db_missing")
    connection = sqlite3.connect(f"file:{queue_db.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        dispatches = connection.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?",
                                        (dispatch_key,)).fetchall()
        if len(dispatches) != 1:
            raise VerificationError("dispatch_cardinality_invalid")
        dispatch = dispatches[0]
        receipt = _row_json(dispatch, "receipt_json")
        body = copy.deepcopy(receipt)
        if body.pop("receipt_digest", None) != digest_json(body) or receipt.get("receipt_digest") != dispatch["receipt_digest"]:
            raise VerificationError("dispatch_receipt_digest_invalid")
        runs = connection.execute("SELECT * FROM runs WHERE run_id = ?", (dispatch["run_id"],)).fetchall()
        attempts = connection.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal",
                                      (dispatch["run_id"],)).fetchall()
        units = connection.execute("SELECT * FROM work_units WHERE work_unit_id = ?",
                                   (dispatch["work_unit_id"],)).fetchall()
    finally:
        connection.close()
    if len(runs) != 1 or len(units) != 1:
        raise VerificationError("run_or_work_unit_cardinality_invalid")
    if len(attempts) != len(invocations):
        raise VerificationError("attempt_invocation_cardinality_mismatch")
    history = receipt.get("executor_failure_receipts", [])
    if [item.get("failure_receipt_digest") for item in history if isinstance(item, Mapping)] != [
            item["failure_receipt_digest"] for item in sorted(failures, key=lambda item: item["attempt"])]:
        raise VerificationError("failure_history_mismatch")
    if receipt.get("executor_launches", len(invocations)) != len(invocations):
        raise VerificationError("launch_count_mismatch")
    latest = attempts[-1]
    if int(dispatch["attempt"]) != int(latest["ordinal"]):
        raise VerificationError("latest_attempt_mismatch")
    run_state, unit_state = runs[0]["state"], units[0]["state"]
    earlier_interrupted = all(row["state"] == "interrupted" for row in attempts[:-1])
    if expected == "retry_success":
        if (receipt.get("executor_status") != "accepted" or len(accepted) != 1
                or len(failures) != len(invocations) - 1 or not earlier_interrupted
                or latest["state"] not in {"running", "integrated"} or run_state != latest["state"]
                or unit_state != latest["state"]):
            raise VerificationError("retry_success_state_invalid")
    elif expected == "exhausted":
        if (receipt.get("executor_status") != "exhausted" or receipt.get("retry_exhausted") is not True
                or latest["state"] != "stopped" or run_state != "stopped" or unit_state != "stopped"
                or len(failures) != MAX_LAUNCHES or accepted):
            raise VerificationError("retry_exhaustion_state_invalid")
    elif expected == "unknown":
        if (receipt.get("executor_status") != "unknown" or len(recoveries) != 1 or failures or accepted
                or run_state not in {"queued", "ready"}):
            raise VerificationError("unknown_recovery_state_invalid")
    else:
        raise VerificationError("expected_status_invalid")
    return {"status": "PASS", "expected": expected, "dispatch_key": dispatch_key, "launches": len(invocations),
            "failures": len(failures), "recoveries": len(recoveries), "attempt_states": [row["state"] for row in attempts]}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--executor-root", type=Path, required=True)
    parser.add_argument("--queue-db", type=Path, required=True)
    parser.add_argument("--dispatch-key", required=True)
    parser.add_argument("--expected", required=True, choices=["retry_success", "exhausted", "unknown"])
    args = parser.parse_args(argv)
    try:
        result = verify_retry(executor_root=args.executor_root, queue_db=args.queue_db,
                              dispatch_key=args.dispatch_key, expected=args.expected)
    except VerificationError as exc:
        result = {"status": "FAIL", "reason": str(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

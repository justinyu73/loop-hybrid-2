#!/usr/bin/env python3
"""Compact receipt and retry-context contract for LH execution attempts."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

RECEIPT_SCHEMA = "lh-execution-receipt/v1"
CAPSULE_SCHEMA = "lh-next-attempt-capsule/v1"
HEX_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
MAX_ATTEMPTS = 4
CHECKS = {"static", "targeted", "core_regression", "negative"}
FAILURE_CLASSES = {"retryable", "unattributed", "terminal"}


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _digest(value: Any) -> bool:
    return isinstance(value, str) and bool(HEX_DIGEST.fullmatch(value))


def _ref(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip()) and "://" in value


def validate_receipt(receipt: Any) -> list[str]:
    if not isinstance(receipt, dict):
        return ["receipt must be an object"]
    required = {"schema", "run_id", "goal_digest", "attempt", "workspace", "provider", "trajectory", "diff_digest", "commands", "verification", "failure"}
    problems = [] if set(receipt) == required else ["receipt fields are invalid"]
    if receipt.get("schema") != RECEIPT_SCHEMA:
        problems.append(f"receipt.schema must be {RECEIPT_SCHEMA}")
    for field in ("run_id",):
        if not isinstance(receipt.get(field), str) or not receipt[field].strip():
            problems.append(f"{field} must be non-empty")
    if not _digest(receipt.get("goal_digest")):
        problems.append("goal_digest must be sha256")
    if not isinstance(receipt.get("attempt"), int) or isinstance(receipt.get("attempt"), bool) or not 1 <= receipt.get("attempt", 0) <= MAX_ATTEMPTS:
        problems.append(f"attempt must be an integer from 1 to {MAX_ATTEMPTS}")
    workspace = receipt.get("workspace")
    if not isinstance(workspace, dict) or set(workspace) != {"ref", "base_revision", "disposable"} or not _ref(workspace.get("ref")) or not _digest(workspace.get("base_revision")) or workspace.get("disposable") is not True:
        problems.append("workspace must bind an external disposable base revision")
    provider = receipt.get("provider")
    if not isinstance(provider, dict) or set(provider) != {"profile", "response_digest", "duration_ms"} or not isinstance(provider.get("profile"), str) or not provider["profile"] or not _digest(provider.get("response_digest")) or not isinstance(provider.get("duration_ms"), int) or provider["duration_ms"] < 0:
        problems.append("provider summary is invalid")
    trajectory = receipt.get("trajectory")
    if not isinstance(trajectory, dict) or set(trajectory) != {"ref", "digest"} or not _ref(trajectory.get("ref")) or not _digest(trajectory.get("digest")):
        problems.append("trajectory must be an external digest-bound reference")
    if not _digest(receipt.get("diff_digest")):
        problems.append("diff_digest must be sha256")
    commands = receipt.get("commands")
    if not isinstance(commands, list) or not commands:
        problems.append("commands must be a non-empty array")
    else:
        for index, command in enumerate(commands):
            if not isinstance(command, dict) or set(command) != {"id", "argv_digest", "exit_code", "duration_ms", "output_ref"} or not isinstance(command.get("id"), str) or not command["id"] or not _digest(command.get("argv_digest")) or not isinstance(command.get("exit_code"), int) or not isinstance(command.get("duration_ms"), int) or command["duration_ms"] < 0 or not _ref(command.get("output_ref")):
                problems.append(f"commands[{index}] is invalid")
    verification = receipt.get("verification")
    if not isinstance(verification, dict) or set(verification) != CHECKS:
        problems.append("verification must contain static, targeted, core_regression, and negative")
    elif any(not isinstance(value, dict) or set(value) != {"exit_code", "evidence_ref"} or not isinstance(value.get("exit_code"), int) or not _ref(value.get("evidence_ref")) for value in verification.values()):
        problems.append("verification entries are invalid")
    failure = receipt.get("failure")
    if failure is not None:
        if not isinstance(failure, dict) or set(failure) != {"classification", "fingerprint", "excerpt_ref"} or failure.get("classification") not in FAILURE_CLASSES or not isinstance(failure.get("fingerprint"), str) or not failure["fingerprint"] or not _ref(failure.get("excerpt_ref")):
            problems.append("failure summary is invalid")
    return problems


def next_attempt_capsule(receipt: dict[str, Any]) -> dict[str, Any]:
    problems = validate_receipt(receipt)
    if problems:
        return {"verdict": "ng", "status": "invalid_execution_receipt", "problems": problems}
    if receipt["failure"] is None:
        return {"verdict": "ng", "status": "failure_summary_required", "problems": ["a next attempt requires a failure summary"]}
    if receipt["attempt"] >= MAX_ATTEMPTS:
        return {"verdict": "pass", "status": "attempt_budget_exhausted", "capsule": None, "problems": []}
    capsule = {
        "schema": CAPSULE_SCHEMA,
        "run_id": receipt["run_id"],
        "goal_digest": receipt["goal_digest"],
        "next_attempt": receipt["attempt"] + 1,
        "workspace": receipt["workspace"],
        "diff_digest": receipt["diff_digest"],
        "failure": receipt["failure"],
        "trajectory": receipt["trajectory"],
    }
    return {"verdict": "pass", "status": "next_attempt_ready", "capsule": capsule, "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an LH execution receipt and form a compact retry capsule")
    parser.add_argument("receipt")
    args = parser.parse_args()
    try:
        receipt = json.loads(Path(args.receipt).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"verdict": "ng", "status": "invalid_execution_receipt", "problems": [str(exc)]}, ensure_ascii=False, indent=2))
        return 2
    result = next_attempt_capsule(receipt)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())

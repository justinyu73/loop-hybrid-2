#!/usr/bin/env python3
"""Value reducer — the deterministic 报红 (value-RED) overlay,
model-free value verdict over a run's receipt.

LH's verifier answers "did the acceptance lamp exit 0?". That is necessary but
gameable: an empty diff, or edits outside the allowed scope, can still exit 0.
This reducer asks the value question — is the pass real? — and DERIVES a
GREEN/RED verdict by code (never self-asserted). Missing evidence is RED, never
a fabricated pass (unknown != pass). It is finding-only: it records/exposes the
verdict and does NOT change the loop's advance decision (LH execution model
unchanged). Making the verdict an acceptance authority is a separate node.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

VERDICT_SCHEMA = "loop-hybrid-value-verdict/v1"
_DIFF_FILE_RE = re.compile(r"^diff --git a/.+? b/(.+)$", re.MULTILINE)


def touched_files(diff_text: str | None) -> list[str]:
    if not diff_text:
        return []
    return sorted({match.group(1) for match in _DIFF_FILE_RE.finditer(diff_text)})


def _in_scope(path: str, allowed_paths: list[str]) -> bool:
    for allowed in allowed_paths:
        if path == allowed or path.startswith(allowed.rstrip("/") + "/"):
            return True
    return False


def value_verdict(*, exit_code: Any, diff_text: str | None, allowed_paths: list[str]) -> dict[str, Any]:
    """Derive a value verdict from receipt evidence. Pure and deterministic."""
    reasons: list[str] = []
    if not isinstance(exit_code, int) or exit_code != 0:
        reasons.append(f"verifier lamp did not pass (exit_code={exit_code!r})")
    touched = touched_files(diff_text)
    if diff_text is None:
        reasons.append("no diff evidence recorded; a pass cannot be confirmed (unknown != pass)")
    elif not touched:
        reasons.append("empty diff: the run changed nothing but the lamp passed (possible lamp gaming)")
    if not allowed_paths:
        reasons.append("admission allowlist is empty; change scope cannot be verified")
    else:
        outside = [path for path in touched if not _in_scope(path, allowed_paths)]
        if outside:
            reasons.append(f"changed files outside the allowed scope: {outside}")
    return {
        "schema": VERDICT_SCHEMA,
        "verdict": "GREEN" if not reasons else "RED",
        "reasons": reasons,
        "evidence": {"exit_code": exit_code, "files_touched": touched, "allowed_paths": list(allowed_paths)},
    }


def _allowed_paths(run_store: Any, run_id: str) -> list[str]:
    try:
        goal = run_store.get_run(run_id).get("goal") or {}
        envelope = goal.get("admission_envelope") or {}
        allowed = envelope.get("allowed_paths")
        return allowed if isinstance(allowed, list) else []
    except (KeyError, ValueError, TypeError):
        return []


def verdict_for_run(run_store: Any, run_id: str) -> dict[str, Any]:
    allowed_paths = _allowed_paths(run_store, run_id)
    latest = run_store.latest_receipt(run_id)
    if not latest or not latest.get("receipt_ref"):
        return value_verdict(exit_code=None, diff_text=None, allowed_paths=allowed_paths)
    try:
        receipt = json.loads((run_store.root / latest["receipt_ref"]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return value_verdict(exit_code=None, diff_text=None, allowed_paths=allowed_paths)
    exit_code = (receipt.get("verification") or {}).get("exit_code")
    diff_text: str | None = None
    diff_ref = receipt.get("diff")
    # The controller stores diff as {"ref","digest"}; older/canary shapes use a
    # plain ref string. Accept both.
    ref_path = diff_ref.get("ref") if isinstance(diff_ref, dict) else diff_ref if isinstance(diff_ref, str) else None
    if ref_path:
        try:
            diff_text = (run_store.root / ref_path).read_text(encoding="utf-8")
        except OSError:
            diff_text = None
    return value_verdict(exit_code=exit_code, diff_text=diff_text, allowed_paths=allowed_paths)


def aggregate(run_store: Any) -> dict[str, Any]:
    """Per-run value rollup (latest receipt each). Finding-only; RED runs are listed."""
    run_ids = sorted({record["run_id"] for record in run_store.usage_records()})
    verdicts = [(run_id, verdict_for_run(run_store, run_id)) for run_id in run_ids]
    green = sum(1 for _run_id, verdict in verdicts if verdict["verdict"] == "GREEN")
    red = sum(1 for _run_id, verdict in verdicts if verdict["verdict"] == "RED")
    return {
        "schema": "loop-hybrid-value-rollup/v1",
        "total": len(verdicts),
        "green": green,
        "red": red,
        "red_runs": [{"run_id": run_id, "reasons": verdict["reasons"]} for run_id, verdict in verdicts if verdict["verdict"] == "RED"],
    }

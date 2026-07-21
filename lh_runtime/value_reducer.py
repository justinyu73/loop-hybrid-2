#!/usr/bin/env python3
"""Value reducer — the deterministic 报红 (value-RED) overlay,
model-free value verdict over a run's receipt.

LH's verifier answers "did the acceptance lamp exit 0?". That is necessary but
gameable: an empty diff, or edits outside the allowed scope, can still exit 0.
This reducer asks the value question — is the pass real? — and DERIVES a
GREEN/RED verdict by code (never self-asserted). Missing evidence is RED, never
a fabricated pass (unknown != pass). The worker's value gate consumes this
verdict: a lamp-passing but value-RED run does NOT advance — the goal routes
to human_required (报红 gates completion). The aggregate rollup remains
finding-only for read surfaces (MCP, status snapshots).
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

VERDICT_SCHEMA = "loop-hybrid-value-verdict/v1"
_DIFF_FILE_RE = re.compile(r"^diff --git a/.+? b/(.+)$", re.MULTILINE)

# W8-2: system-level error signals only — a lamp that exits 0 while its stderr
# carries one of these swallowed an environment error through shell logic.
# Deliberately conservative: bare "error"/"failed"/"warning" would hit linters
# printing "0 errors". Case-insensitive substring match.
STDERR_ERROR_SIGNALS = (
    "permission denied",
    "no such file or directory",
    "cannot open",
    "command not found",
    "traceback (most recent call last)",
)


def touched_files(diff_text: str | None) -> list[str]:
    if not diff_text:
        return []
    return sorted({match.group(1) for match in _DIFF_FILE_RE.finditer(diff_text)})


def _in_scope(path: str, allowed_paths: list[str]) -> bool:
    for allowed in allowed_paths:
        if path == allowed or path.startswith(allowed.rstrip("/") + "/"):
            return True
    return False


def value_verdict(*, exit_code: Any, diff_text: str | None, allowed_paths: list[str], precheck: bool = False, stderr_text: str | None = None) -> dict[str, Any]:
    """Derive a value verdict from receipt evidence. Pure and deterministic."""
    reasons: list[str] = []
    if not isinstance(exit_code, int) or exit_code != 0:
        reasons.append(f"verifier lamp did not pass (exit_code={exit_code!r})")
    elif isinstance(stderr_text, str):
        # W8-2: exit 0 with a system-level error on stderr means the lamp
        # swallowed an environment error through shell logic.
        lowered = stderr_text.lower()
        for signal in STDERR_ERROR_SIGNALS:
            if signal in lowered:
                reasons.append(f"verifier exited 0 but stderr carries a system-level error signal: {signal!r}")
                break
    touched = touched_files(diff_text)
    if diff_text is None:
        reasons.append("no diff evidence recorded; a pass cannot be confirmed (unknown != pass)")
    elif not touched and not precheck:
        # A lamp precheck (verification.precheck) passes on the untouched base
        # BEFORE any model invocation — there is no agent that could game the
        # lamp, so an empty diff there means "already done", not lamp gaming.
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


def _lamp_argv(run_store: Any, run_id: str) -> list[str] | None:
    """The admission envelope's approved lamp argv (same goal source as
    ``_allowed_paths``). None when the envelope carries no lamp — async
    external-verdict runs and legacy fixtures skip the argv check."""
    try:
        goal = run_store.get_run(run_id).get("goal") or {}
        envelope = goal.get("admission_envelope") or {}
        lamp = envelope.get("acceptance_lamp")
        argv = lamp.get("verification_argv") if isinstance(lamp, dict) else None
        return argv if isinstance(argv, list) and argv else None
    except (KeyError, ValueError, TypeError):
        return None


def _ref_parts(value: Any) -> tuple[str | None, Any]:
    """The controller stores refs as {"ref","digest"}; older shapes use a
    plain ref string. Accept both, like the diff handling."""
    if isinstance(value, dict) and isinstance(value.get("ref"), str):
        return value["ref"], value.get("digest")
    if isinstance(value, str):
        return value, None
    return None, None


def _consistency_problems(run_store: Any, run_id: str, receipt: dict[str, Any], latest: dict[str, Any]) -> list[str]:
    """W8-3 source consistency: the lamp that ran must be the lamp that was
    approved, and every recorded artifact ref must resolve inside the run
    store and match its recorded digest. provider.json and other
    model-produced content are never evidence sources (invariant); their
    refs are checked for integrity only, never consulted for the verdict."""
    problems: list[str] = []
    verification = receipt.get("verification") if isinstance(receipt.get("verification"), dict) else {}
    expected_argv = _lamp_argv(run_store, run_id)
    if expected_argv is not None and verification.get("argv") != expected_argv:
        problems.append("receipt verification argv does not match the admission envelope lamp")
    provider = receipt.get("provider") if isinstance(receipt.get("provider"), dict) else {}
    refs = [
        ("receipt", latest.get("receipt_ref"), latest.get("receipt_digest")),
        ("provider artifact", *_ref_parts(provider.get("artifact"))),
        ("diff", *_ref_parts(receipt.get("diff"))),
        ("verifier stdout", *_ref_parts(verification.get("stdout"))),
        ("verifier stderr", *_ref_parts(verification.get("stderr"))),
    ]
    root = run_store.root.resolve()
    for label, ref, digest in refs:
        if not isinstance(ref, str) or not ref:
            continue
        target = (run_store.root / ref).resolve()
        if not target.is_relative_to(root):
            problems.append(f"{label} ref escapes the run store: {ref}")
            continue
        if not target.is_file():
            problems.append(f"{label} artifact is missing: {ref}")
            continue
        if isinstance(digest, str) and digest.startswith("sha256:"):
            actual = "sha256:" + hashlib.sha256(target.read_bytes()).hexdigest()
            if actual != digest:
                problems.append(f"{label} artifact digest mismatch: {ref}")
    return problems


def _read_ref_text(run_store: Any, value: Any) -> str | None:
    ref, _digest = _ref_parts(value)
    if not ref:
        return None
    try:
        return (run_store.root / ref).read_text(encoding="utf-8")
    except OSError:
        return None


def verdict_for_run(run_store: Any, run_id: str) -> dict[str, Any]:
    allowed_paths = _allowed_paths(run_store, run_id)
    latest = run_store.latest_receipt(run_id)
    if not latest or not latest.get("receipt_ref"):
        return value_verdict(exit_code=None, diff_text=None, allowed_paths=allowed_paths)
    try:
        receipt = json.loads((run_store.root / latest["receipt_ref"]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return value_verdict(exit_code=None, diff_text=None, allowed_paths=allowed_paths)
    verification = receipt.get("verification") if isinstance(receipt.get("verification"), dict) else {}
    exit_code = verification.get("exit_code")
    precheck = verification.get("precheck") is True
    diff_text = _read_ref_text(run_store, receipt.get("diff"))
    stderr_text = _read_ref_text(run_store, verification.get("stderr"))
    verdict = value_verdict(exit_code=exit_code, diff_text=diff_text, allowed_paths=allowed_paths, precheck=precheck, stderr_text=stderr_text)
    problems = _consistency_problems(run_store, run_id, receipt, latest)
    if problems:
        verdict["reasons"] = verdict["reasons"] + problems
        verdict["verdict"] = "RED"
    return verdict


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

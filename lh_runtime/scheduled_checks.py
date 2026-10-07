#!/usr/bin/env python3
"""Scheduled checks: re-run read-only integrity checks on this machine and leave a verdict.

CI grades committed bytes, which is not the question "are the seals on the
machine that runs the loop intact right now".  ``run`` re-runs a closed table of
read-only checks against the target repository and writes one verdict
(``lh-scheduled-checks/v1``) into the state root:

- ``contract_seal`` when the target carries ``docs/contracts/seal.json``;
- ``decision_registry`` when the target carries ``decisions/policy.json``
  (its ``verify`` covers both decision ledgers).

Each check is a subprocess of the engine's own ``gate-pack`` tool with a
timeout; nothing repairs and nothing writes into the target.  A pass within
``min_interval_seconds`` of the last one returns that verdict unchanged.

The verdict is ``green`` only when at least one check applies and every check
passes; a failing check is ``red``; a check that cannot run, or no applicable
check at all, is ``unknown``.  ``project`` turns the file into the snapshot
field the status lamp reads; when the checks are enabled, a red, unknown,
missing, unreadable or stale verdict lights ``integrity_check_red``.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ENGINE_ROOT = HERE.parent
SCHEMA = "lh-scheduled-checks/v1"
STATE_FILE = "scheduled-checks.json"
CHECK_TIMEOUT_SECONDS = 120
DEFAULT_MIN_INTERVAL_SECONDS = 3600.0
DEFAULT_MAX_AGE_SECONDS = 3 * 3600.0
# The closed table: name -> (tool under the engine root, marker that makes it apply to a target).
CHECKS = (
    ("contract_seal", "gate-pack/contract_seal/seal.py", "docs/contracts/seal.json"),
    ("decision_registry", "gate-pack/decision_registry/registry.py", "decisions/policy.json"),
)


def validate_config(raw: Any) -> dict[str, float]:
    """The contract's ``scheduled_checks`` block: min_interval_seconds, max_age_seconds."""
    if not isinstance(raw, dict):
        raise ValueError("scheduled_checks must be an object")
    unknown = set(raw) - {"min_interval_seconds", "max_age_seconds"}
    if unknown:
        raise ValueError(f"scheduled_checks has unknown fields: {sorted(unknown)}")
    config = {"min_interval_seconds": raw.get("min_interval_seconds", DEFAULT_MIN_INTERVAL_SECONDS),
              "max_age_seconds": raw.get("max_age_seconds", DEFAULT_MAX_AGE_SECONDS)}
    for field, value in config.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"scheduled_checks.{field} must be a positive number")
        config[field] = float(value)
    if config["max_age_seconds"] < config["min_interval_seconds"]:
        raise ValueError("scheduled_checks.max_age_seconds must not be shorter than min_interval_seconds")
    return config


def state_path(goal_store_root: str | Path) -> Path:
    return Path(goal_store_root) / STATE_FILE


def _read(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and data.get("schema") == SCHEMA else None


def _head(target: Path) -> str | None:
    done = subprocess.run(["git", "-C", str(target), "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return done.stdout.strip() if done.returncode == 0 else None


def _run_check(name: str, tool: str, target: Path) -> dict[str, Any]:
    try:
        done = subprocess.run([sys.executable, "-B", str(ENGINE_ROOT / tool), "verify", "--root", str(target)],
                              capture_output=True, text=True, encoding="utf-8", errors="replace",
                              timeout=CHECK_TIMEOUT_SECONDS, stdin=subprocess.DEVNULL, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"check": name, "status": "error", "error": type(exc).__name__}
    return {"check": name, "status": "pass" if done.returncode == 0 else "fail", "exit": done.returncode,
            "tail": (done.stdout[-600:] if done.returncode else "")}


def run(target: str | Path, path: str | Path, *, min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS, now: float | None = None) -> dict[str, Any]:
    config = validate_config({"min_interval_seconds": min_interval_seconds, "max_age_seconds": max_age_seconds})
    now = time.time() if now is None else float(now)
    target, path = Path(target).resolve(), Path(path)
    previous = _read(path)
    if (previous is not None and isinstance(previous.get("checked_at"), (int, float))
            and 0 <= now - float(previous["checked_at"]) < config["min_interval_seconds"]):
        return previous
    results = [_run_check(name, tool, target) for name, tool, marker in CHECKS if (target / marker).is_file()]
    if any(row["status"] == "fail" for row in results):
        verdict = "red"
    elif results and all(row["status"] == "pass" for row in results):
        verdict = "green"
    else:
        verdict = "unknown"
    body = {"schema": SCHEMA, "checked_at": now, "target": str(target), "source_head": _head(target),
            "max_age_seconds": config["max_age_seconds"], "results": results, "verdict": verdict}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return body


def project(path: str | Path, *, enabled: dict[str, Any] | None, now: float | None = None) -> dict[str, Any] | None:
    """The snapshot field: None when not enabled and no verdict exists; otherwise what the lamp reads."""
    path = Path(path)
    if enabled is None and not path.exists():
        return None
    now = time.time() if now is None else float(now)
    data = _read(path)
    if data is None:
        return {"verdict": "missing" if not path.exists() else "unreadable", "stale": True, "age_seconds": None}
    max_age = float((enabled or {}).get("max_age_seconds") or data.get("max_age_seconds") or DEFAULT_MAX_AGE_SECONDS)
    checked = data.get("checked_at")
    age = now - float(checked) if isinstance(checked, (int, float)) else None
    return {"verdict": data.get("verdict"), "checked_at": checked, "age_seconds": None if age is None else round(age, 3),
            "max_age_seconds": max_age, "stale": age is None or age > max_age,
            "results": [{"check": row.get("check"), "status": row.get("status")} for row in data.get("results", [])]}


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Re-run read-only integrity checks and leave a verdict")
    parser.add_argument("--target", required=True)
    parser.add_argument("--goal-store", required=True, help="the verdict is written beside the goal store")
    parser.add_argument("--min-interval-seconds", type=float, default=DEFAULT_MIN_INTERVAL_SECONDS)
    parser.add_argument("--max-age-seconds", type=float, default=DEFAULT_MAX_AGE_SECONDS)
    args = parser.parse_args(argv)
    result = run(args.target, state_path(args.goal_store), min_interval_seconds=args.min_interval_seconds,
                 max_age_seconds=args.max_age_seconds)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] == "green" else 1


if __name__ == "__main__":
    raise SystemExit(main())

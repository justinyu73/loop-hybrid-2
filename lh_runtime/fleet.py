#!/usr/bin/env python3
"""Fleet: one wake-up walks a registry of projects, each in its own bounded session.

The registry (``lh-fleet-registry/v1``) lists each project's contract and its
desired state, ``enabled`` or ``paused``.  On every wake-up the fleet visits
the projects in order:

- a ``paused`` project is not woken;
- an ``enabled`` project runs one bounded session.  By default that is a
  ``goal_loop_run.py --contract <path> --execute --max-cycles N`` subprocess,
  so the project keeps its own contract, stores, singleton lock and receipts;
- a project that fails is recorded as failed and the next project still runs;
- a project whose lock is held elsewhere reports the driver's ``not_holder``.

The output (``lh-fleet-wake/v1``) gives every project's status and stop
reason.  The fleet holds no state of its own and installs nothing: an external
scheduler (cron, a task scheduler, a CI timer) is what wakes it.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

HERE = Path(__file__).resolve().parent
GOAL_LOOP = HERE / "goal_loop_run.py"
REGISTRY_SCHEMA = "lh-fleet-registry/v1"
WAKE_SCHEMA = "lh-fleet-wake/v1"
DESIRED_STATES = ("enabled", "paused")
DEFAULT_MAX_CYCLES = 30
PROJECT_FIELDS = {"project_id", "contract", "desired_state", "max_cycles", "max_runtime_seconds"}
Session = Callable[[dict[str, Any]], Mapping[str, Any]]
Runner = Callable[..., subprocess.CompletedProcess]


def _positive(value: Any, *, integer: bool) -> bool:
    if isinstance(value, bool):
        return False
    if integer:
        return isinstance(value, int) and value > 0
    return isinstance(value, (int, float)) and value > 0


def load_registry(path: str | Path) -> dict[str, Any]:
    """Read and validate a registry; contract paths resolve against the registry's folder."""
    registry_path = Path(path).resolve()
    try:
        body = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"fleet_registry_unreadable: {exc}") from None
    if not isinstance(body, dict) or set(body) != {"schema", "projects"} or body.get("schema") != REGISTRY_SCHEMA:
        raise ValueError("fleet_registry_schema_invalid")
    if not isinstance(body["projects"], list):
        raise ValueError("fleet_registry_projects_invalid")
    projects, seen = [], set()
    for index, row in enumerate(body["projects"]):
        where = f"projects[{index}]"
        if not isinstance(row, dict) or not {"project_id", "contract", "desired_state"} <= set(row):
            raise ValueError(f"fleet_registry_project_incomplete: {where}")
        unknown = set(row) - PROJECT_FIELDS
        if unknown:
            raise ValueError(f"fleet_registry_field_unknown: {where}: {sorted(unknown)}")
        project_id = row["project_id"]
        if not isinstance(project_id, str) or not project_id.strip():
            raise ValueError(f"fleet_registry_project_id_invalid: {where}")
        if project_id in seen:
            raise ValueError(f"fleet_registry_project_duplicate: {project_id}")
        seen.add(project_id)
        if row["desired_state"] not in DESIRED_STATES:
            raise ValueError(f"fleet_registry_desired_state_invalid: {project_id}")
        if not isinstance(row["contract"], str) or not row["contract"]:
            raise ValueError(f"fleet_registry_contract_invalid: {project_id}")
        max_cycles = row.get("max_cycles", DEFAULT_MAX_CYCLES)
        if not _positive(max_cycles, integer=True):
            raise ValueError(f"fleet_registry_unbounded: {project_id}: max_cycles")
        if "max_runtime_seconds" in row and not _positive(row["max_runtime_seconds"], integer=False):
            raise ValueError(f"fleet_registry_unbounded: {project_id}: max_runtime_seconds")
        contract = Path(row["contract"])
        entry = {"project_id": project_id, "desired_state": row["desired_state"],
                 "contract": str(contract if contract.is_absolute() else (registry_path.parent / contract).resolve()),
                 "max_cycles": max_cycles}
        if "max_runtime_seconds" in row:
            entry["max_runtime_seconds"] = row["max_runtime_seconds"]
        projects.append(entry)
    return {"schema": REGISTRY_SCHEMA, "path": str(registry_path), "projects": projects}


def goal_loop_session(entry: Mapping[str, Any], *, runner: Runner = subprocess.run) -> dict[str, Any]:
    """Run one bounded goal_loop_run session for a project in its own process; return the driver summary."""
    argv = [sys.executable, "-B", str(GOAL_LOOP), "--contract", str(entry["contract"]), "--execute",
            "--max-cycles", str(int(entry.get("max_cycles", DEFAULT_MAX_CYCLES)))]
    if "max_runtime_seconds" in entry:
        argv += ["--max-runtime-seconds", str(entry["max_runtime_seconds"])]
    completed = runner(argv, capture_output=True, text=True, encoding="utf-8", check=False, stdin=subprocess.DEVNULL)
    if completed.returncode != 0:
        raise RuntimeError(f"goal_loop_run exited {completed.returncode}: {(completed.stderr or '')[-500:]}")
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, ValueError):
        raise RuntimeError("goal_loop_run output is not JSON") from None
    driver = payload.get("driver") if isinstance(payload, dict) else None
    if not isinstance(driver, dict):
        raise RuntimeError("goal_loop_run output has no driver summary")
    return driver


def wake(registry: str | Path | Mapping[str, Any], *, session: Session | None = None,
         only: str | None = None) -> dict[str, Any]:
    """Visit every project once.  One project's failure never stops the others."""
    loaded = load_registry(registry) if isinstance(registry, (str, Path)) else dict(registry)
    run_session = session or goal_loop_session
    rows = []
    for entry in loaded["projects"]:
        if only is not None and entry["project_id"] != only:
            continue
        row: dict[str, Any] = {"project_id": entry["project_id"], "desired_state": entry["desired_state"],
                               "contract": entry["contract"]}
        if entry["desired_state"] == "paused":
            rows.append({**row, "status": "paused", "stop_reason": None, "runs_dispatched": 0})
            continue
        try:
            summary = run_session(dict(entry))
        except Exception as exc:  # isolate the project: record it and continue with the next one
            rows.append({**row, "status": "failed", "stop_reason": None, "runs_dispatched": 0,
                         "error": f"{type(exc).__name__}: {str(exc)[:500]}"})
            continue
        rows.append({**row, "status": "ran", "stop_reason": summary.get("stop_reason"),
                     "runs_dispatched": summary.get("runs_dispatched"), "cycles": summary.get("cycles")})
    return {"schema": WAKE_SCHEMA, "registry": loaded.get("path"), "projects": rows,
            "failed": [row["project_id"] for row in rows if row["status"] == "failed"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Wake every enabled project in a fleet registry once")
    parser.add_argument("--registry", required=True, help="lh-fleet-registry/v1 JSON file")
    parser.add_argument("--only", default=None, help="wake only this project id")
    args = parser.parse_args(argv)
    try:
        result = wake(args.registry, only=args.only)
    except ValueError as exc:
        print(json.dumps({"schema": WAKE_SCHEMA, "error": str(exc)}))
        return 2
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

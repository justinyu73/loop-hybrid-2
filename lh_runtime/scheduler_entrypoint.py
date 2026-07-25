#!/usr/bin/env python3
"""Production scheduler seam with one declared owner and durable collisions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Callable

import dispatch_envelope as dispatches

OWNER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
SCHEMA = "loop-hybrid-scheduler-event/v1"
HERE = pathlib.Path(__file__).resolve().parent
GOAL_LOOP = HERE / "goal_loop_run.py"
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _boot_id() -> str | None:
    try:
        return pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None


def classify(payload: dict[str, Any]) -> tuple[str, bool]:
    driver = payload.get("driver") if isinstance(payload.get("driver"), dict) else {}
    stop_reason = driver.get("stop_reason")
    dispatched = driver.get("runs_dispatched")
    if stop_reason == "not_holder":
        if dispatched not in {0, None}:
            return "invalid_collision", False
        return "collision_quarantined", True
    return "tick_completed", True


def append_event(path: pathlib.Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
    finally:
        os.close(descriptor)


def _receipt_bindings(
    goal_args: list[str],
    driver: dict[str, Any],
    dispatch: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Bind runs dispatched by this tick to their durable receipt bytes."""
    try:
        contract_index = goal_args.index("--contract") + 1
        contract_path = pathlib.Path(goal_args[contract_index]).resolve()
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        runtime = contract.get("runtime") if isinstance(contract.get("runtime"), dict) else {}
        run_store_ref = runtime.get("run_store")
        if not isinstance(run_store_ref, str):
            return []
        candidate = pathlib.Path(run_store_ref)
        run_store = candidate if candidate.is_absolute() else (contract_path.parent / candidate).resolve()
    except (IndexError, OSError, ValueError, json.JSONDecodeError):
        return []
    outcomes = driver.get("outcomes") if isinstance(driver.get("outcomes"), list) else []
    run_ids = sorted({
        str(row["run_id"])
        for row in outcomes
        if isinstance(row, dict) and isinstance(row.get("run_id"), str)
    })
    bindings: list[dict[str, Any]] = []
    for run_id in run_ids:
        receipts = sorted(
            (run_store / "artifacts" / run_id).glob("*/receipt.json"),
            key=lambda path: int(path.parent.name) if path.parent.name.isdigit() else -1,
        )
        if not receipts:
            continue
        receipt_path = receipts[-1]
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            digest = "sha256:" + hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        except (OSError, json.JSONDecodeError):
            continue
        if receipt.get("run_id") != run_id:
            continue
        receipt_dispatch = receipt.get("dispatch") if isinstance(receipt.get("dispatch"), dict) else {}
        dispatch_bound = (
            dispatch is not None
            and receipt_dispatch.get("dispatch_id") == dispatch.get("dispatch_id")
            and receipt_dispatch.get("envelope_digest") == dispatch.get("envelope_digest")
        )
        bindings.append({
            "run_id": run_id,
            "attempt": receipt.get("attempt"),
            "receipt": str(receipt_path),
            "receipt_digest": digest,
            "verification_exit_code": (
                receipt.get("verification", {}).get("exit_code")
                if isinstance(receipt.get("verification"), dict)
                else None
            ),
            "dispatch_bound": dispatch_bound,
        })
    return bindings


def execute(
    *,
    owner_id: str,
    event_log: pathlib.Path,
    goal_args: list[str],
    project_id: str | None = None,
    dispatch_envelope: pathlib.Path | None = None,
    runner: Runner = subprocess.run,
) -> tuple[int, dict[str, Any]]:
    if not OWNER_RE.fullmatch(owner_id):
        raise ValueError("owner_id must be a stable lowercase identifier")
    if project_id is not None and not OWNER_RE.fullmatch(project_id):
        raise ValueError("project_id must be a stable lowercase identifier")
    declared = os.environ.get("LH_SCHEDULER_OWNER_ID")
    if declared is not None and declared != owner_id:
        raise ValueError("owner_id does not match LH_SCHEDULER_OWNER_ID")
    dispatch: dict[str, Any] | None = None
    effective_goal_args = list(goal_args)
    if project_id is not None:
        if dispatch_envelope is None:
            raise ValueError("project scheduler invocation requires --dispatch-envelope")
        if "--dispatch-envelope" in effective_goal_args:
            raise ValueError("dispatch envelope may only be supplied at the scheduler boundary")
        try:
            contract_index = effective_goal_args.index("--contract") + 1
            contract_path = pathlib.Path(effective_goal_args[contract_index]).resolve()
        except (IndexError, ValueError):
            raise ValueError("project scheduler invocation requires --contract") from None
        dispatch = dispatches.load_and_validate(
            dispatch_envelope,
            project_id=project_id,
            owner_id=owner_id,
            contract_path=contract_path,
        )
        effective_goal_args.extend(["--dispatch-envelope", str(dispatch_envelope.resolve())])
    completed = runner(
        [sys.executable, "-B", str(GOAL_LOOP), *effective_goal_args],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    outcome, valid = classify(payload)
    driver = payload.get("driver") if isinstance(payload.get("driver"), dict) else {}
    receipt_bindings = _receipt_bindings(goal_args, driver, dispatch)
    dispatched_run_ids = {
        str(row["run_id"])
        for row in (driver.get("outcomes") if isinstance(driver.get("outcomes"), list) else [])
        if isinstance(row, dict) and isinstance(row.get("run_id"), str)
    }
    if (
        dispatch is not None
        and
        completed.returncode == 0
        and int(driver.get("runs_dispatched") or 0) > 0
        and (
            len(receipt_bindings) != len(dispatched_run_ids)
            or any(not row["dispatch_bound"] for row in receipt_bindings)
        )
    ):
        outcome, valid = "dispatch_binding_invalid", False
    event = {
        "schema": SCHEMA,
        "observed_at": _utc_now(),
        "owner_id": owner_id,
        "project_id": project_id,
        "invocation_id": os.environ.get("INVOCATION_ID"),
        "boot_id": _boot_id(),
        "pid": os.getpid(),
        "outcome": outcome if completed.returncode == 0 else "entrypoint_failed",
        "exit_code": completed.returncode,
        "stop_reason": driver.get("stop_reason"),
        "cycles": driver.get("cycles"),
        "runs_dispatched": driver.get("runs_dispatched"),
        "receipt_bindings": receipt_bindings,
        "dispatch_id": dispatch.get("dispatch_id") if dispatch is not None else None,
        "dispatch_digest": dispatch.get("envelope_digest") if dispatch is not None else None,
    }
    append_event(event_log, event)
    public = {
        **payload,
        "scheduler_owner": {
            "owner_id": owner_id,
            "project_id": project_id,
            "event_log": str(event_log),
            "outcome": event["outcome"],
            "collision_quarantined": outcome == "collision_quarantined",
            "dispatch_id": event["dispatch_id"],
            "dispatch_digest": event["dispatch_digest"],
        },
    }
    if completed.returncode != 0:
        public.setdefault("entrypoint_error", completed.stderr[-1000:])
    return (completed.returncode if valid else 1), public


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if "--" not in values:
        raise SystemExit("scheduler_entrypoint requires -- before goal_loop_run arguments")
    boundary = values.index("--")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--project-id")
    parser.add_argument("--event-log", required=True, type=pathlib.Path)
    parser.add_argument("--dispatch-envelope", type=pathlib.Path)
    args = parser.parse_args(values[:boundary])
    goal_args = values[boundary + 1 :]
    if not goal_args:
        parser.error("goal_loop_run arguments are required")
    try:
        exit_code, result = execute(
            owner_id=args.owner_id,
            project_id=args.project_id,
            event_log=args.event_log,
            goal_args=goal_args,
            dispatch_envelope=args.dispatch_envelope,
        )
    except (OSError, ValueError) as exc:
        print(json.dumps({"schema": SCHEMA, "status": "invalid_owner", "reason": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

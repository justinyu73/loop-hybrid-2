#!/usr/bin/env python3
"""Materialize the unified project status to a durable JSON snapshot on disk.

`platform_report` prints the same status to stdout and never writes; this is the
write surface a lightweight platform / SH consumes cold. SH is a JSON pointer
layer that reads local files across projects — it cannot call LH's live MCP
resource — so LH materializes one snapshot it can read. This is a projection,
never an authority: LH's SQLite stores remain the source of truth.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore
from project_status import _wall_age_seconds, build_run_liveness, build_status
from run_store import RunStore

SCHEMA = "loop-hybrid-status-snapshot/v1"
HEARTBEAT_SCHEMA = "loop-hybrid-driver-heartbeat/v1"
DEFAULT_HEARTBEAT_FILENAME = "driver-heartbeat.json"
# Shared bounded default for one executor/attempt budget, in seconds.  Every
# entry point (run/build_worker/controller/CLI executor presets) imports this
# instead of hardcoding its own literal.
DEFAULT_EXECUTOR_TIMEOUT_SECONDS = 900.0
A2_ATTEMPT_PHASES = (
    "clone",
    "checkout",
    "executor",
    "git_add",
    "git_diff",
    "verifier",
    "provider_artifact",
    "diff_artifact",
    "verifier_stdout_artifact",
    "verifier_stderr_artifact",
    "receipt_artifact",
)


def default_heartbeat_path(run_store_root: str | Path) -> Path:
    """Return the durable heartbeat sidecar for one LH run store."""
    return Path(run_store_root) / DEFAULT_HEARTBEAT_FILENAME


def build_heartbeat(
    *,
    holder: str,
    monotonic_ts: float,
    wall_ts: str,
    phase: str,
    cycles: int,
) -> dict[str, Any]:
    """Build the small, process-owned liveness projection.

    A heartbeat records the last driver tick boundary.  It is deliberately
    not a watchdog signal: the controller's foreground thread cannot update
    it while an executor is blocking.
    """
    return {
        "schema": HEARTBEAT_SCHEMA,
        "holder": str(holder),
        "monotonic_ts": float(monotonic_ts),
        "wall_ts": str(wall_ts),
        "phase": str(phase),
        "cycles": int(cycles),
    }


def read_heartbeat(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) and value.get("schema") == HEARTBEAT_SCHEMA else None


def attempt_wall_clock_upper_bound_seconds(timeout_seconds: float) -> float:
    """Derive the A2 bound from the controller's bounded phases."""
    timeout = float(timeout_seconds)
    if timeout <= 0:
        raise ValueError("timeout_seconds must be positive")
    return timeout * len(A2_ATTEMPT_PHASES)


def derive_staleness_threshold_seconds(*, timeout_seconds: float, tick_overhead_seconds: float) -> float:
    """Make the stale threshold strictly larger than A2 plus driver overhead."""
    overhead = max(0.0, float(tick_overhead_seconds))
    base = attempt_wall_clock_upper_bound_seconds(timeout_seconds) + overhead
    # nextafter supplies strict `>` without smuggling in a magic epsilon.
    return math.nextafter(base, math.inf)


def build_snapshot(
    run_store: RunStore,
    goal_store: GoalStore,
    *,
    generated_at: str,
    pricing: dict[str, dict[str, float]] | None = None,
    heartbeat: dict[str, Any] | None = None,
    attempt_timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    tick_overhead_seconds: float = 0.0,
) -> dict[str, Any]:
    heartbeat = heartbeat if heartbeat is not None else read_heartbeat(default_heartbeat_path(run_store.root))
    attempt_bound = attempt_wall_clock_upper_bound_seconds(attempt_timeout_seconds)
    threshold = derive_staleness_threshold_seconds(
        timeout_seconds=attempt_timeout_seconds,
        tick_overhead_seconds=tick_overhead_seconds,
    )
    # B9: top-level driver staleness — a read-only projection of the same
    # threshold used by run_liveness.  A missing/unreadable heartbeat is
    # reported stale (unknown is not live); recovery semantics are unchanged.
    heartbeat_age = _wall_age_seconds(heartbeat)
    stale = heartbeat_age is None or heartbeat_age > threshold
    return {
        "schema": SCHEMA,
        "generated_at": generated_at,
        "run_store_root": str(run_store.root),
        "goal_store_root": str(goal_store.root),
        "status": build_status(run_store, goal_store, pricing=pricing),
        "heartbeat": heartbeat,
        "heartbeat_age_seconds": None if heartbeat_age is None else round(heartbeat_age, 3),
        "stale": stale,
        "attempt_wall_clock_upper_bound_seconds": attempt_bound,
        "tick_overhead_seconds": max(0.0, float(tick_overhead_seconds)),
        "staleness_threshold_seconds": threshold,
        "run_liveness": build_run_liveness(
            run_store,
            heartbeat=heartbeat,
            staleness_threshold_seconds=threshold,
        ),
    }


def write_snapshot(snapshot: dict[str, Any], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(snapshot, ensure_ascii=False, indent=2)
    temporary = out_path.with_name(out_path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(out_path)
    return out_path


def write_heartbeat(heartbeat: dict[str, Any], out_path: Path) -> Path:
    """Atomically publish one heartbeat without requiring a status snapshot."""
    return write_snapshot(heartbeat, out_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write a unified LH project-status snapshot (JSON)")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--run-store", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)

    generated_at = datetime.now(timezone.utc).isoformat()
    snapshot = build_snapshot(
        RunStore(Path(args.run_store)),
        GoalStore(Path(args.goal_store)),
        generated_at=generated_at,
    )
    out = write_snapshot(snapshot, Path(args.out))
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

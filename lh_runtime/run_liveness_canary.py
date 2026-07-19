#!/usr/bin/env python3
"""Canary for per-run ownership projection and the A2-derived stale threshold."""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore
from run_store import RunStore
from status_snapshot import (
    build_snapshot,
    derive_staleness_threshold_seconds,
    attempt_wall_clock_upper_bound_seconds,
)


def _case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        runs = RunStore(root / "runs")
        goals = GoalStore(root / "goals")
        run_a = runs.create_run(goal={"goal_id": "goal-a"}, source_repo=HERE, base_revision="a")
        run_b = runs.create_run(goal={"goal_id": "goal-b"}, source_repo=HERE, base_revision="b")
        attempt_a = runs.begin_attempt(run_a, f"workspace://{run_a}/1")
        attempt_b = runs.begin_attempt(run_b, f"workspace://{run_b}/1")
        runs.acquire_lease(run_a, "owner-a", seconds=120)
        runs.acquire_lease(run_b, "owner-b", seconds=120)

        heartbeat = {
            "schema": "loop-hybrid-driver-heartbeat/v1",
            "holder": runs._process_holder("owner-a"),
            "monotonic_ts": 100.0,
            "wall_ts": datetime.now(timezone.utc).isoformat(),
            "phase": "progress",
            "cycles": 1,
        }
        timeout_seconds = 2.0
        tick_overhead_seconds = 0.5
        snapshot = build_snapshot(
            runs,
            goals,
            generated_at="2026-07-17T00:00:00+00:00",
            heartbeat=heartbeat,
            attempt_timeout_seconds=timeout_seconds,
            tick_overhead_seconds=tick_overhead_seconds,
        )
        records = {item["run_id"]: item for item in snapshot["run_liveness"]}
        bound = attempt_wall_clock_upper_bound_seconds(timeout_seconds)
        threshold = snapshot["staleness_threshold_seconds"]
        cases = [
            _case(
                "two-runs-keep-distinct-attempt-and-owner",
                records[run_a]["attempt"] == attempt_a
                and records[run_b]["attempt"] == attempt_b
                and records[run_a]["owner_holder"] == heartbeat["holder"]
                and records[run_b]["owner_holder"] != records[run_a]["owner_holder"]
                and records[run_a]["liveness"] == "live"
                and records[run_b]["liveness"] == "stale",
                json.dumps({"a": records[run_a], "b": records[run_b]}),
            ),
            _case(
                "threshold-is-derived-and-strictly-exceeds-a2-bound",
                snapshot["attempt_wall_clock_upper_bound_seconds"] == bound
                and snapshot["tick_overhead_seconds"] == tick_overhead_seconds
                and threshold == derive_staleness_threshold_seconds(
                    timeout_seconds=timeout_seconds,
                    tick_overhead_seconds=tick_overhead_seconds,
                )
                and threshold > bound,
                json.dumps({"bound": bound, "threshold": threshold}),
            ),
            _case(
                "snapshot-keeps-v1-and-adds-liveness-at-top-level",
                snapshot["schema"] == "loop-hybrid-status-snapshot/v1"
                and snapshot["status"]["schema"] == "loop-hybrid-project-status/v1"
                and set(("heartbeat", "staleness_threshold_seconds", "run_liveness")) <= set(snapshot),
                json.dumps({"schema": snapshot["schema"], "top_level": sorted(snapshot)}),
            ),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-run-liveness",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "foreground heartbeat is not evidence during a blocking executor",
            "SH applies wall-clock stale/dead classification to this projection",
        ],
    }, ensure_ascii=False, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

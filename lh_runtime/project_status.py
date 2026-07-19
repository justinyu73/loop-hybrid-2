#!/usr/bin/env python3
"""Unified per-project status — the read layer a lightweight platform / SH consumes.

Joins the existing read surfaces (run counts, goal lifecycle, parked/needs-human,
token cost, elapsed time) into one read-only object. It is a projection, never an
authority: LH's SQLite stores remain the source of truth. SH aggregates this
across projects (span of control); this builds one project's view.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
import value_reducer
from goal_store import GoalStore
from run_store import RunStore

SCHEMA = "loop-hybrid-project-status/v1"


def _wall_age_seconds(heartbeat: dict[str, Any] | None, *, now: datetime | None = None) -> float | None:
    if not isinstance(heartbeat, dict) or not isinstance(heartbeat.get("wall_ts"), str):
        return None
    try:
        parsed = datetime.fromisoformat(heartbeat["wall_ts"])
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0.0, current.timestamp() - parsed.timestamp())


def build_run_liveness(
    run_store: RunStore,
    *,
    heartbeat: dict[str, Any] | None,
    staleness_threshold_seconds: float,
) -> list[dict[str, Any]]:
    """Project every run with its current attempt and lease owner.

    The heartbeat is process-wide, so it is only evidence for a running run
    when its holder matches that run's lease owner.  Queued, parked, and
    terminal runs are reported as idle rather than incorrectly called live.
    """
    age = _wall_age_seconds(heartbeat)
    heartbeat_holder = heartbeat.get("holder") if isinstance(heartbeat, dict) else None
    threshold = float(staleness_threshold_seconds)
    with run_store._connect() as conn:
        runs = conn.execute("SELECT run_id, state, attempts FROM runs ORDER BY created_at, run_id").fetchall()
        records: list[dict[str, Any]] = []
        for run in runs:
            attempt = conn.execute(
                "SELECT ordinal, state FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1",
                (run["run_id"],),
            ).fetchone()
            lease = conn.execute(
                "SELECT holder, expires_at FROM leases WHERE run_id = ?",
                (run["run_id"],),
            ).fetchone()
            owner_holder = lease["holder"] if lease is not None else None
            if heartbeat is None or age is None:
                liveness = "unknown"
            elif run["state"] != "running":
                liveness = "idle"
            elif owner_holder is None or owner_holder != heartbeat_holder or age > threshold:
                liveness = "stale"
            else:
                liveness = "live"
            records.append({
                "run_id": run["run_id"],
                "attempt": attempt["ordinal"] if attempt is not None else None,
                "attempt_state": attempt["state"] if attempt is not None else None,
                "state": run["state"],
                "owner_holder": owner_holder,
                "heartbeat_holder": heartbeat_holder,
                "heartbeat_age_seconds": round(age, 3) if age is not None else None,
                "staleness_threshold_seconds": threshold,
                "liveness": liveness,
            })
    return records


def build_status(run_store: RunStore, goal_store: GoalStore, *, pricing: dict[str, dict[str, float]] | None = None) -> dict[str, Any]:
    runs_by_state = run_store.summary().get("runs_by_state", {})
    goals_by_state = goal_store.summary().get("goals_by_state", {})
    parked = [goal["goal_id"] for goal in goal_store.goals_in_state("human_required")]
    cost = token_cost.aggregate(run_store.usage_records(), pricing=pricing)
    value = value_reducer.aggregate(run_store)
    headline = {
        "active_runs": runs_by_state.get("running", 0) + runs_by_state.get("queued", 0) + runs_by_state.get("retry_pending", 0),
        "completed_goals": goals_by_state.get("completed", 0),
        "needs_human": len(parked),
        # W3: events parked in human_required are surfaced too — a command that
        # dies at the matcher (no goal ever created) must not stay invisible.
        "needs_human_events": goal_store.summary().get("events_by_state", {}).get("human_required", 0),
        "value_red": value["red"],
        "total_tokens": cost["total_tokens"],
        "estimated_cost_usd": cost["estimated_cost_usd"],
        "cost_complete": cost["cost_complete"],
        "total_elapsed_seconds": cost["total_elapsed_seconds"],
    }
    return {
        "schema": SCHEMA,
        "headline": headline,
        "runs_by_state": runs_by_state,
        "goals_by_state": goals_by_state,
        "parked_goals": parked,
        "cost": cost,
        "value": value,
    }


def render_text(status: dict[str, Any]) -> str:
    h = status["headline"]
    cost = "estimated" if h["cost_complete"] else "estimated (incomplete — some usage unknown)"
    lines = [
        "LH project status",
        f"  active runs      : {h['active_runs']}   ({status['runs_by_state'] or 'none'})",
        f"  completed goals  : {h['completed_goals']}   ({status['goals_by_state'] or 'none'})",
        f"  needs human      : {h['needs_human']}" + (f"  -> {', '.join(status['parked_goals'])}" if status["parked_goals"] else ""),
        f"  value red (报红)  : {h['value_red']}" + (f"  -> {', '.join(r['run_id'] for r in status['value']['red_runs'])}" if status["value"]["red_runs"] else ""),
        f"  tokens           : {h['total_tokens']}",
        f"  cost (USD)       : {h['estimated_cost_usd']}  [{cost}]",
        f"  elapsed (s)      : {h['total_elapsed_seconds']}",
    ]
    return "\n".join(lines)

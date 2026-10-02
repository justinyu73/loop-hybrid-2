#!/usr/bin/env python3
"""Provider-free always-on driver: call one GoalLoopWorker.tick repeatedly.

The worker is a bounded invoke-once tick; this driver is the missing loop that
wakes it until a stop gate fires. It is provider-free — the ``model`` runner is
injected, so the same driver runs a fixture model (canary) or a real coding-agent
executor (later, opt-in). The driver never uses chat context as a queue, never
starts parallel workers (one holder), and always stops on a gate rather than
spinning: kill switch, budget, idle streak, or a max-cycles cap.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import budget_reducer
import dispatch_gate
import external_verdict as ev
from goal_loop_worker import GoalLoopWorker, ModelRunner, TurningPointRunner
from lifecycle import (
    ForegroundLifecycle,
    LifecycleLease,
    LifecycleOwnershipLost,
    LifecyclePort,
    LifecycleUnavailable,
)
from platform_ports import FileLockSchedulerPort, SchedulerPort
from status_snapshot import build_heartbeat, build_snapshot, default_heartbeat_path, write_heartbeat, write_snapshot


def run_driver(
    worker: GoalLoopWorker,
    *,
    holder: str,
    model: ModelRunner,
    verdict_store: ev.VerdictStore | None = None,
    conclusion_source: ev.ConclusionSource | None = None,
    pause_flag: str | Path | None = None,
    max_cycles: int | None = None,
    max_runs: int | None = None,
    max_runtime_seconds: float | None = None,
    budget_ceiling_tokens: int | None = None,
    budget_scope: str | None = None,
    idle_limit: int = 3,
    backoff_seconds: float = 1.0,
    status_snapshot_out: str | Path | None = None,
    quota_reader: dispatch_gate.QuotaReader | None = None,
    daily_soft_cap_usd: float | None = 2.0,
    daily_hard_cap_usd: float | None = 5.0,
    sleep_fn: Callable[[float], None] | None = None,
    clock_fn: Callable[[], float] | None = None,
    turning_point: TurningPointRunner | None = None,
    scheduler_port: SchedulerPort | None = None,
    lifecycle_port: LifecyclePort | None = None,
    shutdown_flag: str | Path | None = None,
) -> dict[str, Any]:
    """Acquire the process singleton, then run one bounded driver session."""
    lifecycle = lifecycle_port or ForegroundLifecycle(shutdown_flag=shutdown_flag)
    owner_path = lifecycle.owner_path(worker.run_store.root)
    try:
        lease = lifecycle.acquire(
            worker.run_store.root,
            holder,
            scheduler_port=scheduler_port or FileLockSchedulerPort(),
        )
    except LifecycleUnavailable as exc:
        return {
            "stop_reason": "lifecycle_unavailable",
            "cycles": 0,
            "runs_dispatched": 0,
            "idle_streak": 0,
            "outcomes": [],
            "parked_goals": [],
            "heartbeat_path": str(default_heartbeat_path(worker.run_store.root)),
            "budget": {},
            "dispatch_gate": None,
            "owner_lease_path": str(owner_path),
            "lifecycle_error": str(exc),
        }
    if lease is None:
        return {
            "stop_reason": "not_holder",
            "cycles": 0,
            "runs_dispatched": 0,
            "idle_streak": 0,
            "outcomes": [],
            "parked_goals": [],
            "heartbeat_path": str(default_heartbeat_path(worker.run_store.root)),
            "budget": {},
            "dispatch_gate": None,
            "owner_lease_path": str(owner_path),
        }
    summary: dict[str, Any] | None = None
    try:
        summary = _run_driver_loop(
            worker,
            holder=holder,
            model=model,
            verdict_store=verdict_store,
            conclusion_source=conclusion_source,
            pause_flag=pause_flag,
            max_cycles=max_cycles,
            max_runs=max_runs,
            max_runtime_seconds=max_runtime_seconds,
            budget_ceiling_tokens=budget_ceiling_tokens,
            budget_scope=budget_scope,
            idle_limit=idle_limit,
            backoff_seconds=backoff_seconds,
            status_snapshot_out=status_snapshot_out,
            quota_reader=quota_reader,
            daily_soft_cap_usd=daily_soft_cap_usd,
            daily_hard_cap_usd=daily_hard_cap_usd,
            sleep_fn=sleep_fn,
            clock_fn=clock_fn,
            turning_point=turning_point,
            lifecycle=lease,
        )
    except BaseException:
        lease.close("process_exception")
        raise
    final_record = lease.close(summary["stop_reason"])
    summary["lifecycle"] = {
        "owner_lease_path": str(owner_path),
        "recovery": lease.recovery,
        "state": final_record.get("state") if final_record else "ownership_lost",
        "stop_reason": summary["stop_reason"],
    }
    return summary


def _run_driver_loop(
    worker: GoalLoopWorker,
    *,
    holder: str,
    model: ModelRunner,
    verdict_store: ev.VerdictStore | None = None,
    conclusion_source: ev.ConclusionSource | None = None,
    pause_flag: str | Path | None = None,
    max_cycles: int | None = None,
    max_runs: int | None = None,
    max_runtime_seconds: float | None = None,
    budget_ceiling_tokens: int | None = None,
    budget_scope: str | None = None,
    idle_limit: int = 3,
    backoff_seconds: float = 1.0,
    status_snapshot_out: str | Path | None = None,
    quota_reader: dispatch_gate.QuotaReader | None = None,
    daily_soft_cap_usd: float | None = 2.0,
    daily_hard_cap_usd: float | None = 5.0,
    sleep_fn: Callable[[float], None] | None = None,
    clock_fn: Callable[[], float] | None = None,
    turning_point: TurningPointRunner | None = None,
    lifecycle: LifecycleLease | None = None,
) -> dict[str, Any]:
    """Drive the worker until a gate stops it. Returns a bounded session summary.

    Gates (checked before every tick): ``pause_flag`` exists (kill switch),
    ``max_cycles`` reached, ``max_runs`` dispatched (budget proxy — token budget
    is a later node), ``max_runtime_seconds`` wall-clock elapsed. After a tick
    that made no progress, ``idle_limit`` consecutive idle ticks stop the driver
    so it can be restarted cheaply. When those idle ticks are only because goals
    are parked in ``human_required``, the stop reason is ``parked`` (the operator
    is needed) rather than ``idle`` (the queue is empty). An injected
    ``budget_ceiling_tokens`` is reduced from the worker's ``RunStore`` receipts
    before startup dispatch and before each tick; unknown receipt usage stops the
    driver. The W5 owner-durability gate (daily UTC cost caps, quota pressure,
    executor credential failure) runs before every dispatch: a soft cap idles
    the tick, a hard cap ends the session, and an in-flight attempt is never
    touched. Every gate input is re-read each tick, so a cleared condition
    resumes dispatch with no manual reset.
    """
    sleep = sleep_fn if sleep_fn is not None else time.sleep
    # perf_counter is monotonic too, but high resolution on every platform;
    # time.monotonic ticks every ~15.6 ms on Windows, so back-to-back
    # heartbeats could repeat a timestamp.  Values are only compared within
    # this process.
    clock = clock_fn if clock_fn is not None else time.perf_counter
    pause = Path(pause_flag) if pause_flag is not None else None
    snapshot_out = Path(status_snapshot_out) if status_snapshot_out is not None else None
    heartbeat_out = default_heartbeat_path(worker.run_store.root)
    gate = dispatch_gate.DispatchGate(
        worker.run_store,
        quota_reader=quota_reader,
        soft_daily_usd=daily_soft_cap_usd,
        hard_daily_usd=daily_hard_cap_usd,
    )
    gate_state: dict[str, Any] | None = None
    started_at = clock()
    cycles = 0
    runs_dispatched = 0
    idle_streak = 0
    outcomes: list[dict[str, Any]] = []
    stop_reason = "max_cycles"
    budget: dict[str, Any] = {}
    while True:
        if lifecycle is not None and lifecycle.shutdown_requested():
            stop_reason = "shutdown_requested"
            break
        _lifecycle_heartbeat(lifecycle, phase="loop", cycles=cycles)
        if pause is not None and pause.exists():
            stop_reason = "paused"
            break
        budget = budget_reducer.evaluate(
            worker.run_store,
            ceiling_tokens=budget_ceiling_tokens,
            scope=budget_scope,
        )
        if budget["stop_reason"] is not None:
            stop_reason = str(budget["stop_reason"])
            break
        gate_state = gate.evaluate()
        if gate_state["action"] == dispatch_gate.STOP:
            stop_reason = str(gate_state["reason_code"])
            break
        if gate_state["action"] == dispatch_gate.IDLE:
            idle_streak += 1
            _write_heartbeat(worker, heartbeat_out, holder=holder, phase="idle", cycles=cycles, monotonic_ts=clock())
            _lifecycle_heartbeat(lifecycle, phase="idle", cycles=cycles)
            if snapshot_out is not None:
                _refresh_snapshot(worker, snapshot_out, tick_overhead_seconds=backoff_seconds, gate_state=gate_state)
            if idle_streak >= idle_limit:
                stop_reason = str(gate_state["reason_code"])
                break
            sleep(backoff_seconds)
            continue
        if max_cycles is not None and cycles >= max_cycles:
            stop_reason = "max_cycles"
            break
        if max_runs is not None and runs_dispatched >= max_runs:
            stop_reason = "budget"
            break
        if max_runtime_seconds is not None and clock() - started_at >= max_runtime_seconds:
            stop_reason = "timeout"
            break

        _write_heartbeat(worker, heartbeat_out, holder=holder, phase="tick", cycles=cycles, monotonic_ts=clock())
        _lifecycle_heartbeat(lifecycle, phase="tick", cycles=cycles)
        result = worker.tick(holder=holder, model=model, verdict_store=verdict_store, conclusion_source=conclusion_source, turning_point=turning_point)
        cycles += 1

        run = result.get("run")
        if run is not None and run.get("status") != "human_required":
            runs_dispatched += 1
        terminal = result.get("terminal_after")
        if terminal is not None:
            outcomes.append(terminal)

        auth = gate.auth_observation(result)
        if auth is not None:
            gate_state = auth
            stop_reason = str(auth["reason_code"])
            break

        if lifecycle is not None and lifecycle.shutdown_requested():
            stop_reason = "shutdown_requested"
            break

        if result["status"] == "progress":
            idle_streak = 0
            if snapshot_out is not None:
                _refresh_snapshot(worker, snapshot_out, tick_overhead_seconds=backoff_seconds, gate_state=gate_state)
            _write_heartbeat(worker, heartbeat_out, holder=holder, phase="progress", cycles=cycles, monotonic_ts=clock())
            _lifecycle_heartbeat(lifecycle, phase="progress", cycles=cycles)
            continue
        idle_streak += 1
        _write_heartbeat(worker, heartbeat_out, holder=holder, phase="idle", cycles=cycles, monotonic_ts=clock())
        _lifecycle_heartbeat(lifecycle, phase="idle", cycles=cycles)
        if idle_streak >= idle_limit:
            stop_reason = "parked" if _parked_goal_ids(worker) else "idle"
            break
        sleep(backoff_seconds)

    parked_goals = _parked_goal_ids(worker)
    if snapshot_out is not None:
        _refresh_snapshot(worker, snapshot_out, tick_overhead_seconds=backoff_seconds, gate_state=gate_state)
    return {
        "stop_reason": stop_reason,
        "cycles": cycles,
        "runs_dispatched": runs_dispatched,
        "idle_streak": idle_streak,
        "outcomes": outcomes,
        "parked_goals": parked_goals,
        "heartbeat_path": str(heartbeat_out),
        "budget": budget,
        "dispatch_gate": gate_state,
    }


def _lifecycle_heartbeat(lifecycle: LifecycleLease | None, *, phase: str, cycles: int) -> None:
    if lifecycle is None:
        return
    if not lifecycle.heartbeat(phase=phase, cycles=cycles):
        raise LifecycleOwnershipLost("foreground owner lease no longer matches this process")


def _refresh_snapshot(worker: GoalLoopWorker, out_path: Path, *, tick_overhead_seconds: float = 0.0, gate_state: dict[str, Any] | None = None) -> None:
    # LoopController.__init__ always sets timeout_seconds; read it directly.
    timeout_seconds = float(worker.controller.timeout_seconds)
    snapshot = build_snapshot(
        worker.run_store,
        worker.goal_store,
        generated_at=datetime.now(timezone.utc).isoformat(),
        attempt_timeout_seconds=timeout_seconds,
        tick_overhead_seconds=tick_overhead_seconds,
        dispatch_gate=gate_state,
    )
    write_snapshot(snapshot, out_path)


def _write_heartbeat(
    worker: GoalLoopWorker,
    out_path: Path,
    *,
    holder: str,
    phase: str,
    cycles: int,
    monotonic_ts: float,
) -> None:
    process_holder = worker.run_store._process_holder(holder)
    heartbeat = build_heartbeat(
        holder=process_holder,
        monotonic_ts=monotonic_ts,
        wall_ts=datetime.now(timezone.utc).isoformat(),
        phase=phase,
        cycles=cycles,
    )
    write_heartbeat(heartbeat, out_path)


def _parked_goal_ids(worker: GoalLoopWorker) -> list[str]:
    return [goal["goal_id"] for goal in worker.goal_store.goals_in_state("human_required")]

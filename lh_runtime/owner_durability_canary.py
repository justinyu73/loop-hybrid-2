#!/usr/bin/env python3
"""Committed W5 smoke: owner-durability dispatch gate at the driver seam.

Proves, offline with fixture quota readers and fixture executors, that the
driver's pre-dispatch gate enforces the owner-mode durability policy: quota
59/60/80/100 -> dispatch / snapshot note / idle tick / session stop; daily
UTC cost $1.9/$2/$5 -> allow / soft idle / session stop; an executor
credential failure parks dispatch after the probe attempt and marks the
snapshot instead of burning retries; cleared conditions resume on their own;
and a run already in flight is never touched by a stop. No network, no real
provider, no real credentials.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_campaign, make_source_repo
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN_ID = "campaign-w5"
GOAL_ID = f"{CAMPAIGN_ID}:stage-1"
# gpt-5.6-luna is priced at exactly $1.0/Mtok input in token_cost.DEFAULT_PRICING,
# so one seeded receipt's input_tokens map 1:1 onto microdollars.
COST_MODEL = "gpt-5.6-luna"


def _noop_sleep(_seconds: float) -> None:
    return None


def _model(workspace: Path, capsule: dict) -> dict:
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    (src / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "w5 fixture model"}


def _auth_model(_workspace: Path, _capsule: dict) -> dict:
    raise RuntimeError("codex exited 1: 401 Unauthorized: token expired, please log in again")


def _worker(root: Path, tag: str, source: Path, base: str) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    compiler = CampaignCompiler(make_campaign(CAMPAIGN_ID))
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={CAMPAIGN_ID: compiler},
        execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
    )


def _seed_goal(worker: GoalLoopWorker, tag: str) -> None:
    envelope = worker.compilers[CAMPAIGN_ID].compile()["stages"]["stage-1"]
    worker.goal_store.record_event(event_id=f"w5-{tag}", idempotency_key=f"w5-{tag}", source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": "stage-1",
                      "goal": {"feature_contract": "stage-1", "admission_envelope": envelope}}
    })


def _seed_cost(worker: GoalLoopWorker, source: Path, base: str, *, tag: str, input_tokens: int) -> None:
    """Commit one verified attempt whose measured usage costs input_tokens/1e6 USD today."""
    store = worker.run_store
    run_id = f"run-w5-cost-{tag}"
    store.create_run(goal={"goal_id": f"w5-cost-{tag}"}, source_repo=source, base_revision=base, run_id=run_id)
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1",
        "run_id": run_id,
        "attempt": ordinal,
        "usage": {"state": "measured", "model": COST_MODEL, "input_tokens": input_tokens, "output_tokens": 0, "cache_read_tokens": 0},
        "verification": {"argv": ["true"], "exit_code": 0},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _drive(worker: GoalLoopWorker, tag: str, *, model=_model, quota: float | None = None,
           snapshot: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    snap_out = worker.run_store.root.parent / f"{tag}-snapshot.json"
    result = run_driver(
        worker,
        holder=f"w5-{tag}",
        model=model,
        max_cycles=10,
        idle_limit=2,
        sleep_fn=_noop_sleep,
        quota_reader=(lambda: {"used_percent": quota}) if quota is not None else None,
        status_snapshot_out=snap_out if snapshot else None,
    )
    snap = json.loads(snap_out.read_text(encoding="utf-8")) if snap_out.exists() else {}
    return result, snap


def _gate_reason(payload: dict[str, Any]) -> Any:
    gate = payload.get("dispatch_gate")
    return gate.get("reason_code") if isinstance(gate, dict) else None


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        outcomes: dict[str, Any] = {}

        # Quota ladder: one worker per rung, each with a dispatchable goal.
        for tag, quota in (("q59", 59.0), ("q60", 60.0), ("q80", 80.0), ("q100", 100.0)):
            worker = _worker(root, tag, source, base)
            _seed_goal(worker, tag)
            result, snap = _drive(worker, tag, quota=quota)
            outcomes[tag] = {"result": result, "snap_reason": _gate_reason(snap)}

        # Daily cost ladder: seeded receipts price at exactly $1.9 / $2 / $5 today.
        for tag, tokens in (("c190", 1_900_000), ("c200", 2_000_000), ("c500", 5_000_000)):
            worker = _worker(root, tag, source, base)
            _seed_cost(worker, source, base, tag=tag, input_tokens=tokens)
            _seed_goal(worker, tag)
            result, snap = _drive(worker, tag)
            outcomes[tag] = {"result": result, "snap_reason": _gate_reason(snap)}

        # Credential failure: one probe attempt, then the session parks.
        auth_worker = _worker(root, "auth", source, base)
        _seed_goal(auth_worker, "auth")
        auth_result, auth_snap = _drive(auth_worker, "auth", model=_auth_model)
        auth_runnable = auth_worker.run_store.runnable_runs()
        auth_run = auth_worker.run_store.get_run(auth_runnable[0]["run_id"]) if auth_runnable else None
        # Recovery: credentials fixed -> the next session re-probes and completes.
        recovered, _ = _drive(auth_worker, "auth2", snapshot=False)
        recovered_run = auth_worker.run_store.get_run(auth_run["run_id"]) if auth_run is not None else None

        # Quota recovery: 100 stops the first session; 30 dispatches in the next.
        rec_worker = _worker(root, "rec", source, base)
        _seed_goal(rec_worker, "rec")
        rec_stopped, _ = _drive(rec_worker, "rec", quota=100.0, snapshot=False)
        rec_resumed, _ = _drive(rec_worker, "rec2", quota=30.0, snapshot=False)

        # A run already in flight is never touched by a session stop.
        flight_worker = _worker(root, "flight", source, base)
        flight_id = "run-w5-in-flight"
        flight_worker.run_store.create_run(goal={"goal_id": "w5-in-flight"}, source_repo=source, base_revision=base, run_id=flight_id)
        flight_worker.run_store.begin_attempt(flight_id, f"workspace://{flight_id}/1")
        flight_result, _ = _drive(flight_worker, "flight", quota=100.0, snapshot=False)
        flight_after = flight_worker.run_store.get_run(flight_id)

        cases = [
            {"id": "quota-59-dispatches",
             "ok": outcomes["q59"]["result"]["runs_dispatched"] >= 1
             and outcomes["q59"]["result"]["dispatch_gate"]["action"] == "allow",
             "detail": json.dumps({"dispatched": outcomes["q59"]["result"]["runs_dispatched"],
                                   "gate": outcomes["q59"]["result"]["dispatch_gate"]["action"]})},
            {"id": "quota-60-dispatches-with-snapshot-note",
             "ok": outcomes["q60"]["result"]["runs_dispatched"] >= 1
             and outcomes["q60"]["result"]["dispatch_gate"]["action"] == "note"
             and outcomes["q60"]["snap_reason"] == "quota_notify",
             "detail": json.dumps({"gate": outcomes["q60"]["result"]["dispatch_gate"]["action"],
                                   "snap_reason": outcomes["q60"]["snap_reason"]})},
            {"id": "quota-80-idles-without-dispatch",
             "ok": outcomes["q80"]["result"]["runs_dispatched"] == 0
             and outcomes["q80"]["result"]["cycles"] == 0
             and outcomes["q80"]["result"]["stop_reason"] == "quota_soft"
             and outcomes["q80"]["snap_reason"] == "quota_soft",
             "detail": json.dumps({"stop": outcomes["q80"]["result"]["stop_reason"],
                                   "snap_reason": outcomes["q80"]["snap_reason"]})},
            {"id": "quota-100-stops-before-any-tick",
             "ok": outcomes["q100"]["result"]["runs_dispatched"] == 0
             and outcomes["q100"]["result"]["cycles"] == 0
             and outcomes["q100"]["result"]["stop_reason"] == "quota_hard"
             and outcomes["q100"]["snap_reason"] == "quota_hard",
             "detail": json.dumps({"stop": outcomes["q100"]["result"]["stop_reason"],
                                   "snap_reason": outcomes["q100"]["snap_reason"]})},
            {"id": "daily-cost-1.9-allows-dispatch",
             "ok": outcomes["c190"]["result"]["runs_dispatched"] >= 1
             and outcomes["c190"]["result"]["dispatch_gate"]["cost"]["estimated_cost_usd"] >= 1.9,
             "detail": json.dumps({"dispatched": outcomes["c190"]["result"]["runs_dispatched"],
                                   "cost": outcomes["c190"]["result"]["dispatch_gate"]["cost"]["estimated_cost_usd"]})},
            {"id": "daily-cost-2-soft-idles",
             "ok": outcomes["c200"]["result"]["runs_dispatched"] == 0
             and outcomes["c200"]["result"]["cycles"] == 0
             and outcomes["c200"]["result"]["stop_reason"] == "daily_cost_soft"
             and outcomes["c200"]["snap_reason"] == "daily_cost_soft",
             "detail": json.dumps({"stop": outcomes["c200"]["result"]["stop_reason"],
                                   "snap_reason": outcomes["c200"]["snap_reason"]})},
            {"id": "daily-cost-5-stops-without-dispatch",
             "ok": outcomes["c500"]["result"]["runs_dispatched"] == 0
             and outcomes["c500"]["result"]["cycles"] == 0
             and outcomes["c500"]["result"]["stop_reason"] == "daily_cost_hard"
             and outcomes["c500"]["snap_reason"] == "daily_cost_hard",
             "detail": json.dumps({"stop": outcomes["c500"]["result"]["stop_reason"],
                                   "snap_reason": outcomes["c500"]["snap_reason"]})},
            {"id": "auth-failure-parks-after-one-probe",
             "ok": auth_result["stop_reason"] == "executor_auth"
             and auth_result["runs_dispatched"] == 1
             and auth_run is not None and auth_run["attempts"] == 1
             and auth_run["state"] == "retry_pending"
             and _gate_reason(auth_snap) == "executor_auth",
             "detail": json.dumps({"stop": auth_result["stop_reason"], "attempts": auth_run["attempts"] if auth_run else None,
                                   "run_state": auth_run["state"] if auth_run else None, "snap_reason": _gate_reason(auth_snap)})},
            {"id": "fixed-credentials-resume-next-session",
             "ok": recovered["runs_dispatched"] >= 1 and recovered_run is not None and recovered_run["state"] == "verified",
             "detail": json.dumps({"dispatched": recovered["runs_dispatched"],
                                   "run_state": recovered_run["state"] if recovered_run else None})},
            {"id": "recovered-quota-resumes-next-session",
             "ok": rec_stopped["stop_reason"] == "quota_hard" and rec_stopped["runs_dispatched"] == 0
             and rec_resumed["runs_dispatched"] >= 1,
             "detail": json.dumps({"stopped": rec_stopped["stop_reason"], "resumed": rec_resumed["runs_dispatched"]})},
            {"id": "session-stop-never-touches-a-running-run",
             "ok": flight_result["stop_reason"] == "quota_hard" and flight_result["cycles"] == 0
             and flight_after["state"] == "running" and flight_after["attempts"] == 1,
             "detail": json.dumps({"stop": flight_result["stop_reason"], "run_state": flight_after["state"],
                                   "attempts": flight_after["attempts"]})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-owner-durability",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/owner_durability_canary.py",
                         "fixtures": "quota readers and executors only; no network, no real credentials"},
        "known_gaps_open": [
            "production quota_reader wiring (host quota probe) is a later slice; the driver seam accepts any injected reader",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

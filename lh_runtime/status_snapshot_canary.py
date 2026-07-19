#!/usr/bin/env python3
"""Provider-free smoke for the durable status snapshot (materialize surface)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
from goal_store import GoalStore
from project_status import build_status
from run_store import RunStore
from status_snapshot import SCHEMA, build_snapshot, write_snapshot


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _goal(store: GoalStore, *, goal_id: str, event_key: str, final_state: str) -> None:
    store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={"c": goal_id})
    store.create_candidate(event_key, goal_id=goal_id, campaign_id="camp", stage_id="s1", goal={"feature_contract": goal_id})
    if final_state == "human_required":
        store.transition_goal(goal_id, "human_required", expected_state="candidate")
    elif final_state == "completed":
        store.transition_goal(goal_id, "active", expected_state="candidate")
        store.transition_goal(goal_id, "completed", expected_state="active")


def _run_with_usage(store: RunStore, usage: dict) -> None:
    run_id = store.create_run(goal={"feature_contract": "x"}, source_repo=HERE, base_revision="r")
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal, "usage": usage, "verification": {"argv": ["true"], "exit_code": 0, "stdout": "a", "stderr": "b"}}
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        goals = GoalStore(root / "goals")
        runs = RunStore(root / "runs")
        _goal(goals, goal_id="g-parked", event_key="e-parked", final_state="human_required")
        _goal(goals, goal_id="g-done", event_key="e-done", final_state="completed")
        _run_with_usage(runs, token_cost.measured_usage(model="m1", input_tokens=1000, output_tokens=200, cache_read_tokens=5000))

        # W3: events parked in human_required surface in the headline (an intent
        # that dies without ever creating a goal must not stay invisible).
        goals.record_event(event_id="e-stuck", idempotency_key="e-stuck", source="manual_intent", event_type="manual_intent", payload={"campaign_id": "camp"})
        goals.transition_event("e-stuck", "human_required", result={"reason": "no deterministic linkage"})
        status = build_status(runs, goals)
        snapshot = build_snapshot(runs, goals, generated_at="2026-07-16T00:00:00+00:00")
        out = write_snapshot(snapshot, root / "runtime" / "platform_status.json")
        reloaded = json.loads(out.read_text(encoding="utf-8"))

        # B9: top-level driver staleness projection (read-only; recovery unchanged).
        from datetime import datetime, timezone
        fresh_hb = {"schema": "loop-hybrid-driver-heartbeat/v1", "holder": "h", "monotonic_ts": 1.0, "wall_ts": datetime.now(timezone.utc).isoformat(), "phase": "tick", "cycles": 1}
        old_hb = {**fresh_hb, "wall_ts": "2000-01-01T00:00:00+00:00"}
        live_snap = build_snapshot(runs, goals, generated_at="2026-07-16T00:00:00+00:00", heartbeat=fresh_hb)
        stale_snap = build_snapshot(runs, goals, generated_at="2026-07-16T00:00:00+00:00", heartbeat=old_hb)
        no_hb_snap = build_snapshot(runs, goals, generated_at="2026-07-16T00:00:00+00:00")

        h = reloaded["status"]["headline"]
        cases = [
            case("snapshot-schema-and-provenance", reloaded["schema"] == SCHEMA and reloaded["generated_at"] == "2026-07-16T00:00:00+00:00" and reloaded["run_store_root"] == str(runs.root), json.dumps({k: reloaded.get(k) for k in ("schema", "generated_at")})),
            case("snapshot-status-equals-build-status", reloaded["status"] == status, "snapshot status diverged from build_status"),
            case("headline-carries-value-red-and-cost", h["value_red"] == status["headline"]["value_red"] and h["total_tokens"] == 6200 and "estimated_cost_usd" in h and h["needs_human"] == 1, json.dumps(h)),
            case("write-is-atomic-no-tmp-left", out.exists() and not out.with_name(out.name + ".tmp").exists(), str(out)),
            case(
                "top-level-stale-follows-heartbeat-age",
                live_snap["stale"] is False and stale_snap["stale"] is True and no_hb_snap["stale"] is True and live_snap["heartbeat_age_seconds"] is not None,
                json.dumps({"live": live_snap["stale"], "old": stale_snap["stale"], "missing": no_hb_snap["stale"]}),
            ),
            case(
                "human-required-events-surface-in-headline",
                reloaded["status"]["headline"]["needs_human_events"] == 1,
                json.dumps({"needs_human_events": reloaded["status"]["headline"].get("needs_human_events")}),
            ),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-status-snapshot",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "read-only projection; LH SQLite stores remain the source of truth",
            "snapshot is materialized on demand; no resident refresh wired yet",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

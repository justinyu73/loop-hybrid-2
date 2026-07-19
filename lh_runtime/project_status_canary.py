#!/usr/bin/env python3
"""Provider-free smoke for the unified project-status read layer (N4)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from mcp_server import dispatch
from project_status import build_status, render_text
from run_store import RunStore


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
        _run_with_usage(runs, token_cost.unknown_usage(model="m1"))

        status = build_status(runs, goals)
        text = render_text(status)

        knowledge = KnowledgeStore(root / "knowledge")
        read = dispatch({"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": "lh://runtime/status"}}, runs, knowledge, goals)
        mcp_status = json.loads(read["result"]["contents"][0]["text"])
        # status resource must stay gated when goal_store is absent
        gated = dispatch({"jsonrpc": "2.0", "id": 2, "method": "resources/read", "params": {"uri": "lh://runtime/status"}}, runs, knowledge)

        h = status["headline"]
        cases = [
            case("status-joins-runs-goals-parked-cost", h["needs_human"] == 1 and status["parked_goals"] == ["g-parked"] and h["completed_goals"] == 1 and status["cost"]["measured_records"] == 1 and status["cost"]["unknown_records"] == 1 and h["total_tokens"] == 6200, json.dumps(h)),
            case("elapsed-and-cost-incomplete-are-surfaced", isinstance(h["total_elapsed_seconds"], (int, float)) and h["cost_complete"] is False, json.dumps(h)),
            case("render-text-mentions-needs-human-and-goal", "needs human" in text and "g-parked" in text, text),
            case("mcp-status-resource-matches-build", mcp_status["headline"] == h and mcp_status["schema"] == "loop-hybrid-project-status/v1", str(mcp_status["headline"])),
            case("status-resource-gated-without-goal-store", gated.get("error", {}).get("code") == -32602, str(gated)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-project-status",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "read-only projection; LH SQLite stores remain the source of truth",
            "SH cross-project rollup and a richer visual view are later work",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

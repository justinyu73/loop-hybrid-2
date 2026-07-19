#!/usr/bin/env python3
"""Canary for the command ingress (command down) and goal-status report (report up)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from command_ingress import command_status, submit_command
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from mcp_server import dispatch
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _rejects(fn) -> tuple[bool, str]:
    try:
        fn()
        return False, "no error raised"
    except (ValueError, KeyError) as exc:
        return True, f"{type(exc).__name__}: {exc}"


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        goal_store = GoalStore(root / "goals")
        payload = {"campaign_id": "camp-1", "stage_id": "s1"}

        received = submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-1", payload=payload)
        reused = submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-1", payload=payload)

        diff_payload_rejected, diff_detail = _rejects(
            lambda: submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-1", payload={"campaign_id": "camp-1", "stage_id": "s2"})
        )
        bad_type_rejected, bad_type_detail = _rejects(
            lambda: submit_command(goal_store, source="example-commander", event_type="not_a_type", event_id="evt-2", payload=payload)
        )
        missing_field_rejected, missing_detail = _rejects(
            lambda: submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-3", payload={"campaign_id": "camp-1"})
        )

        run_store = RunStore(root / "runs")
        knowledge_store = KnowledgeStore(root / "knowledge")
        goals_read = dispatch({"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": {"uri": "lh://runtime/goals"}}, run_store, knowledge_store, goal_store)
        goals_summary = json.loads(goals_read["result"]["contents"][0]["text"])
        status_call = dispatch({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "lh_goal_status", "arguments": {"event_id": "evt-1"}}}, run_store, knowledge_store, goal_store)
        status_view = json.loads(status_call["result"]["content"][0]["text"])
        # Without goal_store the goals report must stay unavailable (backward-compatible gating).
        gated = dispatch({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "lh_goal_status", "arguments": {"event_id": "evt-1"}}}, run_store, knowledge_store)

        cases = [
            case("valid-submit-creates-one-received-event", received["status"] == "received" and received["state"] == "event_received" and goal_store.summary()["event_count"] == 1, str(received)),
            case("idempotent-replay-reuses-without-second-event", reused["status"] == "reused" and goal_store.summary()["event_count"] == 1, str(reused)),
            case("same-key-different-payload-is-rejected-closed", diff_payload_rejected, diff_detail),
            case("bad-event-type-and-missing-field-are-rejected", bad_type_rejected and missing_field_rejected, f"{bad_type_detail} | {missing_detail}"),
            case("report-up-reads-goal-and-event-state", goals_summary["event_count"] == 1 and status_view["event_id"] == "evt-1" and status_view["event_state"] == "event_received", f"{goals_summary} | {status_view}"),
            case("goal-report-is-gated-without-goal-store", gated["result"]["isError"] is True, str(gated["result"])),
        ]

        submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-4", payload=payload)
        goal_store.create_candidate("evt-4", goal_id="goal-4", campaign_id="camp-1", stage_id="s1", goal={"lamp": "gate-pack"})
        status_bound = command_status(goal_store, "evt-4")
        status_unknown = command_status(goal_store, "evt-never-submitted")
        cases += [
            case("status-reads-event-and-goal-state", status_bound["schema"] == "lh-command-status/v1" and status_bound["event_state"] == "candidate" and status_bound["goal_id"] == "goal-4" and status_bound["goal_state"] == "candidate", str(status_bound)),
            case("status-of-unknown-key-is-unknown-not-error", status_unknown["event_state"] == "unknown" and status_unknown["goal_id"] is None and status_unknown["goal_state"] is None, str(status_unknown)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-command-ingress",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "ingress only records a bounded event; admission, execution, and promotion remain later LH ports",
            "no executor, driver, provider, or GitHub path is wired by this node",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

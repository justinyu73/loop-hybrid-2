#!/usr/bin/env python3
"""Canary for the command ingress (command down) and goal-status report (report up)."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from command_ingress import command_status, submit_command
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from mcp_server import dispatch
from native_delivery_fixture import make_native_run
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

        sh_store = GoalStore(root / "sh-goals")
        sh_missing_task_rejected, sh_missing_task_detail = _rejects(
            lambda: submit_command(
                sh_store,
                source="external_hub",
                event_type="manual_intent",
                event_id="evt-sh-missing-task",
                payload={"campaign_id": "camp-1", "stage_id": "s1"},
            )
        )
        submit_command(
            sh_store,
            source="external_hub",
            event_type="manual_intent",
            event_id="evt-sh-task",
            payload={
                "campaign_id": "camp-1",
                "stage_id": "s1",
                "task_id": "task-1",
            },
        )
        sh_status = command_status(sh_store, "evt-sh-task")
        cases.append(
            case(
                "external-hub-intent-requires-and-projects-task-id",
                sh_missing_task_rejected
                and sh_store.summary()["event_count"] == 1
                and sh_status["event"]["task_id"] == "task-1",
                f"{sh_missing_task_detail} | {sh_status}",
            )
        )

        submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-4", payload=payload)
        goal_store.create_candidate("evt-4", goal_id="goal-4", campaign_id="camp-1", stage_id="s1", goal={"lamp": "gate-pack"})
        status_bound = command_status(goal_store, "evt-4")
        status_unknown = command_status(goal_store, "evt-never-submitted")
        cases += [
            case("status-reads-event-and-goal-state", status_bound["schema"] == "lh-command-status/v1" and status_bound["event_state"] == "candidate" and status_bound["goal_id"] == "goal-4" and status_bound["goal_state"] == "candidate", str(status_bound)),
            case("status-of-unknown-key-is-unknown-not-error", status_unknown["event_state"] == "unknown" and status_unknown["goal_id"] is None and status_unknown["goal_state"] is None, str(status_unknown)),
        ]

        submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-chain", payload=payload)
        derived = goal_store.record_event(
            event_id="derived-evt-chain", idempotency_key="intent-derived:evt-chain",
            source="manual_intent", event_type="goal_candidate",
            payload={"source_event_key": "evt-chain"},
        )
        goal_store.transition_event("evt-chain", "completed", result={"derived_event_key": derived["event_key"]})
        goal_store.create_candidate("intent-derived:evt-chain", goal_id="goal-chain", campaign_id="camp-1", stage_id="s1", goal={"lamp": "gate-pack"})
        repository_root = HERE.parent
        base_revision = subprocess.run(["git", "-C", str(repository_root), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        run_id = make_native_run(
            run_store, repository_root, base_revision, "goal-chain", "command-ingress",
            [{"id": "command-status-check", "commands": [{"id": "repo", "argv": ["test", "-d", ".git"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
            ["git", "rev-parse", "HEAD"], ["loop-hybrid/"], 4,
            goal={"goal_id": "goal-chain", "admission_envelope": {"allowed_paths": ["loop-hybrid/"]}},
            run_id="run-chain",
        )["run_id"]
        goal_store.activate_with_run("goal-chain", run_id, event_key="intent-derived:evt-chain")
        ordinal = run_store.begin_attempt(run_id, "workspace://run-chain/1")
        receipt_ref = run_store.write_artifact(run_id, ordinal, "receipt.json", json.dumps({
            "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
            "workspace": {"ref": "workspace://run-chain/1", "disposable": True, "disposed": True, "base_revision": "base"},
            "provider": {"summary": "canary", "artifact": {"ref": "artifacts/run-chain/1/provider.json", "digest": "sha256:" + "a" * 64}},
            "usage": {"input_tokens": 3, "cached_input_tokens": 1, "output_tokens": 2, "total_tokens": 5},
            "diff": {"ref": "artifacts/run-chain/1/diff.patch", "digest": "sha256:" + "b" * 64},
            "verification": {"argv": ["true"], "exit_code": 0, "stdout": {"ref": "artifacts/run-chain/1/verifier.stdout", "digest": "sha256:" + "c" * 64}, "stderr": {"ref": "artifacts/run-chain/1/verifier.stderr", "digest": "sha256:" + "d" * 64}},
        }, sort_keys=True))
        run_store.finish_attempt(run_id, ordinal, state="human_required", receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"])
        status_chain = command_status(goal_store, "evt-chain", run_store)
        execution = status_chain["execution"]
        cases += [
            case("status-follows-derived-event-to-goal", status_chain["goal_id"] == "goal-chain" and execution["event_chain"] == ["evt-chain", "intent-derived:evt-chain"], str(status_chain)),
            case("status-projects-lh-run-attempt-receipt", execution["run_id"] == "run-chain" and execution["attempt"] == 1 and execution["receipt"]["digest"] == receipt_ref["digest"], str(execution)),
        ]

        # A recurring/standing intent may revive a completed Goal through a
        # new derived event.  Production admission passes that event key
        # explicitly; status must follow the fresh command rather than the
        # Goal's historical source event.
        submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-revive", payload=payload)
        derived_revive = goal_store.record_event(
            event_id="derived-evt-revive", idempotency_key="intent-derived:evt-revive",
            source="manual_intent", event_type="goal_candidate",
            payload={"candidate": {"goal_id": "goal-chain", "campaign_id": "camp-1", "stage_id": "s1", "goal": {"lamp": "gate-pack"}}, "source_event_key": "evt-revive"},
        )
        goal_store.transition_event("evt-revive", "completed", result={"derived_event_key": derived_revive["event_key"]})
        goal_store.transition_event("intent-derived:evt-revive", "completed", result={"admission": {"goal_id": "goal-chain", "run_id": "run-chain"}})
        status_revive = command_status(goal_store, "evt-revive", run_store)
        cases.append(case("status-keeps-revived-command-correlation", status_revive["goal_id"] == "goal-chain" and status_revive["execution"]["run_id"] == "run-chain", str(status_revive)))

        # A rejected/replayed candidate may still carry the compiler's goal_id
        # in its payload.  That proposal is not a persisted binding; status
        # must not project the older goal's Run/Attempt/receipt through it.
        submit_command(goal_store, source="example-commander", event_type="manual_intent", event_id="evt-stale-candidate", payload=payload)
        derived_stale = goal_store.record_event(
            event_id="derived-evt-stale-candidate", idempotency_key="intent-derived:evt-stale-candidate",
            source="manual_intent", event_type="goal_candidate",
            payload={"candidate": {"goal_id": "goal-chain", "campaign_id": "camp-1", "stage_id": "s1", "goal": {"lamp": "gate-pack"}}, "source_event_key": "evt-stale-candidate"},
        )
        goal_store.transition_event("evt-stale-candidate", "completed", result={"derived_event_key": derived_stale["event_key"]})
        goal_store.transition_event(
            "intent-derived:evt-stale-candidate", "human_required",
            result={"status": "human_required", "reason": "goal exists from a different source event and is not re-admissible"},
        )
        status_stale = command_status(goal_store, "evt-stale-candidate", run_store)
        stale_execution = status_stale["execution"]
        cases.append(
            case(
                "status-does-not-bind-unadmitted-candidate",
                status_stale["goal_id"] is None
                and stale_execution["status"] == "not_started"
                and stale_execution["run_id"] is None
                and stale_execution["receipt"] is None,
                str(status_stale),
            )
        )

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

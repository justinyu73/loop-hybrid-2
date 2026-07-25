#!/usr/bin/env python3
"""Canary for the command ingress (command down) and goal-status report (report up)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from command_ingress import _digest, command_status, submit_command
from controller import LoopController
from goal_store import GoalStore
from goal_loop_worker import GoalLoopWorker, process_control_event_by_key
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
        run_id = run_store.create_run(goal={"goal_id": "goal-chain", "admission_envelope": {"allowed_paths": []}}, source_repo=root, base_revision="base", run_id="run-chain")
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
        run_store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"])
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

        # SH control-bus events are durable signals, not Goal admission.
        control_goal_count_before = sum(
            len(goal_store.goals_in_state(state))
            for state in ("candidate", "active", "human_required", "completed", "stopped")
        )
        control_base = {
            "campaign_id": "camp-control",
            "project_id": "fixture-project",
            "correlation_id": "control-1",
            "context_ratio": 0.40,
            "safe_point_observed": False,
        }
        pressure = submit_command(
            goal_store, source="external_hub", event_type="context_pressure",
            event_id="control-1", payload=control_base,
        )
        pressure_replay = submit_command(
            goal_store, source="external_hub", event_type="context_pressure",
            event_id="control-1", payload=control_base,
        )
        packet = {
            "schema": "external-hub-handoff-packet/v1",
            "project_id": "fixture-project",
            "correlation_id": "rollover-no-safe",
            "next_action": "bounded canary action",
            "old_session_handle": "term-predecessor-1",
            "checkpoint_digest": "sha256:checkpoint-proof",
            "predecessor_identity": {"identity_digest": "sha256:predecessor-identity"},
        }
        rollover_payload = {
            "campaign_id": "camp-control",
            "project_id": "fixture-project",
            "correlation_id": "rollover-no-safe",
            "context_ratio": 0.50,
            "safe_point_observed": False,
            "handoff_packet": packet,
        }
        rollover = submit_command(
            goal_store, source="external_hub", event_type="rollover_requested",
            event_id="rollover-no-safe", payload=rollover_payload,
        )
        worker = GoalLoopWorker(
            goal_store=goal_store,
            run_store=run_store,
            controller=LoopController(run_store, root / "control-workspaces"),
            compilers={}, execution_context={},
        )
        pressure_result = worker._process_event(goal_store.get_event("control-1"))
        rollover_result = worker._process_event(goal_store.get_event("rollover-no-safe"))
        valid_rollover = submit_command(
            goal_store, source="external_hub", event_type="rollover_requested",
            event_id="rollover-safe", payload={
                **rollover_payload,
                "correlation_id": "rollover-safe",
                "safe_point_observed": True,
                "handoff_packet": {**packet, "correlation_id": "rollover-safe"},
            },
        )
        accepted_result = worker._process_event(goal_store.get_event("rollover-safe"))
        accepted_status = command_status(goal_store, "rollover-safe")
        predecessor_digest = "sha256:predecessor-identity"
        successor_digest = "sha256:successor-identity"
        pair_digest = _digest({"predecessor": predecessor_digest, "successor": successor_digest})
        heartbeat_payload = {
            "campaign_id": "camp-control",
            "project_id": "fixture-project",
            "correlation_id": "rollover-safe",
            "rollover_event_key": "rollover-safe",
            "heartbeat_id": "hb-rollover-safe-1",
            "successor_handle": "term-successor-1",
            "provider": "codex",
            "observed_at": "2026-07-22T00:00:00+00:00",
            "identity_pair_digest": pair_digest,
            "successor_identity_digest": successor_digest,
            "heartbeat_proof": {
                "schema": "orca-successor-heartbeat/v1",
                "turn_completed": True,
                "output_digest": "sha256:successor-proof",
            },
        }
        heartbeat = submit_command(
            goal_store, source="orca", event_type="successor_heartbeat",
            event_id="heartbeat-rollover-safe", payload=heartbeat_payload,
        )
        heartbeat_result = worker._process_event(goal_store.get_event("heartbeat-rollover-safe"))
        heartbeat_replay = submit_command(
            goal_store, source="orca", event_type="successor_heartbeat",
            event_id="heartbeat-rollover-safe", payload=heartbeat_payload,
        )
        stop_evidence = {
            "schema": "orca-stop-evidence/v1",
            "observed": True,
            "action": "terminal_close_exact_pane",
            "terminal_handle": "term-predecessor-1",
            "post_close_absent": True,
            "predecessor_identity_digest": predecessor_digest,
            "successor_identity_digest": successor_digest,
            "identity_pair_digest": pair_digest,
            "routing_switch_digest": "sha256:routing-switch",
        }
        identity_stage = {
            "predecessor_identity_digest": predecessor_digest,
            "successor_identity_digest": successor_digest,
            "pair_digest": pair_digest,
        }
        route_stage = {"routing_switch_digest": "sha256:routing-switch"}
        close_stage = {
            "predecessor_handle": "term-predecessor-1",
            "checkpoint_digest": "sha256:checkpoint-proof",
            "identity_pair_digest": pair_digest,
            "routing_switch_digest": "sha256:routing-switch",
        }
        post_stage = {
            "old_session_handle": "term-predecessor-1",
            "post_close_absent": True,
            "predecessor_identity_digest": predecessor_digest,
            "successor_identity_digest": successor_digest,
            "stop_evidence": stop_evidence,
        }
        transaction_path = root / "rollover-transaction.json"
        transaction_path.write_text(
            json.dumps(
                {
                    "schema": "external-hub-orca-rollover-transaction/v1",
                    "project_id": "fixture-project",
                    "campaign_id": "camp-control",
                    "correlation_id": "rollover-safe",
                    "pair_digest": pair_digest,
                    "current_stage": "post_close_readback",
                    "evidence": {
                        "identity_bound": {"digest": _digest(identity_stage), "value": identity_stage},
                        "routing_switched": {"digest": _digest(route_stage), "value": route_stage},
                        "predecessor_close_intent": {"digest": _digest(close_stage), "value": close_stage},
                        "post_close_readback": {"digest": _digest(post_stage), "value": post_stage},
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        finalize_payload = {
            "campaign_id": "camp-control",
            "project_id": "fixture-project",
            "correlation_id": "rollover-safe",
            "heartbeat_event_key": "heartbeat-rollover-safe",
            "old_session_handle": "term-predecessor-1",
            "old_session_stopped": True,
            "checkpoint_digest": "sha256:checkpoint-proof",
            "next_action_digest": "sha256:next-action-proof",
            "identity_pair_digest": pair_digest,
            "routing_switch_digest": "sha256:routing-switch",
            "successor_identity_digest": successor_digest,
            "predecessor_identity_digest": predecessor_digest,
            "post_close_digest": _digest(post_stage),
            "transaction_path": str(transaction_path),
            "stop_evidence": stop_evidence,
        }
        finalized = submit_command(
            goal_store, source="orca", event_type="rollover_finalized",
            event_id="finalize-rollover-safe", payload=finalize_payload,
        )
        finalized_result = worker._process_event(goal_store.get_event("finalize-rollover-safe"))
        transaction_original = transaction_path.read_text(encoding="utf-8")
        transaction_tampered = json.loads(transaction_original)
        transaction_tampered["evidence"]["post_close_readback"]["value"][
            "post_close_absent"
        ] = False
        transaction_path.write_text(
            json.dumps(transaction_tampered, sort_keys=True),
            encoding="utf-8",
        )
        transaction_tamper_rejected, transaction_tamper_detail = _rejects(
            lambda: submit_command(
                goal_store,
                source="orca",
                event_type="rollover_finalized",
                event_id="finalize-rollover-safe-tampered-transaction",
                payload=finalize_payload,
            )
        )
        transaction_path.write_text(transaction_original, encoding="utf-8")
        finalize_without_heartbeat, finalize_without_heartbeat_detail = _rejects(
            lambda: submit_command(
                goal_store, source="orca", event_type="rollover_finalized",
                event_id="finalize-without-heartbeat", payload={
                    **finalize_payload,
                    "heartbeat_event_key": "missing-heartbeat",
                },
            )
        )
        invalid_control, invalid_detail = _rejects(
            lambda: submit_command(
                goal_store, source="external_hub", event_type="rollover_requested",
                event_id="rollover-bad", payload={
                    **rollover_payload, "handoff_packet": {"schema": "wrong"},
                },
            )
        )
        foreground_event = submit_command(
            goal_store,
            source="external_hub",
            event_type="context_pressure",
            event_id="foreground-control-1",
            payload={
                **control_base,
                "correlation_id": "foreground-control-1",
            },
        )
        foreground_processed = process_control_event_by_key(
            goal_store,
            event_key="foreground-control-1",
            holder="foreground-canary",
        )
        foreground_replay = process_control_event_by_key(
            goal_store,
            event_key="foreground-control-1",
            holder="foreground-canary",
        )
        non_control = submit_command(
            goal_store,
            source="scheduler",
            event_type="scheduled_tick",
            event_id="foreground-non-control",
            payload={"campaign_id": "camp-control"},
        )
        foreground_non_control, foreground_non_control_detail = _rejects(
            lambda: process_control_event_by_key(
                goal_store,
                event_key=str(non_control["event_key"]),
                holder="foreground-canary",
            )
        )
        control_goal_count = sum(
            len(goal_store.goals_in_state(state))
            for state in ("candidate", "active", "human_required", "completed", "stopped")
        )
        cases += [
            case("context-pressure-control-is-received", pressure["status"] == "received" and pressure_replay["status"] == "reused", str(pressure_replay)),
            case("context-pressure-worker-acks-without-goal", pressure_result["status"] == "context_pressure_ack" and control_goal_count == control_goal_count_before, str(pressure_result)),
            case("rollover-without-safe-point-is-human-required", rollover["status"] == "received" and rollover_result["status"] == "human_required" and rollover_result["old_session_stopped"] is False, str(rollover_result)),
            case("rollover-safe-point-stays-host-handoff-pending", valid_rollover["status"] == "received" and accepted_result["status"] == "rollover_accepted" and accepted_result["successor_heartbeat"] == "not_observed", str(accepted_result)),
            case("control-status-reads-durable-rollover-receipt", accepted_status["event_state"] == "completed" and accepted_status["event"]["event_type"] == "rollover_requested" and accepted_status["event"]["payload_digest"].startswith("sha256:") and accepted_status["control_result"]["status"] == "rollover_accepted" and accepted_status["control_result"]["receipt"]["schema"] == "lh-rollover-control-receipt/v2" and accepted_status["control_result"]["receipt"]["status"] == "rollover_accepted", str(accepted_status)),
            case("successor-heartbeat-opens-stop-gate-with-receipt", heartbeat["status"] == "received" and heartbeat_result["status"] == "successor_heartbeat_observed" and heartbeat_result["old_session_stop_allowed"] is True and heartbeat_result["receipt"]["schema"] == "lh-rollover-control-receipt/v2" and heartbeat_result["receipt"]["digest"].startswith("sha256:") and heartbeat_result["heartbeat"]["identity_pair_digest"] == pair_digest, str(heartbeat_result)),
            case("successor-heartbeat-replay-is-idempotent", heartbeat_replay["status"] == "reused", str(heartbeat_replay)),
            case("rollover-finalized-requires-heartbeat-and-records-stop", finalized["status"] == "received" and finalized_result["status"] == "rollover_finalized" and finalized_result["old_session_stopped"] is True and finalized_result["receipt"]["schema"] == "lh-rollover-control-receipt/v2" and finalized_result["receipt"]["digest"].startswith("sha256:") and finalized_result["stop"]["routing_switch_digest"] == "sha256:routing-switch", str(finalized_result)),
            case(
                "rollover-finalized-rejects-tampered-transaction-evidence",
                transaction_tamper_rejected,
                transaction_tamper_detail,
            ),
            case("rollover-finalized-without-heartbeat-rejected-closed", finalize_without_heartbeat, finalize_without_heartbeat_detail),
            case("rollover-invalid-packet-rejected-closed", invalid_control, invalid_detail),
            case("foreground-control-processes-only-exact-key", foreground_event["status"] == "received" and foreground_processed["status"] == "processed" and foreground_processed["event_state"] == "completed", str(foreground_processed)),
            case("foreground-control-replay-is-action-free", foreground_replay["status"] == "reused" and foreground_replay["event_state"] == "completed", str(foreground_replay)),
            case("foreground-control-rejects-non-control-event", foreground_non_control, foreground_non_control_detail),
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

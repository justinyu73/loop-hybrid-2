#!/usr/bin/env python3
"""Command ingress — a bounded, commander-agnostic entry for issuing a goal event into LH.

This is the "command down" half of the SH<->LH bus. It is deliberately NOT bound
to any single commander (an external hub is one client of many: github, scheduler,
another front-end) and NOT bound to any model, provider, or file path. It only
validates a bounded event contract and delegates durable idempotency to
GoalStore.record_event; it never admits, runs, or promotes anything.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore
from run_store import RunStore
import value_reducer

# Event sources are the GoalLifecycle v1 event types, not commander-specific.
SUPPORTED_EVENT_TYPES = {
    "manual_intent",
    "stage_completion",
    "scheduled_tick",
    "external_verdict",
    "restart",
    "context_pressure",
    "rollover_requested",
    "successor_heartbeat",
    "rollover_finalized",
}

CONTROL_EVENT_TYPES = {
    "context_pressure",
    "rollover_requested",
    "successor_heartbeat",
    "rollover_finalized",
}
SAFE_OBSERVATION_POLICY_DECLARATION = (
    "provider message bodies",
    "secrets",
    "credentials",
)


def _digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _validate_rollover_transaction_evidence(payload: dict[str, Any]) -> None:
    path = Path(str(payload["transaction_path"]))
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"rollover_finalized transaction evidence is unreadable: {type(exc).__name__}") from exc
    if len(raw) > 1_000_000:
        raise ValueError("rollover_finalized transaction evidence exceeds bounded size")
    try:
        transaction = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("rollover_finalized transaction evidence is invalid JSON") from exc
    if not isinstance(transaction, dict) or transaction.get("schema") != "external-hub-orca-rollover-transaction/v1":
        raise ValueError("rollover_finalized transaction evidence schema is invalid")
    expected = {
        "project_id": payload.get("project_id"),
        "campaign_id": payload.get("campaign_id"),
        "correlation_id": payload.get("correlation_id"),
        "pair_digest": payload.get("identity_pair_digest"),
    }
    if any(transaction.get(name) != value for name, value in expected.items()):
        raise ValueError("rollover_finalized transaction identity is invalid")
    if transaction.get("current_stage") not in {
        "post_close_readback",
        "lh_finalize_intent",
        "rollover_finalized",
    }:
        raise ValueError("rollover_finalized transaction has no post-close stage")
    evidence = transaction.get("evidence") if isinstance(transaction.get("evidence"), dict) else {}

    def stage(name: str) -> dict[str, Any]:
        row = evidence.get(name) if isinstance(evidence.get(name), dict) else {}
        value = row.get("value") if isinstance(row.get("value"), dict) else {}
        if not value or row.get("digest") != _digest(value):
            raise ValueError(f"rollover_finalized transaction stage is invalid: {name}")
        return {"digest": row["digest"], "value": value}

    identity = stage("identity_bound")["value"]
    if (
        identity.get("predecessor_identity_digest") != payload.get("predecessor_identity_digest")
        or identity.get("successor_identity_digest") != payload.get("successor_identity_digest")
        or identity.get("pair_digest") != payload.get("identity_pair_digest")
    ):
        raise ValueError("rollover_finalized transaction identity stage conflicts with payload")
    routing = stage("routing_switched")["value"]
    if routing.get("routing_switch_digest") != payload.get("routing_switch_digest"):
        raise ValueError("rollover_finalized transaction route stage conflicts with payload")
    close_intent = stage("predecessor_close_intent")["value"]
    if (
        close_intent.get("predecessor_handle") != payload.get("old_session_handle")
        or close_intent.get("checkpoint_digest") != payload.get("checkpoint_digest")
        or close_intent.get("identity_pair_digest") != payload.get("identity_pair_digest")
        or close_intent.get("routing_switch_digest") != payload.get("routing_switch_digest")
    ):
        raise ValueError("rollover_finalized transaction close intent conflicts with payload")
    post_close = stage("post_close_readback")
    if post_close["digest"] != payload.get("post_close_digest"):
        raise ValueError("rollover_finalized post-close digest conflicts with transaction")
    post_value = post_close["value"]
    if (
        post_value.get("old_session_handle") != payload.get("old_session_handle")
        or post_value.get("post_close_absent") is not True
        or post_value.get("predecessor_identity_digest") != payload.get("predecessor_identity_digest")
        or post_value.get("successor_identity_digest") != payload.get("successor_identity_digest")
        or post_value.get("stop_evidence") != payload.get("stop_evidence")
    ):
        raise ValueError("rollover_finalized post-close stage conflicts with payload")

# Event types that advance bounded campaign work must name a stage.
STAGE_REQUIRED_EVENT_TYPES = {"manual_intent", "stage_completion"}


def _validate_control_payload(event_type: str, payload: dict[str, Any], *, goal_store: GoalStore | None = None) -> None:
    """Validate bounded SH control and Orca host-evidence events."""
    required = ["campaign_id", "project_id", "correlation_id"]
    if event_type in {"context_pressure", "rollover_requested"}:
        required.append("context_ratio")
    missing = [name for name in required if not str(payload.get(name) or "").strip()]
    if missing:
        raise ValueError(f"payload missing required control fields: {missing}")
    if "context_ratio" in payload:
        ratio = payload.get("context_ratio")
        if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or not math.isfinite(float(ratio)) or not 0 <= float(ratio) <= 1:
            raise ValueError("context_ratio must be a finite number between 0 and 1")
    if event_type == "rollover_requested" and not isinstance(payload.get("safe_point_observed", False), bool):
        raise ValueError("safe_point_observed must be boolean")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if len(encoded) > 20000:
        raise ValueError("control payload exceeds bounded size")
    scan_payload = json.loads(encoded)
    if event_type == "rollover_requested":
        packet = scan_payload.get("handoff_packet")
        budget = (
            packet.get("observation_budget")
            if isinstance(packet, dict)
            and isinstance(packet.get("observation_budget"), dict)
            else None
        )
        if (
            isinstance(budget, dict)
            and tuple(budget.get("forbidden") or ())
            == SAFE_OBSERVATION_POLICY_DECLARATION
        ):
            # This exact negative policy is canonical handoff metadata, not
            # secret-bearing content. Any marker elsewhere remains rejected.
            budget["forbidden"] = []
    lowered = json.dumps(
        scan_payload,
        ensure_ascii=False,
        sort_keys=True,
    ).lower()
    if any(marker in lowered for marker in ("transcript", "credential", "password", "cookie", "api_key", "private_key")):
        raise ValueError("control payload contains a forbidden secret/transcript marker")
    if event_type == "rollover_requested":
        packet = payload.get("handoff_packet")
        if not isinstance(packet, dict) or packet.get("schema") != "external-hub-handoff-packet/v1":
            raise ValueError("rollover_requested requires a bounded handoff_packet")
        if packet.get("project_id") != payload.get("project_id"):
            raise ValueError("handoff_packet project_id conflicts with project_id")
        if packet.get("correlation_id") not in {None, payload.get("correlation_id") }:
            raise ValueError("handoff_packet correlation_id conflicts with correlation_id")
    if event_type == "successor_heartbeat":
        for name in (
            "rollover_event_key",
            "heartbeat_id",
            "successor_handle",
            "provider",
            "observed_at",
            "identity_pair_digest",
            "successor_identity_digest",
        ):
            if not isinstance(payload.get(name), str) or not payload[name].strip():
                raise ValueError(f"successor_heartbeat requires {name}")
        if not payload["identity_pair_digest"].startswith("sha256:") or not payload["successor_identity_digest"].startswith("sha256:"):
            raise ValueError("successor_heartbeat identity digests must use sha256")
        proof = payload.get("heartbeat_proof")
        if not isinstance(proof, dict) or proof.get("schema") != "orca-successor-heartbeat/v1":
            raise ValueError("successor_heartbeat requires bounded Orca heartbeat proof")
        if proof.get("turn_completed") is not True or proof.get("output_digest", "").startswith("sha256:") is not True:
            raise ValueError("successor_heartbeat proof must contain turn_completed and output_digest")
        if goal_store is not None:
            try:
                rollover = goal_store.get_event(payload["rollover_event_key"])
            except KeyError as exc:
                raise ValueError("successor_heartbeat references an unknown rollover event") from exc
            if rollover.get("event_type") != "rollover_requested" or (rollover.get("payload") or {}).get("correlation_id") != payload.get("correlation_id"):
                raise ValueError("successor_heartbeat rollover correlation is invalid")
    if event_type == "rollover_finalized":
        for name in (
            "heartbeat_event_key",
            "old_session_handle",
            "checkpoint_digest",
            "identity_pair_digest",
            "routing_switch_digest",
            "successor_identity_digest",
            "predecessor_identity_digest",
            "post_close_digest",
            "transaction_path",
        ):
            if not isinstance(payload.get(name), str) or not payload[name].strip():
                raise ValueError(f"rollover_finalized requires {name}")
        for name in (
            "checkpoint_digest",
            "identity_pair_digest",
            "routing_switch_digest",
            "successor_identity_digest",
            "predecessor_identity_digest",
            "post_close_digest",
        ):
            if not payload[name].startswith("sha256:"):
                raise ValueError(f"rollover_finalized {name} must use sha256")
        transaction_path = Path(payload["transaction_path"])
        if not transaction_path.is_absolute() or transaction_path.suffix != ".json":
            raise ValueError("rollover_finalized transaction_path must be an absolute JSON evidence path")
        if payload.get("old_session_stopped") is not True:
            raise ValueError("rollover_finalized requires old_session_stopped=true")
        stop = payload.get("stop_evidence")
        if not isinstance(stop, dict) or stop.get("schema") != "orca-stop-evidence/v1" or stop.get("observed") is not True:
            raise ValueError("rollover_finalized requires bounded Orca stop evidence")
        if stop.get("post_close_absent") is not True or stop.get("terminal_handle") != payload.get("old_session_handle"):
            raise ValueError("rollover_finalized stop evidence does not prove exact post-close absence")
        for name in (
            "predecessor_identity_digest",
            "successor_identity_digest",
            "identity_pair_digest",
            "routing_switch_digest",
        ):
            if stop.get(name) != payload.get(name):
                raise ValueError(f"rollover_finalized stop evidence conflicts with {name}")
        if payload.get("identity_pair_digest") != _digest(
            {
                "predecessor": payload.get("predecessor_identity_digest"),
                "successor": payload.get("successor_identity_digest"),
            }
        ):
            raise ValueError("rollover_finalized identity pair digest is invalid")
        _validate_rollover_transaction_evidence(payload)
        if goal_store is not None:
            try:
                heartbeat = goal_store.get_event(payload["heartbeat_event_key"])
            except KeyError as exc:
                raise ValueError("rollover_finalized references an unknown heartbeat event") from exc
            if heartbeat.get("event_type") != "successor_heartbeat" or (heartbeat.get("payload") or {}).get("correlation_id") != payload.get("correlation_id"):
                raise ValueError("rollover_finalized heartbeat correlation is invalid")
            heartbeat_payload = heartbeat.get("payload") if isinstance(heartbeat.get("payload"), dict) else {}
            if heartbeat_payload.get("identity_pair_digest") != payload.get("identity_pair_digest"):
                raise ValueError("rollover_finalized identity pair conflicts with heartbeat")
            if heartbeat_payload.get("successor_identity_digest") != payload.get("successor_identity_digest"):
                raise ValueError("rollover_finalized successor identity conflicts with heartbeat")
            rollover_key = heartbeat_payload.get("rollover_event_key")
            try:
                rollover = goal_store.get_event(str(rollover_key))
            except KeyError as exc:
                raise ValueError("rollover_finalized heartbeat references an unknown rollover event") from exc
            rollover_payload = rollover.get("payload") if isinstance(rollover.get("payload"), dict) else {}
            packet = rollover_payload.get("handoff_packet") if isinstance(rollover_payload.get("handoff_packet"), dict) else {}
            if packet.get("old_session_handle") != payload.get("old_session_handle"):
                raise ValueError("rollover_finalized predecessor conflicts with rollover request")
            predecessor = packet.get("predecessor_identity") if isinstance(packet.get("predecessor_identity"), dict) else {}
            if predecessor.get("identity_digest") != payload.get("predecessor_identity_digest"):
                raise ValueError("rollover_finalized predecessor identity conflicts with rollover request")
            if packet.get("checkpoint_digest") != payload.get("checkpoint_digest"):
                raise ValueError("rollover_finalized checkpoint conflicts with rollover request")


def submit_command(
    goal_store: GoalStore,
    *,
    source: str,
    event_type: str,
    event_id: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Validate the bounded command contract and record one durable goal event.

    Returns the GoalStore result (`status` is `received` for a new event or
    `reused` for an idempotent replay). Raises ValueError on a contract
    violation; the caller is rejected closed and nothing is recorded.
    """
    if not isinstance(source, str) or not source.strip():
        raise ValueError("source is required and identifies the commander")
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type!r}")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id is required")
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")

    if event_type in CONTROL_EVENT_TYPES:
        _validate_control_payload(event_type, payload, goal_store=goal_store)
        # Control events are policy signals, not Goal admission requests.
        return goal_store.record_event(
            event_id=event_id,
            source=source,
            event_type=event_type,
            payload=payload,
            idempotency_key=idempotency_key,
        )

    missing: list[str] = []
    if not str(payload.get("campaign_id") or "").strip():
        missing.append("campaign_id")
    if event_type in STAGE_REQUIRED_EVENT_TYPES and not str(payload.get("stage_id") or "").strip():
        missing.append("stage_id")
    if (
        source == "external_hub"
        and event_type == "manual_intent"
        and not str(payload.get("task_id") or "").strip()
    ):
        missing.append("task_id")
    if missing:
        raise ValueError(f"payload missing required fields: {missing}")

    return goal_store.record_event(
        event_id=event_id,
        source=source,
        event_type=event_type,
        payload=payload,
        idempotency_key=idempotency_key,
    )


def _event_goal_id(event: dict[str, Any]) -> str | None:
    """Read an explicitly persisted goal link across the admission shapes.

    Older recurring-goal events may have a null ``goal_id`` column because
    admission reused the Goal's historical source event.  Their result still
    carries the LH-owned admission/candidate link; reading that explicit
    pointer keeps status backward-compatible without matching by campaign or
    stage names.  A candidate embedded in the payload is only a proposal: it
    is not a binding until GoalStore persists the event's ``goal_id`` or a
    successful admission result names one.  A refused admission may echo the
    candidate goal for diagnostics, but its missing ``run_id`` is not a
    command-to-Goal binding.
    """
    direct = event.get("goal_id")
    if isinstance(direct, str) and direct:
        return direct
    result = event.get("result") if isinstance(event.get("result"), dict) else {}
    admission = result.get("admission") if isinstance(result.get("admission"), dict) else {}
    for container in (admission, result):
        value = container.get("goal_id")
        if isinstance(value, str) and value:
            if container is admission and not isinstance(container.get("run_id"), str):
                continue
            if container is admission and not container["run_id"].strip():
                continue
            return value
    return None


def _linked_goal_event(goal_store: GoalStore, event: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Follow the bounded worker handoff from a command to its goal event.

    A manual intent is first recorded under the command's idempotency key.
    The worker may then create a deterministic ``intent-derived:<key>`` event
    and bind the Goal there.  Following that explicit result keeps the
    correlation chain intact without guessing from campaign or stage names.
    """
    current = event
    chain = [event["event_key"]]
    seen = set(chain)
    for _ in range(4):
        if _event_goal_id(current) is not None:
            return current, chain
        result = current.get("result")
        next_key = result.get("derived_event_key") if isinstance(result, dict) else None
        if not isinstance(next_key, str) or not next_key or next_key in seen:
            return current, chain
        try:
            current = goal_store.get_event(next_key)
        except KeyError:
            return current, chain
        chain.append(next_key)
        seen.add(next_key)
    return current, chain


def _execution_projection(goal_store: GoalStore, run_store: RunStore | None, event: dict[str, Any], chain: list[str]) -> dict[str, Any]:
    """Project LH-owned run/attempt/receipt evidence for one command.

    This function only reads LH stores.  The optional run store keeps old
    command ingress fixtures and clients valid when only goal state exists.
    """
    goal_id = _event_goal_id(event)
    goal = None
    if isinstance(goal_id, str) and goal_id:
        try:
            goal = goal_store.get_goal(goal_id)
        except KeyError:
            goal = None
    run_id = goal.get("run_id") if isinstance(goal, dict) else None
    projection: dict[str, Any] = {
        "status": "not_started" if not run_id else "run_linked",
        "event_chain": chain,
        "source_event_key": chain[0],
        "goal_event_key": event.get("event_key"),
        "goal_id": goal_id,
        "goal_state": goal.get("state") if isinstance(goal, dict) else None,
        "run_id": run_id,
        "run_state": None,
        "attempt": None,
        "attempt_state": None,
        "workspace_ref": None,
        "receipt": None,
        "derived_verdict": None,
    }
    if not isinstance(run_id, str) or not run_id or run_store is None:
        return projection
    try:
        run = run_store.get_run(run_id)
    except KeyError:
        projection["status"] = "run_missing"
        return projection
    attempt = run_store.latest_attempt(run_id)
    receipt = run_store.latest_receipt(run_id)
    projection.update({
        "status": "receipt_available" if receipt else "attempt_started" if attempt else "run_linked",
        "run_state": run.get("state"),
        "attempt": attempt.get("ordinal") if attempt else None,
        "attempt_state": attempt.get("state") if attempt else None,
        "workspace_ref": attempt.get("workspace_ref") if attempt else None,
    })
    if receipt:
        projection["receipt"] = {
            "ref": receipt.get("receipt_ref"),
            "digest": receipt.get("receipt_digest"),
        }
        try:
            projection["derived_verdict"] = value_reducer.value_evidence_for_run(run_store, run_id, goal_store=goal_store)
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            projection["derived_verdict"] = {"verdict": "RED", "reasons": [f"projection_error: {type(exc).__name__}"]}
    return projection


def command_status(goal_store: GoalStore, event_key: str, run_store: RunStore | None = None) -> dict[str, Any]:
    """Read back one event, its bound goal, and optional execution evidence.

    An unknown key is a plain answer, not an error: a commander polling for a
    command it never submitted (or one rejected before any record) must get a
    stable shape back with a zero exit code.
    """
    try:
        event = goal_store.get_event(event_key)
    except KeyError:
        return {
            "schema": "lh-command-status/v1",
            "event_key": event_key,
            "event_state": "unknown",
            "control_result": None,
            "goal_id": None,
            "goal_state": None,
            "execution": {
                "status": "unknown",
                "event_chain": [],
                "source_event_key": event_key,
                "goal_event_key": None,
                "goal_id": None,
                "goal_state": None,
                "run_id": None,
                "run_state": None,
                "attempt": None,
                "attempt_state": None,
                "workspace_ref": None,
                "receipt": None,
                "derived_verdict": None,
            },
        }
    goal_event, chain = _linked_goal_event(goal_store, event)
    goal_id = _event_goal_id(goal_event)
    goal_state = None
    if goal_id is not None:
        goal_state = goal_store.get_goal(goal_id)["state"]
    control_result = event.get("result") if event.get("event_type") in CONTROL_EVENT_TYPES and isinstance(event.get("result"), dict) else None
    return {
        "schema": "lh-command-status/v1",
        "event_key": event_key,
        "event_state": event["state"],
        "event": {
            "source": event.get("source"),
            "event_type": event.get("event_type"),
            "payload_digest": event.get("payload_digest"),
            "campaign_id": (event.get("payload") or {}).get("campaign_id"),
            "project_id": (event.get("payload") or {}).get("project_id"),
            "correlation_id": (event.get("payload") or {}).get("correlation_id"),
            "task_id": (event.get("payload") or {}).get("task_id"),
        },
        "control_result": control_result,
        "goal_id": goal_id,
        "goal_state": goal_state,
        "execution": _execution_projection(goal_store, run_store, goal_event, chain),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Issue one bounded goal event into LH (command down)")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--run-store", default=None, help="optional LH RunStore for run/attempt/receipt projection")
    parser.add_argument("--status", action="store_true", help="read back one event by key instead of submitting")
    parser.add_argument(
        "--process-control",
        action="store_true",
        help="claim and process only --event-key; never scan or dispatch Goal/Run work",
    )
    parser.add_argument("--event-key", default=None, help="event key to read in --status mode")
    parser.add_argument("--holder", default="foreground-control", help="bounded lease holder for --process-control")
    parser.add_argument("--source", help="commander id, e.g. hub / github / scheduler")
    parser.add_argument("--event-type", choices=sorted(SUPPORTED_EVENT_TYPES))
    parser.add_argument("--event-id")
    parser.add_argument("--payload", help="JSON object with at least campaign_id")
    parser.add_argument("--idempotency-key", default=None)
    args = parser.parse_args(argv)

    if args.process_control:
        if not args.event_key:
            print(json.dumps({"status": "rejected", "error": "--process-control requires --event-key"}, ensure_ascii=False))
            return 1
        try:
            from goal_loop_worker import process_control_event_by_key

            result = process_control_event_by_key(
                GoalStore(Path(args.goal_store)),
                event_key=args.event_key,
                holder=args.holder,
            )
        except (ValueError, KeyError) as exc:
            print(json.dumps({"status": "rejected", "error": str(exc)}, ensure_ascii=False))
            return 1
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result.get("status") in {"processed", "reused", "busy"} else 1

    if args.status:
        if not args.event_key:
            print(json.dumps({"status": "rejected", "error": "--status requires --event-key"}, ensure_ascii=False))
            return 1
        run_store = RunStore(Path(args.run_store)) if args.run_store else None
        print(json.dumps(command_status(GoalStore(Path(args.goal_store)), args.event_key, run_store), ensure_ascii=False, sort_keys=True))
        return 0

    missing = [name for name in ("source", "event_type", "event_id", "payload") if getattr(args, name) is None]
    if missing:
        print(json.dumps({"status": "rejected", "error": f"missing required arguments: {['--' + name.replace('_', '-') for name in missing]}"}, ensure_ascii=False))
        return 1

    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(json.dumps({"status": "rejected", "error": f"payload is not valid JSON: {exc}"}, ensure_ascii=False))
        return 1
    try:
        result = submit_command(
            GoalStore(Path(args.goal_store)),
            source=args.source,
            event_type=args.event_type,
            event_id=args.event_id,
            payload=payload,
            idempotency_key=args.idempotency_key,
        )
    except (ValueError, KeyError) as exc:
        print(json.dumps({"status": "rejected", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Command ingress — a bounded, commander-agnostic entry for issuing a goal event into LH.

This is the "command down" entry into LH. It is deliberately NOT bound
to any single commander (an external hub is one client of many: a CI system, a scheduler,
another front-end) and NOT bound to any model, provider, or file path. It only
validates a bounded event contract and delegates durable idempotency to
GoalStore.record_event; it never admits, runs, or promotes anything.
"""

from __future__ import annotations

import argparse
import json
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
}

# Event types that advance bounded campaign work must name a stage.
STAGE_REQUIRED_EVENT_TYPES = {"manual_intent", "stage_completion"}


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
        "goal_id": goal_id,
        "goal_state": goal_state,
        "execution": _execution_projection(goal_store, run_store, goal_event, chain),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Issue one bounded goal event into LH (command down)")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--run-store", default=None, help="optional LH RunStore for run/attempt/receipt projection")
    parser.add_argument("--status", action="store_true", help="read back one event by key instead of submitting")
    parser.add_argument("--event-key", default=None, help="event key to read in --status mode")
    parser.add_argument("--source", help="commander id, e.g. hub / ci / scheduler")
    parser.add_argument("--event-type", choices=sorted(SUPPORTED_EVENT_TYPES))
    parser.add_argument("--event-id")
    parser.add_argument("--payload", help="JSON object with at least campaign_id")
    parser.add_argument("--idempotency-key", default=None)
    args = parser.parse_args(argv)

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

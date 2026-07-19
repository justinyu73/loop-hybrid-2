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
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore

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
    if missing:
        raise ValueError(f"payload missing required fields: {missing}")

    return goal_store.record_event(
        event_id=event_id,
        source=source,
        event_type=event_type,
        payload=payload,
        idempotency_key=idempotency_key,
    )


def command_status(goal_store: GoalStore, event_key: str) -> dict[str, Any]:
    """Read back one event and its bound goal, or report it as unknown.

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
            "goal_id": None,
            "goal_state": None,
        }
    goal_id = event.get("goal_id")
    goal_state = None
    if goal_id is not None:
        goal_state = goal_store.get_goal(goal_id)["state"]
    return {
        "schema": "lh-command-status/v1",
        "event_key": event_key,
        "event_state": event["state"],
        "goal_id": goal_id,
        "goal_state": goal_state,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Issue one bounded goal event into LH (command down)")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--status", action="store_true", help="read back one event by key instead of submitting")
    parser.add_argument("--event-key", default=None, help="event key to read in --status mode")
    parser.add_argument("--source", help="commander id, e.g. hub / github / scheduler")
    parser.add_argument("--event-type", choices=sorted(SUPPORTED_EVENT_TYPES))
    parser.add_argument("--event-id")
    parser.add_argument("--payload", help="JSON object with at least campaign_id")
    parser.add_argument("--idempotency-key", default=None)
    args = parser.parse_args(argv)

    if args.status:
        if not args.event_key:
            print(json.dumps({"status": "rejected", "error": "--status requires --event-key"}, ensure_ascii=False))
            return 1
        print(json.dumps(command_status(GoalStore(Path(args.goal_store)), args.event_key), ensure_ascii=False, sort_keys=True))
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

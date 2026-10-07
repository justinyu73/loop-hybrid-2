#!/usr/bin/env python3
"""Open questions: what a human owes, typed and aged, read from the stores.

Every ``human_required`` goal and event becomes one open question with a kind,
its reason, and how long it has been waiting.  Items left waiting longer than
the quiet threshold are marked ``quiet`` so they do not fade out of view.

The kind comes from a closed table of reason codes; an unknown code is still
listed, as ``awaiting_owner`` with its raw reason.  This is a projection: it
reads the stores and decides nothing.
"""
from __future__ import annotations

import json
from typing import Any

try:
    from . import failure_router
except ImportError:  # direct execution keeps lh_runtime on sys.path
    import failure_router  # type: ignore

SCHEMA = "lh-open-questions/v1"
DEFAULT_QUIET_AFTER_SECONDS = 24 * 3600.0
AWAITING_OWNER = "awaiting_owner"
BLOCKED_BY_EVIDENCE = "blocked_by_evidence"
SCOPE_ESCALATION = "scope_escalation"
# Reason-code prefixes in the order they are tried; the first match wins.
KIND_BY_PREFIX = (
    ("execution_fence_unavailable", AWAITING_OWNER),
    ("verifier_unavailable", AWAITING_OWNER),
    ("regression_detected", AWAITING_OWNER),
    ("source_refs_mutated", SCOPE_ESCALATION),
    ("independent_verifier_in_write_scope", SCOPE_ESCALATION),
    ("authority_surface", SCOPE_ESCALATION),
    ("delivery_", BLOCKED_BY_EVIDENCE),
    ("candidate_review_", BLOCKED_BY_EVIDENCE),
)


def classify(reason: str | None) -> str:
    text = str(reason or "")
    for prefix, kind in KIND_BY_PREFIX:
        if text.startswith(prefix):
            return kind
    # No guessing from word shapes: an unlisted code is owed to the owner as written.
    return AWAITING_OWNER


def _run_reason(run_store: Any, run_id: str | None) -> str | None:
    """The reason the run itself recorded: provider routing first, then the receipt."""
    if not run_id:
        return None
    try:
        latest = run_store.latest_attempt(run_id)
    except (KeyError, ValueError):
        return None
    if not isinstance(latest, dict):
        return None
    ordinal = latest.get("ordinal")
    provider = run_store.read_artifact(run_id, ordinal, "provider.json")
    if provider:
        value = json.loads(provider)
        routing = value.get("routing") if isinstance(value.get("routing"), dict) else {}
        if routing.get("reason"):
            return str(routing["reason"])
        if value.get("failure"):
            return str(value["failure"])
    receipt = run_store.read_artifact(run_id, ordinal, "receipt.json")
    if receipt:
        verification = json.loads(receipt).get("verification")
        if isinstance(verification, dict) and verification.get("reason"):
            return str(verification["reason"])
    return None


def _item(source: str, subject: str, reason: str | None, since: float, now: float, quiet_after: float) -> dict[str, Any]:
    waiting = max(0.0, now - float(since))
    routed = failure_router.route(reason)
    # Every item here is parked for a human; a machine route means a machine could act instead.
    return {"source": source, "subject": subject, "kind": classify(reason), "reason": reason or "unrecorded",
            "since": since, "waiting_seconds": round(waiting, 3), "quiet": waiting > quiet_after,
            "route": routed, "machine_route_available": not routed["human_required"]}


def build_open_questions(run_store: Any, goal_store: Any, *, now: float,
                         quiet_after_seconds: float = DEFAULT_QUIET_AFTER_SECONDS) -> dict[str, Any]:
    items = []
    for goal in goal_store.goals_in_state("human_required"):
        items.append(_item("goal", goal["goal_id"], _run_reason(run_store, goal.get("run_id")),
                           goal["updated_at"], now, quiet_after_seconds))
    for event in goal_store.events_in_state("human_required"):
        result = event.get("result") if isinstance(event.get("result"), dict) else {}
        items.append(_item("event", event["event_key"], result.get("reason"), event["updated_at"], now,
                           quiet_after_seconds))
    items.sort(key=lambda item: (-item["waiting_seconds"], item["subject"]))
    return {"schema": SCHEMA, "quiet_after_seconds": quiet_after_seconds, "count": len(items),
            "quiet": sum(1 for item in items if item["quiet"]), "items": items}

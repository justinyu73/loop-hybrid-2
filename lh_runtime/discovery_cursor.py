"""D4/D9: deterministic, bounded, resumable event cursor at the task-area tick.

The cursor reads only the event types an approved binding declares as signals, in
Store ``rowid`` order with a SQL ``LIMIT``. It never scans history, never reads its
own records back as input, and never calls a model: a handoff is a durable record
for the later Planner consumer. The batch, its new candidates and the cursor move in
one Store transaction, so a crash leaves either the whole batch or none of it.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

BINDING_SCHEMA = "lh-discovery-cursor-binding/v1"
BATCH_SCHEMA = "lh-discovery-batch/v1"
CANDIDATE_SCHEMA = "lh-discovery-candidate/v1"
BATCH_EVENT = "task_area_discovery_batch"
CANDIDATE_EVENT = "task_area_discovery_candidate"
OWN_EVENT_PREFIX = "task_area_discovery"
OWN_EVENT_TYPES = frozenset({BATCH_EVENT, CANDIDATE_EVENT})
MAX_OBJECT_CHARS = 256
MAX_BATCH_EVENTS = 200
MAX_BATCH_CANDIDATES = 3
MAX_SIGNALS = 20


class DiscoveryError(ValueError):
    pass


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def _object_ok(value: Any) -> bool:
    """Untrusted event text: bounded, printable, and not padded."""
    return _text(value) and len(value) <= MAX_OBJECT_CHARS and value.isprintable()


def binding(manifest: dict) -> dict | None:
    """Validated approved binding; ``None`` means the task area did not opt in."""
    if "discovery_cursor" not in manifest:
        return None
    raw = manifest["discovery_cursor"]  # null or any non-object is invalid, not "unset"
    try:
        if (not isinstance(raw, dict)
            or set(raw) != {"schema", "batch_events", "batch_candidates", "signals"}
            or raw["schema"] != BINDING_SCHEMA):
            raise DiscoveryError
        for key, cap in (("batch_events", MAX_BATCH_EVENTS),
                         ("batch_candidates", MAX_BATCH_CANDIDATES)):
            if type(raw[key]) is not int or not 1 <= raw[key] <= cap:
                raise DiscoveryError
        signals = raw["signals"]
        if not isinstance(signals, list) or not 1 <= len(signals) <= MAX_SIGNALS:
            raise DiscoveryError
        by_type: dict[str, dict] = {}
        for item in signals:
            if (not isinstance(item, dict)
                or set(item) != {"event_type", "problem_type", "object_field"}
                or not all(_text(value) for value in item.values())
                or item["event_type"].startswith(OWN_EVENT_PREFIX)
                or item["event_type"] in by_type):
                raise DiscoveryError
            by_type[item["event_type"]] = item
    except (DiscoveryError, KeyError, TypeError) as exc:
        raise DiscoveryError("discovery_binding_invalid") from exc
    return {"batch_events": raw["batch_events"], "batch_candidates": raw["batch_candidates"],
            "signals": by_type}


def candidate_id(goal_id: str, problem_type: str, obj: str) -> str:
    """Stable per object + problem type; event versions never mint a new candidate."""
    body = json.dumps([goal_id, problem_type, obj], ensure_ascii=False, separators=(",", ":"))
    return "cand:" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:24]


def _authorize(controller: Any, manifest: dict) -> None:
    """The binding is only as good as the approved, event-settled manifest that holds it."""
    from .task_area import TaskAreaError, manifest_digest
    try:
        controller._verify_reviewed_binding(manifest)
        approval = controller._reviewed_approval_event_receipt(manifest)
        if approval is None or not controller._reviewed_approval_settled(manifest):
            raise DiscoveryError("discovery_approval_pending")
        approved = approval["outcome"].get("materialization_digest")
        if approved != manifest_digest(manifest):
            raise DiscoveryError("discovery_authority_drift")
        if controller.approved_digest is None:
            controller.approved_digest = approved
        # The whole approved shape (seal, plan verifier, envelopes, packets), not just the digest.
        controller._validate(manifest)
    except TaskAreaError as exc:
        raise DiscoveryError("discovery_authority_drift") from exc


def _report(status: str, *, reason: str | None = None, read: int = 0, after: int | None = None,
            new: int = 0, handoff: list[str] | None = None, pending: int = 0,
            coverage: dict | None = None) -> dict[str, Any]:
    """``after=None`` means the durable cursor could not be read: never a made-up 0."""
    handoff = list(handoff or [])
    row: dict[str, Any] = {
        "status": status, "events_read": read,
        "cursor": None if after is None else {"after_rowid": after},
        "new_candidates": new, "handoff": handoff, "pending_candidates": pending,
        "model_handoffs": 1 if handoff else 0, "model_invocations": 0, "coverage": coverage}
    if reason is not None:
        row["reason"] = reason
    return row


def _durable_after(store: Any, manifest: dict) -> int | None:
    try:
        return store.discovery_position(manifest["goal_id"])["after_rowid"]
    except Exception:  # noqa: BLE001 - best effort; an unreadable cursor is reported as unknown
        return None


def observe(store: Any, controller: Any, manifest: dict) -> dict[str, Any] | None:
    """One bounded tick. ``None`` only when the task area never opted in.

    Any failure of the observer is a named ``blocked`` report carrying the durable cursor
    when it can be read; it must never abort the rest of the task-area tick.
    """
    from .work_unit_store import WorkUnitStoreError
    try:
        config = binding(manifest)
        if config is None:
            return None
        _authorize(controller, manifest)
        return _tick(store, config, manifest["goal_id"])
    except DiscoveryError as exc:
        reason = str(exc)
        return _report("waiting" if reason == "discovery_approval_pending"
                       else "rejected" if reason == "discovery_binding_invalid" else "blocked",
                       reason=reason, after=_durable_after(store, manifest))
    except WorkUnitStoreError as exc:
        return _report("blocked", reason=str(exc), after=_durable_after(store, manifest))
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError) as exc:
        return _report("blocked", reason="discovery_observation_failed:" + type(exc).__name__,
                       after=_durable_after(store, manifest))


def _tick(store: Any, config: dict, goal: str) -> dict[str, Any]:
    position = store.discovery_position(goal)
    after = position["after_rowid"]
    rows = store.discovery_read_signals(
        goal, after_rowid=after, event_types=tuple(config["signals"]),
        limit=config["batch_events"])
    found: dict[str, dict] = {}
    read = 0
    coverage = None
    for row in rows:
        signal = config["signals"][row["event_type"]]
        try:
            payload = json.loads(row["payload_json"])
            obj = payload[signal["object_field"]] if isinstance(payload, dict) else None
            if not _object_ok(obj):
                raise KeyError(signal["object_field"])
        except (ValueError, TypeError, KeyError):
            # Stop before the unreadable event: nothing after it is dropped, and the
            # next tick resumes from the recorded position instead of rescanning.
            coverage = {"status": "coverage_unknown", "reason": "event_payload_unreadable",
                        "resume_after_rowid": after, "blocked_rowid": row["rowid"]}
            break
        cid = candidate_id(goal, signal["problem_type"], obj)
        if cid not in found:
            found[cid] = {"schema": CANDIDATE_SCHEMA, "candidate_id": cid, "object": obj,
                          "problem_type": signal["problem_type"],
                          "first_event_id": row["event_id"], "first_rowid": row["rowid"]}
        read += 1
        after = row["rowid"]
    known = store.discovery_known_candidates(goal, list(found))
    fresh = [record for cid, record in found.items() if cid not in known]
    queue = list(position["pending"]) + [record["candidate_id"] for record in fresh]
    handoff = queue[:config["batch_candidates"]]
    pending = queue[config["batch_candidates"]:]
    if not (read or handoff or coverage != position["coverage"]):
        # Nothing new to read, deliver or record: no append, no summary, no model.
        return _report("coverage_unknown" if coverage else "idle", after=after,
                       pending=len(pending), coverage=coverage)
    batch = {"schema": BATCH_SCHEMA, "seq": position["seq"] + 1,
             "from_rowid": position["after_rowid"], "to_rowid": after, "events_read": read,
             "new_candidates": [record["candidate_id"] for record in fresh],
             "handoff": handoff, "pending": pending, "coverage": coverage}
    store.record_discovery_batch(goal, batch=batch, candidates=fresh)
    # A write that claims success is not a record: read the cursor and candidates back.
    landed = store.discovery_position(goal)
    if (landed["seq"] != batch["seq"] or landed["after_rowid"] != after
        or store.discovery_known_candidates(goal, batch["new_candidates"])
        != set(batch["new_candidates"])):
        return _report("blocked", reason="discovery_batch_readback_missing",
                       after=landed["after_rowid"])
    return _report("coverage_unknown" if coverage else "processed", read=read, after=after,
                   new=len(fresh), handoff=handoff, pending=len(pending), coverage=coverage)

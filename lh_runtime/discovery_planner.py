"""P9-5 D1/D3/D6/D7: goal-level Planner consumer for discovery candidates.

The D4/D9 cursor records bounded candidates. This consumer turns each one into a single durable
request, a controlled Planner call, an independent verification and a durable decision, without a
Run: a candidate belongs to the goal, so it carries no Run/Attempt/fence and no fence can protect a
real model process. Only a controlled command port exists, and the binding must say so.

Work is derived from durable state in first-seen order, never from the tick's handoff list, so a
candidate recorded while the consumer was missing is still requested later. A request that cannot
progress blocks the ones behind it. The order for every role call is fixed: the request is
recorded, the role budget is reserved, the role runs, the reservation is settled, and only then is
the result recorded, so a crash never leaves a paid-for answer that a later tick would buy again.

A Planner decision is only a proposal. The consumer refuses adoption fields written by the worker,
malformed or foreign-evidence documents and an unauthorized ``repair`` before any verifier call;
only a GREEN verdict from an independent verifier makes a decision ``applied``. An applied
``repair`` is an authorization record: nothing here dispatches it.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from . import discovery_cursor
from .work_unit_store import WorkUnitStoreError, digest_json

BINDING_SCHEMA = "lh-discovery-planner-binding/v1"
REQUEST_SCHEMA = "lh-discovery-request/v1"
PROPOSAL_SCHEMA = "lh-discovery-proposal/v1"
DECISION_SCHEMA = "lh-discovery-decision/v1"
RECORD_SCHEMA = "lh-discovery-decision-record/v1"
VERDICT_SCHEMA = "lh-discovery-verdict/v1"
REQUEST_EVENT = "task_area_discovery_request"
PROPOSAL_EVENT = "task_area_discovery_proposal"
DECISION_EVENT = "task_area_discovery_decision"
RECORD_EVENTS = frozenset({REQUEST_EVENT, PROPOSAL_EVENT, DECISION_EVENT})
KINDS = ("repair", "propose_optimization", "bounded_diagnostic", "defer", "reject")
NEEDS_EXAM = frozenset({"repair", "propose_optimization", "bounded_diagnostic"})
ADOPTION_FIELDS = frozenset({"adopted", "approved", "applied", "verdict", "status", "pass"})
DOCUMENT_FIELDS = frozenset({
    "schema", "request_digest", "principal", "kind", "reason_hypothesis", "no_action_impact",
    "benefit_or_decision", "cost", "exam_refs", "evidence_refs"})
VERDICT_FIELDS = frozenset({
    "schema", "request_digest", "decision_digest", "principal", "verdict", "checks"})
CHECKS = ("evidence_linked", "comparable", "permission_ok", "verifiable")
MAX_TEXT_CHARS = 2000
MAX_REFS = 20
CONSUMED = "reviewed_task_authority_action_already_consumed"

RolePort = Callable[[str, dict, str], dict]


class PlannerError(ValueError):
    pass


class _Blocked(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def binding(manifest: dict) -> dict | None:
    """Validated approved binding; ``None`` means the task area did not opt in."""
    if "discovery_planner" not in manifest:
        return None
    raw = manifest["discovery_planner"]  # null or any non-object is invalid, not "unset"
    try:
        if (not isinstance(raw, dict) or set(raw) != {"schema", "allow_repair", "port"}
            or raw["schema"] != BINDING_SCHEMA or type(raw["allow_repair"]) is not bool
            or raw["port"] != "controlled_command"
            or "planner_recovery" not in manifest):
            raise PlannerError
        cursor = discovery_cursor.binding(manifest)
        if cursor is None:
            raise PlannerError
    except (PlannerError, discovery_cursor.DiscoveryError, KeyError, TypeError) as exc:
        raise PlannerError("discovery_planner_binding_invalid") from exc
    return {"allow_repair": raw["allow_repair"], "limit": cursor["batch_candidates"]}


def request_id(goal: str, cid: str) -> str:
    return f"discovery-request:{goal}:{cid}:1"


def proposal_id(goal: str, cid: str) -> str:
    return f"discovery-proposal:{goal}:{cid}:1"


def decision_id(goal: str, cid: str) -> str:
    return f"discovery-decision:{goal}:{cid}:1"


def _report(status: str = "idle", **changes: Any) -> dict[str, Any]:
    row: dict[str, Any] = {"status": status, "requests": 0, "planner_calls": 0, "verifier_calls": 0,
                           "decisions": [], "awaiting_decision": 0}
    row.update(changes)
    return row


def observe(store: Any, controller: Any, manifest: dict, *,
            context: Callable[[], tuple[bool, dict, RolePort]]) -> dict[str, Any] | None:
    """One bounded tick. ``None`` only when the task area never opted in.

    ``context`` yields ``(repair_permitted, principals, role_port)`` and is only called once the
    binding and the approval have been accepted. Any failure is a named report, never a crash.
    """
    try:
        config = binding(manifest)
        if config is None:
            return None
        discovery_cursor._authorize(controller, manifest)
        permitted, principals, role_port = context()
        return _tick(store, manifest["goal_id"], config["limit"],
                     permitted and config["allow_repair"], principals, role_port)
    except PlannerError as exc:
        reason = str(exc)
        return _report("rejected" if reason == "discovery_planner_binding_invalid" else "blocked",
                       reason=reason)
    except discovery_cursor.DiscoveryError as exc:
        reason = str(exc)
        return _report("waiting" if reason == "discovery_approval_pending" else "blocked", reason=reason)
    except WorkUnitStoreError as exc:
        return _report("blocked", reason=str(exc))
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
        return _report("blocked", reason="discovery_planner_observation_failed:" + type(exc).__name__)


def _tick(store: Any, goal: str, limit: int, permitted: bool, principals: dict,
          role_port: RolePort) -> dict[str, Any]:
    report = _report()
    blocked = None
    try:
        for row in store.discovery_next_candidates(
                goal, after_rowid=store.discovery_request_position(goal), limit=limit):
            body = _request_body(goal, row["payload"], row["rowid"], permitted)
            payload = {**body, "request_digest": digest_json(body)}
            _write(store, goal, request_id(goal, row["payload"]["candidate_id"]),
                   REQUEST_EVENT, payload, "discovery_request_readback_missing")
            report["requests"] += 1
        for item in store.discovery_open_requests(goal, limit=limit):
            report["decisions"].append(
                _decide(store, goal, item, permitted, principals, role_port, report))
    except _Blocked as exc:
        blocked = exc.reason
    except WorkUnitStoreError as exc:  # keep what this tick already did
        blocked = str(exc)
    try:
        report["awaiting_decision"] = store.discovery_open_count(goal)
    except (WorkUnitStoreError, sqlite3.Error):
        report["awaiting_decision"] = None  # unreadable is not zero
    if blocked is not None:
        report.update(status="blocked", reason=blocked)
    elif report["requests"] or report["planner_calls"] or report["verifier_calls"] or report["decisions"]:
        report["status"] = "processed"
    return report


def _request_body(goal: str, record: dict, rowid: int, permitted: bool) -> dict[str, Any]:
    return {
        "schema": REQUEST_SCHEMA, "goal_id": goal,
        "candidate": {"candidate_id": record["candidate_id"], "object": record["object"],
                      "problem_type": record["problem_type"]},
        "candidate_rowid": rowid,
        "evidence": {"first_event_id": record["first_event_id"], "first_rowid": record["first_rowid"],
                     "event_refs": [record["first_event_id"]]},
        "comparability": "unknown", "known_cost": "unknown",
        "allowed_decisions": list(KINDS), "permissions": {"repair": bool(permitted)}, "version": 1}


def _write(store: Any, goal: str, event_id: str, event_type: str, payload: dict, missing: str) -> None:
    """A write that claims success is not a record: read it back."""
    store.record_discovery_planner_event(goal, event_id=event_id, event_type=event_type, payload=payload)
    if store.discovery_get_event(goal, event_id) != json.loads(json.dumps(payload)):
        raise _Blocked(missing)


def _call(role_port: RolePort, role: str, frame: dict, reservation: str, report: dict) -> dict | None:
    """The role's raw answer, or ``None`` when its budget was already spent without a recorded result."""
    try:
        answer = role_port(role, frame, reservation)
    except WorkUnitStoreError as exc:
        if str(exc) == CONSUMED:
            return None
        raise _Blocked(str(exc)) from exc
    except ValueError as exc:
        if str(exc) == "discovery_role_outcome_unknown":
            report[("planner" if role == "planner" else "verifier") + "_calls"] += 1
        raise _Blocked(str(exc)) from exc
    report[("planner" if role == "planner" else "verifier") + "_calls"] += 1
    return answer


def _object(answer: dict) -> dict | None:
    """The role's stdout as a JSON object that can be stored, or ``None``: it is untrusted bytes."""
    if answer.get("exit_code") != 0 or answer.get("overflow"):
        return None
    raw = answer.get("stdout")
    try:
        value = json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw or "")
        # Storable means it survives the same encoding the Store applies (no lone surrogate, no NaN).
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, RecursionError, AttributeError):
        return None
    return value if isinstance(value, dict) else None


def _candidate_of(goal: str, item: dict) -> str:
    """The candidate id from the request event's own id, for a request whose body is not trusted."""
    return item["event_id"][len(f"discovery-request:{goal}:"):-len(":1")]


def _request_problem(request: Any, goal: str, event_id: str) -> bool:
    """A stored request is only as good as its own digest: never trust it just for being stored."""
    try:
        body = {key: value for key, value in request.items() if key != "request_digest"}
        return not (
            request["schema"] == REQUEST_SCHEMA and request["goal_id"] == goal
            and event_id == request_id(goal, request["candidate"]["candidate_id"])
            and request["request_digest"] == digest_json(body)
            and type(request["permissions"]["repair"]) is bool
            and isinstance(request["candidate"]["candidate_id"], str)
            and _refs(request["evidence"]["event_refs"]))
    except (AttributeError, KeyError, TypeError):
        return True


def _decide(store: Any, goal: str, item: dict, permitted: bool, principals: dict,
            role_port: RolePort, report: dict) -> dict[str, Any]:
    request = item["request"]
    proposal = item["proposal"]
    invalid = _request_problem(request, goal, item["event_id"])
    if invalid:
        proposal = None  # a request that cannot be trusted has no trustworthy proposal either
        digest = request.get("request_digest") if isinstance(request, dict) else None
        cid = _candidate_of(goal, item)
    else:
        digest = request["request_digest"]
        cid = request["candidate"]["candidate_id"]
    body = None if invalid else {key: value for key, value in request.items() if key != "request_digest"}

    def finish(status: str, reason: str | None, verdict: dict | None = None) -> dict[str, Any]:
        # ``proposal`` is read when this runs: it is the stored or freshly recorded answer, if any.
        stored = proposal if isinstance(proposal, dict) else {}
        document = stored.get("decision")
        kind = document.get("kind") if isinstance(document, dict) and document.get("kind") in KINDS else None
        record = {"schema": RECORD_SCHEMA, "request_digest": digest, "request_rowid": item["rowid"],
                  "decision_digest": stored.get("decision_digest")
                  if isinstance(stored.get("decision_digest"), str) else None,
                  "kind": kind, "status": status, "reason": reason, "verdict": verdict}
        _write(store, goal, decision_id(goal, cid), DECISION_EVENT, record,
               "discovery_decision_readback_missing")
        return {"candidate_id": cid, "request_digest": digest, "status": status, "kind": kind,
                "reason": reason}

    if invalid:
        return finish("rejected", "discovery_request_invalid")
    if proposal is None:
        answer = _call(role_port, "planner", {"mode": "discovery_planner", "discovery_request": body},
                       "discovery-planner:" + digest, report)
        if answer is None:
            return finish("rejected", "discovery_role_result_lost")
        document = _object(answer)
        if document is None:
            return finish("rejected", "discovery_planner_output_invalid")
        proposal = {"schema": PROPOSAL_SCHEMA, "request_digest": digest, "decision": document,
                    "decision_digest": digest_json(document)}
        _write(store, goal, proposal_id(goal, cid), PROPOSAL_EVENT, proposal,
               "discovery_proposal_readback_missing")
    if (not isinstance(proposal, dict) or proposal.get("request_digest") != digest
            or proposal.get("decision_digest") != digest_json(proposal.get("decision"))):
        return finish("rejected", "discovery_proposal_invalid")  # a stored answer is checked too
    reason = _judge(proposal["decision"], request, principals["planner"], permitted)
    if reason is not None:
        return finish("rejected", reason)
    answer = _call(role_port, "verifier",
                   {"mode": "discovery_verifier", "discovery_request": body,
                    "discovery_decision": proposal["decision"]},
                   "discovery-verifier:" + digest, report)
    if answer is None:
        return finish("rejected", "discovery_role_result_lost")
    verdict = _object(answer)
    reason = _judge_verdict(verdict, request, proposal, principals)
    if reason is not None:
        return finish("rejected", reason, verdict if isinstance(verdict, dict) else None)
    return finish("applied", None, verdict)


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip()) and len(value) <= MAX_TEXT_CHARS


def _refs(value: Any) -> bool:
    return (isinstance(value, list) and len(value) <= MAX_REFS
            and all(isinstance(item, str) and item.strip() for item in value))


def _judge(document: Any, request: dict, planner: str, permitted: bool) -> str | None:
    """The Planner's document against the request; first refusal wins, in the pinned order."""
    invalid = "discovery_planner_output_invalid"
    if not isinstance(document, dict):
        return invalid
    if ADOPTION_FIELDS & set(document):
        return "discovery_decision_self_adoption"
    if (set(document) != DOCUMENT_FIELDS or document["schema"] != DECISION_SCHEMA
        or document["request_digest"] != request["request_digest"]
        or document["principal"] != planner or document["kind"] not in KINDS
        or not all(_text(document[key]) for key in (
            "reason_hypothesis", "no_action_impact", "benefit_or_decision"))
        or not _refs(document["exam_refs"]) or not _refs(document["evidence_refs"])
        or (document["kind"] in NEEDS_EXAM and not document["exam_refs"])):
        return invalid
    cost = document["cost"]
    if (not isinstance(cost, dict) or set(cost) != {"tests_and_rebind"}
        or not (cost["tests_and_rebind"] == "unknown" or (
            type(cost["tests_and_rebind"]) is int and cost["tests_and_rebind"] >= 0))):
        return invalid
    refs = document["evidence_refs"]
    if not refs or not set(refs) <= set(request["evidence"]["event_refs"]):
        return "discovery_decision_evidence_unlinked"
    # The request's own claim is history; the permission has to hold now.
    if document["kind"] == "repair" and not (permitted and request["permissions"]["repair"] is True):
        return "discovery_decision_repair_not_authorized"
    return None


def _judge_verdict(verdict: Any, request: dict, proposal: dict, principals: dict) -> str | None:
    invalid = "discovery_verdict_invalid"
    if (not isinstance(verdict, dict) or set(verdict) != VERDICT_FIELDS
        or verdict["schema"] != VERDICT_SCHEMA or verdict["verdict"] not in ("GREEN", "RED")
        or not isinstance(verdict["principal"], str) or not verdict["principal"].strip()
        or not isinstance(verdict["checks"], dict) or set(verdict["checks"]) != set(CHECKS)
        or not all(type(value) is bool for value in verdict["checks"].values())
        or verdict["request_digest"] != request["request_digest"]
        or verdict["decision_digest"] != proposal["decision_digest"]
        or (verdict["verdict"] == "GREEN" and not all(verdict["checks"].values()))):
        return invalid
    if (verdict["principal"] == proposal["decision"]["principal"]
        or verdict["principal"] != principals["verifier"]):
        return "discovery_verifier_not_independent"
    return None if verdict["verdict"] == "GREEN" else "discovery_verdict_red"

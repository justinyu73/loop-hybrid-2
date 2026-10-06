#!/usr/bin/env python3
"""Failure router: a fixed answer to "who does what next" after a failure.

``route(reason_code, failure_count=0)`` maps a reason code through one closed
table, without calling any model, to a receipt (``lh-failure-route/v1``) that
names the route, the owner, the next action, and what the route invalidates.
Failure evidence and prior receipts are always preserved.

- Only a closed list of owner actions (``OWNER_ACTIONS``) needs a human; an
  engine code that is really an owner action is mapped onto one of them.
- Every other code is a machine reason and never routes to a human.
- An unknown code routes to the router table itself: the table, not a person,
  is what has to change.
- A machine reason seen ``REPEAT_THRESHOLD`` times routes to an independent
  read-only audit instead of another retry.

This is a projection: it decides nothing for the engine, and the engine's own
state transitions are unchanged.  ``verify_route`` recomputes a receipt.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

SCHEMA = "lh-failure-route/v1"
REPEAT_THRESHOLD = 3
PRESERVES = ("failure_evidence", "prior_receipts")
OWNER_ACTIONS = frozenset({
    "material_scope_expansion", "destructive_effect", "paid_provider_invocation", "runtime_activation",
    "external_account_action", "owner_merge", "product_acceptance", "secret_handling", "publication", "promotion",
})
# family -> (route, owner, next_action, invalidates)
FAMILIES = {
    "check_red": ("retry_same_unit", "executor", "new_attempt_on_the_same_unit", "current_attempt"),
    "review_binding": ("repair_review_binding", "verifier", "repair_review_binding_then_reverify", "review_evidence"),
    "verifier_binding": ("repair_verifier_binding", "verifier", "repair_verifier_binding_then_reverify",
                         "verifier_evidence"),
    "evidence_integrity": ("reread_evidence", "engine", "reread_and_reverify_evidence", "derived_verdict"),
    "planner_repair": ("planner_bounded_repair", "planner", "propose_bounded_repair_for_the_same_goal",
                       "stalled_campaign_order"),
    "recovery_integrity": ("reconcile_recovery_record", "engine", "reconcile_the_recovery_record",
                           "unresolved_recovery_claim"),
    "repeated_failure": ("independent_read_only_audit", "auditor", "run_an_independent_read_only_audit",
                         "same_unit_retry"),
    "unknown": ("repair_router_table", "router", "add_the_code_to_the_router_table", "unclassified_route"),
    "owner_action": ("human_required", "owner", "owner_action_required", "none"),
}
# Exact codes first; each maps to a family or to an owner action.
EXACT = {
    "check_failed": "check_red",
    "delivery_verifier_red": "check_red",
    "delivery_external_conclusion_not_success": "check_red",
    "candidate_review_not_green": "check_red",
    "candidate_review_related_checks_red": "check_red",
    "unrecorded": "evidence_integrity",
    "campaign_consecutive_failures": "planner_repair",
    "source_refs_mutated_by_executor": "destructive_effect",
    "candidate_path_outside_allowed_set": "material_scope_expansion",
    "changed_path_outside_contract_scope": "material_scope_expansion",
    "changed_path_outside_packet_scope": "material_scope_expansion",
    "dispatch_outside_scope": "material_scope_expansion",
    "repair_workspace_outside_scope": "material_scope_expansion",
    # More budget or more attempts than the contract allowed is the owner's to grant.
    "campaign_recovery_requires_authority": "material_scope_expansion",
    "campaign_recovery_child_attempt_budget_exhausted": "material_scope_expansion",
    "campaign_recovery_role_budget_exhausted": "material_scope_expansion",
    "campaign_recovery_incident_deadline_exhausted": "material_scope_expansion",
}
# Then prefixes, in the order they are tried; the first match wins.
PREFIXES = (
    ("execution_fence_unavailable", "runtime_activation"),
    ("source_refs_mutated", "destructive_effect"),
    ("independent_verifier_in_write_scope", "material_scope_expansion"),
    ("authority_surface", "material_scope_expansion"),
    ("delivery_independent_verifier_", "verifier_binding"),
    ("delivery_", "evidence_integrity"),
    ("attempt_receipt_", "evidence_integrity"),
    ("candidate_review_", "review_binding"),
    ("campaign_recovery_", "recovery_integrity"),
)


def digest_json(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _lookup(code: str) -> str:
    if code in OWNER_ACTIONS:
        return code
    if code in EXACT:
        return EXACT[code]
    for prefix, target in PREFIXES:
        if code.startswith(prefix):
            return target
    return "unknown"


def known_codes() -> list[str]:
    """Every exact code and prefix the table names (prefixes as written)."""
    return sorted({*EXACT, *(prefix for prefix, _target in PREFIXES), *OWNER_ACTIONS})


def route(reason_code: Any, failure_count: int = 0) -> dict[str, Any]:
    code = reason_code.strip() if isinstance(reason_code, str) and reason_code.strip() else "unrecorded"
    count = failure_count if isinstance(failure_count, int) and not isinstance(failure_count, bool) else 0
    count = max(0, count)
    target = _lookup(code)
    owner_action = target if target in OWNER_ACTIONS else None
    if owner_action is not None:
        family, normalized = "owner_action", code
    elif target == "unknown":
        family, normalized = "unknown", "unknown_reason_code"
    elif count >= REPEAT_THRESHOLD:
        family, normalized = "repeated_failure", "repeated_failure_threshold"
    else:
        family, normalized = target, code
    path, owner, action, invalidates = FAMILIES[family]
    body = {
        "schema": SCHEMA,
        "input_reason_code": code,
        "reason_code": normalized,
        "family": family,
        "route": path,
        "owner": owner,
        "next_action": action,
        "invalidates": [invalidates],
        "preserves": list(PRESERVES),
        "human_required": owner_action is not None,
        "owner_action": owner_action,
        "failure_count": count,
    }
    return {**body, "route_digest": digest_json(body)}


def verify_route(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Recompute a receipt from its own inputs; any other content is tampering or table drift."""
    if not isinstance(receipt, Mapping) or receipt.get("schema") != SCHEMA:
        return {"ok": False, "reason": "failure_route_schema_invalid"}
    expected = route(receipt.get("input_reason_code"), receipt.get("failure_count", 0))
    if dict(receipt) != expected:
        return {"ok": False, "reason": "failure_route_mismatch"}
    return {"ok": True, "reason": "failure_route_recomputed"}

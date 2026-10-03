#!/usr/bin/env python3
"""Delivery bindings compiled from an operator-authored acceptance lamp.

Every run carries a sealed delivery binding (contract, plan, packet).  The
native-run path gets one from its planner; a plain campaign stage gets one
here, and only when the stage opts in with
``"delivery": {"derive": "acceptance_lamp"}``.  A stage without that field is
returned unchanged, so it still stops at ``planning_required``.

The independent verifier is the stage's own acceptance lamp: written by the
operator before any attempt and bound to the digest of the contract file it
came from.  The planner is therefore named for what it is,
``operator-contract`` -- not a model, and not a test fixture.

A lamp the agent could edit is no independent verifier.  Any repo-relative
path in the lamp's argv that falls inside the stage's ``allowed_paths``
refuses the binding (``independent_verifier_in_write_scope``).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Mapping, Sequence

import delivery_contract as contract_engine

DERIVE_ACCEPTANCE_LAMP = "acceptance_lamp"
PLANNER_PRINCIPAL = "operator-contract"
VERIFIER_PRINCIPAL = "operator-acceptance-lamp"
DEFAULT_CHECKS = ({"id": "diff-hygiene", "argv": ["git", "diff", "--cached", "--check"]},)
CHECK_TIMEOUT_MAX = 300
VERIFIER_TIMEOUT = 300
FORBIDDEN_PATHS = [".git/", "secrets/", "credentials/", "cookies/"]
IDENTITY = ["unit_id", "goal_id", "goal_revision", "node_id", "dispatch_key",
            "run_id", "attempt", "fence", "base_sha", "diff_digest"]
RECEIPTS = ["plan_verdict", "packet_admission", "dispatch", "executor", "delivery_verifier", "completion"]
SOURCE_RECEIPTS = ["plan_verdict", "packet_admission", "dispatch", "executor", "delivery_verifier"]
_WINDOWS_ABSOLUTE = re.compile(r"[A-Za-z]:/")


class DeliveryBindingError(ValueError):
    """A refused binding; the message starts with a stable reason code."""


def _refuse(code: str, detail: Any = None) -> DeliveryBindingError:
    return DeliveryBindingError(code if detail is None else f"{code}:{detail}")


def _opt_in(stage: Mapping[str, Any]) -> dict[str, Any] | None:
    delivery = stage.get("delivery")
    if delivery is None:
        return None
    if (not isinstance(delivery, Mapping) or delivery.get("derive") != DERIVE_ACCEPTANCE_LAMP
            or set(delivery) - {"derive", "checks"}):
        raise _refuse("delivery_opt_in_invalid", stage.get("stage_id"))
    return dict(delivery)


def _obligations(raw: Any) -> list[dict[str, Any]]:
    items = DEFAULT_CHECKS if raw is None else raw
    if not isinstance(items, (list, tuple)) or not items:
        raise _refuse("delivery_opt_in_invalid", "checks")
    obligations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, Mapping) or set(item) - {"id", "argv", "timeout_seconds"}:
            raise _refuse("delivery_opt_in_invalid", "check")
        check_id, argv = item.get("id"), item.get("argv")
        timeout = item.get("timeout_seconds", CHECK_TIMEOUT_MAX)
        if (not isinstance(check_id, str) or not check_id.strip() or check_id in seen
                or not isinstance(argv, list) or not argv
                or any(not isinstance(token, str) or not token for token in argv)
                or isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not 0 < timeout <= CHECK_TIMEOUT_MAX):
            raise _refuse("delivery_opt_in_invalid", f"check:{check_id}")
        seen.add(check_id)
        obligations.append({
            "id": check_id,
            "commands": [{"id": check_id, "argv": list(argv), "cwd": "${WORKTREE}",
                          "expect_exit": 0, "timeout_seconds": timeout}],
            "required_receipts": ["executor"],
        })
    return obligations


def verifier_path_in_write_scope(argv: Sequence[str], allowed_paths: Sequence[str]) -> str | None:
    """The first argv token that names a path the agent may write, if any.

    Flags, absolute paths and inline programs (anything with whitespace) are
    not repo paths; every other token is checked against ``allowed_paths``.
    """
    for token in argv:
        candidate = token.replace("\\", "/")
        if (not candidate or candidate.startswith("-") or any(char.isspace() for char in candidate)
                or candidate.startswith("/") or _WINDOWS_ABSOLUTE.match(candidate)):
            continue
        if any(contract_engine.scope_matches(candidate, prefix) for prefix in allowed_paths):
            return token
    return None


def compile_stage_delivery(
    stage: Mapping[str, Any],
    *,
    campaign_id: str,
    contract_ref: str,
    contract_digest: str,
    source_repo: str,
    base_revision: str,
) -> dict[str, Any]:
    """Return the stage with a sealed delivery binding, or unchanged without the opt-in."""
    compiled = copy.deepcopy(dict(stage))
    delivery = _opt_in(stage)
    if delivery is None:
        return compiled
    for name, value in (("campaign_id", campaign_id), ("contract_ref", contract_ref),
                        ("contract_digest", contract_digest), ("source_repo", source_repo),
                        ("base_revision", base_revision)):
        if not isinstance(value, str) or not value.strip():
            raise _refuse("delivery_binding_input_missing", name)
    stage_id = str(stage.get("stage_id") or "")
    lamp = stage.get("acceptance_lamp")
    argv = lamp.get("verification_argv") if isinstance(lamp, Mapping) else None
    if not stage_id or not isinstance(argv, list) or not argv or any(not isinstance(item, str) or not item for item in argv):
        raise _refuse("acceptance_lamp_missing", stage_id or None)
    allowed = stage.get("allowed_paths")
    if not isinstance(allowed, list) or not allowed or any(not isinstance(item, str) or not item for item in allowed):
        raise _refuse("delivery_opt_in_invalid", "allowed_paths")
    editable = verifier_path_in_write_scope(argv, allowed)
    if editable is not None:
        raise _refuse("independent_verifier_in_write_scope", editable)
    max_attempts = stage.get("max_attempts", 4)
    goal = compiled.get("goal") if isinstance(compiled.get("goal"), dict) else {}
    goal_id = f"{campaign_id}:{stage_id}"
    obligations = _obligations(delivery.get("checks"))
    body: dict[str, Any] = {
        "schema": contract_engine.SCHEMA,
        "contract_version": 1,
        "contract_id": f"campaign-contract-{goal_id}-{stage_id}",
        "unit_id": f"campaign-unit-{goal_id}-{stage_id}",
        "goal": {"id": goal_id, "revision": 1},
        "node": {"id": stage_id, "kind": "campaign-stage"},
        "planner": {"principal": PLANNER_PRINCIPAL, "source": f"{contract_ref}@{contract_digest}"},
        "independent_verifier": {
            "principal": VERIFIER_PRINCIPAL,
            "read_only": True,
            "source_write": False,
            "capability": VERIFIER_PRINCIPAL,
            "argv": list(argv),
            "cwd": "${WORKTREE}",
            "timeout_seconds": VERIFIER_TIMEOUT,
        },
        "outcome": {
            "observable": "the stage acceptance lamp passes on the candidate and delivery evidence is durable",
            "start_state": "queued",
            "success_state": "verified",
            "terminal_states": ["verified", "human_required", "exhausted"],
        },
        "scope": {
            "ownership": PLANNER_PRINCIPAL,
            "allowed_paths": list(allowed),
            "forbidden_paths": list(FORBIDDEN_PATHS),
            "identity": list(IDENTITY),
        },
        "obligations": obligations,
        "required_receipts": list(RECEIPTS),
        "source_required_receipts": list(SOURCE_RECEIPTS),
        "source_obligation_ids": [item["id"] for item in obligations],
        "source_vs_live": {"source_must_not_claim_live": True, "live_required_for_source_delivery": False},
        "repair_same_unit": {
            "enabled": True,
            "route": "same_work_unit_new_attempt",
            "identity_fields": ["unit_id", "dispatch_key", "goal_id", "node_id"],
            "max_attempts": max_attempts,
            "scope_drift_route": "planner_required",
            "unknown_outcome_route": "reconcile_before_retry",
        },
        "authority_store": "run",
        "managed_scope": PLANNER_PRINCIPAL,
    }
    try:
        contract = contract_engine.seal_contract(body)
        plan = contract_engine.plan_delivery_unit(contract)
        packet = contract_engine.bind_packet({
            "schema": "host-delivery-unit-packet/v1",
            "packet_id": f"campaign-packet-{goal_id}-{stage_id}",
            "goal_id": goal_id,
            "goal_revision": 1,
            "node_id": stage_id,
            "write_set": list(allowed),
            "forbidden_paths": list(FORBIDDEN_PATHS),
        }, plan, contract)
    except contract_engine.DeliveryUnitError as exc:
        raise _refuse("delivery_contract_invalid", exc) from exc
    compiled["goal"] = {**goal, "delivery_required": True, "delivery_contract": contract,
                        "delivery_plan": plan, "delivery_packet": packet}
    return compiled


def compile_campaign_delivery(
    campaign: Mapping[str, Any],
    *,
    contract_ref: str,
    contract_digest: str,
    source_repo: str,
    base_revision: str,
) -> dict[str, Any]:
    """Compile every opted-in stage; stages without the opt-in stay byte-identical."""
    compiled = copy.deepcopy(dict(campaign))
    stages = compiled.get("stages")
    if not isinstance(stages, list):
        return compiled
    campaign_id = str(compiled.get("campaign_id") or "")
    compiled["stages"] = [
        compile_stage_delivery(stage, campaign_id=campaign_id, contract_ref=contract_ref,
                               contract_digest=contract_digest, source_repo=source_repo,
                               base_revision=base_revision)
        if isinstance(stage, Mapping) and "delivery" in stage else stage
        for stage in stages
    ]
    return compiled


__all__ = [
    "DeliveryBindingError",
    "compile_campaign_delivery",
    "compile_stage_delivery",
    "verifier_path_in_write_scope",
]

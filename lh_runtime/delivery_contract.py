"""Provider-neutral delivery-unit contract engine.

This module is deliberately independent from external host.  It validates the immutable
contract emitted by a planner, seals the plan verdict, binds a packet, and
grades the durable evidence produced by the LH runtime.  The compatibility
module under ``tools/`` supplies the historical P7 canonical path; this file
never imports that path or a Goal-specific constant.
"""

from __future__ import annotations

import copy
import hashlib
import json
import posixpath
import re
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


SCHEMA = "host-delivery-unit-contract/v1"
PLAN_SCHEMA = "host-delivery-unit-plan-verdict/v1"
DELIVERY_SCHEMA = "host-delivery-unit-delivery-verdict/v1"
ADMISSION_SCHEMA = "host-delivery-unit-admission/v1"
LEGACY_SIDECAR_SCHEMA = "host-delivery-unit-legacy-sidecar/v1"
PLANNING_REQUEST_SCHEMA = "lh-delivery-planning-request/v1"
REPAIRED_CANDIDATE_SCHEMA = "host-p7-checks-repair-candidate-evidence/v1"
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PLACEHOLDER_RE = re.compile(r"\$\{[^}]+\}")
IDENTITY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]*$")


class DeliveryUnitError(ValueError):
    """A delivery unit cannot be admitted or delivered safely."""

    def __init__(self, reason: str, *, detail: Any = None):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


def digest_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _without(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    body = copy.deepcopy(dict(value))
    body.pop(field, None)
    return body


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeliveryUnitError(f"{name}_missing")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    value = _text(name, value).lower()
    if SHA256_RE.fullmatch(value) is None:
        raise DeliveryUnitError(f"{name}_invalid")
    return value


def _mapping(name: str, value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DeliveryUnitError(f"{name}_missing")
    return value


def _strings(name: str, value: Any, *, required: bool = False) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise DeliveryUnitError(f"{name}_invalid")
    result = [item.strip() for item in value]
    if required and not result:
        raise DeliveryUnitError(f"{name}_empty")
    return result


def normalize_scope_path(value: Any, *, name: str = "scope.path") -> str:
    """Normalize a relative scope prefix and reject traversal/absolute paths."""
    if not isinstance(value, str) or not value.strip():
        raise DeliveryUnitError(f"{name}_invalid")
    raw = value.strip().replace("\\", "/")
    if raw.startswith("/") or re.fullmatch(r"[A-Za-z]:/.*", raw):
        raise DeliveryUnitError(f"{name}_absolute")
    normalized = posixpath.normpath(raw)
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        raise DeliveryUnitError(f"{name}_escape")
    return normalized.removeprefix("./").rstrip("/")


def _scope_paths(name: str, value: Any, *, required: bool = True) -> list[str]:
    values = _strings(name, value, required=required)
    result: list[str] = []
    for index, item in enumerate(values):
        normalized = normalize_scope_path(item, name=f"{name}[{index}]")
        if normalized in result:
            raise DeliveryUnitError(f"{name}_duplicate")
        result.append(normalized)
    return result


def scope_matches(path: Any, prefix: Any) -> bool:
    try:
        candidate = normalize_scope_path(path, name="changed_path")
        declared = normalize_scope_path(prefix, name="scope_prefix")
    except DeliveryUnitError:
        return False
    return candidate == declared or candidate.startswith(declared + "/")


def _verify_packet_scope(packet: Mapping[str, Any], contract: Mapping[str, Any]) -> None:
    packet_write = _scope_paths("packet.write_set", packet.get("write_set"))
    scope = _mapping("scope", contract.get("scope"))
    allowed = _scope_paths("scope.allowed_paths", scope.get("allowed_paths"))
    for path in packet_write:
        if not any(scope_matches(path, prefix) for prefix in allowed):
            raise DeliveryUnitError("packet_write_scope_expanded", detail=path)


def verify_changed_paths_scope(
    changed_paths: Sequence[Any], packet: Mapping[str, Any], contract: Mapping[str, Any]
) -> list[str]:
    if not isinstance(changed_paths, list):
        raise DeliveryUnitError("delivery_changed_paths_missing")
    _verify_packet_scope(packet, contract)
    packet_write = _scope_paths("packet.write_set", packet.get("write_set"))
    packet_forbidden = _scope_paths("packet.forbidden_paths", packet.get("forbidden_paths"), required=False)
    scope = _mapping("scope", contract.get("scope"))
    allowed = _scope_paths("scope.allowed_paths", scope.get("allowed_paths"))
    forbidden = _scope_paths("scope.forbidden_paths", scope.get("forbidden_paths"))
    result: list[str] = []
    for index, raw in enumerate(changed_paths):
        path = normalize_scope_path(raw, name=f"changed_paths[{index}]")
        if not any(scope_matches(path, prefix) for prefix in packet_write):
            raise DeliveryUnitError("changed_path_outside_packet_scope", detail=path)
        if not any(scope_matches(path, prefix) for prefix in allowed):
            raise DeliveryUnitError("changed_path_outside_contract_scope", detail=path)
        if any(scope_matches(path, prefix) for prefix in packet_forbidden + forbidden):
            raise DeliveryUnitError("changed_path_forbidden", detail=path)
        result.append(path)
    return sorted(set(result))


def _walk_no_placeholders(value: Any) -> None:
    if isinstance(value, str):
        unresolved = [match.group(0) for match in PLACEHOLDER_RE.finditer(value) if match.group(0) != "${WORKTREE}"]
        if unresolved:
            raise DeliveryUnitError("contract_unresolved_placeholder")
    elif isinstance(value, Mapping):
        for child in value.values():
            _walk_no_placeholders(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for child in value:
            _walk_no_placeholders(child)


def _validate_command(obligation_id: str, command: Mapping[str, Any]) -> None:
    _text(f"obligation.{obligation_id}.command.id", command.get("id"))
    argv = command.get("argv")
    if not isinstance(argv, list) or not argv or any(not isinstance(item, str) or not item for item in argv):
        raise DeliveryUnitError(f"obligation.{obligation_id}.command_argv_invalid")
    _text(f"obligation.{obligation_id}.command.cwd", command.get("cwd"))
    expected = command.get("expect_exit", 0)
    if isinstance(expected, bool) or not isinstance(expected, int):
        raise DeliveryUnitError(f"obligation.{obligation_id}.command_expect_exit_invalid")
    timeout = command.get("timeout_seconds", 300)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 900:
        raise DeliveryUnitError(f"obligation.{obligation_id}.command_timeout_invalid")


def _identity_names(scope: Mapping[str, Any]) -> list[str]:
    names = _strings("scope.identity", scope.get("identity"), required=True)
    if len(set(names)) != len(names) or any(IDENTITY_NAME_RE.fullmatch(name) is None for name in names):
        raise DeliveryUnitError("scope.identity_invalid")
    return names


def validate_contract(contract: Mapping[str, Any], *, require_digest: bool = True) -> dict[str, Any]:
    """Validate one contract without assuming a Goal, node kind, or provider."""
    if not isinstance(contract, Mapping) or contract.get("schema") != SCHEMA:
        raise DeliveryUnitError("contract_schema_invalid")
    body = _without(contract, "contract_digest")
    if require_digest and _digest("contract_digest", contract.get("contract_digest")) != digest_json(body):
        raise DeliveryUnitError("contract_digest_mismatch")
    if contract.get("contract_version") != 1:
        raise DeliveryUnitError("contract_version_invalid")
    _text("contract_id", contract.get("contract_id"))
    _text("unit_id", contract.get("unit_id"))
    goal = _mapping("goal", contract.get("goal"))
    _text("goal.id", goal.get("id"))
    revision = goal.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise DeliveryUnitError("goal.revision_invalid")
    node = _mapping("node", contract.get("node"))
    _text("node.id", node.get("id"))
    _text("node.kind", node.get("kind"))
    planner = _mapping("planner", contract.get("planner"))
    _text("planner.principal", planner.get("principal"))
    _text("planner.source", planner.get("source"))
    verifier = _mapping("independent_verifier", contract.get("independent_verifier"))
    _text("independent_verifier.principal", verifier.get("principal"))
    if verifier.get("read_only") is not True or verifier.get("source_write") is not False:
        raise DeliveryUnitError("independent_verifier_contract_invalid")
    outcome = _mapping("outcome", contract.get("outcome"))
    for field in ("observable", "start_state", "success_state"):
        _text(f"outcome.{field}", outcome.get(field))
    terminal = _strings("outcome.terminal_states", outcome.get("terminal_states"), required=True)
    if outcome["success_state"] not in terminal:
        raise DeliveryUnitError("outcome_success_not_terminal")
    scope = _mapping("scope", contract.get("scope"))
    _text("scope.ownership", scope.get("ownership"))
    _scope_paths("scope.allowed_paths", scope.get("allowed_paths"), required=True)
    _scope_paths("scope.forbidden_paths", scope.get("forbidden_paths"), required=True)
    _identity_names(scope)
    obligations = contract.get("obligations")
    if not isinstance(obligations, list) or not obligations:
        raise DeliveryUnitError("obligations_missing")
    obligation_ids: set[str] = set()
    receipt_names: set[str] = set()
    for item in obligations:
        obligation = _mapping("obligation", item)
        obligation_id = _text("obligation.id", obligation.get("id"))
        if obligation_id in obligation_ids:
            raise DeliveryUnitError("obligation_duplicate")
        obligation_ids.add(obligation_id)
        commands = obligation.get("commands")
        if not isinstance(commands, list) or not commands:
            raise DeliveryUnitError(f"obligation.{obligation_id}.commands_missing")
        for command in commands:
            _validate_command(obligation_id, _mapping(f"obligation.{obligation_id}.command", command))
        receipt_names.update(_strings(f"obligation.{obligation_id}.required_receipts", obligation.get("required_receipts"), required=True))
    declared = _strings("required_receipts", contract.get("required_receipts"), required=True)
    if len(set(declared)) != len(declared) or not receipt_names <= set(declared):
        raise DeliveryUnitError("required_receipts_incomplete")
    boundary = _mapping("source_vs_live", contract.get("source_vs_live"))
    if boundary.get("source_must_not_claim_live") is not True or boundary.get("live_required_for_source_delivery") is not False:
        raise DeliveryUnitError("source_live_boundary_invalid")
    repair = _mapping("repair_same_unit", contract.get("repair_same_unit"))
    if repair.get("enabled") is not True or repair.get("route") not in {"same_work_unit_new_attempt", "same_unit_new_attempt"}:
        raise DeliveryUnitError("repair_same_unit_invalid")
    max_attempts = repair.get("max_attempts")
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
        raise DeliveryUnitError("repair_attempt_budget_invalid")
    authority_store = contract.get("authority_store")
    if authority_store not in {"run", "work_unit"}:
        raise DeliveryUnitError("authority_store_invalid")
    source_receipts = contract.get("source_required_receipts")
    if source_receipts is not None:
        source_receipts = _strings("source_required_receipts", source_receipts, required=True)
        if not set(source_receipts) <= set(declared):
            raise DeliveryUnitError("source_required_receipts_invalid")
    source_obligations = contract.get("source_obligation_ids")
    if source_obligations is not None:
        source_obligations = _strings("source_obligation_ids", source_obligations, required=True)
        if not set(source_obligations) <= obligation_ids:
            raise DeliveryUnitError("source_obligation_ids_invalid")
    effective_source_ids = source_obligations if source_obligations is not None else [
        item["id"] for item in obligations
    ]
    effective_source_receipts = source_receipts if source_receipts is not None else declared
    source_receipt_union = {
        receipt
        for item in obligations
        if item["id"] in effective_source_ids
        for receipt in item["required_receipts"]
    }
    if not source_receipt_union <= set(effective_source_receipts):
        raise DeliveryUnitError("source_required_receipts_incomplete")
    _text("managed_scope", contract.get("managed_scope"))
    _walk_no_placeholders(body)
    return copy.deepcopy(dict(contract))


def seal_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    body = _without(contract, "contract_digest")
    validate_contract(body, require_digest=False)
    return {**body, "contract_digest": digest_json(body)}


def load_canonical_contract(path: str | Path, *, unit_id: str | None = None) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DeliveryUnitError("canonical_contract_unreadable") from exc
    selected: Any = value
    if isinstance(value, Mapping):
        units = value.get("units")
        if unit_id is not None and isinstance(units, list):
            selected = next((item for item in units if isinstance(item, Mapping) and item.get("unit_id") == unit_id), None)
        elif unit_id is not None and isinstance(units, Mapping):
            selected = units.get(unit_id)
        elif unit_id is not None and isinstance(value.get("source_repair_unit"), Mapping) and value["source_repair_unit"].get("unit_id") == unit_id:
            selected = value["source_repair_unit"]
        elif unit_id is not None and value.get("unit_id") != unit_id:
            selected = None
    if selected is None:
        raise DeliveryUnitError("canonical_contract_unit_missing")
    return validate_contract(selected)


def _canonical_identity(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "goal_id": contract["goal"]["id"],
        "goal_revision": contract["goal"]["revision"],
        "node_id": contract["node"]["id"],
        "unit_id": contract["unit_id"],
    }


def plan_delivery_unit(contract: Mapping[str, Any]) -> dict[str, Any]:
    resolved = validate_contract(contract)
    body = {
        "schema": PLAN_SCHEMA,
        "status": "sealed",
        "verdict": "GREEN",
        **_canonical_identity(resolved),
        "contract_digest": resolved["contract_digest"],
        "planner_principal": resolved["planner"]["principal"],
        "obligation_ids": [item["id"] for item in resolved["obligations"]],
        "required_receipts": list(resolved["required_receipts"]),
        "repair_same_unit": copy.deepcopy(resolved["repair_same_unit"]),
        "source_vs_live": copy.deepcopy(resolved["source_vs_live"]),
        "authority_store": resolved["authority_store"],
    }
    return {**body, "plan_verdict_digest": digest_json(body)}


def verify_plan_verdict(plan: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    try:
        resolved = validate_contract(contract)
        if not isinstance(plan, Mapping) or plan.get("schema") != PLAN_SCHEMA:
            raise DeliveryUnitError("plan_verdict_schema_invalid")
        supplied = _digest("plan_verdict_digest", plan.get("plan_verdict_digest"))
        if supplied != digest_json(_without(plan, "plan_verdict_digest")):
            raise DeliveryUnitError("plan_verdict_digest_mismatch")
        if plan.get("status") != "sealed" or plan.get("verdict") != "GREEN":
            raise DeliveryUnitError("plan_verdict_not_green")
        if plan.get("contract_digest") != resolved["contract_digest"]:
            raise DeliveryUnitError("plan_contract_digest_mismatch")
        if plan.get("planner_principal") != resolved["planner"]["principal"]:
            raise DeliveryUnitError("plan_planner_principal_mismatch")
        for field, expected in _canonical_identity(resolved).items():
            if plan.get(field) != expected:
                raise DeliveryUnitError("plan_identity_mismatch")
        if plan.get("obligation_ids") != [item["id"] for item in resolved["obligations"]]:
            raise DeliveryUnitError("plan_obligations_mismatch")
        if plan.get("required_receipts") != resolved["required_receipts"]:
            raise DeliveryUnitError("plan_required_receipts_mismatch")
    except DeliveryUnitError as exc:
        return {"schema": PLAN_SCHEMA, "verdict": "RED", "reason": exc.reason}
    return {"schema": PLAN_SCHEMA, "verdict": "GREEN", "reason": "delivery_unit_plan_verified", "plan_verdict_digest": supplied, "contract_digest": resolved["contract_digest"], "unit_id": resolved["unit_id"]}


def bind_packet(packet: Mapping[str, Any], plan: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    resolved = validate_contract(contract)
    if verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
        raise DeliveryUnitError("plan_verifier_not_green")
    if not isinstance(packet, Mapping):
        raise DeliveryUnitError("packet_missing")
    _verify_packet_scope(packet, resolved)
    body = copy.deepcopy(dict(packet))
    body.update({
        "delivery_unit_managed": True,
        "delivery_unit_id": resolved["unit_id"],
        "delivery_unit_contract_digest": resolved["contract_digest"],
        "delivery_plan_verdict": copy.deepcopy(dict(plan)),
        "delivery_plan_verdict_digest": plan["plan_verdict_digest"],
        "delivery_required_receipts": list(resolved["required_receipts"]),
    })
    return body


def verify_packet_binding(packet: Mapping[str, Any], contract: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    try:
        resolved = validate_contract(contract)
        if verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
            raise DeliveryUnitError("plan_verifier_not_green")
        if not isinstance(packet, Mapping) or packet.get("delivery_unit_managed") is not True:
            raise DeliveryUnitError("delivery_unit_binding_missing")
        _verify_packet_scope(packet, resolved)
        for field, expected in {
            "delivery_unit_id": resolved["unit_id"],
            "delivery_unit_contract_digest": resolved["contract_digest"],
            "delivery_plan_verdict_digest": plan["plan_verdict_digest"],
            "delivery_required_receipts": resolved["required_receipts"],
        }.items():
            if packet.get(field) != expected:
                raise DeliveryUnitError(f"packet_{field}_mismatch")
        embedded = packet.get("delivery_plan_verdict")
        if not isinstance(embedded, Mapping) or dict(embedded) != dict(plan):
            raise DeliveryUnitError("packet_plan_verdict_mismatch")
        for field, expected in _canonical_identity(resolved).items():
            if field in packet and packet.get(field) != expected:
                raise DeliveryUnitError(f"packet_{field}_mismatch")
    except DeliveryUnitError as exc:
        return {"schema": ADMISSION_SCHEMA, "verdict": "RED", "reason": exc.reason}
    body = {"schema": ADMISSION_SCHEMA, "verdict": "GREEN", "reason": "delivery_unit_packet_verified", "unit_id": resolved["unit_id"], "contract_digest": resolved["contract_digest"], "plan_verdict_digest": plan["plan_verdict_digest"]}
    return {**body, "receipt_digest": receipt_digest(body)}


def bind_legacy_sidecar(materialized: Mapping[str, Any], contract: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    resolved = validate_contract(contract)
    if verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
        raise DeliveryUnitError("plan_verifier_not_green")
    packet = materialized.get("packet") if isinstance(materialized.get("packet"), Mapping) else materialized
    if not isinstance(packet, Mapping):
        raise DeliveryUnitError("legacy_packet_missing")
    _verify_packet_scope(packet, resolved)
    materialized_body = _without(materialized, "packet_digest")
    packet_digest = materialized.get("packet_digest") or digest_json(materialized_body)
    body = {
        "schema": LEGACY_SIDECAR_SCHEMA,
        "status": "bound",
        "packet_id": packet.get("packet_id"),
        "packet_digest": packet_digest,
        **_canonical_identity(resolved),
        "delivery_plan_verdict": copy.deepcopy(dict(plan)),
        "delivery_plan_verdict_digest": plan["plan_verdict_digest"],
        "delivery_required_receipts": list(resolved["required_receipts"]),
        "contract_digest": resolved["contract_digest"],
    }
    return {**body, "receipt_digest": receipt_digest(body)}


def verify_legacy_sidecar(materialized: Mapping[str, Any], sidecar: Mapping[str, Any], contract: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    try:
        resolved = validate_contract(contract)
        if not isinstance(sidecar, Mapping) or sidecar.get("schema") != LEGACY_SIDECAR_SCHEMA or sidecar.get("status") != "bound" or not verify_receipt(sidecar):
            raise DeliveryUnitError("legacy_delivery_sidecar_invalid")
        packet = materialized.get("packet") if isinstance(materialized.get("packet"), Mapping) else materialized
        materialized_digest = materialized.get("packet_digest")
        if sidecar.get("packet_digest") != materialized_digest or sidecar.get("packet_id") != packet.get("packet_id"):
            raise DeliveryUnitError("legacy_delivery_sidecar_packet_mismatch")
        _verify_packet_scope(packet, resolved)
        for field, expected in _canonical_identity(resolved).items():
            if sidecar.get(field) != expected:
                raise DeliveryUnitError("legacy_delivery_sidecar_identity_mismatch")
        if sidecar.get("contract_digest") != resolved["contract_digest"] or sidecar.get("delivery_plan_verdict_digest") != plan.get("plan_verdict_digest") or sidecar.get("delivery_plan_verdict") != plan:
            raise DeliveryUnitError("legacy_delivery_sidecar_contract_mismatch")
        if sidecar.get("delivery_required_receipts") != resolved["required_receipts"]:
            raise DeliveryUnitError("legacy_delivery_sidecar_receipts_mismatch")
    except DeliveryUnitError as exc:
        return {"schema": ADMISSION_SCHEMA, "verdict": "RED", "reason": exc.reason}
    body = {"schema": ADMISSION_SCHEMA, "verdict": "GREEN", "reason": "legacy_delivery_sidecar_verified", "unit_id": resolved["unit_id"], "contract_digest": resolved["contract_digest"], "plan_verdict_digest": plan["plan_verdict_digest"]}
    return {**body, "receipt_digest": receipt_digest(body)}


def verify_dispatch_binding(*, envelope: Mapping[str, Any], packet: Mapping[str, Any], contract: Mapping[str, Any], plan: Mapping[str, Any]) -> dict[str, Any]:
    result = verify_packet_binding(packet, contract, plan)
    if result.get("verdict") != "GREEN":
        return result
    resolved = validate_contract(contract)
    for field, expected in _canonical_identity(resolved).items():
        if field in envelope and envelope.get(field) != expected:
            return {"schema": ADMISSION_SCHEMA, "verdict": "RED", "reason": f"envelope_{field}_mismatch"}
    if envelope.get("delivery_unit_contract_digest") not in (None, resolved["contract_digest"]):
        return {"schema": ADMISSION_SCHEMA, "verdict": "RED", "reason": "envelope_contract_digest_mismatch"}
    if envelope.get("delivery_unit_id") not in (None, resolved["unit_id"]):
        return {"schema": ADMISSION_SCHEMA, "verdict": "RED", "reason": "envelope_unit_id_mismatch"}
    body = {key: value for key, value in result.items() if key != "receipt_digest"}
    body["envelope_bound"] = True
    return {**body, "receipt_digest": receipt_digest(body)}


def receipt_digest(receipt: Mapping[str, Any]) -> str:
    body = _without(receipt, "receipt_digest")
    if body.get("schema") == "lh-successor-executor-receipt/v1":
        body.pop("reused", None)
        body.pop("invoked", None)
    return digest_json(body)


def verify_receipt(receipt: Any, *, expected_digest: str | None = None) -> bool:
    if not isinstance(receipt, Mapping):
        return False
    supplied = receipt.get("receipt_digest")
    if isinstance(supplied, str):
        valid = supplied == receipt_digest(receipt)
    else:
        supplied = receipt.get("plan_verdict_digest")
        valid = isinstance(supplied, str) and supplied == digest_json(_without(receipt, "plan_verdict_digest"))
    return valid and (expected_digest is None or supplied == expected_digest)


def _resolve_command_cwd(worktree: str | Path, command_cwd: str) -> str:
    root = Path(worktree).expanduser().resolve()
    rendered = command_cwd.replace("${WORKTREE}", str(root))
    cwd = Path(rendered).expanduser().resolve()
    if cwd != root and root not in cwd.parents:
        raise DeliveryUnitError("delivery_command_cwd_escape")
    return str(cwd)


def _expected_identity(contract: Mapping[str, Any], evidence_identity: Mapping[str, Any]) -> dict[str, Any]:
    expected = _canonical_identity(contract)
    scope = _mapping("scope", contract["scope"])
    for name in _identity_names(scope):
        if name in expected:
            continue
        value = evidence_identity.get(name)
        if value in (None, ""):
            raise DeliveryUnitError(f"delivery_identity_{name}_missing")
        if name.endswith("_digest"):
            _digest(f"delivery_identity_{name}", value)
        elif name == "base_sha" and (not isinstance(value, str) or COMMIT_RE.fullmatch(value.lower()) is None):
            raise DeliveryUnitError("delivery_identity_base_sha_invalid")
        expected[name] = value
    return expected


def recovery_executor_receipt(
    contract: Mapping[str, Any], admission: Mapping[str, Any],
    dispatch: Mapping[str, Any], packet_admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Project unknown execution from its bound receipts, never from expected values."""
    resolved = validate_contract(contract)
    def require(ok: bool, reason: str) -> None:
        if not ok:
            raise DeliveryUnitError("delivery_recovery_executor_" + reason)
    require(isinstance(admission, Mapping) and isinstance(dispatch, Mapping)
            and isinstance(packet_admission, Mapping), "sources_missing")
    admission_body = {k: v for k, v in admission.items()
                      if k not in {"input_digest", "status", "admitted_at"}}
    require(admission.get("input_digest") == digest_json(admission_body), "admission_seal")
    require(verify_receipt(dispatch) and dispatch.get("executor_status") == "unknown", "dispatch_seal")
    unknown = dispatch.get("executor_recovery_receipt")
    require(isinstance(unknown, Mapping), "unknown_missing")
    require(unknown.get("recovery_receipt_digest") == digest_json(
        {k: v for k, v in unknown.items() if k != "recovery_receipt_digest"})
        and unknown.get("outcome") == "unknown", "unknown_seal")
    dispatch_digest = admission.get("current_dispatch_receipt_digest") or (
        admission.get("current_dispatch_receipt") or {}).get("receipt_digest")
    unknown_digest = admission.get("current_unknown_recovery_receipt_digest") or (
        admission.get("current_unknown_recovery_receipt") or {}).get("recovery_receipt_digest")
    require(dispatch_digest == dispatch["receipt_digest"]
            and unknown_digest == unknown["recovery_receipt_digest"], "source_binding")
    require(verify_receipt(packet_admission) and packet_admission.get("verdict") == "GREEN"
            and packet_admission.get("schema") == ADMISSION_SCHEMA, "packet_admission")
    require(admission.get("delivery_contract_digest") == packet_admission.get("contract_digest")
            == resolved["contract_digest"] and packet_admission.get("unit_id") == resolved["unit_id"],
            "contract_binding")
    identity = {}
    for field in ("goal_id", "goal_revision", "node_id", "dispatch_key", "work_unit_id", "run_id", "attempt", "fence"):
        value = dispatch.get(field)
        require(value not in (None, "") and value == unknown.get(field), "identity_" + field)
        if field in {"goal_id", "goal_revision", "node_id"}:
            require(value == _canonical_identity(resolved)[field], "contract_" + field)
        else:
            require(value == admission.get(field), "admission_" + field)
        identity[field] = value
    body = {
        "schema": "host-p7-candidate-recovery-executor-receipt/v1",
        "status": "unknown_preserved", "executor_status": "unknown", **identity,
        "contract_digest": admission["delivery_contract_digest"],
        "unit_id": packet_admission["unit_id"],
        "candidate_recovery_admission_digest": admission["input_digest"],
        "recovery_receipt_digest": unknown["recovery_receipt_digest"],
        "dispatch_receipt_digest": dispatch["receipt_digest"],
        "packet_admission_receipt_digest": packet_admission["receipt_digest"],
    }
    return {**body, "receipt_digest": digest_json(body)}


def repaired_candidate_evidence(
    contract: Mapping[str, Any], binding: Mapping[str, Any],
    original_candidate: Mapping[str, Any], original_checks: Mapping[str, Any],
    repaired_checks: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive current candidate evidence from sealed repair sources, not expectations.

    This is not an execution receipt or a new journal phase.  The original
    candidate and checks remain intact and are explicitly cited as history.
    """
    resolved = validate_contract(contract)
    def require(ok: bool, reason: str) -> None:
        if not ok:
            raise DeliveryUnitError("delivery_repaired_candidate_" + reason)
    require(isinstance(binding, Mapping) and binding.get("schema") == "host-p7-checks-repair-binding/v1"
            and binding.get("repair_digest") == digest_json(_without(binding, "repair_digest")), "binding_seal")
    sources = ((original_candidate, "candidate"), (original_checks, "checks"), (repaired_checks, "checks_repair"))
    for value, phase in sources:
        require(verify_receipt(value) and value.get("phase") == phase, "source_seal")
        require(value.get("contract_digest") == resolved["contract_digest"], "source_contract")
        for field, expected in _canonical_identity(resolved).items():
            require(value.get(field) == expected, "source_" + field)
        for field in ("goal_id", "goal_revision", "node_id", "dispatch_key", "run_id", "attempt", "fence"):
            require(value.get(field) not in (None, "") and value.get(field) == binding.get(field), "source_" + field)
        require(value.get("base_sha") == original_candidate.get("base_sha"), "source_base")
    require(original_checks.get("verdict") == "RED" and repaired_checks.get("verdict") == "GREEN", "source_verdict")
    require(binding.get("original_candidate_receipt_digest") == original_candidate["receipt_digest"]
            and binding.get("original_checks_receipt_digest") == original_checks["receipt_digest"]
            and binding.get("candidate_before_digest") == original_candidate.get("candidate_digest")
            == original_checks.get("candidate_digest")
            and binding.get("candidate_after_digest") == repaired_checks.get("candidate_digest"), "source_link")
    lineage = repaired_checks.get("checks_repair")
    require(isinstance(lineage, Mapping) and lineage.get("schema") == "host-p7-checks-repair-evidence/v1", "lineage_missing")
    for field in ("repair_id", "repair_digest", "original_candidate_receipt_digest", "original_checks_receipt_digest",
                  "candidate_before_digest", "candidate_after_digest", "checks_command_digest", "predecessor_receipt_digest"):
        require(lineage.get(field) == binding.get(field), "lineage_" + field)
    require(all(binding.get(k) is True and lineage.get(k) is True for k in ("same_run", "same_attempt", "same_fence"))
            and binding.get("allow_retry") is False and binding.get("new_attempt") is False
            and lineage.get("new_attempt") is False
            and all(binding.get(k) == lineage.get(k) == 0 for k in ("provider_invocations", "manual_prompts")), "lineage_route")
    paths = binding.get("repair_scope", {}).get("changed_paths")
    require(isinstance(paths, list) and paths and lineage.get("changed_paths") == paths, "lineage_scope")
    commands = binding.get("checks_commands")
    rows = repaired_checks.get("checks")
    require(isinstance(commands, list) and commands and digest_json(commands) == binding.get("checks_command_digest")
            and isinstance(rows, list) and len(rows) == len(commands), "commands")
    require(all(row.get("id") == command.get("id") and row.get("exit_code") == row.get("expect_exit")
                == command.get("expect_exit", 0) for row, command in zip(rows, commands)), "command_result")
    identity = {field: repaired_checks[field] for field in (
        "goal_id", "goal_revision", "node_id", "unit_id", "dispatch_key", "run_id", "attempt", "fence", "base_sha", "diff_digest")}
    _digest("repaired_candidate.diff_digest", identity["diff_digest"])
    _digest("repaired_candidate.original_diff_digest", original_candidate.get("diff_digest"))
    require(original_checks.get("diff_digest") == original_candidate.get("diff_digest"), "original_diff")
    body = {"schema": REPAIRED_CANDIDATE_SCHEMA, "status": "derived_from_checks_repair",
        "execution_receipt": False, "contract_digest": repaired_checks["contract_digest"], **identity,
        "work_unit_id": binding["work_unit_id"], "candidate_digest": repaired_checks["candidate_digest"],
        "candidate_commit": repaired_checks["candidate_commit"], "candidate_kind": repaired_checks["candidate_kind"],
        "changed_paths": list(paths), "lineage": {
            "original_candidate_receipt_digest": original_candidate["receipt_digest"],
            "original_checks_receipt_digest": original_checks["receipt_digest"],
            "checks_repair_receipt_digest": repaired_checks["receipt_digest"],
            "repair_digest": binding["repair_digest"], "original_manifest_digest": binding["original_manifest_digest"],
            "candidate_before_digest": original_candidate["candidate_digest"],
            "original_diff_digest": original_candidate["diff_digest"],
            "candidate_after_digest": repaired_checks["candidate_digest"],
        }}
    return {**body, "receipt_digest": digest_json(body)}


def _verify_repaired_delivery_chain(contract: Mapping[str, Any], evidence: Mapping[str, Any],
                                    *, phase: str, terminal: bool) -> None:
    """Validate both sides of the repair plus every downstream receipt edge."""
    def require(ok: bool, reason: str) -> None:
        if not ok:
            raise DeliveryUnitError("delivery_repair_chain_" + reason)
    receipts = evidence["receipts"]
    binding = evidence.get("checks_repair_binding")
    candidate = repaired_candidate_evidence(contract, binding, receipts.get("original_candidate"),
        receipts.get("original_checks"), receipts.get("checks_repair"))
    require(receipts.get("candidate") == candidate, "candidate")
    checks = receipts["checks_repair"]
    require(receipts.get("checks") == checks and evidence.get("checks_repair") == checks.get("checks_repair"), "checks")
    dispatch = receipts.get("dispatch")
    require(verify_receipt(dispatch) and binding.get("envelope_digest") == dispatch.get("envelope_digest")
            and binding.get("work_unit_id") == dispatch.get("work_unit_id"), "dispatch")
    recovery = evidence.get("candidate_recovery_admission")
    if recovery is not None:
        require(isinstance(recovery, Mapping) and recovery.get("candidate_digest") == candidate["lineage"]["candidate_before_digest"]
                and recovery.get("approved_manifest_context", {}).get("approved_manifest_digest") == binding["original_manifest_digest"],
                "recovery_admission")
        require(checks.get("candidate_recovery", {}).get("admission_digest") == recovery.get("input_digest"), "recovery_link")
    verifier = receipts.get("verifier")
    require(verify_receipt(verifier) and verifier.get("phase") == "verifier" and verifier.get("verdict") == "GREEN"
            and verifier.get("candidate_digest") == candidate["candidate_digest"]
            and verifier.get("checks_digest") == checks["receipt_digest"], "verifier")
    if phase != "final":
        return
    integrated, checked, verified = (receipts.get(name) for name in ("integration", "integration_checks", "integration_verifier"))
    require(all(verify_receipt(value) and value.get("phase") == name for value, name in
        ((integrated, "integration"), (checked, "integration_checks"), (verified, "integration_verifier"))), "integration_seal")
    require(integrated.get("source_candidate_digest") == candidate["candidate_digest"]
            and integrated.get("integration_candidate_digest") == checked.get("candidate_digest") == verified.get("candidate_digest")
            and checked.get("verdict") == verified.get("verdict") == "GREEN"
            and verified.get("checks_digest") == checked["receipt_digest"], "integration_link")
    eligibility = receipts.get("delivery_verifier")
    if eligibility is not None:
        require(verify_receipt(eligibility) and eligibility.get("phase") == "delivery_verifier"
                and eligibility.get("verdict") == "GREEN", "delivery_verifier")
        original = copy.deepcopy(dict(evidence))
        original["terminal_state"] = "eligible"
        original.pop("state_readback", None)
        original["receipts"].pop("delivery_verifier", None)
        original["receipts"].pop("machine_complete", None)
        original["receipts"]["completion"] = original["receipts"]["integration_verifier"]
        require(eligibility.get("delivery_evidence_digest") == digest_json(original), "delivery_evidence")
    if terminal:
        machine = receipts.get("machine_complete")
        require(verify_receipt(machine) and machine.get("phase") == "machine_complete", "machine_seal")
        stages = {name: receipts[name]["receipt_digest"] for name in (
            "candidate", "checks", "verifier", "integration", "integration_checks", "integration_verifier", "delivery_verifier")}
        stages.update(checks_repair=checks["receipt_digest"], checks_original=receipts["original_checks"]["receipt_digest"],
                      candidate_original=receipts["original_candidate"]["receipt_digest"])
        require(machine.get("stage_receipts") == stages, "machine_link")
        require(receipts.get("completion", {}).get("source_receipt_digest") == machine["receipt_digest"], "completion_link")


def verify_delivery(
    contract: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    phase: str = "final",
    allow_machine_complete_missing: bool = False,
    allow_delivery_verifier_missing: bool = False,
) -> dict[str, Any]:
    """Verify the complete obligation and receipt chain for one unit."""
    try:
        if phase not in {"source", "final"}:
            raise DeliveryUnitError("delivery_phase_invalid")
        resolved = validate_contract(contract)
        if not isinstance(evidence, Mapping):
            raise DeliveryUnitError("delivery_evidence_missing")
        if evidence.get("contract_digest") != resolved["contract_digest"]:
            raise DeliveryUnitError("delivery_contract_digest_mismatch")
        if evidence.get("unit_id") != resolved["unit_id"]:
            raise DeliveryUnitError("delivery_unit_id_mismatch")
        if phase == "final" and not allow_machine_complete_missing and evidence.get("terminal_state") != resolved["outcome"]["success_state"]:
            raise DeliveryUnitError("delivery_terminal_state_mismatch")
        worktree = evidence.get("worktree")
        if not isinstance(worktree, str) or not worktree.strip():
            raise DeliveryUnitError("delivery_worktree_missing")
        identity = evidence.get("identity")
        if not isinstance(identity, Mapping):
            raise DeliveryUnitError("delivery_identity_missing")
        expected_identity = _expected_identity(resolved, identity)
        for field, expected in _canonical_identity(resolved).items():
            if identity.get(field) != expected:
                raise DeliveryUnitError(f"delivery_identity_{field}_mismatch")
        if "candidate_digest" in expected_identity:
            candidate = expected_identity["candidate_digest"]
            if evidence.get("candidate_digest_before") != candidate or evidence.get("candidate_digest_after") != candidate or evidence.get("candidate_unchanged") is not True:
                raise DeliveryUnitError("delivery_candidate_snapshot_invalid")
        if evidence.get("verdict") != "GREEN":
            raise DeliveryUnitError("delivery_evidence_not_green")
        packet = evidence.get("packet")
        if not isinstance(packet, Mapping):
            raise DeliveryUnitError("delivery_packet_missing")
        verify_changed_paths_scope(evidence.get("changed_paths"), packet, resolved)
        verifier = evidence.get("verifier")
        expected_verifier = resolved["independent_verifier"]
        if not isinstance(verifier, Mapping):
            raise DeliveryUnitError("delivery_independent_verifier_missing")
        for field, expected in (("principal", expected_verifier["principal"]), ("read_only", True), ("source_write", False), ("verdict", "GREEN"), ("contract_digest", resolved["contract_digest"])):
            if verifier.get(field) != expected:
                raise DeliveryUnitError(f"delivery_independent_verifier_{field}_invalid")
        verifier_receipt = verifier.get("receipt")
        if not isinstance(verifier_receipt, Mapping) or verifier_receipt.get("principal") != expected_verifier["principal"] or verifier_receipt.get("read_only") is not True or verifier_receipt.get("source_write") is not False or verifier_receipt.get("contract_digest") != resolved["contract_digest"] or not verify_receipt(verifier_receipt):
            raise DeliveryUnitError("delivery_independent_verifier_receipt_invalid")
        plan = evidence.get("plan_verdict")
        if verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
            raise DeliveryUnitError("delivery_plan_verdict_missing")
        receipts = evidence.get("receipts")
        if not isinstance(receipts, Mapping):
            raise DeliveryUnitError("delivery_receipts_missing")
        candidate_value, checks_value = receipts.get("candidate"), receipts.get("checks")
        if (evidence.get("checks_repair") is not None or evidence.get("checks_repair_binding") is not None
            or (isinstance(candidate_value, Mapping) and candidate_value.get("schema") == REPAIRED_CANDIDATE_SCHEMA)
            or (isinstance(checks_value, Mapping) and checks_value.get("phase") == "checks_repair")):
            try:
                _verify_repaired_delivery_chain(resolved, evidence, phase=phase,
                    terminal=phase == "final" and not allow_machine_complete_missing)
            except (KeyError, TypeError, AttributeError) as exc:
                raise DeliveryUnitError("delivery_repair_chain_incomplete") from exc
        evidence_phase = evidence.get("phase")
        if evidence_phase is not None and evidence_phase != phase:
            raise DeliveryUnitError("delivery_phase_mismatch")
        if phase == "source":
            required = list(resolved.get("source_required_receipts") or resolved["required_receipts"])
        else:
            required = list(resolved["required_receipts"])
        if allow_machine_complete_missing:
            required = [name for name in required if name != "machine_complete"]
        if allow_delivery_verifier_missing:
            required = [name for name in required if name != "delivery_verifier"]
        recovery_evidence = evidence.get("candidate_recovery")
        recovery_admission = evidence.get("candidate_recovery_admission")
        if recovery_evidence is not None:
            if not isinstance(recovery_admission, Mapping):
                raise DeliveryUnitError("delivery_recovery_admission_missing")
            admission_body = {key: item for key, item in recovery_admission.items()
                              if key not in {"input_digest", "status", "admitted_at"}}
            if recovery_admission.get("input_digest") != digest_json(admission_body):
                raise DeliveryUnitError("delivery_recovery_admission_digest_invalid")
            if recovery_admission.get("input_digest") != recovery_evidence.get("admission_digest"):
                raise DeliveryUnitError("delivery_recovery_admission_evidence_mismatch")
            required_recovery_fields = (
                "run_id", "attempt", "fence", "dispatch_key",
                "completion_contract_digest", "delivery_contract_digest",
                "owner_authorization_digest", "candidate_digest",
                "source_attempt", "source_workspace_inventory", "no_inflight_evidence",
            )
            if any(recovery_admission.get(field) in (None, "") for field in required_recovery_fields):
                raise DeliveryUnitError("delivery_recovery_admission_identity_missing")
            recovery_dispatch_digest = recovery_admission.get("current_dispatch_receipt_digest")
            if recovery_dispatch_digest is None and isinstance(recovery_admission.get("current_dispatch_receipt"), Mapping):
                recovery_dispatch_digest = recovery_admission["current_dispatch_receipt"].get("receipt_digest")
            recovery_unknown_digest = recovery_admission.get("current_unknown_recovery_receipt_digest")
            if recovery_unknown_digest is None and isinstance(recovery_admission.get("current_unknown_recovery_receipt"), Mapping):
                recovery_unknown_digest = recovery_admission["current_unknown_recovery_receipt"].get("recovery_receipt_digest")
            if not recovery_dispatch_digest or not recovery_unknown_digest:
                raise DeliveryUnitError("delivery_recovery_receipt_binding_missing")
            for field, expected in (("run_id", identity.get("run_id")),
                                    ("attempt", identity.get("attempt")),
                                    ("fence", identity.get("fence")),
                                    ("dispatch_key", identity.get("dispatch_key"))):
                if recovery_admission.get(field) != expected:
                    raise DeliveryUnitError("delivery_recovery_identity_mismatch")
            if recovery_admission.get("delivery_contract_digest") != resolved["contract_digest"]:
                raise DeliveryUnitError("delivery_recovery_contract_digest_mismatch")
            if recovery_evidence.get("completion_contract_digest") != recovery_admission.get("completion_contract_digest"):
                raise DeliveryUnitError("delivery_recovery_completion_contract_digest_mismatch")
            source_attempt = recovery_admission["source_attempt"]
            if (not isinstance(source_attempt, Mapping)
                or source_attempt.get("run_id") != recovery_admission["run_id"]
                or not source_attempt.get("workspace_ref")):
                raise DeliveryUnitError("delivery_recovery_source_attempt_invalid")
            if not isinstance(recovery_admission["source_workspace_inventory"], Mapping):
                raise DeliveryUnitError("delivery_recovery_source_inventory_invalid")
            if not isinstance(recovery_admission["no_inflight_evidence"], Mapping):
                raise DeliveryUnitError("delivery_recovery_no_inflight_invalid")
            dispatch_receipt = evidence.get("receipts", {}).get("dispatch") if isinstance(evidence.get("receipts"), Mapping) else None
            if (not isinstance(dispatch_receipt, Mapping)
                or not verify_receipt(dispatch_receipt)
                or dispatch_receipt.get("executor_status") != "unknown"
                or not isinstance(dispatch_receipt.get("executor_recovery_receipt"), Mapping)):
                raise DeliveryUnitError("delivery_recovery_dispatch_unknown_missing")
        def _verify_recovery_executor(value: Any) -> bool:
            if not isinstance(recovery_evidence, Mapping) or not isinstance(value, Mapping):
                return False
            if value.get("schema") != "host-p7-candidate-recovery-executor-receipt/v1":
                return False
            if value.get("status") != "unknown_preserved" or value.get("executor_status") != "unknown":
                return False
            if value.get("candidate_recovery_admission_digest") != recovery_evidence.get("admission_digest"):
                return False
            required_variant_fields = (
                "dispatch_key", "run_id", "attempt", "fence", "contract_digest",
                "unit_id", "goal_id", "goal_revision", "node_id",
            )
            if any(value.get(field) in (None, "") for field in required_variant_fields):
                return False
            try:
                proven = recovery_executor_receipt(resolved, recovery_admission,
                    dispatch_receipt, receipts.get("packet_admission"))
            except (DeliveryUnitError, TypeError, KeyError):
                return False
            # Every newly produced field is derived from checked source receipts.
            return dict(value) == proven
        missing = [name for name in required
                   if not (_verify_recovery_executor(receipts.get(name)) if name == "executor" and recovery_evidence is not None
                           else verify_receipt(receipts.get(name)))]
        if missing:
            raise DeliveryUnitError("delivery_required_receipt_missing", detail=missing)
        for name in required:
            receipt = receipts[name]
            if name == "executor" and recovery_evidence is not None:
                if not _verify_recovery_executor(receipt):
                    raise DeliveryUnitError("delivery_recovery_executor_receipt_invalid")
                for field, expected in expected_identity.items():
                    if field in receipt and receipt.get(field) != expected:
                        raise DeliveryUnitError("delivery_recovery_executor_identity_mismatch")
                if receipt.get("contract_digest") != resolved["contract_digest"]:
                    raise DeliveryUnitError("delivery_recovery_executor_contract_digest_mismatch")
                continue
            if receipt.get("contract_digest") not in (None, resolved["contract_digest"]):
                raise DeliveryUnitError("delivery_receipt_contract_digest_mismatch", detail=name)
            for field, expected in expected_identity.items():
                if field in receipt and receipt.get(field) != expected:
                    raise DeliveryUnitError("delivery_receipt_identity_mismatch", detail=name)
        if not allow_machine_complete_missing and "completion" in required:
            completion = receipts.get("completion")
            if not isinstance(completion, Mapping) or completion.get("terminal_state") != resolved["outcome"]["success_state"]:
                raise DeliveryUnitError("delivery_completion_terminal_state_missing")
        obligations = evidence.get("obligations")
        if not isinstance(obligations, Mapping):
            raise DeliveryUnitError("delivery_obligations_missing")
        failed: list[str] = []
        for obligation in _phase_obligations(resolved, phase):
            oid = obligation["id"]
            row = obligations.get(oid)
            commands = [dict(command) for command in obligation["commands"]]
            if not isinstance(row, Mapping) or row.get("schema") != "host-delivery-unit-obligation-receipt/v1" or row.get("verdict") != "GREEN" or row.get("contract_digest") != resolved["contract_digest"] or row.get("commands") != commands or not verify_receipt(row):
                failed.append(oid)
                continue
            results = row.get("results")
            if not isinstance(results, list) or len(results) != len(commands):
                failed.append(oid)
                continue
            for result, command in zip(results, commands):
                if not isinstance(result, Mapping) or result.get("schema") != "host-delivery-unit-command-receipt/v1" or result.get("command_id") != command["id"] or result.get("argv") != command["argv"] or result.get("exit_code") != command.get("expect_exit", 0) or result.get("expect_exit") != command.get("expect_exit", 0) or result.get("cwd") != _resolve_command_cwd(worktree, command["cwd"]) or not verify_receipt(result):
                    failed.append(oid)
                    break
        if failed:
            raise DeliveryUnitError("delivery_obligation_failed", detail=failed)
        if evidence.get("source_vs_live") != resolved["source_vs_live"]:
            raise DeliveryUnitError("delivery_source_live_boundary_missing")
        if phase == "final" and not allow_machine_complete_missing:
            readback = evidence.get("state_readback")
            if not isinstance(readback, Mapping):
                raise DeliveryUnitError("delivery_state_readback_invalid")
            authority = readback.get("authority_store") or readback.get("authority")
            if authority != resolved["authority_store"]:
                raise DeliveryUnitError("delivery_state_authority_invalid")
            actual = readback.get("terminal_state")
            if authority in {"work_unit", "workunit"}:
                actual = readback.get("work_unit_state", actual)
            elif authority == "run":
                actual = readback.get("run_state", actual)
            if actual != resolved["outcome"]["success_state"]:
                raise DeliveryUnitError("delivery_state_readback_invalid")
    except DeliveryUnitError as exc:
        result: dict[str, Any] = {"schema": DELIVERY_SCHEMA, "verdict": "RED", "reason": exc.reason}
        if exc.detail is not None:
            result["detail"] = exc.detail
        return result
    return {"schema": DELIVERY_SCHEMA, "verdict": "GREEN", "reason": "all_delivery_obligations_verified", "phase": phase, "authority_store": resolved["authority_store"], "unit_id": resolved["unit_id"], "contract_digest": resolved["contract_digest"]}


def _phase_obligations(resolved: Mapping[str, Any], phase: str) -> list[Mapping[str, Any]]:
    if phase not in {"source", "final"}:
        raise DeliveryUnitError("delivery_phase_invalid")
    source_ids = resolved.get("source_obligation_ids") if phase == "source" else None
    return [
        item for item in resolved["obligations"]
        if source_ids is None or item["id"] in source_ids
    ]


def obligation_ids_for_phase(contract: Mapping[str, Any], phase: str) -> list[str]:
    resolved = validate_contract(contract)
    return [item["id"] for item in _phase_obligations(resolved, phase)]


def run_obligation_commands(
    contract: Mapping[str, Any],
    worktree: str | Path,
    *,
    phase: str = "final",
    timeout_cap: float = 900.0,
    env: Mapping[str, str] | None = None,
    command_runner: Any = None,
    execution_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = validate_contract(contract)
    root = Path(worktree).expanduser().resolve()
    obligations: dict[str, Any] = {}
    for obligation in _phase_obligations(resolved, phase):
        results: list[dict[str, Any]] = []
        for command in obligation["commands"]:
            argv = [str(item) for item in command["argv"]]
            cwd = _resolve_command_cwd(root, command["cwd"])
            if command_runner is None:
                raise DeliveryUnitError("execution_fence_unavailable: delivery_command_port_missing")
            command_context = {**(execution_context or {}), "command_id":
                "delivery-obligation:" + digest_json([phase, obligation["id"], command["id"]])}
            completed, fence_evidence, _descriptor = command_runner(
                command_context, phase=command_context.get("execution_phase", "delivery_checks"),
                argv=argv, worktree=cwd,
                timeout_seconds=min(float(command.get("timeout_seconds", 300)), timeout_cap), env=env)
            body = {
                "schema": "host-delivery-unit-command-receipt/v1",
                "command_id": command["id"],
                "argv": argv,
                "cwd": cwd,
                "exit_code": completed.returncode,
                "expect_exit": command.get("expect_exit", 0),
                "stdout_digest": hashlib.sha256(completed.stdout.encode()).hexdigest(),
                "stderr_digest": hashlib.sha256(completed.stderr.encode()).hexdigest(),
                **fence_evidence,
            }
            results.append({**body, "receipt_digest": receipt_digest(body)})
        body = {
            "schema": "host-delivery-unit-obligation-receipt/v1",
            "obligation_id": obligation["id"],
            "contract_digest": resolved["contract_digest"],
            "commands": [dict(item) for item in obligation["commands"]],
            "results": results,
            "verdict": "GREEN" if all(item["exit_code"] == item["expect_exit"] for item in results) else "RED",
        }
        obligations[obligation["id"]] = {**body, "receipt_digest": receipt_digest(body)}
    return {"obligations": obligations, "command_receipts": obligations, "contract_digest": resolved["contract_digest"], "unit_id": resolved["unit_id"]}


def build_planning_request(
    binding: Mapping[str, Any],
    *,
    reason: str,
    state: str,
    fence: Any,
    request_id: str | None = None,
    run_id: str | None = None,
    attempt: Any = None,
) -> dict[str, Any]:
    """Build the one durable, idempotent migration/replan request shape.

    Persistence belongs to the RunStore/WorkUnitStore caller;
    this pure function prevents a request from becoming an unbound bypass.
    """
    if not isinstance(binding, Mapping):
        raise DeliveryUnitError("planning_binding_missing")
    reason = _text("planning.reason", reason)
    state = _text("planning.state", state)
    if isinstance(fence, bool) or not isinstance(fence, int) or fence < 0:
        raise DeliveryUnitError("planning.fence_invalid")
    if run_id is not None:
        run_id = _text("planning.run_id", run_id)
    if attempt is not None and (isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1):
        raise DeliveryUnitError("planning.attempt_invalid")
    body = {
        "schema": PLANNING_REQUEST_SCHEMA,
        "status": "pending",
        "request_id": request_id or "plan-" + digest_json({"binding": dict(binding), "reason": reason, "state": state, "fence": fence})[7:23],
        "binding": copy.deepcopy(dict(binding)),
        "reason": reason,
        "state": state,
        "fence": fence,
    }
    if run_id is not None:
        body["run_id"] = run_id
    if attempt is not None:
        body["attempt"] = attempt
    return {**body, "input_digest": digest_json(body)}


def verify_planning_request(request: Mapping[str, Any]) -> dict[str, Any]:
    try:
        if not isinstance(request, Mapping) or request.get("schema") != PLANNING_REQUEST_SCHEMA or request.get("status") != "pending":
            raise DeliveryUnitError("planning_request_schema_invalid")
        if not _text("planning.request_id", request.get("request_id")):
            raise DeliveryUnitError("planning_request_id_missing")
        if _digest("planning.input_digest", request.get("input_digest")) != digest_json(_without(request, "input_digest")):
            raise DeliveryUnitError("planning_request_digest_mismatch")
        _mapping("planning.binding", request.get("binding"))
        _text("planning.reason", request.get("reason"))
        _text("planning.state", request.get("state"))
        if isinstance(request.get("fence"), bool) or not isinstance(request.get("fence"), int) or request["fence"] < 0:
            raise DeliveryUnitError("planning.fence_invalid")
        if "run_id" in request:
            _text("planning.run_id", request.get("run_id"))
        if "attempt" in request and (isinstance(request.get("attempt"), bool) or not isinstance(request.get("attempt"), int) or request["attempt"] < 1):
            raise DeliveryUnitError("planning.attempt_invalid")
    except DeliveryUnitError as exc:
        return {"schema": PLANNING_REQUEST_SCHEMA, "verdict": "RED", "reason": exc.reason}
    return {"schema": PLANNING_REQUEST_SCHEMA, "verdict": "GREEN", "reason": "planning_request_verified", "input_digest": request["input_digest"]}


def check_planning_request(
    request: Mapping[str, Any],
    contract: Mapping[str, Any],
    *,
    capability: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Check a typed replan directly without creating planner state.

    Planning requests are store-owned migration records.  The contract engine
    can derive and verify their deterministic plan in memory; no synthetic
    planner/verifier/queue receipts or second state file are needed.
    """
    if verify_planning_request(request).get("verdict") != "GREEN":
        raise DeliveryUnitError("planning_request_not_verified")
    resolved = validate_contract(contract)
    binding = request.get("binding")
    if not isinstance(binding, Mapping):
        raise DeliveryUnitError("planning_binding_missing")
    if binding.get("node_kind") != "planning" or binding.get("producer") != "PlanNodeController":
        raise DeliveryUnitError("planning_binding_type_invalid")
    expected = {**_canonical_identity(resolved), "contract_digest": resolved["contract_digest"]}
    if any(binding.get(field) != value for field, value in expected.items()):
        raise DeliveryUnitError("planning_binding_identity_mismatch")
    if not isinstance(capability, Mapping):
        return {
            "schema": "lh-delivery-planning-result/v1",
            "status": "pending",
            "reason": "planning_capability_missing",
            "request_id": request["request_id"],
            "input_digest": request["input_digest"],
        }
    if capability.get("kind") != "delivery-planning" or capability.get("can_write") is not True:
        return {
            "schema": "lh-delivery-planning-result/v1",
            "status": "pending",
            "reason": "planning_capability_invalid",
            "request_id": request["request_id"],
            "input_digest": request["input_digest"],
        }
    planner_principal = _text("planning.capability.principal", capability.get("principal"))
    if planner_principal == resolved["independent_verifier"]["principal"]:
        raise DeliveryUnitError("planning_verifier_identity_not_independent")
    plan = plan_delivery_unit(resolved)
    if verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
        raise DeliveryUnitError("planning_plan_verifier_not_green")
    return {
        "schema": "lh-delivery-planning-result/v1",
        "status": "completed",
        "reason": "planning_contract_checked",
        "request_id": request["request_id"],
        "input_digest": request["input_digest"],
        "plan_verdict": plan,
    }


__all__ = [
    "ADMISSION_SCHEMA", "COMMIT_RE", "DELIVERY_SCHEMA", "DeliveryUnitError",
    "LEGACY_SIDECAR_SCHEMA", "PLAN_SCHEMA", "PLANNING_REQUEST_SCHEMA", "SCHEMA",
    "SHA256_RE", "bind_legacy_sidecar", "bind_packet", "build_planning_request",
    "digest_json", "load_canonical_contract", "normalize_scope_path", "obligation_ids_for_phase", "plan_delivery_unit",
    "receipt_digest", "run_obligation_commands", "scope_matches", "seal_contract",
    "validate_contract", "verify_changed_paths_scope", "verify_delivery", "verify_dispatch_binding",
    "verify_legacy_sidecar", "verify_packet_binding", "verify_plan_verdict", "verify_planning_request", "check_planning_request",
    "verify_receipt",
]

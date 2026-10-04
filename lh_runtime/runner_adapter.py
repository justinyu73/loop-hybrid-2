#!/usr/bin/env python3
"""Capability-bound runner adapters for coding and verification flows.

This module is deliberately a small registry seam.  Callers provide the
capability contract and the adapter callables for one execution.  No provider,
model, executable, or fallback is selected here from ambient state.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from collections.abc import Callable, Mapping
from typing import Any

CAPABILITY_CONTRACT_SCHEMA = "host-provider-neutral-capability-contract/v1"
BINDING_SCHEMA = "host-capability-binding/v1"
REQUIRED_CAPABILITIES = ("coding", "verifier", "checks")
# Planning is an optional extension for the existing P4 contract.  Keeping
# the historical three capabilities required preserves older runner packets;
# a planner role, when declared or requested, still requires an explicit
# planning capability and adapter.
OPTIONAL_CAPABILITIES = ("planning",)
REQUIRED_ROLES = ("coding", "integration", "verifier", "checks")
OPTIONAL_ROLES = ("planner",)
PLANNING_CAPABILITY = "planning"
PLANNER_ROLE = "planner"
SUPPORTED_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES
PLANNER_REQUIRED_CAPABILITIES = REQUIRED_CAPABILITIES + (PLANNING_CAPABILITY,)
ROLE_CAPABILITY = {
    "coding": "coding",
    "integration": "coding",
    "verifier": "verifier",
    "checks": "checks",
    "planner": "planning",
}


class CapabilityError(ValueError):
    """The explicit capability contract cannot admit an adapter."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class PhaseJobPending(Exception):
    """A durable phase has no consumable result; this is not a role verdict."""

    def __init__(self, job):
        self.job = job
        self.reason = ("phase_job_outcome_unknown" if job["status"] == "outcome_unknown"
                       else job.get("waiting_reason", "phase_job_waiting"))
        super().__init__(self.reason)


def digest_json(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _trusted_role_input(request, *, role, phase, store, authority_digest):
    """Project a fixed successor role view from the already bound packet and Store.

    The full controller request still owns process/fence validation. Only this
    closed, bounded projection is sent to the provider and input-attested;
    historical receipts and diagnostics are referenced rather than replayed.
    """
    from .work_unit_completion import validate_full_validation_binding
    dispatch = store.get_dispatch_consumption(request["dispatch_key"])
    if dispatch is None or any(request.get(k) != dispatch.get(k) for k in
        ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt")):
        raise CapabilityError("trusted_role_dispatch_binding_invalid")
    attempt = store.get_attempt(request["run_id"], request["attempt"])
    if attempt is None or attempt["fence"] != request["fence"]:
        raise CapabilityError("trusted_role_attempt_binding_invalid")
    envelope = dispatch["envelope"]
    path = Path(envelope["packet_path"])
    if (not path.is_absolute() or not path.is_file()
        or any(p.is_symlink() for p in (path, *path.parents)) or path.stat().st_size > 1048576):
        raise CapabilityError("trusted_role_packet_unreadable")
    materialized = json.loads(path.read_text(encoding="utf-8"))
    packet = materialized.get("packet", materialized)
    contract = packet.get("completion_contract")
    if (digest_json({k: v for k, v in materialized.items() if k != "packet_digest"}) != request["packet_digest"]
        or request["packet_digest"] != envelope["packet_digest"]
        or not isinstance(contract, dict) or packet.get("completion_contract_digest") != digest_json(contract)):
        raise CapabilityError("trusted_role_packet_binding_invalid")
    validate_full_validation_binding(contract, packet)
    goal = {key: request[key] for key in ("goal_id", "goal_revision", "node_id")}
    rubric = {"packet_digest": request["packet_digest"], "completion_contract_digest": digest_json(contract),
        "checks": copy.deepcopy(contract["checks"]), "integration_checks": copy.deepcopy(contract["integration_checks"]),
        "required_test_delta": packet.get("required_test_delta", False)}
    scope = {"write_set": copy.deepcopy(packet["write_set"]),
             "forbidden_paths": copy.deepcopy(packet.get("forbidden_paths", []))}
    evidence = {key: request[key] for key in ("candidate_digest", "candidate_commit", "checks_digest") if key in request}
    evidence["packet_ref"] = {"path": str(path), "digest": request["packet_digest"]}
    failure = None
    repairs = request.get("completion_repair", [])
    if repairs:
        if not isinstance(repairs, list) or len(repairs) != 1 or not isinstance(repairs[0], dict):
            raise CapabilityError("trusted_role_repair_input_invalid")
        red = repairs[0]
        phase_name = red.get("phase")
        saved = store.get_completion_phase(request["run_id"], red["attempt"], red["fence"], phase_name)
        lineage = store.get_continuation_repair(request["dispatch_key"])
        if (saved is None or saved["state"] != "settled" or saved["evidence"] != red
            or red.get("verdict") != "RED" or phase_name not in {"checks", "verifier", "integration_checks", "integration_verifier"}
            or red["run_id"] != request["run_id"] or red["attempt"] >= request["attempt"]
            or red.get("receipt_digest") != digest_json({k: v for k, v in red.items() if k != "receipt_digest"})
            or lineage is None or lineage["authority_digest"] != authority_digest
            or lineage["red_receipt_digest"] != red["receipt_digest"]):
            raise CapabilityError("trusted_role_settled_repair_required")
        diagnostics = [{key: row[key] for key in ("id", "exit_code", "expect_exit", "stdout_digest", "stderr_digest")
                        if key in row} for row in red.get("checks", []) if row.get("exit_code") != row.get("expect_exit")]
        if len(diagnostics) > 32:
            raise CapabilityError("trusted_role_repair_diagnostics_limit")
        failure = {"phase": phase_name, "verdict": "RED", "receipt_digest": red["receipt_digest"],
            "reason": "completion_" + phase_name + "_red", "diagnostics": diagnostics,
            "evidence_ref": {"state_root": str(store.root), "phase_key": red["phase_key"]}}
        if phase_name in {"verifier", "integration_verifier"}:
            provider = red.get("provider_execution", {})
            result = provider.get("result")
            result_digest, reason_code = provider.get("result_digest"), red.get("reason_code")
            if (reason_code not in {"check_failed", "scope_failed"} or not isinstance(result_digest, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", result_digest) is None
                or red.get("evidence_ref") != result_digest or not isinstance(result, dict)
                or result.get("reason_code") != reason_code or digest_json(result) != result_digest):
                raise CapabilityError("trusted_role_verifier_feedback_invalid")
            failure.update(reason_code=reason_code, result_digest=result_digest)
    fields = {"goal_id", "goal_revision", "node_id", "run_id", "work_unit_id", "dispatch_key", "attempt", "fence",
        "base_sha", "worktree", "branch", "packet_digest", "envelope_digest", "candidate_digest", "candidate_commit",
        "checks_digest", "read_only_required"}
    projected = {key: copy.deepcopy(request[key]) for key in fields if key in request}
    projected.update(packet_path=str(path), execution_phase=phase, completion_repair=[failure] if failure else [],
        role_context={"role": role, "goal": goal, "objective": packet.get("task", ""), "scope": scope,
            "rubric": rubric, "evidence": evidence, "failure": failure})
    if len(json.dumps(projected, ensure_ascii=False).encode()) > 262144:
        raise CapabilityError("trusted_role_input_limit")
    return projected


def _persist_trusted_provider_rejection(scratch: Path, value: Mapping[str, Any]) -> None:
    """Publish one private, immutable diagnostic; never turn it into a receipt."""
    from .platform_ports import make_file_private, sync_directory, PlatformPortUnavailable
    descriptor, temporary = None, None
    try:
        if not scratch.is_absolute() or scratch.resolve(strict=True) != scratch or not scratch.is_dir():
            raise CapabilityError("trusted_provider_rejection_path_invalid")
        target = scratch / "provider-rejection.json"
        if target.exists() or target.is_symlink():
            raise CapabilityError("trusted_provider_rejection_conflict")
        encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(encoded) > 16384:
            raise CapabilityError("trusted_provider_rejection_limit_exceeded")
        descriptor, name = tempfile.mkstemp(prefix=".provider-rejection-", suffix=".tmp", dir=scratch)
        temporary = Path(name)
        make_file_private(descriptor, temporary)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        # Unlike replace, link cannot overwrite a conflicting file or symlink.
        os.link(temporary, target, follow_symlinks=False)
        temporary.unlink()
        temporary = None
        sync_directory(scratch)
    except (OSError, PlatformPortUnavailable):
        raise CapabilityError("trusted_provider_rejection_write_failed") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def normalize_trusted_command_result(stdout, *, phase, request, worktree, allowed_read_roots):
    """Close result-bearing child JSON before it can enter durable evidence.

    This validates the child result only. Controller-owned launch evidence is
    added separately, and plain checks or stdinless commands do not use it.
    """
    def invalid():
        raise CapabilityError("trusted_command_result_invalid")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                invalid()
            value[key] = item
        return value

    def valid_text(value):
        return isinstance(value, str) and bool(value.strip()) and not any(c in value for c in "\r\n\x00")

    def valid_digest(value):
        return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None

    def canonical_path(value, *, directory):
        if not valid_text(value):
            invalid()
        path = Path(value)
        if (not path.is_absolute() or str(path.resolve(strict=True)) != value
                or not (path.is_dir() if directory else path.is_file())):
            invalid()
        return path

    try:
        if not isinstance(stdout, str) or not isinstance(request, Mapping):
            invalid()
        value = json.loads(stdout, object_pairs_hook=unique_object,
                           parse_constant=lambda _: invalid())
        if not isinstance(value, dict):
            invalid()
        assigned = canonical_path(worktree, directory=True)
        if phase == "integration":
            if set(value) != {"worktree", "source_candidate_digest", "integration_candidate_digest"}:
                invalid()
            expected = request.get("candidate_digest")
            if "expected_integration_candidate_digest" in request:
                if (not valid_digest(request.get("integration_inputs_digest"))
                    or not valid_digest(request["expected_integration_candidate_digest"])):
                    invalid()
                expected = request["expected_integration_candidate_digest"]
            if (canonical_path(value["worktree"], directory=True) != assigned
                    or canonical_path(request.get("integration_worktree"), directory=True) != assigned
                    or not valid_digest(request.get("candidate_digest"))
                    or value["source_candidate_digest"] != request["candidate_digest"]
                    or value["integration_candidate_digest"] != expected):
                invalid()
        elif phase == "admission":
            if set(value) != {"verdict", "binding", "satisfied_gates", "evidence_refs"}:
                invalid()
            binding_fields = {"goal_id", "goal_revision", "node_id", "manifest_digest"}
            binding = value["binding"]
            if (not isinstance(binding, dict) or set(binding) != binding_fields
                    or any(binding[key] != request.get(key) for key in binding_fields)
                    or not all(valid_text(binding[key]) for key in ("goal_id", "node_id"))
                    or type(binding["goal_revision"]) is not int or binding["goal_revision"] < 1
                    or type(request.get("goal_revision")) is not int
                    or not valid_digest(binding["manifest_digest"])
                    or value["verdict"] not in {"GREEN", "RED"}):
                invalid()
            expected, gates = request.get("expected_gates"), value["satisfied_gates"]
            if (not isinstance(expected, list) or not isinstance(gates, list)
                    or not all(valid_text(item) for item in [*expected, *gates])
                    or len(set(expected)) != len(expected) or len(set(gates)) != len(gates)
                    or not set(gates).issubset(expected)
                    or (value["verdict"] == "GREEN" and set(gates) != set(expected))):
                invalid()
            if not isinstance(allowed_read_roots, list):
                invalid()
            roots = [assigned, *(canonical_path(root, directory=True) for root in allowed_read_roots)]
            refs = value["evidence_refs"]
            if not isinstance(refs, list) or not refs:
                invalid()
            for ref in refs:
                if not isinstance(ref, dict) or set(ref) != {"path", "digest"} or not valid_digest(ref["digest"]):
                    invalid()
                path = canonical_path(ref["path"], directory=False)
                if not any(path.is_relative_to(root) for root in roots):
                    invalid()
                if "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest() != ref["digest"]:
                    invalid()
        else:
            invalid()
        return value
    except (OSError, RuntimeError, TypeError, ValueError, RecursionError):
        raise CapabilityError("trusted_command_result_invalid") from None


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CapabilityError(f"{name}_missing")
    return value.strip()


def _normalise_identity(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise CapabilityError(f"{name}_missing")
    identity = copy.deepcopy(dict(value))
    if any(not isinstance(key, str) or not key.strip() for key in identity):
        raise CapabilityError(f"{name}_invalid")
    if any(isinstance(item, (dict, list)) for item in identity.values()):
        raise CapabilityError(f"{name}_nested_invalid")
    if any(not isinstance(item, (str, int, float, bool)) for item in identity.values()):
        raise CapabilityError(f"{name}_value_invalid")
    return identity


def validate_contract(raw: Any, *, require_planning: bool = False) -> dict[str, Any]:
    """Validate and normalize an explicit provider-neutral capability contract."""
    if not isinstance(raw, Mapping) or raw.get("schema") != CAPABILITY_CONTRACT_SCHEMA:
        raise CapabilityError("capability_contract_schema_invalid")
    capabilities = raw.get("capabilities")
    roles = raw.get("roles")
    if not isinstance(capabilities, Mapping):
        raise CapabilityError("capabilities_missing")
    if not isinstance(roles, Mapping):
        raise CapabilityError("roles_missing")
    missing = [name for name in REQUIRED_CAPABILITIES if name not in capabilities]
    if missing:
        raise CapabilityError("missing_capability:" + ",".join(missing))
    if require_planning and PLANNING_CAPABILITY not in capabilities:
        raise CapabilityError("missing_capability:" + PLANNING_CAPABILITY)
    missing_roles = [name for name in REQUIRED_ROLES if name not in roles]
    if missing_roles:
        raise CapabilityError("missing_role:" + ",".join(missing_roles))
    if require_planning and PLANNER_ROLE not in roles:
        raise CapabilityError("missing_role:" + PLANNER_ROLE)
    fallback = raw.get("fallback", "none")
    if fallback != "none":
        raise CapabilityError("implicit_fallback_forbidden")
    for field in ("default_provider", "default_model"):
        if field in raw and raw[field] not in (None, ""):
            raise CapabilityError("default_identity_forbidden:" + field)

    normalized_capabilities: dict[str, dict[str, Any]] = {}
    capabilities_to_validate = (*REQUIRED_CAPABILITIES, *OPTIONAL_CAPABILITIES)
    for capability in capabilities_to_validate:
        if capability not in capabilities:
            continue
        value = capabilities.get(capability)
        if not isinstance(value, Mapping):
            raise CapabilityError(f"{capability}_binding_invalid")
        adapter_id = _text(f"{capability}.adapter_id", value.get("adapter_id"))
        identity = _normalise_identity(f"{capability}.identity", value.get("identity"))
        permissions = _text(f"{capability}.permissions", value.get("permissions"))
        expected_permissions = {
            "coding": "workspace_write",
            # ``read_only`` is retained for the already-merged planner packet
            # facade.  The new deterministic controller uses
            # ``task_owned_write`` for its task-owned state, while a legacy
            # planner adapter may only propose a plan in memory.
            "planning": {"task_owned_write", "read_only"},
        }.get(capability, "read_only")
        if isinstance(expected_permissions, set):
            if permissions not in expected_permissions:
                raise CapabilityError(f"{capability}.permissions_invalid")
        elif permissions != expected_permissions:
            raise CapabilityError(f"{capability}.permissions_invalid")
        normalized_capabilities[capability] = {
            "adapter_id": adapter_id,
            "identity": identity,
            "permissions": permissions,
        }
    normalized_roles: dict[str, str] = {}
    roles_to_validate = (*REQUIRED_ROLES, *OPTIONAL_ROLES)
    for role in roles_to_validate:
        if role not in roles:
            continue
        capability = _text(f"role.{role}", roles.get(role))
        if capability not in (*REQUIRED_CAPABILITIES, *OPTIONAL_CAPABILITIES):
            raise CapabilityError(f"role.{role}_capability_invalid")
        if capability != ROLE_CAPABILITY[role]:
            raise CapabilityError(f"role.{role}_capability_mismatch")
        if capability not in normalized_capabilities:
            raise CapabilityError(f"missing_capability:{capability}")
        normalized_roles[role] = capability
    return {
        "schema": CAPABILITY_CONTRACT_SCHEMA,
        "revision": _text("revision", raw.get("revision")),
        "capabilities": normalized_capabilities,
        "roles": normalized_roles,
        "fallback": "none",
    }


def validate_planning_contract(raw: Any) -> dict[str, Any]:
    """Require an explicit planning capability and planner role.

    This named entrypoint keeps the merged P3B packet API available while
    ``validate_contract`` remains the provider-neutral base contract.
    """

    return validate_contract(raw, require_planning=True)


Adapter = Callable[[dict[str, Any]], Mapping[str, Any]]


class CapabilityBoundRunner:
    """Resolve and invoke only adapters explicitly supplied by the caller."""

    def __init__(
        self,
        contract: Mapping[str, Any],
        adapters: Mapping[str, Adapter],
        *,
        require_planning: bool = False,
    ):
        self.contract = validate_contract(contract, require_planning=require_planning)
        if not isinstance(adapters, Mapping):
            raise CapabilityError("adapter_registry_missing")
        self.adapters = dict(adapters)
        self.contract_digest = digest_json(self.contract)

    def capability_for_role(self, role: str) -> str:
        if role not in ROLE_CAPABILITY:
            raise CapabilityError("unknown_role:" + str(role))
        return ROLE_CAPABILITY[role]

    def binding(
        self,
        role: str,
        *,
        work_unit_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        return {**self._binding(role, attempt_id=attempt_id),
                "work_unit_id": _text("work_unit_id", work_unit_id)}

    def native_binding(self, role: str, *, unit_id: str, attempt_id: str) -> dict[str, Any]:
        return {**self._binding(role, attempt_id=attempt_id),
                "identity_profile": "native-run-v1", "authority_store": "run",
                "unit_id": _text("unit_id", unit_id)}

    def _binding(self, role: str, *, attempt_id: str) -> dict[str, Any]:
        role = _text("role", role)
        attempt_id = _text("attempt_id", attempt_id)
        if role not in self.contract["roles"]:
            raise CapabilityError("missing_role:" + role)
        capability = self.capability_for_role(role)
        definition = self.contract["capabilities"].get(capability)
        if not isinstance(definition, Mapping):
            raise CapabilityError("missing_capability:" + capability)
        adapter_id = definition["adapter_id"]
        if adapter_id not in self.adapters or not callable(self.adapters[adapter_id]):
            raise CapabilityError("adapter_unavailable:" + adapter_id)
        identity = copy.deepcopy(definition["identity"])
        return {
            "schema": BINDING_SCHEMA,
            "role": role,
            "capability": capability,
            "adapter_id": adapter_id,
            "identity": identity,
            "identity_digest": digest_json(identity),
            "permissions": definition["permissions"],
            "contract_digest": self.contract_digest,
            "attempt_id": attempt_id,
        }

    def execute(
        self,
        role: str,
        request: Mapping[str, Any],
        *,
        work_unit_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            raise CapabilityError("adapter_request_invalid")
        binding = self.binding(role, work_unit_id=work_unit_id, attempt_id=attempt_id)
        adapter = self.adapters[binding["adapter_id"]]
        payload = copy.deepcopy(dict(request))
        payload["capability_binding"] = copy.deepcopy(binding)
        result = adapter(payload)
        if not isinstance(result, Mapping):
            raise CapabilityError("adapter_result_invalid")
        return {
            "schema": "host-capability-execution-result/v1",
            "binding": binding,
            "result": copy.deepcopy(dict(result)),
        }


CapabilityRunner = CapabilityBoundRunner


def resolve_task_area_execution_binding(manifest: Mapping[str, Any]):
    from .execution_fence import ExecutionFenceUnavailable
    try:
        return _resolve_task_area_execution_binding(manifest)
    except ExecutionFenceUnavailable as exc:
        raise CapabilityError(str(exc)) from exc


def _resolve_task_area_execution_binding(manifest: Mapping[str, Any]):
    """Resolve the externally approved references once, before any command."""
    from .provider_registry import validate_provider_registry
    from .host_ports import resolve_host_ports
    from .goal_loop_run import build_execution_host_binding
    if __package__:
        from . import execution_fence as fences
    else:
        import execution_fence as fences

    raw = manifest.get("execution_binding")
    fields = {"schema", "capability_contract_ref", "provider_registry_ref", "provider_selection",
              "host_contract_ref", "fence", "bootstrap_authority"}
    trusted = isinstance(raw, Mapping) and raw.get("schema") == "host-task-area-execution-binding/v2"
    if trusted:
        fields |= {"project_id", "execution_policy_ref"}
    if (not isinstance(raw, Mapping) or set(raw) != fields
            or raw.get("schema") not in {"host-task-area-execution-binding/v1", "host-task-area-execution-binding/v2"}):
        raise CapabilityError("execution_binding_fields_invalid")

    def read_ref(reference, *, optional=False):
        if reference is None and optional:
            return None
        if not isinstance(reference, Mapping) or set(reference) != {"path", "digest"}:
            raise CapabilityError("execution_binding_reference_invalid")
        if (not isinstance(reference["path"], str) or not reference["path"]
                or not isinstance(reference["digest"], str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", reference["digest"])):
            raise CapabilityError("execution_binding_reference_type_invalid")
        path = Path(reference["path"])
        if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_file():
            raise CapabilityError("execution_binding_reference_path_invalid")
        data = path.read_bytes()
        if "sha256:" + hashlib.sha256(data).hexdigest() != reference["digest"]:
            raise CapabilityError("execution_binding_reference_digest_mismatch")
        value = json.loads(data)
        if not isinstance(value, dict):
            raise CapabilityError("execution_binding_reference_object_invalid")
        return value

    policy, operator = None, None
    if trusted:
        from .execution_fence_trusted import validate_trusted_project_policy, validate_trusted_operator_binding
        policy = validate_trusted_project_policy(read_ref(raw["execution_policy_ref"]))
        operator = validate_trusted_operator_binding(read_ref(policy["operator_binding_ref"]))
        if (raw["project_id"] != policy["project_id"] or operator["project_id"] != policy["project_id"]
                or policy["goal_id"] != manifest.get("goal_id")
                or policy["goal_revision"] != manifest.get("goal_revision")):
            raise CapabilityError("trusted_project_goal_mismatch")
    capabilities = read_ref(raw["capability_contract_ref"])
    normalized = validate_contract(capabilities)
    registry = validate_provider_registry(read_ref(raw["provider_registry_ref"]))
    selection = raw["provider_selection"]
    if not isinstance(selection, Mapping) or set(selection) != {"coding", "verifier"}:
        raise CapabilityError("execution_binding_provider_selection_invalid")
    providers = {}
    for role in ("coding", "verifier"):
        provider_id = _text("execution_binding.provider_selection." + role, selection[role])
        descriptor = registry["providers"].get(provider_id)
        if descriptor is None:
            raise CapabilityError("provider_unregistered:" + str(provider_id))
        capability = normalized["capabilities"][role]
        if (descriptor["identity"] != capability["identity"]
                or descriptor["adapter_id"] != capability["adapter_id"]):
            raise CapabilityError("execution_binding_provider_identity_mismatch")
        if descriptor["adapter_id"] != ("codex-exec-jsonl-v1" if trusted else "bounded-command-v1"):
            raise CapabilityError("execution_binding_adapter_unsupported:" + descriptor["adapter_id"])
        if trusted:
            selected = operator["providers"][role]
            if (selected["provider_id"] != provider_id or selected["adapter_id"] != descriptor["adapter_id"]
                    or selected["model"] != descriptor["identity"].get("model")
                    or descriptor["command"] != [selected["executable"], "exec"]):
                raise CapabilityError("trusted_operator_provider_mismatch")
        providers[role] = {"provider_id": provider_id, **descriptor}
    if (providers["coding"]["identity"] == providers["verifier"]["identity"]
            or providers["coding"]["identity"].get("principal") == providers["verifier"]["identity"].get("principal")):
        raise CapabilityError("execution_binding_verifier_independence_required")
    host = resolve_host_ports(read_ref(raw["host_contract_ref"]))
    execution_host = host["interface"]
    if not isinstance(raw["bootstrap_authority"], Mapping):
        raise CapabilityError("execution_binding_bootstrap_authority_invalid")
    bootstrap = build_execution_host_binding(execution_host, dict(raw["bootstrap_authority"]))
    fence = raw["fence"]
    fence_fields = {"backend_id", "egress_policy_ref"}
    if isinstance(fence, Mapping) and "null_device_check" in fence:
        fence_fields.add("null_device_check")
        if (trusted or fence["null_device_check"] != "git-diff-cached-check-v1"
            or fence.get("backend_id") != "linux-bubblewrap-seccomp"
            or fence.get("egress_policy_ref") is not None):
            raise CapabilityError("null_device_check_profile_invalid")
    if not isinstance(fence, Mapping) or set(fence) != fence_fields:
        raise CapabilityError("execution_binding_fence_fields_invalid")
    _text("execution_binding.fence.backend_id", fence["backend_id"])
    if trusted and (fence["backend_id"] != "trusted-project-local" or fence["egress_policy_ref"] is not None):
        raise CapabilityError("trusted_fence_binding_invalid")
    read_ref(fence["egress_policy_ref"], optional=True)
    for task in manifest.get("tasks", []):
        if task.get("status") != "approved":
            continue
        envelope = task.get("envelope") or {}
        packet = json.loads(Path(envelope["packet_path"]).read_text(encoding="utf-8"))
        packet = packet.get("packet", packet)
        if (packet.get("materialization_inputs") or {}).get("operator_capability_contract_digest") != digest_json(capabilities):
            raise CapabilityError("execution_binding_packet_capability_digest_mismatch")
        if task.get("verifier_argv") != providers["verifier"]["command"]:
            raise CapabilityError("execution_binding_verifier_command_mismatch")
    port = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": fence["backend_id"]},
                                            policy=policy, operator_binding=operator)
    if isinstance(port, fences.DisabledExecutionFencePort):
        raise CapabilityError(str(fences.ExecutionFenceUnavailable(port.reason)))
    if not port.supports_started_notification:
        raise CapabilityError("execution_fence_unavailable: started_notification_unsupported")
    return ResolvedExecutionBinding(raw, capabilities, normalized, providers, host, bootstrap, port,
                                    policy=policy, operator=operator)



def resolve_native_run_execution_binding(seal, dispatch, *, campaign, source_repo, base_revision):
    """Resolve only the opt-in already sealed into this scheduler dispatch."""
    from . import dispatch_envelope as dispatches
    from .provider_registry import validate_provider_registry
    from .work_unit_store import recovery_budget_limits
    if not isinstance(seal, Mapping) or set(seal) != {"contract_ref", "contract_digest", "binding"}:
        raise CapabilityError("native_runtime_seal_invalid")
    path = Path(seal["contract_ref"]).resolve(strict=True)
    raw_contract = path.read_bytes()
    contract = json.loads(raw_contract)
    if (not isinstance(dispatch, Mapping) or set(dispatch) != dispatches.FIELDS
        or dispatch.get("schema") != dispatches.SCHEMA
        or dispatch.get("owner_id") != os.environ.get("LH_SCHEDULER_OWNER_ID")
        or not dispatch.get("owner_id") or dispatch.get("desired_state") != "enabled"
        or dispatch.get("envelope_digest") != digest_json({
            key: value for key, value in dispatch.items() if key != "envelope_digest"})
        or dispatch.get("dispatch_id") != "dispatch-" + digest_json({
            key: value for key, value in dispatch.items()
            if key not in {"dispatch_id", "envelope_digest"}}).removeprefix("sha256:")[:32]
        or Path(dispatch["contract_ref"]).resolve() != path
        or dispatch.get("contract_digest") != seal["contract_digest"]
        or seal["contract_digest"] != "sha256:" + hashlib.sha256(raw_contract).hexdigest()
        or contract.get("schema") != "lh-project-runtime-contract/v1"
        or contract.get("project_id") != dispatch.get("project_id")
        or contract.get("campaign") != campaign
        or campaign.get("campaign_id") != dispatch.get("campaign_id")
        or contract.get("base_revision") != base_revision
        or base_revision != dispatch.get("base_revision")
        or (path.parent / contract["source_repo"]).resolve() != Path(source_repo).resolve()
        or contract.get("planner_recovery") != seal["binding"]):
        raise CapabilityError("native_runtime_dispatch_mismatch")
    config = copy.deepcopy(seal["binding"])
    if (not isinstance(config, dict) or set(config) != {
        "schema", "identity_profile", "planner_argv", "plan_verifier_argv", "planner_provider", "budget"}
        or config["schema"] != "lh-planner-recovery-binding/v1"
        or config["identity_profile"] != "native-run-v1"
        or contract.get("execution_binding", {}).get("schema") != "host-task-area-execution-binding/v1"):
        raise CapabilityError("native_recovery_binding_invalid")
    config["budget"] = recovery_budget_limits(config["budget"])
    if config["budget"]["planner_calls"] != 1 or config["budget"]["plan_verifier_calls"] != 1:
        raise CapabilityError("native_recovery_role_limit_invalid")
    binding = resolve_task_area_execution_binding({"execution_binding": contract["execution_binding"]})
    validate_planning_contract(binding.capabilities)
    reference = contract["execution_binding"]["provider_registry_ref"]
    raw_registry = Path(reference["path"]).read_bytes()
    if "sha256:" + hashlib.sha256(raw_registry).hexdigest() != reference["digest"]:
        raise CapabilityError("native_recovery_registry_changed")
    registry = validate_provider_registry(json.loads(raw_registry))
    planner = registry["providers"].get(config["planner_provider"])
    capability = binding.runner.contract["capabilities"]["planning"]
    if (planner is None or planner["adapter_id"] != capability["adapter_id"]
        or planner["identity"] != capability["identity"]
        or planner["command"] != config["planner_argv"]
        or binding.providers["verifier"]["command"] != config["plan_verifier_argv"]):
        raise CapabilityError("native_recovery_provider_binding_mismatch")
    # Independence is a consumer rejection, before any role claim/launch.
    binding.native_runtime = {
        "identity_profile": "native-run-v1", "project_id": contract["project_id"],
        "campaign_id": campaign["campaign_id"], "source_repo": str(Path(source_repo).resolve()),
        "base_revision": base_revision, "contract_ref": str(path),
        "contract_digest": seal["contract_digest"], "dispatch": dispatches.receipt_binding(dispatch),
        "planner_recovery": config,
    }
    return binding


class ResolvedExecutionBinding:
    """One verified capability/host/fence value shared by all existing phases."""

    # Defaults for a value built without __init__: no carrier and no native opt-in, the legacy launch path.
    phase_carrier = None
    native_runtime = None
    native_store = None

    def __init__(self, raw, capabilities, normalized, providers, host, bootstrap, port, *, policy=None, operator=None):
        self.raw = copy.deepcopy(dict(raw))
        self.capabilities = copy.deepcopy(capabilities)
        self.providers, self.host, self.bootstrap, self.port = providers, host, bootstrap, port
        self.canonical_capability_digest = digest_json(capabilities)
        self.digest = digest_json(raw)
        self.policy, self.operator, self.store = policy, operator, None
        self.native_runtime, self.native_store = None, None
        self.continuation_authority_digest = None
        self.phase_carrier = None
        self.runner = CapabilityBoundRunner(normalized,
            {entry["adapter_id"]: lambda request: request for entry in normalized["capabilities"].values()})

    def attach_store(self, store):
        if self.store is not None and self.store.root != store.root:
            raise CapabilityError("trusted_store_binding_conflict")
        self.store = store
        return self

    def attach_native_store(self, store):
        if not self.native_runtime or self.native_runtime["identity_profile"] != "native-run-v1":
            raise CapabilityError("native_runtime_optin_missing")
        if self.store is not None or (self.native_store is not None and self.native_store is not store):
            raise CapabilityError("native_store_binding_conflict")
        self.native_store = store
        return self

    def _native_command_identity(self, request, phase):
        if (not self.native_runtime or self.native_runtime["identity_profile"] != "native-run-v1"
            or self.native_store is None or "work_unit_id" in request
            or request.get("authority_store") != "run"):
            raise CapabilityError("native_execution_context_optin_missing")
        run = self.native_store.get_run(_text("run_id", request.get("run_id")))
        attempt = self.native_store.latest_attempt(run["run_id"])
        if (attempt is None or run["attempts"] != attempt["ordinal"] or run["fence"] != attempt["fence"]
            or run["base_revision"] != self.native_runtime["base_revision"]
            or Path(run["source_repo"]).resolve() != Path(self.native_runtime["source_repo"])
            or run["goal"].get("campaign_id") != self.native_runtime["campaign_id"]
            or self.native_store.delivery_binding(run["run_id"]).get("verdict") != "GREEN"):
            raise CapabilityError("native_execution_context_authority_invalid")
        identity = self.native_store._delivery_identity(run, ordinal=attempt["ordinal"], fence=attempt["fence"])
        if any(request.get(key) != identity.get(key) or identity.get(key) in (None, "")
               for key in ("goal_id", "goal_revision", "node_id", "unit_id", "run_id", "attempt", "fence", "base_sha")):
            raise CapabilityError("native_execution_context_identity_mismatch")
        if request.get("execution_phase", phase) != phase:
            raise CapabilityError("native_execution_context_phase_mismatch")
        if phase in {"planner", "plan_verifier"}:
            if request.get("identity_profile") != "native-run-v1" or run["state"] not in {"stopped", "human_required"}:
                raise CapabilityError("native_recovery_run_not_terminal")
        elif phase not in {"delivery_checks", "delivery_verifier"} or run["state"] != "running":
            raise CapabilityError("native_execution_phase_unsupported")
        return identity

    def submit_phase(self, job_id: str) -> dict:
        """Start only the fixed control carrier; all role launches stay fenced."""
        from .lifecycle import NativeProcessIdentityPort, observe_process_identity
        if (not sys.platform.startswith("linux") or self.host["interface"] != "headless_cli"
            or self.policy is not None):
            raise CapabilityError("phase_job_independent_carrier_unsupported")
        if self.store is None:
            raise CapabilityError("phase_job_store_binding_missing")
        job = self.store.read_phase_job(job_id)
        if job["status"] != "reserved":
            return job
        current = NativeProcessIdentityPort().current()
        if (current is None or current.as_dict() != job["creator_identity"]
            or job["input"]["execution_binding_digest"] != self.digest):
            raise CapabilityError("phase_job_startup_owner_unknown")
        remaining = job["deadline_at"] - time.time()
        if remaining <= 0:
            raise CapabilityError("phase_job_deadline_expired")
        carrier = Path(__file__).resolve().parents[1] / "tools" / "session_task_area.py"
        folder = self.store.root / "phase-jobs"
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        log_path = folder / (hashlib.sha256(job_id.encode()).hexdigest() + ".carrier.log")
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "ab", buffering=0) as log:
            # This process owns no role capability of its own. It can only claim
            # this approved job and reconstruct the original binding/phase.
            process = subprocess.Popen([sys.executable, "-B", str(carrier),
                "_phase-job-carrier", "--state-root", str(self.store.root), "--job-id", job_id],
                cwd=str(carrier.parent.parent), stdin=subprocess.DEVNULL,
                stdout=log, stderr=log, close_fds=True, start_new_session=True)
        ack_deadline = time.monotonic() + min(5.0, remaining)
        while True:
            job = self.store.read_phase_job(job_id, reconcile=False)
            identity = job.get("carrier_identity")
            if identity is not None:
                if identity.get("pid") != process.pid:
                    raise CapabilityError("phase_job_startup_identity_mismatch")
                observation = observe_process_identity(identity)
                if (observation.status == "alive"
                    or job["status"] in {"result_ready", "settled"}):
                    return job
                raise CapabilityError("phase_job_startup_owner_unknown")
            if process.poll() is not None or time.monotonic() >= ack_deadline:
                # The reservation remains durable. No timeout path resubmits it.
                raise CapabilityError("phase_job_startup_ack_unknown")
            time.sleep(0.01)

    def run_phase_job(self, item: dict, *, timeout_seconds: float,
                      allow_create: bool = True) -> tuple[dict, dict]:
        if self.store is None:
            raise CapabilityError("phase_job_store_binding_missing")
        job_id = "phase-job:" + digest_json([item[key] for key in ("run_id", "attempt", "fence", "phase")])
        prior = next((job for job in self.store.list_phase_jobs(item["goal_id"])
                      if job["job_id"] == job_id), None)
        job = self.store.reserve_phase_job({**item, "deadline_at": time.time() + timeout_seconds},
                                           allow_create=allow_create)
        # Use the Store's original input/deadline on reentry, never the proposal.
        waiting = job["status"] == "waiting"
        if waiting:
            job = self.store.acquire_phase_job_slots(job["job_id"])
            if job["status"] == "waiting":
                raise PhaseJobPending(job)
        if (prior is None or waiting) and job["status"] == "reserved":
            from .lifecycle import NativeProcessIdentityPort
            current = NativeProcessIdentityPort().current()
            if current is None or current.as_dict() != job["creator_identity"]:
                # Another caller acquired the durable reservation; it alone
                # may submit. This caller only reads the same original job.
                raise PhaseJobPending(job)
            self.submit_phase(job["job_id"])
        job = self.store.read_phase_job(job["job_id"])
        if job["status"] not in {"result_ready", "settled"}:
            raise PhaseJobPending(job)
        return self.store.phase_job_result(job["job_id"])["result"], job

    def attach_phase_carrier(self, job: dict):
        if self.store is None or job["input"]["execution_binding_digest"] != self.digest:
            raise CapabilityError("phase_job_execution_binding_changed")
        self.phase_carrier = (job["job_id"], job["carrier_identity"])
        return self

    def _phase_launch_guard(self, request, phase):
        if self.phase_carrier is None:
            return None
        job = self.store.validate_phase_job_launch(*self.phase_carrier)
        item = job["input"]
        if (phase != item["phase"] or any(request.get(key) != item[key]
            for key in ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence", "packet_digest"))):
            raise CapabilityError("phase_job_launch_request_changed")
        return job["deadline_at"] - time.time()

    def attach_continuation(self, authority_digest):
        """An approved task manifest opts into one existing Store authority."""
        if self.store is None or self.policy is None:
            raise CapabilityError("continuation_store_binding_missing")
        authority = self.store.get_continuation_authority(authority_digest)
        if (authority is None or authority.get("execution_binding_digest") != self.digest
            or authority.get("policy_digest") != digest_json(self.policy)
            or authority.get("goal_id") != self.policy["goal_id"]
            or authority.get("goal_revision") != self.policy["goal_revision"]):
            raise CapabilityError("continuation_execution_binding_mismatch")
        if self.continuation_authority_digest not in (None, authority_digest):
            raise CapabilityError("continuation_binding_conflict")
        self.continuation_authority_digest = authority_digest
        return self

    def _trusted_command(self, request, *, phase, argv, worktree, timeout_seconds,
                         input_request, writable, on_started, env):
        if phase in {"planner", "plan_verifier"}:
            raise CapabilityError("trusted_planning_unsupported")
        from . import execution_fence as fences, provider_input_binding as inputs
        from .cli_agent_executor import (compose_trusted_codex_argv, trusted_codex_output_schema,
            normalize_trusted_codex_jsonl, unknown_trusted_usage, resolve_cli)
        from .platform_ports import ManagedProcessTimeout, ManagedProcessUnknown
        if self.store is None:
            raise CapabilityError("trusted_store_binding_missing")
        if self.host["interface"] != "headless_cli":
            raise CapabilityError("execution_host_adapter_unsupported")
        if (request.get("goal_id") != self.policy["goal_id"]
                or request.get("goal_revision") != self.policy["goal_revision"]):
            raise CapabilityError("trusted_goal_binding_mismatch")
        role = "coding" if phase == "coding" else ("verifier" if "verifier" in phase else None)
        if role is not None and self.continuation_authority_digest is not None:
            if input_request is None:
                raise CapabilityError("trusted_role_input_missing")
            input_request = _trusted_role_input(input_request, role=role, phase=phase, store=self.store,
                authority_digest=self.continuation_authority_digest)
        capability = self.runner.binding(role or ("integration" if phase == "integration" else "checks"),
            work_unit_id=_text("work_unit_id", request.get("work_unit_id")), attempt_id=str(request.get("attempt")))
        phase_key = digest_json({"run_id": request.get("run_id"), "attempt": request.get("attempt"),
            "fence": request.get("fence"), "phase": phase, "command_id": request.get("command_id")})
        scratch = self.store.root / "trusted-launches" / phase_key.removeprefix("sha256:")
        scratch.mkdir(parents=True, exist_ok=True, mode=0o700)
        schema_digest = None
        if role is not None:
            if argv != self.providers[role]["command"]:
                raise CapabilityError("trusted_provider_command_mismatch")
            schema = trusted_codex_output_schema(role)
            schema_path = scratch / "output-schema.json"
            schema_bytes = inputs.canonical_json(schema).encode()
            if schema_path.exists() and schema_path.read_bytes() != schema_bytes:
                raise CapabilityError("trusted_output_schema_conflict")
            if not schema_path.exists():
                with schema_path.open("xb") as stream:
                    stream.write(schema_bytes)
                schema_path.chmod(0o600)
            schema_digest = digest_json(schema)
            argv = compose_trusted_codex_argv(self.operator["providers"][role], role=role,
                                             output_schema_path=str(schema_path))
        else:
            argv = [resolve_cli(argv[0]), *argv[1:]]
        prompt = inputs.canonical_json(dict(input_request)) if input_request is not None else ""
        environment = self.port.environment(role, scratch, env or {})
        reservation = self.store.reserve_trusted_launch(goal_id=request["goal_id"],
            goal_revision=request["goal_revision"], policy=self.policy, phase_key=phase_key,
            run_id=request["run_id"], attempt=request["attempt"], fence=request["fence"],
            phase=phase, command_digest=digest_json(argv), is_provider_cli=role is not None,
            execution_binding_digest=self.digest,
            continuation_authority_digest=self.continuation_authority_digest)
        if not reservation["claimed"]:
            raise CapabilityError("trusted_launch_already_reserved")
        context = {"schema": "lh-trusted-project-context/v1", "project_id": self.policy["project_id"],
            "goal_id": request["goal_id"], "goal_revision": request["goal_revision"],
            "execution_binding_digest": self.digest, "policy_digest": digest_json(self.policy),
            "operator_binding_digest": digest_json(self.operator), "phase": phase, "provider_role": role,
            "budget_reservation_key": reservation["reservation_key"], "deadline_at": reservation["deadline_at"],
            "command_digest": digest_json(argv), "input_digest": "sha256:" + hashlib.sha256(prompt.encode()).hexdigest(),
            "environment_digest": digest_json(environment), "output_schema_digest": schema_digest}
        binding = fences.build_attempt_binding(goal={"goal_id": request["goal_id"], "goal_revision": request["goal_revision"]},
            run_id=request["run_id"], attempt=request["attempt"], attempt_fence=request["fence"],
            base_revision=request["base_sha"], clone_root=worktree, verifier_argv=argv,
            adapter_id=capability["adapter_id"], adapter_version="v1", timeout_seconds=timeout_seconds,
            allowed_read_roots=[], allowed_write_roots=[worktree] if writable else [],
            allowed_local_effects=["workspace_write", "scratch_write"] if writable else ["scratch_write"],
            execution_context_digest=digest_json(context))
        binding.update(schema="lh-trusted-project-binding/v1", trusted_context=context,
                       expires_at=min(binding["expires_at"], reservation["deadline_at"]))
        descriptor = self.port.prepare(binding)
        projection, _ = inputs.project_context({})
        input_binding = inputs.build_input_binding(binding_context={
            "goal_revision": binding["goal_revision"], "run_id": binding["run_id"], "attempt": request["attempt"],
            "adapter_id": binding["adapter_id"], "adapter_version": binding["adapter_version"],
            "capability_digest": capability["contract_digest"],
            "authority_digest": self.bootstrap["bootstrap_authority"]["authority_digest"]},
            projection_record=projection, segments=inputs.build_segments(prompt, argv, environment),
            launch_descriptor_digest=descriptor["launch_descriptor_digest"], nonce=binding["controller_nonce"],
            issued_at=time.time(), ttl_seconds=timeout_seconds)
        inputs.attest_before_launch(input_binding, prompt=prompt, command_template=argv,
            environment_projection=environment, launch_descriptor_digest=descriptor["launch_descriptor_digest"],
            now=time.time(), seen_nonces=set())
        try:
            result = self.port.launch(descriptor, argv, input_text=prompt if input_request is not None else None,
                timeout_seconds=timeout_seconds, env_projection=environment, on_started=on_started)
        except (ManagedProcessTimeout, ManagedProcessUnknown) as exc:
            lifecycle = exc.process_lifecycle
            self.store.settle_trusted_launch(reservation["reservation_key"], observation={
                "schema": "lh-trusted-launch-observation/v1", "outcome": "unknown",
                "reason_code": "process_timeout" if isinstance(exc, ManagedProcessTimeout) else "process_readback_unknown",
                "process_terminated": lifecycle["termination_confirmed"], "elapsed_seconds": lifecycle["elapsed_seconds"],
                "usage": unknown_trusted_usage() if role is not None else None})
            raise CapabilityError("trusted_process_outcome_unknown") from None
        lifecycle = result.process_lifecycle
        evidence = {"execution_assurance": self.port.receipt_projection(descriptor),
            "provider_input_binding": input_binding, "capability_binding": capability,
            "execution_context_digest": binding["execution_context_digest"], "process_lifecycle": lifecycle}
        normalized, diagnostics = None, {}
        if role is not None:
            expected = {"packet_digest": request["packet_digest"]}
            if role == "verifier":
                expected["candidate_digest"] = request["candidate_digest"]
                expected["checks_digest"] = request["checks_digest"]
            normalized = normalize_trusted_codex_jsonl(result.stdout, stderr=result.stderr,
                returncode=result.returncode, role=role, model=self.operator["providers"][role]["model"],
                expected=expected, diagnostics=diagnostics)
            evidence["provider_execution"] = normalized
        elif phase in {"admission", "integration"} and input_request is not None:
            try:
                command_result = normalize_trusted_command_result(result.stdout, phase=phase,
                    request=input_request, worktree=worktree,
                    allowed_read_roots=descriptor["binding"]["allowed_read_roots"])
            except CapabilityError:
                self.store.settle_trusted_launch(reservation["reservation_key"], observation={
                    "schema": "lh-trusted-launch-observation/v1", "outcome": "unknown",
                    "reason_code": "provider_result_invalid",
                    "process_terminated": lifecycle["termination_confirmed"],
                    "elapsed_seconds": lifecycle["elapsed_seconds"], "usage": None})
                raise
            result.stdout = inputs.canonical_json(command_result)
            result.stderr = ""
        outcome = normalized["outcome"] if normalized else ("completed" if result.returncode == 0 else "known_failure")
        observation = {"schema": "lh-trusted-launch-observation/v1", "outcome": outcome,
            "reason_code": ("provider_protocol_invalid" if outcome == "unknown" else
                "completed" if outcome == "completed" else "provider_failed") if normalized else (
                "completed" if result.returncode == 0 else "process_exit_nonzero"),
            "process_terminated": lifecycle["termination_confirmed"], "elapsed_seconds": lifecycle["elapsed_seconds"],
            "usage": normalized["usage"] if normalized else None}
        self.store.settle_trusted_launch(reservation["reservation_key"], observation=observation)
        if normalized:
            if outcome == "unknown" or normalized["usage"]["state"] != "observed":
                rejection_binding = {key: reservation[key] for key in ("goal_id", "goal_revision", "run_id",
                    "attempt", "fence", "phase_key", "reservation_key", "policy_digest", "command_digest")}
                rejection_binding.update(execution_binding_digest=self.digest,
                    process_identity_digest=digest_json(lifecycle["process_identity"]),
                    process_lifecycle_digest=digest_json(lifecycle))
                rejection = {"schema": "lh-trusted-provider-rejection/v1", "binding": rejection_binding,
                    "normalization": {key: normalized[key] for key in ("schema", "normalization_version",
                        "outcome", "reason_code", "result_digest", "stdout_digest", "stdout_bytes",
                        "stderr_digest", "stderr_bytes", "usage")},
                    "diagnostics": diagnostics, "observation": observation,
                    "observation_digest": digest_json(observation)}
                rejection["rejection_digest"] = digest_json(rejection)
                _persist_trusted_provider_rejection(scratch, rejection)
                raise CapabilityError("trusted_provider_outcome_unknown")
            result.stdout = inputs.canonical_json(normalized["result"]) if normalized["result"] else ""
            result.stderr = ""
            if outcome != "completed" and result.returncode == 0:
                result.returncode = 1
        return result, evidence, descriptor

    def _null_device_check(self, *, phase, argv, capability, writable, input_request):
        profile = self.raw["fence"].get("null_device_check")
        if profile is None:
            return None
        if profile != "git-diff-cached-check-v1" or self.policy is not None:
            raise CapabilityError("null_device_check_profile_invalid")
        if phase != "delivery_checks":
            return None
        if (not self.native_runtime or self.native_runtime.get("identity_profile") != "native-run-v1"
            or capability.get("adapter_id") != "deterministic-command-v1"
            or capability.get("permissions") != "read_only" or writable or input_request is not None):
            raise CapabilityError("null_device_check_scope_invalid")
        try:
            git_path = Path(shutil.which("git", path=os.defpath) or "").resolve(strict=True)
            if list(argv) != [str(git_path), "diff", "--cached", "--check"]:
                raise CapabilityError("null_device_check_command_invalid")
            digest = "sha256:" + hashlib.sha256(git_path.read_bytes()).hexdigest()
        except (OSError, RuntimeError) as exc:
            raise CapabilityError("null_device_check_executable_unavailable") from exc
        return {"profile": profile, "phase": phase, "argv": list(argv), "executable_sha256": digest}

    def command(self, request: Mapping[str, Any], *, phase: str, argv: list[str],
                worktree: str, timeout_seconds: float, input_request: Mapping[str, Any] | None = None,
                writable: bool = False, on_started=None, env: Mapping[str, str] | None = None):
        """Bind projected bytes and all phase grants before entering the backend."""
        remaining = self._phase_launch_guard(request, phase)
        if remaining is not None:
            if remaining <= 0:
                raise CapabilityError("phase_job_deadline_expired")
            timeout_seconds = min(timeout_seconds, remaining)
        if self.policy is not None:
            if "null_device_check" in self.raw["fence"]:
                raise CapabilityError("null_device_check_profile_invalid")
            return self._trusted_command(request, phase=phase, argv=argv, worktree=worktree,
                timeout_seconds=timeout_seconds, input_request=input_request, writable=writable,
                on_started=on_started, env=env)
        if phase in {"planner", "plan_verifier"} and writable:
            raise CapabilityError("planner_candidate_write_forbidden")
        if __package__:
            from . import execution_fence as fences
        else:
            import execution_fence as fences
        from . import provider_input_binding as inputs
        from .cli_agent_executor import resolve_cli

        if self.host["interface"] != "headless_cli":
            raise CapabilityError("execution_host_adapter_unsupported:" + self.host["interface"])
        if self.raw["fence"]["egress_policy_ref"] is not None:
            raise CapabilityError("egress_policy_profile_unsupported:bounded-command-v1")

        goal_id = _text("execution_context.goal_id", request.get("goal_id"))
        goal_revision = request.get("goal_revision")
        if isinstance(goal_revision, bool) or not isinstance(goal_revision, int) or goal_revision < 1:
            raise CapabilityError("execution_context_goal_revision_invalid")
        role = "coding" if phase == "coding" else "integration" if phase == "integration" else (
            "planner" if phase == "planner" else "verifier" if phase == "plan_verifier" or "verifier" in phase else "checks")
        native = self.native_runtime is not None
        if native:
            identity = self._native_command_identity(request, phase)
        else:
            # A missing WorkUnit field never opts a legacy call into native authority.
            unit = _text("execution_context.work_unit_id", request.get("work_unit_id"))
        run_id = _text("execution_context.run_id", request.get("run_id"))
        base = _text("execution_context.base_sha", request.get("base_sha"))
        attempt = request.get("attempt")
        attempt_fence = request.get("fence")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 1
               for value in (attempt, attempt_fence)):
            raise CapabilityError("execution_context_attempt_identity_missing")
        capability = (self.runner.native_binding(role, unit_id=identity["unit_id"], attempt_id=str(attempt))
                      if native else self.runner.binding(role, work_unit_id=unit, attempt_id=str(attempt)))
        if native and phase in {"planner", "plan_verifier"}:
            config = self.native_runtime["planner_recovery"]
            if argv != config[phase + "_argv"]:
                raise CapabilityError("native_recovery_command_mismatch")
        argv = [resolve_cli(argv[0]), *argv[1:]]
        null_device_check = self._null_device_check(phase=phase, argv=argv, capability=capability,
            writable=writable, input_request=input_request)
        read_roots = []
        if input_request is not None and input_request.get("packet_path"):
            packet_path = Path(input_request["packet_path"]).resolve(strict=True)
            if not packet_path.is_relative_to(Path(worktree)):
                read_roots.append(str(packet_path))
        context = {"phase": phase, "execution_binding_digest": self.digest,
                   "capability": capability, "host_binding_digest": self.host["binding_digest"],
                   "packet_digest": request.get("packet_digest"), "command": argv,
                   "input_digest": digest_json(dict(input_request or {}))}
        binding = fences.build_attempt_binding(
            goal={"goal_id": goal_id, "goal_revision": goal_revision},
            run_id=run_id, attempt=attempt, attempt_fence=attempt_fence,
            base_revision=base,
            clone_root=worktree, verifier_argv=argv,
            adapter_id=capability["adapter_id"],
            adapter_version=str(capability["identity"].get("adapter_version", "v1")),
            timeout_seconds=timeout_seconds, allowed_read_roots=read_roots,
            allowed_write_roots=[worktree] if writable else [],
            allowed_local_effects=["workspace_write", "scratch_write"] if writable else ["scratch_write"],
            execution_context_digest=digest_json(context), null_device_check=null_device_check)
        descriptor = self.port.prepare(binding)
        if descriptor.get("binding") != binding:
            raise fences.ExecutionFenceUnavailable("binding_mismatch")
        projected = self.port.project_paths(descriptor, input_request or {})
        prompt = inputs.canonical_json(projected) if input_request is not None else ""
        projection, _ = inputs.project_context({})
        # The command receives only backend-declared values. Caller environments
        # may carry credentials or controller-private Git handles; neither crosses.
        environment = self.port.project_environment(descriptor, env or {})
        argv = self.port.project_command(descriptor, argv, env or {})
        input_binding = inputs.build_input_binding(binding_context={
            "goal_revision": binding["goal_revision"], "run_id": binding["run_id"], "attempt": attempt,
            "adapter_id": binding["adapter_id"], "adapter_version": binding["adapter_version"],
            "capability_digest": capability["contract_digest"],
            "authority_digest": self.bootstrap["bootstrap_authority"]["authority_digest"]},
            projection_record=projection, segments=inputs.build_segments(prompt, argv, environment),
            launch_descriptor_digest=descriptor["launch_descriptor_digest"],
            nonce=binding["controller_nonce"], issued_at=time.time(), ttl_seconds=timeout_seconds)
        inputs.attest_before_launch(input_binding, prompt=prompt, command_template=argv,
            environment_projection=environment, launch_descriptor_digest=descriptor["launch_descriptor_digest"],
            now=time.time(), seen_nonces=set())
        fence_projection = self.port.receipt_projection(descriptor)
        self._phase_launch_guard(request, phase)
        result = self.port.launch(descriptor, argv, input_text=prompt if input_request is not None else None,
            timeout_seconds=timeout_seconds, env_projection=environment, on_started=on_started)
        return result, {"execution_fence": fence_projection,
                        "provider_input_binding": input_binding, "capability_binding": capability,
                        "execution_context_digest": binding["execution_context_digest"]}, descriptor


__all__ = [
    "Adapter",
    "BINDING_SCHEMA",
    "CAPABILITY_CONTRACT_SCHEMA",
    "CapabilityBoundRunner",
    "CapabilityError",
    "CapabilityRunner",
    "REQUIRED_CAPABILITIES",
    "REQUIRED_ROLES",
    "OPTIONAL_CAPABILITIES",
    "OPTIONAL_ROLES",
    "PLANNING_CAPABILITY",
    "PLANNER_REQUIRED_CAPABILITIES",
    "PLANNER_ROLE",
    "ROLE_CAPABILITY",
    "SUPPORTED_ROLES",
    "digest_json",
    "validate_contract",
    "validate_planning_contract",
]

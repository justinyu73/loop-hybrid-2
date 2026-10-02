"""Durable parent-goal and child WorkUnit state for the parallel wave.

The existing GoalStore and RunStore remain the authority for the serial
single-project loop.  This module is the small parallel-wave projection used
by P2: a parent goal owns immutable child work definitions, each child gets
one durable run, and attempts are protected by a monotonic fence.

All state is rooted at the path supplied by the caller.  The module never
selects a production state directory and has no process-wide singleton.
"""

from __future__ import annotations

import hashlib
import copy
import json
import math
import os
import posixpath
import re
import sqlite3
import subprocess
import time
from datetime import datetime
from itertools import combinations
from pathlib import Path
from urllib.parse import quote
from typing import Any, Iterable, Mapping


try:
    from .lifecycle import NativeProcessIdentityPort, ProcessIdentity, ProcessIdentityPort, observe_process_identity
    from . import trusted_continuation
except ImportError:
    from lifecycle import NativeProcessIdentityPort, ProcessIdentity, ProcessIdentityPort, observe_process_identity
    import trusted_continuation


SCHEMA = "lh-parallel-work-unit-store/v1"
DISPATCH_CONSUMPTION_SCHEMA = "lh-successor-dispatch-consumption/v1"
DISPATCH_RECEIPT_SCHEMA = "lh-successor-dispatch-receipt/v1"
EXECUTOR_FAILURE_RECEIPT_SCHEMA = "lh-successor-executor-failure/v1"
CANDIDATE_RECOVERY_ADMISSION_SCHEMA = "host-p7-candidate-recovery-admission/v1"
CANDIDATE_RECOVERY_DECISION_ID = "HOST-P7-TIMEOUT-CANDIDATE-RESUME-20260908"
PRESERVED_RESULT_DECISION_ID = "HOST-P7-PRESERVED-RESULT-RECOVERY-20260911"
MAX_EXECUTOR_LAUNCHES = 3
RECOVERY_RECORD_SCHEMA = "lh-recovery-record/v1"
RECOVERY_BUDGET_FIELDS = (
    "planner_calls",
    "plan_verifier_calls",
    "planner_timeout_seconds",
    "plan_verifier_timeout_seconds",
    "incident_timeout_seconds",
)
RECOVERY_MAX_CALLS = 100_000
RECOVERY_MAX_SECONDS = 31_536_000.0
WORK_UNIT_STATES = {"pending", "ready", "running", "retry_pending", "verified", "integrated", "stopped"}
RUN_STATES = {"queued", "ready", "running", "retry_pending", "verified", "integrated", "stopped"}
ATTEMPT_STATES = {"ready", "running", "interrupted", "retry_pending", "verified", "integrated", "stopped"}


class WorkUnitStoreError(ValueError):
    """Base error for malformed or inconsistent parallel-wave state."""


class DuplicateWorkUnitError(WorkUnitStoreError):
    """A work-unit or node identity is already bound to different data."""


class DependencyError(WorkUnitStoreError):
    """A dependency is missing, ambiguous, or crosses the parent boundary."""


class DAGCycleError(DependencyError):
    """The child dependency graph contains a cycle."""

    def __init__(self, cycle: Iterable[str]):
        self.cycle = tuple(cycle)
        super().__init__("dependency cycle: " + " -> ".join(self.cycle))


class BaseMismatchError(WorkUnitStoreError):
    """A work definition does not use the parent wave base."""


class LeaseBusyError(WorkUnitStoreError):
    """Another holder owns the live lease for this WorkUnit."""

    def __init__(self, work_unit_id: str, holder: str | None = None):
        self.work_unit_id = work_unit_id
        self.holder = holder
        suffix = f" (holder={holder})" if holder else ""
        super().__init__(f"work unit lease is busy: {work_unit_id}{suffix}")


class FenceError(WorkUnitStoreError):
    """A completion used an old or unrelated attempt fence."""


class _ClosingConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_json(value: Any) -> str:
    """Return the same digest form used by durable scheduler records."""

    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkUnitStoreError(f"{name} must be a non-empty string")
    return value.strip()


def normalize_path(path: str | Path) -> str:
    """Normalize a relative declared path and reject directory escape."""

    value = _required_text("path", str(path)).replace("\\", "/")
    if value.startswith("/") or re.match(r"^[A-Za-z]:/", value):
        raise WorkUnitStoreError("declared paths must be relative")
    normalized = posixpath.normpath(value)
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        raise WorkUnitStoreError("declared path escapes its relative root")
    return normalized.removeprefix("./")


def normalize_paths(paths: Iterable[str | Path] | str | Path | None) -> tuple[str, ...]:
    if paths is None:
        return ()
    if isinstance(paths, (str, Path)):
        paths = (paths,)
    return tuple(sorted({normalize_path(path) for path in paths}))


def normalize_dependencies(dependencies: Iterable[str] | str | None) -> tuple[str, ...]:
    if dependencies is None:
        return ()
    if isinstance(dependencies, str):
        dependencies = (dependencies,)
    result = {_required_text("dependency", dependency) for dependency in dependencies}
    return tuple(sorted(result))


def validate_delivery_after(value: Any) -> list[str]:
    """Keep explicit delivery edges distinct from an absent declaration."""
    if (not isinstance(value, list)
        or any(not isinstance(ref, str) or not ref or ref != ref.strip() for ref in value)):
        raise DependencyError("delivery_after must be a list of nonempty dependency references")
    return list(value)


def paths_overlap(left: str | Path, right: str | Path) -> bool:
    """Return true when two relative path declarations share a path."""

    a = normalize_path(left)
    b = normalize_path(right)
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")


def read_write_conflicts(left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, str]]:
    """Report write/write and write/read conflicts between two definitions."""

    left_reads = tuple(left.get("read_set") or ())
    left_writes = tuple(left.get("write_set") or ())
    right_reads = tuple(right.get("read_set") or ())
    right_writes = tuple(right.get("write_set") or ())
    conflicts: list[dict[str, str]] = []

    def add(source: Iterable[str], source_kind: str, targets: Iterable[str], target_kind: str) -> None:
        for source_path in source:
            for target_path in targets:
                if paths_overlap(source_path, target_path):
                    conflicts.append({
                        "left": str(source_path),
                        "right": str(target_path),
                        "left_kind": source_kind,
                        "right_kind": target_kind,
                    })

    add(left_writes, "write", right_writes, "write")
    add(left_writes, "write", right_reads, "read")
    add(left_reads, "read", right_writes, "write")
    return conflicts


def _json_list(value: str | None, name: str) -> list[str]:
    try:
        decoded = json.loads(value or "[]")
    except json.JSONDecodeError as exc:
        raise WorkUnitStoreError(f"stored {name} is not JSON") from exc
    if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
        raise WorkUnitStoreError(f"stored {name} must be a string list")
    return list(decoded)


def _candidate_recovery_file(path: Any, name: str) -> tuple[Path, dict[str, Any]]:
    """Read one executor evidence file without following a mutable link."""
    if not isinstance(path, str) or not path.strip():
        raise WorkUnitStoreError(f"candidate_recovery_{name}_path_missing")
    candidate = Path(path).expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise WorkUnitStoreError(f"candidate_recovery_{name}_file_missing")
    try:
        value = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkUnitStoreError(f"candidate_recovery_{name}_file_unreadable") from exc
    if not isinstance(value, dict):
        raise WorkUnitStoreError(f"candidate_recovery_{name}_file_invalid")
    return candidate.resolve(), value


def _candidate_recovery_time(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _candidate_recovery_process_alive(identity: Mapping[str, Any], *, identity_port: ProcessIdentityPort | None = None) -> bool:
    observation = observe_process_identity(identity, identity_port=identity_port)
    if observation.status == "unknown":
        raise WorkUnitStoreError("candidate_recovery_process_observation_unavailable")
    return observation.status == "alive"


def _candidate_recovery_process_identity_complete(identity: Any) -> bool:
    """Accept native birth tokens and legacy Linux identity records."""
    return ProcessIdentity.from_dict(identity) is not None

def recovery_budget_limits(budget: Mapping[str, Any]) -> dict[str, int | float]:
    if not isinstance(budget, Mapping):
        raise WorkUnitStoreError("recovery budget missing")
    limits: dict[str, int | float] = {}
    for name in ("planner_calls", "plan_verifier_calls"):
        value = budget.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= RECOVERY_MAX_CALLS:
            raise WorkUnitStoreError(f"recovery budget {name} invalid")
        limits[name] = value
    for name in ("planner_timeout_seconds", "plan_verifier_timeout_seconds", "incident_timeout_seconds"):
        value = budget.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise WorkUnitStoreError(f"recovery budget {name} invalid")
        number = float(value)
        if not math.isfinite(number) or not 0 < number <= RECOVERY_MAX_SECONDS:
            raise WorkUnitStoreError(f"recovery budget {name} invalid")
        limits[name] = number
    return limits

def recovery_valid_phase_binding(
    request: Mapping[str, Any],
    container: Mapping[str, Any],
    *,
    role: str,
    capability_name: str,
    principal: str,
    identity_profile: str = "work-unit-v1",
) -> bool:
    capability = container.get("capability_binding")
    receipt = container.get("execution_fence")
    input_binding = container.get("provider_input_binding")
    execution_context_digest = container.get("execution_context_digest")
    if not all(isinstance(item, Mapping) for item in (capability, receipt, input_binding)):
        return False
    identity = capability.get("identity")
    proofs = receipt.get("proofs")
    segments = input_binding.get("segments")

    def sha256_digest(value: Any) -> bool:
        return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None

    if not isinstance(identity, Mapping) or not isinstance(segments, list):
        return False
    native = identity_profile == "native-run-v1"
    if native:
        identity_matches = (
            request.get("identity_profile") == "native-run-v1"
            and request.get("authority_store") == capability.get("authority_store") == "run"
            and capability.get("identity_profile") == "native-run-v1"
            and "work_unit_id" not in request and "work_unit_id" not in capability
            and isinstance(request.get("unit_id"), str) and bool(request["unit_id"])
            and capability.get("unit_id") == request["unit_id"]
        )
    else:
        identity_matches = capability.get("work_unit_id") == request.get("work_unit_id")
    receipt_launch = receipt.get("launch_descriptor_digest")
    return (
        capability.get("schema") == "host-capability-binding/v1"
        and capability.get("role") == role
        and capability.get("capability") == capability_name
        and capability.get("permissions") == "read_only"
        and identity_matches
        and capability.get("attempt_id") == str(request.get("attempt"))
        and identity.get("principal") == principal
        and capability.get("identity_digest") == digest_json(dict(identity))
        and isinstance(capability.get("adapter_id"), str)
        and bool(capability["adapter_id"].strip())
        and sha256_digest(capability.get("contract_digest"))
        and receipt.get("schema") == "lh-execution-fence-launch/v1"
        and receipt.get("status") == "admitted"
        and sha256_digest(receipt_launch)
        and sha256_digest(receipt.get("binding_digest"))
        and isinstance(receipt.get("backend"), Mapping)
        and isinstance(proofs, (Mapping, list))
        and receipt.get("proofs_digest") == digest_json(proofs)
        and isinstance(receipt.get("provider_control_channel"), Mapping)
        and isinstance(receipt.get("launch_classes"), Mapping)
        and input_binding.get("schema") == "lh-provider-input-binding/v1"
        and input_binding.get("run_id") == request.get("run_id")
        and isinstance(input_binding.get("attempt"), int)
        and not isinstance(input_binding.get("attempt"), bool)
        and input_binding.get("attempt") == request.get("attempt")
        and input_binding.get("goal_revision") == digest_json({
            "goal_id": request.get("goal_id"),
            "goal_revision": request.get("goal_revision", request.get("revision")),
        })
        and input_binding.get("adapter_id") == capability.get("adapter_id")
        and input_binding.get("adapter_version") == identity.get("adapter_version", "v1")
        and input_binding.get("capability_digest") == capability.get("contract_digest")
        and input_binding.get("launch_descriptor_digest") == receipt_launch
        and sha256_digest(input_binding.get("projection_digest"))
        and sha256_digest(input_binding.get("authority_digest"))
        and input_binding.get("provider_input_digest") == digest_json({
            "segments": segments,
            "launch_descriptor_digest": receipt_launch,
        })
        and sha256_digest(input_binding.get("provider_input_digest"))
        and isinstance(input_binding.get("nonce"), str)
        and bool(input_binding["nonce"].strip())
        and sha256_digest(execution_context_digest)
    )

def validate_recovery_plan(record: Mapping[str, Any], *, audit_record=None,
                           identity_profile: str = "work-unit-v1") -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    request = record.get("request")
    result = record.get("result")
    verdict = record.get("verdict")
    if not isinstance(request, Mapping) or not isinstance(result, Mapping) or not isinstance(verdict, Mapping):
        raise WorkUnitStoreError("recovery plan or verdict missing")
    request_id = record.get("request_id")
    request_digest = record.get("request_digest")
    action = result.get("action")
    producer = result.get("producer_identity")
    producer_capability = result.get("capability_binding")
    verifier_capability = verdict.get("capability_binding")
    verifier_principal = verdict.get("principal")
    adapter_metadata = {
        "plan_digest", "probe", "execution_fence", "provider_input_binding",
        "capability_binding", "execution_context_digest",
    }
    canonical_plan = {
        key: value for key, value in result.items() if key not in adapter_metadata
    }
    calculated_plan_digest = digest_json(canonical_plan)
    if (
        result.get("request_id") != request_id
        or result.get("request_digest") != request_digest
        or verdict.get("request_id") != request_id
        or verdict.get("request_digest") != request_digest
        or not isinstance(action, Mapping)
        or action.get("kind") not in {
            "resume_phase", "repair_same_node", "retry_within_budget",
            "collect_evidence", "request_authority", "insufficient_evidence",
            "dispatch_successor",
        }
        or not isinstance(producer, Mapping)
        or not isinstance(producer.get("principal"), str)
        or not isinstance(producer_capability, Mapping)
        or not isinstance(verifier_capability, Mapping)
        or not isinstance(verifier_principal, str)
        or not verifier_principal.strip()
        or verifier_principal == producer.get("principal")
        or not recovery_valid_phase_binding(
            request, result, role="planner", capability_name="planning",
            principal=producer.get("principal"), identity_profile=identity_profile,
        )
        or not recovery_valid_phase_binding(
            request, verdict, role="verifier", capability_name="verifier",
            principal=verifier_principal, identity_profile=identity_profile,
        )
        or verdict.get("verdict") != "GREEN"
        or not isinstance(verdict.get("reasons"), list)
        or not verdict["reasons"]
        or verdict.get("read_only") is not True
        or verdict.get("source_write") is not False
        or result.get("plan_digest") != calculated_plan_digest
        or verdict.get("plan_digest") != calculated_plan_digest
        or verdict.get("candidate_digest") != request.get("candidate_digest")
        or verdict.get("authority_digest") != request.get("authority_digest")
    ):
        raise WorkUnitStoreError("recovery plan/verdict binding invalid")
    if "completed_effects" in request:
        basis = result.get("decision_basis")
        reason = basis.get("reason") if isinstance(basis, Mapping) else None
        if (
            not isinstance(basis, Mapping)
            or basis.get("request_digest") != request_digest
            or basis.get("evidence_refs") != request.get("sanitized_evidence_refs")
            or not isinstance(reason, str)
            or not reason.strip()
            or request.get("reason_code") not in reason
        ):
            raise WorkUnitStoreError("recovery decision basis binding invalid")
    if request.get("incident") is not None:
        basis = result.get("decision_basis")
        if (not isinstance(basis, Mapping)
            or any(not isinstance(basis.get(key), str) or not basis[key].strip()
                   for key in ("hypothesis", "related_change"))
            or basis.get("write_set") != request.get("write_set")
            or basis.get("test_refs") != request.get("test_refs")
            or result.get("test_refs") != request.get("test_refs")):
            raise WorkUnitStoreError("recovery repair decision basis invalid")
        if WorkUnitStore._recovery_audit_required(request):
            audit_ref = WorkUnitStore._recovery_audit_ref(record, audit_record)
            if basis.get("audit_ref") != audit_ref or verdict.get("audit_ref") != audit_ref:
                raise WorkUnitStoreError("recovery decision audit reference invalid")
    if request.get("repair_context_required") is True or "repair_context_ref" in request:
        ref = request.get("repair_context_ref")
        basis = result.get("decision_basis")
        if (request.get("repair_context_required") is not True
            or not isinstance(ref, Mapping) or not isinstance(basis, Mapping)
            or not isinstance(ref.get("evidence_digest"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", ref["evidence_digest"]) is None
            or basis.get("repair_context_digest") != ref["evidence_digest"]
            or verdict.get("repair_context_digest") != ref["evidence_digest"]):
            raise WorkUnitStoreError("recovery repair context binding invalid")
    preconditions = result.get("preconditions")
    authority = result.get("authority_comparison")
    if not isinstance(preconditions, Mapping) or not isinstance(authority, Mapping):
        raise WorkUnitStoreError("recovery plan preconditions missing")
    for key in (
        "candidate_digest", "attempt", "fence", "packet_digest",
        "envelope_digest", "authority_digest", "capability_binding",
    ):
        if preconditions.get(key) != request.get(key):
            raise WorkUnitStoreError(f"recovery plan precondition mismatch: {key}")
    if (
        authority.get("authority_digest") != request.get("authority_digest")
        or authority.get("write_set") != request.get("write_set")
        or authority.get("budget") != request.get("remaining_budget")
    ):
        raise WorkUnitStoreError("recovery plan authority comparison mismatch")
    if (action.get("kind") == "resume_phase" and result.get("target_phase") != "closeout"
        and not (result.get("target_phase") == request.get("phase") == "checks"
                 and request.get("environment_event_ref") is not None and audit_record is not None)):
        raise WorkUnitStoreError("recovery resume phase unsupported")
    if action.get("kind") == "repair_same_node":
        if (
            not isinstance(result.get("repair_packet"), Mapping)
            or result.get("repair_packet") != request.get("repair_packet")
            or not isinstance(result.get("test_refs"), list)
            or not result["test_refs"]
        ):
            raise WorkUnitStoreError("recovery repair packet/test evidence invalid")
    return dict(request), dict(result), dict(verdict)

class WorkUnitStore:
    """SQLite store for one parent Goal and its child WorkUnits.

    ``create_work_unit`` is idempotent for identical immutable inputs.  A
    repeated identity with changed inputs is rejected.  ``register_work_units``
    validates all dependency edges before inserting a batch, so a cycle never
    becomes a partially registered graph.
    """

    def __init__(self, root: str | Path, *, now_fn=None, read_only: bool = False,
                 identity_port: ProcessIdentityPort | None = None):
        self.identity_port = identity_port if identity_port is not None else NativeProcessIdentityPort()
        self.root = Path(root)
        self.db_path = self.root / "work-units.sqlite3"
        self._now_fn = now_fn or time.time
        self.read_only = read_only
        if read_only:
            if not self.db_path.is_file():
                raise WorkUnitStoreError("read_only_queue_missing")
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS trusted_execution_budgets (
                    goal_id TEXT NOT NULL,
                    goal_revision INTEGER NOT NULL,
                    execution_binding_digest TEXT,
                    budget_json TEXT NOT NULL,
                    PRIMARY KEY(goal_id, goal_revision)
                );
                CREATE TABLE IF NOT EXISTS parent_goals (
                    parent_goal_id TEXT PRIMARY KEY,
                    goal_id TEXT NOT NULL,
                    goal_revision INTEGER NOT NULL,
                    base_sha TEXT NOT NULL,
                    dependencies_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS work_units (
                    work_unit_id TEXT PRIMARY KEY,
                    parent_goal_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_kind TEXT NOT NULL DEFAULT 'coding',
                    producer TEXT NOT NULL DEFAULT 'scheduler',
                    worker_id TEXT NOT NULL,
                    base_sha TEXT NOT NULL,
                    dependencies_json TEXT NOT NULL,
                    read_set_json TEXT NOT NULL,
                    delivery_after_json TEXT,
                    write_set_json TEXT NOT NULL,
                    worktree TEXT,
                    branch TEXT,
                    state_root TEXT,
                    state TEXT NOT NULL,
                    run_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(parent_goal_id, node_id),
                    FOREIGN KEY(parent_goal_id) REFERENCES parent_goals(parent_goal_id)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    parent_goal_id TEXT NOT NULL,
                    work_unit_id TEXT NOT NULL UNIQUE,
                    base_sha TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    fence INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(parent_goal_id) REFERENCES parent_goals(parent_goal_id),
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    holder TEXT NOT NULL,
                    fence INTEGER NOT NULL,
                    workspace_ref TEXT NOT NULL,
                    receipt_ref TEXT,
                    receipt_digest TEXT,
                    created_at REAL NOT NULL,
                    finished_at REAL,
                    PRIMARY KEY(run_id, ordinal),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    work_unit_id TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    fence INTEGER NOT NULL,
                    expires_at REAL NOT NULL,
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id)
                );
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    parent_goal_id TEXT NOT NULL,
                    work_unit_id TEXT,
                    run_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(parent_goal_id) REFERENCES parent_goals(parent_goal_id)
                );
                CREATE TABLE IF NOT EXISTS dispatch_consumptions (
                    dispatch_key TEXT PRIMARY KEY,
                    envelope_digest TEXT NOT NULL,
                    parent_goal_id TEXT NOT NULL,
                    goal_id TEXT NOT NULL,
                    goal_revision INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    work_unit_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_digest TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(parent_goal_id) REFERENCES parent_goals(parent_goal_id),
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_work_units_parent ON work_units(parent_goal_id, created_at, node_id);
                CREATE INDEX IF NOT EXISTS idx_runs_parent ON runs(parent_goal_id, created_at, run_id);
                CREATE INDEX IF NOT EXISTS idx_attempts_run ON attempts(run_id, ordinal);
                CREATE INDEX IF NOT EXISTS idx_dispatch_parent ON dispatch_consumptions(parent_goal_id, created_at, dispatch_key);
                CREATE TABLE IF NOT EXISTS completion_phases (
                    phase_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL, fence INTEGER NOT NULL,
                    phase TEXT NOT NULL, binding TEXT NOT NULL,
                    state TEXT NOT NULL, evidence_json TEXT,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS delivery_bindings (
                    work_unit_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    unit_id TEXT NOT NULL,
                    contract_digest TEXT NOT NULL,
                    plan_verdict_digest TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    contract_json TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    verdict_json TEXT,
                    verdict_digest TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS planning_requests (
                    request_id TEXT PRIMARY KEY,
                    work_unit_id TEXT,
                    input_digest TEXT NOT NULL UNIQUE,
                    request_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id)
                );
                CREATE TABLE IF NOT EXISTS candidate_recovery_admissions (
                    admission_id TEXT PRIMARY KEY,
                    input_digest TEXT NOT NULL UNIQUE,
                    dispatch_key TEXT NOT NULL UNIQUE,
                    work_unit_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    fence INTEGER NOT NULL,
                    admission_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(work_unit_id) REFERENCES work_units(work_unit_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_candidate_recovery_run
                    ON candidate_recovery_admissions(run_id, attempt, fence);
                """
            )
            # Stores created before typed planning metadata remain ordinary
            # coding/scheduler units.  Only an explicit persisted definition
            # may authorize the PlanNodeController start exception.
            columns = {row[1] for row in conn.execute("PRAGMA table_info(work_units)").fetchall()}
            if "node_kind" not in columns:
                conn.execute("ALTER TABLE work_units ADD COLUMN node_kind TEXT NOT NULL DEFAULT 'coding'")
            if "producer" not in columns:
                conn.execute("ALTER TABLE work_units ADD COLUMN producer TEXT NOT NULL DEFAULT 'scheduler'")
            if "delivery_after_json" not in columns:
                conn.execute("ALTER TABLE work_units ADD COLUMN delivery_after_json TEXT")

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            wal = Path(str(self.db_path) + "-wal")
            try:
                pending = wal.stat().st_size
            except FileNotFoundError:
                # Absent, or checkpointed and removed by a writer since: no uncheckpointed frames either way.
                pending = 0
            if pending:
                raise WorkUnitStoreError("read_only_queue_uncheckpointed_wal")
            uri = f"file:{quote(str(self.db_path.resolve()), safe='/')}?mode=ro&immutable=1"
            conn = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None, factory=_ClosingConnection)
        else:
            conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None, factory=_ClosingConnection)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _now(self) -> float:
        return float(self._now_fn())

    @staticmethod
    def _recovery_record_from_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        try:
            value = json.loads(row["request_json"])
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("recovery record unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != RECOVERY_RECORD_SCHEMA:
            return None
        return value

    @staticmethod
    def _recovery_budget_limits(budget: Mapping[str, Any]) -> dict[str, int | float]:
        return recovery_budget_limits(budget)

    @staticmethod
    def _recovery_key_identity(request: Mapping[str, Any]) -> dict[str, Any]:
        revision_key = "goal_revision" if "goal_revision" in request else "revision"
        keys = (
            "goal_id", revision_key, "node_id", "work_unit_id", "run_id",
            "attempt", "fence", "phase", "candidate_digest", "reason_code",
            "input_evidence_digest", "authority_digest",
        )
        identity = {key: request[key] for key in keys}
        if request.get("environment_event_ref") is not None:
            identity.update({key: request[key] for key in (
                "predecessor_request_id", "predecessor_request_digest", "environment_event_ref")})
        return identity

    @classmethod
    def _recovery_environment_event_conn(cls, conn, ref):
        if not isinstance(ref, Mapping):
            raise WorkUnitStoreError("recovery_environment_event_ref_invalid")
        row = conn.execute("SELECT * FROM events WHERE event_id=?", (ref.get("event_id"),)).fetchone()
        if row is None:
            raise WorkUnitStoreError("recovery_environment_event_missing")
        event = cls._disposition_event_value(row)
        payload = event["payload"]
        if (event["event_type"] != "task_area_prerequisite_updated"
            or payload.get("observation_kind") not in {"baseline", "changed"}
            or digest_json(payload) != ref.get("payload_digest")):
            raise WorkUnitStoreError("recovery_environment_event_ref_invalid")
        claim_row = conn.execute("SELECT * FROM events WHERE event_id=?",
            ("task-area-event-claimed:" + event["event_id"],)).fetchone()
        settled_row = conn.execute("SELECT * FROM events WHERE event_id=?",
            ("task-area-event-settled:" + event["event_id"],)).fetchone()
        if claim_row is None or settled_row is None:
            raise WorkUnitStoreError("recovery_environment_event_not_settled")
        claim, settled = json.loads(claim_row["payload_json"]), json.loads(settled_row["payload_json"])
        source_digest = digest_json({key: event[key] for key in
            ("event_id", "parent_goal_id", "event_type", "payload", "created_at")})
        outcome = settled.get("outcome", {})
        if (claim.get("source_event_id") != event["event_id"]
            or claim.get("source_event_digest") != source_digest
            or settled.get("source_event_id") != event["event_id"]
            or settled.get("claim_id") != claim.get("claim_id")
            or settled.get("outcome_digest") != digest_json(outcome)
            or outcome.get("status") != "accepted"
            or outcome.get("kind") != "recovery_environment_" + payload["observation_kind"]
            or outcome.get("payload_digest") != ref["payload_digest"]):
            raise WorkUnitStoreError("recovery_environment_event_not_accepted")
        return event

    @classmethod
    def _recovery_environment_packet_conn(cls, conn, request):
        cls._recovery_check_run_identity_conn(conn, request, require_current=True)
        row = conn.execute("SELECT * FROM dispatch_consumptions WHERE run_id=?",
                           (request["run_id"],)).fetchone()
        dispatch = cls._dispatch_row(row) if row else None
        if (not dispatch or dispatch["receipt"].get("executor_status") != "accepted"
            or dispatch["attempt"] != request["attempt"]
            or dispatch["envelope_digest"] != request.get("envelope_digest")
            or dispatch["receipt"].get("fence") != request["fence"]
            or dispatch["receipt_digest"] != digest_json({k: v for k, v in dispatch["receipt"].items()
                                                        if k != "receipt_digest"})):
            raise WorkUnitStoreError("recovery_environment_executor_not_accepted")
        envelope = dispatch["envelope"]
        packet = json.loads(Path(envelope["packet_path"]).read_bytes())
        if (digest_json({k: v for k, v in packet.items() if k != "packet_digest"})
            != envelope["packet_digest"] or request.get("packet_digest") != envelope["packet_digest"]):
            raise WorkUnitStoreError("recovery_environment_commands_drift")
        contract = packet.get("packet", packet).get("completion_contract", {})
        ref = contract.get("recovery_environment_ref")
        if not isinstance(ref, Mapping) or ref != request.get("recovery_environment_ref"):
            raise WorkUnitStoreError("recovery_environment_intent_invalid")
        path = Path(ref["path"])
        raw = path.read_bytes()
        if path.is_symlink() or "sha256:" + hashlib.sha256(raw).hexdigest() != ref.get("digest"):
            raise WorkUnitStoreError("recovery_environment_intent_changed")
        intent = json.loads(raw)
        if intent.get("schema") != "lh-recovery-environment-intent/v1" or intent.get("target_phase") != "checks":
            raise WorkUnitStoreError("recovery_environment_intent_invalid")
        return contract, intent

    @classmethod
    def _recovery_environment_audit_conn(cls, conn, record):
        request = record["request"]
        if request.get("environment_event_ref") is None:
            return None
        original = cls._recovery_record_from_row(
            cls._recovery_record_row_conn(conn, request.get("predecessor_request_id")))
        if (original is None or original["request_digest"] != request.get("predecessor_request_digest")
            or original["request_digest"] != digest_json(original["request"])):
            raise WorkUnitStoreError("recovery_environment_audit_ref_mismatch")
        cls._recovery_audit_ref(original)
        audit = original["audit_result"]
        expected = {key: audit[key] for key in ("request_id", "request_digest", "audit_digest")}
        if request.get("audit_ref") != expected:
            raise WorkUnitStoreError("recovery_environment_audit_ref_mismatch")
        for field in original["request"]:
            if field in {"request_id", "remaining_budget", "observed_cost"}:
                continue
            if request.get(field) != original["request"].get(field):
                raise WorkUnitStoreError("recovery_environment_original_request_changed")
        if request.get("incident") != cls._recovery_incident_conn(conn, request):
            raise WorkUnitStoreError("recovery incident history mismatch")
        if (original.get("status") != "audit_recorded"
            or any(c["phase"] != "audit" for c in original["claims"])
            or original.get("environment_successor_request_id") not in (None, request["request_id"])):
            raise WorkUnitStoreError("recovery_environment_predecessor_already_consumed")
        contract, _ = cls._recovery_environment_packet_conn(conn, request)
        event = cls._recovery_environment_event_conn(conn, request["environment_event_ref"])
        payload = event["payload"]
        red_ref = {key: request["repair_packet"][key] for key in ("phase_key", "receipt_digest")}
        if payload.get("original_red_ref") != red_ref:
            raise WorkUnitStoreError("recovery_environment_red_receipt_mismatch")
        if (payload.get("observation_kind") != "changed" or payload.get("audit_ref") != expected
            or payload.get("commands_digest") != digest_json(contract["checks"])
            or any(payload.get(k) != request.get(k) for k in
                   ("run_id", "attempt", "fence", "candidate_digest", "authority_digest"))):
            raise WorkUnitStoreError("recovery_environment_event_binding_invalid")
        from .task_area import TaskAreaController, manifest_digest
        manifest = json.loads(Path(payload["manifest_ref"]["path"]).read_bytes())
        if manifest_digest(manifest) != request["authority_digest"]:
            raise WorkUnitStoreError("recovery_environment_authority_drift")
        source_ref = manifest["reviewed_task_list_binding"]["source_approval_ref"]
        # Reuse the original source reader's digest, active/revoked and expiry checks.
        TaskAreaController._read_approval_source(source_ref)
        return original

    def recovery_environment_audit(self, request_id):
        with self._connect() as conn:
            record = self._recovery_record_from_row(self._recovery_record_row_conn(conn, request_id))
            if record is None:
                raise KeyError(request_id)
            original = self._recovery_environment_audit_conn(conn, record)
            if original is None:
                self._recovery_audit_ref(record)
                original = record
            return original["audit_result"]

    def recovery_environment_resume(self, run_id, attempt, fence):
        """Read the one applied same-candidate permission, never create an effect."""
        with self._connect() as conn:
            records = [record for _, record in self._recovery_records_for_run_conn(conn, run_id)
                if record["request"].get("environment_event_ref") is not None
                and record["request"]["attempt"] == attempt and record["request"]["fence"] == fence
                and record.get("status") == "applied"]
            if not records:
                return None
            if len(records) != 1:
                raise WorkUnitStoreError("recovery_environment_successor_not_unique")
            record = records[0]
            original = self._recovery_environment_audit_conn(conn, record)
            request, plan, _ = self._recovery_validate_plan(record, audit_record=original)
            applied = record.get("apply", {})
            if (plan["action"] != {"kind": "resume_phase"} or plan["target_phase"] != "checks"
                or applied.get("state") != "applied" or applied.get("action") != plan["action"]
                or applied.get("plan_digest") != plan["plan_digest"]
                or applied.get("request_digest") != record["request_digest"]
                or original.get("environment_successor_request_id") != record["request_id"]):
                raise WorkUnitStoreError("recovery_environment_apply_invalid")
            return {"request_id": record["request_id"],
                "original_red_ref": {k: request["repair_packet"][k] for k in ("phase_key", "receipt_digest")},
                "candidate_digest": request["candidate_digest"],
                "environment_event_ref": request["environment_event_ref"]}

    @staticmethod
    def _recovery_save_conn(
        conn: sqlite3.Connection,
        request_id: str,
        record: Mapping[str, Any],
        now: float,
    ) -> None:
        conn.execute(
            "UPDATE planning_requests SET request_json = ?, status = ?, updated_at = ? WHERE request_id = ?",
            (json.dumps(dict(record), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
             record["status"], now, request_id),
        )

    @staticmethod
    def _recovery_record_row_conn(conn: sqlite3.Connection, request_id: str):
        return conn.execute(
            "SELECT * FROM planning_requests WHERE request_id = ?", (request_id,)
        ).fetchone()

    @classmethod
    def _recovery_records_for_run_conn(
        cls,
        conn: sqlite3.Connection,
        run_id: str,
    ) -> list[tuple[sqlite3.Row, dict[str, Any]]]:
        run = conn.execute("SELECT work_unit_id FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if run is None:
            return []
        rows = conn.execute(
            "SELECT * FROM planning_requests WHERE work_unit_id = ? ORDER BY created_at, request_id",
            (run["work_unit_id"],),
        ).fetchall()
        records = []
        for row in rows:
            record = cls._recovery_record_from_row(row)
            if record is not None and record.get("request", {}).get("run_id") == run_id:
                records.append((row, record))
        return records

    @staticmethod
    def _recovery_check_run_identity_conn(
        conn: sqlite3.Connection,
        request: Mapping[str, Any],
        *,
        require_current: bool,
    ) -> sqlite3.Row:
        revision = request.get("goal_revision", request.get("revision"))
        row = conn.execute(
            "SELECT r.*, p.goal_id, p.goal_revision, w.node_id "
            "FROM runs r JOIN work_units w ON w.work_unit_id = r.work_unit_id "
            "JOIN parent_goals p ON p.parent_goal_id = r.parent_goal_id "
            "WHERE r.run_id = ?",
            (request.get("run_id"),),
        ).fetchone()
        if (
            row is None
            or row["work_unit_id"] != request.get("work_unit_id")
            or row["goal_id"] != request.get("goal_id")
            or int(row["goal_revision"]) != revision
            or row["node_id"] != request.get("node_id")
        ):
            raise FenceError("recovery request Run identity mismatch")
        if require_current:
            attempt, fence = request.get("attempt"), request.get("fence")
            current = conn.execute(
                "SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?",
                (request.get("run_id"), attempt),
            ).fetchone()
            if (
                isinstance(attempt, bool) or not isinstance(attempt, int)
                or isinstance(fence, bool) or not isinstance(fence, int)
                or row["attempts"] != attempt or row["fence"] != fence
                or current is None or current["fence"] != fence
            ):
                raise FenceError("recovery request stale Attempt/fence")
        return row

    @staticmethod
    def _recovery_valid_phase_binding(request, container, *, role, capability_name, principal):
        return recovery_valid_phase_binding(request, container, role=role,
            capability_name=capability_name, principal=principal)

    @classmethod
    def _recovery_incident_conn(cls, conn: sqlite3.Connection, request: Mapping[str, Any]) -> dict[str, Any]:
        """Derive a same-Run streak from settled receipts, never from a caller's count."""
        cls._recovery_check_run_identity_conn(conn, request, require_current=False)
        phase = request.get("phase")
        tests = request.get("test_refs")
        if (phase not in {"checks", "verifier", "integration_checks", "integration_verifier", "delivery_verifier"}
            or not isinstance(tests, list) or not tests
            or any(not isinstance(item, Mapping) or not item.get("id")
                   or not item.get("command_digest") for item in tests)):
            raise WorkUnitStoreError("recovery incident validation binding invalid")
        rows = conn.execute(
            "SELECT a.ordinal, a.fence AS attempt_fence, c.* FROM attempts a "
            "LEFT JOIN completion_phases c ON c.run_id=a.run_id AND c.attempt=a.ordinal "
            "AND c.fence=a.fence AND c.phase=? "
            "WHERE a.run_id=? AND a.ordinal<=? ORDER BY a.ordinal, c.phase_key",
            (phase, request["run_id"], request["attempt"]),
        ).fetchall()
        refs: list[dict[str, Any]] = []
        validation_key = None
        current = None
        for row in rows:
            canonical_key = digest_json([request["run_id"], row["ordinal"], row["attempt_fence"], phase])
            if row["phase_key"] is not None and row["phase_key"] != canonical_key:
                saved = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
                saved = saved.get("repair_claim", saved)
                owner = cls._recovery_record_from_row(cls._recovery_record_row_conn(
                    conn, saved.get("environment_request_id")))
                if (phase != "checks" or owner is None or owner.get("status") != "applied"
                    or owner["request"].get("environment_event_ref") is None
                    or saved.get("predecessor_receipt_digest") != owner["request"]["repair_packet"]["receipt_digest"]
                    or row["phase_key"] != digest_json([request["run_id"], row["ordinal"],
                        row["attempt_fence"], phase, saved["predecessor_receipt_digest"]])):
                    raise WorkUnitStoreError("recovery incident settled receipt invalid")
                continue  # The original Attempt failure remains the incident's evidence.
            if row["state"] != "settled":
                refs = []
                validation_key = None
                continue
            evidence = json.loads(row["evidence_json"])
            if (not isinstance(evidence, Mapping)
                or evidence.get("receipt_digest") != digest_json({
                    key: value for key, value in evidence.items() if key != "receipt_digest"})
                or any(evidence.get(key) != row[key] for key in (
                    "run_id", "attempt", "fence", "phase", "phase_key", "binding"))
                or row["phase_key"] != digest_json([
                    request["run_id"], row["ordinal"], row["attempt_fence"], phase])):
                raise WorkUnitStoreError("recovery incident settled receipt invalid")
            if row["ordinal"] == request["attempt"]:
                current = evidence
            if evidence.get("verdict") != "RED":
                refs = []
                validation_key = None
                continue
            failed_checks = None
            if phase in {"checks", "integration_checks"}:
                checks = evidence.get("checks")
                if (not isinstance(checks, list) or not checks
                    or any(not isinstance(check, Mapping) for check in checks)
                    or [check.get("id") for check in checks] != [item["id"] for item in tests]):
                    raise WorkUnitStoreError("recovery incident checks binding invalid")
                failed_checks = [check["id"] for check in checks
                                 if check.get("exit_code") != check.get("expect_exit")]
                if not failed_checks:
                    raise WorkUnitStoreError("recovery incident RED has no failed check")
            key = digest_json({"phase": phase, "test_refs": tests, "failed_checks": failed_checks})
            if key != validation_key:
                refs = []
                validation_key = key
            refs.append({"phase": phase, "phase_key": row["phase_key"],
                         "receipt_digest": evidence["receipt_digest"],
                         "attempt": row["ordinal"], "fence": row["attempt_fence"]})
        if (not refs or refs[-1]["attempt"] != request["attempt"]
            or refs[-1]["fence"] != request["fence"]
            or not isinstance(current, Mapping)
            or digest_json(current) != request.get("input_evidence_digest")
            or current.get("candidate_digest") != request.get("candidate_digest")):
            raise WorkUnitStoreError("recovery incident current RED binding invalid")
        # Earlier decisions cannot silently relabel this Run's validation contract.
        for _, prior in cls._recovery_records_for_run_conn(conn, request["run_id"]):
            old = prior["request"]
            if (old.get("incident") is not None and old.get("phase") == phase
                and old.get("test_refs") != tests):
                raise WorkUnitStoreError("recovery incident validation contract changed")
        return {"schema": "lh-recovery-incident/v1",
                "incident_id": digest_json({"run_id": request["run_id"],
                    "validation_key": validation_key, "first_failure": refs[0]}),
                "validation_key": validation_key,
                "failure_count": len(refs), "failure_refs": refs}

    def recovery_incident(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Read all relevant Attempt receipts in one snapshot; this does not create state."""
        with self._connect() as conn:
            conn.execute("BEGIN")
            return self._recovery_incident_conn(conn, request)

    @staticmethod
    def recovery_repair_context_ref(context: Mapping[str, Any]) -> dict[str, Any]:
        """Bind evidence, not a claim that the difference is a useful repair."""
        previous = context["previous_request"]
        fields = ("phase", "phase_key", "receipt_digest", "candidate_digest",
                  "candidate_commit", "attempt", "fence")
        return {
            "previous_request_id": previous["request_id"],
            "previous_request_digest": digest_json(previous),
            "previous_plan_digest": context["previous_result"]["plan_digest"],
            "before_candidate_ref": {key: context["before_candidate"][key] for key in fields},
            "after_candidate_ref": {key: context["after_candidate"][key] for key in fields},
            "evidence_digest": digest_json(context),
        }

    @classmethod
    def _recovery_repair_context_conn(cls, conn: sqlite3.Connection,
                                     request: Mapping[str, Any]) -> dict[str, Any]:
        """Rebuild the immediate applied predecessor and the real candidate delta."""
        from .work_unit_completion import candidate_inventory, git_readonly_env

        cls._recovery_check_run_identity_conn(conn, request, require_current=True)
        if (request.get("repair_context_required") is not True
            or request["attempt"] <= 1 or request.get("phase") == "machine_complete"
            or request.get("reason_code") == "machine_complete"):
            raise WorkUnitStoreError("recovery repair context policy invalid")
        predecessors = [record for _, record in cls._recovery_records_for_run_conn(conn, request["run_id"])
                        if record["request"].get("attempt") == request["attempt"] - 1
                        and record.get("status") == "applied"
                        and isinstance(record.get("apply"), Mapping)
                        and record["apply"].get("resulting_attempt") == request["attempt"]]
        if len(predecessors) != 1:
            raise WorkUnitStoreError("recovery repair predecessor missing or ambiguous")
        prior = predecessors[0]
        old, plan, verdict = cls._recovery_validate_plan(prior)
        applied = prior["apply"]
        if (prior.get("request_digest") != digest_json(old)
            or prior.get("request_id") != old.get("request_id")
            or old.get("request_id") != digest_json(cls._recovery_key_identity(old))
            or any(old.get(key) != request.get(key) for key in (
                "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id",
                "authority_digest", "packet_digest", "test_refs", "write_set"))
            or applied.get("state") != "applied"
            or applied.get("action") != plan.get("action")
            or plan.get("action", {}).get("kind") not in {"repair_same_node", "retry_within_budget"}
            or applied.get("request_digest") != prior["request_digest"]
            or applied.get("plan_digest") != plan.get("plan_digest")
            or applied.get("candidate_digest") != old.get("candidate_digest")
            or applied.get("authority_digest") != old.get("authority_digest")):
            raise WorkUnitStoreError("recovery repair predecessor binding invalid")

        def receipt(identity, phase):
            key = digest_json([identity["run_id"], identity["attempt"], identity["fence"], phase])
            row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (key,)).fetchone()
            if row is None or row["state"] != "settled":
                raise WorkUnitStoreError("recovery repair settled receipt missing")
            try:
                evidence = json.loads(row["evidence_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise WorkUnitStoreError("recovery repair settled receipt invalid") from exc
            if (not isinstance(evidence, Mapping)
                or evidence.get("receipt_digest") != digest_json({
                    name: value for name, value in evidence.items() if name != "receipt_digest"})
                or any(evidence.get(name) != row[name] for name in (
                    "run_id", "attempt", "fence", "phase", "phase_key", "binding"))
                or any(evidence.get(name) != identity.get(name) for name in (
                    "run_id", "attempt", "fence", "candidate_digest"))
                or evidence.get("phase") != phase):
                raise WorkUnitStoreError("recovery repair settled receipt binding invalid")
            return evidence

        before, after = receipt(old, "candidate"), receipt(request, "candidate")
        old_red, current_red = receipt(old, old["phase"]), receipt(request, request["phase"])
        if (old_red.get("verdict") != "RED" or current_red.get("verdict") != "RED"
            or digest_json(old_red) != old.get("input_evidence_digest")
            or digest_json(current_red) != request.get("input_evidence_digest")
            or applied.get("retry_evidence") != old_red):
            raise WorkUnitStoreError("recovery repair RED lineage invalid")
        attempts = []
        for identity in (old, request):
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id=? AND ordinal=?",
                                   (identity["run_id"], identity["attempt"])).fetchone()
            if attempt is None or attempt["fence"] != identity["fence"]:
                raise FenceError("recovery repair candidate Attempt/fence invalid")
            attempts.append(attempt)
        workspace = attempts[-1]["workspace_ref"]
        if not isinstance(workspace, str) or not workspace.strip():
            raise WorkUnitStoreError("recovery repair candidate workspace missing")
        root = Path(workspace).expanduser().absolute()
        if any(part.is_symlink() for part in (root, *root.parents)) or not root.is_dir():
            raise WorkUnitStoreError("recovery repair candidate workspace invalid")
        environment = git_readonly_env({key: value for key, value in os.environ.items()
            if key not in {"GIT_INDEX_FILE", "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR",
                           "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"}})
        environment.update(GIT_NO_REPLACE_OBJECTS="1", GIT_NO_LAZY_FETCH="1", GIT_TERMINAL_PROMPT="0")

        def git(*args):
            try:
                return subprocess.check_output(
                    ["git", "--no-replace-objects", "-C", str(root), *args],
                    env=environment, stderr=subprocess.PIPE, timeout=10)
            except (OSError, subprocess.SubprocessError) as exc:
                raise WorkUnitStoreError("recovery repair candidate object or diff unavailable") from exc

        inventory = candidate_inventory(str(root), env=environment)
        if inventory["candidate_digest"] != after["candidate_digest"]:
            raise WorkUnitStoreError("recovery repair candidate drift")
        for candidate in (before, after):
            commit = candidate.get("candidate_commit")
            if (not isinstance(commit, str) or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", commit) is None
                or git("rev-parse", "--verify", commit + "^{commit}").decode().strip() != commit
                or git("rev-parse", commit + "^@").decode().strip() != inventory["head"]):
                raise WorkUnitStoreError("recovery repair candidate object binding invalid")
        patch = git("diff", "--no-ext-diff", "--no-textconv",
                    before["candidate_commit"], after["candidate_commit"], "--")
        try:
            patch_text = patch.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WorkUnitStoreError("recovery repair candidate diff encoding invalid") from exc
        if candidate_inventory(str(root), env=environment)["candidate_digest"] != after["candidate_digest"]:
            raise WorkUnitStoreError("recovery repair candidate drift")
        return {
            "previous_request": old, "previous_result": plan,
            "previous_verdict": verdict, "previous_apply": applied,
            "before_candidate": before, "after_candidate": after,
            "candidate_delta": {"before_commit": before["candidate_commit"],
                                "after_commit": after["candidate_commit"], "patch": patch_text,
                                "patch_digest": "sha256:" + hashlib.sha256(patch).hexdigest()},
        }

    @classmethod
    def _recovery_check_repair_context_conn(cls, conn: sqlite3.Connection,
                                           request: Mapping[str, Any]) -> None:
        if request.get("repair_context_required") is True or "repair_context_ref" in request:
            context = cls._recovery_repair_context_conn(conn, request)
            if request.get("repair_context_ref") != cls.recovery_repair_context_ref(context):
                raise WorkUnitStoreError("recovery repair context binding invalid")

    def recovery_repair_context(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Read-only reconstruction for the original role frame; never backfill a request."""
        with self._connect() as conn:
            conn.execute("BEGIN")
            context = self._recovery_repair_context_conn(conn, request)
            if ("repair_context_ref" in request
                and request["repair_context_ref"] != self.recovery_repair_context_ref(context)):
                raise WorkUnitStoreError("recovery repair context binding invalid")
            return context

    @staticmethod
    def _recovery_audit_required(request: Mapping[str, Any]) -> bool:
        incident = request.get("incident")
        return isinstance(incident, Mapping) and incident.get("failure_count", 0) >= 3

    @classmethod
    def _recovery_validate_audit(cls, record: Mapping[str, Any], audit: Mapping[str, Any]) -> None:
        request = record["request"]
        incident = request.get("incident")
        binding = request.get("audit_binding")
        capabilities = request.get("capability_binding", {})
        verifier = capabilities.get("plan_verifier", {})
        planner = capabilities.get("planner", {})
        metadata = {"audit_digest", "probe", "execution_fence", "provider_input_binding",
                    "capability_binding", "execution_context_digest"}
        if (not cls._recovery_audit_required(request)
            or not isinstance(binding, Mapping) or not isinstance(audit, Mapping)
            or "audit" not in request.get("allowed_actions", [])
            or binding.get("schema") != "lh-planner-recovery-audit-binding/v1"
            or binding.get("provider") != capabilities.get("plan_verifier_provider")
            or binding.get("capability") != "verifier" or binding.get("permissions") != "read_only"
            or audit.get("schema") != "lh-recovery-audit-result/v1" or audit.get("mode") != "audit"
            or audit.get("request_id") != record["request_id"]
            or audit.get("request_digest") != record["request_digest"]
            or audit.get("incident_id") != incident.get("incident_id")
            or audit.get("failure_refs") != incident.get("failure_refs")
            or audit.get("principal") != binding.get("principal")
            or audit.get("principal") == planner.get("identity", {}).get("principal")
            or audit.get("capability_binding") != verifier
            or audit.get("read_only") is not True or audit.get("source_write") is not False
            or audit.get("verdict") != "GREEN"
            or audit.get("audit_digest") != digest_json({
                key: value for key, value in audit.items() if key not in metadata})
            or not cls._recovery_valid_phase_binding(
                request, audit, role="verifier", capability_name="verifier", principal=binding.get("principal"))):
            raise WorkUnitStoreError("recovery audit result binding invalid")
        findings = audit.get("findings")
        if (not isinstance(findings, list) or not findings
            or any(not isinstance(item, Mapping)
                   or not isinstance(item.get("hypothesis"), str) or not item["hypothesis"].strip()
                   or item.get("write_set") != request.get("write_set")
                   or item.get("test_refs") != request.get("test_refs") for item in findings)):
            raise WorkUnitStoreError("recovery audit findings binding invalid")

    @classmethod
    def _recovery_audit_ref(cls, record: Mapping[str, Any], original=None) -> dict[str, Any]:
        if record["request"].get("environment_event_ref") is not None:
            if original is None:
                raise WorkUnitStoreError("recovery_environment_audit_ref_mismatch")
            cls._recovery_audit_ref(original)
            ref = {k: original["audit_result"][k] for k in ("request_id", "request_digest", "audit_digest")}
            if ref != record["request"].get("audit_ref"):
                raise WorkUnitStoreError("recovery_environment_audit_ref_mismatch")
            return ref
        audit = record.get("audit_result")
        cls._recovery_validate_audit(record, audit)
        claims = [claim for claim in record.get("claims", []) if claim.get("phase") == "audit"]
        if len(claims) != 1 or claims[0].get("state") != "success":
            raise WorkUnitStoreError("recovery audit successful claim missing")
        return {"incident_id": audit["incident_id"], "request_id": record["request_id"],
                "audit_digest": audit["audit_digest"]}

    @classmethod
    def _recovery_validate_plan(cls, record: Mapping[str, Any], *, audit_record=None):
        return validate_recovery_plan(record, audit_record=audit_record)

    @staticmethod
    def _recovery_claim_result(record: Mapping[str, Any], *, claimed: bool, reason: str | None = None,
                               waiting: bool = False) -> dict[str, Any]:
        value = dict(record)
        result = {"claimed": claimed, "record": value, **value}
        if reason is not None:
            result["reason"] = reason
        if waiting:
            result["waiting"] = True
        return result

    def get_trusted_execution_budget(self, goal_id, *, goal_revision):
        with self._connect() as conn:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='trusted_execution_budgets'").fetchone():
                return None
            row = conn.execute("SELECT budget_json FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                               (goal_id, goal_revision)).fetchone()
            return json.loads(row[0]) if row else None

    def reserve_trusted_launch(self, *, goal_id, goal_revision, policy, phase_key,
                               run_id, attempt, fence, phase, command_digest,
                               is_provider_cli, now=None, execution_binding_digest=None,
                               continuation_authority_digest=None):
        """Atomically reserve a Goal-wide launch; an unresolved claim is never refunded."""
        if __package__:
            from .execution_fence_trusted import validate_trusted_project_policy
        else:
            from execution_fence_trusted import validate_trusted_project_policy
        moment = self._now() if now is None else now
        if isinstance(moment, bool) or not isinstance(moment, (int, float)) or not math.isfinite(moment):
            raise WorkUnitStoreError("trusted_budget_time_invalid")
        policy = validate_trusted_project_policy(policy, now=moment)
        policy_digest = digest_json(policy)
        if policy["goal_id"] != goal_id or policy["goal_revision"] != goal_revision:
            raise WorkUnitStoreError("trusted_budget_goal_mismatch")
        if (any(isinstance(value, bool) or not isinstance(value, int) or value < 1
                for value in (goal_revision, attempt, fence)) or not isinstance(is_provider_cli, bool)
                or not isinstance(phase, str) or not re.fullmatch(r"[a-z][a-z0-9_:-]{0,127}", phase)
                or any(not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
                       for value in (phase_key, command_digest))):
            raise WorkUnitStoreError("trusted_budget_reservation_invalid")
        if execution_binding_digest is not None and not re.fullmatch(r"sha256:[0-9a-f]{64}", execution_binding_digest):
            raise WorkUnitStoreError("trusted_execution_binding_invalid")
        key = digest_json([goal_id, goal_revision, phase_key])
        fields = {"goal_id": goal_id, "goal_revision": goal_revision, "policy_digest": policy_digest,
            "phase_key": phase_key, "run_id": run_id, "attempt": attempt, "fence": fence,
            "phase": phase, "command_digest": command_digest, "is_provider_cli": is_provider_cli}
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if continuation_authority_digest is not None:
                result = trusted_continuation.reserve(conn, authority_digest=continuation_authority_digest,
                    fields=fields, policy=policy, execution_binding_digest=execution_binding_digest, now=moment,
                    identity_port=self.identity_port)
                conn.execute("COMMIT")
                return result
            row = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                               (goal_id, goal_revision)).fetchone()
            budget = json.loads(row["budget_json"]) if row else None
            if budget is not None:
                if budget["policy_digest"] != policy_digest:
                    raise WorkUnitStoreError("trusted_budget_policy_mismatch")
                if (execution_binding_digest is not None and row["execution_binding_digest"] is not None
                        and row["execution_binding_digest"] != execution_binding_digest):
                    raise WorkUnitStoreError("trusted_execution_binding_mismatch")
                for reservation in budget["reservations"]:
                    if reservation["reservation_key"] == key:
                        if any(reservation[name] != value for name, value in fields.items()):
                            raise WorkUnitStoreError("trusted_budget_reservation_conflict")
                        return {name: value for name, value in reservation.items()
                                if name not in {"observation", "observation_digest"}} | {"claimed": False}
            else:
                # Never reinterpret a historical executor's zero model-call
                # projection as proof of zero consumption under a fresh policy.
                for old in conn.execute("SELECT receipt_json FROM dispatch_consumptions WHERE goal_id=? AND goal_revision=?",
                                        (goal_id, goal_revision)):
                    receipt = json.loads(old[0])
                    if (receipt.get("executor_receipt") or receipt.get("executor_failure_receipts")
                            or receipt.get("executor_launches", 0) or receipt.get("executor_invocations", 0)
                            or receipt.get("executor_status") in {"accepted", "unknown", "exhausted"}):
                        raise WorkUnitStoreError("trusted_budget_legacy_consumption_unbound")
                prior = conn.execute("SELECT a.state FROM attempts a JOIN runs r ON r.run_id=a.run_id "
                    "JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id "
                    "WHERE p.goal_id=? AND p.goal_revision=?", (goal_id, goal_revision))
                if any(old[0] != "ready" for old in prior):
                    raise WorkUnitStoreError("trusted_budget_legacy_consumption_unbound")
                budget = {"schema": "lh-trusted-execution-budget/v1", "goal_id": goal_id,
                    "goal_revision": goal_revision, "policy_digest": policy_digest,
                    "max_cli_launches": policy["max_cli_launches"], "max_wall_seconds": policy["max_wall_seconds"],
                    "max_observed_tokens": policy["max_observed_tokens"], "reserved_cli_launches": 0,
                    "observed_total_tokens": 0, "deadline_at": min(moment + policy["max_wall_seconds"], policy["expires_at"]),
                    "blocked": False, "blocked_reason": None, "reservations": []}
            actual = conn.execute("SELECT r.state AS run_state,r.attempts,r.fence AS current_fence,a.state AS attempt_state,"
                "a.fence,p.goal_id,p.goal_revision FROM runs r JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id "
                "JOIN attempts a ON a.run_id=r.run_id AND a.ordinal=? WHERE r.run_id=?", (attempt, run_id)).fetchone()
            if (actual is None or actual["goal_id"] != goal_id or actual["goal_revision"] != goal_revision
                    or actual["attempts"] != attempt or actual["current_fence"] != fence or actual["fence"] != fence):
                raise WorkUnitStoreError("trusted_budget_attempt_mismatch")
            queued = phase in {"coding", "admission"} and actual["run_state"] in {"queued", "ready"} and actual["attempt_state"] == "ready"
            running = actual["run_state"] in {"running", "verified"} and actual["attempt_state"] in {"running", "verified"}
            if not queued and not running:
                raise WorkUnitStoreError("trusted_budget_phase_state_invalid")
            if moment >= budget["deadline_at"]:
                raise WorkUnitStoreError("trusted_budget_deadline_exhausted")
            if is_provider_cli:
                if budget["blocked"]:
                    raise WorkUnitStoreError("trusted_budget_goal_blocked:" + budget["blocked_reason"])
                if budget["reserved_cli_launches"] >= budget["max_cli_launches"]:
                    raise WorkUnitStoreError("trusted_budget_launches_exhausted")
                if budget["max_observed_tokens"] is not None and budget["observed_total_tokens"] >= budget["max_observed_tokens"]:
                    raise WorkUnitStoreError("trusted_budget_observed_tokens_exhausted")
                budget["reserved_cli_launches"] += 1
            reservation = {"schema": "lh-trusted-launch-reservation/v1", "reservation_key": key,
                "state": "reserved", **fields, "reserved_at": moment, "deadline_at": budget["deadline_at"]}
            budget["reservations"].append({**reservation, "observation": None, "observation_digest": None})
            budget["reservations"].sort(key=lambda value: (value["reserved_at"], value["reservation_key"]))
            bound = execution_binding_digest or (row["execution_binding_digest"] if row else None)
            conn.execute("INSERT INTO trusted_execution_budgets VALUES (?,?,?,?) ON CONFLICT(goal_id,goal_revision) "
                "DO UPDATE SET execution_binding_digest=excluded.execution_binding_digest,budget_json=excluded.budget_json",
                (goal_id, goal_revision, bound, json.dumps(budget, sort_keys=True)))
            return {**reservation, "claimed": True}

    def settle_trusted_launch(self, reservation_key, *, observation, now=None):
        moment = self._now() if now is None else now
        if __package__:
            from .cli_agent_executor import validate_trusted_usage
        else:
            from cli_agent_executor import validate_trusted_usage
        fields = {"schema", "outcome", "reason_code", "process_terminated", "elapsed_seconds", "usage"}
        reasons = {"completed", "process_exit_nonzero", "process_timeout", "process_identity_unknown",
            "process_readback_unknown", "provider_protocol_invalid", "provider_failed", "provider_usage_unknown",
            "output_limit_exceeded", "callback_failed", "provider_result_invalid"}
        if (not isinstance(observation, Mapping) or set(observation) != fields
                or observation["schema"] != "lh-trusted-launch-observation/v1"
                or observation["outcome"] not in {"completed", "known_failure", "unknown"}
                or observation["reason_code"] not in reasons or not isinstance(observation["process_terminated"], bool)
                or isinstance(observation["elapsed_seconds"], bool)
                or not isinstance(observation["elapsed_seconds"], (int, float))
                or not math.isfinite(observation["elapsed_seconds"]) or observation["elapsed_seconds"] < 0):
            raise WorkUnitStoreError("trusted_observation_invalid")
        if observation["usage"] is not None:
            validate_trusted_usage(observation["usage"])
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            continued = trusted_continuation.settle(conn, reservation_key, observation, now=moment)
            if continued is not None:
                conn.execute("COMMIT")
                return continued
            for row in conn.execute("SELECT * FROM trusted_execution_budgets"):
                budget = json.loads(row["budget_json"])
                for reservation in budget["reservations"]:
                    if reservation["reservation_key"] != reservation_key:
                        continue
                    if reservation["observation"] is not None:
                        if reservation["observation"] != observation:
                            raise WorkUnitStoreError("trusted_observation_conflict")
                        return reservation
                    if reservation["is_provider_cli"] != (observation["usage"] is not None):
                        raise WorkUnitStoreError("trusted_observation_usage_binding_invalid")
                    blocked_reason = None
                    if observation["outcome"] == "unknown":
                        blocked_reason = "process_outcome_unknown"
                    elif not observation["process_terminated"]:
                        blocked_reason = "process_termination_unknown"
                    elif reservation["is_provider_cli"] and observation["usage"]["state"] == "unknown":
                        blocked_reason = "provider_usage_unknown"
                    if blocked_reason:
                        budget.update(blocked=True, blocked_reason=budget["blocked_reason"] or blocked_reason)
                    usage = observation["usage"]
                    if usage is not None and usage["state"] == "observed":
                        budget["observed_total_tokens"] += usage["total_tokens"]
                    reservation.update(state="unknown" if blocked_reason else "settled",
                        observation=copy.deepcopy(dict(observation)), observation_digest=digest_json(observation))
                    conn.execute("UPDATE trusted_execution_budgets SET budget_json=? WHERE goal_id=? AND goal_revision=?",
                                 (json.dumps(budget, sort_keys=True), row["goal_id"], row["goal_revision"]))
                    return reservation
            raise WorkUnitStoreError("trusted_reservation_missing")

    def record_continuation_authority(self, authority, *, approved_operation_digest, now=None):
        """Append one separately approved window; never mutate an old budget."""
        if self.read_only:
            raise WorkUnitStoreError("continuation_write_forbidden")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = trusted_continuation.record(conn, authority, approved_operation_digest,
                now=self._now() if now is None else now, identity_port=self.identity_port)
            conn.execute("COMMIT")
            return result

    @staticmethod
    def _applied_window_rows_conn(conn, goal_id, goal_revision):
        """Canonical rows used to prove that an applied window stayed inert."""
        specs = (
            ("parent_goals", "SELECT * FROM parent_goals WHERE goal_id=? AND goal_revision=?"),
            ("work_units", "SELECT w.* FROM work_units w JOIN parent_goals p ON p.parent_goal_id=w.parent_goal_id "
             "WHERE p.goal_id=? AND p.goal_revision=?"),
            ("runs", "SELECT r.* FROM runs r JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id "
             "WHERE p.goal_id=? AND p.goal_revision=?"),
            ("attempts", "SELECT a.* FROM attempts a JOIN runs r ON r.run_id=a.run_id "
             "JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id WHERE p.goal_id=? AND p.goal_revision=?"),
            ("leases", "SELECT l.* FROM leases l JOIN work_units w ON w.work_unit_id=l.work_unit_id "
             "JOIN parent_goals p ON p.parent_goal_id=w.parent_goal_id WHERE p.goal_id=? AND p.goal_revision=?"),
            ("events", "SELECT e.* FROM events e JOIN parent_goals p ON p.parent_goal_id=e.parent_goal_id "
             "WHERE p.goal_id=? AND p.goal_revision=?"),
            ("dispatch_consumptions", "SELECT * FROM dispatch_consumptions WHERE goal_id=? AND goal_revision=?"),
            ("completion_phases", "SELECT c.* FROM completion_phases c JOIN runs r ON r.run_id=c.run_id "
             "JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id WHERE p.goal_id=? AND p.goal_revision=?"),
            ("delivery_bindings", "SELECT d.* FROM delivery_bindings d JOIN work_units w ON w.work_unit_id=d.work_unit_id "
             "JOIN parent_goals p ON p.parent_goal_id=w.parent_goal_id WHERE p.goal_id=? AND p.goal_revision=?"),
            ("planning_requests", "SELECT q.* FROM planning_requests q JOIN work_units w ON w.work_unit_id=q.work_unit_id "
             "JOIN parent_goals p ON p.parent_goal_id=w.parent_goal_id WHERE p.goal_id=? AND p.goal_revision=?"),
            ("candidate_recovery_admissions", "SELECT * FROM candidate_recovery_admissions "
             "WHERE dispatch_key IN (SELECT dispatch_key FROM dispatch_consumptions WHERE goal_id=? AND goal_revision=?)"),
            ("trusted_execution_budgets", "SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?"),
            ("trusted_continuation_authorities", "SELECT * FROM trusted_continuation_authorities "
             "WHERE goal_id=? AND goal_revision=?"),
            ("trusted_continuation_reservations", "SELECT * FROM trusted_continuation_reservations "
             "WHERE goal_id=? AND goal_revision=?"),
            ("trusted_continuation_observations", "SELECT o.* FROM trusted_continuation_observations o "
             "JOIN trusted_continuation_reservations r ON r.reservation_key=o.reservation_key "
             "WHERE r.goal_id=? AND r.goal_revision=?"),
            ("trusted_continuation_repairs", "SELECT x.* FROM trusted_continuation_repairs x "
             "JOIN trusted_continuation_authorities a ON a.authority_digest=x.authority_digest "
             "WHERE a.goal_id=? AND a.goal_revision=?"),
            ("trusted_continuation_successors", "SELECT * FROM trusted_continuation_successors "
             "WHERE goal_id=? AND goal_revision=?"),
        )
        rows = {}
        for table, query in specs:
            if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone() is None:
                continue
            values = [dict(row) for row in conn.execute(
                query, (goal_id, goal_revision))]
            rows[table] = sorted(
                values,
                key=lambda value: json.dumps(
                    value, sort_keys=True, separators=(",", ":"),
                ),
            )
        return rows

    @classmethod
    def _applied_window_preconditions_conn(cls, conn, goal_id, goal_revision):
        rows = cls._applied_window_rows_conn(conn, goal_id, goal_revision)
        body = {
            "schema": "lh-applied-continuation-window-preconditions/v1",
            "goal_id": goal_id,
            "goal_revision": goal_revision,
            "rows": rows,
        }
        return {**body, "digest": digest_json(body)}

    @staticmethod
    def _applied_window_preserved_lease(predecessor, rows, lease):
        """Recognize only the unchanged lease created by R2 admission."""
        admissions = [
            row for row in rows.get("candidate_recovery_admissions", [])
            if row["work_unit_id"] == lease["work_unit_id"]
        ]
        if len(admissions) != 1:
            return False
        saved = admissions[0]
        try:
            admission = json.loads(saved["admission_json"])
        except (TypeError, ValueError):
            return False
        created_at = saved["created_at"]
        if (saved["status"] != "admitted"
                or not isinstance(admission, dict)
                or admission.get("status") != "admitted"
                or admission.get("kind") != "preserved_result_recovery"
                or isinstance(created_at, bool)
                or not isinstance(created_at, (int, float))
                or not math.isfinite(created_at)):
            return False
        scopes = [
            scope for scope in predecessor.get("scope", [])
            if isinstance(scope, dict)
            and scope.get("dispatch_key") == saved["dispatch_key"]
        ]
        dispatches = [
            row for row in rows.get("dispatch_consumptions", [])
            if row["dispatch_key"] == saved["dispatch_key"]
        ]
        runs = [
            row for row in rows.get("runs", [])
            if row["run_id"] == saved["run_id"]
        ]
        attempts = [
            row for row in rows.get("attempts", [])
            if row["run_id"] == saved["run_id"]
            and row["ordinal"] == saved["attempt"]
        ]
        if any(len(matches) != 1 for matches in (scopes, dispatches, runs, attempts)):
            return False
        scope, dispatch, run, attempt = scopes[0], dispatches[0], runs[0], attempts[0]
        return (
            scope.get("allow_coding") is False
            and all(admission.get(key) == saved[key] for key in
                    ("dispatch_key", "work_unit_id", "run_id", "attempt", "fence"))
            and all(scope.get(key) == saved[key] for key in
                    ("dispatch_key", "run_id", "attempt", "fence"))
            and all(dispatch[key] == saved[key] for key in
                    ("dispatch_key", "work_unit_id", "run_id", "attempt"))
            and dispatch["envelope_digest"] == scope.get("envelope_digest")
            and run["work_unit_id"] == saved["work_unit_id"]
            and run["attempts"] == saved["attempt"]
            and run["fence"] == saved["fence"]
            and attempt["fence"] == saved["fence"]
            and lease == {
                "work_unit_id": saved["work_unit_id"],
                "holder": attempt["holder"],
                "fence": saved["fence"],
                "expires_at": created_at + 86400.0,
            }
        )

    def get_applied_continuation_window_preconditions(
        self, predecessor_authority_digest, *, dispatch_keys=()
    ):
        """Capture an inert applied-R3 window before an approval is accepted."""
        with self._connect() as conn:
            conn.execute("BEGIN")
            predecessor = trusted_continuation.get(conn, predecessor_authority_digest)
            if predecessor is None:
                raise WorkUnitStoreError("applied_window_predecessor_missing")
            goal_id, goal_revision = predecessor["goal_id"], predecessor["goal_revision"]
            keys = sorted(dispatch_keys)
            if any(not isinstance(key, str) or not key for key in keys):
                raise WorkUnitStoreError("applied_window_dispatch_keys_invalid")
            rows = self._applied_window_rows_conn(conn, goal_id, goal_revision)
            if (rows.get("trusted_continuation_reservations")
                    or any(not self._applied_window_preserved_lease(predecessor, rows, lease)
                           for lease in rows.get("leases", []))):
                raise WorkUnitStoreError("applied_window_effect_already_started")
            if rows.get("completion_phases"):
                raise WorkUnitStoreError("applied_window_partial_effect")
            if keys:
                dispatched = {
                    row["dispatch_key"]
                    for row in rows.get("dispatch_consumptions", [])
                }
                if dispatched.intersection(keys):
                    raise WorkUnitStoreError("applied_window_dispatch_already_started")
            return self._applied_window_preconditions_conn(
                conn, goal_id, goal_revision)

    def record_applied_continuation_window(
        self, authority, *, approved_operation_digest,
        predecessor_authority_digest, operation_ref, preconditions, now=None,
    ):
        """Atomically append exactly one applied-window authority successor."""
        if self.read_only:
            raise WorkUnitStoreError("continuation_write_forbidden")
        if not isinstance(preconditions, Mapping):
            raise WorkUnitStoreError("applied_window_preconditions_invalid")
        body = {
            key: preconditions.get(key)
            for key in ("schema", "goal_id", "goal_revision", "rows")
        }
        if (preconditions.get("digest") != digest_json(body)
                or preconditions.get("schema")
                != "lh-applied-continuation-window-preconditions/v1"):
            raise WorkUnitStoreError("applied_window_preconditions_seal")
        if (not isinstance(operation_ref, Mapping)
                or set(operation_ref) != {"path", "sha256", "operation_digest"}
                or not all(isinstance(operation_ref.get(key), str)
                            and operation_ref.get(key)
                            for key in ("path", "sha256", "operation_digest"))
                or not trusted_continuation._digest(
                    operation_ref.get("operation_digest"))
                or operation_ref.get("operation_digest")
                != approved_operation_digest):
            raise WorkUnitStoreError("applied_window_operation_ref_invalid")
        moment = self._now() if now is None else now
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            operation = trusted_continuation._read_reference(
                {"path": operation_ref["path"],
                 "sha256": operation_ref["sha256"]},
                "applied_window_operation")
            if (not isinstance(operation, dict)
                    or operation.get("operation_digest")
                    != approved_operation_digest
                    or operation.get("operation_digest")
                    != digest_json({
                        key: value for key, value in operation.items()
                        if key != "operation_digest"
                    })
                    or operation.get("continuation_authority") != authority
                    or operation.get("predecessor_authority_digest")
                    != predecessor_authority_digest
                    or operation.get("preconditions") != preconditions):
                raise WorkUnitStoreError(
                    "applied_window_operation_binding_invalid")
            saved = trusted_continuation.get(conn, authority["authority_digest"])
            if saved is not None:
                relation = conn.execute(
                    "SELECT * FROM trusted_continuation_successors "
                    "WHERE successor_digest=?",
                    (authority["authority_digest"],),
                ).fetchone()
                if (saved != authority or relation is None
                        or relation["predecessor_digest"]
                        != predecessor_authority_digest
                        or json.loads(relation["operation_ref_json"])
                        != dict(operation_ref)
                        or operation.get("continuation_authority") != saved):
                    raise WorkUnitStoreError("applied_window_successor_conflict")
                conn.execute("COMMIT")
                return {"authority": saved, "created": False}
            predecessor = trusted_continuation.get(
                conn, predecessor_authority_digest)
            if (predecessor is None
                    or predecessor["goal_id"] != authority.get("goal_id")
                    or predecessor["goal_revision"] != authority.get("goal_revision")):
                raise WorkUnitStoreError("applied_window_predecessor_invalid")
            prior_successor = conn.execute(
                "SELECT successor_digest FROM trusted_continuation_successors "
                "WHERE predecessor_digest=?",
                (predecessor_authority_digest,),
            ).fetchone() if trusted_continuation._table(
                conn, "trusted_continuation_successors") else None
            if prior_successor is not None:
                raise WorkUnitStoreError("applied_window_successor_conflict")
            current = self._applied_window_preconditions_conn(
                conn, authority["goal_id"], authority["goal_revision"])
            if current != dict(preconditions):
                raise WorkUnitStoreError("applied_window_preconditions_changed")
            result = trusted_continuation.record(
                conn, authority, authority["authority_digest"], now=moment,
                identity_port=self.identity_port,
                predecessor_authority_digest=predecessor_authority_digest,
                successor_operation_ref=dict(operation_ref),
            )
            conn.execute("COMMIT")
            return {"authority": result, "created": True}

    def get_continuation_authority(self, authority_digest):
        with self._connect() as conn:
            return trusted_continuation.get(conn, authority_digest)

    def validate_continuation_authority(self, authority, *, approved_operation_digest, now=None):
        with self._connect() as conn:
            conn.execute("BEGIN")
            return trusted_continuation.record(conn, authority, approved_operation_digest,
                now=self._now() if now is None else now, validate_only=True, identity_port=self.identity_port)

    def get_continuation_repair(self, dispatch_key):
        """Read the append-only A1 lineage; absent tables are never migrated."""
        with self._connect() as conn:
            return trusted_continuation.get_repair(conn, dispatch_key)

    def prepare_continuation_repair(self, dispatch_key, *, authority_digest, attempt, fence, evidence):
        with self._connect() as conn:
            conn.execute("BEGIN")
            return trusted_continuation.repair_plan(conn, authority_digest, dispatch_key,
                attempt=attempt, fence=fence, evidence=evidence, now=self._now(), identity_port=self.identity_port)

    def continuation_accounting(self, authority_digest, *, require_available=False):
        with self._connect() as conn:
            authority = trusted_continuation.get(conn, authority_digest)
            if authority is None:
                raise WorkUnitStoreError("continuation_authority_missing")
            return trusted_continuation.accounting(conn, authority, now=self._now(), require_available=require_available)

    @staticmethod
    def _admission_context_conn(conn, run, context, *, run_id, attempt, fence):
        """Check one real dispatch/current ready Attempt while holding the lock."""
        fields = {"dispatch_key", "goal_id", "goal_revision", "node_id", "work_unit_id",
                  "run_id", "attempt", "fence", "worktree", "base_sha"}
        if not isinstance(context, Mapping) or set(context) != fields:
            raise FenceError("completion admission context invalid")
        for name in fields - {"goal_revision", "attempt", "fence"}:
            _required_text("admission_" + name, context[name])
        if any(isinstance(context[name], bool) or not isinstance(context[name], int)
               or context[name] < 1 for name in ("goal_revision", "attempt", "fence")):
            raise FenceError("completion admission identity invalid")
        dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?",
                                (context["dispatch_key"],)).fetchone()
        current = conn.execute("SELECT * FROM attempts WHERE run_id=? AND ordinal=?",
                               (run_id, attempt)).fetchone()
        work = conn.execute("SELECT * FROM work_units WHERE work_unit_id=?",
                            (context["work_unit_id"],)).fetchone()
        if dispatch is None or current is None or work is None:
            raise FenceError("completion admission authority missing")
        expected = {"run_id": run_id, "attempt": attempt, "fence": fence,
                    "work_unit_id": run["work_unit_id"], "base_sha": run["base_sha"],
                    "goal_id": dispatch["goal_id"], "goal_revision": int(dispatch["goal_revision"]),
                    "node_id": dispatch["node_id"],
                    "worktree": str(Path(current["workspace_ref"]).expanduser().resolve())}
        if any(context.get(name) != value for name, value in expected.items()):
            raise FenceError("completion admission authority mismatch")
        envelope = json.loads(dispatch["envelope_json"])
        if (dispatch["run_id"] != run_id or dispatch["attempt"] != attempt
            or dispatch["work_unit_id"] != work["work_unit_id"]
            or work["run_id"] != run_id or work["node_id"] != dispatch["node_id"]
            or work["base_sha"] != run["base_sha"] or envelope.get("wave_base_sha") != run["base_sha"]
            or run["parent_goal_id"] != dispatch["parent_goal_id"]
            or work["parent_goal_id"] != run["parent_goal_id"]
            or run["attempts"] != attempt or run["fence"] != fence or current["fence"] != fence
            or run["state"] != "queued" or current["state"] != "ready"):
            raise FenceError("completion admission current attempt mismatch")
        return dict(context)

    def claim_completion_phase(self, *, run_id: str, attempt: int, fence: int,
                               phase: str, binding: str, repair_id: str | None = None,
                               predecessor_receipt_digest: str | None = None,
                               recovery_admission_digest: str | None = None,
                               admission_context: Mapping[str, Any] | None = None,
                               environment_request_id: str | None = None) -> dict[str, Any]:
        """Reserve an effect before executing it; an unfinished claim is unknown."""
        key = digest_json([run_id, attempt, fence, phase])
        successor = repair_id is not None or predecessor_receipt_digest is not None
        if successor:
            if (phase not in {"checks_repair", "integration_checks", "delivery_verifier", "checks"} or not isinstance(repair_id, str) or not repair_id.strip()
                or not isinstance(predecessor_receipt_digest, str) or not predecessor_receipt_digest):
                raise FenceError("completion repair successor identity invalid")
            if phase == "checks" and (not environment_request_id or repair_id != predecessor_receipt_digest):
                raise FenceError("recovery_environment_successor_identity_invalid")
            key = digest_json([run_id, attempt, fence, phase, repair_id])
        if environment_request_id is not None and not (successor and phase == "checks"):
            raise FenceError("recovery_environment_successor_identity_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if run is None or run["fence"] != fence or run["attempts"] != attempt:
                raise FenceError("completion stale attempt")
            admission_claim = None
            if admission_context is not None:
                if phase != "admission" or successor or recovery_admission_digest is not None:
                    raise FenceError("completion admission phase invalid")
                admission_claim = self._admission_context_conn(conn, run, admission_context,
                    run_id=run_id, attempt=attempt, fence=fence)
            if phase in {"integration_checks", "delivery_verifier"} and successor:
                recovery_key = digest_json([run_id, attempt, fence, "delivery_recovery"]
                    + ([repair_id] if phase == "delivery_verifier" else []))
                recovery_row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (recovery_key,)).fetchone()
                admission = json.loads(recovery_row["evidence_json"]) if recovery_row else {}
                if (not recovery_row or recovery_row["state"] != "settled"
                    or admission.get("input_digest") != recovery_admission_digest
                    or recovery_row["binding"] != recovery_admission_digest
                    or digest_json({k: v for k, v in admission.items() if k != "input_digest"}) != recovery_admission_digest
                    or admission.get("schema") != "host-p7-delivery-recovery/v1"
                    or admission.get("resume_phase", "integration_checks") != phase
                    or admission.get("repair_id") != repair_id
                    or admission.get("predecessor_receipt_digest") != predecessor_receipt_digest
                    or admission.get("successor_phase_binding") != binding):
                    raise FenceError("delivery_recovery_successor_admission_invalid")
            elif recovery_admission_digest is not None:
                raise FenceError("delivery_recovery_successor_phase_invalid")
            if environment_request_id is not None:
                record = self._recovery_record_from_row(self._recovery_record_row_conn(conn, environment_request_id))
                if record is None or record.get("status") != "applied":
                    raise FenceError("recovery_environment_apply_missing")
                original = self._recovery_environment_audit_conn(conn, record)
                req, plan, _ = self._recovery_validate_plan(record, audit_record=original)
                if (req["run_id"] != run_id or req["attempt"] != attempt or req["fence"] != fence
                    or req["repair_packet"]["receipt_digest"] != predecessor_receipt_digest
                    or plan.get("action") != {"kind": "resume_phase"} or plan.get("target_phase") != "checks"
                    or original.get("environment_successor_request_id") != environment_request_id
                    or record.get("apply", {}).get("plan_digest") != plan["plan_digest"]):
                    raise FenceError("recovery_environment_apply_invalid")
            row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (key,)).fetchone()
            if row:
                if row["binding"] != binding:
                    raise FenceError("completion candidate changed")
                if successor:
                    saved = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
                    saved = saved.get("repair_claim", saved)
                    if (saved.get("repair_id") != repair_id
                        or (phase in {"integration_checks", "delivery_verifier"} and saved.get("recovery_admission_digest") != recovery_admission_digest)
                        or saved.get("predecessor_receipt_digest") != predecessor_receipt_digest):
                        raise FenceError("completion repair replay identity changed")
                return {"claimed": False, "key": key, "state": row["state"],
                        "evidence": json.loads(row["evidence_json"]) if row["evidence_json"] else None}
            if run["state"] != "running" and admission_claim is None:
                raise FenceError("completion terminal effect rejected")
            if environment_request_id is not None:
                records = [r for _, r in self._recovery_records_for_run_conn(conn, run_id)]
                claims = [c for r in records for c in r["claims"]]
                if any(c["state"] in {"claimed", "outcome_unknown"} for c in claims):
                    raise FenceError("recovery Run has unresolved claim")
                deadlines = [c["incident_deadline_at"] for c in claims
                             if isinstance(c.get("incident_deadline_at"), (int, float))]
                if deadlines and self._now() >= min(deadlines):
                    raise FenceError("recovery incident time budget exhausted")
                for name, kinds in (("planner_calls", {"planner"}), ("plan_verifier_calls", {"audit", "plan_verifier"})):
                    if sum(c["phase"] in kinds for c in claims) > record["budget_limits"][name]:
                        raise FenceError("recovery " + name + " budget exceeded")
            metadata = json.dumps({"admission_context": admission_claim}, sort_keys=True) if admission_claim is not None else None
            if successor:
                if conn.execute("SELECT 1 FROM completion_phases WHERE run_id=? AND attempt=? AND fence=? AND state!='settled'",
                                (run_id, attempt, fence)).fetchone():
                    raise FenceError("completion repair has unknown effect")
                rows = conn.execute(
                    "SELECT * FROM completion_phases WHERE run_id=? AND attempt=? AND fence=? AND phase=?",
                    (run_id, attempt, fence, phase)).fetchall()
                predecessor = None
                for existing in rows:
                    evidence = json.loads(existing["evidence_json"]) if existing["evidence_json"] else {}
                    if existing["state"] != "settled":
                        raise FenceError("completion repair predecessor has unknown effect")
                    if (evidence.get("receipt_digest") != digest_json(
                            {k: v for k, v in evidence.items() if k != "receipt_digest"})
                        or evidence.get("phase_key") != existing["phase_key"]
                        or evidence.get("binding") != existing["binding"]
                        or evidence.get("run_id") != run_id or evidence.get("attempt") != attempt
                        or evidence.get("fence") != fence or evidence.get("phase") != phase):
                        raise FenceError("completion repair predecessor seal invalid")
                    if evidence.get("verdict") != "RED":
                        raise FenceError("completion repair predecessor is not RED")
                    if evidence.get("predecessor_receipt_digest") == predecessor_receipt_digest:
                        raise FenceError("completion repair predecessor already consumed")
                    if evidence.get("receipt_digest") == predecessor_receipt_digest:
                        predecessor = evidence
                if predecessor is None:
                    raise FenceError("completion repair predecessor missing")
                metadata = json.dumps({"repair_claim": {"repair_id": repair_id,
                    "predecessor_receipt_digest": predecessor_receipt_digest,
                    **({"environment_request_id": environment_request_id} if environment_request_id is not None else {}),
                    **({"recovery_admission_digest": recovery_admission_digest}
                       if phase in {"integration_checks", "delivery_verifier"} else {})}}, sort_keys=True)
            if phase == "integration" and self._delivery_dependencies_conn(conn, run):
                raise WorkUnitStoreError("delivery_dependencies_not_integrated")
            conn.execute("INSERT INTO completion_phases VALUES(?,?,?,?,?,?, 'claimed', ?)",
                         (key, run_id, attempt, fence, phase, binding, metadata))
            conn.execute("COMMIT")
        return {"claimed": True, "key": key, "state": "claimed"}

    def admit_integration_recovery(self, admission: dict[str, Any], *, phase_snapshot: list[dict],
                                   dispatch: dict, candidate_admission: dict) -> dict:
        """Atomically append an approved recovery, never replace existing evidence."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_integration_recovery")
        run_id, attempt, fence = (admission[k] for k in ("run_id", "attempt", "fence"))
        supplied = admission.get("input_digest")
        if supplied != digest_json({k: v for k, v in admission.items() if k != "input_digest"}):
            raise WorkUnitStoreError("integration_recovery_seal")
        key = digest_json([run_id, attempt, fence, "integration_recovery"])
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if (not run or run["fence"] != fence or run["attempts"] != attempt
                or run["state"] not in {"running", "integrated"}):
                raise FenceError("integration_recovery_ownership_changed")
            rows = [dict(row) for row in conn.execute(
                "SELECT * FROM completion_phases WHERE run_id=? AND attempt=? AND fence=? ORDER BY phase_key",
                (run_id, attempt, fence))]
            if rows != phase_snapshot:
                raise FenceError("integration_recovery_phases_changed")
            # Read immutable admissions again while the write lock is held,
            # including on replay. These getters execute SELECTs only.
            if (self.get_dispatch_consumption(admission["dispatch_key"]) != dispatch
                or self.get_candidate_recovery_admission(admission["dispatch_key"]) != candidate_admission):
                raise FenceError("integration_recovery_admission_changed")
            saved = next((row for row in rows if row["phase_key"] == key), None)
            if saved:
                if (saved["state"] != "settled" or saved["binding"] != supplied
                    or json.loads(saved["evidence_json"]) != admission
                    or any(row["state"] != "settled" for row in rows)):
                    raise FenceError("integration_recovery_replay_conflict")
                return {"created": False, "phase_key": key}
            if any(row["state"] != "settled"
                    or row["phase"] not in {"candidate", "checks", "checks_repair"} for row in rows):
                raise FenceError("integration_recovery_phases_changed")
            conn.execute("INSERT INTO completion_phases VALUES(?,?,?,?,?,?,'settled',?)",
                         (key, run_id, attempt, fence, "integration_recovery", supplied,
                          json.dumps(admission, sort_keys=True)))
            conn.execute("COMMIT")
        return {"created": True, "phase_key": key}

    def admit_delivery_recovery(self, admission: dict[str, Any], *, phase_snapshot: list[dict],
                                dispatch: dict, candidate_admission: dict) -> dict:
        """Append one exact settled-RED continuation under the existing fence."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_delivery_recovery")
        run_id, attempt, fence = (admission[k] for k in ("run_id", "attempt", "fence"))
        supplied = admission.get("input_digest")
        if (admission.get("schema") != "host-p7-delivery-recovery/v1"
            or supplied != digest_json({k: v for k, v in admission.items() if k != "input_digest"})):
            raise WorkUnitStoreError("delivery_recovery_seal")
        resume_phase = admission.get("resume_phase", "integration_checks")
        if resume_phase not in {"integration_checks", "delivery_verifier"}:
            raise WorkUnitStoreError("delivery_recovery_phase")
        final = resume_phase == "delivery_verifier"
        key = digest_json([run_id, attempt, fence, "delivery_recovery"]
                          + ([admission["repair_id"]] if final else []))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if (not run or run["fence"] != fence or run["attempts"] != attempt
                or run["state"] not in {"running", "integrated"}):
                raise FenceError("delivery_recovery_ownership_changed")
            rows = [dict(row) for row in conn.execute(
                "SELECT * FROM completion_phases WHERE run_id=? AND attempt=? AND fence=? ORDER BY phase_key",
                (run_id, attempt, fence))]
            if (rows != phase_snapshot or any(row["state"] != "settled" for row in rows)
                or self.get_dispatch_consumption(admission["dispatch_key"]) != dispatch
                or self.get_candidate_recovery_admission(admission["dispatch_key"]) != candidate_admission):
                raise FenceError("delivery_recovery_snapshot_changed")
            saved = next((row for row in rows if row["phase_key"] == key), None)
            if saved:
                if saved["binding"] != supplied or json.loads(saved["evidence_json"]) != admission:
                    raise FenceError("delivery_recovery_replay_conflict")
                return {"created": False, "phase_key": key}
            allowed = {"candidate", "checks", "checks_repair", "integration_recovery",
                       "verifier", "integration", "integration_checks"}
            if final:
                allowed |= {"delivery_recovery", "integration_verifier", "delivery_verifier"}
                original_key = digest_json([run_id, attempt, fence, "delivery_recovery"])
                original = next((row for row in rows if row["phase_key"] == original_key), None)
                if (not original or original["binding"] != admission.get("original_delivery_recovery_digest")
                    or len([row for row in rows if row["phase"] == "delivery_recovery"]) != 1):
                    raise FenceError("delivery_recovery_predecessor_consumed")
            if any(row["phase"] not in allowed for row in rows):
                raise FenceError("delivery_recovery_downstream_exists")
            predecessors = [row for row in rows if row["phase"] == resume_phase]
            if len(predecessors) != 1:
                raise FenceError("delivery_recovery_predecessor_consumed")
            old = predecessors[0]
            evidence = json.loads(old["evidence_json"])
            if (evidence.get("receipt_digest") != admission["predecessor_receipt_digest"]
                or evidence.get("verdict") != "RED"
                or evidence.get("receipt_digest") != digest_json({k: v for k, v in evidence.items() if k != "receipt_digest"})
                or any(evidence.get(k) != old[k] for k in ("run_id", "attempt", "fence", "phase", "binding", "phase_key"))):
                raise FenceError("delivery_recovery_predecessor_invalid")
            conn.execute("INSERT INTO completion_phases VALUES(?,?,?,?,?,?,'settled',?)",
                (key, run_id, attempt, fence, "delivery_recovery", supplied, json.dumps(admission, sort_keys=True)))
            conn.execute("COMMIT")
        return {"created": True, "phase_key": key}

    def settle_completion_phase(self, key: str, evidence: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (key,)).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (row["run_id"],)).fetchone() if row else None
            if row is None or run is None or run["fence"] != row["fence"] or run["attempts"] != row["attempt"]:
                raise FenceError("completion stale phase")
            if row["state"] != "claimed":
                raise FenceError("completion phase already settled")
            body = {key: value for key, value in evidence.items() if key != "receipt_digest"}
            if evidence.get("binding") != row["binding"] or evidence.get("receipt_digest") != digest_json(body):
                raise FenceError("completion evidence binding invalid")
            metadata = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
            if metadata.get("admission_context") is not None:
                admission = self._admission_context_conn(conn, run, metadata["admission_context"],
                    run_id=row["run_id"], attempt=row["attempt"], fence=row["fence"])
                if evidence.get("admission_context") != admission or evidence.get("phase") != "admission":
                    raise FenceError("completion admission evidence identity invalid")
            if any(evidence.get(k) != v for k, v in metadata.get("repair_claim", {}).items()):
                raise FenceError("completion repair evidence identity invalid")
            conn.execute("UPDATE completion_phases SET state='settled', evidence_json=? WHERE phase_key=?",
                         (json.dumps(evidence, sort_keys=True), key))
            conn.execute("COMMIT")

    def get_completion_phase(self, run_id: str, attempt: int, fence: int,
                             phase: str, repair_id: str | None = None) -> dict[str, Any] | None:
        """Read one durable completion claim for identity-bound reconciliation."""
        run_id = _required_text("run_id", run_id)
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise WorkUnitStoreError("completion attempt invalid")
        if isinstance(fence, bool) or not isinstance(fence, int) or fence < 1:
            raise WorkUnitStoreError("completion fence invalid")
        phase = _required_text("phase", phase)
        key = digest_json([run_id, attempt, fence, phase])
        if repair_id is not None:
            if phase not in {"checks_repair", "integration_checks", "delivery_verifier", "delivery_recovery", "checks"} or not isinstance(repair_id, str) or not repair_id.strip():
                raise WorkUnitStoreError("completion repair identity invalid")
            key = digest_json([run_id, attempt, fence, phase, repair_id])
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM completion_phases WHERE phase_key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        try:
            evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else None
        except (TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("completion phase evidence is not JSON") from exc
        if evidence is not None and not isinstance(evidence, dict):
            raise WorkUnitStoreError("completion phase evidence is not an object")
        return {
            "phase_key": row["phase_key"],
            "run_id": row["run_id"],
            "attempt": int(row["attempt"]),
            "fence": int(row["fence"]),
            "phase": row["phase"],
            "binding": row["binding"],
            "state": row["state"],
            "evidence": evidence,
        }

    def record_completion_phase_failure(self, key: str, evidence: dict[str, Any]) -> None:
        """Append failure evidence while leaving a claimed phase claimable."""
        key = _required_text("phase_key", key)
        if not isinstance(evidence, dict) or evidence.get("verdict") != "RED":
            raise WorkUnitStoreError("completion failure evidence invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM completion_phases WHERE phase_key = ?", (key,)
            ).fetchone()
            if row is None or row["state"] != "claimed":
                conn.execute("ROLLBACK")
                raise FenceError("completion failure claim is not active")
            current = {}
            if row["evidence_json"]:
                try:
                    current = json.loads(row["evidence_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("completion failure history is not JSON") from exc
            if not isinstance(current, dict):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("completion failure history is not an object")
            failures = current.setdefault("failure_evidence", [])
            if not isinstance(failures, list):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("completion failure history is not a list")
            if evidence not in failures:
                failures.append(copy.deepcopy(evidence))
            conn.execute(
                "UPDATE completion_phases SET evidence_json = ? WHERE phase_key = ? AND state = 'claimed'",
                (json.dumps(current, ensure_ascii=False, sort_keys=True), key),
            )
            conn.execute("COMMIT")

    @staticmethod
    def _delivery_retry_budget_conn(
        conn: sqlite3.Connection,
        work_unit_id: str,
    ) -> int | None:
        """Read the retry cap from the durable delivery binding.

        The caller's requested budget is only an upper bound.  A caller must
        not enlarge the contract's same-unit repair allowance by passing a
        larger ``max_attempts`` value to a store method.  ``None`` is kept for
        the explicit planning-control route, which has no coding retry
        budget; normal coding units are required to have a binding before
        they can start.
        """

        row = conn.execute(
            "SELECT contract_json FROM delivery_bindings WHERE work_unit_id = ?",
            (work_unit_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            contract = json.loads(row["contract_json"])
            repair = contract["repair_same_unit"]
            budget = repair["max_attempts"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("delivery retry budget unreadable") from exc
        if isinstance(budget, bool) or not isinstance(budget, int) or budget < 1:
            raise WorkUnitStoreError("delivery retry budget invalid")
        return min(int(budget), MAX_EXECUTOR_LAUNCHES)

    def retry_completion(self, dispatch_key: str, *, attempt: int, fence: int,
                         evidence: dict[str, Any], max_attempts: int,
                         continuation_authority_digest: str | None = None,
                         continuation_snapshot: dict[str, Any] | None = None,
                         recovery_request_id: str | None = None) -> dict[str, Any]:
        """Atomically route a proven completion RED back to the same unit."""
        if (
            isinstance(max_attempts, bool) or not isinstance(max_attempts, int)
            or max_attempts < 1 or not isinstance(evidence, Mapping)
            or evidence.get("verdict") != "RED"
        ):
            raise WorkUnitStoreError("completion retry policy invalid")
        evidence = dict(evidence)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            d = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (dispatch_key,)).fetchone()
            if d is None:
                raise WorkUnitStoreError("completion dispatch missing")
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (d["run_id"],)).fetchone()
            if run is None:
                raise WorkUnitStoreError("completion run missing")
            recovery_record = None
            recovery_row = None
            if recovery_request_id is not None:
                recovery_row = self._recovery_record_row_conn(conn, _required_text("recovery_request_id", recovery_request_id))
                recovery_record = self._recovery_record_from_row(recovery_row)
                if recovery_record is None:
                    raise WorkUnitStoreError("recovery retry request missing")
                if recovery_record.get("status") == "applied":
                    applied = recovery_record.get("apply")
                    applied_action = applied.get("action") if isinstance(applied, Mapping) else None
                    if (
                        not isinstance(applied, Mapping)
                        or applied.get("dispatch_key") != dispatch_key
                        or applied.get("request_digest") != recovery_record.get("request_digest")
                        or not isinstance(applied_action, Mapping)
                        or applied_action.get("kind") not in {"repair_same_node", "retry_within_budget"}
                        or applied.get("retry_evidence") != evidence
                        or applied.get("retry_max_attempts") != max_attempts
                    ):
                        raise FenceError("recovery retry replay dispatch mismatch")
                    conn.execute("COMMIT")
                    return {"status": "reused", "attempt": d["attempt"],
                            "recovery_request_id": recovery_request_id}
                if (
                    recovery_record.get("status") != "plan_verified"
                    or not isinstance(recovery_record.get("apply"), Mapping)
                    or recovery_record["apply"].get("state") != "retry_pending"
                    or recovery_record["apply"].get("request_digest") != recovery_record.get("request_digest")
                ):
                    raise WorkUnitStoreError("recovery retry is not authorized")
                request, result, _ = self._recovery_validate_plan(recovery_record)
                self._recovery_check_run_identity_conn(conn, request, require_current=True)
                self._recovery_check_repair_context_conn(conn, request)
                if (
                    request.get("run_id") != d["run_id"]
                    or request.get("work_unit_id") != d["work_unit_id"]
                    or request.get("attempt") != attempt
                    or request.get("fence") != fence
                    or result.get("action", {}).get("kind") not in {"repair_same_node", "retry_within_budget"}
                    or d["attempt"] != attempt or run["fence"] != fence
                ):
                    raise FenceError("recovery retry stale identity or action")
                if continuation_authority_digest is not None or continuation_snapshot is not None:
                    raise WorkUnitStoreError("recovery retry cannot combine continuation repair")
                retry_intent = recovery_record["apply"]
                approved_attempts = retry_intent.get("max_attempts")
                if approved_attempts is None or retry_intent.get("evidence") is None:
                    raise WorkUnitStoreError("recovery retry intent incomplete")
                if approved_attempts is not None:
                    if (
                        isinstance(approved_attempts, bool)
                        or not isinstance(approved_attempts, int)
                        or approved_attempts < 1
                        or max_attempts != approved_attempts
                    ):
                        raise WorkUnitStoreError("recovery retry attempt budget invalid")
                if retry_intent.get("evidence") is not None and retry_intent.get("evidence") != evidence:
                    raise WorkUnitStoreError("recovery retry evidence intent mismatch")
                if (
                    retry_intent.get("retry_evidence") is not None
                    and retry_intent.get("retry_evidence") != evidence
                ) or (
                    retry_intent.get("retry_max_attempts") is not None
                    and retry_intent.get("retry_max_attempts") != max_attempts
                ):
                    raise WorkUnitStoreError("recovery retry intent replay mismatch")
            if d["attempt"] != attempt or run["fence"] != fence:
                if recovery_record is not None:
                    raise FenceError("recovery retry stale Attempt/fence")
                return {"status": "reused", "attempt": d["attempt"]}
            proof = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (evidence.get("phase_key"),)).fetchone()
            if (proof is None or proof["state"] != "settled" or proof["run_id"] != d["run_id"]
                or proof["attempt"] != attempt or proof["fence"] != fence
                or json.loads(proof["evidence_json"]) != evidence):
                raise FenceError("completion RED proof invalid")
            if conn.execute("SELECT 1 FROM completion_phases WHERE run_id=? AND attempt=? AND state='claimed'",
                            (d["run_id"], attempt)).fetchone():
                raise FenceError("completion effect still active")
            if recovery_record is not None:
                retry_intent = recovery_record["apply"]
                retry_intent["retry_evidence"] = evidence
                retry_intent["retry_max_attempts"] = max_attempts
            old = conn.execute("SELECT * FROM attempts WHERE run_id=? AND ordinal=?", (d["run_id"], attempt)).fetchone()
            body = json.loads(d["receipt_json"])
            body.pop("receipt_digest", None)
            body.setdefault("completion_failures", []).append(evidence)
            binding_budget = self._delivery_retry_budget_conn(conn, d["work_unit_id"])
            effective_max_attempts = min(
                int(max_attempts),
                binding_budget if binding_budget is not None else MAX_EXECUTOR_LAUNCHES,
            )
            launch_budget = min(effective_max_attempts, MAX_EXECUTOR_LAUNCHES)
            exhausted = attempt >= effective_max_attempts or int(body.get("executor_launches", 0)) >= launch_budget
            if recovery_record is not None and exhausted:
                raise WorkUnitStoreError("recovery retry budget exhausted")
            continuation_repair = None
            if continuation_authority_digest is not None:
                if exhausted or not isinstance(continuation_snapshot, dict):
                    raise WorkUnitStoreError("continuation_repair_budget_or_snapshot_invalid")
                continuation_repair = trusted_continuation.record_repair(conn, continuation_authority_digest,
                    dispatch_key, attempt=attempt, fence=fence, evidence=evidence, snapshot=continuation_snapshot,
                    now=self._now(), identity_port=self.identity_port)
            elif continuation_snapshot is not None:
                raise WorkUnitStoreError("continuation_repair_authority_missing")
            conn.execute("UPDATE attempts SET state='stopped',receipt_digest=?,finished_at=? WHERE run_id=? AND ordinal=?",
                         (digest_json(evidence), self._now(), d["run_id"], attempt))
            if exhausted:
                body["executor_status"] = "exhausted"
                conn.execute("UPDATE runs SET state='stopped' WHERE run_id=?", (d["run_id"],))
                conn.execute("UPDATE work_units SET state='stopped' WHERE work_unit_id=?", (d["work_unit_id"],))
            else:
                attempt += 1
                fence += 1
                conn.execute("INSERT INTO attempts(run_id,ordinal,state,holder,fence,workspace_ref,created_at) VALUES(?,?,'ready',?,?,?,?)",
                             (d["run_id"], attempt, old["holder"], fence,
                              continuation_repair["scope"]["workspace_ref"] if continuation_repair else old["workspace_ref"], self._now()))
                conn.execute("UPDATE runs SET state='queued',attempts=?,fence=? WHERE run_id=?", (attempt, fence, d["run_id"]))
                conn.execute("UPDATE work_units SET state='ready' WHERE work_unit_id=?", (d["work_unit_id"],))
                previous_executor = body.pop("executor_receipt", None)
                if previous_executor:
                    body.setdefault("completion_executor_history", []).append(previous_executor)
                if continuation_repair:
                    # The exact original receipt (including unknown) now lives
                    # in the immutable repair row. This slot belongs to A2.
                    body.pop("executor_recovery_receipt", None)
                    body["continuation_repair_digest"] = continuation_repair["repair_digest"]
                body.update(attempt=attempt, fence=fence, executor_status="ready", queue_state="ready", attempt_state="ready")
            conn.execute("DELETE FROM leases WHERE work_unit_id=?", (d["work_unit_id"],))
            body["receipt_digest"] = digest_json(body)
            conn.execute("UPDATE dispatch_consumptions SET attempt=?,receipt_json=?,receipt_digest=? WHERE dispatch_key=?",
                         (attempt, json.dumps(body, sort_keys=True), body["receipt_digest"], dispatch_key))
            if recovery_record is not None:
                now = self._now()
                recovery_record["status"] = "applied"
                recovery_record["apply"].update(
                    state="applied",
                    dispatch_key=dispatch_key,
                    retry_status="retry_scheduled",
                    applied_at=now,
                    resulting_attempt=attempt,
                )
                recovery_record.setdefault("events", []).append(
                    {"event": "applied", "action": recovery_record["apply"]["action"],
                     "dispatch_key": dispatch_key, "at": now}
                )
                self._recovery_save_conn(conn, recovery_request_id, recovery_record, now)
            conn.execute("COMMIT")
        return {"status": "audit_required" if exhausted else "retry_scheduled", "attempt": attempt}

    @staticmethod
    def _parent_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "schema": SCHEMA,
            "parent_goal_id": row["parent_goal_id"],
            "goal_id": row["goal_id"],
            "goal_revision": int(row["goal_revision"]),
            "revision": int(row["goal_revision"]),
            "base_sha": row["base_sha"],
            "base_revision": row["base_sha"],
            "dependencies": _json_list(row["dependencies_json"], "parent dependencies"),
            "state": row["state"],
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }

    @staticmethod
    def _work_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = {
            "schema": SCHEMA,
            "work_unit_id": row["work_unit_id"],
            "parent_goal_id": row["parent_goal_id"],
            "node_id": row["node_id"],
            "node_kind": row["node_kind"],
            "producer": row["producer"],
            "worker_id": row["worker_id"],
            "base_sha": row["base_sha"],
            "dependencies": _json_list(row["dependencies_json"], "dependencies"),
            "read_set": _json_list(row["read_set_json"], "read_set"),
            "write_set": _json_list(row["write_set_json"], "write_set"),
            "worktree": row["worktree"],
            "branch": row["branch"],
            "state_root": row["state_root"],
            "state": row["state"],
            "run_id": row["run_id"],
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }
        # A readonly pre-migration Store has no column; NULL is also absence.
        if "delivery_after_json" in row.keys() and row["delivery_after_json"] is not None:
            result["delivery_after"] = validate_delivery_after(
                json.loads(row["delivery_after_json"]))
        return result

    @staticmethod
    def _run_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "schema": SCHEMA,
            "run_id": row["run_id"],
            "parent_goal_id": row["parent_goal_id"],
            "work_unit_id": row["work_unit_id"],
            "base_sha": row["base_sha"],
            "state": row["state"],
            "attempts": int(row["attempts"]),
            "fence": int(row["fence"]),
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }

    @staticmethod
    def _attempt_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "schema": SCHEMA,
            "run_id": row["run_id"],
            "attempt": int(row["ordinal"]),
            "ordinal": int(row["ordinal"]),
            "state": row["state"],
            "holder": row["holder"],
            "fence": int(row["fence"]),
            "workspace_ref": row["workspace_ref"],
            "receipt_ref": row["receipt_ref"],
            "receipt_digest": row["receipt_digest"],
            "created_at": float(row["created_at"]),
            "finished_at": None if row["finished_at"] is None else float(row["finished_at"]),
        }

    @staticmethod
    def _lease_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "work_unit_id": row["work_unit_id"],
            "holder": row["holder"],
            "fence": int(row["fence"]),
            "expires_at": float(row["expires_at"]),
        }

    @staticmethod
    def _dispatch_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        try:
            envelope = json.loads(row["envelope_json"])
            receipt = json.loads(row["receipt_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("stored dispatch consumption is not JSON") from exc
        if not isinstance(envelope, dict) or not isinstance(receipt, dict):
            raise WorkUnitStoreError("stored dispatch consumption is not an object")
        return {
            "schema": DISPATCH_CONSUMPTION_SCHEMA,
            "dispatch_key": row["dispatch_key"],
            "envelope_digest": row["envelope_digest"],
            "parent_goal_id": row["parent_goal_id"],
            "goal_id": row["goal_id"],
            "goal_revision": int(row["goal_revision"]),
            "node_id": row["node_id"],
            "work_unit_id": row["work_unit_id"],
            "run_id": row["run_id"],
            "attempt": int(row["attempt"]),
            "status": row["status"],
            "envelope": envelope,
            "receipt": receipt,
            "receipt_digest": row["receipt_digest"],
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
            "reused": True,
            "runs_created": 0,
            "attempts_created": 0,
        }

    @staticmethod
    def _delivery_binding_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        try:
            binding = json.loads(row["binding_json"])
            contract = json.loads(row["contract_json"])
            plan = json.loads(row["plan_json"])
            verdict = json.loads(row["verdict_json"]) if row["verdict_json"] else None
        except (TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("stored delivery binding is not JSON") from exc
        if not isinstance(binding, dict) or not isinstance(contract, dict) or not isinstance(plan, dict):
            raise WorkUnitStoreError("stored delivery binding is not an object")
        return {
            "schema": "lh-delivery-contract-binding/v1",
            "status": "bound",
            "work_unit_id": row["work_unit_id"],
            "run_id": row["run_id"],
            "unit_id": row["unit_id"],
            "contract_digest": row["contract_digest"],
            "plan_verdict_digest": row["plan_verdict_digest"],
            "binding": binding,
            "contract": contract,
            "plan_verdict": plan,
            "delivery_verdict": verdict,
            "delivery_verdict_digest": row["verdict_digest"],
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }

    def _parent_or_raise(self, conn: sqlite3.Connection, parent_goal_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM parent_goals WHERE parent_goal_id = ?", (parent_goal_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown parent_goal_id: {parent_goal_id}")
        return row

    @staticmethod
    def _settled_delivery_verifier_receipt(
        conn: sqlite3.Connection,
        *,
        evidence: Mapping[str, Any],
        run_id: str,
        attempt: int,
        fence: int,
    ) -> bool:
        """Require the delivery verdict to cite its durable phase evidence."""
        receipts = evidence.get("receipts") if isinstance(evidence, Mapping) else None
        receipt = receipts.get("delivery_verifier") if isinstance(receipts, Mapping) else None
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("verdict") != "GREEN"
            or receipt.get("phase") != "delivery_verifier"
            or not isinstance(receipt.get("phase_key"), str)
        ):
            return False
        phase = conn.execute(
            "SELECT * FROM completion_phases WHERE phase_key = ? AND run_id = ? AND attempt = ? AND fence = ?",
            (receipt["phase_key"], run_id, int(attempt), int(fence)),
        ).fetchone()
        if phase is None or phase["phase"] != "delivery_verifier" or phase["state"] != "settled" or not phase["evidence_json"]:
            return False
        try:
            stored_receipt = json.loads(phase["evidence_json"])
        except (TypeError, json.JSONDecodeError):
            return False
        if stored_receipt != dict(receipt):
            return False
        body = {key: value for key, value in stored_receipt.items() if key != "receipt_digest"}
        return stored_receipt.get("receipt_digest") == digest_json(body)

    @classmethod
    def _delivery_verdict_allows_terminal(cls, conn: sqlite3.Connection, work_unit_id: str) -> bool:
        row = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        if row is None or not row["verdict_json"]:
            return False
        try:
            payload = json.loads(row["verdict_json"])
        except (TypeError, json.JSONDecodeError):
            return False
        if not isinstance(payload, dict) or payload.get("verdict", {}).get("verdict") != "GREEN" or payload.get("phase") != "final":
            return False
        run_id = payload.get("run_id")
        try:
            attempt = int(payload.get("attempt"))
            fence = int(payload.get("fence"))
        except (TypeError, ValueError):
            return False
        run = conn.execute(
            "SELECT run_id, work_unit_id, attempts, fence FROM runs WHERE run_id = ? AND work_unit_id = ?",
            (run_id, work_unit_id),
        ).fetchone()
        if run is None or int(run["attempts"]) != attempt or int(run["fence"]) != fence:
            return False
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict) or payload.get("phase") != "final":
            return False
        return cls._settled_delivery_verifier_receipt(
            conn,
            evidence=evidence,
            run_id=run_id,
            attempt=attempt,
            fence=fence,
        )

    def get_delivery_binding(self, work_unit_id: str) -> dict[str, Any] | None:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        return self._delivery_binding_row(row)

    def bind_delivery_contract(
        self,
        work_unit_id: str,
        *,
        binding: Mapping[str, Any],
        contract: Mapping[str, Any],
        plan_verdict: Mapping[str, Any],
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist one immutable contract/plan binding in the WorkUnit store."""
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        if not isinstance(binding, Mapping) or binding.get("status") != "bound":
            raise WorkUnitStoreError("delivery binding must be a bound object")
        if not isinstance(contract, Mapping) or not isinstance(plan_verdict, Mapping):
            raise WorkUnitStoreError("delivery contract and plan are required")
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        resolved = engine.validate_contract(contract)
        if engine.verify_plan_verdict(plan_verdict, resolved).get("verdict") != "GREEN":
            raise WorkUnitStoreError("delivery plan verifier is not GREEN")
        if binding.get("contract_digest") != resolved["contract_digest"] or binding.get("unit_id") != resolved["unit_id"] or binding.get("plan_verdict_digest") != plan_verdict.get("plan_verdict_digest"):
            raise WorkUnitStoreError("delivery binding digest mismatch")
        body = json.loads(json.dumps(dict(binding), ensure_ascii=False, sort_keys=True))
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            effective_run_id = run_id or work["run_id"]
            prior = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if prior is not None:
                prior_binding = self._delivery_binding_row(prior) or {}
                prior_body = prior_binding.get("binding") or {}
                same_content = all(prior_body.get(key) == value for key, value in body.items())
                if (
                    not same_content
                ) or prior_binding.get("contract_digest") != resolved["contract_digest"] or prior_binding.get("plan_verdict_digest") != plan_verdict.get("plan_verdict_digest"):
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("delivery binding is already bound to different content")
                conn.execute("COMMIT")
                return prior_binding
            conn.execute(
                "INSERT INTO delivery_bindings(work_unit_id, run_id, unit_id, contract_digest, plan_verdict_digest, binding_json, contract_json, plan_json, verdict_json, verdict_digest, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
                (work_unit_id, effective_run_id, resolved["unit_id"], resolved["contract_digest"], plan_verdict["plan_verdict_digest"], json.dumps(body, ensure_ascii=False, sort_keys=True), json.dumps(dict(resolved), ensure_ascii=False, sort_keys=True), json.dumps(dict(plan_verdict), ensure_ascii=False, sort_keys=True), now, now),
            )
            self._append_event_conn(conn, event_id=f"delivery-binding:{work_unit_id}", parent_goal_id=work["parent_goal_id"], work_unit_id=work_unit_id, run_id=effective_run_id, event_type="delivery_contract_bound", payload={"unit_id": resolved["unit_id"], "contract_digest": resolved["contract_digest"], "plan_verdict_digest": plan_verdict["plan_verdict_digest"]}, created_at=now)
            conn.execute("COMMIT")
        return self.get_delivery_binding(work_unit_id) or {}

    def _supersede_delivery_binding_conn(
        self,
        conn: sqlite3.Connection,
        work_unit_id: str,
        *,
        old_contract_digest: str,
        binding: Mapping[str, Any],
        contract: Mapping[str, Any],
        plan_verdict: Mapping[str, Any],
        request_id: str,
        run_id: str,
        attempt: int,
        fence: int,
        reason: str,
    ) -> dict[str, Any]:
        """Replace a binding only after one fenced, quiescent replan proof.

        The old row is not deleted from the event history.  This method is
        intentionally called from the same SQLite transaction that marks the
        persisted planning request complete, so a planning check cannot be
        mistaken for an active contract.
        """
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        old_contract_digest = _required_text("old_contract_digest", old_contract_digest)
        request_id = _required_text("request_id", request_id)
        run_id = _required_text("run_id", run_id)
        reason = _required_text("reason", reason)
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise WorkUnitStoreError("replan attempt invalid")
        if isinstance(fence, bool) or not isinstance(fence, int) or fence < 0:
            raise WorkUnitStoreError("replan fence invalid")
        current = conn.execute(
            "SELECT * FROM delivery_bindings WHERE work_unit_id = ?",
            (work_unit_id,),
        ).fetchone()
        work = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        if current is None or work is None:
            raise WorkUnitStoreError("delivery_binding_missing_for_replan")
        old_value = self._delivery_binding_row(current) or {}
        current_digest = current["contract_digest"]
        if current_digest == contract["contract_digest"]:
            if old_value.get("binding", {}).get("superseded_by_request_id") == request_id:
                return old_value
            raise WorkUnitStoreError("delivery_binding_already_superseded")
        if current_digest != old_contract_digest:
            raise FenceError("replan old delivery binding changed")

        run = conn.execute(
            "SELECT * FROM runs WHERE run_id = ? AND work_unit_id = ?",
            (run_id, work_unit_id),
        ).fetchone()
        if run is None:
            raise FenceError("replan run identity mismatch")
        if int(run["attempts"]) != int(attempt) or int(run["fence"]) != int(fence):
            raise FenceError("replan fence or attempt is stale")
        if run["state"] not in {"queued", "ready", "retry_pending"}:
            raise WorkUnitStoreError("replan_run_not_quiescent")
        current_attempt = conn.execute(
            "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
            (run_id, int(attempt)),
        ).fetchone()
        if current_attempt is None or current_attempt["state"] not in {"ready", "interrupted", "retry_pending"}:
            raise WorkUnitStoreError("replan_attempt_not_quiescent")
        if conn.execute("SELECT 1 FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone() is not None:
            raise WorkUnitStoreError("replan_lease_active")
        if conn.execute(
            "SELECT 1 FROM completion_phases WHERE run_id = ? AND state = 'claimed' LIMIT 1",
            (run_id,),
        ).fetchone() is not None:
            raise WorkUnitStoreError("replan_effect_unknown")

        try:
            old_contract = json.loads(current["contract_json"])
            old_max = int(old_contract["repair_same_unit"]["max_attempts"])
            new_max = int(contract["repair_same_unit"]["max_attempts"])
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("replan_budget_unreadable") from exc
        if new_max > old_max or int(attempt) > new_max:
            raise WorkUnitStoreError("replan_budget_expanded_or_exhausted")

        body = json.loads(json.dumps(dict(binding), ensure_ascii=False, sort_keys=True))
        if body.get("status") != "bound" or body.get("supersedes_contract_digest") != old_contract_digest:
            raise WorkUnitStoreError("replan_supersession_proof_missing")
        if body.get("superseded_by_request_id") != request_id:
            raise WorkUnitStoreError("replan_request_binding_mismatch")
        now = self._now()
        old_event = {
            "binding": old_value.get("binding"),
            "contract_digest": old_value.get("contract_digest"),
            "plan_verdict_digest": old_value.get("plan_verdict_digest"),
            "delivery_verdict": old_value.get("delivery_verdict"),
        }
        new_event = {
            "binding": body,
            "contract_digest": contract["contract_digest"],
            "plan_verdict_digest": plan_verdict["plan_verdict_digest"],
        }
        self._append_event_conn(
            conn,
            event_id=f"delivery-binding-superseded:{work_unit_id}:{old_contract_digest}:{contract['contract_digest']}",
            parent_goal_id=work["parent_goal_id"],
            work_unit_id=work_unit_id,
            run_id=run_id,
            event_type="delivery_contract_superseded",
            payload={
                "request_id": request_id,
                "reason": reason,
                "run_id": run_id,
                "attempt": int(attempt),
                "fence": int(fence),
                "old": old_event,
                "new": new_event,
            },
            created_at=now,
        )
        conn.execute(
            "UPDATE delivery_bindings SET run_id = ?, unit_id = ?, contract_digest = ?, plan_verdict_digest = ?, binding_json = ?, contract_json = ?, plan_json = ?, verdict_json = NULL, verdict_digest = NULL, updated_at = ? WHERE work_unit_id = ? AND contract_digest = ?",
            (
                run_id,
                contract["unit_id"],
                contract["contract_digest"],
                plan_verdict["plan_verdict_digest"],
                json.dumps(body, ensure_ascii=False, sort_keys=True),
                json.dumps(dict(contract), ensure_ascii=False, sort_keys=True),
                json.dumps(dict(plan_verdict), ensure_ascii=False, sort_keys=True),
                now,
                work_unit_id,
                old_contract_digest,
            ),
        )
        if conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise FenceError("replan binding changed during commit")
        updated = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        return self._delivery_binding_row(updated) or {}

    def supersede_delivery_binding(
        self,
        work_unit_id: str,
        *,
        old_contract_digest: str,
        binding: Mapping[str, Any],
        contract: Mapping[str, Any],
        plan_verdict: Mapping[str, Any],
        request_id: str,
        run_id: str,
        attempt: int,
        fence: int,
        reason: str,
    ) -> dict[str, Any]:
        """Atomically activate a verified same-WorkUnit replacement binding."""
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        try:
            resolved = engine.validate_contract(contract)
        except Exception as exc:
            raise WorkUnitStoreError("replan_contract_invalid") from exc
        if engine.verify_plan_verdict(plan_verdict, resolved).get("verdict") != "GREEN":
            raise WorkUnitStoreError("replan_plan_verifier_not_green")
        if binding.get("contract_digest") != resolved["contract_digest"] or binding.get("plan_verdict_digest") != plan_verdict.get("plan_verdict_digest"):
            raise WorkUnitStoreError("replan_binding_digest_mismatch")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._supersede_delivery_binding_conn(
                    conn,
                    work_unit_id,
                    old_contract_digest=old_contract_digest,
                    binding=binding,
                    contract=resolved,
                    plan_verdict=plan_verdict,
                    request_id=request_id,
                    run_id=run_id,
                    attempt=attempt,
                    fence=fence,
                    reason=reason,
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return result

    def record_delivery_verdict(
        self,
        work_unit_id: str,
        *,
        verdict: Mapping[str, Any],
        evidence: Mapping[str, Any],
        run_id: str,
        attempt: int,
        fence: int,
        phase: str = "final",
        preflight: bool = False,
    ) -> dict[str, Any]:
        """Durably record the engine verdict before terminal promotion."""
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        run_id = _required_text("run_id", run_id)
        if not isinstance(verdict, Mapping) or not isinstance(evidence, Mapping):
            raise WorkUnitStoreError("delivery verdict evidence is required")
        if verdict.get("verdict") not in {"GREEN", "RED"}:
            raise WorkUnitStoreError("delivery verdict invalid")
        now = self._now()
        with self._connect() as conn:
            binding_row = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        if binding_row is None:
            raise WorkUnitStoreError("delivery binding missing for verdict")
        binding_value = self._delivery_binding_row(binding_row) or {}
        contract_value = binding_value.get("contract")
        if not isinstance(contract_value, Mapping):
            raise WorkUnitStoreError("delivery contract evidence missing")
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        checked = engine.verify_delivery(
            contract_value,
            evidence,
            phase=phase,
            allow_machine_complete_missing=bool(preflight),
            allow_delivery_verifier_missing=bool(preflight),
        )
        if checked.get("verdict") != verdict.get("verdict") or checked.get("verdict") != "GREEN":
            raise WorkUnitStoreError("delivery verdict failed independent engine verification")
        if evidence.get("contract_digest") != binding_row["contract_digest"] or evidence.get("unit_id") != binding_row["unit_id"]:
            raise WorkUnitStoreError("delivery verdict binding mismatch")
        identity = evidence.get("identity")
        if not isinstance(identity, Mapping) or identity.get("run_id") != run_id or int(identity.get("attempt", -1)) != int(attempt) or int(identity.get("fence", -1)) != int(fence):
            raise WorkUnitStoreError("delivery verdict execution identity mismatch")
        payload = {"verdict": dict(checked), "evidence": dict(evidence), "run_id": run_id, "attempt": int(attempt), "fence": int(fence), "phase": phase, "preflight": bool(preflight)}
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        digest = digest_json(payload)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            current_run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ? AND work_unit_id = ?",
                (run_id, work_unit_id),
            ).fetchone()
            if row is None or row["run_id"] not in {None, run_id} or current_run is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("delivery binding missing for verdict")
            if int(current_run["attempts"]) != int(attempt) or int(current_run["fence"]) != int(fence):
                conn.execute("ROLLBACK")
                raise FenceError("delivery verdict attempt is stale")
            if evidence.get("contract_digest") != row["contract_digest"] or evidence.get("unit_id") != row["unit_id"]:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("delivery verdict binding mismatch")
            if phase == "final" and not self._settled_delivery_verifier_receipt(
                conn,
                evidence=evidence,
                run_id=run_id,
                attempt=int(attempt),
                fence=int(fence),
            ):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("delivery eligibility phase is not settled")
            prior = json.loads(row["verdict_json"]) if row["verdict_json"] else None
            if prior is not None:
                same_execution = (
                    prior.get("run_id") == run_id
                    and int(prior.get("attempt", -1)) == int(attempt)
                    and int(prior.get("fence", -1)) == int(fence)
                )
                replace_provisional = bool(prior.get("preflight")) and not bool(preflight) and same_execution
                if prior != payload and not replace_provisional:
                    conn.execute("ROLLBACK")
                    raise FenceError("delivery verdict is already bound to different evidence")
                if prior == payload:
                    conn.execute("COMMIT")
                    return self._delivery_binding_row(row) or {}
            conn.execute("UPDATE delivery_bindings SET verdict_json = ?, verdict_digest = ?, updated_at = ?, run_id = ? WHERE work_unit_id = ?", (encoded, digest, now, run_id, work_unit_id))
            conn.execute("COMMIT")
        return self.get_delivery_binding(work_unit_id) or {}

    def record_recovery_request(
        self,
        request: Mapping[str, Any],
        budget: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one namespaced recovery request in the existing planning table."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_recovery_request")
        if not isinstance(request, Mapping):
            raise WorkUnitStoreError("recovery request must be an object")
        value = dict(request)
        string_fields = (
            "request_id", "goal_id", "node_id", "work_unit_id", "run_id",
            "phase", "candidate_digest", "packet_digest", "envelope_digest",
            "authority_digest", "reason_code", "input_evidence_digest",
            "failure_fingerprint",
        )
        for name in string_fields:
            _required_text(name, value.get(name))
        revision = value.get("goal_revision", value.get("revision"))
        for name, item in (("goal_revision", revision), ("attempt", value.get("attempt")),
                           ("fence", value.get("fence"))):
            if isinstance(item, bool) or not isinstance(item, int) or item < 1:
                raise WorkUnitStoreError(f"recovery request {name} invalid")
        if not isinstance(value.get("capability_binding"), Mapping):
            raise WorkUnitStoreError("recovery request capability binding invalid")
        if not isinstance(value.get("sanitized_evidence_refs"), (list, tuple)):
            raise WorkUnitStoreError("recovery request evidence refs invalid")
        if not isinstance(value.get("remaining_budget"), Mapping):
            raise WorkUnitStoreError("recovery request budget snapshot missing")
        limits = self._recovery_budget_limits(budget)
        request_id = value["request_id"]
        identity_digest = digest_json(self._recovery_key_identity(value))
        if identity_digest != request_id:
            raise WorkUnitStoreError("recovery request key mismatch")
        request_digest = digest_json(value)
        immutable_request_digest = digest_json(
            {key: item for key, item in value.items() if key != "remaining_budget"}
        )
        work_unit_id = value["work_unit_id"]
        input_digest = digest_json({"schema": RECOVERY_RECORD_SCHEMA, "request_id": request_id})
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT * FROM planning_requests WHERE request_id = ? OR input_digest = ?",
                (request_id, input_digest),
            ).fetchone()
            if prior is not None:
                record = self._recovery_record_from_row(prior)
                if (
                    record is None
                    or prior["request_id"] != request_id
                    or record.get("request_content_digest") != immutable_request_digest
                    or record.get("budget_limits") != limits
                ):
                    raise WorkUnitStoreError("recovery request identity or budget mismatch")
                conn.execute("COMMIT")
                return record
            self._recovery_check_run_identity_conn(conn, value, require_current=False)
            if "incident" in value and value["incident"] != self._recovery_incident_conn(conn, value):
                raise WorkUnitStoreError("recovery incident history mismatch")
            self._recovery_check_repair_context_conn(conn, value)
            prior_records = self._recovery_records_for_run_conn(conn, value["run_id"])
            if any(record.get("budget_limits") != limits for _, record in prior_records):
                raise WorkUnitStoreError("recovery Run budget limits changed")
            environment_original = None
            if value.get("recovery_environment_ref") is not None:
                self._recovery_environment_packet_conn(conn, value)
            if value.get("environment_event_ref") is not None:
                environment_original = self._recovery_environment_audit_conn(conn, {"request": value})
                if any(c["state"] in {"claimed", "outcome_unknown"}
                       for _, other in prior_records for c in other["claims"]):
                    raise WorkUnitStoreError("recovery Run has unresolved claim")
            record = {
                "schema": RECOVERY_RECORD_SCHEMA,
                "status": "requested",
                "request_id": request_id,
                "request_digest": request_digest,
                "request_content_digest": immutable_request_digest,
                "request": value,
                "result": None,
                "verdict": None,
                "apply": None,
                "claims": [],
                "events": [{"event": "requested", "at": now}],
                "budget_limits": limits,
                "incident_started_at": None,
                "incident_deadline_at": None,
            }
            conn.execute(
                "INSERT INTO planning_requests(request_id, work_unit_id, input_digest, request_json, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'requested', ?, ?)",
                (request_id, work_unit_id, input_digest,
                 json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")), now, now),
            )
            if environment_original is not None:
                environment_original["environment_successor_request_id"] = request_id
                self._recovery_save_conn(conn, environment_original["request_id"], environment_original, now)
            conn.execute("COMMIT")
        return record

    def get_recovery_request(self, request_id: str) -> dict[str, Any] | None:
        request_id = _required_text("request_id", request_id)
        with self._connect() as conn:
            row = self._recovery_record_row_conn(conn, request_id)
        return self._recovery_record_from_row(row)

    def recovery_requests(self, *, work_unit_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM planning_requests"
        args: tuple[Any, ...] = ()
        if work_unit_id is not None:
            query += " WHERE work_unit_id = ?"
            args = (_required_text("work_unit_id", work_unit_id),)
        query += " ORDER BY created_at, request_id"
        with self._connect() as conn:
            rows = conn.execute(query, args).fetchall()
        records = []
        for row in rows:
            record = self._recovery_record_from_row(row)
            if record is not None:
                records.append(record)
        return records

    def unresolved_recovery_readback(self, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Project stopped recovery from one read snapshot; never reserve or reconcile."""
        policy = manifest.get("planner_recovery", {})
        if not isinstance(policy, Mapping) or policy.get("recovery_readback_required") is not True:
            return []
        projection = []
        with self._connect() as conn:
            conn.execute("PRAGMA query_only = ON")
            conn.execute("BEGIN")
            parent = self._parent_row(conn.execute(
                "SELECT * FROM parent_goals WHERE parent_goal_id=?", (manifest["goal_id"],)
            ).fetchone())
            if (parent is None or parent["goal_id"] != manifest["goal_id"]
                or parent["goal_revision"] != manifest["goal_revision"]):
                raise WorkUnitStoreError("recovery_readback_parent_identity_mismatch")
            if parent["state"] == "active":
                return []
            binding = manifest.get("reviewed_task_list_binding") or {}
            authority = binding.get("authorization") or {}
            source_ref = binding.get("source_approval_ref")
            source_budget = None
            if source_ref is not None:
                scope = digest_json({"schema": "lh-reviewed-task-budget-scope/v1",
                    "goal_id": parent["goal_id"],
                    "source_approval_digest": authority["source_approval_digest"],
                    "source_ref_content_digest": source_ref["content_digest"]})
                usage, reservations, settlements = self._task_area_budget_usage_conn(
                    conn, parent["parent_goal_id"], scope)
                usage["pending_reservations"] = len(set(reservations) - set(settlements))
                source_budget = {"source_approval_ref": source_ref,
                    "source_approval_digest": authority["source_approval_digest"],
                    "budget_scope_digest": scope, "budget": authority.get("budget"),
                    "expires_at": authority.get("expires_at"), "usage": usage}
            for task in manifest["tasks"]:
                unit = self._work_row(conn.execute(
                    "SELECT * FROM work_units WHERE parent_goal_id=? AND node_id=?",
                    (parent["parent_goal_id"], task["node_id"])).fetchone())
                if unit is None:
                    continue
                run = self._run_row(conn.execute(
                    "SELECT * FROM runs WHERE work_unit_id=?", (unit["work_unit_id"],)).fetchone())
                if run is None:
                    continue
                attempt = self._attempt_row(conn.execute(
                    "SELECT * FROM attempts WHERE run_id=? AND ordinal=?",
                    (run["run_id"], run["attempts"])).fetchone())
                if (attempt is None or attempt["fence"] != run["fence"]
                    or run["parent_goal_id"] != parent["parent_goal_id"]
                    or unit["run_id"] != run["run_id"]):
                    raise WorkUnitStoreError("recovery_readback_attempt_identity_mismatch")
                identity = {"goal_id": parent["goal_id"], "goal_revision": parent["goal_revision"],
                    "node_id": unit["node_id"], "work_unit_id": unit["work_unit_id"],
                    "run_id": run["run_id"], "attempt": attempt["attempt"], "fence": attempt["fence"]}
                records = [record for _, record in self._recovery_records_for_run_conn(conn, run["run_id"])]
                # The Store query already orders ties by created_at/request_id.
                records.sort(key=lambda row: row["request"]["attempt"])
                for record in records:
                    request = record["request"]
                    stored_attempt = conn.execute(
                        "SELECT fence FROM attempts WHERE run_id=? AND ordinal=?",
                        (run["run_id"], request["attempt"])).fetchone()
                    if (any(request.get(key) != identity[key] for key in (
                            "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id"))
                        or stored_attempt is None or stored_attempt["fence"] != request.get("fence")
                        or request["attempt"] > attempt["attempt"]
                        or record["request_digest"] != digest_json(request)):
                        raise WorkUnitStoreError("recovery_readback_request_identity_mismatch")
                current = next((row for row in reversed(records)
                                if row["request"]["attempt"] == attempt["attempt"]
                                and row["request"]["fence"] == attempt["fence"]), None)
                incident = None
                # Receipt-only incident reconstruction does not create a RecoveryRequest.
                for phase in ("checks", "verifier", "integration_checks", "integration_verifier", "delivery_verifier"):
                    row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?",
                        (digest_json([run["run_id"], attempt["attempt"], attempt["fence"], phase]),)).fetchone()
                    if row is None or row["state"] != "settled":
                        continue
                    evidence = json.loads(row["evidence_json"])
                    if evidence.get("verdict") != "RED":
                        continue
                    if current is not None and any(current["request"].get(key) != value for key, value in (
                        ("phase", phase), ("candidate_digest", evidence.get("candidate_digest")),
                        ("input_evidence_digest", digest_json(evidence)),
                    )):
                        raise WorkUnitStoreError("recovery_readback_current_evidence_mismatch")
                    checks = task.get("completion_contract", {}).get("checks", [])
                    incident = self._recovery_incident_conn(conn, {**identity, "phase": phase,
                        "test_refs": [{"id": check.get("id"), "command_digest": digest_json(dict(check))}
                                      for check in checks if isinstance(check, Mapping) and check.get("id")],
                        "candidate_digest": evidence.get("candidate_digest"),
                        "input_evidence_digest": digest_json(evidence)})
                    break
                if incident is None:
                    # Without current RED evidence, retain only an existing unresolved request.
                    if current is None:
                        continue
                    request = current["request"]
                    applied = current.get("apply")
                    if (request.get("reason_code") == request.get("phase") == "machine_complete"
                        and current.get("status") == "applied"
                        and isinstance(applied, Mapping) and applied.get("state") == "applied"
                        and run["state"] == unit["state"] == "integrated"
                        and not any(claim.get("state") in {"claimed", "outcome_unknown"}
                                    for record in records for claim in record.get("claims", []))):
                        # Execution completion alone is not recovery completion. Read the
                        # same verified decision and durable apply bindings used by apply.
                        _, result, _ = self._recovery_validate_plan(current)
                        action = result["action"]
                        bound = (applied.get("action") == action and all(
                            applied.get(key) == value for key, value in (
                                ("request_digest", current["request_digest"]),
                                ("plan_digest", result["plan_digest"]),
                                ("candidate_digest", request["candidate_digest"]),
                                ("authority_digest", request["authority_digest"]))))
                        closeout = (action.get("kind") == "resume_phase"
                                    and result.get("target_phase") == "closeout"
                                    and applied.get("acknowledgement") == "closeout_already_completed")
                        target = action.get("target_node_id")
                        successor = (action.get("kind") == "dispatch_successor"
                                     and set(action) == {"kind", "target_node_id"}
                                     and isinstance(target, str) and bool(target.strip())
                                     and target != request["node_id"])
                        if bound and (closeout or successor):
                            continue
                    incident = current["request"].get("incident")
                claims = [(record, claim) for record in records for claim in record.get("claims", [])]
                deadlines = [(record, claim) for record, claim in claims
                             if isinstance(claim.get("incident_deadline_at"), (int, float))]
                incident_budget = None
                if deadlines:
                    first, claim = min(deadlines, key=lambda pair: (
                        pair[1]["incident_deadline_at"], pair[1]["started_at"], pair[1]["call_id"]))
                    incident_budget = {"request_id": first["request_id"], "call_id": claim["call_id"],
                        "budget_limits": first.get("budget_limits"),
                        "incident_started_at": claim.get("incident_started_at"),
                        "incident_deadline_at": claim["incident_deadline_at"]}
                status = current.get("status", "unknown") if current else "request_missing"
                waiting, role = status, "controller"
                if status == "outcome_unknown" or any(claim.get("state") == "outcome_unknown" for _, claim in claims):
                    waiting = "reconcile_unknown_effect"
                elif any(claim.get("state") == "claimed" for _, claim in claims):
                    waiting = "claim_outcome_pending"
                elif (self._recovery_audit_required({"incident": incident})
                      and ("audit_binding" in policy or "audit" in authority.get("allowed_actions", []))
                      and not any(record.get("audit_result") is not None for record in records
                                  if record["request"].get("incident", {}).get("incident_id") == (incident or {}).get("incident_id"))):
                    waiting, role = "audit_required", "auditor"
                elif status == "awaiting_authority":
                    role = "owner"
                projection.append({**identity,
                    "execution": {"parent_state": parent["state"], "work_unit_state": unit["state"],
                        "run_state": run["state"], "attempt_state": attempt["state"],
                        "holder": attempt["holder"], "holder_observation": {"status": "unknown"}},
                    "recovery_status": status, "incident_id": (incident or {}).get("incident_id"),
                    "failure_refs": (incident or {}).get("failure_refs", []),
                    "request_ref": ({key: current[key] for key in ("request_id", "request_digest")} if current else None),
                    "prior_request_refs": [{"request_id": record["request_id"], "request_digest": record["request_digest"],
                        "status": record["status"], "plan_digest": (record.get("result") or {}).get("plan_digest")}
                        for record in records if record["request"]["attempt"] < attempt["attempt"]],
                    "claim_refs": [{"request_id": record["request_id"], "call_id": claim["call_id"],
                        "phase": claim["phase"], "state": claim["state"],
                        "owner_process_identity": claim.get("owner_process_identity"),
                        "owner_observation": claim.get("owner_observation")} for record, claim in claims],
                    "source_budget_ref": source_budget, "incident_budget_ref": incident_budget,
                    "waiting_conditions": ["parent_not_active", waiting], "responsible_role": role})
        return projection

    def claim_recovery_phase(self, request_id: str, *, phase: str,
                             disposition_event_id: str | None = None) -> dict[str, Any]:
        """Reserve one planner call before launch; unresolved Run claims block all keys."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_recovery_claim")
        request_id = _required_text("request_id", request_id)
        if phase not in {"planner", "plan_verifier", "audit"}:
            raise WorkUnitStoreError("recovery phase invalid")
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._recovery_record_row_conn(conn, request_id)
            record = self._recovery_record_from_row(row)
            if record is None:
                raise KeyError(f"unknown recovery request: {request_id}")
            request = record["request"]
            parent = conn.execute(
                "SELECT p.state FROM parent_goals p JOIN runs r ON r.parent_goal_id=p.parent_goal_id WHERE r.run_id=?",
                (request["run_id"],)).fetchone()
            if disposition_event_id is not None or (parent is not None and parent["state"] == "stopped"):
                self._recovery_disposition_conn(conn, disposition_event_id, request_id=request_id)
                if phase not in {"planner", "plan_verifier"}:
                    raise WorkUnitStoreError("recovery_disposition_scope_invalid")
            run_records = self._recovery_records_for_run_conn(conn, request["run_id"])
            active = []
            unknown = []
            for other_row, other in run_records:
                for claim in other.get("claims", []):
                    if claim.get("state") == "claimed":
                        observation = observe_process_identity(
                            claim.get("owner_process_identity"), identity_port=self.identity_port
                        )
                        active.append((other_row, other, claim, observation))
                    elif claim.get("state") == "outcome_unknown":
                        unknown.append((other_row, other, claim))
            dead = [entry for entry in active if entry[3].status != "alive"]
            for other_row, other, claim, observation in dead:
                claim["state"] = "outcome_unknown"
                claim["owner_observation"] = {
                    "status": observation.status,
                    "reason": observation.reason,
                    "observed_identity": observation.identity.as_dict() if observation.identity else None,
                }
                other["status"] = "outcome_unknown"
                other.setdefault("events", []).append({
                    "event": "outcome_unknown", "phase": claim["phase"],
                    "call_id": claim["call_id"], "at": now,
                    "owner_observation": claim["owner_observation"],
                })
                self._recovery_save_conn(conn, other_row["request_id"], other, now)
            alive = [entry for entry in active if entry[3].status == "alive"]
            if alive:
                current = self._recovery_record_from_row(
                    self._recovery_record_row_conn(conn, request_id)
                ) or record
                conn.execute("COMMIT")
                return self._recovery_claim_result(
                    current, claimed=False, waiting=True, reason="same_run_claim_owner_alive"
                )
            if dead:
                current = self._recovery_record_from_row(
                    self._recovery_record_row_conn(conn, request_id)
                ) or record
                conn.execute("COMMIT")
                return self._recovery_claim_result(
                    current, claimed=False, reason="same_run_claim_outcome_unknown"
                )
            if unknown:
                conn.execute("COMMIT")
                return self._recovery_claim_result(
                    record, claimed=False, reason="same_run_recovery_outcome_unknown"
                )
            try:
                self._recovery_check_run_identity_conn(conn, request, require_current=True)
            except (FenceError, WorkUnitStoreError) as exc:
                record.setdefault("events", []).append(
                    {"event": "claim_rejected", "phase": phase, "reason": str(exc), "at": now}
                )
                self._recovery_save_conn(conn, request_id, record, now)
                conn.execute("COMMIT")
                return self._recovery_claim_result(record, claimed=False, reason=str(exc))
            if any(other.get("budget_limits") != record.get("budget_limits") for _, other in run_records):
                conn.execute("COMMIT")
                return self._recovery_claim_result(
                    record, claimed=False, reason="recovery_budget_limits_mismatch"
                )
            audit_required = self._recovery_audit_required(request)
            inherited_audit = self._recovery_environment_audit_conn(conn, record)
            if phase == "planner" and request.get("recovery_environment_ref") is not None and inherited_audit is None:
                reason = ("recovery_environment_request_superseded" if record.get("environment_successor_request_id")
                          else "recovery_environment_evidence_required")
                conn.execute("COMMIT")
                return self._recovery_claim_result(record, claimed=False, reason=reason)
            if "incident" in request and request["incident"] != self._recovery_incident_conn(conn, request):
                raise WorkUnitStoreError("recovery incident history mismatch")
            if phase == "audit":
                if (not audit_required or "audit" not in request.get("allowed_actions", [])
                    or not isinstance(request.get("audit_binding"), Mapping)):
                    raise WorkUnitStoreError("recovery audit authority missing")
                incident_id = request["incident"]["incident_id"]
                if any(other["request"].get("incident", {}).get("incident_id") == incident_id
                       and any(claim.get("phase") == "audit" for claim in other.get("claims", []))
                       for _, other in run_records):
                    conn.execute("COMMIT")
                    return self._recovery_claim_result(record, claimed=False, reason="recovery_incident_audit_already_claimed")
                allowed_status = "requested"
            elif phase == "planner":
                allowed_status = "audit_recorded" if audit_required and inherited_audit is None else "requested"
            else:
                allowed_status = "result_recorded"
            existing_phase_claims = [
                claim for claim in record.get("claims", []) if claim.get("phase") == phase
            ]
            if record.get("status") != allowed_status or existing_phase_claims:
                conn.execute("COMMIT")
                return self._recovery_claim_result(
                    record, claimed=False, reason="recovery_phase_already_claimed_or_finished"
                )
            if audit_required and phase != "audit":
                self._recovery_audit_ref(record, inherited_audit)
            if self._phase_policy_conn(conn) is not None and len(self._phase_slots_conn(conn)["model"]["owners"]) >= 2:
                conn.execute("COMMIT")
                return self._recovery_claim_result(record, claimed=False, reason="resource_slot_waiting:model")
            all_claims = [
                claim
                for _, other in run_records
                for claim in other.get("claims", [])
            ]
            call_field = "planner_calls" if phase == "planner" else "plan_verifier_calls"
            shared_phases = {"planner"} if phase == "planner" else {"plan_verifier", "audit"}
            call_count = sum(1 for claim in all_claims if claim.get("phase") in shared_phases)
            if call_count >= record["budget_limits"][call_field]:
                record["status"] = "failed"
                reason = ("planner" if phase == "planner" else "plan_verifier") + "_call_budget_exhausted"
                record["failure"] = {"reason": reason}
                record.setdefault("events", []).append(
                    {"event": "claim_rejected", "phase": phase, "reason": reason, "at": now}
                )
                self._recovery_save_conn(conn, request_id, record, now)
                conn.execute("COMMIT")
                return self._recovery_claim_result(record, claimed=False, reason=reason)
            incident_claims = [claim for claim in all_claims if isinstance(claim.get("started_at"), (int, float))]
            if incident_claims:
                first = min(incident_claims, key=lambda claim: claim["started_at"])
                incident_started = float(first["incident_started_at"])
                incident_deadline = float(first["incident_deadline_at"])
            else:
                incident_started = now
                incident_deadline = now + float(record["budget_limits"]["incident_timeout_seconds"])
            if now >= incident_deadline:
                record["status"] = "failed"
                reason = "incident_time_budget_exhausted"
                record["failure"] = {"reason": reason}
                record.setdefault("events", []).append(
                    {"event": "claim_rejected", "phase": phase, "reason": reason, "at": now}
                )
                self._recovery_save_conn(conn, request_id, record, now)
                conn.execute("COMMIT")
                return self._recovery_claim_result(record, claimed=False, reason=reason)
            current_identity = self.identity_port.current()
            if callable(getattr(current_identity, "as_dict", None)):
                current_identity = current_identity.as_dict()
            owner = ProcessIdentity.from_dict(current_identity)
            if owner is None:
                raise WorkUnitStoreError("recovery claim owner identity unavailable")
            timeout_field = "planner_timeout_seconds" if phase == "planner" else "plan_verifier_timeout_seconds"
            claim_id = os.urandom(16).hex()
            boot_id = None
            if owner.source == "proc" and owner.start_token.startswith("linux:"):
                pieces = owner.start_token.split(":", 2)
                boot_id = pieces[1] if len(pieces) == 3 else None
            claim = {
                "phase": phase,
                "state": "claimed",
                "call_id": claim_id,
                "owner_process_identity": owner.as_dict(),
                "owner_pid": owner.pid,
                "owner_boot_id": boot_id,
                "started_at": now,
                "deadline_at": min(
                    now + float(record["budget_limits"][timeout_field]), incident_deadline
                ),
                "incident_started_at": incident_started,
                "incident_deadline_at": incident_deadline,
            }
            record.setdefault("claims", []).append(claim)
            record["incident_started_at"] = incident_started
            record["incident_deadline_at"] = incident_deadline
            record["status"] = {"planner": "claimed", "plan_verifier": "verifier_claimed",
                                "audit": "audit_claimed"}[phase]
            record.setdefault("events", []).append(
                {"event": "claimed", "phase": phase, "call_id": claim_id,
                 "owner_process_identity": owner.as_dict(), "at": now}
            )
            self._recovery_save_conn(conn, request_id, record, now)
            conn.execute("COMMIT")
        return self._recovery_claim_result(record, claimed=True)

    def finish_recovery_phase(
        self,
        request_id: str,
        phase: str,
        evidence: Mapping[str, Any],
        outcome: str = "success",
    ) -> dict[str, Any]:
        """Append a durable phase result without replacing an earlier result."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_recovery_finish")
        request_id = _required_text("request_id", request_id)
        if phase not in {"planner", "plan_verifier", "audit"} or outcome not in {"success", "unknown", "failed"}:
            raise WorkUnitStoreError("recovery phase outcome invalid")
        if not isinstance(evidence, Mapping):
            raise WorkUnitStoreError("recovery phase evidence must be an object")
        supplied = dict(evidence)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._recovery_record_row_conn(conn, request_id)
            record = self._recovery_record_from_row(row)
            if record is None:
                raise KeyError(f"unknown recovery request: {request_id}")
            field = {"planner": "result", "plan_verifier": "verdict", "audit": "audit_result"}[phase]
            saved = record.get(field)
            if saved is not None:
                if saved != supplied:
                    raise WorkUnitStoreError("recovery phase result is immutable")
                conn.execute("COMMIT")
                return record
            claim = next(
                (item for item in reversed(record.get("claims", []))
                 if item.get("phase") == phase and item.get("state") == "claimed"),
                None,
            )
            if claim is None:
                raise WorkUnitStoreError("recovery phase has no active claim")
            current_identity = self.identity_port.current()
            if callable(getattr(current_identity, "as_dict", None)):
                current_identity = current_identity.as_dict()
            current = ProcessIdentity.from_dict(current_identity)
            owner = ProcessIdentity.from_dict(claim.get("owner_process_identity"))
            if current is None or owner is None or not owner.matches(current):
                raise WorkUnitStoreError("recovery phase owner identity mismatch")
            audit_error = None
            if outcome == "success":
                try:
                    if phase == "audit":
                        self._recovery_validate_audit(record, supplied)
                    elif self._recovery_audit_required(record["request"]):
                        ref = self._recovery_audit_ref(record, self._recovery_environment_audit_conn(conn, record))
                        basis = supplied.get("decision_basis") if phase == "planner" else supplied
                        if not isinstance(basis, Mapping) or basis.get("audit_ref") != ref:
                            raise WorkUnitStoreError("recovery decision audit reference invalid")
                except WorkUnitStoreError as exc:
                    audit_error = str(exc)
            claim["state"] = outcome
            claim["finished_at"] = now
            claim["evidence_digest"] = digest_json(supplied)
            record.setdefault("events", []).append(
                {"event": "phase_finished", "phase": phase, "outcome": outcome,
                 "call_id": claim["call_id"], "evidence_digest": claim["evidence_digest"], "at": now}
            )
            failure_evidence = self._recovery_failure_payload(supplied)
            failure_evidence_digest = digest_json(failure_evidence)
            if outcome == "unknown":
                record["status"] = "outcome_unknown"
                record["failure"] = {
                    "phase": phase,
                    "evidence_digest": failure_evidence_digest,
                    "evidence": failure_evidence,
                }
            elif outcome == "failed":
                record["status"] = "failed"
                record["failure"] = {
                    "phase": phase,
                    "evidence_digest": failure_evidence_digest,
                    "evidence": failure_evidence,
                }
            elif (
                supplied.get("request_id") != request_id
                or supplied.get("request_digest") != record.get("request_digest")
            ):
                record["status"] = "failed"
                record["failure"] = {
                    "phase": phase,
                    "reason": "recovery_phase_binding_invalid",
                    "evidence_digest": failure_evidence_digest,
                    "evidence": failure_evidence,
                }
            elif audit_error is not None:
                record["status"] = "failed"
                record["failure"] = {"phase": phase, "reason": audit_error,
                                     "evidence_digest": failure_evidence_digest, "evidence": failure_evidence}
            else:
                record[field] = supplied
                if phase == "planner":
                    record["status"] = "result_recorded"
                elif phase == "audit":
                    record["status"] = "audit_recorded"
                elif supplied.get("verdict") == "GREEN":
                    record["status"] = "plan_verified"
                else:
                    record["status"] = "failed"
                    record["failure"] = {
                        "phase": phase,
                        "reason": "plan_verifier_not_green",
                        "evidence_digest": failure_evidence_digest,
                        "evidence": failure_evidence,
                    }
            self._recovery_save_conn(conn, request_id, record, now)
            conn.execute("COMMIT")
        return record

    @staticmethod
    def _recovery_failure_payload(evidence: Mapping[str, Any]) -> dict[str, Any]:
        """Persist bounded diagnostics only; raw stdout/stderr/transcripts stay out."""
        text_fields = {
            "reason", "reason_code", "status", "outcome", "error_code",
            "stderr_digest", "stdout_digest", "failure_fingerprint",
        }
        integer_fields = {"exit", "exit_code", "returncode"}
        boolean_fields = {"timed_out", "process_terminated"}
        safe: dict[str, Any] = {}
        if isinstance(evidence.get("sanitized_stderr"), str):
            from .successor_executor import _sanitize_diagnostic
            safe["sanitized_stderr"] = _sanitize_diagnostic(evidence["sanitized_stderr"])
        for name in text_fields:
            value = evidence.get(name)
            if not isinstance(value, str):
                continue
            cleaned = " ".join("".join(char if char.isprintable() else " " for char in value).split())
            if not cleaned:
                continue
            if name.endswith("_digest") and re.fullmatch(r"sha256:[0-9a-f]{64}", cleaned) is None:
                continue
            safe[name] = cleaned[:240]
        for name in integer_fields:
            value = evidence.get(name)
            if isinstance(value, int) and not isinstance(value, bool):
                safe[name] = value
        for name in boolean_fields:
            value = evidence.get(name)
            if isinstance(value, bool):
                safe[name] = value
        return safe

    def apply_recovery_decision(
        self,
        request_id: str,
        *,
        expected_request_digest: str,
        action: Mapping[str, Any] | str,
        evidence: Mapping[str, Any] | None = None,
        max_attempts: int | None = None,
        disposition_event_id: str | None = None,
    ) -> dict[str, Any]:
        """Revalidate and persist a decision; retry effects commit in retry_completion."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_recovery_apply")
        request_id = _required_text("request_id", request_id)
        expected_request_digest = _required_text("expected_request_digest", expected_request_digest)
        if isinstance(action, str):
            action_value: dict[str, Any] = {"kind": action}
        elif isinstance(action, Mapping):
            action_value = dict(action)
        else:
            raise WorkUnitStoreError("recovery action invalid")
        if evidence is not None and not isinstance(evidence, Mapping):
            raise WorkUnitStoreError("recovery apply evidence invalid")
        evidence_value = dict(evidence) if evidence is not None else None
        if max_attempts is not None and (
            isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1
        ):
            raise WorkUnitStoreError("recovery apply attempt budget invalid")
        if action_value.get("kind") in {"repair_same_node", "retry_within_budget"} and (
            evidence is None or max_attempts is None
        ):
            raise WorkUnitStoreError("recovery retry intent requires evidence and max_attempts")
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._recovery_record_row_conn(conn, request_id)
            record = self._recovery_record_from_row(row)
            if record is None:
                raise KeyError(f"unknown recovery request: {request_id}")
            if record.get("request_digest") != expected_request_digest:
                raise WorkUnitStoreError("recovery request digest changed")
            environment_audit = self._recovery_environment_audit_conn(conn, record)
            disposition = None
            parent = conn.execute(
                "SELECT p.state FROM parent_goals p JOIN runs r ON r.parent_goal_id=p.parent_goal_id WHERE r.run_id=?",
                (record["request"]["run_id"],)).fetchone()
            if disposition_event_id is not None or (parent is not None and parent["state"] == "stopped"):
                disposition = self._recovery_disposition_conn(
                    conn, disposition_event_id, request_id=request_id)
                if (action_value != {"kind": "resume_phase"}
                    or (record.get("result") or {}).get("target_phase") != "closeout"):
                    raise WorkUnitStoreError("recovery_disposition_scope_invalid")
            if record.get("status") == "applied":
                prior_apply = record.get("apply")
                if (
                    not isinstance(prior_apply, Mapping)
                    or prior_apply.get("request_digest") != expected_request_digest
                    or prior_apply.get("action") != action_value
                ):
                    raise WorkUnitStoreError("recovery applied decision mismatch")
                if action_value.get("kind") in {"repair_same_node", "retry_within_budget"} and (
                    prior_apply.get("evidence") != evidence_value
                    or prior_apply.get("max_attempts") != max_attempts
                ):
                    raise WorkUnitStoreError("recovery retry intent replay mismatch")
                conn.execute("COMMIT")
                return record
            if ("incident" in record["request"]
                and record["request"]["incident"] != self._recovery_incident_conn(conn, record["request"])):
                raise WorkUnitStoreError("recovery incident history mismatch")
            request, result, verdict = self._recovery_validate_plan(record, audit_record=environment_audit)
            self._recovery_check_repair_context_conn(conn, request)
            if result.get("action") != action_value:
                raise WorkUnitStoreError("recovery apply action differs from verified plan")
            if record.get("status") != "plan_verified":
                raise WorkUnitStoreError("recovery decision is not plan_verified")
            if record.get("apply") is not None:
                prior_apply = record["apply"]
                if (
                    not isinstance(prior_apply, Mapping)
                    or prior_apply.get("request_digest") != expected_request_digest
                    or prior_apply.get("action") != action_value
                ):
                    raise WorkUnitStoreError("recovery apply intent changed")
                if action_value.get("kind") in {"repair_same_node", "retry_within_budget"} and (
                    prior_apply.get("evidence") != evidence_value
                    or prior_apply.get("max_attempts") != max_attempts
                ):
                    raise WorkUnitStoreError("recovery retry intent replay mismatch")
                if prior_apply.get("state") == "retry_pending":
                    conn.execute("COMMIT")
                    return record
            if evidence is not None:
                for key, expected in (
                    ("request_digest", expected_request_digest),
                    ("candidate_digest", request.get("candidate_digest")),
                    ("authority_digest", request.get("authority_digest")),
                ):
                    if key in evidence and evidence[key] != expected:
                        raise WorkUnitStoreError(f"recovery apply evidence mismatch: {key}")
            run = self._recovery_check_run_identity_conn(conn, request, require_current=True)
            environment_resume = (environment_audit is not None and request["phase"] == "checks"
                                  and result.get("target_phase") == "checks")
            if action_value.get("kind") == "resume_phase" and run["state"] != "integrated" and not environment_resume:
                raise WorkUnitStoreError("recovery closeout Run is not integrated")
            run_records = self._recovery_records_for_run_conn(conn, request["run_id"])
            if any(other.get("budget_limits") != record.get("budget_limits") for _, other in run_records):
                raise WorkUnitStoreError("recovery Run budget limits changed")
            if any(
                claim.get("state") in {"claimed", "outcome_unknown"}
                for _, other in run_records for claim in other.get("claims", [])
            ):
                raise WorkUnitStoreError("recovery Run has unresolved claim")
            incident_deadline = record.get("incident_deadline_at")
            if incident_deadline is None:
                claims = [claim for _, other in run_records for claim in other.get("claims", [])]
                claims = [claim for claim in claims if isinstance(claim.get("incident_deadline_at"), (int, float))]
                if claims:
                    incident_deadline = min(claim["incident_deadline_at"] for claim in claims)
            if incident_deadline is not None and now > float(incident_deadline):
                raise WorkUnitStoreError("recovery incident time budget exhausted")
            for phase, field in (("planner", "planner_calls"), ("plan_verifier", "plan_verifier_calls")):
                used = sum(
                    1 for _, other in run_records for claim in other.get("claims", [])
                    if claim.get("phase") in ({"planner"} if phase == "planner" else {"plan_verifier", "audit"})
                )
                if used > record["budget_limits"][field]:
                    raise WorkUnitStoreError(f"recovery {field} budget exceeded")
            self._recovery_check_run_identity_conn(conn, request, require_current=True)
            kind = action_value.get("kind")
            if kind == "dispatch_successor":
                target_node = action_value.get("target_node_id")
                if (set(action_value) != {"kind", "target_node_id"}
                    or not isinstance(target_node, str) or not target_node.strip()
                    or target_node == request.get("node_id")
                    or request.get("reason_code") != "machine_complete"
                    or request.get("phase") != "machine_complete"
                    or run["state"] != "integrated"):
                    raise WorkUnitStoreError("recovery successor dispatch binding invalid")
            if kind == "collect_evidence":
                raise WorkUnitStoreError("recovery action unsupported: collect_evidence")
            if kind == "resume_phase" and result.get("target_phase") != "closeout" and not environment_resume:
                raise WorkUnitStoreError("recovery action unsupported: resume_phase")
            if kind not in {
                "resume_phase", "repair_same_node", "retry_within_budget",
                "request_authority", "insufficient_evidence", "dispatch_successor",
            }:
                raise WorkUnitStoreError("recovery action unsupported")
            apply_state = "retry_pending" if kind in {"repair_same_node", "retry_within_budget"} else "applied"
            record["apply"] = {
                "request_digest": expected_request_digest,
                "plan_digest": result["plan_digest"],
                "candidate_digest": request["candidate_digest"],
                "authority_digest": request["authority_digest"],
                "action": action_value,
                "state": apply_state,
                "max_attempts": max_attempts,
                "evidence": evidence_value,
                "recorded_at": now,
            }
            if disposition is not None:
                record["apply"]["disposition_event_id"] = disposition_event_id
                record["apply"]["disposition_payload_digest"] = disposition["payload_digest"]
            if kind == "resume_phase":
                record["status"] = "applied"
                record["apply"]["acknowledgement"] = ("checks_resume_authorized" if environment_resume
                                                       else "closeout_already_completed")
            elif kind == "dispatch_successor":
                record["status"] = "applied"
            elif kind == "request_authority":
                record["status"] = "awaiting_authority"
                record["apply"]["state"] = "awaiting_authority"
            elif kind == "insufficient_evidence":
                record["status"] = "awaiting_evidence"
                record["apply"]["state"] = "awaiting_evidence"
            else:
                record["status"] = "plan_verified"
            record.setdefault("events", []).append(
                {"event": "decision_applied" if record["status"] == "applied" else "decision_recorded",
                 "action": action_value, "plan_digest": result["plan_digest"], "at": now}
            )
            self._recovery_save_conn(conn, request_id, record, now)
            conn.execute("COMMIT")
        return record

    def record_planning_request(self, request: Mapping[str, Any], *, work_unit_id: str | None = None) -> dict[str, Any]:
        """Persist one idempotent typed migration/replan request."""
        if not isinstance(request, Mapping):
            raise WorkUnitStoreError("planning request must be an object")
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        if engine.verify_planning_request(request).get("verdict") != "GREEN":
            raise WorkUnitStoreError("planning request verifier is not GREEN")
        request_id = _required_text("request_id", request.get("request_id"))
        input_digest = _required_text("input_digest", request.get("input_digest"))
        work_unit_id = None if work_unit_id is None else _required_text("work_unit_id", work_unit_id)
        now = self._now()
        encoded = json.dumps(dict(request), ensure_ascii=False, sort_keys=True)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute("SELECT * FROM planning_requests WHERE request_id = ? OR input_digest = ?", (request_id, input_digest)).fetchone()
            if prior is not None:
                if prior["input_digest"] != input_digest or prior["request_json"] != encoded:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("planning request identity mismatch")
                conn.execute("COMMIT")
                return {"schema": "lh-delivery-planning-request/v1", "status": prior["status"], "request_id": prior["request_id"], "input_digest": prior["input_digest"], "work_unit_id": prior["work_unit_id"], "request": json.loads(prior["request_json"]), "reused": True}
            if work_unit_id is not None and conn.execute("SELECT 1 FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone() is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            conn.execute("INSERT INTO planning_requests(request_id, work_unit_id, input_digest, request_json, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', ?, ?)", (request_id, work_unit_id, input_digest, encoded, now, now))
            conn.execute("COMMIT")
        return {"schema": "lh-delivery-planning-request/v1", "status": "pending", "request_id": request_id, "input_digest": input_digest, "work_unit_id": work_unit_id, "request": dict(request), "reused": False}

    def planning_requests(self, *, work_unit_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM planning_requests"
        args: tuple[Any, ...] = ()
        if work_unit_id is not None:
            query += " WHERE work_unit_id = ?"
            args = (_required_text("work_unit_id", work_unit_id),)
        query += " ORDER BY created_at, request_id"
        with self._connect() as conn:
            rows = conn.execute(query, args).fetchall()
        records = []
        for row in rows:
            try:
                request = json.loads(row["request_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(request, Mapping) or request.get("schema") != "lh-delivery-planning-request/v1":
                continue
            records.append({
                "schema": "lh-delivery-planning-request/v1",
                "status": row["status"],
                "request_id": row["request_id"],
                "input_digest": row["input_digest"],
                "work_unit_id": row["work_unit_id"],
                "request": request,
                "reused": True,
            })
        return records

    def consume_planning_request(
        self,
        request_id: str,
        *,
        contract: Mapping[str, Any],
        capability: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Consume one pending replan through the contract checker.

        Missing capability is intentionally a durable pending result.  The
        contract checker is the only producer of a completed planning verdict;
        this method never treats a caller-supplied GREEN field as completion.
        """
        request_id = _required_text("request_id", request_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM planning_requests WHERE request_id = ?", (request_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown planning request: {request_id}")
        request = json.loads(row["request_json"])
        if not isinstance(request, Mapping) or request.get("schema") != "lh-delivery-planning-request/v1":
            raise WorkUnitStoreError("planning request namespace mismatch")
        if row["status"] == "completed":
            return {
                "schema": "lh-delivery-planning-result/v1",
                "status": "completed",
                "reason": "planning_request_already_completed",
                "request_id": request_id,
                "input_digest": row["input_digest"],
            }
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        result = engine.check_planning_request(request, contract, capability=capability)
        if result.get("status") != "completed":
            return result
        request_binding = request.get("binding")
        if not isinstance(request_binding, Mapping):
            raise WorkUnitStoreError("planning binding missing for replan")
        old_contract_digest = request_binding.get("supersedes_contract_digest")
        if not isinstance(old_contract_digest, str) or not old_contract_digest.strip():
            raise WorkUnitStoreError("planning supersession proof missing")
        run_id = request.get("run_id")
        attempt = request.get("attempt")
        if not isinstance(run_id, str) or not run_id.strip():
            raise WorkUnitStoreError("planning run identity missing")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise WorkUnitStoreError("planning attempt identity missing")
        plan = result.get("plan_verdict")
        if not isinstance(plan, Mapping):
            plan = engine.plan_delivery_unit(contract)
        resolved = engine.validate_contract(contract)
        if engine.verify_plan_verdict(plan, resolved).get("verdict") != "GREEN":
            raise WorkUnitStoreError("planning plan verifier is not GREEN")
        new_binding = dict(request_binding)
        new_binding.update(
            {
                "schema": "lh-delivery-contract-binding/v1",
                "status": "bound",
                "unit_id": resolved["unit_id"],
                "contract_digest": resolved["contract_digest"],
                "plan_verdict_digest": plan["plan_verdict_digest"],
                "goal_id": resolved["goal"]["id"],
                "goal_revision": resolved["goal"]["revision"],
                "node_id": resolved["node"]["id"],
                "node_kind": resolved["node"]["kind"],
                "producer": "PlanNodeController",
                "supersedes_contract_digest": old_contract_digest,
                "superseded_by_request_id": request_id,
                "supersession_reason": request["reason"],
            }
        )
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute("SELECT status, input_digest FROM planning_requests WHERE request_id = ?", (request_id,)).fetchone()
            if current is None or current["input_digest"] != row["input_digest"]:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("planning request changed during consume")
            work_unit_id = row["work_unit_id"]
            if not isinstance(work_unit_id, str) or not work_unit_id.strip():
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("planning work unit missing")
            try:
                binding_value = self._supersede_delivery_binding_conn(
                    conn,
                    work_unit_id,
                    old_contract_digest=old_contract_digest,
                    binding=new_binding,
                    contract=resolved,
                    plan_verdict=plan,
                    request_id=request_id,
                    run_id=run_id,
                    attempt=attempt,
                    fence=int(request["fence"]),
                    reason=str(request["reason"]),
                )
                conn.execute(
                    "UPDATE planning_requests SET status = 'completed', updated_at = ? WHERE request_id = ? AND status = 'pending'",
                    (self._now(), request_id),
                )
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return {**result, "delivery_binding": binding_value}

    def create_parent_goal(
        self,
        parent_goal_id: str | dict[str, Any],
        *,
        goal_id: str | None = None,
        goal_revision: int | None = None,
        revision: int | None = None,
        base_sha: str | None = None,
        base_revision: str | None = None,
        dependencies: Iterable[str] | str | None = None,
        state: str = "active",
    ) -> dict[str, Any]:
        """Create one immutable parent revision, or return its identical replay."""

        payload: dict[str, Any] = {}
        if isinstance(parent_goal_id, dict):
            payload = dict(parent_goal_id)
            parent_goal_id = payload.get("parent_goal_id") or payload.get("goal_id") or payload.get("id")
            goal_id = payload.get("goal_id", goal_id)
            goal_revision = payload.get("goal_revision", payload.get("revision", goal_revision))
            base_sha = payload.get("base_sha", payload.get("base_revision", base_sha))
            dependencies = payload.get("dependencies", payload.get("depends_on", dependencies))
            state = payload.get("state", state)
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        effective_goal_id = _required_text("goal_id", goal_id or parent_goal_id)
        effective_revision = int(goal_revision if goal_revision is not None else (revision if revision is not None else 1))
        if effective_revision < 1:
            raise WorkUnitStoreError("goal_revision must be positive")
        effective_base = _required_text("base_sha", base_sha or base_revision)
        effective_dependencies = normalize_dependencies(dependencies)
        effective_state = _required_text("state", state)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM parent_goals WHERE parent_goal_id = ?", (parent_goal_id,)).fetchone()
            if existing is not None:
                immutable = (
                    existing["goal_id"], int(existing["goal_revision"]), existing["base_sha"],
                    tuple(_json_list(existing["dependencies_json"], "parent dependencies")),
                )
                requested = (effective_goal_id, effective_revision, effective_base, effective_dependencies)
                if immutable != requested:
                    raise DuplicateWorkUnitError("parent_goal_id is already bound to different inputs")
                conn.execute("COMMIT")
                return self._parent_row(existing) or {}
            conn.execute(
                "INSERT INTO parent_goals(parent_goal_id, goal_id, goal_revision, base_sha, dependencies_json, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (parent_goal_id, effective_goal_id, effective_revision, effective_base, json.dumps(effective_dependencies), effective_state, now, now),
            )
            conn.execute("COMMIT")
        return self.get_parent_goal(parent_goal_id)

    register_parent_goal = create_parent_goal
    create_parent = create_parent_goal

    def get_parent_goal(self, parent_goal_id: str) -> dict[str, Any]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM parent_goals WHERE parent_goal_id = ?", (parent_goal_id,)).fetchone()
        value = self._parent_row(row)
        if value is None:
            raise KeyError(f"unknown parent_goal_id: {parent_goal_id}")
        return value

    def set_parent_state(self, parent_goal_id: str, state: str) -> dict[str, Any]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        state = _required_text("state", state)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = self._parent_or_raise(conn, parent_goal_id)
            if parent["state"] != state:
                self._parent_state_ref_conn(conn, parent)
                generation = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE parent_goal_id=? AND event_type='parent_state_changed'",
                    (parent_goal_id,)).fetchone()[0] + 1
                conn.execute("UPDATE parent_goals SET state = ?, updated_at = ? WHERE parent_goal_id = ?",
                             (state, now, parent_goal_id))
                self._append_event_conn(conn, event_id=f"parent-state:{parent_goal_id}:{generation}",
                    parent_goal_id=parent_goal_id, work_unit_id=None, run_id=None,
                    event_type="parent_state_changed", created_at=now,
                    payload={"previous_state": parent["state"], "state": state,
                             "updated_at": now, "generation": generation})
            conn.execute("COMMIT")
        return self.get_parent_goal(parent_goal_id)

    @staticmethod
    def _prepare_work_definition(definition: dict[str, Any], parent_goal_id: str) -> dict[str, Any]:
        if not isinstance(definition, dict):
            raise WorkUnitStoreError("work unit definition must be an object")
        work_unit_id = definition.get("work_unit_id") or definition.get("id") or definition.get("node_id")
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        node_id = _required_text("node_id", definition.get("node_id") or work_unit_id)
        node_kind = _required_text("node_kind", definition.get("node_kind") or "coding")
        producer = _required_text("producer", definition.get("producer") or "scheduler")
        worker_id = _required_text("worker_id", definition.get("worker_id") or work_unit_id)
        base_sha = _required_text("base_sha", definition.get("base_sha") or definition.get("base_revision"))
        dependencies = normalize_dependencies(definition.get("dependencies", definition.get("depends_on")))
        read_set = normalize_paths(definition.get("read_set"))
        write_set = normalize_paths(definition.get("write_set"))
        def optional_text(name: str) -> str | None:
            value = definition.get(name)
            return None if value is None else _required_text(name, value)
        return {
            "work_unit_id": work_unit_id,
            "parent_goal_id": parent_goal_id,
            "node_id": node_id,
            "node_kind": node_kind,
            "producer": producer,
            "worker_id": worker_id,
            "base_sha": base_sha,
            "dependencies": list(dependencies),
            **({"delivery_after": validate_delivery_after(definition["delivery_after"])}
               if "delivery_after" in definition else {}),
            "read_set": list(read_set),
            "write_set": list(write_set),
            "worktree": optional_text("worktree"),
            "branch": optional_text("branch"),
            "state_root": optional_text("state_root"),
        }

    @staticmethod
    def _immutable_work_values(value: dict[str, Any]) -> tuple[Any, ...]:
        return (
            value["parent_goal_id"], value["node_id"], value.get("node_kind", "coding"),
            value.get("producer", "scheduler"), value["worker_id"], value["base_sha"],
            tuple(value["dependencies"]), tuple(value["read_set"]), tuple(value["write_set"]),
            value.get("worktree"), value.get("branch"), value.get("state_root"),
            tuple(value["delivery_after"]) if "delivery_after" in value else None,
        )

    def _existing_work_definitions(self, conn: sqlite3.Connection, parent_goal_id: str) -> list[dict[str, Any]]:
        rows = conn.execute("SELECT * FROM work_units WHERE parent_goal_id = ? ORDER BY created_at, node_id, work_unit_id", (parent_goal_id,)).fetchall()
        return [self._work_row(row) or {} for row in rows]

    @staticmethod
    def _resolve_dependency_from_records(records: dict[str, dict[str, Any]], by_node: dict[str, str], source_id: str, reference: str) -> str:
        direct = reference if reference in records else None
        by_node_id = by_node.get(reference)
        if direct is not None and by_node_id is not None and direct != by_node_id:
            raise DependencyError(f"dependency reference is ambiguous: {source_id} -> {reference}")
        target = direct or by_node_id
        if target is None:
            raise DependencyError(f"dependency is not registered under the same parent: {source_id} -> {reference}")
        return target

    @classmethod
    def _validate_graph_records(cls, records_list: list[dict[str, Any]]) -> dict[str, str]:
        records: dict[str, dict[str, Any]] = {}
        by_node: dict[str, str] = {}
        for record in records_list:
            work_unit_id = record["work_unit_id"]
            node_id = record["node_id"]
            if work_unit_id in records:
                raise DuplicateWorkUnitError(f"duplicate work_unit_id: {work_unit_id}")
            if node_id in by_node:
                raise DuplicateWorkUnitError(f"duplicate node_id: {node_id}")
            records[work_unit_id] = record
            by_node[node_id] = work_unit_id
        edges: dict[str, list[str]] = {}
        for source_id, record in records.items():
            edges[source_id] = [
                cls._resolve_dependency_from_records(records, by_node, source_id, reference)
                for reference in [*record.get("dependencies", []),
                                  *(validate_delivery_after(record["delivery_after"])
                                    if "delivery_after" in record else [])]
            ]
        visiting: list[str] = []
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visiting:
                start = visiting.index(node)
                raise DAGCycleError((*visiting[start:], node))
            if node in visited:
                return
            visiting.append(node)
            for dependency in edges[node]:
                visit(dependency)
            visiting.pop()
            visited.add(node)

        for node in sorted(records):
            visit(node)
        return {source: dependency for source, dependencies in edges.items() for dependency in dependencies}

    def create_work_unit(
        self,
        work_unit_id: str | dict[str, Any],
        *,
        parent_goal_id: str | None = None,
        node_id: str | None = None,
        node_kind: str | None = None,
        producer: str | None = None,
        worker_id: str | None = None,
        base_sha: str | None = None,
        base_revision: str | None = None,
        dependencies: Iterable[str] | str | None = None,
        depends_on: Iterable[str] | str | None = None,
        delivery_after: list[str] | None = None,
        read_set: Iterable[str | Path] | str | Path | None = None,
        write_set: Iterable[str | Path] | str | Path | None = None,
        worktree: str | Path | None = None,
        branch: str | None = None,
        state_root: str | Path | None = None,
    ) -> dict[str, Any]:
        """Insert one child definition; identical replays return the row."""

        if isinstance(work_unit_id, dict):
            payload = dict(work_unit_id)
            parent_goal_id = payload.get("parent_goal_id", parent_goal_id)
            work_unit_id = payload.get("work_unit_id") or payload.get("id") or payload.get("node_id")
            node_id = payload.get("node_id", node_id)
            node_kind = payload.get("node_kind", node_kind)
            producer = payload.get("producer", producer)
            worker_id = payload.get("worker_id", worker_id)
            base_sha = payload.get("base_sha", payload.get("base_revision", base_sha))
            dependencies = payload.get("dependencies", payload.get("depends_on", dependencies))
            if "delivery_after" in payload:
                delivery_after = validate_delivery_after(payload["delivery_after"])
            read_set = payload.get("read_set", read_set)
            write_set = payload.get("write_set", write_set)
            worktree = payload.get("worktree", worktree)
            branch = payload.get("branch", branch)
            state_root = payload.get("state_root", state_root)
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        prepared = self._prepare_work_definition({
            "work_unit_id": work_unit_id,
            "node_id": node_id,
            "node_kind": node_kind,
            "producer": producer,
            "worker_id": worker_id,
            "base_sha": base_sha or base_revision,
            "dependencies": dependencies if dependencies is not None else depends_on,
            **({"delivery_after": delivery_after} if delivery_after is not None else {}),
            "read_set": read_set,
            "write_set": write_set,
            "worktree": None if worktree is None else str(worktree),
            "branch": branch,
            "state_root": None if state_root is None else str(state_root),
        }, parent_goal_id)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._parent_or_raise(conn, parent_goal_id)
            existing_row = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (prepared["work_unit_id"],)).fetchone()
            if existing_row is not None:
                existing = self._work_row(existing_row) or {}
                if self._immutable_work_values(existing) != self._immutable_work_values(prepared):
                    raise DuplicateWorkUnitError("work_unit_id is already bound to different inputs")
                conn.execute("COMMIT")
                return existing
            node_row = conn.execute(
                "SELECT work_unit_id FROM work_units WHERE parent_goal_id = ? AND node_id = ?",
                (parent_goal_id, prepared["node_id"]),
            ).fetchone()
            if node_row is not None:
                raise DuplicateWorkUnitError(f"node_id is already registered: {prepared['node_id']}")
            self._validate_graph_records([*self._existing_work_definitions(conn, parent_goal_id), prepared])
            conn.execute(
                "INSERT INTO work_units(work_unit_id, parent_goal_id, node_id, node_kind, producer, worker_id, base_sha, dependencies_json, read_set_json, write_set_json, worktree, branch, state_root, delivery_after_json, state, run_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                (
                    prepared["work_unit_id"], prepared["parent_goal_id"], prepared["node_id"], prepared["node_kind"], prepared["producer"], prepared["worker_id"], prepared["base_sha"],
                    json.dumps(prepared["dependencies"]), json.dumps(prepared["read_set"]), json.dumps(prepared["write_set"]),
                    prepared["worktree"], prepared["branch"], prepared["state_root"],
                    json.dumps(prepared["delivery_after"]) if "delivery_after" in prepared else None, now, now,
                ),
            )
            conn.execute("COMMIT")
        return self.get_work_unit(prepared["work_unit_id"])

    add_work_unit = create_work_unit

    def register_work_units(self, parent_goal_id: str, work_units: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Atomically add a batch after validating its complete DAG."""

        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        definitions = [
            self._prepare_work_definition({**definition, "parent_goal_id": parent_goal_id}, parent_goal_id)
            for definition in work_units
        ]
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._parent_or_raise(conn, parent_goal_id)
            existing = self._existing_work_definitions(conn, parent_goal_id)
            by_id = {row["work_unit_id"]: row for row in existing}
            combined = list(existing)
            for prepared in definitions:
                prior = by_id.get(prepared["work_unit_id"])
                if prior is not None:
                    if self._immutable_work_values(prior) != self._immutable_work_values(prepared):
                        raise DuplicateWorkUnitError("work_unit_id is already bound to different inputs")
                    continue
                if any(row["node_id"] == prepared["node_id"] for row in combined):
                    raise DuplicateWorkUnitError(f"node_id is already registered: {prepared['node_id']}")
                by_id[prepared["work_unit_id"]] = prepared
                combined.append(prepared)
            self._validate_graph_records(combined)
            existing_ids = {row["work_unit_id"] for row in existing}
            inserted_ids: set[str] = set()
            for prepared in definitions:
                if prepared["work_unit_id"] in existing_ids or prepared["work_unit_id"] in inserted_ids:
                    continue
                conn.execute(
                    "INSERT INTO work_units(work_unit_id, parent_goal_id, node_id, node_kind, producer, worker_id, base_sha, dependencies_json, read_set_json, write_set_json, worktree, branch, state_root, delivery_after_json, state, run_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                    (
                        prepared["work_unit_id"], prepared["parent_goal_id"], prepared["node_id"], prepared["node_kind"], prepared["producer"], prepared["worker_id"], prepared["base_sha"],
                        json.dumps(prepared["dependencies"]), json.dumps(prepared["read_set"]), json.dumps(prepared["write_set"]),
                        prepared["worktree"], prepared["branch"], prepared["state_root"],
                        json.dumps(prepared["delivery_after"]) if "delivery_after" in prepared else None, now, now,
                    ),
                )
                inserted_ids.add(prepared["work_unit_id"])
            conn.execute("COMMIT")
        return self.list_work_units(parent_goal_id)

    register_dag = register_work_units

    def list_work_units(self, parent_goal_id: str) -> list[dict[str, Any]]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM work_units WHERE parent_goal_id = ? ORDER BY created_at, node_id, work_unit_id",
                (parent_goal_id,),
            ).fetchall()
        return [self._work_row(row) or {} for row in rows]

    work_units = list_work_units

    def get_work_unit(self, work_unit_id: str) -> dict[str, Any]:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        value = self._work_row(row)
        if value is None:
            raise KeyError(f"unknown work_unit_id: {work_unit_id}")
        return value

    def resolve_dependency(self, parent_goal_id: str, reference: str) -> dict[str, Any]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        reference = _required_text("dependency", reference)
        records = self.list_work_units(parent_goal_id)
        by_id = {row["work_unit_id"]: row for row in records}
        by_node = {row["node_id"]: row for row in records}
        direct = by_id.get(reference)
        by_node_row = by_node.get(reference)
        if direct is not None and by_node_row is not None and direct["work_unit_id"] != by_node_row["work_unit_id"]:
            raise DependencyError(f"dependency reference is ambiguous: {reference}")
        target = direct or by_node_row
        if target is None:
            raise DependencyError(f"dependency is not registered under the same parent: {reference}")
        return target

    def validate_dag(self, parent_goal_id: str) -> dict[str, Any]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        records = self.list_work_units(parent_goal_id)
        self._validate_graph_records(records)
        return {
            "schema": SCHEMA,
            "status": "valid",
            "parent_goal_id": parent_goal_id,
            "work_unit_ids": [row["work_unit_id"] for row in records],
        }

    check_dag = validate_dag

    def _delivery_dependencies_conn(self, conn: sqlite3.Connection,
                                    run: sqlite3.Row) -> list[str]:
        work = self._work_row(conn.execute(
            "SELECT * FROM work_units WHERE work_unit_id=?", (run["work_unit_id"],)).fetchone())
        if work is None:
            raise WorkUnitStoreError("delivery_dependency_work_unit_missing")
        if "delivery_after" not in work:
            return []
        if work["parent_goal_id"] != run["parent_goal_id"] or work["run_id"] != run["run_id"]:
            raise FenceError("delivery_dependency_run_identity_mismatch")
        self._parent_or_raise(conn, run["parent_goal_id"])
        records = {row["work_unit_id"]: row for row in
                   self._existing_work_definitions(conn, run["parent_goal_id"])}
        by_node = {row["node_id"]: key for key, row in records.items()}
        pending = []
        for reference in work["delivery_after"]:
            target_id = self._resolve_dependency_from_records(
                records, by_node, work["work_unit_id"], reference)
            if target_id == work["work_unit_id"]:
                raise DAGCycleError((target_id, target_id))
            target = records[target_id]
            predecessor = conn.execute("SELECT * FROM runs WHERE run_id=?",
                                       (target["run_id"],)).fetchone()
            attempt = None if predecessor is None else conn.execute(
                "SELECT * FROM attempts WHERE run_id=? AND ordinal=?",
                (predecessor["run_id"], predecessor["attempts"])).fetchone()
            if (target["state"] != "integrated" or predecessor is None
                or predecessor["state"] != "integrated"
                or predecessor["parent_goal_id"] != run["parent_goal_id"]
                or predecessor["work_unit_id"] != target_id
                or predecessor["base_sha"] != target["base_sha"]
                or attempt is None or attempt["state"] != "integrated"
                or attempt["fence"] != predecessor["fence"]):
                pending.append(reference)
        return pending

    def pending_delivery_dependencies(self, *, run_id: str, attempt: int, fence: int,
                                      only_unclaimed: bool = False) -> list[str]:
        """Readonly wait projection; claim/terminal mutations recheck in their transaction."""
        with self._connect() as conn:
            conn.execute("BEGIN")
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if run is None or run["attempts"] != attempt or run["fence"] != fence:
                raise FenceError("completion stale attempt")
            if only_unclaimed and conn.execute(
                "SELECT 1 FROM completion_phases WHERE phase_key=?",
                (digest_json([run_id, attempt, fence, "integration"]),)).fetchone():
                # Existing claims (including unknown) and settled receipts must
                # reach the original binding/reconciliation checks, not waiting.
                return []
            return self._delivery_dependencies_conn(conn, run)

    @staticmethod
    def _run_id(parent_goal_id: str, work_unit_id: str, base_sha: str) -> str:
        return "run-" + hashlib.sha256(_canonical({"parent_goal_id": parent_goal_id, "work_unit_id": work_unit_id, "base_sha": base_sha})).hexdigest()[:32]

    def _ensure_run_conn(self, conn: sqlite3.Connection, work_unit: sqlite3.Row, now: float) -> sqlite3.Row:
        if work_unit["run_id"] is not None:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (work_unit["run_id"],)).fetchone()
            if row is None:
                raise WorkUnitStoreError("work unit points to a missing run")
            return row
        run_id = self._run_id(work_unit["parent_goal_id"], work_unit["work_unit_id"], work_unit["base_sha"])
        conn.execute(
            "INSERT INTO runs(run_id, parent_goal_id, work_unit_id, base_sha, state, attempts, fence, created_at, updated_at) VALUES (?, ?, ?, ?, 'queued', 0, 0, ?, ?)",
            (run_id, work_unit["parent_goal_id"], work_unit["work_unit_id"], work_unit["base_sha"], now, now),
        )
        conn.execute("UPDATE work_units SET run_id = ?, updated_at = ? WHERE work_unit_id = ?", (run_id, now, work_unit["work_unit_id"]))
        self._append_event_conn(
            conn,
            event_id=f"run-created:{run_id}",
            parent_goal_id=work_unit["parent_goal_id"],
            work_unit_id=work_unit["work_unit_id"],
            run_id=run_id,
            event_type="run_created",
            payload={"run_id": run_id, "base_sha": work_unit["base_sha"]},
            created_at=now,
        )
        return conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()

    @staticmethod
    def _append_event_conn(
        conn: sqlite3.Connection,
        *,
        event_id: str,
        parent_goal_id: str,
        work_unit_id: str | None,
        run_id: str | None,
        event_type: str,
        payload: dict[str, Any],
        created_at: float,
    ) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        prior = conn.execute("SELECT parent_goal_id, work_unit_id, run_id, event_type, payload_json FROM events WHERE event_id = ?", (event_id,)).fetchone()
        if prior is not None:
            if tuple(prior) != (parent_goal_id, work_unit_id, run_id, event_type, encoded):
                raise WorkUnitStoreError("event_id is already bound to different content")
            return
        conn.execute(
            "INSERT INTO events(event_id, parent_goal_id, work_unit_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_id, parent_goal_id, work_unit_id, run_id, event_type, encoded, created_at),
        )

    @staticmethod
    def _task_area_budget_usage_conn(conn, parent_goal_id: str,
                                     budget_scope_digest: str) -> tuple[dict[str, Any], dict[str, dict], dict[str, dict]]:
        rows = conn.execute(
            "SELECT event_type, payload_json FROM events WHERE parent_goal_id=? "
            "AND event_type IN ('task_area_budget_reserved', 'task_area_budget_settled')",
            (parent_goal_id,),
        ).fetchall()
        reservations: dict[str, dict] = {}
        settlements: dict[str, dict] = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            if not isinstance(payload, dict):
                raise WorkUnitStoreError("task_area_budget_ledger_invalid")
            if payload.get("budget_scope_digest") != budget_scope_digest:
                continue
            reservation_id = payload.get("reservation_id")
            if not isinstance(reservation_id, str) or not reservation_id:
                raise WorkUnitStoreError("task_area_budget_ledger_invalid")
            target = reservations if row["event_type"] == "task_area_budget_reserved" else settlements
            if reservation_id in target:
                raise WorkUnitStoreError("task_area_budget_ledger_not_unique")
            target[reservation_id] = payload
        if set(settlements) - set(reservations):
            raise WorkUnitStoreError("task_area_budget_settlement_without_reservation")
        usage: dict[str, Any] = {
            "planner_calls": 0, "plan_verifier_calls": 0,
            "executor_invocations": 0, "runtime_seconds": 0.0,
            "unknown_effect": False,
        }
        for reservation_id, reservation in reservations.items():
            counter = reservation.get("counter")
            if counter not in {"planner_calls", "plan_verifier_calls", "executor_invocations"}:
                raise WorkUnitStoreError("task_area_budget_counter_invalid")
            settlement = settlements.get(reservation_id)
            if settlement is None:
                invocations = 0 if reservation.get("recovery_environment_probe") is not None else 1
                runtime = reservation.get("reserved_runtime_seconds")
            else:
                if settlement.get("counter") != counter:
                    raise WorkUnitStoreError("task_area_budget_settlement_binding_mismatch")
                invocations = settlement.get("invocations")
                runtime = settlement.get("runtime_seconds")
                usage["unknown_effect"] = usage["unknown_effect"] or (
                    settlement.get("outcome") == "unknown_effect")
            if (isinstance(invocations, bool) or not isinstance(invocations, int)
                or invocations not in {0, 1}
                or isinstance(runtime, bool) or not isinstance(runtime, (int, float))
                or not math.isfinite(float(runtime)) or float(runtime) < 0):
                raise WorkUnitStoreError("task_area_budget_ledger_invalid")
            usage[counter] += invocations
            usage["runtime_seconds"] += float(runtime)
        return usage, reservations, settlements

    def task_area_budget_usage(self, parent_goal_id: str, *, budget_scope_digest: str) -> dict[str, Any]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        budget_scope_digest = _required_text("budget_scope_digest", budget_scope_digest)
        with self._connect() as conn:
            usage, reservations, settlements = self._task_area_budget_usage_conn(
                conn, parent_goal_id, budget_scope_digest)
        usage["pending_reservations"] = len(set(reservations) - set(settlements))
        return usage

    def reserve_task_area_budget(self, parent_goal_id: str, *, budget_scope_digest: str,
                                 reservation_id: str, counter: str,
                                 budget_limits: Mapping[str, Any],
                                 runtime_seconds: float,
                                 recovery_environment_probe: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Atomically reserve one source-authorized action in the shared event ledger."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_task_area_budget_reservation")
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        budget_scope_digest = _required_text("budget_scope_digest", budget_scope_digest)
        reservation_id = _required_text("reservation_id", reservation_id)
        if counter not in {"planner_calls", "plan_verifier_calls", "executor_invocations"}:
            raise WorkUnitStoreError("task_area_budget_counter_invalid")
        if not isinstance(budget_limits, Mapping):
            raise WorkUnitStoreError("task_area_budget_limits_invalid")
        limits = dict(budget_limits)
        for name in ("planner_calls", "plan_verifier_calls", "executor_invocations"):
            value = limits.get(name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise WorkUnitStoreError("task_area_budget_limits_invalid")
        max_runtime = limits.get("max_runtime_seconds")
        if (isinstance(max_runtime, bool) or not isinstance(max_runtime, (int, float))
            or not math.isfinite(float(max_runtime)) or float(max_runtime) <= 0
            or isinstance(runtime_seconds, bool) or not isinstance(runtime_seconds, (int, float))
            or not math.isfinite(float(runtime_seconds)) or float(runtime_seconds) <= 0):
            raise WorkUnitStoreError("task_area_budget_limits_invalid")
        if float(runtime_seconds) > float(max_runtime):
            raise WorkUnitStoreError("reviewed_task_authority_budget_exhausted:max_runtime_seconds")
        limits_digest = digest_json(limits)
        reservation_key = digest_json({
            "parent_goal_id": parent_goal_id,
            "budget_scope_digest": budget_scope_digest,
            "reservation_id": reservation_id,
        })
        identity = self.identity_port.current()
        if callable(getattr(identity, "as_dict", None)):
            identity = identity.as_dict()
        owner = ProcessIdentity.from_dict(identity)
        if owner is None:
            raise WorkUnitStoreError("task_area_budget_owner_identity_unavailable")
        payload = {
            "schema": "lh-task-area-budget-reservation/v1",
            "reservation_id": reservation_id,
            "budget_scope_digest": budget_scope_digest,
            "counter": counter,
            "budget_limits": limits,
            "budget_limits_digest": limits_digest,
            "reserved_runtime_seconds": float(runtime_seconds),
            "owner_process_identity": owner.as_dict(),
        }
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if recovery_environment_probe is not None:
                if counter != "executor_invocations":
                    raise WorkUnitStoreError("recovery_environment_probe_budget_invalid")
                parent = conn.execute(
                    "SELECT p.parent_goal_id, p.state FROM parent_goals p "
                    "JOIN runs r ON r.parent_goal_id=p.parent_goal_id WHERE r.run_id=?",
                    (recovery_environment_probe["run_id"],)).fetchone()
                if parent is None or parent["parent_goal_id"] != parent_goal_id:
                    raise WorkUnitStoreError("recovery_environment_probe_budget_invalid")
                if parent["state"] != "active":
                    raise WorkUnitStoreError("parent_not_active")
                self._recovery_environment_packet_conn(conn, recovery_environment_probe)
                if (recovery_environment_probe.get("phase") != "checks"
                    or self._recovery_incident_conn(conn, recovery_environment_probe)["failure_count"] < 3):
                    raise WorkUnitStoreError("recovery_environment_probe_budget_invalid")
                payload["recovery_environment_probe"] = dict(recovery_environment_probe)
            usage, reservations, settlements = self._task_area_budget_usage_conn(
                conn, parent_goal_id, budget_scope_digest)
            prior = reservations.get(reservation_id)
            if prior is not None:
                if any(prior.get(key) != value for key, value in payload.items()
                       if key != "owner_process_identity"):
                    raise WorkUnitStoreError("task_area_budget_reservation_binding_mismatch")
                if reservation_id in settlements:
                    if settlements[reservation_id].get("outcome") == "unknown_effect":
                        raise WorkUnitStoreError("reviewed_task_authority_unknown_effect")
                    raise WorkUnitStoreError("reviewed_task_authority_action_already_consumed")
                observation = observe_process_identity(
                    prior.get("owner_process_identity"), identity_port=self.identity_port)
                reason = ("reviewed_task_authority_action_inflight"
                          if observation.status == "alive"
                          else "reviewed_task_authority_unknown_effect")
                raise WorkUnitStoreError(reason)
            if usage["unknown_effect"]:
                raise WorkUnitStoreError("reviewed_task_authority_unknown_effect")
            for pending_id in set(reservations) - set(settlements):
                observation = observe_process_identity(
                    reservations[pending_id].get("owner_process_identity"),
                    identity_port=self.identity_port)
                if observation.status != "alive":
                    raise WorkUnitStoreError("reviewed_task_authority_unknown_effect")
            if usage[counter] + (0 if recovery_environment_probe is not None else 1) > limits[counter]:
                raise WorkUnitStoreError(
                    f"reviewed_task_authority_budget_exhausted:{counter}")
            if usage["runtime_seconds"] + float(runtime_seconds) > float(max_runtime) + 1e-9:
                raise WorkUnitStoreError(
                    "reviewed_task_authority_budget_exhausted:max_runtime_seconds")
            self._append_event_conn(
                conn, event_id="task-area-budget-reserved:" + reservation_key,
                parent_goal_id=parent_goal_id, work_unit_id=None, run_id=None,
                event_type="task_area_budget_reserved", payload=payload,
                created_at=self._now())
            conn.execute("COMMIT")
        return {"status": "reserved", "reservation_id": reservation_id,
                "counter": counter, "runtime_seconds": float(runtime_seconds),
                "budget_limits_digest": limits_digest}

    def settle_task_area_budget(self, parent_goal_id: str, *, budget_scope_digest: str,
                                reservation_id: str, invocations: int,
                                runtime_seconds: float, outcome: str) -> dict[str, Any]:
        """Settle the measured cost; an unsettled reservation stays fully charged."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_task_area_budget_settlement")
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        budget_scope_digest = _required_text("budget_scope_digest", budget_scope_digest)
        reservation_id = _required_text("reservation_id", reservation_id)
        if (isinstance(invocations, bool) or not isinstance(invocations, int)
            or invocations not in {0, 1}
            or isinstance(runtime_seconds, bool) or not isinstance(runtime_seconds, (int, float))
            or not math.isfinite(float(runtime_seconds)) or float(runtime_seconds) < 0
            or outcome not in {"completed", "known_failure", "unknown_effect", "not_invoked"}):
            raise WorkUnitStoreError("task_area_budget_settlement_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            usage, reservations, settlements = self._task_area_budget_usage_conn(
                conn, parent_goal_id, budget_scope_digest)
            reservation = reservations.get(reservation_id)
            if reservation is None:
                raise KeyError(f"unknown task-area budget reservation: {reservation_id}")
            if reservation.get("recovery_environment_probe") is not None:
                if invocations != 0:
                    raise WorkUnitStoreError("recovery_environment_probe_budget_invalid")
            elif (outcome == "not_invoked") != (invocations == 0):
                raise WorkUnitStoreError("task_area_budget_settlement_invalid")
            payload = {
                "schema": "lh-task-area-budget-settlement/v1",
                "reservation_id": reservation_id,
                "budget_scope_digest": budget_scope_digest,
                "counter": reservation["counter"],
                "reservation_digest": digest_json(reservation),
                "invocations": invocations,
                "runtime_seconds": float(runtime_seconds),
                "outcome": outcome,
            }
            prior = settlements.get(reservation_id)
            if prior is not None:
                if prior != payload:
                    raise WorkUnitStoreError("task_area_budget_settlement_already_bound")
            else:
                self._append_event_conn(
                    conn,
                    event_id="task-area-budget-settled:" + digest_json({
                        "parent_goal_id": parent_goal_id,
                        "budget_scope_digest": budget_scope_digest,
                        "reservation_id": reservation_id,
                    }),
                    parent_goal_id=parent_goal_id, work_unit_id=None, run_id=None,
                    event_type="task_area_budget_settled", payload=payload,
                    created_at=self._now())
            conn.execute("COMMIT")
        return {"status": "settled", **payload}

    def ensure_run(self, work_unit_id: str) -> dict[str, Any]:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work_unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work_unit is None:
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            row = self._ensure_run_conn(conn, work_unit, now)
            conn.execute("COMMIT")
        return self._run_row(row) or {}

    create_run = ensure_run

    def get_run(self, run_id: str) -> dict[str, Any]:
        run_id = _required_text("run_id", run_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        value = self._run_row(row)
        if value is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return value

    def get_run_for_work_unit(self, work_unit_id: str) -> dict[str, Any] | None:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        return self._run_row(row)

    def list_runs(self, parent_goal_id: str) -> list[dict[str, Any]]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs WHERE parent_goal_id = ? ORDER BY created_at, run_id", (parent_goal_id,)).fetchall()
        return [self._run_row(row) or {} for row in rows]

    runs = list_runs

    def run_count(self, parent_goal_id: str) -> int:
        return len(self.list_runs(parent_goal_id))

    def get_attempt(self, run_id: str, ordinal: int | None = None) -> dict[str, Any] | None:
        run_id = _required_text("run_id", run_id)
        with self._connect() as conn:
            if ordinal is None:
                row = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1", (run_id,)).fetchone()
            else:
                row = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, int(ordinal))).fetchone()
        return self._attempt_row(row)

    latest_attempt = get_attempt

    def attempts_for_run(self, run_id: str) -> list[dict[str, Any]]:
        run_id = _required_text("run_id", run_id)
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal", (run_id,)).fetchall()
        return [self._attempt_row(row) or {} for row in rows]

    def lease_for(self, work_unit_id: str) -> dict[str, Any] | None:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
        return self._lease_row(row)

    def acquire_lease(self, work_unit_id: str, holder: str, *, seconds: float = 60.0, fence: int | None = None) -> dict[str, Any] | None:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        holder = _required_text("holder", holder)
        if float(seconds) < 0:
            raise WorkUnitStoreError("lease seconds must not be negative")
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work_unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work_unit is None:
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            run = self._ensure_run_conn(conn, work_unit, now)
            current_fence = int(run["fence"])
            if fence is not None and int(fence) != current_fence:
                raise FenceError(f"lease fence mismatch for {work_unit_id}")
            prior = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if prior is not None and float(prior["expires_at"]) > now and prior["holder"] != holder:
                conn.execute("COMMIT")
                return None
            lease_fence = int(prior["fence"]) if prior is not None and prior["holder"] == holder else current_fence
            expires_at = now + float(seconds)
            conn.execute("INSERT OR REPLACE INTO leases(work_unit_id, holder, fence, expires_at) VALUES (?, ?, ?, ?)", (work_unit_id, holder, lease_fence, expires_at))
            conn.execute("COMMIT")
        return {"work_unit_id": work_unit_id, "run_id": run["run_id"], "holder": holder, "fence": lease_fence, "expires_at": expires_at}

    def release_lease(self, work_unit_id: str, holder: str, *, fence: int | None = None) -> bool:
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        holder = _required_text("holder", holder)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if prior is None or prior["holder"] != holder or (fence is not None and int(prior["fence"]) != int(fence)):
                conn.execute("COMMIT")
                return False
            conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (work_unit_id,))
            conn.execute("COMMIT")
        return True

    def start_attempt(
        self,
        work_unit_id: str,
        holder: str,
        *,
        lease_seconds: float = 60.0,
        workspace_ref: str | Path | None = None,
        planning_request: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Start or replay one fenced attempt for a child WorkUnit."""

        work_unit_id = _required_text("work_unit_id", work_unit_id)
        holder = _required_text("holder", holder)
        if float(lease_seconds) < 0:
            raise WorkUnitStoreError("lease seconds must not be negative")
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work_unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work_unit is None:
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            delivery_binding = conn.execute(
                "SELECT 1 FROM delivery_bindings WHERE work_unit_id = ?",
                (work_unit_id,),
            ).fetchone()
            if delivery_binding is None and not self._planning_request_allows_start(
                conn, work_unit_id, planning_request
            ):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("delivery_unit_binding_missing")
            run = self._ensure_run_conn(conn, work_unit, now)
            if run["state"] in {"integrated", "stopped"} or work_unit["state"] in {"integrated", "stopped"}:
                latest = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1", (run["run_id"],)).fetchone()
                conn.execute("COMMIT")
                result = self._attempt_row(latest) or {}
                result.update({"work_unit_id": work_unit_id, "run_id": run["run_id"], "status": work_unit["state"], "reused": True})
                return result

            binding_budget = self._delivery_retry_budget_conn(conn, work_unit_id)
            if binding_budget is not None and int(run["attempts"]) >= binding_budget:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("delivery_retry_budget_exhausted")

            prior_lease = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            lease_live = prior_lease is not None and float(prior_lease["expires_at"]) > now
            if lease_live and prior_lease["holder"] != holder:
                raise LeaseBusyError(work_unit_id, prior_lease["holder"])
            if prior_lease is not None and not lease_live:
                conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (work_unit_id,))
                prior_lease = None

            if run["state"] == "running":
                active_attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND state = 'running' ORDER BY ordinal DESC LIMIT 1", (run["run_id"],)).fetchone()
                if prior_lease is not None and active_attempt is not None and int(active_attempt["fence"]) == int(prior_lease["fence"]) and active_attempt["holder"] == holder:
                    expires_at = now + float(lease_seconds)
                    conn.execute("UPDATE leases SET expires_at = ? WHERE work_unit_id = ?", (expires_at, work_unit_id))
                    conn.execute("COMMIT")
                    result = self._attempt_row(active_attempt) or {}
                    result.update({"work_unit_id": work_unit_id, "run_id": run["run_id"], "status": "running", "reused": True, "expires_at": expires_at})
                    return result
                if active_attempt is not None:
                    conn.execute("UPDATE attempts SET state = 'interrupted', finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running'", (now, run["run_id"], int(active_attempt["ordinal"])))
                next_fence = int(run["fence"]) + 1
                conn.execute("UPDATE runs SET state = 'retry_pending', fence = ?, updated_at = ? WHERE run_id = ?", (next_fence, now, run["run_id"]))
                conn.execute("UPDATE work_units SET state = 'retry_pending', updated_at = ? WHERE work_unit_id = ?", (now, work_unit_id))
                run = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run["run_id"],)).fetchone()

            if run["state"] not in {"queued", "retry_pending"}:
                raise WorkUnitStoreError(f"run is not startable from {run['state']}")
            if self._phase_policy_conn(conn) is not None:
                raise WorkUnitStoreError("phase_job_legacy_mixed_admission_unsupported")
            ordinal = int(run["attempts"]) + 1
            fence = int(run["fence"]) + 1
            workspace = str(workspace_ref or work_unit["worktree"] or f"work-unit:{work_unit_id}")
            expires_at = now + float(lease_seconds)
            conn.execute("UPDATE runs SET state = 'running', attempts = ?, fence = ?, updated_at = ? WHERE run_id = ?", (ordinal, fence, now, run["run_id"]))
            conn.execute(
                "INSERT INTO attempts(run_id, ordinal, state, holder, fence, workspace_ref, receipt_ref, receipt_digest, created_at, finished_at) VALUES (?, ?, 'running', ?, ?, ?, NULL, NULL, ?, NULL)",
                (run["run_id"], ordinal, holder, fence, workspace, now),
            )
            conn.execute("UPDATE work_units SET state = 'running', updated_at = ? WHERE work_unit_id = ?", (now, work_unit_id))
            conn.execute("INSERT OR REPLACE INTO leases(work_unit_id, holder, fence, expires_at) VALUES (?, ?, ?, ?)", (work_unit_id, holder, fence, expires_at))
            self._append_event_conn(
                conn,
                event_id=f"attempt-started:{run['run_id']}:{ordinal}",
                parent_goal_id=work_unit["parent_goal_id"],
                work_unit_id=work_unit_id,
                run_id=run["run_id"],
                event_type="attempt_started",
                payload={"attempt": ordinal, "holder": holder, "fence": fence, "workspace_ref": workspace},
                created_at=now,
            )
            conn.execute("COMMIT")
        return {
            "schema": SCHEMA,
            "work_unit_id": work_unit_id,
            "parent_goal_id": work_unit["parent_goal_id"],
            "run_id": run["run_id"],
            "attempt": ordinal,
            "ordinal": ordinal,
            "state": "running",
            "status": "dispatched",
            "holder": holder,
            "fence": fence,
            "workspace_ref": workspace,
            "expires_at": expires_at,
            "reused": False,
        }

    @staticmethod
    def _planning_request_allows_start(
        conn: sqlite3.Connection,
        work_unit_id: str,
        request: Mapping[str, Any] | None,
    ) -> bool:
        """Allow only a persisted, typed PlanNodeController control request."""
        if not isinstance(request, Mapping):
            return False
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        if engine.verify_planning_request(request).get("verdict") != "GREEN":
            return False
        binding = request.get("binding")
        if not isinstance(binding, Mapping):
            return False
        identity = conn.execute(
            "SELECT w.node_id, w.node_kind, w.producer, p.goal_id, p.goal_revision FROM work_units w JOIN parent_goals p ON p.parent_goal_id = w.parent_goal_id WHERE w.work_unit_id = ?",
            (work_unit_id,),
        ).fetchone()
        if identity is None:
            return False
        if (
            binding.get("work_unit_id") != work_unit_id
            or binding.get("node_kind") != "planning"
            or binding.get("producer") != "PlanNodeController"
            or identity["node_kind"] != "planning"
            or identity["producer"] != "PlanNodeController"
            or binding.get("goal_id") != identity["goal_id"]
            or binding.get("goal_revision") != int(identity["goal_revision"])
            or binding.get("node_id") != identity["node_id"]
        ):
            return False
        plan = binding.get("plan_verdict")
        if not isinstance(plan, Mapping) or plan.get("verdict") != "GREEN" or plan.get("schema") != engine.PLAN_SCHEMA:
            return False
        if (
            plan.get("goal_id") != identity["goal_id"]
            or plan.get("goal_revision") != int(identity["goal_revision"])
            or plan.get("node_id") != identity["node_id"]
            or plan.get("unit_id") != work_unit_id
            or plan.get("controller_mode") != "typed-planning-control"
        ):
            return False
        verifier = plan.get("verifier_receipt")
        if (
            not isinstance(verifier, Mapping)
            or verifier.get("principal") in {None, binding.get("producer")}
            or verifier.get("read_only") is not True
            or verifier.get("source_write") is not False
            or verifier.get("verdict") != "GREEN"
        ):
            return False
        supplied = plan.get("plan_verdict_digest")
        if not isinstance(supplied, str) or supplied != engine.digest_json({key: value for key, value in plan.items() if key != "plan_verdict_digest"}):
            return False
        row = conn.execute(
            "SELECT status, work_unit_id, input_digest, request_json FROM planning_requests WHERE request_id = ?",
            (request.get("request_id"),),
        ).fetchone()
        if row is None:
            return False
        try:
            persisted_request = json.loads(row["request_json"])
        except (TypeError, json.JSONDecodeError):
            return False
        if (
            not isinstance(persisted_request, Mapping)
            or persisted_request.get("schema") != "lh-delivery-planning-request/v1"
        ):
            return False
        return (
            row["status"] == "pending"
            and row["work_unit_id"] == work_unit_id
            and row["input_digest"] == request.get("input_digest")
        )

    begin_attempt = start_attempt

    def reconcile_expired(self) -> list[dict[str, Any]]:
        """Move abandoned running attempts to retry_pending without a new run."""

        now = self._now()
        recovered: list[dict[str, Any]] = []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT w.*, r.run_id, r.state AS run_state, r.fence AS run_fence FROM work_units w JOIN runs r ON r.work_unit_id = w.work_unit_id LEFT JOIN leases l ON l.work_unit_id = w.work_unit_id WHERE w.state = 'running' AND r.state = 'running' AND (l.work_unit_id IS NULL OR l.expires_at <= ?)",
                (now,),
            ).fetchall()
            for row in rows:
                attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND state = 'running' ORDER BY ordinal DESC LIMIT 1", (row["run_id"],)).fetchone()
                if attempt is None:
                    continue
                next_fence = int(row["run_fence"]) + 1
                conn.execute("UPDATE attempts SET state = 'interrupted', finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running'", (now, row["run_id"], int(attempt["ordinal"])))
                conn.execute("UPDATE runs SET state = 'retry_pending', fence = ?, updated_at = ? WHERE run_id = ? AND state = 'running'", (next_fence, now, row["run_id"]))
                conn.execute("UPDATE work_units SET state = 'retry_pending', updated_at = ? WHERE work_unit_id = ?", (now, row["work_unit_id"]))
                conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (row["work_unit_id"],))
                result = {"work_unit_id": row["work_unit_id"], "run_id": row["run_id"], "attempt": int(attempt["ordinal"]), "status": "retry_pending", "recovered_from": "expired_lease"}
                recovered.append(result)
                self._append_event_conn(
                    conn,
                    event_id=f"attempt-reconciled:{row['run_id']}:{attempt['ordinal']}:{next_fence}",
                    parent_goal_id=row["parent_goal_id"],
                    work_unit_id=row["work_unit_id"],
                    run_id=row["run_id"],
                    event_type="attempt_reconciled",
                    payload=result,
                    created_at=now,
                )
            conn.execute("COMMIT")
        return recovered

    recover_expired = reconcile_expired
    reconcile_startup = reconcile_expired

    def _resolve_work_unit_id(self, work_unit_id: str | None, run_id: str | None) -> str:
        if work_unit_id is not None:
            return _required_text("work_unit_id", work_unit_id)
        if run_id is None:
            raise WorkUnitStoreError("work_unit_id or run_id is required")
        run_id = _required_text("run_id", run_id)
        with self._connect() as conn:
            row = conn.execute("SELECT work_unit_id FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return row["work_unit_id"]

    def finish_attempt(
        self,
        work_unit_id: str | None = None,
        *,
        run_id: str | None = None,
        ordinal: int | None = None,
        holder: str,
        fence: int,
        state: str = "verified",
        receipt_ref: str | None = None,
        receipt_digest: str | None = None,
    ) -> bool:
        """Finish the current attempt only when holder and fence still match."""

        work_unit_id = self._resolve_work_unit_id(work_unit_id, run_id)
        holder = _required_text("holder", holder)
        if state not in {"verified", "retry_pending", "stopped"}:
            raise WorkUnitStoreError(f"invalid terminal attempt state: {state}")
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work_unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work_unit is None or run is None:
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            if state == "verified":
                delivery = conn.execute("SELECT verdict_json FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
                if not self._delivery_verdict_allows_terminal(conn, work_unit_id):
                    conn.execute("COMMIT")
                    return False
            if ordinal is None:
                attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1", (run["run_id"],)).fetchone()
            else:
                attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (run["run_id"], int(ordinal))).fetchone()
            lease = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            valid = (
                attempt is not None and attempt["state"] == "running" and run["state"] == "running"
                and int(attempt["fence"]) == int(fence) and int(run["fence"]) == int(fence)
                and attempt["holder"] == holder and lease is not None and lease["holder"] == holder and int(lease["fence"]) == int(fence)
            )
            if not valid:
                conn.execute("COMMIT")
                return False
            conn.execute("UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (state, receipt_ref, receipt_digest, now, run["run_id"], int(attempt["ordinal"]), int(fence)))
            conn.execute("UPDATE runs SET state = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (state, now, run["run_id"], int(fence)))
            conn.execute("UPDATE work_units SET state = ?, updated_at = ? WHERE work_unit_id = ? AND state = 'running'", (state, now, work_unit_id))
            conn.execute("DELETE FROM leases WHERE work_unit_id = ? AND holder = ? AND fence = ?", (work_unit_id, holder, int(fence)))
            self._append_event_conn(
                conn,
                event_id=f"attempt-finished:{run['run_id']}:{attempt['ordinal']}:{fence}",
                parent_goal_id=work_unit["parent_goal_id"],
                work_unit_id=work_unit_id,
                run_id=run["run_id"],
                event_type="attempt_finished",
                payload={"attempt": int(attempt["ordinal"]), "state": state, "fence": int(fence), "receipt_ref": receipt_ref, "receipt_digest": receipt_digest},
                created_at=now,
            )
            conn.execute("COMMIT")
        return True

    def mark_integrated(
        self,
        work_unit_id: str | None = None,
        *,
        run_id: str | None = None,
        holder: str | None = None,
        fence: int | None = None,
        receipt_ref: str | None = None,
        receipt_digest: str | None = None,
    ) -> bool:
        """Advance one verified/running child to integrated with its fence."""

        work_unit_id = self._resolve_work_unit_id(work_unit_id, run_id)
        if holder is not None:
            holder = _required_text("holder", holder)
        now = self._now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            work_unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if work_unit is None or run is None:
                raise KeyError(f"unknown work_unit_id: {work_unit_id}")
            delivery = conn.execute("SELECT verdict_json FROM delivery_bindings WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            if not self._delivery_verdict_allows_terminal(conn, work_unit_id):
                conn.execute("COMMIT")
                return False
            if work_unit["state"] == "integrated" and run["state"] == "integrated":
                conn.execute("COMMIT")
                return True
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1", (run["run_id"],)).fetchone()
            if attempt is None or attempt["state"] not in {"running", "verified"}:
                conn.execute("COMMIT")
                return False
            if fence is not None and int(attempt["fence"]) != int(fence):
                conn.execute("COMMIT")
                return False
            if attempt["state"] == "running":
                lease = conn.execute("SELECT * FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
                if holder is None or fence is None or lease is None or lease["holder"] != holder or int(lease["fence"]) != int(fence) or int(run["fence"]) != int(fence):
                    conn.execute("COMMIT")
                    return False
                effective_fence = int(fence)
            else:
                effective_fence = int(attempt["fence"])
            if self._delivery_dependencies_conn(conn, run):
                raise WorkUnitStoreError("delivery_dependencies_not_integrated")
            conn.execute("UPDATE attempts SET state = 'integrated', receipt_ref = COALESCE(?, receipt_ref), receipt_digest = COALESCE(?, receipt_digest), finished_at = COALESCE(finished_at, ?) WHERE run_id = ? AND ordinal = ?", (receipt_ref, receipt_digest, now, run["run_id"], int(attempt["ordinal"])))
            conn.execute("UPDATE runs SET state = 'integrated', updated_at = ? WHERE run_id = ?", (now, run["run_id"]))
            conn.execute("UPDATE work_units SET state = 'integrated', updated_at = ? WHERE work_unit_id = ?", (now, work_unit_id))
            conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (work_unit_id,))
            self._append_event_conn(
                conn,
                event_id=f"work-unit-integrated:{run['run_id']}:{effective_fence}",
                parent_goal_id=work_unit["parent_goal_id"],
                work_unit_id=work_unit_id,
                run_id=run["run_id"],
                event_type="work_unit_integrated",
                payload={"run_id": run["run_id"], "attempt": int(attempt["ordinal"]), "fence": effective_fence},
                created_at=now,
            )
            conn.execute("COMMIT")
        return True

    integrate_work_unit = mark_integrated
    complete_work_unit = mark_integrated

    def get_dispatch_consumption(self, dispatch_key: str) -> dict[str, Any] | None:
        dispatch_key = _required_text("dispatch_key", dispatch_key)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?",
                (dispatch_key,),
            ).fetchone()
        return self._dispatch_row(row)

    def consume_dispatch(
        self,
        envelope: dict[str, Any],
        *,
        parent_goal_id: str,
        goal_id: str,
        goal_revision: int,
        node_id: str,
        work_unit_id: str,
        worker_id: str,
        base_sha: str,
        read_set: Iterable[str | Path] | str | Path | None,
        write_set: Iterable[str | Path] | str | Path | None,
        worktree: str | Path | None,
        branch: str | None,
        state_root: str | Path | None,
        holder: str,
        lease_seconds: float = 60.0,
        now: float | None = None,
        delivery_binding: Mapping[str, Any] | None = None,
        dependencies: Iterable[str] = (),
        delivery_after: list[str] | None = None,
        enforce_scope_conflicts: bool = False,
        max_active_workers: int | None = None,
        phase_context: dict[str, Any] | None = None,
        resource_requirements: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Consume one successor envelope into a durable LH WorkUnit.

        The dispatch ledger, parent projection, WorkUnit, Run, and first
        ``ready`` Attempt are committed under one SQLite transaction.
        Executor ownership is a separate ``accept_executor_dispatch`` step;
        queue admission alone never creates a running Attempt.  Replaying an
        identical envelope returns the recorded receipt before touching the
        scheduler rows, so timer retries and process restarts cannot create a
        second Run or Attempt.
        """
        if not isinstance(envelope, dict):
            raise WorkUnitStoreError("dispatch envelope must be an object")
        dispatch_key = _required_text("dispatch_key", envelope.get("dispatch_key"))
        supplied_envelope_digest = _required_text("envelope_digest", envelope.get("envelope_digest"))
        envelope_body = dict(envelope)
        envelope_body.pop("envelope_digest", None)
        if supplied_envelope_digest != digest_json(envelope_body):
            raise WorkUnitStoreError("dispatch envelope digest mismatch")
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        goal_id = _required_text("goal_id", goal_id)
        if parent_goal_id != goal_id:
            raise WorkUnitStoreError("successor dispatch may not create a second Goal")
        if isinstance(goal_revision, bool) or not isinstance(goal_revision, int) or goal_revision < 1:
            raise WorkUnitStoreError("goal_revision must be positive")
        node_id = _required_text("node_id", node_id)
        work_unit_id = _required_text("work_unit_id", work_unit_id)
        worker_id = _required_text("worker_id", worker_id)
        base_sha = _required_text("base_sha", base_sha)
        holder = _required_text("holder", holder)
        if float(lease_seconds) < 0:
            raise WorkUnitStoreError("lease seconds must not be negative")
        if not isinstance(delivery_binding, Mapping):
            raise WorkUnitStoreError("delivery_unit_binding_missing")
        try:
            from . import delivery_contract as engine
        except ImportError:
            import delivery_contract as engine  # type: ignore
        raw_contract = delivery_binding.get("contract")
        raw_plan = delivery_binding.get("plan_verdict")
        raw_binding = delivery_binding.get("binding", delivery_binding)
        if not isinstance(raw_contract, Mapping) or not isinstance(raw_plan, Mapping) or not isinstance(raw_binding, Mapping):
            raise WorkUnitStoreError("delivery_unit_binding_invalid")
        try:
            resolved_contract = engine.validate_contract(raw_contract)
        except Exception as exc:
            raise WorkUnitStoreError("delivery_unit_contract_invalid") from exc
        if (
            resolved_contract["goal"]["id"] != goal_id
            or resolved_contract["goal"]["revision"] != goal_revision
            or resolved_contract["node"]["id"] != node_id
            or raw_binding.get("status") != "bound"
            or raw_binding.get("contract_digest") != resolved_contract["contract_digest"]
            or raw_binding.get("unit_id") != resolved_contract["unit_id"]
            or raw_binding.get("plan_verdict_digest") != raw_plan.get("plan_verdict_digest")
            or engine.verify_plan_verdict(raw_plan, resolved_contract).get("verdict") != "GREEN"
        ):
            raise WorkUnitStoreError("delivery_unit_binding_identity_mismatch")
        moment = self._now() if now is None else float(now)
        prepared = self._prepare_work_definition(
            {
                "work_unit_id": work_unit_id,
                "parent_goal_id": parent_goal_id,
                "node_id": node_id,
                "node_kind": resolved_contract["node"]["kind"],
                "producer": "ParallelScheduler",
                "worker_id": worker_id,
                "base_sha": base_sha,
                "dependencies": dependencies,
                **({"delivery_after": delivery_after} if delivery_after is not None else {}),
                "read_set": read_set,
                "write_set": write_set,
                "worktree": None if worktree is None else str(worktree),
                "branch": branch,
                "state_root": None if state_root is None else str(state_root),
            },
            parent_goal_id,
        )

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior_dispatch = conn.execute(
                "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?",
                (dispatch_key,),
            ).fetchone()
            if prior_dispatch is not None:
                if prior_dispatch["envelope_digest"] != supplied_envelope_digest:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("dispatch key is already bound to different envelope")
                prior_work = self._work_row(conn.execute(
                    "SELECT * FROM work_units WHERE work_unit_id=?",
                    (prior_dispatch["work_unit_id"],)).fetchone())
                # Replay does not rebind legacy caller metadata; delivery edges remain immutable.
                if (prior_work is None or prior_dispatch["work_unit_id"] != work_unit_id
                    or ("delivery_after" in prior_work) != ("delivery_after" in prepared)
                    or prior_work.get("delivery_after") != prepared.get("delivery_after")):
                    raise DuplicateWorkUnitError("work_unit_id is already bound to different inputs")
                old_receipt = json.loads(prior_dispatch["receipt_json"])
                if old_receipt.get("resource_requirements") != resource_requirements:
                    raise DuplicateWorkUnitError("dispatch resource requirements changed")
                conn.execute("COMMIT")
                return self._dispatch_row(prior_dispatch) or {}

            resource_material = self._phase_admission_conn(
                conn, work_unit_id, phase_context, envelope, resource_requirements)
            parent = conn.execute(
                "SELECT * FROM parent_goals WHERE parent_goal_id = ?",
                (parent_goal_id,),
            ).fetchone()
            if parent is None:
                conn.execute(
                    "INSERT INTO parent_goals(parent_goal_id, goal_id, goal_revision, base_sha, dependencies_json, state, created_at, updated_at) VALUES (?, ?, ?, ?, '[]', 'active', ?, ?)",
                    (parent_goal_id, goal_id, goal_revision, base_sha, moment, moment),
                )
            else:
                if (
                    parent["goal_id"] != goal_id
                    or int(parent["goal_revision"]) != goal_revision
                    or parent["base_sha"] != base_sha
                ):
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("existing Goal projection mismatch")

            existing_row = conn.execute(
                "SELECT * FROM work_units WHERE work_unit_id = ?",
                (work_unit_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._work_row(existing_row) or {}
                if self._immutable_work_values(existing) != self._immutable_work_values(prepared):
                    conn.execute("ROLLBACK")
                    raise DuplicateWorkUnitError("work_unit_id is already bound to different inputs")
                if existing["parent_goal_id"] != parent_goal_id or existing["node_id"] != node_id:
                    conn.execute("ROLLBACK")
                    raise DuplicateWorkUnitError("successor WorkUnit identity mismatch")
                work_unit = existing_row
            else:
                node_row = conn.execute(
                    "SELECT work_unit_id FROM work_units WHERE parent_goal_id = ? AND node_id = ?",
                    (parent_goal_id, node_id),
                ).fetchone()
                if node_row is not None:
                    conn.execute("ROLLBACK")
                    raise DuplicateWorkUnitError(f"node_id is already registered: {node_id}")
                existing_definitions = self._existing_work_definitions(conn, parent_goal_id)
                self._validate_graph_records([*existing_definitions, prepared])
                conn.execute(
                    "INSERT INTO work_units(work_unit_id, parent_goal_id, node_id, node_kind, producer, worker_id, base_sha, dependencies_json, read_set_json, write_set_json, worktree, branch, state_root, delivery_after_json, state, run_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                    (
                        prepared["work_unit_id"], prepared["parent_goal_id"], prepared["node_id"], prepared["node_kind"], prepared["producer"], prepared["worker_id"], prepared["base_sha"],
                        json.dumps(prepared["dependencies"]), json.dumps(prepared["read_set"]), json.dumps(prepared["write_set"]),
                        prepared["worktree"], prepared["branch"], prepared["state_root"],
                        json.dumps(prepared["delivery_after"]) if "delivery_after" in prepared else None, moment, moment,
                    ),
                )
                work_unit = conn.execute(
                    "SELECT * FROM work_units WHERE work_unit_id = ?",
                    (work_unit_id,),
                ).fetchone()

            if work_unit is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("successor WorkUnit missing after admission")
            if enforce_scope_conflicts:
                active_others = conn.execute(
                    "SELECT * FROM work_units WHERE parent_goal_id = ? AND work_unit_id != ? AND state IN ('ready', 'running', 'verified')",
                    (parent_goal_id, work_unit_id),
                ).fetchall()
                if max_active_workers not in {1, 2, 3} or len(active_others) >= max_active_workers:
                    raise WorkUnitStoreError("task_area_capacity_exhausted")
                for other in active_others:
                    if read_write_conflicts(prepared, self._work_row(other) or {}):
                        raise WorkUnitStoreError("task_area_read_write_overlap")
            run = self._ensure_run_conn(conn, work_unit, moment)
            latest = conn.execute(
                "SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1",
                (run["run_id"],),
            ).fetchone()
            prior_lease = conn.execute(
                "SELECT * FROM leases WHERE work_unit_id = ?",
                (work_unit_id,),
            ).fetchone()
            lease_live = prior_lease is not None and float(prior_lease["expires_at"]) > moment
            if lease_live and prior_lease["holder"] != holder:
                conn.execute("ROLLBACK")
                raise LeaseBusyError(work_unit_id, prior_lease["holder"])

            attempts_created = 0
            if latest is not None and latest["state"] in {"running", "interrupted", "retry_pending"}:
                # Queue admission is not executor ownership.  A legacy row
                # written by the former consumer is downgraded to ready unless
                # its dispatch receipt already carries executor evidence (that
                # replay returned above).  The same fenced Attempt is reused;
                # a retry or restart never increments its ordinal.
                fence = int(latest["fence"])
                workspace = str(worktree or work_unit["worktree"] or f"work-unit:{work_unit_id}")
                conn.execute(
                    "UPDATE attempts SET state = 'ready', holder = ?, workspace_ref = ?, finished_at = NULL WHERE run_id = ? AND ordinal = ?",
                    (holder, workspace, run["run_id"], int(latest["ordinal"])),
                )
                conn.execute(
                    "UPDATE runs SET state = 'queued', updated_at = ? WHERE run_id = ?",
                    (moment, run["run_id"]),
                )
                conn.execute(
                    "UPDATE work_units SET state = 'ready', updated_at = ? WHERE work_unit_id = ?",
                    (moment, work_unit_id),
                )
                conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (work_unit_id,))
                latest = conn.execute(
                    "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
                    (run["run_id"], int(latest["ordinal"])),
                ).fetchone()
            elif latest is not None:
                # The durable dispatch row is the idempotency boundary.  This
                # branch is a replay of a queued Attempt and deliberately does
                # not create a lease or claim executor ownership.
                fence = int(latest["fence"])
                expires_at = None
            else:
                ordinal = int(run["attempts"]) + 1
                fence = int(run["fence"]) + 1
                workspace = str(worktree or work_unit["worktree"] or f"work-unit:{work_unit_id}")
                conn.execute(
                    "UPDATE runs SET state = 'queued', attempts = ?, fence = ?, updated_at = ? WHERE run_id = ?",
                    (ordinal, fence, moment, run["run_id"]),
                )
                conn.execute(
                    "INSERT INTO attempts(run_id, ordinal, state, holder, fence, workspace_ref, receipt_ref, receipt_digest, created_at, finished_at) VALUES (?, ?, 'ready', ?, ?, ?, NULL, NULL, ?, NULL)",
                    (run["run_id"], ordinal, holder, fence, workspace, moment),
                )
                conn.execute(
                    "UPDATE work_units SET state = 'ready', updated_at = ? WHERE work_unit_id = ?",
                    (moment, work_unit_id),
                )
                self._append_event_conn(
                    conn,
                    event_id=f"attempt-ready:{run['run_id']}:{ordinal}",
                    parent_goal_id=parent_goal_id,
                    work_unit_id=work_unit_id,
                    run_id=run["run_id"],
                    event_type="attempt_ready",
                    payload={"attempt": ordinal, "holder": holder, "fence": fence, "workspace_ref": workspace, "source": "successor_queue_admission"},
                    created_at=moment,
                )
                attempts_created = 1
                latest = conn.execute(
                    "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
                    (run["run_id"], ordinal),
                ).fetchone()

            if latest is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("successor Attempt missing after admission")
            attempt_number = int(latest["ordinal"])
            receipt_body = {
                "schema": DISPATCH_RECEIPT_SCHEMA,
                "status": "consumed",
                "dispatch_key": dispatch_key,
                "envelope_digest": supplied_envelope_digest,
                "parent_goal_id": parent_goal_id,
                "goal_id": goal_id,
                "goal_revision": goal_revision,
                "node_id": node_id,
                "work_unit_id": work_unit_id,
                "run_id": run["run_id"],
                "attempt": attempt_number,
                "fence": fence,
                "scheduler": "lh-successor-dispatch-consumer",
                "scheduler_ready": True,
                "queue_state": "ready",
                "attempt_state": "ready",
                "executor_status": "ready",
                "executor_invocations": 0,
                "executor_launches": 0,
                "executor_failure_receipts": [],
                "retry_count": 0,
                "packet_path": envelope.get("packet_path"),
                "packet_digest": envelope.get("packet_digest"),
                "transition_digest": envelope.get("transition_digest"),
                "provider_invocations": 0,
                "manual_prompts": 0,
                "queue_db": str(self.db_path),
                "consumed_at": moment,
            }
            if phase_context is not None:
                receipt_body["admission_policy_ref"] = phase_context["policy_ref"]
            if resource_requirements is not None:
                receipt_body.update(resource_requirements=resource_requirements, resource_material=resource_material)
            receipt_digest = digest_json(receipt_body)
            receipt = {**receipt_body, "receipt_digest": receipt_digest}
            conn.execute(
                "INSERT INTO dispatch_consumptions(dispatch_key, envelope_digest, parent_goal_id, goal_id, goal_revision, node_id, work_unit_id, run_id, attempt, status, envelope_json, receipt_json, receipt_digest, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'consumed', ?, ?, ?, ?, ?)",
                (
                    dispatch_key, supplied_envelope_digest, parent_goal_id, goal_id, goal_revision, node_id,
                    work_unit_id, run["run_id"], attempt_number, json.dumps(envelope, ensure_ascii=False, sort_keys=True),
                    json.dumps(receipt, ensure_ascii=False, sort_keys=True), receipt_digest, moment, moment,
                ),
            )
            self._append_event_conn(
                conn,
                event_id=f"dispatch-consumed:{dispatch_key}",
                parent_goal_id=parent_goal_id,
                work_unit_id=work_unit_id,
                run_id=run["run_id"],
                event_type="successor_dispatch_consumed",
                payload={"dispatch_key": dispatch_key, "envelope_digest": supplied_envelope_digest, "receipt_digest": receipt_digest, "attempt": attempt_number},
                created_at=moment,
            )
            conn.execute("COMMIT")

        result = {
            "schema": DISPATCH_CONSUMPTION_SCHEMA,
            "status": "consumed",
            "dispatch_key": dispatch_key,
            "envelope_digest": supplied_envelope_digest,
            "parent_goal_id": parent_goal_id,
            "goal_id": goal_id,
            "goal_revision": goal_revision,
            "node_id": node_id,
            "work_unit_id": work_unit_id,
            "run_id": run["run_id"],
            "attempt": attempt_number,
            "receipt": receipt,
            "receipt_digest": receipt_digest,
            "work_unit": self.get_work_unit(work_unit_id),
            "run": self.get_run(run["run_id"]),
            "attempt_record": self.get_attempt(run["run_id"], attempt_number),
            "reused": False,
            "runs_created": 1 if int(run["attempts"]) == 0 else 0,
            "attempts_created": attempts_created,
        }
        if delivery_binding is not None:
            result["delivery_binding"] = self.bind_delivery_contract(
                work_unit_id,
                binding=delivery_binding.get("binding", delivery_binding) if isinstance(delivery_binding, Mapping) else delivery_binding,
                contract=delivery_binding.get("contract") if isinstance(delivery_binding, Mapping) else None,
                plan_verdict=delivery_binding.get("plan_verdict") if isinstance(delivery_binding, Mapping) else None,
                run_id=run["run_id"],
            )
        return result

    def accept_executor_dispatch(
        self,
        dispatch_key: str,
        executor_receipt: Mapping[str, Any],
        *,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Promote one queued Attempt only after executor evidence exists.

        ``consume_dispatch`` is intentionally queue-only.  This method is the
        second durable boundary: it verifies the receipt binding, records the
        executor evidence, and only then acquires the WorkUnit lease and moves
        the Attempt/Run/WorkUnit to ``running``.  Replaying the same receipt is
        idempotent and reports ``invoked=False``.
        """
        dispatch_key = _required_text("dispatch_key", dispatch_key)
        if not isinstance(executor_receipt, Mapping):
            raise WorkUnitStoreError("executor receipt must be an object")
        supplied_digest = _required_text("executor_receipt_digest", executor_receipt.get("receipt_digest"))
        canonical_executor_receipt = copy.deepcopy(dict(executor_receipt))
        # These are transport return flags, not bytes covered by receipt_digest.
        canonical_executor_receipt.pop("reused", None)
        canonical_executor_receipt.pop("invoked", None)
        receipt_body = json.loads(json.dumps(canonical_executor_receipt, ensure_ascii=False, sort_keys=True))
        receipt_body.pop("receipt_digest", None)
        if supplied_digest != digest_json(receipt_body):
            raise WorkUnitStoreError("executor receipt digest mismatch")
        if canonical_executor_receipt.get("schema") not in {"lh-successor-executor-receipt/v1", "lh-successor-executor-receipt/v2"} or canonical_executor_receipt.get("status") != "accepted":
            raise WorkUnitStoreError("executor receipt status invalid")
        executor_id = _required_text("executor_id", canonical_executor_receipt.get("executor_id"))
        task_receipt_path = _required_text("task_receipt_path", canonical_executor_receipt.get("task_receipt_path"))
        task_receipt_digest = _required_text("task_receipt_digest", canonical_executor_receipt.get("task_receipt_digest"))
        if not task_receipt_digest.startswith("sha256:"):
            raise WorkUnitStoreError("task receipt digest invalid")
        if canonical_executor_receipt.get("invocation_count") != 1:
            raise WorkUnitStoreError("executor invocation count invalid")
        if float(lease_seconds) < 0:
            raise WorkUnitStoreError("lease seconds must not be negative")
        moment = self._now() if now is None else float(now)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            dispatch = conn.execute(
                "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?",
                (dispatch_key,),
            ).fetchone()
            if dispatch is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown dispatch_key: {dispatch_key}")
            try:
                envelope = json.loads(dispatch["envelope_json"])
                stored_receipt = json.loads(dispatch["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not JSON") from exc
            if not isinstance(envelope, dict) or not isinstance(stored_receipt, dict):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not an object")
            prior_executor = stored_receipt.get("executor_receipt")
            budget_row = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                                     (dispatch["goal_id"], dispatch["goal_revision"])).fetchone()
            trusted_receipt = canonical_executor_receipt.get("schema") == "lh-successor-executor-receipt/v2"
            # The durable Goal budget is the mode authority. Receipt self-schema
            # cannot downgrade a trusted Goal, even on its first acceptance.
            if trusted_receipt != (budget_row is not None):
                raise WorkUnitStoreError("trusted_executor_mode_mismatch")
            if not trusted_receipt and any(name in canonical_executor_receipt for name in (
                "policy_digest", "execution_binding_digest", "execution_assurance", "provider_execution", "cli_launches"
            )):
                raise WorkUnitStoreError("trusted_executor_mode_mismatch")
            if trusted_receipt:
                if __package__:
                    from .execution_fence_trusted import validate_trusted_assurance, ExecutionFenceUnavailable
                else:
                    from execution_fence_trusted import validate_trusted_assurance, ExecutionFenceUnavailable
                try:
                    assurance = validate_trusted_assurance(canonical_executor_receipt.get("execution_assurance"))
                except ExecutionFenceUnavailable:
                    raise WorkUnitStoreError("trusted_executor_assurance_mismatch") from None
                continued_receipt = trusted_continuation.executor_binding(conn, dispatch, canonical_executor_receipt)
                if ((json.loads(budget_row["budget_json"])["policy_digest"] != assurance["policy_digest"] and not continued_receipt)
                        or (budget_row["execution_binding_digest"] != assurance["execution_binding_digest"] and not continued_receipt)
                        or (canonical_executor_receipt.get("continuation_authority_digest") is not None and not continued_receipt)
                        or any(canonical_executor_receipt.get(name) != assurance[name]
                               for name in ("policy_digest", "execution_binding_digest"))
                        or canonical_executor_receipt.get("provider_invocations") is not None
                        or canonical_executor_receipt.get("cli_launches") != 1
                        or "execution_fence" in canonical_executor_receipt):
                    raise WorkUnitStoreError("trusted_executor_assurance_mismatch")
            if prior_executor is not None:
                prior_digest = prior_executor.get("receipt_digest") if isinstance(prior_executor, Mapping) else None
                if prior_digest != supplied_digest:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("dispatch key is already bound to different executor evidence")
                conn.execute("COMMIT")
                result = self._dispatch_row(dispatch) or {}
                result.update({
                    "executor_receipt": copy.deepcopy(dict(prior_executor)),
                    "executor_invocations": 0,
                    "executor_status": "accepted",
                    "invoked": False,
                })
                return result

            prior_launches = stored_receipt.get("executor_launches", stored_receipt.get("executor_invocations", 0))
            if isinstance(prior_launches, bool) or not isinstance(prior_launches, int) or prior_launches < 0:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored executor launch count invalid")
            total_launches = prior_launches + 1
            if total_launches > MAX_EXECUTOR_LAUNCHES:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor retry budget exhausted")

            expected = {
                "dispatch_key": dispatch_key,
                "envelope_digest": dispatch["envelope_digest"],
                "packet_digest": envelope.get("packet_digest"),
                "goal_id": dispatch["goal_id"],
                "goal_revision": int(dispatch["goal_revision"]),
                "node_id": dispatch["node_id"],
                "work_unit_id": dispatch["work_unit_id"],
                "run_id": dispatch["run_id"],
                "attempt": int(dispatch["attempt"]),
            }
            for field, expected_value in expected.items():
                if canonical_executor_receipt.get(field) != expected_value:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError(f"executor receipt {field} mismatch")
            for field in ("envelope_digest", "packet_digest"):
                if not isinstance(canonical_executor_receipt.get(field), str) or not canonical_executor_receipt[field].startswith("sha256:"):
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError(f"executor receipt {field} invalid")

            work_unit = conn.execute(
                "SELECT * FROM work_units WHERE work_unit_id = ?",
                (dispatch["work_unit_id"],),
            ).fetchone()
            run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (dispatch["run_id"],),
            ).fetchone()
            attempt = conn.execute(
                "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
                (dispatch["run_id"], int(dispatch["attempt"])),
            ).fetchone()
            if work_unit is None or run is None or attempt is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor dispatch queue record missing")
            if attempt["state"] != "ready" or run["state"] not in {"queued", "ready"} or work_unit["state"] not in {"ready", "pending"}:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor dispatch queue state invalid")
            if int(canonical_executor_receipt.get("fence", 0)) != int(attempt["fence"]):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor receipt fence mismatch")
            expires_at = moment + float(lease_seconds)
            conn.execute(
                "UPDATE attempts SET state = 'running', holder = ?, receipt_ref = ?, receipt_digest = ?, finished_at = NULL WHERE run_id = ? AND ordinal = ? AND state = 'ready' AND fence = ?",
                (executor_id, task_receipt_path, task_receipt_digest, dispatch["run_id"], int(dispatch["attempt"]), int(attempt["fence"])),
            )
            conn.execute(
                "UPDATE runs SET state = 'running', updated_at = ? WHERE run_id = ? AND state IN ('queued', 'ready')",
                (moment, dispatch["run_id"]),
            )
            conn.execute(
                "UPDATE work_units SET state = 'running', updated_at = ? WHERE work_unit_id = ? AND state IN ('ready', 'pending')",
                (moment, dispatch["work_unit_id"]),
            )
            conn.execute(
                "INSERT OR REPLACE INTO leases(work_unit_id, holder, fence, expires_at) VALUES (?, ?, ?, ?)",
                (dispatch["work_unit_id"], executor_id, int(attempt["fence"]), expires_at),
            )
            updated_receipt_body = copy.deepcopy(stored_receipt)
            updated_receipt_body.pop("receipt_digest", None)
            updated_receipt_body.update({
                "executor_receipt": copy.deepcopy(canonical_executor_receipt),
                "executor_status": "accepted",
                "executor_invocations": total_launches,
                "executor_launches": total_launches,
                "queue_state": "running",
                "attempt_state": "running",
            })
            updated_receipt = {**updated_receipt_body, "receipt_digest": digest_json(updated_receipt_body)}
            updated_receipt_json = json.dumps(updated_receipt, ensure_ascii=False, sort_keys=True)
            conn.execute(
                "UPDATE dispatch_consumptions SET receipt_json = ?, receipt_digest = ?, updated_at = ? WHERE dispatch_key = ?",
                (updated_receipt_json, updated_receipt["receipt_digest"], moment, dispatch_key),
            )
            self._append_event_conn(
                conn,
                event_id=(f"executor-accepted:{dispatch_key}" if int(dispatch["attempt"]) == 1
                          else f"executor-accepted:{dispatch_key}:{int(dispatch['attempt'])}:{int(attempt['fence'])}"),
                parent_goal_id=dispatch["parent_goal_id"],
                work_unit_id=dispatch["work_unit_id"],
                run_id=dispatch["run_id"],
                event_type="successor_executor_accepted",
                payload={
                    "dispatch_key": dispatch_key,
                    "executor_id": executor_id,
                    "attempt": int(dispatch["attempt"]),
                    "fence": int(attempt["fence"]),
                    "executor_receipt_digest": supplied_digest,
                    "task_receipt_digest": task_receipt_digest,
                },
                created_at=moment,
            )
            conn.execute("COMMIT")

        result = self.get_dispatch_consumption(dispatch_key) or {}
        result.update({
            "executor_receipt": copy.deepcopy(canonical_executor_receipt),
            "executor_invocations": total_launches,
            "executor_launches": total_launches,
            "executor_status": "accepted",
            "invoked": True,
            "work_unit": self.get_work_unit(dispatch["work_unit_id"]),
            "run": self.get_run(dispatch["run_id"]),
            "attempt_record": self.get_attempt(dispatch["run_id"], int(dispatch["attempt"])),
        })
        return result

    def retry_executor_dispatch(
        self,
        dispatch_key: str,
        failure_receipt: Mapping[str, Any],
        *,
        now: float | None = None,
        max_total_launches: int = MAX_EXECUTOR_LAUNCHES,
    ) -> dict[str, Any]:
        """Atomically move one known executor failure to a new Attempt.

        The logical dispatch key and WorkUnit remain unchanged.  Only LH owns
        the new Attempt/fence transition; the executor adapter never retries
        inside the failed Attempt.  A repeated failure receipt is an
        idempotent replay, while an unknown or stale fence is rejected.
        """
        dispatch_key = _required_text("dispatch_key", dispatch_key)
        if not isinstance(failure_receipt, Mapping):
            raise WorkUnitStoreError("executor failure receipt must be an object")
        failure = copy.deepcopy(dict(failure_receipt))
        supplied_digest = _required_text("failure_receipt_digest", failure.get("failure_receipt_digest"))
        failure_body = copy.deepcopy(failure)
        failure_body.pop("failure_receipt_digest", None)
        if supplied_digest != digest_json(failure_body):
            raise WorkUnitStoreError("executor failure receipt digest mismatch")
        if (
            failure.get("schema") != EXECUTOR_FAILURE_RECEIPT_SCHEMA
            or failure.get("status") != "failed"
            or failure.get("outcome") != "known_failure"
            or failure.get("retryable") is not True
        ):
            raise WorkUnitStoreError("executor failure receipt is not retryable")
        if isinstance(max_total_launches, bool) or not isinstance(max_total_launches, int) or max_total_launches < 1:
            raise WorkUnitStoreError("max_total_launches must be positive")
        if max_total_launches > MAX_EXECUTOR_LAUNCHES:
            raise WorkUnitStoreError("retry policy exceeds canonical launch budget")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
            "invocation_digest", "workspace_diff_digest",
        ):
            if not failure.get(field):
                raise WorkUnitStoreError(f"executor failure {field} missing")
        if failure.get("dispatch_key") != dispatch_key:
            raise WorkUnitStoreError("executor failure dispatch key mismatch")
        if failure.get("workspace_effects_reconciled") is not True:
            raise WorkUnitStoreError("executor failure workspace effects are not reconciled")
        proof = failure.get("timeout_and_termination_proof")
        if not isinstance(proof, Mapping) or proof.get("process_terminated") is not True:
            raise WorkUnitStoreError("executor failure termination is not proven")
        invocation_path = failure.get("invocation_path")
        if not isinstance(invocation_path, str) or not invocation_path.strip():
            raise WorkUnitStoreError("executor failure invocation path missing")
        try:
            invocation = json.loads(Path(invocation_path).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("executor failure invocation unreadable") from exc
        if not isinstance(invocation, dict):
            raise WorkUnitStoreError("executor failure invocation is not an object")
        invocation_digest = invocation.pop("invocation_digest", None)
        if invocation_digest != failure.get("invocation_digest") or invocation_digest != digest_json(invocation):
            raise WorkUnitStoreError("executor failure invocation digest mismatch")
        moment = self._now() if now is None else float(now)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            dispatch = conn.execute(
                "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?",
                (dispatch_key,),
            ).fetchone()
            if dispatch is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown dispatch_key: {dispatch_key}")
            try:
                envelope = json.loads(dispatch["envelope_json"])
                stored_receipt = json.loads(dispatch["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not JSON") from exc
            if not isinstance(envelope, dict) or not isinstance(stored_receipt, dict):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not an object")

            prior_failures = stored_receipt.get("executor_failure_receipts", [])
            if not isinstance(prior_failures, list):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored executor failure history is invalid")
            if supplied_digest in {
                item.get("failure_receipt_digest")
                for item in prior_failures
                if isinstance(item, Mapping)
            }:
                conn.execute("COMMIT")
                result = self.get_dispatch_consumption(dispatch_key) or {}
                result.update({
                    "retry_scheduled": False,
                    "retry_reused": True,
                    "executor_status": stored_receipt.get("executor_status", "ready"),
                    "executor_invocations": int(stored_receipt.get("executor_invocations", 0)),
                    "executor_launches": int(stored_receipt.get("executor_launches", len(prior_failures))),
                })
                return result

            expected = {
                "envelope_digest": dispatch["envelope_digest"],
                "packet_digest": envelope.get("packet_digest"),
                "goal_id": dispatch["goal_id"],
                "goal_revision": int(dispatch["goal_revision"]),
                "node_id": dispatch["node_id"],
                "work_unit_id": dispatch["work_unit_id"],
                "run_id": dispatch["run_id"],
                "attempt": int(dispatch["attempt"]),
            }
            for field, expected_value in expected.items():
                if failure.get(field) != expected_value:
                    conn.execute("ROLLBACK")
                    raise FenceError(f"executor failure {field} is stale or mismatched")

            work_unit = conn.execute(
                "SELECT * FROM work_units WHERE work_unit_id = ?",
                (dispatch["work_unit_id"],),
            ).fetchone()
            run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (dispatch["run_id"],),
            ).fetchone()
            attempt = conn.execute(
                "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
                (dispatch["run_id"], int(dispatch["attempt"])),
            ).fetchone()
            if work_unit is None or run is None or attempt is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor retry queue record missing")
            if int(attempt["fence"]) != int(failure["fence"]):
                conn.execute("ROLLBACK")
                raise FenceError("executor failure fence is stale")
            if attempt["state"] not in {"ready", "retry_pending"} or run["state"] not in {"queued", "ready", "retry_pending"} or work_unit["state"] not in {"pending", "ready", "retry_pending"}:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor retry queue state invalid")

            launch_count = int(stored_receipt.get("executor_launches", len(prior_failures))) + 1
            if launch_count > max_total_launches:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor retry budget exhausted")
            failure_history = [*prior_failures, failure]
            updated_body = copy.deepcopy(stored_receipt)
            updated_body.pop("receipt_digest", None)
            updated_body.update({
                "executor_failure_receipts": failure_history,
                "last_failure_receipt_digest": supplied_digest,
                "executor_launches": launch_count,
                "executor_invocations": 0,
                "retry_count": len(failure_history),
            })

            if launch_count >= max_total_launches:
                conn.execute(
                    "UPDATE attempts SET state = 'stopped', receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND fence = ?",
                    (f"executor-failure:{supplied_digest}", supplied_digest, moment, dispatch["run_id"], int(dispatch["attempt"]), int(failure["fence"])),
                )
                conn.execute("UPDATE runs SET state = 'stopped', updated_at = ? WHERE run_id = ? AND fence = ?", (moment, dispatch["run_id"], int(failure["fence"])))
                conn.execute("UPDATE work_units SET state = 'stopped', updated_at = ? WHERE work_unit_id = ?", (moment, dispatch["work_unit_id"]))
                conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (dispatch["work_unit_id"],))
                updated_body.update({
                    "executor_status": "exhausted",
                    "queue_state": "stopped",
                    "attempt_state": "stopped",
                    "retry_exhausted": True,
                })
                updated_receipt = {**updated_body, "receipt_digest": digest_json(updated_body)}
                conn.execute(
                    "UPDATE dispatch_consumptions SET receipt_json = ?, receipt_digest = ?, updated_at = ? WHERE dispatch_key = ?",
                    (json.dumps(updated_receipt, ensure_ascii=False, sort_keys=True), updated_receipt["receipt_digest"], moment, dispatch_key),
                )
                self._append_event_conn(
                    conn,
                    event_id=f"executor-retry-exhausted:{dispatch_key}:{int(dispatch['attempt'])}:{int(failure['fence'])}",
                    parent_goal_id=dispatch["parent_goal_id"],
                    work_unit_id=dispatch["work_unit_id"],
                    run_id=dispatch["run_id"],
                    event_type="successor_executor_retry_exhausted",
                    payload={"dispatch_key": dispatch_key, "attempt": int(dispatch["attempt"]), "fence": int(failure["fence"]), "failure_receipt_digest": supplied_digest, "launches": launch_count},
                    created_at=moment,
                )
                conn.execute("COMMIT")
                result = self.get_dispatch_consumption(dispatch_key) or {}
                result.update({"retry_scheduled": False, "retry_exhausted": True, "executor_status": "exhausted", "executor_invocations": 0, "executor_launches": launch_count})
                return result

            next_attempt = int(dispatch["attempt"]) + 1
            next_fence = max(int(run["fence"]), int(failure["fence"])) + 1
            workspace = str(attempt["workspace_ref"])
            conn.execute(
                "UPDATE attempts SET state = 'interrupted', receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state IN ('ready', 'retry_pending') AND fence = ?",
                (f"executor-failure:{supplied_digest}", supplied_digest, moment, dispatch["run_id"], int(dispatch["attempt"]), int(failure["fence"])),
            )
            conn.execute(
                "UPDATE runs SET state = 'queued', attempts = ?, fence = ?, updated_at = ? WHERE run_id = ? AND fence = ?",
                (next_attempt, next_fence, moment, dispatch["run_id"], int(failure["fence"])),
            )
            conn.execute(
                "INSERT INTO attempts(run_id, ordinal, state, holder, fence, workspace_ref, receipt_ref, receipt_digest, created_at, finished_at) VALUES (?, ?, 'ready', ?, ?, ?, NULL, NULL, ?, NULL)",
                (dispatch["run_id"], next_attempt, work_unit["worker_id"], next_fence, workspace, moment),
            )
            conn.execute("UPDATE work_units SET state = 'ready', updated_at = ? WHERE work_unit_id = ?", (moment, dispatch["work_unit_id"]))
            conn.execute("DELETE FROM leases WHERE work_unit_id = ?", (dispatch["work_unit_id"],))
            updated_body.update({
                "attempt": next_attempt,
                "fence": next_fence,
                "executor_status": "ready",
                "queue_state": "ready",
                "attempt_state": "ready",
                "retry_exhausted": False,
            })
            updated_receipt = {**updated_body, "receipt_digest": digest_json(updated_body)}
            conn.execute(
                "UPDATE dispatch_consumptions SET attempt = ?, receipt_json = ?, receipt_digest = ?, updated_at = ? WHERE dispatch_key = ?",
                (next_attempt, json.dumps(updated_receipt, ensure_ascii=False, sort_keys=True), updated_receipt["receipt_digest"], moment, dispatch_key),
            )
            self._append_event_conn(
                conn,
                event_id=f"executor-retry-scheduled:{dispatch_key}:{int(failure['attempt'])}:{int(failure['fence'])}",
                parent_goal_id=dispatch["parent_goal_id"],
                work_unit_id=dispatch["work_unit_id"],
                run_id=dispatch["run_id"],
                event_type="successor_executor_retry_scheduled",
                payload={"dispatch_key": dispatch_key, "previous_attempt": int(failure["attempt"]), "previous_fence": int(failure["fence"]), "attempt": next_attempt, "fence": next_fence, "failure_receipt_digest": supplied_digest, "launches": launch_count},
                created_at=moment,
            )
            conn.execute("COMMIT")

        result = self.get_dispatch_consumption(dispatch_key) or {}
        result.update({
            "retry_scheduled": True,
            "retry_reused": False,
            "executor_status": "ready",
            "executor_invocations": 0,
            "executor_launches": launch_count,
            "retry_count": len(failure_history),
        })
        return result

    schedule_executor_retry = retry_executor_dispatch
    retry_same_work_unit = retry_executor_dispatch

    def _block_trusted_unknown_conn(self, conn, dispatch, recovery):
        """An observed unknown fences the Goal, not just its dispatch journal.

        Called only inside the existing reconciliation transaction. A durable
        invocation may precede every CLI reservation; never invent consumption
        or a process observation to fill that crash window.
        """
        if recovery.get("continuation_authority_digest") is not None:
            if not trusted_continuation.executor_binding(conn, dispatch, recovery, unknown=True):
                raise WorkUnitStoreError("continuation_unknown_binding_invalid")
            return
        row = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                           (dispatch["goal_id"], dispatch["goal_revision"])).fetchone()
        fields = ("execution_binding_digest", "policy_digest")
        if row is None and not any(key in recovery for key in fields):
            return
        if any(not isinstance(recovery.get(key), str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", recovery[key])
               for key in fields):
            raise WorkUnitStoreError("trusted_unknown_binding_missing")
        expected = {key: dispatch[key] for key in (
            "dispatch_key", "envelope_digest", "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt")}
        expected["packet_digest"] = json.loads(dispatch["envelope_json"])["packet_digest"]
        if any(recovery.get(key) != value for key, value in expected.items()):
            raise FenceError("trusted_unknown_dispatch_mismatch")
        actual = conn.execute("SELECT r.attempts,r.fence AS current_fence,a.fence FROM runs r "
            "JOIN attempts a ON a.run_id=r.run_id AND a.ordinal=? WHERE r.run_id=?",
            (dispatch["attempt"], dispatch["run_id"])).fetchone()
        if (actual is None or any(isinstance(recovery.get(key), bool) or not isinstance(recovery.get(key), int)
                                 for key in ("goal_revision", "attempt", "fence"))
                or actual["attempts"] != recovery["attempt"] or actual["current_fence"] != recovery["fence"]
                or actual["fence"] != recovery["fence"]):
            raise FenceError("trusted_unknown_attempt_mismatch")
        if row is None:
            # The ordinary durable unknown below is sufficient: first-budget
            # admission's legacy guard refuses this Goal without resetting it.
            return
        budget = json.loads(row["budget_json"])
        if (row["execution_binding_digest"] != recovery["execution_binding_digest"]
                or budget["policy_digest"] != recovery["policy_digest"]):
            raise WorkUnitStoreError("trusted_unknown_policy_mismatch")
        reservations = [item for item in budget["reservations"] if item["run_id"] == recovery["run_id"]
                        and item["attempt"] == recovery["attempt"] and item["phase"] == "coding"]
        if len(reservations) > 1 or any(
            item["is_provider_cli"] is not True or any(item[key] != recovery[key] for key in (
                "goal_id", "goal_revision", "policy_digest", "run_id", "attempt", "fence"))
            for item in reservations):
            raise WorkUnitStoreError("trusted_unknown_reservation_conflict")
        budget.update(blocked=True, blocked_reason=budget["blocked_reason"] or "process_outcome_unknown")
        conn.execute("UPDATE trusted_execution_budgets SET budget_json=? WHERE goal_id=? AND goal_revision=?",
                     (json.dumps(budget, sort_keys=True), dispatch["goal_id"], dispatch["goal_revision"]))

    def reconcile_executor_unknown(
        self,
        dispatch_key: str,
        recovery_receipt: Mapping[str, Any],
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Persist an unknown executor outcome without changing the Attempt.

        A launch whose process/pipe result cannot be proven is counted against
        the durable budget, but it can never trigger another launch.  Replays
        return the same recovery binding and do not append another event.
        """
        dispatch_key = _required_text("dispatch_key", dispatch_key)
        if not isinstance(recovery_receipt, Mapping):
            raise WorkUnitStoreError("executor recovery receipt must be an object")
        recovery = copy.deepcopy(dict(recovery_receipt))
        supplied_digest = _required_text("recovery_receipt_digest", recovery.get("recovery_receipt_digest"))
        recovery_body = copy.deepcopy(recovery)
        recovery_body.pop("recovery_receipt_digest", None)
        if supplied_digest != digest_json(recovery_body):
            raise WorkUnitStoreError("executor recovery receipt digest mismatch")
        if recovery.get("schema") != "lh-successor-executor-recovery/v1" or recovery.get("status") != "reconciled" or recovery.get("outcome") != "unknown":
            raise WorkUnitStoreError("executor recovery receipt status invalid")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
            "invocation_digest",
        ):
            if not recovery.get(field):
                raise WorkUnitStoreError(f"executor recovery {field} missing")
        if recovery.get("dispatch_key") != dispatch_key:
            raise WorkUnitStoreError("executor recovery dispatch key mismatch")
        evidence = recovery.get("failure_evidence")
        if not isinstance(evidence, Mapping) or evidence.get("exit_code") is not None or evidence.get("stderr") != "unavailable":
            raise WorkUnitStoreError("executor recovery must preserve unknown exit/stderr")
        if not recovery.get("workspace_diff_digest"):
            raise WorkUnitStoreError("executor recovery workspace reconciliation missing")
        moment = self._now() if now is None else float(now)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?", (dispatch_key,)).fetchone()
            if dispatch is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown dispatch_key: {dispatch_key}")
            try:
                envelope = json.loads(dispatch["envelope_json"])
                stored_receipt = json.loads(dispatch["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not JSON") from exc
            if not isinstance(envelope, dict) or not isinstance(stored_receipt, dict):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("stored dispatch consumption is not an object")
            self._block_trusted_unknown_conn(conn, dispatch, recovery)
            prior = stored_receipt.get("executor_recovery_receipt")
            if isinstance(prior, Mapping):
                if prior.get("recovery_receipt_digest") != supplied_digest:
                    conn.execute("ROLLBACK")
                    raise FenceError("executor recovery is already bound to different evidence")
                conn.execute("COMMIT")
                result = self.get_dispatch_consumption(dispatch_key) or {}
                result.update({"executor_status": "unknown", "executor_invocations": 0, "executor_launches": int(stored_receipt.get("executor_launches", 0)), "recovery_reused": True})
                return result
            expected = {
                "envelope_digest": dispatch["envelope_digest"],
                "packet_digest": envelope.get("packet_digest"),
                "goal_id": dispatch["goal_id"],
                "goal_revision": int(dispatch["goal_revision"]),
                "node_id": dispatch["node_id"],
                "work_unit_id": dispatch["work_unit_id"],
                "run_id": dispatch["run_id"],
                "attempt": int(dispatch["attempt"]),
            }
            for field, expected_value in expected.items():
                if recovery.get(field) != expected_value:
                    conn.execute("ROLLBACK")
                    raise FenceError(f"executor recovery {field} is stale or mismatched")
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (dispatch["run_id"], int(dispatch["attempt"]))).fetchone()
            if attempt is None or int(attempt["fence"]) != int(recovery["fence"]):
                conn.execute("ROLLBACK")
                raise FenceError("executor recovery fence is stale")
            launches = int(stored_receipt.get("executor_launches", 0)) + 1
            if launches > MAX_EXECUTOR_LAUNCHES:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("executor retry budget exhausted")
            updated_body = copy.deepcopy(stored_receipt)
            updated_body.pop("receipt_digest", None)
            updated_body.update({
                "executor_status": "unknown",
                "executor_recovery_receipt": recovery,
                "executor_invocations": 0,
                "executor_launches": launches,
                "retry_blocked": True,
            })
            updated_receipt = {**updated_body, "receipt_digest": digest_json(updated_body)}
            conn.execute(
                "UPDATE dispatch_consumptions SET receipt_json = ?, receipt_digest = ?, updated_at = ? WHERE dispatch_key = ?",
                (json.dumps(updated_receipt, ensure_ascii=False, sort_keys=True), updated_receipt["receipt_digest"], moment, dispatch_key),
            )
            self._append_event_conn(
                conn,
                event_id=f"executor-outcome-unknown:{dispatch_key}:{int(recovery['attempt'])}:{int(recovery['fence'])}",
                parent_goal_id=dispatch["parent_goal_id"],
                work_unit_id=dispatch["work_unit_id"],
                run_id=dispatch["run_id"],
                event_type="successor_executor_outcome_unknown",
                payload={"dispatch_key": dispatch_key, "attempt": int(recovery["attempt"]), "fence": int(recovery["fence"]), "recovery_receipt_digest": supplied_digest, "launches": launches},
                created_at=moment,
            )
            conn.execute("COMMIT")
        result = self.get_dispatch_consumption(dispatch_key) or {}
        result.update({"executor_status": "unknown", "executor_invocations": 0, "executor_launches": launches, "recovery_reused": False})
        return result

    reconcile_unknown_executor = reconcile_executor_unknown

    def reconcile_executor_unknown_batch(self, items, *, expected_budget_digest, operation_digest, now=None):
        """Atomically journal exact original unknown launches, without dispatch.

        The public operation performs fresh file/process checks before this
        transaction. This final Store gate rechecks every row before updates.
        """
        if (self.read_only or not isinstance(items, list) or not items
            or not isinstance(operation_digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", operation_digest) is None):
            raise WorkUnitStoreError("recovery_batch_input_invalid")
        moment = self._now() if now is None else now
        pending, results, seen = [], [], set()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            budget_row = None
            for item in items:
                key, recovery = item["dispatch_key"], item["recovery_receipt"]
                if key in seen:
                    raise WorkUnitStoreError("recovery_batch_duplicate")
                seen.add(key)
                dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (key,)).fetchone()
                if dispatch is None:
                    raise WorkUnitStoreError("recovery_batch_dispatch_missing")
                current = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                    (dispatch["goal_id"], dispatch["goal_revision"])).fetchone()
                if (current is None or digest_json(json.loads(current["budget_json"])) != expected_budget_digest
                    or (budget_row is not None and dict(current) != dict(budget_row))):
                    raise WorkUnitStoreError("recovery_batch_budget_drift")
                budget_row = current
                body = {k: v for k, v in recovery.items() if k != "recovery_receipt_digest"}
                evidence = recovery.get("failure_evidence", {})
                envelope, receipt = json.loads(dispatch["envelope_json"]), json.loads(dispatch["receipt_json"])
                actual = conn.execute("SELECT r.attempts,r.fence,a.fence AS attempt_fence FROM runs r JOIN attempts a ON a.run_id=r.run_id AND a.ordinal=? WHERE r.run_id=?", (dispatch["attempt"], dispatch["run_id"])).fetchone()
                if (recovery.get("schema") != "lh-successor-executor-recovery/v1"
                    or recovery.get("status") != "reconciled" or recovery.get("outcome") != "unknown"
                    or recovery.get("recovery_receipt_digest") != digest_json(body)
                    or evidence.get("exit_code") is not None or evidence.get("stderr") != "unavailable"
                    or not recovery.get("workspace_diff_digest")
                    or any(recovery.get(k) != dispatch[k] for k in ("dispatch_key", "envelope_digest", "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt"))
                    or recovery.get("packet_digest") != envelope["packet_digest"]
                    or actual is None or actual["attempts"] != recovery["attempt"]
                    or actual["fence"] != recovery["fence"] or actual["attempt_fence"] != recovery["fence"]
                    or receipt.get("receipt_digest") != dispatch["receipt_digest"]
                    or receipt["receipt_digest"] != digest_json({k: v for k, v in receipt.items() if k != "receipt_digest"})):
                    raise WorkUnitStoreError("recovery_batch_evidence_invalid")
                prior = receipt.get("executor_recovery_receipt")
                if prior is not None:
                    if prior != recovery or receipt.get("executor_status") != "unknown":
                        raise WorkUnitStoreError("recovery_batch_conflict")
                    event = conn.execute("SELECT payload_json FROM events WHERE event_id=?",
                        (f"executor-outcome-unknown:{key}:{recovery['attempt']}:{recovery['fence']}",)).fetchone()
                    if event is None or json.loads(event[0]).get("operation_digest") != operation_digest:
                        raise WorkUnitStoreError("recovery_batch_operation_binding_conflict")
                    results.append({"dispatch_key": key, "recovery_reused": True})
                    continue
                if receipt.get("executor_receipt") or receipt.get("executor_failure_receipts") or receipt.get("executor_launches", 0):
                    raise WorkUnitStoreError("recovery_batch_original_journal_conflict")
                updated = {k: v for k, v in receipt.items() if k != "receipt_digest"}
                updated.update(executor_status="unknown", executor_recovery_receipt=copy.deepcopy(recovery),
                    executor_invocations=0, executor_launches=1, retry_blocked=True)
                updated["receipt_digest"] = digest_json(updated)
                pending.append((dispatch, recovery, updated))
                results.append({"dispatch_key": key, "recovery_reused": False})
            budget = json.loads(budget_row["budget_json"])
            expected_runs = {r["run_id"] for r in budget["reservations"] if r["state"] == "unknown"}
            if expected_runs != {item["recovery_receipt"]["run_id"] for item in items}:
                raise WorkUnitStoreError("recovery_batch_unknown_scope_incomplete")
            for dispatch, recovery, updated in pending:
                self._block_trusted_unknown_conn(conn, dispatch, recovery)
                conn.execute("UPDATE dispatch_consumptions SET receipt_json=?,receipt_digest=?,updated_at=? WHERE dispatch_key=?",
                    (json.dumps(updated, ensure_ascii=False, sort_keys=True), updated["receipt_digest"], moment, dispatch["dispatch_key"]))
                self._append_event_conn(conn, event_id=f"executor-outcome-unknown:{dispatch['dispatch_key']}:{recovery['attempt']}:{recovery['fence']}",
                    parent_goal_id=dispatch["parent_goal_id"], work_unit_id=dispatch["work_unit_id"], run_id=dispatch["run_id"],
                    event_type="successor_executor_outcome_unknown", payload={"dispatch_key": dispatch["dispatch_key"],
                        "attempt": recovery["attempt"], "fence": recovery["fence"], "recovery_receipt_digest": recovery["recovery_receipt_digest"],
                        "launches": 1, "operation_digest": operation_digest}, created_at=moment)
            after = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?", (budget_row["goal_id"], budget_row["goal_revision"])).fetchone()
            if dict(after) != dict(budget_row):
                raise WorkUnitStoreError("recovery_batch_original_budget_changed")
            conn.execute("COMMIT")
        return results

    @staticmethod
    def _candidate_recovery_preconditions(conn, admission):
        """Exact, target-scoped rows; another target's admission is not drift."""
        dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?",
            (admission["dispatch_key"],)).fetchone()
        if dispatch is None:
            raise WorkUnitStoreError("candidate_recovery_dispatch_missing")
        run, work = admission["run_id"], admission["work_unit_id"]
        queries = {
            "dispatch": ("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (admission["dispatch_key"],)),
            "parent_goal": ("SELECT * FROM parent_goals WHERE parent_goal_id=?", (dispatch["parent_goal_id"],)),
            "run": ("SELECT * FROM runs WHERE run_id=?", (run,)),
            "work_unit": ("SELECT * FROM work_units WHERE work_unit_id=?", (work,)),
            "attempt": ("SELECT * FROM attempts WHERE run_id=? AND ordinal=?", (run, admission["attempt"])),
            "budget": ("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                (dispatch["goal_id"], dispatch["goal_revision"])),
            "delivery_binding": ("SELECT * FROM delivery_bindings WHERE work_unit_id=?", (work,)),
            "lease": ("SELECT * FROM leases WHERE work_unit_id=?", (work,)),
            "completion": ("SELECT * FROM completion_phases WHERE run_id=? ORDER BY phase_key", (run,)),
            "admission": ("SELECT * FROM candidate_recovery_admissions WHERE dispatch_key=?", (admission["dispatch_key"],)),
        }
        return {name: digest_json([dict(row) for row in conn.execute(sql, params)])
            for name, (sql, params) in queries.items()}

    def candidate_recovery_preconditions(self, admission):
        with self._connect() as conn:
            return self._candidate_recovery_preconditions(conn, admission)

    def _validate_candidate_recovery_intent(self, conn, admission, moment):
        guard = admission.get("stable_intent_guard")
        if guard is None:
            return
        if (admission.get("kind") != "preserved_result_recovery" or not isinstance(guard, Mapping)
            or set(guard) != {"expires_at", "store_preconditions"}):
            raise WorkUnitStoreError("candidate_recovery_stable_intent_guard_invalid")
        expires = guard["expires_at"]
        if (isinstance(expires, bool) or not isinstance(expires, (int, float))
            or not float("-inf") < expires < float("inf")
            or max(moment, self._now()) >= expires):
            raise WorkUnitStoreError("candidate_recovery_stable_intent_expired")
        if guard["store_preconditions"] != self._candidate_recovery_preconditions(conn, admission):
            raise FenceError("candidate_recovery_stable_intent_precondition_drift")

    def record_candidate_recovery_admission(
        self,
        admission: Mapping[str, Any],
        *,
        approved_manifest_context: Mapping[str, Any] | None = None,
        now: float | None = None,
        _validate_only: bool = False,
    ) -> dict[str, Any]:
        """Admit an existing unknown Attempt for read-only candidate recovery.

        This transition deliberately reuses the queued Attempt and its fence.
        It never calls the executor, creates an Attempt, changes the dispatch
        receipt, or resets the executor launch budget.
        """
        if self.read_only and not _validate_only:
            raise WorkUnitStoreError("candidate_recovery_admission_write_forbidden")
        if not isinstance(admission, Mapping):
            raise WorkUnitStoreError("candidate_recovery_admission_invalid")
        value = copy.deepcopy(dict(admission))
        preserved = value.get("kind") == "preserved_result_recovery"
        authority = approved_manifest_context
        if not isinstance(authority, Mapping):
            raise WorkUnitStoreError("candidate_recovery_approved_manifest_context_missing")
        authority = copy.deepcopy(dict(authority))
        if preserved and authority.get("kind") != "preserved_result_recovery":
            raise WorkUnitStoreError("preserved_result_authority_kind_invalid")
        value["approved_manifest_context"] = authority
        if authority.get("schema") not in {None, "host-p7-candidate-recovery-authority/v1"}:
            raise WorkUnitStoreError("candidate_recovery_approved_manifest_context_invalid")
        authority["schema"] = "host-p7-candidate-recovery-authority/v1"
        approved_digest = authority.get("approved_manifest_digest")
        if (not isinstance(approved_digest, str) or not approved_digest.startswith("sha256:")
            or len(approved_digest) != 71):
            raise WorkUnitStoreError("candidate_recovery_approved_manifest_digest_invalid")
        if value.get("schema") not in {None, CANDIDATE_RECOVERY_ADMISSION_SCHEMA}:
            raise WorkUnitStoreError("candidate_recovery_admission_schema_invalid")
        value["schema"] = CANDIDATE_RECOVERY_ADMISSION_SCHEMA
        status = value.get("status", "approved")
        if status not in {"approved", "admitted", "candidate_recovery"}:
            raise WorkUnitStoreError("candidate_recovery_admission_status_invalid")
        value["status"] = "admitted"
        dispatch_key = _required_text("dispatch_key", value.get("dispatch_key"))
        work_unit_id = _required_text("work_unit_id", value.get("work_unit_id"))
        run_id = _required_text("run_id", value.get("run_id"))
        for field in ("attempt", "fence"):
            if isinstance(value.get(field), bool) or not isinstance(value.get(field), int) or value[field] < 1:
                raise WorkUnitStoreError(f"candidate_recovery_{field}_invalid")
        current_dispatch = value.get("current_dispatch_receipt")
        dispatch_digest = value.get("current_dispatch_receipt_digest")
        if isinstance(current_dispatch, Mapping):
            dispatch_digest = current_dispatch.get("receipt_digest") or digest_json(dict(current_dispatch))
        current_unknown = value.get("current_unknown_recovery_receipt")
        unknown_digest = value.get("current_unknown_recovery_receipt_digest")
        if isinstance(current_unknown, Mapping):
            unknown_digest = current_unknown.get("recovery_receipt_digest") or digest_json(dict(current_unknown))
        required = (
            "source_attempt", "source_workspace_inventory", "candidate_digest",
            "completion_contract_digest", "delivery_contract_digest",
            "owner_authorization_digest", "no_inflight_evidence",
        )
        for field in required:
            if field not in value or value[field] in (None, "", {}):
                raise WorkUnitStoreError(f"candidate_recovery_{field}_missing")
        if not isinstance(dispatch_digest, str) or not dispatch_digest.strip():
            raise WorkUnitStoreError("candidate_recovery_current_dispatch_receipt_missing")
        if not isinstance(unknown_digest, str) or not unknown_digest.strip():
            raise WorkUnitStoreError("candidate_recovery_current_unknown_recovery_receipt_missing")
        if not isinstance(value["source_attempt"], Mapping):
            raise WorkUnitStoreError("candidate_recovery_source_attempt_invalid")
        if not isinstance(value["source_workspace_inventory"], Mapping):
            raise WorkUnitStoreError("candidate_recovery_source_inventory_invalid")
        if not isinstance(value["no_inflight_evidence"], Mapping):
            raise WorkUnitStoreError("candidate_recovery_no_inflight_invalid")
        if not isinstance(value["candidate_digest"], str) or not value["candidate_digest"].startswith("sha256:"):
            raise WorkUnitStoreError("candidate_recovery_candidate_digest_invalid")
        input_supplied = value.get("input_digest") or value.get("admission_digest")
        body = {key: item for key, item in value.items()
                if key not in {"input_digest", "admission_digest", "status", "admitted_at"}}
        input_digest = digest_json(body)
        if input_supplied is not None and input_supplied != input_digest:
            raise WorkUnitStoreError("candidate_recovery_admission_digest_mismatch")
        value["input_digest"] = input_digest
        admission_id = _required_text("admission_id", value.get("admission_id"))
        value["admission_id"] = admission_id
        moment = self._now() if now is None else float(now)

        with self._connect() as conn:
            conn.execute("BEGIN" if _validate_only else "BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT * FROM candidate_recovery_admissions WHERE dispatch_key = ?",
                (dispatch_key,),
            ).fetchone()
            if prior is not None:
                if prior["input_digest"] != input_digest:
                    conn.execute("ROLLBACK")
                    raise FenceError("candidate recovery admission already bound to different evidence")
                dispatch = conn.execute(
                    "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?", (dispatch_key,)
                ).fetchone()
                if dispatch is None:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("candidate_recovery_dispatch_missing")
                try:
                    replay_receipt = json.loads(dispatch["receipt_json"])
                    replay_admission = json.loads(prior["admission_json"])
                except (TypeError, json.JSONDecodeError) as exc:
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("candidate_recovery_replay_unreadable") from exc
                if not isinstance(replay_admission, dict):
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("candidate_recovery_replay_admission_invalid")
                replay_authority = replay_admission.get("approved_manifest_context")
                if not isinstance(replay_authority, Mapping) or dict(replay_authority) != dict(authority):
                    conn.execute("ROLLBACK")
                    raise FenceError("candidate_recovery_replay_authority_changed")
                current_run = conn.execute(
                    "SELECT * FROM runs WHERE run_id = ?", (prior["run_id"],)
                ).fetchone()
                current_attempt = conn.execute(
                    "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?",
                    (prior["run_id"], int(prior["attempt"])),
                ).fetchone()
                if (current_run is None or current_attempt is None
                    or int(current_run["attempts"]) != int(prior["attempt"])
                    or int(current_run["fence"]) != int(prior["fence"])
                    or int(current_attempt["fence"]) != int(prior["fence"])
                    or dispatch["attempt"] != int(prior["attempt"])
                    or dispatch["receipt_digest"] != replay_admission.get("current_dispatch_receipt_digest")):
                    conn.execute("ROLLBACK")
                    raise FenceError("candidate_recovery_replay_current_binding_changed")
                replay_body = {key: item for key, item in replay_receipt.items() if key != "receipt_digest"}
                replay_unknown = replay_receipt.get("executor_recovery_receipt")
                replay_unknown_body = ({key: item for key, item in replay_unknown.items()
                                       if key != "recovery_receipt_digest"}
                                      if isinstance(replay_unknown, Mapping) else None)
                if (replay_receipt.get("receipt_digest") != digest_json(replay_body)
                    or dispatch["receipt_digest"] != replay_receipt.get("receipt_digest")
                    or replay_receipt.get("executor_status") != "unknown"
                    or not isinstance(replay_unknown, Mapping)
                    or replay_unknown.get("recovery_receipt_digest") != digest_json(replay_unknown_body)
                    or replay_unknown.get("recovery_receipt_digest") != replay_admission.get("current_unknown_recovery_receipt_digest", (replay_admission.get("current_unknown_recovery_receipt") or {}).get("recovery_receipt_digest"))):
                    conn.execute("ROLLBACK")
                    raise FenceError("candidate_recovery_replay_unknown_changed")
                conn.execute("COMMIT")
                return self.get_candidate_recovery_admission(dispatch_key) or {}
            self._validate_candidate_recovery_intent(conn, value, moment)
            dispatch = conn.execute(
                "SELECT * FROM dispatch_consumptions WHERE dispatch_key = ?", (dispatch_key,)
            ).fetchone()
            if dispatch is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_dispatch_missing")
            try:
                envelope = json.loads(dispatch["envelope_json"])
                receipt = json.loads(dispatch["receipt_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_dispatch_unreadable") from exc
            if not isinstance(envelope, dict) or not isinstance(receipt, dict):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_dispatch_invalid")
            # The manifest/binding authority is supplied by the already
            # approved task-area activation.  The recovery proposal may cite
            # it, but may not author it.  Bind every current queue identity
            # before accepting any self-sealed owner document.
            run = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            work = conn.execute("SELECT * FROM work_units WHERE work_unit_id = ?", (work_unit_id,)).fetchone()
            attempt = conn.execute(
                "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, value["attempt"])
            ).fetchone()
            if run is None or work is None or attempt is None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_queue_missing")
            authority_identity = {
                "goal_id": dispatch["goal_id"],
                "goal_revision": int(dispatch["goal_revision"]),
                "node_id": dispatch["node_id"],
                "dispatch_key": dispatch_key,
                "work_unit_id": work_unit_id,
                "run_id": run_id,
                "attempt": int(value["attempt"]),
                "fence": int(value["fence"]),
            }
            authority_binding = {
                **authority_identity,
                "envelope_digest": dispatch["envelope_digest"],
                "packet_digest": envelope.get("packet_digest"),
            }
            authority_numbers_ok = all(
                isinstance(authority.get(field), int) and not isinstance(authority.get(field), bool)
                for field in ("goal_revision", "attempt", "fence")
            )
            if (not authority_numbers_ok
                or any(authority.get(field) != expected for field, expected in authority_identity.items())
                or authority.get("decision_id") != (PRESERVED_RESULT_DECISION_ID if preserved else CANDIDATE_RECOVERY_DECISION_ID)
                or not isinstance(authority.get("owner_principal"), str)
                or not authority["owner_principal"].strip()
                or authority.get("binding_digest") != digest_json(authority_binding)
                or int(run["attempts"]) != int(value["attempt"])
                or int(run["fence"]) != int(value["fence"])
                or int(attempt["fence"]) != int(value["fence"])
                or work["node_id"] != dispatch["node_id"]):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_approved_manifest_context_mismatch")
            receipt_body = {key: item for key, item in receipt.items() if key != "receipt_digest"}
            if receipt.get("receipt_digest") != digest_json(receipt_body) or dispatch["receipt_digest"] != receipt.get("receipt_digest"):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_dispatch_receipt_tampered")
            expected_identity = {
                "work_unit_id": dispatch["work_unit_id"], "run_id": dispatch["run_id"],
                "attempt": int(dispatch["attempt"]), "fence": value["fence"],
            }
            if work_unit_id != expected_identity["work_unit_id"] or run_id != expected_identity["run_id"] or value["attempt"] != expected_identity["attempt"]:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_dispatch_identity_mismatch")
            if dispatch["receipt_digest"] != dispatch_digest:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_dispatch_receipt_mismatch")
            if isinstance(current_dispatch, Mapping) and dict(current_dispatch) != receipt:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_dispatch_receipt_identity_mismatch")
            if receipt.get("executor_status") != "unknown":
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_executor_not_unknown")
            recovery = receipt.get("executor_recovery_receipt")
            if not isinstance(recovery, Mapping) or recovery.get("recovery_receipt_digest") != unknown_digest:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_unknown_receipt_mismatch")
            if isinstance(current_unknown, Mapping) and dict(current_unknown) != dict(recovery):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_unknown_receipt_identity_mismatch")
            recovery_body = {key: item for key, item in recovery.items() if key != "recovery_receipt_digest"}
            if recovery.get("recovery_receipt_digest") != digest_json(recovery_body):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_unknown_receipt_tampered")
            # Unknown recovery is admissible only when the executor's own
            # reservation and heartbeat files can still be read back.  A
            # boolean ``inflight=false`` supplied by a caller is not process
            # termination proof and cannot replace these seals.
            invocation_path, invocation = _candidate_recovery_file(
                recovery.get("invocation_path"), "invocation")
            heartbeat_path_value = recovery.get("heartbeat_path")
            if not isinstance(heartbeat_path_value, str) or not heartbeat_path_value.strip():
                # DurableTaskExecutorAdapter reserves
                # ``<safe>.task-receipt.invocation.json`` beside
                # ``<safe>.heartbeat.json``.  Derive only this established
                # sibling name; never scan for an arbitrary heartbeat.
                suffix = ".task-receipt.invocation.json"
                if not invocation_path.name.endswith(suffix):
                    conn.execute("ROLLBACK")
                    raise WorkUnitStoreError("candidate_recovery_heartbeat_path_missing")
                heartbeat_path_value = str(invocation_path.with_name(
                    invocation_path.name[:-len(suffix)] + ".heartbeat.json"))
            heartbeat_path, heartbeat = _candidate_recovery_file(heartbeat_path_value, "heartbeat")
            invocation_body = {key: item for key, item in invocation.items()
                               if key != "invocation_digest"}
            trusted_preserved = preserved and invocation.get("schema") == "lh-trusted-command-invocation/v1"
            if (invocation.get("schema") != ("lh-trusted-command-invocation/v1" if trusted_preserved else "lh-codex-subscription-invocation/v1")
                or invocation.get("status") != "reserved"
                or invocation.get("invocation_digest") != recovery.get("invocation_digest")
                or invocation.get("invocation_digest") != digest_json(invocation_body)):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_invocation_seal_invalid")
            heartbeat_body = {key: item for key, item in heartbeat.items()
                              if key != "heartbeat_digest"}
            if (heartbeat.get("schema") not in ({"lh-successor-task-heartbeat/v1", "lh-successor-task-heartbeat/v2"}
                                               if trusted_preserved else {"lh-successor-task-heartbeat/v1"})
                or heartbeat.get("heartbeat_digest") != recovery.get("heartbeat_digest")
                or heartbeat.get("heartbeat_digest") != digest_json(heartbeat_body)
                or heartbeat.get("status") != "alive"):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_heartbeat_seal_invalid")
            if trusted_preserved:
                budget_row = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
                    (dispatch["goal_id"], dispatch["goal_revision"])).fetchone()
                budget = json.loads(budget_row["budget_json"]) if budget_row else None
                reservations = [row for row in budget["reservations"] if row["run_id"] == run_id and row["phase"] == "coding"] if budget else []
                if (budget is None or len(reservations) != 1 or reservations[0]["state"] != "unknown"
                    or any(e.get("execution_binding_digest") != budget_row["execution_binding_digest"]
                        or e.get("policy_digest") != budget["policy_digest"] for e in (invocation, recovery))
                    or (heartbeat.get("schema") == "lh-successor-task-heartbeat/v2" and any(
                        heartbeat.get(k) != invocation.get(k) for k in ("execution_binding_digest", "policy_digest")))):
                    raise WorkUnitStoreError("preserved_result_trusted_binding_invalid")
            evidence_identity = {
                "dispatch_key": dispatch_key,
                "envelope_digest": dispatch["envelope_digest"],
                "packet_digest": envelope.get("packet_digest"),
                "goal_id": dispatch["goal_id"],
                "goal_revision": int(dispatch["goal_revision"]),
                "node_id": dispatch["node_id"],
                "work_unit_id": work_unit_id,
                "run_id": run_id,
                "attempt": int(value["attempt"]),
                "fence": int(value["fence"]),
            }
            for evidence in (invocation, heartbeat, recovery):
                if any(evidence.get(field) != expected for field, expected in evidence_identity.items()):
                    conn.execute("ROLLBACK")
                    raise FenceError("candidate_recovery_executor_evidence_identity_mismatch")
            if invocation.get("invocation_digest") != recovery.get("invocation_digest"):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_invocation_digest_mismatch")
            if heartbeat.get("invocation_digest") != invocation.get("invocation_digest"):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_heartbeat_invocation_mismatch")
            if (invocation.get("worktree") != value.get("current_workspace_ref")
                or heartbeat.get("worktree") != value.get("current_workspace_ref")):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_executor_scope_mismatch")
            process_identity = recovery.get("process_identity")
            if (not _candidate_recovery_process_identity_complete(process_identity)
                or process_identity != heartbeat.get("process_identity")
                or _candidate_recovery_process_alive(process_identity, identity_port=self.identity_port)):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_process_identity_invalid")
            invocation_time = _candidate_recovery_time(invocation.get("reserved_at"))
            heartbeat_time = _candidate_recovery_time(heartbeat.get("observed_at"))
            recovery_time = _candidate_recovery_time(recovery.get("recorded_at"))
            if (invocation_time is None or heartbeat_time is None or recovery_time is None
                or heartbeat_time < invocation_time or recovery_time < heartbeat_time):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_heartbeat_stale")
            if heartbeat_path.stat().st_mtime_ns < invocation_path.stat().st_mtime_ns:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_heartbeat_file_stale")
            if int(attempt["fence"]) != value["fence"] or int(run["fence"]) != value["fence"]:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_fence_mismatch")
            if attempt["workspace_ref"] != value.get("current_workspace_ref", attempt["workspace_ref"]):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_current_workspace_mismatch")
            source = value["source_attempt"]
            source_ordinal = source.get("attempt", source.get("ordinal"))
            if (source.get("run_id") != run_id or source.get("workspace_ref") is None
                or isinstance(source_ordinal, bool) or not isinstance(source_ordinal, int)
                or source_ordinal < 1
                or (source_ordinal != value["attempt"] if preserved else source_ordinal >= value["attempt"])
                or (preserved and source.get("fence") != value["fence"])):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_source_attempt_mismatch")
            source_row = conn.execute(
                "SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, source_ordinal)
            ).fetchone()
            if (source_row is None or source_row["workspace_ref"] != source["workspace_ref"]
                or (source.get("fence") is not None and int(source_row["fence"]) != int(source["fence"]))):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_source_attempt_not_found")
            source_inventory = value["source_workspace_inventory"]
            source_root = source_inventory.get("root") or source_inventory.get("workspace_ref") or source.get("workspace_ref")
            if not isinstance(source_root, str) or not source_root.strip() or Path(source_root).expanduser().resolve() != Path(source["workspace_ref"]).expanduser().resolve():
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_source_workspace_provenance_mismatch")
            source_unresolved = Path(source_root).expanduser()
            if preserved and any(path.is_symlink() for path in (source_unresolved, *source_unresolved.parents)):
                raise WorkUnitStoreError("preserved_result_workspace_symlink")
            source_path = source_unresolved.resolve()
            if not source_path.is_dir() or (source_path / ".git").is_symlink() or not (source_path / ".git").exists():
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_source_workspace_unreadable")
            try:
                readonly_env = {key: val for key, val in os.environ.items() if key != "GIT_INDEX_FILE"}
                readonly_env["GIT_OPTIONAL_LOCKS"] = "0"
                head = subprocess.check_output(["git", "-C", str(source_path), "rev-parse", "HEAD"], env=readonly_env).decode().strip()
                names = subprocess.check_output(["git", "-C", str(source_path), "ls-files", "-z", "--cached", "--others", "--exclude-standard"], env=readonly_env).split(b"\0")
                entries = []
                for raw in sorted(set(names)):
                    if not raw:
                        continue
                    relative = raw.decode("utf-8")
                    path = source_path / relative
                    if path.is_symlink() or path.is_dir() or (preserved and any(p.is_symlink() for p in path.parents)):
                        raise ValueError("unsupported")
                    entries.append([relative, path.stat().st_mode if path.exists() else None, hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None])
                actual_candidate_digest = digest_json({"head": head, "files": entries})
            except (OSError, ValueError, subprocess.SubprocessError) as exc:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_source_inventory_unreadable") from exc
            if actual_candidate_digest != value["candidate_digest"]:
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_candidate_digest_mismatch")
            inventory_digest = value.get("source_workspace_inventory_digest")
            if inventory_digest is not None and inventory_digest != digest_json(dict(source_inventory)):
                conn.execute("ROLLBACK")
                raise FenceError("candidate_recovery_source_inventory_digest_mismatch")
            owner = value.get("owner_authorization")
            if preserved and (not isinstance(owner, Mapping)
                or owner.get("disposition") != "accept_preserved_result_without_usage_reconstruction"):
                raise WorkUnitStoreError("preserved_result_owner_disposition_missing")
            if (not isinstance(owner, Mapping)
                or not owner.get("decision_id")
                or not owner.get("principal")
                or owner.get("decision_id") != authority["decision_id"]
                or owner.get("principal") != authority["owner_principal"]
                or owner.get("goal_id") != dispatch["goal_id"]
                or owner.get("goal_revision") != int(dispatch["goal_revision"])
                or owner.get("node_id") != dispatch["node_id"]
                or owner.get("dispatch_key") != dispatch_key
                or owner.get("run_id") != run_id
                or owner.get("attempt") != value["attempt"]
                or owner.get("fence") != value["fence"]
                or digest_json(dict(owner)) != value["owner_authorization_digest"]):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_owner_authorization_digest_mismatch")
            no_inflight = value["no_inflight_evidence"]
            scope = no_inflight.get("scope")
            scope_ok = False
            if isinstance(scope, Mapping):
                scope_ok = (
                    scope.get("dispatch_key") == dispatch_key
                    and scope.get("run_id") == run_id
                    and scope.get("attempt") == value["attempt"]
                    and scope.get("fence") == value["fence"]
                    and scope.get("worktree") == value.get("current_workspace_ref")
                )
            elif isinstance(scope, str):
                scope_ok = scope in {f"dispatch:{dispatch_key}", f"{dispatch_key}:{value['attempt']}:{value['fence']}"}
            process_tree = no_inflight.get("process_tree")
            process_rows = no_inflight.get("processes")
            process_rows_ok = isinstance(process_rows, list) and all(
                isinstance(item, Mapping)
                and _candidate_recovery_process_identity_complete(
                    item.get("process_identity", item))
                and not _candidate_recovery_process_alive(item.get("process_identity", item), identity_port=self.identity_port)
                for item in process_rows
            )
            tree_ok = isinstance(process_tree, list) and bool(process_tree) and all(
                isinstance(item, Mapping)
                and (
                    (
                        _candidate_recovery_process_identity_complete(
                            item.get("process_identity", item))
                        and not _candidate_recovery_process_alive(
                            item.get("process_identity", item), identity_port=self.identity_port)
                    )
                    if ("process_identity" in item
                        or any(field in item for field in ("pid", "starttime", "boot_id")))
                    else item.get("state") in {"absent", "dead", "terminated"}
                )
                for item in process_tree
            )
            no_inflight_time = _candidate_recovery_time(
                no_inflight.get("observed_at") or no_inflight.get("checked_at"))
            if (no_inflight.get("run_id") != run_id or no_inflight.get("attempt") != value["attempt"]
                or no_inflight.get("fence") != value["fence"]
                or no_inflight.get("inflight") is not False
                or no_inflight.get("status") not in {"quiescent", "clear", "no_inflight"}
                or not scope_ok
                or not process_rows_ok
                or not tree_ok
                or no_inflight_time is None
                or no_inflight_time < recovery_time
                or (preserved and (no_inflight_time > moment + 5 or moment - no_inflight_time > 300))):
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_no_inflight_scope_invalid")
            if conn.execute("SELECT 1 FROM leases WHERE work_unit_id = ?", (work_unit_id,)).fetchone() is not None:
                conn.execute("ROLLBACK")
                raise LeaseBusyError(work_unit_id)
            claimed = conn.execute(
                "SELECT 1 FROM completion_phases WHERE run_id = ? AND attempt = ? AND fence = ?",
                (run_id, value["attempt"], value["fence"]),
            ).fetchone()
            if claimed is not None:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_completion_already_claimed")
            if run["state"] not in {"queued", "ready"} or attempt["state"] != "ready" or work["state"] not in {"pending", "ready"}:
                conn.execute("ROLLBACK")
                raise WorkUnitStoreError("candidate_recovery_current_attempt_not_ready")
            if _validate_only:
                conn.execute("COMMIT")
                return value
            if value.get("stable_intent_guard") is not None:
                # The caller holds both historical attempt locks. SQLite's
                # writer transaction protects these rows until COMMIT; the
                # last safe point also re-observes the external candidate and
                # process rather than treating an earlier check as a lock.
                self._validate_candidate_recovery_intent(conn, value, moment)
                from .work_unit_completion import candidate_inventory, candidate_inventory_digest
                if candidate_inventory_digest(candidate_inventory(source_path)) != value["source_workspace_inventory_digest"]:
                    raise FenceError("candidate_recovery_candidate_digest_mismatch")
                if _candidate_recovery_process_alive(process_identity, identity_port=self.identity_port):
                    raise WorkUnitStoreError("candidate_recovery_process_identity_invalid")
                self._validate_candidate_recovery_intent(conn, value, moment)
                moment = max(moment, self._now())
                if no_inflight_time > moment + 5 or moment - no_inflight_time > 300:
                    raise WorkUnitStoreError("candidate_recovery_no_inflight_scope_invalid")
            conn.execute("UPDATE runs SET state='running', updated_at=? WHERE run_id=?", (moment, run_id))
            conn.execute("UPDATE work_units SET state='running', updated_at=? WHERE work_unit_id=?", (moment, work_unit_id))
            conn.execute("UPDATE attempts SET state='running' WHERE run_id=? AND ordinal=?", (run_id, value["attempt"]))
            conn.execute(
                "INSERT INTO leases(work_unit_id, holder, fence, expires_at) VALUES (?, ?, ?, ?)",
                (work_unit_id, attempt["holder"], value["fence"], moment + 86400.0),
            )
            conn.execute(
                "INSERT INTO candidate_recovery_admissions(admission_id,input_digest,dispatch_key,work_unit_id,run_id,attempt,fence,admission_json,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (admission_id, input_digest, dispatch_key, work_unit_id, run_id, value["attempt"], value["fence"], json.dumps(value, ensure_ascii=False, sort_keys=True), "admitted", moment, moment),
            )
            self._append_event_conn(
                conn, event_id=admission_id, parent_goal_id=work["parent_goal_id"],
                work_unit_id=work_unit_id, run_id=run_id,
                event_type="candidate_recovery_admitted",
                payload={"admission_id": admission_id, "input_digest": input_digest, "dispatch_key": dispatch_key, "attempt": value["attempt"], "fence": value["fence"]},
                created_at=moment,
            )
            conn.execute("COMMIT")
        return self.get_candidate_recovery_admission(dispatch_key) or {}

    admit_candidate_recovery = record_candidate_recovery_admission
    record_candidate_recovery = record_candidate_recovery_admission

    def validate_candidate_recovery_admission(self, admission, *, approved_manifest_context, now=None):
        return self.record_candidate_recovery_admission(admission,
            approved_manifest_context=approved_manifest_context, now=now, _validate_only=True)

    def get_candidate_recovery_admission(self, key: str) -> dict[str, Any] | None:
        key = _required_text("candidate_recovery_key", key)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM candidate_recovery_admissions WHERE dispatch_key = ? OR admission_id = ? OR input_digest = ?",
                (key, key, key),
            ).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row["admission_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise WorkUnitStoreError("candidate_recovery_admission_unreadable") from exc
        if not isinstance(value, dict):
            raise WorkUnitStoreError("candidate_recovery_admission_not_object")
        value.update({"admission_id": row["admission_id"], "input_digest": row["input_digest"],
                      "dispatch_key": row["dispatch_key"], "status": row["status"]})
        return value

    read_candidate_recovery_admission = get_candidate_recovery_admission

    def disposition_executor_unknown(self, dispatch_key: str, *,
                                     owner_authorization: Mapping[str, Any],
                                     policy: Mapping[str, Any], apply: bool = False) -> dict[str, Any]:
        """Plan or atomically consume an exact canonical owner disposition.

        Policy is supplied by the controller's canonical authority port, never
        taken from the authorization document. Original unknown evidence stays
        intact and its already counted launch is not charged a second time.
        """
        from .successor_executor import disposition_process_dead, disposition_workspace_inventory
        if apply and self.read_only:
            raise WorkUnitStoreError("read_only_disposition_apply_forbidden")
        authorization = copy.deepcopy(dict(owner_authorization))
        authorization_digest = digest_json(authorization)
        connection = self._connect() if apply else WorkUnitStore(self.root, read_only=True)._connect()
        with connection as conn:
            conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
            row = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (dispatch_key,)).fetchone()
            if row is None:
                raise WorkUnitStoreError("disposition_dispatch_missing")
            dispatch = self._dispatch_row(row)
            receipt, envelope = dispatch["receipt"], dispatch["envelope"]
            if (row["receipt_digest"] != receipt.get("receipt_digest")
                or receipt.get("receipt_digest") != digest_json({k:v for k,v in receipt.items() if k != "receipt_digest"})):
                raise WorkUnitStoreError("disposition_receipt_tampered")
            prior = receipt.get("executor_unknown_disposition")
            if prior:
                if prior.get("authorization_digest") != authorization_digest or prior.get("policy_digest") != digest_json(policy):
                    raise FenceError("disposition_already_consumed")
                return {"status": "reused", "dispatch": dispatch, "disposition": prior, "executor_invocations": 0}
            recovery = receipt.get("executor_recovery_receipt")
            if receipt.get("executor_status") != "unknown" or not isinstance(recovery, dict) or receipt.get("executor_receipt"):
                raise WorkUnitStoreError("disposition_requires_unknown")
            recovery_body = {k: v for k, v in recovery.items() if k != "recovery_receipt_digest"}
            if recovery.get("recovery_receipt_digest") != digest_json(recovery_body):
                raise WorkUnitStoreError("disposition_recovery_tampered")
            for field in ("goal_id", "goal_revision", "node_id"):
                if policy.get(field) != dispatch[field]:
                    raise WorkUnitStoreError("disposition_policy_scope_mismatch")
            if (not policy.get("decision_id") or policy.get("max_total_launches") != MAX_EXECUTOR_LAUNCHES
                or recovery["recovery_receipt_digest"] not in policy.get("allowed_recovery_receipt_digests", [])):
                raise WorkUnitStoreError("disposition_policy_not_authorized")
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id=? AND ordinal=?", (row["run_id"], row["attempt"])).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (row["run_id"],)).fetchone()
            work = conn.execute("SELECT * FROM work_units WHERE work_unit_id=?", (row["work_unit_id"],)).fetchone()
            if not attempt or not run or not work:
                raise WorkUnitStoreError("disposition_queue_missing")
            if (attempt["state"] != "ready" or run["state"] not in {"ready", "queued"}
                or work["state"] not in {"pending", "ready"} or run["fence"] != attempt["fence"]
                or run["attempts"] != row["attempt"]):
                raise FenceError("disposition_queue_not_idle")
            lease = conn.execute("SELECT * FROM leases WHERE work_unit_id=?", (row["work_unit_id"],)).fetchone()
            if lease and lease["expires_at"] > self._now():
                raise FenceError("disposition_active_lease")
            identity = {key: dispatch[key] for key in ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt")}
            identity.update({"dispatch_key": dispatch_key, "fence": attempt["fence"],
                             "envelope_digest": row["envelope_digest"], "packet_digest": envelope["packet_digest"]})
            for key, value in identity.items():
                if recovery.get(key) != value:
                    raise FenceError("disposition_recovery_identity_mismatch")
            invocation_path = Path(_required_text("invocation_path", recovery.get("invocation_path")))
            try:
                invocation = json.loads(invocation_path.read_text())
            except (OSError, ValueError) as exc:
                raise WorkUnitStoreError("disposition_invocation_unreadable") from exc
            supplied = invocation.pop("invocation_digest", None)
            if supplied != recovery.get("invocation_digest") or supplied != digest_json(invocation):
                raise WorkUnitStoreError("disposition_invocation_tampered")
            for key, value in identity.items():
                if invocation.get(key) != value:
                    raise FenceError("disposition_invocation_identity_mismatch")
            if recovery.get("worktree") != attempt["workspace_ref"] or invocation.get("worktree") != attempt["workspace_ref"]:
                raise FenceError("disposition_old_workspace_mismatch")
            process_identity = recovery.get("process_identity", {})
            if not disposition_process_dead(process_identity, identity_port=self.identity_port):
                raise WorkUnitStoreError("disposition_process_active")
            old = disposition_workspace_inventory(attempt["workspace_ref"])
            fresh = disposition_workspace_inventory(_required_text("workspace_ref", authorization.get("workspace_ref")))
            old_path, new_path = Path(old["workspace_ref"]), Path(fresh["workspace_ref"])
            if old_path == new_path or old_path in new_path.parents or new_path in old_path.parents:
                raise WorkUnitStoreError("disposition_workspace_not_isolated")
            if not fresh["clean"] or fresh["head"] != work["base_sha"]:
                raise WorkUnitStoreError("disposition_workspace_baseline_mismatch")
            if conn.execute("SELECT 1 FROM attempts WHERE workspace_ref=?", (str(new_path),)).fetchone():
                raise WorkUnitStoreError("disposition_workspace_already_owned")
            launches = receipt.get("executor_launches")
            budget = self._delivery_retry_budget_conn(conn, row["work_unit_id"])
            cap = min(MAX_EXECUTOR_LAUNCHES, budget or MAX_EXECUTOR_LAUNCHES)
            if isinstance(launches, bool) or not isinstance(launches, int) or launches < 1 or launches >= cap or row["attempt"] >= cap:
                raise WorkUnitStoreError("disposition_budget_exhausted")
            expected = {**identity, "decision_id": policy["decision_id"],
                        "recovery_receipt_digest": recovery["recovery_receipt_digest"],
                        "expected_receipt_digest": row["receipt_digest"],
                        "old_workspace_inventory_digest": old["inventory_digest"],
                        "workspace_ref": fresh["workspace_ref"],
                        "workspace_inventory_digest": fresh["inventory_digest"]}
            if not apply:
                return {"status": "planned", "required_authorization": expected,
                        "old_workspace": old, "new_workspace": fresh,
                        "executor_launches": launches, "remaining_launches": cap-launches,
                        "executor_invocations": 0}
            _required_text("disposition_id", authorization.get("disposition_id"))
            for key, value in expected.items():
                if authorization.get(key) != value:
                    raise FenceError(f"disposition_authorization_{key}_mismatch")
            ordinal, fence, moment = int(row["attempt"])+1, int(attempt["fence"])+1, self._now()
            disposition = {"disposition_id": authorization["disposition_id"],
                           "authorization": authorization, "authorization_digest": authorization_digest,
                           "policy_digest": digest_json(policy), "previous_attempt": row["attempt"],
                           "previous_fence": attempt["fence"], "previous_workspace_ref": old["workspace_ref"],
                           "attempt": ordinal, "fence": fence, "workspace_ref": fresh["workspace_ref"],
                           "branch": fresh["branch"], "executor_launches": launches,
                           "previous_receipt": receipt, "old_workspace": old, "new_workspace": fresh}
            conn.execute("UPDATE attempts SET state='interrupted', finished_at=? WHERE run_id=? AND ordinal=?", (moment,row["run_id"],row["attempt"]))
            conn.execute("INSERT INTO attempts(run_id,ordinal,state,holder,fence,workspace_ref,created_at) VALUES(?,?,'ready',?,?,?,?)", (row["run_id"],ordinal,attempt["holder"],fence,fresh["workspace_ref"],moment))
            conn.execute("UPDATE runs SET state='queued',attempts=?,fence=?,updated_at=? WHERE run_id=?", (ordinal,fence,moment,row["run_id"]))
            conn.execute("UPDATE work_units SET state='ready',updated_at=? WHERE work_unit_id=?", (moment,row["work_unit_id"]))
            conn.execute("DELETE FROM leases WHERE work_unit_id=?", (row["work_unit_id"],))
            body = {k:v for k,v in receipt.items() if k != "receipt_digest"}
            # The archived receipt above retains the old unknown. This slot
            # belongs to the current Attempt and must admit a later unknown.
            body.pop("executor_recovery_receipt", None)
            body.update({"executor_unknown_disposition": disposition, "attempt": ordinal, "fence": fence,
                         "executor_status": "ready", "queue_state": "ready", "attempt_state": "ready", "retry_blocked": False})
            updated = {**body,"receipt_digest":digest_json(body)}
            conn.execute("UPDATE dispatch_consumptions SET attempt=?,receipt_json=?,receipt_digest=?,updated_at=? WHERE dispatch_key=?", (ordinal,json.dumps(updated,sort_keys=True),updated["receipt_digest"],moment,dispatch_key))
            self._append_event_conn(conn,event_id=f"executor-unknown-disposition:{dispatch_key}:{row['attempt']}:{attempt['fence']}",parent_goal_id=row["parent_goal_id"],work_unit_id=row["work_unit_id"],run_id=row["run_id"],event_type="successor_executor_unknown_disposition",payload=disposition,created_at=moment)
            conn.execute("COMMIT")
        return {"status":"applied", "disposition":disposition, "executor_invocations":0,
                "dispatch": self.get_dispatch_consumption(dispatch_key)}

    @staticmethod
    def _disposition_event_value(row) -> dict:
        return {"event_id": row["event_id"], "parent_goal_id": row["parent_goal_id"],
                "work_unit_id": row["work_unit_id"], "run_id": row["run_id"],
                "event_type": row["event_type"], "payload": json.loads(row["payload_json"]),
                "created_at": float(row["created_at"])}

    def _parent_state_ref_conn(self, conn, parent) -> dict | None:
        events = [self._disposition_event_value(row) for row in conn.execute(
            "SELECT * FROM events WHERE parent_goal_id=? AND event_type='parent_state_changed'",
            (parent["parent_goal_id"],)).fetchall()]
        if not events:
            return None  # Historical observation only; no invented owner or stop cause.
        for event in events:
            payload = event["payload"]
            if (not isinstance(payload, dict)
                or set(payload) != {"previous_state", "state", "updated_at", "generation"}
                or type(payload["generation"]) is not int
                or not isinstance(payload["previous_state"], str) or not isinstance(payload["state"], str)
                or payload["previous_state"] == payload["state"]
                or event["work_unit_id"] is not None or event["run_id"] is not None
                or event["created_at"] != payload["updated_at"]):
                raise WorkUnitStoreError("recovery_disposition_stop_changed")
        events.sort(key=lambda event: event["payload"]["generation"])
        for index, event in enumerate(events, 1):
            payload = event["payload"]
            if (payload["generation"] != index
                or event["event_id"] != f"parent-state:{parent['parent_goal_id']}:{index}"
                or (index > 1 and payload["previous_state"] != events[index - 2]["payload"]["state"])):
                raise WorkUnitStoreError("recovery_disposition_stop_changed")
        latest = events[-1]
        if (latest["payload"]["state"] != parent["state"]
            or latest["payload"]["updated_at"] != parent["updated_at"]):
            raise WorkUnitStoreError("recovery_disposition_stop_changed")
        return {"event_id": latest["event_id"], "event_digest": digest_json(latest)}

    @staticmethod
    def _recovery_disposition_payload(payload) -> tuple[dict, float]:
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise WorkUnitStoreError("recovery_disposition_schema_invalid")
                value[key] = item
            return value

        if (not isinstance(payload, dict)
            or set(payload) != {"disposition", "raw_json", "approved_disposition_digest"}
            or not isinstance(payload["raw_json"], str)):
            raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        raw = payload["raw_json"].encode("utf-8")
        if "sha256:" + hashlib.sha256(raw).hexdigest() != payload["approved_disposition_digest"]:
            raise WorkUnitStoreError("recovery_disposition_digest_mismatch")
        try:
            document = json.loads(raw, object_pairs_hook=unique)
        except (ValueError, UnicodeError) as exc:
            raise WorkUnitStoreError("recovery_disposition_schema_invalid") from exc
        if (not isinstance(document, dict) or document != payload["disposition"]
            or set(document) != {"schema", "disposition_id", "goal_id", "goal_revision",
                "manifest_digest", "stop_ref", "request_ref", "scope", "expires_at"}
            or document["schema"] != "lh-task-area-recovery-disposition/v1"
            or document["scope"] != "closeout_only"
            or type(document["goal_revision"]) is not int or document["goal_revision"] < 1):
            raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        for field in ("disposition_id", "goal_id"):
            value = document[field]
            if not isinstance(value, str) or not value or value != value.strip():
                raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        request_ref, stop_ref = document["request_ref"], document["stop_ref"]
        if (not isinstance(request_ref, dict)
            or set(request_ref) != {"request_id", "request_digest", "authority_digest"}
            or not isinstance(request_ref["request_id"], str) or not request_ref["request_id"]
            or request_ref["request_id"] != request_ref["request_id"].strip()
            or not isinstance(stop_ref, dict) or set(stop_ref) != {"state", "updated_at", "state_event_ref"}
            or stop_ref["state"] != "stopped" or type(stop_ref["updated_at"]) not in {int, float}
            or not math.isfinite(stop_ref["updated_at"])):
            raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        for value in (document["manifest_digest"], request_ref["request_digest"], request_ref["authority_digest"]):
            if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
                raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        ref = stop_ref["state_event_ref"]
        if ref is not None and (not isinstance(ref, dict) or set(ref) != {"event_id", "event_digest"}
            or not isinstance(ref["event_id"], str) or not ref["event_id"]
            or not isinstance(ref["event_digest"], str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", ref["event_digest"]) is None):
            raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        try:
            expiry = datetime.fromisoformat(document["expires_at"].replace("Z", "+00:00"))
            if expiry.tzinfo is None or expiry.utcoffset().total_seconds() != 0:
                raise ValueError("not UTC")
            expires_at = expiry.timestamp()
        except (AttributeError, TypeError, ValueError, OverflowError) as exc:
            raise WorkUnitStoreError("recovery_disposition_expiry_invalid") from exc
        return document, expires_at

    def _recovery_disposition_conn(self, conn, event_id, *, request_id=None,
                                   manifest_digest=None, payload=None,
                                   require_accepted=True, own_call_id=None) -> dict:
        """Read-only validation on the caller's transaction; never reconcile claims."""
        if not isinstance(event_id, str) or not event_id:
            raise WorkUnitStoreError("recovery_disposition_exact_approval_missing")
        source_row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        source = self._disposition_event_value(source_row) if source_row is not None else None
        if payload is None:
            if source is None:
                raise WorkUnitStoreError("recovery_disposition_exact_approval_missing")
            payload = source["payload"]
        elif source is not None and source["payload"] != payload:
            raise WorkUnitStoreError("recovery_disposition_event_conflict")
        document, expiry = self._recovery_disposition_payload(payload)
        ref = document["request_ref"]
        if (event_id != "task-area-recovery-disposition:" + ref["request_id"]
            or (request_id is not None and request_id != ref["request_id"])
            or (manifest_digest is not None and manifest_digest != document["manifest_digest"])):
            raise WorkUnitStoreError("recovery_disposition_request_mismatch")
        record = self._recovery_record_from_row(self._recovery_record_row_conn(conn, ref["request_id"]))
        if record is None:
            raise WorkUnitStoreError("recovery_disposition_request_mismatch")
        request = record["request"]
        if (ref != {"request_id": record["request_id"], "request_digest": record["request_digest"],
                    "authority_digest": request["authority_digest"]}
            or record["request_digest"] != digest_json(request)
            or document["manifest_digest"] != request["authority_digest"]
            or any(document[key] != request[key] for key in ("goal_id", "goal_revision"))
            or request.get("phase") != "machine_complete" or request.get("reason_code") != "machine_complete"):
            raise WorkUnitStoreError("recovery_disposition_request_mismatch")
        run = self._recovery_check_run_identity_conn(conn, request, require_current=True)
        unit = conn.execute("SELECT state FROM work_units WHERE work_unit_id=?", (request["work_unit_id"],)).fetchone()
        if run["state"] != "integrated" or unit is None or unit["state"] != "integrated":
            raise WorkUnitStoreError("recovery_disposition_scope_invalid")
        parent = self._parent_or_raise(conn, run["parent_goal_id"])
        if parent["parent_goal_id"] != document["goal_id"]:
            raise WorkUnitStoreError("recovery_disposition_request_mismatch")
        stop_ref = {"state": parent["state"], "updated_at": parent["updated_at"],
                    "state_event_ref": self._parent_state_ref_conn(conn, parent)}
        if stop_ref != document["stop_ref"] or parent["state"] != "stopped":
            raise WorkUnitStoreError("recovery_disposition_stop_changed")
        if source is not None and (source["event_type"] != "task_area_recovery_disposition"
            or source["parent_goal_id"] != request["goal_id"]
            or source["work_unit_id"] != request["work_unit_id"] or source["run_id"] != request["run_id"]):
            raise WorkUnitStoreError("recovery_disposition_event_conflict")
        outcome = {"status": "accepted", "kind": "recovery_closeout_disposition",
                   "manifest_digest": document["manifest_digest"], "request_ref": ref,
                   "stop_ref": document["stop_ref"],
                   "disposition_digest": payload["approved_disposition_digest"]}
        applied = record.get("status") == "applied"
        if require_accepted or applied:
            if source is None:
                raise WorkUnitStoreError("recovery_disposition_not_accepted")
            claims, settlements = [], []
            for row in conn.execute(
                "SELECT * FROM events WHERE parent_goal_id=? AND event_type IN "
                "('task_area_event_claimed','task_area_event_settled')", (request["goal_id"],)).fetchall():
                event = self._disposition_event_value(row)
                if event["payload"].get("source_event_id") == event_id:
                    (claims if event["event_type"] == "task_area_event_claimed" else settlements).append(event)
            source_digest = digest_json({key: source[key] for key in
                ("event_id", "parent_goal_id", "event_type", "payload", "created_at")})
            expected_claim = {"source_event_id": event_id, "source_event_digest": source_digest,
                "claim_id": "task-area-event-claim:" + digest_json({
                    "source_event_id": event_id, "source_event_digest": source_digest})}
            expected_settlement = {"source_event_id": event_id, "claim_id": expected_claim["claim_id"],
                                   "outcome": outcome, "outcome_digest": digest_json(outcome)}
            if (len(claims) != 1 or len(settlements) != 1
                or claims[0]["event_id"] != "task-area-event-claimed:" + event_id
                or settlements[0]["event_id"] != "task-area-event-settled:" + event_id
                or any(event["work_unit_id"] is not None or event["run_id"] is not None
                       for event in claims + settlements)
                or claims[0]["payload"] != expected_claim or settlements[0]["payload"] != expected_settlement):
                raise WorkUnitStoreError("recovery_disposition_not_accepted")
        payload_digest = digest_json(payload)
        if applied:
            _, plan, _ = self._recovery_validate_plan(record)
            apply = record.get("apply") or {}
            if (plan.get("action") != {"kind": "resume_phase"} or plan.get("target_phase") != "closeout"
                or apply.get("action") != plan["action"] or apply.get("state") != "applied"
                or apply.get("request_digest") != ref["request_digest"]
                or apply.get("plan_digest") != plan["plan_digest"]
                or apply.get("acknowledgement") != "closeout_already_completed"
                or apply.get("disposition_event_id") != event_id
                or apply.get("disposition_payload_digest") != payload_digest):
                raise WorkUnitStoreError("recovery_disposition_scope_invalid")
        else:
            run_records = self._recovery_records_for_run_conn(conn, request["run_id"])
            all_claims = [(other, claim) for _, other in run_records for claim in other.get("claims", [])]
            if (any(other.get("status") == "outcome_unknown" for _, other in run_records)
                or any(claim.get("state") in {"unknown", "outcome_unknown"} for _, claim in all_claims)):
                raise WorkUnitStoreError("recovery_disposition_unknown_effect")
            inflight = [(other, claim) for other, claim in all_claims if claim.get("state") == "claimed"]
            owner = self.identity_port.current() if own_call_id is not None else None
            owner = owner.as_dict() if callable(getattr(owner, "as_dict", None)) else owner
            if inflight and not (own_call_id is not None and len(inflight) == 1
                and inflight[0][0]["request_id"] == ref["request_id"]
                and inflight[0][1].get("call_id") == own_call_id
                and inflight[0][1].get("phase") in {"planner", "plan_verifier"}
                and isinstance(owner, Mapping) and inflight[0][1].get("owner_process_identity") == owner):
                raise WorkUnitStoreError("recovery_disposition_inflight")
            if own_call_id is not None and not inflight:
                raise WorkUnitStoreError("recovery_disposition_inflight")
            if record.get("status") not in {"requested", "claimed", "result_recorded", "verifier_claimed", "plan_verified"}:
                raise WorkUnitStoreError("recovery_disposition_request_mismatch")
            if not require_accepted and record.get("status") != "requested":
                raise WorkUnitStoreError("recovery_disposition_request_mismatch")
            if self._now() >= expiry:
                raise WorkUnitStoreError("recovery_disposition_expired")
            if any(other["budget_limits"] != record["budget_limits"] for _, other in run_records):
                raise WorkUnitStoreError("recovery_disposition_budget_changed")
            deadlines = [claim["incident_deadline_at"] for _, claim in all_claims]
            if deadlines and self._now() >= min(deadlines):
                raise WorkUnitStoreError("recovery_disposition_deadline_exhausted")
        return {"event_id": event_id, "payload_digest": payload_digest,
                "disposition": document, "outcome": outcome, "record": record}

    def validate_recovery_disposition(self, event_id: str, *, request_id=None,
                                      manifest_digest=None, own_call_id=None,
                                      require_accepted=True) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN")
            return self._recovery_disposition_conn(conn, event_id, request_id=request_id,
                manifest_digest=manifest_digest, own_call_id=own_call_id, require_accepted=require_accepted)

    def record_task_area_recovery_disposition(self, *, raw_json: bytes,
                                              approved_disposition_digest: str,
                                              manifest_digest: str) -> dict:
        if self.read_only:
            raise WorkUnitStoreError("read_only_recovery_disposition")
        if not approved_disposition_digest or not manifest_digest:
            raise WorkUnitStoreError("recovery_disposition_exact_approval_missing")
        if not isinstance(raw_json, bytes):
            raise WorkUnitStoreError("recovery_disposition_schema_invalid")
        payload = {"raw_json": raw_json.decode("utf-8"), "disposition": json.loads(raw_json),
                   "approved_disposition_digest": approved_disposition_digest}
        document, _ = self._recovery_disposition_payload(payload)
        event_id = "task-area-recovery-disposition:" + document["request_ref"]["request_id"]
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            context = self._recovery_disposition_conn(conn, event_id, payload=payload,
                manifest_digest=manifest_digest, require_accepted=False)
            prior = conn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
            request = context["record"]["request"]
            self._append_event_conn(conn, event_id=event_id, parent_goal_id=document["goal_id"],
                work_unit_id=request["work_unit_id"], run_id=request["run_id"],
                event_type="task_area_recovery_disposition", payload=payload, created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": event_id, "appended": prior is None}

    def record_task_area_approval(self, *, event_id: str, parent_goal_id: str,
                                 goal_revision: int, base_sha: str,
                                 payload: dict[str, Any], require_existing: bool = False) -> dict:
        """Record a validated approval and its parent in one atomic transaction."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_task_area_approval")
        event_id = _required_text("event_id", event_id)
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        base_sha = _required_text("base_sha", base_sha)
        if (isinstance(goal_revision, bool) or not isinstance(goal_revision, int)
            or goal_revision < 1):
            raise WorkUnitStoreError("task_area_approval_parent_changed")
        if not isinstance(payload, dict):
            raise WorkUnitStoreError("task_area_approval_payload_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute(
                "SELECT goal_id, goal_revision, base_sha, state FROM parent_goals WHERE parent_goal_id=?",
                (parent_goal_id,)).fetchone()
            if parent is not None and tuple(parent) != (parent_goal_id, goal_revision, base_sha, "active"):
                raise WorkUnitStoreError("task_area_approval_parent_changed")
            prior = conn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
            if require_existing and (parent is None or prior is None):
                raise WorkUnitStoreError("reviewed_task_list_approval_event_missing")
            now = self._now()
            if parent is None:
                conn.execute(
                    "INSERT INTO parent_goals(parent_goal_id, goal_id, goal_revision, base_sha, "
                    "dependencies_json, state, created_at, updated_at) VALUES (?, ?, ?, ?, '[]', 'active', ?, ?)",
                    (parent_goal_id, parent_goal_id, goal_revision, base_sha, now, now))
            self._append_event_conn(
                conn, event_id=event_id, parent_goal_id=parent_goal_id,
                work_unit_id=None, run_id=None, event_type="task_list_approval_updated",
                payload=payload, created_at=now)
            conn.execute("COMMIT")
        return {"event_id": event_id, "appended": prior is None}

    def record_task_area_prerequisite(self, *, event_id: str, parent_goal_id: str,
                                      goal_revision: int, payload: dict[str, Any]) -> dict:
        """Append a controller-validated observation once, never a Run/claim."""
        if self.read_only:
            raise WorkUnitStoreError("read_only_task_area_prerequisite")
        event_id = _required_text("event_id", event_id)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute(
                "SELECT goal_revision, state FROM parent_goals WHERE parent_goal_id=?",
                (parent_goal_id,)).fetchone()
            if parent is None or parent["goal_revision"] != goal_revision or parent["state"] != "active":
                raise WorkUnitStoreError("task_area_prerequisite_parent_changed")
            existed = conn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
            self._append_event_conn(conn, event_id=event_id, parent_goal_id=parent_goal_id,
                work_unit_id=None, run_id=None, event_type="task_area_prerequisite_updated",
                payload=payload, created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": event_id, "appended": existed is None}

    def record_task_source_result(self, *, event_id: str, parent_goal_id: str,
                                  goal_revision: int, kind: str, payload: dict) -> dict:
        """One task-only validation reservation/result in the original event log."""
        if self.read_only or kind not in {"task_source_result_claimed", "task_source_result_verified"}:
            raise WorkUnitStoreError("task_source_result_event_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute("SELECT goal_revision,state FROM parent_goals WHERE parent_goal_id=?",
                                  (parent_goal_id,)).fetchone()
            if parent is None or parent["goal_revision"] != goal_revision or parent["state"] != "active":
                raise WorkUnitStoreError("task_source_result_parent_changed")
            prior = conn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
            self._append_event_conn(conn, event_id=event_id, parent_goal_id=parent_goal_id,
                work_unit_id=None, run_id=None, event_type=kind, payload=payload, created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id":event_id, "appended":prior is None}

    def bind_phase_policy(self, manifest: dict) -> dict:
        """Activate only a source-approved durable task, in this existing Store."""
        from .task_area import TaskAreaController, manifest_digest
        controller = TaskAreaController(self, consumer_factory=lambda task: None,
                                        approved_manifest_digest=manifest_digest(manifest))
        approved = controller._verify_reviewed_binding(manifest)
        if not any(task.get("status") == "approved" and task.get("phase_execution") == "durable-v1"
                   and task.get("node_id") in (approved or {}) for task in manifest["tasks"]):
            raise WorkUnitStoreError("phase_job_approved_task_missing")
        if controller._reviewed_approval_event_receipt(manifest) is None:
            raise WorkUnitStoreError("phase_job_approval_settlement_missing")
        material = controller.phase_policy_material(manifest)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Re-read the source/configuration at the policy commit boundary.
            if controller.phase_policy_material(manifest) != material:
                raise WorkUnitStoreError("phase_job_policy_source_changed")
            prior = self._phase_policy_conn(conn)
            if prior is None:
                self._append_event_conn(conn, event_id="task-area-phase-policy:durable-v1",
                    parent_goal_id=manifest["goal_id"], work_unit_id=None, run_id=None,
                    event_type="task_area_phase_policy_bound", payload=material, created_at=self._now())
                prior = self._phase_policy_conn(conn)
            elif any(prior["payload"].get(key) != material.get(key)
                     for key in ("schema", "phase_execution", "scope")):
                raise WorkUnitStoreError("phase_job_policy_scope_mismatch")
            if any("resource_requirements" in task for task in manifest["tasks"] if task.get("status") == "approved"):
                self._append_event_conn(conn,
                    event_id="task-area-phase-resources:" + digest_json(material["source_approval_ref"]),
                    parent_goal_id=manifest["goal_id"], work_unit_id=None, run_id=None,
                    event_type="task_area_phase_resources_bound", payload=material, created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": prior["event_id"], "event_digest": digest_json(prior)}

    def _phase_policy_conn(self, conn) -> dict | None:
        chain = self._phase_policy_chain_conn(conn)
        return chain[-1] if chain else None

    def _phase_policy_chain_conn(self, conn) -> list[dict]:
        row = conn.execute("SELECT * FROM events WHERE event_id=?",
                           ("task-area-phase-policy:durable-v1",)).fetchone()
        if row is None:
            return []
        chain = [self._disposition_event_value(row)]
        for settled in conn.execute("SELECT * FROM events WHERE event_type='task_area_event_settled' ORDER BY rowid"):
            settlement = json.loads(settled["payload_json"])
            outcome = settlement.get("outcome", {})
            if outcome.get("kind") != "capacity_policy" or outcome.get("status") != "accepted":
                continue
            source = conn.execute("SELECT * FROM events WHERE event_id=?", (settlement["source_event_id"],)).fetchone()
            if source is None or source["event_type"] != "task_area_capacity_policy_updated":
                raise WorkUnitStoreError("capacity_policy_event_invalid")
            event = self._disposition_event_value(source)
            prior = chain[-1]
            if (outcome != self._capacity_outcome_value(event)
                or settlement.get("outcome_digest") != digest_json(outcome)
                or outcome["supersedes_policy_ref"] != {"event_id": prior["event_id"], "event_digest": digest_json(prior)}
                or event["payload"]["scope"] != prior["payload"]["scope"]):
                raise WorkUnitStoreError("capacity_policy_event_invalid")
            chain.append(event)
        return chain

    @staticmethod
    def _capacity_outcome_value(event: dict) -> dict:
        payload = event["payload"]
        return {"status": "accepted", "kind": "capacity_policy", "max_workers": payload["max_workers"],
                "supersedes_policy_ref": payload["capacity_change"]["supersedes_policy_ref"],
                "source_approval_ref": payload["source_approval_ref"]}

    def _capacity_material_conn(self, conn, *, manifest_path, approved_digest, source_ref,
                                replay_event_id=None) -> tuple[str, dict]:
        from .platform_ports import read_private_file
        from .task_area import TaskAreaController, manifest_digest
        manifest = json.loads(read_private_file(Path(manifest_path)))
        controller = TaskAreaController(self, consumer_factory=lambda task: None, approved_manifest_digest=approved_digest)
        if manifest_digest(manifest) != approved_digest:
            raise WorkUnitStoreError("capacity_manifest_changed")
        controller._validate(manifest)
        controller._verify_reviewed_binding(manifest)
        if controller._reviewed_approval_event_receipt(manifest) is None:
            raise WorkUnitStoreError("capacity_approval_missing")
        source, _ = controller._read_approval_source(source_ref)
        change = source.get("capacity_change")
        if (not isinstance(change, dict) or set(change) != {"schema", "supersedes_policy_ref", "runner_config_ref",
                "target_max_workers", "budget_refs", "restore_condition"}
            or change.get("schema") != "lh-task-area-capacity-change/v1"
            or source.get("allowed_actions") != ["change_capacity"]):
            raise WorkUnitStoreError("capacity_approval_missing")
        event_name = source.get("approval_event_id")
        if not isinstance(event_name, str) or not event_name.strip() or event_name != event_name.strip():
            raise WorkUnitStoreError("capacity_approval_missing")
        event_id = "task-area-capacity-policy:" + event_name
        limit = change["target_max_workers"]
        if type(limit) is not int or limit not in (2, 3):
            raise WorkUnitStoreError("capacity_limit_unsupported")
        chain = self._phase_policy_chain_conn(conn)
        if not chain:
            raise WorkUnitStoreError("capacity_policy_changed")
        current = chain[-1]
        material = controller.phase_policy_material(manifest)
        config = controller._read_resource_ref(change["runner_config_ref"])
        if (change["runner_config_ref"] != source.get("runner_config_ref")
            or change["runner_config_ref"] != material["runner_config_ref"]
            or {key: config.get(key) for key in ("store_root", "host_contract_ref")} != material["scope"]
            or material["scope"] != current["payload"]["scope"]):
            raise WorkUnitStoreError("capacity_scope_mismatch")
        original = manifest["reviewed_task_list_binding"]
        if (source.get("scope_digest") != original["authorization"]["scope_digest"]
            or source.get("approved_task_digests") != original["authorization"]["scope"]["approved_task_digests"]
            or source.get("task_list_digest") != original["task_list_digest"]):
            raise WorkUnitStoreError("capacity_scope_mismatch")
        payload = {"schema": material["schema"], "phase_execution": "durable-v1", "scope": material["scope"],
                   "max_workers": limit, "source_approval_ref": source_ref, "capacity_change": change,
                   "manifest_path": str(Path(manifest_path).resolve()), "manifest_digest": approved_digest}
        replay = next((event for event in chain
                       if replay_event_id == event_id == event["event_id"]), None)
        if replay is not None and (replay["payload"] != payload
                or replay["parent_goal_id"] != manifest["goal_id"]
                or replay["work_unit_id"] is not None or replay["run_id"] is not None):
            raise WorkUnitStoreError("capacity_policy_event_invalid")
        refs = change["budget_refs"]
        if not isinstance(refs, list):
            raise WorkUnitStoreError("capacity_budget_refs_mismatch")
        ref_digests = set(map(digest_json, refs))
        approved_refs = []
        for row in conn.execute("SELECT * FROM events WHERE event_type='task_list_approval_updated'"):
            accepted = conn.execute("SELECT payload_json FROM events WHERE event_id=?",
                ("task-area-event-settled:" + row["event_id"],)).fetchone()
            if accepted is None or json.loads(accepted[0]).get("outcome", {}).get("status") != "accepted":
                continue
            ref = json.loads(row["payload_json"])["source_approval_ref"]
            # Exact accepted replay revalidates its original bound refs and
            # bytes, not additional authorities admitted after its settlement.
            if replay is not None and digest_json(ref) not in ref_digests:
                continue
            authority, _ = controller._read_approval_source(ref)
            if authority.get("runner_config_ref") == material["runner_config_ref"]:
                approved_refs.append(ref)
        if (len(refs) != len(approved_refs)
            or sorted(map(digest_json, refs)) != sorted(map(digest_json, approved_refs))
            or source.get("budget") != original["authorization"]["budget"]
            or source.get("stop_conditions") != original["authorization"]["stop_conditions"]):
            raise WorkUnitStoreError("capacity_budget_refs_mismatch")
        restore = change["restore_condition"]
        if (not isinstance(restore, dict) or set(restore) != {"kind", "policy_ref"}
            or restore.get("kind") != "superseded_by_approved_policy"
            or not any(restore["policy_ref"] == {"event_id": event["event_id"], "event_digest": digest_json(event)}
                       and event["payload"]["max_workers"] == 2 for event in chain)):
            raise WorkUnitStoreError("capacity_restore_ref_invalid")
        # A settled replay retains its original result; a new application is a
        # CAS against the effective version in this same write transaction.
        if replay is None and change["supersedes_policy_ref"] != {
                "event_id": current["event_id"], "event_digest": digest_json(current)}:
            raise WorkUnitStoreError("capacity_policy_changed")
        return event_id, payload

    def record_task_area_capacity_policy(self, *, manifest_path, approved_digest, source_ref) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            from .task_area import TaskAreaController
            source, _ = TaskAreaController._read_approval_source(source_ref)
            event_id = "task-area-capacity-policy:" + str(source.get("approval_event_id", ""))
            prior = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
            event_id, payload = self._capacity_material_conn(conn, manifest_path=manifest_path,
                approved_digest=approved_digest, source_ref=source_ref, replay_event_id=event_id if prior else None)
            from .task_area import manifest_digest
            manifest = json.loads(Path(manifest_path).read_bytes())
            if manifest_digest(manifest) != approved_digest:
                raise WorkUnitStoreError("capacity_manifest_changed")
            parent = self._parent_or_raise(conn, manifest["goal_id"])
            if parent["state"] != "active" or parent["goal_revision"] != manifest["goal_revision"]:
                raise WorkUnitStoreError("capacity_scope_mismatch")
            self._append_event_conn(conn, event_id=event_id, parent_goal_id=parent["parent_goal_id"],
                work_unit_id=None, run_id=None, event_type="task_area_capacity_policy_updated", payload=payload,
                created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": event_id, "appended": prior is None}

    def _capacity_settlement_conn(self, conn, source) -> dict:
        event = self._disposition_event_value(source)
        payload = event["payload"]
        event_id, material = self._capacity_material_conn(conn, manifest_path=payload["manifest_path"],
            approved_digest=payload["manifest_digest"], source_ref=payload["source_approval_ref"], replay_event_id=event["event_id"])
        if event_id != event["event_id"] or material != payload:
            raise WorkUnitStoreError("capacity_policy_event_invalid")
        return self._capacity_outcome_value(event)

    def capacity_policy_outcome(self, event_id: str) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN")
            source = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
            if source is None or source["event_type"] != "task_area_capacity_policy_updated":
                raise WorkUnitStoreError("capacity_policy_event_invalid")
            return self._capacity_settlement_conn(conn, source)

    def _phase_context_conn(self, conn, context: dict, *, admission: bool = False) -> tuple[dict, dict]:
        from .platform_ports import read_private_file
        from .task_area import TaskAreaController, manifest_digest
        if not isinstance(context, dict):
            raise WorkUnitStoreError("phase_job_context_missing")
        manifest = json.loads(read_private_file(Path(context["manifest_path"])))
        if manifest_digest(manifest) != context.get("manifest_digest"):
            raise WorkUnitStoreError("phase_job_manifest_changed")
        controller = TaskAreaController(self, consumer_factory=lambda task: None,
                                        approved_manifest_digest=context["manifest_digest"])
        approved = controller._verify_reviewed_binding(manifest)
        task = next((task for task in manifest["tasks"]
                     if task.get("node_id") == context.get("node_id")), None)
        if (task is None or task.get("status") != "approved"
            or task.get("phase_execution") != "durable-v1"
            or (approved or {}).get(task["node_id"]) != context.get("task_digest")):
            raise WorkUnitStoreError("phase_job_task_authority_changed")
        chain = self._phase_policy_chain_conn(conn)
        policy = next((event for event in chain if context.get("policy_ref") == {
                      "event_id": event["event_id"], "event_digest": digest_json(event)}), None)
        if (policy is None or (admission and policy != chain[-1])
            or policy["payload"]["scope"] != controller.phase_policy_material(manifest)["scope"]):
            raise WorkUnitStoreError("phase_job_policy_binding_changed")
        if not admission:
            row = conn.execute("SELECT receipt_json FROM dispatch_consumptions WHERE dispatch_key=?",
                               (task["envelope"]["dispatch_key"],)).fetchone()
            receipt = json.loads(row[0]) if row is not None else {}
            if receipt.get("admission_policy_ref", context["policy_ref"] if policy == chain[0] else None) != context["policy_ref"]:
                raise WorkUnitStoreError("phase_job_policy_binding_changed")
        if "resource_requirements" in task and context.get("resource_requirements_digest") != digest_json(task["resource_requirements"]):
            raise WorkUnitStoreError("phase_job_resource_binding_changed")
        if controller._reviewed_approval_event_receipt(manifest) is None:
            raise WorkUnitStoreError("phase_job_approval_settlement_missing")
        return manifest, task

    def _phase_occupied_conn(self, conn) -> set[str]:
        occupied = {row[0] for row in conn.execute("SELECT work_unit_id FROM work_units "
                    "WHERE state IN ('ready','running','verified','retry_pending')")}
        for row in conn.execute("SELECT work_unit_id,receipt_json FROM dispatch_consumptions"):
            if json.loads(row["receipt_json"]).get("executor_status") == "unknown":
                occupied.add(row["work_unit_id"])
        for job in self._phase_jobs_conn(conn):
            if job["status"] != "settled":
                occupied.add(job["input"]["work_unit_id"])
        for row in conn.execute("SELECT request_json FROM planning_requests"):
            record = json.loads(row[0])
            if record.get("schema") == RECOVERY_RECORD_SCHEMA and (
                record.get("status") in {"requested", "claimed", "audit_claimed", "audit_recorded", "result_recorded",
                    "verifier_claimed", "plan_verified", "outcome_unknown", "awaiting_authority", "awaiting_evidence"}
                or any(claim.get("state") in {"claimed", "unknown", "outcome_unknown"} for claim in record.get("claims", []))):
                occupied.add(record["request"]["work_unit_id"])
        return occupied

    def _phase_admission_conn(self, conn, work_unit_id, context, envelope, resources=None):
        policy = self._phase_policy_conn(conn)
        if policy is None:
            if context is not None or resources is not None:
                raise WorkUnitStoreError("phase_job_policy_missing")
            return
        if context is None:
            raise WorkUnitStoreError("phase_job_legacy_mixed_admission_unsupported")
        manifest, task = self._phase_context_conn(conn, context, admission=True)
        if task["envelope"]["envelope_digest"] != envelope["envelope_digest"]:
            raise WorkUnitStoreError("phase_job_dispatch_binding_changed")
        if task.get("resource_requirements") != resources:
            raise WorkUnitStoreError("phase_job_resource_binding_changed")
        if resources is None and conn.execute("SELECT 1 FROM events WHERE event_type='task_area_phase_resources_bound' LIMIT 1").fetchone():
            raise WorkUnitStoreError("phase_job_legacy_mixed_admission_unsupported")
        from .task_area import TaskAreaController
        controller = TaskAreaController(self, consumer_factory=lambda task: None, approved_manifest_digest=context["manifest_digest"])
        material = controller.resource_material(manifest, task)
        occupied = self._phase_occupied_conn(conn)
        if material is not None:
            self._phase_conflicts_conn(conn, work_unit_id, task, material, occupied)
        if len(occupied - {work_unit_id}) >= policy["payload"]["max_workers"]:
            raise WorkUnitStoreError("task_area_capacity_exhausted")
        return material

    def _phase_conflicts_conn(self, conn, work_unit_id, task, material, occupied):
        for row in conn.execute("SELECT * FROM work_units WHERE work_unit_id != ?", (work_unit_id,)):
            if row["work_unit_id"] not in occupied:
                continue
            dispatch = conn.execute("SELECT receipt_json FROM dispatch_consumptions WHERE work_unit_id=?", (row["work_unit_id"],)).fetchone()
            other = json.loads(dispatch[0]).get("resource_material") if dispatch is not None else None
            if other is None:
                raise WorkUnitStoreError("task_area_resource_binding_unknown")
            conflicts = read_write_conflicts(task, self._work_row(row)) if other["repository"] == material["repository"] else []
            reason = "task_area_read_write_overlap"
            if not conflicts:
                reason = "task_area_resource_conflict"
                for left in material["resources"]:
                    for right in other["resources"]:
                        if (left["kind"] == right["kind"] == "api"
                            and left["resource_key"] != right["resource_key"]
                            and not (left["access"] == right["access"] == "read"
                                     and left["version_digest"] == right["version_digest"])):
                            # Separate contract paths/versions do not prove
                            # independent effects. No alias identity is invented.
                            raise WorkUnitStoreError("task_area_resource_binding_unknown")
                        if (left["resource_key"] == right["resource_key"]
                            and ("write" in (left["access"], right["access"])
                                 or (left["kind"] == "api" and left["version_digest"] != right["version_digest"]))):
                            conflicts.append({"kind": left["kind"], "identity": left["identity"],
                                              "left_ref": left["ref"], "right_ref": right["ref"]})
            if conflicts:
                error = WorkUnitStoreError(reason)
                error.conflicts = [{"work_unit_id": row["work_unit_id"], **conflict} for conflict in conflicts]
                raise error

    def _phase_slots_conn(self, conn) -> dict:
        slots = {"model": {"limit": 2, "owners": []}, "heavy": {"limit": 1, "owners": []},
                 "integration": {"limit": 1, "owners": []}}
        for job in self._phase_jobs_conn(conn):
            if job["status"] in {"waiting", "settled"}:
                continue
            for name, identity in job.get("slot_requirements", {}).items():
                slots[name]["owners"].append({**{key: job["input"][key] for key in
                    ("work_unit_id", "run_id", "attempt", "fence", "phase")}, "job_id": job["job_id"],
                    "resource_key": identity, "state": "unknown" if job["status"] == "outcome_unknown" else job["status"]})
        for row in conn.execute("SELECT request_json FROM planning_requests"):
            record = json.loads(row[0])
            if record.get("schema") != RECOVERY_RECORD_SCHEMA:
                continue
            for claim in record.get("claims", []):
                if claim.get("state") not in {"claimed", "unknown", "outcome_unknown"}:
                    continue
                slots["model"]["owners"].append({**{key: record["request"][key] for key in
                    ("work_unit_id", "run_id", "attempt", "fence")}, "phase": claim["phase"],
                    "state": "unknown" if claim.get("state") in {"unknown", "outcome_unknown"} else "running",
                    "resource_key": "local-store-model", "call_id": claim["call_id"]})
        return slots

    def phase_resource_readback(self) -> dict | None:
        with self._connect() as conn:
            conn.execute("BEGIN")
            policy = self._phase_policy_conn(conn)
            if policy is None:
                return None
            occupied = sorted(self._phase_occupied_conn(conn))
            return {"policy_ref": {"event_id": policy["event_id"], "event_digest": digest_json(policy)},
                    "max_workers": policy["payload"]["max_workers"], "occupied_work_unit_ids": occupied,
                    "draining": len(occupied) > policy["payload"]["max_workers"], "slots": self._phase_slots_conn(conn)}

    def _phase_slot_requirements(self, manifest, task, phase):
        from .task_area import TaskAreaController
        controller = TaskAreaController(self, consumer_factory=lambda task: None, approved_manifest_digest=None)
        material = controller.resource_material(manifest, task)
        required = {}
        if phase in {"coding", "verifier", "integration", "integration_verifier"}:
            required["model"] = "local-store-model"
        if material is not None and phase in material["heavy_phases"]:
            required["heavy"] = "local-store-heavy"
        if phase == "integration":
            required["integration"] = (material["repository"] if material else
                str(Path(controller._resource_git_path(task["envelope"]["worktree"], "objects")).parent))
            if material is not None:
                # One job owns both keys, in the original all-slots transaction.
                # Keep the repository key: a different target is not a new slot.
                required["integration"] = [required["integration"],
                    str(Path(task["completion_contract"]["integration_worktree"]).resolve())]
        return required

    def _phase_waiting_slot_conn(self, conn, required, job_id):
        slots = self._phase_slots_conn(conn)
        for name, identity in required.items():
            identities = set(identity if isinstance(identity, list) else [identity])
            owners = [owner for owner in slots[name]["owners"]
                      if owner.get("job_id") != job_id and identities.intersection(
                          owner["resource_key"] if isinstance(owner["resource_key"], list) else [owner["resource_key"]])]
            if len(owners) >= slots[name]["limit"]:
                return "resource_slot_waiting:" + name
        return None

    def acquire_phase_job_slots(self, job_id: str) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self._phase_job_conn(conn, job_id)
            if job["status"] != "waiting":
                return job
            self._phase_job_authority_conn(conn, job)
            reason = self._phase_waiting_slot_conn(conn, job["slot_requirements"], job_id)
            if reason is not None:
                return {**job, "waiting_reason": reason}
            item = job["input"]
            acquired = {"job_id": job_id, "input_digest": job["input_digest"],
                        "creator_identity": NativeProcessIdentityPort().current().as_dict()}
            if "runtime_slice_seconds" in job:
                acquired["deadline_at"] = self._now() + job["runtime_slice_seconds"]
            self._append_event_conn(conn, event_id="phase-job-slots-acquired:" + job_id,
                parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                event_type="task_area_phase_slots_acquired", payload=acquired, created_at=self._now())
            conn.execute("COMMIT")
        return self.read_phase_job(job_id, reconcile=False)

    def _phase_jobs_conn(self, conn, goal_id=None) -> list[dict]:
        query = "SELECT * FROM events WHERE event_type='task_area_phase_job_reserved'"
        params = ()
        if goal_id is not None:
            query += " AND parent_goal_id=?"
            params = (goal_id,)
        jobs = []
        for row in conn.execute(query + " ORDER BY rowid", params).fetchall():
            reserved = json.loads(row["payload_json"])
            job_id = reserved["job_id"]
            job = {**reserved, "status": "reserved", "carrier_identity": None,
                   "result_ref": None, "result_digest": None, "result_event_id": None,
                   "deadline_at": reserved["input"]["deadline_at"]}
            if "slot_requirements" in reserved:
                acquired = conn.execute("SELECT payload_json FROM events WHERE event_id=?",
                    ("phase-job-slots-acquired:" + job_id,)).fetchone()
                if acquired is None:
                    job["status"] = "waiting"
                    if "runtime_slice_seconds" in reserved:
                        # Queue time is not execution time: the slice has not started.
                        job["deadline_at"] = None
                else:
                    claim = json.loads(acquired[0])
                    if claim.get("input_digest") != job["input_digest"]:
                        raise WorkUnitStoreError("phase_job_event_binding_mismatch")
                    job["creator_identity"] = claim["creator_identity"]
                    if "deadline_at" in claim:
                        job["deadline_at"] = claim["deadline_at"]
            if digest_json(job["input"]) != job["input_digest"]:
                raise WorkUnitStoreError("phase_job_input_digest_mismatch")
            for prefix, status in (("phase-job-claimed:", "running"),
                                   ("phase-job-unknown:", "outcome_unknown"),
                                   ("phase-job-result:", "result_ready")):
                event = conn.execute("SELECT * FROM events WHERE event_id=?", (prefix + job_id,)).fetchone()
                if event is not None:
                    payload = json.loads(event["payload_json"])
                    if payload.get("input_digest") != job["input_digest"]:
                        raise WorkUnitStoreError("phase_job_event_binding_mismatch")
                    job.update(payload, status=status)
                    if status == "result_ready":
                        job["result_event_id"] = event["event_id"]
            if job["result_event_id"] is not None:
                settled = conn.execute("SELECT payload_json FROM events WHERE event_id=?",
                    ("task-area-event-settled:" + job["result_event_id"],)).fetchone()
                if settled is not None:
                    outcome = json.loads(settled[0])["outcome"]
                    job["status"] = "settled" if outcome.get("status") == "accepted" else "outcome_unknown"
            jobs.append(job)
        return jobs

    def list_phase_jobs(self, goal_id: str | None = None) -> list[dict]:
        with self._connect() as conn:
            conn.execute("BEGIN")
            return self._phase_jobs_conn(conn, goal_id)

    def _phase_job_conn(self, conn, job_id: str) -> dict:
        job = next((job for job in self._phase_jobs_conn(conn) if job["job_id"] == job_id), None)
        if job is None:
            raise WorkUnitStoreError("phase_job_missing")
        return job

    def _phase_job_authority_conn(self, conn, job: dict) -> None:
        item = job["input"]
        manifest, task = self._phase_context_conn(conn, item)
        envelope = task["envelope"]
        request = item.get("request")
        dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?",
                                (envelope["dispatch_key"],)).fetchone()
        unit = conn.execute("SELECT * FROM work_units WHERE work_unit_id=?",
                            (item["work_unit_id"],)).fetchone()
        run = conn.execute("SELECT * FROM runs WHERE run_id=?", (item["run_id"],)).fetchone()
        attempt = conn.execute("SELECT * FROM attempts WHERE run_id=? AND ordinal=?",
                               (item["run_id"], item["attempt"])).fetchone()
        parent = self._parent_or_raise(conn, run["parent_goal_id"]) if run is not None else None
        if (dispatch is None or unit is None or run is None or attempt is None
            or parent is None or parent["state"] != "active"
            or run["work_unit_id"] != item["work_unit_id"]
            or run["fence"] != item["fence"] or run["attempts"] != item["attempt"]
            or attempt["fence"] != item["fence"]
            or unit["run_id"] != run["run_id"] or unit["node_id"] != task["node_id"]
            or unit["parent_goal_id"] != run["parent_goal_id"]
            or dispatch["parent_goal_id"] != run["parent_goal_id"]
            or run["base_sha"] != envelope["wave_base_sha"]
            or unit["base_sha"] != envelope["wave_base_sha"]
            or parent["goal_id"] != manifest["goal_id"]
            or parent["goal_revision"] != manifest["goal_revision"]
            or any(item.get(key) != manifest[key] for key in ("goal_id", "goal_revision"))
            or item.get("node_id") != task["node_id"]
            or item.get("execution_binding_digest") != digest_json(manifest["execution_binding"])
            or any(dispatch[key] != item.get(key) for key in
                   ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt"))
            or dispatch["envelope_digest"] != envelope["envelope_digest"]
            or json.loads(dispatch["envelope_json"]) != envelope
            or any(item.get(key) != envelope.get(key) for key in ("packet_digest", "envelope_digest"))):
            raise WorkUnitStoreError("phase_job_attempt_changed")
        # A valid task context cannot lend its approval to a different node's
        # dispatch, even when that Run has the same Attempt ordinal and fence.
        receipt = json.loads(dispatch["receipt_json"])
        identity = {key: item[key] for key in
                    ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence")}
        if (any(receipt.get(key) != value for key, value in identity.items())
            or receipt.get("dispatch_key") != envelope["dispatch_key"]
            or receipt.get("envelope_digest") != envelope["envelope_digest"]
            or receipt.get("packet_digest") != envelope["packet_digest"]
            or receipt.get("receipt_digest") != dispatch["receipt_digest"]
            or digest_json({key: value for key, value in receipt.items() if key != "receipt_digest"})
               != dispatch["receipt_digest"]
            or not isinstance(request, dict)
            or any(request.get(key) != value for key, value in identity.items())
            or request.get("dispatch_key") != envelope["dispatch_key"]
            or request.get("packet_digest") != envelope["packet_digest"]
            or request.get("base_sha") != run["base_sha"]):
            raise WorkUnitStoreError("phase_job_request_identity_changed")
        if item["phase"] == "coding":
            if (attempt["state"] != "ready"
                or request.get("envelope_digest") != envelope["envelope_digest"]
                or request.get("packet_path") != envelope["packet_path"]
                or request.get("worktree") != attempt["workspace_ref"]
                or request.get("branch") != unit["branch"]):
                raise WorkUnitStoreError("phase_job_request_identity_changed")
        elif request.get("execution_phase") != item["phase"]:
            raise WorkUnitStoreError("phase_job_request_identity_changed")
        if not isinstance(item.get("deadline_at"), (float, int)) or not math.isfinite(item["deadline_at"]):
            raise WorkUnitStoreError("phase_job_deadline_expired")
        # A job being reserved has no effective deadline yet, so its proposal's applies; a waiting job
        # has none (its slice starts with its slots); every other job has the one it acquired.
        deadline = job["deadline_at"] if "deadline_at" in job else item["deadline_at"]
        if deadline is not None and self._now() >= deadline:
            raise WorkUnitStoreError("phase_job_deadline_expired")
        if "slot_requirements" in job:
            required = self._phase_slot_requirements(manifest, task, item["phase"])
            if required != job["slot_requirements"]:
                raise WorkUnitStoreError("phase_job_resource_binding_changed")
            if job.get("status") in {"reserved", "running"} and self._phase_waiting_slot_conn(conn, required, job["job_id"]):
                raise WorkUnitStoreError("phase_job_slot_binding_changed")
        for other in self._phase_jobs_conn(conn):
            if (other["job_id"] != job["job_id"] and other["input"]["work_unit_id"] == item["work_unit_id"]
                and other["status"] != "settled"):
                raise WorkUnitStoreError("phase_job_prior_effect_unsettled")
        if item["phase"] != "coding":
            phase = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?",
                                 (item["request"]["idempotency_key"],)).fetchone()
            if (phase is None or phase["state"] != "claimed" or phase["binding"] != item["phase_binding"]
                or phase["run_id"] != item["run_id"] or phase["attempt"] != item["attempt"]
                or phase["fence"] != item["fence"] or phase["phase"] != item["phase"]):
                raise WorkUnitStoreError("phase_job_completion_claim_changed")

    def reserve_phase_job(self, item: dict, *, allow_create: bool = True) -> dict:
        item = json.loads(json.dumps(item, sort_keys=True))
        if item.get("phase") not in {"coding", "checks", "verifier", "integration", "integration_checks", "integration_verifier"}:
            raise WorkUnitStoreError("phase_job_operation_unsupported")
        job_id = "phase-job:" + digest_json([item[key] for key in ("run_id", "attempt", "fence", "phase")])
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = next((job for job in self._phase_jobs_conn(conn) if job["job_id"] == job_id), None)
            if prior is not None:
                if ({k: v for k, v in prior["input"].items() if k != "deadline_at"}
                    != {k: v for k, v in item.items() if k != "deadline_at"}):
                    raise WorkUnitStoreError("phase_job_immutable_input_changed")
                conn.execute("COMMIT")
                return prior
            if not allow_create:
                raise WorkUnitStoreError("phase_job_reservation_missing_for_claim")
            job = {"job_id": job_id, "input": item, "input_digest": digest_json(item),
                   "creator_identity": NativeProcessIdentityPort().current().as_dict()}
            self._phase_job_authority_conn(conn, job)
            # The slice is the execution budget: it starts when the slots are acquired, so time spent
            # waiting for a slot is not spent from it (see `_phase_jobs_conn` and `acquire_phase_job_slots`).
            job["runtime_slice_seconds"] = item["deadline_at"] - self._now()
            manifest, task = self._phase_context_conn(conn, item)
            job["slot_requirements"] = self._phase_slot_requirements(manifest, task, item["phase"])
            self._append_event_conn(conn, event_id="phase-job-reserved:" + job_id,
                parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                event_type="task_area_phase_job_reserved", payload=job, created_at=self._now())
            if self._phase_waiting_slot_conn(conn, job["slot_requirements"], job_id) is None:
                self._append_event_conn(conn, event_id="phase-job-slots-acquired:" + job_id,
                    parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                    event_type="task_area_phase_slots_acquired", payload={"job_id": job_id,
                        "input_digest": job["input_digest"], "creator_identity": job["creator_identity"],
                        "deadline_at": item["deadline_at"]}, created_at=self._now())
            conn.execute("COMMIT")
        return next(value for value in self.list_phase_jobs(item["goal_id"]) if value["job_id"] == job_id)

    def claim_phase_job(self, job_id: str, identity: dict) -> dict:
        current = NativeProcessIdentityPort().current().as_dict()
        if current != identity or observe_process_identity(identity).status != "alive":
            raise WorkUnitStoreError("phase_job_carrier_identity_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self._phase_job_conn(conn, job_id)
            if job["status"] != "reserved":
                raise WorkUnitStoreError("phase_job_already_claimed")
            if (os.getppid() != job["creator_identity"]["pid"]
                or observe_process_identity(job["creator_identity"]).status != "alive"):
                raise WorkUnitStoreError("phase_job_startup_owner_unknown")
            self._phase_job_authority_conn(conn, job)
            item = job["input"]
            self._append_event_conn(conn, event_id="phase-job-claimed:" + job_id,
                parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                event_type="task_area_phase_job_claimed", payload={"job_id": job_id,
                    "input_digest": job["input_digest"], "carrier_identity": identity}, created_at=self._now())
            conn.execute("COMMIT")
        return self.read_phase_job(job_id, reconcile=False)

    def validate_phase_job_launch(self, job_id: str, identity: dict) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self._phase_job_conn(conn, job_id)
            if job["status"] != "running" or job["carrier_identity"] != identity:
                raise WorkUnitStoreError("phase_job_launch_claim_changed")
            current = NativeProcessIdentityPort().current()
            if current is None or current.as_dict() != job["carrier_identity"]:
                raise WorkUnitStoreError("phase_job_carrier_identity_invalid")
            self._phase_job_authority_conn(conn, job)
            conn.execute("COMMIT")
        return job

    def read_phase_job(self, job_id: str, *, reconcile: bool = True) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE" if reconcile else "BEGIN")
            job = self._phase_job_conn(conn, job_id)
            # Immutable result is authoritative even after its process exits.
            if reconcile and job["status"] in {"reserved", "running"}:
                identity = job["carrier_identity"] or job["creator_identity"]
                observed = observe_process_identity(identity)
                if observed.status != "alive" or self._now() >= job["deadline_at"]:
                    item = job["input"]
                    self._append_event_conn(conn, event_id="phase-job-unknown:" + job_id,
                        parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                        event_type="task_area_phase_job_unknown", payload={"job_id": job_id,
                            "input_digest": job["input_digest"], "observation": observed.status,
                            "reason": "phase_job_outcome_unknown"}, created_at=self._now())
                    job = self._phase_job_conn(conn, job_id)
            conn.execute("COMMIT")
        return job

    def _phase_result_bytes(self, job: dict) -> dict:
        from .platform_ports import read_private_file
        ref = job["result_ref"]
        path = Path(ref["path"])
        if path.resolve().parent != (self.root / "phase-jobs").resolve():
            raise WorkUnitStoreError("phase_job_result_path_invalid")
        raw = read_private_file(path)
        result = json.loads(raw)
        if ("sha256:" + hashlib.sha256(raw).hexdigest() != ref["content_digest"]
            or ref["content_digest"] != job["result_digest"]
            or result.get("job_id") != job["job_id"] or result.get("input_digest") != job["input_digest"]
            or not isinstance(result.get("result"), dict)):
            raise WorkUnitStoreError("phase_job_result_binding_mismatch")
        return result

    def record_phase_job_result(self, job_id: str, *, result_ref: dict, carrier_identity: dict) -> dict:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            job = self._phase_job_conn(conn, job_id)
            if job["carrier_identity"] != carrier_identity or job["status"] not in {"running", "result_ready", "settled"}:
                raise WorkUnitStoreError("phase_job_result_owner_changed")
            if job["result_event_id"] is None:
                current = NativeProcessIdentityPort().current()
                if current is None or current.as_dict() != job["carrier_identity"]:
                    raise WorkUnitStoreError("phase_job_carrier_identity_invalid")
            supplied = {**job, "result_ref": result_ref, "result_digest": result_ref["content_digest"]}
            self._phase_result_bytes(supplied)
            item = job["input"]
            self._append_event_conn(conn, event_id="phase-job-result:" + job_id,
                parent_goal_id=item["goal_id"], work_unit_id=item["work_unit_id"], run_id=item["run_id"],
                event_type="task_area_phase_job_result", payload={"job_id": job_id,
                    "input_digest": job["input_digest"], "result_ref": result_ref,
                    "result_digest": result_ref["content_digest"]}, created_at=self._now())
            conn.execute("COMMIT")
        return self.read_phase_job(job_id, reconcile=False)

    def phase_job_result(self, job_id: str) -> dict:
        job = self.read_phase_job(job_id, reconcile=False)
        if job["status"] not in {"result_ready", "settled"}:
            raise WorkUnitStoreError("phase_job_result_unavailable")
        return self._phase_result_bytes(job)

    def _phase_settlement_conn(self, conn, source) -> dict:
        source_payload = json.loads(source["payload_json"])
        job = self._phase_job_conn(conn, source_payload["job_id"])
        item = job["input"]
        if job["result_event_id"] != source["event_id"]:
            raise WorkUnitStoreError("phase_job_result_event_mismatch")
        raw = self._phase_result_bytes(job)["result"]
        if item["phase"] == "coding":
            row = conn.execute("SELECT receipt_json FROM dispatch_consumptions WHERE dispatch_key=?",
                               (item["request"]["dispatch_key"],)).fetchone()
            receipt = json.loads(row[0]) if row is not None else {}
            canonical = {key: value for key, value in raw.items() if key not in {"reused", "invoked"}}
            if receipt.get("executor_receipt") != canonical:
                raise WorkUnitStoreError("phase_job_executor_acceptance_missing")
        else:
            row = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?",
                               (item["request"]["idempotency_key"],)).fetchone()
            evidence = json.loads(row["evidence_json"]) if row is not None else {}
            if (row is None or row["state"] != "settled" or row["binding"] != item["phase_binding"]
                or evidence.get("phase_job_result_digest") != job["result_digest"]):
                raise WorkUnitStoreError("phase_job_completion_settlement_missing")
        return {"status": "accepted", "kind": "phase_job_result", "job_id": job["job_id"],
                "result_digest": job["result_digest"]}

    def settle_phase_job(self, job_id: str) -> dict:
        job = self.read_phase_job(job_id, reconcile=False)
        claim = self.claim_task_area_event(job["result_event_id"])
        with self._connect() as conn:
            source = conn.execute("SELECT * FROM events WHERE event_id=?", (job["result_event_id"],)).fetchone()
            outcome = self._phase_settlement_conn(conn, source)
        return self.settle_task_area_event(job["result_event_id"], claim_id=claim["claim_id"], outcome=outcome)

    def claim_task_area_event(self, source_event_id: str) -> dict[str, Any]:
        """Durably claim one approval/prerequisite event for the task-area consumer.

        The append-only event table is the claim ledger: the deterministic event
        id plus BEGIN IMMEDIATE makes competing bounded runners converge on one
        claim while allowing a restarted runner to resume an unsettled claim.
        """
        source_event_id = _required_text("source_event_id", source_event_id)
        allowed_types = {
            "task_list_approval_updated", "task_area_prerequisite_updated",
            "task_list_revision_pending", "task_area_recovery_disposition",
            "task_area_phase_job_result", "task_area_capacity_policy_updated",
        }
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (source_event_id,)
            ).fetchone()
            if source is None:
                raise KeyError(f"unknown task-area source event: {source_event_id}")
            if source["event_type"] not in allowed_types:
                raise WorkUnitStoreError("task_area_event_type_unsupported")
            source_payload = json.loads(source["payload_json"])
            source_digest = digest_json({
                "event_id": source["event_id"],
                "parent_goal_id": source["parent_goal_id"],
                "event_type": source["event_type"],
                "payload": source_payload,
                "created_at": float(source["created_at"]),
            })
            claim_rows = conn.execute(
                "SELECT * FROM events WHERE parent_goal_id = ? AND event_type = ?",
                (source["parent_goal_id"], "task_area_event_claimed"),
            ).fetchall()
            claims = [row for row in claim_rows
                      if json.loads(row["payload_json"]).get("source_event_id") == source_event_id]
            if len(claims) > 1:
                raise WorkUnitStoreError("task_area_event_claim_not_unique")
            settled_rows = conn.execute(
                "SELECT * FROM events WHERE parent_goal_id = ? AND event_type = ?",
                (source["parent_goal_id"], "task_area_event_settled"),
            ).fetchall()
            settlements = [row for row in settled_rows
                           if json.loads(row["payload_json"]).get("source_event_id") == source_event_id]
            if len(settlements) > 1:
                raise WorkUnitStoreError("task_area_event_settlement_not_unique")
            if claims:
                claim_payload = json.loads(claims[0]["payload_json"])
                if claim_payload.get("source_event_digest") != source_digest:
                    raise WorkUnitStoreError("task_area_event_source_changed_after_claim")
                status = "settled" if settlements else "claimed"
                claim = claim_payload
            else:
                if settlements:
                    raise WorkUnitStoreError("task_area_event_settlement_without_claim")
                claim_id = "task-area-event-claim:" + digest_json({
                    "source_event_id": source_event_id,
                    "source_event_digest": source_digest,
                })
                claim = {
                    "source_event_id": source_event_id,
                    "source_event_digest": source_digest,
                    "claim_id": claim_id,
                }
                self._append_event_conn(
                    conn,
                    event_id=f"task-area-event-claimed:{source_event_id}",
                    parent_goal_id=source["parent_goal_id"],
                    work_unit_id=None,
                    run_id=None,
                    event_type="task_area_event_claimed",
                    payload=claim,
                    created_at=self._now(),
                )
                status = "claimed"
            conn.execute("COMMIT")
        return {"status": status, **claim}

    def settle_task_area_event(self, source_event_id: str, *, claim_id: str,
                               outcome: dict[str, Any]) -> dict[str, Any]:
        """Append the sole durable settlement for an existing task-area claim."""
        source_event_id = _required_text("source_event_id", source_event_id)
        claim_id = _required_text("claim_id", claim_id)
        if not isinstance(outcome, dict):
            raise WorkUnitStoreError("task_area_event_outcome_invalid")
        encoded_outcome = json.loads(json.dumps(outcome, ensure_ascii=False,
                                               sort_keys=True, separators=(",", ":")))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (source_event_id,)
            ).fetchone()
            if source is None:
                raise KeyError(f"unknown task-area source event: {source_event_id}")
            rows = conn.execute(
                "SELECT * FROM events WHERE parent_goal_id = ? AND event_type = ?",
                (source["parent_goal_id"], "task_area_event_claimed"),
            ).fetchall()
            claims = [json.loads(row["payload_json"]) for row in rows
                      if json.loads(row["payload_json"]).get("source_event_id") == source_event_id]
            if len(claims) != 1 or claims[0].get("claim_id") != claim_id:
                raise WorkUnitStoreError("task_area_event_claim_missing_or_mismatched")
            if source["event_type"] == "task_area_phase_job_result":
                if encoded_outcome != self._phase_settlement_conn(conn, source):
                    raise WorkUnitStoreError("phase_job_settlement_mismatch")
            if source["event_type"] == "task_area_capacity_policy_updated":
                if encoded_outcome.get("status") == "rejected":
                    # A caller cannot invent a rejection. Re-run the real
                    # validation (including CAS) inside this settlement tx.
                    try:
                        self._capacity_settlement_conn(conn, source)
                    except (OSError, ValueError, KeyError) as exc:
                        if encoded_outcome != {"status": "rejected", "reason": str(exc)}:
                            raise WorkUnitStoreError("capacity_policy_settlement_mismatch") from exc
                    else:
                        raise WorkUnitStoreError("capacity_policy_settlement_mismatch")
                elif encoded_outcome != self._capacity_settlement_conn(conn, source):
                    raise WorkUnitStoreError("capacity_policy_settlement_mismatch")
            if source["event_type"] == "task_area_recovery_disposition" and encoded_outcome.get("status") == "accepted":
                context = self._recovery_disposition_conn(conn, source_event_id, require_accepted=False)
                source_value = self._disposition_event_value(source)
                source_digest = digest_json({key: source_value[key] for key in
                    ("event_id", "parent_goal_id", "event_type", "payload", "created_at")})
                if (encoded_outcome != context["outcome"]
                    or claims[0].get("source_event_digest") != source_digest
                    or next(row for row in rows if json.loads(row["payload_json"]).get("source_event_id")
                            == source_event_id)["event_id"] != "task-area-event-claimed:" + source_event_id
                    or claim_id != "task-area-event-claim:" + digest_json({
                        "source_event_id": source_event_id, "source_event_digest": source_digest})):
                    raise WorkUnitStoreError("recovery_disposition_settlement_mismatch")
            settlement = {
                "source_event_id": source_event_id,
                "claim_id": claim_id,
                "outcome": encoded_outcome,
                "outcome_digest": digest_json(encoded_outcome),
            }
            rows = conn.execute(
                "SELECT * FROM events WHERE parent_goal_id = ? AND event_type = ?",
                (source["parent_goal_id"], "task_area_event_settled"),
            ).fetchall()
            prior = [json.loads(row["payload_json"]) for row in rows
                     if json.loads(row["payload_json"]).get("source_event_id") == source_event_id]
            if len(prior) > 1:
                raise WorkUnitStoreError("task_area_event_settlement_not_unique")
            if prior:
                if prior[0] != settlement:
                    raise WorkUnitStoreError("task_area_event_settlement_already_bound")
            else:
                self._append_event_conn(
                    conn,
                    event_id=f"task-area-event-settled:{source_event_id}",
                    parent_goal_id=source["parent_goal_id"],
                    work_unit_id=None,
                    run_id=None,
                    event_type="task_area_event_settled",
                    payload=settlement,
                    created_at=self._now(),
                )
            conn.execute("COMMIT")
        return {"status": "settled", **settlement}

    def events(self, parent_goal_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM events"
        params: tuple[Any, ...] = ()
        if parent_goal_id is not None:
            query += " WHERE parent_goal_id = ?"
            params = (_required_text("parent_goal_id", parent_goal_id),)
        query += " ORDER BY created_at, event_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        result = []
        for row in rows:
            result.append({
                "event_id": row["event_id"], "parent_goal_id": row["parent_goal_id"], "work_unit_id": row["work_unit_id"],
                "run_id": row["run_id"], "event_type": row["event_type"], "payload": json.loads(row["payload_json"]), "created_at": float(row["created_at"]),
            })
        return result

    def discovery_position(self, parent_goal_id: str) -> dict[str, Any]:
        """Latest durable discovery batch of one goal; the default is an empty cursor."""
        from .discovery_cursor import BATCH_EVENT
        goal = _required_text("parent_goal_id", parent_goal_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM events WHERE parent_goal_id = ? AND event_type = ? "
                "ORDER BY rowid DESC LIMIT 1", (goal, BATCH_EVENT)).fetchone()
        if row is None:
            return {"seq": 0, "after_rowid": 0, "pending": [], "coverage": None}
        try:
            payload = json.loads(row["payload_json"])
            position = {"seq": payload["seq"], "after_rowid": payload["to_rowid"],
                        "pending": list(payload["pending"]), "coverage": payload["coverage"]}
        except (ValueError, TypeError, KeyError) as exc:
            raise WorkUnitStoreError("discovery_position_unreadable") from exc
        if (type(position["seq"]) is not int or type(position["after_rowid"]) is not int
            or position["seq"] < 1 or position["after_rowid"] < 0):
            raise WorkUnitStoreError("discovery_position_unreadable")
        return position

    def discovery_read_signals(self, parent_goal_id: str, *, after_rowid: int,
                               event_types: tuple[str, ...], limit: int) -> list[dict[str, Any]]:
        """At most ``limit`` declared-signal events after a cursor, in rowid order."""
        from .discovery_cursor import MAX_BATCH_EVENTS, OWN_EVENT_PREFIX
        goal = _required_text("parent_goal_id", parent_goal_id)
        if (type(after_rowid) is not int or after_rowid < 0 or type(limit) is not int
            or not 1 <= limit <= MAX_BATCH_EVENTS or not event_types
            or any(kind.startswith(OWN_EVENT_PREFIX) for kind in event_types)):
            raise WorkUnitStoreError("discovery_read_invalid")
        marks = ",".join("?" for _ in event_types)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT rowid AS position, event_id, event_type, payload_json FROM events "
                f"WHERE parent_goal_id = ? AND rowid > ? AND event_type IN ({marks}) "
                "ORDER BY rowid LIMIT ?", (goal, after_rowid, *event_types, limit)).fetchall()
        return [{"rowid": row["position"], "event_id": row["event_id"],
                 "event_type": row["event_type"], "payload_json": row["payload_json"]}
                for row in rows]

    def discovery_known_candidates(self, parent_goal_id: str,
                                   candidate_ids: list[str]) -> set[str]:
        """Which candidate ids already have a durable record in this goal."""
        goal = _required_text("parent_goal_id", parent_goal_id)
        names = {f"discovery-candidate:{goal}:{cid}": cid for cid in candidate_ids}
        known: set[str] = set()
        ordered = list(names)
        with self._connect() as conn:
            for start in range(0, len(ordered), 200):
                chunk = ordered[start:start + 200]
                marks = ",".join("?" for _ in chunk)
                for row in conn.execute(
                        f"SELECT event_id FROM events WHERE event_id IN ({marks})", chunk):
                    known.add(names[row["event_id"]])
        return known

    def record_discovery_batch(self, parent_goal_id: str, *, batch: dict[str, Any],
                               candidates: list[dict[str, Any]]) -> dict[str, Any]:
        """Append one bounded batch, its new candidates and the cursor in one transaction."""
        from .discovery_cursor import (
            BATCH_EVENT, BATCH_SCHEMA, CANDIDATE_EVENT, CANDIDATE_SCHEMA,
            MAX_BATCH_CANDIDATES, MAX_BATCH_EVENTS)
        goal = _required_text("parent_goal_id", parent_goal_id)
        if self.read_only:
            raise WorkUnitStoreError("discovery_batch_read_only")
        try:
            seq, start, end = batch["seq"], batch["from_rowid"], batch["to_rowid"]
            valid = (batch["schema"] == BATCH_SCHEMA
                     and all(type(value) is int for value in (seq, start, end, batch["events_read"]))
                     and seq >= 1 and 0 <= start <= end
                     and 0 <= batch["events_read"] <= MAX_BATCH_EVENTS
                     and len(batch["handoff"]) <= MAX_BATCH_CANDIDATES
                     and set(batch["handoff"]) | set(batch["pending"])
                        >= set(batch["new_candidates"])
                     and not set(batch["handoff"]) & set(batch["pending"])
                     and [row["candidate_id"] for row in candidates] == batch["new_candidates"]
                     and all(row["schema"] == CANDIDATE_SCHEMA for row in candidates))
        except (KeyError, TypeError):
            valid = False
        if not valid:
            raise WorkUnitStoreError("discovery_batch_invalid")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute("SELECT state FROM parent_goals WHERE parent_goal_id = ?",
                                  (goal,)).fetchone()
            if parent is None or parent["state"] != "active":
                raise WorkUnitStoreError("discovery_parent_not_active")
            last = conn.execute(
                "SELECT payload_json FROM events WHERE parent_goal_id = ? AND event_type = ? "
                "ORDER BY rowid DESC LIMIT 1", (goal, BATCH_EVENT)).fetchone()
            last_seq = json.loads(last["payload_json"])["seq"] if last is not None else 0
            if seq != last_seq + 1:
                raise WorkUnitStoreError("discovery_batch_position_conflict")
            for record in candidates:
                self._append_event_conn(
                    conn, event_id=f"discovery-candidate:{goal}:{record['candidate_id']}",
                    parent_goal_id=goal, work_unit_id=None, run_id=None,
                    event_type=CANDIDATE_EVENT, payload=record, created_at=self._now())
            self._append_event_conn(
                conn, event_id=f"discovery-batch:{goal}:{seq}", parent_goal_id=goal,
                work_unit_id=None, run_id=None, event_type=BATCH_EVENT, payload=batch,
                created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": f"discovery-batch:{goal}:{seq}", "appended": True}

    def record_discovery_planner_event(self, parent_goal_id: str, *, event_id: str,
                                       event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Append one goal-level Planner request, proposal or decision; an identical replay is a no-op."""
        from .discovery_planner import RECORD_EVENTS
        goal = _required_text("parent_goal_id", parent_goal_id)
        event_id = _required_text("event_id", event_id)
        if self.read_only:
            raise WorkUnitStoreError("discovery_planner_read_only")
        if event_type not in RECORD_EVENTS or not isinstance(payload, dict):
            raise WorkUnitStoreError("discovery_planner_event_invalid")
        wanted = json.loads(json.dumps(payload))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            parent = conn.execute("SELECT state FROM parent_goals WHERE parent_goal_id = ?",
                                  (goal,)).fetchone()
            if parent is None or parent["state"] != "active":
                raise WorkUnitStoreError("discovery_parent_not_active")
            row = conn.execute("SELECT event_type, parent_goal_id, payload_json FROM events "
                               "WHERE event_id = ?", (event_id,)).fetchone()
            if row is not None:
                if (row["event_type"] != event_type or row["parent_goal_id"] != goal
                    or json.loads(row["payload_json"]) != wanted):
                    raise WorkUnitStoreError("discovery_planner_event_conflict")
                conn.execute("COMMIT")
                return {"event_id": event_id, "appended": False}
            self._append_event_conn(
                conn, event_id=event_id, parent_goal_id=goal, work_unit_id=None, run_id=None,
                event_type=event_type, payload=payload, created_at=self._now())
            conn.execute("COMMIT")
        return {"event_id": event_id, "appended": True}

    def discovery_get_event(self, parent_goal_id: str, event_id: str) -> dict[str, Any] | None:
        goal = _required_text("parent_goal_id", parent_goal_id)
        with self._connect() as conn:
            row = conn.execute("SELECT payload_json FROM events WHERE event_id = ? AND parent_goal_id = ?",
                               (_required_text("event_id", event_id), goal)).fetchone()
        return json.loads(row["payload_json"]) if row is not None else None

    def _discovery_last_payload(self, goal: str, event_type: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT payload_json FROM events WHERE parent_goal_id = ? AND event_type = ? "
                               "ORDER BY rowid DESC LIMIT 1", (goal, event_type)).fetchone()
        return json.loads(row["payload_json"]) if row is not None else None

    def discovery_request_position(self, parent_goal_id: str) -> int:
        """Candidate rowid the last request covered: requests are made strictly in first-seen order."""
        from .discovery_planner import REQUEST_EVENT
        last = self._discovery_last_payload(_required_text("parent_goal_id", parent_goal_id), REQUEST_EVENT)
        position = 0 if last is None else last.get("candidate_rowid")
        if type(position) is not int or position < 0:
            raise WorkUnitStoreError("discovery_request_position_unreadable")
        return position

    def discovery_decision_position(self, parent_goal_id: str) -> int:
        """Request rowid the last decision covered: decisions are made strictly in request order."""
        from .discovery_planner import DECISION_EVENT
        last = self._discovery_last_payload(_required_text("parent_goal_id", parent_goal_id), DECISION_EVENT)
        position = 0 if last is None else last.get("request_rowid")
        if type(position) is not int or position < 0:
            raise WorkUnitStoreError("discovery_decision_position_unreadable")
        return position

    def discovery_next_candidates(self, parent_goal_id: str, *, after_rowid: int,
                                  limit: int) -> list[dict[str, Any]]:
        """At most ``limit`` recorded candidates after a position, in first-seen order."""
        from .discovery_cursor import CANDIDATE_EVENT, MAX_BATCH_CANDIDATES
        goal = _required_text("parent_goal_id", parent_goal_id)
        if (type(after_rowid) is not int or after_rowid < 0 or type(limit) is not int
            or not 1 <= limit <= MAX_BATCH_CANDIDATES):
            raise WorkUnitStoreError("discovery_read_invalid")
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT rowid AS position, payload_json FROM events WHERE parent_goal_id = ? "
                "AND event_type = ? AND rowid > ? ORDER BY rowid LIMIT ?",
                (goal, CANDIDATE_EVENT, after_rowid, limit)).fetchall()
        return [{"rowid": row["position"], "payload": json.loads(row["payload_json"])} for row in rows]

    def discovery_open_requests(self, parent_goal_id: str, *, limit: int) -> list[dict[str, Any]]:
        """At most ``limit`` requests still without a decision, oldest first, with any proposal."""
        from .discovery_cursor import MAX_BATCH_CANDIDATES
        from .discovery_planner import REQUEST_EVENT, proposal_id
        goal = _required_text("parent_goal_id", parent_goal_id)
        if type(limit) is not int or not 1 <= limit <= MAX_BATCH_CANDIDATES:
            raise WorkUnitStoreError("discovery_read_invalid")
        after = self.discovery_decision_position(goal)
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT rowid AS position, event_id, payload_json FROM events WHERE parent_goal_id = ? "
                "AND event_type = ? AND rowid > ? ORDER BY rowid LIMIT ?",
                (goal, REQUEST_EVENT, after, limit)).fetchall()
        items = []
        prefix = f"discovery-request:{goal}:"
        for row in rows:
            # The candidate id comes from the event's own id: the body is not trusted yet.
            cid = row["event_id"][len(prefix):-len(":1")]
            items.append({"rowid": row["position"], "event_id": row["event_id"],
                          "request": json.loads(row["payload_json"]),
                          "proposal": self.discovery_get_event(goal, proposal_id(goal, cid))})
        return items

    def discovery_open_count(self, parent_goal_id: str) -> int:
        from .discovery_planner import REQUEST_EVENT
        goal = _required_text("parent_goal_id", parent_goal_id)
        after = self.discovery_decision_position(goal)
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM events WHERE parent_goal_id = ? "
                               "AND event_type = ? AND rowid > ?", (goal, REQUEST_EVENT, after)).fetchone()
        return int(row["n"])

    def active_work_units(self, parent_goal_id: str) -> list[dict[str, Any]]:
        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        return [row for row in self.list_work_units(parent_goal_id) if row["state"] in {"running", "verified"}]


def compatible_work_units(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return not read_write_conflicts(left, right)


def compatible_groups(units: Iterable[dict[str, Any]]) -> bool:
    values = list(units)
    return all(compatible_work_units(left, right) for left, right in combinations(values, 2))


__all__ = [
    "ATTEMPT_STATES", "BaseMismatchError", "DAGCycleError", "DependencyError", "DISPATCH_CONSUMPTION_SCHEMA", "DISPATCH_RECEIPT_SCHEMA", "DuplicateWorkUnitError",
    "FenceError", "LeaseBusyError", "RUN_STATES", "SCHEMA", "WORK_UNIT_STATES", "WorkUnitStore",
    "WorkUnitStoreError", "compatible_groups", "compatible_work_units", "digest_json", "normalize_dependencies",
    "normalize_path", "normalize_paths", "paths_overlap", "read_write_conflicts",
]

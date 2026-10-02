"""Deterministic DAG scheduling for a parent Goal's isolated WorkUnits.

The scheduler only admits a wave when the parent is active, dependencies are
integrated, every selected child uses the parent base, and declared write
paths do not conflict with another selected or active child.  A wave contains
at most three children and never invents a one-child parallel wave.

It records one Run per WorkUnit through :class:`WorkUnitStore`.  Replaying a
dispatch after a process restart returns the existing running children or
starts another Attempt on the same Run after an expired lease; it cannot make
a second Run for a child.
"""

from __future__ import annotations

import hashlib
import json
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from . import delivery_contract as lh_delivery_contract
except ImportError:  # direct LH canary execution
    import delivery_contract as lh_delivery_contract  # type: ignore

try:
    from .runner_adapter import PhaseJobPending
    from .work_unit_store import (
        DAGCycleError,
        DependencyError,
        LeaseBusyError,
        WorkUnitStore,
        compatible_work_units,
        digest_json,
        read_write_conflicts,
    )
    from .successor_executor import (
        SuccessorExecutorError,
        SuccessorExecutorPort,
        validate_executor_receipt,
    )
except ImportError:  # direct canary execution keeps lh_runtime on sys.path
    from runner_adapter import PhaseJobPending  # type: ignore
    from work_unit_store import (  # type: ignore
        DAGCycleError,
        DependencyError,
        LeaseBusyError,
        WorkUnitStore,
        compatible_work_units,
        digest_json,
        read_write_conflicts,
    )
    from successor_executor import (  # type: ignore
        SuccessorExecutorError,
        SuccessorExecutorPort,
        validate_executor_receipt,
    )

delivery_unit_contract = lh_delivery_contract

SCHEMA = "lh-parallel-scheduler/v1"
MIN_WORKERS = 2
MAX_WORKERS = 3
ACTIVE_STATES = {"running", "verified"}
WAITING_STATES = {"pending", "retry_pending"}


class SchedulerError(ValueError):
    """Base error for invalid scheduler configuration or admission input."""


class SchedulerConfigurationError(SchedulerError):
    """The requested wave size is outside the two-to-three worker envelope."""


SUCCESSOR_DISPATCH_SCHEMA = "lh-successor-dispatch-envelope/v1"
SUCCESSOR_DISPATCH_RECEIPT_SCHEMA = "lh-successor-dispatch-receipt/v1"


class SuccessorDispatchConsumer:
    """Consume one digest-bound successor envelope through the LH WorkUnit store.

    The first boundary is queue admission.  An optional provider-neutral
    executor port is invoked only after that admission and must return durable
    task-receipt/heartbeat evidence before the Attempt can become running.
    Without a port the Attempt remains ready for a later host-owned consumer.
    The queue ledger is SQLite-backed and shares the existing Goal/Run/Attempt
    store, so a replay returns the original receipt instead of creating another
    child.
    """

    def __init__(
        self,
        store: WorkUnitStore,
        *,
        goal_id: str,
        goal_revision: int,
        node_id: str | None = None,
        holder: str = "lh-post-merge-resume-scheduler",
        lease_seconds: float = 60.0,
        executor: SuccessorExecutorPort | None = None,
        completion_controller: Any = None,
        delivery_contract: Mapping[str, Any] | None = None,
        work_definition: Mapping[str, Any] | None = None,
        candidate_recovery_admission: Mapping[str, Any] | None = None,
        candidate_recovery_authority_context: Mapping[str, Any] | None = None,
    ):
        if not isinstance(store, WorkUnitStore):
            raise SchedulerConfigurationError("store must be a WorkUnitStore")
        self.store = store
        self.goal_id = _required_text("goal_id", goal_id)
        if isinstance(goal_revision, bool) or not isinstance(goal_revision, int) or goal_revision < 1:
            raise SchedulerConfigurationError("goal_revision must be positive")
        self.goal_revision = goal_revision
        resolved_node_id = node_id
        if resolved_node_id is None:
            candidate_contract = delivery_contract
            if candidate_contract is None and completion_controller is not None:
                candidate_contract = getattr(completion_controller, "delivery_contract", None)
            if isinstance(candidate_contract, Mapping):
                candidate_node = candidate_contract.get("node")
                if isinstance(candidate_node, Mapping):
                    resolved_node_id = candidate_node.get("id")
        # A pure LH consumer may learn the node only from the sealed envelope;
        # it must never guess an external host/P7 node at construction time.
        self.node_id = None if resolved_node_id is None else _required_text("node_id", resolved_node_id)
        self.holder = _required_text("holder", holder)
        if float(lease_seconds) < 0:
            raise SchedulerConfigurationError("lease_seconds must not be negative")
        self.lease_seconds = float(lease_seconds)
        self.executor = executor
        self.completion_controller = completion_controller
        self.delivery_contract = dict(delivery_contract) if delivery_contract is not None else None
        self.work_definition = dict(work_definition) if work_definition is not None else {}
        self.phase_context = None
        self.candidate_recovery_admission = (dict(candidate_recovery_admission)
                                             if candidate_recovery_admission is not None else None)
        self.candidate_recovery_authority_context = (
            dict(candidate_recovery_authority_context)
            if candidate_recovery_authority_context is not None else None
        )
        if (self.candidate_recovery_authority_context is not None
            and self.completion_controller is not None
            and hasattr(self.completion_controller, "candidate_recovery_authority_context")):
            self.completion_controller.candidate_recovery_authority_context = dict(
                self.candidate_recovery_authority_context)

    @staticmethod
    def _sha256_digest(name: str, value: Any) -> str:
        if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
            raise SchedulerError(f"{name} must be a sha256 digest")
        try:
            int(value[7:], 16)
        except ValueError as exc:
            raise SchedulerError(f"{name} must be a sha256 digest") from exc
        return value.lower()

    def _validate(self, envelope: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(envelope, dict) or envelope.get("schema") != SUCCESSOR_DISPATCH_SCHEMA:
            raise SchedulerError("successor dispatch envelope schema invalid")
        supplied = self._sha256_digest("envelope_digest", envelope.get("envelope_digest"))
        body = dict(envelope)
        body.pop("envelope_digest", None)
        if supplied != digest_json(body):
            raise SchedulerError("successor dispatch envelope digest mismatch")
        if envelope.get("goal_id") != self.goal_id or envelope.get("goal_revision") != self.goal_revision:
            raise SchedulerError("successor dispatch Goal revision mismatch")
        if self.node_id is None:
            self.node_id = _required_text("node_id", envelope.get("node_id"))
        if envelope.get("node_id") != self.node_id or envelope.get("successor_node_id") != self.node_id:
            raise SchedulerError("successor dispatch node mismatch")
        if envelope.get("first_actionable") != self.node_id:
            raise SchedulerError("successor dispatch projection mismatch")
        if envelope.get("provider_invocations") != 0 or envelope.get("manual_prompts") != 0:
            raise SchedulerError("successor dispatch crossed provider boundary")
        _required_text("dispatch_key", envelope.get("dispatch_key"))
        self._sha256_digest("transition_digest", envelope.get("transition_digest"))
        packet_path = _required_text("packet_path", envelope.get("packet_path"))
        packet_digest = self._sha256_digest("packet_digest", envelope.get("packet_digest"))
        path = Path(packet_path).expanduser().resolve()
        if not path.is_file():
            raise SchedulerError("successor packet missing")
        try:
            packet = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SchedulerError("successor packet unreadable") from exc
        if not isinstance(packet, dict) or packet.get("packet_digest") != packet_digest:
            raise SchedulerError("successor packet digest mismatch")
        packet_without_digest = dict(packet)
        packet_without_digest.pop("packet_digest", None)
        if lh_delivery_contract.digest_json(packet_without_digest) != packet_digest:
            raise SchedulerError("successor packet content digest mismatch")
        packet_body = packet.get("packet", packet)
        if not isinstance(packet_body, dict):
            raise SchedulerError("successor packet body invalid")
        contract = self.delivery_contract
        if contract is None and self.completion_controller is not None:
            candidate = getattr(self.completion_controller, "delivery_contract", None)
            if isinstance(candidate, Mapping):
                contract = dict(candidate)
        if self.completion_controller is None:
            raise SchedulerError("delivery_unit_completion_factory_missing")
        if not isinstance(contract, Mapping):
            raise SchedulerError("delivery_unit_contract_missing")
        try:
            contract = lh_delivery_contract.validate_contract(contract)
            plan = lh_delivery_contract.plan_delivery_unit(contract)
            if packet_body.get("delivery_unit_managed") is not True:
                sidecar_path = envelope.get("delivery_unit_sidecar_path")
                if not isinstance(sidecar_path, str) or not sidecar_path.strip():
                    sidecar_path = str(path.with_name(path.name + ".delivery-unit.json"))
                sidecar_file = Path(sidecar_path).expanduser().resolve()
                if not sidecar_file.is_file():
                    raise SchedulerError("delivery_unit_binding_missing")
                try:
                    sidecar = json.loads(sidecar_file.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise SchedulerError("delivery_unit_sidecar_unreadable") from exc
                admission = lh_delivery_contract.verify_legacy_sidecar(packet, sidecar, contract, plan)
            else:
                admission = lh_delivery_contract.verify_dispatch_binding(
                    envelope=envelope,
                    packet=packet_body,
                    contract=contract,
                    plan=plan,
                )
        except lh_delivery_contract.DeliveryUnitError as exc:
            raise SchedulerError(f"delivery_unit_{exc.reason}") from exc
        if admission.get("verdict") != "GREEN":
            raise SchedulerError(f"delivery_unit_{admission.get('reason', 'dispatch_binding_failed')}")
        binding = {
            "schema": "lh-delivery-contract-binding/v1",
            "status": "bound",
            "unit_id": contract["unit_id"],
            "contract_digest": contract["contract_digest"],
            "plan_verdict_digest": plan["plan_verdict_digest"],
            "goal_id": contract["goal"]["id"],
            "goal_revision": contract["goal"]["revision"],
            "node_id": contract["node"]["id"],
            "node_kind": contract["node"]["kind"],
            "identity": {"goal_id": contract["goal"]["id"], "goal_revision": contract["goal"]["revision"], "node_id": contract["node"]["id"], "unit_id": contract["unit_id"]},
        }
        return {**envelope, "packet_path": str(path), "packet_digest": packet_digest, "envelope_digest": supplied, "_delivery_contract": contract, "_delivery_plan": plan, "_delivery_binding": {"binding": binding, "contract": contract, "plan_verdict": plan}}

    def _validate_candidate_recovery_reuse(
        self, envelope: Mapping[str, Any], dispatch_key: str,
    ) -> dict[str, Any]:
        """Check an existing unknown dispatch before any queue admission write."""
        admission = self.candidate_recovery_admission
        authority = self.candidate_recovery_authority_context
        if not isinstance(admission, Mapping):
            raise SchedulerError("candidate_recovery_admission_context_missing")
        if not isinstance(authority, Mapping):
            raise SchedulerError("candidate_recovery_authority_context_missing")
        dispatch = self.store.get_dispatch_consumption(dispatch_key)
        if not isinstance(dispatch, Mapping):
            raise SchedulerError("candidate_recovery_dispatch_missing")
        receipt = dispatch.get("receipt")
        if not isinstance(receipt, Mapping) or receipt.get("executor_status") != "unknown":
            raise SchedulerError("candidate_recovery_dispatch_unknown_missing")
        for field in ("dispatch_key", "envelope_digest", "goal_id", "goal_revision", "node_id"):
            expected = dispatch_key if field == "dispatch_key" else envelope.get(field)
            if dispatch.get(field) != expected:
                raise SchedulerError("candidate_recovery_dispatch_identity_mismatch")
        for field in ("dispatch_key", "work_unit_id", "run_id", "attempt"):
            if admission.get(field) != dispatch.get(field):
                raise SchedulerError("candidate_recovery_admission_binding_mismatch")
        supplied_dispatch_digest = admission.get("current_dispatch_receipt_digest")
        if supplied_dispatch_digest is None and isinstance(admission.get("current_dispatch_receipt"), Mapping):
            supplied_dispatch_digest = (
                admission["current_dispatch_receipt"].get("receipt_digest")
                or digest_json(dict(admission["current_dispatch_receipt"]))
            )
        if supplied_dispatch_digest != dispatch.get("receipt_digest"):
            raise SchedulerError("candidate_recovery_dispatch_evidence_mismatch")
        recovery = receipt.get("executor_recovery_receipt")
        if not isinstance(recovery, Mapping):
            raise SchedulerError("candidate_recovery_unknown_receipt_missing")
        supplied_unknown_digest = admission.get("current_unknown_recovery_receipt_digest")
        if supplied_unknown_digest is None and isinstance(admission.get("current_unknown_recovery_receipt"), Mapping):
            supplied_unknown_digest = (
                admission["current_unknown_recovery_receipt"].get("recovery_receipt_digest")
                or digest_json(dict(admission["current_unknown_recovery_receipt"]))
            )
        if supplied_unknown_digest != recovery.get("recovery_receipt_digest"):
            raise SchedulerError("candidate_recovery_unknown_evidence_mismatch")
        run_id = admission.get("run_id")
        attempt_no = admission.get("attempt")
        fence = admission.get("fence")
        run = self.store.get_run(str(run_id)) if isinstance(run_id, str) else None
        attempt = (self.store.get_attempt(str(run_id), int(attempt_no))
                   if isinstance(run_id, str) and isinstance(attempt_no, int)
                   and not isinstance(attempt_no, bool) else None)
        if (not isinstance(run, Mapping) or not isinstance(attempt, Mapping)
            or run.get("attempts") != attempt_no or run.get("fence") != fence
            or attempt.get("fence") != fence):
            raise SchedulerError("candidate_recovery_current_binding_missing")
        authority_fields = {
            "goal_id": envelope.get("goal_id"),
            "goal_revision": envelope.get("goal_revision"),
            "node_id": envelope.get("node_id"),
            "dispatch_key": dispatch_key,
            "work_unit_id": admission.get("work_unit_id"),
            "run_id": run_id,
            "attempt": attempt_no,
            "fence": fence,
        }
        if any(authority.get(field) != expected for field, expected in authority_fields.items()):
            raise SchedulerError("candidate_recovery_authority_binding_mismatch")
        if (not isinstance(authority.get("owner_principal"), str)
            or not authority["owner_principal"].strip()):
            raise SchedulerError("candidate_recovery_authority_context_invalid")
        self._sha256_digest("approved_manifest_digest", authority.get("approved_manifest_digest"))
        return dict(dispatch)

    def consume(self, envelope: dict[str, Any], *, now: float | None = None,
                complete: bool = True) -> dict[str, Any]:
        value = self._validate(envelope)
        delivery_binding = value.pop("_delivery_binding", None)
        value.pop("_delivery_contract", None)
        value.pop("_delivery_plan", None)
        dispatch_key = str(value["dispatch_key"])
        safe = hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()[:32]
        packet_path = Path(value["packet_path"])
        if self.candidate_recovery_admission is not None:
            self._validate_candidate_recovery_reuse(value, dispatch_key)
        result = self.store.consume_dispatch(
            value,
            parent_goal_id=self.goal_id,
            goal_id=self.goal_id,
            goal_revision=self.goal_revision,
            node_id=self.node_id,
            work_unit_id=f"dispatch-{safe}",
            worker_id=self.holder,
            base_sha=value["wave_base_sha"],
            read_set=self.work_definition.get("read_set", (f"delivery-dispatch/{packet_path.name}",)),
            write_set=self.work_definition.get("write_set", (f"task-state/{self.goal_id}/{self.node_id}",)),
            dependencies=self.work_definition.get("dependencies", ()),
            **({"delivery_after": self.work_definition["delivery_after"]}
               if "delivery_after" in self.work_definition else {}),
            enforce_scope_conflicts=bool(self.work_definition),
            max_active_workers=self.work_definition.get("max_workers"),
            worktree=value.get("worktree") or str(self.store.root),
            branch=value.get("branch") or f"scheduler/{self.node_id}",
            state_root=str(self.store.root),
            holder=self.holder,
            lease_seconds=self.lease_seconds,
            now=now,
            delivery_binding=delivery_binding,
            phase_context=self.phase_context,
            resource_requirements=self.work_definition.get("resource_requirements"),
        )
        receipt = result.get("receipt")
        if not isinstance(receipt, dict) or receipt.get("schema") != SUCCESSOR_DISPATCH_RECEIPT_SCHEMA:
            raise SchedulerError("successor dispatch receipt invalid")
        if receipt.get("dispatch_key") != dispatch_key or receipt.get("packet_digest") != value["packet_digest"]:
            raise SchedulerError("successor dispatch receipt binding mismatch")
        if not isinstance(result.get("attempt_record"), dict):
            try:
                result["work_unit"] = self.store.get_work_unit(str(result["work_unit_id"]))
                result["run"] = self.store.get_run(str(result["run_id"]))
                result["attempt_record"] = self.store.get_attempt(str(result["run_id"]), int(result["attempt"]))
            except (KeyError, ValueError, TypeError) as exc:
                raise SchedulerError("successor dispatch queue readback missing") from exc
        run_readback = result.get("run")
        unit_readback = result.get("work_unit")
        if not isinstance(run_readback, dict) or not isinstance(unit_readback, dict):
            raise SchedulerError("successor_dispatch_base_revision_readback_missing")
        if (run_readback.get("base_sha") != value["wave_base_sha"]
            or unit_readback.get("base_sha") != value["wave_base_sha"]):
            raise SchedulerError("successor_dispatch_base_revision_mismatch")
        initial_reused = bool(result.get("reused"))
        initial_counts = {
            key: int(result.get(key, 0))
            for key in ("runs_created", "attempts_created")
        }
        executor_receipt = receipt.get("executor_receipt")
        executor_invocations = 0
        executor_recovery = receipt.get("executor_recovery_receipt") if receipt.get("executor_status") == "unknown" else None
        retry_result = None
        phase_pending = None
        if (
            self.executor is not None
            and executor_receipt is None
            and executor_recovery is None
            and receipt.get("executor_status") != "exhausted"
        ):
            attempt_record = result.get("attempt_record")
            if not isinstance(attempt_record, dict) or attempt_record.get("state") != "ready":
                raise SchedulerError("successor Attempt is not ready for executor admission")
            executor_request = {
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_path": value["packet_path"],
                "packet_digest": value["packet_digest"],
                "base_sha": value["wave_base_sha"],
                "worktree": attempt_record.get("workspace_ref") or value.get("worktree"),
                "branch": receipt.get("executor_unknown_disposition", {}).get("branch") or value.get("branch"),
                "goal_id": self.goal_id,
                "goal_revision": self.goal_revision,
                "node_id": self.node_id,
                "work_unit_id": result.get("work_unit_id"),
                "run_id": result.get("run_id"),
                "attempt": result.get("attempt"),
                "fence": attempt_record.get("fence"),
                "completion_repair": receipt.get("completion_failures", [])[-1:],
            }
            try:
                raw_executor_receipt = self.executor.dispatch(executor_request)
                executor_receipt = validate_executor_receipt(raw_executor_receipt, executor_request)
                accepted = self.store.accept_executor_dispatch(
                    dispatch_key,
                    executor_receipt,
                    lease_seconds=self.lease_seconds,
                    now=now,
                )
            except PhaseJobPending as exc:
                phase_pending = exc
            except SuccessorExecutorError as exc:
                details = getattr(exc, "details", {})
                failure = details.get("failure_receipt") if isinstance(details, dict) else None
                recovery = details.get("recovery_receipt") if isinstance(details, dict) else None
                if isinstance(failure, dict):
                    try:
                        retry_result = self.store.retry_executor_dispatch(
                            dispatch_key,
                            failure,
                            now=now,
                        )
                    except Exception as retry_exc:
                        raise SchedulerError(f"executor_retry_failed:{type(retry_exc).__name__}") from retry_exc
                    retry_result["reused"] = initial_reused
                    retry_result["runs_created"] = initial_counts["runs_created"]
                    retry_result["attempts_created"] = initial_counts["attempts_created"]
                    result = retry_result
                    receipt = result.get("receipt")
                    executor_receipt = None
                elif isinstance(recovery, dict):
                    try:
                        reconciled = self.store.reconcile_executor_unknown(
                            dispatch_key,
                            recovery,
                            now=now,
                        )
                    except Exception as recovery_exc:
                        raise SchedulerError(f"executor_recovery_failed:{type(recovery_exc).__name__}") from recovery_exc
                    reconciled["reused"] = initial_reused
                    reconciled["runs_created"] = initial_counts["runs_created"]
                    reconciled["attempts_created"] = initial_counts["attempts_created"]
                    result = reconciled
                    receipt = result.get("receipt")
                    executor_recovery = recovery
                else:
                    raise SchedulerError(str(exc)) from exc
            except Exception as exc:
                raise SchedulerError(f"executor_dispatch_failed:{type(exc).__name__}") from exc
            else:
                accepted["reused"] = initial_reused
                accepted["runs_created"] = initial_counts["runs_created"]
                accepted["attempts_created"] = initial_counts["attempts_created"]
                result = accepted
                receipt = result.get("receipt")
                # The store strips transport-only ``invoked``/``reused`` flags
                # before sealing the durable executor receipt.  Continue with
                # that canonical object so completion evidence has the same
                # bytes on the first delivery and on replay.
                executor_receipt = result.get("executor_receipt")
                if not isinstance(receipt, dict):
                    raise SchedulerError("executor acceptance receipt missing")
                executor_invocations = 1 if result.get("invoked") is True else 0
        if self.phase_context is not None and executor_receipt is not None:
            self.executor.settle_accepted()
        if not isinstance(result.get("work_unit"), dict):
            result["work_unit"] = self.store.get_work_unit(str(result["work_unit_id"]))
        if not isinstance(result.get("run"), dict):
            result["run"] = self.store.get_run(str(result["run_id"]))
        if not isinstance(result.get("attempt_record"), dict):
            result["attempt_record"] = self.store.get_attempt(str(result["run_id"]), int(result["attempt"]))
        executor_status = "unknown" if executor_recovery is not None else receipt.get("executor_status", "ready")
        if executor_status not in {"ready", "accepted", "unknown", "exhausted"}:
            raise SchedulerError("successor executor status invalid")
        output = {
            "schema": SUCCESSOR_DISPATCH_RECEIPT_SCHEMA,
            "status": "consumed",
            "dispatch_key": dispatch_key,
            "envelope_digest": value["envelope_digest"],
            "receipt": receipt,
            "receipt_digest": result.get("receipt_digest"),
            "work_unit_id": result.get("work_unit_id"),
            "run_id": result.get("run_id"),
            "attempt": result.get("attempt"),
            "work_unit": result.get("work_unit"),
            "run": result.get("run"),
            "attempt_record": result.get("attempt_record"),
            "reused": bool(result.get("reused")),
            "runs_created": int(result.get("runs_created", 0)),
            "attempts_created": int(result.get("attempts_created", 0)),
            "scheduler_ready": True,
            "executor_status": executor_status,
            "executor_receipt": executor_receipt,
            "executor_receipt_digest": (
                executor_receipt.get("receipt_digest")
                if isinstance(executor_receipt, dict)
                else None
            ),
            "executor_invocations": executor_invocations,
            "executor_launches": int(receipt.get("executor_launches", 0)),
            "executor_recovery": executor_recovery,
            "retry_scheduled": bool(retry_result and retry_result.get("retry_scheduled")),
            "retry_exhausted": bool(retry_result and retry_result.get("retry_exhausted")),
            "provider_invocations": 0,
            "manual_prompts": 0,
        }
        if phase_pending is not None:
            output["status"] = "waiting"
            output["reason"] = phase_pending.reason
            output["completion"] = {"status": "waiting", "reason": phase_pending.reason,
                                    "phase_job_id": phase_pending.job["job_id"]}
        elif complete and self.completion_controller is not None:
            if self.candidate_recovery_admission is None:
                output["completion"] = self.completion_controller.advance(value, output)
            else:
                output["completion"] = self.completion_controller.advance(
                    value, output, allow_retry=self.completion_controller.permits_preserved_retry,
                    candidate_recovery_admission=self.candidate_recovery_admission)
        else:
            output["completion"] = {"status": "incomplete", "reason": "completion_adapter_missing"}
        return output

    def readback(self, dispatch_key: str) -> dict[str, Any] | None:
        return self.store.get_dispatch_consumption(dispatch_key)


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SchedulerError(f"{name} must be a non-empty string")
    return value.strip()


def _sort_key(unit: dict[str, Any]) -> tuple[float, str, str]:
    return (float(unit.get("created_at") or 0), str(unit.get("node_id") or ""), str(unit.get("work_unit_id") or ""))


class ParallelScheduler:
    """Select and start one compatible 2–3 child wave for a parent Goal."""

    def __init__(
        self,
        store: WorkUnitStore,
        *,
        min_workers: int = MIN_WORKERS,
        max_workers: int = MAX_WORKERS,
        lease_seconds: float = 60.0,
        delivery_contract: Mapping[str, Any] | None = None,
        delivery_packet: Mapping[str, Any] | None = None,
        planning_capability: Mapping[str, Any] | None = None,
    ):
        if not isinstance(store, WorkUnitStore):
            raise SchedulerConfigurationError("store must be a WorkUnitStore")
        if min_workers < MIN_WORKERS or min_workers > MAX_WORKERS:
            raise SchedulerConfigurationError("min_workers must be between 2 and 3")
        if max_workers < min_workers or max_workers > MAX_WORKERS:
            raise SchedulerConfigurationError("max_workers must be between min_workers and 3")
        if float(lease_seconds) < 0:
            raise SchedulerConfigurationError("lease_seconds must not be negative")
        self.store = store
        self.min_workers = int(min_workers)
        self.max_workers = int(max_workers)
        self.lease_seconds = float(lease_seconds)
        self.delivery_contract = dict(delivery_contract) if isinstance(delivery_contract, Mapping) else None
        self.delivery_packet = dict(delivery_packet) if isinstance(delivery_packet, Mapping) else None
        self.planning_capability = dict(planning_capability) if isinstance(planning_capability, Mapping) else None

    @staticmethod
    def _delivery_artifact(
        value: Mapping[str, Any] | None,
        unit: Mapping[str, Any],
        *,
        contract: bool,
    ) -> dict[str, Any] | None:
        """Resolve one canonical contract/packet without inventing identity."""
        if not isinstance(value, Mapping):
            return None
        if contract and value.get("schema") == delivery_unit_contract.SCHEMA:
            return dict(value)
        if not contract and (value.get("delivery_unit_managed") is True or "packet_id" in value):
            return dict(value)
        for key in (str(unit.get("work_unit_id")), str(unit.get("node_id"))):
            candidate = value.get(key)
            if isinstance(candidate, Mapping):
                return dict(candidate)
        return None

    def _consume_pending_replan(self, unit: Mapping[str, Any]) -> dict[str, Any] | None:
        """Route a persisted replan through the contract checker before admission."""
        pending = [
            item
            for item in self.store.planning_requests(work_unit_id=str(unit["work_unit_id"]))
            if item.get("status") == "pending"
        ]
        if not pending:
            return None
        contract = self._delivery_artifact(self.delivery_contract, unit, contract=True)
        if contract is None:
            raise SchedulerError("planning_request_contract_missing")
        request = pending[0]
        result = self.store.consume_planning_request(
            str(request["request_id"]),
            contract=contract,
            capability=self.planning_capability,
        )
        if result.get("status") != "completed":
            reason = result.get("reason") or "planning_request_pending"
            raise SchedulerError(f"planning_request_{reason}")
        return result

    def _delivery_admission(self, parent: Mapping[str, Any], unit: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        contract_input = self._delivery_artifact(self.delivery_contract, unit, contract=True)
        packet = self._delivery_artifact(self.delivery_packet, unit, contract=False)
        if contract_input is None:
            raise SchedulerError("delivery_unit_contract_missing")
        if packet is None:
            raise SchedulerError("delivery_unit_packet_missing")
        contract = delivery_unit_contract.validate_contract(contract_input)
        if contract["goal"]["id"] != parent["goal_id"] or contract["goal"]["revision"] != parent["goal_revision"]:
            raise SchedulerError("delivery_unit_goal_identity_mismatch")
        if contract["node"]["id"] != unit["node_id"]:
            raise SchedulerError("delivery_unit_node_identity_mismatch")
        plan = delivery_unit_contract.plan_delivery_unit(contract)
        admission = delivery_unit_contract.verify_packet_binding(packet, contract, plan)
        if admission.get("verdict") != "GREEN":
            raise SchedulerError(f"delivery_unit_{admission.get('reason', 'packet_binding_failed')}")
        binding = {
            "schema": "lh-delivery-contract-binding/v1",
            "status": "bound",
            "unit_id": contract["unit_id"],
            "contract_digest": contract["contract_digest"],
            "plan_verdict_digest": plan["plan_verdict_digest"],
            "goal_id": contract["goal"]["id"],
            "goal_revision": contract["goal"]["revision"],
            "node_id": contract["node"]["id"],
            "node_kind": contract["node"]["kind"],
            "packet_digest": packet.get("packet_digest"),
            "identity": {
                "goal_id": contract["goal"]["id"],
                "goal_revision": contract["goal"]["revision"],
                "node_id": contract["node"]["id"],
                "unit_id": contract["unit_id"],
            },
        }
        return contract, plan, {"binding": binding, "contract": contract, "plan_verdict": plan}

    def _consume_pending_planning_requests(self, parent_goal_id: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for unit in self.store.list_work_units(parent_goal_id):
            pending = [
                item
                for item in self.store.planning_requests(work_unit_id=unit["work_unit_id"])
                if item.get("status") == "pending" and isinstance(item.get("request"), Mapping)
                and item["request"].get("binding", {}).get("supersedes_contract_digest")
            ]
            if not pending:
                continue
            result = self._consume_pending_replan(unit)
            if result is not None:
                results.append({"work_unit_id": unit["work_unit_id"], "result": result})
        return results

    def _worker_limit(self, worker_count: int | None) -> int:
        if worker_count is None:
            return self.max_workers
        if int(worker_count) not in {MIN_WORKERS, MAX_WORKERS}:
            raise SchedulerConfigurationError("worker_count must be 2 or 3")
        return min(int(worker_count), self.max_workers)

    def inspect(self, parent_goal_id: str, *, base_sha: str | None = None) -> dict[str, Any]:
        """Return deterministic ready, blocked, active and conflict candidates."""

        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        parent = self.store.get_parent_goal(parent_goal_id)
        self.store.validate_dag(parent_goal_id)
        expected_base = parent["base_sha"] if base_sha is None else base_sha
        if expected_base != parent["base_sha"]:
            return {
                "schema": SCHEMA,
                "status": "rejected",
                "reason": "base_mismatch",
                "parent_goal_id": parent_goal_id,
                "expected_base": parent["base_sha"],
                "supplied_base": expected_base,
                "active": [],
                "ready": [],
                "blocked": [],
                "base_mismatch": [],
            }

        active: list[dict[str, Any]] = []
        ready: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        base_mismatch: list[dict[str, Any]] = []
        for unit in self.store.list_work_units(parent_goal_id):
            if unit["state"] in ACTIVE_STATES:
                active.append(unit)
                continue
            if unit["state"] not in WAITING_STATES:
                continue
            if unit["base_sha"] != expected_base:
                base_mismatch.append({
                    "work_unit_id": unit["work_unit_id"],
                    "node_id": unit["node_id"],
                    "base_sha": unit["base_sha"],
                    "expected_base": expected_base,
                })
                continue
            dependencies = []
            for reference in unit["dependencies"]:
                dependency = self.store.resolve_dependency(parent_goal_id, reference)
                dependencies.append({
                    "work_unit_id": dependency["work_unit_id"],
                    "node_id": dependency["node_id"],
                    "state": dependency["state"],
                })
            unmet = [item for item in dependencies if item["state"] != "integrated"]
            if unmet:
                blocked.append({
                    "work_unit_id": unit["work_unit_id"],
                    "node_id": unit["node_id"],
                    "dependencies": unmet,
                })
            else:
                ready.append(unit)
        active.sort(key=_sort_key)
        ready.sort(key=_sort_key)
        return {
            "schema": SCHEMA,
            "status": "ready" if ready else "idle",
            "parent_goal_id": parent_goal_id,
            "goal_revision": parent["goal_revision"],
            "base_sha": expected_base,
            "active": active,
            "ready": ready,
            "blocked": blocked,
            "base_mismatch": base_mismatch,
        }

    def ready_work_units(self, parent_goal_id: str, *, base_sha: str | None = None) -> list[dict[str, Any]]:
        return self.inspect(parent_goal_id, base_sha=base_sha)["ready"]

    @staticmethod
    def _conflict_pairs(units: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        values = sorted(list(units), key=_sort_key)
        conflicts = []
        for left, right in combinations(values, 2):
            paths = read_write_conflicts(left, right)
            if paths:
                conflicts.append({
                    "left_work_unit_id": left["work_unit_id"],
                    "right_work_unit_id": right["work_unit_id"],
                    "paths": paths,
                })
        return conflicts

    @staticmethod
    def _choose_compatible(
        candidates: list[dict[str, Any]],
        occupied: list[dict[str, Any]],
        capacity: int,
    ) -> list[dict[str, Any]]:
        if capacity <= 0:
            return []
        candidates = sorted(candidates, key=_sort_key)
        occupied = sorted(occupied, key=_sort_key)
        for size in range(min(capacity, len(candidates)), 0, -1):
            for choice in combinations(candidates, size):
                group = [*occupied, *choice]
                if all(compatible_work_units(left, right) for left, right in combinations(group, 2)):
                    return list(choice)
        return []

    @staticmethod
    def _record_for_unit(store: WorkUnitStore, unit: dict[str, Any], *, status: str | None = None) -> dict[str, Any]:
        run = store.get_run_for_work_unit(unit["work_unit_id"])
        attempt = store.get_attempt(run["run_id"]) if run is not None else None
        lease = store.lease_for(unit["work_unit_id"])
        record = {
            "schema": SCHEMA,
            "parent_goal_id": unit["parent_goal_id"],
            "work_unit_id": unit["work_unit_id"],
            "node_id": unit["node_id"],
            "worker_id": unit["worker_id"],
            "base_sha": unit["base_sha"],
            "worktree": unit["worktree"],
            "branch": unit["branch"],
            "state_root": unit["state_root"],
            "state": unit["state"],
            "run_id": run["run_id"] if run else None,
            "run_state": run["state"] if run else None,
            "attempt": attempt["attempt"] if attempt else None,
            "fence": attempt["fence"] if attempt else (lease["fence"] if lease else None),
            "holder": attempt["holder"] if attempt else (lease["holder"] if lease else None),
            "status": status or unit["state"],
            "reused": bool(run),
        }
        if lease is not None:
            record["expires_at"] = lease["expires_at"]
        return record

    def dispatch(
        self,
        parent_goal_id: str,
        *,
        holder: str = "parallel-scheduler",
        worker_count: int | None = None,
        base_sha: str | None = None,
    ) -> dict[str, Any]:
        """Dispatch a compatible wave, or return a machine-readable wait/reject."""

        parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        holder = _required_text("holder", holder)
        limit = self._worker_limit(worker_count)
        reconciled = self.store.reconcile_expired()
        parent = self.store.get_parent_goal(parent_goal_id)
        if parent["state"] != "active":
            return self._base_result(parent, status="blocked", reason="parent_not_active", reconciled=reconciled)
        try:
            inspection = self.inspect(parent_goal_id, base_sha=base_sha)
        except DAGCycleError as exc:
            return self._base_result(parent, status="rejected", reason="dag_cycle", reconciled=reconciled, details={"cycle": list(exc.cycle)})
        except DependencyError as exc:
            return self._base_result(parent, status="rejected", reason="missing_dependency", reconciled=reconciled, details={"error": str(exc)})
        if inspection["status"] == "rejected":
            return self._base_result(parent, status="rejected", reason=inspection["reason"], reconciled=reconciled, details=inspection)

        try:
            planning_results = self._consume_pending_planning_requests(parent_goal_id)
        except SchedulerError as exc:
            reason = str(exc)
            return self._base_result(
                parent,
                status="waiting",
                reason=reason,
                reconciled=reconciled,
                details={"inspection": inspection, "planning": "pending"},
            )

        active = list(inspection["active"])
        ready = list(inspection["ready"])
        active_conflicts = self._conflict_pairs(active)
        if active_conflicts:
            return self._base_result(parent, status="rejected", reason="read_write_overlap", reconciled=reconciled, details={"conflicts": active_conflicts, "inspection": inspection})

        capacity = max(0, limit - len(active))
        selected = self._choose_compatible(ready, active, capacity)
        if len(active) + len(selected) < self.min_workers:
            all_conflicts = self._conflict_pairs([*active, *ready])
            if len(active) == 0 and len(ready) >= self.min_workers and len(selected) < self.min_workers:
                return self._base_result(parent, status="rejected", reason="read_write_overlap", reconciled=reconciled, details={"conflicts": all_conflicts, "inspection": inspection})
            if active and all_conflicts and not selected:
                return self._base_result(parent, status="rejected", reason="read_write_overlap", reconciled=reconciled, details={"conflicts": all_conflicts, "inspection": inspection})
            return self._base_result(parent, status="waiting", reason="fewer_than_two_compatible_workers", reconciled=reconciled, details={"inspection": inspection})

        dispatched: list[dict[str, Any]] = []
        for unit in selected:
            child_holder = f"{holder}:{unit['work_unit_id']}"
            try:
                self._consume_pending_replan(unit)
                _contract, _plan, delivery_binding = self._delivery_admission(parent, unit)
                self.store.bind_delivery_contract(
                    unit["work_unit_id"],
                    binding=delivery_binding["binding"],
                    contract=delivery_binding["contract"],
                    plan_verdict=delivery_binding["plan_verdict"],
                )
                started = self.store.start_attempt(
                    unit["work_unit_id"],
                    child_holder,
                    lease_seconds=self.lease_seconds,
                    workspace_ref=unit["worktree"],
                )
            except LeaseBusyError as exc:
                return self._base_result(parent, status="waiting", reason="lease_busy", reconciled=reconciled, details={"work_unit_id": exc.work_unit_id, "holder": exc.holder, "inspection": inspection, "dispatched": dispatched})
            except SchedulerError as exc:
                reason = str(exc)
                status = "waiting" if reason.startswith("planning_request_") else "rejected"
                return self._base_result(parent, status=status, reason=reason, reconciled=reconciled, details={"inspection": inspection, "dispatched": dispatched})
            record = self._record_for_unit(self.store, self.store.get_work_unit(unit["work_unit_id"]), status="dispatched")
            record["reused"] = bool(started.get("reused"))
            dispatched.append(record)

        active_records = [self._record_for_unit(self.store, self.store.get_work_unit(unit["work_unit_id"]), status="active") for unit in active]
        all_workers = sorted([*active_records, *dispatched], key=lambda item: (str(item.get("node_id")), str(item.get("work_unit_id"))))
        status = "dispatched" if dispatched else ("already_running" if active_records else "idle")
        return {
            "schema": SCHEMA,
            "status": status,
            "reason": None,
            "parent_goal_id": parent["parent_goal_id"],
            "goal_id": parent["goal_id"],
            "goal_revision": parent["goal_revision"],
            "base_sha": parent["base_sha"],
            "workers": all_workers,
            "dispatched": dispatched,
            "active": active_records,
            "blocked": inspection["blocked"],
            "base_mismatch": inspection["base_mismatch"],
            "reconciled": reconciled,
            "planning": planning_results,
            "run_ids": [item["run_id"] for item in all_workers if item.get("run_id")],
        }

    schedule = dispatch
    dispatch_ready = dispatch
    schedule_wave = dispatch

    @staticmethod
    def _base_result(parent: dict[str, Any], *, status: str, reason: str, reconciled: list[dict[str, Any]], details: dict[str, Any] | None = None) -> dict[str, Any]:
        result = {
            "schema": SCHEMA,
            "status": status,
            "reason": reason,
            "parent_goal_id": parent["parent_goal_id"],
            "goal_id": parent["goal_id"],
            "goal_revision": parent["goal_revision"],
            "base_sha": parent["base_sha"],
            "workers": [],
            "dispatched": [],
            "active": [],
            "reconciled": reconciled,
            "run_ids": [],
        }
        if details:
            result.update(details)
        return result

    def finish(
        self,
        work_unit_id: str,
        *,
        holder: str,
        fence: int,
        state: str = "verified",
        receipt_ref: str | None = None,
        receipt_digest: str | None = None,
    ) -> bool:
        """Pass the current child result through the store's fence check."""

        return self.store.finish_attempt(
            work_unit_id,
            holder=holder,
            fence=fence,
            state=state,
            receipt_ref=receipt_ref,
            receipt_digest=receipt_digest,
        )

    finish_attempt = finish

    def integrate(
        self,
        work_unit_id: str,
        *,
        holder: str | None = None,
        fence: int | None = None,
        receipt_ref: str | None = None,
        receipt_digest: str | None = None,
    ) -> bool:
        """Release the child dependency only after integration is recorded."""

        return self.store.mark_integrated(
            work_unit_id,
            holder=holder,
            fence=fence,
            receipt_ref=receipt_ref,
            receipt_digest=receipt_digest,
        )

    mark_integrated = integrate

    def recover(self) -> list[dict[str, Any]]:
        """Reconcile expired leases while retaining each WorkUnit's Run id."""

        return self.store.reconcile_expired()


ParallelDAGScheduler = ParallelScheduler
Scheduler = ParallelScheduler


def dispatch_ready_workers(
    store: WorkUnitStore,
    parent_goal_id: str,
    *,
    holder: str = "parallel-scheduler",
    worker_count: int | None = None,
    base_sha: str | None = None,
    lease_seconds: float = 60.0,
) -> dict[str, Any]:
    return ParallelScheduler(store, lease_seconds=lease_seconds).dispatch(
        parent_goal_id,
        holder=holder,
        worker_count=worker_count,
        base_sha=base_sha,
    )


schedule_parallel = dispatch_ready_workers


__all__ = [
    "ACTIVE_STATES", "MAX_WORKERS", "MIN_WORKERS", "ParallelDAGScheduler", "ParallelScheduler", "SCHEMA", "SUCCESSOR_DISPATCH_RECEIPT_SCHEMA", "SUCCESSOR_DISPATCH_SCHEMA", "SuccessorDispatchConsumer", "Scheduler", "SchedulerConfigurationError", "SchedulerError", "dispatch_ready_workers", "schedule_parallel",
]

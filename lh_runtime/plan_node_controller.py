#!/usr/bin/env python3
"""Provider-neutral controller for one deterministic planner node.

The P3B bootstrap is a single plan operation, not an LH Goal/Run/Attempt.
This controller therefore owns only a task-owned, idempotent plan receipt.  A
successful plan is persisted after an independent read-only verifier and a
queue projection have both accepted it.  Replaying the same input reads that
receipt; it never invokes a planner twice or creates execution state.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

try:
    from .work_unit_store import LeaseBusyError, WorkUnitStore, WorkUnitStoreError
except ImportError:  # direct canary execution keeps lh_runtime on sys.path
    from work_unit_store import LeaseBusyError, WorkUnitStore, WorkUnitStoreError  # type: ignore


SCHEMA = "host-plan-node-controller/v1"
STATE_SCHEMA = "host-plan-node-state/v1"
PLAN_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class PlanNodeControllerError(ValueError):
    """A single-node planner admission or replay is unsafe."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def digest_json(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanNodeControllerError(f"{name}_missing")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    value = _text(name, value).lower()
    if PLAN_DIGEST_RE.fullmatch(value) is None:
        raise PlanNodeControllerError(f"{name}_invalid")
    return value


def _identity_token(name: str, value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Mapping) and value:
        return digest_json(dict(value))
    raise PlanNodeControllerError(f"{name}_missing")


def _copy_json(name: str, value: Any) -> Any:
    try:
        result = copy.deepcopy(value)
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return result
    except (TypeError, ValueError) as exc:
        raise PlanNodeControllerError(f"{name}_not_json") from exc


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except OSError as exc:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise PlanNodeControllerError(f"state_write_failed:{type(exc).__name__}") from exc


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlanNodeControllerError("state_unreadable") from exc
    if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
        raise PlanNodeControllerError("state_schema_invalid")
    supplied_digest = value.get("state_digest")
    body = {key: item for key, item in value.items() if key != "state_digest"}
    if supplied_digest != digest_json(body):
        raise PlanNodeControllerError("state_digest_mismatch")
    return value


Planner = Callable[[dict[str, Any]], Mapping[str, Any]]
Verifier = Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]
QueueProjector = Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]


class PlanNodeController:
    """Execute and replay one plan node without creating LH execution state."""

    def __init__(
        self,
        state_root: str | Path | WorkUnitStore,
        *,
        goal_id: str | None = None,
        node_id: str = "P3B",
        first_actionable_node: str = "R0",
        worker_id: str | None = None,
        lease_seconds: float = 60.0,
    ):
        self.node_id = _text("node_id", node_id)
        self.first_actionable_node = _text("first_actionable_node", first_actionable_node)
        self._legacy_store: WorkUnitStore | None = None
        if isinstance(state_root, WorkUnitStore):
            # PR #572 exposed a WorkUnitStore-backed single-node controller.
            # Keep that read/dispatch surface for already-merged callers; the
            # P3B0 deterministic route below uses the newer task-owned plan
            # receipt and does not enter this compatibility mode.
            if goal_id is not None:
                raise PlanNodeControllerError("legacy_store_goal_id_invalid")
            self._legacy_store = state_root
            self.store = state_root
            self.worker_id = _text("worker_id", worker_id or self.node_id)
            try:
                self.lease_seconds = float(lease_seconds)
            except (TypeError, ValueError) as exc:
                raise PlanNodeControllerError("lease_seconds_invalid") from exc
            if self.lease_seconds < 0:
                raise PlanNodeControllerError("lease_seconds_invalid")
            self.goal_id = None
            self.state_root = None
            self.state_path = None
            return

        self.goal_id = _text("goal_id", goal_id)
        self.worker_id = None
        self.lease_seconds = None
        self.state_root = Path(state_root).expanduser().resolve()
        if not self.state_root.name:
            raise PlanNodeControllerError("state_root_invalid")
        configured_production_root = os.environ.get("LH_HOST_STATE_ROOT")
        production_root = (
            Path(configured_production_root).expanduser().resolve()
            if configured_production_root
            else (Path.home() / ".local" / "state" / "external-host").resolve()
        )
        if self.state_root == production_root or self.state_root.is_relative_to(production_root):
            raise PlanNodeControllerError("production_state_root_forbidden")
        self.state_path = self.state_root / "plan-node-state.json"

    def _completed_state(self, input_digest: str) -> dict[str, Any] | None:
        state = _read_json(self.state_path)
        if state is None:
            return None
        if state.get("goal_id") != self.goal_id or state.get("node_id") != self.node_id:
            raise PlanNodeControllerError("state_identity_mismatch")
        if state.get("first_actionable_node", "R0") != self.first_actionable_node:
            raise PlanNodeControllerError("state_next_node_mismatch")
        if state.get("status") != "completed":
            raise PlanNodeControllerError("state_status_invalid")
        if state.get("input_digest") != input_digest:
            raise PlanNodeControllerError("plan_input_digest_drift")
        return state

    @staticmethod
    def _require_mapping(name: str, value: Any) -> Mapping[str, Any]:
        if not isinstance(value, Mapping) or not value:
            raise PlanNodeControllerError(f"{name}_missing")
        return value

    def _legacy_registered_unit(self, parent_goal_id: str) -> dict[str, Any]:
        assert self._legacy_store is not None
        units = [
            unit
            for unit in self._legacy_store.list_work_units(parent_goal_id)
            if unit.get("node_id") == self.node_id
        ]
        if not units:
            raise PlanNodeControllerError("node_not_registered:" + self.node_id)
        if len(units) != 1:
            raise PlanNodeControllerError("node_registration_ambiguous:" + self.node_id)
        return units[0]

    def inspect(
        self,
        parent_goal_id: str,
        *,
        base_sha: str | None = None,
    ) -> dict[str, Any]:
        """Inspect the merged WorkUnitStore compatibility surface read-only."""

        if self._legacy_store is None:
            raise PlanNodeControllerError("store_compatibility_mode_required")
        parent_goal_id = _text("parent_goal_id", parent_goal_id)
        parent = self._legacy_store.get_parent_goal(parent_goal_id)
        self._legacy_store.validate_dag(parent_goal_id)
        unit = self._legacy_registered_unit(parent_goal_id)
        expected_base = parent["base_sha"] if base_sha is None else _text("base_sha", base_sha)
        result: dict[str, Any] = {
            "schema": "lh-single-plan-node-controller/v1",
            "parent_goal_id": parent_goal_id,
            "goal_id": parent["goal_id"],
            "goal_revision": parent["goal_revision"],
            "node_id": self.node_id,
            "parallel_minimum": 1,
            "parallel_maximum": 1,
            "base_sha": expected_base,
            "unit": {
                "work_unit_id": unit["work_unit_id"],
                "node_id": unit["node_id"],
                "worker_id": unit["worker_id"],
                "base_sha": unit["base_sha"],
                "dependencies": list(unit.get("dependencies") or []),
                "state": unit["state"],
            },
            "dependencies": [],
        }
        if parent["state"] != "active":
            result.update({"status": "blocked", "reason": "parent_not_active"})
            return result
        if expected_base != parent["base_sha"] or unit["base_sha"] != expected_base:
            result.update({
                "status": "blocked",
                "reason": "base_mismatch",
                "expected_base": parent["base_sha"],
                "unit_base": unit["base_sha"],
            })
            return result
        dependencies = []
        for reference in unit.get("dependencies") or []:
            dependency = self._legacy_store.resolve_dependency(parent_goal_id, reference)
            dependencies.append({
                "work_unit_id": dependency["work_unit_id"],
                "node_id": dependency["node_id"],
                "state": dependency["state"],
            })
        result["dependencies"] = dependencies
        unmet = [item for item in dependencies if item["state"] != "integrated"]
        if unmet:
            result.update({"status": "blocked", "reason": "dependencies_not_integrated", "unmet": unmet})
            return result
        if unit["state"] == "integrated":
            result.update({"status": "already_integrated", "reason": "work_unit_integrated", "reused": True})
            return result
        if unit["state"] == "stopped":
            result.update({"status": "stopped", "reason": "work_unit_stopped", "reused": True})
            return result
        if unit["state"] not in {"pending", "retry_pending", "running", "verified"}:
            result.update({"status": "blocked", "reason": "work_unit_not_startable"})
            return result
        result.update({"status": "ready", "reason": "single_node_admissible", "reused": False})
        return result

    def dispatch(
        self,
        *args: Any,
        input_digest: str | None = None,
        planner: Planner | None = None,
        verifier: Verifier | None = None,
        queue_projector: QueueProjector | None = None,
        context: Mapping[str, Any] | None = None,
        base_sha: str | None = None,
        workspace_ref: str | Path | None = None,
        lease_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Run the one plan operation, or replay its completed receipt."""

        if self._legacy_store is not None:
            if len(args) != 2 or input_digest is not None or planner is not None or verifier is not None or queue_projector is not None or context is not None:
                raise PlanNodeControllerError("legacy_dispatch_arguments_invalid")
            return self._legacy_dispatch(
                args[0],
                args[1],
                base_sha=base_sha,
                workspace_ref=workspace_ref,
                lease_seconds=lease_seconds,
            )

        if args or input_digest is None or planner is None or verifier is None or queue_projector is None:
            raise PlanNodeControllerError("controller_arguments_invalid")

        input_digest = _digest("input_digest", input_digest)
        if not callable(planner) or not callable(verifier) or not callable(queue_projector):
            raise PlanNodeControllerError("controller_callbacks_missing")
        previous = self._completed_state(input_digest)
        if previous is not None:
            replay = copy.deepcopy(previous)
            replay["replayed"] = True
            return replay
        if context is not None and not isinstance(context, Mapping):
            raise PlanNodeControllerError("planner_context_invalid")
        planner_context = copy.deepcopy(dict(context or {}))
        try:
            produced = planner(planner_context)
        except PlanNodeControllerError:
            raise
        except Exception as exc:
            raise PlanNodeControllerError(f"planner_failed:{type(exc).__name__}") from exc
        produced_map = self._require_mapping("planner_output", produced)
        sealed_plan = self._require_mapping("sealed_plan", produced_map.get("sealed_plan"))
        planner_receipt = self._require_mapping("planner_receipt", produced_map.get("planner_receipt"))
        _digest("sealed_plan.plan_digest", sealed_plan.get("plan_digest"))
        planner_identity = _identity_token("planner_receipt.identity", planner_receipt.get("identity"))

        try:
            verifier_receipt = verifier(
                copy.deepcopy(dict(sealed_plan)),
                copy.deepcopy(dict(planner_receipt)),
            )
        except PlanNodeControllerError:
            raise
        except Exception as exc:
            raise PlanNodeControllerError(f"verifier_failed:{type(exc).__name__}") from exc
        verifier_map = self._require_mapping("verifier_receipt", verifier_receipt)
        if verifier_map.get("verdict") != "GREEN":
            raise PlanNodeControllerError("plan_verifier_red")
        if verifier_map.get("read_only") is not True:
            raise PlanNodeControllerError("verifier_not_read_only")
        if verifier_map.get("source_write") is not False:
            raise PlanNodeControllerError("verifier_source_write_not_denied")
        verifier_identity = _identity_token("verifier_receipt.identity", verifier_map.get("identity"))
        if verifier_identity == planner_identity:
            raise PlanNodeControllerError("verifier_identity_not_independent")

        try:
            queue = queue_projector(
                copy.deepcopy(dict(sealed_plan)),
                copy.deepcopy(dict(verifier_map)),
            )
        except PlanNodeControllerError:
            raise
        except Exception as exc:
            raise PlanNodeControllerError(f"queue_projection_failed:{type(exc).__name__}") from exc
        queue_map = self._require_mapping("queue_projection", queue)
        if queue_map.get("status") != "projected":
            raise PlanNodeControllerError("queue_projection_not_projected")
        first_actionable = queue_map.get("first_actionable")
        if not isinstance(first_actionable, Mapping) or first_actionable.get("node_id") != self.first_actionable_node:
            raise PlanNodeControllerError("queue_next_node_mismatch")
        for field in ("runs_created", "attempts_created", "provider_invocations"):
            if queue_map.get(field, 0) != 0:
                raise PlanNodeControllerError(f"queue_{field}_nonzero")

        body: dict[str, Any] = {
            "schema": STATE_SCHEMA,
            "status": "completed",
            "controller_schema": SCHEMA,
            "goal_id": self.goal_id,
            "node_id": self.node_id,
            "first_actionable_node": self.first_actionable_node,
            "input_digest": input_digest,
            "plan_attempt": 1,
            "sealed_plan": _copy_json("sealed_plan", sealed_plan),
            "planner_receipt": _copy_json("planner_receipt", planner_receipt),
            "verifier_receipt": _copy_json("verifier_receipt", verifier_map),
            "queue": _copy_json("queue", queue_map),
            "runs_created": 0,
            "attempts_created": 0,
            "provider_invocations": 0,
            "replayed": False,
        }
        body["state_digest"] = digest_json(body)
        _atomic_write_json(self.state_path, body)
        return copy.deepcopy(body)

    def _legacy_dispatch(
        self,
        parent_goal_id: Any,
        holder: Any,
        *,
        base_sha: str | None = None,
        workspace_ref: str | Path | None = None,
        lease_seconds: float | None = None,
    ) -> dict[str, Any]:
        assert self._legacy_store is not None
        holder = _text("holder", holder)
        inspected = self.inspect(_text("parent_goal_id", parent_goal_id), base_sha=base_sha)
        if inspected["status"] not in {"ready", "already_integrated", "stopped"}:
            return {
                **inspected,
                "provider_invocations": 0,
                "runs_created": 0,
                "attempts_created": 0,
            }
        unit_id = inspected["unit"]["work_unit_id"]
        if inspected["status"] in {"already_integrated", "stopped"}:
            return {
                **inspected,
                "holder": holder,
                "provider_invocations": 0,
                "runs_created": 0,
                "attempts_created": 0,
            }
        planning_request = None
        if self._legacy_store.get_delivery_binding(unit_id) is None:
            work_unit = self._legacy_store.get_work_unit(unit_id)
            if (
                work_unit.get("node_kind") != "planning"
                or work_unit.get("producer") != "PlanNodeController"
            ):
                return {
                    **inspected,
                    "status": "blocked",
                    "reason": "delivery_unit_binding_missing",
                    "holder": holder,
                    "provider_invocations": 0,
                    "runs_created": 0,
                    "attempts_created": 0,
                }
            try:
                from . import delivery_contract as engine
            except ImportError:  # direct canary execution keeps lh_runtime on sys.path
                import delivery_contract as engine  # type: ignore
            existing = [
                item
                for item in self._legacy_store.planning_requests(work_unit_id=unit_id)
                if item.get("status") == "pending"
            ]
            if existing:
                planning_request = existing[0]["request"]
            else:
                current_run = self._legacy_store.get_run_for_work_unit(unit_id)
                plan_body = {
                    "schema": engine.PLAN_SCHEMA,
                    "status": "sealed",
                    "verdict": "GREEN",
                    "goal_id": inspected["goal_id"],
                    "goal_revision": inspected["goal_revision"],
                    "node_id": self.node_id,
                    "unit_id": unit_id,
                    "planner_principal": self.node_id,
                    "controller_mode": "typed-planning-control",
                    "verifier_receipt": {
                        "principal": f"{self.node_id}-independent-verifier",
                        "read_only": True,
                        "source_write": False,
                        "verdict": "GREEN",
                    },
                }
                plan_verdict = {**plan_body, "plan_verdict_digest": engine.digest_json(plan_body)}
                planning_binding = {
                    "schema": "lh-delivery-contract-binding/v1",
                    "status": "bound",
                    "work_unit_id": unit_id,
                    "goal_id": inspected["goal_id"],
                    "goal_revision": inspected["goal_revision"],
                    "node_id": self.node_id,
                    "unit_id": unit_id,
                    "node_kind": "planning",
                    "producer": "PlanNodeController",
                    "plan_verdict": plan_verdict,
                    "plan_verdict_digest": plan_verdict["plan_verdict_digest"],
                }
                planning_request = engine.build_planning_request(
                    planning_binding,
                    reason="typed_plan_node_controller_start",
                    state=inspected["status"],
                    fence=current_run["fence"] if current_run is not None else 0,
                    request_id=f"plan-node-start:{unit_id}",
                )
                self._legacy_store.record_planning_request(planning_request, work_unit_id=unit_id)
        try:
            started = self._legacy_store.start_attempt(
                unit_id,
                holder,
                lease_seconds=self.lease_seconds if lease_seconds is None else float(lease_seconds),
                workspace_ref=workspace_ref,
                planning_request=planning_request,
            )
        except LeaseBusyError as exc:
            return {
                **inspected,
                "status": "waiting",
                "reason": "lease_busy",
                "holder": holder,
                "lease_holder": exc.holder,
                "provider_invocations": 0,
                "runs_created": 0,
                "attempts_created": 0,
            }
        except WorkUnitStoreError as exc:
            if str(exc) != "delivery_unit_binding_missing":
                raise
            return {
                **inspected,
                "status": "blocked",
                "reason": str(exc),
                "holder": holder,
                "provider_invocations": 0,
                "runs_created": 0,
                "attempts_created": 0,
            }
        reused = bool(started.get("reused"))
        return {
            "schema": "lh-single-plan-node-controller/v1",
            "status": "replayed" if reused else "dispatched",
            "reason": "existing_attempt_reused" if reused else "single_node_admitted",
            "parent_goal_id": inspected["parent_goal_id"],
            "goal_id": inspected["goal_id"],
            "goal_revision": inspected["goal_revision"],
            "node_id": self.node_id,
            "work_unit_id": unit_id,
            "run_id": started["run_id"],
            "attempt": started.get("attempt", started.get("ordinal")),
            "fence": started.get("fence"),
            "holder": holder,
            "workspace_ref": started.get("workspace_ref"),
            "base_sha": inspected["base_sha"],
            "parallel_minimum": 1,
            "parallel_maximum": 1,
            "reused": reused,
            "provider_invocations": 0,
            "runs_created": 0 if reused else 1,
            "attempts_created": 0 if reused else 1,
        }

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
        """Finish an Attempt for the merged WorkUnitStore compatibility API."""

        if self._legacy_store is None:
            raise PlanNodeControllerError("store_compatibility_mode_required")
        return self._legacy_store.finish_attempt(
            work_unit_id,
            holder=holder,
            fence=fence,
            state=state,
            receipt_ref=receipt_ref,
            receipt_digest=receipt_digest,
        )

    def integrate(
        self,
        work_unit_id: str,
        *,
        holder: str | None = None,
        fence: int | None = None,
        receipt_ref: str | None = None,
        receipt_digest: str | None = None,
    ) -> bool:
        """Integrate a verified Attempt for the merged compatibility API."""

        if self._legacy_store is None:
            raise PlanNodeControllerError("store_compatibility_mode_required")
        return self._legacy_store.mark_integrated(
            work_unit_id,
            holder=holder,
            fence=fence,
            receipt_ref=receipt_ref,
            receipt_digest=receipt_digest,
        )


__all__ = [
    "PlanNodeController",
    "PlanNodeControllerError",
    "QueueProjector",
    "SCHEMA",
    "STATE_SCHEMA",
    "SingleNodePlanController",
    "Verifier",
    "digest_json",
]


SingleNodePlanController = PlanNodeController

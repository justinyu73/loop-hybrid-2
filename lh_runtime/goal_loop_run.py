#!/usr/bin/env python3
"""Autonomous driver runner: wire a real coding-agent executor into the driver.

This is the opt-in production entry for full-auto. It is model-agnostic — the
executor is chosen by name from an explicit adapter registry (any CLI preset in
cli_agent_executor), never hardcoded — and gated: dry-run is the default and
prints the resolved plan without invoking any provider; only ``--execute``
constructs the real executor and runs the loop. A mutation executor additionally
requires one controller-issued preventive fence descriptor; the default backend
selection is disabled. Repository actions follow the approved goal envelope; a
human owns direction changes and terminal product acceptance, not every node.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import capability_resolver as cr
import provider_input_binding as provider_inputs
import cli_agent_executor as executors
import dispatch_envelope as dispatches
import execution_fence as execution_fences
import fence_command_runner as delivery_runners
import external_action_port as eap
import external_verdict as ev
import grill_loop
import project_binding
import regression_watch as regression_watch_module
import scheduled_checks as scheduled_checks_module
import lamp_actuator
import status_snapshot
import instance_config
import turning_point as tp
import verifier_normalizer
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker, ModelRunner, TurningPointRunner
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from run_store import RunStore
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

# No executor, judge, or evaluator is built in: every one is a declaration
# (cli_agent_executor.validate_executor_declarations) or a caller's factory.
EXECUTION_HOST_SCHEMA = "lh-execution-host-binding/v1"
BOOTSTRAP_AUTHORITY_SCHEMA = "lh-bootstrap-authority/v1"
EXECUTION_HOSTS = {"headless_cli"}
TRUSTED_BOOTSTRAP_ROOT_ENV = "LH_TRUSTED_BOOTSTRAP_ROOT"
EXPECTED_BOOTSTRAP_DECISION_ID = "LH-EXTERNAL-BOOTSTRAP-001"
EXPECTED_BOOTSTRAP_AUTHORITY_REL = (
    "docs/bootstrap-authority.md"
)
EXPECTED_BOOTSTRAP_AUTHORITY_ANCHOR = "lh-external-bootstrap-001"
EXPECTED_BOOTSTRAP_AUTHORITY_REF = (
    f"{EXPECTED_BOOTSTRAP_AUTHORITY_REL}#{EXPECTED_BOOTSTRAP_AUTHORITY_ANCHOR}"
)


def _validate_sha256(field: str, value: Any) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError(f"{field} must be a sha256 digest")
    payload = value.removeprefix("sha256:")
    if len(payload) != 64 or any(character not in "0123456789abcdefABCDEF" for character in payload):
        raise ValueError(f"{field} must be a sha256 digest")
    return "sha256:" + payload.lower()


def build_execution_host_binding(
    execution_host: str | None,
    bootstrap_authority: dict[str, str] | None,
) -> dict[str, Any] | None:
    """Bind host mechanics separately from model/provider resolution."""
    if execution_host is None:
        if bootstrap_authority is not None:
            raise ValueError("bootstrap_authority requires execution_host")
        return None
    if execution_host not in EXECUTION_HOSTS:
        raise ValueError(
            f"unknown execution_host: {execution_host!r}; "
            f"choose one of {sorted(EXECUTION_HOSTS)}"
        )
    expected = {"decision_id", "authority_ref", "authority_digest", "root"}
    if not isinstance(bootstrap_authority, dict) or set(bootstrap_authority) != expected:
        raise ValueError(
            "execution_host requires bootstrap_authority with exactly "
            "decision_id, authority_ref, authority_digest, and root"
        )
    normalized: dict[str, str] = {}
    for field in ("decision_id", "authority_ref", "root"):
        value = bootstrap_authority.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"bootstrap_authority.{field} must be a non-empty string")
        normalized[field] = value.strip()
    normalized["authority_digest"] = _validate_sha256(
        "bootstrap_authority.authority_digest",
        bootstrap_authority.get("authority_digest"),
    )
    supplied_root = Path(normalized["root"])
    if not supplied_root.is_absolute():
        raise ValueError("bootstrap_authority.root must be an absolute path")
    trusted_root_value = os.environ.get(TRUSTED_BOOTSTRAP_ROOT_ENV)
    if not isinstance(trusted_root_value, str) or not trusted_root_value.strip():
        raise ValueError(
            f"{TRUSTED_BOOTSTRAP_ROOT_ENV} is required for execution_host"
        )
    trusted_root = Path(trusted_root_value.strip())
    if not trusted_root.is_absolute():
        raise ValueError(f"{TRUSTED_BOOTSTRAP_ROOT_ENV} must be an absolute path")
    try:
        resolved_trusted_root = trusted_root.resolve(strict=True)
    except OSError as exc:
        raise ValueError(
            f"{TRUSTED_BOOTSTRAP_ROOT_ENV} is not a readable directory"
        ) from exc
    if trusted_root != resolved_trusted_root or not resolved_trusted_root.is_dir():
        raise ValueError(
            f"{TRUSTED_BOOTSTRAP_ROOT_ENV} must name one canonical directory"
        )
    if supplied_root != resolved_trusted_root:
        raise ValueError(
            "bootstrap_authority.root does not match the operator-owned trust root"
        )
    if normalized["decision_id"] != EXPECTED_BOOTSTRAP_DECISION_ID:
        raise ValueError("bootstrap_authority.decision_id is not canonical")
    if normalized["authority_ref"] != EXPECTED_BOOTSTRAP_AUTHORITY_REF:
        raise ValueError("bootstrap_authority.authority_ref is not canonical")
    authority_path = resolved_trusted_root / EXPECTED_BOOTSTRAP_AUTHORITY_REL
    try:
        authority_bytes = authority_path.read_bytes()
    except OSError as exc:
        raise ValueError("canonical bootstrap authority is not readable") from exc
    anchor = f'<a id="{EXPECTED_BOOTSTRAP_AUTHORITY_ANCHOR}"></a>'.encode()
    if anchor not in authority_bytes:
        raise ValueError("canonical bootstrap authority anchor is missing")
    actual_digest = "sha256:" + hashlib.sha256(authority_bytes).hexdigest()
    if normalized["authority_digest"] != actual_digest:
        raise ValueError(
            "bootstrap_authority.authority_digest does not match canonical bytes"
        )
    normalized["root"] = str(resolved_trusted_root)
    return {
        "schema": EXECUTION_HOST_SCHEMA,
        "host_id": execution_host,
        "adapter": "headless_cli",
        "bootstrap_authority": {
            "schema": BOOTSTRAP_AUTHORITY_SCHEMA,
            **normalized,
        },
    }


def resolve_executor(
    name: str,
    *,
    execute: bool,
    declarations: dict[str, dict[str, Any]] | None = None,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    provider_binding: dict[str, str] | None = None,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
) -> ModelRunner | None:
    """Refuse an undeclared executor (even in dry-run). Return the real model
    only when ``execute`` is true; dry-run returns None so nothing runs."""
    declared = dict(declarations or {})
    factories = dict(factory_overrides or {})
    if name not in factories and name not in declared:
        raise ValueError(f"unknown executor: {name!r}; declared: {sorted({*declared, *factories})}")
    if provider_binding is not None:
        executors._validate_provider_binding(provider_binding, agent=name)
    if not execute:
        return None
    if name in factories:
        kwargs: dict[str, Any] = {"timeout_seconds": timeout_seconds}
        if provider_binding is not None:
            kwargs["provider_binding"] = provider_binding
        return factories[name](**kwargs)
    return executors.make_declared_agent(
        name,
        declared,
        provider_binding=provider_binding,
        timeout_seconds=timeout_seconds,
        execution_fence_port=execution_fence_port,
    )


def _invoke_bound_model(
    model: ModelRunner,
    binding: dict[str, Any],
    workspace: Path,
    capsule: dict[str, Any],
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    try:
        provider_inputs.require_admission()
        capsule = {
            **capsule,
            "provider_binding_context": {
                "goal_revision": provider_inputs.digest_json(
                    dict(capsule.get("goal") or {})),
                "run_id": str(capsule.get("run_id") or ""),
                "attempt": int(capsule.get("attempt") or 0),
                "adapter_id": str(binding.get("binding_id") or binding.get("runner") or ""),
                "adapter_version": str(binding.get("schema") or "v1"),
                "capability_digest": str(binding.get("endpoint_ref_digest") or ""),
                "authority_digest": str(binding.get("authority_digest") or ""),
            },
        }
        provider = model(workspace, capsule)
        if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
            raise ValueError("model runner must return a dict with a bounded summary")
        status = "completed"
    except execution_fences.ExecutionFenceUnavailable as exc:
        provider = {
            "summary": "execution fence unavailable; model not invoked",
            "failure": f"{execution_fences.ERROR_CODE}: {exc.reason}",
            "provider_invocations": 0,
            "execution_fence": {
                "status": "unavailable",
                "error_code": execution_fences.ERROR_CODE,
                "reason": exc.reason,
                "mutation_dispatch": "disabled",
            },
            "routing": {
                "route": "human_required",
                "reason": execution_fences.ERROR_CODE,
            },
        }
        status = "failed"
    except provider_inputs.ProviderInputRejected as exc:
        provider = {
            "summary": "provider input binding rejected; model not invoked",
            "failure": str(exc),
            "provider_invocations": 0,
            "routing": {"route": "human_required", "reason": provider_inputs.REJECTED},
        }
        status = "failed"
    except Exception as exc:
        provider = {
            "summary": "resolved executor invocation failed",
            "failure": f"{type(exc).__name__}: {exc}",
        }
        status = "failed"
    provider["binding_receipt"] = cr.finalize_binding(
        binding,
        capsule,
        provider,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        exit_status=status,
    )
    return provider


class CapabilityRoutingSession:
    """Resolve model resources at invocation time while keeping LH as owner."""

    def __init__(
        self,
        graph: dict[str, Any],
        *,
        timeout_seconds: float,
        run_store_root: str | Path,
        execution_host_binding: dict[str, Any] | None = None,
        factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
        execution_fence_port: execution_fences.ExecutionFencePort | None = None,
        executor_declarations: dict[str, dict[str, Any]] | None = None,
    ):
        self.timeout_seconds = timeout_seconds
        self.run_store_root = Path(run_store_root)
        self.factories = dict(factory_overrides or {})
        self.executor_declarations = dict(executor_declarations or {})
        self.execution_host_binding = execution_host_binding
        self.execution_fence_port = (
            execution_fence_port
            if execution_fence_port is not None
            else execution_fences.DisabledExecutionFencePort()
        )
        self.graph = cr.validate_graph(graph)
        self.selected_nodes: dict[str, dict[str, Any]] = {}
        self.prior_attempts_by_run: dict[str, list[dict[str, Any]]] = {}
        for resource in self.graph["registry"]["resources"]:
            if resource["executor_kind"] != "model":
                continue
            runner = resource["runner"]
            if runner not in self.executor_declarations and runner not in self.factories:
                raise ValueError(f"no declared executor for resource runner {runner!r}")
            if runner in self.factories and (
                resource.get("model") is not None
                or resource.get("provider_binding") is not None
            ):
                raise ValueError("factory override cannot claim model or provider binding actuation")
            provider_binding = resource.get("provider_binding")
            if isinstance(provider_binding, dict):
                executors._validate_provider_binding(
                    provider_binding,
                    agent=runner,
                )

    @property
    def judge_timeout_seconds(self) -> float:
        limits = [
            float(node["budget"]["max_wall_seconds"])
            for node in self.graph["nodes"]
            if node["operation"] == "evaluate_transition"
        ]
        return min([self.timeout_seconds, *limits])

    def _validate_produce_resource(self, resource: dict[str, Any]) -> None:
        if resource["runner"] in self.factories:
            return
        actual = (
            resource["permission_ceiling"],
            resource["network_access"],
            resource["data_boundary"],
        )
        if actual != ("workspace_write", "external", "external"):
            raise ValueError(
                "produce_change CLI resources must declare the actual "
                "workspace_write/external/external execution envelope"
            )

    def _validate_evaluation_resource(self, resource: dict[str, Any]) -> None:
        if resource["runner"] not in self.executor_declarations:
            raise ValueError(
                "evaluate_transition requires a declared executor; declared: "
                f"{sorted(self.executor_declarations)}, got {resource['runner']!r}"
            )
        if resource.get("provider_binding") is not None:
            raise ValueError("evaluate_transition does not support a provider binding")
        if resource.get("model") is None:
            raise ValueError("evaluate_transition requires an explicit model binding")
        actual = (
            resource["permission_ceiling"],
            resource["network_access"],
            resource["data_boundary"],
        )
        if actual != ("read_only", "external", "external"):
            raise ValueError(
                "evaluate_transition resources must declare the actual "
                "read_only/external/external execution envelope"
            )

    def _model(self, resource: dict[str, Any], node: dict[str, Any]) -> ModelRunner:
        runner = resource["runner"]
        timeout_seconds = min(
            self.timeout_seconds,
            float(node["budget"]["max_wall_seconds"]),
        )
        if runner in self.factories:
            return self.factories[runner](timeout_seconds=timeout_seconds)
        if self.execution_host_binding is None:
            raise ValueError(
                "capability production model requires execution_host='headless_cli'"
            )
        return executors.make_declared_agent(
            runner,
            self.executor_declarations,
            model=resource.get("model"),
            provider_binding=resource.get("provider_binding"),
            timeout_seconds=timeout_seconds,
            execution_fence_port=self.execution_fence_port,
        )

    def _bind_host(self, binding: dict[str, Any]) -> dict[str, Any]:
        bound = dict(binding)
        if self.execution_host_binding is not None:
            bound["execution_host"] = dict(self.execution_host_binding)
        return bound

    def preview(self) -> list[dict[str, Any]]:
        resolved = cr.resolve_operation(self.graph, "produce_change")
        if resolved is None:
            return []
        self._validate_produce_resource(resolved["resource"])
        return [self._bind_host(resolved["binding"])]

    def execute(self, workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        run_id = str(capsule.get("run_id"))
        prior_attempts = self.prior_attempts_by_run.setdefault(run_id, [])
        try:
            resolved = cr.resolve_operation(
                self.graph,
                "produce_change",
                selected_nodes=self.selected_nodes,
                prior_attempts=prior_attempts,
                attempt_ordinal=(
                    int(capsule["attempt"])
                    if isinstance(capsule.get("attempt"), int)
                    else None
                ),
            )
        except cr.ResolutionError as exc:
            node = next(
                node for node in self.graph["nodes"]
                if node["operation"] == "produce_change"
            )
            return {
                "summary": "capability resolver found no eligible execution resource",
                "failure": f"{type(exc).__name__}: {exc}",
                "routing": {
                    "route": exc.route,
                    "operation": "produce_change",
                    "node_id": node["node_id"],
                    "authority_ref": node["inputs"]["authority_ref"],
                    "authority_digest": node["inputs"]["authority_digest"],
                    "registry_revision": self.graph["registry"]["revision"],
                    "resolver_policy_revision": self.graph["policy"]["revision"],
                },
            }
        if resolved is None:
            raise cr.ResolutionError("produce_change node is missing")
        self._validate_produce_resource(resolved["resource"])
        self.selected_nodes[resolved["node"]["node_id"]] = resolved["resource"]
        binding = self._bind_host(resolved["binding"])
        invocation_capsule = dict(capsule)
        if self.execution_host_binding is not None:
            invocation_capsule["bootstrap_authority"] = dict(
                self.execution_host_binding["bootstrap_authority"]
            )
        selected_model = self._model(resolved["resource"], resolved["node"])
        if execution_fences.model_requires_fence(selected_model):
            adapter_id, adapter_version = execution_fences.model_fence_identity(
                selected_model
            )
            try:
                fence_binding = execution_fences.build_attempt_binding(
                    goal=(
                        capsule["goal"]
                        if isinstance(capsule.get("goal"), dict)
                        else {}
                    ),
                    run_id=run_id,
                    attempt=int(capsule.get("attempt") or 0),
                    attempt_fence=int(capsule.get("fence") or 0),
                    base_revision=str(capsule.get("base_revision") or ""),
                    clone_root=workspace,
                    verifier_argv=(
                        capsule["verification_commands"]
                        if isinstance(
                            capsule.get("verification_commands"),
                            list,
                        )
                        else []
                    ),
                    adapter_id=adapter_id,
                    adapter_version=adapter_version,
                    timeout_seconds=float(
                        capsule.get("timeout_seconds") or self.timeout_seconds
                    ),
                )
                descriptor = self.execution_fence_port.prepare(fence_binding)
                invocation_capsule["execution_fence"] = descriptor
                fence_projection = (
                    self.execution_fence_port.receipt_projection(descriptor)
                )
            except execution_fences.ExecutionFenceUnavailable as exc:
                return {
                    "summary": "execution fence unavailable; model not invoked",
                    "failure": f"{execution_fences.ERROR_CODE}: {exc.reason}",
                    "provider_invocations": 0,
                    "execution_fence": {
                        "status": "unavailable",
                        "error_code": execution_fences.ERROR_CODE,
                        "reason": exc.reason,
                        "mutation_dispatch": "disabled",
                    },
                    "routing": {
                        "route": "human_required",
                        "reason": execution_fences.ERROR_CODE,
                    },
                }
        else:
            fence_projection = None
        provider = _invoke_bound_model(
            selected_model,
            binding,
            workspace,
            invocation_capsule,
        )
        if fence_projection is not None:
            provider["execution_fence"] = fence_projection
        prior_attempts.append(dict(binding))
        return provider

    def _reference_binding(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        evaluation = next(
            node for node in self.graph["nodes"]
            if node["operation"] == "evaluate_transition"
        )
        run_id = snapshot.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise cr.ResolutionError(
                "evaluate_transition requires a run_id for persisted Attempt evidence",
                route=evaluation["fallback_policy"],
            )
        store = RunStore(self.run_store_root)
        latest = store.latest_receipt(run_id)
        if not isinstance(latest, dict):
            raise cr.ResolutionError(
                "evaluate_transition requires an actual persisted Attempt receipt",
                route=evaluation["fallback_policy"],
            )
        receipt_ref = latest.get("receipt_ref")
        receipt_digest = latest.get("receipt_digest")
        persisted_ordinal = latest.get("ordinal")
        if (
            not isinstance(persisted_ordinal, int)
            or isinstance(persisted_ordinal, bool)
            or persisted_ordinal < 1
            or not isinstance(receipt_ref, str)
            or not receipt_ref
            or not isinstance(receipt_digest, str)
            or not receipt_digest.startswith("sha256:")
        ):
            raise cr.ResolutionError(
                "persisted Attempt receipt metadata is incomplete",
                route=evaluation["fallback_policy"],
            )
        store_root = store.root.resolve()
        receipt_path = (store.root / receipt_ref).resolve()
        if not receipt_path.is_relative_to(store_root):
            raise cr.ResolutionError(
                "persisted Attempt receipt ref escapes RunStore",
                route=evaluation["fallback_policy"],
            )
        try:
            receipt_raw = receipt_path.read_bytes()
        except OSError as exc:
            raise cr.ResolutionError(
                "persisted Attempt receipt is unavailable",
                route=evaluation["fallback_policy"],
            ) from exc
        actual_digest = "sha256:" + hashlib.sha256(receipt_raw).hexdigest()
        if actual_digest != receipt_digest:
            raise cr.ResolutionError(
                "persisted Attempt receipt digest mismatch",
                route=evaluation["fallback_policy"],
            )
        try:
            receipt = json.loads(receipt_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise cr.ResolutionError(
                "persisted Attempt receipt is not valid JSON",
                route=evaluation["fallback_policy"],
            ) from exc
        attempt = receipt.get("attempt") if isinstance(receipt, dict) else None
        binding = receipt.get("binding") if isinstance(receipt, dict) else None
        if (
            not isinstance(receipt, dict)
            or receipt.get("schema") != "loop-hybrid-attempt-receipt/v1"
            or receipt.get("run_id") != run_id
            or not isinstance(attempt, int)
            or isinstance(attempt, bool)
            or attempt < 1
            or attempt != persisted_ordinal
            or not isinstance(binding, dict)
            or binding.get("schema") != cr.BINDING_SCHEMA
            or binding.get("operation") != "produce_change"
            or binding.get("attempt_id") != f"{run_id}:{attempt}"
            or not isinstance(binding.get("binding_id"), str)
            or not isinstance(binding.get("model"), str)
            or not isinstance(binding.get("model_family"), str)
            or binding.get("model_family") != binding.get("model")
            or binding.get("context_isolation") not in {"fresh_process", "shared"}
        ):
            raise cr.ResolutionError(
                "persisted Attempt receipt binding contract is invalid",
                route=evaluation["fallback_policy"],
            )
        return binding

    def resolve_evaluation(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        change_node = next(
            node for node in self.graph["nodes"]
            if node["operation"] == "produce_change"
        )
        reference_binding = self._reference_binding(snapshot)
        selected = {change_node["node_id"]: reference_binding}
        resolved = cr.resolve_operation(
            self.graph,
            "evaluate_transition",
            selected_nodes=selected,
        )
        if resolved is None:
            raise cr.ResolutionError("evaluate_transition node is missing")
        self._validate_evaluation_resource(resolved["resource"])
        resolved["independence_reference"] = {
            "attempt_id": reference_binding.get("attempt_id"),
            "binding_id": reference_binding.get("binding_id"),
            "model": reference_binding.get("model"),
        }
        return resolved

    def write_evaluation_evidence(
        self,
        invocation_id: str,
        *,
        stdout: str,
        stderr: str,
        receipt: dict[str, Any],
    ) -> dict[str, str]:
        root = self.run_store_root / "routing-evidence" / invocation_id
        root.mkdir(parents=True, exist_ok=False)

        def write(name: str, value: str) -> dict[str, str]:
            path = root / name
            content = value[:65536]
            path.write_text(content, encoding="utf-8", newline="")
            return {
                "ref": path.relative_to(self.run_store_root).as_posix(),
                "digest": "sha256:" + hashlib.sha256(content.encode()).hexdigest(),
            }

        receipt["evidence_refs"] = [
            write("stdout.txt", stdout),
            write("stderr.txt", stderr),
        ]
        receipt_path = root / "receipt.json"
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        return {
            "ref": receipt_path.relative_to(self.run_store_root).as_posix(),
            "digest": cr.digest_json(receipt),
        }

    def record_evaluation_resolution_failure(
        self,
        *,
        invocation_id: str,
        kind: str,
        snapshot: dict[str, Any],
        exc: Exception,
    ) -> dict[str, str]:
        node = next(
            node for node in self.graph["nodes"]
            if node["operation"] == "evaluate_transition"
        )
        route = exc.route if isinstance(exc, cr.ResolutionError) else node["fallback_policy"]
        record = {
            "schema": "lh-routing-resolution/v1",
            "invocation_id": invocation_id,
            "invocation_kind": kind,
            "run_id": snapshot.get("run_id"),
            "node_id": node["node_id"],
            "operation": node["operation"],
            "route": route,
            "reason": f"{type(exc).__name__}: {exc}",
            "authority_ref": node["inputs"]["authority_ref"],
            "authority_digest": node["inputs"]["authority_digest"],
            "registry_revision": self.graph["registry"]["revision"],
            "resolver_policy_revision": self.graph["policy"]["revision"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return self.write_evaluation_evidence(
            invocation_id,
            stdout="",
            stderr=record["reason"],
            receipt=record,
        )


def _make_capability_evaluator(
    routing: CapabilityRoutingSession,
    *,
    kind: str,
    prompt_builder: Callable[[dict[str, Any]], str],
    parser: Callable[[str], dict[str, Any]],
    output_schema: dict[str, Any] | None = None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def evaluate(snapshot: dict[str, Any]) -> dict[str, Any]:
        invocation_id = "evaluation-" + uuid.uuid4().hex
        try:
            resolved = routing.resolve_evaluation(snapshot)
        except Exception as exc:
            routing.record_evaluation_resolution_failure(
                invocation_id=invocation_id,
                kind=kind,
                snapshot=snapshot,
                exc=exc,
            )
            raise
        resource = resolved["resource"]
        started_at = datetime.now(timezone.utc)
        prompt = ""
        argv: list[str] = []
        proc: subprocess.CompletedProcess[str] | None = None
        parsed: dict[str, Any] | None = None
        error: Exception | None = None
        try:
            prompt = prompt_builder(snapshot)
            argv = executors.declared_command(
                routing.executor_declarations,
                resource["runner"],
                prompt,
                resource["model"],
            )
            env = dict(os.environ)
            routing.run_store_root.mkdir(parents=True, exist_ok=True)
            if output_schema is not None:
                schema_path = routing.run_store_root / "evaluation-schemas" / f"{kind}.json"
                schema_path.parent.mkdir(parents=True, exist_ok=True)
                schema_path.write_text(json.dumps(output_schema, sort_keys=True), encoding="utf-8")
                env[executors.OUTPUT_SCHEMA_ENV] = str(schema_path)
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=routing.judge_timeout_seconds,
                env=env,
                cwd=routing.run_store_root,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"evaluation {kind} exited {proc.returncode}: {proc.stderr.strip()[:400]}"
                )
            parsed = parser(
                _evaluation_payload(
                    resource["runner"],
                    proc.stdout,
                    require_structured=output_schema is not None,
                )
            )
        except Exception as exc:
            error = exc
        stdout = proc.stdout if proc is not None else ""
        stderr_parts = [proc.stderr] if proc is not None and proc.stderr else []
        if error is not None:
            stderr_parts.append(f"{type(error).__name__}: {error}")
        stderr = "\n".join(stderr_parts)
        provider = {
            "summary": f"capability evaluation {kind} "
            + ("completed" if error is None else "failed"),
            "result": parsed,
            "usage": {
                "state": "unknown",
                "model": resource["model_identity"],
                "reason": "evaluation CLI usage unavailable",
            },
        }
        if error is not None:
            provider["failure"] = f"{type(error).__name__}: {error}"
        receipt = cr.finalize_binding(
            resolved["binding"],
            {
                "run_id": snapshot.get("run_id", "unbound"),
                "attempt": snapshot.get("next_attempt", snapshot.get("attempt", "evaluation")),
                "goal": snapshot,
                "base_revision": None,
            },
            provider,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            exit_status="completed" if error is None else "failed",
        )
        receipt["input_digest"] = cr.digest_json(snapshot)
        receipt["prompt_or_command_digest"] = cr.digest_json(argv)
        receipt["attempt_id"] = (
            f"{snapshot.get('run_id', 'unbound')}:"
            f"{snapshot.get('next_attempt', snapshot.get('attempt', 'evaluation'))}:"
            f"{invocation_id}"
        )
        receipt["invocation_kind"] = kind
        receipt["independence_reference"] = resolved["independence_reference"]
        routing.write_evaluation_evidence(
            invocation_id,
            stdout=stdout,
            stderr=stderr,
            receipt=receipt,
        )
        if error is not None:
            raise error
        assert parsed is not None
        return parsed

    return evaluate


def _evaluation_payload(
    runner: str,
    stdout: str,
    *,
    require_structured: bool = False,
) -> str:
    """Return the payload a declared evaluator emitted on its last stdout line."""
    del runner
    if not require_structured:
        return stdout
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise ValueError("schema-bound evaluation output is empty")
    try:
        value = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise ValueError("schema-bound evaluation output is not JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("schema-bound evaluation output is not an object")
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _compatibility_model(
    model: ModelRunner,
    *,
    executor: str,
    provider_binding: dict[str, str] | None,
    authority: dict[str, str] | None,
) -> ModelRunner:
    authority = authority or {}
    binding = cr.compatibility_binding(
        executor,
        provider_binding=provider_binding,
        authority_ref=authority.get("authority_ref", "cli:inline"),
        authority_digest=authority.get("authority_digest"),
    )

    def invoke(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        return _invoke_bound_model(model, binding, workspace, capsule)

    if execution_fences.model_requires_fence(model):
        adapter_id, adapter_version = execution_fences.model_fence_identity(model)
        execution_fences.mark_mutation_adapter(
            invoke,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
        )
    return invoke


def build_worker(
    *,
    goal_store_root: str | Path,
    run_store_root: str | Path,
    workspace_root: str | Path,
    campaign: dict[str, Any],
    source_repo: str | Path,
    base_revision: str,
    executor_timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    grill_runner: grill_loop.GrillRunner | None = None,
    action_ledger: eap.ActionLedger | None = None,
    external_adapter: eap.ExternalAdapter | None = None,
    knowledge_store_root: str | Path | None = None,
    knowledge_repo_roots: tuple[str | Path, ...] = (),
    dispatch_envelope: dict[str, Any] | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
    native_execution_binding=None,
) -> GoalLoopWorker:
    runs = RunStore(Path(run_store_root))
    if native_execution_binding is not None:
        native_execution_binding.attach_native_store(runs)
        runs.command_runner = native_execution_binding.command
    elif (execution_fence_port is not None
          and not isinstance(execution_fence_port, execution_fences.DisabledExecutionFencePort)
          and runs.command_runner is None):
        # Compatibility runs execute delivery checks and the independent
        # verifier through the same fence; a caller-supplied runner is kept.
        runs.command_runner = delivery_runners.FenceCommandRunner(execution_fence_port)
    campaign_id = campaign["campaign_id"]
    knowledge_store = KnowledgeStore(Path(knowledge_store_root)) if knowledge_store_root is not None else None
    return GoalLoopWorker(
        goal_store=GoalStore(Path(goal_store_root)),
        run_store=runs,
        controller=LoopController(
            runs,
            Path(workspace_root),
            timeout_seconds=executor_timeout_seconds,
            dispatch=(
                dispatches.receipt_binding(dispatch_envelope)
                if dispatch_envelope is not None
                else None
            ),
            execution_fence_port=execution_fence_port,
        ),
        compilers={campaign_id: CampaignCompiler(campaign)},
        execution_context={campaign_id: {"source_repo": Path(source_repo), "base_revision": base_revision}},
        grill_runner=grill_runner,
        action_ledger=action_ledger,
        external_adapter=external_adapter,
        knowledge_store=knowledge_store,
        knowledge_repo_roots=tuple(Path(item) for item in knowledge_repo_roots),
        recovery_binding=native_execution_binding,
    )


def run(
    *,
    executor: str | None = None,
    execute: bool,
    goal_store_root: str | Path,
    run_store_root: str | Path,
    workspace_root: str | Path,
    campaign: dict[str, Any],
    source_repo: str | Path,
    base_revision: str,
    holder: str = "driver",
    pause_flag: str | Path | None = None,
    max_cycles: int | None = None,
    max_runs: int | None = None,
    max_runtime_seconds: float | None = None,
    budget_ceiling_tokens: int | None = None,
    budget_scope: str | None = None,
    idle_limit: int = 3,
    executor_timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    status_snapshot_out: str | Path | None = None,
    verdict_store: ev.VerdictStore | None = None,
    conclusion_source: ev.ConclusionSource | None = None,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
    driver_fn: Callable[..., dict[str, Any]] = run_driver,
    sleep_fn: Callable[[float], None] | None = None,
    judge_executor: str | None = None,
    judge_model: str | None = None,
    executor_binding: dict[str, str] | None = None,
    executor_declarations: dict[str, dict[str, Any]] | None = None,
    execution_graph: dict[str, Any] | None = None,
    execution_host: str | None = None,
    bootstrap_authority: dict[str, str] | None = None,
    compatibility_authority: dict[str, str] | None = None,
    turning_point: TurningPointRunner | None = None,
    quota_reader: Callable[[], dict[str, Any] | None] | None = None,
    daily_soft_cap_usd: float | None = 2.0,
    daily_hard_cap_usd: float | None = 5.0,
    pricing: dict[str, dict[str, float]] | None = None,
    knowledge_store_root: str | Path | None = None,
    knowledge_repo_roots: tuple[str | Path, ...] = (),
    dispatch_envelope: dict[str, Any] | None = None,
    assignment_binding: dict[str, Any] | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
    planner_recovery: dict[str, Any] | None = None,
    regression_watch: dict[str, Any] | None = None,
    scheduled_checks: dict[str, Any] | None = None,
    lamp_actuation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Opt-in per contract: validate before any work so a bad block fails fast.
    watch_config = regression_watch_module.validate_config(regression_watch) if regression_watch is not None else None
    checks_config = scheduled_checks_module.validate_config(scheduled_checks) if scheduled_checks is not None else None
    actuation_config = lamp_actuator.validate_config(lamp_actuation) if lamp_actuation is not None else None
    native_binding = None
    if execute and planner_recovery is not None:
        from lh_runtime.runner_adapter import resolve_native_run_execution_binding
        native_binding = resolve_native_run_execution_binding(
            planner_recovery, dispatch_envelope, campaign=campaign,
            source_repo=source_repo, base_revision=base_revision)
        if execution_fence_port is not None and execution_fence_port is not native_binding.port:
            raise ValueError("native_runtime_fence_override_forbidden")
        execution_fence_port = native_binding.port
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else (
            execution_fences.configured_execution_fence()
            if execute
            else execution_fences.DisabledExecutionFencePort()
        )
    )
    if (verdict_store is None) != (conclusion_source is None):
        raise ValueError("verdict_store and conclusion_source must be supplied together")
    execution_host_binding = build_execution_host_binding(
        execution_host,
        bootstrap_authority,
    )
    declarations = executors.validate_executor_declarations(executor_declarations)
    routing: CapabilityRoutingSession | None = None
    preview_bindings: list[dict[str, Any]] = []
    has_evaluate_node = False
    if execution_graph is not None:
        legacy_fields = {
            "executor": executor,
            "judge_executor": judge_executor,
            "judge_model": judge_model,
            "executor_binding": executor_binding,
            "turning_point": turning_point,
        }
        conflicts = sorted(key for key, value in legacy_fields.items() if value is not None)
        if conflicts:
            raise ValueError(f"execution_graph cannot be combined with legacy routing fields: {conflicts}")
        routing = CapabilityRoutingSession(
            execution_graph,
            timeout_seconds=executor_timeout_seconds,
            run_store_root=run_store_root,
            execution_host_binding=execution_host_binding,
            factory_overrides=factory_overrides,
            execution_fence_port=fence_port,
            executor_declarations=declarations,
        )
        preview_bindings = routing.preview()
        if (
            execute
            and execution_host_binding is None
            and any(
                binding["runner"] not in (factory_overrides or {})
                for binding in preview_bindings
            )
        ):
            raise ValueError(
                "capability execution with a production model requires "
                "execution_host='headless_cli'"
            )
        has_evaluate_node = any(
            node["operation"] == "evaluate_transition" for node in routing.graph["nodes"]
        )
        resolved_executor = "capability-resolved"
        resolved_judge = None
        routing_mode = "capability"
    else:
        if execution_host_binding is not None:
            raise ValueError(
                "execution_host is supported only by capability routing; "
                "legacy executors are compatibility-only"
            )
        if not isinstance(executor, str) or not executor:
            raise ValueError("executor is required when execution_graph is absent")
        if judge_executor is not None and turning_point is not None:
            raise ValueError("judge_executor and turning_point are mutually exclusive")
        if judge_executor is not None and judge_executor not in declarations:
            raise ValueError(f"unknown judge_executor: {judge_executor!r}; declared: {sorted(declarations)}")
        resolved_executor = executor
        resolved_judge = judge_executor
        routing_mode = "compatibility"
    plan = {
        "executor": resolved_executor,
        "executors": sorted(declarations),
        "execute": execute,
        "campaign_id": campaign["campaign_id"],
        "goal_store": str(goal_store_root),
        "run_store": str(run_store_root),
        "knowledge_store": str(knowledge_store_root) if knowledge_store_root is not None else None,
        "knowledge_repos": [str(item) for item in knowledge_repo_roots],
        "judge_executor": resolved_judge,
        "judge_model": judge_model if routing is None else None,
        "dispatch": (
            dispatches.receipt_binding(dispatch_envelope)
            if dispatch_envelope is not None
            else None
        ),
        "assignment_binding": assignment_binding,
        "executor_binding": (
            {key: executor_binding[key] for key in ("runner", "model") if key in executor_binding}
            if isinstance(executor_binding, dict)
            else None
        ),
        "execution_host": execution_host_binding,
        "gates": {
            "pause_flag": str(pause_flag) if pause_flag is not None else None,
            "max_cycles": max_cycles,
            "max_runs": max_runs,
            "max_runtime_seconds": max_runtime_seconds,
            "budget_ceiling_tokens": budget_ceiling_tokens,
            "budget_scope": budget_scope,
            "idle_limit": idle_limit,
            "executor_timeout_seconds": executor_timeout_seconds,
            "status_snapshot_out": str(status_snapshot_out) if status_snapshot_out is not None else None,
            "external_verdict_poll": verdict_store is not None,
            "daily_soft_cap_usd": daily_soft_cap_usd,
            "daily_hard_cap_usd": daily_hard_cap_usd,
            "quota_gate": quota_reader is not None,
        },
        "boundary": (
            "mutation dispatch requires one ExecutionFencePort descriptor with "
            "both proof tracks on the same Attempt; the default backend is "
            "disabled; publish/release and terminal product acceptance remain "
            "outside the driver"
        ),
        "routing": {
            "mode": routing_mode,
            "bindings": preview_bindings,
            "pending_operations": (
                ["evaluate_transition"]
                if routing is not None and has_evaluate_node
                else []
            ),
        },
    }
    if routing is not None:
        model = routing.execute if execute else None
    else:
        raw_model = resolve_executor(
            resolved_executor,
            execute=execute,
            declarations=declarations,
            timeout_seconds=executor_timeout_seconds,
            provider_binding=executor_binding,
            factory_overrides=factory_overrides,
            execution_fence_port=fence_port,
        )
        model = (
            _compatibility_model(
                raw_model,
                executor=resolved_executor,
                provider_binding=executor_binding,
                authority=compatibility_authority,
            )
            if raw_model is not None
            else None
        )
        plan["routing"]["bindings"] = [
            cr.compatibility_binding(
                resolved_executor,
                provider_binding=executor_binding,
                authority_ref=(
                    compatibility_authority.get("authority_ref", "cli:inline")
                    if isinstance(compatibility_authority, dict)
                    else "cli:inline"
                ),
                authority_digest=(
                    compatibility_authority.get("authority_digest")
                    if isinstance(compatibility_authority, dict)
                    else None
                ),
            )
        ]
    if model is None:
        return {"mode": "dry_run", "invoked": False, "plan": plan}
    grill_runner: grill_loop.GrillRunner | None = None
    if routing is not None and has_evaluate_node:
        # Capability evaluation is deliberately post-Attempt.  Turning-point
        # selection happens before an actual produce binding exists and remains
        # deterministic; the grill resolves against the persisted
        # binding they are reviewing.
        grill_runner = _make_capability_evaluator(
            routing,
            kind="grill",
            prompt_builder=grill_loop.build_judge_prompt,
            parser=tp.parse_decision,
            output_schema={
                "type": "object",
                "properties": {
                    "decision": {
                        "type": "string",
                        "pattern": "^(select:.+|parent_done|human_required)$",
                    },
                },
                "required": ["decision"],
                "additionalProperties": False,
            },
        )
    elif judge_executor is not None:
        turning_point = tp.make_cli_judge(
            lambda prompt: executors.declared_command(declarations, judge_executor, prompt, judge_model),
            name=judge_executor,
        )
        # W6a: the same judge CLI layering carries the challenger grill before
        # a run's last allowed attempt. Absent judge = grill stays off (the
        # original max_attempts behavior), so a missing models.judge config
        # never blocks the loop.
        grill_runner = grill_loop.make_cli_judge(
            lambda prompt: executors.declared_command(declarations, judge_executor, prompt, judge_model),
            name=judge_executor,
        )
    worker = build_worker(
        goal_store_root=goal_store_root,
        run_store_root=run_store_root,
        workspace_root=workspace_root,
        campaign=campaign,
        source_repo=source_repo,
        base_revision=base_revision,
        executor_timeout_seconds=executor_timeout_seconds,
        grill_runner=grill_runner,
        knowledge_store_root=knowledge_store_root,
        knowledge_repo_roots=knowledge_repo_roots,
        dispatch_envelope=dispatch_envelope,
        execution_fence_port=fence_port,
        native_execution_binding=native_binding,
    )
    plan["delivery_command_runner"] = (
        {"status": "native"}
        if native_binding is not None
        else delivery_runners.runner_status(worker.run_store.command_runner, fence_port)
    )
    startup_external_resumed = []
    if verdict_store is not None and conclusion_source is not None:
        # Poll before entering the driver: a host that cannot acquire the
        # driver holder may return ``not_holder`` without performing a tick.
        # Restart durability therefore cannot depend on run_driver reaching its
        # first worker.tick.
        startup_external_resumed = worker.controller.resume_external(
            verdict_store=verdict_store,
            source=conclusion_source,
            normalizer=lambda **kwargs: verifier_normalizer.normalize_resolved_run(
                goal_store=worker.goal_store,
                run_store=worker.run_store,
                verdict_store=verdict_store,
                **kwargs,
            ),
        )
    checks_verdict = None
    checks_file = scheduled_checks_module.state_path(goal_store_root)
    if checks_config is not None:
        # Before the driver, so every snapshot it writes carries a current verdict.
        checks_verdict = scheduled_checks_module.run(source_repo, checks_file, **checks_config)
        worker.scheduled_checks = checks_config
    elif checks_file.exists():
        checks_file.unlink()  # disabled: a leftover verdict must not keep the rule alive
    if actuation_config is not None:
        worker.lamp_actuation = actuation_config
    summary = driver_fn(
        worker,
        holder=holder,
        model=model,
        pause_flag=pause_flag,
        max_cycles=max_cycles,
        max_runs=max_runs,
        max_runtime_seconds=max_runtime_seconds,
        budget_ceiling_tokens=budget_ceiling_tokens,
        budget_scope=budget_scope,
        idle_limit=idle_limit,
        status_snapshot_out=status_snapshot_out,
        verdict_store=verdict_store,
        conclusion_source=conclusion_source,
        quota_reader=quota_reader,
        daily_soft_cap_usd=daily_soft_cap_usd,
        daily_hard_cap_usd=daily_hard_cap_usd,
        pricing=pricing,
        sleep_fn=sleep_fn,
        turning_point=turning_point,
    )
    result = {"mode": "execute", "invoked": True, "plan": plan, "startup_external_resumed": startup_external_resumed, "driver": summary}
    if checks_verdict is not None:
        result["scheduled_checks"] = checks_verdict
    if actuation_config is not None:
        # After the driver: judge a fresh snapshot, then act only on what the pinned policy allows.
        fresh = status_snapshot.build_snapshot(
            worker.run_store, worker.goal_store, generated_at=datetime.now(timezone.utc).isoformat(),
            attempt_timeout_seconds=float(worker.controller.timeout_seconds),
            scheduled_checks=checks_config, lamp_actuation=actuation_config,
        )
        result["lamp_actuation"] = lamp_actuator.actuate(
            fresh, config=actuation_config, run_store_root=worker.run_store.root,
            goal_store_root=worker.goal_store.root,
        )
    if watch_config is not None:
        # After the driver: completed goals are looked at again; a regression is raised, never repaired.
        result["regression_watch"] = regression_watch_module.sweep(
            worker.goal_store, source_repo=source_repo, workspace_root=workspace_root, **watch_config,
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the autonomous driver with a real coding-agent executor (opt-in)")
    parser.add_argument("--executor", default=None,
                        help="declared coding executor; defaults to the contract's models.execute when --contract is used")
    parser.add_argument("--executors", default=None,
                        help="JSON file of executor declarations, for runs without a contract")
    parser.add_argument("--judge-executor", default=None,
                        help="optional declared turning-point judge executor; defaults to the contract's models.judge")
    parser.add_argument("--judge-model", default=None, help="optional model id pinned for the judge (e.g. a reasoning-tier model)")
    parser.add_argument(
        "--execution-host",
        default=None,
        choices=sorted(EXECUTION_HOSTS),
        help="execution host for capability-routed production (headless_cli)",
    )
    parser.add_argument("--bootstrap-decision-id", default=None)
    parser.add_argument("--bootstrap-authority-ref", default=None)
    parser.add_argument("--bootstrap-authority-digest", default=None)
    parser.add_argument("--bootstrap-root", default=None)
    parser.add_argument("--execute", action="store_true", help="actually invoke the executor; omit for a dry-run plan")
    # binding: either a Project Runtime Contract (--contract) or the explicit flags below
    parser.add_argument("--contract", default=None, help="path to a Project Runtime Contract; fills the binding flags below")
    parser.add_argument("--instance-config", default=None, help="optional instance-owned paths and executable discovery config")
    parser.add_argument("--dispatch-envelope", default=None, help="scheduler-owned immutable dispatch envelope")
    parser.add_argument("--assignment-packet", default=None, help="external host assignment packet to bind before LH admission")
    parser.add_argument("--assignment-correlation-id", default=None, help="correlation id bound to the assignment packet")
    parser.add_argument("--goal-store", default=None)
    parser.add_argument("--run-store", default=None)
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--campaign", default=None, help="path to a campaign JSON file")
    parser.add_argument("--source-repo", default=None)
    parser.add_argument("--base-revision", default=None)
    parser.add_argument("--pause-flag", default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--max-runs", type=int, default=None)
    parser.add_argument("--max-runtime-seconds", type=float, default=None)
    parser.add_argument("--budget-ceiling-tokens", type=int, default=None)
    parser.add_argument("--budget-scope", default=None)
    parser.add_argument("--idle-limit", type=int, default=3)
    parser.add_argument("--executor-timeout-seconds", type=int, default=int(DEFAULT_EXECUTOR_TIMEOUT_SECONDS))
    parser.add_argument("--status-snapshot-out", default=None, help="opt-in: refresh a JSON status snapshot at this path each progressing tick")
    args = parser.parse_args(argv)

    runtime_environment: dict[str, str] = {}
    if bool(args.assignment_packet) != bool(args.assignment_correlation_id):
        parser.error("--assignment-packet and --assignment-correlation-id must be supplied together")
    if args.contract:
        resolved_project = project_binding.resolve_project(
            args.contract,
            instance_config_path=args.instance_config,
            assignment_packet_path=args.assignment_packet,
            assignment_correlation_id=args.assignment_correlation_id,
        )
        binding = resolved_project["run_kwargs"]
        runtime_environment = resolved_project.get("runtime_environment", {})
        if args.dispatch_envelope:
            owner_id = os.environ.get("LH_SCHEDULER_OWNER_ID")
            if not owner_id:
                parser.error("--dispatch-envelope requires LH_SCHEDULER_OWNER_ID")
            binding["dispatch_envelope"] = dispatches.load_and_validate(
                args.dispatch_envelope,
                project_id=resolved_project["project_id"],
                owner_id=owner_id,
                contract_path=args.contract,
            )
    else:
        if args.dispatch_envelope or args.assignment_packet:
            parser.error("--dispatch-envelope/--assignment-packet require --contract")
        missing = [n for n in ("goal_store", "run_store", "workspace_root", "campaign", "source_repo", "base_revision") if not getattr(args, n.replace("-", "_"))]
        if missing:
            parser.error(f"without --contract these are required: {', '.join('--' + m.replace('_', '-') for m in missing)}")
        binding = {
            "campaign": json.loads(Path(args.campaign).read_text(encoding="utf-8")),
            "source_repo": args.source_repo,
            "base_revision": args.base_revision,
            "goal_store_root": args.goal_store,
            "run_store_root": args.run_store,
            "workspace_root": args.workspace_root,
        }
    if args.executors:
        if args.contract:
            parser.error("--executors is for runs without a contract; declare executors in the contract")
        binding["executor_declarations"] = json.loads(Path(args.executors).read_text(encoding="utf-8"))
    # explicit flags still override / supply the optional gates the contract does not carry
    binding.setdefault("pause_flag", args.pause_flag)
    if args.status_snapshot_out:
        binding.setdefault("status_snapshot_out", args.status_snapshot_out)
    if args.budget_ceiling_tokens is not None:
        binding["budget_ceiling_tokens"] = args.budget_ceiling_tokens
    if args.budget_scope is not None:
        binding["budget_scope"] = args.budget_scope
    if binding.get("execution_graph") is not None:
        if args.executor is not None or args.judge_executor is not None or args.judge_model is not None:
            parser.error(
                "--executor/--judge-executor/--judge-model cannot override a contract execution_graph"
            )
        executor = None
        judge_executor = None
        judge_model = None
    else:
        binding_executor = binding.pop("executor", None)
        executor = args.executor or binding_executor
        if not executor:
            parser.error("--executor is required (or set models.execute in the contract)")
        binding_judge_executor = binding.pop("judge_executor", None)
        judge_executor = args.judge_executor or binding_judge_executor
        binding_judge_model = binding.pop("judge_model", None)
        judge_model = args.judge_model or binding_judge_model

    bootstrap_values = (
        args.bootstrap_decision_id,
        args.bootstrap_authority_ref,
        args.bootstrap_authority_digest,
        args.bootstrap_root,
    )
    if args.execution_host is None and any(value is not None for value in bootstrap_values):
        parser.error("bootstrap authority flags require --execution-host")
    if args.execution_host is not None and not all(
        isinstance(value, str) and value for value in bootstrap_values
    ):
        parser.error(
            "--execution-host requires --bootstrap-decision-id, "
            "--bootstrap-authority-ref, --bootstrap-authority-digest, and "
            "--bootstrap-root"
        )
    bootstrap_authority = (
        {
            "decision_id": args.bootstrap_decision_id,
            "authority_ref": args.bootstrap_authority_ref,
            "authority_digest": args.bootstrap_authority_digest,
            "root": args.bootstrap_root,
        }
        if args.execution_host is not None
        else None
    )

    with instance_config.temporary_environment(runtime_environment):
        result = run(
            executor=executor,
            execute=args.execute,
            max_cycles=args.max_cycles,
            max_runs=args.max_runs,
            max_runtime_seconds=args.max_runtime_seconds,
            budget_ceiling_tokens=args.budget_ceiling_tokens,
            budget_scope=args.budget_scope,
            idle_limit=args.idle_limit,
            executor_timeout_seconds=args.executor_timeout_seconds,
            judge_executor=judge_executor,
            judge_model=judge_model,
            execution_host=args.execution_host,
            bootstrap_authority=bootstrap_authority,
            **binding,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

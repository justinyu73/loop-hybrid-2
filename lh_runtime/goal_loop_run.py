#!/usr/bin/env python3
"""Autonomous driver runner: wire a real coding-agent executor into the driver.

This is the opt-in production entry for full-auto. It is model-agnostic — the
executor is chosen by name from a registry (codex / claude / any CLI preset in
cli_agent_executor), never hardcoded — and gated: dry-run is the default and
prints the resolved plan without invoking any provider; only ``--execute``
constructs the real executor and runs the loop. The executor itself runs inside
the controller's disposable clone. Repository actions follow the approved goal
envelope; a human owns direction changes and terminal product acceptance, not
every node.
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
from typing import Any, Callable, Mapping

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import capability_resolver as cr
import cli_agent_executor as executors
import diff_grader
import dispatch_envelope as dispatches
import external_action_port as eap
import external_verdict as ev
import github_conclusion_source as ghc
import github_pr_adapter as gpa
import grill_loop
import merge_gate as mg
import project_binding
import turning_point as tp
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker, ModelRunner, TurningPointRunner
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from run_store import RunStore
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

# Model-agnostic executor registry. Add a CLI preset here, not a hardcoded model.
EXECUTORS: dict[str, Callable[..., ModelRunner]] = {
    "codex": executors.CODEX,
    "claude": executors.CLAUDE,
    "kimi": executors.KIMI,
    "orca": executors.ORCA,
}
JUDGE_EXECUTORS = {"codex", "claude", "kimi"}
CAPABILITY_EVALUATION_EXECUTORS = {"codex", "claude"}
EXECUTION_HOST_SCHEMA = "lh-execution-host-binding/v1"
BOOTSTRAP_AUTHORITY_SCHEMA = "lh-bootstrap-authority/v1"
EXECUTION_HOSTS = {"external-orca"}
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
        "adapter": "orca-terminal",
        "bootstrap_authority": {
            "schema": BOOTSTRAP_AUTHORITY_SCHEMA,
            **normalized,
        },
    }


def resolve_executor(
    name: str,
    *,
    execute: bool,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    provider_binding: dict[str, str] | None = None,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
) -> ModelRunner | None:
    """Fail closed on an unknown executor (even in dry-run). Return the real
    model only when ``execute`` is true; dry-run returns None so nothing runs."""
    factories = {**EXECUTORS, **(factory_overrides or {})}
    if name not in factories:
        raise ValueError(f"unknown executor: {name!r}; choose one of {sorted(factories)}")
    if provider_binding is not None:
        if name != "orca":
            raise ValueError("provider_binding is currently supported only with executor='orca'")
        executors._validate_provider_binding(
            provider_binding,
            agent=os.environ.get("LH_ORCA_AGENT", "codex"),
        )
    if not execute:
        return None
    kwargs: dict[str, Any] = {"timeout_seconds": timeout_seconds}
    if provider_binding is not None:
        kwargs["provider_binding"] = provider_binding
    return factories[name](**kwargs)


def _invoke_bound_model(
    model: ModelRunner,
    binding: dict[str, Any],
    workspace: Path,
    capsule: dict[str, Any],
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    try:
        provider = model(workspace, capsule)
        if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
            raise ValueError("model runner must return a dict with a bounded summary")
        status = "completed"
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
    ):
        self.timeout_seconds = timeout_seconds
        self.run_store_root = Path(run_store_root)
        self.factories = dict(factory_overrides or {})
        self.execution_host_binding = execution_host_binding
        self.graph = cr.validate_graph(graph)
        self.selected_nodes: dict[str, dict[str, Any]] = {}
        self.prior_attempts_by_run: dict[str, list[dict[str, Any]]] = {}
        for resource in self.graph["registry"]["resources"]:
            if resource["executor_kind"] != "model":
                continue
            runner = resource["runner"]
            if runner not in EXECUTORS and runner not in self.factories:
                raise ValueError(f"no runtime adapter registered for resource runner {runner!r}")
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

    @staticmethod
    def _validate_evaluation_resource(resource: dict[str, Any]) -> None:
        if resource["runner"] not in CAPABILITY_EVALUATION_EXECUTORS:
            raise ValueError(
                "evaluate_transition requires a no-write adapter; choose one of "
                f"{sorted(CAPABILITY_EVALUATION_EXECUTORS)}, got {resource['runner']!r}"
            )
        if resource.get("provider_binding") is not None:
            raise ValueError("evaluate_transition does not support an Orca provider binding")
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
                "capability production model requires the external execution host"
            )
        return executors.make_orca_agent(
            agent=runner,
            model=resource.get("model"),
            provider_binding=resource.get("provider_binding"),
            timeout_seconds=timeout_seconds,
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
        provider = _invoke_bound_model(
            self._model(resolved["resource"], resolved["node"]),
            binding,
            workspace,
            invocation_capsule,
        )
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
            path.write_text(content, encoding="utf-8")
            return {
                "ref": str(path.relative_to(self.run_store_root)),
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
            "ref": str(receipt_path.relative_to(self.run_store_root)),
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
            argv = executors.evaluation_argv(
                resource["runner"],
                prompt,
                resource["model"],
                json_schema=output_schema,
            )
            argv[0] = executors.resolve_cli(argv[0])
            env = dict(os.environ)
            env["PATH"] = f"{Path(argv[0]).parent}:{env.get('PATH', '')}"
            routing.run_store_root.mkdir(parents=True, exist_ok=True)
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
    """Return the model payload from a CLI wire envelope.

    Claude ``--output-format json`` wraps plain text in ``result`` and
    ``--json-schema`` returns the validated object in ``structured_output``.
    Other evaluation adapters currently emit their response text directly.
    """
    if runner != "claude":
        return stdout
    try:
        envelope = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("Claude evaluation output is not a JSON envelope") from exc
    if not isinstance(envelope, dict) or envelope.get("type") != "result":
        raise ValueError("Claude evaluation JSON has no result envelope")
    if envelope.get("subtype") != "success" or envelope.get("is_error") is not False:
        raise ValueError("Claude evaluation result envelope is not a successful result")
    structured = envelope.get("structured_output")
    if isinstance(structured, dict):
        return json.dumps(structured, ensure_ascii=False, sort_keys=True)
    if require_structured:
        raise ValueError("Claude schema-bound evaluation has no structured_output")
    result = envelope.get("result")
    if isinstance(result, str) and result.strip():
        return result
    raise ValueError("Claude evaluation result carries no structured_output or text")


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
    merge_gate: mg.MergeGate | None = None,
    action_ledger: eap.ActionLedger | None = None,
    external_adapter: eap.ExternalAdapter | None = None,
    knowledge_store_root: str | Path | None = None,
    knowledge_repo_roots: tuple[str | Path, ...] = (),
    dispatch_envelope: dict[str, Any] | None = None,
) -> GoalLoopWorker:
    runs = RunStore(Path(run_store_root))
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
        ),
        compilers={campaign_id: CampaignCompiler(campaign)},
        execution_context={campaign_id: {"source_repo": Path(source_repo), "base_revision": base_revision}},
        grill_runner=grill_runner,
        merge_gate=merge_gate,
        action_ledger=action_ledger,
        external_adapter=external_adapter,
        knowledge_store=knowledge_store,
        knowledge_repo_roots=tuple(Path(item) for item in knowledge_repo_roots),
    )


def build_pr_adapter(
    github_pr_adapter: dict[str, Any],
    *,
    run_store_root: str | Path,
    environ: Mapping[str, str] | None = None,
) -> tuple[eap.ActionLedger, eap.ExternalAdapter]:
    """R1: construct the durable action ledger and the draft-PR adapter.

    The ledger lives under the run store root (at-most-once survives
    restarts). Credential binding is deferred until ``perform`` so an idle
    resident tick can still refresh ownership and heartbeat evidence. A
    missing token still raises before any git or API call, and the controller
    records the external-action failure on the Attempt. The remote URL
    defaults to github.com and can be overridden for fixtures or SSH remotes
    via ``LH_GITHUB_GIT_REMOTE``.
    """
    values = os.environ if environ is None else environ
    ledger = eap.ActionLedger(Path(run_store_root) / "action-ledger.sqlite3")
    adapter = gpa.DeferredGitHubPrAdapter(
        owner=github_pr_adapter["owner"],
        repo=github_pr_adapter["repo"],
        base_branch=github_pr_adapter["base_branch"],
        run_store=RunStore(Path(run_store_root)),
        environ=values,
        remote_url=values.get("LH_GITHUB_GIT_REMOTE") or None,
    )
    return ledger, adapter


def build_merge_gate(
    github_pr_adapter: dict[str, Any],
    *,
    run_store_root: str | Path,
    verdict_store: ev.VerdictStore,
    ledger: eap.ActionLedger,
    judge: diff_grader.GraderRunner | None = None,
    environ: Mapping[str, str] | None = None,
) -> mg.DeferredMergeGate:
    """B13: construct the conditional auto-merge gate for an allowlisted repo.

    The merge credential is a SEPARATE token from the R1 draft-PR token
    (LH_GITHUB_MERGE_TOKEN, merge-only scope, allowlisted repo); a missing
    credential is bound only when a resolved row reaches the merge hook, so
    idle ticks never depend on it. The trust-ramp store sits next to the
    action ledger under the run store root, so ramp evidence survives restarts
    with the runs it refers to.
    """
    values = os.environ if environ is None else environ
    return mg.DeferredMergeGate(
        owner=github_pr_adapter["owner"],
        repo=github_pr_adapter["repo"],
        base_branch=github_pr_adapter["base_branch"],
        run_store=RunStore(Path(run_store_root)),
        verdict_store=verdict_store,
        ledger=ledger,
        ramp_store=mg.TrustRampStore(Path(run_store_root) / "ramp.sqlite3"),
        judge=judge,
        environ=values,
    )


def build_github_verdict(
    github_verdict: dict[str, str],
    *,
    run_store_root: str | Path,
    environ: Mapping[str, str] | None = None,
    transport: ghc.Transport | None = None,
) -> tuple[ev.VerdictStore, ev.ConclusionSource]:
    """Construct the durable verdict store and the GitHub conclusion source.

    The store lives under the run store root so the awaiting state survives
    host restarts next to the runs it parks. The token comes from the
    environment only and is bound lazily when an awaiting operation is
    actually polled. An idle tick therefore does not depend on GitHub
    credentials. Missing credentials or an unavailable source leave the run
    parked through the existing poll-and-resume unknown-preserving path. The token is
    never written to the store, receipts, or any artifact.
    """
    store = ev.VerdictStore(Path(run_store_root) / "verdict.sqlite3")

    def sha_resolver(op_key: str) -> str | None:
        action = store.action_for_op_key(op_key)
        external = action.get("external") if isinstance(action, dict) else None
        head_sha = external.get("head_sha") if isinstance(external, dict) else None
        return head_sha if isinstance(head_sha, str) and head_sha.strip() else None

    def source(op_key: str) -> dict[str, str] | None:
        client = ghc.GitHubConclusionSource.from_env(
            github_verdict["owner"],
            github_verdict["repo"],
            github_verdict["workflow"],
            sha_resolver,
            environ=environ,
            transport=transport,
        )
        return client(op_key)

    return store, source


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
    github_verdict: dict[str, str] | None = None,
    github_environ: Mapping[str, str] | None = None,
    github_transport: ghc.Transport | None = None,
    github_pr_adapter: dict[str, Any] | None = None,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
    driver_fn: Callable[..., dict[str, Any]] = run_driver,
    sleep_fn: Callable[[float], None] | None = None,
    judge_executor: str | None = None,
    judge_model: str | None = None,
    executor_binding: dict[str, str] | None = None,
    execution_graph: dict[str, Any] | None = None,
    execution_host: str | None = None,
    bootstrap_authority: dict[str, str] | None = None,
    compatibility_authority: dict[str, str] | None = None,
    turning_point: TurningPointRunner | None = None,
    quota_reader: Callable[[], dict[str, Any] | None] | None = None,
    daily_soft_cap_usd: float | None = 2.0,
    daily_hard_cap_usd: float | None = 5.0,
    knowledge_store_root: str | Path | None = None,
    knowledge_repo_roots: tuple[str | Path, ...] = (),
    dispatch_envelope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if github_verdict is not None:
        if verdict_store is not None or conclusion_source is not None:
            raise ValueError("github_verdict cannot be combined with an explicit verdict_store/conclusion_source")
        verdict_store, conclusion_source = build_github_verdict(
            github_verdict, run_store_root=run_store_root, environ=github_environ, transport=github_transport,
        )
    if (verdict_store is None) != (conclusion_source is None):
        raise ValueError("verdict_store and conclusion_source must be supplied together")
    execution_host_binding = build_execution_host_binding(
        execution_host,
        bootstrap_authority,
    )
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
                "execution_host='external-orca'"
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
        resolved_executor = executor
        resolved_judge = judge_executor
        routing_mode = "compatibility"
    plan = {
        "executor": resolved_executor,
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
            "auto_merge": github_pr_adapter is not None and github_pr_adapter.get("auto_merge") is True,
            "daily_soft_cap_usd": daily_soft_cap_usd,
            "daily_hard_cap_usd": daily_hard_cap_usd,
            "quota_gate": quota_reader is not None,
        },
        "boundary": (
            "executor runs in a disposable clone; commit/push/merge require the "
            "approved goal envelope and named gates; publish/release and terminal "
            "product acceptance remain outside the driver"
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
            timeout_seconds=executor_timeout_seconds,
            provider_binding=executor_binding,
            factory_overrides=factory_overrides,
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
    action_ledger: eap.ActionLedger | None = None
    external_adapter: eap.ExternalAdapter | None = None
    if github_pr_adapter is not None:
        # R1: credential binding is deferred until a real external action.
        # Idle resident ticks can still publish ownership/heartbeat evidence;
        # a missing token still stops that action before any git or API call.
        action_ledger, external_adapter = build_pr_adapter(github_pr_adapter, run_store_root=run_store_root, environ=github_environ)
    grill_runner: grill_loop.GrillRunner | None = None
    grader: diff_grader.GraderRunner | None = None
    if routing is not None and has_evaluate_node:
        # Capability evaluation is deliberately post-Attempt.  Turning-point
        # selection happens before an actual produce binding exists and remains
        # deterministic; grill and diff grading resolve against the persisted
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
        grader = _make_capability_evaluator(
            routing,
            kind="diff_grader",
            prompt_builder=diff_grader.build_grader_prompt,
            parser=diff_grader.parse_grade,
            output_schema={
                "type": "object",
                "properties": {
                    "grade": {
                        "type": "string",
                        "enum": ["routine", "sensitive"],
                    },
                    "rationale": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": diff_grader.MAX_RATIONALE_CHARS,
                    },
                },
                "required": ["grade", "rationale"],
                "additionalProperties": False,
            },
        )
    elif judge_executor is not None:
        factories = {**EXECUTORS, **(factory_overrides or {})}
        if judge_executor not in JUDGE_EXECUTORS:
            raise ValueError(f"unknown judge_executor: {judge_executor!r}; choose one of {sorted(JUDGE_EXECUTORS)}")
        turning_point = tp.make_cli_judge(
            lambda prompt: executors.judge_argv(judge_executor, prompt, judge_model),
            name=judge_executor,
        )
        # W6a: the same judge CLI layering carries the challenger grill before
        # a run's last allowed attempt. Absent judge = grill stays off (the
        # original max_attempts behavior), so a missing models.judge config
        # never blocks the loop.
        grill_runner = grill_loop.make_cli_judge(
            lambda prompt: executors.judge_argv(judge_executor, prompt, judge_model),
            name=judge_executor,
        )
        # B13: the same layering carries the merge gate's semantic diff
        # grader. Absent judge = the grader's deterministic checks still run
        # and everything else routes to a human.
        grader = diff_grader.make_cli_grader(
            lambda prompt: executors.judge_argv(judge_executor, prompt, judge_model),
            name=judge_executor,
        )
    gate: mg.MergeGate | None = None
    if external_adapter is not None and github_pr_adapter is not None and github_pr_adapter.get("auto_merge") is True:
        # B13: the contract declared conditional auto-merge for this repo. A
        # Credential binding is deferred until a resolved row reaches the
        # merge hook. Undeclared repos get no gate at all.
        if verdict_store is None:
            raise ValueError("auto_merge requires the external verdict wiring (github_verdict or verdict_store/conclusion_source)")
        gate = build_merge_gate(
            github_pr_adapter, run_store_root=run_store_root, verdict_store=verdict_store,
            ledger=action_ledger, judge=grader, environ=github_environ,
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
        merge_gate=gate,
        action_ledger=action_ledger,
        external_adapter=external_adapter,
        knowledge_store_root=knowledge_store_root,
        knowledge_repo_roots=knowledge_repo_roots,
        dispatch_envelope=dispatch_envelope,
    )
    startup_external_resumed = []
    if verdict_store is not None and conclusion_source is not None:
        # Poll before entering the driver: a host that cannot acquire the
        # driver holder may return ``not_holder`` without performing a tick.
        # Restart durability therefore cannot depend on run_driver reaching its
        # first worker.tick.
        startup_external_resumed = worker.controller.resume_external(verdict_store=verdict_store, source=conclusion_source)
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
        sleep_fn=sleep_fn,
        turning_point=turning_point,
    )
    return {"mode": "execute", "invoked": True, "plan": plan, "startup_external_resumed": startup_external_resumed, "driver": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the autonomous driver with a real coding-agent executor (opt-in)")
    parser.add_argument("--executor", default=None, choices=sorted(EXECUTORS),
                        help="coding executor; defaults to the contract's models.execute when --contract is used")
    parser.add_argument("--judge-executor", default=None, choices=sorted(JUDGE_EXECUTORS),
                        help="optional turning-point judge executor (M1 model routing); defaults to the contract's models.judge")
    parser.add_argument("--judge-model", default=None, help="optional model id pinned for the judge (e.g. a reasoning-tier model)")
    parser.add_argument(
        "--execution-host",
        default=None,
        choices=sorted(EXECUTION_HOSTS),
        help="host adapter for capability execution; external-host production uses external-orca",
    )
    parser.add_argument("--bootstrap-decision-id", default=None)
    parser.add_argument("--bootstrap-authority-ref", default=None)
    parser.add_argument("--bootstrap-authority-digest", default=None)
    parser.add_argument("--bootstrap-root", default=None)
    parser.add_argument("--execute", action="store_true", help="actually invoke the executor; omit for a dry-run plan")
    # binding: either a Project Runtime Contract (--contract) or the explicit flags below
    parser.add_argument("--contract", default=None, help="path to a Project Runtime Contract; fills the binding flags below")
    parser.add_argument("--dispatch-envelope", default=None, help="scheduler-owned immutable dispatch envelope")
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

    if args.contract:
        resolved_project = project_binding.resolve_project(args.contract)
        binding = resolved_project["run_kwargs"]
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
        if args.dispatch_envelope:
            parser.error("--dispatch-envelope requires --contract")
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

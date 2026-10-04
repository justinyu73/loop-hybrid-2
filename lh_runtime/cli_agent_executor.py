#!/usr/bin/env python3
"""Generic CLI-agent executor: turn a coding-agent CLI into a fenced ModelRunner.

Provider-neutral by design — the same adapter drives an explicitly configured
non-interactive agent CLI; only the argv builder changes.  A disposable
clone is the write target, while ``ExecutionFencePort`` supplies the preventive
kernel boundary before the adapter's first child.  The spine still owns state,
the deterministic verifier, retry, and recovery.
"""
from __future__ import annotations

import json
import hashlib
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import token_cost
import execution_fence as execution_fences
import provider_input_binding as provider_inputs
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

# Worker-process replay wall for provider-input binding nonces.
_SEEN_INPUT_NONCES: set[str] = set()


def _input_binding_gate(
    capsule: dict[str, Any],
    prompt: str,
    argv: list[str],
    env_overlay: dict[str, str] | None,
    descriptor: dict[str, Any],
) -> dict[str, Any] | None:
    """Build and attest the provider-input binding before any launch.

    The production resolver always injects `provider_binding_context`; a
    capsule carrying projected context but no binding context must never
    launch. Direct fixture invocations without either keep the legacy path —
    that residual is named in VRP-14, not silently widened."""
    context = capsule.get("provider_binding_context")
    if not isinstance(context, dict):
        if isinstance(capsule.get("provider_context_texts"), dict):
            raise provider_inputs.ProviderInputRejected("missing_binding_context")
        return None
    projection = capsule.get("provider_context_projection")
    if not isinstance(projection, dict):
        projection = {"projection_digest": provider_inputs.digest_json({})}
    env_projection = {
        key: provider_inputs.digest_text(str(value))
        for key, value in sorted((env_overlay or {}).items())
    }
    descriptor_digest = provider_inputs.digest_json(descriptor)
    segments = provider_inputs.build_segments(prompt, argv, env_projection)
    now = time.time()
    binding = provider_inputs.build_input_binding(
        binding_context=context,
        projection_record=projection,
        segments=segments,
        launch_descriptor_digest=descriptor_digest,
        nonce=f"{context.get('run_id')}:{context.get('attempt')}:{time.time_ns()}",
        issued_at=now,
    )
    attestation = provider_inputs.attest_before_launch(
        binding, prompt=prompt, command_template=argv,
        environment_projection=env_projection,
        launch_descriptor_digest=descriptor_digest, now=now,
        seen_nonces=_SEEN_INPUT_NONCES,
    )
    return {"provider_input_binding": binding, "provider_input_attestation": attestation}

# Given the prompt text, return the argv to run the agent non-interactively in cwd=workspace.
ArgvBuilder = Callable[[str], list[str]]
# Given the agent's stdout, return a token_cost usage record (measured or unknown).
UsageParser = Callable[[str], dict[str, Any]]
# Post-run: given the completed process and context {started_at, capsule}, return a
# usage record. Use this when usage lives in a session log rather than stdout.
UsageCollector = Callable[[subprocess.CompletedProcess, dict[str, Any]], dict[str, Any]]
# Pre-run: read the current cumulative session-log state so the collector can
# bill only the per-invocation delta (session logs are cumulative when reused).
SnapshotFn = Callable[[], dict[str, Any]]
ProviderBinding = dict[str, str]

LOCAL_BINDING_PROVIDER_ID = "lh_local"


def _usage_hooks(agent: str) -> tuple[UsageCollector | None, SnapshotFn | None]:
    """Use the underlying provider collector."""
    if agent == "codex":
        return codex_usage.collector, codex_usage.snapshot
    # A custom adapter may deliberately provide no local usage reader.  The
    # caller still has to name the provider and supply its argv builder.
    return None, None


def _adapter_descriptor(capsule: dict[str, Any]) -> dict[str, Any]:
    """The controller owns descriptor preparation; an adapter never self-prepares."""
    descriptor = capsule.get("execution_fence")
    if isinstance(descriptor, dict):
        return descriptor
    raise execution_fences.ExecutionFenceUnavailable("descriptor_missing")


def _validate_provider_binding(binding: Any, *, agent: str) -> ProviderBinding:
    """Validate the explicit provider tuple before a provider is launched.

    A binding is intentionally not read from ambient environment: callers must
    pass the complete tuple for the individual run.  Credentials are not part
    of this object and URLs with userinfo are rejected so a binding cannot
    smuggle a secret into a provider command or receipt.
    """
    if not isinstance(binding, dict) or set(binding) != {"runner", "base_url", "model"}:
        raise ValueError("provider_binding must contain exactly runner, base_url, and model")
    values = {key: binding.get(key) for key in ("runner", "base_url", "model")}
    if any(not isinstance(value, str) or not value.strip() for value in values.values()):
        raise ValueError("provider_binding fields must be non-empty strings")
    runner = values["runner"].strip()
    base_url = values["base_url"].strip().rstrip("/")
    model = values["model"].strip()
    if runner != agent:
        raise ValueError(f"provider_binding.runner {runner!r} does not match provider {agent!r}")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("provider_binding.base_url must be a credential-free absolute http(s) URL")
    if any(character.isspace() for character in model):
        raise ValueError("provider_binding.model must not contain whitespace")
    return {"runner": runner, "base_url": base_url, "model": model}


def _bind_provider_argv(
    agent: str,
    provider_argv: list[str],
    binding: Any,
    *,
    provider_id: str,
    provider_name: str,
    host: str,
) -> tuple[list[str], dict[str, str], dict[str, str] | None]:
    """Bind one provider tuple through the provider's own per-invocation flags."""
    if binding is None:
        return provider_argv, {}, None
    normalized = _validate_provider_binding(binding, agent=agent)
    if agent == "codex":
        if len(provider_argv) < 2 or provider_argv[1] != "exec":
            raise ValueError(f"Codex {host} provider argv must begin with 'codex exec'")
        base_url = json.dumps(normalized["base_url"])
        provider_config = (
            f'model_providers.{provider_id}={{name="{provider_name}",'
            f"base_url={base_url},wire_api=\"responses\"}}"
        )
        bound_argv = [
            *provider_argv[:2],
            "-c", provider_config,
            "-c", f'model_provider="{provider_id}"',
            "-m", normalized["model"],
            *provider_argv[2:],
        ]
        return bound_argv, {}, {"runner": agent, "model": normalized["model"], "mode": "codex_argv"}
    raise ValueError(f"{host} provider binding needs an explicit adapter for agent {agent!r}")


def make_local_provider_agent(
    agent: str, *, timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    usage_parser: UsageParser | None = None,
    provider_argv_builder: ArgvBuilder | None = None,
    provider_binding: ProviderBinding | None = None, model: str | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Run one provider directly in the local provider sandbox.

    The fence backend starts the pinned provider under the descriptor-signed
    provider-sandbox profile (bubblewrap bind set, provider seccomp table,
    host network).  The provider-binding tuple travels through the
    provider's own per-invocation flags."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if provider_binding is not None and model is not None:
        raise ValueError("model and provider_binding are mutually exclusive")
    if provider_argv_builder is None:
        if agent != "codex":
            raise ValueError(f"unknown local provider agent: {agent!r}")
        provider_argv_builder = lambda prompt: hosted_provider_argv(agent, prompt, model)  # noqa: E731
    if agent == "codex" and usage_parser is None:
        # --ephemeral --json stdout is the per-invocation usage boundary.
        usage_parser = codex_usage.extract_usage_from_jsonl
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else execution_fences.DisabledExecutionFencePort()
    )

    def model_runner(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        del workspace  # the clone is the descriptor's; the fence enforces it
        descriptor = _adapter_descriptor(capsule)
        prompt_text = build_prompt(capsule)
        provider_argv = list(provider_argv_builder(prompt_text))
        provider_argv, provider_env, binding_projection = _bind_provider_argv(
            agent,
            provider_argv,
            provider_binding,
            provider_id=LOCAL_BINDING_PROVIDER_ID,
            provider_name="LH local",
            host="local",
        )
        provider_argv[0] = resolve_cli(provider_argv[0])
        input_binding_records = _input_binding_gate(
            capsule, prompt_text, provider_argv, provider_env, descriptor)
        proc = fence_port.launch_provider(
            descriptor,
            provider_argv,
            env_overlay=provider_env,
            timeout_seconds=timeout_seconds,
        )
        stdout = proc.stdout or ""
        usage: dict[str, Any] | None = None
        try:
            if usage_parser is not None:
                usage = usage_parser(stdout)
        except Exception:  # A parse failure must not fabricate usage; stay unknown.
            usage = None
        if binding_projection is not None and isinstance(usage, dict) and usage.get("state") == token_cost.USAGE_MEASURED:
            # A bound endpoint may price differently, so token counts do not
            # imply the default cost table.
            usage = token_cost.unknown_usage(
                model=str(usage.get("model") or binding_projection.get("model") or agent),
                reason="local provider binding has no verified usage/cost attribution",
            )
        if not isinstance(usage, dict) or usage.get("state") not in {token_cost.USAGE_MEASURED, token_cost.USAGE_UNKNOWN}:
            usage = token_cost.unknown_usage(model=agent, reason="local provider usage unavailable")
        if proc.returncode != 0:
            raise RuntimeError(
                f"{agent} in the local provider sandbox exited {proc.returncode}: "
                f"{((proc.stderr or '') or stdout)[-400:]}"
            )
        execution: dict[str, Any] = {"backend": "local", "agent": agent, "exit_code": proc.returncode}
        if binding_projection is not None:
            execution["provider_binding"] = binding_projection
        result = {
            "summary": f"{agent} executor completed in the local provider sandbox",
            "stdout_tail": stdout[-800:],
            "usage": usage,
            "execution": execution,
        }
        if input_binding_records is not None:
            result.update(input_binding_records)
        return result

    return execution_fences.mark_mutation_adapter(
        model_runner,
        adapter_id=f"{execution_fences.LOCAL_PROVIDER_ADAPTER_PREFIX}{agent}",
    )


def build_prompt(capsule: dict[str, Any]) -> str:
    texts = capsule.get("provider_context_texts")
    return provider_inputs.render_prompt(
        capsule, texts if isinstance(texts, dict) else {})


def make_cli_agent(
    argv_builder: ArgvBuilder,
    *,
    name: str,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    usage_parser: UsageParser | None = None,
    usage_collector: UsageCollector | None = None,
    snapshot_fn: SnapshotFn | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else execution_fences.DisabledExecutionFencePort()
    )

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        descriptor = _adapter_descriptor(capsule)
        prompt_text = build_prompt(capsule)
        argv = list(argv_builder(prompt_text))
        argv[0] = resolve_cli(argv[0])
        input_binding_records = _input_binding_gate(
            capsule, prompt_text, argv, None, descriptor)
        started_at = time.time()
        snap: dict[str, Any] | None = None
        if snapshot_fn is not None:
            try:
                baseline = snapshot_fn()
                snap = baseline if isinstance(baseline, dict) else None
            except Exception:  # A snapshot failure must not block dispatch; the collector degrades.
                snap = None
        proc = fence_port.launch(
            descriptor,
            argv,
            timeout_seconds=timeout_seconds,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"{name} exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        usage = None
        try:
            if usage_collector is not None:
                usage = usage_collector(proc, {"started_at": started_at, "capsule": capsule, "snapshot": snap})
            elif usage_parser is not None:
                usage = usage_parser(proc.stdout)
        except Exception:  # A parse/collect failure must not fabricate usage; stay unknown.
            usage = None
        if not isinstance(usage, dict) or usage.get("state") not in {token_cost.USAGE_MEASURED, token_cost.USAGE_UNKNOWN}:
            usage = token_cost.unknown_usage(model=name)
        result = {"summary": f"{name} executor completed", "stdout_tail": proc.stdout[-800:], "usage": usage}
        if input_binding_records is not None:
            result.update(input_binding_records)
        return result
    return execution_fences.mark_mutation_adapter(model, adapter_id=name)


def resolve_cli(binary: str) -> str:
    """Resolve through instance-owned discovery, preserving explicit paths."""
    try:
        from . import instance_config
    except ImportError:
        import instance_config
    requested = Path(binary).expanduser()
    explicit = binary if (requested.is_absolute() or requested.parent != Path(".")
                          or os.sep in binary or (os.altsep and os.altsep in binary)) else None
    discovered = instance_config.discover_executable(binary, explicit=explicit)
    if not discovered.get("path"):
        raise FileNotFoundError(f"executor_cli_unavailable:{binary}:{discovered.get('source')}")
    return str(discovered["path"])


def run_bounded_judge(
    argv: list[str],
    *,
    name: str,
    timeout_seconds: int = 300,
) -> str:
    """Run one judge call outside the target workspace.

    Judge input is already a bounded snapshot, so the provider gets a fresh
    empty cwd rather than the LH checkout or a target clone.  This prevents a
    judge adapter from turning a read-only advisory call into a workspace
    mutation path.  IDE bridge variables are removed so a headless call cannot
    silently attach to a VS Code session.  Providers still retain their normal
    user configuration and network egress for authentication/inference.
    """
    if not argv:
        raise ValueError("judge argv must not be empty")
    resolved = list(argv)
    resolved[0] = resolve_cli(resolved[0])
    env = dict(os.environ)
    env["PATH"] = f"{Path(resolved[0]).parent}:{env.get('PATH', '')}"
    for key in tuple(env):
        if key.startswith("GEMINI_CLI_IDE_"):
            env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="lh-judge-") as raw_cwd:
        proc = subprocess.run(
            resolved,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
            cwd=raw_cwd,
        )
    if proc.returncode != 0:
        raise RuntimeError(f"{name} judge exited {proc.returncode}: {proc.stderr.strip()[:400]}")
    return proc.stdout


# Model-agnostic presets.  Their broad provider flags remain inside the
# controller-issued kernel descriptor; a clone path alone grants nothing.
CODEX_RESULT_SCHEMA = "lh-codex-normalized-result/v1"
TRUSTED_USAGE_FIELDS = {"state", "input_tokens", "cached_input_tokens", "fresh_input_tokens",
                        "output_tokens", "reasoning_output_tokens", "total_tokens"}


def trusted_codex_output_schema(role):
    if role == "coding":
        properties = {"schema": {"type": "string", "enum": ["lh-codex-worker-result/v1"]},
            "status": {"type": "string", "enum": ["completed", "failed"]},
            "packet_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "reason_code": {"type": "string", "enum": ["completed", "task_failed", "blocked"]}}
    elif role == "verifier":
        properties = {"schema": {"type": "string", "enum": ["lh-codex-verifier-result/v1"]},
            "verdict": {"type": "string", "enum": ["GREEN", "RED"]},
            "candidate_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "checks_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "reason_code": {"type": "string", "enum": ["verified", "check_failed", "scope_failed"]}}
    else:
        raise ValueError("trusted_codex_role_invalid")
    return {"type": "object", "properties": properties, "required": list(properties),
            "additionalProperties": False}


def compose_trusted_codex_argv(provider, *, role, output_schema_path):
    if __package__:
        from .execution_fence_trusted import validate_trusted_operator_binding
    else:
        from execution_fence_trusted import validate_trusted_operator_binding
    # Validate the same closed provider shape without discovering credentials.
    validate_trusted_operator_binding({"schema": "lh-trusted-project-operator-binding/v1",
        "project_id": "argv-validation", "providers": {"coding": provider, "verifier": provider}})
    trusted_codex_output_schema(role)
    path = Path(output_schema_path)
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_file():
        raise ValueError("trusted_output_schema_path_invalid")
    return [provider["executable"], "exec", "--json", "--ephemeral", "--ignore-user-config",
        "--model", provider["model"], "--sandbox", "workspace-write" if role == "coding" else "read-only",
        "-c", 'model_provider="openai"', "-c", 'forced_login_method="chatgpt"',
        "-c", 'approval_policy="never"', "-c", 'features.hooks=false',
        "--output-schema", str(path), "-"]


def unknown_trusted_usage():
    return {key: "unknown" if key == "state" else None for key in TRUSTED_USAGE_FIELDS}


def validate_trusted_usage(value):
    if not isinstance(value, dict) or set(value) != TRUSTED_USAGE_FIELDS:
        raise ValueError("trusted_usage_fields_invalid")
    if value["state"] == "unknown":
        if any(value[key] is not None for key in TRUSTED_USAGE_FIELDS - {"state"}):
            raise ValueError("trusted_unknown_usage_invalid")
    elif value["state"] == "observed":
        if (any(isinstance(value[key], bool) or not isinstance(value[key], int) or value[key] < 0
                for key in TRUSTED_USAGE_FIELDS - {"state"})
                or value["cached_input_tokens"] > value["input_tokens"]
                or value["reasoning_output_tokens"] > value["output_tokens"]
                or value["fresh_input_tokens"] != value["input_tokens"] - value["cached_input_tokens"]
                or value["total_tokens"] != value["input_tokens"] + value["output_tokens"]):
            raise ValueError("trusted_observed_usage_invalid")
    else:
        raise ValueError("trusted_usage_state_invalid")
    return dict(value)


def _normalize_trusted_codex_usage(usage, diagnostics):
    """Accept only the documented legacy and 0.154.0 usage shapes.

    Preserve the existing input-plus-output total without counting cache writes
    again. Optional wire presence is separate; missing counters stay unknown.
    """
    required = {"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"}
    extension = "cache_write_input_tokens"
    known = required | {extension}
    if not isinstance(usage, dict):
        diagnostics["usage_reason"] = "usage_not_object"
        return unknown_trusted_usage()
    diagnostics["unknown_usage_field_count"] = min(65535, len(set(usage) - known))
    for key in known:
        if key in usage:
            value = usage[key]
            valid = isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 2**63 - 1
            diagnostics["field_status"][key] = "nonnegative_integer" if valid else "invalid"
    if not required <= set(usage):
        diagnostics["usage_reason"] = "usage_fields_missing"
    elif set(usage) - known:
        diagnostics["usage_reason"] = "usage_fields_unknown"
    elif "invalid" in diagnostics["field_status"].values():
        diagnostics["usage_reason"] = "usage_token_invalid"
    else:
        try:
            normalized = validate_trusted_usage({"state": "observed",
                **{key: usage[key] for key in required},
                "fresh_input_tokens": usage["input_tokens"] - usage["cached_input_tokens"],
                "total_tokens": usage["input_tokens"] + usage["output_tokens"]})
        except ValueError:
            diagnostics["usage_reason"] = "usage_inconsistent"
        else:
            diagnostics["usage_reason"] = "observed"
            return normalized
    return unknown_trusted_usage()


def normalize_trusted_codex_jsonl(stdout, *, stderr, returncode, role, model, expected,
                                max_bytes=1048576, diagnostics=None):
    """Normalize one bounded stream, without persisting any free-form text."""
    import re
    if diagnostics is None:
        diagnostics = {}
    elif not isinstance(diagnostics, dict):
        raise ValueError("trusted_diagnostics_target_invalid")
    diagnostics.clear()
    diagnostics.update(schema="lh-codex-rejection-diagnostics/v1", usage_reason="not_examined",
        field_status={key: "missing" for key in ("input_tokens", "cached_input_tokens",
            "cache_write_input_tokens", "output_tokens", "reasoning_output_tokens")},
        unknown_usage_field_count=0)
    raw, diagnostic = stdout.encode("utf-8"), stderr.encode("utf-8")
    result = {"schema": CODEX_RESULT_SCHEMA, "normalization_version": 1, "role": role,
        "model": model, "outcome": "unknown", "reason_code": "provider_protocol_invalid",
        "result": None, "result_digest": None, "stdout_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "stdout_bytes": len(raw), "stderr_digest": "sha256:" + hashlib.sha256(diagnostic).hexdigest(),
        "stderr_bytes": len(diagnostic), "usage": unknown_trusted_usage(),
        "cli_launches": 1, "provider_invocations": None}
    if (isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1
            or len(raw) + len(diagnostic) > max_bytes):
        result["reason_code"] = "output_limit_exceeded"
        return result
    def closed_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate json key")
            value[key] = item
        return value

    try:
        schema = trusted_codex_output_schema(role)
        events = [json.loads(line, object_pairs_hook=closed_object) for line in stdout.splitlines() if line.strip()]
        terminal, messages, thread_count, turn_count = None, [], 0, 0
        for event in events:
            if not isinstance(event, dict) or terminal is not None:
                raise ValueError("event order")
            kind = event.get("type")
            if kind == "thread.started":
                thread_count += 1
            elif kind == "turn.started":
                turn_count += 1
            elif kind in {"turn.completed", "turn.failed"}:
                terminal = event
            elif kind in {"item.started", "item.updated", "item.completed"}:
                item = event.get("item")
                if not isinstance(item, dict):
                    raise ValueError("item")
                if kind == "item.completed" and item.get("type") == "agent_message":
                    messages.append(item.get("text"))
            else:
                raise ValueError("event")
        if terminal is None or thread_count != 1 or turn_count != 1:
            raise ValueError("missing terminal")
        result["usage"] = _normalize_trusted_codex_usage(terminal.get("usage"), diagnostics)
        if terminal["type"] == "turn.failed":
            result.update(outcome="known_failure", reason_code="provider_failed")
            return result
        value = (json.loads(messages[-1], object_pairs_hook=closed_object)
                 if messages and isinstance(messages[-1], str) else None)
        if not isinstance(value, dict) or set(value) != set(schema["properties"]):
            raise ValueError("final shape")
        for key, definition in schema["properties"].items():
            if (not isinstance(value[key], str)
                    or ("enum" in definition and value[key] not in definition["enum"])
                    or ("pattern" in definition and re.fullmatch(definition["pattern"], value[key]) is None)
                    or (key.endswith("_digest") and value[key] != expected.get(key))):
                raise ValueError("final binding")
        if isinstance(returncode, bool) or not isinstance(returncode, int):
            raise ValueError("exit")
        outcome = "known_failure" if returncode != 0 or value.get("status") == "failed" else "completed"
        result.update(outcome=outcome, reason_code="process_exit_nonzero" if returncode else value["reason_code"],
            result=value, result_digest=execution_fences.digest_json(value))
        return result
    except (ValueError, TypeError, KeyError, IndexError):
        result.update(outcome="unknown", reason_code="provider_protocol_invalid", result=None, result_digest=None)
        return result


def codex_argv(prompt: str) -> list[str]:
    return ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", prompt]


def hosted_provider_argv(
    agent: str,
    prompt: str,
    model: str | None = None,
) -> list[str]:
    """Build the per-invocation provider argv (ephemeral JSONL stream) for a sandboxed provider."""
    if agent == "codex":
        argv = ["codex", "exec"]
        if model:
            argv += ["-m", model]
        return [
            *argv,
            "--ephemeral",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            prompt,
        ]
    return provider_argv(agent, prompt, model)


def provider_argv(executor: str, prompt: str, model: str | None = None) -> list[str]:
    """Model-pinned argv for an *executing* provider (sandboxed or named CLI agent).

    Carries the provider's broad flags because the kernel fence, not the argv,
    is what bounds a mutation run. This is the shape `judge_argv` used to have
    before D3 split the two postures; nothing on the executor side changed.
    """
    if executor == "codex":
        argv = ["codex", "exec"]
        if model:
            argv += ["-m", model]
        argv += ["--dangerously-bypass-approvals-and-sandbox", prompt]
    elif executor == "agy":
        return judge_argv(executor, prompt, model)
    else:
        raise ValueError(f"no provider argv for executor: {executor!r}")
    return argv


# Tokens that grant a provider write/approval bypass. A judge argv must never
# carry one: the judge runs with no fence, on the host network, and its prompt
# is built from a snapshot that carries provider and external output (D3,
# decision packet Q3). The canary asserts this set against every executor.
BYPASS_TOKENS: tuple[str, ...] = (
    "--dangerously-bypass-approvals-and-sandbox",
    "--yolo",
)


def judge_argv(executor: str, prompt: str, model: str | None = None) -> list[str]:
    """Argv for one bounded turning-point judgment call (M1 model routing).

    The judge is advisory and read-only, so configured adapters must provide a
    no-write argv shape. This core supplies Codex and AGY shapes; an
    unregistered provider is refused.
    """
    if executor == "codex":
        argv = ["codex", "exec"]
        if model:
            argv += ["-m", model]
        argv += ["--sandbox", "read-only", "--ephemeral", prompt]
    elif executor == "agy":
        if not isinstance(model, str) or not model.strip():
            raise ValueError("agy judge requires an explicit model binding")
        if any(character.isspace() for character in model.strip()):
            raise ValueError("agy judge model binding must not contain whitespace")
        argv = [
            "agy",
            "--model", model.strip(),
            "--mode", "plan",
            "--disable-slash-commands",
            "--output-format", "json",
            "--print-timeout", "300s",
            "--print", prompt,
        ]
    else:
        raise ValueError(f"no judge argv for executor: {executor!r}")
    return argv


def evaluation_argv(
    executor: str,
    prompt: str,
    model: str,
    *,
    json_schema: dict[str, Any] | None = None,
) -> list[str]:
    """Build a no-write evaluation invocation for capability routing.

    Provider transport still requires external network access.  Codex gets an
    explicit read-only sandbox.  Other providers need an explicitly registered
    evaluation adapter; this core does not infer one.
    """
    if executor == "codex":
        return [
            "codex", "exec", "-m", model,
            "--sandbox", "read-only",
            "--ephemeral",
            prompt,
        ]
    raise ValueError(f"no no-write evaluation adapter for executor: {executor!r}")


def make_named_cli_agent(
    name: str,
    *,
    model: str | None = None,
    provider_binding: ProviderBinding | None = None,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Build one runtime-selected CLI adapter without assigning it a role."""
    if name == "local":
        agent = _local_agent_name(provider_binding)
        return make_local_provider_agent(
            agent=agent,
            provider_binding=provider_binding,
            model=model,
            timeout_seconds=timeout_seconds,
            execution_fence_port=execution_fence_port,
        )
    if provider_binding is not None:
        raise ValueError("provider_binding is supported only by the local provider sandbox")
    builders: dict[str, ArgvBuilder] = {
        "codex": codex_argv,
    }
    if name not in builders:
        raise ValueError(f"unknown CLI adapter: {name!r}; choose one of {sorted([*builders, 'local'])}")
    builder = builders[name] if model is None else lambda prompt: provider_argv(name, prompt, model)
    collector, snapshot = _usage_hooks(name)
    return make_cli_agent(
        builder,
        name=name,
        timeout_seconds=timeout_seconds,
        usage_collector=collector,
        snapshot_fn=snapshot,
        execution_fence_port=execution_fence_port,
    )


import codex_usage  # noqa: E402

CODEX = lambda **kw: make_cli_agent(codex_argv, name="codex", usage_collector=kw.pop("usage_collector", codex_usage.collector), snapshot_fn=kw.pop("snapshot_fn", codex_usage.snapshot), **kw)  # noqa: E731


def _local_agent_name(provider_binding: Any) -> str:
    """The provider a local run starts: the binding's runner, else explicit config."""
    agent = (
        provider_binding["runner"]
        if isinstance(provider_binding, dict) and isinstance(provider_binding.get("runner"), str)
        else os.environ.get("LH_LOCAL_PROVIDER_AGENT", "").strip()
    )
    if not agent:
        raise ValueError(
            "local provider is not configured; set LH_LOCAL_PROVIDER_AGENT or pass provider_binding"
        )
    return agent


def _configured_local(**kwargs: Any) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    return make_local_provider_agent(agent=_local_agent_name(kwargs.get("provider_binding")), **kwargs)


LOCAL = _configured_local

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
import re
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
    mutation path.  Providers still retain their normal user configuration and
    network egress for authentication/inference.
    """
    if not argv:
        raise ValueError("judge argv must not be empty")
    resolved = list(argv)
    resolved[0] = resolve_cli(resolved[0])
    env = dict(os.environ)
    env["PATH"] = f"{Path(resolved[0]).parent}:{env.get('PATH', '')}"
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


# Trusted provider protocol.  The operator declares the provider command; the
# engine owns the result schema, the usage shape, and the normalization.  A
# provider reads its role input on stdin, finds the closed result schema at the
# path named by OUTPUT_SCHEMA_ENV, and ends its stdout with one result line.
PROVIDER_ADAPTER_ID = "provider-jsonl-v1"
PROVIDER_RESULT_LINE_SCHEMA = "lh-provider-result/v1"
TRUSTED_RESULT_SCHEMA = "lh-provider-normalized-result/v1"
OUTPUT_SCHEMA_ENV = "LH_PROVIDER_OUTPUT_SCHEMA"
TRUSTED_USAGE_FIELDS = {"state", "input_tokens", "cached_input_tokens", "fresh_input_tokens",
                        "output_tokens", "reasoning_output_tokens", "total_tokens"}
_REPORTED_USAGE_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")


def _review_engine():
    if __package__:
        from . import delivery_contract
    else:
        import delivery_contract
    return delivery_contract


def trusted_output_schema(role, *, review_version=1):
    """The closed role result; verifier v2 also carries a candidate review."""
    if review_version not in {1, 2} or (review_version == 2 and role != "verifier"):
        raise ValueError("trusted_review_version_invalid")
    if role == "coding":
        properties = {"schema": {"type": "string", "enum": ["lh-worker-result/v1"]},
            "status": {"type": "string", "enum": ["completed", "failed"]},
            "packet_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "reason_code": {"type": "string", "enum": ["completed", "task_failed", "blocked"]}}
    elif role == "verifier":
        properties = {"schema": {"type": "string", "enum": ["lh-verifier-result/v1"]},
            "verdict": {"type": "string", "enum": ["GREEN", "RED"]},
            "candidate_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "checks_digest": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "reason_code": {"type": "string", "enum": ["verified", "check_failed", "scope_failed"]}}
    else:
        raise ValueError("trusted_role_invalid")
    if review_version == 2:
        properties["schema"] = {"type": "string", "enum": ["lh-verifier-result/v2"]}
        properties["review"] = _review_engine().candidate_review_output_schema()
    return {"type": "object", "properties": properties, "required": list(properties),
            "additionalProperties": False}


def compose_trusted_provider_argv(provider):
    """The operator-declared provider command, validated in its closed shape."""
    if __package__:
        from .execution_fence_trusted import validate_trusted_operator_binding
    else:
        from execution_fence_trusted import validate_trusted_operator_binding
    validate_trusted_operator_binding({"schema": "lh-trusted-project-operator-binding/v1",
        "project_id": "argv-validation", "providers": {"coding": provider, "verifier": provider}})
    return [provider["executable"], *provider["arguments"]]


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


def _normalize_reported_usage(usage, diagnostics):
    """Accept exactly the four reported counters; anything else stays unknown."""
    if usage is None:
        diagnostics["usage_reason"] = "usage_not_reported"
        return unknown_trusted_usage()
    if not isinstance(usage, dict):
        diagnostics["usage_reason"] = "usage_not_object"
        return unknown_trusted_usage()
    for key in _REPORTED_USAGE_FIELDS:
        if key in usage:
            value = usage[key]
            valid = isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 2**63 - 1
            diagnostics["field_status"][key] = "nonnegative_integer" if valid else "invalid"
    if set(usage) != set(_REPORTED_USAGE_FIELDS):
        diagnostics["usage_reason"] = "usage_fields_mismatch"
    elif "invalid" in diagnostics["field_status"].values():
        diagnostics["usage_reason"] = "usage_token_invalid"
    else:
        try:
            normalized = validate_trusted_usage({"state": "observed",
                **{key: usage[key] for key in _REPORTED_USAGE_FIELDS},
                "fresh_input_tokens": usage["input_tokens"] - usage["cached_input_tokens"],
                "total_tokens": usage["input_tokens"] + usage["output_tokens"]})
        except ValueError:
            diagnostics["usage_reason"] = "usage_inconsistent"
        else:
            diagnostics["usage_reason"] = "observed"
            return normalized
    return unknown_trusted_usage()


def normalize_trusted_provider_output(stdout, *, stderr, returncode, role, model, expected,
                                      max_bytes=1048576, diagnostics=None):
    """Normalize one bounded provider stream, without persisting any free-form text.

    Only the last non-empty stdout line is read.  It must be one JSON object:
    ``{"schema": "lh-provider-result/v1", "outcome": "completed" | "failed",
    "result": <role result or null>, "usage": <four counters or null>}``.
    """
    import re
    if diagnostics is None:
        diagnostics = {}
    elif not isinstance(diagnostics, dict):
        raise ValueError("trusted_diagnostics_target_invalid")
    diagnostics.clear()
    diagnostics.update(schema="lh-provider-rejection-diagnostics/v1", usage_reason="not_examined",
        field_status={key: "missing" for key in _REPORTED_USAGE_FIELDS})
    raw, diagnostic = stdout.encode("utf-8"), stderr.encode("utf-8")
    result = {"schema": TRUSTED_RESULT_SCHEMA, "normalization_version": 1, "role": role,
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
        # A bound review context selects v2; without one, v2 is never accepted.
        review_context = expected.get("candidate_review_context") if role == "verifier" else None
        schema = trusted_output_schema(role, review_version=2 if review_context is not None else 1)
        lines = [line for line in stdout.splitlines() if line.strip()]
        line = json.loads(lines[-1], object_pairs_hook=closed_object) if lines else None
        if (not isinstance(line, dict) or set(line) != {"schema", "outcome", "result", "usage"}
                or line["schema"] != PROVIDER_RESULT_LINE_SCHEMA
                or line["outcome"] not in {"completed", "failed"}):
            raise ValueError("result line")
        result["usage"] = _normalize_reported_usage(line["usage"], diagnostics)
        if line["outcome"] == "failed":
            result.update(outcome="known_failure", reason_code="provider_failed")
            return result
        value = line["result"]
        if not isinstance(value, dict) or set(value) != set(schema["properties"]):
            raise ValueError("final shape")
        for key, definition in schema["properties"].items():
            if key == "review":
                if _review_engine().validate_candidate_review_result(value[key], review_context) != value["verdict"]:
                    raise ValueError("final review verdict")
                continue
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


# Declared executors.  The engine knows no provider by name: every executor,
# judge, and evaluator is a declaration whose argv[0] is an absolute path and
# whose argv carries the prompt in the ``{prompt}`` slot.  ``{model}`` and
# ``{base_url}`` slots receive a pinned model and a provider binding's endpoint;
# a value without its slot, or a slot without its value, is refused.  Nothing
# here searches PATH.
EXECUTOR_NAME_RE = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
USAGE_PROTOCOLS = ("none", "lh-usage-line/v1")
PROMPT_SLOT, MODEL_SLOT, BASE_URL_SLOT = "{prompt}", "{model}", "{base_url}"


def validate_executor_declarations(raw: Any) -> dict[str, dict[str, Any]]:
    """Validate ``{name: {"argv": [...], "usage": ...}}``; an absent table declares nothing."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("executors must be an object of named declarations")
    declarations: dict[str, dict[str, Any]] = {}
    for name, declaration in raw.items():
        if not isinstance(name, str) or EXECUTOR_NAME_RE.fullmatch(name) is None:
            raise ValueError(f"executor name is invalid: {name!r}")
        if not isinstance(declaration, dict) or "argv" not in declaration or set(declaration) - {"argv", "usage"}:
            raise ValueError(f"executors.{name} must contain argv and an optional usage protocol")
        argv = declaration["argv"]
        if (not isinstance(argv, list) or not argv
                or any(not isinstance(item, str) or not item or "\x00" in item for item in argv)):
            raise ValueError(f"executors.{name}.argv must be a non-empty list of strings")
        if not Path(argv[0]).is_absolute():
            raise ValueError(f"executors.{name}.argv[0] must be an absolute path")
        if argv.count(PROMPT_SLOT) != 1:
            raise ValueError(f"executors.{name}.argv must carry the {PROMPT_SLOT} slot exactly once")
        if argv.count(MODEL_SLOT) > 1 or argv.count(BASE_URL_SLOT) > 1:
            raise ValueError(f"executors.{name}.argv may carry each slot at most once")
        usage = declaration.get("usage", "none")
        if usage not in USAGE_PROTOCOLS:
            raise ValueError(f"executors.{name}.usage must be one of {list(USAGE_PROTOCOLS)}")
        declarations[name] = {"argv": list(argv), "usage": usage}
    return declarations


def declared_argv(declaration: dict[str, Any], prompt: str, *, model: str | None = None,
                  base_url: str | None = None) -> list[str]:
    """Fill a declaration's slots; a value and its slot must come together."""
    argv = declaration["argv"]
    for slot, value in ((MODEL_SLOT, model), (BASE_URL_SLOT, base_url)):
        if value is not None and slot not in argv:
            raise ValueError(f"executor declaration has no {slot} slot")
        if value is None and slot in argv:
            raise ValueError(f"executor declaration needs a value for {slot}")
    values = {PROMPT_SLOT: prompt, MODEL_SLOT: model, BASE_URL_SLOT: base_url}
    return [values[item] if item in values else item for item in argv]


def declared_command(declarations: dict[str, dict[str, Any]], name: str, prompt: str,
                     model: str | None = None) -> list[str]:
    """The argv for one judge or evaluator call through a declared executor."""
    if name not in declarations:
        raise ValueError(f"unknown executor: {name!r}; declared: {sorted(declarations)}")
    return declared_argv(declarations[name], prompt, model=model)


def parse_usage_line(stdout: str, *, model: str) -> dict[str, Any]:
    """Read ``{"usage": {...}}`` from the last non-empty stdout line, else unknown."""
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    try:
        value = json.loads(lines[-1]) if lines else None
    except ValueError:
        value = None
    usage = value.get("usage") if isinstance(value, dict) else None
    counters = ("input_tokens", "output_tokens", "cache_read_tokens")
    if (isinstance(usage, dict) and {"input_tokens", "output_tokens"} <= set(usage)
            and all(isinstance(usage.get(key, 0), int) and not isinstance(usage.get(key, 0), bool)
                    and usage.get(key, 0) >= 0 for key in counters)):
        reported = usage.get("model")
        return token_cost.measured_usage(model=reported if isinstance(reported, str) and reported else model,
                                         input_tokens=usage["input_tokens"], output_tokens=usage["output_tokens"],
                                         cache_read_tokens=usage.get("cache_read_tokens", 0))
    return token_cost.unknown_usage(model=model, reason="executor reported no usage line")


def make_declared_agent(
    name: str,
    declarations: dict[str, dict[str, Any]],
    *,
    model: str | None = None,
    provider_binding: ProviderBinding | None = None,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Build the fenced ModelRunner for one declared executor."""
    if name not in declarations:
        raise ValueError(f"unknown executor: {name!r}; declared: {sorted(declarations)}")
    declaration = declarations[name]
    base_url = None
    if provider_binding is not None:
        if model is not None:
            raise ValueError("model and provider_binding are mutually exclusive")
        binding = _validate_provider_binding(provider_binding, agent=name)
        model, base_url = binding["model"], binding["base_url"]
    declared_argv(declaration, "", model=model, base_url=base_url)
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else execution_fences.DisabledExecutionFencePort()
    )
    usage_model = model or name

    def runner(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        del workspace  # the clone is the descriptor's; the fence enforces it
        descriptor = _adapter_descriptor(capsule)
        prompt_text = build_prompt(capsule)
        argv = declared_argv(declaration, prompt_text, model=model, base_url=base_url)
        input_binding_records = _input_binding_gate(capsule, prompt_text, argv, None, descriptor)
        proc = fence_port.launch(descriptor, argv, timeout_seconds=timeout_seconds)
        stdout = proc.stdout or ""
        if proc.returncode != 0:
            raise RuntimeError(f"{name} exited {proc.returncode}: {((proc.stderr or '') or stdout).strip()[-400:]}")
        if declaration["usage"] == "lh-usage-line/v1":
            usage = parse_usage_line(stdout, model=usage_model)
        else:
            usage = token_cost.unknown_usage(model=usage_model, reason="executor declares no usage protocol")
        if base_url is not None and usage.get("state") == token_cost.USAGE_MEASURED:
            # A bound endpoint may price differently, so token counts do not
            # imply the declared cost table.
            usage = token_cost.unknown_usage(model=usage_model,
                                             reason="provider binding has no verified usage/cost attribution")
        execution: dict[str, Any] = {"executor": name, "exit_code": proc.returncode}
        if base_url is not None:
            execution["provider_binding"] = {"runner": name, "model": model, "mode": "declared_argv"}
        result = {"summary": f"{name} executor completed", "stdout_tail": stdout[-800:],
                  "usage": usage, "execution": execution}
        if input_binding_records is not None:
            result.update(input_binding_records)
        return result

    return execution_fences.mark_mutation_adapter(runner, adapter_id=f"declared-{name}")

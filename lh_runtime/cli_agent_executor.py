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
import shlex
import shutil
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
import execution_host_adapter as host_adapters
import provider_input_binding as provider_inputs
from provider_registry import is_kimi_executable
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

# Worker-process replay wall for provider-input binding nonces.
_SEEN_INPUT_NONCES: set[str] = set()


def _refuse_kimi(*values: Any) -> None:
    if any(is_kimi_executable(value) for value in values):
        raise ValueError("kimi_retired: new provider execution refused")


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

ORCA_OUTPUT_LIMIT = 800
ORCA_CONTROL_TIMEOUT_SECONDS = 30.0
ORCA_BINDING_PROVIDER_ID = "lh_terminal"
WSL_HOST_KIND_ENV = "ORCA_ORCHESTRATION_COMPATIBILITY_HOST_KIND"


def _wsl_unc_path(workspace: Path) -> str | None:
    """Translate a WSL-local path to the ``\\\\wsl.localhost`` UNC form Orca stores.

    Only consulted when this process runs inside WSL alongside a
    Windows-hosted Orca app (``WSL_HOST_KIND_ENV`` == "wsl"); a native Linux
    Orca host stores/accepts the plain POSIX path and never reaches here.
    """
    distro = os.environ.get("WSL_DISTRO_NAME")
    if not distro:
        return None
    wslpath = shutil.which("wslpath")
    if wslpath:
        try:
            completed = subprocess.run(
                [wslpath, "-w", str(workspace)],
                capture_output=True, text=True, timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            completed = None
        if completed is not None and completed.returncode == 0:
            converted = completed.stdout.strip()
            if converted:
                return converted
    tail = str(workspace).lstrip("/").replace("/", "\\")
    return f"\\\\wsl.localhost\\{distro}\\{tail}"


def _orca_worktree_selector(workspace: Path) -> str:
    """Build the ``--worktree`` selector this Orca host actually resolves.

    A native Linux Orca runtime resolves the POSIX path directly (unchanged
    default). A Windows-hosted Orca app reached from inside WSL stores and
    matches worktree paths only in ``\\\\wsl.localhost\\<distro>\\...`` UNC
    form; the POSIX form fails closed there with ``selector_not_found``.
    """
    return host_adapters.PathCodec.for_environment().selector(workspace)


def _orca_repo_path_matches(repo_path: Any, workspace: Path) -> bool:
    """Match a registered repo's stored path against the disposable clone.

    Orca may have normalized the path (e.g. the WSL UNC form above); accept
    either representation instead of only the exact POSIX string.
    """
    if not isinstance(repo_path, str):
        return False
    if repo_path == str(workspace):
        return True
    unc = _wsl_unc_path(workspace)
    return unc is not None and repo_path == unc


class OrcaCommandError(RuntimeError):
    """An Orca CLI command returned a structured ``ok: false`` result."""

    def __init__(self, command: str, code: str, message: str):
        super().__init__(f"orca {command}: {code}: {message}")
        self.code = code
        self.message = message


def _usage_hooks(agent: str) -> tuple[UsageCollector | None, SnapshotFn | None]:
    """Use the underlying provider collector; Orca is not a token authority."""
    if agent == "codex":
        return codex_usage.collector, codex_usage.snapshot
    # A custom adapter may deliberately provide no local usage reader.  The
    # caller still has to name the provider and supply its argv builder.
    return None, None


def resolve_orca_cli() -> str:
    """Resolve Orca IDE CLI, preferring the user-local install over GNOME Orca."""
    return host_adapters.discover_orca_cli()


def _orca_json(
    orca_cli: str,
    request: dict[str, Any],
    *,
    cwd: Path,
    timeout_seconds: float,
    execution_fence_port: execution_fences.ExecutionFencePort,
    execution_fence_descriptor: dict[str, Any],
    allow_nonzero: bool = False,
) -> dict[str, Any]:
    del cwd
    normalized = host_adapters.validate_control_request(request)
    args = execution_fences.compose_control_argv(normalized)
    proc = execution_fence_port.launch_control(
        execution_fence_descriptor,
        {**normalized, "orca_cli": orca_cli},
        timeout_seconds=timeout_seconds,
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        if proc.returncode != 0 and not allow_nonzero:
            raise RuntimeError(f"orca {' '.join(args[:2])} exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        raise RuntimeError(f"orca {' '.join(args[:2])} returned invalid JSON: {proc.stdout[-400:]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"orca {' '.join(args[:2])} returned a non-object JSON result")
    if payload.get("ok") is False:
        error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
        code = error.get("code") if isinstance(error.get("code"), str) else "unknown"
        message = error.get("message") if isinstance(error.get("message"), str) else "command failed"
        raise OrcaCommandError(" ".join(args[:2]), code, message)
    if proc.returncode != 0 and not allow_nonzero:
        raise RuntimeError(f"orca {' '.join(args[:2])} exited {proc.returncode}: {proc.stderr.strip()[:400]}")
    result = payload.get("result")
    return result if isinstance(result, dict) else payload


def _adapter_descriptor(capsule: dict[str, Any]) -> dict[str, Any]:
    """The controller owns descriptor preparation; an adapter never self-prepares."""
    descriptor = capsule.get("execution_fence")
    if isinstance(descriptor, dict):
        return descriptor
    raise execution_fences.ExecutionFenceUnavailable("descriptor_missing")


def _validate_provider_binding(binding: Any, *, agent: str) -> ProviderBinding:
    """Validate the explicit provider tuple before a terminal is created.

    A binding is intentionally not read from ambient environment: callers must
    pass the complete tuple for the individual run.  Credentials are not part
    of this object and URLs with userinfo are rejected so a binding cannot
    smuggle a secret into a terminal command or receipt.
    """
    if not isinstance(binding, dict) or set(binding) != {"runner", "base_url", "model"}:
        raise ValueError("provider_binding must contain exactly runner, base_url, and model")
    values = {key: binding.get(key) for key in ("runner", "base_url", "model")}
    if any(not isinstance(value, str) or not value.strip() for value in values.values()):
        raise ValueError("provider_binding fields must be non-empty strings")
    runner = values["runner"].strip()
    _refuse_kimi(agent, runner)
    base_url = values["base_url"].strip().rstrip("/")
    model = values["model"].strip()
    if runner != agent:
        raise ValueError(f"provider_binding.runner {runner!r} does not match Orca provider {agent!r}")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("provider_binding.base_url must be a credential-free absolute http(s) URL")
    if any(character.isspace() for character in model):
        raise ValueError("provider_binding.model must not contain whitespace")
    return {"runner": runner, "base_url": base_url, "model": model}


def _bind_orca_provider_argv(agent: str, provider_argv: list[str], binding: Any) -> tuple[list[str], dict[str, str], dict[str, str] | None]:
    """Apply only a proven provider-side channel inside one Orca terminal.

    Orca's terminal-create API exposes a command string but no key/value env
    option.  The wrapper therefore scopes an env overlay to its child shell,
    while Codex uses its documented per-invocation config flags. Kimi is retired.
    """
    _refuse_kimi(agent, provider_argv[0] if provider_argv else None)
    if binding is None:
        return provider_argv, {}, None
    normalized = _validate_provider_binding(binding, agent=agent)
    if agent == "codex":
        if len(provider_argv) < 2 or provider_argv[1] != "exec":
            raise ValueError("Codex Orca provider argv must begin with 'codex exec'")
        base_url = json.dumps(normalized["base_url"])
        provider_config = (
            f'model_providers.{ORCA_BINDING_PROVIDER_ID}={{name="LH per-terminal",'
            f"base_url={base_url},wire_api=\"responses\"}}"
        )
        bound_argv = [
            *provider_argv[:2],
            "-c", provider_config,
            "-c", f'model_provider="{ORCA_BINDING_PROVIDER_ID}"',
            "-m", normalized["model"],
            *provider_argv[2:],
        ]
        return bound_argv, {}, {"runner": agent, "model": normalized["model"], "mode": "codex_argv"}
    raise ValueError(f"Orca provider binding needs an explicit adapter for agent {agent!r}")


def _orca_command(provider_argv: list[str], *, output_path: Path | None = None, env_overlay: dict[str, str] | None = None) -> str:
    if not provider_argv:
        raise ValueError("provider argv must not be empty")
    executable = Path(provider_argv[0])
    env_prefix = " ".join(
        f"{name}={shlex.quote(value)}"
        for name, value in sorted((env_overlay or {}).items())
    )
    prefix = f"PATH={shlex.quote(str(executable.parent))}:$PATH"
    if env_prefix:
        prefix = f"{env_prefix} {prefix}"
    command = f"{prefix} exec {shlex.join(provider_argv)}"
    if output_path is None:
        return command
    captured = shlex.quote(str(output_path))
    # Orca's PTY may not retain child stdout after the process exits. Capture
    # the provider's JSONL stream inside the disposable clone and replay it;
    # LH reads the same transient file directly before disposing the workspace.
    return f"({command}) > {captured} 2>&1; status=$?; cat {captured}; exit $status"


def make_orca_agent(
    agent: str, *, timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    usage_parser: UsageParser | None = None, usage_collector: UsageCollector | None = None,
    snapshot_fn: SnapshotFn | None = None, orca_cli: str | None = None,
    provider_argv_builder: ArgvBuilder | None = None, output_limit: int = ORCA_OUTPUT_LIMIT,
    provider_binding: ProviderBinding | None = None, model: str | None = None,
    execution_fence_port: execution_fences.ExecutionFencePort | None = None,
    execution_host_adapter: host_adapters.OrcaExecutionHostAdapter | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Run one model adapter in an Orca terminal inside LH's existing clone."""
    _refuse_kimi(agent)
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if provider_binding is not None and model is not None:
        raise ValueError("model and provider_binding are mutually exclusive")
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else execution_fences.DisabledExecutionFencePort()
    )
    selected_model = model
    builders: dict[str, ArgvBuilder] = {
        name: (
            lambda prompt, selected=name: hosted_provider_argv(
                selected,
                prompt,
                selected_model,
            )
        )
        for name in ("codex",)
    }
    if provider_argv_builder is None:
        try:
            provider_argv_builder = builders[agent]
        except KeyError as exc:
            raise ValueError(f"unknown Orca-hosted agent: {agent!r}") from exc
    default_collector, default_snapshot = _usage_hooks(agent)
    if agent == "codex" and usage_parser is None and usage_collector is None:
        # Orca-hosted Codex uses --ephemeral --json below. Its stdout is the
        # only trustworthy per-invocation usage boundary when an outer Codex
        # agent shares the same CODEX_HOME/session tree.
        usage_parser = codex_usage.extract_usage_from_jsonl
        snapshot_fn = None
    else:
        usage_collector = usage_collector if usage_collector is not None else (None if usage_parser is not None else default_collector)
        snapshot_fn = snapshot_fn if snapshot_fn is not None else default_snapshot

    def control_call(
        orca: str,
        request: dict[str, Any],
        *,
        cwd: Path,
        descriptor: dict[str, Any],
        session: host_adapters.AttemptControlSession | None = None,
        allow_nonzero: bool = False,
    ) -> dict[str, Any]:
        if session is not None:
            return session.call(request)
        return _orca_json(
            orca,
            request,
            cwd=cwd,
            timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
            execution_fence_port=fence_port,
            execution_fence_descriptor=descriptor,
            allow_nonzero=allow_nonzero,
        )

    def ensure_workspace_registration(
        orca: str,
        workspace: Path,
        descriptor: dict[str, Any],
        session: host_adapters.AttemptControlSession | None = None,
    ) -> tuple[str | None, bool]:
        """Register an LH clone only when Orca cannot resolve its path.

        Orca terminals can target external Git worktrees that the runtime
        already discovers.  LH's controller normally creates an independent
        disposable clone instead, so the fallback imports that clone as a
        transient Orca project setup.  The caller owns cleanup of a setup it
        created; pre-existing registrations are never deleted here.
        """
        listed = control_call(
            orca, {"op": "repo_list"}, cwd=workspace,
            descriptor=descriptor, session=session,
        )
        repos = listed.get("repos") if isinstance(listed.get("repos"), list) else []
        for repo in repos:
            if isinstance(repo, dict) and _orca_repo_path_matches(repo.get("path"), workspace):
                repo_id = repo.get("id")
                return (repo_id, False) if isinstance(repo_id, str) and repo_id else (None, False)
        added = control_call(
            orca, {"op": "repo_add", "path": str(workspace)}, cwd=workspace,
            descriptor=descriptor, session=session,
        )
        repo = added.get("repo") if isinstance(added.get("repo"), dict) else {}
        repo_id = repo.get("id")
        if not isinstance(repo_id, str) or not repo_id.strip():
            raise RuntimeError("Orca repo add returned no repository id")
        return repo_id, True

    def cleanup_workspace_registration(
        orca: str,
        repo_id: str | None,
        owned: bool,
        workspace: Path,
        descriptor: dict[str, Any],
        session: host_adapters.AttemptControlSession | None = None,
    ) -> None:
        if not owned or not repo_id:
            return
        try:
            control_call(
                orca, {"op": "project_setup_delete", "setup": repo_id},
                cwd=workspace, descriptor=descriptor, session=session,
            )
        except Exception:
            # Registration cleanup is best effort; the LH receipt must retain
            # the provider outcome even if Orca is going away concurrently.
            pass

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        descriptor = _adapter_descriptor(capsule)
        prompt_text = build_prompt(capsule)
        provider_argv = list(provider_argv_builder(prompt_text))
        _refuse_kimi(provider_argv[0] if provider_argv else None)
        provider_argv, provider_env, binding_projection = _bind_orca_provider_argv(agent, provider_argv, provider_binding)
        provider_argv[0] = resolve_cli(provider_argv[0])
        input_binding_records = _input_binding_gate(
            capsule, prompt_text, provider_argv, provider_env, descriptor)
        started_at = time.time()
        snap: dict[str, Any] | None = None
        if snapshot_fn is not None:
            try:
                baseline = snapshot_fn()
                snap = baseline if isinstance(baseline, dict) else None
            except Exception:
                snap = None
        resolved_orca = orca_cli or resolve_orca_cli()
        repo_id: str | None = None
        owns_registration = False
        provider_output_path = workspace / ".lh-orca-provider-output.jsonl"
        control_session: host_adapters.AttemptControlSession | None = None
        if execution_host_adapter is not None:
            def host_transport(request: dict[str, Any]) -> dict[str, Any]:
                control_timeout = ORCA_CONTROL_TIMEOUT_SECONDS
                if request.get("op") == "terminal_wait":
                    # Orca's wait timeout is the remote wait window.  Keep the
                    # extra control margin used by the legacy direct path so
                    # the local subprocess cannot kill a long-running Attempt
                    # before Orca reports its requested wait result.
                    control_timeout += int(request["timeout_ms"]) / 1000.0
                return _orca_json(
                    resolved_orca,
                    request,
                    cwd=workspace,
                    timeout_seconds=control_timeout,
                    execution_fence_port=fence_port,
                    execution_fence_descriptor=descriptor,
                    allow_nonzero=request.get("op") == "terminal_wait",
                )

            control_session = execution_host_adapter.begin_attempt(
                orca_cli=resolved_orca,
                transport=host_transport,
                workspace=workspace,
                run_id=str(capsule.get("run_id") or ""),
                attempt=int(capsule.get("attempt") or 0),
            )
        create_request = {
            "op": "terminal_create",
            "worktree_selector": _orca_worktree_selector(workspace),
            "title": f"LH {agent} attempt {capsule.get('run_id')}#{capsule.get('attempt')}",
            # The fence composes the terminal command itself; an adapter never
            # passes a free-form --command string (B-line packet constraint 2).
            "provider_argv": provider_argv,
            "output_path": str(provider_output_path),
            "env_overlay": provider_env,
        }
        try:
            try:
                created = control_call(
                    resolved_orca, create_request, cwd=workspace,
                    descriptor=descriptor, session=control_session,
                )
            except OrcaCommandError as exc:
                if exc.code != "selector_not_found":
                    raise
                repo_id, owns_registration = ensure_workspace_registration(
                    resolved_orca,
                    workspace,
                    descriptor,
                    control_session,
                )
                created = control_call(
                    resolved_orca, create_request, cwd=workspace,
                    descriptor=descriptor, session=control_session,
                )
        except Exception:
            cleanup_workspace_registration(
                resolved_orca,
                repo_id,
                owns_registration,
                workspace,
                descriptor,
                control_session,
            )
            raise
        terminal = created.get("terminal") if isinstance(created.get("terminal"), dict) else {}
        handle = terminal.get("handle")
        if not isinstance(handle, str) or not handle.strip():
            cleanup_workspace_registration(
                resolved_orca,
                repo_id,
                owns_registration,
                workspace,
                descriptor,
                control_session,
            )
            raise RuntimeError("Orca terminal create returned no runtime handle")
        wait_payload: dict[str, Any] | None = None
        read_error: str | None = None
        stdout = ""
        try:
            waited = control_call(
                resolved_orca,
                {"op": "terminal_wait", "handle": handle,
                 "timeout_ms": max(1, int(timeout_seconds * 1000))},
                cwd=workspace, descriptor=descriptor, session=control_session,
                allow_nonzero=True,
            )
            wait_payload = waited.get("wait") if isinstance(waited.get("wait"), dict) else None
            if not isinstance(wait_payload, dict) or wait_payload.get("satisfied") is not True:
                raise TimeoutError(f"Orca terminal did not exit within {timeout_seconds}s: {wait_payload}")
            try:
                captured_stdout = provider_output_path.read_text(encoding="utf-8")
                if captured_stdout:
                    stdout = captured_stdout
            except OSError:
                pass
            try:
                tail_request = {"op": "terminal_read", "handle": handle,
                                "limit": max(1, int(output_limit))}
                read_payload = control_call(
                    resolved_orca, tail_request,
                    cwd=workspace, descriptor=descriptor, session=control_session,
                )
                read_terminal = read_payload.get("terminal") if isinstance(read_payload.get("terminal"), dict) else {}
                tail = read_terminal.get("tail")
                if isinstance(tail, list) and tail:
                    stdout = "\n".join(str(line) for line in tail)
            except Exception as exc:
                read_error = f"{type(exc).__name__}: {exc}"
        except (subprocess.TimeoutExpired, TimeoutError):
            try:
                control_call(
                    resolved_orca,
                    {"op": "terminal_stop", "worktree_selector": _orca_worktree_selector(workspace)},
                    cwd=workspace, descriptor=descriptor, session=control_session,
                )
            except Exception:
                pass
            raise
        finally:
            try:
                control_call(
                    resolved_orca,
                    {"op": "terminal_close", "handle": handle},
                    cwd=workspace, descriptor=descriptor, session=control_session,
                )
            except Exception:
                pass
            try:
                provider_output_path.unlink()
            except OSError:
                pass
            cleanup_workspace_registration(
                resolved_orca,
                repo_id,
                owns_registration,
                workspace,
                descriptor,
                control_session,
            )
        exit_code = wait_payload.get("exitCode") if isinstance(wait_payload, dict) else None
        if not isinstance(exit_code, int):
            raise RuntimeError(f"Orca terminal returned no integer exitCode: {wait_payload}")
        completed = subprocess.CompletedProcess(provider_argv, exit_code, stdout, "")
        usage: dict[str, Any] | None = None
        try:
            if usage_collector is not None:
                usage = usage_collector(completed, {"started_at": started_at, "capsule": capsule, "snapshot": snap})
            elif usage_parser is not None:
                usage = usage_parser(stdout)
        except Exception:
            usage = None
        if binding_projection is not None and isinstance(usage, dict) and usage.get("state") == token_cost.USAGE_MEASURED:
            # A per-terminal endpoint may be a relay with different pricing or
            # billing semantics.  The local provider parser proves token
            # counts, but not that the provider's default pricing table applies
            # to this binding.  Keep AC4 blocked until endpoint-specific
            # attribution is supplied rather than emitting a misleading cost.
            usage = token_cost.unknown_usage(
                model=str(usage.get("model") or binding_projection.get("model") or agent),
                reason="per-terminal provider binding has no verified usage/cost attribution",
            )
        if not isinstance(usage, dict) or usage.get("state") not in {token_cost.USAGE_MEASURED, token_cost.USAGE_UNKNOWN}:
            usage = token_cost.unknown_usage(model=agent, reason="Orca-hosted provider usage unavailable")
        if exit_code != 0:
            raise RuntimeError(f"{agent} via Orca exited {exit_code}: {stdout[-400:]}")
        execution: dict[str, Any] = {"backend": "orca", "agent": agent, "terminal_handle": handle, "exit_code": exit_code}
        if binding_projection is not None:
            execution["provider_binding"] = binding_projection
        if read_error is not None:
            execution["read_error"] = read_error
        result = {"summary": f"{agent} executor completed via Orca terminal", "stdout_tail": stdout[-800:], "usage": usage, "execution": execution}
        if input_binding_records is not None:
            result.update(input_binding_records)
        return result

    return execution_fences.mark_mutation_adapter(
        model,
        adapter_id=f"orca-{agent}",
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
    _refuse_kimi(name)
    fence_port = (
        execution_fence_port
        if execution_fence_port is not None
        else execution_fences.DisabledExecutionFencePort()
    )

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        descriptor = _adapter_descriptor(capsule)
        prompt_text = build_prompt(capsule)
        argv = list(argv_builder(prompt_text))
        _refuse_kimi(argv[0] if argv else None)
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
    _refuse_kimi(binary)
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
    _refuse_kimi(name, argv[0])
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
    if isinstance(provider, dict):
        _refuse_kimi(provider.get("provider_id"), provider.get("executable"))
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


def codex_orca_argv(prompt: str) -> list[str]:
    return ["codex", "exec", "--ephemeral", "--json", "--dangerously-bypass-approvals-and-sandbox", prompt]


def kimi_argv(prompt: str) -> list[str]:
    raise ValueError("kimi_retired: argv construction refused")


def hosted_provider_argv(
    agent: str,
    prompt: str,
    model: str | None = None,
) -> list[str]:
    """Build provider argv for an Orca terminal without making Orca a model."""
    _refuse_kimi(agent)
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
    """Model-pinned argv for an *executing* provider (Orca terminal, named CLI agent).

    Carries the provider's broad flags because the kernel fence, not the argv,
    is what bounds a mutation run. This is the shape `judge_argv` used to have
    before D3 split the two postures; nothing on the executor side changed.
    """
    _refuse_kimi(executor)
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
    no-write argv shape. This core supplies Codex and AGY shapes; Kimi is
    retired and an unregistered provider fails closed.
    """
    _refuse_kimi(executor)
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
    _refuse_kimi(executor)
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
    _refuse_kimi(name)
    if name == "orca":
        if model is not None:
            raise ValueError("Orca model selection must use a complete provider_binding")
        agent = (
            provider_binding["runner"]
            if isinstance(provider_binding, dict) and isinstance(provider_binding.get("runner"), str)
            else os.environ.get("LH_ORCA_AGENT", "").strip()
        )
        if not agent:
            raise ValueError("Orca provider is not configured; set LH_ORCA_AGENT or pass provider_binding")
        return make_orca_agent(
            agent=agent,
            provider_binding=provider_binding,
            timeout_seconds=timeout_seconds,
            execution_fence_port=execution_fence_port,
        )
    if provider_binding is not None:
        raise ValueError("provider_binding is supported only by the Orca execution host adapter")
    builders: dict[str, ArgvBuilder] = {
        "codex": codex_argv,
    }
    if name not in builders:
        raise ValueError(f"unknown CLI adapter: {name!r}; choose one of {sorted([*builders, 'orca'])}")
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
def KIMI(**kwargs: Any) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Retained import boundary; no new Kimi adapter may be constructed."""
    raise ValueError("kimi_retired: adapter construction refused")


def _configured_orca(**kwargs: Any) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    agent = os.environ.get("LH_ORCA_AGENT", "").strip()
    if not agent:
        raise ValueError("Orca provider is not configured; set LH_ORCA_AGENT explicitly")
    return make_orca_agent(agent=agent, **kwargs)


ORCA = _configured_orca

#!/usr/bin/env python3
"""Generic CLI-agent executor: turn ANY coding-agent CLI into a spine ModelRunner.

Model-agnostic by design — the same adapter drives Codex, Claude Code, or any other
non-interactive agent CLI; only the argv builder changes. The agent does the real work
inside the controller's disposable clone (the loop's isolation IS the sandbox); the
spine still owns state, the deterministic verifier, retry, and recovery.
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import token_cost
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

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
    if agent == "claude":
        return claude_usage.collector, claude_usage.snapshot
    if agent == "kimi":
        return kimi_usage.collector, kimi_usage.snapshot
    raise ValueError(f"unknown Orca-hosted agent: {agent!r}; choose codex, claude, or kimi")


def resolve_orca_cli() -> str:
    """Resolve Orca IDE CLI, preferring the user-local install over GNOME Orca."""
    explicit = os.environ.get("LH_ORCA_CLI")
    if explicit:
        parts = shlex.split(explicit)
        if len(parts) != 1:
            raise ValueError("LH_ORCA_CLI must contain one executable path or name")
        return resolve_cli(parts[0])
    local_bin = Path.home() / ".local" / "bin"
    candidates = [local_bin / "orca", local_bin / "orca-ide"]
    candidates.extend(sorted(local_bin.glob("orca-ide-*"), reverse=True))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    for binary in ("orca-ide", "orca"):
        found = shutil.which(binary)
        if found:
            return found
    raise FileNotFoundError("Orca IDE CLI not found; set LH_ORCA_CLI")


def _orca_json(orca_cli: str, args: list[str], *, cwd: Path, timeout_seconds: float, allow_nonzero: bool = False) -> dict[str, Any]:
    proc = subprocess.run(
        [orca_cli, *args], cwd=cwd, capture_output=True, text=True, timeout=timeout_seconds,
        env={**os.environ, "PATH": f"{Path(orca_cli).parent}:{os.environ.get('PATH', '')}"},
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
    while Codex uses its documented per-invocation config flags.  Kimi's known
    global registry is deliberately rejected rather than silently using it.
    """
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
    if agent == "claude":
        bound_argv = [provider_argv[0], "--model", normalized["model"], *provider_argv[1:]]
        return bound_argv, {"ANTHROPIC_BASE_URL": normalized["base_url"]}, {"runner": agent, "model": normalized["model"], "mode": "claude_env"}
    if agent == "kimi":
        raise ValueError("Kimi does not support non-default per-process provider binding; refusing ambient registry fallback")
    raise ValueError(f"Orca provider binding is unsupported for agent {agent!r}")


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
    agent: str = "codex", *, timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    usage_parser: UsageParser | None = None, usage_collector: UsageCollector | None = None,
    snapshot_fn: SnapshotFn | None = None, orca_cli: str | None = None,
    provider_argv_builder: ArgvBuilder | None = None, output_limit: int = ORCA_OUTPUT_LIMIT,
    provider_binding: ProviderBinding | None = None, model: str | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Run one model adapter in an Orca terminal inside LH's existing clone."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if provider_binding is not None and model is not None:
        raise ValueError("model and provider_binding are mutually exclusive")
    selected_model = model
    builders: dict[str, ArgvBuilder] = {
        name: (
            lambda prompt, selected=name: hosted_provider_argv(
                selected,
                prompt,
                selected_model,
            )
        )
        for name in ("codex", "claude", "kimi")
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

    def ensure_workspace_registration(orca: str, workspace: Path) -> tuple[str | None, bool]:
        """Register an LH clone only when Orca cannot resolve its path.

        Orca terminals can target external Git worktrees that the runtime
        already discovers.  LH's controller normally creates an independent
        disposable clone instead, so the fallback imports that clone as a
        transient Orca project setup.  The caller owns cleanup of a setup it
        created; pre-existing registrations are never deleted here.
        """
        listed = _orca_json(
            orca, ["repo", "list", "--json"], cwd=workspace,
            timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
        )
        repos = listed.get("repos") if isinstance(listed.get("repos"), list) else []
        for repo in repos:
            if isinstance(repo, dict) and repo.get("path") == str(workspace):
                repo_id = repo.get("id")
                return (repo_id, False) if isinstance(repo_id, str) and repo_id else (None, False)
        added = _orca_json(
            orca, ["repo", "add", "--path", str(workspace), "--json"], cwd=workspace,
            timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
        )
        repo = added.get("repo") if isinstance(added.get("repo"), dict) else {}
        repo_id = repo.get("id")
        if not isinstance(repo_id, str) or not repo_id.strip():
            raise RuntimeError("Orca repo add returned no repository id")
        return repo_id, True

    def cleanup_workspace_registration(orca: str, repo_id: str | None, owned: bool, workspace: Path) -> None:
        if not owned or not repo_id:
            return
        try:
            _orca_json(
                orca, ["project", "setup-delete", "--setup", repo_id, "--json"],
                cwd=workspace, timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
            )
        except Exception:
            # Registration cleanup is best effort; the LH receipt must retain
            # the provider outcome even if Orca is going away concurrently.
            pass

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        provider_argv = list(provider_argv_builder(build_prompt(capsule)))
        provider_argv, provider_env, binding_projection = _bind_orca_provider_argv(agent, provider_argv, provider_binding)
        provider_argv[0] = resolve_cli(provider_argv[0])
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
        create_args = [
            "terminal", "create", "--worktree", f"path:{workspace}", "--title",
            f"LH {agent} attempt {capsule.get('run_id')}#{capsule.get('attempt')}",
            "--command", _orca_command(provider_argv, output_path=provider_output_path, env_overlay=provider_env), "--json",
        ]
        try:
            try:
                created = _orca_json(
                    resolved_orca, create_args, cwd=workspace,
                    timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
                )
            except OrcaCommandError as exc:
                if exc.code != "selector_not_found":
                    raise
                repo_id, owns_registration = ensure_workspace_registration(resolved_orca, workspace)
                created = _orca_json(
                    resolved_orca, create_args, cwd=workspace,
                    timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
                )
        except Exception:
            cleanup_workspace_registration(resolved_orca, repo_id, owns_registration, workspace)
            raise
        terminal = created.get("terminal") if isinstance(created.get("terminal"), dict) else {}
        handle = terminal.get("handle")
        if not isinstance(handle, str) or not handle.strip():
            cleanup_workspace_registration(resolved_orca, repo_id, owns_registration, workspace)
            raise RuntimeError("Orca terminal create returned no runtime handle")
        wait_payload: dict[str, Any] | None = None
        read_error: str | None = None
        stdout = ""
        try:
            waited = _orca_json(
                resolved_orca,
                ["terminal", "wait", "--terminal", handle, "--for", "exit", "--timeout-ms",
                 str(max(1, int(timeout_seconds * 1000))), "--json"],
                cwd=workspace, timeout_seconds=timeout_seconds + ORCA_CONTROL_TIMEOUT_SECONDS, allow_nonzero=True,
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
                read_payload = _orca_json(
                    resolved_orca, ["terminal", "read", "--terminal", handle, "--limit", str(max(1, int(output_limit))), "--json"],
                    cwd=workspace, timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS,
                )
                read_terminal = read_payload.get("terminal") if isinstance(read_payload.get("terminal"), dict) else {}
                tail = read_terminal.get("tail")
                if isinstance(tail, list) and tail:
                    stdout = "\n".join(str(line) for line in tail)
            except Exception as exc:
                read_error = f"{type(exc).__name__}: {exc}"
        except (subprocess.TimeoutExpired, TimeoutError):
            try:
                _orca_json(resolved_orca, ["terminal", "stop", "--worktree", f"path:{workspace}", "--json"], cwd=workspace, timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS)
            except Exception:
                pass
            raise
        finally:
            try:
                _orca_json(resolved_orca, ["terminal", "close", "--terminal", handle, "--json"], cwd=workspace, timeout_seconds=ORCA_CONTROL_TIMEOUT_SECONDS)
            except Exception:
                pass
            try:
                provider_output_path.unlink()
            except OSError:
                pass
            cleanup_workspace_registration(resolved_orca, repo_id, owns_registration, workspace)
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
        return {"summary": f"{agent} executor completed via Orca terminal", "stdout_tail": stdout[-800:], "usage": usage, "execution": execution}

    return model


def build_prompt(capsule: dict[str, Any]) -> str:
    goal = capsule.get("goal", {})
    bootstrap = capsule.get("bootstrap_authority")
    bootstrap_text = (
        "\n\nBOOTSTRAP AUTHORITY (routing facts only; target repo authority still wins):\n"
        + json.dumps(bootstrap, ensure_ascii=False, indent=2)
        if isinstance(bootstrap, dict)
        else ""
    )
    return (
        f"You are the executor in an automated loop, attempt #{capsule.get('attempt')}.\n"
        f"Repository CWD is a disposable clone at base revision {capsule.get('base_revision')}.\n\n"
        f"GOAL (satisfy exactly this, nothing more):\n{json.dumps(goal, ensure_ascii=False, indent=2)}"
        f"{bootstrap_text}\n\n"
        "Make the minimal change in this repo to satisfy the goal. Add/adjust tests only if the goal needs them. "
        "Do NOT git commit, push, or touch anything outside this repo. When done, stop."
    )


def make_cli_agent(
    argv_builder: ArgvBuilder,
    *,
    name: str,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    usage_parser: UsageParser | None = None,
    usage_collector: UsageCollector | None = None,
    snapshot_fn: SnapshotFn | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        argv = argv_builder(build_prompt(capsule))
        argv[0] = resolve_cli(argv[0])
        started_at = time.time()
        snap: dict[str, Any] | None = None
        if snapshot_fn is not None:
            try:
                baseline = snapshot_fn()
                snap = baseline if isinstance(baseline, dict) else None
            except Exception:  # A snapshot failure must not block dispatch; the collector degrades.
                snap = None
        # The resolved CLI may itself need its runtime neighbours (e.g. an nvm
        # node script whose shebang is `/usr/bin/env node`); under systemd/cron
        # the ambient PATH is minimal, so put the CLI's own bin dir first.
        env = dict(os.environ)
        env["PATH"] = f"{Path(argv[0]).parent}:{env.get('PATH', '')}"
        proc = subprocess.run(argv, cwd=workspace, capture_output=True, text=True, timeout=timeout_seconds, env=env)
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
        return {"summary": f"{name} executor completed", "stdout_tail": proc.stdout[-800:], "usage": usage}
    return model


def resolve_cli(binary: str) -> str:
    """Resolve an executor CLI to an absolute path.

    The resident driver runs under systemd/cron with a minimal PATH, so a
    bare ``codex`` is not found even though a login shell finds it.  Probe
    PATH first, then the standard per-user install locations for agent CLIs
    (nvm node bin, ~/.local/bin, ~/.kimi-code/bin).  Raises FileNotFoundError
    with the searched locations when the CLI is genuinely absent."""
    from shutil import which

    found = which(binary)
    if found:
        return found
    home = Path.home()
    candidates: list[Path] = []
    nvm = home / ".nvm" / "versions" / "node"
    if nvm.is_dir():
        for version in sorted(nvm.iterdir(), reverse=True):
            candidates.append(version / "bin" / binary)
    candidates += [
        home / ".local" / "bin" / binary,
        home / ".kimi-code" / "bin" / binary,
        home / ".codex" / "bin" / binary,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    raise FileNotFoundError(f"executor CLI {binary!r} not found on PATH or in {[str(c) for c in candidates]}")


# Model-agnostic presets. The disposable clone is the sandbox, so full-auto is intended here.
def codex_argv(prompt: str) -> list[str]:
    return ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", prompt]


def codex_orca_argv(prompt: str) -> list[str]:
    return ["codex", "exec", "--ephemeral", "--json", "--dangerously-bypass-approvals-and-sandbox", prompt]


def claude_argv(prompt: str) -> list[str]:
    return ["claude", "-p", prompt, "--permission-mode", "bypassPermissions"]


def kimi_argv(prompt: str) -> list[str]:
    return ["kimi", "-p", prompt]


def hosted_provider_argv(
    agent: str,
    prompt: str,
    model: str | None = None,
) -> list[str]:
    """Build provider argv for an Orca terminal without making Orca a model."""
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
    return judge_argv(agent, prompt, model)


def judge_argv(executor: str, prompt: str, model: str | None = None) -> list[str]:
    """Argv for one bounded turning-point judgment call (M1 model routing).

    Unlike the executor argv builders, the judge may pin a specific model
    (e.g. a reasoning-tier model for judgment, a coding-tier model for
    execution) via the CLI's own model flag.
    """
    if executor == "codex":
        argv = ["codex", "exec"]
        if model:
            argv += ["-m", model]
        argv += ["--dangerously-bypass-approvals-and-sandbox", prompt]
    elif executor == "claude":
        argv = ["claude"]
        if model:
            argv += ["--model", model]
        argv += ["-p", prompt, "--permission-mode", "bypassPermissions"]
    elif executor == "kimi":
        argv = ["kimi"]
        if model:
            argv += ["-m", model]
        argv += ["-p", prompt]
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
    explicit read-only sandbox; Claude gets no tools and plan-only permission.
    Kimi currently has no equivalent enforceable no-tools flag, so it is not a
    capability evaluation adapter.
    """
    if executor == "codex":
        return [
            "codex", "exec", "-m", model,
            "--sandbox", "read-only",
            "--ephemeral",
            prompt,
        ]
    if executor == "claude":
        argv = [
            "claude", "--model", model,
            "-p", prompt,
            "--permission-mode", "plan",
            "--tools", "",
            "--no-session-persistence",
            "--safe-mode",
            "--output-format", "json",
        ]
        if json_schema is not None:
            argv += [
                "--json-schema",
                json.dumps(
                    json_schema,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ]
        return argv
    raise ValueError(f"no no-write evaluation adapter for executor: {executor!r}")


def make_named_cli_agent(
    name: str,
    *,
    model: str | None = None,
    provider_binding: ProviderBinding | None = None,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Build one runtime-selected CLI adapter without assigning it a role."""
    if name == "orca":
        if model is not None:
            raise ValueError("Orca model selection must use a complete provider_binding")
        agent = (
            provider_binding["runner"]
            if isinstance(provider_binding, dict) and isinstance(provider_binding.get("runner"), str)
            else os.environ.get("LH_ORCA_AGENT", "codex")
        )
        return make_orca_agent(
            agent=agent,
            provider_binding=provider_binding,
            timeout_seconds=timeout_seconds,
        )
    if provider_binding is not None:
        raise ValueError("provider_binding is supported only by the Orca execution host adapter")
    builders: dict[str, ArgvBuilder] = {
        "codex": codex_argv,
        "claude": claude_argv,
        "kimi": kimi_argv,
    }
    if name not in builders:
        raise ValueError(f"unknown CLI adapter: {name!r}; choose one of {sorted([*builders, 'orca'])}")
    builder = builders[name] if model is None else lambda prompt: judge_argv(name, prompt, model)
    collector, snapshot = _usage_hooks(name)
    return make_cli_agent(
        builder,
        name=name,
        timeout_seconds=timeout_seconds,
        usage_collector=collector,
        snapshot_fn=snapshot,
    )


import claude_usage  # noqa: E402
import codex_usage  # noqa: E402
import kimi_usage  # noqa: E402

CODEX = lambda **kw: make_cli_agent(codex_argv, name="codex", usage_collector=kw.pop("usage_collector", codex_usage.collector), snapshot_fn=kw.pop("snapshot_fn", codex_usage.snapshot), **kw)  # noqa: E731
CLAUDE = lambda **kw: make_cli_agent(claude_argv, name="claude", usage_collector=kw.pop("usage_collector", claude_usage.collector), snapshot_fn=kw.pop("snapshot_fn", claude_usage.snapshot), **kw)   # noqa: E731
KIMI = lambda **kw: make_cli_agent(kimi_argv, name="kimi", usage_collector=kw.pop("usage_collector", kimi_usage.collector), snapshot_fn=kw.pop("snapshot_fn", kimi_usage.snapshot), **kw)   # noqa: E731
ORCA = lambda **kw: make_orca_agent(agent=os.environ.get("LH_ORCA_AGENT", "codex"), **kw)  # noqa: E731

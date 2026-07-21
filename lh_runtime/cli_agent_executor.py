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
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

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


def build_prompt(capsule: dict[str, Any]) -> str:
    goal = capsule.get("goal", {})
    return (
        f"You are the executor in an automated loop, attempt #{capsule.get('attempt')}.\n"
        f"Repository CWD is a disposable clone at base revision {capsule.get('base_revision')}.\n\n"
        f"GOAL (satisfy exactly this, nothing more):\n{json.dumps(goal, ensure_ascii=False, indent=2)}\n\n"
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


def claude_argv(prompt: str) -> list[str]:
    return ["claude", "-p", prompt, "--permission-mode", "bypassPermissions"]


def kimi_argv(prompt: str) -> list[str]:
    return ["kimi", "-p", prompt]


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


import claude_usage  # noqa: E402
import codex_usage  # noqa: E402
import kimi_usage  # noqa: E402

CODEX = lambda **kw: make_cli_agent(codex_argv, name="codex", usage_collector=kw.pop("usage_collector", codex_usage.collector), snapshot_fn=kw.pop("snapshot_fn", codex_usage.snapshot), **kw)  # noqa: E731
CLAUDE = lambda **kw: make_cli_agent(claude_argv, name="claude", usage_collector=kw.pop("usage_collector", claude_usage.collector), snapshot_fn=kw.pop("snapshot_fn", claude_usage.snapshot), **kw)   # noqa: E731
KIMI = lambda **kw: make_cli_agent(kimi_argv, name="kimi", usage_collector=kw.pop("usage_collector", kimi_usage.collector), snapshot_fn=kw.pop("snapshot_fn", kimi_usage.snapshot), **kw)   # noqa: E731

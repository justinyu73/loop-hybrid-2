#!/usr/bin/env python3
"""ExecutionHostPort: the named seam host/Orca supplies for disposable-clone actuation.

Authority: ``docs/contracts/model-routing-v1.md#lh-capability-model-routing-002``
(inner ring: "LH -> host/Orca ExecutionHostPort -> target project -> LH") and
external host ``LH-EXTERNAL-BOOTSTRAP-001`` step 5-6: "LH selects the model binding.
host/Orca supplies a separate ExecutionHostPort for disposable worktree,
terminal, and CLI actuation... The admitted execution model starts only in
the disposable target clone...".

This module does not reimplement actuation.  ``cli_agent_executor.make_orca_agent``
already calls the real Orca CLI (``repo add``/``terminal create``/``wait``/
``read``/``close``) against the disposable clone LH's controller creates
(``controller.py:_workspace`` -- see ``lh-disposable-workspace/v1``); LH owns
that clone per ``docs/contracts/goal-lifecycle-v1.md`` ("LH owns durable Goal
state and the disposable execution clone pinned to base_revision"). What was
missing was a name for the port itself and a durable, externally-readable
signal colocated with the clone -- this repo's existing idiom (see the
sibling ``.git/lh-disposable-workspace.json`` marker) -- so anything outside
LH's own call stack (a human, an HOST-side tool, a future async redesign) can
observe that an Attempt requested host actuation and what the real Orca CLI
returned, without parsing LH's receipt schema.

``make_execution_host_port`` is a thin wrapper: it validates the binding, and
mirrors the durable pattern already exercised by the mandatory
``lh-orca-executor-cut5`` gate.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cli_agent_executor as executors
import execution_fence as execution_fences
import execution_host_adapter as host_adapters

REQUEST_MARKER_NAME = "lh-execution-host-request.json"
REQUEST_SCHEMA = "loop-hybrid-execution-host-request/v1"
RECEIPT_MARKER_NAME = "lh-execution-host-receipt.json"
RECEIPT_SCHEMA = "loop-hybrid-execution-host-receipt/v1"


def _marker_dir(workspace: Path) -> Path:
    """Colocate with the disposable clone's Git-root marker under ``.git/``.

    ``.git/`` is never part of ``allowed_paths`` for a produce_change diff and
    is disposed with the rest of the clone, matching the durability window of
    every other per-attempt marker this repo already writes here.  A target may
    be a subtree of a monorepo, so walk only its parents to find the clone's
    Git root; retain the local fallback for hermetic non-Git fixtures.
    """
    resolved = workspace.resolve()
    for candidate in (resolved, *resolved.parents):
        marker = candidate / ".git"
        if marker.is_dir():
            return marker
        if marker.is_file():
            try:
                line = marker.read_text(encoding="utf-8").splitlines()[0]
                prefix, _, value = line.partition(":")
                if prefix.strip().lower() == "gitdir" and value.strip():
                    gitdir = Path(value.strip())
                    if not gitdir.is_absolute():
                        gitdir = candidate / gitdir
                    return gitdir.resolve()
            except (OSError, IndexError):
                pass
    return workspace / ".git"


def write_execution_host_request(
    workspace: Path,
    *,
    run_id: str,
    attempt: int,
    agent: str,
    execution_host_binding: dict[str, Any],
) -> Path:
    """Durable, externally-readable signal: this Attempt now needs host actuation."""
    marker_dir = _marker_dir(workspace)
    marker_dir.mkdir(parents=True, exist_ok=True)
    body = {
        "schema": REQUEST_SCHEMA,
        "run_id": run_id,
        "attempt": attempt,
        "agent": agent,
        "host_id": execution_host_binding.get("host_id"),
        "bootstrap_authority": execution_host_binding.get("bootstrap_authority"),
        "requested_at": time.time(),
    }
    path = marker_dir / REQUEST_MARKER_NAME
    path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8", newline="")
    return path


def read_execution_host_request(workspace: Path) -> dict[str, Any] | None:
    path = _marker_dir(workspace) / REQUEST_MARKER_NAME
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def record_execution_host_receipt(
    workspace: Path,
    *,
    run_id: str,
    attempt: int,
    agent: str,
    result: dict[str, Any],
) -> Path:
    """Durable evidence: what the real Orca CLI actually returned for this Attempt.

    Reuses the repo's ``record_*`` naming idiom (see ``record_event``,
    ``record_grill_me_execute_override``) for the write side of a durable,
    externally-readable artifact; ``read_execution_host_receipt`` is its
    ``read_*`` counterpart.
    """
    marker_dir = _marker_dir(workspace)
    marker_dir.mkdir(parents=True, exist_ok=True)
    execution = result.get("execution") if isinstance(result.get("execution"), dict) else {}
    body = {
        "schema": RECEIPT_SCHEMA,
        "run_id": run_id,
        "attempt": attempt,
        "agent": agent,
        "backend": execution.get("backend"),
        "terminal_handle": execution.get("terminal_handle"),
        "exit_code": execution.get("exit_code"),
        "summary": result.get("summary"),
        "usage_state": (result.get("usage") or {}).get("state"),
        "recorded_at": time.time(),
    }
    path = marker_dir / RECEIPT_MARKER_NAME
    path.write_text(json.dumps(body, sort_keys=True), encoding="utf-8", newline="")
    return path


def read_execution_host_receipt(workspace: Path) -> dict[str, Any] | None:
    path = _marker_dir(workspace) / RECEIPT_MARKER_NAME
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def make_execution_host_port(
    *,
    agent: str,
    execution_host_binding: dict[str, Any],
    timeout_seconds: float,
    execution_fence_port: execution_fences.ExecutionFencePort,
    orca_cli: str | None = None,
    provider_argv_builder: executors.ArgvBuilder | None = None,
    provider_binding: executors.ProviderBinding | None = None,
    model: str | None = None,
) -> Callable[[Path, dict[str, Any]], dict[str, Any]]:
    """Build the named ExecutionHostPort ModelRunner for one Attempt's agent.

    Delegates all Orca CLI actuation to ``cli_agent_executor.make_orca_agent``
    (unchanged, already the mandatory ``lh-orca-executor-cut5`` gate); adds
    only the durable request/receipt markers around it.
    """
    if not isinstance(execution_host_binding, dict) or execution_host_binding.get("host_id") != "external-orca":
        raise ValueError("make_execution_host_port requires an external-orca execution_host_binding")
    host_adapter = host_adapters.OrcaExecutionHostAdapter()
    inner = executors.make_orca_agent(
        agent=agent,
        timeout_seconds=timeout_seconds,
        orca_cli=orca_cli,
        provider_argv_builder=provider_argv_builder,
        provider_binding=provider_binding,
        model=model,
        execution_fence_port=execution_fence_port,
        execution_host_adapter=host_adapter,
    )

    def port(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        run_id = str(capsule.get("run_id") or "unknown-run")
        attempt = int(capsule.get("attempt") or 0)
        write_execution_host_request(
            workspace,
            run_id=run_id,
            attempt=attempt,
            agent=agent,
            execution_host_binding=execution_host_binding,
        )
        result = inner(workspace, capsule)
        record_execution_host_receipt(
            workspace,
            run_id=run_id,
            attempt=attempt,
            agent=agent,
            result=result,
        )
        return result

    return execution_fences.mark_mutation_adapter(port, adapter_id=f"execution-host-port-{agent}")

#!/usr/bin/env python3
"""Run delivery checks and the independent verifier through the execution fence port.

``RunStore`` executes every delivery obligation command and the independent
verifier through its ``command_runner``.  The native-run path supplies one from
its execution binding; the compatibility path gets this one.  Each command is
its own launch: a fresh single-use descriptor from ``port.prepare`` and the
port's own environment projection.  The receipt carries the backend's own
projection, including what it does not contain -- never a fixture claim.

A bare command name resolves on the operator's PATH; a relative path resolves
inside the worktree.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import execution_fence as fences  # noqa: E402

ADAPTER_ID = "lh-delivery-command"
ADAPTER_VERSION = "v1"
def resolve_command(argv: Sequence[str], worktree: str) -> list[str]:
    """Resolve ``argv[0]`` to the executable the launch will run."""
    if not argv or not isinstance(argv[0], str) or not argv[0]:
        raise fences.ExecutionFenceUnavailable("delivery_command_argv_invalid")
    name = argv[0]
    if os.path.isabs(name):
        executable = name
    elif "/" in name or "\\" in name:
        executable = str((Path(worktree) / name).resolve())
    else:
        found = shutil.which(name)
        if found is None:
            raise fences.ExecutionFenceUnavailable(f"delivery_command_not_found:{name}")
        executable = found
    return [executable, *[str(item) for item in argv[1:]]]


class FenceCommandRunner:
    """``RunStore.command_runner`` backed by one ``ExecutionFencePort``."""

    def __init__(self, port: fences.ExecutionFencePort):
        if isinstance(port, fences.DisabledExecutionFencePort):
            raise fences.ExecutionFenceUnavailable(port.reason)
        self.port = port

    @property
    def backend(self) -> str:
        module = sys.modules.get(type(self.port).__module__)
        return str(getattr(self.port, "backend_id", None) or getattr(module, "BACKEND_ID", None)
                   or type(self.port).__name__)

    def __call__(
        self,
        request: Mapping[str, Any],
        *,
        phase: str,
        argv: Sequence[str],
        worktree: str,
        timeout_seconds: float,
        input_request: Mapping[str, Any] | None = None,
        writable: bool = False,
        on_started: Callable[[Any], None] | None = None,
        env: Mapping[str, str] | None = None,
    ):
        import json

        if not isinstance(request, Mapping):
            raise fences.ExecutionFenceUnavailable("delivery_execution_context_missing")
        goal_id, run_id, base = request.get("goal_id"), request.get("run_id"), request.get("base_sha")
        revision, attempt, fence = request.get("goal_revision"), request.get("attempt"), request.get("fence")
        if (any(not isinstance(value, str) or not value for value in (goal_id, run_id, base))
                or any(isinstance(value, bool) or not isinstance(value, int) or value < 1
                       for value in (revision, attempt, fence))):
            raise fences.ExecutionFenceUnavailable("delivery_execution_context_identity_missing")
        command = resolve_command(argv, worktree)
        context = {"phase": phase, "command": command, "command_id": request.get("command_id"),
                   "unit_id": request.get("unit_id"), "node_id": request.get("node_id"),
                   "input_digest": fences.digest_json(dict(input_request or {}))}
        binding = fences.build_attempt_binding(
            goal={"goal_id": goal_id, "goal_revision": revision},
            run_id=run_id, attempt=attempt, attempt_fence=fence, base_revision=base,
            clone_root=str(Path(worktree).resolve()), verifier_argv=command,
            adapter_id=ADAPTER_ID,
            adapter_version=ADAPTER_VERSION,
            timeout_seconds=timeout_seconds, allowed_read_roots=[],
            allowed_write_roots=[str(Path(worktree).resolve())] if writable else [],
            allowed_local_effects=["workspace_write", "scratch_write"] if writable else ["scratch_write"],
            execution_context_digest=fences.digest_json(context))
        descriptor = self.port.prepare(binding)
        if descriptor.get("binding") != binding:
            raise fences.ExecutionFenceUnavailable("binding_mismatch")
        projected = self.port.project_paths(descriptor, input_request or {})
        environment = self.port.project_environment(descriptor, env or {})
        command = self.port.project_command(descriptor, command, env or {})
        evidence = {"execution_fence": self.port.receipt_projection(descriptor)}
        completed = self.port.launch(
            descriptor, command,
            input_text=json.dumps(projected, sort_keys=True) if input_request is not None else None,
            timeout_seconds=timeout_seconds, env_projection=environment, on_started=on_started)
        return completed, evidence, descriptor


def runner_status(runner: Any, port: fences.ExecutionFencePort) -> dict[str, Any]:
    """What executes delivery commands for this run, for the run plan."""
    if isinstance(runner, FenceCommandRunner):
        return {"status": "fenced", "backend": runner.backend}
    if runner is None:
        reason = port.reason if isinstance(port, fences.DisabledExecutionFencePort) else "runner_not_installed"
        return {"status": "unavailable", "reason": reason}
    return {"status": "caller_supplied", "runner": getattr(runner, "__qualname__", type(runner).__name__)}


__all__ = ["FenceCommandRunner", "resolve_command", "runner_status"]

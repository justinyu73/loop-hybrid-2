#!/usr/bin/env python3
"""Run delivery checks and the independent verifier inside the execution fence.

``RunStore`` executes every delivery obligation command and the independent
verifier through its ``command_runner``.  The native-run path supplies one from
its execution binding; the compatibility path gets this one.  Each command is
its own fenced launch: a fresh single-use descriptor from ``port.prepare``, the
fence-owned environment only, and the backend's own containment (on Linux:
bubblewrap with no network, read-only clone unless the caller asks for a
writable one, provider seccomp table).  The receipt carries the backend's real
fence projection -- never a fixture claim.

A bare command name resolves the way the sandbox resolves it: against the
fence's own PATH (``/usr/bin:/bin`` on Linux), never the host PATH, because a
pyenv shim or a toolcache interpreter outside the system roots cannot start
inside the fence.

The fence has no ``/dev``.  ``git diff --cached --check`` -- the default
delivery check -- needs ``/dev/null``, so it runs under the fence's closed
``git-diff-cached-check-v1`` grant (the same one the native path uses): only
``/dev/null`` is exposed, the git binary is pinned by digest, and the command
must match exactly.  Other commands get no device tree.
"""

from __future__ import annotations

import hashlib
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
SANDBOX_PATH = ("/usr/bin", "/bin")
NULL_DEVICE_PROFILE = "git-diff-cached-check-v1"
NULL_DEVICE_ADAPTER_ID = "deterministic-command-v1"
NULL_DEVICE_PHASE = "delivery_checks"


def null_device_grant(command: Sequence[str], *, phase: str, writable: bool,
                      input_request: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The fence's closed grant for ``git diff --cached --check``, or None."""
    if (phase != NULL_DEVICE_PHASE or writable or input_request is not None or os.name == "nt"
            or list(command[1:]) != ["diff", "--cached", "--check"] or Path(command[0]).name != "git"):
        return None
    git = Path(command[0]).resolve(strict=True)
    return {"profile": NULL_DEVICE_PROFILE, "phase": phase, "argv": [str(git), *command[1:]],
            "executable_sha256": "sha256:" + hashlib.sha256(git.read_bytes()).hexdigest()}


def resolve_command(argv: Sequence[str], worktree: str) -> list[str]:
    """Resolve ``argv[0]`` the way the fence will see it."""
    if not argv or not isinstance(argv[0], str) or not argv[0]:
        raise fences.ExecutionFenceUnavailable("delivery_command_argv_invalid")
    name = argv[0]
    if os.path.isabs(name):
        executable = name
    elif "/" in name or "\\" in name:
        executable = str((Path(worktree) / name).resolve())
    elif os.name == "nt":
        found = shutil.which(name)
        if found is None:
            raise fences.ExecutionFenceUnavailable(f"delivery_command_not_found:{name}")
        executable = found
    else:
        candidates = [Path(directory) / name for directory in SANDBOX_PATH]
        found_path = next((path for path in candidates if path.is_file() and os.access(path, os.X_OK)), None)
        if found_path is None:
            raise fences.ExecutionFenceUnavailable(f"delivery_command_not_in_sandbox_path:{name}")
        executable = str(found_path)
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
        grant = null_device_grant(command, phase=phase, writable=writable, input_request=input_request)
        if grant is not None:
            command = list(grant["argv"])
        context = {"phase": phase, "command": command, "command_id": request.get("command_id"),
                   "unit_id": request.get("unit_id"), "node_id": request.get("node_id"),
                   "input_digest": fences.digest_json(dict(input_request or {}))}
        binding = fences.build_attempt_binding(
            goal={"goal_id": goal_id, "goal_revision": revision},
            run_id=run_id, attempt=attempt, attempt_fence=fence, base_revision=base,
            clone_root=str(Path(worktree).resolve()), verifier_argv=command,
            adapter_id=NULL_DEVICE_ADAPTER_ID if grant is not None else ADAPTER_ID,
            adapter_version=ADAPTER_VERSION,
            timeout_seconds=timeout_seconds, allowed_read_roots=[],
            allowed_write_roots=[str(Path(worktree).resolve())] if writable else [],
            allowed_local_effects=["workspace_write", "scratch_write"] if writable else ["scratch_write"],
            execution_context_digest=fences.digest_json(context), null_device_check=grant)
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

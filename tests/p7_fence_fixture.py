"""Explicit non-kernel fixtures; never production defaults or fence proofs."""

from __future__ import annotations

import copy
import json
import subprocess
from typing import Any, Callable, Mapping, Sequence

try:
    from lh_runtime.execution_fence import ExecutionFencePort
except ImportError:
    from execution_fence import ExecutionFencePort


class ExplicitFixtureFence(ExecutionFencePort):
    """Opt in one existing spawn fixture, preserving its original Process."""

    supports_started_notification = True

    def __init__(self, *, spawn: Callable[..., Any]):
        self.spawn = spawn

    def project_environment(self, descriptor: Mapping[str, Any],
                            environment: Mapping[str, str]) -> dict[str, str]:
        """Forward only the source-sanitized map explicitly given to this fixture."""
        return dict(environment)

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        return {"schema": "explicit-fixture-launch/v1",
                "evidence_kind": "non-kernel-fixture",
                "binding": copy.deepcopy(dict(binding))}

    def launch(self, descriptor: Mapping[str, Any], argv: Sequence[str], *,
               input_text: str | None = None, timeout_seconds: float,
               env_projection: Mapping[str, str] | None = None,
               on_started: Callable[[Any], None] | None = None) -> subprocess.CompletedProcess[str]:
        kwargs = {"cwd": str(descriptor["binding"]["clone_root"]),
                  "env": dict(env_projection) if env_projection is not None else None,
                  "stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "text": True}
        if input_text is not None:
            kwargs["stdin"] = subprocess.PIPE
        process = self.spawn(list(argv), **kwargs)
        if on_started is not None:
            on_started(process)
        # The successor owns termination and unknown receipts. Preserve its
        # original Process, TimeoutExpired and readback exceptions unchanged.
        if input_text is None:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        else:
            stdout, stderr = process.communicate(input=input_text, timeout=timeout_seconds)
        return subprocess.CompletedProcess(list(argv), process.returncode, stdout, stderr)

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        return {"fixture_evidence_kind": "non-kernel-fixture"}


def fixture_command_runner(request: Mapping[str, Any], *, phase: str,
                           argv: Sequence[str], worktree: str, timeout_seconds: float,
                           input_request: Mapping[str, Any] | None = None,
                           writable: bool = False, on_started: Callable[[Any], None] | None = None,
                           env: Mapping[str, str] | None = None):
    """Transparently run the original source exam command and scoped env.

    Invocation is explicit. Empty metadata makes no receipt proof claim;
    the returned descriptor remains visibly non-kernel fixture evidence.
    """
    input_text = json.dumps(input_request) if input_request is not None else None
    if on_started is None:
        completed = subprocess.run(list(argv), cwd=worktree, env=env,
            input=input_text, timeout=timeout_seconds, capture_output=True,
            text=True, check=False)
    else:
        with subprocess.Popen(list(argv), cwd=worktree, env=env,
                stdin=subprocess.PIPE if input_text is not None else None,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                on_started(process)
                stdout, stderr = process.communicate(input=input_text, timeout=timeout_seconds)
            except BaseException:
                process.kill()
                process.communicate()
                raise
            completed = subprocess.CompletedProcess(list(argv), process.returncode, stdout, stderr)
    return completed, {}, {"schema": "explicit-fixture-command/v1",
                           "evidence_kind": "non-kernel-fixture", "phase": phase}

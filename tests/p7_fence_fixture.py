"""Explicit non-kernel fixtures; never production defaults or fence proofs."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
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
        return {"schema": "p7-explicit-fixture-launch/v1",
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
    return completed, {}, {"schema": "p7-explicit-fixture-command/v1",
                           "evidence_kind": "non-kernel-fixture", "phase": phase}


def fixture_orca_linux_binding(root: Path, *, authority_root: Path) -> dict[str, Any]:
    """Explicit optional-Orca/Linux configuration for existing route canaries.

    References bind real fixture bytes. This does not install trust, launch a
    provider, or claim backend containment; callers separately scope trust.
    """
    try:
        from lh_runtime.host_ports import headless_contract
    except ImportError:
        from host_ports import headless_contract
    root.mkdir(parents=True, exist_ok=True)
    coding = {"principal": "fixture-coding", "kind": "controlled_command", "adapter_version": "v1"}
    verifier = {"principal": "fixture-independent-verifier", "kind": "controlled_command", "adapter_version": "v1"}
    capabilities = {"schema": "host-provider-neutral-capability-contract/v1", "revision": "fixture-1",
        "capabilities": {
            "coding": {"adapter_id": "bounded-command-v1", "identity": coding, "permissions": "workspace_write"},
            "verifier": {"adapter_id": "bounded-command-v1", "identity": verifier, "permissions": "read_only"},
            "checks": {"adapter_id": "deterministic-command-v1", "identity": {"principal": "fixture-checks"},
                       "permissions": "read_only"}},
        "roles": {"coding": "coding", "integration": "coding", "verifier": "verifier", "checks": "checks"},
        "fallback": "none", "default_provider": None, "default_model": None}
    registry = {"schema": "host-provider-registry/v1", "revision": "fixture-1", "fallback": "none",
        "default_provider": None, "providers": {
            "fixture-coding": {"adapter_id": "bounded-command-v1", "identity": coding,
                               "command": [sys.executable, "-B", "fixture-coding.py"]},
            "fixture-verifier": {"adapter_id": "bounded-command-v1", "identity": verifier,
                                 "command": [sys.executable, "-B", "fixture-verifier.py"]}}}
    host = {**headless_contract(), "selected_adapter": "orca"}

    def reference(name, value):
        path = root / name
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
        return {"path": str(path.resolve()),
                "digest": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()}

    authority_root = authority_root.resolve()
    relative = "docs/bootstrap-authority.md"
    return {"schema": "host-task-area-execution-binding/v1",
        "capability_contract_ref": reference("capabilities.json", capabilities),
        "provider_registry_ref": reference("providers.json", registry),
        "provider_selection": {"coding": "fixture-coding", "verifier": "fixture-verifier"},
        "host_contract_ref": reference("host.json", host),
        "host_adapters_ref": reference("adapters.json", {"orca": {"adapter_id": "orca", "identity": {"kind": "fixture"}}}),
        "fence": {"backend_id": "linux-bubblewrap-seccomp", "egress_policy_ref": None},
        "bootstrap_authority": {"decision_id": "LH-EXTERNAL-BOOTSTRAP-001",
            "authority_ref": relative + "#lh-external-bootstrap-001",
            "authority_digest": "sha256:" + hashlib.sha256((authority_root / relative).read_bytes()).hexdigest(),
            "root": str(authority_root)}}

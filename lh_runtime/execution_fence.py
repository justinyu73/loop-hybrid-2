"""Preventive execution boundary for mutation-capable adapters.

The controller prepares one immutable descriptor.  The selected adapter may
launch only through the same port instance and descriptor.  The default port
is unavailable.  The engine ships no kernel sandbox: a containment backend is
an operator-supplied port, and the explicit ``local-process`` backend runs owned
process groups while stating that it contains nothing.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import platform
import re
import secrets
import subprocess
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


DESCRIPTOR_SCHEMA = "lh-execution-fence-launch/v1"
BINDING_SCHEMA = "lh-execution-fence-binding/v1"
BINDING_SCHEMA_V2 = "lh-execution-fence-binding/v2"
PROOF_SCHEMA = "lh-execution-fence-proof/v1"
ERROR_CODE = "execution_fence_unavailable"

REQUIRED_PROOF_TRACKS = (
    "filesystem_effect_containment",
    "provider_control_egress",
    "provider_sandbox",
)


def digest_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


class ExecutionFenceUnavailable(RuntimeError):
    """The mutation adapter has no admissible launch descriptor."""

    code = ERROR_CODE

    def __init__(self, reason: str):
        super().__init__(f"{ERROR_CODE}: {reason}")
        self.reason = reason


def _platform_backend(name: str):
    """Import by the caller's package form; import failures never select a fallback."""
    return importlib.import_module("." + name, __package__) if __package__ else importlib.import_module(name)


class ExecutionFencePort(ABC):
    """Backend-neutral controller/adapter seam."""

    supports_started_notification = False

    def project_environment(self, descriptor: Mapping[str, Any],
                            environment: Mapping[str, str]) -> dict[str, str]:
        del descriptor
        return {key: environment[key] for key in ("LANG", "LC_ALL") if key in environment}

    def project_command(self, descriptor: Mapping[str, Any], argv: Sequence[str],
                        environment: Mapping[str, str]) -> list[str]:
        del descriptor, environment
        return list(argv)

    def project_paths(self, descriptor: Mapping[str, Any], value: Mapping[str, Any],
                      *, reverse: bool = False) -> dict[str, Any]:
        """Project only declared top-level path fields, never arbitrary text."""
        import copy
        del descriptor, reverse
        return copy.deepcopy(dict(value))

    @abstractmethod
    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        """Return one immutable descriptor or raise ExecutionFenceUnavailable."""

    @abstractmethod
    def launch(
        self,
        descriptor: Mapping[str, Any],
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float,
        env_projection: Mapping[str, str] | None = None,
        on_started: Callable[[Any], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Create the single child admitted by ``descriptor``."""

    @abstractmethod
    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        """Return the bounded evidence projection stored on the Attempt receipt."""

class DisabledExecutionFencePort(ExecutionFencePort):
    """Default posture: mutation-capable dispatch has no backend."""

    def __init__(self, reason: str = "backend_not_configured"):
        self.reason = reason

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        del binding
        raise ExecutionFenceUnavailable(self.reason)

    def launch(
        self,
        descriptor: Mapping[str, Any],
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float,
        env_projection: Mapping[str, str] | None = None,
        on_started: Callable[[Any], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        del descriptor, argv, input_text, timeout_seconds, env_projection, on_started
        raise ExecutionFenceUnavailable(self.reason)

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        del descriptor
        raise ExecutionFenceUnavailable(self.reason)


def build_attempt_binding(
    *,
    goal: Mapping[str, Any],
    run_id: str,
    attempt: int,
    attempt_fence: int,
    base_revision: str,
    clone_root: str | Path,
    verifier_argv: Sequence[str],
    adapter_id: str,
    adapter_version: str,
    timeout_seconds: float,
    now: float | None = None,
    nonce: str | None = None,
    allowed_read_roots: Sequence[str | Path] | None = None,
    allowed_write_roots: Sequence[str | Path] | None = None,
    allowed_local_effects: Sequence[str] | None = None,
    execution_context_digest: str | None = None,
) -> dict[str, Any]:
    """Build the closed controller-owned input to ``ExecutionFencePort``."""
    clone = Path(clone_root)
    try:
        resolved_clone = clone.resolve(strict=True)
    except OSError as exc:
        raise ExecutionFenceUnavailable("clone_root_unreadable") from exc
    if not resolved_clone.is_dir() or clone.absolute() != resolved_clone:
        raise ExecutionFenceUnavailable("clone_root_not_canonical")
    if not isinstance(run_id, str) or not run_id:
        raise ExecutionFenceUnavailable("run_id_missing")
    if not isinstance(attempt, int) or attempt < 1:
        raise ExecutionFenceUnavailable("attempt_invalid")
    if not isinstance(attempt_fence, int) or attempt_fence < 1:
        raise ExecutionFenceUnavailable("attempt_fence_invalid")
    if not isinstance(base_revision, str) or not base_revision:
        raise ExecutionFenceUnavailable("base_revision_missing")
    commands = [str(item) for item in verifier_argv]
    if not commands or any(not item for item in commands):
        raise ExecutionFenceUnavailable("verification_command_invalid")
    if timeout_seconds <= 0:
        raise ExecutionFenceUnavailable("timeout_invalid")
    created_at = float(time.time() if now is None else now)
    controller_nonce = nonce or secrets.token_urlsafe(24)
    goal_revision = digest_json(dict(goal))
    identity = {
        "goal_revision": goal_revision,
        "run_id": run_id,
        "attempt": attempt,
        "attempt_fence": attempt_fence,
        "base_revision": base_revision,
        "clone_root": str(resolved_clone),
        "adapter_id": adapter_id,
        "adapter_version": adapter_version,
    }
    binding = {
        "schema": BINDING_SCHEMA,
        **identity,
        "allowed_write_roots": [str(resolved_clone)],
        "allowed_local_effects": ["workspace_write"],
        "verification_commands": [commands],
        "provider_control_channel": {
            "type": "stdio",
            "channel_id": "provider-control-" + digest_json(identity)[-32:],
            "attempt": attempt,
        },
        "controller_nonce": controller_nonce,
        "created_at": created_at,
        "expires_at": created_at + float(timeout_seconds),
        "idempotency_key": "fence-" + digest_json(
            {**identity, "controller_nonce": controller_nonce}
        )[-40:],
    }
    if execution_context_digest is not None:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", execution_context_digest):
            raise ExecutionFenceUnavailable("execution_context_digest_invalid")
        binding.update({
            "schema": BINDING_SCHEMA_V2,
            "allowed_read_roots": [str(Path(root)) for root in (allowed_read_roots or [])],
            "allowed_write_roots": [str(Path(root)) for root in (allowed_write_roots or [])],
            "allowed_local_effects": list(allowed_local_effects or []),
            "execution_context_digest": execution_context_digest,
        })
        validate_phase_roots(binding)
    elif any(value is not None for value in
             (allowed_read_roots, allowed_write_roots, allowed_local_effects)):
        raise ExecutionFenceUnavailable("phase_roots_require_v2_context")
    return binding


def validate_phase_roots(binding: Mapping[str, Any]) -> None:
    """The v2 root grant is explicit, canonical and non-overlapping."""
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(binding.get("execution_context_digest", ""))):
        raise ExecutionFenceUnavailable("execution_context_digest_invalid")
    roots = []
    for field in ("allowed_read_roots", "allowed_write_roots"):
        values = binding.get(field)
        if not isinstance(values, list) or len(values) != len(set(values)):
            raise ExecutionFenceUnavailable("phase_roots_invalid")
        for raw in values:
            path = Path(raw)
            if (not path.is_absolute() or str(path.resolve(strict=True)) != raw
                    or path == Path(path.anchor)):
                raise ExecutionFenceUnavailable("phase_root_not_canonical")
            roots.append((field, path))
    effects = binding.get("allowed_local_effects")
    if (not isinstance(effects, list) or len(effects) != len(set(effects))
            or set(effects) - {"workspace_write", "scratch_write"}
            or bool(binding["allowed_write_roots"]) != ("workspace_write" in effects)):
        raise ExecutionFenceUnavailable("local_effects_invalid")
    clone = Path(binding["clone_root"])
    if any(Path(root) != clone for root in binding["allowed_write_roots"]):
        raise ExecutionFenceUnavailable("write_roots_invalid")
    if any(left == right or left in right.parents or right in left.parents
           for index, (_, left) in enumerate(roots)
           for _, right in roots[index + 1:]):
        raise ExecutionFenceUnavailable("phase_roots_overlap")


def _model_attr(model: Any, name: str, default: Any = None) -> Any:
    value = getattr(model, name, default)
    if value is not default:
        return value
    owner = getattr(model, "__self__", None)
    return getattr(owner, name, default) if owner is not None else default


def model_requires_fence(model: Any) -> bool:
    return _model_attr(model, "requires_execution_fence", False) is True


def model_fence_identity(model: Any) -> tuple[str, str]:
    adapter_id = _model_attr(model, "execution_fence_adapter_id", None)
    adapter_version = _model_attr(model, "execution_fence_adapter_version", None)
    if not isinstance(adapter_id, str) or not adapter_id:
        adapter_id = "mutation-adapter"
    if not isinstance(adapter_version, str) or not adapter_version:
        adapter_version = "unknown"
    return adapter_id, adapter_version


def mark_mutation_adapter(
    model: Any,
    *,
    adapter_id: str,
    adapter_version: str = "v1",
) -> Any:
    setattr(model, "requires_execution_fence", True)
    setattr(model, "execution_fence_adapter_id", adapter_id)
    setattr(model, "execution_fence_adapter_version", adapter_version)
    return model


def configured_execution_fence(
    environ: Mapping[str, str] | None = None,
    *,
    platform_name: str | None = None,
    policy: Mapping[str, Any] | None = None,
    operator_binding: Mapping[str, Any] | None = None,
) -> ExecutionFencePort:
    """Resolve an explicit backend; absence always means disabled."""
    values = os.environ if environ is None else environ
    selected = values.get("LH_EXECUTION_FENCE_BACKEND", "disabled").strip()
    host = (platform_name or platform.system()).strip().lower()
    if selected == "trusted-project-local":
        if policy is None or operator_binding is None:
            return DisabledExecutionFencePort("trusted_policy_missing")
        if os.name == "nt" and host in {"windows", "win32", "msys", "cygwin"}:
            if not _platform_backend("execution_fence_trusted_windows").windows_process_group_available():
                return DisabledExecutionFencePort("process_group_capability_unavailable")
        elif host not in {"linux", "gnu/linux", "darwin", "macos", "macosx"} or os.name != "posix":
            return DisabledExecutionFencePort("process_group_capability_unavailable")
        return _platform_backend("execution_fence_trusted").TrustedProjectExecutionFence(
            policy=policy, operator_binding=operator_binding)
    if selected == "disabled":
        return DisabledExecutionFencePort()
    if selected == "local-process":
        if os.name == "nt" and host in {"windows", "win32", "msys", "cygwin"}:
            if not _platform_backend("execution_fence_trusted_windows").windows_process_group_available():
                return DisabledExecutionFencePort("process_group_capability_unavailable")
        elif os.name != "posix":
            return DisabledExecutionFencePort("process_group_capability_unavailable")
        return _platform_backend("execution_fence_local").LocalProcessExecutionFence()
    return DisabledExecutionFencePort("backend_configuration_invalid")

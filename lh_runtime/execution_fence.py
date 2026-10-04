"""Preventive execution boundary for mutation-capable adapters.

The controller prepares one immutable descriptor.  The selected adapter may
launch only through the same port instance and descriptor.  The default port
is unavailable; enabling a kernel backend is an explicit runtime choice.
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

# Backend identifiers belong to the portable descriptor contract; concrete
# platform implementations are imported only by the selector below.
LINUX_BACKEND_ID = "linux-bubblewrap-seccomp"
WINDOWS_BACKEND_ID = "windows-native-appcontainer-job"
WINDOWS_BACKEND_VERSION = "1.0"
BACKEND_ID = LINUX_BACKEND_ID
REQUIRED_PROOF_TRACKS = (
    "filesystem_effect_containment",
    "provider_control_egress",
    "provider_sandbox",
)
PROVIDER_SANDBOX_ENFORCED_BY = "lh-client-composed"

# lh-egress-endpoint: the host-side egress policy is an HOST-owned,
# registry-bound artefact; the fence reads it at prepare() and refuses a
# provider the policy does not declare. This is client-side preflight
# and provenance -- the policy names its own `enforced_by` so a receipt can
# never be read as host mediation (agy vote: false attestation).
EGRESS_POLICY_SCHEMA = "host-execution-host-egress-policy/v1"
EGRESS_POLICY_ENFORCED_BY = "lh-client-preflight"
_EGRESS_VALUE_RE = r"[A-Za-z0-9._:/-]{1,64}"

# Local execution host: LH itself starts the provider under the signed
# provider-sandbox profile.
# The descriptor admits exactly one provider launch.
LOCAL_PROVIDER_ADAPTER_PREFIX = "local-provider-"
LOCAL_PROVIDER_LAUNCH_BUDGET = 1
# Default provider syscall table for a generated profile: mount, namespace,
# tracing, kernel-module, BPF and keyring routes are denied, while the sockets
# and subprocesses a provider CLI needs stay available.
PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS = (
    "mount", "umount2", "pivot_root", "chroot",
    "fsopen", "fsconfig", "fsmount", "move_mount", "open_tree",
    "open_by_handle_at", "name_to_handle_at",
    "ptrace", "process_vm_readv", "process_vm_writev",
    "setns", "unshare",
    "kexec_load", "kexec_file_load", "init_module", "finit_module", "delete_module",
    "bpf", "perf_event_open",
    "keyctl", "add_key", "request_key",
)


def _egress_policy_path() -> Path:
    override = os.environ.get("LH_EGRESS_POLICY", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "governance" / "execution-host-egress-policy.json"


def load_local_provider_policy() -> tuple[dict[str, Any], str]:
    """The policy for a local provider launch; any defect refuses the launch.

    The provider-sandbox profile is mandatory here."""
    path = _egress_policy_path()
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ExecutionFenceUnavailable("egress_policy_unreadable") from exc
    try:
        policy = json.loads(raw)
    except ValueError as exc:
        raise ExecutionFenceUnavailable("egress_policy_invalid") from exc
    if (
        not isinstance(policy, dict)
        or policy.get("schema") != EGRESS_POLICY_SCHEMA
        or policy.get("enforced_by") != EGRESS_POLICY_ENFORCED_BY
        or not isinstance(policy.get("providers"), dict)
    ):
        raise ExecutionFenceUnavailable("egress_policy_invalid")
    if not isinstance(policy.get("provider_sandbox_profile"), dict):
        raise ExecutionFenceUnavailable("egress_policy_sandbox_profile_missing")
    return policy, "sha256:" + hashlib.sha256(raw).hexdigest()


def validate_local_provider(
    policy: Mapping[str, Any],
    agent: str,
    *,
    provider_path: str,
    provider_digest: str | None,
) -> dict[str, Any]:
    """The provider entry a local launch may use: listed and pinned by digest."""
    provider_policy = (policy.get("providers") or {}).get(agent)
    if not isinstance(provider_policy, dict):
        raise ExecutionFenceUnavailable("egress_policy_provider_not_listed")
    if (
        provider_policy.get("path") != provider_path
        or provider_policy.get("sha256") != provider_digest
    ):
        raise ExecutionFenceUnavailable("egress_policy_provider_mismatch")
    return provider_policy


def _validate_provider_argv_against_policy(
    provider_argv: Sequence[str], egress_policy: Mapping[str, Any]
) -> None:
    """Every token beyond argv[0] must be declared by the policy.

    codex vote (4a): pinning argv[0] alone leaves the bypass flags and any
    injected argument free; the allowlist walks the whole vector. Free text
    exists in exactly the declared prompt positions and nowhere else.
    """
    flags = set(egress_policy.get("flags") or [])
    value_flags = set(egress_policy.get("value_flags") or [])
    prompt_flags = set(egress_policy.get("prompt_flags") or [])
    trailing_prompt = bool(egress_policy.get("trailing_prompt"))
    tokens = [str(item) for item in provider_argv[1:]]
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in prompt_flags:
            if index + 1 >= len(tokens):
                raise ExecutionFenceUnavailable("control_provider_argv_outside_policy")
            index += 2
            continue
        if token in value_flags:
            if index + 1 >= len(tokens) or not re.fullmatch(_EGRESS_VALUE_RE, tokens[index + 1]):
                raise ExecutionFenceUnavailable("control_provider_argv_outside_policy")
            index += 2
            continue
        if token in flags:
            index += 1
            continue
        if trailing_prompt and index == len(tokens) - 1:
            return
        raise ExecutionFenceUnavailable("control_provider_argv_outside_policy")
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


def normalize_sandbox_profile(raw: Mapping[str, Any], agent: str) -> dict[str, Any]:
    """Compatibility façade; Linux provider-sandbox code is platform-owned."""
    return _platform_backend("execution_fence_linux").normalize_sandbox_profile(raw, agent)


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

    def launch_provider(
        self,
        descriptor: Mapping[str, Any],
        provider_argv: Sequence[str],
        *,
        env_overlay: Mapping[str, str] | None = None,
        input_text: str | None = None,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        """Start the one provider a local-provider descriptor admits.

        Only a kernel backend that applies the signed provider-sandbox profile
        itself implements this; every other port refuses."""
        del descriptor, provider_argv, env_overlay, input_text, timeout_seconds
        raise ExecutionFenceUnavailable("local_provider_unsupported")


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
    null_device_check: Mapping[str, Any] | None = None,
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
    if null_device_check is not None:
        if execution_context_digest is None or not isinstance(null_device_check, Mapping):
            raise ExecutionFenceUnavailable("null_device_check_requires_phase_binding")
        binding["null_device_check"] = json.loads(json.dumps(dict(null_device_check)))
        validate_null_device_check(binding)
    return binding


def validate_null_device_check(binding: Mapping[str, Any]) -> None:
    """Closed, default-off grant for the one approved deterministic check."""
    grant = binding.get("null_device_check")
    if (not isinstance(grant, dict)
        or set(grant) != {"profile", "phase", "argv", "executable_sha256"}
        or grant.get("profile") != "git-diff-cached-check-v1"
        or grant.get("phase") != "delivery_checks"
        or binding.get("schema") != BINDING_SCHEMA_V2
        or binding.get("adapter_id") != "deterministic-command-v1"
        or binding.get("allowed_write_roots") != []
        or binding.get("allowed_local_effects") != ["scratch_write"]):
        raise ExecutionFenceUnavailable("null_device_check_grant_invalid")
    argv = grant.get("argv")
    if (not isinstance(argv, list) or len(argv) != 4
        or not isinstance(argv[0], str) or not Path(argv[0]).is_absolute()
        or argv[1:] != ["diff", "--cached", "--check"]
        or binding.get("verification_commands") != [argv]
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(grant.get("executable_sha256", "")))):
        raise ExecutionFenceUnavailable("null_device_check_command_invalid")


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


def __getattr__(name: str) -> Any:
    """Lazy compatibility exports; platform backends stay out of core imports."""
    if name in {
        "DENIED_SYSCALLS",
        "EXPECTED_BWRAP_VERSION",
        "LinuxBubblewrapExecutionFence",
    }:
        return getattr(_platform_backend("execution_fence_linux"), name)
    raise AttributeError(name)

def configured_execution_fence(
    environ: Mapping[str, str] | None = None,
    *,
    platform_name: str | None = None,
    policy: Mapping[str, Any] | None = None,
    operator_binding: Mapping[str, Any] | None = None,
) -> ExecutionFencePort:
    """Resolve an explicit platform backend; absence always means disabled."""
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
    if selected == LINUX_BACKEND_ID:
        if host not in {"linux", "gnu/linux"}:
            return DisabledExecutionFencePort("backend_platform_mismatch")
        return _platform_backend("execution_fence_linux").LinuxBubblewrapExecutionFence.discover()
    if selected == WINDOWS_BACKEND_ID:
        if host not in {"windows", "win32", "msys", "cygwin"}:
            return DisabledExecutionFencePort("backend_platform_mismatch")
        return _platform_backend("execution_fence_windows").WindowsNativeExecutionFence.discover(
            environ=values,
            platform_name=host,
        )
    if selected in {"macos-native", "macos", "darwin"} and host in {
        "darwin",
        "macos",
        "macosx",
    }:
        return DisabledExecutionFencePort("macos_execution_fence_unsupported")
    if host in {"darwin", "macos", "macosx"}:
        return DisabledExecutionFencePort("macos_execution_fence_unsupported")
    return DisabledExecutionFencePort("backend_configuration_invalid")

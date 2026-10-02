#!/usr/bin/env python3
"""Typed, attempt-bound control client for an host/Orca ExecutionHost.

The adapter owns the boundary between LH's immutable Attempt and the host
control plane.  It deliberately does not start a subprocess itself: the
caller supplies the already-admitted ``ExecutionFencePort`` transport.  This
keeps the P4 distinction intact while making the control schema, capability
handshake, path representation, and cleanup identity explicit in one place.

Authority: ``docs/contracts/model-routing-v1.md#lh-capability-model-routing-002``.
The host runs the selected model; LH still owns Goal/Run/Attempt state,
provider selection, retry, receipt, and verdict.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import platform as platform_module
import re
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


SCHEMA = "lh-orca-execution-host-adapter/v1"
CAPABILITY_SCHEMA = "external-orca-cli-capability/v1"
CAPABILITY_PROTOCOL = "1"
CAPABILITY_OPERATION = "capability_probe"
MINIMUM_ORCA_CLI_VERSION = "1.0.0"
WSL_HOST_KIND_ENV = "ORCA_ORCHESTRATION_COMPATIBILITY_HOST_KIND"

CONTROL_OPERATIONS = (
    CAPABILITY_OPERATION,
    "repo_list",
    "repo_add",
    "project_setup_delete",
    "terminal_create",
    "terminal_wait",
    "terminal_read",
    "terminal_stop",
    "terminal_close",
)
REQUIRED_OPERATIONS = frozenset(CONTROL_OPERATIONS)
REQUIRED_PATH_CODECS = frozenset({"posix", "windows-drive", "macos-posix"})
REQUIRED_PLATFORMS = frozenset({"linux", "windows", "macos"})

# Released Orca exposes its versioned host vector through ``status --json``
# rather than a standalone ``capabilities`` command. These markers are the
# minimum runtime capabilities needed to prove the closed control operations;
# the normalized response remains this adapter's exact schema.
_STATUS_OPERATION_CAPABILITIES: dict[str, frozenset[str]] = {
    CAPABILITY_OPERATION: frozenset({"runtime.status.compat.v1"}),
    "repo_list": frozenset({"project-host-setup.v1"}),
    "repo_add": frozenset({"project-host-setup.v1"}),
    "project_setup_delete": frozenset({"project-host-setup.v1"}),
    "terminal_create": frozenset({"terminal.multiplex.v1", "workspace-run-context.v1"}),
    "terminal_wait": frozenset({"terminal.multiplex.v1"}),
    "terminal_read": frozenset({"terminal.multiplex.v1", "terminal.binary-stream.v1"}),
    "terminal_stop": frozenset({"terminal.multiplex.v1"}),
    "terminal_close": frozenset({"terminal.multiplex.v1"}),
}
_STATUS_PATH_CODEC_CAPABILITY = "folder-workspace.path-status.v1"

_VERSION_RE = re.compile(r"^(?:v)?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:[-+][0-9A-Za-z.-]+)?$")
_DRIVE_RE = re.compile(r"^([A-Za-z]):(?:/|$)")
_WSL_RE = re.compile(r"^//wsl(?:\.localhost|\.|\$)/([^/]+)(/.*)?$", re.IGNORECASE)
_HANDLE_RE = re.compile(r"^[^\s\x00-\x1f\x7f]{1,256}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHELL_CONTROL_TOKENS = frozenset({";", "&&", "||", "|", ">", ">>", "<", "`"})
_SHELL_CONTROL_FLAGS = frozenset({"--command", "--shell", "--eval"})


class ExecutionHostAdapterError(ValueError):
    """The host contract cannot be admitted safely."""


class CapabilityNegotiationError(ExecutionHostAdapterError):
    """The host did not advertise the exact capability contract required."""


class ControlRequestRejected(ExecutionHostAdapterError):
    """A control request is outside the closed schema or Attempt binding."""


class StaleAttemptHandle(ControlRequestRejected):
    """A terminal handle is unknown, already closed, or belongs elsewhere."""


def _version(value: Any) -> tuple[int, int, int]:
    if not isinstance(value, str):
        raise CapabilityNegotiationError("orca_capability_version_invalid")
    match = _VERSION_RE.fullmatch(value.strip())
    if match is None:
        raise CapabilityNegotiationError("orca_capability_version_invalid")
    return tuple(int(item) for item in match.groups())  # type: ignore[return-value]


def _sha256_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _text(value: Any, field: str, *, max_length: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise ControlRequestRejected(f"{field}_invalid")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ControlRequestRejected(f"{field}_control_character")
    return value


def _canonical_posix(value: str) -> str:
    if not value.startswith("/"):
        raise ControlRequestRejected("path_not_absolute")
    parts = value.split("/")
    if any(part in {".", ".."} for part in parts):
        raise ControlRequestRejected("path_traversal_rejected")
    clean = [part for part in parts if part]
    return "/" + "/".join(clean)


def _canonical_windows(value: str) -> str:
    normalized = value.replace("\\", "/")
    drive = _DRIVE_RE.match(normalized)
    if drive:
        prefix = drive.group(1).upper() + ":"
        tail = normalized[2:]
        parts = tail.split("/")
        if any(part in {".", ".."} for part in parts):
            raise ControlRequestRejected("path_traversal_rejected")
        clean = [part for part in parts if part]
        return prefix + ("/" + "/".join(clean) if clean else "/")
    if not normalized.startswith("//"):
        raise ControlRequestRejected("windows_path_not_absolute")
    parts = normalized.split("/")
    if len(parts) < 4 or not parts[2] or not parts[3]:
        raise ControlRequestRejected("windows_unc_invalid")
    if any(part in {".", ".."} for part in parts[2:]):
        raise ControlRequestRejected("path_traversal_rejected")
    clean = [part for part in parts[2:] if part]
    return "//" + "/".join(clean)


@dataclass(frozen=True)
class PathCodec:
    """Canonical path/selector representation for one host family.

    ``windows`` emits native drive/UNC separators.  ``wsl`` accepts the local
    POSIX path used by LH and emits the Windows-hosted Orca UNC selector.  The
    canonical comparison form remains path-only, so the same Attempt binding
    can be tested on Linux, Windows, and macOS without conflating host syntax
    with Goal identity.
    """

    platform: str = "linux"
    wsl_distro: str | None = None

    def __post_init__(self) -> None:
        selected = self.platform.strip().lower()
        if selected not in {"linux", "macos", "windows", "wsl"}:
            raise ControlRequestRejected("path_codec_platform_unsupported")
        if selected == "wsl" and not (self.wsl_distro or "").strip():
            raise ControlRequestRejected("wsl_distro_missing")
        object.__setattr__(self, "platform", selected)
        if self.wsl_distro is not None:
            object.__setattr__(self, "wsl_distro", self.wsl_distro.strip())

    @classmethod
    def for_environment(cls) -> "PathCodec":
        if os.environ.get(WSL_HOST_KIND_ENV) == "wsl":
            distro = os.environ.get("WSL_DISTRO_NAME")
            if distro:
                return cls("wsl", distro)
            raise ControlRequestRejected("wsl_distro_missing")
        system = platform_module.system().strip().lower()
        if system.startswith("win"):
            return cls("windows")
        if system in {"darwin", "mac", "macos"}:
            return cls("macos")
        return cls("linux")

    def canonical(self, value: str | Path) -> str:
        raw = _text(str(value), "path", max_length=32768)
        if self.platform in {"linux", "macos"}:
            return _canonical_posix(raw)
        if self.platform == "windows":
            return _canonical_windows(raw)
        # WSL accepts either the local path LH owns or the UNC path Orca
        # stores.  Both compare as the same POSIX canonical path.
        if raw.startswith("/"):
            return _canonical_posix(raw)
        normalized = raw.replace("\\", "/")
        match = _WSL_RE.fullmatch(normalized)
        if match is None or match.group(1) != self.wsl_distro:
            raise ControlRequestRejected("wsl_path_invalid")
        return _canonical_posix(match.group(2) or "/")

    def cli_path(self, value: str | Path) -> str:
        canonical = self.canonical(value)
        if self.platform == "wsl":
            return "\\\\wsl.localhost\\" + str(self.wsl_distro) + canonical.replace("/", "\\")
        if self.platform == "windows":
            return canonical.replace("/", "\\")
        return canonical

    def selector(self, value: str | Path) -> str:
        return "path:" + self.cli_path(value)

    def selector_matches(self, selector: Any, value: str | Path) -> bool:
        if not isinstance(selector, str) or not selector.startswith("path:"):
            return False
        try:
            return self.canonical(selector[5:]) == self.canonical(value)
        except ControlRequestRejected:
            return False

    def same_path(self, left: Any, right: str | Path) -> bool:
        if not isinstance(left, str):
            return False
        try:
            return self.canonical(left) == self.canonical(right)
        except ControlRequestRejected:
            return False

    def is_within(self, candidate: Any, root: str | Path) -> bool:
        if not isinstance(candidate, str):
            return False
        try:
            child = self.canonical(candidate)
            parent = self.canonical(root)
        except ControlRequestRejected:
            return False
        return child == parent or child.startswith(parent.rstrip("/") + "/")


def semantic_attempt_identity(run_id: str, attempt: int) -> dict[str, Any]:
    """Return the host-independent portion of a disposable Attempt identity."""
    normalized_run = _text(run_id, "run_id", max_length=256)
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        raise ControlRequestRejected("attempt_invalid")
    return {
        "schema": "lh-disposable-attempt-identity/v1",
        "kind": "disposable_attempt",
        "run_id": normalized_run,
        "attempt": attempt,
    }


def discover_orca_cli(explicit: str | None = None) -> str:
    """Discover one executable without invoking it or consulting a fixed user path."""
    candidate = explicit if explicit is not None else os.environ.get("LH_ORCA_CLI")
    if candidate:
        # An existing file is taken verbatim: POSIX shlex would strip the
        # separators out of a Windows path before it is ever looked up.
        if Path(candidate).is_file():
            return str(candidate)
        parts = shlex.split(candidate)
        if len(parts) != 1:
            raise ExecutionHostAdapterError("LH_ORCA_CLI_must_name_one_executable")
        resolved = shutil.which(parts[0]) if not Path(parts[0]).is_file() else parts[0]
        if resolved:
            return str(resolved)
        raise FileNotFoundError(f"Orca CLI not found: {parts[0]}")
    # external host owns the app/userData-derived resolver.  Prefer its verified result
    # over PATH/local fallbacks so a WSL shell cannot accidentally select an
    # unrelated stale Linux install when the paired Windows app is live.
    resolver_path = Path(__file__).resolve().parents[1] / "tools" / "orca_cli_resolver.py"
    if resolver_path.is_file():
        try:
            spec = importlib.util.spec_from_file_location(
                "host_orca_cli_resolver_for_lh", resolver_path
            )
            if spec is not None and spec.loader is not None:
                resolver = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(resolver)
                candidates = resolver.candidate_resolutions(
                    environ=dict(os.environ),
                    state_root=os.environ.get("LH_HOST_STATE_ROOT"),
                )
                for item in candidates:
                    resolved = item.get("cli") if isinstance(item, dict) else None
                    if not isinstance(resolved, str) or not resolved:
                        continue
                    if Path(resolved).is_file():
                        return resolved
                    found = shutil.which(resolved)
                    if found:
                        return str(found)
        except (AttributeError, ImportError, OSError, TypeError, ValueError):
            # A missing/broken external host resolver must not turn discovery into an
            # arbitrary command; the validated local fallbacks below remain.
            pass
    local_bin = Path.home() / ".local" / "bin"
    candidates = [local_bin / "orca", local_bin / "orca-ide"]
    candidates.extend(sorted(local_bin.glob("orca-ide-*"), reverse=True))
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)
    for name in ("orca-ide", "orca"):
        resolved = shutil.which(name)
        if resolved:
            return str(resolved)
    raise FileNotFoundError("Orca IDE CLI not found; set LH_ORCA_CLI")


def _validate_handle(value: Any, field: str = "handle") -> str:
    if not isinstance(value, str) or _HANDLE_RE.fullmatch(value) is None:
        raise ControlRequestRejected(f"{field}_invalid")
    return value


def validate_provider_argv(provider_argv: Any) -> list[str]:
    """Validate argv as data; no request may carry a free-form shell command."""
    if not isinstance(provider_argv, (list, tuple)) or not provider_argv:
        raise ControlRequestRejected("provider_argv_empty")
    normalized: list[str] = []
    for index, value in enumerate(provider_argv):
        if not isinstance(value, str) or not value or len(value) > 32768:
            raise ControlRequestRejected(f"provider_argv_{index}_invalid")
        if "\x00" in value or any(ord(character) == 127 for character in value):
            raise ControlRequestRejected(f"provider_argv_{index}_control_character")
        token = value
        if token in _SHELL_CONTROL_TOKENS:
            raise ControlRequestRejected("provider_shell_control_token")
        if index == 0 and any(character in token for character in ";|&<>`$()"):
            raise ControlRequestRejected("provider_executable_shell_syntax")
        if index > 0 and token in _SHELL_CONTROL_FLAGS:
            raise ControlRequestRejected("provider_freeform_shell_flag")
        normalized.append(token)
    return normalized


def provider_command_spec(
    provider_argv: Any,
    *,
    env_overlay: Mapping[str, Any] | None = None,
    output_path: str | None = None,
) -> dict[str, Any]:
    """Build the only command representation accepted by the adapter.

    It is an argv plus digest, never a caller-supplied shell string.  The
    platform fence remains the sole component allowed to compose Orca's
    legacy terminal ``--command`` argument.
    """
    argv = validate_provider_argv(provider_argv)
    env: dict[str, str] = {}
    for name, value in sorted((env_overlay or {}).items()):
        if not isinstance(name, str) or _ENV_NAME_RE.fullmatch(name) is None:
            raise ControlRequestRejected("provider_environment_name_invalid")
        env[name] = _text(value, f"provider_environment_{name}", max_length=32768)
    body: dict[str, Any] = {"argv": argv, "env": env}
    if output_path is not None:
        body["output_path"] = _text(output_path, "output_path", max_length=32768)
    return {
        "schema": "lh-provider-command-spec/v1",
        **body,
        "argv_digest": _sha256_json(body),
    }


_REQUEST_FIELDS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    CAPABILITY_OPERATION: (frozenset(), frozenset()),
    "repo_list": (frozenset(), frozenset()),
    "repo_add": (frozenset({"path"}), frozenset()),
    "project_setup_delete": (frozenset({"setup"}), frozenset()),
    "terminal_create": (
        frozenset({"worktree_selector", "title", "provider_argv"}),
        frozenset({"output_path", "env_overlay"}),
    ),
    "terminal_wait": (frozenset({"handle", "timeout_ms"}), frozenset()),
    "terminal_read": (frozenset({"handle", "limit"}), frozenset()),
    "terminal_stop": (frozenset({"worktree_selector"}), frozenset()),
    "terminal_close": (frozenset({"handle"}), frozenset()),
}
_COMMON_OPTIONAL_FIELDS = frozenset({"orca_cli"})


def validate_control_request(
    request: Mapping[str, Any],
    *,
    path_codec: PathCodec | None = None,
) -> dict[str, Any]:
    """Validate one closed control request before any host call."""
    if not isinstance(request, Mapping):
        raise ControlRequestRejected("control_request_not_object")
    op = request.get("op")
    if not isinstance(op, str) or op not in _REQUEST_FIELDS:
        raise ControlRequestRejected("control_op_unknown")
    required, optional = _REQUEST_FIELDS[op]
    allowed = frozenset({"op"}) | required | optional | _COMMON_OPTIONAL_FIELDS
    unknown = set(request) - set(allowed)
    if unknown:
        raise ControlRequestRejected("control_request_unknown_field")
    missing = set(required) - set(request)
    if missing:
        raise ControlRequestRejected("control_request_required_field_missing")
    normalized = dict(request)
    if "orca_cli" in normalized:
        _text(normalized["orca_cli"], "orca_cli", max_length=32768)
    if op in {"repo_add"}:
        _text(normalized["path"], "path", max_length=32768)
    elif op == "project_setup_delete":
        _validate_handle(normalized["setup"], "setup")
    elif op == "terminal_create":
        selector = _text(normalized["worktree_selector"], "worktree_selector", max_length=32768)
        if not selector.startswith("path:") or len(selector) == 5:
            raise ControlRequestRejected("worktree_selector_invalid")
        if path_codec is not None and not path_codec.selector_matches(selector, selector[5:]):
            # This branch only verifies syntax.  AttemptControlSession performs
            # the exact workspace comparison; accepting an unknown root here
            # would make selector validation depend on an ambient cwd.
            raise ControlRequestRejected("worktree_selector_invalid")
        _text(normalized["title"], "title", max_length=512)
        validate_provider_argv(normalized["provider_argv"])
        if "output_path" in normalized:
            _text(normalized["output_path"], "output_path", max_length=32768)
        if "env_overlay" in normalized:
            provider_command_spec(
                normalized["provider_argv"],
                env_overlay=normalized.get("env_overlay"),
                output_path=normalized.get("output_path"),
            )
    elif op in {"terminal_wait", "terminal_read", "terminal_close"}:
        _validate_handle(normalized["handle"])
        if op == "terminal_wait":
            timeout_ms = normalized["timeout_ms"]
            if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int) or not 1 <= timeout_ms <= 86_400_000:
                raise ControlRequestRejected("timeout_ms_invalid")
        if op == "terminal_read":
            limit = normalized["limit"]
            if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100_000:
                raise ControlRequestRejected("limit_invalid")
    elif op == "terminal_stop":
        selector = _text(normalized["worktree_selector"], "worktree_selector", max_length=32768)
        if not selector.startswith("path:") or len(selector) == 5:
            raise ControlRequestRejected("worktree_selector_invalid")
    return normalized


@dataclass(frozen=True)
class NegotiatedCapability:
    schema: str
    protocol: str
    version: str
    operations: tuple[str, ...]
    path_codecs: tuple[str, ...]
    platforms: tuple[str, ...]

    @property
    def operation_set(self) -> frozenset[str]:
        return frozenset(self.operations)


def _status_capability_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the released Orca ``status --json`` response.

    Orca's status endpoint is an observation surface. It does not claim
    arbitrary future CLI verbs: only the versioned runtime markers mapped
    above can produce the adapter's closed operation set.
    """
    if isinstance(payload.get("result"), Mapping):
        payload = payload["result"]
    runtime = payload.get("runtime")
    if not isinstance(runtime, Mapping):
        raise CapabilityNegotiationError("orca_status_runtime_missing")
    if runtime.get("state") != "ready" or runtime.get("reachable") is not True:
        raise CapabilityNegotiationError("orca_status_runtime_not_ready")
    version = runtime.get("appVersion")
    markers = runtime.get("capabilities")
    if not isinstance(markers, list) or any(
        not isinstance(item, str) or not item for item in markers
    ):
        raise CapabilityNegotiationError("orca_status_capabilities_invalid")
    advertised = set(markers)
    missing = {
        marker
        for required in _STATUS_OPERATION_CAPABILITIES.values()
        for marker in required
        if marker not in advertised
    }
    if _STATUS_PATH_CODEC_CAPABILITY not in advertised:
        missing.add(_STATUS_PATH_CODEC_CAPABILITY)
    if missing:
        raise CapabilityNegotiationError("orca_status_capability_missing")
    return {
        "capability": {
            "schema": CAPABILITY_SCHEMA,
            "protocol": CAPABILITY_PROTOCOL,
            "version": version,
            "operations": list(CONTROL_OPERATIONS),
            "path_codecs": sorted(REQUIRED_PATH_CODECS),
            "platforms": sorted(REQUIRED_PLATFORMS),
        }
    }


def negotiate_capability(
    payload: Mapping[str, Any],
    *,
    minimum_version: str = MINIMUM_ORCA_CLI_VERSION,
) -> NegotiatedCapability:
    """Parse and verify the host's versioned capability response."""
    if not isinstance(payload, Mapping):
        raise CapabilityNegotiationError("orca_capability_response_invalid")
    capability = payload.get("capability")
    if capability is None:
        payload = _status_capability_payload(payload)
        capability = payload.get("capability")
    if not isinstance(capability, Mapping):
        raise CapabilityNegotiationError("orca_capability_missing")
    expected_fields = {"schema", "protocol", "version", "operations", "path_codecs", "platforms"}
    if set(capability) != expected_fields:
        raise CapabilityNegotiationError("orca_capability_schema_not_closed")
    if capability.get("schema") != CAPABILITY_SCHEMA or capability.get("protocol") != CAPABILITY_PROTOCOL:
        raise CapabilityNegotiationError("orca_capability_schema_invalid")
    actual_version = _version(capability.get("version"))
    if actual_version < _version(minimum_version):
        raise CapabilityNegotiationError("orca_capability_version_unsupported")
    operations = capability.get("operations")
    codecs = capability.get("path_codecs")
    platforms = capability.get("platforms")
    if not all(isinstance(item, list) for item in (operations, codecs, platforms)):
        raise CapabilityNegotiationError("orca_capability_lists_invalid")
    if any(not isinstance(item, str) or not item for item in (*operations, *codecs, *platforms)):
        raise CapabilityNegotiationError("orca_capability_list_item_invalid")
    if len(set(operations)) != len(operations) or not REQUIRED_OPERATIONS.issubset(operations):
        raise CapabilityNegotiationError("orca_capability_operation_missing")
    if len(set(codecs)) != len(codecs) or not REQUIRED_PATH_CODECS.issubset(codecs):
        raise CapabilityNegotiationError("orca_capability_path_codec_missing")
    if len(set(platforms)) != len(platforms) or not REQUIRED_PLATFORMS.issubset(platforms):
        raise CapabilityNegotiationError("orca_capability_platform_missing")
    return NegotiatedCapability(
        schema=str(capability["schema"]),
        protocol=str(capability["protocol"]),
        version=str(capability["version"]),
        operations=tuple(operations),
        path_codecs=tuple(codecs),
        platforms=tuple(platforms),
    )


ControlTransport = Callable[[Mapping[str, Any]], Mapping[str, Any]]


class OrcaControlClient:
    """Capability-negotiated transport; no caller can submit a free command."""

    def __init__(
        self,
        *,
        orca_cli: str,
        transport: ControlTransport,
        minimum_version: str = MINIMUM_ORCA_CLI_VERSION,
    ) -> None:
        self.orca_cli = _text(orca_cli, "orca_cli", max_length=32768)
        if not callable(transport):
            raise TypeError("transport must be callable")
        self._transport = transport
        self.minimum_version = minimum_version
        self._capability: NegotiatedCapability | None = None

    @property
    def capability(self) -> NegotiatedCapability | None:
        return self._capability

    def _raw(self, request: Mapping[str, Any]) -> dict[str, Any]:
        response = self._transport(dict(request))
        if not isinstance(response, Mapping):
            raise ExecutionHostAdapterError("orca_control_response_invalid")
        return dict(response)

    def negotiate(self) -> NegotiatedCapability:
        response = self._raw({"op": CAPABILITY_OPERATION, "orca_cli": self.orca_cli})
        capability = negotiate_capability(response, minimum_version=self.minimum_version)
        self._capability = capability
        return capability

    def call(self, request: Mapping[str, Any]) -> dict[str, Any]:
        normalized = validate_control_request(request)
        if normalized["op"] == CAPABILITY_OPERATION:
            capability = self.negotiate()
            return {
                "capability": {
                    "schema": capability.schema,
                    "protocol": capability.protocol,
                    "version": capability.version,
                    "operations": list(capability.operations),
                    "path_codecs": list(capability.path_codecs),
                    "platforms": list(capability.platforms),
                }
            }
        if self._capability is None:
            self.negotiate()
        assert self._capability is not None
        if normalized["op"] not in self._capability.operation_set:
            raise CapabilityNegotiationError("orca_capability_operation_not_advertised")
        return self._raw({**normalized, "orca_cli": self.orca_cli})


@dataclass(frozen=True)
class AttemptIdentity:
    run_id: str
    attempt: int
    workspace: str
    worktree_selector: str


class AttemptControlSession:
    """One host control session whose handles and setups cannot cross Attempts."""

    def __init__(
        self,
        client: OrcaControlClient,
        *,
        workspace: str | Path,
        run_id: str,
        attempt: int,
        path_codec: PathCodec,
    ) -> None:
        semantic_attempt_identity(run_id, attempt)
        workspace_canonical = path_codec.canonical(workspace)
        self.client = client
        self.path_codec = path_codec
        self.identity = AttemptIdentity(
            run_id=run_id,
            attempt=attempt,
            workspace=workspace_canonical,
            worktree_selector=path_codec.selector(workspace),
        )
        self._handles: dict[str, AttemptIdentity] = {}
        self._owned_setups: set[str] = set()

    @property
    def worktree_selector(self) -> str:
        return self.identity.worktree_selector

    @property
    def active_handles(self) -> tuple[str, ...]:
        return tuple(sorted(self._handles))

    @property
    def owned_setups(self) -> tuple[str, ...]:
        return tuple(sorted(self._owned_setups))

    def _validate_attempt_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        normalized = validate_control_request(request, path_codec=None)
        op = normalized["op"]
        if op == "repo_add" and not self.path_codec.same_path(normalized["path"], self.identity.workspace):
            raise ControlRequestRejected("control_repo_path_invalid")
        if op in {"terminal_create", "terminal_stop"}:
            if not self.path_codec.selector_matches(normalized["worktree_selector"], self.identity.workspace):
                raise ControlRequestRejected("control_selector_invalid")
        if op == "terminal_create" and "output_path" in normalized:
            if not self.path_codec.is_within(normalized["output_path"], self.identity.workspace):
                raise ControlRequestRejected("control_output_path_invalid")
            # Re-run the command-spec validation here so the path and argv are
            # bound together before the transport receives the request.
            provider_command_spec(
                normalized["provider_argv"],
                env_overlay=normalized.get("env_overlay"),
                output_path=normalized["output_path"],
            )
        if op == "project_setup_delete":
            setup = _validate_handle(normalized["setup"], "setup")
            if setup not in self._owned_setups:
                raise ControlRequestRejected("setup_not_attempt_owned")
        if op in {"terminal_wait", "terminal_read", "terminal_close"}:
            handle = _validate_handle(normalized["handle"])
            if handle not in self._handles:
                raise StaleAttemptHandle("terminal_handle_not_attempt_owned")
        if op == "terminal_stop" and not self._handles:
            raise StaleAttemptHandle("terminal_stop_without_active_attempt_handle")
        if op == "terminal_create" and self._handles:
            raise ControlRequestRejected("attempt_already_has_active_terminal")
        return normalized

    @staticmethod
    def _dict_response(response: Mapping[str, Any], field: str) -> Mapping[str, Any]:
        value = response.get(field)
        if not isinstance(value, Mapping):
            raise ExecutionHostAdapterError(f"orca_{field}_response_invalid")
        return value

    def call(self, request: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._validate_attempt_request(request)
        response = self.client.call(normalized)
        op = normalized["op"]
        if op == "repo_add":
            repo = self._dict_response(response, "repo")
            setup = _validate_handle(repo.get("id"), "repo_id")
            if "path" in repo and not self.path_codec.same_path(repo["path"], self.identity.workspace):
                raise ExecutionHostAdapterError("orca_repo_path_response_mismatch")
            self._owned_setups.add(setup)
        elif op == "terminal_create":
            terminal = self._dict_response(response, "terminal")
            handle = _validate_handle(terminal.get("handle"))
            self._handles[handle] = self.identity
        elif op == "terminal_close":
            handle = _validate_handle(normalized["handle"])
            self._handles.pop(handle, None)
        elif op == "project_setup_delete":
            self._owned_setups.discard(_validate_handle(normalized["setup"], "setup"))
        return response


class OrcaExecutionHostAdapter:
    """Factory for one capability-negotiated, Attempt-bound control session."""

    def __init__(
        self,
        *,
        path_codec: PathCodec | None = None,
        minimum_version: str = MINIMUM_ORCA_CLI_VERSION,
    ) -> None:
        self.path_codec = path_codec
        self.minimum_version = minimum_version

    def begin_attempt(
        self,
        *,
        orca_cli: str,
        transport: ControlTransport,
        workspace: str | Path,
        run_id: str,
        attempt: int,
        path_codec: PathCodec | None = None,
    ) -> AttemptControlSession:
        codec = path_codec or self.path_codec or PathCodec.for_environment()
        client = OrcaControlClient(
            orca_cli=orca_cli,
            transport=transport,
            minimum_version=self.minimum_version,
        )
        return AttemptControlSession(
            client,
            workspace=workspace,
            run_id=run_id,
            attempt=attempt,
            path_codec=codec,
        )


__all__ = [
    "CAPABILITY_OPERATION",
    "CAPABILITY_PROTOCOL",
    "CAPABILITY_SCHEMA",
    "CONTROL_OPERATIONS",
    "CapabilityNegotiationError",
    "ControlRequestRejected",
    "ExecutionHostAdapterError",
    "MINIMUM_ORCA_CLI_VERSION",
    "NegotiatedCapability",
    "OrcaControlClient",
    "OrcaExecutionHostAdapter",
    "PathCodec",
    "StaleAttemptHandle",
    "AttemptControlSession",
    "discover_orca_cli",
    "negotiate_capability",
    "provider_command_spec",
    "semantic_attempt_identity",
    "validate_control_request",
    "validate_provider_argv",
]

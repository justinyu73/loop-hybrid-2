"""Windows execution-fence backend.

The portable controller never guesses that a Windows process is contained.
Admission requires a version-pinned native helper which attests the two
load-bearing proof tracks before it is allowed to launch a provider.  The
helper is the only component allowed to use Windows AppContainer/ACL and Job
Object APIs; this module owns the closed protocol, descriptor binding, and
receipt-bound refusal semantics around it.

There is deliberately no Python fallback.  A Windows host without the native
helper, with a helper version mismatch, or with an incomplete attestation is
an unavailable execution fence and therefore starts zero provider children.
"""
from __future__ import annotations

import hashlib
import json
import ntpath
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

if __package__:
    from .execution_fence import (
        BINDING_SCHEMA, DESCRIPTOR_SCHEMA, ExecutionFencePort, ExecutionFenceUnavailable,
        PROOF_SCHEMA, REQUIRED_PROOF_TRACKS, WINDOWS_BACKEND_ID, WINDOWS_BACKEND_VERSION, digest_json,
    )
else:
    from execution_fence import (
        BINDING_SCHEMA, DESCRIPTOR_SCHEMA, ExecutionFencePort, ExecutionFenceUnavailable,
        PROOF_SCHEMA, REQUIRED_PROOF_TRACKS, WINDOWS_BACKEND_ID, WINDOWS_BACKEND_VERSION, digest_json,
    )


WINDOWS_HELPER_ENV = "LH_WINDOWS_EXECUTION_FENCE_HELPER"
WINDOWS_HELPER_PROTOCOL = "lh-windows-execution-fence-helper/v1"
WINDOWS_ATTESTATION_SCHEMA = "lh-windows-execution-fence-attestation/v1"
WINDOWS_HELPER_VERSION_OUTPUT = (
    f"LH Windows Execution Fence Helper {WINDOWS_BACKEND_VERSION}"
)


def _sha256_file(path: str | Path) -> str | None:
    try:
        return "sha256:" + hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def _platform_name(value: str | None = None) -> str:
    raw = value if value is not None else platform.system()
    return str(raw).strip().lower().replace(" ", "_")


def _is_windows(platform_name: str) -> bool:
    return platform_name in {"windows", "win32", "msys", "cygwin"}


def _is_absolute_executable(value: str) -> bool:
    return Path(value).is_absolute() or ntpath.isabs(value)


def _validate_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema",
        "goal_revision",
        "run_id",
        "attempt",
        "attempt_fence",
        "base_revision",
        "clone_root",
        "allowed_write_roots",
        "allowed_local_effects",
        "verification_commands",
        "provider_control_channel",
        "controller_nonce",
        "created_at",
        "expires_at",
        "idempotency_key",
        "adapter_id",
        "adapter_version",
    }
    if set(binding) != required or binding.get("schema") != BINDING_SCHEMA:
        raise ExecutionFenceUnavailable("binding_fields_invalid")
    clone = Path(str(binding["clone_root"]))
    try:
        resolved_clone = clone.resolve(strict=True)
    except OSError as exc:
        raise ExecutionFenceUnavailable("clone_root_unreadable") from exc
    if str(resolved_clone) != str(clone) or not resolved_clone.is_dir():
        raise ExecutionFenceUnavailable("clone_root_not_canonical")
    if binding.get("allowed_write_roots") != [str(resolved_clone)]:
        raise ExecutionFenceUnavailable("write_roots_invalid")
    if binding.get("allowed_local_effects") != ["workspace_write"]:
        raise ExecutionFenceUnavailable("local_effects_invalid")
    channel = binding.get("provider_control_channel")
    if (
        not isinstance(channel, dict)
        or set(channel) != {"type", "channel_id", "attempt"}
        or channel.get("type") != "stdio"
        or channel.get("attempt") != binding.get("attempt")
    ):
        raise ExecutionFenceUnavailable("provider_control_channel_invalid")
    if not isinstance(binding.get("expires_at"), (int, float)):
        raise ExecutionFenceUnavailable("expiry_invalid")
    return dict(binding)


def _filesystem_policy(binding: Mapping[str, Any]) -> dict[str, Any]:
    clone_root = str(binding["clone_root"])
    return {
        "allowed_write_roots": [clone_root],
        "source": "deny",
        "sibling": "deny",
        "user_state": "deny",
        "socket_paths": "deny",
        "device_paths": "deny",
        "symlink_escape": "deny",
        "implementation": "windows-appcontainer-acl",
    }


def _egress_policy(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "network": "deny",
        "socket": "deny",
        "dns": "deny",
        "process_control": "deny",
        "ipc": "deny",
        "provider_control_channel": dict(binding["provider_control_channel"]),
        "implementation": "windows-appcontainer-no-network-capability",
    }


class WindowsNativeExecutionFence(ExecutionFencePort):
    """Helper-backed Windows AppContainer + Job Object execution fence."""

    def __init__(
        self,
        *,
        helper_path: str,
        helper_version: str = WINDOWS_BACKEND_VERSION,
        helper_digest: str | None = None,
        platform_name: str | None = None,
        clock: Callable[[], float] = time.time,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ):
        self.helper_path = str(Path(helper_path).resolve())
        self.helper_version = str(helper_version)
        self.helper_digest = helper_digest or _sha256_file(self.helper_path)
        self.platform_name = _platform_name(platform_name)
        self.clock = clock
        self.runner = runner
        self._prepared: dict[str, dict[str, Any]] = {}
        self._consumed: set[str] = set()
        self._attestations: dict[str, dict[str, Any]] = {}
        self.launch_count = 0

    @classmethod
    def discover(
        cls,
        *,
        environ: Mapping[str, str] | None = None,
        platform_name: str | None = None,
        expected_version: str = WINDOWS_BACKEND_VERSION,
        clock: Callable[[], float] = time.time,
    ) -> ExecutionFencePort:
        values = os.environ if environ is None else environ
        host = _platform_name(platform_name)
        if not _is_windows(host):
            return _disabled("backend_platform_mismatch")
        raw = values.get(WINDOWS_HELPER_ENV, "").strip()
        if not raw:
            return _disabled("windows_helper_missing")
        path = Path(raw)
        if not path.is_absolute():
            return _disabled("windows_helper_not_absolute")
        try:
            resolved = path.resolve(strict=True)
        except OSError:
            return _disabled("windows_helper_unreadable")
        if resolved != path or not path.is_file():
            return _disabled("windows_helper_not_canonical")
        digest = _sha256_file(path)
        if digest is None:
            return _disabled("windows_helper_unreadable")
        try:
            result = subprocess.run(
                [str(path), "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return _disabled("windows_helper_version_unreadable")
        version = result.stdout.strip()
        expected_output = f"LH Windows Execution Fence Helper {expected_version}"
        if result.returncode != 0 or version != expected_output:
            return _disabled("windows_helper_version_unsupported")
        return cls(
            helper_path=str(path),
            helper_version=expected_version,
            helper_digest=digest,
            platform_name=host,
            clock=clock,
        )

    def _validate_backend(self) -> None:
        if not _is_windows(self.platform_name):
            raise ExecutionFenceUnavailable("backend_platform_mismatch")
        if self.helper_version != WINDOWS_BACKEND_VERSION:
            raise ExecutionFenceUnavailable("windows_helper_version_unsupported")
        if not _is_absolute_executable(self.helper_path):
            raise ExecutionFenceUnavailable("windows_helper_not_absolute")
        current = _sha256_file(self.helper_path)
        if current is None:
            raise ExecutionFenceUnavailable("windows_helper_missing")
        if current != self.helper_digest:
            raise ExecutionFenceUnavailable("windows_helper_drifted")

    def _exchange(self, payload: Mapping[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
        self._validate_backend()
        if timeout_seconds <= 0:
            raise ExecutionFenceUnavailable("helper_timeout_invalid")
        try:
            result = self.runner(
                [self.helper_path, "--protocol", "json"],
                input=json.dumps(dict(payload), ensure_ascii=False, sort_keys=True),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ExecutionFenceUnavailable("windows_helper_timeout") from exc
        except OSError as exc:
            raise ExecutionFenceUnavailable("windows_helper_launch_failed") from exc
        if result.returncode != 0:
            raise ExecutionFenceUnavailable("windows_helper_rejected")
        try:
            response = json.loads(result.stdout)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ExecutionFenceUnavailable("windows_helper_response_invalid") from exc
        if not isinstance(response, dict):
            raise ExecutionFenceUnavailable("windows_helper_response_invalid")
        return response

    @staticmethod
    def _validate_attestation(
        attestation: Mapping[str, Any],
        *,
        binding: Mapping[str, Any],
        backend_id: str,
    ) -> dict[str, Any]:
        if (
            attestation.get("schema") != WINDOWS_ATTESTATION_SCHEMA
            or attestation.get("protocol") != WINDOWS_HELPER_PROTOCOL
            or attestation.get("status") != "admitted"
            or attestation.get("backend_id") != backend_id
            or attestation.get("backend_version") != WINDOWS_BACKEND_VERSION
        ):
            raise ExecutionFenceUnavailable("windows_proof_missing")
        tracks = attestation.get("proof_tracks")
        capabilities = attestation.get("capabilities")
        if not isinstance(tracks, list) or set(tracks) != set(REQUIRED_PROOF_TRACKS):
            raise ExecutionFenceUnavailable("windows_proof_missing")
        if not isinstance(capabilities, dict):
            raise ExecutionFenceUnavailable("windows_proof_missing")
        if (
            capabilities.get("filesystem_effect_containment") != "admissible"
            or capabilities.get("provider_control_egress") != "admissible"
            or capabilities.get("provider_sandbox") != "not_applicable"
            or capabilities.get("network") != "denied"
            or capabilities.get("process_control") != "denied"
            or capabilities.get("write_roots") != list(binding["allowed_write_roots"])
        ):
            raise ExecutionFenceUnavailable("windows_proof_missing")
        return json.loads(json.dumps(dict(attestation)))

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_backend()
        normalized = _validate_binding(binding)
        if str(normalized["adapter_id"]).startswith("orca-"):
            raise ExecutionFenceUnavailable("adapter_provider_channel_unsupported")
        if str(normalized["adapter_id"]).startswith("execution-host-port-"):
            raise ExecutionFenceUnavailable("windows_control_plane_unsupported")
        binding_digest = digest_json(normalized)
        filesystem = _filesystem_policy(normalized)
        egress = _egress_policy(normalized)
        backend = {
            "backend_id": WINDOWS_BACKEND_ID,
            "backend_version": self.helper_version,
            "helper_sha256": self.helper_digest,
            "filesystem_policy_digest": digest_json(filesystem),
            "egress_policy_digest": digest_json(egress),
            "enforcement": {
                "filesystem": "appcontainer_acl",
                "network": "appcontainer_no_network_capability",
                "process": "job_object_kill_on_close_active_process_limit_1",
            },
        }
        attestation = self._exchange(
            {
                "protocol": WINDOWS_HELPER_PROTOCOL,
                "op": "prepare",
                "backend": backend,
                "binding": normalized,
                "filesystem_policy": filesystem,
                "egress_policy": egress,
            },
            timeout_seconds=max(1.0, float(normalized["expires_at"]) - self.clock()),
        )
        attestation = self._validate_attestation(
            attestation,
            binding=normalized,
            backend_id=WINDOWS_BACKEND_ID,
        )
        backend["attestation_digest"] = digest_json(attestation)
        backend_digest = digest_json(backend)
        proofs = {
            "filesystem_effect_containment": {
                "schema": PROOF_SCHEMA,
                "track": "filesystem_effect_containment",
                "result": "admissible",
                "attempt_binding_digest": binding_digest,
                "policy_digest": backend["filesystem_policy_digest"],
                "backend_digest": backend_digest,
            },
            "provider_control_egress": {
                "schema": PROOF_SCHEMA,
                "track": "provider_control_egress",
                "result": "admissible",
                "attempt_binding_digest": binding_digest,
                "policy_digest": backend["egress_policy_digest"],
                "backend_digest": backend_digest,
            },
            "provider_sandbox": {
                "schema": PROOF_SCHEMA,
                "track": "provider_sandbox",
                "result": "not_applicable",
                "attempt_binding_digest": binding_digest,
                "backend_digest": backend_digest,
            },
        }
        body = {
            "schema": DESCRIPTOR_SCHEMA,
            "binding": normalized,
            "binding_digest": binding_digest,
            "backend": backend,
            "backend_digest": backend_digest,
            "proofs": proofs,
            "proofs_digest": digest_json(proofs),
            "launch_classes": {"control": 0, "mutation": 1},
            "mutation_dispatch": "enabled_for_descriptor",
            "helper_attestation": attestation,
        }
        descriptor = {**body, "launch_descriptor_digest": digest_json(body)}
        digest = descriptor["launch_descriptor_digest"]
        self._prepared[digest] = json.loads(json.dumps(descriptor))
        self._attestations[digest] = attestation
        return descriptor

    def _validate_descriptor(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(descriptor, Mapping):
            raise ExecutionFenceUnavailable("descriptor_missing")
        normalized = json.loads(json.dumps(dict(descriptor)))
        digest = normalized.get("launch_descriptor_digest")
        body = {key: value for key, value in normalized.items() if key != "launch_descriptor_digest"}
        if (
            normalized.get("schema") != DESCRIPTOR_SCHEMA
            or not isinstance(digest, str)
            or digest_json(body) != digest
        ):
            raise ExecutionFenceUnavailable("descriptor_digest_invalid")
        if self._prepared.get(digest) != normalized:
            raise ExecutionFenceUnavailable("descriptor_not_prepared")
        if digest in self._consumed:
            raise ExecutionFenceUnavailable("descriptor_replayed")
        binding = normalized.get("binding")
        if not isinstance(binding, dict):
            raise ExecutionFenceUnavailable("binding_fields_invalid")
        normalized_binding = _validate_binding(binding)
        if normalized.get("binding_digest") != digest_json(normalized_binding):
            raise ExecutionFenceUnavailable("binding_digest_invalid")
        backend = normalized.get("backend")
        if (
            not isinstance(backend, dict)
            or backend.get("backend_id") != WINDOWS_BACKEND_ID
            or backend.get("backend_version") != self.helper_version
            or backend.get("helper_sha256") != self.helper_digest
            or normalized.get("backend_digest") != digest_json(backend)
        ):
            raise ExecutionFenceUnavailable("backend_binding_invalid")
        attestation = normalized.get("helper_attestation")
        if not isinstance(attestation, dict):
            raise ExecutionFenceUnavailable("windows_proof_missing")
        if backend.get("attestation_digest") != digest_json(attestation):
            raise ExecutionFenceUnavailable("windows_proof_digest_invalid")
        self._validate_attestation(
            attestation,
            binding=normalized_binding,
            backend_id=WINDOWS_BACKEND_ID,
        )
        proofs = normalized.get("proofs")
        if not isinstance(proofs, dict) or set(proofs) != set(REQUIRED_PROOF_TRACKS):
            raise ExecutionFenceUnavailable("proof_track_incomplete")
        for track in REQUIRED_PROOF_TRACKS:
            proof = proofs.get(track)
            expected = "not_applicable" if track == "provider_sandbox" else "admissible"
            if (
                not isinstance(proof, dict)
                or proof.get("schema") != PROOF_SCHEMA
                or proof.get("track") != track
                or proof.get("result") != expected
                or proof.get("attempt_binding_digest") != normalized["binding_digest"]
                or proof.get("backend_digest") != normalized["backend_digest"]
            ):
                raise ExecutionFenceUnavailable("proof_track_invalid")
        if normalized.get("proofs_digest") != digest_json(proofs):
            raise ExecutionFenceUnavailable("proofs_digest_invalid")
        if float(normalized_binding["expires_at"]) <= float(self.clock()):
            raise ExecutionFenceUnavailable("descriptor_expired")
        return normalized

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
        if on_started is not None:
            raise ExecutionFenceUnavailable("started_notification_unsupported")
        self._validate_backend()
        normalized = self._validate_descriptor(descriptor)
        if int((normalized.get("launch_classes") or {}).get("mutation", 0)) < 1:
            raise ExecutionFenceUnavailable("launch_class_not_authorized")
        if timeout_seconds <= 0:
            raise ExecutionFenceUnavailable("launch_timeout_invalid")
        if not argv or not all(isinstance(item, str) and item for item in argv):
            raise ExecutionFenceUnavailable("adapter_argv_invalid")
        if not _is_absolute_executable(argv[0]):
            raise ExecutionFenceUnavailable("adapter_executable_not_absolute")
        if env_projection and set(env_projection) - {"LANG", "LC_ALL"}:
            raise ExecutionFenceUnavailable("environment_projection_invalid")
        digest = normalized["launch_descriptor_digest"]
        self._consumed.add(digest)
        self.launch_count += 1
        response = self._exchange(
            {
                "protocol": WINDOWS_HELPER_PROTOCOL,
                "op": "launch",
                "descriptor": normalized,
                "descriptor_digest": digest,
                "argv": list(argv),
                "input_text": input_text,
                "env_projection": dict(env_projection or {}),
            },
            timeout_seconds=timeout_seconds,
        )
        if (
            response.get("status") != "completed"
            or response.get("descriptor_digest") != digest
            or response.get("proofs_digest") != normalized["proofs_digest"]
            or response.get("provider_child_count") != 1
        ):
            raise ExecutionFenceUnavailable("windows_launch_attestation_invalid")
        result = response.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("returncode"), int):
            raise ExecutionFenceUnavailable("windows_helper_result_invalid")
        return subprocess.CompletedProcess(
            list(argv),
            int(result["returncode"]),
            str(result.get("stdout") or ""),
            str(result.get("stderr") or ""),
        )

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._validate_descriptor(descriptor)
        backend = normalized["backend"]
        return {
            "schema": DESCRIPTOR_SCHEMA,
            "status": "admitted",
            "launch_descriptor_digest": normalized["launch_descriptor_digest"],
            "binding_digest": normalized["binding_digest"],
            "backend": {
                "backend_id": backend["backend_id"],
                "backend_version": backend["backend_version"],
                "filesystem_policy_digest": backend["filesystem_policy_digest"],
                "egress_policy_digest": backend["egress_policy_digest"],
                "attestation_digest": backend["attestation_digest"],
            },
            "proofs": normalized["proofs"],
            "proofs_digest": normalized["proofs_digest"],
            "provider_control_channel": normalized["binding"]["provider_control_channel"],
            "launch_classes": normalized["launch_classes"],
            "mutation_dispatch": normalized["mutation_dispatch"],
        }


def _disabled(reason: str) -> ExecutionFencePort:
    from execution_fence import DisabledExecutionFencePort

    return DisabledExecutionFencePort(reason)


__all__ = [
    "WINDOWS_ATTESTATION_SCHEMA",
    "WINDOWS_BACKEND_ID",
    "WINDOWS_BACKEND_VERSION",
    "WINDOWS_HELPER_ENV",
    "WindowsNativeExecutionFence",
]

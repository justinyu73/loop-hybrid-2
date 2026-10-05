"""Local process execution: the fence port with owned process control and no containment.

The engine ships no kernel sandbox.  This backend is selected explicitly
(``LH_EXECUTION_FENCE_BACKEND=local-process``) and runs each admitted command in
the disposable clone as one owned process group with a deadline and an output
limit.  It contains nothing: the child sees the operator's environment, network,
and file system.  Every receipt says so, and every proof track records
``not_contained`` rather than a claim this backend cannot make.

Descriptors stay single-use and digest-bound, so a replayed or edited descriptor
is refused before any process starts.
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

if __package__:
    from .execution_fence import (BINDING_SCHEMA, BINDING_SCHEMA_V2, DESCRIPTOR_SCHEMA, PROOF_SCHEMA,
                                  REQUIRED_PROOF_TRACKS, ExecutionFencePort, ExecutionFenceUnavailable,
                                  digest_json, validate_phase_roots)
    from .platform_ports import ManagedProcessUnknown, run_managed_process
else:
    from execution_fence import (BINDING_SCHEMA, BINDING_SCHEMA_V2, DESCRIPTOR_SCHEMA, PROOF_SCHEMA,
                                 REQUIRED_PROOF_TRACKS, ExecutionFencePort, ExecutionFenceUnavailable,
                                 digest_json, validate_phase_roots)
    from platform_ports import ManagedProcessUnknown, run_managed_process

BACKEND_ID = "local-process"
BACKEND_VERSION = "1"
NOT_CONTAINED = "not_contained"


class LocalProcessExecutionFence(ExecutionFencePort):
    """Owned local process groups behind the fence port; no containment claim."""

    supports_started_notification = True

    def __init__(self, *, clock: Callable[[], float] = time.time, identity_port: Any = None):
        self.clock = clock
        self.identity_port = identity_port
        self._prepared: dict[str, dict[str, Any]] = {}
        self._consumed: set[str] = set()
        self._lifecycles: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _validate_binding(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(binding, Mapping) or binding.get("schema") not in {BINDING_SCHEMA, BINDING_SCHEMA_V2}:
            raise ExecutionFenceUnavailable("binding_schema_invalid")
        normalized = json.loads(json.dumps(dict(binding)))
        clone = Path(str(normalized.get("clone_root") or ""))
        try:
            canonical = clone.is_absolute() and str(clone.resolve(strict=True)) == str(clone) and clone.is_dir()
        except OSError:
            canonical = False
        if not canonical:
            raise ExecutionFenceUnavailable("clone_root_not_canonical")
        if normalized["schema"] == BINDING_SCHEMA_V2:
            validate_phase_roots(normalized)
        expires_at = normalized.get("expires_at")
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or expires_at <= self.clock():
            raise ExecutionFenceUnavailable("descriptor_expired")
        return normalized

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._validate_binding(binding)
        binding_digest = digest_json(normalized)
        backend = {"backend_id": BACKEND_ID, "backend_version": BACKEND_VERSION,
                   "kernel_containment": False, "provider_egress_enforced": False}
        backend_digest = digest_json(backend)
        proofs = {track: {"schema": PROOF_SCHEMA, "track": track, "result": NOT_CONTAINED,
                          "attempt_binding_digest": binding_digest, "backend_digest": backend_digest}
                  for track in REQUIRED_PROOF_TRACKS}
        body = {"schema": DESCRIPTOR_SCHEMA, "binding": normalized, "binding_digest": binding_digest,
                "backend": backend, "backend_digest": backend_digest, "proofs": proofs,
                "proofs_digest": digest_json(proofs), "launch_classes": {"mutation": 1},
                "mutation_dispatch": "enabled_for_descriptor"}
        descriptor = {**body, "launch_descriptor_digest": digest_json(body)}
        with self._lock:
            self._prepared[descriptor["launch_descriptor_digest"]] = copy.deepcopy(descriptor)
        return descriptor

    def _admitted(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(descriptor, Mapping):
            raise ExecutionFenceUnavailable("descriptor_missing")
        normalized = json.loads(json.dumps(dict(descriptor)))
        digest = normalized.get("launch_descriptor_digest")
        body = {key: value for key, value in normalized.items() if key != "launch_descriptor_digest"}
        if normalized.get("schema") != DESCRIPTOR_SCHEMA or digest != digest_json(body):
            raise ExecutionFenceUnavailable("descriptor_digest_invalid")
        if self._prepared.get(digest) != normalized:
            raise ExecutionFenceUnavailable("descriptor_not_prepared")
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
        with self._lock:
            normalized = self._admitted(descriptor)
            digest = normalized["launch_descriptor_digest"]
            if digest in self._consumed:
                raise ExecutionFenceUnavailable("descriptor_replayed")
            if normalized["binding"]["expires_at"] <= self.clock():
                raise ExecutionFenceUnavailable("descriptor_expired")
            if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                    or timeout_seconds <= 0):
                raise ExecutionFenceUnavailable("launch_timeout_invalid")
            self._consumed.add(digest)
        command = [str(item) for item in argv]
        # Nothing is contained, so nothing is hidden: the child runs with the
        # operator's environment plus the caller's explicit projection.
        environment = {**os.environ, **{str(k): str(v) for k, v in (env_projection or {}).items()}}
        deadline = min(normalized["binding"]["expires_at"], time.time() + float(timeout_seconds))
        try:
            result = run_managed_process(command, cwd=normalized["binding"]["clone_root"],
                                         input_text=input_text, env=environment, deadline_at=deadline,
                                         on_started=on_started, identity_port=self.identity_port)
        except ManagedProcessUnknown as exc:
            self._lifecycles[digest] = exc.process_lifecycle
            raise ExecutionFenceUnavailable(f"local_process_{exc.reason}") from None
        except subprocess.TimeoutExpired as exc:
            lifecycle = getattr(exc, "process_lifecycle", None)
            if lifecycle is not None:
                self._lifecycles[digest] = lifecycle
            raise
        self._lifecycles[digest] = result.process_lifecycle
        return subprocess.CompletedProcess(command, result.returncode, result.stdout, result.stderr)

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._admitted(descriptor)
        digest = normalized["launch_descriptor_digest"]
        lifecycle = self._lifecycles.get(digest)
        return {
            "schema": DESCRIPTOR_SCHEMA,
            "status": "admitted",
            "launch_descriptor_digest": digest,
            "binding_digest": normalized["binding_digest"],
            "backend": dict(normalized["backend"]),
            "kernel_containment": False,
            "provider_egress_enforced": False,
            "proofs": normalized["proofs"],
            "proofs_digest": normalized["proofs_digest"],
            "provider_control_channel": normalized["binding"]["provider_control_channel"],
            "launch_classes": normalized["launch_classes"],
            "mutation_dispatch": normalized["mutation_dispatch"],
            "process_lifecycle_outcome": lifecycle.get("outcome") if isinstance(lifecycle, dict) else None,
        }


__all__ = ["BACKEND_ID", "LocalProcessExecutionFence"]

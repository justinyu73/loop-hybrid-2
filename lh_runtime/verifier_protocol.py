#!/usr/bin/env python3
"""Read-only verifier protocol with digest-bound candidate receipts."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from runner_adapter import CapabilityBoundRunner, CapabilityError

RECEIPT_SCHEMA = "host-verifier-receipt/v1"
REPAIR_ROUTE = "repair_same_node_new_attempt"


class VerifierProtocolError(ValueError):
    """A verifier packet cannot be admitted or verified safely."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def digest_json(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise VerifierProtocolError(f"{name}_missing")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    value = _text(name, value)
    if not value.startswith("sha256:") or len(value) != 71:
        raise VerifierProtocolError(f"{name}_invalid")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise VerifierProtocolError(f"{name}_invalid") from exc
    return value.lower()


def _commit(name: str, value: Any) -> str:
    value = _text(name, value)
    if len(value) != 40 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise VerifierProtocolError(f"{name}_invalid")
    return value.lower()


def _path_allowed(path: str, allowed_paths: Sequence[str]) -> bool:
    normalized = path.rstrip("/")
    return any(
        normalized == allowed.rstrip("/")
        or normalized.startswith(allowed.rstrip("/") + "/")
        for allowed in allowed_paths
    )


class ReadOnlyWorkspace:
    """Narrow source view passed to check adapters; mutation methods reject."""

    def __init__(self, root: str | Path):
        self._root = Path(root).resolve()

    def read_bytes(self, relative: str) -> bytes:
        path = (self._root / relative).resolve()
        if not path.is_relative_to(self._root):
            raise VerifierProtocolError("verifier_read_escape")
        return path.read_bytes()

    def read_text(self, relative: str, *, encoding: str = "utf-8") -> str:
        return self.read_bytes(relative).decode(encoding)

    def exists(self, relative: str) -> bool:
        path = (self._root / relative).resolve()
        return path.is_relative_to(self._root) and path.exists()

    def write_bytes(self, relative: str, data: bytes) -> None:
        del relative, data
        raise VerifierProtocolError("verifier_source_write_denied")

    def write_text(self, relative: str, data: str, *, encoding: str = "utf-8") -> None:
        del relative, data, encoding
        raise VerifierProtocolError("verifier_source_write_denied")

    def mkdir(self, relative: str) -> None:
        del relative
        raise VerifierProtocolError("verifier_source_write_denied")

    def unlink(self, relative: str) -> None:
        del relative
        raise VerifierProtocolError("verifier_source_write_denied")


def _normalise_checks(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise VerifierProtocolError("checks_missing")
    checks: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise VerifierProtocolError(f"check_{index}_invalid")
        check_id = _text(f"check_{index}.id", raw.get("id"))
        exit_code = raw.get("exit_code")
        if not isinstance(exit_code, int) or isinstance(exit_code, bool):
            raise VerifierProtocolError(f"check_{index}.exit_code_invalid")
        checks.append({
            "id": check_id,
            "exit_code": exit_code,
            "stdout_digest": _digest(f"check_{index}.stdout_digest", raw.get("stdout_digest")),
            "stderr_digest": _digest(f"check_{index}.stderr_digest", raw.get("stderr_digest")),
        })
    return checks


class VerifierProtocol:
    """Verify a candidate without receiving a writable source object."""

    def __init__(self, runner: CapabilityBoundRunner, *, node_id: str):
        self.runner = runner
        self.node_id = _text("node_id", node_id)

    def execute_checks(
        self,
        workspace: str | Path,
        candidate: Mapping[str, Any],
        *,
        work_unit_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        view = ReadOnlyWorkspace(workspace)
        try:
            result = self.runner.execute(
                "checks",
                {"workspace": view, "candidate": copy.deepcopy(dict(candidate))},
                work_unit_id=work_unit_id,
                attempt_id=attempt_id,
            )
        except CapabilityError as exc:
            raise VerifierProtocolError(exc.reason) from exc
        return result

    def verify_candidate(
        self,
        candidate: Mapping[str, Any],
        *,
        allowed_paths: Sequence[str],
        worker_binding: Mapping[str, Any],
        check_results: Any,
        packet_digest: str,
        plan_digest: str,
        work_unit_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        if not isinstance(candidate, Mapping):
            raise VerifierProtocolError("candidate_missing")
        node_id = _text("candidate.node_id", candidate.get("node_id"))
        if node_id != self.node_id:
            raise VerifierProtocolError("candidate_node_mismatch")
        base_sha = _commit("candidate.base_sha", candidate.get("base_sha"))
        candidate_sha = _commit("candidate.candidate_sha", candidate.get("candidate_sha"))
        packet_digest = _digest("packet_digest", packet_digest)
        plan_digest = _digest("plan_digest", plan_digest)
        if not isinstance(allowed_paths, Sequence) or not allowed_paths:
            raise VerifierProtocolError("allowed_paths_missing")
        normalized_allowed = [_text("allowed_path", item) for item in allowed_paths]
        changed_paths = candidate.get("changed_paths")
        if not isinstance(changed_paths, list) or any(not isinstance(item, str) for item in changed_paths):
            raise VerifierProtocolError("candidate_changed_paths_invalid")
        outside = [path for path in changed_paths if not _path_allowed(path, normalized_allowed)]
        if outside:
            raise VerifierProtocolError("candidate_path_outside_allowed_set")
        if not isinstance(worker_binding, Mapping):
            raise VerifierProtocolError("worker_binding_missing")
        worker_identity = _text("worker_binding.identity_digest", worker_binding.get("identity_digest"))
        try:
            verifier_binding = self.runner.binding(
                "verifier", work_unit_id=work_unit_id, attempt_id=attempt_id
            )
            checks_binding = self.runner.binding(
                "checks", work_unit_id=work_unit_id, attempt_id=attempt_id
            )
        except CapabilityError as exc:
            raise VerifierProtocolError(exc.reason) from exc
        if worker_identity == verifier_binding["identity_digest"]:
            raise VerifierProtocolError("verifier_identity_not_independent")
        checks = _normalise_checks(check_results)
        failed = [check["id"] for check in checks if check["exit_code"] != 0]
        body: dict[str, Any] = {
            "schema": RECEIPT_SCHEMA,
            "node_id": node_id,
            "base_sha": base_sha,
            "candidate_sha": candidate_sha,
            "packet_digest": packet_digest,
            "plan_digest": plan_digest,
            "work_unit_id": _text("work_unit_id", work_unit_id),
            "attempt_id": _text("attempt_id", attempt_id),
            "worker_identity_digest": worker_identity,
            "verifier_binding": {
                "adapter_id": verifier_binding["adapter_id"],
                "identity_digest": verifier_binding["identity_digest"],
                "contract_digest": verifier_binding["contract_digest"],
            },
            "checks_binding": {
                "adapter_id": checks_binding["adapter_id"],
                "identity_digest": checks_binding["identity_digest"],
                "contract_digest": checks_binding["contract_digest"],
            },
            "changed_paths": list(changed_paths),
            "allowed_paths": normalized_allowed,
            "source_write": "denied",
            "read_only": True,
            "checks": checks,
            "verdict": "RED" if failed else "GREEN",
            "route": REPAIR_ROUTE if failed else "advance",
        }
        if failed:
            body["reason"] = "check_failed:" + ",".join(failed)
        body["receipt_digest"] = digest_json(body)
        return body


__all__ = [
    "RECEIPT_SCHEMA",
    "REPAIR_ROUTE",
    "ReadOnlyWorkspace",
    "VerifierProtocol",
    "VerifierProtocolError",
    "digest_json",
]

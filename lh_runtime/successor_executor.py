"""Provider-neutral executor port for scheduler-visible successor packets.

The post-merge controller owns transition and queue admission.  This module
owns the next, deliberately separate boundary: an executor may accept a
packet only when it can leave durable task-receipt and heartbeat evidence.
The adapter in this file is an explicit, task-owned controlled adapter for
offline acceptance.  It is never selected implicitly by production code;
real Luna/Codex adapters implement :class:`SuccessorExecutorPort` at the
host boundary.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol


try:
    from .lifecycle import NativeProcessIdentityPort, ProcessIdentity, ProcessIdentityPort, observe_process_identity
    from .platform_ports import (
        FileLockPort, PlatformPortUnavailable, PortableFileLock, locked_file,
        make_file_private, read_private_file, sync_directory,
    )
except ImportError:
    from lifecycle import NativeProcessIdentityPort, ProcessIdentity, ProcessIdentityPort, observe_process_identity
    from platform_ports import (
        FileLockPort, PlatformPortUnavailable, PortableFileLock, locked_file,
        make_file_private, read_private_file, sync_directory,
    )


SCHEMA = "lh-successor-executor-receipt/v1"
TASK_RECEIPT_SCHEMA = "lh-successor-task-receipt/v1"
HEARTBEAT_SCHEMA = "lh-successor-task-heartbeat/v1"
FAILURE_RECEIPT_SCHEMA = "lh-successor-executor-failure/v1"
RECOVERY_RECEIPT_SCHEMA = "lh-successor-executor-recovery/v1"
TIMEOUT_OBSERVATION_SCHEMA = "lh-successor-executor-timeout-observation/v1"
MAX_TOTAL_LAUNCHES = 3


class SuccessorExecutorError(ValueError):
    """The executor port rejected a packet or could not prove ownership."""

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.details = dict(details or {})


class SuccessorExecutorPort(Protocol):
    """Provider-neutral port consumed after successor queue admission."""

    def dispatch(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        """Accept one idempotent packet and return digest-bound task evidence."""


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SuccessorExecutorError(f"{name}_missing")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    text = _required_text(name, value).lower()
    if not text.startswith("sha256:") or len(text) != 71:
        raise SuccessorExecutorError(f"{name}_invalid")
    try:
        int(text[7:], 16)
    except ValueError as exc:
        raise SuccessorExecutorError(f"{name}_invalid") from exc
    return text


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _safe_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _assignment_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: value[field]
        for field in ("worktree", "branch")
        if value.get(field) is not None
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _process_identity(*, identity_port: ProcessIdentityPort | None = None) -> dict[str, Any]:
    port = identity_port if identity_port is not None else NativeProcessIdentityPort()
    try:
        observed = port.current()
    except (OSError, ValueError, RuntimeError) as exc:
        raise SuccessorExecutorError("process_identity_observation_unavailable") from exc
    identity = ProcessIdentity.from_dict(observed.as_dict() if observed is not None else None)
    if identity is None:
        raise SuccessorExecutorError("process_identity_observation_unavailable")
    return identity.as_dict()


def _process_identity_for_pid(pid: int, *, identity_port: ProcessIdentityPort | None = None) -> dict[str, Any]:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid < 1:
        raise SuccessorExecutorError("process_identity_invalid")
    port = identity_port if identity_port is not None else NativeProcessIdentityPort()
    try:
        observed = port.observe(pid)
    except (OSError, ValueError, RuntimeError) as exc:
        raise SuccessorExecutorError("process_identity_observation_unavailable") from exc
    identity = ProcessIdentity.from_dict(observed.as_dict() if observed is not None else None)
    if identity is None or identity.pid != pid:
        raise SuccessorExecutorError("process_identity_observation_unavailable")
    return identity.as_dict()

def _atomic_create(path: pathlib.Path, value: Mapping[str, Any], *,
                   private_durable: bool = False) -> bool:
    encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if path.exists():
        try:
            existing = read_private_file(path) if private_durable else path.read_bytes()
            if existing != encoded:
                return False
            if private_durable:
                sync_directory(path.parent)
            return True
        except OSError as exc:
            raise SuccessorExecutorError("executor_receipt_readback_failed") from exc
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor: int | None = None
    temporary: pathlib.Path | None = None
    try:
        descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = pathlib.Path(raw)
        make_file_private(descriptor, temporary)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise SuccessorExecutorError("executor_receipt_write_failed") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None and temporary.exists():
            temporary.unlink()
    if private_durable:
        if read_private_file(path) != encoded:
            return False
        sync_directory(path.parent)
    return True


def _read_object(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SuccessorExecutorError("executor_evidence_unreadable") from exc
    if not isinstance(value, dict):
        raise SuccessorExecutorError("executor_evidence_not_object")
    return value


def _sanitize_diagnostic(value: Any, *, limit: int = 2048) -> str:
    """Keep bounded diagnostics without persisting credentials or transcripts."""

    text = value if isinstance(value, str) else str(value or "")
    text = re.sub(r"(?i)(api[_-]?key|authorization|cookie|token)\s*[:=]\s*\S+", r"\1=[redacted]", text)
    text = re.sub(r"(?i)bearer\s+\S+", "Bearer [redacted]", text)
    return text[:limit]


def _workspace_snapshot(worktree: pathlib.Path) -> dict[str, Any]:
    """Read a small, digestable view of task-worktree effects.

    The executor never resets or cleans a task worktree.  A Git status is the
    preferred observation; a directory listing is retained for the controlled
    non-Git test worktree.  Only the digest is persisted in failure evidence.
    """

    try:
        completed = subprocess.run(
            ["git", "-C", str(worktree), "status", "--porcelain=v1", "--untracked-files=all"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        completed = None
    if completed is not None and completed.returncode == 0:
        status = completed.stdout
        return {
            "method": "git_status",
            "status_digest": digest_json(status),
            "entry_count": len(status.splitlines()),
        }
    try:
        entries = []
        for path in sorted(worktree.rglob("*")):
            if path.is_file():
                entries.append(path.relative_to(worktree).as_posix())
    except OSError:
        entries = []
    return {
        "method": "directory_listing",
        "entry_digest": digest_json(entries),
        "entry_count": len(entries),
    }


def _workspace_diff_digest(before: Any, after: Any) -> str | None:
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return None
    return digest_json({"before": dict(before), "after": dict(after)})


def _process_is_alive(identity: Any, *, identity_port: ProcessIdentityPort | None = None) -> bool:
    observation = observe_process_identity(identity, identity_port=identity_port)
    if observation.status == "unknown":
        raise SuccessorExecutorError("process_observation_unavailable")
    return observation.status == "alive"


def disposition_process_dead(identity: Mapping[str, Any], *, identity_port: ProcessIdentityPort | None = None) -> bool:
    """Observe a complete process identity without inferring a historical exit."""
    if ProcessIdentity.from_dict(identity) is None:
        raise SuccessorExecutorError("disposition_process_identity_incomplete")
    observation = observe_process_identity(identity, identity_port=identity_port)
    if observation.status == "unknown":
        raise SuccessorExecutorError("disposition_process_observation_unavailable")
    return observation.status == "confirmed_dead"

def disposition_workspace_inventory(worktree: str) -> dict[str, Any]:
    """Hash current bytes, including ignored files; never infer a lost baseline."""
    root = pathlib.Path(worktree).expanduser().resolve(strict=True)
    def git(*args):
        return subprocess.check_output(["git", "--no-optional-locks", "-C", str(root), *args], stderr=subprocess.PIPE).decode().strip()
    try:
        if pathlib.Path(git("rev-parse", "--show-toplevel")).resolve() != root:
            raise ValueError("workspace is not a repository root")
        entries = []
        def walk_error(error):
            raise error
        for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
            dirs[:] = sorted(d for d in dirs if not (pathlib.Path(directory) == root and d == ".git"))
            for name in sorted(files + [d for d in dirs if (pathlib.Path(directory) / d).is_symlink()]):
                path = pathlib.Path(directory) / name
                if path == root / ".git":
                    continue
                relative = path.relative_to(root).as_posix()
                if path.is_symlink():
                    entries.append([relative, "symlink", os.readlink(path)])
                else:
                    mode = path.lstat().st_mode
                    if not stat.S_ISREG(mode):
                        raise ValueError("nonregular workspace entry")
                    entries.append([relative, mode, hashlib.sha256(path.read_bytes()).hexdigest()])
        body = {"workspace_ref": str(root), "head": git("rev-parse", "HEAD"),
                "branch": git("symbolic-ref", "--short", "HEAD"),
                "status": git("status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"),
                "current_inventory_only": True, "entries": entries}
        return {"workspace_ref": str(root), "head": body["head"], "branch": body["branch"],
                "clean": not body["status"], "current_inventory_only": True,
                "inventory_digest": digest_json(body), "inventory_body": body}
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise SuccessorExecutorError("disposition_workspace_inventory_unavailable") from exc


def _terminate_process(process: Any) -> bool:
    killed = False
    try:
        process.kill()
        killed = True
    except (OSError, AttributeError, TypeError):
        return False
    try:
        process.communicate(timeout=10)
    except (OSError, subprocess.SubprocessError, AttributeError, TypeError):
        return False
    return killed


class DurableTaskExecutorAdapter:
    """Explicit controlled adapter that leaves real task evidence on disk.

    This adapter does not invoke a model or provider.  Its ownership is the
    current bounded process, identified by PID/starttime/boot id.  The durable
    task receipt and heartbeat are the evidence consumed by the scheduler and
    by independent verification.  A repeated key returns the original
    evidence without a second invocation.
    """

    def __init__(
        self,
        root: str | pathlib.Path,
        *,
        executor_id: str = "controlled-headless-executor",
        process_identity: Mapping[str, Any] | None = None,
        identity_port: ProcessIdentityPort | None = None,
        lock_port: FileLockPort | None = None,
    ):
        self.identity_port = identity_port if identity_port is not None else NativeProcessIdentityPort()
        self.lock_port = lock_port if lock_port is not None else PortableFileLock()
        self.root = pathlib.Path(root).expanduser().resolve()
        self.executor_id = _required_text("executor_id", executor_id)
        self.process_identity = dict(process_identity or _process_identity(identity_port=self.identity_port))
        if not isinstance(self.process_identity.get("pid"), int) or self.process_identity["pid"] < 1:
            raise SuccessorExecutorError("process_identity_invalid")
        self.invocations = 0

    @contextmanager
    def _lock(self, path: pathlib.Path):
        try:
            with locked_file(path, lock_port=self.lock_port):
                yield
        except PlatformPortUnavailable as exc:
            raise SuccessorExecutorError(exc.reason) from exc

    def _paths(self, dispatch_key: str) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
        safe = _safe_key(dispatch_key)
        directory = self.root / "successor-executor"
        return (
            directory / f"{safe}.task-receipt.json",
            directory / f"{safe}.heartbeat.json",
            directory / f"{safe}.lock",
        )

    def _validate_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            raise SuccessorExecutorError("executor_request_invalid")
        result = dict(request)
        _required_text("dispatch_key", result.get("dispatch_key"))
        _digest("envelope_digest", result.get("envelope_digest"))
        packet_path = pathlib.Path(_required_text("packet_path", result.get("packet_path"))).expanduser().resolve()
        packet_digest = _digest("packet_digest", result.get("packet_digest"))
        try:
            packet = json.loads(packet_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SuccessorExecutorError("executor_packet_unreadable") from exc
        if not isinstance(packet, dict) or packet.get("packet_digest") != packet_digest:
            raise SuccessorExecutorError("executor_packet_digest_mismatch")
        body = copy.deepcopy(packet)
        body.pop("packet_digest", None)
        if digest_json(body) != packet_digest:
            raise SuccessorExecutorError("executor_packet_digest_mismatch")
        result["packet_path"] = str(packet_path)
        result["packet_digest"] = packet_digest
        for field in ("goal_id", "node_id", "work_unit_id", "run_id"):
            _required_text(field, result.get(field))
        if result.get("worktree") is not None:
            result["worktree"] = str(
                pathlib.Path(_required_text("worktree", result["worktree"])).expanduser().resolve()
            )
        if result.get("branch") is not None:
            result["branch"] = _required_text("branch", result["branch"])
        for field in ("goal_revision", "attempt", "fence"):
            value = result.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise SuccessorExecutorError(f"{field}_invalid")
        return result

    @staticmethod
    def _validate_stored(
        value: Mapping[str, Any],
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        body = copy.deepcopy(dict(value))
        supplied = body.pop("receipt_digest", None)
        # These flags describe the in-process port result, not the durable
        # receipt bytes.  They must never change the digest-bound evidence.
        body.pop("reused", None)
        body.pop("invoked", None)
        expected_fields = {"execution_binding_digest", "policy_digest"}
        expected_present = expected_fields.intersection(request)
        if expected_present and expected_present != expected_fields:
            raise SuccessorExecutorError("trusted_executor_expected_binding_invalid")
        if expected_present and any(
            not isinstance(request[name], str) or re.fullmatch(r"sha256:[0-9a-f]{64}", request[name]) is None
            for name in expected_fields
        ):
            raise SuccessorExecutorError("trusted_executor_expected_binding_invalid")
        trusted = value.get("schema") == "lh-successor-executor-receipt/v2"
        if expected_present and not trusted:
            raise SuccessorExecutorError("trusted_executor_mode_mismatch")
        if value.get("schema") not in {SCHEMA, "lh-successor-executor-receipt/v2"} or supplied != digest_json(body):
            raise SuccessorExecutorError("executor_receipt_digest_mismatch")
        if value.get("status") != "accepted" or value.get("invocation_count") != 1:
            raise SuccessorExecutorError("executor_receipt_status_invalid")
        for field in ("dispatch_key", "envelope_digest", "packet_digest", "goal_id", "node_id", "work_unit_id", "run_id"):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_receipt_binding_mismatch")
        for field in ("goal_revision", "attempt", "fence"):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_receipt_binding_mismatch")
        for field in ("worktree", "branch"):
            if value.get(field) is not None or request.get(field) is not None:
                if value.get(field) != request.get(field):
                    raise SuccessorExecutorError("executor_receipt_assignment_mismatch")
        task_path = pathlib.Path(_required_text("task_receipt_path", value.get("task_receipt_path")))
        heartbeat_path = pathlib.Path(_required_text("heartbeat_path", value.get("heartbeat_path")))
        task = _read_object(task_path)
        heartbeat = _read_object(heartbeat_path)
        task_body = copy.deepcopy(task)
        task_digest = task_body.pop("receipt_digest", None)
        heartbeat_body = copy.deepcopy(heartbeat)
        heartbeat_digest = heartbeat_body.pop("heartbeat_digest", None)
        if task.get("schema") != ("lh-successor-task-receipt/v2" if trusted else TASK_RECEIPT_SCHEMA) or task_digest != digest_json(task_body):
            raise SuccessorExecutorError("task_receipt_digest_mismatch")
        if heartbeat.get("schema") != ("lh-successor-task-heartbeat/v2" if trusted else HEARTBEAT_SCHEMA) or heartbeat_digest != digest_json(heartbeat_body):
            raise SuccessorExecutorError("heartbeat_digest_mismatch")
        if value.get("task_receipt_digest") != task_digest or value.get("heartbeat_digest") != heartbeat_digest:
            raise SuccessorExecutorError("executor_evidence_binding_mismatch")
        binding_fields = (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt",
            "fence", "executor_id",
        )
        for field in binding_fields:
            if task.get(field) != value.get(field) or heartbeat.get(field) != value.get(field):
                raise SuccessorExecutorError("executor_evidence_binding_mismatch")
        for field in ("worktree", "branch"):
            if value.get(field) is not None or task.get(field) is not None or heartbeat.get(field) is not None:
                if task.get(field) != value.get(field) or heartbeat.get(field) != value.get(field):
                    raise SuccessorExecutorError("executor_evidence_assignment_mismatch")
        if task.get("status") != "accepted" or heartbeat.get("status") != "alive":
            raise SuccessorExecutorError("executor_evidence_status_invalid")
        if task.get("packet_path") != request.get("packet_path"):
            raise SuccessorExecutorError("executor_evidence_binding_mismatch")
        if task.get("heartbeat_path") != value.get("heartbeat_path"):
            raise SuccessorExecutorError("executor_evidence_binding_mismatch")
        if value.get("process_identity") != task.get("process_identity") or value.get("process_identity") != heartbeat.get("process_identity"):
            raise SuccessorExecutorError("executor_evidence_binding_mismatch")
        if value.get("invocation_digest") is not None:
            if task.get("invocation_digest") != value.get("invocation_digest") or heartbeat.get("invocation_digest") != value.get("invocation_digest"):
                raise SuccessorExecutorError("executor_invocation_binding_conflict")
        for evidence in (value, task, heartbeat):
            if (evidence.get("provider_invocations", 0) != (None if trusted else 0)
                    or evidence.get("manual_prompts", 0) != 0):
                raise SuccessorExecutorError("executor_provider_boundary_crossed")
            if not trusted and any(name in evidence for name in (
                "policy_digest", "execution_binding_digest", "execution_assurance", "provider_execution", "cli_launches"
            )):
                raise SuccessorExecutorError("trusted_executor_mode_mismatch")
        if trusted:
            if __package__:
                from .execution_fence_trusted import validate_trusted_assurance, ExecutionFenceUnavailable
            else:
                from execution_fence_trusted import validate_trusted_assurance, ExecutionFenceUnavailable
            try:
                assurance = validate_trusted_assurance(value.get("execution_assurance"))
            except ExecutionFenceUnavailable:
                raise SuccessorExecutorError("trusted_executor_assurance_invalid") from None
            for evidence in (value, task, heartbeat):
                if evidence.get("provider_invocations") is not None or evidence.get("cli_launches") != 1:
                    raise SuccessorExecutorError("trusted_executor_counters_invalid")
                for name in ("execution_binding_digest", "policy_digest"):
                    if evidence.get(name) != assurance[name] or (name in request and request[name] != evidence[name]):
                        raise SuccessorExecutorError("trusted_executor_binding_conflict")
                if "execution_fence" in evidence:
                    raise SuccessorExecutorError("trusted_kernel_proof_forbidden")
            if task.get("execution_assurance") != assurance or task.get("provider_execution") != value.get("provider_execution"):
                raise SuccessorExecutorError("trusted_executor_evidence_conflict")
        return dict(value)

    def dispatch(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        value = self._validate_request(request)
        dispatch_key = str(value["dispatch_key"])
        task_path, heartbeat_path, lock_path = self._paths(dispatch_key)
        lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock(lock_path):
            if task_path.exists():
                stored_path = task_path.with_suffix(".executor-receipt.json")
                if not stored_path.exists():
                    raise SuccessorExecutorError("executor_receipt_missing")
                stored = self._validate_stored(_read_object(stored_path), value)
                return {**stored, "reused": True, "invoked": False}

            heartbeat_body = {
                "schema": HEARTBEAT_SCHEMA,
                "status": "alive",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "process_identity": copy.deepcopy(self.process_identity),
                **_assignment_fields(value),
                "observed_at": _utc_now(),
            }
            heartbeat = {**heartbeat_body, "heartbeat_digest": digest_json(heartbeat_body)}
            _atomic_create(heartbeat_path, heartbeat)

            task_body = {
                "schema": TASK_RECEIPT_SCHEMA,
                "status": "accepted",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "process_identity": copy.deepcopy(self.process_identity),
                **_assignment_fields(value),
                "packet_path": value["packet_path"],
                "heartbeat_path": str(heartbeat_path),
                "heartbeat_digest": heartbeat["heartbeat_digest"],
                "accepted_at": _utc_now(),
            }
            task = {**task_body, "receipt_digest": digest_json(task_body)}
            _atomic_create(task_path, task)

            receipt_body = {
                "schema": SCHEMA,
                "status": "accepted",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "process_identity": copy.deepcopy(self.process_identity),
                "task_receipt_path": str(task_path),
                "task_receipt_digest": task["receipt_digest"],
                "heartbeat_path": str(heartbeat_path),
                "heartbeat_digest": heartbeat["heartbeat_digest"],
                **_assignment_fields(value),
                "invocation_count": 1,
                "provider_invocations": 0,
                "manual_prompts": 0,
                "accepted_at": _utc_now(),
            }
            receipt = {**receipt_body, "receipt_digest": digest_json(receipt_body)}
            receipt_path = task_path.with_suffix(".executor-receipt.json")
            _atomic_create(receipt_path, receipt)
            self._validate_stored(receipt, value)
            self.invocations += 1
            return {**receipt, "reused": False, "invoked": True}


class CodexSubscriptionExecutorAdapter(DurableTaskExecutorAdapter):
    """Execute one successor packet through the installed Codex subscription CLI.

    This is an explicit host adapter, not a resident watcher and not an API
    client.  The fleet scheduler selects it only for the explicit
    ``codex-subscription`` mode.  A pre-launch invocation reservation is
    durable under the same dispatch lock; a restart with no completed task
    receipt fails closed instead of invoking Codex a second time.
    """

    provider = "codex-subscription"
    adapter = "lh_runtime.cli_agent_executor.CODEX"
    invocation_schema = "lh-codex-subscription-invocation/v1"

    def __init__(
        self,
        root: str | pathlib.Path,
        *,
        executor_id: str = "codex-subscription-luna",
        timeout_seconds: float = 900.0,
        spawn: Callable[..., Any] = subprocess.Popen,
        identity_port: ProcessIdentityPort | None = None,
        lock_port: FileLockPort | None = None,
        execution_fence_port: Any = None,
        execution_binding: Any = None,
    ):
        super().__init__(root, executor_id=executor_id, identity_port=identity_port, lock_port=lock_port)
        if float(timeout_seconds) <= 0:
            raise SuccessorExecutorError("executor_timeout_invalid")
        self.timeout_seconds = float(timeout_seconds)
        self.spawn = spawn
        self.execution_binding = execution_binding
        self.execution_fence_port = execution_binding.port if execution_binding is not None else execution_fence_port

    def _trusted_fields(self):
        binding = getattr(self, "execution_binding", None)
        if binding is None or binding.policy is None:
            return {}
        result = {"execution_binding_digest": binding.digest,
                  "policy_digest": digest_json(binding.policy)}
        continuation = getattr(binding, "continuation_authority_digest", None)
        if continuation is not None:
            result["continuation_authority_digest"] = continuation
        return result

    def _command(self, request, packet):
        try:
            from .cli_agent_executor import codex_argv, resolve_cli
        except ImportError:
            from cli_agent_executor import codex_argv, resolve_cli
        argv = list(codex_argv(self._prompt(request, packet)))
        if not argv:
            raise SuccessorExecutorError("codex_argv_empty")
        return [resolve_cli(argv[0]), *argv[1:]]

    @staticmethod
    def _packet(path: pathlib.Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SuccessorExecutorError("executor_packet_unreadable") from exc
        if not isinstance(value, dict):
            raise SuccessorExecutorError("executor_packet_invalid")
        return value

    @staticmethod
    def _prompt(request: Mapping[str, Any], packet: Mapping[str, Any]) -> str:
        task = packet.get("task")
        task_text = task.strip() if isinstance(task, str) and task.strip() else "Follow the digest-bound packet instructions."
        return (
            "You are the authorized task executor for an existing Goal.\n"
            f"Goal={request.get('goal_id')} rev={request.get('goal_revision')} node={request.get('node_id')}.\n"
            f"Read the digest-bound packet at {request.get('packet_path')} and verify packet_digest={request.get('packet_digest')}.\n"
            f"Work only in the assigned worktree: {request.get('worktree')}.\n"
            "Complete the packet task and its required checks. Do not create another Goal, Run, or Attempt; do not commit, push, merge, or modify production state, CURSOR.md, or the decision ledger.\n"
            f"Task: {task_text}\n"
            "Completion repair evidence (machine check hashes, not new authority): "
            + json.dumps(request.get("completion_repair", []), sort_keys=True)[:16384]
        )

    @staticmethod
    def _process_result(process: Any, *, timeout_seconds: float) -> tuple[int, str, str]:
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            terminated = _terminate_process(process)
            raise SuccessorExecutorError(
                "executor_process_timeout",
                details={
                    "termination_proof": {
                        "timeout": True,
                        "termination_attempted": True,
                        "process_terminated": terminated,
                    }
                },
            ) from exc
        except (OSError, subprocess.SubprocessError) as exc:
            raise SuccessorExecutorError("executor_process_readback_failed") from exc
        code = getattr(process, "returncode", None)
        if isinstance(code, bool) or not isinstance(code, int):
            raise SuccessorExecutorError("executor_process_exit_invalid")
        return code, stdout if isinstance(stdout, str) else str(stdout or ""), stderr if isinstance(stderr, str) else str(stderr or "")

    def _validate_invocation(
        self,
        value: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        worktree: pathlib.Path,
    ) -> dict[str, Any]:
        body = copy.deepcopy(dict(value))
        supplied = body.pop("invocation_digest", None)
        if (
            value.get("schema") != self.invocation_schema
            or value.get("status") != "reserved"
            or not isinstance(supplied, str)
            or supplied != digest_json(body)
        ):
            raise SuccessorExecutorError("executor_invocation_binding_conflict")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
        ):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_invocation_binding_conflict")
        if (
            value.get("executor_id") != self.executor_id
            or value.get("provider") != self.provider
            or value.get("adapter") != self.adapter
            or value.get("worktree") != str(worktree)
        ):
            raise SuccessorExecutorError("executor_invocation_binding_conflict")
        if request.get("branch") is not None and value.get("branch") != request.get("branch"):
            raise SuccessorExecutorError("executor_invocation_binding_conflict")
        if any(value.get(key) != expected for key, expected in self._trusted_fields().items()):
            raise SuccessorExecutorError("trusted_executor_binding_conflict")
        return dict(value)

    def _attempt_paths(
        self,
        dispatch_key: str,
        *,
        attempt: int,
        fence: int,
    ) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
        """Keep the original reservation path and scope later attempts.

        The unscoped path is part of the already deployed rev6 evidence.  It
        is therefore never replaced.  A later Attempt gets a distinct durable
        idempotency namespace while the logical dispatch key stays unchanged.
        """

        if attempt == 1:
            return self._paths(dispatch_key)
        return self._paths(f"{dispatch_key}|attempt={attempt}|fence={fence}")

    @staticmethod
    def _timeout_observation_path(task_path: pathlib.Path) -> pathlib.Path:
        """Return the append-only evidence path for one invocation namespace."""
        return task_path.with_suffix(".timeout-observation.json")

    def _validate_timeout_observation(
        self,
        value: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        reservation: Mapping[str, Any],
        heartbeat: Mapping[str, Any],
        observation_path: pathlib.Path,
        invocation_path: pathlib.Path,
        heartbeat_path: pathlib.Path,
    ) -> dict[str, Any]:
        """Validate timeout facts without turning them into an exit proof."""
        body = copy.deepcopy(dict(value))
        supplied = body.pop("timeout_observation_digest", None)
        if (
            value.get("schema") != TIMEOUT_OBSERVATION_SCHEMA
            or value.get("status") != "observed"
            or not isinstance(supplied, str)
            or supplied != digest_json(body)
        ):
            raise SuccessorExecutorError("executor_timeout_observation_invalid")
        if value.get("observation_path") != str(observation_path):
            raise SuccessorExecutorError("executor_timeout_observation_path_conflict")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
        ):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_timeout_observation_binding_conflict")
        if value.get("invocation_digest") != reservation.get("invocation_digest"):
            raise SuccessorExecutorError("executor_timeout_observation_invocation_conflict")
        if value.get("invocation_path") != str(invocation_path):
            raise SuccessorExecutorError("executor_timeout_observation_invocation_path_conflict")
        if (
            value.get("executor_id") != self.executor_id
            or value.get("provider") != self.provider
            or value.get("adapter") != self.adapter
        ):
            raise SuccessorExecutorError("executor_timeout_observation_executor_conflict")
        for field in ("worktree", "branch"):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_timeout_observation_assignment_conflict")
        if (
            value.get("heartbeat_path") != str(heartbeat_path)
            or heartbeat.get("heartbeat_path") != str(heartbeat_path)
        ):
            raise SuccessorExecutorError("executor_timeout_observation_heartbeat_conflict")
        heartbeat_body = copy.deepcopy(dict(heartbeat))
        heartbeat_digest = heartbeat_body.pop("heartbeat_digest", None)
        if (
            heartbeat.get("schema") != ("lh-successor-task-heartbeat/v2" if self._trusted_fields() else HEARTBEAT_SCHEMA)
            or heartbeat.get("status") != "alive"
            or heartbeat_digest != digest_json(heartbeat_body)
            or value.get("heartbeat_digest") != heartbeat_digest
        ):
            raise SuccessorExecutorError("executor_timeout_observation_heartbeat_invalid")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
        ):
            if heartbeat.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_timeout_observation_heartbeat_conflict")
        if heartbeat.get("invocation_digest") != reservation.get("invocation_digest"):
            raise SuccessorExecutorError("executor_timeout_observation_heartbeat_conflict")
        if value.get("process_identity") != heartbeat.get("process_identity"):
            raise SuccessorExecutorError("executor_timeout_observation_process_conflict")
        if not isinstance(value.get("process_identity"), Mapping):
            raise SuccessorExecutorError("executor_timeout_observation_process_invalid")
        if value.get("timeout") is not True or value.get("termination_attempted") is not True:
            raise SuccessorExecutorError("executor_timeout_observation_facts_invalid")
        if value.get("process_terminated") is not False:
            raise SuccessorExecutorError("executor_timeout_observation_termination_conflict")
        timeout_seconds = value.get("timeout_seconds")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise SuccessorExecutorError("executor_timeout_observation_timeout_invalid")
        return dict(value)

    def _timeout_observation(
        self,
        request: Mapping[str, Any],
        reservation: Mapping[str, Any],
        *,
        process_identity: Mapping[str, Any],
        heartbeat: Mapping[str, Any],
        observation_path: pathlib.Path,
        invocation_path: pathlib.Path,
        termination_proof: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Build bounded timeout facts; no exit, stderr, or termination inference."""
        body = {
            "schema": TIMEOUT_OBSERVATION_SCHEMA,
            **self._trusted_fields(),
            "status": "observed",
            "observation_path": str(observation_path),
            "dispatch_key": request["dispatch_key"],
            "envelope_digest": request["envelope_digest"],
            "packet_digest": request["packet_digest"],
            "goal_id": request["goal_id"],
            "goal_revision": request["goal_revision"],
            "node_id": request["node_id"],
            "work_unit_id": request["work_unit_id"],
            "run_id": request["run_id"],
            "attempt": request["attempt"],
            "fence": request["fence"],
            "executor_id": self.executor_id,
            "provider": self.provider,
            "adapter": self.adapter,
            **_assignment_fields({**request, "worktree": request.get("worktree")}),
            "invocation_digest": reservation["invocation_digest"],
            "invocation_path": str(invocation_path),
            "heartbeat_path": heartbeat.get("heartbeat_path"),
            "heartbeat_digest": heartbeat.get("heartbeat_digest"),
            "process_identity": copy.deepcopy(dict(process_identity)),
            "timeout": True,
            "timeout_seconds": self.timeout_seconds,
            "termination_attempted": termination_proof.get("termination_attempted") is True,
            "process_terminated": termination_proof.get("process_terminated") is True,
            "observed_at": _utc_now(),
        }
        return {**body, "timeout_observation_digest": digest_json(body)}

    @staticmethod
    def _validate_failure_receipt(
        value: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        worktree: pathlib.Path,
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        body = copy.deepcopy(dict(value))
        supplied = body.pop("failure_receipt_digest", None)
        if (
            value.get("schema") != FAILURE_RECEIPT_SCHEMA
            or value.get("status") != "failed"
            or value.get("outcome") != "known_failure"
            or value.get("retryable") is not True
            or not isinstance(supplied, str)
            or supplied != digest_json(body)
        ):
            raise SuccessorExecutorError("executor_failure_receipt_invalid")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
        ):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_failure_binding_conflict")
        if value.get("invocation_digest") != reservation.get("invocation_digest"):
            raise SuccessorExecutorError("executor_failure_binding_conflict")
        for field in ("execution_binding_digest", "policy_digest"):
            if field in reservation and value.get(field) != reservation[field]:
                raise SuccessorExecutorError("trusted_executor_binding_conflict")
        if value.get("worktree") != str(worktree):
            raise SuccessorExecutorError("executor_failure_assignment_conflict")
        if request.get("branch") is not None and value.get("branch") != request.get("branch"):
            raise SuccessorExecutorError("executor_failure_assignment_conflict")
        code = value.get("exit_code")
        if code is not None and (isinstance(code, bool) or not isinstance(code, int)):
            raise SuccessorExecutorError("executor_failure_exit_invalid")
        proof = value.get("timeout_and_termination_proof")
        if not isinstance(proof, Mapping) or proof.get("process_terminated") is not True:
            raise SuccessorExecutorError("executor_failure_termination_unproven")
        if value.get("workspace_effects_reconciled") is not True:
            raise SuccessorExecutorError("executor_failure_workspace_unreconciled")
        return dict(value)

    @staticmethod
    def _validate_recovery_receipt(
        value: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        reservation: Mapping[str, Any],
        timeout_observation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = copy.deepcopy(dict(value))
        supplied = body.pop("recovery_receipt_digest", None)
        if (
            value.get("schema") != RECOVERY_RECEIPT_SCHEMA
            or value.get("status") != "reconciled"
            or value.get("outcome") != "unknown"
            or not isinstance(supplied, str)
            or supplied != digest_json(body)
        ):
            raise SuccessorExecutorError("executor_recovery_receipt_invalid")
        for field in (
            "dispatch_key", "envelope_digest", "packet_digest", "goal_id",
            "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
        ):
            if value.get(field) != request.get(field):
                raise SuccessorExecutorError("executor_recovery_binding_conflict")
        if value.get("invocation_digest") != reservation.get("invocation_digest"):
            raise SuccessorExecutorError("executor_recovery_binding_conflict")
        evidence = value.get("failure_evidence")
        if not isinstance(evidence, Mapping):
            raise SuccessorExecutorError("executor_recovery_failure_evidence_invalid")
        if evidence.get("exit_code") is not None:
            raise SuccessorExecutorError("executor_recovery_exit_must_be_unknown")
        if evidence.get("stderr") != "unavailable":
            raise SuccessorExecutorError("executor_recovery_stderr_must_be_unavailable")
        observation_digest = value.get("timeout_observation_digest")
        if observation_digest is not None:
            if timeout_observation is None or observation_digest != timeout_observation.get("timeout_observation_digest"):
                raise SuccessorExecutorError("executor_recovery_timeout_observation_mismatch")
            if value.get("timeout_observation_path") != timeout_observation.get("observation_path"):
                raise SuccessorExecutorError("executor_recovery_timeout_observation_path_mismatch")
            if evidence.get("timeout_observation_digest") != observation_digest:
                raise SuccessorExecutorError("executor_recovery_timeout_evidence_mismatch")
        elif timeout_observation is not None:
            raise SuccessorExecutorError("executor_recovery_timeout_observation_missing")
        return dict(value)

    def _failure_receipt(
        self,
        request: Mapping[str, Any],
        reservation: Mapping[str, Any],
        worktree: pathlib.Path,
        *,
        phase: str,
        process_identity: Mapping[str, Any] | None,
        exit_code: int | None,
        stdout: Any = "",
        stderr: Any = "",
        termination_proof: Mapping[str, Any] | None = None,
        invocation_path: pathlib.Path | None = None,
    ) -> dict[str, Any]:
        after = _workspace_snapshot(worktree)
        before = reservation.get("workspace_before")
        baseline = before if isinstance(before, Mapping) else {"status": "unavailable"}
        diff_digest = _workspace_diff_digest(baseline, after) or digest_json({"before": baseline, "after": after})
        proof = dict(termination_proof or {})
        proof.setdefault("process_terminated", True)
        body = {
            "schema": FAILURE_RECEIPT_SCHEMA,
            **self._trusted_fields(),
            "status": "failed",
            "outcome": "known_failure",
            "retryable": True,
            "dispatch_key": request["dispatch_key"],
            "envelope_digest": request["envelope_digest"],
            "packet_digest": request["packet_digest"],
            "goal_id": request["goal_id"],
            "goal_revision": request["goal_revision"],
            "node_id": request["node_id"],
            "work_unit_id": request["work_unit_id"],
            "run_id": request["run_id"],
            "attempt": request["attempt"],
            "fence": request["fence"],
            "executor_id": self.executor_id,
            "provider": self.provider,
            "adapter": self.adapter,
            **_assignment_fields({**request, "worktree": str(worktree)}),
            "invocation_digest": reservation["invocation_digest"],
            "invocation_path": str(invocation_path) if invocation_path is not None else None,
            "process_identity": copy.deepcopy(dict(process_identity or {})),
            "phase": phase,
            "exit_code": exit_code,
            "timeout_and_termination_proof": proof,
            "sanitized_diagnostics": {
                "stdout": _sanitize_diagnostic(stdout),
                "stderr": _sanitize_diagnostic(stderr),
                "stdout_digest": digest_json(_sanitize_diagnostic(stdout)),
                "stderr_digest": digest_json(_sanitize_diagnostic(stderr)),
            },
            "workspace_before": copy.deepcopy(before),
            "workspace_after": after,
            "workspace_diff_digest": diff_digest,
            "workspace_baseline_available": isinstance(before, Mapping),
            "workspace_effects_reconciled": isinstance(before, Mapping),
            "recorded_at": _utc_now(),
        }
        if self._trusted_fields():
            body["sanitized_diagnostics"] = {
                "stdout_digest": digest_json(stdout), "stderr_digest": digest_json(stderr),
                "stdout_bytes": len(str(stdout).encode()), "stderr_bytes": len(str(stderr).encode())}
        return {**body, "failure_receipt_digest": digest_json(body)}

    def _unknown_recovery_receipt(
        self,
        request: Mapping[str, Any],
        reservation: Mapping[str, Any],
        worktree: pathlib.Path,
        *,
        reason: str,
        heartbeat: Mapping[str, Any] | None,
        invocation_path: pathlib.Path | None = None,
        timeout_observation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        after = _workspace_snapshot(worktree)
        before = reservation.get("workspace_before")
        observed_identity = (
            heartbeat.get("process_identity")
            if isinstance(heartbeat, Mapping) and heartbeat.get("process_identity") is not None
            else reservation.get("process_identity")
        )
        process_observation = observe_process_identity(observed_identity, identity_port=self.identity_port)
        baseline = before if isinstance(before, Mapping) else {"status": "unavailable"}
        workspace_diff_digest = _workspace_diff_digest(baseline, after) or digest_json({"before": baseline, "after": after})
        observation_fields = {}
        if timeout_observation is not None:
            observation_fields = {
                "timeout_observation_path": timeout_observation.get("observation_path"),
                "timeout_observation_digest": timeout_observation.get("timeout_observation_digest"),
            }
        failure_evidence = {
            "phase": "reservation_reconciliation",
            "exit_code": None,
            "stderr": "unavailable",
            "unrecoverable": True,
            "reason": "legacy reservation did not persist exit code or stderr",
            "termination_proof": False,
        }
        if timeout_observation is not None:
            failure_evidence.update({
                "phase": "process_timeout",
                "reason": "timeout observed without termination proof",
                "timeout_observation_digest": timeout_observation.get("timeout_observation_digest"),
            })
        body = {
            "schema": RECOVERY_RECEIPT_SCHEMA,
            **self._trusted_fields(),
            "status": "reconciled",
            "outcome": "unknown",
            "dispatch_key": request["dispatch_key"],
            "envelope_digest": request["envelope_digest"],
            "packet_digest": request["packet_digest"],
            "goal_id": request["goal_id"],
            "goal_revision": request["goal_revision"],
            "node_id": request["node_id"],
            "work_unit_id": request["work_unit_id"],
            "run_id": request["run_id"],
            "attempt": request["attempt"],
            "fence": request["fence"],
            "executor_id": self.executor_id,
            "provider": self.provider,
            "adapter": self.adapter,
            **_assignment_fields({**request, "worktree": str(worktree)}),
            "invocation_digest": reservation["invocation_digest"],
            "invocation_path": str(invocation_path) if invocation_path is not None else None,
            "process_identity": copy.deepcopy(observed_identity or {}),
            "reason": reason,
            "active_process_observed": process_observation.status == "alive",
            "process_observation": process_observation.status,
            "heartbeat_digest": (
                heartbeat.get("heartbeat_digest") if isinstance(heartbeat, Mapping) else None
            ),
            **observation_fields,
            "failure_evidence": failure_evidence,
            "workspace_before": copy.deepcopy(before),
            "workspace_after": after,
            "workspace_diff_digest": workspace_diff_digest,
            "workspace_baseline_available": isinstance(before, Mapping),
            "workspace_effects_reconciled": isinstance(before, Mapping),
            "recorded_at": _utc_now(),
        }
        return {**body, "recovery_receipt_digest": digest_json(body)}

    def _raise_retryable(self, failure: Mapping[str, Any]) -> None:
        raise SuccessorExecutorError(
            "executor_retry_required",
            details={"failure_receipt": copy.deepcopy(dict(failure))},
        )

    def _raise_unknown(self, recovery: Mapping[str, Any]) -> None:
        raise SuccessorExecutorError(
            "executor_outcome_unknown",
            details={"recovery_receipt": copy.deepcopy(dict(recovery))},
        )

    def prepare_unknown_reconciliation(self, request: Mapping[str, Any], *, recorded_at=None):
        """Read one existing invocation; there is intentionally no dispatch path.

        No lock, sidecar, workspace, command or process is created here. The
        operation caller seals this result and owns the later atomic journal.
        """
        value = self._validate_request(request)
        value.update(self._trusted_fields())
        task, heartbeat_path, _ = self._attempt_paths(value["dispatch_key"], attempt=value["attempt"], fence=value["fence"])
        invocation_path = task.with_suffix(".invocation.json")
        recovery_path = task.with_suffix(".recovery-receipt.json")
        for path in (invocation_path, heartbeat_path):
            if (any(item.is_symlink() for item in (path, *path.parents))
                or not path.is_file() or path.stat().st_size > 1048576):
                raise SuccessorExecutorError("recovery_no_launch_evidence_missing_or_unsafe")
        for path in (task, task.with_suffix(".executor-receipt.json"), task.with_suffix(".failure-receipt.json")):
            if path.exists() or path.is_symlink():
                raise SuccessorExecutorError("recovery_no_launch_terminal_conflict")
        root = pathlib.Path(value["worktree"])
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise SuccessorExecutorError("recovery_no_launch_workspace_symlink")
        invocation = self._validate_invocation(_read_object(invocation_path), value, worktree=root)
        heartbeat = _read_object(heartbeat_path)
        body = {k: v for k, v in heartbeat.items() if k != "heartbeat_digest"}
        if (heartbeat.get("schema") not in {"lh-successor-task-heartbeat/v1", "lh-successor-task-heartbeat/v2"}
            or heartbeat.get("heartbeat_digest") != digest_json(body) or heartbeat.get("status") != "alive"
            or heartbeat.get("invocation_digest") != invocation["invocation_digest"]
            or any(heartbeat.get(k) != value.get(k) for k in ("dispatch_key", "envelope_digest", "packet_digest", "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence", "worktree"))
            or heartbeat.get("executor_id") != self.executor_id):
            raise SuccessorExecutorError("recovery_no_launch_heartbeat_invalid")
        for name in ("execution_binding_digest", "policy_digest"):
            if name in value and (invocation.get(name) != value[name]
                or (heartbeat["schema"].endswith("/v2") and heartbeat.get(name) != value[name])):
                raise SuccessorExecutorError("recovery_no_launch_trusted_binding_invalid")
        try:
            first = datetime.fromisoformat(invocation["reserved_at"].replace("Z", "+00:00")).timestamp()
            observed = datetime.fromisoformat(heartbeat["observed_at"].replace("Z", "+00:00")).timestamp()
            if observed < first or heartbeat_path.stat().st_mtime_ns < invocation_path.stat().st_mtime_ns:
                raise ValueError("stale")
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise SuccessorExecutorError("recovery_no_launch_heartbeat_stale") from exc
        if not disposition_process_dead(heartbeat.get("process_identity"), identity_port=self.identity_port):
            raise SuccessorExecutorError("recovery_no_launch_process_not_dead")
        recovery = self._unknown_recovery_receipt(value, invocation, root,
            reason="legacy_reservation_without_failure_receipt", heartbeat=heartbeat, invocation_path=invocation_path)
        recovery.pop("recovery_receipt_digest", None)
        recovery["heartbeat_path"] = str(heartbeat_path)
        recovery.update({k: value[k] for k in ("execution_binding_digest", "policy_digest") if k in value})
        if recorded_at is not None:
            recovery["recorded_at"] = recorded_at
        recovery["recovery_receipt_digest"] = digest_json(recovery)
        return {"recovery_receipt": recovery, "recovery_path": str(recovery_path),
            "invocation_path": str(invocation_path), "heartbeat_path": str(heartbeat_path)}

    def dispatch(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        value = self._validate_request(request)
        value.update(self._trusted_fields())
        dispatch_key = str(value["dispatch_key"])
        attempt = int(value["attempt"])
        fence = int(value["fence"])
        if attempt > MAX_TOTAL_LAUNCHES:
            raise SuccessorExecutorError("executor_retry_budget_exhausted")
        task_path, heartbeat_path, lock_path = self._attempt_paths(
            dispatch_key,
            attempt=attempt,
            fence=fence,
        )
        invocation_path = task_path.with_suffix(".invocation.json")
        receipt_path = task_path.with_suffix(".executor-receipt.json")
        failure_path = task_path.with_suffix(".failure-receipt.json")
        recovery_path = task_path.with_suffix(".recovery-receipt.json")
        timeout_observation_path = self._timeout_observation_path(task_path)
        if timeout_observation_path.is_symlink():
            raise SuccessorExecutorError("executor_timeout_observation_path_conflict")
        worktree_text = _required_text("worktree", value.get("worktree"))
        worktree = pathlib.Path(worktree_text).expanduser().resolve(strict=True)
        if not worktree.is_dir():
            raise SuccessorExecutorError("worktree_invalid")
        packet_path = pathlib.Path(value["packet_path"])
        packet = self._packet(packet_path)
        lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock(lock_path):
            if task_path.exists():
                if timeout_observation_path.exists():
                    raise SuccessorExecutorError("executor_timeout_observation_conflict")
                if not receipt_path.exists():
                    raise SuccessorExecutorError("executor_receipt_missing")
                if not invocation_path.exists():
                    raise SuccessorExecutorError("executor_invocation_missing")
                reservation = self._validate_invocation(
                    _read_object(invocation_path),
                    value,
                    worktree=worktree,
                )
                stored = self._validate_stored(_read_object(receipt_path), value)
                if stored.get("invocation_digest") != reservation["invocation_digest"]:
                    raise SuccessorExecutorError("executor_invocation_binding_conflict")
                return {**stored, "reused": True, "invoked": False}
            if receipt_path.exists():
                raise SuccessorExecutorError("executor_receipt_orphaned")
            if failure_path.exists():
                if timeout_observation_path.exists():
                    raise SuccessorExecutorError("executor_timeout_observation_conflict")
                reservation = self._validate_invocation(
                    _read_object(invocation_path),
                    value,
                    worktree=worktree,
                )
                failure = self._validate_failure_receipt(
                    _read_object(failure_path),
                    value,
                    worktree=worktree,
                    reservation=reservation,
                )
                self._raise_retryable(failure)
            if recovery_path.exists():
                reservation = self._validate_invocation(
                    _read_object(invocation_path),
                    value,
                    worktree=worktree,
                )
                heartbeat = _read_object(heartbeat_path) if heartbeat_path.exists() else None
                timeout_observation = None
                if timeout_observation_path.exists():
                    if heartbeat is None:
                        raise SuccessorExecutorError("executor_timeout_observation_heartbeat_missing")
                    timeout_observation = self._validate_timeout_observation(
                        _read_object(timeout_observation_path), value,
                        reservation=reservation, heartbeat=heartbeat,
                        observation_path=timeout_observation_path,
                        invocation_path=invocation_path,
                        heartbeat_path=heartbeat_path,
                    )
                recovery = self._validate_recovery_receipt(
                    _read_object(recovery_path),
                    value,
                    reservation=reservation,
                    timeout_observation=timeout_observation,
                )
                self._raise_unknown(recovery)
            if invocation_path.exists():
                reservation = self._validate_invocation(
                    _read_object(invocation_path),
                    value,
                    worktree=worktree,
                )
                heartbeat = _read_object(heartbeat_path) if heartbeat_path.exists() else None
                timeout_observation = None
                if timeout_observation_path.exists():
                    if heartbeat is None:
                        raise SuccessorExecutorError("executor_timeout_observation_heartbeat_missing")
                    timeout_observation = self._validate_timeout_observation(
                        _read_object(timeout_observation_path), value,
                        reservation=reservation, heartbeat=heartbeat,
                        observation_path=timeout_observation_path,
                        invocation_path=invocation_path,
                        heartbeat_path=heartbeat_path,
                    )
                recovery = self._unknown_recovery_receipt(
                    value,
                    reservation,
                    worktree,
                    reason=(
                        "active_process_or_missing_termination_proof"
                        if observe_process_identity(reservation.get("process_identity"), identity_port=self.identity_port).status != "confirmed_dead"
                        else "legacy_reservation_without_failure_receipt"
                    ),
                    heartbeat=heartbeat,
                    invocation_path=invocation_path,
                    timeout_observation=timeout_observation,
                )
                _atomic_create(recovery_path, recovery)
                self._raise_unknown(recovery)

            if timeout_observation_path.exists():
                raise SuccessorExecutorError("executor_timeout_observation_orphaned")

            # Immutable accepted receipts above remain readable. Every new child,
            # including a legacy wrapper's child, needs an explicit backend grant.
            if self.execution_fence_port is None:
                raise SuccessorExecutorError("execution_fence_unavailable: backend_not_configured")
            if not self.execution_fence_port.supports_started_notification:
                raise SuccessorExecutorError("execution_fence_unavailable: started_notification_unsupported")

            if digest_json({key: item for key, item in packet.items() if key != "packet_digest"}) != value["packet_digest"]:
                raise SuccessorExecutorError("executor_packet_digest_mismatch")
            packet_body = packet.get("packet", packet)
            if not isinstance(packet_body, Mapping):
                raise SuccessorExecutorError("executor_packet_invalid")
            supplied_bases = [item for item in (
                packet_body.get("base_sha"), value.get("base_sha"), value.get("wave_base_sha")
            ) if item is not None]
            base_revision = _required_text("executor_base_revision", supplied_bases[0] if supplied_bases else None)
            if any(item != base_revision for item in supplied_bases):
                raise SuccessorExecutorError("executor_base_revision_mismatch")

            workspace_before = _workspace_snapshot(worktree)
            invocation_body = {
                "schema": self.invocation_schema,
                **self._trusted_fields(),
                "status": "reserved",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "adapter": self.adapter,
                "provider": self.provider,
                **_assignment_fields({**value, "worktree": str(worktree)}),
                "workspace_before": workspace_before,
                "reserved_at": _utc_now(),
            }
            invocation = {**invocation_body, "invocation_digest": digest_json(invocation_body)}
            _atomic_create(invocation_path, invocation)

            def record_failure(
                *,
                phase: str,
                process_identity: Mapping[str, Any] | None = None,
                exit_code: int | None = None,
                stdout: Any = "",
                stderr: Any = "",
                termination_proof: Mapping[str, Any] | None = None,
            ) -> None:
                failure = self._failure_receipt(
                    value,
                    invocation,
                    worktree,
                    phase=phase,
                    process_identity=process_identity,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    termination_proof=termination_proof,
                    invocation_path=invocation_path,
                )
                _atomic_create(failure_path, failure)
                self._raise_retryable(failure)

            try:
                argv = self._command(value, packet)
            except SuccessorExecutorError as exc:
                record_failure(
                    phase="codex_command_resolution",
                    stderr=str(exc),
                    termination_proof={
                        "process_started": False,
                        "process_terminated": True,
                    },
                )
            except (FileNotFoundError, ImportError, OSError, ValueError) as exc:
                record_failure(
                    phase="codex_command_resolution",
                    stderr=str(exc),
                    termination_proof={
                        "process_started": False,
                        "process_terminated": True,
                    },
                )

            process, process_identity, heartbeat = None, None, None
            fence_evidence = {}

            def on_started(child):
                nonlocal process, process_identity, heartbeat
                try:
                    if process is not None:
                        raise SuccessorExecutorError("executor_started_notification_repeated")
                    process = child
                    pid = getattr(process, "pid", None)
                    if isinstance(pid, bool) or not isinstance(pid, int) or pid < 1:
                        raise SuccessorExecutorError("executor_process_identity_invalid")
                    process_identity = _process_identity_for_pid(pid, identity_port=self.identity_port)
                    heartbeat_body = {
                        "schema": "lh-successor-task-heartbeat/v2" if self._trusted_fields() else HEARTBEAT_SCHEMA,
                        **self._trusted_fields(),
                        "status": "alive",
                        "dispatch_key": dispatch_key,
                        "envelope_digest": value["envelope_digest"],
                        "packet_digest": value["packet_digest"],
                        "goal_id": value["goal_id"],
                        "goal_revision": value["goal_revision"],
                        "node_id": value["node_id"],
                        "work_unit_id": value["work_unit_id"],
                        "run_id": value["run_id"],
                        "attempt": value["attempt"],
                        "fence": value["fence"],
                        "executor_id": self.executor_id,
                        "provider": self.provider,
                        "process_identity": copy.deepcopy(process_identity),
                        "invocation_digest": invocation["invocation_digest"],
                        "heartbeat_path": str(heartbeat_path),
                        **_assignment_fields({**value, "worktree": str(worktree)}),
                        "observed_at": _utc_now(),
                    }
                    if self._trusted_fields():
                        heartbeat_body.update(provider_invocations=None, cli_launches=1, manual_prompts=0)
                    heartbeat = {**heartbeat_body, "heartbeat_digest": digest_json(heartbeat_body)}
                    _atomic_create(heartbeat_path, heartbeat)
                    self.invocations += 1
                except BaseException:
                    if not self._trusted_fields():
                        _terminate_process(child)
                    raise

            def launch():
                if self.execution_binding is not None:
                    bound_request = {**value, "base_sha": base_revision}
                    completed, evidence, _ = self.execution_binding.command(bound_request,
                        phase="coding", argv=argv, worktree=str(worktree),
                        timeout_seconds=self.timeout_seconds, input_request=value,
                        writable=True, on_started=on_started)
                    fence_evidence.update(evidence)
                    return completed
                # Compatibility is a wrapper only, never an unfenced route.
                if __package__:
                    from . import execution_fence as fences
                else:
                    import execution_fence as fences
                descriptor = self.execution_fence_port.prepare(fences.build_attempt_binding(
                    goal={"goal_id": value["goal_id"], "goal_revision": value["goal_revision"]},
                    run_id=value["run_id"], attempt=value["attempt"], attempt_fence=value["fence"],
                    base_revision=base_revision,
                    clone_root=worktree, verifier_argv=argv, adapter_id=self.adapter,
                    adapter_version="v1", timeout_seconds=self.timeout_seconds))
                fence_evidence["execution_fence"] = self.execution_fence_port.receipt_projection(descriptor)
                environment = dict(os.environ)
                environment["PATH"] = str(pathlib.Path(argv[0]).parent) + os.pathsep + environment.get("PATH", "")
                for name in ("OPENAI_API_KEY", "OPENAI_API_BASE", "OPENAI_BASE_URL"):
                    environment.pop(name, None)
                projected_environment = self.execution_fence_port.project_environment(descriptor, environment)
                return self.execution_fence_port.launch(descriptor, argv,
                    timeout_seconds=self.timeout_seconds, on_started=on_started,
                    env_projection=projected_environment)

            try:
                try:
                    completed = launch()
                except subprocess.TimeoutExpired as exc:
                    terminated = _terminate_process(process) if process is not None else True
                    raise SuccessorExecutorError("executor_process_timeout", details={
                        "termination_proof": {"timeout": True, "termination_attempted": True,
                                              "process_terminated": terminated}}) from exc
                except (OSError, subprocess.SubprocessError) as exc:
                    if process is None:
                        record_failure(phase="process_launch", stderr=str(exc),
                            termination_proof={"process_started": False, "process_terminated": True})
                    raise SuccessorExecutorError("executor_process_readback_failed") from exc
                if heartbeat is None or process_identity is None:
                    raise SuccessorExecutorError("executor_started_notification_missing")
                code, stdout, stderr = completed.returncode, completed.stdout, completed.stderr
            except SuccessorExecutorError as exc:
                if str(exc) == "executor_process_timeout":
                    termination_proof = getattr(exc, "details", {}).get("termination_proof", {})
                    if isinstance(termination_proof, Mapping) and termination_proof.get("process_terminated") is True:
                        record_failure(
                            phase="process_timeout",
                            process_identity=process_identity,
                            stderr="unavailable",
                            termination_proof=termination_proof,
                        )
                    if isinstance(termination_proof, Mapping) and termination_proof.get("process_terminated") is not True:
                        observation = self._timeout_observation(
                            value,
                            invocation,
                            process_identity=process_identity,
                            heartbeat=heartbeat,
                            observation_path=timeout_observation_path,
                            invocation_path=invocation_path,
                            termination_proof=termination_proof,
                        )
                        _atomic_create(timeout_observation_path, observation)
                        self._validate_timeout_observation(
                            observation,
                            value,
                            reservation=invocation,
                            heartbeat=heartbeat,
                            observation_path=timeout_observation_path,
                            invocation_path=invocation_path,
                            heartbeat_path=heartbeat_path,
                        )
                raise
            if code != 0:
                record_failure(
                    phase="process_exit",
                    process_identity=process_identity,
                    exit_code=code,
                    stdout=stdout,
                    stderr=stderr,
                    termination_proof={
                        "observed_returncode": True,
                        "process_terminated": True,
                    },
                )
            output_digest = digest_json({"stdout": stdout, "stderr": stderr})

            task_body = {
                "schema": "lh-successor-task-receipt/v2" if self._trusted_fields() else TASK_RECEIPT_SCHEMA,
                **self._trusted_fields(),
                "status": "accepted",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "provider": self.provider,
                "adapter": self.adapter,
                "process_identity": copy.deepcopy(process_identity),
                **_assignment_fields({**value, "worktree": str(worktree)}),
                "packet_path": value["packet_path"],
                "heartbeat_path": str(heartbeat_path),
                "heartbeat_digest": heartbeat["heartbeat_digest"],
                "invocation_digest": invocation["invocation_digest"],
                "provider_invocations": 0,
                "manual_prompts": 0,
                "output_digest": output_digest,
                "accepted_at": _utc_now(),
                **fence_evidence,
            }
            if self._trusted_fields():
                task_body.update(provider_invocations=None, cli_launches=1)
            task = {**task_body, "receipt_digest": digest_json(task_body)}
            _atomic_create(task_path, task)
            receipt_body = {
                "schema": "lh-successor-executor-receipt/v2" if self._trusted_fields() else SCHEMA,
                **self._trusted_fields(),
                "status": "accepted",
                "dispatch_key": dispatch_key,
                "envelope_digest": value["envelope_digest"],
                "packet_digest": value["packet_digest"],
                "goal_id": value["goal_id"],
                "goal_revision": value["goal_revision"],
                "node_id": value["node_id"],
                "work_unit_id": value["work_unit_id"],
                "run_id": value["run_id"],
                "attempt": value["attempt"],
                "fence": value["fence"],
                "executor_id": self.executor_id,
                "provider": self.provider,
                "adapter": self.adapter,
                "process_identity": copy.deepcopy(process_identity),
                **_assignment_fields({**value, "worktree": str(worktree)}),
                "task_receipt_path": str(task_path),
                "task_receipt_digest": task["receipt_digest"],
                "heartbeat_path": str(heartbeat_path),
                "heartbeat_digest": heartbeat["heartbeat_digest"],
                "invocation_digest": invocation["invocation_digest"],
                "invocation_count": 1,
                "executor_invocations": 1,
                "provider_invocations": 0,
                "manual_prompts": 0,
                "output_digest": output_digest,
                "accepted_at": _utc_now(),
                **fence_evidence,
            }
            if self._trusted_fields():
                receipt_body.update(provider_invocations=None, cli_launches=1)
            receipt = {**receipt_body, "receipt_digest": digest_json(receipt_body)}
            self._validate_invocation(
                _read_object(invocation_path),
                value,
                worktree=worktree,
            )
            _atomic_create(receipt_path, receipt)
            self._validate_stored(receipt, value)
            return {**receipt, "reused": False, "invoked": True}


class BoundCommandExecutorAdapter(CodexSubscriptionExecutorAdapter):
    """Explicit registry command, using the existing durable executor journal."""

    invocation_schema = "lh-bounded-command-invocation/v1"

    def __init__(self, root, *, execution_binding, executor_id=None, **kwargs):
        selection = execution_binding.providers["coding"]
        self.provider = selection["provider_id"]
        self.adapter = selection["adapter_id"]
        if execution_binding.policy is not None:
            self.invocation_schema = "lh-trusted-command-invocation/v1"
        super().__init__(root, execution_binding=execution_binding,
                         executor_id=executor_id or self.provider, **kwargs)

    def _command(self, request, packet):
        del request, packet
        return list(self.execution_binding.providers["coding"]["command"])


def validate_executor_receipt(value: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    """Validate any provider adapter's durable receipt at the scheduler port."""

    if not isinstance(value, Mapping) or not isinstance(request, Mapping):
        raise SuccessorExecutorError("executor_receipt_invalid")
    # The concrete adapter's validator also checks the task receipt and
    # heartbeat files.  Reuse it here so an arbitrary provider-neutral port
    # cannot downgrade the boundary to a database-only acknowledgement.
    return DurableTaskExecutorAdapter._validate_stored(dict(value), request)


__all__ = [
    "FAILURE_RECEIPT_SCHEMA",
    "HEARTBEAT_SCHEMA",
    "MAX_TOTAL_LAUNCHES",
    "RECOVERY_RECEIPT_SCHEMA",
    "SCHEMA",
    "TIMEOUT_OBSERVATION_SCHEMA",
    "SuccessorExecutorError",
    "SuccessorExecutorPort",
    "TASK_RECEIPT_SCHEMA",
    "CodexSubscriptionExecutorAdapter",
    "DurableTaskExecutorAdapter",
    "digest_json",
    "validate_executor_receipt",
]

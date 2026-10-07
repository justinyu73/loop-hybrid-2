#!/usr/bin/env python3
"""Portable foreground lifecycle.

The foreground process is the lifecycle.  Whatever starts it never becomes a
second owner: the RunStore lock and this durable owner record remain the
single runtime boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Protocol, Sequence

try:
    from .platform_ports import FileLockHandle, FileLockSchedulerPort, SchedulerPort
except ImportError:
    from platform_ports import FileLockHandle, FileLockSchedulerPort, SchedulerPort


OWNER_LEASE_SCHEMA = "lh-runtime-owner-lease/v1"
ADAPTER_SCHEMA = "lh-runtime-lifecycle-adapter/v1"
OWNER_LEASE_FILENAME = "driver.owner.json"

class LifecycleUnavailable(RuntimeError):
    """The host cannot provide a verifiable foreground owner identity."""


class LifecycleOwnershipLost(RuntimeError):
    """The durable owner record no longer belongs to this process."""


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    start_token: str
    source: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "start_token": self.start_token,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | None) -> "ProcessIdentity | None":
        if not isinstance(value, Mapping):
            return None
        pid = value.get("pid")
        token = value.get("start_token")
        source = value.get("source")
        # Read legacy Linux records without mutating their digest-bound bytes.
        if "start_token" not in value and "source" not in value:
            boot_id, starttime = value.get("boot_id"), value.get("starttime")
            if isinstance(boot_id, str) and boot_id and isinstance(starttime, str) and starttime:
                token, source = f"linux:{boot_id}:{starttime}", "proc"
        if not isinstance(pid, int) or isinstance(pid, bool):
            return None
        if pid <= 0 or not isinstance(token, str) or not token:
            return None
        if not isinstance(source, str) or not source:
            return None
        return cls(pid=pid, start_token=token, source=source)

    def matches(self, other: "ProcessIdentity | None") -> bool:
        return (
            other is not None
            and self.pid == other.pid
            and self.start_token == other.start_token
            and self.source == other.source
        )


class ProcessIdentityPort(Protocol):
    def current(self) -> ProcessIdentity | None:
        ...

    def observe(self, pid: int) -> ProcessIdentity | None:
        ...

    def inspect(self, identity: ProcessIdentity) -> "ProcessObservation":
        ...


@dataclass(frozen=True)
class ProcessObservation:
    status: Literal["alive", "confirmed_dead", "unknown"]
    identity: ProcessIdentity | None = None
    reason: str = ""


def _identity_value(value: Any) -> ProcessIdentity | None:
    # Both the package import and the historic standalone import are public.
    if not isinstance(value, Mapping) and callable(getattr(value, "as_dict", None)):
        value = value.as_dict()
    return ProcessIdentity.from_dict(value)


def observe_process_identity(identity: Any, *, identity_port: ProcessIdentityPort | None = None) -> ProcessObservation:
    """A missing observation is unknown, never evidence of process death."""
    expected = _identity_value(identity)
    if expected is None:
        return ProcessObservation("unknown", reason="process_identity_incomplete")
    port = identity_port if identity_port is not None else NativeProcessIdentityPort()
    try:
        inspect = getattr(port, "inspect", None)
        if callable(inspect):
            observation = inspect(expected)
            status = getattr(observation, "status", None)
            observed = _identity_value(getattr(observation, "identity", None))
            if status == "alive" and not expected.matches(observed):
                return ProcessObservation("unknown", reason="process_observation_identity_mismatch")
            if status in {"alive", "confirmed_dead", "unknown"}:
                return ProcessObservation(status, observed, str(getattr(observation, "reason", "")))
            return ProcessObservation("unknown", reason="process_observation_invalid")
        observed = _identity_value(port.observe(expected.pid))
    except (OSError, ValueError, RuntimeError) as exc:
        return ProcessObservation("unknown", reason=f"process_observation_unavailable:{type(exc).__name__}")
    if observed is None:
        return ProcessObservation("unknown", reason="process_observation_unavailable")
    if observed.source != expected.source or observed.pid != expected.pid:
        return ProcessObservation("unknown", reason="process_identity_backend_mismatch")
    return ProcessObservation("alive" if expected.matches(observed) else "confirmed_dead", observed,
                              "identity_match" if expected.matches(observed) else "pid_reused")


class NativeProcessIdentityPort:
    """Read a host-native process birth token without trusting PID alone."""

    def current(self) -> ProcessIdentity | None:
        return self.observe(os.getpid())

    def observe(self, pid: int) -> ProcessIdentity | None:
        if pid <= 0:
            return None
        if sys.platform.startswith("linux"):
            return self._linux(pid)
        if os.name == "nt":
            return self._windows(pid)
        return self._posix(pid)

    def inspect(self, identity: ProcessIdentity) -> ProcessObservation:
        if sys.platform.startswith("linux"):
            return self._inspect_linux(identity)
        if os.name == "nt":
            if identity.source != "process-times" or not identity.start_token.startswith("windows:"):
                return ProcessObservation("unknown", reason="process_identity_backend_mismatch")
            observation = self._windows_observation(identity.pid)
            if observation.identity is not None and not identity.matches(observation.identity):
                return ProcessObservation("confirmed_dead", observation.identity, "pid_reused")
            return observation
        observed = self.observe(identity.pid)
        if observed is not None:
            if observed.source != identity.source:
                return ProcessObservation("unknown", reason="process_identity_backend_mismatch")
            return ProcessObservation("alive" if identity.matches(observed) else "confirmed_dead",
                                      observed, "identity_match" if identity.matches(observed) else "pid_reused")
        if os.name == "posix":
            try:
                # Signal zero is an observation only on POSIX, never Windows.
                os.kill(identity.pid, 0)
            except ProcessLookupError:
                return ProcessObservation("confirmed_dead", reason="process_absent")
            except OSError:
                pass
        return ProcessObservation("unknown", reason="process_observation_unavailable")

    @staticmethod
    def _inspect_linux(identity: ProcessIdentity) -> ProcessObservation:
        if identity.source != "proc" or not identity.start_token.startswith("linux:"):
            return ProcessObservation("unknown", reason="process_identity_backend_mismatch")
        try:
            _, expected_boot, _ = identity.start_token.split(":", 2)
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
            if not boot_id:
                raise ValueError("empty boot identity")
            if expected_boot != boot_id:
                return ProcessObservation("confirmed_dead", reason="boot_changed")
            try:
                raw = Path(f"/proc/{identity.pid}/stat").read_text(encoding="utf-8")
            except FileNotFoundError:
                # A restricted proc view can hide a live PID while exposing
                # boot metadata. Only a kernel existence probe proves absence.
                try:
                    os.kill(identity.pid, 0)
                except ProcessLookupError:
                    return ProcessObservation("confirmed_dead", reason="process_absent")
                except OSError:
                    pass
                return ProcessObservation("unknown", reason="process_observation_unavailable")
            values = raw.rpartition(")")[2].split()
            start_ticks = values[19]
            if not start_ticks.isdecimal():
                raise ValueError("invalid process birth token")
            observed = ProcessIdentity(identity.pid, f"linux:{boot_id}:{start_ticks}", "proc")
        except (OSError, ValueError, IndexError, UnicodeError):
            return ProcessObservation("unknown", reason="process_observation_unavailable")
        return ProcessObservation("alive" if identity.matches(observed) else "confirmed_dead", observed,
                                  "identity_match" if identity.matches(observed) else "pid_reused")

    @staticmethod
    def _linux(pid: int) -> ProcessIdentity | None:
        try:
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            _, _, fields = raw.rpartition(")")
            values = fields.split()
            # The first value after the comm field is field 3; starttime is
            # field 22, hence index 19 in this tail.
            start_ticks = values[19]
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
        except (IndexError, OSError, UnicodeDecodeError):
            return None
        if not boot_id or not start_ticks:
            return None
        return ProcessIdentity(pid=pid, start_token=f"linux:{boot_id}:{start_ticks}", source="proc")

    @staticmethod
    def _posix(pid: int) -> ProcessIdentity | None:
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "lstart="],
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        value = result.stdout.strip()
        if result.returncode != 0 or not value:
            return None
        token = hashlib.sha256(value.encode("utf-8")).hexdigest()
        return ProcessIdentity(pid=pid, start_token=f"posix:{token}", source="ps")

    @staticmethod
    def _windows(pid: int) -> ProcessIdentity | None:
        observation = NativeProcessIdentityPort._windows_observation(pid)
        return observation.identity if observation.status == "alive" else None

    @staticmethod
    def _windows_observation(pid: int) -> ProcessObservation:
        unknown = ProcessObservation("unknown", reason="process_observation_unavailable")
        if isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0xFFFFFFFF:
            return unknown
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
                ctypes.POINTER(wintypes.FILETIME)] * 4
            kernel32.GetProcessTimes.restype = wintypes.BOOL
            kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            kernel32.WaitForSingleObject.restype = wintypes.DWORD
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL
            # Fixed valid access mask and positive DWORD PID: invalid parameter
            # is a missing process; access denied and all other failures are unknown.
            handle = kernel32.OpenProcess(0x00101000, False, pid)
            if not handle:
                if ctypes.get_last_error() == 87:
                    return ProcessObservation("confirmed_dead", reason="process_absent")
                return unknown
            creation = wintypes.FILETIME()
            exit_time = wintypes.FILETIME()
            kernel_time = wintypes.FILETIME()
            user_time = wintypes.FILETIME()
            try:
                ok = kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_time),
                    ctypes.byref(kernel_time),
                    ctypes.byref(user_time),
                )
                if not ok:
                    return unknown
                status = kernel32.WaitForSingleObject(handle, 0)
                if status not in (0, 258):
                    return unknown
                value = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
                observed = ProcessIdentity(pid=pid, start_token=f"windows:{value}", source="process-times")
                return ProcessObservation("alive" if status == 258 else "confirmed_dead", observed,
                                          "identity_match" if status == 258 else "process_exited")
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError, TypeError):
            return unknown


def _utc_now(clock: Callable[[], float]) -> str:
    return datetime.fromtimestamp(clock(), tz=timezone.utc).isoformat()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_owner_record(path: str | Path) -> dict[str, Any] | None:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) and value.get("schema") == OWNER_LEASE_SCHEMA else None


def _read_owner_record(path: Path) -> tuple[dict[str, Any] | None, str]:
    if not path.exists():
        return None, "no_previous_record"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, "previous_record_unreadable"
    if not isinstance(value, dict) or value.get("schema") != OWNER_LEASE_SCHEMA:
        return None, "previous_record_invalid"
    return value, ""


def _same_owner(record: Mapping[str, Any] | None, *, owner_id: str, lease_id: str, identity: ProcessIdentity) -> bool:
    if not isinstance(record, Mapping):
        return False
    record_identity = ProcessIdentity.from_dict(record.get("process_identity"))
    return (
        record.get("owner_id") == owner_id
        and record.get("lease_id") == lease_id
        and identity.matches(record_identity)
    )


def _classify_previous(
    record: Mapping[str, Any] | None,
    status: str,
    identity_port: ProcessIdentityPort,
) -> str:
    if record is None:
        return status
    state = record.get("state")
    if state != "running":
        return f"previous_{state or 'unknown'}"
    previous = ProcessIdentity.from_dict(record.get("process_identity"))
    if previous is None:
        return "previous_identity_missing"
    observed = identity_port.observe(previous.pid)
    if observed is None:
        return "previous_process_exited"
    if previous.matches(observed):
        return "previous_owner_identity_alive"
    return "pid_reused"


class _ShutdownController:
    def __init__(self, flag: str | Path | None) -> None:
        self.flag = Path(flag) if flag is not None else None
        self.event = threading.Event()
        self._previous: dict[int, Any] = {}
        self._installed = False

    def install(self) -> None:
        if self.flag is not None and self.flag.exists():
            self.event.set()
        if threading.current_thread() is not threading.main_thread():
            return
        # SIGBREAK exists only on Windows (CTRL_BREAK_EVENT), its graceful-stop signal.
        for signal_number in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None),
                              getattr(signal, "SIGBREAK", None)):
            if signal_number is None:
                continue
            self._previous[signal_number] = signal.getsignal(signal_number)
            signal.signal(signal_number, self._handle)
        self._installed = True

    def _handle(self, _signum: int, _frame: Any) -> None:
        self.event.set()

    def requested(self) -> bool:
        if self.flag is not None and self.flag.exists():
            self.event.set()
        return self.event.is_set()

    def restore(self) -> None:
        if not self._installed:
            return
        for signal_number, previous in self._previous.items():
            signal.signal(signal_number, previous)
        self._installed = False


def build_foreground_descriptor(command: Sequence[str]) -> dict[str, Any]:
    argv = tuple(str(item) for item in command)
    if not argv or any(not item for item in argv):
        raise ValueError("foreground command must be a non-empty argv")
    return {
        "schema": ADAPTER_SCHEMA,
        "platform": "any",
        "adapter": "foreground",
        "optional": False,
        "foreground": {"argv": list(argv), "shell": False, "bounded_session": True},
        "owner": {"lease": OWNER_LEASE_SCHEMA, "pid_identity": "birth-token"},
        "shutdown": {"mode": "graceful-signal-or-flag", "preserve_inflight_attempt": True},
    }


class LifecycleLease:
    def __init__(
        self,
        *,
        root: Path,
        owner_id: str,
        handle: FileLockHandle,
        scheduler: SchedulerPort,
        identity: ProcessIdentity,
        lease_id: str,
        recovery: str,
        shutdown: _ShutdownController,
        clock: Callable[[], float],
    ) -> None:
        self.root = root
        self.owner_id = owner_id
        self.handle = handle
        self.scheduler = scheduler
        self.identity = identity
        self.lease_id = lease_id
        self.recovery = recovery
        self.shutdown = shutdown
        self.clock = clock
        self.owner_path = root / OWNER_LEASE_FILENAME
        self.closed = False
        self.final_record: dict[str, Any] | None = None

    def shutdown_requested(self) -> bool:
        return self.shutdown.requested()

    def heartbeat(self, *, phase: str, cycles: int) -> bool:
        if self.closed:
            return False
        record = read_owner_record(self.owner_path)
        if not _same_owner(record, owner_id=self.owner_id, lease_id=self.lease_id, identity=self.identity):
            return False
        updated = dict(record)
        updated.update({"state": "running", "phase": phase, "cycles": cycles, "updated_at": _utc_now(self.clock)})
        _atomic_write_json(self.owner_path, updated)
        return True

    def close(self, stop_reason: str) -> dict[str, Any] | None:
        if self.closed:
            return self.final_record
        try:
            record = read_owner_record(self.owner_path)
            if _same_owner(record, owner_id=self.owner_id, lease_id=self.lease_id, identity=self.identity):
                updated = dict(record)
                updated.update({
                    "state": "shutdown_complete" if stop_reason == "shutdown_requested" else "stopped",
                    "stop_reason": stop_reason,
                    "finished_at": _utc_now(self.clock),
                    "updated_at": _utc_now(self.clock),
                })
                _atomic_write_json(self.owner_path, updated)
                self.final_record = updated
        finally:
            self.shutdown.restore()
            self.scheduler.release(self.handle)
            self.closed = True
        return self.final_record


class LifecyclePort(Protocol):
    def owner_path(self, root: str | Path) -> Path:
        ...

    def acquire(
        self,
        root: str | Path,
        owner_id: str,
        *,
        scheduler_port: SchedulerPort | None = None,
    ) -> LifecycleLease | None:
        ...


class ForegroundLifecycle:
    """Common daemon lifecycle with platform-neutral ownership evidence."""

    def __init__(
        self,
        *,
        identity_port: ProcessIdentityPort | None = None,
        shutdown_flag: str | Path | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.identity_port = identity_port or NativeProcessIdentityPort()
        self.shutdown_flag = shutdown_flag
        self.clock = clock

    def owner_path(self, root: str | Path) -> Path:
        return Path(root) / OWNER_LEASE_FILENAME

    def acquire(
        self,
        root: str | Path,
        owner_id: str,
        *,
        scheduler_port: SchedulerPort | None = None,
    ) -> LifecycleLease | None:
        if not owner_id or any(character.isspace() for character in owner_id):
            raise ValueError("owner_id must be a non-empty stable identifier")
        identity = self.identity_port.current()
        if identity is None:
            raise LifecycleUnavailable("process birth identity is unavailable")
        run_root = Path(root)
        scheduler = scheduler_port or FileLockSchedulerPort()
        owner_path = self.owner_path(run_root)
        previous, read_status = _read_owner_record(owner_path)
        handle = scheduler.acquire(run_root)
        if handle is None:
            return None
        lease_id = uuid.uuid4().hex
        recovery = _classify_previous(previous, read_status, self.identity_port)
        shutdown = _ShutdownController(self.shutdown_flag)
        record = {
            "schema": OWNER_LEASE_SCHEMA,
            "owner_id": owner_id,
            "process_identity": identity.as_dict(),
            "lease_id": lease_id,
            "state": "running",
            "phase": "acquired",
            "cycles": 0,
            "recovery": recovery,
            "acquired_at": _utc_now(self.clock),
            "updated_at": _utc_now(self.clock),
        }
        try:
            _atomic_write_json(owner_path, record)
            shutdown.install()
        except BaseException:
            scheduler.release(handle)
            raise
        return LifecycleLease(
            root=run_root,
            owner_id=owner_id,
            handle=handle,
            scheduler=scheduler,
            identity=identity,
            lease_id=lease_id,
            recovery=recovery,
            shutdown=shutdown,
            clock=self.clock,
        )


__all__ = [
    "ADAPTER_SCHEMA",
    "ForegroundLifecycle",
    "LifecycleLease",
    "LifecycleOwnershipLost",
    "LifecyclePort",
    "LifecycleUnavailable",
    "NativeProcessIdentityPort",
    "OWNER_LEASE_FILENAME",
    "OWNER_LEASE_SCHEMA",
    "ProcessIdentity",
    "ProcessIdentityPort",
    "build_foreground_descriptor",
    "read_owner_record",
]

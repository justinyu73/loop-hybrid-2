"""Portable seams between LH's core and the host platform.

The port layer keeps Goal/Run/Attempt, receipt, retry, verifier, and
single-holder mechanics independent of the machine that hosts them.  Host
features are opt-in capability adapters; the portable core never guesses a
host service, display, path encoding, or executable location.
"""

from __future__ import annotations

import errno
import os
import shutil
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence


SUPPORTED_PLATFORM_FAMILIES = frozenset({"darwin", "linux", "windows"})


def normalize_platform_name(platform_name: str | None = None) -> str:
    """Map runtime platform spellings to the small portable platform set."""
    value = str(sys.platform if platform_name is None else platform_name).strip().lower()
    if value.startswith("win") or value in {"cygwin", "msys"}:
        return "windows"
    if value.startswith("linux"):
        return "linux"
    if value.startswith("darwin") or value in {"mac", "macos", "macosx"}:
        return "darwin"
    return value or "unsupported"


def is_supported_platform(platform_name: str | None = None) -> bool:
    return normalize_platform_name(platform_name) in SUPPORTED_PLATFORM_FAMILIES


class PlatformPortUnavailable(RuntimeError):
    """A platform port cannot provide the requested capability."""

    code = "platform_port_unavailable"

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


class UnsupportedPlatformError(PlatformPortUnavailable):
    """The portable local process path is unavailable on this platform."""

    code = "unsupported_platform"

    def __init__(self, platform_name: str | None = None):
        self.platform_name = normalize_platform_name(platform_name)
        super().__init__(self.code)


class CapabilityUnavailable(PlatformPortUnavailable):
    """An optional capability was requested without an eligible adapter."""

    code = "optional_capability_unavailable"

    def __init__(self, capability: str, platform_name: str | None = None):
        self.capability = capability
        self.platform_name = normalize_platform_name(platform_name)
        super().__init__(f"{capability}/{self.platform_name}")


@dataclass(frozen=True, init=False)
class OptionalCapabilityAdapter:
    """Explicit, lazy provider for one optional host capability.

    The factory is called only after a caller names the capability and an
    eligible platform.  There is no module-level registry or auto-discovery.
    ``platforms`` is accepted as a keyword alias for ``platform_names`` to
    keep small adapters readable at call sites.
    """

    capability: str
    platform_names: tuple[str, ...]
    factory: Callable[[], Any]
    adapter_id: str

    def __init__(
        self,
        capability: str,
        platform_names: Sequence[str] | None = None,
        factory: Callable[[], Any] | None = None,
        adapter_id: str = "optional-capability",
        *,
        platforms: Sequence[str] | None = None,
    ) -> None:
        selected_platforms = platform_names if platform_names is not None else platforms
        if not isinstance(capability, str) or not capability.strip():
            raise ValueError("capability must be a non-empty string")
        if selected_platforms is None or not selected_platforms:
            raise ValueError("platform_names must not be empty")
        if any(not isinstance(item, str) or not item.strip() for item in selected_platforms):
            raise ValueError("platform_names must contain non-empty strings")
        if not callable(factory):
            raise TypeError("factory must be callable")
        if not isinstance(adapter_id, str) or not adapter_id.strip():
            raise ValueError("adapter_id must be a non-empty string")
        normalized = tuple(dict.fromkeys(normalize_platform_name(item) for item in selected_platforms))
        object.__setattr__(self, "capability", capability.strip())
        object.__setattr__(self, "platform_names", normalized)
        object.__setattr__(self, "factory", factory)
        object.__setattr__(self, "adapter_id", adapter_id.strip())

    @property
    def capability_id(self) -> str:
        return self.capability

    @property
    def platforms(self) -> tuple[str, ...]:
        return self.platform_names

    def supports(self, platform_name: str | None = None) -> bool:
        return normalize_platform_name(platform_name) in self.platform_names

    def build(self) -> Any:
        return self.factory()


class OptionalCapabilityRegistry:
    """Resolve only adapters explicitly supplied by the caller."""

    def __init__(self, adapters: Sequence[OptionalCapabilityAdapter] = ()) -> None:
        self._adapters: dict[tuple[str, str], OptionalCapabilityAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: OptionalCapabilityAdapter) -> None:
        if not isinstance(adapter, OptionalCapabilityAdapter):
            raise TypeError("adapter must be an OptionalCapabilityAdapter")
        for platform_name in adapter.platform_names:
            key = (adapter.capability, platform_name)
            if key in self._adapters:
                raise ValueError(f"duplicate capability adapter: {adapter.capability}/{platform_name}")
            self._adapters[key] = adapter

    def available(self, capability: str, *, platform_name: str | None = None) -> bool:
        return (capability, normalize_platform_name(platform_name)) in self._adapters

    def adapter(
        self,
        capability: str,
        *,
        platform_name: str | None = None,
    ) -> OptionalCapabilityAdapter:
        key = (capability, normalize_platform_name(platform_name))
        try:
            return self._adapters[key]
        except KeyError as exc:
            raise CapabilityUnavailable(capability, platform_name) from exc

    def resolve(self, capability: str, *, platform_name: str | None = None) -> Any:
        return self.adapter(capability, platform_name=platform_name).build()


def resolve_optional_capability(
    capability: str,
    *,
    platform_name: str | None = None,
    adapters: Sequence[OptionalCapabilityAdapter] = (),
) -> Any:
    """Resolve a named optional capability from an explicit adapter list."""
    return OptionalCapabilityRegistry(adapters).resolve(capability, platform_name=platform_name)


def _output_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


@dataclass(frozen=True)
class ProcessResult:
    """The platform-neutral subset of ``CompletedProcess`` LH persists."""

    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class ManagedProcessResult(subprocess.CompletedProcess):
    """An observed child result; group quiescence is separate from its exit."""

    def __init__(self, args, returncode, stdout, stderr, process_lifecycle):
        super().__init__(args, returncode, stdout, stderr)
        self.process_lifecycle = process_lifecycle


class ManagedProcessTimeout(subprocess.TimeoutExpired):
    def __init__(self, argv, process_lifecycle):
        super().__init__(argv, process_lifecycle["elapsed_seconds"])
        self.process_lifecycle = process_lifecycle

    def __str__(self):
        return "managed_process_timeout"


class ManagedProcessUnknown(RuntimeError):
    def __init__(self, reason, process_lifecycle):
        self.reason, self.process_lifecycle = reason, process_lifecycle
        super().__init__(reason)


def run_managed_process(argv, *, cwd, input_text, env, deadline_at,
                        max_output_bytes=1048576, on_started=None, identity_port=None):
    """Explicitly request the optional trusted owned-process-group adapter."""
    if __package__:
        from .execution_fence_trusted import run_trusted_managed_process
    else:
        from execution_fence_trusted import run_trusted_managed_process
    return run_trusted_managed_process(argv, cwd=cwd, input_text=input_text, env=env,
        deadline_at=deadline_at, max_output_bytes=max_output_bytes,
        on_started=on_started, identity_port=identity_port)


class ProcessTimeout(TimeoutError):
    """An argv process exceeded the deadline supplied by the caller."""

    def __init__(
        self,
        argv: Sequence[str],
        *,
        stdout: str | bytes | None = None,
        stderr: str | bytes | None = None,
    ) -> None:
        self.argv = tuple(str(item) for item in argv)
        self.stdout = _output_text(stdout)
        self.stderr = _output_text(stderr)
        super().__init__(f"process timed out: {self.argv[0] if self.argv else '<empty argv>'}")


class ProcessPort(Protocol):
    """Execute an argv vector without shell interpolation."""

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        ...


class ShellPort(Protocol):
    """Separate seam for the rare caller that intentionally needs a shell string."""

    def run_shell(
        self,
        command: str,
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        ...


class UnsupportedPlatformPort:
    """Port whose calls are unavailable on this platform."""

    def __init__(self, platform_name: str | None = None):
        self.platform_name = normalize_platform_name(platform_name)
        self.reason = "unsupported_platform"

    def _raise(self) -> None:
        raise UnsupportedPlatformError(self.platform_name)

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del argv, cwd, timeout, env
        self._raise()

    def run_shell(
        self,
        command: str,
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del command, cwd, timeout, env
        self._raise()


class LocalProcessPort:
    """Native argv adapter; shell execution is deliberately not implicit."""

    def __init__(self, *, platform_name: str | None = None):
        self.platform_name = normalize_platform_name(platform_name)

    def launch_unavailable(self, argv: Sequence[str]) -> str | None:
        """Why ``argv`` cannot be launched, checked without running it.

        ``None`` means it can be launched, or that only the launch can tell: a
        program named by a relative path resolves inside a working directory the
        caller may not have created yet.  Executability follows ``shutil.which``
        (the execute bit on POSIX, ``PATHEXT`` on Windows).
        """
        if isinstance(argv, (str, bytes)) or not argv or not str(argv[0]):
            return "argv_empty"
        head = str(argv[0])
        if Path(head).is_absolute():
            if not Path(head).exists():
                return "not_found"
            return None if shutil.which(head) else "not_executable"
        if os.sep in head or (os.altsep and os.altsep in head):
            return None
        return None if shutil.which(head) else "not_on_path"

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        if not is_supported_platform(self.platform_name):
            raise UnsupportedPlatformError(self.platform_name)
        if isinstance(argv, (str, bytes)):
            raise TypeError("ProcessPort.run requires argv; use ShellPort for a command string")
        normalized = tuple(str(item) for item in argv)
        if not normalized:
            raise ValueError("argv must not be empty")
        try:
            completed = subprocess.run(
                list(normalized),
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=dict(env) if env is not None else None,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProcessTimeout(normalized, stdout=exc.stdout, stderr=exc.stderr) from exc
        return ProcessResult(
            args=normalized,
            returncode=completed.returncode,
            stdout=_output_text(completed.stdout),
            stderr=_output_text(completed.stderr),
        )


class LocalShellPort:
    """Explicit shell-string adapter kept separate from ``LocalProcessPort``."""

    def __init__(self, *, platform_name: str | None = None):
        self.platform_name = normalize_platform_name(platform_name)

    def run_shell(
        self,
        command: str,
        *,
        cwd: str | Path | None = None,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        if not is_supported_platform(self.platform_name):
            raise UnsupportedPlatformError(self.platform_name)
        if not isinstance(command, str) or not command:
            raise ValueError("shell command must be a non-empty string")
        try:
            completed = subprocess.run(
                command,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=dict(env) if env is not None else None,
                shell=True,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProcessTimeout((command,), stdout=exc.stdout, stderr=exc.stderr) from exc
        return ProcessResult(
            args=(command,),
            returncode=completed.returncode,
            stdout=_output_text(completed.stdout),
            stderr=_output_text(completed.stderr),
        )


def build_process_port(
    platform_name: str | None = None,
    *,
    adapter: ProcessPort | None = None,
) -> ProcessPort:
    """Build the local process seam or an explicitly supplied adapter."""
    if adapter is not None:
        if not callable(getattr(adapter, "run", None)):
            raise TypeError("process adapter must provide run")
        return adapter
    if not is_supported_platform(platform_name):
        return UnsupportedPlatformPort(platform_name)
    return LocalProcessPort(platform_name=platform_name)


def build_shell_port(
    platform_name: str | None = None,
    *,
    adapter: ShellPort | None = None,
) -> ShellPort:
    """Build the explicit shell seam or an explicitly supplied adapter."""
    if adapter is not None:
        if not callable(getattr(adapter, "run_shell", None)):
            raise TypeError("shell adapter must provide run_shell")
        return adapter
    if not is_supported_platform(platform_name):
        return UnsupportedPlatformPort(platform_name)
    return LocalShellPort(platform_name=platform_name)


class DeadlineExpired(TimeoutError):
    """A monotonic attempt deadline has elapsed."""


class Deadline(Protocol):
    def remaining(self) -> float:
        ...

    def check(self) -> None:
        ...

    def timeout(self) -> float:
        ...


class DeadlinePort(Protocol):
    """Create monotonic deadlines without process-global signal handlers."""

    def start(self, seconds: float) -> Deadline:
        ...


class MonotonicDeadline:
    def __init__(self, seconds: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        if seconds <= 0:
            raise ValueError("deadline seconds must be positive")
        self._clock = clock
        self._expires_at = clock() + float(seconds)

    def remaining(self) -> float:
        return max(0.0, self._expires_at - self._clock())

    def check(self) -> None:
        if self.remaining() <= 0:
            raise DeadlineExpired("attempt deadline exceeded")

    def timeout(self) -> float:
        self.check()
        # subprocess accepts a positive float; check() preserves the hard
        # boundary while the small floor avoids passing a rounded zero.
        return max(0.001, self.remaining())


class MonotonicDeadlinePort:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock

    def start(self, seconds: float) -> Deadline:
        return MonotonicDeadline(seconds, clock=self._clock)


@dataclass(frozen=True)
class PlatformPaths:
    """Per-install paths with no operator-specific host default."""

    instance_root: Path
    state_root: Path
    run_root: Path
    workspace_root: Path
    cache_root: Path
    logs_root: Path

    @classmethod
    def from_instance_root(cls, root: str | Path) -> "PlatformPaths":
        base = Path(root).expanduser()
        return cls(
            instance_root=base,
            state_root=base / "state",
            run_root=base / "run",
            workspace_root=base / "workspaces",
            cache_root=base / "cache",
            logs_root=base / "logs",
        )

    @classmethod
    def from_environment(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        home: str | Path | None = None,
        platform_name: str | None = None,
    ) -> "PlatformPaths":
        values = dict(os.environ if env is None else env)
        explicit_root = values.get("LH_INSTANCE_ROOT", "").strip()
        if explicit_root:
            defaults = cls.from_instance_root(explicit_root)
        else:
            system = normalize_platform_name(platform_name)
            if not is_supported_platform(system):
                raise UnsupportedPlatformError(system)
            home_path = Path(home or values.get("HOME") or Path.home())
            if system == "windows":
                base = Path(values.get("LOCALAPPDATA") or home_path / "AppData" / "Local") / "LoopHybrid"
            elif system == "darwin":
                base = home_path / "Library" / "Application Support" / "LoopHybrid"
            else:
                base = Path(values.get("XDG_STATE_HOME") or home_path / ".local" / "state") / "loop-hybrid"
            defaults = cls.from_instance_root(base)

        def override(name: str, fallback: Path) -> Path:
            value = values.get(name, "").strip()
            return Path(value).expanduser() if value else fallback

        return cls(
            instance_root=defaults.instance_root,
            state_root=override("LH_STATE_ROOT", defaults.state_root),
            run_root=override("LH_RUN_ROOT", defaults.run_root),
            workspace_root=override("LH_WORKSPACE_ROOT", defaults.workspace_root),
            cache_root=override("LH_CACHE_ROOT", defaults.cache_root),
            logs_root=override("LH_LOG_ROOT", defaults.logs_root),
        )


# Task-owned scratch variables were renamed; an old name is refused, never ignored,
# so a protection somebody configured cannot fail silently.
RENAMED_ENVIRONMENT = {
    "LH_HOST_STATE_ROOT": "LH_TASK_STATE_ROOT",
    "LH_HOST_TMP_ROOT": "LH_TASK_TMP_ROOT",
}


class RenamedEnvironmentError(ValueError):
    """An environment variable was set under a name the engine no longer reads."""


def refuse_renamed_environment(env: Mapping[str, str] | None = None) -> None:
    values = os.environ if env is None else env
    for old, new in RENAMED_ENVIRONMENT.items():
        if old in values:
            raise RenamedEnvironmentError(f"renamed_environment:{old}->{new}")


def production_roots(env: Mapping[str, str] | None = None) -> tuple[Path, ...]:
    """Every root the engine itself would use for live state, from the platform path rules."""
    refuse_renamed_environment(env)
    paths = PlatformPaths.from_environment(env)
    roots = (paths.instance_root, paths.state_root, paths.run_root, paths.workspace_root,
             paths.cache_root, paths.logs_root)
    return tuple(dict.fromkeys(Path(root).expanduser().resolve() for root in roots))


def inside_production(path: str | Path, env: Mapping[str, str] | None = None) -> bool:
    """True when ``path`` is a production root or lies inside one."""
    candidate = Path(path).expanduser().resolve()
    return any(candidate == root or candidate.is_relative_to(root) for root in production_roots(env))


@dataclass(frozen=True)
class FileLockHandle:
    fd: int
    path: Path
    backend: str


class FileLockPort(Protocol):
    def acquire(self, path: str | Path) -> FileLockHandle | None:
        ...

    def release(self, handle: FileLockHandle) -> None:
        ...



def _private_path(path: str | Path) -> Path:
    """Inspect lexical components; resolving first would hide a reparse point."""
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        raise PlatformPortUnavailable("private_file_permissions_unavailable:path_not_absolute")
    try:
        for component in (candidate, *candidate.parents):
            metadata = component.lstat()
            if (stat.S_ISLNK(metadata.st_mode)
                    or getattr(metadata, "st_file_attributes", 0) & 0x400):
                raise PlatformPortUnavailable("private_file_permissions_unavailable:reparse_point")
    except OSError as exc:
        raise PlatformPortUnavailable("private_file_permissions_unavailable:path_unreadable") from exc
    return candidate


if __package__:
    from .platform_files_windows import WindowsFileAdapter as _NativeWindowsFileAdapter
else:
    from platform_files_windows import WindowsFileAdapter as _NativeWindowsFileAdapter


class _WindowsFileAdapter(_NativeWindowsFileAdapter):
    """Keep the existing file-port seam and shared error/path contracts."""

    def __init__(self, reason):
        super().__init__(reason, unavailable=PlatformPortUnavailable,
                         private_path=_private_path)


def make_file_private(fd: int, path: str | Path) -> None:
    """Protect an already-created private file before publishing its bytes.

    The caller creates the descriptor with mkstemp or mode 0600.  On POSIX
    hosts without fchmod that creation guarantee can be verified directly;
    a platform whose ACL cannot be verified must provide its own adapter.
    """
    if os.name == "nt":
        api = _WindowsFileAdapter("private_file_permissions_unavailable")
        handle = api.open(path,0x60080)
        try:
            borrowed = api.borrowed_handle(fd)
            if api.identity(handle) != api.identity(borrowed):
                api.fail("descriptor_path_mismatch",0)
            api.protect(handle)
            verify_file_private(path,fd=fd)
        finally:
            api.close(handle)
        return
    if os.name != "posix":
        raise PlatformPortUnavailable("private_file_permissions_unavailable")
    chmod = getattr(os, "fchmod", None)
    if chmod is not None:
        chmod(fd, 0o600)
    metadata = os.fstat(fd)
    if (not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_uid != os.geteuid()):
        raise PlatformPortUnavailable("private_file_permissions_unavailable")



def verify_file_private(path: str | Path, *, fd: int | None = None) -> None:
    """Verify an existing private file, never repairing its ACL or contents."""
    candidate = _private_path(path)
    if os.name == "nt":
        api = _WindowsFileAdapter("private_file_permissions_unavailable")
        handle = api.open(candidate, 0x20080)
        try:
            identity = api.identity(handle)
            if fd is not None:
                borrowed = api.borrowed_handle(fd)
                if api.identity(borrowed) != identity:
                    api.fail("descriptor_path_mismatch",0)
                api.verify(borrowed)
            api.verify(handle)
            api.guard(candidate)
            # Reopen to detect a path replacement since the initial read.
            current = api.open(candidate,0x20080)
            try:
                if api.identity(current) != identity:
                    api.fail("path_identity_changed",0)
            finally:
                api.close(current)
        finally:
            api.close(handle)
        return
    if os.name != "posix":
        raise PlatformPortUnavailable("private_file_permissions_unavailable")
    metadata = os.fstat(fd) if fd is not None else candidate.lstat()
    path_metadata = candidate.lstat()
    if (not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_uid != os.geteuid()
            or (metadata.st_dev,metadata.st_ino) != (path_metadata.st_dev,path_metadata.st_ino)):
        raise PlatformPortUnavailable("private_file_permissions_unavailable")
    _private_path(candidate)


def read_private_file(path: str | Path) -> bytes:
    """Read the verified object through the same fd before and after reading."""
    candidate = _private_path(path)
    try:
        with candidate.open("rb") as stream:
            verify_file_private(candidate,fd=stream.fileno())
            raw = stream.read()
            verify_file_private(candidate,fd=stream.fileno())
            return raw
    except OSError as exc:
        raise PlatformPortUnavailable("private_file_permissions_unavailable:read_failed") from exc


def sync_directory(path: str | Path) -> None:
    """Keep directory durability inside the native file adapter."""
    if os.name == "nt":
        api = _WindowsFileAdapter("directory_sync_unavailable")
        handle = api.open(path,0x40000000,directory=True)
        try:
            api.check(api.flush(handle),"flush_directory")
            api.guard(path)
        finally:
            api.check(api.close(handle),"close_directory")
        return
    if os.name != "posix":
        raise PlatformPortUnavailable("directory_sync_unavailable")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_tree(path: str | Path) -> None:
    """Best-effort removal of a disposable tree, including read-only files.

    Git marks object files read-only; POSIX unlinks them anyway, but Windows
    refuses until the write bit is restored.  Failures stay swallowed, exactly
    like ``shutil.rmtree(..., ignore_errors=True)``.
    """
    import shutil

    def _retry(func, target, _exc) -> None:
        try:
            os.chmod(target, stat.S_IWRITE)
            func(target)
        except OSError:
            pass

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=_retry)
    else:
        shutil.rmtree(path, onerror=_retry)


@contextmanager
def locked_file(path: str | Path, *, lock_port: FileLockPort | None = None,
                timeout_seconds: float = 5.0):
    """Serialize bounded writes; unavailability never yields an unlocked body."""
    port = lock_port if lock_port is not None else PortableFileLock()
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        try:
            handle = port.acquire(path)
        except (ImportError, OSError) as exc:
            raise PlatformPortUnavailable("file_lock_unavailable") from exc
        if handle is not None:
            break
        if time.monotonic() >= deadline:
            raise PlatformPortUnavailable("file_lock_busy")
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    try:
        yield handle
    finally:
        port.release(handle)


class PortableFileLock:
    """Use the host's native advisory lock only inside this adapter."""

    _BUSY_ERRNOS = {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}

    def acquire(self, path: str | Path) -> FileLockHandle | None:
        lock_path = Path(path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        backend = "windows-msvcrt" if os.name == "nt" else "posix-flock"
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"\0")
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (ImportError, OSError) as exc:
            os.close(fd)
            if isinstance(exc, OSError) and exc.errno in self._BUSY_ERRNOS:
                return None
            raise
        return FileLockHandle(fd=fd, path=lock_path, backend=backend)

    def release(self, handle: FileLockHandle) -> None:
        try:
            if handle.backend == "windows-msvcrt":
                import msvcrt

                os.lseek(handle.fd, 0, os.SEEK_SET)
                msvcrt.locking(handle.fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fd, fcntl.LOCK_UN)
        finally:
            os.close(handle.fd)


class SchedulerPort(Protocol):
    def lock_path(self, root: str | Path) -> Path:
        ...

    def acquire(self, root: str | Path) -> FileLockHandle | None:
        ...

    def release(self, handle: FileLockHandle) -> None:
        ...


class FileLockSchedulerPort:
    def __init__(self, lock_port: FileLockPort | None = None, *, filename: str = "driver.lock") -> None:
        self.lock_port = lock_port or PortableFileLock()
        self.filename = filename

    def lock_path(self, root: str | Path) -> Path:
        return Path(root) / self.filename

    def acquire(self, root: str | Path) -> FileLockHandle | None:
        return self.lock_port.acquire(self.lock_path(root))

    def release(self, handle: FileLockHandle) -> None:
        self.lock_port.release(handle)


class SecretStorePort(Protocol):
    """Read-only secret seam; persistence belongs to a later install adapter."""

    def get(self, name: str, default: str | None = None) -> str | None:
        ...


class EnvironmentSecretStore:
    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self._values = dict(os.environ if values is None else values)

    def get(self, name: str, default: str | None = None) -> str | None:
        value = self._values.get(name)
        return value if value is not None else default


class ExecutionHostPort(Protocol):
    def __call__(self, workspace: Path, capsule: Mapping[str, Any]) -> dict[str, Any]:
        ...


# ExecutionFencePort already exists as LH's lifecycle fence contract.  Export
# the canonical type through the platform-port vocabulary instead of creating
# a second authority or a second implementation.
try:
    from .execution_fence import ExecutionFencePort  # noqa: E402
except ImportError:
    from execution_fence import ExecutionFencePort  # noqa: E402


__all__ = [
    "CapabilityUnavailable",
    "Deadline",
    "DeadlineExpired",
    "DeadlinePort",
    "EnvironmentSecretStore",
    "ExecutionFencePort",
    "ExecutionHostPort",
    "FileLockHandle",
    "FileLockPort",
    "FileLockSchedulerPort",
    "LocalProcessPort",
    "LocalShellPort",
    "MonotonicDeadline",
    "MonotonicDeadlinePort",
    "OptionalCapabilityAdapter",
    "OptionalCapabilityRegistry",
    "PlatformPaths",
    "PlatformPortUnavailable",
    "PortableFileLock",
    "locked_file",
    "make_file_private",
    "sync_directory",
    "ProcessPort",
    "ProcessResult",
    "ProcessTimeout",
    "SchedulerPort",
    "SecretStorePort",
    "ShellPort",
    "SUPPORTED_PLATFORM_FAMILIES",
    "UnsupportedPlatformError",
    "UnsupportedPlatformPort",
    "build_process_port",
    "build_shell_port",
    "is_supported_platform",
    "normalize_platform_name",
    "resolve_optional_capability",
]

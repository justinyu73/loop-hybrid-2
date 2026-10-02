"""Linux bubblewrap/seccomp execution-fence backend.

This module owns Linux-only namespace, mount, and libseccomp mechanics. The
portable execution_fence module exposes only the descriptor/port contract
and imports this backend lazily.
"""
from __future__ import annotations

import atexit
import ctypes
import ctypes.util
import errno
import hashlib
import json
import os
import shutil
import shlex
import stat
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

if __package__:
    from .execution_fence import (
        LINUX_BACKEND_ID, BINDING_SCHEMA, BINDING_SCHEMA_V2, CONTROL_LAUNCH_BUDGET,
        CONTROL_OPS, DESCRIPTOR_SCHEMA, EGRESS_POLICY_ENFORCED_BY, EXECUTION_HOST_ADAPTER_PREFIX,
        ExecutionFencePort, ExecutionFenceUnavailable, PROOF_SCHEMA,
        _validate_provider_argv_against_policy, digest_json, load_egress_policy,
        validate_egress_provider, DisabledExecutionFencePort, validate_phase_roots, validate_null_device_check,
    )
else:
    from execution_fence import (
        LINUX_BACKEND_ID, BINDING_SCHEMA, BINDING_SCHEMA_V2, CONTROL_LAUNCH_BUDGET,
        CONTROL_OPS, DESCRIPTOR_SCHEMA, EGRESS_POLICY_ENFORCED_BY, EXECUTION_HOST_ADAPTER_PREFIX,
        ExecutionFencePort, ExecutionFenceUnavailable, PROOF_SCHEMA,
        _validate_provider_argv_against_policy, digest_json, load_egress_policy,
        validate_egress_provider, DisabledExecutionFencePort, validate_phase_roots, validate_null_device_check,
    )

BACKEND_ID = LINUX_BACKEND_ID
EXPECTED_BWRAP_VERSION = "0.9.0"
REQUIRED_PROOF_TRACKS = (
    "filesystem_effect_containment",
    "provider_control_egress",
    "provider_sandbox",
)
# host-bline-provider-sandbox: the hosted provider's own containment is composed
# by this client, not enforced by the execution host -- the proof track says
# exactly that and no more (packet §7: proof semantics do not upgrade).
PROVIDER_SANDBOX_ENFORCED_BY = "lh-client-composed"
PROVIDER_SECCOMP_PROGRAM_BASENAME = ".lh-provider-seccomp.bpf"
PROVIDER_SECCOMP_TMP_PREFIX = "lh-host-provider-seccomp-"
CODEX_TRANSIENT_HOME = "/tmp/codex-home"
CODEX_PROVIDER_HOME_FILES = ("auth.json", "config.toml")

# The child receives an already-open, Attempt-bound stdio channel.  Creation
# of alternate network, socket, IPC, signal, namespace, or process-control
# routes is rejected by the filter.
DENIED_SYSCALLS = (
    "socket",
    "socketpair",
    "connect",
    "bind",
    "listen",
    "accept",
    "accept4",
    "sendto",
    "recvfrom",
    "sendmsg",
    "recvmsg",
    "recvmmsg",
    "sendmmsg",
    "getsockname",
    "getpeername",
    "setsockopt",
    "getsockopt",
    "shutdown",
    "pipe",
    "pipe2",
    "eventfd",
    "eventfd2",
    "memfd_create",
    "msgget",
    "msgsnd",
    "msgrcv",
    "msgctl",
    "semget",
    "semop",
    "semtimedop",
    "semctl",
    "shmget",
    "shmat",
    "shmdt",
    "shmctl",
    "mq_open",
    "mq_unlink",
    "mq_timedsend",
    "mq_timedreceive",
    "mq_notify",
    "mq_getsetattr",
    "kill",
    "tkill",
    "tgkill",
    "pidfd_send_signal",
    "ptrace",
    "process_vm_readv",
    "process_vm_writev",
    "pidfd_open",
    "pidfd_getfd",
    "clone",
    "clone3",
    "fork",
    "vfork",
    "unshare",
    "setns",
    "rt_sigqueueinfo",
    "rt_tgsigqueueinfo",
    "io_uring_setup",
    "io_uring_register",
    "io_uring_enter",
    "open_by_handle_at",
    "name_to_handle_at",
    "mount",
    "umount2",
    "pivot_root",
    "chroot",
    "fsopen",
    "fsmount",
    "fsconfig",
    "move_mount",
    "open_tree",
)


def _sha256_file(path: Any) -> str | None:
    try:
        return "sha256:" + hashlib.sha256(Path(str(path)).read_bytes()).hexdigest()
    except OSError:
        return None


def normalize_sandbox_profile(
    raw: Mapping[str, Any], agent: str
) -> dict[str, Any]:
    """The effective provider-sandbox profile for one agent, as a signed value.

    host-bline-provider-sandbox §7: the profile (bubblewrap pin, bind set,
    network mode, provider seccomp table, env, cwd) comes from the policy
    artefact and is signed into the descriptor at ``prepare()``. Everything
    here is deterministic so ``digest_json`` of the result is stable."""
    if not isinstance(raw, Mapping) or not raw:
        raise ExecutionFenceUnavailable("egress_policy_sandbox_profile_missing")
    bubblewrap = raw.get("bubblewrap")
    seccomp = raw.get("seccomp")
    homes = raw.get("provider_home_ro_binds")
    if (
        not isinstance(bubblewrap, Mapping)
        or not isinstance(bubblewrap.get("path"), str)
        or not str(bubblewrap.get("sha256", "")).startswith("sha256:")
        or not isinstance(bubblewrap.get("version"), str)
        or raw.get("network") != "host"
        or not isinstance(raw.get("ro_binds"), list)
        or not isinstance(homes, Mapping)
        or not isinstance(seccomp, Mapping)
        or seccomp.get("default") != "allow"
        or not isinstance(seccomp.get("denied_syscalls"), list)
        or not seccomp.get("denied_syscalls")
        or not isinstance(raw.get("flags"), list)
    ):
        raise ExecutionFenceUnavailable("egress_policy_sandbox_profile_invalid")
    agent_homes = homes.get(agent)
    if not isinstance(agent_homes, list):
        raise ExecutionFenceUnavailable(
            "egress_policy_sandbox_profile_provider_missing"
        )
    environment = {
        str(name): str(value)
        for name, value in sorted((raw.get("env") or {}).items())
    }
    provider_home_ro_files: list[dict[str, str]] = []
    if agent == "codex":
        # CODEX_HOME is both the credential/config root and the app-server
        # state root.  Mounting the real provider home read-only therefore
        # lets Codex authenticate but prevents `exec` from starting.  Keep
        # the source home read-only, expose only the required files through
        # read-only binds, and leave the transient state directory writable.
        if len(agent_homes) != 1:
            raise ExecutionFenceUnavailable(
                "egress_policy_codex_home_binding_ambiguous"
            )
        source_home = Path(str(agent_homes[0]))
        for name in CODEX_PROVIDER_HOME_FILES:
            source = source_home / name
            if not source.is_file():
                raise ExecutionFenceUnavailable(
                    f"egress_policy_codex_provider_file_missing:{name}"
                )
            provider_home_ro_files.append({
                "source": str(source),
                "target": f"{CODEX_TRANSIENT_HOME}/{name}",
            })
        environment["CODEX_HOME"] = CODEX_TRANSIENT_HOME
        environment["HOME"] = "/tmp"
    return {
        "bubblewrap": {
            "path": str(bubblewrap["path"]),
            "sha256": str(bubblewrap["sha256"]),
            "version": str(bubblewrap["version"]),
        },
        "network": "host",
        "namespaces": {
            "user": "new",
            "pid": "new",
            "ipc": "new",
            "uts": "new",
            "network": "host",
            "nested_user_namespaces": "disabled",
        },
        "ro_binds": sorted(
            {str(item) for item in [*raw["ro_binds"], *agent_homes]}
        ),
        "workspace": {"mount": "rw", "cwd": "clone_root"},
        "env": environment,
        "provider_home_ro_files": provider_home_ro_files,
        "flags": [str(item) for item in raw["flags"]],
        "seccomp": {
            "default": "allow",
            "denied_action": f"errno:{errno.EPERM}",
            "denied_syscalls": sorted(
                {str(item) for item in seccomp["denied_syscalls"]}
            ),
        },
    }


def _path_is_under(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _provider_runtime_binding(
    provider_path: str, sandbox: Mapping[str, Any]
) -> dict[str, str] | None:
    """Bind a script provider's interpreter to the signed read-only profile.

    "resolve_cli" deliberately pins Codex to its real "codex.js" file.  That
    file's "#!/usr/bin/env node" shebang still needs the matching nvm "bin"
    directory after the sandbox clears the ambient environment.  The runtime
    directory is derived from the pinned file and admitted only when both the
    directory and the resolved interpreter are covered by a profile bind.  No
    ambient PATH entry is consulted or added.  The interpreter bytes are
    included in the binding so a later provider launch cannot silently swap
    the runtime underneath the descriptor.
    """
    provider = Path(provider_path).resolve(strict=False)
    try:
        with provider.open("r", encoding="utf-8", errors="replace") as handle:
            first_line = handle.readline(256)
    except (OSError, UnicodeError):
        return None
    shebang = first_line[2:].strip().split() if first_line.startswith("#!") else []
    if shebang != ["/usr/bin/env", "node"]:
        return None

    bound_roots: list[Path] = []
    for raw_root in sandbox.get("ro_binds") or []:
        try:
            bound_roots.append(Path(str(raw_root)).resolve(strict=False))
        except (OSError, RuntimeError):
            continue
    for parent in provider.parents:
        runtime_dir = parent / "bin"
        runtime = runtime_dir / "node"
        try:
            runtime_dir = runtime_dir.resolve(strict=False)
            runtime = runtime.resolve(strict=False)
        except (OSError, RuntimeError):
            continue
        if not runtime.is_file() or not os.access(runtime, os.X_OK):
            continue
        if not bound_roots or not all(
            any(_path_is_under(candidate, root) for root in bound_roots)
            for candidate in (runtime_dir, runtime)
        ):
            continue
        runtime_digest = _sha256_file(runtime)
        if runtime_digest is None:
            raise ExecutionFenceUnavailable("provider_interpreter_unreadable")
        return {
            "interpreter": "node",
            "path": str(runtime),
            "sha256": runtime_digest,
        }
    raise ExecutionFenceUnavailable("provider_interpreter_not_bound")


def _validate_provider_runtime_binding(
    provider_path: str,
    sandbox: Mapping[str, Any],
    declared: Mapping[str, Any] | None,
) -> None:
    actual = _provider_runtime_binding(provider_path, sandbox)
    if actual != declared:
        raise ExecutionFenceUnavailable("control_provider_interpreter_drifted")


def _provider_runtime_bin_dirs(
    provider_path: str, sandbox: Mapping[str, Any]
) -> tuple[str, ...]:
    binding = _provider_runtime_binding(provider_path, sandbox)
    if binding is None:
        return ()
    return (str(Path(binding["path"]).parent),)


def _sandbox_bwrap_argv(
    sandbox: Mapping[str, Any],
    *,
    provider_bin_dir: str,
    provider_runtime_bin_dirs: Sequence[str] = (),
    clone_root: str,
    env_overlay: Mapping[str, str] | None,
) -> list[str]:
    """The deterministic bubblewrap vector for one provider launch."""
    argv: list[str] = [str(sandbox["bubblewrap"]["path"])]
    namespaces = sandbox.get("namespaces") or {}
    for namespace, flag in (
        ("user", "--unshare-user"),
        ("pid", "--unshare-pid"),
        ("ipc", "--unshare-ipc"),
        ("uts", "--unshare-uts"),
    ):
        if namespaces.get(namespace) == "new":
            argv.append(flag)
    if namespaces.get("nested_user_namespaces") == "disabled":
        argv.append("--disable-userns")
    argv.extend(str(item) for item in sandbox.get("flags") or [])
    for path in sandbox.get("ro_binds") or []:
        argv.extend(["--ro-bind-try", str(path), str(path)])
    argv.extend(["--dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp"])
    if sandbox.get("provider_home_ro_files"):
        argv.extend(["--dir", CODEX_TRANSIENT_HOME])
        for item in sandbox["provider_home_ro_files"]:
            if not isinstance(item, Mapping):
                raise ExecutionFenceUnavailable("provider_home_ro_file_binding_invalid")
            source = item.get("source")
            target = item.get("target")
            if (
                not isinstance(source, str)
                or not isinstance(target, str)
                or not target.startswith(CODEX_TRANSIENT_HOME + "/")
                or "/" in target[len(CODEX_TRANSIENT_HOME) + 1 :]
            ):
                raise ExecutionFenceUnavailable("provider_home_ro_file_binding_invalid")
            argv.extend(["--ro-bind", source, target])
    argv.extend(["--bind", clone_root, clone_root, "--chdir", clone_root])
    path_dirs = list(dict.fromkeys([
        provider_bin_dir,
        *provider_runtime_bin_dirs,
        "/usr/bin",
        "/bin",
    ]))
    merged_env = {
        "TMPDIR": "/tmp",
        **{str(k): str(v) for k, v in (sandbox.get("env") or {}).items()},
        **{str(k): str(v) for k, v in (env_overlay or {}).items()},
        # PATH is a fence-owned value.  An env overlay cannot remove a signed
        # provider interpreter or introduce a target-owned executable path.
        "PATH": os.pathsep.join(path_dirs),
    }
    argv.append("--clearenv")
    for name, value in sorted(merged_env.items()):
        argv.extend(["--setenv", name, value])
    return argv


def compose_terminal_command(
    provider_argv: Sequence[str],
    *,
    output_path: str | None = None,
    env_overlay: Mapping[str, str] | None = None,
    sandbox: Mapping[str, Any] | None = None,
    seccomp_program_path: str | None = None,
    clone_root: str | None = None,
) -> str:
    """Compose the one shell string a control-plane terminal may run.

    The fence composes this itself so an adapter never passes a free-form
    ``--command`` string (decision packet constraint 2). With ``sandbox``
    (host-bline-provider-sandbox), the provider argv is wrapped in the
    descriptor-signed bubblewrap profile; the seccomp program rides an fd
    redirect because a detached terminal string cannot inherit one."""
    argv = [str(item) for item in provider_argv]
    if not argv:
        raise ExecutionFenceUnavailable("control_provider_argv_empty")
    if sandbox is not None:
        if not seccomp_program_path or not clone_root:
            raise ExecutionFenceUnavailable("provider_sandbox_compose_invalid")
        provider_runtime_bin_dirs = _provider_runtime_bin_dirs(argv[0], sandbox)
        bwrap_argv = _sandbox_bwrap_argv(
            sandbox,
            provider_bin_dir=str(Path(argv[0]).parent),
            provider_runtime_bin_dirs=provider_runtime_bin_dirs,
            clone_root=str(clone_root),
            env_overlay=env_overlay,
        )
        bwrap_argv.extend(["--seccomp", "9"])
        command = (
            f"exec 9<{shlex.quote(str(seccomp_program_path))}; "
            f"exec {shlex.join(bwrap_argv)} -- {shlex.join(argv)}"
        )
    else:
        env_prefix = " ".join(
            f"{name}={shlex.quote(str(value))}"
            for name, value in sorted((env_overlay or {}).items())
        )
        prefix = f"PATH={shlex.quote(str(Path(argv[0]).parent))}:$PATH"
        if env_prefix:
            prefix = f"{env_prefix} {prefix}"
        command = f"{prefix} exec {shlex.join(argv)}"
    if output_path is None:
        return command
    captured = shlex.quote(str(output_path))
    return f"({command}) > {captured} 2>&1; status=$?; cat {captured}; exit $status"


def _selector_matches_clone(selector: str, clone_root: str) -> bool:
    """A worktree selector may name the clone in POSIX form or in the
    ``\\\\wsl.localhost\\<distro>\\...`` UNC form a Windows-hosted Orca stores
    (cli_agent_executor._orca_worktree_selector emits both). Either way it must
    resolve to exactly the descriptor-bound clone root."""
    if not selector.startswith("path:"):
        return False
    value = selector[len("path:"):]
    if value == clone_root:
        return True
    distro = os.environ.get("WSL_DISTRO_NAME")
    unc_prefix = f"\\\\wsl.localhost\\{distro}\\" if distro else None
    if unc_prefix and value.startswith(unc_prefix):
        posix = "/" + value[len(unc_prefix):].replace("\\", "/")
        return posix == clone_root
    return False


def compose_control_argv(request: Mapping[str, Any]) -> list[str]:
    """Map one structured control request to Orca CLI arguments.

    Free strings are structurally impossible: every op has a closed argument
    schema, and ``terminal_create``'s command is composed here."""
    op = request.get("op")
    if op == "capability_probe":
        # Released Orca exposes its versioned runtime vector through status;
        # the adapter normalizes that observation into its closed capability
        # schema before admitting any other control operation.
        return ["status", "--json"]
    if op == "repo_list":
        return ["repo", "list", "--json"]
    if op == "repo_add":
        return ["repo", "add", "--path", str(request["path"]), "--json"]
    if op == "project_setup_delete":
        return ["project", "setup-delete", "--setup", str(request["setup"]), "--json"]
    if op == "terminal_create":
        command = compose_terminal_command(
            request["provider_argv"],
            output_path=request.get("output_path"),
            env_overlay=request.get("env_overlay"),
            sandbox=request.get("provider_sandbox_profile"),
            seccomp_program_path=request.get("provider_seccomp_program_path"),
            clone_root=request.get("clone_root"),
        )
        return [
            "terminal", "create",
            "--worktree", str(request["worktree_selector"]),
            "--title", str(request["title"]),
            "--command", command,
            "--json",
        ]
    if op == "terminal_wait":
        return [
            "terminal", "wait", "--terminal", str(request["handle"]),
            "--for", "exit", "--timeout-ms", str(int(request["timeout_ms"])), "--json",
        ]
    if op == "terminal_read":
        return [
            "terminal", "read", "--terminal", str(request["handle"]),
            "--limit", str(int(request["limit"])), "--json",
        ]
    if op == "terminal_stop":
        return ["terminal", "stop", "--worktree", str(request["worktree_selector"]), "--json"]
    if op == "terminal_close":
        return ["terminal", "close", "--terminal", str(request["handle"]), "--json"]
    raise ExecutionFenceUnavailable("control_op_unknown")




class LinuxBubblewrapExecutionFence(ExecutionFencePort):
    """bubblewrap 0.9.0 mount/net namespaces plus a libseccomp filter."""

    supports_started_notification = True

    def project_environment(self, descriptor: Mapping[str, Any],
                            environment: Mapping[str, str]) -> dict[str, str]:
        projected = super().project_environment(descriptor, environment)
        if descriptor["binding"]["schema"] != BINDING_SCHEMA_V2:
            return projected
        if environment.get("PYTHONDONTWRITEBYTECODE") == "1":
            projected["PYTHONDONTWRITEBYTECODE"] = "1"
        scratch = "scratch_write" in descriptor["binding"]["allowed_local_effects"]
        for name, target in (("LH_HOST_STATE_ROOT", "/tmp/host-state"),
                             ("LH_HOST_TMP_ROOT", "/tmp/host-tmp"), ("TMPDIR", "/tmp")):
            if name in environment:
                if not scratch:
                    raise ExecutionFenceUnavailable("scratch_environment_not_granted")
                projected[name] = target
        if environment.get("LH_ROOT"):
            root = str(descriptor["binding"]["clone_root"])
            value = environment["LH_ROOT"]
            if value == root or value.startswith(root + "/"):
                projected["LH_ROOT"] = "/workspace" + value[len(root):]
        return projected

    def project_command(self, descriptor: Mapping[str, Any], argv: Sequence[str],
                        environment: Mapping[str, str]) -> list[str]:
        mappings = self._path_mappings(descriptor["binding"])
        projected_env = self.project_environment(descriptor, environment)
        for name in ("LH_HOST_STATE_ROOT", "LH_HOST_TMP_ROOT", "TMPDIR"):
            if name in environment and name in projected_env:
                source = environment[name]
                if not Path(source).is_absolute() or Path(source) == Path("/"):
                    raise ExecutionFenceUnavailable("scratch_path_invalid")
                mappings.append((source, projected_env[name]))
        output = list(argv)
        for index, raw in enumerate(output[1:], start=1):
            for source, target in sorted(mappings, key=lambda pair: -len(pair[0])):
                if raw == source or raw.startswith(source + "/"):
                    output[index] = target + raw[len(source):]
                    break
        return output

    @staticmethod
    def _path_mappings(binding: Mapping[str, Any]) -> list[tuple[str, str]]:
        return [(str(binding["clone_root"]), "/workspace"), *[
            (root, f"/inputs/{index}") for index, root in
            enumerate(binding.get("allowed_read_roots", []))
            if root != binding["clone_root"]]]

    def project_paths(self, descriptor: Mapping[str, Any], value: Mapping[str, Any],
                      *, reverse: bool = False) -> dict[str, Any]:
        projected = json.loads(json.dumps(dict(value)))
        mappings = self._path_mappings(descriptor["binding"])
        if reverse:
            mappings = [(child, host) for host, child in mappings]
        for field in ("worktree", "source_worktree", "integration_worktree", "packet_path", "git_index_file"):
            raw = projected.get(field)
            if not isinstance(raw, str):
                continue
            for source, target in sorted(mappings, key=lambda pair: -len(pair[0])):
                if raw == source or raw.startswith(source + "/"):
                    projected[field] = target + raw[len(source):]
                    break
        if "evidence_refs" in projected:
            refs = projected["evidence_refs"]
            if not isinstance(refs, list):
                raise ExecutionFenceUnavailable("evidence_path_projection_invalid")
            for ref in refs:
                if not isinstance(ref, dict) or not isinstance(ref.get("path"), str):
                    raise ExecutionFenceUnavailable("evidence_path_projection_invalid")
                raw = ref["path"]
                if not Path(raw).is_absolute() or ".." in raw.split("/"):
                    raise ExecutionFenceUnavailable("evidence_path_projection_invalid")
                matched = False
                for source, target in sorted(mappings, key=lambda pair: -len(pair[0])):
                    if raw != source and not raw.startswith(source + "/"):
                        continue
                    mapped = target + raw[len(source):]
                    # A child path cannot turn a clone symlink into permission
                    # for the controller to read an ungranted host file.
                    host_path, host_root = (mapped, target) if reverse else (raw, source)
                    try:
                        resolved = Path(host_path).resolve(strict=True)
                        allowed = Path(host_root).resolve(strict=True)
                    except OSError as exc:
                        raise ExecutionFenceUnavailable("evidence_path_projection_unavailable") from exc
                    if resolved != allowed and allowed not in resolved.parents:
                        raise ExecutionFenceUnavailable("evidence_path_projection_escape")
                    ref["path"] = mapped
                    matched = True
                    break
                if not matched:
                    raise ExecutionFenceUnavailable("evidence_path_projection_unmapped")
        return projected

    def __init__(
        self,
        *,
        bubblewrap_path: str,
        bubblewrap_version: str,
        seccomp_library: str,
        proof_tracks: Sequence[str] = REQUIRED_PROOF_TRACKS,
        clock: Callable[[], float] = time.time,
    ):
        self.bubblewrap_path = str(Path(bubblewrap_path).resolve())
        self.bubblewrap_version = bubblewrap_version
        self.seccomp_library = seccomp_library
        self.proof_tracks = tuple(proof_tracks)
        self.clock = clock
        self._prepared: dict[str, dict[str, Any]] = {}
        self._consumed: set[str] = set()
        self.launch_count = 0
        self._control_counts: dict[str, int] = {}
        self.control_audit: dict[str, list[dict[str, Any]]] = {}
        self._provider_seccomp_paths: dict[str, list[str]] = {}
        # A failed control RPC can occur before the adapter has a terminal
        # handle with which to request cleanup.  The process-exit hook keeps
        # those task-owned temporary files out of the target clone and avoids
        # leaving them behind after an interrupted Attempt.
        atexit.register(self._cleanup_all_provider_seccomp)

    def _cleanup_provider_seccomp(self, descriptor_digest: str) -> None:
        for path in self._provider_seccomp_paths.pop(descriptor_digest, []):
            try:
                Path(path).unlink()
            except OSError:
                pass

    def _cleanup_all_provider_seccomp(self) -> None:
        for descriptor_digest in list(self._provider_seccomp_paths):
            self._cleanup_provider_seccomp(descriptor_digest)

    def _write_provider_seccomp_program(
        self, program: bytes, descriptor_digest: str
    ) -> str:
        """Persist a descriptor-derived BPF file outside the target clone.

        Orca receives a detached shell command and therefore cannot inherit
        the in-process memfd directly.  The file is task-owned, mode 0600,
        tracked by descriptor digest, and removed on terminal close or process
        exit.  It is deliberately not part of the target repository diff.
        """
        try:
            fd, program_path = tempfile.mkstemp(
                prefix=PROVIDER_SECCOMP_TMP_PREFIX, suffix=".bpf"
            )
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(program)
                os.chmod(program_path, 0o600)
            except BaseException:
                try:
                    os.close(fd)
                except OSError:
                    pass
                try:
                    Path(program_path).unlink()
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise ExecutionFenceUnavailable(
                "provider_seccomp_program_unwritable"
            ) from exc
        self._provider_seccomp_paths.setdefault(descriptor_digest, []).append(
            program_path
        )
        return program_path

    @classmethod
    def discover(
        cls,
        *,
        expected_version: str = EXPECTED_BWRAP_VERSION,
        clock: Callable[[], float] = time.time,
    ) -> ExecutionFencePort:
        binary = shutil.which("bwrap")
        library = ctypes.util.find_library("seccomp")
        if binary is None:
            return DisabledExecutionFencePort("bubblewrap_missing")
        if library is None:
            return DisabledExecutionFencePort("seccomp_library_missing")
        try:
            result = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return DisabledExecutionFencePort("bubblewrap_version_unreadable")
        version = result.stdout.strip().removeprefix("bubblewrap ").strip()
        if result.returncode != 0 or version != expected_version:
            return DisabledExecutionFencePort("bubblewrap_version_unsupported")
        return cls(
            bubblewrap_path=binary,
            bubblewrap_version=version,
            seccomp_library=library,
            clock=clock,
        )

    def _validate_backend(self) -> None:
        if self.bubblewrap_version != EXPECTED_BWRAP_VERSION:
            raise ExecutionFenceUnavailable("bubblewrap_version_unsupported")
        if Path(self.bubblewrap_path).name != "bwrap":
            raise ExecutionFenceUnavailable("bubblewrap_identity_invalid")
        if not Path(self.bubblewrap_path).is_file():
            raise ExecutionFenceUnavailable("bubblewrap_missing")
        if set(self.proof_tracks) != set(REQUIRED_PROOF_TRACKS):
            raise ExecutionFenceUnavailable("proof_track_incomplete")
        if not self.seccomp_library:
            raise ExecutionFenceUnavailable("seccomp_library_missing")

    @staticmethod
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
        phase_binding = binding.get("schema") == BINDING_SCHEMA_V2
        if phase_binding:
            required |= {"allowed_read_roots", "execution_context_digest"}
        if "null_device_check" in binding:
            required.add("null_device_check")
            validate_null_device_check(binding)
            grant = binding["null_device_check"]
            try:
                git_path = Path(shutil.which("git", path=os.defpath) or "").resolve(strict=True)
                device = os.lstat("/dev/null")
            except (OSError, RuntimeError) as exc:
                raise ExecutionFenceUnavailable("null_device_check_identity_unavailable") from exc
            if (str(git_path) != grant["argv"][0]
                or _sha256_file(git_path) != grant["executable_sha256"]
                or not stat.S_ISCHR(device.st_mode)
                or (os.major(device.st_rdev), os.minor(device.st_rdev)) != (1, 3)):
                raise ExecutionFenceUnavailable("null_device_check_identity_changed")
        if set(binding) != required or binding.get("schema") not in {BINDING_SCHEMA, BINDING_SCHEMA_V2}:
            raise ExecutionFenceUnavailable("binding_fields_invalid")
        clone = Path(str(binding["clone_root"]))
        try:
            resolved_clone = clone.resolve(strict=True)
        except OSError as exc:
            raise ExecutionFenceUnavailable("clone_root_unreadable") from exc
        if str(resolved_clone) != str(clone) or not resolved_clone.is_dir():
            raise ExecutionFenceUnavailable("clone_root_not_canonical")
        if phase_binding:
            validate_phase_roots(binding)
        elif binding.get("allowed_write_roots") != [str(resolved_clone)]:
            raise ExecutionFenceUnavailable("write_roots_invalid")
        if not phase_binding and binding.get("allowed_local_effects") != ["workspace_write"]:
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

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_backend()
        normalized = self._validate_binding(binding)
        if str(normalized["adapter_id"]).startswith("orca-"):
            raise ExecutionFenceUnavailable(
                "adapter_provider_channel_unsupported"
            )
        binding_digest = digest_json(normalized)
        adapter_id = str(normalized["adapter_id"])
        launch_classes = {"control": 0, "mutation": 1}
        control_plane: dict[str, Any] | None = None
        if adapter_id.startswith(EXECUTION_HOST_ADAPTER_PREFIX):
            # Control-plane RPCs run outside the sandbox; pin both binaries by
            # digest now so a later launch cannot swap them (packet constraint 2).
            agent = adapter_id[len(EXECUTION_HOST_ADAPTER_PREFIX):]
            orca_value = os.environ.get("LH_ORCA_CLI", "").strip()
            if not orca_value:
                raise ExecutionFenceUnavailable("control_orca_cli_unpinned")
            orca_path = Path(orca_value)
            orca_digest = _sha256_file(orca_path)
            if not orca_path.is_absolute() or orca_digest is None:
                raise ExecutionFenceUnavailable("control_orca_cli_unreadable")
            provider_path = shutil.which(agent)
            if provider_path is not None:
                provider_path = str(Path(provider_path).resolve(strict=False))
            provider_digest = _sha256_file(provider_path) if provider_path else None
            if provider_path is None or provider_digest is None:
                raise ExecutionFenceUnavailable("control_provider_unresolved")
            policy, policy_digest = load_egress_policy()
            provider_policy = validate_egress_provider(
                policy,
                agent,
                provider_path=str(provider_path),
                provider_digest=provider_digest,
                orca_path=str(orca_path),
                orca_digest=orca_digest,
            )
            launch_classes = {"control": CONTROL_LAUNCH_BUDGET, "mutation": 0}
            sandbox_profile = normalize_sandbox_profile(
                policy.get("provider_sandbox_profile") or {}, agent
            )
            runtime_binding = _provider_runtime_binding(
                str(provider_path), sandbox_profile
            )
            pinned_bwrap = sandbox_profile["bubblewrap"]
            if _sha256_file(pinned_bwrap["path"]) != pinned_bwrap["sha256"]:
                raise ExecutionFenceUnavailable("provider_sandbox_bwrap_mismatch")
            provider_binding = {
                "agent": agent,
                "path": str(provider_path),
                "sha256": provider_digest,
            }
            if runtime_binding is not None:
                provider_binding["runtime"] = runtime_binding
            control_plane = {
                "orca_cli": {"path": str(orca_path), "sha256": orca_digest},
                "provider": provider_binding,
                "egress_policy": {
                    "digest": policy_digest,
                    "issuer": str(policy.get("issuer")),
                    "enforced_by": EGRESS_POLICY_ENFORCED_BY,
                    "flags": sorted(str(x) for x in provider_policy.get("flags") or []),
                    "value_flags": sorted(str(x) for x in provider_policy.get("value_flags") or []),
                    "prompt_flags": sorted(str(x) for x in provider_policy.get("prompt_flags") or []),
                    "trailing_prompt": bool(provider_policy.get("trailing_prompt")),
                },
                "provider_sandbox": {
                    "profile": sandbox_profile,
                    "profile_digest": digest_json(sandbox_profile),
                },
            }
        syscall_policy = {
            "default": "allow",
            "denied_action": f"errno:{errno.EPERM}",
            "denied_syscalls": list(DENIED_SYSCALLS),
        }
        syscall_policy_digest = digest_json(syscall_policy)
        mount_policy = {
            "clone_mount": "/workspace",
            "clone_source": normalized["clone_root"],
            "write_roots": normalized["allowed_write_roots"],
            "system_roots": ["/usr", "/bin", "/lib", "/lib64"],
            "home": "absent",
            "host_state": "absent",
            "device_tree": "absent",
        }
        if normalized["schema"] == BINDING_SCHEMA_V2:
            mount_policy.update(read_roots=normalized["allowed_read_roots"],
                                path_mappings=self._path_mappings(normalized),
                                scratch="ephemeral" if "scratch_write" in normalized["allowed_local_effects"] else "absent")
        mount_policy_digest = digest_json(mount_policy)
        if "null_device_check" in normalized:
            mount_policy["device_tree"] = "null_device_check_only"
            mount_policy["null_device_check"] = normalized["null_device_check"]
            mount_policy_digest = digest_json(mount_policy)
        namespace_policy = {
            "user": "new",
            "ipc": "new",
            "pid": "new",
            "network": "new_without_interfaces",
            "uts": "new",
            "cgroup": "new",
            "nested_user_namespaces": "disabled",
            "capabilities": "none",
            "session": "new",
        }
        namespace_policy_digest = digest_json(namespace_policy)
        backend = {
            "backend_id": BACKEND_ID,
            "backend_version": self.bubblewrap_version,
            "bubblewrap_path": self.bubblewrap_path,
            "seccomp_library": self.seccomp_library,
            "mount_policy_digest": mount_policy_digest,
            "namespace_policy_digest": namespace_policy_digest,
            "syscall_policy_digest": syscall_policy_digest,
        }
        backend_digest = digest_json(backend)
        proofs = {
            "filesystem_effect_containment": {
                "schema": PROOF_SCHEMA,
                "track": "filesystem_effect_containment",
                "result": "admissible",
                "attempt_binding_digest": binding_digest,
                "policy_digest": mount_policy_digest,
                "backend_digest": backend_digest,
            },
            "provider_control_egress": {
                "schema": PROOF_SCHEMA,
                "track": "provider_control_egress",
                "result": "admissible",
                "attempt_binding_digest": binding_digest,
                "policy_digest": syscall_policy_digest,
                "namespace_policy_digest": namespace_policy_digest,
                "provider_control_channel_digest": digest_json(
                    normalized["provider_control_channel"]
                ),
                "backend_digest": backend_digest,
            },
            "provider_sandbox": {
                "schema": PROOF_SCHEMA,
                "track": "provider_sandbox",
                # A pure-mutation descriptor launches its child under this
                # kernel fence itself; there is no hosted provider terminal
                # for a composed sandbox to apply to.
                "result": "not_applicable",
                "attempt_binding_digest": binding_digest,
                "backend_digest": backend_digest,
            },
        }
        if control_plane is not None:
            # The provider process runs on the execution host (an Orca
            # terminal), not under this kernel fence. Saying "admissible"
            # here would be a false attestation (packet constraint 3).
            egress = proofs["provider_control_egress"]
            egress["result"] = "delegated_to_execution_host"
            egress["control_plane_digest"] = digest_json(control_plane)
            sandbox_proof = proofs["provider_sandbox"]
            sandbox_proof["result"] = "applied"
            sandbox_proof["profile_digest"] = control_plane[
                "provider_sandbox"
            ]["profile_digest"]
            sandbox_proof["enforced_by"] = PROVIDER_SANDBOX_ENFORCED_BY
        body = {
            "schema": DESCRIPTOR_SCHEMA,
            "binding": normalized,
            "binding_digest": binding_digest,
            "backend": backend,
            "backend_digest": backend_digest,
            "proofs": proofs,
            "proofs_digest": digest_json(proofs),
            "launch_classes": launch_classes,
            "mutation_dispatch": (
                "enabled_for_descriptor"
                if launch_classes["mutation"] > 0
                else "delegated_to_execution_host"
            ),
        }
        if control_plane is not None:
            body["control_plane"] = control_plane
        descriptor = {**body, "launch_descriptor_digest": digest_json(body)}
        digest = descriptor["launch_descriptor_digest"]
        self._prepared[digest] = json.loads(json.dumps(descriptor))
        return descriptor

    def _validate_descriptor(
        self,
        descriptor: Mapping[str, Any],
        *,
        require_prepared: bool,
    ) -> dict[str, Any]:
        if not isinstance(descriptor, Mapping):
            raise ExecutionFenceUnavailable("descriptor_missing")
        normalized = json.loads(json.dumps(dict(descriptor)))
        digest = normalized.get("launch_descriptor_digest")
        body = {
            key: value
            for key, value in normalized.items()
            if key != "launch_descriptor_digest"
        }
        if (
            normalized.get("schema") != DESCRIPTOR_SCHEMA
            or not isinstance(digest, str)
            or digest_json(body) != digest
        ):
            raise ExecutionFenceUnavailable("descriptor_digest_invalid")
        if require_prepared and self._prepared.get(digest) != normalized:
            raise ExecutionFenceUnavailable("descriptor_not_prepared")
        if digest in self._consumed:
            raise ExecutionFenceUnavailable("descriptor_replayed")
        binding = normalized.get("binding")
        self._validate_binding(binding if isinstance(binding, dict) else {})
        if normalized.get("binding_digest") != digest_json(binding):
            raise ExecutionFenceUnavailable("binding_digest_invalid")
        proofs = normalized.get("proofs")
        if not isinstance(proofs, dict) or set(proofs) != set(REQUIRED_PROOF_TRACKS):
            raise ExecutionFenceUnavailable("proof_track_incomplete")
        classes = normalized.get("launch_classes") or {"control": 0, "mutation": 1}
        control_plane = normalized.get("control_plane")
        hosted = (
            int(classes.get("mutation", 1)) == 0
            and int(classes.get("control", 0)) > 0
            and isinstance(control_plane, dict)
        )
        for track in REQUIRED_PROOF_TRACKS:
            proof = proofs.get(track)
            allowed_results = {"admissible"}
            if track == "provider_control_egress" and hosted:
                # A host-delegated descriptor must say so -- and only such a
                # descriptor may (B-line packet constraint 3).
                allowed_results = {"delegated_to_execution_host"}
            if track == "provider_sandbox":
                # Hosted descriptors carry the composed profile; mutation
                # descriptors have no hosted provider to sandbox. Either way
                # only its own honest value is admissible.
                allowed_results = {"applied"} if hosted else {"not_applicable"}
            if (
                not isinstance(proof, dict)
                or proof.get("schema") != PROOF_SCHEMA
                or proof.get("track") != track
                or proof.get("result") not in allowed_results
                or proof.get("attempt_binding_digest")
                != normalized.get("binding_digest")
                or proof.get("backend_digest") != normalized.get("backend_digest")
            ):
                raise ExecutionFenceUnavailable("proof_track_invalid")
        if hosted:
            sandbox = (control_plane or {}).get("provider_sandbox")
            sandbox_proof = proofs.get("provider_sandbox") or {}
            if (
                not isinstance(sandbox, dict)
                or digest_json(sandbox.get("profile"))
                != sandbox.get("profile_digest")
                or sandbox_proof.get("profile_digest")
                != sandbox.get("profile_digest")
                or sandbox_proof.get("enforced_by")
                != PROVIDER_SANDBOX_ENFORCED_BY
            ):
                raise ExecutionFenceUnavailable(
                    "provider_sandbox_profile_digest_invalid"
                )
        if normalized.get("proofs_digest") != digest_json(proofs):
            raise ExecutionFenceUnavailable("proofs_digest_invalid")
        backend = normalized.get("backend")
        if (
            not isinstance(backend, dict)
            or backend.get("backend_id") != BACKEND_ID
            or backend.get("backend_version") != self.bubblewrap_version
            or backend.get("bubblewrap_path") != self.bubblewrap_path
            or normalized.get("backend_digest") != digest_json(backend)
        ):
            raise ExecutionFenceUnavailable("backend_binding_invalid")
        if float(binding["expires_at"]) <= float(self.clock()):
            raise ExecutionFenceUnavailable("descriptor_expired")
        return normalized

    def _seccomp_fd(self) -> int:
        return self._seccomp_program_fd(DENIED_SYSCALLS)

    def _provider_seccomp_program(
        self, seccomp_policy: Mapping[str, Any]
    ) -> bytes:
        """Compile the descriptor-signed provider syscall table to BPF bytes.

        The provider terminal is a detached shell string, so the program
        travels as a file plus an fd redirect instead of an inherited fd."""
        fd = self._seccomp_program_fd(
            [str(item) for item in seccomp_policy.get("denied_syscalls") or []]
        )
        try:
            chunks: list[bytes] = []
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
        finally:
            os.close(fd)

    def _seccomp_program_fd(self, denied_syscalls: Sequence[str]) -> int:
        library = ctypes.CDLL(self.seccomp_library, use_errno=True)
        library.seccomp_init.argtypes = [ctypes.c_uint32]
        library.seccomp_init.restype = ctypes.c_void_p
        library.seccomp_release.argtypes = [ctypes.c_void_p]
        library.seccomp_rule_add.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.c_uint,
        ]
        library.seccomp_rule_add.restype = ctypes.c_int
        library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
        library.seccomp_syscall_resolve_name.restype = ctypes.c_int
        library.seccomp_export_bpf.argtypes = [ctypes.c_void_p, ctypes.c_int]
        library.seccomp_export_bpf.restype = ctypes.c_int
        allow = 0x7FFF0000
        deny = 0x00050000 | errno.EPERM
        context = library.seccomp_init(allow)
        if not context:
            raise ExecutionFenceUnavailable("seccomp_init_failed")
        fd = -1
        try:
            for name in denied_syscalls:
                syscall = library.seccomp_syscall_resolve_name(name.encode())
                if syscall < 0:
                    continue
                result = library.seccomp_rule_add(context, deny, syscall, 0)
                if result != 0:
                    raise ExecutionFenceUnavailable(
                        f"seccomp_rule_failed:{name}"
                    )
            fd = os.memfd_create("lh-execution-fence-seccomp", flags=0)
            if library.seccomp_export_bpf(context, fd) != 0:
                raise ExecutionFenceUnavailable("seccomp_export_failed")
            os.lseek(fd, 0, os.SEEK_SET)
            return fd
        except Exception:
            if fd >= 0:
                os.close(fd)
            raise
        finally:
            library.seccomp_release(context)

    @staticmethod
    def _sandbox_executable(
        argv: Sequence[str],
        clone_root: Path,
    ) -> tuple[list[str], list[str]]:
        if not argv or not isinstance(argv[0], str) or not argv[0]:
            raise ExecutionFenceUnavailable("adapter_argv_invalid")
        executable = Path(argv[0])
        if not executable.is_absolute():
            raise ExecutionFenceUnavailable("adapter_executable_not_absolute")
        try:
            resolved = executable.resolve(strict=True)
        except OSError as exc:
            raise ExecutionFenceUnavailable("adapter_executable_missing") from exc
        mounts: list[str] = []
        try:
            relative = resolved.relative_to(clone_root)
        except ValueError:
            relative = None
        if relative is not None:
            sandbox_executable = "/workspace/" + relative.as_posix()
        elif any(
            resolved == root or root in resolved.parents
            for root in (
                Path("/usr"),
                Path("/bin"),
                Path("/lib"),
                Path("/lib64"),
            )
        ):
            sandbox_executable = str(resolved)
        else:
            sandbox_executable = "/adapter/entry"
            mounts = [
                "--dir",
                "/adapter",
                "--ro-bind",
                str(resolved),
                sandbox_executable,
            ]
        return [sandbox_executable, *[str(item) for item in argv[1:]]], mounts

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
        self._validate_backend()
        normalized = self._validate_descriptor(
            descriptor,
            require_prepared=True,
        )
        classes = normalized.get("launch_classes") or {"mutation": 1}
        if int(classes.get("mutation", 0)) < 1:
            raise ExecutionFenceUnavailable("launch_class_not_authorized")
        if timeout_seconds <= 0:
            raise ExecutionFenceUnavailable("launch_timeout_invalid")
        if env_projection:
            allowed_env = {"LANG", "LC_ALL"}
            if normalized["binding"]["schema"] == BINDING_SCHEMA_V2:
                exact_values = {"LH_HOST_STATE_ROOT": "/tmp/host-state", "LH_HOST_TMP_ROOT": "/tmp/host-tmp",
                                "TMPDIR": "/tmp", "PYTHONDONTWRITEBYTECODE": "1"}
                for name, expected in exact_values.items():
                    if name in env_projection and env_projection[name] != expected:
                        raise ExecutionFenceUnavailable("environment_projection_invalid")
                if "LH_ROOT" in env_projection and not (
                    env_projection["LH_ROOT"] == "/workspace"
                    or env_projection["LH_ROOT"].startswith("/workspace/")):
                    raise ExecutionFenceUnavailable("environment_projection_invalid")
                allowed_env |= set(exact_values) | {"LH_ROOT"}
            if set(env_projection) - allowed_env:
                raise ExecutionFenceUnavailable("environment_projection_invalid")
        if on_started is not None and not callable(on_started):
            raise ExecutionFenceUnavailable("started_notification_invalid")
        binding = normalized["binding"]
        clone_root = Path(binding["clone_root"])
        if "null_device_check" in binding and (
            list(argv) != binding["null_device_check"]["argv"] or input_text is not None
        ):
            raise ExecutionFenceUnavailable("null_device_check_launch_mismatch")
        sandbox_argv, adapter_mounts = self._sandbox_executable(argv, clone_root)
        if binding["schema"] == BINDING_SCHEMA_V2:
            mappings = self._path_mappings(binding)
            for index, raw in enumerate(sandbox_argv[1:], start=1):
                for source, target in sorted(mappings, key=lambda pair: -len(pair[0])):
                    if raw == source or raw.startswith(source + "/"):
                        sandbox_argv[index] = target + raw[len(source):]
                        break
        seccomp_fd = self._seccomp_fd()
        digest = normalized["launch_descriptor_digest"]
        command = [
            self.bubblewrap_path,
            "--unshare-all",
            "--unshare-user",
            "--disable-userns",
            "--die-with-parent",
            "--new-session",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--setenv",
            "PATH",
            "/usr/bin:/bin",
            "--setenv",
            "HOME",
            "/nonexistent",
            "--setenv",
            "TMPDIR",
            "/tmp",
            "--setenv",
            "LH_PROVIDER_CONTROL_CHANNEL",
            "stdio",
            "--setenv",
            "LH_EXECUTION_FENCE_DESCRIPTOR_DIGEST",
            digest,
            "--ro-bind",
            "/usr",
            "/usr",
            "--ro-bind-try",
            "/bin",
            "/bin",
            "--ro-bind-try",
            "/lib",
            "/lib",
            "--ro-bind-try",
            "/lib64",
            "/lib64",
            "--dir",
            "/etc",
            "--ro-bind-try",
            "/etc/ld.so.cache",
            "/etc/ld.so.cache",
            "--proc",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--dir",
            "/workspace",
            "--bind" if str(clone_root) in binding["allowed_write_roots"] else "--ro-bind",
            str(clone_root),
            "/workspace",
            "--chdir",
            "/workspace",
            *adapter_mounts,
            "--seccomp",
            str(seccomp_fd),
        ]
        if binding["schema"] == BINDING_SCHEMA_V2:
            if "scratch_write" in binding["allowed_local_effects"]:
                command.extend(["--dir", "/tmp/host-state", "--dir", "/tmp/host-tmp"])
            for source, target in self._path_mappings(binding)[1:]:
                command.extend(["--ro-bind", source, target])
            if "scratch_write" not in binding["allowed_local_effects"]:
                command.extend(["--remount-ro", "/tmp"])
        if "null_device_check" in binding:
            command.extend(["--dir", "/dev", "--dev-bind", "/dev/null", "/dev/null"])
        for name, value in sorted((env_projection or {}).items()):
            command.extend(["--setenv", name, value])
        command.extend(["--", *sandbox_argv])
        self._consumed.add(digest)
        self.launch_count += 1
        try:
            process = subprocess.Popen(command, stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                pass_fds=(seccomp_fd,))
            try:
                if on_started is not None:
                    on_started(process)
                stdout, stderr = process.communicate(input=input_text, timeout=timeout_seconds)
            except BaseException:
                try:
                    process.kill()
                    process.communicate(timeout=5)
                except (OSError, subprocess.SubprocessError):
                    raise ExecutionFenceUnavailable("started_child_termination_unknown")
                raise
            return subprocess.CompletedProcess(list(argv), process.returncode, stdout, stderr)
        except subprocess.TimeoutExpired:
            raise
        except OSError as exc:
            raise ExecutionFenceUnavailable("backend_launch_failed") from exc
        finally:
            os.close(seccomp_fd)

    def launch_control(
        self,
        descriptor: Mapping[str, Any],
        request: Mapping[str, Any],
        *,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        """One structured Orca control-plane RPC, outside the sandbox.

        orca.exe cannot start under ``--unshare-all`` (Windows interop plus a
        daemon socket), so a control launch runs on the host -- but only a
        closed request schema, against binaries pinned by digest at
        ``prepare()``, within the descriptor-signed budget, audited per call."""
        self._validate_backend()
        normalized = self._validate_descriptor(
            descriptor,
            require_prepared=True,
        )
        if timeout_seconds <= 0:
            raise ExecutionFenceUnavailable("launch_timeout_invalid")
        classes = normalized.get("launch_classes") or {}
        budget = int(classes.get("control", 0))
        if budget < 1:
            raise ExecutionFenceUnavailable("launch_class_not_authorized")
        digest = normalized["launch_descriptor_digest"]
        used = self._control_counts.get(digest, 0)
        if used >= budget:
            raise ExecutionFenceUnavailable("descriptor_exhausted")
        plane = normalized.get("control_plane") or {}
        orca = plane.get("orca_cli") or {}
        orca_path = orca.get("path")
        if _sha256_file(orca_path) != orca.get("sha256"):
            raise ExecutionFenceUnavailable("control_orca_cli_drifted")
        requested_cli = request.get("orca_cli")
        if requested_cli is not None and str(requested_cli) != str(orca_path):
            raise ExecutionFenceUnavailable("control_orca_cli_mismatch")
        binding = normalized["binding"]
        clone_root = str(binding["clone_root"])
        op = request.get("op")
        if op not in CONTROL_OPS:
            raise ExecutionFenceUnavailable("control_op_unknown")
        audit_extra: dict[str, Any] = {}
        if op == "terminal_create":
            provider = plane.get("provider") or {}
            provider_argv = [str(item) for item in request.get("provider_argv") or []]
            if not provider_argv:
                raise ExecutionFenceUnavailable("control_provider_argv_empty")
            if provider_argv[0] != provider.get("path") or _sha256_file(
                provider_argv[0]
            ) != provider.get("sha256"):
                raise ExecutionFenceUnavailable("control_provider_binary_unpinned")
            egress_policy = plane.get("egress_policy")
            if not isinstance(egress_policy, dict):
                raise ExecutionFenceUnavailable("egress_policy_unbound")
            _validate_provider_argv_against_policy(provider_argv, egress_policy)
            if not _selector_matches_clone(
                str(request.get("worktree_selector")), clone_root
            ):
                raise ExecutionFenceUnavailable("control_selector_invalid")
            output_path = request.get("output_path")
            if output_path is not None and not str(output_path).startswith(
                clone_root + os.sep
            ):
                raise ExecutionFenceUnavailable("control_output_path_invalid")
            # host-bline-provider-sandbox: the profile signed at prepare() is
            # the only runtime basis -- the policy artefact is deliberately
            # not re-read here (codex amend 4d).
            sandbox = plane.get("provider_sandbox")
            if not isinstance(sandbox, dict) or not isinstance(
                sandbox.get("profile"), dict
            ):
                raise ExecutionFenceUnavailable("provider_sandbox_unbound")
            profile = sandbox["profile"]
            declared_runtime = provider.get("runtime")
            _validate_provider_runtime_binding(
                provider_argv[0], profile, declared_runtime
            )
            if digest_json(profile) != sandbox.get("profile_digest"):
                raise ExecutionFenceUnavailable(
                    "provider_sandbox_profile_digest_invalid"
                )
            pinned_bwrap = profile.get("bubblewrap") or {}
            if _sha256_file(pinned_bwrap.get("path")) != pinned_bwrap.get(
                "sha256"
            ):
                raise ExecutionFenceUnavailable("provider_sandbox_bwrap_drifted")
            program = self._provider_seccomp_program(profile.get("seccomp") or {})
            program_path = self._write_provider_seccomp_program(program, digest)
            request = {
                **request,
                "provider_sandbox_profile": profile,
                "provider_seccomp_program_path": program_path,
                "clone_root": clone_root,
            }
            expected_command = compose_terminal_command(
                provider_argv,
                output_path=output_path,
                env_overlay=request.get("env_overlay"),
                sandbox=profile,
                seccomp_program_path=program_path,
                clone_root=clone_root,
            )
            audit_extra = {
                "command_digest": "sha256:"
                + hashlib.sha256(expected_command.encode()).hexdigest(),
                "provider_sandbox_profile_digest": sandbox["profile_digest"],
                "seccomp_program_sha256": "sha256:"
                + hashlib.sha256(program).hexdigest(),
                "seccomp_program_path": program_path,
                "seccomp_program_scope": "task_temp_outside_clone",
            }
            if isinstance(declared_runtime, dict):
                audit_extra["provider_runtime"] = dict(declared_runtime)
        elif op == "repo_add" and str(request.get("path")) != clone_root:
            raise ExecutionFenceUnavailable("control_repo_path_invalid")
        elif op == "terminal_stop" and not _selector_matches_clone(
            str(request.get("worktree_selector")), clone_root
        ):
            raise ExecutionFenceUnavailable("control_selector_invalid")
        argv = [str(orca_path), *compose_control_argv(request)]
        if op == "terminal_create":
            sent_command = argv[argv.index("--command") + 1]
            if (
                "sha256:" + hashlib.sha256(sent_command.encode()).hexdigest()
                != audit_extra["command_digest"]
            ):
                raise ExecutionFenceUnavailable("control_command_digest_mismatch")
        self._control_counts[digest] = used + 1
        self.control_audit.setdefault(digest, []).append(
            {
                "op": op,
                "argv_digest": digest_json(argv),
                "sequence": used + 1,
                "at": self.clock(),
                **audit_extra,
            }
        )
        try:
            return subprocess.run(
                argv,
                cwd=clone_root,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise
        except OSError as exc:
            raise ExecutionFenceUnavailable("control_launch_failed") from exc
        finally:
            if op == "terminal_close":
                self._cleanup_provider_seccomp(digest)

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        normalized = self._validate_descriptor(
            descriptor,
            require_prepared=True,
        )
        return {
            "schema": DESCRIPTOR_SCHEMA,
            "status": "admitted",
            "launch_descriptor_digest": normalized["launch_descriptor_digest"],
            "binding_digest": normalized["binding_digest"],
            "backend": {
                "backend_id": normalized["backend"]["backend_id"],
                "backend_version": normalized["backend"]["backend_version"],
                "mount_policy_digest": normalized["backend"][
                    "mount_policy_digest"
                ],
                "namespace_policy_digest": normalized["backend"][
                    "namespace_policy_digest"
                ],
                "syscall_policy_digest": normalized["backend"][
                    "syscall_policy_digest"
                ],
            },
            "proofs": normalized["proofs"],
            "proofs_digest": normalized["proofs_digest"],
            "provider_control_channel": normalized["binding"][
                "provider_control_channel"
            ],
            "launch_classes": normalized.get("launch_classes")
            or {"control": 0, "mutation": 1},
            "control_plane": (
                {
                    "egress_policy": (normalized.get("control_plane") or {}).get("egress_policy"),
                    "orca_cli_sha256": ((normalized.get("control_plane") or {}).get("orca_cli") or {}).get("sha256"),
                    "provider_sha256": ((normalized.get("control_plane") or {}).get("provider") or {}).get("sha256"),
                    "provider_runtime": ((normalized.get("control_plane") or {}).get("provider") or {}).get("runtime"),
                    # Digest and enforcer only: the profile enumerates trust-
                    # surface paths, and a receipt must not restate the bind
                    # set contents (packet §7: ~/.codex scope named, not leaked).
                    "provider_sandbox": (
                        {
                            "profile_digest": ((normalized.get("control_plane") or {}).get("provider_sandbox") or {}).get("profile_digest"),
                            "enforced_by": PROVIDER_SANDBOX_ENFORCED_BY,
                        }
                        if isinstance((normalized.get("control_plane") or {}).get("provider_sandbox"), dict)
                        else None
                    ),
                }
                if isinstance(normalized.get("control_plane"), dict)
                else None
            ),
            "control_launches": list(
                self.control_audit.get(normalized["launch_descriptor_digest"], ())
            ),
            "mutation_dispatch": normalized["mutation_dispatch"],
        }

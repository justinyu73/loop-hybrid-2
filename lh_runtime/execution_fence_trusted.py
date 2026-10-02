"""Explicit project trust through the existing fence port, without kernel proof.

The controller and same-user project processes are trusted. Descriptor and
budget checks prevent accidental replay; they are not an adversarial host
security boundary. Credentials are opaque operator-owned locators.
"""
from __future__ import annotations

import copy
import hashlib
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import threading
import time
from typing import Any, Mapping

if __package__:
    from .execution_fence import ExecutionFencePort, ExecutionFenceUnavailable, digest_json, validate_phase_roots
    from .platform_ports import ManagedProcessResult, ManagedProcessTimeout, ManagedProcessUnknown
else:
    from execution_fence import ExecutionFencePort, ExecutionFenceUnavailable, digest_json, validate_phase_roots
    from platform_ports import ManagedProcessResult, ManagedProcessTimeout, ManagedProcessUnknown

POLICY_SCHEMA = "lh-trusted-project-policy/v1"
OPERATOR_SCHEMA = "lh-trusted-project-operator-binding/v1"
BACKEND_ID = "trusted-project-local"
ADAPTER_ID = "codex-exec-jsonl-v1"
POLICY_FIELDS = {"schema", "mode", "project_id", "goal_id", "goal_revision",
    "operator_binding_ref", "max_cli_launches", "max_wall_seconds", "max_observed_tokens",
    "unknown_usage", "expires_at"}
PROVIDER_FIELDS = {"provider_id", "adapter_id", "executable", "executable_digest", "model",
    "endpoint", "auth_locator", "tool_path"}
CONTEXT_FIELDS = {"schema", "project_id", "goal_id", "goal_revision", "execution_binding_digest",
    "policy_digest", "operator_binding_digest", "phase", "provider_role", "budget_reservation_key",
    "deadline_at", "command_digest", "input_digest", "environment_digest", "output_schema_digest"}
ASSURANCE_FIELDS = {"schema", "mode", "kernel_containment", "provider_egress_enforced",
    "policy_digest", "execution_binding_digest", "launch_descriptor_digest", "command_digest",
    "process_identity_digest", "lifecycle_outcome"}


def _reject(reason):
    raise ExecutionFenceUnavailable(reason)


def _shape(value, fields, reason):
    if not isinstance(value, Mapping) or set(value) != fields:
        _reject(reason)


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and not any(c in value for c in "\r\n\x00")


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def _positive(value, integer=False):
    return (not isinstance(value, bool) and isinstance(value, int if integer else (int, float))
            and math.isfinite(value) and value > 0)


def _path(value, *, directory=False):
    if not _text(value):
        _reject("trusted_path_invalid")
    path = Path(value)
    try:
        valid = (path.is_absolute() and str(path.resolve(strict=True)) == value
                 and path != Path(path.anchor)
                 and (path.is_dir() if directory else path.is_file()))
    except OSError:
        valid = False
    if not valid:
        _reject("trusted_path_invalid")
    return path


def validate_trusted_project_policy(raw: Mapping[str, Any], *, now=None):
    _shape(raw, POLICY_FIELDS, "trusted_policy_fields_invalid")
    if (raw["schema"] != POLICY_SCHEMA or raw["mode"] != "trusted-project"
            or raw["unknown_usage"] != "stop"
            or not all(_text(raw[key]) for key in ("project_id", "goal_id"))
            or not all(_positive(raw[key], True) for key in ("goal_revision", "max_cli_launches"))
            or not all(_positive(raw[key]) for key in ("max_wall_seconds", "expires_at"))
            or (raw["max_observed_tokens"] is not None and not _positive(raw["max_observed_tokens"], True))):
        _reject("trusted_policy_invalid")
    if raw["expires_at"] <= (time.time() if now is None else now):
        _reject("trusted_policy_expired")
    ref = raw["operator_binding_ref"]
    _shape(ref, {"path", "digest"}, "trusted_operator_reference_invalid")
    _path(ref["path"])
    if not _digest(ref["digest"]):
        _reject("trusted_operator_reference_invalid")
    return copy.deepcopy(dict(raw))


def validate_trusted_operator_binding(raw):
    _shape(raw, {"schema", "project_id", "providers"}, "trusted_operator_fields_invalid")
    if raw["schema"] != OPERATOR_SCHEMA or not _text(raw["project_id"]):
        _reject("trusted_operator_invalid")
    _shape(raw["providers"], {"coding", "verifier"}, "trusted_operator_roles_invalid")
    for provider in raw["providers"].values():
        _shape(provider, PROVIDER_FIELDS, "trusted_provider_fields_invalid")
        if (provider["adapter_id"] != ADAPTER_ID or not _text(provider["provider_id"])
                or not _text(provider["model"]) or any(c.isspace() for c in provider["model"])):
            _reject("trusted_provider_invalid")
        executable = _path(provider["executable"])
        if (not _digest(provider["executable_digest"]) or not os.access(executable, os.X_OK)
                or "sha256:" + hashlib.sha256(executable.read_bytes()).hexdigest() != provider["executable_digest"]):
            _reject("trusted_executable_mismatch")
        if provider["endpoint"] != {"kind": "codex_chatgpt", "base_url": None}:
            _reject("trusted_endpoint_unsupported")
        _shape(provider["auth_locator"], {"kind", "path"}, "trusted_auth_locator_invalid")
        if provider["auth_locator"]["kind"] != "codex_home":
            _reject("trusted_auth_locator_invalid")
        _path(provider["auth_locator"]["path"], directory=True)
        paths = provider["tool_path"]
        if not isinstance(paths, list) or not paths:
            _reject("trusted_tool_path_invalid")
        for path in paths:
            _path(path, directory=True)
    return copy.deepcopy(dict(raw))


def validate_trusted_assurance(raw):
    _shape(raw, ASSURANCE_FIELDS, "trusted_assurance_fields_invalid")
    if (raw["schema"] != "lh-execution-assurance/v1" or raw["mode"] != "trusted-project"
            or raw["kernel_containment"] is not False or raw["provider_egress_enforced"] is not False
            or raw["lifecycle_outcome"] not in {"exited", "timed_out", "unknown"}
            or not all(_digest(raw[key]) for key in ASSURANCE_FIELDS if key.endswith("_digest"))):
        _reject("trusted_assurance_invalid")
    return copy.deepcopy(dict(raw))


def run_trusted_managed_process(argv, *, cwd, input_text, env, deadline_at,
                        max_output_bytes=1048576, on_started=None, identity_port=None):
    """Bound one owned process group, without claiming an adversarial sandbox.

    Only an explicitly supplied environment crosses this boundary. Streams stay
    bounded in memory; exceptions contain closed codes, never argv or output.
    """
    if os.name == "nt":
        if __package__:
            from .execution_fence_trusted_windows import run_trusted_managed_process as run_windows
        else:
            from execution_fence_trusted_windows import run_trusted_managed_process as run_windows
        return run_windows(argv, cwd=cwd, input_text=input_text, env=env,
            deadline_at=deadline_at, max_output_bytes=max_output_bytes,
            on_started=on_started, identity_port=identity_port)
    try:
        from .lifecycle import NativeProcessIdentityPort, ProcessIdentity, observe_process_identity
    except ImportError:
        from lifecycle import NativeProcessIdentityPort, ProcessIdentity, observe_process_identity
    started = time.time()
    monotonic_started = time.monotonic()
    lifecycle = {"schema": "lh-managed-process-lifecycle/v1", "process_identity": None,
        "process_group_id": None, "started_at": started, "ended_at": started,
        "deadline_at": deadline_at, "elapsed_seconds": 0.0, "timed_out": False,
        "termination_attempted": False, "termination_confirmed": False,
        "termination_scope": "owned_process_group", "outcome": "unknown"}
    def finish(outcome):
        lifecycle.update(outcome=outcome, ended_at=time.time(),
                         elapsed_seconds=time.monotonic() - monotonic_started)
        return dict(lifecycle)
    if (os.name != "posix" or not all(callable(getattr(os, name, None))
            for name in ("getpgid", "killpg", "setsid"))):
        raise ManagedProcessUnknown("process_group_capability_unavailable", finish("unknown"))
    if (isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float))
            or not math.isfinite(deadline_at) or isinstance(max_output_bytes, bool)
            or not isinstance(max_output_bytes, int) or max_output_bytes < 1
            or isinstance(argv, (str, bytes)) or not argv
            or not all(isinstance(word, str) and word and "\x00" not in word for word in argv)
            or not isinstance(env, Mapping)
            or any(not isinstance(key, str) or not key or "=" in key or "\x00" in key
                   or not isinstance(value, str) or "\x00" in value for key, value in env.items())
            or (input_text is not None and not isinstance(input_text, str))
            or (on_started is not None and not callable(on_started))):
        raise ManagedProcessUnknown("managed_process_input_invalid", finish("unknown"))
    try:
        input_bytes = (input_text or "").encode("utf-8")
    except UnicodeError:
        raise ManagedProcessUnknown("managed_process_input_invalid", finish("unknown")) from None
    if deadline_at <= started:
        lifecycle.update(timed_out=True, termination_confirmed=True)
        raise ManagedProcessTimeout(tuple(argv), finish("timed_out"))
    identity_port = identity_port or NativeProcessIdentityPort()
    try:
        if ProcessIdentity.from_dict(identity_port.current().as_dict()) is None:
            raise ValueError("identity unavailable")
    except (OSError, ValueError, RuntimeError, AttributeError):
        raise ManagedProcessUnknown("process_identity_unknown", finish("unknown")) from None
    process = None
    identity = None
    owned_identity = None
    def group_present():
        try:
            os.killpg(process.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            return None
    def terminate_group():
        lifecycle["termination_attempted"] = True
        # A reused leader PID is never an authority to signal its new group.
        try:
            for action in (signal.SIGTERM, signal.SIGKILL):
                if group_present() is False:
                    break
                # Re-check birth identity before EACH signal. A missing read is
                # not death. Only our unreused group may outlive its leader.
                if owned_identity is None:
                    return False
                observed = observe_process_identity(owned_identity, identity_port=identity_port)
                if observed.status == "unknown" or (
                    observed.identity is not None and not owned_identity.matches(observed.identity)):
                    return False
                if observed.status == "alive":
                    if os.getpgid(process.pid) != process.pid:
                        return False
                elif observed.reason != "process_absent" or process.poll() is None:
                    return False
                os.killpg(process.pid, action)
                try:
                    process.wait(timeout=0.35)
                except subprocess.TimeoutExpired:
                    pass
                until = time.monotonic() + 0.15
                while group_present() is True and time.monotonic() < until:
                    time.sleep(0.01)
            process.wait(timeout=0.35)
            return group_present() is False
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
            return False
    selector = selectors.DefaultSelector()
    output = {"stdout": bytearray(), "stderr": bytearray()}
    failure = None
    try:
        process = subprocess.Popen(list(argv), cwd=str(cwd), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(env), shell=False,
            close_fds=True, start_new_session=True, bufsize=0)
        identity = identity_port.observe(process.pid)
        owned_identity = identity
        if identity is None or identity.pid != process.pid or os.getpgid(process.pid) != process.pid:
            failure = "process_identity_unknown"
            raise RuntimeError(failure)
        lifecycle.update(process_identity=identity.as_dict(), process_group_id=process.pid)
        if on_started is not None:
            try:
                on_started(process)
            except BaseException:
                failure = "callback_failed"
                raise
        pending = memoryview(input_bytes)
        for name in ("stdout", "stderr"):
            pipe = getattr(process, name)
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
        if pending:
            os.set_blocking(process.stdin.fileno(), False)
            selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
        else:
            process.stdin.close()
        monotonic_deadline = monotonic_started + (deadline_at - started)
        while selector.get_map() or process.poll() is None:
            left = monotonic_deadline - time.monotonic()
            if left <= 0:
                failure = "process_timeout"
                lifecycle["timed_out"] = True
                break
            for key, _ in selector.select(min(left, 0.05)):
                if key.data == "stdin":
                    try:
                        written = os.write(key.fd, pending[:65536])
                        pending = pending[written:]
                    except BrokenPipeError:
                        pending = memoryview(b"")
                    if not pending:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                else:
                    data = os.read(key.fd, 65536)
                    if not data:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                    elif sum(map(len, output.values())) + len(data) > max_output_bytes:
                        failure = "output_limit_exceeded"
                        break
                    else:
                        output[key.data].extend(data)
            if failure:
                break
        if failure is None and group_present() is not False:
            failure = "process_group_not_quiescent"
        if failure:
            lifecycle["termination_confirmed"] = terminate_group()
            if failure == "process_timeout":
                raise ManagedProcessTimeout(tuple(argv), finish("timed_out"))
            raise ManagedProcessUnknown(failure, finish("unknown"))
        lifecycle["termination_confirmed"] = True
        return ManagedProcessResult(list(argv), process.returncode,
            output["stdout"].decode("utf-8", errors="strict"),
            output["stderr"].decode("utf-8", errors="strict"), finish("exited"))
    except (ManagedProcessTimeout, ManagedProcessUnknown):
        raise
    except BaseException:
        if process is not None:
            lifecycle["termination_confirmed"] = terminate_group()
        raise ManagedProcessUnknown(failure or "process_readback_unknown", finish("unknown")) from None
    finally:
        selector.close()
        if process is not None:
            for name in ("stdin", "stdout", "stderr"):
                pipe = getattr(process, name, None)
                if pipe is not None:
                    pipe.close()



class TrustedProjectExecutionFence(ExecutionFencePort):
    supports_started_notification = True

    def __init__(self, *, policy, operator_binding, identity_port=None):
        self.policy = validate_trusted_project_policy(policy)
        self.operator = validate_trusted_operator_binding(operator_binding)
        if self.policy["project_id"] != self.operator["project_id"]:
            _reject("trusted_project_mismatch")
        self.identity_port = identity_port
        self._prepared, self._finished = {}, {}
        self._lock = threading.Lock()

    def environment(self, role, scratch, supplied):
        provider = self.operator["providers"][role or "coding"]
        result = {"PATH": os.pathsep.join(provider["tool_path"]), "HOME": str(scratch),
            "TMPDIR": str(scratch), "PYTHONDONTWRITEBYTECODE": "1",
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0"}
        for key in ("LANG", "LC_ALL"):
            if key in supplied:
                result[key] = supplied[key]
        if role is not None:
            result["CODEX_HOME"] = provider["auth_locator"]["path"]
        else:
            for key in ("LH_HOST_TMP_ROOT", "LH_HOST_STATE_ROOT", "LH_STATE_ROOT", "GIT_OPTIONAL_LOCKS"):
                if key in supplied:
                    result[key] = supplied[key]
        return result

    def prepare(self, binding):
        if not isinstance(binding, Mapping) or "trusted_context" not in binding:
            _reject("trusted_context_missing")
        expected = {"schema", "goal_revision", "run_id", "attempt", "attempt_fence", "base_revision",
            "clone_root", "adapter_id", "adapter_version", "allowed_write_roots", "allowed_local_effects",
            "verification_commands", "provider_control_channel", "controller_nonce", "created_at",
            "expires_at", "idempotency_key", "allowed_read_roots", "execution_context_digest", "trusted_context"}
        _shape(binding, expected, "trusted_binding_fields_invalid")
        context = binding["trusted_context"]
        _shape(context, CONTEXT_FIELDS, "trusted_context_fields_invalid")
        if (binding["schema"] != "lh-trusted-project-binding/v1"
                or context["schema"] != "lh-trusted-project-context/v1"
                or binding["execution_context_digest"] != digest_json(context)
                or context["policy_digest"] != digest_json(self.policy)
                or context["operator_binding_digest"] != digest_json(self.operator)
                or any(context[key] != self.policy[key] for key in ("project_id", "goal_id", "goal_revision"))
                or binding["goal_revision"] != digest_json({"goal_id": context["goal_id"], "goal_revision": context["goal_revision"]})
                or context["provider_role"] not in {None, "coding", "verifier"}
                or not _text(context["phase"]) or not _text(binding["controller_nonce"])
                or not _text(binding["run_id"]) or not _text(binding["idempotency_key"])
                or not _positive(binding["attempt"], True) or not _positive(binding["attempt_fence"], True)
                or not re.fullmatch(r"[0-9a-f]{40,64}", str(binding["base_revision"]))
                or not all(_digest(context[k]) for k in CONTEXT_FIELDS
                           if k.endswith("_digest") and context[k] is not None)
                or not _digest(context["budget_reservation_key"])):
            _reject("trusted_binding_mismatch")
        if (not _positive(binding["expires_at"]) or not _positive(context["deadline_at"])
                or binding["expires_at"] > min(context["deadline_at"], self.policy["expires_at"])
                or binding["expires_at"] <= time.time()):
            _reject("trusted_binding_expired")
        _path(binding["clone_root"], directory=True)
        validate_phase_roots(binding)
        commands = binding["verification_commands"]
        if (not isinstance(commands, list) or len(commands) != 1 or not isinstance(commands[0], list)
                or not commands[0] or any(not _text(word) for word in commands[0])
                or digest_json(commands[0]) != context["command_digest"]):
            _reject("trusted_command_invalid")
        executable = _path(commands[0][0])
        validate_trusted_operator_binding(self.operator)
        body = {"schema": "lh-trusted-project-launch/v1", "backend_id": BACKEND_ID,
            "backend_version": "1", "binding": copy.deepcopy(dict(binding)),
            "binding_digest": digest_json(binding),
            "executable_digest": "sha256:" + hashlib.sha256(executable.read_bytes()).hexdigest()}
        descriptor = {**body, "launch_descriptor_digest": digest_json(body)}
        with self._lock:
            self._prepared[descriptor["launch_descriptor_digest"]] = copy.deepcopy(descriptor)
        return descriptor

    def launch(self, descriptor, argv, *, input_text=None, timeout_seconds,
               env_projection=None, on_started=None):
        key = descriptor.get("launch_descriptor_digest") if isinstance(descriptor, Mapping) else None
        with self._lock:
            accepted = self._prepared.get(key)
            if accepted is None or accepted != descriptor:
                _reject("trusted_descriptor_not_prepared")
            binding = accepted["binding"]
            context = binding["trusted_context"]
            if (binding["expires_at"] <= time.time() or not _positive(timeout_seconds)
                    or isinstance(argv, (str, bytes)) or not isinstance(argv, (tuple, list))
                    or (input_text is not None and not isinstance(input_text, str))
                    or not isinstance(env_projection, Mapping)
                    or digest_json(list(argv)) != context["command_digest"]
                    or "sha256:" + hashlib.sha256((input_text or "").encode()).hexdigest() != context["input_digest"]
                    or digest_json(dict(env_projection or {})) != context["environment_digest"]):
                _reject("trusted_launch_input_mismatch")
            executable = _path(argv[0])
            if "sha256:" + hashlib.sha256(executable.read_bytes()).hexdigest() != accepted["executable_digest"]:
                _reject("trusted_executable_mismatch")
            validate_trusted_operator_binding(self.operator)
            if context["output_schema_digest"] is not None:
                if argv.count("--output-schema") != 1:
                    _reject("trusted_output_schema_mismatch")
                schema_path = _path(argv[argv.index("--output-schema") + 1])
                if "sha256:" + hashlib.sha256(schema_path.read_bytes()).hexdigest() != context["output_schema_digest"]:
                    _reject("trusted_output_schema_mismatch")
            del self._prepared[key]
        try:
            result = run_trusted_managed_process(argv, cwd=binding["clone_root"], input_text=input_text,
                env=env_projection or {}, deadline_at=min(binding["expires_at"], time.time() + timeout_seconds),
                on_started=on_started, identity_port=self.identity_port)
        except BaseException as exc:
            lifecycle = getattr(exc, "process_lifecycle", None)
            if lifecycle is not None:
                self._finished[key] = (copy.deepcopy(accepted), lifecycle)
            raise
        self._finished[key] = (copy.deepcopy(accepted), result.process_lifecycle)
        return result

    def receipt_projection(self, descriptor):
        key = descriptor.get("launch_descriptor_digest")
        saved = self._finished.get(key)
        if saved is None or saved[0] != descriptor:
            _reject("trusted_lifecycle_not_observed")
        context, lifecycle = descriptor["binding"]["trusted_context"], saved[1]
        return validate_trusted_assurance({"schema": "lh-execution-assurance/v1", "mode": "trusted-project",
            "kernel_containment": False, "provider_egress_enforced": False,
            "policy_digest": context["policy_digest"], "execution_binding_digest": context["execution_binding_digest"],
            "launch_descriptor_digest": key, "command_digest": context["command_digest"],
            "process_identity_digest": digest_json(lifecycle["process_identity"]), "lifecycle_outcome": lifecycle["outcome"]})

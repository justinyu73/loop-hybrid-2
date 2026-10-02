"""Owned Windows process jobs for the explicit trusted-project policy.

Job ownership is lifecycle control, not adversarial kernel containment.
Only the explicitly supplied environment and three stdio handles cross launch.
"""
from __future__ import annotations

import math
import os
import subprocess
import sys
import threading
import time
from typing import Mapping

if __package__:
    from .lifecycle import NativeProcessIdentityPort, ProcessIdentity, observe_process_identity
    from .platform_ports import ManagedProcessResult, ManagedProcessTimeout, ManagedProcessUnknown
else:
    from lifecycle import NativeProcessIdentityPort, ProcessIdentity, observe_process_identity
    from platform_ports import ManagedProcessResult, ManagedProcessTimeout, ManagedProcessUnknown


class _NativeFailure(RuntimeError):
    """Closed reason only: never expose command, environment or output."""


class _WindowsAPI:
    def __init__(self):
        import ctypes as c
        from ctypes import wintypes as w

        self.c, self.w = c, w
        self.kernel = c.WinDLL("kernel32", use_last_error=True)
        self.handles = set()
        self.lock = threading.Lock()

        class SecurityAttributes(c.Structure):
            _fields_ = [("length", w.DWORD), ("descriptor", c.c_void_p),
                        ("inherit", w.BOOL)]

        class StartupInfo(c.Structure):
            _fields_ = [("cb", w.DWORD), ("reserved", w.LPWSTR),
                ("desktop", w.LPWSTR), ("title", w.LPWSTR)] + [
                (name, w.DWORD) for name in ("x", "y", "xsize", "ysize",
                    "xchars", "ychars", "fill", "flags")] + [
                ("show", w.WORD), ("reserved_size", w.WORD),
                ("reserved_bytes", c.c_void_p), ("stdin", w.HANDLE),
                ("stdout", w.HANDLE), ("stderr", w.HANDLE)]

        class StartupInfoEx(c.Structure):
            _fields_ = [("info", StartupInfo), ("attributes", c.c_void_p)]

        class ProcessInfo(c.Structure):
            _fields_ = [("process", w.HANDLE), ("thread", w.HANDLE),
                        ("pid", w.DWORD), ("tid", w.DWORD)]

        class BasicLimits(c.Structure):
            _fields_ = [("process_time", c.c_longlong), ("job_time", c.c_longlong),
                ("flags", w.DWORD), ("minimum", c.c_size_t), ("maximum", c.c_size_t),
                ("active_limit", w.DWORD), ("affinity", c.c_size_t),
                ("priority", w.DWORD), ("scheduling", w.DWORD)]

        class IoCounters(c.Structure):
            _fields_ = [(name, c.c_ulonglong) for name in
                        ("reads", "writes", "other", "read_bytes", "write_bytes", "other_bytes")]

        class ExtendedLimits(c.Structure):
            _fields_ = [("basic", BasicLimits), ("io", IoCounters),
                ("process_memory", c.c_size_t), ("job_memory", c.c_size_t),
                ("peak_process_memory", c.c_size_t), ("peak_job_memory", c.c_size_t)]

        class Accounting(c.Structure):
            _fields_ = [(name, c.c_longlong) for name in
                ("user_time", "kernel_time", "period_user", "period_kernel")] + [
                (name, w.DWORD) for name in ("faults", "total", "active", "terminated")]

        class ProcessIdList(c.Structure):
            # Bounded ULONG_PTR list; oversized or incomplete readbacks are rejected.
            _fields_ = [("assigned", w.DWORD), ("listed", w.DWORD),
                        ("pids", c.c_size_t * 1024)]

        self.SecurityAttributes = SecurityAttributes
        self.StartupInfoEx, self.ProcessInfo = StartupInfoEx, ProcessInfo
        self.ExtendedLimits, self.Accounting = ExtendedLimits, Accounting
        self.ProcessIdList = ProcessIdList
        p = c.c_void_p
        hp, dp = c.POINTER(w.HANDLE), c.POINTER(w.DWORD)

        def bind(name, result, args):
            fn = getattr(self.kernel, name)
            fn.restype, fn.argtypes = result, args
            setattr(self, name, fn)

        bind("CreateJobObjectW", w.HANDLE, [p, w.LPCWSTR])
        bind("SetInformationJobObject", w.BOOL, [w.HANDLE, c.c_int, p, w.DWORD])
        bind("QueryInformationJobObject", w.BOOL, [w.HANDLE, c.c_int, p, w.DWORD, dp])
        bind("TerminateJobObject", w.BOOL, [w.HANDLE, w.UINT])
        bind("IsProcessInJob", w.BOOL, [w.HANDLE, w.HANDLE, c.POINTER(w.BOOL)])
        bind("CreatePipe", w.BOOL, [hp, hp, p, w.DWORD])
        bind("SetHandleInformation", w.BOOL, [w.HANDLE, w.DWORD, w.DWORD])
        bind("InitializeProcThreadAttributeList", w.BOOL, [p, w.DWORD, w.DWORD, c.POINTER(c.c_size_t)])
        bind("UpdateProcThreadAttribute", w.BOOL,
             [p, w.DWORD, c.c_size_t, p, c.c_size_t, p, p])
        bind("DeleteProcThreadAttributeList", None, [p])
        bind("CreateProcessW", w.BOOL,
             [w.LPCWSTR, w.LPWSTR, p, p, w.BOOL, w.DWORD, p, w.LPCWSTR,
              c.POINTER(StartupInfoEx), c.POINTER(ProcessInfo)])
        bind("ResumeThread", w.DWORD, [w.HANDLE])
        bind("WaitForSingleObject", w.DWORD, [w.HANDLE, w.DWORD])
        bind("GetExitCodeProcess", w.BOOL, [w.HANDLE, dp])
        bind("GetProcessTimes", w.BOOL, [w.HANDLE] + [c.POINTER(w.FILETIME)] * 4)
        bind("ReadFile", w.BOOL, [w.HANDLE, p, w.DWORD, dp, p])
        bind("WriteFile", w.BOOL, [w.HANDLE, p, w.DWORD, dp, p])
        bind("CloseHandle", w.BOOL, [w.HANDLE])

    @staticmethod
    def check(ok, reason="process_readback_unknown"):
        if not ok:
            raise _NativeFailure(reason)

    def own(self, handle):
        if handle is None or handle == self.c.c_void_p(-1).value or handle == 0:
            raise _NativeFailure("process_group_capability_unavailable")
        with self.lock:
            self.handles.add(handle)
        return handle

    def close(self, handle):
        with self.lock:
            if handle not in self.handles:
                return
            self.handles.remove(handle)
            self.CloseHandle(handle)

    def active(self, job):
        info = self.Accounting()
        self.check(self.QueryInformationJobObject(
            job, 1, self.c.byref(info), self.c.sizeof(info), None))
        return info.active

    def members(self, job) -> tuple[int, ...]:
        info = self.ProcessIdList()
        self.check(self.QueryInformationJobObject(
            job, 3, self.c.byref(info), self.c.sizeof(info), None))
        count = info.listed
        self.check(info.assigned == count and count <= len(info.pids))
        members = tuple(info.pids[:count])
        self.check(all(pid > 0 for pid in members) and len(set(members)) == count)
        return members

    def identity(self, process, pid):
        stamps = [self.w.FILETIME() for _ in range(4)]
        self.check(self.GetProcessTimes(process, *(self.c.byref(item) for item in stamps)),
                   "process_identity_unknown")
        value = (stamps[0].dwHighDateTime << 32) | stamps[0].dwLowDateTime
        return ProcessIdentity(pid, f"windows:{value}", "process-times")


def windows_process_group_available():
    """Discover documented API support without spawning or choosing a policy."""
    if os.name != "nt":
        return False
    try:
        if sys.getwindowsversion().major < 10:
            return False  # PROC_THREAD_ATTRIBUTE_JOB_LIST requires Windows 10.
        _WindowsAPI()
        return True
    except (AttributeError, OSError, TypeError, ValueError):
        return False


class _Process:
    def __init__(self, api, information, argv):
        self.api, self.pid, self.args = api, information.pid, list(argv)
        self.handle = api.own(information.process)
        self.thread = api.own(information.thread)
        self.returncode = None

    def poll(self):
        if self.returncode is None:
            status = self.api.WaitForSingleObject(self.handle, 0)
            if status == 258:  # WAIT_TIMEOUT, not an exit-code guess.
                return None
            self.api.check(status == 0)
            value = self.api.w.DWORD()
            self.api.check(self.api.GetExitCodeProcess(self.handle, self.api.c.byref(value)))
            self.returncode = value.value
        return self.returncode


def _pipe(api, *, input_pipe=False):
    c, w = api.c, api.w
    read, write = w.HANDLE(), w.HANDLE()
    attributes = api.SecurityAttributes(c.sizeof(api.SecurityAttributes), None, True)
    api.check(api.CreatePipe(c.byref(read), c.byref(write), c.byref(attributes), 65536))
    read_handle, write_handle = api.own(read.value), api.own(write.value)
    parent, child = (write_handle, read_handle) if input_pipe else (read_handle, write_handle)
    api.check(api.SetHandleInformation(parent, 1, 0))
    return parent, child


def _spawn(api, job, pipes, argv, cwd, env):
    c, w = api.c, api.w
    size = c.c_size_t()
    api.InitializeProcThreadAttributeList(None, 2, 0, c.byref(size))
    api.check(size.value > 0, "process_group_capability_unavailable")
    attributes = c.create_string_buffer(size.value)
    api.check(api.InitializeProcThreadAttributeList(attributes, 2, 0, c.byref(size)),
              "process_group_capability_unavailable")
    try:
        jobs = (w.HANDLE * 1)(job)
        handles = (w.HANDLE * 3)(*(pipes[name][1] for name in ("stdin", "stdout", "stderr")))
        # Values and backing storage remain live until the attribute list is deleted.
        for key, value in ((0x0002000D, jobs), (0x00020002, handles)):
            api.check(api.UpdateProcThreadAttribute(
                attributes, 0, key, value, c.sizeof(value), None, None),
                "process_group_capability_unavailable")
        startup = api.StartupInfoEx()
        startup.info.cb = c.sizeof(startup)
        startup.info.flags = 0x100  # STARTF_USESTDHANDLES
        startup.info.stdin, startup.info.stdout, startup.info.stderr = handles
        startup.attributes = c.cast(attributes, c.c_void_p)
        information = api.ProcessInfo()
        command = c.create_unicode_buffer(subprocess.list2cmdline(list(argv)))
        # Empty mapping is an empty environment, never implicit host inheritance.
        environment = c.create_unicode_buffer(
            "\0".join(f"{key}={env[key]}" for key in sorted(env, key=str.upper)) + "\0\0")
        # JOB_LIST attaches atomically during creation. Suspension is only the
        # identity/callback gate; there is no create-then-assign orphan window.
        # DETACHED_PROCESS keeps this stdio-only child free of an implicit console.
        flags = 0x00080000 | 0x00000400 | 0x00000004 | 0x00000008
        api.check(api.CreateProcessW(argv[0], command, None, None, True, flags,
            environment, str(cwd), c.byref(startup), c.byref(information)),
            "process_group_capability_unavailable")
        return _Process(api, information, argv)
    finally:
        api.DeleteProcThreadAttributeList(attributes)


class _PipePump:
    """Three bounded workers; each owns and finally closes its parent pipe end.

    Native reads/writes may block individually, but never block the controller.
    Job termination closes the child ends; retained worker references keep any
    in-flight native buffer alive until its operation actually returns.
    """
    def __init__(self, api, pipes, input_bytes, limit):
        self.api, self.pipes, self.input_bytes, self.limit = api, pipes, input_bytes, limit
        self.output = {"stdout": bytearray(), "stderr": bytearray()}
        self.lock, self.stop, self.changed = threading.Lock(), threading.Event(), threading.Event()
        self.failure, self.threads, self.assigned = None, [], set()

    def reject(self, reason):
        with self.lock:
            if self.failure is None:
                self.failure = reason
        self.stop.set()
        self.changed.set()

    def _run(self, name):
        api, handle = self.api, self.pipes[name][0]
        c, w = api.c, api.w
        try:
            if name == "stdin":
                pending = memoryview(self.input_bytes)
                while pending and not self.stop.is_set():
                    block = c.create_string_buffer(bytes(pending[:65536]))
                    count = w.DWORD()
                    if not api.WriteFile(handle, block, min(len(pending), 65536), c.byref(count), None):
                        if c.get_last_error() in (109, 232, 233):
                            break  # Child may intentionally close stdin.
                        raise _NativeFailure("process_readback_unknown")
                    api.check(count.value > 0)
                    pending = pending[count.value:]
            else:
                block = c.create_string_buffer(65536)
                while not self.stop.is_set():
                    count = w.DWORD()
                    if not api.ReadFile(handle, block, len(block), c.byref(count), None):
                        if c.get_last_error() in (109, 232, 233):
                            break
                        raise _NativeFailure("process_readback_unknown")
                    if not count.value:
                        break
                    with self.lock:
                        if sum(map(len, self.output.values())) + count.value > self.limit:
                            self.failure = self.failure or "output_limit_exceeded"
                            self.stop.set()
                        else:
                            self.output[name].extend(block.raw[:count.value])
                    self.changed.set()
        except BaseException:
            self.reject("process_readback_unknown")
        finally:
            api.close(handle)
            self.changed.set()

    def start(self):
        for name in ("stdout", "stderr", "stdin"):
            thread = threading.Thread(target=self._run, args=(name,), daemon=True)
            thread.start()
            self.threads.append(thread)
            self.assigned.add(self.pipes[name][0])

    def running(self):
        return any(thread.is_alive() for thread in self.threads)

    def join(self, seconds):
        until = time.monotonic() + seconds
        for thread in self.threads:
            thread.join(max(0.0, until - time.monotonic()))
        return not self.running()


def run_trusted_managed_process(argv, *, cwd, input_text, env, deadline_at,
                                max_output_bytes=1048576, on_started=None, identity_port=None):
    started, monotonic_started = time.time(), time.monotonic()
    lifecycle = {"schema": "lh-managed-process-lifecycle/v1", "process_identity": None,
        "process_group_id": None, "started_at": started, "ended_at": started,
        "deadline_at": deadline_at, "elapsed_seconds": 0.0, "timed_out": False,
        "termination_attempted": False, "termination_confirmed": False,
        "termination_scope": "owned_process_group", "outcome": "unknown"}

    def finish(outcome):
        lifecycle.update(outcome=outcome, ended_at=time.time(),
                         elapsed_seconds=time.monotonic() - monotonic_started)
        return dict(lifecycle)

    if not windows_process_group_available():
        raise ManagedProcessUnknown("process_group_capability_unavailable", finish("unknown"))
    if (isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float))
            or not math.isfinite(deadline_at) or isinstance(max_output_bytes, bool)
            or not isinstance(max_output_bytes, int) or max_output_bytes < 1
            or not isinstance(argv, (list, tuple)) or not argv
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
    identity_port = identity_port if identity_port is not None else NativeProcessIdentityPort()
    try:
        current = ProcessIdentity.from_dict(identity_port.current().as_dict())
        if current is None or current.pid != os.getpid():
            raise ValueError("identity unavailable")
    except (OSError, ValueError, RuntimeError, AttributeError, TypeError):
        raise ManagedProcessUnknown("process_identity_unknown", finish("unknown")) from None

    api, job, process, pump = None, None, None, None
    failure, pipes = None, {}
    monotonic_deadline = monotonic_started + (deadline_at - started)

    def check_deadline():
        nonlocal failure
        if time.monotonic() >= monotonic_deadline:
            lifecycle["timed_out"] = True
            failure = "process_timeout"
            raise _NativeFailure(failure)

    def terminate():
        lifecycle["termination_attempted"] = True
        try:
            # The retained, non-inherited job handle owns this scope even if
            # identity observation fails. Never reopen a job or signal by PID.
            api.check(api.TerminateJobObject(job, 1))
            until = time.monotonic() + 1.0
            while True:
                if api.active(job) == 0 and process.poll() is not None:
                    return True
                if time.monotonic() >= until:
                    return False
                time.sleep(0.01)
        except (OSError, ValueError, RuntimeError):
            return False

    try:
        api = _WindowsAPI()
        job = api.own(api.CreateJobObjectW(None, None))
        limits = api.ExtendedLimits()
        limits.basic.flags = 0x2000  # KILL_ON_JOB_CLOSE; no breakaway flags.
        api.check(api.SetInformationJobObject(job, 9, api.c.byref(limits), api.c.sizeof(limits)),
                  "process_group_capability_unavailable")
        for name in ("stdin", "stdout", "stderr"):
            pipes[name] = _pipe(api, input_pipe=name == "stdin")
        check_deadline()
        process = _spawn(api, job, pipes, argv, cwd, env)
        for _, child in pipes.values():
            api.close(child)
        bound = api.w.BOOL()
        api.check(api.IsProcessInJob(process.handle, job, api.c.byref(bound)) and bound.value,
                  "process_identity_unknown")
        failure = "process_identity_unknown"
        identity = ProcessIdentity.from_dict(identity_port.observe(process.pid).as_dict())
        if identity is None or not identity.matches(api.identity(process.handle, process.pid)):
            raise _NativeFailure(failure)
        lifecycle.update(process_identity=identity.as_dict(), process_group_id=process.pid)
        check_deadline()
        if on_started is not None:
            failure = "callback_failed"
            on_started(process)
        check_deadline()
        failure = "process_identity_unknown"
        observed = observe_process_identity(identity, identity_port=identity_port)
        if observed.status != "alive" or not identity.matches(observed.identity):
            raise _NativeFailure(failure)
        pump = _PipePump(api, pipes, input_bytes, max_output_bytes)
        failure = None
        pump.start()
        api.check(api.ResumeThread(process.thread) == 1, "process_readback_unknown")
        api.close(process.thread)
        while True:
            check_deadline()
            if pump.failure:
                raise _NativeFailure(pump.failure)
            code = process.poll()
            if code is not None:
                if api.active(job) != 0:
                    # Accounting may lag the retained root handle's confirmed exit.
                    # Only a fresh, complete empty/root-only list permits waiting.
                    if any(pid != process.pid for pid in api.members(job)):
                        raise _NativeFailure("process_group_not_quiescent")
                elif not pump.running():
                    break
            else:
                observed = observe_process_identity(identity, identity_port=identity_port)
                if observed.status != "alive" and process.poll() is None:
                    raise _NativeFailure("process_identity_unknown")
            pump.changed.wait(min(0.02, max(0.0, monotonic_deadline - time.monotonic())))
            pump.changed.clear()
        if pump.failure:
            raise _NativeFailure(pump.failure)
        stdout = pump.output["stdout"].decode("utf-8", errors="strict")
        stderr = pump.output["stderr"].decode("utf-8", errors="strict")
        lifecycle["termination_confirmed"] = True
        return ManagedProcessResult(list(argv), process.returncode, stdout, stderr, finish("exited"))
    except BaseException as exc:
        reason = failure or (str(exc) if isinstance(exc, _NativeFailure) else "process_readback_unknown")
        if pump is not None:
            pump.stop.set()
        if process is not None:
            lifecycle["termination_confirmed"] = terminate()
        elif api is not None and job is not None:
            try:
                lifecycle["termination_confirmed"] = api.active(job) == 0
            except (OSError, RuntimeError):
                pass
        if pump is not None:
            # Closing the last job handle also kills on a failed explicit
            # termination. Confirmation still comes only from the readback above.
            api.close(job)
            if not pump.join(0.75):
                reason = "process_readback_unknown"
        if reason == "process_timeout":
            raise ManagedProcessTimeout(tuple(argv), finish("timed_out")) from None
        raise ManagedProcessUnknown(reason, finish("unknown")) from None
    finally:
        if api is not None:
            if job is not None:
                api.close(job)
            # Running workers own their buffers/handles until native I/O returns.
            # Do not close a handle underneath a still-pending synchronous call.
            assigned = pump.assigned if pump is not None else set()
            for handle in tuple(api.handles):
                if handle not in assigned:
                    api.close(handle)

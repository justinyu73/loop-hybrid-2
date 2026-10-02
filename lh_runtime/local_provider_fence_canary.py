#!/usr/bin/env python3
"""Local provider fence: a kernel-contained provider run on Linux without Orca.

LH starts the provider itself under the descriptor-signed provider-sandbox
profile (bubblewrap bind set, provider seccomp table, host network) -- no Orca
control plane, no ``LH_ORCA_CLI``, no ``orca`` on PATH.  Executor wiring and
the unsupported-backend refusals are platform-neutral and run everywhere; the
bubblewrap cases need Linux with bubblewrap and libseccomp.

Every refusal is asserted by its reason code, so removing a check (digest
re-verification, single-use budget, argv policy, environment allowlist) turns
the corresponding case red instead of passing for a different reason.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import execution_fence as fences  # noqa: E402

AGENT = "codex"
CODEX_RULES = {
    "flags": ["exec", "--ephemeral", "--json", "--dangerously-bypass-approvals-and-sandbox"],
    "value_flags": ["-m"],
    "prompt_flags": [],
    "trailing_prompt": True,
}
KEYCTL_SYSCALL = {"x86_64": 250, "aarch64": 219}
# The fake provider reports what it observed from inside the sandbox.  It is a
# plain Python script so no node runtime binding is involved.
PROVIDER_SOURCE = """#!/usr/bin/python3
import ctypes, json, os, platform, sys, time
report = {
    "argv": sys.argv[1:],
    "cwd": os.getcwd(),
    "path": os.environ.get("PATH"),
    "home": os.environ.get("HOME"),
    "codex_home": os.environ.get("CODEX_HOME"),
    "overlay": os.environ.get("LH_CANARY_OVERLAY"),
    "env_keys": sorted(os.environ),
}
try:
    with open("provider-wrote.txt", "w") as handle:
        handle.write("ok")
    report["clone_write"] = True
except OSError as exc:
    report["clone_write"] = exc.errno
try:
    with open(os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "escape.txt"), "w") as handle:
        handle.write("x")
    report["ro_write"] = True
except OSError as exc:
    report["ro_write"] = exc.errno
number = {"x86_64": 250, "aarch64": 219}.get(platform.machine())
if number is not None:
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(number, 0, -3, 0)
    report["keyctl"] = [result, ctypes.get_errno()]
report["stdin"] = sys.stdin.read()
if os.environ.get("LH_CANARY_MODE") == "sleep":
    time.sleep(120)
print(json.dumps(report))
"""


def case(case_id: str, ok: bool, detail: str) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _refusal(action: Callable[[], Any]) -> str | None:
    """Return the fence refusal reason, or None when ``action`` was admitted."""
    try:
        action()
    except fences.ExecutionFenceUnavailable as exc:
        return exc.reason
    return None


def _keyctl_errno() -> int | None:
    number = KEYCTL_SYSCALL.get(platform.machine())
    if number is None:
        return None
    libc = ctypes.CDLL(None, use_errno=True)
    result = libc.syscall(number, 0, -3, 0)
    return ctypes.get_errno() if result < 0 else 0


def _fixture_root() -> Path:
    # bubblewrap mounts a fresh tmpfs on /tmp, so the provider and its bind
    # set must live outside /tmp to stay visible inside the sandbox.
    base = Path.home() / ".cache" / "lh-local-provider-canary"
    base.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="run-", dir=base)).resolve()


def _write_provider(bin_dir: Path) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    provider = bin_dir / AGENT
    provider.write_text(PROVIDER_SOURCE, encoding="utf-8")
    provider.chmod(0o755)
    return provider.resolve()


def _codex_home(root: Path) -> Path:
    home = root / "codex-home"
    home.mkdir(parents=True, exist_ok=True)
    (home / "auth.json").write_text("{}\n", encoding="utf-8")
    (home / "config.toml").write_text("", encoding="utf-8")
    return home.resolve()


def _policy(provider: Path, bwrap: Path, bin_dir: Path, codex_home: Path) -> dict[str, Any]:
    # Deliberately no "orca_cli": the local provider path must not need one.
    return {
        "schema": fences.EGRESS_POLICY_SCHEMA,
        "issuer": "lh-local-provider-canary",
        "enforced_by": fences.EGRESS_POLICY_ENFORCED_BY,
        "providers": {AGENT: {"path": str(provider), "sha256": _sha256(provider), **CODEX_RULES}},
        "provider_sandbox_profile": {
            "bubblewrap": {"path": str(bwrap), "sha256": _sha256(bwrap), "version": "0.9.0"},
            "network": "host",
            "ro_binds": ["/usr", "/bin", "/lib", "/lib64", "/etc", str(bin_dir)],
            "provider_home_ro_binds": {AGENT: [str(codex_home)]},
            "env": {"TERM": "dumb"},
            "flags": ["--die-with-parent", "--new-session"],
            "seccomp": {
                "default": "allow",
                "denied_syscalls": list(fences.PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS),
            },
        },
    }


def _binding(clone: Path) -> dict[str, Any]:
    return fences.build_attempt_binding(
        goal={"goal_id": "local-provider-canary"},
        run_id="run-local-provider",
        attempt=1,
        attempt_fence=1,
        base_revision="0" * 40,
        clone_root=clone,
        verifier_argv=["true"],
        adapter_id=fences.LOCAL_PROVIDER_ADAPTER_PREFIX + AGENT,
        adapter_version="v1",
        timeout_seconds=300,
    )


def _provider_argv(provider: Path, prompt: str) -> list[str]:
    return [str(provider), "exec", "--ephemeral", "--json",
            "--dangerously-bypass-approvals-and-sandbox", prompt]


def _report(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    lines = [line for line in (proc.stdout or "").splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else {}


def _processes_running(marker: str) -> list[str]:
    found: list[str] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if marker in command:
            found.append(f"{entry.name}:{command[:120]}")
    return found


def linux_cases() -> list[dict[str, Any]]:
    bwrap_found = shutil.which("bwrap")
    if not sys.platform.startswith("linux") or bwrap_found is None:
        reason = "local_provider_fence_requires_linux_bubblewrap"
        return [case(name, False, reason) for name in (
            "L1-prepare-without-orca", "L2-provider-runs-inside-sandbox",
            "L3-refusals-are-reasoned", "L4-timeout-ends-process-group",
            "L7-instance-policy-drives-local-provider")]
    root = _fixture_root()
    saved = {name: os.environ.get(name) for name in ("PATH", "LH_EGRESS_POLICY", "LH_ORCA_CLI")}
    cases: list[dict[str, Any]] = []
    try:
        bin_dir = root / "bin"
        provider = _write_provider(bin_dir)
        codex_home = _codex_home(root)
        clone = root / "clone"
        clone.mkdir()
        bwrap = Path(bwrap_found).resolve()
        policy_path = root / "policy.json"
        policy_path.write_text(json.dumps(_policy(provider, bwrap, bin_dir, codex_home)), encoding="utf-8")
        os.environ["PATH"] = f"{bin_dir}:/usr/bin:/bin"
        os.environ["LH_EGRESS_POLICY"] = str(policy_path)
        os.environ.pop("LH_ORCA_CLI", None)
        orca_absent = shutil.which("orca") is None and "LH_ORCA_CLI" not in os.environ
        fence = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": fences.LINUX_BACKEND_ID})
        if isinstance(fence, fences.DisabledExecutionFencePort):
            reason = f"linux_backend_unavailable:{fence.reason}"
            return [case(name, False, reason) for name in (
                "L1-prepare-without-orca", "L2-provider-runs-inside-sandbox",
                "L3-refusals-are-reasoned", "L4-timeout-ends-process-group",
                "L7-instance-policy-drives-local-provider")]

        # L1: prepare pins provider and bwrap, carries no control plane.
        descriptor = fence.prepare(_binding(clone))
        local = descriptor.get("local_provider") or {}
        proofs = descriptor.get("proofs") or {}
        receipt = fence.receipt_projection(descriptor)
        l1 = (
            orca_absent
            and "control_plane" not in descriptor
            and (local.get("provider") or {}).get("path") == str(provider)
            and (local.get("provider") or {}).get("sha256") == _sha256(provider)
            and descriptor.get("launch_classes") == {"control": 0, "mutation": 0, "provider": 1}
            and {track: (proofs.get(track) or {}).get("result") for track in fences.REQUIRED_PROOF_TRACKS} == {
                "filesystem_effect_containment": "applied_by_provider_sandbox",
                "provider_control_egress": "host_network_policy_preflight",
                "provider_sandbox": "applied",
            }
            and (proofs.get("provider_sandbox") or {}).get("enforced_by") == "lh-client-composed"
            and ((receipt.get("local_provider") or {}).get("provider_sandbox") or {}).get("profile_digest")
            == (local.get("provider_sandbox") or {}).get("profile_digest")
        )
        cases.append(case("L1-prepare-without-orca", l1, json.dumps({
            "orca_absent": orca_absent, "launch_classes": descriptor.get("launch_classes"),
            "proofs": {track: (proofs.get(track) or {}).get("result") for track in proofs},
            "has_control_plane": "control_plane" in descriptor})))

        # L2: the provider observes the sandbox from the inside.
        baseline_keyctl = _keyctl_errno()
        argv = _provider_argv(provider, "PROMPT-L2")
        proc = fence.launch_provider(descriptor, argv, env_overlay={"LH_CANARY_OVERLAY": "overlay-ok"},
                                     input_text="stdin-ok", timeout_seconds=60)
        report = _report(proc)
        # PWD is set by bubblewrap --chdir; everything else is fence-owned or the overlay.
        allowed_env = {"PATH", "HOME", "TERM", "TMPDIR", "CODEX_HOME", "LH_CANARY_OVERLAY", "LC_CTYPE", "PWD"}
        l2 = (
            proc.returncode == 0
            and report.get("argv") == argv[1:]
            and report.get("cwd") == str(clone)
            and report.get("clone_write") is True and (clone / "provider-wrote.txt").is_file()
            and report.get("ro_write") in {errno.EROFS, errno.EACCES}
            and not (bin_dir / "escape.txt").exists()
            and str(report.get("path", "")).split(":")[0] == str(bin_dir)
            and report.get("home") == "/tmp" and report.get("codex_home") == "/tmp/codex-home"
            and report.get("overlay") == "overlay-ok"
            and report.get("stdin") == "stdin-ok"
            and set(report.get("env_keys") or []) <= allowed_env
            and (report.get("keyctl") or [None, None])[1] == errno.EPERM
            and baseline_keyctl != errno.EPERM
        )
        cases.append(case("L2-provider-runs-inside-sandbox", l2, json.dumps(
            {"returncode": proc.returncode, "report": report, "baseline_keyctl": baseline_keyctl,
             "stderr": (proc.stderr or "")[-300:]})))

        # L3: every refusal names its own reason.
        refusals: dict[str, str | None] = {}
        refusals["replayed"] = _refusal(lambda: fence.launch_provider(
            descriptor, argv, timeout_seconds=30))
        tampered = json.loads(json.dumps(fence.prepare(_binding(clone))))
        tampered["proofs"]["provider_sandbox"]["result"] = "not_applicable"
        refusals["tampered"] = _refusal(lambda: fence.launch_provider(
            tampered, argv, timeout_seconds=30))
        swapped = fence.prepare(_binding(clone))
        original = provider.read_bytes()
        provider.write_bytes(original + b"# swapped after prepare\n")
        try:
            refusals["swapped_binary"] = _refusal(lambda: fence.launch_provider(
                swapped, argv, timeout_seconds=30))
        finally:
            provider.write_bytes(original)
        outside = fence.prepare(_binding(clone))
        refusals["argv_outside_policy"] = _refusal(lambda: fence.launch_provider(
            outside, [*argv[:-1], "--unknown-flag", argv[-1]], timeout_seconds=30))
        overlay = fence.prepare(_binding(clone))
        refusals["env_overlay"] = _refusal(lambda: fence.launch_provider(
            overlay, argv, env_overlay={"LD_PRELOAD": "/x.so"}, timeout_seconds=30))
        classes = fence.prepare(_binding(clone))
        refusals["mutation_launch"] = _refusal(lambda: fence.launch(
            classes, ["/bin/true"], timeout_seconds=30))
        refusals["control_launch"] = _refusal(lambda: fence.launch_control(
            classes, {"op": "repo_list"}, timeout_seconds=30))
        expected = {
            "replayed": "descriptor_replayed",
            "tampered": "descriptor_digest_invalid",
            "swapped_binary": "local_provider_binary_unpinned",
            "argv_outside_policy": "control_provider_argv_outside_policy",
            "env_overlay": "local_provider_env_overlay_invalid",
            "mutation_launch": "launch_class_not_authorized",
            "control_launch": "launch_class_not_authorized",
        }
        cases.append(case("L3-refusals-are-reasoned", refusals == expected, json.dumps(refusals)))

        # L4: a timeout ends the whole sandboxed process group.
        slow = fence.prepare(_binding(clone))
        timed_out = False
        try:
            fence.launch_provider(slow, _provider_argv(provider, "PROMPT-L4"),
                                  env_overlay={"LH_CANARY_MODE": "sleep"}, timeout_seconds=3)
        except subprocess.TimeoutExpired:
            timed_out = True
        survivors: list[str] = []
        for _ in range(20):
            survivors = _processes_running(str(provider))
            if not survivors:
                break
            time.sleep(0.25)
        cases.append(case("L4-timeout-ends-process-group", timed_out and not survivors,
                          json.dumps({"timed_out": timed_out, "survivors": survivors})))

        # L7: an instance-generated policy (no hand-written profile, no Orca)
        # is enough to prepare and run the local provider.
        import instance_config
        home = root / "home"
        home.mkdir()
        environ = {"PATH": f"{bin_dir}:{bwrap.parent}:/usr/bin:/bin", "HOME": str(home),
                   "CODEX_HOME": str(codex_home)}
        config = instance_config.initialize_instance(
            root / "instance" / "instance.json", system="Linux", environ=environ, home=home, cwd=root,
            overrides={"cli": {"providers": {AGENT: str(provider)}}})
        generated = json.loads(config.egress_policy_path.read_text(encoding="utf-8"))
        profile = generated.get("provider_sandbox_profile") or {}
        os.environ["LH_EGRESS_POLICY"] = str(config.egress_policy_path)
        generated_descriptor = fence.prepare(_binding(clone))
        generated_run = fence.launch_provider(generated_descriptor, _provider_argv(provider, "PROMPT-L7"),
                                              timeout_seconds=60)
        l7 = (
            "orca_cli" not in generated
            and (profile.get("bubblewrap") or {}).get("path") == str(bwrap)
            and profile.get("network") == "host"
            and generated_run.returncode == 0
            and _report(generated_run).get("cwd") == str(clone)
        )
        cases.append(case("L7-instance-policy-drives-local-provider", l7, json.dumps({
            "has_orca_cli": "orca_cli" in generated, "profile_keys": sorted(profile),
            "returncode": generated_run.returncode, "stderr": (generated_run.stderr or "")[-300:]})))
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        cases.append(case("linux-cases-crashed", False, f"{type(exc).__name__}: {exc}"))
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        shutil.rmtree(root, ignore_errors=True)
    return cases


class _RecordingPort(fences.ExecutionFencePort):
    """Executor-side double: records the provider launch, refuses everything else."""

    def __init__(self) -> None:
        self.provider_calls: list[dict[str, Any]] = []
        self.other_calls = 0

    def prepare(self, binding: Any) -> dict[str, Any]:
        self.other_calls += 1
        raise fences.ExecutionFenceUnavailable("recording_port_prepare")

    def launch(self, descriptor: Any, argv: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.other_calls += 1
        raise fences.ExecutionFenceUnavailable("recording_port_mutation_launch")

    def launch_control(self, descriptor: Any, request: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.other_calls += 1
        raise fences.ExecutionFenceUnavailable("recording_port_control_launch")

    def receipt_projection(self, descriptor: Any) -> dict[str, Any]:
        return {}

    def launch_provider(self, descriptor: Any, provider_argv: Any, *, env_overlay: Any = None,
                        input_text: Any = None, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        argv = [str(item) for item in provider_argv]
        self.provider_calls.append({"argv": argv, "env_overlay": dict(env_overlay or {}),
                                    "descriptor": descriptor})
        stdout = json.dumps({"type": "turn.completed", "usage": {"input_tokens": 3, "output_tokens": 2}})
        return subprocess.CompletedProcess(argv, 0, stdout + "\n", "")


def executor_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    root = Path(tempfile.mkdtemp(prefix="lh-local-provider-executor-")).resolve()
    saved_path = os.environ.get("PATH")
    try:
        bin_dir = root / "bin"
        bin_dir.mkdir()
        if os.name == "nt":
            (bin_dir / "codex.cmd").write_text("@echo off\r\n", encoding="utf-8")
        else:
            fake = bin_dir / AGENT
            fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake.chmod(0o755)
        os.environ["PATH"] = str(bin_dir) + os.pathsep + (saved_path or "")
        clone = root / "clone"
        clone.mkdir()
        import goal_loop_run
        binding = {"runner": AGENT, "base_url": "https://provider.example/v1", "model": "model-x"}
        port = _RecordingPort()
        model = goal_loop_run.resolve_executor("local", execute=True, provider_binding=binding,
                                               execution_fence_port=port)
        descriptor = {"schema": "recording"}
        result = model(clone, {"goal": {"goal_id": "g"}, "execution_fence": descriptor,
                               "run_id": "run-x", "attempt": 1})
        call = port.provider_calls[0] if port.provider_calls else {"argv": []}
        argv = call["argv"]
        execution = result.get("execution") or {}
        l5 = (
            len(port.provider_calls) == 1
            and port.other_calls == 0
            and call.get("descriptor") is descriptor
            and argv[1:2] == ["exec"]
            and "-m" in argv and argv[argv.index("-m") + 1] == "model-x"
            and any(token.startswith("model_providers.lh_local=") for token in argv)
            and 'model_provider="lh_local"' in argv
            and execution.get("backend") == "local"
            and (execution.get("provider_binding") or {}).get("model") == "model-x"
            and getattr(model, "execution_fence_adapter_id", None) == fences.LOCAL_PROVIDER_ADAPTER_PREFIX + AGENT
            and getattr(model, "requires_execution_fence", False) is True
        )
        cases.append(case("L5-local-executor-carries-provider-binding", l5, json.dumps(
            {"argv": argv, "execution": execution, "other_calls": port.other_calls})))
        try:
            goal_loop_run.resolve_executor("codex", execute=True, provider_binding=binding,
                                           execution_fence_port=port)
            plain_refused = False
        except ValueError:
            plain_refused = True
        cases.append(case("L5-plain-codex-still-refuses-provider-binding", plain_refused, str(plain_refused)))
    except Exception as exc:
        cases.append(case("L5-executor-cases-crashed", False, f"{type(exc).__name__}: {exc}"))
    finally:
        if saved_path is None:
            os.environ.pop("PATH", None)
        else:
            os.environ["PATH"] = saved_path
        shutil.rmtree(root, ignore_errors=True)
    return cases


def unsupported_cases() -> list[dict[str, Any]]:
    try:
        disabled_reason = _refusal(lambda: fences.DisabledExecutionFencePort().launch_provider(
            {}, ["codex"], timeout_seconds=1))
        import execution_fence_trusted
        trusted_inherits = (
            execution_fence_trusted.TrustedProjectExecutionFence.launch_provider
            is fences.ExecutionFencePort.launch_provider
        )
        windows_source = (HERE / "execution_fence_windows.py").read_text(encoding="utf-8")
        windows_inherits = "def launch_provider" not in windows_source
    except Exception as exc:
        return [case("L6-other-backends-refuse-local-provider", False, f"{type(exc).__name__}: {exc}")]
    ok = disabled_reason == "local_provider_unsupported" and trusted_inherits and windows_inherits
    return [case("L6-other-backends-refuse-local-provider", ok, json.dumps({
        "disabled": disabled_reason, "trusted_inherits_refusal": trusted_inherits,
        "windows_inherits_refusal": windows_inherits}))]


def main() -> int:
    cases = [*executor_cases(), *unsupported_cases(), *linux_cases()]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-local-provider-fence",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "provider network egress is host network by design; only argv is preflighted against the policy",
            "kernel-contained local provider runs are Linux-only; Windows and macOS refuse with local_provider_unsupported",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

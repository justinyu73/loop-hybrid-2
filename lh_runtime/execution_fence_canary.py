#!/usr/bin/env python3
"""Offline positive/negative canary for the preventive execution boundary."""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import cli_agent_executor as executors
import delivery_contract as delivery_engine
import execution_fence as fences
import goal_loop_run
from _fixture import make_source_repo
from controller import LoopController
from run_store import RunStore


FIXED_TIME = 1_800_000_000.0
FIXED_NONCE = "n03-controller-nonce-fixture"

PROBE = r'''#!/usr/bin/env python3
import errno
import json
import os
import socket
import sys

targets = dict(zip(sys.argv[1::2], sys.argv[2::2]))
result = {
    "control_input": sys.stdin.read(),
    "control_channel": os.environ.get("LH_PROVIDER_CONTROL_CHANNEL"),
    "descriptor_digest": os.environ.get("LH_EXECUTION_FENCE_DESCRIPTOR_DIGEST"),
    "filesystem": {},
    "egress": {},
}

try:
    with open("/workspace/allowed-write.txt", "w", encoding="utf-8") as handle:
        handle.write("clone-write-ok\n")
    result["filesystem"]["clone"] = {"allowed": True}
except OSError as exc:
    result["filesystem"]["clone"] = {"allowed": False, "errno": exc.errno}

for name, path in targets.items():
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("escape\n")
        result["filesystem"][name] = {"denied": False}
    except OSError as exc:
        result["filesystem"][name] = {
            "denied": True,
            "errno": exc.errno,
        }

for name, family, kind in (
    ("tcp", socket.AF_INET, socket.SOCK_STREAM),
    ("udp", socket.AF_INET, socket.SOCK_DGRAM),
    ("unix_socket", socket.AF_UNIX, socket.SOCK_STREAM),
):
    try:
        handle = socket.socket(family, kind)
        handle.close()
        result["egress"][name] = {"denied": False}
    except OSError as exc:
        result["egress"][name] = {"denied": True, "errno": exc.errno}

try:
    socket.getaddrinfo("example.invalid", 443)
    result["egress"]["dns"] = {"denied": False}
except OSError as exc:
    result["egress"]["dns"] = {"denied": True, "errno": exc.errno}

try:
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    os.close(write_fd)
    result["egress"]["ipc"] = {"denied": False}
except OSError as exc:
    result["egress"]["ipc"] = {"denied": True, "errno": exc.errno}

try:
    os.kill(os.getpid(), 0)
    result["egress"]["signal"] = {"denied": False}
except OSError as exc:
    result["egress"]["signal"] = {"denied": True, "errno": exc.errno}

try:
    child = os.fork()
    if child == 0:
        os._exit(0)
    os.waitpid(child, 0)
    result["egress"]["process_control"] = {"denied": False}
except OSError as exc:
    result["egress"]["process_control"] = {
        "denied": True,
        "errno": exc.errno,
    }

print(json.dumps(result, sort_keys=True))
'''


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _prepare_root() -> Path:
    # Hermetic per-run root. The previous fixed path under
    # /tmp/host-run/20260730 was owned by whichever account ran the canary
    # first: any other principal (the receipt verifier runs checks as
    # host-verifier in a PrivateTmp mount) hit PermissionError at mkdir and
    # the gate could never pass under the service. mkdtemp is unique per
    # invocation and writable by the caller whoever that is; the rmtree
    # identity guard above the old path is unnecessary once the path is
    # not predictable. main() removes the whole run directory.
    run_root = Path(tempfile.mkdtemp(prefix="lh-fence-canary-")).resolve()
    task_root = run_root / "task-root"
    task_root.mkdir()
    return task_root


def _binding(clone: Path) -> dict[str, Any]:
    return fences.build_attempt_binding(
        goal={
            "goal_id": "LH-EXAMPLE-GOAL-002",
            "node_id": "N03",
        },
        run_id="run-n03-fixture",
        attempt=1,
        attempt_fence=1,
        base_revision="2b5498e4d9c80abab62658f4c73415f3a48f97a4",
        clone_root=clone,
        verifier_argv=["python3", "-B", "lh_runtime/execution_fence_canary.py"],
        adapter_id="fixture-mutation-adapter",
        adapter_version="v1",
        timeout_seconds=60,
        now=FIXED_TIME,
        nonce=FIXED_NONCE,
    )


class MutationAdapterSpy:
    requires_execution_fence = True
    execution_fence_adapter_id = "mutation-adapter-spy"
    execution_fence_adapter_version = "v1"

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        del workspace, capsule
        self.calls += 1
        return {"summary": "unexpected invocation"}


def _delivery_fixture(
    *, goal_id: str, goal_revision: int, node_id: str, unit_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the real generic contract/plan for the controller fence fixture.

    N03 is a native LH canary, so its direct RunStore producer must carry the
    same persisted delivery binding as a normal Goal admission.  The fixture
    uses the shared engine only to seal and plan; it does not reimplement any
    contract verification.
    """
    body = {
        "schema": delivery_engine.SCHEMA,
        "contract_version": 1,
        "contract_id": f"contract-{unit_id}",
        "unit_id": unit_id,
        "goal": {"id": goal_id, "revision": goal_revision},
        "node": {"id": node_id, "kind": "coding"},
        "planner": {"principal": "planner-n03-canary", "source": "native-fence-fixture"},
        "independent_verifier": {
            "principal": "verifier-n03-canary",
            "read_only": True,
            "source_write": False,
            "capability": "n03-independent-verifier",
            "argv": [sys.executable, "-B", "-c", "pass"],
            "cwd": "${WORKTREE}",
            "timeout_seconds": 10,
        },
        "outcome": {
            "observable": "execution fence rejects an unavailable mutation adapter",
            "start_state": "running",
            "success_state": "verified",
            "terminal_states": ["verified", "human_required"],
        },
        "scope": {
            "ownership": "native-execution-fence-canary",
            "allowed_paths": ["src/"],
            "forbidden_paths": ["secrets/"],
            "identity": [
                "unit_id", "goal_id", "goal_revision", "node_id", "dispatch_key",
                "run_id", "attempt", "fence", "base_sha", "diff_digest",
            ],
        },
        "obligations": [{
            "id": "fence-fixture-check",
            "commands": [{
                "id": "bounded-check",
                "argv": [sys.executable, "-B", "-c", "pass"],
                "cwd": "${WORKTREE}",
                "timeout_seconds": 5,
                "expect_exit": 0,
            }],
            "required_receipts": ["executor"],
        }],
        "required_receipts": [
            "plan_verdict", "packet_admission", "dispatch", "executor",
            "delivery_verifier", "completion",
        ],
        "source_required_receipts": [
            "plan_verdict", "packet_admission", "dispatch", "executor",
            "delivery_verifier",
        ],
        "source_vs_live": {
            "source_must_not_claim_live": True,
            "live_required_for_source_delivery": False,
        },
        "repair_same_unit": {
            "enabled": True,
            "route": "same_work_unit_new_attempt",
            "max_attempts": 1,
        },
        "authority_store": "run",
        "managed_scope": "native-execution-fence-canary",
    }
    contract = delivery_engine.seal_contract(body)
    return contract, delivery_engine.plan_delivery_unit(contract)


def _unavailable_case(
    port: fences.ExecutionFencePort,
    clone: Path,
) -> tuple[bool, str]:
    spy = MutationAdapterSpy()
    try:
        descriptor = port.prepare(_binding(clone))
        spy(
            clone,
            {
                "execution_fence": descriptor,
                "run_id": "run-n03-fixture",
                "attempt": 1,
            },
        )
        reason = "unexpected_admission"
    except fences.ExecutionFenceUnavailable as exc:
        reason = exc.reason
    return spy.calls == 0, reason


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-out")
    args = parser.parse_args(argv)
    root = _prepare_root()
    clone = root / "clone"
    clone.mkdir()
    probe = clone / "probe.py"
    probe.write_text(PROBE, encoding="utf-8")
    probe.chmod(probe.stat().st_mode | stat.S_IXUSR)

    outside = {
        "sibling": root / "sibling" / "sentinel.txt",
        "source": root / "source" / "sentinel.txt",
        "home": root / "home" / "sentinel.txt",
        "state": root / "state" / "sentinel.txt",
        "socket": root / "socket" / "sentinel.sock",
        "device": root / "device" / "sentinel.dev",
    }
    for name, path in outside.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{name}-sentinel\n", encoding="utf-8")
    escape = clone / "escape-link"
    escape.symlink_to(outside["sibling"])
    before = {name: _digest(path) for name, path in outside.items()}

    discovered = fences.LinuxBubblewrapExecutionFence.discover(
        clock=lambda: FIXED_TIME,
    )
    cases: list[dict[str, Any]] = []
    cases.append(
        _case(
            "backend-is-bubblewrap-0.9.0-plus-seccomp",
            isinstance(discovered, fences.LinuxBubblewrapExecutionFence)
            and discovered.bubblewrap_version == "0.9.0",
            {
                "backend": type(discovered).__name__,
                "version": getattr(discovered, "bubblewrap_version", None),
                "seccomp": getattr(discovered, "seccomp_library", None),
            },
        )
    )
    if not isinstance(discovered, fences.LinuxBubblewrapExecutionFence):
        payload = {
            "check_id": "lh-preventive-execution-fence-n03",
            "status": "fail",
            "blocking_failures": cases,
            "provider_invocations": 0,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        shutil.rmtree(root)
        return 1

    descriptor = discovered.prepare(_binding(clone))
    projection = discovered.receipt_projection(descriptor)
    proofs = descriptor["proofs"]
    same_attempt = (
        set(proofs) == set(fences.REQUIRED_PROOF_TRACKS)
        and all(
            proof["attempt_binding_digest"] == descriptor["binding_digest"]
            for proof in proofs.values()
        )
        and proofs["filesystem_effect_containment"]["result"] == "admissible"
        and proofs["provider_control_egress"]["result"] == "admissible"
        # A pure-mutation descriptor has no hosted provider terminal, and the
        # third track must say so rather than borrow "admissible".
        and proofs["provider_sandbox"]["result"] == "not_applicable"
    )
    cases.append(
        _case(
            "same-immutable-attempt-carries-both-admissible-proofs",
            same_attempt,
            {
                "binding_digest": descriptor["binding_digest"],
                "proofs_digest": descriptor["proofs_digest"],
                "tracks": sorted(proofs),
            },
        )
    )

    target_args: list[str] = []
    for name, path in outside.items():
        target_args.extend([name, str(path)])
    target_args.extend(
        [
            "device_node",
            "/dev/null",
            "symlink_escape",
            "/workspace/escape-link",
        ]
    )
    completed = discovered.launch(
        descriptor,
        [sys.executable, "/workspace/probe.py", *target_args],
        input_text="attempt-bound-provider-control\n",
        timeout_seconds=20,
    )
    try:
        probe_result = json.loads(completed.stdout)
    except json.JSONDecodeError:
        probe_result = {
            "decode_error": completed.stdout,
            "stderr": completed.stderr,
        }
    after = {name: _digest(path) for name, path in outside.items()}
    cases.append(
        _case(
            "clone-write-succeeds",
            completed.returncode == 0
            and probe_result.get("filesystem", {})
            .get("clone", {})
            .get("allowed")
            is True
            and (clone / "allowed-write.txt").read_text(encoding="utf-8")
            == "clone-write-ok\n",
            {
                "returncode": completed.returncode,
                "stderr": completed.stderr,
            },
        )
    )
    filesystem_cases = [
        "sibling",
        "source",
        "home",
        "state",
        "socket",
        "device",
        "device_node",
        "symlink_escape",
    ]
    cases.append(
        _case(
            "outside-and-symlink-writes-denied-with-unchanged-digests",
            all(
                probe_result.get("filesystem", {})
                .get(name, {})
                .get("denied")
                is True
                for name in filesystem_cases
            )
            and before == after,
            {
                "syscalls": probe_result.get("filesystem"),
                "before": before,
                "after": after,
            },
        )
    )
    egress_cases = [
        "dns",
        "tcp",
        "udp",
        "unix_socket",
        "ipc",
        "signal",
        "process_control",
    ]
    cases.append(
        _case(
            "alternate-egress-ipc-and-signal-denied",
            all(
                probe_result.get("egress", {}).get(name, {}).get("denied")
                is True
                for name in egress_cases
            ),
            probe_result.get("egress"),
        )
    )
    cases.append(
        _case(
            "only-attempt-bound-provider-control-channel-remains",
            probe_result.get("control_input")
            == "attempt-bound-provider-control\n"
            and probe_result.get("control_channel") == "stdio"
            and probe_result.get("descriptor_digest")
            == descriptor["launch_descriptor_digest"],
            {
                "channel": probe_result.get("control_channel"),
                "descriptor_digest": probe_result.get("descriptor_digest"),
            },
        )
    )

    launch_count = discovered.launch_count
    try:
        discovered.launch(
            descriptor,
            ["/usr/bin/true"],
            timeout_seconds=2,
        )
        replay_reason = "unexpected_replay"
    except fences.ExecutionFenceUnavailable as exc:
        replay_reason = exc.reason
    cases.append(
        _case(
            "descriptor-replay-does-not-create-another-child",
            replay_reason == "descriptor_replayed"
            and discovered.launch_count == launch_count,
            {
                "reason": replay_reason,
                "launch_count": discovered.launch_count,
            },
        )
    )

    incomplete = fences.LinuxBubblewrapExecutionFence(
        bubblewrap_path=discovered.bubblewrap_path,
        bubblewrap_version=discovered.bubblewrap_version,
        seccomp_library=discovered.seccomp_library,
        proof_tracks=("filesystem_effect_containment",),
        clock=lambda: FIXED_TIME,
    )
    proof_blocked, proof_reason = _unavailable_case(incomplete, clone)
    misconfigured = fences.LinuxBubblewrapExecutionFence(
        bubblewrap_path=discovered.bubblewrap_path,
        bubblewrap_version="0.8.0",
        seccomp_library=discovered.seccomp_library,
        clock=lambda: FIXED_TIME,
    )
    config_blocked, config_reason = _unavailable_case(misconfigured, clone)
    disabled_blocked, disabled_reason = _unavailable_case(
        fences.DisabledExecutionFencePort(),
        clone,
    )
    cases.append(
        _case(
            "missing-backend-proof-or-valid-configuration-invokes-no-adapter",
            proof_blocked
            and config_blocked
            and disabled_blocked
            and proof_reason == "proof_track_incomplete"
            and config_reason == "bubblewrap_version_unsupported"
            and disabled_reason == "backend_not_configured",
            {
                "proof": proof_reason,
                "configuration": config_reason,
                "backend": disabled_reason,
                "provider_child_count": 0,
            },
        )
    )

    controller_root = root / "controller-fixture"
    controller_root.mkdir()
    source_repo, source_base = make_source_repo(controller_root)
    run_store = RunStore(controller_root / "runs")
    delivery_contract, delivery_plan = _delivery_fixture(
        goal_id="n03-unavailable",
        goal_revision=1,
        node_id="N03",
        unit_id="unit-n03-unavailable",
    )
    run_id = run_store.create_run(
        goal={
            "goal_id": "n03-unavailable",
            "goal_revision": 1,
            "node_id": "N03",
            "delivery_contract": delivery_contract,
            "delivery_plan": delivery_plan,
        },
        source_repo=source_repo,
        base_revision=source_base,
        max_attempts=1,
        run_id="run-n03-unavailable-fixture",
    )
    controller_spy = MutationAdapterSpy()
    controller_result = LoopController(
        run_store,
        controller_root / "workspaces",
        execution_fence_port=fences.DisabledExecutionFencePort(),
    ).tick(
        run_id,
        holder="n03-canary",
        model=controller_spy,
        verifier_argv=[
            sys.executable,
            "-c",
            "raise SystemExit(1)",
        ],
    )
    receipt_meta = run_store.latest_receipt(run_id)
    receipt = (
        json.loads(
            (run_store.root / receipt_meta["receipt_ref"]).read_text(
                encoding="utf-8"
            )
        )
        if receipt_meta
        else {}
    )
    cases.append(
        _case(
            "controller-records-human-required-before-provider-child",
            controller_result.get("status") == "human_required"
            and run_store.get_run(run_id)["state"] == "human_required"
            and controller_spy.calls == 0
            and receipt.get("execution_fence", {}).get("error_code")
            == fences.ERROR_CODE
            and receipt.get("provider", {}).get("provider_invocations") == 0,
            {
                "result": {
                    "status": controller_result.get("status"),
                    "reason": controller_result.get("reason"),
                    "provider_invocations": controller_result.get(
                        "provider_invocations"
                    ),
                },
                "run_state": run_store.get_run(run_id)["state"],
                "adapter_calls": controller_spy.calls,
                "receipt_fence": receipt.get("execution_fence"),
            },
        )
    )

    named_adapter_results: dict[str, str] = {}
    for adapter_name in ("codex",):
        adapter = executors.make_named_cli_agent(
            adapter_name,
            execution_fence_port=fences.DisabledExecutionFencePort(),
        )
        try:
            adapter(
                clone,
                {
                    "run_id": f"direct-{adapter_name}",
                    "attempt": 1,
                    "goal": {},
                    "base_revision": "base",
                },
            )
            named_adapter_results[adapter_name] = "unexpected_launch"
        except fences.ExecutionFenceUnavailable as exc:
            named_adapter_results[adapter_name] = exc.reason
    orca_adapter = executors.make_orca_agent(
        agent="codex",
        execution_fence_port=fences.DisabledExecutionFencePort(),
    )
    try:
        orca_adapter(
            clone,
            {
                "run_id": "direct-orca",
                "attempt": 1,
                "goal": {},
                "base_revision": "base",
            },
        )
        named_adapter_results["orca"] = "unexpected_launch"
    except fences.ExecutionFenceUnavailable as exc:
        named_adapter_results["orca"] = exc.reason
    cases.append(
        _case(
            "all-production-mutation-adapters-use-the-common-fence",
            all(
                reason == "descriptor_missing"
                for reason in named_adapter_results.values()
            )
            and set(named_adapter_results) == {"codex", "orca"},
            named_adapter_results,
        )
    )
    # Kimi retirement changes provider availability, not common-fence behavior.
    kimi_factory_rejected = False
    kimi_refusal_reason = ""
    try:
        executors.make_named_cli_agent(
            "kimi",
            execution_fence_port=fences.DisabledExecutionFencePort(),
        )
    except ValueError as exc:
        kimi_refusal_reason = str(exc)
        kimi_factory_rejected = "kimi_retired" in kimi_refusal_reason
    cases.append(
        _case(
            "retired-kimi-factory-refused-before-execution",
            kimi_factory_rejected,
            {"rejected": kimi_factory_rejected, "reason": kimi_refusal_reason},
        )
    )
    try:
        executors.make_named_cli_agent(
            "claude",
            execution_fence_port=fences.DisabledExecutionFencePort(),
        )
        retired_adapter_rejected = False
    except ValueError:
        retired_adapter_rejected = True
    cases.append(
        _case(
            "retired-provider-adapter-fails-closed",
            retired_adapter_rejected,
            {"rejected": retired_adapter_rejected},
        )
    )

    # Council annex 3: the "structurally cannot bypass" claim holds only if
    # every entry the production loop can select is mutation-marked.
    registry_marks: dict[str, Any] = {}
    for executor_name, factory in sorted(goal_loop_run.EXECUTORS.items()):
        build_kwargs: dict[str, Any] = {"timeout_seconds": 5}
        if executor_name == "orca":
            build_kwargs["orca_cli"] = "/bin/true"
            previous_orca_agent = os.environ.get("LH_ORCA_AGENT")
            os.environ["LH_ORCA_AGENT"] = "codex"
            try:
                built = factory(**build_kwargs)
            finally:
                if previous_orca_agent is None:
                    os.environ.pop("LH_ORCA_AGENT", None)
                else:
                    os.environ["LH_ORCA_AGENT"] = previous_orca_agent
        else:
            built = factory(**build_kwargs)
        registry_marks[executor_name] = {
            "requires_fence": fences.model_requires_fence(built),
            "identity": fences.model_fence_identity(built),
        }
    cases.append(
        _case(
            "every-registered-executor-factory-is-mutation-marked",
            set(registry_marks) == set(goal_loop_run.EXECUTORS)
            and all(item["requires_fence"] for item in registry_marks.values()),
            registry_marks,
        )
    )

    unsupported_binding = _binding(clone)
    unsupported_binding["adapter_id"] = "orca-codex"
    unsupported_launches = discovered.launch_count
    try:
        discovered.prepare(unsupported_binding)
        unsupported_reason = "unexpected_admission"
    except fences.ExecutionFenceUnavailable as exc:
        unsupported_reason = exc.reason
    cases.append(
        _case(
            "unsupported-orca-control-channel-is-not-admitted",
            unsupported_reason == "adapter_provider_channel_unsupported"
            and discovered.launch_count == unsupported_launches,
            {
                "reason": unsupported_reason,
                "provider_child_count": 0,
            },
        )
    )

    direct_marker = clone / "direct-adapter-marker"
    fake_adapter = clone / "fake-adapter"
    fake_adapter.write_text(
        "#!/bin/sh\nprintf invoked > direct-adapter-marker\n",
        encoding="utf-8",
    )
    fake_adapter.chmod(fake_adapter.stat().st_mode | stat.S_IXUSR)
    direct = executors.make_cli_agent(
        lambda _prompt: [str(fake_adapter)],
        name="fixture-direct",
        timeout_seconds=2,
        execution_fence_port=discovered,
    )
    try:
        direct(
            clone,
            {
                "run_id": "direct-run",
                "attempt": 1,
                "goal": {},
                "base_revision": "base",
            },
        )
        direct_reason = "unexpected_direct_launch"
    except fences.ExecutionFenceUnavailable as exc:
        direct_reason = exc.reason
    cases.append(
        _case(
            "direct-adapter-launch-without-controller-descriptor-is-rejected",
            direct_reason == "descriptor_missing"
            and not direct_marker.exists(),
            {
                "reason": direct_reason,
                "marker_exists": direct_marker.exists(),
            },
        )
    )

    # ---- sandbox launch classes (decision packet
    # docs/active/lh-auto-runner-gap-review-plan.md#lh-egress-mediation-decision-packet)
    bin_dir = root / "control-bin"
    bin_dir.mkdir()
    fake_orca = bin_dir / "fixture-orca"
    fake_orca.write_text('#!/bin/sh\necho \'{"ok": true, "result": {}}\'\n', encoding="utf-8")
    fake_orca.chmod(0o755)
    fake_provider = bin_dir / "fixturecodex"
    fake_provider.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_provider.chmod(0o755)
    # The profile pins its own bubblewrap by digest; a private copy lets the
    # drift case mutate the pinned binary without touching the system one.
    fixture_bwrap = bin_dir / "bwrap-fixture"
    shutil.copy2(discovered.bubblewrap_path, fixture_bwrap)
    fixture_home = root / "fixture-provider-home"
    fixture_home.mkdir()

    def _sandbox_profile_body(**overrides):
        # A hosted runner's python lives outside /usr (hostedtoolcache) and
        # needs its own prefix bound plus LD_LIBRARY_PATH surviving
        # --clearenv; on a plain host the prefix is /usr and the variable is
        # empty, so both additions are no-ops there.
        interpreter_prefix = str(Path(sys.executable).resolve().parents[1])
        body = {
            "bubblewrap": {
                "path": str(fixture_bwrap),
                "sha256": _digest(fixture_bwrap),
                "version": discovered.bubblewrap_version,
            },
            "network": "host",
            "ro_binds": [
                "/usr", "/bin", "/lib", "/lib64", "/etc",
                "/run/systemd/resolve", interpreter_prefix,
            ],
            "provider_home_ro_binds": {"fixturecodex": [str(fixture_home)]},
            "env": {
                "HOME": str(fixture_home),
                "LD_LIBRARY_PATH": os.environ.get("LD_LIBRARY_PATH", ""),
            },
            "flags": ["--die-with-parent", "--new-session"],
            "seccomp": {
                "default": "allow",
                "denied_action": "errno:EPERM",
                "denied_syscalls": [
                    "mount", "umount2", "pivot_root", "chroot", "ptrace",
                    "setns", "unshare", "open_by_handle_at",
                    "name_to_handle_at",
                ],
            },
        }
        body.update(overrides)
        return body

    def _policy_body(**overrides):
        body = {
            "schema": fences.EGRESS_POLICY_SCHEMA,
            "issuer": "host-owner",
            "enforced_by": fences.EGRESS_POLICY_ENFORCED_BY,
            "control_launch_budget_max": fences.CONTROL_LAUNCH_BUDGET,
            "orca_cli": {"path": str(fake_orca), "sha256": _digest(fake_orca)},
            "provider_sandbox_profile": _sandbox_profile_body(),
            "providers": {
                "fixturecodex": {
                    "path": str(fake_provider),
                    "sha256": _digest(fake_provider),
                    "flags": ["exec", "--json"],
                    "value_flags": ["-m"],
                    "prompt_flags": [],
                    "trailing_prompt": True,
                }
            },
        }
        body.update(overrides)
        return body

    policy_path = root / "egress-policy.json"
    policy_path.write_text(json.dumps(_policy_body()), encoding="utf-8")
    old_env = {k: os.environ.get(k) for k in ("LH_ORCA_CLI", "PATH", "LH_EGRESS_POLICY")}
    os.environ["LH_ORCA_CLI"] = str(fake_orca)
    os.environ["PATH"] = f"{bin_dir}:{old_env['PATH'] or ''}"
    os.environ["LH_EGRESS_POLICY"] = str(policy_path)
    try:
        host_binding = fences.build_attempt_binding(
            goal={"goal_id": "LH-EXAMPLE-GOAL-002", "node_id": "sandbox"},
            run_id="run-sandbox-fixture",
            attempt=1,
            attempt_fence=1,
            base_revision="2b5498e4d9c80abab62658f4c73415f3a48f97a4",
            clone_root=clone,
            verifier_argv=["python3", "-B", "lh_runtime/execution_fence_canary.py"],
            adapter_id="execution-host-port-fixturecodex",
            adapter_version="v1",
            timeout_seconds=60,
            now=FIXED_TIME,
            nonce="sandbox-controller-nonce-fixture",
        )
        host_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME,
        )
        alias_dir = root / "provider-alias"
        alias_dir.mkdir()
        provider_alias = alias_dir / "fixturecodex"
        provider_alias.symlink_to(fake_provider)
        canonical_policy = _policy_body()
        canonical_policy["providers"]["fixturecodex"]["path"] = str(
            fake_provider.resolve()
        )
        policy_path.write_text(json.dumps(canonical_policy), encoding="utf-8")
        prior_path = os.environ["PATH"]
        os.environ["PATH"] = f"{alias_dir}{os.pathsep}{prior_path}"
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            alias_reason = "admitted"
        except fences.ExecutionFenceUnavailable as exc:
            alias_reason = exc.reason
        finally:
            os.environ["PATH"] = prior_path
            policy_path.write_text(json.dumps(_policy_body()), encoding="utf-8")
        host_descriptor = host_fence.prepare(host_binding)
        egress_proof = host_descriptor["proofs"]["provider_control_egress"]
        cases.append(
            _case(
                "host-descriptor-declares-delegated-egress-not-admissible",
                alias_reason == "admitted"
                and host_descriptor["launch_classes"]
                == {"control": fences.CONTROL_LAUNCH_BUDGET, "mutation": 0}
                and egress_proof["result"] == "delegated_to_execution_host"
                and host_descriptor["proofs"]["filesystem_effect_containment"][
                    "result"
                ]
                == "admissible",
                {
                    "launch_classes": host_descriptor["launch_classes"],
                    "egress_result": egress_proof["result"],
                    "provider_alias": str(provider_alias),
                    "provider_canonical": str(fake_provider.resolve()),
                    "provider_alias_reason": alias_reason,
                },
            )
        )
        lied = json.loads(json.dumps(host_descriptor))
        lied["proofs"]["provider_control_egress"]["result"] = "admissible"
        try:
            host_fence.launch_control(
                lied, {"op": "repo_list"}, timeout_seconds=5
            )
            lie_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            lie_reason = exc.reason
        cases.append(
            _case(
                "rewriting-delegated-egress-to-admissible-breaks-the-digest",
                lie_reason == "descriptor_digest_invalid",
                {"reason": lie_reason},
            )
        )
        tampered = json.loads(json.dumps(host_descriptor))
        tampered["launch_classes"] = {"control": 999, "mutation": 1}
        try:
            host_fence.launch_control(
                tampered, {"op": "repo_list"}, timeout_seconds=5
            )
            tamper_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            tamper_reason = exc.reason
        cases.append(
            _case(
                "raising-your-own-launch-budget-breaks-the-digest",
                tamper_reason == "descriptor_digest_invalid",
                {"reason": tamper_reason},
            )
        )
        egress_bound = (host_descriptor.get("control_plane") or {}).get("egress_policy")
        expected_policy_digest = _digest(policy_path)
        projection_control = host_fence.receipt_projection(host_descriptor).get(
            "control_plane"
        ) or {}
        cases.append(
            _case(
                "descriptor-and-receipt-carry-the-policy-digest-and-its-enforcer",
                isinstance(egress_bound, dict)
                and egress_bound.get("digest") == expected_policy_digest
                and egress_bound.get("enforced_by") == "lh-client-preflight"
                and (projection_control.get("egress_policy") or {}).get("digest")
                == expected_policy_digest,
                {"egress_policy": egress_bound},
            )
        )
        good_create = {
            "op": "terminal_create",
            "provider_argv": [
                str(fake_provider), "exec", "--json", "-m", "model-x", "do the thing",
            ],
            "worktree_selector": f"path:{clone}",
            "title": "sandbox-fixture",
        }
        argv_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME
        )
        argv_descriptor = argv_fence.prepare(host_binding)
        try:
            argv_fence.launch_control(argv_descriptor, good_create, timeout_seconds=5)
            good_reason = "admitted"
        except fences.ExecutionFenceUnavailable as exc:
            good_reason = exc.reason
        evil_create = dict(good_create)
        evil_create["provider_argv"] = [str(fake_provider), "--evil-flag", "x", "prompt"]
        try:
            argv_fence.launch_control(argv_descriptor, evil_create, timeout_seconds=5)
            evil_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            evil_reason = exc.reason
        cases.append(
            _case(
                "provider-argv-outside-the-policy-is-refused-inside-it-admitted",
                good_reason == "admitted"
                and evil_reason == "control_provider_argv_outside_policy",
                {"good": good_reason, "evil": evil_reason},
            )
        )
        policy_path.write_text(json.dumps(_policy_body(providers={})), encoding="utf-8")
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            unlisted_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            unlisted_reason = exc.reason
        drifted = _policy_body()
        drifted["providers"]["fixturecodex"]["sha256"] = "sha256:" + "0" * 64
        policy_path.write_text(json.dumps(drifted), encoding="utf-8")
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            drift_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            drift_reason = exc.reason
        no_profile = _policy_body()
        del no_profile["provider_sandbox_profile"]
        policy_path.write_text(json.dumps(no_profile), encoding="utf-8")
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            no_profile_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            no_profile_reason = exc.reason
        foreign_profile = _policy_body(
            provider_sandbox_profile=_sandbox_profile_body(
                provider_home_ro_binds={"someone-else": []}
            )
        )
        policy_path.write_text(json.dumps(foreign_profile), encoding="utf-8")
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            foreign_profile_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            foreign_profile_reason = exc.reason
        policy_path.unlink()
        try:
            fences.LinuxBubblewrapExecutionFence.discover(
                clock=lambda: FIXED_TIME
            ).prepare(host_binding)
            missing_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            missing_reason = exc.reason
        cases.append(
            _case(
                "policy-outside-values-fail-closed-at-prepare",
                unlisted_reason == "egress_policy_provider_not_listed"
                and drift_reason == "egress_policy_provider_mismatch"
                and no_profile_reason == "egress_policy_sandbox_profile_missing"
                and foreign_profile_reason
                == "egress_policy_sandbox_profile_provider_missing"
                and missing_reason == "egress_policy_unreadable",
                {"unlisted": unlisted_reason, "drift": drift_reason,
                 "no_profile": no_profile_reason,
                 "foreign_profile": foreign_profile_reason,
                 "missing": missing_reason},
            )
        )
        policy_path.write_text(json.dumps(_policy_body()), encoding="utf-8")
        try:
            host_fence.launch(
                host_descriptor,
                ["/bin/true"],
                timeout_seconds=5,
            )
            class_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            class_reason = exc.reason
        cases.append(
            _case(
                "host-descriptor-cannot-run-a-mutation-launch",
                class_reason == "launch_class_not_authorized",
                {"reason": class_reason},
            )
        )
        try:
            host_fence.launch_control(
                host_descriptor,
                {"op": "system", "argv": ["/bin/sh", "-c", "id"]},
                timeout_seconds=5,
            )
            freeform_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            freeform_reason = exc.reason
        try:
            host_fence.launch_control(
                host_descriptor,
                {
                    "op": "terminal_create",
                    "worktree_selector": f"path:{clone}",
                    "title": "fixture",
                    "provider_argv": ["/bin/sh", "-c", "id"],
                    "output_path": str(clone / "out.log"),
                    "env_overlay": {},
                },
                timeout_seconds=5,
            )
            unpinned_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            unpinned_reason = exc.reason
        cases.append(
            _case(
                "free-form-and-unpinned-provider-control-requests-are-refused",
                freeform_reason == "control_op_unknown"
                and unpinned_reason == "control_provider_binary_unpinned",
                {"freeform": freeform_reason, "unpinned": unpinned_reason},
            )
        )
        admitted = host_fence.launch_control(
            host_descriptor,
            {
                "op": "terminal_create",
                "worktree_selector": f"path:{clone}",
                "title": "fixture",
                "provider_argv": [str(fake_provider), "exec", "--json"],
                "output_path": str(clone / "out.log"),
                "env_overlay": {"LH_STAGE_MARKER": "fixture"},
            },
            timeout_seconds=5,
        )
        used_before_exhaust = host_fence._control_counts[
            host_descriptor["launch_descriptor_digest"]
        ]
        exhaust_reason = "never_exhausted"
        for _ in range(fences.CONTROL_LAUNCH_BUDGET):
            try:
                host_fence.launch_control(
                    host_descriptor, {"op": "repo_list"}, timeout_seconds=5
                )
            except fences.ExecutionFenceUnavailable as exc:
                exhaust_reason = exc.reason
                break
        audit = host_fence.control_audit[
            host_descriptor["launch_descriptor_digest"]
        ]
        cases.append(
            _case(
                "pinned-terminal-create-is-admitted-then-the-budget-exhausts",
                admitted.returncode == 0
                and used_before_exhaust == 1
                and exhaust_reason == "descriptor_exhausted"
                and len(audit) == fences.CONTROL_LAUNCH_BUDGET
                and audit[0]["op"] == "terminal_create",
                {
                    "first_rc": admitted.returncode,
                    "exhaust_reason": exhaust_reason,
                    "audit_ops": [item["op"] for item in audit[:3]],
                    "audit_len": len(audit),
                },
            )
        )
        import os as _os
        distro = _os.environ.get("WSL_DISTRO_NAME", "Ubuntu")
        _os.environ.setdefault("WSL_DISTRO_NAME", distro)
        unc_selector = "path:" + "\\\\wsl.localhost\\" + distro + str(clone).replace("/", "\\")
        unc_fence = fences.LinuxBubblewrapExecutionFence.discover(clock=lambda: FIXED_TIME)
        unc_descriptor = unc_fence.prepare(host_binding)
        unc_ok = unc_fence.launch_control(
            unc_descriptor,
            {"op": "terminal_stop", "worktree_selector": unc_selector},
            timeout_seconds=5,
        )
        try:
            unc_fence.launch_control(
                unc_descriptor,
                {"op": "terminal_stop",
                 "worktree_selector": "path:\\\\wsl.localhost\\" + distro + "\\etc"},
                timeout_seconds=5,
            )
            foreign_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            foreign_reason = exc.reason
        cases.append(
            _case(
                "unc-selector-for-the-clone-is-accepted-and-a-foreign-one-refused",
                unc_ok.returncode == 0 and foreign_reason == "control_selector_invalid",
                {"unc_rc": unc_ok.returncode, "foreign": foreign_reason},
            )
        )
        fresh_plain = host_fence.prepare(_binding(clone))
        try:
            host_fence.launch_control(
                fresh_plain, {"op": "repo_list"}, timeout_seconds=5
            )
            plain_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            plain_reason = exc.reason
        cases.append(
            _case(
                "plain-mutation-descriptor-cannot-run-a-control-launch",
                plain_reason == "launch_class_not_authorized",
                {"reason": plain_reason},
            )
        )

        # ---- lh-provider-sandbox (packet §7 v2, exam cases a-e)
        plane_sandbox = (host_descriptor.get("control_plane") or {}).get(
            "provider_sandbox"
        ) or {}
        sandbox_proof = host_descriptor["proofs"].get("provider_sandbox") or {}
        projection_sandbox = (
            host_fence.receipt_projection(host_descriptor).get("control_plane")
            or {}
        ).get("provider_sandbox") or {}
        lied_sandbox = json.loads(json.dumps(host_descriptor))
        lied_sandbox["proofs"]["provider_sandbox"]["result"] = (
            "delegated_to_execution_host"
        )
        try:
            host_fence.launch_control(
                lied_sandbox, {"op": "repo_list"}, timeout_seconds=5
            )
            sandbox_lie_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            sandbox_lie_reason = exc.reason
        cases.append(
            _case(
                "provider-sandbox-track-is-applied-composed-and-unforgeable",
                sandbox_proof.get("result") == "applied"
                and sandbox_proof.get("enforced_by") == "lh-client-composed"
                and sandbox_proof.get("profile_digest")
                == plane_sandbox.get("profile_digest")
                and plane_sandbox.get("profile_digest")
                == fences.digest_json(plane_sandbox.get("profile"))
                and projection_sandbox.get("profile_digest")
                == plane_sandbox.get("profile_digest")
                and json.dumps(projection_sandbox).find(str(fixture_home)) < 0
                and sandbox_lie_reason == "descriptor_digest_invalid",
                {
                    "proof": {k: sandbox_proof.get(k) for k in (
                        "result", "enforced_by", "profile_digest")},
                    "lie": sandbox_lie_reason,
                },
            )
        )

        create_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME
        )
        create_descriptor = create_fence.prepare(host_binding)
        create_profile = (
            create_descriptor["control_plane"]["provider_sandbox"]["profile"]
        )
        create_argv = [
            str(fake_provider), "exec", "--json", "sandboxed prompt",
        ]
        admitted_create = create_fence.launch_control(
            create_descriptor,
            {
                "op": "terminal_create",
                "worktree_selector": f"path:{clone}",
                "title": "sandbox-fixture",
                "provider_argv": create_argv,
                "output_path": str(clone / "sandbox-out.log"),
                "env_overlay": {"LH_STAGE_MARKER": "sandbox"},
            },
            timeout_seconds=5,
        )
        create_audit = create_fence.control_audit[
            create_descriptor["launch_descriptor_digest"]
        ][-1]
        program_path = Path(str(create_audit.get("seccomp_program_path") or ""))
        expected_command = fences.compose_terminal_command(
            create_argv,
            output_path=str(clone / "sandbox-out.log"),
            env_overlay={"LH_STAGE_MARKER": "sandbox"},
            sandbox=create_profile,
            seccomp_program_path=str(program_path),
            clone_root=str(clone),
        )
        expected_digest = "sha256:" + hashlib.sha256(
            expected_command.encode()
        ).hexdigest()
        cases.append(
            _case(
                "terminal-create-command-digest-equals-the-descriptor-derived-one",
                admitted_create.returncode == 0
                and create_audit.get("command_digest") == expected_digest
                and create_audit.get("provider_sandbox_profile_digest")
                == fences.digest_json(create_profile)
                and program_path.is_file()
                and create_audit.get("seccomp_program_scope")
                == "task_temp_outside_clone"
                and not str(program_path).startswith(str(clone) + os.sep)
                and create_audit.get("seccomp_program_sha256")
                == _digest(program_path)
                and str(fixture_bwrap) in expected_command
                and "--die-with-parent" in expected_command
                and "--unshare-net" not in expected_command,
                {
                    "rc": admitted_create.returncode,
                    "audit_digest": create_audit.get("command_digest"),
                    "expected_digest": expected_digest,
                    "program_path": str(program_path),
                },
            )
        )
        create_fence._cleanup_provider_seccomp(
            create_descriptor["launch_descriptor_digest"]
        )

        sandbox_probe = clone / "sandbox-probe.py"
        sandbox_probe.write_text(
            "import json, os, socket, sys\n"
            "out = {}\n"
            "with open(sys.argv[1], 'w') as h:\n"
            "    h.write('inside-ok')\n"
            "out['inside'] = 'ok'\n"
            "for name, target in (('outside', sys.argv[2]),"
            " ('mnt_c', '/mnt/c/lh-sandbox-canary-probe'),"
            " ('symlink', sys.argv[3])):\n"
            "    try:\n"
            "        open(target, 'w').write('escape')\n"
            "        out[name] = 'wrote'\n"
            "    except OSError as exc:\n"
            "        out[name] = exc.errno\n"
            "try:\n"
            "    handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
            "    handle.close()\n"
            "    out['socket'] = 'ok'\n"
            "except OSError as exc:\n"
            "    out['socket'] = exc.errno\n"
            "try:\n"
            "    os.unshare(os.CLONE_NEWUTS)\n"
            "    out['unshare'] = 'ok'\n"
            "except OSError as exc:\n"
            "    out['unshare'] = exc.errno\n"
            "print(json.dumps(out))\n",
            encoding="utf-8",
        )
        escape2 = clone / "escape2-link"
        if not escape2.exists():
            escape2.symlink_to(outside["home"])
        probe_program = create_fence._provider_seccomp_program(
            create_profile["seccomp"]
        )
        probe_program_path = clone / ".sandbox-probe-seccomp.bpf"
        probe_program_path.write_bytes(probe_program)
        # The escape target must live outside /tmp: the profile synthesizes a
        # private tmpfs there, so a /tmp path is shadowed (host untouched)
        # rather than refused, and the exam wants the refusal errno forms.
        outside_escape = "/home/lh-sandbox-canary-escape.txt"
        probe_command = fences.compose_terminal_command(
            [
                sys.executable,
                str(sandbox_probe),
                str(clone / "inside-ok.txt"),
                outside_escape,
                str(escape2),
            ],
            sandbox=create_profile,
            seccomp_program_path=str(probe_program_path),
            clone_root=str(clone),
        )
        probe_run = subprocess.run(
            ["bash", "-c", probe_command],
            capture_output=True,
            text=True,
            timeout=30,
        )
        try:
            probe_out = json.loads(probe_run.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            probe_out = {"decode_error": probe_run.stdout,
                         "stderr": probe_run.stderr}
        denied_forms = {errno.EPERM, errno.ENOENT}
        cases.append(
            _case(
                "sandboxed-provider-keeps-network-loses-everything-outside-workspace",
                probe_run.returncode == 0
                and probe_out.get("inside") == "ok"
                and probe_out.get("outside") in denied_forms
                and probe_out.get("mnt_c") in denied_forms
                and probe_out.get("symlink") in denied_forms
                and probe_out.get("socket") == "ok"
                and probe_out.get("unshare") == errno.EPERM
                and not Path(outside_escape).exists(),
                {"probe": probe_out, "stderr": probe_run.stderr[-400:]},
            )
        )

        stale_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME
        )
        stale_descriptor = stale_fence.prepare(host_binding)
        stale_digest = (
            stale_descriptor["control_plane"]["provider_sandbox"][
                "profile_digest"
            ]
        )
        policy_path.write_text(
            json.dumps(_policy_body(
                provider_sandbox_profile=_sandbox_profile_body(
                    ro_binds=["/usr"]
                )
            )),
            encoding="utf-8",
        )
        stale_admitted = stale_fence.launch_control(
            stale_descriptor,
            {
                "op": "terminal_create",
                "worktree_selector": f"path:{clone}",
                "title": "stale-policy-fixture",
                "provider_argv": [str(fake_provider), "exec", "--json"],
                "output_path": str(clone / "stale-out.log"),
                "env_overlay": {},
            },
            timeout_seconds=5,
        )
        stale_audit = stale_fence.control_audit[
            stale_descriptor["launch_descriptor_digest"]
        ][-1]
        policy_path.write_text(json.dumps(_policy_body()), encoding="utf-8")
        cases.append(
            _case(
                "policy-rewritten-after-prepare-never-reaches-the-launch",
                stale_admitted.returncode == 0
                and stale_audit.get("provider_sandbox_profile_digest")
                == stale_digest,
                {
                    "rc": stale_admitted.returncode,
                    "audit_digest": stale_audit.get(
                        "provider_sandbox_profile_digest"
                    ),
                },
            )
        )

        tamper_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME
        )
        tamper_base = tamper_fence.prepare(host_binding)
        unfenced = json.loads(json.dumps(tamper_base))
        unfenced["control_plane"]["provider_sandbox"]["profile"]["flags"] = [
            "--new-session"
        ]
        try:
            tamper_fence.launch_control(
                unfenced, {"op": "repo_list"}, timeout_seconds=5
            )
            die_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            die_reason = exc.reason
        netted = json.loads(json.dumps(tamper_base))
        netted["control_plane"]["provider_sandbox"]["profile"]["namespaces"][
            "network"
        ] = "new"
        try:
            tamper_fence.launch_control(
                netted, {"op": "repo_list"}, timeout_seconds=5
            )
            net_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            net_reason = exc.reason
        cases.append(
            _case(
                "stripping-die-with-parent-or-adding-unshare-net-breaks-the-digest",
                die_reason == "descriptor_digest_invalid"
                and net_reason == "descriptor_digest_invalid",
                {"die_with_parent": die_reason, "unshare_net": net_reason},
            )
        )

        drift_fence = fences.LinuxBubblewrapExecutionFence.discover(
            clock=lambda: FIXED_TIME
        )
        drift_descriptor = drift_fence.prepare(host_binding)
        with open(fixture_bwrap, "ab") as handle:
            handle.write(b"\n# drifted\n")
        try:
            drift_fence.launch_control(
                drift_descriptor,
                {
                    "op": "terminal_create",
                    "worktree_selector": f"path:{clone}",
                    "title": "drift-fixture",
                    "provider_argv": [str(fake_provider), "exec", "--json"],
                    "output_path": str(clone / "drift-out.log"),
                    "env_overlay": {},
                },
                timeout_seconds=5,
            )
            bwrap_drift_reason = "unexpected_admission"
        except fences.ExecutionFenceUnavailable as exc:
            bwrap_drift_reason = exc.reason
        shutil.copy2(discovered.bubblewrap_path, fixture_bwrap)
        cases.append(
            _case(
                "pinned-provider-bwrap-drift-refuses-the-launch",
                bwrap_drift_reason == "provider_sandbox_bwrap_drifted",
                {"reason": bwrap_drift_reason},
            )
        )
    finally:
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    failures = [
        {"id": item["id"], "detail": item["detail"]}
        for item in cases
        if not item["ok"]
    ]
    payload = {
        "check_id": "lh-preventive-execution-fence-n03",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "evidence": {
            "base_revision": "2b5498e4d9c80abab62658f4c73415f3a48f97a4",
            "backend_id": fences.BACKEND_ID,
            "backend_version": discovered.bubblewrap_version,
            "launch_descriptor_digest": descriptor[
                "launch_descriptor_digest"
            ],
            "binding_digest": descriptor["binding_digest"],
            "proofs_digest": descriptor["proofs_digest"],
            "proof_tracks": {
                name: fences.digest_json(proof)
                for name, proof in sorted(proofs.items())
            },
            "mount_policy_digest": projection["backend"][
                "mount_policy_digest"
            ],
            "syscall_policy_digest": projection["backend"][
                "syscall_policy_digest"
            ],
            "namespace_policy_digest": projection["backend"][
                "namespace_policy_digest"
            ],
            "sentinel_digests_before": before,
            "sentinel_digests_after": after,
        },
        "provider_invocations": 0,
        "fixture_processes": discovered.launch_count,
        "verification": {
            "command": "python3 -B lh_runtime/execution_fence_canary.py",
            "authority": "docs/contracts/goal-lifecycle-v1.md#lh-preventive-execution-fence-003",
        },
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.artifact_out:
        output = Path(args.artifact_out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    shutil.rmtree(root.parent, ignore_errors=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

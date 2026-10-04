#!/usr/bin/env python3
"""Offline positive/negative canary for the preventive execution boundary."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import socket
import stat
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
    cases.append(
        _case(
            "all-production-mutation-adapters-use-the-common-fence",
            all(
                reason == "descriptor_missing"
                for reason in named_adapter_results.values()
            )
            and set(named_adapter_results) == {"codex"},
            named_adapter_results,
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

#!/usr/bin/env python3
"""Cross-platform execution-fence selector and Windows protocol canary.

The Windows positive arm is deliberately a helper-protocol fixture, not a
live AppContainer claim.  It proves the controller's admission contract:
helper version, capability attestation, descriptor/proof digests, and one
provider-child result are all required.  Native Windows installation and
kernel readback remain platform acceptance work.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import execution_fence as fences  # noqa: E402
from execution_fence_windows import (  # noqa: E402
    WINDOWS_ATTESTATION_SCHEMA,
    WINDOWS_BACKEND_ID,
    WINDOWS_BACKEND_VERSION,
    WINDOWS_HELPER_ENV,
    WINDOWS_HELPER_PROTOCOL,
    WindowsNativeExecutionFence,
)


HELPER = r'''#!/usr/bin/env python3
import json, os, sys

BACKEND = "windows-native-appcontainer-job"
VERSION = "1.0"
PROTOCOL = "lh-windows-execution-fence-helper/v1"
ATTESTATION = "lh-windows-execution-fence-attestation/v1"
if len(sys.argv) == 2 and sys.argv[1] == "--version":
    print("LH Windows Execution Fence Helper " + VERSION)
    raise SystemExit(0)
request = json.loads(sys.stdin.read())
count = os.environ.get("LH_FAKE_WINDOWS_FENCE_COUNT")
if count:
    with open(count, "a", encoding="utf-8") as handle:
        handle.write(request.get("op", "unknown") + "\n")
if request.get("op") == "prepare":
    binding = request.get("binding") or {}
    capabilities = {
        "filesystem_effect_containment": "admissible",
        "provider_control_egress": "admissible",
        "provider_sandbox": "not_applicable",
        "network": "denied",
        "process_control": "denied",
        "write_roots": list(binding.get("allowed_write_roots") or []),
    }
    if os.environ.get("LH_FAKE_WINDOWS_FENCE_MODE") == "missing-proof":
        capabilities.pop("provider_control_egress", None)
    print(json.dumps({
        "schema": ATTESTATION,
        "protocol": PROTOCOL,
        "status": "admitted",
        "backend_id": BACKEND,
        "backend_version": VERSION,
        "proof_tracks": [
            "filesystem_effect_containment",
            "provider_control_egress",
            "provider_sandbox",
        ],
        "capabilities": capabilities,
    }, sort_keys=True))
elif request.get("op") == "launch":
    print(json.dumps({
        "status": "completed",
        "descriptor_digest": request.get("descriptor_digest"),
        "proofs_digest": (request.get("descriptor") or {}).get("proofs_digest"),
        "provider_child_count": 1,
        "result": {"returncode": 0, "stdout": "windows-fixture\n", "stderr": ""},
    }, sort_keys=True))
else:
    raise SystemExit(2)
'''


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _binding(clone: Path) -> dict[str, Any]:
    return fences.build_attempt_binding(
        goal={"goal_id": "p4-platform-canary", "node_id": "P4"},
        run_id="p4-platform-run",
        attempt=1,
        attempt_fence=1,
        base_revision="fixture-base",
        clone_root=clone,
        verifier_argv=["python3", "-B", "fixture-check.py"],
        adapter_id="fixture-mutation-adapter",
        adapter_version="v1",
        timeout_seconds=30,
    )


def _count_lines(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]


def main() -> int:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-platform-fence-") as raw:
        root = Path(raw)
        clone = root / "clone"
        clone.mkdir()
        helper = root / "windows-fence-helper.py"
        helper.write_text(HELPER, encoding="utf-8")
        helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
        count = root / "helper-ops.log"
        old_env = {
            WINDOWS_HELPER_ENV: os.environ.get(WINDOWS_HELPER_ENV),
            "LH_FAKE_WINDOWS_FENCE_COUNT": os.environ.get("LH_FAKE_WINDOWS_FENCE_COUNT"),
            "LH_FAKE_WINDOWS_FENCE_MODE": os.environ.get("LH_FAKE_WINDOWS_FENCE_MODE"),
        }
        try:
            os.environ[WINDOWS_HELPER_ENV] = str(helper)
            os.environ["LH_FAKE_WINDOWS_FENCE_COUNT"] = str(count)
            os.environ.pop("LH_FAKE_WINDOWS_FENCE_MODE", None)

            missing_env = dict(os.environ)
            missing_env.pop(WINDOWS_HELPER_ENV, None)
            missing = fences.configured_execution_fence(
                {"LH_EXECUTION_FENCE_BACKEND": WINDOWS_BACKEND_ID},
                # A configured Windows backend without its native helper must
                # stay unavailable before any provider child is considered.
                # The fake helper is restored below for the positive protocol
                # arm.
                platform_name="windows",
            )
            cases.append(_case(
                "windows-selector-without-helper-fails-closed",
                isinstance(missing, fences.DisabledExecutionFencePort)
                and missing.reason == "windows_helper_missing",
                {"type": type(missing).__name__, "reason": getattr(missing, "reason", None)},
            ))

            mac = fences.configured_execution_fence(
                {"LH_EXECUTION_FENCE_BACKEND": "macos-native"},
                platform_name="darwin",
            )
            cases.append(_case(
                "macos-without-equivalent-backend-fails-closed",
                isinstance(mac, fences.DisabledExecutionFencePort)
                and mac.reason == "macos_execution_fence_unsupported",
                {"type": type(mac).__name__, "reason": getattr(mac, "reason", None)},
            ))

            mismatch = fences.configured_execution_fence(
                {"LH_EXECUTION_FENCE_BACKEND": fences.BACKEND_ID},
                platform_name="windows",
            )
            cases.append(_case(
                "linux-backend-is-not-relabelled-as-windows-support",
                isinstance(mismatch, fences.DisabledExecutionFencePort)
                and mismatch.reason == "backend_platform_mismatch",
                {"type": type(mismatch).__name__, "reason": getattr(mismatch, "reason", None)},
            ))

            binding = _binding(clone)
            port = WindowsNativeExecutionFence.discover(
                environ=os.environ,
                platform_name="windows",
            )
            descriptor = port.prepare(binding)
            receipt = port.receipt_projection(descriptor)
            proofs = descriptor["proofs"]
            cases.append(_case(
                "helper-attestation-binds-both-required-proof-tracks",
                set(proofs) == set(fences.REQUIRED_PROOF_TRACKS)
                and proofs["filesystem_effect_containment"]["result"] == "admissible"
                and proofs["provider_control_egress"]["result"] == "admissible"
                and proofs["provider_sandbox"]["result"] == "not_applicable"
                and receipt["backend"]["backend_id"] == WINDOWS_BACKEND_ID
                and receipt["backend"]["attestation_digest"]
                == fences.digest_json(descriptor["helper_attestation"]),
                {"backend": receipt["backend"], "proofs": sorted(proofs)},
            ))

            completed = port.launch(
                descriptor,
                [sys.executable, "-c", "print('fixture')"],
                input_text="attempt-bound\n",
                timeout_seconds=5,
            )
            cases.append(_case(
                "admitted-launch-returns-one-bound-provider-result",
                completed.returncode == 0
                and completed.stdout == "windows-fixture\n",
                {"returncode": completed.returncode, "stdout": completed.stdout},
            ))
            try:
                port.launch(descriptor, [sys.executable, "-c", "pass"], timeout_seconds=5)
                replay_reason = "unexpected_replay"
            except fences.ExecutionFenceUnavailable as exc:
                replay_reason = exc.reason
            cases.append(_case(
                "descriptor-replay-does-not-request-another-provider",
                replay_reason == "descriptor_replayed"
                and _count_lines(count).count("launch") == 1,
                {"reason": replay_reason, "helper_ops": _count_lines(count)},
            ))

            os.environ["LH_FAKE_WINDOWS_FENCE_MODE"] = "missing-proof"
            blocked = WindowsNativeExecutionFence.discover(
                environ=os.environ,
                platform_name="windows",
            )
            try:
                blocked.prepare(binding)
                proof_reason = "unexpected_admission"
            except fences.ExecutionFenceUnavailable as exc:
                proof_reason = exc.reason
            cases.append(_case(
                "missing-proof-stops-before-provider-launch",
                proof_reason == "windows_proof_missing"
                and _count_lines(count).count("launch") == 1,
                {"reason": proof_reason, "helper_ops": _count_lines(count)},
            ))
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
        "check_id": "lh-cross-platform-execution-fence-p4",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "cases": cases,
        "blocking_failures": failures,
        "native_platform_acceptance": {
            "windows": "not_run_in_this_linux_host",
            "macos": "unsupported_fail_closed",
        },
        "verification": {
            "command": "python3 -B lh_runtime/execution_fence_platform_canary.py",
            "authority": "docs/codex-handoff/lh-host-productization-plan.md#p4-cross-platform-execution-fence",
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

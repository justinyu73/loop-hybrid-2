#!/usr/bin/env python3
"""The local-process fence backend: owned process groups, honest receipts.

Platform-neutral.  Every case runs a real child through the explicit
``local-process`` backend in a disposable directory and asserts what the port
promises: descriptors are single-use and digest-bound, the child runs in the
clone with the caller's input, a deadline ends the whole process group, and the
receipt never claims containment this backend does not provide.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import execution_fence as fences  # noqa: E402

CHECK_ID = "lh-local-process-fence"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _binding(clone: Path, *, timeout_seconds: float = 60) -> dict[str, Any]:
    return fences.build_attempt_binding(
        goal={"goal_id": "local-process-canary"}, run_id="run-local", attempt=1, attempt_fence=1,
        base_revision="0" * 40, clone_root=clone, verifier_argv=[sys.executable],
        adapter_id="declared-fixture", adapter_version="v1", timeout_seconds=timeout_seconds)


def _port() -> fences.ExecutionFencePort:
    return fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "local-process"})


def _refusal(action) -> str | None:
    try:
        action()
    except fences.ExecutionFenceUnavailable as exc:
        return exc.reason
    return None


def selector_case() -> dict[str, Any]:
    chosen = type(_port()).__name__
    unset = fences.configured_execution_fence({})
    unknown = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "kernel-sandbox"})
    ok = (chosen == "LocalProcessExecutionFence"
          and isinstance(unset, fences.DisabledExecutionFencePort)
          and isinstance(unknown, fences.DisabledExecutionFencePort)
          and unknown.reason == "backend_configuration_invalid")
    return case("backend-is-explicit-and-unknown-names-stay-disabled", ok,
                {"local": chosen, "unset": type(unset).__name__, "unknown": getattr(unknown, "reason", None)})


def run_case(clone: Path) -> dict[str, Any]:
    port = _port()
    descriptor = port.prepare(_binding(clone))
    script = ("import os, pathlib, sys; data = sys.stdin.read(); "
              "pathlib.Path('written.txt').write_text(data, encoding='utf-8'); print(os.getcwd())")
    proc = port.launch(descriptor, [sys.executable, "-c", script], input_text="stdin-ok", timeout_seconds=60)
    receipt = port.receipt_projection(descriptor)
    written = (clone / "written.txt").read_text(encoding="utf-8") if (clone / "written.txt").is_file() else None
    results = {track: (receipt.get("proofs") or {}).get(track, {}).get("result") for track in fences.REQUIRED_PROOF_TRACKS}
    ok = (proc.returncode == 0 and Path(proc.stdout.strip()).resolve() == clone and written == "stdin-ok"
          and receipt.get("kernel_containment") is False and receipt.get("provider_egress_enforced") is False
          and set(results.values()) == {"not_contained"}
          and receipt.get("launch_classes") == {"mutation": 1}
          and receipt.get("process_lifecycle_outcome") == "exited")
    return case("child-runs-in-the-clone-and-the-receipt-claims-no-containment", ok,
                {"returncode": proc.returncode, "cwd": proc.stdout.strip(), "written": written,
                 "proofs": results, "kernel_containment": receipt.get("kernel_containment"),
                 "outcome": receipt.get("process_lifecycle_outcome")})


def nonzero_case(clone: Path) -> dict[str, Any]:
    port = _port()
    descriptor = port.prepare(_binding(clone))
    proc = port.launch(descriptor, [sys.executable, "-c", "import sys; sys.exit(7)"], timeout_seconds=60)
    return case("nonzero-exit-is-returned-not-raised", proc.returncode == 7, {"returncode": proc.returncode})


def refusal_case(clone: Path) -> dict[str, Any]:
    port = _port()
    used = port.prepare(_binding(clone))
    port.launch(used, [sys.executable, "-c", "pass"], timeout_seconds=60)
    tampered = json.loads(json.dumps(port.prepare(_binding(clone))))
    tampered["launch_classes"] = {"mutation": 2}
    foreign = _port().prepare(_binding(clone))
    late_port = _port()
    late = late_port.prepare(_binding(clone, timeout_seconds=1))
    late_port.clock = lambda: time.time() + 3600
    refusals = {
        "replayed": _refusal(lambda: port.launch(used, [sys.executable, "-c", "pass"], timeout_seconds=60)),
        "tampered": _refusal(lambda: port.launch(tampered, [sys.executable, "-c", "pass"], timeout_seconds=60)),
        "foreign": _refusal(lambda: port.launch(foreign, [sys.executable, "-c", "pass"], timeout_seconds=60)),
        "expired": _refusal(lambda: late_port.launch(late, [sys.executable, "-c", "pass"], timeout_seconds=60)),
    }
    expected = {"replayed": "descriptor_replayed", "tampered": "descriptor_digest_invalid",
                "foreign": "descriptor_not_prepared", "expired": "descriptor_expired"}
    return case("descriptors-are-single-use-digest-bound-and-expire", refusals == expected, refusals)


def timeout_case(clone: Path) -> dict[str, Any]:
    port = _port()
    descriptor = port.prepare(_binding(clone))
    marker = clone / "child-survived.txt"
    script = ("import subprocess, sys, time; "
              "subprocess.Popen([sys.executable, '-c', "
              f"\"import time, pathlib; time.sleep(4); pathlib.Path({str(marker)!r}).write_text('x')\"]); "
              "time.sleep(30)")
    started = time.monotonic()
    timed_out = False
    try:
        port.launch(descriptor, [sys.executable, "-c", script], timeout_seconds=1.5)
    except subprocess.TimeoutExpired:
        timed_out = True
    elapsed = time.monotonic() - started
    time.sleep(5)
    receipt = port.receipt_projection(descriptor)
    ok = (timed_out and elapsed < 20 and not marker.exists()
          and receipt.get("process_lifecycle_outcome") == "timed_out")
    return case("deadline-ends-the-whole-process-group", ok,
                {"timed_out": timed_out, "elapsed": round(elapsed, 2), "grandchild_wrote": marker.exists(),
                 "outcome": receipt.get("process_lifecycle_outcome")})


def main() -> int:
    cases: list[dict[str, Any]] = []
    for build in (selector_case,):
        try:
            cases.append(build())
        except Exception as exc:  # A crash is a failed exam, never a skipped one.
            cases.append(case(build.__name__.replace("_", "-"), False, f"{type(exc).__name__}: {exc}"))
    for build in (run_case, nonzero_case, refusal_case, timeout_case):
        with tempfile.TemporaryDirectory(prefix="lh-local-process-") as raw:
            try:
                cases.append(build(Path(raw).resolve()))
            except Exception as exc:
                cases.append(case(build.__name__.replace("_", "-"), False, f"{type(exc).__name__}: {exc}"))
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
        "known_gaps_open": [
            "local-process contains nothing: the child sees the operator's environment, network, and files",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

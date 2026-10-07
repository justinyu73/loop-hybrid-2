#!/usr/bin/env python3
"""Fence conformance: measure what an execution fence backend claims about isolation.

Every fence receipt states two claims: ``kernel_containment`` (the child cannot
write or read outside the allowed roots) and ``provider_egress_enforced`` (the
child cannot open outbound connections).  This kit drives a backend the way the
engine's command boundary does (``prepare`` -> ``project_environment`` ->
``project_command`` -> ``launch`` -> ``receipt_projection``) and probes:

- the descriptor binds the given binding; an edited or replayed descriptor is refused;
- a write and a read outside the allowed roots, and a connection to a loopback
  listener the kit itself opens (nothing leaves the machine);
- a secret in the caller's environment mapping never reaches the child;
- the deadline ends a child that overruns it, and output stays bounded;
- a backend that offers started notifications delivers a real process.

A probe fails when measurement contradicts the claim: a backend claiming
containment that lets the write through is RED, and so is any backend that
forwards the caller's environment.  A backend that claims nothing and contains
nothing is consistent.

    python3 gate-pack/fence_conformance/conformance.py --backend execution_fence_local:LocalProcessExecutionFence
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lh_runtime"))

from execution_fence import build_attempt_binding  # noqa: E402

SCHEMA = "lh-fence-conformance/v1"
OUTPUT_LIMIT_BYTES = 1048576
OUTPUT_SLACK = 1.1


def _copy(value: Any) -> Any:
    return json.loads(json.dumps(value))


class _Probe:
    def __init__(self, factory: Callable[[], Any], root: Path):
        self.port = factory()
        self.root = root
        self.clone = (root / "clone")
        self.clone.mkdir(parents=True)
        self.clone = self.clone.resolve()
        self.outside = (root / "outside")
        self.outside.mkdir()
        self.outside = self.outside.resolve()

    def descriptor(self, *, timeout_seconds: float = 60) -> dict[str, Any]:
        binding = build_attempt_binding(
            goal={"goal_id": "fence-conformance", "goal_revision": 1}, run_id="fence-conformance", attempt=1,
            attempt_fence=1, base_revision="0" * 40, clone_root=self.clone, verifier_argv=[sys.executable],
            adapter_id="fence-conformance", adapter_version="v1", timeout_seconds=timeout_seconds,
            allowed_read_roots=[], allowed_write_roots=[self.clone], allowed_local_effects=["workspace_write"],
            execution_context_digest="sha256:" + "c" * 64)  # explicit roots are a v2 binding
        descriptor = self.port.prepare(binding)
        self.last_binding = binding
        return descriptor

    def launch(self, descriptor: dict[str, Any], code: str, *, timeout_seconds: float = 30,
               environment: dict[str, str] | None = None, on_started: Callable[[Any], None] | None = None):
        projected = self.port.project_environment(descriptor, environment or {})
        argv = self.port.project_command(descriptor, [sys.executable, "-c", code], environment or {})
        return self.port.launch(descriptor, argv, timeout_seconds=timeout_seconds, env_projection=projected,
                                on_started=on_started)

    def claims(self, descriptor: dict[str, Any]) -> dict[str, bool]:
        receipt = self.port.receipt_projection(descriptor)
        return {key: receipt.get(key) is True for key in ("kernel_containment", "provider_egress_enforced")}


def _row(claimed: Any, observed: Any, ok: bool, detail: Any = None) -> dict[str, Any]:
    return {"claimed": claimed, "observed": observed, "ok": bool(ok), "detail": detail}


def _refused(action: Callable[[], Any]) -> tuple[bool, str]:
    try:
        action()
        return False, "accepted"
    except Exception as exc:  # any refusal counts; the kind is reported
        return True, type(exc).__name__


def check_backend(factory: Callable[[], Any], root: str | Path) -> dict[str, Any]:
    probe = _Probe(factory, Path(root))
    probes: dict[str, dict[str, Any]] = {}

    first = probe.descriptor()
    probes["binding_round_trip"] = _row(None, first.get("binding") == probe.last_binding,
                                        first.get("binding") == probe.last_binding)
    claims = probe.claims(first)

    marker = probe.outside / "tampered-ran.txt"
    edited = _copy(probe.descriptor())
    edited["binding"]["allowed_write_roots"] = [*edited["binding"].get("allowed_write_roots", []), str(probe.outside)]
    refused, kind = _refused(lambda: probe.launch(edited, f"open({str(marker)!r}, 'w').write('x')"))
    probes["tampered_descriptor_refused"] = _row(None, refused, refused and not marker.exists(), kind)

    replay = probe.descriptor()
    probe.launch(replay, "pass")
    refused, kind = _refused(lambda: probe.launch(replay, "pass"))
    probes["replayed_descriptor_refused"] = _row(None, refused, refused, kind)

    target = probe.outside / "written.txt"
    probe.launch(probe.descriptor(), f"try:\n    open({str(target)!r}, 'w').write('x')\nexcept Exception:\n    pass")
    wrote = target.exists()
    probes["write_outside"] = _row(claims["kernel_containment"], wrote, not (claims["kernel_containment"] and wrote))

    secret_value = "fence-conformance-" + os.urandom(8).hex()
    secret = probe.outside / "secret.txt"
    secret.write_text(secret_value, encoding="utf-8")
    read = probe.launch(probe.descriptor(), f"try:\n    print(open({str(secret)!r}).read())\nexcept Exception:\n    pass")
    leaked = secret_value in (read.stdout or "")
    probes["read_outside"] = _row(claims["kernel_containment"], leaked, not (claims["kernel_containment"] and leaked))

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    accepted: list[bool] = []

    def accept() -> None:
        try:
            connection, _address = listener.accept()
            accepted.append(True)
            connection.close()
        except OSError:
            pass
    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    port_number = listener.getsockname()[1]
    probe.launch(probe.descriptor(), "import socket\ntry:\n"
                 f"    socket.create_connection(('127.0.0.1', {port_number}), timeout=5).sendall(b'x')\n"
                 "except Exception:\n    pass")
    thread.join(timeout=10)
    listener.close()
    connected = bool(accepted)
    probes["egress"] = _row(claims["provider_egress_enforced"], connected,
                            not (claims["provider_egress_enforced"] and connected))

    token = "fence-conformance-caller-" + os.urandom(8).hex()
    shown = probe.launch(probe.descriptor(), "import os\nprint(os.environ.get('LH_FENCE_PROBE_CALLER', ''))",
                         environment={"LH_FENCE_PROBE_CALLER": token, "LANG": "C"})
    forwarded = token in (shown.stdout or "")
    probes["caller_environment_not_forwarded"] = _row(None, forwarded, not forwarded)

    started = time.monotonic()
    ended, kind = _refused(lambda: probe.launch(probe.descriptor(timeout_seconds=60), "import time\ntime.sleep(30)",
                                                timeout_seconds=1))
    elapsed = round(time.monotonic() - started, 2)
    probes["deadline_enforced"] = _row(None, {"ended": ended, "seconds": elapsed}, ended and elapsed < 15, kind)

    try:
        loud = probe.launch(probe.descriptor(), f"import sys\nsys.stdout.write('x' * {3 * OUTPUT_LIMIT_BYTES})")
        size = len((loud.stdout or "").encode("utf-8"))
        bounded, detail = size <= OUTPUT_LIMIT_BYTES * OUTPUT_SLACK, {"bytes": size}
    except Exception as exc:  # refusing an oversized child is also a bound
        bounded, detail = True, {"refused": type(exc).__name__}
    probes["output_bounded"] = _row(None, detail, bounded)

    offered = bool(getattr(probe.port, "supports_started_notification", False))
    seen: list[Any] = []
    probe.launch(probe.descriptor(), "pass", on_started=seen.append if offered else None)
    real = bool(seen) and isinstance(getattr(seen[0], "pid", None), int)
    probes["started_notification"] = _row(offered, real, real if offered else True)

    verdict = "GREEN" if all(row["ok"] for row in probes.values()) else "RED"
    return {"schema": SCHEMA, "backend": type(probe.port).__name__, "claims": claims, "probes": probes,
            "verdict": verdict}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Measure an execution fence backend's isolation claims")
    parser.add_argument("--backend", default="execution_fence_local:LocalProcessExecutionFence",
                        help="module:Class of the backend, importable with lh_runtime on the path")
    args = parser.parse_args(argv)
    module_name, _, class_name = args.backend.partition(":")
    backend = getattr(importlib.import_module(module_name), class_name)
    with tempfile.TemporaryDirectory(prefix="fence-conformance-", ignore_cleanup_errors=True) as raw:
        report = check_backend(backend, Path(raw))
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0 if report["verdict"] == "GREEN" else 1


if __name__ == "__main__":
    raise SystemExit(main())

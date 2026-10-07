#!/usr/bin/env python3
"""Fence conformance: a backend's isolation claims are measured, not trusted.

``gate-pack/fence_conformance/conformance.py`` drives any ``ExecutionFencePort``
the way the engine's command boundary does and probes what the receipt claims:
writes and reads outside the allowed roots, a loopback egress, the caller's
environment, the deadline, the output limit and the started notification.  A
claim that measurement contradicts is RED, whether the claim is strong or weak.

This exam runs the kit on three backends: the shipped ``local-process`` (claims
nothing, contains nothing: GREEN), a backend that claims containment it does not
have (RED), and an honest backend that contains its Python probes and says so
(GREEN).
"""
from __future__ import annotations

import ast
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(REPO / "lh_runtime"))

from execution_fence_local import LocalProcessExecutionFence  # noqa: E402

CHECK_ID = "fence-conformance"
PRELUDE = r'''
import os, sys
_write = [os.path.realpath(p) for p in {write!r}]
_read = [os.path.realpath(p) for p in {read!r}] + [os.path.realpath(p) for p in (sys.prefix, sys.base_prefix, sys.exec_prefix)]
def _inside(path, roots):
    try:
        real = os.path.realpath(os.fspath(path))
    except TypeError:
        return True
    return any(real == root or real.startswith(root + os.sep) for root in roots)
def _hook(event, args):
    if event == "open" and args and isinstance(args[0], (str, bytes, os.PathLike)):
        mode = str(args[1]) if len(args) > 1 and args[1] is not None else "r"
        writing = any(flag in mode for flag in "wax+")
        if not _inside(args[0], _write if writing else _read + _write):
            raise PermissionError("contained: " + ("write" if writing else "read") + " outside the allowed roots")
    if event == "socket.connect":
        raise PermissionError("contained: egress refused")
sys.addaudithook(_hook)
'''


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def kit():
    import conformance
    return conformance


class LyingBackend(LocalProcessExecutionFence):
    """Claims containment and egress control it does not have."""

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        return {**super().receipt_projection(descriptor), "kernel_containment": True, "provider_egress_enforced": True}


class HonestContainingBackend(LocalProcessExecutionFence):
    """Contains Python probes with an audit hook, and claims exactly that."""

    def project_command(self, descriptor: Mapping[str, Any], argv: Sequence[str],
                        environment: Mapping[str, str]) -> list[str]:
        argv = list(argv)
        if len(argv) >= 3 and argv[1] == "-c":
            binding = descriptor["binding"]
            write = [binding["clone_root"], *binding.get("allowed_write_roots", [])]
            read = [binding["clone_root"], *binding.get("allowed_read_roots", [])]
            argv = [argv[0], "-c", PRELUDE.format(write=write, read=read) + "\n" + argv[2], *argv[3:]]
        return argv

    def receipt_projection(self, descriptor: Mapping[str, Any]) -> dict[str, Any]:
        return {**super().receipt_projection(descriptor), "kernel_containment": True, "provider_egress_enforced": True}


def _run(factory: Callable[[], Any], root: Path) -> dict[str, Any]:
    return kit().check_backend(factory, root)


def c1_local(root: Path) -> dict[str, Any]:
    report = _run(LocalProcessExecutionFence, root)
    probes = report["probes"]
    ok = (report["verdict"] == "GREEN" and report["claims"] == {"kernel_containment": False, "provider_egress_enforced": False}
          and probes["write_outside"]["observed"] is True and probes["read_outside"]["observed"] is True
          and probes["egress"]["observed"] is True and all(row["ok"] for row in probes.values()))
    return case("local-process-claims-nothing-and-is-green", ok,
                {"verdict": report["verdict"], "failed": [n for n, r in probes.items() if not r["ok"]]})


def c2_lying(root: Path) -> dict[str, Any]:
    report = _run(LyingBackend, root)
    failed = sorted(name for name, row in report["probes"].items() if not row["ok"])
    ok = report["verdict"] == "RED" and {"write_outside", "read_outside", "egress"} <= set(failed)
    return case("a-backend-that-claims-containment-it-lacks-is-red", ok, {"verdict": report["verdict"], "failed": failed})


def c3_honest(root: Path) -> dict[str, Any]:
    report = _run(HonestContainingBackend, root)
    probes = report["probes"]
    ok = (report["verdict"] == "GREEN" and report["claims"]["kernel_containment"] is True
          and probes["write_outside"]["observed"] is False and probes["read_outside"]["observed"] is False
          and probes["egress"]["observed"] is False)
    return case("an-honest-containing-backend-is-green", ok,
                {"verdict": report["verdict"], "observed": {n: r["observed"] for n, r in probes.items()}})


def c4_shape(root: Path) -> dict[str, Any]:
    report = _run(LocalProcessExecutionFence, root)
    required = {"binding_round_trip", "tampered_descriptor_refused", "replayed_descriptor_refused", "write_outside",
                "read_outside", "egress", "caller_environment_not_forwarded", "deadline_enforced", "output_bounded",
                "started_notification"}
    tree = ast.parse((HERE / "conformance.py").read_text(encoding="utf-8"))
    imported = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported |= {(node.module or "").split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    allowed = {"__future__", "argparse", "importlib", "json", "os", "socket", "subprocess", "sys", "tempfile",
               "threading", "time", "pathlib", "typing", "execution_fence"}
    ok = (report.get("schema") == "lh-fence-conformance/v1" and required <= set(report["probes"])
          and imported <= allowed)
    return case("the-report-covers-every-probe-and-the-kit-stays-local", ok,
                {"missing": sorted(required - set(report.get("probes", {}))), "extra_imports": sorted(imported - allowed)})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="fence-conformance-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("local-process-claims-nothing-and-is-green", lambda: c1_local(root / "local")),
            ("a-backend-that-claims-containment-it-lacks-is-red", lambda: c2_lying(root / "lying")),
            ("an-honest-containing-backend-is-green", lambda: c3_honest(root / "honest")),
            ("the-report-covers-every-probe-and-the-kit-stays-local", lambda: c4_shape(root / "shape")),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:  # a crash or a missing kit is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

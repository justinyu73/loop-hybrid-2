#!/usr/bin/env python3
"""Read-only P5 checks for the LH portable runtime contract.

The checker uses only the task's temporary directory for its behavioral probe.
It does not discover services, providers, user installations, or runtime
state.  A platform-specific feature must be supplied as an explicit optional
capability adapter; the core process path fails closed for an unknown host.
"""

from __future__ import annotations

import ast
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
# Monorepo keeps LH under loop-hybrid/; the public projection is LH at the root.
LH_ROOT = REPO / "loop-hybrid" if (REPO / "loop-hybrid").is_dir() else REPO
CORE_PATH = LH_ROOT / "lh_runtime" / "platform_ports.py"
CORE_IMPORT_ALLOWLIST = frozenset({"fcntl", "msvcrt"})
HOST_SPECIFIC_IMPORTS = frozenset({"ctypes", "fcntl", "msvcrt", "signal", "winreg"})
FIXED_PATH_PATTERNS = (
    re.compile(r"/(?:home|users)/[a-z0-9._-]+", re.IGNORECASE),
    re.compile(r"\b[a-z]:[\\/][^\s\"']+", re.IGNORECASE),
)
FIXED_HOST_TOKENS = (
    re.compile(r"(?<![a-z])wsl(?![a-z])", re.IGNORECASE),
    re.compile(r"(?<![a-z])systemd(?![a-z])", re.IGNORECASE),
    re.compile(r"(?<![a-z])x11(?![a-z])", re.IGNORECASE),
    re.compile(r"(?<![a-z])unc(?![a-z])", re.IGNORECASE),
    re.compile(r"(?<![a-z])nvm(?![a-z])", re.IGNORECASE),
)
REQUIRED_CORE_TYPES = frozenset({
    "OptionalCapabilityAdapter",
    "OptionalCapabilityRegistry",
    "UnsupportedPlatformPort",
})


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _module_name(node: ast.Import | ast.ImportFrom) -> str:
    if isinstance(node, ast.Import):
        return node.names[0].name.split(".", 1)[0].lower()
    return (node.module or "").split(".", 1)[0].lower()


def _class_ancestors(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> tuple[str, ...]:
    names: list[str] = []
    current = parents.get(node)
    while current is not None:
        if isinstance(current, ast.ClassDef):
            names.append(current.name)
        current = parents.get(current)
    return tuple(names)


def core_contract_issues(path: Path = CORE_PATH) -> list[str]:
    """Return static portability findings for one core source file."""
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError) as exc:
        return [f"source-unreadable:{type(exc).__name__}"]

    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    issues: list[str] = []
    defined_types = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    issues.extend(f"missing-type:{name}" for name in sorted(REQUIRED_CORE_TYPES - defined_types))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = _module_name(node)
            if module not in HOST_SPECIFIC_IMPORTS:
                continue
            ancestors = _class_ancestors(node, parents)
            if module not in CORE_IMPORT_ALLOWLIST or "PortableFileLock" not in ancestors:
                issues.append(f"host-import-outside-adapter:{module}")
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
            for pattern in FIXED_PATH_PATTERNS:
                if pattern.search(value):
                    issues.append(f"fixed-path-literal:{pattern.pattern}")
            for pattern in FIXED_HOST_TOKENS:
                if pattern.search(value):
                    issues.append(f"host-default-literal:{pattern.pattern}")
    return sorted(set(issues))


def _temporary_directory(prefix: str) -> tempfile.TemporaryDirectory[str]:
    configured = os.environ.get("LH_HOST_TMP_ROOT", "").strip()
    if not configured:
        return tempfile.TemporaryDirectory(prefix=prefix)
    parent = Path(configured).expanduser() / "portable-runtime-contract"
    parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=str(parent))


def _behavioral_cases() -> list[dict[str, Any]]:
    sys.path.insert(0, str(CORE_PATH.parent))
    from platform_ports import (  # noqa: E402
        CapabilityUnavailable,
        LocalProcessPort,
        OptionalCapabilityAdapter,
        OptionalCapabilityRegistry,
        PlatformPaths,
        ProcessResult,
        UnsupportedPlatformError,
        build_process_port,
    )

    cases: list[dict[str, Any]] = []
    with _temporary_directory("lh-p5-contract-") as raw:
        root = Path(raw)
        result = LocalProcessPort(platform_name="linux").run(
            [sys.executable, "-B", "-c", "print('portable-core')"],
            cwd=root,
            timeout=2,
        )
        cases.append(_case(
            "supported-platform-keeps-argv-process-behavior",
            isinstance(result, ProcessResult)
            and result.returncode == 0
            and result.stdout.strip() == "portable-core",
            repr(result),
        ))

        marker = root / "unsupported-child-ran"
        unsupported = build_process_port(platform_name="freebsd")
        try:
            unsupported.run([
                sys.executable,
                "-B",
                "-c",
                f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')",
            ], cwd=root, timeout=2)
        except UnsupportedPlatformError as exc:
            rejected = (
                exc.code == "unsupported_platform"
                and exc.platform_name == "freebsd"
                and not marker.exists()
            )
        else:
            rejected = False
        cases.append(_case(
            "unsupported-platform-rejects-before-child",
            rejected,
            {"marker_exists": marker.exists(), "port": type(unsupported).__name__},
        ))

        adapter_calls: list[str] = []
        adapter = OptionalCapabilityAdapter(
            "fixture.host-feature",
            platforms=("freebsd",),
            factory=lambda: adapter_calls.append("built") or {"source": "fixture"},
            adapter_id="fixture-host-adapter",
        )
        registry = OptionalCapabilityRegistry([adapter])
        try:
            registry.resolve("fixture.host-feature", platform_name="linux")
        except CapabilityUnavailable:
            missing_is_closed = True
        else:
            missing_is_closed = False
        provided = registry.resolve("fixture.host-feature", platform_name="freebsd")
        cases.append(_case(
            "optional-capability-requires-explicit-platform-adapter",
            missing_is_closed and provided == {"source": "fixture"} and adapter_calls == ["built"],
            {"provided": provided, "adapter_calls": adapter_calls},
        ))

        paths = PlatformPaths.from_environment(
            {"HOME": str(root / "fixture-home"), "XDG_STATE_HOME": str(root / "fixture-state")},
            platform_name="linux",
        )
        explicit_unknown = PlatformPaths.from_environment(
            {"LH_INSTANCE_ROOT": str(root / "explicit-root")},
            platform_name="freebsd",
        )
        try:
            PlatformPaths.from_environment({"HOME": str(root / "fixture-home")}, platform_name="freebsd")
        except UnsupportedPlatformError:
            unknown_default_is_closed = True
        else:
            unknown_default_is_closed = False
        cases.append(_case(
            "paths-are-explicit-or-platform-derived",
            paths.state_root == root / "fixture-state" / "loop-hybrid" / "state"
            and explicit_unknown.instance_root == root / "explicit-root"
            and unknown_default_is_closed,
            {"paths": repr(paths), "explicit_unknown": repr(explicit_unknown)},
        ))
    return cases


def run() -> dict[str, Any]:
    static_issues = core_contract_issues()
    cases = [
        _case("portable-core-static-contract", not static_issues, static_issues),
        *_behavioral_cases(),
    ]
    failures = [
        {"id": item["id"], "detail": item["detail"]}
        for item in cases
        if not item["ok"]
    ]
    return {
        "schema": "lh-portable-runtime-contract/v1",
        "check_id": "lh-portable-runtime-contract-p5",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "native Windows and macOS hosted acceptance remain separate from this local check",
            "provider authorization, live activation, merge, and Goal completion are outside this contract",
        ],
    }


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

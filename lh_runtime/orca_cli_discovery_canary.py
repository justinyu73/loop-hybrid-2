#!/usr/bin/env python3
"""Optional Orca CLI discovery for the live capability acceptance.

The Orca execution host is an optional adapter.  Its CLI is resolved from an
explicit ``LH_ORCA_CLI`` first and then from the shared executable discovery;
a missing CLI is a named error.  No fixed location on another platform (for
example a Windows install seen through a WSL mount) is ever consulted, and
resolution never starts a process.

The host-supplied resolver hook in ``discover_orca_cli`` (a ``tools/``
module that only some hosts provide) is held out of these cases, so the exam
grades the product's own rules identically in every checkout layout.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

CHECK_ID = "lh-orca-cli-discovery"
HOST_SPECIFIC_TEXT = ("/mnt/", "WSL host", "owner run")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _fake_orca(directory: Path, name: str = "orca") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        path = directory / f"{name}.cmd"
        path.write_text("@echo off\r\n", encoding="utf-8")
    else:
        path = directory / name
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        path.chmod(0o755)
    return path


@contextlib.contextmanager
def _environment(home: Path, path_dirs: list[Path], explicit: str | None) -> Iterator[None]:
    names = ("LH_ORCA_CLI", "PATH", "HOME", "USERPROFILE")
    saved = {name: os.environ.get(name) for name in names}
    original_spec = importlib.util.spec_from_file_location

    def without_host_resolver(name: str, location: Any = None, *args: Any, **kwargs: Any):
        if location is not None and Path(str(location)).name == "orca_cli_resolver.py":
            return None
        return original_spec(name, location, *args, **kwargs)

    try:
        os.environ.pop("LH_ORCA_CLI", None)
        if explicit is not None:
            os.environ["LH_ORCA_CLI"] = explicit
        os.environ["PATH"] = os.pathsep.join(str(item) for item in path_dirs)
        os.environ["HOME"] = str(home)
        os.environ["USERPROFILE"] = str(home)
        importlib.util.spec_from_file_location = without_host_resolver
        yield
    finally:
        importlib.util.spec_from_file_location = original_spec
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _resolve(**environment: Any) -> tuple[str | None, str | None]:
    import capability_live_acceptance as live

    with _environment(**environment):
        try:
            return live._resolve_live_orca_cli(), None
        except (FileNotFoundError, ValueError) as exc:
            return None, f"{type(exc).__name__}: {exc}"


def discovery_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-orca-discovery-") as raw:
        root = Path(raw).resolve()
        home = root / "home"
        home.mkdir()
        empty = root / "empty-bin"
        empty.mkdir()
        tool_bin = root / "tool-bin"
        explicit_tool = _fake_orca(root / "explicit-bin", "orca-explicit")
        path_tool = _fake_orca(tool_bin)

        resolved, error = _resolve(home=home, path_dirs=[tool_bin], explicit=str(explicit_tool))
        cases.append(case("explicit-override-wins", resolved == str(explicit_tool) and error is None,
                          {"resolved": resolved, "error": error}))

        resolved, error = _resolve(home=home, path_dirs=[tool_bin], explicit=None)
        cases.append(case("path-discovery-when-unset",
                          resolved is not None and Path(resolved).resolve() == path_tool.resolve(),
                          {"resolved": resolved, "error": error}))

        resolved, error = _resolve(home=home, path_dirs=[empty], explicit=None)
        cases.append(case("missing-tool-is-named-unavailable",
                          resolved is None and error is not None and "LH_ORCA_CLI" in error,
                          {"resolved": resolved, "error": error}))

        resolved, error = _resolve(home=home, path_dirs=[empty], explicit="no-such-orca-cli")
        cases.append(case("invalid-explicit-is-named",
                          resolved is None and error is not None and "no-such-orca-cli" in error,
                          {"resolved": resolved, "error": error}))

        # Record every location consulted and refuse any process start while
        # resolving with nothing configured.
        queried: list[str] = []
        spawned: list[Any] = []
        original_is_file, original_exists = Path.is_file, Path.exists
        original_which, original_popen = shutil.which, subprocess.Popen

        def is_file(self: Path) -> bool:
            queried.append(str(self))
            return original_is_file(self)

        def exists(self: Path, *args: Any, **kwargs: Any) -> bool:
            queried.append(str(self))
            return original_exists(self, *args, **kwargs)

        def which(cmd: str, *args: Any, **kwargs: Any) -> str | None:
            queried.append(f"which:{cmd}")
            return original_which(cmd, *args, **kwargs)

        def popen(*args: Any, **kwargs: Any) -> Any:
            spawned.append(args[:1])
            raise AssertionError("discovery must not start a process")

        Path.is_file, Path.exists = is_file, exists  # type: ignore[method-assign]
        shutil.which, subprocess.Popen = which, popen  # type: ignore[assignment,misc]
        try:
            resolved, error = _resolve(home=home, path_dirs=[empty], explicit=None)
        finally:
            Path.is_file, Path.exists = original_is_file, original_exists  # type: ignore[method-assign]
            shutil.which, subprocess.Popen = original_which, original_popen  # type: ignore[assignment,misc]
        cross_host = [item for item in queried
                      if item.replace("\\", "/").startswith("/mnt/") or "wsl" in item.lower()]
        cases.append(case("no-cross-host-fallback",
                          not cross_host and not spawned and resolved is None,
                          {"cross_host_queries": cross_host, "spawned": len(spawned), "error": error}))
    return cases


def text_case() -> dict[str, Any]:
    source = (HERE / "capability_live_acceptance.py").read_text(encoding="utf-8")
    found = [marker for marker in HOST_SPECIFIC_TEXT if marker in source]
    return case("no-host-specific-text", not found, {"found": found})


def main() -> int:
    try:
        cases = [*discovery_cases(), text_case()]
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        cases = [case("orca-cli-discovery-crashed", False, f"{type(exc).__name__}: {exc}")]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

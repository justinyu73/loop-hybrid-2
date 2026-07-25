#!/usr/bin/env python3
"""One canonical predicate for LH authority-surface paths.

The value reducer, semantic diff grader, and merge gate must not carry separate
copies of this policy.  They all call :func:`is_authority_path`; a future path
class is therefore added once and is covered by the committed canary.
"""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Iterable


AUTHORITY_PREFIXES = (
    ".github/",
    "deploy/",
    "docs/active/",
    "docs/contracts/",
    "gate-pack/",
)

AUTHORITY_BASENAMES = frozenset({
    "AGENTS.md",
    "CLAUDE.md",
    "CURSOR.md",
    "GOVERNANCE.md",
    "project_runtime_contract.json",
})

AUTHORITY_FILES = frozenset({
    "lh_runtime/authority_surface.py",
    "lh_runtime/capability_resolver.py",
    "lh_runtime/diff_grader.py",
    "lh_runtime/goal_loop_run.py",
    "lh_runtime/goal_loop_worker.py",
    "lh_runtime/merge_gate.py",
    "lh_runtime/merge_trust.py",
    "lh_runtime/project_binding.py",
    "lh_runtime/value_reducer.py",
})

AUTHORITY_SUFFIXES = ("_canary.py",)
AUTHORITY_POLICY_MARKERS = (
    "branch-protection",
    "merge-policy",
    "promotion-policy",
    "routing-policy",
)


def _normalize(path: str) -> str:
    candidate = path.replace("\\", "/").removeprefix("./").strip("/")
    if not candidate or candidate == ".":
        return ""
    parts = PurePosixPath(candidate).parts
    if ".." in parts:
        return candidate
    return "/".join(parts)


def is_authority_path(path: str, *, lamp_paths: Iterable[str] = ()) -> bool:
    """Return whether a repo-relative path can change execution authority."""
    normalized = _normalize(path)
    if not normalized:
        return False
    basename = PurePosixPath(normalized).name
    dynamic_lamps = {_normalize(item) for item in lamp_paths if isinstance(item, str)}
    return (
        normalized in AUTHORITY_FILES
        or basename in AUTHORITY_BASENAMES
        or normalized in dynamic_lamps
        or any(normalized == prefix.rstrip("/") or normalized.startswith(prefix)
               for prefix in AUTHORITY_PREFIXES)
        or any(basename.endswith(suffix) for suffix in AUTHORITY_SUFFIXES)
        or any(marker in basename.lower() for marker in AUTHORITY_POLICY_MARKERS)
    )


def authority_paths(paths: Iterable[str], *, lamp_paths: Iterable[str] = ()) -> list[str]:
    """Return sorted unique protected paths from one diff."""
    lamps = tuple(lamp_paths)
    return sorted({
        path for path in paths
        if isinstance(path, str) and is_authority_path(path, lamp_paths=lamps)
    })

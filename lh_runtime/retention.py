#!/usr/bin/env python3
"""Retention: remove engine scratch only when it is superseded and unreferenced.

A store accumulates scratch the engine creates and never removes: verifier
snapshots, trusted-launch scratch, failed-check diagnostics, recovery copies,
and content-addressed review proofs.  An entry may be removed only when every
one of these holds:

- nothing settled refers to it: its name (or, for a content-addressed file,
  its digest) appears in no SQLite text and no JSON file under the store root;
- it is older than the grace period, measured by its newest modification time;
- it is not the newest entry of its kind, so the latest work stays inspectable;
- it is a real directory or file inside the store root, with no symlink or
  junction anywhere inside it, and it is not itself a repository root.

Anything that fails a test is kept, with the reason.  Planning is the default;
``--apply`` removes the planned entries and nothing else.  Scratch names carry
no run identity, so "newest" is decided per kind, not per run.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

if __package__:
    from .platform_ports import remove_tree
else:
    from platform_ports import remove_tree

SCHEMA = "lh-retention-plan/v1"
DEFAULT_GRACE_HOURS = 24.0
# Engine-owned kinds: a folder under the store root, or a name prefix at the root itself.
CATEGORY_DIRS = ("completion-snapshots", "trusted-launches", "completion-check-diagnostics", "candidate-reviews")
ROOT_PREFIXES = ("candidate-recovery-",)
CORPUS_SUFFIXES = (".json", ".jsonl")
MIN_DIGEST_TOKEN = 16


def _is_link(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _entries(root: Path) -> Iterable[tuple[str, Path]]:
    for name in CATEGORY_DIRS:
        folder = root / name
        if _is_link(folder):
            yield name, folder
        elif folder.is_dir():
            for child in sorted(folder.iterdir()):
                yield name, child
    for child in sorted(root.iterdir()):
        if child.name.startswith(ROOT_PREFIXES):
            yield next(p for p in ROOT_PREFIXES if child.name.startswith(p)).rstrip("-"), child


def _walk(path: Path) -> Iterable[Path]:
    """Every path under ``path`` without following any link."""
    stack = [path]
    while stack:
        current = stack.pop()
        yield current
        if current.is_dir() and not _is_link(current):
            stack.extend(current.iterdir())


def _newest_mtime(path: Path) -> float:
    return max(item.lstat().st_mtime for item in _walk(path))


def _size(path: Path) -> int:
    return sum(item.lstat().st_size for item in _walk(path) if item.is_file() and not _is_link(item))


def _sqlite_text(path: Path) -> Iterable[str]:
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    except sqlite3.Error:
        return
    try:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            for row in conn.execute(f"SELECT * FROM {quoted}"):
                for value in row:
                    if isinstance(value, str):
                        yield value
                    elif isinstance(value, bytes):
                        yield value.decode("utf-8", errors="ignore")
    except sqlite3.Error:
        yield "\0unreadable-store\0"
    finally:
        conn.close()


def _corpus(root: Path, candidates: set[Path]) -> tuple[str, list[str]]:
    """All settled text under the root: SQLite text columns and JSON files outside the candidates."""
    parts: list[str] = []
    unreadable: list[str] = []
    for item in _walk(root):
        if item == root or _is_link(item) or not item.is_file():
            continue
        if any(item == candidate or candidate in item.parents for candidate in candidates):
            continue
        if item.suffix in CORPUS_SUFFIXES:
            try:
                parts.append(item.read_text(encoding="utf-8", errors="ignore"))
            except OSError:
                unreadable.append(str(item))
        elif item.suffix in (".sqlite3", ".sqlite", ".db"):
            texts = list(_sqlite_text(item))
            if "\0unreadable-store\0" in texts:
                unreadable.append(str(item))
            parts.extend(texts)
    return "\n".join(parts), unreadable


def _tokens(path: Path) -> list[str]:
    tokens = [path.name]
    if path.is_file() and len(path.stem) >= MIN_DIGEST_TOKEN:
        tokens.append(path.stem)  # a content-addressed file is cited by its digest
    return tokens


def plan(root: Path, *, grace_hours: float = DEFAULT_GRACE_HOURS, now: float | None = None) -> dict[str, Any]:
    root = Path(root)
    if _is_link(root) or not root.is_dir():
        raise ValueError("retention_root_invalid")
    root = root.resolve()
    now = time.time() if now is None else float(now)
    entries = list(_entries(root))
    corpus, unreadable = _corpus(root, {path for _category, path in entries if not _is_link(path)})
    keep: list[dict[str, Any]] = []
    removable: list[dict[str, Any]] = []
    mtimes = {path: _newest_mtime(path) for _category, path in entries if not _is_link(path)}
    newest: dict[str, Path] = {}
    for category, path in entries:
        if path in mtimes and (category not in newest or (mtimes[path], path.name) > (mtimes[newest[category]], newest[category].name)):
            newest[category] = path
    for category, path in entries:
        row = {"path": str(path), "category": category}
        if _is_link(path):
            keep.append({**row, "reason": "link_not_followed"})
            continue
        if not path.resolve().is_relative_to(root):
            keep.append({**row, "reason": "outside_root"})
            continue
        if path.is_dir() and (path / ".git").exists():
            keep.append({**row, "reason": "git_repository"})
            continue
        inner = list(_walk(path))
        if any(_is_link(item) for item in inner):
            keep.append({**row, "reason": "link_not_followed"})
            continue
        if unreadable:
            keep.append({**row, "reason": "references_unreadable"})
            continue
        if any(token in corpus for token in _tokens(path)):
            keep.append({**row, "reason": "referenced"})
            continue
        mtime = mtimes[path]
        if now - mtime < grace_hours * 3600:
            keep.append({**row, "reason": "within_grace"})
            continue
        if newest.get(category) == path:
            keep.append({**row, "reason": "newest_in_category"})
            continue
        removable.append({**row, "bytes": _size(path), "age_hours": round((now - mtime) / 3600, 2)})
    return {"schema": SCHEMA, "root": str(root), "applied": False, "grace_hours": grace_hours, "now": now,
            "remove": removable, "keep": keep, "unreadable_references": unreadable, "errors": []}


def apply(result: dict[str, Any]) -> dict[str, Any]:
    root = Path(result["root"])
    errors = []
    for row in result["remove"]:
        path = Path(row["path"])
        # Re-check at removal time: the plan may be stale, and a link must never be followed.
        if _is_link(path) or not path.resolve().is_relative_to(root) or any(_is_link(item) for item in _walk(path)):
            errors.append({"path": str(path), "error": "changed_since_plan"})
            continue
        if path.is_dir():
            remove_tree(path)
        else:
            try:
                os.chmod(path, 0o600)
                path.unlink()
            except OSError:
                pass
        if path.exists():
            errors.append({"path": str(path), "error": "not_removed"})
    return {**result, "applied": True, "errors": errors}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan (or with --apply, perform) removal of superseded engine scratch")
    parser.add_argument("--root", required=True, help="store root")
    parser.add_argument("--grace-hours", type=float, default=DEFAULT_GRACE_HOURS)
    parser.add_argument("--now", type=float, default=None, help="evaluation time (epoch seconds); default: now")
    parser.add_argument("--apply", action="store_true", help="remove the planned entries")
    args = parser.parse_args(argv)
    try:
        result = plan(Path(args.root), grace_hours=args.grace_hours, now=args.now)
    except ValueError as exc:
        print(json.dumps({"schema": SCHEMA, "error": str(exc)}))
        return 2
    if args.apply:
        result = apply(result)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if not result["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Deterministic allowlist indexer feeding the KnowledgeStore ingest side (W1).

The store's ``ingest`` is already revision-aware: an unchanged file reuses its
document, a changed file retires the stale chunks of the same source_uri.  This
indexer is the missing producer — it walks a fixed allowlist of repo paths and
feeds each UTF-8 text file once per revision.  Re-running it is an incremental
update, never a rebuild.  No provider, no network, no embeddings.

Usage:
  python3 lh_runtime/knowledge_indexer.py --repo <path> --store <path>
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from knowledge_store import KnowledgeStore

# (relative directory, suffix) pairs; anything outside stays unindexed.
ALLOWLIST: tuple[tuple[str, str], ...] = (
    ("docs/contracts", ".md"),
    ("docs/active", ".md"),
    ("lh_runtime", ".py"),
)


def head_revision(repo: Path) -> str:
    out = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def allowlisted_files(repo: Path) -> list[Path]:
    files: list[Path] = []
    for rel_dir, suffix in ALLOWLIST:
        base = repo / rel_dir
        if not base.is_dir():
            continue
        for path in sorted(base.rglob(f"*{suffix}")):
            if path.is_file() and "__pycache__" not in path.parts:
                files.append(path)
    return sorted(files)


def index_repo(*, repo: str | Path, store: KnowledgeStore, revision: str | None = None) -> dict[str, Any]:
    repo_path = Path(repo)
    rev = revision if revision is not None else head_revision(repo_path)
    results: list[dict[str, Any]] = []
    counts = {"indexed": 0, "reused": 0, "revised": 0, "skipped": 0}
    for path in allowlisted_files(repo_path):
        rel = path.relative_to(repo_path).as_posix()
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            counts["skipped"] += 1
            results.append({"source_uri": f"repo://{rel}", "status": "skipped"})
            continue
        outcome = store.ingest_or_revise(source_uri=f"repo://{rel}", revision=rev, content=content)
        counts[outcome["status"]] += 1
        results.append({"source_uri": f"repo://{rel}", **outcome})
    return {"revision": rev, "counts": counts, "results": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Index allowlisted repo files into a KnowledgeStore (deterministic, no provider)")
    parser.add_argument("--repo", required=True, help="repository root to index")
    parser.add_argument("--store", required=True, help="KnowledgeStore root directory")
    args = parser.parse_args(argv)
    summary = index_repo(repo=args.repo, store=KnowledgeStore(Path(args.store)))
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

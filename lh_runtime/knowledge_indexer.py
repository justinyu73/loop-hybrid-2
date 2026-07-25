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
from run_store import RunStore
import value_reducer

# (relative directory, suffix) pairs; anything outside stays unindexed.
ALLOWLIST: tuple[tuple[str, str], ...] = (
    ("docs/contracts", ".md"),
    ("docs/active", ".md"),
    ("lh_runtime", ".py"),
)
ROOT_ALLOWLIST = ("AGENTS.md", "CLAUDE.md", "project_runtime_contract.json")
CANONICAL_ALLOWLIST = ("docs/bootstrap-authority.md",)


def head_revision(repo: Path) -> str:
    out = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def allowlisted_files(repo: Path) -> list[Path]:
    files: list[Path] = []
    for rel in ROOT_ALLOWLIST + CANONICAL_ALLOWLIST:
        path = repo / rel
        if path.is_file():
            files.append(path)
    for rel_dir, suffix in ALLOWLIST:
        base = repo / rel_dir
        if not base.is_dir():
            continue
        for path in sorted(base.rglob(f"*{suffix}")):
            if path.is_file() and "__pycache__" not in path.parts:
                files.append(path)
    return sorted(files)


def index_repo(*, repo: str | Path, store: KnowledgeStore, revision: str | None = None, source_prefix: str | None = None) -> dict[str, Any]:
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
        source_uri = f"repo://{source_prefix}/{rel}" if source_prefix else f"repo://{rel}"
        outcome = store.ingest_or_revise(source_uri=source_uri, revision=rev, content=content)
        counts[outcome["status"]] += 1
        results.append({"source_uri": source_uri, **outcome})
    return {"revision": rev, "counts": counts, "results": results}


def index_run_evidence(*, run_store: RunStore, store: KnowledgeStore) -> dict[str, Any]:
    """Index receipt pointers and bounded failure findings, never raw artifacts."""
    results: list[dict[str, Any]] = []
    counts = {"receipts": 0, "failures": 0, "skipped": 0}
    for run in run_store.terminal_runs():
        run_id = run["run_id"]
        meta = run_store.latest_receipt(run_id)
        if not isinstance(meta, dict) or not isinstance(meta.get("receipt_digest"), str):
            counts["skipped"] += 1
            continue
        try:
            verdict = value_reducer.verdict_for_run(run_store, run_id)
            attempt = run_store.latest_attempt(run_id) or {}
            summary = {
                "schema": "loop-hybrid-evidence-context/v1",
                "run_id": run_id,
                "run_state": run.get("state"),
                "attempt": attempt.get("ordinal"),
                "attempt_state": attempt.get("state"),
                "receipt_digest": meta["receipt_digest"],
                "corpus": "receipt",
                "derived_verdict": verdict.get("verdict"),
                "reasons": list(verdict.get("reasons") or [])[:8],
                "evidence": verdict.get("evidence") if isinstance(verdict.get("evidence"), dict) else {},
            }
            content = json.dumps(summary, ensure_ascii=False, sort_keys=True)
        except Exception:
            counts["skipped"] += 1
            continue
        receipt_uri = f"lh://receipt/{run_id}/{attempt.get('ordinal', 'latest')}"
        outcome = store.ingest_or_revise(source_uri=receipt_uri, revision=meta["receipt_digest"], content=content)
        counts["receipts"] += 1
        results.append({"source_uri": receipt_uri, **outcome})
        if summary["derived_verdict"] == "RED":
            failure_uri = f"lh://failure/{run_id}/{attempt.get('ordinal', 'latest')}"
            failure_content = json.dumps({**summary, "corpus": "failure corpus"}, ensure_ascii=False, sort_keys=True)
            failure = store.ingest_or_revise(source_uri=failure_uri, revision=meta["receipt_digest"], content=failure_content)
            counts["failures"] += 1
            results.append({"source_uri": failure_uri, **failure})
    return {"counts": counts, "results": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Index allowlisted repo files into a KnowledgeStore (deterministic, no provider)")
    parser.add_argument("--repo", required=True, help="repository root to index")
    parser.add_argument("--store", required=True, help="KnowledgeStore root directory")
    parser.add_argument("--source-prefix", default=None, help="optional stable repo label in source URIs")
    parser.add_argument("--run-store", default=None, help="optional LH RunStore root for receipt/failure corpus indexing")
    args = parser.parse_args(argv)
    store = KnowledgeStore(Path(args.store))
    summary = index_repo(repo=args.repo, store=store, source_prefix=args.source_prefix)
    if args.run_store:
        summary["evidence"] = index_run_evidence(run_store=RunStore(Path(args.run_store)), store=store)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

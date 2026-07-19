#!/usr/bin/env python3
"""Deterministic proof for the W1 knowledge indexer wiring.

Proves the ingest side is real: an allowlist walk indexes files once per
revision, re-runs reuse unchanged documents, a changed file retires its stale
chunks, and the read-only MCP search surface returns hits from a store this
indexer filled.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mcp_server
from knowledge_indexer import index_repo
from knowledge_store import KnowledgeStore
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _fixture_repo(root: Path) -> None:
    (root / "docs" / "contracts").mkdir(parents=True)
    (root / "docs" / "active").mkdir(parents=True)
    (root / "lh_runtime").mkdir(parents=True)
    (root / "runtime").mkdir(parents=True)
    (root / "docs" / "contracts" / "goal-hierarchy.md").write_text(
        "Parent goals roll up only when every child is terminal.\n", encoding="utf-8")
    (root / "docs" / "active" / "track.md").write_text(
        "The serial worker dispatches one durable run per tick.\n", encoding="utf-8")
    (root / "lh_runtime" / "controller.py").write_text(
        "# The controller acquires a lease before each attempt.\n", encoding="utf-8")
    (root / "runtime" / "notes.md").write_text(
        "Outside the allowlist: must never be indexed.\n", encoding="utf-8")


def _mcp_search(store: KnowledgeStore, runs: RunStore, query: str) -> list[dict]:
    response = mcp_server.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
         "params": {"name": "lh_search_knowledge", "arguments": {"query": query}}},
        runs, store)
    payload = json.loads(response["result"]["content"][0]["text"])
    return payload


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        repo = Path(raw) / "repo"
        repo.mkdir()
        _fixture_repo(repo)
        store = KnowledgeStore(Path(raw) / "knowledge")
        runs = RunStore(Path(raw) / "runs")

        first = index_repo(repo=repo, store=store, revision="sha-a")
        second = index_repo(repo=repo, store=store, revision="sha-a")

        (repo / "docs" / "active" / "track.md").write_text(
            "The worker now resumes parked external verdicts on each tick.\n", encoding="utf-8")
        third = index_repo(repo=repo, store=store, revision="sha-b")
        stale = store.search("serial worker dispatches", revision="sha-a")
        fresh = store.search("resumes parked external", revision="sha-b")
        restamped = store.search("controller lease", revision="sha-b")

        mcp_hits = _mcp_search(store, runs, "controller lease")
        outside = store.search("never be indexed")

        cases = [
            case("first-run-indexes-only-allowlist",
                 first["counts"] == {"indexed": 3, "reused": 0, "revised": 0, "skipped": 0}
                 and store.summary()["active_documents"] == 3,
                 str(first["counts"])),
            case("second-run-reuses-unchanged-documents",
                 second["counts"] == {"indexed": 0, "reused": 3, "revised": 0, "skipped": 0},
                 str(second["counts"])),
            case("changed-file-reindexed-unchanged-restamped",
                 third["counts"] == {"indexed": 1, "reused": 0, "revised": 2, "skipped": 0},
                 str(third["counts"])),
            case("stale-revision-retired-restamped-searchable",
                 not stale and fresh and fresh[0]["source_uri"] == "repo://docs/active/track.md"
                 and restamped and restamped[0]["source_uri"] == "repo://lh_runtime/controller.py",
                 f"fresh={fresh} restamped={restamped}"),
            case("mcp-search-returns-indexed-content",
                 bool(mcp_hits) and mcp_hits[0]["source_uri"] == "repo://lh_runtime/controller.py"
                 and mcp_hits[0]["document_hash"].startswith("sha256:"),
                 str(mcp_hits)),
            case("non-allowlist-path-is-never-indexed", not outside, str(outside)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-knowledge-indexer", "status": "pass" if not failures else "fail",
                      "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["Indexing is manual CLI only; no driver/worker auto-trigger in W1."]},
                     ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

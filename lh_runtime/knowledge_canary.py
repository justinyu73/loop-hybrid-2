#!/usr/bin/env python3
"""Deterministic proof for version-aware local FTS5 retrieval."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from knowledge_store import KnowledgeStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        store = KnowledgeStore(Path(raw) / "knowledge")
        first = store.ingest(source_uri="repo://lh/controller.py", revision="sha-a", content="The controller acquires a lease and writes durable attempt receipts.")
        reused = store.ingest(source_uri="repo://lh/controller.py", revision="sha-a", content="The controller acquires a lease and writes durable attempt receipts.")
        lease_hits = store.search("controller lease", revision="sha-a")
        changed = store.ingest(source_uri="repo://lh/controller.py", revision="sha-b", content="The startup reconciler finalizes a durable receipt after a crash.")
        stale_hits = store.search("lease", revision="sha-a")
        fresh_hits = store.search("startup reconciler", revision="sha-b")
        other = store.ingest(source_uri="repo://lh/docs.md", revision="sha-b", content="The disposable workspace is rebuilt for each model attempt.")
        capped = store.search("workspace model controller", max_results=1)
        cases = [
            case("fts5-indexes-code-text", first["status"] == "indexed" and lease_hits and lease_hits[0]["source_uri"] == "repo://lh/controller.py", str(lease_hits)),
            case("identical-source-is-not-reindexed", reused["status"] == "reused" and reused["document_id"] == first["document_id"], reused["status"]),
            case("new-source-version-retires-stale-chunks", changed["status"] == "indexed" and not stale_hits and fresh_hits and fresh_hits[0]["revision"] == "sha-b", str(fresh_hits)),
            case("retrieval-carries-provenance", fresh_hits and fresh_hits[0]["document_hash"].startswith("sha256:") and fresh_hits[0]["chunk_hash"].startswith("sha256:"), str(fresh_hits)),
            case("result-count-is-bounded", other["status"] == "indexed" and len(capped) == 1 and store.summary() == {"active_documents": 2, "active_chunks": 2}, str(store.summary())),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-knowledge-fts5", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["This lexical FTS5 phase does not add an embedding model or semantic vector index."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

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
from knowledge_indexer import index_repo, index_run_evidence
from knowledge_store import KnowledgeStore
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _fixture_repo(root: Path) -> None:
    (root / "docs" / "contracts").mkdir(parents=True)
    (root / "docs" / "active").mkdir(parents=True)
    (root / "lh_runtime").mkdir(parents=True)
    (root / "runtime").mkdir(parents=True)
    (root / "AGENTS.md").write_text(
        "Canonical source owns the acceptance boundary; summaries are routing only.\n", encoding="utf-8")
    (root / "docs" / "bootstrap-authority.md").write_text(
        "The canonical source defines the project value boundary and human gate.\n", encoding="utf-8")
    (root / "docs" / "contracts" / "goal-hierarchy.md").write_text(
        "Parent goals roll up only when every child is terminal.\n", encoding="utf-8")
    (root / "docs" / "active" / "track.md").write_text(
        "The serial worker dispatches one durable run per tick.\n", encoding="utf-8")
    (root / "lh_runtime" / "controller.py").write_text(
        "# The controller acquires a lease before each attempt.\n", encoding="utf-8")
    (root / "runtime" / "notes.md").write_text(
        "shouldnotindexmarker must stay outside the allowlist.\n", encoding="utf-8")


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
        initial_document_count = store.summary()["active_documents"]
        second = index_repo(repo=repo, store=store, revision="sha-a")

        (repo / "docs" / "active" / "track.md").write_text(
            "The worker now resumes parked external verdicts on each tick.\n", encoding="utf-8")
        third = index_repo(repo=repo, store=store, revision="sha-b")
        stale = store.search("serial worker dispatches", revision="sha-a")
        fresh = store.search("resumes parked external", revision="sha-b")
        restamped = store.search("controller lease", revision="sha-b")
        canonical = store.search("canonical source human gate", revision="sha-b")

        run_id = runs.create_run(goal={"feature_contract": "failure corpus"}, source_repo=repo, base_revision="sha-b", run_id="run-indexer-failure")
        ordinal = runs.begin_attempt(run_id, f"workspace://{run_id}/1")
        receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal, "usage": {"state": "unknown", "reason": "fixture"}, "verification": {"argv": ["false"], "exit_code": 1}}
        ref = runs.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
        runs.finish_attempt(run_id, ordinal, state="stopped", receipt_ref=ref["ref"], receipt_digest=ref["digest"])
        evidence = index_run_evidence(run_store=runs, store=store)
        failure_hits = store.search("failure corpus", max_results=5)
        context = store.bounded_context("canonical failure receipt", max_results=5, max_chars=120)

        mcp_hits = _mcp_search(store, runs, "controller lease")
        outside = store.search("shouldnotindexmarker")

        cases = [
            case("first-run-indexes-only-allowlist",
                 first["counts"] == {"indexed": 5, "reused": 0, "revised": 0, "skipped": 0}
                 and initial_document_count == 5,
                 str(first["counts"])),
            case("second-run-reuses-unchanged-documents",
                 second["counts"] == {"indexed": 0, "reused": 5, "revised": 0, "skipped": 0},
                 str(second["counts"])),
            case("changed-file-reindexed-unchanged-restamped",
                 third["counts"] == {"indexed": 1, "reused": 0, "revised": 4, "skipped": 0},
                 str(third["counts"])),
            case("stale-revision-retired-restamped-searchable",
                 not stale and fresh and fresh[0]["source_uri"] == "repo://docs/active/track.md"
                 and restamped and restamped[0]["source_uri"] == "repo://lh_runtime/controller.py",
                 f"fresh={fresh} restamped={restamped}"),
            case("canonical-root-and-spec-are-indexed",
                 canonical and {hit["source_uri"] for hit in canonical} >= {"repo://AGENTS.md", "repo://docs/bootstrap-authority.md"},
                 str(canonical)),
            case("mcp-search-returns-indexed-content",
                 bool(mcp_hits) and mcp_hits[0]["source_uri"] == "repo://lh_runtime/controller.py"
                 and mcp_hits[0]["document_hash"].startswith("sha256:"),
                 str(mcp_hits)),
            case("non-allowlist-path-is-never-indexed", not outside, str(outside)),
            case("receipt-and-failure-corpus-index-without-raw-output",
                 evidence["counts"] == {"receipts": 1, "failures": 1, "skipped": 0}
                 and failure_hits
                 and failure_hits[0]["source_uri"].startswith("lh://failure/")
                 and '"verification"' not in failure_hits[0]["text"],
                 json.dumps({"evidence": evidence["counts"], "failure": failure_hits}, ensure_ascii=False)),
            case("next-goal-context-is-bounded-and-provenanced",
                 context["schema"] == "loop-hybrid-goal-context/v1"
                 and context["authority"] == "advisory_only"
                 and context["gate_mutation"] == "forbidden"
                 and context["chars"] <= 120
                 and all(item["source_uri"] and item["document_hash"].startswith("sha256:") for item in context["hits"]),
                 json.dumps(context, ensure_ascii=False)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-knowledge-indexer", "status": "pass" if not failures else "fail",
                      "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["Index refresh is bounded to Goal dispatch; no embeddings or semantic vector index are used."]},
                     ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

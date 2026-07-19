#!/usr/bin/env python3
"""Protocol-level canary for the read-only native LH MCP surface."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from knowledge_store import KnowledgeStore
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        run_store = RunStore(root / "runs")
        run_id = run_store.create_run(goal={"feature_contract": "read state"}, source_repo=root, base_revision="fixture")
        knowledge_store = KnowledgeStore(root / "knowledge")
        knowledge_store.ingest(source_uri="repo://docs/lh.md", revision="fixture", content="The startup reconciler recovers expired leases.")
        requests = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}},
            {"jsonrpc": "2.0", "id": 2, "method": "resources/read", "params": {"uri": "lh://runtime/summary"}},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "lh_get_run", "arguments": {"run_id": run_id}}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "lh_search_knowledge", "arguments": {"query": "reconciler", "revision": "fixture"}}},
            {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "lh_start_run", "arguments": {}}},
        ]
        completed = subprocess.run([sys.executable, str(HERE / "mcp_server.py"), "--run-store", str(root / "runs"), "--knowledge-store", str(root / "knowledge")], input="\n".join(json.dumps(item) for item in requests) + "\n", capture_output=True, text=True, check=True)
        responses = [json.loads(line) for line in completed.stdout.splitlines()]
        summary = json.loads(responses[1]["result"]["contents"][0]["text"])
        run_view = json.loads(responses[2]["result"]["content"][0]["text"])
        search = json.loads(responses[3]["result"]["content"][0]["text"])
        rejected = responses[4]["result"]
        cases = [
            case("stdio-initialize-negotiates-read-capabilities", responses[0]["result"]["protocolVersion"] == "2025-06-18" and set(responses[0]["result"]["capabilities"]) == {"resources", "tools"}, str(responses[0])),
            case("runtime-summary-is-a-resource", summary["run_store"]["runs_by_state"] == {"queued": 1} and summary["knowledge_store"] == {"active_documents": 1, "active_chunks": 1}, str(summary)),
            case("run-tool-is-read-only-view", run_view["run_id"] == run_id and run_view["state"] == "queued", str(run_view)),
            case("knowledge-tool-keeps-provenance", len(search) == 1 and search[0]["source_uri"] == "repo://docs/lh.md", str(search)),
            case("unknown-write-tool-is-rejected", rejected["isError"] is True, str(rejected)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-runtime-mcp", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["This local stdio server exposes only read-only resources/tools; provider execution and GitHub actions remain controller ports."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

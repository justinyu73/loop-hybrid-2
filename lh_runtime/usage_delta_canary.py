#!/usr/bin/env python3
"""Committed W7 smoke: the executor hands its pre-run snapshot to the collector.

Usage logs that a provider keeps are cumulative; billing the whole log to one
attempt fabricates phantom cost.  ``make_cli_agent`` therefore takes the
caller's snapshot before the subprocess and passes it to the usage collector,
so a collector can bill only what the invocation appended.  The engine ships no
provider-specific collector; this smoke proves the hook order with a recording
collector.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import FixtureExecutionFencePort, capsule_with_fence
from cli_agent_executor import make_cli_agent


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        seen: dict[str, Any] = {}

        def recording_collector(_proc: Any, context: dict[str, Any]) -> dict[str, Any]:
            seen["context"] = context
            return {"state": "measured", "model": "fixture", "input_tokens": 1, "output_tokens": 1, "cache_read_tokens": 0}

        fixture_fence = FixtureExecutionFencePort()
        agent = make_cli_agent(
            lambda _prompt: ["sh", "-c", "true"],
            name="fixture",
            usage_collector=recording_collector,
            snapshot_fn=lambda: {"path": "fixture-session", "usage": None},
            execution_fence_port=fixture_fence,
        )
        workspace = root / "ws"
        workspace.mkdir()
        agent(workspace, capsule_with_fence(fixture_fence, workspace, {"attempt": 1, "goal": {"feature_contract": "x"}, "base_revision": "base"}))
        passed = seen.get("context", {}).get("snapshot") == {"path": "fixture-session", "usage": None}

        cases = [
            {"id": "executor-passes-snapshot-to-collector",
             "ok": passed and seen.get("context", {}).get("started_at") is not None,
             "detail": json.dumps({"snapshot_in_context": passed})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-usage-delta",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/usage_delta_canary.py",
                         "fixtures": "a recording collector and a fixture fence; no real CLI"},
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

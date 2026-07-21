#!/usr/bin/env python3
"""Committed W7 smoke: per-invocation delta usage attribution.

Session logs are cumulative; when a file is resumed or shared, billing the
whole file to one attempt fabricates phantom cost (live evidence: a ~$0.02
attempt was billed cache_read=214,769,152 tokens). Proves, offline with
fixture JSONL session files and injected session_roots, that the executor
snapshot + collector delta bills only what the invocation appended: pre-
existing cumulative stays out, a brand-new file bills in full, an unchanged
or inconsistent file reports unknown (never zero, never phantom, never
negative), and the same delta shape holds for the claude transcript and kimi
wire collectors, which shared the flaw. Also proves make_cli_agent takes the
snapshot before the subprocess and passes it to the collector.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import claude_usage
import codex_usage
import kimi_usage
from cli_agent_executor import make_cli_agent

STARTED_AT = 2000.0


def _touch(path: Path, mtime: float = STARTED_AT + 10) -> None:
    os.utime(path, (mtime, mtime))


def _codex_line(input_tokens: int, cached: int, output: int) -> str:
    return json.dumps({
        "type": "turn",
        "token_usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached,
            "output_tokens": output,
            "total_tokens": input_tokens + output,
        },
    })


def _claude_line(input_tokens: int, cache_read: int, cache_creation: int, output: int) -> str:
    return json.dumps({
        "type": "assistant",
        "message": {
            "model": "claude-opus-4-8",
            "usage": {
                "input_tokens": input_tokens,
                "cache_read_input_tokens": cache_read,
                "cache_creation_input_tokens": cache_creation,
                "output_tokens": output,
            },
        },
    })


def _kimi_line(input_other: int, cache_read: int, cache_creation: int, output: int) -> str:
    return json.dumps({
        "type": "usage.record",
        "model": "kimi-code/k3",
        "usage": {"inputOther": input_other, "output": output, "inputCacheRead": cache_read, "inputCacheCreation": cache_creation},
    })


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # 1) Pre-existing cumulative + small append -> only the delta is billed.
        codex_root = root / "codex"
        session_dir = codex_root / "2026" / "07" / "20"
        session_dir.mkdir(parents=True)
        session = session_dir / "rollout-shared.jsonl"
        session.write_text(_codex_line(1_000_000, 500_000, 10_000) + "\n", encoding="utf-8")
        baseline = codex_usage.snapshot(session_roots=(codex_root,))
        with session.open("a", encoding="utf-8") as handle:
            handle.write(_codex_line(1_001_000, 500_000, 10_100) + "\n")
        _touch(session)
        delta = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": baseline}, session_roots=(codex_root,))

        # 2) Brand-new session file -> its full cumulative counts are the invocation's.
        fresh_root = root / "codex-fresh"
        (fresh_root / "2026").mkdir(parents=True)
        fresh_baseline = codex_usage.snapshot(session_roots=(fresh_root,))
        fresh_session = fresh_root / "2026" / "rollout-new.jsonl"
        fresh_session.write_text(_codex_line(2_000, 500, 100) + "\n", encoding="utf-8")
        _touch(fresh_session)
        fresh = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": fresh_baseline}, session_roots=(fresh_root,))

        # 3) Snapshot taken, nothing appended -> unknown, never zero, never phantom.
        unchanged = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": baseline}, session_roots=(codex_root,))
        # (the file's content is back at baseline in a parallel root)
        stale_root = root / "codex-stale"
        (stale_root / "2026").mkdir(parents=True)
        stale_session = stale_root / "2026" / "rollout-same.jsonl"
        stale_session.write_text(_codex_line(1_000_000, 500_000, 10_000) + "\n", encoding="utf-8")
        stale_baseline = codex_usage.snapshot(session_roots=(stale_root,))
        _touch(stale_session)
        unchanged = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": stale_baseline}, session_roots=(stale_root,))

        # 4) Run-2 regression: huge pre-existing cumulative + tiny append -> tiny delta only.
        huge_root = root / "codex-huge"
        (huge_root / "2026").mkdir(parents=True)
        huge_session = huge_root / "2026" / "rollout-resumed.jsonl"
        huge_session.write_text(_codex_line(214_800_000, 214_769_152, 900_000) + "\n", encoding="utf-8")
        huge_baseline = codex_usage.snapshot(session_roots=(huge_root,))
        with huge_session.open("a", encoding="utf-8") as handle:
            handle.write(_codex_line(214_800_016, 214_769_152, 900_008) + "\n")
        _touch(huge_session)
        huge = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": huge_baseline}, session_roots=(huge_root,))

        # 5) Counters went backwards (file replaced with a smaller cumulative) -> unknown.
        backwards_root = root / "codex-backwards"
        (backwards_root / "2026").mkdir(parents=True)
        backwards_session = backwards_root / "2026" / "rollout-replaced.jsonl"
        backwards_session.write_text(_codex_line(1_000_000, 500_000, 10_000) + "\n", encoding="utf-8")
        backwards_baseline = codex_usage.snapshot(session_roots=(backwards_root,))
        backwards_session.write_text(_codex_line(50, 0, 5) + "\n", encoding="utf-8")
        _touch(backwards_session)
        backwards = codex_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": backwards_baseline}, session_roots=(backwards_root,))

        # 6) Claude transcript: same flaw, same delta fix.
        claude_root = root / "claude"
        transcript_dir = claude_root / "proj"
        transcript_dir.mkdir(parents=True)
        transcript = transcript_dir / "session.jsonl"
        transcript.write_text(_claude_line(100_000, 800_000, 5_000, 2_000) + "\n", encoding="utf-8")
        claude_baseline = claude_usage.snapshot(session_roots=(claude_root,))
        with transcript.open("a", encoding="utf-8") as handle:
            handle.write(_claude_line(120, 400, 0, 30) + "\n")
        _touch(transcript)
        claude = claude_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": claude_baseline}, session_roots=(claude_root,))

        # 7) Kimi wire log: same flaw, same delta fix.
        kimi_root = root / "kimi"
        wire_dir = kimi_root / "wd" / "session_1" / "agents" / "main"
        wire_dir.mkdir(parents=True)
        wire = wire_dir / "wire.jsonl"
        wire.write_text(_kimi_line(50_000, 600_000, 1_000, 3_000) + "\n", encoding="utf-8")
        kimi_baseline = kimi_usage.snapshot(session_roots=(kimi_root,))
        with wire.open("a", encoding="utf-8") as handle:
            handle.write(_kimi_line(60, 200, 0, 40) + "\n")
        _touch(wire)
        kimi = kimi_usage.collector(object(), {"started_at": STARTED_AT, "snapshot": kimi_baseline}, session_roots=(kimi_root,))

        # 8) Executor protocol: snapshot taken before the subprocess and handed to the collector.
        seen: dict[str, Any] = {}

        def recording_collector(_proc: Any, context: dict[str, Any]) -> dict[str, Any]:
            seen["context"] = context
            return {"state": "measured", "model": "fixture", "input_tokens": 1, "output_tokens": 1, "cache_read_tokens": 0}

        agent = make_cli_agent(
            lambda _prompt: ["sh", "-c", "true"],
            name="fixture",
            usage_collector=recording_collector,
            snapshot_fn=lambda: {"path": "fixture-session", "usage": None},
        )
        workspace = root / "ws"
        workspace.mkdir()
        agent(workspace, {"attempt": 1, "goal": {"feature_contract": "x"}, "base_revision": "base"})
        passed = seen.get("context", {}).get("snapshot") == {"path": "fixture-session", "usage": None}

        cases = [
            {"id": "appended-delta-only-is-billed",
             "ok": delta.get("state") == "measured" and delta["input_tokens"] == 1_000
             and delta["output_tokens"] == 100 and delta["cache_read_tokens"] == 0,
             "detail": json.dumps(delta)},
            {"id": "brand-new-session-file-bills-in-full",
             "ok": fresh_baseline.get("path") is None
             and fresh.get("state") == "measured" and fresh["input_tokens"] == 1_500
             and fresh["output_tokens"] == 100 and fresh["cache_read_tokens"] == 500,
             "detail": json.dumps({"baseline": fresh_baseline, "usage": fresh})},
            {"id": "no-append-since-snapshot-is-unknown",
             "ok": unchanged.get("state") == "unknown",
             "detail": json.dumps(unchanged)},
            {"id": "huge-preexisting-cumulative-bills-only-append",
             "ok": huge.get("state") == "measured" and huge["input_tokens"] == 16
             and huge["output_tokens"] == 8 and huge["cache_read_tokens"] == 0,
             "detail": json.dumps(huge)},
            {"id": "backwards-counters-are-unknown-never-negative",
             "ok": backwards.get("state") == "unknown",
             "detail": json.dumps(backwards)},
            {"id": "claude-transcript-delta-only",
             "ok": claude.get("state") == "measured" and claude["input_tokens"] == 120
             and claude["output_tokens"] == 30 and claude["cache_read_tokens"] == 400
             and claude["model"] == "claude-opus-4-8",
             "detail": json.dumps(claude)},
            {"id": "kimi-wire-delta-only",
             "ok": kimi.get("state") == "measured" and kimi["input_tokens"] == 60
             and kimi["output_tokens"] == 40 and kimi["cache_read_tokens"] == 200
             and kimi["model"] == "kimi-code/k3",
             "detail": json.dumps(kimi)},
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
                         "fixtures": "JSONL session files in tempdir with injected session_roots; no real CLI"},
        "known_gaps_open": [
            "collectors called without a snapshot keep the legacy whole-file behavior; production executor presets always snapshot",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

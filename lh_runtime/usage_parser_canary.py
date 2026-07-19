#!/usr/bin/env python3
"""Committed G-b smoke: claude/kimi usage collectors parse real session-log shapes."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import claude_usage
import kimi_usage
import token_cost


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # Claude transcript shape: assistant messages with message.usage + model.
        claude_dir = root / "claude" / "proj"
        claude_dir.mkdir(parents=True)
        transcript = claude_dir / "sess-1.jsonl"
        transcript.write_text(
            json.dumps({"type": "assistant", "message": {"model": "claude-opus-4-8", "usage": {
                "input_tokens": 100, "cache_read_input_tokens": 50000, "cache_creation_input_tokens": 3000, "output_tokens": 200}}}) + "\n"
            + json.dumps({"type": "assistant", "message": {"model": "claude-opus-4-8", "usage": {
                "input_tokens": 50, "cache_read_input_tokens": 6000, "cache_creation_input_tokens": 100, "output_tokens": 40}}}) + "\n"
            + json.dumps({"type": "user", "message": {"content": "ignored"}}) + "\n",
            encoding="utf-8",
        )
        extracted = claude_usage.extract_usage_from_transcript(transcript)
        cases.append(case(
            "claude-transcript-maps-usage-and-model",
            extracted["state"] == "measured"
            and extracted["model"] == "claude-opus-4-8"
            and extracted["input_tokens"] == 3250  # 100+3000 + 50+100
            and extracted["output_tokens"] == 240
            and extracted["cache_read_tokens"] == 56000,
            json.dumps(extracted),
        ))
        empty_claude = claude_usage.collector(object(), {"started_at": 0}, session_roots=(root / "nowhere",))
        cases.append(case(
            "claude-missing-log-is-unknown-not-zero",
            empty_claude["state"] == "unknown",
            str(empty_claude),
        ))

        # Kimi wire shape: usage.record events with model; step.end usage too.
        wire_dir = root / "kimi" / "wd_proj_x" / "session_s1" / "agents" / "main"
        wire_dir.mkdir(parents=True)
        wire = wire_dir / "wire.jsonl"
        wire.write_text(
            json.dumps({"type": "usage.record", "model": "kimi-code/k3", "usage": {
                "inputOther": 4567, "output": 42, "inputCacheRead": 16640, "inputCacheCreation": 0}, "usageScope": "turn", "time": 1}) + "\n"
            + json.dumps({"type": "context.append_loop_event", "event": {"type": "step.end", "usage": {
                "inputOther": 100, "output": 5, "inputCacheRead": 200, "inputCacheCreation": 10}}}) + "\n",
            encoding="utf-8",
        )
        k_extracted = kimi_usage.extract_usage_from_wire_file(wire)
        cases.append(case(
            "kimi-wire-maps-usage-and-model",
            k_extracted["state"] == "measured"
            and k_extracted["model"] == "kimi-code/k3"
            and k_extracted["input_tokens"] == 4677  # 4567+0 + 100+10
            and k_extracted["output_tokens"] == 47
            and k_extracted["cache_read_tokens"] == 16840,
            json.dumps(k_extracted),
        ))
        empty_kimi = kimi_usage.collector(object(), {"started_at": 0}, session_roots=(root / "nowhere",))
        cases.append(case(
            "kimi-missing-log-is-unknown-not-zero",
            empty_kimi["state"] == "unknown",
            str(empty_kimi),
        ))

        # Latest-file selection honours since_ts (attribution = newest since run start).
        older = claude_dir / "sess-old.jsonl"
        older.write_text(transcript.read_text(encoding="utf-8"), encoding="utf-8")
        import os
        os.utime(older, (1000, 1000))
        os.utime(transcript, (2000, 2000))
        latest = claude_usage.find_latest_transcript((root / "claude",), since_ts=1500)
        cases.append(case(
            "claude-latest-transcript-since-run-start",
            latest == transcript,
            str(latest),
        ))

        # Pricing covers the real model ids now (no silent unknown).
        cost = token_cost.compute_cost(extracted)
        k_cost = token_cost.compute_cost(k_extracted)
        cases.append(case(
            "real-model-ids-have-pricing",
            cost["state"] == "measured" and k_cost["state"] == "measured"
            and cost["cost_usd"] > 0 and k_cost["cost_usd"] > 0,
            json.dumps({"claude": cost.get("cost_usd"), "kimi": k_cost.get("cost_usd")}),
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-usage-parsers-gb",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/usage_parser_canary.py"},
        "known_gaps_open": [
            "attribution assumes serial single-worker (newest session file since run start)",
            "kimi-code/k3 rates are reported launch figures; re-check when Moonshot publishes the final card",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Provider-free smoke for the codex session usage collector.

Uses a fixture rollout JSONL in the real codex shape (cumulative token_usage +
per-turn last_token_usage) captured during the live smoke.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
from codex_usage import collector, extract_usage_from_session_file, find_latest_session


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


# Real codex shape (numbers from the live-smoke capture).
SESSION_LINE = json.dumps({
    "type": "turn",
    "token_usage": {"input_tokens": 152109, "cached_input_tokens": 84352, "output_tokens": 516, "reasoning_output_tokens": 382, "total_tokens": 152625},
    "last_token_usage": {"input_tokens": 42371, "cached_input_tokens": 0, "output_tokens": 100, "total_tokens": 42471},
})

PRICING = {"codex": {"input": 2.5, "output": 10.0, "cache_read": 0.25}}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        sessions = root / "sessions" / "2026" / "07" / "15"
        sessions.mkdir(parents=True)
        older = sessions / "rollout-old.jsonl"
        newer = sessions / "rollout-new.jsonl"
        older.write_text('{"token_usage":{"input_tokens":10,"cached_input_tokens":0,"output_tokens":2,"total_tokens":12}}\n', encoding="utf-8")
        newer.write_text(SESSION_LINE + "\n", encoding="utf-8")
        os.utime(older, (1000, 1000))
        os.utime(newer, (2000, 2000))

        extracted = extract_usage_from_session_file(newer)
        latest = find_latest_session((root / "sessions",), since_ts=1500)
        collected = collector(object(), {"started_at": 1500}, session_roots=(root / "sessions",))
        empty = collector(object(), {"started_at": 0}, session_roots=(root / "empty",))
        cost = token_cost.compute_cost(extracted, pricing=PRICING)
        expected_cost = round((67757 * 2.5 + 516 * 10.0 + 84352 * 0.25) / 1_000_000, 6)

        # M4: the session log's real model id wins over the executor-name fallback.
        modeled = sessions / "rollout-modeled.jsonl"
        modeled.write_text(
            json.dumps({"type": "response_item", "payload": {"model": "gpt-5.6-luna"}}) + "\n"
            + json.dumps({"token_usage": {"input_tokens": 100, "cached_input_tokens": 10, "output_tokens": 5, "total_tokens": 105}}) + "\n",
            encoding="utf-8",
        )
        extracted_modeled = extract_usage_from_session_file(modeled)

        cases = [
            case("extract-maps-fresh-cached-output", extracted["state"] == "measured" and extracted["input_tokens"] == 67757 and extracted["cache_read_tokens"] == 84352 and extracted["output_tokens"] == 516, str(extracted)),
            case("picks-cumulative-over-last-turn", extracted["input_tokens"] + extracted["output_tokens"] + 0 == 68273 and extracted["cache_read_tokens"] == 84352, "cumulative 152625 chosen, not last 42471"),
            case("find-latest-since-timestamp", latest == newer, str(latest)),
            case("collector-returns-measured", collected["state"] == "measured" and collected["input_tokens"] == 67757, str(collected)),
            case("missing-session-is-unknown-not-zero", empty["state"] == "unknown", str(empty)),
            case("cost-is-cache-aware-on-real-shape", cost["state"] == "measured" and cost["cost_usd"] == expected_cost, f"{cost} expected {expected_cost}"),
            case("real-model-id-wins-over-executor-fallback", extracted_modeled["model"] == "gpt-5.6-luna" and extracted["model"] == "codex", json.dumps({"modeled": extracted_modeled["model"], "fallback": extracted["model"]})),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-codex-usage",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "attribution assumes serial single-worker (newest session since run start)",
            "pricing calibrated 2026-07-18 to official provider pages; re-check rates when providers reprice",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

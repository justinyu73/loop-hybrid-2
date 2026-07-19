#!/usr/bin/env python3
"""Provider-free smoke for token accounting: capture, cost estimate, unknown-safety."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
from cli_agent_executor import make_cli_agent
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


PRICING = {"m1": {"input": 1.0, "output": 2.0, "cache_read": 0.1}}


def _measured_usage_line_parser(stdout: str) -> dict:
    # fixture format: "USAGE <input> <output> <cache_read>"
    for line in stdout.splitlines():
        if line.startswith("USAGE "):
            _tag, i, o, c = line.split()
            return token_cost.measured_usage(model="m1", input_tokens=int(i), output_tokens=int(o), cache_read_tokens=int(c))
    return token_cost.unknown_usage(model="m1")


def _write_receipt(store: RunStore, run_id: str, usage: dict) -> None:
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
        "usage": usage, "verification": {"argv": ["true"], "exit_code": 0, "stdout": "x", "stderr": "y"},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        measured = token_cost.measured_usage(model="m1", input_tokens=1_000_000, output_tokens=1_000_000, cache_read_tokens=1_000_000)
        cost_measured = token_cost.compute_cost(measured, pricing=PRICING)
        cost_unknown = token_cost.compute_cost(token_cost.unknown_usage(model="m1"), pricing=PRICING)
        cost_unpriced = token_cost.compute_cost(token_cost.measured_usage(model="mystery", input_tokens=10, output_tokens=10), pricing=PRICING)

        # executor stamps usage when a parser is supplied; stays unknown otherwise.
        (root / "ws1").mkdir()
        (root / "ws2").mkdir()
        argv = lambda _prompt: [sys.executable, "-c", "print('USAGE 100 20 5000')"]
        with_parser = make_cli_agent(argv, name="m1", usage_parser=_measured_usage_line_parser)(root / "ws1", {"attempt": 1, "goal": {}, "base_revision": "r"})
        without_parser = make_cli_agent(argv, name="m1")(root / "ws2", {"attempt": 1, "goal": {}, "base_revision": "r"})

        # receipt -> usage_records -> aggregate, with a measured + an unknown attempt.
        store = RunStore(root / "runs")
        r1 = store.create_run(goal={"feature_contract": "x"}, source_repo=root, base_revision="r")
        r2 = store.create_run(goal={"feature_contract": "y"}, source_repo=root, base_revision="r")
        _write_receipt(store, r1, token_cost.measured_usage(model="m1", input_tokens=500_000, output_tokens=100_000, cache_read_tokens=0))
        _write_receipt(store, r2, token_cost.unknown_usage(model="m1"))
        records = store.usage_records()
        rollup = token_cost.aggregate(records, pricing=PRICING)

        cases = [
            case("cost-estimate-is-cache-aware", cost_measured["state"] == "measured" and cost_measured["cost_usd"] == 3.1, str(cost_measured)),
            case("unknown-usage-is-never-zero-cost", cost_unknown["state"] == "unknown", str(cost_unknown)),
            case("unpriced-model-is-unknown-cost", cost_unpriced["state"] == "unknown", str(cost_unpriced)),
            case("executor-with-parser-stamps-measured-usage", with_parser["usage"]["state"] == "measured" and with_parser["usage"]["input_tokens"] == 100, str(with_parser["usage"])),
            case("executor-without-parser-stays-unknown", without_parser["usage"]["state"] == "unknown", str(without_parser["usage"])),
            case("rollup-sums-measured-and-flags-unknown", rollup["measured_records"] == 1 and rollup["unknown_records"] == 1 and rollup["total_tokens"] == 600_000 and rollup["cost_complete"] is False, str(rollup)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-token-accounting",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "codex/claude usage parsers are pluggable and calibrated against real CLI output (human live smoke)",
            "pricing calibrated 2026-07-18 to official provider pages; cost remains an API-equivalent estimate, not a billing export",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

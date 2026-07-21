#!/usr/bin/env python3
"""Committed W9e smoke: human void of a poisoned usage record.

A phantom usage record written before W7's delta attribution (live evidence:
cache_read=214,769,152 tokens, ~$32) blocks the daily cost gate for the rest
of its UTC day. Proves, offline, that a human-recorded append-only
correction excludes the record from usage_records / token_cost.aggregate /
the dispatch gate's daily cost, that the original receipt and digest stay
byte-identical, that voiding a nonexistent record is a clear error, that a
duplicate void reports already-voided, and that corrections are durable
across a store reopen. Nothing in the engine voids automatically.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import token_cost
from dispatch_gate import DispatchGate
from run_store import RunStore

# gpt-5.6-luna is priced in token_cost.DEFAULT_PRICING; these counts price at
# tens of dollars — the phantom shape from the live incident.
PHANTOM_USAGE = {"state": "measured", "model": "gpt-5.6-luna", "input_tokens": 214_800_000, "output_tokens": 900_000, "cache_read_tokens": 214_769_152}
SMALL_USAGE = {"state": "measured", "model": "gpt-5.6-luna", "input_tokens": 100, "output_tokens": 10, "cache_read_tokens": 0}
VOID_REASON = "phantom cache_read from pre-W7 cumulative attribution (live incident)"


def _seed(store: RunStore, run_id: str, usage: dict[str, Any]) -> None:
    store.create_run(goal={"goal_id": f"goal-{run_id}"}, source_repo=HERE, base_revision="base", run_id=run_id)
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1",
        "run_id": run_id,
        "attempt": ordinal,
        "usage": usage,
        "verification": {"argv": ["true"], "exit_code": 0},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _receipt_bytes(store: RunStore, run_id: str) -> bytes:
    meta = store.latest_receipt(run_id)
    return (store.root / meta["receipt_ref"]).read_bytes()


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        store = RunStore(root / "runs")
        _seed(store, "run-w9e-phantom", PHANTOM_USAGE)
        _seed(store, "run-w9e-honest", SMALL_USAGE)

        before_records = store.usage_records()
        before_cost = token_cost.aggregate(before_records)["estimated_cost_usd"]
        gate_before = DispatchGate(store).evaluate()
        receipt_before = _receipt_bytes(store, "run-w9e-phantom")

        # The human void goes through the CLI entry point (the only caller).
        cli = subprocess.run(
            [sys.executable, "-B", str(HERE / "usage_void.py"), "--run-store", str(root / "runs"),
             "--run-id", "run-w9e-phantom", "--attempt", "1", "--reason", VOID_REASON],
            capture_output=True, text=True,
        )
        correction = json.loads(cli.stdout) if cli.returncode == 0 and cli.stdout.strip() else {}

        after_records = store.usage_records()
        after_cost = token_cost.aggregate(after_records)["estimated_cost_usd"]
        gate_after = DispatchGate(store).evaluate()
        receipt_after = _receipt_bytes(store, "run-w9e-phantom")
        digest_meta = store.latest_receipt("run-w9e-phantom")
        digest_actual = "sha256:" + hashlib.sha256(receipt_after).hexdigest()

        # Duplicate void reports already-voided without a second row.
        duplicate = store.void_usage("run-w9e-phantom", 1, reason=VOID_REASON)

        # Voiding a record that does not exist is a clear error, nothing written.
        missing_error = None
        try:
            store.void_usage("run-w9e-phantom", 99, reason="no such attempt")
        except ValueError as exc:
            missing_error = str(exc)

        # Durability: a fresh store object on the same root sees the correction.
        reopened = RunStore(root / "runs")
        reopened_records = reopened.usage_records()
        corrections = reopened.usage_corrections()

        cases = [
            {"id": "phantom-blocks-dispatch-gate-before-void",
             "ok": len(before_records) == 2 and before_cost > 5.0
             and gate_before["action"] == "stop" and gate_before["reason_code"] == "daily_cost_hard",
             "detail": json.dumps({"records": len(before_records), "cost": before_cost,
                                   "gate": gate_before["reason_code"]})},
            {"id": "void-excludes-record-from-usage-and-cost",
             "ok": cli.returncode == 0 and correction.get("status") == "voided"
             and [record["run_id"] for record in after_records] == ["run-w9e-honest"]
             and after_cost < 0.01,
             "detail": json.dumps({"correction": correction, "records": len(after_records), "cost": after_cost})},
            {"id": "dispatch-gate-heals-after-void",
             "ok": gate_after["action"] == "allow" and gate_after["cost"]["estimated_cost_usd"] < 0.01,
             "detail": json.dumps({"action": gate_after["action"], "cost": gate_after["cost"]})},
            {"id": "original-receipt-byte-identical-after-void",
             "ok": receipt_before == receipt_after and digest_actual == digest_meta["receipt_digest"],
             "detail": json.dumps({"digest": digest_actual})},
            {"id": "duplicate-void-reports-already-voided",
             "ok": duplicate.get("status") == "already_voided" and len(corrections) == 1,
             "detail": json.dumps({"duplicate": duplicate, "corrections": len(corrections)})},
            {"id": "void-of-nonexistent-record-is-a-clear-error",
             "ok": missing_error is not None and "no recorded usage" in missing_error and len(corrections) == 1,
             "detail": json.dumps({"error": missing_error})},
            {"id": "correction-is-durable-across-reopen",
             "ok": [record["run_id"] for record in reopened_records] == ["run-w9e-honest"]
             and len(corrections) == 1 and corrections[0]["reason"] == VOID_REASON
             and corrections[0]["run_id"] == "run-w9e-phantom" and corrections[0]["ordinal"] == 1,
             "detail": json.dumps({"corrections": corrections})},
            {"id": "non-voided-record-is-unaffected",
             "ok": after_records[0]["run_id"] == "run-w9e-honest"
             and after_records[0]["input_tokens"] == 100 and after_records[0]["state"] == "measured",
             "detail": json.dumps(after_records[0] if after_records else None)},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-usage-void",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/usage_void_canary.py",
                         "fixtures": "seeded stores only; no provider, no network"},
        "known_gaps_open": [
            "voiding is human-only by design; nothing re-arms a voided record except a later manual decision (no un-void command yet)",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

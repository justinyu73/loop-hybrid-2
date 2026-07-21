#!/usr/bin/env python3
"""W9e human CLI: void a poisoned usage record (append-only correction).

Usage records written before per-invocation delta attribution (W7) can carry
phantom totals that block the daily cost gate for the rest of the UTC day.
This command records a human's void decision as a durable correction row —
the original receipt and its digest are never touched. Nothing in the engine
invokes it; only a human runs it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from run_store import RunStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Void a poisoned usage record (human-only, append-only correction)")
    parser.add_argument("--run-store", required=True, help="path to the run store root")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt", type=int, required=True, help="attempt ordinal of the record to void")
    parser.add_argument("--reason", required=True, help="why this record is being voided")
    args = parser.parse_args(argv)
    correction = RunStore(Path(args.run_store)).void_usage(args.run_id, args.attempt, reason=args.reason)
    print(json.dumps(correction, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

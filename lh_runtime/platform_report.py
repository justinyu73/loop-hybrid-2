#!/usr/bin/env python3
"""Lightweight read-only platform report: one-glance project status + cost.

Reads LH's durable stores and prints a unified status (runs, goals, needs-human,
tokens, estimated cost, elapsed). This is the seed of the local platform view;
it consumes the same read layer SH aggregates across projects. It never writes.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from goal_store import GoalStore
from project_status import build_status, render_text
from run_store import RunStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print a unified LH project status (read-only)")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--run-store", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    status = build_status(RunStore(Path(args.run_store)), GoalStore(Path(args.goal_store)))
    print(json.dumps(status, ensure_ascii=False, indent=2) if args.json else render_text(status))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

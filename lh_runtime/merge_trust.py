#!/usr/bin/env python3
"""Human-held trust-ramp administration for the B13 merge gate.

The engine records ramp events itself (a parked PR found merged on poll
without a merge record in the gate's ledger counts as a human merge), but it
NEVER revokes: revocation is human-held, and this CLI is the human's handle
for it — plus manual recording when evidence must be entered by hand.

Usage:
  python3 -B lh_runtime/merge_trust.py --store <ramp.sqlite3> record \
      --repo owner/name --goal-type campaign:stage --run-id run-... --pr-number 7
  python3 -B lh_runtime/merge_trust.py --store <ramp.sqlite3> revoke \
      --repo owner/name --goal-type campaign:stage --reason "why evidence no longer counts"
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from merge_gate import RAMP_REQUIRED, RAMP_WINDOW_SECONDS, TrustRampStore, record_human_merge


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Human-held trust-ramp administration for the B13 merge gate")
    parser.add_argument("--store", required=True, help="path to the trust-ramp SQLite (next to action-ledger.sqlite3)")
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record", help="record one human merge as ramp evidence (idempotent per run+PR)")
    record.add_argument("--repo", required=True, help="owner/name")
    record.add_argument("--goal-type", required=True, help="campaign_id:stage_id")
    record.add_argument("--run-id", required=True)
    record.add_argument("--pr-number", type=int, required=True)
    revoke = sub.add_parser("revoke", help="revoke the ramp evidence for a repo+goal-type (takes effect on the next evaluation)")
    revoke.add_argument("--repo", required=True, help="owner/name")
    revoke.add_argument("--goal-type", required=True, help="campaign_id:stage_id")
    revoke.add_argument("--reason", required=True)
    args = parser.parse_args(argv)

    store = TrustRampStore(Path(args.store))
    at = time.time()
    if args.command == "record":
        result = record_human_merge(store, repo=args.repo, goal_type=args.goal_type, run_id=args.run_id, pr_number=args.pr_number, at=at)
    else:
        store.revoke(args.repo, args.goal_type, args.reason, at=at)
        result = {"revoked": True, "repo": args.repo, "goal_type": args.goal_type, "reason": args.reason, "recorded_at": at}
    result["count"] = store.count(args.repo, args.goal_type, now=at)
    result["at_level"] = store.at_level(args.repo, args.goal_type, now=at)
    result["ramp"] = {"required": RAMP_REQUIRED, "window_seconds": RAMP_WINDOW_SECONDS}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

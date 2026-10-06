#!/usr/bin/env python3
"""Acceptance timeline: verification, acceptance, and promotion stay separate.

A green verification is not acceptance, and acceptance is not promotion.  The
timeline is append-only and enforces their order: acceptance must reference a
recorded verification of the same task, and promotion must reference a
recorded acceptance.  ``project`` reports the three states side by side and
never folds one into another.

Usage:
  timeline.py record  --timeline F --task T --kind verification|acceptance|promotion --ref R
  timeline.py project --timeline F --task T
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

SCHEMA = "lh-acceptance-timeline/v1"
KINDS = ("verification", "acceptance", "promotion")
PREREQUISITE = {"acceptance": ("verification", "acceptance_requires_verification"),
                "promotion": ("acceptance", "promotion_requires_acceptance")}


def _events(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def record(path: Path, task: str, kind: str, ref: str) -> dict[str, Any]:
    events = _events(path)
    if kind in PREREQUISITE:
        required, reason = PREREQUISITE[kind]
        if not any(event["event_id"] == ref and event["task"] == task and event["kind"] == required
                   for event in events):
            return {"status": "refused", "reason": reason}
    body = {"schema": SCHEMA, "task": task, "kind": kind, "ref": ref, "sequence": len(events) + 1}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    event = {**body, "event_id": "evt-" + hashlib.sha256(raw).hexdigest()[:24]}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(event, sort_keys=True) + "\n")
    return {"status": "recorded", "event_id": event["event_id"]}


def project(path: Path, task: str) -> dict[str, Any]:
    latest: dict[str, Any] = {kind: None for kind in KINDS}
    for event in _events(path):
        if event.get("task") == task and event.get("kind") in latest:
            latest[event["kind"]] = {"event_id": event["event_id"], "ref": event["ref"]}
    return {"status": "projected", "task": task, **latest}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["record", "project"])
    parser.add_argument("--timeline", type=Path, required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--kind", choices=KINDS)
    parser.add_argument("--ref")
    args = parser.parse_args(argv)
    if args.command == "record":
        if args.kind is None or not args.ref:
            result = {"status": "refused", "reason": "kind_and_ref_required"}
        else:
            result = record(args.timeline, args.task, args.kind, args.ref)
    else:
        result = project(args.timeline, args.task)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") in {"recorded", "projected"} else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

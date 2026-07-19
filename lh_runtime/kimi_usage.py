#!/usr/bin/env python3
"""Kimi usage collector: read token usage from the Kimi Code session wire log.

Kimi Code writes a per-session wire.jsonl under ~/.kimi-code/sessions/<wd>/session-*/
agents/main/ whose ``usage.record`` events carry per-turn counts in the shape
``{"inputOther", "output", "inputCacheRead", "inputCacheCreation"}`` plus the real
model id (e.g. ``kimi-code/k3``).  This maps to a token_cost usage record so cost
separates fresh input, cache reads, and output.

Attribution: a serial single-worker run produces one fresh wire file, so the
newest file modified since the run started is that run's log.  Unknown-safe: any
missing file or absent usage stays ``unknown``, never a fabricated zero.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import token_cost

DEFAULT_SESSION_ROOTS: tuple[Path, ...] = (Path.home() / ".kimi-code" / "sessions",)


def extract_usage_from_wire_file(path: str | Path, *, model: str = "kimi") -> dict[str, Any] | None:
    """Return cumulative usage (sum of turn-scoped usage.record events) mapped to
    a measured usage record, or None if absent.  The record carries the real
    model id when present; ``model`` is only the fallback."""
    totals = {"inputOther": 0, "output": 0, "inputCacheRead": 0, "inputCacheCreation": 0}
    found = False
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        usage: dict[str, Any] | None = None
        if obj.get("type") == "usage.record" and isinstance(obj.get("usage"), dict):
            usage = obj["usage"]
            if isinstance(obj.get("model"), str) and obj["model"].strip():
                model = obj["model"].strip()
        else:
            event = obj.get("event")
            if isinstance(event, dict) and isinstance(event.get("usage"), dict):
                usage = event["usage"]
        if usage:
            found = True
            for key in totals:
                totals[key] += int(usage.get(key, 0))
    if not found:
        return None
    return token_cost.measured_usage(
        model=model,
        input_tokens=totals["inputOther"] + totals["inputCacheCreation"],
        output_tokens=totals["output"],
        cache_read_tokens=totals["inputCacheRead"],
    )


def find_latest_wire(session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS, *, since_ts: float = 0.0) -> Path | None:
    latest: Path | None = None
    latest_mtime = -1.0
    for root in session_roots:
        root = Path(root)
        if not root.exists():
            continue
        for path in root.glob("*/session_*/agents/main/wire.jsonl"):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime >= since_ts - 1.0 and mtime > latest_mtime:
                latest_mtime = mtime
                latest = path
    return latest


def collector(
    proc: subprocess.CompletedProcess,
    context: dict[str, Any],
    *,
    session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS,
    model: str = "kimi",
) -> dict[str, Any]:
    since = float(context.get("started_at", 0.0)) if isinstance(context, dict) else 0.0
    path = find_latest_wire(session_roots, since_ts=since)
    if path is None:
        return token_cost.unknown_usage(model=model, reason="no kimi wire file found since run start")
    usage = extract_usage_from_wire_file(path, model=model)
    if usage is None:
        return token_cost.unknown_usage(model=model, reason="kimi wire file has no usage records")
    return usage

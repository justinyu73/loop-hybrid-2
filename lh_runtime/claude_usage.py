#!/usr/bin/env python3
"""Claude usage collector: read token usage from the Claude Code transcript log.

Claude Code writes per-session transcripts under ~/.claude/projects/<dir>/<session>.jsonl
whose assistant messages carry ``message.usage`` in the shape
``{"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens",
"output_tokens"}`` plus the real model id (e.g. ``claude-opus-4-8``).  This maps to
a token_cost usage record so cost separates fresh input, cache reads, and output.

Attribution: a serial single-worker run produces one fresh transcript, so the
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

DEFAULT_SESSION_ROOTS: tuple[Path, ...] = (Path.home() / ".claude" / "projects",)


def extract_usage_from_transcript(path: str | Path, *, model: str = "claude") -> dict[str, Any] | None:
    """Return cumulative usage (sum of assistant message usages) mapped to a
    measured usage record, or None if absent.  Fresh input folds in
    cache-creation tokens (they are billed as fresh writes); cache reads stay
    separate.  The record carries the real model id when present."""
    totals = {"input": 0, "cache_read": 0, "cache_creation": 0, "output": 0}
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
        if obj.get("type") != "assistant":
            continue
        message = obj.get("message")
        if not isinstance(message, dict):
            continue
        usage = message.get("usage")
        if not isinstance(usage, dict) or not usage:
            continue
        found = True
        if isinstance(message.get("model"), str) and message["model"].strip():
            model = message["model"].strip()
        totals["input"] += int(usage.get("input_tokens", 0))
        totals["cache_read"] += int(usage.get("cache_read_input_tokens", 0))
        totals["cache_creation"] += int(usage.get("cache_creation_input_tokens", 0))
        totals["output"] += int(usage.get("output_tokens", 0))
    if not found:
        return None
    return token_cost.measured_usage(
        model=model,
        input_tokens=totals["input"] + totals["cache_creation"],
        output_tokens=totals["output"],
        cache_read_tokens=totals["cache_read"],
    )


def find_latest_transcript(session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS, *, since_ts: float = 0.0) -> Path | None:
    latest: Path | None = None
    latest_mtime = -1.0
    for root in session_roots:
        root = Path(root)
        if not root.exists():
            continue
        for path in root.glob("*/*.jsonl"):
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
    model: str = "claude",
) -> dict[str, Any]:
    since = float(context.get("started_at", 0.0)) if isinstance(context, dict) else 0.0
    path = find_latest_transcript(session_roots, since_ts=since)
    if path is None:
        return token_cost.unknown_usage(model=model, reason="no claude transcript found since run start")
    usage = extract_usage_from_transcript(path, model=model)
    if usage is None:
        return token_cost.unknown_usage(model=model, reason="claude transcript has no usage records")
    return usage

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


def snapshot(*, session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS, model: str = "kimi") -> dict[str, Any]:
    """Pre-invocation baseline for delta attribution (wire logs are cumulative
    when a session is resumed; see codex_usage.snapshot)."""
    path = find_latest_wire(session_roots)
    usage = extract_usage_from_wire_file(path, model=model) if path is not None else None
    return {"path": str(path) if path is not None else None, "usage": usage}


def _delta_usage(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, int] | None:
    """Counter-wise current-minus-baseline; None when any counter went backwards."""
    delta: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens", "cache_read_tokens"):
        diff = int(current.get(key, 0)) - int(baseline.get(key, 0))
        if diff < 0:
            return None
        delta[key] = diff
    return delta


def collector(
    proc: subprocess.CompletedProcess,
    context: dict[str, Any],
    *,
    session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS,
    model: str = "kimi",
) -> dict[str, Any]:
    since = float(context.get("started_at", 0.0)) if isinstance(context, dict) else 0.0
    baseline = context.get("snapshot") if isinstance(context, dict) else None
    path = find_latest_wire(session_roots, since_ts=since)
    if path is None:
        return token_cost.unknown_usage(model=model, reason="no kimi wire file found since run start")
    usage = extract_usage_from_wire_file(path, model=model)
    if usage is None:
        return token_cost.unknown_usage(model=model, reason="kimi wire file has no usage records")
    if not isinstance(baseline, dict):
        return usage  # legacy call without a pre-invocation snapshot
    if baseline.get("path") != str(path):
        return usage  # the invocation produced a fresh wire file: the counts are its own
    base_usage = baseline.get("usage")
    if not isinstance(base_usage, dict) or base_usage.get("state") != token_cost.USAGE_MEASURED:
        return token_cost.unknown_usage(model=model, reason="snapshot baseline has no measured usage for the same wire file")
    delta = _delta_usage(usage, base_usage)
    if delta is None:
        return token_cost.unknown_usage(model=model, reason="wire counters went backwards since the snapshot")
    if not any(delta.values()):
        return token_cost.unknown_usage(model=model, reason="no new usage recorded since the snapshot")
    return token_cost.measured_usage(
        model=str(usage.get("model") or model),
        input_tokens=delta["input_tokens"],
        output_tokens=delta["output_tokens"],
        cache_read_tokens=delta["cache_read_tokens"],
    )

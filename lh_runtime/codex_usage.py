#!/usr/bin/env python3
"""Codex usage collector: read token usage from the codex CLI session log.

Codex writes a per-invocation rollout JSONL under ~/.codex/sessions/... whose
cumulative ``token_usage`` object carries the counts (input_tokens includes the
cached portion; cached_input_tokens is the cache-read part; output_tokens; and
total_tokens = input_tokens + output_tokens). This maps to a token_cost usage
record so cost separates fresh input, cache reads, and output.

Attribution: a serial single-worker run produces one fresh session file, so the
newest file modified since the run started is that run's log. Unknown-safe: any
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

DEFAULT_SESSION_ROOTS: tuple[Path, ...] = (
    Path.home() / ".codex" / "sessions",
    Path.home() / ".config" / "codex" / "sessions",
)


def _find_token_usage(obj: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        if all(key in obj for key in ("input_tokens", "output_tokens", "total_tokens")):
            found.append(obj)
        for value in obj.values():
            found.extend(_find_token_usage(value))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(_find_token_usage(value))
    return found


def _find_model_ids(obj: Any) -> list[str]:
    """Collect ``model`` string fields (the session log records the real model
    id per response item, e.g. ``gpt-5.6-luna``)."""
    found: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "model" and isinstance(value, str) and value.strip():
                found.append(value.strip())
            else:
                found.extend(_find_model_ids(value))
    elif isinstance(obj, list):
        for value in obj:
            found.extend(_find_model_ids(value))
    return found


def extract_usage_from_session_file(path: str | Path, *, model: str = "codex") -> dict[str, Any] | None:
    """Return the cumulative usage (the token_usage object with the largest
    total_tokens), mapped to a measured usage record, or None if absent.

    The usage record carries the real model id found in the session log when
    present (pricing is per model id); ``model`` is only the fallback."""
    best: dict[str, Any] | None = None
    model_ids: list[str] = []
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
        model_ids.extend(_find_model_ids(obj))
        for usage in _find_token_usage(obj):
            total = usage.get("total_tokens")
            if isinstance(total, int) and (best is None or total > int(best.get("total_tokens", -1))):
                best = usage
    if best is None:
        return None
    if model_ids:
        counts = {mid: model_ids.count(mid) for mid in set(model_ids)}
        model = max(counts, key=lambda mid: (counts[mid], mid))
    input_tokens = int(best.get("input_tokens", 0))
    cached = int(best.get("cached_input_tokens", 0))
    output_tokens = int(best.get("output_tokens", 0))
    fresh_input = max(input_tokens - cached, 0)
    return token_cost.measured_usage(model=model, input_tokens=fresh_input, output_tokens=output_tokens, cache_read_tokens=cached)


def find_latest_session(session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS, *, since_ts: float = 0.0) -> Path | None:
    latest: Path | None = None
    latest_mtime = -1.0
    for root in session_roots:
        root = Path(root)
        if not root.exists():
            continue
        for path in root.rglob("*.jsonl"):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime >= since_ts - 1.0 and mtime > latest_mtime:
                latest_mtime = mtime
                latest = path
    return latest


def snapshot(*, session_roots: tuple[Path, ...] = DEFAULT_SESSION_ROOTS, model: str = "codex") -> dict[str, Any]:
    """Pre-invocation baseline for delta attribution.

    Session files are cumulative: when a file is resumed or shared, its
    counts include history this invocation never spent. The executor takes
    this snapshot BEFORE launching the CLI; the collector then bills only
    the per-invocation delta instead of the whole file."""
    path = find_latest_session(session_roots)
    usage = extract_usage_from_session_file(path, model=model) if path is not None else None
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
    model: str = "codex",
) -> dict[str, Any]:
    since = float(context.get("started_at", 0.0)) if isinstance(context, dict) else 0.0
    baseline = context.get("snapshot") if isinstance(context, dict) else None
    path = find_latest_session(session_roots, since_ts=since)
    if path is None:
        return token_cost.unknown_usage(model=model, reason="no codex session file found since run start")
    usage = extract_usage_from_session_file(path, model=model)
    if usage is None:
        return token_cost.unknown_usage(model=model, reason="codex session file has no token_usage")
    if not isinstance(baseline, dict):
        return usage  # legacy call without a pre-invocation snapshot
    if baseline.get("path") != str(path):
        return usage  # the invocation produced a fresh session file: the counts are its own
    base_usage = baseline.get("usage")
    if not isinstance(base_usage, dict) or base_usage.get("state") != token_cost.USAGE_MEASURED:
        return token_cost.unknown_usage(model=model, reason="snapshot baseline has no measured usage for the same session file")
    delta = _delta_usage(usage, base_usage)
    if delta is None:
        return token_cost.unknown_usage(model=model, reason="session counters went backwards since the snapshot")
    if not any(delta.values()):
        return token_cost.unknown_usage(model=model, reason="no new usage recorded since the snapshot")
    return token_cost.measured_usage(
        model=str(usage.get("model") or model),
        input_tokens=delta["input_tokens"],
        output_tokens=delta["output_tokens"],
        cache_read_tokens=delta["cache_read_tokens"],
    )

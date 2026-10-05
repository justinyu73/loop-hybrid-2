#!/usr/bin/env python3
"""Token usage cost estimation — model-agnostic, cache-aware, unknown-safe.

Usage is captured as raw counts per run. Cost is an *estimate* derived at read
time from the operator's declared pricing table, so a pricing change never
requires re-running. The engine ships no prices: without a declared rate a cost
stays ``unknown``, and unknown usage is never reported as zero cost.
"""

from __future__ import annotations

from typing import Any

USAGE_MEASURED = "measured"
USAGE_UNKNOWN = "unknown"

# A pricing table maps a model id to per-million-token USD rates:
#   {"model-id": {"input": <$/Mtok>, "output": <$/Mtok>, "cache_read": <$/Mtok>}}
# Cache reads are priced separately; they must not be priced as fresh input.


def measured_usage(*, model: str, input_tokens: int, output_tokens: int, cache_read_tokens: int = 0) -> dict[str, Any]:
    return {
        "state": USAGE_MEASURED,
        "model": model,
        "input_tokens": int(input_tokens),
        "output_tokens": int(output_tokens),
        "cache_read_tokens": int(cache_read_tokens),
    }


def unknown_usage(*, model: str | None = None, reason: str = "provider did not report usage") -> dict[str, Any]:
    return {"state": USAGE_UNKNOWN, "model": model, "reason": reason}


def compute_cost(usage: dict[str, Any] | None, *, pricing: dict[str, dict[str, float]] | None = None) -> dict[str, Any]:
    """Estimate cost from a usage record. Never manufactures a number from
    unknown usage or an unpriced model."""
    table = pricing or {}
    if not isinstance(usage, dict) or usage.get("state") != USAGE_MEASURED:
        return {"state": USAGE_UNKNOWN, "basis": "estimated", "reason": "usage is not measured"}
    model = usage.get("model")
    rates = table.get(model) if isinstance(model, str) else None
    if rates is None:
        return {"state": USAGE_UNKNOWN, "basis": "estimated", "reason": f"no pricing for model {model!r}"}
    input_tokens = int(usage.get("input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    cache_read_tokens = int(usage.get("cache_read_tokens", 0))
    cost = (
        input_tokens * rates.get("input", 0.0)
        + output_tokens * rates.get("output", 0.0)
        + cache_read_tokens * rates.get("cache_read", 0.0)
    ) / 1_000_000
    return {
        "state": USAGE_MEASURED,
        "basis": "estimated",
        "model": model,
        "cost_usd": round(cost, 6),
        "total_tokens": input_tokens + output_tokens + cache_read_tokens,
        "breakdown": {"input_tokens": input_tokens, "output_tokens": output_tokens, "cache_read_tokens": cache_read_tokens},
    }


def aggregate(usages: list[dict[str, Any]], *, pricing: dict[str, dict[str, float]] | None = None) -> dict[str, Any]:
    """Roll up a list of usage records. Measured records sum; unknown records are
    counted separately so an unknown is never silently treated as zero."""
    total_tokens = 0
    total_cost = 0.0
    total_elapsed = 0.0
    measured = 0
    unknown = 0
    priced = True
    for usage in usages:
        if isinstance(usage, dict) and isinstance(usage.get("elapsed_seconds"), (int, float)):
            total_elapsed += float(usage["elapsed_seconds"])
        if not isinstance(usage, dict) or usage.get("state") != USAGE_MEASURED:
            unknown += 1
            continue
        measured += 1
        total_tokens += int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0)) + int(usage.get("cache_read_tokens", 0))
        cost = compute_cost(usage, pricing=pricing)
        if cost.get("state") == USAGE_MEASURED:
            total_cost += float(cost["cost_usd"])
        else:
            priced = False
    return {
        "measured_records": measured,
        "unknown_records": unknown,
        "total_tokens": total_tokens,
        "estimated_cost_usd": round(total_cost, 6),
        "total_elapsed_seconds": round(total_elapsed, 3),
        "cost_complete": priced and unknown == 0,
        "basis": "estimated",
    }

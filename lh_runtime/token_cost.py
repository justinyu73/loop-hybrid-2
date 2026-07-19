#!/usr/bin/env python3
"""Token usage cost estimation — model-agnostic, cache-aware, unknown-safe.

Usage is captured as raw counts per run (see cli_agent_executor). Cost is an
*estimate* derived at read time from a configurable pricing table, so a pricing
change never requires re-running. Unknown usage is never reported as zero cost;
it stays ``unknown`` (matching LH's no-fabrication discipline). Rates below are
calibrated to the providers' official pricing pages (see table comment).
"""

from __future__ import annotations

from typing import Any

USAGE_MEASURED = "measured"
USAGE_UNKNOWN = "unknown"

# Per-million-token USD rates. Override via `pricing` argument or an external
# config. Cache reads are billed separately (they dominate real usage, so they
# must not be priced as fresh input).
#
# 2026-07-18: calibrated against the providers' official pricing pages —
# OpenAI Codex row (gpt-5.3-codex): https://platform.openai.com/docs/pricing
# Anthropic Sonnet 5 (Claude Code default): https://www.anthropic.com/pricing
# NOTE: Sonnet 5 rates are introductory through 2026-08-31, then $3/$15.
# When either CLI runs on a subscription login these figures are API-equivalent
# estimates, not billed amounts — the `estimated` basis label stays honest.
DEFAULT_PRICING: dict[str, dict[str, float]] = {
    # "model-id": {"input": <$/Mtok>, "output": <$/Mtok>, "cache_read": <$/Mtok>}
    "codex": {"input": 1.75, "output": 14.0, "cache_read": 0.175},
    "claude": {"input": 2.0, "output": 10.0, "cache_read": 0.20},
    # Real model ids seen in codex session logs (usage records carry the real
    # id since M4; executor-name keys above are the fallback when no id is
    # found). gpt-5.6-luna per OpenAI's official pricing page 2026-07-18.
    "gpt-5.6-luna": {"input": 1.0, "output": 6.0, "cache_read": 0.10},
    # Real model ids seen in claude/kimi session logs.
    # claude-opus-4-8 per anthropic.com/pricing; Sonnet 5 intro through 2026-08-31;
    # kimi-code/k3 reported launch rates ($3/$15, cached $0.30);
    # kimi-code/kimi-for-coding = K2.7 Code rates.
    "claude-opus-4-8": {"input": 5.0, "output": 25.0, "cache_read": 0.50},
    "claude-sonnet-5": {"input": 2.0, "output": 10.0, "cache_read": 0.20},
    "kimi-code/k3": {"input": 3.0, "output": 15.0, "cache_read": 0.30},
    "kimi-code/kimi-for-coding": {"input": 0.95, "output": 4.0, "cache_read": 0.19},
    # kimi = Kimi K2.7 Code (Moonshot official rate card 2026-07: input $0.95,
    # output $4.00, cache-hit input $0.19). Kimi Code CLI runs on membership,
    # so these are API-equivalent estimates like the other rows.
    "kimi": {"input": 0.95, "output": 4.0, "cache_read": 0.19},
}


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
    table = pricing if pricing is not None else DEFAULT_PRICING
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

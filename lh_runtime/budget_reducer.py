"""Reduce receipt-backed usage into a durable driver budget decision."""

from __future__ import annotations

from typing import Any

import token_cost


BUDGET_EXHAUSTED = "budget_exhausted"
BUDGET_UNKNOWN = "budget_unknown"


def evaluate(
    run_store: Any,
    *,
    ceiling_tokens: int | None,
    scope: str | None = None,
    pricing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the receipt-backed budget state for one injected scope."""
    if ceiling_tokens is None:
        return {
            "state": "disabled",
            "allowed": True,
            "stop_reason": None,
            "scope": scope,
            "ceiling_tokens": None,
            "total_tokens": 0,
            "remaining_tokens": None,
            "usage_records": 0,
            "measured_records": 0,
            "unknown_records": 0,
            "estimated_cost_usd": 0.0,
            "cost_complete": True,
        }

    if ceiling_tokens < 0:
        raise ValueError("ceiling_tokens must be non-negative")

    records = run_store.usage_records()
    aggregate = token_cost.aggregate(records, pricing=pricing)
    total_tokens = int(aggregate["total_tokens"])
    unknown_records = int(aggregate["unknown_records"])

    if unknown_records:
        state = "unknown"
        stop_reason = BUDGET_UNKNOWN
        allowed = False
    elif total_tokens >= ceiling_tokens:
        state = "exhausted"
        stop_reason = BUDGET_EXHAUSTED
        allowed = False
    else:
        state = "available"
        stop_reason = None
        allowed = True

    return {
        "state": state,
        "allowed": allowed,
        "stop_reason": stop_reason,
        "scope": scope,
        "ceiling_tokens": ceiling_tokens,
        "total_tokens": total_tokens,
        "remaining_tokens": max(ceiling_tokens - total_tokens, 0),
        "usage_records": len(records),
        "measured_records": len(records) - unknown_records,
        "unknown_records": unknown_records,
        "estimated_cost_usd": aggregate["estimated_cost_usd"],
        "cost_complete": aggregate["cost_complete"],
    }

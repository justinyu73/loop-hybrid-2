"""Bounded evidence-driven projections over an operator routing authority.

The operator profile remains the authority.  A projection is a short-lived,
digest-bound overlay that may adjust only scores and may downgrade health.
Provider/model identity, capabilities, permissions, and trust never come from
the target project or from a model's self-report.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import capability_resolver as cr

OBSERVATION_SCHEMA = "lh-routing-observation/v1"
PROJECTION_SCHEMA = "lh-routing-adaptive-projection/v1"
ACCEPTANCE_AUTHORITY = "committed_lamp+value_reducer"
OUTCOMES = {"accepted", "rejected", "transport_failure"}
_HEALTH_RANK = {"unavailable": 0, "unknown": 0, "degraded": 1, "healthy": 2}
_IDENTITY_FIELDS = (
    "binding_id",
    "executor_kind",
    "runner",
    "provider_ref",
    "model_family",
    "model",
    "endpoint_ref",
    "capabilities",
    "tools",
    "permission_ceiling",
    "network_access",
    "data_boundary",
    "context_limit",
    "context_isolation",
    "trust_tier",
)


def _instant(name: str, value: Any) -> tuple[str, datetime]:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must carry a timezone")
    return value, parsed.astimezone(timezone.utc)


def _digest(name: str, value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
    ):
        raise ValueError(f"{name} must be a sha256 digest")
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise ValueError(f"{name} must be a sha256 digest") from exc
    return value


def _number(name: str, value: Any, *, minimum: float = 0.0) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or float(value) < minimum
    ):
        raise ValueError(f"{name} must be a number >= {minimum}")
    return float(value)


def _identity_digest(resource: dict[str, Any]) -> str:
    return cr.digest_json({
        field: resource.get(field)
        for field in _IDENTITY_FIELDS
    })


def validate_policy(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("adaptive policy must be an object")
    expected = {
        "enabled",
        "min_samples",
        "max_score_delta",
        "projection_ttl_seconds",
        "failure_downgrade_count",
        "allow_health_downgrade",
    }
    if set(raw) != expected:
        raise ValueError("adaptive policy has missing or unexpected fields")
    if raw.get("enabled") is not True:
        raise ValueError("adaptive policy must be explicitly enabled")
    if raw.get("allow_health_downgrade") not in {True, False}:
        raise ValueError("allow_health_downgrade must be boolean")
    min_samples = int(_number("min_samples", raw.get("min_samples"), minimum=1))
    ttl = int(
        _number(
            "projection_ttl_seconds",
            raw.get("projection_ttl_seconds"),
            minimum=60,
        )
    )
    downgrade_count = int(
        _number(
            "failure_downgrade_count",
            raw.get("failure_downgrade_count"),
            minimum=1,
        )
    )
    return {
        "enabled": True,
        "min_samples": min_samples,
        "max_score_delta": _number(
            "max_score_delta",
            raw.get("max_score_delta"),
            minimum=0.0,
        ),
        "projection_ttl_seconds": ttl,
        "failure_downgrade_count": downgrade_count,
        "allow_health_downgrade": raw["allow_health_downgrade"],
    }


def validate_observation(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("schema") != OBSERVATION_SCHEMA:
        raise ValueError(f"observation.schema must be {OBSERVATION_SCHEMA}")
    expected = {
        "schema",
        "binding_id",
        "receipt_digest",
        "acceptance_authority",
        "outcome",
        "latency_seconds",
        "cost_usd",
        "measured_at",
    }
    if set(raw) != expected:
        raise ValueError("observation has missing or unexpected fields")
    binding_id = raw.get("binding_id")
    if not isinstance(binding_id, str) or not binding_id.strip():
        raise ValueError("observation.binding_id must be non-empty")
    outcome = raw.get("outcome")
    if outcome not in OUTCOMES:
        raise ValueError(f"observation.outcome must be one of {sorted(OUTCOMES)}")
    authority = raw.get("acceptance_authority")
    expected_authority = (
        "transport"
        if outcome == "transport_failure"
        else ACCEPTANCE_AUTHORITY
    )
    if authority != expected_authority:
        raise ValueError(
            "observation acceptance authority does not match its outcome"
        )
    measured_text, _ = _instant("observation.measured_at", raw.get("measured_at"))
    return {
        "schema": OBSERVATION_SCHEMA,
        "binding_id": binding_id.strip(),
        "receipt_digest": _digest(
            "observation.receipt_digest",
            raw.get("receipt_digest"),
        ),
        "acceptance_authority": authority,
        "outcome": outcome,
        "latency_seconds": _number(
            "observation.latency_seconds",
            raw.get("latency_seconds"),
        ),
        "cost_usd": _number("observation.cost_usd", raw.get("cost_usd")),
        "measured_at": measured_text,
    }


def _bounded(value: float, base: float, delta: float) -> float:
    return round(max(0.0, min(base + delta, max(base - delta, value))), 8)


def build_projection(
    graph: dict[str, Any],
    observations: list[dict[str, Any]],
    *,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Build a short-lived score/health overlay from external proof only."""
    normalized = cr.validate_graph(graph)
    authority_digest = normalized.get("routing_authority_digest")
    if not isinstance(authority_digest, str):
        raise ValueError("adaptive projection requires routing_authority_digest")
    policy = validate_policy(normalized["policy"].get("adaptive"))
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    checked = [validate_observation(item) for item in observations]
    receipt_digests = [item["receipt_digest"] for item in checked]
    if len(receipt_digests) != len(set(receipt_digests)):
        raise ValueError("adaptive observations contain replayed receipt digests")
    known = {
        resource["binding_id"]: resource
        for resource in normalized["registry"]["resources"]
    }
    unknown = sorted({item["binding_id"] for item in checked} - set(known))
    if unknown:
        raise ValueError(f"observations reference unknown bindings: {unknown}")
    adjustments: list[dict[str, Any]] = []
    for binding_id, resource in sorted(known.items()):
        samples = [item for item in checked if item["binding_id"] == binding_id]
        if len(samples) < policy["min_samples"]:
            continue
        accepted = sum(item["outcome"] == "accepted" for item in samples)
        transport_failures = sum(
            item["outcome"] == "transport_failure"
            for item in samples
        )
        success_rate = accepted / len(samples)
        delta = policy["max_score_delta"]
        base_scores = resource["scores"]
        quality_target = (
            base_scores["quality"] + ((success_rate * 2.0) - 1.0) * delta
        )
        measured_cost = sum(item["cost_usd"] for item in samples) / len(samples)
        measured_latency = (
            sum(item["latency_seconds"] for item in samples) / len(samples)
        )
        health = resource["health"]
        if (
            policy["allow_health_downgrade"]
            and health == "healthy"
            and transport_failures >= policy["failure_downgrade_count"]
        ):
            health = "degraded"
        adjustments.append({
            "binding_id": binding_id,
            "identity_digest": _identity_digest(resource),
            "sample_count": len(samples),
            "receipt_digests": sorted(
                item["receipt_digest"]
                for item in samples
            ),
            "outcome_counts": {
                outcome: sum(item["outcome"] == outcome for item in samples)
                for outcome in sorted(OUTCOMES)
            },
            "scores": {
                "quality": _bounded(quality_target, base_scores["quality"], delta),
                "cost": _bounded(measured_cost, base_scores["cost"], delta),
                "latency": _bounded(
                    measured_latency,
                    base_scores["latency"],
                    delta,
                ),
            },
            "health": health,
        })
    generated_at = now.isoformat()
    valid_until = (
        now + timedelta(seconds=policy["projection_ttl_seconds"])
    ).isoformat()
    return {
        "schema": PROJECTION_SCHEMA,
        "base_routing_authority_digest": authority_digest,
        "registry_revision": normalized["registry"]["revision"],
        "generated_at": generated_at,
        "valid_until": valid_until,
        "observations_digest": cr.digest_json(checked),
        "adjustments": adjustments,
    }


def apply_projection(
    graph: dict[str, Any],
    projection: dict[str, Any],
    *,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Apply only a valid, current, identity-bound score/health projection."""
    normalized = cr.validate_graph(graph)
    policy = validate_policy(normalized["policy"].get("adaptive"))
    if not isinstance(projection, dict) or projection.get("schema") != PROJECTION_SCHEMA:
        raise ValueError(f"projection.schema must be {PROJECTION_SCHEMA}")
    expected = {
        "schema",
        "base_routing_authority_digest",
        "registry_revision",
        "generated_at",
        "valid_until",
        "observations_digest",
        "adjustments",
    }
    if set(projection) != expected:
        raise ValueError("projection has missing or unexpected fields")
    base_digest = _digest(
        "projection.base_routing_authority_digest",
        projection.get("base_routing_authority_digest"),
    )
    if base_digest != normalized.get("routing_authority_digest"):
        raise ValueError("projection is bound to a different routing authority")
    if projection.get("registry_revision") != normalized["registry"]["revision"]:
        raise ValueError("projection is bound to a different registry revision")
    generated_text, generated_at = _instant(
        "projection.generated_at",
        projection.get("generated_at"),
    )
    valid_text, valid_until = _instant(
        "projection.valid_until",
        projection.get("valid_until"),
    )
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if (
        generated_at > now
        or valid_until <= generated_at
        or valid_until <= now
        or (valid_until - generated_at).total_seconds()
        > policy["projection_ttl_seconds"]
    ):
        raise ValueError("adaptive projection is stale or inverted")
    _digest("projection.observations_digest", projection.get("observations_digest"))
    adjustments = projection.get("adjustments")
    if not isinstance(adjustments, list):
        raise ValueError("projection.adjustments must be an array")
    resources = {
        resource["binding_id"]: resource
        for resource in normalized["registry"]["resources"]
    }
    seen: set[str] = set()
    seen_receipts: set[str] = set()
    for adjustment in adjustments:
        if not isinstance(adjustment, dict) or set(adjustment) != {
            "binding_id",
            "identity_digest",
            "sample_count",
            "receipt_digests",
            "outcome_counts",
            "scores",
            "health",
        }:
            raise ValueError("projection adjustment has an invalid shape")
        binding_id = adjustment.get("binding_id")
        if binding_id in seen or binding_id not in resources:
            raise ValueError("projection adjustment binding is duplicate or unknown")
        seen.add(binding_id)
        resource = resources[binding_id]
        if _digest(
            f"{binding_id}.identity_digest",
            adjustment.get("identity_digest"),
        ) != _identity_digest(resource):
            raise ValueError("projection resource identity does not match authority")
        sample_count = adjustment.get("sample_count")
        receipt_digests = adjustment.get("receipt_digests")
        if (
            not isinstance(sample_count, int)
            or sample_count < policy["min_samples"]
            or not isinstance(receipt_digests, list)
            or len(receipt_digests) != sample_count
        ):
            raise ValueError("projection sample evidence is incomplete")
        for digest in receipt_digests:
            _digest(f"{binding_id}.receipt_digest", digest)
            if digest in seen_receipts:
                raise ValueError("projection replays a receipt digest")
            seen_receipts.add(digest)
        outcomes = adjustment.get("outcome_counts")
        if (
            not isinstance(outcomes, dict)
            or set(outcomes) != OUTCOMES
            or any(
                not isinstance(value, int) or value < 0
                for value in outcomes.values()
            )
            or sum(outcomes.values()) != sample_count
        ):
            raise ValueError("projection outcome counts are incomplete")
        scores = adjustment.get("scores")
        if not isinstance(scores, dict) or set(scores) != {
            "quality",
            "cost",
            "latency",
        }:
            raise ValueError("projection scores have an invalid shape")
        health = adjustment.get("health")
        if health not in _HEALTH_RANK:
            raise ValueError("projection health is outside the closed set")
        if _HEALTH_RANK[health] > _HEALTH_RANK[resource["health"]]:
            raise ValueError("adaptive projection cannot upgrade health")
        if health != resource["health"] and (
            not policy["allow_health_downgrade"]
            or outcomes["transport_failure"]
            < policy["failure_downgrade_count"]
        ):
            raise ValueError("health downgrade lacks policy-bound failure evidence")
        projected_scores = {
            key: _number(f"{binding_id}.scores.{key}", scores[key])
            for key in ("quality", "cost", "latency")
        }
        for key, value in projected_scores.items():
            if abs(value - resource["scores"][key]) > (
                policy["max_score_delta"] + 1e-9
            ):
                raise ValueError("projection score exceeds the approved delta")
        expected_quality = _bounded(
            resource["scores"]["quality"]
            + (
                (
                    outcomes["accepted"] / sample_count
                ) * 2.0 - 1.0
            ) * policy["max_score_delta"],
            resource["scores"]["quality"],
            policy["max_score_delta"],
        )
        if projected_scores["quality"] != expected_quality:
            raise ValueError("projection quality is not derived from closed outcomes")
        resource["scores"] = projected_scores
        resource["health"] = health
    normalized["adaptive_projection_digest"] = cr.digest_json(projection)
    return normalized

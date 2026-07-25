#!/usr/bin/env python3
"""Provider-free acceptance gate for bounded adaptive routing projections."""
from __future__ import annotations

import copy
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import adaptive_routing as ar
import capability_resolver as cr
from routing_authority_canary import routing_authority, work_graph

NOW = datetime(2026, 7, 24, 13, 30, tzinfo=timezone.utc)


def _rejects(fn: Callable[[], Any]) -> bool:
    try:
        fn()
        return False
    except ValueError:
        return True


def _observation(
    ordinal: int,
    outcome: str,
    *,
    binding_id: str = "edit-codex",
) -> dict[str, Any]:
    return {
        "schema": ar.OBSERVATION_SCHEMA,
        "binding_id": binding_id,
        "receipt_digest": "sha256:" + f"{ordinal:064x}",
        "acceptance_authority": (
            "transport"
            if outcome == "transport_failure"
            else ar.ACCEPTANCE_AUTHORITY
        ),
        "outcome": outcome,
        "latency_seconds": ordinal / 10,
        "cost_usd": ordinal / 100,
        "measured_at": (NOW + timedelta(seconds=ordinal)).isoformat(),
    }


def _graph() -> dict[str, Any]:
    authority = routing_authority()
    authority["policy"]["adaptive"] = {
        "enabled": True,
        "min_samples": 3,
        "max_score_delta": 0.25,
        "projection_ttl_seconds": 3600,
        "failure_downgrade_count": 2,
        "allow_health_downgrade": True,
    }
    return cr.compose_graph(work_graph(), authority, at=NOW)


def main() -> int:
    cases: list[dict[str, Any]] = []
    graph = _graph()
    observations = [
        _observation(1, "accepted"),
        _observation(2, "accepted"),
        _observation(3, "rejected"),
    ]
    projection = ar.build_projection(graph, observations, at=NOW)
    applied = ar.apply_projection(graph, projection, at=NOW)
    original = next(
        item for item in graph["registry"]["resources"]
        if item["binding_id"] == "edit-codex"
    )
    projected = next(
        item for item in applied["registry"]["resources"]
        if item["binding_id"] == "edit-codex"
    )
    projected_binding = cr.resolve_operation(
        applied,
        "produce_change",
    )["binding"]
    cases.append({
        "id": "committed-verdict-and-measured-cost-latency-project-scores",
        "ok": (
            len(projection["adjustments"]) == 1
            and projected["scores"]["quality"] > original["scores"]["quality"]
            and projected["scores"]["cost"] == 0.02
            and projected["scores"]["latency"] == 0.2
            and projected["runner"] == original["runner"]
            and projected["model_family"] == original["model_family"]
            and projected["capabilities"] == original["capabilities"]
            and projected["permission_ceiling"] == original["permission_ceiling"]
            and projected["trust_tier"] == original["trust_tier"]
            and isinstance(applied.get("adaptive_projection_digest"), str)
            and projected_binding.get("adaptive_projection_digest")
            == applied.get("adaptive_projection_digest")
        ),
        "detail": json.dumps(
            {
                "scores": projected["scores"],
                "trust": projected["trust_tier"],
                "projection": applied.get("adaptive_projection_digest"),
            },
            sort_keys=True,
        ),
    })

    insufficient = ar.build_projection(graph, observations[:2], at=NOW)
    failures = [
        _observation(4, "transport_failure"),
        _observation(5, "transport_failure"),
        _observation(6, "accepted"),
    ]
    degraded_projection = ar.build_projection(graph, failures, at=NOW)
    degraded = ar.apply_projection(graph, degraded_projection, at=NOW)
    degraded_resource = next(
        item for item in degraded["registry"]["resources"]
        if item["binding_id"] == "edit-codex"
    )
    cases.append({
        "id": "minimum-sample-gate-and-health-downgrade-only",
        "ok": (
            insufficient["adjustments"] == []
            and degraded_resource["health"] == "degraded"
            and degraded_resource["trust_tier"] == original["trust_tier"]
        ),
        "detail": (
            f"insufficient={len(insufficient['adjustments'])}; "
            f"health={degraded_resource['health']}"
        ),
    })

    replayed = observations + [copy.deepcopy(observations[0])]
    self_scored = copy.deepcopy(observations[0])
    self_scored["acceptance_authority"] = "model_self_report"
    cases.append({
        "id": "replay-and-model-self-score-are-rejected",
        "ok": (
            _rejects(lambda: ar.build_projection(graph, replayed, at=NOW))
            and _rejects(lambda: ar.validate_observation(self_scored))
        ),
        "detail": "receipt replay and model self-report both fail closed",
    })

    wrong_base = copy.deepcopy(projection)
    wrong_base["base_routing_authority_digest"] = "sha256:" + "f" * 64
    oversized_score = copy.deepcopy(projection)
    oversized_score["adjustments"][0]["scores"]["quality"] += 1
    overlong_ttl = copy.deepcopy(projection)
    overlong_ttl["valid_until"] = (NOW + timedelta(hours=2)).isoformat()
    changed_identity = copy.deepcopy(graph)
    changed_identity["registry"]["resources"][0]["provider_ref"] = "different"
    stale_at = NOW + timedelta(hours=2)
    cases.append({
        "id": "authority-identity-and-ttl-tamper-are-rejected",
        "ok": (
            _rejects(
                lambda: ar.apply_projection(graph, wrong_base, at=NOW)
            )
            and _rejects(
                lambda: ar.apply_projection(graph, oversized_score, at=NOW)
            )
            and _rejects(
                lambda: ar.apply_projection(graph, overlong_ttl, at=NOW)
            )
            and _rejects(
                lambda: ar.apply_projection(
                    changed_identity,
                    projection,
                    at=NOW,
                )
            )
            and _rejects(
                lambda: ar.apply_projection(graph, projection, at=stale_at)
            )
        ),
        "detail": (
            "base digest, score delta, resource identity, and projection TTL "
            "are bound"
        ),
    })

    degraded_base = copy.deepcopy(graph)
    degraded_base["registry"]["resources"][0]["health"] = "degraded"
    degraded_base["registry"]["resources"][0]["health_evidence"][
        "status"
    ] = "degraded"
    upgrade = copy.deepcopy(projection)
    upgrade["adjustments"][0]["health"] = "healthy"
    cases.append({
        "id": "projection-cannot-upgrade-health-or-trust",
        "ok": _rejects(
            lambda: ar.apply_projection(degraded_base, upgrade, at=NOW)
        ),
        "detail": "automatic projection has no promotion path",
    })

    failures_out = [
        {"id": case["id"], "detail": case["detail"]}
        for case in cases
        if not case["ok"]
    ]
    print(json.dumps({
        "check_id": "lh-adaptive-routing",
        "status": "pass" if not failures_out else "fail",
        "total": len(cases),
        "blocking_failures": failures_out,
        "verification": {
            "command": "python3 -B lh_runtime/adaptive_routing_canary.py",
            "provider_invocations": 0,
        },
    }, ensure_ascii=False, indent=2))
    return 0 if not failures_out else 1


if __name__ == "__main__":
    raise SystemExit(main())

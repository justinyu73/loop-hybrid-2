#!/usr/bin/env python3
"""Read provider-reported 5HR quota pressure without invoking a model.

Standalone port: the source's external-ledger CLI was intentionally removed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA = "lh-provider-quota-policy/v1"
OBSERVATION_SCHEMA = "lh-provider-quota-observation/v1"


def parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty ISO-8601 string")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("JSON root must be an object")
    return value


def validate_policy(policy: Any) -> list[str]:
    if not isinstance(policy, dict):
        return ["quota policy must be an object"]
    problems: list[str] = []
    if policy.get("schema") != SCHEMA:
        problems.append(f"schema must be {SCHEMA!r}")
    if not isinstance(policy.get("policy_id"), str) or not policy["policy_id"].strip():
        problems.append("policy_id must be non-empty")
    if policy.get("window_minutes") != 300:
        problems.append("window_minutes must be 300")
    notify = policy.get("notify_used_percent")
    soft = policy.get("soft_used_percent")
    hard = policy.get("hard_used_percent")
    if not isinstance(notify, (int, float)) or isinstance(notify, bool) or not 0 < notify < 100:
        problems.append("notify_used_percent must be between 0 and 100")
    if not isinstance(soft, (int, float)) or isinstance(soft, bool) or not 0 < soft < 100:
        problems.append("soft_used_percent must be between 0 and 100")
    if not isinstance(hard, (int, float)) or isinstance(hard, bool) or not 0 < hard <= 100:
        problems.append("hard_used_percent must be between 0 and 100")
    if isinstance(notify, (int, float)) and isinstance(soft, (int, float)) and notify >= soft:
        problems.append("notify_used_percent must be lower than soft_used_percent")
    if isinstance(soft, (int, float)) and isinstance(hard, (int, float)) and soft >= hard:
        problems.append("soft_used_percent must be lower than hard_used_percent")
    for field, minimum in [("max_age_seconds", 1), ("clock_skew_seconds", 0)]:
        value = policy.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            problems.append(f"{field} must be an integer >= {minimum}")
    sources = policy.get("sources")
    if not isinstance(sources, dict) or sources.get("codex") != ["codex_app_server", "codex_session_jsonl"]:
        problems.append("sources.codex must prefer codex_app_server with codex_session_jsonl fallback")
    if policy.get("unknown_or_stale_policy") != "zero_cost_only":
        problems.append("unknown_or_stale_policy must be 'zero_cost_only'")
    claims = policy.get("cannot_claim")
    required = {"fixed token denominator", "permission to invoke a provider", "background monitoring"}
    if not isinstance(claims, list) or not required.issubset(set(claims)):
        problems.append(f"cannot_claim must include {sorted(required)}")
    return problems


def extract_app_server_observation(response: Any, *, observed_at: str) -> dict[str, Any]:
    if not isinstance(response, dict):
        raise ValueError("app-server response must be an object")
    result = response.get("result")
    snapshot = result.get("rateLimits") if isinstance(result, dict) else None
    primary = snapshot.get("primary") if isinstance(snapshot, dict) else None
    if not isinstance(primary, dict) or primary.get("windowDurationMins") != 300:
        raise ValueError("app-server response has no 5HR primary window")
    used = primary.get("usedPercent")
    resets_at = primary.get("resetsAt")
    if not isinstance(used, (int, float)) or isinstance(used, bool) or not 0 <= used <= 100:
        raise ValueError("app-server 5HR usedPercent must be between 0 and 100")
    if not isinstance(resets_at, int) or isinstance(resets_at, bool) or resets_at < 1:
        raise ValueError("app-server 5HR resetsAt must be a positive epoch second")
    observed = parse_timestamp(observed_at)
    return {
        "schema": OBSERVATION_SCHEMA, "runner": "codex", "thread_id": None,
        "window_minutes": 300, "used_percent": float(used),
        "remaining_percent": float(100 - used), "observed_at": observed.isoformat(),
        "resets_at": datetime.fromtimestamp(resets_at, tz=timezone.utc).isoformat(),
        "limit_id": snapshot.get("limitId"), "plan_type": snapshot.get("planType"),
        "source": "codex_app_server",
        "cannot_claim": ["fixed token denominator", "provider billing accuracy", "credentials copied"],
    }


def observe(
    policy: dict[str, Any], *, runner: str, evaluated_at: str | None = None,
    app_server_reader: Any,
) -> dict[str, Any]:
    problems = validate_policy(policy)
    now = parse_timestamp(evaluated_at) if evaluated_at else datetime.now(timezone.utc)
    if problems:
        return {"verdict": "ng", "status": "invalid_policy", "allowed_to_dispatch": False, "problems": problems}
    if (policy.get("sources") or {}).get(runner) != ["codex_app_server", "codex_session_jsonl"]:
        return {"verdict": "pass", "status": "quota_unknown_stopped", "allowed_to_dispatch": False,
                "runner": runner, "observation": None, "problems": [],
                "cannot_claim": ["zero usage", "permission to invoke a provider"]}
    try:
        observation = extract_app_server_observation(app_server_reader(), observed_at=now.isoformat())
    except (OSError, ValueError, RuntimeError, TimeoutError, json.JSONDecodeError) as exc:
        return {"verdict": "pass", "status": "quota_unknown_stopped", "allowed_to_dispatch": False,
                "runner": runner, "observation": None, "problems": [f"codex_app_server: {type(exc).__name__}: {exc}"],
                "cannot_claim": ["zero usage", "permission to invoke a provider"]}
    observed = parse_timestamp(observation["observed_at"])
    age = (now - observed).total_seconds()
    if age < -policy["clock_skew_seconds"]:
        status, allowed = "quota_future_stopped", False
    elif age > policy["max_age_seconds"]:
        status, allowed = "quota_stale_stopped", False
    elif observation["used_percent"] >= policy["hard_used_percent"]:
        status, allowed = "quota_hard_stopped", False
    elif observation["used_percent"] >= policy["soft_used_percent"]:
        status, allowed = "quota_soft_degradation", True
    elif observation["used_percent"] >= policy["notify_used_percent"]:
        status, allowed = "quota_notify", True
    else:
        status, allowed = "quota_normal", True
    return {
        "verdict": "pass", "status": status, "allowed_to_dispatch": allowed,
        "runner": runner, "policy_id": policy["policy_id"], "evaluated_at": now.isoformat(),
        "observation_age_seconds": int(age), "observation": observation,
        "degradation_required": status == "quota_soft_degradation", "zero_cost_only": not allowed,
        "problems": [], "source_problems": [],
    }

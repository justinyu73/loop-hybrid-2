"""Compile a human-ratified bounded delegation into an exact-call envelope."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import provider_egress as pe

SCHEMA = "loop-hybrid-provider-execution-delegation/v1"


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def validate(value: Any, policy: dict[str, Any]) -> list[str]:
    if not isinstance(value, dict):
        return ["delegation must be an object"]
    problems: list[str] = []
    if value.get("schema") != SCHEMA: problems.append(f"schema must be {SCHEMA}")
    for field in ("delegation_id", "target_repo", "expires_at"):
        if not isinstance(value.get(field), str) or not value[field]: problems.append(f"{field} must be non-empty")
    if not isinstance(value.get("roles"), list) or not value["roles"] or not set(value["roles"]).issubset({"router", "executor"}): problems.append("roles must contain router and/or executor")
    profiles = value.get("provider_profiles")
    if not isinstance(profiles, list) or not profiles or not set(profiles).issubset(set((policy.get("provider_profiles") or {}))): problems.append("provider_profiles must be allowed by policy")
    if not isinstance(value.get("max_calls"), int) or value.get("max_calls", 0) < 1: problems.append("max_calls must be >= 1")
    contract = {key: value.get(key) for key in ("schema", "delegation_id", "target_repo", "roles", "provider_profiles", "max_calls", "expires_at")}
    ratification = value.get("ratification") or {}
    if ratification.get("state") != "ratified" or ratification.get("ratified_by") != "human" or ratification.get("contract_digest") != _digest(contract): problems.append("ratification must be human and bind delegation contract")
    try: _time(value.get("expires_at", ""))
    except Exception: problems.append("expires_at is invalid")
    return problems


def compile_exact_call(delegation: dict[str, Any], *, role: str, provider_profile: str, capsule: dict[str, Any], requested_at: str, call_suffix: str, policy: dict[str, Any], prior_admissions: list[dict[str, Any]]) -> dict[str, Any]:
    problems = validate(delegation, policy)
    if role not in (delegation.get("roles") or []): problems.append("role is outside delegation")
    if provider_profile not in (delegation.get("provider_profiles") or []): problems.append("provider_profile is outside delegation")
    try:
        if _time(requested_at) >= _time(delegation.get("expires_at", "")): problems.append("delegation is expired")
    except Exception: problems.append("requested_at is invalid")
    prefix = str(delegation.get("delegation_id", "")) + ":"
    used = len([row for row in prior_admissions if row.get("type") == "provider_egress_admitted" and str(row.get("call_id", "")).startswith(prefix)])
    if used >= delegation.get("max_calls", 0): problems.append("delegation call cap reached")
    if problems:
        return {"verdict": "ng", "status": "delegation_stopped", "provider_calls": 0, "problems": problems}
    call_id = prefix + call_suffix
    envelope = {"schema": pe.ENVELOPE_SCHEMA, "call_id": call_id, "phase": role, "provider_profile": provider_profile, "capsule": capsule,
                "authorization": {"kind": "exact_call", "call_id": call_id, "capsule_digest": capsule.get("digest"), "expires_at": delegation["expires_at"]},
                "requested_at": requested_at, "cannot_claim": ["provider invoked", "platform authorization", "review passed", "product acceptance"]}
    problems = pe.validate_envelope(envelope, policy)
    return {"verdict": "pass" if not problems else "ng", "status": "exact_call_ready" if not problems else "invalid_exact_call", "provider_calls": 0,
            "envelope": envelope, "delegation_digest": _digest(delegation), "problems": problems}

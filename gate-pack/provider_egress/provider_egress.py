"""Fail-closed contract for host-authorized provider egress; never launches a provider."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ENVELOPE_SCHEMA = "loop-hybrid-provider-call-envelope/v1"
POLICY_SCHEMA = "loop-hybrid-provider-egress-policy/v1"
CAPABILITY_SCHEMA = "loop-hybrid-host-egress-capability/v1"
HostVerifier = Callable[[dict[str, Any]], dict[str, Any]]


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


class AppendOnlyLedger:
    """Hash-chained metadata only; capsule bytes and authorization tokens are excluded."""
    def __init__(self, path: Path):
        self.path = path

    def read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: list[dict[str, Any]] = []
        prior = "genesis"
        for seq, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            row = json.loads(line)
            body = {key: value for key, value in row.items() if key != "event_hash"}
            if row.get("seq") != seq or row.get("prior_hash") != prior or row.get("event_hash") != digest(body):
                raise ValueError("egress ledger hash chain is invalid")
            rows.append(row)
            prior = row["event_hash"]
        return rows

    def append(self, event: dict[str, Any]) -> dict[str, Any]:
        rows = self.read()
        body = {"seq": len(rows) + 1, "prior_hash": rows[-1]["event_hash"] if rows else "genesis", **event}
        row = {**body, "event_hash": digest(body)}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (json.dumps(row, sort_keys=True) + "\n").encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        return row


def validate_policy(policy: Any) -> list[str]:
    if not isinstance(policy, dict):
        return ["policy must be an object"]
    problems: list[str] = []
    if policy.get("schema") != POLICY_SCHEMA:
        problems.append(f"policy.schema must be {POLICY_SCHEMA}")
    for field in ("policy_id", "gateway_id"):
        if not isinstance(policy.get(field), str) or not policy[field]:
            problems.append(f"policy.{field} must be non-empty")
    issuers = policy.get("trusted_host_issuers")
    if not isinstance(issuers, list) or not issuers or not all(isinstance(item, str) and item for item in issuers):
        problems.append("policy.trusted_host_issuers must be a non-empty string array")
    profiles = policy.get("provider_profiles")
    if not isinstance(profiles, dict) or not profiles:
        problems.append("policy.provider_profiles must be a non-empty object")
    required = {"network_boundary": "host_managed_sole_egress", "credential_boundary": "gateway_only", "audit_boundary": "host_append_only"}
    for field, expected in required.items():
        if policy.get(field) != expected:
            problems.append(f"policy.{field} must be {expected}")
    return problems


def validate_envelope(envelope: Any, policy: dict[str, Any]) -> list[str]:
    if not isinstance(envelope, dict):
        return ["envelope must be an object"]
    problems: list[str] = []
    allowed = {"schema", "call_id", "phase", "provider_profile", "capsule", "authorization", "requested_at", "cannot_claim"}
    if set(envelope) - allowed:
        problems.append("envelope contains forbidden fields")
    if envelope.get("schema") != ENVELOPE_SCHEMA:
        problems.append(f"envelope.schema must be {ENVELOPE_SCHEMA}")
    for field in ("call_id", "phase", "provider_profile", "requested_at"):
        if not isinstance(envelope.get(field), str) or not envelope[field]:
            problems.append(f"envelope.{field} must be non-empty")
    if envelope.get("provider_profile") not in (policy.get("provider_profiles") or {}):
        problems.append("provider_profile is not allowed")
    capsule = envelope.get("capsule")
    if not isinstance(capsule, dict) or set(capsule) != {"handle", "digest", "classification", "byte_count"}:
        problems.append("capsule must contain only handle, digest, classification, and byte_count")
        capsule = {}
    if not isinstance(capsule.get("digest"), str) or not capsule.get("digest", "").startswith("sha256:") or len(capsule.get("digest", "")) != 71:
        problems.append("capsule.digest must be a sha256 digest")
    if capsule.get("classification") not in {"public_safe", "provider_approved"}:
        problems.append("capsule.classification is not allowed")
    if not isinstance(capsule.get("byte_count"), int) or isinstance(capsule.get("byte_count"), bool) or capsule.get("byte_count", -1) < 0:
        problems.append("capsule.byte_count must be an integer >= 0")
    auth = envelope.get("authorization")
    if not isinstance(auth, dict) or set(auth) != {"kind", "call_id", "capsule_digest", "expires_at"}:
        problems.append("authorization must contain only kind, call_id, capsule_digest, and expires_at")
        auth = {}
    if auth.get("kind") != "exact_call" or auth.get("call_id") != envelope.get("call_id") or auth.get("capsule_digest") != capsule.get("digest"):
        problems.append("authorization must bind the exact call and capsule digest")
    try:
        if timestamp(auth.get("expires_at", "")) <= timestamp(envelope.get("requested_at", "")):
            problems.append("authorization must expire after requested_at")
    except (TypeError, ValueError):
        problems.append("authorization/request timestamps are invalid")
    required = {"provider invoked", "platform authorization", "review passed", "product acceptance"}
    if not isinstance(envelope.get("cannot_claim"), list) or not required.issubset(set(envelope["cannot_claim"])):
        problems.append("cannot_claim is incomplete")
    return problems


def validate_host_facts(facts: Any, envelope: dict[str, Any], policy: dict[str, Any], now: datetime) -> list[str]:
    if not isinstance(facts, dict):
        return ["host verifier must return an object"]
    checks = {"schema": CAPABILITY_SCHEMA, "gateway_id": policy.get("gateway_id"), "call_id": envelope.get("call_id"),
              "capsule_digest": envelope["capsule"]["digest"], "provider_profile": envelope.get("provider_profile"),
              "network_boundary": "host_managed_sole_egress", "credential_boundary": "gateway_only", "audit_boundary": "host_append_only",
              "payload_verified_by_host": True, "provider_profile_verified_by_host": True, "quota_status": "allowed"}
    problems = [f"host capability {field} does not match {expected!r}" for field, expected in checks.items() if facts.get(field) != expected]
    if facts.get("issuer") not in policy.get("trusted_host_issuers", []):
        problems.append("host capability issuer is not trusted")
    try:
        if timestamp(facts.get("expires_at", "")) <= now:
            problems.append("host capability is expired")
    except (TypeError, ValueError):
        problems.append("host capability expires_at is invalid")
    return problems


def admit(envelope: dict[str, Any], policy: dict[str, Any], ledger: AppendOnlyLedger, *, host_verifier: HostVerifier | None = None, evaluated_at: str | None = None) -> dict[str, Any]:
    problems = [*validate_policy(policy), *validate_envelope(envelope, policy)]
    call_id = envelope.get("call_id") if isinstance(envelope, dict) else None
    capsule_digest = (envelope.get("capsule") or {}).get("digest") if isinstance(envelope, dict) else None
    def stop(status: str, issues: list[str] | None = None) -> dict[str, Any]:
        row = ledger.append({"type": "provider_egress_stopped", "call_id": call_id, "capsule_digest": capsule_digest, "status": status, "problems": issues or [], "provider_calls": 0})
        return {"verdict": "ng" if issues else "pass", "status": status, "provider_calls": 0, "event": row, "problems": issues or []}
    if problems:
        return stop("invalid_contract", problems)
    prior = [row for row in ledger.read() if row.get("call_id") == call_id]
    admitted = [row for row in prior if row.get("type") == "provider_egress_admitted"]
    if admitted:
        return {"verdict": "pass", "status": "duplicate_noop", "provider_calls": 0, "event": admitted[-1], "problems": []}
    if prior and prior[-1].get("status") != "platform_authorization_required":
        return {"verdict": "ng", "status": "call_id_conflict", "provider_calls": 0, "event": prior[-1], "problems": ["call_id has a prior non-resumable stop"]}
    if host_verifier is None:
        return stop("platform_authorization_required")
    try:
        facts = host_verifier(envelope)
    except Exception as exc:
        return stop("host_verifier_error", [f"host verifier raised {type(exc).__name__}"])
    problems = validate_host_facts(facts, envelope, policy, timestamp(evaluated_at) if evaluated_at else datetime.now(timezone.utc))
    if problems:
        return stop("platform_authorization_invalid", problems)
    row = ledger.append({"type": "provider_egress_admitted", "call_id": call_id, "capsule_digest": capsule_digest,
                         "provider_profile": envelope["provider_profile"], "phase": envelope["phase"], "gateway_id": facts["gateway_id"],
                         "capability_id": facts.get("capability_id"), "host_facts_digest": digest(facts), "status": "gateway_dispatch_ready",
                         "provider_calls": 0, "cannot_claim": ["provider invoked", "provider response", "review passed", "product acceptance"]})
    return {"verdict": "pass", "status": "gateway_dispatch_ready", "provider_calls": 0, "event": row, "problems": []}

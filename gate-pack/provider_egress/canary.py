#!/usr/bin/env python3
"""Canary for the standalone host-attested, metadata-only egress contract."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import delegation
import provider_egress as pe

HERE = Path(__file__).resolve().parent
POLICY = json.loads((HERE / "policy.example.json").read_text())
ENVELOPE = json.loads((HERE / "envelope.example.json").read_text())
NOW = "2026-07-11T12:05:00Z"


def facts(request: dict) -> dict:
    return {"schema": pe.CAPABILITY_SCHEMA, "issuer": "managed-host", "gateway_id": POLICY["gateway_id"], "capability_id": "cap-example",
            "call_id": request["call_id"], "capsule_digest": request["capsule"]["digest"], "provider_profile": request["provider_profile"],
            "network_boundary": "host_managed_sole_egress", "credential_boundary": "gateway_only", "audit_boundary": "host_append_only",
            "payload_verified_by_host": True, "provider_profile_verified_by_host": True, "quota_status": "allowed", "expires_at": "2026-07-11T12:30:00Z"}


def forged_facts(request: dict) -> dict:
    value = facts(request)
    value["payload_verified_by_host"] = False
    return value


def ratified_delegation() -> dict:
    value = {"schema": delegation.SCHEMA, "delegation_id": "canary-delegation", "target_repo": "loop-hybrid", "roles": ["executor"],
             "provider_profiles": ["codex_p3"], "max_calls": 1, "expires_at": "2026-07-11T13:00:00Z"}
    contract = {key: value[key] for key in ("schema", "delegation_id", "target_repo", "roles", "provider_profiles", "max_calls", "expires_at")}
    value["ratification"] = {"state": "ratified", "ratified_by": "human", "contract_digest": delegation._digest(contract)}
    return value


def check(case_id: str, condition: bool, detail: str) -> dict:
    return {"id": case_id, "ok": condition, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "events.jsonl"
        stopped = pe.admit(ENVELOPE, POLICY, pe.AppendOnlyLedger(path), evaluated_at=NOW)
    with tempfile.TemporaryDirectory() as tmp:
        ledger = pe.AppendOnlyLedger(Path(tmp) / "events.jsonl")
        admitted = pe.admit(ENVELOPE, POLICY, ledger, host_verifier=facts, evaluated_at=NOW)
        metadata_only = "sealed-capsule:example" not in json.dumps(ledger.read())
    bad = copy.deepcopy(ENVELOPE); bad["authorization"]["call_id"] = "other"
    with tempfile.TemporaryDirectory() as tmp:
        invalid = pe.admit(bad, POLICY, pe.AppendOnlyLedger(Path(tmp) / "events.jsonl"), host_verifier=facts, evaluated_at=NOW)
    with tempfile.TemporaryDirectory() as tmp:
        forged = pe.admit(ENVELOPE, POLICY, pe.AppendOnlyLedger(Path(tmp) / "events.jsonl"), host_verifier=forged_facts, evaluated_at=NOW)
    ready = delegation.compile_exact_call(ratified_delegation(), role="executor", provider_profile="codex_p3", capsule=copy.deepcopy(ENVELOPE["capsule"]), requested_at="2026-07-11T12:00:00Z", call_suffix="1", policy=POLICY, prior_admissions=[])
    capped = delegation.compile_exact_call(ratified_delegation(), role="executor", provider_profile="codex_p3", capsule=copy.deepcopy(ENVELOPE["capsule"]), requested_at="2026-07-11T12:00:00Z", call_suffix="2", policy=POLICY, prior_admissions=[{"type": "provider_egress_admitted", "call_id": "canary-delegation:1"}])
    cases = [check("missing-host-stops", stopped["status"] == "platform_authorization_required" and stopped["provider_calls"] == 0, stopped["status"]),
             check("verified-host-is-metadata-only", admitted["status"] == "gateway_dispatch_ready" and admitted["provider_calls"] == 0 and metadata_only, admitted["status"]),
             check("forged-host-facts-stop", forged["status"] == "platform_authorization_invalid" and forged["provider_calls"] == 0, forged["status"]),
             check("exact-binding-stops", invalid["status"] == "invalid_contract", invalid["status"]),
             check("ratified-delegation-compiles", ready["status"] == "exact_call_ready" and ready["provider_calls"] == 0, ready["status"]),
             check("delegation-call-cap-stops", capped["status"] == "delegation_stopped", capped["status"])]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "provider-egress-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["gateway_dispatch_ready is not provider execution; a real host verifier remains external"]}, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

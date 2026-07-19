#!/usr/bin/env python3
"""Canary for the manual, host-egress-bound, model-neutral Design Grill."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parent / "provider_egress")]
import design_grill as dg  # noqa: E402
import provider_egress as pe  # noqa: E402


def host_facts(envelope: dict, policy: dict) -> dict:
    return {"schema": pe.CAPABILITY_SCHEMA, "issuer": "managed-host", "gateway_id": policy["gateway_id"], "capability_id": "design-grill-canary", "call_id": envelope["call_id"], "capsule_digest": envelope["capsule"]["digest"], "provider_profile": envelope["provider_profile"], "network_boundary": "host_managed_sole_egress", "credential_boundary": "gateway_only", "audit_boundary": "host_append_only", "payload_verified_by_host": True, "provider_profile_verified_by_host": True, "quota_status": "allowed", "expires_at": "2026-07-13T01:30:00Z"}


def result(contract: dict, route: str = "require_maintainer") -> dict:
    return {"schema": dg.RESULT_SCHEMA, "call_id": contract["call_id"], "contract_digest": dg.digest(contract), "review_id": contract["review_id"], "runner": contract["runner"], "model_binding": contract["model_binding"], "findings": ["bounded feasibility concern"], "recommendation": "review the documented trade-off", "route": route, "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM)}


def execution_receipt(contract: dict, policy: dict, ledger: pe.AppendOnlyLedger) -> dict:
    admitted = pe.admit(contract["egress_envelope"], policy, ledger, host_verifier=lambda envelope: host_facts(envelope, policy), evaluated_at="2026-07-12T23:05:00Z")
    return {"schema": dg.HOST_RECEIPT_SCHEMA, "status": "gateway_execution_recorded", "provider_calls": 1, "event": admitted["event"], "execution": {"call_id": contract["call_id"], "capsule_digest": contract["capsule_digest"], "runner": contract["runner"], "model_binding": contract["model_binding"], "exit_code": 0, "artifact_ref": "runtime/design-grill-canary/output.json", "output_digest": "sha256:" + "0" * 64, "completed_at": "2026-07-12T23:06:00Z", "claim_level": "process_bound"}}


def check(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    policy = dg.load_json(HERE.parent / "provider_egress" / "policy.example.json")
    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as external_tmp:
        root = Path(tmp); (root / "AGENTS.md").write_text("# Goal\nAssess feasibility.\n", encoding="utf-8")
        spec = {"schema": dg.SPEC_SCHEMA, "grill_id": "model-neutral-review", "question": "Can an arbitrary model challenge a claimed design?", "documents": [{"ref": "AGENTS.md", "start_line": 1, "end_line": 2}], "subject": {"kind": "model_output", "label": "a proposed design", "binding": {"runner": "claude", "model": "opus"}}, "review_slots": [{"id": "counterevidence", "objective": "Find counterexamples.", "separation_from": ["subject"]}, {"id": "alternative-framing", "objective": "Find a different framing.", "separation_from": ["subject", "counterevidence"]}], "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM)}
        legacy = {"schema": "loop-hybrid-design-grill-spec/v1", "grill_id": "legacy", "question": "x", "documents": [{"ref": "AGENTS.md"}], "required_roles": ["griller", "challenger"], "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM)}
        session = Path(external_tmp) / "model-neutral-review"
        prepared = dg.prepare(spec, root, session)
        inside_stopped = dg.prepare(spec, root, root / "runtime" / "design-grills" / "inside-lh")
        legacy_stopped = dg.prepare(legacy, root, Path(external_tmp) / "legacy")
        target = root / "caller-target"; (target / "docs").mkdir(parents=True)
        (target / "docs" / "product-map.md").write_text("# Product map\nCaller-owned planning context.\n", encoding="utf-8")
        external_spec = {**spec, "grill_id": "caller-context-review", "documents": [{"ref": "docs/product-map.md", "start_line": 1, "end_line": 2}]}
        external_session = Path(external_tmp) / "caller-context-review"
        external_prepared = dg.prepare(external_spec, root, external_session, context_root=target)
        external_capsule = json.loads((external_session / "capsule.json").read_text(encoding="utf-8"))
        traversal = dg.prepare({**external_spec, "documents": [{"ref": "../outside.md", "start_line": 1, "end_line": 1}]}, root, Path(external_tmp) / "traversal", context_root=target)
        inside_target = dg.prepare(external_spec, root, target / "session", context_root=target)
        same_subject = dg.request(session, review_id="counterevidence", runner="claude", model="opus", provider_profile="claude_o", requested_at="2026-07-12T23:00:00Z", expires_at="2026-07-13T01:00:00Z", policy=policy)
        dependent_early = dg.request(session, review_id="alternative-framing", runner="claude", model="sonnet", provider_profile="claude_o", requested_at="2026-07-12T23:00:00Z", expires_at="2026-07-13T01:00:00Z", policy=policy)
        counterevidence = dg.request(session, review_id="counterevidence", runner="codex", model="gpt-5", provider_profile="codex_p3", requested_at="2026-07-12T23:00:00Z", expires_at="2026-07-13T01:00:00Z", policy=policy)
        alternative = dg.request(session, review_id="alternative-framing", runner="claude", model="sonnet", provider_profile="claude_o", requested_at="2026-07-12T23:00:00Z", expires_at="2026-07-13T01:00:00Z", policy=policy)
        ledger = pe.AppendOnlyLedger(Path(external_tmp) / "egress.jsonl")
        receipt_one = execution_receipt(counterevidence.get("contract", {}), policy, ledger)
        first = dg.record(session, counterevidence.get("contract", {}), receipt_one, result(counterevidence.get("contract", {})))
        missing = dg.record(session, alternative.get("contract", {}), {}, result(alternative.get("contract", {})))
        receipt_two = execution_receipt(alternative.get("contract", {}), policy, ledger)
        final = dg.record(session, alternative.get("contract", {}), receipt_two, result(alternative.get("contract", {})))
    cases = [
        check("prepare-model-neutral-topology", prepared["status"] == "design_grill_prepared", prepared["status"]),
        check("inside-lh-session-stops", inside_stopped["status"] == "invalid_design_grill_spec", inside_stopped["status"]),
        check("legacy-fixed-roles-stop", legacy_stopped["status"] == "invalid_design_grill_spec", legacy_stopped["status"]),
        check("caller-context-prepares-without-path-binding", external_prepared["status"] == "design_grill_prepared" and external_capsule["repository_root"] == "caller-owned-context" and str(target) not in json.dumps(external_capsule), external_prepared["status"]),
        check("caller-context-traversal-and-in-place-session-stop", traversal["status"] == "invalid_design_grill_spec" and inside_target["status"] == "invalid_design_grill_spec", f"{traversal['status']}/{inside_target['status']}"),
        check("subject-separation-and-slot-dependency-stop", same_subject["status"] == "design_grill_binding_separation_required" and dependent_early["status"] == "design_grill_separation_dependency_required", f"{same_subject['status']}/{dependent_early['status']}"),
        check("host-receipt-is-required", missing["status"] == "invalid_design_grill_result", missing["status"]),
        check("configured-reviews-wait-human", first["status"] == "design_grill_more_results_required" and final["status"] == "human_design_decision_required" and final["provider_calls"] == 0, f"{first['status']}/{final['status']}"),
    ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "design-grill-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "known_gaps_open": ["The host must execute and capture any provider call; this gate only issues and verifies contracts."]}, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

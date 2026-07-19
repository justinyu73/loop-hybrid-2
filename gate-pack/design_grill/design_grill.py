#!/usr/bin/env python3
"""Manual, host-egress-bound Design Grill; never dispatches a provider."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE.parent / "provider_egress"))
import provider_egress as pe  # noqa: E402

SPEC_SCHEMA = "loop-hybrid-design-grill-spec/v2"
CAPSULE_SCHEMA = "loop-hybrid-design-grill-capsule/v3"
CONTRACT_SCHEMA = "loop-hybrid-design-grill-call-contract/v3"
RESULT_SCHEMA = "loop-hybrid-design-grill-result/v2"
EVENT_SCHEMA = "loop-hybrid-design-grill-event/v3"
HOST_RECEIPT_SCHEMA = "loop-hybrid-design-grill-host-receipt/v1"
ROUTES = {"continue", "require_maintainer", "review_unavailable"}
REQUIRED_CANNOT_CLAIM = {
    "goal ratification", "plan approval", "implementation authorization",
    "provider invocation authority", "product acceptance", "protected-core approval",
}
BLOCKED_CONTEXT_PARTS = {".git", "auth", "payments", "secrets", "credentials"}


def digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("JSON root must be an object")
    return value


def _safe_ref(ref: Any, root: Path, *, local_only: bool) -> Path | None:
    if not isinstance(ref, str) or not ref or Path(ref).is_absolute() or ".." in Path(ref).parts:
        return None
    if any(part in BLOCKED_CONTEXT_PARTS or part.startswith(".env") for part in Path(ref).parts):
        return None
    if local_only and ref != "AGENTS.md" and ref != "README.md" and not ref.startswith(("docs/", "gate-pack/")):
        return None
    path = (root / ref).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return None
    return path if path.is_file() else None


def _events(capsule_dir: Path) -> list[dict[str, Any]]:
    path = capsule_dir / "events.jsonl"
    return [] if not path.exists() else [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append(capsule_dir: Path, event: dict[str, Any]) -> None:
    events = _events(capsule_dir)
    stamped = {**event, "seq": len(events) + 1, "recorded_at": datetime.now(timezone.utc).isoformat()}
    with (capsule_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(stamped, ensure_ascii=False, sort_keys=True) + "\n")


def _valid_binding(binding: Any) -> bool:
    return binding is None or (isinstance(binding, dict) and set(binding) == {"runner", "model"} and all(isinstance(binding.get(field), str) and binding[field] for field in ("runner", "model")))


def _binding_key(binding: dict[str, str]) -> tuple[str, str]:
    return binding["runner"], binding["model"]


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("sha256:") and len(value) == 71


def _external_session_problems(engine_root: Path, context_root: Path, session_dir: Path) -> list[str]:
    if not session_dir.is_absolute():
        return ["session_dir must be an absolute caller-owned path"]
    resolved_session = session_dir.resolve()
    for root, label in ((engine_root, "loop-hybrid repository"), (context_root, "caller context root")):
        try:
            resolved_session.relative_to(root.resolve())
            return [f"session_dir must be outside the {label}"]
        except ValueError:
            continue
    return []


def validate_spec(spec: Any) -> list[str]:
    if not isinstance(spec, dict):
        return ["spec must be an object"]
    problems: list[str] = []
    allowed = {"schema", "grill_id", "question", "documents", "subject", "review_slots", "cannot_claim"}
    if set(spec) - allowed:
        problems.append("spec contains unsupported fields")
    if spec.get("schema") != SPEC_SCHEMA:
        problems.append(f"schema must be {SPEC_SCHEMA}")
    for field in ("grill_id", "question"):
        if not isinstance(spec.get(field), str) or not spec[field].strip():
            problems.append(f"{field} must be non-empty")
    docs = spec.get("documents")
    if not isinstance(docs, list) or not docs or any(not isinstance(item, dict) for item in docs):
        problems.append("documents must be a non-empty object array")
    subject = spec.get("subject")
    if not isinstance(subject, dict) or set(subject) != {"kind", "label", "binding"}:
        problems.append("subject must contain only kind, label, and binding")
    elif not isinstance(subject.get("kind"), str) or not subject["kind"].strip() or not isinstance(subject.get("label"), str) or not subject["label"].strip() or not _valid_binding(subject.get("binding")):
        problems.append("subject values are invalid")
    slots = spec.get("review_slots")
    if not isinstance(slots, list) or not slots:
        problems.append("review_slots must be a non-empty array")
    else:
        ids = [item.get("id") for item in slots if isinstance(item, dict)]
        if len(ids) != len(slots) or any(not isinstance(item, str) or not item.strip() for item in ids) or len(set(ids)) != len(ids):
            problems.append("review slot ids must be unique non-empty strings")
        known = {"subject", *[item for item in ids if isinstance(item, str)]}
        for slot in slots:
            if not isinstance(slot, dict) or set(slot) != {"id", "objective", "separation_from"}:
                problems.append("each review slot must contain only id, objective, and separation_from")
                continue
            refs = slot.get("separation_from")
            if not isinstance(slot.get("objective"), str) or not slot["objective"].strip() or not isinstance(refs, list) or any(not isinstance(ref, str) or ref not in known or ref == slot.get("id") for ref in refs) or len(set(refs)) != len(refs):
                problems.append("review slot objective or separation_from is invalid")
            if "subject" in refs and isinstance(subject, dict) and subject.get("binding") is None:
                problems.append("subject binding is required when a review slot separates from subject")
    claims = spec.get("cannot_claim")
    if not isinstance(claims, list) or not REQUIRED_CANNOT_CLAIM.issubset(set(claims)):
        problems.append("cannot_claim is missing authority boundaries")
    return problems


def prepare(spec: dict[str, Any], root: Path, session_dir: Path, *, context_root: Path | None = None) -> dict[str, Any]:
    """Freeze explicit documents from LH or a caller-supplied, non-persistent root."""
    source_root = (context_root or root).resolve()
    problems = validate_spec(spec)
    if not source_root.is_dir():
        problems.append("context_root must be a readable directory")
    problems.extend(_external_session_problems(root, source_root, session_dir))
    documents: list[dict[str, str]] = []
    for item in spec.get("documents", []):
        path = _safe_ref(item.get("ref"), source_root, local_only=context_root is None) if isinstance(item, dict) else None
        if path is None:
            problems.append("document ref is outside the safe document set")
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        start, end = item.get("start_line", 1), item.get("end_line", len(lines))
        if not isinstance(start, int) or not isinstance(end, int) or start < 1 or end < start or end > len(lines) or end - start > 800:
            problems.append("document line range is invalid")
            continue
        documents.append({"ref": f"{item['ref']}:{start}-{end}", "text": "\n".join(lines[start - 1:end]) + "\n"})
    if problems:
        return {"verdict": "ng", "status": "invalid_design_grill_spec", "provider_calls": 0, "problems": problems}
    if session_dir.exists():
        return {"verdict": "ng", "status": "design_grill_session_exists", "provider_calls": 0, "problems": ["caller-owned session cannot be rewritten"]}
    context_digest = digest([{"ref": document["ref"], "digest": digest(document["text"])} for document in documents])
    capsule = {"schema": CAPSULE_SCHEMA, "grill_id": spec["grill_id"], "repository_root": "caller-owned-context", "context_digest": context_digest, "question": spec["question"], "subject": spec["subject"], "review_slots": spec["review_slots"], "documents": documents, "cannot_claim": sorted(REQUIRED_CANNOT_CLAIM)}
    raw = json.dumps(capsule, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    session_dir.mkdir(parents=True)
    (session_dir / "capsule.json").write_bytes(raw)
    manifest = {"grill_id": spec["grill_id"], "repository_root": "caller-owned-context", "context_digest": context_digest, "capsule_digest": "sha256:" + hashlib.sha256(raw).hexdigest(), "subject": spec["subject"], "review_slots": spec["review_slots"]}
    (session_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return {"verdict": "pass", "status": "design_grill_prepared", "provider_calls": 0, "session_dir": str(session_dir), "capsule_digest": manifest["capsule_digest"], "problems": []}


def request(session_dir: Path, *, review_id: str, runner: str, model: str, provider_profile: str, requested_at: str, expires_at: str, policy: dict[str, Any]) -> dict[str, Any]:
    try:
        capsule, manifest = load_json(session_dir / "capsule.json"), load_json(session_dir / "manifest.json")
        datetime.fromisoformat(requested_at.replace("Z", "+00:00")); datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"verdict": "ng", "status": "invalid_design_grill_request", "provider_calls": 0, "problems": [str(exc)]}
    slots = {slot["id"]: slot for slot in manifest.get("review_slots", []) if isinstance(slot, dict) and isinstance(slot.get("id"), str)}
    slot = slots.get(review_id)
    if slot is None or not runner or not model:
        return {"verdict": "ng", "status": "invalid_design_grill_request", "provider_calls": 0, "problems": ["review_id, runner, or model is invalid"]}
    binding = {"runner": runner, "model": model}
    events = _events(session_dir)
    issued = {event.get("review_id"): {"runner": event.get("runner"), "model": event.get("model_binding")} for event in events if event.get("type") == "design_grill_review_issued"}
    separation_bindings: list[dict[str, str]] = []
    for ref in slot.get("separation_from", []):
        if ref == "subject":
            subject_binding = manifest.get("subject", {}).get("binding")
            if not isinstance(subject_binding, dict):
                return {"verdict": "ng", "status": "design_grill_subject_binding_required", "provider_calls": 0, "problems": ["subject binding is required for this review slot"]}
            separation_bindings.append(subject_binding)
        elif ref not in issued:
            return {"verdict": "ng", "status": "design_grill_separation_dependency_required", "provider_calls": 0, "problems": [f"review slot {ref} must be issued first"]}
        else:
            separation_bindings.append(issued[ref])
    if any(_binding_key(binding) == _binding_key(other) for other in separation_bindings):
        return {"verdict": "ng", "status": "design_grill_binding_separation_required", "provider_calls": 0, "problems": ["review binding must differ from every configured separation target"]}
    payload = {"repository_root": manifest["repository_root"], "capsule_digest": manifest["capsule_digest"], "review_id": review_id, "objective": slot["objective"], "separation_from": slot["separation_from"], "subject": manifest["subject"], "runner": runner, "model_binding": model, "provider_profile": provider_profile, "requested_at": requested_at, "expires_at": expires_at, "max_calls": 1, "tools": [], "mcp_enabled": False, "background": False}
    call_id = f"design-grill:{capsule['grill_id']}:{review_id}:{digest(payload)[7:23]}"
    envelope = {"schema": pe.ENVELOPE_SCHEMA, "call_id": call_id, "phase": "design_grill", "provider_profile": provider_profile, "capsule": {"handle": str(session_dir / "capsule.json"), "digest": manifest["capsule_digest"], "classification": "public_safe", "byte_count": (session_dir / "capsule.json").stat().st_size}, "authorization": {"kind": "exact_call", "call_id": call_id, "capsule_digest": manifest["capsule_digest"], "expires_at": expires_at}, "requested_at": requested_at, "cannot_claim": ["provider invoked", "platform authorization", "review passed", "product acceptance"]}
    problems = pe.validate_policy(policy) + pe.validate_envelope(envelope, policy)
    if problems:
        return {"verdict": "ng", "status": "invalid_host_egress_envelope", "provider_calls": 0, "problems": problems}
    contract = {"schema": CONTRACT_SCHEMA, "call_id": call_id, **payload, "egress_envelope": envelope, "cannot_claim": sorted(REQUIRED_CANNOT_CLAIM)}
    prior = [event for event in events if event.get("type") == "design_grill_review_issued" and event.get("review_id") == review_id]
    if prior:
        return {"verdict": "pass" if prior[-1].get("contract_digest") == digest(contract) else "ng", "status": "host_egress_admission_required" if prior[-1].get("contract_digest") == digest(contract) else "design_grill_issuance_conflict", "provider_calls": 0, "contract": contract, "problems": [] if prior[-1].get("contract_digest") == digest(contract) else ["review slot already has a different immutable call contract"]}
    _append(session_dir, {"type": "design_grill_review_issued", "schema": EVENT_SCHEMA, "call_id": call_id, "review_id": review_id, "runner": runner, "model_binding": model, "contract_digest": digest(contract), "provider_calls": 0, "cannot_claim": sorted(REQUIRED_CANNOT_CLAIM)})
    return {"verdict": "pass", "status": "host_egress_admission_required", "provider_calls": 0, "contract": contract, "problems": []}


def validate_result(contract: Any, result: Any) -> list[str]:
    if not isinstance(contract, dict) or contract.get("schema") != CONTRACT_SCHEMA or not isinstance(result, dict):
        return ["contract or result is invalid"]
    required = {"schema", "call_id", "contract_digest", "review_id", "runner", "model_binding", "findings", "recommendation", "route", "cannot_claim"}
    problems = [] if set(result) == required else ["result fields are invalid"]
    if result.get("schema") != RESULT_SCHEMA: problems.append(f"result.schema must be {RESULT_SCHEMA}")
    for field in ("call_id", "review_id", "runner", "model_binding"):
        if result.get(field) != contract.get(field): problems.append(f"result.{field} must match contract")
    if result.get("contract_digest") != digest(contract): problems.append("result.contract_digest must match contract")
    if not isinstance(result.get("findings"), list) or len(result["findings"]) > 32 or any(not isinstance(item, str) or not item.strip() for item in result["findings"]): problems.append("findings must be bounded strings")
    if not isinstance(result.get("recommendation"), str) or not result["recommendation"].strip() or result.get("route") not in ROUTES: problems.append("recommendation or route is invalid")
    if not isinstance(result.get("cannot_claim"), list) or not REQUIRED_CANNOT_CLAIM.issubset(set(result["cannot_claim"])): problems.append("result cannot_claim is incomplete")
    return problems


def validate_host_receipt(contract: dict[str, Any], receipt: Any) -> list[str]:
    if not isinstance(receipt, dict) or set(receipt) != {"schema", "status", "provider_calls", "event", "execution"}:
        return ["host execution receipt fields are invalid"]
    problems: list[str] = []
    if receipt.get("schema") != HOST_RECEIPT_SCHEMA or receipt.get("status") != "gateway_execution_recorded" or receipt.get("provider_calls") != 1:
        problems.append("host execution receipt status is invalid")
    event = receipt.get("event")
    if not isinstance(event, dict) or event.get("status") != "gateway_dispatch_ready" or event.get("call_id") != contract.get("call_id") or event.get("capsule_digest") != contract.get("egress_envelope", {}).get("capsule", {}).get("digest"):
        problems.append("host admission does not bind the exact call and capsule")
    execution = receipt.get("execution")
    expected = {"call_id", "capsule_digest", "runner", "model_binding", "exit_code", "artifact_ref", "output_digest", "completed_at", "claim_level"}
    if not isinstance(execution, dict) or set(execution) != expected:
        return [*problems, "host execution evidence fields are invalid"]
    for field in ("call_id", "runner", "model_binding"):
        if execution.get(field) != contract.get(field):
            problems.append(f"host execution {field} does not match contract")
    if execution.get("capsule_digest") != contract.get("egress_envelope", {}).get("capsule", {}).get("digest"):
        problems.append("host execution capsule digest does not match contract")
    if execution.get("exit_code") != 0 or not isinstance(execution.get("artifact_ref"), str) or not execution["artifact_ref"] or not _is_digest(execution.get("output_digest")) or execution.get("claim_level") != "process_bound":
        problems.append("host execution evidence is incomplete")
    try:
        pe.timestamp(execution.get("completed_at", ""))
    except (TypeError, ValueError):
        problems.append("host execution completion time is invalid")
    return problems


def record(session_dir: Path, contract: dict[str, Any], host_receipt: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    problems = validate_result(contract, result)
    problems.extend(validate_host_receipt(contract, host_receipt))
    events = _events(session_dir)
    issued = next((item for item in events if item.get("type") == "design_grill_review_issued" and item.get("call_id") == contract.get("call_id") and item.get("contract_digest") == digest(contract)), None)
    if issued is None: problems.append("immutable review issuance is required")
    if problems:
        return {"verdict": "ng", "status": "invalid_design_grill_result", "provider_calls": 0, "problems": problems}
    if any(item.get("type") == "design_grill_review_recorded" and item.get("call_id") == contract["call_id"] for item in events):
        return {"verdict": "ng", "status": "design_grill_result_replayed", "provider_calls": 0, "problems": ["call result already recorded"]}
    _append(session_dir, {"type": "design_grill_review_recorded", "schema": EVENT_SCHEMA, "call_id": contract["call_id"], "review_id": contract["review_id"], "contract_digest": digest(contract), "host_execution_digest": digest(host_receipt["execution"]), "result": result, "model_display": {"label": contract["model_binding"], "claim_level": "process_bound"}, "provider_calls": 0, "cannot_claim": sorted(REQUIRED_CANNOT_CLAIM)})
    review_ids = {item.get("review_id") for item in _events(session_dir) if item.get("type") == "design_grill_review_recorded"}
    manifest = load_json(session_dir / "manifest.json")
    required_ids = {slot["id"] for slot in manifest["review_slots"]}
    return {"verdict": "pass", "status": "human_design_decision_required" if required_ids.issubset(review_ids) else "design_grill_more_results_required", "provider_calls": 0, "recorded_review_ids": sorted(review_ids), "problems": []}


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual host-egress-bound, model-neutral Design Grill")
    parser.add_argument("--json", action="store_true")
    sub = parser.add_subparsers(dest="mode", required=True)
    prepare_args = sub.add_parser("prepare"); prepare_args.add_argument("--spec", required=True); prepare_args.add_argument("--session-dir", required=True); prepare_args.add_argument("--context-root")
    request_args = sub.add_parser("request"); request_args.add_argument("--session-dir", required=True); request_args.add_argument("--review-id", required=True); request_args.add_argument("--runner", required=True); request_args.add_argument("--model", required=True); request_args.add_argument("--provider-profile", required=True); request_args.add_argument("--requested-at", required=True); request_args.add_argument("--expires-at", required=True)
    verify_args = sub.add_parser("verify-result"); verify_args.add_argument("--contract", required=True); verify_args.add_argument("--result", required=True)
    record_args = sub.add_parser("record"); record_args.add_argument("--session-dir", required=True); record_args.add_argument("--contract", required=True); record_args.add_argument("--host-receipt", required=True); record_args.add_argument("--result", required=True)
    args = parser.parse_args()
    if args.mode == "prepare": result = prepare(load_json(Path(args.spec)), ROOT, Path(args.session_dir), context_root=Path(args.context_root) if args.context_root else None)
    elif args.mode == "request": result = request(Path(args.session_dir), review_id=args.review_id, runner=args.runner, model=args.model, provider_profile=args.provider_profile, requested_at=args.requested_at, expires_at=args.expires_at, policy=load_json(HERE.parent / "provider_egress" / "policy.example.json"))
    elif args.mode == "verify-result":
        problems = validate_result(load_json(Path(args.contract)), load_json(Path(args.result))); result = {"verdict": "pass" if not problems else "ng", "status": "design_grill_result_verified" if not problems else "invalid_design_grill_result", "provider_calls": 0, "problems": problems}
    else: result = record(Path(args.session_dir), load_json(Path(args.contract)), load_json(Path(args.host_receipt)), load_json(Path(args.result)))
    print(json.dumps(result, ensure_ascii=False, indent=2)); return 0 if result["verdict"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

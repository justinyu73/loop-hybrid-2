"""Mechanical checker for the compact optimization claim registry."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import capability_resolver as cr

SCHEMA = "lh-optimization-claims/v1"
PROOF_RANK = {
    "fixture": 0,
    "offline_canary": 1,
    "bounded_live": 2,
    "resident_live": 3,
    "human_acceptance": 4,
    "release": 5,
}
STATUSES = {"open", "complete", "blocked", "superseded"}
EXPECTED_CLAIMS = {
    "OPT-GOV-1",
    "OPT-GOV-2",
    "OPT-ROUTE-1",
    "OPT-ROUTE-2",
    "OPT-DOC-1",
    "OPT-ADAPT-1",
}


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _ref_path(root: Path, ref: str) -> Path:
    path_text = ref.split("#", 1)[0]
    candidate = (root / path_text).resolve()
    if not candidate.is_relative_to(root.resolve()):
        raise ValueError(f"evidence ref escapes repo: {ref}")
    return candidate


def _bounded_live_ok(path: Path) -> tuple[bool, str]:
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"live evidence unreadable: {exc}"
    producer = evidence.get("producer")
    evaluator = evidence.get("evaluator")
    ok = (
        evidence.get("schema") == "lh-capability-routing-live-evidence/v1"
        and evidence.get("status") == "pass"
        and evidence.get("provider_invocations") == 2
        and evidence.get("source_checkout_unchanged") is True
        and evidence.get("disposable_workspace") is True
        and evidence.get("promotion_performed") is False
        and isinstance(producer, dict)
        and isinstance(evaluator, dict)
        and producer.get("lamp_verdict") == "GREEN"
        and producer.get("value_verdict") == "GREEN"
        and evaluator.get("verdict") == "accept"
        and evaluator.get("exit_status") == "completed"
        and producer.get("binding_id") != evaluator.get("binding_id")
        and producer.get("model_family") != evaluator.get("model_family")
    )
    return ok, "bounded live producer/evaluator proof" if ok else "bounded live fields do not close"


def check(root: Path, registry: dict[str, Any] | None = None) -> dict[str, Any]:
    failures: list[dict[str, str]] = []
    registry_path = root / "docs" / "active" / "optimization-claims.json"
    if registry is None:
        try:
            registry = json.loads(registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            registry = {}
            failures.append({"id": "registry-readable", "detail": str(exc)})
    if not isinstance(registry, dict) or registry.get("schema") != SCHEMA:
        failures.append({"id": "registry-schema", "detail": f"want {SCHEMA}"})
        claims: list[Any] = []
    else:
        claims = registry.get("claims") if isinstance(registry.get("claims"), list) else []
        if not claims:
            failures.append({"id": "registry-claims", "detail": "claims must be a non-empty array"})
    ids = [
        item.get("id")
        for item in claims
        if isinstance(item, dict)
    ]
    if len(ids) != len(set(ids)):
        failures.append({"id": "claim-ids", "detail": "claim ids must be unique"})
    if set(ids) != EXPECTED_CLAIMS:
        failures.append({
            "id": "claim-set",
            "detail": f"want exactly {sorted(EXPECTED_CLAIMS)}",
        })
    for claim in claims:
        if not isinstance(claim, dict):
            failures.append({"id": "claim-shape", "detail": "claim must be an object"})
            continue
        claim_id = str(claim.get("id"))
        expected = {
            "id",
            "status",
            "summary",
            "required_proof",
            "observed_proof",
            "evidence_refs",
            "next_node",
        }
        if set(claim) != expected:
            failures.append({"id": claim_id, "detail": "claim fields are not closed"})
            continue
        status = claim.get("status")
        required = claim.get("required_proof")
        observed = claim.get("observed_proof")
        refs = claim.get("evidence_refs")
        if status not in STATUSES:
            failures.append({"id": claim_id, "detail": "invalid status"})
        if required not in PROOF_RANK or observed not in PROOF_RANK:
            failures.append({"id": claim_id, "detail": "invalid proof class"})
        elif status == "complete" and PROOF_RANK[observed] < PROOF_RANK[required]:
            failures.append({"id": claim_id, "detail": "complete claim lacks required proof class"})
        if not isinstance(refs, list) or not refs:
            failures.append({"id": claim_id, "detail": "evidence refs must be non-empty"})
            continue
        for ref in refs:
            if not isinstance(ref, str) or not ref.strip():
                failures.append({"id": claim_id, "detail": "evidence ref must be non-empty"})
                continue
            try:
                evidence_path = _ref_path(root, ref)
            except ValueError as exc:
                failures.append({"id": claim_id, "detail": str(exc)})
                continue
            if not evidence_path.is_file():
                failures.append({"id": claim_id, "detail": f"missing evidence: {ref}"})
        if claim_id == "OPT-ROUTE-2" and refs:
            live_ok, detail = _bounded_live_ok(_ref_path(root, refs[0]))
            if not live_ok:
                failures.append({"id": claim_id, "detail": detail})

    governance = (root / "GOVERNANCE.md").read_text(encoding="utf-8")
    for required_text in (
        "Goal-scoped delegation",
        "Per-node confirmation is not required",
        "merge only when the Project Runtime Contract grants that action",
    ):
        if required_text not in governance:
            failures.append({
                "id": "goal-scoped-governance",
                "detail": f"missing governance phrase: {required_text}",
            })

    contract = json.loads(
        (root / "project_runtime_contract.json").read_text(encoding="utf-8")
    )
    if "work_graph" not in contract or "models" in contract:
        failures.append({
            "id": "authority-split",
            "detail": "active contract must use work_graph without models",
        })
    else:
        spec = root / "docs" / "contracts" / "model-routing-v1.md"
        expected_digest = _sha256(spec)
        node_digests = {
            node.get("inputs", {}).get("authority_digest")
            for node in contract["work_graph"].get("nodes", [])
            if isinstance(node, dict)
        }
        if node_digests != {expected_digest}:
            failures.append({
                "id": "authority-digest",
                "detail": "work graph authority digest is stale",
            })
        profile_id = contract["work_graph"].get("routing_profile")
        profile = root / "deploy" / "model-routing" / "profiles" / f"{profile_id}.json"
        if not profile.is_file():
            failures.append({
                "id": "routing-profile",
                "detail": f"operator profile missing: {profile}",
            })
        else:
            try:
                profile_data = json.loads(profile.read_text(encoding="utf-8"))
                cr.validate_routing_authority(profile_data)
                evidence_pairs = [(
                    profile_data["policy"]["evidence_ref"],
                    profile_data["policy"]["evidence_digest"],
                )]
                for resource in profile_data["registry"]["resources"]:
                    for evidence_key in ("health_evidence", "score_evidence"):
                        evidence = resource[evidence_key]
                        evidence_pairs.append((
                            evidence["source_ref"],
                            evidence["digest"],
                        ))
                for ref, digest in evidence_pairs:
                    if _sha256(_ref_path(root, ref)) != digest:
                        raise ValueError(f"profile evidence digest is stale: {ref}")
            except (KeyError, OSError, json.JSONDecodeError, ValueError) as exc:
                failures.append({
                    "id": "routing-profile",
                    "detail": str(exc),
                })

    active_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((root / "docs" / "active").glob("*.md"))
    )
    for stale in (
        "merge is always human-held",
        "promotion remain project/human-owned",
        "狀態：規劃中",
        "<pending>",
    ):
        if stale in active_text:
            failures.append({
                "id": "active-doc-drift",
                "detail": f"stale active wording remains: {stale}",
            })

    return {
        "check_id": "lh-optimization-claims",
        "status": "pass" if not failures else "fail",
        "total_claims": len(claims),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/claim_checker.py",
            "provider_invocations": 0,
        },
    }


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    result = check(root)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

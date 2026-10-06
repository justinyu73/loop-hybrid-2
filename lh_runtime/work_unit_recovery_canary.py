#!/usr/bin/env python3
"""Work-unit recovery: the store API's whole lifecycle on real Runs.

``record_recovery_request`` -> ``claim_recovery_phase`` -> ``finish_recovery_phase``
-> ``apply_recovery_decision``, on Runs the work-unit fixture drives through the
real completion flow: two that integrate, and one whose checks fail three times
so the engine itself stops it as ``audit_required``.  The phase-binding metadata
(capability, fence, provider input) is fixture data in the work-unit profile;
everything that decides is the store's own code.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))  # the work-unit fixture imports lh_runtime as a package
sys.path.insert(2, str(HERE.parent / "tests"))

import candidate_review_work_unit_canary as fixture  # noqa: E402
from lh_runtime.work_unit_store import FenceError, WorkUnitStore, WorkUnitStoreError, digest_json  # noqa: E402

CHECK_ID = "lh-work-unit-recovery"
ONE_CALL = {"planner_calls": 1, "plan_verifier_calls": 1, "planner_timeout_seconds": 60.0,
            "plan_verifier_timeout_seconds": 60.0, "incident_timeout_seconds": 600.0}
MANY_CALLS = {**ONE_CALL, "planner_calls": 20, "plan_verifier_calls": 20}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def d(char: str) -> str:
    return "sha256:" + char * 64


def meta(request: dict[str, Any], role: str, capability: str, principal: str) -> dict[str, Any]:
    identity = {"principal": principal, "adapter_version": "v1"}
    launch = d("1")
    return {
        "capability_binding": {"schema": "host-capability-binding/v1", "role": role, "capability": capability,
                               "permissions": "read_only", "work_unit_id": request["work_unit_id"],
                               "attempt_id": str(request["attempt"]), "identity": identity,
                               "identity_digest": digest_json(identity), "adapter_id": "fixture-adapter",
                               "contract_digest": d("2")},
        "execution_fence": {"schema": "lh-execution-fence-launch/v1", "status": "admitted",
                            "launch_descriptor_digest": launch, "binding_digest": d("3"), "backend": {"id": "fixture"},
                            "proofs": {}, "proofs_digest": digest_json({}), "provider_control_channel": {},
                            "launch_classes": {}},
        "provider_input_binding": {
            "schema": "lh-provider-input-binding/v1", "run_id": request["run_id"], "attempt": request["attempt"],
            "goal_revision": digest_json({"goal_id": request["goal_id"], "goal_revision": request["goal_revision"]}),
            "adapter_id": "fixture-adapter", "adapter_version": "v1", "capability_digest": d("2"),
            "launch_descriptor_digest": launch, "projection_digest": d("4"), "authority_digest": d("5"),
            "provider_input_digest": digest_json({"segments": [], "launch_descriptor_digest": launch}),
            "segments": [], "nonce": "fixture-nonce"},
        "execution_context_digest": d("6"),
    }


def make_request(run: dict[str, Any], *, phase: str = "checks", reason: str = "check_failed",
                 evidence: str = d("7"), candidate: str = d("b"), **extra: Any) -> dict[str, Any]:
    request = {"goal_id": fixture.GOAL_ID, "goal_revision": fixture.GOAL_REVISION, "node_id": fixture.NODE_ID,
               "work_unit_id": run["work_unit_id"], "run_id": run["run_id"], "attempt": int(run["attempts"]),
               "fence": int(run["fence"]), "phase": phase, "candidate_digest": candidate, "packet_digest": d("9"),
               "envelope_digest": d("a"), "authority_digest": d("c"), "reason_code": reason,
               "input_evidence_digest": evidence, "failure_fingerprint": "fixture-failure",
               "sanitized_evidence_refs": [], "remaining_budget": {}, "write_set": ["src/"], **extra}
    request["capability_binding"] = meta(request, "coding", "coding", "coder")["capability_binding"]
    request["request_id"] = digest_json(WorkUnitStore._recovery_key_identity(request))
    return request


def plan(record: dict[str, Any], action: dict[str, Any], **extra: Any) -> dict[str, Any]:
    request = record["request"]
    body = {"request_id": record["request_id"], "request_digest": record["request_digest"], "action": action,
            "producer_identity": {"principal": "planner"},
            "preconditions": {key: request[key] for key in ("candidate_digest", "attempt", "fence", "packet_digest",
                                                             "envelope_digest", "authority_digest", "capability_binding")},
            "authority_comparison": {"authority_digest": request["authority_digest"], "write_set": request["write_set"],
                                     "budget": request["remaining_budget"]}, **extra}
    body["plan_digest"] = digest_json(body)
    return {**body, **meta(request, "planner", "planning", "planner")}


def verdict(record: dict[str, Any], result: dict[str, Any], *, principal: str = "reviewer") -> dict[str, Any]:
    request = record["request"]
    return {"request_id": record["request_id"], "request_digest": record["request_digest"], "principal": principal,
            "verdict": "GREEN", "reasons": ["bound to this request"], "read_only": True, "source_write": False,
            "plan_digest": result["plan_digest"], "candidate_digest": request["candidate_digest"],
            "authority_digest": request["authority_digest"], **meta(request, "verifier", "verifier", principal)}


def reviewed(store: WorkUnitStore, request: dict[str, Any], budget: dict[str, Any], action: dict[str, Any], *,
             verifier_principal: str = "reviewer", stop_after_planner: bool = False, **plan_extra: Any) -> dict[str, Any]:
    """Drive one request through planner and verifier; returns the stored record."""
    request = {**request, "remaining_budget": dict(budget)}
    request["request_id"] = digest_json(WorkUnitStore._recovery_key_identity(request))
    record = store.record_recovery_request(request, budget)
    if not store.claim_recovery_phase(record["request_id"], phase="planner")["claimed"]:
        return store.get_recovery_request(record["request_id"])
    result = plan(store.get_recovery_request(record["request_id"]), action, **plan_extra)
    record = store.finish_recovery_phase(record["request_id"], "planner", result)
    if stop_after_planner:
        return record
    store.claim_recovery_phase(record["request_id"], phase="plan_verifier")
    return store.finish_recovery_phase(record["request_id"], "plan_verifier",
                                       verdict(record, result, principal=verifier_principal))


def refusal(action: Callable[[], Any]) -> str:
    try:
        action()
        return "accepted"
    except (WorkUnitStoreError, FenceError, KeyError) as exc:
        return str(exc)


def integrated_run(root: Path) -> tuple[WorkUnitStore, dict[str, Any]]:
    harness = fixture.Harness(root, executor_mode="right", policy=False)
    harness.drive()
    unit = harness.store.list_work_units(fixture.GOAL_ID)[0]
    return harness.store, harness.store.get_run(unit["run_id"])


def c1_identity(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    wrong_node = make_request(run, node_id="another-node", remaining_budget=dict(ONE_CALL))
    wrong_node["request_id"] = digest_json(WorkUnitStore._recovery_key_identity(wrong_node))
    forged = {**make_request(run, evidence=d("e"), remaining_budget=dict(ONE_CALL)), "request_id": d("f")}
    results = {"wrong_node": refusal(lambda: store.record_recovery_request(wrong_node, ONE_CALL)),
               "forged_request_id": refusal(lambda: store.record_recovery_request(forged, ONE_CALL))}
    ok = "Run identity mismatch" in results["wrong_node"] and "key mismatch" in results["forged_request_id"]
    return case("a-request-must-name-the-real-run-and-its-own-key", ok, results)


def c2_budget(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    first = reviewed(store, make_request(run, evidence=d("1")), ONE_CALL, {"kind": "request_authority"})
    second = make_request(run, evidence=d("2"), remaining_budget=dict(ONE_CALL))
    second["request_id"] = digest_json(WorkUnitStore._recovery_key_identity(second))
    store.record_recovery_request(second, ONE_CALL)
    claim = store.claim_recovery_phase(second["request_id"], phase="planner")
    ok = (first["status"] == "plan_verified" and claim["claimed"] is False
          and claim.get("reason") == "planner_call_budget_exhausted")
    return case("planner-calls-are-budgeted-across-the-run", ok,
                {"first": first["status"], "second_claim": claim.get("reason")})


def c3_rules_at_apply(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    record = reviewed(store, make_request(run, evidence=d("3")), MANY_CALLS, {"kind": "request_authority"},
                      verifier_principal="planner")
    outcome = refusal(lambda: store.apply_recovery_decision(
        record["request_id"], expected_request_digest=record["request_digest"], action={"kind": "request_authority"}))
    ok = record["status"] == "plan_verified" and "binding invalid" in outcome
    return case("apply-revalidates-the-plan-and-refuses-a-self-review", ok, {"status": record["status"], "apply": outcome})


def c4_actions(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    rows: dict[str, Any] = {}

    def apply(record: dict[str, Any], action: dict[str, Any], **kwargs: Any) -> Any:
        try:
            out = store.apply_recovery_decision(record["request_id"], expected_request_digest=record["request_digest"],
                                                action=action, **kwargs)
            return [out["status"], (out.get("apply") or {}).get("state")]
        except (WorkUnitStoreError, FenceError) as exc:
            return f"refused: {exc}"

    cases = {
        "request_authority": ({"kind": "request_authority"}, {}, {}, "checks", "check_failed"),
        "insufficient_evidence": ({"kind": "insufficient_evidence"}, {}, {}, "checks", "check_failed"),
        "resume_phase": ({"kind": "resume_phase"}, {"target_phase": "closeout"}, {}, "checks", "check_failed"),
        "dispatch_successor": ({"kind": "dispatch_successor", "target_node_id": "next-node"}, {}, {},
                               "machine_complete", "machine_complete"),
        "retry_within_budget": ({"kind": "retry_within_budget"}, {}, {"evidence": {"note": "bounded retry"},
                                                                      "max_attempts": 3}, "checks", "check_failed"),
        "collect_evidence": ({"kind": "collect_evidence"}, {}, {}, "checks", "check_failed"),
    }
    for index, (name, (action, extra, kwargs, phase, reason)) in enumerate(cases.items()):
        record = reviewed(store, make_request(run, phase=phase, reason=reason, evidence=d("abcdef"[index])),
                          MANY_CALLS, action, **extra)
        rows[name] = apply(record, action, **kwargs)
    expected = {
        "request_authority": ["awaiting_authority", "awaiting_authority"],
        "insufficient_evidence": ["awaiting_evidence", "awaiting_evidence"],
        "resume_phase": ["applied", "applied"],
        "dispatch_successor": ["applied", "applied"],
        "retry_within_budget": ["plan_verified", "retry_pending"],
    }
    ok = (all(rows.get(name) == value for name, value in expected.items())
          and str(rows.get("collect_evidence", "")).startswith("refused") and "collect_evidence" in rows["collect_evidence"])
    return case("each-action-lands-in-its-own-state", ok, rows)


def c5_order(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    early = reviewed(store, make_request(run, evidence=d("8")), MANY_CALLS, {"kind": "request_authority"},
                     stop_after_planner=True)
    before = refusal(lambda: store.apply_recovery_decision(
        early["request_id"], expected_request_digest=early["request_digest"], action={"kind": "request_authority"}))
    store.claim_recovery_phase(early["request_id"], phase="plan_verifier")
    store.finish_recovery_phase(early["request_id"], "plan_verifier", verdict(early, early["result"]))
    differs = refusal(lambda: store.apply_recovery_decision(
        early["request_id"], expected_request_digest=early["request_digest"], action={"kind": "insufficient_evidence"}))
    ok = "plan or verdict missing" in before and "differs from verified plan" in differs
    return case("nothing-applies-before-review-or-other-than-reviewed", ok, {"before_review": before, "other_action": differs})


def _settled_checks(store: WorkUnitStore, run: dict[str, Any]) -> dict[str, Any]:
    db = next(store.root.glob("*.sqlite3"))
    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute("SELECT evidence_json FROM completion_phases WHERE run_id=? AND attempt=? AND phase='checks'",
                           (run["run_id"], int(run["attempts"]))).fetchone()
    finally:
        conn.close()
    return json.loads(row[0])


def c6_incident(root: Path) -> dict[str, Any]:
    harness = fixture.Harness(root, executor_mode="wrong-positive", policy=False, max_attempts=3)
    steps = harness.drive(limit=8)
    store = harness.store
    run = store.get_run(store.list_work_units(fixture.GOAL_ID)[0]["run_id"])
    current = _settled_checks(store, run)
    tests = [{"id": check["id"], "command_digest": digest_json(check.get("argv"))} for check in current["checks"]]
    request = make_request(run, evidence=digest_json(current), candidate=current["candidate_digest"], test_refs=tests,
                           remaining_budget=dict(MANY_CALLS))
    request["incident"] = store.recovery_incident(request)
    request["request_id"] = digest_json(WorkUnitStore._recovery_key_identity(request))
    record = store.record_recovery_request(request, MANY_CALLS)
    claim = store.claim_recovery_phase(record["request_id"], phase="planner")
    applied = refusal(lambda: store.apply_recovery_decision(
        record["request_id"], expected_request_digest=record["request_digest"], action={"kind": "request_authority"}))
    final = (steps[-1].get("completion") or {}).get("status") if steps else None
    ok = (final == "audit_required" and request["incident"]["failure_count"] == 3
          and claim["claimed"] is False and applied != "accepted")
    return case("three-failures-require-a-read-only-audit-first", ok,
                {"engine_stop": final, "failure_count": request["incident"]["failure_count"],
                 "planner_claim": claim.get("reason"), "apply": applied})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-work-unit-recovery-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        try:
            budget_store, budget_run = integrated_run(root / "one-call")
            store, run = integrated_run(root / "many-calls")
        except Exception as exc:  # a crash is a failed exam, never a skip
            print(json.dumps({"check_id": CHECK_ID, "status": "fail", "error": f"{type(exc).__name__}: {exc}"}))
            return 1
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("a-request-must-name-the-real-run-and-its-own-key", lambda: c1_identity(budget_store, budget_run)),
            ("planner-calls-are-budgeted-across-the-run", lambda: c2_budget(budget_store, budget_run)),
            ("apply-revalidates-the-plan-and-refuses-a-self-review", lambda: c3_rules_at_apply(store, run)),
            ("each-action-lands-in-its-own-state", lambda: c4_actions(store, run)),
            ("nothing-applies-before-review-or-other-than-reviewed", lambda: c5_order(store, run)),
            ("three-failures-require-a-read-only-audit-first", lambda: c6_incident(root / "incident")),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Failure router: a fixed, recomputable answer to "who does what next" after a failure.

The router never calls a model.  Every reason code maps through one closed
table to a route, an owner, a next action, and what it invalidates; failure
evidence and prior receipts are always preserved.  Only a closed list of owner
actions may need a human.  Any machine reason never does: an unknown code goes
to the router table itself, and a repeated failure goes to a read-only audit.
The router is a projection here: open questions carry its route, and a machine
reason parked for a human is flagged, but no state changes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from goal_store import GoalStore  # noqa: E402
from run_store import RunStore  # noqa: E402
import open_questions  # noqa: E402

CHECK_ID = "lh-failure-router"
OWNER_ACTIONS = {
    "material_scope_expansion", "destructive_effect", "paid_provider_invocation", "runtime_activation",
    "external_account_action", "owner_merge", "product_acceptance", "secret_handling", "publication", "promotion",
}
# reason code -> (route, owner, human_required); one representative per family the engine emits.
EXPECTED = {
    "check_failed": ("retry_same_unit", "executor", False),
    "delivery_verifier_red": ("retry_same_unit", "executor", False),
    "candidate_review_not_green": ("retry_same_unit", "executor", False),
    "candidate_review_related_checks_red": ("retry_same_unit", "executor", False),
    "candidate_review_ref_digest_mismatch": ("repair_review_binding", "verifier", False),
    "delivery_independent_verifier_snapshot_mismatch": ("repair_verifier_binding", "verifier", False),
    "delivery_source_evidence_missing": ("reread_evidence", "engine", False),
    "attempt_receipt_missing_or_digest_mismatch": ("reread_evidence", "engine", False),
    "unrecorded": ("reread_evidence", "engine", False),
    "campaign_consecutive_failures": ("planner_bounded_repair", "planner", False),
    "campaign_recovery_role_outcome_unknown": ("reconcile_recovery_record", "engine", False),
    "source_refs_mutated_by_executor": ("human_required", "owner", True),
    "changed_path_outside_contract_scope": ("human_required", "owner", True),
    "authority_surface_touched": ("human_required", "owner", True),
    "independent_verifier_in_write_scope": ("human_required", "owner", True),
    "execution_fence_unavailable": ("human_required", "owner", True),
    "campaign_recovery_requires_authority": ("human_required", "owner", True),
    "publication": ("human_required", "owner", True),
}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def router():
    import failure_router
    return failure_router


def c1_table() -> dict[str, Any]:
    wrong = {}
    for code, (route, owner, human) in EXPECTED.items():
        got = router().route(code)
        if (got.get("schema") != "lh-failure-route/v1" or got.get("route") != route or got.get("owner") != owner
                or got.get("human_required") is not human or got.get("input_reason_code") != code
                or "failure_evidence" not in got.get("preserves", []) or "prior_receipts" not in got.get("preserves", [])
                or not got.get("next_action") or not got.get("invalidates")):
            wrong[code] = {key: got.get(key) for key in ("route", "owner", "human_required", "preserves")}
    return case("every-code-routes-as-the-table-says", not wrong, wrong or len(EXPECTED))


def c2_unknown() -> dict[str, Any]:
    got = router().route("a_code_nobody_has_seen")
    ok = (got.get("reason_code") == "unknown_reason_code" and got.get("route") == "repair_router_table"
          and got.get("owner") == "router" and got.get("human_required") is False
          and got.get("input_reason_code") == "a_code_nobody_has_seen")
    return case("an-unknown-code-goes-to-the-router-table-not-a-human", ok, got)


def c3_owner_actions() -> dict[str, Any]:
    module = router()
    declared = set(getattr(module, "OWNER_ACTIONS", ()))
    offenders = {}
    for code in [*EXPECTED, *sorted(OWNER_ACTIONS), *module.known_codes()]:
        got = module.route(code)
        human = got.get("human_required") is True
        if human != (got.get("owner_action") in OWNER_ACTIONS):
            offenders[code] = {"human_required": got.get("human_required"), "owner_action": got.get("owner_action")}
    ok = declared == OWNER_ACTIONS and not offenders and all(module.route(name)["human_required"] for name in OWNER_ACTIONS)
    return case("only-owner-actions-need-a-human", ok, {"declared": sorted(declared), "offenders": offenders})


def c4_threshold() -> dict[str, Any]:
    module = router()
    two, three = module.route("check_failed", failure_count=2), module.route("check_failed", failure_count=3)
    owner = module.route("source_refs_mutated_by_executor", failure_count=5)
    ok = (two.get("route") == "retry_same_unit" and three.get("route") == "independent_read_only_audit"
          and three.get("owner") == "auditor" and three.get("human_required") is False
          and three.get("reason_code") == "repeated_failure_threshold" and three.get("input_reason_code") == "check_failed"
          and owner.get("route") == "human_required")
    return case("a-repeated-failure-goes-to-a-read-only-audit", ok,
                {"two": two.get("route"), "three": three.get("route"), "owner_code_at_five": owner.get("route")})


def c5_recomputable() -> dict[str, Any]:
    module = router()
    first, second = module.route("delivery_source_evidence_missing"), module.route("delivery_source_evidence_missing")
    good = module.verify_route(first)
    rerouted = copy.deepcopy(first)
    rerouted["route"] = "human_required"
    regest = copy.deepcopy(first)
    regest["route_digest"] = "sha256:" + "0" * 64
    body = {key: value for key, value in first.items() if key != "route_digest"}
    ok = (first == second and good.get("ok") is True
          and module.verify_route(rerouted).get("ok") is False and module.verify_route(regest).get("ok") is False
          and first.get("route_digest") == "sha256:" + hashlib.sha256(
              json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest())
    return case("the-same-input-gives-the-same-recomputable-receipt", ok,
                {"verify": good, "tampered_route": module.verify_route(rerouted).get("ok"),
                 "tampered_digest": module.verify_route(regest).get("ok")})


def c6_coverage() -> dict[str, Any]:
    gaps = {prefix: router().route(prefix + "x" if prefix.endswith("_") else prefix).get("route")
            for prefix, _kind in open_questions.KIND_BY_PREFIX}
    unknown = {prefix: route for prefix, route in gaps.items() if route == "repair_router_table"}
    return case("every-open-question-family-has-a-known-route", not unknown, gaps)


def _tree(root: Path) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def c7_projection(root: Path) -> dict[str, Any]:
    runs, goals = RunStore(root / "runs"), GoalStore(root / "goals")
    for key, reason in (("parked-machine", "delivery_source_evidence_missing"),
                        ("parked-owner", "source_refs_mutated_by_executor")):
        event = goals.record_event(event_id=key, idempotency_key=key, source="fixture", event_type="fixture",
                                   payload={"fixture": key})
        goals.transition_event(event["event_key"], "human_required", result={"reason": reason})
    before = (_tree(runs.root), _tree(goals.root))
    projected = open_questions.build_open_questions(runs, goals, now=10**10)
    after = (_tree(runs.root), _tree(goals.root))
    items = {item["subject"]: item for item in projected["items"]}
    machine, owner = items.get("parked-machine", {}), items.get("parked-owner", {})
    ok = (before == after and (machine.get("route") or {}).get("route") == "reread_evidence"
          and machine.get("machine_route_available") is True
          and (owner.get("route") or {}).get("route") == "human_required"
          and owner.get("machine_route_available") is False)
    return case("a-machine-reason-parked-for-a-human-is-flagged-without-changing-state", ok,
                {"machine": machine, "owner": owner, "store_unchanged": before == after})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-failure-router-") as raw:
        root = Path(raw).resolve()
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("every-code-routes-as-the-table-says", c1_table),
            ("an-unknown-code-goes-to-the-router-table-not-a-human", c2_unknown),
            ("only-owner-actions-need-a-human", c3_owner_actions),
            ("a-repeated-failure-goes-to-a-read-only-audit", c4_threshold),
            ("the-same-input-gives-the-same-recomputable-receipt", c5_recomputable),
            ("every-open-question-family-has-a-known-route", c6_coverage),
            ("a-machine-reason-parked-for-a-human-is-flagged-without-changing-state", lambda: c7_projection(root / "c7")),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

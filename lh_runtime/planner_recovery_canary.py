#!/usr/bin/env python3
"""Planner recovery: the review rules and the campaign state machine, as the contract states them.

Part A checks the plan/verdict rules both recovery paths share
(``validate_recovery_plan``): one well-bound record passes, and each single
violation is refused.

Part B drives the campaign path through ``GoalLoopWorker`` with a real
GoalStore.  Two seams are test stand-ins, named here so nobody mistakes them
for coverage: the native binding (which in production is resolved from a
sealed contract, a provider registry and a scheduler dispatch) and the reader
of failed children's receipts (``_campaign_child``).  Everything between them,
including claims, budgets, deadlines, role calls, plan validation and the final
state, is the engine's own code.
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))  # the recovery path imports lh_runtime as a package

from controller import LoopController  # noqa: E402
from goal_loop_worker import GoalLoopWorker  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from lifecycle import NativeProcessIdentityPort  # noqa: E402
from run_store import RunStore  # noqa: E402
from work_unit_store import WorkUnitStoreError, digest_json, validate_recovery_plan  # noqa: E402

CHECK_ID = "lh-planner-recovery"
BUDGET = {"planner_calls": 1, "plan_verifier_calls": 1, "planner_timeout_seconds": 60.0,
          "plan_verifier_timeout_seconds": 60.0, "incident_timeout_seconds": 600.0}
METADATA = ("plan_digest", "probe", "execution_fence", "provider_input_binding", "capability_binding",
            "execution_context_digest")
PRINCIPALS = {"coding": "coder", "planning": "planner", "verifier": "reviewer"}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def d(char: str) -> str:
    return "sha256:" + char * 64


def phase_metadata(request: dict[str, Any], *, role: str, capability: str, principal: str) -> dict[str, Any]:
    """Command-boundary metadata that satisfies the native phase-binding rules."""
    identity = {"principal": principal, "adapter_version": "v1"}
    launch = d("1")
    return {
        "capability_binding": {
            "schema": "host-capability-binding/v1", "role": role, "capability": capability,
            "permissions": "read_only", "identity_profile": "native-run-v1", "authority_store": "run",
            "unit_id": request["unit_id"], "attempt_id": str(request["attempt"]), "identity": identity,
            "identity_digest": digest_json(identity), "adapter_id": "fixture-adapter", "contract_digest": d("2"),
        },
        "execution_fence": {
            "schema": "lh-execution-fence-launch/v1", "status": "admitted", "launch_descriptor_digest": launch,
            "binding_digest": d("3"), "backend": {"id": "fixture"}, "proofs": {}, "proofs_digest": digest_json({}),
            "provider_control_channel": {}, "launch_classes": {},
        },
        "provider_input_binding": {
            "schema": "lh-provider-input-binding/v1", "run_id": request["run_id"], "attempt": request["attempt"],
            "goal_revision": digest_json({"goal_id": request["goal_id"], "goal_revision": request["goal_revision"]}),
            "adapter_id": "fixture-adapter", "adapter_version": "v1", "capability_digest": d("2"),
            "launch_descriptor_digest": launch, "projection_digest": d("4"), "authority_digest": d("5"),
            "provider_input_digest": digest_json({"segments": [], "launch_descriptor_digest": launch}),
            "segments": [], "nonce": "fixture-nonce",
        },
        "execution_context_digest": d("6"),
    }


def make_request(*, stop_event_key: str = "stop-1", stop_payload_digest: str = d("7"),
                 runtime: dict[str, Any] | None = None, remaining: int = 1) -> dict[str, Any]:
    child = {"goal_id": "campaign:stage-1", "run_id": "run-1", "attempt": 1, "fence": 1,
             "receipt_ref": "artifacts/run-1/1/receipt.json", "receipt_digest": d("8"), "remaining_attempts": remaining}
    evidence = [{key: child[key] for key in ("goal_id", "run_id", "attempt", "fence", "receipt_ref", "receipt_digest")}]
    request = {
        "request_id": "campaign-recovery:" + stop_event_key, "reason_code": "campaign_consecutive_failures",
        "phase": "checks", "goal_id": "campaign:stage-1", "goal_revision": 1, "node_id": "stage-1",
        "unit_id": "unit-1", "run_id": "run-1", "attempt": 1, "fence": 1, "base_sha": "a" * 40,
        "authority_store": "run", "identity_profile": "native-run-v1", "packet_digest": d("9"),
        "envelope_digest": d("a"), "candidate_digest": d("b"), "authority_digest": d("c"),
        "stop_event_key": stop_event_key, "stop_payload_digest": stop_payload_digest,
        "contract_digest": (runtime or {}).get("contract_digest", d("e")),
        "dispatch": (runtime or {}).get("dispatch", {"dispatch_id": "dispatch-1"}),
        "child_receipts": [child], "sanitized_evidence_refs": evidence, "completed_effects": evidence,
        "write_set": ["src/"], "remaining_budget": {"child_attempts": {"run-1": remaining}, **BUDGET},
    }
    request["capability_binding"] = phase_metadata(request, role="coding", capability="coding",
                                                   principal=PRINCIPALS["coding"])["capability_binding"]
    return request


def planner_body(request: dict[str, Any], *, kind: str = "request_authority") -> dict[str, Any]:
    plan = {
        "request_id": request["request_id"], "request_digest": digest_json(request),
        "action": {"kind": kind}, "producer_identity": {"principal": PRINCIPALS["planning"]},
        "decision_basis": {"request_digest": digest_json(request), "evidence_refs": request["sanitized_evidence_refs"],
                           "reason": "campaign_consecutive_failures: the same acceptance keeps failing"},
        "preconditions": {key: request[key] for key in ("candidate_digest", "attempt", "fence", "packet_digest",
                                                         "envelope_digest", "authority_digest", "capability_binding")},
        "authority_comparison": {"authority_digest": request["authority_digest"], "write_set": request["write_set"],
                                 "budget": request["remaining_budget"]},
    }
    return {**plan, "plan_digest": digest_json(plan)}


def verdict_body(request: dict[str, Any], plan_digest: str) -> dict[str, Any]:
    return {"request_id": request["request_id"], "request_digest": digest_json(request),
            "principal": PRINCIPALS["verifier"], "verdict": "GREEN", "reasons": ["the plan is bound and bounded"],
            "read_only": True, "source_write": False, "plan_digest": plan_digest,
            "candidate_digest": request["candidate_digest"], "authority_digest": request["authority_digest"]}


def full_record(request: dict[str, Any]) -> dict[str, Any]:
    result = {**planner_body(request), **phase_metadata(request, role="planner", capability="planning",
                                                        principal=PRINCIPALS["planning"])}
    verdict = {**verdict_body(request, result["plan_digest"]),
               **phase_metadata(request, role="verifier", capability="verifier", principal=PRINCIPALS["verifier"])}
    return {"request_id": request["request_id"], "request_digest": digest_json(request), "request": request,
            "result": result, "verdict": verdict}


def _refused(record: dict[str, Any]) -> bool:
    try:
        validate_recovery_plan(record, identity_profile="native-run-v1")
        return False
    except WorkUnitStoreError:
        return True


def _replan(record: dict[str, Any]) -> None:
    """Recompute the plan digest after a planner-side edit, as a planner that means it would."""
    canonical = {key: value for key, value in record["result"].items() if key not in METADATA}
    record["result"]["plan_digest"] = record["verdict"]["plan_digest"] = digest_json(canonical)


def part_a() -> dict[str, Any]:
    base = full_record(make_request())
    try:
        validate_recovery_plan(copy.deepcopy(base), identity_profile="native-run-v1")
        accepted = True
    except WorkUnitStoreError as exc:
        accepted = f"refused: {exc}"
    violations: dict[str, Callable[[dict[str, Any]], None]] = {
        "same-principal": lambda r: (r["verdict"].update(principal=PRINCIPALS["planning"]),
                                     r["verdict"].update(phase_metadata(r["request"], role="verifier",
                                                         capability="verifier", principal=PRINCIPALS["planning"]))),
        "verifier-not-read-only": lambda r: r["verdict"].update(read_only=False),
        "verifier-wrote-source": lambda r: r["verdict"].update(source_write=True),
        "verdict-not-green": lambda r: r["verdict"].update(verdict="RED"),
        "verdict-without-reasons": lambda r: r["verdict"].update(reasons=[]),
        "action-outside-the-closed-set": lambda r: (r["result"].update(action={"kind": "merge_now"}), _replan(r)),
        "plan-edited-after-its-digest": lambda r: r["result"]["action"].update(kind="collect_evidence"),
        "request-digest-mismatch": lambda r: r.update(request_digest=d("f")),
        "candidate-digest-mismatch": lambda r: r["verdict"].update(candidate_digest=d("f")),
        "authority-digest-mismatch": lambda r: r["verdict"].update(authority_digest=d("f")),
        "basis-omits-the-reason-code": lambda r: (r["result"]["decision_basis"].update(reason="looks fine"), _replan(r)),
    }
    not_refused = []
    for name, mutate in violations.items():
        record = copy.deepcopy(base)
        mutate(record)
        if not _refused(record):
            not_refused.append(name)
    return case("plan-and-verdict-rules-refuse-each-violation", accepted is True and not not_refused,
                {"well_bound_record": accepted, "accepted_violations": not_refused, "checked": len(violations)})


class Binding:
    """Stand-in for the native binding: runtime facts, three principals, and a command boundary."""

    def __init__(self, root: Path, *, principals: dict[str, str] | None = None, planner_exit: int = 0,
                 fail_before_start: bool = False):
        contract = root / "contract.json"
        contract.write_text('{"fixture": "sealed contract"}', encoding="utf-8")
        import hashlib
        self.native_runtime = {
            "identity_profile": "native-run-v1", "project_id": "project", "campaign_id": "campaign",
            "source_repo": str(root), "base_revision": "a" * 40, "contract_ref": str(contract),
            "contract_digest": "sha256:" + hashlib.sha256(contract.read_bytes()).hexdigest(),
            "dispatch": {"dispatch_id": "dispatch-1"},
            "planner_recovery": {"planner_argv": ["planner"], "plan_verifier_argv": ["reviewer"], "budget": dict(BUDGET)},
        }
        chosen = principals or PRINCIPALS
        self.runner = SimpleNamespace(contract={"capabilities": {
            name: {"identity": {"principal": chosen[name]}} for name in ("coding", "planning", "verifier")}})
        self.planner_exit = planner_exit
        self.fail_before_start = fail_before_start
        self.calls: list[str] = []

    def command(self, request, *, phase, argv, worktree, timeout_seconds, input_request, writable, on_started):
        self.calls.append(phase)
        if self.fail_before_start:
            raise OSError("the role command could not be launched")
        on_started(SimpleNamespace(pid=os.getpid()))
        if phase == "planner":
            role, capability, principal = "planner", "planning", PRINCIPALS["planning"]
            body = planner_body(request)
            code = self.planner_exit
        else:
            role, capability, principal = "verifier", "verifier", PRINCIPALS["verifier"]
            body = verdict_body(request, input_request["planner_result"]["plan_digest"])
            code = 0
        completed = subprocess.CompletedProcess(argv, code, json.dumps(body) if code == 0 else "", "")
        return completed, phase_metadata(request, role=role, capability=capability, principal=principal), None


class Worker(GoalLoopWorker):
    """Reads failed children from the request itself instead of real failed runs (a named test seam)."""

    def _campaign_child(self, goal_id: str) -> dict[str, Any]:
        return copy.deepcopy(self._children[goal_id])


def campaign(root: Path, *, binding_kwargs: dict[str, Any] | None = None,
             edit: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
             remaining: int = 1) -> tuple[dict[str, Any], Binding]:
    root.mkdir(parents=True)
    goals, runs = GoalStore(root / "goals"), RunStore(root / "runs")
    binding = Binding(root, **(binding_kwargs or {}))
    worker = Worker(goal_store=goals, run_store=runs, controller=LoopController(runs, root / "ws"),
                    compilers={}, execution_context={}, recovery_binding=binding)
    stop_payload = {"campaign_id": "campaign", "failed_goal_ids": ["campaign:stage-1"],
                    "dispatch": binding.native_runtime["dispatch"]}
    stop = goals.record_event(event_id="stop-1", idempotency_key="stop-1", source="stop_lines",
                              event_type="human_required", payload=stop_payload)
    goals.transition_event(stop["event_key"], "human_required", result=stop_payload)  # as the engine records it
    request = make_request(stop_event_key=stop["event_key"], stop_payload_digest=stop["payload_digest"],
                           runtime=binding.native_runtime, remaining=remaining)
    record = {"schema": "lh-recovery-record/v1", "request_id": request["request_id"], "status": "requested",
              "result": None, "verdict": None, "apply": None, "claims": [], "events": [],
              "budget_limits": dict(BUDGET), "incident_started_at": None, "incident_deadline_at": None}
    if edit is not None:
        edit(request, record)
    record["request"] = request
    record["request_digest"] = digest_json(request)
    worker._children = {child["goal_id"]: child for child in request["child_receipts"]}
    goals.record_event(event_id=request["request_id"], idempotency_key=request["request_id"],
                       source="campaign_recovery", event_type="campaign_recovery_requested", payload={"record": record})
    worker._process_one_event("recovery-canary")
    stored = goals.get_event(request["request_id"])
    return {"state": stored["state"], **(stored["result"] or {})}, binding


def part_b(root: Path) -> list[dict[str, Any]]:
    rows = []
    out, binding = campaign(root / "happy")
    rows.append(case("a-reviewed-plan-waits-for-authority-and-is-never-applied",
                     out.get("status") == "awaiting_authority" and out.get("reason") == "campaign_recovery_requires_authority"
                     and (out.get("apply") or {}).get("status") == "awaiting_authority"
                     and binding.calls == ["planner", "plan_verifier"] and out.get("state") == "human_required",
                     {"status": out.get("status"), "reason": out.get("reason"), "calls": binding.calls}))
    out, binding = campaign(root / "exhausted", remaining=0)
    rows.append(case("an-exhausted-child-is-not-replenished",
                     out.get("status") == "awaiting_authority"
                     and out.get("reason") == "campaign_recovery_child_attempt_budget_exhausted",
                     {"status": out.get("status"), "reason": out.get("reason")}))
    out, binding = campaign(root / "same", binding_kwargs={"principals": {**PRINCIPALS, "verifier": PRINCIPALS["planning"]}})
    rows.append(case("principals-that-are-not-independent-are-refused-before-any-call",
                     out.get("status") == "rejected" and out.get("reason") == "campaign_recovery_verifier_not_independent"
                     and binding.calls == [], {"status": out.get("status"), "reason": out.get("reason"), "calls": binding.calls}))
    # A role that started may have had effects, so a non-zero exit is recorded as outcome_unknown;
    # a role that never started is a plain rejection.  Neither is retried.
    started, started_binding = campaign(root / "role-failed", binding_kwargs={"planner_exit": 3})
    unstarted, unstarted_binding = campaign(root / "role-unlaunched", binding_kwargs={"fail_before_start": True})
    rows.append(case("a-failing-role-is-recorded-not-retried",
                     started.get("status") == "outcome_unknown" and started.get("reason") == "campaign_recovery_role_failed"
                     and started_binding.calls == ["planner"]
                     and unstarted.get("status") == "rejected" and unstarted.get("reason") == "campaign_recovery_role_failed"
                     and unstarted_binding.calls == ["planner"],
                     {"started": [started.get("status"), started.get("reason")],
                      "never_started": [unstarted.get("status"), unstarted.get("reason")]}))

    def recorded_plan(request: dict[str, Any], record: dict[str, Any]) -> None:
        record["result"] = {**planner_body(request), **phase_metadata(request, role="planner", capability="planning",
                                                                      principal=PRINCIPALS["planning"])}
        record["status"] = "result_recorded"
        record["claims"] = [{"call_id": "planner-call", "phase": "planner", "state": "success"}]
    out, binding = campaign(root / "restart", edit=recorded_plan)
    rows.append(case("after-a-restart-a-recorded-planner-is-not-called-again",
                     out.get("status") == "awaiting_authority" and binding.calls == ["plan_verifier"],
                     {"status": out.get("status"), "calls": binding.calls}))

    child = subprocess.Popen([sys.executable, "-c", "pass"])
    dead = NativeProcessIdentityPort().observe(child.pid)
    child.wait()
    time.sleep(0.2)

    def stale_claim(_request: dict[str, Any], record: dict[str, Any]) -> None:
        record["claims"] = [{"call_id": "planner-call", "phase": "planner", "state": "claimed",
                             "owner_process_identity": dead.as_dict() if dead else None}]
        record["status"] = "claimed"
    out, binding = campaign(root / "stale", edit=stale_claim)
    rows.append(case("a-claim-whose-process-is-gone-becomes-outcome-unknown",
                     dead is not None and out.get("status") == "outcome_unknown" and binding.calls == [],
                     {"status": out.get("status"), "calls": binding.calls}))

    def expired(_request: dict[str, Any], record: dict[str, Any]) -> None:
        now = time.time()
        record["incident_started_at"], record["incident_deadline_at"] = now - 1000, now - 1
    out, binding = campaign(root / "deadline", edit=expired)
    rows.append(case("a-spent-incident-deadline-waits-for-authority",
                     out.get("status") == "awaiting_authority"
                     and out.get("reason") == "campaign_recovery_incident_deadline_exhausted" and binding.calls == [],
                     {"status": out.get("status"), "reason": out.get("reason"), "calls": binding.calls}))

    def tampered(request: dict[str, Any], _record: dict[str, Any]) -> None:
        request["stop_payload_digest"] = d("0")
    out, binding = campaign(root / "tampered", edit=tampered)
    rows.append(case("a-changed-stop-line-is-an-authority-mismatch",
                     out.get("status") == "rejected" and out.get("reason") == "campaign_recovery_authority_mismatch"
                     and binding.calls == [], {"status": out.get("status"), "reason": out.get("reason")}))
    return rows


def main() -> int:
    results: list[dict[str, Any]] = []
    try:
        results.append(part_a())
    except Exception as exc:  # a crash is a failed exam, never a skip
        results.append(case("plan-and-verdict-rules-refuse-each-violation", False, f"{type(exc).__name__}: {exc}"))
    names = ["a-reviewed-plan-waits-for-authority-and-is-never-applied", "an-exhausted-child-is-not-replenished",
             "principals-that-are-not-independent-are-refused-before-any-call", "a-failing-role-is-recorded-not-retried",
             "after-a-restart-a-recorded-planner-is-not-called-again",
             "a-claim-whose-process-is-gone-becomes-outcome-unknown", "a-spent-incident-deadline-waits-for-authority",
             "a-changed-stop-line-is-an-authority-mismatch"]
    with tempfile.TemporaryDirectory(prefix="lh-planner-recovery-") as raw:
        try:
            results.extend(part_b(Path(raw).resolve()))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:300]}"
            done = {row["id"] for row in results}
            results.extend(case(name, False, detail) for name in names if name not in done)
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

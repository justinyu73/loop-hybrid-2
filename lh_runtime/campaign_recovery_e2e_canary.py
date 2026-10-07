#!/usr/bin/env python3
"""Campaign recovery, end to end and isolated: real failures, real binding, real fence, stand-in roles.

Everything is built inside a temporary root: a git source, the sealed native
chain (contract, capability contract, provider registry, host contract,
bootstrap authority, dispatch), three seeded goals whose acceptance lamp fails,
and stand-in planner/verifier scripts that import no engine module.  The
production entry ``goal_loop_run.run`` drives the campaign in-process with the
``local-process`` fence: the children fail through real Runs, the stop line
fires, a recovery request is built from their real receipts, and the roles are
launched through the real binding's command boundary.  No scheduler entry, no
network, no user configuration; the three environment variables the chain needs
are set for the run and restored.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))
sys.path.insert(2, str(HERE.parent / "tests"))

import native_recovery_binding_canary as chain_fixture  # noqa: E402
from campaign_compiler import CampaignCompiler  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from native_delivery_fixture import make_native_bundle  # noqa: E402
from lh_runtime import goal_loop_run as glr  # noqa: E402
from lh_runtime.run_store import RunStore  # noqa: E402

CHECK_ID = "lh-campaign-recovery-e2e"
INJECTED = ("LH_SCHEDULER_OWNER_ID", "LH_TRUSTED_BOOTSTRAP_ROOT", "LH_EXECUTION_FENCE_BACKEND")
ROLE_SCRIPT = r'''"""Stand-in recovery role: answer the projected frame on stdin; imports no engine module."""
import hashlib, json, sys
from pathlib import Path

def digest(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()

mode, log = sys.argv[1], Path(sys.argv[2])
frame = json.loads(sys.stdin.read() or "{}")
request = frame["recovery_request"]
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"role": mode, "frame_mode": frame.get("mode"), "request_id": request.get("request_id")}) + "\n")
request_digest = digest(request)
if mode in ("planner", "bad-planner"):
    plan = {"request_id": request["request_id"], "request_digest": request_digest, "action": {"kind": "request_authority"},
            "producer_identity": {"principal": "planner"},
            "decision_basis": {"request_digest": request_digest, "evidence_refs": request["sanitized_evidence_refs"],
                               "reason": request["reason_code"] + ": the same acceptance keeps failing"},
            "preconditions": {k: request[k] for k in ("candidate_digest", "attempt", "fence", "packet_digest",
                                                       "envelope_digest", "authority_digest", "capability_binding")},
            "authority_comparison": {"authority_digest": request["authority_digest"], "write_set": request["write_set"],
                                     "budget": request["remaining_budget"]}}
    plan["plan_digest"] = digest(plan) if mode == "planner" else "sha256:" + "0" * 64
    print(json.dumps(plan))
else:
    print(json.dumps({"request_id": request["request_id"], "request_digest": request_digest, "principal": "reviewer",
                      "verdict": "GREEN" if mode == "reviewer" else "RED", "reasons": ["checked the bound plan"],
                      "read_only": True, "source_write": False,
                      "plan_digest": frame["planner_result"]["plan_digest"],
                      "candidate_digest": request["candidate_digest"], "authority_digest": request["authority_digest"]}))
'''


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def git_state(repo: Path) -> dict[str, str]:
    def run(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True).stdout
    return {"head": run("rev-parse", "HEAD").strip(), "status": run("status", "--porcelain", "--untracked-files=all")}


def scenario(root: Path, *, planner: str = "planner", reviewer: str = "reviewer",
             children_match_lamp: bool = True) -> dict[str, Any]:
    root.mkdir(parents=True)
    script = root / "role.py"
    script.write_text(ROLE_SCRIPT, encoding="utf-8")
    log = root / "roles.jsonl"
    failing = [sys.executable, "-B", "-c", "raise SystemExit(1)"]
    # A mismatched child: its acceptance lamp passes while its delivery verifier fails, so the child
    # stops without a failing acceptance receipt -- recovery must refuse to build on it.
    lamp = failing if children_match_lamp else [sys.executable, "-B", "-c", "pass"]
    child_verifier = failing
    planner_argv = [sys.executable, "-B", str(script), planner, str(log)]
    reviewer_argv = [sys.executable, "-B", str(script), reviewer, str(log)]

    def contract_edit(contract: dict[str, Any]) -> None:
        contract["campaign"]["stages"][0]["acceptance_lamp"]["verification_argv"] = lamp
        contract["planner_recovery"].update(planner_argv=planner_argv, plan_verifier_argv=reviewer_argv)

    def registry_edit(registry: dict[str, Any]) -> None:
        registry["providers"]["planner"]["command"] = planner_argv
        registry["providers"]["reviewer"]["command"] = reviewer_argv

    chain = chain_fixture.Chain(root / "chain", contract_edit=contract_edit, registry_edit=registry_edit)
    runtime = root / "chain" / "rt"
    goals = GoalStore(runtime / "goals")
    envelope = CampaignCompiler(chain.campaign).compile()["stages"]["feature"]
    for index in range(3):
        goal_id = f"native-campaign:feature-{index}"
        bundle = make_native_bundle(
            chain.source, chain.base, goal_id, "feature",
            [{"id": "noop", "commands": [{"id": "noop", "argv": [sys.executable, "-B", "-c", "pass"], "cwd": "${WORKTREE}",
                                          "expect_exit": 0, "timeout_seconds": 30}], "required_receipts": ["executor"]}],
            child_verifier, ["src/"], 1, goal={"feature_contract": "fixture", "admission_envelope": envelope})
        goals.record_event(event_id=f"seed-{index}", idempotency_key=f"seed-{index}", source="manual_intent",
                           event_type="goal_candidate",
                           payload={"candidate": {"goal_id": goal_id, "campaign_id": "native-campaign",
                                                  "stage_id": "feature", "goal": bundle["goal"]}})

    def factory(*, timeout_seconds: float = 900) -> Callable[..., dict[str, Any]]:
        def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
            (workspace / "src").mkdir(exist_ok=True)
            (workspace / "src" / f"attempt-{capsule['attempt']}.txt").write_bytes(b"fixture\n")
            return {"summary": "fixture change", "usage": {"state": "unknown"}}
        return model

    before_env = {key: os.environ.get(key) for key in INJECTED}
    before_source = git_state(chain.source)
    error = None
    with chain_fixture.environment(LH_SCHEDULER_OWNER_ID=chain_fixture.OWNER, LH_TRUSTED_BOOTSTRAP_ROOT=chain.trust_root,
                                   LH_EXECUTION_FENCE_BACKEND="local-process"):
        try:
            glr.run(executor="fixture", execute=True, goal_store_root=runtime / "goals", run_store_root=runtime / "runs",
                    workspace_root=runtime / "ws", campaign=chain.campaign, source_repo=chain.source,
                    base_revision=chain.base, max_cycles=12, idle_limit=2, sleep_fn=lambda _s: None,
                    factory_overrides={"fixture": factory}, planner_recovery=chain.seal, dispatch_envelope=chain.dispatch)
        except Exception as exc:  # recorded and judged by the cases, never swallowed silently
            error = f"{type(exc).__name__}: {exc}"
    runs = RunStore(runtime / "runs")
    children = [goals.get_goal(f"native-campaign:feature-{index}") for index in range(3)]
    return {
        "error": error,
        "events": {event["event_type"]: event for event in goals.events_from("campaign_recovery")},
        "children": [{"state": child["state"], "run": runs.get_run(child["run_id"]) if child.get("run_id") else None}
                     for child in children],
        "roles": [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.exists() else [],
        "source_unchanged": git_state(chain.source) == before_source,
        "environment_restored": {key: os.environ.get(key) for key in INJECTED} == before_env,
    }


def _record(out: dict[str, Any]) -> dict[str, Any]:
    event = out["events"].get("campaign_recovery_requested") or {}
    return {"state": event.get("state"), **(event.get("result") or {})}


def c1_c2_c5_happy(root: Path) -> list[dict[str, Any]]:
    out = scenario(root)
    record = _record(out)
    children_kept = all(child["state"] == "stopped" and child["run"] and child["run"]["attempts"] == 1
                        and child["run"]["state"] == "stopped" for child in out["children"])
    result, verdict = record.get("result") or {}, record.get("verdict") or {}
    claims = record.get("claims") or []
    roles_ok = ([row["role"] for row in out["roles"]] == ["planner", "reviewer"]
                and {row["request_id"] for row in out["roles"]} == {record.get("request_id")}
                and [row["frame_mode"] for row in out["roles"]] == ["planner", "plan_verifier"])
    metadata_ok = all(isinstance(container.get(key), dict) for container in (result, verdict)
                      for key in ("execution_fence", "provider_input_binding", "capability_binding"))
    metadata_ok = (metadata_ok and result["provider_input_binding"].get("schema") == "lh-provider-input-binding/v1"
                   and result["capability_binding"].get("role") == "planner"
                   and verdict["capability_binding"].get("role") == "verifier")
    claims_ok = (len(claims) == 2 and all(claim.get("state") == "success" and claim.get("process_identity")
                                          and claim.get("stdout_digest") for claim in claims))
    return [
        case("real-failures-end-waiting-for-authority-and-nothing-is-applied",
             out["error"] is None and record.get("state") == "human_required"
             and record.get("status") == "awaiting_authority"
             and record.get("reason") == "campaign_recovery_child_attempt_budget_exhausted"
             and (record.get("apply") or {}).get("status") == "awaiting_authority" and children_kept,
             {"error": out["error"], "status": record.get("status"), "reason": record.get("reason"),
              "children": [child["state"] for child in out["children"]]}),
        case("roles-run-once-each-through-the-real-fence", roles_ok and metadata_ok and claims_ok,
             {"roles": out["roles"], "metadata": metadata_ok, "claims": [claim.get("state") for claim in claims]}),
        case("the-run-is-isolated", out["source_unchanged"] and out["environment_restored"],
             {"source_unchanged": out["source_unchanged"], "environment_restored": out["environment_restored"]}),
    ]


def c3_bad_roles(root: Path) -> dict[str, Any]:
    bad_plan = _record(scenario(root / "bad-plan", planner="bad-planner"))
    red_verdict = _record(scenario(root / "red-verdict", reviewer="red-reviewer"))
    ok = all(row.get("status") == "rejected" and row.get("reason") == "campaign_recovery_plan_invalid"
             for row in (bad_plan, red_verdict))
    return case("a-role-answer-that-is-not-bound-or-not-green-is-rejected", ok,
                {"bad_plan": [bad_plan.get("status"), bad_plan.get("reason")],
                 "red_verdict": [red_verdict.get("status"), red_verdict.get("reason")]})


def c4_mismatch(root: Path) -> dict[str, Any]:
    out = scenario(root, children_match_lamp=False)
    rejected = out["events"].get("campaign_recovery_rejected") or {}
    result = rejected.get("result") or {}
    ok = (out["error"] is None and rejected.get("state") == "human_required"
          and result.get("reason") == "campaign_recovery_child_receipt_mismatch"
          and "campaign_recovery_requested" not in out["events"])
    return case("a-child-receipt-mismatch-is-recorded-not-a-crash", ok,
                {"error": out["error"], "state": rejected.get("state"), "reason": result.get("reason")})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-campaign-recovery-e2e-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        groups: list[tuple[list[str], Callable[[], list[dict[str, Any]]]]] = [
            (["real-failures-end-waiting-for-authority-and-nothing-is-applied",
              "roles-run-once-each-through-the-real-fence", "the-run-is-isolated"], lambda: c1_c2_c5_happy(root / "happy")),
            (["a-role-answer-that-is-not-bound-or-not-green-is-rejected"], lambda: [c3_bad_roles(root / "bad")]),
            (["a-child-receipt-mismatch-is-recorded-not-a-crash"], lambda: [c4_mismatch(root / "mismatch")]),
        ]
        for names, build in groups:
            try:
                results.extend(build())
            except Exception as exc:  # a crash is a failed exam, never a skip
                results.extend(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}") for name in names)
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

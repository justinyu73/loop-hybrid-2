#!/usr/bin/env python3
"""Committed W6a smoke: challenger grill before a run's last allowed attempt.

Proves, offline with fixture models and fixture grill runners, that a sync
run reaching its final attempt first passes a bounded challenger judgment:
runner-fixable injects the diagnosis into the final capsule (same executor);
goal-broken skips the final attempt and routes the goal to a human; a failed
final attempt after runner-fixable routes human with the grill chain as
durable evidence; a judge outage, out-of-set output, or absent judge config
records the FailureCase and routes human; a verifier PASS is not marked
resolved when the subsequent value gate is RED; and the grill never fires
before the final attempt or on a first-attempt success. No network, no real
CLI, no real credentials.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import make_campaign, make_source_repo
from campaign_compiler import CampaignCompiler
from controller import LoopController
from grill_loop import MAX_DIAGNOSIS_CHARS, grill_evidence, validate_decision
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner
from native_delivery_fixture import make_native_bundle

CAMPAIGN_ID = "campaign-w6a"
GOAL_ID = f"{CAMPAIGN_ID}:stage-1"
DIAGNOSIS = "lamp needs a staged change under src/; write the file, then stop"


def _campaign(
    source: Path,
    base: str,
    *,
    allowed_paths: list[str] | None = None,
    verifier_argv: list[str] | None = None,
) -> dict:
    """Lamp requires a specific marker, so a failing attempt can still leave a
    (varying) staged change: the W6b no-progress line stops a run after two
    identical failure signatures, and these scenarios need three distinct
    failures to reach the final attempt."""
    campaign = make_campaign(CAMPAIGN_ID)
    allowed = list(allowed_paths or ["src/"])
    lamp_argv = list(verifier_argv or ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"])
    campaign["stages"][0]["acceptance_lamp"] = {
        "id": "stage-1-lamp",
        "smoke": "src/out.txt carries the fixed marker",
        "verification_argv": lamp_argv,
    }
    stage = campaign["stages"][0]
    stage["allowed_paths"] = allowed
    compiled = CampaignCompiler(campaign).compile()["stages"]
    binding = make_native_bundle(
        source,
        base,
        GOAL_ID,
        "stage-1",
        [{
            "id": "grill-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if Path('src/out.txt').is_file() else 1)"],
        allowed,
        int(stage["max_attempts"]),
        goal={"feature_contract": stage["goal"], "admission_envelope": compiled["stage-1"]},
    )
    stage["goal"] = {
        **stage["goal"],
        "delivery_required": True,
        "delivery_contract": binding["contract"],
        "delivery_plan": binding["plan"],
        "delivery_packet": binding["packet"],
    }
    return campaign


def _worker(
    root: Path,
    tag: str,
    source: Path,
    base: str,
    *,
    grill_runner=None,
    allowed_paths: list[str] | None = None,
    verifier_argv: list[str] | None = None,
) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs", command_runner=fixture_command_runner)
    compiler = CampaignCompiler(
        _campaign(source, base, allowed_paths=allowed_paths, verifier_argv=verifier_argv)
    )
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={CAMPAIGN_ID: compiler},
        execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        grill_runner=grill_runner,
    )


def _seed_goal(
    worker: GoalLoopWorker,
    tag: str,
    *,
    allowed_paths: list[str] | None = None,
    verifier_argv: list[str] | None = None,
) -> None:
    envelope = worker.compilers[CAMPAIGN_ID].compile()["stages"]["stage-1"]
    allowed = list(allowed_paths or ["src/"])
    verifier = list(
        verifier_argv
        or [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if Path('src/out.txt').is_file() else 1)"]
    )
    binding = make_native_bundle(
        worker.execution_context[CAMPAIGN_ID]["source_repo"],
        worker.execution_context[CAMPAIGN_ID]["base_revision"],
        GOAL_ID,
        "stage-1",
        [{
            "id": "grill-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        verifier,
        allowed,
        int(envelope["max_attempts"]),
        goal={"feature_contract": "stage-1", "admission_envelope": envelope},
    )
    worker.goal_store.record_event(event_id=f"w6a-{tag}", idempotency_key=f"w6a-{tag}", source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": "stage-1",
                      "goal": binding["goal"]}
    })


def _model(capsules: list[dict], *, succeed_from: int | None = None):
    """Records every capsule; failing attempts leave a varying wrong marker so
    consecutive failure signatures differ (see _campaign)."""
    def model(workspace: Path, capsule: dict) -> dict:
        capsules.append(dict(capsule))
        src = workspace / "src"
        src.mkdir(exist_ok=True)
        fixed = succeed_from is not None and int(capsule["attempt"]) >= succeed_from
        (src / "out.txt").write_text("fixed\n" if fixed else f"wrong {capsule['attempt']}\n", encoding="utf-8")
        return {"summary": "w6a fixture model"}
    return model


def _same_failure_model(capsules: list[dict], *, succeed_from: int | None = None):
    """Leave the same wrong diff until the selected attempt succeeds."""
    def model(workspace: Path, capsule: dict) -> dict:
        capsules.append(dict(capsule))
        src = workspace / "src"
        src.mkdir(exist_ok=True)
        fixed = succeed_from is not None and int(capsule["attempt"]) >= succeed_from
        (src / "out.txt").write_text("fixed\n" if fixed else "same wrong\n", encoding="utf-8")
        return {"summary": "w6a repeated fixture model"}
    return model


def _same_failure_then_scope_creep(capsules: list[dict]):
    """Raise a FailureCase, then pass its lamp with an out-of-scope edit."""
    def model(workspace: Path, capsule: dict) -> dict:
        capsules.append(dict(capsule))
        src = workspace / "src"
        src.mkdir(exist_ok=True)
        if int(capsule["attempt"]) < 3:
            (src / "out.txt").write_text("same wrong\n", encoding="utf-8")
        else:
            (src / "out.txt").write_text("fixed\n", encoding="utf-8")
            outside = workspace / "outside"
            outside.mkdir(exist_ok=True)
            (outside / "leak.txt").write_text("scope creep\n", encoding="utf-8")
        return {"summary": "w6a value-red fixture model"}
    return model


def _same_failure_then_authority_surface(capsules: list[dict]):
    """Raise a FailureCase, then pass the lamp with an in-scope authority edit."""
    def model(workspace: Path, capsule: dict) -> dict:
        capsules.append(dict(capsule))
        src = workspace / "src"
        src.mkdir(exist_ok=True)
        if int(capsule["attempt"]) < 3:
            (src / "out.txt").write_text("same wrong\n", encoding="utf-8")
        else:
            (src / "out.txt").write_text("fixed\n", encoding="utf-8")
            gate_pack = workspace / "gate-pack"
            gate_pack.mkdir(exist_ok=True)
            (gate_pack / "verify.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        return {"summary": "w6a authority-surface value-red fixture model"}
    return model


def _grill(calls: list[dict], response: Any = None, *, error: bool = False):
    def grill(snapshot: dict) -> Any:
        calls.append(snapshot)
        if error:
            raise RuntimeError("grill judge fixture outage")
        return response
    return grill


def _ticks(worker: GoalLoopWorker, tag: str, model, count: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for _ in range(count):
        result = worker.tick(holder=f"w6a-{tag}", model=model)
    return result


def _run_id(worker: GoalLoopWorker) -> str:
    runs = worker.run_store.runnable_runs() or worker.run_store.terminal_runs()
    return runs[0]["run_id"]


def _grill_text(capsule: dict) -> str | None:
    texts = capsule.get("provider_context_texts")
    return texts.get("grill_note") if isinstance(texts, dict) else None


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # A: runner-fixable -> the final attempt runs with the diagnosis injected.
        worker_a = _worker(root, "a", source, base, grill_runner=_grill(calls_a := [], {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_a, "a")
        capsules_a: list[dict] = []
        model_a = _model(capsules_a, succeed_from=4)
        _ticks(worker_a, "a", model_a, 3)
        calls_before_final = len(calls_a)
        _ticks(worker_a, "a", model_a, 1)
        run_a = worker_a.run_store.get_run(_run_id(worker_a))
        goal_a = worker_a.goal_store.get_goal(GOAL_ID)["state"]
        evidence_a = grill_evidence(worker_a.run_store, run_a["run_id"])
        case_a = worker_a.run_store.failure_case_for_run(run_a["run_id"])
        replay_a = worker_a.run_store.record_grill_result(
            case_a["failure_case_id"],
            generation=1,
            fence=case_a["claim_fence"],
            decision="runner-fixable",
            diagnosis=DIAGNOSIS,
        ) if case_a is not None else {}

        # B: goal-broken -> no final attempt, goal routes human with the diagnosis.
        worker_b = _worker(root, "b", source, base, grill_runner=_grill(calls_b := [], {"decision": "goal-broken", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_b, "b")
        capsules_b: list[dict] = []
        _ticks(worker_b, "b", _model(capsules_b), 3)
        tick_b = _ticks(worker_b, "b", _model(capsules_b), 1)
        run_b = worker_b.run_store.get_run(_run_id(worker_b))
        goal_b = worker_b.goal_store.get_goal(GOAL_ID)["state"]
        evidence_b = grill_evidence(worker_b.run_store, run_b["run_id"])

        # C: runner-fixable but the final attempt still fails -> human with the grill chain.
        worker_c = _worker(root, "c", source, base, grill_runner=_grill(calls_c := [], {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_c, "c")
        capsules_c: list[dict] = []
        tick_c = _ticks(worker_c, "c", _model(capsules_c), 4)
        run_c = worker_c.run_store.get_run(_run_id(worker_c))
        goal_c = worker_c.goal_store.get_goal(GOAL_ID)["state"]

        # D: judge outage is recorded and routes human without another attempt.
        worker_d = _worker(root, "d", source, base, grill_runner=_grill(calls_d := [], error=True))
        _seed_goal(worker_d, "d")
        capsules_d: list[dict] = []
        _ticks(worker_d, "d", _model(capsules_d, succeed_from=4), 4)
        run_d = worker_d.run_store.get_run(_run_id(worker_d))
        goal_d = worker_d.goal_store.get_goal(GOAL_ID)["state"]
        case_d = worker_d.run_store.failure_case_for_run(run_d["run_id"])
        failed_d = [item for item in worker_d.run_store.failure_outbox() if item["event_type"] == "grill_failed"]

        # E: out-of-set decision is recorded and routes human.
        worker_e = _worker(root, "e", source, base, grill_runner=_grill(calls_e := [], {"decision": "retry-forever", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_e, "e")
        capsules_e: list[dict] = []
        _ticks(worker_e, "e", _model(capsules_e, succeed_from=4), 4)
        run_e = worker_e.run_store.get_run(_run_id(worker_e))
        goal_e = worker_e.goal_store.get_goal(GOAL_ID)["state"]
        case_e = worker_e.run_store.failure_case_for_run(run_e["run_id"])

        # F: no judge configured cannot silently consume the final attempt.
        worker_f = _worker(root, "f", source, base)
        _seed_goal(worker_f, "f")
        capsules_f: list[dict] = []
        _ticks(worker_f, "f", _model(capsules_f, succeed_from=4), 4)
        run_f = worker_f.run_store.get_run(_run_id(worker_f))
        goal_f = worker_f.goal_store.get_goal(GOAL_ID)["state"]
        case_f = worker_f.run_store.failure_case_for_run(run_f["run_id"])

        # G: identical signatures raise grill after attempt 2; a failed guided
        # attempt creates generation 2, whose guided attempt passes the checker.
        worker_g = _worker(root, "g", source, base, grill_runner=_grill(calls_g := [], {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_g, "g")
        capsules_g: list[dict] = []
        model_g = _same_failure_model(capsules_g, succeed_from=4)
        tick_g2 = _ticks(worker_g, "g", model_g, 2)
        run_g2 = worker_g.run_store.get_run(_run_id(worker_g))
        case_g2 = worker_g.run_store.failure_case_for_run(run_g2["run_id"])
        _ticks(worker_g, "g", model_g, 1)
        case_g3 = worker_g.run_store.failure_case_for_run(run_g2["run_id"])
        tick_g4 = _ticks(worker_g, "g", model_g, 1)
        run_g4 = worker_g.run_store.get_run(run_g2["run_id"])
        case_g4 = worker_g.run_store.failure_case_for_run(run_g2["run_id"])
        outbox_g = worker_g.run_store.failure_outbox()

        # H: first-attempt success never touches the grill.
        worker_h = _worker(root, "h", source, base, grill_runner=_grill(calls_h := [], {"decision": "goal-broken", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_h, "h")
        _ticks(worker_h, "h", _model([], succeed_from=1), 1)
        run_h = worker_h.run_store.get_run(_run_id(worker_h))

        # I: verifier PASS is not a complete checker PASS. A subsequent value
        # RED must keep the FailureCase unresolved and must not emit closed.
        worker_i = _worker(root, "i", source, base, grill_runner=_grill(calls_i := [], {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_i, "i")
        capsules_i: list[dict] = []
        # The third attempt writes outside the packet scope.  The mandatory
        # delivery gate must reject that terminal transition; one bounded
        # restart tick then reconciles the receipt to human_required.
        tick_i = _ticks(worker_i, "i", _same_failure_then_scope_creep(capsules_i), 4)
        run_i = worker_i.run_store.get_run(_run_id(worker_i))
        case_i = worker_i.run_store.failure_case_for_run(run_i["run_id"])
        outbox_i = worker_i.run_store.failure_outbox()
        events_i = worker_i.run_store.events(run_i["run_id"])
        scope_red_i = [
            event for event in events_i
            if event["event_type"] == "delivery_terminal_rejected"
            and event["payload"].get("reason") == "changed_path_outside_packet_scope"
            and event["payload"].get("delivery", {}).get("detail") == "outside/leak.txt"
        ]

        # J: the packet explicitly permits the authority path, so delivery is
        # GREEN; the independent value reducer must still keep the FailureCase
        # unresolved and record machine_resolved=False.
        authority_allowed = ["src/", "gate-pack/"]
        worker_j = _worker(
            root,
            "j",
            source,
            base,
            grill_runner=_grill(calls_j := [], {"decision": "runner-fixable", "diagnosis": DIAGNOSIS}),
            allowed_paths=authority_allowed,
            verifier_argv=["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"],
        )
        _seed_goal(
            worker_j,
            "j",
            allowed_paths=authority_allowed,
            verifier_argv=[sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if Path('src/out.txt').is_file() else 1)"],
        )
        capsules_j: list[dict] = []
        tick_j = _ticks(worker_j, "j", _same_failure_then_authority_surface(capsules_j), 4)
        run_j = worker_j.run_store.get_run(_run_id(worker_j))
        case_j = worker_j.run_store.failure_case_for_run(run_j["run_id"])
        outbox_j = worker_j.run_store.failure_outbox()

        rejects = [
            validate_decision(raw)["type"] == "reject"
            for raw in (
                {"decision": "runner-fixable", "diagnosis": "x", "extra": 1},
                {"decision": "runner-fixable", "diagnosis": 42},
                {"decision": "runner-fixable", "diagnosis": " "},
                {"decision": "runner-fixable", "diagnosis": "x" * (MAX_DIAGNOSIS_CHARS + 1)},
                {"decision": "retry-forever", "diagnosis": "x"},
                "not-a-dict",
            )
        ]

        cases = [
            {"id": "runner-fixable-injects-diagnosis-into-final-capsule",
             "ok": len(calls_a) == 1 and len(capsules_a) == 4
             and all(_grill_text(capsule) is None for capsule in capsules_a[:3])
             and _grill_text(capsules_a[3]) == DIAGNOSIS
             and run_a["state"] == "verified" and goal_a == "completed"
             and evidence_a is not None
             and evidence_a["decision"] == "runner-fixable"
             and evidence_a["diagnosis"] == DIAGNOSIS
             and evidence_a["attempts_used"] == 3
             and case_a is not None and case_a["state"] == "resolved"
             and case_a["next_node_id"] == "campaign_completed"
             and replay_a.get("status") == "reused",
             "detail": json.dumps({"grill_calls": len(calls_a), "attempts": len(capsules_a),
                                   "note": _grill_text(capsules_a[3]) if len(capsules_a) == 4 else None,
                                   "run_state": run_a["state"], "goal": goal_a})},
            {"id": "grill-fires-only-before-the-final-attempt",
             "ok": calls_before_final == 0 and len(calls_a) == 1
             and calls_a[0]["attempts_used"] == 3 and calls_a[0]["next_attempt"] == 4
             and len(calls_a[0]["prior_attempts"]) == 3,
             "detail": json.dumps({"calls_before_final": calls_before_final, "snapshot": {
                 "attempts_used": calls_a[0].get("attempts_used"), "next_attempt": calls_a[0].get("next_attempt"),
                 "prior": len(calls_a[0].get("prior_attempts", []))} if calls_a else None})},
            {"id": "goal-broken-skips-final-attempt-and-routes-human",
             "ok": len(calls_b) == 1 and len(capsules_b) == 3 and run_b["attempts"] == 3
             and goal_b == "human_required"
             and tick_b.get("run", {}).get("status") == "human_required"
             and tick_b.get("run", {}).get("grill", {}).get("diagnosis") == DIAGNOSIS
             and evidence_b is not None and evidence_b["decision"] == "goal-broken",
             "detail": json.dumps({"attempts": run_b["attempts"], "dispatches": len(capsules_b),
                                   "goal": goal_b, "route": tick_b.get("run", {}).get("status")})},
            {"id": "failed-final-attempt-routes-human-with-grill-evidence",
             "ok": len(calls_c) == 1 and len(capsules_c) == 4
             and _grill_text(capsules_c[3]) == DIAGNOSIS
             and run_c["state"] == "stopped" and goal_c == "human_required"
             and tick_c.get("terminal_after", {}).get("status") == "human_required"
             and tick_c.get("terminal_after", {}).get("grill", {}).get("diagnosis") == DIAGNOSIS,
             "detail": json.dumps({"run_state": run_c["state"], "goal": goal_c,
                                   "terminal": tick_c.get("terminal_after", {}).get("status")})},
            {"id": "judge-outage-records-once-and-routes-human",
             "ok": len(calls_d) == 1 and len(capsules_d) == 3
             and run_d["state"] == "retry_pending" and goal_d == "human_required"
             and case_d is not None and case_d["state"] == "human_required"
             and len(failed_d) == 1,
             "detail": json.dumps({"grill_calls": len(calls_d), "attempts": len(capsules_d),
                                   "run_state": run_d["state"], "goal": goal_d,
                                   "failure_case": case_d, "failed_events": len(failed_d)})},
            {"id": "out-of-set-output-records-and-routes-human",
             "ok": len(calls_e) == 1 and len(capsules_e) == 3
             and run_e["state"] == "retry_pending" and goal_e == "human_required"
             and case_e is not None and case_e["state"] == "human_required",
             "detail": json.dumps({"grill_calls": len(calls_e), "attempts": len(capsules_e),
                                   "run_state": run_e["state"], "goal": goal_e,
                                   "failure_case": case_e})},
            {"id": "closed-set-validation-rejects-malformed-output",
             "ok": all(rejects) and len(rejects) == 6,
             "detail": json.dumps({"rejects": rejects})},
            {"id": "no-judge-configured-records-and-routes-human",
             "ok": len(capsules_f) == 3 and all(_grill_text(capsule) is None for capsule in capsules_f)
             and run_f["state"] == "retry_pending" and goal_f == "human_required"
             and case_f is not None and case_f["state"] == "human_required",
             "detail": json.dumps({"attempts": len(capsules_f), "run_state": run_f["state"],
                                   "goal": goal_f, "failure_case": case_f})},
            {"id": "identical-failure-regrills-then-checks-and-closes",
             "ok": tick_g2.get("run", {}).get("failure_case", {}).get("state") == "grill_required"
             and run_g2["attempts"] == 2 and case_g2 is not None and case_g2["grill_generation"] == 1
             and case_g3 is not None and case_g3["state"] == "grill_required" and case_g3["grill_generation"] == 2
             and len(calls_g) == 2 and len(capsules_g) == 4
             and _grill_text(capsules_g[2]) == DIAGNOSIS
             and _grill_text(capsules_g[3]) == DIAGNOSIS
             and run_g4["state"] == "verified"
             and case_g4 is not None and case_g4["state"] == "resolved"
             and case_g4["grill_generation"] == 2
             and case_g4["next_node_id"] == "campaign_completed"
             and tick_g4.get("terminal_after", {}).get("failure_case", {}).get("next_node_id") == "campaign_completed"
             and len([item for item in outbox_g if item["event_type"] == "grill_result"]) == 2
             and len([item for item in outbox_g if item["event_type"] == "resolution_checked"]) == 2,
             "detail": json.dumps({"after_2": case_g2, "after_3": case_g3, "after_4": case_g4,
                                   "calls": len(calls_g), "capsules": len(capsules_g),
                                   "outbox": [item["event_type"] for item in outbox_g]})},
            {"id": "first-attempt-success-never-invokes-grill",
             "ok": len(calls_h) == 0 and run_h["state"] == "verified",
             "detail": json.dumps({"grill_calls": len(calls_h), "run_state": run_h["state"]})},
            {"id": "scope-red-never-prematurely-resolves-failure-case",
             "ok": len(calls_i) == 1 and len(capsules_i) == 4
             and run_i["state"] == "human_required"
             and scope_red_i
             and not any(item["event_type"] == "fence_rejected" for item in events_i)
             and case_i is not None and case_i["state"] == "remediation_running"
             and len([item for item in outbox_i if item["event_type"] == "failure_case_closed"]) == 0,
             "detail": json.dumps({"calls": len(calls_i), "capsules": len(capsules_i),
                                   "terminal": tick_i.get("terminal_before"), "scope_red": scope_red_i, "case": case_i,
                                   "outbox": [item["event_type"] for item in outbox_i]})},
            {"id": "value-red-never-prematurely-resolves-failure-case",
             "ok": len(calls_j) == 1 and len(capsules_j) == 3
             and run_j["state"] == "verified"
             and case_j is not None and case_j["state"] == "human_required"
             and any(item["event_type"] == "resolution_checked" and item["payload"].get("machine_resolved") is False for item in outbox_j)
             and len([item for item in outbox_j if item["event_type"] == "failure_case_closed"]) == 0
             and not any(item["event_type"] == "delivery_terminal_rejected" for item in worker_j.run_store.events(run_j["run_id"])),
             "detail": json.dumps({"calls": len(calls_j), "capsules": len(capsules_j),
                                   "run": run_j, "case": case_j,
                                   "outbox": [item["event_type"] for item in outbox_j]})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-grill-loop",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/grill_loop_canary.py",
                         "fixtures": "injected models and grill runners only; no network, no real CLI"},
        "known_gaps_open": [
            "live judge CLI smoke needs per-run approval and is not covered here",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

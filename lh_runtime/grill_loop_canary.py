#!/usr/bin/env python3
"""Committed W6a smoke: challenger grill before a run's last allowed attempt.

Proves, offline with fixture models and fixture grill runners, that a sync
run reaching its final attempt first passes a bounded challenger judgment:
runner-fixable injects the diagnosis into the final capsule (same executor);
goal-broken skips the final attempt and routes the goal to a human; a failed
final attempt after runner-fixable routes human with the grill chain as
durable evidence; a judge outage, out-of-set output, or absent judge config
degrades to the original max_attempts behavior; and the grill never fires
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
from _fixture import make_campaign, make_source_repo
from campaign_compiler import CampaignCompiler
from controller import LoopController
from grill_loop import MAX_DIAGNOSIS_CHARS, grill_evidence, validate_decision
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN_ID = "campaign-w6a"
GOAL_ID = f"{CAMPAIGN_ID}:stage-1"
DIAGNOSIS = "lamp needs a staged change under src/; write the file, then stop"


def _campaign() -> dict:
    """Lamp requires a specific marker, so a failing attempt can still leave a
    (varying) staged change: the W6b no-progress line stops a run after two
    identical failure signatures, and these scenarios need three distinct
    failures to reach the final attempt."""
    campaign = make_campaign(CAMPAIGN_ID)
    campaign["stages"][0]["acceptance_lamp"] = {
        "id": "stage-1-lamp",
        "smoke": "src/out.txt carries the fixed marker",
        "verification_argv": ["sh", "-c", "test -f src/out.txt && grep -q '^fixed$' src/out.txt"],
    }
    return campaign


def _worker(root: Path, tag: str, source: Path, base: str, *, grill_runner=None) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    compiler = CampaignCompiler(_campaign())
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={CAMPAIGN_ID: compiler},
        execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
        grill_runner=grill_runner,
    )


def _seed_goal(worker: GoalLoopWorker, tag: str) -> None:
    envelope = worker.compilers[CAMPAIGN_ID].compile()["stages"]["stage-1"]
    worker.goal_store.record_event(event_id=f"w6a-{tag}", idempotency_key=f"w6a-{tag}", source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": "stage-1",
                      "goal": {"feature_contract": "stage-1", "admission_envelope": envelope}}
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

        # D: judge outage -> degrade; the final attempt dispatches as today.
        worker_d = _worker(root, "d", source, base, grill_runner=_grill(calls_d := [], error=True))
        _seed_goal(worker_d, "d")
        capsules_d: list[dict] = []
        _ticks(worker_d, "d", _model(capsules_d, succeed_from=4), 4)
        run_d = worker_d.run_store.get_run(_run_id(worker_d))

        # E: out-of-set decision -> reject -> degrade to the original dispatch.
        worker_e = _worker(root, "e", source, base, grill_runner=_grill(calls_e := [], {"decision": "retry-forever", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_e, "e")
        capsules_e: list[dict] = []
        _ticks(worker_e, "e", _model(capsules_e, succeed_from=4), 4)
        run_e = worker_e.run_store.get_run(_run_id(worker_e))

        # F: no grill configured -> identical to current behavior.
        worker_f = _worker(root, "f", source, base)
        _seed_goal(worker_f, "f")
        capsules_f: list[dict] = []
        _ticks(worker_f, "f", _model(capsules_f, succeed_from=4), 4)
        run_f = worker_f.run_store.get_run(_run_id(worker_f))

        # H: first-attempt success never touches the grill.
        worker_h = _worker(root, "h", source, base, grill_runner=_grill(calls_h := [], {"decision": "goal-broken", "diagnosis": DIAGNOSIS}))
        _seed_goal(worker_h, "h")
        _ticks(worker_h, "h", _model([], succeed_from=1), 1)
        run_h = worker_h.run_store.get_run(_run_id(worker_h))

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
             and all("grill_note" not in capsule for capsule in capsules_a[:3])
             and capsules_a[3].get("grill_note") == DIAGNOSIS
             and run_a["state"] == "verified" and goal_a == "completed"
             and evidence_a == {"decision": "runner-fixable", "diagnosis": DIAGNOSIS, "attempts_used": 3},
             "detail": json.dumps({"grill_calls": len(calls_a), "attempts": len(capsules_a),
                                   "note": capsules_a[3].get("grill_note") if len(capsules_a) == 4 else None,
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
             and capsules_c[3].get("grill_note") == DIAGNOSIS
             and run_c["state"] == "stopped" and goal_c == "human_required"
             and tick_c.get("terminal_after", {}).get("status") == "human_required"
             and tick_c.get("terminal_after", {}).get("grill", {}).get("diagnosis") == DIAGNOSIS,
             "detail": json.dumps({"run_state": run_c["state"], "goal": goal_c,
                                   "terminal": tick_c.get("terminal_after", {}).get("status")})},
            {"id": "judge-outage-degrades-to-original-dispatch",
             "ok": len(calls_d) == 1 and len(capsules_d) == 4
             and "grill_note" not in capsules_d[3] and run_d["state"] == "verified",
             "detail": json.dumps({"grill_calls": len(calls_d), "attempts": len(capsules_d), "run_state": run_d["state"]})},
            {"id": "out-of-set-output-rejected-and-degrades",
             "ok": len(calls_e) == 1 and len(capsules_e) == 4
             and "grill_note" not in capsules_e[3] and run_e["state"] == "verified",
             "detail": json.dumps({"grill_calls": len(calls_e), "attempts": len(capsules_e), "run_state": run_e["state"]})},
            {"id": "closed-set-validation-rejects-malformed-output",
             "ok": all(rejects) and len(rejects) == 6,
             "detail": json.dumps({"rejects": rejects})},
            {"id": "no-judge-configured-keeps-current-behavior",
             "ok": len(capsules_f) == 4 and all("grill_note" not in capsule for capsule in capsules_f)
             and run_f["state"] == "verified",
             "detail": json.dumps({"attempts": len(capsules_f), "run_state": run_f["state"]})},
            {"id": "first-attempt-success-never-invokes-grill",
             "ok": len(calls_h) == 0 and run_h["state"] == "verified",
             "detail": json.dumps({"grill_calls": len(calls_h), "run_state": run_h["state"]})},
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

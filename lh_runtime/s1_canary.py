#!/usr/bin/env python3
"""Committed S1 smoke: authority-surface safety bolt + improvement bridge.

Part 1: a diff touching the acceptance authority (gate-pack/, *_canary.py,
the lamp's own script, AGENTS.md, docs/contracts/, .github/workflows/) is
deterministically value-RED — the engine can never modify its own exam, and
such work routes to a human through the existing value-gate path. Part 2:
improvement findings become standard manual_intent commands 1:1 with
idempotency-keyed dedup, flowing into the same pipeline (unknown campaign
routes human_required downstream). Offline fixtures only; no model anywhere
in the bridge.
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
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from improvement_intent import load_findings, submit_findings
from run_store import RunStore
from value_reducer import value_verdict, verdict_for_run

CAMPAIGN_ID = "campaign-s1"
DIFF_HEADER = "diff --git a/{path} b/{path}\nnew file mode 100644\nindex 0000000..1111111\n--- /dev/null\n+++ b/{path}\n@@ -0,0 +1 @@\n+x\n"


def _diff(path: str) -> str:
    return DIFF_HEADER.format(path=path)


def _verdict(path: str, *, lamp_argv=None) -> dict[str, Any]:
    return value_verdict(exit_code=0, diff_text=_diff(path), allowed_paths=["src/", "gate-pack/", "tests/", ".github/", "docs/", ""], lamp_argv=lamp_argv)


def _seed_run(store: RunStore, run_id: str, *, lamp_argv: list[str], diff_path: str) -> None:
    goal = {"goal_id": f"goal-{run_id}", "admission_envelope": {
        "allowed_paths": ["tests/", "src/"],
        "acceptance_lamp": {"id": "lamp", "smoke": "fixture", "verification_argv": lamp_argv},
    }}
    store.create_run(goal=goal, source_repo=HERE, base_revision="base", run_id=run_id)
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", _diff(diff_path))
    stderr_ref = store.write_artifact(run_id, ordinal, "verifier.stderr", "")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "diff": diff_ref, "verification": {"argv": lamp_argv, "exit_code": 0, "stderr": stderr_ref}}
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])


def _worker(root: Path, source: Path, base: str) -> GoalLoopWorker:
    runs = RunStore(root / "runs")
    compiler = CampaignCompiler(make_campaign(CAMPAIGN_ID))
    return GoalLoopWorker(
        goal_store=GoalStore(root / "goals"),
        run_store=runs,
        controller=LoopController(runs, root / "workspaces"),
        compilers={CAMPAIGN_ID: compiler},
        execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
    )


def _exploding_model(_workspace: Path, _capsule: dict) -> dict:
    raise AssertionError("the bridge or derivation must never invoke a model here")


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # Part 1, verdict-level.
        gate_pack = _verdict("gate-pack/verify.sh")
        canary_file = _verdict("lh_runtime/foo_canary.py")
        agents = _verdict("AGENTS.md")
        contracts = _verdict("docs/contracts/goal-lifecycle-v1.md")
        workflows = _verdict(".github/workflows/ci.yml")
        src_green = _verdict("src/out.txt")

        # Part 1, lamp-script derived from the run's own envelope.
        store = RunStore(root / "runs")
        _seed_run(store, "run-s1-lamp", lamp_argv=["python3", "-B", "tests/stage-2-smoke.py"], diff_path="tests/stage-2-smoke.py")
        lamp_verdict = verdict_for_run(store, "run-s1-lamp")

        # Part 2: findings -> commands with dedup.
        findings_path = root / "findings.json"
        findings_path.write_text(json.dumps({"findings": [
            {"finding_id": "f-1", "campaign_id": CAMPAIGN_ID, "stage_id": "stage-1", "summary": "flake in lamp", "suggested_goal": "stabilize the lamp"},
            {"finding_id": "f-2", "campaign_id": "campaign-ghost", "stage_id": "stage-1", "summary": "unknown campaign"},
        ]}), encoding="utf-8")
        source, base = make_source_repo(root)
        worker = _worker(root, source, base)
        submitted = submit_findings(worker.goal_store, findings_path)
        resubmitted = submit_findings(worker.goal_store, findings_path)

        # The known-campaign finding derives a candidate; the ghost routes human_required.
        tick1 = worker.tick(holder="s1", model=_exploding_model)
        tick2 = worker.tick(holder="s1", model=_exploding_model)
        derived = [event for event in (tick1.get("event"), tick2.get("event")) if event]
        derived_statuses = [event.get("status") for event in derived]
        ghost_events = [event for event in (tick1.get("event"), tick2.get("event")) if event and event.get("status") == "human_required"]

        # Malformed finding: clear error, nothing submitted.
        bad_path = root / "bad.json"
        bad_path.write_text(json.dumps([{"finding_id": "f-bad", "summary": "no campaign"}]), encoding="utf-8")
        bad_error = None
        try:
            load_findings(bad_path)
        except ValueError as exc:
            bad_error = str(exc)

        cases = [
            {"id": "gate-pack-touch-is-red",
             "ok": gate_pack["verdict"] == "RED" and any("authority surface touched: gate-pack/verify.sh" in r for r in gate_pack["reasons"]),
             "detail": json.dumps(gate_pack["reasons"])},
            {"id": "canary-file-touch-is-red",
             "ok": canary_file["verdict"] == "RED" and any("authority surface touched: lh_runtime/foo_canary.py" in r for r in canary_file["reasons"]),
             "detail": json.dumps(canary_file["reasons"])},
            {"id": "lamp-script-touch-is-red-via-envelope",
             "ok": lamp_verdict["verdict"] == "RED" and any("authority surface touched: tests/stage-2-smoke.py" in r for r in lamp_verdict["reasons"]),
             "detail": json.dumps(lamp_verdict["reasons"])},
            {"id": "agents-md-touch-is-red",
             "ok": agents["verdict"] == "RED" and any("authority surface touched: AGENTS.md" in r for r in agents["reasons"]),
             "detail": json.dumps(agents["reasons"])},
            {"id": "contracts-touch-is-red",
             "ok": contracts["verdict"] == "RED" and any("authority surface touched: docs/contracts/goal-lifecycle-v1.md" in r for r in contracts["reasons"]),
             "detail": json.dumps(contracts["reasons"])},
            {"id": "workflows-touch-is-red",
             "ok": workflows["verdict"] == "RED" and any("authority surface touched: .github/workflows/ci.yml" in r for r in workflows["reasons"]),
             "detail": json.dumps(workflows["reasons"])},
            {"id": "normal-src-diff-stays-green",
             "ok": src_green["verdict"] == "GREEN",
             "detail": json.dumps(src_green["reasons"])},
            {"id": "findings-become-commands-with-dedup",
             "ok": len(submitted) == 2 and all(row["status"] == "received" for row in submitted)
             and all(row["status"] == "reused" for row in resubmitted)
             and {row["idempotency_key"] for row in submitted} == {"improvement:f-1", "improvement:f-2"},
             "detail": json.dumps({"submitted": submitted, "resubmitted": [r["status"] for r in resubmitted]})},
            {"id": "pipeline-derives-known-and-routes-ghost",
             "ok": "derived_candidate_event" in derived_statuses and len(ghost_events) == 1,
             "detail": json.dumps({"statuses": derived_statuses})},
            {"id": "malformed-finding-is-a-clear-error",
             "ok": bad_error is not None and "campaign_id" in bad_error,
             "detail": json.dumps({"error": bad_error})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-authority-surface",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/s1_canary.py",
                         "fixtures": "verdict-level diffs + seeded stores; no provider, no network"},
        "known_gaps_open": [
            "the improvement gate's artifact shape is defined here (finding_id/campaign_id/stage_id/summary); aligning improvement_loop's writer to it is a follow-up",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

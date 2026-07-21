#!/usr/bin/env python3
"""Committed W8 smoke: evidence-integrity hardening (three mechanical fixes).

W8-1 staging-integrity: a model that makes a file unreadable breaks
``git add -A``; the attempt must finish as a failure with the staging error
on the receipt, the verifier must never run, and the attempt can never go
green. W8-2 stderr-error-red: a lamp exiting 0 with a system-level error on
its stderr is RED. W8-3 source-consistency: the receipt's lamp argv must be
the admission envelope's approved argv, and every recorded artifact ref must
resolve inside the run store with a matching digest. Offline fixtures only.
"""
from __future__ import annotations

import json
import os
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
from run_store import RunStore
from value_reducer import verdict_for_run

CAMPAIGN_ID = "campaign-w8"
GOAL_ID = f"{CAMPAIGN_ID}:stage-1"
DIFF_TEXT = "diff --git a/src/hello.txt b/src/hello.txt\nnew file mode 100644\nindex 0000000..111111\n--- /dev/null\n+++ b/src/hello.txt\n@@ -0,0 +1 @@\n+hello\n"


def _worker(root: Path, tag: str, source: Path, base: str) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    compiler = CampaignCompiler(make_campaign(CAMPAIGN_ID))
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={CAMPAIGN_ID: compiler},
        execution_context={CAMPAIGN_ID: {"source_repo": source, "base_revision": base}},
    )


def _seed_goal(worker: GoalLoopWorker, tag: str) -> None:
    envelope = worker.compilers[CAMPAIGN_ID].compile()["stages"]["stage-1"]
    worker.goal_store.record_event(event_id=f"w8-{tag}", idempotency_key=f"w8-{tag}", source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": "stage-1",
                      "goal": {"feature_contract": "stage-1", "admission_envelope": envelope}}
    })


def _locking_model(workspace: Path, _capsule: dict) -> dict:
    """Creates a staged change the controller cannot read back (chmod 000)."""
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    target = src / "locked.txt"
    target.write_text("locked\n", encoding="utf-8")
    os.chmod(target, 0)
    return {"summary": "w8 locking fixture"}


def _writable_model(workspace: Path, _capsule: dict) -> dict:
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    (src / "out.txt").write_text("ok\n", encoding="utf-8")
    return {"summary": "w8 writable fixture"}


def _goal(*, lamp_argv: list[str] | None = None) -> dict:
    envelope: dict[str, Any] = {"allowed_paths": ["src/"]}
    if lamp_argv is not None:
        envelope["acceptance_lamp"] = {"id": "lamp", "smoke": "fixture", "verification_argv": lamp_argv}
    return {"feature_contract": "x", "admission_envelope": envelope}


def _seed_run(store: RunStore, *, goal: dict, argv: list[str], exit_code: int = 0,
              stderr_text: str = "", tamper_diff: str | None = None, delete_diff: bool = False) -> str:
    run_id = store.create_run(goal=goal, source_repo=HERE, base_revision="r")
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", DIFF_TEXT)
    stdout_ref = store.write_artifact(run_id, ordinal, "verifier.stdout", "")
    stderr_ref = store.write_artifact(run_id, ordinal, "verifier.stderr", stderr_text)
    provider_ref = store.write_artifact(run_id, ordinal, "provider.json", json.dumps({"summary": "fixture"}))
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
        "provider": {"summary": "fixture", "artifact": provider_ref},
        "diff": diff_ref,
        "verification": {"argv": argv, "exit_code": exit_code, "stdout": stdout_ref, "stderr": stderr_ref},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])
    if tamper_diff is not None:
        (store.root / diff_ref["ref"]).write_text(tamper_diff, encoding="utf-8")
    if delete_diff:
        (store.root / diff_ref["ref"]).unlink()
    return run_id


def _receipt(store: RunStore, run_id: str) -> dict[str, Any]:
    meta = store.latest_receipt(run_id)
    return json.loads((store.root / meta["receipt_ref"]).read_text(encoding="utf-8"))


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # W8-1: unreadable file breaks staging; the attempt can never go green.
        worker_lock = _worker(root, "lock", source, base)
        _seed_goal(worker_lock, "lock")
        tick_lock = worker_lock.tick(holder="w8-lock", model=_locking_model)
        run_lock = worker_lock.run_store.get_run(tick_lock["run"]["run_id"])
        receipt_lock = _receipt(worker_lock.run_store, run_lock["run_id"])
        verification_lock = receipt_lock["verification"]

        # W8-1 control: a normal writable model still goes green, no staging_error key.
        worker_ok = _worker(root, "ok", source, base)
        _seed_goal(worker_ok, "ok")
        tick_ok = worker_ok.tick(holder="w8-ok", model=_writable_model)
        receipt_ok = _receipt(worker_ok.run_store, tick_ok["run"]["run_id"])

        # W8-2 + W8-3 fixtures on raw stores.
        store = RunStore(root / "verdict-runs")
        red_stderr = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"], stderr_text="sh: lamp.sh: line 3: /etc/shadow: Permission denied\n"))
        harmless_stderr = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"], stderr_text="0 errors, 0 warnings\n"))
        empty_stderr = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"]))
        nonzero_exit = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"], exit_code=1))
        swapped_argv = verdict_for_run(store, _seed_run(store, goal=_goal(lamp_argv=["sh", "-c", "true"]), argv=["sh", "-c", "false"]))
        consistent = verdict_for_run(store, _seed_run(store, goal=_goal(lamp_argv=["sh", "-c", "true"]), argv=["sh", "-c", "true"]))
        tampered = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"], tamper_diff="diff --git a/src/evil.txt b/src/evil.txt\n"))
        deleted = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["true"], delete_diff=True))
        no_lamp_mismatch = verdict_for_run(store, _seed_run(store, goal=_goal(), argv=["sh", "-c", "anything"]))

        cases = [
            {"id": "staging-failure-never-goes-green",
             "ok": tick_lock["run"]["status"] == "retry_pending" and run_lock["state"] == "retry_pending"
             and verification_lock["exit_code"] == -1
             and isinstance(verification_lock.get("staging_error"), str)
             and "git add -A" in verification_lock["staging_error"],
             "detail": json.dumps({"status": tick_lock["run"]["status"], "exit_code": verification_lock["exit_code"],
                                   "staging_error": verification_lock.get("staging_error")})},
            {"id": "staging-failure-skips-the-verifier",
             "ok": verification_lock["exit_code"] == -1,
             "detail": "exit_code -1 is only ever recorded when the verifier never ran"},
            {"id": "writable-model-path-unchanged",
             "ok": tick_ok["run"]["status"] == "verified" and "staging_error" not in receipt_ok["verification"],
             "detail": json.dumps({"status": tick_ok["run"]["status"], "keys": sorted(receipt_ok["verification"])})},
            {"id": "exit-zero-with-system-error-stderr-is-red",
             "ok": red_stderr["verdict"] == "RED" and any("permission denied" in reason for reason in red_stderr["reasons"]),
             "detail": json.dumps(red_stderr["reasons"])},
            {"id": "exit-zero-with-harmless-stderr-stays-green",
             "ok": harmless_stderr["verdict"] == "GREEN",
             "detail": json.dumps(harmless_stderr["reasons"])},
            {"id": "exit-zero-with-empty-stderr-unchanged",
             "ok": empty_stderr["verdict"] == "GREEN",
             "detail": json.dumps(empty_stderr["reasons"])},
            {"id": "nonzero-exit-rule-unchanged",
             "ok": nonzero_exit["verdict"] == "RED" and any("did not pass" in reason for reason in nonzero_exit["reasons"])
             and not any("stderr" in reason for reason in nonzero_exit["reasons"]),
             "detail": json.dumps(nonzero_exit["reasons"])},
            {"id": "swapped-lamp-argv-is-red",
             "ok": swapped_argv["verdict"] == "RED" and any("argv" in reason for reason in swapped_argv["reasons"]),
             "detail": json.dumps(swapped_argv["reasons"])},
            {"id": "consistent-receipt-stays-green",
             "ok": consistent["verdict"] == "GREEN",
             "detail": json.dumps(consistent["reasons"])},
            {"id": "tampered-artifact-digest-is-red",
             "ok": tampered["verdict"] == "RED" and any("digest mismatch" in reason and "diff" in reason for reason in tampered["reasons"]),
             "detail": json.dumps(tampered["reasons"])},
            {"id": "deleted-artifact-is-red",
             "ok": deleted["verdict"] == "RED" and any("missing" in reason for reason in deleted["reasons"]),
             "detail": json.dumps(deleted["reasons"])},
            {"id": "envelope-without-lamp-skips-argv-check",
             "ok": no_lamp_mismatch["verdict"] == "GREEN",
             "detail": json.dumps(no_lamp_mismatch["reasons"])},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-evidence-integrity",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/evidence_integrity_canary.py",
                         "fixtures": "injected models and seeded stores only; no network"},
        "known_gaps_open": [
            "provider.json and other model-produced content are never evidence sources (invariant, documented in value_reducer); their refs are integrity-checked only",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

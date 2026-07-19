#!/usr/bin/env python3
"""End-to-end native LH MVP: SQLite, lease, disposable execution, receipt, retry."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from controller import LoopController
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def model_that_changes(workspace: Path, capsule: dict) -> dict:
    (workspace / "agent-change.txt").write_text(f"attempt {capsule['attempt']}\n", encoding="utf-8")
    return {"summary": "added a bounded fixture change"}


def model_that_retries(workspace: Path, capsule: dict) -> dict:
    (workspace / "failed-attempt.txt").write_text(str(capsule["attempt"]), encoding="utf-8")
    return {"summary": "fixture action for a failing verifier"}


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source = root / "source"; source.mkdir()
        git("init", "-q", str(source))
        git("-C", str(source), "config", "user.email", "canary@example.invalid")
        git("-C", str(source), "config", "user.name", "Canary")
        (source / "baseline.txt").write_text("unchanged\n", encoding="utf-8")
        git("-C", str(source), "add", "baseline.txt")
        git("-C", str(source), "commit", "-qm", "baseline")
        base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        store = RunStore(root / "run-store")
        controller = LoopController(store, root / "workspaces")
        goal = {"feature_contract": "add a disposable fixture change"}
        successful_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        lease_held = store.acquire_lease(successful_run, "other-worker")
        busy = controller.tick(successful_run, holder="controller", model=model_that_changes, verifier_argv=["git", "diff", "--check"])
        store.release_lease(successful_run, "other-worker")
        done = controller.tick(successful_run, holder="controller", model=model_that_changes, verifier_argv=["git", "diff", "--check"])
        receipt = json.loads((store.root / done["receipt_ref"]).read_text(encoding="utf-8"))
        interrupted_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        store.begin_attempt(interrupted_run, "workspace://interrupted/1")
        recovered = controller.tick(interrupted_run, holder="controller", model=model_that_changes, verifier_argv=["git", "diff", "--check"])
        receipt_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        receipt_attempt = store.begin_attempt(receipt_run, "workspace://receipt/1")
        receipt_body = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": receipt_run, "attempt": receipt_attempt, "verification": {"exit_code": 0}}
        store.write_artifact(receipt_run, receipt_attempt, "receipt.json", json.dumps(receipt_body, sort_keys=True))
        reconciled = controller.startup()
        retry_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        retries = [controller.tick(retry_run, holder="controller", model=model_that_retries, verifier_argv=[sys.executable, "-c", "raise SystemExit(1)"]) for _ in range(4)]
        source_clean = subprocess.run(["git", "-C", str(source), "status", "--porcelain"], capture_output=True, text=True).stdout == ""
        events = [event["event_type"] for event in store.events(successful_run)]
        cases = [
            case("lease-excludes-second-controller", lease_held and busy.get("status") == "lease_busy", busy.get("status", "")),
            case("model-runs-in-disposable-clone", done.get("status") == "verified" and source_clean, done.get("status", "")),
            case("receipt-keeps-outputs-by-reference", "stdout" not in receipt and receipt["verification"]["stdout"]["ref"].endswith("verifier.stdout"), done["receipt_ref"]),
            case("controller-records-durable-events", events == ["run_created", "attempt_started", "attempt_finished"], ",".join(events)),
            case("expired-attempt-recovers-to-next-tick", recovered.get("status") == "verified" and store.get_run(interrupted_run)["attempts"] == 2 and "attempt_reconciled" in [row["event_type"] for row in store.events(interrupted_run)], recovered.get("status", "")),
            case("startup-reconciler-finalizes-durable-receipt", reconciled == [{"run_id": receipt_run, "attempt": 1, "status": "verified", "recovered_from": "receipt"}] and store.get_run(receipt_run)["state"] == "verified", str(reconciled)),
            case("failed-verifier-retries-until-fourth-attempt", [row["status"] for row in retries] == ["retry_pending", "retry_pending", "retry_pending", "stopped"], str([row["status"] for row in retries])),
            case("workspaces-are-disposed-not-source-reset", not any((root / "workspaces").rglob("agent-change.txt")), "workspace artifacts only"),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-native-runtime-mvp", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["The model runner is injected for this provider-free canary; a provider adapter and GitHub adapter remain separate LH ports."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

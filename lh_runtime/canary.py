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
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from controller import LoopController
from native_delivery_fixture import make_native_run
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


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
        store = RunStore(root / "run-store", command_runner=fixture_command_runner)
        controller = LoopController(store, root / "workspaces")
        goal = {"feature_contract": "add a disposable fixture change"}
        checks = [{
            "id": "native-runtime-check",
            "commands": [{"id": "diff-check", "argv": ["git", "diff", "--check"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}],
            "required_receipts": ["executor"],
        }]
        verifier = ["sh", "-c", "test -f agent-change.txt || test -f failed-attempt.txt"]
        successful_run = make_native_run(
            store, source, base, "native-mvp", "runtime", checks, verifier,
            ["agent-change.txt", "failed-attempt.txt"], 4, goal=goal,
        )["run_id"]
        lease_held = store.acquire_lease(successful_run, "other-worker")
        busy = controller.tick(successful_run, holder="controller", model=model_that_changes, verifier_argv=["sh", "-c", "! git diff --cached --quiet"])
        store.release_lease(successful_run, "other-worker")
        done = controller.tick(successful_run, holder="controller", model=model_that_changes, verifier_argv=["sh", "-c", "! git diff --cached --quiet"])
        # Restart after the real controller/assembler has atomically persisted
        # final delivery evidence.  A durable verified Run is replay-safe: the
        # startup reconciler must not redispatch its model or manufacture a
        # second Attempt.  The separate bare-receipt case below remains a
        # fail-closed negative for receipts without that final evidence.
        verified_restart = controller.startup()
        receipt = json.loads((store.root / done["receipt_ref"]).read_text(encoding="utf-8"))
        monorepo = root / "monorepo"; monorepo.mkdir()
        nested_source = monorepo / "loop-hybrid"; nested_source.mkdir()
        git("init", "-q", str(monorepo))
        git("-C", str(monorepo), "config", "user.email", "canary@example.invalid")
        git("-C", str(monorepo), "config", "user.name", "Canary")
        (nested_source / "baseline.txt").write_text("nested baseline\n", encoding="utf-8")
        git("-C", str(monorepo), "add", "loop-hybrid/baseline.txt")
        git("-C", str(monorepo), "commit", "-qm", "nested baseline")
        nested_base = subprocess.run(["git", "-C", str(monorepo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        nested_seen: dict[str, str] = {}

        def nested_model(workspace: Path, capsule: dict) -> dict:
            nested_seen["top_level"] = subprocess.run(
                ["git", "-C", str(workspace), "rev-parse", "--show-toplevel"],
                check=True, capture_output=True, text=True,
            ).stdout.strip()
            nested_seen["prefix"] = subprocess.run(
                ["git", "-C", str(workspace), "rev-parse", "--show-prefix"],
                check=True, capture_output=True, text=True,
            ).stdout
            (workspace / "nested-change.txt").write_text(
                f"attempt {capsule['attempt']}\n", encoding="utf-8"
            )
            return {"summary": "added a monorepo subtree fixture change"}

        nested_store = RunStore(root / "nested-run-store", command_runner=fixture_command_runner)
        nested_controller = LoopController(nested_store, root / "nested-workspaces")
        nested_run = make_native_run(
            nested_store, nested_source, nested_base, "native-mvp-nested", "runtime",
            [{
                "id": "nested-runtime-check",
                "commands": [{"id": "diff-check", "argv": ["git", "diff", "--check"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}],
                "required_receipts": ["executor"],
            }],
            ["sh", "-c", "test -f nested-change.txt"], ["nested-change.txt"], 4,
            goal={"feature_contract": "add a nested fixture change"},
        )["run_id"]
        nested_done = nested_controller.tick(
            nested_run,
            holder="nested-controller",
            model=nested_model,
            verifier_argv=["sh", "-c", "test -f nested-change.txt"],
        )
        nested_receipt = json.loads(
            (nested_store.root / nested_done["receipt_ref"]).read_text(encoding="utf-8")
        )
        nested_diff = (
            nested_store.root / nested_receipt["diff"]["ref"]
        ).read_text(encoding="utf-8")
        nested_workspace = root / "nested-workspaces" / nested_run / "1"
        nested_source_clean = subprocess.run(
            ["git", "-C", str(monorepo), "status", "--porcelain"],
            capture_output=True, text=True,
        ).stdout == ""
        interrupted_run = make_native_run(
            store, source, base, "native-mvp-interrupted", "runtime", checks, verifier,
            ["agent-change.txt", "failed-attempt.txt"], 4, goal=goal,
        )["run_id"]
        store.begin_attempt(interrupted_run, "workspace://interrupted/1")
        recovered = controller.tick(interrupted_run, holder="controller", model=model_that_changes, verifier_argv=["sh", "-c", "! git diff --cached --quiet"])
        receipt_run = make_native_run(
            store, source, base, "native-mvp-receipt", "runtime", checks, verifier,
            ["agent-change.txt", "failed-attempt.txt"], 4, goal=goal,
        )["run_id"]
        receipt_attempt = store.begin_attempt(receipt_run, "workspace://receipt/1")
        receipt_body = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": receipt_run, "attempt": receipt_attempt, "verification": {"exit_code": 0}}
        store.write_artifact(receipt_run, receipt_attempt, "receipt.json", json.dumps(receipt_body, sort_keys=True))
        reconciled = controller.startup()
        retry_run = make_native_run(
            store, source, base, "native-mvp-retry", "runtime", checks, verifier,
            ["agent-change.txt", "failed-attempt.txt"], 4, goal=goal,
        )["run_id"]
        retries = [controller.tick(retry_run, holder="controller", model=model_that_retries, verifier_argv=[sys.executable, "-c", "raise SystemExit(1)"]) for _ in range(4)]
        source_clean = subprocess.run(["git", "-C", str(source), "status", "--porcelain"], capture_output=True, text=True).stdout == ""
        events = [event["event_type"] for event in store.events(successful_run)]
        cases = [
            case("lease-excludes-second-controller", lease_held and busy.get("status") == "lease_busy", busy.get("status", "")),
            case("model-runs-in-disposable-clone", done.get("status") == "verified" and source_clean, done.get("status", "")),
            case(
                "monorepo-subtree-attempt-is-verified",
                nested_done.get("status") == "verified",
                json.dumps({"result": nested_done, "verification": nested_receipt.get("verification")}, sort_keys=True),
            ),
            case("monorepo-subtree-runs-from-target-cwd", nested_seen.get("top_level") == str(nested_workspace.resolve()) and nested_seen.get("prefix") == "loop-hybrid/\n", json.dumps(nested_seen)),
            case("monorepo-subtree-diff-is-target-relative", "a/nested-change.txt b/nested-change.txt" in nested_diff and nested_source_clean, nested_diff),
            case("monorepo-clone-root-is-disposed", not nested_workspace.exists(), str(nested_workspace)),
            case("receipt-keeps-outputs-by-reference", "stdout" not in receipt and receipt["verification"]["stdout"]["ref"].endswith("verifier.stdout"), done["receipt_ref"]),
            case("controller-records-durable-events", events == ["run_created", "attempt_started", "attempt_finished"], ",".join(events)),
            case("expired-attempt-recovers-to-next-tick", recovered.get("status") == "verified" and store.get_run(interrupted_run)["attempts"] == 2 and "attempt_reconciled" in [row["event_type"] for row in store.events(interrupted_run)], recovered.get("status", "")),
            case(
                "startup-replays-durable-final-without-redispatch",
                done.get("status") == "verified"
                and verified_restart == []
                and store.get_run(successful_run)["state"] == "verified"
                and store.get_run(successful_run)["attempts"] == 1,
                json.dumps({"done": done.get("status"), "startup": verified_restart, "run": store.get_run(successful_run)["state"]}),
            ),
        case(
            "startup-reconciler-rejects-bare-receipt-with-delivery-binding",
            reconciled == [{"run_id": receipt_run, "attempt": 1, "status": "human_required", "recovered_from": "receipt", "reason": "delivery_terminal_commit_missing"}]
            and store.get_run(receipt_run)["state"] == "human_required",
            str(reconciled),
        ),
            case("failed-verifier-retries-until-fourth-attempt", [row["status"] for row in retries] == ["retry_pending", "retry_pending", "retry_pending", "stopped"], str([row["status"] for row in retries])),
            case("workspaces-are-disposed-not-source-reset", not any((root / "workspaces").rglob("agent-change.txt")), "workspace artifacts only"),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-native-runtime-mvp", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["The model runner is injected for this provider-free canary; provider and external-service adapters remain separate LH ports."]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

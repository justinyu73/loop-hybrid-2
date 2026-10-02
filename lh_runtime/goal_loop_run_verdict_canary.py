#!/usr/bin/env python3
"""Production-entry proof for restart poll-resume before a not_holder exit."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
import external_action_port as eap
from controller import LoopController
from _fixture import make_campaign, make_source_repo
from external_verdict import VerdictStore
from goal_store import GoalStore
from native_delivery_fixture import make_native_run
from run_store import RunStore
import goal_loop_run as fixture_glr
from p7_native_runstore_fixture import explicit_runstore_factory
from p7_fence_fixture import fixture_command_runner


def _campaign() -> dict:
    return make_campaign("campaign-a3")


def _fake_factory(*, timeout_seconds: float = 900):
    def model(_workspace: Path, _capsule: dict) -> dict:
        return {"summary": f"unused fake executor ({timeout_seconds})"}
    return model


class _SeedActionAdapter:
    def perform(self, op_key: str, _request: dict[str, object]) -> dict[str, str]:
        return {"operation_key": op_key, "external_id": "a3-action", "head_sha": "a3-head"}


def _seed(root: Path, source: Path, base: str) -> int:
    runs = RunStore(root / "runs", command_runner=fixture_command_runner)
    goals = GoalStore(root / "goals")
    bundle = make_native_run(
        runs, source, base, "a3-parked", "restart-verdict",
        [{"id": "a3-source-check", "commands": [{"id": "source-file", "argv": ["test", "-s", "src/a3.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]},
         {"id": "a3-final-check", "final_only": True, "commands": [{"id": "final-file", "argv": ["test", "-s", "src/a3.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
        ["test", "-s", "src/a3.txt"], ["src/"], 4, phase="async",
        goal={"goal_id": "a3-parked", "campaign_id": "campaign-a3", "stage_id": "restart", "feature_contract": "restart verdict fixture"},
        run_id="run-a3-parked",
    )
    goal = runs.get_run("run-a3-parked")["goal"]
    event = goals.record_event(event_id="a3-goal-event", idempotency_key="a3-goal-event", source="a3-canary", event_type="goal_candidate", payload={"goal_id": "a3-parked"})
    goals.create_candidate(event["event_key"], goal_id="a3-parked", campaign_id="campaign-a3", stage_id="restart", goal=goal, revision=bundle["contract"]["goal"]["revision"])
    goals.activate_with_run("a3-parked", "run-a3-parked", event_key=event["event_key"])
    verdicts = VerdictStore(root / "verdict.sqlite3")
    controller = LoopController(runs, root / "workspaces")
    ledger = eap.ActionLedger(root / "runs" / "action-ledger.sqlite3")

    def model(workspace: Path, _capsule: dict[str, object]) -> dict[str, object]:
        target = workspace / "src" / "a3.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("a3 candidate\n", encoding="utf-8")
        return {"summary": "restart verdict fixture", "usage": {"state": "unknown"}}

    parked = controller.tick_async(
        "run-a3-parked", holder="a3-seed", model=model,
        verdict_store=verdicts, action_ledger=ledger,
        adapter=_SeedActionAdapter(), action_id="open-pr",
    )
    if parked.get("status") != "awaiting_external_verdict":
        raise AssertionError({"parked": parked, "run": runs.get_run("run-a3-parked")})
    return 0


def _resume(root: Path, source: Path, base: str) -> int:
    from goal_loop_run import run
    verdicts = VerdictStore(root / "verdict.sqlite3")

    def not_holder_driver(_worker, **_kwargs):
        return {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}

    with explicit_runstore_factory(fixture_glr):
        result = run(
                executor="fake",
                execute=True,
                goal_store_root=root / "goals",
                run_store_root=root / "runs",
                workspace_root=root / "workspaces",
                campaign=_campaign(),
                source_repo=source,
                base_revision=base,
                executor_timeout_seconds=0.25,
                verdict_store=verdicts,
                conclusion_source=lambda _op_key: {"conclusion": "success"},
                factory_overrides={"fake": _fake_factory},
                driver_fn=not_holder_driver,
            )
    (root / "result.json").write_text(json.dumps(result, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        mode, root, source, base = sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5]
        return _seed(root, source, base) if mode == "seed" else _resume(root, source, base)

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        seed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", "seed", str(root), str(source), base], capture_output=True, text=True)
        resume = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", "resume", str(root), str(source), base], capture_output=True, text=True)
        result = json.loads((root / "result.json").read_text(encoding="utf-8")) if (root / "result.json").exists() else {}
        runs = RunStore(root / "runs")
        verdicts = VerdictStore(root / "verdict.sqlite3")
        run_id = "run-a3-parked"
        final_run = runs.get_run(run_id)
        verdict_state = verdicts.state(run_id)
        cases = [
            {
                "id": "production-entry-polls-before-not-holder-exit",
                "ok": seed.returncode == 0 and resume.returncode == 0
                and len(result.get("startup_external_resumed", [])) == 1
                and result["startup_external_resumed"][0].get("run_id") == run_id
                and result["startup_external_resumed"][0].get("conclusion") == "success"
                and result["startup_external_resumed"][0].get("state") == "verified"
                and result["startup_external_resumed"][0].get("normalized", {}).get("status") in {"ready", "already_normalized"}
                and result["startup_external_resumed"][0].get("delivery", {}).get("verdict") == "GREEN"
                and final_run["state"] == "verified" and verdict_state == {"state": "verified", "conclusion": "success"},
                "detail": {"seed_exit": seed.returncode, "resume_exit": resume.returncode, "startup_external_resumed": result.get("startup_external_resumed"), "run_state": final_run["state"], "verdict_state": verdict_state},
            },
            {
                "id": "not-holder-is-retryable-host-outcome",
                "ok": result.get("driver", {}).get("stop_reason") == "not_holder" and final_run["state"] != "stopped",
                "detail": {"driver": result.get("driver"), "run_state": final_run["state"]},
            },
        ]
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "lh-production-verdict-resume", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "cases": cases}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

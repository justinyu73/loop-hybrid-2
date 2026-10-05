"""Offline B12 live-smoke harness for the complete durable LH spine."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import token_cost
from campaign_compiler import CampaignCompiler
from command_ingress import submit_command
from goal_loop_run import run as goal_loop_run
from goal_store import GoalStore
from project_status import build_status
from native_delivery_fixture import make_native_run
from run_store import RunStore
from contextlib import nullcontext
import goal_loop_run as fixture_glr
from p7_native_runstore_fixture import explicit_runstore_factory


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def _campaign() -> dict[str, Any]:
    def stage(stage_id: str, feature: str, next_stage_id: str | None) -> dict[str, Any]:
        filename = "hello.txt" if stage_id == "s1" else "world.txt"
        word = "hello" if stage_id == "s1" else "world"
        return {
            "stage_id": stage_id,
            "goal": {"feature_contract": feature},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {
                "id": f"{stage_id}-lamp",
                "smoke": f"test -f src/{filename}",
                "verification_argv": ["sh", "-c", f"test -f src/{filename} && grep -qx {word} src/{filename}"],
            },
            "max_attempts": 2,
            "next_stage_id": next_stage_id,
        }

    return {
        "schema": "lh-campaign/v1",
        "campaign_id": "b12-live-smoke",
        "stages": [
            stage("s1", "create src/hello.txt containing exactly hello", "s2"),
            stage("s2", "create src/world.txt containing exactly world", None),
        ],
    }


def _bind_campaign_delivery(root: Path, campaign: dict[str, Any], source: Path, base: str) -> None:
    """Attach producer-owned sealed bindings to each campaign stage.

    The helper only prepares Goal revision input; it does not start an
    Attempt, write a receipt, or claim a terminal state.  Stage completion
    therefore carries an explicit contract for the next producer as well.
    """
    bundles = RunStore(root / "campaign-delivery-bundles")
    for stage in campaign["stages"]:
        stage_id = stage["stage_id"]
        filename = "hello.txt" if stage_id == "s1" else "world.txt"
        word = "hello" if stage_id == "s1" else "world"
        goal_id = f"{campaign['campaign_id']}:{stage_id}"
        native = make_native_run(
            bundles,
            source,
            base,
            goal_id,
            stage_id,
            [{
                "id": f"{stage_id}-delivery-check",
                "commands": [{
                    "id": "diff-check",
                    "argv": ["git", "diff", "--check"],
                    "cwd": "${WORKTREE}",
                    "expect_exit": 0,
                    "timeout_seconds": 10,
                }],
                "required_receipts": ["executor"],
            }],
            ["sh", "-c", f"grep -qx {word} src/{filename}"],
            stage["allowed_paths"],
            stage["max_attempts"],
            goal={"feature_contract": stage["goal"]},
            run_id=f"bundle-{stage_id}",
        )
        persisted = bundles.get_run(native["run_id"])["goal"]
        stage["goal"] = {
            "feature_contract": stage["goal"],
            "delivery_contract": native["contract"],
            "delivery_plan": native["plan"],
            "delivery_packet": persisted["delivery_packet"],
        }


def _prepare(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    source = root / "source"
    source.mkdir(parents=True, exist_ok=True)
    _git("init", "-q", str(source))
    _git("-C", str(source), "config", "user.email", "b12@example.invalid")
    _git("-C", str(source), "config", "user.name", "B12 Live Smoke")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    _git("-C", str(source), "add", "baseline.txt")
    _git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    campaign = _campaign()
    _bind_campaign_delivery(root, campaign, source, base)
    campaign_path = root / "campaign.json"
    campaign_path.write_text(json.dumps(campaign, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    goals = GoalStore(root / "goals")
    compiled = CampaignCompiler(campaign).compile()
    envelope = compiled["stages"]["s1"]
    candidate = {
        "goal_id": "b12-live-smoke:s1",
        "campaign_id": campaign["campaign_id"],
        "stage_id": "s1",
        "goal": {**campaign["stages"][0]["goal"], "admission_envelope": envelope},
    }
    seeded = submit_command(
        goals,
        source="scheduler",
        event_type="manual_intent",
        event_id="b12-command-1",
        idempotency_key="b12-command-1",
        payload={"campaign_id": campaign["campaign_id"], "stage_id": "s1", "candidate": candidate},
    )
    replay = submit_command(
        goals,
        source="scheduler",
        event_type="manual_intent",
        event_id="b12-command-1",
        idempotency_key="b12-command-1",
        payload={"campaign_id": campaign["campaign_id"], "stage_id": "s1", "candidate": candidate},
    )
    return {
        "source": source,
        "base": base,
        "campaign": campaign,
        "campaign_path": campaign_path,
        "seeded": seeded,
        "replay": replay,
    }


def _fake_factory(*, timeout_seconds: float = 900):
    del timeout_seconds

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        stage_id = capsule.get("goal", {}).get("stage_id")
        if stage_id == "s1":
            target, content = workspace / "src" / "hello.txt", "hello\n"
        elif stage_id == "s2":
            target, content = workspace / "src" / "world.txt", "world\n"
        else:
            raise AssertionError(f"unexpected stage in offline model: {stage_id!r}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {
            "summary": f"offline B12 model completed {stage_id}",
            "usage": token_cost.measured_usage(model="offline-b12", input_tokens=2, output_tokens=1),
        }

    return model


def _declarations(root: Path) -> dict[str, Any] | None:
    """The executor declarations an execute-mode harness wrote for its sessions."""
    path = root / "executors.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def _session(root: Path, *, phase: str, executor: str, offline: bool, hold_after: bool) -> int:
    campaign = json.loads((root / "campaign.json").read_text(encoding="utf-8"))
    factory_overrides = {"fake": _fake_factory} if offline else None
    selected_executor = "fake" if offline else executor
    try:
        with explicit_runstore_factory(fixture_glr) if offline else nullcontext():
            result = goal_loop_run(
                executor=selected_executor,
                execute=True,
                executor_declarations=None if offline else _declarations(root),
                goal_store_root=root / "goals",
                run_store_root=root / "runs",
                workspace_root=root / "workspaces",
                campaign=campaign,
                source_repo=root / "source",
                base_revision=(root / "base.txt").read_text(encoding="utf-8").strip(),
                pause_flag=root / "loop-pause-all",
                max_cycles=1 if phase == "first" else 16,
                max_runtime_seconds=900,
                executor_timeout_seconds=120,
                factory_overrides=factory_overrides,
                sleep_fn=(lambda _seconds: None) if offline else None,
            )
    except Exception as exc:
        result = {"error": f"{type(exc).__name__}: {exc}"}
        (root / f"{phase}-result.json").write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        return 1
    (root / f"{phase}-result.json").write_text(json.dumps(result, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    (root / f"{phase}-complete").write_text("bounded session complete\n", encoding="utf-8")
    if hold_after:
        while True:
            time.sleep(1.0)
    return 0


def _run_restart_sessions(root: Path, *, executor: str, offline: bool) -> dict[str, Any]:
    script = str(Path(__file__).resolve())
    command = [sys.executable, "-B", script, "--child-session", str(root)]
    first = subprocess.Popen(command + ["first", executor, "offline" if offline else "execute", "--hold-after"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    complete = root / "first-complete"
    deadline = time.monotonic() + 30.0
    while not complete.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if first.poll() is None:
        first.kill()
    first_exit = first.wait(timeout=10.0)
    second = subprocess.run(command + ["second", executor, "offline" if offline else "execute"], capture_output=True, text=True, check=False)
    first_result = json.loads((root / "first-result.json").read_text(encoding="utf-8")) if (root / "first-result.json").exists() else {}
    second_result = json.loads((root / "second-result.json").read_text(encoding="utf-8")) if (root / "second-result.json").exists() else {}
    return {
        "first_exit": first_exit,
        "first_killed_after_bounded_session": complete.exists() and first_exit < 0,
        "first": first_result,
        "second_exit": second.returncode,
        "second": second_result,
        "second_stderr": second.stderr[-1000:],
    }


def _attempt_rows(store: RunStore) -> list[dict[str, Any]]:
    with store._connect() as conn:
        rows = conn.execute("SELECT run_id, ordinal, state, receipt_ref FROM attempts ORDER BY run_id, ordinal").fetchall()
    return [dict(row) for row in rows]


def _event_rows(store: GoalStore) -> list[dict[str, Any]]:
    with store._connect() as conn:
        rows = conn.execute("SELECT event_key, event_type, state, goal_id FROM goal_events ORDER BY created_at, event_key").fetchall()
    return [dict(row) for row in rows]


def _receipt(store: RunStore, run_id: str, usage: dict[str, Any]) -> None:
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", "")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1",
        "run_id": run_id,
        "attempt": ordinal,
        "usage": usage,
        "diff": diff_ref,
        "verification": {"argv": ["git", "diff", "--check"], "exit_code": 1},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    if not store.finish_attempt(
        run_id,
        ordinal,
        state="human_required",
        receipt_ref=ref["ref"],
        receipt_digest=ref["digest"],
        fence=store.attempt_fence(run_id, ordinal),
    ):
        raise AssertionError("budget fixture receipt was not durably parked")


def _budget_case(root: Path, source: Path, base: str, campaign: dict[str, Any], *, unknown: bool, executor: str, offline: bool) -> dict[str, Any]:
    label = "unknown" if unknown else "exhausted"
    run_root = root / f"budget-{label}"
    goals = GoalStore(run_root / "goals")
    store = RunStore(run_root / "runs")
    run_id = f"budget-{label}"
    make_native_run(
        store,
        source,
        base,
        run_id,
        "budget",
        [{
            "id": "budget-delivery-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["git", "rev-parse", "HEAD"],
        ["src/"],
        4,
        goal={"goal_id": run_id, "feature_contract": "budget accounting fixture"},
        run_id=run_id,
    )
    usage = token_cost.unknown_usage(model="b12") if unknown else token_cost.measured_usage(model="offline-b12", input_tokens=1, output_tokens=1)
    _receipt(store, run_id, usage)
    calls = {"count": 0}

    def counting_factory(*, timeout_seconds: float = 900):
        model = _fake_factory(timeout_seconds=timeout_seconds)

        def counted(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
            calls["count"] += 1
            return model(workspace, capsule)

        return counted

    overrides = {"fake": counting_factory} if offline else None
    selected_executor = "fake" if offline else executor
    result = goal_loop_run(
        executor=selected_executor,
        execute=True,
        executor_declarations=None if offline else _declarations(root),
        goal_store_root=goals.root,
        run_store_root=store.root,
        workspace_root=run_root / "workspaces",
        campaign=campaign,
        source_repo=source,
        base_revision=base,
        budget_ceiling_tokens=2,
        budget_scope=f"b12/{label}",
        max_cycles=2,
        factory_overrides=overrides,
        sleep_fn=lambda _seconds: None,
    )
    expected = "budget_unknown" if unknown else "budget_exhausted"
    driver = result.get("driver", {})
    return {
        "stop_reason": driver.get("stop_reason"),
        "cycles": driver.get("cycles"),
        "runs_dispatched": driver.get("runs_dispatched"),
        "expected": expected,
        "provider_calls": calls["count"],
        "usage_records": store.usage_records(),
        "ok": driver.get("stop_reason") == expected and driver.get("cycles") == 0 and driver.get("runs_dispatched") == 0 and calls["count"] == 0,
    }


def _run_existing_canary(name: str) -> dict[str, Any]:
    completed = subprocess.run([sys.executable, "-B", str(HERE / name)], capture_output=True, text=True, check=False)
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        payload = {"stdout_tail": completed.stdout[-1000:], "stderr_tail": completed.stderr[-1000:]}
    return {"exit": completed.returncode, "result": payload, "ok": completed.returncode == 0}


def _harness(root: Path, *, execute: bool, executor: str | None,
             declarations: dict[str, Any] | None = None) -> dict[str, Any]:
    prepared = _prepare(root)
    (root / "base.txt").write_text(prepared["base"], encoding="utf-8")
    if execute:
        (root / "executors.json").write_text(json.dumps(declarations), encoding="utf-8")
    executor = executor if execute else "fake"
    dry_plan = goal_loop_run(
        executor=executor,
        execute=False,
        executor_declarations=declarations if execute else None,
        factory_overrides=None if execute else {"fake": _fake_factory},
        goal_store_root=root / "goals",
        run_store_root=root / "runs",
        workspace_root=root / "workspaces",
        campaign=prepared["campaign"],
        source_repo=prepared["source"],
        base_revision=prepared["base"],
        max_cycles=1,
        max_runtime_seconds=900,
    )
    restart = _run_restart_sessions(root, executor=executor, offline=not execute)
    runs = RunStore(root / "runs")
    goals = GoalStore(root / "goals")
    attempts = _attempt_rows(runs)
    events = _event_rows(goals)
    receipt_paths = [str(runs.root / row["receipt_ref"]) for row in attempts if row.get("receipt_ref")]
    final_report = {
        "schema": "lh-b12-live-smoke-report/v1",
        "mode": "execute" if execute else "dry-run",
        "executor": executor,
        "base_revision": prepared["base"],
        "command_event": {"seeded": prepared["seeded"], "replay": prepared["replay"]},
        "restart": restart,
        "durable": {
            "attempts": attempts,
            "receipt_count": len(receipt_paths),
            "receipt_paths": receipt_paths,
            "events": events,
            "status": build_status(runs, goals),
        },
    }
    verified_runs = runs.summary().get("runs_by_state", {}).get("verified", 0)
    stage_events = [row for row in events if row["event_type"] == "verified_stage"]
    completed_goals = goals.summary().get("goals_by_state", {}).get("completed", 0)
    budget_exhausted = _budget_case(root, prepared["source"], prepared["base"], prepared["campaign"], unknown=False, executor=executor, offline=not execute)
    budget_unknown = _budget_case(root, prepared["source"], prepared["base"], prepared["campaign"], unknown=True, executor=executor, offline=not execute)
    supervisor = _run_existing_canary("supervisor_canary.py")
    fencing = _run_existing_canary("attempt_fencing_canary.py")
    assertions = {
        "dry_run_plan_has_no_invocation": dry_plan.get("mode") == "dry_run" and dry_plan.get("invoked") is False,
        "command_ingress_received_and_replayed": prepared["seeded"].get("status") == "received" and prepared["replay"].get("status") == "reused",
        "first_bounded_session_killed_after_completion": restart["first_killed_after_bounded_session"],
        "restart_second_session_completed": restart["second_exit"] == 0 and restart["second"].get("driver", {}).get("runs_dispatched", 0) >= 1,
        "multi_stage_next_transition": bool(stage_events) and completed_goals == 2,
        "durable_receipts_and_no_duplicate_attempts": len(receipt_paths) == 2 and verified_runs == 2 and all(row["ordinal"] == 1 for row in attempts),
        "budget_exhausted": budget_exhausted["ok"],
        "budget_unknown": budget_unknown["ok"],
        "supervisor_singleton_and_scheduled_tick": supervisor["ok"],
        "fencing_regression": fencing["ok"],
        "final_report_written": (root / "final-report.json").exists(),
    }
    final_report["budget"] = {"exhausted": budget_exhausted, "unknown": budget_unknown}
    final_report["supervisor"] = supervisor["result"]
    final_report["fencing"] = fencing["result"]
    final_report["assertions"] = assertions
    final_report_path = root / "final-report.json"
    final_report_path.write_text(json.dumps(final_report, ensure_ascii=False, indent=2), encoding="utf-8")
    assertions["final_report_written"] = final_report_path.exists()
    final_report["assertions"] = assertions
    final_report_path.write_text(json.dumps(final_report, ensure_ascii=False, indent=2), encoding="utf-8")
    report = {
        "check_id": "lh-b12-live-smoke",
        "status": "pass" if all(assertions.values()) else "fail",
        "mode": "execute" if execute else "dry-run",
        "executor": executor,
        "provider_invocations": "human-execute" if execute else 0,
        "blocking_failures": [key for key, ok in assertions.items() if not ok],
        "assertions": assertions,
        "durable": final_report["durable"],
        "restart": restart,
        "budget": {"exhausted": budget_exhausted, "unknown": budget_unknown},
        "supervisor": supervisor["result"],
        "fencing": fencing["result"],
        "final_report": str(root / "final-report.json"),
    }
    (root / "b12-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run B12's provider-free comprehensive LH live-smoke harness")
    parser.add_argument("--execute", action="store_true", help="use a real declared executor; human-only")
    parser.add_argument("--dry-run", action="store_true", help="explicitly select the provider-free harness mode")
    parser.add_argument("--executor", default=None, help="declared executor name for --execute")
    parser.add_argument("--executors", default=None, help="executor declaration file for --execute")
    parser.add_argument("--work-root", default=None)
    args = parser.parse_args(argv)
    if args.execute and args.dry_run:
        parser.error("choose either --execute or --dry-run")
    declarations = None
    if args.execute:
        if not args.executor or not args.executors:
            parser.error("--execute requires --executor and --executors")
        import cli_agent_executor as executors
        declarations = executors.validate_executor_declarations(
            json.loads(Path(args.executors).read_text(encoding="utf-8")))

    if args.work_root:
        root = Path(args.work_root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        report = _harness(root, execute=args.execute, executor=args.executor, declarations=declarations)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "pass" else 1

    with tempfile.TemporaryDirectory(prefix="lh-b12-live-smoke-") as raw:
        report = _harness(Path(raw), execute=args.execute, executor=args.executor, declarations=declarations)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    if len(sys.argv) >= 6 and sys.argv[1] == "--child-session":
        child_root = Path(sys.argv[2])
        phase = sys.argv[3]
        child_executor = sys.argv[4]
        child_offline = sys.argv[5] == "offline"
        raise SystemExit(_session(child_root, phase=phase, executor=child_executor, offline=child_offline, hold_after="--hold-after" in sys.argv[6:]))
    raise SystemExit(main())

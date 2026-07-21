#!/usr/bin/env python3
"""W9d live smoke gate: one bounded goal through the production run() entry.

Two modes:

- default (committed in verify.sh): fully offline. The same fixture chain is
  driven through ``goal_loop_run.run()`` with an injected fake executor —
  command event -> admission -> dispatch -> executor -> lamp + value gate ->
  completed. No provider binary is touched, not even ``--version``.
- ``--live`` (opt-in, never in verify.sh): the same chain with a REAL coding
  CLI (default codex, ``--executor`` to switch). Preflight-skips (exit 0,
  status "skip") when the CLI is missing or unauthenticated — a live smoke
  must never go red on an absent provider. When it runs, it asserts the run
  verified through the real model path (lamp is red-on-base, so no precheck),
  the goal completed, and the billed usage is a sane measured delta (W7
  phantom guard: total tokens well under 2M, cost under $1).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import goal_loop_run as glr
import token_cost
from _fixture import make_source_repo
from campaign_compiler import CAMPAIGN_SCHEMA
from cli_agent_executor import resolve_cli
from goal_store import GoalStore
from run_store import RunStore
from value_reducer import verdict_for_run

CAMPAIGN_ID = "campaign-w9d"
STAGE_ID = "stage-live"
GOAL_ID = f"{CAMPAIGN_ID}:{STAGE_ID}"
MARKER_LINE = "lh-live-ok"
MAX_LIVE_TOTAL_TOKENS = 2_000_000
MAX_LIVE_COST_USD = 1.0


def _campaign() -> dict:
    """One stage, red-on-base lamp the model can satisfy in a single attempt."""
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "stages": [{
            "stage_id": STAGE_ID,
            "goal": {"feature_contract": f"create src/live-marker.txt whose only line is exactly {MARKER_LINE}"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {
                "id": "live-lamp",
                "smoke": "src/live-marker.txt carries the exact marker line",
                "verification_argv": ["sh", "-c", f"grep -qx '{MARKER_LINE}' src/live-marker.txt"],
            },
            "max_attempts": 1,
            "next_stage_id": None,
        }],
    }


def _seed_goal(goal_store: GoalStore) -> None:
    from campaign_compiler import CampaignCompiler
    envelope = CampaignCompiler(_campaign()).compile()["stages"][STAGE_ID]
    goal_store.record_event(event_id="w9d-seed", idempotency_key="w9d-seed", source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                      "goal": {"feature_contract": STAGE_ID, "admission_envelope": envelope}}
    })


def _only_run(run_store: RunStore) -> dict[str, Any]:
    runs = run_store.runnable_runs() or run_store.terminal_runs()
    return run_store.get_run(runs[0]["run_id"])


def _receipt(run_store: RunStore, run_id: str) -> dict[str, Any]:
    meta = run_store.latest_receipt(run_id)
    return json.loads((run_store.root / meta["receipt_ref"]).read_text(encoding="utf-8"))


def _drive(root: Path, source: Path, base: str, *, executor: str, factory_overrides=None) -> dict[str, Any]:
    goal_store = GoalStore(root / "goals")
    _seed_goal(goal_store)
    return glr.run(
        executor=executor,
        execute=True,
        goal_store_root=root / "goals",
        run_store_root=root / "runs",
        workspace_root=root / "workspaces",
        campaign=_campaign(),
        source_repo=source,
        base_revision=base,
        max_cycles=8,
        idle_limit=1,
        sleep_fn=lambda _seconds: None,
        factory_overrides=factory_overrides,
    )


def _fake_factory(calls: list[dict]):
    def factory(*, timeout_seconds: float = 900):
        def model(workspace: Path, _capsule: dict) -> dict:
            calls.append({"timeout_seconds": timeout_seconds})
            src = workspace / "src"
            src.mkdir(exist_ok=True)
            (src / "live-marker.txt").write_text(MARKER_LINE + "\n", encoding="utf-8")
            return {"summary": "w9d offline fixture executor"}
        return model
    return factory


def _dry() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        calls: list[dict] = []
        result = _drive(root, source, base, executor="fake", factory_overrides={"fake": _fake_factory(calls)})
        run_store = RunStore(root / "runs")
        run = _only_run(run_store)
        receipt = _receipt(run_store, run["run_id"])
        verification = receipt["verification"]
        goal_state = GoalStore(root / "goals").get_goal(GOAL_ID)["state"]
        verdict = verdict_for_run(run_store, run["run_id"])
        cases = [
            {"id": "production-entry-executes-the-chain",
             "ok": result.get("mode") == "execute" and result.get("invoked") is True,
             "detail": json.dumps({"mode": result.get("mode")})},
            {"id": "run-verified-through-the-model-path",
             "ok": run["state"] == "verified" and run["attempts"] == 1
             and verification.get("exit_code") == 0 and "precheck" not in verification,
             "detail": json.dumps({"state": run["state"], "attempts": run["attempts"],
                                   "exit_code": verification.get("exit_code")})},
            {"id": "goal-completed",
             "ok": goal_state == "completed",
             "detail": json.dumps({"goal": goal_state})},
            {"id": "receipt-passes-evidence-integrity",
             "ok": verdict["verdict"] == "GREEN",
             "detail": json.dumps({"verdict": verdict["verdict"], "reasons": verdict["reasons"]})},
            {"id": "exactly-one-executor-call",
             "ok": len(calls) == 1,
             "detail": json.dumps({"calls": len(calls)})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-live-smoke",
        "mode": "dry",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/live_smoke_canary.py",
                         "fixtures": "injected fake executor; no provider binary touched"},
        "known_gaps_open": [
            "the real-CLI chain runs only via --live (opt-in, human-triggered); it skips cleanly when no provider is available",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _preflight(executor: str) -> str | None:
    """Cheap conservative probe; a skip reason, or None when the CLI looks usable."""
    try:
        binary = resolve_cli(executor)
    except (FileNotFoundError, ValueError) as exc:
        return f"executor CLI unavailable: {exc}"
    try:
        version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"executor CLI probe failed: {exc}"
    if version.returncode != 0:
        return f"executor CLI --version exited {version.returncode}"
    if executor == "codex":
        try:
            login = subprocess.run([binary, "login", "status"], capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return f"codex auth probe failed: {exc}"
        if login.returncode != 0:
            return "codex is not authenticated"
    return None


def _live(executor: str) -> int:
    reason = _preflight(executor)
    if reason is not None:
        print(json.dumps({"check_id": "lh-live-smoke", "mode": "live", "status": "skip", "reason": reason}, ensure_ascii=False, indent=2))
        return 0
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        result = _drive(root, source, base, executor=executor)
        run_store = RunStore(root / "runs")
        run = _only_run(run_store)
        receipt = _receipt(run_store, run["run_id"])
        verification = receipt["verification"]
        usage = receipt.get("usage") if isinstance(receipt.get("usage"), dict) else {}
        total_tokens = int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0)) + int(usage.get("cache_read_tokens", 0))
        cost = token_cost.compute_cost(usage)
        goal_state = GoalStore(root / "goals").get_goal(GOAL_ID)["state"]
        receipt_summary = {
            "run_id": run["run_id"],
            "attempt": receipt.get("attempt"),
            "exit_code": verification.get("exit_code"),
            "usage": usage,
            "cost": cost,
        }
        checks = {
            "run_verified_real_model_path": run["state"] == "verified" and verification.get("exit_code") == 0 and "precheck" not in verification,
            "goal_completed": goal_state == "completed",
            "usage_is_measured": usage.get("state") == "measured",
            "usage_is_a_sane_delta": usage.get("state") == "measured" and total_tokens < MAX_LIVE_TOTAL_TOKENS,
            "cost_under_one_usd": cost.get("state") == "measured" and float(cost.get("cost_usd", 0.0)) < MAX_LIVE_COST_USD,
        }
        failures = [{"id": name} for name, ok in checks.items() if not ok]
        print(json.dumps({
            "check_id": "lh-live-smoke",
            "mode": "live",
            "executor": executor,
            "status": "pass" if not failures else "fail",
            "blocking_failures": failures,
            "checks": checks,
            "receipt": receipt_summary,
            "driver": {"stop_reason": result.get("driver", {}).get("stop_reason"), "runs_dispatched": result.get("driver", {}).get("runs_dispatched")},
        }, ensure_ascii=False, indent=2))
        return 0 if not failures else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="W9d live smoke gate (dry by default; --live uses a real coding CLI)")
    parser.add_argument("--live", action="store_true", help="run the chain with a real coding CLI (never in verify.sh)")
    parser.add_argument("--executor", default="codex", choices=sorted(glr.EXECUTORS), help="live-mode executor CLI")
    args = parser.parse_args(argv)
    return _live(args.executor) if args.live else _dry()


if __name__ == "__main__":
    raise SystemExit(main())

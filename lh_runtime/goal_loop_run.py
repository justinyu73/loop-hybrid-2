#!/usr/bin/env python3
"""Autonomous driver runner: wire a real coding-agent executor into the driver.

This is the opt-in production entry for full-auto. It is model-agnostic — the
executor is chosen by name from a registry (codex / claude / any CLI preset in
cli_agent_executor), never hardcoded — and gated: dry-run is the default and
prints the resolved plan without invoking any provider; only ``--execute``
constructs the real executor and runs the loop. The executor itself runs inside
the controller's disposable clone (the isolation IS the sandbox); output stops
at a PR — push, merge, and promotion remain human/project-owned.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Mapping

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import cli_agent_executor as executors
import external_verdict as ev
import github_conclusion_source as ghc
import grill_loop
import project_binding
import turning_point as tp
from campaign_compiler import CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker, ModelRunner, TurningPointRunner
from goal_store import GoalStore
from run_store import RunStore
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

# Model-agnostic executor registry. Add a CLI preset here, not a hardcoded model.
EXECUTORS: dict[str, Callable[..., ModelRunner]] = {
    "codex": executors.CODEX,
    "claude": executors.CLAUDE,
    "kimi": executors.KIMI,
}


def resolve_executor(
    name: str,
    *,
    execute: bool,
    timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
) -> ModelRunner | None:
    """Fail closed on an unknown executor (even in dry-run). Return the real
    model only when ``execute`` is true; dry-run returns None so nothing runs."""
    factories = {**EXECUTORS, **(factory_overrides or {})}
    if name not in factories:
        raise ValueError(f"unknown executor: {name!r}; choose one of {sorted(factories)}")
    if not execute:
        return None
    return factories[name](timeout_seconds=timeout_seconds)


def build_worker(
    *,
    goal_store_root: str | Path,
    run_store_root: str | Path,
    workspace_root: str | Path,
    campaign: dict[str, Any],
    source_repo: str | Path,
    base_revision: str,
    executor_timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    grill_runner: grill_loop.GrillRunner | None = None,
) -> GoalLoopWorker:
    runs = RunStore(Path(run_store_root))
    campaign_id = campaign["campaign_id"]
    return GoalLoopWorker(
        goal_store=GoalStore(Path(goal_store_root)),
        run_store=runs,
        controller=LoopController(runs, Path(workspace_root), timeout_seconds=executor_timeout_seconds),
        compilers={campaign_id: CampaignCompiler(campaign)},
        execution_context={campaign_id: {"source_repo": Path(source_repo), "base_revision": base_revision}},
        grill_runner=grill_runner,
    )


def build_github_verdict(
    github_verdict: dict[str, str],
    *,
    run_store_root: str | Path,
    environ: Mapping[str, str] | None = None,
    transport: ghc.Transport | None = None,
) -> tuple[ev.VerdictStore, ghc.GitHubConclusionSource]:
    """Construct the durable verdict store and the GitHub conclusion source.

    The store lives under the run store root so the awaiting state survives
    host restarts next to the runs it parks. The token comes from the
    environment only; when it is absent ``from_env`` raises before the loop
    starts, so nothing is dispatched or polled. The token is never written
    to the store, receipts, or any artifact.
    """
    store = ev.VerdictStore(Path(run_store_root) / "verdict.sqlite3")

    def sha_resolver(op_key: str) -> str | None:
        action = store.action_for_op_key(op_key)
        external = action.get("external") if isinstance(action, dict) else None
        head_sha = external.get("head_sha") if isinstance(external, dict) else None
        return head_sha if isinstance(head_sha, str) and head_sha.strip() else None

    source = ghc.GitHubConclusionSource.from_env(
        github_verdict["owner"], github_verdict["repo"], github_verdict["workflow"], sha_resolver,
        environ=environ, transport=transport,
    )
    return store, source


def run(
    *,
    executor: str,
    execute: bool,
    goal_store_root: str | Path,
    run_store_root: str | Path,
    workspace_root: str | Path,
    campaign: dict[str, Any],
    source_repo: str | Path,
    base_revision: str,
    holder: str = "driver",
    pause_flag: str | Path | None = None,
    max_cycles: int | None = None,
    max_runs: int | None = None,
    max_runtime_seconds: float | None = None,
    budget_ceiling_tokens: int | None = None,
    budget_scope: str | None = None,
    idle_limit: int = 3,
    executor_timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
    status_snapshot_out: str | Path | None = None,
    verdict_store: ev.VerdictStore | None = None,
    conclusion_source: ev.ConclusionSource | None = None,
    github_verdict: dict[str, str] | None = None,
    github_environ: Mapping[str, str] | None = None,
    github_transport: ghc.Transport | None = None,
    factory_overrides: dict[str, Callable[..., ModelRunner]] | None = None,
    driver_fn: Callable[..., dict[str, Any]] = run_driver,
    sleep_fn: Callable[[float], None] | None = None,
    judge_executor: str | None = None,
    judge_model: str | None = None,
    turning_point: TurningPointRunner | None = None,
    quota_reader: Callable[[], dict[str, Any] | None] | None = None,
    daily_soft_cap_usd: float | None = 2.0,
    daily_hard_cap_usd: float | None = 5.0,
) -> dict[str, Any]:
    if github_verdict is not None:
        if verdict_store is not None or conclusion_source is not None:
            raise ValueError("github_verdict cannot be combined with an explicit verdict_store/conclusion_source")
        verdict_store, conclusion_source = build_github_verdict(
            github_verdict, run_store_root=run_store_root, environ=github_environ, transport=github_transport,
        )
    if (verdict_store is None) != (conclusion_source is None):
        raise ValueError("verdict_store and conclusion_source must be supplied together")
    if judge_executor is not None and turning_point is not None:
        raise ValueError("judge_executor and turning_point are mutually exclusive")
    plan = {
        "executor": executor,
        "execute": execute,
        "campaign_id": campaign["campaign_id"],
        "goal_store": str(goal_store_root),
        "run_store": str(run_store_root),
        "judge_executor": judge_executor,
        "judge_model": judge_model,
        "gates": {
            "pause_flag": str(pause_flag) if pause_flag is not None else None,
            "max_cycles": max_cycles,
            "max_runs": max_runs,
            "max_runtime_seconds": max_runtime_seconds,
            "budget_ceiling_tokens": budget_ceiling_tokens,
            "budget_scope": budget_scope,
            "idle_limit": idle_limit,
            "executor_timeout_seconds": executor_timeout_seconds,
            "status_snapshot_out": str(status_snapshot_out) if status_snapshot_out is not None else None,
            "external_verdict_poll": verdict_store is not None,
            "daily_soft_cap_usd": daily_soft_cap_usd,
            "daily_hard_cap_usd": daily_hard_cap_usd,
            "quota_gate": quota_reader is not None,
        },
        "boundary": "executor runs in a disposable clone; output stops at a PR; push/merge/promotion are human/project-owned",
    }
    model = resolve_executor(executor, execute=execute, timeout_seconds=executor_timeout_seconds, factory_overrides=factory_overrides)
    if model is None:
        return {"mode": "dry_run", "invoked": False, "plan": plan}
    grill_runner: grill_loop.GrillRunner | None = None
    if judge_executor is not None:
        factories = {**EXECUTORS, **(factory_overrides or {})}
        if judge_executor not in factories:
            raise ValueError(f"unknown judge_executor: {judge_executor!r}; choose one of {sorted(factories)}")
        turning_point = tp.make_cli_judge(
            lambda prompt: executors.judge_argv(judge_executor, prompt, judge_model),
            name=judge_executor,
        )
        # W6a: the same judge CLI layering carries the challenger grill before
        # a run's last allowed attempt. Absent judge = grill stays off (the
        # original max_attempts behavior), so a missing models.judge config
        # never blocks the loop.
        grill_runner = grill_loop.make_cli_judge(
            lambda prompt: executors.judge_argv(judge_executor, prompt, judge_model),
            name=judge_executor,
        )
    worker = build_worker(
        goal_store_root=goal_store_root,
        run_store_root=run_store_root,
        workspace_root=workspace_root,
        campaign=campaign,
        source_repo=source_repo,
        base_revision=base_revision,
        executor_timeout_seconds=executor_timeout_seconds,
        grill_runner=grill_runner,
    )
    startup_external_resumed = []
    if verdict_store is not None and conclusion_source is not None:
        # Poll before entering the driver: a host that cannot acquire the
        # driver holder may return ``not_holder`` without performing a tick.
        # Restart durability therefore cannot depend on run_driver reaching its
        # first worker.tick.
        startup_external_resumed = worker.controller.resume_external(verdict_store=verdict_store, source=conclusion_source)
    summary = driver_fn(
        worker,
        holder=holder,
        model=model,
        pause_flag=pause_flag,
        max_cycles=max_cycles,
        max_runs=max_runs,
        max_runtime_seconds=max_runtime_seconds,
        budget_ceiling_tokens=budget_ceiling_tokens,
        budget_scope=budget_scope,
        idle_limit=idle_limit,
        status_snapshot_out=status_snapshot_out,
        verdict_store=verdict_store,
        conclusion_source=conclusion_source,
        quota_reader=quota_reader,
        daily_soft_cap_usd=daily_soft_cap_usd,
        daily_hard_cap_usd=daily_hard_cap_usd,
        sleep_fn=sleep_fn,
        turning_point=turning_point,
    )
    return {"mode": "execute", "invoked": True, "plan": plan, "startup_external_resumed": startup_external_resumed, "driver": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the autonomous driver with a real coding-agent executor (opt-in)")
    parser.add_argument("--executor", default=None, choices=sorted(EXECUTORS),
                        help="coding executor; defaults to the contract's models.execute when --contract is used")
    parser.add_argument("--judge-executor", default=None, choices=sorted(EXECUTORS),
                        help="optional turning-point judge executor (M1 model routing); defaults to the contract's models.judge")
    parser.add_argument("--judge-model", default=None, help="optional model id pinned for the judge (e.g. a reasoning-tier model)")
    parser.add_argument("--execute", action="store_true", help="actually invoke the executor; omit for a dry-run plan")
    # binding: either a Project Runtime Contract (--contract) or the explicit flags below
    parser.add_argument("--contract", default=None, help="path to a Project Runtime Contract; fills the binding flags below")
    parser.add_argument("--goal-store", default=None)
    parser.add_argument("--run-store", default=None)
    parser.add_argument("--workspace-root", default=None)
    parser.add_argument("--campaign", default=None, help="path to a campaign JSON file")
    parser.add_argument("--source-repo", default=None)
    parser.add_argument("--base-revision", default=None)
    parser.add_argument("--pause-flag", default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--max-runs", type=int, default=None)
    parser.add_argument("--max-runtime-seconds", type=float, default=None)
    parser.add_argument("--budget-ceiling-tokens", type=int, default=None)
    parser.add_argument("--budget-scope", default=None)
    parser.add_argument("--idle-limit", type=int, default=3)
    parser.add_argument("--executor-timeout-seconds", type=int, default=int(DEFAULT_EXECUTOR_TIMEOUT_SECONDS))
    parser.add_argument("--status-snapshot-out", default=None, help="opt-in: refresh a JSON status snapshot at this path each progressing tick")
    args = parser.parse_args(argv)

    if args.contract:
        binding = project_binding.resolve_project(args.contract)["run_kwargs"]
    else:
        missing = [n for n in ("goal_store", "run_store", "workspace_root", "campaign", "source_repo", "base_revision") if not getattr(args, n.replace("-", "_"))]
        if missing:
            parser.error(f"without --contract these are required: {', '.join('--' + m.replace('_', '-') for m in missing)}")
        binding = {
            "campaign": json.loads(Path(args.campaign).read_text(encoding="utf-8")),
            "source_repo": args.source_repo,
            "base_revision": args.base_revision,
            "goal_store_root": args.goal_store,
            "run_store_root": args.run_store,
            "workspace_root": args.workspace_root,
        }
    # explicit flags still override / supply the optional gates the contract does not carry
    binding.setdefault("pause_flag", args.pause_flag)
    if args.status_snapshot_out:
        binding.setdefault("status_snapshot_out", args.status_snapshot_out)
    if args.budget_ceiling_tokens is not None:
        binding["budget_ceiling_tokens"] = args.budget_ceiling_tokens
    if args.budget_scope is not None:
        binding["budget_scope"] = args.budget_scope
    executor = args.executor or binding.pop("executor", None)
    if not executor:
        parser.error("--executor is required (or set models.execute in the contract)")
    judge_executor = args.judge_executor or binding.pop("judge_executor", None)
    judge_model = args.judge_model or binding.pop("judge_model", None)

    result = run(
        executor=executor,
        execute=args.execute,
        max_cycles=args.max_cycles,
        max_runs=args.max_runs,
        max_runtime_seconds=args.max_runtime_seconds,
        budget_ceiling_tokens=args.budget_ceiling_tokens,
        budget_scope=args.budget_scope,
        idle_limit=args.idle_limit,
        executor_timeout_seconds=args.executor_timeout_seconds,
        judge_executor=judge_executor,
        judge_model=judge_model,
        **binding,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Provider-free smoke for wiring a real executor into the driver (opt-in, gated).

Proves selection, the dry-run gate, fail-closed on unknown executors, and that
--execute actually threads the chosen model into the driver — all without
invoking codex/claude. Actually running a real coding-agent CLI is a separate
human live smoke (see the autonomous-driver contract in the project's docs).
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import cli_agent_executor as executors
from _fixture import make_campaign, make_source_repo
from goal_loop_run import EXECUTORS, resolve_executor, run
from goal_store import GoalStore


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _noop_sleep(_seconds: float) -> None:
    return None


def campaign() -> dict:
    return make_campaign("campaign-exec")


def fake_executor_factory(*, timeout_seconds: int = 900):
    def model(workspace: Path, capsule: dict) -> dict:
        path = workspace / "src"
        path.mkdir(exist_ok=True)
        (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
        return {"summary": f"fake executor (timeout={timeout_seconds})"}
    return model


class _Spy:
    def __init__(self) -> None:
        self.called = False

    def __call__(self, *, timeout_seconds: int = 900):
        self.called = True
        return fake_executor_factory(timeout_seconds=timeout_seconds)


def _source_repo(root: Path) -> tuple[Path, str]:
    return make_source_repo(root)


def _seed(goal_root: Path, camp: dict) -> None:
    from campaign_compiler import CampaignCompiler
    envelope = CampaignCompiler(camp).compile()["stages"]["stage-1"]
    GoalStore(goal_root).record_event(
        event_id="exec-seed-1", idempotency_key="exec-seed-1", source="manual_intent", event_type="goal_candidate",
        payload={"candidate": {"goal_id": "campaign-exec:stage-1", "campaign_id": "campaign-exec", "stage_id": "stage-1", "goal": {"feature_contract": "stage-1", "admission_envelope": envelope}}},
    )


def _rejects(fn) -> tuple[bool, str]:
    try:
        fn()
        return False, "no error raised"
    except (ValueError, KeyError) as exc:
        return True, f"{type(exc).__name__}: {exc}"


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = _source_repo(root)
        camp = campaign()

        presets_ok = set(EXECUTORS) == {"codex", "claude", "kimi"} \
            and executors.codex_argv("P") == ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "P"] \
            and executors.claude_argv("P") == ["claude", "-p", "P", "--permission-mode", "bypassPermissions"] \
            and executors.kimi_argv("P") == ["kimi", "-p", "P"]

        spy = _Spy()
        dry = run(executor="codex", execute=False, goal_store_root=root / "d-goals", run_store_root=root / "d-runs",
                  workspace_root=root / "d-ws", campaign=camp, source_repo=source, base_revision=base,
                  factory_overrides={"codex": spy})

        unknown_rejected, unknown_detail = _rejects(lambda: resolve_executor("gpt-nope", execute=False))

        _seed(root / "x-goals", camp)
        executed = run(executor="fake", execute=True, goal_store_root=root / "x-goals", run_store_root=root / "x-runs",
                       workspace_root=root / "x-ws", campaign=camp, source_repo=source, base_revision=base,
                       max_cycles=30, factory_overrides={"fake": fake_executor_factory}, sleep_fn=_noop_sleep)
        executed_done = GoalStore(root / "x-goals").get_goal("campaign-exec:stage-1")["state"] == "completed"

        _seed(root / "p-goals", camp)
        flag = root / "loop-pause-all"
        flag.write_text("stop\n", encoding="utf-8")
        gated = run(executor="fake", execute=True, goal_store_root=root / "p-goals", run_store_root=root / "p-runs",
                    workspace_root=root / "p-ws", campaign=camp, source_repo=source, base_revision=base,
                    pause_flag=flag, max_cycles=30, factory_overrides={"fake": fake_executor_factory}, sleep_fn=_noop_sleep)

        cases = [
            case("registry-presets-are-model-agnostic-bypass-argv", presets_ok, f"executors={sorted(EXECUTORS)}"),
            case("dry-run-is-default-and-never-invokes-executor", dry["mode"] == "dry_run" and dry["invoked"] is False and spy.called is False, str(dry)),
            case("unknown-executor-fails-closed", unknown_rejected, unknown_detail),
            case("execute-threads-real-model-into-driver", executed["invoked"] is True and executed["driver"]["runs_dispatched"] == 1 and executed_done, str(executed)),
            case("kill-switch-honored-through-run-entry", gated["invoked"] is True and gated["driver"]["stop_reason"] == "paused" and gated["driver"]["runs_dispatched"] == 0, str(gated)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-executor-wiring",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "actually invoking codex/claude is a human live smoke; this gate injects a fake executor",
            "GitHub PR adapter and promotion remain later/human-owned nodes",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

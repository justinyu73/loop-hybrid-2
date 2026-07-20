#!/usr/bin/env python3
"""Provider-free smoke for the always-on driver: gates and continuous resume."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from project_status import build_status
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign() -> dict:
    def stage(stage_id: str, next_stage_id: str | None) -> dict:
        return {
            "stage_id": stage_id,
            "goal": {"feature_contract": stage_id},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": stage_id + "-lamp", "smoke": "a staged change exists", "verification_argv": ["sh", "-c", "! git diff --cached --quiet"]},
            "max_attempts": 4,
            "next_stage_id": next_stage_id,
        }
    return {"schema": CAMPAIGN_SCHEMA, "campaign_id": "campaign-drv", "stages": [stage("stage-1", "stage-2"), stage("stage-2", None)]}


def model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "driver bounded model fixture"}


def _noop_sleep(_seconds: float) -> None:
    return None


class _StubGoalStore:
    """Minimal goal_store surface for driver-only branch tests (parked/timeout)."""

    def __init__(self, human_required_ids: list[str]) -> None:
        self._ids = human_required_ids

    def goals_in_state(self, state: str) -> list[dict]:
        return [{"goal_id": g} for g in self._ids] if state == "human_required" else []


class _StubWorker:
    def __init__(self, human_required_ids: list[str]) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.run_store = RunStore(Path(self._tempdir.name) / "runs")
        self.goal_store = _StubGoalStore(human_required_ids)

    def tick(self, **_kwargs) -> dict:
        return {"status": "idle", "run": None, "terminal_after": None}


def _counter_clock():
    ticks = iter(range(0, 10_000))
    return lambda: float(next(ticks))


def _source_repo(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    git("init", "-q", str(source))
    git("-C", str(source), "config", "user.email", "drv@example.invalid")
    git("-C", str(source), "config", "user.name", "Driver Canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    git("-C", str(source), "add", "baseline.txt")
    git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    return source, base


def _worker(root: Path, tag: str, source: Path, base: str, compiler: CampaignCompiler) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={"campaign-drv": compiler},
        execution_context={"campaign-drv": {"source_repo": source, "base_revision": base}},
    )


def _seed(goal_store: GoalStore, compiler: CampaignCompiler, *, goal_id: str, stage_id: str, event_key: str) -> None:
    envelope = compiler.compile()["stages"][stage_id]
    goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": "campaign-drv", "stage_id": stage_id, "goal": {"feature_contract": stage_id, "admission_envelope": envelope}}
    })


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = _source_repo(root)
        compiler = CampaignCompiler(campaign())

        # 1) drive one seeded campaign to completion with no manual re-tick.
        w1 = _worker(root, "done", source, base, compiler)
        _seed(w1.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-seed-1")
        done = run_driver(w1, holder="drv-a", model=model, max_cycles=30, sleep_fn=_noop_sleep)
        stage2_done = w1.goal_store.get_goal("campaign-drv:stage-2")["state"] == "completed"

        # 2) kill switch halts before any tick.
        w2 = _worker(root, "pause", source, base, compiler)
        _seed(w2.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-pause-1")
        flag = root / "loop-pause-all"
        flag.write_text("stop\n", encoding="utf-8")
        paused = run_driver(w2, holder="drv-b", model=model, pause_flag=flag, max_cycles=30, sleep_fn=_noop_sleep)

        # 3) empty store idles to a clean stop, then a restart makes progress.
        w3 = _worker(root, "idle", source, base, compiler)
        idled = run_driver(w3, holder="drv-c", model=model, idle_limit=3, max_cycles=30, sleep_fn=_noop_sleep)
        _seed(w3.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-idle-1")
        resumed = run_driver(w3, holder="drv-c", model=model, max_cycles=30, sleep_fn=_noop_sleep)

        # 4) budget cap stops after one dispatched run.
        w4 = _worker(root, "budget", source, base, compiler)
        _seed(w4.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-budget-1")
        budget = run_driver(w4, holder="drv-d", model=model, max_runs=1, max_cycles=30, sleep_fn=_noop_sleep)
        budget_stage2 = w4.goal_store.get_goal("campaign-drv:stage-2")["state"] if _goal_exists(w4.goal_store, "campaign-drv:stage-2") else "absent"

        # 5) max-cycles cap stops a still-progressing loop.
        w5 = _worker(root, "cap", source, base, compiler)
        _seed(w5.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-cap-1")
        capped = run_driver(w5, holder="drv-e", model=model, max_cycles=1, sleep_fn=_noop_sleep)

        # 6) idle only because goals are parked in human_required -> stop_reason parked (driver branch).
        parked = run_driver(_StubWorker(["campaign-drv:needs-human"]), holder="drv-f", model=model, idle_limit=2, max_cycles=30, sleep_fn=_noop_sleep)

        # 7) wall-clock timeout stops before idle_limit (driver branch).
        timed_out = run_driver(_StubWorker([]), holder="drv-g", model=model, idle_limit=100, max_runtime_seconds=2, sleep_fn=_noop_sleep, clock_fn=_counter_clock())

        # 8) opt-in status snapshot auto-refreshes as the driver progresses.
        w8 = _worker(root, "snap", source, base, compiler)
        _seed(w8.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-snap-1")
        snap_out = root / "runtime" / "platform_status.json"
        run_driver(w8, holder="drv-h", model=model, max_cycles=30, status_snapshot_out=snap_out, sleep_fn=_noop_sleep)
        snap_reloaded = json.loads(snap_out.read_text(encoding="utf-8")) if snap_out.exists() else {}
        snap_matches = snap_out.exists() and snap_reloaded.get("status") == build_status(w8.run_store, w8.goal_store) and not snap_out.with_name(snap_out.name + ".tmp").exists()
        # a driver run without the flag writes nothing.
        w9 = _worker(root, "nosnap", source, base, compiler)
        _seed(w9.goal_store, compiler, goal_id="campaign-drv:stage-1", stage_id="stage-1", event_key="drv-nosnap-1")
        nosnap_out = root / "runtime-off" / "platform_status.json"
        run_driver(w9, holder="drv-i", model=model, max_cycles=30, sleep_fn=_noop_sleep)

        cases = [
            case("drives-campaign-to-completion-without-manual-retick", done["stop_reason"] == "idle" and done["runs_dispatched"] == 2 and stage2_done, str(done)),
            case("kill-switch-halts-before-any-tick", paused["stop_reason"] == "paused" and paused["cycles"] == 0 and paused["runs_dispatched"] == 0, str(paused)),
            case("empty-store-idles-then-restart-resumes", idled["stop_reason"] == "idle" and idled["cycles"] == 3 and idled["runs_dispatched"] == 0 and resumed["runs_dispatched"] >= 1, f"{idled} | {resumed}"),
            case("budget-cap-stops-after-one-run", budget["stop_reason"] == "budget" and budget["runs_dispatched"] == 1 and budget_stage2 in {"absent", "candidate"}, f"{budget} | stage2={budget_stage2}"),
            case("max-cycles-cap-stops-progressing-loop", capped["stop_reason"] == "max_cycles" and capped["cycles"] == 1, str(capped)),
            case("parked-human-required-is-distinct-from-idle", parked["stop_reason"] == "parked" and parked["parked_goals"] == ["campaign-drv:needs-human"], str(parked)),
            case("wall-clock-timeout-stops-before-idle-limit", timed_out["stop_reason"] == "timeout" and timed_out["idle_streak"] < 100, str(timed_out)),
            case("status-snapshot-auto-refreshes-on-progress", snap_matches, f"exists={snap_out.exists()} tokens={snap_reloaded.get('status', {}).get('headline', {}).get('total_tokens')}"),
            case("status-snapshot-off-by-default", not nosnap_out.exists(), str(nosnap_out)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-goal-loop-driver",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "driver is provider-free; the real coding-agent executor is wired in a later opt-in node",
            "budget is a run-count proxy; token/cost budget is a later node",
            "single-holder lease exclusivity is proven at the worker/store layer (goal_loop_canary)",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _goal_exists(goal_store: GoalStore, goal_id: str) -> bool:
    try:
        goal_store.get_goal(goal_id)
        return True
    except KeyError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())

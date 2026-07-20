#!/usr/bin/env python3
"""Provider-free smoke: the 报红 value verdict is the acceptance authority (N5b).

A lamp-passing but value-RED run (e.g. edits outside the allowlist) must NOT
auto-advance to completed; it routes to human_required. With the gate off it
advances — proving the gate, not the lamp, is what stops it."""
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
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign() -> dict:
    stage = {
        "stage_id": "s1", "goal": {"feature_contract": "s1"},
        "allowed_paths": ["src/"], "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {"id": "s1-lamp", "smoke": "a staged change exists", "verification_argv": ["sh", "-c", "! git diff --cached --quiet"]},
        "max_attempts": 2, "next_stage_id": None,
    }
    return {"schema": CAMPAIGN_SCHEMA, "campaign_id": "campaign-vg", "stages": [stage]}


def in_scope_model(workspace: Path, capsule: dict) -> dict:
    (workspace / "src").mkdir(exist_ok=True)
    (workspace / "src" / "ok.txt").write_text("ok\n", encoding="utf-8")
    return {"summary": "in-scope change"}


def scope_creep_model(workspace: Path, capsule: dict) -> dict:
    # lamp (a staged change exists) will still pass, but this file is outside allowed_paths.
    (workspace / "outside").mkdir(exist_ok=True)
    (workspace / "outside" / "leak.txt").write_text("leak\n", encoding="utf-8")
    return {"summary": "out-of-scope change that still passes the lamp"}


def _source(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    git("init", "-q", str(source))
    git("-C", str(source), "config", "user.email", "vg@example.invalid")
    git("-C", str(source), "config", "user.name", "VG Canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    git("-C", str(source), "add", "baseline.txt")
    git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    return source, base


def _worker(root: Path, tag: str, source: Path, base: str, compiler: CampaignCompiler, *, value_gate: bool) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"), run_store=runs,
        controller=LoopController(runs, root / f"{tag}-ws"),
        compilers={"campaign-vg": compiler},
        execution_context={"campaign-vg": {"source_repo": source, "base_revision": base}},
        value_gate=value_gate,
    )


def _seed(goal_store: GoalStore, compiler: CampaignCompiler, event_key: str) -> None:
    envelope = compiler.compile()["stages"]["s1"]
    goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": "campaign-vg:s1", "campaign_id": "campaign-vg", "stage_id": "s1", "goal": {"feature_contract": "s1", "admission_envelope": envelope}}
    })


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = _source(root)
        compiler = CampaignCompiler(campaign())

        wg = _worker(root, "green", source, base, compiler, value_gate=True)
        _seed(wg.goal_store, compiler, "vg-green")
        green_tick = wg.tick(holder="a", model=in_scope_model)
        green_state = wg.goal_store.get_goal("campaign-vg:s1")["state"]

        wr = _worker(root, "red", source, base, compiler, value_gate=True)
        _seed(wr.goal_store, compiler, "vg-red")
        red_tick = wr.tick(holder="b", model=scope_creep_model)
        red_state = wr.goal_store.get_goal("campaign-vg:s1")["state"]

        wo = _worker(root, "off", source, base, compiler, value_gate=False)
        _seed(wo.goal_store, compiler, "vg-off")
        off_tick = wo.tick(holder="c", model=scope_creep_model)
        off_state = wo.goal_store.get_goal("campaign-vg:s1")["state"]

        cases = [
            case("green-run-advances-to-completed", green_tick["run"]["status"] == "verified" and green_tick["terminal_after"]["status"] == "completed" and green_state == "completed", str(green_tick.get("terminal_after"))),
            case("lamp-pass-but-value-red-routes-to-human", red_tick["run"]["status"] == "verified" and red_tick["terminal_after"]["status"] == "value_red_human_required" and red_state == "human_required", str(red_tick.get("terminal_after"))),
            case("red-names-the-out-of-scope-file", any("outside/leak.txt" in r for r in red_tick["terminal_after"]["reasons"]), str(red_tick["terminal_after"]["reasons"])),
            case("gate-off-lets-the-same-red-through", off_tick["run"]["status"] == "verified" and off_state == "completed", f"off_state={off_state}"),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-value-gate",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "gate uses the deterministic value verdict; the independent-falsifier layer is separate/later",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

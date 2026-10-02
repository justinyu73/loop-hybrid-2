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
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from native_delivery_fixture import make_native_bundle
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign(*, allowed_paths: list[str] | None = None, max_attempts: int = 2, must_edit: bool = False) -> dict:
    allowed = list(allowed_paths or ["src/"])
    goal = {"feature_contract": "s1"}
    if must_edit:
        goal["must_have"] = ["disposable clone receives a bounded repo edit"]
    stage = {
        "stage_id": "s1", "goal": goal,
        "allowed_paths": allowed, "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {"id": "s1-lamp", "smoke": "a staged change exists", "verification_argv": ["sh", "-c", "! git diff --cached --quiet"]},
        "max_attempts": max_attempts, "next_stage_id": None,
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


def authority_surface_model(workspace: Path, capsule: dict) -> dict:
    # The delivery packet explicitly permits this path in the value-only case;
    # the independent value reducer still rejects authority-surface edits.
    target = workspace / "gate-pack" / "verify.sh"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    return {"summary": "in-scope authority-surface edit for value RED"}


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


def _worker(root: Path, tag: str, source: Path, base: str, compiler: CampaignCompiler, *, value_gate: bool, grill_runner=None) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs", command_runner=fixture_command_runner)
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"), run_store=runs,
        controller=LoopController(runs, root / f"{tag}-ws"),
        compilers={"campaign-vg": compiler},
        execution_context={"campaign-vg": {"source_repo": source, "base_revision": base}},
        value_gate=value_gate,
        grill_runner=grill_runner,
    )


def _bind_compiler(compiler: CampaignCompiler, source: Path, base: str, *, allowed: list[str], verifier: list[str], max_attempts: int) -> dict:
    envelope = compiler.compile()["stages"]["s1"]
    binding = make_native_bundle(
        source,
        base,
        "campaign-vg:s1",
        "s1",
        [{
            "id": "value-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        verifier,
        allowed,
        max_attempts,
        goal={"feature_contract": "s1", "admission_envelope": envelope},
    )
    bound = binding["goal"]["admission_envelope"]
    compiler.stages["s1"] = bound
    return bound


def _seed(goal_store: GoalStore, compiler: CampaignCompiler, event_key: str) -> None:
    envelope = compiler.compile()["stages"]["s1"]
    goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": "campaign-vg:s1", "campaign_id": "campaign-vg", "stage_id": "s1", "goal": {"feature_contract": "s1", "admission_envelope": envelope}}
    })


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = _source(root)
        compiler = CampaignCompiler(campaign(max_attempts=2))
        _bind_compiler(
            compiler,
            source,
            base,
            allowed=["src/"],
            verifier=["sh", "-c", "test -f src/ok.txt"],
            max_attempts=2,
        )

        wg = _worker(root, "green", source, base, compiler, value_gate=True)
        _seed(wg.goal_store, compiler, "vg-green")
        green_tick = wg.tick(holder="a", model=in_scope_model)
        green_state = wg.goal_store.get_goal("campaign-vg:s1")["state"]

        scope_compiler = CampaignCompiler(campaign(max_attempts=1, must_edit=True))
        _bind_compiler(
            scope_compiler,
            source,
            base,
            allowed=["src/"],
            verifier=["sh", "-c", "test -f outside/leak.txt"],
            max_attempts=1,
        )
        wr = _worker(root, "red", source, base, scope_compiler, value_gate=True)
        _seed(wr.goal_store, scope_compiler, "vg-red")
        red_tick = wr.tick(holder="b", model=scope_creep_model)
        red_tick_2 = wr.tick(holder="b", model=scope_creep_model)
        red_state = wr.goal_store.get_goal("campaign-vg:s1")["state"]

        value_compiler = CampaignCompiler(campaign(allowed_paths=["gate-pack/"], max_attempts=3, must_edit=True))
        _bind_compiler(
            value_compiler,
            source,
            base,
            allowed=["gate-pack/"],
            verifier=["sh", "-c", "test -f gate-pack/verify.sh"],
            max_attempts=3,
        )
        wv = _worker(
            root,
            "value-red",
            source,
            base,
            value_compiler,
            value_gate=True,
            grill_runner=lambda _snapshot: {"decision": "runner-fixable", "diagnosis": "controlled value-red remediation"},
        )
        _seed(wv.goal_store, value_compiler, "vg-value-red")
        wv.tick(holder="v", model=lambda workspace, capsule: {"summary": "controlled failed attempt"})
        wv.tick(holder="v", model=lambda workspace, capsule: {"summary": "controlled failed attempt"})
        value_red_tick = wv.tick(holder="v", model=authority_surface_model)
        value_red_state = wv.goal_store.get_goal("campaign-vg:s1")["state"]

        off_compiler = CampaignCompiler(campaign(allowed_paths=["gate-pack/"], max_attempts=1, must_edit=True))
        _bind_compiler(
            off_compiler,
            source,
            base,
            allowed=["gate-pack/"],
            verifier=["sh", "-c", "test -f gate-pack/verify.sh"],
            max_attempts=1,
        )
        wo = _worker(root, "off", source, base, off_compiler, value_gate=False)
        _seed(wo.goal_store, off_compiler, "vg-off")
        off_tick = wo.tick(holder="c", model=authority_surface_model)
        off_state = wo.goal_store.get_goal("campaign-vg:s1")["state"]

        cases = [
            case("green-run-advances-to-completed", green_tick["run"]["status"] == "verified" and green_tick["terminal_after"]["status"] == "completed" and green_state == "completed", str(green_tick.get("terminal_after"))),
            case(
                "lamp-pass-but-delivery-scope-red-never-completes",
                red_tick["run"]["status"] == "human_required"
                and red_state == "stopped"
                and any("outside/leak.txt" in json.dumps(event, sort_keys=True) for event in wr.run_store.events(red_tick["run"]["run_id"]))
                and not any(event["event_type"] == "fence_rejected" for event in wr.run_store.events(red_tick["run"]["run_id"])),
                str({"first": red_tick, "second": red_tick_2}),
            ),
            case("value-red-routes-human-and-is-not-machine-resolved", value_red_tick["run"]["status"] == "verified" and value_red_tick["terminal_after"] is not None and value_red_tick["terminal_after"]["status"] == "value_red_human_required" and value_red_state == "human_required" and value_red_tick["terminal_after"].get("failure_case", {}).get("machine_resolved") is False, str(value_red_tick.get("terminal_after"))),
            case("value-red-names-authority-surface", value_red_tick["terminal_after"] is not None and any("authority surface touched" in reason for reason in value_red_tick["terminal_after"]["reasons"]), str((value_red_tick.get("terminal_after") or {}).get("reasons"))),
            case("gate-off-lets-legal-value-red-through", off_tick["run"]["status"] == "verified" and off_tick["terminal_after"]["status"] == "completed" and off_state == "completed", f"off_state={off_state}"),
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

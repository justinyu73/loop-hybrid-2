#!/usr/bin/env python3
"""Committed W2 smoke: worker dispatch wires the async external-verdict leg.

Proves an envelope-declared ``external_verdict`` stage dispatches through
controller.tick_async (park, poll, resume), that executor failure retries
without parking, that a retry with an identical diff dedupes the external
action on op_key, that an async stage without wiring routes to a human, and
that the classic local-verifier path is byte-for-byte unchanged.  Fixture
adapter only — no network, no credentials, no real provider.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import external_action_port as eap
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from external_verdict import VerdictStore
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


class FixtureAdapter:
    """Counts perform calls; idempotent on op_key like a real adapter must be."""

    def __init__(self) -> None:
        self.calls = 0
        self.seen: dict[str, dict[str, Any]] = {}

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        if op_key not in self.seen:
            self.calls += 1
            self.seen[op_key] = {"pr": f"fixture-pr-{self.calls}", "op_key": op_key}
        return self.seen[op_key]


def campaign() -> dict:
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": "campaign-w2",
        "stages": [{
            "stage_id": "stage-async",
            "goal": {"feature_contract": "stage-async"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": "async-lamp", "smoke": "git diff --check", "verification_argv": ["git", "diff", "--check"]},
            "max_attempts": 4,
            "next_stage_id": None,
        }],
    }


def async_envelope(compiler: CampaignCompiler) -> dict:
    envelope = compiler.compile()["stages"]["stage-async"]
    del envelope["acceptance_lamp"]
    envelope["external_verdict"] = {"action_id": "open-pr"}
    return envelope


def seed(store: GoalStore, *, goal_id: str, envelope: dict, event_key: str) -> None:
    store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": "campaign-w2", "stage_id": "stage-async",
                      "goal": {"feature_contract": "stage-async", "admission_envelope": envelope}}
    })


def model(workspace: Path, capsule: dict) -> dict:
    src = workspace / "src"
    src.mkdir(exist_ok=True)
    (src / "out.txt").write_text("deterministic\n", encoding="utf-8")
    return {"summary": "w2 async fixture"}


def failing_model(workspace: Path, capsule: dict) -> dict:
    raise RuntimeError("w2 executor failure fixture")


def make_worker(root: Path, name: str, source: Path, base: str, compiler: CampaignCompiler,
                *, wired: bool = True) -> tuple[GoalLoopWorker, VerdictStore, FixtureAdapter]:
    goals = GoalStore(root / f"{name}-goals")
    runs = RunStore(root / f"{name}-runs")
    verdicts = VerdictStore(root / f"{name}-verdicts")
    adapter = FixtureAdapter()
    kwargs: dict[str, Any] = {}
    if wired:
        kwargs = {"action_ledger": eap.ActionLedger(root / f"{name}-ledger" / "ledger.sqlite3"), "external_adapter": adapter}
    worker = GoalLoopWorker(
        goal_store=goals, run_store=runs, controller=LoopController(runs, root / f"{name}-workspaces"),
        compilers={"campaign-w2": compiler},
        execution_context={"campaign-w2": {"source_repo": source, "base_revision": base}},
        **kwargs,
    )
    return worker, verdicts, adapter


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source = root / "source"
        source.mkdir()
        git("init", "-q", str(source))
        git("-C", str(source), "config", "user.email", "w2@example.invalid")
        git("-C", str(source), "config", "user.name", "W2 Canary")
        (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
        git("-C", str(source), "add", "baseline.txt")
        git("-C", str(source), "commit", "-qm", "baseline")
        base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
        compiler = CampaignCompiler(campaign())

        # Flow A: park -> success verdict -> verified.
        worker_a, verdicts_a, adapter_a = make_worker(root, "a", source, base, compiler)
        seed(worker_a.goal_store, goal_id="campaign-w2:async-a", envelope=async_envelope(compiler), event_key="w2-a-seed")
        parked_a = worker_a.tick(holder="w2-a", model=model, verdict_store=verdicts_a, conclusion_source=lambda op_key: None)
        run_a = parked_a["run"]["run_id"]
        run_a_state = RunStore(root / "a-runs").get_run(run_a)["state"]
        op_key_a = parked_a["run"].get("op_key")
        awaiting_a_after_park = verdicts_a.awaiting()
        landed_a = worker_a.tick(holder="w2-a", model=model, verdict_store=verdicts_a,
                                 conclusion_source=lambda op_key: {"conclusion": "success"} if op_key == op_key_a else None)
        run_a_after = RunStore(root / "a-runs").get_run(run_a)["state"]

        # Flow B: park -> failure verdict -> retry; identical diff dedupes the action.
        worker_b, verdicts_b, adapter_b = make_worker(root, "b", source, base, compiler)
        seed(worker_b.goal_store, goal_id="campaign-w2:async-b", envelope=async_envelope(compiler), event_key="w2-b-seed")
        parked_b = worker_b.tick(holder="w2-b", model=model, verdict_store=verdicts_b, conclusion_source=lambda op_key: None)
        run_b = parked_b["run"]["run_id"]
        op_key_b = parked_b["run"].get("op_key")
        failed_b = worker_b.tick(holder="w2-b", model=model, verdict_store=verdicts_b,
                                 conclusion_source=lambda op_key: {"conclusion": "failure"} if op_key == op_key_b else None)

        # Flow C: executor failure retries without parking.
        worker_c, verdicts_c, adapter_c = make_worker(root, "c", source, base, compiler)
        seed(worker_c.goal_store, goal_id="campaign-w2:async-c", envelope=async_envelope(compiler), event_key="w2-c-seed")
        failed_c = worker_c.tick(holder="w2-c", model=failing_model, verdict_store=verdicts_c, conclusion_source=lambda op_key: None)

        # Flow D: async envelope on an unwired worker routes to a human.
        worker_d, verdicts_d, _adapter_d = make_worker(root, "d", source, base, compiler, wired=False)
        seed(worker_d.goal_store, goal_id="campaign-w2:async-d", envelope=async_envelope(compiler), event_key="w2-d-seed")
        unwired_d = worker_d.tick(holder="w2-d", model=model)
        goal_d = GoalStore(root / "d-goals").get_goal("campaign-w2:async-d")["state"]

        # Flow E: classic verification_argv envelope still takes the local verifier,
        # even on a fully wired worker.
        worker_e, verdicts_e, adapter_e = make_worker(root, "e", source, base, compiler)
        seed(worker_e.goal_store, goal_id="campaign-w2:sync-e", envelope=compiler.compile()["stages"]["stage-async"], event_key="w2-e-seed")
        sync_e = worker_e.tick(holder="w2-e", model=model, verdict_store=verdicts_e, conclusion_source=lambda op_key: None)

        cases = [
            case("async-envelope-parks-run",
                 parked_a["run"]["status"] == "awaiting_external_verdict"
                 and run_a_state == "awaiting_external_verdict"
                 and awaiting_a_after_park == [(run_a, op_key_a)]
                 and adapter_a.calls == 1,
                 str(parked_a["run"])),
            case("landed-success-verdict-verifies-run",
                 landed_a["external_resumed"] == [{"run_id": run_a, "op_key": op_key_a, "conclusion": "success", "state": "verified"}]
                 and run_a_after == "verified",
                 str(landed_a["external_resumed"])),
            case("landed-failure-verdict-retries-run",
                 failed_b["external_resumed"] == [{"run_id": run_b, "op_key": op_key_b, "conclusion": "failure", "state": "retry_pending"}],
                 str(failed_b["external_resumed"])),
            case("retry-with-identical-diff-dedupes-external-action",
                 adapter_b.calls == 1
                 and failed_b["run"]["status"] == "awaiting_external_verdict"
                 and failed_b["run"]["attempt"] == 2,
                 f"calls={adapter_b.calls} run={failed_b['run']}"),
            case("executor-failure-retries-without-park",
                 failed_c["run"]["status"] == "retry_pending"
                 and verdicts_c.awaiting() == []
                 and adapter_c.calls == 0,
                 str(failed_c["run"])),
            case("async-envelope-without-wiring-routes-human",
                 unwired_d["run"]["status"] == "human_required" and goal_d == "human_required",
                 str(unwired_d["run"])),
            case("sync-envelope-still-takes-local-verifier",
                 sync_e["run"]["status"] == "verified" and adapter_e.calls == 0 and verdicts_e.awaiting() == [],
                 str(sync_e["run"])),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-worker-async-dispatch", "status": "pass" if not failures else "fail",
        "total": len(cases), "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/worker_async_canary.py", "adapter": "fixture only, no network"},
        "known_gaps_open": [
            "A retried async run re-parks in run_store, but VerdictStore.park is INSERT OR IGNORE, "
            "so the re-parked run is not re-polled; pre-existing controller.tick_async limitation, out of W2 scope.",
            "github_conclusion_source remains Deferred; canary uses a fixture conclusion source.",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

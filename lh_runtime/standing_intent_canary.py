#!/usr/bin/env python3
"""Committed W9f smoke: the standing intent emitter (autonomous intent source).

Proves, offline with injected clocks and fixture models, that a campaign can
declare recurring daily intents: the worker emits exactly one manual_intent
per UTC day through the human command path (natural idempotency dedup),
open goals (candidate/active/human_required) block emission while completed
ones do not, a new UTC day emits a new window key, a green-on-base health
check then completes via the W3 precheck with zero model calls, and invalid
declarations (non-daily interval, unknown stage, non-admissible stage) are
rejected at compile time. Emission is pure deterministic — no model anywhere.
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_source_repo
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from controller import LoopController
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from run_store import RunStore

CAMPAIGN_ID = "campaign-w9f"
STAGE_ID = "health"
GOAL_ID = f"{CAMPAIGN_ID}:{STAGE_ID}"
DAY_ONE = datetime(2026, 1, 2, tzinfo=timezone.utc)
DAY_TWO = datetime(2026, 1, 3, tzinfo=timezone.utc)
DAY_ONE_KEY = f"standing:{CAMPAIGN_ID}:{STAGE_ID}:{DAY_ONE.date().isoformat()}"
DAY_TWO_KEY = f"standing:{CAMPAIGN_ID}:{STAGE_ID}:{DAY_TWO.date().isoformat()}"


def _campaign(*, standing: Any = None, green_lamp: bool = True) -> dict:
    campaign = {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "stages": [{
            "stage_id": STAGE_ID,
            "goal": {"feature_contract": "daily self health check"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            # green-on-base: the daily check completes via the W3 precheck at zero model cost
            "acceptance_lamp": {"id": "health-lamp", "smoke": "repo is healthy", "verification_argv": ["true"]},
            "max_attempts": 1,
            "next_stage_id": None,
        }],
    }
    if not green_lamp:
        campaign["stages"][0]["human_only"] = True
    if standing is not None:
        campaign["standing_intents"] = standing
    return campaign


STANDING = [{"stage_id": STAGE_ID, "interval": "daily", "intent": "run the daily self health check"}]


def _worker(root: Path, tag: str, source: Path, base: str, campaign: dict, *, now_fn=None) -> GoalLoopWorker:
    runs = RunStore(root / f"{tag}-runs")
    compiler = CampaignCompiler(campaign)
    return GoalLoopWorker(
        goal_store=GoalStore(root / f"{tag}-goals"),
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={compiler.campaign_id: compiler},
        execution_context={compiler.campaign_id: {"source_repo": source, "base_revision": base}},
        now_fn=now_fn,
    )


def _model_calls(calls: list[dict]):
    def model(_workspace: Path, _capsule: dict) -> dict:
        calls.append({})
        return {"summary": "w9f fixture model (must not run for a green health check)"}
    return model


def _compiler_error(campaign: dict) -> str | None:
    try:
        CampaignCompiler(campaign)
    except ValueError as exc:
        return str(exc)
    return None


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # Compiler contract.
        valid = CampaignCompiler(_campaign(standing=STANDING)).standing_intents
        bad_interval = _compiler_error(_campaign(standing=[{"stage_id": STAGE_ID, "interval": "hourly", "intent": "x"}]))
        bad_stage = _compiler_error(_campaign(standing=[{"stage_id": "ghost", "interval": "daily", "intent": "x"}]))
        bad_admission = _compiler_error(_campaign(standing=STANDING, green_lamp=False))

        # Day one, first tick: emit once and derive the candidate.
        worker = _worker(root, "main", source, base, _campaign(standing=STANDING), now_fn=lambda: DAY_ONE)
        calls: list[dict] = []
        model = _model_calls(calls)
        tick1 = worker.tick(holder="w9f", model=model)
        event1 = worker.goal_store.get_event(DAY_ONE_KEY)

        # Day one, second tick: admit + dispatch; the green lamp completes via precheck.
        tick2 = worker.tick(holder="w9f", model=model)
        goal_after_tick2 = worker.goal_store.get_goal(GOAL_ID)["state"]

        # Day one, third tick: completed goal does not block, but the key dedups.
        tick3 = worker.tick(holder="w9f", model=model)

        # Simulated next day: a new window key is emitted.
        worker._now_fn = lambda: DAY_TWO  # fixture clock flip; same durable stores
        tick_day2 = worker.tick(holder="w9f", model=model)
        event_day2 = worker.goal_store.get_event(DAY_TWO_KEY)

        # Open goal (human_required) blocks emission.
        blocked = _worker(root, "blocked", source, base, _campaign(standing=STANDING), now_fn=lambda: DAY_ONE)
        envelope = blocked.compilers[CAMPAIGN_ID].compile()["stages"][STAGE_ID]
        blocked.goal_store.record_event(event_id="w9f-block-seed", idempotency_key="w9f-block-seed", source="manual_intent", event_type="goal_candidate", payload={
            "candidate": {"goal_id": GOAL_ID, "campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID,
                          "goal": {"feature_contract": STAGE_ID, "admission_envelope": envelope}}
        })
        blocked.goal_store.create_candidate("w9f-block-seed", goal_id=GOAL_ID, campaign_id=CAMPAIGN_ID, stage_id=STAGE_ID,
                                            goal={"feature_contract": STAGE_ID, "admission_envelope": envelope})
        blocked.goal_store.transition_event("w9f-block-seed", "completed")
        blocked.goal_store.transition_goal(GOAL_ID, "human_required", expected_state="candidate")
        tick_blocked = blocked.tick(holder="w9f-blocked", model=model)

        # Campaign without standing_intents: unchanged behavior.
        plain = _worker(root, "plain", source, base, _campaign(), now_fn=lambda: DAY_ONE)
        tick_plain = plain.tick(holder="w9f-plain", model=model)

        emitted_tick1 = tick1.get("standing_emitted", [])
        cases = [
            {"id": "compiler-accepts-valid-standing-intents",
             "ok": valid == [{"stage_id": STAGE_ID, "interval": "daily", "intent": "run the daily self health check"}],
             "detail": json.dumps(valid)},
            {"id": "compiler-rejects-non-daily-interval",
             "ok": bad_interval is not None and "daily" in bad_interval,
             "detail": json.dumps({"error": bad_interval})},
            {"id": "compiler-rejects-unknown-stage",
             "ok": bad_stage is not None and "ghost" in bad_stage,
             "detail": json.dumps({"error": bad_stage})},
            {"id": "compiler-rejects-non-admissible-stage",
             "ok": bad_admission is not None and "auto-admissible" in bad_admission,
             "detail": json.dumps({"error": bad_admission})},
            {"id": "first-window-tick-emits-exactly-one-command",
             "ok": len(emitted_tick1) == 1 and emitted_tick1[0]["idempotency_key"] == DAY_ONE_KEY
             and event1["source"] == "standing_intent" and event1["event_type"] == "manual_intent"
             and event1["payload"]["intent"] == "run the daily self health check"
             and tick1.get("event", {}).get("status") == "derived_candidate_event",
             "detail": json.dumps({"emitted": emitted_tick1, "derived": tick1.get("event", {}).get("status")})},
            {"id": "green-health-check-completes-at-zero-model-cost",
             "ok": goal_after_tick2 == "completed" and len(calls) == 0,
             "detail": json.dumps({"goal": goal_after_tick2, "model_calls": len(calls)})},
            {"id": "same-window-re-emission-is-deduped",
             "ok": tick3.get("standing_emitted") == [] and tick2.get("standing_emitted") == [],
             "detail": json.dumps({"tick2": tick2.get("standing_emitted"), "tick3": tick3.get("standing_emitted")})},
            {"id": "next-window-emits-new-key",
             "ok": len(tick_day2.get("standing_emitted", [])) == 1
             and tick_day2["standing_emitted"][0]["idempotency_key"] == DAY_TWO_KEY
             and event_day2["source"] == "standing_intent",
             "detail": json.dumps({"emitted": tick_day2.get("standing_emitted")})},
            {"id": "open-goal-blocks-emission",
             "ok": tick_blocked.get("standing_emitted") == [],
             "detail": json.dumps({"emitted": tick_blocked.get("standing_emitted")})},
            {"id": "campaign-without-standing-intents-is-unchanged",
             "ok": tick_plain.get("standing_emitted") == [],
             "detail": json.dumps({"emitted": tick_plain.get("standing_emitted")})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-standing-intent",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/standing_intent_canary.py",
                         "fixtures": "injected clocks and models only; no provider, no network"},
        "known_gaps_open": [
            "a completed stage's NEXT-day goal derives a candidate that the existing re-admission guard routes to a human (goal exists, not re-admissible); completed-goal revival for recurring intents is a later slice",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

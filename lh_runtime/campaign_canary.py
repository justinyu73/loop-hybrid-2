#!/usr/bin/env python3
"""Committed G2 smoke: compile a bounded campaign and reduce stage receipts."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from goal_store import GoalStore


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def campaign() -> dict:
    lamp = {"id": "stage-1-smoke", "smoke": "python3 -B tests/stage-1-smoke.py", "verification_argv": ["python3", "-B", "tests/stage-1-smoke.py"]}
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": "campaign-g2-fixture",
        "stages": [
            {
                "stage_id": "stage-1",
                "goal": {"must_have": ["stage one"], "must_not": ["publish"]},
                "allowed_paths": ["src/"],
                "allowed_side_effects": ["workspace", "artifact"],
                "acceptance_lamp": lamp,
                "max_attempts": 4,
                "next_stage_id": "stage-2",
            },
            {
                "stage_id": "stage-2",
                "goal": {"must_have": ["stage two"], "must_not": ["publish"]},
                "allowed_paths": ["src/"],
                "allowed_side_effects": ["workspace", "artifact"],
                "acceptance_lamp": {"id": "stage-2-smoke", "smoke": "python3 -B tests/stage-2-smoke.py", "verification_argv": ["python3", "-B", "tests/stage-2-smoke.py"]},
                "max_attempts": 4,
                "next_stage_id": None,
            },
            {
                "stage_id": "human-stage",
                "goal": {"must_have": ["human judgment"], "must_not": ["publish"]},
                "allowed_paths": ["docs/"],
                "allowed_side_effects": ["workspace"],
                "human_only": True,
                "next_stage_id": None,
            },
        ],
    }


def main() -> int:
    compiler = CampaignCompiler(campaign())
    compiled = compiler.compile()
    completion = {
        "campaign_id": "campaign-g2-fixture",
        "stage_id": "stage-1",
        "receipt_id": "receipt-1",
        "verification": {"exit_code": 0},
    }
    first = compiler.advance(completion)
    second = compiler.advance(completion)
    with tempfile.TemporaryDirectory() as raw:
        store = GoalStore(Path(raw))
        event = first["event"]
        stored_first = store.record_event(**event)
        stored_second = store.record_event(**event)
        candidate_first = store.create_candidate(
            stored_first["event_key"],
            goal_id=first["candidate"]["goal_id"],
            campaign_id=first["candidate"]["campaign_id"],
            stage_id=first["candidate"]["stage_id"],
            goal=first["candidate"]["goal"],
        )
        candidate_second = store.create_candidate(
            stored_first["event_key"],
            goal_id=first["candidate"]["goal_id"],
            campaign_id=first["candidate"]["campaign_id"],
            stage_id=first["candidate"]["stage_id"],
            goal=first["candidate"]["goal"],
        )
        human_campaign = dict(campaign())
        human_campaign["stages"] = [human_campaign["stages"][0], {**human_campaign["stages"][1], "stage_id": "stage-2", "human_only": True, "acceptance_lamp": None}]
        human_result = CampaignCompiler(human_campaign).advance(completion)
        cases = [
            case("compiler-emits-versioned-envelope", compiled["schema"] == "lh-campaign-admission-envelope/v1" and compiled["digest"].startswith("sha256:"), compiled["digest"]),
            case("green-deterministic-stage-emits-stable-candidate", first["status"] == "candidate_ready" and second["candidate_key"] == first["candidate_key"] and first["event"]["idempotency_key"] == second["event"]["idempotency_key"], first["candidate_key"]),
            case("goal-store-deduplicates-compiled-candidate", stored_first["status"] == "received" and stored_second["status"] == "reused" and candidate_first["status"] == "created" and candidate_second["status"] == "reused" and store.summary()["goal_count"] == 1, str(store.summary())),
            case("human-only-next-stage-does-not-queue", human_result["status"] == "human_required" and human_result["event"] is None, str(human_result)),
            case("non_green_lamp_does_not_queue", compiler.advance({**completion, "verification": {"exit_code": 1}})["status"] == "human_required", "human_required"),
            case("unknown_next_stage_is_rejected", _unknown_stage_rejected(), "unknown stage rejected"),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-campaign-compiler-g2",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/campaign_canary.py", "required_schema": "lh-campaign-admission-envelope/v1"},
        "known_gaps_open": ["G2 compiles policy and emits a candidate event; G3 performs deterministic matching and G4 performs admission/run bridging."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _unknown_stage_rejected() -> bool:
    broken = campaign()
    broken["stages"][0] = {**broken["stages"][0], "next_stage_id": "missing"}
    try:
        CampaignCompiler(broken)
    except ValueError:
        return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())

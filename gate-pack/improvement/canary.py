#!/usr/bin/env python3
"""Reliability canary for the standalone, non-mutating improvement gate."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import improvement_loop as il  # noqa: E402

HERE = Path(__file__).resolve().parent


def result(case_id: str, passed: bool, detail: str) -> dict:
    return {"id": case_id, "ok": passed, "detail": detail}


def main() -> int:
    proposal = il.load_json(HERE / "proposal.example.json")
    executed = il.run_local_shadow(proposal)
    evaluation = il.evaluate_shadow(proposal, executed.get("shadow", {}))
    decision = il.decide(proposal, evaluation)
    regression = copy.deepcopy(executed["shadow"])
    regression["runs"][0]["candidate_pass"] = False
    regression["runs"][0]["falsifier_verdict"] = "fail"
    rejected = il.decide(proposal, il.evaluate_shadow(proposal, regression))
    constitutional = copy.deepcopy(proposal)
    constitutional["target_ref"] = "gate-pack/boundary_seal/enum.json"
    guarded = il.decide(constitutional, il.evaluate_shadow(constitutional, executed["shadow"]))
    cases = [
        result("local-shadow-evidence-authorized", decision["status"] == "evidence_authorized" and not decision["mutation_performed"], decision["status"]),
        result("regression-auto-rejected", rejected["status"] == "automatic_reject_recorded", rejected["status"]),
        result("constitutional-target-waits-human", guarded["status"] == "promotion_waiting_human", guarded["status"]),
    ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "improvement-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "known_gaps_open": ["this gate authorizes evidence only; it never applies or rolls back a candidate"]}, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

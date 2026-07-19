#!/usr/bin/env python3
"""Canary for the independent falsifier: host replay, majority, and triggers."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import independent_falsifier as iff  # noqa: E402
import design_grill as dg  # noqa: E402


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _subject(*, high_risk: bool = True, sample_rate: float = 0.0) -> dict:
    return iff.build_subject(
        subject_ref="fixture-green-regression",
        goal="The feature must preserve the existing health check while adding the requested behavior.",
        lamp={"id": "acceptance-lamp", "output": "PASS", "exit_code": 0},
        diff=("diff --git a/src/feature.py b/src/feature.py\n"
               "new file mode 100644\n--- /dev/null\n+++ b/src/feature.py\n"
               "+def feature(): return True\n"),
        allowed_paths=["src/"],
        receipt_digest="sha256:" + "a" * 64,
        stage_id="feature-stage",
        binding={"runner": "subject-runner", "model": "subject-model"},
        high_risk=high_risk,
        sample_rate=sample_rate,
    )


def _result(binding: dict[str, str], *, kind: str, witness: dict | None, findings: list, refuted: bool = True) -> dict:
    return {
        "schema": iff.RESULT_SCHEMA,
        "subject_ref": "fixture-green-regression",
        "falsifier_binding": binding,
        "findings": findings,
        "witness": witness,
        "refute_kind": kind,
        "refuted": refuted,
        "route": "human_required" if kind != "none" else "continue",
    }


def _failure_witness() -> dict:
    return {"argv": [sys.executable, "-c", "raise SystemExit(3)"], "expected_observation": {"exit_code": 3}}


def _healthy_witness() -> dict:
    return {"argv": [sys.executable, "-c", "print('healthy')"], "expected_observation": {"exit_code": 1}}


def main() -> int:
    subject = _subject()
    bindings = [
        {"runner": "runner-a", "model": "model-a"},
        {"runner": "runner-b", "model": "model-b"},
        {"runner": "runner-c", "model": "model-c"},
    ]
    witnessed = [_result(bindings[0], kind="witnessed", witness=_failure_witness(), findings=["diff:src/feature.py regression observed"]),
                 _result(bindings[1], kind="witnessed", witness=_failure_witness(), findings=["diff:src/feature.py side effect observed"]),
                 _result(bindings[2], kind="none", witness=None, findings=[], refuted=False)]
    majority = iff.evaluate_green(subject, witnessed, cwd=Path.cwd())
    false_positive = iff.evaluate_green(subject, [_result(bindings[0], kind="witnessed", witness=_healthy_witness(), findings=["diff:src/feature.py suspected regression"]),
                                                   _result(bindings[1], kind="none", witness=None, findings=[], refuted=False),
                                                   _result(bindings[2], kind="none", witness=None, findings=[], refuted=False)], cwd=Path.cwd())
    argued = iff.evaluate_green(subject, [_result(bindings[0], kind="argued", witness=None, findings=["goal:original intent requires preserving the health check"]),
                                          _result(bindings[1], kind="none", witness=None, findings=[], refuted=False),
                                          _result(bindings[2], kind="none", witness=None, findings=[], refuted=False)], cwd=Path.cwd())
    no_trigger_subject = _subject(high_risk=False, sample_rate=0.0)
    signature = iff.trigger(no_trigger_subject)["signature"]
    no_trigger = iff.evaluate_green(no_trigger_subject, [], seen_signatures={signature})
    red = copy.deepcopy(subject); red["lamp"]["exit_code"] = 1
    red_result = iff.evaluate_green(red, witnessed, cwd=Path.cwd())
    with tempfile.TemporaryDirectory() as raw, tempfile.TemporaryDirectory() as external:
        prepared = iff.prepare(subject, context_root=Path(external) / "bounded-context", session_dir=Path(raw) / "falsifier-session")
        policy = dg.load_json(HERE.parent / "provider_egress" / "policy.example.json")
        requested = iff.request(Path(raw) / "falsifier-session", bindings=bindings, provider_profiles=["codex_p3"] * 3,
                                requested_at="2026-07-16T10:00:00Z", expires_at="2026-07-16T12:00:00Z", policy=policy)
    cases = [
        case("green-regression-two-witnesses-flips", majority["verdict"] == "human_required" and majority["witnessed_refutes"] == 2 and majority["route"] == "human_required", majority.get("status", "")),
        case("false-positive-witness-is-discarded", false_positive["verdict"] == "GREEN" and false_positive["witnessed_refutes"] == 0 and false_positive["results"][0]["refute_kind"] == "none", false_positive.get("status", "")),
        case("argued-refute-is-consultation-only", argued["verdict"] == "GREEN" and argued["route"] == "human_required" and argued["argued_refutes"] == 1 and argued["consultation_findings"], argued.get("status", "")),
        case("non-triggered-green-skips-falsifier", no_trigger["verdict"] == "GREEN" and not no_trigger["falsifier_ran"] and no_trigger["status"] == "falsifier_not_triggered", no_trigger.get("status", "")),
        case("red-never-runs-falsifier", red_result["verdict"] == "RED" and not red_result["falsifier_ran"], red_result.get("status", "")),
        case("safe-capsule-and-three-separated-bindings", prepared.get("status") == "design_grill_prepared" and requested.get("verdict") == "pass", f"{prepared.get('status')}/{requested.get('status')}"),
        case("process-bound-receipt-is-recorded", bool(majority.get("process_receipts")) and all(receipt["execution"]["claim_level"] == "process_bound" and receipt["execution"]["exit_code"] == 3 for receipt in majority["process_receipts"]), str(majority.get("process_receipts"))),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    print(json.dumps({"check_id": "independent-falsifier-canary", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": ["provider execution and model selection remain host-bound; this canary uses synthetic host responses"],}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

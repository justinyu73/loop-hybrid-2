#!/usr/bin/env python3
"""Committed B13-prerequisite smoke: closed-set semantic diff grader.

Proves, offline (fixture judge callables; one python3 CLI fixture for the
CLI wiring), that the deterministic pre-checks route authority-surface and
oversize diffs to a human without any model call, that a well-formed judge
answer grades routine/sensitive, and that EVERY judge anomaly — outside-set
grade, extra keys, empty or over-long rationale, non-dict output, a raising
judge, no judge at all — degrades to {"grade": "human", "source": "fallback"}
and never to a silent routine. No network, no real model.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from diff_grader import (
    MAX_CHANGED_LINES,
    build_grader_prompt,
    grade_diff,
    make_cli_grader,
    parse_grade,
    validate_grade,
)

CLEAN_DIFF = (
    "diff --git a/src/out.txt b/src/out.txt\n"
    "index 0000000..3be9c81 100644\n"
    "--- a/src/out.txt\n"
    "+++ b/src/out.txt\n"
    "@@ -1 +1 @@\n"
    "-old\n"
    "+new\n"
)
AUTHORITY_DIFF = CLEAN_DIFF.replace("src/out.txt", "gate-pack/verify.sh")
CANARY_DIFF = CLEAN_DIFF.replace("src/out.txt", "lh_runtime/widget_canary.py")
GOVERNANCE_DIFF = CLEAN_DIFF.replace("src/out.txt", "GOVERNANCE.md")
OVERSIZE_DIFF = (
    "diff --git a/src/big.txt b/src/big.txt\n"
    "--- a/src/big.txt\n"
    "+++ b/src/big.txt\n"
    + "".join(f"+line {i}\n" for i in range(MAX_CHANGED_LINES + 1))
)


def _judge(raw: Any):
    def judge(_snapshot: dict) -> Any:
        return raw
    return judge


def _raising_judge(_snapshot: dict) -> Any:
    raise RuntimeError("judge fixture outage")


def main() -> int:
    authority = grade_diff(AUTHORITY_DIFF, judge=_judge({"grade": "routine", "rationale": "x"}))
    canary = grade_diff(CANARY_DIFF, judge=_judge({"grade": "routine", "rationale": "x"}))
    governance = grade_diff(GOVERNANCE_DIFF, judge=_judge({"grade": "routine", "rationale": "x"}))
    oversize = grade_diff(OVERSIZE_DIFF, judge=_judge({"grade": "routine", "rationale": "x"}))
    routine = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "routine", "rationale": "small docs tweak"}))
    sensitive = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "sensitive", "rationale": "touches control flow"}))
    outside_set = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "critical", "rationale": "x"}))
    extra_keys = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "routine", "rationale": "x", "confidence": 0.9}))
    empty_rationale = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "routine", "rationale": "  "}))
    long_rationale = grade_diff(CLEAN_DIFF, judge=_judge({"grade": "routine", "rationale": "x" * 1001}))
    non_dict = grade_diff(CLEAN_DIFF, judge=_judge(["routine"]))
    raised = grade_diff(CLEAN_DIFF, judge=_raising_judge)
    absent = grade_diff(CLEAN_DIFF, judge=None)

    # CLI wiring: a python3 fixture CLI prints the raw grade; parse_grade
    # tolerates surrounding prose.
    cli = make_cli_grader(
        lambda prompt: ["python3", "-c", "import sys; sys.stdout.write('prose {\"grade\": \"routine\", \"rationale\": \"cli ok\"} tail')"],
        name="fixture",
        timeout_seconds=60,
    )
    cli_grade = grade_diff(CLEAN_DIFF, judge=cli)
    bad_cli = make_cli_grader(lambda prompt: ["python3", "-c", "import sys; sys.exit(3)"], name="fixture-bad", timeout_seconds=60)
    bad_cli_grade = grade_diff(CLEAN_DIFF, judge=bad_cli)
    prompt = build_grader_prompt({"schema": "lh-diff-grade/v1", "diff": CLEAN_DIFF, "files_touched": ["src/out.txt"], "changed_lines": 2})
    parsed = parse_grade('noise {"grade": "sensitive", "rationale": "p"}')

    fallbacks = [outside_set, extra_keys, empty_rationale, long_rationale, non_dict, raised, absent, bad_cli_grade]
    cases = [
        {"id": "authority-surface-routes-human-deterministic",
         "ok": authority["grade"] == "human" and authority["source"] == "deterministic" and "gate-pack/verify.sh" in authority["rationale"],
         "detail": json.dumps(authority)},
        {"id": "canary-file-routes-human-deterministic",
             "ok": canary["grade"] == "human" and canary["source"] == "deterministic",
             "detail": json.dumps(canary)},
        {"id": "governance-routes-human-through-shared-predicate",
             "ok": governance["grade"] == "human" and governance["source"] == "deterministic"
             and "GOVERNANCE.md" in governance["rationale"],
             "detail": json.dumps(governance)},
        {"id": "oversize-diff-routes-human-deterministic",
         "ok": oversize["grade"] == "human" and oversize["source"] == "deterministic" and str(MAX_CHANGED_LINES) in oversize["rationale"],
         "detail": json.dumps(oversize)},
        {"id": "clean-diff-judge-routine",
         "ok": routine == {"grade": "routine", "source": "judge", "rationale": "small docs tweak"},
         "detail": json.dumps(routine)},
        {"id": "clean-diff-judge-sensitive",
         "ok": sensitive["grade"] == "sensitive" and sensitive["source"] == "judge",
         "detail": json.dumps(sensitive)},
        {"id": "every-anomaly-falls-back-never-routine",
         "ok": all(item["grade"] == "human" and item["source"] == "fallback" for item in fallbacks),
         "detail": json.dumps([item["rationale"] for item in fallbacks])},
        {"id": "validate-grade-rejects-closed-set-violations",
         "ok": validate_grade({"grade": "routine", "rationale": "ok"})["grade"] == "routine"
         and validate_grade({"grade": "ROUTINE", "rationale": "ok"})["grade"] == "reject"
         and validate_grade({"rationale": "ok"})["grade"] == "reject",
         "detail": "case-sensitive closed set; missing grade rejects"},
        {"id": "cli-grader-roundtrip",
         "ok": cli_grade["grade"] == "routine" and cli_grade["source"] == "judge" and cli_grade["rationale"] == "cli ok",
         "detail": json.dumps(cli_grade)},
        {"id": "prompt-carries-closed-contract",
         "ok": '"routine"' in prompt and '"sensitive"' in prompt and "EXACTLY one JSON object" in prompt
         and parsed == {"grade": "sensitive", "rationale": "p"},
         "detail": json.dumps({"parsed": parsed})},
    ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-diff-grader",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/diff_grader_canary.py",
                         "fixtures": "fixture judge callables + a python3 CLI fixture; no network, no real model"},
        "known_gaps_open": [
            "judge quality (does a real model grade real diffs well) is a live-smoke concern, not provable offline",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

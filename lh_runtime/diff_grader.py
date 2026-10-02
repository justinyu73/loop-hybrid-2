#!/usr/bin/env python3
"""B13 prerequisite: closed-set semantic diff grader (the W6a closed-set extension).

The R2 merge gate must never auto-merge a diff it cannot classify. This
grader answers one bounded question about a run's unified diff inside a
CLOSED grade space:

- ``routine``   — an ordinary in-scope change the gate may auto-merge when
  every other condition holds
- ``sensitive`` — anything subtle; the PR stays for human review
- ``human``     — the grader itself could not grade safely; the PR stays
  for human review

Grading is layered, cheapest and most deterministic first:

1. Deterministic pre-checks (``source="deterministic"``): a diff touching
   the S1 authority surface (same static path classes value_reducer uses)
   or exceeding a changed-line cap is ``human`` without any model call.
2. An optional judge model on the ``models.judge`` layering grades the rest
   inside the closed wire format ``{"grade": "routine"|"sensitive",
   "rationale": "<=1000 chars non-empty>"}``.
3. ANY anomaly — extra keys, an outside-set grade, an empty or over-long
   rationale, non-dict output, a judge that raises, no judge configured —
   degrades to ``{"grade": "human", "source": "fallback"}``. A fallback is
   NEVER silently ``routine``: the cost of a wrong ``sensitive`` grade is a
   human review, the cost of a wrong ``routine`` grade is an unreviewed
   auto-merge, so every doubt resolves toward the human side.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable

import authority_surface
import value_reducer
from cli_agent_executor import run_bounded_judge

GRADE_SCHEMA = "lh-diff-grade/v1"
CLOSED_GRADES = ("routine", "sensitive")
MAX_RATIONALE_CHARS = 1000
# A diff larger than this is not auto-graded at all — it routes to a human.
MAX_CHANGED_LINES = 400
# The judge's view of the diff is bounded, like every other model surface.
DIFF_PROMPT_CHAR_CAP = 16000

# Snapshot in (plain dict, no store handles), raw grade out — the same
# callable shape as the grill-loop judge.
GraderRunner = Callable[[dict[str, Any]], Any]


def _changed_lines(diff_text: str) -> int:
    added = removed = 0
    for line in diff_text.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            added += 1
        elif line.startswith("-") and not line.startswith("---"):
            removed += 1
    return added + removed


def validate_grade(raw: Any) -> dict[str, Any]:
    """Validate raw grader output against the closed grade space.

    Returns ``{"grade": "routine"|"sensitive", "rationale": str}`` or
    ``{"grade": "reject", "reason": str}``. The wire form is exactly
    ``{"grade": <choice>, "rationale": <str>}``; extra keys reject.
    """
    if not isinstance(raw, dict):
        return {"grade": "reject", "reason": "output is not a dict"}
    extra = sorted(set(raw) - {"grade", "rationale"})
    if extra:
        return {"grade": "reject", "reason": f"unexpected keys: {extra}"}
    grade = raw.get("grade")
    if not isinstance(grade, str) or grade not in CLOSED_GRADES:
        return {"grade": "reject", "reason": f"grade outside closed set: {grade!r}"}
    rationale = raw.get("rationale")
    if not isinstance(rationale, str) or not rationale.strip():
        return {"grade": "reject", "reason": "rationale must be a non-empty string"}
    rationale = rationale.strip()
    if len(rationale) > MAX_RATIONALE_CHARS:
        return {"grade": "reject", "reason": f"rationale exceeds {MAX_RATIONALE_CHARS} chars"}
    return {"grade": grade, "rationale": rationale}


def grade_diff(
    diff_text: str | None,
    *,
    judge: GraderRunner | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Grade a unified diff. Always returns {"grade", "source", "rationale"}.

    ``grade`` is one of ``routine`` / ``sensitive`` / ``human``; ``source``
    is ``deterministic`` / ``judge`` / ``fallback``. Only a ``routine`` grade
    lets the merge gate proceed, and only the deterministic checks or a
    well-formed judge answer can produce it.
    """
    text = diff_text if isinstance(diff_text, str) else ""
    touched = value_reducer.touched_files(text) if text else []
    authority = authority_surface.authority_paths(touched)
    if authority:
        return {"grade": "human", "source": "deterministic", "rationale": f"authority surface touched: {authority}"}
    changed = _changed_lines(text)
    if changed > MAX_CHANGED_LINES:
        return {
            "grade": "human",
            "source": "deterministic",
            "rationale": f"diff too large to auto-grade: {changed} changed lines (cap {MAX_CHANGED_LINES})",
        }
    if judge is None:
        return {"grade": "human", "source": "fallback", "rationale": "no judge configured; semantic grading routes to a human"}
    snapshot = {"schema": GRADE_SCHEMA, "diff": text[:DIFF_PROMPT_CHAR_CAP], "files_touched": touched, "changed_lines": changed}
    if isinstance(context, dict):
        snapshot["context"] = dict(context)
        if isinstance(context.get("run_id"), str):
            snapshot["run_id"] = context["run_id"]
    try:
        raw = judge(snapshot)
    except Exception as exc:  # a judge outage is not a grade; route to a human
        return {"grade": "human", "source": "fallback", "rationale": f"judge unavailable: {type(exc).__name__}"}
    verdict = validate_grade(raw)
    if verdict["grade"] == "reject":
        return {"grade": "human", "source": "fallback", "rationale": f"judge output rejected: {verdict['reason']}"}
    return {"grade": verdict["grade"], "source": "judge", "rationale": verdict["rationale"]}


def build_grader_prompt(snapshot: dict[str, Any]) -> str:
    """Prompt for one bounded grader call (M1 model routing).

    The judge sees only the snapshot and the closed output contract — the
    same hard boundaries as every other model surface in the loop.
    """
    return (
        "You are the diff grader in an automated loop. A run produced the "
        "unified diff below (JSON payload with the diff and its stat).\n"
        f"{json.dumps(snapshot, ensure_ascii=False, indent=2)}\n\n"
        "Grade the diff's review risk inside a CLOSED two-choice space:\n"
        '- "routine"   — an ordinary in-scope change a maintainer would wave through\n'
        '- "sensitive" — anything subtle: control flow, concurrency, auth, data-loss\n'
        "  risk, migrations, dependency or config changes, or anything you are unsure about\n"
        "Reply with EXACTLY one JSON object on one line and nothing else:\n"
        '{"grade": "routine", "rationale": "<why this is routine>"}\n'
        '{"grade": "sensitive", "rationale": "<what needs a human eye>"}\n'
        "When in doubt, grade sensitive — a wrong routine grade auto-merges; "
        "a wrong sensitive grade only costs a human review."
    )


def parse_grade(text: str) -> dict[str, Any]:
    """Extract the judge's raw grade from CLI stdout.

    Tolerates surrounding prose by scanning for a JSON object carrying a
    ``grade`` key. Raises ValueError when no such object exists — the caller
    treats that as a reject and routes to a human.
    """
    try:
        value = json.loads(text)
        if isinstance(value, dict) and "grade" in value:
            return value
        if isinstance(value, dict) and "status" in value:
            if value.get("status") != "SUCCESS" or not isinstance(value.get("response"), str):
                raise ValueError("judge JSON envelope is not successful")
            return parse_grade(value["response"])
    except json.JSONDecodeError:
        pass
    for match in re.finditer(r"\{[^{}]*\}", text):
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "grade" in value:
            return value
    raise ValueError("judge output carries no grade JSON")


def make_cli_grader(
    argv_builder: "Callable[[str], list[str]]",
    *,
    name: str,
    timeout_seconds: int = 300,
) -> GraderRunner:
    """Wrap a non-interactive model CLI as a GraderRunner.

    Mirrors the grill-loop judge wiring: the returned callable sends
    build_grader_prompt(snapshot) to the CLI and returns the parsed raw
    grade for validate_grade. Any CLI failure or unparseable output raises —
    grade_diff catches it and routes to a human, so a judge outage never
    stops the loop and never produces a silent ``routine``.
    """

    def grader(snapshot: dict[str, Any]) -> Any:
        prompt = build_grader_prompt(snapshot)
        argv = argv_builder(prompt)
        return parse_grade(run_bounded_judge(argv, name=f"diff grader {name}", timeout_seconds=timeout_seconds))

    return grader

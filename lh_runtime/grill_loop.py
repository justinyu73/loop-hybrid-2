"""W6a grill-loop: challenger judgment before a run's last allowed attempt.

When a sync-path run has burned all but its final attempt (at least two
failures in), the worker offers the run's durable evidence to an optional
challenger — a judge CLI on the ``models.judge`` layering — which answers
inside a CLOSED two-choice decision space:

- ``runner-fixable`` — the executor gets one more attempt with the diagnosis
  injected into its capsule (same executor, same model; guidance only)
- ``goal-broken``    — the final attempt is not spent; the goal routes to a
  human with the diagnosis attached

Anything else — malformed output, extra keys, an over-long or empty
diagnosis, a judge that raises — is rejected and the worker degrades to the
original max_attempts behavior. The grill is advisory everywhere: it never
touches the lamp, scope, allowed paths, or goal content, and its output is
never an acceptance authority. The async external-verdict path is out of
scope (it has no local lamp output to read).
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

from cli_agent_executor import resolve_cli
from turning_point import parse_decision

SNAPSHOT_SCHEMA = "lh-grill-loop/v1"

CLOSED_DECISIONS = ("runner-fixable", "goal-broken")
MAX_DIAGNOSIS_CHARS = 2000
SUMMARY_CHAR_CAP = 500
ARTIFACT_CHAR_CAP = 2000

# Snapshot in (plain dict, no store handles), raw decision out — the same
# callable shape as the turning-point judge.
GrillRunner = Callable[[dict[str, Any]], Any]


def should_grill(run: dict[str, Any]) -> bool:
    """True only when the run's next dispatch is its last allowed attempt and
    at least two attempts have already failed."""
    try:
        attempts = int(run["attempts"])
        max_attempts = int(run["max_attempts"])
    except (KeyError, TypeError, ValueError):
        return False
    return attempts >= 2 and attempts == max_attempts - 1


def _read_capped(path: Path, cap: int) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:cap]
    except OSError:
        return None


def build_snapshot(run_store: Any, run: dict[str, Any]) -> dict[str, Any]:
    """Build the bounded challenger input from the run's durable artifacts.

    Prior attempts contribute their receipt verdict, provider summary, diff,
    and verifier output — every field read from the artifact refs and capped,
    so the judge sees bounded evidence and nothing else.
    """
    prior: list[dict[str, Any]] = []
    for ordinal in range(1, int(run["attempts"]) + 1):
        base = run_store.artifacts / run["run_id"] / str(ordinal)
        exit_code: Any = None
        provider_summary: str | None = None
        raw_receipt = _read_capped(base / "receipt.json", 64 * 1024)
        if raw_receipt is not None:
            try:
                receipt = json.loads(raw_receipt)
            except json.JSONDecodeError:
                receipt = None
            if isinstance(receipt, dict):
                verification = receipt.get("verification")
                if isinstance(verification, dict):
                    exit_code = verification.get("exit_code")
                provider = receipt.get("provider")
                if isinstance(provider, dict) and isinstance(provider.get("summary"), str):
                    provider_summary = provider["summary"][:SUMMARY_CHAR_CAP]
        prior.append({
            "ordinal": ordinal,
            "exit_code": exit_code,
            "provider_summary": provider_summary,
            "diff": _read_capped(base / "diff.patch", ARTIFACT_CHAR_CAP),
            "verifier_stdout": _read_capped(base / "verifier.stdout", ARTIFACT_CHAR_CAP),
            "verifier_stderr": _read_capped(base / "verifier.stderr", ARTIFACT_CHAR_CAP),
        })
    goal = run["goal"] if isinstance(run.get("goal"), dict) else {}
    return {
        "schema": SNAPSHOT_SCHEMA,
        "run_id": run["run_id"],
        "goal_id": goal.get("goal_id"),
        "feature_contract": goal.get("feature_contract"),
        "attempts_used": int(run["attempts"]),
        "max_attempts": int(run["max_attempts"]),
        "next_attempt": int(run["attempts"]) + 1,
        "prior_attempts": prior,
    }


def validate_decision(raw: Any) -> dict[str, Any]:
    """Validate raw challenger output against the closed decision space.

    Returns ``{"type": "runner-fixable"|"goal-broken", "diagnosis": str}`` or
    ``{"type": "reject", "reason": str}`` — a reject degrades to the original
    final-attempt dispatch. The wire form is exactly
    ``{"decision": <choice>, "diagnosis": <str>}``; extra keys reject.
    """
    if not isinstance(raw, dict):
        return {"type": "reject", "reason": "output is not a dict"}
    extra = sorted(set(raw) - {"decision", "diagnosis"})
    if extra:
        return {"type": "reject", "reason": f"unexpected keys: {extra}"}
    decision = raw.get("decision")
    if not isinstance(decision, str) or decision not in CLOSED_DECISIONS:
        return {"type": "reject", "reason": f"decision outside closed set: {decision!r}"}
    diagnosis = raw.get("diagnosis")
    if not isinstance(diagnosis, str) or not diagnosis.strip():
        return {"type": "reject", "reason": "diagnosis must be a non-empty string"}
    diagnosis = diagnosis.strip()
    if len(diagnosis) > MAX_DIAGNOSIS_CHARS:
        return {"type": "reject", "reason": f"diagnosis exceeds {MAX_DIAGNOSIS_CHARS} chars"}
    return {"type": decision, "diagnosis": diagnosis}


def grill_evidence(run_store: Any, run_id: str) -> dict[str, Any] | None:
    """Return the latest grill decision durably recorded for a run, if any."""
    evidence: dict[str, Any] | None = None
    for event in run_store.events(run_id):
        if event.get("event_type") == "grill_decision" and isinstance(event.get("payload"), dict):
            evidence = event["payload"]
    return evidence


def build_judge_prompt(snapshot: dict[str, Any]) -> str:
    """Prompt for one bounded challenger call (M1 model routing).

    The judge sees only the snapshot and the closed output contract — the
    same hard boundaries as every other model surface in the loop.
    """
    return (
        "You are the challenger in an automated loop. A run has repeatedly "
        "failed its acceptance verifier and has exactly one attempt left.\n"
        "Read the durable evidence (JSON):\n"
        f"{json.dumps(snapshot, ensure_ascii=False, indent=2)}\n\n"
        "Decide whether one more executor attempt with your guidance can plausibly "
        "fix the failure, or whether the goal itself is broken (contradictory, "
        "impossible, or mis-specified).\n"
        "Reply with EXACTLY one JSON object on one line and nothing else:\n"
        '{"decision": "runner-fixable", "diagnosis": "<what the executor should do differently>"}\n'
        '{"decision": "goal-broken", "diagnosis": "<why no executor attempt can succeed>"}\n'
        "The diagnosis is guidance only: you may NOT change the verifier, the "
        "scope, the allowed paths, or the goal, and your output is never an "
        "acceptance authority."
    )


def make_cli_judge(
    argv_builder: "Callable[[str], list[str]]",
    *,
    name: str,
    timeout_seconds: int = 300,
) -> GrillRunner:
    """Wrap a non-interactive model CLI as a GrillRunner.

    Mirrors the turning-point judge wiring: the returned callable sends
    build_judge_prompt(snapshot) to the CLI and returns the parsed raw
    decision for validate_decision. Any CLI failure or unparseable output
    raises — the worker catches it and degrades to the original dispatch, so
    a judge outage never stops the loop.
    """

    def grill(snapshot: dict[str, Any]) -> Any:
        prompt = build_judge_prompt(snapshot)
        argv = argv_builder(prompt)
        argv[0] = resolve_cli(argv[0])
        env = dict(os.environ)
        env["PATH"] = f"{Path(argv[0]).parent}:{env.get('PATH', '')}"
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_seconds, env=env)
        if proc.returncode != 0:
            raise RuntimeError(f"grill judge {name} exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        return parse_decision(proc.stdout)

    return grill

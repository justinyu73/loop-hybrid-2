"""H3 bounded turning-point judgment node (GoalHierarchy v1 contract §5).

The node is an *optional* enhancement at the run-selection point: a model may
pick among the already-legal options, nothing more.  It receives a plain
snapshot dict (no store handles), so it structurally cannot admit goals,
widen scope, edit ``depends_on``/``priority``, or touch any gate.  Its raw
output is validated against the closed decision space:

- ``select:<goal_id>``  — the goal must be in the passed runnable set
- ``parent_done``       — legal only when the rollup is already satisfied
                          (a no-op confirm; the deterministic H1 rollup is what
                          actually completes the parent)
- ``human_required``    — routed through the existing human_required path

Anything else — malformed output, an out-of-set select, an admission or
scope/gate attempt — is rejected, and the caller falls back to the H2
deterministic selector.  When the node is disabled the H2 path runs the whole
hierarchy unchanged.  The model's output is never an acceptance authority:
verification lamps and the value gate decide completion exactly as before.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Callable

from cli_agent_executor import resolve_cli

SNAPSHOT_SCHEMA = "lh-turning-point/v1"

CLOSED_DECISIONS = ("select", "parent_done", "human_required")


def build_snapshot(
    eligible: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    rollup_satisfied: bool,
    gate: dict[str, Any],
) -> dict[str, Any]:
    """Build the bounded model input: runnable children plus a gate snapshot.

    ``eligible`` is the H2-filtered, priority-ordered ``(run, goal)`` list.
    The snapshot is plain JSON-shaped data; it carries no store references.
    """
    return {
        "schema": SNAPSHOT_SCHEMA,
        "runnable_children": [
            {
                "goal_id": goal["goal_id"],
                "run_id": run["run_id"],
                "priority": int(goal.get("priority") or 0),
                "depends_on": list(goal.get("depends_on") or []),
            }
            for run, goal in eligible
        ],
        "rollup_satisfied": bool(rollup_satisfied),
        "gate": dict(gate),
    }


def validate_decision(
    raw: Any,
    *,
    runnable_goal_ids: list[str],
    rollup_satisfied: bool,
) -> dict[str, Any]:
    """Validate raw model output against the closed decision space.

    Returns one of::

        {"type": "select", "goal_id": <id>}   # id in runnable_goal_ids
        {"type": "parent_done"}               # rollup already satisfied
        {"type": "human_required"}
        {"type": "reject", "reason": <str>}   # caller falls back to H2

    The expected wire form is ``{"decision": "select:<goal_id>"}``,
    ``{"decision": "parent_done"}`` or ``{"decision": "human_required"}``.
    """
    if not isinstance(raw, dict):
        return {"type": "reject", "reason": "output is not a dict"}
    decision = raw.get("decision")
    if not isinstance(decision, str):
        return {"type": "reject", "reason": "missing decision string"}
    if decision == "parent_done":
        if not rollup_satisfied:
            return {"type": "reject", "reason": "parent_done before rollup is satisfied"}
        return {"type": "parent_done"}
    if decision == "human_required":
        return {"type": "human_required"}
    if decision.startswith("select:"):
        goal_id = decision[len("select:"):].strip()
        if goal_id and goal_id in runnable_goal_ids:
            return {"type": "select", "goal_id": goal_id}
        return {"type": "reject", "reason": f"select target not in runnable set: {goal_id or '<empty>'}"}
    return {"type": "reject", "reason": f"decision outside closed set: {decision}"}


def build_judge_prompt(snapshot: dict[str, Any]) -> str:
    """Prompt for one bounded judgment call (M1 model routing).

    The judge sees only the snapshot and the closed output contract — the
    same hard boundaries as every other model surface in the loop.
    """
    runnable = [child["goal_id"] for child in snapshot.get("runnable_children", [])]
    return (
        "You are the bounded turning-point judge in an automated loop.\n"
        "Pick the next step from this snapshot (JSON):\n"
        f"{json.dumps(snapshot, ensure_ascii=False, indent=2)}\n\n"
        "Reply with EXACTLY one JSON object on one line and nothing else:\n"
        '{"decision": "select:<goal_id>"} — <goal_id> must be one of: '
        + (", ".join(runnable) if runnable else "(empty runnable set)")
        + '\n{"decision": "human_required"} — only when a human must judge.\n'
        "You may NOT admit new goals, change priorities/dependencies, declare "
        "completion, or mention any goal outside the runnable set."
    )


def parse_decision(text: str) -> dict[str, Any]:
    """Extract the judge's raw decision from CLI stdout.

    Tolerates surrounding prose by scanning for a JSON object carrying a
    ``decision`` key. Raises ValueError when no such object exists — the
    caller treats that as a reject and falls back to deterministic routing.
    """
    try:
        value = json.loads(text)
        if isinstance(value, dict) and "decision" in value:
            return value
    except json.JSONDecodeError:
        pass
    for match in re.finditer(r"\{[^{}]*\}", text):
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "decision" in value:
            return value
    raise ValueError("judge output carries no decision JSON")


def make_cli_judge(
    argv_builder: "Callable[[str], list[str]]",
    *,
    name: str,
    timeout_seconds: int = 300,
) -> "Callable[[dict[str, Any]], Any]":
    """Wrap a non-interactive model CLI as a TurningPointRunner.

    The returned callable sends build_judge_prompt(snapshot) to the CLI and
    returns the parsed raw decision for validate_decision. Any CLI failure
    or unparseable output raises — the worker catches it and falls back to
    the deterministic H2 selector, so a judge outage never stops the loop.
    """

    def judge(snapshot: dict[str, Any]) -> Any:
        prompt = build_judge_prompt(snapshot)
        argv = argv_builder(prompt)
        argv[0] = resolve_cli(argv[0])
        env = dict(os.environ)
        env["PATH"] = f"{Path(argv[0]).parent}:{env.get('PATH', '')}"
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_seconds, env=env)
        if proc.returncode != 0:
            raise RuntimeError(f"judge {name} exited {proc.returncode}: {proc.stderr.strip()[:400]}")
        return parse_decision(proc.stdout)

    return judge

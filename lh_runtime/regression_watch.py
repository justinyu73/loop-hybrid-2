#!/usr/bin/env python3
"""Regression watch: look at completed goals again, and raise a regression instead of repairing it.

A goal that reached ``completed`` used to stay done for good, so a later change
that broke its acceptance lamp went unnoticed.  ``sweep`` re-runs the lamp of
completed goals against the current source HEAD:

- only goals whose current revision carries an acceptance lamp are looked at,
  the longest-unchecked first, at most ``max_goals`` per pass, and a goal is not
  looked at again within ``min_interval_seconds``;
- each lamp runs in a disposable workspace from the same ``WorkspacePort`` the
  controller uses, through a process port, under a timeout; the workspace is
  removed afterwards and nothing is written to the source;
- green moves only the last-checked state (``regression-watch.json`` beside the
  goal store); red records one ``regression_detected`` event parked in
  ``human_required``, keyed by goal, source HEAD and lamp, so the same
  regression is raised once; a lamp that cannot be launched or does not finish
  is ``unknown``, neither red nor green.

A regression never reopens the goal and never creates a run: it is an open
question for the owner, who may re-issue the goal through the usual command.

    python3 lh_runtime/regression_watch.py --goal-store <dir> --source-repo <repo> --workspace-root <dir>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

try:
    from .goal_store import GoalStore
    from .platform_ports import LocalProcessPort, ProcessPort, ProcessTimeout, remove_tree
    from .workspace_port import GitCloneWorkspace, WorkspacePort, WorkspaceRequest
except ImportError:  # direct execution keeps lh_runtime on sys.path
    from goal_store import GoalStore  # type: ignore
    from platform_ports import LocalProcessPort, ProcessPort, ProcessTimeout, remove_tree  # type: ignore
    from workspace_port import GitCloneWorkspace, WorkspacePort, WorkspaceRequest  # type: ignore

SCHEMA = "lh-regression-watch/v1"
STATE_SCHEMA = "lh-regression-watch-state/v1"
EVENT_SOURCE = "regression_watch"
EVENT_TYPE = "regression_detected"
REASON = "regression_detected"
HOLDER = "regression-watch"
DEFAULT_MAX_GOALS = 5
DEFAULT_MIN_INTERVAL_SECONDS = 24 * 3600.0
DEFAULT_TIMEOUT_SECONDS = 600.0


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def validate_config(raw: Any) -> dict[str, Any]:
    """The contract's ``regression_watch`` block: max_goals, min_interval_seconds, timeout_seconds."""
    if not isinstance(raw, dict):
        raise ValueError("regression_watch must be an object")
    unknown = set(raw) - {"max_goals", "min_interval_seconds", "timeout_seconds"}
    if unknown:
        raise ValueError(f"regression_watch has unknown fields: {sorted(unknown)}")
    config = {
        "max_goals": raw.get("max_goals", DEFAULT_MAX_GOALS),
        "min_interval_seconds": raw.get("min_interval_seconds", DEFAULT_MIN_INTERVAL_SECONDS),
        "timeout_seconds": raw.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS),
    }
    if isinstance(config["max_goals"], bool) or not isinstance(config["max_goals"], int) or config["max_goals"] < 1:
        raise ValueError("regression_watch.max_goals must be a positive integer")
    for field in ("min_interval_seconds", "timeout_seconds"):
        value = config[field]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"regression_watch.{field} must be a positive number")
        config[field] = float(value)
    return config


def _lamp(goal: dict[str, Any]) -> list[str] | None:
    revision = goal.get("current_revision") if isinstance(goal.get("current_revision"), dict) else {}
    body = revision.get("goal") if isinstance(revision.get("goal"), dict) else {}
    envelope = body.get("admission_envelope") if isinstance(body.get("admission_envelope"), dict) else {}
    lamp = envelope.get("acceptance_lamp") if isinstance(envelope.get("acceptance_lamp"), dict) else {}
    argv = lamp.get("verification_argv")
    if isinstance(argv, list) and argv and all(isinstance(item, str) and item.strip() for item in argv):
        return list(argv)
    return None


def _state_path(store: GoalStore) -> Path:
    return Path(store.root) / "regression-watch.json"


def _load_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"schema": STATE_SCHEMA, "goals": {}}
    if not isinstance(data, dict) or data.get("schema") != STATE_SCHEMA or not isinstance(data.get("goals"), dict):
        return {"schema": STATE_SCHEMA, "goals": {}}
    return data


def _git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False)


def _layout(source_repo: Path) -> tuple[Path, Path, str]:
    source = Path(source_repo).resolve()
    top = _git("-C", str(source), "rev-parse", "--show-toplevel")
    head = _git("-C", str(source), "rev-parse", "HEAD")
    if top.returncode != 0 or head.returncode != 0:
        raise ValueError(f"source repo is not a readable git checkout: {source}")
    git_root = Path(top.stdout.strip()).resolve()
    return git_root, source.relative_to(git_root), head.stdout.strip()


def _check_one(goal_id: str, argv: list[str], *, git_root: Path, subdir: Path, head: str, workspace_root: Path,
               process_port: ProcessPort, workspace_port: WorkspacePort, timeout: float) -> dict[str, Any]:
    check = getattr(process_port, "launch_unavailable", None)
    unready = check(argv) if callable(check) else None
    if unready is not None:
        return {"outcome": "unknown", "reason": f"verifier_unavailable:{unready}"}
    nonce = hashlib.sha256(f"{goal_id}:{head}:{time.time_ns()}".encode()).hexdigest()[:16]

    def run(command: list[str]):
        return process_port.run(command, timeout=timeout)

    prepared = None
    try:
        prepared = workspace_port.prepare(WorkspaceRequest(
            run_id=f"regression-{nonce}", attempt=1, base_revision=head, source_root=git_root,
            target_subdir=subdir, workspace_root=workspace_root, run=run,
        ))
        try:
            completed = process_port.run(argv, cwd=prepared.workspace, timeout=timeout)
        except ProcessTimeout:
            return {"outcome": "unknown", "reason": "lamp_timed_out"}
        except OSError as exc:
            if isinstance(exc, TimeoutError):
                raise
            return {"outcome": "unknown", "reason": f"verifier_unavailable:launch_failed:{type(exc).__name__}"}
        return {"outcome": "green" if completed.returncode == 0 else "red", "exit_code": completed.returncode,
                "stderr_tail": completed.stderr[-400:]}
    except (RuntimeError, ProcessTimeout) as exc:
        return {"outcome": "unknown", "reason": f"workspace_unavailable: {str(exc)[:200]}"}
    finally:
        if prepared is not None:
            remove_tree(prepared.git_root)
        remove_tree(workspace_root / f"regression-{nonce}")


def _raise(store: GoalStore, goal: dict[str, Any], argv: list[str], head: str, checked: dict[str, Any]) -> dict[str, Any]:
    goal_id = goal["goal_id"]
    lamp_digest = _digest(argv)
    key = f"regression:{goal_id}:{head}:{lamp_digest[7:23]}"
    payload = {"goal_id": goal_id, "revision_id": goal.get("current_revision_id"), "source_head": head,
               "lamp_digest": lamp_digest, "exit_code": checked.get("exit_code")}
    recorded = store.record_event(event_id=key, idempotency_key=key, source=EVENT_SOURCE, event_type=EVENT_TYPE,
                                  payload=payload)
    if recorded.get("status") == "reused":
        return {"event_key": key, "status": "already_raised"}
    if store.claim_event(key, HOLDER):
        try:
            store.transition_event(key, "human_required", result={
                "reason": f"{REASON}:{goal_id}", "goal_id": goal_id, "source_head": head,
                "exit_code": checked.get("exit_code"), "stderr_tail": checked.get("stderr_tail", ""),
            })
        finally:
            store.release_event(key, HOLDER)
    return {"event_key": key, "status": "raised"}


def sweep(
    goal_store: GoalStore,
    *,
    source_repo: str | Path,
    workspace_root: str | Path,
    max_goals: int = DEFAULT_MAX_GOALS,
    min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    now: float | None = None,
    process_port: ProcessPort | None = None,
    workspace_port: WorkspacePort | None = None,
) -> dict[str, Any]:
    validate_config({"max_goals": max_goals, "min_interval_seconds": min_interval_seconds,
                     "timeout_seconds": timeout_seconds})
    now = time.time() if now is None else float(now)
    process_port = process_port or LocalProcessPort()
    workspace_port = workspace_port or GitCloneWorkspace()
    root = Path(workspace_root).resolve()
    git_root, subdir, head = _layout(Path(source_repo))
    state_path = _state_path(goal_store)
    state = _load_state(state_path)
    seen = state["goals"]
    due = []
    skipped = []
    for goal in goal_store.goals_in_state("completed"):
        argv = _lamp(goal)
        if argv is None:
            skipped.append({"goal_id": goal["goal_id"], "reason": "no_acceptance_lamp"})
            continue
        last = seen.get(goal["goal_id"], {}).get("checked_at")
        if isinstance(last, (int, float)) and now - float(last) < min_interval_seconds:
            skipped.append({"goal_id": goal["goal_id"], "reason": "checked_recently"})
            continue
        due.append((float(last) if isinstance(last, (int, float)) else float("-inf"), goal["goal_id"], goal, argv))
    due.sort(key=lambda row: (row[0], row[1]))
    checked = []
    for _last, goal_id, goal, argv in due[:max_goals]:
        result = _check_one(goal_id, argv, git_root=git_root, subdir=subdir, head=head, workspace_root=root,
                            process_port=process_port, workspace_port=workspace_port, timeout=timeout_seconds)
        row = {"goal_id": goal_id, "source_head": head, **result}
        if result["outcome"] == "red":
            row["regression"] = _raise(goal_store, goal, argv, head, result)
        checked.append(row)
        seen[goal_id] = {"checked_at": now, "source_head": head, "outcome": result["outcome"]}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"schema": SCHEMA, "source_head": head, "checked": checked, "skipped": skipped,
            "deferred": [row[1] for row in due[max_goals:]]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Re-run the acceptance lamps of completed goals")
    parser.add_argument("--goal-store", required=True)
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--max-goals", type=int, default=DEFAULT_MAX_GOALS)
    parser.add_argument("--min-interval-seconds", type=float, default=DEFAULT_MIN_INTERVAL_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    result = sweep(GoalStore(args.goal_store), source_repo=args.source_repo, workspace_root=args.workspace_root,
                   max_goals=args.max_goals, min_interval_seconds=args.min_interval_seconds,
                   timeout_seconds=args.timeout_seconds)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

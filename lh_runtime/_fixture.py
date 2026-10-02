"""Shared construction helpers for LH canary fixtures.

These helpers only BUILD fixture objects (a git source repo, a campaign dict,
a goal candidate row). They never assert: each canary keeps its own cases and
expectations, so no canary's verdict can depend on another canary's behavior.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence

from campaign_compiler import CAMPAIGN_SCHEMA
from execution_fence import ExecutionFencePort, build_attempt_binding, digest_json
from goal_store import GoalStore


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


class FixtureExecutionFencePort(ExecutionFencePort):
    """Explicit canary-only launch seam for pre-existing adapter unit tests."""

    def prepare(self, binding: Mapping[str, Any]) -> dict[str, Any]:
        body = {
            "schema": "lh-fixture-execution-fence/v1",
            "binding": dict(binding),
        }
        return {**body, "launch_descriptor_digest": digest_json(body)}

    def launch(
        self,
        descriptor: Mapping[str, Any],
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float,
        env_projection: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        binding = descriptor.get("binding")
        cwd = (
            Path(binding["clone_root"])
            if isinstance(binding, dict)
            and isinstance(binding.get("clone_root"), str)
            else None
        )
        environment = dict(os.environ)
        environment.update(env_projection or {})
        return subprocess.run(
            list(argv),
            cwd=cwd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=environment,
        )

    def receipt_projection(
        self,
        descriptor: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema": "lh-fixture-execution-fence/v1",
            "status": "fixture_only",
            "launch_descriptor_digest": descriptor[
                "launch_descriptor_digest"
            ],
        }


def capsule_with_fence(
    port: ExecutionFencePort,
    workspace: Path,
    capsule: Mapping[str, Any],
    *,
    adapter_id: str = "fixture-adapter",
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    """Attach an explicitly prepared descriptor; adapters never self-prepare."""
    binding = build_attempt_binding(
        goal=capsule["goal"] if isinstance(capsule.get("goal"), dict) else {},
        run_id=str(capsule.get("run_id") or "fixture-run"),
        attempt=int(capsule["attempt"]) if isinstance(capsule.get("attempt"), int) else 1,
        attempt_fence=int(capsule["fence"]) if isinstance(capsule.get("fence"), int) else 1,
        base_revision=str(capsule.get("base_revision") or "fixture-base"),
        clone_root=workspace,
        verifier_argv=["fixture-direct-adapter"],
        adapter_id=adapter_id,
        adapter_version="v1",
        timeout_seconds=timeout_seconds,
    )
    return {**capsule, "execution_fence": port.prepare(binding)}


def make_source_repo(root: Path, *, name: str = "source", user: str = "fixture") -> tuple[Path, str]:
    """Create a git repo with one baseline commit; return (path, base revision)."""
    source = root / name
    source.mkdir(parents=True, exist_ok=True)
    _git("init", "-q", str(source))
    _git("-C", str(source), "config", "user.email", f"{user}@example.invalid")
    _git("-C", str(source), "config", "user.name", f"{user} canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8", newline="")
    _git("-C", str(source), "add", "baseline.txt")
    _git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    return source, base


def make_campaign(
    campaign_id: str = "campaign-fixture",
    *,
    stage_id: str = "stage-1",
    next_stage_id: str | None = None,
) -> dict:
    """One bounded campaign dict: a single stage, or two when next_stage_id is set."""

    def stage(sid: str, nxt: str | None) -> dict:
        return {
            "stage_id": sid,
            "goal": {"feature_contract": sid},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {
                "id": f"{sid}-lamp",
                "smoke": "a staged change exists",
                "verification_argv": ["sh", "-c", "! git diff --cached --quiet"],
            },
            "max_attempts": 4,
            "next_stage_id": nxt,
        }

    stages = [stage(stage_id, next_stage_id)]
    if next_stage_id is not None:
        stages.append(stage(next_stage_id, None))
    return {"schema": CAMPAIGN_SCHEMA, "campaign_id": campaign_id, "stages": stages}


def make_goal(
    store: GoalStore,
    goal_id: str,
    *,
    campaign_id: str,
    stage_id: str = "stage-1",
    parent_goal_id: str | None = None,
    depends_on: list[str] | None = None,
    priority: int = 0,
) -> dict[str, object]:
    """Record one intent event and create the matching candidate; returns the stored row."""
    event_key = f"fixture-event:{goal_id}"
    store.record_event(
        event_id=f"evt-{goal_id}",
        idempotency_key=event_key,
        source="manual",
        event_type="manual_intent",
        payload={"campaign_id": campaign_id, "goal_id": goal_id},
    )
    return store.create_candidate(
        event_key,
        goal_id=goal_id,
        campaign_id=campaign_id,
        stage_id=stage_id,
        goal={"must_have": [goal_id]},
        parent_goal_id=parent_goal_id,
        depends_on=depends_on,
        priority=priority,
    )

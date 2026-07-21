"""G4 bounded candidate admission and deterministic Goal-to-Run bridge."""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from goal_store import GoalStore
from run_store import RunStore


ENVELOPE_SCHEMA = "lh-campaign-admission-envelope/v1"
FORBIDDEN_SIDE_EFFECTS = {"push", "merge", "publish", "external_action", "credential"}


def _id(prefix: str, value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return prefix + hashlib.sha256(raw).hexdigest()[:32]


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _pin_base_revision(source_repo: Path, base_revision: str) -> str | None:
    """Resolve a (possibly moving) ref name to its commit SHA at admission time.

    Pinning at admission closes the time-of-check/time-of-use gap between
    admitting a run and cloning the workspace later: the run record carries
    the exact commit, not a branch name that can drift. Returns None when the
    ref cannot be resolved (not a git repo, unknown ref)."""
    proc = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "--verify", f"{base_revision}^{{commit}}"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


class GoalAdmissionBridge:
    """Admission policy for G4; the controller remains unchanged."""

    def __init__(self, goal_store: GoalStore, run_store: RunStore):
        self.goal_store = goal_store
        self.run_store = run_store

    @staticmethod
    def _reasons(envelope: Any, source_repo: Path, base_revision: str, verification_argv: list[str] | None, max_attempts: int | None) -> list[str]:
        reasons: list[str] = []
        if not isinstance(envelope, dict) or envelope.get("schema") != ENVELOPE_SCHEMA:
            reasons.append("invalid_admission_envelope")
            return reasons
        if envelope.get("human_only") is True:
            reasons.append("human_only_stage")
        auto = envelope.get("auto_admission")
        if not isinstance(auto, dict) or auto.get("eligible") is not True:
            reasons.append("envelope_not_auto_eligible")
        allowed_paths = envelope.get("allowed_paths")
        if not isinstance(allowed_paths, list) or not allowed_paths or any(not isinstance(path, str) or not path.strip() for path in allowed_paths):
            reasons.append("invalid_allowed_paths")
        side_effects = envelope.get("allowed_side_effects")
        if not isinstance(side_effects, list) or any(not isinstance(item, str) or not item.strip() for item in side_effects):
            reasons.append("invalid_allowed_side_effects")
        elif sorted(set(side_effects) & FORBIDDEN_SIDE_EFFECTS):
            reasons.append("forbidden_external_side_effect")
        lamp = envelope.get("acceptance_lamp")
        lamp_ok = isinstance(lamp, dict) and isinstance(lamp.get("verification_argv"), list) and bool(lamp["verification_argv"])
        external = envelope.get("external_verdict")
        external_ok = isinstance(external, dict) and isinstance(external.get("action_id"), str) and bool(external["action_id"].strip())
        if not lamp_ok and not external_ok:
            reasons.append("missing_acceptance_lamp")
        if not source_repo.exists() or not source_repo.is_dir():
            reasons.append("source_repo_unavailable")
        if not isinstance(base_revision, str) or not base_revision.strip():
            reasons.append("missing_base_revision")
        attempts = envelope.get("max_attempts") if max_attempts is None else max_attempts
        if not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= 4:
            reasons.append("invalid_attempt_budget")
        if verification_argv is not None and (not isinstance(verification_argv, list) or any(not isinstance(item, str) or not item.strip() for item in verification_argv)):
            reasons.append("invalid_verification_argv")
        return reasons

    def admit(
        self,
        goal_id: str,
        *,
        source_repo: str | Path,
        base_revision: str,
        envelope: dict[str, Any],
        verification_argv: list[str] | None = None,
        max_attempts: int | None = None,
    ) -> dict[str, Any]:
        goal_id = _text("goal_id", goal_id)
        source_repo = Path(source_repo)
        base_revision = _text("base_revision", base_revision)
        goal = self.goal_store.get_goal(goal_id)
        revision = goal.get("current_revision")
        revision_id = revision.get("revision_id") if isinstance(revision, dict) else None
        if not isinstance(revision_id, str) or not revision_id:
            raise ValueError("candidate has no current revision")
        reasons = self._reasons(envelope, source_repo, base_revision, verification_argv, max_attempts)
        if reasons:
            if goal["state"] == "candidate":
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="candidate")
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": reasons}
        if goal["state"] not in {"candidate", "active"}:
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": [f"goal_state_{goal['state']}"]}
        pinned_revision = _pin_base_revision(source_repo, base_revision)
        if pinned_revision is None:
            if goal["state"] == "candidate":
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="candidate")
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": ["unresolvable_base_revision"]}
        attempts = envelope["max_attempts"] if max_attempts is None else max_attempts
        run_id = _id("run-goal-", {"goal_id": goal_id, "revision_id": revision_id, "base_revision": pinned_revision, "envelope": envelope})
        run_goal = {
            "schema": "lh-goal-run/v1",
            "goal_id": goal_id,
            "revision_id": revision_id,
            "campaign_id": goal["campaign_id"],
            "stage_id": goal["stage_id"],
            "admission_envelope": envelope,
        }
        if pinned_revision != base_revision:
            run_goal["base_ref"] = base_revision
        existing_run = None
        try:
            existing_run = self.run_store.get_run(run_id)
        except KeyError:
            existing_run = None
        if existing_run is not None and existing_run["state"] in {"stopped", "verified"}:
            # Revision-bump: a terminal run is never re-linked. A new command
            # re-issues the work as revision N+1 (new deterministic run_id,
            # old run kept as history).
            if existing_run["state"] == "stopped":
                # Failure loop: the revision cap turns an endless fail-retry
                # loop into human_required (unchanged semantics).
                try:
                    bumped = self.goal_store.bump_revision(goal_id)
                except ValueError:
                    return {
                        "status": "human_required",
                        "goal_id": goal_id,
                        "run_id": None,
                        "reasons": ["revision_cap_reached"],
                    }
            else:
                # W9g: success cycle. A VERIFIED run must never be re-linked
                # into a revived goal (the W9f day-1 bug: the old verified run
                # was re-linked and its stale receipt consumed). Recurring
                # after a success is a fresh cycle, not a fail-retry loop, so
                # the fail-loop revision cap does not apply here.
                bumped = self._bump_revision_after_success(goal_id)
            revision_id = bumped["revision_id"]
            run_id = _id("run-goal-", {"goal_id": goal_id, "revision_id": revision_id, "base_revision": pinned_revision, "envelope": envelope})
            run_goal["revision_id"] = revision_id
        self.run_store.create_run(goal=run_goal, source_repo=source_repo, base_revision=pinned_revision, max_attempts=attempts, run_id=run_id)
        linked = self.goal_store.activate_with_run(goal_id, run_id)
        return {
            "status": "reused" if goal["state"] == "active" else "active",
            "goal_id": goal_id,
            "revision_id": revision_id,
            "run_id": run_id,
            "run_state": self.run_store.get_run(run_id)["state"],
            "goal_state": linked["state"],
        }

    def _bump_revision_after_success(self, goal_id: str) -> dict[str, Any]:
        """W9g: revision-bump after a VERIFIED run, without the fail-loop cap.

        MAX_GOAL_REVISIONS exists to stop endless fail-retry loops; a verified
        run is a completed success cycle, so recurring (e.g. a daily standing
        health check) starts a fresh cycle and the cap does not apply. Mirrors
        GoalStore.bump_revision's write path against the same store; kept here
        because the capped variant lives in GoalStore and the two policies
        must stay visibly separate."""
        now = time.time()
        with self.goal_store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT current_revision_id FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown goal_id: {goal_id}")
                current = conn.execute("SELECT * FROM goal_revisions WHERE revision_id = ?", (row["current_revision_id"],)).fetchone()
                if current is None:
                    raise ValueError("goal has no current revision to bump")
                next_seq = int(current["revision"]) + 1
                goal_payload = json.loads(current["goal_json"])
                revision_id = _id("rev-", {"goal_id": goal_id, "revision": next_seq, "goal": goal_payload})
                conn.execute(
                    "INSERT INTO goal_revisions(revision_id, goal_id, revision, goal_json, goal_digest, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (revision_id, goal_id, next_seq, current["goal_json"], current["goal_digest"], now),
                )
                conn.execute("UPDATE goals SET current_revision_id = ?, updated_at = ? WHERE goal_id = ?", (revision_id, now, goal_id))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {"goal_id": goal_id, "revision": next_seq, "revision_id": revision_id}

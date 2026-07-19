"""Deterministic Goal event matcher and candidate reducer for G3."""
from __future__ import annotations

import hashlib
import json
from typing import Any


MATCH_RESULT_SCHEMA = "lh-goal-match-result/v1"
ROUTES = {"bind", "candidate", "stale", "conflict", "human_required"}


def _id(prefix: str, value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return prefix + hashlib.sha256(raw).hexdigest()[:32]


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


class GoalMatcher:
    """Reduce structured events against an active-goal snapshot.

    Matching is explicit and deterministic.  The matcher never infers intent,
    changes GoalStore state, or admits a candidate.
    """

    def __init__(self, active_goals: list[dict[str, Any]]):
        if not isinstance(active_goals, list):
            raise ValueError("active_goals must be a list")
        for index, goal in enumerate(active_goals):
            if not isinstance(goal, dict) or goal.get("state") != "active":
                raise ValueError(f"active_goals[{index}] must be an active Goal object")
        self.active_goals = active_goals

    @staticmethod
    def _event(event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        if not isinstance(event, dict):
            raise ValueError("event must be an object")
        event_key = event.get("event_key") or event.get("idempotency_key") or event.get("event_id")
        event_key = _text("event_key", event_key)
        payload = event.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("event.payload must be an object")
        return event_key, payload

    @staticmethod
    def _revision(goal: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
        revision = goal.get("current_revision")
        if not isinstance(revision, dict):
            return None, {}
        value = revision.get("revision_id")
        return value if isinstance(value, str) else None, revision.get("goal", {}) if isinstance(revision.get("goal", {}), dict) else {}

    @staticmethod
    def _result(event_key: str, route: str, *, reason: str, goal: dict[str, Any] | None = None, candidate: dict[str, Any] | None = None, candidate_key: str | None = None) -> dict[str, Any]:
        if route not in ROUTES:
            raise ValueError(f"unsupported matcher route: {route}")
        revision_id = None
        goal_id = None
        if goal is not None:
            goal_id = goal.get("goal_id")
            revision_id, _ = GoalMatcher._revision(goal)
        return {
            "schema": MATCH_RESULT_SCHEMA,
            "event_key": event_key,
            "route": route,
            "goal_id": goal_id,
            "revision_id": revision_id,
            "candidate_key": candidate_key,
            "candidate": candidate,
            "reason": reason,
        }

    def _by_goal_id(self, goal_id: str) -> list[dict[str, Any]]:
        return [goal for goal in self.active_goals if goal.get("goal_id") == goal_id]

    def _candidate(self, event_key: str, candidate: Any, *, reason: str) -> dict[str, Any]:
        if not isinstance(candidate, dict):
            return self._result(event_key, "human_required", reason="candidate payload is missing")
        for name in ("goal_id", "campaign_id", "stage_id", "goal"):
            if name not in candidate:
                return self._result(event_key, "human_required", reason=f"candidate missing {name}")
        if not isinstance(candidate["goal"], dict):
            return self._result(event_key, "human_required", reason="candidate.goal must be an object")
        return self._result(
            event_key,
            "candidate",
            reason=reason,
            candidate=candidate,
            candidate_key=_id("candidate-", event_key),
        )

    def reduce(self, event: dict[str, Any]) -> dict[str, Any]:
        event_key, payload = self._event(event)
        if payload.get("scope_widening") is True or payload.get("promotion_required") is True:
            return self._result(event_key, "human_required", reason="scope or promotion is outside the matcher envelope")

        explicit_goal_id = payload.get("goal_id")
        if explicit_goal_id is not None:
            explicit_goal_id = _text("payload.goal_id", explicit_goal_id)
            matches = self._by_goal_id(explicit_goal_id)
            if len(matches) != 1:
                return self._result(event_key, "conflict" if len(matches) > 1 else "human_required", reason="explicit goal is not uniquely active")
            goal = matches[0]
            current_revision_id, _ = self._revision(goal)
            requested_revision_id = payload.get("revision_id")
            if requested_revision_id is not None and requested_revision_id != current_revision_id:
                return self._result(event_key, "stale", reason="event revision does not match active revision", goal=goal)
            return self._result(event_key, "bind", reason="explicit goal and revision match", goal=goal)

        fingerprint = payload.get("failure_fingerprint")
        if fingerprint is not None:
            fingerprint = _text("payload.failure_fingerprint", fingerprint)
            matches = []
            for goal in self.active_goals:
                _, goal_payload = self._revision(goal)
                fingerprints = goal_payload.get("failure_fingerprints", [])
                if isinstance(fingerprints, list) and fingerprint in fingerprints:
                    matches.append(goal)
            if len(matches) == 1:
                return self._result(event_key, "bind", reason="failure fingerprint matches one active goal", goal=matches[0])
            if len(matches) > 1:
                return self._result(event_key, "conflict", reason="failure fingerprint matches multiple active goals")

        candidate = payload.get("candidate")
        if candidate is not None:
            candidate_goal_id = candidate.get("goal_id") if isinstance(candidate, dict) else None
            if isinstance(candidate_goal_id, str):
                matches = self._by_goal_id(candidate_goal_id)
                if len(matches) > 1:
                    return self._result(event_key, "conflict", reason="candidate maps to multiple active goals")
                if len(matches) == 1:
                    requested_revision_id = candidate.get("revision_id") or payload.get("revision_id")
                    current_revision_id, _ = self._revision(matches[0])
                    if requested_revision_id is not None and requested_revision_id != current_revision_id:
                        return self._result(event_key, "stale", reason="candidate revision is stale", goal=matches[0])
                    return self._result(event_key, "bind", reason="candidate maps to existing active goal", goal=matches[0])
            return self._candidate(event_key, candidate, reason="no active goal matches candidate")

        return self._result(event_key, "human_required", reason="event has no deterministic linkage or candidate")

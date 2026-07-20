"""Durable Goal event and lifecycle state for the native LH loop.

This module is the G1 storage primitive.  It records stable input events,
candidate goals, immutable goal revisions, and idempotent processing claims.
It deliberately does not match natural language, compile campaigns, admit a
candidate, or create a RunStore run; those policies belong to later nodes.

H1 (GoalHierarchy v1, docs/contracts/goal-hierarchy-v1.md): goals may form a tree via
``parent_goal_id``; ``depends_on`` (same-parent siblings only) and
``priority`` are stored here but only consumed by the H2 selector.  Child
transitions recompute the parent rollup in the same transaction; a stopped
goal routes dependent siblings to ``human_required``.  Goals without
hierarchy fields behave exactly as the flat G1 lifecycle.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


GOAL_STATES = {
    "candidate",
    "active",
    "stale",
    "conflict",
    "human_required",
    "completed",
    "stopped",
}

MAX_GOAL_REVISIONS = 4

ALLOWED_TRANSITIONS = {
    "candidate": {"active", "stale", "conflict", "human_required", "stopped"},
    "active": {"stale", "conflict", "human_required", "completed", "stopped"},
    "stale": {"human_required"},
    "conflict": {"human_required"},
    "human_required": {"candidate", "active", "stopped"},
    "completed": set(),
    "stopped": {"candidate", "human_required"},
}


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _id(prefix: str, value: Any) -> str:
    return prefix + hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


class GoalStore:
    """The durable G1 Goal state store.

    ``record_event`` and ``create_candidate`` use stable keys and SQLite
    transactions.  A process may terminate before or after either call and a
    later process can replay the same key without creating a second object.
    ``transition_goal`` is only a guarded persistence primitive; it does not
    perform G2 campaign admission checks.  Its H1 hierarchy effects (parent
    rollup, dependency break) are deterministic recomputations applied in the
    same transaction, so a crash cannot leave a half-applied rollup.
    """

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "goals.sqlite3"
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS goal_events (
                    event_key TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    goal_id TEXT,
                    revision_id TEXT,
                    result_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS goals (
                    goal_id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    stage_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    current_revision_id TEXT,
                    run_id TEXT,
                    source_event_key TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(source_event_key) REFERENCES goal_events(event_key)
                );
                CREATE TABLE IF NOT EXISTS goal_revisions (
                    revision_id TEXT PRIMARY KEY,
                    goal_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    goal_json TEXT NOT NULL,
                    goal_digest TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(goal_id, revision),
                    FOREIGN KEY(goal_id) REFERENCES goals(goal_id)
                );
                CREATE TABLE IF NOT EXISTS goal_claims (
                    claim_id TEXT PRIMARY KEY,
                    event_key TEXT NOT NULL,
                    claim_type TEXT NOT NULL,
                    goal_id TEXT,
                    revision_id TEXT,
                    result_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    UNIQUE(event_key, claim_type),
                    FOREIGN KEY(event_key) REFERENCES goal_events(event_key)
                );
                CREATE TABLE IF NOT EXISTS goal_event_leases (
                    event_key TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    FOREIGN KEY(event_key) REFERENCES goal_events(event_key)
                );
                CREATE INDEX IF NOT EXISTS idx_goal_events_state ON goal_events(state);
                CREATE INDEX IF NOT EXISTS idx_goals_state ON goals(state);
                CREATE INDEX IF NOT EXISTS idx_goal_claims_event ON goal_claims(event_key);
                """
            )
            columns = {row[1] for row in conn.execute("PRAGMA table_info(goals)")}
            if "run_id" not in columns:
                conn.execute("ALTER TABLE goals ADD COLUMN run_id TEXT")
            # H1 hierarchy columns: additive, nullable/defaulted so existing
            # flat goals keep identical behavior (GoalHierarchy v1 contract).
            if "parent_goal_id" not in columns:
                conn.execute("ALTER TABLE goals ADD COLUMN parent_goal_id TEXT")
            if "depends_on_json" not in columns:
                conn.execute("ALTER TABLE goals ADD COLUMN depends_on_json TEXT NOT NULL DEFAULT '[]'")
            if "priority" not in columns:
                conn.execute("ALTER TABLE goals ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
            event_columns = {row[1] for row in conn.execute("PRAGMA table_info(goal_events)")}
            if "result_json" not in event_columns:
                conn.execute("ALTER TABLE goal_events ADD COLUMN result_json TEXT")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @staticmethod
    def _event_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value.pop("payload_json"))
        value["result"] = json.loads(value.pop("result_json")) if value.get("result_json") else None
        return value

    def _goal_row(self, conn: sqlite3.Connection, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["depends_on"] = json.loads(value.pop("depends_on_json") or "[]")
        revision = None
        if value["current_revision_id"] is not None:
            revision_row = conn.execute(
                "SELECT * FROM goal_revisions WHERE revision_id = ?", (value["current_revision_id"],)
            ).fetchone()
            if revision_row is not None:
                revision = dict(revision_row)
                revision["goal"] = json.loads(revision.pop("goal_json"))
        value["current_revision"] = revision
        return value

    def record_event(
        self,
        *,
        event_id: str,
        source: str,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Persist one event or return the durable result for a replay.

        Reusing a key with a different source, type, or payload is rejected;
        silently merging those events would break the lifecycle boundary.
        """
        event_id = _required_text("event_id", event_id)
        source = _required_text("source", source)
        event_type = _required_text("event_type", event_type)
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        event_key = _required_text("idempotency_key", idempotency_key or event_id)
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload_digest = _digest(payload)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT * FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
            if existing is not None:
                if (
                    existing["source"] != source
                    or existing["event_type"] != event_type
                    or existing["payload_digest"] != payload_digest
                ):
                    conn.execute("ROLLBACK")
                    raise ValueError("idempotency key is already bound to a different event")
                conn.execute("COMMIT")
                result = self._event_row(existing)
                assert result is not None
                result["status"] = "reused"
                return result
            prior_id = conn.execute("SELECT event_key FROM goal_events WHERE event_id = ?", (event_id,)).fetchone()
            if prior_id is not None:
                conn.execute("ROLLBACK")
                raise ValueError("event_id is already bound to a different idempotency key")
            conn.execute(
                "INSERT INTO goal_events(event_key, event_id, source, event_type, payload_json, payload_digest, state, goal_id, revision_id, result_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'event_received', NULL, NULL, NULL, ?, ?)",
                (event_key, event_id, source, event_type, payload_json, payload_digest, now, now),
            )
            conn.execute("COMMIT")
        return {
            "status": "received",
            "event_key": event_key,
            "event_id": event_id,
            "state": "event_received",
            "payload_digest": payload_digest,
        }

    def get_event(self, event_key: str) -> dict[str, Any]:
        event_key = _required_text("event_key", event_key)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
        value = self._event_row(row)
        if value is None:
            raise KeyError(f"unknown event_key: {event_key}")
        return value

    def claim_event(self, event_key: str, holder: str, seconds: int = 60) -> bool:
        """Claim one pending Goal event; an expired lease can be recovered."""
        event_key = _required_text("event_key", event_key)
        holder = _required_text("holder", holder)
        if not 1 <= seconds <= 3600:
            raise ValueError("seconds must be between 1 and 3600")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            event = conn.execute("SELECT state FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
            if event is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown event_key: {event_key}")
            if event["state"] not in {"event_received", "candidate"}:
                conn.execute("ROLLBACK")
                return False
            lease = conn.execute("SELECT holder, expires_at FROM goal_event_leases WHERE event_key = ?", (event_key,)).fetchone()
            if lease is not None and lease["expires_at"] > now and lease["holder"] != holder:
                conn.execute("COMMIT")
                return False
            conn.execute("INSERT OR REPLACE INTO goal_event_leases(event_key, holder, expires_at) VALUES (?, ?, ?)", (event_key, holder, now + seconds))
            conn.execute("COMMIT")
        return True

    def release_event(self, event_key: str, holder: str) -> None:
        event_key = _required_text("event_key", event_key)
        holder = _required_text("holder", holder)
        with self._connect() as conn:
            conn.execute("DELETE FROM goal_event_leases WHERE event_key = ? AND holder = ?", (event_key, holder))

    def transition_event(self, event_key: str, new_state: str, *, result: dict[str, Any] | None = None) -> dict[str, Any]:
        """Persist a worker outcome without creating another Goal or Run."""
        event_key = _required_text("event_key", event_key)
        if new_state != "event_received" and new_state not in GOAL_STATES:
            raise ValueError(f"invalid event state: {new_state}")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event = conn.execute("SELECT event_key FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
                if event is None:
                    raise KeyError(f"unknown event_key: {event_key}")
                conn.execute(
                    "UPDATE goal_events SET state = ?, result_json = ?, updated_at = ? WHERE event_key = ?",
                    (new_state, json.dumps(result, ensure_ascii=False, sort_keys=True) if result is not None else None, time.time(), event_key),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_event(event_key)

    @staticmethod
    def _claim_result(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {**json.loads(row["result_json"]), "claim_id": row["claim_id"], "status": "reused"}

    def _claim_once(
        self,
        conn: sqlite3.Connection,
        *,
        event_key: str,
        claim_type: str,
        goal_id: str | None,
        revision_id: str | None,
        result: dict[str, Any],
        now: float,
    ) -> dict[str, Any]:
        existing = conn.execute(
            "SELECT * FROM goal_claims WHERE event_key = ? AND claim_type = ?", (event_key, claim_type)
        ).fetchone()
        reused = self._claim_result(existing)
        if reused is not None:
            return reused
        claim_id = _id("claim-", {"event_key": event_key, "claim_type": claim_type})
        conn.execute(
            "INSERT INTO goal_claims(claim_id, event_key, claim_type, goal_id, revision_id, result_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (claim_id, event_key, claim_type, goal_id, revision_id, json.dumps(result, ensure_ascii=False, sort_keys=True), now),
        )
        return {**result, "claim_id": claim_id, "status": "created"}

    def create_candidate(
        self,
        event_key: str,
        *,
        goal_id: str,
        campaign_id: str,
        stage_id: str,
        goal: dict[str, Any],
        revision: int = 1,
        revision_id: str | None = None,
        parent_goal_id: str | None = None,
        depends_on: list[str] | None = None,
        priority: int = 0,
    ) -> dict[str, Any]:
        """Durably record a normalized candidate for an existing event.

        This is persistence only.  It does not decide whether the candidate
        is admitted; G2/G3/G4 own those checks and transitions.

        H1 hierarchy fields (GoalHierarchy v1 contract): ``parent_goal_id``
        links the candidate under one parent goal; ``depends_on`` may only
        reference same-parent siblings; ``priority`` only breaks ties among
        simultaneously runnable siblings (enforced by the H2 selector, not
        here).  A broken hierarchy spec (missing/cross-parent/terminal
        parent, cycle, self/cross-parent dependency) fails closed: the
        candidate is stored, then routed to ``human_required``.
        """
        event_key = _required_text("event_key", event_key)
        goal_id = _required_text("goal_id", goal_id)
        campaign_id = _required_text("campaign_id", campaign_id)
        stage_id = _required_text("stage_id", stage_id)
        if not isinstance(goal, dict):
            raise ValueError("goal must be an object")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise ValueError("revision must be a positive integer")
        if parent_goal_id is not None:
            parent_goal_id = _required_text("parent_goal_id", parent_goal_id)
        if depends_on is None:
            depends_on = []
        if not isinstance(depends_on, list) or any(not isinstance(dep, str) or not dep.strip() for dep in depends_on):
            raise ValueError("depends_on must be a list of non-empty strings")
        depends_on = [dep.strip() for dep in dict.fromkeys(depends_on)]
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise ValueError("priority must be an integer")
        revision_id = revision_id or _id("rev-", {"goal_id": goal_id, "revision": revision, "goal": goal})
        revision_id = _required_text("revision_id", revision_id)
        goal_digest = _digest(goal)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event = conn.execute("SELECT * FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
                if event is None:
                    raise KeyError(f"unknown event_key: {event_key}")
                prior_claim = conn.execute(
                    "SELECT * FROM goal_claims WHERE event_key = ? AND claim_type = 'candidate_created'", (event_key,)
                ).fetchone()
                reused = self._claim_result(prior_claim)
                if reused is not None:
                    conn.execute("COMMIT")
                    return reused
                if event["state"] != "event_received":
                    raise ValueError(f"event is not awaiting candidate creation: {event['state']}")
                existing_goal = conn.execute("SELECT * FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if existing_goal is not None and existing_goal["source_event_key"] != event_key:
                    raise ValueError("goal_id is already owned by a different source event")
                existing_revision = conn.execute(
                    "SELECT * FROM goal_revisions WHERE revision_id = ?", (revision_id,)
                ).fetchone()
                if existing_revision is not None and existing_revision["goal_digest"] != goal_digest:
                    raise ValueError("revision_id is already bound to a different goal payload")
                if existing_goal is None:
                    conn.execute(
                        "INSERT INTO goals(goal_id, campaign_id, stage_id, state, current_revision_id, run_id, source_event_key, parent_goal_id, depends_on_json, priority, created_at, updated_at) VALUES (?, ?, ?, 'candidate', ?, NULL, ?, ?, ?, ?, ?, ?)",
                        (goal_id, campaign_id, stage_id, revision_id, event_key, parent_goal_id, json.dumps(depends_on, ensure_ascii=False), priority, now, now),
                    )
                elif existing_goal["campaign_id"] != campaign_id or existing_goal["stage_id"] != stage_id:
                    raise ValueError("goal_id is already bound to a different campaign stage")
                elif (
                    existing_goal["parent_goal_id"] != parent_goal_id
                    or json.loads(existing_goal["depends_on_json"] or "[]") != depends_on
                    or existing_goal["priority"] != priority
                ):
                    raise ValueError("goal_id is already bound to different hierarchy fields")
                hierarchy_violation = self._validate_hierarchy(
                    conn, goal_id=goal_id, parent_goal_id=parent_goal_id, depends_on=depends_on
                )
                final_state = "candidate"
                if hierarchy_violation is not None and existing_goal is None:
                    # candidate -> human_required is a legal transition; the
                    # event stays bound so a human can inspect the broken spec.
                    final_state = "human_required"
                    conn.execute("UPDATE goals SET state = 'human_required', updated_at = ? WHERE goal_id = ?", (now, goal_id))
                if existing_revision is None:
                    conn.execute(
                        "INSERT INTO goal_revisions(revision_id, goal_id, revision, goal_json, goal_digest, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (revision_id, goal_id, revision, json.dumps(goal, ensure_ascii=False, sort_keys=True), goal_digest, now),
                    )
                conn.execute(
                    "UPDATE goal_events SET state = 'candidate', goal_id = ?, revision_id = ?, updated_at = ? WHERE event_key = ?",
                    (goal_id, revision_id, now, event_key),
                )
                result = {
                    "state": final_state,
                    "event_key": event_key,
                    "goal_id": goal_id,
                    "revision_id": revision_id,
                    "revision": revision,
                    "goal_digest": goal_digest,
                }
                if parent_goal_id is not None:
                    result["parent_goal_id"] = parent_goal_id
                if depends_on:
                    result["depends_on"] = depends_on
                if priority:
                    result["priority"] = priority
                if hierarchy_violation is not None:
                    result["hierarchy_violation"] = hierarchy_violation
                claimed = self._claim_once(
                    conn,
                    event_key=event_key,
                    claim_type="candidate_created",
                    goal_id=goal_id,
                    revision_id=revision_id,
                    result=result,
                    now=now,
                )
                conn.execute("COMMIT")
                return claimed
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def transition_goal(
        self,
        goal_id: str,
        new_state: str,
        *,
        expected_state: str | None = None,
        event_key: str | None = None,
    ) -> dict[str, Any]:
        """Apply one guarded lifecycle state change without admission logic."""
        goal_id = _required_text("goal_id", goal_id)
        if new_state not in GOAL_STATES:
            raise ValueError(f"invalid goal state: {new_state}")
        if expected_state is not None and expected_state not in GOAL_STATES:
            raise ValueError(f"invalid expected goal state: {expected_state}")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT * FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown goal_id: {goal_id}")
                current = row["state"]
                if expected_state is not None and current != expected_state:
                    raise ValueError(f"goal is {current}, expected {expected_state}")
                if new_state != current and new_state not in ALLOWED_TRANSITIONS[current]:
                    raise ValueError(f"invalid goal transition: {current} -> {new_state}")
                if event_key is not None:
                    event_key = _required_text("event_key", event_key)
                    event = conn.execute("SELECT event_key FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
                    if event is None:
                        raise KeyError(f"unknown event_key: {event_key}")
                    event_goal = conn.execute("SELECT goal_id FROM goal_events WHERE event_key = ?", (event_key,)).fetchone()
                    if event_goal["goal_id"] not in (None, goal_id):
                        raise ValueError("event_key is already bound to a different goal")
                if new_state != current:
                    conn.execute("UPDATE goals SET state = ?, updated_at = ? WHERE goal_id = ?", (new_state, now, goal_id))
                    self._after_goal_transition(conn, goal_id=goal_id, new_state=new_state, now=now)
                if event_key is not None:
                    conn.execute(
                        "UPDATE goal_events SET state = ?, goal_id = ?, updated_at = ? WHERE event_key = ?",
                        (new_state, goal_id, now, event_key),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_goal(goal_id)

    @staticmethod
    def _validate_hierarchy(
        conn: sqlite3.Connection,
        *,
        goal_id: str,
        parent_goal_id: str | None,
        depends_on: list[str],
    ) -> str | None:
        """Return a human-readable violation reason, or None when the spec holds."""
        if parent_goal_id is None and not depends_on:
            return None
        if parent_goal_id is not None:
            parent = conn.execute("SELECT state, parent_goal_id FROM goals WHERE goal_id = ?", (parent_goal_id,)).fetchone()
            if parent is None:
                return f"parent goal does not exist: {parent_goal_id}"
            if parent["state"] not in {"candidate", "active", "human_required"}:
                return f"parent goal is not extendable in state: {parent['state']}"
            ancestor: str | None = parent_goal_id
            while ancestor is not None:
                if ancestor == goal_id:
                    return "hierarchy cycle detected"
                row = conn.execute("SELECT parent_goal_id FROM goals WHERE goal_id = ?", (ancestor,)).fetchone()
                ancestor = row["parent_goal_id"] if row is not None else None
        for dep in depends_on:
            if dep == goal_id:
                return f"goal cannot depend on itself: {dep}"
            dep_row = conn.execute("SELECT parent_goal_id FROM goals WHERE goal_id = ?", (dep,)).fetchone()
            if dep_row is None:
                return f"depends_on target does not exist: {dep}"
            if dep_row["parent_goal_id"] != parent_goal_id:
                return f"depends_on target is not a sibling: {dep}"
        return None

    def _after_goal_transition(
        self,
        conn: sqlite3.Connection,
        *,
        goal_id: str,
        new_state: str,
        now: float,
    ) -> None:
        """H1 hierarchy effects, applied in the same transaction (GoalHierarchy v1).

        Dependency break: a stopped goal routes same-parent siblings that list
        it in ``depends_on`` to ``human_required`` (only ``completed`` releases
        a dependency).  Rollup: the parent's state is recomputed from its
        children, recursively upward; the rollup is a derived query over the
        children, never a stored redundant value.
        """
        row = conn.execute("SELECT parent_goal_id FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
        parent_id = row["parent_goal_id"]
        if new_state == "stopped":
            if parent_id is None:
                siblings = conn.execute("SELECT goal_id, state, depends_on_json FROM goals WHERE parent_goal_id IS NULL").fetchall()
            else:
                siblings = conn.execute("SELECT goal_id, state, depends_on_json FROM goals WHERE parent_goal_id = ?", (parent_id,)).fetchall()
            for sibling in siblings:
                if sibling["goal_id"] == goal_id or sibling["state"] not in {"candidate", "active"}:
                    continue
                if goal_id in json.loads(sibling["depends_on_json"] or "[]"):
                    # candidate/active -> human_required are both legal transitions.
                    conn.execute("UPDATE goals SET state = 'human_required', updated_at = ? WHERE goal_id = ?", (now, sibling["goal_id"]))
        if parent_id is not None:
            self._recompute_rollup(conn, parent_id, now)

    def _recompute_rollup(self, conn: sqlite3.Connection, parent_id: str, now: float) -> None:
        parent = conn.execute("SELECT goal_id, state, parent_goal_id FROM goals WHERE goal_id = ?", (parent_id,)).fetchone()
        if parent is None or parent["state"] not in {"active", "human_required"}:
            # Rollup only moves admitted parents; candidate parents cannot lawfully
            # reach completed, and terminal parents cannot move at all.
            return
        children = conn.execute("SELECT state FROM goals WHERE parent_goal_id = ?", (parent_id,)).fetchall()
        if not children:
            return
        states = {child["state"] for child in children}
        if "human_required" in states:
            target = "human_required"
        elif states <= {"completed", "stopped"}:
            target = "completed"
        else:
            target = "active"
        current = parent["state"]
        changed = False
        if current == target:
            pass
        elif target in ALLOWED_TRANSITIONS[current]:
            conn.execute("UPDATE goals SET state = ?, updated_at = ? WHERE goal_id = ?", (target, now, parent_id))
            changed = True
        elif current == "human_required" and target == "completed":
            # human_required -> completed is not a legal direct transition;
            # re-enter through active, then complete (both steps legal).
            conn.execute("UPDATE goals SET state = 'active', updated_at = ? WHERE goal_id = ?", (now, parent_id))
            conn.execute("UPDATE goals SET state = 'completed', updated_at = ? WHERE goal_id = ?", (now, parent_id))
            changed = True
        if changed and parent["parent_goal_id"] is not None:
            self._recompute_rollup(conn, parent["parent_goal_id"], now)

    def get_goal(self, goal_id: str) -> dict[str, Any]:
        goal_id = _required_text("goal_id", goal_id)
        with self._connect() as conn:
            value = self._goal_row(conn, conn.execute("SELECT * FROM goals WHERE goal_id = ?", (goal_id,)).fetchone())
        if value is None:
            raise KeyError(f"unknown goal_id: {goal_id}")
        return value

    def bump_revision(self, goal_id: str) -> dict[str, Any]:
        """Create revision N+1 for an existing goal (revision-bump re-run).

        When a goal's run is exhausted (stopped) and a new command re-issues
        the work, a new revision yields a new deterministic run_id while the
        old run stays as history.  Capped at MAX_GOAL_REVISIONS; beyond it the
        caller routes to human_required instead of looping forever.
        """
        goal_id = _required_text("goal_id", goal_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT * FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown goal_id: {goal_id}")
                current = conn.execute(
                    "SELECT * FROM goal_revisions WHERE revision_id = ?", (row["current_revision_id"],)
                ).fetchone()
                if current is None:
                    raise ValueError("goal has no current revision to bump")
                next_seq = int(current["revision"]) + 1
                if next_seq > MAX_GOAL_REVISIONS:
                    raise ValueError(f"goal revision cap reached ({MAX_GOAL_REVISIONS})")
                goal_payload = json.loads(current["goal_json"])
                revision_id = _id("rev-", {"goal_id": goal_id, "revision": next_seq, "goal": goal_payload})
                conn.execute(
                    "INSERT INTO goal_revisions(revision_id, goal_id, revision, goal_json, goal_digest, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (revision_id, goal_id, next_seq, current["goal_json"], current["goal_digest"], now),
                )
                conn.execute(
                    "UPDATE goals SET current_revision_id = ?, updated_at = ? WHERE goal_id = ?",
                    (revision_id, now, goal_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {"goal_id": goal_id, "revision": next_seq, "revision_id": revision_id}

    def activate_with_run(self, goal_id: str, run_id: str, *, event_key: str | None = None) -> dict[str, Any]:
        """Persist the G4 candidate-to-active transition and its deterministic run link."""
        goal_id = _required_text("goal_id", goal_id)
        run_id = _required_text("run_id", run_id)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT * FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown goal_id: {goal_id}")
                if row["state"] == "active":
                    if row["run_id"] != run_id:
                        raise ValueError("active goal is already linked to a different run")
                    conn.execute("COMMIT")
                    return self.get_goal(goal_id)
                if row["state"] != "candidate":
                    raise ValueError(f"goal is not an admissible candidate: {row['state']}")
                source_event_key = event_key or row["source_event_key"]
                source_event_key = _required_text("event_key", source_event_key)
                event = conn.execute("SELECT goal_id FROM goal_events WHERE event_key = ?", (source_event_key,)).fetchone()
                if event is None:
                    raise KeyError(f"unknown event_key: {source_event_key}")
                if event["goal_id"] not in (None, goal_id):
                    raise ValueError("event_key is already bound to a different goal")
                conn.execute("UPDATE goals SET state = 'active', run_id = ?, updated_at = ? WHERE goal_id = ?", (run_id, now, goal_id))
                conn.execute("UPDATE goal_events SET state = 'active', goal_id = ?, updated_at = ? WHERE event_key = ?", (goal_id, now, source_event_key))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_goal(goal_id)

    def active_goals(self, *, campaign_id: str | None = None, stage_id: str | None = None) -> list[dict[str, Any]]:
        clauses = ["state = 'active'"]
        params: list[str] = []
        if campaign_id is not None:
            clauses.append("campaign_id = ?")
            params.append(_required_text("campaign_id", campaign_id))
        if stage_id is not None:
            clauses.append("stage_id = ?")
            params.append(_required_text("stage_id", stage_id))
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM goals WHERE " + " AND ".join(clauses) + " ORDER BY goal_id", params).fetchall()
            return [self._goal_row(conn, row) for row in rows]

    def goals_in_state(self, state: str) -> list[dict[str, Any]]:
        state = _required_text("state", state)
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM goals WHERE state = ? ORDER BY goal_id", (state,)).fetchall()
            return [self._goal_row(conn, row) for row in rows]

    def pending_events(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM goal_events WHERE state IN ('event_received', 'candidate') ORDER BY created_at, event_key").fetchall()
        return [self._event_row(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            event_count = conn.execute("SELECT COUNT(*) FROM goal_events").fetchone()[0]
            goal_count = conn.execute("SELECT COUNT(*) FROM goals").fetchone()[0]
            revision_count = conn.execute("SELECT COUNT(*) FROM goal_revisions").fetchone()[0]
            claim_count = conn.execute("SELECT COUNT(*) FROM goal_claims").fetchone()[0]
            states = conn.execute("SELECT state, COUNT(*) AS count FROM goals GROUP BY state ORDER BY state").fetchall()
            event_states = conn.execute("SELECT state, COUNT(*) AS count FROM goal_events GROUP BY state ORDER BY state").fetchall()
        return {
            "event_count": event_count,
            "goal_count": goal_count,
            "revision_count": revision_count,
            "claim_count": claim_count,
            "goals_by_state": {row["state"]: row["count"] for row in states},
            "events_by_state": {row["state"]: row["count"] for row in event_states},
        }

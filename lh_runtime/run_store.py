"""SQLite-backed durable state for the native Loop Hybrid MVP."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any


RUN_STATES = {"queued", "running", "retry_pending", "verified", "stopped", "awaiting_external_verdict"}
PROCESS_START_NS = time.time_ns()
PROCESS_NONCE = secrets.token_urlsafe(18)


class RunStore:
    """The only component that persists native LH run state."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir(exist_ok=True)
        self.db_path = self.root / "loop.sqlite3"
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    goal_json TEXT NOT NULL,
                    source_repo TEXT NOT NULL,
                    base_revision TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    fence INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    workspace_ref TEXT NOT NULL,
                    fence INTEGER NOT NULL DEFAULT 0,
                    receipt_ref TEXT,
                    receipt_digest TEXT,
                    created_at REAL NOT NULL,
                    finished_at REAL,
                    PRIMARY KEY(run_id, ordinal),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    run_id TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS usage_corrections (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(run_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS failure_cases (
                    failure_case_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    goal_id TEXT NOT NULL,
                    signature_json TEXT NOT NULL,
                    acceptance_digest TEXT NOT NULL,
                    base_revision TEXT NOT NULL,
                    state TEXT NOT NULL,
                    grill_generation INTEGER NOT NULL DEFAULT 1,
                    claim_fence INTEGER NOT NULL DEFAULT 0,
                    claim_holder TEXT,
                    claim_expires_at REAL,
                    plan_digest TEXT,
                    latest_check_receipt_digest TEXT,
                    next_node_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS failure_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    failure_case_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(failure_case_id) REFERENCES failure_cases(failure_case_id)
                );
                """
            )
            run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
            if "fence" not in run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN fence INTEGER NOT NULL DEFAULT 0")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(attempts)")}
            if "receipt_digest" not in columns:
                conn.execute("ALTER TABLE attempts ADD COLUMN receipt_digest TEXT")
            if "fence" not in columns:
                conn.execute("ALTER TABLE attempts ADD COLUMN fence INTEGER NOT NULL DEFAULT 0")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["goal"] = json.loads(value.pop("goal_json"))
        return value

    def create_run(self, *, goal: dict[str, Any], source_repo: Path, base_revision: str, max_attempts: int = 4, run_id: str | None = None) -> str:
        if not 1 <= max_attempts <= 4:
            raise ValueError("max_attempts must be an integer from 1 to 4")
        run_id = run_id or "run-" + uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
            goal_json = json.dumps(goal, sort_keys=True)
            existing = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if existing is not None:
                if (existing["goal_json"], existing["source_repo"], existing["base_revision"], existing["max_attempts"]) != (goal_json, str(source_repo), base_revision, max_attempts):
                    raise ValueError("run_id is already bound to different run inputs")
                return run_id
            conn.execute(
                "INSERT INTO runs(run_id, goal_json, source_repo, base_revision, state, attempts, fence, max_attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'queued', 0, 0, ?, ?, ?)",
                (run_id, goal_json, str(source_repo), base_revision, max_attempts, now, now),
            )
        self.append_event(run_id, "run_created", {"base_revision": base_revision, "max_attempts": max_attempts})
        return run_id

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        value = self._row(row)
        if value is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return value

    @staticmethod
    def _stable_id(prefix: str, payload: Any) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        return prefix + hashlib.sha256(encoded).hexdigest()[:32]

    @staticmethod
    def _append_event_conn(
        conn: sqlite3.Connection,
        *,
        event_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_at: float,
    ) -> str:
        encoded = json.dumps(payload, sort_keys=True)
        prior = conn.execute(
            "SELECT run_id, event_type, payload_json FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if prior is not None:
            if (prior["run_id"], prior["event_type"], prior["payload_json"]) != (run_id, event_type, encoded):
                raise ValueError("event_id is already bound to different event content")
            return event_id
        conn.execute(
            "INSERT INTO events(event_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_id, run_id, event_type, encoded, created_at),
        )
        return event_id

    @staticmethod
    def _append_outbox_conn(
        conn: sqlite3.Connection,
        *,
        event_id: str,
        failure_case_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_at: float,
    ) -> str:
        encoded = json.dumps(payload, sort_keys=True)
        prior = conn.execute(
            "SELECT failure_case_id, event_type, payload_json FROM failure_outbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if prior is not None:
            if (prior["failure_case_id"], prior["event_type"], prior["payload_json"]) != (
                failure_case_id,
                event_type,
                encoded,
            ):
                raise ValueError("outbox event_id is already bound to different event content")
            return event_id
        conn.execute(
            "INSERT INTO failure_outbox(event_id, failure_case_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_id, failure_case_id, event_type, encoded, created_at),
        )
        return event_id

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        event_id: str | None = None,
    ) -> str:
        event_id = event_id or "evt-" + uuid.uuid4().hex
        with self._connect() as conn:
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type=event_type,
                payload=payload,
                created_at=time.time(),
            )
        return event_id

    def events(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]

    @staticmethod
    def _failure_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["signature"] = json.loads(value.pop("signature_json"))
        return value

    def latest_failure_signature(self, run_id: str) -> dict[str, Any] | None:
        receipt = self.latest_receipt(run_id)
        if receipt is None:
            return None
        try:
            payload = json.loads((self.root / receipt["receipt_ref"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        verification = payload.get("verification") if isinstance(payload, dict) else None
        diff = payload.get("diff") if isinstance(payload, dict) else None
        exit_code = verification.get("exit_code") if isinstance(verification, dict) else None
        diff_digest = diff.get("digest") if isinstance(diff, dict) else None
        if not isinstance(exit_code, int) or exit_code == 0 or not isinstance(diff_digest, str):
            return None
        return {"exit_code": exit_code, "diff_digest": diff_digest}

    def ensure_failure_case(
        self,
        run_id: str,
        *,
        signature: dict[str, Any],
        acceptance_argv: list[str],
        trigger: str,
        receipt_digest: str | None = None,
    ) -> dict[str, Any]:
        run = self.get_run(run_id)
        goal = run["goal"] if isinstance(run.get("goal"), dict) else {}
        goal_id = str(goal.get("goal_id") or run_id)
        acceptance_digest = self._stable_id("sha256:", acceptance_argv)
        identity = {
            "source_repo": run["source_repo"],
            "campaign_id": goal.get("campaign_id"),
            "goal_id": goal_id,
            "feature_contract": goal.get("feature_contract"),
            "base_revision": run["base_revision"],
            "acceptance_digest": acceptance_digest,
            "signature": signature,
        }
        failure_case_id = self._stable_id("fc-", identity)
        now = time.time()
        payload = {
            "schema": "loop-hybrid-failure-case-event/v1",
            "failure_case_id": failure_case_id,
            "run_id": run_id,
            "goal_id": goal_id,
            "state": "grill_required",
            "grill_generation": 1,
            "trigger": trigger,
            "signature": signature,
            "acceptance_digest": acceptance_digest,
            "base_revision": run["base_revision"],
            "receipt_digest": receipt_digest,
        }
        outbox_id = f"failure-case:{failure_case_id}:opened"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO failure_cases(failure_case_id, run_id, goal_id, signature_json, acceptance_digest, base_revision, state, grill_generation, claim_fence, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'grill_required', 1, 0, ?, ?)",
                    (
                        failure_case_id,
                        run_id,
                        goal_id,
                        json.dumps(signature, sort_keys=True),
                        acceptance_digest,
                        run["base_revision"],
                        now,
                        now,
                    ),
                )
                self._append_event_conn(
                    conn,
                    event_id=outbox_id,
                    run_id=run_id,
                    event_type="failure_case_opened",
                    payload=payload,
                    created_at=now,
                )
                self._append_outbox_conn(
                    conn,
                    event_id=outbox_id,
                    failure_case_id=failure_case_id,
                    event_type="failure_case_opened",
                    payload=payload,
                    created_at=now,
                )
            conn.execute("COMMIT")
        case = self.failure_case(failure_case_id)
        case["created"] = existing is None
        return case

    def failure_case(self, failure_case_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
        value = self._failure_row(row)
        if value is None:
            raise KeyError(f"unknown failure_case_id: {failure_case_id}")
        return value

    def failure_case_for_run(
        self,
        run_id: str,
        *,
        states: set[str] | None = None,
    ) -> dict[str, Any] | None:
        query = "SELECT * FROM failure_cases WHERE run_id = ?"
        values: list[Any] = [run_id]
        if states:
            ordered = sorted(states)
            query += " AND state IN (" + ",".join("?" for _ in ordered) + ")"
            values.extend(ordered)
        query += " ORDER BY updated_at DESC, failure_case_id DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(query, values).fetchone()
        return self._failure_row(row)

    def claim_grill(
        self,
        failure_case_id: str,
        *,
        holder: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] == "grill_claimed" and float(row["claim_expires_at"] or 0) > now:
                conn.execute("COMMIT")
                return {
                    "status": "busy",
                    "failure_case_id": failure_case_id,
                    "generation": int(row["grill_generation"]),
                    "fence": int(row["claim_fence"]),
                }
            if row["state"] not in {"grill_required", "grill_claimed"}:
                conn.execute("COMMIT")
                return {
                    "status": "not_claimable",
                    "failure_case_id": failure_case_id,
                    "state": row["state"],
                }
            fence = int(row["claim_fence"]) + 1
            generation = int(row["grill_generation"])
            updated = conn.execute(
                "UPDATE failure_cases SET state = 'grill_claimed', claim_fence = ?, claim_holder = ?, claim_expires_at = ?, updated_at = ? "
                "WHERE failure_case_id = ? AND claim_fence = ?",
                (fence, holder, now + lease_seconds, now, failure_case_id, int(row["claim_fence"])),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                return {"status": "busy", "failure_case_id": failure_case_id}
            conn.execute("COMMIT")
        return {
            "status": "claimed",
            "failure_case_id": failure_case_id,
            "generation": generation,
            "fence": fence,
        }

    def record_grill_result(
        self,
        failure_case_id: str,
        *,
        generation: int,
        fence: int,
        decision: str,
        diagnosis: str,
    ) -> dict[str, Any]:
        now = time.time()
        plan_digest = self._stable_id("sha256:", {"decision": decision, "diagnosis": diagnosis})
        state = "remediation_running" if decision == "runner-fixable" else "human_required"
        event_id = f"failure-case:{failure_case_id}:grill:{generation}:result"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] != "grill_claimed" or int(row["grill_generation"]) != generation or int(row["claim_fence"]) != fence:
                prior = conn.execute(
                    "SELECT payload_json FROM failure_outbox WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                conn.execute("COMMIT")
                if prior is not None:
                    return {**json.loads(prior["payload_json"]), "status": "reused"}
                return {"status": "fence_rejected", "failure_case_id": failure_case_id}
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": failure_case_id,
                "run_id": row["run_id"],
                "goal_id": row["goal_id"],
                "event": "grill_result",
                "state": state,
                "grill_generation": generation,
                "grill_fence": fence,
                "decision": decision,
                "diagnosis": diagnosis,
                "plan_digest": plan_digest,
            }
            run_attempts = conn.execute(
                "SELECT attempts FROM runs WHERE run_id = ?",
                (row["run_id"],),
            ).fetchone()
            conn.execute(
                "UPDATE failure_cases SET state = ?, plan_digest = ?, claim_holder = NULL, claim_expires_at = NULL, updated_at = ? "
                "WHERE failure_case_id = ?",
                (state, plan_digest, now, failure_case_id),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=row["run_id"],
                event_type="grill_decision",
                payload={
                    "failure_case_id": failure_case_id,
                    "decision": decision,
                    "diagnosis": diagnosis,
                    "attempts_used": int(run_attempts["attempts"]) if run_attempts is not None else 0,
                    "grill_generation": generation,
                    "plan_digest": plan_digest,
                },
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=failure_case_id,
                event_type="grill_result",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return {**payload, "status": "recorded"}

    def record_grill_failure(
        self,
        failure_case_id: str,
        *,
        generation: int,
        fence: int,
        reason: str,
    ) -> dict[str, Any]:
        now = time.time()
        event_id = f"failure-case:{failure_case_id}:grill:{generation}:failed"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] != "grill_claimed" or int(row["grill_generation"]) != generation or int(row["claim_fence"]) != fence:
                prior = conn.execute("SELECT payload_json FROM failure_outbox WHERE event_id = ?", (event_id,)).fetchone()
                conn.execute("COMMIT")
                if prior is not None:
                    return {**json.loads(prior["payload_json"]), "status": "reused"}
                return {"status": "fence_rejected", "failure_case_id": failure_case_id}
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": failure_case_id,
                "run_id": row["run_id"],
                "goal_id": row["goal_id"],
                "event": "grill_failed",
                "state": "human_required",
                "grill_generation": generation,
                "grill_fence": fence,
                "reason": reason[:500],
            }
            conn.execute(
                "UPDATE failure_cases SET state = 'human_required', claim_holder = NULL, claim_expires_at = NULL, updated_at = ? WHERE failure_case_id = ?",
                (now, failure_case_id),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=row["run_id"],
                event_type="grill_failed",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=failure_case_id,
                event_type="grill_failed",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return {**payload, "status": "recorded"}

    def record_resolution(
        self,
        run_id: str,
        *,
        ordinal: int,
        receipt_digest: str,
        exit_code: int,
        machine_resolved: bool | None = None,
        allow_regrill: bool = True,
        checker_reason: str | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE run_id = ? AND state = 'remediation_running' ORDER BY updated_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            run = conn.execute("SELECT max_attempts FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            resolved = exit_code == 0 if machine_resolved is None else machine_resolved
            has_budget = allow_regrill and run is not None and ordinal < int(run["max_attempts"])
            if resolved:
                state = "resolved"
            elif has_budget:
                state = "grill_required"
            else:
                state = "human_required"
            next_generation = int(row["grill_generation"]) if resolved or not has_budget else int(row["grill_generation"]) + 1
            event_id = f"failure-case:{row['failure_case_id']}:check:{ordinal}:{receipt_digest}"
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": row["failure_case_id"],
                "run_id": run_id,
                "goal_id": row["goal_id"],
                "event": "resolution_checked",
                "state": state,
                "machine_resolved": resolved,
                "checker": "approved_deterministic_verifier_and_value_gate",
                "checker_reason": checker_reason,
                "verifier_exit_code": exit_code,
                "attempt": ordinal,
                "receipt_digest": receipt_digest,
                "grill_generation": int(row["grill_generation"]),
                "next_grill_generation": next_generation if not resolved and has_budget else None,
                "regrill_required": not resolved and has_budget,
            }
            conn.execute(
                "UPDATE failure_cases SET state = ?, grill_generation = ?, latest_check_receipt_digest = ?, plan_digest = CASE WHEN ? THEN NULL ELSE plan_digest END, "
                "claim_holder = NULL, claim_expires_at = NULL, updated_at = ? WHERE failure_case_id = ?",
                (
                    state,
                    next_generation,
                    receipt_digest,
                    1 if not resolved and has_budget else 0,
                    now,
                    row["failure_case_id"],
                ),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type="resolution_checked",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=row["failure_case_id"],
                event_type="resolution_checked",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return payload

    def record_next_node(self, run_id: str, *, next_node_id: str | None) -> dict[str, Any] | None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE run_id = ? AND state = 'resolved' ORDER BY updated_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            receipt_digest = row["latest_check_receipt_digest"]
            event_id = f"failure-case:{row['failure_case_id']}:closed:{receipt_digest}"
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": row["failure_case_id"],
                "run_id": run_id,
                "goal_id": row["goal_id"],
                "event": "failure_case_closed",
                "state": "resolved",
                "machine_resolved": True,
                "checker_receipt_digest": receipt_digest,
                "next_node_id": next_node_id,
            }
            conn.execute(
                "UPDATE failure_cases SET next_node_id = ?, updated_at = ? WHERE failure_case_id = ?",
                (next_node_id, now, row["failure_case_id"]),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type="failure_case_closed",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=row["failure_case_id"],
                event_type="failure_case_closed",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return payload

    def failure_outbox(self, *, after_sequence: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM failure_outbox WHERE sequence > ? ORDER BY sequence",
                (int(after_sequence),),
            ).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]

    def summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute("SELECT state, COUNT(*) AS count FROM runs GROUP BY state ORDER BY state").fetchall()
            events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {"runs_by_state": {row["state"]: row["count"] for row in rows}, "event_count": events}

    def runnable_runs(self) -> list[dict[str, Any]]:
        """Return queued/retry-pending runs in deterministic creation order."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs WHERE state IN ('queued', 'retry_pending') ORDER BY created_at, run_id").fetchall()
        return [self._row(row) for row in rows]

    def terminal_runs(self) -> list[dict[str, Any]]:
        """Return runs whose result still needs Goal lifecycle reduction."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs WHERE state IN ('verified', 'stopped') ORDER BY updated_at, run_id").fetchall()
        return [self._row(row) for row in rows]

    def latest_receipt(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT ordinal, receipt_ref, receipt_digest "
                "FROM attempts WHERE run_id = ? AND receipt_ref IS NOT NULL "
                "ORDER BY ordinal DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return None if row is None else dict(row)

    def latest_receipt_projection(self) -> dict[str, Any] | None:
        """Return the newest receipt pointer without exposing artifact content.

        This is the read-only bridge used by status snapshots.  Receipt bytes,
        provider output, and verifier stdout/stderr remain in the RunStore
        artifact boundary; callers receive only durable identity and digest.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT r.run_id, r.goal_json, r.state AS run_state, r.updated_at,
                       a.ordinal, a.state AS attempt_state, a.workspace_ref,
                       a.receipt_ref, a.receipt_digest, a.finished_at,
                       (SELECT COUNT(*) FROM attempts WHERE receipt_ref IS NOT NULL) AS receipt_count
                FROM runs r
                JOIN attempts a ON a.run_id = r.run_id
                WHERE a.receipt_ref IS NOT NULL
                  AND a.ordinal = (
                      SELECT MAX(a2.ordinal) FROM attempts a2
                      WHERE a2.run_id = r.run_id AND a2.receipt_ref IS NOT NULL
                  )
                ORDER BY r.updated_at DESC, a.ordinal DESC, r.run_id DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        try:
            goal = json.loads(row["goal_json"])
        except (TypeError, json.JSONDecodeError):
            goal = {}
        return {
            "schema": "loop-hybrid-receipt-projection/v1",
            "run_id": row["run_id"],
            "goal_id": goal.get("goal_id") if isinstance(goal, dict) else None,
            "run_state": row["run_state"],
            "attempt": row["ordinal"],
            "attempt_state": row["attempt_state"],
            "workspace_ref": row["workspace_ref"],
            "receipt": {"ref": row["receipt_ref"], "digest": row["receipt_digest"]},
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
            "receipt_count": row["receipt_count"],
        }

    def latest_attempt(self, run_id: str) -> dict[str, Any] | None:
        """Read the newest attempt metadata without exposing the SQLite store.

        Command/report adapters use this projection to correlate a goal with
        the durable run/attempt boundary.  It is intentionally read-only;
        attempt ownership, fencing, and receipt writes remain RunStore's
        responsibility.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT run_id, ordinal, state, workspace_ref, fence, receipt_ref, receipt_digest, created_at, finished_at "
                "FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return None if row is None else dict(row)

    def usage_corrections(self) -> list[dict[str, Any]]:
        """List the human-recorded usage voids (W9e), oldest first."""
        with self._connect() as conn:
            rows = conn.execute("SELECT run_id, ordinal, reason, created_at FROM usage_corrections ORDER BY created_at, run_id, ordinal").fetchall()
        return [dict(row) for row in rows]

    def void_usage(self, run_id: str, ordinal: int, *, reason: str) -> dict[str, Any]:
        """W9e: human-only void of a poisoned usage record.

        Appends a correction row beside the evidence — the original receipt
        and its recorded digest are never modified. Voiding a record that
        does not exist is a clear error; a duplicate void reports
        already-voided instead of writing a second row. Nothing in the
        engine calls this automatically."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        with self._connect() as conn:
            attempt = conn.execute("SELECT receipt_ref FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, int(ordinal))).fetchone()
            if attempt is None or attempt["receipt_ref"] is None:
                raise ValueError(f"no recorded usage for run {run_id} attempt {ordinal}")
            existing = conn.execute("SELECT reason, created_at FROM usage_corrections WHERE run_id = ? AND ordinal = ?", (run_id, int(ordinal))).fetchone()
            if existing is not None:
                return {"status": "already_voided", "run_id": run_id, "ordinal": int(ordinal), "reason": existing["reason"], "created_at": existing["created_at"]}
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO usage_corrections(run_id, ordinal, reason, created_at) VALUES (?, ?, ?, ?)", (run_id, int(ordinal), reason.strip(), now))
            conn.execute("COMMIT")
        return {"status": "voided", "run_id": run_id, "ordinal": int(ordinal), "reason": reason.strip(), "created_at": now}

    def usage_records(self) -> list[dict[str, Any]]:
        """Read the per-attempt token usage stamped into each committed receipt.

        A missing or unreadable usage block is reported as ``unknown`` — it is
        never silently treated as zero usage. Records a human voided through
        ``void_usage`` (W9e, e.g. a phantom-attribution record) are excluded;
        the correction lives beside the receipt, never inside it."""
        with self._connect() as conn:
            rows = conn.execute("SELECT run_id, ordinal, state, created_at, finished_at, receipt_ref FROM attempts WHERE receipt_ref IS NOT NULL ORDER BY run_id, ordinal").fetchall()
            voided = {(row["run_id"], row["ordinal"]) for row in conn.execute("SELECT run_id, ordinal FROM usage_corrections")}
        records: list[dict[str, Any]] = []
        for row in rows:
            if (row["run_id"], row["ordinal"]) in voided:
                continue
            usage: dict[str, Any] | None = None
            try:
                receipt = json.loads((self.root / row["receipt_ref"]).read_text(encoding="utf-8"))
                candidate = receipt.get("usage")
                if isinstance(candidate, dict):
                    usage = candidate
            except (OSError, json.JSONDecodeError):
                usage = None
            if usage is None:
                usage = {"state": "unknown", "reason": "receipt has no readable usage"}
            elapsed = None
            if row["created_at"] is not None and row["finished_at"] is not None:
                elapsed = round(float(row["finished_at"]) - float(row["created_at"]), 3)
            records.append({
                "run_id": row["run_id"], "attempt": row["ordinal"], "attempt_state": row["state"],
                "created_at": row["created_at"], "finished_at": row["finished_at"], "elapsed_seconds": elapsed,
                **usage,
            })
        return records

    @staticmethod
    def _process_holder(holder: str) -> str:
        label = str(holder or "worker")
        return f"{label}:pid={os.getpid()}:start={PROCESS_START_NS}:nonce={PROCESS_NONCE}"

    def _record_fence_rejection(self, run_id: str, *, operation: str, ordinal: int | None = None, supplied_fence: int | None = None, current_fence: int | None = None) -> None:
        self.append_event(
            run_id,
            "fence_rejected",
            {
                "operation": operation,
                "attempt": ordinal,
                "supplied_fence": supplied_fence,
                "current_fence": current_fence,
            },
        )

    def acquire_lease(self, run_id: str, holder: str, seconds: int = 60) -> bool:
        effective_holder = self._process_holder(holder)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT holder, expires_at FROM leases WHERE run_id = ?", (run_id,)).fetchone()
            if row is not None and row["expires_at"] > now and row["holder"] != effective_holder:
                conn.execute("COMMIT")
                return False
            conn.execute("INSERT OR REPLACE INTO leases VALUES (?, ?, ?)", (run_id, effective_holder, now + seconds))
            conn.execute("COMMIT")
        return True

    def release_lease(self, run_id: str, holder: str) -> None:
        effective_holder = self._process_holder(holder)
        with self._connect() as conn:
            conn.execute("DELETE FROM leases WHERE run_id = ? AND holder = ?", (run_id, effective_holder))

    def _reconcile_expired_run(self, run_id: str) -> dict[str, Any] | None:
        """Recover one expired running attempt without re-invoking a model."""
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            lease = conn.execute("SELECT expires_at FROM leases WHERE run_id = ?", (run_id,)).fetchone()
            if run is None or run["state"] != "running" or (lease is not None and lease["expires_at"] > now):
                conn.execute("COMMIT")
                return None
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND state = 'running' ORDER BY ordinal DESC LIMIT 1", (run_id,)).fetchone()
            if attempt is None:
                conn.execute("COMMIT")
                return None
            current_fence = int(run["fence"])
            if int(attempt["fence"]) != current_fence:
                conn.execute("COMMIT")
                self._record_fence_rejection(
                    run_id,
                    operation="reconcile",
                    ordinal=int(attempt["ordinal"]),
                    supplied_fence=int(attempt["fence"]),
                    current_fence=current_fence,
                )
                return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
            receipt_path = self.artifacts / run_id / str(attempt["ordinal"]) / "receipt.json"
            receipt_ref = receipt_digest = None
            exit_code: int | None = None
            try:
                receipt_raw = receipt_path.read_text(encoding="utf-8")
                receipt = json.loads(receipt_raw)
                if receipt.get("schema") == "loop-hybrid-attempt-receipt/v1" and receipt.get("run_id") == run_id and receipt.get("attempt") == attempt["ordinal"] and isinstance(receipt.get("verification", {}).get("exit_code"), int):
                    import hashlib
                    receipt_ref = str(receipt_path.relative_to(self.root))
                    receipt_digest = "sha256:" + hashlib.sha256(receipt_raw.encode()).hexdigest()
                    exit_code = receipt["verification"]["exit_code"]
            except (OSError, json.JSONDecodeError):
                pass
            if exit_code is not None:
                max_attempts = conn.execute("SELECT max_attempts FROM runs WHERE run_id = ?", (run_id,)).fetchone()["max_attempts"]
                state = "verified" if exit_code == 0 else "stopped" if attempt["ordinal"] >= max_attempts else "retry_pending"
                next_fence = current_fence + 1
                updated_attempt = conn.execute("UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (state, receipt_ref, receipt_digest, now, run_id, attempt["ordinal"], current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = ?, fence = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (state, next_fence, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    self._record_fence_rejection(run_id, operation="reconcile", ordinal=int(attempt["ordinal"]), supplied_fence=current_fence, current_fence=current_fence)
                    return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
                result = {"run_id": run_id, "attempt": attempt["ordinal"], "status": state, "recovered_from": "receipt"}
            else:
                next_fence = current_fence + 1
                updated_attempt = conn.execute("UPDATE attempts SET state = 'interrupted', finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (now, run_id, attempt["ordinal"], current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = 'retry_pending', fence = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (next_fence, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    self._record_fence_rejection(run_id, operation="reconcile", ordinal=int(attempt["ordinal"]), supplied_fence=current_fence, current_fence=current_fence)
                    return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
                result = {"run_id": run_id, "attempt": attempt["ordinal"], "status": "retry_pending", "recovered_from": "interrupted"}
            conn.execute("DELETE FROM leases WHERE run_id = ?", (run_id,))
            conn.execute("COMMIT")
        self.append_event(run_id, "attempt_reconciled", result)
        return result

    def recover_stale_run(self, run_id: str) -> bool:
        """Compatibility wrapper for a controller that wakes one run."""
        return self._reconcile_expired_run(run_id) is not None

    def reconcile_startup(self) -> list[dict[str, Any]]:
        """Scan every running run after process start and recover expired leases."""
        with self._connect() as conn:
            run_ids = [row["run_id"] for row in conn.execute("SELECT run_id FROM runs WHERE state = 'running'")]
        return [result for run_id in run_ids if (result := self._reconcile_expired_run(run_id)) is not None]

    def begin_attempt(self, run_id: str, workspace_ref: str) -> int:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT attempts, state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if run is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown run_id: {run_id}")
            if run["state"] not in {"queued", "retry_pending"}:
                conn.execute("ROLLBACK")
                raise ValueError(f"run is not executable from {run['state']}")
            ordinal = run["attempts"] + 1
            fence = int(run["fence"]) + 1
            conn.execute("UPDATE runs SET attempts = ?, fence = ?, state = 'running', updated_at = ? WHERE run_id = ?", (ordinal, fence, now, run_id))
            conn.execute(
                "INSERT INTO attempts(run_id, ordinal, state, workspace_ref, fence, receipt_ref, receipt_digest, created_at, finished_at) VALUES (?, ?, 'running', ?, ?, NULL, NULL, ?, NULL)",
                (run_id, ordinal, workspace_ref, fence, now),
            )
            conn.execute("COMMIT")
        self.append_event(run_id, "attempt_started", {"attempt": ordinal, "workspace_ref": workspace_ref, "fence": fence})
        return ordinal

    def attempt_fence(self, run_id: str, ordinal: int) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
        if row is None:
            raise KeyError(f"unknown attempt: {run_id}/{ordinal}")
        return int(row["fence"])

    def finish_attempt(self, run_id: str, ordinal: int, *, state: str, receipt_ref: str, receipt_digest: str, fence: int | None = None) -> bool:
        if state not in RUN_STATES - {"running", "queued"}:
            raise ValueError(f"invalid terminal attempt state: {state}")
        now = time.time()
        rejection: dict[str, Any] | None = None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            current_fence = int(run["fence"]) if run is not None else None
            attempt_fence = int(attempt["fence"]) if attempt is not None else None
            expected_fence = attempt_fence if fence is None else fence
            if (
                run is None
                or attempt is None
                or attempt["state"] != "running"
                or attempt_fence != current_fence
                or expected_fence != current_fence
            ):
                rejection = {"operation": "finish", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                conn.execute("COMMIT")
            else:
                updated_attempt = conn.execute("UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (state, receipt_ref, receipt_digest, now, run_id, ordinal, current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (state, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    rejection = {"operation": "finish", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                else:
                    conn.execute("COMMIT")
        if rejection is not None:
            self._record_fence_rejection(run_id, **rejection)
            return False
        self.append_event(run_id, "attempt_finished", {"attempt": ordinal, "state": state, "receipt_ref": receipt_ref})
        return True

    def park_external_verdict(self, run_id: str, ordinal: int, *, receipt_ref: str, receipt_digest: str, fence: int | None = None) -> bool:
        """Park a run awaiting an async external verdict (Method A). Not a crash, not finished:
        the attempt stays open (finished_at NULL) and reconcile ignores it (state != running)."""
        now = time.time()
        rejection: dict[str, Any] | None = None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            current_fence = int(run["fence"]) if run is not None else None
            attempt_fence = int(attempt["fence"]) if attempt is not None else None
            expected_fence = attempt_fence if fence is None else fence
            if (
                run is None
                or attempt is None
                or run["state"] != "running"
                or attempt["state"] != "running"
                or attempt_fence != current_fence
                or expected_fence != current_fence
            ):
                rejection = {"operation": "park_external_verdict", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                conn.execute("COMMIT")
            else:
                updated_attempt = conn.execute("UPDATE attempts SET state = 'awaiting_external_verdict', receipt_ref = ?, receipt_digest = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (receipt_ref, receipt_digest, run_id, ordinal, current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = 'awaiting_external_verdict', updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    rejection = {"operation": "park_external_verdict", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                else:
                    conn.execute("COMMIT")
        if rejection is not None:
            self._record_fence_rejection(run_id, **rejection)
            return False
        self.append_event(run_id, "external_verdict_parked", {"attempt": ordinal})
        return True

    def resolve_external_verdict(self, run_id: str, new_state: str) -> None:
        """Finalize a parked run once its external verdict lands: verified / retry_pending / stopped."""
        if new_state not in {"verified", "retry_pending", "stopped"}:
            raise ValueError(f"invalid external verdict state: {new_state}")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if run is None or run["state"] != "awaiting_external_verdict":
                conn.execute("ROLLBACK")
                raise ValueError("run is not awaiting an external verdict")
            conn.execute("UPDATE attempts SET state = ?, finished_at = ? WHERE run_id = ? AND state = 'awaiting_external_verdict'", (new_state, now, run_id))
            conn.execute("UPDATE runs SET state = ?, updated_at = ? WHERE run_id = ?", (new_state, now, run_id))
            conn.execute("COMMIT")
        self.append_event(run_id, "external_verdict_resolved", {"state": new_state})

    def write_artifact(self, run_id: str, ordinal: int, name: str, content: str) -> dict[str, str]:
        import hashlib

        path = self.artifacts / run_id / str(ordinal) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        return {"ref": str(path.relative_to(self.root)), "digest": "sha256:" + hashlib.sha256(content.encode()).hexdigest()}

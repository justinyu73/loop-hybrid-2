"""SQLite-backed durable state for the native Loop Hybrid MVP."""
from __future__ import annotations

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

    def append_event(self, run_id: str, event_type: str, payload: dict[str, Any]) -> str:
        event_id = "evt-" + uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO events(event_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (event_id, run_id, event_type, json.dumps(payload, sort_keys=True), time.time()),
            )
        return event_id

    def events(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)).fetchall()
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
            row = conn.execute("SELECT receipt_ref, receipt_digest FROM attempts WHERE run_id = ? AND receipt_ref IS NOT NULL ORDER BY ordinal DESC LIMIT 1", (run_id,)).fetchone()
        return None if row is None else dict(row)

    def usage_records(self) -> list[dict[str, Any]]:
        """Read the per-attempt token usage stamped into each committed receipt.

        A missing or unreadable usage block is reported as ``unknown`` — it is
        never silently treated as zero usage."""
        with self._connect() as conn:
            rows = conn.execute("SELECT run_id, ordinal, state, created_at, finished_at, receipt_ref FROM attempts WHERE receipt_ref IS NOT NULL ORDER BY run_id, ordinal").fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
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

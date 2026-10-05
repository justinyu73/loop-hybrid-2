#!/usr/bin/env python3
"""Method A: async external verdict with POLL-ON-STARTUP resume.

The loop opens an external action (a PR) and PARKS the run as awaiting a verdict.
No always-on daemon and no webhook are required: whenever a controller process next
starts, it POLLS the (durable) awaiting runs against a conclusion source (real CI, or
a stub here) and resumes them. This survives operator-host outage the same way the
spine survives kill-9 — the awaiting state lives in SQLite, not in a live process.

此 port 組合 external_action_port 與持久化 VerdictStore，不持有任何憑證。
conclusion source 由使用者透過引擎 API 注入；測試使用的 fixture source 只證明
隔離接線，不代表真實 CI 驗收。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Callable

import external_action_port as eap

# Given an op_key, return None if the external verdict is still pending, else a dict
# like {"conclusion": "success" | "failure"}. A real source queries the CI that runs the change.
ConclusionSource = Callable[[str], dict[str, Any] | None]


class _ClosingConnection(sqlite3.Connection):
    """Keep the transaction context contract while closing on context exit."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class VerdictStore:
    """Durable record of runs awaiting / resolved on an external verdict."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS verdicts (run_id TEXT PRIMARY KEY, op_key TEXT NOT NULL, action_json TEXT NOT NULL, state TEXT NOT NULL, conclusion TEXT, dispatched_at REAL NOT NULL, resolved_at REAL)")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, factory=_ClosingConnection)
        conn.row_factory = sqlite3.Row
        return conn

    def park(self, run_id: str, op_key: str, action: dict[str, Any], *, at: float) -> None:
        """Park a run awaiting its external verdict.

        A retry after a failure verdict re-parks the SAME run_id with a NEW
        op_key (the new attempt's action). The row must therefore upsert:
        reset to awaiting with the current round's op_key and timestamps.
        An ignored re-park would leave the row resolved forever, and the run
        would never be polled again (live-found gap).
        """
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO verdicts(run_id, op_key, action_json, state, conclusion, dispatched_at, resolved_at) VALUES (?, ?, ?, 'awaiting_external_verdict', NULL, ?, NULL) "
                "ON CONFLICT(run_id) DO UPDATE SET op_key = excluded.op_key, action_json = excluded.action_json, state = 'awaiting_external_verdict', conclusion = NULL, dispatched_at = excluded.dispatched_at, resolved_at = NULL",
                (run_id, op_key, json.dumps(action, sort_keys=True), at),
            )
            conn.execute("COMMIT")

    def awaiting(self) -> list[tuple[str, str]]:
        with self._connect() as conn:
            return [(r["run_id"], r["op_key"]) for r in conn.execute("SELECT run_id, op_key FROM verdicts WHERE state = 'awaiting_external_verdict'")]

    def resolve(self, run_id: str, state: str, conclusion: str, *, at: float) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE verdicts SET state = ?, conclusion = ?, resolved_at = ? WHERE run_id = ? AND state = 'awaiting_external_verdict'", (state, conclusion, at, run_id))
            conn.execute("COMMIT")

    def action_for_op_key(self, op_key: str) -> dict[str, Any] | None:
        """Return the parked action record for an op_key, or None when unknown."""
        with self._connect() as conn:
            row = conn.execute("SELECT action_json FROM verdicts WHERE op_key = ?", (op_key,)).fetchone()
        return None if row is None else json.loads(row["action_json"])

    def action_for_run(self, run_id: str) -> dict[str, Any] | None:
        """Return the parked action record for a run_id, or None when unknown."""
        with self._connect() as conn:
            row = conn.execute("SELECT action_json FROM verdicts WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else json.loads(row["action_json"])

    def state(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT state, conclusion FROM verdicts WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else {"state": row["state"], "conclusion": row["conclusion"]}

    def full_record(self, run_id: str) -> dict[str, Any] | None:
        """The whole durable row: the normalizer binds digests and duration to
        it, so it must come from one read, not from stitched accessors."""
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM verdicts WHERE run_id = ?", (run_id,)).fetchone()
        return None if row is None else dict(row)

    def resolved_verified(self) -> list[dict[str, Any]]:
        """Rows whose conclusion landed as success: the crash-recovery scan
        re-offers these to the normalizer when the run never crossed."""
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM verdicts WHERE state = 'verified'")]


def dispatch_external(store: VerdictStore, ledger: eap.ActionLedger, adapter: eap.ExternalAdapter,
                      *, run_id: str, op_key: str, request: dict[str, Any], at: float) -> dict[str, Any]:
    """Open the external action at most once, then park the run awaiting its verdict."""
    sent = eap.dispatch(ledger, adapter, op_key=op_key, request=request, at=at)
    store.park(run_id, op_key, {"request": request, "external": sent["result"]}, at=at)
    return {"status": "awaiting_external_verdict", "run_id": run_id, "op_key": op_key, "external": sent["result"], "deduped": sent["deduped"]}


def poll_and_resume(store: VerdictStore, source: ConclusionSource, *, at: float) -> list[dict[str, Any]]:
    """Called on controller startup: resume every awaiting run whose verdict has landed.

    A pending verdict leaves the run awaiting (no premature resolution). A success maps
    to verified; any other conclusion maps to retry_pending for the loop to retry.
    A source ERROR (unavailable, 403, malformed) is not a verdict either: the run
    stays parked and polling continues with the next one — a bad source must never
    crash the driver.
    """
    resumed: list[dict[str, Any]] = []
    for run_id, op_key in store.awaiting():
        try:
            verdict = source(op_key)
        except Exception:  # a source failure is not a verdict; keep the run parked
            continue
        if verdict is None:
            continue
        state = "verified" if verdict.get("conclusion") == "success" else "retry_pending"
        store.resolve(run_id, state, verdict.get("conclusion", "unknown"), at=at)
        resumed.append({"run_id": run_id, "op_key": op_key, "conclusion": verdict.get("conclusion"), "state": state})
    return resumed

#!/usr/bin/env python3
"""Generic external-action port with operation-key idempotency (the dedup leg).

Stub-only: NO real GitHub/provider credentials, network, or egress. It models the
runtime port through which the loop performs an outward side-effect (open a PR,
post a status) exactly once across crashes and retries.

The at-most-once guarantee needs BOTH sides to key on the same operation_key:
  - the local ActionLedger, so a completed action is never re-issued; and
  - the external adapter, so an action performed just before a crash (local record
    lost) is NOT duplicated when the loop retries — the external system recognises
    the key and returns the existing result instead of a second side-effect.
A real adapter must therefore send an idempotency key its API honours (e.g.
GitHub's Idempotency-Key); that credentialed wiring is deliberately out of scope.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Protocol


def operation_key(run_id: str, action_id: str, payload: Any) -> str:
    """Deterministic key: stable across retries of the same logical action."""
    raw = json.dumps({"run_id": run_id, "action_id": action_id, "payload": payload}, sort_keys=True, ensure_ascii=False).encode()
    return "op-" + hashlib.sha256(raw).hexdigest()[:32]


class ExternalAdapter(Protocol):
    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Perform the side-effect; MUST be idempotent on op_key (at-most-once)."""
        ...


class ActionLedger:
    """Durable record of performed external actions, keyed by operation_key."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS operations (op_key TEXT PRIMARY KEY, result_json TEXT NOT NULL, recorded_at REAL NOT NULL)")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def get(self, op_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT result_json FROM operations WHERE op_key = ?", (op_key,)).fetchone()
        return None if row is None else json.loads(row["result_json"])

    def put(self, op_key: str, result: dict[str, Any], *, at: float) -> None:
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO operations VALUES (?, ?, ?)", (op_key, json.dumps(result, sort_keys=True), at))
            conn.execute("COMMIT")


def dispatch(ledger: ActionLedger, adapter: ExternalAdapter, *, op_key: str, request: dict[str, Any], at: float) -> dict[str, Any]:
    """Perform an external action at most once for op_key."""
    existing = ledger.get(op_key)
    if existing is not None:
        return {"op_key": op_key, "sent": False, "deduped": True, "result": existing}
    result = adapter.perform(op_key, request)  # external side is also idempotent on op_key
    ledger.put(op_key, result, at=at)
    return {"op_key": op_key, "sent": True, "deduped": False, "result": result}

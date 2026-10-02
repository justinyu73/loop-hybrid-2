#!/usr/bin/env python3
"""Generic external-action port with operation-key idempotency (the dedup leg).

本模組定義介面與本地 ledger，不直接讀取 provider 憑證或發出網路請求。
goal_loop_run 已可依 Project Runtime Contract 注入 GitHubPrAdapter；
是否允許外部作用仍由該 contract、授權及 adapter 能力判定，不能由
介面存在或離線 canary 推論 live 已通過。

The at-most-once guarantee needs BOTH sides to key on the same operation_key:
  - the local ActionLedger, so a completed action is never re-issued; and
  - the external adapter, so an action performed just before a crash (local record
    lost) is NOT duplicated when the loop retries — the external system recognises
    the key and returns the existing result instead of a second side-effect.
實際 adapter 必須對 operation_key 實作可驗證的去重或既有效果讀回；
本地 ledger 本身不保證外部 API exactly-once，也不假設 GitHub 提供
通用 Idempotency-Key。既有 GitHub 接線見 github_pr_adapter.py。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Protocol


class _ClosingConnection(sqlite3.Connection):
    """Keep the transaction context contract while closing on context exit."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


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
        conn = sqlite3.connect(self.path, factory=_ClosingConnection)
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

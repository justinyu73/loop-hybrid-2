#!/usr/bin/env python3
"""Live-ish offline canary for the driver's unconditional heartbeat boundary."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import goal_loop_driver
from goal_loop_driver import run_driver
from run_store import RunStore


class _IdleGoalStore:
    def goals_in_state(self, state: str) -> list[dict]:
        return []


class _IdleWorker:
    def __init__(self, root: Path) -> None:
        self.run_store = RunStore(root / "runs")
        self.goal_store = _IdleGoalStore()

    def tick(self, **_kwargs) -> dict:
        return {"status": "idle", "run": None, "terminal_after": None}


def _case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        worker = _IdleWorker(root)
        history: list[dict] = []
        original_write = goal_loop_driver.write_heartbeat

        def capture(heartbeat: dict, out_path: Path) -> Path:
            history.append(json.loads(json.dumps(heartbeat)))
            return original_write(heartbeat, out_path)

        goal_loop_driver.write_heartbeat = capture
        try:
            result = run_driver(
                worker,
                holder="idle-canary",
                model=lambda _workspace, _capsule: {"summary": "unused"},
                max_cycles=4,
                idle_limit=4,
                backoff_seconds=0,
                sleep_fn=lambda _seconds: None,
            )
        finally:
            goal_loop_driver.write_heartbeat = original_write

        heartbeat_path = worker.run_store.root / "driver-heartbeat.json"
        final = json.loads(heartbeat_path.read_text(encoding="utf-8")) if heartbeat_path.exists() else {}
        monotonic_values = [float(item["monotonic_ts"]) for item in history]
        required = {"holder", "monotonic_ts", "wall_ts", "phase", "cycles"}
        identity = str(final.get("holder", ""))
        cases = [
            _case(
                "idle-only-writes-heartbeat-at-every-tick",
                result["stop_reason"] == "idle" and result["cycles"] == 4 and len(history) >= 4,
                json.dumps({"result": result, "writes": len(history)}),
            ),
            _case(
                "default-heartbeat-exists-without-snapshot-flag",
                heartbeat_path.exists() and not (worker.run_store.root / "platform_status.json").exists(),
                str(heartbeat_path),
            ),
            _case(
                "heartbeat-fields-monotonic-and-process-specific",
                all(required <= set(item) for item in history)
                and all(right > left for left, right in zip(monotonic_values, monotonic_values[1:]))
                and all(token in identity for token in ("pid=", "start=", "nonce="))
                and final.get("phase") == "idle"
                and final.get("cycles") == 4,
                json.dumps({"final": final, "monotonic_writes": len(monotonic_values)}),
            ),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-driver-heartbeat",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "the foreground thread cannot heartbeat while an executor is blocking",
            "cross-project staleness classification is covered by the SH canary",
        ],
    }, ensure_ascii=False, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

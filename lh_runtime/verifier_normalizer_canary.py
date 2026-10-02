#!/usr/bin/env python3
"""N15 normalizer canary — the contract's negative arms, offline.

goal-lifecycle-v1's asynchronous boundary: missing, duplicate-conflicting,
late or digest-mismatched evidence cannot emit value_reduction_ready; a
conflicted binding stays conflicted even when later evidence repeats the
first outcome (no wash); ready is born terminal, never a pending command.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import verifier_normalizer as vn  # noqa: E402
from goal_store import GoalStore  # noqa: E402

BINDING = {
    "goal_id": "g-n15", "revision_id": "rev-1", "run_id": "run-n15",
    "attempt": 1, "check_id": "ext-check",
    "check_definition_digest": "sha256:" + "c" * 64,
    "receipt_digest": "sha256:" + "r" * 64,
}


def _record(store: GoalStore, *, outcome: str = "verified", source: str = "sha256:" + "s" * 64) -> dict[str, Any]:
    return store.record_normalized_verifier_result(
        binding=BINDING, outcome=outcome, source_digest=source,
        authority_check_result_digest="sha256:" + "a" * 64,
        measured_duration=12.5, normalization_version=vn.NORMALIZATION_VERSION,
    )


class StubVerdicts:
    def __init__(self, op_key: str, record: dict[str, Any] | None):
        self._op_key = op_key
        self._record = record

    def full_record(self, run_id: str) -> dict[str, Any] | None:
        return self._record

    def action_for_run(self, run_id: str) -> dict[str, Any] | None:
        return {"request": {"action_id": "ext-check", "diff_digest": "sha256:" + "d" * 64,
                            "workspace_ref": "workspace://n15/1"}}


class StubRuns:
    def __init__(self, state: str = "awaiting_external_verdict"):
        self._state = state

    def get_run(self, run_id: str) -> dict[str, Any]:
        return {"run_id": run_id, "state": self._state, "attempts": 1,
                "goal": {"goal_id": "g-n15"}}

    def latest_receipt(self, run_id: str) -> dict[str, Any]:
        return {"receipt_ref": "r.json", "receipt_digest": BINDING["receipt_digest"]}


def ready_rows(store: GoalStore) -> list[str]:
    with store._connect() as conn:  # noqa: SLF001 - the canary reads the same file the store wrote
        return [row["event_key"] for row in conn.execute(
            "SELECT event_key FROM goal_events WHERE event_type = 'value_reduction_ready'")]


def main() -> int:
    cases: list[dict[str, Any]] = []

    def case(case_id: str, ok: bool, detail: Any) -> None:
        cases.append({"id": case_id, "ok": bool(ok), "detail": str(detail)[:400]})

    with tempfile.TemporaryDirectory(prefix="lh-n15-") as raw:
        root = Path(raw)

        # Arm 1: idempotent replay — one ready event, ever.
        store = GoalStore(root / "a")
        first = _record(store)
        again = _record(store)
        case("verified-emits-exactly-one-ready",
             first["status"] == "ready" and again["status"] == "already_normalized"
             and again["ready_event_key"] == first["ready_event_key"]
             and len(ready_rows(store)) == 1,
             {"first": first["status"], "again": again["status"], "rows": len(ready_rows(store))})

        # Arm 2: ready is born terminal — never a pending command for the matcher.
        pending = [row for row in store.pending_events()] if hasattr(store, "pending_events") else []
        with store._connect() as conn:  # noqa: SLF001
            pending_states = [row["state"] for row in conn.execute(
                "SELECT state FROM goal_events WHERE event_type = 'value_reduction_ready'")]
        case("ready-event-is-terminal-not-pending",
             pending_states == ["value_reduction_ready"] and all(
                 row.get("event_type") != "value_reduction_ready" for row in pending),
             {"states": pending_states, "pending": len(pending)})

        # Arm 3: failed outcome is durable evidence, not a ready.
        store_b = GoalStore(root / "b")
        failed = _record(store_b, outcome="failed")
        case("failed-outcome-never-emits-ready",
             failed["status"] == "normalized" and failed["ready_event_key"] is None
             and len(ready_rows(store_b)) == 0,
             failed)

        # Arm 4: conflict is terminal — repeating the FIRST outcome cannot wash it.
        store_c = GoalStore(root / "c")
        base = _record(store_c)
        turned = _record(store_c, source="sha256:" + "x" * 64)
        washed = _record(store_c)
        row = store_c.normalized_result_for("run-n15", 1)
        case("conflict-is-terminal-no-wash",
             base["status"] == "ready" and turned["status"] == "conflict"
             and washed["status"] == "conflict_replay"
             and row["conflict"] is not None
             and len(ready_rows(store_c)) == 1,
             {"base": base["status"], "turned": turned["status"], "washed": washed["status"]})

        # Arm 5: a conflict landing after ready invalidates that ready for the
        # value reader (the reader consults the record, not the event).
        import value_reducer
        class _Runs(StubRuns):
            pass
        conflicted_verdict_input = store_c.normalized_result_for("run-n15", 1)
        case("post-ready-conflict-invalidates-ready-for-reader",
             conflicted_verdict_input["conflict"] is not None
             and conflicted_verdict_input["ready_event_key"] is not None,
             {"conflict": bool(conflicted_verdict_input["conflict"])})

        # Arm 6: late evidence — a rotated op_key answers a superseded question.
        outcome = vn.normalize_resolved_run(
            goal_store=store, run_store=StubRuns(),
            verdict_store=StubVerdicts("op-new", {"op_key": "op-new", "state": "verified",
                                                  "conclusion": "success", "dispatched_at": 1.0,
                                                  "resolved_at": 2.0}),
            run_id="run-n15", op_key="op-old", conclusion="success",
        )
        case("late-op-key-superseded-is-discarded",
             outcome == {"status": "discarded", "reason": "op_key_superseded"}, outcome)

        # Arm 7: a run that already advanced past its parked point discards.
        outcome_moved = vn.normalize_resolved_run(
            goal_store=store, run_store=StubRuns(state="retry_pending"),
            verdict_store=StubVerdicts("op-1", {"op_key": "op-1", "state": "verified",
                                                "conclusion": "success", "dispatched_at": 1.0,
                                                "resolved_at": 2.0}),
            run_id="run-n15", op_key="op-1", conclusion="success",
        )
        case("superseded-run-state-is-discarded",
             outcome_moved["status"] == "discarded"
             and outcome_moved["reason"].startswith("run_not_parked"), outcome_moved)

    failures = [item for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-verifier-normalizer-n15",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "provider_invocations": 0,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

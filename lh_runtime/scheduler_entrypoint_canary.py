#!/usr/bin/env python3
"""Offline checks for scheduler owner and collision quarantine."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import dispatch_envelope as dispatches
from scheduler_entrypoint import execute


def _runner(payload: dict[str, object], exit_code: int = 0):
    def run(*_args, **_kwargs):
        return subprocess.CompletedProcess([], exit_code, json.dumps(payload), "fixture-error" if exit_code else "")
    return run


def _write_envelope(path: Path, contract: Path) -> dict[str, object]:
    body = {
        "schema": dispatches.SCHEMA,
        "project_id": "example-project",
        "owner_id": "lh-framework-production-v1",
        "contract_ref": str(contract.resolve()),
        "contract_digest": dispatches.digest_file(contract),
        "desired_state": "enabled",
        "desired_state_event_id": "fixture:example-project:enabled",
        "desired_state_digest": "sha256:" + "d" * 64,
        "campaign_id": "example-project-campaign",
        "base_revision": "main",
        "issued_at": "2026-07-24T00:00:00+00:00",
        "source_invocation_id": "fixture-invocation",
    }
    envelope = {
        **body,
        "dispatch_id": "dispatch-" + dispatches.digest_json(body).removeprefix("sha256:")[:32],
    }
    envelope["envelope_digest"] = dispatches.digest_json(envelope)
    path.write_text(json.dumps(envelope), encoding="utf-8")
    return envelope


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="lh-scheduler-owner-") as directory:
        root = Path(directory)
        event_log = root / "events.jsonl"
        contract = root / "contract.json"
        contract.write_text(json.dumps({
            "schema": "lh-project-runtime-contract/v1",
            "project_id": "example-project",
            "campaign": {"campaign_id": "example-project-campaign"},
            "base_revision": "main",
            "runtime": {"run_store": "runs"},
        }), encoding="utf-8")
        envelope_path = root / "dispatch.json"
        envelope = _write_envelope(envelope_path, contract)
        receipt = root / "runs" / "artifacts" / "run-fixture" / "1" / "receipt.json"
        receipt.parent.mkdir(parents=True)
        receipt.write_text(json.dumps({
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": "run-fixture",
            "attempt": 1,
            "dispatch": dispatches.receipt_binding(envelope),
            "verification": {"exit_code": 0},
        }), encoding="utf-8")
        goal_args = ["--contract", str(contract)]
        code, collision = execute(
            owner_id="lh-framework-production-v1",
            project_id="example-project",
            event_log=event_log,
            goal_args=goal_args,
            dispatch_envelope=envelope_path,
            runner=_runner({"mode": "execute", "driver": {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}}),
        )
        cases.append({"id": "second-holder-is-quarantined", "ok": code == 0 and collision["scheduler_owner"]["collision_quarantined"] is True})

        code, normal = execute(
            owner_id="lh-framework-production-v1",
            project_id="example-project",
            event_log=event_log,
            goal_args=goal_args,
            dispatch_envelope=envelope_path,
            runner=_runner({
                "mode": "execute",
                "driver": {
                    "stop_reason": "idle",
                    "cycles": 3,
                    "runs_dispatched": 1,
                    "outcomes": [{"run_id": "run-fixture", "status": "completed"}],
                },
            }),
        )
        rows = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
        cases.append({
            "id": "production-tick-keeps-project-identity",
            "ok": (
                code == 0
                and normal["driver"]["runs_dispatched"] == 1
                and normal["scheduler_owner"]["project_id"] == "example-project"
                and rows[1]["project_id"] == "example-project"
                and rows[1]["receipt_bindings"][0]["run_id"] == "run-fixture"
                and rows[1]["receipt_bindings"][0]["verification_exit_code"] == 0
                and rows[1]["receipt_bindings"][0]["dispatch_bound"] is True
                and rows[1]["dispatch_id"] == envelope["dispatch_id"]
            ),
        })
        cases.append({"id": "events-are-append-only", "ok": len(rows) == 2 and rows[0]["outcome"] == "collision_quarantined" and rows[1]["outcome"] == "tick_completed"})

        code, invalid = execute(
            owner_id="lh-framework-production-v1",
            project_id="example-project",
            event_log=event_log,
            goal_args=goal_args,
            dispatch_envelope=envelope_path,
            runner=_runner({"mode": "execute", "driver": {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 1}}),
        )
        cases.append({"id": "collision-cannot-report-dispatch", "ok": code == 1 and invalid["scheduler_owner"]["outcome"] == "invalid_collision"})

        unbound = json.loads(receipt.read_text(encoding="utf-8"))
        del unbound["dispatch"]
        receipt.write_text(json.dumps(unbound), encoding="utf-8")
        code, invalid_binding = execute(
            owner_id="lh-framework-production-v1",
            project_id="example-project",
            event_log=event_log,
            goal_args=goal_args,
            dispatch_envelope=envelope_path,
            runner=_runner({
                "mode": "execute",
                "driver": {
                    "stop_reason": "idle",
                    "cycles": 1,
                    "runs_dispatched": 1,
                    "outcomes": [{"run_id": "run-fixture", "status": "completed"}],
                },
            }),
        )
        cases.append({
            "id": "dispatched-run-requires-envelope-bound-receipt",
            "ok": code == 1 and invalid_binding["scheduler_owner"]["outcome"] == "dispatch_binding_invalid",
        })

    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "lh-scheduler-entrypoint", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "cases": cases}, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

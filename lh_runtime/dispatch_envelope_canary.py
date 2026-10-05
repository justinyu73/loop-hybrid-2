#!/usr/bin/env python3
"""Offline acceptance for immutable cross-layer dispatch identity."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import dispatch_envelope as dispatches
from controller import LoopController
from run_store import RunStore


def _fixture(root: Path) -> tuple[Path, Path, dict[str, object]]:
    contract = root / "contract.json"
    contract.write_text(json.dumps({
        "schema": "lh-project-runtime-contract/v1",
        "project_id": "example-project",
        "campaign": {
            "schema": "lh-campaign/v1",
            "campaign_id": "example-project-campaign",
            "stages": [],
        },
        "source_repo": ".",
        "base_revision": "main",
        "runtime": {
            "goal_store": "runtime/goals",
            "run_store": "runtime/runs",
            "workspace_root": "runtime/workspaces",
        },
        "executors": {"coder": {"argv": [sys.executable, "-c", "pass", "{prompt}"]}},
        "models": {"execute": "coder"},
    }), encoding="utf-8")
    body = {
        "schema": dispatches.SCHEMA,
        "project_id": "example-project",
        "owner_id": "lh-framework-production-v1",
        "contract_ref": str(contract.resolve()),
        "contract_digest": dispatches.digest_file(contract),
        "desired_state": "enabled",
        "desired_state_event_id": "declared:example-project:enabled",
        "desired_state_digest": "sha256:" + "a" * 64,
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
    envelope_path = root / "dispatch.json"
    envelope_path.write_text(json.dumps(envelope), encoding="utf-8")
    return contract, envelope_path, envelope


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="lh-dispatch-envelope-") as directory:
        root = Path(directory)
        contract, envelope_path, expected = _fixture(root)
        observed = dispatches.load_and_validate(
            envelope_path,
            project_id="example-project",
            owner_id="lh-framework-production-v1",
            contract_path=contract,
        )
        cases.append({
            "id": "valid-envelope-binds-contract-and-desired-state",
            "ok": observed == expected,
        })

        binding = dispatches.receipt_binding(observed)
        controller = LoopController(
            RunStore(root / "runs"),
            root / "workspaces",
            dispatch=binding,
        )
        receipt = controller._bind_dispatch({"schema": "loop-hybrid-attempt-receipt/v1"})
        cases.append({
            "id": "attempt-receipt-carries-same-dispatch-digest",
            "ok": receipt.get("dispatch") == binding,
        })

        environment = dict(os.environ)
        environment["LH_SCHEDULER_OWNER_ID"] = "lh-framework-production-v1"
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).with_name("goal_loop_run.py")),
                "--contract",
                str(contract),
                "--dispatch-envelope",
                str(envelope_path),
            ],
            cwd=root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            dry_run = json.loads(completed.stdout)
        except json.JSONDecodeError:
            dry_run = {}
        cases.append({
            "id": "goal-loop-cli-retains-validated-dispatch-in-plan",
            "ok": (
                completed.returncode == 0
                and dry_run.get("mode") == "dry_run"
                and dry_run.get("plan", {}).get("dispatch") == binding
            ),
        })

        tampered = dict(expected)
        tampered["desired_state_event_id"] = "other-event"
        envelope_path.write_text(json.dumps(tampered), encoding="utf-8")
        try:
            dispatches.load_and_validate(
                envelope_path,
                project_id="example-project",
                owner_id="lh-framework-production-v1",
                contract_path=contract,
            )
        except ValueError:
            rejected = True
        else:
            rejected = False
        cases.append({"id": "tampered-envelope-is-rejected-before-execution", "ok": rejected})

    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-dispatch-envelope",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
    }, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

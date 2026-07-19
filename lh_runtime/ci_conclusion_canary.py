#!/usr/bin/env python3
"""Committed G6 smoke: one CI conclusion source on the existing verdict path."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ci_conclusion_adapter as ci
import external_action_port as eap
from controller import LoopController
from external_verdict import VerdictStore
from run_store import RunStore


class FakeTransport:
    def __init__(self, responses: dict[str, dict[str, str]]):
        self.responses = responses
        self.requests: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.requests.append((url, headers, timeout))
        op_key = parse_qs(urlsplit(url).query).get("operation_key", [""])[0]
        return json.dumps(self.responses[op_key], sort_keys=True).encode("utf-8")


class IdempotentActionAdapter:
    def __init__(self):
        self.results: dict[str, dict[str, str]] = {}
        self.calls: list[str] = []

    def perform(self, op_key: str, request: dict[str, object]) -> dict[str, str]:
        if op_key not in self.results:
            self.calls.append(op_key)
            self.results[op_key] = {"operation_key": op_key, "external_id": "ci-action-1"}
        return self.results[op_key]


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def park_run(root: Path, name: str, op_key: str, conclusion_store: VerdictStore, runs: RunStore) -> str:
    run_id = runs.create_run(goal={"goal_id": name}, source_repo=root, base_revision="fixture", run_id=name)
    runs.begin_attempt(run_id, f"workspace://{name}/1")
    conclusion_store.park(run_id, op_key, {"request": {"source": "g6-canary"}}, at=1.0)
    runs.park_external_verdict(run_id, 1, receipt_ref="receipt.json", receipt_digest="sha256:g6")
    return run_id


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        action = IdempotentActionAdapter()
        op_key = eap.operation_key("run-g6-action", "ci-check", {"revision": "r1"})
        first = eap.dispatch(eap.ActionLedger(root / "actions"), action, op_key=op_key, request={"revision": "r1"}, at=1.0)
        replay = eap.dispatch(eap.ActionLedger(root / "actions"), action, op_key=op_key, request={"revision": "r1"}, at=2.0)
        crash_retry = eap.dispatch(eap.ActionLedger(root / "actions-after-crash"), action, op_key=op_key, request={"revision": "r1"}, at=3.0)

        responses = {
            "op-g6-pending": {"operation_key": "op-g6-pending", "conclusion": "pending"},
            "op-g6-success": {"operation_key": "op-g6-success", "conclusion": "success"},
            "op-g6-failure": {"operation_key": "op-g6-failure", "conclusion": "failure"},
        }
        transport = FakeTransport(responses)
        adapter = ci.CIConclusionAdapter("https://ci.example.invalid/conclusions", "fixture-token", transport=transport)
        pending_runs = RunStore(root / "pending-runs")
        pending_verdicts = VerdictStore(root / "pending-verdicts")
        pending_controller = LoopController(pending_runs, root / "pending-workspaces")
        pending_id = park_run(root, "run-g6-pending", "op-g6-pending", pending_verdicts, pending_runs)
        pending = pending_controller.resume_external(verdict_store=pending_verdicts, source=adapter)
        pending_state = pending_runs.get_run(pending_id)["state"]

        success_runs = RunStore(root / "success-runs")
        success_verdicts = VerdictStore(root / "success-verdicts")
        success_controller = LoopController(success_runs, root / "success-workspaces")
        success_id = park_run(root, "run-g6-success", "op-g6-success", success_verdicts, success_runs)
        success = success_controller.resume_external(verdict_store=success_verdicts, source=adapter)
        success_state = success_runs.get_run(success_id)["state"]

        failure_runs = RunStore(root / "failure-runs")
        failure_verdicts = VerdictStore(root / "failure-verdicts")
        failure_controller = LoopController(failure_runs, root / "failure-workspaces")
        failure_id = park_run(root, "run-g6-failure", "op-g6-failure", failure_verdicts, failure_runs)
        failure = failure_controller.resume_external(verdict_store=failure_verdicts, source=adapter)
        failure_state = failure_runs.get_run(failure_id)["state"]

        invalid_transport = FakeTransport({"op-g6-invalid": {"operation_key": "other", "conclusion": "success"}})
        invalid_adapter = ci.CIConclusionAdapter("https://ci.example.invalid/conclusions", "fixture-token", transport=invalid_transport)
        try:
            invalid_adapter("op-g6-invalid")
        except ci.CIResponseInvalid as exc:
            invalid_detail = type(exc).__name__
        else:
            invalid_detail = "accepted-invalid-response"

        try:
            ci.CIConclusionAdapter.from_env({})
        except ci.CICredentialsMissing as exc:
            missing_credentials_detail = type(exc).__name__
        else:
            missing_credentials_detail = "accepted-missing-credentials"

        request_url, request_headers, request_timeout = transport.requests[0]
        cases = [
            case("operation-key-is-stable-across-local-replay", first["sent"] and replay["deduped"] and first["result"] == replay["result"], str({"first": first, "replay": replay})),
            case("external-adapter-dedupes-after-local-ledger-loss", crash_retry["result"] == first["result"] and len(action.calls) == 1, str({"crash_retry": crash_retry, "calls": action.calls})),
            case("pending-ci-leaves-run-awaiting", pending == [] and pending_state == "awaiting_external_verdict", str({"pending": pending, "state": pending_state})),
            case("ci-success-resumes-verified", success == [{"run_id": success_id, "op_key": "op-g6-success", "conclusion": "success", "state": "verified"}] and success_state == "verified", str(success)),
            case("ci-failure-resumes-retry", failure == [{"run_id": failure_id, "op_key": "op-g6-failure", "conclusion": "failure", "state": "retry_pending"}] and failure_state == "retry_pending", str(failure)),
            case("request-binds-operation-key-and-bearer", "operation_key=op-g6-pending" in request_url and request_headers["Authorization"] == "Bearer fixture-token" and request_timeout == 10.0, request_url),
            case("mismatched-operation-key-fails-closed", invalid_detail == "CIResponseInvalid", invalid_detail),
            case("missing-credentials-fail-closed", missing_credentials_detail == "CICredentialsMissing", missing_credentials_detail),
        ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    result = {
        "check_id": "lh-goal-loop-g6-ci-conclusion",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/ci_conclusion_canary.py",
            "path": "existing external_verdict -> controller.resume_external",
            "network": "none; injected transport only",
        },
        "known_gaps_open": ["No provider-specific GitHub, push, merge, publish, quota, or promotion adapter is included in this G6 slice."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

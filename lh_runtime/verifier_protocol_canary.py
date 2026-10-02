#!/usr/bin/env python3
"""Acceptance canary for the P4 capability-bound runner/verifier protocol."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from runner_adapter import CapabilityBoundRunner, CapabilityError, digest_json  # noqa: E402
from verifier_protocol import (  # noqa: E402
    ReadOnlyWorkspace,
    VerifierProtocol,
    VerifierProtocolError,
)


def _raw_digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def contract() -> dict[str, Any]:
    return {
        "schema": "host-provider-neutral-capability-contract/v1",
        "revision": "p4-canary-1",
        "capabilities": {
            "coding": {
                "adapter_id": "adapter.coding",
                "identity": {"provider_ref": "sample-coder", "model_ref": "sample-model"},
                "permissions": "workspace_write",
            },
            "verifier": {
                "adapter_id": "adapter.verifier",
                "identity": {"provider_ref": "sample-verifier", "model_ref": "other-model"},
                "permissions": "read_only",
            },
            "checks": {
                "adapter_id": "adapter.checks",
                "identity": {"tool_ref": "fixture-check"},
                "permissions": "read_only",
            },
        },
        "roles": {"coding": "coding", "integration": "coding", "verifier": "verifier", "checks": "checks"},
        "fallback": "none",
        "default_provider": None,
        "default_model": None,
    }


def _checks(_: dict[str, Any]) -> dict[str, Any]:
    return {
        "checks": [
            {"id": "dependencies", "exit_code": 0, "stdout_digest": _raw_digest("dependencies"), "stderr_digest": _raw_digest("")},
            {"id": "build", "exit_code": 0, "stdout_digest": _raw_digest("build"), "stderr_digest": _raw_digest("")},
        ]
    }


def runner(*, checks: Any = _checks) -> CapabilityBoundRunner:
    return CapabilityBoundRunner(
        contract(),
        {
            "adapter.coding": lambda _: {"status": "changed"},
            "adapter.verifier": lambda _: {"status": "verified"},
            "adapter.checks": checks,
        },
    )


def candidate() -> dict[str, Any]:
    return {
        "node_id": "P4",
        "base_sha": "a" * 40,
        "candidate_sha": "b" * 40,
        "changed_paths": ["loop-hybrid/lh_runtime/runner_adapter.py"],
    }


def _check_results() -> list[dict[str, Any]]:
    return _checks({})["checks"]


def run_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    bound = runner()
    coding = bound.binding("coding", work_unit_id="wu-p4", attempt_id="attempt-p4:1")
    integration = bound.binding("integration", work_unit_id="wu-p4", attempt_id="attempt-p4:1")
    verifier = bound.binding("verifier", work_unit_id="wu-p4", attempt_id="attempt-p4:1")
    cases.append({
        "id": "capabilities-are-explicitly-bound",
        "ok": coding["capability"] == "coding"
        and integration["capability"] == "coding"
        and verifier["capability"] == "verifier"
        and coding["contract_digest"].startswith("sha256:"),
    })
    cases.append({"id": "worker-and-verifier-identities-differ", "ok": coding["identity_digest"] != verifier["identity_digest"]})

    missing = copy.deepcopy(contract())
    del missing["capabilities"]["verifier"]
    try:
        CapabilityBoundRunner(missing, {})
    except CapabilityError as exc:
        cases.append({"id": "missing-verifier-capability-rejects", "ok": exc.reason == "missing_capability:verifier"})
    else:
        cases.append({"id": "missing-verifier-capability-rejects", "ok": False})

    no_default = copy.deepcopy(contract())
    no_default["default_model"] = "implicit-model"
    try:
        CapabilityBoundRunner(no_default, {})
    except CapabilityError as exc:
        cases.append({"id": "default-model-is-forbidden", "ok": exc.reason == "default_identity_forbidden:default_model"})
    else:
        cases.append({"id": "default-model-is-forbidden", "ok": False})

    try:
        ReadOnlyWorkspace(tempfile.gettempdir()).write_text("x", "blocked")
    except VerifierProtocolError as exc:
        cases.append({"id": "verifier-source-write-is-denied", "ok": exc.reason == "verifier_source_write_denied"})
    else:
        cases.append({"id": "verifier-source-write-is-denied", "ok": False})

    protocol = VerifierProtocol(bound, node_id="P4")
    receipt = protocol.verify_candidate(
        candidate(),
        allowed_paths=["loop-hybrid/lh_runtime/"],
        worker_binding=coding,
        check_results=_check_results(),
        packet_digest="sha256:" + "c" * 64,
        plan_digest="sha256:" + "d" * 64,
        work_unit_id="wu-p4",
        attempt_id="attempt-p4:1",
    )
    receipt_body = {key: value for key, value in receipt.items() if key != "receipt_digest"}
    cases.append({"id": "green-receipt-is-digest-bound", "ok": receipt["verdict"] == "GREEN" and receipt["receipt_digest"] == digest_json(receipt_body), "detail": receipt["receipt_digest"]})

    failed = protocol.verify_candidate(
        candidate(),
        allowed_paths=["loop-hybrid/lh_runtime/"],
        worker_binding=coding,
        check_results=[{**_check_results()[0], "exit_code": 1}],
        packet_digest="sha256:" + "c" * 64,
        plan_digest="sha256:" + "d" * 64,
        work_unit_id="wu-p4",
        attempt_id="attempt-p4:2",
    )
    cases.append({"id": "check-red-routes-to-same-node", "ok": failed["verdict"] == "RED" and failed["route"] == "repair_same_node_new_attempt" and failed["node_id"] == "P4"})

    same_identity = copy.deepcopy(coding)
    same_identity["identity_digest"] = verifier["identity_digest"]
    try:
        protocol.verify_candidate(
            candidate(), allowed_paths=["loop-hybrid/lh_runtime/"], worker_binding=same_identity,
            check_results=_check_results(), packet_digest="sha256:" + "c" * 64,
            plan_digest="sha256:" + "d" * 64, work_unit_id="wu-p4", attempt_id="attempt-p4:3",
        )
    except VerifierProtocolError as exc:
        cases.append({"id": "same-verifier-identity-rejects", "ok": exc.reason == "verifier_identity_not_independent"})
    else:
        cases.append({"id": "same-verifier-identity-rejects", "ok": False})

    outside = copy.deepcopy(candidate())
    outside["changed_paths"] = ["scripts/host"]
    try:
        protocol.verify_candidate(
            outside, allowed_paths=["loop-hybrid/lh_runtime/"], worker_binding=coding,
            check_results=_check_results(), packet_digest="sha256:" + "c" * 64,
            plan_digest="sha256:" + "d" * 64, work_unit_id="wu-p4", attempt_id="attempt-p4:4",
        )
    except VerifierProtocolError as exc:
        cases.append({"id": "outside-path-rejects", "ok": exc.reason == "candidate_path_outside_allowed_set"})
    else:
        cases.append({"id": "outside-path-rejects", "ok": False})

    unavailable = CapabilityBoundRunner(contract(), {"adapter.coding": lambda _: {}})
    try:
        unavailable.binding("verifier", work_unit_id="wu-p4", attempt_id="attempt-p4:5")
    except CapabilityError as exc:
        cases.append({"id": "missing-adapter-rejects", "ok": exc.reason == "adapter_unavailable:adapter.verifier"})
    else:
        cases.append({"id": "missing-adapter-rejects", "ok": False})
    return cases


def main() -> int:
    cases = run_cases()
    failures = [case for case in cases if not case["ok"]]
    if failures:
        print(json.dumps({"status": "fail", "failures": failures}, ensure_ascii=False, indent=2))
        return 1
    print("runner-verifier: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Provider-free gate for split work graph and operator routing authority."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import capability_resolver as cr
import cli_agent_executor as executors
from _fixture import make_campaign, make_source_repo
from goal_loop_run import _evaluation_payload
from project_binding import CONTRACT_SCHEMA, resolve_project

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64


def _node(node_id: str, operation: str) -> dict[str, Any]:
    evaluation = operation == "evaluate_transition"
    return {
        "node_id": node_id,
        "operation": operation,
        "required_capabilities": (
            ["bounded_judgment"] if evaluation else ["repo_edit", "test_reasoning"]
        ),
        "required_tools": [] if evaluation else ["git", "shell"],
        "permissions": {
            "filesystem": "read_only" if evaluation else "workspace_write",
            "network": "external",
        },
        "data_boundary": "external",
        "minimum_context": 32000,
        "minimum_trust": "process_bound" if evaluation else "claimed",
        "inputs": {
            "authority_ref": "docs/spec.md#feature",
            "authority_digest": DIGEST_A,
        },
        "acceptance": (
            {"decision_space_ref": "lh:closed-decision-space"}
            if evaluation
            else {"lamp_ref": "campaign.stage.acceptance_lamp"}
        ),
        "independence": {
            "from_nodes": ["change"] if evaluation else [],
            "minimum_level": "model_family" if evaluation else "none",
        },
        "budget": {
            "max_wall_seconds": 300,
            "max_uncached_input_tokens": 32000,
            "max_output_tokens": 4000,
        },
        "fallback_policy": "human_required" if evaluation else "next_eligible",
    }


def work_graph() -> dict[str, Any]:
    return {
        "schema": cr.WORK_GRAPH_SCHEMA,
        "routing_profile": "operator-default",
        "nodes": [
            _node("change", "produce_change"),
            _node("evaluation", "evaluate_transition"),
        ],
    }


def _resource(
    binding_id: str,
    runner: str,
    capabilities: list[str],
    *,
    model: str | None = None,
    permission: str = "workspace_write",
    tools: list[str] | None = None,
) -> dict[str, Any]:
    identity = model or f"ambient:{runner}"
    resource = {
        "binding_id": binding_id,
        "executor_kind": "model",
        "runner": runner,
        "provider_ref": f"operator:{runner}",
        "model_family": identity,
        "endpoint_ref": f"ambient:{runner}",
        "capabilities": capabilities,
        "tools": tools if tools is not None else ["git", "shell"],
        "permission_ceiling": permission,
        "network_access": "external",
        "data_boundary": "external",
        "context_limit": 128000,
        "context_isolation": "fresh_process",
        "health": "healthy",
        "trust_tier": "process_bound" if model else "claimed",
        "eval_revision": "eval-bootstrap-1",
        "scores": {"quality": 1, "cost": 0, "latency": 0},
        "health_evidence": {
            "status": "healthy",
            "observed_at": "2026-07-24T11:00:00+00:00",
            "valid_until": "2027-07-24T11:00:00+00:00",
            "source_ref": "docs/archive/routing-live-evidence.json",
            "digest": DIGEST_B,
        },
        "score_evidence": {
            "eval_revision": "eval-bootstrap-1",
            "observed_at": "2026-07-24T11:00:00+00:00",
            "source_ref": "docs/archive/routing-live-evidence.json",
            "digest": DIGEST_C,
        },
    }
    if model:
        resource["model"] = model
    return resource


def routing_authority() -> dict[str, Any]:
    return {
        "schema": cr.ROUTING_AUTHORITY_SCHEMA,
        "profile_id": "operator-default",
        "owner": "loop-hybrid-operator",
        "revision": "routing-1",
        "issued_at": "2026-07-24T11:00:00+00:00",
        "valid_until": "2027-07-24T11:00:00+00:00",
        "previous_revision": "routing-bootstrap",
        "registry": {
            "revision": "registry-1",
            "resources": [
                _resource(
                    "edit-codex",
                    "codex",
                    ["repo_edit", "test_reasoning"],
                ),
                _resource(
                    "evaluate-codex",
                    "codex",
                    ["bounded_judgment"],
                    model="judge-codex",
                    permission="read_only",
                    tools=[],
                ),
            ],
        },
        "policy": {
            "revision": "policy-1",
            "owner": "loop-hybrid-operator",
            "approved_at": "2026-07-24T11:30:00+00:00",
            "rollback_revision": "policy-bootstrap",
            "evidence_ref": "docs/archive/routing-live-evidence.json",
            "evidence_digest": DIGEST_A,
            "weights": {"quality": 1, "cost": 0, "latency": 0},
            "allow_degraded": False,
            "retry": "next_eligible",
        },
    }


def _rejects(fn: Callable[[], Any]) -> bool:
    try:
        fn()
        return False
    except (ValueError, SystemExit):
        return True


def main() -> int:
    cases: list[dict[str, Any]] = []
    composed = cr.compose_graph(work_graph(), routing_authority(), at=NOW)
    change = cr.resolve_operation(composed, "produce_change")
    evaluation = cr.resolve_operation(
        composed,
        "evaluate_transition",
        selected_nodes={"change": change["resource"]},
    )
    change_binding = change["binding"]
    cases.append({
        "id": "split-authority-composes-and-binds-provenance",
        "ok": (
            change_binding["binding_id"] == "edit-codex"
            and evaluation["binding"]["binding_id"] == "evaluate-codex"
            and change_binding["routing_profile"] == "operator-default"
            and change_binding["registry_owner"] == "loop-hybrid-operator"
            and change_binding["resource_health_evidence_digest"] == DIGEST_B
            and change_binding["resource_score_evidence_digest"] == DIGEST_C
            and change_binding["policy_evidence_digest"] == DIGEST_A
            and change_binding["registry_rollback_revision"] == "routing-bootstrap"
        ),
        "detail": json.dumps(change_binding, sort_keys=True),
    })

    identity_work = work_graph()
    identity_work["nodes"][0]["provider"] = "forbidden"
    stale = routing_authority()
    stale["registry"]["resources"][0]["health_evidence"]["valid_until"] = (
        "2026-07-24T11:30:00+00:00"
    )
    owner_mismatch = routing_authority()
    owner_mismatch["policy"]["owner"] = "target-project"
    profile_mismatch = routing_authority()
    profile_mismatch["profile_id"] = "other-profile"
    bad_digest = routing_authority()
    bad_digest["registry"]["resources"][0]["score_evidence"]["digest"] = "claimed"
    cases.append({
        "id": "identity-staleness-owner-profile-and-digest-tamper-reject",
        "ok": all((
            _rejects(lambda: cr.validate_work_graph(identity_work)),
            _rejects(lambda: cr.compose_graph(work_graph(), stale, at=NOW)),
            _rejects(lambda: cr.compose_graph(work_graph(), owner_mismatch, at=NOW)),
            _rejects(lambda: cr.compose_graph(work_graph(), profile_mismatch, at=NOW)),
            _rejects(lambda: cr.compose_graph(work_graph(), bad_digest, at=NOW)),
        )),
        "detail": "all five authority violations rejected",
    })

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        profiles = root / "profiles"
        profiles.mkdir()
        (profiles / "operator-default.json").write_text(
            json.dumps(routing_authority()),
            encoding="utf-8",
        )
        contract = {
            "schema": CONTRACT_SCHEMA,
            "project_id": "split-routing-fixture",
            "campaign": make_campaign("split-routing-fixture"),
            "source_repo": str(source),
            "base_revision": base,
            "runtime": {
                "goal_store": "goals",
                "run_store": "runs",
                "workspace_root": "workspaces",
            },
            "work_graph": work_graph(),
        }
        contract_path = root / "project_runtime_contract.json"
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        resolved = resolve_project(
            contract_path,
            routing_profile_dir=profiles,
        )["run_kwargs"]["execution_graph"]
        contract["work_graph"]["routing_profile"] = "../escape"
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        escape_rejected = _rejects(lambda: resolve_project(
            contract_path,
            routing_profile_dir=profiles,
        ))
        cases.append({
            "id": "project-contract-selects-only-fixed-operator-profile-id",
            "ok": (
                resolved["routing_profile"] == "operator-default"
                and resolved["registry"]["owner"] == "loop-hybrid-operator"
                and escape_rejected
            ),
            "detail": json.dumps({
                "profile": resolved.get("routing_profile"),
                "owner": resolved["registry"].get("owner"),
                "escape_rejected": escape_rejected,
            }),
        })

    schema = {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["accept", "reject"]},
            "rationale": {"type": "string"},
        },
        "required": ["verdict", "rationale"],
        "additionalProperties": False,
    }
    codex_argv = executors.evaluation_argv(
        "codex",
        "prompt",
        "judge-codex",
        json_schema=schema,
    )
    envelope_payload = _evaluation_payload(
        "codex",
        json.dumps({
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "",
            "structured_output": {
                "verdict": "accept",
                "rationale": "fixture",
            },
        }),
        require_structured=True,
    )
    rejected_wires = [
        "plain text",
        '{"type":"result"}\n{"type":"result"}',
        json.dumps({
            "type": "result",
            "subtype": "success",
            "is_error": True,
            "structured_output": {
                "verdict": "accept",
                "rationale": "fixture",
            },
        }),
        json.dumps({
            "type": "result",
            "subtype": "error",
            "is_error": False,
            "structured_output": {
                "verdict": "accept",
                "rationale": "fixture",
            },
        }),
        json.dumps({
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "{\"verdict\":\"accept\",\"rationale\":\"bypass\"}",
        }),
        json.dumps({"verdict": "accept", "rationale": "naked bypass"}),
    ]
    cases.append({
        "id": "provider-neutral-evaluation-wire-is-schema-bound-before-parser",
        "ok": (
            codex_argv[:4] == ["codex", "exec", "-m", "judge-codex"]
            and "--sandbox" in codex_argv
            and "read-only" in codex_argv
            and "--dangerously-bypass-approvals-and-sandbox" not in codex_argv
            and json.loads(envelope_payload) == {
                "verdict": "accept",
                "rationale": "fixture",
            }
            and all(
                _rejects(
                    lambda wire=wire: _evaluation_payload(
                        "codex",
                        wire,
                        require_structured=True,
                    )
                )
                for wire in rejected_wires
            )
        ),
        "detail": json.dumps({
            "argv_flags": [
                item for item in codex_argv
                if item in {"--sandbox", "read-only"}
            ],
            "payload": envelope_payload,
        }),
    })

    failures = [
        {"id": case["id"], "detail": case["detail"]}
        for case in cases if not case["ok"]
    ]
    print(json.dumps({
        "check_id": "lh-routing-authority",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {
            "command": "python3 -B lh_runtime/routing_authority_canary.py",
            "provider_invocations": 0,
        },
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

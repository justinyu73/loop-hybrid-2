#!/usr/bin/env python3
"""Bounded live acceptance for capability routing on a disposable target clone.

The source checkout is read-only. LH creates its normal disposable clone,
resolves a produce-change resource, verifies one exact probe file, then resolves
an independent no-tools evaluator from the persisted Attempt binding. The
script prints a bounded evidence object and never pushes, merges, or publishes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import capability_resolver as cr
import cli_agent_executor as executors
import execution_fence as execution_fences
import execution_host_port as ehp
from campaign_compiler import CampaignCompiler
from goal_loop_run import CapabilityRoutingSession, _make_capability_evaluator, run
from goal_store import GoalStore
from run_store import RunStore

PROBE_PATH = "docs/agent-reports/lh-capability-live-probe.txt"
PROBE_CONTENT = "LH capability routing live acceptance\n"


def _run_text(argv: list[str], *, cwd: Path) -> str:
    proc = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _node(
    node_id: str,
    operation: str,
    *,
    authority_ref: str,
    authority_digest: str,
) -> dict[str, Any]:
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
            "authority_ref": authority_ref,
            "authority_digest": authority_digest,
        },
        "acceptance": (
            {"decision_space_ref": "lh:live-acceptance/accept-or-reject"}
            if evaluation
            else {"lamp_ref": "campaign.stage.acceptance_lamp"}
        ),
        "independence": {
            "from_nodes": ["change"] if evaluation else [],
            "minimum_level": "model_family" if evaluation else "none",
        },
        "budget": {
            "max_wall_seconds": 600 if not evaluation else 300,
            "max_uncached_input_tokens": 64000,
            "max_output_tokens": 4000 if not evaluation else 1000,
        },
        "fallback_policy": "human_required" if evaluation else "next_eligible",
    }


def _resource(
    binding_id: str,
    runner: str,
    capabilities: list[str],
    *,
    evidence_digest: str,
    observed_at: str,
    valid_until: str,
    model: str | None = None,
    permission: str = "workspace_write",
    tools: list[str] | None = None,
) -> dict[str, Any]:
    identity = model or f"ambient:{runner}"
    resource = {
        "binding_id": binding_id,
        "executor_kind": "model",
        "runner": runner,
        "provider_ref": f"operator-live:{runner}",
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
        "eval_revision": "example-project-live-1",
        "scores": {"quality": 1, "cost": 0, "latency": 0},
        "health_evidence": {
            "status": "healthy",
            "observed_at": observed_at,
            "valid_until": valid_until,
            "source_ref": "live-preflight:cli-version",
            "digest": evidence_digest,
        },
        "score_evidence": {
            "eval_revision": "example-project-live-1",
            "observed_at": observed_at,
            "source_ref": "live-preflight:neutral-bootstrap",
            "digest": evidence_digest,
        },
    }
    if model is not None:
        resource["model"] = model
    return resource


WINDOWS_ORCA_DEFAULT = "/mnt/c/Users/user/AppData/Local/Programs/orca/resources/bin/orca.exe"


def _resolve_live_orca_cli() -> str:
    """Same resolution as execution_host_port_live_canary: on this WSL host the
    running Orca is the Windows app; ``~/.local/bin/orca`` is a stale Linux
    install that answers runtime_unavailable (owner run 2026-08-24)."""
    explicit = os.environ.get("LH_ORCA_CLI")
    if explicit:
        return explicit
    if Path(WINDOWS_ORCA_DEFAULT).is_file():
        return WINDOWS_ORCA_DEFAULT
    return executors.resolve_orca_cli()


def _bootstrap_authority(trusted_root: Path) -> dict[str, str]:
    authority_path = trusted_root / "docs" / "bootstrap-authority.md"
    return {
        "decision_id": "LH-EXTERNAL-BOOTSTRAP-001",
        "authority_ref": "docs/bootstrap-authority.md#lh-external-bootstrap-001",
        "authority_digest": _sha256_bytes(authority_path.read_bytes()),
        "root": str(trusted_root),
    }


def _routing_inputs(source: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    authority_path = source / "docs" / "product-feature-map.md"
    authority_digest = _sha256_bytes(authority_path.read_bytes())
    now = datetime.now(timezone.utc)
    observed_at = now.isoformat()
    valid_until = (now + timedelta(hours=24)).isoformat()
    preflight = {
        "source_head": _run_text(["git", "rev-parse", "HEAD"], cwd=source),
        "codex_version": _run_text(["codex", "--version"], cwd=source),
        "claude_version": _run_text(["claude", "--version"], cwd=source),
        "observed_at": observed_at,
    }
    preflight_digest = cr.digest_json(preflight)
    work_graph = {
        "schema": cr.WORK_GRAPH_SCHEMA,
        "routing_profile": "example-project-live",
        "nodes": [
            _node(
                "change",
                "produce_change",
                authority_ref="docs/product-feature-map.md",
                authority_digest=authority_digest,
            ),
            _node(
                "evaluation",
                "evaluate_transition",
                authority_ref="docs/product-feature-map.md",
                authority_digest=authority_digest,
            ),
        ],
    }
    routing = {
        "schema": cr.ROUTING_AUTHORITY_SCHEMA,
        "profile_id": "example-project-live",
        "owner": "loop-hybrid-operator",
        "revision": "example-project-live-1",
        "issued_at": observed_at,
        "valid_until": valid_until,
        "previous_revision": "example-project-live-bootstrap",
        "registry": {
            "revision": "example-project-live-registry-1",
            "resources": [
                _resource(
                    "example-project-edit-codex",
                    "codex",
                    ["repo_edit", "test_reasoning"],
                    evidence_digest=preflight_digest,
                    observed_at=observed_at,
                    valid_until=valid_until,
                ),
                _resource(
                    "example-project-evaluate-claude",
                    "claude",
                    ["bounded_judgment"],
                    evidence_digest=preflight_digest,
                    observed_at=observed_at,
                    valid_until=valid_until,
                    model="sonnet",
                    permission="read_only",
                    tools=[],
                ),
            ],
        },
        "policy": {
            "revision": "example-project-live-policy-1",
            "owner": "loop-hybrid-operator",
            "approved_at": observed_at,
            "rollback_revision": "example-project-live-policy-bootstrap",
            "evidence_ref": "live-preflight:operator-approval",
            "evidence_digest": preflight_digest,
            "weights": {"quality": 1, "cost": 0, "latency": 0},
            "allow_degraded": False,
            "retry": "next_eligible",
        },
    }
    return work_graph, routing, preflight


def _campaign() -> dict[str, Any]:
    return {
        "schema": "lh-campaign/v1",
        "campaign_id": "example-project-capability-live",
        "stages": [{
            "stage_id": "disposable-probe",
            "goal": {
                "must_have": [
                    f"create {PROBE_PATH} with exactly the required one-line content",
                ],
                "must_not": [
                    "modify the source checkout",
                    "push",
                    "merge",
                    "publish",
                    "modify files outside docs/agent-reports/",
                ],
            },
            "allowed_paths": ["docs/agent-reports/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {
                "id": "example-project-capability-live-probe",
                "smoke": f"exact content at {PROBE_PATH}",
                "verification_argv": [
                    "python3",
                    "-c",
                    (
                        "from pathlib import Path; "
                        f"assert Path({PROBE_PATH!r}).read_text() == {PROBE_CONTENT!r}"
                    ),
                ],
            },
            "max_attempts": 2,
            "next_stage_id": None,
        }],
    }


def _seed(goal_root: Path, campaign: dict[str, Any]) -> str:
    envelope = CampaignCompiler(campaign).compile()["stages"]["disposable-probe"]
    goal_id = "example-project-capability-live:disposable-probe"
    GoalStore(goal_root).record_event(
        event_id="example-project-capability-live-seed",
        idempotency_key="example-project-capability-live-seed",
        source="manual_intent",
        event_type="goal_candidate",
        payload={"candidate": {
            "goal_id": goal_id,
            "campaign_id": campaign["campaign_id"],
            "stage_id": "disposable-probe",
            "goal": {
                "feature_contract": "disposable capability-routing probe",
                "admission_envelope": envelope,
            },
        }},
    )
    return goal_id


def _parse_evaluation(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("evaluation output is not one JSON object") from exc
    if not isinstance(value, dict) or set(value) != {"verdict", "rationale"}:
        raise ValueError("evaluation output does not match the closed key set")
    verdict = value.get("verdict")
    rationale = value.get("rationale")
    if verdict not in {"accept", "reject"}:
        raise ValueError("evaluation verdict is outside the closed decision set")
    if not isinstance(rationale, str) or not rationale.strip():
        raise ValueError("evaluation rationale must be non-empty")
    rationale = rationale.strip()
    if len(rationale) > 1000:
        raise ValueError("evaluation rationale exceeds 1000 characters")
    return {"verdict": verdict, "rationale": rationale}


def _preserve_failure_evidence(root: Path) -> Path:
    """Keep evaluator stdout/stderr after the disposable run root is removed."""
    durable = Path(tempfile.mkdtemp(prefix="lh-routing-failure-evidence-"))
    source = root / "runs" / "routing-evidence"
    if source.is_dir():
        shutil.copytree(source, durable / "routing-evidence")
    return durable


def _workspace_is_disposed_at(
    receipt: dict[str, Any],
    base_revision: str,
) -> bool:
    workspace = receipt.get("workspace")
    return bool(
        isinstance(workspace, dict)
        and workspace.get("disposable") is True
        and workspace.get("disposed") is True
        and workspace.get("base_revision") == base_revision
    )


def _evaluation_prompt(snapshot: dict[str, Any]) -> str:
    return (
        "Review only this bounded live-acceptance evidence. The producer ran in "
        "an LH disposable clone; the committed lamp and value reducer are the "
        "acceptance authorities. Return reject if the receipt is missing, the "
        "lamp/value verdict is not GREEN, or source checkout/promotion boundaries "
        "are not explicit. Return exactly one JSON object and no prose: "
        '{"verdict":"accept|reject","rationale":"short reason"}\n'
        + json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--base-revision", default="HEAD")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--execution-host",
        choices=("external-orca",),
        help="required with --execute since LH #93: a production model only runs through the ExecutionHostPort",
    )
    parser.add_argument(
        "--host-root",
        default="/path/to/external-host-checkout",
        help="external host checkout whose LH-EXTERNAL-BOOTSTRAP-001 authority doc is digest-bound into the binding",
    )
    parser.add_argument(
        "--keep-root",
        help="persist goals/runs/workspaces under this directory instead of a TemporaryDirectory (diagnosis)",
    )
    args = parser.parse_args(argv)
    source = Path(args.source_repo).resolve()
    if args.execute and args.execution_host is None:
        parser.error("--execute requires --execution-host external-orca")
    bootstrap_authority = None
    factory_overrides = None
    fence_port = None
    if args.execution_host is not None:
        bootstrap_authority = _bootstrap_authority(Path(args.host_root).resolve())
        os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = bootstrap_authority["root"]
        orca_cli_path = _resolve_live_orca_cli()
        # prepare() pins the Orca CLI from LH_ORCA_CLI (sandbox launch classes);
        # export the resolved path so the pin and the port agree.
        os.environ["LH_ORCA_CLI"] = orca_cli_path
        fence_port = execution_fences.configured_execution_fence()
        if isinstance(fence_port, execution_fences.DisabledExecutionFencePort):
            # Fail before the Attempt, not after: an unconfigured fence routes the
            # whole run to human_required without ever invoking the provider.
            parser.error(
                "--execution-host needs LH_EXECUTION_FENCE_BACKEND=linux-bubblewrap-seccomp "
                "(same value as deploy/systemd/loop-hybrid-supervisor.service.in)"
            )
        orca_cli = orca_cli_path

        def codex_execution_host_port(*, timeout_seconds: float):
            return ehp.make_execution_host_port(
                agent="codex",
                execution_host_binding={
                    "schema": "lh-execution-host-binding/v1",
                    "host_id": args.execution_host,
                    "adapter": "orca-terminal",
                    "bootstrap_authority": bootstrap_authority,
                },
                timeout_seconds=timeout_seconds,
                execution_fence_port=fence_port,
                orca_cli=orca_cli,
            )

        factory_overrides = {"codex": codex_execution_host_port}
    before = _run_text(["git", "status", "--porcelain=v1"], cwd=source)
    base = _run_text(["git", "rev-parse", args.base_revision], cwd=source)
    work_graph, authority, preflight = _routing_inputs(source)
    graph = cr.compose_graph(work_graph, authority)
    campaign = _campaign()
    import contextlib
    keep = (
        contextlib.nullcontext(str(Path(args.keep_root).resolve()))
        if args.keep_root
        else tempfile.TemporaryDirectory(prefix="lh-example-project-capability-")
    )
    with keep as raw:
        root = Path(raw)
        root.mkdir(parents=True, exist_ok=True)
        goal_id = _seed(root / "goals", campaign)
        result = run(
            execution_graph=graph,
            execute=args.execute,
            goal_store_root=root / "goals",
            run_store_root=root / "runs",
            workspace_root=root / "workspaces",
            campaign=campaign,
            source_repo=source,
            base_revision=base,
            max_cycles=30,
            max_runs=1,
            max_runtime_seconds=900,
            idle_limit=1,
            executor_timeout_seconds=600,
            execution_host=args.execution_host,
            bootstrap_authority=bootstrap_authority,
            factory_overrides=factory_overrides,
            # One fence instance for controller and port: descriptors are
            # prepared in-memory per instance, a second instance reads
            # descriptor_not_prepared at the verifier (run 4, 2026-08-23).
            execution_fence_port=fence_port,
        )
        if not args.execute:
            print(json.dumps({
                "schema": "lh-capability-live-acceptance-plan/v1",
                "mode": "dry_run",
                "source_repo": str(source),
                "source_revision": base,
                "routing": result["plan"]["routing"],
                "provider_invocations": 0,
            }, ensure_ascii=False, indent=2))
            return 0
        goal_store = GoalStore(root / "goals")
        goal = goal_store.get_goal(goal_id)
        if not isinstance(goal, dict) or not isinstance(goal.get("run_id"), str):
            raise RuntimeError(f"live probe produced no run_id: {goal}")
        run_id = goal["run_id"]
        store = RunStore(root / "runs")
        latest = store.latest_receipt(run_id)
        value = __import__("value_reducer").value_evidence_for_run(store, run_id, goal_store=goal_store)
        if (
            goal.get("state") != "completed"
            or not isinstance(latest, dict)
            or value.get("verdict") != "GREEN"
        ):
            if isinstance(latest, dict):
                failed_receipt = store.root / latest["receipt_ref"]
                print(f"receipt: {failed_receipt}", file=sys.stderr)
                print(failed_receipt.read_text(encoding="utf-8")[:8000], file=sys.stderr)
            raise RuntimeError(
                f"producer live probe did not close: goal={goal.get('state')} "
                f"value={value}"
            )
        receipt_path = store.root / latest["receipt_ref"]
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        producer_binding = receipt.get("binding")
        if not isinstance(producer_binding, dict):
            raise RuntimeError("producer receipt has no binding")
        after_producer = _run_text(
            ["git", "status", "--porcelain=v1"],
            cwd=source,
        )
        if before != after_producer:
            raise RuntimeError("source checkout changed during producer invocation")
        evaluator_session = CapabilityRoutingSession(
            graph,
            timeout_seconds=300,
            run_store_root=root / "runs",
        )
        evaluator = _make_capability_evaluator(
            evaluator_session,
            kind="live_acceptance",
            prompt_builder=_evaluation_prompt,
            parser=_parse_evaluation,
            output_schema={
                "type": "object",
                "properties": {
                    "verdict": {
                        "type": "string",
                        "enum": ["accept", "reject"],
                    },
                    "rationale": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 1000,
                    },
                },
                "required": ["verdict", "rationale"],
                "additionalProperties": False,
            },
        )
        workspace = receipt.get("workspace")
        workspace_proof = _workspace_is_disposed_at(receipt, base)
        if not workspace_proof:
            raise RuntimeError(
                "producer receipt does not prove a disposed disposable workspace "
                f"at source revision {base}: {workspace}"
            )
        verification_exit_code = receipt.get("verification", {}).get("exit_code")
        lamp_verdict = "GREEN" if verification_exit_code == 0 else "RED"
        evaluation_snapshot = {
            "run_id": run_id,
            "attempt": receipt.get("attempt"),
            "receipt_ref": latest["receipt_ref"],
            "receipt_digest": latest["receipt_digest"],
            "producer_binding_id": producer_binding.get("binding_id"),
            "producer_model_family": producer_binding.get("model_family"),
            "verification_exit_code": verification_exit_code,
            "lamp_verdict": lamp_verdict,
            "value_verdict": value.get("verdict"),
            "source_revision": base,
            "source_checkout_unchanged": before == after_producer,
            "disposable_workspace": workspace_proof,
            "workspace_ref": workspace.get("ref"),
            "workspace_base_revision": workspace.get("base_revision"),
            "promotion_performed": False,
        }
        try:
            evaluation = evaluator(evaluation_snapshot)
        except Exception:
            durable = _preserve_failure_evidence(root)
            print(
                f"evaluation failure evidence preserved at {durable}",
                file=sys.stderr,
            )
            raise
        evidence_dirs = sorted((store.root / "routing-evidence").glob("evaluation-*"))
        evaluation_receipt = json.loads(
            (evidence_dirs[-1] / "receipt.json").read_text(encoding="utf-8")
        )
        after = _run_text(["git", "status", "--porcelain=v1"], cwd=source)
        if before != after:
            raise RuntimeError("source checkout changed during disposable acceptance")
        evaluator_binding = evaluation_receipt
        accepted = (
            evaluation.get("verdict") == "accept"
            and evaluator_binding.get("binding_id") == "example-project-evaluate-claude"
            and evaluator_binding.get("model_family")
            != producer_binding.get("model_family")
            and evaluator_binding.get("exit_status") == "completed"
        )
        evidence = {
            "schema": "lh-capability-routing-live-evidence/v1",
            "status": "pass" if accepted else "fail",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_project": source.name,
            "source_repo": str(source),
            "source_revision": base,
            "source_checkout_unchanged": before == after,
            "disposable_workspace": workspace_proof,
            "promotion_performed": False,
            "provider_invocations": 2,
            "routing_profile": graph["routing_profile"],
            "routing_authority_digest": graph["routing_authority_digest"],
            "producer": {
                "run_id": run_id,
                "attempt": receipt.get("attempt"),
                "binding_id": producer_binding.get("binding_id"),
                "model_family": producer_binding.get("model_family"),
                "receipt_digest": latest["receipt_digest"],
                "verification_exit_code": verification_exit_code,
                "lamp_verdict": lamp_verdict,
                "value_verdict": value.get("verdict"),
            },
            "evaluator": {
                "binding_id": evaluator_binding.get("binding_id"),
                "model_family": evaluator_binding.get("model_family"),
                "independence_reference": evaluator_binding.get("independence_reference"),
                "exit_status": evaluator_binding.get("exit_status"),
                "evidence_digest": cr.digest_json(evaluator_binding),
                "verdict": evaluation.get("verdict"),
                "rationale": evaluation.get("rationale"),
            },
            "preflight": preflight,
        }
        if not accepted:
            evidence["failure_evidence_root"] = str(
                _preserve_failure_evidence(root)
            )
        print(json.dumps(evidence, ensure_ascii=False, indent=2))
        return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())

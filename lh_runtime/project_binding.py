#!/usr/bin/env python3
"""Resolve a Project Runtime Contract into the loop's run() kwargs.

This is the universal-engine seam: the loop stops being a single-project script
driven by loose CLI flags and becomes an engine bound to a project by a
machine-readable contract the project owns. Onboarding project #101 = writing its
contract; no LH code changes. run()/build_worker are unchanged — the resolver just
produces the kwarg bundle they already expect.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import assignment_packet
import capability_resolver as cr
import cli_agent_executor as executors
import delivery_binding
import instance_config as ic
import regression_watch
import scheduled_checks

CONTRACT_SCHEMA = "lh-project-runtime-contract/v1"
REQUIRED_RUNTIME = ("goal_store", "run_store", "workspace_root")
ROUTING_PROFILE_DIR = (
    Path(__file__).resolve().parent.parent
    / "deploy"
    / "model-routing"
    / "profiles"
)


def _validate_pricing(raw: Any) -> dict[str, dict[str, float]]:
    """``{model_id: {"input", "output", "cache_read"}}`` in USD per million tokens."""
    if not isinstance(raw, dict):
        raise SystemExit("contract.pricing must map model ids to rates")
    pricing: dict[str, dict[str, float]] = {}
    for model, rates in raw.items():
        if (not isinstance(model, str) or not model.strip() or not isinstance(rates, dict)
                or not {"input", "output"} <= set(rates) or set(rates) - {"input", "output", "cache_read"}
                or any(isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0
                       for value in rates.values())):
            raise SystemExit(f"contract.pricing.{model} must hold non-negative input/output[/cache_read] rates")
        pricing[model] = {key: float(value) for key, value in rates.items()}
    return pricing


def resolve_project(
    contract_path: str | Path,
    *,
    routing_profile_dir: str | Path | None = None,
    instance_config_path: str | Path | None = None,
    assignment_packet_path: str | Path | None = None,
    assignment_correlation_id: str | None = None,
) -> dict[str, Any]:
    """Load a contract file and return {project_id, run_kwargs} for run()."""
    path = Path(contract_path).resolve()
    if not path.exists():
        raise SystemExit(f"contract not found: {path}")
    contract = json.loads(path.read_text(encoding="utf-8"))
    if contract.get("schema") != CONTRACT_SCHEMA:
        raise SystemExit(f"unsupported contract schema: {contract.get('schema')!r} (want {CONTRACT_SCHEMA})")
    for field in ("project_id", "campaign", "source_repo", "base_revision", "runtime"):
        if not contract.get(field):
            raise SystemExit(f"contract missing required field: {field}")
    selected_instance_path = ic.discover_instance_config(
        instance_config_path,
        include_default=False,
    )
    instance = ic.InstanceConfig.load(selected_instance_path) if selected_instance_path is not None else None
    base_dir = path.parent

    def resolve(rel: str, kind: str = "repo") -> str:
        candidate = Path(rel)
        if candidate.is_absolute():
            return str(candidate.resolve(strict=False))
        if instance is not None:
            return instance.resolve_path(kind, candidate)
        return str((base_dir / candidate).resolve())

    source_repo = resolve(contract["source_repo"], "repo")
    assignment_binding: dict[str, Any] | None = None
    if assignment_packet_path is not None:
        try:
            packet = assignment_packet.load_assignment_packet(
                assignment_packet_path,
                project_id=str(contract["project_id"]),
                campaign_id=str(contract["campaign"]["campaign_id"]),
                source_repo=source_repo,
                expected_correlation_id=assignment_correlation_id,
            )
        except (OSError, ValueError) as exc:
            raise SystemExit(f"assignment packet invalid: {exc}") from exc
        target_contract = packet.get("target_contract")
        if not isinstance(target_contract, dict):
            raise SystemExit("assignment packet did not provide a target contract")
        packet_assignment = packet.get("assignment")
        if not isinstance(packet_assignment, dict):
            raise SystemExit("assignment packet did not provide a normalized assignment")
        if target_contract.get("source_repo") != contract.get("source_repo"):
            raise SystemExit("assignment packet target source binding differs from contract")
        contract = target_contract
        source_repo = resolve(contract["source_repo"], "repo")
        assignment_binding = packet["binding"]

    runtime = contract["runtime"]
    if not isinstance(runtime, dict):
        raise SystemExit("contract.runtime must be an object")
    for field in REQUIRED_RUNTIME:
        if not runtime.get(field):
            raise SystemExit(f"contract.runtime missing required field: {field}")

    campaign = json.loads(json.dumps(contract["campaign"], ensure_ascii=False))
    if assignment_binding is not None:
        stages = campaign.get("stages")
        stage_id = assignment_binding.get("stage_id")
        stage = next(
            (
                item for item in stages
                if isinstance(item, dict) and item.get("stage_id") == stage_id
            ),
            None,
        ) if isinstance(stages, list) else None
        if not isinstance(stage, dict):
            raise SystemExit("assignment packet stage is missing from the target contract")
        existing = stage.get("goal_assignment")
        if existing is not None:
            try:
                existing = assignment_packet.goal_assignment.normalize_assignment(existing)
            except ValueError as exc:
                raise SystemExit(f"target contract goal_assignment is invalid: {exc}") from exc
            if existing != packet_assignment:
                raise SystemExit("target contract goal_assignment conflicts with the packet")
        stage["goal_assignment"] = packet_assignment

    # Stages that opt in with "delivery": {"derive": "acceptance_lamp"} get a
    # sealed delivery binding from their own lamp, bound to this file's bytes;
    # every other stage is returned unchanged.
    try:
        campaign = delivery_binding.compile_campaign_delivery(
            campaign,
            contract_ref=str(path),
            contract_digest="sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
            source_repo=source_repo,
            base_revision=str(contract["base_revision"]),
        )
    except delivery_binding.DeliveryBindingError as exc:
        raise SystemExit(f"contract delivery binding refused: {exc}") from exc

    run_kwargs: dict[str, Any] = {
        "campaign": campaign,  # deep-validated by CampaignCompiler in build_worker
        "source_repo": source_repo,
        "base_revision": str(contract["base_revision"]),
        "goal_store_root": resolve(runtime["goal_store"], "state"),
        "run_store_root": resolve(runtime["run_store"], "state"),
        "workspace_root": resolve(runtime["workspace_root"], "workspace"),
    }
    if contract.get("planner_recovery") is not None:
        # Carry the original seal, not a synthesized execution authority.
        if assignment_binding is not None:
            raise SystemExit("native recovery requires the original project contract")
        run_kwargs["planner_recovery"] = {
            "contract_ref": str(path),
            "contract_digest": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
            "binding": contract["planner_recovery"],
        }
    if assignment_binding is not None:
        run_kwargs["assignment_binding"] = assignment_binding
    if runtime.get("status_snapshot_out"):
        run_kwargs["status_snapshot_out"] = resolve(runtime["status_snapshot_out"], "logs")
    if runtime.get("pause_flag"):
        run_kwargs["pause_flag"] = resolve(runtime["pause_flag"], "state")
    if runtime.get("knowledge_store"):
        run_kwargs["knowledge_store_root"] = resolve(runtime["knowledge_store"], "cache")
    knowledge_repos = runtime.get("knowledge_repos")
    if knowledge_repos is not None:
        if not isinstance(knowledge_repos, list) or not all(isinstance(item, str) and item.strip() for item in knowledge_repos):
            raise SystemExit("contract.runtime.knowledge_repos must be an array of non-empty strings")
        run_kwargs["knowledge_repo_roots"] = tuple(resolve(item, "repo") for item in knowledge_repos)
    if contract.get("executors") is not None:
        try:
            run_kwargs["executor_declarations"] = executors.validate_executor_declarations(contract["executors"])
        except ValueError as exc:
            raise SystemExit(f"contract.executors invalid: {exc}") from exc
    if contract.get("pricing") is not None:
        run_kwargs["pricing"] = _validate_pricing(contract["pricing"])
    if contract.get("regression_watch") is not None:
        try:
            run_kwargs["regression_watch"] = regression_watch.validate_config(contract["regression_watch"])
        except ValueError as exc:
            raise SystemExit(f"contract.regression_watch invalid: {exc}") from exc
    if contract.get("scheduled_checks") is not None:
        try:
            run_kwargs["scheduled_checks"] = scheduled_checks.validate_config(contract["scheduled_checks"])
        except ValueError as exc:
            raise SystemExit(f"contract.scheduled_checks invalid: {exc}") from exc
    models = contract.get("models")
    execution_graph = contract.get("execution_graph")
    work_graph = contract.get("work_graph")
    configured = [
        name for name, value in (
            ("models", models),
            ("execution_graph", execution_graph),
            ("work_graph", work_graph),
        )
        if value is not None
    ]
    if len(configured) > 1:
        raise SystemExit(
            "contract models, execution_graph, and work_graph are mutually exclusive"
        )
    if work_graph is not None:
        try:
            normalized_work = cr.validate_work_graph(work_graph)
            profile_root = Path(
                routing_profile_dir
                if routing_profile_dir is not None
                else ROUTING_PROFILE_DIR
            ).resolve()
            profile_path = (
                profile_root / f"{normalized_work['routing_profile']}.json"
            ).resolve()
            if not profile_path.is_relative_to(profile_root):
                raise ValueError("routing profile escapes the operator profile directory")
            if not profile_path.is_file():
                raise ValueError(
                    f"operator routing profile not found: {profile_path}"
                )
            routing_authority = json.loads(
                profile_path.read_text(encoding="utf-8")
            )
            run_kwargs["execution_graph"] = cr.compose_graph(
                normalized_work,
                routing_authority,
            )
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise SystemExit(f"contract.work_graph invalid: {exc}") from exc
    if execution_graph is not None:
        try:
            run_kwargs["execution_graph"] = cr.validate_graph(execution_graph)
        except ValueError as exc:
            raise SystemExit(f"contract.execution_graph invalid: {exc}") from exc
    if models is not None:
        if not isinstance(models, dict) or not isinstance(models.get("execute"), str):
            raise SystemExit("contract.models must be an object with a string 'execute' field")
        for optional in ("judge", "judge_model"):
            if optional in models and not isinstance(models[optional], str):
                raise SystemExit(f"contract.models.{optional} must be a string")
        run_kwargs["executor"] = models["execute"]
        execute_binding = models.get("execute_binding")
        if execute_binding is not None:
            if not isinstance(execute_binding, dict) or set(execute_binding) != {"runner", "base_url", "model"}:
                raise SystemExit("contract.models.execute_binding must contain exactly runner, base_url, and model")
            if any(not isinstance(execute_binding.get(field), str) or not execute_binding[field].strip() for field in ("runner", "base_url", "model")):
                raise SystemExit("contract.models.execute_binding fields must be non-empty strings")
            run_kwargs["executor_binding"] = {field: execute_binding[field].strip() for field in ("runner", "base_url", "model")}
        if models.get("judge"):
            run_kwargs["judge_executor"] = models["judge"]
        if models.get("judge_model"):
            run_kwargs["judge_model"] = models["judge_model"]
        run_kwargs["compatibility_authority"] = {
            "authority_ref": str(path),
            "authority_digest": cr.digest_json(contract),
        }
    if "external_verdict" in contract:
        raise SystemExit("contract.external_verdict is not supported; inject a verdict store and "
                         "conclusion source through the engine API")
    result: dict[str, Any] = {"project_id": contract["project_id"], "run_kwargs": run_kwargs}
    if instance is not None:
        result["instance_config"] = instance.readback()
        result["runtime_environment"] = instance.environment_overlay()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve a Project Runtime Contract (prints the run kwargs)")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--instance-config", default=None)
    args = parser.parse_args(argv)
    print(json.dumps(resolve_project(args.contract, instance_config_path=args.instance_config), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

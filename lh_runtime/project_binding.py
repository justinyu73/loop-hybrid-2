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
import json
from pathlib import Path
from typing import Any

CONTRACT_SCHEMA = "lh-project-runtime-contract/v1"
REQUIRED_RUNTIME = ("goal_store", "run_store", "workspace_root")


def resolve_project(contract_path: str | Path) -> dict[str, Any]:
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
    runtime = contract["runtime"]
    for field in REQUIRED_RUNTIME:
        if not runtime.get(field):
            raise SystemExit(f"contract.runtime missing required field: {field}")

    base_dir = path.parent

    def resolve(rel: str) -> str:
        candidate = Path(rel)
        return str(candidate if candidate.is_absolute() else (base_dir / candidate).resolve())

    run_kwargs: dict[str, Any] = {
        "campaign": contract["campaign"],  # deep-validated by CampaignCompiler in build_worker
        "source_repo": resolve(contract["source_repo"]),
        "base_revision": str(contract["base_revision"]),
        "goal_store_root": resolve(runtime["goal_store"]),
        "run_store_root": resolve(runtime["run_store"]),
        "workspace_root": resolve(runtime["workspace_root"]),
    }
    if runtime.get("status_snapshot_out"):
        run_kwargs["status_snapshot_out"] = resolve(runtime["status_snapshot_out"])
    if runtime.get("pause_flag"):
        run_kwargs["pause_flag"] = resolve(runtime["pause_flag"])
    models = contract.get("models")
    if models is not None:
        if not isinstance(models, dict) or not isinstance(models.get("execute"), str):
            raise SystemExit("contract.models must be an object with a string 'execute' field")
        for optional in ("judge", "judge_model"):
            if optional in models and not isinstance(models[optional], str):
                raise SystemExit(f"contract.models.{optional} must be a string")
        run_kwargs["executor"] = models["execute"]
        if models.get("judge"):
            run_kwargs["judge_executor"] = models["judge"]
        if models.get("judge_model"):
            run_kwargs["judge_model"] = models["judge_model"]
    return {"project_id": contract["project_id"], "run_kwargs": run_kwargs}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve a Project Runtime Contract (prints the run kwargs)")
    parser.add_argument("--contract", required=True)
    args = parser.parse_args(argv)
    print(json.dumps(resolve_project(args.contract), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

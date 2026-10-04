#!/usr/bin/env python3
"""Provider-free smoke for the Project Runtime Contract resolver (universal binding)."""
from __future__ import annotations

import json
import contextlib
import io
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import make_source_repo
from campaign_canary import campaign as fixture_campaign
from campaign_compiler import CampaignCompiler
from goal_loop_driver import run_driver
from goal_loop_run import build_worker, main as goal_loop_main
from instance_config import initialize_instance
from project_binding import CONTRACT_SCHEMA, resolve_project
from native_delivery_fixture import make_native_bundle


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "binding fixture model"}


def _stage_binding(campaign: dict, source: Path, base: str, *, campaign_id: str) -> dict:
    compiled = CampaignCompiler(campaign).compile()["stages"]
    for stage in campaign["stages"]:
        stage_id = stage["stage_id"]
        if stage.get("human_only") is True or "max_attempts" not in stage:
            continue
        binding = make_native_bundle(
            source,
            base,
            f"{campaign_id}:{stage_id}",
            stage_id,
            [{
                "id": "project-binding-source-check",
                "commands": [{
                    "id": "diff-check",
                    "argv": ["git", "diff", "--cached", "--check"],
                    "cwd": "${WORKTREE}",
                    "expect_exit": 0,
                    "timeout_seconds": 10,
                }],
                "required_receipts": ["executor"],
            }],
            [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if any(Path('src').glob('attempt-*.txt')) else 1)"],
            ["src/"],
            int(stage["max_attempts"]),
            goal={"feature_contract": stage["goal"], "admission_envelope": compiled[stage_id]},
        )
        stage["goal"] = {
            **stage["goal"],
            "delivery_required": True,
            "delivery_contract": binding["contract"],
            "delivery_plan": binding["plan"],
            "delivery_packet": binding["packet"],
        }
    return campaign


def _seed(worker, compiler, *, goal_id: str, stage_id: str, event_key: str) -> None:
    envelope = compiler.compile()["stages"][stage_id]
    campaign_id = next(iter(worker.execution_context))
    context = worker.execution_context[campaign_id]
    source = Path(context["source_repo"])
    base = str(context["base_revision"])
    binding = make_native_bundle(
        source,
        base,
        goal_id,
        stage_id,
        [{
            "id": "project-binding-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if any(Path('src').glob('attempt-*.txt')) else 1)"],
        ["src/"],
        int(envelope["max_attempts"]),
        goal={"feature_contract": stage_id, "admission_envelope": envelope},
    )
    worker.goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": campaign_id, "stage_id": stage_id, "goal": binding["goal"]}
    })


def _write_contract(root: Path, source: Path, base: str, *, source_ref: str | None = None) -> Path:
    campaign = _stage_binding(fixture_campaign(), source, base, campaign_id="campaign-g2-fixture")
    contract = {
        "schema": CONTRACT_SCHEMA,
        "project_id": "demo-project",
        "campaign": campaign,
        "source_repo": str(source) if source_ref is None else source_ref,
        "base_revision": base,
        "runtime": {
            "goal_store": "runtime/goals",
            "run_store": "runtime/runs",
            "workspace_root": "runtime/ws",
            "status_snapshot_out": "runtime/platform_status.json",
        },
    }
    path = root / "project_runtime_contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        contract_path = _write_contract(root, source, base)

        resolved = resolve_project(contract_path)
        kw = resolved["run_kwargs"]

        # contract-relative runtime paths resolve next to the contract file
        expect_goals = str((root / "runtime" / "goals").resolve())
        resolve_ok = (
            resolved["project_id"] == "demo-project"
            and kw["source_repo"] == str(source.resolve())
            and kw["base_revision"] == base
            and kw["goal_store_root"] == expect_goals
            and kw["campaign"]["campaign_id"] == "campaign-g2-fixture"
            and kw["status_snapshot_out"] == str((root / "runtime" / "platform_status.json").resolve())
        )

        # the resolved bundle is a WORKING binding: build a worker from it and drive it
        worker = build_worker(
            goal_store_root=kw["goal_store_root"],
            run_store_root=kw["run_store_root"],
            workspace_root=kw["workspace_root"],
            campaign=kw["campaign"],
            source_repo=kw["source_repo"],
            base_revision=kw["base_revision"],
        )
        compiler = worker.compilers["campaign-g2-fixture"]
        _seed(worker, compiler, goal_id="campaign-g2-fixture:stage-1", stage_id="stage-1", event_key="bind-seed-1")
        summary = run_driver(worker, holder="bind", model=_model, max_cycles=30, sleep_fn=lambda _s: None)
        # Binding works iff the resolved bundle cloned source_repo@base_revision and ran the loop.
        # This fixture intentionally has no judge and a lamp that stays red; FC-P0 therefore parks
        # the goal instead of silently consuming the final attempt. Completion versus the expected
        # parked result is orthogonal to the project-binding contract.
        drove = (
            summary["runs_dispatched"] >= 1
            and (len(summary["outcomes"]) >= 1 or len(summary["parked_goals"]) >= 1)
        )

        def _bad(mutate) -> bool:
            bad = json.loads(contract_path.read_text())
            mutate(bad)
            p = root / "bad.json"
            p.write_text(json.dumps(bad), encoding="utf-8")
            try:
                resolve_project(p)
                return False
            except SystemExit:
                return True

        bad_schema = _bad(lambda c: c.__setitem__("schema", "wrong/v9"))
        missing_field = _bad(lambda c: c.__delitem__("source_repo"))

        # The engine wires no hosted service: a contract that still carries an
        # external_verdict block is refused instead of being silently ignored.
        service_contract = json.loads(contract_path.read_text())
        service_contract["external_verdict"] = {"owner": "o", "repo": "r", "workflow": "CI"}
        service_path = root / "external-verdict.json"
        service_path.write_text(json.dumps(service_contract), encoding="utf-8")
        try:
            resolve_project(service_path)
            external_verdict_refused = False
        except SystemExit:
            external_verdict_refused = True

        # An instance-owned binding changes only installation placement and
        # executable environment; project intent remains in the contract.
        configured_repo_root = root / "configured repo"
        configured_source, configured_base = make_source_repo(configured_repo_root)
        contract_root = root / "contract"
        contract_root.mkdir()
        configured_contract = _write_contract(
            contract_root,
            configured_source,
            configured_base,
            source_ref="source",
        )
        instance = initialize_instance(
            root / "instance" / "instance.json",
            system="Linux",
            environ={"HOME": str(root / "clean-user"), "PATH": ""},
            cwd=root,
            overrides={
                "paths": {
                    "repo": str(configured_repo_root),
                    "state": str(root / "configured state"),
                    "workspace": str(root / "configured workspace"),
                    "cache": str(root / "configured cache"),
                    "logs": str(root / "configured logs"),
                },
            },
        )
        configured = resolve_project(configured_contract, instance_config_path=instance.path)
        configured_kw = configured["run_kwargs"]
        prior_instance_env = os.environ.get("LH_INSTANCE_CONFIG")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            cli_status = goal_loop_main([
                "--contract", str(configured_contract),
                "--instance-config", str(instance.path),
                "--executor", "codex",
            ])
        config_consumed = (
            cli_status == 0
            and configured_kw["source_repo"] == str(configured_source.resolve())
            and configured_kw["goal_store_root"] == str((root / "configured state" / "runtime" / "goals").resolve())
            and configured_kw["workspace_root"] == str((root / "configured workspace" / "runtime" / "ws").resolve())
            and configured["instance_config"]["config_digest"] == instance.config_digest
            and os.environ.get("LH_INSTANCE_CONFIG") == prior_instance_env
            and "configured state" in output.getvalue()
        )

        cases = [
            case("resolves-contract-to-run-kwargs", resolve_ok, json.dumps({k: kw[k] for k in ("source_repo", "base_revision", "goal_store_root")})),
            case("resolved-bundle-drives-the-loop", drove, str(summary)),
            case("bad-schema-and-missing-field-rejected", bad_schema and missing_field, f"bad_schema={bad_schema} missing_field={missing_field}"),
            case("external-verdict-service-block-refused", external_verdict_refused,
                 f"refused={external_verdict_refused}"),
            case("instance-config-is-consumed-by-binding-and-cli", config_consumed,
                 json.dumps({"status": cli_status, "binding": configured_kw, "output": output.getvalue()[-300:]}, ensure_ascii=False)),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-project-binding",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "resolver produces run() kwargs on the LH side; SH-side project_id->contract command-down is a later slice",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

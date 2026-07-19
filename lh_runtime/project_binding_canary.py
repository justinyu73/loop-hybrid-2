#!/usr/bin/env python3
"""Provider-free smoke for the Project Runtime Contract resolver (universal binding)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_source_repo
from campaign_canary import campaign as fixture_campaign
from goal_loop_driver import run_driver
from goal_loop_run import build_worker
from project_binding import CONTRACT_SCHEMA, resolve_project


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "binding fixture model"}


def _seed(worker, compiler, *, goal_id: str, stage_id: str, event_key: str) -> None:
    envelope = compiler.compile()["stages"][stage_id]
    worker.goal_store.record_event(event_id=event_key, idempotency_key=event_key, source="manual_intent", event_type="goal_candidate", payload={
        "candidate": {"goal_id": goal_id, "campaign_id": worker.execution_context and next(iter(worker.execution_context)), "stage_id": stage_id, "goal": {"feature_contract": stage_id, "admission_envelope": envelope}}
    })


def _write_contract(root: Path, source: Path, base: str) -> Path:
    contract = {
        "schema": CONTRACT_SCHEMA,
        "project_id": "demo-project",
        "campaign": fixture_campaign(),
        "source_repo": str(source),  # absolute; resolver also accepts contract-relative
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
        # binding works iff the resolved bundle cloned source_repo@base_revision and ran the loop
        # (a run was dispatched and reduced to a terminal outcome); stage completion depends on the
        # campaign's acceptance lamp, which is orthogonal to the binding.
        drove = summary["runs_dispatched"] >= 1 and len(summary["outcomes"]) >= 1

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

        cases = [
            case("resolves-contract-to-run-kwargs", resolve_ok, json.dumps({k: kw[k] for k in ("source_repo", "base_revision", "goal_store_root")})),
            case("resolved-bundle-drives-the-loop", drove, str(summary)),
            case("bad-schema-and-missing-field-rejected", bad_schema and missing_field, f"bad_schema={bad_schema} missing_field={missing_field}"),
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

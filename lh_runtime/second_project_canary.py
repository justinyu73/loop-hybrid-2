#!/usr/bin/env python3
"""Committed B5 smoke: one engine, two independent fixture projects (universal binding).

Proves the Project Runtime Contract is not a single-project convenience: two
distinct projects — own source repo, own contract dir, own campaign_id, own
runtime roots — resolve and drive through the SAME resolver + worker path,
with disjoint stores and per-project status output.  Additive canary only;
resolver / build_worker / run_driver are reused unchanged.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _fixture import make_campaign, make_source_repo
from goal_loop_driver import run_driver
from goal_loop_run import build_worker
from goal_store import GoalStore
from project_binding import CONTRACT_SCHEMA, resolve_project
from run_store import RunStore


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _model(workspace: Path, capsule: dict) -> dict:
    path = workspace / "src"
    path.mkdir(exist_ok=True)
    (path / f"attempt-{capsule['attempt']}.txt").write_text("bounded\n", encoding="utf-8")
    return {"summary": "b5 fixture model"}


def _make_project(root: Path, name: str, campaign_id: str) -> dict[str, object]:
    """One independent fixture project: contract dir + own source repo + contract."""
    proj = root / name
    proj.mkdir()
    source, base = make_source_repo(proj, user=f"b5-{name}")
    contract = {
        "schema": CONTRACT_SCHEMA,
        "project_id": f"b5-project-{name}",
        "campaign": make_campaign(campaign_id, stage_id="stage-only"),
        "source_repo": str(source),
        "base_revision": base,
        "runtime": {
            "goal_store": "runtime/goals",
            "run_store": "runtime/runs",
            "workspace_root": "runtime/ws",
            "status_snapshot_out": "runtime/platform_status.json",
        },
    }
    contract_path = proj / "project_runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    return {"name": name, "dir": proj, "campaign_id": campaign_id, "contract_path": contract_path}


def _drive(project: dict[str, object]) -> dict[str, object]:
    """Resolve the contract, seed one candidate, and drive the loop via run_driver."""
    resolved = resolve_project(Path(project["contract_path"]))
    kw = resolved["run_kwargs"]
    worker = build_worker(
        goal_store_root=kw["goal_store_root"],
        run_store_root=kw["run_store_root"],
        workspace_root=kw["workspace_root"],
        campaign=kw["campaign"],
        source_repo=kw["source_repo"],
        base_revision=kw["base_revision"],
    )
    campaign_id = project["campaign_id"]
    goal_id = f"{campaign_id}:stage-only"
    envelope = worker.compilers[campaign_id].compile()["stages"]["stage-only"]
    worker.goal_store.record_event(
        event_id=f"b5-seed-{project['name']}",
        idempotency_key=f"b5-seed-{project['name']}",
        source="manual_intent",
        event_type="goal_candidate",
        payload={"candidate": {"goal_id": goal_id, "campaign_id": campaign_id, "stage_id": "stage-only", "goal": {"feature_contract": "stage-only", "admission_envelope": envelope}}},
    )
    summary = run_driver(
        worker,
        holder=f"b5-{project['name']}",
        model=_model,
        max_cycles=30,
        sleep_fn=lambda _s: None,
        status_snapshot_out=kw["status_snapshot_out"],
    )
    return {
        "project_id": resolved["project_id"],
        "run_kwargs": kw,
        "summary": summary,
        "goal_id": goal_id,
        "goal_state": worker.goal_store.get_goal(goal_id)["state"],
    }


def main() -> int:
    cases: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        proj_a = _make_project(root, "alpha", "campaign-b5-alpha")
        proj_b = _make_project(root, "beta", "campaign-b5-beta")

        driven_a = _drive(proj_a)
        driven_b = _drive(proj_b)

        # 1. Both projects bind and drive to a terminal outcome through the
        #    same engine — onboarding a new project is writing a contract.
        drove = (
            driven_a["summary"]["runs_dispatched"] >= 1
            and driven_b["summary"]["runs_dispatched"] >= 1
            and driven_a["goal_state"] == "completed"
            and driven_b["goal_state"] == "completed"
        )
        cases.append(case(
            "both-projects-bind-and-drive",
            drove,
            json.dumps({
                "a": {"dispatched": driven_a["summary"]["runs_dispatched"], "goal": driven_a["goal_state"]},
                "b": {"dispatched": driven_b["summary"]["runs_dispatched"], "goal": driven_b["goal_state"]},
            }),
        ))

        # 2. Contract-relative runtime paths resolve per contract; the two
        #    projects' runtime roots are disjoint subtrees.
        kw_a, kw_b = driven_a["run_kwargs"], driven_b["run_kwargs"]
        roots_a = {kw_a["goal_store_root"], kw_a["run_store_root"], kw_a["workspace_root"]}
        roots_b = {kw_b["goal_store_root"], kw_b["run_store_root"], kw_b["workspace_root"]}
        under_a = all(str(path).startswith(str(proj_a["dir"].resolve())) for path in roots_a)
        under_b = all(str(path).startswith(str(proj_b["dir"].resolve())) for path in roots_b)
        cases.append(case(
            "relative-paths-resolve-per-contract",
            under_a and under_b and roots_a.isdisjoint(roots_b),
            json.dumps({"a": sorted(roots_a), "b": sorted(roots_b)}),
        ))

        # 3. Campaign isolation: each store holds only its own campaign/goals.
        store_a = GoalStore(kw_a["goal_store_root"])
        store_b = GoalStore(kw_b["goal_store_root"])
        goals_a = store_a.goals_in_state("completed")
        goals_b = store_b.goals_in_state("completed")
        runs_a = RunStore(kw_a["run_store_root"]).summary()["runs_by_state"]
        runs_b = RunStore(kw_b["run_store_root"]).summary()["runs_by_state"]
        isolated = (
            goals_a
            and goals_b
            and all(goal["campaign_id"] == "campaign-b5-alpha" for goal in goals_a)
            and all(goal["campaign_id"] == "campaign-b5-beta" for goal in goals_b)
            and all(not goal["goal_id"].startswith("campaign-b5-beta") for goal in goals_a)
            and all(not goal["goal_id"].startswith("campaign-b5-alpha") for goal in goals_b)
            and runs_a and runs_b
        )
        cases.append(case(
            "campaign-isolation",
            bool(isolated),
            json.dumps({
                "a_goals": [goal["goal_id"] for goal in goals_a],
                "b_goals": [goal["goal_id"] for goal in goals_b],
                "a_runs": runs_a, "b_runs": runs_b,
            }),
        ))

        # 4. Per-project status output: each project materializes its own
        #    snapshot at its own path, and each snapshot reports its own
        #    store roots (snapshot schema v1 has no project_id field; roots
        #    are the per-project identity — see known_gaps_open).
        snap_a_path = Path(kw_a["status_snapshot_out"])
        snap_b_path = Path(kw_b["status_snapshot_out"])
        snap_a = json.loads(snap_a_path.read_text(encoding="utf-8")) if snap_a_path.exists() else None
        snap_b = json.loads(snap_b_path.read_text(encoding="utf-8")) if snap_b_path.exists() else None
        status_ok = (
            snap_a is not None
            and snap_b is not None
            and snap_a_path != snap_b_path
            and snap_a["run_store_root"] == str(kw_a["run_store_root"])
            and snap_b["run_store_root"] == str(kw_b["run_store_root"])
            and snap_a["run_store_root"] != snap_b["run_store_root"]
            and snap_a["goal_store_root"] != snap_b["goal_store_root"]
        )
        cases.append(case(
            "status-output-per-project",
            status_ok,
            json.dumps({
                "a": {"path": str(snap_a_path), "run_root": None if snap_a is None else snap_a["run_store_root"]},
                "b": {"path": str(snap_b_path), "run_root": None if snap_b is None else snap_b["run_store_root"]},
            }),
        ))

        # 5. Malformed contracts are still rejected in the two-project setup.
        def _bad(contract_path: Path, mutate) -> bool:
            bad = json.loads(contract_path.read_text(encoding="utf-8"))
            mutate(bad)
            bad_path = contract_path.parent / "bad_contract.json"
            bad_path.write_text(json.dumps(bad), encoding="utf-8")
            try:
                resolve_project(bad_path)
                return False
            except SystemExit:
                return True

        bad_schema = _bad(Path(proj_b["contract_path"]), lambda c: c.__setitem__("schema", "wrong/v9"))
        missing_field = _bad(Path(proj_b["contract_path"]), lambda c: c.__delitem__("source_repo"))
        cases.append(case(
            "malformed-contract-rejected",
            bad_schema and missing_field,
            f"bad_schema={bad_schema} missing_field={missing_field}",
        ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-second-project-b5",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/second_project_canary.py",
            "preflight": "second-project fixture preflight (archived project documentation)",
        },
        "known_gaps_open": [
            "Restart/crash durability is out of scope for B5 by design (binding/isolation proof; durability is covered by B12 live-smoke).",
            "Snapshot schema v1 carries no project_id field; per-project status identity is proven via per-contract snapshot path and store roots. Threading project_id into the snapshot is a possible follow-up.",
            "Resolver-side collision detection (two contracts sharing roots/project_id) is deferred per preflight fork (a) scope-min.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

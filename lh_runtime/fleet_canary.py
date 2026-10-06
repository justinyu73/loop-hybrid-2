#!/usr/bin/env python3
"""Fleet: one wake-up walks a registry of projects, each in its own bounded session.

Every enabled project runs with its own contract, stores, singleton lock and
receipts.  A broken project does not stop the others, a paused project is not
woken, a project already held elsewhere reports ``not_holder`` instead of
running twice, and every project reports its own stop reason.  The trigger is
always an external scheduler; the fleet installs nothing.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import second_project_canary as projects_fixture  # noqa: E402
from goal_loop_driver import run_driver  # noqa: E402
from goal_loop_run import build_worker  # noqa: E402
from p7_native_runstore_fixture import explicit_runstore_factory  # noqa: E402
from project_binding import resolve_project  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-fleet"
REGISTRY_SCHEMA = "lh-fleet-registry/v1"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def fleet():
    import fleet as module
    return module


class Projects:
    """Real fixture projects (own source repo, contract and runtime roots) and an in-process session."""

    def __init__(self, root: Path, names: tuple[str, ...]):
        root.mkdir(parents=True)
        self.root = root
        self.by_id: dict[str, dict[str, Any]] = {}
        for name in names:
            project = projects_fixture._make_project(root, name, f"fleet-{name}")
            self.by_id[f"b5-project-{name}"] = project
        self.calls: list[str] = []
        self.goal_states: dict[str, str] = {}

    def session(self, entry: dict[str, Any]) -> dict[str, Any]:
        """Resolve the contract, seed one candidate and drive one bounded session (as the B5 canary does)."""
        self.calls.append(entry["project_id"])
        project = self.by_id[entry["project_id"]]
        kw = resolve_project(Path(entry["contract"]))["run_kwargs"]
        with explicit_runstore_factory(projects_fixture.fixture_glr):
            worker = build_worker(goal_store_root=kw["goal_store_root"], run_store_root=kw["run_store_root"],
                                  workspace_root=kw["workspace_root"], campaign=kw["campaign"],
                                  source_repo=kw["source_repo"], base_revision=kw["base_revision"])
        campaign_id = project["campaign_id"]
        stage = worker.compilers[campaign_id].compile()["stages"]["stage-only"]
        goal_id = f"{campaign_id}:stage-only"
        worker.goal_store.record_event(
            event_id=f"fleet-seed-{campaign_id}", idempotency_key=f"fleet-seed-{campaign_id}", source="manual_intent",
            event_type="goal_candidate",
            payload={"candidate": {"goal_id": goal_id, "campaign_id": campaign_id, "stage_id": "stage-only",
                                   "goal": stage["goal"] | {"admission_envelope": stage}}})
        summary = run_driver(worker, holder=f"fleet-{campaign_id}", model=projects_fixture._model,
                             max_cycles=int(entry["max_cycles"]), sleep_fn=lambda _s: None)
        try:
            self.goal_states[entry["project_id"]] = worker.goal_store.get_goal(goal_id)["state"]
        except KeyError:  # never admitted, e.g. the session was not the holder
            self.goal_states[entry["project_id"]] = "absent"
        return summary

    def registry(self, states: dict[str, str], *, contracts: dict[str, Path] | None = None) -> Path:
        rows = []
        for project_id, project in self.by_id.items():
            contract = (contracts or {}).get(project_id, project["contract_path"])
            rows.append({"project_id": project_id, "contract": str(contract),
                         "desired_state": states.get(project_id, "enabled"), "max_cycles": 30})
        path = self.root / f"registry-{len(list(self.root.glob('registry-*')))}.json"
        path.write_text(json.dumps({"schema": REGISTRY_SCHEMA, "projects": rows}), encoding="utf-8")
        return path

    def stores(self, project_id: str) -> tuple[RunStore, GoalStore]:
        runtime = Path(self.by_id[project_id]["dir"]) / "runtime"
        return RunStore(runtime / "runs"), GoalStore(runtime / "goals")


def _run_count(runs: RunStore) -> int:
    return sum(runs.summary()["runs_by_state"].values())


def _rows(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["project_id"]: row for row in result.get("projects", [])}


A, B = "b5-project-alpha", "b5-project-beta"


def c1_c5_each_completes(root: Path) -> list[dict[str, Any]]:
    projects = Projects(root, ("alpha", "beta"))
    result = fleet().wake(projects.registry({}), session=projects.session)
    rows = _rows(result)
    both = all(rows.get(pid, {}).get("status") == "ran" and rows[pid].get("stop_reason")
               and projects.goal_states.get(pid) == "completed" for pid in (A, B))
    runs_a, goals_a = projects.stores(A)
    runs_b, goals_b = projects.stores(B)
    campaigns_a = {goal["campaign_id"] for goal in goals_a.goals_in_state("completed")}
    campaigns_b = {goal["campaign_id"] for goal in goals_b.goals_in_state("completed")}
    disjoint = (runs_a.root != runs_b.root and goals_a.root != goals_b.root
                and _run_count(runs_a) >= 1 and _run_count(runs_b) >= 1
                and campaigns_a == {"fleet-alpha"} and campaigns_b == {"fleet-beta"})
    ids_a, ids_b = sorted(campaigns_a), sorted(campaigns_b)
    return [
        case("two-projects-each-complete", result.get("schema") == "lh-fleet-wake/v1" and both,
             {"rows": rows, "goal_states": projects.goal_states}),
        case("stores-are-disjoint", disjoint, {"alpha": ids_a, "beta": ids_b,
                                               "runs": [_run_count(runs_a), _run_count(runs_b)]}),
    ]


def c2_broken(root: Path) -> dict[str, Any]:
    projects = Projects(root, ("alpha", "beta"))
    broken = root / "broken-contract.json"
    broken.write_text("{ not json", encoding="utf-8")
    result = fleet().wake(projects.registry({}, contracts={A: broken}), session=projects.session)
    rows = _rows(result)
    ok = (rows.get(A, {}).get("status") == "failed" and bool(rows[A].get("error"))
          and rows.get(B, {}).get("status") == "ran" and projects.goal_states.get(B) == "completed")
    return case("a-broken-project-does-not-block-the-others", ok, {"rows": rows})


def c3_paused(root: Path) -> dict[str, Any]:
    projects = Projects(root, ("alpha", "beta"))
    result = fleet().wake(projects.registry({A: "paused"}), session=projects.session)
    rows = _rows(result)
    runs_a, _ = projects.stores(A)
    ok = (A not in projects.calls and rows.get(A, {}).get("status") == "paused" and _run_count(runs_a) == 0
          and rows.get(B, {}).get("status") == "ran" and projects.goal_states.get(B) == "completed")
    return case("a-paused-project-is-not-woken", ok, {"calls": projects.calls, "rows": rows})


HOLDER = '''
import sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from platform_ports import FileLockSchedulerPort
root, ready, release = Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])
root.mkdir(parents=True, exist_ok=True)
handle = FileLockSchedulerPort().acquire(root)
ready.write_text("held" if handle is not None else "refused", encoding="utf-8")
deadline = time.time() + 120
while not release.exists() and time.time() < deadline:
    time.sleep(0.05)
'''


def c4_not_holder(root: Path) -> dict[str, Any]:
    projects = Projects(root, ("alpha", "beta"))
    runs_a, _ = projects.stores(A)
    script, ready, release = root / "holder.py", root / "ready", root / "release"
    script.write_text(HOLDER, encoding="utf-8")
    child = subprocess.Popen([sys.executable, "-B", str(script), str(HERE), str(runs_a.root), str(ready), str(release)],
                             stdin=subprocess.DEVNULL)
    try:
        deadline = time.time() + 60
        while not ready.exists() and time.time() < deadline:
            time.sleep(0.05)
        held = ready.exists() and ready.read_text(encoding="utf-8") == "held"
        result = fleet().wake(projects.registry({}), session=projects.session)
    finally:
        release.write_text("go", encoding="utf-8")
        child.wait(timeout=60)
    rows = _rows(result)
    row_a = rows.get(A, {})
    ok = (held and row_a.get("status") == "ran" and row_a.get("stop_reason") == "not_holder"
          and row_a.get("runs_dispatched") == 0 and projects.goal_states.get(A) == "absent" and _run_count(runs_a) == 0 and rows.get(B, {}).get("status") == "ran"
          and projects.goal_states.get(B) == "completed")
    return case("a-project-held-elsewhere-reports-not-holder", ok, {"held": held, "rows": rows})


def c6_registry_closed(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True)
    contract = root / "c.json"
    contract.write_text("{}", encoding="utf-8")
    good = {"project_id": "p-one", "contract": str(contract), "desired_state": "enabled"}
    bad = {
        "wrong_schema": {"schema": "other/v1", "projects": [good]},
        "unknown_state": {"schema": REGISTRY_SCHEMA, "projects": [{**good, "desired_state": "maybe"}]},
        "unknown_field": {"schema": REGISTRY_SCHEMA, "projects": [{**good, "command": "rm -rf /"}]},
        "duplicate_id": {"schema": REGISTRY_SCHEMA, "projects": [good, dict(good)]},
        "unbounded": {"schema": REGISTRY_SCHEMA, "projects": [{**good, "max_cycles": 0}]},
    }
    refused = {}
    for label, body in bad.items():
        path = root / f"{label}.json"
        path.write_text(json.dumps(body), encoding="utf-8")
        try:
            fleet().load_registry(path)
            refused[label] = "accepted"
        except ValueError as exc:
            refused[label] = f"refused: {exc}"
    path = root / "good.json"
    path.write_text(json.dumps({"schema": REGISTRY_SCHEMA, "projects": [good]}), encoding="utf-8")
    loaded = fleet().load_registry(path)
    ok = all(value.startswith("refused") for value in refused.values()) and loaded["projects"][0]["project_id"] == "p-one"
    return case("the-registry-is-closed", ok, refused)


def c7_default_session(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True)
    contract = root / "contract.json"
    contract.write_text("{}", encoding="utf-8")
    seen: list[list[str]] = []

    def runner(argv, **_kwargs):
        seen.append(list(argv))
        payload = {"mode": "execute", "driver": {"stop_reason": "idle", "cycles": 2, "runs_dispatched": 0}}
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    def failing(argv, **_kwargs):
        return subprocess.CompletedProcess(argv, 2, "", "contract invalid")

    entry = {"project_id": "p-one", "contract": str(contract), "desired_state": "enabled", "max_cycles": 7}
    summary = fleet().goal_loop_session(entry, runner=runner)
    argv = seen[0] if seen else []
    bounded = (any(Path(part).name == "goal_loop_run.py" for part in argv) and "--execute" in argv
               and argv[argv.index("--contract") + 1] == str(contract) and argv[argv.index("--max-cycles") + 1] == "7")
    try:
        fleet().goal_loop_session(entry, runner=failing)
        failure = "returned"
    except RuntimeError as exc:
        failure = f"raised: {exc}"
    ok = bounded and summary.get("stop_reason") == "idle" and failure.startswith("raised")
    return case("the-default-session-is-a-bounded-goal-loop-run", ok,
                {"argv": argv, "summary": summary, "failure": failure})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-fleet-") as raw:
        root = Path(raw).resolve()
        results: list[dict[str, Any]] = []
        groups: list[tuple[list[str], Callable[[], Any]]] = [
            (["two-projects-each-complete", "stores-are-disjoint"], lambda: c1_c5_each_completes(root / "c1")),
            (["a-broken-project-does-not-block-the-others"], lambda: [c2_broken(root / "c2")]),
            (["a-paused-project-is-not-woken"], lambda: [c3_paused(root / "c3")]),
            (["a-project-held-elsewhere-reports-not-holder"], lambda: [c4_not_holder(root / "c4")]),
            (["the-registry-is-closed"], lambda: [c6_registry_closed(root / "c6")]),
            (["the-default-session-is-a-bounded-goal-loop-run"], lambda: [c7_default_session(root / "c7")]),
        ]
        for names, action in groups:
            try:
                results.extend(action())
            except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
                detail = f"{type(exc).__name__}: {str(exc)[:300]}"
                results.extend(case(name, False, detail) for name in names)
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

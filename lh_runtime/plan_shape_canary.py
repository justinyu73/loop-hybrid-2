#!/usr/bin/env python3
"""Plan shape: the engine checks a sealed plan's structure before any verifier sees it.

A plan that declares ``lh-sealed-plan/v1`` is checked by a fixed list: no cycle,
unique units, workers and worktrees, no unresolved placeholder, known
dependencies and dispatchable nodes, and parallel groups that neither touch the
same paths nor depend on each other.  A defect is refused with its code and the
injected verifier is never called.  A plan that declares no shape still goes to
the verifier, and the receipt says it was not shape-checked.  State files under
the earlier host-era schema names are refused, not replayed.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from plan_node_controller import PlanNodeController, PlanNodeControllerError  # noqa: E402
import work_unit_store  # noqa: E402

CHECK_ID = "lh-plan-shape"
PLAN_SCHEMA = "lh-sealed-plan/v1"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def shape():
    import plan_shape
    return plan_shape


def digest_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def seal(plan: dict[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in plan.items() if key != "plan_digest"}
    return {**body, "plan_digest": digest_json(body)}


def good_plan() -> dict[str, Any]:
    return seal({
        "schema": PLAN_SCHEMA,
        "base_sha": "a" * 40,
        "work_units": [
            {"node_id": "design", "depends_on": [], "read_set": ["docs/"], "write_set": ["docs/design.md"],
             "worker_id": "w-design", "worktree": "wt/design"},
            {"node_id": "api", "depends_on": ["design"], "read_set": ["docs/design.md"], "write_set": ["src/api/"],
             "worker_id": "w-api", "worktree": "wt/api"},
            {"node_id": "ui", "depends_on": ["design"], "read_set": ["docs/design.md"], "write_set": ["src/ui/"],
             "worker_id": "w-ui", "worktree": "wt/ui"},
            {"node_id": "release", "depends_on": ["api", "ui"], "read_set": ["src/"], "write_set": ["CHANGELOG.md"],
             "worker_id": "w-release", "worktree": "wt/release"},
        ],
        "dispatchable_nodes": ["design"],
        "parallel_groups": [["api", "ui"]],
    })


def unit(plan: dict[str, Any], node_id: str) -> dict[str, Any]:
    return next(item for item in plan["work_units"] if item["node_id"] == node_id)


def defects() -> dict[str, dict[str, Any]]:
    def mutate(change: Callable[[dict[str, Any]], None], *, reseal: bool = True) -> dict[str, Any]:
        plan = copy.deepcopy(good_plan())
        change(plan)
        return seal(plan) if reseal else plan
    return {
        "plan_schema_invalid": mutate(lambda p: p["work_units"][0].update(write_set="not-a-list")),
        "plan_digest_mismatch": mutate(lambda p: unit(p, "ui")["write_set"].append("src/ui2/"), reseal=False),
        "plan_placeholder_unresolved": mutate(lambda p: unit(p, "api").update(worktree="wt/${UNIT}")),
        "work_unit_duplicate": mutate(lambda p: p["work_units"].append({**copy.deepcopy(unit(p, "ui")),
                                                                          "worker_id": "w-ui-2", "worktree": "wt/ui-2"})),
        "worker_not_unique": mutate(lambda p: unit(p, "ui").update(worker_id="w-api")),
        "worktree_not_unique": mutate(lambda p: unit(p, "ui").update(worktree="wt/api")),
        "dependency_unknown": mutate(lambda p: unit(p, "release")["depends_on"].append("ghost")),
        "graph_cycle": mutate(lambda p: unit(p, "design")["depends_on"].append("release")),
        "dispatchable_node_unknown": mutate(lambda p: p["dispatchable_nodes"].append("ghost")),
        "parallel_group_path_overlap": mutate(lambda p: unit(p, "ui")["write_set"].append("src/api/routes.py")),
        "parallel_group_dependent": mutate(lambda p: unit(p, "ui")["depends_on"].append("api")),
    }


class Calls:
    def __init__(self) -> None:
        self.planner = self.verifier = self.queue = 0


def dispatch(root: Path, plan: dict[str, Any], calls: Calls, *, input_char: str = "3") -> dict[str, Any]:
    def planner(_context: dict[str, Any]) -> dict[str, Any]:
        calls.planner += 1
        return {"sealed_plan": copy.deepcopy(plan), "planner_receipt": {"identity": "planner"}}

    def verifier(_plan: dict[str, Any], _receipt: dict[str, Any]) -> dict[str, Any]:
        calls.verifier += 1
        return {"verdict": "GREEN", "identity": "verifier", "read_only": True, "source_write": False}

    controller = PlanNodeController(root, goal_id="shape-goal")

    def project(_plan: dict[str, Any], _verdict: dict[str, Any]) -> dict[str, Any]:
        calls.queue += 1
        return {"status": "projected", "first_actionable": {"node_id": controller.first_actionable_node},
                "runs_created": 0, "attempts_created": 0, "provider_invocations": 0}

    return controller.dispatch(input_digest="sha256:" + input_char * 64, planner=planner, verifier=verifier,
                               queue_projector=project)


def c1_defects(root: Path) -> dict[str, Any]:
    wrong = {}
    for index, (code, plan) in enumerate(defects().items()):
        found = shape().check_plan(plan)
        calls = Calls()
        try:
            dispatch(root / f"d{index}", plan, calls)
            refused = "accepted"
        except PlanNodeControllerError as exc:
            refused = exc.reason
        if (found.get("verdict") != "RED" or code not in found.get("reasons", [])
                or refused != f"plan_shape_red:{code}" or calls.verifier != 0 or calls.queue != 0):
            wrong[code] = {"check": found, "dispatch": refused, "verifier_calls": calls.verifier}
    return case("every-defect-is-refused-with-its-code-before-the-verifier", not wrong, wrong or len(defects()))


def c2_good(root: Path) -> dict[str, Any]:
    calls = Calls()
    result = dispatch(root, good_plan(), calls)
    found = shape().check_plan(good_plan())
    ok = (found.get("verdict") == "GREEN" and found.get("reasons") == [] and result.get("status") == "completed"
          and result.get("shape_checked") is True and calls.verifier == 1 and calls.queue == 1
          and result.get("schema") == "lh-plan-node-state/v1")
    return case("a-sound-plan-is-checked-then-verified-once", ok,
                {"check": found, "shape_checked": result.get("shape_checked"), "schema": result.get("schema"),
                 "verifier_calls": calls.verifier})


def c3_pure() -> dict[str, Any]:
    plan = defects()["graph_cycle"]
    frozen = copy.deepcopy(plan)
    first, second = shape().check_plan(plan), shape().check_plan(copy.deepcopy(plan))
    return case("the-check-is-pure", first == second and plan == frozen, {"first": first})


def c4_same_as_scheduler() -> dict[str, Any]:
    pairs = [
        (["src/a/"], [], ["src/b/"], []),
        (["src/a/"], [], ["src/a/x.py"], []),
        (["src/a/"], [], ["docs/"], ["src/a/b.py"]),
        (["src/a/"], ["docs/"], ["src/b/"], ["docs/"]),
        (["./src/a"], [], ["src/a/"], []),
    ]
    disagreements = []
    for left_w, left_r, right_w, right_r in pairs:
        plan = copy.deepcopy(good_plan())
        unit(plan, "api").update(write_set=left_w, read_set=left_r)
        unit(plan, "ui").update(write_set=right_w, read_set=right_r)
        plan = seal(plan)
        overlap = "parallel_group_path_overlap" in shape().check_plan(plan).get("reasons", [])
        scheduler_conflict = not work_unit_store.compatible_work_units(
            {"read_set": left_r, "write_set": left_w}, {"read_set": right_r, "write_set": right_w})
        if overlap != scheduler_conflict:
            disagreements.append({"left": [left_w, left_r], "right": [right_w, right_r],
                                  "plan_shape": overlap, "scheduler": scheduler_conflict})
    return case("path-overlap-agrees-with-the-scheduler", not disagreements, disagreements or len(pairs))


def c5_undeclared(root: Path) -> dict[str, Any]:
    calls = Calls()
    result = dispatch(root, {"plan_digest": "sha256:" + "9" * 64}, calls)
    ok = result.get("status") == "completed" and result.get("shape_checked") is False and calls.verifier == 1
    return case("an-undeclared-plan-goes-to-the-verifier-marked-unchecked", ok,
                {"shape_checked": result.get("shape_checked"), "verifier_calls": calls.verifier})


def c6_legacy_state(root: Path) -> dict[str, Any]:
    calls = Calls()
    dispatch(root, good_plan(), calls)
    path = root / "plan-node-state.json"
    state = json.loads(path.read_text(encoding="utf-8"))
    state["schema"], state["controller_schema"] = "host-plan-node-state/v1", "host-plan-node-controller/v1"
    # A file written under the old name carries a digest of its own bytes, so reseal it the same way.
    state["state_digest"] = digest_json({key: value for key, value in state.items() if key != "state_digest"})
    path.write_text(json.dumps(state), encoding="utf-8")
    # Owner decision (2026-10-06): the old names are no longer accepted.  The file is refused
    # outright -- never silently re-planned and never accepted under another name.
    try:
        dispatch(root, good_plan(), calls)
        outcome = "accepted"
    except PlanNodeControllerError as exc:
        outcome = exc.reason
    ok = outcome == "state_schema_invalid" and calls.planner == 1 and calls.verifier == 1
    return case("a-state-file-under-the-old-schema-name-is-refused", ok,
                {"outcome": outcome, "planner_calls": calls.planner})


def c7_residue(root: Path) -> dict[str, Any]:
    source = (HERE / "plan_node_controller.py").read_text(encoding="utf-8")
    controller = PlanNodeController(root, goal_id="residue-goal")
    found = {token: token in source for token in ("P3B", '"R0"', "PR #", "host-plan-node")}
    ok = (not any(found.values()) and controller.node_id != "P3B" and controller.first_actionable_node != "R0")
    return case("no-host-era-names-remain-in-the-plan-node-controller", ok,
                {"found": found, "node_id": controller.node_id, "first_actionable": controller.first_actionable_node})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-plan-shape-") as raw:
        root = Path(raw).resolve()
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("every-defect-is-refused-with-its-code-before-the-verifier", lambda: c1_defects(root / "c1")),
            ("a-sound-plan-is-checked-then-verified-once", lambda: c2_good(root / "c2")),
            ("the-check-is-pure", c3_pure),
            ("path-overlap-agrees-with-the-scheduler", c4_same_as_scheduler),
            ("an-undeclared-plan-goes-to-the-verifier-marked-unchecked", lambda: c5_undeclared(root / "c5")),
            ("a-state-file-under-the-old-schema-name-is-refused", lambda: c6_legacy_state(root / "c6")),
            ("no-host-era-names-remain-in-the-plan-node-controller", lambda: c7_residue(root / "c7")),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Normal successor: reviewed work continues by rule, exceptions go to the Planner port.

A task area of reviewed tasks (candidate review v2) whose predecessor's
integrated receipt chain re-verifies from the original Store releases its
approved successor without calling the recovery port.  RED results, unsettled
recovery requests, forged receipts, and legacy tasks keep the existing
Planner path.  Only approved tasks are admitted; review suggestions never
become work on their own.

The executor is an absolute-path script launched through the explicit fixture
fence; the reviewer is a callable; the recovery port is a counting stand-in for
the Planner.  No model, network, or credential is involved.
"""
from __future__ import annotations

import copy
import json
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parent / "tests"))
sys.path.insert(0, str(HERE))

import candidate_review_work_unit_canary as fixture  # noqa: E402
from lh_runtime import delivery_contract as engine  # noqa: E402
from lh_runtime import work_unit_completion as completion  # noqa: E402
from lh_runtime.parallel_scheduler import SuccessorDispatchConsumer  # noqa: E402
from lh_runtime.task_area import TaskAreaController, TaskAreaError, manifest_body_digest, manifest_digest  # noqa: E402
from lh_runtime.work_unit_store import WorkUnitStore, digest_json  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402

CHECK_ID = "lh-normal-successor"
GOAL_ID = "normal-successor"
GOAL_REVISION = 1


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing API is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


EXECUTOR_SCRIPT = """import json, pathlib, sys
worktree, relative, mode, attempt = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
texts = {"right": %r, "wrong-negative": %r}
target = worktree / relative
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(texts[mode], encoding="utf-8")
print(json.dumps({"attempt": attempt, "wrote": mode}))
""" % (fixture.RIGHT, fixture.WRONG_NEGATIVE)


def _target(node: str) -> str:
    return f"src/{node.lower()}/m.py"


def _checks(node: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    load = f"from pathlib import Path; d = {{}}; exec(Path({_target(node)!r}).read_text(encoding='utf-8'), d); "
    related = [{"id": f"related-{node}", "argv": [sys.executable, "-B", "-c",
                load + "assert d['double'](0) == 0 and d['double'](3) == 6"],
                "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}]
    full = [{"id": f"full-{node}", "argv": [sys.executable, "-B", "-c", load + "assert callable(d['double'])"],
             "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}]
    return related, full


class Executor(fixture.CommandExecutorAdapter):
    def __init__(self, root: Path, node: str, mode: str, script: Path, launches: list):
        super().__init__(root / "executors" / node, timeout_seconds=60, spawn=subprocess.Popen,
                         execution_fence_port=fixture.ExplicitFixtureFence(spawn=subprocess.Popen))
        self.node, self.mode, self.script, self.launches = node, mode, script, launches

    def _command(self, request, packet):
        self.launches.append((self.node, request["attempt"]))
        return [sys.executable, "-B", str(self.script), str(request["worktree"]), _target(self.node),
                self.mode, str(request["attempt"])]


class Reviewer:
    def __init__(self):
        self.calls = 0

    def __call__(self, request: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        scope: dict[str, Any] = {}
        exec((Path(request["worktree"]) / _target(request["node_id"])).read_text(encoding="utf-8"), scope)  # noqa: S102
        correct = scope["double"](-2) == -4 and scope["double"](3) == 6
        evidence = {"verdict": "GREEN" if correct else "RED", "principal": fixture.VERIFIER, "read_only": True,
                    "source_write": False, "candidate_digest": request["candidate_digest"],
                    "checks_digest": request["checks_digest"], "evidence_ref": "normal-successor-reviewer"}
        context = request.get("candidate_review_context")
        if not isinstance(context, dict):
            return evidence
        document = fixture.review_document(context, blocking=not correct, suggestion=correct)
        return {**evidence, "review": document}


class PlannerPort:
    """A counting stand-in for the existing recovery (Planner) port."""

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, task: dict[str, Any], **_kwargs) -> dict[str, Any]:
        self.calls.append(task["node_id"])
        return {"status": "idle", "progress": False}


class Area:
    def __init__(self, root: Path, *, nodes: dict[str, dict[str, Any]], review: bool = True,
                 mismatch: bool = False):
        self.root = root
        root.mkdir(parents=True)
        self.base_repo = root / "base"
        fixture.write(self.base_repo / "README.md", "normal successor fixture\n")
        fixture._git("init", "-q", cwd=self.base_repo)
        fixture._git("config", "user.email", "area@example.invalid", cwd=self.base_repo)
        fixture._git("config", "user.name", "Area Fixture", cwd=self.base_repo)
        fixture._git("add", "README.md", cwd=self.base_repo)
        fixture._git("commit", "-qm", "base", cwd=self.base_repo)
        self.base = fixture._git("rev-parse", "HEAD", cwd=self.base_repo)
        self.policy = fixture.review_policy(root / "policy") if review else None
        self.store = WorkUnitStore(root / "store")
        self.reviewer, self.planner, self.launches = Reviewer(), PlannerPort(), []
        self.script = fixture.write(root / "bin" / "executor.py", EXECUTOR_SCRIPT).resolve()
        self.nodes = nodes
        self.tasks: dict[str, dict[str, Any]] = {}
        self.completions: dict[str, dict[str, Any]] = {}
        for node, spec in nodes.items():
            self.tasks[node] = self._task(node, spec, mismatch=mismatch)

    def _task(self, node: str, spec: dict[str, Any], *, mismatch: bool) -> dict[str, Any]:
        related, full = _checks(node)
        body = fixture.delivery_contract(self.policy, max_attempts=spec.get("max_attempts", 2))
        body = {key: value for key, value in body.items() if key != "contract_digest"}
        body.update(goal={"id": GOAL_ID, "revision": GOAL_REVISION}, node={"id": node, "kind": "coding"},
                    unit_id=node, contract_id=f"normal-successor-{node}")
        body["scope"] = {**body["scope"], "allowed_paths": [f"src/{node.lower()}/"]}
        body["obligations"] = [{"id": f"full-{node}-obligation", "commands": full, "required_receipts": ["checks"]}]
        body["source_obligation_ids"] = [f"full-{node}-obligation"]
        delivery = engine.seal_contract(body)
        completion_policy = copy.deepcopy(self.policy)
        if mismatch and completion_policy is not None:
            completion_policy["requirements"] = [{"id": "double", "text": "a different requirement"}]
        contract = {"checks": related + full, "integration_checks": full, "full_validation_plan": {"commands": full},
                    "max_attempts": spec.get("max_attempts", 2),
                    "integration_worktree": str(self.root / ("integration-" + node)),
                    "verifier_principal": fixture.VERIFIER}
        if completion_policy is not None:
            contract["candidate_review"] = completion_policy
        self.completions[node] = contract
        worktree = self.root / ("worker-" + node)
        subprocess.run(["git", "clone", "-q", str(self.base_repo), str(worktree)], check=True)
        plan = engine.plan_delivery_unit(delivery)
        packet = engine.bind_packet({"packet_id": node, "goal_id": GOAL_ID, "goal_revision": GOAL_REVISION,
                                     "node_id": node, "write_set": [f"src/{node.lower()}/"],
                                     "forbidden_paths": ["secrets"], "task": f"implement double for {node}",
                                     "completion_contract": contract,
                                     "completion_contract_digest": digest_json(contract),
                                     "targeted_commands": related, "full_validation_ref": "#/full_validation_plan"},
                                    plan, delivery)
        packet["packet_digest"] = digest_json(packet)
        packet_path = fixture.write(self.root / (node + ".json"), json.dumps(packet, sort_keys=True))
        envelope = {"schema": "lh-successor-dispatch-envelope/v1", "dispatch_key": node, "goal_id": GOAL_ID,
                    "goal_revision": GOAL_REVISION, "node_id": node, "successor_node_id": node,
                    "first_actionable": node, "packet_path": str(packet_path), "packet_digest": packet["packet_digest"],
                    "wave_base_sha": self.base, "worktree": str(worktree), "branch": "area/" + node,
                    "transition_digest": "sha256:" + "1" * 64, "provider_invocations": 0, "manual_prompts": 0}
        envelope["envelope_digest"] = digest_json(envelope)
        return {"node_id": node, "status": spec.get("status", "approved"), "depends_on": spec.get("depends_on", []),
                "read_set": [], "write_set": [f"src/{node.lower()}/"], "envelope": envelope,
                "delivery_contract": delivery, "completion_contract": contract}

    def manifest(self, **status: str) -> dict[str, Any]:
        tasks = []
        for node, task in self.tasks.items():
            tasks.append({**copy.deepcopy(task), "status": status.get(node, task["status"])})
        value = {"schema": "lh-task-area/v1", "goal_id": GOAL_ID, "goal_revision": GOAL_REVISION,
                 "base_sha": self.base, "planner_principal": "planner", "tasks": tasks}
        value["plan_verifier"] = {"principal": "independent-plan-verifier", "read_only": True, "source_write": False,
                                  "verdict": "GREEN", "manifest_digest": manifest_body_digest(value)}
        value["approval"] = {"status": "approved", "manifest_digest": manifest_digest(value)}
        return value

    def controller(self, manifest: dict[str, Any]) -> TaskAreaController:
        def factory(task: dict[str, Any]) -> SuccessorDispatchConsumer:
            node = task["node_id"]
            controller = completion.WorkUnitCompletionController(
                self.store, contract=self.completions[node], command_runner=fixture_command_runner,
                verifier=self.reviewer, integrator=fixture._integrator, delivery_contract=task["delivery_contract"])
            executor = Executor(self.root, node, self.nodes[node].get("mode", "right"), self.script, self.launches)
            return SuccessorDispatchConsumer(self.store, goal_id=GOAL_ID, goal_revision=GOAL_REVISION, node_id=node,
                                             executor=executor, completion_controller=controller,
                                             delivery_contract=task["delivery_contract"])
        return TaskAreaController(self.store, consumer_factory=factory,
                                  approved_manifest_digest=manifest_digest(manifest), recovery_port=self.planner)

    def tick(self, manifest: dict[str, Any], cycles: int = 6) -> dict[str, Any]:
        return self.controller(manifest).tick(manifest, max_cycles=cycles)

    def states(self) -> dict[str, str]:
        return {unit["node_id"]: unit["state"] for unit in self.store.list_work_units(GOAL_ID)}


def _launched(area: Area) -> list[str]:
    return [node for node, _attempt in area.launches]


def _blocked(result: dict[str, Any]) -> str:
    return json.dumps([*result.get("blocked", []), *result.get("recovery", [])], default=str)


def c1_c7_normal(root: Path) -> list[dict[str, Any]]:
    area = Area(root, nodes={"A": {}, "B": {"depends_on": ["A"]}})
    manifest = area.manifest()
    area.tick(manifest)
    first = case("reviewed-successor-runs-without-a-planner-call",
                 area.states() == {"A": "integrated", "B": "integrated"} and area.planner.calls == []
                 and _launched(area) == ["A", "B"],
                 {"states": area.states(), "planner_calls": area.planner.calls, "launches": area.launches})
    before = len(area.launches)
    replay = area.tick(manifest)
    seventh = case("replay-is-idempotent",
                   len(area.launches) == before and area.planner.calls == [] and replay.get("known_executor_invocations") == 0,
                   {"launches_after_replay": len(area.launches) - before, "planner_calls": area.planner.calls,
                    "executor_invocations": replay.get("known_executor_invocations")})
    return [first, seventh]


def c2_legacy(root: Path) -> dict[str, Any]:
    area = Area(root, nodes={"A": {}, "B": {"depends_on": ["A"]}}, review=False)
    area.tick(area.manifest())
    ok = area.states().get("A") == "integrated" and "B" not in area.states() and "A" in area.planner.calls
    return case("legacy-successor-keeps-the-planner-handoff", ok,
                {"states": area.states(), "planner_calls": area.planner.calls, "launches": area.launches})


def c3_red(root: Path) -> dict[str, Any]:
    area = Area(root, nodes={"A": {"mode": "wrong-negative", "max_attempts": 1}, "B": {"depends_on": ["A"]}, "C": {}})
    area.tick(area.manifest())
    states = area.states()
    ok = ("A" in area.planner.calls and "B" not in states and states.get("C") == "integrated"
          and states.get("A") != "integrated")
    return case("red-review-goes-to-the-existing-planner-port", ok,
                {"states": states, "planner_calls": area.planner.calls, "launches": area.launches})


def _integrated_then(root: Path, mutate: Callable[[Area], None]) -> tuple[Area, dict[str, Any]]:
    area = Area(root, nodes={"A": {}, "B": {"depends_on": ["A"], "status": "pending"}})
    area.tick(area.manifest())
    mutate(area)
    result = area.tick(area.manifest(B="approved"))
    return area, result


def c4_forged(root: Path) -> dict[str, Any]:
    def forge(area: Area) -> None:
        with sqlite3.connect(area.store.root / "work-units.sqlite3") as conn:
            rows = conn.execute("SELECT phase_key, evidence_json FROM completion_phases").fetchall()
            for key, raw in rows:
                evidence = json.loads(raw) if raw else None
                if isinstance(evidence, dict) and evidence.get("phase") == "integration_verifier":
                    evidence["evidence_ref"] = "forged-after-settlement"
                    conn.execute("UPDATE completion_phases SET evidence_json = ? WHERE phase_key = ?",
                                 (json.dumps(evidence), key))
    area, result = _integrated_then(root, forge)
    text = _blocked(result)
    ok = "B" not in area.states() and "task_area_normal_completion_" in text
    return case("forged-receipt-chain-never-releases-the-successor", ok,
                {"states": area.states(), "planner_calls": area.planner.calls, "blocked": text[:400]})


def c5_unsettled(root: Path) -> dict[str, Any]:
    def unsettled(area: Area) -> None:
        run = area.store.list_runs(GOAL_ID)[0]
        original = area.store.recovery_requests

        def reads_back_one_open_request(*, work_unit_id=None):
            records = list(original(work_unit_id=work_unit_id))
            if work_unit_id in (None, run["work_unit_id"]):
                records.append({"status": "requested", "request": {"run_id": run["run_id"]}})
            return records
        area.store.recovery_requests = reads_back_one_open_request
    area, result = _integrated_then(root, unsettled)
    ok = "B" not in area.states() and "A" in area.planner.calls
    return case("unsettled-recovery-request-blocks-the-rule", ok,
                {"states": area.states(), "planner_calls": area.planner.calls, "blocked": _blocked(result)[:300],
                 "note": "the Store readback reports one open recovery request for A's run"})


def c6_mismatch(root: Path) -> dict[str, Any]:
    area = Area(root, nodes={"A": {}}, mismatch=True)
    manifest = area.manifest()
    try:
        area.controller(manifest).tick(manifest)
    except TaskAreaError as exc:
        return case("mismatched-review-policy-is-refused",
                    str(exc) == "task_area_candidate_review_contract_mismatch" and area.launches == [], str(exc))
    return case("mismatched-review-policy-is-refused", False, {"launches": area.launches, "states": area.states()})


def c8_admission(root: Path) -> dict[str, Any]:
    area = Area(root, nodes={"A": {}, "B": {"depends_on": ["A"], "status": "pending"}})
    area.tick(area.manifest())
    with area.store._connect() as conn:
        suggestions = conn.execute("SELECT COUNT(*) FROM events WHERE event_type = ?",
                                   ("task_area_discovery_candidate",)).fetchone()[0]
    units = sorted(area.states())
    ok = units == ["A"] and area.states()["A"] == "integrated" and suggestions >= 1 and area.planner.calls == []
    return case("only-approved-successors-are-admitted", ok,
                {"work_units": units, "discovery_suggestions": suggestions, "planner_calls": area.planner.calls})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-normal-successor-") as raw:
        root = Path(raw).resolve()
        try:
            normal = c1_c7_normal(root / "c1")
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:400]}"
            normal = [case("reviewed-successor-runs-without-a-planner-call", False, detail),
                      case("replay-is-idempotent", False, detail)]
        results = [
            normal[0],
            guarded("legacy-successor-keeps-the-planner-handoff", lambda: c2_legacy(root / "c2")),
            guarded("red-review-goes-to-the-existing-planner-port", lambda: c3_red(root / "c3")),
            guarded("forged-receipt-chain-never-releases-the-successor", lambda: c4_forged(root / "c4")),
            guarded("unsettled-recovery-request-blocks-the-rule", lambda: c5_unsettled(root / "c5")),
            guarded("mismatched-review-policy-is-refused", lambda: c6_mismatch(root / "c6")),
            normal[1],
            guarded("only-approved-successors-are-admitted", lambda: c8_admission(root / "c8")),
        ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(results),
        "results": results,
        "blocking_failures": [row["id"] for row in failures],
        "known_gaps_open": [
            "the Planner is a counting stand-in; a host that wakes the task area is outside the engine",
            "the unsettled-request case reads back an injected open request instead of creating one",
        ],
    }, indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

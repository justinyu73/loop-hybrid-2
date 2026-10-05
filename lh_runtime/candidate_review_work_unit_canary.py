#!/usr/bin/env python3
"""Candidate review v2 on the work-unit completion path.

The first end-to-end exam of ``WorkUnitCompletionController`` in this
repository.  A real ``WorkUnitStore`` and ``SuccessorDispatchConsumer`` admit a
dispatch; a declared command executor writes the candidate; the completion
controller runs the related checks, asks a reviewer for a bound review, and
either integrates the candidate or schedules a bounded new attempt whose
dispatch carries the red review back to the executor.

The executor is an absolute-path Python script launched through the explicit
fixture fence; the reviewer is a callable that reads the candidate it is
shown.  No model, network, or credential is involved.
"""
from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parent / "tests"))

from lh_runtime import cli_agent_executor as executors  # noqa: E402
from lh_runtime import delivery_contract as engine  # noqa: E402
from lh_runtime import runner_adapter  # noqa: E402
from lh_runtime import work_unit_completion as completion  # noqa: E402
from lh_runtime.parallel_scheduler import SuccessorDispatchConsumer  # noqa: E402
from lh_runtime.successor_executor import CommandExecutorAdapter  # noqa: E402
from lh_runtime.work_unit_store import WorkUnitStore, digest_json  # noqa: E402
from p7_fence_fixture import ExplicitFixtureFence, fixture_command_runner  # noqa: E402

CHECK_ID = "lh-candidate-review-work-unit"
GOAL_ID = "candidate-review-work-unit"
GOAL_REVISION = 1
NODE_ID = "double"
VERIFIER = "candidate-review-verifier"
SPEC_TEXT = "double(n) returns 2*n for every integer, including negative integers.\n"
RIGHT = "def double(n):\n    return n * 2\n"
WRONG_NEGATIVE = "def double(n):\n    return abs(n) * 2\n"
WRONG_POSITIVE = "def double(n):\n    return n + 1\n"
# Related checks only cover non-negative inputs, so a negative-input defect
# passes them and is left for the reviewer to catch.
RELATED = ("from pathlib import Path; d = {}; exec(Path('src/m1.py').read_text(encoding='utf-8'), d); "
           "assert d['double'](0) == 0 and d['double'](3) == 6")
FULL = ("from pathlib import Path; d = {}; exec(Path('src/m1.py').read_text(encoding='utf-8'), d); "
        "assert callable(d['double'])")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing API is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def reason_of(action: Callable[[], Any]) -> str | None:
    try:
        action()
    except engine.DeliveryUnitError as exc:
        return exc.reason
    except (ValueError, KeyError, TypeError) as exc:
        return str(exc)
    return None


def sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    return path


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def review_policy(root: Path) -> dict[str, Any]:
    spec = write(root / "approved" / "requirements.txt", SPEC_TEXT)
    return {"schema": "lh-candidate-review-contract/v2",
            "spec_ref": {"path": str(spec.resolve()), "content_digest": sha256(spec.read_bytes())},
            "requirements": [{"id": "double", "text": "double(n) = 2*n for positive, zero, and negative integers"}],
            "caller_context_refs": []}


def review_document(context: dict[str, Any], *, blocking: bool, suggestion: bool = False) -> dict[str, Any]:
    finding = {"id": "negative-double" if blocking else "double-style", "blocking": blocking,
               "requirement_id": "double",
               "condition": "negative integer input" if blocking else "optional readability",
               "location": "src/m1.py:double", "impact": "wrong result" if blocking else "style only",
               "evidence": ["double(-2) did not return -4" if blocking else "behavior is unchanged"]}
    return {"schema": "lh-candidate-review-result/v2", "status": "completed",
            "context_digest": context["context_digest"], "candidate_digest": context["candidate_digest"],
            "base_sha": context["base_sha"], "checks_digest": context["checks_digest"],
            "requirements": [{"id": "double", "verdict": "RED" if blocking else "GREEN",
                              "evidence": ["inspected positive, zero, and negative branches"]}],
            "scope": {"verdict": "GREEN", "evidence": ["only src/m1.py changed inside src/"]},
            "findings": [finding] if blocking or suggestion else []}


# -- fixture: contracts, packet, executor, reviewer -----------------------------

def delivery_contract(policy: dict[str, Any] | None, *, max_attempts: int) -> dict[str, Any]:
    command = {"id": "full-behavior", "argv": [sys.executable, "-B", "-c", FULL], "cwd": "${WORKTREE}",
               "expect_exit": 0, "timeout_seconds": 30}
    body = {
        "schema": engine.SCHEMA, "contract_version": 1, "contract_id": "candidate-review-work-unit-contract",
        "unit_id": "candidate-review-work-unit", "goal": {"id": GOAL_ID, "revision": GOAL_REVISION},
        "node": {"id": NODE_ID, "kind": "coding"},
        "planner": {"principal": "candidate-review-planner", "source": "candidate-review-fixture"},
        "independent_verifier": {"principal": VERIFIER, "read_only": True, "source_write": False},
        "outcome": {"observable": "the reviewed candidate is integrated", "start_state": "ready",
                    "success_state": "integrated", "terminal_states": ["integrated", "human_required", "exhausted"]},
        "scope": {"ownership": "task-owned", "allowed_paths": ["src/"], "forbidden_paths": ["secrets"],
                  "identity": ["goal_id", "goal_revision", "node_id", "unit_id"]},
        "obligations": [{"id": "full-behavior-obligation", "commands": [command], "required_receipts": ["checks"]}],
        "required_receipts": ["plan_verdict", "packet_admission", "dispatch", "executor", "candidate", "checks",
                              "verifier", "integration", "integration_checks", "integration_verifier",
                              "machine_complete", "completion", "delivery_verifier"],
        "source_required_receipts": ["plan_verdict", "candidate", "checks", "verifier"],
        "source_obligation_ids": ["full-behavior-obligation"],
        "source_vs_live": {"source_must_not_claim_live": True, "live_required_for_source_delivery": False},
        "repair_same_unit": {"enabled": True, "route": "same_unit_new_attempt", "max_attempts": max_attempts},
        "authority_store": "work_unit", "managed_scope": "candidate-review-work-unit",
    }
    if policy is not None:
        body["candidate_review"] = copy.deepcopy(policy)
    return engine.seal_contract(body)


def _commands() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    related = [{"id": "related-behavior", "argv": [sys.executable, "-B", "-c", RELATED], "cwd": "${WORKTREE}",
                "expect_exit": 0, "timeout_seconds": 30}]
    full = [{"id": "full-behavior", "argv": [sys.executable, "-B", "-c", FULL], "cwd": "${WORKTREE}",
             "expect_exit": 0, "timeout_seconds": 30}]
    return related, full


def completion_contract(root: Path, policy: dict[str, Any] | None, *, max_attempts: int) -> dict[str, Any]:
    related, full = _commands()
    contract = {"checks": related + full, "integration_checks": full, "full_validation_plan": {"commands": full},
                "max_attempts": max_attempts, "integration_worktree": str(root / "integration"),
                "verifier_principal": VERIFIER}
    if policy is not None:
        contract["candidate_review"] = copy.deepcopy(policy)
    return contract


EXECUTOR_SCRIPT = """import json, pathlib, sys
worktree, mode, attempt = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
texts = {"right": %r, "wrong-negative": %r, "wrong-positive": %r}
if mode == "fix-on-retry":
    mode = "wrong-negative" if attempt == 1 else "right"
target = worktree / "src" / "m1.py"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(texts[mode], encoding="utf-8")
print(json.dumps({"attempt": attempt, "wrote": mode}))
""" % (RIGHT, WRONG_NEGATIVE, WRONG_POSITIVE)


class DeclaredScriptExecutor(CommandExecutorAdapter):
    """A command executor bound to one absolute-path script; it logs its repair input."""

    def __init__(self, root: Path, *, script: Path, mode: str, log: Path):
        super().__init__(root / "executor", timeout_seconds=60, spawn=subprocess.Popen,
                         execution_fence_port=ExplicitFixtureFence(spawn=subprocess.Popen))
        self.script, self.mode, self.log = script, mode, log

    def _command(self, request, packet):
        with self.log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"attempt": request["attempt"],
                                     "completion_repair": request.get("completion_repair", [])},
                                    sort_keys=True, default=str) + "\n")
        return [sys.executable, "-B", str(self.script), str(request["worktree"]), self.mode, str(request["attempt"])]


class Reviewer:
    """Read the candidate it is shown and return bound evidence in one of several shapes."""

    def __init__(self, mode: str):
        self.mode, self.calls = mode, 0

    def __call__(self, request: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        text = (Path(request["worktree"]) / "src" / "m1.py").read_text(encoding="utf-8")
        scope: dict[str, Any] = {}
        exec(text, scope)  # noqa: S102 -- the fixture's own candidate
        correct = scope["double"](-2) == -4 and scope["double"](3) == 6
        evidence = {"verdict": "GREEN" if correct else "RED", "principal": VERIFIER, "read_only": True,
                    "source_write": False, "candidate_digest": request["candidate_digest"],
                    "checks_digest": request["checks_digest"], "evidence_ref": "candidate-review-reviewer"}
        context = request.get("candidate_review_context")
        if self.mode == "no-review" or not isinstance(context, dict):
            return {**evidence, "verdict": "GREEN"}
        if self.mode == "foreign":
            foreign = {**context, "candidate_digest": "sha256:" + "f" * 64}
            return {**evidence, "verdict": "GREEN", "review": review_document(foreign, blocking=False)}
        document = review_document(context, blocking=not correct, suggestion=self.mode == "suggestion" and correct)
        return {**evidence, "review": document}


def _integrator(request: dict[str, Any]) -> dict[str, Any]:
    target = Path(request["integration_worktree"])
    if target.exists():
        # A later attempt replaces the integration copy; git objects are read-only.
        def writable(function, path, _info):
            Path(path).chmod(0o700)
            function(path)
        shutil.rmtree(target, onerror=writable)
    shutil.copytree(request["worktree"], target)
    return {"worktree": str(target), "source_candidate_digest": request["candidate_digest"],
            "integration_candidate_digest": completion.candidate_digest(str(target))}


class Harness:
    def __init__(self, root: Path, *, executor_mode: str, reviewer_mode: str = "honest",
                 policy: bool = True, max_attempts: int = 3, policy_override: dict[str, Any] | None = None):
        self.root = root
        root.mkdir(parents=True)
        self.policy = review_policy(root) if policy else None
        self.delivery = delivery_contract(self.policy, max_attempts=max_attempts)
        completion_policy = policy_override if policy_override is not None else self.policy
        self.completion = completion_contract(root, completion_policy, max_attempts=max_attempts)
        self.worktree = root / "worktree"
        write(self.worktree / "README.md", "candidate review fixture\n")
        _git("init", "-q", cwd=self.worktree)
        _git("config", "user.email", "review@example.invalid", cwd=self.worktree)
        _git("config", "user.name", "Review Fixture", cwd=self.worktree)
        _git("add", "README.md", cwd=self.worktree)
        _git("commit", "-qm", "base", cwd=self.worktree)
        self.base = _git("rev-parse", "HEAD", cwd=self.worktree)
        self.store = WorkUnitStore(root / "queue")
        self.reviewer = Reviewer(reviewer_mode)
        self.log = root / "executor-requests.jsonl"
        script = write(root / "bin" / "declared_executor.py", EXECUTOR_SCRIPT)
        self.executor = DeclaredScriptExecutor(root, script=script.resolve(), mode=executor_mode, log=self.log)
        self.controller = completion.WorkUnitCompletionController(
            self.store, contract=self.completion, command_runner=fixture_command_runner,
            verifier=self.reviewer, integrator=_integrator, delivery_contract=self.delivery)
        self.consumer = SuccessorDispatchConsumer(
            self.store, goal_id=GOAL_ID, goal_revision=GOAL_REVISION, node_id=NODE_ID,
            executor=self.executor, completion_controller=self.controller, delivery_contract=self.delivery)
        self.envelope = self._envelope()

    def _envelope(self) -> dict[str, Any]:
        related, full = _commands()
        plan = engine.plan_delivery_unit(self.delivery)
        packet_body = {"schema": "candidate-review-packet/v1", "packet_id": "candidate-review-packet",
                       "goal_id": GOAL_ID, "goal_revision": GOAL_REVISION, "node_id": NODE_ID,
                       "task": "make double(n) correct for every integer", "write_set": ["src/"],
                       "forbidden_paths": ["secrets"], "completion_contract": self.completion,
                       "completion_contract_digest": digest_json(self.completion),
                       "targeted_commands": related, "full_validation_ref": "#/full_validation_plan"}
        packet = engine.bind_packet(packet_body, plan, self.delivery)
        packet["packet_digest"] = digest_json(packet)
        packet_path = write(self.root / "packet.json", json.dumps(packet, sort_keys=True) + "\n")
        body = {"schema": "lh-successor-dispatch-envelope/v1", "status": "ready",
                "dispatch_key": "candidate-review-dispatch", "transition_digest": "sha256:" + "1" * 64,
                "goal_id": GOAL_ID, "goal_revision": GOAL_REVISION, "predecessor_node_id": "plan",
                "node_id": NODE_ID, "successor_node_id": NODE_ID, "first_actionable": NODE_ID,
                "lease_generation": 1, "packet_id": packet["packet_id"], "packet_path": str(packet_path),
                "packet_digest": packet["packet_digest"], "wave_base_sha": self.base,
                "worktree": str(self.worktree), "branch": "agent/candidate-review",
                "provider_invocations": 0, "manual_prompts": 0,
                "delivery_unit_id": self.delivery["unit_id"],
                "delivery_unit_contract_digest": self.delivery["contract_digest"]}
        return {**body, "envelope_digest": digest_json(body)}

    def consume(self) -> dict[str, Any]:
        return self.consumer.consume(self.envelope)

    def drive(self, limit: int = 6) -> list[dict[str, Any]]:
        """Consume until the unit integrates, stops, or the step limit is reached."""
        steps = []
        for _ in range(limit):
            step = self.consume()
            steps.append(step)
            if (step.get("completion") or {}).get("status") != "retry_scheduled" and not step.get("retry_scheduled"):
                break
        return steps

    def requests(self) -> list[dict[str, Any]]:
        if not self.log.is_file():
            return []
        return [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines() if line.strip()]

    def attempts(self) -> list[str]:
        runs = self.store.list_runs(GOAL_ID)
        return [row["state"] for run in runs for row in self.store.attempts_for_run(run["run_id"])]


@contextlib.contextmanager
def capture_final_delivery():
    """Record the final evidence the controller grades (read-only observation)."""
    captured: list[dict[str, Any]] = []
    original = completion.delivery_unit_contract.verify_delivery

    def spy(contract, evidence, **kwargs):
        captured.append({"contract": copy.deepcopy(contract), "evidence": copy.deepcopy(evidence),
                         "kwargs": dict(kwargs)})
        return original(contract, evidence, **kwargs)

    completion.delivery_unit_contract.verify_delivery = spy
    try:
        yield captured
    finally:
        completion.delivery_unit_contract.verify_delivery = original


def _status(steps: list[dict[str, Any]]) -> list[Any]:
    return [((step.get("completion") or {}).get("status"), (step.get("completion") or {}).get("reason"),
             step.get("retry_scheduled")) for step in steps]


# -- cases ------------------------------------------------------------------------

def c1_policy_conflict(root: Path) -> dict[str, Any]:
    other = review_policy(root.with_name(root.name + "-other"))
    other["requirements"] = [{"id": "double", "text": "a different requirement text"}]
    try:
        Harness(root, executor_mode="right", policy_override=other)
    except ValueError as exc:
        return case("review-policy-must-match-the-delivery-contract",
                    str(exc) == "completion_candidate_review_policy_conflict", str(exc))
    return case("review-policy-must-match-the-delivery-contract", False, "controller accepted a conflicting policy")


def c2_related_checks_first(root: Path) -> dict[str, Any]:
    harness = Harness(root, executor_mode="wrong-positive", max_attempts=1)
    steps = harness.drive()
    run_id = steps[-1].get("run_id") if steps else None
    phase = harness.store.get_completion_phase(run_id, 1, 1, "verifier") if run_id else None
    reason = ((phase or {}).get("evidence") or {}).get("reason")
    ok = harness.reviewer.calls == 0 and reason == "candidate_review_related_checks_red"
    return case("related-checks-run-before-the-reviewer", ok,
                {"reviewer_calls": harness.reviewer.calls, "verifier_phase_reason": reason,
                 "status": _status(steps), "attempts": harness.attempts()})


def c3_c7_rework_and_chain(root: Path) -> list[dict[str, Any]]:
    harness = Harness(root, executor_mode="fix-on-retry")
    with capture_final_delivery() as captured:
        steps = harness.drive()
    requests = harness.requests()
    second = requests[1] if len(requests) > 1 else {}
    repair = (second.get("completion_repair") or [{}])[0] if second else {}
    review = repair.get("review") or {}
    feedback = (bool(review) and any(f.get("blocking") for f in review.get("findings", []))
                and isinstance(repair.get("review_ref"), dict))
    final = next((row for row in reversed(captured) if row["kwargs"].get("phase") == "final"), None)
    verdict = engine.verify_delivery(final["contract"], final["evidence"], **final["kwargs"]) if final else {}
    integrated = (steps[-1].get("completion") or {}).get("status") == "integrated" if steps else False
    third = case("red-review-returns-its-findings-to-the-next-attempt",
                 len(requests) == 2 and feedback and integrated and verdict.get("verdict") == "GREEN",
                 {"executor_requests": len(requests), "feedback_has_blocking_review": feedback,
                  "status": _status(steps), "attempts": harness.attempts(), "final_delivery": verdict.get("verdict"),
                  "final_reason": verdict.get("reason")})
    if final is None:
        return [third, case("work-unit-delivery-chain-is-linked", False, "no final delivery evidence captured")]
    evidence, contract, kwargs = final["evidence"], final["contract"], final["kwargs"]

    def graded(mutate: Callable[[dict[str, Any]], None]) -> str:
        value = copy.deepcopy(evidence)
        mutate(value)
        return engine.verify_delivery(contract, value, **kwargs).get("verdict", "RED")

    def related_digest(value: dict[str, Any]) -> None:
        value["receipts"]["verifier"]["related_checks"]["receipt_digest"] = "sha256:" + "0" * 64

    def foreign_full(value: dict[str, Any]) -> None:
        value["receipts"]["checks"]["candidate_digest"] = "sha256:" + "1" * 64

    early_ref = ((evidence.get("receipts") or {}).get("verifier") or {}).get("review_ref") or {}
    raw_path = Path(early_ref["path"]) if early_ref.get("path") else None
    observed = {"related-digest": graded(related_digest), "foreign-full-checks": graded(foreign_full)}
    if raw_path is not None and raw_path.is_file():
        original = raw_path.read_bytes()
        raw_path.write_bytes(original + b" ")
        observed["early-review-bytes"] = engine.verify_delivery(contract, evidence, **kwargs).get("verdict", "RED")
        raw_path.write_bytes(original)
    else:
        observed["early-review-bytes"] = "no early review ref"
    seventh = case("work-unit-delivery-chain-is-linked",
                   observed == {"related-digest": "RED", "foreign-full-checks": "RED", "early-review-bytes": "RED"}
                   and verdict.get("verdict") == "GREEN", observed)
    return [third, seventh]


def c4_bounded_rework(root: Path) -> dict[str, Any]:
    harness = Harness(root, executor_mode="wrong-negative", max_attempts=2)
    steps = harness.drive(limit=6)
    launches = len(harness.requests())
    integrated = any((step.get("completion") or {}).get("status") == "integrated" for step in steps)
    ok = launches == 2 and not integrated and harness.reviewer.calls == 2
    return case("rework-is-bounded-by-the-attempt-budget", ok,
                {"executor_launches": launches, "reviewer_calls": harness.reviewer.calls,
                 "status": _status(steps), "attempts": harness.attempts()})


def c5_missing_or_foreign(root: Path) -> dict[str, Any]:
    observed = {}
    for mode in ("no-review", "foreign"):
        harness = Harness(root / mode, executor_mode="right", reviewer_mode=mode, max_attempts=1)
        steps = harness.drive(limit=3)
        observed[mode] = {"integrated": any((s.get("completion") or {}).get("status") == "integrated" for s in steps),
                          "status": _status(steps)}
    ok = all(row["integrated"] is False for row in observed.values())
    return case("missing-or-foreign-review-never-completes", ok, observed)


def c6_suggestions(root: Path) -> dict[str, Any]:
    harness = Harness(root, executor_mode="right", reviewer_mode="suggestion")
    steps = harness.drive()
    integrated = (steps[-1].get("completion") or {}).get("status") == "integrated" if steps else False
    with harness.store._connect() as conn:
        rows = conn.execute("SELECT event_type, payload_json FROM events WHERE event_type = ?",
                            ("task_area_discovery_candidate",)).fetchall()
    candidates = [json.loads(row["payload_json"]) for row in rows]
    replay = harness.consume()
    with harness.store._connect() as conn:
        after = conn.execute("SELECT COUNT(*) FROM events WHERE event_type = ?",
                             ("task_area_discovery_candidate",)).fetchone()[0]
        units = conn.execute("SELECT COUNT(*) FROM work_units").fetchone()[0]
    ok = (integrated and len(candidates) == 1 and candidates[0].get("problem_type") == "review_optimization"
          and after == 1 and units == 1 and (replay.get("executor_invocations") in (0, None)))
    return case("non-blocking-suggestions-are-recorded-once", ok,
                {"integrated": integrated, "candidates": len(candidates), "after_replay": after,
                 "work_units": units, "problem_type": candidates[0].get("problem_type") if candidates else None})


def c8_provider_schema(root: Path) -> dict[str, Any]:
    schema_of = executors.trusted_output_schema
    normalize = executors.normalize_trusted_provider_output
    policy = review_policy(root)
    context = engine.candidate_review_context(
        policy, candidate_digest="sha256:" + "c" * 64, base_sha="b" * 40, checks_digest="sha256:" + "d" * 64,
        scope={"allowed_paths": ["src/"]}, identity={"run_id": "r"})
    expected = {"candidate_digest": context["candidate_digest"], "checks_digest": context["checks_digest"],
                "candidate_review_context": context}
    schema = schema_of("verifier", review_version=2)
    base = {"schema": "lh-verifier-result/v2", "status": "completed", "verdict": "GREEN",
            "candidate_digest": context["candidate_digest"], "checks_digest": context["checks_digest"],
            "reason_code": "verified", "review": review_document(context, blocking=False)}
    base = {key: base[key] for key in schema["properties"] if key in base}

    def run(value: dict[str, Any], *, with_context: bool = True) -> dict[str, Any]:
        line = json.dumps({"schema": "lh-provider-result/v1", "outcome": "completed", "result": value,
                           "usage": {"state": "unknown"}})
        return normalize(line + "\n", stderr="", returncode=0, role="verifier", model="fixture-model",
                         expected=expected if with_context else {k: v for k, v in expected.items()
                                                                 if k != "candidate_review_context"})

    accepted = run(base)
    mutations: dict[str, Callable[[dict[str, Any]], None]] = {
        "missing-review": lambda v: v.pop("review"),
        "foreign-context": lambda v: v["review"].update(context_digest="sha256:" + "9" * 64),
        "extra-field": lambda v: v["review"].update(approved=True),
        "empty-requirements": lambda v: v["review"].update(requirements=[]),
        "verdict-disagrees": lambda v: v.update(verdict="RED"),
        "v1-schema": lambda v: v.update(schema="lh-verifier-result/v1"),
    }
    refused = {}
    for name, mutate in mutations.items():
        value = copy.deepcopy(base)
        mutate(value)
        refused[name] = run(value).get("outcome") != "completed"
    without_context = run(base, with_context=False).get("outcome") != "completed"
    ok = (accepted.get("outcome") == "completed" and (accepted.get("result") or {}).get("review") == base["review"]
          and all(refused.values()) and without_context and schema.get("additionalProperties") is False
          and schema["properties"]["review"].get("additionalProperties") is False)
    return case("provider-verifier-v2-is-explicit", ok,
                {"accepted": accepted.get("outcome"), "refused": refused, "v2_without_context_refused": without_context})


class _StubStore:
    """The narrow Store view the trusted role projection reads."""

    def __init__(self, root: Path, *, dispatch: dict[str, Any], attempt: dict[str, Any],
                 phases: dict[tuple[Any, ...], dict[str, Any]], lineage: dict[str, Any]):
        self.root, self._dispatch, self._attempt, self._phases, self._lineage = root, dispatch, attempt, phases, lineage

    def get_dispatch_consumption(self, _key):
        return self._dispatch

    def get_attempt(self, _run_id, _attempt):
        return self._attempt

    def get_completion_phase(self, _run_id, attempt, fence, phase, *_rest):
        return self._phases.get((attempt, fence, phase))

    def get_continuation_repair(self, _key):
        return self._lineage


def c9_trusted_feedback(root: Path) -> dict[str, Any]:
    harness_root = root / "fixture"
    policy = review_policy(harness_root)
    related, full = _commands()
    contract = completion_contract(harness_root, policy, max_attempts=3)
    packet_body = {"schema": "candidate-review-packet/v1", "task": "fix double", "write_set": ["src/"],
                   "forbidden_paths": ["secrets"], "completion_contract": contract,
                   "completion_contract_digest": digest_json(contract), "targeted_commands": related,
                   "full_validation_ref": "#/full_validation_plan"}
    packet = {**packet_body, "packet_digest": digest_json(packet_body)}
    packet_path = write(root / "packet.json", json.dumps(packet, sort_keys=True))
    context = engine.candidate_review_context(
        policy, candidate_digest="sha256:" + "c" * 64, base_sha="b" * 40, checks_digest="sha256:" + "d" * 64,
        scope={"allowed_paths": ["src/"]}, identity={"run_id": "r"})
    sealed = engine.seal_candidate_review_proof(review_document(context, blocking=True), context, root / "state")
    red = {"phase": "verifier", "verdict": "RED", "run_id": "r", "attempt": 1, "fence": 1, "phase_key": "k",
           "reason_code": "check_failed", **sealed}
    red["receipt_digest"] = digest_json(red)
    request = {"dispatch_key": "d", "goal_id": GOAL_ID, "goal_revision": 1, "node_id": NODE_ID,
               "work_unit_id": "w", "run_id": "r", "attempt": 2, "fence": 2, "packet_digest": packet["packet_digest"],
               "completion_repair": [red]}
    dispatch = {key: request[key] for key in ("goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt")}
    dispatch["envelope"] = {"packet_path": str(packet_path.resolve()), "packet_digest": packet["packet_digest"]}

    def project(red_row: dict[str, Any], extra: dict[str, Any] | None = None) -> Any:
        store = _StubStore(root / "state", dispatch=dispatch, attempt={"fence": 2},
                           phases={(1, 1, "verifier"): {"state": "settled", "evidence": red_row}},
                           lineage={"authority_digest": "auth", "red_receipt_digest": red_row["receipt_digest"]})
        return runner_adapter._trusted_role_input({**request, "completion_repair": [red_row], **(extra or {})},
                                                  role="coding", phase="coding", store=store, authority_digest="auth")

    projected = project(red)
    failure = (projected.get("role_context") or {}).get("failure") or {}
    carries = failure.get("review") == red["review"] and failure.get("review_ref") == red["review_ref"]
    raw = Path(red["review_ref"]["path"])
    original = raw.read_bytes()
    raw.write_bytes(original + b" ")
    tampered = reason_of(lambda: project(red))
    raw.write_bytes(original)
    stale = reason_of(lambda: project(red, {"candidate_review_context": {**context, "policy": {**policy, "requirements": [
        {"id": "other", "text": "x"}]}}, "candidate_digest": context["candidate_digest"],
        "checks_digest": context["checks_digest"], "base_sha": context["base_sha"]}))
    ok = (carries and tampered is not None and "trusted_role_review_feedback_invalid" in tampered
          and stale is not None and "trusted_role_review_context_invalid" in stale)
    return case("trusted-feedback-carries-the-red-review", ok,
                {"carries_review": carries, "tampered": tampered, "stale_context": stale})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-review-work-unit-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("review-policy-must-match-the-delivery-contract", lambda: c1_policy_conflict(root / "c1")),
            guarded("related-checks-run-before-the-reviewer", lambda: c2_related_checks_first(root / "c2")),
        ]
        try:
            results.extend(c3_c7_rework_and_chain(root / "c3"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:400]}"
            results.extend([case("red-review-returns-its-findings-to-the-next-attempt", False, detail),
                            case("work-unit-delivery-chain-is-linked", False, detail)])
        results.extend([
            guarded("rework-is-bounded-by-the-attempt-budget", lambda: c4_bounded_rework(root / "c4")),
            guarded("missing-or-foreign-review-never-completes", lambda: c5_missing_or_foreign(root / "c5")),
            guarded("non-blocking-suggestions-are-recorded-once", lambda: c6_suggestions(root / "c6")),
            guarded("provider-verifier-v2-is-explicit", lambda: c8_provider_schema(root / "c8")),
            guarded("trusted-feedback-carries-the-red-review", lambda: c9_trusted_feedback(root / "c9")),
        ])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(results),
        "results": results,
        "blocking_failures": [row["id"] for row in failures],
        "known_gaps_open": [
            "the reviewer is a deterministic callable; a real reviewing model is a human live smoke",
            "suggestions are recorded for later admission only; admitting them is X2",
        ],
    }, indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

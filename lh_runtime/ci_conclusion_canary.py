#!/usr/bin/env python3
"""Committed G6 smoke: one CI conclusion source on the existing verdict path."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
import ci_conclusion_adapter as ci
import external_action_port as eap
from controller import LoopController
from external_verdict import VerdictStore
import verifier_normalizer
from goal_store import GoalStore
from native_delivery_fixture import make_native_run
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


class FakeTransport:
    def __init__(self, responses: dict[str, dict[str, str]]):
        self.responses = responses
        self.requests: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.requests.append((url, headers, timeout))
        op_key = parse_qs(urlsplit(url).query).get("operation_key", [""])[0]
        return json.dumps(self.responses[op_key], sort_keys=True).encode("utf-8")


class IdempotentActionAdapter:
    def __init__(self):
        self.results: dict[str, dict[str, str]] = {}
        self.calls: list[str] = []

    def perform(self, op_key: str, request: dict[str, object]) -> dict[str, str]:
        if op_key not in self.results:
            self.calls.append(op_key)
            self.results[op_key] = {"operation_key": op_key, "external_id": "ci-action-1"}
        return self.results[op_key]


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _repo(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "config", "user.email", "ci@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(source), "config", "user.name", "CI Canary"], check=True)
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source), "add", "baseline.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "baseline"], check=True)
    base = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    return source, base


def _checks(name: str) -> list[dict[str, object]]:
    return [
        {
            "id": "source-check",
            "commands": [{
                "id": "source-diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        },
        {
            "id": "final-ci-check",
            "phase": "final",
            "commands": [{
                "id": "normalized-ci-file",
                "argv": [
                    sys.executable,
                    "-B",
                    "-c",
                    f"from pathlib import Path; assert Path('src/{name}.txt').read_text(encoding='utf-8') == 'ci candidate\\n'",
                ],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        },
    ]


def _verifier(name: str) -> list[str]:
    program = (
        "from pathlib import Path; import subprocess; "
        "assert Path('baseline.txt').read_text(encoding='utf-8') == 'baseline\\n'; "
        f"assert Path('src/{name}.txt').read_text(encoding='utf-8') == 'ci candidate\\n'; "
        "assert Path(subprocess.check_output(['git','rev-parse','--show-toplevel'], text=True).strip()).resolve() == Path.cwd().resolve()"
    )
    return [sys.executable, "-B", "-c", program]


def park_run(
    *,
    root: Path,
    source: Path,
    base: str,
    name: str,
    conclusion_store: VerdictStore,
    runs: RunStore,
    goals: GoalStore,
    controller: LoopController,
    action_ledger: eap.ActionLedger,
    adapter: object,
    outcome: str,
) -> tuple[str, dict[str, object]]:
    bundle = make_native_run(
        runs,
        source,
        base,
        name,
        "ci",
        _checks(name),
        _verifier(name),
        ["src/"],
        3,
        phase="async",
        goal={"goal_id": name, "feature_contract": "g6 native async fixture"},
        run_id=name,
    )
    goal = runs.get_run(bundle["run_id"])["goal"]
    event = goals.record_event(
        event_id=f"{name}-goal-event",
        source="g6-native-canary",
        event_type="goal_candidate",
        payload={"goal_id": name, "revision": bundle["contract"]["goal"]["revision"]},
    )
    goals.create_candidate(
        event["event_key"],
        goal_id=name,
        campaign_id="g6-native-canary",
        stage_id="ci-conclusion",
        goal=goal,
        revision=bundle["contract"]["goal"]["revision"],
    )
    goals.activate_with_run(name, bundle["run_id"], event_key=event["event_key"])

    def model(workspace: Path, _capsule: dict[str, object]) -> dict[str, object]:
        target = workspace / "src" / f"{name}.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("ci candidate\n", encoding="utf-8")
        return {"summary": "native CI provider shape", "usage": {"state": "unknown"}}

    parked = controller.tick_async(
        bundle["run_id"],
        holder=f"g6-{name}",
        model=model,
        verdict_store=conclusion_store,
        action_ledger=action_ledger,
        adapter=adapter,
        action_id="open-pr",
    )
    if parked.get("status") != "awaiting_external_verdict":
        raise AssertionError({
            "parked": parked,
            "run_state": runs.get_run(bundle["run_id"])["state"],
            "source_delivery": runs.verify_delivery(bundle["run_id"], phase="source"),
        })
    assert runs.verify_delivery(bundle["run_id"], phase="source")["verdict"] == "GREEN"
    return bundle["run_id"], {"parked": parked, "outcome": outcome, "goals": goals}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        action = IdempotentActionAdapter()
        external_action = IdempotentActionAdapter()
        op_key = eap.operation_key("run-g6-action", "ci-check", {"revision": "r1"})
        first = eap.dispatch(eap.ActionLedger(root / "actions"), action, op_key=op_key, request={"revision": "r1"}, at=1.0)
        replay = eap.dispatch(eap.ActionLedger(root / "actions"), action, op_key=op_key, request={"revision": "r1"}, at=2.0)
        crash_retry = eap.dispatch(eap.ActionLedger(root / "actions-after-crash"), action, op_key=op_key, request={"revision": "r1"}, at=3.0)

        responses: dict[str, dict[str, str]] = {}
        transport = FakeTransport(responses)
        adapter = ci.CIConclusionAdapter("https://ci.example.invalid/conclusions", "fixture-token", transport=transport)
        source, base = _repo(root)

        pending_runs = RunStore(root / "pending-runs", command_runner=fixture_command_runner)
        pending_verdicts = VerdictStore(root / "pending-verdicts")
        pending_goals = GoalStore(root / "pending-goals")
        pending_controller = LoopController(pending_runs, root / "pending-workspaces")
        pending_id, pending_info = park_run(
            root=root,
            source=source,
            base=base,
            name="run-g6-pending",
            conclusion_store=pending_verdicts,
            runs=pending_runs,
            goals=pending_goals,
            controller=pending_controller,
            action_ledger=eap.ActionLedger(root / "pending-actions"),
            adapter=external_action,
            outcome="pending",
        )
        pending_op = pending_info["parked"]["op_key"]
        responses[pending_op] = {"operation_key": pending_op, "conclusion": "pending"}
        pending = pending_controller.resume_external(
            verdict_store=pending_verdicts,
            source=adapter,
            normalizer=lambda **kwargs: verifier_normalizer.normalize_resolved_run(
                goal_store=pending_goals,
                run_store=pending_runs,
                verdict_store=pending_verdicts,
                **kwargs,
            ),
        )
        pending_state = pending_runs.get_run(pending_id)["state"]

        success_runs = RunStore(root / "success-runs", command_runner=fixture_command_runner)
        success_verdicts = VerdictStore(root / "success-verdicts")
        success_goals = GoalStore(root / "success-goals")
        success_controller = LoopController(success_runs, root / "success-workspaces")
        success_id, success_info = park_run(
            root=root,
            source=source,
            base=base,
            name="run-g6-success",
            conclusion_store=success_verdicts,
            runs=success_runs,
            goals=success_goals,
            controller=success_controller,
            action_ledger=eap.ActionLedger(root / "success-actions"),
            adapter=external_action,
            outcome="success",
        )
        success_op = success_info["parked"]["op_key"]
        responses[success_op] = {"operation_key": success_op, "conclusion": "success"}
        success = success_controller.resume_external(
            verdict_store=success_verdicts,
            source=adapter,
            normalizer=lambda **kwargs: verifier_normalizer.normalize_resolved_run(
                goal_store=success_goals,
                run_store=success_runs,
                verdict_store=success_verdicts,
                **kwargs,
            ),
        )
        success_state = success_runs.get_run(success_id)["state"]
        success_source = success_runs.delivery_evidence(success_id, phase="source")
        success_final = success_runs.delivery_evidence(success_id, phase="final")

        failure_runs = RunStore(root / "failure-runs", command_runner=fixture_command_runner)
        failure_verdicts = VerdictStore(root / "failure-verdicts")
        failure_goals = GoalStore(root / "failure-goals")
        failure_controller = LoopController(failure_runs, root / "failure-workspaces")
        failure_id, failure_info = park_run(
            root=root,
            source=source,
            base=base,
            name="run-g6-failure",
            conclusion_store=failure_verdicts,
            runs=failure_runs,
            goals=failure_goals,
            controller=failure_controller,
            action_ledger=eap.ActionLedger(root / "failure-actions"),
            adapter=external_action,
            outcome="failure",
        )
        failure_op = failure_info["parked"]["op_key"]
        responses[failure_op] = {"operation_key": failure_op, "conclusion": "failure"}
        failure = failure_controller.resume_external(
            verdict_store=failure_verdicts,
            source=adapter,
            normalizer=lambda **kwargs: verifier_normalizer.normalize_resolved_run(
                goal_store=failure_goals,
                run_store=failure_runs,
                verdict_store=failure_verdicts,
                **kwargs,
            ),
        )
        failure_state = failure_runs.get_run(failure_id)["state"]

        invalid_transport = FakeTransport({"op-g6-invalid": {"operation_key": "other", "conclusion": "success"}})
        invalid_adapter = ci.CIConclusionAdapter("https://ci.example.invalid/conclusions", "fixture-token", transport=invalid_transport)
        try:
            invalid_adapter("op-g6-invalid")
        except ci.CIResponseInvalid as exc:
            invalid_detail = type(exc).__name__
        else:
            invalid_detail = "accepted-invalid-response"

        try:
            ci.CIConclusionAdapter.from_env({})
        except ci.CICredentialsMissing as exc:
            missing_credentials_detail = type(exc).__name__
        else:
            missing_credentials_detail = "accepted-missing-credentials"

        request_url, request_headers, request_timeout = transport.requests[0]
        cases = [
            case("operation-key-is-stable-across-local-replay", first["sent"] and replay["deduped"] and first["result"] == replay["result"], str({"first": first, "replay": replay})),
            case("external-adapter-dedupes-after-local-ledger-loss", crash_retry["result"] == first["result"] and len(action.calls) == 1, str({"crash_retry": crash_retry, "calls": action.calls})),
            case("pending-ci-leaves-run-awaiting", pending == [] and pending_state == "awaiting_external_verdict", str({"pending": pending, "state": pending_state})),
            case("ci-success-resumes-verified", bool(success) and success[0].get("run_id") == success_id and success[0].get("op_key") == success_op and success[0].get("conclusion") == "success" and success[0].get("state") == "verified" and success_state == "verified", str(success)),
            case("source-parks-before-final-ci-obligation", success_source is not None and set(success_source["obligations"]) == {"source-check"}, str(success_source)),
            case("final-binds-normalized-ci-obligation", success_final is not None and set(success_final["obligations"]) == {"source-check", "final-ci-check"} and success_final["external_readback"]["normalized"]["outcome"] == "verified" and success_runs.verify_delivery(success_id, phase="final")["verdict"] == "GREEN", str(success_final)),
            case("ci-failure-resumes-retry", failure == [{"run_id": failure_id, "op_key": failure_op, "conclusion": "failure", "state": "retry_pending"}] and failure_state == "retry_pending", str(failure)),
            case("request-binds-operation-key-and-bearer", f"operation_key={pending_op}" in request_url and request_headers["Authorization"] == "Bearer fixture-token" and request_timeout == 10.0, request_url),
            case("mismatched-operation-key-fails-closed", invalid_detail == "CIResponseInvalid", invalid_detail),
            case("missing-credentials-fail-closed", missing_credentials_detail == "CICredentialsMissing", missing_credentials_detail),
        ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    result = {
        "check_id": "lh-goal-loop-g6-ci-conclusion",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/ci_conclusion_canary.py",
            "path": "existing external_verdict -> controller.resume_external",
            "network": "none; injected transport only",
        },
        "known_gaps_open": ["No service-specific push, merge, publish, quota, or promotion adapter is included in this G6 slice."],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

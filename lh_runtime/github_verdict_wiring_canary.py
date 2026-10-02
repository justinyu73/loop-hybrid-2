#!/usr/bin/env python3
"""Committed W4 smoke: production wiring for the GitHub verdict source.

Proves, offline with a fixture transport, that a contract-declared
``external_verdict`` block flows from ``resolve_project`` into ``run()``,
which builds the durable VerdictStore plus the GitHub conclusion source and
resumes parked runs: success verifies, failure retries, pending stays parked,
a missing token leaves the run parked while the resident driver still ticks,
a missing head_sha is not a verdict, an undeclared contract behaves exactly as
before, and the token never lands in any written file. No network, no real
GitHub, no real credentials.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from _fixture import make_campaign, make_source_repo
import external_action_port as eap
from controller import LoopController
from external_verdict import VerdictStore
from goal_store import GoalStore
from github_conclusion_source import GitHubCredentialsMissing, GitHubResponseInvalid
from native_delivery_fixture import make_native_run
from project_binding import CONTRACT_SCHEMA, resolve_project
from run_store import RunStore
from p7_native_runstore_fixture import explicit_runstore_factory
from p7_fence_fixture import fixture_command_runner
import goal_loop_run as glr

TOKEN = "w4-fixture-token"
GITHUB_VERDICT = {"owner": "octo-org", "repo": "fixture-repo", "workflow": "w4-build"}
SHA = "w4-head-sha-0001"
OP_KEY = "op-w4"
RUN_ID = "run-w4-parked"


class FakeTransport:
    """Records call count and the last URL; never touches the network."""

    def __init__(self, workflow_runs: list[dict[str, Any]]):
        self.payload = {"workflow_runs": workflow_runs}
        self.calls = 0
        self.last_url: str | None = None
        self.last_headers: dict[str, str] = {}

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.calls += 1
        self.last_url = url
        self.last_headers = dict(headers)
        return json.dumps(self.payload, sort_keys=True).encode("utf-8")


class _SeedActionAdapter:
    def __init__(self, head_sha: str | None):
        self.head_sha = head_sha

    def perform(self, op_key: str, _request: dict[str, Any]) -> dict[str, Any]:
        external: dict[str, Any] = {"pr": "fixture-pr-1"}
        if self.head_sha is not None:
            external["head_sha"] = self.head_sha
        return {"operation_key": op_key, "external_id": "w4-action", **external}


def workflow_run(status: str, conclusion: str | None) -> dict[str, Any]:
    return {"name": GITHUB_VERDICT["workflow"], "head_sha": SHA, "status": status,
            "conclusion": conclusion, "created_at": "2026-07-20T01:00:00Z"}


def _fake_factory(*, timeout_seconds: float = 900):
    def model(_workspace: Path, _capsule: dict) -> dict:
        return {"summary": f"unused fake executor ({timeout_seconds})"}
    return model


def _not_holder_driver(_worker: Any, **_kwargs: Any) -> dict[str, Any]:
    return {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}


def _seed(root: Path, source: Path, base: str, *, head_sha: str | None = SHA) -> None:
    """Park one run awaiting an external verdict, with the head_sha recorded
    in the parked action's external result (what a real PR adapter returns)."""
    runs = RunStore(root / "runs", command_runner=fixture_command_runner)
    goals = GoalStore(root / "goals")
    checks = [
        {"id": "w4-source-check", "commands": [{"id": "source-file", "argv": ["test", "-s", "src/w4.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]},
        {"id": "w4-final-check", "final_only": True, "commands": [{"id": "final-file", "argv": ["test", "-s", "src/w4.txt"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]},
    ]
    bundle = make_native_run(
        runs, source, base, "w4-parked", "github-verdict", checks,
        ["test", "-s", "src/w4.txt"], ["src/"], 4, phase="async",
        goal={"goal_id": "w4-parked", "campaign_id": "campaign-w4", "stage_id": "w4", "feature_contract": "github verdict fixture"},
        run_id=RUN_ID,
    )
    goal = runs.get_run(RUN_ID)["goal"]
    event = goals.record_event(event_id="w4-goal-event", idempotency_key="w4-goal-event", source="w4-canary", event_type="goal_candidate", payload={"goal_id": "w4-parked"})
    goals.create_candidate(event["event_key"], goal_id="w4-parked", campaign_id="campaign-w4", stage_id="w4", goal=goal, revision=bundle["contract"]["goal"]["revision"])
    goals.activate_with_run("w4-parked", RUN_ID, event_key=event["event_key"])
    verdicts = VerdictStore(root / "runs" / "verdict.sqlite3")
    ledger = eap.ActionLedger(root / "runs" / "action-ledger.sqlite3")
    controller = LoopController(runs, root / "workspaces")

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        target = workspace / "src" / "w4.txt"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("w4 candidate\n", encoding="utf-8")
        return {"summary": "native GitHub verdict fixture", "usage": {"state": "unknown"}}

    parked = controller.tick_async(
        RUN_ID,
        holder="w4-seed",
        model=model,
        verdict_store=verdicts,
        action_ledger=ledger,
        adapter=_SeedActionAdapter(head_sha),
        action_id="open-pr",
    )
    if parked.get("status") != "awaiting_external_verdict":
        raise AssertionError({"parked": parked, "run": runs.get_run(RUN_ID)})


def _resume(root: Path, source: Path, base: str, transport: FakeTransport,
            *, environ: dict[str, str] | None = None, driver_fn=_not_holder_driver) -> dict[str, Any]:
    env = {"LH_GITHUB_TOKEN": TOKEN} if environ is None else environ
    with explicit_runstore_factory(glr):
        return glr.run(
            executor="fake",
            execute=True,
            goal_store_root=root / "goals",
            run_store_root=root / "runs",
            workspace_root=root / "workspaces",
            campaign=make_campaign("campaign-w4"),
            source_repo=source,
            base_revision=base,
            # Wiring includes real final Git I/O, not a deadline assertion.
            # Match the bounded native fixture; cumulative expiry and process
            # timeout refusal live in test_generic_delivery_runstore.py.
            executor_timeout_seconds=30,
            github_verdict=dict(GITHUB_VERDICT),
            github_environ=env,
            github_transport=transport,
            factory_overrides={"fake": _fake_factory},
            driver_fn=driver_fn,
        )


def _write_contract(root: Path, source: Path, base: str, *, external_verdict: Any = None) -> Path:
    contract = {
        "schema": CONTRACT_SCHEMA,
        "project_id": "w4-project",
        "campaign": make_campaign("campaign-w4"),
        "source_repo": str(source),
        "base_revision": base,
        "runtime": {"goal_store": "rt/goals", "run_store": "rt/runs", "workspace_root": "rt/ws"},
    }
    if external_verdict is not None:
        contract["external_verdict"] = external_verdict
    path = root / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)

        # Case setup: one parked run per scenario root.
        scenario: dict[str, dict[str, Any]] = {}
        for name, runs_payload in (
            ("success", [workflow_run("completed", "success")]),
            ("failure", [workflow_run("completed", "failure")]),
            ("pending", [workflow_run("in_progress", None)]),
            ("no-sha", [workflow_run("completed", "success")]),
        ):
            case_root = root / name
            case_root.mkdir()
            _seed(case_root, source, base, head_sha=None if name == "no-sha" else SHA)
            transport = FakeTransport(runs_payload)
            error: str | None = None
            try:
                result = _resume(case_root, source, base, transport)
            except (GitHubCredentialsMissing, GitHubResponseInvalid) as exc:
                result, error = {}, type(exc).__name__
            final = RunStore(case_root / "runs").get_run(RUN_ID)
            verdict = VerdictStore(case_root / "runs" / "verdict.sqlite3").state(RUN_ID)
            scenario[name] = {"result": result, "error": error, "transport": transport,
                              "run_state": final["state"], "verdict": verdict}
            (case_root / "result.json").write_text(
                json.dumps(result, ensure_ascii=False, sort_keys=True), encoding="utf-8")

        # Missing token: the external verdict stays parked, no transport call
        # occurs, and the resident driver still runs to refresh liveness.
        missing_root = root / "missing-token"
        missing_root.mkdir()
        _seed(missing_root, source, base)
        missing_transport = FakeTransport([workflow_run("completed", "success")])
        driver_called = {"value": False}

        def recording_driver(_worker: Any, **_kwargs: Any) -> dict[str, Any]:
            driver_called["value"] = True
            return {}

        missing_error: str | None = None
        try:
            _resume(missing_root, source, base, missing_transport, environ={}, driver_fn=recording_driver)
        except GitHubCredentialsMissing as exc:
            missing_error = type(exc).__name__
        missing_run_state = RunStore(missing_root / "runs").get_run(RUN_ID)["state"]

        # Contract passthrough: declared block flows into run_kwargs; invalid blocks rejected.
        declared = resolve_project(_write_contract(root, source, base, external_verdict=dict(GITHUB_VERDICT)))["run_kwargs"]
        plain_root = root / "plain"
        plain_root.mkdir()
        undeclared = resolve_project(_write_contract(plain_root, source, base))["run_kwargs"]
        invalid_rejected = 0
        for index, bad in enumerate(({"owner": "", "repo": "r", "workflow": "w"}, "not-an-object")):
            bad_root = root / f"bad{index}"
            bad_root.mkdir()
            try:
                resolve_project(_write_contract(bad_root, source, base, external_verdict=bad))
            except SystemExit:
                invalid_rejected += 1

        # Undeclared contract: run() needs no verdict store, plan unchanged.
        dry = glr.run(
            executor="fake", execute=False,
            goal_store_root=root / "plain-goals", run_store_root=root / "plain-runs",
            workspace_root=root / "plain-ws", campaign=make_campaign("campaign-w4"),
            source_repo=source, base_revision=base, factory_overrides={"fake": _fake_factory},
        )

        # Token hygiene: scan every file written under the fixture roots.
        scanned = 0
        token_leaks: list[str] = []
        for path in root.rglob("*"):
            if path.is_file() and TOKEN.encode() in path.read_bytes():
                token_leaks.append(str(path.relative_to(root)))
            scanned += path.is_file()

        success = scenario["success"]
        failure = scenario["failure"]
        pending = scenario["pending"]
        no_sha = scenario["no-sha"]
        success_rows = success["result"].get("startup_external_resumed", [])
        success_resume = success_rows[0] if len(success_rows) == 1 else {}
        cases = [
            {"id": "declared-contract-passes-github-verdict-kwargs",
             "ok": declared.get("github_verdict") == GITHUB_VERDICT and invalid_rejected == 2,
             "detail": json.dumps({"github_verdict": declared.get("github_verdict"), "invalid_rejected": invalid_rejected})},
            {"id": "undeclared-contract-behaves-as-before",
             "ok": "github_verdict" not in undeclared and dry["mode"] == "dry_run"
             and dry["plan"]["gates"]["external_verdict_poll"] is False,
             "detail": json.dumps({"has_github_verdict": "github_verdict" in undeclared, "dry_mode": dry["mode"]})},
            {"id": "ci-success-resumes-parked-run-to-verified",
             "ok": success["error"] is None
             and success_resume.get("run_id") == RUN_ID
             and success_resume.get("conclusion") == "success"
             and success_resume.get("state") == "verified"
             and success_resume.get("normalized", {}).get("status") in {"ready", "already_normalized"}
             and success_resume.get("delivery", {}).get("verdict") == "GREEN"
             and success["run_state"] == "verified"
             and success["verdict"] == {"state": "verified", "conclusion": "success"}
             and success["transport"].calls == 1
             and success["transport"].last_headers.get("Authorization") == f"Bearer {TOKEN}",
             "detail": json.dumps({"resumed": success["result"].get("startup_external_resumed"),
                                   "run_state": success["run_state"], "verdict": success["verdict"]})},
            {"id": "ci-failure-resumes-parked-run-to-retry-pending",
             "ok": failure["error"] is None and failure["run_state"] == "retry_pending"
             and failure["verdict"] == {"state": "retry_pending", "conclusion": "failure"},
             "detail": json.dumps({"run_state": failure["run_state"], "verdict": failure["verdict"]})},
            {"id": "ci-pending-leaves-run-parked",
             "ok": pending["error"] is None and pending["result"].get("startup_external_resumed") == []
             and pending["run_state"] == "awaiting_external_verdict"
             and pending["verdict"] == {"state": "awaiting_external_verdict", "conclusion": None}
             and pending["transport"].calls == 1,
             "detail": json.dumps({"run_state": pending["run_state"], "verdict": pending["verdict"]})},
            {"id": "missing-token-keeps-verdict-parked-and-driver-live",
             "ok": missing_error is None and driver_called["value"]
             and missing_transport.calls == 0 and missing_run_state == "awaiting_external_verdict",
             "detail": json.dumps({"error": missing_error, "driver_called": driver_called["value"],
                                   "transport_calls": missing_transport.calls, "run_state": missing_run_state})},
            {"id": "missing-head-sha-stays-parked-not-a-verdict",
             "ok": no_sha["error"] is None and no_sha["run_state"] == "awaiting_external_verdict"
             and no_sha["transport"].calls == 0,
             "detail": json.dumps({"error": no_sha["error"], "run_state": no_sha["run_state"]})},
            {"id": "token-never-written-to-artifacts",
             "ok": not token_leaks and scanned > 0,
             "detail": json.dumps({"files_scanned": scanned, "leaks": token_leaks})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-github-verdict-wiring",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/github_verdict_wiring_canary.py",
                         "transport": "fixture only, no network, no real credentials"},
        "known_gaps_open": [
            "live GitHub smoke needs per-run approval and is not covered here",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

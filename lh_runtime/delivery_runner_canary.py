#!/usr/bin/env python3
"""Delivery binding and fence-backed delivery runner for the contract path.

A campaign stage that opts in with ``"delivery": {"derive": "acceptance_lamp"}``
gets a sealed delivery binding compiled from its operator-authored acceptance
lamp, and the compatibility run path executes delivery checks through the
configured execution fence.  Nothing here imports a ``tests/`` fixture: the
point of the exam is that a README-style contract reaches ``verified`` on the
production path alone.

Every case is platform-neutral.  E4/E5 run real children through the explicit
local-process fence, which contains nothing and says so in the evidence.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import execution_fence as fences  # noqa: E402

CHECK_ID = "lh-delivery-runner"
CAMPAIGN_ID = "delivery-runner-canary"
STAGE_ID = "hello"
TARGET_FILE = "src/hello.txt"
TARGET_TEXT = "hello from a declared executor"
LAMP = [sys.executable, "-B", "-c",
        "import pathlib, sys; path = pathlib.Path('src/hello.txt'); "
        f"sys.exit(0 if path.is_file() and path.read_text(encoding='utf-8').strip() == {TARGET_TEXT!r} else 1)"]
FIXTURE_MODULES = frozenset({"native_delivery_fixture", "p7_fence_fixture", "p7_native_runstore_fixture"})
# The declared executor writes the stage's target; the lamp then verifies it.
WRITER = ("import pathlib, sys; target = pathlib.Path('src/hello.txt'); "
          "target.parent.mkdir(parents=True, exist_ok=True); "
          f"target.write_text({TARGET_TEXT!r} + chr(10), encoding='utf-8')")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _git(*argv: str, cwd: Path) -> str:
    return subprocess.run(["git", *argv], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _source_repo(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir(parents=True)
    _git("init", "-q", cwd=source)
    (source / "README.md").write_text("delivery runner canary\n", encoding="utf-8")
    _git("add", "-A", cwd=source)
    _git("-c", "user.name=lh-canary", "-c", "user.email=lh-canary@example.invalid", "commit", "-qm", "base", cwd=source)
    return source, _git("rev-parse", "HEAD", cwd=source)


def _stage(*, opt_in: bool, lamp: list[str] | None = None, allowed: list[str] | None = None) -> dict[str, Any]:
    stage: dict[str, Any] = {
        "stage_id": STAGE_ID,
        "goal": {"feature_contract": f"Create {TARGET_FILE} containing exactly one line: {TARGET_TEXT}."},
        "allowed_paths": list(allowed or ["src/"]),
        "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {"id": f"{STAGE_ID}-lamp", "smoke": f"{TARGET_FILE} holds the expected line",
                            "verification_argv": list(lamp or LAMP)},
        "max_attempts": 1,
        "next_stage_id": None,
    }
    if opt_in:
        stage["delivery"] = {"derive": "acceptance_lamp"}
    return stage


def _campaign(*, opt_in: bool) -> dict[str, Any]:
    return {"schema": "lh-campaign/v1", "campaign_id": CAMPAIGN_ID, "stages": [_stage(opt_in=opt_in)]}


def _fake_factory(*, timeout_seconds: float = 900):
    import token_cost

    del timeout_seconds

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        target = Path(workspace) / TARGET_FILE
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(TARGET_TEXT + "\n", encoding="utf-8")
        return {"summary": "delivery runner canary model",
                "usage": token_cost.measured_usage(model="canary", input_tokens=2, output_tokens=1)}

    return model


def _attempt_states(run_store_root: Path) -> list[str]:
    from run_store import RunStore

    with RunStore(run_store_root)._connect() as conn:
        return [row["state"] for row in conn.execute("SELECT state FROM attempts ORDER BY run_id, ordinal").fetchall()]


def _run_fake(root: Path, campaign: dict[str, Any], source: Path, base: str,
              port: fences.ExecutionFencePort) -> dict[str, Any]:
    import goal_loop_run
    from command_ingress import submit_command
    from goal_store import GoalStore

    submit_command(GoalStore(root / "goals"), source="delivery-runner-canary", event_type="manual_intent",
                   event_id="canary-1", payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID})
    return goal_loop_run.run(
        executor="fake", execute=True, goal_store_root=root / "goals", run_store_root=root / "runs",
        workspace_root=root / "workspaces", campaign=campaign, source_repo=source, base_revision=base,
        holder="delivery-runner-canary", max_cycles=3, idle_limit=1, factory_overrides={"fake": _fake_factory},
        execution_fence_port=port, sleep_fn=lambda _seconds: None)


# -- E1: the opt-in stage compiles into a sealed binding ---------------------

def e1_binding_seals() -> dict[str, Any]:
    import delivery_binding
    import delivery_contract

    with tempfile.TemporaryDirectory(prefix="lh-delivery-binding-") as raw:
        source, base = _source_repo(Path(raw))
        stage = _stage(opt_in=True)
        before = json.dumps(stage, sort_keys=True)
        digest = "sha256:" + "a" * 64
        compiled = delivery_binding.compile_stage_delivery(
            stage, campaign_id=CAMPAIGN_ID, contract_ref="project_runtime_contract.json",
            contract_digest=digest, source_repo=str(source), base_revision=base)
    goal = compiled.get("goal") or {}
    contract = goal.get("delivery_contract") or {}
    plan = goal.get("delivery_plan") or {}
    packet = goal.get("delivery_packet") or {}
    delivery_contract.validate_contract(contract)
    delivery_contract.verify_plan_verdict(plan, contract)
    delivery_contract.verify_packet_binding(packet, contract, plan)
    verifier = contract.get("independent_verifier") or {}
    planner = contract.get("planner") or {}
    ok = (
        goal.get("delivery_required") is True
        and verifier.get("argv") == LAMP
        and verifier.get("read_only") is True and verifier.get("source_write") is False
        and planner.get("principal") == "operator-contract"
        and planner.get("source") == f"project_runtime_contract.json@{digest}"
        and (contract.get("scope") or {}).get("allowed_paths") == ["src/"]
        and json.dumps(stage, sort_keys=True) == before
    )
    return case("lamp-derived-binding-seals", ok, {
        "planner": planner, "verifier_argv_matches_lamp": verifier.get("argv") == LAMP,
        "obligations": [item.get("id") for item in contract.get("obligations") or []],
        "input_unchanged": json.dumps(stage, sort_keys=True) == before})


# -- E2: a verifier the agent could edit is refused ---------------------------

def e2_write_scope_refused() -> dict[str, Any]:
    import delivery_binding

    refusals: dict[str, str | None] = {}
    with tempfile.TemporaryDirectory(prefix="lh-delivery-binding-") as raw:
        source, base = _source_repo(Path(raw))
        variants = {
            "readme-example": (["sh", "gate-pack/verify.sh"], ["lh_runtime/", "gate-pack/"]),
            "dot-relative": (["sh", "./src/check.sh"], ["src/"]),
            "outside-scope": (["sh", "checks/verify.sh"], ["src/"]),
        }
        for name, (lamp, allowed) in variants.items():
            try:
                delivery_binding.compile_stage_delivery(
                    _stage(opt_in=True, lamp=lamp, allowed=allowed), campaign_id=CAMPAIGN_ID,
                    contract_ref="c.json", contract_digest="sha256:" + "b" * 64,
                    source_repo=str(source), base_revision=base)
                refusals[name] = None
            except delivery_binding.DeliveryBindingError as exc:
                refusals[name] = str(exc).split(":")[0]
    expected = {"readme-example": "independent_verifier_in_write_scope",
                "dot-relative": "independent_verifier_in_write_scope",
                "outside-scope": None}
    return case("verifier-in-write-scope-is-refused", refusals == expected, refusals)


# -- E3: without the opt-in nothing changes -----------------------------------

def e3_no_opt_in_unchanged() -> dict[str, Any]:
    import delivery_binding

    with tempfile.TemporaryDirectory(prefix="lh-delivery-runner-") as raw:
        root = Path(raw)
        source, base = _source_repo(root)
        campaign = _campaign(opt_in=False)
        before = json.dumps(campaign, sort_keys=True)
        compiled = delivery_binding.compile_campaign_delivery(
            campaign, contract_ref="c.json", contract_digest="sha256:" + "c" * 64,
            source_repo=str(source), base_revision=base)
        unchanged = json.dumps(compiled, sort_keys=True) == before
        result = _run_fake(root, compiled, source, base, fences.DisabledExecutionFencePort("canary-disabled"))
        states = _attempt_states(root / "runs")
    del result
    ok = unchanged and states == []
    return case("stage-without-opt-in-is-unchanged", ok, {"unchanged": unchanged, "attempts": states})


# -- E6: no fence, no runner, and the plan says why ---------------------------

def e6_disabled_fence_reports() -> dict[str, Any]:
    import delivery_binding

    with tempfile.TemporaryDirectory(prefix="lh-delivery-runner-") as raw:
        root = Path(raw)
        source, base = _source_repo(root)
        compiled = delivery_binding.compile_campaign_delivery(
            _campaign(opt_in=True), contract_ref="c.json", contract_digest="sha256:" + "d" * 64,
            source_repo=str(source), base_revision=base)
        result = _run_fake(root, compiled, source, base, fences.DisabledExecutionFencePort("canary-disabled"))
        states = _attempt_states(root / "runs")
    runner = (result.get("plan") or {}).get("delivery_command_runner")
    leaked = sorted(FIXTURE_MODULES & set(sys.modules))
    ok = (runner == {"status": "unavailable", "reason": "canary-disabled"}
          and states == ["human_required"] and not leaked)
    return case("disabled-fence-reports-runner-unavailable", ok,
                {"delivery_command_runner": runner, "attempts": states, "fixtures_loaded": leaked})


# -- E4: delivery checks run through the configured fence port ----------------

def e4_check_through_fence() -> dict[str, Any]:
    from fence_command_runner import FenceCommandRunner

    with tempfile.TemporaryDirectory(prefix="lh-delivery-runner-") as raw:
        root = Path(raw).resolve()
        port = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "local-process"})
        source, base = _source_repo(root)
        (source / "staged.txt").write_text("staged\n", encoding="utf-8")
        _git("add", "staged.txt", cwd=source)
        context = {"goal_id": f"{CAMPAIGN_ID}:{STAGE_ID}", "goal_revision": 1, "node_id": STAGE_ID,
                   "unit_id": "canary-unit", "run_id": "run-e4", "attempt": 1, "fence": 1, "base_sha": base,
                   "authority_store": "run", "worktree": str(source), "execution_phase": "delivery_checks",
                   "command_id": "e4"}
        runner = FenceCommandRunner(port)
        diff_check, _, _ = runner(dict(context), phase="delivery_checks",
                                  argv=["git", "diff", "--cached", "--check"],
                                  worktree=str(source), timeout_seconds=60)
        probe, evidence, _ = runner(dict(context, command_id="e4-cwd"), phase="delivery_checks",
                                    argv=[sys.executable, "-B", "-c", "import os; print(os.getcwd())"],
                                    worktree=str(source), timeout_seconds=60)
        fence_evidence = evidence.get("execution_fence") or {}
        ok = (diff_check.returncode == 0 and probe.returncode == 0
              and Path(probe.stdout.strip()).resolve() == source.resolve()
              and (fence_evidence.get("backend") or {}).get("backend_id") == "local-process"
              and fence_evidence.get("kernel_containment") is False)
        return case("delivery-check-runs-through-the-fence-port", ok, {
            "diff_check": [diff_check.returncode, (diff_check.stderr or "")[-200:]],
            "cwd": probe.stdout.strip(), "backend": (fence_evidence.get("backend") or {}).get("backend_id"),
            "kernel_containment": fence_evidence.get("kernel_containment")})


# -- E5: the README contract path reaches verified with production code only ---

def e5_child(root: Path) -> int:
    """Run the README contract path in a fresh process and report what it loaded."""
    import goal_loop_run
    from command_ingress import submit_command
    from goal_store import GoalStore
    from run_store import RunStore

    source, base = _source_repo(root)
    contract = {
        "schema": "lh-project-runtime-contract/v1",
        "project_id": "delivery-runner-canary",
        "campaign": _campaign(opt_in=True),
        "source_repo": str(source),
        "base_revision": base,
        "runtime": {"goal_store": str(root / "goals"), "run_store": str(root / "runs"),
                    "workspace_root": str(root / "workspaces")},
        "executors": {"coder": {"argv": [sys.executable, "-B", "-c", WRITER, "{prompt}"]}},
        "models": {"execute": "coder"},
    }
    contract_path = root / "project_runtime_contract.json"
    contract_path.write_text(json.dumps(contract, indent=2), encoding="utf-8")
    submit_command(GoalStore(root / "goals"), source="delivery-runner-canary", event_type="manual_intent",
                   event_id="e5", payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID})
    os.environ["LH_EXECUTION_FENCE_BACKEND"] = "local-process"
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        code = goal_loop_run.main(["--contract", str(contract_path), "--execute",
                                   "--max-cycles", "3", "--idle-limit", "1"])
    runs = RunStore(root / "runs")
    with runs._connect() as conn:
        states = [row["state"] for row in conn.execute("SELECT state FROM attempts").fetchall()]
        rows = [value for row in conn.execute(
            "SELECT delivery_source_evidence_json, delivery_final_evidence_json FROM runs").fetchall()
            for value in row if value]
    evidence = " ".join(str(value) for value in rows)
    print(json.dumps({
        "exit": code,
        "attempts": states,
        "fixtures_loaded": sorted(FIXTURE_MODULES & set(sys.modules)),
        "delivery_through_local_process": "local-process" in evidence,
        "tail": output.getvalue()[-300:],
    }))
    return 0


def e5_contract_path() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="lh-delivery-runner-") as raw:
        root = Path(raw).resolve()
        child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--e5-child", str(root)],
                               capture_output=True, text=True, timeout=600)
        lines = [line for line in child.stdout.splitlines() if line.strip().startswith("{")]
        report = json.loads(lines[-1]) if lines else {"stderr": child.stderr[-600:]}
    ok = (child.returncode == 0 and report.get("attempts") == ["verified"]
          and report.get("fixtures_loaded") == [] and report.get("delivery_through_local_process") is True)
    return case("contract-path-verifies-without-test-fixtures", ok, report)


def _guarded(name: str, action) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        return case(name, False, f"{type(exc).__name__}: {exc}")


def main() -> int:
    cases = [
        _guarded("lamp-derived-binding-seals", e1_binding_seals),
        _guarded("verifier-in-write-scope-is-refused", e2_write_scope_refused),
        _guarded("stage-without-opt-in-is-unchanged", e3_no_opt_in_unchanged),
        _guarded("delivery-check-runs-through-the-fence-port", e4_check_through_fence),
        _guarded("contract-path-verifies-without-test-fixtures", e5_contract_path),
        _guarded("disabled-fence-reports-runner-unavailable", e6_disabled_fence_reports),
    ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
        "known_gaps_open": [
            "local-process delivery checks are not contained; a containment backend is an operator-supplied port",
        ],
    }, ensure_ascii=False, indent=2, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--e5-child":
        raise SystemExit(e5_child(Path(sys.argv[2])))
    raise SystemExit(main())

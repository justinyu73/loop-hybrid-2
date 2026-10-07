#!/usr/bin/env python3
"""Verifier readiness: a verifier that cannot run never spends an attempt.

Measured before this gate existed: with the acceptance lamp pointing at a
program that does not exist, every tick raised ``FileNotFoundError`` and still
consumed one attempt; after the fourth the run sat in ``human_required`` with
no recorded reason.

The controller now checks, before an attempt begins, that the verifier can be
launched at all.  Not ready means no attempt, no model call, a single
``verifier_unavailable`` event, and ``waiting_for_verifier``; the next tick
looks again, so a verifier that appears lets the run continue with no retry
machinery.  A launch that fails only after the attempt began (a path inside the
clone) ends that attempt with a typed ``verifier_unavailable`` reason instead
of an exception.  The worker does not count waiting as progress, so the driver
idles rather than spins.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))
from _fixture import make_campaign, make_source_repo  # noqa: E402
from campaign_compiler import CampaignCompiler  # noqa: E402
from controller import LoopController  # noqa: E402
from goal_loop_worker import GoalLoopWorker  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from native_delivery_fixture import make_native_run  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402
import open_questions  # noqa: E402

CHECK_ID = "lh-verifier-readiness"
WINDOWS = sys.platform == "win32"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


class Model:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, workspace: Path, _capsule: dict) -> dict:
        self.calls += 1
        (Path(workspace) / "bounded.txt").write_text("x\n", encoding="utf-8")
        return {"summary": "readiness fixture", "usage": {"state": "unknown"}}


def _verifier_path(root: Path) -> Path:
    return root / "bin" / ("verifier.cmd" if WINDOWS else "verifier")


def _install_verifier(path: Path) -> None:
    """A verifier that passes once bounded.txt exists in its working directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if WINDOWS:
        path.write_text("@if exist bounded.txt (exit /b 0) else (exit /b 1)\r\n", encoding="utf-8")
    else:
        path.write_text("#!/bin/sh\ntest -f bounded.txt\n", encoding="utf-8")
        path.chmod(0o755)


def _run(root: Path, argv: list[str]) -> tuple[RunStore, LoopController, str]:
    source, base = make_source_repo(root)
    store = RunStore(root / "runs", command_runner=fixture_command_runner)
    controller = LoopController(store, root / "workspaces")
    run_id = make_native_run(
        store, source, base, "verifier-readiness", "readiness",
        [{"id": "bounded", "commands": [{"id": "file", "argv": ["git", "rev-parse", "HEAD"], "cwd": "${WORKTREE}",
                                          "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
        argv, ["bounded.txt"], 4, goal={"case": "verifier-readiness"}, run_id="run-readiness",
    )["run_id"]
    return store, controller, run_id


def _unavailable_events(store: RunStore, run_id: str) -> list[dict[str, Any]]:
    return [row for row in store.events(run_id) if row.get("event_type") == "verifier_unavailable"]


def c1_missing_spends_nothing(root: Path) -> dict[str, Any]:
    argv = [str(_verifier_path(root))]
    store, controller, run_id = _run(root, argv)
    model = Model()
    results = [controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv) for _ in range(3)]
    run = store.get_run(run_id)
    events = _unavailable_events(store, run_id)
    ok = (all(row.get("status") == "waiting_for_verifier" for row in results)
          and all(str(row.get("reason", "")).startswith("verifier_unavailable") for row in results)
          and run["attempts"] == 0 and model.calls == 0 and run["state"] in {"queued", "retry_pending"}
          and len(events) == 1)
    return case("a-missing-verifier-spends-no-attempt-and-no-model-call", ok,
                {"results": results, "attempts": run["attempts"], "state": run["state"], "model_calls": model.calls,
                 "events": len(events)})


def c2_not_on_path(root: Path) -> dict[str, Any]:
    argv = ["lh-verifier-readiness-not-on-path-x29"]
    store, controller, run_id = _run(root, argv)
    model = Model()
    result = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
    run = store.get_run(run_id)
    ok = (result.get("status") == "waiting_for_verifier" and run["attempts"] == 0 and model.calls == 0
          and len(_unavailable_events(store, run_id)) == 1)
    return case("a-verifier-not-on-path-is-not-ready", ok, {"result": result, "attempts": run["attempts"]})


def c3_appears_then_runs(root: Path) -> dict[str, Any]:
    verifier = _verifier_path(root)
    argv = [str(verifier)]
    store, controller, run_id = _run(root, argv)
    model = Model()
    waiting = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
    _install_verifier(verifier)
    done = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
    run = store.get_run(run_id)
    ok = (waiting.get("status") == "waiting_for_verifier" and done.get("status") == "verified"
          and run["state"] == "verified" and run["attempts"] == 1 and model.calls == 1)
    return case("a-verifier-that-appears-lets-the-same-run-continue", ok,
                {"waiting": waiting, "done": done.get("status"), "attempts": run["attempts"], "model_calls": model.calls})


def c4_launch_failure_is_typed(root: Path) -> dict[str, Any]:
    argv = ["checks/verifier-missing-inside-the-clone"]  # resolved inside the clone, so only launch can tell
    store, controller, run_id = _run(root, argv)
    model = Model()
    try:
        result = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
        raised = None
    except Exception as exc:
        result, raised = {}, f"{type(exc).__name__}: {exc}"
    run = store.get_run(run_id)
    events = _unavailable_events(store, run_id)
    ok = (raised is None and result.get("status") == "human_required"
          and str(result.get("reason", "")).startswith("verifier_unavailable")
          and run["state"] == "human_required" and run["attempts"] == 1 and len(events) == 1
          and events[0].get("payload", {}).get("attempt") == 1)
    return case("a-launch-failure-after-the-attempt-began-is-typed-not-raised", ok,
                {"raised": raised, "result": result, "state": run["state"], "attempts": run["attempts"],
                 "events": events})


def c8_final_launch_failure_is_typed(root: Path) -> dict[str, Any]:
    verifier = _verifier_path(root)
    _install_verifier(verifier)
    argv = [str(verifier)]
    store, controller, run_id = _run(root, argv)

    class DeletingModel(Model):
        def __call__(self, workspace: Path, capsule: dict) -> dict:
            result = super().__call__(workspace, capsule)
            verifier.unlink()  # the precheck launched it; the final launch cannot
            return result

    model = DeletingModel()
    try:
        result = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
        raised = None
    except Exception as exc:
        result, raised = {}, f"{type(exc).__name__}: {exc}"
    run = store.get_run(run_id)
    events = _unavailable_events(store, run_id)
    ok = (raised is None and model.calls == 1 and result.get("status") == "human_required"
          and str(result.get("reason", "")).startswith("verifier_unavailable:launch_failed")
          and result.get("provider_invocations") == 1 and run["state"] == "human_required" and len(events) == 1)
    return case("a-final-launch-failure-after-the-model-ran-is-typed", ok,
                {"raised": raised, "result": result, "state": run["state"], "events": len(events)})


def c5_open_question_kind(_root: Path) -> dict[str, Any]:
    kind = open_questions.classify("verifier_unavailable:not_found")
    # Unknown codes also land on awaiting_owner; require an explicit entry, not the fallback.
    listed = dict(open_questions.KIND_BY_PREFIX).get("verifier_unavailable")
    return case("verifier-unavailable-awaits-the-owner",
                kind == open_questions.AWAITING_OWNER and listed == open_questions.AWAITING_OWNER,
                {"kind": kind, "listed": listed})


def c6_red_path_unchanged(root: Path) -> dict[str, Any]:
    verifier = root / "bin" / ("red.cmd" if WINDOWS else "red")
    verifier.parent.mkdir(parents=True, exist_ok=True)
    if WINDOWS:
        verifier.write_text("@exit /b 1\r\n", encoding="utf-8")
    else:
        verifier.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        verifier.chmod(0o755)
    argv = [str(verifier)]
    store, controller, run_id = _run(root, argv)
    model = Model()
    result = controller.tick(run_id, holder="readiness", model=model, verifier_argv=argv)
    run = store.get_run(run_id)
    ok = (result.get("status") == "retry_pending" and run["attempts"] == 1 and model.calls == 1
          and not _unavailable_events(store, run_id))
    return case("a-verifier-that-runs-red-keeps-the-retry-path", ok,
                {"result": result.get("status"), "attempts": run["attempts"], "model_calls": model.calls})


def c7_worker_waits_idle(root: Path) -> dict[str, Any]:
    source, base = make_source_repo(root)
    campaign_id = "campaign-readiness"
    campaign = make_campaign(campaign_id)
    missing = str(_verifier_path(root))
    campaign["stages"][0]["acceptance_lamp"]["verification_argv"] = [missing]
    runs = RunStore(root / "w-runs", command_runner=fixture_command_runner)
    worker = GoalLoopWorker(
        goal_store=GoalStore(root / "w-goals"), run_store=runs,
        controller=LoopController(runs, root / "w-workspaces"),
        compilers={campaign_id: CampaignCompiler(campaign)},
        execution_context={campaign_id: {"source_repo": source, "base_revision": base}},
    )
    goal_id = f"{campaign_id}:stage-1"
    envelope = worker.compilers[campaign_id].compile()["stages"]["stage-1"]
    goal = {"feature_contract": "stage-1", "admission_envelope": envelope}
    worker.goal_store.record_event(event_id="readiness", idempotency_key="readiness", source="manual_intent",
                                   event_type="goal_candidate",
                                   payload={"candidate": {"goal_id": goal_id, "campaign_id": campaign_id,
                                                          "stage_id": "stage-1", "goal": goal}})
    worker.goal_store.create_candidate("readiness", goal_id=goal_id, campaign_id=campaign_id, stage_id="stage-1",
                                       goal=goal)
    native = make_native_run(
        runs, source, base, goal_id, "readiness",
        [{"id": "c", "commands": [{"id": "f", "argv": ["git", "rev-parse", "HEAD"], "cwd": "${WORKTREE}",
                                    "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
        [missing], ["src/"], 4, goal=goal,
    )
    worker.goal_store.activate_with_run(goal_id, native["run_id"])
    model = Model()
    worker.tick(holder="readiness", model=model)  # first pass may reconcile startup state
    second = worker.tick(holder="readiness", model=model)
    run = runs.get_run(native["run_id"])
    ok = (second.get("status") == "idle" and (second.get("run") or {}).get("status") == "waiting_for_verifier"
          and run["attempts"] == 0 and model.calls == 0)
    return case("the-worker-does-not-count-waiting-as-progress", ok,
                {"status": second.get("status"), "run": second.get("run"), "attempts": run["attempts"]})


def main() -> int:
    builds = [
        ("a-missing-verifier-spends-no-attempt-and-no-model-call", c1_missing_spends_nothing),
        ("a-verifier-not-on-path-is-not-ready", c2_not_on_path),
        ("a-verifier-that-appears-lets-the-same-run-continue", c3_appears_then_runs),
        ("a-launch-failure-after-the-attempt-began-is-typed-not-raised", c4_launch_failure_is_typed),
        ("verifier-unavailable-awaits-the-owner", c5_open_question_kind),
        ("a-verifier-that-runs-red-keeps-the-retry-path", c6_red_path_unchanged),
        ("the-worker-does-not-count-waiting-as-progress", c7_worker_waits_idle),
        ("a-final-launch-failure-after-the-model-ran-is-typed", c8_final_launch_failure_is_typed),
    ]
    with tempfile.TemporaryDirectory(prefix="lh-verifier-readiness-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        results = []
        for name, build in builds:
            (root / name).mkdir()
            results.append(guarded(name, lambda build=build, name=name: build(root / name)))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

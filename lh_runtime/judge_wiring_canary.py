#!/usr/bin/env python3
"""Committed M1/M2 smoke: model routing — judge argv, CLI judge wrapper, run()/contract wiring."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))
import cli_agent_executor as executors
import diff_grader
import project_binding
import turning_point as tp
from executor_wiring_canary import _noop_sleep, _seed, _source_repo, campaign, case, fake_executor_factory
from goal_loop_run import run
from p7_native_runstore_fixture import explicit_runstore_factory
import goal_loop_run as fixture_glr


def _ok(fn) -> tuple[bool, str]:
    try:
        fn()
        return False, "no error raised"
    except (ValueError, KeyError, RuntimeError) as exc:
        return True, f"{type(exc).__name__}: {exc}"


def main() -> int:
    cases: list[dict[str, object]] = []

    # parse_decision: clean JSON, prose-wrapped JSON, and garbage.
    clean = tp.parse_decision('{"decision": "select:g1"}')
    noisy = tp.parse_decision('Here is my choice:\n{"decision": "human_required"}\nThanks.')
    agy_envelope = tp.parse_decision(json.dumps({
        "status": "SUCCESS",
        "response": '{"decision": "select:g1"}',
    }))
    agy_grade_envelope = diff_grader.parse_grade(json.dumps({
        "status": "SUCCESS",
        "response": '{"grade": "sensitive", "rationale": "fixture"}',
    }))
    agy_failure_raises, _ = _ok(lambda: tp.parse_decision(json.dumps({
        "status": "ERROR",
        "response": '{"decision": "human_required"}',
    })))
    agy_grade_failure_raises, _ = _ok(lambda: diff_grader.parse_grade(json.dumps({
        "status": "ERROR",
        "response": '{"grade": "routine", "rationale": "fixture"}',
    })))
    garbage_raises, _ = _ok(lambda: tp.parse_decision("no json at all"))
    nodec_raises, _ = _ok(lambda: tp.parse_decision('{"other": 1}'))
    cases.append(case(
        "parse-decision-clean-noisy-garbage",
        clean == {"decision": "select:g1"}
        and noisy == {"decision": "human_required"}
        and agy_envelope == {"decision": "select:g1"}
        and agy_grade_envelope == {"grade": "sensitive", "rationale": "fixture"}
        and agy_failure_raises
        and agy_grade_failure_raises
        and garbage_raises
        and nodec_raises,
        json.dumps({
            "clean": clean,
            "noisy": noisy,
            "agy_envelope": agy_envelope,
            "agy_grade_envelope": agy_grade_envelope,
            "agy_failure_raises": agy_failure_raises,
            "agy_grade_failure_raises": agy_grade_failure_raises,
        }),
    ))

    # judge_argv: per-CLI shape, with and without a pinned model. D3: the judge
    # posture is read-only; the executor posture lives in provider_argv and is
    # asserted separately below. Retired adapters must fail closed.
    claude_rejected, _ = _ok(lambda: executors.judge_argv("claude", "P"))
    shapes_ok = (
        executors.judge_argv("codex", "P") == ["codex", "exec", "--sandbox", "read-only", "--ephemeral", "P"]
        and executors.judge_argv("codex", "P", "gpt-5.6-sol") == ["codex", "exec", "-m", "gpt-5.6-sol", "--sandbox", "read-only", "--ephemeral", "P"]
        and executors.judge_argv("agy", "P", "gemini-3.1-pro-high") == [
            "agy", "--model", "gemini-3.1-pro-high", "--mode", "plan",
            "--disable-slash-commands", "--output-format", "json",
            "--print-timeout", "300s", "--print", "P",
        ]
    )
    unknown_raises, _ = _ok(lambda: executors.judge_argv("nope", "P"))
    agy_model_required, _ = _ok(lambda: executors.judge_argv("agy", "P"))
    cases.append(case(
        "judge-argv-shapes-with-model-pin",
        shapes_ok and claude_rejected and unknown_raises and agy_model_required,
        f"shapes_ok={shapes_ok} claude_rejected={claude_rejected} agy_model_required={agy_model_required}",
    ))

    # D3 mutation guard: no judge argv for any executor, with or without a
    # model pin, carries a bypass token -- and the executor posture still does,
    # so the split did not quietly make mutation runs read-only. Revert
    # judge_argv to the old shape and the first half goes red; point
    # hosted_provider_argv back at judge_argv and the second half goes red.
    judge_shapes = [
        executors.judge_argv(name, "P", model)
        for name, model in (("codex", None), ("codex", "m"),
                            ("agy", "gemini-3.1-pro-high"))
    ]
    judge_carries_bypass = [
        argv for argv in judge_shapes if any(token in argv for token in executors.BYPASS_TOKENS)
    ]
    executor_keeps_bypass = (
        any(token in executors.provider_argv("codex", "P", "m") for token in executors.BYPASS_TOKENS)
        and any(token in executors.hosted_provider_argv("codex", "P", "m") for token in executors.BYPASS_TOKENS)
    )
    cases.append(case(
        "judge-argv-never-carries-a-bypass-token-and-executor-argv-still-does",
        not judge_carries_bypass and executor_keeps_bypass,
        f"judge_carries_bypass={judge_carries_bypass} executor_keeps_bypass={executor_keeps_bypass}",
    ))

    # The shared process seam must detach IDE bridge state and never use the
    # caller's repository as the judge cwd.
    previous_ide_port = os.environ.get("GEMINI_CLI_IDE_SERVER_PORT")
    os.environ["GEMINI_CLI_IDE_SERVER_PORT"] = "fixture-ide-port"
    try:
        bounded_stdout = executors.run_bounded_judge([
            sys.executable,
            "-c",
            "import json, os; print(json.dumps({'status':'SUCCESS','response':'{\\\"decision\\\":\\\"select:g1\\\"}', 'cwd':os.getcwd(), 'ide_keys':sorted(k for k in os.environ if k.startswith('GEMINI_CLI_IDE_'))}))",
        ], name="fixture")
    finally:
        if previous_ide_port is None:
            os.environ.pop("GEMINI_CLI_IDE_SERVER_PORT", None)
        else:
            os.environ["GEMINI_CLI_IDE_SERVER_PORT"] = previous_ide_port
    bounded_payload = json.loads(bounded_stdout)
    cases.append(case(
        "bounded-judge-detaches-ide-and-target-cwd",
        bounded_payload.get("ide_keys") == []
        and Path(str(bounded_payload.get("cwd", ""))).name.startswith("lh-judge-"),
        json.dumps({"cwd": bounded_payload.get("cwd"), "ide_keys": bounded_payload.get("ide_keys")}),
    ))

    # make_cli_judge roundtrip via a fake CLI; failure and garbage raise.
    fake_cli = lambda prompt: [sys.executable, "-c", "import json; print(json.dumps({'decision': 'select:g1'}))"]
    judge = tp.make_cli_judge(fake_cli, name="fake")
    raw = judge({"runnable_children": [{"goal_id": "g1", "run_id": "r1", "priority": 0, "depends_on": []}]})
    failing = tp.make_cli_judge(lambda prompt: [sys.executable, "-c", "import sys; sys.exit(1)"], name="fail")
    garbage = tp.make_cli_judge(lambda prompt: [sys.executable, "-c", "print('sorry, cannot help')"], name="garbage")
    fail_raises, _ = _ok(lambda: failing({}))
    garbage_raises, _ = _ok(lambda: garbage({}))
    cases.append(case(
        "cli-judge-roundtrip-and-failures",
        raw == {"decision": "select:g1"} and fail_raises and garbage_raises,
        json.dumps({"raw": raw}),
    ))

    with tempfile.TemporaryDirectory() as raw_dir:
        root = Path(raw_dir)
        source, base = _source_repo(root)
        camp = campaign()

        # run() threads turning_point into the driver: the judge sees the
        # snapshot and its in-set select is honored end to end.
        class JudgeSpy:
            def __init__(self) -> None:
                self.called = False
                self.snapshot = None

            def __call__(self, snapshot):
                self.called = True
                self.snapshot = snapshot
                return {"decision": "select:campaign-exec:stage-1"}

        spy = JudgeSpy()
        _seed(root / "j-goals", camp, source=source, base=base)
        with explicit_runstore_factory(fixture_glr):
            wired = run(
                executor="fake", execute=True, goal_store_root=root / "j-goals", run_store_root=root / "j-runs",
                workspace_root=root / "j-ws", campaign=camp, source_repo=source, base_revision=base,
                max_cycles=30, factory_overrides={"fake": fake_executor_factory}, sleep_fn=_noop_sleep,
                turning_point=spy,
            )
        from goal_store import GoalStore
        wired_done = GoalStore(root / "j-goals").get_goal("campaign-exec:stage-1")["state"] == "completed"
        cases.append(case(
            "run-threads-turning-point-into-driver",
            wired["invoked"] is True and spy.called and wired_done
            and any(child["goal_id"] == "campaign-exec:stage-1" for child in spy.snapshot["runnable_children"]),
            json.dumps({"called": spy.called, "done": wired_done, "dispatched": wired["driver"]["runs_dispatched"]}),
        ))

        # A judge that emits garbage is rejected and the deterministic path
        # still completes the goal.
        class GarbageJudge:
            def __call__(self, snapshot):
                return "not a decision"

        _seed(root / "g-goals", camp, source=source, base=base)
        with explicit_runstore_factory(fixture_glr):
            garbage_run = run(
                executor="fake", execute=True, goal_store_root=root / "g-goals", run_store_root=root / "g-runs",
                workspace_root=root / "g-ws", campaign=camp, source_repo=source, base_revision=base,
                max_cycles=30, factory_overrides={"fake": fake_executor_factory}, sleep_fn=_noop_sleep,
                turning_point=GarbageJudge(),
            )
        garbage_done = GoalStore(root / "g-goals").get_goal("campaign-exec:stage-1")["state"] == "completed"
        cases.append(case(
            "garbage-judge-falls-back-deterministic",
            garbage_run["driver"]["runs_dispatched"] == 1 and garbage_done,
            json.dumps({"done": garbage_done}),
        ))

        # judge_executor validation: unknown name and mutual exclusion are rejected.
        _seed(root / "u-goals", camp, source=source, base=base)
        unknown_raises, _ = _ok(lambda: run(
            executor="fake", execute=True, goal_store_root=root / "u-goals", run_store_root=root / "u-runs",
            workspace_root=root / "u-ws", campaign=camp, source_repo=source, base_revision=base,
            factory_overrides={"fake": fake_executor_factory}, judge_executor="nope",
        ))
        both_raises, _ = _ok(lambda: run(
            executor="fake", execute=True, goal_store_root=root / "b-goals", run_store_root=root / "b-runs",
            workspace_root=root / "b-ws", campaign=camp, source_repo=source, base_revision=base,
            factory_overrides={"fake": fake_executor_factory}, judge_executor="codex", turning_point=JudgeSpy(),
        ))
        cases.append(case("judge-executor-validation-rejected", unknown_raises and both_raises, f"unknown={unknown_raises} both={both_raises}"))

        # Contract models field resolves into run kwargs; bad shapes are rejected.
        contract = {
            "schema": project_binding.CONTRACT_SCHEMA,
            "project_id": "p-models",
            "campaign": camp,
            "source_repo": str(source),
            "base_revision": base,
            "runtime": {"goal_store": "runtime/goals", "run_store": "runtime/runs", "workspace_root": "runtime/ws"},
            "models": {
                "execute": "local",
                "execute_binding": {"runner": "codex", "base_url": "https://mock.example/v1", "model": "fixture-codex"},
                "judge": "codex",
                "judge_model": "fixture-judge",
            },
        }
        contract_path = root / "c" / "project_runtime_contract.json"
        contract_path.parent.mkdir()
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        kw = project_binding.resolve_project(contract_path)["run_kwargs"]
        mapping_ok = (
            kw.get("executor") == "local"
            and kw.get("executor_binding") == {"runner": "codex", "base_url": "https://mock.example/v1", "model": "fixture-codex"}
            and kw.get("judge_executor") == "codex"
            and kw.get("judge_model") == "fixture-judge"
        )

        def _bad_models(mutate) -> bool:
            bad = json.loads(contract_path.read_text(encoding="utf-8"))
            mutate(bad)
            bad_path = root / "c" / "bad_models.json"
            bad_path.write_text(json.dumps(bad), encoding="utf-8")
            try:
                project_binding.resolve_project(bad_path)
                return False
            except SystemExit:
                return True

        bad_missing = _bad_models(lambda c: c.__setitem__("models", {"judge": "codex"}))
        bad_type = _bad_models(lambda c: c.__setitem__("models", {"execute": 42}))
        bad_binding = _bad_models(lambda c: c["models"].__setitem__("execute_binding", {"runner": "codex", "model": "fixture-codex"}))
        cases.append(case(
            "contract-models-resolve-and-validated",
            mapping_ok and bad_missing and bad_type and bad_binding,
            json.dumps({"executor": kw.get("executor"), "binding": kw.get("executor_binding"), "judge": kw.get("judge_executor")}),
        ))

    # resolve_cli: PATH 外的標準安裝位置可解（systemd/cron 環境沒有 login PATH）。
    fake_home = Path(tempfile.mkdtemp())
    fake_bin = fake_home / ".local" / "bin"
    fake_bin.mkdir(parents=True)
    fake_cli = fake_bin / "fake-cli-x1"
    fake_cli.write_text("#!/bin/sh\n", encoding="utf-8")
    fake_cli.chmod(0o700)
    non_executable_cli = fake_bin / "fake-cli-non-executable-x2"
    non_executable_cli.write_text("#!/bin/sh\n", encoding="utf-8")
    non_executable_cli.chmod(0o600)
    old_home = os.environ.get("HOME")
    os.environ["HOME"] = str(fake_home)
    try:
        resolved = executors.resolve_cli("fake-cli-x1")
        non_executable_rejected = []
        for requested in (non_executable_cli.name, str(non_executable_cli)):
            try:
                executors.resolve_cli(requested)
            except FileNotFoundError:
                non_executable_rejected.append(requested)
        missing_raises = False
        try:
            executors.resolve_cli("definitely-missing-cli-x9")
        except FileNotFoundError:
            missing_raises = True
    finally:
        if old_home is not None:
            os.environ["HOME"] = old_home
    # which() 分支：PATH 上的 fake binary（不依賴 runner 有沒有裝真 CLI）。
    path_dir = Path(tempfile.mkdtemp())
    path_cli = path_dir / "fake-cli-y2"
    path_cli.write_text("#!/bin/sh\n", encoding="utf-8")
    path_cli.chmod(0o755)
    path_alias = path_dir / "fake-cli-y2-alias"
    path_alias.symlink_to(path_cli)
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{path_dir}:{old_path}"
    try:
        resolved_on_path = executors.resolve_cli("fake-cli-y2")
        resolved_on_path_alias = executors.resolve_cli("fake-cli-y2-alias")
    finally:
        os.environ["PATH"] = old_path
    cases.append(case(
        "resolve-cli-falls-back-to-standard-locations",
        resolved == str(fake_cli.resolve())
        and missing_raises
        and resolved_on_path == str(path_cli.resolve())
        and resolved_on_path_alias == str(path_cli.resolve()),
        json.dumps({
            "resolved": resolved,
            "missing_raises": missing_raises,
            "on_path": resolved_on_path,
            "on_path_alias": resolved_on_path_alias,
        }),
    ))
    cases.append(case(
        "resolve-cli-rejects-non-executable",
        non_executable_rejected == [non_executable_cli.name, str(non_executable_cli)],
        json.dumps({"rejected": non_executable_rejected}),
    ))

    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-judge-wiring",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/judge_wiring_canary.py",
            "spec": "docs/contracts/model-routing-v1.md",
        },
        "known_gaps_open": [
            "Real CLI judge invocations are a human live-smoke path; this canary proves wiring with fakes only.",
        ],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

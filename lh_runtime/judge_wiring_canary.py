#!/usr/bin/env python3
"""Committed M1/M2 smoke: model routing — declared judge command, CLI judge wrapper, run()/contract wiring."""
from __future__ import annotations

import json
import os
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


def _fake_cli(path: Path, *, mode: int) -> Path:
    """A runnable fake: a shell script with an execute bit, or a .cmd on Windows (PATHEXT)."""
    if sys.platform == "win32":
        path = path.with_name(path.name + ".cmd")
        path.write_text("@exit /b 0\r\n", encoding="utf-8")
        return path
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(mode)
    return path


def main() -> int:
    cases: list[dict[str, object]] = []

    # parse_decision: clean JSON, prose-wrapped JSON, and garbage.
    clean = tp.parse_decision('{"decision": "select:g1"}')
    noisy = tp.parse_decision('Here is my choice:\n{"decision": "human_required"}\nThanks.')
    grade = diff_grader.parse_grade('verdict: {"grade": "sensitive", "rationale": "fixture"}')
    garbage_raises, _ = _ok(lambda: tp.parse_decision("no json at all"))
    nodec_raises, _ = _ok(lambda: tp.parse_decision('{"other": 1}'))
    cases.append(case(
        "parse-decision-clean-noisy-garbage",
        clean == {"decision": "select:g1"}
        and noisy == {"decision": "human_required"}
        and grade == {"grade": "sensitive", "rationale": "fixture"}
        and garbage_raises
        and nodec_raises,
        json.dumps({
            "clean": clean,
            "noisy": noisy,
            "grade": grade,
        }),
    ))

    # A judge is a declared command: the prompt fills its {prompt} slot and a
    # pinned model its {model} slot.  No judge name is built in.
    declarations = executors.validate_executor_declarations({
        "reviewer": {"argv": [sys.executable, "-c", "pass", "{model}", "{prompt}"]},
        "plain-reviewer": {"argv": [sys.executable, "-c", "pass", "{prompt}"]},
    })
    pinned = executors.declared_command(declarations, "reviewer", "P", "m1")
    undeclared_raises, _ = _ok(lambda: executors.declared_command(declarations, "nope", "P"))
    unslotted_raises, _ = _ok(lambda: executors.declared_command(declarations, "plain-reviewer", "P", "m1"))
    cases.append(case(
        "judge-is-a-declared-command-with-slots",
        pinned == [sys.executable, "-c", "pass", "m1", "P"] and undeclared_raises and unslotted_raises,
        json.dumps({"pinned": pinned, "undeclared": undeclared_raises, "unslotted": unslotted_raises}),
    ))

    # The shared process seam never uses the caller's repository as the judge cwd.
    bounded_stdout = executors.run_bounded_judge([
        sys.executable,
        "-c",
        "import json, os; print(json.dumps({'decision': 'select:g1', 'cwd': os.getcwd()}))",
    ], name="fixture")
    bounded_payload = json.loads(bounded_stdout)
    cases.append(case(
        "bounded-judge-runs-outside-the-target-cwd",
        Path(str(bounded_payload.get("cwd", ""))).name.startswith("lh-judge-"),
        json.dumps({"cwd": bounded_payload.get("cwd")}),
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
            factory_overrides={"fake": fake_executor_factory}, judge_executor="reviewer", turning_point=JudgeSpy(),
            executor_declarations={"reviewer": {"argv": [sys.executable, "-c", "pass", "{prompt}"]}},
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
            "executors": {
                "coder": {"argv": [sys.executable, "-c", "pass", "{base_url}", "{model}", "{prompt}"]},
                "reviewer": {"argv": [sys.executable, "-c", "pass", "{model}", "{prompt}"]},
            },
            "models": {
                "execute": "coder",
                "execute_binding": {"runner": "coder", "base_url": "https://mock.example/v1", "model": "fixture-model"},
                "judge": "reviewer",
                "judge_model": "fixture-judge",
            },
        }
        contract_path = root / "c" / "project_runtime_contract.json"
        contract_path.parent.mkdir()
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        kw = project_binding.resolve_project(contract_path)["run_kwargs"]
        mapping_ok = (
            kw.get("executor") == "coder"
            and kw.get("executor_binding") == {"runner": "coder", "base_url": "https://mock.example/v1", "model": "fixture-model"}
            and kw.get("judge_executor") == "reviewer"
            and kw.get("judge_model") == "fixture-judge"
            and sorted(kw.get("executor_declarations") or {}) == ["coder", "reviewer"]
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

        bad_missing = _bad_models(lambda c: c.__setitem__("models", {"judge": "reviewer"}))
        bad_type = _bad_models(lambda c: c.__setitem__("models", {"execute": 42}))
        bad_binding = _bad_models(lambda c: c["models"].__setitem__("execute_binding", {"runner": "coder", "model": "fixture-model"}))
        bad_executor = _bad_models(lambda c: c["executors"].__setitem__("relative", {"argv": ["agent", "{prompt}"]}))
        cases.append(case(
            "contract-models-resolve-and-validated",
            mapping_ok and bad_missing and bad_type and bad_binding and bad_executor,
            json.dumps({"executor": kw.get("executor"), "binding": kw.get("executor_binding"), "judge": kw.get("judge_executor")}),
        ))

    # resolve_cli resolves an explicit path or a PATH entry; it never guesses a
    # per-user install location.
    fake_home = Path(tempfile.mkdtemp())
    fake_bin = fake_home / ".local" / "bin"
    fake_bin.mkdir(parents=True)
    fake_cli = _fake_cli(fake_bin / "fake-cli-x1", mode=0o700)
    non_executable_cli = fake_bin / "fake-cli-non-executable-x2"
    non_executable_cli.write_text("#!/bin/sh\n", encoding="utf-8")
    non_executable_cli.chmod(0o600)  # no execute bit; on Windows, no PATHEXT suffix
    old_home = os.environ.get("HOME")
    os.environ["HOME"] = str(fake_home)
    try:
        guessed = None
        try:
            guessed = executors.resolve_cli("fake-cli-x1")
        except FileNotFoundError:
            guessed = None
        resolved = executors.resolve_cli(str(fake_cli))
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
    path_cli = _fake_cli(path_dir / "fake-cli-y2", mode=0o755)
    path_alias = path_dir / ("fake-cli-y2-alias" + path_cli.suffix)
    alias_error = None
    try:
        path_alias.symlink_to(path_cli)
    except OSError as exc:  # Windows without symlink privilege: a failed case, never a skip
        alias_error = f"symlink_privilege_required: {exc}"
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{path_dir}{os.pathsep}{old_path}"
    try:
        resolved_on_path = executors.resolve_cli("fake-cli-y2")
        resolved_on_path_alias = executors.resolve_cli("fake-cli-y2-alias") if alias_error is None else None
    finally:
        os.environ["PATH"] = old_path
    cases.append(case(
        "resolve-cli-uses-explicit-paths-and-path-only",
        guessed is None
        and resolved == str(fake_cli.resolve())
        and missing_raises
        and resolved_on_path == str(path_cli.resolve())
        and resolved_on_path_alias == str(path_cli.resolve()),
        json.dumps({
            "guessed": guessed,
            "resolved": resolved,
            "missing_raises": missing_raises,
            "on_path": resolved_on_path,
            "on_path_alias": resolved_on_path_alias,
            "alias_error": alias_error,
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

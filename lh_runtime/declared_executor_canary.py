#!/usr/bin/env python3
"""Declared executors: the only way the engine runs a model, on any platform.

An executor is a declaration -- an absolute argv[0], a ``{prompt}`` slot, and
optional ``{model}``/``{base_url}`` slots -- never a built-in name.  These cases
prove the declaration rules, the slot filling, the neutral usage line, and one
real run through the explicit local-process fence in a disposable directory.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cli_agent_executor as executors  # noqa: E402
import execution_fence as fences  # noqa: E402
import goal_loop_run  # noqa: E402
import token_cost  # noqa: E402

CHECK_ID = "lh-declared-executor"
PY = sys.executable
# Writes the prompt into the workspace and reports usage on its last line.
WRITER = ("import json, pathlib, sys; pathlib.Path('declared.txt').write_text(sys.argv[1], encoding='utf-8'); "
          "print(json.dumps({'usage': {'input_tokens': 7, 'output_tokens': 3, 'model': sys.argv[2]}}))")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _refused(raw: Any) -> str | None:
    try:
        executors.validate_executor_declarations(raw)
    except ValueError as exc:
        return str(exc)
    return None


def declaration_rules_case() -> dict[str, Any]:
    refusals = {
        "relative_argv0": _refused({"coder": {"argv": ["agent", "{prompt}"]}}),
        "no_prompt_slot": _refused({"coder": {"argv": [PY, "-c", "pass"]}}),
        "two_prompt_slots": _refused({"coder": {"argv": [PY, "{prompt}", "{prompt}"]}}),
        "unknown_usage": _refused({"coder": {"argv": [PY, "{prompt}"], "usage": "vendor-log"}}),
        "unknown_field": _refused({"coder": {"argv": [PY, "{prompt}"], "env": {}}}),
        "bad_name": _refused({"Coder!": {"argv": [PY, "{prompt}"]}}),
    }
    accepted = executors.validate_executor_declarations({"coder": {"argv": [PY, "-c", "pass", "{prompt}"]}})
    ok = all(reason is not None for reason in refusals.values()) and accepted["coder"]["usage"] == "none"
    return case("declarations-are-closed-and-absolute", ok, {"refusals": refusals, "accepted": accepted})


def slot_case() -> dict[str, Any]:
    declaration = executors.validate_executor_declarations(
        {"coder": {"argv": [PY, "{model}", "{base_url}", "{prompt}"]}})["coder"]
    filled = executors.declared_argv(declaration, "P", model="m1", base_url="https://endpoint.invalid/v1")
    plain = executors.validate_executor_declarations({"coder": {"argv": [PY, "{prompt}"]}})["coder"]
    problems = {}
    for label, kwargs, target in (
        ("value_without_slot", {"model": "m1"}, plain),
        ("slot_without_value", {"model": None, "base_url": "https://endpoint.invalid/v1"}, declaration),
    ):
        try:
            executors.declared_argv(target, "P", **kwargs)
            problems[label] = "accepted"
        except ValueError as exc:
            problems[label] = f"refused: {exc}"
    ok = (filled == [PY, "m1", "https://endpoint.invalid/v1", "P"]
          and all(value.startswith("refused") for value in problems.values()))
    return case("slots-fill-together-or-refuse", ok, {"filled": filled, "problems": problems})


def usage_line_case() -> dict[str, Any]:
    measured = executors.parse_usage_line('noise\n{"usage": {"input_tokens": 5, "output_tokens": 2}}\n', model="m")
    garbage = executors.parse_usage_line("not json\n", model="m")
    negative = executors.parse_usage_line('{"usage": {"input_tokens": -1, "output_tokens": 2}}', model="m")
    ok = (measured.get("state") == token_cost.USAGE_MEASURED and measured["input_tokens"] == 5
          and garbage.get("state") == token_cost.USAGE_UNKNOWN and negative.get("state") == token_cost.USAGE_UNKNOWN)
    return case("usage-line-is-measured-or-unknown-never-invented", ok,
                {"measured": measured, "garbage": garbage.get("state"), "negative": negative.get("state")})


def resolution_case() -> dict[str, Any]:
    declarations = executors.validate_executor_declarations({"coder": {"argv": [PY, "-c", "pass", "{prompt}"]}})
    detail: dict[str, Any] = {}
    for label, action in (
        ("undeclared_executor", lambda: goal_loop_run.resolve_executor("other", execute=True, declarations=declarations)),
        ("no_declarations", lambda: goal_loop_run.resolve_executor("coder", execute=True)),
        ("undeclared_judge", lambda: executors.declared_command(declarations, "judge", "P")),
    ):
        try:
            action()
            detail[label] = "accepted"
        except ValueError as exc:
            detail[label] = f"refused: {exc}"
    dry = goal_loop_run.resolve_executor("coder", execute=False, declarations=declarations)
    ok = all(str(value).startswith("refused") for value in detail.values()) and dry is None
    return case("only-declared-names-resolve", ok, {**detail, "dry_run": dry})


def local_run_case(clone: Path) -> dict[str, Any]:
    declarations = executors.validate_executor_declarations(
        {"coder": {"argv": [PY, "-c", WRITER, "{prompt}", "{model}"], "usage": "lh-usage-line/v1"}})
    port = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "local-process"})
    agent = executors.make_declared_agent("coder", declarations, model="fixture-model", execution_fence_port=port)
    adapter_id, _version = fences.model_fence_identity(agent)
    binding = fences.build_attempt_binding(
        goal={"goal_id": "declared"}, run_id="run-declared", attempt=1, attempt_fence=1,
        base_revision="0" * 40, clone_root=clone, verifier_argv=[PY], adapter_id=adapter_id,
        adapter_version="v1", timeout_seconds=60)
    descriptor = port.prepare(binding)
    result = agent(clone, {"run_id": "run-declared", "attempt": 1, "goal": {"goal_id": "declared"},
                           "base_revision": "0" * 40, "execution_fence": descriptor})
    written = (clone / "declared.txt").read_text(encoding="utf-8") if (clone / "declared.txt").is_file() else ""
    usage = result.get("usage") or {}
    ok = (adapter_id == "declared-coder" and fences.model_requires_fence(agent)
          and written.strip() != "" and usage.get("state") == token_cost.USAGE_MEASURED
          and usage.get("input_tokens") == 7 and usage.get("model") == "fixture-model"
          and (result.get("execution") or {}).get("executor") == "coder")
    return case("declared-executor-runs-through-the-local-fence", ok,
                {"adapter": adapter_id, "prompt_written": bool(written.strip()), "usage": usage,
                 "execution": result.get("execution")})


def failure_case(clone: Path) -> dict[str, Any]:
    declarations = executors.validate_executor_declarations(
        {"coder": {"argv": [PY, "-c", "import sys; sys.exit(3)", "{prompt}"]}})
    port = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "local-process"})
    agent = executors.make_declared_agent("coder", declarations, execution_fence_port=port)
    binding = fences.build_attempt_binding(
        goal={"goal_id": "declared"}, run_id="run-declared", attempt=1, attempt_fence=1,
        base_revision="0" * 40, clone_root=clone, verifier_argv=[PY], adapter_id="declared-coder",
        adapter_version="v1", timeout_seconds=60)
    descriptor = port.prepare(binding)
    try:
        agent(clone, {"run_id": "run-declared", "attempt": 1, "goal": {"goal_id": "declared"},
                      "base_revision": "0" * 40, "execution_fence": descriptor})
        outcome = "returned"
    except RuntimeError as exc:
        outcome = f"raised: {exc}"
    return case("nonzero-exit-fails-the-attempt", outcome.startswith("raised: coder exited 3"), {"outcome": outcome})


def main() -> int:
    cases: list[dict[str, Any]] = []
    for build in (declaration_rules_case, slot_case, usage_line_case, resolution_case):
        try:
            cases.append(build())
        except Exception as exc:  # A crash is a failed exam, never a skipped one.
            cases.append(case(build.__name__.replace("_", "-"), False, f"{type(exc).__name__}: {exc}"))
    for build in (local_run_case, failure_case):
        with tempfile.TemporaryDirectory(prefix="lh-declared-") as raw:
            try:
                cases.append(build(Path(raw).resolve()))
            except Exception as exc:
                cases.append(case(build.__name__.replace("_", "-"), False, f"{type(exc).__name__}: {exc}"))
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Committed W3 smoke: lamp precheck skips the model when the lamp is already green.

A green-on-base lamp means the work was already done; the controller must
finish the run verified WITHOUT a model invocation, and the value reducer must
not read that precheck empty diff as lamp gaming (there is no agent yet).
A red-on-base lamp takes the classic model path unchanged.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
import value_reducer
from controller import LoopController
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner
from native_delivery_fixture import make_native_run


def git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def forbidden_model(workspace: Path, capsule: dict) -> dict:
    raise AssertionError("model must not be invoked when the lamp is already green")


def counting_model(workspace: Path, capsule: dict) -> dict:
    counting_model.calls += 1
    (workspace / "agent-change.txt").write_text(f"attempt {capsule['attempt']}\n", encoding="utf-8")
    return {"summary": "w3 model-path fixture"}


counting_model.calls = 0


def make_source(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    git("init", "-q", str(source))
    git("-C", str(source), "config", "user.email", "w3@example.invalid")
    git("-C", str(source), "config", "user.name", "W3 Canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    git("-C", str(source), "add", "baseline.txt")
    git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    return source, base


def _check(obligation_id: str, command_id: str, argv: list[str]) -> dict[str, object]:
    return {
        "id": obligation_id,
        "commands": [{
            "id": command_id,
            "argv": argv,
            "cwd": "${WORKTREE}",
            "expect_exit": 0,
            "timeout_seconds": 10,
        }],
        "required_receipts": ["executor"],
    }


def _verifier(flag: Path | None = None, *, fail: bool = False) -> list[str]:
    if fail:
        program = "raise SystemExit(1)"
    else:
        flag_check = ""
        if flag is not None:
            flag_check = f"; assert Path({str(flag)!r}).is_file()"
        program = (
            "from pathlib import Path; import subprocess, os; "
            "assert Path('baseline.txt').read_text(encoding='utf-8') == 'baseline\\n'"
            f"{flag_check}; "
            "assert Path(subprocess.check_output(['git','rev-parse','--show-toplevel'], text=True).strip()).resolve() == Path.cwd().resolve()"
        )
    return [sys.executable, "-B", "-c", program]


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source(root)
        store = RunStore(root / "run-store", command_runner=fixture_command_runner)
        controller = LoopController(store, root / "workspaces")
        goal = {"feature_contract": "w3 precheck fixture", "admission_envelope": {"requires_non_empty_diff": False}}
        checks = [_check("lamp-check", "lamp-diff-check", ["git", "diff", "--check"])]
        green = make_native_run(
            store, source, base, "w3-green", "lamp", checks, _verifier(), ["src/"], 2,
            goal=goal, run_id="run-w3-green",
        )
        prechecked = controller.tick(green["run_id"], holder="w3", model=forbidden_model, verifier_argv=_verifier())
        receipt = json.loads((store.root / prechecked["receipt_ref"]).read_text(encoding="utf-8"))
        verdict = value_reducer.verdict_for_run(store, green["run_id"])

        failing_checks = checks + [_check(
            "extra-red-obligation",
            "intentional-red",
            [sys.executable, "-B", "-c", "raise SystemExit(7)"],
        )]
        red_obligation = make_native_run(
            store, source, base, "w3-red-obligation", "lamp", failing_checks, _verifier(), ["src/"], 2,
            goal=goal, run_id="run-w3-red-obligation",
        )
        counting_model.calls = 0
        red_obligation_result = controller.tick(
            red_obligation["run_id"], holder="w3", model=forbidden_model, verifier_argv=_verifier()
        )
        red_obligation_model_calls = counting_model.calls

        red = make_native_run(
            store, source, base, "w3-red", "lamp", checks, _verifier(fail=True), ["src/"], 2,
            goal=goal, run_id="run-w3-red",
        )
        counting_model.calls = 0
        retried = controller.tick(red["run_id"], holder="w3", model=counting_model, verifier_argv=_verifier(fail=True))

        flip_flag = root / "flip-flag"
        flip_lamp = ["sh", "-c", f"test -f '{flip_flag}' && exit 0 || (touch '{flip_flag}'; exit 1)"]
        post = make_native_run(
            store, source, base, "w3-post-model", "lamp", checks, _verifier(flip_flag), ["src/"], 2,
            goal=goal, run_id="run-w3-post-model",
        )
        empty_after_model = controller.tick(
            post["run_id"], holder="w3", model=lambda ws, cap: {"summary": "changed nothing"},
            verifier_argv=flip_lamp)

        cases = [
            case("green-lamp-verifies-without-model",
                 prechecked["status"] == "verified" and prechecked.get("precheck") is True,
                 str(prechecked)),
            case("precheck-receipt-is-marked-and-self-describing",
                 receipt["verification"].get("precheck") is True
                 and receipt["verification"]["exit_code"] == 0
                 and receipt["provider"]["summary"] == "lamp precheck passed without model invocation"
                 and receipt["usage"]["state"] == "unknown",
                 json.dumps(receipt["verification"])[:200]),
            case("precheck-empty-diff-is-not-lamp-gaming",
                 verdict["verdict"] == "GREEN",
                 json.dumps(verdict["reasons"])),
            case("red-obligation-cannot-promote-precheck",
                 red_obligation_result["status"] == "retry_pending"
                 and red_obligation_model_calls == 0
                 and store.verify_delivery(red_obligation["run_id"], phase="final")["verdict"] == "RED",
                 json.dumps({
                     "result": red_obligation_result,
                     "calls": red_obligation_model_calls,
                     "delivery": store.verify_delivery(red_obligation["run_id"], phase="final"),
                 })),
            case("red-lamp-takes-the-model-path",
                 retried["status"] == "retry_pending" and counting_model.calls == 1
                 and "precheck" not in retried,
                 f"calls={counting_model.calls} status={retried['status']}"),
            case("post-model-empty-diff-still-red",
                 empty_after_model["status"] == "verified"
                 and "precheck" not in empty_after_model
                 and value_reducer.verdict_for_run(store, post["run_id"])["verdict"] == "RED",
                 json.dumps(value_reducer.verdict_for_run(store, post["run_id"])["reasons"])),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-lamp-precheck", "status": "pass" if not failures else "fail",
        "total": len(cases), "blocking_failures": failures,
        "verification": {"command": "python3 -B lh_runtime/lamp_precheck_canary.py"},
        "known_gaps_open": ["Precheck covers the sync local-verifier path only; the async external_verdict path has no local lamp to precheck."],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

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
import value_reducer
from controller import LoopController
from run_store import RunStore


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


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source(root)
        store = RunStore(root / "run-store")
        controller = LoopController(store, root / "workspaces")
        goal = {"feature_contract": "w3 precheck fixture", "admission_envelope": {"allowed_paths": ["src/"]}}

        green_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        prechecked = controller.tick(green_run, holder="w3", model=forbidden_model, verifier_argv=["git", "diff", "--check"])
        receipt = json.loads((store.root / prechecked["receipt_ref"]).read_text(encoding="utf-8"))
        verdict = value_reducer.verdict_for_run(store, green_run)

        red_run = store.create_run(goal=goal, source_repo=source, base_revision=base)
        counting_model.calls = 0
        retried = controller.tick(red_run, holder="w3", model=counting_model, verifier_argv=[sys.executable, "-c", "raise SystemExit(1)"])

        post_model = store.create_run(goal=goal, source_repo=source, base_revision=base)
        flip_flag = root / "flip-flag"
        flip_lamp = ["sh", "-c", f"test -f '{flip_flag}' && exit 0 || (touch '{flip_flag}'; exit 1)"]
        empty_after_model = controller.tick(
            post_model, holder="w3", model=lambda ws, cap: {"summary": "changed nothing"},
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
            case("red-lamp-takes-the-model-path",
                 retried["status"] == "retry_pending" and counting_model.calls == 1
                 and "precheck" not in retried,
                 f"calls={counting_model.calls} status={retried['status']}"),
            case("post-model-empty-diff-still-red",
                 empty_after_model["status"] == "verified"
                 and "precheck" not in empty_after_model
                 and value_reducer.verdict_for_run(store, post_model)["verdict"] == "RED",
                 json.dumps(value_reducer.verdict_for_run(store, post_model)["reasons"])),
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

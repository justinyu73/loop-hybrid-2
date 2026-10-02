#!/usr/bin/env python3
"""Provider-free smoke for the deterministic value reducer (报红 overlay)."""
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
from native_delivery_fixture import make_native_run
from run_store import RunStore
from value_reducer import aggregate, value_verdict, verdict_for_run


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _diff(path: str) -> str:
    return (
        f"diff --git a/{path} b/{path}\n"
        "new file mode 100644\nindex 0000000..111111\n--- /dev/null\n"
        f"+++ b/{path}\n@@ -0,0 +1 @@\n+hello\n"
    )


def _seed_run(store: RunStore, *, allowed: list[str], exit_code: int, diff_text: str) -> str:
    base = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    run_id = make_native_run(
        store, ROOT, base, f"value-{exit_code}-{allowed[0]}", "reducer",
        [{"id": "value-check", "commands": [{"id": "repo", "argv": ["test", "-d", ".git"], "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 10}], "required_receipts": ["executor"]}],
        ["git", "rev-parse", "HEAD"], allowed, 4,
        goal={"feature_contract": "x", "admission_envelope": {"allowed_paths": allowed}},
        run_id=None,
    )["run_id"]
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", diff_text)
    # W8-3: verifier stdout/stderr are artifact refs, not inline strings —
    # the integrity check resolves every recorded ref inside the run store.
    stdout_ref = store.write_artifact(run_id, ordinal, "verifier.stdout", "a")
    stderr_ref = store.write_artifact(run_id, ordinal, "verifier.stderr", "b")
    receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
               "diff": diff_ref["ref"],
               "verification": {"argv": ["true"], "exit_code": exit_code, "stdout": stdout_ref, "stderr": stderr_ref}}
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="human_required", receipt_ref=ref["ref"], receipt_digest=ref["digest"])
    return run_id


def main() -> int:
    green = value_verdict(exit_code=0, diff_text=_diff("src/hello.txt"), allowed_paths=["src/"])
    creep = value_verdict(exit_code=0, diff_text=_diff("secrets/key"), allowed_paths=["src/"])
    empty = value_verdict(exit_code=0, diff_text="", allowed_paths=["src/"])
    lamp_fail = value_verdict(exit_code=1, diff_text=_diff("src/hello.txt"), allowed_paths=["src/"])
    missing = value_verdict(exit_code=None, diff_text=None, allowed_paths=["src/"])

    with tempfile.TemporaryDirectory() as raw:
        store = RunStore(Path(raw) / "runs")
        green_run = _seed_run(store, allowed=["src/"], exit_code=0, diff_text=_diff("src/hello.txt"))
        red_run = _seed_run(store, allowed=["src/"], exit_code=0, diff_text=_diff("secrets/key"))
        rollup = aggregate(store)
        green_run_verdict = verdict_for_run(store, green_run)

    cases = [
        case("green-when-lamp-passes-in-scope-nonempty", green["verdict"] == "GREEN" and not green["reasons"], str(green)),
        case("red-on-scope-creep-names-the-file", creep["verdict"] == "RED" and any("secrets/key" in r for r in creep["reasons"]), str(creep["reasons"])),
        case("red-on-empty-diff-lamp-gaming", empty["verdict"] == "RED" and any("empty diff" in r for r in empty["reasons"]), str(empty["reasons"])),
        case("red-on-lamp-fail", lamp_fail["verdict"] == "RED", str(lamp_fail["reasons"])),
        case("red-on-missing-evidence-unknown-not-pass", missing["verdict"] == "RED" and any("unknown != pass" in r for r in missing["reasons"]), str(missing["reasons"])),
        case("end-to-end-run-verdict-and-rollup", green_run_verdict["verdict"] == "GREEN" and rollup["green"] == 1 and rollup["red"] == 1 and rollup["red_runs"][0]["run_id"] == red_run, str(rollup)),
    ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-value-reducer",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "known_gaps_open": [
            "finding-only: the value verdict does not gate loop advance (LH execution model unchanged)",
            "independent-falsifier (second-model refutation) is a later slice; this is the deterministic layer",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

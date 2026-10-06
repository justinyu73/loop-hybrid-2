#!/usr/bin/env python3
"""Clone push boundary: an executor in a disposable clone cannot move the source repository.

The disposable clone is created from the local source checkout, so before
this boundary its ``origin`` pointed straight at the source repository and an
executor could ``git push`` a branch into it.  Each case runs the real
controller on the synchronous delivery path; the model is a function that
tries to push from inside the workspace it was given.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

from controller import LoopController  # noqa: E402
from native_delivery_fixture import make_native_run  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-clone-push-boundary"
CHECK = "from pathlib import Path; assert Path('src/out.txt').read_text(encoding='utf-8').strip() == 'done'"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def git(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", check=check)


def refs(source: Path) -> str:
    return git("for-each-ref", "--format=%(refname) %(objectname)", cwd=source).stdout


def run_with(root: Path, attempt: Callable[[Path], dict[str, Any]]) -> dict[str, Any]:
    root.mkdir(parents=True)
    source = root / "source"
    source.mkdir()
    for args in (("init", "-q"), ("config", "user.email", "boundary@example.invalid"),
                 ("config", "user.name", "Boundary Canary")):
        git(*args, cwd=source)
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    git("add", "baseline.txt", cwd=source)
    git("commit", "-qm", "baseline", cwd=source)
    base = git("rev-parse", "HEAD", cwd=source).stdout.strip()
    runs = RunStore(root / "runs", command_runner=fixture_command_runner)
    checks = [{"id": "out", "commands": [{"id": "out", "argv": [sys.executable, "-B", "-c", CHECK],
                                          "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}],
               "required_receipts": ["executor"]}]
    verifier = [sys.executable, "-B", "-c", CHECK]
    bundle = make_native_run(runs, source, base, root.name, "boundary", checks, verifier, ["src/"], 1,
                             run_id=root.name)
    before = refs(source)
    observed: dict[str, Any] = {}

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        (workspace / "src").mkdir(exist_ok=True)
        (workspace / "src" / "out.txt").write_text("done\n", encoding="utf-8")
        git("config", "user.email", "agent@example.invalid", cwd=workspace)
        git("config", "user.name", "Agent", cwd=workspace)
        observed.update(attempt(workspace))
        marker = workspace / ".git" / "lh-disposable-workspace.json"
        observed["marker"] = json.loads(marker.read_text(encoding="utf-8")) if marker.is_file() else None
        return {"summary": "boundary probe", "usage": {"state": "unknown"}}

    result = LoopController(runs, root / "workspaces").tick(bundle["run_id"], holder="boundary", model=model,
                                                            verifier_argv=verifier)
    receipt = json.loads(runs.read_artifact(bundle["run_id"], 1, "receipt.json") or "{}")
    return {"state": runs.get_run(bundle["run_id"])["state"], "result_status": result.get("status"),
            "source_refs_unchanged": refs(source) == before, "observed": observed,
            "receipt_text": json.dumps(receipt, default=str), "source": source}


def commit_candidate(workspace: Path) -> None:
    git("add", "-A", cwd=workspace)
    git("commit", "-qm", "candidate", cwd=workspace)


def c1_origin(root: Path) -> dict[str, Any]:
    def attempt(workspace: Path) -> dict[str, Any]:
        commit_candidate(workspace)
        pushed = git("push", "origin", "HEAD:refs/heads/from-origin", cwd=workspace, check=False)
        return {"push_exit": pushed.returncode}
    run = run_with(root, attempt)
    ok = run["observed"].get("push_exit", 0) != 0 and run["source_refs_unchanged"]
    return case("pushing-to-origin-is-refused", ok,
                {"push_exit": run["observed"].get("push_exit"), "source_refs_unchanged": run["source_refs_unchanged"]})


def c2_new_remote(root: Path) -> dict[str, Any]:
    source_holder: dict[str, Path] = {}

    def attempt(workspace: Path) -> dict[str, Any]:
        commit_candidate(workspace)
        source = workspace.parents[2] / "source"
        source_holder["source"] = source
        git("remote", "add", "sneaky", str(source), cwd=workspace)
        pushed = git("push", "sneaky", "HEAD:refs/heads/from-new-remote", cwd=workspace, check=False)
        return {"push_exit": pushed.returncode}
    run = run_with(root, attempt)
    ok = run["observed"].get("push_exit", 0) != 0 and run["source_refs_unchanged"]
    return case("a-remote-added-by-the-executor-is-refused-too", ok,
                {"push_exit": run["observed"].get("push_exit"), "source_refs_unchanged": run["source_refs_unchanged"]})


def c3_bypass(root: Path) -> dict[str, Any]:
    def attempt(workspace: Path) -> dict[str, Any]:
        commit_candidate(workspace)
        source = workspace.parents[2] / "source"
        pushed = git("push", "--no-verify", str(source), "HEAD:refs/heads/bypassed", cwd=workspace, check=False)
        return {"push_exit": pushed.returncode}
    run = run_with(root, attempt)
    moved = not run["source_refs_unchanged"]
    ok = run["state"] != "verified" and (not moved or "source_refs_mutated" in run["receipt_text"])
    return case("a-bypassed-push-cannot-verify", ok,
                {"push_exit": run["observed"].get("push_exit"), "source_refs_moved": moved, "state": run["state"],
                 "receipt_names_reason": "source_refs_mutated" in run["receipt_text"]})


def c4_normal(root: Path) -> dict[str, Any]:
    run = run_with(root, lambda workspace: {})
    marker = run["observed"].get("marker") or {}
    ok = run["state"] == "verified" and run["source_refs_unchanged"] and marker.get("push_sealed") is True
    return case("a-normal-run-is-unaffected-and-marked-sealed", ok,
                {"state": run["state"], "push_sealed": marker.get("push_sealed")})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-clone-push-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("pushing-to-origin-is-refused", lambda: c1_origin(root / "c1")),
            guarded("a-remote-added-by-the-executor-is-refused-too", lambda: c2_new_remote(root / "c2")),
            guarded("a-bypassed-push-cannot-verify", lambda: c3_bypass(root / "c3")),
            guarded("a-normal-run-is-unaffected-and-marked-sealed", lambda: c4_normal(root / "c4")),
        ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures],
                      "known_gaps_open": ["without a containment backend an executor can still write outside the "
                                          "clone; the refs comparison makes a moved source branch fail the attempt"]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

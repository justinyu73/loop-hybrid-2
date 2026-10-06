#!/usr/bin/env python3
"""Retry verifier canary: a separate, read-only implementation checks executor retries.

A real ``WorkUnitStore`` and ``SuccessorDispatchConsumer`` drive a declared
command executor that fails and then succeeds (or keeps failing).  The
verifier then reads the store through a read-only connection and the executor's
digest-bound receipts, and must agree with what happened -- and refuse when
the evidence is tampered with or incomplete.
"""
from __future__ import annotations

import ast
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
TOOL = HERE / "verify_retry.py"
sys.path.insert(0, str(REPO / "lh_runtime"))
import candidate_review_work_unit_canary as fixture  # noqa: E402

CHECK_ID = "retry-verifier"

FLAKY = """import pathlib, sys
worktree, attempt, mode = pathlib.Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
if mode == "always-fail" or (mode == "fail-once" and attempt == 1):
    sys.stderr.write("controlled failure on attempt %d" % attempt)
    sys.exit(17)
target = worktree / "src" / "m1.py"
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text("def double(n):\\n    return n * 2\\n", encoding="utf-8")
"""


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


class FlakyExecutor(fixture.DeclaredScriptExecutor):
    def __init__(self, root: Path, script: Path, mode: str, log: Path):
        super().__init__(root, script=script, mode=mode, log=log)

    def _command(self, request, packet):
        self.log.open("a", encoding="utf-8").write(json.dumps({"attempt": request["attempt"]}) + "\n")
        return [sys.executable, "-B", str(self.script), str(request["worktree"]), str(request["attempt"]), self.mode]


def scenario(root: Path, mode: str, steps: int = 4) -> dict[str, Any]:
    harness = fixture.Harness(root, executor_mode="right", policy=False, max_attempts=3)
    script = fixture.write(root / "bin" / "flaky.py", FLAKY).resolve()
    harness.consumer.executor = FlakyExecutor(root, script, mode, root / "launches.jsonl")
    results = []
    for _ in range(steps):
        result = harness.consume()
        results.append({k: result.get(k) for k in ("executor_status", "retry_scheduled", "retry_exhausted", "attempt")})
        if result.get("executor_status") == "accepted" or result.get("retry_exhausted"):
            break
    return {"executor_root": root / "executor", "queue_db": harness.store.root / "work-units.sqlite3",
            "dispatch_key": harness.envelope["dispatch_key"], "steps": results}


def verify(run: dict[str, Any], expected: str) -> tuple[int, dict[str, Any]]:
    if not TOOL.is_file():
        raise FileNotFoundError("gate-pack/retry_verifier/verify_retry.py is not provided")
    done = subprocess.run([sys.executable, "-B", str(TOOL), "--executor-root", str(run["executor_root"]),
                           "--queue-db", str(run["queue_db"]), "--dispatch-key", run["dispatch_key"],
                           "--expected", expected], capture_output=True, text=True, encoding="utf-8", timeout=120)
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError:
        return done.returncode, {"unparsed": done.stdout[-300:], "stderr": done.stderr[-300:]}


def _evidence(run: dict[str, Any], suffix: str) -> list[Path]:
    return sorted((run["executor_root"] / "successor-executor").glob(f"*{suffix}"))


def _digests(run: dict[str, Any]) -> dict[str, str]:
    paths = [run["queue_db"], *sorted((run["executor_root"] / "successor-executor").glob("*.json"))]
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def c1_c5(root: Path) -> list[dict[str, Any]]:
    run = scenario(root, "fail-once")
    before = _digests(run)
    code, result = verify(run, "retry_success")
    after = _digests(run)
    first = case("a-retry-that-succeeded-verifies", code == 0 and result.get("status") == "PASS"
                 and result.get("launches") == 2, {"steps": run["steps"], "result": result})
    fifth = case("the-verifier-never-writes-the-evidence", before == after and bool(before),
                 {"files": len(before), "unchanged": before == after})
    return [first, fifth]


def c2_exhausted(root: Path) -> dict[str, Any]:
    run = scenario(root, "always-fail")
    exhausted = verify(run, "exhausted")
    as_success = verify(run, "retry_success")
    ok = exhausted[0] == 0 and exhausted[1].get("status") == "PASS" and as_success[0] != 0
    return case("an-exhausted-chain-verifies-only-as-exhausted", ok,
                {"steps": run["steps"], "exhausted": exhausted[1], "as_success": as_success[1].get("reason")})


def c3_tampered(root: Path) -> dict[str, Any]:
    run = scenario(root, "fail-once")
    failure = _evidence(run, ".failure-receipt.json")[0]
    value = json.loads(failure.read_text(encoding="utf-8"))
    value["exit_code"] = 0
    failure.write_text(json.dumps(value), encoding="utf-8")
    code, result = verify(run, "retry_success")
    return case("a-tampered-failure-receipt-fails",
                code != 0 and "digest_mismatch" in str(result.get("reason")), result)


def c4_missing(root: Path) -> dict[str, Any]:
    run = scenario(root, "fail-once")
    invocation = _evidence(run, ".invocation.json")[0]
    invocation.unlink()
    code, result = verify(run, "retry_success")
    return case("a-missing-invocation-fails", code != 0 and result.get("status") == "FAIL", result)


def c6_duplicate(root: Path) -> dict[str, Any]:
    run = scenario(root, "fail-once")
    invocation = _evidence(run, ".invocation.json")[0]
    invocation.with_name("copy-" + invocation.name).write_bytes(invocation.read_bytes())
    code, result = verify(run, "retry_success")
    return case("a-duplicated-launch-record-fails",
                code != 0 and result.get("reason") == "duplicate_invocation_attempt", result)


def c7_independent() -> dict[str, Any]:
    if not TOOL.is_file():
        return case("the-verifier-is-an-independent-implementation", False, "verify_retry.py is not provided")
    tree = ast.parse(TOOL.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = {name for name in imported if name.split(".")[0] in {
        "lh_runtime", "successor_executor", "work_unit_store", "parallel_scheduler", "work_unit_completion"}}
    return case("the-verifier-is-an-independent-implementation", not forbidden,
                {"imports": sorted(imported), "forbidden": sorted(forbidden)})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="retry-verifier-") as raw:
        root = Path(raw).resolve()
        results = []
        try:
            results.extend(c1_c5(root / "c1"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:300]}"
            results.extend([case("a-retry-that-succeeded-verifies", False, detail),
                            case("the-verifier-never-writes-the-evidence", False, detail)])
        results.extend([
            guarded("an-exhausted-chain-verifies-only-as-exhausted", lambda: c2_exhausted(root / "c2")),
            guarded("a-tampered-failure-receipt-fails", lambda: c3_tampered(root / "c3")),
            guarded("a-missing-invocation-fails", lambda: c4_missing(root / "c4")),
            guarded("a-duplicated-launch-record-fails", lambda: c6_duplicate(root / "c6")),
            guarded("the-verifier-is-an-independent-implementation", c7_independent),
        ])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

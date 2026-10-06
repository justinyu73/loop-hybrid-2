#!/usr/bin/env python3
"""Known defects found during extraction, each held by its own regression case.

D1  The independent verifier copied the candidate into its snapshot but split
    ``git ls-files -z`` on a literal backslash-zero instead of NUL, so a
    tracked file the candidate deleted stayed in the snapshot and a correct
    deletion could never verify.
D2  A candidate-review reference was refused when a path component was a
    symlink, but a Windows directory junction passed the check.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import delivery_contract as engine  # noqa: E402
from controller import LoopController  # noqa: E402
from native_delivery_fixture import make_native_run  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-known-defects"
CHECK = ("from pathlib import Path; assert not Path('src/old.txt').exists(); "
         "assert Path('src/new.txt').read_text(encoding='utf-8').strip() == 'new'")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
                          encoding="utf-8").stdout.strip()


def d1_deletion(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True)
    source = root / "source"
    source.mkdir()
    for args in (("init", "-q"), ("config", "user.email", "d1@example.invalid"), ("config", "user.name", "D1")):
        git(*args, cwd=source)
    (source / "src").mkdir()
    (source / "src" / "old.txt").write_text("old\n", encoding="utf-8")
    git("add", "-A", cwd=source)
    git("commit", "-qm", "baseline", cwd=source)
    base = git("rev-parse", "HEAD", cwd=source)
    runs = RunStore(root / "runs", command_runner=fixture_command_runner)
    checks = [{"id": "replace", "commands": [{"id": "replace", "argv": [sys.executable, "-B", "-c", CHECK],
                                              "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}],
               "required_receipts": ["executor"]}]
    verifier = [sys.executable, "-B", "-c", CHECK]
    bundle = make_native_run(runs, source, base, "d1", "deletion", checks, verifier, ["src/"], 1, run_id="d1")

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        (workspace / "src" / "old.txt").unlink()
        (workspace / "src" / "new.txt").write_text("new\n", encoding="utf-8")
        return {"summary": "replace a tracked file", "usage": {"state": "unknown"}}

    result = LoopController(runs, root / "workspaces").tick("d1", holder="d1", model=model, verifier_argv=verifier)
    final = runs.verify_delivery("d1", phase="final")
    state = runs.get_run("d1")["state"]
    return case("a-deleted-tracked-file-can-verify", state == "verified" and final.get("verdict") == "GREEN",
                {"state": state, "tick": result.get("status"), "final": final.get("reason")})


def _junction(target: Path, link: Path) -> str:
    """Create a directory junction (Windows) or a directory symlink (elsewhere)."""
    if os.name == "nt":
        import _winapi
        _winapi.CreateJunction(str(target), str(link))
        return "junction"
    link.symlink_to(target, target_is_directory=True)
    return "symlink"


def d2_junction(root: Path) -> dict[str, Any]:
    real = root / "real"
    real.mkdir(parents=True)
    spec = real / "requirements.txt"
    spec.write_text("double(n) = 2*n\n", encoding="utf-8")
    kind = _junction(real, root / "linked")
    through = root / "linked" / "requirements.txt"
    digest = "sha256:" + __import__("hashlib").sha256(spec.read_bytes()).hexdigest()
    policy = {"schema": "lh-candidate-review-contract/v2",
              "spec_ref": {"path": str(through), "content_digest": digest},
              "requirements": [{"id": "double", "text": "double(n) = 2*n"}], "caller_context_refs": []}
    direct = {**policy, "spec_ref": {"path": str(spec), "content_digest": digest}}
    try:
        engine.validate_candidate_review_policy(policy)
        refused = None
    except engine.DeliveryUnitError as exc:
        refused = exc.reason
    engine.validate_candidate_review_policy(direct)
    return case("a-reference-through-a-junction-is-refused", refused == "candidate_review_ref_unreadable",
                {"link_kind": kind, "refused": refused})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-known-defects-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("a-deleted-tracked-file-can-verify", lambda: d1_deletion(root / "d1")),
            guarded("a-reference-through-a-junction-is-refused", lambda: d2_junction(root / "d2")),
        ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures],
                      "known_gaps_open": ["directory junctions exist only on Windows; elsewhere the same case "
                                          "runs with a directory symlink"]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

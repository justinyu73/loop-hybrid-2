#!/usr/bin/env python3
"""Retention: drop only what is superseded and unreferenced; planning is the default.

A store accumulates engine-owned scratch: verifier snapshots, launch scratch,
check diagnostics, sealed review copies.  Retention may remove an entry only
when nothing settled refers to it, it is older than the grace period, and it
lives inside the store root.  Everything else is kept, with the reason.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import candidate_review_canary as review_fixture  # noqa: E402
import effect_guard_canary as runs_fixture  # noqa: E402

CHECK_ID = "lh-retention"
TOOL = HERE / "retention.py"
DAY = 24 * 3600


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def retention(root: Path, *, apply: bool = False, grace_hours: float = 24, now: float | None = None) -> tuple[int, dict[str, Any]]:
    if not TOOL.is_file():
        raise FileNotFoundError("lh_runtime/retention.py is not provided")
    args = [sys.executable, "-B", str(TOOL), "--root", str(root), "--grace-hours", str(grace_hours)]
    if apply:
        args.append("--apply")
    if now is not None:
        args += ["--now", str(now)]
    done = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", timeout=120)
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError:
        return done.returncode, {"unparsed": done.stdout[-300:], "stderr": done.stderr[-300:]}


def make_entry(path: Path, *, age_days: float, directory: bool = True, clone: bool = False,
               repository: bool = False) -> Path:
    if directory:
        path.mkdir(parents=True)
        (path / "data.txt").write_text("scratch\n", encoding="utf-8")
        if clone:  # the verifier's disposable clone: a git repo one level down, objects read-only
            obj = path / "source" / ".git" / "objects" / "ab" / "cdef"
            obj.parent.mkdir(parents=True)
            obj.write_bytes(b"object")
            obj.chmod(0o444)
        if repository:  # a repository root placed directly in a category is never scratch
            (path / ".git").mkdir()
            (path / ".git" / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("scratch\n", encoding="utf-8")
    stamp = time.time() - age_days * DAY
    for item in [path, *path.rglob("*")] if directory else [path]:
        os.utime(item, (stamp, stamp))
    return path


def store(root: Path) -> dict[str, Path]:
    root.mkdir(parents=True)
    entries = {
        "old_snapshot": make_entry(root / "completion-snapshots" / "verify-old", age_days=5, clone=True),
        "fresh_snapshot": make_entry(root / "completion-snapshots" / "verify-fresh", age_days=0),
        "repository": make_entry(root / "completion-snapshots" / "verify-repo", age_days=5, repository=True),
        "old_launch": make_entry(root / "trusted-launches" / "phase-old", age_days=5),
        "newest_launch": make_entry(root / "trusted-launches" / "phase-new", age_days=2),
        "referenced_diagnostic": make_entry(root / "completion-check-diagnostics" / "failed-kept", age_days=5),
        "unreferenced_diagnostic": make_entry(root / "completion-check-diagnostics" / "failed-gone", age_days=6),
    }
    receipt = root / "artifacts" / "run-1" / "1" / "receipt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"checks": [{"stdout_ref": str(entries["referenced_diagnostic"] / "data.txt")}]}),
                       encoding="utf-8")
    return entries


def _paths(rows: list[dict[str, Any]]) -> set[str]:
    return {Path(row["path"]).name for row in rows}


def c1_plan_only(root: Path) -> dict[str, Any]:
    entries = store(root)
    code, plan = retention(root)
    removable = _paths(plan.get("remove", []))
    ok = code == 0 and plan.get("applied") is False and {"verify-old", "phase-old", "failed-gone"} <= removable \
        and all(path.exists() for path in entries.values())
    return case("planning-deletes-nothing", ok, {"remove": sorted(removable), "applied": plan.get("applied")})


def c2_referenced(root: Path) -> dict[str, Any]:
    entries = store(root)
    code, result = retention(root, apply=True)
    kept = {Path(row["path"]).name: row.get("reason") for row in result.get("keep", [])}
    ok = entries["referenced_diagnostic"].exists() and kept.get("failed-kept") == "referenced"
    return case("referenced-evidence-is-never-removed", ok, {"kept": kept})


def c3_c4_removed_and_grace(root: Path) -> list[dict[str, Any]]:
    entries = store(root)
    code, result = retention(root, apply=True)
    removed = not entries["old_snapshot"].exists() and not entries["old_launch"].exists() \
        and not entries["unreferenced_diagnostic"].exists()
    kept = {Path(row["path"]).name: row.get("reason") for row in result.get("keep", [])}
    return [
        case("superseded-unreferenced-and-old-is-removed", code == 0 and removed and result.get("applied") is True,
             {"removed": sorted(_paths(result.get("remove", [])))}),
        case("within-the-grace-period-is-kept",
             entries["fresh_snapshot"].exists() and kept.get("verify-fresh") == "within_grace", {"kept": kept}),
        case("the-newest-entry-of-each-kind-is-kept",
             entries["newest_launch"].exists() and kept.get("phase-new") == "newest_in_category", {"kept": kept}),
        case("a-repository-root-is-never-scratch",
             (entries["repository"] / ".git" / "HEAD").is_file() and kept.get("verify-repo") == "git_repository",
             {"kept": kept}),
    ]


def _link_outside(target: Path, link: Path) -> str:
    if os.name == "nt":
        import _winapi
        _winapi.CreateJunction(str(target), str(link))
        return "junction"
    link.symlink_to(target, target_is_directory=True)
    return "symlink"


def c5_outside(root: Path) -> dict[str, Any]:
    outside = root / "outside"
    outside.mkdir(parents=True)
    precious = outside / "precious.txt"
    precious.write_text("keep me\n", encoding="utf-8")
    store_root = root / "store"
    store(store_root)
    kind = _link_outside(outside, store_root / "completion-snapshots" / "verify-link")
    code, result = retention(store_root, apply=True, now=time.time() + 30 * DAY)
    kept = {Path(row["path"]).name: row.get("reason") for row in result.get("keep", [])}
    ok = code == 0 and precious.is_file() and outside.is_dir() and kept.get("verify-link") == "link_not_followed"
    return case("nothing-outside-the-store-root-is-touched", ok,
                {"link_kind": kind, "precious_exists": precious.is_file(), "kept": kept})


def c6_delivery(root: Path) -> dict[str, Any]:
    policy = review_fixture.policy(root / "policy")
    run = runs_fixture.Run(root / "run", files={"src/m1.py": review_fixture.CANDIDATE}, allowed=["src/"], policy=policy)
    before = run.runs.verify_delivery(run.run_id, phase="final").get("verdict")
    reviews = list((run.runs.root / "candidate-reviews").glob("*.json"))
    stamp = time.time() - 10 * DAY
    for path in [*run.runs.root.rglob("*")]:
        os.utime(path, (stamp, stamp))
    code, result = retention(run.runs.root, apply=True)
    after = run.runs.verify_delivery(run.run_id, phase="final").get("verdict")
    ok = before == "GREEN" and after == "GREEN" and reviews and all(path.exists() for path in reviews)
    return case("delivery-still-verifies-after-retention", ok,
                {"before": before, "after": after, "reviews": len(reviews), "apply_exit": code})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-retention-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("planning-deletes-nothing", lambda: c1_plan_only(root / "c1")),
            guarded("referenced-evidence-is-never-removed", lambda: c2_referenced(root / "c2")),
        ]
        try:
            results.extend(c3_c4_removed_and_grace(root / "c3"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:300]}"
            results.extend([case(name, False, detail) for name in (
                "superseded-unreferenced-and-old-is-removed", "within-the-grace-period-is-kept",
                "the-newest-entry-of-each-kind-is-kept", "a-repository-root-is-never-scratch")])
        results.extend([
            guarded("nothing-outside-the-store-root-is-touched", lambda: c5_outside(root / "c5")),
            guarded("delivery-still-verifies-after-retention", lambda: c6_delivery(root / "c6")),
        ])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

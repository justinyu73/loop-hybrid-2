#!/usr/bin/env python3
"""Prefix-aware ceremony grader canary (host+LH stage-3 C-merge shape).

Three fixtures, all synthetic git repos:
1. subdir-green: grader run from a `loop-hybrid/` subdirectory of a merged
   tree still reads the base units file (`ref:./path`), so a grandfathered
   ceremony-done unit stays grandfathered instead of reading as newly washed.
   (`git log --name-only` needs no fix: `is_value` substring-matches, so
   prefixed paths still hit the value surface.)
2. merge-commit-head2: at the merge commit itself HEAD~1 (the other lineage)
   lacks the units file; the HEAD^2 fallback compares against the carried
   parent, so an unwashed merge is green...
3. washed-merge-red: ...and a merge commit that also flips a unit to done
   with no value-surface backing stays RED (the fallback is not a waiver).
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ceremony_grader as grader  # noqa: E402


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True, text=True, check=True,
        env={"GIT_AUTHOR_NAME": "c", "GIT_AUTHOR_EMAIL": "c@x", "GIT_COMMITTER_NAME": "c",
             "GIT_COMMITTER_EMAIL": "c@x", "GIT_CONFIG_GLOBAL": "/dev/null", "PATH": "/usr/bin:/bin"},
    )
    return proc.stdout.strip()


def _units(units_path: Path, status: dict[str, str]) -> None:
    units_path.parent.mkdir(parents=True, exist_ok=True)
    units_path.write_text(json.dumps(
        {"units": [{"id": uid, "name": name, "status": st}
                   for uid, (name, st) in status.items()]},
        indent=2), encoding="utf-8")


CFG = {"units": "gate-pack/units.json", "value_surface": ("src/",),
       "match_threshold": 0.5, "scan_depth": 20}


def main() -> int:
    cases = []
    with tempfile.TemporaryDirectory(prefix="lh-grader-canary-") as raw:
        root = Path(raw)
        # -- other lineage (plays external host main)
        host = root / "host"; host.mkdir()
        _git(host, "init", "-q", "-b", "main")
        (host / "README.md").write_text("host\n", encoding="utf-8")
        _git(host, "add", "-A"); _git(host, "commit", "-qm", "host root")
        # -- LH lineage under prefix, with a value-backed done unit
        lh = root / "lh"; lh.mkdir()
        _git(lh, "init", "-q", "-b", "master")
        sub = lh / "loop-hybrid"
        # U2 is legacy ceremony-done: its name matches no commit subject, so
        # only the grandfather (base comparison) keeps it out of violations.
        _units(sub / "gate-pack" / "units.json",
               {"U1": ("ship parser fix", "not-done"),
                "U2": ("polish ceremony deck", "done")})
        _git(lh, "add", "-A"); _git(lh, "commit", "-qm", "seed units")
        (sub / "src").mkdir(parents=True)
        (sub / "src" / "parser.py").write_text("ok\n", encoding="utf-8")
        _units(sub / "gate-pack" / "units.json",
               {"U1": ("ship parser fix", "done"),
                "U2": ("polish ceremony deck", "done")})
        _git(lh, "add", "-A"); _git(lh, "commit", "-qm", "ship parser fix")
        # -- merge unrelated histories (the C shape)
        _git(host, "remote", "add", "lh", str(lh))
        _git(host, "fetch", "-q", "lh", "master")
        _git(host, "merge", "-q", "--allow-unrelated-histories", "-m", "merge lh", "FETCH_HEAD")
        subdir = host / "loop-hybrid"

        # 1: at the merge commit, HEAD~1 lacks units -> HEAD^2 fallback, unwashed -> green
        gate = grader.ceremony_done_gate(subdir, CFG)
        cases.append(("merge-commit-uses-the-carried-parent-and-stays-green",
                      gate["violations"] == [], gate))

        # 2: one commit past the merge, the grandfather still holds via ./
        (subdir / "src" / "extra.py").write_text("more\n", encoding="utf-8")
        _git(host, "add", "-A"); _git(host, "commit", "-qm", "post-merge value commit")
        gate = grader.ceremony_done_gate(subdir, CFG)
        cases.append(("post-merge-commit-keeps-the-grandfather-via-cwd-relative-base",
                      gate["violations"] == [], gate))
        _git(host, "reset", "-q", "--hard", "HEAD~1")

        # 3: a merge that also washes a unit must stay red
        _git(host, "reset", "-q", "--hard", "HEAD~1")
        _git(host, "merge", "-q", "--no-commit", "--allow-unrelated-histories", "FETCH_HEAD")
        _units(subdir / "gate-pack" / "units.json",
               {"U1": ("ship parser fix", "done"),
                "U2": ("polish ceremony deck", "done"),
                "U9": ("stage a demo banner", "done")})
        _git(host, "add", "-A"); _git(host, "commit", "-qm", "merge lh + wash")
        gate = grader.ceremony_done_gate(subdir, CFG)
        cases.append(("washed-merge-commit-stays-red", gate["violations"] == ["U9"], gate))

        # 4: a plain non-merge commit with a fresh units file has no HEAD^2;
        # everything reads newly-done (fail-closed), proving the fallback
        # is merge-shaped, not a general waiver
        plain = root / "plain"; plain.mkdir()
        _git(plain, "init", "-q", "-b", "main")
        _units(plain / "gate-pack" / "units.json", {"U1": ("brand new done", "done")})
        _git(plain, "add", "-A"); _git(plain, "commit", "-qm", "first commit")
        gate = grader.ceremony_done_gate(plain, CFG)
        cases.append(("non-merge-first-commit-stays-fail-closed",
                      gate["violations"] == ["U1"], gate))

    failures = [{"id": cid, "detail": detail} for cid, ok, detail in cases if not ok]
    print(json.dumps({
        "check_id": "lh-ceremony-grader-prefix",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "provider_invocations": 0,
    }, indent=2, default=str))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

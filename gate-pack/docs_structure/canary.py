#!/usr/bin/env python3
"""Docs structure canary (entry-governance P5).

Maintainer-side, deterministic, no provider. It enforces the *structural* rules of
the three-layer docs convention; semantic judgement (is this text an instruction?
should this artifact be slimmed?) stays with human batch review.

Checks:
- docs/ root holds no loose .md besides README.md.
- docs/contracts/, docs/active/, docs/archive/ all exist.
- docs/active/ holds at most MAX_ACTIVE_DOCS markdown files (one track one file).
- Total .md under docs/ stays under MAX_TOTAL_DOCS — an anti-doc-explosion
  ratchet: lower the constant as docs are slimmed, never raise it.
- AGENTS.md is at most MAX_AGENTS_LINES lines and carries no reading-order
  instructions (P3: any "read these N files in order" directive is deleted).
- CLI convention mirrors: when the same skill name has a tracked SKILL.md under
  both .claude/skills/ and .codex/skills/, their contents must be identical
  (P7 exception: mirrors are allowed only if generated or compared).

Usage:
  python3 gate-pack/docs_structure/canary.py              # check; exit 1 if RED
  python3 gate-pack/docs_structure/canary.py --json       # machine output
  python3 gate-pack/docs_structure/canary.py --self-test  # flip-test on fixtures
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

SEAL_DIR = Path(__file__).resolve().parent
REPO_ROOT = SEAL_DIR.parent.parent

MAX_ACTIVE_DOCS = 3
# Ratchet initialised at the batch-A landing count (29). P5 target is 24 after
# later slimming batches; only ever lower this constant, never raise it.
MAX_TOTAL_DOCS = 29
MAX_AGENTS_LINES = 40
FORBIDDEN_AGENTS_TOKENS = ("讀取順序",)

KNOWN_GAPS = [
    {"id": "cli-mirror-skip-when-untracked",
     "detail": ".claude/ and .codex/ are gitignored in this repo; the mirror comparison "
               "only runs when both SKILL.md copies are tracked"},
]


def _tracked(root: Path) -> set[str]:
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files"],
            capture_output=True, text=True, check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return set()
    return set(out.splitlines())


def _mirror_pairs(root: Path, tracked: set[str]) -> tuple[list[str], list[str]]:
    """Return (mismatched names, compared names) for tracked skill mirrors."""
    claude = root / ".claude" / "skills"
    codex = root / ".codex" / "skills"
    mismatched: list[str] = []
    compared: list[str] = []
    if not claude.is_dir() or not codex.is_dir():
        return mismatched, compared
    for skill in sorted(p.name for p in claude.iterdir() if p.is_dir()):
        a = claude / skill / "SKILL.md"
        b = codex / skill / "SKILL.md"
        rel_a = f".claude/skills/{skill}/SKILL.md"
        rel_b = f".codex/skills/{skill}/SKILL.md"
        if not a.is_file() or not b.is_file():
            continue
        if rel_a not in tracked or rel_b not in tracked:
            continue
        compared.append(skill)
        if a.read_text(encoding="utf-8") != b.read_text(encoding="utf-8"):
            mismatched.append(skill)
    return mismatched, compared


def check(root: Path, tracked: set[str] | None = None) -> dict:
    """Return the standard reliability-canary payload plus structure detail."""
    if tracked is None:
        tracked = _tracked(root)
    docs = root / "docs"

    missing_dirs = [d for d in ("contracts", "active", "archive") if not (docs / d).is_dir()]
    loose = sorted(p.name for p in docs.glob("*.md") if p.name != "README.md") if docs.is_dir() else []
    active_docs = sorted(p.name for p in (docs / "active").glob("*.md")) if (docs / "active").is_dir() else []
    total_docs = len(list(docs.rglob("*.md"))) if docs.is_dir() else 0

    agents = root / "AGENTS.md"
    agents_lines = 0
    agents_forbidden: list[str] = []
    if agents.is_file():
        agents_text = agents.read_text(encoding="utf-8")
        agents_lines = len(agents_text.splitlines())
        agents_forbidden = [t for t in FORBIDDEN_AGENTS_TOKENS if t in agents_text]

    mismatched, compared = _mirror_pairs(root, tracked)
    mirror_ok = not mismatched
    mirror_detail = (
        "" if mirror_ok else f"tracked mirror SKILL.md differs: {', '.join(mismatched)}"
    ) or (f"compared {len(compared)} mirror(s)" if compared else "no tracked mirror pair; skipped")

    canaries = [
        {"id": "three-layer-dirs", "ok": not missing_dirs,
         "detail": "" if not missing_dirs else f"missing docs dir(s): {', '.join(missing_dirs)}"},
        {"id": "no-loose-root-docs", "ok": not loose,
         "detail": "" if not loose else f"loose .md at docs/ root: {', '.join(loose)}"},
        {"id": "active-count", "ok": len(active_docs) <= MAX_ACTIVE_DOCS,
         "detail": "" if len(active_docs) <= MAX_ACTIVE_DOCS
         else f"docs/active/ holds {len(active_docs)} .md (> {MAX_ACTIVE_DOCS}); one track one file"},
        {"id": "total-docs-ratchet", "ok": total_docs <= MAX_TOTAL_DOCS,
         "detail": "" if total_docs <= MAX_TOTAL_DOCS
         else f"{total_docs} .md under docs/ (> {MAX_TOTAL_DOCS}); anti-doc-explosion ratchet — "
              "slim or archive before adding"},
        {"id": "agents-md-entry", "ok": agents.is_file() and agents_lines <= MAX_AGENTS_LINES and not agents_forbidden,
         "detail": "" if (agents.is_file() and agents_lines <= MAX_AGENTS_LINES and not agents_forbidden)
         else (f"AGENTS.md missing" if not agents.is_file()
               else f"AGENTS.md has {agents_lines} lines (> {MAX_AGENTS_LINES})" if agents_lines > MAX_AGENTS_LINES
               else f"AGENTS.md contains forbidden token(s): {', '.join(agents_forbidden)}")},
        {"id": "cli-convention-mirror", "ok": mirror_ok, "detail": mirror_detail},
    ]
    blocking = [{"id": c["id"], "detail": c["detail"]} for c in canaries if not c["ok"]]
    return {
        "check_id": "docs-structure", "status": "pass" if not blocking else "fail",
        "total": len(canaries), "blocking_failures": blocking,
        "known_gaps_open": KNOWN_GAPS,
        "counts": {"total_docs": total_docs, "active_docs": len(active_docs),
                   "agents_lines": agents_lines, "mirrors_compared": len(compared)},
    }


def _print_human(r: dict) -> None:
    print(f"docs-structure: {r['status'].upper()}")
    c = r["counts"]
    print(f"  docs total={c['total_docs']}/{MAX_TOTAL_DOCS} active={c['active_docs']}/{MAX_ACTIVE_DOCS}"
          f" AGENTS.md lines={c['agents_lines']}/{MAX_AGENTS_LINES} mirrors={c['mirrors_compared']}")
    for f in r["blocking_failures"]:
        print(f"  FAIL {f['id']}: {f['detail']}")


def _make_clean_root(root: Path) -> None:
    for layer in ("contracts", "active", "archive"):
        (root / "docs" / layer).mkdir(parents=True, exist_ok=True)
        (root / "docs" / layer / "one.md").write_text("# x\n", encoding="utf-8")
    (root / "AGENTS.md").write_text("# AGENTS.md\n\n## Commands\n- npm test\n", encoding="utf-8")


def _self_test() -> int:
    """Flip-test: every rule must go RED on a violating fixture, GREEN on a clean one."""
    failures: list[str] = []

    def expect(name: str, cond: bool) -> None:
        print(f"  {'ok  ' if cond else 'BAD '} {name}")
        if not cond:
            failures.append(name)

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        _make_clean_root(root)
        expect("clean fixture passes", check(root)["status"] == "pass")

        (root / "docs" / "loose.md").write_text("# loose\n", encoding="utf-8")
        expect("loose root doc detected",
               "no-loose-root-docs" in {f["id"] for f in check(root)["blocking_failures"]})
        (root / "docs" / "loose.md").unlink()

        for i in range(MAX_ACTIVE_DOCS):  # clean fixture already has one -> now over the cap
            (root / "docs" / "active" / f"extra{i}.md").write_text("# x\n", encoding="utf-8")
        expect("active overflow detected",
               "active-count" in {f["id"] for f in check(root)["blocking_failures"]})
        for i in range(MAX_ACTIVE_DOCS):
            (root / "docs" / "active" / f"extra{i}.md").unlink()

        existing = len(list((root / "docs").rglob("*.md")))
        for i in range(MAX_TOTAL_DOCS - existing + 1):
            (root / "docs" / "archive" / f"filler{i}.md").write_text("# x\n", encoding="utf-8")
        expect("total ratchet detected",
               "total-docs-ratchet" in {f["id"] for f in check(root)["blocking_failures"]})
        for i in range(MAX_TOTAL_DOCS - existing + 1):
            (root / "docs" / "archive" / f"filler{i}.md").unlink()

        (root / "docs" / "archive").rename(root / "docs" / "archive_bak")
        expect("missing layer dir detected",
               "three-layer-dirs" in {f["id"] for f in check(root)["blocking_failures"]})
        (root / "docs" / "archive_bak").rename(root / "docs" / "archive")

        (root / "AGENTS.md").write_text("# AGENTS.md\n" + "pad\n" * MAX_AGENTS_LINES, encoding="utf-8")
        expect("oversized AGENTS.md detected",
               "agents-md-entry" in {f["id"] for f in check(root)["blocking_failures"]})
        (root / "AGENTS.md").write_text("# AGENTS.md\n讀取順序：a b c\n", encoding="utf-8")
        expect("reading-order token detected",
               "agents-md-entry" in {f["id"] for f in check(root)["blocking_failures"]})
        (root / "AGENTS.md").write_text("# AGENTS.md\n", encoding="utf-8")

        for base in (".claude", ".codex"):
            (root / base / "skills" / "demo").mkdir(parents=True, exist_ok=True)
        (root / ".claude" / "skills" / "demo" / "SKILL.md").write_text("v1\n", encoding="utf-8")
        (root / ".codex" / "skills" / "demo" / "SKILL.md").write_text("v2\n", encoding="utf-8")
        tracked = {".claude/skills/demo/SKILL.md", ".codex/skills/demo/SKILL.md"}
        expect("mirror mismatch detected",
               "cli-convention-mirror" in {f["id"] for f in check(root, tracked)["blocking_failures"]})
        (root / ".codex" / "skills" / "demo" / "SKILL.md").write_text("v1\n", encoding="utf-8")
        expect("mirror match passes", check(root, tracked)["status"] == "pass")
        expect("untracked mirror skipped", check(root, set())["status"] == "pass")

    if failures:
        print(f"docs-structure self-test: FAIL ({len(failures)} expectation(s) unmet)")
        return 1
    print("docs-structure self-test: PASS (all rules flip RED on violation, GREEN when clean)")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return _self_test()
    if not (REPO_ROOT / "docs").is_dir():
        # Framework-only consumers (e.g. the public Loop Hybrid 2 release) ship
        # without a docs layer; the three-layer rules are meaningful only where
        # one exists.  Report skip instead of failing.
        r = {"check_id": "docs-structure", "status": "skip", "reason": "no docs/ layer in this repo", "blocking_failures": []}
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0
    r = check(REPO_ROOT)
    if "--json" in argv:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        _print_human(r)
    return 0 if r["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

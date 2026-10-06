#!/usr/bin/env python3
"""Docs contracts canary: a contract document may only name what the code has.

Every contract under ``docs/contracts/`` describes the engine as it is.  Any
backticked repository path, schema id (``name/vN``), command-line flag, or
``LH_*`` environment name it mentions must exist in the tracked sources, and
every contract ends with a ``## 與現行程式的差異`` section that names where the
earlier design and the current code disagree (or says there is none).

Usage:
  python3 gate-pack/docs_contracts/canary.py              # check; exit 1 if RED
  python3 gate-pack/docs_contracts/canary.py --self-test  # flip-test on fixtures
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

CHECK_ID = "docs-contracts"
REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = (
    "goal-lifecycle-v1.md",
    "goal-hierarchy-v1.md",
    "model-routing-v1.md",
    "candidate-review-v2.md",
    "autonomous-driver-v1.md",
    "command-ingress-v1.md",
    "status-snapshot-v1.md",
    "observability-v1.md",
    "token-accounting-v1.md",
    "campaign-requirement-template.md",
    "entry-governance-v1.md",
    "planner-recovery-v1.md",
)
DIFFERENCES_HEADING = "## 與現行程式的差異"
SOURCE_PREFIXES = ("lh_runtime/", "gate-pack/", "tests/", "tools/")
TICKED = re.compile(r"`([^`\n]+)`")
PATH_RE = re.compile(r"^(?:lh_runtime|gate-pack|tests|tools)/[A-Za-z0-9_./-]+$")
SCHEMA_RE = re.compile(r"^[a-z][a-z0-9.-]*/v[0-9]+$")
FLAG_RE = re.compile(r"^--[a-z][a-z0-9-]*$")
ENV_RE = re.compile(r"^LH_[A-Z0-9_]+$")
ARG_RE = re.compile(r"""add_argument\(\s*["'](--[a-z][a-z0-9-]*)["']""")


def _tracked(root: Path) -> list[str]:
    try:
        out = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    return out.splitlines()


def _sources(root: Path, tracked: list[str]) -> tuple[str, set[str]]:
    texts, flags = [], set()
    for name in tracked:
        if name.endswith(".py") and name.startswith(SOURCE_PREFIXES):
            try:
                text = (root / name).read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            texts.append(text)
            if name.startswith("lh_runtime/"):
                flags.update(ARG_RE.findall(text))
    return "\n".join(texts), flags


def check(root: Path, tracked: list[str] | None = None, contracts: tuple[str, ...] = CONTRACTS) -> dict:
    tracked = _tracked(root) if tracked is None else tracked
    corpus, flags = _sources(root, tracked)
    folder = root / "docs" / "contracts"
    missing = [name for name in contracts if not (folder / name).is_file()]
    bad = {"paths": [], "schemas": [], "flags": [], "environment": [], "differences": []}
    for name in contracts:
        path = folder / name
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        if DIFFERENCES_HEADING not in text.splitlines():
            bad["differences"].append(name)
        for token in TICKED.findall(text):
            token = token.strip()
            if PATH_RE.match(token):
                relative = token.rstrip("/")
                if not (root / relative).exists():
                    bad["paths"].append(f"{name}: {token}")
            elif SCHEMA_RE.match(token):
                if token not in corpus:
                    bad["schemas"].append(f"{name}: {token}")
            elif FLAG_RE.match(token):
                if token not in flags:
                    bad["flags"].append(f"{name}: {token}")
            elif ENV_RE.match(token):
                if token not in corpus:
                    bad["environment"].append(f"{name}: {token}")
    cases = [
        {"id": "contract-set-is-present", "ok": not missing, "detail": missing},
        {"id": "referenced-paths-exist", "ok": not bad["paths"], "detail": bad["paths"][:20]},
        {"id": "schema-ids-exist-in-code", "ok": not bad["schemas"], "detail": bad["schemas"][:20]},
        {"id": "cli-flags-exist-in-code", "ok": not bad["flags"], "detail": bad["flags"][:20]},
        {"id": "environment-names-exist-in-code", "ok": not bad["environment"], "detail": bad["environment"][:20]},
        {"id": "every-contract-marks-its-differences", "ok": not bad["differences"], "detail": bad["differences"]},
    ]
    failures = [case["id"] for case in cases if not case["ok"]]
    return {"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(cases),
            "results": cases, "blocking_failures": failures}


def _fixture(root: Path, body: str) -> list[str]:
    (root / "lh_runtime").mkdir(parents=True)
    (root / "lh_runtime" / "engine.py").write_text(
        'SCHEMA = "lh-fixture/v1"\nENV = "LH_FIXTURE_ROOT"\n'
        'parser.add_argument("--fixture-flag")\n', encoding="utf-8")
    (root / "docs" / "contracts").mkdir(parents=True)
    (root / "docs" / "contracts" / "one.md").write_text(body, encoding="utf-8")
    return ["lh_runtime/engine.py", "docs/contracts/one.md"]


def self_test() -> dict:
    clean = ("# One\n\n`lh_runtime/engine.py` writes `lh-fixture/v1`; run with `--fixture-flag` "
             "and `LH_FIXTURE_ROOT`.\n\n" + DIFFERENCES_HEADING + "\n\n無。\n")
    violations = {
        "path": clean.replace("`lh_runtime/engine.py`", "`lh_runtime/missing.py`"),
        "schema": clean.replace("`lh-fixture/v1`", "`lh-missing/v1`"),
        "flag": clean.replace("`--fixture-flag`", "`--missing-flag`"),
        "environment": clean.replace("`LH_FIXTURE_ROOT`", "`LH_MISSING_ROOT`"),
        "differences": clean.replace(DIFFERENCES_HEADING, "## Notes"),
    }
    observed = {}
    with tempfile.TemporaryDirectory(prefix="docs-contracts-") as raw:
        root = Path(raw) / "clean"
        tracked = _fixture(root, clean)
        observed["clean"] = check(root, tracked, ("one.md",))["status"]
        for name, body in violations.items():
            root = Path(raw) / name
            tracked = _fixture(root, body)
            observed[name] = check(root, tracked, ("one.md",))["status"]
    ok = observed == {"clean": "pass", **{name: "fail" for name in violations}}
    return {"id": "checks-flip-on-violation", "ok": ok, "detail": observed}


def main(argv: list[str]) -> int:
    flipped = self_test()
    if "--self-test" in argv:
        print(json.dumps(flipped, ensure_ascii=False, indent=2))
        return 0 if flipped["ok"] else 1
    result = check(REPO_ROOT)
    result["results"].append(flipped)
    if not flipped["ok"]:
        result["blocking_failures"].append(flipped["id"])
        result["status"] = "fail"
    result["total"] = len(result["results"])
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

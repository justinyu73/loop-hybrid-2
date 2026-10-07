#!/usr/bin/env python3
"""Contract seal: a sealed document cannot change without a visible reseal.

A rule that says "changing a contract needs approval" stops nobody: an agent
that never reads it is unaffected, and one that reads it can edit it -- together
with the check that reads the same edited text.  The seal gives the check an
anchor the edited text cannot move: one small JSON file holding a digest per
sealed document.  Changing a contract now forces a visible change to that file,
which is short enough to review in a pull request at a glance.

The scope comes from the repository and the command line, never from the seal:
``discover`` lists the tracked and untracked-but-not-ignored files matching the
``--include`` patterns.  A check whose scope is defined by the artifact it
checks cannot see that artifact shrink, so a seal that drops an entry, or
records a narrower scope than the one asked for, is broken.

Resealing is legitimate when an approved change really alters a contract.  It
is not blocked; it is made loud: ``reseal`` always recomputes the whole scope
and records who resealed and why.  There is no per-file reseal.

Digests normalise CRLF to LF, so Windows and Linux checkouts seal alike.

    python3 gate-pack/contract_seal/seal.py verify --root .
    python3 gate-pack/contract_seal/seal.py reseal --root . --sealed-by <name> --reason <text>
"""
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

SCHEMA = "lh-contract-seal/v1"
DEFAULT_INCLUDE = ("docs/contracts/*.md",)
DEFAULT_SEAL = "docs/contracts/seal.json"


def digest_bytes(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def discover(root: Path, include: list[str]) -> list[str]:
    """Files in scope: tracked or untracked-but-not-ignored, matching a pattern."""
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        check=True, capture_output=True,
    ).stdout.decode("utf-8").split("\0")
    # fnmatch's "*" also crosses "/", so "docs/contracts/*.md" covers nested files too.
    return sorted({path for path in listed if path and any(fnmatch.fnmatchcase(path, pattern) for pattern in include)})


def observe(root: Path, include: list[str], seal_rel: str) -> dict[str, str | None]:
    return {
        path: (digest_bytes((root / path).read_bytes()) if (root / path).is_file() else None)
        for path in discover(root, include)
        if path != seal_rel
    }


def load_seal(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema") != SCHEMA or not isinstance(data.get("files"), dict):
        return None
    return data


def verify(root: Path, include: list[str], seal_rel: str) -> dict[str, Any]:
    seal = load_seal(root / seal_rel)
    observed = observe(root, include, seal_rel)
    problems: list[dict[str, Any]] = []
    if seal is None:
        problems.append({"kind": "seal_unreadable", "path": seal_rel})
        sealed: dict[str, Any] = {}
    else:
        sealed = seal["files"]
        if sorted(seal.get("scope") or []) != sorted(include):
            problems.append({"kind": "scope_mismatch", "sealed": seal.get("scope"), "asked": include})
    for path, digest in observed.items():
        if path not in sealed:
            problems.append({"kind": "unsealed_file", "path": path})
        elif digest is None:
            problems.append({"kind": "sealed_file_missing", "path": path})
        elif digest != sealed[path]:
            problems.append({"kind": "digest_mismatch", "path": path, "sealed": sealed[path], "observed": digest})
    for path in sorted(set(sealed) - set(observed)):
        problems.append({"kind": "sealed_file_missing", "path": path})
    return {"schema": SCHEMA, "verdict": "intact" if not problems else "broken", "seal": seal_rel,
            "scope": include, "covered": sorted(observed), "problems": problems}


def reseal(root: Path, include: list[str], seal_rel: str, *, sealed_by: str, reason: str) -> dict[str, Any]:
    if not sealed_by.strip() or not reason.strip():
        raise ValueError("reseal needs a non-empty --sealed-by and --reason")
    observed = observe(root, include, seal_rel)
    missing = sorted(path for path, digest in observed.items() if digest is None)
    if missing:
        raise ValueError(f"cannot seal files that are missing from the working tree: {missing}")
    body = {"schema": SCHEMA, "sealed_by": sealed_by.strip(), "reason": reason.strip(), "scope": sorted(include),
            "files": dict(sorted(observed.items()))}
    path = root / seal_rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"schema": SCHEMA, "verdict": "resealed", "seal": seal_rel, "covered": sorted(observed)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seal documents so they cannot change without a visible reseal")
    parser.add_argument("command", choices=("discover", "verify", "reseal"))
    parser.add_argument("--root", default=".")
    parser.add_argument("--include", action="append", help="fnmatch pattern; repeatable")
    parser.add_argument("--seal", default=DEFAULT_SEAL, help="seal file path relative to --root")
    parser.add_argument("--sealed-by", default="")
    parser.add_argument("--reason", default="")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    include = sorted(args.include or DEFAULT_INCLUDE)
    try:
        if args.command == "discover":
            result: dict[str, Any] = {"schema": SCHEMA, "scope": include,
                                      "covered": [p for p in discover(root, include) if p != args.seal]}
        elif args.command == "verify":
            result = verify(root, include, args.seal)
        else:
            result = reseal(root, include, args.seal, sealed_by=args.sealed_by, reason=args.reason)
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(json.dumps({"schema": SCHEMA, "verdict": "error", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["verdict"] != "broken" else 1


if __name__ == "__main__":
    sys.exit(main())

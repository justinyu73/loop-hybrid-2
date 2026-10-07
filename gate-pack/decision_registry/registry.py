#!/usr/bin/env python3
"""Decision registry: the exam paper exists before the answer.

A target repository records each decision point in an append-only ledger
before the work it governs.  Four rules follow:

1. A decision cannot be registered without acceptance probes and a declared
   surface.  A criterion written after the work is a description of the work.
2. "Done" is never stored.  ``readback`` re-runs the probes and derives the
   disposition each time.
3. Absence is a signal.  ``orphans`` reads git, not the ledger's own
   discipline: a commit that touches a guarded path must name a decision that
   is already registered in that commit's own tree.  Each commit is judged on
   its own, so a later commit cannot wash an earlier orphan.
4. What merged is what was registered.  Paths changed by commits that name a
   decision must fall inside its declared surface (default deny), and bound
   artefacts keep their registration digest unless a named row supersedes them.

5. An exam never seen red proves nothing.  ``red-proof`` runs a decision's
   probes in a disposable clone at a given commit and records, per probe, that
   it ran and failed there; a probe already passing there is not a proof.  A
   probe covering existing behaviour may be exempted at registration with a
   reason.  Approval never requires green: the red is what shows the gap is
   real, and green is what the approved work produces.

This is not tamper-proof: the ledger is a file the governed agent can write.
It turns a silent bypass into a visible edit in a diff.

Files in the target repository:
  decisions/policy.json          {"schema": "lh-decision-policy/v1", "guarded_prefixes": [...],
                                  "require_red_proof": false}
  decisions/registrations.jsonl  append-only rows, hash-chained
  decisions/red-proofs.jsonl     append-only red proofs, hash-chained

Usage:
  registry.py register  --root R --row row.json
  registry.py verify    --root R
  registry.py orphans   --root R --range A..B
  registry.py readback  --root R --decision ID --range A..B
  registry.py red-proof --root R --decision ID --at REV
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

POLICY_SCHEMA = "lh-decision-policy/v1"
ROW_SCHEMA = "lh-decision-registration/v1"
PROOF_SCHEMA = "lh-decision-red-proof/v1"
LEDGER = "decisions/registrations.jsonl"
POLICY = "decisions/policy.json"
PROOFS = "decisions/red-proofs.jsonl"


class Refused(ValueError):
    def __init__(self, reason: str, detail: Any = None):
        super().__init__(reason)
        self.reason, self.detail = reason, detail


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _rows(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def ledger_rows(root: Path) -> list[dict[str, Any]]:
    path = root / LEDGER
    return _rows(path.read_text(encoding="utf-8")) if path.is_file() else []


def proof_rows(root: Path) -> list[dict[str, Any]]:
    path = root / PROOFS
    return _rows(path.read_text(encoding="utf-8")) if path.is_file() else []


def _exempt_reason(probe: dict[str, Any]) -> str | None:
    red = probe.get("red_proof")
    return red["exempt"] if isinstance(red, dict) and isinstance(red.get("exempt"), str) else None


def _chain_problems(rows: list[dict[str, Any]], ledger: str) -> list[dict[str, Any]]:
    problems = []
    previous = None
    for index, row in enumerate(rows):
        body = {key: value for key, value in row.items() if key != "row_digest"}
        if row.get("row_digest") != _digest(body):
            problems.append({"ledger": ledger, "row": index, "reason": "row_digest_mismatch"})
        if row.get("prev_digest") != previous:
            problems.append({"ledger": ledger, "row": index, "reason": "chain_broken"})
        previous = row.get("row_digest")
    return problems


def _git(root: Path, *args: str, check: bool = True) -> str:
    done = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True,
                          encoding="utf-8", errors="replace", check=False)
    if check and done.returncode != 0:
        raise Refused("git_unreadable", done.stderr.strip()[:200])
    return done.stdout


def register(root: Path, raw: dict[str, Any]) -> dict[str, Any]:
    decision_id = raw.get("decision_id")
    if not isinstance(decision_id, str) or not decision_id.strip():
        raise Refused("decision_id_required")
    probes = raw.get("acceptance")
    if not isinstance(probes, list) or not probes:
        raise Refused("acceptance_probes_required")
    for probe in probes:
        if (not isinstance(probe, dict) or not isinstance(probe.get("id"), str)
                or not isinstance(probe.get("argv"), list) or not probe["argv"]
                or not all(isinstance(item, str) for item in probe["argv"])
                or type(probe.get("expect_exit", 0)) is not int):
            raise Refused("acceptance_probe_invalid", probe)
        red = probe.get("red_proof")
        if red is not None and (not isinstance(red, dict) or set(red) != {"exempt"}
                                or not isinstance(red["exempt"], str) or not red["exempt"].strip()):
            raise Refused("red_proof_exempt_reason_required", probe.get("id"))
    surface = raw.get("surface")
    if not isinstance(surface, list) or not all(isinstance(item, str) and item for item in surface):
        raise Refused("surface_required")
    candidates = raw.get("candidates")
    if not isinstance(candidates, list) or len(candidates) < 2:
        raise Refused("candidates_required")
    rows = ledger_rows(root)
    known = {row["decision_id"] for row in rows}
    supersedes = raw.get("supersedes")
    if supersedes is not None and supersedes not in known:
        raise Refused("supersedes_unknown", supersedes)
    if decision_id in known:
        raise Refused("decision_id_duplicate", decision_id)
    artefacts = []
    for relative in raw.get("artefacts") or []:
        path = root / relative
        if not isinstance(relative, str) or not path.is_file():
            raise Refused("artefact_missing", relative)
        artefacts.append({"path": relative, "sha256": _file_digest(path)})
    body = {"schema": ROW_SCHEMA, "decision_id": decision_id, "question": raw.get("question", ""),
            "candidates": list(candidates), "acceptance": probes, "surface": list(surface),
            "artefacts": artefacts, "supersedes": supersedes,
            "prev_digest": rows[-1]["row_digest"] if rows else None}
    row = {**body, "row_digest": _digest(body)}
    path = root / LEDGER
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return {"status": "registered", "decision_id": decision_id, "row_digest": row["row_digest"]}


def verify(root: Path) -> dict[str, Any]:
    rows = ledger_rows(root)
    problems = _chain_problems(rows, LEDGER) + _chain_problems(proof_rows(root), PROOFS)
    superseded = {row.get("supersedes") for row in rows if row.get("supersedes")}
    for row in rows:
        if row.get("decision_id") in superseded:
            continue
        for artefact in row.get("artefacts", []):
            path = root / artefact["path"]
            if not path.is_file() or _file_digest(path) != artefact["sha256"]:
                problems.append({"decision_id": row.get("decision_id"), "reason": "artefact_changed",
                                 "path": artefact["path"]})
    return {"status": "pass" if not problems else "fail", "rows": len(rows), "problems": problems}


def _guarded_prefixes(root: Path) -> list[str]:
    policy = json.loads((root / POLICY).read_text(encoding="utf-8"))
    if policy.get("schema") != POLICY_SCHEMA or not isinstance(policy.get("guarded_prefixes"), list):
        raise Refused("policy_invalid")
    return [str(item) for item in policy["guarded_prefixes"]]


def _commits(root: Path, revision_range: str) -> list[dict[str, Any]]:
    shas = _git(root, "rev-list", "--reverse", revision_range).split()
    commits = []
    for sha in shas:
        subject = _git(root, "show", "-s", "--format=%s", sha).strip()
        body = _git(root, "show", "-s", "--format=%b", sha)
        files = [line.strip() for line in
                 _git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", sha).splitlines()
                 if line.strip()]
        ledger = _git(root, "show", f"{sha}:{LEDGER}", check=False)
        known = {row.get("decision_id") for row in _rows(ledger)} if ledger.strip() else set()
        # Squash merges carry each original subject on a "* " line; prose does not count.
        lines = [subject] + [line.strip()[2:].strip() for line in body.splitlines() if line.strip().startswith("* ")]
        commits.append({"sha": sha, "subject": subject, "files": files, "known": known, "lines": lines})
    return commits


def _names(lines: list[str], decision_id: str) -> bool:
    pattern = re.compile(r"(?<![A-Za-z0-9_-])" + re.escape(decision_id) + r"(?![A-Za-z0-9_-])")
    return any(pattern.search(line) for line in lines)


def orphans(root: Path, revision_range: str) -> dict[str, Any]:
    prefixes = _guarded_prefixes(root)
    found = []
    for commit in _commits(root, revision_range):
        matched = [path for path in commit["files"] if path.startswith(tuple(prefixes))]
        if not matched:
            continue
        if any(_names(commit["lines"], decision_id) for decision_id in commit["known"] if decision_id):
            continue
        found.append({"sha": commit["sha"], "subject": commit["subject"], "paths": matched})
    return {"status": "pass" if not found else "fail", "orphans": found}


def _run_probe(root: Path, probe: dict[str, Any]) -> dict[str, Any]:
    try:
        done = subprocess.run(probe["argv"], cwd=root, capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=int(probe.get("timeout_seconds", 120)))
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"id": probe["id"], "ok": False, "error": type(exc).__name__}
    ok = done.returncode == probe.get("expect_exit", 0)
    if ok and "expect_stdout" in probe:
        ok = done.stdout.strip() == str(probe["expect_stdout"]).strip()
    return {"id": probe["id"], "ok": ok, "exit": done.returncode}


def _current_row(root: Path, decision_id: str) -> dict[str, Any]:
    rows = [row for row in ledger_rows(root) if row.get("decision_id") == decision_id]
    if not rows:
        raise Refused("decision_unregistered", decision_id)
    return rows[-1]


def _require_red_proof(root: Path) -> bool:
    path = root / POLICY
    if not path.is_file():
        return False
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise Refused("policy_invalid") from exc
    return isinstance(policy, dict) and policy.get("require_red_proof") is True


def _remove(path: Path) -> None:
    for parent, _dirs, files in os.walk(path):
        for name in files:
            os.chmod(os.path.join(parent, name), stat.S_IWRITE)  # git objects are read-only on Windows
    shutil.rmtree(path)


def red_proof(root: Path, decision_id: str, revision: str) -> dict[str, Any]:
    row = _current_row(root, decision_id)
    sha = _git(root, "rev-parse", "--verify", f"{revision}^{{commit}}").strip()
    probes = [probe for probe in row.get("acceptance", []) if _exempt_reason(probe) is None]
    exempt = [probe["id"] for probe in row.get("acceptance", []) if _exempt_reason(probe) is not None]
    scratch = Path(tempfile.mkdtemp(prefix="red-proof-"))
    try:
        clone = scratch / "clone"
        cloned = subprocess.run(["git", "clone", "--quiet", "--no-local", str(root), str(clone)],
                                capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if cloned.returncode != 0:
            raise Refused("clone_failed", cloned.stderr.strip()[:200])
        _git(clone, "checkout", "--quiet", "--detach", sha)
        results = [(probe, _run_probe(clone, probe)) for probe in probes]
    finally:
        _remove(scratch)
    unrunnable = [result for _probe, result in results if "error" in result]
    if unrunnable:
        raise Refused("probe_unrunnable_at_rev", unrunnable)
    green = [result["id"] for _probe, result in results if result["ok"]]
    if green:
        raise Refused("probe_green_at_rev", green)
    existing = proof_rows(root)
    previous = existing[-1]["row_digest"] if existing else None
    recorded = []
    path = root / PROOFS
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        for probe, result in results:
            body = {"schema": PROOF_SCHEMA, "decision_id": decision_id, "decision_row_digest": row["row_digest"],
                    "probe_id": probe["id"], "probe_digest": _digest(probe), "commit": sha,
                    "exit": result["exit"], "verdict": "red", "prev_digest": previous}
            proof = {**body, "row_digest": _digest(body)}
            stream.write(json.dumps(proof, ensure_ascii=False, sort_keys=True) + "\n")
            previous = proof["row_digest"]
            recorded.append(probe["id"])
    return {"status": "pass", "decision_id": decision_id, "commit": sha, "recorded": recorded, "exempt": exempt}


def _red_proof_state(proofs: list[dict[str, Any]], row: dict[str, Any], probe: dict[str, Any]) -> str:
    if _exempt_reason(probe) is not None:
        return "exempt"
    digest = _digest(probe)
    proven = any(proof.get("decision_row_digest") == row.get("row_digest") and proof.get("probe_id") == probe["id"]
                 and proof.get("probe_digest") == digest and proof.get("verdict") == "red" for proof in proofs)
    return "proven" if proven else "missing"


def readback(root: Path, decision_id: str, revision_range: str) -> dict[str, Any]:
    row = _current_row(root, decision_id)
    surface = tuple(row.get("surface", []))
    drift = []
    for commit in _commits(root, revision_range):
        if not _names(commit["lines"], decision_id):
            continue
        # Default deny: only the declared surface and the decision ledgers may change.
        outside = [path for path in commit["files"] if not (path in (LEDGER, PROOFS) or path.startswith(surface))]
        if outside:
            drift.append({"sha": commit["sha"], "paths": outside})
    proofs = proof_rows(root)
    probes = []
    for probe in row.get("acceptance", []):
        result = {**_run_probe(root, probe), "red_proof": _red_proof_state(proofs, row, probe)}
        if _exempt_reason(probe) is not None:
            result["exempt_reason"] = _exempt_reason(probe)
        probes.append(result)
    disposition = "passing" if probes and all(item["ok"] for item in probes) else "failing"
    unproven = [item["id"] for item in probes if item["red_proof"] == "missing"] if _require_red_proof(root) else []
    status = "pass" if not drift and disposition == "passing" and not unproven else "fail"
    return {"status": status, "decision_id": decision_id, "drift": drift, "disposition": disposition,
            "probes": probes, "red_proof_missing": unproven}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["register", "verify", "orphans", "readback", "red-proof"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--row", type=Path)
    parser.add_argument("--range", dest="revision_range")
    parser.add_argument("--decision")
    parser.add_argument("--at", dest="revision", help="red-proof: the commit the probes must fail at")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        if args.command == "register":
            if args.row is None:
                raise Refused("row_required")
            result = register(root, json.loads(args.row.read_text(encoding="utf-8")))
        elif args.command == "verify":
            result = verify(root)
        elif args.command == "orphans":
            if not args.revision_range:
                raise Refused("range_required")
            result = orphans(root, args.revision_range)
        elif args.command == "red-proof":
            if not args.decision or not args.revision:
                raise Refused("decision_and_revision_required")
            result = red_proof(root, args.decision, args.revision)
        else:
            if not args.decision or not args.revision_range:
                raise Refused("decision_and_range_required")
            result = readback(root, args.decision, args.revision_range)
    except Refused as exc:
        result = {"status": "refused", "reason": exc.reason, "detail": exc.detail}
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") in {"registered", "pass"} else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

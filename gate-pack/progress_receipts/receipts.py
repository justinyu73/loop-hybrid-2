#!/usr/bin/env python3
"""Progress receipts: ask for a receipt instead of writing one.

A progress claim is something the judged agent writes about itself.  A
receipt is different: it exists only because a check really ran.  The two
roles are split:

    requester               request  ->  [ queue ]  ->  verifier
    (writes requests)                                   (runs checks, writes receipts)

- A request names a check id, never a command.  The verifier looks the id up
  in its own registry (``lh-checks-registry/v1``), whose digest is written into
  every receipt, so a changed command leaves a trace.
- The verifier runs the check in a clone pinned to the repository's HEAD and
  records that commit, so an edit made after the request cannot change what
  was checked.
- Receipts land in an append-only, hash-chained ledger; rewriting one line
  breaks verification of every later one.
- Whether requester and verifier are really different principals is a
  deployment fact this code cannot create.  Every verdict measures whether
  the caller can still write the ledger and states the answer.

Usage:
  receipts.py request --queue Q --task T --session S --check ID --nonce N
  receipts.py request --queue Q --file request.json
  receipts.py serve   --queue Q --registry checks.json --repo REPO --ledger L
  receipts.py verdict --ledger L --task T --check ID [--nonce N]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

REGISTRY_SCHEMA = "lh-checks-registry/v1"
RECEIPT_SCHEMA = "lh-progress-receipt/v1"
REQUEST_FIELDS = {"task", "session", "check", "nonce"}


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _refused(reason: str, **detail: Any) -> dict[str, Any]:
    return {"status": "refused", "reason": reason, **detail}


def _valid_request(value: Any) -> str | None:
    if not isinstance(value, dict) or set(value) != REQUEST_FIELDS:
        return "request_field_forbidden"
    if not all(isinstance(value[key], str) and value[key].strip() for key in REQUEST_FIELDS):
        return "request_invalid"
    return None


def request(queue: Path, value: dict[str, Any]) -> dict[str, Any]:
    reason = _valid_request(value)
    if reason is not None:
        return _refused(reason)
    request_id = _digest(value).removeprefix("sha256:")[:32]
    folder = queue / "requests"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{request_id}.json").write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    return {"status": "requested", "request_id": request_id}


def _ledger(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append(path: Path, body: dict[str, Any]) -> dict[str, Any]:
    rows = _ledger(path)
    body = {**body, "prev_digest": rows[-1]["receipt_digest"] if rows else None}
    row = {**body, "receipt_digest": _digest(body)}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return row


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
                          encoding="utf-8").stdout.strip()


def _remove(path: Path) -> None:
    def writable(function, target, _info):
        os.chmod(target, stat.S_IWRITE)
        function(target)
    shutil.rmtree(path, onerror=writable)


def _run_check(repo: Path, check: dict[str, Any]) -> dict[str, Any]:
    commit = _git(repo, "rev-parse", "HEAD")
    parent = Path(tempfile.mkdtemp(prefix="progress-receipt-"))
    try:
        snapshot = parent / "snapshot"
        subprocess.run(["git", "clone", "--no-hardlinks", "--quiet", str(repo), str(snapshot)], check=True,
                       capture_output=True)
        _git(snapshot, "checkout", "--detach", "--quiet", commit)
        try:
            done = subprocess.run(check["argv"], cwd=snapshot, capture_output=True,
                                  timeout=int(check.get("timeout_seconds", 300)))
            exit_code, output = done.returncode, done.stdout + done.stderr
        except (OSError, subprocess.TimeoutExpired) as exc:
            exit_code, output = None, type(exc).__name__.encode()
    finally:
        _remove(parent)
    return {"commit": commit, "exit_code": exit_code,
            "ok": exit_code == check.get("expect_exit", 0),
            "output_digest": "sha256:" + hashlib.sha256(output).hexdigest()}


def serve(queue: Path, registry_path: Path, repo: Path, ledger: Path) -> dict[str, Any]:
    raw = registry_path.read_bytes()
    registry = json.loads(raw)
    if registry.get("schema") != REGISTRY_SCHEMA or not isinstance(registry.get("checks"), dict):
        return _refused("registry_invalid")
    registry_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    requests, answers = queue / "requests", queue / "answers"
    answers.mkdir(parents=True, exist_ok=True)
    answered = []
    for path in sorted(requests.glob("*.json")) if requests.is_dir() else []:
        answer_path = answers / path.name
        if answer_path.exists():
            continue  # a replayed request is never run twice
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            value = None
        reason = _valid_request(value)
        if reason is None and value["check"] not in registry["checks"]:
            reason = "check_unregistered"
        if reason is not None:
            answer = {"request_id": path.stem, "status": "rejected", "reason": reason}
        else:
            outcome = _run_check(repo, registry["checks"][value["check"]])
            receipt = _append(ledger, {"schema": RECEIPT_SCHEMA, **value, **outcome,
                                       "registry_digest": registry_digest})
            answer = {"request_id": path.stem, "status": "receipted", "receipt_digest": receipt["receipt_digest"]}
        answer_path.write_text(json.dumps(answer, sort_keys=True), encoding="utf-8")
        answered.append(answer)
    return {"status": "served", "answered": answered}


def _chain_intact(rows: list[dict[str, Any]]) -> bool:
    previous = None
    for row in rows:
        body = {key: value for key, value in row.items() if key != "receipt_digest"}
        if row.get("receipt_digest") != _digest(body) or row.get("prev_digest") != previous:
            return False
        previous = row["receipt_digest"]
    return True


def verdict(ledger: Path, task: str, check: str, nonce: str | None) -> dict[str, Any]:
    separated = ledger.exists() and not os.access(ledger, os.W_OK) and not os.access(ledger.parent, os.W_OK)
    boundary = {"principal_separated": separated,
                "cannot_claim": [] if separated else [
                    "the caller can write the receipt ledger, so requester and verifier are not separated "
                    "principals; a receipt here proves a check ran, not that the requester could not forge one"]}
    rows = _ledger(ledger)
    if not _chain_intact(rows):
        return {"status": "fail", "reason": "chain_broken", **boundary}
    matches = [row for row in rows if row.get("task") == task and row.get("check") == check
               and (nonce is None or row.get("nonce") == nonce)]
    if not matches:
        return {"status": "fail", "reason": "no_receipt", **boundary}
    latest = matches[-1]
    if latest.get("ok") is not True:
        return {"status": "fail", "reason": "check_failed", "receipt": latest, **boundary}
    return {"status": "pass", "receipt": latest, **boundary}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    req = sub.add_parser("request")
    req.add_argument("--queue", type=Path, required=True)
    req.add_argument("--file", type=Path)
    for name in REQUEST_FIELDS:
        req.add_argument(f"--{name}")
    srv = sub.add_parser("serve")
    srv.add_argument("--queue", type=Path, required=True)
    srv.add_argument("--registry", type=Path, required=True)
    srv.add_argument("--repo", type=Path, required=True)
    srv.add_argument("--ledger", type=Path, required=True)
    ver = sub.add_parser("verdict")
    ver.add_argument("--ledger", type=Path, required=True)
    ver.add_argument("--task", required=True)
    ver.add_argument("--check", required=True)
    ver.add_argument("--nonce")
    args = parser.parse_args(argv)
    if args.command == "request":
        if args.file is not None:
            value = json.loads(args.file.read_text(encoding="utf-8"))
        else:
            value = {name: getattr(args, name) for name in REQUEST_FIELDS}
        result = request(args.queue, value)
    elif args.command == "serve":
        result = serve(args.queue, args.registry, args.repo.resolve(), args.ledger)
    else:
        result = verdict(args.ledger, args.task, args.check, args.nonce)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") in {"requested", "served", "pass"} else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Progress receipts canary: progress is a receipt for a check that ran, not a claim.

Every case drives ``receipts.py`` and ``timeline.py`` through their command
lines against a throwaway repository, queue, registry, and ledger.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
RECEIPTS = HERE / "receipts.py"
TIMELINE = HERE / "timeline.py"
CHECK_ID = "progress-receipts"
CHECK = "from pathlib import Path; assert Path('state.txt').read_text(encoding='utf-8').strip() == 'ok'"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def run(script: Path, *args: str) -> tuple[int, dict[str, Any]]:
    if not script.is_file():
        raise FileNotFoundError(f"{script.relative_to(HERE.parent.parent)} is not provided")
    done = subprocess.run([sys.executable, "-B", str(script), *args], capture_output=True, text=True,
                          encoding="utf-8", timeout=120)
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError:
        return done.returncode, {"unparsed": done.stdout[-300:], "stderr": done.stderr[-300:]}


class Fixture:
    def __init__(self, root: Path, *, state: str = "ok"):
        self.root = root
        self.repo = root / "repo"
        self.repo.mkdir(parents=True)
        self.git("init", "-q")
        self.git("config", "user.email", "receipts@example.invalid")
        self.git("config", "user.name", "Receipts Canary")
        self.write_state(state)
        self.commit("state")
        self.queue = root / "queue"
        self.ledger = root / "verifier" / "ledger.jsonl"
        self.registry = root / "verifier" / "checks.json"
        self.registry.parent.mkdir(parents=True)
        self.set_registry({"value-is-ok": {"argv": [sys.executable, "-B", "-c", CHECK], "expect_exit": 0}})

    def git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True,
                              text=True, encoding="utf-8").stdout.strip()

    def write_state(self, text: str) -> None:
        (self.repo / "state.txt").write_text(text + "\n", encoding="utf-8")

    def commit(self, message: str) -> str:
        self.git("add", "-A")
        self.git("commit", "-qm", message)
        return self.git("rev-parse", "HEAD")

    def set_registry(self, checks: dict[str, Any]) -> None:
        self.registry.write_text(json.dumps({"schema": "lh-checks-registry/v1", "checks": checks}), encoding="utf-8")

    def request(self, *, task: str = "T1", check: str = "value-is-ok", nonce: str = "n1") -> tuple[int, dict[str, Any]]:
        return run(RECEIPTS, "request", "--queue", str(self.queue), "--task", task, "--session", "S1",
                   "--check", check, "--nonce", nonce)

    def serve(self) -> tuple[int, dict[str, Any]]:
        return run(RECEIPTS, "serve", "--queue", str(self.queue), "--registry", str(self.registry),
                   "--repo", str(self.repo), "--ledger", str(self.ledger))

    def verdict(self, *, task: str = "T1", check: str = "value-is-ok", nonce: str | None = None) -> tuple[int, dict[str, Any]]:
        args = ["verdict", "--ledger", str(self.ledger), "--task", task, "--check", check]
        if nonce is not None:
            args += ["--nonce", nonce]
        return run(RECEIPTS, *args)

    def receipts(self) -> list[dict[str, Any]]:
        if not self.ledger.is_file():
            return []
        return [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines() if line.strip()]


def c1_no_command(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    smuggled = root / "smuggled.json"
    smuggled.write_text(json.dumps({"task": "T1", "session": "S1", "check": "value-is-ok", "nonce": "n1",
                                    "argv": ["rm", "-rf", "/"]}), encoding="utf-8")
    via_file = run(RECEIPTS, "request", "--queue", str(fx.queue), "--file", str(smuggled))
    # A request written straight into the queue, bypassing the command line.
    (fx.queue / "requests").mkdir(parents=True, exist_ok=True)
    (fx.queue / "requests" / "direct.json").write_text(json.dumps(
        {"task": "T1", "session": "S1", "check": "value-is-ok", "nonce": "n9", "command": "echo hi"}), encoding="utf-8")
    fx.request(check="not-registered", nonce="n2")
    served = fx.serve()
    answers = {row.get("reason") for row in served[1].get("answered", []) if row.get("status") == "rejected"}
    ok = (via_file[0] != 0 and via_file[1].get("reason") == "request_field_forbidden"
          and {"request_field_forbidden", "check_unregistered"} <= answers and fx.receipts() == [])
    return case("a-request-cannot-name-a-command", ok,
                {"via_file": via_file[1].get("reason"), "rejections": sorted(answers), "receipts": len(fx.receipts())})


def c2_receipt(root: Path) -> dict[str, Any]:
    good = Fixture(root / "good")
    good.request()
    good.serve()
    passing = good.verdict(nonce="n1")
    bad = Fixture(root / "bad", state="bad")
    bad.request()
    bad.serve()
    failing = bad.verdict(nonce="n1")
    receipt = (good.receipts() or [{}])[0]
    fields = {"task", "session", "check", "nonce", "exit_code", "ok", "commit", "registry_digest",
              "output_digest", "prev_digest", "receipt_digest"}
    ok = (passing[0] == 0 and fields <= set(receipt) and receipt.get("ok") is True
          and failing[0] != 0 and (bad.receipts() or [{}])[0].get("ok") is False)
    return case("a-receipt-exists-only-if-the-check-ran", ok,
                {"passing": passing[1].get("status"), "failing": failing[1].get("reason"),
                 "fields_missing": sorted(fields - set(receipt))})


def c3_snapshot(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    head = fx.git("rev-parse", "HEAD")
    fx.request()
    fx.write_state("bad")  # an uncommitted edit after the request
    fx.serve()
    receipt = (fx.receipts() or [{}])[0]
    ok = receipt.get("ok") is True and receipt.get("commit") == head
    return case("the-check-runs-on-a-pinned-snapshot", ok, {"receipt_ok": receipt.get("ok"),
                                                            "commit": receipt.get("commit"), "head": head})


def c4_registry(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.request(nonce="n1")
    fx.serve()
    first_digest = "sha256:" + hashlib.sha256(fx.registry.read_bytes()).hexdigest()
    fx.set_registry({"value-is-ok": {"argv": [sys.executable, "-B", "-c", CHECK + " and True"], "expect_exit": 0}})
    second_digest = "sha256:" + hashlib.sha256(fx.registry.read_bytes()).hexdigest()
    fx.request(nonce="n2")
    fx.serve()
    receipts = fx.receipts()
    ok = (len(receipts) == 2 and receipts[0].get("registry_digest") == first_digest
          and receipts[1].get("registry_digest") == second_digest and first_digest != second_digest)
    return case("the-registry-is-bound-into-the-receipt", ok,
                [row.get("registry_digest") for row in receipts])


def c5_chain(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.request(nonce="n1")
    fx.serve()
    fx.request(nonce="n2")
    fx.serve()
    lines = fx.ledger.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    row["ok"] = not row["ok"]
    body = {key: value for key, value in row.items() if key != "receipt_digest"}
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    row["receipt_digest"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    lines[0] = json.dumps(row, sort_keys=True)
    fx.ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    edited = fx.verdict(nonce="n2")
    return case("the-ledger-is-hash-chained", edited[0] != 0 and edited[1].get("reason") == "chain_broken", edited[1])


def c6_nonce(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.request(nonce="n1")
    fx.serve()
    again = fx.serve()
    other = fx.verdict(nonce="n-other")
    ok = len(fx.receipts()) == 1 and other[0] != 0 and other[1].get("reason") == "no_receipt" \
        and not again[1].get("answered")
    return case("the-nonce-binds-the-request-and-replays-do-not-rerun", ok,
                {"receipts": len(fx.receipts()), "other_nonce": other[1].get("reason"),
                 "second_serve": again[1].get("answered")})


def c7_no_receipt(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    claimed = fx.verdict(task="T-claimed")
    return case("a-progress-claim-without-a-receipt-is-not-progress",
                claimed[0] != 0 and claimed[1].get("reason") == "no_receipt", claimed[1])


def c8_principal(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.request()
    fx.serve()
    result = fx.verdict(nonce="n1")
    ok = result[1].get("principal_separated") is False and bool(result[1].get("cannot_claim"))
    return case("principal-separation-is-measured-not-assumed", ok,
                {"principal_separated": result[1].get("principal_separated"),
                 "cannot_claim": result[1].get("cannot_claim")})


def c9_timeline(root: Path) -> dict[str, Any]:
    root.mkdir(parents=True)
    path = str(root / "timeline.jsonl")
    early_accept = run(TIMELINE, "record", "--timeline", path, "--task", "T1", "--kind", "acceptance", "--ref", "nothing")
    verification = run(TIMELINE, "record", "--timeline", path, "--task", "T1", "--kind", "verification",
                       "--ref", "receipt:abc")
    early_promote = run(TIMELINE, "record", "--timeline", path, "--task", "T1", "--kind", "promotion",
                        "--ref", verification[1].get("event_id", ""))
    projected = run(TIMELINE, "project", "--timeline", path, "--task", "T1")[1]
    acceptance = run(TIMELINE, "record", "--timeline", path, "--task", "T1", "--kind", "acceptance",
                     "--ref", verification[1].get("event_id", ""))
    promotion = run(TIMELINE, "record", "--timeline", path, "--task", "T1", "--kind", "promotion",
                    "--ref", acceptance[1].get("event_id", ""))
    final = run(TIMELINE, "project", "--timeline", path, "--task", "T1")[1]
    ok = (early_accept[0] != 0 and early_accept[1].get("reason") == "acceptance_requires_verification"
          and early_promote[0] != 0 and early_promote[1].get("reason") == "promotion_requires_acceptance"
          and projected.get("verification") is not None and projected.get("acceptance") is None
          and projected.get("promotion") is None and acceptance[0] == 0 and promotion[0] == 0
          and all(final.get(kind) is not None for kind in ("verification", "acceptance", "promotion")))
    return case("verification-acceptance-and-promotion-stay-separate", ok,
                {"early_accept": early_accept[1].get("reason"), "early_promote": early_promote[1].get("reason"),
                 "after_verification": projected, "final": {k: bool(final.get(k)) for k in
                                                            ("verification", "acceptance", "promotion")}})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="progress-receipts-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("a-request-cannot-name-a-command", lambda: c1_no_command(root / "c1")),
            guarded("a-receipt-exists-only-if-the-check-ran", lambda: c2_receipt(root / "c2")),
            guarded("the-check-runs-on-a-pinned-snapshot", lambda: c3_snapshot(root / "c3")),
            guarded("the-registry-is-bound-into-the-receipt", lambda: c4_registry(root / "c4")),
            guarded("the-ledger-is-hash-chained", lambda: c5_chain(root / "c5")),
            guarded("the-nonce-binds-the-request-and-replays-do-not-rerun", lambda: c6_nonce(root / "c6")),
            guarded("a-progress-claim-without-a-receipt-is-not-progress", lambda: c7_no_receipt(root / "c7")),
            guarded("principal-separation-is-measured-not-assumed", lambda: c8_principal(root / "c8")),
            guarded("verification-acceptance-and-promotion-stay-separate", lambda: c9_timeline(root / "c9")),
        ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures],
                      "known_gaps_open": ["role separation is only real when requester and verifier run as "
                                          "different principals; the tool measures and reports it"]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

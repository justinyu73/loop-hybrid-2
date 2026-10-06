#!/usr/bin/env python3
"""Advancement canary: an already-green check is not advancement.

Receipts are produced by the real progress-receipts tool against a throwaway
repository: a check is red at the baseline commit and green (or still red) at
the closing commit.  The advancement tool then judges an assignment that fixed
its baseline and closing receipts before the work was judged.
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
TOOL = HERE / "advancement.py"
RECEIPTS = HERE.parent / "progress_receipts" / "receipts.py"
CHECK_ID = "advancement"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def run(script: Path, *args: str) -> tuple[int, dict[str, Any]]:
    if not script.is_file():
        raise FileNotFoundError(f"{script.name} is not provided")
    done = subprocess.run([sys.executable, "-B", str(script), *args], capture_output=True, text=True,
                          encoding="utf-8", timeout=120)
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError:
        return done.returncode, {"unparsed": done.stdout[-300:], "stderr": done.stderr[-300:]}


def check_program(name: str) -> list[str]:
    return [sys.executable, "-B", "-c",
            f"from pathlib import Path; assert Path('{name}.txt').read_text(encoding='utf-8').strip() == 'done'"]


class Fixture:
    """A repository whose checks a verifier runs into a real receipt ledger."""

    def __init__(self, root: Path):
        self.root = root
        self.repo = root / "repo"
        self.repo.mkdir(parents=True)
        for args in (("init", "-q"), ("config", "user.email", "adv@example.invalid"),
                     ("config", "user.name", "Advancement Canary")):
            self.git(*args)
        self.queue, self.ledger = root / "queue", root / "verifier" / "ledger.jsonl"
        self.registry = root / "verifier" / "checks.json"
        self.registry.parent.mkdir(parents=True)
        self.registry.write_text(json.dumps({"schema": "lh-checks-registry/v1", "checks": {
            "feature-a": {"argv": check_program("a"), "expect_exit": 0},
            "feature-b": {"argv": check_program("b"), "expect_exit": 0},
            "unrelated": {"argv": [sys.executable, "-B", "-c", "pass"], "expect_exit": 0}}}), encoding="utf-8")
        self.nonce = 0

    def git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True,
                              text=True, encoding="utf-8").stdout.strip()

    def state(self, **values: str) -> None:
        for name, text in values.items():
            (self.repo / f"{name}.txt").write_text(text + "\n", encoding="utf-8")
        self.git("add", "-A")
        self.git("commit", "-qm", "state", "--allow-empty")

    def receipt(self, check: str, task: str = "T1") -> str:
        self.nonce += 1
        nonce = f"n{self.nonce}"
        run(RECEIPTS, "request", "--queue", str(self.queue), "--task", task, "--session", "S1",
            "--check", check, "--nonce", nonce)
        run(RECEIPTS, "serve", "--queue", str(self.queue), "--registry", str(self.registry),
            "--repo", str(self.repo), "--ledger", str(self.ledger))
        rows = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
        return next(row["receipt_digest"] for row in reversed(rows) if row["nonce"] == nonce)

    def evaluate(self, criteria: list[dict[str, str]], task: str = "T1") -> tuple[int, dict[str, Any]]:
        assignment = self.root / f"assignment-{self.nonce}-{len(criteria)}.json"
        assignment.write_text(json.dumps({"schema": "lh-advancement-assignment/v1", "task": task,
                                          "criteria": criteria}), encoding="utf-8")
        return run(TOOL, "evaluate", "--ledger", str(self.ledger), "--assignment", str(assignment))


def crit(cid: str, check: str, baseline: str, closing: str) -> dict[str, str]:
    return {"id": cid, "check": check, "baseline_receipt": baseline, "closing_receipt": closing}


def c1_already_green(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="done")
    baseline = fx.receipt("feature-a")
    fx.state(a="done", other="touched")
    closing = fx.receipt("feature-a")
    code, result = fx.evaluate([crit("A", "feature-a", baseline, closing)])
    verdicts = [row.get("verdict") for row in result.get("criteria", [])]
    return case("an-already-green-baseline-is-not-advancement",
                code != 0 and result.get("status") == "not_advanced" and verdicts == ["already_green"], result)


def c2_red_to_green(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="todo")
    baseline = fx.receipt("feature-a")
    fx.state(a="done")
    closing = fx.receipt("feature-a")
    code, result = fx.evaluate([crit("A", "feature-a", baseline, closing)])
    return case("red-to-green-is-advancement", code == 0 and result.get("status") == "advanced", result)


def c3_assigned_only(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="todo")
    baseline = fx.receipt("feature-a")
    unrelated = fx.receipt("unrelated")  # a worker-chosen green check
    code, result = fx.evaluate([crit("A", "feature-a", baseline, unrelated)])
    return case("only-the-assigned-check-counts",
                code != 0 and result.get("status") == "refused" and result.get("reason") == "receipt_check_mismatch",
                result)


def c4_unknown(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="todo")
    baseline = fx.receipt("feature-a")
    forged = "sha256:" + hashlib.sha256(b"not a receipt").hexdigest()
    code, result = fx.evaluate([crit("A", "feature-a", baseline, forged)])
    return case("an-unknown-receipt-is-refused",
                code != 0 and result.get("reason") == "receipt_unknown", result)


def c5_every_criterion(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="todo", b="todo")
    base_a, base_b = fx.receipt("feature-a"), fx.receipt("feature-b")
    fx.state(a="done", b="todo")
    close_a, close_b = fx.receipt("feature-a"), fx.receipt("feature-b")
    code, result = fx.evaluate([crit("A", "feature-a", base_a, close_a), crit("B", "feature-b", base_b, close_b)])
    verdicts = {row.get("id"): row.get("verdict") for row in result.get("criteria", [])}
    return case("every-criterion-must-advance",
                code != 0 and result.get("status") == "not_advanced"
                and verdicts == {"A": "advanced", "B": "still_red"}, result)


def c6_order(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="done")
    early = fx.receipt("feature-a")
    fx.state(a="todo")
    late = fx.receipt("feature-a")
    # The assignment names the later receipt as its baseline.
    code, result = fx.evaluate([crit("A", "feature-a", late, early)])
    return case("the-baseline-must-precede-the-closing-receipt",
                code != 0 and result.get("reason") == "receipt_order_invalid", result)


def c7_rewritten(root: Path) -> dict[str, Any]:
    fx = Fixture(root)
    fx.state(a="todo")
    baseline = fx.receipt("feature-a")
    fx.state(a="done")
    closing = fx.receipt("feature-a")
    lines = fx.ledger.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    row["ok"] = True
    lines[0] = json.dumps(row, sort_keys=True)
    fx.ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    code, result = fx.evaluate([crit("A", "feature-a", baseline, closing)])
    return case("a-rewritten-ledger-is-refused", code != 0 and result.get("reason") == "chain_broken", result)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="advancement-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("an-already-green-baseline-is-not-advancement", lambda: c1_already_green(root / "c1")),
            guarded("red-to-green-is-advancement", lambda: c2_red_to_green(root / "c2")),
            guarded("only-the-assigned-check-counts", lambda: c3_assigned_only(root / "c3")),
            guarded("an-unknown-receipt-is-refused", lambda: c4_unknown(root / "c4")),
            guarded("every-criterion-must-advance", lambda: c5_every_criterion(root / "c5")),
            guarded("the-baseline-must-precede-the-closing-receipt", lambda: c6_order(root / "c6")),
            guarded("a-rewritten-ledger-is-refused", lambda: c7_rewritten(root / "c7")),
        ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Provider-free proof that a hanging verifier is hard-time-bounded."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from controller import LoopController
from run_store import RunStore


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def _model(workspace: Path, capsule: dict) -> dict:
    (workspace / "bounded.txt").write_text(f"attempt {capsule['attempt']}\n", encoding="utf-8")
    return {"summary": "timeout canary model"}


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source = root / "source"
        source.mkdir()
        _git("init", "-q", str(source))
        _git("-C", str(source), "config", "user.email", "timeout@example.invalid")
        _git("-C", str(source), "config", "user.name", "Timeout Canary")
        (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
        _git("-C", str(source), "add", "baseline.txt")
        _git("-C", str(source), "commit", "-qm", "baseline")
        base = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()

        store = RunStore(root / "runs")
        # Budget and sleep are sized so the proof is wall-clock-independent:
        # the verifier sleeps far longer than the budget, and the assertions
        # only require (a) the attempt converges to retry with the timeout
        # exit code and (b) the tick returns well before the sleep would end.
        budget_seconds = 2.0
        sleep_seconds = 10
        controller = LoopController(store, root / "workspaces", timeout_seconds=budget_seconds)
        run_id = store.create_run(goal={"case": "hanging-verifier"}, source_repo=source, base_revision=base, run_id="run-timeout")
        started = time.monotonic()
        result = controller.tick(
            run_id,
            holder="timeout-canary",
            model=_model,
            verifier_argv=[sys.executable, "-c", f"import time; time.sleep({sleep_seconds})"],
        )
        elapsed = time.monotonic() - started
        run = store.get_run(run_id)
        receipt_meta = store.latest_receipt(run_id)
        receipt = json.loads((store.root / receipt_meta["receipt_ref"]).read_text(encoding="utf-8")) if receipt_meta else {}
        cases = [
            {
                "id": "verifier-timeout-converges-to-retry",
                "ok": result.get("status") == "retry_pending" and run["state"] == "retry_pending" and receipt.get("verification", {}).get("exit_code") == 124,
                "detail": {"result": result, "run_state": run["state"], "exit_code": receipt.get("verification", {}).get("exit_code")},
            },
            {
                "id": "hanging-verifier-does-not-block-indefinitely",
                "ok": elapsed < sleep_seconds - 2,
                "detail": {"elapsed_seconds": round(elapsed, 3), "budget_seconds": budget_seconds, "sleep_seconds": sleep_seconds},
            },
        ]
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "lh-attempt-timeout", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "cases": cases}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

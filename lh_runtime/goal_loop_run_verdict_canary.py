#!/usr/bin/env python3
"""Production-entry proof for restart poll-resume before a not_holder exit."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from _fixture import make_campaign, make_source_repo
from external_verdict import VerdictStore
from run_store import RunStore


def _campaign() -> dict:
    return make_campaign("campaign-a3")


def _fake_factory(*, timeout_seconds: float = 900):
    def model(_workspace: Path, _capsule: dict) -> dict:
        return {"summary": f"unused fake executor ({timeout_seconds})"}
    return model


def _seed(root: Path, source: Path, base: str) -> int:
    runs = RunStore(root / "runs")
    run_id = runs.create_run(goal={"goal_id": "a3-parked"}, source_repo=source, base_revision=base, run_id="run-a3-parked")
    ordinal = runs.begin_attempt(run_id, "workspace://a3/1")
    verdicts = VerdictStore(root / "verdict.sqlite3")
    verdicts.park(run_id, "op-a3", {"request": {"case": "restart"}}, at=1.0)
    runs.park_external_verdict(run_id, ordinal, receipt_ref="missing-receipt", receipt_digest="sha256:a3")
    return 0


def _resume(root: Path, source: Path, base: str) -> int:
    from goal_loop_run import run
    verdicts = VerdictStore(root / "verdict.sqlite3")

    def not_holder_driver(_worker, **_kwargs):
        return {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}

    result = run(
            executor="fake",
            execute=True,
            goal_store_root=root / "goals",
            run_store_root=root / "runs",
            workspace_root=root / "workspaces",
            campaign=_campaign(),
            source_repo=source,
            base_revision=base,
            executor_timeout_seconds=0.25,
            verdict_store=verdicts,
            conclusion_source=lambda op_key: {"conclusion": "success"} if op_key == "op-a3" else None,
            factory_overrides={"fake": _fake_factory},
            driver_fn=not_holder_driver,
        )
    (root / "result.json").write_text(json.dumps(result, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        mode, root, source, base = sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5]
        return _seed(root, source, base) if mode == "seed" else _resume(root, source, base)

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        source, base = make_source_repo(root)
        seed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", "seed", str(root), str(source), base], capture_output=True, text=True)
        resume = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", "resume", str(root), str(source), base], capture_output=True, text=True)
        result = json.loads((root / "result.json").read_text(encoding="utf-8")) if (root / "result.json").exists() else {}
        runs = RunStore(root / "runs")
        verdicts = VerdictStore(root / "verdict.sqlite3")
        run_id = "run-a3-parked"
        final_run = runs.get_run(run_id)
        verdict_state = verdicts.state(run_id)
        cases = [
            {
                "id": "production-entry-polls-before-not-holder-exit",
                "ok": seed.returncode == 0 and resume.returncode == 0 and result.get("startup_external_resumed") == [{"run_id": run_id, "op_key": "op-a3", "conclusion": "success", "state": "verified"}] and final_run["state"] == "verified" and verdict_state == {"state": "verified", "conclusion": "success"},
                "detail": {"seed_exit": seed.returncode, "resume_exit": resume.returncode, "startup_external_resumed": result.get("startup_external_resumed"), "run_state": final_run["state"], "verdict_state": verdict_state},
            },
            {
                "id": "not-holder-is-retryable-host-outcome",
                "ok": result.get("driver", {}).get("stop_reason") == "not_holder" and final_run["state"] != "stopped",
                "detail": {"driver": result.get("driver"), "run_state": final_run["state"]},
            },
        ]
    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({"check_id": "lh-production-verdict-resume", "status": "pass" if not failures else "fail", "total": len(cases), "blocking_failures": failures, "cases": cases}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

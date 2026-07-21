#!/usr/bin/env python3
"""W9b proof: CLI executor flags coexist with a contract's models section.

Before the fix, passing --executor while the contract also declared
models.execute crashed with "multiple values for keyword argument 'executor'"
(the binding pop only ran when the flag was absent). Flag must win when both
are present; contract value applies when the flag is absent. Dry-run only —
no executor is constructed.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import goal_loop_run


def case(case_id: str, ok: bool, detail: str) -> dict:
    return {"id": case_id, "ok": ok, "detail": detail}


def _contract(root: Path) -> Path:
    (root / "goals").mkdir()
    (root / "runs").mkdir()
    (root / "ws").mkdir()
    campaign = {
        "schema": "lh-campaign/v1",
        "campaign_id": "cli-flags",
        "stages": [{
            "stage_id": "s1",
            "goal": {"feature_contract": "s1"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": "l", "smoke": "s", "verification_argv": ["true"]},
            "max_attempts": 1,
            "next_stage_id": None,
        }],
    }
    contract = {
        "schema": "lh-project-runtime-contract/v1",
        "project_id": "cli-flags",
        "campaign": campaign,
        "source_repo": str(root),
        "base_revision": "main",
        "runtime": {"goal_store": str(root / "goals"), "run_store": str(root / "runs"), "workspace_root": str(root / "ws")},
        "models": {"execute": "codex", "judge": "kimi", "judge_model": "kimi-code/k3"},
    }
    path = root / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def _plan(argv: list[str]) -> dict:
    import contextlib
    import io
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = goal_loop_run.main(argv)
    assert rc == 0, f"main returned {rc}"
    return json.loads(out.getvalue())["plan"]


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        contract = _contract(Path(raw))
        both = _plan(["--contract", str(contract), "--executor", "claude", "--judge-executor", "claude", "--judge-model", "claude-x"])
        contract_only = _plan(["--contract", str(contract)])
        cases = [
            case("flag-and-contract-coexist-flag-wins",
                 both["executor"] == "claude" and both["judge_executor"] == "claude" and both["judge_model"] == "claude-x",
                 json.dumps({"executor": both["executor"], "judge": both["judge_executor"]})),
            case("contract-models-applies-without-flags",
                 contract_only["executor"] == "codex" and contract_only["judge_executor"] == "kimi" and contract_only["judge_model"] == "kimi-code/k3",
                 json.dumps({"executor": contract_only["executor"], "judge": contract_only["judge_executor"]})),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({"check_id": "lh-cli-executor-flags", "status": "pass" if not failures else "fail",
                      "total": len(cases), "blocking_failures": failures,
                      "known_gaps_open": []}, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

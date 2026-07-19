#!/usr/bin/env python3
"""Deterministic threshold canary for the standalone quota gate."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quota_monitor as qm  # noqa: E402


HERE = Path(__file__).resolve().parent
NOW = "2026-07-12T12:00:00Z"


def response(used: int) -> dict:
    return {"result": {"rateLimits": {"limitId": "codex", "planType": "test",
            "primary": {"usedPercent": used, "windowDurationMins": 300, "resetsAt": 1783850000}}}}


def main() -> int:
    policy = qm.load_json(HERE / "quota_policy.example.json")
    expected = {59: "quota_normal", 60: "quota_notify", 80: "quota_soft_degradation", 100: "quota_hard_stopped"}
    observed = {used: qm.observe(policy, runner="codex", evaluated_at=NOW,
                                  app_server_reader=lambda used=used: response(used))["status"]
                for used in expected}
    ok = not qm.validate_policy(policy) and observed == expected
    print(f"quota: {'PASS' if ok else 'FAIL'} {observed}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

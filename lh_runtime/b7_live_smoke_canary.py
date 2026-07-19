#!/usr/bin/env python3
"""Offline gate and optional live smoke for the first GitHub adapter."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from github_conclusion_source import GitHubConclusionSource  # noqa: E402


# Live smoke target owner is host configuration, read from the environment with
# no default baked in. When LH_LIVE_SMOKE_OWNER is unset the live (--execute)
# path skips and records a known gap; the offline dry-run is unaffected.
OWNER = os.environ.get("LH_LIVE_SMOKE_OWNER", "").strip()
# Placeholder owner for the offline canned fixtures only; never a real account.
FIXTURE_OWNER = "example-owner"
REPO = "loop-hybrid"
WORKFLOW = "CI"
DEFAULT_SHA = "8e8620d2fabb5783241ed8aa3e81af0e4270b276"


class FakeTransport:
    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.calls.append((url, headers, timeout))
        return json.dumps(self.payload, sort_keys=True).encode("utf-8")


def _source(payload: dict[str, Any]) -> tuple[GitHubConclusionSource, FakeTransport]:
    transport = FakeTransport(payload)
    source = GitHubConclusionSource(
        FIXTURE_OWNER,
        REPO,
        WORKFLOW,
        "offline-fixture-token",
        sha_resolver=lambda _op: DEFAULT_SHA,
        transport=transport,
    )
    return source, transport


def _run_canned(conclusion: str) -> tuple[dict[str, str] | None, FakeTransport]:
    source, transport = _source(
        {
            "workflow_runs": [
                {
                    "name": WORKFLOW,
                    "head_sha": DEFAULT_SHA,
                    "status": "completed",
                    "conclusion": conclusion,
                    "created_at": "2026-07-18T00:00:00Z",
                }
            ]
        }
    )
    return source("b7-live-smoke"), transport


def _case(case_id: str, ok: bool, detail: object) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _dry_run() -> int:
    success, success_transport = _run_canned("success")
    failure, failure_transport = _run_canned("failure")
    success_expected = {"operation_key": "b7-live-smoke", "conclusion": "success"}
    failure_expected = {"operation_key": "b7-live-smoke", "conclusion": "failure"}
    success_request = success_transport.calls[0] if success_transport.calls else None
    failure_request = failure_transport.calls[0] if failure_transport.calls else None
    cases = [
        _case("canned-success-verdict", success == success_expected, success),
        _case("canned-failure-verdict", failure == failure_expected, failure),
        _case(
            "uses-verified-repository-workflow-sha",
            bool(success_request)
            and success_request[0].startswith(f"https://api.github.com/repos/{FIXTURE_OWNER}/{REPO}/actions/runs?")
            and success_request[2] == 10.0
            and bool(failure_request),
            {"success_request": success_request, "failure_request": failure_request},
        ),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    result = {
        "check_id": "lh-b7-live-smoke",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _execute(sha: str) -> int:
    if not OWNER:
        result = {
            "check_id": "lh-b7-live-smoke",
            "status": "skip",
            "mode": "execute",
            "known_gaps_open": [
                {
                    "id": "live-smoke-owner-unset",
                    "detail": "LH_LIVE_SMOKE_OWNER is not set; live smoke skipped. "
                              "Offline behaviour (--dry-run) is unaffected.",
                }
            ],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    source = GitHubConclusionSource.from_env(
        owner=OWNER,
        repo=REPO,
        workflow=WORKFLOW,
        sha_resolver=lambda _op: sha,
    )
    result = source("b7-live-smoke")
    if result != {"operation_key": "b7-live-smoke", "conclusion": "success"}:
        raise AssertionError(f"expected completed CI success for {sha}, got {result!r}")
    print(json.dumps({"check_id": "lh-b7-live-smoke", "status": "pass", "mode": "execute", "sha": sha, "result": result}, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the offline or live B7 GitHub conclusion smoke")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--sha", default=None)
    args = parser.parse_args(argv)
    if not args.execute:
        return _dry_run()
    sha = (args.sha or os.environ.get("LH_B7_SMOKE_SHA") or DEFAULT_SHA).strip()
    if not sha:
        raise ValueError("a non-empty SHA is required")
    return _execute(sha)


if __name__ == "__main__":
    raise SystemExit(main())

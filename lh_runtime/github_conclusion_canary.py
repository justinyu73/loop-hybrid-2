#!/usr/bin/env python3
"""Provider-free canary for the read-only GitHub Actions conclusion source."""

from __future__ import annotations

import json
import sys
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from typing import Any

HERE = __import__("pathlib").Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from github_conclusion_source import (  # noqa: E402
    GitHubAdapterError,
    GitHubConclusionSource,
    GitHubCredentialsMissing,
    GitHubResponseInvalid,
)


class FakeTransport:
    def __init__(self, payload: Any = None, error: BaseException | None = None):
        self.payload = payload
        self.error = error
        self.requests: list[tuple[str, dict[str, str], float]] = []

    def __call__(self, url: str, headers: dict[str, str], timeout: float) -> bytes:
        self.requests.append((url, headers, timeout))
        if self.error is not None:
            raise self.error
        if isinstance(self.payload, bytes):
            return self.payload
        return json.dumps(self.payload, sort_keys=True).encode("utf-8")


def run_case(case_id: str, fn) -> dict[str, Any]:
    try:
        detail = fn()
        ok = bool(detail[0])
        rendered = detail[1]
    except Exception as exc:  # a canary case must report, not hide, an unexpected failure
        ok = False
        rendered = f"unexpected {type(exc).__name__}: {exc}"
    return {"id": case_id, "ok": ok, "detail": rendered}


def source(payload: Any, *, resolver=lambda _op: "sha-1", transport: FakeTransport | None = None) -> tuple[GitHubConclusionSource, FakeTransport]:
    fake = transport or FakeTransport(payload)
    return (
        GitHubConclusionSource(
            "octo-org",
            "fixture-repo",
            "build",
            "fixture-token",
            resolver,
            api_root="https://api.github.invalid",
            timeout=4.5,
            transport=fake,
        ),
        fake,
    )


def run(workflow_run: dict[str, Any], *, workflow: str = "build", resolver=lambda _op: "sha-1") -> tuple[dict[str, str] | None, FakeTransport]:
    adapter, transport = source({"workflow_runs": [workflow_run]}, resolver=resolver)
    return adapter("op-1"), transport


def main() -> int:
    success, success_transport = run({"name": "build", "head_sha": "sha-1", "status": "completed", "conclusion": "success", "created_at": "2026-07-18T01:00:00Z"})
    failure, _ = run({"name": "build", "head_sha": "sha-1", "status": "completed", "conclusion": "failure", "created_at": "2026-07-18T01:00:00Z"})
    timed_out, _ = run({"name": "build", "head_sha": "sha-1", "status": "completed", "conclusion": "timed_out", "created_at": "2026-07-18T01:00:00Z"})
    pending, _ = run({"name": "build", "head_sha": "sha-1", "status": "in_progress", "conclusion": None, "created_at": "2026-07-18T01:00:00Z"})
    no_sha_match, _ = run({"name": "build", "head_sha": "other-sha", "status": "completed", "conclusion": "success", "created_at": "2026-07-18T01:00:00Z"})
    latest, _ = source({
        "workflow_runs": [
            {"name": "build", "head_sha": "sha-1", "status": "completed", "conclusion": "success", "created_at": "2026-07-17T01:00:00Z"},
            {"name": "build", "head_sha": "sha-1", "status": "completed", "conclusion": "failure", "created_at": "2026-07-18T01:00:00Z"},
        ]
    })[0]("op-1"), None
    wrong_workflow, _ = run({"name": "test", "head_sha": "sha-1", "status": "completed", "conclusion": "success", "created_at": "2026-07-18T01:00:00Z"})

    def invalid_sha() -> tuple[bool, str]:
        adapter, _ = source({"workflow_runs": []}, resolver=lambda _op: None)
        try:
            adapter("op-1")
        except GitHubResponseInvalid as exc:
            return True, type(exc).__name__
        return False, "accepted missing SHA"

    def missing_token() -> tuple[bool, str]:
        try:
            GitHubConclusionSource.from_env("octo-org", "fixture-repo", "build", lambda _op: "sha-1", {})
        except GitHubCredentialsMissing as exc:
            return True, type(exc).__name__
        return False, "accepted missing token"

    def http_error() -> tuple[bool, str]:
        transport = FakeTransport(error=HTTPError("https://api.github.invalid", 500, "fixture", {}, None))
        adapter, _ = source({}, transport=transport)
        try:
            adapter("op-1")
        except GitHubAdapterError as exc:
            return True, type(exc).__name__
        return False, "accepted HTTP error as verdict"

    def invalid_json() -> tuple[bool, str]:
        adapter, _ = source(b"not-json")
        try:
            adapter("op-1")
        except GitHubResponseInvalid as exc:
            return True, type(exc).__name__
        return False, "accepted invalid JSON"

    def missing_runs() -> tuple[bool, str]:
        adapter, _ = source({})
        try:
            adapter("op-1")
        except GitHubResponseInvalid as exc:
            return True, type(exc).__name__
        return False, "accepted missing workflow_runs"

    def request_contract() -> tuple[bool, str]:
        url, headers, timeout = success_transport.requests[0]
        query = parse_qs(urlsplit(url).query)
        ok = (
            url.startswith("https://api.github.invalid/repos/octo-org/fixture-repo/actions/runs?")
            and query == {"head_sha": ["sha-1"]}
            and headers["Accept"] == "application/vnd.github+json"
            and headers["Authorization"] == "Bearer fixture-token"
            and headers["X-GitHub-Api-Version"] == "2022-11-28"
            and timeout == 4.5
        )
        return ok, json.dumps({"url": url, "timeout": timeout}, sort_keys=True)

    cases = [
        run_case("completed-success-is-success", lambda: (success == {"operation_key": "op-1", "conclusion": "success"}, str(success))),
        run_case("completed-failure-is-failure", lambda: (failure == {"operation_key": "op-1", "conclusion": "failure"}, str(failure))),
        run_case("completed-timed-out-fails-closed", lambda: (timed_out == {"operation_key": "op-1", "conclusion": "failure"}, str(timed_out))),
        run_case("in-progress-is-pending", lambda: (pending is None, str(pending))),
        run_case("head-sha-without-run-is-pending", lambda: (no_sha_match is None, str(no_sha_match))),
        run_case("latest-created-run-wins", lambda: (latest == {"operation_key": "op-1", "conclusion": "failure"}, str(latest))),
        run_case("other-workflow-is-filtered", lambda: (wrong_workflow is None, str(wrong_workflow))),
        run_case("missing-sha-fails-closed", invalid_sha),
        run_case("missing-token-fails-closed", missing_token),
        run_case("HTTP-error-is-adapter-error", http_error),
        run_case("non-JSON-is-response-invalid", invalid_json),
        run_case("missing-workflow-runs-is-response-invalid", missing_runs),
        run_case("request-binds-sha-and-read-only-headers", request_contract),
    ]
    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    result = {
        "check_id": "lh-github-conclusion-source",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

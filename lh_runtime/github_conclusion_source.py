#!/usr/bin/env python3
"""Read-only GitHub Actions conclusion source for the external-verdict seam.

The source performs one GET against the Actions Runs API for the commit SHA
resolved from an operation key.  It returns only a pending result (``None``)
or a normalized terminal verdict; transport and response errors are raised so
they cannot be mistaken for a CI conclusion.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen


class GitHubAdapterError(RuntimeError):
    """GitHub cannot provide a trustworthy conclusion because the source failed."""


class GitHubCredentialsMissing(GitHubAdapterError):
    """The GitHub token required by the read-only source is absent."""


class GitHubResponseInvalid(GitHubAdapterError):
    """The GitHub response is outside the Actions Runs API contract."""


Transport = Callable[[str, Mapping[str, str], float], bytes]
ShaResolver = Callable[[str], str | None]


def _http_transport(url: str, headers: Mapping[str, str], timeout: float) -> bytes:
    request = Request(url, headers=dict(headers), method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read()
    except HTTPError as exc:
        raise GitHubAdapterError(f"GitHub source returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise GitHubAdapterError("GitHub source is unavailable") from exc


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"GitHub {field} must be a non-empty string")
    return value.strip()


def _normalize_response(raw: bytes, *, op_key: str, workflow: str, sha: str) -> dict[str, str] | None:
    if not isinstance(raw, bytes):
        raise GitHubResponseInvalid("GitHub response must be UTF-8 JSON bytes")
    try:
        payload: Any = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GitHubResponseInvalid("GitHub response must be UTF-8 JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("workflow_runs"), list):
        raise GitHubResponseInvalid("GitHub response must contain workflow_runs as a list")

    matching: list[dict[str, Any]] = []
    for run in payload["workflow_runs"]:
        if not isinstance(run, dict):
            raise GitHubResponseInvalid("GitHub workflow_runs entries must be objects")
        for field in ("name", "head_sha", "status", "created_at"):
            if not isinstance(run.get(field), str):
                raise GitHubResponseInvalid(f"GitHub workflow run {field} must be a string")
        if "conclusion" in run and run["conclusion"] is not None and not isinstance(run["conclusion"], str):
            raise GitHubResponseInvalid("GitHub workflow run conclusion must be a string or null")
        if run["name"] == workflow and run["head_sha"] == sha:
            matching.append(run)

    if not matching:
        return None
    latest = max(matching, key=lambda run: run["created_at"])
    if latest["status"] != "completed":
        return None
    conclusion = "success" if latest.get("conclusion") == "success" else "failure"
    return {"operation_key": op_key, "conclusion": conclusion}


class GitHubConclusionSource:
    """Callable ``ConclusionSource`` backed by GitHub Actions Runs API GETs."""

    def __init__(
        self,
        owner: str,
        repo: str,
        workflow: str,
        token: str,
        sha_resolver: ShaResolver,
        *,
        api_root: str = "https://api.github.com",
        timeout: float = 10.0,
        transport: Transport | None = None,
    ):
        self.owner = _require_text(owner, "owner")
        self.repo = _require_text(repo, "repo")
        self.workflow = _require_text(workflow, "workflow")
        if not isinstance(token, str) or not token.strip():
            raise GitHubCredentialsMissing("GitHub token is required")
        if not callable(sha_resolver):
            raise ValueError("GitHub sha_resolver must be callable")
        if not isinstance(api_root, str) or not api_root.strip():
            raise ValueError("GitHub API root must be a non-empty URL")
        parts = urlsplit(api_root.strip())
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise ValueError("GitHub API root must be an HTTP(S) URL")
        if timeout <= 0:
            raise ValueError("GitHub timeout must be positive")
        self.api_root = api_root.strip().rstrip("/")
        self._token = token.strip()
        self._sha_resolver = sha_resolver
        self.timeout = timeout
        self._transport = transport or _http_transport

    @classmethod
    def from_env(
        cls,
        owner: str | None = None,
        repo: str | None = None,
        workflow: str | None = None,
        sha_resolver: ShaResolver | None = None,
        environ: Mapping[str, str] | None = None,
        transport: Transport | None = None,
    ) -> "GitHubConclusionSource":
        values = os.environ if environ is None else environ
        token = values.get("LH_GITHUB_TOKEN", "").strip()
        if not token:
            raise GitHubCredentialsMissing("LH_GITHUB_TOKEN is required")
        if owner is None or repo is None or workflow is None or sha_resolver is None:
            raise ValueError("owner, repo, workflow, and sha_resolver are required")
        api_root = values.get("LH_GITHUB_API_ROOT", "https://api.github.com").strip() or "https://api.github.com"
        raw_timeout = values.get("LH_GITHUB_TIMEOUT_SECONDS", "10")
        try:
            timeout = float(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("LH_GITHUB_TIMEOUT_SECONDS must be numeric") from exc
        return cls(owner, repo, workflow, token, sha_resolver, api_root=api_root, timeout=timeout, transport=transport)

    def _runs_url(self, sha: str) -> str:
        path = f"/repos/{quote(self.owner, safe='')}/{quote(self.repo, safe='')}/actions/runs"
        return f"{self.api_root}{path}?{urlencode({'head_sha': sha})}"

    def __call__(self, op_key: str) -> dict[str, str] | None:
        if not isinstance(op_key, str) or not op_key.strip():
            raise GitHubResponseInvalid("operation_key must be a non-empty string")
        sha = self._sha_resolver(op_key)
        if not isinstance(sha, str) or not sha.strip():
            raise GitHubResponseInvalid("sha_resolver must return a non-empty head_sha")
        sha = sha.strip()
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "loop-hybrid-github-conclusion/1",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            raw = self._transport(self._runs_url(sha), headers, self.timeout)
        except GitHubAdapterError:
            raise
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise GitHubAdapterError("GitHub source is unavailable") from exc
        return _normalize_response(raw, op_key=op_key, workflow=self.workflow, sha=sha)

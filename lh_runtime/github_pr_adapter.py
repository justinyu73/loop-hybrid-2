#!/usr/bin/env python3
"""R1 draft-PR adapter: turn a parked run's diff into a draft PR with evidence.

The async external-verdict path (controller.tick_async) parks a run and hands
this adapter the operation request. The adapter materializes the run's
recorded diff onto a NEW branch inside the ``lh/`` namespace of the target
repo, pushes it, and opens a DRAFT pull request whose body is the full
evidence package (goal/run/attempt, lamp verdict, value verdict, usage, diff
stat) so the daily human review is a seconds-long read.

Credential grant/use split (R1 approval): a human grants one scoped token
(write ``lh/*`` branches + open draft PRs on one repo) via the environment;
every use inside that scope is automatic, anything outside it is an error.
Hard boundaries: pushes go ONLY to ``lh/*`` branches (checked before any git
call); draft PRs only — never merge, close, or delete; the token is never
stored, printed, or written to any artifact, log, or the store; the adapter
is idempotent on op_key — a retry after a crash reuses the existing branch
and PR instead of duplicating them.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import token_cost
import value_reducer
from github_conclusion_source import GitHubCredentialsMissing

BRANCH_NAMESPACE = "lh/"
SECTION_CHAR_CAP = 2000
BODY_CHAR_CAP = 8000
COMMIT_MESSAGE_CAP = 200
PR_TITLE_CAP = 120

# (argv, cwd) -> CompletedProcess; injectable so fixtures can count calls.
GitRunner = Callable[[list[str], Path | None], subprocess.CompletedProcess]
# (method, url, headers, body) -> response bytes; injectable for fixtures.
PrTransport = Callable[[str, str, Mapping[str, str], bytes], bytes]

_REF_FORBIDDEN = re.compile(r"[ ~^:?*\[\\]")


class GitHubPrError(RuntimeError):
    """The draft-PR adapter could not complete the action safely."""


def _default_git(argv: list[str], cwd: Path | None) -> subprocess.CompletedProcess:
    return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=120)


def _http_transport(method: str, url: str, headers: Mapping[str, str], body: bytes) -> bytes:
    request = Request(url, data=body if method != "GET" else None, headers=dict(headers), method=method)
    try:
        with urlopen(request, timeout=30) as response:
            return response.read()
    except HTTPError as exc:
        raise GitHubPrError(f"GitHub API returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise GitHubPrError("GitHub API is unavailable") from exc


def _branch_name(goal_id: str, run_id: str) -> str:
    """Branch inside the lh/ namespace; ref-forbidden characters are folded."""
    safe_goal = _REF_FORBIDDEN.sub("-", goal_id).strip("-.") or "goal"
    return f"{BRANCH_NAMESPACE}{safe_goal}/{run_id[-12:]}"


def _validate_lh_branch(branch: str) -> str:
    """Hard boundary: anything outside the lh/ namespace never reaches git."""
    if not isinstance(branch, str) or not branch.startswith(BRANCH_NAMESPACE) or ".." in branch:
        raise GitHubPrError(f"branch escapes the {BRANCH_NAMESPACE} namespace: {branch!r}")
    proc = subprocess.run(["git", "check-ref-format", "--branch", branch], capture_output=True, text=True)
    if proc.returncode != 0:
        raise GitHubPrError(f"invalid branch name: {branch!r}")
    return branch


def _parse_workspace_ref(workspace_ref: Any) -> tuple[str, int]:
    """request.workspace_ref is 'workspace://<run_id>/<ordinal>' (controller.tick_async)."""
    if not isinstance(workspace_ref, str):
        raise GitHubPrError("workspace_ref must be a string")
    parts = workspace_ref.removeprefix("workspace://").rsplit("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1].isdigit():
        raise GitHubPrError(f"workspace_ref is not in workspace://<run_id>/<ordinal> shape: {workspace_ref!r}")
    return parts[0], int(parts[1])


def _bounded(text: Any, cap: int = SECTION_CHAR_CAP) -> str:
    if not isinstance(text, str) or not text:
        return "(none recorded)"
    if len(text) <= cap:
        return text
    return text[:cap] + f"\n... [truncated {len(text) - cap} chars]"


def _diff_stat(diff_text: str) -> str:
    files = additions = deletions = 0
    for line in diff_text.splitlines():
        if line.startswith("diff --git "):
            files += 1
        elif line.startswith("+") and not line.startswith("+++"):
            additions += 1
        elif line.startswith("-") and not line.startswith("---"):
            deletions += 1
    return f"{files} file(s), +{additions}/-{deletions} line(s)"


class GitHubPrAdapter:
    """ExternalAdapter: push an lh/* branch and open a draft PR with evidence."""

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        base_branch: str,
        run_store: Any,
        environ: Mapping[str, str] | None = None,
        remote_url: str | None = None,
        api_root: str = "https://api.github.com",
        git_runner: GitRunner | None = None,
        transport: PrTransport | None = None,
    ):
        values = os.environ if environ is None else environ
        token = values.get("LH_GITHUB_TOKEN", "").strip()
        if not token:
            # Credential grant is human-held: without it the adapter raises
            # before ANY git or API call — the loop degrades to producing
            # artifacts/PR requests instead of crashing mid-action.
            raise GitHubCredentialsMissing("LH_GITHUB_TOKEN is required for the draft-PR adapter")
        self._token = token
        for name, value in (("owner", owner), ("repo", repo), ("base_branch", base_branch)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"GitHub {name} must be a non-empty string")
        self.owner = owner.strip()
        self.repo = repo.strip()
        self.base_branch = base_branch.strip()
        self.run_store = run_store
        self.remote = remote_url or f"https://github.com/{self.owner}/{self.repo}.git"
        self.api_root = api_root.rstrip("/")
        self._git_runner = git_runner or _default_git
        self._transport = transport or _http_transport

    # -- git helpers ---------------------------------------------------------

    def _git(self, argv: list[str], cwd: Path | None) -> subprocess.CompletedProcess:
        proc = self._git_runner(argv, cwd)
        if proc.returncode != 0:
            raise GitHubPrError(f"git {argv[0]} failed: {str(proc.stderr).strip()[:400]}")
        return proc

    def _remote_branch_sha(self, branch: str) -> str | None:
        proc = self._git(["git", "ls-remote", self.remote, f"refs/heads/{branch}"], None)
        line = proc.stdout.strip()
        return line.split()[0] if line else None

    def _materialize_and_push(self, workspace: Path, branch: str, *, goal_id: str, run_id: str, ordinal: int, diff_text: str) -> str:
        self._git(["git", "clone", "--quiet", self.remote, "repo"], workspace)
        repo = workspace / "repo"
        self._git(["git", "checkout", "--quiet", self.base_branch], repo)
        self._git(["git", "checkout", "--quiet", "-b", branch], repo)
        patch = workspace / "change.patch"
        patch.write_text(diff_text, encoding="utf-8")
        self._git(["git", "apply", str(patch)], repo)
        self._git(["git", "add", "-A"], repo)
        message = f"lh: {goal_id} (run {run_id}, attempt {ordinal})"[:COMMIT_MESSAGE_CAP]
        self._git(["git", "-c", "user.name=loop-hybrid", "-c", "user.email=loop-hybrid@localhost",
                   "commit", "--quiet", "-m", message], repo)
        head_sha = self._git(["git", "rev-parse", "HEAD"], repo).stdout.strip()
        self._git(["git", "push", "--quiet", "origin", branch], repo)
        return head_sha

    # -- evidence package ------------------------------------------------------

    def _read_artifact(self, run_id: str, ordinal: int, name: str) -> str | None:
        try:
            return (self.run_store.artifacts / run_id / str(ordinal) / name).read_text(encoding="utf-8")
        except OSError:
            return None

    def _evidence_body(self, *, goal_id: str, run_id: str, ordinal: int, base_revision: str, diff_text: str) -> str:
        receipt: dict[str, Any] = {}
        raw = self._read_artifact(run_id, ordinal, "receipt.json")
        if raw is not None:
            try:
                parsed = json.loads(raw)
                receipt = parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                receipt = {}
        verification = receipt.get("verification") if isinstance(receipt.get("verification"), dict) else {}
        lamp_argv = verification.get("argv") if isinstance(verification.get("argv"), list) else None
        exit_code = verification.get("exit_code")
        stdout_tail = self._read_artifact(run_id, ordinal, "verifier.stdout")
        stderr_tail = self._read_artifact(run_id, ordinal, "verifier.stderr")
        verdict = value_reducer.verdict_for_run(self.run_store, run_id)
        usage = receipt.get("usage") if isinstance(receipt.get("usage"), dict) else None
        if usage is not None and usage.get("state") == token_cost.USAGE_MEASURED:
            cost = token_cost.compute_cost(usage)
            usage_text = (
                f"input {usage.get('input_tokens', 0)} + cache_read {usage.get('cache_read_tokens', 0)} "
                f"+ output {usage.get('output_tokens', 0)} tokens; estimated cost ${cost.get('cost_usd', 0.0):.4f} (estimated)"
            )
        elif usage is not None:
            usage_text = f"unknown ({usage.get('reason', 'no reason recorded')})"
        else:
            usage_text = "(none recorded)"
        lamp_text = (
            f"argv: {json.dumps(lamp_argv)}\nexit_code: {exit_code}"
            if lamp_argv is not None or exit_code is not None
            else "not run locally (async path — CI is the lamp)"
        )
        verdict_text = verdict["verdict"]
        if verdict["reasons"]:
            verdict_text += "\n" + "\n".join(f"- {reason}" for reason in verdict["reasons"])
        sections = [
            f"## Evidence package\n\n- goal: `{goal_id}`\n- run: `{run_id}` attempt {ordinal}\n- base revision: `{base_revision}`",
            f"## Acceptance lamp\n{lamp_text}",
            f"### verifier stdout (tail)\n{_bounded(stdout_tail)}",
            f"### verifier stderr (tail)\n{_bounded(stderr_tail)}",
            f"## Value verdict\n{verdict_text}",
            f"## Usage\n{usage_text}",
            f"## Diff stat\n{_diff_stat(diff_text)}",
        ]
        body = "\n\n".join(sections)
        return body[:BODY_CHAR_CAP]

    # -- PR open/reuse ----------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "loop-hybrid-draft-pr/1",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        }

    def _api(self, method: str, path: str, payload: dict[str, Any] | None) -> Any:
        body = json.dumps(payload, sort_keys=True).encode("utf-8") if payload is not None else b""
        raw = self._transport(method, f"{self.api_root}{path}", self._headers(), body)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GitHubPrError("GitHub API response is not JSON") from exc

    def _find_or_open_pr(self, branch: str, *, title: str, body: str) -> dict[str, Any]:
        # Idempotent on retry: reuse the existing open PR for this branch.
        query = f"head={quote(self.owner)}:{quote(branch, safe='')}&state=open"
        existing = self._api("GET", f"/repos/{quote(self.owner, safe='')}/{quote(self.repo, safe='')}/pulls?{query}", None)
        if isinstance(existing, list) and existing:
            pr = existing[0]
            return {"number": pr.get("number"), "url": pr.get("html_url"), "reused": True}
        created = self._api("POST", f"/repos/{quote(self.owner, safe='')}/{quote(self.repo, safe='')}/pulls",
                            {"title": title, "head": branch, "base": self.base_branch, "body": body, "draft": True})
        if not isinstance(created, dict) or not isinstance(created.get("number"), int):
            raise GitHubPrError("GitHub PR create returned an unexpected shape")
        return {"number": created["number"], "url": created.get("html_url"), "reused": False}

    # -- the ExternalAdapter contract -------------------------------------------

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Materialize the run's diff onto an lh/* branch and open a draft PR.

        Returns the record the VerdictStore parks (W4's sha_resolver reads
        ``external.head_sha`` from it). Idempotent: an existing remote branch
        is not re-pushed and an existing open PR is reused."""
        if not isinstance(request, dict):
            raise GitHubPrError("request must be an object")
        run_id, ordinal = _parse_workspace_ref(request.get("workspace_ref"))
        diff_text = self._read_artifact(run_id, ordinal, "diff.patch")
        if diff_text is None:
            raise GitHubPrError(f"diff artifact is missing for {run_id} attempt {ordinal}")
        expected_digest = request.get("diff_digest")
        actual_digest = "sha256:" + hashlib.sha256(diff_text.encode()).hexdigest()
        if isinstance(expected_digest, str) and expected_digest != actual_digest:
            raise GitHubPrError(f"diff digest mismatch: request {expected_digest} vs artifact {actual_digest}")
        run = self.run_store.get_run(run_id)
        goal = run["goal"] if isinstance(run.get("goal"), dict) else {}
        goal_id = str(goal.get("goal_id") or "goal")
        branch = _validate_lh_branch(_branch_name(goal_id, run_id))

        head_sha = self._remote_branch_sha(branch)
        if head_sha is None:
            with tempfile.TemporaryDirectory(prefix="lh-pr-") as raw:
                head_sha = self._materialize_and_push(Path(raw), branch, goal_id=goal_id, run_id=run_id, ordinal=ordinal, diff_text=diff_text)
        title = f"[lh] {goal_id}: run {run_id[-12:]} (attempt {ordinal})"[:PR_TITLE_CAP]
        body = self._evidence_body(goal_id=goal_id, run_id=run_id, ordinal=ordinal,
                                   base_revision=str(run.get("base_revision") or ""), diff_text=diff_text)
        pr = self._find_or_open_pr(branch, title=title, body=body)
        return {"head_sha": head_sha, "pr_url": pr["url"], "pr_number": pr["number"], "branch": branch}

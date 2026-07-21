#!/usr/bin/env python3
"""Committed R1 smoke: draft-PR adapter + evidence packaging.

Proves, offline (a local bare repo as the git remote and a fixture HTTP
transport), that the adapter materializes a parked run's diff onto an lh/*
branch, pushes it, and opens a draft PR whose body carries the full evidence
chain — that retries reuse the branch and PR instead of duplicating them,
that a branch outside the lh/ namespace and a missing token both error
before any git or API call, that the token leaks into no output, and that
the returned head_sha is the pushed commit a W4 sha_resolver would read from
the parked VerdictStore action record. No network, no real GitHub.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import external_action_port as eap
import external_verdict as ev
import goal_loop_run as glr
from _fixture import make_campaign, make_source_repo
from github_pr_adapter import GitHubPrAdapter, GitHubPrError, _validate_lh_branch
from github_conclusion_source import GitHubCredentialsMissing
from project_binding import CONTRACT_SCHEMA, resolve_project
from run_store import RunStore

TOKEN = "r1-fixture-token"
GOAL_ID = "campaign-r1:stage-pr"
LAMP_ARGV = ["sh", "-c", "grep -q lh-r1 src/out.txt"]
DIFF_TEXT = (
    "diff --git a/src/out.txt b/src/out.txt\n"
    "new file mode 100644\n"
    "index 0000000..3be9c81\n"
    "--- /dev/null\n"
    "+++ b/src/out.txt\n"
    "@@ -0,0 +1 @@\n"
    "+lh-r1\n"
)


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def _make_remote(root: Path) -> Path:
    remote = root / "remote.git"
    _git("init", "-q", "--bare", "--initial-branch=master", str(remote))
    seed = root / "seed"
    _git("clone", "-q", str(remote), str(seed))
    _git("-C", str(seed), "config", "user.email", "r1@example.invalid")
    _git("-C", str(seed), "config", "user.name", "R1 Canary")
    (seed / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    _git("-C", str(seed), "add", "baseline.txt")
    _git("-C", str(seed), "commit", "-qm", "baseline")
    _git("-C", str(seed), "push", "-q", "origin", "master")
    return remote


class FixtureTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict, bytes]] = []
        self.prs: list[dict[str, Any]] = []

    def __call__(self, method: str, url: str, headers: dict, body: bytes) -> bytes:
        self.calls.append((method, url, dict(headers), body))
        if method == "GET":
            return json.dumps(self.prs).encode("utf-8")
        payload = json.loads(body.decode("utf-8"))
        pr = {"number": 42, "html_url": "https://github.invalid/o/r/pull/42",
              "draft": payload.get("draft"), "head": payload.get("head"),
              "base": payload.get("base"), "title": payload.get("title"), "body": payload.get("body")}
        if not any(existing["number"] == 42 for existing in self.prs):
            self.prs.append(pr)
        return json.dumps(pr).encode("utf-8")


def _counting_git(calls: list[list[str]]):
    def runner(argv: list[str], cwd: Path | None) -> subprocess.CompletedProcess:
        calls.append(argv)
        return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=120)
    return runner


def _seed_store(root: Path, *, stdout: str = "lamp ok\n", stderr: str = "") -> tuple[RunStore, str, str]:
    store = RunStore(root / "runs")
    run_id = "run-r1-parked"
    goal = {"goal_id": GOAL_ID, "admission_envelope": {
        "allowed_paths": ["src/"],
        "acceptance_lamp": {"id": "lamp", "smoke": "marker", "verification_argv": LAMP_ARGV},
    }}
    store.create_run(goal=goal, source_repo=root, base_revision="base-r1", run_id=run_id)
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", DIFF_TEXT)
    stdout_ref = store.write_artifact(run_id, ordinal, "verifier.stdout", stdout)
    stderr_ref = store.write_artifact(run_id, ordinal, "verifier.stderr", stderr)
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
        "usage": {"state": "measured", "model": "gpt-5.6-luna", "input_tokens": 100, "output_tokens": 10, "cache_read_tokens": 0},
        "diff": diff_ref,
        "verification": {"argv": LAMP_ARGV, "exit_code": 0, "stdout": stdout_ref, "stderr": stderr_ref},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])
    return store, run_id, diff_ref["digest"]


def _request(run_id: str, diff_digest: str) -> dict[str, Any]:
    return {"diff_digest": diff_digest, "workspace_ref": f"workspace://{run_id}/1", "action_id": "open-pr"}


def _write_contract(root: Path, source: Path, base: str, *, adapter: Any) -> Path:
    contract = {
        "schema": CONTRACT_SCHEMA,
        "project_id": "r1-project",
        "campaign": make_campaign("campaign-r1c"),
        "source_repo": str(source),
        "base_revision": base,
        "runtime": {"goal_store": "rt/goals", "run_store": "rt/runs", "workspace_root": "rt/ws"},
        "external_verdict": {"owner": "o", "repo": "r", "workflow": "CI", "adapter": adapter},
    }
    path = root / "contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        remote = _make_remote(root)
        store, run_id, diff_digest = _seed_store(root)
        git_calls: list[list[str]] = []
        transport = FixtureTransport()
        adapter = GitHubPrAdapter(
            owner="o", repo="r", base_branch="master", run_store=store,
            environ={"LH_GITHUB_TOKEN": TOKEN}, remote_url=str(remote),
            api_root="https://api.github.invalid",
            git_runner=_counting_git(git_calls), transport=transport,
        )
        op_key = "op-r1-1"
        first = adapter.perform(op_key, _request(run_id, diff_digest))
        second = adapter.perform(op_key, _request(run_id, diff_digest))
        push_calls = [argv for argv in git_calls if argv[:2] == ["git", "push"]]
        post_calls = [call for call in transport.calls if call[0] == "POST"]

        # Inspect the pushed branch in a fresh clone.
        check = root / "check"
        _git("clone", "-q", "--branch", first["branch"], str(remote), str(check))
        applied = (check / "src" / "out.txt").read_text(encoding="utf-8") if (check / "src" / "out.txt").exists() else ""
        remote_sha = subprocess.run(["git", "ls-remote", str(remote), f"refs/heads/{first['branch']}"],
                                    check=True, capture_output=True, text=True).stdout.split()[0]

        pr_body = transport.prs[0]["body"] if transport.prs else ""

        # Ledger + parked verdict record: what W4's sha_resolver reads back.
        verdicts = ev.VerdictStore(root / "runs" / "verdict.sqlite3")
        ledger = eap.ActionLedger(root / "runs" / "action-ledger.sqlite3")
        sent = ev.dispatch_external(verdicts, ledger, adapter, run_id=run_id, op_key=op_key, request=_request(run_id, diff_digest), at=1.0)
        parked = verdicts.action_for_op_key(op_key)

        # Token hygiene: scan every written file under the store root.
        leaks = [str(path.relative_to(root)) for path in (root / "runs").rglob("*")
                 if path.is_file() and TOKEN.encode() in path.read_bytes()]

        # Missing token: construction raises before any git or API call.
        missing_error = None
        try:
            GitHubPrAdapter(owner="o", repo="r", base_branch="master", run_store=store, environ={},
                            remote_url=str(remote), git_runner=_counting_git([]), transport=FixtureTransport())
        except GitHubCredentialsMissing as exc:
            missing_error = type(exc).__name__

        # Branch namespace guard.
        escape_error = None
        try:
            _validate_lh_branch("../evil")
        except GitHubPrError as exc:
            escape_error = str(exc)
        valid_branch = _validate_lh_branch("lh/ok/x")

        # Long stdout: the evidence body stays bounded.
        store_long, run_long, digest_long = _seed_store(root / "long", stdout="x" * 5000)
        adapter_long = GitHubPrAdapter(owner="o", repo="r", base_branch="master", run_store=store_long,
                                       environ={"LH_GITHUB_TOKEN": TOKEN}, remote_url=str(remote),
                                       api_root="https://api.github.invalid", transport=FixtureTransport())
        long_body = adapter_long._evidence_body(goal_id=GOAL_ID, run_id=run_long, ordinal=1,
                                                base_revision="base-r1", diff_text=DIFF_TEXT)

        # Contract adapter block validation.
        contract_source, contract_base = make_source_repo(root / "contract")
        declared = resolve_project(_write_contract(root, contract_source, contract_base, adapter={"type": "github_pr", "owner": "o", "repo": "r", "base_branch": "master"}))["run_kwargs"]
        bad_type = None
        try:
            resolve_project(_write_contract(root, contract_source, contract_base, adapter={"type": "merge_bot", "owner": "o", "repo": "r", "base_branch": "master"}))
        except SystemExit as exc:
            bad_type = str(exc)

        # run() wires ledger + adapter into the worker; absent block stays None.
        captured: dict[str, Any] = {}

        def capture_driver(worker: Any, **_kwargs: Any) -> dict[str, Any]:
            captured["ledger"] = worker.action_ledger is not None
            captured["adapter"] = worker.external_adapter is not None
            return {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}

        wired_source, wired_base = make_source_repo(root / "wired")
        glr.run(
            executor="fake", execute=True,
            goal_store_root=root / "wired-goals", run_store_root=root / "wired-runs", workspace_root=root / "wired-ws",
            campaign=make_campaign("campaign-r1w"), source_repo=wired_source, base_revision=wired_base,
            executor_timeout_seconds=0.25,
            github_verdict={"owner": "o", "repo": "r", "workflow": "CI"},
            github_pr_adapter={"owner": "o", "repo": "r", "base_branch": "master"},
            github_environ={"LH_GITHUB_TOKEN": TOKEN},
            factory_overrides={"fake": lambda *, timeout_seconds=900: (lambda _ws, _cap: {"summary": "unused"})},
            driver_fn=capture_driver,
        )
        plain_worker = glr.build_worker(
            goal_store_root=root / "plain-goals", run_store_root=root / "plain-runs", workspace_root=root / "plain-ws",
            campaign=make_campaign("campaign-r1p"), source_repo=wired_source, base_revision=wired_base,
        )

        cases = [
            {"id": "perform-pushes-lh-branch-with-diff-applied",
             "ok": first["branch"].startswith("lh/") and applied == "lh-r1\n" and first["head_sha"] == remote_sha,
             "detail": json.dumps({"branch": first["branch"], "applied": applied.strip(), "sha_match": first["head_sha"] == remote_sha})},
            {"id": "draft-pr-opened-with-evidence-body",
             "ok": len(post_calls) == 1
             and transport.prs[0]["draft"] is True and transport.prs[0]["head"] == first["branch"]
             and transport.prs[0]["base"] == "master"
             and "exit_code: 0" in pr_body and "GREEN" in pr_body
             and "input 100" in pr_body and "1 file(s), +1/-0" in pr_body
             and GOAL_ID in pr_body and run_id in pr_body and "base-r1" in pr_body,
             "detail": json.dumps({"draft": transport.prs[0]["draft"], "body_len": len(pr_body)})},
            {"id": "evidence-body-is-bounded",
             "ok": "truncated" in long_body and len(long_body) <= 8000,
             "detail": json.dumps({"body_len": len(long_body)})},
            {"id": "op-key-retry-reuses-branch-and-pr",
             "ok": len(push_calls) == 1 and len(post_calls) == 1
             and second["pr_number"] == first["pr_number"] and second["head_sha"] == first["head_sha"],
             "detail": json.dumps({"pushes": len(push_calls), "posts": len(post_calls)})},
            {"id": "ledger-and-verdict-store-see-one-action",
             "ok": sent["deduped"] is False and parked is not None
             and parked["external"]["head_sha"] == remote_sha,
             "detail": json.dumps({"deduped": sent["deduped"], "parked_sha": (parked or {}).get("external", {}).get("head_sha", "")[:12]})},
            {"id": "branch-outside-namespace-errors",
             "ok": escape_error is not None and "namespace" in escape_error and valid_branch == "lh/ok/x",
             "detail": json.dumps({"error": escape_error})},
            {"id": "missing-token-errors-before-any-call",
             "ok": missing_error == "GitHubCredentialsMissing",
             "detail": json.dumps({"error": missing_error})},
            {"id": "token-appears-nowhere-in-outputs",
             "ok": not leaks and TOKEN not in pr_body,
             "detail": json.dumps({"leaks": leaks, "in_body": TOKEN in pr_body})},
            {"id": "contract-adapter-block-validated",
             "ok": declared.get("github_pr_adapter") == {"owner": "o", "repo": "r", "base_branch": "master"}
             and bad_type is not None and "github_pr" in bad_type,
             "detail": json.dumps({"declared": declared.get("github_pr_adapter"), "error": bad_type})},
            {"id": "run-wires-ledger-and-adapter-into-worker",
             "ok": captured.get("ledger") is True and captured.get("adapter") is True
             and plain_worker.action_ledger is None and plain_worker.external_adapter is None,
             "detail": json.dumps({"wired": captured, "plain": [plain_worker.action_ledger, plain_worker.external_adapter]})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-github-pr-adapter",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/github_pr_adapter_canary.py",
                         "fixtures": "local bare repo as remote + fixture HTTP transport; no network, no real credentials"},
        "known_gaps_open": [
            "the live GitHub smoke (real repo, real draft PR, human review/merge) is a separate node per the R/S execution order",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

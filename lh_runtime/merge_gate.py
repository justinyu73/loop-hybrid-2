#!/usr/bin/env python3
"""B13 conditional auto-merge: the merge gate for engine-produced draft PRs.

The R1 adapter's output stops at a draft PR. This gate is the B13 exception:
for a repo whose Project Runtime Contract declares ``auto_merge: true``, an
engine-produced draft PR may be squash-merged by the loop when EVERY
condition below holds. Any miss returns ``{"merged": False, "reason": ...}``
and does nothing — the PR simply stays for the daily human review (the
existing human route; no retry storm, because a resolved run is not polled
again).

Conditions, in evaluation order:

1. a parked external action exists for the run (pr_number, branch, head_sha);
2. the external verdict conclusion is exactly ``success`` (CI green);
3. value_reducer's verdict for the run is GREEN (authority-surface diffs are
   already RED there);
4. the diff artifact grades ``routine`` in diff_grader's closed set;
5. branch protection on the base branch is VERIFIED via the API — the GET
   must parse and carry required status checks; an HTTP error or an
   unprotected branch routes to a human (protection is only ever read,
   never modified);
6. the durable trust ramp is at level for (repo, goal_type): N=3 human-merge
   events inside a 90-day window, recorded after the latest revocation.

Credential grant/use split (mirrors R1, with a SEPARATE token): a human
grants one merge-only scoped token via ``LH_GITHUB_MERGE_TOKEN``; a missing
token raises MergeCredentialsMissing at CONSTRUCTION — no token, no gate.
``LH_AUTO_MERGE_DISABLE`` is the operator's kill switch: when set, every
evaluation routes to a human before any API call. The token is never stored,
printed, or written to any artifact, log, or the store.

Idempotency and audit: the merge goes through eap.dispatch with an
operation_key derived from (run_id, pr_number, head_sha), so a crash-retry
never double-merges; the adapter-style perform is itself idempotent (a
GET-first check returns an already-merged PR instead of merging again). The
ledger record and the return value carry pr_number, head_sha,
merge_commit_sha, and a conditions snapshot, so the full chain is readable
by a human after the fact.

Trust ramp evidence is human merges: a resolved run whose PR is found merged
on poll WITHOUT a merge record in this gate's ledger was merged by a human
and records one durable ramp event (deterministic, no model). Engine merges
never feed the ramp — that would be self-reinforcing. Revocation is
human-held via lh_runtime/merge_trust.py; the engine never revokes.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import diff_grader
import external_action_port as eap
import value_reducer


class _ClosingConnection(sqlite3.Connection):
    """Keep the transaction context contract while closing on context exit."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


MERGE_TOKEN_ENV = "LH_GITHUB_MERGE_TOKEN"
KILL_SWITCH_ENV = "LH_AUTO_MERGE_DISABLE"
RAMP_WINDOW_SECONDS = 90 * 86400
RAMP_REQUIRED = 3

# (method, url, headers, body) -> response bytes; injectable for fixtures.
MergeTransport = Callable[[str, str, Mapping[str, str], bytes], bytes]


class MergeGateError(RuntimeError):
    """The merge gate could not complete the action safely."""


class MergeCredentialsMissing(MergeGateError):
    """The merge-scoped token required by the gate is absent."""


def _http_transport(method: str, url: str, headers: Mapping[str, str], body: bytes) -> bytes:
    request = Request(url, data=body if method != "GET" else None, headers=dict(headers), method=method)
    try:
        with urlopen(request, timeout=30) as response:
            return response.read()
    except HTTPError as exc:
        raise MergeGateError(f"GitHub API returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise MergeGateError("GitHub API is unavailable") from exc


def merge_op_key(run_id: str, pr_number: int, head_sha: str) -> str:
    """The deterministic at-most-once key for one logical merge action."""
    return eap.operation_key(run_id, "merge-pr", {"pr_number": pr_number, "head_sha": head_sha})


def record_human_merge(ramp_store: "TrustRampStore", *, repo: str, goal_type: str, run_id: str, pr_number: int, at: float) -> dict[str, Any]:
    """Record one human merge as durable ramp evidence (idempotent per run+PR)."""
    inserted = ramp_store.record(repo, goal_type, run_id, pr_number, at=at)
    return {"inserted": inserted, "repo": repo, "goal_type": goal_type, "run_id": run_id, "pr_number": pr_number, "recorded_at": at}


class TrustRampStore:
    """Durable, append-only trust-ramp evidence and revocations.

    Lives next to action-ledger.sqlite3 under the run store root. Events are
    (repo, goal_type) scoped human merges; ``at_level`` counts only events
    inside the decay window AND recorded after the latest revocation for the
    pair, so expired evidence does not count and revocation takes effect on
    the very next evaluation.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ramp_events ("
                "repo TEXT NOT NULL, goal_type TEXT NOT NULL, run_id TEXT NOT NULL, "
                "pr_number INTEGER NOT NULL, recorded_at REAL NOT NULL, "
                "UNIQUE(repo, goal_type, run_id, pr_number))"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ramp_revocations ("
                "repo TEXT NOT NULL, goal_type TEXT NOT NULL, reason TEXT NOT NULL, recorded_at REAL NOT NULL)"
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, factory=_ClosingConnection)
        conn.row_factory = sqlite3.Row
        return conn

    def record(self, repo: str, goal_type: str, run_id: str, pr_number: int, *, at: float) -> bool:
        """Append one ramp event; returns True when it is new (dedupe by run+PR)."""
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO ramp_events VALUES (?, ?, ?, ?, ?)",
                (repo, goal_type, run_id, pr_number, at),
            )
            conn.execute("COMMIT")
            return cursor.rowcount == 1

    def revoke(self, repo: str, goal_type: str, reason: str, *, at: float) -> None:
        """Append a revocation; events recorded before it stop counting."""
        with self._connect() as conn:
            conn.execute("INSERT INTO ramp_revocations VALUES (?, ?, ?, ?)", (repo, goal_type, reason, at))
            conn.execute("COMMIT")

    def _latest_revocation(self, conn: sqlite3.Connection, repo: str, goal_type: str) -> float:
        row = conn.execute(
            "SELECT MAX(recorded_at) AS latest FROM ramp_revocations WHERE repo = ? AND goal_type = ?",
            (repo, goal_type),
        ).fetchone()
        return float(row["latest"]) if row is not None and row["latest"] is not None else 0.0

    def count(self, repo: str, goal_type: str, *, now: float, window_seconds: float = RAMP_WINDOW_SECONDS) -> int:
        """Events for (repo, goal_type) inside the window and after the latest revocation."""
        with self._connect() as conn:
            floor = self._latest_revocation(conn, repo, goal_type)
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM ramp_events WHERE repo = ? AND goal_type = ? AND recorded_at >= ? AND recorded_at > ?",
                (repo, goal_type, now - window_seconds, floor),
            ).fetchone()
        return int(row["n"])

    def at_level(self, repo: str, goal_type: str, *, now: float, window_seconds: float = RAMP_WINDOW_SECONDS, required: int = RAMP_REQUIRED) -> bool:
        return self.count(repo, goal_type, now=now, window_seconds=window_seconds) >= required


class MergeGate:
    """Conditional auto-merge for one allowlisted repo's engine draft PRs.

    Structurally satisfies eap.ExternalAdapter: perform() is idempotent on
    op_key, so eap.dispatch gives the at-most-once merge guarantee.
    """

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        base_branch: str,
        run_store: Any,
        verdict_store: Any,
        ledger: eap.ActionLedger,
        ramp_store: TrustRampStore,
        goal_store: Any = None,
        judge: diff_grader.GraderRunner | None = None,
        environ: Mapping[str, str] | None = None,
        api_root: str = "https://api.github.com",
        transport: MergeTransport | None = None,
    ):
        values = os.environ if environ is None else environ
        token = values.get(MERGE_TOKEN_ENV, "").strip()
        if not token:
            # Credential grant is human-held: without it the gate raises
            # before ANY API call — the loop keeps producing draft PRs for
            # human review instead of crashing mid-action.
            raise MergeCredentialsMissing(f"{MERGE_TOKEN_ENV} is required for the merge gate")
        self._token = token
        # Kill switch: checked before any condition or API call.
        self._disabled = values.get(KILL_SWITCH_ENV, "").strip().lower() in {"1", "true", "yes"}
        for name, value in (("owner", owner), ("repo", repo), ("base_branch", base_branch)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"GitHub {name} must be a non-empty string")
        self.owner = owner.strip()
        self.repo = repo.strip()
        self.base_branch = base_branch.strip()
        self.repo_key = f"{self.owner}/{self.repo}"
        self.run_store = run_store
        self.verdict_store = verdict_store
        self.ledger = ledger
        self.ramp_store = ramp_store
        self.goal_store = goal_store
        self._judge = judge
        self.api_root = api_root.rstrip("/")
        self._transport = transport or _http_transport
        self._o = quote(self.owner, safe="")
        self._r = quote(self.repo, safe="")

    # -- GitHub API helpers (mirror github_pr_adapter) -------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "loop-hybrid-merge-gate/1",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        }

    def _api(self, method: str, path: str, payload: dict[str, Any] | None) -> Any:
        body = json.dumps(payload, sort_keys=True).encode("utf-8") if payload is not None else b""
        raw = self._transport(method, f"{self.api_root}{path}", self._headers(), body)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MergeGateError("GitHub API response is not JSON") from exc

    # -- run evidence helpers ---------------------------------------------------

    def _goal_type(self, run_id: str) -> str:
        """f"{campaign_id}:{stage_id}" from the run's goal; goal_id as fallback."""
        try:
            goal = self.run_store.get_run(run_id).get("goal") or {}
        except (KeyError, TypeError):
            return "unknown"
        campaign_id, stage_id = goal.get("campaign_id"), goal.get("stage_id")
        if isinstance(campaign_id, str) and campaign_id.strip() and isinstance(stage_id, str) and stage_id.strip():
            return f"{campaign_id.strip()}:{stage_id.strip()}"
        goal_id = goal.get("goal_id")
        return goal_id.strip() if isinstance(goal_id, str) and goal_id.strip() else "unknown"

    def _run_diff(self, run_id: str) -> str | None:
        """The run's diff artifact, read the same way value_reducer reads it."""
        latest = self.run_store.latest_receipt(run_id)
        if not latest or not latest.get("receipt_ref"):
            return None
        try:
            receipt = json.loads((self.run_store.root / latest["receipt_ref"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        ref = receipt.get("diff")
        ref = ref.get("ref") if isinstance(ref, dict) else ref
        if not isinstance(ref, str) or not ref:
            return None
        try:
            return (self.run_store.root / ref).read_text(encoding="utf-8")
        except OSError:
            return None

    def _parked_pr(self, run_id: str) -> tuple[int, str, str | None] | None:
        """(pr_number, head_sha, branch) from the parked action record, or None."""
        action = self.verdict_store.action_for_run(run_id)
        external = action.get("external") if isinstance(action, dict) else None
        if not isinstance(external, dict):
            return None
        pr_number, head_sha = external.get("pr_number"), external.get("head_sha")
        if not isinstance(pr_number, int) or not isinstance(head_sha, str) or not head_sha.strip():
            return None
        branch = external.get("branch")
        return pr_number, head_sha.strip(), branch if isinstance(branch, str) else None

    def _protection_verified(self) -> bool:
        """Branch protection must parse AND carry required status checks."""
        try:
            protection = self._api("GET", f"/repos/{self._o}/{self._r}/branches/{quote(self.base_branch, safe='')}/protection", None)
        except MergeGateError:
            return False
        if not isinstance(protection, dict):
            return False
        checks = protection.get("required_status_checks")
        if not isinstance(checks, dict):
            return False
        contexts = checks.get("contexts")
        listed = checks.get("checks")
        return bool((isinstance(contexts, list) and contexts) or (isinstance(listed, list) and listed))

    # -- the ExternalAdapter contract (idempotent merge) ------------------------

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Squash-merge the PR, idempotently: a GET-first check means an
        already-merged PR (crash-retry, or a human merged it) returns the
        existing result instead of a second merge call."""
        pr_number, head_sha = request["pr_number"], request["head_sha"]
        pr = self._api("GET", f"/repos/{self._o}/{self._r}/pulls/{pr_number}", None)
        if not isinstance(pr, dict):
            raise MergeGateError("PR lookup returned an unexpected shape")
        if pr.get("merged") is True:
            return {"merged": True, "already_merged": True, "pr_number": pr_number, "head_sha": head_sha,
                    "merge_commit_sha": pr.get("merge_commit_sha"), "conditions": request.get("conditions")}
        result = self._api("PUT", f"/repos/{self._o}/{self._r}/pulls/{pr_number}/merge",
                           {"merge_method": "squash", "sha": head_sha})
        if not isinstance(result, dict) or result.get("merged") is not True:
            raise MergeGateError("the merge call was not accepted")
        return {"merged": True, "already_merged": False, "pr_number": pr_number, "head_sha": head_sha,
                "merge_commit_sha": result.get("sha"), "conditions": request.get("conditions")}

    # -- the gate ----------------------------------------------------------------

    def evaluate_and_merge(self, run_id: str, *, at: float) -> dict[str, Any]:
        """Merge the run's PR when every B13 condition holds; else do nothing."""
        refused: dict[str, Any] = {"merged": False, "run_id": run_id}
        if self._disabled:
            return {**refused, "reason": f"auto-merge disabled via {KILL_SWITCH_ENV}; the PR stays for human review"}
        verdict = self.verdict_store.state(run_id)
        if verdict is None:
            return {**refused, "reason": "no parked external action for this run"}
        parked = self._parked_pr(run_id)
        if parked is None:
            return {**refused, "reason": "the parked action record is missing pr_number/head_sha"}
        pr_number, head_sha, branch = parked
        refused = {**refused, "pr_number": pr_number, "head_sha": head_sha}
        if verdict.get("conclusion") != "success":
            return {**refused, "reason": f"external conclusion is {verdict.get('conclusion')!r}, not success"}
        value = value_reducer.value_evidence_for_run(
            self.run_store, run_id, goal_store=self.goal_store,
        )
        if value.get("verdict") != "GREEN":
            return {**refused, "reason": "value verdict is not GREEN", "value_reasons": value.get("reasons", [])}
        grade = diff_grader.grade_diff(
            self._run_diff(run_id),
            judge=self._judge,
            context={"run_id": run_id},
        )
        if grade["grade"] != "routine":
            return {**refused, "reason": f"diff grade is {grade['grade']!r} ({grade['source']})", "grade": grade}
        if not self._protection_verified():
            return {**refused, "reason": "branch protection is not verified for the base branch", "grade": grade}
        goal_type = self._goal_type(run_id)
        ramp_count = self.ramp_store.count(self.repo_key, goal_type, now=at)
        if not self.ramp_store.at_level(self.repo_key, goal_type, now=at):
            return {**refused, "grade": grade,
                    "reason": f"trust ramp not at level for {self.repo_key} {goal_type} ({ramp_count}/{RAMP_REQUIRED})"}
        snapshot = {
            "value_verdict": "GREEN",
            "diff_grade": grade["grade"],
            "grade_source": grade["source"],
            "ramp_count": ramp_count,
            "protection_verified": True,
        }
        op_key = merge_op_key(run_id, pr_number, head_sha)
        request = {"run_id": run_id, "pr_number": pr_number, "head_sha": head_sha, "branch": branch, "conditions": snapshot}
        try:
            outcome = eap.dispatch(self.ledger, self, op_key=op_key, request=request, at=at)
        except MergeGateError as exc:
            # A merge conflict or API error routes to a human; the resolved
            # run is not polled again, so this cannot retry-storm.
            return {**refused, "reason": f"merge call failed: {exc}", "conditions": snapshot}
        result = outcome["result"]
        return {
            "merged": result.get("merged") is True,
            "run_id": run_id,
            "reason": "squash-merged" if result.get("merged") is True else "merge was not performed",
            "op_key": op_key,
            "deduped": outcome["deduped"],
            "pr_number": pr_number,
            "head_sha": head_sha,
            "merge_commit_sha": result.get("merge_commit_sha"),
            "already_merged": result.get("already_merged", False),
            "conditions": snapshot,
        }

    # -- trust-ramp observation (human merges are the evidence) -------------------

    def observe_human_merge(self, run_id: str, *, at: float) -> dict[str, Any] | None:
        """A resolved run whose PR is found merged WITHOUT a merge record in
        this gate's ledger was merged by a human: record one durable ramp
        event. Deterministic (no model); API errors are not evidence."""
        parked = self._parked_pr(run_id)
        if parked is None:
            return None
        pr_number, head_sha, _branch = parked
        if self.ledger.get(merge_op_key(run_id, pr_number, head_sha)) is not None:
            return None  # the gate itself merged this PR; engine merges are not ramp evidence
        try:
            pr = self._api("GET", f"/repos/{self._o}/{self._r}/pulls/{pr_number}", None)
        except MergeGateError:
            return None
        if not isinstance(pr, dict) or pr.get("merged") is not True:
            return None
        goal_type = self._goal_type(run_id)
        record = record_human_merge(self.ramp_store, repo=self.repo_key, goal_type=goal_type, run_id=run_id, pr_number=pr_number, at=at)
        return {"run_id": run_id, "pr_number": pr_number, "goal_type": goal_type, "recorded": record["inserted"]}

    def on_poll_resolved(self, rows: list[dict[str, Any]], *, at: float) -> list[dict[str, Any]]:
        """Worker hook: observe every resolved row for human merges, then
        evaluate the verified-with-success ones. A gate error on one row
        routes that PR to a human and must never crash the tick."""
        outcomes: list[dict[str, Any]] = []
        for row in rows:
            run_id = row.get("run_id")
            if not isinstance(run_id, str):
                continue
            entry: dict[str, Any] = {"run_id": run_id, "observed_human_merge": None, "evaluation": None}
            try:
                entry["observed_human_merge"] = self.observe_human_merge(run_id, at=at)
                if row.get("state") == "verified":
                    entry["evaluation"] = self.evaluate_and_merge(run_id, at=at)
            except Exception as exc:  # degrade to human review; the tick continues
                entry["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
            outcomes.append(entry)
        return outcomes


class DeferredMergeGate:
    """Bind merge authority only when a resolved external row needs it."""

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        base_branch: str,
        run_store: Any,
        verdict_store: Any,
        ledger: eap.ActionLedger,
        ramp_store: TrustRampStore,
        goal_store: Any = None,
        judge: diff_grader.GraderRunner | None = None,
        environ: Mapping[str, str] | None = None,
        api_root: str = "https://api.github.com",
        transport: MergeTransport | None = None,
    ):
        self.kwargs = {
            "owner": owner,
            "repo": repo,
            "base_branch": base_branch,
            "run_store": run_store,
            "verdict_store": verdict_store,
            "ledger": ledger,
            "ramp_store": ramp_store,
            "goal_store": goal_store,
            "judge": judge,
            "environ": os.environ if environ is None else environ,
            "api_root": api_root,
            "transport": transport,
        }

    def on_poll_resolved(self, rows: list[dict[str, Any]], *, at: float) -> list[dict[str, Any]]:
        if not rows:
            return []
        try:
            gate = MergeGate(**self.kwargs)
        except MergeCredentialsMissing as exc:
            return [
                {
                    "run_id": row.get("run_id"),
                    "observed_human_merge": None,
                    "evaluation": {
                        "merged": False,
                        "reason": str(exc),
                    },
                }
                for row in rows
                if isinstance(row, dict) and isinstance(row.get("run_id"), str)
            ]
        return gate.on_poll_resolved(rows, at=at)

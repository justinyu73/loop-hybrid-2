#!/usr/bin/env python3
"""Committed B13 acceptance lamp: conditional auto-merge gate.

Proves, offline (a fixture HTTP transport; no network, no real GitHub, no
real credentials), the R2 acceptance lamp from the approved spec: a repo
outside the allowlist's trust evidence does not merge; an authority-surface
diff does not merge; CI not green / unknown does not merge; an op_key retry
never double-merges; the trust ramp is durable, decays after 90 days, and a
revocation returns the next evaluation to human review; a judge anomaly
routes to a human; branch protection must be verified via the API; a missing
merge token raises at construction with zero API calls; the kill switch
routes to a human before any call; a human merge found on poll feeds the
ramp exactly once; and when every condition holds the gate squash-merges
with the sha guard and a readable audit chain.
"""
from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import external_action_port as eap
import external_verdict as ev
import goal_loop_run as glr
import merge_trust
from _fixture import make_campaign, make_source_repo
from merge_gate import (
    MergeCredentialsMissing,
    MergeGate,
    MergeGateError,
    TrustRampStore,
    merge_op_key,
)

TOKEN = "r2-fixture-merge-token"
HEAD_SHA = "head-sha-r2-fixture"
PR_NUMBER = 7
GOAL_TYPE = "campaign-r2:stage-merge"
REPO_KEY = "o/r"
NOW = 200.0 * 86400  # well past the epoch so 100-day-old events stay positive
LAMP_ARGV = ["sh", "-c", "grep -q lh-r2 src/out.txt"]
DIFF_TEXT = (
    "diff --git a/src/out.txt b/src/out.txt\n"
    "new file mode 100644\n"
    "index 0000000..3be9c81\n"
    "--- /dev/null\n"
    "+++ b/src/out.txt\n"
    "@@ -0,0 +1 @@\n"
    "+lh-r2\n"
)
AUTHORITY_DIFF = DIFF_TEXT.replace("src/out.txt", "gate-pack/verify.sh")


class FixtureTransport:
    """Answers branch protection, PR lookup, and merge PUT for any repo."""

    def __init__(self, *, merged: bool = False, protection: Any = ...):
        self.calls: list[tuple[str, str, bytes]] = []
        self.puts: list[dict[str, Any]] = []
        self.pr_merged = merged
        self.merge_commit = "merge-commit-sha-r2"
        # `...` = protected with required checks; None = HTTP 404; any other
        # value is returned verbatim (e.g. protection without status checks).
        self.protection = {"required_status_checks": {"strict": True, "contexts": ["CI"]}} if protection is ... else protection

    def __call__(self, method: str, url: str, headers: dict, body: bytes) -> bytes:
        self.calls.append((method, url, body))
        if url.endswith("/protection"):
            if self.protection is None:
                raise MergeGateError("GitHub API returned HTTP 404")
            return json.dumps(self.protection).encode("utf-8")
        if method == "PUT" and url.endswith("/merge"):
            payload = json.loads(body.decode("utf-8"))
            self.puts.append(payload)
            self.pr_merged = True
            return json.dumps({"merged": True, "sha": self.merge_commit}).encode("utf-8")
        if method == "GET" and "/pulls/" in url:
            return json.dumps({"number": PR_NUMBER, "merged": self.pr_merged,
                               "merge_commit_sha": self.merge_commit if self.pr_merged else None}).encode("utf-8")
        raise MergeGateError(f"unexpected fixture call: {method} {url}")


def _routine_judge(_snapshot: dict) -> dict:
    return {"grade": "routine", "rationale": "fixture routine grade"}


def _seed(root: Path, *, run_id: str = "run-r2-parked", diff_text: str = DIFF_TEXT,
          allowed: tuple = ("src/",), conclusion: str = "success"):
    store_root = root / "runs"
    from run_store import RunStore
    store = RunStore(store_root)
    goal = {"goal_id": GOAL_TYPE, "campaign_id": "campaign-r2", "stage_id": "stage-merge",
            "admission_envelope": {"allowed_paths": list(allowed),
                                   "acceptance_lamp": {"id": "lamp", "smoke": "marker", "verification_argv": LAMP_ARGV}}}
    store.create_run(goal=goal, source_repo=root, base_revision="base-r2", run_id=run_id)
    ordinal = store.begin_attempt(run_id, f"workspace://{run_id}/1")
    diff_ref = store.write_artifact(run_id, ordinal, "diff.patch", diff_text)
    stdout_ref = store.write_artifact(run_id, ordinal, "verifier.stdout", "lamp ok\n")
    stderr_ref = store.write_artifact(run_id, ordinal, "verifier.stderr", "")
    receipt = {
        "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
        "diff": diff_ref,
        "verification": {"argv": LAMP_ARGV, "exit_code": 0, "stdout": stdout_ref, "stderr": stderr_ref},
    }
    ref = store.write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True))
    store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=ref["ref"], receipt_digest=ref["digest"])
    verdicts = ev.VerdictStore(store_root / "verdict.sqlite3")
    verdicts.park(run_id, "op-r2-open",
                  {"request": {"action_id": "open-pr"},
                   "external": {"head_sha": HEAD_SHA, "pr_number": PR_NUMBER, "branch": "lh/g/x",
                                "pr_url": "https://github.invalid/o/r/pull/7"}}, at=1.0)
    verdicts.resolve(run_id, "verified" if conclusion == "success" else "retry_pending", conclusion, at=2.0)
    ledger = eap.ActionLedger(store_root / "action-ledger.sqlite3")
    ramp = TrustRampStore(store_root / "ramp.sqlite3")
    return store, verdicts, ledger, ramp, run_id


def _gate(store, verdicts, ledger, ramp, *, transport, judge=_routine_judge, owner="o", repo="r", environ=None):
    return MergeGate(owner=owner, repo=repo, base_branch="master", run_store=store, verdict_store=verdicts,
                     ledger=ledger, ramp_store=ramp, judge=judge,
                     environ={"LH_GITHUB_MERGE_TOKEN": TOKEN} if environ is None else environ,
                     api_root="https://api.github.invalid", transport=transport)


def _ramp_at_level(ramp: TrustRampStore, *, repo: str = REPO_KEY, at: float = NOW) -> None:
    for i in range(3):
        ramp.record(repo, GOAL_TYPE, f"run-human-{i}", 100 + i, at=at)


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # 1+2: all conditions met -> squash merge with sha guard + audit; retry dedupes.
        store, verdicts, ledger, ramp, run_id = _seed(root / "ok")
        _ramp_at_level(ramp)
        transport = FixtureTransport()
        gate = _gate(store, verdicts, ledger, ramp, transport=transport)
        polled = gate.on_poll_resolved([{"run_id": run_id, "state": "verified"}], at=NOW)
        merged = polled[0]["evaluation"] or {}
        retry = gate.evaluate_and_merge(run_id, at=NOW + 10)
        ledger_record = ledger.get(merge_op_key(run_id, PR_NUMBER, HEAD_SHA))
        snapshot = merged.get("conditions") or {}
        ok_merge = (
            merged.get("merged") is True and merged.get("reason") == "squash-merged"
            and transport.puts == [{"merge_method": "squash", "sha": HEAD_SHA}]
            and merged.get("pr_number") == PR_NUMBER and merged.get("head_sha") == HEAD_SHA
            and merged.get("merge_commit_sha") == transport.merge_commit
            and snapshot.get("value_verdict") == "GREEN" and snapshot.get("diff_grade") == "routine"
            and snapshot.get("ramp_count") == 3 and snapshot.get("protection_verified") is True
            and ledger_record is not None and ledger_record.get("merge_commit_sha") == transport.merge_commit
        )
        retry_deduped = retry.get("merged") is True and retry.get("deduped") is True and len(transport.puts) == 1

        # 3: repo outside the allowlist's trust evidence -> no merge.
        store3, verdicts3, ledger3, ramp3, run3 = _seed(root / "mismatch")
        _ramp_at_level(ramp3)  # evidence sits under "o/r", the gate is "outside/r"
        transport3 = FixtureTransport()
        gate3 = _gate(store3, verdicts3, ledger3, ramp3, transport=transport3, owner="outside")
        mismatch = gate3.evaluate_and_merge(run3, at=NOW)
        mismatch_ok = mismatch["merged"] is False and "trust ramp" in mismatch["reason"] and not transport3.puts

        # 4: authority-surface diff -> no merge.
        store4, verdicts4, ledger4, ramp4, run4 = _seed(root / "authority", diff_text=AUTHORITY_DIFF, allowed=("gate-pack/",))
        _ramp_at_level(ramp4)
        transport4 = FixtureTransport()
        gate4 = _gate(store4, verdicts4, ledger4, ramp4, transport=transport4)
        authority = gate4.evaluate_and_merge(run4, at=NOW)
        authority_ok = authority["merged"] is False and not transport4.puts and not transport4.calls

        # 5+6: CI not green / unknown / nothing parked -> no merge.
        store5, verdicts5, ledger5, ramp5, run5 = _seed(root / "red", conclusion="failure")
        _ramp_at_level(ramp5)
        transport5 = FixtureTransport()
        gate5 = _gate(store5, verdicts5, ledger5, ramp5, transport=transport5)
        red = gate5.evaluate_and_merge(run5, at=NOW)
        store6, verdicts6, ledger6, ramp6, run6 = _seed(root / "unknown", conclusion="unknown")
        _ramp_at_level(ramp6)
        transport6 = FixtureTransport()
        gate6 = _gate(store6, verdicts6, ledger6, ramp6, transport=transport6)
        unknown = gate6.evaluate_and_merge(run6, at=NOW)
        ghost = gate6.evaluate_and_merge("run-ghost", at=NOW)
        ci_ok = (
            red["merged"] is False and "failure" in red["reason"]
            and unknown["merged"] is False and "unknown" in unknown["reason"]
            and ghost["merged"] is False and "no parked" in ghost["reason"]
            and not transport5.puts and not transport6.puts
        )

        # 7: ramp durability + 90-day decay.
        store7, verdicts7, ledger7, ramp7, run7 = _seed(root / "decay")
        old_at = NOW - 100 * 86400
        _ramp_at_level(ramp7, at=old_at)
        transport7 = FixtureTransport()
        gate7 = _gate(store7, verdicts7, ledger7, ramp7, transport=transport7)
        decayed = gate7.evaluate_and_merge(run7, at=NOW)
        reopened = TrustRampStore(root / "decay" / "runs" / "ramp.sqlite3")
        decay_ok = (
            decayed["merged"] is False and "trust ramp" in decayed["reason"] and not transport7.puts
            and reopened.count(REPO_KEY, GOAL_TYPE, now=NOW) == 0
            and reopened.count(REPO_KEY, GOAL_TYPE, now=old_at + 1) == 3
        )

        # 8: revocation returns the next evaluation to human review (human-held CLI).
        store8, verdicts8, ledger8, ramp8, run8 = _seed(root / "revoke")
        _ramp_at_level(ramp8)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            merge_trust.main(["--store", str(root / "revoke" / "runs" / "ramp.sqlite3"), "revoke",
                              "--repo", REPO_KEY, "--goal-type", GOAL_TYPE, "--reason", "fixture revocation"])
        cli_out = json.loads(buffer.getvalue())
        transport8 = FixtureTransport()
        gate8 = _gate(store8, verdicts8, ledger8, ramp8, transport=transport8)
        revoked = gate8.evaluate_and_merge(run8, at=NOW)
        buffer2 = io.StringIO()
        with contextlib.redirect_stdout(buffer2):
            merge_trust.main(["--store", str(root / "revoke" / "runs" / "ramp.sqlite3"), "record",
                              "--repo", REPO_KEY, "--goal-type", GOAL_TYPE, "--run-id", "run-manual", "--pr-number", "55"])
        cli_record = json.loads(buffer2.getvalue())
        revoke_ok = (
            cli_out["revoked"] is True and cli_out["at_level"] is False
            and revoked["merged"] is False and "trust ramp" in revoked["reason"] and not transport8.puts
            and cli_record["inserted"] is True
        )

        # 9: judge anomaly -> human (never a silent routine).
        store9, verdicts9, ledger9, ramp9, run9 = _seed(root / "judge")
        _ramp_at_level(ramp9)

        def _raising_judge(_snapshot: dict) -> Any:
            raise RuntimeError("judge fixture outage")

        transport9 = FixtureTransport()
        gate9 = _gate(store9, verdicts9, ledger9, ramp9, transport=transport9, judge=_raising_judge)
        judged = gate9.evaluate_and_merge(run9, at=NOW)
        judge_ok = judged["merged"] is False and "diff grade" in judged["reason"] and judged["grade"]["source"] == "fallback" and not transport9.puts

        # 10: branch protection unproven -> no merge (404, or no required checks).
        store10, verdicts10, ledger10, ramp10, run10 = _seed(root / "noprot")
        _ramp_at_level(ramp10)
        transport10 = FixtureTransport(protection=None)
        gate10 = _gate(store10, verdicts10, ledger10, ramp10, transport=transport10)
        unprotected = gate10.evaluate_and_merge(run10, at=NOW)
        transport10b = FixtureTransport(protection={"url": "https://github.invalid/protection"})
        gate10b = _gate(store10, verdicts10, ledger10, ramp10, transport=transport10b)
        no_checks = gate10b.evaluate_and_merge(run10, at=NOW)
        protection_ok = (
            unprotected["merged"] is False and "protection" in unprotected["reason"]
            and no_checks["merged"] is False and "protection" in no_checks["reason"]
            and not transport10.puts and not transport10b.puts
        )

        # 11: missing merge token -> construction error, zero API calls.
        store11, verdicts11, ledger11, ramp11, _run11 = _seed(root / "notoken")
        transport11 = FixtureTransport()
        missing_error = None
        try:
            _gate(store11, verdicts11, ledger11, ramp11, transport=transport11, environ={})
        except MergeCredentialsMissing:
            missing_error = "MergeCredentialsMissing"
        token_ok = missing_error == "MergeCredentialsMissing" and not transport11.calls

        # 12: kill switch -> human route before any condition or API call.
        store12, verdicts12, ledger12, ramp12, run12 = _seed(root / "kill")
        _ramp_at_level(ramp12)
        transport12 = FixtureTransport()
        gate12 = _gate(store12, verdicts12, ledger12, ramp12, transport=transport12,
                       environ={"LH_GITHUB_MERGE_TOKEN": TOKEN, "LH_AUTO_MERGE_DISABLE": "1"})
        killed = gate12.evaluate_and_merge(run12, at=NOW)
        kill_ok = killed["merged"] is False and "disabled" in killed["reason"] and not transport12.calls

        # 13: a human merge found on poll feeds the ramp exactly once; an
        # engine-recorded merge is NOT ramp evidence.
        store13, verdicts13, ledger13, ramp13, run13 = _seed(root / "observe")
        transport13 = FixtureTransport(merged=True)
        gate13 = _gate(store13, verdicts13, ledger13, ramp13, transport=transport13)
        first = gate13.observe_human_merge(run13, at=NOW)
        second = gate13.observe_human_merge(run13, at=NOW)
        ledger13.put(merge_op_key(run13, PR_NUMBER, HEAD_SHA), {"merged": True}, at=NOW)
        engine = gate13.observe_human_merge(run13, at=NOW)
        observe_ok = (
            first == {"run_id": run13, "pr_number": PR_NUMBER, "goal_type": GOAL_TYPE, "recorded": True}
            and second["recorded"] is False and engine is None
            and ramp13.count(REPO_KEY, GOAL_TYPE, now=NOW) == 1
        )

        # 14: all conditions met but the PR is already merged -> GET-first
        # idempotency returns the existing merge; no second merge call.
        store14, verdicts14, ledger14, ramp14, run14 = _seed(root / "premerged")
        _ramp_at_level(ramp14)
        transport14 = FixtureTransport(merged=True)
        gate14 = _gate(store14, verdicts14, ledger14, ramp14, transport=transport14)
        premerged = gate14.evaluate_and_merge(run14, at=NOW)
        premerged_ok = premerged["merged"] is True and premerged["already_merged"] is True and not transport14.puts

        # 15: run() wires the gate only when the contract declares auto_merge.
        captured: dict[str, Any] = {}

        def capture_driver(worker: Any, **_kwargs: Any) -> dict[str, Any]:
            captured["gate"] = worker.merge_gate is not None
            return {"stop_reason": "not_holder", "cycles": 0, "runs_dispatched": 0}

        wired_source, wired_base = make_source_repo(root / "wired")
        glr.run(
            executor="fake", execute=True,
            goal_store_root=root / "wired-goals", run_store_root=root / "wired-runs", workspace_root=root / "wired-ws",
            campaign=make_campaign("campaign-r2w"), source_repo=wired_source, base_revision=wired_base,
            executor_timeout_seconds=0.25,
            github_verdict={"owner": "o", "repo": "r", "workflow": "CI"},
            github_pr_adapter={"owner": "o", "repo": "r", "base_branch": "master", "auto_merge": True},
            github_environ={"LH_GITHUB_TOKEN": "r2-fixture-pr-token", "LH_GITHUB_MERGE_TOKEN": TOKEN},
            factory_overrides={"fake": lambda *, timeout_seconds=900: (lambda _ws, _cap: {"summary": "unused"})},
            driver_fn=capture_driver,
        )
        plain_worker = glr.build_worker(
            goal_store_root=root / "plain-goals", run_store_root=root / "plain-runs", workspace_root=root / "plain-ws",
            campaign=make_campaign("campaign-r2p"), source_repo=wired_source, base_revision=wired_base,
        )
        wiring_ok = captured.get("gate") is True and plain_worker.merge_gate is None
        deferred_merge: dict[str, Any] = {}

        def capture_deferred_merge(worker: Any, **_kwargs: Any) -> dict[str, Any]:
            deferred_merge["gate"] = worker.merge_gate is not None
            return {"stop_reason": "idle", "cycles": 1, "runs_dispatched": 0}

        deferred_merge_result = glr.run(
            executor="fake", execute=True,
            goal_store_root=root / "deferred-goals", run_store_root=root / "deferred-runs",
            workspace_root=root / "deferred-ws",
            campaign=make_campaign("campaign-r2-deferred"),
            source_repo=wired_source, base_revision=wired_base,
            executor_timeout_seconds=0.25,
            github_verdict={"owner": "o", "repo": "r", "workflow": "CI"},
            github_pr_adapter={"owner": "o", "repo": "r", "base_branch": "master", "auto_merge": True},
            github_environ={},
            factory_overrides={"fake": lambda *, timeout_seconds=900: (lambda _ws, _cap: {"summary": "unused"})},
            driver_fn=capture_deferred_merge,
        )
        deferred_merge_ok = (
            deferred_merge.get("gate") is True
            and deferred_merge_result["driver"]["stop_reason"] == "idle"
        )

        cases = [
            {"id": "all-conditions-met-squash-merges-with-audit", "ok": ok_merge,
             "detail": json.dumps({"puts": transport.puts, "conditions": snapshot, "ledger": ledger_record is not None})},
            {"id": "op-key-retry-no-duplicate-merge", "ok": retry_deduped,
             "detail": json.dumps({"deduped": retry.get("deduped"), "puts": len(transport.puts)})},
            {"id": "repo-outside-allowlist-evidence-no-merge", "ok": mismatch_ok, "detail": mismatch["reason"]},
            {"id": "authority-surface-diff-no-merge", "ok": authority_ok, "detail": authority["reason"]},
            {"id": "ci-not-green-or-unknown-no-merge", "ok": ci_ok,
             "detail": json.dumps({"failure": red["reason"], "unknown": unknown["reason"], "ghost": ghost["reason"]})},
            {"id": "trust-ramp-durable-and-decays", "ok": decay_ok,
             "detail": json.dumps({"now": reopened.count(REPO_KEY, GOAL_TYPE, now=NOW), "at_event": reopened.count(REPO_KEY, GOAL_TYPE, now=old_at + 1)})},
            {"id": "revocation-returns-to-human-review", "ok": revoke_ok,
             "detail": json.dumps({"cli": cli_out["at_level"], "gate": revoked["reason"]})},
            {"id": "judge-anomaly-routes-human", "ok": judge_ok, "detail": judged["reason"]},
            {"id": "branch-protection-unproven-no-merge", "ok": protection_ok,
             "detail": json.dumps({"http404": unprotected["reason"], "no_checks": no_checks["reason"]})},
            {"id": "missing-merge-token-construction-error", "ok": token_ok, "detail": json.dumps({"error": missing_error, "calls": len(transport11.calls)})},
            {"id": "kill-switch-routes-human-before-any-call", "ok": kill_ok, "detail": killed["reason"]},
            {"id": "human-merge-on-poll-feeds-ramp-once", "ok": observe_ok,
             "detail": json.dumps({"first": first, "second": second, "engine": engine})},
            {"id": "already-merged-pr-dedupes-via-get-first", "ok": premerged_ok,
             "detail": json.dumps({"already_merged": premerged.get("already_merged"), "puts": len(transport14.puts)})},
            {"id": "run-wires-gate-only-when-declared", "ok": wiring_ok,
             "detail": json.dumps({"declared": captured.get("gate"), "plain": plain_worker.merge_gate is None})},
            {"id": "idle-runtime-does-not-require-merge-credential", "ok": deferred_merge_ok,
             "detail": json.dumps({"declared": deferred_merge, "driver": deferred_merge_result["driver"]})},
        ]
    failures = [{"id": case["id"], "detail": case["detail"]} for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-merge-gate",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "verification": {"command": "python3 -B lh_runtime/merge_gate_canary.py",
                         "fixtures": "fixture HTTP transport; no network, no real GitHub, no real credentials"},
        "known_gaps_open": [
            "the live GitHub smoke (real repo, real protection, real squash merge) is a separate node per the R/S execution order",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

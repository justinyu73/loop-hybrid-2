#!/usr/bin/env python3
"""Effect guard: a post-run external effect re-verifies delivery and its target first.

A run that finished is not yet permission to act outside the workspace.  Before
an injected effect (merge, publish, deploy) the guard re-reads the final
delivery verdict of the current attempt, refuses a diff that touches the
authority surface, requires an explicit grant when the contract carries
candidate review v2, re-reads the target right before the effect, records a
prepared marker before sending, and after a lost response only reads back.

Runs come from the public native fixture on the synchronous delivery path;
the target is a counting stand-in.  No service, model, or credential is used.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import candidate_review_canary as review_fixture  # noqa: E402
import external_action_port as eap  # noqa: E402
import native_delivery_fixture  # noqa: E402
from controller import LoopController  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-effect-guard"
EFFECT = "publish"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing API is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def guard_module():
    import importlib
    return importlib.import_module("effect_guard")


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _exists(paths: list[str]) -> list[str]:
    program = "from pathlib import Path; assert all(Path(p).is_file() for p in %r)" % (paths,)
    return [sys.executable, "-B", "-c", program]


class Run:
    """One native run on the synchronous delivery path."""

    def __init__(self, root: Path, *, files: dict[str, str], allowed: list[str], policy: dict | None = None,
                 tick: bool = True):
        root.mkdir(parents=True)
        self.source = root / "source"
        self.source.mkdir()
        _git("init", "-q", cwd=self.source)
        _git("config", "user.email", "effect@example.invalid", cwd=self.source)
        _git("config", "user.name", "Effect Guard", cwd=self.source)
        review_fixture.write(self.source / "baseline.txt", "baseline\n")
        _git("add", "baseline.txt", cwd=self.source)
        _git("commit", "-qm", "baseline", cwd=self.source)
        self.base = _git("rev-parse", "HEAD", cwd=self.source)
        self.runs = RunStore(root / "runs", command_runner=fixture_command_runner)
        paths = sorted(files)
        checks = [{"id": "files-present", "commands": [{"id": "present", "argv": _exists(paths), "cwd": "${WORKTREE}",
                                                       "expect_exit": 0, "timeout_seconds": 30}],
                   "required_receipts": ["executor"]}]
        verifier = review_fixture._reviewer("green") if policy is not None else _exists(paths)
        with review_fixture._sealing_with(policy):
            bundle = native_delivery_fixture.make_native_run(
                self.runs, self.source, self.base, root.name, "effect", checks, verifier, allowed, 2,
                run_id=root.name)
        self.run_id = bundle["run_id"]
        self.contract = bundle["contract"]
        self.result = None
        if tick:
            def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
                for relative, text in files.items():
                    review_fixture.write(workspace / relative, text)
                return {"summary": "effect guard candidate", "usage": {"state": "unknown"}}
            self.result = LoopController(self.runs, root / "workspaces").tick(
                self.run_id, holder="effect-guard", model=model, verifier_argv=_exists(paths))


class Target:
    """A counting external target with an identity and an at-most-once effect."""

    def __init__(self, identity: dict[str, Any], *, done: bool = False, lose_response: bool = False):
        self.identity, self.done, self.lose_response = dict(identity), done, lose_response
        self.readbacks, self.performs = 0, 0

    def readback(self, request: dict[str, Any]) -> dict[str, Any]:
        self.readbacks += 1
        return {"state": "done" if self.done else "absent", "identity": dict(self.identity),
                "result": {"published": True} if self.done else {}}

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        self.performs += 1
        self.done = True
        if self.lose_response:
            self.lose_response = False
            raise ConnectionError("response lost after the effect was sent")
        return {"published": True, "op_key": op_key}


def _dispatch(run: Run, target: Target, ledger: eap.ActionLedger, *, expected: dict[str, Any] | None = None,
              grant: dict | None = None, before_effect: Callable[[], None] | None = None) -> dict[str, Any]:
    expected = expected if expected is not None else {"base": run.base, "head": "candidate-1"}
    return guard_module().guarded_dispatch(
        ledger, target, run_store=run.runs, run_id=run.run_id, effect=EFFECT, expected=expected,
        request={"channel": "fixture"}, at=1.0, grant=grant, before_effect=before_effect)


def _ledger_rows(ledger: eap.ActionLedger) -> int:
    with ledger._connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]


def _good(root: Path) -> Run:
    return Run(root, files={"src/out.txt": "published content\n"}, allowed=["src/"])


def c1_final_green(root: Path) -> dict[str, Any]:
    run = Run(root, files={"src/out.txt": "x\n"}, allowed=["src/"], tick=False)
    target, ledger = Target({"base": run.base, "head": "candidate-1"}), eap.ActionLedger(root / "ledger.sqlite3")
    outcome = _dispatch(run, target, ledger)
    ok = outcome.get("status") == "refused" and target.performs == 0 and _ledger_rows(ledger) == 0
    return case("effect-needs-final-delivery-green", ok, {"outcome": outcome, "performs": target.performs})


def c2_authority(root: Path) -> dict[str, Any]:
    run = Run(root, files={"src/publish_canary.py": "print('changed exam')\n"}, allowed=["src/"])
    target, ledger = Target({"base": run.base, "head": "candidate-1"}), eap.ActionLedger(root / "ledger.sqlite3")
    outcome = _dispatch(run, target, ledger)
    ok = (run.runs.get_run(run.run_id)["state"] == "verified" and outcome.get("status") == "refused"
          and "authority_surface_touched" in str(outcome.get("reason")) and target.performs == 0)
    return case("authority-surface-blocks-the-effect", ok,
                {"run_state": run.runs.get_run(run.run_id)["state"], "outcome": outcome, "performs": target.performs})


def c3_review_grant(root: Path) -> dict[str, Any]:
    policy = review_fixture.policy(root / "policy")
    run = Run(root / "run", files={"src/m1.py": review_fixture.CANDIDATE}, allowed=["src/"], policy=policy)
    target = Target({"base": run.base, "head": "candidate-1"})
    ledger = eap.ActionLedger(root / "ledger.sqlite3")
    missing = _dispatch(run, target, ledger)
    wrong = _dispatch(run, target, ledger, grant={"schema": "lh-effect-grant/v1", "effect": EFFECT,
                                                  "run_id": run.run_id, "contract_digest": "sha256:" + "0" * 64})
    performs_before = target.performs
    granted = _dispatch(run, target, ledger, grant={"schema": "lh-effect-grant/v1", "effect": EFFECT,
                                                    "run_id": run.run_id,
                                                    "contract_digest": run.contract["contract_digest"]})
    ok = (run.runs.get_run(run.run_id)["state"] == "verified"
          and missing.get("status") == "refused" and wrong.get("status") == "refused" and performs_before == 0
          and granted.get("status") == "done" and target.performs == 1)
    return case("review-v2-needs-an-explicit-grant", ok,
                {"run_state": run.runs.get_run(run.run_id)["state"], "missing": missing.get("reason"),
                 "wrong": wrong.get("reason"), "granted": granted.get("status"), "performs": target.performs})


def c4_base(root: Path) -> dict[str, Any]:
    run = _good(root)
    target = Target({"base": "f" * 40, "head": "candidate-1"})
    outcome = _dispatch(run, target, eap.ActionLedger(root / "ledger.sqlite3"),
                        expected={"base": "f" * 40, "head": "candidate-1"})
    ok = outcome.get("status") == "refused" and target.performs == 0
    return case("base-must-be-the-reviewed-base", ok, {"outcome": outcome, "performs": target.performs})


def c5_target(root: Path) -> dict[str, Any]:
    run = _good(root / "run")
    changed = Target({"base": run.base, "head": "someone-else"})
    first = _dispatch(run, changed, eap.ActionLedger(root / "ledger-a.sqlite3"))
    moving = Target({"base": run.base, "head": "candidate-1"})

    def wait_while_target_moves() -> None:
        moving.identity["head"] = "pushed-during-the-wait"
    second = _dispatch(run, moving, eap.ActionLedger(root / "ledger-b.sqlite3"), before_effect=wait_while_target_moves)
    ok = (first.get("status") == "refused" and "effect_target_changed" in str(first.get("reason"))
          and second.get("status") == "refused" and "effect_target_changed" in str(second.get("reason"))
          and changed.performs == 0 and moving.performs == 0 and moving.readbacks >= 2)
    return case("target-must-match-right-before-the-effect", ok,
                {"changed": first, "moved": second, "moving_readbacks": moving.readbacks})


def c6_already_done(root: Path) -> dict[str, Any]:
    run = _good(root)
    target = Target({"base": run.base, "head": "candidate-1"}, done=True)
    outcome = _dispatch(run, target, eap.ActionLedger(root / "ledger.sqlite3"))
    ok = outcome.get("status") == "done" and target.performs == 0
    return case("already-done-target-is-not-redone", ok, {"outcome": outcome, "performs": target.performs})


def c7_c8_lost_and_dedupe(root: Path) -> list[dict[str, Any]]:
    run = _good(root)
    target = Target({"base": run.base, "head": "candidate-1"}, lose_response=True)
    ledger = eap.ActionLedger(root / "ledger.sqlite3")
    try:
        first: Any = _dispatch(run, target, ledger)
    except ConnectionError as exc:
        first = {"raised": str(exc)}
    performs_after_loss = target.performs
    target.done = False  # the external side has not yet exposed the effect
    unresolved = _dispatch(run, target, ledger)
    target.done = True   # now the readback can confirm it
    confirmed = _dispatch(run, target, ledger)
    seventh = case("lost-response-is-never-replayed",
                   performs_after_loss == 1 and unresolved.get("status") == "unknown"
                   and confirmed.get("status") == "done" and target.performs == 1,
                   {"first": first, "unresolved": unresolved, "confirmed": confirmed, "performs": target.performs})
    readbacks = target.readbacks
    again = _dispatch(run, target, ledger)
    eighth = case("completed-effect-dedupes",
                  again.get("status") == "done" and again.get("deduped") is True
                  and target.readbacks == readbacks and target.performs == 1,
                  {"again": again, "readbacks_added": target.readbacks - readbacks, "performs": target.performs})
    return [seventh, eighth]


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-effect-guard-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("effect-needs-final-delivery-green", lambda: c1_final_green(root / "c1")),
            guarded("authority-surface-blocks-the-effect", lambda: c2_authority(root / "c2")),
            guarded("review-v2-needs-an-explicit-grant", lambda: c3_review_grant(root / "c3")),
            guarded("base-must-be-the-reviewed-base", lambda: c4_base(root / "c4")),
            guarded("target-must-match-right-before-the-effect", lambda: c5_target(root / "c5")),
            guarded("already-done-target-is-not-redone", lambda: c6_already_done(root / "c6")),
        ]
        try:
            results.extend(c7_c8_lost_and_dedupe(root / "c7"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:400]}"
            results.extend([case("lost-response-is-never-replayed", False, detail),
                            case("completed-effect-dedupes", False, detail)])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(results),
        "results": results,
        "blocking_failures": [row["id"] for row in failures],
        "known_gaps_open": [
            "the target is a counting stand-in; a real service adapter and its own protection rules are the injector's",
            "a grant is only checked for its binding; producing one is the injecting project's authorization",
        ],
    }, indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

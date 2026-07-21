"""Native LH controller MVP: lease a run, execute it once, then persist facts."""
from __future__ import annotations

import hashlib
import json
import signal
import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

import external_action_port as eap
import external_verdict as ev
import token_cost
from run_store import RunStore
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS

ModelRunner = Callable[[Path, dict[str, Any]], dict[str, Any]]


def _digest(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode()).hexdigest()


class AttemptTimeout(TimeoutError):
    """The controller's single attempt budget was exhausted."""


class _TimeoutBudget:
    def __init__(self, seconds: float):
        self.seconds = seconds

    def timeout(self) -> float:
        return self.seconds


def _output_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


@contextmanager
def _hard_io_timeout(seconds: float):
    """Interrupt a blocking artifact write on the POSIX/WSL controller thread."""
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 0)

    def alarm(_signum, _frame):
        raise AttemptTimeout("artifact write exceeded attempt timeout budget")

    signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer != (0.0, 0.0):
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)


class LoopController:
    """A trigger-safe controller; callers may invoke ``tick`` repeatedly."""

    def __init__(self, store: RunStore, workspace_root: str | Path, *, timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.store = store
        self.workspace_root = Path(workspace_root)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = float(timeout_seconds)

    def _run(self, argv: list[str], *, cwd: str | Path | None, budget: _TimeoutBudget) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=budget.timeout())
        except subprocess.TimeoutExpired as exc:
            raise AttemptTimeout(f"subprocess timed out: {argv[0]}") from exc

    def _workspace(self, run: dict[str, Any], ordinal: int, budget: _TimeoutBudget) -> tuple[Path, str]:
        workspace = self.workspace_root / run["run_id"] / str(ordinal)
        workspace.parent.mkdir(parents=True, exist_ok=True)
        completed = self._run(["git", "clone", "--quiet", "--no-local", run["source_repo"], str(workspace)], cwd=None, budget=budget)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or "could not create disposable workspace")
        checkout = self._run(["git", "-C", str(workspace), "checkout", "--quiet", "--detach", run["base_revision"]], cwd=None, budget=budget)
        if checkout.returncode != 0:
            raise RuntimeError(checkout.stderr.strip() or "could not checkout base revision")
        marker = {"schema": "loop-hybrid-disposable-workspace/v1", "run_id": run["run_id"], "attempt": ordinal, "base_revision": run["base_revision"]}
        (workspace / ".git" / "lh-disposable-workspace.json").write_text(json.dumps(marker, sort_keys=True), encoding="utf-8")
        return workspace, f"workspace://{run['run_id']}/{ordinal}"

    def _write_artifact(self, run_id: str, ordinal: int, name: str, content: str, budget: _TimeoutBudget) -> dict[str, str]:
        with _hard_io_timeout(budget.timeout()):
            result = self.store.write_artifact(run_id, ordinal, name, content)
        return result

    @staticmethod
    def _staging_error(add: subprocess.CompletedProcess, diff: subprocess.CompletedProcess) -> str | None:
        """W8-1: staging must be checked — a failed ``git add``/``git diff``
        (e.g. an unreadable file the model created) makes the attempt's
        evidence chain unreliable, so the attempt can never go green."""
        if add.returncode != 0:
            return f"git add -A exited {add.returncode}: {add.stderr.strip()[:400]}"
        if diff.returncode != 0:
            return f"git diff --cached exited {diff.returncode}: {diff.stderr.strip()[:400]}"
        return None

    def _previous_failure_signature(self, run_id: str, ordinal: int) -> dict[str, Any] | None:
        """W6b: the previous attempt's failure signature — its verifier exit
        code and diff digest, read from the durable receipt. None when the
        receipt is missing or unreadable, so an unreadable history never
        stops a run."""
        receipt_path = self.store.artifacts / run_id / str(ordinal - 1) / "receipt.json"
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        verification = receipt.get("verification") if isinstance(receipt, dict) else None
        diff = receipt.get("diff") if isinstance(receipt, dict) else None
        exit_code = verification.get("exit_code") if isinstance(verification, dict) else None
        digest = diff.get("digest") if isinstance(diff, dict) else None
        if not isinstance(exit_code, int) or not isinstance(digest, str):
            return None
        return {"exit_code": exit_code, "diff_digest": digest}

    def tick(self, run_id: str, *, holder: str, model: ModelRunner, verifier_argv: list[str], grill_note: str | None = None) -> dict[str, Any]:
        self.store.recover_stale_run(run_id)
        if not self.store.acquire_lease(run_id, holder):
            return {"status": "lease_busy", "run_id": run_id}
        workspace: Path | None = None
        ordinal: int | None = None
        try:
            budget = _TimeoutBudget(self.timeout_seconds)
            run = self.store.get_run(run_id)
            if run["state"] not in {"queued", "retry_pending"}:
                return {"status": "not_runnable", "run_id": run_id, "state": run["state"]}
            ordinal_hint = run["attempts"] + 1
            workspace_ref = f"workspace://{run_id}/{ordinal_hint}"
            ordinal = self.store.begin_attempt(run_id, workspace_ref)
            workspace, workspace_ref = self._workspace(run, ordinal, budget)
            fence = self.store.attempt_fence(run_id, ordinal)
            # W3 lamp precheck: if the acceptance lamp already passes on the
            # untouched base, the work was already done — finish verified
            # without spending a model invocation (a green-on-base lamp means
            # "already done", not "run the model anyway").
            try:
                pre = subprocess.run(verifier_argv, cwd=workspace, capture_output=True, text=True, timeout=budget.timeout())
            except subprocess.TimeoutExpired:
                pre = None
            if pre is not None and pre.returncode == 0:
                pre_add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
                pre_diff = self._run(["git", "-C", str(workspace), "diff", "--cached", "--binary"], cwd=None, budget=budget)
            # W8-1: a precheck whose staging failed is not evidence — fall
            # through to the model path, whose own staging check records it.
            if pre is not None and pre.returncode == 0 and self._staging_error(pre_add, pre_diff) is None:
                pre_provider = {"summary": "lamp precheck passed without model invocation", "precheck": True}
                pre_provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(pre_provider, sort_keys=True), budget)
                pre_diff_ref = self._write_artifact(run_id, ordinal, "diff.patch", pre_diff.stdout, budget)
                pre_stdout_ref = self._write_artifact(run_id, ordinal, "verifier.stdout", pre.stdout, budget)
                pre_stderr_ref = self._write_artifact(run_id, ordinal, "verifier.stderr", pre.stderr, budget)
                pre_receipt = {
                    "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
                    "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": run["base_revision"]},
                    "provider": {"summary": pre_provider["summary"], "artifact": pre_provider_ref},
                    "usage": token_cost.unknown_usage(reason="lamp precheck: no model invocation"),
                    "diff": pre_diff_ref,
                    "verification": {"argv": verifier_argv, "exit_code": 0, "stdout": pre_stdout_ref, "stderr": pre_stderr_ref, "precheck": True},
                }
                pre_receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(pre_receipt, sort_keys=True), budget)
                if not self.store.finish_attempt(run_id, ordinal, state="verified", receipt_ref=pre_receipt_ref["ref"], receipt_digest=pre_receipt_ref["digest"], fence=fence):
                    return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
                return {"status": "verified", "run_id": run_id, "attempt": ordinal, "precheck": True, "receipt_ref": pre_receipt_ref["ref"], "receipt_digest": pre_receipt_ref["digest"]}
            capsule = {"run_id": run_id, "attempt": ordinal, "fence": fence, "timeout_seconds": budget.timeout(), "goal": run["goal"], "base_revision": run["base_revision"], "workspace_ref": workspace_ref}
            if grill_note is not None:
                # W6a: challenger diagnosis for the final attempt — guidance
                # only, additive to the capsule the executor already receives.
                capsule["grill_note"] = grill_note
            provider: dict[str, Any]
            try:
                provider = model(workspace, capsule)
                if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
                    raise ValueError("model runner must return a dict with a bounded summary")
            except Exception as exc:  # Provider failures are facts for the next controller tick.
                provider = {"summary": "model invocation failed", "failure": f"{type(exc).__name__}: {exc}"}
            # Stage everything first so new/untracked files the executor created
            # appear in the diff; a plain `git diff` omits them, which would make
            # the value reducer see an empty diff for a real new-file change.
            add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
            diff = self._run(["git", "-C", str(workspace), "diff", "--cached", "--binary"], cwd=None, budget=budget)
            staging_error = self._staging_error(add, diff)
            verified = None
            if "failure" not in provider and staging_error is None:
                try:
                    verified = subprocess.run(verifier_argv, cwd=workspace, capture_output=True, text=True, timeout=budget.timeout())
                except subprocess.TimeoutExpired as exc:
                    verified = subprocess.CompletedProcess(verifier_argv, 124, _output_text(exc.stdout), _output_text(exc.stderr) + "\nverifier timed out")
            provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(provider, sort_keys=True), budget)
            diff_ref = self._write_artifact(run_id, ordinal, "diff.patch", diff.stdout, budget)
            stdout = verified.stdout if verified else ""
            stderr = verified.stderr if verified else staging_error or provider["failure"]
            stdout_ref = self._write_artifact(run_id, ordinal, "verifier.stdout", stdout, budget)
            stderr_ref = self._write_artifact(run_id, ordinal, "verifier.stderr", stderr, budget)
            exit_code = verified.returncode if verified else -1
            run_after = self.store.get_run(run_id)
            state = "verified" if exit_code == 0 else "stopped" if ordinal >= run_after["max_attempts"] else "retry_pending"
            no_progress: dict[str, Any] | None = None
            if state == "retry_pending":
                # W6b no-progress line: two consecutive attempts with an
                # identical failure signature (same lamp exit, same diff
                # digest) mean the loop is not moving — stop the run early
                # instead of burning the remaining attempts.
                signature = {"exit_code": exit_code, "diff_digest": diff_ref["digest"]}
                if self._previous_failure_signature(run_id, ordinal) == signature:
                    state = "stopped"
                    no_progress = {"attempt": ordinal, "max_attempts": run_after["max_attempts"], "signature": signature}
            verification: dict[str, Any] = {"argv": verifier_argv, "exit_code": exit_code, "stdout": stdout_ref, "stderr": stderr_ref}
            if staging_error is not None:
                # W8-1: staging failed, so the verifier never ran and the
                # attempt cannot go green; the reason stays on the receipt.
                verification["staging_error"] = staging_error
            receipt = {
                "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
                "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": run["base_revision"]},
                "provider": {"summary": provider["summary"], "artifact": provider_ref},
                "usage": provider.get("usage") if isinstance(provider.get("usage"), dict) else token_cost.unknown_usage(reason="model did not report usage"),
                "diff": diff_ref,
                "verification": verification,
            }
            receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
            if not self.store.finish_attempt(run_id, ordinal, state=state, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
            if no_progress is not None:
                self.store.append_event(run_id, "no_progress_stop", no_progress)
            return {"status": state, "run_id": run_id, "attempt": ordinal, "receipt_ref": receipt_ref["ref"], "receipt_digest": receipt_ref["digest"]}
        except AttemptTimeout as exc:
            return {"status": "attempt_timeout", "run_id": run_id, "attempt": ordinal, "reason": str(exc)}
        finally:
            if workspace is not None:
                shutil.rmtree(workspace, ignore_errors=True)
            self.store.release_lease(run_id, holder)

    def startup(self) -> list[dict[str, Any]]:
        """Call once after process start before triggers dispatch individual runs."""
        return self.store.reconcile_startup()

    def tick_async(self, run_id: str, *, holder: str, model: ModelRunner, verdict_store: ev.VerdictStore,
                   action_ledger: eap.ActionLedger, adapter: eap.ExternalAdapter, action_id: str = "open-pr") -> dict[str, Any]:
        """Method A: execute, open an external action (PR) at most once, then PARK the run
        awaiting its async CI verdict. No local verifier — the verdict lands later via
        resume_external (poll-on-startup). An executor failure falls back to retry/stopped."""
        self.store.recover_stale_run(run_id)
        if not self.store.acquire_lease(run_id, holder):
            return {"status": "lease_busy", "run_id": run_id}
        workspace: Path | None = None
        ordinal: int | None = None
        try:
            budget = _TimeoutBudget(self.timeout_seconds)
            run = self.store.get_run(run_id)
            if run["state"] not in {"queued", "retry_pending"}:
                return {"status": "not_runnable", "run_id": run_id, "state": run["state"]}
            ordinal_hint = run["attempts"] + 1
            workspace_ref = f"workspace://{run_id}/{ordinal_hint}"
            ordinal = self.store.begin_attempt(run_id, workspace_ref)
            workspace, workspace_ref = self._workspace(run, ordinal, budget)
            fence = self.store.attempt_fence(run_id, ordinal)
            capsule = {"run_id": run_id, "attempt": ordinal, "fence": fence, "timeout_seconds": budget.timeout(), "goal": run["goal"], "base_revision": run["base_revision"], "workspace_ref": workspace_ref}
            try:
                provider = model(workspace, capsule)
                if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
                    raise ValueError("model runner must return a dict with a bounded summary")
            except Exception as exc:
                provider = {"summary": "model invocation failed", "failure": f"{type(exc).__name__}: {exc}"}
            diff = self._run(["git", "-C", str(workspace), "diff", "--binary"], cwd=None, budget=budget).stdout
            diff_ref = self._write_artifact(run_id, ordinal, "diff.patch", diff, budget)
            provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(provider, sort_keys=True), budget)
            base_receipt = {"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
                            "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": run["base_revision"]},
                            "provider": {"summary": provider["summary"], "artifact": provider_ref}, "diff": diff_ref}
            if "failure" in provider:
                run_after = self.store.get_run(run_id)
                state = "stopped" if ordinal >= run_after["max_attempts"] else "retry_pending"
                receipt = {**base_receipt, "verification": {"mode": "external_async", "dispatched": False, "reason": provider["failure"]}}
                receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
                if not self.store.finish_attempt(run_id, ordinal, state=state, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                    return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
                return {"status": state, "run_id": run_id, "attempt": ordinal}
            op_key = eap.operation_key(run_id, action_id, diff_ref["digest"])
            dispatched = ev.dispatch_external(verdict_store, action_ledger, adapter, run_id=run_id, op_key=op_key,
                                              request={"diff_digest": diff_ref["digest"], "workspace_ref": workspace_ref, "action_id": action_id}, at=time.time())
            receipt = {**base_receipt, "verification": {"mode": "external_async", "dispatched": True, "op_key": op_key, "external": dispatched["external"]}}
            receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
            if not self.store.park_external_verdict(run_id, ordinal, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
            return {"status": "awaiting_external_verdict", "run_id": run_id, "attempt": ordinal, "op_key": op_key}
        except AttemptTimeout as exc:
            return {"status": "attempt_timeout", "run_id": run_id, "attempt": ordinal, "reason": str(exc)}
        finally:
            if workspace is not None:
                shutil.rmtree(workspace, ignore_errors=True)
            self.store.release_lease(run_id, holder)

    def resume_external(self, *, verdict_store: ev.VerdictStore, source: ev.ConclusionSource) -> list[dict[str, Any]]:
        """Poll-on-startup: resolve every parked run whose external verdict has landed."""
        resumed = ev.poll_and_resume(verdict_store, source, at=time.time())
        for row in resumed:
            try:
                self.store.resolve_external_verdict(row["run_id"], row["state"])
            except ValueError:
                pass  # already resolved, or not tracked by this run_store
        return resumed

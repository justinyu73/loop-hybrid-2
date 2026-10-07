"""Native LH controller MVP: lease a run, execute it once, then persist facts."""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Mapping

import capability_resolver as cr
import delivery_contract as delivery_engine
import execution_fence as execution_fences
import provider_input_binding as provider_inputs
import external_action_port as eap
import external_verdict as ev
import token_cost
from platform_ports import (
    DeadlineExpired,
    DeadlinePort,
    LocalProcessPort,
    MonotonicDeadlinePort,
    ProcessPort,
    ProcessResult,
    ProcessTimeout,
    remove_tree,
)
from run_store import RunStore
from status_snapshot import DEFAULT_EXECUTOR_TIMEOUT_SECONDS
from workspace_port import GitCloneWorkspace, WorkspacePort, WorkspaceRequest

ModelRunner = Callable[[Path, dict[str, Any]], dict[str, Any]]
# Reason-code prefix and event type for a verifier that cannot be launched.
VERIFIER_UNAVAILABLE = "verifier_unavailable"


def _digest(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode()).hexdigest()


class AttemptTimeout(TimeoutError):
    """The controller's single attempt budget was exhausted."""

    def __init__(self, message: str, *, stdout: str = "", stderr: str = "") -> None:
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(message)


class _TimeoutBudget:
    def __init__(self, seconds: float, deadline_port: DeadlinePort):
        self.seconds = seconds
        self._deadline_port = deadline_port
        self._deadline = deadline_port.start(seconds)

    def timeout(self) -> float:
        try:
            return self._deadline.timeout()
        except DeadlineExpired as exc:
            raise AttemptTimeout(str(exc)) from exc

    def check(self) -> None:
        try:
            self._deadline.check()
        except DeadlineExpired as exc:
            raise AttemptTimeout(str(exc)) from exc

    def reset(self) -> None:
        """Start the next bounded phase after a timed-out child process.

        A verifier timeout is a recorded attempt fact, not permission to skip
        receipt persistence.  The watchdog is therefore renewed for the
        controller's bounded recording phase after the child has been killed.
        """
        self._deadline = self._deadline_port.start(self.seconds)


class LoopController:
    """A trigger-safe controller; callers may invoke ``tick`` repeatedly."""

    def __init__(
        self,
        store: RunStore,
        workspace_root: str | Path,
        *,
        timeout_seconds: float = DEFAULT_EXECUTOR_TIMEOUT_SECONDS,
        lease_seconds: float = 60.0,
        dispatch: dict[str, str] | None = None,
        execution_fence_port: execution_fences.ExecutionFencePort | None = None,
        process_port: ProcessPort | None = None,
        deadline_port: DeadlinePort | None = None,
        workspace_port: WorkspacePort | None = None,
    ):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.store = store
        self.workspace_root = Path(workspace_root)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = float(timeout_seconds)
        self.lease_seconds = float(lease_seconds)
        self.dispatch = dict(dispatch) if dispatch is not None else None
        self.execution_fence_port = (
            execution_fence_port
            if execution_fence_port is not None
            else execution_fences.DisabledExecutionFencePort()
        )
        self.process_port = process_port or LocalProcessPort()
        self.deadline_port = deadline_port or MonotonicDeadlinePort()
        self.workspace_port = workspace_port or GitCloneWorkspace()

    def _bind_dispatch(self, receipt: dict[str, Any]) -> dict[str, Any]:
        if self.dispatch is not None:
            receipt["dispatch"] = dict(self.dispatch)
        return receipt

    def _run(self, argv: list[str], *, cwd: str | Path | None, budget: _TimeoutBudget) -> ProcessResult:
        try:
            budget.check()
            return self.process_port.run(argv, cwd=cwd, timeout=budget.timeout())
        except ProcessTimeout as exc:
            raise AttemptTimeout(
                f"subprocess timed out: {argv[0]}",
                stdout=exc.stdout,
                stderr=exc.stderr,
            ) from exc

    def _source_layout(self, source_repo: str | Path, budget: _TimeoutBudget) -> tuple[Path, Path]:
        """Resolve a source checkout into its Git root and target subtree.

        A project contract may intentionally bind a target inside a larger
        checkout.  Git can clone the checkout root, but not the nested target
        directory, so the Attempt runs from the corresponding subtree of the
        disposable clone.
        """
        source = Path(source_repo).resolve()
        probe = self._run(
            ["git", "-C", str(source), "rev-parse", "--show-toplevel"],
            cwd=None,
            budget=budget,
        )
        if probe.returncode != 0 or not probe.stdout.strip():
            return source, Path(".")
        git_root = Path(probe.stdout.strip()).resolve()
        try:
            target_subdir = source.relative_to(git_root)
        except ValueError as exc:
            raise RuntimeError("source checkout is outside its Git root") from exc
        return git_root, target_subdir

    def _workspace(
        self,
        run: dict[str, Any],
        ordinal: int,
        budget: _TimeoutBudget,
    ) -> tuple[Path, str, Path]:
        source_root, target_subdir = self._source_layout(run["source_repo"], budget)
        prepared = self.workspace_port.prepare(WorkspaceRequest(
            run_id=run["run_id"], attempt=ordinal, base_revision=run["base_revision"], source_root=source_root,
            target_subdir=target_subdir, workspace_root=self.workspace_root,
            run=lambda argv: self._run(argv, cwd=None, budget=budget)))
        git_root = Path(prepared.git_root).resolve()
        # Whatever the backend, the workspace and everything removed after the Attempt stay inside the root.
        if not git_root.is_relative_to(self.workspace_root.resolve()) or git_root == self.workspace_root.resolve():
            raise RuntimeError("workspace_outside_root")
        if not Path(prepared.workspace).is_dir():
            raise RuntimeError("source target subtree is missing from disposable workspace")
        return Path(prepared.workspace), prepared.ref, Path(prepared.git_root)

    def _source_refs_digest(self, run: dict[str, Any], budget: _TimeoutBudget) -> str:
        """Digest of every ref in the source repository the clone was made from."""
        source_root, _target = self._source_layout(run["source_repo"], budget)
        listed = self._run(["git", "-C", str(source_root), "for-each-ref", "--format=%(refname) %(objectname)"],
                           cwd=None, budget=budget)
        return _digest(f"{listed.returncode}\n{listed.stdout}")

    @staticmethod
    def _source_refs_guard(provider: dict[str, Any], before: str, after: str) -> dict[str, Any]:
        """An executor that moved a source ref has crossed the clone; a human must look."""
        if before == after:
            return provider
        return {**provider, "failure": "source_refs_mutated_by_executor",
                "routing": {"route": "human_required", "reason": "source_refs_mutated_by_executor"}}

    def _write_artifact(self, run_id: str, ordinal: int, name: str, content: str, budget: _TimeoutBudget) -> dict[str, str]:
        budget.check()
        result = self.store.write_artifact(run_id, ordinal, name, content)
        budget.check()
        return result

    def _prepare_execution_fence(
        self,
        *,
        model: ModelRunner,
        run: dict[str, Any],
        ordinal: int,
        attempt_fence: int,
        workspace: Path,
        verifier_argv: list[str],
        budget: _TimeoutBudget,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if not execution_fences.model_requires_fence(model):
            return None, None
        adapter_id, adapter_version = execution_fences.model_fence_identity(model)
        binding = execution_fences.build_attempt_binding(
            goal=run["goal"],
            run_id=run["run_id"],
            attempt=ordinal,
            attempt_fence=attempt_fence,
            base_revision=run["base_revision"],
            clone_root=workspace,
            verifier_argv=verifier_argv,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            timeout_seconds=budget.timeout(),
        )
        descriptor = self.execution_fence_port.prepare(binding)
        projection = self.execution_fence_port.receipt_projection(descriptor)
        return descriptor, projection

    def _finish_execution_fence_unavailable(
        self,
        *,
        run_id: str,
        ordinal: int,
        attempt_fence: int,
        workspace_ref: str,
        base_revision: str,
        reason: str,
        budget: _TimeoutBudget,
        mode: str,
    ) -> dict[str, Any]:
        provider = {
            "summary": "execution fence unavailable; model not invoked",
            "failure": f"{execution_fences.ERROR_CODE}: {reason}",
            "provider_invocations": 0,
        }
        provider_ref = self._write_artifact(
            run_id,
            ordinal,
            "provider.json",
            json.dumps(provider, sort_keys=True),
            budget,
        )
        empty_diff = self._write_artifact(
            run_id,
            ordinal,
            "diff.patch",
            "",
            budget,
        )
        receipt = self._bind_dispatch(
            {
                "schema": "loop-hybrid-attempt-receipt/v1",
                "run_id": run_id,
                "attempt": ordinal,
                "workspace": {
                    "ref": workspace_ref,
                    "disposable": True,
                    "disposed": True,
                    "base_revision": base_revision,
                },
                "provider": {
                    "summary": provider["summary"],
                    "artifact": provider_ref,
                    "provider_invocations": 0,
                },
                "usage": token_cost.unknown_usage(
                    reason="execution fence unavailable: no model invocation"
                ),
                "diff": empty_diff,
                "execution_fence": {
                    "status": "unavailable",
                    "error_code": execution_fences.ERROR_CODE,
                    "reason": reason,
                    "mutation_dispatch": "disabled",
                },
                "verification": {
                    "mode": mode,
                    "dispatched": False,
                    "reason": execution_fences.ERROR_CODE,
                },
                "routing": {
                    "route": "human_required",
                    "reason": execution_fences.ERROR_CODE,
                },
            }
        )
        receipt_ref = self._write_artifact(
            run_id,
            ordinal,
            "receipt.json",
            json.dumps(receipt, sort_keys=True),
            budget,
        )
        if not self.store.finish_attempt(
            run_id,
            ordinal,
            state="human_required",
            receipt_ref=receipt_ref["ref"],
            receipt_digest=receipt_ref["digest"],
            fence=attempt_fence,
        ):
            return {
                "status": "fence_rejected",
                "run_id": run_id,
                "attempt": ordinal,
                "fence": attempt_fence,
            }
        self.store.append_event(
            run_id,
            "execution_fence_unavailable",
            {
                "attempt": ordinal,
                "error_code": execution_fences.ERROR_CODE,
                "reason": reason,
                "provider_invocations": 0,
            },
            event_id=f"execution-fence-unavailable:{run_id}:{ordinal}",
        )
        return {
            "status": "human_required",
            "run_id": run_id,
            "attempt": ordinal,
            "reason": execution_fences.ERROR_CODE,
            "provider_invocations": 0,
            "receipt_ref": receipt_ref["ref"],
            "receipt_digest": receipt_ref["digest"],
        }

    def _verifier_unready(self, verifier_argv: list[str]) -> str | None:
        """Why the verifier cannot be launched before an attempt begins, if it cannot.

        Only the launching port can answer; a port without ``launch_unavailable``
        is not pre-checked, and its launch errors are typed after the fact.
        """
        check = getattr(self.process_port, "launch_unavailable", None)
        reason = check(verifier_argv) if callable(check) else None
        if reason is None:
            try:
                self.workspace_root.mkdir(parents=True, exist_ok=True)
                writable = os.access(self.workspace_root, os.W_OK)
            except OSError:
                writable = False
            if not writable:
                reason = "workspace_root_not_writable"
        return f"{VERIFIER_UNAVAILABLE}:{reason}" if reason is not None else None

    def _finish_verifier_unavailable(
        self,
        *,
        run_id: str,
        ordinal: int,
        attempt_fence: int,
        workspace_ref: str,
        base_revision: str,
        reason: str,
        provider: dict[str, Any] | None,
        budget: _TimeoutBudget,
    ) -> dict[str, Any]:
        """End an attempt whose verifier failed to launch: typed, never raised."""
        invocations = 0 if provider is None else 1
        provider = provider or {"summary": "verifier could not be launched; model not invoked"}
        provider = {**provider, "failure": reason, "provider_invocations": invocations}
        provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(provider, sort_keys=True), budget)
        empty_diff = self._write_artifact(run_id, ordinal, "diff.patch", "", budget)
        receipt = self._bind_dispatch({
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": run_id,
            "attempt": ordinal,
            "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": base_revision},
            "provider": {"summary": provider["summary"], "artifact": provider_ref, "provider_invocations": invocations},
            "usage": token_cost.unknown_usage(reason="verifier could not be launched"),
            "diff": empty_diff,
            "verification": {"dispatched": False, "reason": reason},
            "routing": {"route": "human_required", "reason": reason},
        })
        receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
        if not self.store.finish_attempt(run_id, ordinal, state="human_required", receipt_ref=receipt_ref["ref"],
                                         receipt_digest=receipt_ref["digest"], fence=attempt_fence):
            return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": attempt_fence}
        self.store.append_event(
            run_id,
            VERIFIER_UNAVAILABLE,
            {"attempt": ordinal, "reason": reason, "provider_invocations": invocations},
            event_id=f"verifier-unavailable:{run_id}:{ordinal}",
        )
        return {"status": "human_required", "run_id": run_id, "attempt": ordinal, "reason": reason,
                "provider_invocations": invocations, "receipt_ref": receipt_ref["ref"],
                "receipt_digest": receipt_ref["digest"]}

    @staticmethod
    def _staging_error(add: ProcessResult, diff: ProcessResult) -> str | None:
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

    def _delivery_preflight(self, run_id: str, *, phase: str) -> dict[str, Any]:
        """Check the persisted Run binding before any executable effect."""
        result = self.store.delivery_preflight(run_id, phase=phase)
        if result.get("verdict") != "GREEN" and result.get("required"):
            planned = self.store.plan_pending_delivery(run_id)
            if planned.get("status") == "delivery_bound":
                result = self.store.delivery_preflight(run_id, phase=phase)
                if result.get("verdict") == "GREEN":
                    return result
            # Missing/invalid contract is a durable planning/migration request,
            # not permission to run a standalone attempt or to let the caller
            # promote itself with request metadata.
            return self.store.record_planning_request(
                run_id,
                reason=str(result.get("reason", "delivery_binding_invalid")),
                phase=phase,
            ) | {"verdict": "RED", "required": True}
        return result

    def _record_delivery_from_provider(
        self,
        *,
        run_id: str,
        ordinal: int,
        fence: int,
        phase: str,
        provider: Mapping[str, Any],
        workspace: Path | None = None,
        diff_digest: str | None = None,
        changed_paths: list[str] | None = None,
        checker: dict[str, Any] | None = None,
        provider_ref: dict[str, str] | None = None,
        dispatch_key: str | None = None,
        terminal_state: str | None = None,
    ) -> dict[str, Any]:
        """Assemble delivery evidence from Run/checker facts, never the model.

        The CLI model contract intentionally returns only its bounded summary,
        usage and execution metadata.  RunStore assembles the packet, receipts,
        obligations and verifier package from the persisted contract plus the
        actual diff/checker result.  Any provider ``delivery_evidence`` field is
        ignored, so a fake GREEN package cannot cross this gate.
        """
        if not self.store.delivery_required(run_id):
            return {"verdict": "RED", "required": True, "reason": "delivery_binding_missing", "phase": phase}
        if workspace is None or not isinstance(diff_digest, str) or not isinstance(changed_paths, list):
            return {"verdict": "RED", "required": True, "reason": "delivery_runtime_facts_missing", "phase": phase}
        executed = self.store.run_delivery_obligations(run_id, workspace, phase=phase)
        if not isinstance(executed, dict) or not isinstance(executed.get("obligations"), dict):
            return {"verdict": "RED", "required": True, "reason": "delivery_obligations_missing", "phase": phase}
        if dispatch_key is None:
            dispatch_key = f"run:{run_id}:attempt:{ordinal}:diff:{diff_digest}"
        evidence = self.store.assemble_delivery_evidence(
            run_id,
            ordinal,
            phase=phase,
            worktree=workspace,
            diff_digest=diff_digest,
            changed_paths=changed_paths,
            dispatch_key=dispatch_key,
            obligations=executed["obligations"],
            checker=checker,
            provider=dict(provider),
            provider_ref=provider_ref,
            terminal_state=terminal_state,
        )
        if evidence.get("verdict") != "GREEN":
            return {**evidence, "required": True}
        if phase == "final" and terminal_state in {"verified", "integrated"}:
            # The final package must be committed together with the terminal
            # state; finish_attempt_with_delivery performs that atomic write.
            return {**evidence, "required": True, "pending_terminal": True, "evidence": evidence}
        return self.store.record_delivery_evidence(
            run_id, ordinal, phase=phase, evidence=evidence, fence=fence,
        )

    def _finish_attempt_after_receipt(
        self,
        *,
        run_id: str,
        ordinal: int,
        state: str,
        receipt_ref: dict[str, str],
        terminal_evidence: dict[str, Any] | None,
        fence: int,
        effective_max_attempts: int,
    ) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
        """Finish one receipt, retaining bounded delivery retry semantics."""
        finish_kwargs: dict[str, Any] = {
            "run_id": run_id,
            "ordinal": ordinal,
            "state": state,
            "receipt_ref": receipt_ref["ref"],
            "receipt_digest": receipt_ref["digest"],
            "fence": fence,
        }
        if state == "verified" and terminal_evidence is not None:
            finish_result = self.store.finish_attempt_with_delivery_result(**finish_kwargs, evidence=terminal_evidence)
            finished = finish_result.get("ok") is True
        else:
            finish_result = {"ok": True, "kind": "ordinary_finish"}
            finished = self.store.finish_attempt(**finish_kwargs)
        if finished:
            return state, None, None
        if finish_result.get("committed") is True:
            return state, {
                "status": "delivery_rejected",
                "run_id": run_id,
                "attempt": ordinal,
                "reason": "delivery_final_readback_failed_after_commit",
                "delivery": finish_result,
            }, None
        fence_rejected = {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
        if finish_result.get("kind") in {"fence_rejected", "ordinary_finish"}:
            return state, fence_rejected, None
        retry_state = "human_required" if ordinal >= effective_max_attempts else "retry_pending"
        finish_kwargs["state"] = retry_state
        if not self.store.finish_attempt(**finish_kwargs):
            return retry_state, fence_rejected, None
        return retry_state, None, finish_result

    def _materialized_diff_facts(
        self,
        workspace: Path,
        *,
        budget: _TimeoutBudget,
    ) -> dict[str, Any]:
        """Hash the candidate that is actually materialized in the clone.

        A durable source receipt names the exact staged patch and paths.  The
        final phase must re-materialize that same patch, not trust a caller's
        copied ``diff_digest``/``changed_paths`` fields.  Staging here also
        makes newly-created files visible to ``git diff``.
        """
        add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
        diff = self._run(
            ["git", "-C", str(workspace), "diff", "--cached", "--binary", "--relative"],
            cwd=None,
            budget=budget,
        )
        if add.returncode != 0 or diff.returncode != 0:
            return {
                "verdict": "RED",
                "reason": "delivery_candidate_diff_unreadable",
                "digest": None,
                "changed_paths": [],
            }
        changed_paths = [
            parts[3][2:]
            for line in diff.stdout.splitlines()
            if line.startswith("diff --git ")
            for parts in [line.split()]
            if len(parts) >= 4 and parts[3].startswith("b/")
        ]
        return {
            "verdict": "GREEN",
            "digest": _digest(diff.stdout),
            "changed_paths": sorted(set(changed_paths)),
            "bytes": len(diff.stdout.encode("utf-8")),
        }

    def _assemble_external_final_delivery(
        self,
        *,
        run_id: str,
        conclusion: Mapping[str, Any],
        normalized: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """Run final-only obligations on the durable source candidate after CI."""
        run = self.store.get_run(run_id)
        latest = self.store.latest_attempt(run_id)
        if not isinstance(latest, dict) or not isinstance(latest.get("ordinal"), int):
            return None, {"verdict": "RED", "reason": "delivery_attempt_missing", "phase": "final"}
        source = self.store.delivery_evidence(run_id, phase="source", ordinal=int(latest["ordinal"]))
        if not isinstance(source, dict):
            return None, {"verdict": "RED", "reason": "delivery_source_evidence_missing", "phase": "final"}
        binding = self.store.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return None, {"verdict": "RED", "reason": binding.get("reason", "delivery_binding_invalid"), "phase": "final"}
        if conclusion.get("conclusion") != "success":
            return None, {"verdict": "RED", "reason": "delivery_external_conclusion_not_success", "phase": "final"}
        normalized_projection = dict(normalized) if isinstance(normalized, Mapping) else None
        if (
            not isinstance(normalized_projection, dict)
            or normalized_projection.get("status") not in {"ready", "already_normalized"}
            or normalized_projection.get("outcome") != "verified"
        ):
            return None, {"verdict": "RED", "reason": "delivery_normalized_ci_evidence_missing", "phase": "final"}
        normalized_digest = delivery_engine.digest_json(normalized_projection)
        external = {
            "op_key": conclusion.get("op_key"),
            "conclusion": conclusion.get("conclusion"),
            "normalized": normalized_projection,
            "normalized_digest": normalized_digest,
        }
        ordinal = int(latest["ordinal"])
        budget = _TimeoutBudget(self.timeout_seconds, self.deadline_port)
        workspace: Path | None = None
        cleanup: Path | None = None
        patch_path: Path | None = None
        try:
            workspace, _workspace_ref, cleanup = self._workspace(run, ordinal, budget)
            patch = self.store.read_artifact(run_id, ordinal, "diff.patch")
            if not isinstance(patch, str) or not patch.strip():
                return None, {"verdict": "RED", "reason": "delivery_source_diff_missing", "phase": "final"}
            source_diff_digest = source.get("diff_digest")
            if not isinstance(source_diff_digest, str) or _digest(patch) != source_diff_digest:
                return None, {"verdict": "RED", "reason": "delivery_source_diff_digest_mismatch", "phase": "final"}
            # Keep the patch beside (not inside) the disposable Git clone;
            # otherwise ``git add -A`` would turn this transport artifact into
            # part of the candidate being compared with the source receipt.
            patch_path = cleanup.parent / f".lh-source-diff-{ordinal}.patch"
            patch_path.write_text(patch, encoding="utf-8", newline="")
            applied = self._run(
                ["git", "-C", str(workspace), "apply", "--binary", "--whitespace=nowarn", str(patch_path)],
                cwd=None,
                budget=budget,
            )
            if applied.returncode != 0:
                return None, {"verdict": "RED", "reason": "delivery_source_diff_apply_failed", "phase": "final"}
            materialized_before = self._materialized_diff_facts(workspace, budget=budget)
            expected_paths = sorted(set(source.get("changed_paths") or []))
            if (
                materialized_before.get("verdict") != "GREEN"
                or materialized_before.get("digest") != source_diff_digest
                or materialized_before.get("changed_paths") != expected_paths
            ):
                return None, {
                    "verdict": "RED",
                    "reason": "delivery_source_candidate_mismatch",
                    "phase": "final",
                    "detail": {
                        "expected_digest": source_diff_digest,
                        "observed_digest": materialized_before.get("digest"),
                        "expected_paths": expected_paths,
                        "observed_paths": materialized_before.get("changed_paths"),
                    },
                }
            executed = self.store.run_delivery_obligations(run_id, workspace, phase="final")
            if not isinstance(executed, dict) or executed.get("verdict") != "GREEN":
                return None, {**(executed if isinstance(executed, dict) else {}), "verdict": "RED", "phase": "final"}
            materialized_after = self._materialized_diff_facts(workspace, budget=budget)
            if (
                materialized_after.get("verdict") != "GREEN"
                or materialized_after.get("digest") != source_diff_digest
                or materialized_after.get("changed_paths") != expected_paths
            ):
                return None, {"verdict": "RED", "reason": "delivery_final_candidate_mutated", "phase": "final"}
            source_identity = source.get("identity") if isinstance(source.get("identity"), dict) else {}
            final = self.store.assemble_delivery_evidence(
                run_id,
                ordinal,
                phase="final",
                worktree=workspace,
                diff_digest=source_diff_digest,
                changed_paths=expected_paths,
                dispatch_key=str(source.get("dispatch_key") or f"run:{run_id}:attempt:{ordinal}"),
                obligations=executed["obligations"],
                checker={
                    "argv": ["normalized-ci", str(conclusion.get("op_key") or "unknown")],
                    "exit_code": 0,
                    "stdout": json.dumps(normalized_projection, sort_keys=True),
                    "stderr": "",
                },
                provider={"summary": "controller assembled normalized external final"},
                external_readback=external,
                terminal_state="verified",
            )
            if final.get("verdict") != "GREEN":
                return None, {**final, "phase": "final"}
            return final, {"verdict": "GREEN", "phase": "final", "contract_digest": binding["contract_digest"], "identity": source_identity}
        except (OSError, RuntimeError, AttemptTimeout) as exc:
            return None, {"verdict": "RED", "reason": f"delivery_final_materialization_failed:{type(exc).__name__}", "phase": "final"}
        finally:
            if patch_path is not None:
                patch_path.unlink(missing_ok=True)
            if cleanup is not None:
                remove_tree(cleanup)

    def tick(self, run_id: str, *, holder: str, model: ModelRunner, verifier_argv: list[str], grill_note: str | None = None, knowledge_context: dict[str, Any] | None = None) -> dict[str, Any]:
        self.store.recover_stale_run(run_id)
        if not self.store.acquire_lease(run_id, holder, seconds=self.lease_seconds):
            return {"status": "lease_busy", "run_id": run_id}
        workspace: Path | None = None
        workspace_cleanup: Path | None = None
        ordinal: int | None = None
        try:
            budget = _TimeoutBudget(self.timeout_seconds, self.deadline_port)
            run = self.store.get_run(run_id)
            if run["state"] not in {"queued", "retry_pending"}:
                return {"status": "not_runnable", "run_id": run_id, "state": run["state"]}
            delivery_preflight = self._delivery_preflight(run_id, phase="source")
            if delivery_preflight.get("verdict") != "GREEN":
                return {
                    "status": "planning_required",
                    "run_id": run_id,
                    "reason": delivery_preflight.get("reason", "delivery_binding_invalid"),
                    "delivery": delivery_preflight,
                }
            remediation_case = self.store.failure_case_for_run(
                run_id,
                states={"remediation_running"},
            )
            unready = self._verifier_unready(verifier_argv)
            if unready is not None:
                # No attempt, no model call: record once per pending attempt and reason, look again next tick.
                self.store.append_event(
                    run_id,
                    VERIFIER_UNAVAILABLE,
                    {"reason": unready, "attempt": None, "provider_invocations": 0},
                    event_id=f"verifier-unavailable:{run_id}:pending:{run['attempts']}:{_digest(unready)[7:23]}",
                )
                return {"status": "waiting_for_verifier", "run_id": run_id, "reason": unready}
            ordinal_hint = run["attempts"] + 1
            workspace_ref = f"workspace://{run_id}/{ordinal_hint}"
            try:
                ordinal = self.store.begin_attempt(run_id, workspace_ref)
            except ValueError as exc:
                if str(exc) == "repair_attempt_budget_exhausted":
                    return {"status": "human_required", "run_id": run_id, "reason": str(exc)}
                raise
            workspace_cleanup = self.workspace_root / run_id / str(ordinal)
            workspace, workspace_ref, workspace_cleanup = self._workspace(run, ordinal, budget)
            fence = self.store.attempt_fence(run_id, ordinal)
            goal = run.get("goal") if isinstance(run.get("goal"), dict) else {}
            envelope = goal.get("admission_envelope") if isinstance(goal.get("admission_envelope"), dict) else {}
            requires_non_empty_diff = envelope.get("requires_non_empty_diff") is True
            # W3 lamp precheck: if the acceptance lamp already passes on the
            # untouched base, the work was already done — finish verified
            # without spending a model invocation.  A bounded edit requirement
            # always disables this shortcut.
            try:
                pre = self._run(verifier_argv, cwd=workspace, budget=budget)
            except AttemptTimeout:
                pre = None
                budget.reset()
            except OSError as exc:  # a launch failure; timeouts (also OSError) are handled above
                if isinstance(exc, TimeoutError):
                    raise
                return self._finish_verifier_unavailable(
                    run_id=run_id, ordinal=ordinal, attempt_fence=fence, workspace_ref=workspace_ref,
                    base_revision=run["base_revision"], reason=f"{VERIFIER_UNAVAILABLE}:launch_failed:{type(exc).__name__}",
                    provider=None, budget=budget,
                )
            if pre is not None and pre.returncode == 0:
                pre_add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
                pre_diff = self._run(["git", "-C", str(workspace), "diff", "--cached", "--binary", "--relative"], cwd=None, budget=budget)
            prechecked = (
                pre is not None
                and pre.returncode == 0
                and not requires_non_empty_diff
                and self._staging_error(pre_add, pre_diff) is None
            )
            execution_fence_projection: dict[str, Any] | None = None
            if prechecked:
                pre_binding = cr.deterministic_binding(
                    run_id=run_id,
                    attempt=ordinal,
                    base_revision=run["base_revision"],
                    verifier_argv=verifier_argv,
                    goal=run["goal"],
                )
                provider = {
                    "summary": "lamp precheck passed without model invocation",
                    "precheck": True,
                    "binding_receipt": pre_binding,
                }
                add, diff, verified = pre_add, pre_diff, pre
                staging_error = None
                diff_bytes = len(diff.stdout.encode("utf-8"))
                files_touched: list[str] = []
                bounded_edit_failure = None
            else:
                capsule = {"run_id": run_id, "attempt": ordinal, "fence": fence, "timeout_seconds": budget.timeout(), "goal": run["goal"], "base_revision": run["base_revision"], "workspace_ref": workspace_ref, "verification_commands": verifier_argv}
                advisory: dict[str, Any] = {}
                if grill_note is not None:
                    # W6a: challenger diagnosis for the final attempt — guidance
                    # only, additive to the capsule the executor already receives.
                    advisory["grill_note"] = grill_note
                if knowledge_context is not None:
                    # Advisory context only: it cannot alter admission, verifier,
                    # scope, budget, or promotion ownership.
                    advisory["knowledge_context"] = knowledge_context
                if advisory:
                    # Raw advisory stays controller-only; the capsule carries the
                    # bounded projection goal-lifecycle-v1 requires.
                    projection, texts = provider_inputs.project_context(advisory)
                    capsule["provider_context_projection"] = projection
                    capsule["provider_context_texts"] = texts
                if execution_fences.model_requires_fence(model):
                    try:
                        descriptor, execution_fence_projection = (
                            self._prepare_execution_fence(
                                model=model,
                                run=run,
                                ordinal=ordinal,
                                attempt_fence=fence,
                                workspace=workspace,
                                verifier_argv=verifier_argv,
                                budget=budget,
                            )
                        )
                    except execution_fences.ExecutionFenceUnavailable as exc:
                        return self._finish_execution_fence_unavailable(
                            run_id=run_id,
                            ordinal=ordinal,
                            attempt_fence=fence,
                            workspace_ref=workspace_ref,
                            base_revision=run["base_revision"],
                            reason=exc.reason,
                            budget=budget,
                            mode="local",
                        )
                    capsule["execution_fence"] = descriptor
                source_refs_before = self._source_refs_digest(run, budget)
                try:
                    provider = model(workspace, capsule)
                    if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
                        raise ValueError("model runner must return a dict with a bounded summary")
                except execution_fences.ExecutionFenceUnavailable as exc:
                    provider = {
                        "summary": "execution fence unavailable; model not invoked",
                        "failure": f"{execution_fences.ERROR_CODE}: {exc.reason}",
                        "provider_invocations": 0,
                        "execution_fence": {
                            "status": "unavailable",
                            "error_code": execution_fences.ERROR_CODE,
                            "reason": exc.reason,
                            "mutation_dispatch": "disabled",
                        },
                        "routing": {
                            "route": "human_required",
                            "reason": execution_fences.ERROR_CODE,
                        },
                    }
                except Exception as exc:  # Provider failures are facts for the next controller tick.
                    provider = {"summary": "model invocation failed", "failure": f"{type(exc).__name__}: {exc}"}
                provider = self._source_refs_guard(provider, source_refs_before, self._source_refs_digest(run, budget))
                # Stage everything first so new/untracked files the executor created
                # appear in the diff; a plain `git diff` omits them, which would make
                # the value reducer see an empty diff for a real new-file change.
                add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
                diff = self._run(["git", "-C", str(workspace), "diff", "--cached", "--binary", "--relative"], cwd=None, budget=budget)
                staging_error = self._staging_error(add, diff)
                diff_bytes = len(diff.stdout.encode("utf-8"))
                files_touched = [
                    parts[3][2:]
                    for line in diff.stdout.splitlines()
                    if line.startswith("diff --git ")
                    for parts in [line.split()]
                    if len(parts) >= 4 and parts[3].startswith("b/")
                ]
                bounded_edit_failure = None
                if requires_non_empty_diff and not diff.stdout.strip():
                    bounded_edit_failure = "bounded_repo_edit_required_but_diff_empty"
                    provider.setdefault("failure", bounded_edit_failure)
                verified = None
                if "failure" not in provider and staging_error is None:
                    try:
                        verified = self._run(verifier_argv, cwd=workspace, budget=budget)
                    except AttemptTimeout as exc:
                        stderr = exc.stderr.rstrip()
                        if stderr:
                            stderr += "\n"
                        verified = ProcessResult(
                            tuple(verifier_argv),
                            124,
                            exc.stdout,
                            stderr + "verifier timed out",
                        )
                        budget.reset()
                    except OSError as exc:  # a launch failure; timeouts (also OSError) are handled above
                        if isinstance(exc, TimeoutError):
                            raise
                        return self._finish_verifier_unavailable(
                            run_id=run_id, ordinal=ordinal, attempt_fence=fence, workspace_ref=workspace_ref,
                            base_revision=run["base_revision"], reason=f"{VERIFIER_UNAVAILABLE}:launch_failed:{type(exc).__name__}",
                            provider=provider, budget=budget,
                        )
            provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(provider, sort_keys=True), budget)
            diff_ref = self._write_artifact(run_id, ordinal, "diff.patch", diff.stdout, budget)
            stdout = verified.stdout if verified else ""
            stderr = verified.stderr if verified else staging_error or provider["failure"]
            stdout_ref = self._write_artifact(run_id, ordinal, "verifier.stdout", stdout, budget)
            stderr_ref = self._write_artifact(run_id, ordinal, "verifier.stderr", stderr, budget)
            if prechecked:
                pre_binding["evidence_refs"] = [provider_ref, diff_ref, stdout_ref, stderr_ref]
            exit_code = verified.returncode if verified else -1
            effective_max_attempts = self.store.effective_max_attempts(run_id)
            routing = provider.get("routing") if isinstance(provider.get("routing"), dict) else None
            routing_route = routing.get("route") if isinstance(routing, dict) else None
            if routing_route == "human_required":
                state = "human_required"
            elif routing_route == "stop":
                state = "stopped"
            else:
                state = "verified" if exit_code == 0 else "stopped" if ordinal >= effective_max_attempts else "retry_pending"
            repeated_failure: dict[str, Any] | None = None
            if not prechecked and state == "retry_pending" and remediation_case is None:
                # FC-P0 precedence over the old W6b stop: two consecutive
                # attempts with an identical failure signature raise one
                # durable FailureCase.  The next worker tick must grill it
                # before another executor dispatch; the run stays retryable.
                signature = {"exit_code": exit_code, "diff_digest": diff_ref["digest"]}
                if self._previous_failure_signature(run_id, ordinal) == signature:
                    repeated_failure = {
                        "attempt": ordinal,
                        "max_attempts": effective_max_attempts,
                        "signature": signature,
                    }
            verification: dict[str, Any] = {"argv": verifier_argv, "exit_code": exit_code, "stdout": stdout_ref, "stderr": stderr_ref}
            if prechecked:
                verification["precheck"] = True
            delivery_result: dict[str, Any] | None = None
            terminal_delivery_evidence: dict[str, Any] | None = None
            if state == "verified" and self.store.delivery_required(run_id):
                sync_dispatch_key = eap.operation_key(run_id, "sync-delivery", diff_ref["digest"])
                delivery_result = self._record_delivery_from_provider(
                    run_id=run_id,
                    ordinal=ordinal,
                    fence=fence,
                    phase="final",
                    provider=provider,
                    workspace=workspace,
                    diff_digest=diff_ref["digest"],
                    changed_paths=files_touched,
                    checker={
                        "argv": verifier_argv,
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                    },
                    provider_ref=provider_ref,
                    dispatch_key=sync_dispatch_key,
                    terminal_state="verified",
                )
                terminal_delivery_evidence = delivery_result.get("evidence") if isinstance(delivery_result, dict) else None
                verification["delivery"] = {
                    key: value for key, value in (delivery_result or {}).items() if key != "evidence"
                }
                if delivery_result.get("verdict") != "GREEN":
                    state = "human_required" if ordinal >= effective_max_attempts else "retry_pending"
            if requires_non_empty_diff:
                verification["bounded_repo_edit"] = {
                    "required": True,
                    "observed": diff_bytes > 0,
                    "must_be_non_empty": True,
                    "diff_bytes": diff_bytes,
                    "files_touched": files_touched,
                    "reason": bounded_edit_failure,
                    "diff_ref": diff_ref,
                }
            if staging_error is not None:
                # W8-1: staging failed, so the verifier never ran and the
                # attempt cannot go green; the reason stays on the receipt.
                verification["staging_error"] = staging_error
            receipt = self._bind_dispatch({
                "schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
                "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": run["base_revision"]},
                "provider": {"summary": provider["summary"], "artifact": provider_ref},
                "usage": token_cost.unknown_usage(reason="lamp precheck: no model invocation") if prechecked else provider.get("usage") if isinstance(provider.get("usage"), dict) else token_cost.unknown_usage(reason="model did not report usage"),
                "diff": diff_ref,
                "verification": verification,
            })
            if routing is not None:
                receipt["routing"] = routing
            if execution_fence_projection is not None:
                receipt["execution_fence"] = execution_fence_projection
            elif isinstance(provider.get("execution_fence"), dict):
                receipt["execution_fence"] = provider["execution_fence"]
            binding_receipt = pre_binding if prechecked else provider.get("binding_receipt")
            if (
                isinstance(binding_receipt, dict)
                and (prechecked or binding_receipt.get("schema") == cr.BINDING_SCHEMA)
            ):
                if not prechecked:
                    binding_receipt = dict(binding_receipt)
                    binding_receipt["evidence_refs"] = [provider_ref, diff_ref, stdout_ref, stderr_ref]
                receipt["binding"] = binding_receipt
            receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
            state, finish_response, delivery_retry = self._finish_attempt_after_receipt(
                run_id=run_id,
                ordinal=ordinal,
                state=state,
                receipt_ref=receipt_ref,
                terminal_evidence=terminal_delivery_evidence,
                fence=fence,
                effective_max_attempts=effective_max_attempts,
            )
            if finish_response is not None:
                return finish_response
            if delivery_retry is not None and not prechecked:
                return {
                    "status": state,
                    "run_id": run_id,
                    "attempt": ordinal,
                    "delivery": delivery_retry,
                }
            failure_case = None
            if remediation_case is not None and exit_code != 0:
                failure_case = self.store.record_resolution(
                    run_id,
                    ordinal=ordinal,
                    receipt_digest=receipt_ref["digest"],
                    exit_code=exit_code,
                    checker_reason="deterministic_verifier_failed",
                )
            elif remediation_case is not None:
                # Defer PASS until GoalLoopWorker evaluates the value gate.
                failure_case = remediation_case
            elif repeated_failure is not None:
                failure_case = self.store.ensure_failure_case(
                    run_id,
                    signature=repeated_failure["signature"],
                    acceptance_argv=verifier_argv,
                    trigger="same_run_identical_signature_x2",
                    receipt_digest=receipt_ref["digest"],
                )
                self.store.append_event(
                    run_id,
                    "repeated_failure_detected",
                    {**repeated_failure, "failure_case_id": failure_case["failure_case_id"]},
                    event_id=f"failure-case:{failure_case['failure_case_id']}:repeated",
                )
            result = {
                "status": state,
                "run_id": run_id,
                "attempt": ordinal,
                "receipt_ref": receipt_ref["ref"],
                "receipt_digest": receipt_ref["digest"],
                "failure_case": failure_case,
            }
            if prechecked:
                result["precheck"] = True
            return result
        except AttemptTimeout as exc:
            return {"status": "attempt_timeout", "run_id": run_id, "attempt": ordinal, "reason": str(exc)}
        finally:
            cleanup = workspace_cleanup or workspace
            if cleanup is not None:
                remove_tree(cleanup)
            self.store.release_lease(run_id, holder)

    def startup(self) -> list[dict[str, Any]]:
        """Call once after process start before triggers dispatch individual runs."""
        return self.store.reconcile_startup()

    def tick_async(self, run_id: str, *, holder: str, model: ModelRunner, verdict_store: ev.VerdictStore,
                   action_ledger: eap.ActionLedger, adapter: eap.ExternalAdapter, action_id: str = "open-pr",
                   knowledge_context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Method A: execute, open an external action (PR) at most once, then PARK the run
        awaiting its async CI verdict. No local verifier — the verdict lands later via
        resume_external (poll-on-startup). An executor failure falls back to retry/stopped."""
        self.store.recover_stale_run(run_id)
        if not self.store.acquire_lease(run_id, holder, seconds=self.lease_seconds):
            return {"status": "lease_busy", "run_id": run_id}
        workspace: Path | None = None
        workspace_cleanup: Path | None = None
        ordinal: int | None = None
        try:
            budget = _TimeoutBudget(self.timeout_seconds, self.deadline_port)
            run = self.store.get_run(run_id)
            if run["state"] not in {"queued", "retry_pending"}:
                return {"status": "not_runnable", "run_id": run_id, "state": run["state"]}
            delivery_preflight = self._delivery_preflight(run_id, phase="source")
            if delivery_preflight.get("verdict") != "GREEN":
                return {
                    "status": "planning_required",
                    "run_id": run_id,
                    "reason": delivery_preflight.get("reason", "delivery_binding_invalid"),
                    "delivery": delivery_preflight,
            }
            ordinal_hint = run["attempts"] + 1
            workspace_ref = f"workspace://{run_id}/{ordinal_hint}"
            try:
                ordinal = self.store.begin_attempt(run_id, workspace_ref)
            except ValueError as exc:
                if str(exc) == "repair_attempt_budget_exhausted":
                    return {"status": "human_required", "run_id": run_id, "reason": str(exc)}
                raise
            workspace_cleanup = self.workspace_root / run_id / str(ordinal)
            workspace, workspace_ref, workspace_cleanup = self._workspace(run, ordinal, budget)
            fence = self.store.attempt_fence(run_id, ordinal)
            capsule = {"run_id": run_id, "attempt": ordinal, "fence": fence, "timeout_seconds": budget.timeout(), "goal": run["goal"], "base_revision": run["base_revision"], "workspace_ref": workspace_ref, "verification_commands": ["external_async", action_id]}
            if knowledge_context is not None:
                projection, texts = provider_inputs.project_context(
                    {"knowledge_context": knowledge_context})
                capsule["provider_context_projection"] = projection
                capsule["provider_context_texts"] = texts
            execution_fence_projection: dict[str, Any] | None = None
            if execution_fences.model_requires_fence(model):
                try:
                    descriptor, execution_fence_projection = (
                        self._prepare_execution_fence(
                            model=model,
                            run=run,
                            ordinal=ordinal,
                            attempt_fence=fence,
                            workspace=workspace,
                            verifier_argv=["external_async", action_id],
                            budget=budget,
                        )
                    )
                except execution_fences.ExecutionFenceUnavailable as exc:
                    return self._finish_execution_fence_unavailable(
                        run_id=run_id,
                        ordinal=ordinal,
                        attempt_fence=fence,
                        workspace_ref=workspace_ref,
                        base_revision=run["base_revision"],
                        reason=exc.reason,
                        budget=budget,
                        mode="external_async",
                    )
                capsule["execution_fence"] = descriptor
            source_refs_before = self._source_refs_digest(run, budget)
            try:
                provider = model(workspace, capsule)
                if not isinstance(provider, dict) or not isinstance(provider.get("summary"), str):
                    raise ValueError("model runner must return a dict with a bounded summary")
            except execution_fences.ExecutionFenceUnavailable as exc:
                provider = {
                    "summary": "execution fence unavailable; model not invoked",
                    "failure": f"{execution_fences.ERROR_CODE}: {exc.reason}",
                    "provider_invocations": 0,
                    "execution_fence": {
                        "status": "unavailable",
                        "error_code": execution_fences.ERROR_CODE,
                        "reason": exc.reason,
                        "mutation_dispatch": "disabled",
                    },
                    "routing": {
                        "route": "human_required",
                        "reason": execution_fences.ERROR_CODE,
                    },
                }
            except Exception as exc:
                provider = {"summary": "model invocation failed", "failure": f"{type(exc).__name__}: {exc}"}
            provider = self._source_refs_guard(provider, source_refs_before, self._source_refs_digest(run, budget))
            # Stage everything first so new/untracked files the executor created
            # appear in the diff — a plain `git diff` omits them, which would
            # hand the external adapter an empty patch for a real new-file change.
            add = self._run(["git", "-C", str(workspace), "add", "-A"], cwd=None, budget=budget)
            diff = self._run(["git", "-C", str(workspace), "diff", "--cached", "--binary", "--relative"], cwd=None, budget=budget)
            staging_error = self._staging_error(add, diff)
            diff_ref = self._write_artifact(run_id, ordinal, "diff.patch", diff.stdout, budget)
            provider_ref = self._write_artifact(run_id, ordinal, "provider.json", json.dumps(provider, sort_keys=True), budget)
            files_touched = [
                parts[3][2:]
                for line in diff.stdout.splitlines()
                if line.startswith("diff --git ")
                for parts in [line.split()]
                if len(parts) >= 4 and parts[3].startswith("b/")
            ]
            base_receipt = self._bind_dispatch({"schema": "loop-hybrid-attempt-receipt/v1", "run_id": run_id, "attempt": ordinal,
                            "workspace": {"ref": workspace_ref, "disposable": True, "disposed": True, "base_revision": run["base_revision"]},
                            "provider": {"summary": provider["summary"], "artifact": provider_ref}, "diff": diff_ref})
            if execution_fence_projection is not None:
                base_receipt["execution_fence"] = execution_fence_projection
            elif isinstance(provider.get("execution_fence"), dict):
                base_receipt["execution_fence"] = provider["execution_fence"]
            if isinstance(provider.get("routing"), dict):
                base_receipt["routing"] = provider["routing"]
            dispatch_error: str | None = None
            if "failure" in provider:
                dispatch_error = provider["failure"]
            elif staging_error is not None:
                dispatch_error = staging_error
            if dispatch_error is not None:
                run_after = self.store.get_run(run_id)
                route = (
                    provider.get("routing", {}).get("route")
                    if isinstance(provider.get("routing"), dict)
                    else None
                )
                state = (
                    "human_required"
                    if route == "human_required"
                    else "stopped"
                    if ordinal >= self.store.effective_max_attempts(run_id)
                    else "retry_pending"
                )
                receipt = {**base_receipt, "verification": {"mode": "external_async", "dispatched": False, "reason": dispatch_error}}
                receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
                if not self.store.finish_attempt(run_id, ordinal, state=state, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                    return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
                return {"status": state, "run_id": run_id, "attempt": ordinal}
            delivery_result: dict[str, Any] | None = None
            op_key = eap.operation_key(run_id, action_id, diff_ref["digest"])
            if self.store.delivery_required(run_id):
                delivery_result = self._record_delivery_from_provider(
                    run_id=run_id,
                    ordinal=ordinal,
                    fence=fence,
                    phase="source",
                    provider=provider,
                    workspace=workspace,
                    diff_digest=diff_ref["digest"],
                    changed_paths=files_touched,
                    provider_ref=provider_ref,
                    dispatch_key=op_key,
                )
                if delivery_result.get("verdict") != "GREEN":
                    run_after = self.store.get_run(run_id)
                    state = "human_required" if ordinal >= self.store.effective_max_attempts(run_id) else "retry_pending"
                    receipt = {
                        **base_receipt,
                        "verification": {
                            "mode": "external_async",
                            "dispatched": False,
                            "reason": delivery_result.get("reason", "delivery_source_not_green"),
                            "delivery": delivery_result,
                        },
                    }
                    receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
                    if not self.store.finish_attempt(run_id, ordinal, state=state, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                        return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
                    return {"status": state, "run_id": run_id, "attempt": ordinal, "delivery": delivery_result}
            try:
                dispatched = ev.dispatch_external(verdict_store, action_ledger, adapter, run_id=run_id, op_key=op_key,
                                                  request={"diff_digest": diff_ref["digest"], "workspace_ref": workspace_ref, "action_id": action_id}, at=time.time())
            except Exception as exc:
                # An adapter failure is an attempt failure, not a driver crash:
                # record it on the receipt and let retry/stopped semantics decide.
                run_after = self.store.get_run(run_id)
                state = "stopped" if ordinal >= self.store.effective_max_attempts(run_id) else "retry_pending"
                reason = f"external dispatch failed: {type(exc).__name__}: {exc}"
                receipt = {**base_receipt, "verification": {"mode": "external_async", "dispatched": False, "reason": reason[:500]}}
                receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
                if not self.store.finish_attempt(run_id, ordinal, state=state, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                    return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
                return {"status": state, "run_id": run_id, "attempt": ordinal}
            receipt = {**base_receipt, "verification": {"mode": "external_async", "dispatched": True, "op_key": op_key, "external": dispatched["external"], **({"delivery": delivery_result} if delivery_result is not None else {})}}
            receipt_ref = self._write_artifact(run_id, ordinal, "receipt.json", json.dumps(receipt, sort_keys=True), budget)
            if not self.store.park_external_verdict(run_id, ordinal, receipt_ref=receipt_ref["ref"], receipt_digest=receipt_ref["digest"], fence=fence):
                return {"status": "fence_rejected", "run_id": run_id, "attempt": ordinal, "fence": fence}
            return {"status": "awaiting_external_verdict", "run_id": run_id, "attempt": ordinal, "op_key": op_key}
        except AttemptTimeout as exc:
            return {"status": "attempt_timeout", "run_id": run_id, "attempt": ordinal, "reason": str(exc)}
        finally:
            cleanup = workspace_cleanup or workspace
            if cleanup is not None:
                remove_tree(cleanup)
            self.store.release_lease(run_id, holder)

    def resume_external(
        self,
        *,
        verdict_store: ev.VerdictStore,
        source: ev.ConclusionSource,
        normalizer: Callable[..., dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Poll-on-startup: resolve every parked run whose external verdict has landed.

        With a normalizer, a success conclusion crosses the run to verified
        only after the LH-owned normalized record and its ready event are
        durable (goal-lifecycle-v1 asynchronous boundary). The recovery pass
        re-offers conclusions that landed before a crash while their run
        never crossed; it scans only runs still parked, so it stays bounded
        by crash remnants and not by history.
        """
        resumed = ev.poll_and_resume(verdict_store, source, at=time.time())
        if normalizer is not None:
            seen = {row["run_id"] for row in resumed}
            for record in verdict_store.resolved_verified():
                if record["run_id"] in seen:
                    continue
                try:
                    run = self.store.get_run(record["run_id"])
                except (KeyError, ValueError):
                    continue
                if run["state"] != "awaiting_external_verdict":
                    continue
                resumed.append({"run_id": record["run_id"], "op_key": record["op_key"],
                                "conclusion": record["conclusion"], "state": "verified",
                                "recovered": True})
        for row in resumed:
            if normalizer is not None and row["state"] == "verified":
                normalized = normalizer(run_id=row["run_id"], op_key=row["op_key"],
                                        conclusion=row["conclusion"])
                row["normalized"] = normalized
                if (
                    not isinstance(normalized, dict)
                    or
                    normalized.get("status") not in {"ready", "already_normalized"}
                    or normalized.get("outcome") != "verified"
                ):
                    # Late, conflicting or unnormalizable evidence must not
                    # cross the run; the typed reason travels on the row.
                    continue
            if row.get("state") == "verified" and self.store.delivery_required(row["run_id"]):
                normalized = row.get("normalized")
                final_evidence, delivery_gate = self._assemble_external_final_delivery(
                    run_id=row["run_id"],
                    conclusion=row,
                    normalized=normalized if isinstance(normalized, dict) else None,
                )
                if final_evidence is not None and delivery_gate.get("verdict") == "GREEN":
                    accepted = self.store.resolve_external_verdict_with_delivery(
                        row["run_id"], final_evidence=final_evidence, new_state="verified"
                    )
                    row["delivery"] = delivery_gate if accepted else {
                        "verdict": "RED",
                        "reason": "delivery_final_readback_failed",
                        "phase": "final",
                    }
                    if not accepted:
                        row["state"] = "human_required"
                else:
                    row["delivery"] = delivery_gate
                    row["state"] = "human_required"
                if row.get("state") == "human_required":
                    try:
                        self.store.resolve_external_verdict(row["run_id"], "human_required")
                    except ValueError:
                        pass
                continue
            try:
                self.store.resolve_external_verdict(row["run_id"], row["state"])
            except ValueError:
                pass  # already resolved, or not tracked by this run_store
        return resumed

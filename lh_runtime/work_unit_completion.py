"""Durable completion effects for an admitted WorkUnit, independent of delivery watches.

Adapters are explicit capabilities, never success defaults. An interrupted effect
stays reserved for reconciliation; this journal does not promise exactly-once
execution across an unknown subprocess outcome.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import copy
from contextlib import contextmanager
from pathlib import Path
import subprocess
from typing import Any, Callable, Iterator, Mapping
import sys

from .work_unit_store import WorkUnitStore, digest_json
from .runner_adapter import PhaseJobPending

CANDIDATE_RECOVERY_ADMISSION_SCHEMA = "lh-candidate-recovery-admission/v1"
CHECKS_REPAIR_BINDING_SCHEMA = "lh-checks-repair-binding/v1"

try:
    from . import delivery_contract as delivery_unit_contract
except ImportError:  # direct LH canary execution
    import delivery_contract  # type: ignore


def git_readonly_env(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Preserve caller Git config while disabling optional lock writes."""
    env = {str(key): str(value) for key, value in (os.environ if base is None else base).items()}
    raw_count = env.get("GIT_CONFIG_COUNT", "0")
    try:
        count = int(raw_count)
    except (TypeError, ValueError) as exc:
        raise ValueError("GIT_CONFIG_COUNT must be a non-negative integer") from exc
    if count < 0:
        raise ValueError("GIT_CONFIG_COUNT must be a non-negative integer")
    for index in range(count):
        if f"GIT_CONFIG_KEY_{index}" not in env or f"GIT_CONFIG_VALUE_{index}" not in env:
            raise ValueError("GIT_CONFIG_COUNT has missing key/value pair")
    env["GIT_OPTIONAL_LOCKS"] = "0"
    return env


@contextmanager
def private_git_index(worktree: str, state_root: Path, *, label: str = "read",
                      base: Mapping[str, str] | None = None) -> Iterator[dict[str, str]]:
    """Run read ports against a disposable index copied from one workspace."""
    root = Path(worktree).resolve()
    clean = {key: value for key, value in (os.environ if base is None else base).items()
             if key != "GIT_INDEX_FILE"}
    env = git_readonly_env(clean)
    try:
        index_text = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "--git-path", "index"],
            env=env,
        ).decode().strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("completion_git_index_unreadable") from exc
    index = Path(index_text)
    if not index.is_absolute():
        index = root / index
    if not index.is_file() or index.is_symlink():
        raise ValueError("completion_git_index_missing")
    with tempfile.TemporaryDirectory(prefix=f"git-index-{label}-", dir=state_root) as scratch:
        private = Path(scratch) / "index"
        shutil.copy2(index, private)
        yield {**env, "GIT_INDEX_FILE": str(private)}


@contextmanager
def task_owned_check_environment(
    store_root: Path, worktree: str, *, base: Mapping[str, str],
) -> Iterator[tuple[dict[str, str], dict[str, str]]]:
    """Give one check batch disposable state roots outside the scheduler store."""
    store = Path(store_root).expanduser().resolve()
    configured = base.get("LH_HOST_TMP_ROOT")
    parent = Path(configured).expanduser().resolve() if configured else Path(tempfile.gettempdir()).resolve()
    production = Path.home().expanduser().resolve() / ".local" / "state" / "external-host"
    if parent == production or production in parent.parents:
        raise ValueError("completion_check_tmp_root_production")
    if parent == store or store in parent.parents:
        raise ValueError("completion_check_tmp_root_store_overlap")
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="completion-check-", dir=parent) as scratch:
        root = Path(scratch)
        state_root = root / "completion-state"
        tmp_root = root / "completion-tmp"
        state_root.mkdir()
        tmp_root.mkdir()
        replacements = {
            "${WORKTREE}": worktree,
            "${TASK_OWNED_STATE_ROOT}": str(state_root),
            "${TASK_OWNED_TMP_ROOT}": str(tmp_root),
        }
        env = {
            **base,
            "LH_HOST_STATE_ROOT": str(state_root),
            "LH_HOST_TMP_ROOT": str(tmp_root),
            "PYTHONDONTWRITEBYTECODE": "1",
            "LH_ROOT": str(Path(worktree) / "loop-hybrid"),
        }
        yield env, replacements


def child_check_environment(base: Mapping[str, str]) -> dict[str, str]:
    """Detach child-owned Git repositories from controller read namespaces."""
    internal = {"GIT_INDEX_FILE", "GIT_OPTIONAL_LOCKS", "GIT_DIR",
                "GIT_WORK_TREE", "GIT_COMMON_DIR"}
    return {key: value for key, value in base.items() if key not in internal}


def candidate_digest(worktree: str, *, env: Mapping[str, str] | None = None) -> str:
    root = Path(worktree).resolve()
    def git(*args: str) -> bytes:
        return subprocess.check_output(
            ["git", "-C", str(root), *args],
            env=git_readonly_env(env),
        )
    files = git("ls-files", "-z", "--cached", "--others", "--exclude-standard").split(b"\0")
    entries = []
    for raw in sorted(set(files)):
        if not raw:
            continue
        name = raw.decode("utf-8")
        path = root / name
        if path.is_symlink():
            raise ValueError("completion_symlink_unsupported")
        if path.is_dir():
            raise ValueError("completion_submodule_unsupported")
        entries.append([name, path.stat().st_mode if path.exists() else None,
                        hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None])
    return digest_json({"head": git("rev-parse", "HEAD").decode().strip(), "files": entries})


def candidate_inventory(worktree: str, *, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Read the source candidate identity without touching its index or refs."""
    root = Path(worktree).resolve()
    readonly = git_readonly_env(env)
    try:
        head = subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"], env=readonly,
        ).decode().strip()
        names = subprocess.check_output(
            ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            env=readonly,
        ).split(b"\0")
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("candidate_recovery_source_inventory_unreadable") from exc
    files: list[dict[str, Any]] = []
    for raw in sorted(set(names)):
        if not raw:
            continue
        relative = raw.decode("utf-8")
        path = root / relative
        if path.is_symlink() or path.is_dir():
            raise ValueError("candidate_recovery_source_inventory_unsupported")
        files.append({
            "path": relative,
            "mode": path.stat().st_mode if path.exists() else None,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None,
        })
    return {"schema": "lh-candidate-source-inventory/v1", "root": str(root), "head": head,
            "files": files, "candidate_digest": candidate_digest(str(root), env=readonly)}


def candidate_inventory_digest(inventory: Mapping[str, Any]) -> str:
    if isinstance(inventory, (str, Path)):
        inventory = candidate_inventory(str(inventory))
    if not isinstance(inventory, Mapping) or inventory.get("schema") != "lh-candidate-source-inventory/v1":
        raise ValueError("candidate_recovery_source_inventory_invalid")
    return digest_json(dict(inventory))


def candidate_snapshot_copy(source: str | Path, state_root: Path, *, target: Path | None = None) -> str:
    """Materialize an untracked candidate into a task-owned disposable repo."""
    source_root = Path(source).resolve()
    if target is None:
        target = Path(tempfile.mkdtemp(prefix="candidate-recovery-", dir=state_root)) / "candidate"
    else:
        target = Path(target)
        if (not target.is_absolute() or not target.is_relative_to(state_root.resolve())
            or target.exists() or any(p.is_symlink() for p in (target, *target.parents))):
            raise ValueError("continuation_repair_snapshot_target_invalid")
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with private_git_index(str(source_root), state_root, label="candidate-recovery-source") as source_env:
        clone_env = {key: value for key, value in source_env.items() if key != "GIT_INDEX_FILE"}
        origin_result = subprocess.run(
            ["git", "-C", str(source_root), "remote", "get-url", "origin"],
            capture_output=True, text=True, check=False, env=clone_env,
        )
        source_origin = origin_result.stdout.strip() if origin_result.returncode == 0 else None
        subprocess.run(["git", "clone", "--no-hardlinks", "--quiet", str(source_root), str(target)],
                       check=True, env=clone_env)
        head = subprocess.check_output(["git", "-C", str(source_root), "rev-parse", "HEAD"], env=source_env).decode().strip()
        subprocess.run(["git", "-C", str(target), "checkout", "--detach", "--quiet", head],
                       check=True, env=clone_env)
        if source_origin:
            subprocess.run(["git", "-C", str(target), "remote", "set-url", "origin", source_origin],
                           check=True, env=clone_env)
        else:
            subprocess.run(["git", "-C", str(target), "remote", "remove", "origin"],
                           check=True, env=clone_env)
        names = subprocess.check_output(
            ["git", "-C", str(source_root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            env=source_env,
        ).split(b"\0")
        for raw in set(names):
            if not raw:
                continue
            relative = raw.decode("utf-8")
            origin, destination = source_root / relative, target / relative
            if origin.is_file() and not origin.is_symlink():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(origin, destination)
            elif not origin.exists() and destination.is_file():
                destination.unlink()
    return str(target)


def candidate_object(worktree: str, state_root: Path) -> str:
    """Create an unpublished content object without moving HEAD, refs or index."""
    with tempfile.TemporaryDirectory(prefix="candidate-index-", dir=state_root) as scratch:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(scratch) / "index"),
            "GIT_AUTHOR_NAME": "LH candidate snapshot", "GIT_AUTHOR_EMAIL": "lh@localhost",
            "GIT_COMMITTER_NAME": "LH candidate snapshot", "GIT_COMMITTER_EMAIL": "lh@localhost",
            "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00"}
        env = git_readonly_env(env)
        def git(*args):
            return subprocess.check_output(["git", "-C", worktree, *args], env=env).decode().strip()
        git("read-tree", "HEAD")
        git("add", "-A")
        tree = git("write-tree")
        return git("commit-tree", tree, "-p", git("rev-parse", "HEAD"), "-m", "LH unpublished candidate snapshot")


def candidate_diff_digest(worktree: str, base_sha: str, *, env: Mapping[str, str] | None = None) -> str:
    completed = subprocess.run(
        ["git", "-C", worktree, "diff", "--binary", base_sha],
        capture_output=True,
        check=False,
        env=git_readonly_env(env),
    )
    if completed.returncode != 0:
        raise ValueError("completion_candidate_diff_unreadable")
    return "sha256:" + hashlib.sha256(completed.stdout).hexdigest()


def validate_checks_repair_binding_shape(binding: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the immutable, self-sealed shape of a checks repair binding.

    Identity and source/runtime readback are checked by the controller and the
    task-area entrypoint.  This helper only checks the portable admission
    shape, so an independently produced binding cannot smuggle an unsealed
    overlay or a new-attempt route into the completion engine.
    """
    if not isinstance(binding, Mapping):
        raise ValueError("checks_repair_binding_invalid")
    value = dict(binding)
    if value.get("schema") != CHECKS_REPAIR_BINDING_SCHEMA or value.get("status") != "approved":
        raise ValueError("checks_repair_binding_schema_invalid")
    required = (
        "repair_id", "original_manifest_digest", "goal_id", "goal_revision",
        "node_id", "dispatch_key", "work_unit_id", "run_id", "attempt", "fence",
        "envelope_digest", "phase", "original_candidate_receipt_digest", "original_checks_receipt_digest",
        "candidate_before_digest", "candidate_after_digest", "candidate_overlay",
        "repair_scope", "checks_commands", "checks_command_digest",
        "independent_verifier", "runtime_source", "state_root", "same_run",
        "same_attempt", "same_fence", "allow_retry", "provider_invocations",
        "manual_prompts", "new_attempt",
    )
    if any(field not in value or value[field] in (None, "") for field in required):
        raise ValueError("checks_repair_binding_field_missing")
    supplied = value.get("repair_digest")
    body = {key: item for key, item in value.items() if key != "repair_digest"}
    if supplied != digest_json(body):
        raise ValueError("checks_repair_binding_digest_mismatch")
    if "predecessor_receipt_digest" in value:
        predecessor = value["predecessor_receipt_digest"]
        if (not isinstance(predecessor, str) or len(predecessor) != 71
            or not predecessor.startswith("sha256:")
            or any(c not in "0123456789abcdef" for c in predecessor[7:])):
            raise ValueError("checks_repair_binding_predecessor_invalid")
    for name in (
        "original_manifest_digest", "original_candidate_receipt_digest",
        "original_checks_receipt_digest", "candidate_before_digest",
        "candidate_after_digest",
    ):
        digest = value.get(name)
        if (not isinstance(digest, str) or not digest.startswith("sha256:")
            or len(digest) != 71):
            raise ValueError(f"checks_repair_binding_{name}_invalid")
        try:
            int(digest[7:], 16)
        except ValueError as exc:
            raise ValueError(f"checks_repair_binding_{name}_invalid") from exc
    if value.get("phase") != "checks":
        raise ValueError("checks_repair_binding_phase_invalid")
    if any(value.get(field) is not True for field in ("same_run", "same_attempt", "same_fence")):
        raise ValueError("checks_repair_binding_identity_route_invalid")
    if value.get("allow_retry") is not False or value.get("new_attempt") is not False:
        raise ValueError("checks_repair_binding_retry_route_invalid")
    if value.get("provider_invocations") != 0 or value.get("manual_prompts") != 0:
        raise ValueError("checks_repair_binding_provider_effect_invalid")
    overlay = value.get("candidate_overlay")
    if (not isinstance(overlay, Mapping) or not isinstance(overlay.get("path"), str)
        or not overlay.get("path") or overlay.get("candidate_digest") != value.get("candidate_after_digest")):
        raise ValueError("checks_repair_binding_candidate_overlay_invalid")
    scope = value.get("repair_scope")
    if (not isinstance(scope, Mapping) or not isinstance(scope.get("changed_paths"), list)
        or not scope["changed_paths"]
        or any(not isinstance(path, str) or not path for path in scope["changed_paths"])):
        raise ValueError("checks_repair_binding_scope_invalid")
    commands = value.get("checks_commands")
    if (not isinstance(commands, list) or not commands
        or value.get("checks_command_digest") != digest_json(commands)):
        raise ValueError("checks_repair_binding_commands_invalid")
    verifier = value.get("independent_verifier")
    if (not isinstance(verifier, Mapping) or not isinstance(verifier.get("principal"), str)
        or not verifier.get("principal") or verifier.get("read_only") is not True
        or verifier.get("source_write") is not False):
        raise ValueError("checks_repair_binding_verifier_invalid")
    source = value.get("runtime_source")
    if (not isinstance(source, Mapping) or not isinstance(source.get("source_root"), str)
        or not source.get("source_root") or not isinstance(source.get("head"), str)
        or not source.get("head")):
        raise ValueError("checks_repair_binding_runtime_source_invalid")
    if not isinstance(value.get("state_root"), str) or not value["state_root"].strip():
        raise ValueError("checks_repair_binding_state_root_invalid")
    return value


def validate_full_validation_binding(contract: Mapping[str, Any], packet: Mapping[str, Any]) -> None:
    """Keep the source exam exact; a declared fan-in has a separate full exam."""
    commands = contract.get("checks", [])
    integration = contract.get("integration_checks", [])
    full = contract.get("full_validation_plan", {}).get("commands", [])
    combined = contract.get("integration_full_validation_plan")
    if (not commands or not integration or not full
        or packet.get("full_validation_ref") != "#/full_validation_plan"
        or commands != packet.get("targeted_commands", []) + full):
        raise ValueError("completion_full_checks_binding_mismatch")
    if "integration_inputs" not in contract:
        if combined is not None or integration != full:
            raise ValueError("completion_full_checks_binding_mismatch")
    elif (not isinstance(contract["integration_inputs"], list) or not contract["integration_inputs"]
        or not isinstance(combined, dict) or not combined.get("commands")
        or integration != combined["commands"]
        or packet.get("integration_full_validation_ref") != "#/integration_full_validation_plan"
        or packet.get("integration_full_validation_plan") != combined):
        raise ValueError("completion_fanin_full_checks_binding_mismatch")


class WorkUnitCompletionController:
    def __init__(self, store: WorkUnitStore, *, contract: dict[str, Any],
                 verifier: Callable | None = None, integrator: Callable | None = None,
                 binding_receipt: Mapping[str, Any] | None = None,
                 delivery_contract: Mapping[str, Any] | None = None,
                 candidate_recovery_admission: Mapping[str, Any] | None = None,
                 candidate_recovery_authority_context: Mapping[str, Any] | None = None,
                 integration_proof_reader: Callable | None = None,
                 execution_binding: Any = None, command_runner: Callable | None = None):
        self.store, self.contract = store, contract
        self.verifier, self.integrator = verifier, integrator
        self.integration_proof_reader = integration_proof_reader
        self.execution_binding = execution_binding
        self.phase_job_port = None
        if execution_binding is not None:
            execution_binding.attach_store(store)
        self.command_runner = execution_binding.command if execution_binding is not None else command_runner
        self.binding_receipt = dict(binding_receipt) if binding_receipt is not None else None
        self.delivery_contract = dict(delivery_contract) if delivery_contract is not None else None
        # A review policy is part of both sealed contracts or of neither.
        self.review_policy = None
        if "candidate_review" in contract or (self.delivery_contract is not None
                                              and "candidate_review" in self.delivery_contract):
            if self.delivery_contract is None or contract.get("candidate_review") != self.delivery_contract.get("candidate_review"):
                raise ValueError("completion_candidate_review_policy_conflict")
            self.review_policy = delivery_unit_contract.validate_candidate_review_policy(contract["candidate_review"])
        self.candidate_recovery_admission = (dict(candidate_recovery_admission)
                                             if candidate_recovery_admission is not None else None)
        self.candidate_recovery_authority_context = (
            dict(candidate_recovery_authority_context)
            if candidate_recovery_authority_context is not None else None
        )

    @property
    def permits_preserved_retry(self):
        digest = getattr(self.execution_binding, "continuation_authority_digest", None)
        admission = self.candidate_recovery_admission
        if not digest or not admission or admission.get("kind") != "preserved_result_recovery":
            return False
        authority = self.store.get_continuation_authority(digest)
        return bool(authority and authority.get("schema") == "lh-trusted-continuation-authority/v2"
            and any(scope.get("dispatch_key") == admission.get("dispatch_key")
                    for scope in authority.get("repair_scopes", [])))

    def phase_binding(self, context: Mapping[str, Any], candidate_binding: str) -> str:
        trusted = ({"execution_binding_digest": self.execution_binding.digest}
                   if self.execution_binding is not None and self.execution_binding.policy is not None else {})
        continuation = getattr(self.execution_binding, "continuation_authority_digest", None)
        if continuation is not None:
            trusted["continuation_authority_digest"] = continuation
        provenance = ({"integration_provenance": {key: context[key] for key in
            ("integration_inputs_digest", "integration_receipt_digest")}}
            if self.contract.get("integration_inputs") and "integration_receipt_digest" in context else {})
        return digest_json({"candidate": candidate_binding, "contract": self.contract, **provenance,
            **trusted,
            "delivery_contract_digest": self.delivery_contract.get("contract_digest") if self.delivery_contract else None,
            "identity": {key: context[key] for key in ("run_id", "attempt", "fence", "work_unit_id", "packet_digest")}})

    def _validate_binding_receipt(self, envelope: dict[str, Any], result: dict[str, Any],
                                  packet: dict[str, Any]) -> None:
        binding = self.binding_receipt
        if binding is None:
            return
        if binding.get("schema") != "lh-completion-binding/v1" or binding.get("status") != "bound":
            raise ValueError("completion_binding_receipt_invalid")
        supplied = binding.get("receipt_digest")
        body = {key: value for key, value in binding.items() if key != "receipt_digest"}
        if supplied != digest_json(body):
            raise ValueError("completion_binding_receipt_digest_mismatch")
        attempt_record = result.get("attempt_record") or {}
        expected = {
            "goal_id": envelope.get("goal_id"),
            "goal_revision": envelope.get("goal_revision"),
            "node_id": envelope.get("node_id"),
            "dispatch_key": envelope.get("dispatch_key"),
            "envelope_digest": envelope.get("envelope_digest"),
            "packet_digest": envelope.get("packet_digest"),
            "work_unit_id": result.get("work_unit_id"),
            "run_id": result.get("run_id"),
            "attempt": result.get("attempt"),
            "fence": attempt_record.get("fence"),
            "worktree": attempt_record.get("workspace_ref") or envelope.get("worktree"),
            "integration_worktree": self.contract.get("integration_worktree"),
        }
        for field, value in expected.items():
            if binding.get(field) != value:
                raise ValueError("completion_binding_identity_mismatch")
        if binding.get("completion_contract_digest") != digest_json(self.contract):
            raise ValueError("completion_binding_contract_digest_mismatch")
        if binding.get("completion_contract") != self.contract:
            raise ValueError("completion_binding_contract_mismatch")
        mode = binding.get("packet_contract_mode")
        if mode not in {"embedded", "sidecar"}:
            raise ValueError("completion_binding_packet_mode_invalid")
        embedded_digest = packet.get("completion_contract_digest")
        if mode == "embedded" and embedded_digest != digest_json(self.contract):
            raise ValueError("completion_packet_contract_digest_mismatch")
        if mode == "sidecar" and embedded_digest is not None:
            raise ValueError("completion_binding_packet_mode_mismatch")

    def _effect(self, context: dict[str, Any], phase: str, binding: str, call: Callable,
                *, verifier_reconciliation: Mapping[str, Any] | None = None) -> dict:
        if self.execution_binding is not None:
            context = {**context, "execution_phase": phase}
        environment_resume = context.get("environment_resume") if phase == "checks" else None
        if environment_resume is not None:
            binding = digest_json({"candidate": binding, "environment_resume": environment_resume})
        binding = self.phase_binding(context, binding)
        durable = self.phase_job_port is not None and phase in {
            "checks", "verifier", "integration", "integration_checks", "integration_verifier"}
        phase_job = None
        successor = {}
        if environment_resume is not None:
            predecessor = environment_resume["original_red_ref"]["receipt_digest"]
            successor = {"repair_id": predecessor, "predecessor_receipt_digest": predecessor,
                         "environment_request_id": environment_resume["request_id"]}
        repair = context.get("checks_repair") or {}
        if phase == "checks_repair" and repair.get("predecessor_receipt_digest") is not None:
            successor = {"repair_id": repair["repair_id"],
                         "predecessor_receipt_digest": repair["predecessor_receipt_digest"]}
        delivery_recovery = context.get("delivery_recovery")
        if delivery_recovery is not None and phase == delivery_recovery.get("resume_phase", "integration_checks"):
            successor = {"repair_id": delivery_recovery["repair_id"],
                         "predecessor_receipt_digest": delivery_recovery["predecessor_receipt_digest"],
                         "recovery_admission_digest": delivery_recovery["input_digest"]}
        claim = self.store.claim_completion_phase(run_id=context["run_id"],
            attempt=context["attempt"], fence=context["fence"], phase=phase, binding=binding,
            **successor)
        if not claim["claimed"] and not (durable and claim["state"] == "claimed"):
            if claim["state"] == "claimed" and verifier_reconciliation is not None:
                if phase != "verifier":
                    raise ValueError("completion_reconciliation_phase_forbidden")
                if (verifier_reconciliation.get("phase_key") != claim["key"]
                    or verifier_reconciliation.get("phase_binding") != binding):
                    raise ValueError("completion_reconciliation_binding_mismatch")
                try:
                    evidence = call({**context, "idempotency_key": claim["key"],
                                     "read_only_required": True,
                                     "verifier_reconciliation": dict(verifier_reconciliation)})
                except Exception as exc:
                    failure = {
                        "phase": phase, "binding": binding, "phase_key": claim["key"],
                        "run_id": context["run_id"], "attempt": context["attempt"],
                        "fence": context["fence"], "verdict": "RED",
                        "reason": str(exc),
                        "repair_binding_digest": verifier_reconciliation["repair_digest"],
                        "repair_id": verifier_reconciliation["repair_id"],
                    }
                    failure["receipt_digest"] = digest_json(failure)
                    self.store.record_completion_phase_failure(claim["key"], failure)
                    raise
                evidence = {**evidence, "phase": phase, "binding": binding,
                    "phase_key": claim["key"], "run_id": context["run_id"],
                    "attempt": context["attempt"], "fence": context["fence"],
                    "repair_binding_digest": verifier_reconciliation["repair_digest"],
                    "repair_id": verifier_reconciliation["repair_id"]}
                if self.delivery_contract is not None:
                    evidence.update({
                        "contract_digest": self.delivery_contract["contract_digest"],
                        "unit_id": self.delivery_contract["unit_id"],
                        "goal_id": context["goal_id"],
                        "goal_revision": context["goal_revision"],
                        "node_id": context["node_id"],
                        "dispatch_key": context["dispatch_key"],
                        "base_sha": context["base_sha"],
                        "diff_digest": context["diff_digest"],
                    })
                evidence["receipt_digest"] = digest_json(evidence)
                if evidence.get("verdict") != "GREEN":
                    self.store.record_completion_phase_failure(claim["key"], evidence)
                    return evidence
                self.store.settle_completion_phase(claim["key"], evidence)
                return evidence
            if claim["state"] != "settled":
                raise ValueError("completion_reconciliation_required:" + phase)
            evidence = claim["evidence"]
            if verifier_reconciliation is not None and (
                not isinstance(evidence, Mapping)
                or evidence.get("repair_binding_digest") != verifier_reconciliation.get("repair_digest")
                or evidence.get("repair_id") != verifier_reconciliation.get("repair_id")
            ):
                raise ValueError("completion_reconciliation_replay_mismatch")
            if durable:
                _raw, phase_job = self.phase_job_port(
                    {**context, "idempotency_key": claim["key"]}, phase, binding, allow_create=False)
                if evidence.get("phase_job_result_digest") != phase_job["result_digest"]:
                    raise ValueError("phase_job_completion_result_changed")
                if phase_job["status"] != "settled":
                    self.store.settle_phase_job(phase_job["job_id"])
            return evidence
        try:
            if durable:
                evidence, phase_job = self.phase_job_port(
                    {**context, "idempotency_key": claim["key"]}, phase, binding,
                    allow_create=claim["claimed"])
                # The port returns only a real immutable result. A reserved
                # slot waiter raises PhaseJobPending and retains this claim;
                # neither the local callable nor integration prebuild runs.
                evidence = {**evidence, "phase_job_result_digest": phase_job["result_digest"]}
            else:
                evidence = call({**context, "idempotency_key": claim["key"]})
        except PhaseJobPending:
            raise
        except Exception as exc:
            failure = {
                "phase": phase, "binding": binding, "phase_key": claim["key"],
                "run_id": context["run_id"], "attempt": context["attempt"],
                "fence": context["fence"], "verdict": "RED", "reason": str(exc),
                **successor,
            }
            failure["receipt_digest"] = digest_json(failure)
            if verifier_reconciliation is not None or successor:
                self.store.record_completion_phase_failure(claim["key"], failure)
            raise
        evidence = {**evidence, "phase": phase, "binding": binding, "phase_key": claim["key"],
                    "run_id": context["run_id"], "attempt": context["attempt"], "fence": context["fence"],
                    **successor}
        if context.get("candidate_recovery") is not None:
            evidence["candidate_recovery"] = copy.deepcopy(context["candidate_recovery"])
        if context.get("checks_repair") is not None:
            evidence["checks_repair"] = copy.deepcopy(context["checks_repair"])
        if self.delivery_contract is not None:
            evidence.update({
                "contract_digest": self.delivery_contract["contract_digest"],
                "unit_id": self.delivery_contract["unit_id"],
                "goal_id": context["goal_id"],
                "goal_revision": context["goal_revision"],
                "node_id": context["node_id"],
                "dispatch_key": context["dispatch_key"],
                "base_sha": context["base_sha"],
                "diff_digest": context["diff_digest"],
            })
        if verifier_reconciliation is not None:
            evidence["repair_binding_digest"] = verifier_reconciliation["repair_digest"]
            evidence["repair_id"] = verifier_reconciliation["repair_id"]
        evidence["receipt_digest"] = digest_json(evidence)
        if verifier_reconciliation is not None and evidence.get("verdict") != "GREEN":
            self.store.record_completion_phase_failure(claim["key"], evidence)
            return evidence
        self.store.settle_completion_phase(claim["key"], evidence)
        if phase_job is not None:
            self.store.settle_phase_job(phase_job["job_id"])
        return evidence

    def execute_phase_operation(self, phase: str, request: dict) -> dict:
        """Carrier seam: one existing operation, never advance or settlement."""
        if request.get("execution_phase") != phase:
            raise ValueError("phase_job_operation_binding_changed")
        if phase == "checks":
            return self._checks(request, self.contract["checks"])
        if phase in {"verifier", "integration_verifier"}:
            return self._verify(request)
        if phase == "integration":
            if self.integrator is None:
                raise ValueError("completion_integrator_missing")
            return self.integrator(request)
        if phase == "integration_checks":
            return self._run_integration_checks(
                request, self.contract["integration_checks"], self.delivery_contract)
        raise ValueError("phase_job_operation_unsupported")

    def _run_integration_checks(self, req, commands, contract, recovery=None):
        before_proof = None
        if recovery is not None:
            before_proof = self.integration_proof_reader(req["worktree"])
            if before_proof["proof_digest"] != recovery["integration_proof_digest"]:
                raise ValueError("delivery_recovery_integration_drift")
        with private_git_index(req["worktree"], self.store.root, label="integration-checks") as delivery_env:
            checked = self._checks(req, commands)
            with task_owned_check_environment(
                self.store.root, req["worktree"], base=delivery_env
            ) as (delivery_env, _replacements):
                delivery_checked = delivery_unit_contract.run_obligation_commands(
                    contract, Path(req["worktree"]), env=child_check_environment(delivery_env),
                    command_runner=self.command_runner, execution_context=req,
                )
            checked["delivery_contract_digest"] = contract["contract_digest"]
            checked["delivery_obligations"] = delivery_checked["obligations"]
            checked["verdict"] = "GREEN" if (
                checked["verdict"] == "GREEN"
                and all(row.get("verdict") == "GREEN" for row in delivery_checked["obligations"].values())
            ) else "RED"
            if recovery is not None:
                checked["integration_proof_before"] = before_proof
                checked["integration_proof_after"] = self.integration_proof_reader(req["worktree"])
            return checked

    def _checks(self, request: dict, commands: list[dict]) -> dict:
        if self.execution_binding is not None:
            snapshot = candidate_snapshot_copy(request["worktree"], self.store.root)
            request = {**request, "source_worktree": request["worktree"], "worktree": snapshot}
        with private_git_index(request["worktree"], self.store.root, label="checks") as private_env:
            return self._checks_with_env({**request, "_git_env": private_env}, commands)

    def _checks_with_env(self, request: dict, commands: list[dict]) -> dict:
        readonly_env = request.get("_git_env") or git_readonly_env(os.environ)
        before = candidate_digest(request["worktree"], env=readonly_env)
        commit = candidate_object(request["worktree"], self.store.root)
        rows = []
        with task_owned_check_environment(self.store.root, request["worktree"], base=readonly_env) as (env, replacements):
            replacements["${WAVE_BASE_SHA}"] = request["base_sha"]
            # Checks may create their own repositories; the controller's index belongs
            # only to candidate reads in this worktree.
            env = child_check_environment(env)
            # Parent reads stay protected; child exams own disposable repositories
            # and may deliberately exercise Git's optional-lock behavior.
            def expand(value: str) -> str:
                if request.get("source_worktree") and request["source_worktree"] != request["worktree"]:
                    value = value.replace(request["source_worktree"], request["worktree"])
                for key, replacement in replacements.items():
                    value = value.replace(key, replacement)
                if "${" in value:
                    raise ValueError("completion_unresolved_check_variable")
                if "@candidate_sha@" in value:
                    if self.contract.get("runtime_bindings", {}).get("@candidate_sha@") != "unpublished_candidate_object":
                        raise ValueError("completion_candidate_variable_unbound")
                    value = value.replace("@candidate_sha@", commit)
                if "@candidate_" in value:
                    raise ValueError("completion_unknown_runtime_variable")
                return value
            for command in commands:
                argv = command.get("argv")
                if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
                    raise ValueError("completion_check_argv_invalid")
                argv = [expand(x) for x in argv]
                cwd = Path(expand(command.get("cwd", request["worktree"]))).resolve()
                if cwd != Path(request["worktree"]) and Path(request["worktree"]) not in cwd.parents:
                    raise ValueError("completion_check_cwd_escape")
                if self.command_runner is None:
                    raise ValueError("execution_fence_unavailable: completion_command_port_missing")
                command_request = {**request, "command_id": command["id"]}
                if request.get("candidate_review_check_mode") is True:
                    command_request["candidate_review_check"] = copy.deepcopy(command)
                result, fence_evidence, _descriptor = self.command_runner(command_request,
                    phase=request.get("execution_phase", "checks"),
                    argv=argv, worktree=str(cwd), timeout_seconds=float(command.get("timeout_seconds", 300)), env=env)
                result.stdout = result.stdout.encode() if isinstance(result.stdout, str) else result.stdout
                result.stderr = result.stderr.encode() if isinstance(result.stderr, str) else result.stderr
                diagnostic = {}
                if (result.returncode != command.get("expect_exit", 0)
                        and not (self.execution_binding is not None and self.execution_binding.policy is not None)):
                    diagnostic_root = self.store.root / "completion-check-diagnostics"
                    diagnostic_root.mkdir(mode=0o700, exist_ok=True)
                    diagnostic_dir = Path(tempfile.mkdtemp(prefix="failed-", dir=diagnostic_root))
                    for stream in ("stdout", "stderr"):
                        data = getattr(result, stream)
                        path = diagnostic_dir / (stream + ".log")
                        path.write_bytes(data[:1048576])
                        path.chmod(0o600)
                        diagnostic[stream + "_ref"] = str(path)
                        diagnostic[stream + "_truncated"] = len(data) > 1048576
                rows.append({"id": command["id"], "argv": argv, "exit_code": result.returncode,
                    "expect_exit": command.get("expect_exit", 0),
                    "stdout_digest": hashlib.sha256(result.stdout).hexdigest(),
                    "stderr_digest": hashlib.sha256(result.stderr).hexdigest(), **diagnostic, **fence_evidence})
            if before != candidate_digest(request["worktree"], env=readonly_env):
                raise ValueError("completion_checks_mutated_candidate")
            return {"candidate_digest": before, "candidate_commit": commit,
                    "candidate_kind": "unpublished_snapshot_object", "checks": rows,
                    "verdict": "GREEN" if all(row["exit_code"] == row["expect_exit"] for row in rows) else "RED"}

    def _verify(self, request: dict) -> dict:
        related = None
        if self.review_policy is not None and "review_related_commands" in request:
            # The packet's targeted commands run first; a red one costs no review.
            related = self._checks({**request, "execution_phase": "verifier", "candidate_review_check_mode": True},
                                   request["review_related_commands"])
            related.update(schema="lh-candidate-review-related-checks/v2",
                           commands_digest=digest_json(request["review_related_commands"]),
                           contract_digest=self.delivery_contract["contract_digest"],
                           **{key: request[key] for key in ("run_id", "attempt", "fence", "base_sha")})
            related["receipt_digest"] = digest_json(related)
            request = {**request, "checks_digest": related["receipt_digest"], "candidate_commit": related["candidate_commit"]}
            if related["verdict"] != "GREEN":
                return {"verdict": "RED", "reason_code": "check_failed", "reason": "candidate_review_related_checks_red",
                        "candidate_digest": request["candidate_digest"], "checks_digest": request["checks_digest"],
                        "related_checks": related, "checks": related["checks"]}
        review_context = None
        if self.review_policy is not None:
            identity = {key: request[key] for key in ("goal_id", "goal_revision", "node_id", "dispatch_key",
                                                      "run_id", "attempt", "fence", "base_sha", "diff_digest")}
            identity.update(unit_id=self.delivery_contract["unit_id"], contract_digest=self.delivery_contract["contract_digest"])
            review_context = delivery_unit_contract.candidate_review_context(
                self.review_policy, candidate_digest=request["candidate_digest"], base_sha=request["base_sha"],
                checks_digest=request["checks_digest"], scope=self.delivery_contract["scope"], identity=identity)
            request = {**request, "candidate_review_context": review_context}
        source = Path(request["worktree"])
        snapshot_root = self.store.root / "completion-snapshots"
        snapshot_root.mkdir(exist_ok=True)
        snapshot = Path(tempfile.mkdtemp(prefix="verify-", dir=snapshot_root)) / "source"
        with private_git_index(str(source), self.store.root, label="verifier-source") as source_env:
            clone_env = {key: value for key, value in source_env.items() if key != "GIT_INDEX_FILE"}
            subprocess.run(["git", "clone", "--no-hardlinks", "--quiet", str(source), str(snapshot)],
                           check=True, env=clone_env)
            subprocess.run(["git", "-C", str(snapshot), "checkout", "--detach", "--quiet",
                            subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"],
                                                     env=source_env).decode().strip()],
                           check=True, env=clone_env)
            names = subprocess.check_output(
                ["git", "-C", str(source), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                env=source_env,
            ).split(b"\0")
            for raw in set(names):
                if not raw:
                    continue
                relative = raw.decode()
                origin, destination = source / relative, snapshot / relative
                if origin.is_file() and not origin.is_symlink():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(origin, destination)
                elif not origin.exists() and destination.is_file():
                    destination.unlink()
            with private_git_index(str(snapshot), self.store.root, label="verifier-snapshot") as snapshot_env:
                if candidate_digest(str(snapshot), env=snapshot_env) != request["candidate_digest"]:
                    raise ValueError("completion_snapshot_mismatch")
                evidence = self.verifier({**request, "worktree": str(snapshot),
                                          "git_index_file": snapshot_env["GIT_INDEX_FILE"],
                                          "read_only_required": True})
                if (evidence.get("principal") in (None, "", request["worker_id"])
                    or evidence.get("principal") != self.contract.get("verifier_principal")
                    or evidence.get("read_only") is not True or evidence.get("source_write") is not False
                    or evidence.get("candidate_digest") != request["candidate_digest"]
                    or evidence.get("checks_digest") != request["checks_digest"]
                    or evidence.get("verdict") not in {"GREEN", "RED"}
                    or not evidence.get("evidence_ref")):
                    raise ValueError("completion_verifier_binding_invalid")
                if candidate_digest(request["worktree"], env=source_env) != request["candidate_digest"]:
                    raise ValueError("completion_verifier_mutated_candidate")
                if candidate_digest(str(snapshot), env=snapshot_env) != request["candidate_digest"]:
                    raise ValueError("completion_verifier_mutated_snapshot")
                if review_context is not None:
                    # The reviewer's verdict must be the one its own findings support.
                    verdict = delivery_unit_contract.validate_candidate_review_result(evidence.get("review"), review_context)
                    if verdict != evidence["verdict"]:
                        raise ValueError("completion_candidate_review_verdict_mismatch")
                    evidence = {**evidence, **delivery_unit_contract.seal_candidate_review_proof(
                        evidence["review"], review_context, self.store.root)}
                    if related is not None:
                        evidence["related_checks"] = related
                    self._publish_review_suggestions(evidence, request)
                return evidence

    def _publish_review_suggestions(self, evidence: dict, request: dict) -> None:
        """Record non-blocking findings as discovery candidates; never approve or dispatch one."""
        from .discovery_cursor import BATCH_SCHEMA, CANDIDATE_SCHEMA, candidate_id
        goal = request["goal_id"]
        records = []
        for finding in evidence["review"]["findings"]:
            if finding["blocking"]:
                continue
            obj = finding["location"] + "::" + finding["id"]
            records.append({"schema": CANDIDATE_SCHEMA,
                            "candidate_id": candidate_id(goal, "review_optimization", obj),
                            "object": obj, "problem_type": "review_optimization", "finding": copy.deepcopy(finding),
                            "review_ref": copy.deepcopy(evidence["review_ref"]),
                            "context_digest": evidence["candidate_review_context"]["context_digest"],
                            "first_event_id": "candidate-review:" + evidence["review_ref"]["content_digest"]})
        if not records:
            return
        known = self.store.discovery_known_candidates(goal, [row["candidate_id"] for row in records])
        fresh = [row for row in records if row["candidate_id"] not in known]
        if not fresh:
            return
        position = self.store.discovery_position(goal)
        for row in fresh:
            row["first_rowid"] = position["after_rowid"]
        names = [row["candidate_id"] for row in fresh]
        batch = {"schema": BATCH_SCHEMA, "seq": position["seq"] + 1,
                 "from_rowid": position["after_rowid"], "to_rowid": position["after_rowid"], "events_read": 0,
                 "new_candidates": names, "handoff": [], "pending": position["pending"] + names,
                 "coverage": position["coverage"]}
        self.store.record_discovery_batch(goal, batch=batch, candidates=fresh)

    def _retry_or_incomplete(self, envelope: dict, result: dict, evidence: dict,
                             *, allow_retry: bool) -> dict:
        if not allow_retry:
            return {
                "status": "incomplete",
                "reason": "completion_retry_disabled",
                "phase": evidence.get("phase"),
                "evidence": evidence,
            }
        extra = {}
        if self.candidate_recovery_admission is not None:
            if not self.permits_preserved_retry:
                raise ValueError("candidate_recovery_retry_forbidden")
            authority = self.execution_binding.continuation_authority_digest
            plan = self.store.prepare_continuation_repair(envelope["dispatch_key"], authority_digest=authority,
                attempt=result["attempt"], fence=result["attempt_record"]["fence"], evidence=evidence)
            if plan.get("prior"):
                return {"status": "reused", "attempt": plan["scope"]["next_attempt"]}
            admission = plan["admission"]
            scope = plan["scope"]
            context = {"run_id": result["run_id"], "attempt": result["attempt"], "fence": result["attempt_record"]["fence"],
                "work_unit_id": result["work_unit_id"], "worker_id": result["attempt_record"]["holder"],
                "packet_digest": envelope["packet_digest"], "dispatch_key": envelope["dispatch_key"],
                "goal_id": envelope["goal_id"], "goal_revision": envelope["goal_revision"], "node_id": envelope["node_id"],
                "base_sha": envelope["wave_base_sha"], "worktree": admission["current_workspace_ref"],
                "diff_digest": evidence["diff_digest"]}
            def snapshot(_request):
                if candidate_inventory(admission["current_workspace_ref"]) != admission["source_workspace_inventory"]:
                    raise ValueError("continuation_repair_preserved_source_drift")
                target = candidate_snapshot_copy(admission["current_workspace_ref"], self.store.root,
                    target=Path(scope["workspace_ref"]))
                return {"authority_digest": authority, "red_receipt_digest": evidence["receipt_digest"],
                    "workspace_ref": target, "inventory": candidate_inventory(target)}
            proof = self._effect(context, "continuation_repair",
                digest_json([authority, evidence["receipt_digest"], scope, admission["input_digest"]]), snapshot)
            extra = {"continuation_authority_digest": authority, "continuation_snapshot": proof}
        return self.store.retry_completion(
            envelope["dispatch_key"], attempt=result["attempt"],
            fence=result["attempt_record"]["fence"], evidence=evidence,
            max_attempts=min(
                int(self.contract.get("max_attempts", 3)),
                int(self.delivery_contract["repair_same_unit"]["max_attempts"]),
            ),
            **extra,
        )

    def _preflight_candidate_recovery(
        self,
        envelope: Mapping[str, Any],
        result: Mapping[str, Any],
        admission: Mapping[str, Any],
        *, materialize: bool = True,
    ) -> dict[str, str]:
        """Complete recovery checks before the Store admission can mutate state."""
        if self.verifier is None or self.integrator is None:
            raise ValueError("completion_adapter_missing")
        if self.delivery_contract is None:
            raise ValueError("delivery_unit_contract_missing")
        try:
            materialized = json.loads(Path(str(envelope["packet_path"])).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("completion_materialized_packet_unreadable") from exc
        if not isinstance(materialized, dict):
            raise ValueError("completion_materialized_packet_invalid")
        packet_body = {key: value for key, value in materialized.items() if key != "packet_digest"}
        if digest_json(packet_body) != envelope.get("packet_digest"):
            raise ValueError("completion_packet_digest_mismatch")
        packet = materialized.get("packet", materialized)
        if not isinstance(packet, dict):
            raise ValueError("completion_packet_body_invalid")
        self._validate_binding_receipt(dict(envelope), dict(result), packet)
        delivery_contract = delivery_unit_contract.validate_contract(self.delivery_contract)
        plan = delivery_unit_contract.plan_delivery_unit(delivery_contract)
        if packet.get("delivery_unit_managed") is True:
            packet_admission = delivery_unit_contract.verify_dispatch_binding(
                envelope=envelope, packet=packet, contract=delivery_contract, plan=plan)
        else:
            sidecar_path = envelope.get("delivery_unit_sidecar_path")
            if not isinstance(sidecar_path, str) or not sidecar_path.strip():
                sidecar_path = str(Path(str(envelope["packet_path"])).with_name(
                    Path(str(envelope["packet_path"])).name + ".delivery-unit.json"))
            try:
                sidecar = json.loads(Path(sidecar_path).expanduser().resolve().read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError("delivery_unit_sidecar_unreadable") from exc
            packet_admission = delivery_unit_contract.verify_legacy_sidecar(
                materialized, sidecar, delivery_contract, plan)
        if packet_admission.get("verdict") != "GREEN":
            raise ValueError(f"delivery_unit_{packet_admission.get('reason', 'packet_binding_failed')}")
        if (packet.get("completion_contract_digest") != digest_json(self.contract)
            and self.binding_receipt is None):
            raise ValueError("completion_contract_binding_missing")
        if packet.get("completion_contract") not in (None, self.contract):
            raise ValueError("completion_resolved_contract_mismatch")
        validate_full_validation_binding(self.contract, packet)
        if self.contract.get("integration_inputs"):
            raise ValueError("completion_fanin_recovery_unsupported")
        attempt = result.get("attempt_record") or {}
        current_workspace = str(Path(str(attempt.get("workspace_ref") or envelope.get("worktree"))).resolve())
        if not self.contract.get("integration_worktree") or not self.contract.get("verifier_principal"):
            raise ValueError("completion_runtime_bindings_missing")
        integration_root = str(Path(str(self.contract["integration_worktree"])).resolve())
        inventory = admission.get("source_workspace_inventory")
        if not isinstance(inventory, Mapping):
            raise ValueError("candidate_recovery_source_inventory_missing")
        source_workspace = str(Path(str(inventory.get("root") or inventory.get("workspace_ref") or inventory.get("source_root") or "")).expanduser().resolve())
        if not source_workspace or not Path(source_workspace).is_dir():
            raise ValueError("candidate_recovery_source_workspace_missing")
        if source_workspace == integration_root or Path(source_workspace) in Path(integration_root).parents:
            raise ValueError("completion_integration_not_isolated")
        with private_git_index(source_workspace, self.store.root, label="candidate-recovery-preflight") as source_env:
            observed = candidate_inventory(source_workspace, env=source_env)
            changed = subprocess.check_output(
                ["git", "-C", source_workspace, "diff", "--name-only", str(envelope["wave_base_sha"])],
                env=source_env,
            ).decode().splitlines()
            changed += subprocess.check_output(
                ["git", "-C", source_workspace, "ls-files", "--others", "--exclude-standard"],
                env=source_env,
            ).decode().splitlines()
        if observed.get("candidate_digest") != admission.get("candidate_digest"):
            raise ValueError("candidate_recovery_candidate_digest_mismatch")
        expected_inventory_digest = admission.get("source_workspace_inventory_digest")
        if expected_inventory_digest and candidate_inventory_digest(observed) != expected_inventory_digest:
            raise ValueError("candidate_recovery_source_inventory_digest_mismatch")
        if not changed:
            raise ValueError("completion_candidate_noop")
        try:
            changed = delivery_unit_contract.verify_changed_paths_scope(changed, packet, delivery_contract)
        except delivery_unit_contract.DeliveryUnitError as exc:
            raise ValueError("completion_candidate_scope_escape") from exc
        if packet.get("required_test_delta") and not any(path.startswith("tests/") for path in changed):
            raise ValueError("completion_test_delta_missing")
        if not materialize:
            return {"current_workspace": current_workspace, "source_workspace": source_workspace,
                    "execution_workspace": source_workspace}
        execution_workspace = None
        if admission.get("kind") == "preserved_result_recovery":
            previous = self.store.get_completion_phase(result["run_id"], int(result["attempt"]),
                int((result.get("attempt_record") or {})["fence"]), "candidate")
            if previous is not None:
                evidence = previous.get("evidence") or {}
                provenance = evidence.get("candidate_recovery") or {}
                candidate_root = Path(str(provenance.get("execution_workspace", "")))
                if (previous["state"] != "settled" or provenance.get("kind") != "preserved_result_recovery"
                    or provenance.get("admission_digest") != admission.get("input_digest")
                    or evidence.get("receipt_digest") != digest_json({k: v for k, v in evidence.items() if k != "receipt_digest"})
                    or not candidate_root.is_absolute() or not candidate_root.is_relative_to(self.store.root.resolve())
                    or any(path.is_symlink() for path in (candidate_root, *candidate_root.parents))):
                    raise ValueError("preserved_result_snapshot_provenance_invalid")
                execution_workspace = str(candidate_root)
        if execution_workspace is None:
            execution_workspace = candidate_snapshot_copy(source_workspace, self.store.root)
        with private_git_index(execution_workspace, self.store.root, label="candidate-recovery-preflight-copy") as copy_env:
            if candidate_digest(execution_workspace, env=copy_env) != admission.get("candidate_digest"):
                raise ValueError("candidate_recovery_snapshot_mismatch")
        if Path(execution_workspace).resolve() == Path(integration_root).resolve() or Path(execution_workspace).resolve() in Path(integration_root).parents:
            raise ValueError("completion_integration_not_isolated")
        return {"current_workspace": current_workspace, "source_workspace": source_workspace,
                "execution_workspace": execution_workspace}

    def _preflight_checks_repair(
        self,
        envelope: Mapping[str, Any],
        result: Mapping[str, Any],
        binding_input: Mapping[str, Any],
        *,
        packet: Mapping[str, Any],
        delivery_contract: Mapping[str, Any],
        current_workspace: str,
        integration_root: str,
    ) -> dict[str, Any]:
        """Admit a corrected candidate without rewriting the original RED phases."""
        binding = validate_checks_repair_binding_shape(binding_input)
        expected_identity = {
            "goal_id": envelope.get("goal_id"),
            "goal_revision": envelope.get("goal_revision"),
            "node_id": envelope.get("node_id"),
            "dispatch_key": envelope.get("dispatch_key"),
            "work_unit_id": result.get("work_unit_id"),
            "run_id": result.get("run_id"),
            "attempt": result.get("attempt"),
            "fence": (result.get("attempt_record") or {}).get("fence"),
        }
        for field, expected in expected_identity.items():
            if binding.get(field) != expected:
                raise ValueError("checks_repair_binding_identity_mismatch")
        if binding.get("envelope_digest") not in (None, envelope.get("envelope_digest")):
            raise ValueError("checks_repair_binding_envelope_digest_mismatch")
        if Path(binding["state_root"]).expanduser().resolve() != self.store.root.resolve():
            raise ValueError("checks_repair_binding_state_root_mismatch")
        if binding["independent_verifier"].get("principal") != self.contract.get("verifier_principal"):
            raise ValueError("checks_repair_binding_verifier_principal_mismatch")
        if binding["checks_commands"] != self.contract.get("checks"):
            raise ValueError("checks_repair_binding_commands_mismatch")

        candidate_phase = self.store.get_completion_phase(
            str(result["run_id"]), int(result["attempt"]), int(binding["fence"]), "candidate"
        )
        checks_phase = self.store.get_completion_phase(
            str(result["run_id"]), int(result["attempt"]), int(binding["fence"]), "checks"
        )
        if (not candidate_phase or candidate_phase.get("state") != "settled"
            or not checks_phase or checks_phase.get("state") != "settled"):
            raise ValueError("checks_repair_binding_original_phases_missing")
        candidate_evidence = candidate_phase.get("evidence")
        checks_evidence = checks_phase.get("evidence")
        for evidence, name, phase in (
            (candidate_evidence, "candidate", "candidate"),
            (checks_evidence, "checks", "checks"),
        ):
            try:
                evidence_attempt = int(evidence.get("attempt", -1)) if isinstance(evidence, Mapping) else -1
                evidence_fence = int(evidence.get("fence", -1)) if isinstance(evidence, Mapping) else -1
                result_attempt = int(result.get("attempt", -2))
                binding_fence = int(binding.get("fence", -2))
            except (TypeError, ValueError):
                raise ValueError(f"checks_repair_binding_{name}_receipt_invalid")
            if (not isinstance(evidence, Mapping)
                or evidence.get("receipt_digest") != digest_json(
                    {key: value for key, value in evidence.items() if key != "receipt_digest"}
                )
                or evidence.get("phase") != phase
                or evidence.get("phase_key") != (candidate_phase if name == "candidate" else checks_phase).get("phase_key")
                or evidence.get("run_id") != result.get("run_id")
                or evidence_attempt != result_attempt
                or evidence_fence != binding_fence):
                raise ValueError(f"checks_repair_binding_{name}_receipt_invalid")
        if checks_evidence.get("verdict") != "RED":
            raise ValueError("checks_repair_binding_original_checks_not_red")
        candidate_before = candidate_evidence.get("candidate_digest")
        if (candidate_before != checks_evidence.get("candidate_digest")
            or binding.get("candidate_before_digest") != candidate_before
            or binding.get("original_candidate_receipt_digest") != candidate_evidence.get("receipt_digest")
            or binding.get("original_checks_receipt_digest") != checks_evidence.get("receipt_digest")):
            raise ValueError("checks_repair_binding_original_receipts_mismatch")

        overlay = Path(str(binding["candidate_overlay"]["path"])).expanduser().resolve()
        state_root = self.store.root.resolve()
        if (not overlay.is_dir() or overlay.is_symlink() or overlay == state_root
            or state_root not in overlay.parents):
            raise ValueError("checks_repair_binding_overlay_scope_invalid")
        if overlay == Path(current_workspace).resolve() or overlay == Path(integration_root).resolve():
            raise ValueError("checks_repair_binding_overlay_identity_invalid")
        with private_git_index(str(overlay), self.store.root, label="checks-repair-overlay") as readonly_env:
            observed_candidate = candidate_digest(str(overlay), env=readonly_env)
            changed = subprocess.check_output(
                ["git", "-C", str(overlay), "diff", "--name-only", envelope["wave_base_sha"]],
                env=readonly_env,
            ).decode().splitlines()
            changed += subprocess.check_output(
                ["git", "-C", str(overlay), "ls-files", "--others", "--exclude-standard"],
                env=readonly_env,
            ).decode().splitlines()
        changed = sorted(set(changed))
        declared = sorted(set(binding["repair_scope"]["changed_paths"]))
        if observed_candidate != binding["candidate_after_digest"]:
            raise ValueError("checks_repair_binding_candidate_digest_mismatch")
        if changed != declared or not changed:
            raise ValueError("checks_repair_binding_scope_mismatch")
        try:
            checked_scope = delivery_unit_contract.verify_changed_paths_scope(
                changed, packet, delivery_contract
            )
        except delivery_unit_contract.DeliveryUnitError as exc:
            raise ValueError("checks_repair_binding_scope_escape") from exc
        if checked_scope != declared:
            raise ValueError("checks_repair_binding_scope_mismatch")
        return {
            "binding": binding,
            "candidate_phase": candidate_phase,
            "checks_phase": checks_phase,
            "candidate_evidence": dict(candidate_evidence),
            "checks_evidence": dict(checks_evidence),
            "overlay": str(overlay),
            "changed_paths": checked_scope,
        }

    def advance(self, envelope: dict, result: dict, *, allow_retry: bool = True,
                verifier_reconciliation: Mapping[str, Any] | None = None,
                candidate_recovery_admission: Mapping[str, Any] | None = None,
                checks_repair_binding: Mapping[str, Any] | None = None,
                stop_after_checks: bool = False,
                delivery_recovery: Mapping[str, Any] | None = None) -> dict:
        """Advance all ready machine phases; no R3 state is consulted."""
        if not isinstance(allow_retry, bool):
            raise ValueError("completion_allow_retry_invalid")
        if delivery_recovery is not None:
            if allow_retry or stop_after_checks or checks_repair_binding is None:
                raise ValueError("delivery_recovery_route_invalid")
            final_recovery = delivery_recovery.get("resume_phase") == "delivery_verifier"
            saved = self.store.get_completion_phase(delivery_recovery["run_id"],
                delivery_recovery["attempt"], delivery_recovery["fence"], "delivery_recovery",
                delivery_recovery["repair_id"] if final_recovery else None)
            if (not saved or saved["state"] != "settled"
                or saved["binding"] != delivery_recovery.get("input_digest")
                or saved["evidence"] != delivery_recovery):
                raise ValueError("delivery_recovery_admission_missing")
            if not callable(self.integration_proof_reader):
                raise ValueError("delivery_recovery_proof_reader_missing")
            checks_recovery = (self.store.get_completion_phase(delivery_recovery["run_id"],
                delivery_recovery["attempt"], delivery_recovery["fence"], "delivery_recovery")["evidence"]
                if final_recovery else delivery_recovery)
        else:
            final_recovery, checks_recovery = False, None
        if not isinstance(stop_after_checks, bool) or (stop_after_checks and (checks_repair_binding is None or allow_retry)):
            raise ValueError("completion_checks_checkpoint_route_invalid")
        if verifier_reconciliation is not None and allow_retry:
            raise ValueError("completion_reconciliation_requires_retry_disabled")
        recovery_input = candidate_recovery_admission or self.candidate_recovery_admission
        recovery = isinstance(recovery_input, Mapping)
        recovery_preflight: dict[str, str] | None = None
        if checks_repair_binding is not None:
            if verifier_reconciliation is not None:
                raise ValueError("checks_repair_binding_reconciliation_conflict")
            if allow_retry:
                raise ValueError("checks_repair_binding_requires_retry_disabled")
            if not recovery:
                raise ValueError("checks_repair_binding_candidate_recovery_required")
            # Fail before any Store admission or phase claim on malformed input.
            validate_checks_repair_binding_shape(checks_repair_binding)
        if recovery and allow_retry and not self.permits_preserved_retry:
            raise ValueError("candidate_recovery_retry_forbidden")
        if recovery:
            if result.get("executor_status") != "unknown":
                raise ValueError("candidate_recovery_executor_unknown_required")
            if self.delivery_contract is None:
                raise ValueError("delivery_unit_contract_missing")
            supplied_completion_digest = recovery_input.get("completion_contract_digest")
            if supplied_completion_digest != digest_json(self.contract):
                raise ValueError("candidate_recovery_completion_contract_digest_mismatch")
            supplied_delivery_digest = recovery_input.get("delivery_contract_digest")
            if supplied_delivery_digest != self.delivery_contract.get("contract_digest"):
                raise ValueError("candidate_recovery_delivery_contract_digest_mismatch")
            preserved = recovery_input.get("kind") == "preserved_result_recovery"
            recovery_preflight = self._preflight_candidate_recovery(envelope, result, recovery_input,
                materialize=not preserved)
            admission = self.store.record_candidate_recovery_admission(
                recovery_input,
                approved_manifest_context=self.candidate_recovery_authority_context,
            )
            if not admission:
                raise ValueError("candidate_recovery_admission_missing")
            self.candidate_recovery_admission = dict(admission)
            if preserved:
                recovery_preflight = self._preflight_candidate_recovery(envelope, result, admission)
            canonical_dispatch = self.store.get_dispatch_consumption(envelope["dispatch_key"])
            if not isinstance(canonical_dispatch, Mapping):
                raise ValueError("candidate_recovery_dispatch_missing")
            canonical_receipt = canonical_dispatch.get("receipt")
            if not isinstance(canonical_receipt, Mapping) or canonical_receipt.get("executor_status") != "unknown":
                raise ValueError("candidate_recovery_dispatch_unknown_missing")
            supplied_dispatch_digest = (self.candidate_recovery_admission.get("current_dispatch_receipt_digest")
                                        or (self.candidate_recovery_admission.get("current_dispatch_receipt") or {}).get("receipt_digest"))
            supplied_unknown_digest = (self.candidate_recovery_admission.get("current_unknown_recovery_receipt_digest")
                                       or (self.candidate_recovery_admission.get("current_unknown_recovery_receipt") or {}).get("recovery_receipt_digest"))
            if supplied_dispatch_digest != canonical_dispatch.get("receipt_digest") or supplied_unknown_digest != (canonical_receipt.get("executor_recovery_receipt") or {}).get("recovery_receipt_digest"):
                raise ValueError("candidate_recovery_dispatch_evidence_mismatch")
            refreshed = self.store.get_attempt(result["run_id"], int(result["attempt"]))
            if not isinstance(refreshed, Mapping):
                raise ValueError("candidate_recovery_attempt_missing")
            result = {**result, "attempt_record": dict(refreshed)}
            recovery_receipt = (result.get("receipt") or {}).get("executor_recovery_receipt")
            if not isinstance(recovery_receipt, Mapping):
                raise ValueError("candidate_recovery_unknown_receipt_missing")
        elif result.get("executor_status") != "accepted":
            return {"status": "incomplete", "reason": "completion_executor_pending"}
        if self.verifier is None or self.integrator is None:
            return {"status": "incomplete", "reason": "completion_adapter_missing"}
        if self.delivery_contract is None:
            return {"status": "incomplete", "reason": "delivery_unit_contract_missing"}
        try:
            materialized_packet = json.loads(Path(envelope["packet_path"]).read_text())
            if not isinstance(materialized_packet, dict):
                raise ValueError("completion_materialized_packet_invalid")
            contract = self.contract
            packet_body = {key: value for key, value in materialized_packet.items() if key != "packet_digest"}
            if digest_json(packet_body) != envelope["packet_digest"]:
                raise ValueError("completion_packet_digest_mismatch")
            packet = materialized_packet.get("packet", materialized_packet)
            if not isinstance(packet, dict):
                raise ValueError("completion_packet_body_invalid")
            self._validate_binding_receipt(envelope, result, packet)
            delivery_contract = delivery_unit_contract.validate_contract(self.delivery_contract)
            effective_retry_budget = min(
                int(contract.get("max_attempts", 3)),
                int(delivery_contract["repair_same_unit"]["max_attempts"]),
            )
            if effective_retry_budget < 1:
                raise ValueError("completion_retry_budget_invalid")
            plan = delivery_unit_contract.plan_delivery_unit(delivery_contract)
            if packet.get("delivery_unit_managed") is True:
                admission = delivery_unit_contract.verify_dispatch_binding(
                    envelope=envelope,
                    packet=packet,
                    contract=delivery_contract,
                    plan=plan,
                )
            else:
                sidecar_path = envelope.get("delivery_unit_sidecar_path")
                if not isinstance(sidecar_path, str) or not sidecar_path.strip():
                    sidecar_path = str(Path(envelope["packet_path"]).with_name(
                        Path(envelope["packet_path"]).name + ".delivery-unit.json"
                    ))
                try:
                    sidecar = json.loads(Path(sidecar_path).expanduser().resolve().read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise ValueError("delivery_unit_sidecar_unreadable") from exc
                admission = delivery_unit_contract.verify_legacy_sidecar(
                    materialized_packet, sidecar, delivery_contract, plan
                )
            if admission.get("verdict") != "GREEN":
                raise ValueError(f"delivery_unit_{admission.get('reason', 'packet_binding_failed')}")
            if (packet.get("completion_contract_digest") != digest_json(contract)
                and self.binding_receipt is None):
                raise ValueError("completion_contract_binding_missing")
            if packet.get("completion_contract") not in (None, contract):
                raise ValueError("completion_resolved_contract_mismatch")
            commands = contract.get("checks", [])
            integration_commands = contract.get("integration_checks", [])
            if not commands or not integration_commands:
                raise ValueError("completion_full_checks_missing")
            validate_full_validation_binding(contract, packet)
            if self.review_policy is not None and any(value is not None for value in
                                                      (checks_repair_binding, verifier_reconciliation, delivery_recovery)):
                # A v2 review cannot re-seal an immutable historical repair chain
                # by reading its old checks as related checks.
                raise ValueError("candidate_review_historical_repair_contract_unsupported")
            if contract.get("integration_inputs") and any(value is not None for value in (
                checks_repair_binding, verifier_reconciliation, delivery_recovery)):
                raise ValueError("completion_fanin_recovery_unsupported")
            current_workspace = str(Path(result["attempt_record"].get("workspace_ref") or envelope["worktree"]).resolve())
            source_workspace = current_workspace
            execution_workspace = current_workspace
            if recovery:
                if recovery_preflight is None:
                    raise ValueError("candidate_recovery_preflight_missing")
                source_workspace = recovery_preflight["source_workspace"]
                execution_workspace = recovery_preflight["execution_workspace"]
            root = execution_workspace
            if not contract.get("integration_worktree") or not contract.get("verifier_principal"):
                raise ValueError("completion_runtime_bindings_missing")
            integration_root = str(Path(contract["integration_worktree"]).resolve())
            continued_repair = self.store.get_continuation_repair(envelope["dispatch_key"])
            if continued_repair is not None:
                scope = continued_repair["scope"]
                if (result["attempt"] != scope["next_attempt"] or result["attempt_record"]["fence"] != scope["next_fence"]
                    or current_workspace != scope["workspace_ref"]
                    or getattr(self.execution_binding, "continuation_authority_digest", None) != continued_repair["authority_digest"]):
                    raise ValueError("continuation_repair_completion_identity_drift")
                integration_root = scope["integration_workspace_ref"]
            if root == integration_root or Path(root) in Path(integration_root).parents:
                raise ValueError("completion_integration_not_isolated")
            checks_repair_preflight = None
            if checks_repair_binding is not None:
                checks_repair_preflight = self._preflight_checks_repair(
                    envelope,
                    result,
                    checks_repair_binding,
                    packet=packet,
                    delivery_contract=delivery_contract,
                    current_workspace=current_workspace,
                    integration_root=integration_root,
                )
                execution_workspace = checks_repair_preflight["overlay"]
                root = execution_workspace
            attempt = result["attempt_record"]
            with private_git_index(root, self.store.root, label="candidate") as readonly_env:
                context = {"run_id": result["run_id"], "attempt": result["attempt"],
                           "fence": attempt["fence"], "work_unit_id": result["work_unit_id"],
                           "worker_id": attempt["holder"], "worktree": root,
                           "source_worktree": source_workspace if recovery else str(Path(envelope["worktree"]).resolve()),
                           "base_sha": envelope["wave_base_sha"],
                           "packet_digest": envelope["packet_digest"],
                           "dispatch_key": envelope["dispatch_key"],
                           "goal_id": envelope["goal_id"],
                           "goal_revision": envelope["goal_revision"],
                           "node_id": envelope["node_id"],
                           "diff_digest": candidate_diff_digest(root, envelope["wave_base_sha"],
                                                                env=readonly_env)}
                if recovery:
                    context["candidate_recovery"] = {
                        "schema": "lh-candidate-recovery-evidence/v1",
                        "admission_id": self.candidate_recovery_admission["admission_id"],
                        "admission_digest": self.candidate_recovery_admission["input_digest"],
                        "source_workspace": source_workspace,
                        "source_attempt": copy.deepcopy(self.candidate_recovery_admission["source_attempt"]),
                        "current_workspace": current_workspace,
                        "executor_status": "unknown",
                        "completion_contract_digest": digest_json(self.contract),
                        "delivery_contract_digest": self.delivery_contract["contract_digest"],
                        "contract_digest": delivery_contract["contract_digest"],
                        "unit_id": delivery_contract["unit_id"],
                        "goal_id": context["goal_id"],
                        "goal_revision": context["goal_revision"],
                        "node_id": context["node_id"],
                        "executor_recovery_receipt_digest": recovery_receipt["recovery_receipt_digest"],
                    }
                    if self.candidate_recovery_admission.get("kind") == "preserved_result_recovery":
                        context["candidate_recovery"].update(kind="preserved_result_recovery",
                            execution_workspace=execution_workspace,
                            source_inventory_digest=self.candidate_recovery_admission["source_workspace_inventory_digest"])
                if checks_repair_preflight is not None:
                    repair = checks_repair_preflight["binding"]
                    context["checks_repair"] = {
                        "schema": "lh-checks-repair-evidence/v1",
                        "repair_id": repair["repair_id"],
                        "repair_digest": repair["repair_digest"],
                        "original_candidate_receipt_digest": repair["original_candidate_receipt_digest"],
                        "original_checks_receipt_digest": repair["original_checks_receipt_digest"],
                        "candidate_before_digest": repair["candidate_before_digest"],
                        "candidate_after_digest": repair["candidate_after_digest"],
                        "checks_command_digest": repair["checks_command_digest"],
                        "changed_paths": list(checks_repair_preflight["changed_paths"]),
                        "same_run": True,
                        "same_attempt": True,
                        "same_fence": True,
                        "provider_invocations": 0,
                        "manual_prompts": 0,
                        "new_attempt": False,
                    }
                    if "predecessor_receipt_digest" in repair:
                        context["checks_repair"]["predecessor_receipt_digest"] = repair["predecessor_receipt_digest"]
                if checks_repair_preflight is not None:
                    repair = checks_repair_preflight["binding"]
                    candidate = repair["candidate_after_digest"]
                    changed = list(checks_repair_preflight["changed_paths"])
                    candidate_receipt = checks_repair_preflight["candidate_evidence"]
                    checks = self._effect(
                        context,
                        "checks_repair",
                        digest_json({
                            "repair_digest": repair["repair_digest"],
                            "candidate_before_digest": repair["candidate_before_digest"],
                            "candidate_after_digest": repair["candidate_after_digest"],
                            "checks_command_digest": repair["checks_command_digest"],
                        }),
                        lambda req: self._checks(req, repair["checks_commands"]),
                    )
                    if checks["candidate_digest"] != candidate:
                        raise ValueError("checks_repair_binding_result_candidate_mismatch")
                else:
                    candidate = candidate_digest(root, env=readonly_env)
                    changed = subprocess.check_output(
                        ["git", "-C", root, "diff", "--name-only", envelope["wave_base_sha"]],
                        env=readonly_env,
                    ).decode().splitlines()
                    changed += subprocess.check_output(
                        ["git", "-C", root, "ls-files", "--others", "--exclude-standard"],
                        env=readonly_env,
                    ).decode().splitlines()
                    if not changed:
                        raise ValueError("completion_candidate_noop")
                    try:
                        changed = delivery_unit_contract.verify_changed_paths_scope(
                            changed, packet, delivery_contract
                        )
                    except delivery_unit_contract.DeliveryUnitError as exc:
                        raise ValueError("completion_candidate_scope_escape") from exc
                    if packet.get("required_test_delta") and not any(path.startswith("tests/") for path in changed):
                        raise ValueError("completion_test_delta_missing")
                    candidate_receipt = self._effect(
                        context,
                        "candidate",
                        candidate,
                        lambda req: {
                            "candidate_digest": candidate,
                            "candidate_commit": candidate_object(req["worktree"], self.store.root),
                            "candidate_kind": "unpublished_snapshot_object",
                            "changed_paths": sorted(set(changed)),
                        },
                    )
                    if self.review_policy is not None:
                        # Review the candidate right after its related checks and
                        # before the full checks, so a requirement defect is found
                        # before the most expensive validation runs.
                        review_request = {**context, "candidate_digest": candidate,
                                          "candidate_commit": candidate_receipt["candidate_commit"],
                                          "checks_digest": digest_json(packet["targeted_commands"]),
                                          "review_related_commands": packet["targeted_commands"]}
                        verification = self._effect(review_request, "verifier",
                            digest_json([candidate, packet["targeted_commands"], self.review_policy]), self._verify)
                        if verification["verdict"] == "RED":
                            return self._retry_or_incomplete(envelope, result, verification, allow_retry=allow_retry)
                    if contract.get("recovery_environment_ref") is not None:
                        resumed = self.store.recovery_environment_resume(
                            context["run_id"], context["attempt"], context["fence"])
                        if resumed is not None:
                            if (recovery or delivery_recovery is not None or verifier_reconciliation is not None
                                or contract.get("integration_inputs")):
                                raise ValueError("recovery_environment_mixed_recovery_unsupported")
                            if resumed["candidate_digest"] != candidate:
                                raise ValueError("recovery_environment_candidate_drift")
                            context["environment_resume"] = resumed
                    checks = self._effect(context, "checks", candidate, lambda req: self._checks(req, commands))
            if checks["verdict"] == "RED":
                return self._retry_or_incomplete(envelope, result, checks, allow_retry=allow_retry)
            if stop_after_checks:
                return {"status": "incomplete", "reason": "completion_checks_checkpoint",
                        "checks": checks, "run_id": result["run_id"], "attempt": result["attempt"],
                        "fence": attempt["fence"]}
            request = {**context, "candidate_digest": candidate, "candidate_commit": checks["candidate_commit"],
                       "checks_digest": checks["receipt_digest"]}
            if self.review_policy is None:
                verification = self._effect(
                    request, "verifier", digest_json([candidate, checks["receipt_digest"]]), self._verify,
                    verifier_reconciliation=verifier_reconciliation,
                )
            if verification["verdict"] == "RED":
                return self._retry_or_incomplete(envelope, result, verification, allow_retry=allow_retry)
            pending_delivery = self.store.pending_delivery_dependencies(
                run_id=context["run_id"], attempt=context["attempt"], fence=context["fence"],
                only_unclaimed=True)
            if pending_delivery:
                return {"status": "waiting", "reason": "delivery_dependencies_not_integrated",
                        "dependencies": pending_delivery, "run_id": context["run_id"],
                        "attempt": context["attempt"], "fence": context["fence"]}
            def run_integrator(req):
                if verifier_reconciliation is not None:
                    with private_git_index(req["worktree"], self.store.root,
                                           label="integrator-source") as source_env:
                        marker = {
                            "schema": "host-q4-reconciliation-marker/v1",
                            "repair_id": verifier_reconciliation["repair_id"],
                            "repair_digest": verifier_reconciliation["repair_digest"],
                            "git_index_scope": "private_index_per_workspace",
                            "integration_prebuild": "claim_then_prebuild",
                        }
                        return self.integrator({
                            **req,
                            "git_index_file": source_env["GIT_INDEX_FILE"],
                            "reconciliation_marker": marker,
                        })
                return self.integrator(req)
            integrated = self._effect({**request, "integration_worktree": integration_root}, "integration",
                digest_json([candidate, integration_root]), run_integrator)
            if str(Path(integrated.get("worktree", "")).resolve()) != integration_root:
                raise ValueError("completion_integration_binding_invalid")
            with private_git_index(integration_root, self.store.root, label="integration") as integration_env:
                merged = candidate_digest(integration_root, env=integration_env)
                if (integrated.get("source_candidate_digest") != candidate
                    or integrated.get("integration_candidate_digest") != merged):
                    raise ValueError("completion_integration_candidate_missing")
                integrated_context = {
                    **context,
                    "worktree": integration_root,
                    "diff_digest": candidate_diff_digest(integration_root, envelope["wave_base_sha"],
                                                         env=integration_env),
                }
                if contract.get("integration_inputs"):
                    integrated_context.update(
                        integration_inputs_digest=integrated["integration_inputs_digest"],
                        integration_receipt_digest=integrated["receipt_digest"],
                        integration_inputs=integrated["integration_inputs"],
                    )
            def run_integration_checks(req):
                return self._run_integration_checks(req, integration_commands, delivery_contract,
                                                    delivery_recovery)
            check_context = {**integrated_context, **({"delivery_recovery": checks_recovery}
                if checks_recovery is not None else {})}
            check_binding = (digest_json([merged, checks_recovery["repair_id"],
                checks_recovery["runtime_source"], checks_recovery["predecessor_receipt_digest"]])
                             if checks_recovery is not None else merged)
            if final_recovery:
                cached = self.store.get_completion_phase(context["run_id"], context["attempt"],
                    context["fence"], "integration_checks", checks_recovery["repair_id"])
                if (not cached or cached["state"] != "settled" or cached["evidence"].get("verdict") != "GREEN"
                    or cached["evidence"].get("receipt_digest") != delivery_recovery["integration_checks_receipt_digest"]):
                    raise ValueError("delivery_recovery_checks_reuse_missing")
            final_checks = self._effect(check_context, "integration_checks", check_binding,
                run_integration_checks)
            if final_checks["verdict"] != "GREEN":
                return self._retry_or_incomplete(envelope, result, final_checks, allow_retry=allow_retry)
            final_verify = self._effect({**integrated_context, "candidate_digest": merged,
                "checks_digest": final_checks["receipt_digest"]}, "integration_verifier",
                digest_json([merged, final_checks["receipt_digest"]]), self._verify)
            if final_verify["verdict"] != "GREEN":
                return self._retry_or_incomplete(envelope, result, final_verify, allow_retry=allow_retry)
            delivery_evidence = None
            delivery_result = None
            candidate_delivery = candidate_receipt
            if delivery_contract is not None:
                delivery_plan = delivery_unit_contract.plan_delivery_unit(delivery_contract)
                independent_receipt = final_verify
                if recovery:
                    executor_delivery = delivery_unit_contract.recovery_executor_receipt(
                        delivery_contract, self.candidate_recovery_admission, result.get("receipt"), admission)
                else:
                    executor_delivery = result.get("executor_receipt")
                if checks_repair_preflight is not None:
                    candidate_delivery = delivery_unit_contract.repaired_candidate_evidence(delivery_contract,
                        checks_repair_preflight["binding"], checks_repair_preflight["candidate_evidence"],
                        checks_repair_preflight["checks_evidence"], checks)
                delivery_receipts = {
                    "plan_verdict": delivery_plan,
                    "packet_admission": admission,
                    "dispatch": result.get("receipt"),
                    "executor": executor_delivery,
                    "candidate": candidate_delivery,
                    "checks": checks,
                    "verifier": verification,
                    "integration": integrated,
                    "integration_checks": final_checks,
                    "integration_verifier": final_verify,
                    "completion": final_verify,
                }
                if checks_repair_preflight is not None:
                    delivery_receipts["original_candidate"] = checks_repair_preflight["candidate_evidence"]
                    delivery_receipts["original_checks"] = checks_repair_preflight["checks_evidence"]
                    delivery_receipts["checks_repair"] = checks
                delivery_evidence = {
                    "contract_digest": delivery_contract["contract_digest"],
                    "unit_id": delivery_contract["unit_id"],
                    "terminal_state": "eligible",
                    "worktree": integration_root,
                    "packet": copy.deepcopy(packet),
                    "changed_paths": sorted(set(changed)),
                    "identity": {key: integrated_context[key] for key in (
                        "goal_id", "goal_revision", "node_id", "dispatch_key", "run_id", "attempt", "fence", "base_sha", "diff_digest")}
                        | {"unit_id": delivery_contract["unit_id"]},
                    "verdict": "GREEN",
                    "verifier": {**final_verify, "receipt": independent_receipt},
                    "plan_verdict": delivery_plan,
                    "receipts": delivery_receipts,
                    "obligations": final_checks.get("delivery_obligations", {}),
                    "source_vs_live": delivery_contract["source_vs_live"],
                    "dispatch_binding": admission,
                }
                if checks_repair_preflight is not None:
                    delivery_evidence["checks_repair"] = copy.deepcopy(context["checks_repair"])
                    delivery_evidence["checks_repair_binding"] = copy.deepcopy(checks_repair_preflight["binding"])
                    delivery_evidence["original_checks_red"] = {
                        "candidate_receipt_digest": checks_repair_preflight["candidate_evidence"]["receipt_digest"],
                        "checks_receipt_digest": checks_repair_preflight["checks_evidence"]["receipt_digest"],
                        "checks_verdict": checks_repair_preflight["checks_evidence"].get("verdict"),
                        "candidate_digest": checks_repair_preflight["candidate_evidence"].get("candidate_digest"),
                    }
                if recovery:
                    delivery_evidence["candidate_recovery"] = copy.deepcopy(context["candidate_recovery"])
                    delivery_evidence["candidate_recovery_admission"] = copy.deepcopy(self.candidate_recovery_admission)
                    delivery_evidence["candidate_digest_before"] = candidate
                    delivery_evidence["candidate_digest_after"] = candidate
                    delivery_evidence["candidate_unchanged"] = True
                delivery_result = delivery_unit_contract.verify_delivery(
                    delivery_contract,
                    delivery_evidence,
                    phase="final",
                    allow_machine_complete_missing=True,
                    allow_delivery_verifier_missing=True,
                )
                delivery_receipt = self._effect(
                    {**integrated_context, **({"delivery_recovery": delivery_recovery} if final_recovery else {})},
                    "delivery_verifier",
                    (digest_json([merged, delivery_recovery["repair_id"], delivery_recovery["runtime_source"],
                        delivery_recovery["predecessor_receipt_digest"]]) if final_recovery
                     else delivery_unit_contract.digest_json(delivery_evidence)),
                    lambda req: {
                        "status": "delivery_verified" if delivery_result.get("verdict") == "GREEN" else "delivery_rejected",
                        "verdict": delivery_result.get("verdict", "RED"),
                        "delivery_verdict": delivery_result,
                        "delivery_evidence_digest": delivery_unit_contract.digest_json(delivery_evidence),
                    },
                )
                delivery_evidence["receipts"]["delivery_verifier"] = delivery_receipt
                if delivery_result.get("verdict") != "GREEN" or delivery_receipt.get("verdict") != "GREEN":
                    return self._retry_or_incomplete(envelope, result, delivery_receipt, allow_retry=allow_retry)
                self.store.record_delivery_verdict(
                    result["work_unit_id"],
                    verdict=delivery_result,
                    evidence=delivery_evidence,
                    run_id=context["run_id"],
                    attempt=context["attempt"],
                    fence=context["fence"],
                    phase="final",
                    preflight=True,
                )
            stage_receipts = {
                "candidate": candidate_delivery["receipt_digest"],
                "checks": checks["receipt_digest"],
                "verifier": verification["receipt_digest"],
                "integration": integrated["receipt_digest"],
                "integration_checks": final_checks["receipt_digest"],
                "integration_verifier": final_verify["receipt_digest"],
            }
            if checks_repair_preflight is not None:
                stage_receipts["candidate_original"] = candidate_receipt["receipt_digest"]
                stage_receipts["checks_repair"] = checks["receipt_digest"]
                stage_receipts["checks_original"] = checks_repair_preflight["checks_evidence"]["receipt_digest"]
            if delivery_evidence is not None:
                stage_receipts["delivery_verifier"] = delivery_receipt["receipt_digest"]
            machine_complete = self._effect(
                context,
                "machine_complete",
                digest_json(stage_receipts),
                lambda req: {"status": "machine_complete", "stage_receipts": dict(stage_receipts)},
            )
            stage_receipts["machine_complete"] = machine_complete["receipt_digest"]
            run = self.store.get_run(result["run_id"])
            if run["state"] == "running" and not self.store.finish_attempt(result["work_unit_id"],
                    ordinal=context["attempt"], holder=attempt["holder"], fence=attempt["fence"],
                    state="verified", receipt_digest=final_verify["receipt_digest"]):
                raise ValueError("completion_verified_fence_rejected")
            if not self.store.mark_integrated(result["work_unit_id"], holder=attempt["holder"],
                    fence=attempt["fence"], receipt_digest=final_verify["receipt_digest"]):
                raise ValueError("completion_terminal_fence_rejected")
            integrated_run = self.store.get_run(result["run_id"])
            if delivery_evidence is not None and delivery_result is not None:
                integrated_work_unit = self.store.get_work_unit(result["work_unit_id"])
                completion_body = {
                    "phase": "completion",
                    "status": "integrated",
                    "terminal_state": integrated_work_unit.get("state"),
                    "contract_digest": delivery_contract["contract_digest"],
                    "unit_id": delivery_contract["unit_id"],
                    "goal_id": integrated_context["goal_id"],
                    "goal_revision": integrated_context["goal_revision"],
                    "node_id": integrated_context["node_id"],
                    "dispatch_key": integrated_context["dispatch_key"],
                    "run_id": integrated_context["run_id"],
                    "attempt": integrated_context["attempt"],
                    "fence": integrated_context["fence"],
                    "base_sha": integrated_context["base_sha"],
                    "diff_digest": integrated_context["diff_digest"],
                    "source_receipt_digest": machine_complete["receipt_digest"],
                }
                completion_receipt = {**completion_body,
                                      "receipt_digest": delivery_unit_contract.receipt_digest(completion_body)}
                delivery_evidence["terminal_state"] = integrated_work_unit.get("state")
                delivery_evidence["receipts"]["machine_complete"] = machine_complete
                delivery_evidence["receipts"]["completion"] = completion_receipt
                delivery_evidence["state_readback"] = {
                    "authority_store": delivery_contract["authority_store"],
                    "run_state": integrated_run.get("state"),
                    "work_unit_state": integrated_work_unit.get("state"),
                }
                delivery_result = delivery_unit_contract.verify_delivery(delivery_contract, delivery_evidence, phase="final")
                if delivery_result.get("verdict") != "GREEN":
                    return {"status": "incomplete", "reason": "delivery_verifier_red", "delivery": delivery_result,
                            "stage_receipts": stage_receipts}
            return {"status": "integrated", "receipt": final_verify,
                    "stage_receipts": stage_receipts, "live_acceptance": False,
                    **({"delivery": delivery_result, "delivery_evidence": delivery_evidence}
                       if delivery_result is not None else {})}
        except PhaseJobPending as exc:
            return {"status": "waiting", "reason": exc.reason, "phase_job_id": exc.job["job_id"],
                    "run_id": result["run_id"], "attempt": result["attempt"],
                    "fence": result["attempt_record"]["fence"]}
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
            return {"status": "incomplete", "reason": str(exc)}

"""Bounded task-area orchestration over existing LH admission and completion.

The canonical manifest is an input, not a second queue or state store. Only
WorkUnitStore states release dependencies. Consumer admission owns identity;
this caller never starts an Attempt itself or treats callback GREEN as done.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Callable

from .parallel_scheduler import ParallelScheduler, SuccessorDispatchConsumer
from .work_unit_store import WorkUnitStore, digest_json, validate_delivery_after
from .platform_ports import (
    FileLockSchedulerPort,
    PlatformPortUnavailable,
    make_file_private,
    read_private_file,
    sync_directory,
    verify_file_private,
)


class TaskAreaError(ValueError):
    pass


class _ManifestPersistenceOutcomeUnknown(TaskAreaError):
    def __init__(self, expected_digest: str):
        super().__init__("reviewed_manifest_persistence_outcome_unknown")
        self.expected_digest = expected_digest


def manifest_digest(manifest: dict) -> str:
    return digest_json({key: value for key, value in manifest.items() if key != "approval"})


def manifest_body_digest(manifest: dict) -> str:
    return digest_json({key: value for key, value in manifest.items()
                       if key not in {"approval", "plan_verifier"}})


class TaskAreaController:
    def __init__(self, store: WorkUnitStore, *, consumer_factory: Callable,
                 approved_manifest_digest: str | None, max_workers: int = 3,
                 recovery_port: Callable | None = None):
        if isinstance(max_workers, bool) or max_workers not in {1, 2, 3}:
            raise TaskAreaError("worker_limit_invalid")
        self.store = store
        self.consumer_factory = consumer_factory
        self.approved_digest = approved_manifest_digest
        self.max_workers = max_workers
        self.recovery_port = recovery_port
        self._disposition_manifest_path: Path | None = None
        self._unresolved_manifest_events: dict[str, tuple[str, str]] = {}

    @staticmethod
    def _delivery_fields(task: dict) -> dict:
        fields = {}
        if "resource_requirements" in task:
            fields["resource_requirements"] = TaskAreaController._resource_shape(task["resource_requirements"])
        if "phase_execution" in task:
            if task["phase_execution"] != "durable-v1":
                raise TaskAreaError("task_area_phase_execution_invalid")
            fields["phase_execution"] = task["phase_execution"]
        if "delivery_after" not in task:
            return fields
        try:
            return {**fields, "delivery_after": validate_delivery_after(task["delivery_after"])}
        except ValueError as exc:
            raise TaskAreaError(f"task_area_graph_invalid:{exc}") from exc

    @staticmethod
    def _resource_shape(value: dict) -> dict:
        reason = "task_area_resource_requirements_invalid"
        kinds = {"repo", "git_common_dir", "index", "store", "scratch", "api", "external_write", "process_service"}
        phases = {"coding", "checks", "verifier", "integration", "integration_checks", "integration_verifier"}
        if (not isinstance(value, dict) or set(value) != {"schema", "resources"}
            or value.get("schema") != "lh-task-resources/v1" or not isinstance(value.get("resources"), list)
            or len(value["resources"]) > 64):
            raise TaskAreaError(reason)
        seen = set()
        for item in value["resources"]:
            if (not isinstance(item, dict) or set(item) != {"kind", "ref", "access", "version_digest", "phases"}
                or not isinstance(item.get("kind"), str) or item["kind"] not in kinds
                or item.get("access") not in ("read", "write")
                or not isinstance(item.get("phases"), list) or not item["phases"]
                or any(not isinstance(p, str) or p not in phases for p in item["phases"])
                or len(set(item["phases"])) != len(item["phases"])
                or not isinstance(item.get("version_digest"), str)
                or len(item["version_digest"]) != 71 or not item["version_digest"].startswith("sha256:")
                or any(c not in "0123456789abcdef" for c in item["version_digest"][7:])):
                raise TaskAreaError(reason)
            ref = item["ref"]
            if (not isinstance(ref, dict) or set(ref) != {"path", "content_digest"}
                or not isinstance(ref.get("path"), str) or not isinstance(ref.get("content_digest"), str)):
                raise TaskAreaError(reason)
            key = (item["kind"], ref["path"])
            if key in seen:
                raise TaskAreaError(reason)
            seen.add(key)
        return value

    @staticmethod
    def _read_resource_ref(ref: dict) -> dict:
        try:
            if not isinstance(ref, dict) or set(ref) != {"path", "content_digest"}:
                raise ValueError("ref")
            path = Path(ref["path"])
            if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_file():
                raise ValueError("path")
            raw = path.read_bytes()
            value = json.loads(raw)
            if not isinstance(value, dict) or "sha256:" + hashlib.sha256(raw).hexdigest() != ref["content_digest"]:
                raise ValueError("digest")
            return value
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise TaskAreaError("task_area_resource_binding_unknown") from exc

    @staticmethod
    def _resource_git_path(worktree: str, argument: str) -> str:
        from .work_unit_completion import git_readonly_env
        root = Path(worktree).resolve(strict=True)
        try:
            result = subprocess.run(["git", "--no-replace-objects", "-C", str(root), "rev-parse", "--git-path", argument],
                check=True, capture_output=True, text=True, env=git_readonly_env(), timeout=5)
            path = Path(result.stdout.strip())
            return str((path if path.is_absolute() else root / path).resolve())
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise TaskAreaError("task_area_resource_binding_unknown") from exc

    def resource_material(self, manifest: dict, task: dict, config: dict | None = None) -> dict | None:
        """Resolve only source-bound declarations against this runner's actual inputs."""
        if config is None:
            binding = manifest.get("reviewed_task_list_binding") or manifest.get("reviewed_task_list_draft")
            source, _ = self._read_approval_source(binding["source_approval_ref"])
            config = self._read_resource_ref(source["runner_config_ref"])
        if "resource_bindings_ref" not in config:
            if "resource_requirements" in task:
                raise TaskAreaError("task_area_resource_binding_unknown")
            return None
        declaration = self._resource_shape(task.get("resource_requirements"))
        scope = {key: config[key] for key in ("store_root", "host_contract_ref")}
        catalog = self._read_resource_ref(config["resource_bindings_ref"])
        if (set(catalog) != {"schema", "scope", "bindings"} or catalog.get("schema") != "lh-task-resource-catalog/v1"
            or catalog.get("scope") != scope or not isinstance(catalog.get("bindings"), list)):
            raise TaskAreaError("task_area_resource_requirements_invalid")
        rows = [row for row in catalog["bindings"] if isinstance(row, dict)
                and row.get("goal_id") == manifest["goal_id"] and row.get("node_id") == task["node_id"]]
        if len(rows) != 1:
            raise TaskAreaError("task_area_resource_binding_unknown")
        row = rows[0]
        envelope = task["envelope"]
        packet_ref = {"path": envelope["packet_path"],
                      "content_digest": "sha256:" + hashlib.sha256(Path(envelope["packet_path"]).read_bytes()).hexdigest()}
        if (set(row) != {"goal_id", "node_id", "worktree", "packet_ref", "integration_worktree", "resources"}
            or row.get("resources") != declaration or row.get("packet_ref") != packet_ref
            or row.get("worktree") != envelope["worktree"]
            or row.get("integration_worktree") != task["completion_contract"]["integration_worktree"]):
            raise TaskAreaError("task_area_resource_identity_mismatch")
        root = str(Path(envelope["worktree"]).resolve(strict=True))
        common = str(Path(self._resource_git_path(root, "objects")).parent)
        actual = {"repo": {"common_dir": common, "worktree": root},
                  "git_common_dir": {"path": common}, "index": {"path": self._resource_git_path(root, "index")},
                  "store": {"path": str(self.store.root.resolve())},
                  "scratch": {"path": str(Path(os.environ.get("LH_TASK_TMP_ROOT", tempfile.gettempdir())).resolve())}}
        material, seen = [], set()
        for resource in declaration["resources"]:
            kind = resource["kind"]
            document = self._read_resource_ref(resource["ref"])
            if (set(document) != {"schema", "kind", "scope", "target", "version_digest"}
                or document.get("schema") != "lh-task-resource-binding/v1" or document.get("kind") != kind
                or document.get("scope") != scope or document.get("version_digest") != resource["version_digest"]):
                raise TaskAreaError("task_area_resource_requirements_invalid")
            target = document["target"]
            if kind in actual:
                if target != actual[kind]:
                    raise TaskAreaError("task_area_resource_identity_mismatch")
                if set(resource["phases"]) != {"coding", "checks", "verifier", "integration", "integration_checks", "integration_verifier"}:
                    raise TaskAreaError("task_area_resource_requirements_invalid")
                seen.add(kind)
            elif kind == "api":
                if not isinstance(target, dict) or set(target) != {"contract_ref"}:
                    raise TaskAreaError("task_area_resource_requirements_invalid")
                self._read_resource_ref(target["contract_ref"])
                if resource["version_digest"] != target["contract_ref"]["content_digest"]:
                    raise TaskAreaError("task_area_resource_identity_mismatch")
            else:
                # This execution binding has no observed service/external-write
                # capability. A correctly typed caller label cannot create one.
                raise TaskAreaError("task_area_resource_binding_unknown")
            identity = target if kind != "api" else {"path": target["contract_ref"]["path"]}
            material.append({**resource, "identity": identity, "resource_key": digest_json([kind, identity])})
        if seen != set(actual):
            raise TaskAreaError("task_area_resource_requirements_invalid")
        heavy = []
        full = task["completion_contract"].get("full_validation_plan", {})
        configured = config.get("full_validation_bindings", [])
        if not isinstance(configured, list):
            raise TaskAreaError("task_area_resource_requirements_invalid")
        matched = [entry for entry in configured if isinstance(entry, dict)
                   and entry.get("goal_id") == manifest["goal_id"] and entry.get("node_id") == task["node_id"]]
        if full.get("resource_class") == "heavy" or matched:
            expected = {"goal_id": manifest["goal_id"], "node_id": task["node_id"], "packet_ref": packet_ref,
                "plan_id": full.get("plan_id"), "plan_digest": digest_json(full), "resource_class": "heavy",
                "phases": ["checks", "integration_checks"], "commands": full.get("commands")}
            if (matched != [expected] or not isinstance(full.get("plan_id"), str) or not full["plan_id"].strip()
                or full.get("resource_class") != "heavy" or not full.get("commands")
                or any(task["completion_contract"].get(phase) != full["commands"] for phase in expected["phases"])):
                raise TaskAreaError("task_area_resource_identity_mismatch")
            heavy = expected["phases"]
        return {"resources": material, "repository": common, "heavy_phases": heavy,
                "declaration_digest": digest_json(declaration)}

    @staticmethod
    def _validate_task_graph(tasks: list[dict]) -> None:
        try:
            WorkUnitStore._validate_graph_records([
                {"work_unit_id": task["node_id"], "node_id": task["node_id"],
                 "dependencies": task.get("depends_on", []),
                 **TaskAreaController._delivery_fields(task)} for task in tasks])
        except (ValueError, KeyError) as exc:
            if isinstance(exc, TaskAreaError):
                raise
            raise TaskAreaError(f"task_area_graph_invalid:{exc}") from exc

    @staticmethod
    def _reviewed_task_digest(manifest: dict, task: dict) -> str:
        envelope = task.get("envelope")
        if not isinstance(envelope, dict):
            raise TaskAreaError("reviewed_task_envelope_missing")
        packet_path = Path(envelope.get("packet_path", ""))
        try:
            packet = json.loads(packet_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise TaskAreaError("reviewed_task_packet_invalid") from exc
        packet_body = packet.get("packet", packet)
        identity = {
            "node_id": task.get("node_id"),
            "goal_id": envelope.get("goal_id"),
            "goal_revision": envelope.get("goal_revision"),
            "base_sha": envelope.get("wave_base_sha"),
            "packet_digest": envelope.get("packet_digest"),
            "envelope_digest": envelope.get("envelope_digest"),
            "task": packet_body.get("task"),
            "depends_on": list(task.get("depends_on", [])),
            "read_set": list(task.get("read_set", [])),
            "write_set": list(task.get("write_set", [])),
            "required_preconditions": list(task.get("required_preconditions", [])),
            **TaskAreaController._delivery_fields(task),
        }
        return digest_json(identity)

    @staticmethod
    def _read_approval_source(source_ref: dict) -> tuple[dict, str]:
        if not isinstance(source_ref, dict):
            raise TaskAreaError("reviewed_source_approval_ref_missing")
        path_value = source_ref.get("path")
        expected = source_ref.get("content_digest")
        if not isinstance(path_value, str) or not isinstance(expected, str):
            raise TaskAreaError("reviewed_source_approval_ref_invalid")
        try:
            raw = Path(path_value).read_bytes()
            source = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise TaskAreaError("reviewed_source_approval_unreadable") from exc
        actual = "sha256:" + hashlib.sha256(raw).hexdigest()
        if actual != expected or source_ref.get("approval_id") != source.get("approval_id"):
            raise TaskAreaError("reviewed_source_approval_digest_mismatch")
        if (source.get("schema") != "host-reviewed-task-list-source-approval/v1"
            or source.get("status") != "approved" or source.get("revoked") is not False):
            raise TaskAreaError("reviewed_source_approval_not_active")
        expires_at = source.get("expires_at")
        try:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except (AttributeError, TypeError, ValueError) as exc:
            raise TaskAreaError("reviewed_source_approval_expiry_invalid") from exc
        if expiry.tzinfo is None or expiry <= datetime.now(timezone.utc):
            raise TaskAreaError("reviewed_source_approval_expired")
        if not isinstance(source.get("issuer"), str) or not source["issuer"].strip():
            raise TaskAreaError("reviewed_source_approval_issuer_missing")
        return source, actual

    @staticmethod
    def _task_area_event_digest(event: dict) -> str:
        return digest_json({
            "event_id": event["event_id"],
            "parent_goal_id": event["parent_goal_id"],
            "event_type": event["event_type"],
            "payload": event["payload"],
            "created_at": float(event["created_at"]),
        })

    def _task_area_event_receipt(self, manifest: dict, event_id: str,
                                 event_type: str) -> dict | None:
        events = self.store.events(manifest["goal_id"])
        source = next((row for row in events
                       if row["event_id"] == event_id
                       and row["event_type"] == event_type), None)
        if source is None:
            raise TaskAreaError("reviewed_task_list_event_source_missing")
        claims = [row["payload"] for row in events
                  if row["event_type"] == "task_area_event_claimed"
                  and row["payload"].get("source_event_id") == event_id]
        settlements = [row["payload"] for row in events
                       if row["event_type"] == "task_area_event_settled"
                       and row["payload"].get("source_event_id") == event_id]
        if len(claims) > 1 or len(settlements) > 1:
            raise TaskAreaError("reviewed_task_list_event_claim_or_settlement_not_unique")
        source_digest = self._task_area_event_digest(source)
        if not claims:
            if settlements:
                raise TaskAreaError("reviewed_task_list_event_settlement_without_claim")
            return None
        claim = claims[0]
        if claim.get("source_event_digest") != source_digest:
            raise TaskAreaError("reviewed_task_list_event_source_digest_mismatch")
        if not settlements:
            return {"source": source, "source_event_digest": source_digest,
                    "claim": claim, "settlement": None, "outcome": None}
        settlement = settlements[0]
        outcome = settlement.get("outcome")
        if (claim.get("claim_id") != settlement.get("claim_id")
            or not isinstance(outcome, dict)
            or settlement.get("outcome_digest") != digest_json(outcome)):
            raise TaskAreaError("reviewed_task_list_event_settlement_binding_invalid")
        return {"source": source, "source_event_digest": source_digest,
                "claim": claim, "settlement": settlement, "outcome": outcome}

    def _reviewed_approval_event_receipt(self, manifest: dict) -> dict | None:
        binding = manifest.get("reviewed_task_list_binding")
        materialization = binding.get("materialization") if isinstance(binding, dict) else None
        event_id = materialization.get("source_event_id") if isinstance(materialization, dict) else None
        if not isinstance(event_id, str) or not event_id:
            raise TaskAreaError("reviewed_task_list_approval_event_missing")
        receipt = self._task_area_event_receipt(
            manifest, event_id, "task_list_approval_updated")
        if receipt is None or receipt["settlement"] is None:
            return None
        source, outcome = receipt["source"], receipt["outcome"]
        payload = source.get("payload", {})
        if (source.get("parent_goal_id") != manifest.get("goal_id")
            or payload.get("source_approval_ref") != binding.get("source_approval_ref")
            or payload.get("source_approval_digest")
                != materialization.get("source_approval_digest")
            or payload.get("task_list_digest") != binding.get("task_list_digest")):
            raise TaskAreaError("reviewed_task_list_approval_event_binding_invalid")
        projection = manifest.get("reviewed_task_list_pending_revision")
        expected_materialization_digest = (
            projection.get("source_approval_materialization_digest")
            if isinstance(projection, dict) else manifest_digest(manifest))
        if (outcome.get("status") != "accepted"
            or outcome.get("kind") != "task_list_materialized"
            or outcome.get("materialization_digest") != expected_materialization_digest):
            raise TaskAreaError("reviewed_task_list_approval_event_not_accepted")
        return {**receipt, "event_ref": {
            "event_id": event_id,
            "source_event_digest": receipt["source_event_digest"],
        }}

    @staticmethod
    def _read_digest_bound_json(ref: dict, *, reason: str) -> tuple[dict, bytes]:
        if (not isinstance(ref, dict) or set(ref) != {"path", "content_digest"}
            or not isinstance(ref.get("path"), str)
            or not isinstance(ref.get("content_digest"), str)):
            raise TaskAreaError(reason + "_ref_invalid")
        path = Path(ref["path"]).expanduser()
        if not path.is_absolute():
            raise TaskAreaError(reason + "_path_not_absolute")
        try:
            raw = read_private_file(path)
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, ValueError, PlatformPortUnavailable) as exc:
            raise TaskAreaError(reason + "_unreadable") from exc
        actual = "sha256:" + hashlib.sha256(raw).hexdigest()
        if actual != ref["content_digest"] or not isinstance(value, dict):
            raise TaskAreaError(reason + "_digest_or_shape_mismatch")
        return value, raw

    def phase_policy_material(self, manifest: dict) -> dict | None:
        """Derive the fixed local-Store policy from the original approved source.

        A CLI path is only a selector. The source-bound runner file, including
        its host contract, must name this actual Store; no task supplies a pool.
        """
        if not any("phase_execution" in task for task in manifest.get("tasks", [])):
            return None
        for task in manifest["tasks"]:
            self._delivery_fields(task)
        binding = manifest.get("reviewed_task_list_binding") or manifest.get("reviewed_task_list_draft")
        if not isinstance(binding, dict):
            raise TaskAreaError("phase_job_reviewed_source_required")
        source_ref = binding.get("source_approval_ref")
        source, _ = self._read_approval_source(source_ref)
        ref = source.get("runner_config_ref")
        # Like the existing external execution-binding references, this file
        # is digest-bound public configuration, not a private authority file.
        if (not isinstance(ref, dict) or set(ref) != {"path", "content_digest"}
            or not isinstance(ref.get("path"), str) or not isinstance(ref.get("content_digest"), str)):
            raise TaskAreaError("phase_job_runner_config_ref_invalid")
        try:
            path = Path(ref["path"])
            if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_file():
                raise TaskAreaError("phase_job_runner_config_path_invalid")
            raw = path.read_bytes()
            config = json.loads(raw)
        except (OSError, UnicodeError, ValueError) as exc:
            raise TaskAreaError("phase_job_runner_config_unreadable") from exc
        if ("sha256:" + hashlib.sha256(raw).hexdigest() != ref["content_digest"]
            or not isinstance(config, dict)):
            raise TaskAreaError("phase_job_runner_config_digest_or_shape_mismatch")
        if (not {"schema", "store_root", "host_contract_ref"} <= set(config)
            or set(config) - {"schema", "store_root", "host_contract_ref", "resource_bindings_ref", "full_validation_bindings"}
            or config.get("schema") != "lh-task-area-phase-runner-config/v1"
            or config.get("store_root") != str(self.store.root.resolve())):
            raise TaskAreaError("phase_job_runner_store_mismatch")
        from .runner_adapter import resolve_task_area_execution_binding
        resolved = resolve_task_area_execution_binding(manifest)
        if config.get("host_contract_ref") != resolved.raw.get("host_contract_ref"):
            raise TaskAreaError("phase_job_runner_host_mismatch")
        for task in manifest["tasks"]:
            if task.get("phase_execution") == "durable-v1":
                self.resource_material(manifest, task, config)
        return {"schema": "lh-task-area-phase-policy/v1", "phase_execution": "durable-v1",
                "max_workers": 2,
                "scope": {key: config[key] for key in ("store_root", "host_contract_ref")},
                "runner_config_ref": ref, "source_approval_ref": source_ref}

    @staticmethod
    def _reviewed_candidate_task_digest(manifest: dict, task: dict) -> str:
        if not isinstance(task, dict) or not isinstance(task.get("node_id"), str):
            raise TaskAreaError("reviewed_revision_task_material_invalid")
        for field in ("depends_on", "read_set", "write_set", "required_preconditions"):
            if not isinstance(task.get(field, []), list):
                raise TaskAreaError("reviewed_revision_task_material_shape_invalid")
        envelope = task.get("envelope")
        if not isinstance(envelope, dict):
            raise TaskAreaError("reviewed_revision_task_envelope_missing")
        node = task["node_id"]
        if (envelope.get("node_id") != node
            or envelope.get("goal_id") != manifest.get("goal_id")
            or envelope.get("goal_revision") != manifest.get("goal_revision")
            or envelope.get("wave_base_sha") != manifest.get("base_sha")):
            raise TaskAreaError("reviewed_revision_task_owner_scope_mismatch")
        packet_path = Path(envelope.get("packet_path", "")).expanduser()
        if not packet_path.is_absolute():
            raise TaskAreaError("reviewed_revision_packet_path_not_absolute")
        try:
            materialized = json.loads(read_private_file(packet_path).decode("utf-8"))
        except (OSError, UnicodeError, ValueError, PlatformPortUnavailable) as exc:
            raise TaskAreaError("reviewed_revision_packet_unreadable") from exc
        if not isinstance(materialized, dict):
            raise TaskAreaError("reviewed_revision_packet_invalid")
        packet_body = materialized.get("packet", materialized)
        packet_digest_body = {key: value for key, value in materialized.items()
                              if key != "packet_digest"}
        envelope_digest_body = {key: value for key, value in envelope.items()
                                if key != "envelope_digest"}
        if (not isinstance(packet_body, dict)
            or digest_json(packet_digest_body) != envelope.get("packet_digest")
            or digest_json(envelope_digest_body) != envelope.get("envelope_digest")):
            raise TaskAreaError("reviewed_revision_packet_or_envelope_digest_mismatch")
        identity = {
            "node_id": node,
            "goal_id": envelope.get("goal_id"),
            "goal_revision": envelope.get("goal_revision"),
            "base_sha": envelope.get("wave_base_sha"),
            "packet_digest": envelope.get("packet_digest"),
            "envelope_digest": envelope.get("envelope_digest"),
            "task": packet_body.get("task"),
            "depends_on": list(task.get("depends_on", [])),
            "read_set": list(task.get("read_set", [])),
            "write_set": list(task.get("write_set", [])),
            "required_preconditions": list(task.get("required_preconditions", [])),
            **TaskAreaController._delivery_fields(task),
        }
        return digest_json(identity)

    @staticmethod
    def _reviewed_task_material_digest(task: dict) -> str:
        if not isinstance(task, dict):
            raise TaskAreaError("reviewed_revision_task_material_invalid")
        return digest_json({key: value for key, value in task.items() if key != "status"})

    def _archive_prior_manifest(self, manifest_path: Path, prior_manifest: dict,
                                prior_bytes: bytes) -> dict:
        if not Path(manifest_path).is_absolute():
            raise TaskAreaError("reviewed_manifest_path_must_be_absolute")
        content_digest = "sha256:" + hashlib.sha256(prior_bytes).hexdigest()
        path = Path(manifest_path).with_name(
            Path(manifest_path).name + ".prior-" + content_digest.removeprefix("sha256:") + ".json")
        ref = {"path": str(path.resolve()), "content_digest": content_digest}
        if path.exists():
            try:
                verify_file_private(path)
                existing = read_private_file(path)
            except (OSError, PlatformPortUnavailable, TaskAreaError) as exc:
                raise _ManifestPersistenceOutcomeUnknown(manifest_digest(prior_manifest)) from exc
            if existing != prior_bytes:
                raise TaskAreaError("reviewed_prior_manifest_archive_conflict")
            return ref
        try:
            self._write_atomic_bytes(path, prior_bytes, manifest_digest(prior_manifest))
        except _ManifestPersistenceOutcomeUnknown as exc:
            raise _ManifestPersistenceOutcomeUnknown(manifest_digest(prior_manifest)) from exc
        except TaskAreaError as exc:
            if str(exc) == "reviewed_manifest_persistence_unavailable":
                raise _ManifestPersistenceOutcomeUnknown(manifest_digest(prior_manifest)) from exc
            raise
        try:
            verify_file_private(path)
            readback = read_private_file(path)
        except (OSError, PlatformPortUnavailable, TaskAreaError) as exc:
            raise _ManifestPersistenceOutcomeUnknown(manifest_digest(prior_manifest)) from exc
        if readback != prior_bytes:
            raise _ManifestPersistenceOutcomeUnknown(manifest_digest(prior_manifest))
        return ref

    @staticmethod
    def _read_archived_prior_manifest(ref: dict) -> tuple[dict, bytes]:
        if (not isinstance(ref, dict) or set(ref) != {"path", "content_digest"}
            or not isinstance(ref.get("path"), str)
            or not isinstance(ref.get("content_digest"), str)):
            raise TaskAreaError("reviewed_prior_manifest_ref_invalid")
        path = Path(ref["path"]).expanduser()
        if not path.is_absolute():
            raise TaskAreaError("reviewed_prior_manifest_path_not_absolute")
        try:
            verify_file_private(path)
            raw = read_private_file(path)
            prior = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, ValueError,
                PlatformPortUnavailable) as exc:
            raise TaskAreaError("reviewed_prior_manifest_archive_invalid") from exc
        actual = "sha256:" + hashlib.sha256(raw).hexdigest()
        if (actual != ref["content_digest"]
            or not isinstance(prior, dict)):
            raise TaskAreaError("reviewed_prior_manifest_digest_mismatch")
        return prior, raw

    def _revision_effect_exists(self, manifest: dict, task: dict | None,
                                units: dict[str, dict]) -> bool:
        node = task.get("node_id") if isinstance(task, dict) else None
        unit = units.get(node) if isinstance(node, str) else None
        if unit is not None:
            run = self.store.get_run_for_work_unit(unit["work_unit_id"])
            if run is not None or unit.get("state") not in {"pending", "ready"}:
                return True
            if self.store.recovery_requests(work_unit_id=unit["work_unit_id"]):
                return True
        envelope = task.get("envelope") if isinstance(task, dict) else None
        dispatch_key = envelope.get("dispatch_key") if isinstance(envelope, dict) else None
        if isinstance(dispatch_key, str) and dispatch_key:
            return self.store.get_dispatch_consumption(dispatch_key) is not None
        return False

    def _revision_task_classification(self, manifest: dict, candidate_rows: dict,
                                      candidate_tasks: dict, candidate_digests: dict) -> dict:
        binding = manifest["reviewed_task_list_binding"]
        original_list = binding["task_list"]
        original_rows = {row["node_id"]: row for row in original_list["tasks"]}
        approved = binding["authorization"]["scope"]["approved_task_digests"]
        original_tasks = {task["node_id"]: task for task in manifest.get("tasks", [])}
        direct = set()
        all_nodes = set(original_rows) | set(candidate_rows) | set(original_tasks)
        for node in all_nodes:
            if node not in original_rows or node not in candidate_rows:
                direct.add(node)
            else:
                original_task = original_tasks.get(node)
                candidate_task = candidate_tasks.get(node)
                row_fields_changed = {
                    key: value for key, value in candidate_rows[node].items()
                    if key not in {"status", "task_digest"}
                } != {
                    key: value for key, value in original_rows[node].items()
                    if key not in {"status", "task_digest"}
                }
                material_changed = (
                    original_task is None or candidate_task is None
                    or self._reviewed_task_material_digest(original_task)
                        != self._reviewed_task_material_digest(candidate_task))
                baseline_digest = approved.get(node, original_rows[node].get("task_digest"))
                if (candidate_digests.get(node) != baseline_digest
                    or row_fields_changed or material_changed):
                    direct.add(node)

        dependents: dict[str, set[str]] = {}
        for node, row in original_rows.items():
            deps = original_tasks.get(node, {}).get(
                "depends_on", row.get("depends_on", []))
            if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps):
                raise TaskAreaError("reviewed_original_task_dependencies_invalid")
            deps = [*deps, *self._delivery_fields(original_tasks.get(node, row)).get("delivery_after", [])]
            for dependency in deps:
                dependents.setdefault(dependency, set()).add(node)
        for node, task in candidate_tasks.items():
            deps = task.get("depends_on", [])
            if not isinstance(deps, list) or any(not isinstance(dep, str) for dep in deps):
                raise TaskAreaError("reviewed_candidate_task_dependencies_invalid")
            deps = [*deps, *self._delivery_fields(task).get("delivery_after", [])]
            for dependency in deps:
                dependents.setdefault(dependency, set()).add(node)
        affected = set(direct)
        pending = list(direct)
        while pending:
            changed_node = pending.pop()
            for dependent in dependents.get(changed_node, set()):
                if dependent not in affected:
                    affected.add(dependent)
                    pending.append(dependent)

        units = {unit["node_id"]: unit
                 for unit in self.store.list_work_units(manifest["goal_id"])}
        dispositions = {}
        for node in sorted(all_nodes):
            if node in affected:
                task = original_tasks.get(node) or candidate_tasks.get(node)
                if self._revision_effect_exists(manifest, task, units):
                    disposition = "existing_effect_reconcile"
                else:
                    disposition = "pending_review"
            elif node in approved and candidate_digests.get(node) == approved[node]:
                disposition = "unchanged_original_approval"
            else:
                disposition = "not_originally_approved"
            dispositions[node] = disposition
        unchanged = sorted(node for node, disposition in dispositions.items()
                           if disposition == "unchanged_original_approval")
        return {
            "schema": "host-reviewed-task-list-revision-classification/v1",
            "original_task_list_digest": binding["task_list_digest"],
            "original_revision": original_list.get("revision"),
            "original_approved_task_digests": dict(approved),
            "candidate_task_digests": dict(sorted(candidate_digests.items())),
            "candidate_task_material_digests": {
                node: self._reviewed_task_material_digest(candidate_tasks[node])
                for node in sorted(candidate_tasks)
            },
            "affected_nodes": sorted(affected),
            "unchanged_approved_nodes": unchanged,
            "new_nodes": sorted(set(candidate_rows) - set(original_rows)),
            "removed_nodes": sorted(set(original_rows) - set(candidate_rows)),
            "task_dispositions": dispositions,
        }

    def _build_pending_revision_projection(self, prior_manifest: dict, event: dict,
                                           proposal_ref: dict,
                                           prior_manifest_ref: dict) -> dict:
        binding = prior_manifest.get("reviewed_task_list_binding")
        if not isinstance(binding, dict):
            raise TaskAreaError("reviewed_task_list_binding_missing")
        receipt = self._reviewed_approval_event_receipt(prior_manifest)
        if receipt is None:
            raise TaskAreaError("reviewed_task_list_approval_event_pending")
        source_approval_ref = binding.get("source_approval_ref")
        source_approval_event_ref = receipt["event_ref"]
        if (event.get("event_type") != "task_list_revision_pending"
            or event.get("parent_goal_id") != prior_manifest.get("goal_id")
            or event.get("payload") != {"proposal_ref": proposal_ref}):
            raise TaskAreaError("reviewed_task_list_revision_event_binding_invalid")
        proposal, _proposal_bytes = self._read_digest_bound_json(
            proposal_ref, reason="reviewed_task_list_revision_proposal")
        expected_proposal_fields = {
            "schema", "status", "task_list_id", "revision", "base_revision",
            "base_task_list_digest", "source_approval_ref",
            "source_approval_event_ref", "supersedes", "candidate_task_list",
            "candidate_task_list_digest", "task_material_refs",
        }
        if (set(proposal) != expected_proposal_fields
            or proposal.get("schema") != "host-task-list-pending-revision/v1"
            or proposal.get("status") != "pending_review"):
            raise TaskAreaError("reviewed_task_list_revision_proposal_invalid")
        task_list_id = binding["task_list"].get("task_list_id")
        previous = prior_manifest.get("reviewed_task_list_pending_revision")
        base_list = (previous.get("candidate_task_list")
                     if isinstance(previous, dict) else binding["task_list"])
        base_digest = (previous.get("candidate_task_list_digest")
                       if isinstance(previous, dict) else binding["task_list_digest"])
        base_revision = base_list.get("revision") if isinstance(base_list, dict) else None
        if (not isinstance(base_revision, int) or isinstance(base_revision, bool)
            or proposal.get("task_list_id") != task_list_id
            or proposal.get("base_revision") != base_revision
            or proposal.get("base_task_list_digest") != base_digest
            or not isinstance(proposal.get("revision"), int)
            or isinstance(proposal.get("revision"), bool)
            or proposal["revision"] != base_revision + 1
            or proposal.get("source_approval_ref") != source_approval_ref
            or proposal.get("source_approval_event_ref") != source_approval_event_ref):
            raise TaskAreaError("reviewed_task_list_revision_base_or_source_mismatch")
        supersedes = proposal.get("supersedes")
        if (not isinstance(supersedes, dict)
            or set(supersedes) != {"name", "revision", "task_list_digest"}
            or supersedes != {"name": task_list_id, "revision": base_revision,
                              "task_list_digest": base_digest}):
            raise TaskAreaError("reviewed_task_list_revision_supersedes_mismatch")

        original_list = binding["task_list"]
        candidate_list = proposal.get("candidate_task_list")
        if (not isinstance(candidate_list, dict)
            or set(candidate_list) != set(original_list)
            or candidate_list.get("schema") != original_list.get("schema")
            or candidate_list.get("task_list_id") != task_list_id
            or candidate_list.get("revision") != proposal["revision"]
            or any(candidate_list.get(key) != original_list.get(key)
                   for key in ("goal_id", "goal_revision", "base_sha"))
            or any(candidate_list.get(key) != original_list.get(key)
                   for key in set(original_list) - {"revision", "tasks"})):
            raise TaskAreaError("reviewed_task_list_revision_owner_scope_mismatch")
        candidate_digest = proposal.get("candidate_task_list_digest")
        if not isinstance(candidate_digest, str) or digest_json(candidate_list) != candidate_digest:
            raise TaskAreaError("reviewed_task_list_revision_candidate_digest_mismatch")
        rows = candidate_list.get("tasks")
        refs = proposal.get("task_material_refs")
        if not isinstance(rows, list) or not isinstance(refs, dict):
            raise TaskAreaError("reviewed_task_list_revision_material_refs_invalid")
        candidate_rows = {}
        for row in rows:
            if (not isinstance(row, dict) or not isinstance(row.get("node_id"), str)
                or not isinstance(row.get("task_digest"), str)
                or row["node_id"] in candidate_rows
                or not set(row).issubset({"node_id", "task_digest", "status", "depends_on",
                                          "read_set", "write_set", "required_preconditions", "delivery_after", "phase_execution", "resource_requirements"})):
                raise TaskAreaError("reviewed_task_list_revision_task_row_invalid")
            candidate_rows[row["node_id"]] = row
        if set(refs) != set(candidate_rows):
            raise TaskAreaError("reviewed_task_list_revision_material_set_mismatch")
        candidate_tasks, candidate_digests = {}, {}
        for node, row in candidate_rows.items():
            task, _task_bytes = self._read_digest_bound_json(
                refs[node], reason="reviewed_revision_task_material")
            if task.get("node_id") != node:
                raise TaskAreaError("reviewed_revision_task_material_node_mismatch")
            actual_digest = self._reviewed_candidate_task_digest(prior_manifest, task)
            if row["task_digest"] != actual_digest:
                raise TaskAreaError("reviewed_revision_task_material_digest_mismatch")
            for row_key, task_key in (("depends_on", "depends_on"), ("read_set", "read_set"),
                                      ("write_set", "write_set"),
                                      ("required_preconditions", "required_preconditions")):
                if row_key in row and row[row_key] != list(task.get(task_key, [])):
                    raise TaskAreaError("reviewed_revision_task_row_material_mismatch")
            if self._delivery_fields(row) != self._delivery_fields(task):
                raise TaskAreaError("reviewed_revision_task_row_material_mismatch")
            candidate_tasks[node] = task
            candidate_digests[node] = actual_digest
        if any("delivery_after" in task for task in candidate_tasks.values()):
            self._validate_task_graph(list(candidate_tasks.values()))
        classification = self._revision_task_classification(
            prior_manifest, candidate_rows, candidate_tasks, candidate_digests)
        event_ref = {"event_id": event["event_id"],
                     "source_event_digest": self._task_area_event_digest(event)}
        return {
            "schema": "host-reviewed-task-list-pending-revision-projection/v1",
            "proposal_ref": proposal_ref,
            "event_ref": event_ref,
            "source_approval_event_ref": source_approval_event_ref,
            "source_approval_materialization_digest": receipt["outcome"]["materialization_digest"],
            "task_list_id": task_list_id,
            "revision": proposal["revision"],
            "candidate_task_list": candidate_list,
            "candidate_task_list_digest": candidate_digest,
            "classification": classification,
            "prior_manifest_ref": prior_manifest_ref,
        }

    @staticmethod
    def _apply_revision_task_classification(manifest: dict, approved: dict,
                                            classification: dict) -> bool:
        changed = False
        dispositions = classification.get("task_dispositions", {})
        for task in manifest.get("tasks", []):
            node = task["node_id"]
            if node in approved:
                target = ("pending" if dispositions.get(node) == "pending_review"
                          else "approved")
            else:
                target = "deferred" if task.get("status") == "deferred" else "pending"
            if task.get("status") != target:
                task["status"] = target
                changed = True
        return changed

    def _verify_pending_revision_projection(self, manifest: dict,
                                            approved: dict[str, str]) -> None:
        projection = manifest.get("reviewed_task_list_pending_revision")
        expected_fields = {
            "schema", "proposal_ref", "event_ref", "source_approval_event_ref",
            "source_approval_materialization_digest", "task_list_id", "revision",
            "candidate_task_list", "candidate_task_list_digest", "classification",
            "prior_manifest_ref",
        }
        if (not isinstance(projection, dict) or set(projection) != expected_fields
            or projection.get("schema")
                != "host-reviewed-task-list-pending-revision-projection/v1"):
            raise TaskAreaError("reviewed_task_list_pending_revision_projection_invalid")
        prior, _prior_bytes = self._read_archived_prior_manifest(
            projection.get("prior_manifest_ref"))
        if (prior.get("reviewed_task_list_binding")
                != manifest.get("reviewed_task_list_binding")
            or prior.get("reviewed_task_list_draft")
                != manifest.get("reviewed_task_list_draft")):
            raise TaskAreaError("reviewed_task_list_revision_changed_original_binding")
        prior_approved = self._verify_reviewed_binding(prior)
        if prior_approved != approved:
            raise TaskAreaError("reviewed_task_list_revision_original_scope_changed")

        event_ref = projection.get("event_ref")
        if (not isinstance(event_ref, dict) or set(event_ref) != {"event_id", "source_event_digest"}
            or not isinstance(event_ref.get("event_id"), str)
            or not isinstance(event_ref.get("source_event_digest"), str)):
            raise TaskAreaError("reviewed_task_list_revision_event_ref_invalid")
        receipt = self._task_area_event_receipt(
            manifest, event_ref["event_id"], "task_list_revision_pending")
        if receipt is None or receipt["source_event_digest"] != event_ref["source_event_digest"]:
            raise TaskAreaError("reviewed_task_list_revision_event_claim_missing")
        if receipt["settlement"] is not None:
            expected_outcome = {
                "status": "accepted",
                "kind": "task_list_revision_pending",
                "materialization_digest": manifest_digest(manifest),
                "candidate_task_list_digest": projection.get("candidate_task_list_digest"),
            }
            if receipt["outcome"] != expected_outcome:
                raise TaskAreaError("reviewed_task_list_revision_settlement_mismatch")
        source_event = receipt["source"]
        proposal_ref = projection.get("proposal_ref")
        if (source_event.get("payload") != {"proposal_ref": proposal_ref}
            or projection.get("source_approval_event_ref")
                != self._reviewed_approval_event_receipt(prior)["event_ref"]):
            raise TaskAreaError("reviewed_task_list_revision_event_binding_invalid")
        expected_projection = self._build_pending_revision_projection(
            prior, source_event, proposal_ref, projection["prior_manifest_ref"])
        if expected_projection != projection:
            raise TaskAreaError("reviewed_task_list_revision_projection_drift")
        expected_manifest = json.loads(json.dumps(prior, ensure_ascii=False))
        expected_manifest["reviewed_task_list_pending_revision"] = expected_projection
        self._apply_revision_task_classification(
            expected_manifest, approved, expected_projection["classification"])
        self._seal_reviewed_manifest(expected_manifest)
        if expected_manifest != manifest:
            raise TaskAreaError("reviewed_task_list_revision_projection_manifest_drift")

    @staticmethod
    def _scope_verification_evidence(*, source_ref: dict, source_digest: str,
                                     list_digest: str, scope: dict,
                                     approved: dict, current: dict) -> dict:
        checked = {node: TaskAreaController._reviewed_task_digest({}, current[node])
                   for node in approved}
        return {
            "schema": "host-reviewed-task-list-scope-verification/v1",
            "verdict": "verified",
            "checker": "lh-runtime.task-area.reviewed-scope/v1",
            "source_approval_ref": source_ref,
            "source_approval_digest": source_digest,
            "task_list_digest": list_digest,
            "scope_digest": digest_json(scope),
            "owner_scope": {key: scope[key]
                            for key in ("goal_id", "goal_revision", "base_sha")},
            "approved_task_digests": approved,
            "checked_task_digests": checked,
            "checks": [
                "source_bytes_match_digest_and_active_status",
                "task_list_digest_matches_source_and_review",
                "owner_goal_revision_base_match_manifest",
                "approved_digest_map_is_subset_of_reviewed_list",
                "current_approved_task_material_matches_source",
            ],
        }

    def _verify_reviewed_binding(self, manifest: dict) -> dict[str, str] | None:
        binding = manifest.get("reviewed_task_list_binding")
        if binding is None:
            return None
        if not isinstance(binding, dict) or binding.get("schema") != "host-reviewed-task-list-binding/v1":
            raise TaskAreaError("reviewed_task_list_binding_invalid")
        task_list = binding.get("task_list")
        list_digest = binding.get("task_list_digest")
        review = binding.get("review")
        authorization = binding.get("authorization")
        materialization = binding.get("materialization")
        if (not isinstance(task_list, dict) or not isinstance(list_digest, str)
            or digest_json(task_list) != list_digest
            or not isinstance(review, dict) or review.get("status") != "reviewed"
            or review.get("task_list_digest") != list_digest
            or not isinstance(authorization, dict) or authorization.get("status") != "approved"
            or authorization.get("task_list_digest") != list_digest
            or not isinstance(materialization, dict)):
            raise TaskAreaError("reviewed_task_list_authorization_invalid")
        source_ref = binding.get("source_approval_ref")
        if source_ref != authorization.get("source_approval_ref"):
            raise TaskAreaError("reviewed_source_approval_ref_mismatch")
        source, source_digest = self._read_approval_source(source_ref)
        scope = authorization.get("scope")
        if any("phase_execution" in task for task in manifest.get("tasks", [])):
            self.phase_policy_material(manifest)
            if authorization.get("runner_config_ref") != source.get("runner_config_ref"):
                raise TaskAreaError("phase_job_runner_authorization_mismatch")
        approved = source.get("approved_task_digests")
        if (not isinstance(source.get("allowed_actions"), list)
            or not isinstance(source.get("budget"), dict)
            or not isinstance(source.get("stop_conditions"), list)):
            raise TaskAreaError("reviewed_source_authority_terms_missing")
        if (not isinstance(scope, dict) or not isinstance(approved, dict)
            or not approved or scope.get("approved_task_digests") != approved
            or digest_json(scope) != authorization.get("scope_digest")
            or source.get("scope_digest") != authorization.get("scope_digest")
            or authorization.get("source_approval_digest") != source_digest
            or authorization.get("source_task_list_digest") != source.get("task_list_digest")
            or source.get("task_list_digest") != list_digest
            or materialization.get("source_approval_digest") != source_digest
            or materialization.get("source_task_list_digest") != list_digest
            or materialization.get("task_list_digest") != list_digest
            or materialization.get("approved_task_digests") != approved
            or binding.get("authorization_digest") != digest_json(authorization)):
            raise TaskAreaError("reviewed_task_list_scope_or_source_mismatch")
        reviewer = source.get("reviewer", source.get("issuer"))
        if (authorization.get("schema") != "host-task-list-authorization/v1"
            or authorization.get("authorization_id") != source.get("approval_id")
            or authorization.get("approver") != reviewer
            or authorization.get("allowed_actions") != source.get("allowed_actions")
            or authorization.get("budget") != source.get("budget")
            or authorization.get("stop_conditions") != source.get("stop_conditions", [])
            or authorization.get("expires_at") != source.get("expires_at")
            or authorization.get("revoked") is not False
            or binding.get("review", {}).get("reviewer") != reviewer
            or binding.get("review", {}).get("review_id") != source.get("approval_id")):
            raise TaskAreaError("reviewed_task_list_authority_terms_mismatch")
        if (scope.get("goal_id") != manifest.get("goal_id")
            or scope.get("goal_revision") != manifest.get("goal_revision")
            or scope.get("base_sha") != manifest.get("base_sha")
            or task_list.get("goal_id") != manifest.get("goal_id")
            or task_list.get("goal_revision") != manifest.get("goal_revision")
            or task_list.get("base_sha") != manifest.get("base_sha")):
            raise TaskAreaError("reviewed_task_list_owner_scope_mismatch")
        listed = task_list.get("tasks")
        if not isinstance(listed, list):
            raise TaskAreaError("reviewed_task_list_tasks_invalid")
        list_rows = {}
        for row in listed:
            if not isinstance(row, dict) or not isinstance(row.get("node_id"), str):
                raise TaskAreaError("reviewed_task_list_task_invalid")
            node = row["node_id"]
            if node in list_rows or not isinstance(row.get("task_digest"), str):
                raise TaskAreaError("reviewed_task_list_task_not_unique")
            list_rows[node] = row
        for node, digest in approved.items():
            if node not in list_rows or digest != list_rows[node]["task_digest"]:
                raise TaskAreaError("reviewed_task_approval_not_bound_to_list")
        current = {task.get("node_id"): task for task in manifest.get("tasks", [])
                   if isinstance(task, dict) and isinstance(task.get("node_id"), str)}
        if len(current) != len(manifest.get("tasks", [])):
            raise TaskAreaError("reviewed_manifest_task_identity_invalid")
        for node, digest in approved.items():
            task = current.get(node)
            if task is None:
                raise TaskAreaError("reviewed_approved_task_missing")
            if self._reviewed_task_digest(manifest, task) != digest:
                raise TaskAreaError("reviewed_approved_task_material_changed")
            if self._delivery_fields(list_rows[node]) != self._delivery_fields(task):
                raise TaskAreaError("reviewed_approved_task_material_changed")
            if materialization.get("materialized_task_digests", {}).get(node) != digest:
                raise TaskAreaError("reviewed_materialized_task_digest_mismatch")
        for node, row in list_rows.items():
            task = current.get(node)
            if task is not None and self._reviewed_task_digest(manifest, task) != row["task_digest"]:
                if node in approved:
                    raise TaskAreaError("reviewed_approved_task_material_changed")
        evidence = binding.get("scope_verification")
        if evidence is not None:
            expected_evidence = self._scope_verification_evidence(
                source_ref=source_ref, source_digest=source_digest,
                list_digest=list_digest, scope=scope, approved=approved,
                current=current)
            if evidence != expected_evidence:
                raise TaskAreaError("reviewed_task_list_scope_verification_mismatch")
        if manifest.get("reviewed_task_list_pending_revision") is not None:
            self._verify_pending_revision_projection(manifest, approved)
        return approved

    def _set_reviewed_task_classification(self, manifest: dict) -> bool:
        """Keep changed/out-of-list tasks pending; only source-bound digests run."""
        approved = self._verify_reviewed_binding(manifest)
        if approved is None:
            return False
        projection = manifest.get("reviewed_task_list_pending_revision")
        if isinstance(projection, dict):
            changed = self._apply_revision_task_classification(
                manifest, approved, projection["classification"])
            if changed:
                self._seal_reviewed_manifest(manifest)
            return changed
        changed = False
        rows = {row["node_id"]: row for row in
                manifest["reviewed_task_list_binding"]["task_list"]["tasks"]}
        for task in manifest.get("tasks", []):
            node = task["node_id"]
            is_authorized = node in approved and node in rows
            target = "approved" if is_authorized else (
                "deferred" if task.get("status") == "deferred" else "pending")
            if task.get("status") != target:
                task["status"] = target
                changed = True
        if changed:
            # This is an internal, read-only scope recheck, not a human approval.
            if any("delivery_after" in task for task in manifest["tasks"]):
                self._validate_task_graph(manifest["tasks"])
            manifest["plan_verifier"] = {
                "principal": "task-area-reviewed-list-scope-verifier",
                "read_only": True,
                "source_write": False,
                "verdict": "GREEN",
                "manifest_digest": manifest_body_digest(manifest),
            }
            manifest["approval"] = {
                "status": "approved",
                "manifest_digest": manifest_digest(manifest),
            }
        return changed

    @staticmethod
    def _write_manifest_atomic(path: Path, manifest: dict) -> None:
        encoded = (json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                              indent=2) + "\n").encode("utf-8")
        TaskAreaController._write_atomic_bytes(
            path, encoded, manifest_digest(manifest))

    @staticmethod
    def _write_atomic_bytes(path: Path, encoded: bytes,
                            expected_manifest_digest: str) -> None:
        path = Path(path).expanduser()
        if not path.is_absolute():
            raise TaskAreaError("reviewed_manifest_path_must_be_absolute")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                         dir=str(path.parent))
        replacement_may_have_occurred = False
        try:
            make_file_private(fd, temporary)
            with os.fdopen(fd, "wb") as stream:
                fd = -1
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            replacement_may_have_occurred = True
            os.replace(temporary, path)
            verify_file_private(path)
            if read_private_file(path) != encoded:
                raise _ManifestPersistenceOutcomeUnknown(expected_manifest_digest)
            sync_directory(path.parent)
        except _ManifestPersistenceOutcomeUnknown:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise
        except (OSError, PlatformPortUnavailable) as exc:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary)
            except OSError:
                pass
            if replacement_may_have_occurred:
                raise _ManifestPersistenceOutcomeUnknown(expected_manifest_digest) from exc
            raise TaskAreaError("reviewed_manifest_persistence_unavailable") from exc
        except TaskAreaError as exc:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary)
            except OSError:
                pass
            if replacement_may_have_occurred:
                raise _ManifestPersistenceOutcomeUnknown(expected_manifest_digest) from exc
            raise
        except BaseException:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise

    @staticmethod
    def _seal_reviewed_manifest(manifest: dict) -> None:
        if any("delivery_after" in task for task in manifest["tasks"]):
            TaskAreaController._validate_task_graph(manifest["tasks"])
        manifest["plan_verifier"] = {
            "principal": "task-area-reviewed-list-scope-verifier",
            "read_only": True,
            "source_write": False,
            "verdict": "GREEN",
            "manifest_digest": manifest_body_digest(manifest),
        }
        manifest["approval"] = {
            "status": "approved",
            "manifest_digest": manifest_digest(manifest),
        }

    def _reconcile_materialized_approval_event(self, manifest: dict, event: dict,
                                               manifest_path: Path) -> dict:
        binding = manifest.get("reviewed_task_list_binding")
        if not isinstance(binding, dict):
            raise TaskAreaError("reviewed_materialization_binding_missing")
        approved = self._verify_reviewed_binding(manifest)
        if approved is None:
            raise TaskAreaError("reviewed_materialization_binding_missing")
        draft = manifest.get("reviewed_task_list_draft")
        materialization = binding.get("materialization", {})
        source_ref = binding.get("source_approval_ref")
        source_digest = materialization.get("source_approval_digest")
        task_list_digest = binding.get("task_list_digest")
        payload = event.get("payload", {})
        if (event.get("event_type") != "task_list_approval_updated"
            or materialization.get("source_event_id") != event.get("event_id")
            or binding.get("review", {}).get("approval_event_id") != event.get("event_id")
            or (draft is not None and draft.get("source_approval_ref") != source_ref)
            or payload.get("source_approval_ref", source_ref) != source_ref
            or payload.get("source_approval_digest") != source_digest
            or payload.get("task_list_digest") != task_list_digest
            or (payload.get("reviewer") is not None
                and payload.get("reviewer") != binding["review"].get("reviewer"))
            or (payload.get("materialization_digest") is not None
                and payload["materialization_digest"]
                    != materialization.get("approval_event_input_digest"))):
            raise TaskAreaError("reviewed_approval_event_reconciliation_mismatch")
        if draft is not None:
            if (draft.get("task_list_digest") != task_list_digest
                or draft.get("source_approval_ref") != source_ref):
                raise TaskAreaError("reviewed_draft_materialization_binding_mismatch")
        for task in manifest.get("tasks", []):
            target = "approved" if task.get("node_id") in approved else (
                "deferred" if task.get("status") == "deferred" else "pending")
            if task.get("status") != target:
                raise TaskAreaError("reviewed_materialization_classification_drift")
        digest = manifest_digest(manifest)
        if (manifest.get("approval") != {"status": "approved", "manifest_digest": digest}
            or manifest.get("plan_verifier", {}).get("verdict") != "GREEN"
            or manifest["plan_verifier"].get("read_only") is not True
            or manifest["plan_verifier"].get("source_write") is not False
            or manifest["plan_verifier"].get("manifest_digest") != manifest_body_digest(manifest)):
            raise TaskAreaError("reviewed_materialization_seal_invalid")
        try:
            readback = json.loads(read_private_file(Path(manifest_path)).decode("utf-8"))
        except (OSError, UnicodeError, ValueError, PlatformPortUnavailable) as exc:
            raise _ManifestPersistenceOutcomeUnknown(digest) from exc
        if not isinstance(readback, dict) or manifest_digest(readback) != digest:
            raise _ManifestPersistenceOutcomeUnknown(digest)
        try:
            verify_file_private(Path(manifest_path))
            sync_directory(Path(manifest_path).parent)
        except (OSError, PlatformPortUnavailable, TaskAreaError) as exc:
            raise _ManifestPersistenceOutcomeUnknown(digest) from exc
        return readback

    def _materialize_existing_binding_event(self, manifest: dict, event: dict,
                                            manifest_path: Path) -> dict:
        binding = manifest.get("reviewed_task_list_binding")
        if not isinstance(binding, dict):
            raise TaskAreaError("reviewed_task_list_binding_missing")
        materialization = binding.get("materialization")
        if not isinstance(materialization, dict):
            raise TaskAreaError("reviewed_task_list_materialization_missing")
        existing_event_id = materialization.get("source_event_id")
        if existing_event_id is not None:
            return self._reconcile_materialized_approval_event(manifest, event, manifest_path)
        self._verify_reviewed_binding(manifest)
        payload = event.get("payload", {})
        input_digest = manifest_digest(manifest)
        if (payload.get("source_approval_digest")
            != materialization.get("source_approval_digest")
            or payload.get("task_list_digest") != binding.get("task_list_digest")
            or (payload.get("materialization_digest") is not None
                and payload["materialization_digest"] != input_digest)):
            raise TaskAreaError("reviewed_approval_event_binding_mismatch")
        materialization["source_event_id"] = event["event_id"]
        materialization["approval_event_input_digest"] = input_digest
        binding["review"]["approval_event_id"] = event["event_id"]
        source, source_digest = self._read_approval_source(binding["source_approval_ref"])
        scope = binding["authorization"]["scope"]
        approved = source["approved_task_digests"]
        current = {task["node_id"]: task for task in manifest["tasks"]}
        binding["scope_verification"] = self._scope_verification_evidence(
            source_ref=binding["source_approval_ref"], source_digest=source_digest,
            list_digest=binding["task_list_digest"], scope=scope,
            approved=approved, current=current)
        self._set_reviewed_task_classification(manifest)
        self._seal_reviewed_manifest(manifest)
        self._verify_reviewed_binding(manifest)
        self._write_manifest_atomic(manifest_path, manifest)
        return manifest

    def _materialize_approval_event(self, manifest: dict, event: dict,
                                   manifest_path: Path) -> dict:
        draft = manifest.get("reviewed_task_list_draft")
        if (not isinstance(draft, dict)
            or draft.get("schema") != "host-reviewed-task-list-draft/v1"
            or draft.get("status") != "draft"):
            raise TaskAreaError("reviewed_task_list_draft_missing")
        if manifest.get("reviewed_task_list_binding") is not None:
            return self._reconcile_materialized_approval_event(manifest, event, manifest_path)
        materialized = self._prepare_approval_materialization(manifest, event)
        self._write_manifest_atomic(manifest_path, materialized)
        return materialized

    def _prepare_approval_materialization(self, manifest: dict, event: dict) -> dict:
        """Validate and derive admission fields without persisting authority."""
        draft = manifest.get("reviewed_task_list_draft")
        if (not isinstance(draft, dict)
            or draft.get("schema") != "host-reviewed-task-list-draft/v1"
            or draft.get("status") != "draft"):
            raise TaskAreaError("reviewed_task_list_draft_missing")
        if manifest.get("reviewed_task_list_binding") is not None:
            raise TaskAreaError("reviewed_draft_has_preexisting_binding")
        approval = manifest.get("approval")
        if isinstance(approval, dict) and approval.get("status") == "approved":
            raise TaskAreaError("reviewed_draft_has_preexisting_approval")
        if any(task.get("status") == "approved" for task in manifest.get("tasks", [])):
            raise TaskAreaError("reviewed_draft_has_preapproved_task")
        task_list = draft.get("task_list")
        list_digest = draft.get("task_list_digest")
        if (not isinstance(task_list, dict) or not isinstance(list_digest, str)
            or digest_json(task_list) != list_digest
            or task_list.get("goal_id") != manifest.get("goal_id")
            or task_list.get("goal_revision") != manifest.get("goal_revision")
            or task_list.get("base_sha") != manifest.get("base_sha")):
            raise TaskAreaError("reviewed_task_list_draft_digest_or_scope_mismatch")
        source_ref = draft.get("source_approval_ref")
        payload = event.get("payload", {})
        if (payload.get("source_approval_ref") != source_ref
            or payload.get("task_list_digest") != list_digest):
            raise TaskAreaError("reviewed_approval_event_source_mismatch")
        source, source_digest = self._read_approval_source(source_ref)
        if (payload.get("source_approval_digest") != source_digest
            or source.get("task_list_digest") != list_digest
            or (payload.get("materialization_digest") is not None
                and payload["materialization_digest"] != manifest_digest(manifest))
            or source.get("approval_event_id") not in {None, event.get("event_id")}
            or (source.get("reviewer") is not None
                and payload.get("reviewer") != source.get("reviewer"))):
            raise TaskAreaError("reviewed_approval_event_authority_mismatch")
        list_rows = task_list.get("tasks")
        if not isinstance(list_rows, list):
            raise TaskAreaError("reviewed_task_list_tasks_invalid")
        current = {task.get("node_id"): task for task in manifest.get("tasks", [])}
        row_by_node = {}
        for row in list_rows:
            if (not isinstance(row, dict) or not isinstance(row.get("node_id"), str)
                or row.get("status") not in {"draft", "pending", "reviewed", "approved"}
                or row["node_id"] in row_by_node):
                raise TaskAreaError("reviewed_task_list_task_invalid")
            row_by_node[row["node_id"]] = row
            task = current.get(row["node_id"])
            if task is None or self._reviewed_task_digest(manifest, task) != row.get("task_digest"):
                raise TaskAreaError("reviewed_draft_task_material_mismatch")
            if self._delivery_fields(row) != self._delivery_fields(task):
                raise TaskAreaError("reviewed_draft_task_material_mismatch")
        if len(row_by_node) != len(current):
            raise TaskAreaError("reviewed_draft_task_set_mismatch")
        approved = source.get("approved_task_digests")
        if not isinstance(approved, dict) or not approved:
            raise TaskAreaError("reviewed_source_approved_tasks_missing")
        for node, digest in approved.items():
            if node not in row_by_node or row_by_node[node].get("task_digest") != digest:
                raise TaskAreaError("reviewed_source_task_scope_mismatch")
        scope = {
            "goal_id": manifest["goal_id"],
            "goal_revision": manifest["goal_revision"],
            "base_sha": manifest["base_sha"],
            "approved_task_digests": approved,
        }
        if source.get("scope_digest") != digest_json(scope):
            raise TaskAreaError("reviewed_source_owner_scope_mismatch")
        materialized = json.loads(json.dumps(manifest, ensure_ascii=False))
        for task in materialized["tasks"]:
            task["status"] = "approved" if task["node_id"] in approved else "pending"
        materialized_tasks = {
            task["node_id"]: self._reviewed_task_digest(materialized, task)
            for task in materialized["tasks"]
        }
        binding = {
            "schema": "host-reviewed-task-list-binding/v1",
            "task_list": task_list,
            "task_list_digest": list_digest,
            "source_approval_ref": source_ref,
            "review": {
                "schema": "host-task-list-review/v1",
                "status": "reviewed",
                "review_id": source.get("approval_id"),
                "reviewer": source.get("reviewer", source.get("issuer")),
                "task_list_digest": list_digest,
                "approval_event_id": event["event_id"],
            },
            "authorization": {
                "schema": "host-task-list-authorization/v1",
                "status": "approved",
                "authorization_id": source.get("approval_id"),
                "approver": source.get("reviewer", source.get("issuer")),
                "task_list_digest": list_digest,
                "scope": scope,
                "scope_digest": digest_json(scope),
                "source_approval_ref": source_ref,
                "source_approval_digest": source_digest,
                "source_task_list_digest": list_digest,
                "allowed_actions": source.get("allowed_actions", []),
                "budget": source.get("budget", {}),
                "stop_conditions": source.get("stop_conditions", []),
                "expires_at": source["expires_at"],
                "revoked": False,
            },
            "materialization": {
                "source_event_id": event["event_id"],
                "approval_event_input_digest": manifest_digest(manifest),
                "source_approval_digest": source_digest,
                "source_task_list_digest": list_digest,
                "task_list_digest": list_digest,
                "approved_task_digests": approved,
                "materialized_task_digests": materialized_tasks,
                "task_classification": {
                    node: "approved" if node in approved else "pending"
                    for node in materialized_tasks
                },
            },
        }
        if any("phase_execution" in task for task in materialized["tasks"]):
            binding["authorization"]["runner_config_ref"] = source.get("runner_config_ref")
        binding["authorization_digest"] = digest_json(binding["authorization"])
        materialized["reviewed_task_list_binding"] = binding
        # Re-run the independent binding/scope verifier over the materialized
        # source bytes before sealing the existing task-area admission fields.
        self._verify_reviewed_binding(materialized)
        binding["scope_verification"] = self._scope_verification_evidence(
            source_ref=source_ref, source_digest=source_digest,
            list_digest=list_digest, scope=scope, approved=approved,
            current={task["node_id"]: task for task in materialized["tasks"]})
        self._seal_reviewed_manifest(materialized)
        self._verify_reviewed_binding(materialized)
        return materialized

    def prepare_reviewed_approval(self, manifest: dict) -> tuple[dict, dict]:
        """Prepare an intake event under the caller's task-area lock.

        The upstream reference supplies trust; its digest binds bytes, not an
        issuer identity. Only the original consumer persists the derived manifest.
        """
        binding = manifest.get("reviewed_task_list_binding")
        draft = manifest.get("reviewed_task_list_draft")
        route = binding if binding is not None else draft
        if not isinstance(route, dict):
            raise TaskAreaError("reviewed_task_list_draft_missing")
        source_ref = route.get("source_approval_ref")
        source, source_digest = self._read_approval_source(source_ref)
        event_id = source.get("approval_event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise TaskAreaError("reviewed_source_approval_event_id_missing")
        if event_id != event_id.strip():
            raise TaskAreaError("reviewed_source_approval_event_id_invalid")
        payload = {
            "reviewer": source.get("reviewer", source.get("issuer")),
            "source_approval_ref": source_ref,
            "source_approval_digest": source_digest,
            "task_list_digest": route.get("task_list_digest"),
        }
        event = {"event_id": event_id, "parent_goal_id": manifest["goal_id"],
                 "event_type": "task_list_approval_updated", "payload": payload}
        prior = next((row for row in self.store.events(manifest["goal_id"])
                      if row["event_id"] == event_id), None)
        if prior is not None:
            if (prior["event_type"] != event["event_type"]
                or prior["work_unit_id"] is not None or prior["run_id"] is not None
                or any(prior["payload"].get(key) != value for key, value in payload.items())):
                raise TaskAreaError("reviewed_approval_event_binding_mismatch")
            # Retain the first input digest and all original payload bytes/fields;
            # the current manifest digest may have changed through materialization.
            event = prior
        if binding is None:
            admission = self._prepare_approval_materialization(manifest, event)
        else:
            self._verify_reviewed_binding(manifest)
            materialization = binding["materialization"]
            if (materialization.get("source_event_id") != event_id
                or binding["review"].get("approval_event_id") != event_id
                or (draft is not None and (
                    not isinstance(draft, dict)
                    or draft.get("source_approval_ref") != source_ref
                    or draft.get("task_list_digest") != binding["task_list_digest"]))
                or (event["payload"].get("materialization_digest") is not None
                    and event["payload"]["materialization_digest"]
                        != materialization.get("approval_event_input_digest"))):
                raise TaskAreaError("reviewed_approval_event_reconciliation_mismatch")
            if prior is None:
                raise TaskAreaError("reviewed_task_list_approval_event_missing")
            admission = json.loads(json.dumps(manifest, ensure_ascii=False))
        return event, admission

    def merge_prerequisite_intent(self, manifest: dict, ref: dict) -> dict:
        """Read the approved immutable intent; SCM observations are separate."""
        if (ref.get("kind") != "scm_merge_ci"
            or ref not in manifest.get("source_prerequisites", [])
            or not any(task.get("status") == "approved"
                       and ref in task.get("required_preconditions", [])
                       for task in manifest.get("tasks", []))):
            raise TaskAreaError("merge_prerequisite_not_bound")
        try:
            raw = Path(ref["path"]).read_bytes()
            intent = json.loads(raw)
        except (KeyError, OSError, TypeError, ValueError) as exc:
            raise TaskAreaError("merge_prerequisite_intent_unreadable") from exc
        if "sha256:" + hashlib.sha256(raw).hexdigest() != ref.get("content_digest"):
            raise TaskAreaError("merge_prerequisite_intent_drift")
        if not isinstance(intent, dict) or set(intent) != {
            "repository_id", "pr_number", "expected_head_sha", "required_checks"}:
            raise TaskAreaError("merge_prerequisite_intent_invalid")
        repo, pr, head, checks = (intent[k] for k in
                                  ("repository_id", "pr_number", "expected_head_sha", "required_checks"))
        if (not isinstance(repo, str) or len(repo.split("/")) != 2
            or any(not part or part in {".", ".."}
                   or any(not (c.isascii() and (c.isalnum() or c in "._-")) for c in part)
                   for part in repo.split("/"))
            or isinstance(pr, bool) or not isinstance(pr, int) or pr < 1
            or not isinstance(head, str) or len(head) != 40
            or any(c not in "0123456789abcdef" for c in head)
            or not isinstance(checks, list) or not 1 <= len(checks) <= 20
            or any(not isinstance(c, str) or not c.strip() for c in checks)
            or len(checks) != len(set(checks))):
            raise TaskAreaError("merge_prerequisite_intent_invalid")
        return intent

    @staticmethod
    def merge_prerequisite_event_id(manifest: dict, ref: dict) -> str:
        binding = manifest["reviewed_task_list_binding"]
        return "task-area-merge:" + digest_json({
            "goal_id": manifest["goal_id"], "goal_revision": manifest["goal_revision"],
            "task_list_digest": binding["task_list_digest"],
            "source_approval_digest": binding["authorization"]["source_approval_digest"],
            "prerequisite_ref": ref})

    def publish_merge_prerequisite(self, manifest: dict, *, manifest_path: Path,
                                   ref: dict, merge: dict, ci: dict) -> dict:
        """Recheck under the existing short lock after external read-only IO."""
        lock = FileLockSchedulerPort(filename="task-area.lock")
        handle = lock.acquire(self.store.root)
        if handle is None:
            return {"status": "waiting", "reason": "task_area_controller_busy", "appended": False}
        try:
            if json.loads(read_private_file(manifest_path)) != manifest:
                raise TaskAreaError("merge_prerequisite_manifest_changed")
            self._verify_reviewed_binding(manifest)
            self._validate(manifest)
            if not self._reviewed_approval_settled(manifest):
                raise TaskAreaError("reviewed_task_list_approval_event_pending")
            binding = manifest["reviewed_task_list_binding"]
            event_id = self.merge_prerequisite_event_id(manifest, ref)
            payload = {"status": "satisfied", "prerequisite_ref": ref,
                       "merge_readback": {"schema": "lh-task-area-merge-readback/v1",
                           "task_list_digest": binding["task_list_digest"],
                           "source_approval_digest": binding["authorization"]["source_approval_digest"],
                           "merge": merge, "ci": ci}}
            self._validate_prerequisite_event(manifest, {"event_id": event_id, "payload": payload})
            return self.store.record_task_area_prerequisite(
                event_id=event_id, parent_goal_id=manifest["goal_id"],
                goal_revision=manifest["goal_revision"], payload=payload)
        finally:
            lock.release(handle)

    def recovery_environment_context(self, manifest: dict, task: dict) -> dict | None:
        """Read the sealed checks intent and the current accepted, settled RED."""
        from .work_unit_completion import candidate_inventory
        contract = task.get("completion_contract", {})
        ref = contract.get("recovery_environment_ref")
        if ref is None or task.get("status") != "approved":
            return None
        self._verify_reviewed_binding(manifest)
        approval_receipt = self._reviewed_approval_event_receipt(manifest)
        if approval_receipt is None:
            raise TaskAreaError("reviewed_task_list_approval_event_pending")
        approved = approval_receipt["outcome"]["materialization_digest"]
        if approved != manifest_digest(manifest):
            raise TaskAreaError("recovery_environment_authority_drift")
        if self.approved_digest is None:
            # Re-entry reads the original claim/settlement-bound approval;
            # absent or different approval evidence never supplies authority.
            self.approved_digest = approved
        self._validate(manifest)
        envelope = task["envelope"]
        dispatch = self.store.get_dispatch_consumption(envelope["dispatch_key"])
        if dispatch is None:
            return None
        if self.store.get_parent_goal(manifest["goal_id"])["state"] != "active":
            raise TaskAreaError("parent_not_active")
        run = self.store.get_run(dispatch["run_id"])
        attempt = self.store.get_attempt(run["run_id"], run["attempts"])
        unit = self.store.get_work_unit(dispatch["work_unit_id"])
        if (dispatch["attempt"] != run["attempts"] or attempt["fence"] != run["fence"]
            or dispatch["envelope_digest"] != envelope["envelope_digest"]
            or run["work_unit_id"] != dispatch["work_unit_id"]
            or unit.get("run_id") != run["run_id"]
            or unit["node_id"] != task["node_id"]
            or any(dispatch.get(k) != envelope.get(k) for k in ("goal_id", "goal_revision", "node_id"))):
            raise TaskAreaError("recovery_environment_executor_not_accepted")
        receipt = dispatch["receipt"]
        if (receipt.get("receipt_digest") != dispatch["receipt_digest"]
            or receipt.get("receipt_digest") != digest_json({k: v for k, v in receipt.items()
                                                           if k != "receipt_digest"})
            or receipt.get("fence") != attempt["fence"] or receipt.get("attempt") != run["attempts"]
            or receipt.get("packet_digest") != envelope["packet_digest"]):
            raise TaskAreaError("recovery_environment_executor_not_accepted")
        row = self.store.get_completion_phase(run["run_id"], run["attempts"], run["fence"], "checks")
        if (run["state"] == "queued" and attempt["state"] == unit["state"] == "ready"
            and receipt.get("executor_status") == receipt.get("queue_state") == receipt.get("attempt_state") == "ready"
            and row is None):
            return None  # The original retry has queued A2/A3; coding has not run yet.
        if receipt.get("executor_status") != "accepted":
            raise TaskAreaError("recovery_environment_executor_not_accepted")
        if run["state"] == unit["state"] == attempt["state"] == "integrated":
            return None
        if row is None or row["state"] != "settled" or row["evidence"].get("verdict") != "RED":
            return None
        red = row["evidence"]
        if (row["phase_key"] != digest_json([run["run_id"], run["attempts"], run["fence"], "checks"])
            or red.get("phase_key") != row["phase_key"]
            or red.get("receipt_digest") != digest_json({k: v for k, v in red.items()
                                                       if k != "receipt_digest"})):
            raise TaskAreaError("recovery_environment_red_receipt_mismatch")
        request = {"goal_id": manifest["goal_id"], "goal_revision": manifest["goal_revision"],
            "node_id": task["node_id"], "work_unit_id": dispatch["work_unit_id"],
            "run_id": run["run_id"], "attempt": run["attempts"], "fence": run["fence"],
            "phase": "checks", "packet_digest": envelope["packet_digest"],
            "envelope_digest": envelope["envelope_digest"],
            "candidate_digest": red["candidate_digest"], "authority_digest": manifest_digest(manifest),
            "input_evidence_digest": digest_json(red), "recovery_environment_ref": ref,
            "test_refs": [{"id": c["id"], "command_digest": digest_json(c)} for c in contract["checks"]]}
        with self.store._connect() as conn:
            sealed_contract, intent = self.store._recovery_environment_packet_conn(conn, request)
        if sealed_contract != contract:
            raise TaskAreaError("recovery_environment_commands_drift")
        if (set(intent) != {"schema", "target_phase", "check_ref", "probe_command"}
            or intent["check_ref"] not in request["test_refs"]):
            raise TaskAreaError("recovery_environment_intent_invalid")
        command = intent["probe_command"]
        if (not isinstance(command, dict) or set(command) != {"argv", "cwd", "timeout_seconds", "expect_exit"}
            or not isinstance(command["argv"], list) or not command["argv"]
            or any(not isinstance(arg, str) or not arg for arg in command["argv"])
            or not isinstance(command["cwd"], str) or not command["cwd"]
            or isinstance(command["timeout_seconds"], bool)
            or not isinstance(command["timeout_seconds"], (int, float))
            or not 0 < command["timeout_seconds"] < float("inf")
            or type(command["expect_exit"]) is not int or command["expect_exit"] != 0):
            raise TaskAreaError("recovery_environment_intent_invalid")
        incident = self.store.recovery_incident(request)
        if incident["failure_count"] < 3:
            return None
        root = str(Path(attempt.get("workspace_ref") or envelope["worktree"]).resolve())
        if candidate_inventory(root)["candidate_digest"] != red["candidate_digest"]:
            raise TaskAreaError("recovery_environment_candidate_drift")
        records = [r for r in self.store.recovery_requests(work_unit_id=dispatch["work_unit_id"])
                   if r["request"]["run_id"] == run["run_id"]]
        originals = [r for r in records if r["request"]["attempt"] == run["attempts"]
                     and r["request"]["phase"] == "checks" and not r["request"].get("environment_event_ref")]
        if len(originals) > 1:
            raise TaskAreaError("recovery_environment_audit_ref_mismatch")
        return {"request": request, "intent": intent, "worktree": root, "envelope": envelope,
            "original_red_ref": {"phase_key": row["phase_key"], "receipt_digest": red["receipt_digest"]},
            "commands_digest": digest_json(contract["checks"]), "incident": incident,
            "original": originals[0] if originals else None, "records": records}

    @staticmethod
    def recovery_environment_event_id(payload: dict) -> str:
        return "task-area-environment:" + digest_json({k: payload[k] for k in (
            "run_id", "attempt", "fence", "original_red_ref", "intent_digest", "observation_kind",
            "environment_digest")})

    def publish_recovery_environment(self, manifest: dict, *, manifest_path: Path, payload: dict) -> dict:
        lock = FileLockSchedulerPort(filename="task-area.lock")
        handle = lock.acquire(self.store.root)
        if handle is None:
            return {"status": "waiting", "reason": "task_area_controller_busy", "appended": False}
        try:
            if json.loads(read_private_file(manifest_path)) != manifest:
                raise TaskAreaError("recovery_environment_authority_drift")
            event_id = self.recovery_environment_event_id(payload)
            # Retain the identical producer validation, while the generic event
            # entry remains owned by process_task_area_events after append/claim.
            self._validate_recovery_environment_event(manifest, {"event_id": event_id, "payload": payload})
            return self.store.record_task_area_prerequisite(event_id=event_id,
                parent_goal_id=manifest["goal_id"], goal_revision=manifest["goal_revision"], payload=payload)
        finally:
            lock.release(handle)

    def _validate_recovery_environment_event(self, manifest: dict, event: dict) -> dict:
        from .runner_adapter import resolve_task_area_execution_binding
        payload = event["payload"]
        task = next((t for t in manifest["tasks"] if t["node_id"] == payload.get("node_id")), None)
        context = self.recovery_environment_context(manifest, task) if task else None
        if context is None:
            raise TaskAreaError("recovery_environment_red_receipt_mismatch")
        request = context["request"]
        for field, expected, reason in (
            ("original_red_ref", context["original_red_ref"], "red_receipt_mismatch"),
            ("authority_digest", request["authority_digest"], "authority_drift"),
            ("commands_digest", context["commands_digest"], "commands_drift"),
            ("candidate_digest", request["candidate_digest"], "candidate_drift")):
            if payload.get(field) != expected:
                raise TaskAreaError("recovery_environment_" + reason)
        if (any(payload.get(k) != request[k] for k in ("run_id", "attempt", "fence"))
            or payload.get("intent_digest") != request["recovery_environment_ref"]["digest"]
            or payload.get("observation_kind") not in {"baseline", "changed"}):
            raise TaskAreaError("recovery_environment_event_binding_invalid")
        proof = payload.get("probe_evidence", {})
        frame, environment = proof.get("request", {}), proof.get("environment")
        binding = resolve_task_area_execution_binding(manifest)
        capability = binding.runner.binding("checks", work_unit_id=request["work_unit_id"],
                                            attempt_id=str(request["attempt"]))
        command = context["intent"]["probe_command"]
        replacements = {"${WORKTREE}": context["worktree"],
                        "${WAVE_BASE_SHA}": context["envelope"]["wave_base_sha"]}
        def expand(value):
            for key, replacement in replacements.items():
                value = value.replace(key, replacement)
            return value
        expected_argv = [expand(arg) for arg in command["argv"]]
        execution_context = {"phase": "checks", "execution_binding_digest": binding.digest,
            "capability": capability, "host_binding_digest": binding.host["binding_digest"],
            "packet_digest": request["packet_digest"], "command": expected_argv,
            "input_digest": digest_json(frame)}
        if (proof.get("capability_binding") != capability
            or proof.get("execution_context_digest") != digest_json(execution_context)
            or proof.get("argv") != expected_argv
            or proof.get("cwd") != str(Path(expand(command["cwd"])).resolve())
            or not self.store._recovery_valid_phase_binding(request, proof, role="checks",
                capability_name="checks", principal=capability["identity"]["principal"])
            or any(frame.get(k) != request[k] for k in (
                "goal_id", "goal_revision", "node_id", "work_unit_id", "run_id", "attempt", "fence",
                "packet_digest", "envelope_digest"))
            or frame.get("worktree") != context["worktree"] or frame.get("read_only_required") is not True
            or proof.get("probe_command_digest") != digest_json(context["intent"]["probe_command"])
            or proof.get("exit_code") != 0
            or not isinstance(environment, dict) or not environment
            or payload.get("environment_digest") != digest_json(environment)
            or proof.get("stdout_digest") != digest_json(proof.get("readback"))
            or proof.get("readback", {}).get("environment") != environment):
            raise TaskAreaError("recovery_environment_probe_failed")
        reservation_id = proof.get("budget_reservation_id")
        events = self.store.events(manifest["goal_id"])
        reservations = [e["payload"] for e in events if e["event_type"] == "task_area_budget_reserved"
                        and e["payload"].get("reservation_id") == reservation_id]
        settlements = [e["payload"] for e in events
                       if e["event_type"] == "task_area_budget_settled"
                       and e["payload"].get("reservation_id") == reservation_id]
        if (len(reservations) != 1 or len(settlements) != 1
            or reservations[0].get("recovery_environment_probe") != request
            or settlements[0].get("reservation_digest") != digest_json(reservations[0])
            or settlements[0].get("invocations") != 0 or settlements[0].get("outcome") != "completed"):
            raise TaskAreaError("recovery_environment_probe_unknown")
        if payload["observation_kind"] == "changed":
            original = context["original"]
            if original is None or original.get("status") != "audit_recorded":
                raise TaskAreaError("recovery_environment_audit_ref_mismatch")
            audit = self.store.recovery_environment_audit(original["request_id"])
            if payload.get("audit_ref") != {k: audit[k] for k in ("request_id", "request_digest", "audit_digest")}:
                raise TaskAreaError("recovery_environment_audit_ref_mismatch")
            with self.store._connect() as conn:
                baseline = self.store._recovery_environment_event_conn(conn, payload.get("baseline_event_ref"))
            prior = baseline["payload"]
            if (prior.get("observation_kind") != "baseline" or any(prior.get(k) != payload.get(k) for k in (
                "run_id", "attempt", "fence", "original_red_ref", "intent_digest", "candidate_digest",
                "authority_digest", "commands_digest"))):
                raise TaskAreaError("recovery_environment_event_binding_invalid")
            if prior["environment_digest"] == payload["environment_digest"]:
                raise TaskAreaError("recovery_environment_unchanged")
        elif payload.get("audit_ref") is not None or payload.get("baseline_event_ref") is not None:
            raise TaskAreaError("recovery_environment_event_binding_invalid")
        if event.get("event_id") != self.recovery_environment_event_id(payload):
            raise TaskAreaError("recovery_environment_event_binding_invalid")
        return {"status": "accepted", "kind": "recovery_environment_" + payload["observation_kind"],
                "payload_digest": digest_json(payload), "environment_digest": payload["environment_digest"]}

    def _validate_prerequisite_event(self, manifest: dict, event: dict) -> dict:
        payload = event.get("payload", {})
        if payload.get("observation_kind") in {"baseline", "changed"}:
            return self._validate_recovery_environment_event(manifest, event)
        ref = payload.get("prerequisite_ref")
        if payload.get("status") != "satisfied" or not isinstance(ref, dict):
            raise TaskAreaError("task_area_prerequisite_event_invalid")
        if any(key in payload for key in ("integrated_run", "live_accepted", "live_acceptance")):
            raise TaskAreaError("task_area_prerequisite_cannot_claim_live_or_integrated")
        known = {item.get("prerequisite_id"): item
                 for item in manifest.get("source_prerequisites", [])
                 if isinstance(item, dict)}
        expected = known.get(ref.get("prerequisite_id"))
        if expected != ref:
            raise TaskAreaError("task_area_prerequisite_not_in_bound_source")
        required = [item for task in manifest.get("tasks", [])
                    for item in task.get("required_preconditions", [])]
        if ref not in required:
            raise TaskAreaError("task_area_prerequisite_not_required")
        try:
            raw = Path(ref["path"]).read_bytes()
        except (KeyError, OSError, TypeError) as exc:
            raise TaskAreaError("task_area_prerequisite_source_missing") from exc
        if "sha256:" + hashlib.sha256(raw).hexdigest() != ref.get("content_digest"):
            raise TaskAreaError("task_area_prerequisite_source_digest_mismatch")
        if ref.get("kind") == "source_result":
            from .source_result import validate_event
            try:
                validate_event(self, manifest, event)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                raise TaskAreaError(str(exc)) from exc
        if ref.get("kind") == "scm_merge_ci":
            self._verify_reviewed_binding(manifest)
            intent = self.merge_prerequisite_intent(manifest, ref)
            binding = manifest["reviewed_task_list_binding"]
            proof = payload.get("merge_readback")
            if (not isinstance(proof, dict) or proof.get("schema") != "lh-task-area-merge-readback/v1"
                or proof.get("task_list_digest") != binding["task_list_digest"]
                or proof.get("source_approval_digest") != binding["authorization"]["source_approval_digest"]
                or event.get("event_id") != self.merge_prerequisite_event_id(manifest, ref)):
                raise TaskAreaError("merge_prerequisite_readback_binding_invalid")
            merge, ci = proof.get("merge"), proof.get("ci")
            if (not isinstance(merge, dict) or merge.get("merged") is not True
                or merge.get("repository_id") != intent["repository_id"]
                or merge.get("pr_number") != intent["pr_number"]
                or merge.get("head_sha") != intent["expected_head_sha"]
                or not isinstance(merge.get("merge_sha"), str) or len(merge["merge_sha"]) != 40
                or any(c not in "0123456789abcdef" for c in merge["merge_sha"])
                or not isinstance(ci, dict) or ci.get("repository_id") != intent["repository_id"]
                or ci.get("sha") != merge["merge_sha"]
                or ci.get("status") != "completed" or ci.get("conclusion") != "success"):
                raise TaskAreaError("merge_prerequisite_not_satisfied")
            checks = ci.get("checks")
            if (not isinstance(checks, list) or not checks
                or any(not isinstance(c, dict) or c.get("status") != "COMPLETED"
                       or c.get("conclusion") not in {"SUCCESS", "SUCCEEDED", "PASS", "PASSED"}
                       for c in checks)
                or not set(intent["required_checks"]).issubset({c.get("name") for c in checks})):
                raise TaskAreaError("merge_prerequisite_checks_not_satisfied")
        return {"status": "accepted", "kind": "prerequisite_satisfied",
                "prerequisite_id": ref["prerequisite_id"],
                "content_digest": ref["content_digest"]}

    def _apply_pending_revision_event(self, manifest: dict, event: dict,
                                      manifest_path: Path) -> dict:
        projection = manifest.get("reviewed_task_list_pending_revision")
        event_id = event.get("event_id")
        if (isinstance(projection, dict)
            and projection.get("event_ref", {}).get("event_id") == event_id):
            self._verify_reviewed_binding(manifest)
            if event.get("payload") != {"proposal_ref": projection.get("proposal_ref")}:
                raise TaskAreaError("reviewed_task_list_revision_event_binding_invalid")
            return {
                "status": "accepted", "manifest": manifest,
                "outcome": {
                    "status": "accepted", "kind": "task_list_revision_pending",
                    "materialization_digest": manifest_digest(manifest),
                    "candidate_task_list_digest": projection["candidate_task_list_digest"],
                },
            }

        self._verify_reviewed_binding(manifest)
        if isinstance(projection, dict):
            prior_event_ref = projection.get("event_ref", {})
            prior_receipt = self._task_area_event_receipt(
                manifest, prior_event_ref.get("event_id", ""),
                "task_list_revision_pending")
            if prior_receipt is None or prior_receipt["settlement"] is None:
                return {"status": "waiting", "reason":
                        "reviewed_task_list_revision_settlement_pending"}
            if prior_receipt["outcome"].get("status") != "accepted":
                raise TaskAreaError("reviewed_task_list_prior_revision_not_accepted")
        if self._reviewed_approval_event_receipt(manifest) is None:
            return {"status": "waiting", "reason":
                    "reviewed_task_list_approval_event_pending"}

        try:
            prior_bytes = read_private_file(manifest_path)
            readback = json.loads(prior_bytes.decode("utf-8"))
        except (OSError, UnicodeError, ValueError, PlatformPortUnavailable) as exc:
            raise _ManifestPersistenceOutcomeUnknown(manifest_digest(manifest)) from exc
        if not isinstance(readback, dict) or readback != manifest:
            raise _ManifestPersistenceOutcomeUnknown(manifest_digest(manifest))

        # Validate the entire proposal and its task materials before creating
        # the immutable byte-preserving prior-manifest record.
        pending = self._build_pending_revision_projection(
            manifest, event, event.get("payload", {}).get("proposal_ref"), {})
        prior_ref = self._archive_prior_manifest(
            manifest_path, manifest, prior_bytes)
        pending["prior_manifest_ref"] = prior_ref
        updated = json.loads(json.dumps(manifest, ensure_ascii=False))
        updated["reviewed_task_list_pending_revision"] = pending
        binding_approved = manifest["reviewed_task_list_binding"][
            "authorization"]["scope"]["approved_task_digests"]
        self._apply_revision_task_classification(
            updated, binding_approved, pending["classification"])
        self._seal_reviewed_manifest(updated)
        self._verify_reviewed_binding(updated)
        self._write_manifest_atomic(manifest_path, updated)
        return {
            "status": "accepted", "manifest": updated,
            "outcome": {
                "status": "accepted", "kind": "task_list_revision_pending",
                "materialization_digest": manifest_digest(updated),
                "candidate_task_list_digest": pending["candidate_task_list_digest"],
            },
        }

    def process_task_area_events(self, manifest: dict, *, manifest_path: Path,
                                 max_events: int) -> dict[str, Any]:
        if isinstance(max_events, bool) or not 1 <= max_events <= 100:
            raise TaskAreaError("cycle_limit_invalid")
        self._disposition_manifest_path = Path(manifest_path)
        lock = FileLockSchedulerPort(filename="task-area.lock")
        handle = lock.acquire(self.store.root)
        if handle is None:
            return {"status": "waiting", "reason": "task_area_controller_busy",
                    "manifest": manifest, "processed_events": 0}
        processed = 0
        try:
            reviewed_route = (manifest.get("reviewed_task_list_draft") is not None
                              or manifest.get("reviewed_task_list_binding") is not None)
            settled_ids = {event["payload"].get("source_event_id")
                           for event in self.store.events(manifest["goal_id"])
                           if event["event_type"] == "task_area_event_settled"}
            for event in self.store.events(manifest["goal_id"]):
                if event["event_type"] not in {
                    "task_list_approval_updated", "task_area_prerequisite_updated",
                    "task_list_revision_pending", "task_area_recovery_disposition",
                    "task_area_phase_job_result", "task_area_capacity_policy_updated"}:
                    continue
                if event["event_type"] == "task_area_phase_job_result":
                    if event["event_id"] in settled_ids:
                        continue
                    job = next((job for job in self.store.list_phase_jobs(manifest["goal_id"])
                                if job["result_event_id"] == event["event_id"]), None)
                    if (job is None or job["input"]["manifest_digest"] != manifest_digest(manifest)
                        or not any(task.get("status") == "approved"
                                   and task.get("phase_execution") == "durable-v1"
                                   and task["node_id"] == job["input"]["node_id"]
                                   for task in manifest["tasks"])):
                        raise TaskAreaError("phase_job_result_manifest_changed")
                    # Claim only under the short event lock. The original
                    # executor/completion consumer accepts and settles outside
                    # this lock; an immutable result never authorizes a phase.
                    self.store.claim_task_area_event(event["event_id"])
                    continue
                if (event["event_type"] == "task_list_approval_updated"
                    and not reviewed_route):
                    continue
                if (event["event_type"] == "task_list_revision_pending"
                    and manifest.get("reviewed_task_list_binding") is None):
                    continue
                if event["event_id"] in settled_ids:
                    continue
                if processed >= max_events:
                    break
                claim = self.store.claim_task_area_event(event["event_id"])
                if claim["status"] == "settled":
                    settled_ids.add(event["event_id"])
                    continue
                unresolved = self._unresolved_manifest_events.get(event["event_id"])
                if unresolved is not None:
                    expected_digest, prior_digest = unresolved
                    try:
                        readback = json.loads(
                            read_private_file(manifest_path).decode("utf-8"))
                        if not isinstance(readback, dict):
                            raise ValueError("reviewed_manifest_readback_not_object")
                        readback_digest = manifest_digest(readback)
                    except (OSError, UnicodeError, ValueError, TypeError,
                            PlatformPortUnavailable):
                        return {"status": "waiting",
                                "reason": "reviewed_manifest_persistence_outcome_unknown",
                                "outcome": "unknown", "manifest": manifest,
                                "processed_events": processed,
                                "executor_invocations": 0}
                    if readback_digest not in {expected_digest, prior_digest}:
                        return {"status": "waiting",
                                "reason": "reviewed_manifest_persistence_readback_unresolved",
                                "outcome": "unknown", "manifest": manifest,
                                "processed_events": processed,
                                "executor_invocations": 0}
                    manifest = readback
                    self._unresolved_manifest_events.pop(event["event_id"], None)
                prior_manifest_digest = manifest_digest(manifest)
                try:
                    if event["event_type"] == "task_list_approval_updated":
                        if manifest.get("reviewed_task_list_draft") is not None:
                            manifest = self._materialize_approval_event(
                                manifest, event, manifest_path)
                        else:
                            manifest = self._materialize_existing_binding_event(
                                manifest, event, manifest_path)
                        outcome = {"status": "accepted", "kind": "task_list_materialized",
                                   "materialization_digest": manifest_digest(manifest)}
                    elif event["event_type"] == "task_area_capacity_policy_updated":
                        outcome = self.store.capacity_policy_outcome(event["event_id"])
                    elif event["event_type"] == "task_list_revision_pending":
                        revision_result = self._apply_pending_revision_event(
                            manifest, event, manifest_path)
                        if revision_result.get("status") == "waiting":
                            return {"status": "waiting", "reason": revision_result["reason"],
                                    "manifest": manifest, "processed_events": processed,
                                    "executor_invocations": 0}
                        manifest = revision_result["manifest"]
                        outcome = revision_result["outcome"]
                    elif event["event_type"] == "task_area_recovery_disposition":
                        fresh = json.loads(manifest_path.read_bytes())
                        if fresh != manifest or self.approved_digest != manifest_digest(fresh):
                            raise TaskAreaError("recovery_disposition_manifest_changed")
                        self._validate(fresh)
                        if fresh.get("reviewed_task_list_binding") is not None:
                            self._verify_reviewed_binding(fresh)
                        context = self.store.validate_recovery_disposition(event["event_id"],
                            manifest_digest=manifest_digest(fresh), require_accepted=False)
                        if not any(task.get("status") == "approved"
                            and task.get("node_id") == context["record"]["request"]["node_id"]
                            for task in fresh["tasks"]):
                            raise TaskAreaError("recovery_disposition_request_mismatch")
                        outcome = context["outcome"]
                    else:
                        outcome = self._validate_prerequisite_event(manifest, event)
                    self.store.settle_task_area_event(
                        event["event_id"], claim_id=claim["claim_id"], outcome=outcome)
                    processed += 1
                    settled_ids.add(event["event_id"])
                except _ManifestPersistenceOutcomeUnknown as exc:
                    self._unresolved_manifest_events[event["event_id"]] = (
                        exc.expected_digest, prior_manifest_digest)
                    return {"status": "waiting", "reason": str(exc),
                            "outcome": "unknown", "manifest": manifest,
                            "processed_events": processed,
                            "executor_invocations": 0}
                except (TaskAreaError, OSError, ValueError, KeyError) as exc:
                    outcome = {"status": "rejected", "reason": str(exc)}
                    self.store.settle_task_area_event(
                        event["event_id"], claim_id=claim["claim_id"], outcome=outcome)
                    return {"status": "rejected", "reason": str(exc),
                            "manifest": manifest, "processed_events": processed + 1,
                            "executor_invocations": 0}
            return {"status": "processed" if processed else "idle",
                    "manifest": manifest, "processed_events": processed}
        finally:
            lock.release(handle)

    def _settled_prerequisites(self, manifest: dict) -> set[tuple[str, str]]:
        events = self.store.events(manifest["goal_id"])
        result: set[tuple[str, str]] = set()
        for settlement_event in events:
            if settlement_event["event_type"] != "task_area_event_settled":
                continue
            settlement = settlement_event.get("payload", {})
            source_id = settlement.get("source_event_id")
            source = next((event for event in events
                           if event["event_id"] == source_id
                           and event["event_type"] == "task_area_prerequisite_updated"), None)
            claims = [event for event in events
                      if event["event_type"] == "task_area_event_claimed"
                      and event.get("payload", {}).get("source_event_id") == source_id]
            if source is None or len(claims) != 1:
                continue
            if source["payload"].get("observation_kind") in {"baseline", "changed"}:
                # Recovery evidence is not a first-launch prerequisite.
                continue
            claim_event = claims[0]
            claim = claim_event.get("payload", {})
            outcome = settlement.get("outcome", {})
            expected_source_digest = digest_json({
                "event_id": source["event_id"],
                "parent_goal_id": source["parent_goal_id"],
                "event_type": source["event_type"],
                "payload": source["payload"],
                "created_at": float(source["created_at"]),
            })
            if (claim_event.get("event_id") != f"task-area-event-claimed:{source_id}"
                or settlement_event.get("event_id") != f"task-area-event-settled:{source_id}"
                or claim.get("source_event_digest") != expected_source_digest
                or claim.get("claim_id") != settlement.get("claim_id")
                or settlement.get("outcome_digest") != digest_json(outcome)):
                continue
            try:
                expected_outcome = self._validate_prerequisite_event(manifest, source)
            except (TaskAreaError, OSError, ValueError, KeyError, TypeError):
                continue
            if outcome == expected_outcome:
                result.add((expected_outcome["prerequisite_id"],
                            expected_outcome["content_digest"]))
        return result

    def _task_preconditions_ready(self, manifest: dict, task: dict) -> bool:
        satisfied = self._settled_prerequisites(manifest)
        for ref in task.get("required_preconditions", []):
            if (not isinstance(ref, dict)
                or (ref.get("prerequisite_id"), ref.get("content_digest")) not in satisfied):
                return False
        return True

    def _reviewed_approval_settled(self, manifest: dict) -> bool:
        binding = manifest.get("reviewed_task_list_binding")
        materialization = binding.get("materialization") if isinstance(binding, dict) else None
        source_event_id = materialization.get("source_event_id") if isinstance(materialization, dict) else None
        if not isinstance(source_event_id, str) or not source_event_id:
            return False
        events = self.store.events(manifest["goal_id"])
        source = next((event for event in events
                       if event["event_id"] == source_event_id
                       and event["event_type"] == "task_list_approval_updated"), None)
        claims = [event for event in events
                  if event["event_type"] == "task_area_event_claimed"
                  and event["payload"].get("source_event_id") == source_event_id]
        settlements = [event for event in events
                       if event["event_type"] == "task_area_event_settled"
                       and event["payload"].get("source_event_id") == source_event_id]
        if source is None or len(claims) != 1 or len(settlements) != 1:
            return False
        claim = claims[0]["payload"]
        settlement = settlements[0]["payload"]
        outcome = settlement.get("outcome", {})
        source_digest = digest_json({
            "event_id": source["event_id"],
            "parent_goal_id": source["parent_goal_id"],
            "event_type": source["event_type"],
            "payload": source["payload"],
            "created_at": float(source["created_at"]),
        })
        return (
            claim.get("claim_id") == settlement.get("claim_id")
            and claim.get("source_event_digest") == source_digest
            and settlement.get("outcome_digest") == digest_json(outcome)
            and outcome.get("status") == "accepted"
            and outcome.get("kind") == "task_list_materialized"
            and outcome.get("materialization_digest") == manifest_digest(manifest)
        )

    def _register_approved_waiting_units(self, manifest: dict,
                                         tasks: list[dict]) -> set[str]:
        """Register the approved graph in the existing Store without creating Runs."""
        goal = manifest["goal_id"]
        existing = self.store.list_work_units(goal)
        available = {unit["node_id"] for unit in existing}
        by_node = {task["node_id"]: task for task in tasks}
        needed: set[str] = set()

        def include_dependencies(node: str) -> None:
            if node in needed or node not in by_node:
                return
            needed.add(node)
            for dependency in [*by_node[node].get("depends_on", []),
                               *by_node[node].get("delivery_after", [])]:
                include_dependencies(dependency)

        for task in tasks:
            if task.get("status") == "approved":
                include_dependencies(task["node_id"])

        from . import delivery_contract as delivery_engine
        definitions: list[dict[str, Any]] = []
        added: set[str] = set()
        while True:
            advanced = False
            for node in sorted(needed - available - added):
                task = by_node[node]
                dependencies = list(task.get("depends_on", []))
                if any(dependency not in available and dependency not in added
                       for dependency in [*dependencies, *task.get("delivery_after", [])]):
                    continue
                envelope = task.get("envelope")
                contract = task.get("delivery_contract")
                if not isinstance(envelope, dict) or not isinstance(contract, dict):
                    if task.get("status") == "approved":
                        raise TaskAreaError("task_area_waiting_work_unit_binding_missing")
                    continue
                try:
                    resolved = delivery_engine.validate_contract(contract)
                    dispatch_key = envelope["dispatch_key"]
                    if (envelope.get("goal_id") != goal
                        or envelope.get("goal_revision") != manifest["goal_revision"]
                        or envelope.get("node_id") != node
                        or envelope.get("wave_base_sha") != manifest["base_sha"]
                        or not isinstance(dispatch_key, str) or not dispatch_key):
                        raise ValueError("task_area_waiting_envelope_identity_mismatch")
                except (ValueError, KeyError, TypeError) as exc:
                    if task.get("status") == "approved":
                        raise TaskAreaError(str(exc)) from exc
                    continue
                safe = hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()[:32]
                definitions.append({
                    "work_unit_id": f"dispatch-{safe}",
                    "parent_goal_id": goal,
                    "node_id": node,
                    "node_kind": resolved["node"]["kind"],
                    "producer": "ParallelScheduler",
                    "worker_id": f"task-area:{node}",
                    "base_sha": envelope["wave_base_sha"],
                    "dependencies": dependencies,
                    **self._delivery_fields(task),
                    "read_set": task.get("read_set", []),
                    "write_set": task.get("write_set", []),
                    "worktree": envelope.get("worktree") or str(self.store.root),
                    "branch": envelope.get("branch") or f"scheduler/{node}",
                    "state_root": str(self.store.root),
                })
                added.add(node)
                advanced = True
            if not advanced:
                break
        if definitions:
            self.store.register_work_units(goal, definitions)
        return added

    def _validate(self, manifest: dict) -> list[dict]:
        digest = manifest_digest(manifest)
        approval = manifest.get("approval", {})
        if (manifest.get("schema") != "lh-task-area/v1"
            or not self.approved_digest or digest != self.approved_digest
            or approval.get("status") != "approved"
            or approval.get("manifest_digest") != digest):
            raise TaskAreaError("task_area_approval_mismatch")
        verifier = manifest.get("plan_verifier", {})
        if (verifier.get("verdict") != "GREEN"
            or verifier.get("read_only") is not True
            or verifier.get("source_write") is not False
            or not verifier.get("principal")
            or verifier["principal"] == manifest.get("planner_principal")
            or verifier.get("manifest_digest") != manifest_body_digest(manifest)):
            raise TaskAreaError("task_area_plan_verifier_invalid")
        tasks = manifest.get("tasks", [])
        if not isinstance(tasks, list):
            raise TaskAreaError("task_area_tasks_invalid")
        records = []
        for task in tasks:
            node = task.get("node_id")
            if not isinstance(node, str) or not node or task.get("status") not in {
                "approved", "pending", "deferred"}:
                raise TaskAreaError("task_area_task_invalid")
            records.append({"work_unit_id": node, "node_id": node,
                            "dependencies": task.get("depends_on", []),
                            **self._delivery_fields(task)})
            if task["status"] != "approved":
                continue
            self._normal_review_task(task)
            envelope = task.get("envelope", {})
            if any(envelope.get(key) != manifest.get(key)
                   for key in ("goal_id", "goal_revision")) or (
                envelope.get("node_id") != node
                or envelope.get("wave_base_sha") != manifest.get("base_sha")):
                raise TaskAreaError("task_area_envelope_identity_mismatch")
            try:
                packet = json.loads(Path(envelope["packet_path"]).read_text())
                packet = packet.get("packet", packet)
                if sorted(packet.get("write_set", [])) != sorted(task.get("write_set", [])):
                    raise TaskAreaError("task_area_write_set_mismatch")
            except (OSError, ValueError, KeyError) as exc:
                raise TaskAreaError(f"task_area_packet_invalid:{exc}") from exc
        try:
            self.store._validate_graph_records(records)
        except (ValueError, KeyError) as exc:
            raise TaskAreaError(f"task_area_graph_invalid:{exc}") from exc
        return tasks

    def tick(self, manifest: dict, *, max_cycles: int = 4) -> dict[str, Any]:
        self._validate(manifest)
        if "planner_recovery" in manifest and self.recovery_port is None:
            reason = "task_area_planner_recovery_port_unavailable"
            return {"schema": "lh-task-area-tick/v1", "status": "blocked",
                    "reason": reason, "cycles": 0, "results": [],
                    "blocked": [{"node_id": task["node_id"], "reason": reason}
                                for task in manifest["tasks"]
                                if task["status"] == "approved"],
                    "executor_invocations": 0, "known_executor_invocations": 0,
                    "manual_prompts": 0}
        if self.recovery_port is not None:
            return self._tick_with_recovery(manifest, max_cycles=max_cycles)
        if getattr(self, "phase_jobs_enabled", False):
            # Admission is serialized by the existing Store transaction. The
            # bounded submit/ack and subsequent role IO never hold this lock.
            return self._tick_owned(manifest, max_cycles=max_cycles)
        port = FileLockSchedulerPort(filename="task-area.lock")
        handle = port.acquire(self.store.root)
        if handle is None:
            return {"status": "waiting", "reason": "task_area_controller_busy",
                    "cycles": 0, "executor_invocations": 0, "results": []}
        try:
            return self._tick_owned(manifest, max_cycles=max_cycles)
        finally:
            port.release(handle)

    @staticmethod
    def _normal_review_task(task: dict) -> bool:
        """Only a task whose sealed contracts carry candidate review v2 continues by rule.

        Legacy tasks keep their Planner handoff. The review itself belongs to
        the delivery contract; this controller neither produces nor defines it.
        """
        completion = task.get("completion_contract", {})
        delivery = task.get("delivery_contract", {})
        if not isinstance(completion, dict) or not isinstance(delivery, dict):
            return False
        if "candidate_review" not in completion and "candidate_review" not in delivery:
            return False
        policy = completion.get("candidate_review")
        if (not isinstance(policy, dict)
                or policy.get("schema") != "lh-candidate-review-contract/v2"
                or delivery.get("candidate_review") != policy):
            raise TaskAreaError("task_area_candidate_review_contract_mismatch")
        from . import delivery_contract
        delivery_contract.validate_candidate_review_policy(policy)
        return True

    def _normal_completion(self, manifest: dict, task: dict, unit: dict) -> dict:
        """Re-read the original Store's integrated receipt chain for one reviewed task.

        A state label, an exit code, or a caller's PASS is not enough: every
        phase must be settled for this envelope, contract, Attempt and fence,
        and the machine receipt must reference exactly those phase receipts.
        """
        from . import delivery_contract
        run = self.store.get_run_for_work_unit(unit["work_unit_id"])
        envelope = task["envelope"]
        contract = task["delivery_contract"]
        if (task.get("status") != "approved" or not run
                or unit.get("state") != "integrated" or run.get("state") != "integrated"
                or unit.get("node_id") != task["node_id"]
                or unit.get("parent_goal_id") != manifest["goal_id"]
                or run.get("parent_goal_id") != manifest["goal_id"]
                or run.get("work_unit_id") != unit["work_unit_id"]):
            raise TaskAreaError("task_area_normal_completion_identity_invalid")
        attempt = self.store.get_attempt(run["run_id"], run["attempts"])
        dispatch = self.store.get_dispatch_consumption(envelope["dispatch_key"])
        if (not attempt or not dispatch or attempt.get("fence") != run.get("fence")
                or dispatch.get("run_id") != run["run_id"]
                or dispatch.get("work_unit_id") != unit["work_unit_id"]
                or dispatch.get("attempt") != attempt["attempt"]
                or dispatch.get("envelope_digest") != envelope["envelope_digest"]
                or (dispatch.get("envelope") or {}).get("packet_digest") != envelope["packet_digest"]):
            raise TaskAreaError("task_area_normal_completion_dispatch_invalid")
        identity = {"run_id": run["run_id"], "attempt": attempt["attempt"], "fence": attempt["fence"],
                    "goal_id": manifest["goal_id"], "goal_revision": manifest["goal_revision"],
                    "node_id": task["node_id"], "dispatch_key": envelope["dispatch_key"],
                    "base_sha": manifest["base_sha"], "unit_id": contract["unit_id"],
                    "contract_digest": contract["contract_digest"]}
        names = ("candidate", "checks", "verifier", "integration", "integration_checks",
                 "integration_verifier", "delivery_verifier", "machine_complete")
        green = {"checks", "verifier", "integration_checks", "integration_verifier", "delivery_verifier"}
        phases = {}
        for name in names:
            row = self.store.get_completion_phase(run["run_id"], attempt["attempt"], attempt["fence"], name)
            evidence = row.get("evidence") if isinstance(row, dict) else None
            if not isinstance(row, dict) or row.get("state") != "settled" or not isinstance(evidence, dict):
                raise TaskAreaError("task_area_normal_completion_consumer_coverage_missing")
            if (evidence.get("receipt_digest") != digest_json({key: value for key, value in evidence.items()
                                                                  if key != "receipt_digest"})
                    or any(evidence.get(key) != value for key, value in identity.items())
                    or evidence.get("phase") != name or evidence.get("phase_key") != row["phase_key"]
                    or evidence.get("binding") != row["binding"]
                    or (name in green and evidence.get("verdict") != "GREEN")):
                raise TaskAreaError("task_area_normal_completion_receipt_invalid:" + name)
            phases[name] = evidence
        machine = phases["machine_complete"]
        stages = machine.get("stage_receipts")
        if (machine.get("status") != "machine_complete" or not isinstance(stages, dict)
                or any(stages.get(name) != phases[name]["receipt_digest"] for name in names[:-1])):
            raise TaskAreaError("task_area_normal_completion_chain_invalid")
        for name in ("verifier", "integration_verifier"):
            evidence = phases[name]
            if (evidence.get("principal") != task["completion_contract"]["verifier_principal"]
                    or evidence.get("principal") == attempt["holder"]
                    or evidence.get("read_only") is not True or evidence.get("source_write") is not False):
                raise TaskAreaError("task_area_normal_completion_verifier_invalid")
            delivery_contract.verify_candidate_review_proof(contract, evidence)
        source, checks = phases["candidate"], phases["checks"]
        joined, final, verified = (phases[name] for name in ("integration", "integration_checks", "integration_verifier"))
        if (checks.get("candidate_digest") != source.get("candidate_digest")
                or phases["verifier"].get("candidate_digest") != source.get("candidate_digest")
                or joined.get("source_candidate_digest") != source.get("candidate_digest")
                or joined.get("integration_candidate_digest") != final.get("candidate_digest")
                or verified.get("candidate_digest") != final.get("candidate_digest")
                or verified.get("checks_digest") != final["receipt_digest"]):
            raise TaskAreaError("task_area_normal_completion_candidate_chain_invalid")
        # A valid normal chain never washes an unresolved exception or unknown effect.
        if self._unsettled_recovery(unit, run):
            raise TaskAreaError("task_area_normal_completion_recovery_unsettled")
        return machine

    def _unsettled_recovery(self, unit: dict, run: dict) -> bool:
        return any(record.get("request", {}).get("run_id") == run["run_id"]
                   and record.get("status") in {"requested", "claimed", "result_recorded", "verifier_claimed",
                                                "plan_verified", "outcome_unknown"}
                   for record in self.store.recovery_requests(work_unit_id=unit["work_unit_id"]))

    def _normal_recovery_needed(self, manifest: dict, task: dict) -> bool:
        """Whether this round must reach the existing recovery (Planner) port for ``task``."""
        if not self._normal_review_task(task):
            return True
        units = {unit["node_id"]: unit for unit in self.store.list_work_units(manifest["goal_id"])}
        unit = units.get(task["node_id"])
        if unit is None:
            return False  # Nothing dispatched; approval and preconditions stay with admission.
        run = self.store.get_run_for_work_unit(unit["work_unit_id"])
        if run is None:
            return False
        if self._unsettled_recovery(unit, run):
            return True
        if unit.get("state") == "integrated":
            self._normal_completion(manifest, task, unit)
            # A legacy successor still consumes its original Planner handoff.
            return any(task["node_id"] in target.get("depends_on", [])
                       and target.get("status") == "approved" and not self._normal_review_task(target)
                       for target in manifest["tasks"])
        for name in ("checks", "verifier", "integration_checks", "integration_verifier", "delivery_verifier"):
            row = self.store.get_completion_phase(run["run_id"], run["attempts"], run["fence"], name)
            if (isinstance(row, dict) and row.get("state") in {"settled", "failed"}
                    and isinstance(row.get("evidence"), dict) and row["evidence"].get("verdict") == "RED"):
                return True  # The original port validates and claims the actual RED request.
        return False  # Pending phases reconcile through their original consumer.

    def _tick_with_recovery(self, manifest: dict, *, max_cycles: int) -> dict[str, Any]:
        """Use the original scheduler; slow planning never owns its global lock.

        The configured port reserves in WorkUnitStore before launching and
        rechecks the current fence when applying. A new process uses that same
        record, including unknown claims; this loop is not another queue.
        """
        if isinstance(max_cycles, bool) or not 1 <= max_cycles <= 100:
            raise TaskAreaError("cycle_limit_invalid")
        results, recoveries, blocked = [], [], []
        cycles = 0
        for _ in range(max_cycles):
            lock = FileLockSchedulerPort(filename="task-area.lock")
            durable = getattr(self, "phase_jobs_enabled", False)
            handle = None if durable else lock.acquire(self.store.root)
            if handle is None and not durable:
                blocked.append({"reason": "task_area_controller_busy"})
                break
            try:
                eligible = self._eligible_dispatch_nodes(manifest)
                current = self._tick_owned(manifest, max_cycles=1,
                                           eligible_node_ids=eligible)
            finally:
                if handle is not None:
                    lock.release(handle)
            cycles += 1
            results.extend(current.get("results", []))
            blocked = current.get("blocked", [])
            if current.get("reason") == "parent_not_active":
                # No active-parent scheduler exception: only the exact settled
                # disposition's existing closeout may reach the original role port.
                # The global file lock above is released before any role IO.
                stopped_recoveries = []
                for event in self.store.events(manifest["goal_id"]):
                    if event["event_type"] != "task_area_recovery_disposition":
                        continue
                    try:
                        context = self.store.validate_recovery_disposition(event["event_id"],
                            manifest_digest=self.approved_digest)
                        request = context["record"]["request"]
                        task = next((item for item in manifest["tasks"]
                                     if item.get("node_id") == request["node_id"]
                                     and item.get("status") == "approved"), None)
                        if task is None or self._disposition_manifest_path is None:
                            raise TaskAreaError("recovery_disposition_request_mismatch")
                        outcome = self.recovery_port(task, stopped_disposition={
                            "event_id": event["event_id"], "request_id": request["request_id"],
                            "manifest_path": str(self._disposition_manifest_path)})
                        if not isinstance(outcome, dict):
                            raise TaskAreaError("recovery_port_result_invalid")
                        stopped_recoveries.append({"node_id": request["node_id"],
                            "disposition_event_id": event["event_id"], **outcome})
                    except (ValueError, KeyError, OSError) as exc:
                        stopped_recoveries.append({"disposition_event_id": event["event_id"],
                            "status": "blocked", "reason": str(exc), "progress": False})
                if stopped_recoveries:
                    current["recovery"] = stopped_recoveries
                    if "unresolved_recovery" in current:
                        current["unresolved_recovery"] = self.store.unresolved_recovery_readback(manifest)
                return current
            progress = False
            # Include integrated units: a crash may occur after delivery but
            # before the completion handoff is acknowledged. The original
            # consumer still skips their coding/checks/integration effects.
            for task in manifest["tasks"]:
                if task["status"] != "approved":
                    continue
                if (durable and task.get("phase_execution") == "durable-v1"
                    and any(job["input"]["node_id"] == task["node_id"] and job["status"] != "settled"
                            for job in self.store.list_phase_jobs(manifest["goal_id"]))):
                    # Waiting/unknown execution is not a RED or a new P/V request.
                    continue
                try:
                    if not self._normal_recovery_needed(manifest, task):
                        # A reviewed task needs no role call: its integrated chain was
                        # re-verified, so the next bounded cycle may release its successor.
                        progress = progress or any(
                            row.get("node_id") == task["node_id"]
                            and (row.get("completion") or {}).get("status") == "integrated"
                            for row in current.get("results", []))
                        continue
                    outcome = self.recovery_port(task)
                    if not isinstance(outcome, dict):
                        raise TaskAreaError("recovery_port_result_invalid")
                except Exception as exc:
                    outcome = {"status": "blocked", "reason": str(exc), "progress": False}
                recoveries.append({"node_id": task["node_id"], **outcome})
                progress = progress or outcome.get("progress") is True
            if not current.get("results") and not progress:
                break
            # Only this round's real wait can be released here. Historical
            # results are reporting, never an excuse to spin another cycle.
            delivery_released = False
            for row in current.get("results", []):
                completion = row.get("completion") or {}
                if (completion.get("status") == "waiting"
                    and completion.get("reason") == "delivery_dependencies_not_integrated"
                    and not self.store.pending_delivery_dependencies(**{
                        key: completion[key] for key in ("run_id", "attempt", "fence")})):
                    delivery_released = True
            if not progress and not delivery_released and not any(
                (row.get("completion") or {}).get("status") == "machine_complete"
                for row in current.get("results", [])
            ):
                break
        known = sum(row.get("executor_invocations") or 0 for row in results)
        unknown = any(row.get("executor_invocations") is None for row in results)
        return {"schema": "lh-task-area-tick/v1", "status": "processed" if results or any(
                    row.get("progress") is True for row in recoveries) else "idle",
                "cycles": cycles, "results": results, "blocked": blocked,
                "recovery": recoveries,
                "executor_invocations": None if unknown else known,
                "known_executor_invocations": known, "manual_prompts": 0}

    def _eligible_dispatch_nodes(self, manifest: dict) -> set[str]:
        """Hold a ready successor until its integrated predecessor handoff settles."""
        eligible = {task["node_id"] for task in manifest["tasks"]
                    if task.get("status") == "approved"}
        current_digest = manifest_digest(manifest)
        units = {unit["node_id"]: unit
                 for unit in self.store.list_work_units(manifest["goal_id"])}
        tasks = {task["node_id"]: task for task in manifest["tasks"]}

        for target in manifest["tasks"]:
            target_node = target.get("node_id")
            if target_node not in eligible:
                continue
            for dependency in target.get("depends_on", []):
                source = units.get(dependency)
                if not source or source.get("state") != "integrated":
                    continue
                try:
                    if self._normal_review_task(target) and self._normal_review_task(tasks[dependency]):
                        self._normal_completion(manifest, tasks[dependency], source)
                        continue
                except (ValueError, KeyError, TypeError, OSError):
                    eligible.discard(target_node)
                    break
                source_run = self.store.get_run_for_work_unit(source["work_unit_id"])
                if not source_run:
                    eligible.discard(target_node)
                    break
                source_attempt = int(source_run.get("attempts", 0))
                source_fence = int(source_run.get("fence", -1))
                completion = self.store.get_completion_phase(
                    source_run["run_id"], source_attempt, source_fence, "machine_complete")
                evidence = completion.get("evidence") if completion else None
                if (not completion or completion.get("state") != "settled"
                    or not isinstance(evidence, dict)
                    or evidence.get("status") != "machine_complete"):
                    eligible.discard(target_node)
                    break

                records = [record for record in self.store.recovery_requests(
                    work_unit_id=source["work_unit_id"])
                    if record.get("request", {}).get("run_id") == source_run["run_id"]
                    and record.get("request", {}).get("reason_code") == "machine_complete"]
                if (len(records) != 1 or any(
                    record.get("request", {}).get("authority_digest") != current_digest
                    for record in records
                ) or any(record.get("status") in {
                    "requested", "claimed", "result_recorded", "verifier_claimed",
                    "plan_verified", "outcome_unknown",
                } for record in records)):
                    eligible.discard(target_node)
                    break

                record = records[0]
                action = (record.get("result") or {}).get("action")
                apply = record.get("apply")
                if (not isinstance(action, dict)
                    or action.get("kind") != "dispatch_successor"
                    or record.get("status") != "applied"
                    or action.get("target_node_id") != target_node
                    or not isinstance(apply, dict)
                    or apply.get("action") != action
                    or apply.get("authority_digest") != current_digest):
                    eligible.discard(target_node)
                    break
        return eligible

    def _tick_owned(self, manifest: dict, *, max_cycles: int,
                    eligible_node_ids: set[str] | None = None) -> dict[str, Any]:
        if isinstance(max_cycles, bool) or not 1 <= max_cycles <= 100:
            raise TaskAreaError("cycle_limit_invalid")
        tasks = self._validate(manifest)
        goal = manifest["goal_id"]
        durable = getattr(self, "phase_jobs_enabled", False)
        ranks = {}
        if durable:
            self.max_workers = self.store.phase_resource_readback()["max_workers"]
            by_node = {task["node_id"]: task for task in tasks}
            def depth(node):
                if node not in ranks:
                    task = by_node[node]
                    edges = {*task.get("depends_on", []), *task.get("delivery_after", [])}
                    ranks[node] = max((depth(dep) + 1 for dep in edges), default=0)
                return ranks[node]
            for node in by_node:
                depth(node)
        parent = self.store.create_parent_goal(goal, goal_revision=manifest["goal_revision"],
                                               base_sha=manifest["base_sha"])
        results, blocked = [], []
        if parent["state"] != "active":
            result = {"status": "blocked", "reason": "parent_not_active",
                      "cycles": 0, "executor_invocations": 0, "results": []}
            if manifest.get("planner_recovery", {}).get("recovery_readback_required") is True:
                try:
                    result["unresolved_recovery"] = self.store.unresolved_recovery_readback(manifest)
                except (ValueError, KeyError, TypeError) as exc:
                    raise TaskAreaError(f"task_area_recovery_readback_failed:{exc}") from exc
            return result
        if (manifest.get("reviewed_task_list_binding") is not None
            or any("delivery_after" in task for task in tasks)):
            self._register_approved_waiting_units(manifest, tasks)
        # At most one replay per tick unless the durable Attempt changes. A
        # pending external outcome must not consume all cycles or re-launch.
        visited = set()
        delivery_waits: dict[str, dict] = {}
        for cycle in range(max_cycles):
            ready_results = {job["input"]["node_id"] for job in self.store.list_phase_jobs(goal)
                             if job["status"] == "result_ready"} if durable else set()
            units = {unit["node_id"]: unit for unit in self.store.list_work_units(goal)}
            occupied = [unit for unit in units.values()
                        if unit["state"] in {"ready", "running", "verified"}]
            candidates, blocked = [], []
            for task in tasks:
                node = task["node_id"]
                if eligible_node_ids is not None and node not in eligible_node_ids:
                    continue
                unit = units.get(node)
                run = self.store.get_run_for_work_unit(unit["work_unit_id"]) if unit else None
                attempt = self.store.get_attempt(run["run_id"]) if run else None
                visit = (node, attempt["attempt"] if attempt else 0)
                if unit and unit["state"] == "integrated":
                    continue
                unmet = [dep for dep in task.get("depends_on", [])
                         if units.get(dep, {}).get("state") != "integrated"]
                if task["status"] != "approved" or unmet:
                    blocked.append({"node_id": node, "reason": task["status"]
                                    if task["status"] != "approved" else "dependencies",
                                    "dependencies": unmet})
                    continue
                if self._normal_review_task(task):
                    try:
                        for dependency in task.get("depends_on", []):
                            producer = next(item for item in tasks if item["node_id"] == dependency)
                            if self._normal_review_task(producer):
                                self._normal_completion(manifest, producer, units[dependency])
                    except (ValueError, KeyError, TypeError, OSError) as exc:
                        blocked.append({"node_id": node, "reason": str(exc)})
                        continue
                missing_preconditions = [ref for ref in task.get("required_preconditions", [])
                                        if (not isinstance(ref, dict)
                                            or (ref.get("prerequisite_id"),
                                                ref.get("content_digest"))
                                            not in self._settled_prerequisites(manifest))]
                if missing_preconditions:
                    blocked.append({"node_id": node,
                                    "reason": "required_preconditions",
                                    "preconditions": missing_preconditions})
                    continue
                if visit in visited:
                    waiting = delivery_waits.get(node)
                    if (self.recovery_port is not None or waiting is None
                        or run is None or attempt is None
                        or waiting != {"run_id": run["run_id"], "attempt": attempt["attempt"],
                                       "fence": attempt["fence"]}
                        or self.store.pending_delivery_dependencies(**waiting)):
                        continue
                candidates.append({**task, "work_unit_id": unit["work_unit_id"] if unit else node,
                                   "created_at": -1 if node in ready_results else ranks.get(node, 0), "_visit": visit})
            # Existing active nodes can replay their own durable consumer, but
            # remain occupied for every other node (including unrelated callers).
            eligible = [candidate for candidate in candidates
                        if ParallelScheduler._choose_compatible([candidate],
                            [u for u in occupied if u["node_id"] != candidate["node_id"]], 1)]
            if ready_results:
                # Consume existing results before competing for a new line or
                # slot. Other callers are still serialized by Store admission.
                ready = [candidate for candidate in eligible if candidate["node_id"] in ready_results]
                if ready:
                    eligible = ready
            selected = ParallelScheduler._choose_compatible(eligible, [], self.max_workers)
            occupied_nodes = {unit["node_id"] for unit in occupied}
            # A lowered limit prevents new occupancy, not consumption of an
            # already admitted line's result. Store still guards admission.
            remaining = max(0, self.max_workers - len(occupied_nodes))
            draining = []
            for item in selected:
                if item["node_id"] in occupied_nodes:
                    draining.append(item)
                elif remaining:
                    draining.append(item)
                    remaining -= 1
            selected = draining
            if not selected:
                break
            def consume(task):
                consumer = self.consumer_factory(task)
                if not isinstance(consumer, SuccessorDispatchConsumer) or consumer.store is not self.store:
                    raise TaskAreaError("task_area_consumer_invalid")
                consumer.work_definition = {
                    "dependencies": task.get("depends_on", []),
                    **self._delivery_fields(task),
                    "read_set": task.get("read_set", []),
                    "write_set": task.get("write_set", []),
                    "max_workers": self.max_workers,
                }
                result = consumer.consume(task["envelope"], complete=False)
                return consumer, result
            with ThreadPoolExecutor(max_workers=len(selected)) as pool:
                futures = [(task, pool.submit(consume, task)) for task in selected]
                for task, future in futures:
                    visited.add(task["_visit"])
                    try:
                        consumer, result = future.result()
                    except Exception as exc:
                        result = {"status": "blocked", "reason": str(exc),
                                  "executor_invocations": None, "invocation_accounting": "unknown"}
                        if getattr(exc, "conflicts", None):
                            result["conflicts"] = exc.conflicts
                    else:
                        # Execution overlaps; checks, verification and integration
                        # settle in deterministic selected order on this caller.
                        try:
                            recovery = getattr(consumer, "candidate_recovery_admission", None)
                            if (result.get("reason") in {"phase_job_waiting", "phase_job_outcome_unknown"}
                                or str(result.get("reason", "")).startswith("resource_slot_waiting:")):
                                pass  # Preserve the original pending job; no completion admission.
                            elif recovery is None:
                                if self.recovery_port is None:
                                    result["completion"] = consumer.completion_controller.advance(task["envelope"], result)
                                else:
                                    result["completion"] = consumer.completion_controller.advance(
                                        task["envelope"], result, allow_retry=False)
                            else:
                                result["completion"] = consumer.completion_controller.advance(
                                    task["envelope"], result,
                                    allow_retry=consumer.completion_controller.permits_preserved_retry,
                                    candidate_recovery_admission=recovery)
                        except Exception as exc:
                            result["completion"] = {"status": "incomplete", "reason": str(exc)}
                    results.append({"node_id": task["node_id"], **result})
                    delivery_waits.pop(task["node_id"], None)
                    completion = result.get("completion") or {}
                    if (self.recovery_port is None and "delivery_after" in task
                        and completion.get("status") == "waiting"
                        and completion.get("reason") == "delivery_dependencies_not_integrated"):
                        delivery_waits[task["node_id"]] = {
                            key: completion[key] for key in ("run_id", "attempt", "fence")}
                    # Re-read ordinal: consume may have scheduled a bounded
                    # repair. Do not mark its new ordinal visited prematurely.
                    old = task["_visit"][1]
                    if old == 0 and result.get("attempt") and not result.get("retry_scheduled"):
                        visited.add((task["node_id"], result["attempt"]))
        known = sum(r.get("executor_invocations") or 0 for r in results)
        unknown = any(r.get("executor_invocations") is None for r in results)
        return {"schema": "lh-task-area-tick/v1", "status": "processed" if results else "idle",
                "cycles": cycle + 1, "results": results, "blocked": blocked,
                "executor_invocations": None if unknown else known,
                "known_executor_invocations": known,
                "manual_prompts": 0}

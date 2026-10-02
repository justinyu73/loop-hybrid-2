"""Task-level source evidence at the existing prerequisite boundary; no Run authority."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

from .platform_ports import FileLockSchedulerPort
from .work_unit_store import digest_json


class SourceResultError(ValueError):
    pass


def _read(path):
    target = Path(path)
    if not target.is_absolute() or target.resolve() != target or not target.is_file():
        raise SourceResultError("source_result_path_invalid")
    raw = target.read_bytes()
    return raw, "sha256:" + hashlib.sha256(raw).hexdigest()


def _role_bindings(manifest, intent):
    """Resolve identities from the original operator-owned, digest-bound registries."""
    from .runner_adapter import validate_contract
    from .provider_registry import select_provider, validate_provider_registry
    binding = manifest["execution_binding"]
    documents = {}
    for name in ("capability_contract_ref", "provider_registry_ref"):
        reference = binding[name]
        raw, digest = _read(reference["path"])
        if digest != reference.get("digest"):
            raise SourceResultError("source_result_role_registry_drift")
        documents[name] = json.loads(raw)
    capabilities = validate_contract(documents["capability_contract_ref"])
    registry = validate_provider_registry(documents["provider_registry_ref"])
    for name, capability_name in (("producer", "checks"), ("verifier", "verifier")):
        role = intent[name]
        descriptor = select_provider(registry, role.get("provider_id"))
        capability = capabilities["capabilities"][capability_name]
        if (descriptor["identity"] != capability["identity"]
            or descriptor["adapter_id"] != capability["adapter_id"]
            or capability["permissions"] != "read_only"
            or descriptor["identity"].get("principal") != role["principal"]):
            raise SourceResultError("source_result_provider_identity_mismatch")
        command = descriptor["command"]
        if (len(command) != 3 or command[1] != "-B" or command[2] != role["path"]
            or (name == "verifier" and command != role["argv"])):
            raise SourceResultError("source_result_provider_command_mismatch")
        _read(command[0])


def context(controller, manifest, ref):
    controller._verify_reviewed_binding(manifest)
    from .task_area import manifest_digest
    approval = controller._reviewed_approval_event_receipt(manifest)
    if approval is None:
        raise SourceResultError("source_result_approval_pending")
    approved = approval["outcome"].get("materialization_digest")
    if approved != manifest_digest(manifest):
        raise SourceResultError("source_result_authority_drift")
    if controller.approved_digest is None:
        controller.approved_digest = approved
    controller._validate(manifest)
    if not controller._reviewed_approval_settled(manifest):
        raise SourceResultError("source_result_approval_pending")
    if ref not in manifest.get("source_prerequisites", []):
        raise SourceResultError("source_result_intent_unbound")
    raw, digest = _read(ref["path"])
    if digest != ref.get("content_digest") or ref.get("kind") != "source_result":
        raise SourceResultError("source_result_intent_drift")
    intent = json.loads(raw)
    if intent.get("schema") != "lh-source-result-intent/v1":
        raise SourceResultError("source_result_intent_invalid")
    targets = [task for task in manifest["tasks"] if task.get("node_id") == intent.get("next_node_id")
               and task.get("status") == "approved" and ref in task.get("required_preconditions", [])]
    if len(targets) != 1:
        raise SourceResultError("source_result_target_not_approved")
    for key in ("task_id", "action_id", "tree"):
        if not isinstance(intent.get(key), str) or not intent[key].strip():
            raise SourceResultError("source_result_identity_missing")
    if type(intent.get("revision")) is not int or intent["revision"] < 1:
        raise SourceResultError("source_result_revision_invalid")
    source, input_digest = _read(intent["input_path"])
    if input_digest != intent.get("input_digest"):
        raise SourceResultError("source_result_input_drift")
    if json.loads(source) != {key:intent[key] for key in ("task_id", "action_id", "revision", "tree")}:
        raise SourceResultError("source_result_input_identity_mismatch")
    producer, verifier = intent["producer"], intent["verifier"]
    if (not producer.get("principal") or not verifier.get("principal")
        or producer["principal"] == verifier["principal"]
        or producer.get("path") == verifier.get("path")):
        raise SourceResultError("source_result_independence_missing")
    _role_bindings(manifest, intent)
    for role in (producer, verifier):
        _, actual = _read(role["path"])
        if actual != "sha256:" + role["sha256"]:
            raise SourceResultError("source_result_role_source_drift")
    argv = verifier.get("argv")
    if (not isinstance(argv, list) or len(argv) != 3 or argv[1] != "-B"
        or argv[2] != verifier["path"] or not all(isinstance(v, str) for v in argv)):
        raise SourceResultError("source_result_verifier_command_invalid")
    _read(argv[0])
    timeout = verifier.get("timeout_seconds")
    if type(timeout) is not int or not 1 <= timeout <= 30:
        raise SourceResultError("source_result_verifier_budget_invalid")
    if not Path(intent["result_path"]).exists():
        raise SourceResultError("source_result_producer_coverage_missing")
    result_raw, result_digest = _read(intent["result_path"])
    result = json.loads(result_raw)
    if (result.get("schema") != "lh-source-result/v1"
        or any(result.get(key) != intent[key] for key in ("task_id", "action_id", "revision", "tree", "input_digest"))
        or result.get("producer") != producer["principal"]
        or result.get("runner_sha256") != producer["sha256"]
        or type(result.get("exit_code")) is not int or result["exit_code"] != 0
        or result.get("source_unchanged") is not True):
        raise SourceResultError("source_result_binding_mismatch")
    artifact = result.get("artifact", {})
    _, artifact_digest = _read(artifact["path"])
    if artifact_digest != "sha256:" + artifact["sha256"]:
        raise SourceResultError("source_result_artifact_drift")
    binding = manifest["reviewed_task_list_binding"]
    request = {"schema":"lh-source-result-request/v1", "goal_id":manifest["goal_id"],
        "goal_revision":manifest["goal_revision"], "prerequisite_ref":ref,
        "task_list_digest":binding["task_list_digest"],
        "source_approval_digest":binding["authorization"]["source_approval_digest"],
        **{key:intent[key] for key in ("task_id", "action_id", "revision", "tree", "input_digest", "result_path")},
        "result_digest":result_digest, "verifier":verifier}
    request["request_digest"] = digest_json(request)
    return intent, request


def event_id(request):
    # A changed output cannot create a new slot around an unresolved claim.
    return "task-source-result:" + digest_json({key:request[key] for key in (
        "goal_id", "goal_revision", "task_id", "action_id", "revision", "tree",
        "input_digest", "prerequisite_ref", "task_list_digest", "source_approval_digest")})


def _receipt(store, goal, identity, kind):
    rows = [row for row in store.events(goal) if row["event_id"] == identity]
    if not rows:
        return None
    if len(rows) != 1 or rows[0]["event_type"] != kind:
        raise SourceResultError("source_result_receipt_identity_mismatch")
    return rows[0]["payload"]


def validate_event(controller, manifest, event):
    payload = event["payload"]
    ref = payload["prerequisite_ref"]
    _, request = context(controller, manifest, ref)
    identity = event_id(request)
    if event.get("event_id") != identity:
        raise SourceResultError("source_result_event_identity_mismatch")
    claim = _receipt(controller.store, manifest["goal_id"], identity+":claim", "task_source_result_claimed")
    proof = _receipt(controller.store, manifest["goal_id"], identity+":verified", "task_source_result_verified")
    if claim != request or proof is None or payload.get("verification") != proof:
        raise SourceResultError("source_result_independent_receipt_missing")
    expected = {"schema":"lh-source-result-verification/v1", "request_digest":request["request_digest"],
                "result_digest":request["result_digest"],
                "principal":request["verifier"]["principal"], "verdict":"GREEN"}
    if proof.get("request") != request or proof.get("verdict") != expected or proof.get("exit_code") != 0:
        raise SourceResultError("source_result_independent_verdict_invalid")


def observe(controller, manifest, manifest_path, *, verifier_port):
    if manifest.get("reviewed_task_list_binding") is None or not controller._reviewed_approval_settled(manifest):
        return []
    from .task_area import manifest_digest
    controller.approved_digest = manifest_digest(manifest)
    refs = [ref for ref in manifest.get("source_prerequisites", [])
            if isinstance(ref, dict) and ref.get("kind") == "source_result"]
    if len(refs) > 3:
        return [{"status":"rejected", "reason":"source_result_observation_limit", "appended":False}]
    observations = []
    for ref in refs:
        row = {"prerequisite_id":ref.get("prerequisite_id"), "appended":False}
        try:
            intent, request = context(controller, manifest, ref)
            identity = event_id(request)
            lock = FileLockSchedulerPort(filename="task-area.lock")
            handle = lock.acquire(controller.store.root)
            if handle is None:
                observations.append({**row, "status":"waiting", "reason":"task_area_controller_busy"})
                continue
            try:
                if json.loads(Path(manifest_path).read_bytes()) != manifest or context(controller, manifest, ref)[1] != request:
                    raise SourceResultError("source_result_input_changed")
                proof = _receipt(controller.store, manifest["goal_id"], identity+":verified", "task_source_result_verified")
                if proof is None:
                    reserved = controller.store.record_task_source_result(
                        event_id=identity+":claim", parent_goal_id=manifest["goal_id"],
                        goal_revision=manifest["goal_revision"], kind="task_source_result_claimed", payload=request)
                    if not reserved["appended"]:
                        raise SourceResultError("source_result_verification_outcome_unknown")
            finally:
                lock.release(handle)
            if proof is None:
                # Explicitly bound deterministic checker, outside the controller lock.
                if context(controller, manifest, ref)[1] != request:
                    raise SourceResultError("source_result_input_changed")
                completed = verifier_port(intent, request)
                try:
                    verdict = json.loads(completed.stdout)
                except ValueError:
                    verdict = None
                proof = {"request":request, "verdict":verdict, "exit_code":completed.returncode,
                    "stdout_sha256":hashlib.sha256(completed.stdout.encode()).hexdigest(),
                    "stderr_sha256":hashlib.sha256(completed.stderr.encode()).hexdigest()}
                controller.store.record_task_source_result(event_id=identity+":verified",
                    parent_goal_id=manifest["goal_id"], goal_revision=manifest["goal_revision"],
                    kind="task_source_result_verified", payload=proof)
            handle = lock.acquire(controller.store.root)
            if handle is None:
                observations.append({**row, "status":"waiting", "reason":"task_area_controller_busy"})
                continue
            try:
                if json.loads(Path(manifest_path).read_bytes()) != manifest:
                    raise SourceResultError("source_result_manifest_changed")
                payload = {"status":"satisfied", "prerequisite_ref":ref, "verification":proof}
                validate_event(controller, manifest, {"event_id":identity, "payload":payload})
                existing = _receipt(controller.store, manifest["goal_id"], identity,
                                    "task_area_prerequisite_updated")
                if existing is not None:
                    if existing != payload:
                        raise SourceResultError("source_result_publication_drift")
                    observations.append({**row, "event_id":identity, "status":"accepted"})
                    continue
                published = controller.store.record_task_area_prerequisite(event_id=identity,
                    parent_goal_id=manifest["goal_id"], goal_revision=manifest["goal_revision"], payload=payload)
                observations.append({**row, **published, "status":"accepted"})
            finally:
                lock.release(handle)
        except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            observations.append({**row, "status":"rejected", "reason":str(exc) or type(exc).__name__})
    return observations

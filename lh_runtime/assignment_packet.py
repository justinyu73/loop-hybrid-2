"""Read and verify one external host assignment packet for the LH binding seam.

The packet is an input to LH admission.  This module only reads the packet,
the target repository's recorded ref, and the target-owned authority blobs.
It returns a small binding record; Goal and Run creation remain in LH.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

try:
    from . import goal_assignment
except ImportError:  # direct script execution keeps lh_runtime on sys.path
    import goal_assignment


PACKET_SCHEMA = "lh-host-assignment-packet/v1"
BINDING_SCHEMA = "lh-assignment-packet-binding/v1"
COMMIT_RE = re.compile(r"^[0-9a-f]{40,64}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REF_RE = re.compile(r"^[A-Za-z0-9._/-]+$")


def _canonical_digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _raw_digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    text = _text(name, value)
    if not DIGEST_RE.fullmatch(text):
        raise ValueError(f"{name} must be a sha256 digest")
    return text


def _commit(name: str, value: Any) -> str:
    text = _text(name, value)
    if not COMMIT_RE.fullmatch(text):
        raise ValueError(f"{name} must be an exact commit id")
    return text


def _relative_ref(name: str, value: Any) -> str:
    text = _text(name, value)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{name} must be a relative repository path")
    return text


def _git_ref(repo: Path, ref: str) -> str:
    if (
        not REF_RE.fullmatch(ref)
        or ref.startswith("-")
        or ".." in ref
        or "//" in ref
    ):
        raise ValueError("target remote ref is invalid")
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError("target remote ref is unavailable")
    return completed.stdout.strip()


def _git_blob(repo: Path, ref: str, relative: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repo), "show", f"{ref}:{relative}"],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"target authority blob is unavailable: {relative}")
    return completed.stdout


def _git_has_commit(repo: Path, commit: str) -> bool:
    return subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-e", f"{commit}^{{commit}}"],
        capture_output=True,
        check=False,
    ).returncode == 0


def _is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool:
    return subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        check=False,
    ).returncode == 0


def _load_json_blob(name: str, raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def _packet_path(path: str | Path) -> Path:
    packet = Path(path).expanduser().resolve(strict=True)
    if not packet.is_file():
        raise ValueError("assignment packet is not a file")
    trusted_value = os.environ.get("LH_TRUSTED_BOOTSTRAP_ROOT")
    if trusted_value:
        trusted = Path(trusted_value).expanduser().resolve(strict=True)
        if not trusted.is_dir() or not packet.is_relative_to(trusted):
            raise ValueError("assignment packet is outside the trusted external host root")
        if not packet.is_relative_to(trusted / "docs" / "codex-handoff"):
            raise ValueError("assignment packet is outside the handoff area")
    return packet


def _stage_id_from_ref(ref: str, contract_ref: str) -> str:
    prefix = f"{contract_ref}#campaign.stages["
    suffix = "].goal.must_have"
    if not ref.startswith(prefix) or not ref.endswith(suffix):
        raise ValueError("criterion authority reference is not a target stage")
    stage_id = ref[len(prefix) : -len(suffix)]
    return _text("criterion stage_id", stage_id)


def load_assignment_packet(
    path: str | Path,
    *,
    project_id: str,
    campaign_id: str,
    source_repo: str | Path,
    expected_correlation_id: str | None = None,
    expected_task_id: str | None = None,
) -> dict[str, Any]:
    """Return a verified packet binding and its target contract.

    ``source_repo`` is used only to read the packet's recorded target remote
    ref.  The working tree files are not treated as the target authority.
    """
    packet_path = _packet_path(path)
    packet_raw = packet_path.read_bytes()
    packet = _load_json_blob("assignment packet", packet_raw)
    if packet.get("schema") != PACKET_SCHEMA:
        raise ValueError(f"assignment packet schema must be {PACKET_SCHEMA}")
    packet_id = _text("assignment packet.packet_id", packet.get("packet_id"))
    if expected_correlation_id is not None and packet_id != expected_correlation_id:
        raise ValueError("assignment packet does not match the correlation id")
    if packet.get("packet_state") != "paused_registration":
        raise ValueError("assignment packet is not a paused registration")
    if packet.get("execution_status") != "not_started":
        raise ValueError("assignment packet execution status is not not_started")

    assignment_meta = packet.get("assignment")
    if not isinstance(assignment_meta, dict):
        raise ValueError("assignment packet.assignment must be an object")
    packet_task_id = _text("assignment.task_id", assignment_meta.get("task_id"))
    if expected_task_id is not None:
        if packet_task_id != _text("expected_task_id", expected_task_id):
            raise ValueError("assignment packet task_id does not match launch task_id")
    if assignment_meta.get("project_id") != project_id:
        raise ValueError("assignment packet project_id does not match")
    if assignment_meta.get("dispatch") is not False:
        raise ValueError("assignment packet dispatch must be false")
    if assignment_meta.get("self_accept") is not False:
        raise ValueError("assignment packet self_accept must be false")

    normalized_assignment = goal_assignment.normalize_assignment(
        packet.get("goal_assignment")
    )
    if normalized_assignment["project_id"] != project_id:
        raise ValueError("goal assignment project_id does not match")
    if normalized_assignment["assigner_ref"] != f"host-packet:{packet_id}":
        raise ValueError("goal assignment assigner_ref does not name this packet")
    packet_assignment_digest = _digest(
        "assignment packet.assignment_digest", packet.get("assignment_digest")
    )
    actual_assignment_digest = goal_assignment._digest(normalized_assignment)
    if packet_assignment_digest != actual_assignment_digest:
        raise ValueError("assignment packet digest does not match goal_assignment")

    source_binding = packet.get("source_binding")
    if not isinstance(source_binding, dict):
        raise ValueError("assignment packet.source_binding must be an object")
    target_repo = Path(source_repo).expanduser().resolve(strict=True)
    if not target_repo.is_dir():
        raise ValueError("target source repository is not a directory")
    target_ref = _text("source_binding.target_remote", source_binding.get("target_remote"))
    target_base_sha = _commit(
        "source_binding.target_base_sha", source_binding.get("target_base_sha")
    )
    resolved_target = _git_ref(target_repo, target_ref)
    if resolved_target != target_base_sha:
        raise ValueError("target remote ref does not match the packet base")
    if not _git_has_commit(target_repo, normalized_assignment["base_revision"]):
        raise ValueError("goal assignment base revision is unavailable")
    if not _is_ancestor(
        target_repo, normalized_assignment["base_revision"], target_base_sha
    ):
        raise ValueError("goal assignment base revision is not in the target base history")

    contract_binding = source_binding.get("target_contract")
    authority_binding = source_binding.get("target_authority")
    registry_binding = source_binding.get("target_check_registry")
    if not all(isinstance(item, dict) for item in (contract_binding, authority_binding, registry_binding)):
        raise ValueError("target authority bindings are incomplete")
    contract_ref = _relative_ref("target_contract.ref", contract_binding.get("ref"))
    authority_ref = _relative_ref("target_authority.ref", authority_binding.get("ref"))
    registry_ref = _relative_ref(
        "target_check_registry.ref", registry_binding.get("ref")
    )
    contract_raw = _git_blob(target_repo, target_ref, contract_ref)
    authority_raw = _git_blob(target_repo, target_ref, authority_ref)
    registry_raw = _git_blob(target_repo, target_ref, registry_ref)
    if _raw_digest(contract_raw) != _digest(
        "target_contract.raw_sha256", contract_binding.get("raw_sha256")
    ):
        raise ValueError("target contract bytes differ from the packet")
    if _canonical_digest(_load_json_blob("target contract", contract_raw)) != _digest(
        "target_contract.digest", contract_binding.get("digest")
    ):
        raise ValueError("target contract digest differs from the packet")
    if _raw_digest(authority_raw) != _digest(
        "target_authority.digest", authority_binding.get("digest")
    ):
        raise ValueError("target authority bytes differ from the packet")
    if _raw_digest(registry_raw) != _digest(
        "target_check_registry.raw_sha256", registry_binding.get("raw_sha256")
    ):
        raise ValueError("target check registry bytes differ from the packet")
    target_contract = _load_json_blob("target contract", contract_raw)
    target_registry = _load_json_blob("target check registry", registry_raw)
    if _canonical_digest(target_registry) != _digest(
        "target_check_registry.digest", registry_binding.get("digest")
    ):
        raise ValueError("target check registry digest differs from the packet")

    if target_contract.get("project_id") != project_id:
        raise ValueError("target contract project_id does not match")
    target_campaign = target_contract.get("campaign")
    if not isinstance(target_campaign, dict) or target_campaign.get("campaign_id") != campaign_id:
        raise ValueError("target contract campaign_id does not match")
    if target_contract.get("base_revision") != normalized_assignment["base_revision"]:
        raise ValueError("goal assignment base revision differs from target contract")
    target_authority = target_contract.get("authority")
    if not isinstance(target_authority, dict):
        raise ValueError("target contract authority binding is missing")
    if target_authority.get("digest") != authority_binding.get("digest"):
        raise ValueError("target contract authority digest differs from packet")
    if target_registry.get("target_project") != project_id:
        raise ValueError("target check registry project does not match")

    stages = target_campaign.get("stages")
    if not isinstance(stages, list) or not stages:
        raise ValueError("target contract has no campaign stages")
    criteria = normalized_assignment["criteria"]
    stage_ids = {
        _stage_id_from_ref(item["criterion_authority_ref"], contract_ref)
        for item in criteria
    }
    if len(stage_ids) != 1:
        raise ValueError("goal assignment criteria span multiple target stages")
    stage_id = next(iter(stage_ids))
    stage = next(
        (item for item in stages if isinstance(item, dict) and item.get("stage_id") == stage_id),
        None,
    )
    if not isinstance(stage, dict):
        raise ValueError("goal assignment stage is missing from target contract")
    expected_criterion_digest = _canonical_digest(stage.get("goal"))
    if any(
        item["criterion_authority_digest"] != expected_criterion_digest
        for item in criteria
    ):
        raise ValueError("criterion authority digest differs from target stage")

    onboarding = target_contract.get("onboarding")
    if not isinstance(onboarding, dict):
        raise ValueError("target contract onboarding binding is missing")
    packet_paths = packet.get("allowed_paths")
    if not isinstance(packet_paths, dict):
        raise ValueError("packet allowed paths are missing")
    target_allowed = packet_paths.get("target")
    if target_allowed != stage.get("allowed_paths") or target_allowed != onboarding.get("allowed_paths"):
        raise ValueError("packet allowed paths differ from target contract")
    target_effects = packet.get("allowed_side_effects")
    if target_effects != stage.get("allowed_side_effects") or target_effects != onboarding.get("allowed_side_effects"):
        raise ValueError("packet side effects differ from target contract")

    profile = _text("onboarding.pilot_profile", onboarding.get("pilot_profile"))
    profiles = target_registry.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("target check registry profiles are missing")
    profile_ids = profiles.get(profile)
    if not isinstance(profile_ids, list) or not all(isinstance(item, str) for item in profile_ids):
        raise ValueError("target check registry profile is missing")
    raw_checks = target_registry.get("checks")
    if not isinstance(raw_checks, dict):
        raise ValueError("target check registry checks are missing")
    packet_checks = packet.get("target_checks")
    if not isinstance(packet_checks, list) or [item.get("id") for item in packet_checks if isinstance(item, dict)] != profile_ids:
        raise ValueError("packet checks do not match the target profile")
    criterion_check_ids = {item["check_id"] for item in criteria}
    if criterion_check_ids != set(profile_ids):
        raise ValueError("goal assignment checks do not match the target profile")
    for item in packet_checks:
        if not isinstance(item, dict):
            raise ValueError("packet target check must be an object")
        check_id = _text("target check.id", item.get("id"))
        definition = raw_checks.get(check_id)
        if not isinstance(definition, dict):
            raise ValueError(f"target check is missing: {check_id}")
        if item.get("argv") != definition.get("command") or item.get("expect_exit_code") != definition.get("expect_exit_code"):
            raise ValueError(f"packet target check differs from registry: {check_id}")
        if item.get("registry") != registry_ref:
            raise ValueError(f"packet target check registry ref differs: {check_id}")
        if item.get("definition_digest") != _canonical_digest(definition):
            raise ValueError(f"packet target check digest differs: {check_id}")
    for item in criteria:
        if item["check_definition_digest"] != _canonical_digest(raw_checks[item["check_id"]]):
            raise ValueError(f"goal assignment check digest differs: {item['check_id']}")

    return {
        "binding": {
            "schema": BINDING_SCHEMA,
            "packet_id": packet_id,
            "packet_path": str(packet_path),
            "packet_digest": _raw_digest(packet_raw),
            "assignment_id": _text("assignment.assignment_id", assignment_meta.get("assignment_id")),
            "task_id": packet_task_id,
            "assignment_digest": actual_assignment_digest,
            "project_id": project_id,
            "campaign_id": campaign_id,
            "stage_id": stage_id,
            "base_revision": normalized_assignment["base_revision"],
            "target_ref": target_ref,
            "target_base_sha": target_base_sha,
            "target_contract_digest": _digest("target_contract.digest", contract_binding.get("digest")),
            "target_check_registry_digest": _digest("target_check_registry.digest", registry_binding.get("digest")),
            "target_authority_digest": _digest("target_authority.digest", authority_binding.get("digest")),
        },
        "assignment": normalized_assignment,
        "target_contract": target_contract,
    }

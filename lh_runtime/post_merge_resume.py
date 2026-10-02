#!/usr/bin/env python3
"""Provider-neutral SCM merge event resume for one sequential plan edge.

The module owns the event contract and the durable, exactly-once state
transition.  SCM readers, lease acquisition, queue projection, and successor
dispatch are injected ports.  No SCM, host UI, or model integration is needed
by the core.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any


try:
    from .platform_ports import locked_file, make_file_private, sync_directory
except ImportError:
    from platform_ports import locked_file, make_file_private, sync_directory


SCHEMA = "host-post-merge-resume/v1"
EVENT_SCHEMA = "host-scm-merge-event/v1"
STATE_SCHEMA = "host-post-merge-resume-state/v1"
TRANSITION_SCHEMA = "host-post-merge-transition-receipt/v1"
POLLER_SCHEMA = "host-post-merge-resume-poller/v1"
POLLER_STATE_SCHEMA = "host-post-merge-resume-poller-state/v1"
PENDING_WATCH_SCHEMA = "host-post-merge-pending-watch/v1"
BINDING_REPAIR_SCHEMA = "host-post-merge-pending-watch-binding-repair/v1"
SUPERSEDE_RECEIPT_SCHEMA = "host-post-merge-pending-watch-supersede/v1"
SCHEDULER_SCHEMA = "host-post-merge-controller-scheduler/v1"
EVENT_PORT = "scm_merge_event"
DEFAULT_GOAL_ID = "LH-EXAMPLE-GOAL-001"
DEFAULT_GOAL_REVISION = 5
DEFAULT_NODE_ID = "R3"
DEFAULT_SUCCESSOR_NODE_ID = "successor"
REQUIRED_BINDING = (
    "repository_id",
    "goal_id",
    "goal_revision",
    "node_id",
    "pr_number",
    "expected_head_sha",
    "merge_sha",
)
POLL_REQUIRED_BINDING = (
    "repository_id",
    "goal_id",
    "goal_revision",
    "node_id",
    "pr_number",
    "expected_head_sha",
)
SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
PENDING_CI_STATES = frozenset({"QUEUED", "IN_PROGRESS", "PENDING", "RUNNING", "WAITING"})
SUCCESS_CI_STATES = frozenset({"SUCCESS", "SUCCEEDED", "PASS", "PASSED"})
COORDINATOR_CHILD_SCHEMA = "host-coordinator-child-lease/v1"
COORDINATOR_CHILD_LEASE_KIND = "node_scoped_generation_fenced"
COORDINATOR_CHILD_REQUIRED_BINDING = (
    "repository_id",
    "project_id",
    "goal_id",
    "goal_revision",
    "coordinator_id",
    "parent_worktree",
    "parent_lease_generation",
    "parent_fence_token_digest",
    "child_node_id",
    "child_packet_digest",
    "child_worktree",
    "child_branch",
)
LINEAGE_PERMANENT_REASONS = frozenset({
    "foreign_coordinator",
    "path_only_identity",
    "stale_generation",
    "stale_fence_digest",
    "child_idempotency_conflict",
    "child_lease_schema_invalid",
    "child_lease_kind_invalid",
    "child_lease_expired",
    "child_lease_binding_mismatch",
    "child_session_id_missing",
    "child_fence_digest_mismatch",
})
DEFAULT_MAX_POLL_ATTEMPTS = 6
DEFAULT_INITIAL_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_POLL_CLAIM_SECONDS = 30.0


class PostMergeResumeError(ValueError):
    """A contract, state, or callback result cannot be trusted."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


MergeReader = Callable[[Mapping[str, Any]], Mapping[str, Any]]
CIReader = Callable[[Mapping[str, Any]], Mapping[str, Any]]
GenerationAcquirer = Callable[[Mapping[str, Any]], Mapping[str, Any] | int]
Projector = Callable[[Mapping[str, Any]], Mapping[str, Any]]
SuccessorDispatcher = Callable[[Mapping[str, Any]], Mapping[str, Any]]
PollMergeReader = Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
PollCIReader = Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
PollerFactory = Callable[..., "BoundedPostMergeResumePoller"]
LineageAdmission = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def digest_json(value: Any) -> str:
    """Digest JSON values using the repository's canonical JSON ordering."""
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PostMergeResumeError("json_value_invalid") from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _copy_json(name: str, value: Any) -> Any:
    try:
        result = copy.deepcopy(value)
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return result
    except (TypeError, ValueError) as exc:
        raise PostMergeResumeError(f"{name}_not_json") from exc


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PostMergeResumeError(f"{name}_missing")
    return value.strip()


def _sha(name: str, value: Any) -> str:
    value = _text(name, value)
    if SHA_RE.fullmatch(value) is None:
        raise PostMergeResumeError(f"{name}_invalid")
    return value.lower()


def _digest(name: str, value: Any) -> str:
    value = _text(name, value).lower()
    if DIGEST_RE.fullmatch(value) is None:
        raise PostMergeResumeError(f"{name}_invalid")
    return value


def _positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PostMergeResumeError(f"{name}_invalid")
    return value


def _pr_number(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PostMergeResumeError("pr_number_invalid")
    return value


def _normalise_binding(
    binding: Mapping[str, Any],
    *,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        raise PostMergeResumeError("event_binding_missing")
    result = {
        "repository_id": _text("repository_id", binding.get("repository_id")),
        "goal_id": _text("goal_id", binding.get("goal_id")),
        "goal_revision": _positive_int("goal_revision", binding.get("goal_revision")),
        "node_id": _text("node_id", binding.get("node_id")),
        "pr_number": _pr_number(binding.get("pr_number")),
        "expected_head_sha": _sha("expected_head_sha", binding.get("expected_head_sha")),
        "merge_sha": _sha("merge_sha", binding.get("merge_sha")),
    }
    if expected is not None:
        expected_binding = _normalise_binding(expected)
        mismatches = {
            key: {"expected": expected_binding[key], "observed": result[key]}
            for key in REQUIRED_BINDING
            if result[key] != expected_binding[key]
        }
        if mismatches:
            raise PostMergeResumeError("event_binding_mismatch")
    return result


def _normalise_poll_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the identity a poller can know before merge discovery."""
    if not isinstance(binding, Mapping):
        raise PostMergeResumeError("poll_binding_missing")
    result = {
        "repository_id": _text("repository_id", binding.get("repository_id")),
        "goal_id": _text("goal_id", binding.get("goal_id")),
        "goal_revision": _positive_int("goal_revision", binding.get("goal_revision")),
        "node_id": _text("node_id", binding.get("node_id")),
        "pr_number": _pr_number(binding.get("pr_number")),
        "expected_head_sha": _sha("expected_head_sha", binding.get("expected_head_sha")),
    }
    if binding.get("merge_sha") is not None:
        result["merge_sha"] = _sha("merge_sha", binding.get("merge_sha"))
    return result


def build_merge_event(
    binding: Mapping[str, Any],
    *,
    event_id: str | None = None,
    generation: int | None = None,
    merged_at: str = "owner-merge-observed",
    source: str = "scm",
) -> dict[str, Any]:
    """Build a digest-bound event suitable for the ``scm_merge_event`` port."""
    normalized = _normalise_binding(binding)
    event = {
        "schema": EVENT_SCHEMA,
        "event_port": EVENT_PORT,
        "event_id": _text("event_id", event_id or f"scm-event-{uuid.uuid4().hex}"),
        "source": _text("source", source),
        "owner_merge": True,
        "merged_at": _text("merged_at", merged_at),
        "binding": normalized,
        "binding_digest": digest_json(normalized),
    }
    if generation is not None:
        event["generation"] = _positive_int("generation", generation)
    event["event_digest"] = digest_json(event)
    return event


def _event_body(event: Mapping[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(dict(event))
    body.pop("event_digest", None)
    return body


def validate_merge_event(
    event: Mapping[str, Any],
    *,
    expected_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate event bytes and return a normalized, digest-checked event."""
    if not isinstance(event, Mapping) or event.get("schema") != EVENT_SCHEMA:
        raise PostMergeResumeError("event_schema_invalid")
    if event.get("event_port") != EVENT_PORT:
        raise PostMergeResumeError("event_port_invalid")
    event_id = _text("event_id", event.get("event_id"))
    source = _text("source", event.get("source"))
    if event.get("owner_merge") is not True:
        raise PostMergeResumeError("owner_merge_evidence_missing")
    _text("merged_at", event.get("merged_at"))
    binding = _normalise_binding(event.get("binding"), expected=expected_binding)
    binding_digest = _digest("binding_digest", event.get("binding_digest"))
    if binding_digest != digest_json(binding):
        raise PostMergeResumeError("binding_digest_mismatch")
    supplied_digest = _digest("event_digest", event.get("event_digest"))
    body = _event_body(event)
    body["binding"] = binding
    body["binding_digest"] = binding_digest
    if supplied_digest != digest_json(body):
        raise PostMergeResumeError("event_digest_mismatch")
    normalized: dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "event_port": EVENT_PORT,
        "event_id": event_id,
        "source": source,
        "owner_merge": True,
        "merged_at": str(event["merged_at"]).strip(),
        "binding": binding,
        "binding_digest": binding_digest,
        "event_digest": supplied_digest,
    }
    if "generation" in event:
        normalized["generation"] = _positive_int("generation", event.get("generation"))
    return normalized


def _file_digest(value: Mapping[str, Any]) -> str:
    return digest_json(dict(value))


def _read_merge(
    event: Mapping[str, Any],
    binding: Mapping[str, Any],
    reader: MergeReader | None,
) -> dict[str, Any]:
    raw: Any
    if reader is None:
        raw = event.get("merge_readback")
    else:
        raw = reader(copy.deepcopy(dict(event)))
    if not isinstance(raw, Mapping):
        raise PostMergeResumeError("merge_readback_missing")
    repository_id = raw.get("repository_id")
    if repository_id != binding["repository_id"]:
        raise PostMergeResumeError("merge_readback_repository_mismatch")
    pr_number = raw.get("pr_number", raw.get("number"))
    if pr_number != binding["pr_number"]:
        raise PostMergeResumeError("merge_readback_pr_mismatch")
    state = str(raw.get("state", "")).upper()
    merged = raw.get("merged") is True or state == "MERGED"
    if not merged:
        raise PostMergeResumeError("merge_not_observed")
    merge_sha_value = raw.get("merge_sha")
    if merge_sha_value is None and isinstance(raw.get("merge_commit"), Mapping):
        merge_sha_value = raw["merge_commit"].get("oid")
    merge_sha = _sha("merge_readback.merge_sha", merge_sha_value)
    if merge_sha != binding["merge_sha"]:
        raise PostMergeResumeError("merge_readback_sha_mismatch")
    head_sha_value = raw.get("head_sha", raw.get("headRefOid"))
    head_sha = _sha("merge_readback.head_sha", head_sha_value)
    if head_sha != binding["expected_head_sha"]:
        raise PostMergeResumeError("merge_readback_head_mismatch")
    merged_at = raw.get("merged_at", raw.get("mergedAt"))
    _text("merge_readback.merged_at", merged_at)
    normalized = _copy_json("merge_readback", dict(raw))
    normalized.update(
        {
            "repository_id": binding["repository_id"],
            "pr_number": binding["pr_number"],
            "state": "MERGED",
            "merge_sha": merge_sha,
            "head_sha": head_sha,
            "merged_at": str(merged_at).strip(),
        }
    )
    normalized["readback_digest"] = _file_digest(
        {key: value for key, value in normalized.items() if key != "readback_digest"}
    )
    return normalized


def _ci_state(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_ci(
    event: Mapping[str, Any],
    binding: Mapping[str, Any],
    merge: Mapping[str, Any],
    reader: CIReader | None,
) -> tuple[str, dict[str, Any] | None]:
    raw: Any
    if reader is None:
        raw = event.get("ci_readback")
    else:
        raw = reader(
            {
                "event": copy.deepcopy(dict(event)),
                "binding": copy.deepcopy(dict(binding)),
                "merge_readback": copy.deepcopy(dict(merge)),
            }
        )
    if not isinstance(raw, Mapping):
        raise PostMergeResumeError("ci_readback_missing")
    repository_id = raw.get("repository_id")
    if repository_id != binding["repository_id"]:
        raise PostMergeResumeError("ci_readback_repository_mismatch")
    sha_value = raw.get("sha", raw.get("head_sha", raw.get("merge_sha")))
    sha = _sha("ci_readback.sha", sha_value)
    if sha != binding["merge_sha"]:
        raise PostMergeResumeError("ci_readback_sha_mismatch")
    checks = raw.get("checks")
    if checks is not None:
        if not isinstance(checks, list) or not checks:
            raise PostMergeResumeError("ci_checks_invalid")
        normalized_checks: list[dict[str, Any]] = []
        pending = False
        for check in checks:
            if not isinstance(check, Mapping):
                raise PostMergeResumeError("ci_check_invalid")
            check_state = _ci_state(check.get("conclusion", check.get("state", check.get("status"))))
            if check_state in PENDING_CI_STATES or not check_state:
                pending = True
            elif check_state not in SUCCESS_CI_STATES:
                normalized_checks.append(_copy_json("ci_check", dict(check)))
                result = _copy_json("ci_readback", dict(raw))
                result["checks"] = normalized_checks + [dict(check)]
                result["readback_digest"] = _file_digest(
                    {key: value for key, value in result.items() if key != "readback_digest"}
                )
                return "rejected", result
            normalized_checks.append(_copy_json("ci_check", dict(check)))
        normalized = _copy_json("ci_readback", dict(raw))
        normalized["sha"] = sha
        normalized["checks"] = normalized_checks
        normalized["status"] = "pending" if pending else "success"
        normalized["readback_digest"] = _file_digest(
            {key: value for key, value in normalized.items() if key != "readback_digest"}
        )
        return ("waiting" if pending else "green"), normalized

    conclusion = _ci_state(raw.get("conclusion", raw.get("status")))
    if conclusion in PENDING_CI_STATES or not conclusion:
        normalized = _copy_json("ci_readback", dict(raw))
        normalized["sha"] = sha
        normalized["status"] = "pending"
        normalized["readback_digest"] = _file_digest(
            {key: value for key, value in normalized.items() if key != "readback_digest"}
        )
        return "waiting", normalized
    normalized = _copy_json("ci_readback", dict(raw))
    normalized["sha"] = sha
    normalized["status"] = "success" if conclusion in SUCCESS_CI_STATES else "failure"
    normalized["readback_digest"] = _file_digest(
        {key: value for key, value in normalized.items() if key != "readback_digest"}
    )
    return ("green" if conclusion in SUCCESS_CI_STATES else "rejected"), normalized


def _read_polled_merge(
    raw: Mapping[str, Any] | None,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a merge readback before the poller creates an event."""
    if not isinstance(raw, Mapping):
        raise PostMergeResumeError("merge_readback_missing")
    if raw.get("repository_id") != binding["repository_id"]:
        raise PostMergeResumeError("merge_readback_repository_mismatch")
    pr_number = raw.get("pr_number", raw.get("number"))
    if pr_number != binding["pr_number"]:
        raise PostMergeResumeError("merge_readback_pr_mismatch")
    state = str(raw.get("state", "")).upper()
    if not (raw.get("merged") is True or state == "MERGED"):
        raise PostMergeResumeError("merge_not_observed")
    merge_sha_value = raw.get("merge_sha")
    if merge_sha_value is None and isinstance(raw.get("merge_commit"), Mapping):
        merge_sha_value = raw["merge_commit"].get("oid")
    merge_sha = _sha("merge_readback.merge_sha", merge_sha_value)
    expected_merge_sha = binding.get("merge_sha")
    if expected_merge_sha is not None and merge_sha != expected_merge_sha:
        raise PostMergeResumeError("merge_readback_sha_mismatch")
    head_sha_value = raw.get("head_sha", raw.get("headRefOid"))
    head_sha = _sha("merge_readback.head_sha", head_sha_value)
    if head_sha != binding["expected_head_sha"]:
        raise PostMergeResumeError("merge_readback_head_mismatch")
    merged_at = _text("merge_readback.merged_at", raw.get("merged_at", raw.get("mergedAt")))
    normalized = _copy_json("merge_readback", dict(raw))
    normalized.update(
        {
            "repository_id": binding["repository_id"],
            "pr_number": binding["pr_number"],
            "state": "MERGED",
            "merge_sha": merge_sha,
            "head_sha": head_sha,
            "merged_at": merged_at,
        }
    )
    normalized["readback_digest"] = _file_digest(
        {key: value for key, value in normalized.items() if key != "readback_digest"}
    )
    return normalized


def _production_state_root() -> Path:
    configured = os.environ.get("LH_HOST_STATE_ROOT")
    return (
        Path(configured).expanduser().resolve()
        if configured
        else (Path.home() / ".local" / "state" / "external-host").resolve()
    )


def _reject_reason(exc: BaseException) -> str:
    if isinstance(exc, PostMergeResumeError):
        return exc.reason
    return f"callback_failed:{type(exc).__name__}"


def _mapping_result(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PostMergeResumeError(f"{name}_invalid")
    return _copy_json(name, dict(value))


class PostMergeResumeController:
    """Consume SCM merge events and apply one durable successor transition."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        expected_binding: Mapping[str, Any],
        successor_node_id: str = DEFAULT_SUCCESSOR_NODE_ID,
        initial_generation: int = 1,
        allow_production_state_root: bool = False,
    ):
        self.state_root = Path(state_root).expanduser().resolve()
        if (
            not allow_production_state_root
            and (self.state_root == _production_state_root() or self.state_root.is_relative_to(_production_state_root()))
        ):
            raise PostMergeResumeError("production_state_root_forbidden")
        self.state_path = self.state_root / "post-merge-resume-state.json"
        self.lock_path = self.state_root / "post-merge-resume-state.lock"
        self.binding = _normalise_binding(expected_binding)
        self.successor_node_id = _text("successor_node_id", successor_node_id)
        self.initial_generation = _positive_int("initial_generation", initial_generation)

    @contextmanager
    def _lock(self):
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with locked_file(self.lock_path):
            yield

    def _new_state(self) -> dict[str, Any]:
        return {
            "schema": STATE_SCHEMA,
            "controller_schema": SCHEMA,
            "goal_id": self.binding["goal_id"],
            "goal_revision": self.binding["goal_revision"],
            "node_id": self.binding["node_id"],
            "successor_node_id": self.successor_node_id,
            "binding": copy.deepcopy(self.binding),
            "binding_digest": digest_json(self.binding),
            "status": "waiting",
            "predecessor_state": "candidate",
            "current_generation": self.initial_generation,
            "deliveries": {},
            "transitions": [],
            "successor_dispatches": {},
            "metrics": {
                "events_received": 0,
                "events_rejected": 0,
                "transitions_applied": 0,
                "duplicate_transitions": 0,
                "manual_prompts": 0,
                "successor_dispatches": 0,
                "provider_invocations": 0,
            },
        }

    def _read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._new_state()
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PostMergeResumeError("state_unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
            raise PostMergeResumeError("state_schema_invalid")
        supplied = value.get("state_digest")
        body = {key: item for key, item in value.items() if key != "state_digest"}
        if supplied != digest_json(body):
            raise PostMergeResumeError("state_digest_mismatch")
        if value.get("goal_id") != self.binding["goal_id"] or value.get("goal_revision") != self.binding["goal_revision"]:
            raise PostMergeResumeError("state_goal_identity_mismatch")
        if value.get("node_id") != self.binding["node_id"] or value.get("binding_digest") != digest_json(self.binding):
            raise PostMergeResumeError("state_binding_mismatch")
        return value

    def _write_state(self, state: Mapping[str, Any]) -> None:
        body = copy.deepcopy(dict(state))
        body.pop("state_digest", None)
        body["state_digest"] = digest_json(body)
        payload = (json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        descriptor, raw_temp_path = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.",
            suffix=".tmp",
            dir=self.state_root,
        )
        temp_path = Path(raw_temp_path)
        try:
            make_file_private(descriptor, temp_path)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.state_path)
            sync_directory(self.state_root)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temp_path.exists():
                temp_path.unlink()

    @staticmethod
    def _metrics(state: Mapping[str, Any]) -> dict[str, int]:
        raw = state.get("metrics") if isinstance(state.get("metrics"), Mapping) else {}
        return {
            key: int(raw.get(key, 0))
            for key in (
                "events_received",
                "events_rejected",
                "transitions_applied",
                "duplicate_transitions",
                "manual_prompts",
                "successor_dispatches",
                "provider_invocations",
            )
        }

    def _result(
        self,
        status: str,
        *,
        reason: str | None = None,
        state: Mapping[str, Any] | None = None,
        **values: Any,
    ) -> dict[str, Any]:
        metrics = self._metrics(state or self._new_state())
        result: dict[str, Any] = {
            "schema": SCHEMA,
            "status": status,
            "goal_id": self.binding["goal_id"],
            "goal_revision": self.binding["goal_revision"],
            "node_id": self.binding["node_id"],
            "successor_node_id": self.successor_node_id,
            "metrics": metrics,
            "manual_prompts": metrics["manual_prompts"],
            "provider_invocations": metrics["provider_invocations"],
            "goal_created": 0,
            "runs_created": 0,
            "attempts_created": 0,
        }
        if reason:
            result["reason"] = reason
        result.update(_copy_json("result_values", values))
        return result

    @staticmethod
    def _event_identity(event: Mapping[str, Any]) -> tuple[str, str, str]:
        return (
            str(event["event_id"]),
            str(event["event_digest"]),
            digest_json(event["binding"]),
        )

    @staticmethod
    def _record_delivery(
        state: dict[str, Any],
        event: Mapping[str, Any],
        *,
        status: str,
        reason: str | None = None,
        transition_key: str | None = None,
    ) -> None:
        deliveries = state.setdefault("deliveries", {})
        record: dict[str, Any] = {
            "event_id": event["event_id"],
            "event_digest": event["event_digest"],
            "binding_digest": event["binding_digest"],
            "transition_key": transition_key,
            "status": status,
        }
        if reason:
            record["reason"] = reason
        deliveries[event["event_digest"]] = record

    @staticmethod
    def _bump(state: dict[str, Any], key: str) -> None:
        metrics = state.setdefault("metrics", {})
        metrics[key] = int(metrics.get(key, 0)) + 1

    def _default_generation(self, request: Mapping[str, Any], current: int) -> dict[str, Any]:
        del request
        return {"generation": current + 1, "lease_id": f"r3-generation-{current + 1}"}

    def _default_projection(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "status": "projected",
            "first_actionable": self.successor_node_id,
            "transition_key": request["transition_key"],
        }

    @staticmethod
    def _default_dispatch(request: Mapping[str, Any]) -> dict[str, Any]:
        return {"status": "dispatched", "dispatch_key": request["dispatch_key"]}

    def _lease_generation(self, value: Mapping[str, Any] | int, current: int) -> tuple[int, dict[str, Any]]:
        if isinstance(value, int) and not isinstance(value, bool):
            generation = value
            lease = {"generation": generation}
        elif isinstance(value, Mapping):
            lease = _mapping_result("generation_lease", value)
            generation = lease.get("generation", lease.get("lease_generation"))
        else:
            raise PostMergeResumeError("generation_lease_invalid")
        generation = _positive_int("generation", generation)
        if generation <= current:
            raise PostMergeResumeError("generation_not_advanced")
        lease["generation"] = generation
        return generation, lease

    def _pending_dispatch(
        self,
        state: dict[str, Any],
        transition: dict[str, Any],
        dispatcher: SuccessorDispatcher,
    ) -> dict[str, Any] | None:
        dispatch = transition.get("successor_dispatch")
        if not isinstance(dispatch, Mapping) or dispatch.get("status") != "pending":
            return None
        try:
            receipt = _mapping_result("successor_dispatch_receipt", dispatcher(copy.deepcopy(dict(dispatch["request"]))))
        except Exception as exc:  # callback errors remain observable and retriable
            dispatch["last_error"] = _reject_reason(exc)
            dispatch_key = dispatch.get("dispatch_key")
            ledger = state.get("successor_dispatches", {}).get(dispatch_key)
            if isinstance(ledger, dict):
                ledger["last_error"] = dispatch["last_error"]
            self._write_state(state)
            return self._result(
                "waiting",
                reason="successor_dispatch_pending",
                state=state,
                transition_receipt=transition.get("transition_receipt"),
            )
        dispatch["status"] = "dispatched"
        dispatch["receipt"] = receipt
        dispatch["receipt_digest"] = digest_json(receipt)
        transition["successor_dispatch"] = dispatch
        dispatch_key = dispatch.get("dispatch_key")
        ledger = state.get("successor_dispatches", {}).get(dispatch_key)
        if isinstance(ledger, dict):
            ledger.update(
                {
                    "status": "dispatched",
                    "receipt_digest": dispatch["receipt_digest"],
                }
            )
        self._write_state(state)
        return self._result(
            "resumed",
            state=state,
            transition_receipt=transition.get("transition_receipt"),
            successor_dispatch_receipt=receipt,
            replayed=True,
        )

    def handle_event(
        self,
        event: Mapping[str, Any],
        *,
        merge_reader: MergeReader | None = None,
        ci_reader: CIReader | None = None,
        acquire_generation: GenerationAcquirer | None = None,
        project_first_actionable: Projector | None = None,
        dispatch_successor: SuccessorDispatcher | None = None,
    ) -> dict[str, Any]:
        """Read back one event and apply at most one successor transition."""
        try:
            normalized = validate_merge_event(event, expected_binding=self.binding)
        except PostMergeResumeError as exc:
            return self._result("rejected", reason=exc.reason)

        merge_reader = merge_reader
        ci_reader = ci_reader
        acquire_generation = acquire_generation or (lambda request: self._default_generation(request, int(request["current_generation"])))
        project_first_actionable = project_first_actionable or self._default_projection
        dispatch_successor = dispatch_successor or self._default_dispatch
        transition_key = digest_json(self.binding)
        event_id, event_digest, binding_digest = self._event_identity(normalized)
        with self._lock():
            state = self._read_state()
            self._bump(state, "events_received")
            deliveries = state.setdefault("deliveries", {})
            prior_by_id = [
                item
                for item in deliveries.values()
                if isinstance(item, Mapping) and item.get("event_id") == event_id
            ]
            if prior_by_id and all(item.get("event_digest") != event_digest for item in prior_by_id):
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason="event_id_conflict", transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason="event_id_conflict", state=state)

            transition_list = state.get("transitions")
            transitions = transition_list if isinstance(transition_list, list) else []
            existing_transition = next(
                (
                    item
                    for item in transitions
                    if isinstance(item, Mapping) and item.get("transition_key") == transition_key
                ),
                None,
            )
            if existing_transition is not None:
                if isinstance(existing_transition, dict) and existing_transition.get("successor_dispatch", {}).get("status") == "pending":
                    pending = self._pending_dispatch(state, existing_transition, dispatch_successor)
                    if pending is not None:
                        return pending
                self._bump(state, "duplicate_transitions")
                self._record_delivery(state, normalized, status="duplicate", transition_key=transition_key)
                self._write_state(state)
                return self._result(
                    "duplicate",
                    reason="transition_already_applied",
                    state=state,
                    transition_receipt=existing_transition.get("transition_receipt"),
                    replayed=True,
                )

            prior = deliveries.get(event_digest)
            if isinstance(prior, Mapping) and prior.get("status") == "rejected":
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason=str(prior.get("reason") or "event_previously_rejected"), transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason=str(prior.get("reason") or "event_previously_rejected"), state=state, replayed=True)

            current_generation = _positive_int("current_generation", state.get("current_generation"))
            event_generation = normalized.get("generation")
            if event_generation is not None and event_generation != current_generation:
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason="generation_mismatch", transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason="generation_mismatch", state=state)
            if state.get("status") == "resumed":
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason="predecessor_already_integrated", transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason="predecessor_already_integrated", state=state)

            try:
                merge = _read_merge(normalized, self.binding, merge_reader)
            except Exception as exc:
                reason = _reject_reason(exc)
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason=reason, transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason=reason, state=state)

            try:
                ci_status, ci = _read_ci(normalized, self.binding, merge, ci_reader)
            except Exception as exc:
                reason = _reject_reason(exc)
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason=reason, transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason=reason, state=state, merge_readback=merge)

            if ci_status == "waiting":
                self._record_delivery(state, normalized, status="waiting", transition_key=transition_key)
                self._write_state(state)
                return self._result(
                    "waiting",
                    reason="ci_pending",
                    state=state,
                    merge_readback=merge,
                    ci_readback=ci,
                    replayed=bool(prior),
                )
            if ci_status != "green":
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason="ci_not_green", transition_key=transition_key)
                self._write_state(state)
                return self._result(
                    "rejected",
                    reason="ci_not_green",
                    state=state,
                    merge_readback=merge,
                    ci_readback=ci,
                )
            assert ci is not None

            lease_request = {
                "event_id": event_id,
                "event_digest": event_digest,
                "idempotency_key": event_id,
                "lease_kind": "post_merge_resume",
                "binding": copy.deepcopy(self.binding),
                "binding_digest": binding_digest,
                "transition_key": transition_key,
                "current_generation": current_generation,
            }
            try:
                lease_raw = acquire_generation(copy.deepcopy(lease_request))
                new_generation, lease = self._lease_generation(lease_raw, current_generation)
            except Exception as exc:
                reason = _reject_reason(exc)
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason=reason, transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason=reason, state=state, merge_readback=merge, ci_readback=ci)

            dispatch_key = "successor-" + transition_key.removeprefix("sha256:")[:32]
            projection_request = {
                "event": copy.deepcopy(normalized),
                "binding": copy.deepcopy(self.binding),
                "idempotency_key": event_id,
                "transition_key": transition_key,
                "merge_readback": copy.deepcopy(merge),
                "ci_readback": copy.deepcopy(ci),
                "lease": copy.deepcopy(lease),
                "successor_node_id": self.successor_node_id,
            }
            try:
                projection = _mapping_result("first_actionable_projection", project_first_actionable(copy.deepcopy(projection_request)))
                if projection.get("status") != "projected":
                    raise PostMergeResumeError("first_actionable_not_projected")
                if projection.get("first_actionable") != self.successor_node_id:
                    raise PostMergeResumeError("successor_projection_mismatch")
            except Exception as exc:
                reason = _reject_reason(exc)
                self._bump(state, "events_rejected")
                self._record_delivery(state, normalized, status="rejected", reason=reason, transition_key=transition_key)
                self._write_state(state)
                return self._result("rejected", reason=reason, state=state, merge_readback=merge, ci_readback=ci, lease=lease)

            transition_id = "transition-" + uuid.uuid4().hex
            transition_body: dict[str, Any] = {
                "schema": TRANSITION_SCHEMA,
                "transition_id": transition_id,
                "transition_key": transition_key,
                "event_id": event_id,
                "event_digest": event_digest,
                "binding_digest": binding_digest,
                "from_state": "candidate",
                "to_state": "integrated",
                "predecessor_node_id": self.binding["node_id"],
                "successor_node_id": self.successor_node_id,
                "lease_generation": new_generation,
                "lease": lease,
                "merge_readback_digest": merge["readback_digest"],
                "ci_readback_digest": ci["readback_digest"],
                "first_actionable": self.successor_node_id,
                "projection": projection,
            }
            transition_receipt = copy.deepcopy(transition_body)
            transition_receipt["transition_digest"] = digest_json(transition_body)
            dispatch_request = {
                "dispatch_key": dispatch_key,
                "idempotency_key": event_id,
                "transition_id": transition_id,
                "transition_digest": transition_receipt["transition_digest"],
                "goal_id": self.binding["goal_id"],
                "goal_revision": self.binding["goal_revision"],
                "predecessor_node_id": self.binding["node_id"],
                "successor_node_id": self.successor_node_id,
                "lease_generation": new_generation,
                "first_actionable": self.successor_node_id,
                "event_digest": event_digest,
            }
            transition: dict[str, Any] = {
                **transition_body,
                "transition_receipt": transition_receipt,
                "successor_dispatch": {
                    "dispatch_key": dispatch_key,
                    "status": "pending",
                    "request": dispatch_request,
                },
            }
            state["status"] = "resumed"
            state["predecessor_state"] = "integrated"
            state["current_generation"] = new_generation
            state["first_actionable"] = self.successor_node_id
            state.setdefault("transitions", []).append(transition)
            state.setdefault("successor_dispatches", {})[dispatch_key] = {
                "status": "pending",
                "transition_id": transition_id,
                "transition_digest": transition_receipt["transition_digest"],
            }
            self._bump(state, "transitions_applied")
            self._bump(state, "successor_dispatches")
            self._record_delivery(state, normalized, status="resumed", transition_key=transition_key)
            self._write_state(state)

            try:
                dispatch_receipt = _mapping_result("successor_dispatch_receipt", dispatch_successor(copy.deepcopy(dispatch_request)))
            except Exception as exc:
                transition["successor_dispatch"]["last_error"] = _reject_reason(exc)
                self._write_state(state)
                return self._result(
                    "waiting",
                    reason="successor_dispatch_pending",
                    state=state,
                    transition_receipt=transition_receipt,
                    merge_readback=merge,
                    ci_readback=ci,
                    lease=lease,
                )

            transition["successor_dispatch"].update(
                {
                    "status": "dispatched",
                    "receipt": dispatch_receipt,
                    "receipt_digest": digest_json(dispatch_receipt),
                }
            )
            state["successor_dispatches"][dispatch_key].update(
                {
                    "status": "dispatched",
                    "receipt_digest": transition["successor_dispatch"]["receipt_digest"],
                }
            )
            self._write_state(state)
            return self._result(
                "resumed",
                state=state,
                transition_receipt=transition_receipt,
                merge_readback=merge,
                ci_readback=ci,
                lease=lease,
                successor_dispatch_receipt=dispatch_receipt,
                replayed=False,
            )

    def read_state(self) -> dict[str, Any]:
        """Read the digest-bound state without changing it."""
        with self._lock():
            return copy.deepcopy(self._read_state())


class BoundedPostMergeResumePoller:
    """Run one provider-neutral post-merge recovery poll and then exit.

    The poller is deliberately a one-shot adapter.  It creates a durable
    pending request before any SCM read, derives a stable event id from the
    supplied idempotency key, and delegates the generation-fenced transition
    to :class:`PostMergeResumeController`.  A later invocation reuses that
    request; it never starts a resident loop or invokes a model/provider.
    """

    def __init__(
        self,
        state_root: str | Path,
        *,
        expected_binding: Mapping[str, Any],
        idempotency_key: str,
        successor_node_id: str = DEFAULT_SUCCESSOR_NODE_ID,
        initial_generation: int = 1,
        allow_production_state_root: bool = False,
    ):
        self.state_root = Path(state_root).expanduser().resolve()
        production_root = _production_state_root()
        if (
            not allow_production_state_root
            and (self.state_root == production_root or self.state_root.is_relative_to(production_root))
        ):
            raise PostMergeResumeError("production_state_root_forbidden")
        self.allow_production_state_root = allow_production_state_root
        self.binding = _normalise_poll_binding(expected_binding)
        self.idempotency_key = _text("idempotency_key", idempotency_key)
        self.successor_node_id = _text("successor_node_id", successor_node_id)
        self.initial_generation = _positive_int("initial_generation", initial_generation)
        self.state_path = self.state_root / "post-merge-resume-poller-state.json"
        self.lock_path = self.state_root / "post-merge-resume-poller-state.lock"

    @contextmanager
    def _lock(self):
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with locked_file(self.lock_path):
            yield

    def _new_state(self) -> dict[str, Any]:
        return {
            "schema": POLLER_STATE_SCHEMA,
            "poller_schema": POLLER_SCHEMA,
            "binding": copy.deepcopy(self.binding),
            "binding_digest": digest_json(self.binding),
            "successor_node_id": self.successor_node_id,
            "idempotency_key": self.idempotency_key,
            "pending_resume": True,
            "status": "pending",
            "poll_count": 0,
            "lease_generation": self.initial_generation,
            "lease": None,
            "event": None,
            "merge_readback": None,
            "ci_readback": None,
            "controller_result": None,
            "transition_receipt": None,
            "successor_dispatch_receipt": None,
            "last_result": None,
        }

    def _read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._new_state()
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PostMergeResumeError("poller_state_unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != POLLER_STATE_SCHEMA:
            raise PostMergeResumeError("poller_state_schema_invalid")
        supplied = value.get("state_digest")
        body = {key: item for key, item in value.items() if key != "state_digest"}
        if supplied != digest_json(body):
            raise PostMergeResumeError("poller_state_digest_mismatch")
        if value.get("poller_schema") != POLLER_SCHEMA:
            raise PostMergeResumeError("poller_schema_mismatch")
        if value.get("idempotency_key") != self.idempotency_key:
            raise PostMergeResumeError("poll_idempotency_conflict")
        if value.get("successor_node_id") != self.successor_node_id:
            raise PostMergeResumeError("poll_successor_mismatch")
        try:
            stored_binding = _normalise_poll_binding(value.get("binding"))
        except PostMergeResumeError as exc:
            raise PostMergeResumeError("poll_binding_invalid") from exc
        for key in POLL_REQUIRED_BINDING:
            if stored_binding.get(key) != self.binding.get(key):
                raise PostMergeResumeError("poll_binding_mismatch")
        if self.binding.get("merge_sha") is not None and stored_binding.get("merge_sha") != self.binding.get("merge_sha"):
            raise PostMergeResumeError("poll_binding_mismatch")
        if value.get("binding_digest") != digest_json(stored_binding):
            raise PostMergeResumeError("poll_binding_digest_mismatch")
        if not isinstance(value.get("pending_resume"), bool):
            raise PostMergeResumeError("poll_pending_resume_invalid")
        if not isinstance(value.get("status"), str) or not value["status"].strip():
            raise PostMergeResumeError("poll_status_invalid")
        _positive_int("lease_generation", value.get("lease_generation"))
        poll_count = value.get("poll_count")
        if isinstance(poll_count, bool) or not isinstance(poll_count, int) or poll_count < 0:
            raise PostMergeResumeError("poll_count_invalid")
        return value

    def _write_state(self, state: Mapping[str, Any]) -> None:
        body = copy.deepcopy(dict(state))
        body.pop("state_digest", None)
        body["state_digest"] = digest_json(body)
        payload = (
            json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        descriptor, raw_temp_path = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.",
            suffix=".tmp",
            dir=self.state_root,
        )
        temp_path = Path(raw_temp_path)
        try:
            make_file_private(descriptor, temp_path)
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.state_path)
            sync_directory(self.state_root)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temp_path.exists():
                temp_path.unlink()

    def _result(
        self,
        state: Mapping[str, Any],
        status: str,
        *,
        reason: str | None = None,
        **values: Any,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": POLLER_SCHEMA,
            "status": status,
            "idempotency_key": self.idempotency_key,
            "goal_id": self.binding["goal_id"],
            "goal_revision": self.binding["goal_revision"],
            "node_id": self.binding["node_id"],
            "successor_node_id": self.successor_node_id,
            "pending_resume": bool(state.get("pending_resume")),
            "poll_count": int(state.get("poll_count", 0)),
            "manual_prompts": 0,
            "provider_invocations": 0,
            "goal_created": 0,
            "runs_created": 0,
            "attempts_created": 0,
        }
        if reason:
            result["reason"] = reason
        result.update(_copy_json("poll_result_values", values))
        return result

    def _save_result(
        self,
        state: dict[str, Any],
        status: str,
        *,
        pending: bool,
        reason: str,
        **values: Any,
    ) -> dict[str, Any]:
        state["status"] = status
        state["pending_resume"] = pending
        state["last_result"] = {
            "status": status,
            "reason": reason,
            **_copy_json("last_result_values", values),
        }
        self._write_state(state)
        return self._result(state, status, reason=reason, **values)

    def _controller(self, binding: Mapping[str, Any]) -> PostMergeResumeController:
        return PostMergeResumeController(
            self.state_root,
            expected_binding=binding,
            successor_node_id=self.successor_node_id,
            initial_generation=self.initial_generation,
            allow_production_state_root=self.allow_production_state_root,
        )

    @staticmethod
    def _transition(
        controller_state: Mapping[str, Any],
        binding: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        transition_key = digest_json(binding)
        transitions = controller_state.get("transitions")
        if not isinstance(transitions, list):
            return None
        for item in reversed(transitions):
            if isinstance(item, Mapping) and item.get("transition_key") == transition_key:
                return dict(item)
        return None

    def _stored_resume(
        self,
        state: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        raw_binding = state.get("binding")
        raw_merge = state.get("merge_readback")
        raw_event = state.get("event")
        if not isinstance(raw_binding, Mapping) or "merge_sha" not in raw_binding:
            return None
        if not isinstance(raw_merge, Mapping) or not isinstance(raw_event, Mapping):
            raise PostMergeResumeError("poll_resume_record_incomplete")
        binding = _normalise_binding(raw_binding)
        merge = _read_polled_merge(raw_merge, binding)
        event = validate_merge_event(raw_event, expected_binding=binding)
        return event, binding, merge

    def _poll_request(self, state: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "idempotency_key": self.idempotency_key,
            "event_port": EVENT_PORT,
            "binding": copy.deepcopy(self.binding),
            "poll_generation": int(state["lease_generation"]),
            "successor_node_id": self.successor_node_id,
        }

    def _record_controller_result(
        self,
        state: dict[str, Any],
        controller: PostMergeResumeController,
        binding: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        stored_result = _copy_json("controller_result", dict(result))
        state["controller_result"] = stored_result
        ci_readback = result.get("ci_readback")
        if isinstance(ci_readback, Mapping):
            state["ci_readback"] = _copy_json("ci_readback", dict(ci_readback))
        transition_receipt = result.get("transition_receipt")
        if isinstance(transition_receipt, Mapping):
            state["transition_receipt"] = _copy_json("transition_receipt", dict(transition_receipt))
            generation = transition_receipt.get("lease_generation")
            if isinstance(generation, int) and not isinstance(generation, bool):
                state["lease_generation"] = generation
                state["lease"] = _copy_json("lease", transition_receipt.get("lease", {}))

        controller_state = controller.read_state()
        transition = self._transition(controller_state, binding)
        if transition is not None:
            dispatch = transition.get("successor_dispatch")
            if isinstance(dispatch, Mapping) and dispatch.get("status") == "dispatched":
                state["transition_receipt"] = _copy_json(
                    "transition_receipt", transition.get("transition_receipt", transition)
                )
                receipt = dispatch.get("receipt")
                if isinstance(receipt, Mapping):
                    state["successor_dispatch_receipt"] = _copy_json("dispatch_receipt", dict(receipt))
                return self._save_result(
                    state,
                    "completed",
                    pending=False,
                    reason="successor_dispatched_once",
                    controller_result=stored_result,
                    merge_readback=result.get("merge_readback"),
                    ci_readback=result.get("ci_readback"),
                    transition_receipt=state["transition_receipt"],
                    successor_dispatch_receipt=state.get("successor_dispatch_receipt"),
                )
            if result.get("status") in {"resumed", "duplicate"}:
                return self._save_result(
                    state,
                    "waiting",
                    pending=True,
                    reason="successor_dispatch_pending",
                    controller_result=stored_result,
                )

        if result.get("status") == "waiting":
            return self._save_result(
                state,
                "waiting",
                pending=True,
                reason=str(result.get("reason") or "controller_waiting"),
                controller_result=stored_result,
            )
        if result.get("status") == "rejected":
            return self._save_result(
                state,
                "rejected",
                pending=False,
                reason=str(result.get("reason") or "controller_rejected"),
                controller_result=stored_result,
            )
        return self._save_result(
            state,
            "rejected",
            pending=False,
            reason="transition_receipt_missing",
            controller_result=stored_result,
        )

    def poll_once(
        self,
        *,
        merge_reader: PollMergeReader | None = None,
        ci_reader: PollCIReader | None = None,
        acquire_generation: GenerationAcquirer | None = None,
        project_first_actionable: Projector | None = None,
        dispatch_successor: SuccessorDispatcher | None = None,
    ) -> dict[str, Any]:
        """Poll at most once; a pending result is retried by a later process."""
        with self._lock():
            existed = self.state_path.exists()
            state = self._read_state()
            if not existed:
                self._write_state(state)
            if not state["pending_resume"]:
                status = "duplicate" if state.get("status") == "completed" else str(state.get("status"))
                return self._result(state, status, reason="poll_already_settled")

            state["poll_count"] = int(state["poll_count"]) + 1
            self._write_state(state)

            try:
                stored = self._stored_resume(state)
            except PostMergeResumeError as exc:
                return self._save_result(state, "rejected", pending=False, reason=exc.reason)

            if stored is not None:
                event, binding, merge = stored
                controller = self._controller(binding)
                try:
                    controller_state = controller.read_state()
                except PostMergeResumeError as exc:
                    return self._save_result(state, "rejected", pending=False, reason=exc.reason)
                transition = self._transition(controller_state, binding)
                if transition is not None:
                    dispatch = transition.get("successor_dispatch")
                    if isinstance(dispatch, Mapping) and dispatch.get("status") == "dispatched":
                        return self._save_result(
                            state,
                            "completed",
                            pending=False,
                            reason="successor_dispatched_once",
                            transition_receipt=transition.get("transition_receipt"),
                            successor_dispatch_receipt=dispatch.get("receipt"),
                        )
                    if isinstance(dispatch, Mapping) and dispatch.get("status") == "pending":
                        result = controller.handle_event(
                            event,
                            dispatch_successor=dispatch_successor,
                        )
                        return self._record_controller_result(state, controller, binding, result)
            else:
                binding = None
                event = None
                merge = None

            if merge is None:
                if merge_reader is None:
                    return self._save_result(
                        state,
                        "waiting",
                        pending=True,
                        reason="merge_pending",
                    )
                try:
                    raw_merge = merge_reader(self._poll_request(state))
                except Exception as exc:
                    return self._save_result(
                        state,
                        "waiting",
                        pending=True,
                        reason="merge_readback_unavailable",
                        readback_error=_reject_reason(exc),
                    )
                if raw_merge is None:
                    return self._save_result(
                        state,
                        "waiting",
                        pending=True,
                        reason="merge_pending",
                    )
                try:
                    merge = _read_polled_merge(raw_merge, self.binding)
                except PostMergeResumeError as exc:
                    if exc.reason in {"merge_not_observed", "merge_readback_missing"}:
                        return self._save_result(
                            state,
                            "waiting",
                            pending=True,
                            reason="merge_pending",
                        )
                    return self._save_result(state, "rejected", pending=False, reason=exc.reason)
                binding = {**self.binding, "merge_sha": merge["merge_sha"]}
                event = build_merge_event(
                    binding,
                    event_id=self.idempotency_key,
                    generation=int(state["lease_generation"]),
                    merged_at=str(merge["merged_at"]),
                    source="bounded_poller",
                )
                state["binding"] = copy.deepcopy(binding)
                state["binding_digest"] = digest_json(binding)
                state["merge_readback"] = copy.deepcopy(merge)
                state["event"] = copy.deepcopy(event)
                self._write_state(state)

            assert binding is not None and event is not None and merge is not None
            ci_readback = state.get("ci_readback") if isinstance(state.get("ci_readback"), Mapping) else None
            if ci_reader is not None:
                try:
                    raw_ci = ci_reader(
                        {
                            "idempotency_key": self.idempotency_key,
                            "event": copy.deepcopy(event),
                            "binding": copy.deepcopy(binding),
                            "merge_readback": copy.deepcopy(merge),
                        }
                    )
                except Exception as exc:
                    return self._save_result(
                        state,
                        "waiting",
                        pending=True,
                        reason="ci_readback_unavailable",
                        readback_error=_reject_reason(exc),
                    )
                if raw_ci is None:
                    return self._save_result(
                        state,
                        "waiting",
                        pending=True,
                        reason="ci_pending",
                    )
                try:
                    ci_readback = _copy_json("ci_readback", dict(raw_ci))
                except PostMergeResumeError as exc:
                    return self._save_result(state, "rejected", pending=False, reason=exc.reason)
                state["ci_readback"] = ci_readback
                self._write_state(state)
            if ci_readback is None:
                return self._save_result(
                    state,
                    "waiting",
                    pending=True,
                    reason="ci_pending",
                )

            controller = self._controller(binding)
            result = controller.handle_event(
                event,
                merge_reader=lambda _request: copy.deepcopy(merge),
                ci_reader=lambda _request: copy.deepcopy(ci_readback),
                acquire_generation=acquire_generation,
                project_first_actionable=project_first_actionable,
                dispatch_successor=dispatch_successor,
            )
            return self._record_controller_result(state, controller, binding, result)

    def run_once(self, **kwargs: Any) -> dict[str, Any]:
        """Alias that makes the one-shot process boundary explicit."""
        return self.poll_once(**kwargs)

    def read_state(self) -> dict[str, Any]:
        """Read durable poller state without querying SCM or CI."""
        with self._lock():
            return copy.deepcopy(self._read_state())


def _atomic_json_file(path: Path, value: Mapping[str, Any]) -> None:
    """Write one durable JSON record with a digest-safe replace."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, raw_temp_path = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temp_path = Path(raw_temp_path)
    try:
        make_file_private(descriptor, temp_path)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            body = copy.deepcopy(dict(value))
            body.pop("record_digest", None)
            body["record_digest"] = digest_json(body)
            stream.write(
                (json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
        sync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temp_path.exists():
            temp_path.unlink()


class PendingPostMergeWatchStore:
    """Durable pre-merge watch state owned by a task, not a resident service."""

    def __init__(self, state_root: str | Path, *, allow_production_state_root: bool = False):
        self.state_root = Path(state_root).expanduser().resolve()
        production_root = _production_state_root()
        if (
            not allow_production_state_root
            and (self.state_root == production_root or self.state_root.is_relative_to(production_root))
        ):
            raise PostMergeResumeError("production_state_root_forbidden")
        self.allow_production_state_root = allow_production_state_root
        self.watch_path = self.state_root / "post-merge-resume-pending-watch.json"
        self.lock_path = self.state_root / "post-merge-resume-pending-watch.lock"

    @contextmanager
    def _lock(self):
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with locked_file(self.lock_path):
            yield

    def _read_unlocked(self) -> dict[str, Any] | None:
        if not self.watch_path.exists():
            return None
        try:
            value = json.loads(self.watch_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PostMergeResumeError("pending_watch_unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != PENDING_WATCH_SCHEMA:
            raise PostMergeResumeError("pending_watch_schema_invalid")
        supplied = value.get("record_digest")
        body = {key: item for key, item in value.items() if key != "record_digest"}
        if supplied != digest_json(body):
            raise PostMergeResumeError("pending_watch_digest_mismatch")
        try:
            binding = _normalise_poll_binding(value.get("binding"))
        except PostMergeResumeError as exc:
            raise PostMergeResumeError("pending_watch_binding_invalid") from exc
        if "merge_sha" in binding:
            raise PostMergeResumeError("pending_watch_merge_sha_forbidden")
        if value.get("binding_digest") != digest_json(binding):
            raise PostMergeResumeError("pending_watch_binding_digest_mismatch")
        if not isinstance(value.get("idempotency_key"), str) or not value["idempotency_key"].strip():
            raise PostMergeResumeError("pending_watch_idempotency_invalid")
        if value.get("status") not in {"pending", "completed", "rejected", "expired"}:
            raise PostMergeResumeError("pending_watch_status_invalid")
        if not isinstance(value.get("pending_resume"), bool):
            raise PostMergeResumeError("pending_watch_pending_invalid")
        if isinstance(value.get("poll_count"), bool) or not isinstance(value.get("poll_count"), int) or value["poll_count"] < 0:
            raise PostMergeResumeError("pending_watch_poll_count_invalid")
        repair_receipt = value.get("binding_repair_receipt")
        if repair_receipt is not None:
            if not isinstance(repair_receipt, Mapping) or repair_receipt.get("schema") != BINDING_REPAIR_SCHEMA:
                raise PostMergeResumeError("pending_watch_binding_repair_invalid")
            receipt_body = copy.deepcopy(dict(repair_receipt))
            supplied_receipt_digest = receipt_body.pop("receipt_digest", None)
            if supplied_receipt_digest != digest_json(receipt_body):
                raise PostMergeResumeError("pending_watch_binding_repair_digest_mismatch")
        supersede_receipt = value.get("supersede_receipt")
        if supersede_receipt is not None:
            if not isinstance(supersede_receipt, Mapping) or supersede_receipt.get("schema") != SUPERSEDE_RECEIPT_SCHEMA:
                raise PostMergeResumeError("pending_watch_supersede_receipt_invalid")
            receipt_body = copy.deepcopy(dict(supersede_receipt))
            supplied_receipt_digest = receipt_body.pop("receipt_digest", None)
            if supplied_receipt_digest != digest_json(receipt_body):
                raise PostMergeResumeError("pending_watch_supersede_receipt_digest_mismatch")
        return value

    def read(self) -> dict[str, Any] | None:
        with self._lock():
            value = self._read_unlocked()
            return copy.deepcopy(value) if value is not None else None

    def repair_pending_binding(
        self,
        binding: Mapping[str, Any],
        *,
        expected_binding_digest: str,
        expected_idempotency_key: str,
        repair_reason: str,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Safely rebind one unpolled watch while preserving its idempotency key.

        This is a narrow recovery for a pre-poll repository alias/configuration
        error.  It never changes the PR, Goal, node, or expected head, and it
        refuses once any merge/CI readback or transition work has happened.
        """
        try:
            normalized = _normalise_poll_binding(binding)
            expected_digest = _digest("expected_binding_digest", expected_binding_digest)
            expected_key = _text("expected_idempotency_key", expected_idempotency_key)
            reason = _text("repair_reason", repair_reason)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PostMergeResumeError):
                raise
            raise PostMergeResumeError("pending_watch_repair_arguments_invalid") from exc
        moment = time.time() if now is None else float(now)
        immutable_fields = (
            "goal_id",
            "goal_revision",
            "node_id",
            "pr_number",
            "expected_head_sha",
        )
        with self._lock():
            state = self._read_unlocked()
            if state is None:
                raise PostMergeResumeError("no_pending_watch")
            if state.get("idempotency_key") != expected_key:
                raise PostMergeResumeError("pending_watch_repair_idempotency_conflict")

            existing_receipt = state.get("binding_repair_receipt")
            if isinstance(existing_receipt, Mapping):
                if (
                    state.get("binding_digest") == digest_json(normalized)
                    and existing_receipt.get("new_binding_digest") == digest_json(normalized)
                    and existing_receipt.get("old_binding_digest") == expected_digest
                ):
                    return {**copy.deepcopy(state), "repaired": True, "reused": True}
                raise PostMergeResumeError("pending_watch_binding_repair_conflict")

            if state.get("status") != "pending" or state.get("pending_resume") is not True:
                raise PostMergeResumeError("pending_watch_repair_not_pending")
            if int(state.get("poll_count", 0)) != 0:
                raise PostMergeResumeError("pending_watch_repair_after_poll_forbidden")
            if state.get("poll_claim") is not None:
                raise PostMergeResumeError("pending_watch_repair_claimed")
            if any(
                state.get(field) is not None
                for field in (
                    "merge_readback",
                    "ci_readback",
                    "readback_digest",
                    "transition_receipt",
                    "successor_dispatch_receipt",
                )
            ):
                raise PostMergeResumeError("pending_watch_repair_after_readback_forbidden")

            old_binding = _normalise_poll_binding(state.get("binding"))
            if state.get("binding_digest") != expected_digest:
                raise PostMergeResumeError("pending_watch_repair_binding_mismatch")
            if any(old_binding[field] != normalized[field] for field in immutable_fields):
                raise PostMergeResumeError("pending_watch_repair_identity_mismatch")
            if old_binding["repository_id"] == normalized["repository_id"]:
                raise PostMergeResumeError("pending_watch_repair_noop")

            receipt_body = {
                "schema": BINDING_REPAIR_SCHEMA,
                "status": "rebound",
                "watch_id": state.get("watch_id"),
                "idempotency_key": expected_key,
                "old_binding": old_binding,
                "old_binding_digest": expected_digest,
                "new_binding": normalized,
                "new_binding_digest": digest_json(normalized),
                "repair_reason": reason,
                "previous_record_digest": state.get("record_digest"),
                "repaired_at": moment,
            }
            repair_receipt = {
                **receipt_body,
                "receipt_digest": digest_json(receipt_body),
            }
            state["binding"] = copy.deepcopy(normalized)
            state["binding_digest"] = repair_receipt["new_binding_digest"]
            state["binding_repair_receipt"] = repair_receipt
            state["updated_at"] = moment
            state["next_poll_at"] = moment
            state["last_reason"] = "pending_watch_binding_repaired"
            _atomic_json_file(self.watch_path, state)
            repaired = self._read_unlocked()
            if repaired is None:
                raise PostMergeResumeError("pending_watch_repair_readback_missing")
            return {**copy.deepcopy(repaired), "repaired": True, "reused": False}

    def settle_superseded(
        self,
        *,
        expected_idempotency_key: str,
        superseding_goal_revision: int,
        decision_id: str,
        canonical_goal_digest: str,
        source_revision: str,
        reason: str,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Atomically retire one pending watch superseded by a newer Goal map."""
        key = _text("expected_idempotency_key", expected_idempotency_key)
        revision = _positive_int("superseding_goal_revision", superseding_goal_revision)
        decision = _text("decision_id", decision_id)
        goal_digest = _digest("canonical_goal_digest", canonical_goal_digest)
        source = _sha("source_revision", source_revision)
        settled_reason = _text("reason", reason)
        moment = time.time() if now is None else float(now)
        with self._lock():
            state = self._read_unlocked()
            if state is None:
                raise PostMergeResumeError("no_pending_watch")
            if state.get("idempotency_key") != key:
                raise PostMergeResumeError("pending_watch_supersede_idempotency_conflict")
            if state.get("pending_resume") is not True:
                return {**copy.deepcopy(state), "reused": True}
            binding = _normalise_poll_binding(state.get("binding"))
            if int(binding["goal_revision"]) >= revision:
                raise PostMergeResumeError("pending_watch_not_superseded")
            receipt_body = {
                "schema": SUPERSEDE_RECEIPT_SCHEMA,
                "watch_id": state.get("watch_id"),
                "idempotency_key": key,
                "superseded_goal_revision": int(binding["goal_revision"]),
                "superseding_goal_revision": revision,
                "decision_id": decision,
                "canonical_goal_digest": goal_digest,
                "source_revision": source,
                "previous_record_digest": state.get("record_digest"),
                "reason": settled_reason,
                "settled_at": moment,
            }
            state.update({
                "status": "rejected",
                "pending_resume": False,
                "updated_at": moment,
                "last_reason": settled_reason,
                "superseded_by_goal_revision": revision,
                "supersede_receipt": {
                    **receipt_body,
                    "receipt_digest": digest_json(receipt_body),
                },
            })
            _atomic_json_file(self.watch_path, state)
            settled = self._read_unlocked()
            if settled is None:
                raise PostMergeResumeError("pending_watch_supersede_readback_missing")
            return {**copy.deepcopy(settled), "reused": False}

    def arm(
        self,
        binding: Mapping[str, Any],
        *,
        idempotency_key: str,
        now: float | None = None,
        initial_generation: int = 1,
        max_attempts: int = DEFAULT_MAX_POLL_ATTEMPTS,
        initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    ) -> dict[str, Any]:
        """Create the pending watch before merge observation; repeat arm is idempotent."""
        try:
            normalized = _normalise_poll_binding(binding)
            if "merge_sha" in normalized:
                raise PostMergeResumeError("pending_watch_merge_sha_forbidden")
            key = _text("idempotency_key", idempotency_key)
            generation = _positive_int("initial_generation", initial_generation)
            attempts = _positive_int("max_attempts", max_attempts)
            initial_backoff = float(initial_backoff_seconds)
            max_backoff = float(max_backoff_seconds)
            if initial_backoff <= 0 or max_backoff < initial_backoff:
                raise PostMergeResumeError("pending_watch_backoff_invalid")
        except (TypeError, ValueError) as exc:
            if isinstance(exc, PostMergeResumeError):
                raise
            raise PostMergeResumeError("pending_watch_arguments_invalid") from exc
        moment = time.time() if now is None else float(now)
        with self._lock():
            existing = self._read_unlocked()
            binding_digest = digest_json(normalized)
            if existing is not None:
                if existing.get("idempotency_key") != key or existing.get("binding_digest") != binding_digest:
                    raise PostMergeResumeError("pending_watch_idempotency_conflict")
                return {**copy.deepcopy(existing), "reused": True}
            record = {
                "schema": PENDING_WATCH_SCHEMA,
                "watch_id": "watch-" + digest_json({"key": key}).removeprefix("sha256:")[:32],
                "idempotency_key": key,
                "binding": normalized,
                "binding_digest": binding_digest,
                "status": "pending",
                "pending_resume": True,
                "created_at": moment,
                "updated_at": moment,
                "poll_count": 0,
                "next_poll_at": moment,
                "backoff_seconds": initial_backoff,
                "initial_backoff_seconds": initial_backoff,
                "max_backoff_seconds": max_backoff,
                "max_attempts": attempts,
                "initial_generation": generation,
                "poll_claim": None,
                "last_reason": "pending_merge_observation",
                "last_poll_result": None,
                "merge_readback": None,
                "ci_readback": None,
                "readback_digest": None,
                "transition_receipt": None,
                "successor_dispatch_receipt": None,
            }
            _atomic_json_file(self.watch_path, record)
            return {**record, "record_digest": digest_json(record), "reused": False}

    def claim(self, *, now: float) -> dict[str, Any]:
        """Claim one due tick so concurrent/repeated schedulers cannot double-poll."""
        with self._lock():
            state = self._read_unlocked()
            if state is None:
                return {"state": None, "claimed": False, "reason": "no_pending_watch"}
            if not state.get("pending_resume"):
                return {"state": state, "claimed": False, "reason": "watch_settled"}
            if float(state.get("next_poll_at", 0)) > now:
                return {"state": state, "claimed": False, "reason": "backoff_not_elapsed"}
            claim = state.get("poll_claim")
            if isinstance(claim, Mapping) and float(claim.get("expires_at", 0)) > now:
                return {"state": state, "claimed": False, "reason": "poll_claimed"}
            token = "claim-" + uuid.uuid4().hex
            state["poll_claim"] = {"token": token, "claimed_at": now, "expires_at": now + DEFAULT_POLL_CLAIM_SECONDS}
            state["updated_at"] = now
            _atomic_json_file(self.watch_path, state)
            return {"state": state, "claimed": True, "claim_token": token}

    def finish(
        self,
        claim_token: str,
        *,
        now: float,
        poll_result: Mapping[str, Any],
        outcome: str,
        reason: str,
        count_poll: bool,
    ) -> dict[str, Any] | None:
        """Persist one tick result; a lost claim is left for the next bounded tick."""
        with self._lock():
            state = self._read_unlocked()
            if state is None:
                return None
            claim = state.get("poll_claim")
            if not isinstance(claim, Mapping) or claim.get("token") != claim_token:
                return None
            result = _copy_json("scheduler_poll_result", dict(poll_result))
            state["poll_claim"] = None
            state["updated_at"] = now
            state["last_poll_result"] = result
            for key in ("merge_readback", "ci_readback"):
                if isinstance(result.get(key), Mapping):
                    state[key] = _copy_json(key, dict(result[key]))
            readback = {
                key: result.get(key)
                for key in ("merge_readback", "ci_readback", "transition_receipt", "successor_dispatch_receipt")
                if isinstance(result.get(key), Mapping)
            }
            if readback:
                state["readback_digest"] = digest_json(readback)
            if isinstance(result.get("transition_receipt"), Mapping):
                state["transition_receipt"] = _copy_json("transition_receipt", dict(result["transition_receipt"]))
            if isinstance(result.get("successor_dispatch_receipt"), Mapping):
                state["successor_dispatch_receipt"] = _copy_json(
                    "successor_dispatch_receipt", dict(result["successor_dispatch_receipt"])
                )
            if count_poll:
                state["poll_count"] = int(state.get("poll_count", 0)) + 1
            if outcome == "completed":
                state.update({
                    "status": "completed",
                    "pending_resume": False,
                    "completed_at": now,
                    "last_reason": reason,
                })
            elif outcome == "rejected":
                state.update({
                    "status": "rejected",
                    "pending_resume": False,
                    "last_reason": reason,
                })
            else:
                if count_poll and int(state["poll_count"]) >= int(state["max_attempts"]):
                    state.update({
                        "status": "expired",
                        "pending_resume": False,
                        "last_reason": "bounded_retry_exhausted",
                    })
                else:
                    delay = float(state.get("backoff_seconds", DEFAULT_INITIAL_BACKOFF_SECONDS))
                    state.update({
                        "status": "pending",
                        "pending_resume": True,
                        "next_poll_at": now + delay,
                        "backoff_seconds": min(
                            float(state.get("max_backoff_seconds", DEFAULT_MAX_BACKOFF_SECONDS)),
                            max(delay * 2, DEFAULT_INITIAL_BACKOFF_SECONDS),
                        ),
                        "last_reason": reason,
                    })
            _atomic_json_file(self.watch_path, state)
            return state


class PostMergeResumeControllerScheduler:
    """One provider-neutral controller tick; it polls only a due pending watch."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        successor_node_id: str = DEFAULT_SUCCESSOR_NODE_ID,
        poller_factory: PollerFactory | None = None,
        lineage_admission: LineageAdmission | Mapping[str, Any] | None = None,
        allow_production_state_root: bool = False,
    ):
        self.state_root = Path(state_root).expanduser().resolve()
        self.watch_store = PendingPostMergeWatchStore(
            self.state_root,
            allow_production_state_root=allow_production_state_root,
        )
        self.successor_node_id = _text("successor_node_id", successor_node_id)
        self.poller_factory = poller_factory or BoundedPostMergeResumePoller
        self.lineage_admission = lineage_admission
        self.allow_production_state_root = allow_production_state_root

    def arm_pending_watch(self, binding: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        return self.watch_store.arm(binding, **kwargs)

    def read_watch(self) -> dict[str, Any] | None:
        return self.watch_store.read()

    @staticmethod
    def _result(
        state: Mapping[str, Any] | None,
        status: str,
        *,
        reason: str,
        poller_invocations: int = 0,
        poll_result: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": SCHEDULER_SCHEMA,
            "status": status,
            "reason": reason,
            "pending_resume": bool(state.get("pending_resume")) if state else False,
            "idempotency_key": state.get("idempotency_key") if state else None,
            "poll_count": int(state.get("poll_count", 0)) if state else 0,
            "poller_invocations": poller_invocations,
            "provider_invocations": 0,
            "manual_prompts": 0,
            "executor_invocations": 0,
            "executor_status": None,
            "goal_created": 0,
            "runs_created": 0,
            "attempts_created": 0,
            "successor_dispatches": 1 if status == "completed" else 0,
            "transitions": 0,
            "successor_dispatches": 0,
        }
        if isinstance(poll_result, Mapping):
            result["poll_result"] = _copy_json("scheduler_poll_result", dict(poll_result))
            for key in ("manual_prompts", "provider_invocations", "executor_invocations", "goal_created", "runs_created", "attempts_created"):
                result[key] = int(poll_result.get(key, result[key]))
            controller_result = poll_result.get("controller_result")
            controller_metrics = (
                controller_result.get("metrics")
                if isinstance(controller_result, Mapping)
                and isinstance(controller_result.get("metrics"), Mapping)
                else {}
            )
            result["transitions"] = int(controller_metrics.get("transitions_applied", 0))
            result["successor_dispatches"] = int(controller_metrics.get("successor_dispatches", 0))
            dispatch_receipt = (
                controller_result.get("successor_dispatch_receipt")
                if isinstance(controller_result, Mapping)
                else None
            )
            if isinstance(dispatch_receipt, Mapping):
                result["runs_created"] = int(dispatch_receipt.get("runs_created", result["runs_created"]))
                result["attempts_created"] = int(dispatch_receipt.get("attempts_created", result["attempts_created"]))
                result["executor_invocations"] = int(dispatch_receipt.get("executor_invocations", result["executor_invocations"]))
                result["executor_status"] = dispatch_receipt.get("executor_status")
        return result

    def _verify_admission(self, state: Mapping[str, Any]) -> tuple[bool, dict[str, Any] | None, str]:
        request = {
            "schema": SCHEDULER_SCHEMA,
            "binding": copy.deepcopy(state["binding"]),
            "idempotency_key": state["idempotency_key"],
            "successor_node_id": self.successor_node_id,
        }
        if self.lineage_admission is None:
            return False, None, "coordinator_child_lineage_missing"
        try:
            raw = self.lineage_admission(request) if callable(self.lineage_admission) else self.lineage_admission
            receipt = _mapping_result("coordinator_child_admission", raw)
        except Exception as exc:
            return False, None, _reject_reason(exc)
        if receipt.get("ok") is not True:
            supplied_reason = receipt.get("reason")
            reason = supplied_reason if isinstance(supplied_reason, str) and supplied_reason.strip() else "coordinator_child_lineage_missing"
            return False, receipt, reason if reason in LINEAGE_PERMANENT_REASONS or reason.endswith("_missing") else "coordinator_child_lineage_missing"
        if receipt.get("schema") != COORDINATOR_CHILD_SCHEMA:
            return False, receipt, "coordinator_child_lineage_missing"
        if receipt.get("lease_kind") != COORDINATOR_CHILD_LEASE_KIND:
            return False, receipt, "coordinator_child_lineage_missing"
        if receipt.get("status") != "held":
            return False, receipt, "coordinator_child_lineage_missing"
        for key in ("coordinator_id", "session_id", "parent_worktree", "child_node_id", "worktree", "branch"):
            if not isinstance(receipt.get(key), str) or not receipt[key].strip():
                return False, receipt, "coordinator_child_lineage_missing"
        child_binding = receipt.get("binding")
        if not isinstance(child_binding, Mapping):
            return False, receipt, "coordinator_child_lineage_missing"
        if receipt.get("coordinator_id") != child_binding.get("coordinator_id"):
            return False, receipt, "coordinator_child_lineage_missing"
        try:
            for field in COORDINATOR_CHILD_REQUIRED_BINDING:
                value = child_binding.get(field)
                if field in {"goal_revision", "parent_lease_generation"}:
                    _positive_int(field, value)
                elif field == "parent_fence_token_digest" or field == "child_packet_digest":
                    _digest(field, value)
                else:
                    _text(field, value)
            if child_binding.get("goal_id") != state["binding"].get("goal_id"):
                return False, receipt, "coordinator_child_lineage_missing"
            if child_binding.get("goal_revision") != state["binding"].get("goal_revision"):
                return False, receipt, "coordinator_child_lineage_missing"
            if child_binding.get("child_node_id") != state["binding"].get("node_id"):
                return False, receipt, "coordinator_child_lineage_missing"
            if receipt.get("binding_digest") != digest_json(dict(child_binding)):
                return False, receipt, "coordinator_child_lineage_missing"
            if receipt.get("parent_lease_generation") != child_binding.get("parent_lease_generation"):
                return False, receipt, "coordinator_child_lineage_missing"
            if receipt.get("parent_fence_token_digest") != child_binding.get("parent_fence_token_digest"):
                return False, receipt, "coordinator_child_lineage_missing"
            generation = _positive_int("generation", receipt.get("generation"))
            parent_generation = _positive_int("parent_lease_generation", receipt.get("parent_lease_generation"))
            if generation != parent_generation + 1:
                return False, receipt, "stale_generation"
            fence_digest = _digest("fence_token_digest", receipt.get("fence_token_digest"))
            fence_token = _text("fence_token", receipt.get("fence_token"))
            expected_fence_digest = "sha256:" + hashlib.sha256(fence_token.encode("utf-8")).hexdigest()
            if fence_digest != expected_fence_digest:
                return False, receipt, "stale_fence_digest"
            verification = receipt.get("lineage_verification")
            if not isinstance(verification, Mapping) or verification.get("ok") is not True:
                return False, receipt, "coordinator_child_lineage_missing"
            if (
                verification.get("session_id") != receipt.get("session_id")
                or verification.get("generation") != generation
                or verification.get("fence_token_digest") != fence_digest
            ):
                return False, receipt, "stale_fence_digest"
        except PostMergeResumeError:
            return False, receipt, "coordinator_child_lineage_missing"
        return True, receipt, ""

    def tick(
        self,
        *,
        now: float | None = None,
        merge_reader: PollMergeReader | None = None,
        ci_reader: PollCIReader | None = None,
        acquire_generation: GenerationAcquirer | None = None,
        project_first_actionable: Projector | None = None,
        dispatch_successor: SuccessorDispatcher | None = None,
    ) -> dict[str, Any]:
        """Claim and run at most one bounded poll, then return to the caller."""
        moment = time.time() if now is None else float(now)
        claim = self.watch_store.claim(now=moment)
        state = claim.get("state")
        if state is None:
            return self._result(None, "idle", reason="no_pending_watch")
        if not claim.get("claimed"):
            if claim.get("reason") == "watch_settled":
                status = "duplicate" if state.get("status") == "completed" else str(state.get("status"))
                return self._result(state, status, reason="watch_settled")
            return self._result(state, "waiting", reason=str(claim.get("reason")))

        claim_token = str(claim["claim_token"])
        admitted, admission_receipt, admission_reason = self._verify_admission(state)
        if not admitted:
            blocked_result = {
                "schema": SCHEDULER_SCHEMA,
                "status": "rejected" if admission_reason in LINEAGE_PERMANENT_REASONS else "waiting",
                "reason": admission_reason,
                "admission": admission_receipt,
            }
            admission_outcome = "rejected" if admission_reason in LINEAGE_PERMANENT_REASONS else "blocked"
            finished = self.watch_store.finish(
                claim_token,
                now=moment,
                poll_result=blocked_result,
                outcome=admission_outcome,
                reason=admission_reason,
                count_poll=False,
            )
            return self._result(
                finished or state,
                "rejected" if admission_outcome == "rejected" else "waiting",
                reason=admission_reason,
                poll_result=blocked_result,
            )

        try:
            poller = self.poller_factory(
                self.state_root,
                expected_binding=state["binding"],
                idempotency_key=state["idempotency_key"],
                successor_node_id=self.successor_node_id,
                initial_generation=int(state.get("initial_generation", 1)),
                allow_production_state_root=self.allow_production_state_root,
            )
            poll_result = _mapping_result(
                "poll_result",
                poller.poll_once(
                    merge_reader=merge_reader,
                    ci_reader=ci_reader,
                    acquire_generation=acquire_generation,
                    project_first_actionable=project_first_actionable,
                    dispatch_successor=dispatch_successor,
                ),
            )
        except Exception as exc:
            poll_result = {
                "schema": SCHEDULER_SCHEMA,
                "status": "waiting",
                "reason": "poller_failed:" + _reject_reason(exc),
                "manual_prompts": 0,
                "provider_invocations": 0,
            }

        if admission_receipt is not None:
            poll_result["coordinator_child_admission"] = admission_receipt

        poll_status = str(poll_result.get("status") or "")
        pending = poll_result.get("pending_resume") is True or poll_status == "waiting"
        if not pending and poll_status in {"completed", "resumed", "duplicate"}:
            outcome, final_status = "completed", "completed"
            final_reason = str(poll_result.get("reason") or "successor_dispatched_once")
        elif poll_status == "rejected":
            outcome, final_status = "rejected", "rejected"
            final_reason = str(poll_result.get("reason") or "poll_rejected")
        else:
            outcome, final_status = "pending", "waiting"
            final_reason = str(poll_result.get("reason") or "bounded_poll_pending")
        finished = self.watch_store.finish(
            claim_token,
            now=moment,
            poll_result=poll_result,
            outcome=outcome,
            reason=final_reason,
            count_poll=True,
        )
        if finished is None:
            return self._result(state, "waiting", reason="poll_claim_lost", poller_invocations=1, poll_result=poll_result)
        if outcome == "pending" and not finished.get("pending_resume"):
            return self._result(finished, "expired", reason=str(finished.get("last_reason")), poller_invocations=1, poll_result=poll_result)
        return self._result(
            finished,
            final_status,
            reason=final_reason,
            poller_invocations=1,
            poll_result=poll_result,
        )


PostMergeResumeScheduler = PostMergeResumeControllerScheduler


__all__ = [
    "DEFAULT_GOAL_ID",
    "DEFAULT_GOAL_REVISION",
    "DEFAULT_NODE_ID",
    "DEFAULT_SUCCESSOR_NODE_ID",
    "EVENT_PORT",
    "EVENT_SCHEMA",
    "POLLER_SCHEMA",
    "POLLER_STATE_SCHEMA",
    "POLL_REQUIRED_BINDING",
    "PENDING_WATCH_SCHEMA",
    "SCHEDULER_SCHEMA",
    "COORDINATOR_CHILD_SCHEMA",
    "COORDINATOR_CHILD_LEASE_KIND",
    "COORDINATOR_CHILD_REQUIRED_BINDING",
    "DEFAULT_MAX_POLL_ATTEMPTS",
    "DEFAULT_INITIAL_BACKOFF_SECONDS",
    "DEFAULT_MAX_BACKOFF_SECONDS",
    "PendingPostMergeWatchStore",
    "PostMergeResumeControllerScheduler",
    "PostMergeResumeScheduler",
    "PostMergeResumeController",
    "BoundedPostMergeResumePoller",
    "PostMergeResumeError",
    "REQUIRED_BINDING",
    "SCHEMA",
    "STATE_SCHEMA",
    "TRANSITION_SCHEMA",
    "build_merge_event",
    "digest_json",
    "validate_merge_event",
]

"""Small, test-only native RunStore delivery fixture.

The helper deliberately stops at the admission boundary.  It seals the one
shared LH contract engine, plans it, binds a packet, and persists a Goal/Run;
the canary that calls it still has to execute the controller, verifier, and
external phases.  In particular, this module never starts an Attempt, writes
a receipt, or asserts a GREEN terminal state for its caller.
"""

from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
# Monorepo layout keeps LH under loop-hybrid/; the public projection is LH at the root.
LH_ROOT = ROOT / "loop-hybrid" if (ROOT / "loop-hybrid").is_dir() else ROOT
LH_RUNTIME = LH_ROOT / "lh_runtime"
for _path in (LH_ROOT, LH_RUNTIME):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import delivery_contract as contract_engine  # noqa: E402
from run_store import RunStore  # noqa: E402


_COMMIT = re.compile(r"^[0-9a-fA-F]{40}$")
_PHASES = {"sync", "async"}
_FORBIDDEN_PATHS = [".git/", "secrets/", "credentials/", "cookies/"]
_SEALED_DELIVERY_FIELDS = frozenset({
    "delivery_contract",
    "delivery_unit_contract",
    "delivery_plan",
    "delivery_plan_verdict",
    "delivery_packet",
    "delivery_unit_packet",
})
_IDENTITY = [
    "unit_id",
    "goal_id",
    "goal_revision",
    "node_id",
    "dispatch_key",
    "run_id",
    "attempt",
    "fence",
    "base_sha",
    "diff_digest",
]
_RECEIPTS = [
    "plan_verdict",
    "packet_admission",
    "dispatch",
    "executor",
    "delivery_verifier",
    "completion",
]
_SOURCE_RECEIPTS = [
    "plan_verdict",
    "packet_admission",
    "dispatch",
    "executor",
    "delivery_verifier",
]


def _strings(name: str, values: Sequence[str], *, required: bool = True) -> list[str]:
    if not isinstance(values, (list, tuple)):
        raise TypeError(f"{name} must be a sequence")
    result = [value.strip() for value in values]
    if any(not value for value in result) or (required and not result):
        raise ValueError(f"{name} must contain non-empty strings")
    if len(set(result)) != len(result):
        raise ValueError(f"{name} must not contain duplicates")
    return result


def _copy_checks(checks: Sequence[Mapping[str, Any]], phase: str) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(checks, (list, tuple)) or not checks:
        raise ValueError("checks must contain at least one explicit obligation")
    obligations: list[dict[str, Any]] = []
    source_ids: list[str] = []
    seen: set[str] = set()
    for raw in checks:
        if not isinstance(raw, Mapping):
            raise TypeError("each check must be a mapping")
        row = copy.deepcopy(dict(raw))
        obligation_id = row.get("id")
        if not isinstance(obligation_id, str) or not obligation_id.strip() or obligation_id in seen:
            raise ValueError("each check needs a unique id")
        commands = row.get("commands")
        receipts = row.get("required_receipts")
        if not isinstance(commands, list) or not commands:
            raise ValueError(f"check {obligation_id!r} needs explicit commands")
        if not isinstance(receipts, list) or not receipts:
            raise ValueError(f"check {obligation_id!r} needs required_receipts")
        # ``phase``/``source_required`` are fixture routing metadata, not a
        # second contract field.  They select source obligation IDs and are
        # removed before the canonical engine seals the contract.
        source_marker = row.pop("source_required", None)
        row_phase = row.pop("phase", None)
        final_only = row.pop("final_only", False)
        if source_marker is not None and not isinstance(source_marker, bool):
            raise ValueError(f"check {obligation_id!r} source_required must be bool")
        if row_phase is not None and row_phase not in {"source", "final"}:
            raise ValueError(f"check {obligation_id!r} phase must be source/final")
        if not isinstance(final_only, bool):
            raise ValueError(f"check {obligation_id!r} final_only must be bool")
        seen.add(obligation_id)
        obligations.append(row)
        source = source_marker is not False and not final_only and row_phase != "final"
        if phase == "sync":
            source = True
        if source:
            source_ids.append(obligation_id)
    if not source_ids:
        raise ValueError("phase requires at least one source obligation")
    return obligations, source_ids


def _verifier(argv: Sequence[str]) -> list[str]:
    values = _strings("verifier_argv", argv)
    # A native fixture must exercise the configured read-only verifier.  A
    # bare ``pass``/``true`` command would only manufacture a GREEN shape and
    # is rejected at fixture construction rather than becoming a canary.
    if values in (["pass"], ["true"], ["/bin/true"]):
        raise ValueError("verifier_argv must verify a real file/content fact")
    if values[:2] == [sys.executable, "-c"] and len(values) < 3:
        raise ValueError("python verifier needs a bounded program")
    return values


def make_native_run(
    store: RunStore | None,
    repo: str | Path,
    base: str,
    goal_id: str,
    node_id: str,
    checks: Sequence[Mapping[str, Any]],
    verifier_argv: Sequence[str],
    allowed_paths: Sequence[str],
    max_attempts: int,
    phase: str = "sync",
    *,
    goal: Mapping[str, Any] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build, and optionally persist, a real native binding.

    ``checks`` are full obligation rows (``id``, ``commands``, and
    ``required_receipts``).  For ``phase='async'`` a row marked
    ``final_only=True`` or ``phase='final'`` is excluded from source
    eligibility but remains in the final contract.  All command argv and the
    independent verifier argv are caller-supplied and bounded by the shared
    engine; no success receipt is fabricated here.
    """
    if phase not in _PHASES:
        raise ValueError(f"unsupported native fixture phase: {phase}")
    if not isinstance(base, str) or _COMMIT.fullmatch(base) is None:
        raise ValueError("base must be a 40-character commit")
    if not isinstance(goal_id, str) or not goal_id.strip():
        raise ValueError("goal_id is required")
    if not isinstance(node_id, str) or not node_id.strip():
        raise ValueError("node_id is required")
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 4:
        raise ValueError("max_attempts must be an integer from 1 to 4")
    repo_path = Path(repo).expanduser().resolve()
    if not repo_path.is_dir():
        raise ValueError("repo must be an existing directory")
    preserved_goal = copy.deepcopy(dict(goal)) if isinstance(goal, Mapping) else {}
    expected_unit_id = f"native-unit-{goal_id}-{node_id}"
    for field, expected in (("goal_id", goal_id), ("node_id", node_id), ("unit_id", expected_unit_id)):
        observed = preserved_goal.get(field)
        if observed not in (None, expected):
            raise ValueError(f"native fixture {field} identity mismatch")
    observed_revision = preserved_goal.get("goal_revision", 1)
    if isinstance(observed_revision, bool) or not isinstance(observed_revision, int) or observed_revision < 1:
        raise ValueError("goal_revision must be a positive integer")
    observed_phase = preserved_goal.get("delivery_phase")
    if observed_phase not in (None, phase):
        raise ValueError("native fixture delivery_phase identity mismatch")
    if preserved_goal.get("delivery_required") is False:
        raise ValueError("native fixture cannot disable mandatory delivery")
    allowed = _strings("allowed_paths", allowed_paths)

    for label, value in (("goal", preserved_goal), ("admission_envelope", preserved_goal.get("admission_envelope"))):
        if not isinstance(value, Mapping):
            continue
        for field, expected in (("unit_id", expected_unit_id), ("delivery_phase", phase)):
            observed = value.get(field)
            if observed not in (None, expected):
                raise ValueError(f"native fixture {label}.{field} identity mismatch")
        for field, expected in (("allowed_paths", allowed), ("forbidden_paths", _FORBIDDEN_PATHS)):
            observed = value.get(field)
            if observed is None:
                continue
            try:
                normalized = _strings(f"{label}.{field}", observed)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"native fixture {label}.{field} constraint invalid") from exc
            if normalized != list(expected):
                raise ValueError(f"native fixture {label}.{field} constraint mismatch")
        existing = sorted(field for field in _SEALED_DELIVERY_FIELDS if field in value and value[field] is not None)
        if existing:
            raise ValueError(f"native fixture cannot overwrite sealed delivery input: {label}.{existing[0]}")
    obligations, source_ids = _copy_checks(checks, phase)
    verifier = _verifier(verifier_argv)

    body: dict[str, Any] = {
        "schema": contract_engine.SCHEMA,
        "contract_version": 1,
        "contract_id": f"native-contract-{goal_id}-{node_id}",
        "unit_id": expected_unit_id,
        "goal": {"id": goal_id, "revision": observed_revision},
        "node": {"id": node_id, "kind": "native-canary"},
        "planner": {"principal": "native-test-fixture-planner", "source": "tests/native_delivery_fixture.py"},
        "independent_verifier": {
            "principal": "native-test-fixture-verifier",
            "read_only": True,
            "source_write": False,
            "capability": "native-bounded-readonly-file-verifier",
            "argv": verifier,
            "cwd": "${WORKTREE}",
            "timeout_seconds": 30,
        },
        "outcome": {
            "observable": "native RunStore reaches verified only after durable delivery evidence",
            "start_state": "queued",
            "success_state": "verified",
            "terminal_states": ["verified", "human_required", "exhausted"],
        },
        "scope": {
            "ownership": "task-owned-native-canary",
            "allowed_paths": allowed,
            "forbidden_paths": list(_FORBIDDEN_PATHS),
            "identity": list(_IDENTITY),
        },
        "obligations": obligations,
        "required_receipts": list(_RECEIPTS),
        "source_required_receipts": list(_SOURCE_RECEIPTS),
        "source_obligation_ids": source_ids,
        "source_vs_live": {
            "source_must_not_claim_live": True,
            "live_required_for_source_delivery": False,
        },
        "repair_same_unit": {
            "enabled": True,
            "route": "same_work_unit_new_attempt",
            "identity_fields": ["unit_id", "dispatch_key", "goal_id", "node_id"],
            "max_attempts": max_attempts,
            "scope_drift_route": "planner_required",
            "unknown_outcome_route": "reconcile_before_retry",
        },
        "authority_store": "run",
        "managed_scope": f"native-{phase}-canary",
    }
    contract = contract_engine.seal_contract(body)
    plan = contract_engine.plan_delivery_unit(contract)
    packet = contract_engine.bind_packet(
        {
            "schema": "host-delivery-unit-packet/v1",
            "packet_id": f"native-packet-{goal_id}-{node_id}",
            "goal_id": goal_id,
            "goal_revision": observed_revision,
            "node_id": node_id,
            "write_set": list(allowed),
            "forbidden_paths": list(_FORBIDDEN_PATHS),
        },
        plan,
        contract,
    )
    envelope = preserved_goal.get("admission_envelope")
    if not isinstance(envelope, Mapping):
        envelope = {}
    else:
        envelope = copy.deepcopy(dict(envelope))
    envelope.update({
        "delivery_contract": contract,
        "delivery_plan": plan,
        "delivery_packet": packet,
        "allowed_paths": list(allowed),
        "forbidden_paths": list(_FORBIDDEN_PATHS),
    })
    goal_payload = {
        **preserved_goal,
        "goal_id": goal_id,
        "goal_revision": observed_revision,
        "node_id": node_id,
        "unit_id": contract["unit_id"],
        "delivery_required": True,
        "delivery_phase": phase,
        "delivery_contract": contract,
        "delivery_plan": plan,
        "delivery_packet": packet,
        "admission_envelope": envelope,
    }
    if store is not None:
        run_id = store.create_run(
            goal=goal_payload,
            source_repo=repo_path,
            base_revision=base,
            max_attempts=max_attempts,
            run_id=run_id,
        )
    return {"run_id": run_id, "contract": contract, "plan": plan, "packet": packet, "goal": goal_payload}


def make_native_bundle(
    repo: str | Path,
    base: str,
    goal_id: str,
    node_id: str,
    checks: Sequence[Mapping[str, Any]],
    verifier_argv: Sequence[str],
    allowed_paths: Sequence[str],
    max_attempts: int,
    phase: str = "sync",
    *,
    goal: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return only the sealed Goal binding; never create a Run or receipt."""
    return make_native_run(
        None,
        repo,
        base,
        goal_id,
        node_id,
        checks,
        verifier_argv,
        allowed_paths,
        max_attempts,
        phase,
        goal=goal,
    )


__all__ = ["make_native_bundle", "make_native_run"]

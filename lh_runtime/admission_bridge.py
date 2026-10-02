"""G4 bounded candidate admission and deterministic Goal-to-Run bridge."""
from __future__ import annotations

import hashlib
import json
import copy
import subprocess
import time
from pathlib import Path
from typing import Any

try:
    from . import delivery_contract as delivery_engine
except ImportError:
    import delivery_contract as delivery_engine  # type: ignore

from goal_store import GoalStore
from run_store import RunStore


ENVELOPE_SCHEMA = "lh-campaign-admission-envelope/v1"
FORBIDDEN_SIDE_EFFECTS = {"push", "merge", "publish", "external_action", "credential"}
_DELIVERY_FIELDS = (
    "delivery_required",
    "delivery_managed",
    "delivery_contract",
    "delivery_unit_contract",
    "delivery_plan",
    "delivery_plan_verdict",
    "delivery_binding",
    "delivery_unit_id",
    "delivery_unit_managed",
    "delivery_contract_digest",
    "delivery_unit_contract_digest",
    "delivery_packet",
    "delivery_unit_packet",
    "delivery_candidate",
    "delivery_definition",
    "delivery_planning_capability",
    "planning_capability",
)


def _rebind_delivery_for_revision(
    run_goal: dict[str, Any], envelope: dict[str, Any], revision: int,
) -> dict[str, Any]:
    """Re-seal a persisted delivery binding for a fresh Goal revision.

    A successful recurring Goal is a new Run/Goal identity, not a relink of
    the old terminal Run.  Its immutable contract therefore needs the bumped
    numeric revision before the new Run is created.  This uses the shared
    engine's seal/plan/packet APIs; it does not invent obligations or widen
    the persisted scope.
    """
    persisted_contract = run_goal.get("delivery_contract")
    if not isinstance(persisted_contract, dict):
        # Some older but valid Goal producers persisted the complete binding
        # only inside their durable admission envelope.  That envelope is
        # supplied here from the current Goal row, never from a new request.
        persisted_contract = envelope.get("delivery_contract")
    if not isinstance(persisted_contract, dict):
        raise ValueError("delivery_contract_missing_for_revision_rebind")
    # The request envelope is not an authority for an already persisted Run.
    # If it carries a binding, it must agree with the persisted one; otherwise
    # a caller could replace the original contract while asking for a retry.
    for candidate in (envelope.get("delivery_contract"),):
        if isinstance(candidate, dict) and candidate != persisted_contract:
            raise ValueError("delivery_contract_revision_rebind_identity_mismatch")
    contract = copy.deepcopy(persisted_contract)
    try:
        delivery_engine.validate_contract(contract)
    except Exception as exc:
        raise ValueError(f"delivery_contract_revision_rebind_invalid:{type(exc).__name__}") from exc
    old_revision = contract.get("goal", {}).get("revision") if isinstance(contract.get("goal"), dict) else None
    if old_revision != run_goal.get("goal_revision"):
        raise ValueError("delivery_contract_revision_rebind_revision_mismatch")
    if contract.get("goal", {}).get("id") != run_goal.get("goal_id"):
        raise ValueError("delivery_contract_revision_rebind_goal_mismatch")
    contract_node = contract.get("node") if isinstance(contract.get("node"), dict) else {}
    if run_goal.get("node_id") not in (None, contract_node.get("id")):
        raise ValueError("delivery_contract_revision_rebind_node_mismatch")
    if run_goal.get("unit_id") not in (None, contract.get("unit_id")):
        raise ValueError("delivery_contract_revision_rebind_unit_mismatch")
    original_plan = run_goal.get("delivery_plan")
    if not isinstance(original_plan, dict):
        original_plan = envelope.get("delivery_plan")
    if not isinstance(original_plan, dict):
        raise ValueError("delivery_plan_missing_for_revision_rebind")
    if delivery_engine.verify_plan_verdict(original_plan, contract).get("verdict") != "GREEN":
        raise ValueError("delivery_plan_invalid_for_revision_rebind")
    original_packet = run_goal.get("delivery_packet")
    if not isinstance(original_packet, dict):
        original_packet = envelope.get("delivery_packet")
    if not isinstance(original_packet, dict):
        raise ValueError("delivery_packet_missing_for_revision_rebind")
    if delivery_engine.verify_packet_binding(original_packet, contract, original_plan).get("verdict") != "GREEN":
        raise ValueError("delivery_packet_invalid_for_revision_rebind")
    if run_goal.get("delivery_contract_digest") not in (None, contract.get("contract_digest")):
        raise ValueError("delivery_contract_digest_mismatch_for_revision_rebind")
    if run_goal.get("delivery_unit_contract_digest") not in (None, contract.get("contract_digest")):
        raise ValueError("delivery_unit_contract_digest_mismatch_for_revision_rebind")
    for candidate in (envelope.get("delivery_plan"),):
        if isinstance(candidate, dict) and candidate != original_plan:
            raise ValueError("delivery_plan_revision_rebind_identity_mismatch")
    envelope_packet = envelope.get("delivery_packet")
    envelope_has_binding = any(
        isinstance(envelope.get(key), dict)
        for key in ("delivery_contract", "delivery_plan")
    )
    if envelope_has_binding and not isinstance(envelope_packet, dict):
        raise ValueError("delivery_packet_missing_for_revision_rebind")
    if isinstance(envelope_packet, dict):
        if envelope_packet != original_packet:
            raise ValueError("delivery_packet_revision_rebind_identity_mismatch")

    body = copy.deepcopy(contract)
    contract_goal = body.get("goal") if isinstance(body.get("goal"), dict) else {}
    contract_goal["revision"] = revision
    body["goal"] = contract_goal
    body.pop("contract_digest", None)
    old_body = copy.deepcopy(contract)
    old_body.pop("contract_digest", None)
    old_body_goal = old_body.get("goal") if isinstance(old_body.get("goal"), dict) else {}
    old_body_goal.pop("revision", None)
    new_body_goal = copy.deepcopy(body.get("goal")) if isinstance(body.get("goal"), dict) else {}
    new_body_goal.pop("revision", None)
    old_body["goal"] = old_body_goal
    body_without_revision = copy.deepcopy(body)
    body_without_revision["goal"] = new_body_goal
    if old_body != body_without_revision:
        raise ValueError("delivery_contract_revision_rebind_scope_drift")
    rebound = delivery_engine.seal_contract(body)
    plan = delivery_engine.plan_delivery_unit(rebound)
    packet = copy.deepcopy(original_packet)
    packet["goal_id"] = rebound["goal"]["id"]
    packet["goal_revision"] = revision
    packet["node_id"] = rebound["node"]["id"]
    packet = delivery_engine.bind_packet(packet, plan, rebound)
    rebound_envelope = copy.deepcopy(envelope)
    rebound_envelope.update({
        "delivery_contract": rebound,
        "delivery_plan": plan,
        "delivery_packet": packet,
        "delivery_contract_digest": rebound.get("contract_digest"),
        "delivery_unit_contract_digest": rebound.get("contract_digest"),
        "delivery_unit_id": rebound.get("unit_id"),
        "goal_revision": revision,
    })
    run_goal.update({
        "goal_revision": revision,
        "delivery_contract": rebound,
        "delivery_plan": plan,
        "delivery_packet": packet,
        "delivery_contract_digest": rebound.get("contract_digest"),
        "delivery_unit_contract_digest": rebound.get("contract_digest"),
        "delivery_unit_id": rebound.get("unit_id"),
    })
    return rebound_envelope


def _payload_with_delivery_binding(
    current_payload: dict[str, Any], run_goal: dict[str, Any], envelope: dict[str, Any], revision: int,
) -> dict[str, Any]:
    """Copy a validated recurring binding into the complete next Goal payload."""
    payload = copy.deepcopy(current_payload)
    payload["goal_revision"] = revision
    payload["admission_envelope"] = copy.deepcopy(envelope)
    for key in _DELIVERY_FIELDS:
        if key in run_goal:
            payload[key] = copy.deepcopy(run_goal[key])
    nested = payload.get("feature_contract")
    if isinstance(nested, dict):
        nested["admission_envelope"] = copy.deepcopy(envelope)
        nested["goal_revision"] = revision
        for key in _DELIVERY_FIELDS:
            if key in run_goal:
                nested[key] = copy.deepcopy(run_goal[key])
    return payload


def _id(prefix: str, value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return prefix + hashlib.sha256(raw).hexdigest()[:32]


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _pin_base_revision(source_repo: Path, base_revision: str) -> str | None:
    """Resolve a (possibly moving) ref name to its commit SHA at admission time.

    Pinning at admission closes the time-of-check/time-of-use gap between
    admitting a run and cloning the workspace later: the run record carries
    the exact commit, not a branch name that can drift. Returns None when the
    ref cannot be resolved (not a git repo, unknown ref)."""
    proc = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "--verify", f"{base_revision}^{{commit}}"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


class GoalAdmissionBridge:
    """Admission policy for G4; the controller remains unchanged."""

    def __init__(self, goal_store: GoalStore, run_store: RunStore):
        self.goal_store = goal_store
        self.run_store = run_store

    @staticmethod
    def _reasons(envelope: Any, source_repo: Path, base_revision: str, verification_argv: list[str] | None, max_attempts: int | None) -> list[str]:
        reasons: list[str] = []
        if not isinstance(envelope, dict) or envelope.get("schema") != ENVELOPE_SCHEMA:
            reasons.append("invalid_admission_envelope")
            return reasons
        if envelope.get("human_only") is True:
            reasons.append("human_only_stage")
        auto = envelope.get("auto_admission")
        if not isinstance(auto, dict) or auto.get("eligible") is not True:
            reasons.append("envelope_not_auto_eligible")
        allowed_paths = envelope.get("allowed_paths")
        if not isinstance(allowed_paths, list) or not allowed_paths or any(not isinstance(path, str) or not path.strip() for path in allowed_paths):
            reasons.append("invalid_allowed_paths")
        side_effects = envelope.get("allowed_side_effects")
        if not isinstance(side_effects, list) or any(not isinstance(item, str) or not item.strip() for item in side_effects):
            reasons.append("invalid_allowed_side_effects")
        elif sorted(set(side_effects) & FORBIDDEN_SIDE_EFFECTS):
            reasons.append("forbidden_external_side_effect")
        lamp = envelope.get("acceptance_lamp")
        lamp_ok = isinstance(lamp, dict) and isinstance(lamp.get("verification_argv"), list) and bool(lamp["verification_argv"])
        external = envelope.get("external_verdict")
        external_ok = isinstance(external, dict) and isinstance(external.get("action_id"), str) and bool(external["action_id"].strip())
        if not lamp_ok and not external_ok:
            reasons.append("missing_acceptance_lamp")
        if not source_repo.exists() or not source_repo.is_dir():
            reasons.append("source_repo_unavailable")
        if not isinstance(base_revision, str) or not base_revision.strip():
            reasons.append("missing_base_revision")
        attempts = envelope.get("max_attempts") if max_attempts is None else max_attempts
        if not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= 4:
            reasons.append("invalid_attempt_budget")
        if verification_argv is not None and (not isinstance(verification_argv, list) or any(not isinstance(item, str) or not item.strip() for item in verification_argv)):
            reasons.append("invalid_verification_argv")
        return reasons

    def admit(
        self,
        goal_id: str,
        *,
        source_repo: str | Path,
        base_revision: str,
        envelope: dict[str, Any],
        event_key: str | None = None,
        verification_argv: list[str] | None = None,
        max_attempts: int | None = None,
    ) -> dict[str, Any]:
        goal_id = _text("goal_id", goal_id)
        if event_key is not None:
            event_key = _text("event_key", event_key)
        source_repo = Path(source_repo)
        base_revision = _text("base_revision", base_revision)
        goal = self.goal_store.get_goal(goal_id)
        revision = goal.get("current_revision")
        revision_id = revision.get("revision_id") if isinstance(revision, dict) else None
        if not isinstance(revision_id, str) or not revision_id:
            raise ValueError("candidate has no current revision")
        reasons = self._reasons(envelope, source_repo, base_revision, verification_argv, max_attempts)
        if reasons:
            if goal["state"] == "candidate":
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="candidate")
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": reasons}
        if goal["state"] not in {"candidate", "active"}:
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": [f"goal_state_{goal['state']}"]}
        pinned_revision = _pin_base_revision(source_repo, base_revision)
        if pinned_revision is None:
            if goal["state"] == "candidate":
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="candidate")
            return {"status": "human_required", "goal_id": goal_id, "run_id": None, "reasons": ["unresolvable_base_revision"]}
        attempts = envelope["max_attempts"] if max_attempts is None else max_attempts
        run_id = _id("run-goal-", {"goal_id": goal_id, "revision_id": revision_id, "base_revision": pinned_revision, "envelope": envelope})
        run_goal = {
            "schema": "lh-goal-run/v1",
            "goal_id": goal_id,
            "revision_id": revision_id,
            "campaign_id": goal["campaign_id"],
            "stage_id": goal["stage_id"],
            "admission_envelope": envelope,
        }
        # Preserve the numeric Goal revision as a producer-owned identity
        # field.  Delivery contracts still remain the authority for their
        # canonical identity, but omitting this field would leave the Run
        # unable to prove that admission and delivery refer to one revision.
        if isinstance(revision, dict) and isinstance(revision.get("revision"), int):
            run_goal["goal_revision"] = int(revision["revision"])
        # Carry the immutable generic delivery binding from the persisted Goal
        # revision into the Run.  No default contract is invented from a
        # campaign name or phase-shaped metadata; absent binding is handled by
        # RunStore as a durable planning request.
        revision_goal = goal.get("current_revision", {}).get("goal") if isinstance(goal.get("current_revision"), dict) else None
        persisted_envelope = revision_goal.get("admission_envelope") if isinstance(revision_goal, dict) else None
        if isinstance(persisted_envelope, dict):
            # Recurring admission must use the persisted request envelope as
            # its binding source.  The caller's envelope remains policy input.
            run_goal["admission_envelope"] = copy.deepcopy(persisted_envelope)
        else:
            # Keep caller policy such as requires_non_empty_diff, but never
            # promote caller-supplied delivery bytes into Run authority.
            run_goal["admission_envelope"] = {
                key: copy.deepcopy(value)
                for key, value in envelope.items()
                if key not in _DELIVERY_FIELDS
            }
        if isinstance(revision_goal, dict):
            for key in _DELIVERY_FIELDS:
                if key in revision_goal:
                    run_goal[key] = json.loads(json.dumps(revision_goal[key], sort_keys=True))
            # CampaignCompiler carries the authorized stage definition under
            # ``feature_contract`` when a successor stage is derived.  A
            # producer-supplied generic binding nested there is still the
            # persisted Goal revision's authority; lift only the explicit
            # delivery fields so the RunStore receives the same contract,
            # plan, packet, and planning capability as a directly admitted
            # candidate.  No default contract is synthesized from a stage
            # name or envelope.
            nested_feature_contract = revision_goal.get("feature_contract")
            if isinstance(nested_feature_contract, dict):
                for key in _DELIVERY_FIELDS:
                    if key in nested_feature_contract and key not in run_goal:
                        run_goal[key] = json.loads(json.dumps(nested_feature_contract[key], sort_keys=True))
        persisted_contract = run_goal.get("delivery_contract")
        if not isinstance(persisted_contract, dict) and isinstance(revision_goal, dict):
            persisted_envelope = revision_goal.get("admission_envelope")
            if isinstance(persisted_envelope, dict):
                persisted_contract = persisted_envelope.get("delivery_contract")
        if not isinstance(persisted_contract, dict) and isinstance(revision_goal, dict):
            feature_contract = revision_goal.get("feature_contract")
            if isinstance(feature_contract, dict):
                persisted_contract = feature_contract.get("delivery_contract")
        if isinstance(persisted_contract, dict):
            contract_node = persisted_contract.get("node")
            if isinstance(contract_node, dict) and isinstance(contract_node.get("id"), str) and contract_node["id"].strip():
                run_goal["node_id"] = contract_node["id"].strip()
            if isinstance(persisted_contract.get("unit_id"), str) and persisted_contract["unit_id"].strip():
                run_goal["unit_id"] = persisted_contract["unit_id"].strip()
        if pinned_revision != base_revision:
            run_goal["base_ref"] = base_revision
        existing_run = None
        linked_run_id = goal.get("run_id") if isinstance(goal.get("run_id"), str) else None
        try:
            # GoalStore's durable run link is authoritative for a revived
            # candidate.  Recomputing from the caller envelope here would miss
            # the previous revision's persisted envelope and accidentally
            # admit another Run at the same revision without a bump.
            existing_run = self.run_store.get_run(linked_run_id or run_id)
        except KeyError:
            existing_run = None
        if existing_run is not None and existing_run["state"] == "human_required":
            # A human-required Run is an unresolved execution gate, not a
            # terminal cycle.  A new candidate must not idempotently reuse it
            # or bind it active; the operator must resolve the parked Run
            # first.  Keep the old Run/Attempt/receipt as history.
            if goal["state"] == "candidate":
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="candidate")
            return {
                "status": "human_required",
                "goal_id": goal_id,
                "run_id": None,
                "reasons": ["existing_run_human_required"],
            }
        if existing_run is not None and existing_run["state"] not in {"stopped", "verified"}:
            # A queued/running/external-wait Run already linked to this Goal
            # is the replay authority.  Do not recompute an id from a stale
            # caller envelope and create a second Run at the same revision.
            linked = self.goal_store.activate_with_run(
                goal_id, existing_run["run_id"], event_key=event_key,
            )
            return {
                "status": "reused",
                "goal_id": goal_id,
                "revision_id": revision_id,
                "run_id": existing_run["run_id"],
                "run_state": existing_run["state"],
                "goal_state": linked["state"],
            }
        if existing_run is not None and existing_run["state"] in {"stopped", "verified"}:
            # Revision-bump: a terminal run is never re-linked. A new command
            # re-issues the work as revision N+1 (new deterministic run_id,
            # old run kept as history).
            current_revision = goal.get("current_revision")
            if not isinstance(current_revision, dict) or not isinstance(current_revision.get("goal"), dict):
                return {
                    "status": "human_required",
                    "goal_id": goal_id,
                    "run_id": None,
                    "reasons": ["goal_revision_payload_missing"],
                }
            current_revision_id = current_revision.get("revision_id")
            current_number = current_revision.get("revision")
            if not isinstance(current_revision_id, str) or not isinstance(current_number, int):
                return {
                    "status": "human_required",
                    "goal_id": goal_id,
                    "run_id": None,
                    "reasons": ["goal_revision_identity_missing"],
                }
            next_number = current_number + 1
            candidate_run_goal = copy.deepcopy(run_goal)
            persisted_envelope = candidate_run_goal.get("admission_envelope")
            if not isinstance(persisted_envelope, dict):
                return {
                    "status": "human_required",
                    "goal_id": goal_id,
                    "run_id": None,
                    "reasons": ["delivery_envelope_missing_for_revision_rebind"],
                }
            # Validate and rebind before consuming a Goal revision.  This is
            # deliberately outside the store transaction: all operations here
            # are pure copies; the existing bump path below is the one atomic
            # durable write and is fenced by current_revision_id.
            try:
                rebound_envelope = _rebind_delivery_for_revision(
                    candidate_run_goal, persisted_envelope, next_number,
                )
                next_payload = _payload_with_delivery_binding(
                    current_revision["goal"], candidate_run_goal, rebound_envelope, next_number,
                )
            except (TypeError, ValueError, KeyError) as exc:
                return {
                    "status": "human_required",
                    "goal_id": goal_id,
                    "run_id": None,
                    "reasons": [f"delivery_contract_revision_rebind_failed:{type(exc).__name__}"],
                }
            if existing_run["state"] == "stopped":
                # Failure loop: the revision cap turns an endless fail-retry
                # loop into human_required (unchanged semantics).
                try:
                    bumped = self.goal_store.bump_revision(
                        goal_id,
                        goal_payload=next_payload,
                        expected_revision_id=current_revision_id,
                    )
                except ValueError as exc:
                    message = str(exc)
                    if message == "goal_revision_changed":
                        reason = "goal_revision_changed"
                    elif message.startswith("goal revision cap reached"):
                        reason = "revision_cap_reached"
                    else:
                        reason = f"revision_bump_failed:{type(exc).__name__}"
                    return {
                        "status": "human_required",
                        "goal_id": goal_id,
                        "run_id": None,
                        "reasons": [reason],
                    }
            else:
                # W9g: success cycle. A VERIFIED run must never be re-linked
                # into a revived goal (the W9f day-1 bug: the old verified run
                # was re-linked and its stale receipt consumed). Recurring
                # after a success is a fresh cycle, not a fail-retry loop, so
                # the fail-loop revision cap does not apply here.
                try:
                    bumped = self._bump_revision_after_success(
                        goal_id,
                        goal_payload=next_payload,
                        expected_revision_id=current_revision_id,
                    )
                except ValueError:
                    return {
                        "status": "human_required",
                        "goal_id": goal_id,
                        "run_id": None,
                        "reasons": ["goal_revision_changed"],
                    }
            # Build the Run only from the row just committed.  In particular,
            # its deterministic id is derived from the persisted new envelope,
            # not the caller's pre-bump copy.
            persisted = self.goal_store.get_goal(goal_id)
            persisted_revision = persisted.get("current_revision")
            persisted_payload = persisted_revision.get("goal") if isinstance(persisted_revision, dict) else None
            if not isinstance(persisted_revision, dict) or persisted_revision.get("revision_id") != bumped["revision_id"] or not isinstance(persisted_payload, dict):
                return {
                    "status": "human_required",
                    "goal_id": goal_id,
                    "run_id": None,
                    "reasons": ["goal_revision_readback_mismatch"],
                }
            run_goal = copy.deepcopy(candidate_run_goal)
            run_goal["revision_id"] = persisted_revision["revision_id"]
            run_goal["goal_revision"] = int(persisted_revision["revision"])
            run_goal["admission_envelope"] = copy.deepcopy(persisted_payload.get("admission_envelope", rebound_envelope))
            for key in _DELIVERY_FIELDS:
                if key in persisted_payload:
                    run_goal[key] = copy.deepcopy(persisted_payload[key])
            envelope = copy.deepcopy(run_goal["admission_envelope"])
            revision_id = persisted_revision["revision_id"]
            run_id = _id("run-goal-", {
                "goal_id": goal_id,
                "revision_id": revision_id,
                "base_revision": pinned_revision,
                "envelope": envelope,
            })
        self.run_store.create_run(goal=run_goal, source_repo=source_repo, base_revision=pinned_revision, max_attempts=attempts, run_id=run_id)
        # Preserve the command-to-goal correlation for recurring/revived
        # goals.  Without the explicit event key, activate_with_run falls
        # back to the goal's original source event, so a new standing intent
        # can create a real Run while command-status still reports no goal.
        linked = self.goal_store.activate_with_run(goal_id, run_id, event_key=event_key)
        return {
            "status": "reused" if goal["state"] == "active" else "active",
            "goal_id": goal_id,
            "revision_id": revision_id,
            "run_id": run_id,
            "run_state": self.run_store.get_run(run_id)["state"],
            "goal_state": linked["state"],
        }

    def _bump_revision_after_success(
        self,
        goal_id: str,
        *,
        goal_payload: dict[str, Any] | None = None,
        expected_revision_id: str | None = None,
    ) -> dict[str, Any]:
        """W9g: revision-bump after a VERIFIED run, without the fail-loop cap.

        MAX_GOAL_REVISIONS exists to stop endless fail-retry loops; a verified
        run is a completed success cycle, so recurring (e.g. a daily standing
        health check) starts a fresh cycle and the cap does not apply. Mirrors
        GoalStore.bump_revision's write path against the same store; kept here
        because the capped variant lives in GoalStore and the two policies
        must stay visibly separate."""
        if goal_payload is not None and not isinstance(goal_payload, dict):
            raise ValueError("goal_payload must be an object")
        if expected_revision_id is not None:
            expected_revision_id = _text("expected_revision_id", expected_revision_id)
        now = time.time()
        with self.goal_store._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT current_revision_id FROM goals WHERE goal_id = ?", (goal_id,)).fetchone()
                if row is None:
                    raise KeyError(f"unknown goal_id: {goal_id}")
                current = conn.execute("SELECT * FROM goal_revisions WHERE revision_id = ?", (row["current_revision_id"],)).fetchone()
                if current is None:
                    raise ValueError("goal has no current revision to bump")
                if expected_revision_id is not None and current["revision_id"] != expected_revision_id:
                    raise ValueError("goal_revision_changed")
                next_seq = int(current["revision"]) + 1
                next_payload = json.loads(current["goal_json"]) if goal_payload is None else json.loads(
                    json.dumps(goal_payload, ensure_ascii=False, sort_keys=True)
                )
                next_json = json.dumps(next_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                next_digest = "sha256:" + hashlib.sha256(next_json.encode()).hexdigest()
                revision_id = _id("rev-", {"goal_id": goal_id, "revision": next_seq, "goal": next_payload})
                conn.execute(
                    "INSERT INTO goal_revisions(revision_id, goal_id, revision, goal_json, goal_digest, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (revision_id, goal_id, next_seq, next_json, next_digest, now),
                )
                update_sql = "UPDATE goals SET current_revision_id = ?, updated_at = ? WHERE goal_id = ?"
                update_args: tuple[Any, ...] = (revision_id, now, goal_id)
                if expected_revision_id is not None:
                    update_sql += " AND current_revision_id = ?"
                    update_args += (expected_revision_id,)
                updated = conn.execute(update_sql, update_args)
                if updated.rowcount != 1:
                    raise ValueError("goal_revision_changed")
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {"goal_id": goal_id, "revision": next_seq, "revision_id": revision_id}

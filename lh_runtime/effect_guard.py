#!/usr/bin/env python3
"""Effect guard: re-verify delivery and the target before an external effect.

A finished run is not by itself permission to act outside the workspace.  Any
injected post-run effect (merge, publish, deploy) that goes through
``guarded_dispatch`` gets the same preconditions, whatever service it reaches:

- the run's final delivery verdict is GREEN for its current attempt;
- the attempt's diff does not touch the authority surface;
- a contract carrying candidate review v2 grants no effect by itself: an
  explicit ``lh-effect-grant/v1`` bound to the effect, run, and contract
  digest must be supplied by the injecting project;
- the target is read back right before the effect (again after any wait the
  caller runs) and must still be the reviewed identity on the reviewed base;
- a prepared marker is recorded before the effect is sent, so a lost response
  is settled only by reading the target back, never by sending again.

The engine ships no target.  An injected ``EffectTarget`` implements
``readback`` and an ``op_key``-idempotent ``perform``.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Protocol

if __package__:
    from . import authority_surface, value_reducer
    from . import external_action_port as eap
else:
    import authority_surface
    import external_action_port as eap
    import value_reducer

GRANT_SCHEMA = "lh-effect-grant/v1"
REVIEW_POLICY_SCHEMA = "lh-candidate-review-contract/v2"


class EffectRefused(ValueError):
    """The effect's preconditions do not hold; nothing was sent or recorded."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class EffectTarget(Protocol):
    def readback(self, request: dict[str, Any]) -> dict[str, Any]:
        """``{"state": "absent" | "done", "identity": {...}, "result": {...}}``."""

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Send the effect; the external side must deduplicate on ``op_key``."""


def _keys(run_id: str, effect: str, expected: Mapping[str, Any]) -> tuple[str, str, str]:
    payload = dict(expected)
    return (eap.operation_key(run_id, effect, payload),
            eap.operation_key(run_id, effect + "-prepare", payload),
            eap.operation_key(run_id, effect + "-readback", payload))


def delivery_guard(run_store: Any, run_id: str, *, effect: str,
                   grant: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return the final delivery binding, or raise ``EffectRefused``."""
    try:
        if run_store.delivery_required(run_id) is not True:
            raise EffectRefused("delivery_binding_required_missing")
        binding = run_store.delivery_preflight(run_id, phase="final")
        if not isinstance(binding, dict) or binding.get("verdict") != "GREEN":
            reason = binding.get("reason") if isinstance(binding, dict) else None
            raise EffectRefused(str(reason or "delivery_binding_invalid"))
        latest = run_store.latest_attempt(run_id)
        ordinal = latest.get("ordinal") if isinstance(latest, dict) else None
        if type(ordinal) is not int or ordinal < 1:
            raise EffectRefused("delivery_attempt_not_current")
        final = run_store.verify_delivery(run_id, phase="final", ordinal=ordinal)
        if not isinstance(final, dict) or final.get("verdict") != "GREEN":
            reason = final.get("reason") if isinstance(final, dict) else None
            raise EffectRefused(str(reason or "delivery_final_not_green"))
        touched = value_reducer.touched_files(run_store.read_artifact(run_id, ordinal, "diff.patch"))
        protected = authority_surface.authority_paths(touched)
        if protected:
            raise EffectRefused("authority_surface_touched:" + ",".join(protected))
        contract = binding.get("contract") if isinstance(binding.get("contract"), dict) else {}
        review = contract.get("candidate_review")
        if isinstance(review, dict) and review.get("schema") == REVIEW_POLICY_SCHEMA:
            # Reviewed source delivery is not effect authority; the project grants it.
            if grant is None:
                raise EffectRefused("candidate_review_v2_effect_grant_missing")
            if (not isinstance(grant, Mapping) or set(grant) != {"schema", "effect", "run_id", "contract_digest"}
                    or grant.get("schema") != GRANT_SCHEMA or grant.get("effect") != effect
                    or grant.get("run_id") != run_id or grant.get("contract_digest") != contract.get("contract_digest")):
                raise EffectRefused("candidate_review_v2_effect_grant_invalid")
        return binding
    except EffectRefused:
        raise
    except (AttributeError, KeyError, TypeError, ValueError, OSError) as exc:
        raise EffectRefused("delivery_readback_unavailable:" + type(exc).__name__) from exc


def _observe(target: EffectTarget, request: dict[str, Any], expected: Mapping[str, Any]) -> dict[str, Any]:
    observed = target.readback(dict(request))
    if not isinstance(observed, dict) or observed.get("state") not in {"absent", "done"}:
        raise EffectRefused("effect_target_readback_unknown")
    if observed.get("identity") != dict(expected):
        raise EffectRefused("effect_target_changed")
    return observed


def guarded_dispatch(ledger: eap.ActionLedger, target: EffectTarget, *, run_store: Any, run_id: str,
                     effect: str, expected: Mapping[str, Any], request: dict[str, Any], at: float,
                     grant: Mapping[str, Any] | None = None,
                     before_effect: Callable[[], None] | None = None) -> dict[str, Any]:
    """Send one post-run effect at most once, only while its preconditions hold."""
    op_key, prepare_key, readback_key = _keys(run_id, effect, expected)
    for key in (op_key, readback_key):
        recorded = ledger.get(key)
        if recorded is not None:
            return {"status": "done", "deduped": True, "op_key": op_key, "result": recorded, "performed": False}
    if ledger.get(prepare_key) is not None:
        # Sent before with an unknown outcome: only the target can settle it.
        try:
            observed = _observe(target, request, expected)
        except EffectRefused as exc:
            return {"status": "unknown", "op_key": op_key, "reason": "effect_outcome_unresolved:" + exc.reason}
        if observed["state"] != "done":
            return {"status": "unknown", "op_key": op_key, "reason": "effect_outcome_unresolved_no_replay"}
        ledger.put(readback_key, {**observed.get("result", {}), "confirmed_by_readback": True}, at=at)
        return {"status": "done", "deduped": False, "op_key": op_key, "result": observed.get("result", {}),
                "performed": False, "confirmed_by_readback": True}
    try:
        base = run_store.get_run(run_id).get("base_revision")
        if not isinstance(base, str) or expected.get("base") != base:
            raise EffectRefused("effect_base_not_reviewed")
        checks = (before_effect, None)
        for wait in checks:
            delivery_guard(run_store, run_id, effect=effect, grant=grant)
            observed = _observe(target, request, expected)
            if observed["state"] == "done":
                ledger.put(readback_key, {**observed.get("result", {}), "confirmed_by_readback": True}, at=at)
                return {"status": "done", "deduped": False, "op_key": op_key, "result": observed.get("result", {}),
                        "performed": False, "already_done": True}
            if wait is not None:
                # Earlier checks cannot authorize an effect after the caller's wait.
                wait()
    except EffectRefused as exc:
        return {"status": "refused", "op_key": op_key, "reason": exc.reason, "performed": False}
    ledger.put(prepare_key, {"prepared": True, "unknown": True, "run_id": run_id, "effect": effect,
                             "expected": dict(expected)}, at=at)
    result = target.perform(op_key, {**request, "expected": dict(expected)})
    ledger.put(op_key, result, at=at)
    return {"status": "done", "deduped": False, "op_key": op_key, "result": result, "performed": True}

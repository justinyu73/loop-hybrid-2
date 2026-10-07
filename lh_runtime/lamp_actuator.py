#!/usr/bin/env python3
"""Lamp actuation: act on a lit lamp mechanically, unless the lamp is the owner's.

A lamp that lights is not by itself a human gate.  For the lamp kinds a pinned
policy (``lh-lamp-actuation-policy/v1``) allows, ``actuate`` runs that kind's
named verb once per incident and appends a receipt.  By construction it never:

- runs a command string taken from a snapshot: it reads only the ids of the
  fired rules and dispatches to ``VERBS``, a closed table whose arguments it
  builds itself; a policy entry naming an unknown verb is skipped;
- acts while the policy bytes differ from the digest the contract pins: it halts,
  records why, and the status lamp lights ``lamp_actuation_policy_drift``;
- acts on an ``OWNER_ONLY`` kind, even when the policy lists it.

An incident is a kind from the moment it starts firing until it stops; it is
actuated at most once.  State lives beside the goal store: ``lamp-actuation.json``
and the append-only ``lamp-actuation-receipts.jsonl``.

The one verb today is ``retention_reclaim`` (``retention.plan`` then ``apply``)
for the ``scratch_reclaimable`` lamp.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    from . import retention
except ImportError:  # direct execution keeps lh_runtime on sys.path
    import retention  # type: ignore

SCHEMA = "lh-lamp-actuation/v1"
POLICY_SCHEMA = "lh-lamp-actuation-policy/v1"
RECEIPT_SCHEMA = "lh-lamp-actuation-receipt/v1"
STATE_FILE = "lamp-actuation.json"
RECEIPTS_FILE = "lamp-actuation-receipts.jsonl"
DEFAULT_SCRATCH_THRESHOLD = 20
# Never actuated, whatever a policy says: these are the owner's, or the host's.
OWNER_ONLY = frozenset({
    "needs_human", "dispatch_stopped", "integrity_check_red", "code_identity_stale", "heartbeat_stale",
    "lamp_actuation_policy_drift",
})


def _retention_reclaim(*, run_store_root: Path, **_ignored: Any) -> dict[str, Any]:
    applied = retention.apply(retention.plan(run_store_root))
    return {"removed": len(applied["remove"]) - len(applied["errors"]), "errors": applied["errors"]}


VERBS: dict[str, Callable[..., dict[str, Any]]] = {"retention_reclaim": _retention_reclaim}


def validate_config(raw: Any) -> dict[str, Any]:
    """The contract's ``lamp_actuation`` block: policy, policy_digest, scratch_threshold."""
    if not isinstance(raw, dict):
        raise ValueError("lamp_actuation must be an object")
    unknown = set(raw) - {"policy", "policy_digest", "scratch_threshold"}
    if unknown:
        raise ValueError(f"lamp_actuation has unknown fields: {sorted(unknown)}")
    if not isinstance(raw.get("policy"), str) or not raw["policy"].strip():
        raise ValueError("lamp_actuation.policy must name the policy file")
    digest = raw.get("policy_digest")
    if not isinstance(digest, str) or not digest.startswith("sha256:") or len(digest) != 71:
        raise ValueError("lamp_actuation.policy_digest must be a sha256: digest")
    threshold = raw.get("scratch_threshold", DEFAULT_SCRATCH_THRESHOLD)
    if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold < 1:
        raise ValueError("lamp_actuation.scratch_threshold must be a positive integer")
    return {"policy": raw["policy"], "policy_digest": digest, "scratch_threshold": threshold}


def policy_digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def state_path(goal_store_root: str | Path) -> Path:
    return Path(goal_store_root) / STATE_FILE


def _read_state(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"schema": SCHEMA, "halted": False, "incidents": {}}
    if not isinstance(data, dict) or data.get("schema") != SCHEMA or not isinstance(data.get("incidents"), dict):
        return {"schema": SCHEMA, "halted": False, "incidents": {}}
    return data


def project(goal_store_root: str | Path, *, enabled: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """The snapshot field the lamp reads; None when actuation is not enabled."""
    if enabled is None:
        return None
    state = _read_state(state_path(goal_store_root))
    return {"halted": state.get("halted") is True, "reason": state.get("reason")}


def scratch_projection(run_store_root: str | Path, *, enabled: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """How much scratch retention could reclaim now; None when actuation is not enabled."""
    if enabled is None:
        return None
    try:
        reclaimable: int | None = len(retention.plan(Path(run_store_root))["remove"])
    except (OSError, ValueError):
        reclaimable = None
    return {"reclaimable": reclaimable, "threshold": int(enabled.get("scratch_threshold", DEFAULT_SCRATCH_THRESHOLD))}


def _load_policy(config: Mapping[str, Any]) -> tuple[dict[str, str] | None, str | None]:
    try:
        raw = Path(config["policy"]).read_bytes()
    except OSError:
        return None, "policy_unreadable"
    if policy_digest(raw) != config["policy_digest"]:
        return None, "policy_digest_mismatch"
    try:
        data = json.loads(raw.decode("utf-8"))
    except ValueError:
        return None, "policy_invalid"
    allow = data.get("allow") if isinstance(data, dict) and data.get("schema") == POLICY_SCHEMA else None
    if not isinstance(allow, list) or not all(isinstance(row, dict) and isinstance(row.get("kind"), str)
                                              and isinstance(row.get("verb"), str) for row in allow):
        return None, "policy_invalid"
    return {row["kind"]: row["verb"] for row in allow}, None


def actuate(snapshot: Mapping[str, Any], *, config: Mapping[str, Any], run_store_root: str | Path,
            goal_store_root: str | Path, now: float | None = None) -> dict[str, Any]:
    now = time.time() if now is None else float(now)
    path = state_path(goal_store_root)
    state = _read_state(path)
    allow, problem = _load_policy(config)
    if problem is not None:
        state.update({"halted": True, "reason": problem, "halted_at": now})
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {"schema": SCHEMA, "halted": True, "reason": problem, "actions": []}
    lamp = snapshot.get("lamp") if isinstance(snapshot.get("lamp"), Mapping) else {}
    # Only rule ids are read: nothing else in the snapshot can steer what runs.
    kinds = [str(row.get("id")) for row in lamp.get("fired", []) if isinstance(row, Mapping)]
    incidents = {kind: value for kind, value in state["incidents"].items() if kind in kinds}
    actions = []
    for kind in kinds:
        incident = incidents.setdefault(kind, {"first_seen": now})
        if kind in OWNER_ONLY:
            actions.append({"kind": kind, "outcome": "owner_only"})
            continue
        verb = (allow or {}).get(kind)
        if verb is None:
            actions.append({"kind": kind, "outcome": "not_allowed"})
            continue
        if verb not in VERBS:
            actions.append({"kind": kind, "verb": verb, "outcome": "unknown_verb_skipped"})
            continue
        if "actuated_at" in incident:
            actions.append({"kind": kind, "verb": verb, "outcome": "already_actuated"})
            continue
        result = VERBS[verb](run_store_root=Path(run_store_root))
        incident["actuated_at"] = now
        receipt = {"schema": RECEIPT_SCHEMA, "kind": kind, "verb": verb, "incident_first_seen": incident["first_seen"],
                   "actuated_at": now, "policy_digest": config["policy_digest"], "result": result}
        receipts = Path(goal_store_root) / RECEIPTS_FILE
        receipts.parent.mkdir(parents=True, exist_ok=True)
        with receipts.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(receipt, ensure_ascii=False, sort_keys=True) + "\n")
        actions.append({"kind": kind, "verb": verb, "outcome": "actuated", "result": result})
    state = {"schema": SCHEMA, "halted": False, "reason": None, "incidents": incidents, "checked_at": now}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"schema": SCHEMA, "halted": False, "actions": actions}

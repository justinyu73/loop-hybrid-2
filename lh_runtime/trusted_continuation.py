"""Append-only continuation records, used only by WorkUnitStore transactions.

The original trusted budget is evidence, never a writable continuation account.
Every caller must supply the exact separately approved authority digest.
"""
from __future__ import annotations

import copy
import json
import math
import re
import hashlib
import os
import subprocess
from pathlib import Path


def _api():
    if __package__:
        from .work_unit_store import WorkUnitStoreError, digest_json
    else:
        from work_unit_store import WorkUnitStoreError, digest_json
    return WorkUnitStoreError, digest_json


def _process_dead(identity, *, identity_port=None):
    if __package__:
        from .successor_executor import disposition_process_dead
    else:
        from successor_executor import disposition_process_dead
    return disposition_process_dead(identity, identity_port=identity_port)


def _require(condition, reason):
    if not condition:
        error, _ = _api()
        raise error("continuation_" + reason)


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def _number(value, *, integer=False, positive=True):
    return (not isinstance(value, bool) and isinstance(value, int if integer else (int, float))
            and math.isfinite(value) and (value > 0 if positive else value >= 0))


def _exists(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trusted_continuation_authorities'").fetchone() is not None


def _schema(conn):
    # Deliberately not a Store-constructor migration: R1/R2 do not create these.
    for sql in (
        "CREATE TABLE IF NOT EXISTS trusted_continuation_authorities (authority_digest TEXT PRIMARY KEY, authority_id TEXT NOT NULL UNIQUE, goal_id TEXT NOT NULL, goal_revision INTEGER NOT NULL, authority_json TEXT NOT NULL, created_at REAL NOT NULL)",
        "CREATE TABLE IF NOT EXISTS trusted_continuation_reservations (reservation_key TEXT PRIMARY KEY, authority_digest TEXT NOT NULL, goal_id TEXT NOT NULL, goal_revision INTEGER NOT NULL, phase_key TEXT NOT NULL, reservation_json TEXT NOT NULL, created_at REAL NOT NULL, UNIQUE(goal_id,goal_revision,phase_key), FOREIGN KEY(authority_digest) REFERENCES trusted_continuation_authorities(authority_digest))",
        "CREATE TABLE IF NOT EXISTS trusted_continuation_observations (reservation_key TEXT PRIMARY KEY, observation_json TEXT NOT NULL, observation_digest TEXT NOT NULL, created_at REAL NOT NULL, FOREIGN KEY(reservation_key) REFERENCES trusted_continuation_reservations(reservation_key))",
    ):
        conn.execute(sql)


def _successor_schema(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trusted_continuation_successors "
        "(successor_digest TEXT PRIMARY KEY, predecessor_digest TEXT NOT NULL "
        "UNIQUE, goal_id TEXT NOT NULL, goal_revision INTEGER NOT NULL, "
        "operation_ref_json TEXT NOT NULL, created_at REAL NOT NULL, "
        "FOREIGN KEY(successor_digest) REFERENCES "
        "trusted_continuation_authorities(authority_digest), "
        "FOREIGN KEY(predecessor_digest) REFERENCES "
        "trusted_continuation_authorities(authority_digest))"
    )


_AUTHORITY_FIELDS = {"schema", "authority_id", "goal_id", "goal_revision", "original_budget_digest",
    "original_execution_binding_digest", "execution_binding_digest", "policy_digest", "original_deadline_at",
    "unknown_reservation_keys", "historical_cli_launches", "historical_consumption", "reference_cli_limit",
    "max_new_cli_launches", "max_wall_seconds", "max_observed_tokens", "expires_at", "scope",
    "owner_disposition", "supersedes", "authority_digest", "original_operation_ref"}
_V2_FIELDS = _AUTHORITY_FIELDS | {"original_policy_digest", "policy_transition", "repair_scopes", "runtime_source"}
_RED_PHASES = {"checks", "verifier", "integration_checks", "integration_verifier"}


def _authority_shape(value):
    return isinstance(value, dict) and ((value.get("schema") == "lh-trusted-continuation-authority/v1"
        and set(value) == _AUTHORITY_FIELDS) or (value.get("schema") == "lh-trusted-continuation-authority/v2"
        and set(value) == _V2_FIELDS))


def _table(conn, name):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _state_root(conn):
    return Path(next(row[2] for row in conn.execute("PRAGMA database_list") if row[1] == "main")).resolve().parent


def _object(raw, reason):
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        _require(False, reason)
    _require(isinstance(value, dict), reason)
    return value


def get(conn, digest):
    if not _exists(conn):
        return None
    row = conn.execute("SELECT * FROM trusted_continuation_authorities WHERE authority_digest=?", (digest,)).fetchone()
    if row is None:
        return None
    _, digest_json = _api()
    value = _object(row["authority_json"], "authority_unreadable")
    _require(_authority_shape(value), "authority_shape_invalid")
    _require(_digest(digest) and digest == value["authority_digest"]
        == digest_json({k: v for k, v in value.items() if k != "authority_digest"})
        and all(row[k] == value[k] for k in ("authority_digest", "authority_id", "goal_id", "goal_revision"))
        and _number(row["created_at"]), "authority_durable_binding_drift")
    return value


def _reservation(conn, row):
    _, digest_json = _api()
    value = _object(row["reservation_json"], "reservation_unreadable")
    fields = {"schema", "reservation_key", "state", "goal_id", "goal_revision", "policy_digest", "phase_key",
        "run_id", "attempt", "fence", "phase", "command_digest", "is_provider_cli", "reserved_at", "deadline_at",
        "continuation_authority_digest", "reservation_digest"}
    _require(set(value) == fields and value["schema"] == "lh-trusted-launch-reservation/v1"
        and value["state"] == "reserved" and isinstance(value["is_provider_cli"], bool), "reservation_shape_invalid")
    _require(all(_digest(value[k]) for k in ("reservation_key", "policy_digest", "phase_key", "command_digest",
        "continuation_authority_digest", "reservation_digest"))
        and value["reservation_digest"] == digest_json({k: v for k, v in value.items() if k != "reservation_digest"})
        and all(row[k] == value[k] for k in ("reservation_key", "goal_id", "goal_revision", "phase_key"))
        and row["authority_digest"] == value["continuation_authority_digest"]
        and row["created_at"] == value["reserved_at"]
        and value["reservation_key"] == digest_json([row["authority_digest"], value["goal_id"], value["goal_revision"], value["phase_key"]]),
        "reservation_durable_binding_drift")
    authority = get(conn, row["authority_digest"])
    _require(authority is not None and all(value[k] == authority[k] for k in ("goal_id", "goal_revision", "policy_digest"))
        and all(_number(value[k], integer=True) for k in ("goal_revision", "attempt", "fence"))
        and all(_number(value[k]) for k in ("reserved_at", "deadline_at"))
        and value["reserved_at"] < value["deadline_at"] <= authority["expires_at"]
        and isinstance(value["phase"], str) and re.fullmatch(r"[a-z][a-z0-9_:-]{0,127}", value["phase"]) is not None
        and any(_scope_matches(conn, authority, scope, value) for scope in authority["scope"]),
        "reservation_authority_binding_drift")
    return value


def _observation(row, reservation):
    if row is None:
        return None
    _, digest_json = _api()
    value = _object(row["observation_json"], "observation_unreadable")
    _require(set(value) == {"schema", "outcome", "reason_code", "process_terminated", "elapsed_seconds", "usage"}
        and value["schema"] == "lh-trusted-launch-observation/v1"
        and value["outcome"] in {"completed", "known_failure", "unknown"}
        and value["reason_code"] in {"completed", "process_exit_nonzero", "process_timeout", "process_identity_unknown",
            "process_readback_unknown", "provider_protocol_invalid", "provider_failed", "provider_usage_unknown",
            "output_limit_exceeded", "callback_failed", "provider_result_invalid"}
        and isinstance(value["process_terminated"], bool)
        and _number(value["elapsed_seconds"], positive=False), "observation_shape_invalid")
    _require(row["reservation_key"] == reservation["reservation_key"]
        and row["observation_digest"] == digest_json(value) and _number(row["created_at"])
        and reservation["is_provider_cli"] == (value["usage"] is not None), "observation_durable_binding_drift")
    if value["usage"] is not None:
        if __package__:
            from .cli_agent_executor import validate_trusted_usage
        else:
            from cli_agent_executor import validate_trusted_usage
        validate_trusted_usage(value["usage"])
    return value


def _original(conn, value):
    _, digest_json = _api()
    row = conn.execute("SELECT * FROM trusted_execution_budgets WHERE goal_id=? AND goal_revision=?",
        (value["goal_id"], value["goal_revision"])).fetchone()
    _require(row is not None, "original_budget_missing")
    budget = json.loads(row["budget_json"])
    _require(digest_json(budget) == value["original_budget_digest"]
        and row["execution_binding_digest"] == value["original_execution_binding_digest"]
        and budget["policy_digest"] == value.get("original_policy_digest", value["policy_digest"])
        and budget["deadline_at"] == value["original_deadline_at"], "original_binding_drift")
    return budget


def _read_reference(reference, name):
    _require(isinstance(reference, dict) and set(reference) in ({"path", "sha256"}, {"path", "digest"}), name + "_reference_invalid")
    path = Path(reference["path"]).expanduser()
    _require(path.is_absolute() and not any(p.is_symlink() for p in (path, *path.parents))
        and path.is_file() and path.stat().st_size <= 1048576, name + "_path_invalid")
    raw = path.read_bytes()
    expected = reference.get("sha256", reference.get("digest", "")).removeprefix("sha256:")
    _require(hashlib.sha256(raw).hexdigest() == expected, name + "_digest_drift")
    return _object(raw, name + "_not_object")


def history_binding(ref, *, goal_id, goal_revision, supersedes=None):
    """Resolve the immutable original approval, not a caller-selected subset."""
    _, digest_json = _api()
    manifest = _read_reference(ref, "original_operation")
    digest = digest_json({k: v for k, v in manifest.items() if k != "approval"})
    approval = manifest.get("approval", {})
    _require(manifest.get("goal_id") == goal_id and manifest.get("goal_revision") == goal_revision
        and approval.get("status") == "approved" and approval.get("manifest_digest") == digest
        and (supersedes is None or supersedes == digest), "original_approval_binding_invalid")
    operation = manifest.get("operation", {})
    history = _read_reference(operation.get("history_ref"), "history")
    _require(isinstance(history.get("goal_id"), str) and bool(history["goal_id"])
        and _number(history.get("goal_revision"), integer=True), "history_goal_invalid")
    if "outer_goal_id" in operation or "outer_goal_revision" in operation:
        _require(history["goal_id"] == operation.get("outer_goal_id")
            and history["goal_revision"] == operation.get("outer_goal_revision"), "history_goal_binding_invalid")
    expected = []
    for item in history.get("runs", []):
        row = item.get("dispatch", {})
        _require(all(k in row for k in ("dispatch_key", "receipt_digest", "executor_launches")), "history_dispatch_missing")
        expected.append({"goal_id": history["goal_id"], "goal_revision": history["goal_revision"],
            **{k: row[k] for k in ("dispatch_key", "receipt_digest", "executor_launches")}})
    _require(bool(expected) and len({r["dispatch_key"] for r in expected}) == len(expected)
        and all(_number(r["executor_launches"], integer=True) for r in expected)
        and sum(r["executor_launches"] for r in expected) == history.get("legacy_launches") == operation.get("legacy_launches"), "history_total_binding_invalid")
    return manifest, sorted(expected, key=lambda r: r["dispatch_key"])


def _history(conn, value):
    _, digest_json = _api()
    rows = value["historical_consumption"]
    manifest, expected = history_binding(value["original_operation_ref"], goal_id=value["goal_id"],
        goal_revision=value["goal_revision"], supersedes=value["supersedes"])
    _require(digest_json(manifest.get("execution_binding")) == value["original_execution_binding_digest"], "original_execution_binding_invalid")
    _require(isinstance(rows, list) and bool(rows), "history_missing")
    _require(sorted(rows, key=lambda r: r["dispatch_key"]) == expected, "history_scope_incomplete")
    seen, total = set(), 0
    for ref in rows:
        _require(isinstance(ref, dict) and set(ref) == {
            "goal_id", "goal_revision", "dispatch_key", "receipt_digest", "executor_launches"}, "history_shape_invalid")
        _require(ref["dispatch_key"] not in seen and _number(ref["executor_launches"], integer=True), "history_duplicate_or_invalid")
        seen.add(ref["dispatch_key"])
        row = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (ref["dispatch_key"],)).fetchone()
        _require(row is not None and row["goal_id"] != value["goal_id"] and all(
            row[key] == ref[key] for key in ("goal_id", "goal_revision", "receipt_digest")), "history_binding_drift")
        receipt = json.loads(row["receipt_json"])
        _require(receipt.get("receipt_digest") == ref["receipt_digest"]
            and digest_json({k: v for k, v in receipt.items() if k != "receipt_digest"}) == ref["receipt_digest"]
            and receipt.get("executor_launches") == ref["executor_launches"], "history_consumption_drift")
        total += ref["executor_launches"]
    _require(total == value["historical_cli_launches"], "history_total_invalid")
    return manifest


def bootstrap_authority_for_source(original, root):
    """Reconstruct a source-bound tuple; this never records or grants authority.

    A changed document requires the exact registered source transition artifact.
    The enclosing continuation still binds clean source/ancestry and requires
    its own new exact operation approval. Historical records remain untouched.
    """
    fields = {"root", "authority_ref", "decision_id", "authority_digest"}
    _require(isinstance(original, dict) and set(original) == fields
        and all(isinstance(original[k], str) and original[k] for k in fields),
        "bootstrap_identity_changed")
    relative = "docs/bootstrap-authority.md"
    anchor = "lh-external-bootstrap-001"
    _require(original["authority_ref"] == relative + "#" + anchor
        and original["decision_id"] == "LH-EXTERNAL-BOOTSTRAP-001"
        and _digest(original["authority_digest"]), "bootstrap_identity_changed")

    def read_at(base, name):
        path = Path(base) / name
        try:
            _require(path.is_absolute() and path.is_file()
                and not any(p.is_symlink() for p in (path, *path.parents))
                and path.stat().st_size <= 1048576, "bootstrap_path_invalid")
            return path.read_bytes()
        except OSError as exc:
            _require(False, "bootstrap_reference_unreadable")
            raise AssertionError from exc

    root = Path(root)
    raw = read_at(root, relative)
    observed = "sha256:" + hashlib.sha256(raw).hexdigest()
    _require(('<a id="' + anchor + '"></a>').encode() in raw,
        "bootstrap_authority_drift")
    binding = copy.deepcopy(original)
    binding["root"] = str(root)
    if observed == original["authority_digest"]:
        return binding

    historical = read_at(original["root"], relative)
    _require("sha256:" + hashlib.sha256(historical).hexdigest()
        == original["authority_digest"], "bootstrap_original_authority_drift")
    path = "docs/codex-handoff/p8-repo-source/BOOTSTRAP-AUTHORITY-TRANSITION.json"
    transition_raw = read_at(root, path)
    _require(hashlib.sha256(transition_raw).hexdigest()
        == "491f7e95ee2d5459847b872f0f4ebb6fc017c7e4bac4c37d3b053b54bd57be99",
        "bootstrap_transition_digest_drift")
    transition = _object(transition_raw, "bootstrap_transition_invalid")
    identity = {k: original[k] for k in fields - {"root"}}
    replacement = {**identity, "authority_digest": observed}
    _require(transition.get("schema") == "host-bootstrap-authority-source-transition/v1"
        and transition.get("decision_id") == "HOST-P8-BOOTSTRAP-AUTHORITY-TRANSITION-20260923-1"
        and transition.get("supersedes") == identity
        and transition.get("replacement") == replacement
        and transition.get("allowed_binding_changes") == ["root", "authority_digest"]
        and transition.get("requires_new_operation_exact_approval") is True
        and transition.get("production_apply_authorized") is False
        and transition.get("provider_launch_authorized") is False,
        "bootstrap_transition_scope_invalid")
    binding["authority_digest"] = observed
    return binding


def execution_binding(value, manifest):
    """Resolve the two named v2 differences; every other binding stays exact."""
    _, digest_json = _api()
    original = manifest["execution_binding"]
    policy = _read_reference(original["execution_policy_ref"], "original_policy")
    if value["schema"] != "lh-trusted-continuation-authority/v2":
        _require(digest_json(policy) == value["policy_digest"] and _number(policy.get("expires_at"))
            and value["expires_at"] <= policy["expires_at"], "policy_expiry_expanded")
        return original, policy
    transition = value["policy_transition"]
    _require(isinstance(transition, dict) and set(transition) == {
        "schema", "original_policy_ref", "new_policy_ref"}
        and transition["schema"] == "lh-trusted-policy-window-transition/v1"
        and transition["original_policy_ref"] == original["execution_policy_ref"]
        and value["original_policy_digest"] == digest_json(policy), "policy_transition_invalid")
    newer = _read_reference(transition["new_policy_ref"], "new_policy")
    _require(set(newer) == set(policy) and all(newer[k] == policy[k] for k in policy if k != "expires_at")
        and _number(newer.get("expires_at")) and newer["expires_at"] > policy["expires_at"]
        and value["expires_at"] <= newer["expires_at"] and digest_json(newer) == value["policy_digest"],
        "policy_transition_expands_scope")
    source = value["runtime_source"]
    _require(isinstance(source, dict) and set(source) == {"root", "commit", "tree"}
        and all(isinstance(source[k], str) and source[k] for k in source), "runtime_source_invalid")
    root = Path(source["root"])
    _require(root.is_absolute() and root.is_dir() and not any(p.is_symlink() for p in (root, *root.parents)),
        "runtime_source_path_invalid")
    env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")
    def git(*args):
        result = subprocess.run(["git", "--no-optional-locks", "-C", str(root), *args],
            env=env, capture_output=True, text=True, timeout=15)
        _require(result.returncode == 0, "runtime_source_ancestry_invalid")
        return result.stdout.strip()
    original_sha = manifest.get("operation", {}).get("source_sha")
    _require(isinstance(original_sha, str) and re.fullmatch(r"[0-9a-f]{40,64}", original_sha) is not None,
        "original_source_missing")
    _require(git("rev-parse", "HEAD^{tree}") == source["tree"]
        and git("rev-parse", source["commit"] + "^{tree}") == source["tree"]
        and not git("status", "--porcelain", "--untracked-files=all"), "runtime_source_drift")
    git("merge-base", "--is-ancestor", original_sha, source["commit"])
    git("merge-base", "--is-ancestor", source["commit"], "HEAD")
    binding = copy.deepcopy(original)
    binding["execution_policy_ref"] = copy.deepcopy(transition["new_policy_ref"])
    binding["bootstrap_authority"] = bootstrap_authority_for_source(
        original["bootstrap_authority"], root)
    _require(digest_json(binding) == value["execution_binding_digest"], "execution_binding_expands_scope")
    return binding, newer


def _repair_scopes(conn, value):
    if value["schema"] != "lh-trusted-continuation-authority/v2":
        return []
    scopes = value["repair_scopes"]
    preserved = {s["dispatch_key"]: s for s in value["scope"] if not s["allow_coding"]}
    _require(isinstance(scopes, list) and len(scopes) == len(preserved)
        and {s.get("dispatch_key") for s in scopes if isinstance(s, dict)} == set(preserved), "repair_scope_invalid")
    fields = {"dispatch_key", "run_id", "attempt", "fence", "next_attempt", "next_fence", "max_repairs",
        "allowed_red_phases", "workspace_ref", "integration_workspace_ref"}
    for scope in scopes:
        old = preserved[scope["dispatch_key"]]
        _require(set(scope) == fields and all(scope[k] == old[k] for k in ("run_id", "attempt", "fence"))
            and all(_number(scope[k], integer=True) for k in ("attempt", "fence", "next_attempt", "next_fence", "max_repairs"))
            and scope["max_repairs"] == 1 and scope["next_attempt"] == scope["attempt"] + 1
            and scope["next_fence"] == scope["fence"] + 1
            and isinstance(scope["allowed_red_phases"], list) and len(scope["allowed_red_phases"]) == 4
            and set(scope["allowed_red_phases"]) == _RED_PHASES, "repair_limits_invalid")
        prefix = _state_root(conn) / "continuation-repairs" / scope["run_id"]
        name = f"a{scope['next_attempt']}-f{scope['next_fence']}"
        for key, suffix in (("workspace_ref", ""), ("integration_workspace_ref", "-integration")):
            path = Path(scope[key])
            _require(path == prefix / (name + suffix) and path.is_absolute()
                and not any(p.is_symlink() for p in (path, *path.parents)), "repair_workspace_outside_scope")
    return scopes


def get_repair(conn, dispatch_key):
    if not _table(conn, "trusted_continuation_repairs"):
        return None
    row = conn.execute("SELECT * FROM trusted_continuation_repairs WHERE dispatch_key=?", (dispatch_key,)).fetchone()
    if row is None:
        return None
    _, digest_json = _api()
    value = _object(row["repair_json"], "repair_unreadable")
    _require(set(value) == {"schema", "authority_digest", "dispatch_key", "run_id", "scope", "original_dispatch_receipt",
        "admission_digest", "red_phase_key", "red_receipt_digest", "snapshot_receipt", "created_at", "repair_digest"}
        and value["schema"] == "lh-trusted-continuation-repair/v1"
        and value["repair_digest"] == digest_json({k: v for k, v in value.items() if k != "repair_digest"})
        and all(value[k] == row[k] for k in ("authority_digest", "dispatch_key", "run_id", "repair_digest", "created_at")),
        "repair_durable_binding_drift")
    authority = get(conn, value["authority_digest"])
    _require(authority is not None and value["scope"] in _repair_scopes(conn, authority), "repair_authority_drift")
    scope = value["scope"]
    old = value["original_dispatch_receipt"]
    _require(isinstance(old, dict) and old.get("receipt_digest") == digest_json({k: v for k, v in old.items() if k != "receipt_digest"})
        and old.get("executor_status") == "unknown" and all(old.get(k) == scope[k] for k in ("run_id", "attempt", "fence", "dispatch_key")),
        "repair_original_receipt_drift")
    phase = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (value["red_phase_key"],)).fetchone()
    evidence = json.loads(phase["evidence_json"]) if phase else {}
    _require(phase is not None and phase["state"] == "settled" and phase["phase"] in scope["allowed_red_phases"]
        and evidence.get("verdict") == "RED" and evidence.get("receipt_digest") == value["red_receipt_digest"]
        and evidence.get("receipt_digest") == digest_json({k: v for k, v in evidence.items() if k != "receipt_digest"})
        and all(phase[k] == scope[k] for k in ("run_id", "attempt", "fence")), "repair_red_receipt_drift")
    snapshot = value["snapshot_receipt"]
    saved = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (snapshot.get("phase_key"),)).fetchone()
    _require(saved is not None and saved["state"] == "settled" and saved["phase"] == "continuation_repair"
        and json.loads(saved["evidence_json"]) == snapshot
        and snapshot.get("receipt_digest") == digest_json({k: v for k, v in snapshot.items() if k != "receipt_digest"})
        and snapshot.get("workspace_ref") == scope["workspace_ref"]
        and snapshot.get("authority_digest") == value["authority_digest"]
        and snapshot.get("red_receipt_digest") == value["red_receipt_digest"], "repair_snapshot_receipt_drift")
    return value


def _scope_matches(conn, authority, scope, fields):
    if all(fields[k] == scope[k] for k in ("run_id", "attempt", "fence")):
        return fields["phase"] != "coding" or scope["allow_coding"]
    repair = get_repair(conn, scope["dispatch_key"])
    return (repair is not None and repair["authority_digest"] == authority["authority_digest"]
        and fields["run_id"] == scope["run_id"] and fields["attempt"] == repair["scope"]["next_attempt"]
        and fields["fence"] == repair["scope"]["next_fence"])


def preserved_admission(conn, value, scope, *, identity_port=None):
    """Original evidence remains valid even after the current dispatch is A2."""
    _, digest_json = _api()
    row = conn.execute("SELECT receipt_json FROM dispatch_consumptions WHERE dispatch_key=?", (scope["dispatch_key"],)).fetchone()
    repaired = get_repair(conn, scope["dispatch_key"])
    receipt = repaired["original_dispatch_receipt"] if repaired else (json.loads(row[0]) if row else {})
    row = conn.execute("SELECT admission_json FROM candidate_recovery_admissions WHERE dispatch_key=?", (scope["dispatch_key"],)).fetchone()
    admission = json.loads(row[0]) if row else {}
    recovery = receipt.get("executor_recovery_receipt") or {}
    _require(admission.get("kind") == "preserved_result_recovery" and receipt.get("executor_status") == "unknown"
        and admission.get("current_dispatch_receipt_digest") == receipt.get("receipt_digest")
        and admission.get("current_unknown_recovery_receipt_digest") == recovery.get("recovery_receipt_digest")
        and recovery.get("recovery_receipt_digest") == digest_json({k: v for k, v in recovery.items() if k != "recovery_receipt_digest"})
        and all(admission.get(k) == scope[k] for k in ("run_id", "attempt", "fence")), "preserved_admission_drift")
    _require(_process_dead(recovery.get("process_identity"), identity_port=identity_port), "preserved_process_not_dead")
    if value["schema"] == "lh-trusted-continuation-authority/v2":
        from .work_unit_completion import candidate_inventory
        _require(candidate_inventory(admission["current_workspace_ref"]) == admission["source_workspace_inventory"],
            "preserved_source_drift")
    return admission


def accounting(conn, value, *, now, require_available=False):
    budget = _original(conn, value)
    _history(conn, value)
    reservations = [_reservation(conn, row) for row in conn.execute(
        "SELECT * FROM trusted_continuation_reservations WHERE goal_id=? AND goal_revision=? ORDER BY created_at,reservation_key",
        (value["goal_id"], value["goal_revision"]))] if _exists(conn) else []
    observations = {r["reservation_key"]: _observation(conn.execute(
        "SELECT * FROM trusted_continuation_observations WHERE reservation_key=?", (r["reservation_key"],)).fetchone(), r)
        for r in reservations}
    _require(all(r["continuation_authority_digest"] == value["authority_digest"]
        and r["deadline_at"] == reservations[0]["deadline_at"] for r in reservations), "reservation_window_drift")
    unknown = [key for key, obs in observations.items() if obs is not None and (obs["outcome"] == "unknown"
        or obs["process_terminated"] is not True or (obs["usage"] is not None and obs["usage"]["state"] != "observed"))]
    unsettled = [key for key, obs in observations.items() if obs is None]
    count = sum(r["is_provider_cli"] for r in reservations)
    observed = sum(obs["usage"]["total_tokens"] for obs in observations.values()
        if obs is not None and obs["usage"] is not None and obs["usage"]["state"] == "observed")
    deadline = reservations[0]["deadline_at"] if reservations else None
    if require_available:
        _require(not unknown and not unsettled, "new_unknown_or_inflight_blocks_scope")
        _require((now < deadline if deadline is not None else now + value["max_wall_seconds"] <= value["expires_at"])
            and now < value["expires_at"], "deadline_exhausted")
        _require(count < value["max_new_cli_launches"] and count + budget["reserved_cli_launches"] < value["reference_cli_limit"],
            "launches_exhausted")
        _require(observed + budget["observed_total_tokens"] < value["max_observed_tokens"], "observed_tokens_exhausted")
    return {"original_budget_digest": value["original_budget_digest"], "original_deadline_at": value["original_deadline_at"],
        "original_unknown_tokens": None, "whole_history_token_limit_proven": False,
        "historical_cli_launches": value["historical_cli_launches"], "original_reference_cli_launches": budget["reserved_cli_launches"],
        "new_cli_launches": count, "remaining_new_cli_launches": value["max_new_cli_launches"] - count,
        "reference_cli_launches": budget["reserved_cli_launches"] + count,
        "aggregate_cli_launches": value["historical_cli_launches"] + budget["reserved_cli_launches"] + count,
        "new_observed_tokens": observed, "deadline_at": deadline, "unknown": unknown, "unsettled": unsettled,
        "reservations": reservations, "observations": observations}


def repair_plan(conn, authority_digest, dispatch_key, *, attempt, fence, evidence, now, identity_port=None):
    _, digest_json = _api()
    value = get(conn, authority_digest)
    _require(value is not None and value["schema"] == "lh-trusted-continuation-authority/v2", "repair_authority_missing")
    manifest = _history(conn, value)
    execution_binding(value, manifest)
    scopes = [s for s in _repair_scopes(conn, value) if s["dispatch_key"] == dispatch_key]
    _require(len(scopes) == 1, "repair_not_authorized")
    scope = scopes[0]
    prior = get_repair(conn, dispatch_key)
    if prior is not None:
        _require(prior["red_receipt_digest"] == evidence.get("receipt_digest"), "repair_already_consumed")
        return {"scope": scope, "prior": prior}
    _require(attempt == scope["attempt"] and fence == scope["fence"], "repair_attempt_mismatch")
    row = conn.execute("SELECT d.*,r.fence,r.attempts FROM dispatch_consumptions d JOIN runs r ON r.run_id=d.run_id WHERE dispatch_key=?",
        (dispatch_key,)).fetchone()
    _require(row is not None and row["run_id"] == scope["run_id"] and row["attempt"] == row["attempts"] == attempt
        and row["fence"] == fence, "repair_current_identity_drift")
    proof = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (evidence.get("phase_key"),)).fetchone()
    _require(proof is not None and proof["state"] == "settled" and proof["phase"] in scope["allowed_red_phases"]
        and evidence.get("verdict") == "RED" and evidence.get("receipt_digest") == digest_json({k: v for k, v in evidence.items() if k != "receipt_digest"})
        and json.loads(proof["evidence_json"]) == evidence and all(proof[k] == scope[k] for k in ("run_id", "attempt", "fence")),
        "repair_settled_red_required")
    _require(not conn.execute("SELECT 1 FROM completion_phases WHERE run_id=? AND attempt=? AND state!='settled'",
        (scope["run_id"], attempt)).fetchone(), "repair_unresolved_effect")
    admission = preserved_admission(conn, value, scope, identity_port=identity_port)
    account = accounting(conn, value, now=now, require_available=True)
    _require(account["remaining_new_cli_launches"] >= 3, "repair_complete_launch_capacity_unavailable")
    return {"scope": scope, "admission": admission, "original_dispatch_receipt": json.loads(row["receipt_json"])}


def record_repair(conn, authority_digest, dispatch_key, *, attempt, fence, evidence, snapshot, now, identity_port=None):
    _, digest_json = _api()
    plan = repair_plan(conn, authority_digest, dispatch_key, attempt=attempt, fence=fence, evidence=evidence, now=now,
        identity_port=identity_port)
    if plan.get("prior") is not None:
        return plan["prior"]
    scope = plan["scope"]
    saved = conn.execute("SELECT * FROM completion_phases WHERE phase_key=?", (snapshot.get("phase_key"),)).fetchone()
    _require(saved is not None and saved["state"] == "settled" and saved["phase"] == "continuation_repair"
        and json.loads(saved["evidence_json"]) == snapshot and all(saved[k] == scope[k] for k in ("run_id", "attempt", "fence"))
        and snapshot.get("authority_digest") == authority_digest and snapshot.get("red_receipt_digest") == evidence["receipt_digest"]
        and snapshot.get("workspace_ref") == scope["workspace_ref"], "repair_snapshot_missing")
    from .work_unit_completion import candidate_inventory
    inventory = candidate_inventory(scope["workspace_ref"])
    _require(inventory["candidate_digest"] == plan["admission"]["candidate_digest"]
        and inventory["head"] == plan["admission"]["source_workspace_inventory"]["head"]
        and inventory == snapshot.get("inventory"), "repair_snapshot_drift")
    body = {"schema": "lh-trusted-continuation-repair/v1", "authority_digest": authority_digest,
        "dispatch_key": dispatch_key, "run_id": scope["run_id"], "scope": scope,
        "original_dispatch_receipt": plan["original_dispatch_receipt"], "admission_digest": plan["admission"]["input_digest"],
        "red_phase_key": evidence["phase_key"], "red_receipt_digest": evidence["receipt_digest"],
        "snapshot_receipt": snapshot, "created_at": now}
    body["repair_digest"] = digest_json(body)
    conn.execute("INSERT INTO trusted_continuation_repairs VALUES (?,?,?,?,?,?,?)", (dispatch_key, authority_digest,
        scope["run_id"], json.dumps(body, sort_keys=True), body["repair_digest"], now, scope["next_attempt"]))
    return body


def record(conn, authority, approved_digest, *, now, validate_only=False,
           identity_port=None, predecessor_authority_digest=None,
           successor_operation_ref=None):
    _, digest_json = _api()
    _require(_authority_shape(authority), "authority_shape_invalid")
    value = copy.deepcopy(authority)
    _require(_digest(approved_digest) and approved_digest == value["authority_digest"]
        and approved_digest == digest_json({k: v for k, v in value.items() if k != "authority_digest"}), "approval_mismatch")
    _require(all(isinstance(value[k], str) and value[k].strip() for k in ("authority_id", "goal_id")), "identity_invalid")
    _require(all(_digest(value[k]) for k in ("original_budget_digest", "original_execution_binding_digest",
        "execution_binding_digest", "policy_digest", "supersedes")), "digest_invalid")
    _require(all(_number(value[k], integer=True) for k in ("goal_revision", "historical_cli_launches",
        "reference_cli_limit", "max_new_cli_launches", "max_observed_tokens"))
        and all(_number(value[k]) for k in ("original_deadline_at", "max_wall_seconds", "expires_at"))
        and _number(now), "limits_invalid")
    owner = value["owner_disposition"]
    _require(isinstance(owner, dict) and set(owner) == {"principal", "decision_id", "unknown_usage_unrecoverable",
        "whole_history_token_limit_unprovable", "accept_unknown_history_risk", "accounting"}
        and all(isinstance(owner[k], str) and owner[k].strip() for k in ("principal", "decision_id"))
        and all(owner[k] is True for k in ("unknown_usage_unrecoverable", "whole_history_token_limit_unprovable", "accept_unknown_history_risk"))
        and owner["accounting"] == "known-continuation-observations-only", "owner_disposition_invalid")
    budget = _original(conn, value)
    manifest = _history(conn, value)
    execution_binding(value, manifest)
    prior = get(conn, approved_digest)
    if prior is not None:
        _require(prior == value, "authority_conflict")
        return prior
    _require(now + value["max_wall_seconds"] <= value["expires_at"], "complete_window_unavailable")
    _require(budget["blocked"] is True and budget["blocked_reason"] == "provider_usage_unknown", "original_unknown_required")
    _require(value["reference_cli_limit"] == budget["max_cli_launches"]
        and value["max_new_cli_launches"] <= budget["max_cli_launches"] - budget["reserved_cli_launches"]
        and value["max_wall_seconds"] <= budget["max_wall_seconds"]
        and budget["max_observed_tokens"] is not None
        and value["max_observed_tokens"] <= budget["max_observed_tokens"], "limits_expand_original")
    unknown = []
    for reservation in budget["reservations"]:
        observation = reservation.get("observation")
        _require(observation is not None, "original_inflight_unresolved")
        if reservation["state"] == "unknown":
            _require(reservation["is_provider_cli"] is True and observation["process_terminated"] is True
                and observation["outcome"] == "completed" and observation["usage"]["state"] == "unknown",
                "original_outcome_not_disposed")
            unknown.append(reservation)
    _require(bool(unknown) and sorted(value["unknown_reservation_keys"]) == sorted(r["reservation_key"] for r in unknown), "unknown_scope_incomplete")
    scopes = value["scope"]
    _require(isinstance(scopes, list) and 2 <= len(scopes) <= 3, "scope_invalid")
    tasks = {task["envelope"]["dispatch_key"]: task for task in manifest.get("tasks", []) if task.get("status") == "approved"}
    _require(set(tasks) == {scope.get("dispatch_key") for scope in scopes}, "original_task_scope_mismatch")
    seen = set()
    for scope in scopes:
        _require(isinstance(scope, dict) and set(scope) == {"dispatch_key", "run_id", "attempt", "fence", "envelope_digest", "allow_coding"}
            and isinstance(scope["allow_coding"], bool) and _digest(scope["envelope_digest"])
            and all(_number(scope[k], integer=True) for k in ("attempt", "fence"))
            and all(isinstance(scope[k], str) and scope[k] for k in ("dispatch_key", "run_id"))
            and scope["dispatch_key"] not in seen, "scope_identity_invalid")
        seen.add(scope["dispatch_key"])
        task = tasks[scope["dispatch_key"]]
        envelope = task["envelope"]
        if __package__:
            from .work_unit_store import WorkUnitStore
        else:
            from work_unit_store import WorkUnitStore
        work_id = "dispatch-" + hashlib.sha256(scope["dispatch_key"].encode()).hexdigest()[:32]
        _require(envelope.get("goal_id") == value["goal_id"] and envelope.get("goal_revision") == value["goal_revision"]
            and envelope.get("envelope_digest") == scope["envelope_digest"]
            and scope["run_id"] == WorkUnitStore._run_id(value["goal_id"], work_id, manifest["base_sha"]), "original_task_identity_mismatch")
        dispatch = conn.execute("SELECT * FROM dispatch_consumptions WHERE dispatch_key=?", (scope["dispatch_key"],)).fetchone()
        if dispatch is not None:
            run = conn.execute("SELECT * FROM runs WHERE run_id=?", (scope["run_id"],)).fetchone()
            _require(run is not None and dispatch["goal_id"] == value["goal_id"] and dispatch["goal_revision"] == value["goal_revision"]
                and all(dispatch[k] == scope[k] for k in ("run_id", "attempt", "envelope_digest"))
                and run["attempts"] == scope["attempt"] and run["fence"] == scope["fence"], "current_scope_drift")
        else:
            _require(scope["allow_coding"] is True and scope["attempt"] == scope["fence"] == 1, "missing_preserved_dispatch")
        original = [r for r in unknown if r["run_id"] == scope["run_id"]]
        if original:
            _require(not scope["allow_coding"] and len(original) == 1
                and all(scope[k] == original[0][k] for k in ("attempt", "fence")), "preserved_coding_forbidden")
            receipt = json.loads(dispatch["receipt_json"])
            recovery = receipt.get("executor_recovery_receipt")
            admission_row = conn.execute("SELECT admission_json FROM candidate_recovery_admissions WHERE dispatch_key=?", (scope["dispatch_key"],)).fetchone()
            admission = json.loads(admission_row[0]) if admission_row else {}
            _require(receipt.get("executor_status") == "unknown" and isinstance(recovery, dict)
                and recovery.get("recovery_receipt_digest") == digest_json({k: v for k, v in recovery.items() if k != "recovery_receipt_digest"})
                and admission.get("kind") == "preserved_result_recovery"
                and admission.get("current_unknown_recovery_receipt_digest") == recovery["recovery_receipt_digest"]
                and all(admission.get(k) == scope[k] for k in ("run_id", "attempt", "fence")), "preserved_admission_missing")
            _require(_process_dead(recovery.get("process_identity"), identity_port=identity_port), "preserved_process_not_dead")
        else:
            _require(scope["allow_coding"] is True, "unexpected_preserved_scope")
    _require(all(any(s["run_id"] == r["run_id"] for s in scopes) for r in unknown), "unknown_run_omitted")
    _repair_scopes(conn, value)
    if _exists(conn):
        older = list(conn.execute(
            "SELECT authority_digest FROM trusted_continuation_authorities "
            "WHERE goal_id=? AND goal_revision=?",
            (value["goal_id"], value["goal_revision"])))
        if predecessor_authority_digest is None:
            _require(not older, "additional_authority_requires_separate_transition")
        else:
            _require(_digest(predecessor_authority_digest)
                     and predecessor_authority_digest != value["authority_digest"]
                     and len(older) == 1
                     and older[0]["authority_digest"] == predecessor_authority_digest,
                     "successor_predecessor_invalid")
            predecessor = get(conn, predecessor_authority_digest)
            _require(predecessor is not None
                     and predecessor["goal_id"] == value["goal_id"]
                     and predecessor["goal_revision"] == value["goal_revision"],
                     "successor_predecessor_missing")
            successor_changes = {
                "authority_id", "authority_digest", "expires_at",
                "execution_binding_digest", "policy_digest",
                "policy_transition", "runtime_source",
            }
            _require(predecessor["schema"] == value["schema"]
                     == "lh-trusted-continuation-authority/v2"
                     and set(predecessor) == set(value) == _V2_FIELDS
                     and all(value[key] == predecessor[key]
                             for key in _V2_FIELDS - successor_changes)
                     and value["authority_id"] != predecessor["authority_id"]
                     and value["expires_at"] > predecessor["expires_at"],
                     "successor_authority_allowlist_invalid")
            _require(isinstance(successor_operation_ref, dict)
                     and set(successor_operation_ref)
                     == {"path", "sha256", "operation_digest"}
                     and all(isinstance(successor_operation_ref[key], str)
                             and successor_operation_ref[key]
                             for key in successor_operation_ref)
                     and _digest(successor_operation_ref["operation_digest"]),
                     "successor_operation_ref_invalid")
            if _table(conn, "trusted_continuation_successors"):
                _require(conn.execute(
                    "SELECT 1 FROM trusted_continuation_successors "
                    "WHERE predecessor_digest=? OR successor_digest=?",
                    (predecessor_authority_digest, value["authority_digest"]),
                ).fetchone() is None, "successor_conflict")
    if validate_only:
        return value
    _schema(conn)
    if value["schema"] == "lh-trusted-continuation-authority/v2":
        conn.execute("CREATE TABLE IF NOT EXISTS trusted_continuation_repairs (dispatch_key TEXT PRIMARY KEY, authority_digest TEXT NOT NULL, run_id TEXT NOT NULL, repair_json TEXT NOT NULL, repair_digest TEXT NOT NULL, created_at REAL NOT NULL, next_attempt INTEGER NOT NULL, FOREIGN KEY(authority_digest) REFERENCES trusted_continuation_authorities(authority_digest))")
    conn.execute("INSERT INTO trusted_continuation_authorities VALUES (?,?,?,?,?,?)", (approved_digest,
        value["authority_id"], value["goal_id"], value["goal_revision"], json.dumps(value, sort_keys=True), now))
    if predecessor_authority_digest is not None:
        _successor_schema(conn)
        conn.execute(
            "INSERT INTO trusted_continuation_successors "
            "(successor_digest,predecessor_digest,goal_id,goal_revision,"
            "operation_ref_json,created_at) VALUES (?,?,?,?,?,?)",
            (approved_digest, predecessor_authority_digest, value["goal_id"],
             value["goal_revision"], json.dumps(successor_operation_ref,
             sort_keys=True), now),
        )
    return value


def reserve(conn, *, authority_digest, fields, policy, execution_binding_digest, now, identity_port=None):
    _, digest_json = _api()
    value = get(conn, authority_digest)
    _require(value is not None, "authority_missing")
    if _table(conn, "trusted_continuation_successors"):
        _require(conn.execute(
            "SELECT 1 FROM trusted_continuation_successors "
            "WHERE predecessor_digest=?",
            (authority_digest,),
        ).fetchone() is None, "authority_superseded")
    budget = _original(conn, value)
    _history(conn, value)
    _require(fields["goal_id"] == value["goal_id"] and fields["goal_revision"] == value["goal_revision"]
        and fields["policy_digest"] == value["policy_digest"]
        and execution_binding_digest == value["execution_binding_digest"], "execution_binding_mismatch")
    _require(value["expires_at"] <= policy["expires_at"], "policy_expiry_expanded")
    dispatch = conn.execute("SELECT d.*,r.state AS run_state,r.fence AS current_fence,r.attempts,a.state AS attempt_state,a.fence AS attempt_fence,p.goal_id AS actual_goal,p.goal_revision AS actual_revision FROM dispatch_consumptions d JOIN runs r ON r.run_id=d.run_id JOIN parent_goals p ON p.parent_goal_id=r.parent_goal_id JOIN attempts a ON a.run_id=r.run_id AND a.ordinal=d.attempt WHERE d.run_id=?", (fields["run_id"],)).fetchone()
    _require(dispatch is not None, "dispatch_missing")
    _require(dispatch["goal_id"] == dispatch["actual_goal"] == value["goal_id"]
        and dispatch["goal_revision"] == dispatch["actual_revision"] == value["goal_revision"], "actual_goal_mismatch")
    matching = [s for s in value["scope"] if s["dispatch_key"] == dispatch["dispatch_key"]]
    _require(len(matching) == 1, "dispatch_outside_scope")
    scope = matching[0]
    for preserved in value["scope"]:
        if preserved["allow_coding"]:
            continue
        preserved_admission(conn, value, preserved, identity_port=identity_port)
    _require(_scope_matches(conn, value, scope, fields)
        and dispatch["envelope_digest"] == scope["envelope_digest"]
        and dispatch["attempts"] == fields["attempt"] == dispatch["attempt"]
        and dispatch["current_fence"] == fields["fence"] == dispatch["attempt_fence"], "attempt_binding_mismatch")
    queued = fields["phase"] in {"coding", "admission"} and dispatch["run_state"] in {"queued", "ready"} and dispatch["attempt_state"] == "ready"
    running = dispatch["run_state"] in {"running", "verified"} and dispatch["attempt_state"] in {"running", "verified"}
    _require(queued or running, "phase_state_invalid")
    key = digest_json([authority_digest, fields["goal_id"], fields["goal_revision"], fields["phase_key"]])
    existing = conn.execute("SELECT * FROM trusted_continuation_reservations WHERE goal_id=? AND goal_revision=? AND phase_key=?", (fields["goal_id"], fields["goal_revision"], fields["phase_key"])).fetchone()
    if existing:
        saved = _reservation(conn, existing)
        _observation(conn.execute("SELECT * FROM trusted_continuation_observations WHERE reservation_key=?",
            (saved["reservation_key"],)).fetchone(), saved)
        _require(saved["reservation_key"] == key and all(saved[k] == v for k, v in fields.items()), "reservation_conflict")
        return {**saved, "claimed": False}
    reservations = [_reservation(conn, row) for row in conn.execute("SELECT * FROM trusted_continuation_reservations WHERE goal_id=? AND goal_revision=? ORDER BY created_at,reservation_key", (fields["goal_id"], fields["goal_revision"]))]
    observations = {prior["reservation_key"]: _observation(conn.execute(
        "SELECT * FROM trusted_continuation_observations WHERE reservation_key=?", (prior["reservation_key"],)).fetchone(), prior)
        for prior in reservations}
    _require(all(r["continuation_authority_digest"] == authority_digest
        and r["deadline_at"] == reservations[0]["deadline_at"] for r in reservations), "reservation_window_drift")
    known_tokens = budget["observed_total_tokens"]
    for prior in reservations:
        observed = observations.get(prior["reservation_key"])
        _require(observed is not None or prior["run_id"] != fields["run_id"], "same_run_reservation_unresolved")
        if observed is not None:
            _require(observed["outcome"] != "unknown" and observed["process_terminated"] is True
                and (not prior["is_provider_cli"] or observed["usage"]["state"] == "observed"), "new_unknown_blocks_scope")
            if prior["is_provider_cli"]:
                known_tokens += observed["usage"]["total_tokens"]
    deadline = reservations[0]["deadline_at"] if reservations else now + value["max_wall_seconds"]
    _require(deadline <= value["expires_at"] and now < deadline, "deadline_exhausted")
    count = sum(r["is_provider_cli"] for r in reservations)
    if fields["is_provider_cli"]:
        _require(count < value["max_new_cli_launches"] and count + budget["reserved_cli_launches"] < value["reference_cli_limit"], "launches_exhausted")
        _require(known_tokens < value["max_observed_tokens"], "observed_tokens_exhausted")
    reservation = {"schema": "lh-trusted-launch-reservation/v1", "reservation_key": key, "state": "reserved",
        **fields, "reserved_at": now, "deadline_at": deadline, "continuation_authority_digest": authority_digest}
    reservation["reservation_digest"] = digest_json(reservation)
    conn.execute("INSERT INTO trusted_continuation_reservations VALUES (?,?,?,?,?,?,?)", (key, authority_digest,
        fields["goal_id"], fields["goal_revision"], fields["phase_key"], json.dumps(reservation, sort_keys=True), now))
    return {**reservation, "claimed": True}


def executor_binding(conn, dispatch, evidence, *, unknown=False):
    """Validate a new coding receipt against its exact continued reservation."""
    if not _exists(conn):
        return False
    _, digest_json = _api()
    authority_digest = evidence.get("continuation_authority_digest")
    authority = get(conn, authority_digest)
    if authority is None:
        return False
    _original(conn, authority)
    _history(conn, authority)
    if (authority["goal_id"] != dispatch["goal_id"] or authority["goal_revision"] != dispatch["goal_revision"]
        or authority["execution_binding_digest"] != evidence.get("execution_binding_digest")
        or authority["policy_digest"] != evidence.get("policy_digest")):
        return False
    for row in conn.execute("SELECT * FROM trusted_continuation_reservations WHERE authority_digest=?", (authority_digest,)):
        reservation = _reservation(conn, row)
        observation = _observation(conn.execute("SELECT * FROM trusted_continuation_observations WHERE reservation_key=?",
            (reservation["reservation_key"],)).fetchone(), reservation)
        if observation is None:
            continue
        if (reservation["phase"] == "coding" and all(reservation[k] == evidence.get(k) for k in ("run_id", "attempt", "fence"))
            and reservation["is_provider_cli"] and observation["process_terminated"] is True):
            if unknown:
                return observation["outcome"] == "unknown" or observation["usage"]["state"] == "unknown"
            assurance = evidence.get("execution_assurance") or {}
            return (observation["outcome"] == "completed" and observation["usage"]["state"] == "observed"
                and reservation["command_digest"] == assurance.get("command_digest"))
    return False


def settle(conn, reservation_key, observation, *, now):
    _, digest_json = _api()
    if not _exists(conn):
        return None
    row = conn.execute("SELECT * FROM trusted_continuation_reservations WHERE reservation_key=?", (reservation_key,)).fetchone()
    if row is None:
        return None
    reservation = _reservation(conn, row)
    _require(_number(now), "observation_time_invalid")
    _require(reservation["is_provider_cli"] == (observation["usage"] is not None), "usage_binding_invalid")
    prior = _observation(conn.execute("SELECT * FROM trusted_continuation_observations WHERE reservation_key=?",
        (reservation_key,)).fetchone(), reservation)
    if prior:
        _require(prior == observation, "observation_conflict")
    else:
        conn.execute("INSERT INTO trusted_continuation_observations VALUES (?,?,?,?)", (reservation_key,
            json.dumps(observation, sort_keys=True), digest_json(observation), now))
    unknown = (observation["outcome"] == "unknown" or not observation["process_terminated"]
        or (reservation["is_provider_cli"] and observation["usage"]["state"] == "unknown"))
    return {**reservation, "state": "unknown" if unknown else "settled",
        "observation": copy.deepcopy(observation), "observation_digest": digest_json(observation)}

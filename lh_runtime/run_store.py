"""SQLite-backed durable state for the native Loop Hybrid MVP."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Mapping


try:
    from . import delivery_contract as delivery_engine
    from .platform_ports import remove_tree
except ImportError:
    if __package__:
        raise
    import delivery_contract as delivery_engine
    from platform_ports import remove_tree


RUN_STATES = {
    "queued",
    "running",
    "retry_pending",
    "verified",
    "stopped",
    "human_required",
    "awaiting_external_verdict",
}
DELIVERY_PHASES = {"source", "final"}
DELIVERY_TERMINAL_STATES = {"verified", "integrated"}
PROCESS_START_NS = time.time_ns()
PROCESS_NONCE = secrets.token_urlsafe(18)


def _snapshot_digest(root: str | Path) -> str:
    """Digest a verifier snapshot without treating ``.git`` as source state."""
    target = Path(root).expanduser().resolve()
    entries: list[dict[str, str | int]] = []
    if not target.is_dir():
        return "sha256:" + hashlib.sha256(b"missing-verifier-snapshot").hexdigest()
    for current, dirnames, filenames in os.walk(target, topdown=True, followlinks=False):
        dirnames[:] = sorted(name for name in dirnames if name != ".git")
        filenames = sorted(name for name in filenames if name != ".git")
        current_path = Path(current)
        for name in [*dirnames, *filenames]:
            path = current_path / name
            relative = str(path.relative_to(target)).replace(os.sep, "/")
            try:
                mode = path.lstat().st_mode & 0o777
                if path.is_symlink():
                    entries.append({"path": relative, "kind": "symlink", "value": os.readlink(path), "mode": mode})
                elif path.is_file():
                    entries.append({
                        "path": relative,
                        "kind": "file",
                        "value": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "mode": mode,
                    })
                elif path.is_dir():
                    entries.append({"path": relative, "kind": "directory", "value": "", "mode": mode})
            except OSError:
                entries.append({"path": relative, "kind": "unreadable", "value": "", "mode": 0})
    encoded = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _independent_verifier_config(contract: Any) -> tuple[dict[str, Any] | None, str | None]:
    verifier = contract.get("independent_verifier") if isinstance(contract, dict) else None
    if not isinstance(verifier, dict):
        return None, "delivery_independent_verifier_capability_missing"
    capability = verifier.get("capability")
    if not isinstance(capability, str) or not capability.strip():
        return None, "delivery_independent_verifier_capability_missing"
    argv = verifier.get("argv")
    if not isinstance(argv, list) or not argv or any(not isinstance(item, str) or not item for item in argv):
        return None, "delivery_independent_verifier_argv_missing"
    if verifier.get("read_only") is not True or verifier.get("source_write") is not False:
        return None, "delivery_independent_verifier_contract_invalid"
    timeout = verifier.get("timeout_seconds", 300)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 900:
        return None, "delivery_independent_verifier_timeout_invalid"
    cwd = verifier.get("cwd", "${WORKTREE}")
    if not isinstance(cwd, str) or not cwd.strip():
        return None, "delivery_independent_verifier_cwd_missing"
    return {"capability": capability, "argv": list(argv), "cwd": cwd, "timeout_seconds": float(timeout)}, None


def _delivery_from_goal(goal: Any, key: str) -> dict[str, Any] | None:
    """Read one canonical delivery projection from a Goal/Run.

    The generic contract remains the authority when an older producer did not
    persist a packet projection.  In that case ``RunStore.delivery_packet``
    materializes one from the sealed contract scope; it never takes paths or
    identities from a model response.
    """
    if not isinstance(goal, dict):
        return None
    candidates: list[Any] = [goal.get(key)]
    envelope = goal.get("admission_envelope")
    if isinstance(envelope, dict):
        candidates.append(envelope.get(key))
    for candidate in candidates:
        if isinstance(candidate, dict):
            return json.loads(json.dumps(candidate, sort_keys=True))
    return None


class _ClosingConnection(sqlite3.Connection):
    """Keep the transaction context contract while closing on context exit."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class RunStore:
    """The only component that persists native LH run state."""

    def __init__(self, root: str | Path, *, command_runner=None):
        self.command_runner = command_runner
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir(exist_ok=True)
        self.db_path = self.root / "loop.sqlite3"
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    goal_json TEXT NOT NULL,
                    source_repo TEXT NOT NULL,
                    base_revision TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    fence INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    delivery_attempt_ceiling INTEGER,
                    delivery_contract_json TEXT,
                    delivery_plan_json TEXT,
                    delivery_contract_digest TEXT,
                    delivery_source_evidence_json TEXT,
                    delivery_source_digest TEXT,
                    delivery_final_evidence_json TEXT,
                    delivery_final_digest TEXT,
                    delivery_terminal_state TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    workspace_ref TEXT NOT NULL,
                    fence INTEGER NOT NULL DEFAULT 0,
                    receipt_ref TEXT,
                    receipt_digest TEXT,
                    delivery_source_ref TEXT,
                    delivery_source_digest TEXT,
                    delivery_final_ref TEXT,
                    delivery_final_digest TEXT,
                    created_at REAL NOT NULL,
                    finished_at REAL,
                    PRIMARY KEY(run_id, ordinal),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    run_id TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS usage_corrections (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(run_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS failure_cases (
                    failure_case_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    goal_id TEXT NOT NULL,
                    signature_json TEXT NOT NULL,
                    acceptance_digest TEXT NOT NULL,
                    base_revision TEXT NOT NULL,
                    state TEXT NOT NULL,
                    grill_generation INTEGER NOT NULL DEFAULT 1,
                    claim_fence INTEGER NOT NULL DEFAULT 0,
                    claim_holder TEXT,
                    claim_expires_at REAL,
                    plan_digest TEXT,
                    latest_check_receipt_digest TEXT,
                    next_node_id TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS failure_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    failure_case_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY(failure_case_id) REFERENCES failure_cases(failure_case_id)
                );
                """
            )
            run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
            if "fence" not in run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN fence INTEGER NOT NULL DEFAULT 0")
            for column, declaration in (
                ("delivery_contract_json", "TEXT"),
                ("delivery_plan_json", "TEXT"),
                ("delivery_contract_digest", "TEXT"),
                ("delivery_attempt_ceiling", "INTEGER"),
                ("delivery_source_evidence_json", "TEXT"),
                ("delivery_source_digest", "TEXT"),
                ("delivery_final_evidence_json", "TEXT"),
                ("delivery_final_digest", "TEXT"),
                ("delivery_terminal_state", "TEXT"),
            ):
                if column not in run_columns:
                    conn.execute(f"ALTER TABLE runs ADD COLUMN {column} {declaration}")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(attempts)")}
            if "receipt_digest" not in columns:
                conn.execute("ALTER TABLE attempts ADD COLUMN receipt_digest TEXT")
            if "fence" not in columns:
                conn.execute("ALTER TABLE attempts ADD COLUMN fence INTEGER NOT NULL DEFAULT 0")
            for column, declaration in (
                ("delivery_source_ref", "TEXT"),
                ("delivery_source_digest", "TEXT"),
                ("delivery_final_ref", "TEXT"),
                ("delivery_final_digest", "TEXT"),
            ):
                if column not in columns:
                    conn.execute(f"ALTER TABLE attempts ADD COLUMN {column} {declaration}")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=5,
            isolation_level=None,
            factory=_ClosingConnection,
        )
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["goal"] = json.loads(value.pop("goal_json"))
        for name in (
            "delivery_contract",
            "delivery_plan",
            "delivery_source_evidence",
            "delivery_final_evidence",
        ):
            raw_name = f"{name}_json"
            raw = value.pop(raw_name, None)
            value[name] = json.loads(raw) if isinstance(raw, str) and raw else None
        return value

    @staticmethod
    def _contract_repair_max(contract: Any) -> int | None:
        repair = contract.get("repair_same_unit") if isinstance(contract, dict) else None
        value = repair.get("max_attempts") if isinstance(repair, dict) else None
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            return None
        return value

    @classmethod
    def _effective_max_attempts_from_row(cls, row: Any) -> int:
        persisted = row["max_attempts"] if isinstance(row, sqlite3.Row) else row.get("max_attempts")
        try:
            ceiling = int(persisted)
        except (TypeError, ValueError):
            ceiling = 4
        stored_ceiling = row["delivery_attempt_ceiling"] if isinstance(row, sqlite3.Row) else row.get("delivery_attempt_ceiling")
        try:
            if stored_ceiling is not None:
                ceiling = min(ceiling, int(stored_ceiling))
        except (TypeError, ValueError):
            pass
        raw_contract = row["delivery_contract_json"] if isinstance(row, sqlite3.Row) else row.get("delivery_contract_json")
        if isinstance(raw_contract, str) and raw_contract:
            try:
                repair_max = cls._contract_repair_max(json.loads(raw_contract))
            except (TypeError, json.JSONDecodeError):
                repair_max = None
            if repair_max is not None:
                ceiling = min(ceiling, repair_max)
        return max(1, ceiling)

    def effective_max_attempts(self, run_id: str) -> int:
        """Return the durable minimum of Run and contract repair ceilings."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT max_attempts, delivery_attempt_ceiling, delivery_contract_json FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return self._effective_max_attempts_from_row(row)

    def create_run(self, *, goal: dict[str, Any], source_repo: Path, base_revision: str, max_attempts: int = 4, run_id: str | None = None) -> str:
        if not 1 <= max_attempts <= 4:
            raise ValueError("max_attempts must be an integer from 1 to 4")
        run_id = run_id or "run-" + uuid.uuid4().hex
        now = time.time()
        delivery_contract = _delivery_from_goal(goal, "delivery_contract")
        delivery_plan = _delivery_from_goal(goal, "delivery_plan")
        delivery_attempt_ceiling = min(
            max_attempts,
            self._contract_repair_max(delivery_contract) or max_attempts,
        )
        contract_json = json.dumps(delivery_contract, sort_keys=True) if delivery_contract is not None else None
        plan_json = json.dumps(delivery_plan, sort_keys=True) if delivery_plan is not None else None
        contract_digest = delivery_contract.get("contract_digest") if isinstance(delivery_contract, dict) else None
        with self._connect() as conn:
            goal_json = json.dumps(goal, sort_keys=True)
            existing = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if existing is not None:
                if (
                    existing["goal_json"], existing["source_repo"], existing["base_revision"], existing["max_attempts"],
                    existing["delivery_contract_json"], existing["delivery_plan_json"],
                ) != (goal_json, str(source_repo), base_revision, max_attempts, contract_json, plan_json):
                    raise ValueError("run_id is already bound to different run inputs")
                return run_id
            conn.execute(
                "INSERT INTO runs(run_id, goal_json, source_repo, base_revision, state, attempts, fence, max_attempts, delivery_attempt_ceiling, delivery_contract_json, delivery_plan_json, delivery_contract_digest, created_at, updated_at) VALUES (?, ?, ?, ?, 'queued', 0, 0, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, goal_json, str(source_repo), base_revision, max_attempts, delivery_attempt_ceiling, contract_json, plan_json, contract_digest, now, now),
            )
        self.append_event(
            run_id,
            "run_created",
            {
                "base_revision": base_revision,
                "max_attempts": max_attempts,
                "delivery_contract_digest": contract_digest,
                "delivery_bound": delivery_contract is not None,
            },
        )
        return run_id

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        value = self._row(row)
        if value is None:
            raise KeyError(f"unknown run_id: {run_id}")
        return value

    # -- generic delivery binding -------------------------------------------

    @staticmethod
    def _delivery_identity(run: dict[str, Any], *, ordinal: int, fence: int,
                           diff_digest: str | None = None,
                           dispatch_key: str | None = None) -> dict[str, Any]:
        goal = run.get("goal") if isinstance(run.get("goal"), dict) else {}
        contract = (
            run.get("delivery_contract")
            if isinstance(run.get("delivery_contract"), dict)
            else _delivery_from_goal(goal, "delivery_contract")
        )
        if not isinstance(contract, dict):
            contract = {}
        contract_goal = contract.get("goal") if isinstance(contract.get("goal"), dict) else {}
        contract_node = contract.get("node") if isinstance(contract.get("node"), dict) else {}

        def first(*values: Any) -> Any:
            for value in values:
                if value not in (None, ""):
                    return value
            return None

        identity: dict[str, Any] = {
            "goal_id": first(contract_goal.get("id"), contract_goal.get("goal_id"), goal.get("goal_id")),
            "goal_revision": first(contract_goal.get("revision"), contract_goal.get("goal_revision"), goal.get("goal_revision"), goal.get("revision")),
            "node_id": first(contract_node.get("id"), contract_node.get("node_id"), goal.get("node_id")),
            "unit_id": first(contract.get("unit_id"), goal.get("unit_id"), goal.get("delivery_unit_id")),
            "run_id": run.get("run_id"),
            "attempt": ordinal,
            "fence": fence,
            "base_sha": run.get("base_revision"),
        }
        if dispatch_key is not None:
            identity["dispatch_key"] = dispatch_key
        if diff_digest is not None:
            identity["diff_digest"] = diff_digest
        return {key: value for key, value in identity.items() if value not in (None, "")}

    @staticmethod
    def _delivery_authority(run: dict[str, Any], *, ordinal: int | None = None,
                            fence: int | None = None, phase: str | None = None,
                            terminal_state: str | None = None) -> dict[str, Any]:
        return {
            "authority_store": "run",
            "run_id": run.get("run_id"),
            "run_state": run.get("state"),
            "attempt": ordinal,
            "fence": fence,
            "phase": phase,
            "terminal_state": terminal_state,
        }

    @staticmethod
    def _delivery_binding_identity_reason(run: dict[str, Any], contract: dict[str, Any]) -> str | None:
        run_goal = run.get("goal") if isinstance(run.get("goal"), dict) else {}
        contract_goal = contract.get("goal") if isinstance(contract.get("goal"), dict) else {}
        contract_node = contract.get("node") if isinstance(contract.get("node"), dict) else {}
        identity_pairs = (
            ("goal_id", run_goal.get("goal_id"), contract_goal.get("id", contract_goal.get("goal_id"))),
            ("goal_revision", run_goal.get("goal_revision", run_goal.get("revision")), contract_goal.get("revision", contract_goal.get("goal_revision"))),
            ("node_id", run_goal.get("node_id"), contract_node.get("id", contract_node.get("node_id"))),
        )
        for field, persisted, bound in identity_pairs:
            if persisted not in (None, "") and bound not in (None, "") and persisted != bound:
                return f"delivery_binding_{field}_mismatch"
        return None

    def delivery_required(self, run_id: str) -> bool:
        self.get_run(run_id)
        return True

    def delivery_binding(self, run_id: str) -> dict[str, Any]:
        """Return the immutable contract/plan binding owned by this RunStore."""
        run = self.get_run(run_id)
        contract = run.get("delivery_contract") or _delivery_from_goal(run.get("goal"), "delivery_contract")
        plan = run.get("delivery_plan") or _delivery_from_goal(run.get("goal"), "delivery_plan")
        if not isinstance(contract, dict):
            return {"verdict": "RED", "reason": "delivery_contract_missing", "run_id": run_id}
        if not isinstance(plan, dict):
            return {"verdict": "RED", "reason": "delivery_plan_missing", "run_id": run_id}
        try:
            resolved = delivery_engine.validate_contract(contract)
            plan_result = delivery_engine.verify_plan_verdict(plan, resolved)
        except Exception as exc:
            return {"verdict": "RED", "reason": f"delivery_binding_invalid:{type(exc).__name__}", "run_id": run_id}
        if not isinstance(resolved, dict):
            return {"verdict": "RED", "reason": "delivery_contract_invalid", "run_id": run_id}
        if not isinstance(plan_result, dict) or plan_result.get("verdict") != "GREEN":
            reason = plan_result.get("reason") if isinstance(plan_result, dict) else "delivery_plan_not_green"
            return {"verdict": "RED", "reason": str(reason), "run_id": run_id}
        authority_store = resolved.get("authority_store", "work_unit")
        if authority_store != "run":
            # B never manufactures evidence for a WorkUnit-owned contract.
            return {"verdict": "RED", "reason": "delivery_authority_store_not_run", "authority_store": authority_store, "run_id": run_id}
        persisted_digest = run.get("delivery_contract_digest")
        if persisted_digest not in (None, resolved.get("contract_digest")):
            return {"verdict": "RED", "reason": "delivery_contract_digest_persisted_mismatch", "run_id": run_id}
        identity_reason = self._delivery_binding_identity_reason(run, resolved)
        if identity_reason is not None:
            return {"verdict": "RED", "reason": identity_reason, "run_id": run_id}
        # A contract may satisfy the generic schema while still omitting the
        # concrete read-only verifier adapter that this RunStore can invoke.
        # Reject that at admission, before a model/attempt/transport starts;
        # the worker must not discover a missing capability only after work.
        _verifier, verifier_error = _independent_verifier_config(resolved)
        if verifier_error is not None:
            return {"verdict": "RED", "reason": verifier_error, "run_id": run_id}
        return {
            "verdict": "GREEN",
            "contract": resolved,
            "plan": plan,
            "contract_digest": resolved.get("contract_digest"),
            "unit_id": resolved.get("unit_id"),
            "authority_store": authority_store,
            "run_id": run_id,
        }

    def delivery_packet(self, run_id: str) -> dict[str, Any] | None:
        """Return the packet bound to a Run's sealed delivery contract.

        A packet explicitly persisted by admission is verified as-is.  For
        older producers that persisted only the complete contract/plan, the
        packet is materialized deterministically from that contract's declared
        scope.  No model/provider field can widen this packet.
        """
        binding = self.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return None
        run = self.get_run(run_id)
        packet = _delivery_from_goal(run.get("goal"), "delivery_packet")
        contract = binding["contract"]
        scope = contract.get("scope") if isinstance(contract.get("scope"), dict) else {}
        if packet is None:
            packet = {
                "schema": "host-delivery-unit-packet/v1",
                "packet_id": f"packet-{binding['unit_id']}",
                "goal_id": contract["goal"]["id"],
                "goal_revision": contract["goal"]["revision"],
                "node_id": contract["node"]["id"],
                "write_set": list(scope.get("allowed_paths") or []),
                "forbidden_paths": list(scope.get("forbidden_paths") or []),
            }
        try:
            bound = delivery_engine.bind_packet(packet, binding["plan"], contract)
            result = delivery_engine.verify_packet_binding(bound, contract, binding["plan"])
        except Exception:
            return None
        if not isinstance(result, dict) or result.get("verdict") != "GREEN":
            return None
        return bound

    def delivery_preflight(self, run_id: str, *, phase: str = "source") -> dict[str, Any]:
        """Check only persisted binding before a model, checker, or transport.

        This is deliberately distinct from ``verify_delivery``: source
        preflight must not demand final CI/owner receipts that do not exist
        before a PR, while it still refuses a missing/foreign contract.
        """
        if phase not in DELIVERY_PHASES:
            raise ValueError(f"invalid delivery phase: {phase}")
        run = self.get_run(run_id)
        binding = self.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return {**binding, "required": True, "phase": phase}
        return {**binding, "required": True, "phase": phase}

    def _command_execution_context(self, run_id: str, worktree: str | Path, *, phase: str,
                                   supplied: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Read native Run authority without inventing a WorkUnit identity."""
        run = self.get_run(run_id)
        attempt = self.latest_attempt(run_id)
        if (attempt is None or any(isinstance(attempt.get(name), bool)
            or not isinstance(attempt.get(name), int) or attempt[name] < 1 for name in ("ordinal", "fence"))
            or run["attempts"] != attempt["ordinal"] or run["fence"] != attempt["fence"]):
            raise ValueError("delivery_execution_context_attempt_missing_or_stale")
        bound = self.delivery_binding(run_id)
        if bound.get("verdict") != "GREEN":
            raise ValueError("delivery_execution_context_authority_invalid")
        context = {**self._delivery_identity(run, ordinal=attempt["ordinal"], fence=attempt["fence"]),
                   "authority_store": "run", "worktree": str(Path(worktree).expanduser().resolve()),
                   "execution_phase": phase}
        if (any(not isinstance(context.get(name), str) or not context[name].strip()
                for name in ("goal_id", "node_id", "unit_id", "run_id", "base_sha", "worktree"))
            or isinstance(context.get("goal_revision"), bool)
            or not isinstance(context.get("goal_revision"), int) or context["goal_revision"] < 1):
            raise ValueError("delivery_execution_context_authority_missing")
        if supplied is not None and (not isinstance(supplied, Mapping) or "work_unit_id" in supplied
            or any(supplied.get(name) != value for name, value in context.items())):
            raise ValueError("delivery_execution_context_authority_mismatch")
        return context

    def _run_independent_verifier(
        self,
        contract: dict[str, Any],
        worktree: str | Path,
        *,
        execution_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        config, config_error = _independent_verifier_config(contract)
        if config is None:
            return {
                "verdict": "RED",
                "reason": config_error or "delivery_independent_verifier_capability_missing",
                "invocation": {"invoked": False},
            }
        source = Path(worktree).expanduser().resolve()
        if not source.is_dir():
            return {
                "verdict": "RED",
                "reason": "delivery_independent_verifier_snapshot_source_missing",
                "invocation": {"invoked": False},
            }
        source_before = _snapshot_digest(source)
        snapshot_parent = Path(tempfile.mkdtemp(prefix="lh-delivery-verifier-"))
        snapshot_root = snapshot_parent / "source"
        invocation: dict[str, Any] = {
            "capability": config["capability"],
            "argv": list(config["argv"]),
            "cwd": config["cwd"],
            "timeout_seconds": config["timeout_seconds"],
            "invoked": False,
        }
        snapshot: dict[str, Any] = {"source_before": source_before}
        try:
            # Do not copy a symlink from the source checkout into the verifier
            # clone.  A link can point back through a worktree's .git file or
            # outside the checkout, so a verifier command could mutate the
            # real source while the before/after digest only covers the clone.
            for current, dirnames, filenames in os.walk(source, topdown=True, followlinks=False):
                current_path = Path(current)
                entries = [current_path / name for name in [*dirnames, *filenames]]
                linked = [path for path in entries if path.is_symlink()]
                if linked:
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_symlink_unsafe",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                dirnames[:] = [name for name in dirnames if name != ".git"]
                filenames = [name for name in filenames if name != ".git"]
            git_probe = subprocess.run(
                ["git", "-C", str(source), "rev-parse", "--show-toplevel"],
                capture_output=True,
                text=True,
                check=False,
            )
            if git_probe.returncode == 0 and git_probe.stdout.strip():
                # A verifier that is allowed to inspect Git must receive an
                # actual independent repository.  Copying only files leaves
                # ``git diff``/HEAD checks meaningless and can accidentally
                # preserve a worktree .git pointer.  The clone has no
                # alternates and candidate files are overlaid below so staged,
                # unstaged, added, and deleted files match the source snapshot.
                repo_root = Path(git_probe.stdout.strip()).expanduser().resolve()
                try:
                    source_relative = source.relative_to(repo_root)
                except ValueError:
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_source_outside_root",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                prefix_probe = subprocess.run(
                    ["git", "-C", str(source), "rev-parse", "--show-prefix"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if prefix_probe.returncode != 0 or prefix_probe.stdout.strip().rstrip("/") != source_relative.as_posix().rstrip(".").rstrip("/"):
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_prefix_mismatch",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                snapshot_worktree = snapshot_root / source_relative
                snapshot["source_relative"] = source_relative.as_posix() if str(source_relative) != "." else ""
                head = subprocess.run(
                    ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if head.returncode != 0 or not head.stdout.strip():
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_head_missing",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                cloned = subprocess.run(
                    ["git", "clone", "--no-hardlinks", "--quiet", str(repo_root), str(snapshot_root)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if cloned.returncode != 0:
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_clone_failed",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                alternates = snapshot_root / ".git" / "objects" / "info" / "alternates"
                if alternates.exists():
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_alternates_shared",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                checkout = subprocess.run(
                    ["git", "-C", str(snapshot_root), "checkout", "--detach", "--quiet", head.stdout.strip()],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if checkout.returncode != 0:
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_checkout_failed",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                tracked = subprocess.run(
                    ["git", "-C", str(source), "ls-files", "-z", "--cached"],
                    capture_output=True,
                    check=False,
                )
                if tracked.returncode != 0:
                    return {
                        "verdict": "RED",
                        "reason": "delivery_independent_verifier_git_index_unreadable",
                        "invocation": invocation,
                        "snapshot": snapshot,
                    }
                for raw in tracked.stdout.split(b"\\0"):
                    if not raw:
                        continue
                    relative = Path(os.fsdecode(raw))
                    origin = source / relative
                    destination = snapshot_worktree / relative
                    if not origin.exists() and destination.exists():
                        if destination.is_dir() and not destination.is_symlink():
                            shutil.rmtree(destination)
                        else:
                            destination.unlink()
                for current, dirnames, filenames in os.walk(source, topdown=True, followlinks=False):
                    current_path = Path(current)
                    dirnames[:] = [name for name in dirnames if name != ".git"]
                    for name in filenames:
                        if name == ".git":
                            continue
                        origin = current_path / name
                        relative = origin.relative_to(source)
                        destination = snapshot_worktree / relative
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(origin, destination)
            else:
                # Preserve the existing non-Git negative-test fixture.  Native
                # canaries use real Git worktrees and therefore never claim
                # this files-only fallback as their verifier capability.
                shutil.copytree(
                    source,
                    snapshot_root,
                    symlinks=False,
                    dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns(".git"),
                )
                snapshot_worktree = snapshot_root
            before = _snapshot_digest(snapshot_worktree)
            snapshot["before"] = before
            snapshot["source_snapshot_equal"] = before == source_before
            if not snapshot["source_snapshot_equal"]:
                return {
                    "verdict": "RED",
                    "reason": "delivery_independent_verifier_snapshot_mismatch",
                    "invocation": invocation,
                    "snapshot": snapshot,
                }
            rendered_argv = [item.replace("${WORKTREE}", str(snapshot_worktree)) for item in config["argv"]]
            rendered_cwd = config["cwd"].replace("${WORKTREE}", str(snapshot_worktree))
            cwd = Path(rendered_cwd).expanduser().resolve()
            if cwd != snapshot_worktree.resolve() and snapshot_worktree.resolve() not in cwd.parents:
                return {
                    "verdict": "RED",
                    "reason": "delivery_independent_verifier_cwd_escape",
                    "invocation": invocation,
                    "snapshot": snapshot,
                }
            if self.command_runner is None:
                return {"verdict": "RED", "reason": "execution_fence_unavailable: delivery_verifier_command_port_missing",
                        "invocation": invocation, "snapshot": snapshot}
            try:
                if not isinstance(execution_context, Mapping) or not isinstance(execution_context.get("run_id"), str):
                    raise ValueError("delivery_execution_context_authority_missing")
                context = self._command_execution_context(execution_context["run_id"], source,
                    phase="delivery_verifier", supplied=execution_context)
                bound_contract = self.delivery_binding(context["run_id"])["contract"]
                if contract != bound_contract:
                    raise ValueError("delivery_execution_context_contract_mismatch")
            except (ValueError, KeyError) as exc:
                return {"verdict": "RED", "reason": str(exc), "invocation": invocation, "snapshot": snapshot}
            review_context = None
            if "candidate_review" in contract:
                try:
                    review_context = delivery_engine.candidate_review_context(
                        contract["candidate_review"], candidate_digest=source_before, base_sha=context["base_sha"],
                        checks_digest=execution_context["checks_digest"], scope=contract["scope"],
                        identity={key: context[key] for key in ("goal_id", "goal_revision", "node_id", "unit_id",
                                                                "run_id", "attempt", "fence", "base_sha")})
                    context = {**context, "candidate_digest": source_before,
                               "checks_digest": execution_context["checks_digest"],
                               "candidate_review_context": review_context}
                except (ValueError, KeyError) as exc:
                    return {"verdict": "RED", "reason": str(exc), "invocation": invocation, "snapshot": snapshot}
            # A reviewing verifier receives its bound context on stdin.
            review_input = ({"input_request": {**context, "worktree": str(snapshot_worktree)}}
                            if review_context is not None else {})
            try:
                completed, fence_evidence, _descriptor = self.command_runner(
                    context, phase=context["execution_phase"], argv=rendered_argv,
                    worktree=str(cwd), timeout_seconds=float(config["timeout_seconds"]), **review_input)
                invocation.update({
                    **fence_evidence,
                    "invoked": True,
                    "exit_code": completed.returncode,
                    "stdout_digest": hashlib.sha256(completed.stdout.encode()).hexdigest(),
                    "stderr_digest": hashlib.sha256(completed.stderr.encode()).hexdigest(),
                })
            except subprocess.TimeoutExpired as exc:
                invocation.update({
                    "invoked": True,
                    "exit_code": None,
                    "timeout": True,
                    "stdout_digest": hashlib.sha256((exc.stdout or "").encode() if isinstance(exc.stdout, str) else b"").hexdigest(),
                    "stderr_digest": hashlib.sha256((exc.stderr or "").encode() if isinstance(exc.stderr, str) else b"").hexdigest(),
                })
            except OSError as exc:
                invocation.update({"invoked": True, "exit_code": None, "error": type(exc).__name__})
            after = _snapshot_digest(snapshot_worktree)
            source_after = _snapshot_digest(source)
            snapshot.update({
                "after": after,
                "unchanged": before == after,
                "source_after": source_after,
                "source_unchanged": source_before == source_after,
            })
            if invocation.get("timeout"):
                reason = "delivery_independent_verifier_timeout"
            elif invocation.get("exit_code") != 0:
                reason = "delivery_independent_verifier_failed"
            elif snapshot.get("source_unchanged") is not True:
                reason = "delivery_independent_verifier_source_mutated"
            elif snapshot.get("unchanged") is not True:
                reason = "delivery_independent_verifier_snapshot_mutated"
            else:
                reason = "independent_verifier_passed"
            review_proof: dict[str, Any] = {}
            if review_context is not None and reason == "independent_verifier_passed":
                # Exit 0 is not a review: the output must be a review whose own
                # findings support GREEN, and only then is it sealed.
                try:
                    value = delivery_engine.load_candidate_review_json(completed.stdout)
                    if (not isinstance(value, dict) or value.get("verdict") != "GREEN"
                            or delivery_engine.validate_candidate_review_result(value.get("review"), review_context) != "GREEN"):
                        raise ValueError("candidate_review_not_green")
                    review_proof = delivery_engine.seal_candidate_review_proof(value["review"], review_context, self.root)
                    review_proof.update(candidate_digest=source_before, checks_digest=review_context["checks_digest"])
                except (ValueError, KeyError, TypeError, OSError) as exc:
                    reason = "delivery_independent_review_invalid:" + str(exc)
            return {
                "verdict": "GREEN" if reason == "independent_verifier_passed" else "RED",
                "reason": reason,
                "invocation": invocation,
                "snapshot": snapshot,
                **review_proof,
            }
        except OSError as exc:
            return {
                "verdict": "RED",
                "reason": f"delivery_independent_verifier_snapshot_error:{type(exc).__name__}",
                "invocation": invocation,
                "snapshot": snapshot,
            }
        finally:
            remove_tree(snapshot_parent)

    @staticmethod
    def _verify_independent_verifier_evidence(contract: dict[str, Any], verifier: Any) -> dict[str, Any]:
        config, config_error = _independent_verifier_config(contract)
        if config is None:
            return {"verdict": "RED", "reason": config_error or "delivery_independent_verifier_capability_missing"}
        if not isinstance(verifier, dict):
            return {"verdict": "RED", "reason": "delivery_independent_verifier_missing"}
        if verifier.get("capability") != config["capability"]:
            return {"verdict": "RED", "reason": "delivery_independent_verifier_capability_mismatch"}
        invocation = verifier.get("invocation")
        snapshot = verifier.get("snapshot")
        if not isinstance(invocation, dict) or invocation.get("invoked") is not True:
            return {"verdict": "RED", "reason": "delivery_independent_verifier_not_invoked"}
        if invocation.get("argv") != config["argv"] or invocation.get("cwd") != config["cwd"]:
            return {"verdict": "RED", "reason": "delivery_independent_verifier_invocation_mismatch"}
        if invocation.get("exit_code") != 0:
            return {"verdict": "RED", "reason": "delivery_independent_verifier_failed"}
        if not isinstance(invocation.get("stdout_digest"), str) or not isinstance(invocation.get("stderr_digest"), str):
            return {"verdict": "RED", "reason": "delivery_independent_verifier_output_missing"}
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("unchanged") is not True
            or snapshot.get("before") != snapshot.get("after")
            or snapshot.get("source_unchanged") is not True
            or snapshot.get("source_before") != snapshot.get("source_after")
        ):
            return {"verdict": "RED", "reason": "delivery_independent_verifier_snapshot_invalid"}
        if verifier.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": "delivery_independent_verifier_not_green"}
        if "candidate_review" in contract:
            try:
                delivery_engine.verify_candidate_review_proof(contract, verifier)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                return {"verdict": "RED", "reason": str(exc)}
        return {"verdict": "GREEN", "reason": "independent_verifier_evidence_valid"}

    def run_delivery_obligations(self, run_id: str, worktree: str | Path, *, phase: str) -> dict[str, Any]:
        """Run the contract's original obligation commands in the authority workspace.

        A provider may describe a GREEN obligation receipt, but it cannot make
        that receipt authoritative.  The generic engine owns command receipt
        construction; this adapter only supplies the persisted Run binding and
        returns the engine's original results for the controller to embed before
        source eligibility or final closure.
        """
        if phase not in DELIVERY_PHASES:
            raise ValueError(f"invalid delivery phase: {phase}")
        binding = self.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return {"verdict": "RED", "required": True, "phase": phase, "reason": binding.get("reason", "delivery_binding_invalid")}
        if self.command_runner is None:
            return {"verdict": "RED", "required": True, "phase": phase,
                    "reason": "execution_fence_unavailable: delivery_command_port_missing",
                    "invocation": {"invoked": False}}
        try:
            context = self._command_execution_context(run_id, worktree, phase="delivery_checks")
        except (ValueError, KeyError) as exc:
            return {"verdict": "RED", "required": True, "phase": phase, "reason": str(exc),
                    "invocation": {"invoked": False}}
        try:
            package = delivery_engine.run_obligation_commands(binding["contract"], worktree, phase=phase,
                command_runner=self.command_runner, execution_context=context)
        except Exception as exc:
            return {"verdict": "RED", "required": True, "phase": phase, "reason": f"delivery_obligation_runner_error:{type(exc).__name__}"}
        obligations = package.get("obligations") if isinstance(package, dict) else None
        if not isinstance(obligations, dict):
            return {"verdict": "RED", "required": True, "phase": phase, "reason": "delivery_obligations_missing"}
        if package.get("contract_digest") != binding.get("contract_digest"):
            return {"verdict": "RED", "required": True, "phase": phase, "reason": "delivery_contract_digest_mismatch"}
        failed = [
            obligation_id
            for obligation_id, row in obligations.items()
            if not isinstance(row, dict)
            or row.get("verdict") != "GREEN"
            or row.get("contract_digest") != binding.get("contract_digest")
        ]
        result: dict[str, Any] = {
            "verdict": "GREEN" if not failed else "RED",
            "required": True,
            "phase": phase,
            "contract_digest": binding.get("contract_digest"),
            "obligations": obligations,
        }
        if failed:
            result["reason"] = "delivery_obligation_failed"
            result["detail"] = failed
        else:
            result["reason"] = "delivery_obligations_executed"
        return result

    def assemble_delivery_evidence(
        self,
        run_id: str,
        ordinal: int,
        *,
        phase: str,
        worktree: str | Path,
        diff_digest: str,
        changed_paths: list[str],
        dispatch_key: str,
        obligations: dict[str, Any],
        checker: dict[str, Any] | None = None,
        provider: dict[str, Any] | None = None,
        provider_ref: dict[str, str] | None = None,
        terminal_state: str | None = None,
        external_readback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Assemble a delivery package from authority-owned runtime facts.

        Model/CLI output is deliberately limited to advisory metadata.  The
        packet, plan/admission receipts, obligation receipts, and independent
        verifier receipt are produced here from the persisted Run binding and
        checker/CI originals.  A caller-supplied ``delivery_evidence`` field is
        never copied into this package.
        """
        if phase not in DELIVERY_PHASES:
            return {"verdict": "RED", "reason": "invalid_delivery_phase", "phase": phase}
        binding = self.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": binding.get("reason", "delivery_binding_invalid"), "phase": phase}
        packet = self.delivery_packet(run_id)
        if not isinstance(packet, dict):
            return {"verdict": "RED", "reason": "delivery_packet_missing", "phase": phase}
        run = self.get_run(run_id)
        fence = self.attempt_fence(run_id, ordinal)
        identity = self._delivery_identity(
            run,
            ordinal=ordinal,
            fence=fence,
            diff_digest=diff_digest,
            dispatch_key=dispatch_key,
        )
        contract = binding["contract"]
        plan = binding["plan"]

        def receipt(body: dict[str, Any]) -> dict[str, Any]:
            try:
                digest = delivery_engine.receipt_digest(body)
            except AttributeError:
                return {"verdict": "RED", "reason": "delivery_receipt_builder_unavailable", "phase": phase}
            return {**body, "receipt_digest": digest}

        packet_admission = delivery_engine.verify_packet_binding(
            packet,
            contract,
            plan,
        )
        if not isinstance(packet_admission, dict) or packet_admission.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": "delivery_packet_admission_not_green", "phase": phase, "detail": packet_admission}
        envelope = {
            "delivery_unit_id": binding["unit_id"],
            "delivery_unit_contract_digest": binding["contract_digest"],
            **identity,
        }
        dispatch_receipt = delivery_engine.verify_dispatch_binding(
            envelope=envelope,
            packet=packet,
            contract=contract,
            plan=plan,
        )
        if not isinstance(dispatch_receipt, dict) or dispatch_receipt.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": "delivery_dispatch_not_green", "phase": phase, "detail": dispatch_receipt}

        obligation_rows = obligations if isinstance(obligations, dict) else {}
        required_obligation_ids = delivery_engine.obligation_ids_for_phase(contract, phase)
        if set(obligation_rows) != set(required_obligation_ids):
            return {
                "verdict": "RED",
                "reason": "delivery_obligation_phase_mismatch",
                "phase": phase,
                "required": required_obligation_ids,
                "observed": sorted(obligation_rows),
            }
        obligations_green = bool(obligation_rows) and all(
            isinstance(row, dict)
            and row.get("verdict") == "GREEN"
            and row.get("contract_digest") == binding["contract_digest"]
            for row in obligation_rows.values()
        )
        checker_exit = 0 if obligations_green else 1
        checker_argv: list[str] = ["delivery-obligations"]
        checker_stdout = ""
        checker_stderr = ""
        if isinstance(checker, dict):
            if isinstance(checker.get("exit_code"), int):
                checker_exit = checker["exit_code"]
            if isinstance(checker.get("argv"), list):
                checker_argv = [str(item) for item in checker["argv"]]
            if isinstance(checker.get("stdout"), str):
                checker_stdout = checker["stdout"]
            if isinstance(checker.get("stderr"), str):
                checker_stderr = checker["stderr"]
        checker_green = checker_exit == 0 and obligations_green
        verifier_run = self._run_independent_verifier(contract, worktree,
            execution_context={**identity, "authority_store": "run", "worktree": str(Path(worktree).expanduser().resolve()),
                               "execution_phase": "delivery_verifier",
                               **({"checks_digest": delivery_engine.digest_json(obligation_rows)}
                                  if "candidate_review" in contract else {})})
        verifier_body = {
            "schema": "lh-delivery-unit-independent-verifier/v1",
            "principal": contract["independent_verifier"]["principal"],
            "read_only": True,
            "source_write": False,
            "capability": contract["independent_verifier"].get("capability"),
            "verdict": verifier_run.get("verdict", "RED"),
            "reason": verifier_run.get("reason"),
            "contract_digest": binding["contract_digest"],
            "invocation": verifier_run.get("invocation"),
            "snapshot": verifier_run.get("snapshot"),
            **{key: verifier_run[key] for key in ("review", "review_ref", "candidate_review_context",
                                                  "candidate_digest", "checks_digest") if key in verifier_run},
            "checker": {
                "argv": checker_argv,
                "exit_code": checker_exit,
                "stdout_digest": hashlib.sha256(checker_stdout.encode()).hexdigest(),
                "stderr_digest": hashlib.sha256(checker_stderr.encode()).hexdigest(),
            },
            **identity,
        }
        verifier_receipt = receipt(verifier_body)
        verifier_valid = verifier_run.get("verdict") == "GREEN"
        if verifier_receipt.get("reason") == "delivery_receipt_builder_unavailable":
            return verifier_receipt
        if not verifier_valid:
            return {**verifier_body, "receipt": verifier_receipt, "phase": phase}
        if phase == "final" and external_readback is not None:
            normalized = external_readback.get("normalized") if isinstance(external_readback, dict) else None
            if (
                not isinstance(normalized, dict)
                or normalized.get("status") not in {"ready", "already_normalized"}
                or normalized.get("outcome") != "verified"
            ):
                return {"verdict": "RED", "reason": "delivery_normalized_ci_evidence_missing", "phase": phase}
        delivery_green = checker_green and verifier_valid
        provider_summary = provider.get("summary") if isinstance(provider, dict) else "controller assembled delivery"
        executor_body = {
            "schema": "lh-delivery-unit-executor-receipt/v1",
            "receipt_name": "executor",
            "verdict": "GREEN" if delivery_green else "RED",
            "contract_digest": binding["contract_digest"],
            "provider_summary_digest": hashlib.sha256(str(provider_summary).encode()).hexdigest(),
            "provider_artifact": provider_ref,
            **identity,
        }
        executor_receipt = receipt(executor_body)
        if executor_receipt.get("verdict") == "RED":
            return executor_receipt
        plan_receipt = dict(plan)
        receipts: dict[str, Any] = {
            "plan_verdict": plan_receipt,
            "packet_admission": packet_admission,
            "dispatch": dispatch_receipt,
            "executor": executor_receipt,
            "delivery_verifier": verifier_receipt,
        }
        if phase == "final":
            completion_body = {
                "schema": "lh-delivery-unit-completion-receipt/v1",
                "receipt_name": "completion",
                "verdict": "GREEN" if delivery_green else "RED",
                "terminal_state": terminal_state,
                "contract_digest": binding["contract_digest"],
                "external": external_readback,
                **identity,
            }
            receipts["completion"] = receipt(completion_body)
        evidence: dict[str, Any] = {
            "contract_digest": binding["contract_digest"],
            "unit_id": binding["unit_id"],
            "phase": phase,
            "worktree": str(Path(worktree).resolve()),
            "identity": identity,
            "run_id": run_id,
            "attempt": ordinal,
            "fence": fence,
            "dispatch_key": dispatch_key,
            "diff_digest": diff_digest,
            "changed_paths": sorted(set(changed_paths)),
            "verdict": "GREEN" if delivery_green else "RED",
            "packet": packet,
            "verifier": {**verifier_body, "receipt": verifier_receipt},
            "plan_verdict": plan_receipt,
            "receipts": receipts,
            "obligations": obligation_rows,
            "source_vs_live": contract["source_vs_live"],
        }
        if phase == "final":
            evidence["terminal_state"] = terminal_state
            evidence["state_readback"] = {
                "authority_store": "run",
                "run_id": run_id,
                "attempt": ordinal,
                "fence": fence,
                "run_state": terminal_state,
                "terminal_state": terminal_state,
            }
            if external_readback is not None:
                evidence["external_readback"] = external_readback
        return evidence

    def record_planning_request(
        self,
        run_id: str,
        *,
        reason: str,
        phase: str = "source",
        request_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Durably park a legacy/missing binding for planning migration."""
        run = self.get_run(run_id)
        binding = self._delivery_identity(
            run,
            ordinal=int(run.get("attempts", 0)),
            fence=int(run.get("fence", 0)),
        )
        payload = {
            "schema": "lh-delivery-planning-request/v1",
            "run_id": run_id,
            "phase": phase,
            "reason": reason,
            "attempts": run.get("attempts", 0),
            "fence": run.get("fence", 0),
            "preserve_execution": True,
            "binding": binding,
        }
        current_binding = run.get("delivery_contract") or _delivery_from_goal(run.get("goal"), "delivery_contract")
        current_digest = run.get("delivery_contract_digest") or (
            current_binding.get("contract_digest") if isinstance(current_binding, dict) else None
        )
        if request_payload is not None:
            payload = json.loads(json.dumps(request_payload, sort_keys=True))
        else:
            try:
                payload = delivery_engine.build_planning_request(
                    binding,
                    reason=reason,
                    state=str(run.get("state", "queued")),
                    fence=int(run.get("fence", 0)),
                    request_id=f"plan:{run_id}:{phase}",
                )
            except delivery_engine.DeliveryUnitError:
                # Keep the explicit durable fallback shape; it still cannot
                # activate execution and is visible to the planning consumer.
                pass
        if request_payload is None and isinstance(current_digest, str) and current_digest:
            payload["supersedes"] = current_digest
        request_id = payload.get("request_id", "default") if isinstance(payload, dict) else "default"
        event_id = f"delivery-planning-required:{run_id}:{phase}:{request_id}"
        self.append_event(run_id, "delivery_planning_required", payload, event_id=event_id)
        return {"run_id": run_id, **payload, "status": "planning_required"}

    def _build_delivery_planning_request(
        self,
        run: dict[str, Any],
        contract: dict[str, Any],
        *,
        reason: str,
        supersedes: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        goal = contract.get("goal") if isinstance(contract.get("goal"), dict) else {}
        node = contract.get("node") if isinstance(contract.get("node"), dict) else {}
        binding = self._delivery_identity(
            run,
            ordinal=int(run.get("attempts", 0)),
            fence=int(run.get("fence", 0)),
        )
        binding.update({
            "goal_id": goal.get("id", goal.get("goal_id")),
            "goal_revision": goal.get("revision", goal.get("goal_revision")),
            "node_id": node.get("id", node.get("node_id")),
            "unit_id": contract.get("unit_id"),
            "contract_digest": contract.get("contract_digest"),
            "node_kind": "planning",
            "producer": "PlanNodeController",
        })
        payload = delivery_engine.build_planning_request(
            binding,
            reason=reason,
            state=str(run.get("state", "queued")),
            fence=int(run.get("fence", 0)),
            request_id=request_id or f"plan:{run.get('run_id')}:{contract.get('contract_digest', '')[:16]}",
        )
        if supersedes is not None:
            payload = {**payload, "supersedes": supersedes}
            body = {key: value for key, value in payload.items() if key != "input_digest"}
            payload["input_digest"] = delivery_engine.digest_json(body)
        return payload

    def plan_pending_delivery(self, run_id: str) -> dict[str, Any]:
        """Consume an explicit producer-supplied planning candidate once.

        A candidate is allowed to contain a complete contract (or the
        contract body plus an optional plan); the runtime never invents scope,
        identity, or obligations from a Goal name.  Missing candidates remain
        a durable planning request and cannot start an Attempt.
        """
        run = self.get_run(run_id)
        goal = run.get("goal") if isinstance(run.get("goal"), dict) else {}
        candidate: Any = goal.get("delivery_candidate") or goal.get("delivery_definition")
        envelope = goal.get("admission_envelope")
        if candidate is None and isinstance(envelope, dict):
            candidate = envelope.get("delivery_candidate") or envelope.get("delivery_definition")
        if not isinstance(candidate, dict):
            return self.record_planning_request(run_id, reason="delivery_contract_missing")
        contract = candidate.get("contract") or candidate.get("delivery_contract") or candidate
        if not isinstance(contract, dict):
            return self.record_planning_request(run_id, reason="delivery_planning_candidate_invalid")
        request_payload: dict[str, Any] | None = None
        try:
            if "contract_digest" in contract:
                resolved = delivery_engine.validate_contract(contract)
            else:
                resolved = delivery_engine.seal_contract(contract)
            supersedes = candidate.get("supersedes") or candidate.get("supersedes_contract_digest")
            request = self._build_delivery_planning_request(
                run,
                resolved,
                reason="delivery_contract_replan" if supersedes is not None else "delivery_contract_missing",
                supersedes=supersedes,
                request_id=candidate.get("request_id"),
            )
            request_payload = request
            capability = candidate.get("planning_capability") or candidate.get("capability")
            if capability is None:
                envelope = goal.get("admission_envelope")
                if isinstance(envelope, dict):
                    capability = envelope.get("delivery_planning_capability") or envelope.get("planning_capability")
            planning_result = delivery_engine.check_planning_request(
                request, resolved, capability=capability,
            )
            if not isinstance(planning_result, dict) or planning_result.get("status") != "completed":
                return self.record_planning_request(
                    run_id,
                    reason=str((planning_result or {}).get("reason", "delivery_planning_pending")) if isinstance(planning_result, dict) else "delivery_planning_pending",
                    request_payload=request,
                )
            # The contract checker is the single source of the canonical
            # sealed plan; ignore any caller-supplied plan projection.
            plan = planning_result.get("plan_verdict")
            if not isinstance(plan, dict):
                return self.record_planning_request(
                    run_id,
                    reason="delivery_planning_plan_missing",
                    request_payload=request,
                )
            if supersedes is not None:
                result = self.replan_delivery_contract(
                    run_id,
                    contract=resolved,
                    plan=plan,
                    supersedes=supersedes,
                    fence=candidate.get("fence", candidate.get("replan_fence")),
                    request_id=candidate.get("request_id"),
                )
                return {
                    "status": "delivery_replanned",
                    "run_id": run_id,
                    "binding": result,
                    "planning": planning_result,
                    "supersedes": supersedes,
                }
            result = self.bind_delivery_contract(run_id, contract=resolved, plan=plan)
        except Exception as exc:
            return self.record_planning_request(
                run_id,
                reason=f"delivery_planning_rejected:{type(exc).__name__}",
                request_payload=request_payload,
            )
        return {"status": "delivery_bound", "run_id": run_id, "binding": result, "planning": planning_result}

    def bind_delivery_contract(self, run_id: str, *, contract: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
        """Persist a validated contract/plan without changing Run identity."""
        resolved = delivery_engine.validate_contract(contract)
        plan_result = delivery_engine.verify_plan_verdict(plan, resolved)
        if not isinstance(plan_result, dict) or plan_result.get("verdict") != "GREEN":
            raise ValueError(str(plan_result.get("reason", "delivery_plan_not_green")))
        if resolved.get("authority_store", "work_unit") != "run":
            raise ValueError("delivery_authority_store_not_run")
        existing = self.get_run(run_id)
        identity_reason = self._delivery_binding_identity_reason(existing, resolved)
        if identity_reason is not None:
            raise ValueError(identity_reason)
        existing_binding = existing.get("delivery_contract") or _delivery_from_goal(existing.get("goal"), "delivery_contract")
        existing_digest = existing.get("delivery_contract_digest") or (
            existing_binding.get("contract_digest") if isinstance(existing_binding, dict) else None
        )
        existing_plan = existing.get("delivery_plan") or _delivery_from_goal(existing.get("goal"), "delivery_plan")
        existing_plan_digest = existing_plan.get("plan_verdict_digest") if isinstance(existing_plan, dict) else None
        new_plan_digest = plan.get("plan_verdict_digest") if isinstance(plan, dict) else None
        if existing_digest and existing_digest != resolved.get("contract_digest"):
            raise ValueError("delivery_binding_change_requires_replan")
        if existing_plan_digest and existing_plan_digest != new_plan_digest:
            raise ValueError("delivery_plan_change_requires_replan")
        if int(existing.get("attempts", 0)) > 0 and (
            existing_digest != resolved.get("contract_digest") or existing_plan_digest != new_plan_digest
        ):
            raise ValueError("delivery_binding_change_requires_replan")
        contract_json = json.dumps(resolved, sort_keys=True)
        plan_json = json.dumps(plan, sort_keys=True)
        digest = resolved.get("contract_digest")
        existing_ceiling = existing.get("delivery_attempt_ceiling")
        try:
            existing_ceiling = int(existing_ceiling)
        except (TypeError, ValueError):
            existing_ceiling = int(existing.get("max_attempts", 4))
        delivery_attempt_ceiling = min(
            existing_ceiling,
            int(existing.get("max_attempts", 4)),
            self._contract_repair_max(resolved) or int(existing.get("max_attempts", 4)),
        )
        now = time.time()
        with self._connect() as conn:
            updated = conn.execute(
                "UPDATE runs SET delivery_contract_json = ?, delivery_plan_json = ?, delivery_contract_digest = ?, delivery_attempt_ceiling = ?, updated_at = ? WHERE run_id = ?",
                (contract_json, plan_json, digest, delivery_attempt_ceiling, now, run_id),
            ).rowcount
            if updated != 1:
                raise KeyError(f"unknown run_id: {run_id}")
        self.append_event(run_id, "delivery_contract_bound", {"contract_digest": digest, "unit_id": resolved.get("unit_id")})
        return self.delivery_binding(run_id)

    def replan_delivery_contract(
        self,
        run_id: str,
        *,
        contract: dict[str, Any],
        plan: dict[str, Any],
        supersedes: str,
        fence: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Apply an explicit, fenced contract supersession without resetting budget.

        Planning owns the decision to replan.  RunStore only checks the old
        digest, current execution fence, and inactive phase before replacing
        the binding; attempts, max_attempts, and historical artifacts remain
        untouched.
        """
        if not isinstance(supersedes, str) or not supersedes:
            raise ValueError("delivery_replan_supersedes_missing")
        resolved = delivery_engine.validate_contract(contract)
        plan_result = delivery_engine.verify_plan_verdict(plan, resolved)
        if not isinstance(plan_result, dict) or plan_result.get("verdict") != "GREEN":
            raise ValueError(str(plan_result.get("reason", "delivery_plan_not_green")))
        if resolved.get("authority_store", "work_unit") != "run":
            raise ValueError("delivery_authority_store_not_run")
        if fence is None:
            raise ValueError("delivery_replan_fence_missing")
        contract_json = json.dumps(resolved, sort_keys=True)
        plan_json = json.dumps(plan, sort_keys=True)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            current = self._row(row)
            if current is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown run_id: {run_id}")
            current_digest = current.get("delivery_contract_digest")
            current_fence = int(current.get("fence", 0))
            if current_digest != supersedes:
                conn.execute("ROLLBACK")
                raise ValueError("delivery_replan_supersedes_mismatch")
            if fence != current_fence:
                conn.execute("ROLLBACK")
                raise ValueError("delivery_replan_fence_mismatch")
            identity_reason = self._delivery_binding_identity_reason(current, resolved)
            if identity_reason is not None:
                conn.execute("ROLLBACK")
                raise ValueError(identity_reason)
            if current.get("state") not in {"queued", "retry_pending"}:
                conn.execute("ROLLBACK")
                raise ValueError("delivery_replan_active_attempt")
            latest = conn.execute(
                "SELECT ordinal, state, fence, receipt_ref, receipt_digest, finished_at FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if latest is not None:
                latest_state = str(latest["state"])
                if latest_state in {"running", "awaiting_external_verdict"} or latest["finished_at"] is None:
                    conn.execute("ROLLBACK")
                    raise ValueError("delivery_replan_attempt_not_reconciled")
                if not self._receipt_artifact_valid(run_id, int(latest["ordinal"]), latest["receipt_ref"], latest["receipt_digest"]):
                    conn.execute("ROLLBACK")
                    raise ValueError("delivery_replan_attempt_receipt_invalid")
            if resolved.get("contract_digest") == current_digest:
                conn.execute("ROLLBACK")
                raise ValueError("delivery_replan_noop")
            current_ceiling = current.get("delivery_attempt_ceiling")
            try:
                current_ceiling = int(current_ceiling)
            except (TypeError, ValueError):
                current_ceiling = min(
                    int(current.get("max_attempts", 4)),
                    self._contract_repair_max(current.get("delivery_contract")) or int(current.get("max_attempts", 4)),
                )
            delivery_attempt_ceiling = min(
                current_ceiling,
                int(current.get("max_attempts", 4)),
                self._contract_repair_max(resolved) or int(current.get("max_attempts", 4)),
            )
            updated = conn.execute(
                "UPDATE runs SET delivery_contract_json = ?, delivery_plan_json = ?, delivery_contract_digest = ?, delivery_attempt_ceiling = ?, delivery_source_evidence_json = NULL, delivery_source_digest = NULL, delivery_final_evidence_json = NULL, delivery_final_digest = NULL, delivery_terminal_state = NULL, updated_at = ? WHERE run_id = ? AND state IN ('queued', 'retry_pending') AND fence = ? AND delivery_contract_digest = ?",
                (contract_json, plan_json, resolved.get("contract_digest"), delivery_attempt_ceiling, now, run_id, current_fence, current_digest),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                raise ValueError("delivery_replan_fence_rejected")
            event_payload = {
                "request_id": request_id,
                "supersedes": supersedes,
                "contract_digest": resolved.get("contract_digest"),
                "plan_verdict_digest": plan.get("plan_verdict_digest"),
                "fence": current_fence,
                "attempts": current.get("attempts", 0),
                "max_attempts": current.get("max_attempts"),
                "delivery_attempt_ceiling": delivery_attempt_ceiling,
            }
            self._append_event_conn(
                conn,
                event_id=f"delivery-replanned:{run_id}:{resolved.get('contract_digest')}",
                run_id=run_id,
                event_type="delivery_contract_replanned",
                payload=event_payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return self.delivery_binding(run_id)

    def _delivery_evidence_row(self, run_id: str, ordinal: int | None = None) -> dict[str, Any] | None:
        with self._connect() as conn:
            if ordinal is None:
                row = conn.execute("SELECT * FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1", (run_id,)).fetchone()
            else:
                row = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
        if row is None:
            return None
        return dict(row)

    def delivery_evidence(self, run_id: str, *, phase: str = "final", ordinal: int | None = None) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if phase == "source":
            raw = run.get("delivery_source_evidence")
        elif phase == "final":
            raw = run.get("delivery_final_evidence")
        else:
            raise ValueError(f"invalid delivery phase: {phase}")
        if isinstance(raw, dict):
            return raw
        row = self._delivery_evidence_row(run_id, ordinal)
        if row is None:
            return None
        ref = row.get("delivery_source_ref" if phase == "source" else "delivery_final_ref")
        if not isinstance(ref, str) or not ref:
            return None
        path = (self.root / ref).resolve()
        try:
            if not path.is_relative_to(self.root.resolve()):
                return None
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _verify_delivery_evidence(self, run: dict[str, Any], evidence: dict[str, Any], *, phase: str) -> dict[str, Any]:
        binding = self.delivery_binding(str(run["run_id"]))
        if binding.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": binding.get("reason", "delivery_binding_invalid"), "phase": phase}
        if phase == "final" and run.get("state") not in {"verified", "integrated"}:
            # A caller cannot put a future terminal state in evidence while
            # the authority Run is still running/parked.  Terminal callers use
            # the atomic transition helpers below, which validate the same
            # package against the intended state before committing it.
            return {"verdict": "RED", "reason": "delivery_terminal_state_not_durable", "phase": phase}
        verifier_result = self._verify_independent_verifier_evidence(
            binding["contract"], evidence.get("verifier") if isinstance(evidence, dict) else None,
        )
        if verifier_result.get("verdict") != "GREEN":
            return {**verifier_result, "phase": phase}
        try:
            verify_kwargs: dict[str, Any] = {"phase": phase}
            # Source eligibility intentionally precedes CI/owner/final state.
            # The engine's compatibility switch relaxes only final-only
            # receipts; it never skips the explicit source receipt set or
            # obligations.  An interface mismatch is a real RED, not a
            # fallback to an older verifier that could erase phase semantics.
            if phase == "source":
                verify_kwargs["allow_machine_complete_missing"] = True
            result = delivery_engine.verify_delivery(binding["contract"], evidence, **verify_kwargs)
        except Exception as exc:
            return {"verdict": "RED", "reason": f"delivery_verifier_error:{type(exc).__name__}", "phase": phase}
        if not isinstance(result, dict):
            return {"verdict": "RED", "reason": "delivery_verifier_invalid_result", "phase": phase}
        return {**result, "phase": phase, "authority_store": "run", "contract_digest": binding.get("contract_digest")}

    def record_delivery_evidence(
        self,
        run_id: str,
        ordinal: int,
        *,
        phase: str,
        evidence: dict[str, Any],
        fence: int | None = None,
    ) -> dict[str, Any]:
        """Verify and persist one source/final evidence package.

        The supplied package is accepted only after it binds to the current
        Run/Attempt fence and the generic engine verifies the phase.  The
        caller cannot make ``finish_attempt`` trust an arbitrary digest: the
        evidence is read back from these durable fields/artifacts.
        """
        if phase not in DELIVERY_PHASES:
            raise ValueError(f"invalid delivery phase: {phase}")
        run = self.get_run(run_id)
        binding = self.delivery_binding(run_id)
        if binding.get("verdict") != "GREEN":
            return {"verdict": "RED", "reason": binding.get("reason", "delivery_binding_invalid"), "phase": phase}
        row = self._delivery_evidence_row(run_id, ordinal)
        if row is None:
            return {"verdict": "RED", "reason": "delivery_attempt_missing", "phase": phase}
        current_fence = int(run.get("fence", 0))
        attempt_fence = int(row.get("fence", 0))
        expected_fence = attempt_fence if fence is None else fence
        if attempt_fence != current_fence or expected_fence != current_fence or row.get("state") not in {"running", "awaiting_external_verdict"}:
            return {"verdict": "RED", "reason": "delivery_attempt_fence_mismatch", "phase": phase}
        if not isinstance(evidence, dict):
            return {"verdict": "RED", "reason": "delivery_evidence_missing", "phase": phase}
        expected_digest = binding.get("contract_digest")
        if evidence.get("contract_digest") != expected_digest:
            return {"verdict": "RED", "reason": "delivery_contract_digest_mismatch", "phase": phase}
        identity = evidence.get("identity")
        if not isinstance(identity, dict):
            return {"verdict": "RED", "reason": "delivery_identity_missing", "phase": phase}
        diff_digest = identity.get("diff_digest") or evidence.get("diff_digest")
        dispatch_key = identity.get("dispatch_key") or evidence.get("dispatch_key")
        expected_identity = self._delivery_identity(
            run,
            ordinal=ordinal,
            fence=current_fence,
            diff_digest=diff_digest if isinstance(diff_digest, str) else None,
            dispatch_key=dispatch_key if isinstance(dispatch_key, str) else None,
        )
        for field, value in expected_identity.items():
            if field in identity and identity.get(field) != value:
                return {"verdict": "RED", "reason": f"delivery_identity_{field}_mismatch", "phase": phase}
        if identity.get("run_id") != run_id or identity.get("attempt") != ordinal or identity.get("fence") != current_fence:
            return {"verdict": "RED", "reason": "delivery_execution_identity_mismatch", "phase": phase}
        result = self._verify_delivery_evidence(run, evidence, phase=phase)
        if result.get("verdict") != "GREEN":
            return result
        raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        artifact_name = f"delivery-{phase}.json"
        artifact = self.write_artifact(run_id, ordinal, artifact_name, raw)
        now = time.time()
        with self._connect() as conn:
            if phase == "source":
                updated_attempt = conn.execute(
                    "UPDATE attempts SET delivery_source_ref = ?, delivery_source_digest = ? WHERE run_id = ? AND ordinal = ? AND state IN ('running', 'awaiting_external_verdict') AND fence = ?",
                    (artifact["ref"], artifact["digest"], run_id, ordinal, current_fence),
                ).rowcount
                updated_run = conn.execute(
                    "UPDATE runs SET delivery_source_evidence_json = ?, delivery_source_digest = ?, updated_at = ? WHERE run_id = ? AND fence = ?",
                    (raw, artifact["digest"], now, run_id, current_fence),
                ).rowcount
            else:
                updated_attempt = conn.execute(
                    "UPDATE attempts SET delivery_final_ref = ?, delivery_final_digest = ? WHERE run_id = ? AND ordinal = ? AND state IN ('running', 'awaiting_external_verdict') AND fence = ?",
                    (artifact["ref"], artifact["digest"], run_id, ordinal, current_fence),
                ).rowcount
                updated_run = conn.execute(
                    "UPDATE runs SET delivery_final_evidence_json = ?, delivery_final_digest = ?, delivery_terminal_state = ?, updated_at = ? WHERE run_id = ? AND fence = ?",
                    (raw, artifact["digest"], evidence.get("terminal_state"), now, run_id, current_fence),
                ).rowcount
            if updated_attempt != 1 or updated_run != 1:
                return {"verdict": "RED", "reason": "delivery_evidence_fence_rejected", "phase": phase}
        self.append_event(run_id, "delivery_evidence_recorded", {"attempt": ordinal, "phase": phase, "digest": artifact["digest"], "contract_digest": expected_digest})
        return {**result, "required": True, "phase": phase, "evidence_ref": artifact["ref"], "evidence_digest": artifact["digest"]}

    def verify_delivery(self, run_id: str, *, phase: str = "final", ordinal: int | None = None) -> dict[str, Any]:
        evidence = self.delivery_evidence(run_id, phase=phase, ordinal=ordinal)
        if evidence is None:
            return {"verdict": "RED", "required": True, "reason": f"delivery_{phase}_evidence_missing", "phase": phase}
        run = self.get_run(run_id)
        result = self._verify_delivery_evidence(run, evidence, phase=phase)
        result["required"] = True
        return result

    def _receipt_artifact_valid(self, run_id: str, ordinal: int, receipt_ref: str, receipt_digest: str) -> bool:
        if not isinstance(receipt_ref, str) or not isinstance(receipt_digest, str):
            return False
        path = (self.root / receipt_ref).resolve()
        try:
            if not path.is_relative_to(self.root.resolve()):
                return False
            raw = path.read_bytes()
            if "sha256:" + hashlib.sha256(raw).hexdigest() != receipt_digest:
                return False
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        return (
            isinstance(value, dict)
            and value.get("schema") == "loop-hybrid-attempt-receipt/v1"
            and value.get("run_id") == run_id
            and value.get("attempt") == ordinal
        )

    @staticmethod
    def _stable_id(prefix: str, payload: Any) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        return prefix + hashlib.sha256(encoded).hexdigest()[:32]

    @staticmethod
    def _append_event_conn(
        conn: sqlite3.Connection,
        *,
        event_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_at: float,
    ) -> str:
        encoded = json.dumps(payload, sort_keys=True)
        prior = conn.execute(
            "SELECT run_id, event_type, payload_json FROM events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if prior is not None:
            if (prior["run_id"], prior["event_type"], prior["payload_json"]) != (run_id, event_type, encoded):
                raise ValueError("event_id is already bound to different event content")
            return event_id
        conn.execute(
            "INSERT INTO events(event_id, run_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_id, run_id, event_type, encoded, created_at),
        )
        return event_id

    @staticmethod
    def _append_outbox_conn(
        conn: sqlite3.Connection,
        *,
        event_id: str,
        failure_case_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_at: float,
    ) -> str:
        encoded = json.dumps(payload, sort_keys=True)
        prior = conn.execute(
            "SELECT failure_case_id, event_type, payload_json FROM failure_outbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if prior is not None:
            if (prior["failure_case_id"], prior["event_type"], prior["payload_json"]) != (
                failure_case_id,
                event_type,
                encoded,
            ):
                raise ValueError("outbox event_id is already bound to different event content")
            return event_id
        conn.execute(
            "INSERT INTO failure_outbox(event_id, failure_case_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_id, failure_case_id, event_type, encoded, created_at),
        )
        return event_id

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        event_id: str | None = None,
    ) -> str:
        event_id = event_id or "evt-" + uuid.uuid4().hex
        with self._connect() as conn:
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type=event_type,
                payload=payload,
                created_at=time.time(),
            )
        return event_id

    def events(self, run_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM events WHERE run_id = ? ORDER BY sequence", (run_id,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]

    @staticmethod
    def _failure_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["signature"] = json.loads(value.pop("signature_json"))
        return value

    def latest_failure_signature(self, run_id: str) -> dict[str, Any] | None:
        receipt = self.latest_receipt(run_id)
        if receipt is None:
            return None
        try:
            payload = json.loads((self.root / receipt["receipt_ref"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        verification = payload.get("verification") if isinstance(payload, dict) else None
        diff = payload.get("diff") if isinstance(payload, dict) else None
        exit_code = verification.get("exit_code") if isinstance(verification, dict) else None
        diff_digest = diff.get("digest") if isinstance(diff, dict) else None
        if not isinstance(exit_code, int) or exit_code == 0 or not isinstance(diff_digest, str):
            return None
        return {"exit_code": exit_code, "diff_digest": diff_digest}

    def ensure_failure_case(
        self,
        run_id: str,
        *,
        signature: dict[str, Any],
        acceptance_argv: list[str],
        trigger: str,
        receipt_digest: str | None = None,
    ) -> dict[str, Any]:
        run = self.get_run(run_id)
        goal = run["goal"] if isinstance(run.get("goal"), dict) else {}
        goal_id = str(goal.get("goal_id") or run_id)
        acceptance_digest = self._stable_id("sha256:", acceptance_argv)
        identity = {
            "source_repo": run["source_repo"],
            "campaign_id": goal.get("campaign_id"),
            "goal_id": goal_id,
            "feature_contract": goal.get("feature_contract"),
            "base_revision": run["base_revision"],
            "acceptance_digest": acceptance_digest,
            "signature": signature,
        }
        failure_case_id = self._stable_id("fc-", identity)
        now = time.time()
        payload = {
            "schema": "loop-hybrid-failure-case-event/v1",
            "failure_case_id": failure_case_id,
            "run_id": run_id,
            "goal_id": goal_id,
            "state": "grill_required",
            "grill_generation": 1,
            "trigger": trigger,
            "signature": signature,
            "acceptance_digest": acceptance_digest,
            "base_revision": run["base_revision"],
            "receipt_digest": receipt_digest,
        }
        outbox_id = f"failure-case:{failure_case_id}:opened"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO failure_cases(failure_case_id, run_id, goal_id, signature_json, acceptance_digest, base_revision, state, grill_generation, claim_fence, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'grill_required', 1, 0, ?, ?)",
                    (
                        failure_case_id,
                        run_id,
                        goal_id,
                        json.dumps(signature, sort_keys=True),
                        acceptance_digest,
                        run["base_revision"],
                        now,
                        now,
                    ),
                )
                self._append_event_conn(
                    conn,
                    event_id=outbox_id,
                    run_id=run_id,
                    event_type="failure_case_opened",
                    payload=payload,
                    created_at=now,
                )
                self._append_outbox_conn(
                    conn,
                    event_id=outbox_id,
                    failure_case_id=failure_case_id,
                    event_type="failure_case_opened",
                    payload=payload,
                    created_at=now,
                )
            conn.execute("COMMIT")
        case = self.failure_case(failure_case_id)
        case["created"] = existing is None
        return case

    def failure_case(self, failure_case_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
        value = self._failure_row(row)
        if value is None:
            raise KeyError(f"unknown failure_case_id: {failure_case_id}")
        return value

    def failure_case_for_run(
        self,
        run_id: str,
        *,
        states: set[str] | None = None,
    ) -> dict[str, Any] | None:
        query = "SELECT * FROM failure_cases WHERE run_id = ?"
        values: list[Any] = [run_id]
        if states:
            ordered = sorted(states)
            query += " AND state IN (" + ",".join("?" for _ in ordered) + ")"
            values.extend(ordered)
        query += " ORDER BY updated_at DESC, failure_case_id DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(query, values).fetchone()
        return self._failure_row(row)

    def claim_grill(
        self,
        failure_case_id: str,
        *,
        holder: str,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] == "grill_claimed" and float(row["claim_expires_at"] or 0) > now:
                conn.execute("COMMIT")
                return {
                    "status": "busy",
                    "failure_case_id": failure_case_id,
                    "generation": int(row["grill_generation"]),
                    "fence": int(row["claim_fence"]),
                }
            if row["state"] not in {"grill_required", "grill_claimed"}:
                conn.execute("COMMIT")
                return {
                    "status": "not_claimable",
                    "failure_case_id": failure_case_id,
                    "state": row["state"],
                }
            fence = int(row["claim_fence"]) + 1
            generation = int(row["grill_generation"])
            updated = conn.execute(
                "UPDATE failure_cases SET state = 'grill_claimed', claim_fence = ?, claim_holder = ?, claim_expires_at = ?, updated_at = ? "
                "WHERE failure_case_id = ? AND claim_fence = ?",
                (fence, holder, now + lease_seconds, now, failure_case_id, int(row["claim_fence"])),
            ).rowcount
            if updated != 1:
                conn.execute("ROLLBACK")
                return {"status": "busy", "failure_case_id": failure_case_id}
            conn.execute("COMMIT")
        return {
            "status": "claimed",
            "failure_case_id": failure_case_id,
            "generation": generation,
            "fence": fence,
        }

    def record_grill_result(
        self,
        failure_case_id: str,
        *,
        generation: int,
        fence: int,
        decision: str,
        diagnosis: str,
    ) -> dict[str, Any]:
        now = time.time()
        plan_digest = self._stable_id("sha256:", {"decision": decision, "diagnosis": diagnosis})
        state = "remediation_running" if decision == "runner-fixable" else "human_required"
        event_id = f"failure-case:{failure_case_id}:grill:{generation}:result"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] != "grill_claimed" or int(row["grill_generation"]) != generation or int(row["claim_fence"]) != fence:
                prior = conn.execute(
                    "SELECT payload_json FROM failure_outbox WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                conn.execute("COMMIT")
                if prior is not None:
                    return {**json.loads(prior["payload_json"]), "status": "reused"}
                return {"status": "fence_rejected", "failure_case_id": failure_case_id}
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": failure_case_id,
                "run_id": row["run_id"],
                "goal_id": row["goal_id"],
                "event": "grill_result",
                "state": state,
                "grill_generation": generation,
                "grill_fence": fence,
                "decision": decision,
                "diagnosis": diagnosis,
                "plan_digest": plan_digest,
            }
            run_attempts = conn.execute(
                "SELECT attempts FROM runs WHERE run_id = ?",
                (row["run_id"],),
            ).fetchone()
            conn.execute(
                "UPDATE failure_cases SET state = ?, plan_digest = ?, claim_holder = NULL, claim_expires_at = NULL, updated_at = ? "
                "WHERE failure_case_id = ?",
                (state, plan_digest, now, failure_case_id),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=row["run_id"],
                event_type="grill_decision",
                payload={
                    "failure_case_id": failure_case_id,
                    "decision": decision,
                    "diagnosis": diagnosis,
                    "attempts_used": int(run_attempts["attempts"]) if run_attempts is not None else 0,
                    "grill_generation": generation,
                    "plan_digest": plan_digest,
                },
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=failure_case_id,
                event_type="grill_result",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return {**payload, "status": "recorded"}

    def record_grill_failure(
        self,
        failure_case_id: str,
        *,
        generation: int,
        fence: int,
        reason: str,
    ) -> dict[str, Any]:
        now = time.time()
        event_id = f"failure-case:{failure_case_id}:grill:{generation}:failed"
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE failure_case_id = ?",
                (failure_case_id,),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown failure_case_id: {failure_case_id}")
            if row["state"] != "grill_claimed" or int(row["grill_generation"]) != generation or int(row["claim_fence"]) != fence:
                prior = conn.execute("SELECT payload_json FROM failure_outbox WHERE event_id = ?", (event_id,)).fetchone()
                conn.execute("COMMIT")
                if prior is not None:
                    return {**json.loads(prior["payload_json"]), "status": "reused"}
                return {"status": "fence_rejected", "failure_case_id": failure_case_id}
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": failure_case_id,
                "run_id": row["run_id"],
                "goal_id": row["goal_id"],
                "event": "grill_failed",
                "state": "human_required",
                "grill_generation": generation,
                "grill_fence": fence,
                "reason": reason[:500],
            }
            conn.execute(
                "UPDATE failure_cases SET state = 'human_required', claim_holder = NULL, claim_expires_at = NULL, updated_at = ? WHERE failure_case_id = ?",
                (now, failure_case_id),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=row["run_id"],
                event_type="grill_failed",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=failure_case_id,
                event_type="grill_failed",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return {**payload, "status": "recorded"}

    def record_resolution(
        self,
        run_id: str,
        *,
        ordinal: int,
        receipt_digest: str,
        exit_code: int,
        machine_resolved: bool | None = None,
        allow_regrill: bool = True,
        checker_reason: str | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE run_id = ? AND state = 'remediation_running' ORDER BY updated_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            run = conn.execute("SELECT max_attempts FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            resolved = exit_code == 0 if machine_resolved is None else machine_resolved
            has_budget = allow_regrill and run is not None and ordinal < int(run["max_attempts"])
            if resolved:
                state = "resolved"
            elif has_budget:
                state = "grill_required"
            else:
                state = "human_required"
            next_generation = int(row["grill_generation"]) if resolved or not has_budget else int(row["grill_generation"]) + 1
            event_id = f"failure-case:{row['failure_case_id']}:check:{ordinal}:{receipt_digest}"
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": row["failure_case_id"],
                "run_id": run_id,
                "goal_id": row["goal_id"],
                "event": "resolution_checked",
                "state": state,
                "machine_resolved": resolved,
                "checker": "approved_deterministic_verifier_and_value_gate",
                "checker_reason": checker_reason,
                "verifier_exit_code": exit_code,
                "attempt": ordinal,
                "receipt_digest": receipt_digest,
                "grill_generation": int(row["grill_generation"]),
                "next_grill_generation": next_generation if not resolved and has_budget else None,
                "regrill_required": not resolved and has_budget,
            }
            conn.execute(
                "UPDATE failure_cases SET state = ?, grill_generation = ?, latest_check_receipt_digest = ?, plan_digest = CASE WHEN ? THEN NULL ELSE plan_digest END, "
                "claim_holder = NULL, claim_expires_at = NULL, updated_at = ? WHERE failure_case_id = ?",
                (
                    state,
                    next_generation,
                    receipt_digest,
                    1 if not resolved and has_budget else 0,
                    now,
                    row["failure_case_id"],
                ),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type="resolution_checked",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=row["failure_case_id"],
                event_type="resolution_checked",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return payload

    def record_next_node(self, run_id: str, *, next_node_id: str | None) -> dict[str, Any] | None:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM failure_cases WHERE run_id = ? AND state = 'resolved' ORDER BY updated_at DESC LIMIT 1",
                (run_id,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            receipt_digest = row["latest_check_receipt_digest"]
            event_id = f"failure-case:{row['failure_case_id']}:closed:{receipt_digest}"
            payload = {
                "schema": "loop-hybrid-failure-case-event/v1",
                "failure_case_id": row["failure_case_id"],
                "run_id": run_id,
                "goal_id": row["goal_id"],
                "event": "failure_case_closed",
                "state": "resolved",
                "machine_resolved": True,
                "checker_receipt_digest": receipt_digest,
                "next_node_id": next_node_id,
            }
            conn.execute(
                "UPDATE failure_cases SET next_node_id = ?, updated_at = ? WHERE failure_case_id = ?",
                (next_node_id, now, row["failure_case_id"]),
            )
            self._append_event_conn(
                conn,
                event_id=event_id,
                run_id=run_id,
                event_type="failure_case_closed",
                payload=payload,
                created_at=now,
            )
            self._append_outbox_conn(
                conn,
                event_id=event_id,
                failure_case_id=row["failure_case_id"],
                event_type="failure_case_closed",
                payload=payload,
                created_at=now,
            )
            conn.execute("COMMIT")
        return payload

    def failure_outbox(self, *, after_sequence: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM failure_outbox WHERE sequence > ? ORDER BY sequence",
                (int(after_sequence),),
            ).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]

    def summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            rows = conn.execute("SELECT state, COUNT(*) AS count FROM runs GROUP BY state ORDER BY state").fetchall()
            events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {"runs_by_state": {row["state"]: row["count"] for row in rows}, "event_count": events}

    def runnable_runs(self) -> list[dict[str, Any]]:
        """Return queued/retry-pending runs in deterministic creation order."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs WHERE state IN ('queued', 'retry_pending') ORDER BY created_at, run_id").fetchall()
        return [self._row(row) for row in rows]

    def terminal_runs(self) -> list[dict[str, Any]]:
        """Return runs whose result still needs Goal lifecycle reduction."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE state IN ('verified', 'stopped', 'human_required') ORDER BY updated_at, run_id"
            ).fetchall()
        return [self._row(row) for row in rows]

    def latest_receipt(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT ordinal, receipt_ref, receipt_digest, delivery_source_ref, delivery_source_digest, delivery_final_ref, delivery_final_digest "
                "FROM attempts WHERE run_id = ? AND receipt_ref IS NOT NULL "
                "ORDER BY ordinal DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return None if row is None else dict(row)

    def latest_receipt_projection(self) -> dict[str, Any] | None:
        """Return the newest receipt pointer without exposing artifact content.

        This is the read-only bridge used by status snapshots.  Receipt bytes,
        provider output, and verifier stdout/stderr remain in the RunStore
        artifact boundary; callers receive only durable identity and digest.
        """
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT r.run_id, r.goal_json, r.state AS run_state, r.updated_at,
                       a.ordinal, a.state AS attempt_state, a.workspace_ref,
                       a.receipt_ref, a.receipt_digest, a.delivery_source_ref, a.delivery_source_digest,
                       a.delivery_final_ref, a.delivery_final_digest, a.finished_at,
                       (SELECT COUNT(*) FROM attempts WHERE receipt_ref IS NOT NULL) AS receipt_count
                FROM runs r
                JOIN attempts a ON a.run_id = r.run_id
                WHERE a.receipt_ref IS NOT NULL
                  AND a.ordinal = (
                      SELECT MAX(a2.ordinal) FROM attempts a2
                      WHERE a2.run_id = r.run_id AND a2.receipt_ref IS NOT NULL
                  )
                ORDER BY r.updated_at DESC, a.ordinal DESC, r.run_id DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        try:
            goal = json.loads(row["goal_json"])
        except (TypeError, json.JSONDecodeError):
            goal = {}
        return {
            "schema": "loop-hybrid-receipt-projection/v1",
            "run_id": row["run_id"],
            "goal_id": goal.get("goal_id") if isinstance(goal, dict) else None,
            "run_state": row["run_state"],
            "attempt": row["ordinal"],
            "attempt_state": row["attempt_state"],
            "workspace_ref": row["workspace_ref"],
            "receipt": {"ref": row["receipt_ref"], "digest": row["receipt_digest"]},
            "delivery": {
                "source_ref": row["delivery_source_ref"],
                "source_digest": row["delivery_source_digest"],
                "final_ref": row["delivery_final_ref"],
                "final_digest": row["delivery_final_digest"],
            },
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
            "receipt_count": row["receipt_count"],
        }

    def latest_attempt(self, run_id: str) -> dict[str, Any] | None:
        """Read the newest attempt metadata without exposing the SQLite store.

        Command/report adapters use this projection to correlate a goal with
        the durable run/attempt boundary.  It is intentionally read-only;
        attempt ownership, fencing, and receipt writes remain RunStore's
        responsibility.
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT run_id, ordinal, state, workspace_ref, fence, receipt_ref, receipt_digest, delivery_source_ref, delivery_source_digest, delivery_final_ref, delivery_final_digest, created_at, finished_at "
                "FROM attempts WHERE run_id = ? ORDER BY ordinal DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        return None if row is None else dict(row)

    def usage_corrections(self) -> list[dict[str, Any]]:
        """List the human-recorded usage voids (W9e), oldest first."""
        with self._connect() as conn:
            rows = conn.execute("SELECT run_id, ordinal, reason, created_at FROM usage_corrections ORDER BY created_at, run_id, ordinal").fetchall()
        return [dict(row) for row in rows]

    def void_usage(self, run_id: str, ordinal: int, *, reason: str) -> dict[str, Any]:
        """W9e: human-only void of a poisoned usage record.

        Appends a correction row beside the evidence — the original receipt
        and its recorded digest are never modified. Voiding a record that
        does not exist is a clear error; a duplicate void reports
        already-voided instead of writing a second row. Nothing in the
        engine calls this automatically."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        with self._connect() as conn:
            attempt = conn.execute("SELECT receipt_ref FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, int(ordinal))).fetchone()
            if attempt is None or attempt["receipt_ref"] is None:
                raise ValueError(f"no recorded usage for run {run_id} attempt {ordinal}")
            existing = conn.execute("SELECT reason, created_at FROM usage_corrections WHERE run_id = ? AND ordinal = ?", (run_id, int(ordinal))).fetchone()
            if existing is not None:
                return {"status": "already_voided", "run_id": run_id, "ordinal": int(ordinal), "reason": existing["reason"], "created_at": existing["created_at"]}
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO usage_corrections(run_id, ordinal, reason, created_at) VALUES (?, ?, ?, ?)", (run_id, int(ordinal), reason.strip(), now))
            conn.execute("COMMIT")
        return {"status": "voided", "run_id": run_id, "ordinal": int(ordinal), "reason": reason.strip(), "created_at": now}

    def usage_records(self) -> list[dict[str, Any]]:
        """Read the per-attempt token usage stamped into each committed receipt.

        A missing or unreadable usage block is reported as ``unknown`` — it is
        never silently treated as zero usage. Records a human voided through
        ``void_usage`` (W9e, e.g. a phantom-attribution record) are excluded;
        the correction lives beside the receipt, never inside it."""
        with self._connect() as conn:
            rows = conn.execute("SELECT run_id, ordinal, state, created_at, finished_at, receipt_ref FROM attempts WHERE receipt_ref IS NOT NULL ORDER BY run_id, ordinal").fetchall()
            voided = {(row["run_id"], row["ordinal"]) for row in conn.execute("SELECT run_id, ordinal FROM usage_corrections")}
        records: list[dict[str, Any]] = []
        for row in rows:
            if (row["run_id"], row["ordinal"]) in voided:
                continue
            usage: dict[str, Any] | None = None
            try:
                receipt = json.loads((self.root / row["receipt_ref"]).read_text(encoding="utf-8"))
                candidate = receipt.get("usage")
                if isinstance(candidate, dict):
                    usage = candidate
            except (OSError, json.JSONDecodeError):
                usage = None
            if usage is None:
                usage = {"state": "unknown", "reason": "receipt has no readable usage"}
            elapsed = None
            if row["created_at"] is not None and row["finished_at"] is not None:
                elapsed = round(float(row["finished_at"]) - float(row["created_at"]), 3)
            records.append({
                "run_id": row["run_id"], "attempt": row["ordinal"], "attempt_state": row["state"],
                "created_at": row["created_at"], "finished_at": row["finished_at"], "elapsed_seconds": elapsed,
                **usage,
            })
        return records

    @staticmethod
    def _process_holder(holder: str) -> str:
        label = str(holder or "worker")
        return f"{label}:pid={os.getpid()}:start={PROCESS_START_NS}:nonce={PROCESS_NONCE}"

    def _record_fence_rejection(self, run_id: str, *, operation: str, ordinal: int | None = None, supplied_fence: int | None = None, current_fence: int | None = None) -> None:
        self.append_event(
            run_id,
            "fence_rejected",
            {
                "operation": operation,
                "attempt": ordinal,
                "supplied_fence": supplied_fence,
                "current_fence": current_fence,
            },
        )

    def acquire_lease(self, run_id: str, holder: str, seconds: int = 60) -> bool:
        effective_holder = self._process_holder(holder)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT holder, expires_at FROM leases WHERE run_id = ?", (run_id,)).fetchone()
            if row is not None and row["expires_at"] > now and row["holder"] != effective_holder:
                conn.execute("COMMIT")
                return False
            conn.execute("INSERT OR REPLACE INTO leases VALUES (?, ?, ?)", (run_id, effective_holder, now + seconds))
            conn.execute("COMMIT")
        return True

    def release_lease(self, run_id: str, holder: str) -> None:
        effective_holder = self._process_holder(holder)
        with self._connect() as conn:
            conn.execute("DELETE FROM leases WHERE run_id = ? AND holder = ?", (run_id, effective_holder))

    def _reconcile_expired_run(self, run_id: str) -> dict[str, Any] | None:
        """Recover one expired running attempt without re-invoking a model."""
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute(
                "SELECT state, fence, goal_json, max_attempts, delivery_attempt_ceiling, delivery_contract_json, delivery_final_evidence_json FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            lease = conn.execute("SELECT expires_at FROM leases WHERE run_id = ?", (run_id,)).fetchone()
            if run is None or run["state"] != "running" or (lease is not None and lease["expires_at"] > now):
                conn.execute("COMMIT")
                return None
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id = ? AND state = 'running' ORDER BY ordinal DESC LIMIT 1", (run_id,)).fetchone()
            if attempt is None:
                conn.execute("COMMIT")
                return None
            current_fence = int(run["fence"])
            if int(attempt["fence"]) != current_fence:
                conn.execute("COMMIT")
                self._record_fence_rejection(
                    run_id,
                    operation="reconcile",
                    ordinal=int(attempt["ordinal"]),
                    supplied_fence=int(attempt["fence"]),
                    current_fence=current_fence,
                )
                return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
            receipt_path = self.artifacts / run_id / str(attempt["ordinal"]) / "receipt.json"
            receipt_ref = receipt_digest = None
            exit_code: int | None = None
            try:
                receipt_raw = receipt_path.read_text(encoding="utf-8")
                receipt = json.loads(receipt_raw)
                if receipt.get("schema") == "loop-hybrid-attempt-receipt/v1" and receipt.get("run_id") == run_id and receipt.get("attempt") == attempt["ordinal"] and isinstance(receipt.get("verification", {}).get("exit_code"), int):
                    import hashlib
                    receipt_ref = receipt_path.relative_to(self.root).as_posix()
                    receipt_digest = "sha256:" + hashlib.sha256(receipt_raw.encode()).hexdigest()
                    exit_code = receipt["verification"]["exit_code"]
            except (OSError, json.JSONDecodeError):
                pass
            if exit_code is not None:
                max_attempts = self._effective_max_attempts_from_row(run)
                delivery_bound = bool(run["delivery_contract_json"])
                if not delivery_bound:
                    try:
                        json.loads(run["goal_json"])
                        delivery_bound = True
                    except (TypeError, json.JSONDecodeError):
                        delivery_bound = False
                # Reconciliation runs only on a live ``running`` row.  A
                # receipt or non-empty final projection cannot substitute for
                # the atomic terminal commit; terminal readback is handled by
                # ``finish_attempt_with_delivery`` on the normal path.
                recovery_reason = None
                state = (
                    "human_required"
                    if exit_code == 0 and delivery_bound
                    else "human_required"
                    if exit_code == 0
                    else "stopped"
                    if attempt["ordinal"] >= max_attempts
                    else "retry_pending"
                )
                if exit_code == 0:
                    recovery_reason = "delivery_terminal_commit_missing" if delivery_bound else "delivery_binding_missing"
                next_fence = current_fence + 1
                updated_attempt = conn.execute("UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (state, receipt_ref, receipt_digest, now, run_id, attempt["ordinal"], current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = ?, fence = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (state, next_fence, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    self._record_fence_rejection(run_id, operation="reconcile", ordinal=int(attempt["ordinal"]), supplied_fence=current_fence, current_fence=current_fence)
                    return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
                result = {"run_id": run_id, "attempt": attempt["ordinal"], "status": state, "recovered_from": "receipt"}
                if recovery_reason is not None:
                    result["reason"] = recovery_reason
            else:
                next_fence = current_fence + 1
                updated_attempt = conn.execute("UPDATE attempts SET state = 'interrupted', finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (now, run_id, attempt["ordinal"], current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = 'retry_pending', fence = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (next_fence, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    self._record_fence_rejection(run_id, operation="reconcile", ordinal=int(attempt["ordinal"]), supplied_fence=current_fence, current_fence=current_fence)
                    return {"run_id": run_id, "attempt": attempt["ordinal"], "status": "fence_rejected", "recovered_from": "reconcile"}
                result = {"run_id": run_id, "attempt": attempt["ordinal"], "status": "retry_pending", "recovered_from": "interrupted"}
            conn.execute("DELETE FROM leases WHERE run_id = ?", (run_id,))
            conn.execute("COMMIT")
        self.append_event(run_id, "attempt_reconciled", result)
        return result

    def recover_stale_run(self, run_id: str) -> bool:
        """Compatibility wrapper for a controller that wakes one run."""
        return self._reconcile_expired_run(run_id) is not None

    def reconcile_startup(self) -> list[dict[str, Any]]:
        """Scan every running run after process start and recover expired leases."""
        with self._connect() as conn:
            run_ids = [row["run_id"] for row in conn.execute("SELECT run_id FROM runs WHERE state = 'running'")]
        return [result for run_id in run_ids if (result := self._reconcile_expired_run(run_id)) is not None]

    def begin_attempt(self, run_id: str, workspace_ref: str) -> int:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT attempts, state, fence, max_attempts, delivery_attempt_ceiling, delivery_contract_json FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if run is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown run_id: {run_id}")
            if run["state"] not in {"queued", "retry_pending"}:
                conn.execute("ROLLBACK")
                raise ValueError(f"run is not executable from {run['state']}")
            ordinal = run["attempts"] + 1
            effective_max = self._effective_max_attempts_from_row(run)
            if ordinal > effective_max:
                conn.execute(
                    "UPDATE runs SET state = 'human_required', updated_at = ? WHERE run_id = ? AND state IN ('queued', 'retry_pending')",
                    (now, run_id),
                )
                conn.execute("COMMIT")
                self.append_event(
                    run_id,
                    "repair_attempt_budget_exhausted",
                    {"attempt": ordinal, "effective_max_attempts": effective_max},
                )
                raise ValueError("repair_attempt_budget_exhausted")
            fence = int(run["fence"]) + 1
            conn.execute("UPDATE runs SET attempts = ?, fence = ?, state = 'running', updated_at = ? WHERE run_id = ?", (ordinal, fence, now, run_id))
            conn.execute(
                "INSERT INTO attempts(run_id, ordinal, state, workspace_ref, fence, receipt_ref, receipt_digest, created_at, finished_at) VALUES (?, ?, 'running', ?, ?, NULL, NULL, ?, NULL)",
                (run_id, ordinal, workspace_ref, fence, now),
            )
            conn.execute("COMMIT")
        self.append_event(run_id, "attempt_started", {"attempt": ordinal, "workspace_ref": workspace_ref, "fence": fence})
        return ordinal

    def attempt_fence(self, run_id: str, ordinal: int) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
        if row is None:
            raise KeyError(f"unknown attempt: {run_id}/{ordinal}")
        return int(row["fence"])

    def finish_attempt(self, run_id: str, ordinal: int, *, state: str, receipt_ref: str, receipt_digest: str, fence: int | None = None) -> bool:
        if state not in RUN_STATES - {"running", "queued"}:
            raise ValueError(f"invalid terminal attempt state: {state}")
        self.delivery_required(run_id)
        if not self._receipt_artifact_valid(run_id, ordinal, receipt_ref, receipt_digest):
            self.append_event(
                run_id,
                "delivery_terminal_rejected",
                {"attempt": ordinal, "state": state, "reason": "attempt_receipt_missing_or_digest_mismatch"},
            )
            return False
        if state == "verified":
            delivery = self.verify_delivery(run_id, phase="final", ordinal=ordinal)
            if delivery.get("verdict") != "GREEN":
                self.append_event(
                    run_id,
                    "delivery_terminal_rejected",
                    {"attempt": ordinal, "state": state, "reason": delivery.get("reason", "delivery_final_not_green"), "delivery": delivery},
                )
                return False
        now = time.time()
        rejection: dict[str, Any] | None = None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            current_fence = int(run["fence"]) if run is not None else None
            attempt_fence = int(attempt["fence"]) if attempt is not None else None
            expected_fence = attempt_fence if fence is None else fence
            if (
                run is None
                or attempt is None
                or attempt["state"] != "running"
                or attempt_fence != current_fence
                or expected_fence != current_fence
            ):
                rejection = {"operation": "finish", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                conn.execute("COMMIT")
            else:
                updated_attempt = conn.execute("UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (state, receipt_ref, receipt_digest, now, run_id, ordinal, current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (state, now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    rejection = {"operation": "finish", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                else:
                    conn.execute("COMMIT")
        if rejection is not None:
            self._record_fence_rejection(run_id, **rejection)
            return False
        self.append_event(run_id, "attempt_finished", {"attempt": ordinal, "state": state, "receipt_ref": receipt_ref})
        return True

    def finish_attempt_with_delivery_result(
        self,
        run_id: str,
        ordinal: int,
        *,
        state: str,
        receipt_ref: str,
        receipt_digest: str,
        evidence: dict[str, Any],
        fence: int | None = None,
    ) -> dict[str, Any]:
        """Atomically promote a Run with its final delivery evidence.

        Final evidence is assembled before this call but is not durable until
        the attempt and Run transition commit together.  Validation uses a
        future-state snapshot, then the SQL CAS re-reads the live fence/attempt
        and stores the final evidence with the terminal update.  A later
        readback therefore observes the actual Run state, never a caller's
        claimed ``state_readback``.
        """
        def outcome(ok: bool, kind: str, reason: str | None = None, **extra: Any) -> dict[str, Any]:
            result: dict[str, Any] = {
                "ok": ok,
                "kind": kind,
                "run_id": run_id,
                "attempt": ordinal,
                "fence": fence,
            }
            if reason is not None:
                result["reason"] = reason
            result.update(extra)
            return result

        if state not in {"verified", "integrated"}:
            raise ValueError("delivery terminal state must be verified or integrated")
        if not self._receipt_artifact_valid(run_id, ordinal, receipt_ref, receipt_digest):
            self.append_event(
                run_id,
                "delivery_terminal_rejected",
                {"attempt": ordinal, "state": state, "reason": "attempt_receipt_missing_or_digest_mismatch"},
            )
            return outcome(False, "delivery_rejected", "attempt_receipt_missing_or_digest_mismatch")
        if not isinstance(evidence, dict):
            return outcome(False, "delivery_rejected", "delivery_evidence_missing")
        run = self.get_run(run_id)
        current = self.delivery_binding(run_id)
        if current.get("verdict") != "GREEN":
            return outcome(False, "delivery_rejected", str(current.get("reason") or "delivery_binding_invalid"), delivery=current)
        latest = self._delivery_evidence_row(run_id, ordinal)
        if latest is None:
            return outcome(False, "delivery_rejected", "delivery_attempt_missing")
        current_fence = int(run.get("fence", 0))
        expected_fence = int(latest.get("fence", 0)) if fence is None else fence
        if (
            latest.get("state") != "running"
            or int(latest.get("fence", 0)) != current_fence
            or expected_fence != current_fence
            or run.get("state") != "running"
        ):
            self._record_fence_rejection(
                run_id,
                operation="finish_with_delivery",
                ordinal=ordinal,
                supplied_fence=expected_fence,
                current_fence=current_fence,
            )
            return outcome(False, "fence_rejected", "fence_rejected", current_fence=current_fence)
        readback = evidence.get("state_readback")
        if (
            not isinstance(readback, dict)
            or readback.get("authority_store") != "run"
            or readback.get("run_id") not in (None, run_id)
            or readback.get("run_state") != state
            or readback.get("terminal_state") != state
        ):
            self.append_event(
                run_id,
                "delivery_terminal_rejected",
                {"attempt": ordinal, "state": state, "reason": "delivery_state_readback_not_terminal"},
            )
            return outcome(False, "delivery_rejected", "delivery_state_readback_not_terminal")
        future = dict(run)
        future["state"] = state
        verification = self._verify_delivery_evidence(future, evidence, phase="final")
        if verification.get("verdict") != "GREEN":
            self.append_event(
                run_id,
                "delivery_terminal_rejected",
                {"attempt": ordinal, "state": state, "reason": verification.get("reason", "delivery_final_not_green"), "delivery": verification},
            )
            return outcome(False, "delivery_rejected", str(verification.get("reason", "delivery_final_not_green")), delivery=verification)
        raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        artifact = self.write_artifact(run_id, ordinal, "delivery-final.json", raw)
        now = time.time()
        rejection: dict[str, Any] | None = None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            live_fence = int(row["fence"]) if row is not None else None
            if (
                row is None
                or attempt is None
                or row["state"] != "running"
                or attempt["state"] != "running"
                or int(attempt["fence"]) != live_fence
                or live_fence != current_fence
                or expected_fence != live_fence
            ):
                conn.execute("ROLLBACK")
                rejection = {"operation": "finish_with_delivery", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": live_fence}
            else:
                updated_attempt = conn.execute(
                    "UPDATE attempts SET state = ?, receipt_ref = ?, receipt_digest = ?, delivery_final_ref = ?, delivery_final_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?",
                    (state, receipt_ref, receipt_digest, artifact["ref"], artifact["digest"], now, run_id, ordinal, live_fence),
                ).rowcount
                updated_run = conn.execute(
                    "UPDATE runs SET state = ?, delivery_final_evidence_json = ?, delivery_final_digest = ?, delivery_terminal_state = ?, updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?",
                    (state, raw, artifact["digest"], state, now, run_id, live_fence),
                ).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    rejection = {"operation": "finish_with_delivery", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": live_fence}
                else:
                    conn.execute("COMMIT")
        if rejection is not None:
            self._record_fence_rejection(run_id, **rejection)
            return outcome(False, "fence_rejected", "fence_rejected", current_fence=rejection.get("current_fence"))
        final = self.verify_delivery(run_id, phase="final", ordinal=ordinal)
        if final.get("verdict") != "GREEN":
            self.append_event(run_id, "delivery_terminal_rejected", {"attempt": ordinal, "state": state, "reason": "delivery_final_readback_failed", "delivery": final})
            return outcome(
                False,
                "delivery_rejected",
                "delivery_final_readback_failed",
                delivery=final,
                committed=True,
            )
        self.append_event(
            run_id,
            "attempt_finished",
            {"attempt": ordinal, "state": state, "receipt_ref": receipt_ref, "delivery_final_ref": artifact["ref"]},
        )
        return outcome(True, "finished")

    def finish_attempt_with_delivery(
        self,
        run_id: str,
        ordinal: int,
        *,
        state: str,
        receipt_ref: str,
        receipt_digest: str,
        evidence: dict[str, Any],
        fence: int | None = None,
    ) -> bool:
        """Bool compatibility wrapper for the typed terminal operation."""
        result = self.finish_attempt_with_delivery_result(
            run_id,
            ordinal,
            state=state,
            receipt_ref=receipt_ref,
            receipt_digest=receipt_digest,
            evidence=evidence,
            fence=fence,
        )
        return result.get("ok") is True

    def park_external_verdict(self, run_id: str, ordinal: int, *, receipt_ref: str, receipt_digest: str, fence: int | None = None) -> bool:
        """Park a run awaiting an async external verdict (Method A). Not a crash, not finished:
        the attempt stays open (finished_at NULL) and reconcile ignores it (state != running)."""
        self.delivery_required(run_id)
        if not self._receipt_artifact_valid(run_id, ordinal, receipt_ref, receipt_digest):
            self.append_event(run_id, "delivery_external_rejected", {"attempt": ordinal, "reason": "attempt_receipt_missing_or_digest_mismatch"})
            return False
        source = self.verify_delivery(run_id, phase="source", ordinal=ordinal)
        if source.get("verdict") != "GREEN":
            self.append_event(run_id, "delivery_external_rejected", {"attempt": ordinal, "reason": source.get("reason", "delivery_source_not_green"), "delivery": source})
            return False
        now = time.time()
        rejection: dict[str, Any] | None = None
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            current_fence = int(run["fence"]) if run is not None else None
            attempt_fence = int(attempt["fence"]) if attempt is not None else None
            expected_fence = attempt_fence if fence is None else fence
            if (
                run is None
                or attempt is None
                or run["state"] != "running"
                or attempt["state"] != "running"
                or attempt_fence != current_fence
                or expected_fence != current_fence
            ):
                rejection = {"operation": "park_external_verdict", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                conn.execute("COMMIT")
            else:
                updated_attempt = conn.execute("UPDATE attempts SET state = 'awaiting_external_verdict', receipt_ref = ?, receipt_digest = ? WHERE run_id = ? AND ordinal = ? AND state = 'running' AND fence = ?", (receipt_ref, receipt_digest, run_id, ordinal, current_fence)).rowcount
                updated_run = conn.execute("UPDATE runs SET state = 'awaiting_external_verdict', updated_at = ? WHERE run_id = ? AND state = 'running' AND fence = ?", (now, run_id, current_fence)).rowcount
                if updated_attempt != 1 or updated_run != 1:
                    conn.execute("ROLLBACK")
                    rejection = {"operation": "park_external_verdict", "ordinal": ordinal, "supplied_fence": expected_fence, "current_fence": current_fence}
                else:
                    conn.execute("COMMIT")
        if rejection is not None:
            self._record_fence_rejection(run_id, **rejection)
            return False
        self.append_event(run_id, "external_verdict_parked", {"attempt": ordinal})
        return True

    def resolve_external_verdict(self, run_id: str, new_state: str) -> None:
        """Finalize a parked run once its external verdict lands.

        External CI is only one final-phase input.  A successful conclusion
        cannot promote the Run unless the RunStore has its own complete final
        delivery evidence; the durable manual-review state preserves that
        decision rather than silently looping on the same CI result.
        """
        if new_state not in {"verified", "retry_pending", "stopped", "human_required"}:
            raise ValueError(f"invalid external verdict state: {new_state}")
        if new_state == "verified":
            final = self.verify_delivery(run_id, phase="final")
            if final.get("verdict") != "GREEN":
                self.append_event(run_id, "delivery_terminal_rejected", {"state": new_state, "reason": final.get("reason", "delivery_final_not_green"), "delivery": final})
                raise ValueError(f"delivery final gate rejected: {final.get('reason', 'RED')}")
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if run is None or run["state"] != "awaiting_external_verdict":
                conn.execute("ROLLBACK")
                raise ValueError("run is not awaiting an external verdict")
            conn.execute("UPDATE attempts SET state = ?, finished_at = ? WHERE run_id = ? AND state = 'awaiting_external_verdict'", (new_state, now, run_id))
            conn.execute("UPDATE runs SET state = ?, updated_at = ? WHERE run_id = ?", (new_state, now, run_id))
            conn.execute("COMMIT")
        self.append_event(run_id, "external_verdict_resolved", {"state": new_state})

    def resolve_external_verdict_with_delivery(
        self,
        run_id: str,
        *,
        final_evidence: dict[str, Any],
        new_state: str = "verified",
    ) -> bool:
        """Close an external run with CI/source evidence in one transition.

        CI's normalized conclusion is an input, not the Run's terminal proof.
        This method validates the assembled final package against a future
        ``verified`` snapshot, then atomically stores the package and changes
        the parked Run/Attempt.  A subsequent readback validates the actual
        persisted Run state.
        """
        if new_state not in {"verified", "integrated"}:
            raise ValueError("delivery external terminal state must be verified or integrated")
        if not isinstance(final_evidence, dict):
            return False
        run = self.get_run(run_id)
        if run.get("state") != "awaiting_external_verdict":
            return False
        latest = self.latest_attempt(run_id)
        if not isinstance(latest, dict) or latest.get("state") != "awaiting_external_verdict":
            return False
        ordinal = int(latest["ordinal"])
        fence = int(latest["fence"])
        readback = final_evidence.get("state_readback")
        if (
            not isinstance(readback, dict)
            or readback.get("authority_store") != "run"
            or readback.get("run_id") not in (None, run_id)
            or readback.get("run_state") != new_state
            or readback.get("terminal_state") != new_state
        ):
            self.append_event(run_id, "delivery_terminal_rejected", {"state": new_state, "reason": "delivery_state_readback_not_terminal"})
            return False
        future = dict(run)
        future["state"] = new_state
        verification = self._verify_delivery_evidence(future, final_evidence, phase="final")
        if verification.get("verdict") != "GREEN":
            self.append_event(run_id, "delivery_terminal_rejected", {"state": new_state, "reason": verification.get("reason", "delivery_final_not_green"), "delivery": verification})
            return False
        raw = json.dumps(final_evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        artifact = self.write_artifact(run_id, ordinal, "delivery-final.json", raw)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT state, fence FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            attempt = conn.execute("SELECT state, fence FROM attempts WHERE run_id = ? AND ordinal = ?", (run_id, ordinal)).fetchone()
            live_fence = int(row["fence"]) if row is not None else None
            if (
                row is None
                or attempt is None
                or row["state"] != "awaiting_external_verdict"
                or attempt["state"] != "awaiting_external_verdict"
                or int(attempt["fence"]) != live_fence
                or live_fence != fence
            ):
                conn.execute("ROLLBACK")
                self._record_fence_rejection(run_id, operation="resolve_external_with_delivery", ordinal=ordinal, supplied_fence=fence, current_fence=live_fence)
                return False
            updated_attempt = conn.execute(
                "UPDATE attempts SET state = ?, delivery_final_ref = ?, delivery_final_digest = ?, finished_at = ? WHERE run_id = ? AND ordinal = ? AND state = 'awaiting_external_verdict' AND fence = ?",
                (new_state, artifact["ref"], artifact["digest"], now, run_id, ordinal, live_fence),
            ).rowcount
            updated_run = conn.execute(
                "UPDATE runs SET state = ?, delivery_final_evidence_json = ?, delivery_final_digest = ?, delivery_terminal_state = ?, updated_at = ? WHERE run_id = ? AND state = 'awaiting_external_verdict' AND fence = ?",
                (new_state, raw, artifact["digest"], new_state, now, run_id, live_fence),
            ).rowcount
            if updated_attempt != 1 or updated_run != 1:
                conn.execute("ROLLBACK")
                self._record_fence_rejection(run_id, operation="resolve_external_with_delivery", ordinal=ordinal, supplied_fence=fence, current_fence=live_fence)
                return False
            conn.execute("COMMIT")
        final = self.verify_delivery(run_id, phase="final", ordinal=ordinal)
        if final.get("verdict") != "GREEN":
            self.append_event(run_id, "delivery_terminal_rejected", {"state": new_state, "reason": "delivery_final_readback_failed", "delivery": final})
            return False
        self.append_event(run_id, "external_verdict_resolved", {"state": new_state, "delivery_final_ref": artifact["ref"]})
        return True

    def write_artifact(self, run_id: str, ordinal: int, name: str, content: str) -> dict[str, str]:
        import hashlib

        path = self.artifacts / run_id / str(ordinal) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        return {"ref": path.relative_to(self.root).as_posix(), "digest": "sha256:" + hashlib.sha256(content.encode()).hexdigest()}

    def read_artifact(self, run_id: str, ordinal: int, name: str) -> str | None:
        """Read one task-owned artifact after enforcing its RunStore boundary."""
        if Path(name).name != name or Path(name).is_absolute():
            return None
        path = (self.artifacts / run_id / str(int(ordinal)) / name).resolve()
        try:
            if not path.is_relative_to(self.root.resolve()):
                return None
            return path.read_text(encoding="utf-8")
        except OSError:
            return None

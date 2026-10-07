"""One serial G5 worker that closes the provider-free Goal loop."""
from __future__ import annotations

import copy
import json
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import external_action_port as eap
import external_verdict as ev
import grill_loop
import turning_point as tp
import value_reducer
import verifier_normalizer
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import GOAL_CANDIDATE_SCHEMA, CampaignCompiler
from command_ingress import submit_command
from controller import LoopController
from goal_matcher import GoalMatcher
from goal_store import GoalStore
from knowledge_indexer import index_repo, index_run_evidence
from knowledge_store import KnowledgeStore
from run_store import RunStore


ModelRunner = Callable[[Path, dict[str, Any]], dict[str, Any]]

# H3: optional bounded turning-point node.  Receives a plain snapshot dict
# (turning_point.build_snapshot) and returns a raw decision for
# turning_point.validate_decision.  It gets no store handles, so it cannot
# admit goals, widen scope, or touch any gate.
TurningPointRunner = Callable[[dict[str, Any]], Any]

GoalLookup = Callable[[str], dict[str, Any] | None]


def eligible_runs(
    runs: list[dict[str, Any]],
    goal_lookup: GoalLookup,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """H2 eligibility filter + ordering (GoalHierarchy v1 contract §3).

    ``runs`` must already be in FIFO order (``created_at, run_id`` ascending),
    which is what ``RunStore.runnable_runs()`` returns.  A run is eligible only
    when its goal is ``active``, still holds this run, and every
    ``depends_on`` target is ``completed`` — only ``completed`` releases a
    dependency.  The result is ordered by goal ``priority`` descending; the
    sort is stable, so equal priorities keep the input FIFO order.  Goals
    without hierarchy fields (priority 0, no depends_on) therefore reproduce
    the exact pre-H2 FIFO order.
    """
    eligible: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for run in runs:
        goal_data = run["goal"] if isinstance(run.get("goal"), dict) else {}
        goal_id = goal_data.get("goal_id")
        if not isinstance(goal_id, str):
            continue
        goal = goal_lookup(goal_id)
        if goal is None:
            continue
        if goal["state"] != "active" or goal.get("run_id") != run["run_id"]:
            continue
        if any(
            (dep_goal := goal_lookup(dep)) is None or dep_goal["state"] != "completed"
            for dep in goal.get("depends_on") or []
        ):
            continue
        eligible.append((run, goal))
    eligible.sort(key=lambda pair: -int(pair[1].get("priority") or 0))
    return eligible


def select_next_runnable(
    runs: list[dict[str, Any]],
    goal_lookup: GoalLookup,
) -> dict[str, Any] | None:
    """H2 deterministic selector: the first eligible run (serial, single holder)."""
    eligible = eligible_runs(runs, goal_lookup)
    return eligible[0][0] if eligible else None


class GoalLoopWorker:
    """Claim and advance exactly one durable path per ``tick``.

    A tick is deliberately bounded: reconcile/poll, reduce one completed run,
    process one Goal event, dispatch one runnable run, then reduce that run's
    result.  Repeated ticks provide continuous resume without using chat
    context as a queue or starting parallel workers.
    """

    def __init__(
        self,
        *,
        goal_store: GoalStore,
        run_store: RunStore,
        controller: LoopController,
        compilers: dict[str, CampaignCompiler],
        execution_context: dict[str, dict[str, Any]],
        value_gate: bool = True,
        action_ledger: eap.ActionLedger | None = None,
        external_adapter: eap.ExternalAdapter | None = None,
        grill_runner: grill_loop.GrillRunner | None = None,
        now_fn: Callable[[], datetime] | None = None,
        knowledge_store: KnowledgeStore | None = None,
        knowledge_repo_roots: tuple[Path, ...] = (),
        recovery_binding=None,
    ):
        if (action_ledger is None) != (external_adapter is None):
            raise ValueError("action_ledger and external_adapter must be supplied together")
        self.goal_store = goal_store
        self.run_store = run_store
        self.controller = controller
        self.compilers = compilers
        self.execution_context = execution_context
        # W2: the async external-verdict dispatch leg (controller.tick_async).
        # Both must be present for an envelope-declared ``external_verdict``
        # stage to dispatch; without them such a stage routes to human.
        self.action_ledger = action_ledger
        self.external_adapter = external_adapter
        # W6a: optional challenger grill before a sync run's last allowed
        # attempt. None = today's behavior; the grill is advisory everywhere.
        self.grill_runner = grill_runner
        # W9f: clock for the standing-intent day window; injectable so the
        # emitter's window math stays testable without sleeping.
        self._now_fn = now_fn if now_fn is not None else lambda: datetime.now(timezone.utc)
        # When true, the deterministic 报红 value verdict is the acceptance
        # authority: a lamp-passing but value-RED run does not auto-advance, it
        # routes to human_required (LH execution model: 报红 gates completion).
        self.value_gate = value_gate
        self.recovery_binding = recovery_binding
        self.knowledge_store = knowledge_store
        self.knowledge_repo_roots = tuple(Path(item).resolve() for item in knowledge_repo_roots)

    def tick(
        self,
        *,
        holder: str,
        model: ModelRunner,
        verdict_store: ev.VerdictStore | None = None,
        conclusion_source: ev.ConclusionSource | None = None,
        turning_point: TurningPointRunner | None = None,
    ) -> dict[str, Any]:
        if (verdict_store is None) != (conclusion_source is None):
            raise ValueError("verdict_store and conclusion_source must be supplied together")
        standing = self._emit_standing_intents()
        startup = self.controller.startup()
        external = []
        if verdict_store is not None and conclusion_source is not None:
            # The asynchronous boundary: a success conclusion crosses to
            # verified only through the LH-owned normalized record and its
            # value_reduction_ready event (goal-lifecycle-v1).
            def normalize(**kwargs: Any) -> dict[str, Any]:
                return verifier_normalizer.normalize_resolved_run(
                    goal_store=self.goal_store, run_store=self.run_store,
                    verdict_store=verdict_store, **kwargs,
                )

            external = self.controller.resume_external(
                verdict_store=verdict_store, source=conclusion_source,
                normalizer=normalize,
            )
        terminal_before = self._reduce_one_terminal_run()
        self._enqueue_campaign_recoveries()
        event_result = self._process_one_event(holder)
        run_result = self._dispatch_one_run(holder, model, turning_point=turning_point, verdict_store=verdict_store)
        terminal_after = self._reduce_run_result(run_result) if run_result and run_result.get("status") in {"verified", "stopped", "human_required"} else None
        campaign_stops = self._campaign_failure_lines()
        # A run waiting for its verifier did nothing; counting it would make the driver spin.
        waiting = run_result is not None and run_result.get("status") == "waiting_for_verifier"
        progressed = any(item is not None and item != [] for item in (standing, startup, external, terminal_before, event_result, None if waiting else run_result, terminal_after, campaign_stops))
        return {
            "status": "progress" if progressed else "idle",
            "standing_emitted": standing,
            "startup_reconciled": startup,
            "external_resumed": external,
            "terminal_before": terminal_before,
            "event": event_result,
            "run": run_result,
            "terminal_after": terminal_after,
            "campaign_stops": campaign_stops,
        }

    def _goal_is_open(self, goal_id: str) -> bool:
        """W9f: an open goal blocks re-emission — candidate/active means work
        is in flight; human_required means the daily pass must look first.
        completed/stopped goals do not block: a new daily check may start."""
        try:
            goal = self.goal_store.get_goal(goal_id)
        except KeyError:
            return False
        return goal["state"] in {"candidate", "active", "human_required"}

    def _emit_standing_intents(self) -> list[dict[str, Any]]:
        """W9f standing intent emitter (deterministic, no model anywhere).

        At the start of every tick, each campaign's declared standing intents
        emit one manual_intent command per UTC day through the same durable
        event path a human command takes. Emission happens only when no
        command with today's idempotency key exists yet (record_event dedups
        naturally) and the stage's goal is not open. An emission failure is
        noted and skipped — it never crashes the tick.
        """
        emitted: list[dict[str, Any]] = []
        day = self._now_fn().astimezone(timezone.utc).date().isoformat()
        for campaign_id, compiler in self.compilers.items():
            for standing in getattr(compiler, "standing_intents", []):
                stage_id = standing["stage_id"]
                goal_id = f"{campaign_id}:{stage_id}"
                key = f"standing:{campaign_id}:{stage_id}:{day}"
                try:
                    if self._goal_is_open(goal_id):
                        continue
                    result = submit_command(
                        self.goal_store,
                        source="standing_intent",
                        event_type="manual_intent",
                        event_id=f"evt-{key}",
                        payload={"campaign_id": campaign_id, "stage_id": stage_id, "intent": standing["intent"]},
                        idempotency_key=key,
                    )
                    if result.get("status") != "reused":
                        emitted.append({"goal_id": goal_id, "idempotency_key": key, "intent": standing["intent"]})
                except Exception as exc:  # an emission failure must never crash the tick
                    emitted.append({"goal_id": goal_id, "idempotency_key": key, "note": f"emission skipped: {type(exc).__name__}: {exc}"})
        return emitted

    def _campaign_failure_lines(self) -> list[dict[str, Any]]:
        """W6b campaign consecutive-failure line (deterministic, no model).

        The count is derived from durable goal states on every call — goals
        of one campaign ordered by updated_at, taking the trailing run of
        human_required/stopped goals; any completed goal resets it to zero.
        When the count reaches the campaign's declared threshold, the
        remaining active/candidate goals of that campaign route to a human in
        one batch and a durable event records the stop. A completed goal
        establishes a new episode anchor, so a later independent failure
        episode can fire once again without replaying the earlier stop.
        """
        stops: list[dict[str, Any]] = []
        for campaign_id, compiler in self.compilers.items():
            threshold = int(getattr(compiler, "failure_stop_threshold", 3))
            outcomes = [
                goal
                for state in ("human_required", "stopped", "completed")
                for goal in self.goal_store.goals_in_state(state)
                if goal.get("campaign_id") == campaign_id
            ]
            outcomes.sort(key=lambda goal: (float(goal.get("updated_at") or 0), goal["goal_id"]))
            consecutive = 0
            failed_goals: list[dict[str, Any]] = []
            episode_anchor = "initial"
            for goal in outcomes:
                if goal["state"] == "completed":
                    consecutive = 0
                    failed_goals = []
                    episode_anchor = f"{goal['goal_id']}:{float(goal.get('updated_at') or 0):.9f}"
                else:
                    consecutive += 1
                    failed_goals.append(goal)
            if consecutive < threshold:
                continue
            episode_id = hashlib.sha256(f"{campaign_id}\0{episode_anchor}".encode()).hexdigest()[:16]
            event_key = f"campaign-failure-line:{campaign_id}:{episode_id}"
            try:
                self.goal_store.get_event(event_key)
                continue
            except KeyError:
                pass
            routed: list[str] = []
            pending = [*self.goal_store.active_goals(campaign_id=campaign_id),
                       *(goal for goal in self.goal_store.goals_in_state("candidate")
                         if goal.get("campaign_id") == campaign_id)]
            affected = (self._campaign_affected_goals(failed_goals, pending)
                        if self.recovery_binding is not None else {goal["goal_id"] for goal in pending})
            for goal in pending:
                if goal["goal_id"] in affected:
                    self.goal_store.transition_goal(goal["goal_id"], "human_required", expected_state=goal["state"])
                    routed.append(goal["goal_id"])
            payload = {
                "campaign_id": campaign_id,
                "consecutive_failures": consecutive,
                "threshold": threshold,
                "episode_id": episode_id,
                "routed_goal_ids": sorted(routed),
            }
            if self.recovery_binding is not None:
                payload["failed_goal_ids"] = [goal["goal_id"] for goal in failed_goals]
                payload["dispatch"] = self.recovery_binding.native_runtime["dispatch"]
            event = self.goal_store.record_event(
                event_id=event_key,
                idempotency_key=event_key,
                source="stop_lines",
                event_type="human_required",
                payload=payload,
            )
            self.goal_store.transition_event(event["event_key"], "human_required", result=payload)
            stops.append({**payload, "event_key": event_key})
        return stops


    @staticmethod
    def _campaign_scope(goal: dict[str, Any]) -> dict[str, Any]:
        revision = goal.get("current_revision") or {}
        payload = revision.get("goal") or {}
        envelope = payload.get("admission_envelope") or {}
        return {"write_set": envelope.get("allowed_paths") or [],
                "read_set": payload.get("read_set") or []}

    def _campaign_affected_goals(self, failed, pending) -> set[str]:
        from lh_runtime.work_unit_store import read_write_conflicts
        affected = {goal["goal_id"]: goal for goal in failed}
        changed = True
        while changed:
            changed = False
            for goal in pending:
                if goal["goal_id"] in affected:
                    continue
                scope = self._campaign_scope(goal)
                if (set(goal.get("depends_on") or []) & affected.keys()
                    or not scope["write_set"]
                    or any(not self._campaign_scope(other)["write_set"]
                           or read_write_conflicts(scope, self._campaign_scope(other))
                           for other in affected.values())):
                    affected[goal["goal_id"]] = goal
                    changed = True
        return set(affected)

    def _campaign_artifact(self, reference: dict[str, Any]) -> bytes:
        path = (self.run_store.root / reference["ref"]).resolve()
        if not path.is_relative_to(self.run_store.root.resolve()):
            raise ValueError("campaign_recovery_child_receipt_mismatch")
        raw = path.read_bytes()
        if "sha256:" + hashlib.sha256(raw).hexdigest() != reference["digest"]:
            raise ValueError("campaign_recovery_child_receipt_mismatch")
        return raw

    def _campaign_child(self, goal_id: str) -> dict[str, Any]:
        from lh_runtime.work_unit_store import digest_json
        runtime = self.recovery_binding.native_runtime
        goal = self.goal_store.get_goal(goal_id)
        run = self.run_store.get_run(goal["run_id"])
        attempt = self.run_store.latest_attempt(run["run_id"])
        revision = goal["current_revision"]
        if (goal["campaign_id"] != runtime["campaign_id"] or goal["state"] not in {"stopped", "human_required"}
            or run["state"] not in {"stopped", "human_required"} or not attempt
            or attempt["state"] not in {"stopped", "human_required"}
            or revision["revision_id"] != run["goal"]["revision_id"]
            or run["goal"]["goal_id"] != goal_id
            or run["attempts"] != attempt["ordinal"] or run["fence"] != attempt["fence"]
            or not self.run_store._receipt_artifact_valid(
                run["run_id"], attempt["ordinal"], attempt["receipt_ref"], attempt["receipt_digest"])):
            raise ValueError("campaign_recovery_child_receipt_mismatch")
        identity = {**self.run_store._delivery_identity(run, ordinal=attempt["ordinal"], fence=attempt["fence"]),
                    "authority_store": "run", "identity_profile": "native-run-v1"}
        self.recovery_binding._native_command_identity(identity, "planner")
        receipt = json.loads(self._campaign_artifact({"ref": attempt["receipt_ref"], "digest": attempt["receipt_digest"]}))
        verification = receipt.get("verification") or {}
        envelope = run["goal"]["admission_envelope"]
        if (receipt.get("dispatch") != runtime["dispatch"]
            or receipt.get("workspace", {}).get("base_revision") != runtime["base_revision"]
            or verification.get("exit_code") in (None, 0) or verification.get("precheck")
            or verification.get("argv") != envelope["acceptance_lamp"]["verification_argv"]):
            raise ValueError("campaign_recovery_child_receipt_mismatch")
        # Validate the actual evidence artifacts too; retain references, not model prose.
        patch = self._campaign_artifact(receipt["diff"])
        self._campaign_artifact(verification["stdout"])
        self._campaign_artifact(verification["stderr"])
        self._campaign_artifact(receipt["provider"]["artifact"])
        ceiling = self.run_store.effective_max_attempts(run["run_id"])
        return {**identity, "revision_id": revision["revision_id"],
                "receipt_ref": attempt["receipt_ref"], "receipt_digest": attempt["receipt_digest"],
                "candidate_digest": receipt["diff"]["digest"], "diff_ref": receipt["diff"],
                "diff_excerpt": patch[:32768].decode("utf-8", errors="replace"),
                "diff_excerpt_truncated": len(patch) > 32768,
                "verification": verification,
                "packet_digest": digest_json(run["goal"]["delivery_packet"]),
                "envelope_digest": digest_json(envelope),
                "write_set": list(envelope["allowed_paths"]),
                "attempt_limit": ceiling, "remaining_attempts": max(0, ceiling - run["attempts"])}

    def _enqueue_campaign_recoveries(self) -> None:
        """Replay durable stop inputs, not completed effects or synthetic Runs."""
        if self.recovery_binding is None:
            return
        from lh_runtime.work_unit_store import digest_json
        runtime = self.recovery_binding.native_runtime
        for stop in self.goal_store.events_from("stop_lines"):
            payload = stop["payload"]
            if (payload.get("campaign_id") != runtime["campaign_id"]
                or payload.get("dispatch") != runtime["dispatch"]
                or not payload.get("failed_goal_ids")):
                continue
            key = "campaign-recovery:" + stop["event_key"]
            try:
                self.goal_store.get_event(key)
                continue
            except KeyError:
                pass
            try:
                children = [self._campaign_child(goal_id) for goal_id in payload["failed_goal_ids"]]
            except (KeyError, ValueError, OSError) as exc:
                rejection = {"stop_event_key": stop["event_key"], "reason": "campaign_recovery_child_receipt_mismatch",
                             "detail": type(exc).__name__}
                event = self.goal_store.record_event(event_id=key, idempotency_key=key,
                    source="campaign_recovery", event_type="campaign_recovery_rejected", payload=rejection)
                # record_event returns the event's key and digest, not its payload.
                self.goal_store.transition_event(event["event_key"], "human_required", result=rejection)
                continue
            anchor = children[-1]
            evidence = [{"goal_id": child["goal_id"], "run_id": child["run_id"],
                         "attempt": child["attempt"], "fence": child["fence"],
                         "receipt_ref": child["receipt_ref"], "receipt_digest": child["receipt_digest"],
                         "diff_ref": child["diff_ref"]} for child in children]
            tests = [{"id": child["goal_id"] + ":acceptance",
                      "command_digest": digest_json(child["verification"]["argv"]),
                      "argv": child["verification"]["argv"]} for child in children]
            write_set = sorted({path for child in children for path in child["write_set"]})
            authority = {"contract_digest": runtime["contract_digest"], "dispatch": runtime["dispatch"],
                         "stop_payload_digest": stop["payload_digest"],
                         "children": [{key: child[key] for key in
                             ("goal_id", "revision_id", "unit_id", "run_id", "attempt_limit", "write_set",
                              "packet_digest", "envelope_digest")} for child in children]}
            request = {key: anchor[key] for key in
                       ("goal_id", "goal_revision", "node_id", "unit_id", "run_id", "attempt", "fence",
                        "base_sha", "authority_store", "identity_profile", "packet_digest", "envelope_digest")}
            request.update({
                "request_id": key, "reason_code": "campaign_consecutive_failures", "phase": "checks",
                "campaign_id": runtime["campaign_id"], "project_id": runtime["project_id"],
                "stop_event_key": stop["event_key"], "stop_payload_digest": stop["payload_digest"],
                "contract_digest": runtime["contract_digest"], "dispatch": runtime["dispatch"],
                "child_receipts": children, "sanitized_evidence_refs": evidence, "test_refs": tests,
                "candidate_digest": digest_json([child["candidate_digest"] for child in children]),
                "input_evidence_digest": digest_json(evidence), "authority_digest": digest_json(authority),
                "authority": authority, "write_set": write_set,
                "remaining_budget": {"child_attempts": {child["run_id"]: child["remaining_attempts"] for child in children},
                                     **runtime["planner_recovery"]["budget"]},
                "completed_effects": evidence,
                "repair_packet": {"child_receipts": evidence, "write_set": write_set, "test_refs": tests},
                "capability_binding": self.recovery_binding.runner.native_binding(
                    "coding", unit_id=anchor["unit_id"], attempt_id=str(anchor["attempt"])),
            })
            record = {"schema": "lh-recovery-record/v1", "request_id": key,
                      "request_digest": digest_json(request), "request": request, "status": "requested",
                      "result": None, "verdict": None, "apply": None, "claims": [], "events": [],
                      "budget_limits": runtime["planner_recovery"]["budget"],
                      "incident_started_at": None, "incident_deadline_at": None}
            self.goal_store.record_event(event_id=key, idempotency_key=key, source="campaign_recovery",
                event_type="campaign_recovery_requested", payload={"record": record})

    def _consume_campaign_recovery(self, event: dict[str, Any]) -> dict[str, Any]:
        """The existing claimed Goal event owns bounded review, never child retry authority."""
        from lh_runtime.lifecycle import NativeProcessIdentityPort, observe_process_identity
        from lh_runtime.work_unit_store import digest_json, validate_recovery_plan
        if self.recovery_binding is None:
            return {"status": "campaign_recovery_optin_missing", "event_key": event["event_key"]}
        event = self.goal_store.get_event(event["event_key"])
        record = copy.deepcopy(event["result"] or event["payload"]["record"])
        request = record["request"]
        runtime = self.recovery_binding.native_runtime

        def save(status, *, reason=None, terminal=False):
            nonlocal event
            record["status"] = status
            if reason is not None:
                record["reason"] = reason
            record["events"].append({"type": status, "at": time.time(), "reason": reason})
            event = self.goal_store.transition_event(event["event_key"],
                "human_required" if terminal else "event_received", result=record,
                expected_result_digest=digest_json(event["result"]))

        def validate_inputs():
            stop = self.goal_store.get_event(request["stop_event_key"])
            if (record["request_digest"] != digest_json(request)
                or request["identity_profile"] != "native-run-v1"
                or request["contract_digest"] != runtime["contract_digest"]
                or request["dispatch"] != runtime["dispatch"]
                or stop["payload_digest"] != request["stop_payload_digest"]
                or stop["payload"].get("failed_goal_ids") != [child["goal_id"] for child in request["child_receipts"]]
                or record["budget_limits"] != runtime["planner_recovery"]["budget"]
                or "sha256:" + hashlib.sha256(Path(runtime["contract_ref"]).read_bytes()).hexdigest()
                   != runtime["contract_digest"]):
                raise ValueError("campaign_recovery_authority_mismatch")
            for child in request["child_receipts"]:
                if self._campaign_child(child["goal_id"]) != child:
                    raise ValueError("campaign_recovery_child_receipt_mismatch")

        if record["status"] in {"awaiting_authority", "rejected", "outcome_unknown"}:
            return record
        try:
            validate_inputs()  # after claim_event, so claim-time receipt drift is observable
        except (ValueError, KeyError, OSError) as exc:
            reason = str(exc) if str(exc).startswith("campaign_recovery_") else "campaign_recovery_child_receipt_mismatch"
            save("rejected", reason=reason, terminal=True)
            return record
        capabilities = self.recovery_binding.runner.contract["capabilities"]
        principals = [capabilities[name]["identity"]["principal"] for name in ("coding", "planning", "verifier")]
        if len(set(principals)) != 3:
            save("rejected", reason="campaign_recovery_verifier_not_independent", terminal=True)
            return record

        unresolved = [claim for claim in record["claims"] if claim["state"] == "claimed"]
        if unresolved:
            for claim in unresolved:
                observation = observe_process_identity(claim.get("owner_process_identity"))
                if observation.status == "alive":
                    return {"status": "campaign_recovery_role_inflight", "request_id": record["request_id"]}
                claim["state"] = "outcome_unknown"
                claim["owner_observation"] = observation.status
            save("outcome_unknown", reason="campaign_recovery_role_outcome_unknown", terminal=True)
            return record

        for phase, field in (("planner", "result"), ("plan_verifier", "verdict")):
            if record[field] is not None:
                continue
            if any(claim["phase"] == phase for claim in record["claims"]):
                save("outcome_unknown", reason="campaign_recovery_role_outcome_unknown", terminal=True)
                return record
            owner = NativeProcessIdentityPort().current()
            if owner is None:
                save("rejected", reason="campaign_recovery_owner_identity_unknown", terminal=True)
                return record
            now = time.time()
            if record["incident_started_at"] is None:
                record["incident_started_at"] = now
                record["incident_deadline_at"] = now + record["budget_limits"]["incident_timeout_seconds"]
            remaining = record["incident_deadline_at"] - now
            if remaining <= 0:
                save("awaiting_authority", reason="campaign_recovery_incident_deadline_exhausted", terminal=True)
                return record
            limit = record["budget_limits"][phase + "_calls"]
            if sum(claim["phase"] == phase for claim in record["claims"]) >= limit:
                save("awaiting_authority", reason="campaign_recovery_role_budget_exhausted", terminal=True)
                return record
            claim = {"call_id": digest_json([record["request_id"], phase]), "phase": phase, "state": "claimed",
                     "started_at": now, "owner_process_identity": owner.as_dict(),
                     "incident_started_at": record["incident_started_at"],
                     "incident_deadline_at": record["incident_deadline_at"]}
            record["claims"].append(claim)
            save("claimed" if phase == "planner" else "verifier_claimed")

            def on_started(process):
                identity = NativeProcessIdentityPort().observe(process.pid)
                if identity is None:
                    raise ValueError("campaign_recovery_role_process_identity_unknown")
                claim["process_identity"] = identity.as_dict()
                save(record["status"])

            try:
                validate_inputs()
                timeout = min(record["budget_limits"][phase + "_timeout_seconds"],
                              record["incident_deadline_at"] - time.time())
                if timeout <= 0:
                    raise ValueError("campaign_recovery_incident_deadline_exhausted")
                frame = {"mode": phase, "recovery_request": request}
                if phase == "plan_verifier":
                    frame["planner_result"] = record["result"]
                completed, binding, _ = self.recovery_binding.command(request, phase=phase,
                    argv=runtime["planner_recovery"][phase + "_argv"], worktree=runtime["source_repo"],
                    timeout_seconds=timeout, input_request=frame, writable=False, on_started=on_started)
                if completed.returncode != 0:
                    raise ValueError("campaign_recovery_role_failed")
                result = json.loads(completed.stdout)
                if not isinstance(result, dict):
                    raise ValueError("campaign_recovery_role_result_invalid")
                # Metadata is from the real command boundary, not provider-authored claims.
                result.update(binding)
                claim["state"] = "success"
                claim["finished_at"] = time.time()
                claim["stdout_digest"] = "sha256:" + hashlib.sha256(completed.stdout.encode()).hexdigest()
                record[field] = result
            except Exception as exc:
                claim["state"] = "outcome_unknown" if "process_identity" in claim else "failed"
                save("outcome_unknown" if claim["state"] == "outcome_unknown" else "rejected",
                     reason=str(exc) if str(exc).startswith("campaign_recovery_") else "campaign_recovery_role_failed",
                     terminal=True)
                return record
            # Public durable seam: a restart here skips the recorded role.
            save("result_recorded" if phase == "planner" else "verdict_recorded")

        try:
            validate_inputs()
            validate_recovery_plan(record, identity_profile=runtime["identity_profile"])
        except (ValueError, KeyError, OSError) as exc:
            save("rejected", reason="campaign_recovery_plan_invalid", terminal=True)
            return record
        # A valid review is not authority to replenish a child's native ceiling.
        exhausted = any(child["remaining_attempts"] == 0 for child in request["child_receipts"])
        reason = ("campaign_recovery_child_attempt_budget_exhausted" if exhausted
                  else "campaign_recovery_requires_authority")
        record["apply"] = {"status": "awaiting_authority", "reason": reason, "request_digest": record["request_digest"]}
        save("awaiting_authority", reason=reason, terminal=True)
        return record

    def _process_one_event(self, holder: str) -> dict[str, Any] | None:
        for event in self.goal_store.pending_events():
            if not self.goal_store.claim_event(event["event_key"], holder):
                continue
            try:
                result = self._process_event(event)
            finally:
                self.goal_store.release_event(event["event_key"], holder)
            # A command-down manual_intent is intentionally normalized into a
            # derived candidate event before admission.  Consume that one
            # deterministic follow-up in the same bounded tick so a
            # max_cycles=1 execution can still create its single Goal/Run and
            # dispatch its Attempt.  Only this explicit derivation is followed;
            # arbitrary event chains remain one event per tick.
            derived_key = (
                result.get("derived_event_key")
                if isinstance(result, dict)
                and result.get("status") == "derived_candidate_event"
                else None
            )
            if (
                event.get("event_type") == "manual_intent"
                and event.get("source") != "standing_intent"
                and isinstance(event.get("payload"), dict)
                and isinstance(event["payload"].get("correlation_id"), str)
                and event["payload"]["correlation_id"].strip()
                and isinstance(derived_key, str)
                and derived_key
            ):
                try:
                    derived = self.goal_store.get_event(derived_key)
                except KeyError:
                    derived = None
                if (
                    derived is not None
                    and derived.get("state") in {"event_received", "candidate"}
                    and self.goal_store.claim_event(derived_key, holder)
                ):
                    try:
                        result = {
                            **result,
                            "derived_event_result": self._process_event(derived),
                        }
                    finally:
                        self.goal_store.release_event(derived_key, holder)
            return result
        return None

    def _process_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_key = event["event_key"]
        if event["source"] == "campaign_recovery" and event["event_type"] == "campaign_recovery_requested":
            return self._consume_campaign_recovery(event)
        if event["event_type"] == "scheduled_tick":
            result = {
                "status": "scheduled_tick_consumed",
                "event_key": event_key,
                "wake_only": True,
            }
            self.goal_store.transition_event(event_key, "completed", result=result)
            return result
        if event["event_type"] == "stage_completion" and "candidate" not in event["payload"]:
            return self._derive_stage_completion(event)
        if event["event_type"] == "manual_intent" and "candidate" not in event["payload"] and "goal_id" not in event["payload"]:
            return self._derive_intent_candidate(event)
        matcher = GoalMatcher(self.goal_store.active_goals())
        reduced = matcher.reduce(event)
        route = reduced["route"]
        if route == "candidate":
            candidate = reduced["candidate"]
            try:
                stored = self.goal_store.create_candidate(
                    event_key,
                    goal_id=candidate["goal_id"],
                    campaign_id=candidate["campaign_id"],
                    stage_id=candidate["stage_id"],
                    goal=candidate["goal"],
                )
            except ValueError:
                # A re-issued command for a goal that already exists from a
                # different source event must not crash the tick.  When the
                # goal is already a candidate, proceed to admission with it;
                # parked/terminal goals (human_required/stopped/completed)
                # revive as candidates; admission re-checks the complete
                # envelope and context before any fresh Run is created.
                try:
                    existing = self.goal_store.get_goal(candidate["goal_id"])
                except KeyError:
                    existing = None
                if existing is not None and existing["state"] in {"human_required", "stopped", "completed"}:
                    # A new explicit candidate is the re-admission signal for
                    # a parked or terminal goal.  The full admission policy
                    # still decides whether it may run; invalid or human-only
                    # candidates remain human_required.
                    self.goal_store.transition_goal(existing["goal_id"], "candidate", expected_state=existing["state"])
                elif existing is None or existing["state"] != "candidate":
                    result = {
                        **reduced,
                        "status": "human_required",
                        "reason": "goal exists from a different source event and is not re-admissible",
                    }
                    self.goal_store.transition_event(event_key, "human_required", result=result)
                    return result
                stored = {"state": "reused", "goal_id": existing["goal_id"]}
            envelope = candidate["goal"].get("admission_envelope") if isinstance(candidate["goal"], dict) else None
            context = self.execution_context.get(candidate["campaign_id"])
            if not isinstance(envelope, dict) or not isinstance(context, dict):
                result = {**reduced, "status": "human_required", "reason": "candidate lacks durable execution context or admission envelope"}
                self.goal_store.transition_event(event_key, "human_required", result=result)
                return result
            admission = GoalAdmissionBridge(self.goal_store, self.run_store).admit(
                candidate["goal_id"],
                source_repo=context.get("source_repo", ""),
                base_revision=context.get("base_revision", ""),
                envelope=envelope,
                event_key=event_key,
            )
            if admission["status"] in {"active", "reused"}:
                # The candidate is consumed: a successful admission completes
                # its source event. Leaving it pending re-processes the same
                # candidate every tick — which, now that terminal goals can be
                # revived (W9g/W9h), would re-admit and re-run forever.
                result = {**reduced, "status": admission["status"], "admission": admission, "stored": stored}
                self.goal_store.transition_event(event_key, "completed", result=result)
                return result
            result = {**reduced, "status": "human_required", "admission": admission}
            self.goal_store.transition_event(event_key, "human_required", result=result)
            return result
        if route == "bind":
            self.goal_store.transition_event(event_key, "active", result=reduced)
            return reduced
        self.goal_store.transition_event(event_key, route, result=reduced)
        return reduced

    def _derive_intent_candidate(self, event: dict[str, Any]) -> dict[str, Any]:
        """Derive one candidate from a manual_intent command (MVP W1).

        A commander (e.g. an external hub's command-down) sends only
        ``{campaign_id, stage_id, intent}``; without this derivation the
        matcher routes every such event to ``human_required`` and the loop
        silently never walks.  The derivation mirrors
        ``_derive_stage_completion``: build the candidate from the compiled
        campaign stage (its goal and admission envelope), record it as a new
        deduped event, and let the normal candidate path pick it up next
        tick.  Unknown campaign/stage or a non-auto-admissible stage routes
        to ``human_required`` exactly as before.
        """
        event_key = event["event_key"]
        payload = event["payload"]
        campaign_id = payload.get("campaign_id")
        compiler = self.compilers.get(campaign_id)
        stage_id = payload.get("stage_id")
        fail: str | None = None
        stage: dict[str, Any] | None = None
        if compiler is None:
            fail = "no compiled campaign for intent"
        else:
            stage = compiler.stages.get(stage_id) if isinstance(stage_id, str) else None
            if stage is None:
                fail = f"unknown stage_id for intent: {stage_id}"
            elif not stage["auto_admission"]["eligible"]:
                reasons = ";".join(stage["auto_admission"]["reasons"])
                fail = reasons or "stage is not auto-admissible"
        if fail is not None:
            result = {"status": "human_required", "reason": fail, "source_event_key": event_key}
            self.goal_store.transition_event(event_key, "human_required", result=result)
            return result
        candidate = {
            "schema": GOAL_CANDIDATE_SCHEMA,
            "goal_id": f"{campaign_id}:{stage_id}",
            "campaign_id": campaign_id,
            "stage_id": stage_id,
            "goal": {
                "feature_contract": stage["goal"],
                "admission_envelope": stage,
            },
        }
        derived_key = f"intent-derived:{event_key}"
        stored = self.goal_store.record_event(
            event_id=f"evt-{derived_key}",
            idempotency_key=derived_key,
            source="manual_intent",
            event_type="goal_candidate",
            payload={"candidate": candidate, "source_event_key": event_key},
        )
        result = {"status": "derived_candidate_event", "derived_event_key": stored["event_key"], "source_event_key": event_key}
        self.goal_store.transition_event(event_key, "completed", result=result)
        return result

    def _derive_stage_completion(self, event: dict[str, Any]) -> dict[str, Any]:
        payload = event["payload"]
        campaign_id = payload.get("campaign_id")
        compiler = self.compilers.get(campaign_id)
        if compiler is None:
            result = {"status": "human_required", "reason": "no compiled campaign for stage completion"}
            self.goal_store.transition_event(event["event_key"], "human_required", result=result)
            return result
        advanced = compiler.advance(payload)
        if advanced["status"] == "candidate_ready":
            stored = self.goal_store.record_event(**advanced["event"])
            result = {"status": "derived_candidate_event", "derived_event_key": stored["event_key"], "source_event_key": event["event_key"]}
            self.goal_store.transition_event(event["event_key"], "completed", result=result)
            return result
        state = "human_required" if advanced["status"] == "human_required" else "completed"
        self.goal_store.transition_event(event["event_key"], state, result=advanced)
        return {"status": advanced["status"], "source_event_key": event["event_key"], "reason": advanced.get("reason")}

    def _goal_lookup(self, goal_id: str) -> dict[str, Any] | None:
        try:
            return self.goal_store.get_goal(goal_id)
        except KeyError:
            return None

    def _turning_point_pick(
        self,
        turning_point: TurningPointRunner,
        eligible: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """H3: let the optional model node pick among the legal options.

        Returns ``(run, route)``: exactly one is non-None.  ``run`` is the
        chosen run to dispatch; ``route`` is a worker result for the
        ``parent_done`` / ``human_required`` decisions.  Any rejection falls
        back to the deterministic H2 order (``eligible[0]``).

        ``rollup_satisfied`` is always False here: a non-empty eligible set
        means at least one child is still ``active``, so no parent rollup can
        be complete.  ``parent_done`` from the model is therefore always
        rejected on this path (it is only ever a no-op confirm of what the
        deterministic H1 rollup already did; see turning_point.py).
        """
        rollup_satisfied = False
        snapshot = tp.build_snapshot(
            eligible,
            rollup_satisfied=rollup_satisfied,
            gate={"value_gate_enabled": self.value_gate, "serial_single_holder": True},
        )
        try:
            raw = turning_point(snapshot)
        except Exception:  # a failing model must never stop the loop
            raw = None
        decision = tp.validate_decision(
            raw,
            runnable_goal_ids=[goal["goal_id"] for _, goal in eligible],
            rollup_satisfied=rollup_satisfied,
        )
        kind = decision["type"]
        if kind == "select":
            for run, goal in eligible:
                if goal["goal_id"] == decision["goal_id"]:
                    return run, None
        elif kind == "human_required":
            parent_ids = {goal.get("parent_goal_id") for _, goal in eligible}
            if len(parent_ids) == 1 and None not in parent_ids:
                target = next(iter(parent_ids))
            else:
                target = eligible[0][1]["goal_id"]
            self.goal_store.transition_goal(target, "human_required", expected_state="active")
            return None, {"status": "human_required", "goal_id": target, "reason": "turning-point judgment requested a human"}
        # reject (including parent_done and out-of-set select): deterministic fallback
        return eligible[0][0], None

    def _goal_knowledge_context(self, run: dict[str, Any]) -> dict[str, Any] | None:
        """Refresh bounded local sources and retrieve advisory next-Goal context."""
        if self.knowledge_store is None:
            return None
        try:
            for repo in self.knowledge_repo_roots:
                index_repo(repo=repo, store=self.knowledge_store, source_prefix=repo.name)
            index_run_evidence(run_store=self.run_store, store=self.knowledge_store)
            goal = run.get("goal") if isinstance(run.get("goal"), dict) else {}
            feature = goal.get("feature_contract", "")
            if isinstance(feature, (dict, list)):
                feature = json.dumps(feature, ensure_ascii=False, sort_keys=True)
            query = f"{str(feature)[:700]} verification receipt failure restart retry budget"
            return self.knowledge_store.bounded_context(query, max_results=4, max_chars=2400)
        except Exception as exc:
            # Retrieval is advisory.  A broken index must not stop execution or
            # widen any acceptance path.
            return {
                "schema": "loop-hybrid-goal-context/v1",
                "query": "",
                "authority": "advisory_only",
                "gate_mutation": "forbidden",
                "hits": [],
                "chars": 0,
                "unavailable": type(exc).__name__,
            }

    def _dispatch_one_run(
        self,
        holder: str,
        model: ModelRunner,
        turning_point: TurningPointRunner | None = None,
        verdict_store: ev.VerdictStore | None = None,
    ) -> dict[str, Any] | None:
        eligible = eligible_runs(self.run_store.runnable_runs(), self._goal_lookup)
        if not eligible:
            return None
        run = eligible[0][0]
        if turning_point is not None:
            picked, route = self._turning_point_pick(turning_point, eligible)
            if route is not None:
                return route
            run = picked if picked is not None else eligible[0][0]
        goal_id = run["goal"]["goal_id"]
        envelope = run["goal"].get("admission_envelope")
        verifier_argv = None
        if isinstance(envelope, dict) and isinstance(envelope.get("acceptance_lamp"), dict):
            verifier_argv = envelope["acceptance_lamp"].get("verification_argv")
        if isinstance(verifier_argv, list) and verifier_argv and all(isinstance(item, str) and item.strip() for item in verifier_argv):
            grill_note, grill_route = self._grill_before_final_attempt(
                run,
                goal_id,
                holder=holder,
                acceptance_argv=verifier_argv,
            )
            if grill_route is not None:
                return grill_route
            return self.controller.tick(run["run_id"], holder=holder, model=model, verifier_argv=verifier_argv, grill_note=grill_note, knowledge_context=self._goal_knowledge_context(run))
        # W2: an envelope-declared external_verdict (and no local verifier) takes
        # the async leg — dispatch the external action at most once, park the run.
        external = envelope.get("external_verdict") if isinstance(envelope, dict) else None
        if isinstance(external, dict) and isinstance(external.get("action_id"), str) and external["action_id"].strip():
            if verdict_store is None or self.action_ledger is None:
                result = {"status": "human_required", "run_id": run["run_id"], "reason": "external verdict wiring is missing"}
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
                return result
            return self.controller.tick_async(
                run["run_id"], holder=holder, model=model, verdict_store=verdict_store,
                action_ledger=self.action_ledger, adapter=self.external_adapter,
                action_id=external["action_id"].strip(), knowledge_context=self._goal_knowledge_context(run),
            )
        result = {"status": "human_required", "run_id": run["run_id"], "reason": "durable verification plan is missing"}
        self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
        return result

    def _grill_before_final_attempt(
        self,
        run: dict[str, Any],
        goal_id: str,
        *,
        holder: str,
        acceptance_argv: list[str],
    ) -> tuple[str | None, dict[str, Any] | None]:
        """W6a challenger grill before a sync run's last allowed attempt.

        Returns ``(note, route)``: ``note`` is the diagnosis to inject into the
        remediation capsule; ``route`` short-circuits dispatch when the goal is
        broken, the claim is busy, or the challenger is unavailable/invalid.
        FC-P0 claims and fences the generation before any challenger call.
        Advisory only: the grill never touches the lamp, the scope, or the goal
        content, and its output is never an acceptance authority.
        """
        failure_case = self.run_store.failure_case_for_run(
            run["run_id"],
            states={"grill_required", "grill_claimed"},
        )
        if failure_case is None and grill_loop.should_grill(run):
            signature = self.run_store.latest_failure_signature(run["run_id"])
            if signature is not None:
                failure_case = self.run_store.ensure_failure_case(
                    run["run_id"],
                    signature=signature,
                    acceptance_argv=acceptance_argv,
                    trigger="attempt_budget_before_final",
                    receipt_digest=(self.run_store.latest_receipt(run["run_id"]) or {}).get("receipt_digest"),
                )
        if failure_case is None:
            return None, None
        claim = self.run_store.claim_grill(
            failure_case["failure_case_id"],
            holder=holder,
        )
        if claim["status"] != "claimed":
            return None, {
                "status": "grill_busy",
                "run_id": run["run_id"],
                "goal_id": goal_id,
                "failure_case_id": failure_case["failure_case_id"],
                "claim": claim,
            }
        generation = int(claim["generation"])
        fence = int(claim["fence"])
        if self.grill_runner is None:
            failure = self.run_store.record_grill_failure(
                failure_case["failure_case_id"],
                generation=generation,
                fence=fence,
                reason="judge_not_configured",
            )
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            event = self.goal_store.record_event(
                event_id=f"grill-unavailable:{failure_case['failure_case_id']}:{generation}",
                idempotency_key=f"grill-unavailable:{failure_case['failure_case_id']}:{generation}",
                source="grill_loop",
                event_type="human_required",
                payload={"run_id": run["run_id"], "goal_id": goal_id, "failure_case": failure},
            )
            self.goal_store.transition_event(event["event_key"], "human_required", result=failure)
            return None, {
                "status": "human_required",
                "run_id": run["run_id"],
                "goal_id": goal_id,
                "reason": "failure case requires a configured challenger",
                "failure_case": failure,
            }
        snapshot = grill_loop.build_snapshot(self.run_store, run)
        snapshot["failure_case_id"] = failure_case["failure_case_id"]
        snapshot["grill_generation"] = generation
        try:
            raw = self.grill_runner(snapshot)
        except Exception as exc:
            failure = self.run_store.record_grill_failure(
                failure_case["failure_case_id"],
                generation=generation,
                fence=fence,
                reason=f"judge_error:{type(exc).__name__}:{str(exc)[:300]}",
            )
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            return None, {
                "status": "human_required",
                "run_id": run["run_id"],
                "goal_id": goal_id,
                "reason": "challenger failed; durable failure recorded",
                "failure_case": failure,
            }
        decision = grill_loop.validate_decision(raw)
        if decision["type"] == "reject":
            failure = self.run_store.record_grill_failure(
                failure_case["failure_case_id"],
                generation=generation,
                fence=fence,
                reason=f"judge_output_rejected:{decision['reason']}",
            )
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            return None, {
                "status": "human_required",
                "run_id": run["run_id"],
                "goal_id": goal_id,
                "reason": "challenger output rejected; durable failure recorded",
                "failure_case": failure,
            }
        recorded = self.run_store.record_grill_result(
            failure_case["failure_case_id"],
            generation=generation,
            fence=fence,
            decision=decision["type"],
            diagnosis=decision["diagnosis"],
        )
        evidence = {
            "failure_case_id": failure_case["failure_case_id"],
            "decision": decision["type"],
            "diagnosis": decision["diagnosis"],
            "attempts_used": int(run["attempts"]),
            "grill_generation": generation,
            "plan_digest": recorded.get("plan_digest"),
        }
        if decision["type"] == "goal-broken":
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            event = self.goal_store.record_event(
                event_id=f"grill-goal-broken:{failure_case['failure_case_id']}:{generation}",
                idempotency_key=f"grill-goal-broken:{failure_case['failure_case_id']}:{generation}",
                source="grill_loop",
                event_type="human_required",
                payload={"run_id": run["run_id"], "goal_id": goal_id, "grill": evidence},
            )
            self.goal_store.transition_event(event["event_key"], "human_required", result=evidence)
            return None, {"status": "human_required", "run_id": run["run_id"], "goal_id": goal_id,
                          "reason": "grill judged the goal broken; final attempt not dispatched", "grill": evidence}
        return decision["diagnosis"], None

    def _reduce_one_terminal_run(self) -> dict[str, Any] | None:
        for run in self.run_store.terminal_runs():
            result = self._reduce_run_result({"status": run["state"], "run_id": run["run_id"]})
            if result is not None:
                return result
        return None

    def _reduce_run_result(self, run_result: dict[str, Any]) -> dict[str, Any] | None:
        run_id = run_result.get("run_id")
        if not isinstance(run_id, str):
            return None
        run = self.run_store.get_run(run_id)
        goal_data = run["goal"]
        goal_id = goal_data.get("goal_id") if isinstance(goal_data, dict) else None
        if not isinstance(goal_id, str):
            return None
        goal = self._goal_lookup(goal_id)
        if goal is None:
            # Orphaned terminal run (its goal was retired): nothing to reduce.
            # Skipping is cheap and must not crash the tick.
            return None
        if goal["state"] != "active":
            return None
        receipt_meta = self.run_store.latest_receipt(run_id)
        if receipt_meta is None:
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            return {"status": "human_required", "run_id": run_id, "reason": "terminal run has no receipt"}
        receipt_path = self.run_store.root / receipt_meta["receipt_ref"]
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            return {"status": "human_required", "run_id": run_id, "reason": f"receipt unreadable: {exc}"}
        if run["state"] == "verified":
            if self.run_store.delivery_required(run_id):
                delivery = self.run_store.verify_delivery(run_id, phase="final")
                if delivery.get("verdict") != "GREEN":
                    # Historical/legacy verified Runs remain readable, but a
                    # new Goal completion may not project them as a fresh
                    # success without the same RunStore-owned final binding.
                    self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
                    return {
                        "status": "human_required",
                        "run_id": run_id,
                        "reason": delivery.get("reason", "delivery_final_not_green"),
                        "delivery": delivery,
                    }
            lamp_equivalent = False
            if self.value_gate:
                verdict = value_reducer.value_evidence_for_run(
                    self.run_store, run_id, goal_store=self.goal_store,
                )
                # An async GREEN carries the ready proof; the compiler's own
                # lamp check downstream receives the lamp-equivalent form
                # only on that proof, never on the bare async receipt.
                lamp_equivalent = bool(verdict.get("ready_event_key"))
                if verdict["verdict"] == "RED":
                    failure_case = self.run_store.record_resolution(
                        run_id,
                        ordinal=run["attempts"],
                        receipt_digest=receipt_meta["receipt_digest"],
                        exit_code=int(receipt.get("verification", {}).get("exit_code", 0)),
                        machine_resolved=False,
                        allow_regrill=False,
                        checker_reason="value_gate_red",
                    )
                    self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
                    red_event = self.goal_store.record_event(
                        event_id=f"value-red:{run_id}:{receipt_meta['receipt_digest']}",
                        idempotency_key=f"value-red:{run_id}:{receipt_meta['receipt_digest']}",
                        source="value_reducer",
                        event_type="human_required",
                        payload={"run_id": run_id, "goal_id": goal_id, "value_verdict": verdict},
                    )
                    self.goal_store.transition_event(red_event["event_key"], "human_required", result=verdict)
                    return {
                        "status": "value_red_human_required",
                        "run_id": run_id,
                        "event_key": red_event["event_key"],
                        "reasons": verdict["reasons"],
                        "failure_case": failure_case,
                    }
            resolution = self.run_store.record_resolution(
                run_id,
                ordinal=run["attempts"],
                receipt_digest=receipt_meta["receipt_digest"],
                exit_code=int(receipt.get("verification", {}).get("exit_code", 0)),
                machine_resolved=True,
                checker_reason="lamp_and_value_gate_green",
            )
            compiler = self.compilers.get(goal["campaign_id"])
            if compiler is None:
                self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
                return {"status": "human_required", "run_id": run_id, "reason": "no compiled campaign for verified run"}
            completion_verification = receipt.get("verification")
            if lamp_equivalent and isinstance(completion_verification, dict):
                completion_verification = {**completion_verification, "exit_code": 0}
            completion = {
                "campaign_id": goal["campaign_id"],
                "stage_id": goal["stage_id"],
                "receipt_id": receipt_meta["receipt_digest"],
                "verification": completion_verification,
            }
            advanced = compiler.advance(completion)
            if advanced["status"] == "candidate_ready":
                stored = self.goal_store.record_event(**advanced["event"])
                self.goal_store.transition_goal(goal_id, "completed", expected_state="active")
                closed = self.run_store.record_next_node(
                    run_id,
                    next_node_id=advanced["candidate"]["goal_id"],
                )
                return {
                    "status": "completed_with_next_event",
                    "run_id": run_id,
                    "derived_event_key": stored["event_key"],
                    "failure_case": closed or resolution,
                }
            if advanced["status"] == "completed":
                self.goal_store.transition_goal(goal_id, "completed", expected_state="active")
                closed = self.run_store.record_next_node(
                    run_id,
                    next_node_id="campaign_completed",
                )
                return {"status": "completed", "run_id": run_id, "failure_case": closed or resolution}
            self.goal_store.transition_goal(goal_id, "completed", expected_state="active")
            result_event = self.goal_store.record_event(
                event_id=f"run-result:{run_id}:{receipt_meta['receipt_digest']}",
                idempotency_key=f"run-result:{run_id}:{receipt_meta['receipt_digest']}",
                source="run_completion",
                event_type="human_required",
                payload={"run_id": run_id, "goal_id": goal_id, "reason": advanced.get("reason"), "verification": receipt.get("verification")},
            )
            self.goal_store.transition_event(result_event["event_key"], "human_required", result=advanced)
            return {"status": "human_required", "run_id": run_id, "event_key": result_event["event_key"], "reason": advanced.get("reason")}
        routing = receipt.get("routing")
        if isinstance(routing, dict) and routing.get("route") == "human_required":
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            result_event = self.goal_store.record_event(
                event_id=f"routing-human:{run_id}:{receipt_meta['receipt_digest']}",
                idempotency_key=f"routing-human:{run_id}:{receipt_meta['receipt_digest']}",
                source="capability_resolver",
                event_type="human_required",
                payload={"run_id": run_id, "goal_id": goal_id, "routing": routing},
            )
            self.goal_store.transition_event(
                result_event["event_key"],
                "human_required",
                result={"run_id": run_id, "routing": routing},
            )
            return {
                "status": "human_required",
                "run_id": run_id,
                "event_key": result_event["event_key"],
                "reason": "capability resolver requested human routing",
                "routing": routing,
            }
        grill = grill_loop.grill_evidence(self.run_store, run_id)
        if grill is not None:
            # W6a: the final attempt ran with grill guidance and still failed —
            # the goal needs a human, with the grill chain attached as evidence.
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            result_event = self.goal_store.record_event(
                event_id=f"grill-final-failed:{run_id}",
                idempotency_key=f"grill-final-failed:{run_id}",
                source="grill_loop",
                event_type="human_required",
                payload={"run_id": run_id, "goal_id": goal_id, "grill": grill, "verification": receipt.get("verification")},
            )
            self.goal_store.transition_event(result_event["event_key"], "human_required", result={"run_id": run_id, "grill": grill, "verification": receipt.get("verification")})
            return {"status": "human_required", "run_id": run_id, "reason": "final attempt failed after a runner-fixable grill", "grill": grill}
        self.goal_store.transition_goal(goal_id, "stopped", expected_state="active")
        result_event = self.goal_store.record_event(
            event_id=f"run-result:{run_id}:{receipt_meta['receipt_digest']}",
            idempotency_key=f"run-result:{run_id}:{receipt_meta['receipt_digest']}",
            source="run_completion",
            event_type="run_stopped",
            payload={"run_id": run_id, "goal_id": goal_id, "verification": receipt.get("verification")},
        )
        self.goal_store.transition_event(result_event["event_key"], "stopped", result={"run_id": run_id, "verification": receipt.get("verification")})
        return {"status": "stopped", "run_id": run_id, "event_key": result_event["event_key"]}


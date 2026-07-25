"""One serial G5 worker that closes the provider-free Goal loop."""
from __future__ import annotations

import json
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import external_action_port as eap
import external_verdict as ev
import grill_loop
import merge_gate as merge_gate_mod
import turning_point as tp
import value_reducer
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
        merge_gate: merge_gate_mod.MergeGate | None = None,
        now_fn: Callable[[], datetime] | None = None,
        knowledge_store: KnowledgeStore | None = None,
        knowledge_repo_roots: tuple[Path, ...] = (),
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
        # B13: optional conditional auto-merge gate, consulted only when a
        # parked run resolves. None = output stops at the draft PR.
        self.merge_gate = merge_gate
        # W9f: clock for the standing-intent day window; injectable so the
        # emitter's window math stays testable without sleeping.
        self._now_fn = now_fn if now_fn is not None else lambda: datetime.now(timezone.utc)
        # When true, the deterministic 报红 value verdict is the acceptance
        # authority: a lamp-passing but value-RED run does not auto-advance, it
        # routes to human_required (LH execution model: 报红 gates completion).
        self.value_gate = value_gate
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
            external = self.controller.resume_external(verdict_store=verdict_store, source=conclusion_source)
        auto_merge = []
        if self.merge_gate is not None and external:
            # B13: human merges found on poll feed the trust ramp; runs that
            # resolved verified-with-success go through the gate's conditions.
            auto_merge = self.merge_gate.on_poll_resolved(external, at=time.time())
        terminal_before = self._reduce_one_terminal_run()
        event_result = self._process_one_event(holder)
        run_result = self._dispatch_one_run(holder, model, turning_point=turning_point, verdict_store=verdict_store)
        terminal_after = self._reduce_run_result(run_result) if run_result and run_result.get("status") in {"verified", "stopped"} else None
        campaign_stops = self._campaign_failure_lines()
        progressed = any(item is not None and item != [] for item in (standing, startup, external, auto_merge, terminal_before, event_result, run_result, terminal_after, campaign_stops))
        return {
            "status": "progress" if progressed else "idle",
            "standing_emitted": standing,
            "startup_reconciled": startup,
            "external_resumed": external,
            "auto_merge": auto_merge,
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
            episode_anchor = "initial"
            for goal in outcomes:
                if goal["state"] == "completed":
                    consecutive = 0
                    episode_anchor = f"{goal['goal_id']}:{float(goal.get('updated_at') or 0):.9f}"
                else:
                    consecutive += 1
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
            for goal in self.goal_store.active_goals(campaign_id=campaign_id):
                self.goal_store.transition_goal(goal["goal_id"], "human_required", expected_state="active")
                routed.append(goal["goal_id"])
            for goal in self.goal_store.goals_in_state("candidate"):
                if goal.get("campaign_id") != campaign_id:
                    continue
                self.goal_store.transition_goal(goal["goal_id"], "human_required", expected_state="candidate")
                routed.append(goal["goal_id"])
            payload = {
                "campaign_id": campaign_id,
                "consecutive_failures": consecutive,
                "threshold": threshold,
                "episode_id": episode_id,
                "routed_goal_ids": sorted(routed),
            }
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

    def _process_one_event(self, holder: str) -> dict[str, Any] | None:
        for event in self.goal_store.pending_events():
            if not self.goal_store.claim_event(event["event_key"], holder):
                continue
            try:
                return self._process_event(event)
            finally:
                self.goal_store.release_event(event["event_key"], holder)
        return None

    def _process_control_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Durably acknowledge SH context control without creating work.

        Host/provider handoff remains Orca-owned. LH records the policy result,
        accepts bounded host evidence, and only permits the old-session stop
        transition after a durable successor heartbeat event.
        """
        event_key = event["event_key"]
        event_type = event["event_type"]
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        base = {
            "schema": "lh-rollover-control-ack/v1",
            "event_key": event_key,
            "control_event": event_type,
            "project_id": payload.get("project_id"),
            "campaign_id": payload.get("campaign_id"),
            "correlation_id": payload.get("correlation_id"),
            "payload_digest": event.get("payload_digest"),
            "context_ratio": payload.get("context_ratio"),
            "goal_created": False,
            "run_created": False,
            "old_session_stopped": False,
            "successor_heartbeat": "not_observed",
        }

        def control_receipt(status: str, evidence: dict[str, Any]) -> dict[str, Any]:
            body = {
                "schema": "lh-rollover-control-receipt/v2",
                "event_key": event_key,
                "event_type": event_type,
                "project_id": payload.get("project_id"),
                "campaign_id": payload.get("campaign_id"),
                "correlation_id": payload.get("correlation_id"),
                "payload_digest": event.get("payload_digest"),
                "status": status,
                "evidence": evidence,
            }
            digest = "sha256:" + hashlib.sha256(
                json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            return {
                "schema": "lh-rollover-control-receipt/v2",
                "ref": f"goal_event://{event_key}",
                "digest": digest,
                "status": status,
                "event_key": event_key,
                "event_type": event_type,
                "project_id": payload.get("project_id"),
                "campaign_id": payload.get("campaign_id"),
                "correlation_id": payload.get("correlation_id"),
                "payload_digest": event.get("payload_digest"),
                "evidence_digest": "sha256:" + hashlib.sha256(
                    json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            }

        if event_type == "context_pressure":
            result = {
                **base,
                "status": "context_pressure_ack",
                "action": "notify_only",
                "checkpoint": {"status": "not_requested"},
            }
            result["receipt"] = control_receipt("context_pressure_ack", {"action": "notify_only"})
            self.goal_store.transition_event(event_key, "completed", result=result)
            return result

        if event_type == "successor_heartbeat":
            rollover_key = payload.get("rollover_event_key")
            try:
                rollover = self.goal_store.get_event(str(rollover_key))
            except (KeyError, TypeError):
                rollover = None
            rollover_result = rollover.get("result") if isinstance(rollover, dict) and isinstance(rollover.get("result"), dict) else {}
            if rollover is None or rollover_result.get("status") != "rollover_accepted":
                result = {
                    **base,
                    "status": "human_required",
                    "reason": "rollover_acceptance_missing",
                    "action": "host_handoff_blocked",
                    "old_session_stop_allowed": False,
                }
                result["receipt"] = control_receipt("human_required", {"reason": result["reason"]})
                self.goal_store.transition_event(event_key, "human_required", result=result)
                return result
            proof = payload.get("heartbeat_proof") if isinstance(payload.get("heartbeat_proof"), dict) else {}
            evidence = {
                "rollover_event_key": rollover_key,
                "heartbeat_id": payload.get("heartbeat_id"),
                "successor_handle": payload.get("successor_handle"),
                "provider": payload.get("provider"),
                "observed_at": payload.get("observed_at"),
                "output_digest": proof.get("output_digest"),
                "turn_completed": proof.get("turn_completed"),
                "identity_pair_digest": payload.get("identity_pair_digest"),
                "successor_identity_digest": payload.get("successor_identity_digest"),
            }
            result = {
                **base,
                "status": "successor_heartbeat_observed",
                "action": "ready_for_old_session_stop",
                "successor_heartbeat": "observed",
                "old_session_stop_allowed": True,
                "checkpoint": {
                    "status": "control_acknowledged",
                    "next_action": "stop predecessor only after this receipt is read back",
                },
                "heartbeat": evidence,
            }
            result["receipt"] = control_receipt("successor_heartbeat_observed", evidence)
            self.goal_store.transition_event(event_key, "completed", result=result)
            return result

        if event_type == "rollover_finalized":
            heartbeat_key = payload.get("heartbeat_event_key")
            try:
                heartbeat = self.goal_store.get_event(str(heartbeat_key))
            except (KeyError, TypeError):
                heartbeat = None
            heartbeat_result = heartbeat.get("result") if isinstance(heartbeat, dict) and isinstance(heartbeat.get("result"), dict) else {}
            if heartbeat is None or heartbeat_result.get("status") != "successor_heartbeat_observed":
                result = {
                    **base,
                    "status": "human_required",
                    "reason": "successor_heartbeat_required_before_stop",
                    "action": "old_session_stop_blocked",
                    "old_session_stop_allowed": False,
                }
                result["receipt"] = control_receipt("human_required", {"reason": result["reason"]})
                self.goal_store.transition_event(event_key, "human_required", result=result)
                return result
            evidence = {
                "heartbeat_event_key": heartbeat_key,
                "old_session_handle": payload.get("old_session_handle"),
                "old_session_stopped": payload.get("old_session_stopped"),
                "checkpoint_digest": payload.get("checkpoint_digest"),
                "next_action_digest": payload.get("next_action_digest"),
                "stop_evidence": payload.get("stop_evidence"),
                "identity_pair_digest": payload.get("identity_pair_digest"),
                "routing_switch_digest": payload.get("routing_switch_digest"),
                "successor_identity_digest": payload.get("successor_identity_digest"),
                "predecessor_identity_digest": payload.get("predecessor_identity_digest"),
                "post_close_digest": payload.get("post_close_digest"),
                "transaction_path": payload.get("transaction_path"),
            }
            result = {
                **base,
                "status": "rollover_finalized",
                "action": "handoff_complete",
                "successor_heartbeat": "observed",
                "old_session_stopped": True,
                "old_session_stop_allowed": True,
                "checkpoint": {"status": "handoff_complete", "digest": payload.get("checkpoint_digest")},
                "stop": evidence,
            }
            result["receipt"] = control_receipt("rollover_finalized", evidence)
            self.goal_store.transition_event(event_key, "completed", result=result)
            return result

        packet = payload.get("handoff_packet")
        safe_point = payload.get("safe_point_observed") is True
        if not safe_point:
            result = {
                **base,
                "status": "human_required",
                "reason": "safe_point_required",
                "action": "host_handoff_blocked",
                "checkpoint": {"status": "not_started"},
                "handoff_packet_present": isinstance(packet, dict),
            }
            self.goal_store.transition_event(event_key, "human_required", result=result)
            return result
        if not isinstance(packet, dict) or packet.get("schema") != "external-hub-handoff-packet/v1":
            result = {
                **base,
                "status": "human_required",
                "reason": "handoff_packet_invalid",
                "action": "host_handoff_blocked",
                "checkpoint": {"status": "not_started"},
            }
            self.goal_store.transition_event(event_key, "human_required", result=result)
            return result
        result = {
            **base,
            "status": "rollover_accepted",
            "action": "host_handoff_pending",
            "checkpoint": {
                "status": "control_acknowledged",
                "park_state": "not_claimed",
                "proof": "LH has not stopped or parked a host session in this bounded node",
            },
            "handoff_packet_present": True,
            "successor_heartbeat_required": True,
        }
        predecessor = packet.get("predecessor_identity") if isinstance(packet.get("predecessor_identity"), dict) else {}
        result["receipt"] = control_receipt(
            "rollover_accepted",
            {
                "handoff_packet_present": True,
                "safe_point_observed": True,
                "predecessor_identity_digest": predecessor.get("identity_digest"),
                "checkpoint_digest": packet.get("checkpoint_digest"),
            },
        )
        self.goal_store.transition_event(event_key, "completed", result=result)
        return result

    def _process_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_key = event["event_key"]
        if event["event_type"] in {"context_pressure", "rollover_requested", "successor_heartbeat", "rollover_finalized"}:
            return self._process_control_event(event)
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
                # terminal goals (stopped/completed) revive as candidates;
                # anything else (active/conflicting payload) is a human decision.
                try:
                    existing = self.goal_store.get_goal(candidate["goal_id"])
                except KeyError:
                    existing = None
                if existing is not None and existing["state"] in {"stopped", "completed"}:
                    # Re-issued command for a terminal goal: revive it as a
                    # candidate; admission decides revision-bump vs cap.
                    # completed mirrors stopped (daily standing intents recur
                    # after a successful cycle — the fresh run comes from the
                    # W9g verified-run bump at admission).
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
            if self.value_gate:
                verdict = value_reducer.verdict_for_run(self.run_store, run_id)
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
            completion = {
                "campaign_id": goal["campaign_id"],
                "stage_id": goal["stage_id"],
                "receipt_id": receipt_meta["receipt_digest"],
                "verification": receipt.get("verification"),
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


def process_control_event_by_key(
    goal_store: GoalStore,
    *,
    event_key: str,
    holder: str,
) -> dict[str, Any]:
    """Process exactly one named control event without starting the Goal loop.

    This foreground seam exists for operator windows where resident workers are
    intentionally stopped. It never scans pending events, dispatches a run,
    invokes a provider, or processes a non-control event.
    """
    event = goal_store.get_event(event_key)
    control_types = {
        "context_pressure",
        "rollover_requested",
        "successor_heartbeat",
        "rollover_finalized",
    }
    if event.get("event_type") not in control_types:
        raise ValueError("foreground processor only accepts LH control events")
    if event.get("state") != "event_received":
        return {
            "status": "reused" if event.get("state") in {"completed", "human_required"} else "rejected",
            "event_key": event_key,
            "event_state": event.get("state"),
            "result": event.get("result"),
        }
    if not goal_store.claim_event(event_key, holder):
        return {
            "status": "busy",
            "event_key": event_key,
            "event_state": goal_store.get_event(event_key).get("state"),
        }
    try:
        processor = object.__new__(GoalLoopWorker)
        processor.goal_store = goal_store
        result = processor._process_control_event(event)
        return {
            "status": "processed",
            "event_key": event_key,
            "event_state": goal_store.get_event(event_key).get("state"),
            "result": result,
        }
    finally:
        goal_store.release_event(event_key, holder)

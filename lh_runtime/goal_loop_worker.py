"""One serial G5 worker that closes the provider-free Goal loop."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import external_action_port as eap
import external_verdict as ev
import grill_loop
import turning_point as tp
import value_reducer
from admission_bridge import GoalAdmissionBridge
from campaign_compiler import GOAL_CANDIDATE_SCHEMA, CampaignCompiler
from command_ingress import submit_command
from controller import LoopController
from goal_matcher import GoalMatcher
from goal_store import GoalStore
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
        terminal_before = self._reduce_one_terminal_run()
        event_result = self._process_one_event(holder)
        run_result = self._dispatch_one_run(holder, model, turning_point=turning_point, verdict_store=verdict_store)
        terminal_after = self._reduce_run_result(run_result) if run_result and run_result.get("status") in {"verified", "stopped"} else None
        campaign_stops = self._campaign_failure_lines()
        progressed = any(item is not None and item != [] for item in (standing, startup, external, terminal_before, event_result, run_result, terminal_after, campaign_stops))
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
        one batch and a durable event records the stop. The line fires once
        per campaign (idempotent event key).
        """
        stops: list[dict[str, Any]] = []
        for campaign_id, compiler in self.compilers.items():
            threshold = int(getattr(compiler, "failure_stop_threshold", 3))
            event_key = f"campaign-failure-line:{campaign_id}"
            try:
                self.goal_store.get_event(event_key)
                continue  # the line already fired for this campaign
            except KeyError:
                pass
            outcomes = [
                goal
                for state in ("human_required", "stopped", "completed")
                for goal in self.goal_store.goals_in_state(state)
                if goal.get("campaign_id") == campaign_id
            ]
            outcomes.sort(key=lambda goal: (float(goal.get("updated_at") or 0), goal["goal_id"]))
            consecutive = 0
            for goal in outcomes:
                consecutive = 0 if goal["state"] == "completed" else consecutive + 1
            if consecutive < threshold:
                continue
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
            stops.append(payload)
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

    def _process_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_key = event["event_key"]
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
            grill_note, grill_route = self._grill_before_final_attempt(run, goal_id)
            if grill_route is not None:
                return grill_route
            return self.controller.tick(run["run_id"], holder=holder, model=model, verifier_argv=verifier_argv, grill_note=grill_note)
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
                action_id=external["action_id"].strip(),
            )
        result = {"status": "human_required", "run_id": run["run_id"], "reason": "durable verification plan is missing"}
        self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
        return result

    def _grill_before_final_attempt(
        self,
        run: dict[str, Any],
        goal_id: str,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """W6a challenger grill before a sync run's last allowed attempt.

        Returns ``(note, route)``: ``note`` is the diagnosis to inject into the
        final attempt's capsule; ``route`` short-circuits the dispatch when the
        grill judged the goal broken. Every degrade — no runner configured, a
        judge that raises, output outside the closed set — returns
        ``(None, None)`` so the original final-attempt dispatch proceeds
        unchanged. Advisory only: the grill never touches the lamp, the scope,
        or the goal content, and its output is never an acceptance authority.
        """
        if self.grill_runner is None or not grill_loop.should_grill(run):
            return None, None
        snapshot = grill_loop.build_snapshot(self.run_store, run)
        try:
            raw = self.grill_runner(snapshot)
        except Exception:  # a failing judge must never stop the loop
            return None, None
        decision = grill_loop.validate_decision(raw)
        if decision["type"] == "reject":
            return None, None
        evidence = {"decision": decision["type"], "diagnosis": decision["diagnosis"], "attempts_used": int(run["attempts"])}
        self.run_store.append_event(run["run_id"], "grill_decision", evidence)
        if decision["type"] == "goal-broken":
            self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
            event = self.goal_store.record_event(
                event_id=f"grill-goal-broken:{run['run_id']}",
                idempotency_key=f"grill-goal-broken:{run['run_id']}",
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
                    self.goal_store.transition_goal(goal_id, "human_required", expected_state="active")
                    red_event = self.goal_store.record_event(
                        event_id=f"value-red:{run_id}:{receipt_meta['receipt_digest']}",
                        idempotency_key=f"value-red:{run_id}:{receipt_meta['receipt_digest']}",
                        source="value_reducer",
                        event_type="human_required",
                        payload={"run_id": run_id, "goal_id": goal_id, "value_verdict": verdict},
                    )
                    self.goal_store.transition_event(red_event["event_key"], "human_required", result=verdict)
                    return {"status": "value_red_human_required", "run_id": run_id, "event_key": red_event["event_key"], "reasons": verdict["reasons"]}
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
                return {"status": "completed_with_next_event", "run_id": run_id, "derived_event_key": stored["event_key"]}
            if advanced["status"] == "completed":
                self.goal_store.transition_goal(goal_id, "completed", expected_state="active")
                return {"status": "completed", "run_id": run_id}
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

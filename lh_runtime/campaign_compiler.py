"""Compile an approved campaign into a deterministic admission envelope."""
from __future__ import annotations

import hashlib
import json
from typing import Any


CAMPAIGN_SCHEMA = "lh-campaign/v1"
ENVELOPE_SCHEMA = "lh-campaign-admission-envelope/v1"
GOAL_CANDIDATE_SCHEMA = "lh-goal-candidate/v1"
FORBIDDEN_SIDE_EFFECTS = {"push", "merge", "publish", "external_action", "credential"}


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _id(prefix: str, value: Any) -> str:
    return prefix + hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:32]


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _strings(name: str, value: Any, *, required: bool = True) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    if required and not value:
        raise ValueError(f"{name} must not be empty")
    return [item.strip() for item in value]


class CampaignCompiler:
    """Pure compiler plus deterministic stage-completion reducer.

    The compiler consumes an already approved structured campaign.  It never
    reads Markdown, invokes a model, performs admission, or creates a run.
    """

    def __init__(self, campaign: dict[str, Any]):
        if not isinstance(campaign, dict) or campaign.get("schema") != CAMPAIGN_SCHEMA:
            raise ValueError(f"campaign.schema must be {CAMPAIGN_SCHEMA}")
        self.campaign_id = _text("campaign_id", campaign.get("campaign_id"))
        threshold = campaign.get("failure_stop_threshold", 3)
        if not isinstance(threshold, int) or isinstance(threshold, bool) or not 3 <= threshold <= 5:
            raise ValueError("campaign.failure_stop_threshold must be an integer from 3 to 5")
        # W6b: consecutive per-campaign goal failures that route the whole
        # campaign to a human. Top level because the line spans stages.
        self.failure_stop_threshold = threshold
        raw_stages = campaign.get("stages")
        if not isinstance(raw_stages, list) or not raw_stages:
            raise ValueError("campaign.stages must be a non-empty list")
        self.stages = {self._stage_id(stage): self._compile_stage(stage) for stage in raw_stages}
        if len(self.stages) != len(raw_stages):
            raise ValueError("campaign stage_id values must be unique")
        for stage in self.stages.values():
            next_stage_id = stage["next_stage_id"]
            if next_stage_id is not None and next_stage_id not in self.stages:
                raise ValueError(f"unknown next_stage_id: {next_stage_id}")
        raw_standing = campaign.get("standing_intents")
        if raw_standing is None:
            self.standing_intents: list[dict[str, str]] = []
        else:
            if not isinstance(raw_standing, list):
                raise ValueError("campaign.standing_intents must be a list")
            # W9f: recurring deterministic intents, compiled after the stages
            # so each reference is checked against the compiled admission.
            self.standing_intents = [self._compile_standing_intent(item) for item in raw_standing]
        self.envelope = {
            "schema": ENVELOPE_SCHEMA,
            "campaign_id": self.campaign_id,
            "stages": self.stages,
        }
        self.envelope["digest"] = _digest(self.envelope)

    @staticmethod
    def _stage_id(stage: Any) -> str:
        if not isinstance(stage, dict):
            raise ValueError("each campaign stage must be an object")
        return _text("stage_id", stage.get("stage_id"))

    def _compile_standing_intent(self, item: Any) -> dict[str, str]:
        """Validate one standing_intents entry: a recurring manual_intent the
        worker emits once per UTC day. Interval is daily only (finer intervals
        are a human decision); the stage must exist and be auto-admissible."""
        if not isinstance(item, dict):
            raise ValueError("campaign.standing_intents entries must be objects")
        stage_id = _text("standing_intents.stage_id", item.get("stage_id"))
        if item.get("interval") != "daily":
            raise ValueError(f"{stage_id}: standing_intents.interval must be 'daily'")
        intent = _text(f"{stage_id}.standing_intents.intent", item.get("intent"))
        stage = self.stages.get(stage_id)
        if stage is None:
            raise ValueError(f"standing_intents references unknown stage_id: {stage_id}")
        if not stage["auto_admission"]["eligible"]:
            raise ValueError(f"standing_intents stage is not auto-admissible: {stage_id}")
        return {"stage_id": stage_id, "interval": "daily", "intent": intent}

    def _compile_stage(self, stage: dict[str, Any]) -> dict[str, Any]:
        stage_id = self._stage_id(stage)
        goal = stage.get("goal")
        if not isinstance(goal, dict) or not goal:
            raise ValueError(f"{stage_id}: goal must be a non-empty object")
        allowed_paths = _strings(f"{stage_id}.allowed_paths", stage.get("allowed_paths"))
        allowed_side_effects = _strings(f"{stage_id}.allowed_side_effects", stage.get("allowed_side_effects"), required=False)
        max_attempts = stage.get("max_attempts", 4)
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or not 1 <= max_attempts <= 4:
            raise ValueError(f"{stage_id}.max_attempts must be an integer from 1 to 4")
        human_only = stage.get("human_only", False)
        if not isinstance(human_only, bool):
            raise ValueError(f"{stage_id}.human_only must be boolean")
        lamp = stage.get("acceptance_lamp")
        if lamp is not None:
            if not isinstance(lamp, dict):
                raise ValueError(f"{stage_id}.acceptance_lamp must be an object")
            lamp = {
                "id": _text(f"{stage_id}.acceptance_lamp.id", lamp.get("id")),
                "smoke": _text(f"{stage_id}.acceptance_lamp.smoke", lamp.get("smoke")),
                "verification_argv": _strings(f"{stage_id}.acceptance_lamp.verification_argv", lamp.get("verification_argv")),
            }
        reasons: list[str] = []
        if human_only:
            reasons.append("human_only_stage")
        # W2 contract surface: a stage may declare an external async verdict
        # instead of a local lamp. The compiled envelope carries it through so
        # admission (which waives the lamp for a well-formed external_verdict)
        # and worker dispatch can see it; without this pass-through the declared
        # field was silently dropped at compile time.
        external_verdict = stage.get("external_verdict")
        if external_verdict is not None:
            if not isinstance(external_verdict, dict) or not isinstance(external_verdict.get("action_id"), str) or not external_verdict["action_id"].strip():
                raise ValueError(f"{stage_id}.external_verdict must be an object with a non-empty action_id")
            external_verdict = {"action_id": external_verdict["action_id"].strip()}
        if lamp is None and external_verdict is None:
            reasons.append("missing_acceptance_lamp")
        forbidden = sorted(set(allowed_side_effects) & FORBIDDEN_SIDE_EFFECTS)
        if forbidden:
            reasons.append("forbidden_side_effect:" + ",".join(forbidden))
        compiled = {
            "schema": ENVELOPE_SCHEMA,
            "campaign_id": self.campaign_id,
            "stage_id": stage_id,
            "goal": goal,
            "allowed_paths": allowed_paths,
            "allowed_side_effects": allowed_side_effects,
            "acceptance_lamp": lamp,
            "human_only": human_only,
            "max_attempts": max_attempts,
            "next_stage_id": stage.get("next_stage_id"),
            "auto_admission": {"eligible": not reasons, "reasons": reasons},
        }
        if external_verdict is not None:
            compiled["external_verdict"] = external_verdict
        return compiled

    def compile(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.envelope, ensure_ascii=False, sort_keys=True))

    def advance(self, completion: dict[str, Any]) -> dict[str, Any]:
        """Turn one verified deterministic stage receipt into one next candidate."""
        if not isinstance(completion, dict):
            raise ValueError("completion must be an object")
        if completion.get("campaign_id") != self.campaign_id:
            raise ValueError("completion campaign_id does not match compiled campaign")
        stage_id = _text("completion.stage_id", completion.get("stage_id"))
        receipt_id = _text("completion.receipt_id", completion.get("receipt_id"))
        stage = self.stages.get(stage_id)
        if stage is None:
            raise ValueError(f"unknown completion stage_id: {stage_id}")
        if not stage["auto_admission"]["eligible"]:
            return {"status": "human_required", "reason": ";".join(stage["auto_admission"]["reasons"]), "event": None}
        verification = completion.get("verification")
        if not isinstance(verification, dict) or verification.get("exit_code") != 0:
            return {"status": "human_required", "reason": "acceptance_lamp_not_green", "event": None}
        next_stage_id = stage["next_stage_id"]
        if next_stage_id is None:
            return {"status": "completed", "reason": "campaign_has_no_next_stage", "event": None}
        next_stage = self.stages[next_stage_id]
        if not next_stage["auto_admission"]["eligible"]:
            return {"status": "human_required", "reason": ";".join(next_stage["auto_admission"]["reasons"]), "event": None}
        event_key = f"stage-completion:{self.campaign_id}:{stage_id}:{receipt_id}"
        candidate_goal_id = f"{self.campaign_id}:{next_stage_id}"
        candidate = {
            "schema": GOAL_CANDIDATE_SCHEMA,
            "goal_id": candidate_goal_id,
            "campaign_id": self.campaign_id,
            "stage_id": next_stage_id,
            "goal": {
                "feature_contract": next_stage["goal"],
                "admission_envelope": next_stage,
            },
        }
        event = {
            "event_id": _id("evt-", event_key),
            "idempotency_key": event_key,
            "source": "stage_completion",
            "event_type": "verified_stage",
            "payload": {
                "campaign_id": self.campaign_id,
                "completed_stage_id": stage_id,
                "receipt_id": receipt_id,
                "verification": verification,
                "candidate": candidate,
            },
        }
        return {
            "status": "candidate_ready",
            "event": event,
            "candidate": candidate,
            "candidate_key": _id("candidate-", event_key),
        }

"""Capability-based resource selection for one LH execution graph."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

GRAPH_SCHEMA = "lh-capability-routing/v1"
WORK_GRAPH_SCHEMA = "lh-capability-work-graph/v1"
ROUTING_AUTHORITY_SCHEMA = "lh-routing-authority/v1"
BINDING_SCHEMA = "lh-attempt-binding/v1"
OPERATIONS = {"produce_change", "evaluate_transition"}
FILESYSTEM_LEVELS = {"read_only": 0, "workspace_write": 1}
NETWORK_LEVELS = {"none": 0, "allowlisted": 1, "external": 2}
DATA_LEVELS = {"local": 0, "approved_region": 1, "external": 2}
TRUST_LEVELS = {"claimed": 0, "process_bound": 1}
INDEPENDENCE_LEVELS = {"none", "context", "model_family"}
HEALTH_STATES = {"healthy", "degraded", "unavailable", "unknown"}
FALLBACK_POLICIES = {"stop", "next_eligible", "human_required"}
RETRY_POLICIES = {"same_best", "next_eligible"}
_PROFILE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
IDENTITY_KEYS = {
    "executor", "judge", "judge_model", "model", "preferred_model", "provider",
    "runner", "writer_model", "checker_model", "challenger_model",
}


class ResolutionError(ValueError):
    def __init__(self, message: str, *, route: str = "stop"):
        super().__init__(message)
        self.route = route


def digest_json(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _digest(name: str, value: Any) -> str:
    digest = _text(name, value)
    if not digest.startswith("sha256:") or len(digest) != 71:
        raise ValueError(f"{name} must be a sha256 digest")
    try:
        int(digest.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise ValueError(f"{name} must be a sha256 digest") from exc
    return digest


def _instant(name: str, value: Any) -> tuple[str, datetime]:
    text = _text(name, value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must carry a timezone")
    return text, parsed.astimezone(timezone.utc)


def _strings(name: str, value: Any, *, required: bool = False) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    if required and not value:
        raise ValueError(f"{name} must not be empty")
    return [item.strip() for item in value]


def _number(name: str, value: Any, *, minimum: float = 0.0) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or float(value) < minimum:
        raise ValueError(f"{name} must be a number >= {minimum}")
    return float(value)


def _enum(name: str, value: Any, allowed: dict[str, int] | set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}")
    return value


def _reject_identity_keys(value: Any, *, path: str) -> None:
    if isinstance(value, dict):
        forbidden = sorted(str(key) for key in value if key in IDENTITY_KEYS)
        if forbidden:
            raise ValueError(f"{path} contains executor identity field(s): {forbidden}")
        for key, child in value.items():
            _reject_identity_keys(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_identity_keys(child, path=f"{path}[{index}]")


def _validate_authority(node_id: str, inputs: Any) -> dict[str, str]:
    if not isinstance(inputs, dict) or set(inputs) != {"authority_ref", "authority_digest"}:
        raise ValueError(f"{node_id}.inputs must contain exactly authority_ref and authority_digest")
    authority_ref = _text(f"{node_id}.inputs.authority_ref", inputs.get("authority_ref"))
    authority_digest = _digest(
        f"{node_id}.inputs.authority_digest",
        inputs.get("authority_digest"),
    )
    return {"authority_ref": authority_ref, "authority_digest": authority_digest}


def _validate_node(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("execution_graph.nodes entries must be objects")
    _reject_identity_keys(raw, path="execution_graph.nodes")
    node_id = _text("node.node_id", raw.get("node_id"))
    operation = _enum(f"{node_id}.operation", raw.get("operation"), OPERATIONS)
    permissions = raw.get("permissions")
    if not isinstance(permissions, dict):
        raise ValueError(f"{node_id}.permissions must be an object")
    filesystem = _enum(f"{node_id}.permissions.filesystem", permissions.get("filesystem"), FILESYSTEM_LEVELS)
    network = _enum(f"{node_id}.permissions.network", permissions.get("network"), NETWORK_LEVELS)
    independence = raw.get("independence", {})
    if not isinstance(independence, dict):
        raise ValueError(f"{node_id}.independence must be an object")
    minimum_level = _enum(
        f"{node_id}.independence.minimum_level",
        independence.get("minimum_level", "none"),
        INDEPENDENCE_LEVELS,
    )
    fallback_policy = _enum(
        f"{node_id}.fallback_policy",
        raw.get("fallback_policy", "stop"),
        FALLBACK_POLICIES,
    )
    budget = raw.get("budget", {})
    if not isinstance(budget, dict):
        raise ValueError(f"{node_id}.budget must be an object")
    acceptance = raw.get("acceptance", {})
    if not isinstance(acceptance, dict):
        raise ValueError(f"{node_id}.acceptance must be an object")
    normalized_budget = {
        "max_wall_seconds": _number(f"{node_id}.budget.max_wall_seconds", budget.get("max_wall_seconds", 900), minimum=1),
        "max_uncached_input_tokens": int(_number(
            f"{node_id}.budget.max_uncached_input_tokens",
            budget.get("max_uncached_input_tokens", 0),
        )),
        "max_output_tokens": int(_number(f"{node_id}.budget.max_output_tokens", budget.get("max_output_tokens", 0))),
    }
    return {
        "node_id": node_id,
        "operation": operation,
        "required_capabilities": _strings(
            f"{node_id}.required_capabilities", raw.get("required_capabilities"), required=True,
        ),
        "required_tools": _strings(f"{node_id}.required_tools", raw.get("required_tools", [])),
        "permissions": {"filesystem": filesystem, "network": network},
        "data_boundary": _enum(f"{node_id}.data_boundary", raw.get("data_boundary", "external"), DATA_LEVELS),
        "minimum_context": int(_number(f"{node_id}.minimum_context", raw.get("minimum_context", 0))),
        "minimum_trust": _enum(
            f"{node_id}.minimum_trust", raw.get("minimum_trust", "claimed"), TRUST_LEVELS,
        ),
        "inputs": _validate_authority(node_id, raw.get("inputs")),
        "acceptance": acceptance,
        "independence": {
            "from_nodes": _strings(f"{node_id}.independence.from_nodes", independence.get("from_nodes", [])),
            "minimum_level": minimum_level,
        },
        "budget": normalized_budget,
        "fallback_policy": fallback_policy,
    }


def _validate_resource_evidence(
    raw: Any,
    *,
    binding_id: str,
    health: str,
    eval_revision: str,
    at: datetime | None,
    required: bool,
) -> dict[str, Any]:
    health_raw = raw.get("health_evidence") if isinstance(raw, dict) else None
    score_raw = raw.get("score_evidence") if isinstance(raw, dict) else None
    if not required and health_raw is None and score_raw is None:
        return {}
    if not isinstance(health_raw, dict):
        raise ValueError(f"{binding_id}.health_evidence must be an object")
    if not isinstance(score_raw, dict):
        raise ValueError(f"{binding_id}.score_evidence must be an object")
    health_status = _enum(
        f"{binding_id}.health_evidence.status",
        health_raw.get("status"),
        HEALTH_STATES,
    )
    if health_status != health:
        raise ValueError(f"{binding_id}.health_evidence.status must match health")
    health_observed, health_observed_at = _instant(
        f"{binding_id}.health_evidence.observed_at",
        health_raw.get("observed_at"),
    )
    health_valid, health_valid_until = _instant(
        f"{binding_id}.health_evidence.valid_until",
        health_raw.get("valid_until"),
    )
    if health_valid_until <= health_observed_at:
        raise ValueError(f"{binding_id}.health_evidence.valid_until must follow observed_at")
    if at is not None and health_valid_until <= at:
        raise ValueError(f"{binding_id}.health_evidence is stale")
    score_eval = _text(
        f"{binding_id}.score_evidence.eval_revision",
        score_raw.get("eval_revision"),
    )
    if score_eval != eval_revision:
        raise ValueError(f"{binding_id}.score_evidence.eval_revision must match eval_revision")
    score_observed, _score_observed_at = _instant(
        f"{binding_id}.score_evidence.observed_at",
        score_raw.get("observed_at"),
    )
    return {
        "health_evidence": {
            "status": health_status,
            "observed_at": health_observed,
            "valid_until": health_valid,
            "source_ref": _text(
                f"{binding_id}.health_evidence.source_ref",
                health_raw.get("source_ref"),
            ),
            "digest": _digest(
                f"{binding_id}.health_evidence.digest",
                health_raw.get("digest"),
            ),
        },
        "score_evidence": {
            "eval_revision": score_eval,
            "observed_at": score_observed,
            "source_ref": _text(
                f"{binding_id}.score_evidence.source_ref",
                score_raw.get("source_ref"),
            ),
            "digest": _digest(
                f"{binding_id}.score_evidence.digest",
                score_raw.get("digest"),
            ),
        },
    }


def _validate_resource(
    raw: Any,
    *,
    require_evidence: bool = False,
    at: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("execution_graph.registry.resources entries must be objects")
    binding_id = _text("resource.binding_id", raw.get("binding_id"))
    executor_kind = _enum(
        f"{binding_id}.executor_kind",
        raw.get("executor_kind"),
        {"model"},
    )
    runner = raw.get("runner")
    if executor_kind == "model":
        runner = _text(f"{binding_id}.runner", runner)
    elif runner is not None:
        runner = _text(f"{binding_id}.runner", runner)
    scores = raw.get("scores", {})
    if not isinstance(scores, dict):
        raise ValueError(f"{binding_id}.scores must be an object")
    provider_binding = raw.get("provider_binding")
    if provider_binding is not None:
        if not isinstance(provider_binding, dict) or set(provider_binding) != {"runner", "base_url", "model"}:
            raise ValueError(f"{binding_id}.provider_binding must contain runner, base_url, and model")
        provider_binding = {
            key: _text(f"{binding_id}.provider_binding.{key}", provider_binding.get(key))
            for key in ("runner", "base_url", "model")
        }
        if _PROFILE_RE.fullmatch(provider_binding["runner"]) is None:
            raise ValueError(
                f"{binding_id}.provider_binding.runner must be an explicit provider adapter"
            )
        if provider_binding["runner"] != runner:
            raise ValueError(
                f"{binding_id}.provider_binding.runner must equal resource runner"
            )
        try:
            parsed = urlparse(provider_binding["base_url"])
            parsed_port = parsed.port
        except ValueError as exc:
            raise ValueError(
                f"{binding_id}.provider_binding.base_url must contain a valid "
                "host and port"
            ) from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or not parsed.hostname
            or (parsed_port is not None and parsed_port < 1)
            or any(character.isspace() for character in provider_binding["base_url"])
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                f"{binding_id}.provider_binding.base_url must be a "
                "credential-free absolute http(s) URL"
            )
        if any(character.isspace() for character in provider_binding["model"]):
            raise ValueError(
                f"{binding_id}.provider_binding.model must not contain whitespace"
            )
        provider_binding["base_url"] = provider_binding["base_url"].rstrip("/")
    model = raw.get("model")
    if model is not None:
        model = _text(f"{binding_id}.model", model)
        if provider_binding is not None:
            raise ValueError(
                f"{binding_id}.model and provider_binding are mutually exclusive"
            )
    model_identity = (
        provider_binding["model"]
        if isinstance(provider_binding, dict)
        else model or f"ambient:{runner}"
    )
    endpoint_ref = _text(f"{binding_id}.endpoint_ref", raw.get("endpoint_ref"))
    expected_endpoint_ref = (
        provider_binding["base_url"]
        if isinstance(provider_binding, dict)
        else f"ambient:{runner}"
    )
    if endpoint_ref != expected_endpoint_ref:
        raise ValueError(
            f"{binding_id}.endpoint_ref must equal the actuation endpoint {expected_endpoint_ref!r}"
        )
    model_family = _text(f"{binding_id}.model_family", raw.get("model_family"))
    if model_family != model_identity:
        raise ValueError(
            f"{binding_id}.model_family must equal the bound model identity {model_identity!r}"
        )
    derived_trust = "process_bound" if model is not None or provider_binding is not None else "claimed"
    declared_trust = _enum(
        f"{binding_id}.trust_tier",
        raw.get("trust_tier", "claimed"),
        TRUST_LEVELS,
    )
    if declared_trust != derived_trust:
        raise ValueError(
            f"{binding_id}.trust_tier must equal derived actuation trust {derived_trust!r}"
        )
    eval_revision = _text(f"{binding_id}.eval_revision", raw.get("eval_revision"))
    health = _enum(f"{binding_id}.health", raw.get("health"), HEALTH_STATES)
    normalized = {
        "binding_id": binding_id,
        "executor_kind": executor_kind,
        "runner": runner,
        "provider_ref": _text(
            f"{binding_id}.provider_ref", raw.get("provider_ref", "unattributed")
        ).removeprefix("declared:"),
        "model_family": model_family,
        "endpoint_ref": endpoint_ref,
        "capabilities": _strings(f"{binding_id}.capabilities", raw.get("capabilities"), required=True),
        "tools": _strings(f"{binding_id}.tools", raw.get("tools", [])),
        "permission_ceiling": _enum(
            f"{binding_id}.permission_ceiling", raw.get("permission_ceiling", "read_only"), FILESYSTEM_LEVELS,
        ),
        "network_access": _enum(
            f"{binding_id}.network_access", raw.get("network_access", "external"), NETWORK_LEVELS,
        ),
        "data_boundary": _enum(
            f"{binding_id}.data_boundary", raw.get("data_boundary", "external"), DATA_LEVELS,
        ),
        "context_limit": int(_number(f"{binding_id}.context_limit", raw.get("context_limit", 0))),
        "context_isolation": _enum(
            f"{binding_id}.context_isolation", raw.get("context_isolation", "fresh_process"),
            {"fresh_process", "shared"},
        ),
        "health": health,
        "trust_tier": derived_trust,
        "eval_revision": eval_revision,
        "scores": {
            "quality": _number(f"{binding_id}.scores.quality", scores.get("quality", 0)),
            "cost": _number(f"{binding_id}.scores.cost", scores.get("cost", 0)),
            "latency": _number(f"{binding_id}.scores.latency", scores.get("latency", 0)),
        },
        "model": model,
        "model_identity": model_identity,
        "provider_binding": provider_binding,
    }
    normalized.update(_validate_resource_evidence(
        raw,
        binding_id=binding_id,
        health=health,
        eval_revision=eval_revision,
        at=at,
        required=require_evidence,
    ))
    return normalized


def _validate_nodes(nodes_raw: Any) -> list[dict[str, Any]]:
    if not isinstance(nodes_raw, list) or not nodes_raw:
        raise ValueError("execution_graph.nodes must be a non-empty list")
    nodes = [_validate_node(item) for item in nodes_raw]
    if len({node["node_id"] for node in nodes}) != len(nodes):
        raise ValueError("execution_graph node_id values must be unique")
    if sum(node["operation"] == "produce_change" for node in nodes) != 1:
        raise ValueError("execution_graph must contain exactly one produce_change node")
    if sum(node["operation"] == "evaluate_transition" for node in nodes) > 1:
        raise ValueError("execution_graph may contain at most one evaluate_transition node")
    seen: set[str] = set()
    for node in nodes:
        references = set(node["independence"]["from_nodes"])
        if not references <= seen:
            raise ValueError(f"{node['node_id']}.independence.from_nodes must reference earlier nodes")
        seen.add(node["node_id"])
    return nodes


def _validate_policy(policy_raw: Any) -> dict[str, Any]:
    if not isinstance(policy_raw, dict):
        raise ValueError("execution_graph.policy must be an object")
    weights = policy_raw.get("weights", {})
    if not isinstance(weights, dict):
        raise ValueError("execution_graph.policy.weights must be an object")
    policy = {
        "revision": _text("execution_graph.policy.revision", policy_raw.get("revision")),
        "weights": {
            "quality": _number("policy.weights.quality", weights.get("quality", 1)),
            "cost": _number("policy.weights.cost", weights.get("cost", 1)),
            "latency": _number("policy.weights.latency", weights.get("latency", 1)),
        },
        "allow_degraded": policy_raw.get("allow_degraded", False),
        "retry": _enum("execution_graph.policy.retry", policy_raw.get("retry", "same_best"), RETRY_POLICIES),
    }
    if not isinstance(policy["allow_degraded"], bool):
        raise ValueError("execution_graph.policy.allow_degraded must be boolean")
    for field in (
        "owner",
        "approved_at",
        "rollback_revision",
        "evidence_ref",
        "evidence_digest",
    ):
        if field in policy_raw:
            policy[field] = (
                _digest(f"execution_graph.policy.{field}", policy_raw[field])
                if field == "evidence_digest"
                else _text(f"execution_graph.policy.{field}", policy_raw[field])
            )
    if "adaptive" in policy_raw:
        if not isinstance(policy_raw["adaptive"], dict):
            raise ValueError("execution_graph.policy.adaptive must be an object")
        policy["adaptive"] = dict(policy_raw["adaptive"])
    return policy


def validate_graph(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("schema") != GRAPH_SCHEMA:
        raise ValueError(f"execution_graph.schema must be {GRAPH_SCHEMA}")
    registry_raw = raw.get("registry")
    if not isinstance(registry_raw, dict) or not isinstance(registry_raw.get("resources"), list):
        raise ValueError("execution_graph.registry.resources must be a list")
    nodes = _validate_nodes(raw.get("nodes"))
    resources = [_validate_resource(item) for item in registry_raw["resources"]]
    if len({resource["binding_id"] for resource in resources}) != len(resources):
        raise ValueError("execution_graph binding_id values must be unique")
    registry = {
        "revision": _text("execution_graph.registry.revision", registry_raw.get("revision")),
        "resources": resources,
    }
    for field in (
        "owner",
        "valid_until",
        "previous_revision",
        "profile_id",
        "authority_digest",
    ):
        if field in registry_raw:
            registry[field] = (
                _digest(f"execution_graph.registry.{field}", registry_raw[field])
                if field == "authority_digest"
                else _text(f"execution_graph.registry.{field}", registry_raw[field])
            )
    normalized = {
        "schema": GRAPH_SCHEMA,
        "nodes": nodes,
        "registry": registry,
        "policy": _validate_policy(raw.get("policy")),
    }
    if "routing_profile" in raw:
        normalized["routing_profile"] = _text("execution_graph.routing_profile", raw["routing_profile"])
    if "routing_authority_digest" in raw:
        normalized["routing_authority_digest"] = _digest(
            "execution_graph.routing_authority_digest",
            raw["routing_authority_digest"],
        )
    if "adaptive_projection_digest" in raw:
        normalized["adaptive_projection_digest"] = _digest(
            "execution_graph.adaptive_projection_digest",
            raw["adaptive_projection_digest"],
        )
    return normalized


def validate_work_graph(raw: Any) -> dict[str, Any]:
    """Validate the target-owned graph. It contains no provider identity."""
    if not isinstance(raw, dict) or raw.get("schema") != WORK_GRAPH_SCHEMA:
        raise ValueError(f"work_graph.schema must be {WORK_GRAPH_SCHEMA}")
    profile = _text("work_graph.routing_profile", raw.get("routing_profile"))
    if not _PROFILE_RE.fullmatch(profile):
        raise ValueError("work_graph.routing_profile must be a simple profile id")
    return {
        "schema": WORK_GRAPH_SCHEMA,
        "routing_profile": profile,
        "nodes": _validate_nodes(raw.get("nodes")),
    }


def validate_routing_authority(
    raw: Any,
    *,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Validate the LH/operator-owned registry, evidence, policy, and rollback."""
    if not isinstance(raw, dict) or raw.get("schema") != ROUTING_AUTHORITY_SCHEMA:
        raise ValueError(
            f"routing authority schema must be {ROUTING_AUTHORITY_SCHEMA}"
        )
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    profile = _text("routing.profile_id", raw.get("profile_id"))
    if not _PROFILE_RE.fullmatch(profile):
        raise ValueError("routing.profile_id must be a simple profile id")
    issued_text, issued_at = _instant("routing.issued_at", raw.get("issued_at"))
    valid_text, valid_until = _instant("routing.valid_until", raw.get("valid_until"))
    if valid_until <= issued_at or valid_until <= now:
        raise ValueError("routing authority validity window is stale or inverted")
    registry_raw = raw.get("registry")
    policy_raw = raw.get("policy")
    if not isinstance(registry_raw, dict) or not isinstance(registry_raw.get("resources"), list):
        raise ValueError("routing.registry.resources must be a list")
    resources = [
        _validate_resource(item, require_evidence=True, at=now)
        for item in registry_raw["resources"]
    ]
    if len({resource["binding_id"] for resource in resources}) != len(resources):
        raise ValueError("routing registry binding_id values must be unique")
    policy = _validate_policy(policy_raw)
    owner = _text("routing.owner", raw.get("owner"))
    policy_owner = _text("routing.policy.owner", policy_raw.get("owner"))
    if policy_owner != owner:
        raise ValueError("routing.policy.owner must match routing.owner")
    approved_text, approved_at = _instant(
        "routing.policy.approved_at",
        policy_raw.get("approved_at"),
    )
    if approved_at > now:
        raise ValueError("routing.policy.approved_at cannot be in the future")
    policy.update({
        "owner": policy_owner,
        "approved_at": approved_text,
        "rollback_revision": _text(
            "routing.policy.rollback_revision",
            policy_raw.get("rollback_revision"),
        ),
        "evidence_ref": _text(
            "routing.policy.evidence_ref",
            policy_raw.get("evidence_ref"),
        ),
        "evidence_digest": _digest(
            "routing.policy.evidence_digest",
            policy_raw.get("evidence_digest"),
        ),
    })
    return {
        "schema": ROUTING_AUTHORITY_SCHEMA,
        "profile_id": profile,
        "owner": owner,
        "revision": _text("routing.revision", raw.get("revision")),
        "issued_at": issued_text,
        "valid_until": valid_text,
        "previous_revision": _text(
            "routing.previous_revision",
            raw.get("previous_revision"),
        ),
        "registry": {
            "revision": _text(
                "routing.registry.revision",
                registry_raw.get("revision"),
            ),
            "resources": resources,
        },
        "policy": policy,
    }


def compose_graph(
    work_graph: Any,
    routing_authority: Any,
    *,
    at: datetime | None = None,
) -> dict[str, Any]:
    """Bind target work requirements to an operator-owned routing authority."""
    work = validate_work_graph(work_graph)
    routing = validate_routing_authority(routing_authority, at=at)
    if work["routing_profile"] != routing["profile_id"]:
        raise ValueError("work_graph routing_profile does not match routing authority")
    routing_digest = digest_json(routing)
    return validate_graph({
        "schema": GRAPH_SCHEMA,
        "nodes": work["nodes"],
        "routing_profile": routing["profile_id"],
        "routing_authority_digest": routing_digest,
        "registry": {
            **routing["registry"],
            "owner": routing["owner"],
            "valid_until": routing["valid_until"],
            "previous_revision": routing["previous_revision"],
            "profile_id": routing["profile_id"],
            "authority_digest": routing_digest,
        },
        "policy": routing["policy"],
    })


def _eligible(node: dict[str, Any], resource: dict[str, Any], policy: dict[str, Any]) -> bool:
    if resource["executor_kind"] != "model":
        return False
    allowed_health = {"healthy", "degraded"} if policy["allow_degraded"] else {"healthy"}
    return (
        resource["health"] in allowed_health
        and set(node["required_capabilities"]) <= set(resource["capabilities"])
        and set(node["required_tools"]) <= set(resource["tools"])
        and FILESYSTEM_LEVELS[resource["permission_ceiling"]] <= FILESYSTEM_LEVELS[node["permissions"]["filesystem"]]
        and NETWORK_LEVELS[resource["network_access"]] <= NETWORK_LEVELS[node["permissions"]["network"]]
        and DATA_LEVELS[resource["data_boundary"]] <= DATA_LEVELS[node["data_boundary"]]
        and resource["context_limit"] >= node["minimum_context"]
        and TRUST_LEVELS[resource["trust_tier"]] >= TRUST_LEVELS[node["minimum_trust"]]
    )


def _independent(candidate: dict[str, Any], reference: dict[str, Any], level: str) -> bool:
    if level == "none":
        return True
    if candidate["context_isolation"] != "fresh_process":
        return False
    if level == "context":
        return True
    if candidate["model_family"] == reference["model_family"]:
        return False
    return level == "model_family"


def _score(resource: dict[str, Any], policy: dict[str, Any]) -> float:
    weights = policy["weights"]
    scores = resource["scores"]
    return (
        scores["quality"] * weights["quality"]
        - scores["cost"] * weights["cost"]
        - scores["latency"] * weights["latency"]
    )


def resolve_operation(
    graph: dict[str, Any],
    operation: str,
    *,
    selected_nodes: dict[str, dict[str, Any]] | None = None,
    prior_attempts: list[dict[str, Any]] | None = None,
    attempt_ordinal: int | None = None,
) -> dict[str, Any] | None:
    normalized = validate_graph(graph)
    nodes = [node for node in normalized["nodes"] if node["operation"] == operation]
    if not nodes:
        return None
    node = nodes[0]
    resources = [
        resource for resource in normalized["registry"]["resources"]
        if _eligible(node, resource, normalized["policy"])
    ]
    selected_nodes = selected_nodes or {}
    for reference_id in node["independence"]["from_nodes"]:
        reference = selected_nodes.get(reference_id)
        if reference is None:
            raise ResolutionError(
                f"{node['node_id']} requires unresolved reference node {reference_id}",
                route=node["fallback_policy"],
            )
        resources = [
            resource for resource in resources
            if _independent(resource, reference, node["independence"]["minimum_level"])
        ]
    prior_attempts = prior_attempts or []
    used = {item.get("binding_id") for item in prior_attempts}
    if normalized["policy"]["retry"] == "next_eligible":
        if used:
            resources = [resource for resource in resources if resource["binding_id"] not in used]
        elif isinstance(attempt_ordinal, int) and attempt_ordinal > 1:
            ordered = sorted(
                resources,
                key=lambda item: (-_score(item, normalized["policy"]), item["binding_id"]),
            )
            resources = ordered[attempt_ordinal - 1:]
    if not resources:
        raise ResolutionError(
            f"no eligible resource for {node['node_id']} ({operation})",
            route=node["fallback_policy"],
        )
    resource = sorted(
        resources,
        key=lambda item: (-_score(item, normalized["policy"]), item["binding_id"]),
    )[0]
    binding = {
        "schema": BINDING_SCHEMA,
        "node_id": node["node_id"],
        "operation": operation,
        "binding_id": resource["binding_id"],
        "executor_kind": resource["executor_kind"],
        "runner": resource["runner"],
        "provider_ref": "declared:" + resource["provider_ref"],
        "model_family": resource["model_family"],
        "model": resource["model_identity"],
        "context_isolation": resource["context_isolation"],
        "endpoint_ref_digest": digest_json(resource["endpoint_ref"]),
        "trust_tier": resource["trust_tier"],
        "authority_ref": node["inputs"]["authority_ref"],
        "authority_digest": node["inputs"]["authority_digest"],
        "registry_revision": normalized["registry"]["revision"],
        "resolver_policy_revision": normalized["policy"]["revision"],
        "routing_profile": normalized.get("routing_profile"),
        "routing_authority_digest": normalized.get("routing_authority_digest"),
        "registry_owner": normalized["registry"].get("owner"),
        "routing_valid_until": normalized["registry"].get("valid_until"),
        "registry_rollback_revision": normalized["registry"].get("previous_revision"),
        "policy_evidence_digest": normalized["policy"].get("evidence_digest"),
        "policy_rollback_revision": normalized["policy"].get("rollback_revision"),
        "resource_health_evidence_digest": (
            resource.get("health_evidence", {}).get("digest")
            if isinstance(resource.get("health_evidence"), dict)
            else None
        ),
        "resource_score_evidence_digest": (
            resource.get("score_evidence", {}).get("digest")
            if isinstance(resource.get("score_evidence"), dict)
            else None
        ),
        "selection_reason": "highest_eligible_policy_score",
        "score": _score(resource, normalized["policy"]),
        "node_contract_digest": digest_json(node),
        "budget": dict(node["budget"]),
    }
    if isinstance(normalized.get("adaptive_projection_digest"), str):
        binding["adaptive_projection_digest"] = normalized[
            "adaptive_projection_digest"
        ]
    return {
        "binding": binding,
        "resource": resource,
        "node": node,
        "graph": normalized,
    }


def compatibility_binding(
    runner: str,
    *,
    provider_binding: dict[str, str] | None = None,
    authority_ref: str = "cli:inline",
    authority_digest: str | None = None,
) -> dict[str, Any]:
    claimed_model = (
        provider_binding.get("model")
        if isinstance(provider_binding, dict) and isinstance(provider_binding.get("model"), str)
        else "ambient-unknown"
    )
    identity = {"runner": runner, "provider_binding": provider_binding}
    return {
        "schema": BINDING_SCHEMA,
        "node_id": "compatibility-execute",
        "operation": "produce_change",
        "binding_id": f"compatibility:{runner}:{claimed_model}",
        "executor_kind": "model",
        "runner": runner,
        "provider_ref": "claimed-ambient-provider",
        "model_family": claimed_model,
        "model": claimed_model,
        "endpoint_ref_digest": digest_json("ambient-unknown"),
        "trust_tier": "claimed",
        "authority_ref": authority_ref,
        "authority_digest": authority_digest or digest_json(identity),
        "registry_revision": "compatibility",
        "resolver_policy_revision": "compatibility",
        "selection_reason": "explicit_compatibility_override",
        "score": None,
    }


def finalize_binding(
    binding: dict[str, Any],
    capsule: dict[str, Any],
    provider: dict[str, Any],
    *,
    started_at: datetime,
    finished_at: datetime,
    exit_status: str,
) -> dict[str, Any]:
    receipt = dict(binding)
    receipt.update({
        "attempt_id": f"{capsule.get('run_id')}:{capsule.get('attempt')}",
        "input_digest": digest_json(capsule),
        "prompt_or_command_digest": digest_json({
            "goal": capsule.get("goal"),
            "attempt": capsule.get("attempt"),
            "base_revision": capsule.get("base_revision"),
        }),
        "output_digest": digest_json({
            key: value for key, value in provider.items() if key != "binding_receipt"
        }),
        "evidence_refs": [],
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "finished_at": finished_at.astimezone(timezone.utc).isoformat(),
        "exit_status": exit_status,
        "usage": provider.get("usage") if isinstance(provider.get("usage"), dict) else {"state": "unknown"},
    })
    # goal-lifecycle-v1 provider-input binding: receipts retain the records
    # and digests, never raw prompt or context. `input_digest` and
    # `prompt_or_command_digest` keep their legacy recipe semantics.
    projection = capsule.get("provider_context_projection")
    if isinstance(projection, dict):
        receipt["provider_context_projection"] = projection
    for key in ("provider_input_binding", "provider_input_attestation"):
        value = provider.get(key)
        if isinstance(value, dict):
            receipt[key] = value
    bound = provider.get("provider_input_binding")
    if isinstance(bound, dict):
        receipt["provider_input_digest"] = bound.get("provider_input_digest")
    return receipt


def deterministic_binding(
    *,
    run_id: str,
    attempt: int,
    base_revision: str,
    verifier_argv: list[str],
    goal: dict[str, Any],
) -> dict[str, Any]:
    authority = goal.get("admission_envelope") if isinstance(goal.get("admission_envelope"), dict) else goal
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema": BINDING_SCHEMA,
        "attempt_id": f"{run_id}:{attempt}",
        "node_id": "acceptance-lamp",
        "operation": "run_gate",
        "binding_id": "lh:deterministic-verifier",
        "executor_kind": "deterministic",
        "runner": "subprocess",
        "provider_ref": "local",
        "model_family": "none",
        "model": "none",
        "endpoint_ref_digest": digest_json("local"),
        "trust_tier": "process_bound",
        "authority_ref": "goal.admission_envelope",
        "authority_digest": digest_json(authority),
        "registry_revision": "lh-core",
        "resolver_policy_revision": "deterministic",
        "selection_reason": "acceptance_lamp_precheck",
        "input_digest": digest_json({"goal": goal, "base_revision": base_revision}),
        "prompt_or_command_digest": digest_json(verifier_argv),
        "output_digest": digest_json({"exit_code": 0}),
        "evidence_refs": [],
        "started_at": now,
        "finished_at": now,
        "exit_status": "completed",
        "usage": {"state": "unknown", "reason": "deterministic executor"},
    }

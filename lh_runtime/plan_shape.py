#!/usr/bin/env python3
"""Plan shape: a fixed structural check of a sealed plan, before any verifier.

A plan that declares ``lh-sealed-plan/v1`` carries ``base_sha``, its
``work_units`` (``node_id``, ``depends_on``, ``read_set``, ``write_set``, and
optionally ``worker_id`` and ``worktree``), ``dispatchable_nodes``, optional
``parallel_groups``, and ``plan_digest`` over everything else.

``check_plan`` is pure.  It answers GREEN, or RED with every reason it found:
the plan must not have a cycle, duplicate units, shared workers or worktrees,
unresolved ``${...}`` placeholders, unknown dependencies or dispatchable nodes,
and no two members of a parallel group may touch the same paths or depend on
each other.  Path overlap is decided by the same function the scheduler uses,
so the two never disagree.  Judging whether the plan is *sensible* stays with
the injected verifier.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

try:
    from .work_unit_store import WorkUnitStoreError, normalize_path, read_write_conflicts
except ImportError:  # direct execution keeps lh_runtime on sys.path
    from work_unit_store import WorkUnitStoreError, normalize_path, read_write_conflicts  # type: ignore

SCHEMA = "lh-sealed-plan/v1"
CHECK_SCHEMA = "lh-plan-shape-check/v1"
PLAN_FIELDS = {"schema", "base_sha", "work_units", "dispatchable_nodes", "parallel_groups", "plan_digest"}
UNIT_FIELDS = {"node_id", "depends_on", "read_set", "write_set", "worker_id", "worktree"}
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
PLACEHOLDER_RE = re.compile(r"\$\{[^}]*\}")


class _Invalid(ValueError):
    pass


def digest_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def declares_shape(plan: Any) -> bool:
    return isinstance(plan, Mapping) and plan.get("schema") == SCHEMA


def _strings(value: Any, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise _Invalid
    if not allow_empty and not value:
        raise _Invalid
    return list(value)


def _shape(plan: Any) -> list[dict[str, Any]]:
    """Types and closed field sets; raises on anything malformed."""
    if (not isinstance(plan, Mapping) or plan.get("schema") != SCHEMA or set(plan) - PLAN_FIELDS
            or not {"schema", "base_sha", "work_units", "dispatchable_nodes", "plan_digest"} <= set(plan)
            or not isinstance(plan["base_sha"], str) or not SHA_RE.fullmatch(plan["base_sha"])
            or not isinstance(plan["plan_digest"], str) or not DIGEST_RE.fullmatch(plan["plan_digest"])
            or not isinstance(plan["work_units"], list) or not plan["work_units"]):
        raise _Invalid
    _strings(plan["dispatchable_nodes"], allow_empty=False)
    groups = plan.get("parallel_groups", [])
    if not isinstance(groups, list) or any(len(_strings(group, allow_empty=False)) < 2 for group in groups):
        raise _Invalid
    units = []
    for item in plan["work_units"]:
        if (not isinstance(item, Mapping) or set(item) - UNIT_FIELDS
                or not {"node_id", "depends_on", "read_set", "write_set"} <= set(item)
                or not isinstance(item["node_id"], str) or not item["node_id"].strip()):
            raise _Invalid
        for name in ("worker_id", "worktree"):
            if name in item and (not isinstance(item[name], str) or not item[name].strip()):
                raise _Invalid
        try:
            for name in ("read_set", "write_set"):
                [normalize_path(path) for path in _strings(item[name], allow_empty=(name == "read_set"))]
        except WorkUnitStoreError:
            raise _Invalid from None
        _strings(item["depends_on"])
        units.append(dict(item))
    return units


def _has_placeholder(value: Any) -> bool:
    if isinstance(value, str):
        return bool(PLACEHOLDER_RE.search(value))
    if isinstance(value, Mapping):
        return any(_has_placeholder(key) or _has_placeholder(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_has_placeholder(item) for item in value)
    return False


def _cycle(nodes: list[str], edges: dict[str, list[str]]) -> bool:
    incoming = {node: 0 for node in nodes}
    for node in nodes:
        for dependency in edges[node]:
            if dependency in incoming:
                incoming[node] += 1
    ready = [node for node, count in incoming.items() if count == 0]
    dependents = {node: [other for other in nodes if node in edges[other]] for node in nodes}
    seen = 0
    while ready:
        node = ready.pop()
        seen += 1
        for other in dependents[node]:
            incoming[other] -= 1
            if incoming[other] == 0:
                ready.append(other)
    return seen != len(nodes)


def _reaches(start: str, goal: str, edges: dict[str, list[str]]) -> bool:
    stack, seen = list(edges.get(start, ())), set()
    while stack:
        node = stack.pop()
        if node == goal:
            return True
        if node not in seen:
            seen.add(node)
            stack.extend(edges.get(node, ()))
    return False


def check_plan(plan: Any) -> dict[str, Any]:
    try:
        units = _shape(plan)
    except _Invalid:
        return {"schema": CHECK_SCHEMA, "verdict": "RED", "reasons": ["plan_schema_invalid"]}
    reasons: list[str] = []
    body = {key: value for key, value in plan.items() if key != "plan_digest"}
    if digest_json(body) != plan["plan_digest"]:
        reasons.append("plan_digest_mismatch")
    if _has_placeholder(body):
        reasons.append("plan_placeholder_unresolved")
    ids = [item["node_id"] for item in units]
    if len(set(ids)) != len(ids):
        reasons.append("work_unit_duplicate")
    for field, code in (("worker_id", "worker_not_unique"), ("worktree", "worktree_not_unique")):
        values = [item[field] for item in units if field in item]
        if len(set(values)) != len(values):
            reasons.append(code)
    known = set(ids)
    edges = {node: [] for node in known}
    for item in units:
        edges[item["node_id"]].extend(item["depends_on"])
    if any(dependency not in known for item in units for dependency in item["depends_on"]):
        reasons.append("dependency_unknown")
    if _cycle(sorted(known), edges):
        reasons.append("graph_cycle")
    if any(node not in known for node in plan["dispatchable_nodes"]):
        reasons.append("dispatchable_node_unknown")
    by_id = {item["node_id"]: item for item in units}
    for group in plan.get("parallel_groups", []):
        if any(node not in known for node in group):
            reasons.append("parallel_group_node_unknown")
            continue
        for index, left in enumerate(group):
            for right in group[index + 1:]:
                if read_write_conflicts(by_id[left], by_id[right]):  # symmetric, as the scheduler uses it
                    reasons.append("parallel_group_path_overlap")
                if _reaches(left, right, edges) or _reaches(right, left, edges):
                    reasons.append("parallel_group_dependent")
    ordered = list(dict.fromkeys(reasons))
    return {"schema": CHECK_SCHEMA, "verdict": "RED" if ordered else "GREEN", "reasons": ordered}

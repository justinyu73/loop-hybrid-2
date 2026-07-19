#!/usr/bin/env python3
"""Minimal read-only MCP stdio surface for native LH state and retrieval."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import project_status
import token_cost
import value_reducer
from goal_store import GoalStore
from knowledge_store import KnowledgeStore
from run_store import RunStore

PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "loop-hybrid-runtime", "version": "0.1.0"}


def _response(request: dict[str, Any], result: dict[str, Any] | None = None, error: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if "id" not in request:
        return None
    value: dict[str, Any] = {"jsonrpc": "2.0", "id": request["id"]}
    if error is not None:
        value["error"] = error
    else:
        value["result"] = result or {}
    return value


def _tool_result(value: Any, is_error: bool = False) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False, sort_keys=True)}], "isError": is_error}


def _run_view(store: RunStore, run_id: str) -> dict[str, Any]:
    run = store.get_run(run_id)
    return {"run_id": run["run_id"], "goal": run["goal"], "base_revision": run["base_revision"], "state": run["state"], "attempts": run["attempts"], "max_attempts": run["max_attempts"], "recent_events": store.events(run_id)[-10:]}


def _goal_status(goal_store: GoalStore, arguments: dict[str, Any]) -> dict[str, Any]:
    if set(arguments) == {"event_id"} and isinstance(arguments["event_id"], str):
        event = goal_store.get_event(arguments["event_id"])
        view = {"event_id": event["event_id"], "event_state": event["state"], "goal_id": event.get("goal_id")}
        if event.get("goal_id"):
            goal = goal_store.get_goal(event["goal_id"])
            view.update({"goal_state": goal["state"], "run_id": goal.get("run_id"), "campaign_id": goal.get("campaign_id"), "stage_id": goal.get("stage_id")})
        return view
    if set(arguments) == {"goal_id"} and isinstance(arguments["goal_id"], str):
        goal = goal_store.get_goal(arguments["goal_id"])
        return {"goal_id": goal["goal_id"], "goal_state": goal["state"], "run_id": goal.get("run_id"), "campaign_id": goal.get("campaign_id"), "stage_id": goal.get("stage_id")}
    raise ValueError("goal_status requires exactly one of event_id or goal_id as a string")


def dispatch(request: dict[str, Any], run_store: RunStore, knowledge_store: KnowledgeStore, goal_store: GoalStore | None = None) -> dict[str, Any] | None:
    method = request.get("method")
    params = request.get("params") or {}
    if method == "initialize":
        return _response(request, {"protocolVersion": PROTOCOL_VERSION, "capabilities": {"resources": {}, "tools": {}}, "serverInfo": SERVER_INFO})
    if method == "resources/list":
        resources = [
            {"uri": "lh://runtime/summary", "name": "LH runtime summary", "description": "Read-only run and knowledge-store counts", "mimeType": "application/json"},
            {"uri": "lh://runtime/cost", "name": "LH token cost", "description": "Read-only estimated token usage and cost rollup", "mimeType": "application/json"},
            {"uri": "lh://runtime/value", "name": "LH value verdicts", "description": "Read-only deterministic value (报红) verdict rollup: green/red per run", "mimeType": "application/json"},
        ]
        if goal_store is not None:
            resources.append({"uri": "lh://runtime/goals", "name": "LH goal summary", "description": "Read-only goal-lifecycle counts", "mimeType": "application/json"})
            resources.append({"uri": "lh://runtime/status", "name": "LH project status", "description": "Read-only unified project status: runs, goals, parked, cost, elapsed", "mimeType": "application/json"})
        return _response(request, {"resources": resources})
    if method == "resources/read":
        uri = params.get("uri")
        if uri == "lh://runtime/summary":
            content = {"run_store": run_store.summary(), "knowledge_store": knowledge_store.summary()}
            if goal_store is not None:
                content["goal_store"] = goal_store.summary()
            return _response(request, {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, ensure_ascii=False, sort_keys=True)}]})
        if uri == "lh://runtime/cost":
            rollup = token_cost.aggregate(run_store.usage_records())
            return _response(request, {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(rollup, ensure_ascii=False, sort_keys=True)}]})
        if uri == "lh://runtime/value":
            return _response(request, {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(value_reducer.aggregate(run_store), ensure_ascii=False, sort_keys=True)}]})
        if uri == "lh://runtime/goals" and goal_store is not None:
            return _response(request, {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(goal_store.summary(), ensure_ascii=False, sort_keys=True)}]})
        if uri == "lh://runtime/status" and goal_store is not None:
            return _response(request, {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(project_status.build_status(run_store, goal_store), ensure_ascii=False, sort_keys=True)}]})
        return _response(request, error={"code": -32602, "message": "unknown read-only resource"})
    if method == "tools/list":
        tools = [
            {"name": "lh_get_run", "description": "Read one durable LH run and its last ten events.", "inputSchema": {"type": "object", "required": ["run_id"], "properties": {"run_id": {"type": "string"}}, "additionalProperties": False}},
            {"name": "lh_search_knowledge", "description": "Search active code/docs chunks with source provenance.", "inputSchema": {"type": "object", "required": ["query"], "properties": {"query": {"type": "string"}, "revision": {"type": "string"}, "max_results": {"type": "integer", "minimum": 1, "maximum": 10}}, "additionalProperties": False}},
        ]
        if goal_store is not None:
            tools.append({"name": "lh_goal_status", "description": "Read one goal-lifecycle state by event_id or goal_id.", "inputSchema": {"type": "object", "properties": {"event_id": {"type": "string"}, "goal_id": {"type": "string"}}, "additionalProperties": False}})
        return _response(request, {"tools": tools})
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        try:
            if name == "lh_get_run" and set(arguments) == {"run_id"} and isinstance(arguments["run_id"], str):
                return _response(request, _tool_result(_run_view(run_store, arguments["run_id"])))
            if name == "lh_search_knowledge" and isinstance(arguments.get("query"), str) and set(arguments).issubset({"query", "revision", "max_results"}):
                max_results = arguments.get("max_results", 5)
                revision = arguments.get("revision")
                if revision is not None and not isinstance(revision, str):
                    raise ValueError("revision must be a string")
                return _response(request, _tool_result(knowledge_store.search(arguments["query"], revision=revision, max_results=max_results)))
            if name == "lh_goal_status" and goal_store is not None:
                return _response(request, _tool_result(_goal_status(goal_store, arguments)))
            return _response(request, _tool_result({"error": "tool is unknown or arguments are invalid"}, is_error=True))
        except (KeyError, ValueError) as exc:
            return _response(request, _tool_result({"error": str(exc)}, is_error=True))
    if method == "notifications/initialized":
        return None
    return _response(request, error={"code": -32601, "message": "method not found"})


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only native LH MCP server over stdio")
    parser.add_argument("--run-store", required=True)
    parser.add_argument("--knowledge-store", required=True)
    parser.add_argument("--goal-store", default=None)
    args = parser.parse_args()
    run_store = RunStore(Path(args.run_store))
    knowledge_store = KnowledgeStore(Path(args.knowledge_store))
    goal_store = GoalStore(Path(args.goal_store)) if args.goal_store else None
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("request must be an object")
            response = dispatch(request, run_store, knowledge_store, goal_store)
        except (ValueError, json.JSONDecodeError) as exc:
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(exc)}}
        if response is not None:
            print(json.dumps(response, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

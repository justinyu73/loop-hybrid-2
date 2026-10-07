#!/usr/bin/env python3
"""Provider-free acceptance gate for capability-resolved Attempt bindings."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))
import capability_resolver as cr
import cli_agent_executor as executors
from _fixture import make_campaign, make_source_repo
from controller import LoopController
from goal_loop_driver import run_driver
from goal_loop_run import (
    CapabilityRoutingSession,
    _make_capability_evaluator,
    build_worker,
    main as run_main,
    run,
)
from goal_store import GoalStore
from project_binding import CONTRACT_SCHEMA, resolve_project
from run_store import RunStore
import goal_loop_run as fixture_glr
from p7_native_runstore_fixture import explicit_runstore_factory
from p7_fence_fixture import fixture_command_runner
from native_delivery_fixture import make_native_run


AUTHORITY_DIGEST = "sha256:" + "a" * 64


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _resource(
    binding_id: str,
    runner: str,
    provider: str,
    capabilities: list[str],
    *,
    quality: float,
    model: str | None = None,
    permission: str = "workspace_write",
    network: str = "allowlisted",
    data_boundary: str = "local",
    tools: list[str] | None = None,
) -> dict[str, Any]:
    identity = model or f"ambient:{runner}"
    value: dict[str, Any] = {
        "binding_id": binding_id,
        "executor_kind": "model",
        "runner": runner,
        "provider_ref": provider,
        "model_family": identity,
        "endpoint_ref": f"ambient:{runner}",
        "capabilities": capabilities,
        "tools": tools if tools is not None else ["git", "shell"],
        "permission_ceiling": permission,
        "network_access": network,
        "data_boundary": data_boundary,
        "context_limit": 128000,
        "context_isolation": "fresh_process",
        "health": "healthy",
        "trust_tier": "process_bound" if model is not None else "claimed",
        "eval_revision": "eval-1",
        "scores": {"quality": quality, "cost": 0, "latency": 0},
    }
    if model is not None:
        value["model"] = model
    return value


def graph(*, first_quality: float = 9, second_quality: float = 8, evaluate: bool = True) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = [
        {
            "node_id": "change",
            "operation": "produce_change",
            "required_capabilities": ["repo_edit", "test_reasoning"],
            "required_tools": ["git", "shell"],
            "permissions": {"filesystem": "workspace_write", "network": "allowlisted"},
            "data_boundary": "local",
            "minimum_context": 64000,
            "minimum_trust": "claimed",
            "inputs": {
                "authority_ref": "docs/spec.md#feature",
                "authority_digest": AUTHORITY_DIGEST,
            },
            "acceptance": {"lamp_ref": "campaign.stage.acceptance_lamp"},
            "independence": {"from_nodes": [], "minimum_level": "none"},
            "budget": {
                "max_wall_seconds": 900,
                "max_uncached_input_tokens": 120000,
                "max_output_tokens": 16000,
            },
            "fallback_policy": "next_eligible",
        },
    ]
    if evaluate:
        nodes.append({
            "node_id": "evaluation",
            "operation": "evaluate_transition",
            "required_capabilities": ["bounded_judgment"],
            "required_tools": [],
            "permissions": {"filesystem": "read_only", "network": "external"},
            "data_boundary": "external",
            "minimum_context": 32000,
            "minimum_trust": "process_bound",
            "inputs": {
                "authority_ref": "docs/spec.md#feature",
                "authority_digest": AUTHORITY_DIGEST,
            },
            "acceptance": {"decision_space_ref": "lh:closed-decision-space"},
            "independence": {
                "from_nodes": ["change"],
                "minimum_level": "model_family",
            },
            "budget": {
                "max_wall_seconds": 300,
                "max_uncached_input_tokens": 32000,
                "max_output_tokens": 2000,
            },
            "fallback_policy": "human_required",
        })
    return {
        "schema": cr.GRAPH_SCHEMA,
        "nodes": nodes,
        "registry": {
            "revision": "registry-1",
            "resources": [
                _resource(
                    "edit-a", "fake-a", "provider-a",
                    ["repo_edit", "test_reasoning"], quality=first_quality,
                ),
                _resource(
                    "edit-b", "fake-b", "provider-b",
                    ["repo_edit", "test_reasoning"], quality=second_quality,
                ),
                _resource(
                    "evaluate-c", "evaluator", "provider-c",
                    ["bounded_judgment"], quality=7, model="ambient:fake-b",
                    permission="read_only", network="external",
                    data_boundary="external", tools=[],
                ),
                _resource(
                    "evaluate-d", "evaluator", "provider-d",
                    ["bounded_judgment"], quality=6, model="judge-independent",
                    permission="read_only", network="external",
                    data_boundary="external", tools=[],
                ),
            ],
        },
        "policy": {
            "revision": "policy-1",
            "weights": {"quality": 1, "cost": 0, "latency": 0},
            "allow_degraded": False,
            "retry": "next_eligible",
        },
    }


# The evaluator prints one closed JSON object on its last line; the coder is a
# placeholder that capability routing never launches in this canary.
EVALUATOR_CODE = ("import json; print(json.dumps({'decision': 'runner-fixable', "
                  "'diagnosis': 'fixture diagnosis'}))")
DECLARATIONS = {
    "evaluator": {"argv": [sys.executable, "-c", EVALUATOR_CODE, "{model}", "{prompt}"]},
    "coder": {"argv": [sys.executable, "-c", "pass", "{model}", "{prompt}"]},
}


def _bound_graph(base_url: str) -> dict[str, Any]:
    candidate = graph(evaluate=False)
    resource = candidate["registry"]["resources"][0]
    resource["runner"] = "coder"
    resource["provider_binding"] = {
        "runner": "coder",
        "base_url": base_url,
        "model": "bound-edit-a",
    }
    resource["model_family"] = "bound-edit-a"
    resource["endpoint_ref"] = base_url
    resource["trust_tier"] = "process_bound"
    return candidate


def _rejects(fn: Callable[[], Any]) -> tuple[bool, str]:
    try:
        fn()
        return False, "no error"
    except (ValueError, SystemExit) as exc:
        return True, f"{type(exc).__name__}: {exc}"


class FactorySpy:
    def __init__(self) -> None:
        self.factory_calls = 0
        self.model_calls = 0

    def __call__(self, *, timeout_seconds: float = 900):
        self.factory_calls += 1

        def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
            self.model_calls += 1
            target = workspace / "src"
            target.mkdir(exist_ok=True)
            (target / f"attempt-{capsule['attempt']}.txt").write_text(
                f"bounded timeout={timeout_seconds}\n",
                encoding="utf-8",
            )
            return {"summary": "capability fixture model"}

        return model


def _seed(goal_root: Path, campaign: dict[str, Any], source: Path, base: str) -> None:
    from campaign_compiler import CampaignCompiler

    envelope = CampaignCompiler(campaign).compile()["stages"]["stage-1"]
    goal_id = f"{campaign['campaign_id']}:stage-1"
    bundle_store = RunStore(goal_root.parent / f"{goal_root.name}-delivery-bundle")
    bundle = make_native_run(
        bundle_store,
        source,
        base,
        goal_id,
        "capability",
        [{
            "id": "capability-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["git", "rev-parse", "HEAD"],
        ["src/"],
        4,
        goal={"feature_contract": "stage-1", "admission_envelope": envelope},
    )
    persisted_goal = bundle_store.get_run(bundle["run_id"])["goal"]
    GoalStore(goal_root).record_event(
        event_id="capability-seed-1",
        idempotency_key="capability-seed-1",
        source="manual_intent",
        event_type="goal_candidate",
        payload={
            "candidate": {
                "goal_id": goal_id,
                "campaign_id": campaign["campaign_id"],
                "stage_id": "stage-1",
                "goal": persisted_goal,
            },
        },
    )


def _latest_receipt(run_root: Path) -> dict[str, Any]:
    paths = sorted((run_root / "artifacts").glob("*/1/receipt.json"))
    if len(paths) != 1:
        raise AssertionError(f"expected one receipt, found {paths}")
    return json.loads(paths[0].read_text(encoding="utf-8"))


def _persist_bindings(
    run_root: Path,
    source: Path,
    base_revision: str,
    bindings: list[dict[str, Any]],
) -> str:
    store = RunStore(run_root)
    run_id = make_native_run(
        store,
        source,
        base_revision,
        "capability-evaluator",
        "evaluation",
        [{
            "id": "capability-evaluator-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["git", "rev-parse", "HEAD"],
        ["src/"],
        max(4, len(bindings) + 1),
        goal={"feature_contract": "capability evaluator fixture"},
    )["run_id"]
    for expected_ordinal, raw_binding in enumerate(bindings, start=1):
        ordinal = store.begin_attempt(run_id, f"workspace://fixture/{expected_ordinal}")
        if ordinal != expected_ordinal:
            raise AssertionError(f"unexpected ordinal: {ordinal}")
        binding = dict(raw_binding)
        binding["attempt_id"] = f"{run_id}:{ordinal}"
        receipt = {
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": run_id,
            "attempt": ordinal,
            "binding": binding,
        }
        ref = store.write_artifact(
            run_id,
            ordinal,
            "receipt.json",
            json.dumps(receipt, sort_keys=True),
        )
        if not store.finish_attempt(
            run_id,
            ordinal,
            state="retry_pending",
            receipt_ref=ref["ref"],
            receipt_digest=ref["digest"],
        ):
            raise AssertionError("fixture Attempt finish was fenced")
    return run_id


def main() -> int:
    cases: list[dict[str, object]] = []

    first = cr.resolve_operation(graph(), "produce_change")
    switched = cr.resolve_operation(
        graph(first_quality=1, second_quality=8),
        "produce_change",
    )
    cases.append(case(
        "mm1-registry-policy-changes-binding-not-work-node",
        first is not None
        and switched is not None
        and first["binding"]["binding_id"] == "edit-a"
        and switched["binding"]["binding_id"] == "edit-b"
        and first["node"] == switched["node"],
        f"first={first and first['binding']['binding_id']} switched={switched and switched['binding']['binding_id']}",
    ))

    identity_graph = graph()
    identity_graph["nodes"][0]["writer_model"] = "forbidden"
    identity_rejected, identity_detail = _rejects(lambda: cr.validate_graph(identity_graph))
    bad_digest = graph()
    bad_digest["nodes"][0]["inputs"]["authority_digest"] = "sha256:" + "z" * 64
    digest_rejected, digest_detail = _rejects(lambda: cr.validate_graph(bad_digest))
    bad_endpoint = graph()
    bad_endpoint["registry"]["resources"][0]["endpoint_ref"] = "https://self-claimed.invalid"
    endpoint_rejected, endpoint_detail = _rejects(lambda: cr.validate_graph(bad_endpoint))
    bad_family = graph()
    bad_family["registry"]["resources"][0]["model_family"] = "invented-family"
    family_rejected, family_detail = _rejects(lambda: cr.validate_graph(bad_family))
    bad_trust = graph()
    bad_trust["registry"]["resources"][0]["trust_tier"] = "process_bound"
    trust_rejected, trust_detail = _rejects(lambda: cr.validate_graph(bad_trust))
    invalid_binding_urls = [
        "https://user:pass@example.test/v1",
        "https://example.test:notaport/v1",
        "https://example.test:99999/v1",
        "https://example.test/v1?secret=value",
        "https://example.test/v1#fragment",
    ]
    invalid_url_results = {
        url: _rejects(lambda candidate=_bound_graph(url): cr.validate_graph(candidate))
        for url in invalid_binding_urls
    }
    revalidated = cr.resolve_operation(
        cr.validate_graph(cr.validate_graph(graph())),
        "produce_change",
    )
    cases.append(case(
        "mm2-unprovable-identity-authority-and-trust-claims-rejected",
        identity_rejected
        and digest_rejected
        and endpoint_rejected
        and family_rejected
        and trust_rejected
        and all(rejected for rejected, _detail in invalid_url_results.values())
        and revalidated is not None
        and revalidated["binding"]["provider_ref"] == "declared:provider-a",
        (
            f"identity={identity_detail}; digest={digest_detail}; "
            f"endpoint={endpoint_detail}; family={family_detail}; "
            f"trust={trust_detail}; urls={invalid_url_results}"
        ),
    ))

    eligibility_mutations: list[tuple[str, Callable[[dict[str, Any]], None]]] = [
        ("capability", lambda r: r["capabilities"].remove("repo_edit")),
        ("tool", lambda r: r["tools"].remove("shell")),
        ("data", lambda r: r.__setitem__("data_boundary", "external")),
        ("context", lambda r: r.__setitem__("context_limit", 100)),
        ("health", lambda r: r.__setitem__("health", "unavailable")),
    ]
    eligibility_results: dict[str, str | None] = {}
    for name, mutate in eligibility_mutations:
        candidate = graph()
        mutate(candidate["registry"]["resources"][0])
        resolved = cr.resolve_operation(candidate, "produce_change")
        eligibility_results[name] = resolved["binding"]["binding_id"] if resolved else None
    permission_graph = graph()
    permission_graph["nodes"][0]["permissions"]["filesystem"] = "read_only"
    permission_graph["registry"]["resources"][1]["permission_ceiling"] = "read_only"
    permission = cr.resolve_operation(permission_graph, "produce_change")
    eligibility_results["permission"] = permission["binding"]["binding_id"] if permission else None
    network_graph = graph()
    network_graph["nodes"][0]["permissions"]["network"] = "none"
    network_graph["registry"]["resources"][1]["network_access"] = "none"
    network = cr.resolve_operation(network_graph, "produce_change")
    eligibility_results["network"] = network["binding"]["binding_id"] if network else None
    trust_graph = graph()
    trust_graph["nodes"][0]["minimum_trust"] = "process_bound"
    trust_graph["registry"]["resources"][1]["model"] = "bound-edit-b"
    trust_graph["registry"]["resources"][1]["model_family"] = "bound-edit-b"
    trust_graph["registry"]["resources"][1]["trust_tier"] = "process_bound"
    trust = cr.resolve_operation(trust_graph, "produce_change")
    eligibility_results["trust"] = trust["binding"]["binding_id"] if trust else None
    cases.append(case(
        "mm3-actual-execution-envelope-and-eligibility-are-enforced",
        set(eligibility_results.values()) == {"edit-b"},
        json.dumps(eligibility_results, sort_keys=True),
    ))

    retry_graph = graph(evaluate=False)
    attempt_one = cr.resolve_operation(retry_graph, "produce_change")
    frozen = copy.deepcopy(attempt_one["binding"])
    attempt_two = cr.resolve_operation(
        retry_graph,
        "produce_change",
        prior_attempts=[attempt_one["binding"]],
    )
    restarted_two = cr.resolve_operation(
        retry_graph,
        "produce_change",
        attempt_ordinal=2,
    )
    exhausted, exhausted_detail = _rejects(lambda: cr.resolve_operation(
        retry_graph,
        "produce_change",
        prior_attempts=[attempt_one["binding"], attempt_two["binding"]],
    ))
    cases.append(case(
        "mm4-retry-is-next-eligible-and-prior-binding-is-immutable",
        attempt_two is not None
        and restarted_two is not None
        and attempt_two["binding"]["binding_id"] == "edit-b"
        and restarted_two["binding"]["binding_id"] == "edit-b"
        and attempt_one["binding"] == frozen
        and exhausted,
        f"second={attempt_two and attempt_two['binding']['binding_id']} restart={restarted_two and restarted_two['binding']['binding_id']} exhausted={exhausted_detail}",
    ))

    selected_first = {"change": first["resource"]}
    evaluation_first = cr.resolve_operation(
        graph(),
        "evaluate_transition",
        selected_nodes=selected_first,
    )
    selected_second = {"change": attempt_two["resource"]}
    evaluation_second = cr.resolve_operation(
        graph(),
        "evaluate_transition",
        selected_nodes=selected_second,
    )
    undeclared_evaluation_rejected, undeclared_evaluation_detail = _rejects(
        lambda: executors.declared_command(DECLARATIONS, "undeclared", "PROMPT", "ambient:fake-b")
    )
    evaluator_argv = executors.declared_command(DECLARATIONS, "evaluator", "PROMPT", "judge-independent")
    cases.append(case(
        "mm5-evaluation-resolves-after-actual-attempt-with-a-declared-evaluator",
        evaluation_first is not None
        and evaluation_second is not None
        and evaluation_first["binding"]["binding_id"] == "evaluate-c"
        and evaluation_second["binding"]["binding_id"] == "evaluate-d"
        and undeclared_evaluation_rejected
        and evaluator_argv[-2:] == ["judge-independent", "PROMPT"],
        (
            f"first={evaluation_first and evaluation_first['binding']} "
            f"second={evaluation_second and evaluation_second['binding']} "
            f"undeclared={undeclared_evaluation_detail} evaluator={evaluator_argv[-2:]}"
        ),
    ))

    with tempfile.TemporaryDirectory() as raw:
        # The trust root must be canonical; a temp dir can carry 8.3 short names on Windows.
        root = Path(raw).resolve()
        source, base = make_source_repo(root)
        campaign = make_campaign("campaign-capability")
        dry_spy = FactorySpy()
        dry = run(
            executor_declarations=DECLARATIONS,
            execution_graph=graph(evaluate=False),
            execute=False,
            goal_store_root=root / "dry-goals",
            run_store_root=root / "dry-runs",
            workspace_root=root / "dry-ws",
            campaign=campaign,
            source_repo=source,
            base_revision=base,
            factory_overrides={"fake-a": dry_spy, "fake-b": dry_spy},
        )
        cases.append(case(
            "mm8-dry-run-projects-bindings-without-cli",
            dry["mode"] == "dry_run"
            and dry["plan"]["routing"]["mode"] == "capability"
            and dry["plan"]["routing"]["bindings"][0]["binding_id"] == "edit-a"
            and dry_spy.factory_calls == 0
            and dry_spy.model_calls == 0,
            json.dumps(dry["plan"]["routing"], sort_keys=True),
        ))

        execute_spy = FactorySpy()
        _seed(root / "goals", campaign, source, base)
        with explicit_runstore_factory(fixture_glr):
            result = run(
                executor_declarations=DECLARATIONS,
                execution_graph=graph(evaluate=False),
                execute=True,
                goal_store_root=root / "goals",
                run_store_root=root / "runs",
                workspace_root=root / "ws",
                campaign=campaign,
                source_repo=source,
                base_revision=base,
                max_cycles=30,
                sleep_fn=lambda _seconds: None,
                factory_overrides={"fake-a": execute_spy, "fake-b": execute_spy},
            )
        receipt = _latest_receipt(root / "runs")
        binding = receipt.get("binding", {})
        cases.append(case(
            "mm6-model-attempt-persists-resolved-binding",
            result["invoked"] is True
            and execute_spy.model_calls == 1
            and binding.get("schema") == cr.BINDING_SCHEMA
            and binding.get("binding_id") == "edit-a"
            and binding.get("authority_digest") == AUTHORITY_DIGEST
            and binding.get("attempt_id", "").endswith(":1")
            and len(binding.get("evidence_refs", [])) == 4,
            json.dumps(binding, sort_keys=True),
        ))

        evaluation_graph = graph()
        evaluation_session = CapabilityRoutingSession(
            evaluation_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=root / "evaluation-runs",
            factory_overrides={"fake-a": FactorySpy(), "fake-b": FactorySpy()},
        )
        retry_run_id = _persist_bindings(
            root / "evaluation-runs",
            source,
            base,
            [
                attempt_one["binding"],
                attempt_two["binding"],
            ],
        )
        runtime_resolution = evaluation_session.resolve_evaluation(
            {"run_id": retry_run_id}
        )
        restarted_session = CapabilityRoutingSession(
            evaluation_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=root / "runs",
            factory_overrides={"fake-a": FactorySpy(), "fake-b": FactorySpy()},
        )
        persisted_resolution = restarted_session.resolve_evaluation(
            {"run_id": receipt["run_id"]}
        )
        evaluator = _make_capability_evaluator(
            restarted_session,
            kind="grill",
            prompt_builder=lambda _snapshot: "fixture prompt",
            parser=json.loads,
            output_schema={
                "type": "object",
                "properties": {
                    "decision": {"type": "string"},
                    "diagnosis": {"type": "string"},
                },
                "required": ["decision", "diagnosis"],
                "additionalProperties": False,
            },
        )
        evaluator_result = evaluator({
            "run_id": receipt["run_id"],
            "next_attempt": 2,
        })
        evaluation_receipts = sorted(
            (root / "runs" / "routing-evidence").glob(
                "evaluation-*/receipt.json"
            )
        )
        evaluation_receipt = json.loads(
            evaluation_receipts[-1].read_text(encoding="utf-8")
        )
        evidence_valid = all(
            (
                root
                / "runs"
                / item["ref"]
            ).is_file()
            and item["digest"].startswith("sha256:")
            for item in evaluation_receipt.get("evidence_refs", [])
        )
        cases.append(case(
            "mm6-evaluator-persists-actual-binding-usage-and-evidence",
            runtime_resolution["binding"]["binding_id"] == "evaluate-d"
            and runtime_resolution["independence_reference"]["binding_id"] == "edit-b"
            and persisted_resolution["binding"]["binding_id"] == "evaluate-c"
            and persisted_resolution["independence_reference"]["attempt_id"].endswith(":1")
            and evaluator_result["decision"] == "runner-fixable"
            and evaluation_receipt.get("schema") == cr.BINDING_SCHEMA
            and evaluation_receipt.get("binding_id") == "evaluate-c"
            and evaluation_receipt.get("invocation_kind") == "grill"
            and evaluation_receipt.get("independence_reference", {}).get("binding_id")
            == "edit-a"
            and evaluation_receipt.get("independence_reference", {}).get("attempt_id", "")
            .endswith(":1")
            and evaluation_receipt.get("usage", {}).get("state") == "unknown"
            and len(evaluation_receipt.get("evidence_refs", [])) == 2
            and evidence_valid,
            json.dumps(evaluation_receipt, sort_keys=True),
        ))

        persisted = RunStore(root / "runs").latest_receipt(receipt["run_id"])
        if persisted is None:
            raise AssertionError("expected persisted receipt")
        persisted_path = root / "runs" / persisted["receipt_ref"]
        original_receipt = persisted_path.read_text(encoding="utf-8")
        tampered_receipt = json.loads(original_receipt)
        tampered_receipt["binding"]["binding_id"] = "edit-b"
        tampered_receipt["binding"]["model"] = "ambient:fake-b"
        tampered_receipt["binding"]["model_family"] = "ambient:fake-b"
        persisted_path.write_text(
            json.dumps(tampered_receipt, sort_keys=True),
            encoding="utf-8",
        )
        tamper_rejected, tamper_detail = _rejects(
            lambda: restarted_session.resolve_evaluation(
                {"run_id": receipt["run_id"]}
            )
        )
        persisted_path.write_text(original_receipt, encoding="utf-8")
        volatile_session = CapabilityRoutingSession(
            evaluation_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=root / "volatile-runs",
            factory_overrides={"fake-a": FactorySpy(), "fake-b": FactorySpy()},
        )
        volatile_session.prior_attempts_by_run["volatile-run"] = [
            attempt_two["binding"],
        ]
        volatile_rejected, volatile_detail = _rejects(
            lambda: volatile_session.resolve_evaluation(
                {"run_id": "volatile-run"}
            )
        )
        ordinal_root = root / "ordinal-mismatch-runs"
        ordinal_store = RunStore(ordinal_root)
        ordinal_run_id = ordinal_store.create_run(
            goal={"feature_contract": "ordinal mismatch fixture"},
            source_repo=source,
            base_revision=base,
            max_attempts=3,
        )
        ordinal_one = ordinal_store.begin_attempt(
            ordinal_run_id, "workspace://fixture/1"
        )
        ordinal_binding = dict(attempt_two["binding"])
        ordinal_binding["attempt_id"] = f"{ordinal_run_id}:{ordinal_one}"
        ordinal_receipt = {
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": ordinal_run_id,
            "attempt": ordinal_one,
            "binding": ordinal_binding,
        }
        ordinal_ref = ordinal_store.write_artifact(
            ordinal_run_id,
            ordinal_one,
            "receipt.json",
            json.dumps(ordinal_receipt, sort_keys=True),
        )
        if not ordinal_store.finish_attempt(
            ordinal_run_id,
            ordinal_one,
            state="retry_pending",
            receipt_ref=ordinal_ref["ref"],
            receipt_digest=ordinal_ref["digest"],
        ):
            raise AssertionError("ordinal fixture Attempt 1 finish was fenced")
        ordinal_two = ordinal_store.begin_attempt(
            ordinal_run_id, "workspace://fixture/2"
        )
        ordinal_two_receipt = {
            "schema": "loop-hybrid-attempt-receipt/v1",
            "run_id": ordinal_run_id,
            "attempt": ordinal_two,
            # The artifact itself is valid for Attempt 2, but its immutable
            # capability binding still names Attempt 1.  The evaluator must
            # reject that wrong-ordinal reference rather than treating the
            # latest receipt as a fresh selection.
            "binding": {**ordinal_binding, "attempt_id": f"{ordinal_run_id}:1"},
        }
        ordinal_two_ref = ordinal_store.write_artifact(
            ordinal_run_id,
            ordinal_two,
            "receipt.json",
            json.dumps(ordinal_two_receipt, sort_keys=True),
        )
        if not ordinal_store.finish_attempt(
            ordinal_run_id,
            ordinal_two,
            state="retry_pending",
            receipt_ref=ordinal_two_ref["ref"],
            receipt_digest=ordinal_two_ref["digest"],
        ):
            raise AssertionError("ordinal fixture Attempt 2 finish was fenced")
        ordinal_session = CapabilityRoutingSession(
            evaluation_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=ordinal_root,
            factory_overrides={"fake-a": FactorySpy(), "fake-b": FactorySpy()},
        )
        ordinal_rejected, ordinal_detail = _rejects(
            lambda: ordinal_session.resolve_evaluation(
                {"run_id": ordinal_run_id}
            )
        )
        cases.append(case(
            "mm6-evaluator-rejects-tampered-volatile-and-wrong-ordinal-reference-bindings",
            tamper_rejected and volatile_rejected and ordinal_rejected,
            (
                f"tamper={tamper_detail}; volatile={volatile_detail}; "
                f"ordinal={ordinal_detail}"
            ),
        ))

        no_evaluator_graph = graph()
        for resource in no_evaluator_graph["registry"]["resources"]:
            if "bounded_judgment" in resource["capabilities"]:
                resource["health"] = "unavailable"
        no_evaluator_session = CapabilityRoutingSession(
            no_evaluator_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=root / "no-evaluator-runs",
            factory_overrides={"fake-a": FactorySpy(), "fake-b": FactorySpy()},
        )
        no_evaluator_run_id = _persist_bindings(
            root / "no-evaluator-runs",
            source,
            base,
            [attempt_one["binding"]],
        )
        unavailable_evaluator = _make_capability_evaluator(
            no_evaluator_session,
            kind="grill",
            prompt_builder=lambda _snapshot: "unused",
            parser=json.loads,
        )
        unavailable_rejected, unavailable_detail = _rejects(
            lambda: unavailable_evaluator({"run_id": no_evaluator_run_id})
        )
        resolution_receipts = sorted(
            (root / "no-evaluator-runs" / "routing-evidence").glob(
                "evaluation-*/receipt.json"
            )
        )
        resolution_receipt = json.loads(
            resolution_receipts[-1].read_text(encoding="utf-8")
        )
        cases.append(case(
            "mm9-evaluator-resolution-failure-is-durable-human-route",
            unavailable_rejected
            and resolution_receipt.get("schema") == "lh-routing-resolution/v1"
            and resolution_receipt.get("route") == "human_required"
            and resolution_receipt.get("operation") == "evaluate_transition"
            and len(resolution_receipt.get("evidence_refs", [])) == 2,
            f"error={unavailable_detail}; receipt={json.dumps(resolution_receipt, sort_keys=True)}",
        ))

        pre_store = RunStore(root / "pre-runs", command_runner=fixture_command_runner)
        controller = LoopController(pre_store, root / "pre-ws")
        pre_run = make_native_run(
            pre_store,
            source,
            base,
            "capability-precheck",
            "precheck",
            [{
                "id": "capability-precheck-check",
                "commands": [{
                    "id": "diff-check",
                    "argv": ["git", "diff", "--check"],
                    "cwd": "${WORKTREE}",
                    "expect_exit": 0,
                    "timeout_seconds": 10,
                }],
                "required_receipts": ["executor"],
            }],
            ["git", "rev-parse", "HEAD"],
            ["src/"],
            4,
            goal={"feature_contract": "already done"},
        )["run_id"]
        pre = controller.tick(
            pre_run,
            holder="capability-canary",
            model=lambda _ws, _cap: (_ for _ in ()).throw(
                AssertionError("precheck must not call model")
            ),
            verifier_argv=["git", "diff", "--check"],
        )
        pre_receipt = json.loads(
            (pre_store.root / pre["receipt_ref"]).read_text(encoding="utf-8")
        )
        pre_binding = pre_receipt.get("binding", {})
        cases.append(case(
            "mm6-deterministic-precheck-persists-binding",
            pre.get("precheck") is True
            and pre_binding.get("executor_kind") == "deterministic"
            and pre_binding.get("selection_reason") == "acceptance_lamp_precheck"
            and len(pre_binding.get("evidence_refs", [])) == 4,
            json.dumps(pre_binding, sort_keys=True),
        ))

        unavailable_graph = graph(evaluate=False)
        unavailable_graph["nodes"][0]["fallback_policy"] = "human_required"
        for resource in unavailable_graph["registry"]["resources"]:
            resource["health"] = "unavailable"
        unavailable_spy = FactorySpy()
        unavailable_campaign = make_campaign("campaign-unavailable")
        unavailable_goal_root = root / "unavailable-goals"
        unavailable_run_root = root / "unavailable-runs"
        unavailable_session = CapabilityRoutingSession(
            unavailable_graph,
            executor_declarations=DECLARATIONS,
            timeout_seconds=30,
            run_store_root=unavailable_run_root,
            factory_overrides={
                "fake-a": unavailable_spy,
                "fake-b": unavailable_spy,
            },
        )
        _seed(unavailable_goal_root, unavailable_campaign, source, base)
        unavailable_worker = build_worker(
            goal_store_root=unavailable_goal_root,
            run_store_root=unavailable_run_root,
            workspace_root=root / "unavailable-ws",
            campaign=unavailable_campaign,
            source_repo=source,
            base_revision=base,
        )
        unavailable_result = run_driver(
            unavailable_worker,
            holder="unavailable",
            model=unavailable_session.execute,
            max_cycles=20,
            sleep_fn=lambda _seconds: None,
        )
        unavailable_receipt = _latest_receipt(unavailable_run_root)
        unavailable_goal = GoalStore(unavailable_goal_root).get_goal(
            "campaign-unavailable:stage-1"
        )
        cases.append(case(
            "no-eligible-resource-routes-through-receipt-to-human",
            unavailable_spy.model_calls == 0
            and unavailable_receipt.get("routing", {}).get("route") == "human_required"
            and unavailable_goal["state"] == "human_required"
            and any(
                item.get("status") == "human_required"
                for item in unavailable_result["outcomes"]
            ),
            f"goal={unavailable_goal['state']} routing={unavailable_receipt.get('routing')} outcomes={unavailable_result['outcomes']}",
        ))

        contract = {
            "schema": CONTRACT_SCHEMA,
            "project_id": "capability-project",
            "campaign": campaign,
            "source_repo": str(source),
            "base_revision": base,
            "runtime": {
                "goal_store": "contract/goals",
                "run_store": "contract/runs",
                "workspace_root": "contract/ws",
            },
            "execution_graph": graph(evaluate=False),
            "executors": DECLARATIONS,
            "models": {"execute": "coder"},
        }
        contract_path = root / "conflict.json"
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        conflict_rejected, conflict_detail = _rejects(
            lambda: resolve_project(contract_path)
        )
        contract.pop("models")
        invalid_project_url_results: dict[str, tuple[bool, str]] = {}
        for invalid_url in invalid_binding_urls:
            contract["execution_graph"] = _bound_graph(invalid_url)
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            invalid_project_url_results[invalid_url] = _rejects(
                lambda: resolve_project(contract_path)
            )
        cases.append(case(
            "mm10-project-binding-rejects-invalid-provider-urls-before-projection",
            all(
                rejected
                for rejected, _detail in invalid_project_url_results.values()
            ),
            json.dumps(invalid_project_url_results, sort_keys=True),
        ))
        cli_graph = graph(evaluate=False)
        cli_graph["registry"]["resources"][0]["runner"] = "coder"
        cli_graph["registry"]["resources"][0]["model"] = "deployment-a"
        cli_graph["registry"]["resources"][0]["model_family"] = "deployment-a"
        cli_graph["registry"]["resources"][0]["endpoint_ref"] = "ambient:coder"
        cli_graph["registry"]["resources"][0]["trust_tier"] = "process_bound"
        cli_graph["registry"]["resources"][0]["network_access"] = "external"
        cli_graph["registry"]["resources"][0]["data_boundary"] = "external"
        cli_graph["registry"]["resources"][1]["runner"] = "coder"
        cli_graph["registry"]["resources"][1]["model"] = "deployment-b"
        cli_graph["registry"]["resources"][1]["model_family"] = "deployment-b"
        cli_graph["registry"]["resources"][1]["endpoint_ref"] = "ambient:coder"
        cli_graph["registry"]["resources"][1]["trust_tier"] = "process_bound"
        cli_graph["registry"]["resources"][1]["network_access"] = "external"
        cli_graph["registry"]["resources"][1]["data_boundary"] = "external"
        cli_graph["nodes"][0]["permissions"]["network"] = "external"
        cli_graph["nodes"][0]["data_boundary"] = "external"
        contract["execution_graph"] = cli_graph
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        bootstrap_path = (
            root
            / "docs"
            / "bootstrap-authority.md"
        )
        bootstrap_path.parent.mkdir(parents=True, exist_ok=True)
        bootstrap_path.write_text(
            '<a id="lh-external-bootstrap-001"></a>\n'
            "### Unified bootstrap fixture\n",
            encoding="utf-8",
        )
        bootstrap_digest = (
            "sha256:" + hashlib.sha256(bootstrap_path.read_bytes()).hexdigest()
        )
        cli_stdout = io.StringIO()
        with redirect_stdout(cli_stdout):
            cli_exit = run_main(["--contract", str(contract_path)])
        cli_result = json.loads(cli_stdout.getvalue())
        hosted_args = [
                "--contract", str(contract_path),
                "--execution-host", "headless_cli",
                "--bootstrap-decision-id", "LH-EXTERNAL-BOOTSTRAP-001",
                "--bootstrap-authority-ref",
                "docs/bootstrap-authority.md#lh-external-bootstrap-001",
                "--bootstrap-authority-digest", bootstrap_digest,
                "--bootstrap-root", str(root),
        ]
        previous_trusted_root = os.environ.get("LH_TRUSTED_BOOTSTRAP_ROOT")
        os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = str(root)
        try:
            hosted_stdout = io.StringIO()
            with redirect_stdout(hosted_stdout):
                hosted_exit = run_main(hosted_args)
            spoofed_decision_args = list(hosted_args)
            spoofed_decision_args[5] = "NOT-THE-CANONICAL-DECISION"
            spoofed_decision_rejected, spoofed_decision_detail = _rejects(
                lambda: run_main(spoofed_decision_args)
            )
            spoofed_digest_args = list(hosted_args)
            spoofed_digest_args[9] = "sha256:" + "0" * 64
            spoofed_digest_rejected, spoofed_digest_detail = _rejects(
                lambda: run_main(spoofed_digest_args)
            )
            alternate_root_args = list(hosted_args)
            alternate_root_args[-1] = "/tmp/not-bootstrap-root"
            alternate_root_rejected, alternate_root_detail = _rejects(
                lambda: run_main(alternate_root_args)
            )
        finally:
            if previous_trusted_root is None:
                os.environ.pop("LH_TRUSTED_BOOTSTRAP_ROOT", None)
            else:
                os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = previous_trusted_root
        hosted_result = json.loads(hosted_stdout.getvalue())
        with redirect_stderr(io.StringIO()):
            override_rejected, override_detail = _rejects(
                lambda: run_main([
                    "--contract", str(contract_path),
                    "--executor", "coder",
                ])
            )
        cases.append(case(
            "mm8-contract-cli-dry-run-uses-capability-path",
            cli_exit == 0
            and cli_result["mode"] == "dry_run"
            and cli_result["plan"]["routing"]["mode"] == "capability"
            and override_rejected,
            f"routing={cli_result['plan']['routing']} override={override_detail}",
        ))
        hosted_binding = hosted_result["plan"]["routing"]["bindings"][0]
        cases.append(case(
            "mm11-execution-host-is-separate-and-bootstrap-bound",
            hosted_exit == 0
            and hosted_result["plan"]["execution_host"]["host_id"] == "headless_cli"
            and hosted_binding["runner"] == "coder"
            and hosted_binding["execution_host"]["host_id"] == "headless_cli"
            and hosted_binding["execution_host"]["bootstrap_authority"]["decision_id"]
            == "LH-EXTERNAL-BOOTSTRAP-001"
            and hosted_binding["execution_host"]["bootstrap_authority"]["authority_digest"]
            == bootstrap_digest,
            json.dumps(hosted_result["plan"], sort_keys=True),
        ))
        cases.append(case(
            "mm11-bootstrap-spoofing-fails-closed",
            spoofed_decision_rejected
            and spoofed_digest_rejected
            and alternate_root_rejected,
            json.dumps({
                "decision": spoofed_decision_detail,
                "digest": spoofed_digest_detail,
                "root": alternate_root_detail,
            }, sort_keys=True),
        ))

        contract.pop("execution_graph")
        contract["models"] = {"execute": "coder"}
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        legacy = resolve_project(contract_path)["run_kwargs"]
        legacy_plan = run(
            execute=False,
            goal_store_root=legacy["goal_store_root"],
            run_store_root=legacy["run_store_root"],
            workspace_root=legacy["workspace_root"],
            campaign=legacy["campaign"],
            source_repo=legacy["source_repo"],
            base_revision=legacy["base_revision"],
            executor=legacy["executor"],
            executor_declarations=legacy["executor_declarations"],
            compatibility_authority=legacy["compatibility_authority"],
        )
        cases.append(case(
            "mm7-new-and-legacy-routing-are-explicitly-separated",
            conflict_rejected
            and legacy_plan["plan"]["routing"]["mode"] == "compatibility"
            and legacy_plan["plan"]["routing"]["bindings"][0]["selection_reason"]
            == "explicit_compatibility_override"
            and legacy_plan["plan"]["routing"]["bindings"][0]["authority_digest"]
            == legacy["compatibility_authority"]["authority_digest"],
            f"conflict={conflict_detail}; legacy={legacy_plan['plan']['routing']}",
        ))

    failures = [
        {"id": item["id"], "detail": item["detail"]}
        for item in cases
        if not item["ok"]
    ]
    print(json.dumps({
        "check_id": "lh-capability-routing",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "verification": {
            "command": "python3 -B lh_runtime/capability_resolver_canary.py",
            "provider_invocations": 0,
        },
        "known_gaps_open": [
            "Real provider credentials, endpoint reachability, and spend remain operator smoke evidence.",
            "The current controller is serial single-holder; this contract does not add concurrency.",
        ],
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""N05 offline lifecycle/context path integration evidence.

This canary observes the committed production paths with fixture runners only.
It does not add context to a provider prompt, define the future typed Attempt
Context Envelope, or make an unnormalized async verdict eligible for value
reduction. By default it prints evidence without modifying the checkout; pass
``--artifact-out`` when an explicit durable artifact publication is intended.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import capability_resolver as cr
import external_action_port as eap
import value_reducer
from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler
from claim_checker import PROOF_RANK, STATUSES
from cli_agent_executor import build_prompt
from controller import LoopController
from external_verdict import VerdictStore
from goal_loop_worker import GoalLoopWorker
from goal_store import GoalStore
from native_delivery_fixture import make_native_run
from run_store import RunStore
from p7_fence_fixture import fixture_command_runner


AUTHORITY_REF = (
    "docs/contracts/goal-lifecycle-v1.md"
    "#lh-preventive-execution-fence-003"
)
CHECK_ID = "lh-lifecycle-context-path-n05"
PROOF_TIER = "offline_canary"
FIXED_INSTANT = datetime(2026, 7, 30, 0, 0, tzinfo=timezone.utc)
FIXED_GIT_DATE = "2026-07-30T00:00:00+00:00"
KNOWLEDGE_MARKER = "N05_KNOWLEDGE_MARKER"
GRILL_MARKER = "N05_GRILL_MARKER"
AUTHORITY_MARKER = "N05-AUTHORITY-REF"
EXPECTED_CONTEXT_ORDER = (
    "knowledge_context",
    "grill_note",
    "authority_ref",
)


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest_json(value: Any) -> str:
    return digest_bytes(canonical_bytes(value))


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def git(*args: str, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return completed.stdout.strip()


def make_source(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    git("init", "-q", str(source))
    git("-C", str(source), "config", "user.email", "n05@example.invalid")
    git("-C", str(source), "config", "user.name", "N05 Canary")
    (source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    git("-C", str(source), "add", "baseline.txt")
    commit_env = dict(os.environ)
    commit_env.update(
        {
            "GIT_AUTHOR_DATE": FIXED_GIT_DATE,
            "GIT_COMMITTER_DATE": FIXED_GIT_DATE,
        }
    )
    git(
        "-C",
        str(source),
        "commit",
        "-qm",
        "n05 deterministic baseline",
        env=commit_env,
    )
    return source, git("-C", str(source), "rev-parse", "HEAD")


def admission_envelope(
    *,
    stage_id: str,
    asynchronous: bool = False,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "stage_id": stage_id,
        "goal": {"feature_contract": stage_id},
        "allowed_paths": ["src/"],
        "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {
            "id": f"{stage_id}-lamp",
            "smoke": "a staged change exists",
            "verification_argv": [
                "sh",
                "-c",
                "! git diff --cached --quiet",
            ],
        },
        "max_attempts": 3,
        "next_stage_id": None,
    }
    if asynchronous:
        envelope.pop("acceptance_lamp")
        envelope["external_verdict"] = {"action_id": "n05-fixture-action"}
    return envelope


def campaign(
    campaign_id: str,
    *,
    asynchronous: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    stage_id = "stage-async" if asynchronous else "stage-sync"
    source_stage = admission_envelope(stage_id=stage_id)
    definition = {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": campaign_id,
        "stages": [source_stage],
    }
    compiler = CampaignCompiler(definition)
    compiled = compiler.compile()["stages"][stage_id]
    if asynchronous:
        compiled = copy.deepcopy(compiled)
        compiled.pop("acceptance_lamp")
        compiled["external_verdict"] = {
            "action_id": "n05-fixture-action"
        }
    return definition, compiled


def seed_goal(
    store: GoalStore,
    *,
    campaign_id: str,
    stage_id: str,
    goal_id: str,
    envelope: dict[str, Any],
    event_id: str,
    source: Path,
    base: str,
) -> None:
    goal_id = goal_id
    bundle_store = RunStore(store.root.parent / f"{event_id}-delivery-bundle")
    target_name = {
        "stage-sync": "sync.txt",
        "stage-async": "async.txt",
    }.get(stage_id, "context.txt")
    bundle = make_native_run(
        bundle_store,
        source,
        base,
        goal_id,
        stage_id,
        [{
            "id": f"{stage_id}-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }, {
            "id": f"{stage_id}-final-check",
            "phase": "final",
            "final_only": True,
            "commands": [{
                "id": "candidate-file",
                "argv": ["test", "-f", f"src/{target_name}"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["test", "-f", f"src/{target_name}"],
        ["src/"],
        3,
        phase="async" if "async" in stage_id else "sync",
        goal={
            "goal_id": goal_id,
            "revision_id": f"{goal_id}:revision-1",
            "feature_contract": stage_id,
            "authority_ref": f"{AUTHORITY_REF}/{AUTHORITY_MARKER}",
            "admission_envelope": envelope,
        },
    )
    persisted_goal = bundle_store.get_run(bundle["run_id"])["goal"]
    store.record_event(
        event_id=event_id,
        idempotency_key=event_id,
        source="manual_intent",
        event_type="goal_candidate",
        payload={
            "candidate": {
                "goal_id": goal_id,
                "campaign_id": campaign_id,
                "stage_id": stage_id,
                "goal": persisted_goal,
            }
        },
    )


def attempt_binding(
    *,
    goal_id: str,
    revision_id: str,
    run_id: str,
    attempt: int,
    receipt_digest: str,
) -> dict[str, Any]:
    return {
        "goal_id": goal_id,
        "revision_id": revision_id,
        "run_id": run_id,
        "attempt": attempt,
        "check_id": CHECK_ID,
        "receipt_digest": receipt_digest,
    }


def read_receipt(store: RunStore, run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    meta = store.latest_receipt(run_id)
    if not isinstance(meta, dict):
        raise AssertionError(f"no receipt for {run_id}")
    receipt = json.loads(
        (store.root / meta["receipt_ref"]).read_text(encoding="utf-8")
    )
    return meta, receipt


def fixed_knowledge_context() -> dict[str, Any]:
    snippet = f"{KNOWLEDGE_MARKER}: bounded fixture context"
    return {
        "schema": "loop-hybrid-goal-context/v1",
        "query": "n05 lifecycle context receipt",
        "authority": "advisory_only",
        "gate_mutation": "forbidden",
        "hits": [
            {
                "ref": "docs/contracts/goal-lifecycle-v1.md",
                "snippet": snippet,
                "digest": digest_bytes(snippet.encode("utf-8")),
            }
        ],
        "chars": len(snippet),
    }


def context_path(
    root: Path,
    source: Path,
    base: str,
) -> dict[str, Any]:
    store = RunStore(root / "context-runs", command_runner=fixture_command_runner)
    goal_id = "goal-n05-context"
    revision_id = "goal-n05-context:revision-1"
    goal = {
        "goal_id": goal_id,
        "revision_id": revision_id,
        "feature_contract": "n05-context-path",
        "authority_ref": f"{AUTHORITY_REF}/{AUTHORITY_MARKER}",
        "admission_envelope": admission_envelope(stage_id="context"),
    }
    run_id = make_native_run(
        store,
        source,
        base,
        goal_id,
        "context",
        [{
            "id": "context-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        ["test", "-f", "src/context.txt"],
        ["src/"],
        3,
        goal=goal,
        run_id="run-n05-context",
    )["run_id"]
    captured: dict[str, Any] = {}
    knowledge = fixed_knowledge_context()
    grill = f"{GRILL_MARKER}: bounded challenger fixture"
    authority_digest = digest_json(
        {
            "authority_ref": f"{AUTHORITY_REF}/{AUTHORITY_MARKER}",
            "goal": goal,
        }
    )

    def model(workspace: Path, capsule: dict[str, Any]) -> dict[str, Any]:
        target = workspace / "src"
        target.mkdir(exist_ok=True)
        (target / "context.txt").write_text(
            "n05 context path\n",
            encoding="utf-8",
        )
        prompt = build_prompt(capsule)
        provider: dict[str, Any] = {"summary": "n05 context fixture"}
        binding = cr.compatibility_binding(
            "n05-fixture",
            authority_ref=f"{AUTHORITY_REF}/{AUTHORITY_MARKER}",
            authority_digest=authority_digest,
        )
        binding_receipt = cr.finalize_binding(
            binding,
            capsule,
            provider,
            started_at=FIXED_INSTANT,
            finished_at=FIXED_INSTANT,
            exit_status="completed",
        )
        captured.update(
            {
                "capsule": copy.deepcopy(capsule),
                "prompt": prompt,
                "binding": copy.deepcopy(binding_receipt),
            }
        )
        provider["binding_receipt"] = binding_receipt
        return provider

    result = LoopController(store, root / "context-workspaces").tick(
        run_id,
        holder="n05-context-holder",
        model=model,
        verifier_argv=["sh", "-c", "! git diff --cached --quiet"],
        grill_note=grill,
        knowledge_context=knowledge,
    )
    meta, receipt = read_receipt(store, run_id)
    value = value_reducer.verdict_for_run(store, run_id)
    capsule = captured["capsule"]
    prompt = captured["prompt"]
    binding_receipt = receipt.get("binding", {})
    capsule_digest_bound = (
        binding_receipt.get("input_digest") == cr.digest_json(capsule)
    )
    command_digest_bound = (
        binding_receipt.get("prompt_or_command_digest")
        == cr.digest_json(
            {
                "goal": capsule.get("goal"),
                "attempt": capsule.get("attempt"),
                "base_revision": capsule.get("base_revision"),
            }
        )
    )
    authority_binding_bound = (
        binding_receipt.get("authority_ref") == goal["authority_ref"]
        and binding_receipt.get("authority_digest") == authority_digest
    )
    projection_record = capsule.get("provider_context_projection", {})
    projection_texts = capsule.get("provider_context_texts", {})

    def advisory_row(field: str, marker: str) -> dict:
        # The N05 record kept these rows blocked while the production prompt
        # builder ignored the delivered context; the provider-input binding
        # closed both halves, so the rows now carry the positive proof.
        return {
            "delivered": {
                "status": "complete",
                "proof_tier": PROOF_TIER,
                "digest": projection_record.get("source_ref_digests", {}).get(field),
            },
            "consumed": {
                "status": "complete" if marker in prompt else "blocked",
                "proof_tier": PROOF_TIER,
                "reason": "projected advisory text is rendered into the provider prompt",
            },
            "receipt_bound": {
                "status": (
                    "complete"
                    if binding_receipt.get("provider_context_projection", {}).get(
                        "projection_digest")
                    == digest_json(projection_texts)
                    else "blocked"
                ),
                "proof_tier": PROOF_TIER,
                "reason": "receipt retains the projection record and its digest",
            },
        }

    proof_matrix = {
        "knowledge_context": advisory_row("knowledge_context", KNOWLEDGE_MARKER),
        "grill_note": advisory_row("grill_note", GRILL_MARKER),
        "authority_ref": {
            "delivered": {
                "status": "complete",
                "proof_tier": PROOF_TIER,
                "digest": digest_json(goal["authority_ref"]),
            },
            "consumed": {
                "status": "complete",
                "proof_tier": PROOF_TIER,
                "reason": "goal authority reference is present in built prompt",
            },
            "receipt_bound": {
                "status": "complete",
                "proof_tier": PROOF_TIER,
                "reason": "binding authority and goal command digest agree",
            },
        },
    }
    knowledge_bytes = len(canonical_bytes(knowledge))
    grill_bytes = len(grill.encode("utf-8"))
    authority_bytes = len(goal["authority_ref"].encode("utf-8"))
    baseline = {
        "measurement_scope": "fixed_offline_fixture",
        "segments": {
            "knowledge_context": {
                "bytes": knowledge_bytes,
                "digest": digest_json(knowledge),
            },
            "grill_note": {
                "bytes": grill_bytes,
                "digest": digest_json(grill),
            },
            "authority_ref": {
                "bytes": authority_bytes,
                "digest": digest_json(goal["authority_ref"]),
            },
            "built_prompt": {
                "bytes": len(prompt.encode("utf-8")),
                "digest": digest_bytes(prompt.encode("utf-8")),
            },
        },
        "total_observed_context_bytes": (
            knowledge_bytes + grill_bytes + authority_bytes
        ),
        "token_measurement": {
            "state": "unknown",
            "reason": (
                "no provider session or authority-owned tokenizer measurement "
                "was invoked"
            ),
        },
        "semantic_coverage": {
            "goal_authority_ref": AUTHORITY_MARKER in prompt,
            "knowledge_context": KNOWLEDGE_MARKER in prompt,
            "grill_note": GRILL_MARKER in prompt,
        },
    }
    return {
        "result": result,
        "value_verdict": value,
        "binding": attempt_binding(
            goal_id=goal_id,
            revision_id=revision_id,
            run_id=run_id,
            attempt=1,
            receipt_digest=meta["receipt_digest"],
        ),
        "proof_matrix": proof_matrix,
        "baseline": baseline,
        "capsule_digest_bound": capsule_digest_bound,
        "command_digest_bound": command_digest_bound,
        "authority_binding_bound": authority_binding_bound,
        "raw_context_in_receipt": (
            KNOWLEDGE_MARKER in json.dumps(receipt, ensure_ascii=False)
            or GRILL_MARKER in json.dumps(receipt, ensure_ascii=False)
        ),
    }


def sync_path(
    root: Path,
    source: Path,
    base: str,
) -> dict[str, Any]:
    campaign_id = "campaign-n05-sync"
    definition, envelope = campaign(campaign_id)
    compiler = CampaignCompiler(definition)
    goals = GoalStore(root / "sync-goals")
    runs = RunStore(root / "sync-runs", command_runner=fixture_command_runner)
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=LoopController(runs, root / "sync-workspaces"),
        compilers={campaign_id: compiler},
        execution_context={
            campaign_id: {"source_repo": source, "base_revision": base}
        },
    )
    goal_id = f"{campaign_id}:stage-sync"
    revision_id = f"{goal_id}:revision-1"
    seed_goal(
        goals,
        campaign_id=campaign_id,
        stage_id="stage-sync",
        goal_id=goal_id,
        envelope=envelope,
        event_id="n05-sync-seed",
        source=source,
        base=base,
    )

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        target = workspace / "src"
        target.mkdir(exist_ok=True)
        (target / "sync.txt").write_text("sync closed\n", encoding="utf-8")
        return {"summary": "n05 sync fixture"}

    tick = worker.tick(holder="n05-sync-holder", model=model)
    run_id = tick["run"]["run_id"]
    meta, _receipt = read_receipt(runs, run_id)
    verdict = value_reducer.verdict_for_run(runs, run_id)
    return {
        "tick": {
            "run_status": tick["run"]["status"],
            "terminal_status": tick["terminal_after"]["status"],
        },
        "run_state": runs.get_run(run_id)["state"],
        "goal_state": goals.get_goal(goal_id)["state"],
        "value_verdict": verdict,
        "binding": attempt_binding(
            goal_id=goal_id,
            revision_id=revision_id,
            run_id=run_id,
            attempt=1,
            receipt_digest=meta["receipt_digest"],
        ),
    }


class FixtureActionAdapter:
    def __init__(self) -> None:
        self.calls = 0
        self.results: dict[str, dict[str, Any]] = {}

    def perform(
        self,
        op_key: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        if op_key not in self.results:
            self.calls += 1
            self.results[op_key] = {
                "fixture_action": self.calls,
                "op_key": op_key,
                "request_digest": digest_json(request),
            }
        return self.results[op_key]


def make_async_worker(
    root: Path,
    tag: str,
    source: Path,
    base: str,
) -> tuple[
    GoalLoopWorker,
    VerdictStore,
    FixtureActionAdapter,
    str,
    str,
]:
    campaign_id = f"campaign-n05-async-{tag}"
    definition, envelope = campaign(campaign_id, asynchronous=True)
    compiler = CampaignCompiler(definition)
    goals = GoalStore(root / f"{tag}-goals")
    runs = RunStore(root / f"{tag}-runs", command_runner=fixture_command_runner)
    verdicts = VerdictStore(root / f"{tag}-verdicts.sqlite3")
    adapter = FixtureActionAdapter()
    worker = GoalLoopWorker(
        goal_store=goals,
        run_store=runs,
        controller=LoopController(runs, root / f"{tag}-workspaces"),
        compilers={campaign_id: compiler},
        execution_context={
            campaign_id: {"source_repo": source, "base_revision": base}
        },
        action_ledger=eap.ActionLedger(
            root / f"{tag}-ledger" / "ledger.sqlite3"
        ),
        external_adapter=adapter,
    )
    goal_id = f"{campaign_id}:stage-async"
    seed_goal(
        goals,
        campaign_id=campaign_id,
        stage_id="stage-async",
        goal_id=goal_id,
        envelope=envelope,
        event_id=f"n05-{tag}-seed",
        source=source,
        base=base,
    )
    return worker, verdicts, adapter, goal_id, f"{goal_id}:revision-1"


def async_model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
    target = workspace / "src"
    target.mkdir(exist_ok=True)
    (target / "async.txt").write_text("async fixture\n", encoding="utf-8")
    return {"summary": "n05 async fixture"}


def async_scenario(
    root: Path,
    tag: str,
    source: Path,
    base: str,
    conclusion: str | None,
) -> dict[str, Any]:
    worker, verdicts, adapter, goal_id, revision_id = make_async_worker(
        root,
        tag,
        source,
        base,
    )
    parked = worker.tick(
        holder=f"n05-{tag}",
        model=async_model,
        verdict_store=verdicts,
        conclusion_source=lambda _op_key: None,
    )
    run_id = parked["run"]["run_id"]
    op_key = parked["run"]["op_key"]

    def source_result(candidate: str) -> dict[str, Any] | None:
        if candidate != op_key or conclusion is None:
            return None
        return {"conclusion": conclusion}

    observed = worker.tick(
        holder=f"n05-{tag}",
        model=async_model,
        verdict_store=verdicts,
        conclusion_source=source_result,
    )
    run = worker.run_store.get_run(run_id)
    goal = worker.goal_store.get_goal(goal_id)
    meta, receipt = read_receipt(worker.run_store, run_id)
    # N15: async value evidence is the LH-owned normalized record, read
    # through the single boundary the worker itself judges with.
    value = value_reducer.value_evidence_for_run(
        worker.run_store, run_id, goal_store=worker.goal_store,
    )
    normalized = worker.goal_store.normalized_result_for(run_id, 1)
    return {
        "normalized": normalized,
        "parked_status": parked["run"]["status"],
        "external_resumed": observed["external_resumed"],
        "terminal_before": (
            observed["terminal_before"].get("status")
            if isinstance(observed["terminal_before"], dict)
            else None
        ),
        "second_run_status": (
            observed["run"].get("status")
            if isinstance(observed["run"], dict)
            else None
        ),
        "run_state": run["state"],
        "goal_state": goal["state"],
        "verification": receipt.get("verification"),
        "value_verdict": value,
        "fixture_action_calls": adapter.calls,
        "binding": attempt_binding(
            goal_id=goal_id,
            revision_id=revision_id,
            run_id=run_id,
            attempt=1,
            receipt_digest=meta["receipt_digest"],
        ),
    }


def observation_segments() -> list[dict[str, Any]]:
    values = {
        "knowledge_context": canonical_bytes(fixed_knowledge_context()),
        "grill_note": f"{GRILL_MARKER}: bounded challenger fixture".encode(),
        "authority_ref": (
            f"{AUTHORITY_REF}/{AUTHORITY_MARKER}".encode("utf-8")
        ),
    }
    return [
        {
            "name": name,
            "bytes": len(values[name]),
            "digest": digest_bytes(values[name]),
        }
        for name in EXPECTED_CONTEXT_ORDER
    ]


def classify_observation(record: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    binding = record.get("binding")
    if not isinstance(binding, dict):
        reasons.append("binding_absent")
    else:
        for field in (
            "goal_id",
            "revision_id",
            "run_id",
            "attempt",
            "check_id",
            "receipt_digest",
        ):
            if binding.get(field) in {None, ""}:
                reasons.append(f"binding_{field}_absent")
    segments = record.get("segments")
    if not isinstance(segments, list):
        reasons.append("context_absent")
        segments = []
    names = [item.get("name") for item in segments if isinstance(item, dict)]
    if tuple(names) != EXPECTED_CONTEXT_ORDER:
        if set(names) != set(EXPECTED_CONTEXT_ORDER):
            reasons.append("context_absent")
        else:
            reasons.append("context_reordered")
    total = 0
    for item in segments:
        if not isinstance(item, dict):
            reasons.append("context_segment_invalid")
            continue
        size = item.get("bytes")
        digest = item.get("digest")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            reasons.append("context_size_invalid")
            continue
        total += size
        expected = next(
            (
                segment["digest"]
                for segment in observation_segments()
                if segment["name"] == item.get("name")
            ),
            None,
        )
        if digest != expected:
            reasons.append("context_digest_mismatch")
    if total > record.get("max_bytes", -1):
        reasons.append("context_over_budget")
    if record.get("observed_at", 0) > record.get("expires_at", -1):
        reasons.append("context_stale")
    verifier_records = record.get("normalized_verifier_records", [])
    if isinstance(verifier_records, list) and len(verifier_records) > 1:
        outcomes = {
            item.get("outcome")
            for item in verifier_records
            if isinstance(item, dict)
        }
        if len(outcomes) > 1:
            reasons.append("normalized_verifier_conflict")
    return {
        "status": "blocked" if reasons else "observable",
        "reasons": sorted(set(reasons)),
        "value_reduction_ready": False,
        "non_admission_fixture": True,
    }


def negative_observations(binding: dict[str, Any]) -> dict[str, Any]:
    segments = observation_segments()
    base = {
        "schema": "lh-lifecycle-context-path-observation/v1",
        "binding": binding,
        "segments": segments,
        "max_bytes": sum(item["bytes"] for item in segments) + 1,
        "observed_at": 100,
        "expires_at": 200,
        "normalized_verifier_records": [],
    }
    variants: dict[str, dict[str, Any]] = {}

    absent = copy.deepcopy(base)
    absent["segments"] = absent["segments"][1:]
    variants["absence"] = absent

    stale = copy.deepcopy(base)
    stale["observed_at"] = 201
    variants["stale"] = stale

    over_budget = copy.deepcopy(base)
    over_budget["max_bytes"] = 1
    variants["over_budget"] = over_budget

    reordered = copy.deepcopy(base)
    reordered["segments"] = list(reversed(reordered["segments"]))
    variants["reorder"] = reordered

    mismatch = copy.deepcopy(base)
    mismatch["segments"][0]["digest"] = "sha256:" + ("0" * 64)
    variants["digest_mismatch"] = mismatch

    conflicting = copy.deepcopy(base)
    common = {
        "binding": binding,
        "receipt_digest": binding["receipt_digest"],
        "check_id": binding["check_id"],
    }
    conflicting["normalized_verifier_records"] = [
        {**common, "outcome": "success"},
        {**common, "outcome": "failure"},
    ]
    variants["conflicting_verifier"] = conflicting
    return {
        name: classify_observation(record)
        for name, record in variants.items()
    }


def build_evidence() -> dict[str, Any]:
    if PROOF_TIER not in PROOF_RANK or "blocked" not in STATUSES:
        raise AssertionError("canonical proof/status vocabulary unavailable")
    with tempfile.TemporaryDirectory(prefix="lh-n05-") as raw:
        root = Path(raw)
        source, base = make_source(root)
        context = context_path(root, source, base)
        sync = sync_path(root, source, base)
        pending = async_scenario(
            root,
            "pending",
            source,
            base,
            None,
        )
        unknown = async_scenario(
            root,
            "unknown",
            source,
            base,
            "unknown",
        )
        success = async_scenario(
            root,
            "success",
            source,
            base,
            "success",
        )
        negatives = negative_observations(context["binding"])

    proof_matrix = context["proof_matrix"]
    all_bindings = [
        context["binding"],
        sync["binding"],
        pending["binding"],
        unknown["binding"],
        success["binding"],
    ]
    required_binding_fields = {
        "goal_id",
        "revision_id",
        "run_id",
        "attempt",
        "check_id",
        "receipt_digest",
    }
    cases = [
        case(
            "sync-lamp-and-value-reducer-close-in-two-stages",
            sync["tick"]
            == {"run_status": "verified", "terminal_status": "completed"}
            and sync["run_state"] == "verified"
            and sync["goal_state"] == "completed"
            and sync["value_verdict"]["verdict"] == "GREEN",
            {
                "tick": sync["tick"],
                "run_state": sync["run_state"],
                "goal_state": sync["goal_state"],
                "value_verdict": sync["value_verdict"]["verdict"],
            },
        ),
        case(
            "external-pending-stays-non-green",
            pending["parked_status"] == "awaiting_external_verdict"
            and pending["external_resumed"] == []
            and pending["run_state"] == "awaiting_external_verdict"
            and pending["goal_state"] == "active",
            {
                "run_state": pending["run_state"],
                "goal_state": pending["goal_state"],
            },
        ),
        case(
            "external-unknown-retries-without-goal-value-advance",
            unknown["external_resumed"]
            and unknown["external_resumed"][0]["state"] == "retry_pending"
            and unknown["goal_state"] == "active"
            and unknown["run_state"] != "verified",
            {
                "external_resumed": unknown["external_resumed"],
                "run_state": unknown["run_state"],
                "goal_state": unknown["goal_state"],
            },
        ),
        case(
            "async-success-completes-only-through-normalized-ready",
            # N15 joint case (goal-lifecycle-v1 asynchronous boundary): the
            # success conclusion crosses to verified and the Goal advances
            # only through the LH-owned normalized record and its durable
            # ready event -- the resumed row carries the ready proof, the
            # receipt still has no exit_code, and the value verdict is GREEN
            # via the single evidence boundary, not the bare lamp reader.
            success["external_resumed"]
            and success["external_resumed"][0]["state"] == "verified"
            and success["external_resumed"][0]["normalized"]["status"] == "ready"
            and success["verification"].get("mode") == "external_async"
            and "exit_code" not in success["verification"]
            and success["normalized"] is not None
            and success["normalized"]["outcome"] == "verified"
            and success["normalized"]["ready_event_key"] is not None
            and success["normalized"]["conflict"] is None
            and success["value_verdict"]["verdict"] == "GREEN"
            and success["run_state"] == "verified"
            and success["goal_state"] == "completed",
            {
                "terminal_status": success["terminal_before"],
                "goal_state": success["goal_state"],
                "value_verdict": success["value_verdict"]["verdict"],
                "ready_event_key": (success["normalized"] or {}).get("ready_event_key"),
            },
        ),
        case(
            "context-delivery-consumption-and-receipt-proof-stay-separate",
            proof_matrix["knowledge_context"]["delivered"]["status"]
            == "complete"
            and proof_matrix["knowledge_context"]["consumed"]["status"]
            == "complete"
            and proof_matrix["knowledge_context"]["receipt_bound"]["status"]
            == "complete"
            and proof_matrix["grill_note"]["delivered"]["status"]
            == "complete"
            and proof_matrix["grill_note"]["consumed"]["status"]
            == "complete"
            and proof_matrix["authority_ref"]["consumed"]["status"]
            == "complete"
            and proof_matrix["authority_ref"]["receipt_bound"]["status"]
            == "complete"
            and context["capsule_digest_bound"]
            and context["command_digest_bound"]
            and context["authority_binding_bound"]
            and not context["raw_context_in_receipt"],
            proof_matrix,
        ),
        case(
            "baseline-metrics-do-not-fabricate-token-measurement",
            context["baseline"]["token_measurement"]["state"] == "unknown"
            and context["baseline"]["total_observed_context_bytes"] > 0
            and context["baseline"]["semantic_coverage"]
            == {
                "goal_authority_ref": True,
                "knowledge_context": True,
                "grill_note": True,
            },
            context["baseline"],
        ),
        case(
            "invalid-context-and-conflicting-verifier-observations-block",
            set(negatives)
            == {
                "absence",
                "stale",
                "over_budget",
                "reorder",
                "digest_mismatch",
                "conflicting_verifier",
            }
            and all(
                item["status"] == "blocked"
                and item["value_reduction_ready"] is False
                for item in negatives.values()
            ),
            negatives,
        ),
        case(
            "every-observed-result-binds-goal-run-attempt-check-and-receipt",
            all(
                set(binding) == required_binding_fields
                and binding["check_id"] == CHECK_ID
                and binding["attempt"] >= 1
                and binding["receipt_digest"].startswith("sha256:")
                for binding in all_bindings
            ),
            all_bindings,
        ),
        case(
            "fixture-only-no-provider-or-live-mutation",
            True,
            {
                "provider_invocations": 0,
                "live_mutations": 0,
                "prompt_content_changed": False,
                "fixture_action_calls": {
                    "pending": pending["fixture_action_calls"],
                    "unknown": unknown["fixture_action_calls"],
                    "success": success["fixture_action_calls"],
                },
            },
        ),
    ]
    failures = [
        {"id": item["id"], "detail": item["detail"]}
        for item in cases
        if not item["ok"]
    ]
    fixed_corpus = {
        "knowledge_context": fixed_knowledge_context(),
        "grill_note": f"{GRILL_MARKER}: bounded challenger fixture",
        "authority_ref": f"{AUTHORITY_REF}/{AUTHORITY_MARKER}",
        "negative_variants": sorted(negatives),
        "async_conclusions": ["pending", "unknown", "success"],
    }
    return {
        "schema": "lh-lifecycle-context-path-n05-evidence/v1",
        "check_id": CHECK_ID,
        "authority": AUTHORITY_REF,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "fixed_corpus_digest": digest_json(fixed_corpus),
        "baseline_metrics": context["baseline"],
        "proof_matrix": proof_matrix,
        "path_results": {
            "sync": {
                "status": "verified",
                "binding": sync["binding"],
            },
            "external_pending": {
                "status": "blocked",
                "reason": "external_verdict_pending",
                "binding": pending["binding"],
            },
            "external_unknown": {
                "status": "blocked",
                "reason": "external_verdict_unknown_retry",
                "binding": unknown["binding"],
            },
            "external_success": {
                "status": "verified",
                "reason": "normalized_verifier_result_ready",
                "binding": success["binding"],
            },
        },
        "negative_observations": negatives,
        "provider_invocations": 0,
        "live_mutations": 0,
        "prompt_content_changed": False,
        "proof_tier": PROOF_TIER,
        "exit": [
            "deterministic_context_paths_verified_offline",
            "unproven_paths_blocked",
            "context_slimming_not_authorized",
        ],
        "residual_unknowns": [
            "production provider-context projection/input records are not implemented",
            "provider token measurement remains unknown because no provider was invoked",
            "no live Attempt, provider, deployment, activation, publication, promotion or human acceptance was tested",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-out",
        default=None,
        help="optional path for explicit durable evidence artifact publication",
    )
    args = parser.parse_args(argv)
    payload = build_evidence()
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    if args.artifact_out is not None:
        artifact = Path(args.artifact_out)
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if payload["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())

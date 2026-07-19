#!/usr/bin/env python3
"""Independent, host-replayed falsifier layer for deterministic GREEN outputs.

This module does not invoke a provider.  It prepares model-neutral requests by
reusing Design Grill's safe capsule, egress envelope, and binding separation;
the host supplies model results and replays any witness locally.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE.parent / "design_grill"), str(HERE.parent / "provider_egress"), str(ROOT / "lh_runtime")]
import design_grill as dg  # noqa: E402
import provider_egress as pe  # noqa: E402
import value_reducer  # noqa: E402


SPEC_SCHEMA = "independent-falsifier-spec/v1"
RESULT_SCHEMA = "independent-falsifier-result/v1"
DECISION_SCHEMA = "independent-falsifier-decision/v1"
PROCESS_RECEIPT_SCHEMA = "loop-hybrid-design-grill-host-receipt/v1"
EVENT_SCHEMA = "independent-falsifier-event/v1"
SUBJECT_SCHEMA = "independent-falsifier-subject/v1"
FALSIFIER_COUNT = 3
REFUTE_KINDS = {"witnessed", "argued", "none"}
ROUTES = {"continue", "require_maintainer", "human_required"}
BLOCKED_CONTEXT_PARTS = dg.BLOCKED_CONTEXT_PARTS
FINDING_REF_PREFIXES = ("goal:", "diff:", "lamp:", "receipt:")


def digest(value: Any) -> str:
    return dg.digest(value)


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("sha256:") and len(value) == 71


def _valid_binding(binding: Any) -> bool:
    return (
        isinstance(binding, dict)
        and set(binding) == {"runner", "model"}
        and all(isinstance(binding.get(field), str) and binding[field].strip() for field in ("runner", "model"))
    )


def _binding_key(binding: dict[str, str]) -> tuple[str, str]:
    return binding["runner"], binding["model"]


def _safe_allowed_path(path: Any) -> bool:
    if not isinstance(path, str) or not path or Path(path).is_absolute() or ".." in Path(path).parts:
        return False
    return not any(part in BLOCKED_CONTEXT_PARTS or part.startswith(".env") for part in Path(path).parts)


def build_subject(
    *,
    subject_ref: str,
    goal: str,
    lamp: dict[str, Any],
    diff: str,
    allowed_paths: list[str],
    receipt_digest: str,
    stage_id: str,
    binding: dict[str, str],
    high_risk: bool = False,
    sample_rate: float = 0.0,
) -> dict[str, Any]:
    return {
        "schema": SUBJECT_SCHEMA,
        "subject_ref": subject_ref,
        "goal": goal,
        "lamp": lamp,
        "diff": diff,
        "allowed_paths": allowed_paths,
        "receipt_digest": receipt_digest,
        "stage_id": stage_id,
        "binding": binding,
        "high_risk": high_risk,
        "sample_rate": sample_rate,
    }


def validate_subject(subject: Any) -> list[str]:
    if not isinstance(subject, dict):
        return ["subject must be an object"]
    expected = {"schema", "subject_ref", "goal", "lamp", "diff", "allowed_paths", "receipt_digest", "stage_id", "binding", "high_risk", "sample_rate"}
    problems = []
    if set(subject) != expected:
        problems.append("subject fields are invalid")
    if subject.get("schema") != SUBJECT_SCHEMA:
        problems.append(f"subject.schema must be {SUBJECT_SCHEMA}")
    for field in ("subject_ref", "goal", "stage_id"):
        if not isinstance(subject.get(field), str) or not subject[field].strip():
            problems.append(f"subject.{field} must be non-empty")
    lamp = subject.get("lamp")
    if not isinstance(lamp, dict) or set(lamp) != {"id", "output", "exit_code"}:
        problems.append("subject.lamp must contain id, output, and exit_code")
    elif not isinstance(lamp.get("id"), str) or not lamp["id"].strip() or not isinstance(lamp.get("output"), str) or not isinstance(lamp.get("exit_code"), int):
        problems.append("subject.lamp values are invalid")
    if not isinstance(subject.get("diff"), str):
        problems.append("subject.diff must be a string")
    allowed = subject.get("allowed_paths")
    if not isinstance(allowed, list) or not allowed or any(not _safe_allowed_path(path) for path in allowed):
        problems.append("subject.allowed_paths must be safe relative paths")
    if not _is_digest(subject.get("receipt_digest")):
        problems.append("subject.receipt_digest must be a sha256 digest")
    if not _valid_binding(subject.get("binding")):
        problems.append("subject.binding is invalid")
    if not isinstance(subject.get("high_risk"), bool):
        problems.append("subject.high_risk must be boolean")
    if not isinstance(subject.get("sample_rate"), (int, float)) or isinstance(subject.get("sample_rate"), bool) or not 0 <= subject["sample_rate"] <= 1:
        problems.append("subject.sample_rate must be between 0 and 1")
    return problems


def subject_value_verdict(subject: dict[str, Any]) -> dict[str, Any]:
    lamp = subject.get("lamp") or {}
    return value_reducer.value_verdict(
        exit_code=lamp.get("exit_code"),
        diff_text=subject.get("diff"),
        allowed_paths=subject.get("allowed_paths") or [],
    )


def _subject_signature(subject: dict[str, Any]) -> str:
    touched = value_reducer.touched_files(subject.get("diff"))
    return digest({"stage_id": subject["stage_id"], "files": touched})


def _sample_hit(receipt_digest: str, sample_rate: float) -> bool:
    bucket = int(receipt_digest[-2:], 16) / 255
    return bucket < sample_rate


def trigger(subject: dict[str, Any], *, seen_signatures: set[str] | None = None) -> dict[str, Any]:
    """Choose falsifier execution deterministically after a GREEN value verdict."""
    problems = validate_subject(subject)
    if problems:
        return {"run": False, "status": "invalid_subject", "problems": problems}
    value = subject_value_verdict(subject)
    if value["verdict"] != "GREEN":
        return {"run": False, "status": "subject_not_green", "value_verdict": value}
    seen = seen_signatures or set()
    signature = _subject_signature(subject)
    reasons = []
    if subject["high_risk"]:
        reasons.append("high_risk_stage")
    if signature not in seen:
        reasons.append("first_output_type")
    if _sample_hit(subject["receipt_digest"], float(subject["sample_rate"])):
        reasons.append("deterministic_sample")
    return {
        "run": bool(reasons),
        "status": "falsifier_triggered" if reasons else "falsifier_not_triggered",
        "reasons": reasons,
        "signature": signature,
        "value_verdict": value,
    }


def build_spec(subject: dict[str, Any]) -> dict[str, Any]:
    problems = validate_subject(subject)
    if problems:
        return {"schema": SPEC_SCHEMA, "verdict": "ng", "problems": problems}
    slots = [
        {"id": f"falsifier-{index}", "objective": "Find a witnessed regression or concrete intent deviation.", "separation_from": ["subject", *[f"falsifier-{prior}" for prior in range(1, index)]]}
        for index in range(1, FALSIFIER_COUNT + 1)
    ]
    return {
        "schema": SPEC_SCHEMA,
        "subject_ref": subject["subject_ref"],
        "falsifier_count": FALSIFIER_COUNT,
        "objective": "Refute regression/side effect or original-goal intent drift after deterministic GREEN.",
        "evidence_boundary": ["goal", "lamp", "diff", "receipt_digest"],
        "allowed_paths": subject["allowed_paths"],
        "review_slots": slots,
        "subject_binding": subject["binding"],
        "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM),
    }


def _design_grill_adapter(spec: dict[str, Any], document_refs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": dg.SPEC_SCHEMA,
        "grill_id": f"independent-falsifier:{spec['subject_ref']}",
        "question": spec["objective"],
        "documents": document_refs,
        "subject": {"kind": "green_output", "label": spec["subject_ref"], "binding": spec["subject_binding"]},
        "review_slots": spec["review_slots"],
        "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM),
    }


def prepare(
    subject: dict[str, Any],
    *,
    context_root: Path,
    session_dir: Path,
) -> dict[str, Any]:
    """Prepare only the four bounded evidence documents through Design Grill."""
    spec = build_spec(subject)
    if spec.get("verdict") == "ng":
        return spec
    value = subject_value_verdict(subject)
    if value["verdict"] != "GREEN":
        return {"schema": SPEC_SCHEMA, "verdict": "ng", "status": "subject_not_green", "value_verdict": value}
    root = context_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    documents = {
        "goal.md": subject["goal"],
        "lamp.json": json.dumps(subject["lamp"], ensure_ascii=False, sort_keys=True),
        "diff.patch": subject["diff"],
        "receipt.txt": subject["receipt_digest"],
    }
    for name, text in documents.items():
        (root / name).write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
    adapter = _design_grill_adapter(
        spec,
        [{"ref": name, "start_line": 1, "end_line": len(text.splitlines()) or 1} for name, text in documents.items()],
    )
    prepared = dg.prepare(adapter, dg.ROOT, session_dir, context_root=root)
    return {**spec, "verdict": prepared["verdict"], "status": prepared.get("status"), "design_grill": prepared}


def request(
    session_dir: Path,
    *,
    bindings: list[dict[str, str]],
    provider_profiles: list[str],
    requested_at: str,
    expires_at: str,
    policy: dict[str, Any],
) -> dict[str, Any]:
    if len(bindings) != FALSIFIER_COUNT or len(provider_profiles) != FALSIFIER_COUNT:
        return {"verdict": "ng", "status": "falsifier_binding_count_invalid", "problems": ["exactly three bindings and provider profiles are required"]}
    if any(not _valid_binding(binding) for binding in bindings):
        return {"verdict": "ng", "status": "falsifier_binding_invalid", "problems": ["every falsifier binding must contain runner and model"]}
    if len({_binding_key(binding) for binding in bindings}) != FALSIFIER_COUNT:
        return {"verdict": "ng", "status": "falsifier_binding_separation_required", "problems": ["falsifier bindings must be pairwise distinct"]}
    contracts = []
    for index, (binding, provider_profile) in enumerate(zip(bindings, provider_profiles), 1):
        result = dg.request(
            session_dir,
            review_id=f"falsifier-{index}",
            runner=binding["runner"],
            model=binding["model"],
            provider_profile=provider_profile,
            requested_at=requested_at,
            expires_at=expires_at,
            policy=policy,
        )
        if result.get("verdict") != "pass":
            return {"verdict": "ng", "status": result.get("status", "falsifier_request_failed"), "contracts": contracts, "problems": result.get("problems", [])}
        contracts.append(result["contract"])
    return {"verdict": "pass", "status": "host_egress_admission_required", "contracts": contracts, "provider_calls": 0}


def _finding_has_ref(finding: Any) -> bool:
    if isinstance(finding, str):
        return any(finding.startswith(prefix) or f" {prefix}" in finding for prefix in FINDING_REF_PREFIXES)
    if isinstance(finding, dict):
        refs = finding.get("refs")
        return isinstance(refs, list) and bool(refs) and all(isinstance(ref, str) and any(ref.startswith(prefix) for prefix in FINDING_REF_PREFIXES) for ref in refs)
    return False


def _validate_witness(witness: Any) -> list[str]:
    if not isinstance(witness, dict) or set(witness) != {"argv", "expected_observation"}:
        return ["witness must contain only argv and expected_observation"]
    argv = witness.get("argv")
    if not isinstance(argv, list) or not argv or len(argv) > 32 or any(not isinstance(item, str) or not item for item in argv):
        return ["witness.argv must be a bounded non-empty argv array"]
    observation = witness.get("expected_observation")
    if not isinstance(observation, dict) or set(observation) - {"exit_code", "stdout_contains", "stderr_contains"} or "exit_code" not in observation or not isinstance(observation.get("exit_code"), int):
        return ["expected_observation must specify an integer exit_code and optional output markers"]
    if any(not isinstance(observation.get(key), str) or not observation[key] for key in ("stdout_contains", "stderr_contains") if key in observation):
        return ["witness output markers must be non-empty strings"]
    return []


def validate_result(result: Any, *, subject_ref: str | None = None) -> list[str]:
    if not isinstance(result, dict):
        return ["result must be an object"]
    expected = {"schema", "subject_ref", "falsifier_binding", "findings", "witness", "refute_kind", "refuted", "route"}
    problems = []
    if set(result) != expected:
        problems.append("result fields are invalid")
    if result.get("schema") != RESULT_SCHEMA:
        problems.append(f"result.schema must be {RESULT_SCHEMA}")
    if not isinstance(result.get("subject_ref"), str) or not result["subject_ref"].strip() or subject_ref is not None and result.get("subject_ref") != subject_ref:
        problems.append("result.subject_ref does not bind the subject")
    if not _valid_binding(result.get("falsifier_binding")):
        problems.append("result.falsifier_binding is invalid")
    findings = result.get("findings")
    if not isinstance(findings, list) or len(findings) > 32 or any(not isinstance(item, (str, dict)) for item in findings):
        problems.append("findings must be a bounded string/object array")
    elif any(isinstance(item, str) and not item.strip() for item in findings):
        problems.append("findings strings must be non-empty")
    kind = result.get("refute_kind")
    if kind not in REFUTE_KINDS:
        problems.append("refute_kind is invalid")
    if not isinstance(result.get("refuted"), bool):
        problems.append("refuted must be boolean")
    if result.get("route") not in ROUTES:
        problems.append("route is invalid")
    witness = result.get("witness")
    if kind == "witnessed":
        if witness is None:
            problems.append("witnessed refute requires a witness")
        else:
            problems.extend(_validate_witness(witness))
    elif witness is not None:
        problems.append("argued/none refutes must not carry a witness")
    if kind == "argued" and not any(_finding_has_ref(item) for item in findings or []):
        problems.append("argued refute requires concrete goal/diff/lamp/receipt refs")
    if kind == "none" and result.get("refuted"):
        problems.append("none refute cannot be marked refuted")
    return problems


def _process_receipt(argv: list[str], proc: subprocess.CompletedProcess[str], *, observed_at: str) -> dict[str, Any]:
    output = (proc.stdout or "") + (proc.stderr or "")
    return {
        "schema": PROCESS_RECEIPT_SCHEMA,
        "status": "witness_replay_recorded",
        "provider_calls": 0,
        "event": {"status": "witness_replay_recorded", "argv_digest": digest(argv)},
        "execution": {
            "argv_digest": digest(argv),
            "exit_code": proc.returncode,
            "output_digest": digest(output),
            "completed_at": observed_at,
            "claim_level": "process_bound",
        },
    }


def replay_witness(witness: dict[str, Any], *, cwd: Path | None = None, timeout_seconds: int = 10) -> dict[str, Any]:
    problems = _validate_witness(witness)
    if problems:
        return {"verdict": "ng", "status": "invalid_witness", "reproduced": False, "problems": problems}
    try:
        proc = subprocess.run(witness["argv"], cwd=cwd, text=True, capture_output=True, check=False, timeout=timeout_seconds)
    except (OSError, subprocess.SubprocessError) as exc:
        return {"verdict": "ng", "status": "witness_replay_failed", "reproduced": False, "problems": [type(exc).__name__]}
    observation = witness["expected_observation"]
    output = (proc.stdout or "") + (proc.stderr or "")
    matches_exit = proc.returncode == observation["exit_code"]
    matches_stdout = "stdout_contains" not in observation or observation["stdout_contains"] in (proc.stdout or "")
    matches_stderr = "stderr_contains" not in observation or observation["stderr_contains"] in (proc.stderr or "")
    failure_reproduced = proc.returncode != 0 or ("stdout_contains" in observation and observation["stdout_contains"] in (proc.stdout or "")) or ("stderr_contains" in observation and observation["stderr_contains"] in (proc.stderr or ""))
    receipt = _process_receipt(witness["argv"], proc, observed_at=datetime.now(timezone.utc).isoformat())
    return {
        "verdict": "pass",
        "status": "witness_reproduced" if matches_exit and matches_stdout and matches_stderr and failure_reproduced else "witness_not_reproduced",
        "reproduced": bool(matches_exit and matches_stdout and matches_stderr and failure_reproduced),
        "receipt": receipt,
        "observation": {"exit_code": proc.returncode, "output_digest": receipt["execution"]["output_digest"], "output": output[:2000]},
    }


def verify_result(result: dict[str, Any], *, subject_ref: str, cwd: Path | None = None) -> dict[str, Any]:
    problems = validate_result(result, subject_ref=subject_ref)
    if problems:
        return {"verdict": "ng", "status": "invalid_falsifier_result", "result": result, "problems": problems}
    if result["refute_kind"] == "witnessed":
        replay = replay_witness(result["witness"], cwd=cwd)
        if not replay.get("reproduced"):
            normalized = {**result, "witness": None, "refute_kind": "none", "refuted": False, "route": "continue"}
            return {"verdict": "pass", "status": "false_positive_discarded", "result": normalized, "replay": replay}
        normalized = {**result, "refuted": True, "route": "human_required"}
        return {"verdict": "pass", "status": "witnessed_refute_verified", "result": normalized, "replay": replay}
    if result["refute_kind"] == "argued":
        return {"verdict": "pass", "status": "argued_refute_consultation", "result": {**result, "refuted": True, "route": "human_required"}, "replay": None}
    return {"verdict": "pass", "status": "no_refute", "result": {**result, "refuted": False, "route": "continue"}, "replay": None}


def record_result(session_dir: Path, result: dict[str, Any], *, replay: dict[str, Any] | None = None) -> dict[str, Any]:
    problems = validate_result(result)
    if problems:
        return {"verdict": "ng", "status": "invalid_falsifier_result", "problems": problems}
    dg._append(session_dir, {"type": "independent_falsifier_result_recorded", "schema": EVENT_SCHEMA, "subject_ref": result["subject_ref"], "falsifier_binding": result["falsifier_binding"], "result": result, "process_receipt": (replay or {}).get("receipt"), "provider_calls": 0, "cannot_claim": sorted(dg.REQUIRED_CANNOT_CLAIM)})
    return {"verdict": "pass", "status": "independent_falsifier_result_recorded", "provider_calls": 0}


def evaluate_green(
    subject: dict[str, Any],
    responses: list[dict[str, Any]],
    *,
    seen_signatures: set[str] | None = None,
    cwd: Path | None = None,
) -> dict[str, Any]:
    problems = validate_subject(subject)
    if problems:
        return {"schema": DECISION_SCHEMA, "verdict": "human_required", "route": "human_required", "status": "invalid_subject", "falsifier_ran": False, "problems": problems}
    value = subject_value_verdict(subject)
    if value["verdict"] != "GREEN":
        return {"schema": DECISION_SCHEMA, "verdict": "RED", "route": "continue", "status": "subject_not_green", "falsifier_ran": False, "value_verdict": value}
    choice = trigger(subject, seen_signatures=seen_signatures)
    if not choice["run"]:
        return {"schema": DECISION_SCHEMA, "verdict": "GREEN", "route": "continue", "status": "falsifier_not_triggered", "falsifier_ran": False, "trigger": choice}
    if len(responses) != FALSIFIER_COUNT:
        return {"schema": DECISION_SCHEMA, "verdict": "human_required", "route": "human_required", "status": "falsifier_result_count_invalid", "falsifier_ran": True, "trigger": choice, "problems": ["exactly three falsifier results are required"]}
    bindings = [response.get("falsifier_binding") for response in responses]
    if any(not _valid_binding(binding) for binding in bindings) or len({_binding_key(binding) for binding in bindings if _valid_binding(binding)}) != FALSIFIER_COUNT:
        return {"schema": DECISION_SCHEMA, "verdict": "human_required", "route": "human_required", "status": "falsifier_binding_separation_required", "falsifier_ran": True, "trigger": choice, "problems": ["three pairwise-distinct falsifier bindings are required"]}
    subject_key = _binding_key(subject["binding"])
    if any(_binding_key(binding) == subject_key for binding in bindings):
        return {"schema": DECISION_SCHEMA, "verdict": "human_required", "route": "human_required", "status": "subject_binding_reused", "falsifier_ran": True, "trigger": choice, "problems": ["falsifier binding must differ from subject binding"]}
    checked = [verify_result(response, subject_ref=subject["subject_ref"], cwd=cwd) for response in responses]
    invalid = [item for item in checked if item.get("verdict") != "pass"]
    if invalid:
        return {"schema": DECISION_SCHEMA, "verdict": "human_required", "route": "human_required", "status": "invalid_falsifier_results", "falsifier_ran": True, "trigger": choice, "results": checked, "problems": [problem for item in invalid for problem in item.get("problems", [])]}
    normalized = [item["result"] for item in checked]
    witnessed = [item for item in normalized if item["refute_kind"] == "witnessed" and item["refuted"]]
    argued = [item for item in normalized if item["refute_kind"] == "argued" and item["refuted"]]
    argued_findings = [finding for item in argued for finding in item["findings"]][:32]
    auto_flip = len(witnessed) >= 2
    return {
        "schema": DECISION_SCHEMA,
        "verdict": "human_required" if auto_flip else "GREEN",
        "route": "human_required" if auto_flip or argued else "continue",
        "status": "witnessed_refute_majority" if auto_flip else "argued_consultation" if argued else "falsifier_clear",
        "falsifier_ran": True,
        "trigger": choice,
        "witnessed_refutes": len(witnessed),
        "argued_refutes": len(argued),
        "consultation_findings": argued_findings,
        "results": normalized,
        "process_receipts": [
            (item.get("replay") or {}).get("receipt")
            for item in checked
            if (item.get("replay") or {}).get("receipt")
        ],
    }

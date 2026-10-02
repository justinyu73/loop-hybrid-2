"""Provider-free canary for the external host packet to LH assignment binding."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

import assignment_packet
import goal_assignment
import project_binding


def _digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _write_json(path: Path, value: Any) -> bytes:
    raw = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def _fixture(root: Path) -> tuple[Path, Path, str, dict[str, Any]]:
    target = root / "target"
    host = root / "host"
    subprocess.run(["git", "init", "-q", str(target)], check=True, capture_output=True)
    _git(target, "config", "user.email", "canary@example.invalid")
    _git(target, "config", "user.name", "assignment canary")
    (target / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    _git(target, "add", "baseline.txt")
    _git(target, "commit", "-qm", "baseline")
    base_revision = subprocess.run(
        ["git", "-C", str(target), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    authority_raw = b"fixture target authority\n"
    (target / "AGENTS.md").write_bytes(authority_raw)
    stage = {
        "stage_id": "stage-1",
        "goal": {"must_have": ["bounded target edit"]},
        "allowed_paths": [".lh-pilot/"],
        "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {
            "id": "check-one",
            "smoke": "fixture",
            "verification_argv": ["printf", "ok"],
        },
        "max_attempts": 1,
        "next_stage_id": None,
    }
    campaign = {
        "schema": "lh-campaign/v1",
        "campaign_id": "fixture-campaign",
        "standing_intents": [],
        "stages": [stage],
    }
    contract = {
        "schema": "lh-project-runtime-contract/v1",
        "project_id": "fixture-project",
        "authority": {"ref": "AGENTS.md", "digest": _sha256(authority_raw)},
        "campaign": campaign,
        "source_repo": ".",
        "base_revision": base_revision,
        "runtime": {
            "goal_store": "runtime/goals",
            "run_store": "runtime/runs",
            "workspace_root": "runtime/workspaces",
        },
        "onboarding": {
            "schema": "host-project-onboarding/v1",
            "check_registry": "governance/checks-registry.json",
            "allowed_paths": [".lh-pilot/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "pilot_profile": "pilot-free",
        },
    }
    check = {
        "command": ["printf", "ok"],
        "expect_exit_code": 0,
        "side_effects": ["workspace"],
        "description": "fixture check",
    }
    registry = {
        "schema": "host-check-registry/v1",
        "target_project": "fixture-project",
        "checks": {"check-one": check},
        "profiles": {"pilot-free": ["check-one"]},
    }
    _write_json(target / "project_runtime_contract.json", contract)
    _write_json(target / "governance" / "checks-registry.json", registry)
    _git(target, "add", "AGENTS.md", "project_runtime_contract.json", "governance/checks-registry.json")
    _git(target, "commit", "-qm", "contract")
    target_base_sha = subprocess.run(
        ["git", "-C", str(target), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    _git(target, "update-ref", "refs/remotes/origin/main", target_base_sha)

    assignment = {
        "schema": goal_assignment.ASSIGNMENT_SCHEMA,
        "project_id": "fixture-project",
        "assigner_ref": "host-packet:packet-fixture-1",
        "base_revision": base_revision,
        "verification_budget": {
            "schema": goal_assignment.VERIFICATION_BUDGET_SCHEMA,
            "max_seconds": 30.0,
        },
        "criteria": [{
            "criterion_id": "AC-1",
            "criterion_authority_ref": (
                "project_runtime_contract.json#campaign.stages[stage-1].goal.must_have"
            ),
            "criterion_authority_digest": _digest(stage["goal"]),
            "check_id": "check-one",
            "check_definition_digest": _digest(check),
        }],
    }
    packet = {
        "schema": assignment_packet.PACKET_SCHEMA,
        "packet_id": "packet-fixture-1",
        "packet_state": "paused_registration",
        "execution_status": "not_started",
        "assignment": {
            "assignment_id": "assignment-fixture-1",
            "task_id": "task-fixture-1",
            "project_id": "fixture-project",
            "dispatch": False,
            "self_accept": False,
        },
        "goal_assignment": assignment,
        "assignment_digest": goal_assignment._digest(assignment),
        "source_binding": {
            "target_remote": "origin/main",
            "target_base_sha": target_base_sha,
            "target_contract": {
                "ref": "project_runtime_contract.json",
                "digest": _digest(contract),
                "raw_sha256": _sha256(
                    (target / "project_runtime_contract.json").read_bytes()
                ),
            },
            "target_authority": {
                "ref": "AGENTS.md",
                "digest": _sha256(authority_raw),
            },
            "target_check_registry": {
                "ref": "governance/checks-registry.json",
                "digest": _digest(registry),
                "raw_sha256": _sha256(
                    (target / "governance" / "checks-registry.json").read_bytes()
                ),
            },
        },
        "allowed_paths": {"target": [".lh-pilot/"]},
        "allowed_side_effects": ["workspace", "artifact"],
        "target_checks": [{
            "id": "check-one",
            "argv": ["printf", "ok"],
            "expect_exit_code": 0,
            "registry": "governance/checks-registry.json",
            "definition_digest": _digest(check),
        }],
    }
    packet_path = host / "docs" / "codex-handoff" / "packet.json"
    _write_json(packet_path, packet)
    return target, packet_path, base_revision, assignment


def _case(case_id: str, body: Callable[[], tuple[bool, Any]]) -> dict[str, Any]:
    try:
        ok, detail = body()
    except Exception as exc:  # a canary reports a finding instead of raising
        ok, detail = False, f"{type(exc).__name__}: {exc}"
    return {"id": case_id, "ok": bool(ok), "detail": str(detail)}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-assignment-packet-") as raw:
        root = Path(raw)
        target, packet_path, base_revision, assignment = _fixture(root)
        contract_path = target / "project_runtime_contract.json"
        prior_root = os.environ.get("LH_TRUSTED_BOOTSTRAP_ROOT")
        os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = str(root / "host")
        try:
            loaded = assignment_packet.load_assignment_packet(
                packet_path,
                project_id="fixture-project",
                campaign_id="fixture-campaign",
                source_repo=target,
                expected_correlation_id="packet-fixture-1",
            )
            resolved = project_binding.resolve_project(
                contract_path,
                assignment_packet_path=packet_path,
                assignment_correlation_id="packet-fixture-1",
            )

            tampered = json.loads(packet_path.read_text(encoding="utf-8"))
            tampered["assignment_digest"] = "sha256:" + "0" * 64
            tampered_path = root / "host" / "docs" / "codex-handoff" / "tampered.json"
            _write_json(tampered_path, tampered)
            try:
                assignment_packet.load_assignment_packet(
                    tampered_path,
                    project_id="fixture-project",
                    campaign_id="fixture-campaign",
                    source_repo=target,
                    expected_correlation_id="packet-fixture-1",
                )
            except ValueError as exc:
                tamper_rejected = "digest" in str(exc)
            else:
                tamper_rejected = False
        finally:
            if prior_root is None:
                os.environ.pop("LH_TRUSTED_BOOTSTRAP_ROOT", None)
            else:
                os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = prior_root

    campaign_stage = resolved["run_kwargs"]["campaign"]["stages"][0]
    cases = [
        _case(
            "packet-binds-recorded-target-ref-and-digests",
            lambda: (
                loaded["binding"]["target_base_sha"]
                != loaded["binding"]["base_revision"]
                and loaded["binding"]["assignment_digest"] == goal_assignment._digest(assignment),
                loaded["binding"],
            ),
        ),
        _case(
            "resolver-injects-assignment-before-campaign-compilation",
            lambda: (
                campaign_stage["goal_assignment"] == assignment
                and resolved["run_kwargs"]["assignment_binding"]["stage_id"] == "stage-1"
                and resolved["run_kwargs"]["base_revision"] == base_revision,
                resolved["run_kwargs"]["assignment_binding"],
            ),
        ),
        _case("tampered-assignment-digest-is-rejected", lambda: (tamper_rejected, "digest")),
    ]
    failures = [item for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": "lh-assignment-packet-binding",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

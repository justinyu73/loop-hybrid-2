#!/usr/bin/env python3
"""Native recovery binding: the whole sealed chain resolves, and any single change is refused.

``resolve_native_run_execution_binding`` is the only way planner recovery is
switched on for a native run.  It reads a project contract sealed into a
scheduler dispatch, the contract's ``planner_recovery`` block, and its
``execution_binding``: a capability contract, a provider registry and a host
contract (each pinned by digest), the bootstrap authority, and the fence.  This
exam builds that chain from real files, shows that it resolves, and changes one
thing at a time to show each change is refused before anything runs.
"""
from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterator

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))  # runner_adapter resolves its collaborators as package modules

from lh_runtime import dispatch_envelope as dispatches  # noqa: E402
from lh_runtime.host_ports import headless_contract  # noqa: E402
from lh_runtime.runner_adapter import CapabilityError, resolve_native_run_execution_binding  # noqa: E402

CHECK_ID = "lh-native-recovery-binding"
OWNER = "native-recovery-owner"
BUDGET = {"planner_calls": 1, "plan_verifier_calls": 1, "planner_timeout_seconds": 60.0,
          "plan_verifier_timeout_seconds": 60.0, "incident_timeout_seconds": 600.0}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    return {"path": str(path.resolve()), "digest": sha(path)}


@contextlib.contextmanager
def environment(**values: str) -> Iterator[None]:
    saved = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class Chain:
    """Every file of one sealed native-recovery chain; ``edit`` hooks change exactly one thing."""

    def __init__(self, root: Path, *, contract_edit: Callable[[dict[str, Any]], None] | None = None,
                 registry_edit: Callable[[dict[str, Any]], None] | None = None,
                 capability_edit: Callable[[dict[str, Any]], None] | None = None):
        root.mkdir(parents=True)
        self.root = root
        self.source = root / "source"
        self.source.mkdir()
        for args in (("init", "-q"), ("config", "user.email", "native@example.invalid"),
                     ("config", "user.name", "Native Fixture")):
            subprocess.run(["git", *args], cwd=self.source, check=True, capture_output=True)
        (self.source / "README.md").write_text("native recovery fixture\n", encoding="utf-8")
        subprocess.run(["git", "add", "-A"], cwd=self.source, check=True, capture_output=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=self.source, check=True, capture_output=True)
        self.base = subprocess.run(["git", "rev-parse", "HEAD"], cwd=self.source, check=True, capture_output=True,
                                   text=True).stdout.strip()
        anchor = root / "trust" / "docs" / "bootstrap-authority.md"
        anchor.parent.mkdir(parents=True)
        anchor.write_text('<a id="lh-external-bootstrap-001"></a>\n### Bootstrap authority fixture\n', encoding="utf-8")
        self.trust_root = str((root / "trust").resolve())
        planner_argv = [sys.executable, "-B", str(root / "bin" / "planner.py")]
        verifier_argv = [sys.executable, "-B", str(root / "bin" / "reviewer.py")]
        capabilities = {
            "schema": "host-provider-neutral-capability-contract/v1", "revision": "1", "fallback": "none",
            "capabilities": {
                "coding": {"adapter_id": "bounded-command-v1", "permissions": "workspace_write",
                           "identity": {"principal": "coder", "provider": "fixture-coder"}},
                "verifier": {"adapter_id": "bounded-command-v1", "permissions": "read_only",
                             "identity": {"principal": "reviewer", "provider": "fixture-reviewer"}},
                "checks": {"adapter_id": "bounded-command-v1", "permissions": "read_only",
                           "identity": {"principal": "checker", "provider": "fixture-checker"}},
                "planning": {"adapter_id": "bounded-command-v1", "permissions": "read_only",
                             "identity": {"principal": "planner", "provider": "fixture-planner"}},
            },
            "roles": {"coding": "coding", "integration": "coding", "verifier": "verifier", "checks": "checks",
                      "planner": "planning"},
        }
        if capability_edit is not None:
            capability_edit(capabilities)
        registry = {
            "schema": "host-provider-registry/v1", "revision": "1", "fallback": "none",
            "providers": {
                "coder": {"adapter_id": "bounded-command-v1", "identity": capabilities["capabilities"]["coding"]["identity"],
                          "command": [sys.executable, "-B", str(root / "bin" / "coder.py")]},
                "reviewer": {"adapter_id": "bounded-command-v1",
                             "identity": capabilities["capabilities"]["verifier"]["identity"], "command": verifier_argv},
                "planner": {"adapter_id": "bounded-command-v1",
                            "identity": capabilities["capabilities"]["planning"]["identity"], "command": planner_argv},
            },
        }
        if registry_edit is not None:
            registry_edit(registry)
        self.registry_path = root / "refs" / "registry.json"
        refs = {"capability": write_json(root / "refs" / "capabilities.json", capabilities),
                "registry": write_json(self.registry_path, registry),
                "host": write_json(root / "refs" / "host.json", headless_contract())}
        self.campaign = {"schema": "lh-campaign/v1", "campaign_id": "native-campaign", "stages": [{
            "stage_id": "feature", "goal": {"must_have": ["the change"], "must_not": ["push"]},
            "allowed_paths": ["src/"], "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {"id": "lamp", "smoke": "check", "verification_argv": [sys.executable, "-c", "pass"]},
            "max_attempts": 2, "next_stage_id": None}]}
        contract = {
            "schema": "lh-project-runtime-contract/v1", "project_id": "native-project", "campaign": self.campaign,
            "source_repo": "source", "base_revision": self.base,
            "runtime": {"goal_store": "rt/goals", "run_store": "rt/runs", "workspace_root": "rt/ws"},
            "execution_binding": {
                "schema": "host-task-area-execution-binding/v1", "capability_contract_ref": refs["capability"],
                "provider_registry_ref": refs["registry"], "provider_selection": {"coding": "coder", "verifier": "reviewer"},
                "host_contract_ref": refs["host"], "fence": {"backend_id": "local-process", "egress_policy_ref": None},
                "bootstrap_authority": {"decision_id": "LH-EXTERNAL-BOOTSTRAP-001",
                                        "authority_ref": "docs/bootstrap-authority.md#lh-external-bootstrap-001",
                                        "authority_digest": sha(anchor), "root": self.trust_root}},
            "planner_recovery": {"schema": "lh-planner-recovery-binding/v1", "identity_profile": "native-run-v1",
                                 "planner_argv": planner_argv, "plan_verifier_argv": verifier_argv,
                                 "planner_provider": "planner", "budget": dict(BUDGET)},
        }
        if contract_edit is not None:
            contract_edit(contract)
        self.contract_path = root / "contract.json"
        self.contract_path.write_text(json.dumps(contract, indent=2, sort_keys=True), encoding="utf-8")
        self.contract = contract
        self.seal = {"contract_ref": str(self.contract_path.resolve()), "contract_digest": sha(self.contract_path),
                     "binding": copy.deepcopy(contract["planner_recovery"])}
        self.dispatch = self.envelope()

    def envelope(self, **changes: Any) -> dict[str, Any]:
        body = {"schema": dispatches.SCHEMA, "project_id": "native-project", "owner_id": OWNER,
                "contract_ref": str(self.contract_path.resolve()), "contract_digest": self.seal["contract_digest"],
                "desired_state": "enabled", "desired_state_event_id": "fixture:enabled",
                "desired_state_digest": "sha256:" + "d" * 64, "campaign_id": "native-campaign",
                "base_revision": self.base, "issued_at": "2026-10-07T00:00:00+00:00",
                "source_invocation_id": "fixture-invocation", **changes}
        envelope = {**body, "dispatch_id": "dispatch-" + dispatches.digest_json(body).removeprefix("sha256:")[:32]}
        envelope["envelope_digest"] = dispatches.digest_json(envelope)
        return envelope

    def resolve(self, *, seal: dict[str, Any] | None = None, dispatch: dict[str, Any] | None = None,
                campaign: dict[str, Any] | None = None, base: str | None = None, owner: str = OWNER) -> Any:
        with environment(LH_SCHEDULER_OWNER_ID=owner, LH_TRUSTED_BOOTSTRAP_ROOT=self.trust_root):
            return resolve_native_run_execution_binding(
                seal or self.seal, dispatch or self.dispatch, campaign=campaign or self.campaign,
                source_repo=str(self.source), base_revision=base or self.base)


def refused(action: Callable[[], Any]) -> str:
    try:
        action()
        return "accepted"
    except (CapabilityError, ValueError, OSError) as exc:
        return f"{type(exc).__name__}: {exc}"


def c1_resolves(root: Path) -> dict[str, Any]:
    chain = Chain(root)
    binding = chain.resolve()
    runtime = binding.native_runtime
    ok = (runtime["identity_profile"] == "native-run-v1" and runtime["contract_digest"] == chain.seal["contract_digest"]
          and runtime["planner_recovery"]["budget"]["planner_calls"] == 1
          and runtime["dispatch"]["dispatch_id"] == chain.dispatch["dispatch_id"]
          and binding.providers["verifier"]["command"] == chain.contract["planner_recovery"]["plan_verifier_argv"])
    return case("a-sealed-chain-resolves", ok, {"project": runtime.get("project_id"), "campaign": runtime.get("campaign_id")})


def c2_refusals(root: Path) -> dict[str, Any]:
    results: dict[str, str] = {}
    base = Chain(root / "base")
    # The contract file changes after it was sealed into the dispatch.
    tampered = Chain(root / "contract-changed")
    tampered.contract_path.write_text(tampered.contract_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    results["contract_changed_after_seal"] = refused(tampered.resolve)
    # The pinned provider registry changes after the contract pinned it.
    drift = Chain(root / "registry-changed")
    drift.registry_path.write_text(drift.registry_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    results["registry_changed_after_pin"] = refused(drift.resolve)
    results["planner_command_differs_from_registry"] = refused(Chain(
        root / "planner-argv", contract_edit=lambda c: c["planner_recovery"].update(planner_argv=["/bin/other"])).resolve)
    results["role_budget_is_not_one"] = refused(Chain(
        root / "budget", contract_edit=lambda c: c["planner_recovery"]["budget"].update(planner_calls=2)).resolve)
    results["identity_profile_is_not_native"] = refused(Chain(
        root / "profile", contract_edit=lambda c: c["planner_recovery"].update(identity_profile="work-unit-v1")).resolve)
    results["verifier_is_not_independent"] = refused(Chain(
        root / "same-principal", capability_edit=lambda c: c["capabilities"]["verifier"].update(
            identity=dict(c["capabilities"]["coding"]["identity"]))).resolve)
    results["dispatch_owner_differs"] = refused(lambda: base.resolve(owner="someone-else"))
    results["desired_state_is_paused"] = refused(lambda: base.resolve(dispatch=base.envelope(desired_state="paused")))
    other_campaign = copy.deepcopy(base.campaign)
    other_campaign["stages"][0]["max_attempts"] = 3
    results["campaign_differs"] = refused(lambda: base.resolve(campaign=other_campaign))
    results["base_revision_differs"] = refused(lambda: base.resolve(base="0" * 40))
    expected = {
        "contract_changed_after_seal": "native_runtime_dispatch_mismatch",
        "registry_changed_after_pin": "execution_binding_reference_digest_mismatch",
        "planner_command_differs_from_registry": "native_recovery_provider_binding_mismatch",
        "role_budget_is_not_one": "native_recovery_role_limit_invalid",
        "identity_profile_is_not_native": "native_recovery_binding_invalid",
        "verifier_is_not_independent": "execution_binding_verifier_independence_required",
        "dispatch_owner_differs": "native_runtime_dispatch_mismatch",
        "desired_state_is_paused": "native_runtime_dispatch_mismatch",
        "campaign_differs": "native_runtime_dispatch_mismatch",
        "base_revision_differs": "native_runtime_dispatch_mismatch",
    }
    wrong = {name: results.get(name) for name, reason in expected.items()
             if results.get(name) != f"CapabilityError: {reason}"}
    return case("each-single-change-is-refused-for-its-reason", not wrong, {"wrong": wrong, "outcomes": results})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-native-recovery-binding-") as raw:
        root = Path(raw).resolve()
        for name, build in (("a-sealed-chain-resolves", lambda: c1_resolves(root / "c1")),
                            ("each-single-change-is-refused-for-its-reason", lambda: c2_refusals(root / "c2"))):
            try:
                results.append(build())
            except Exception as exc:  # a crash is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:400]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

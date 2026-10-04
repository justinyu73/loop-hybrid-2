#!/usr/bin/env python3
"""The engine is a pure goal loop: no host product, platform shim, or vendor.

Goal, Run, Attempt, the disposable clone, the executor, the verifier, and the
verdict are the whole architecture.  Nothing in this repository may name a host
product, a platform bridge, or a retired vendor, and no code path may exist
that only such a product could exercise:

- the executor registry holds only the headless executors;
- capability routing actuates its production model headlessly;
- command ingress carries no agent-session rollover protocol;
- the execution fence has no out-of-sandbox control plane;
- instance init writes no host-product entry into config or egress policy;
- the host contract is the headless baseline with no adapter catalogue.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

CHECK_ID = "lh-pure-loop-boundary"
SELF = "lh_runtime/pure_loop_boundary_canary.py"
VOCABULARY = re.compile("|".join(("orca", "wsl", "kimi", "vscode", "openclaw")), re.IGNORECASE)
# Portability guards that forbid a platform token must name it.  Only these exact
# lines may carry it; any other line in the same file still fails.
GUARD_LINES = {
    "tools/portable_runtime_contract.py": ('re.compile(r"(?<![a-z])wsl(?![a-z])", re.IGNORECASE)',),
    "lh_runtime/platform_ports_canary.py": ('"wsl.localhost"',),
}
SESSION_ROLLOVER_EVENTS = ("context_pressure", "rollover_requested", "successor_heartbeat", "rollover_finalized")


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _tracked_text() -> dict[str, str]:
    listed = subprocess.run(["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True)
    files: dict[str, str] = {}
    for raw in listed.stdout.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8")
        path = REPO / rel
        if not path.is_file():
            continue
        data = path.read_bytes()
        if b"\0" in data:
            continue
        files[rel] = data.decode("utf-8", errors="replace")
    return files


def vocabulary_case(files: dict[str, str]) -> dict[str, Any]:
    hits: list[str] = []
    for rel, text in sorted(files.items()):
        if rel == SELF:
            continue
        allowed = GUARD_LINES.get(rel, ())
        for number, line in enumerate(text.splitlines(), 1):
            if VOCABULARY.search(line) and not any(marker in line for marker in allowed):
                hits.append(f"{rel}:{number}")
    return case("no-host-product-vocabulary", not hits, {"hits": len(hits), "first": hits[:25]})


def executor_registry_case() -> dict[str, Any]:
    import goal_loop_run as run

    names = sorted({*run.EXECUTORS, *run.HOST_EXECUTORS})
    hosts = sorted(run.EXECUTION_HOSTS)
    return case("executor-registry-is-headless", names == ["codex", "local"] and hosts == ["headless_cli"],
                {"executors": names, "execution_hosts": hosts})


def capability_headless_case() -> dict[str, Any]:
    import execution_fence as fences
    import goal_loop_run as run

    session = run.CapabilityRoutingSession.__new__(run.CapabilityRoutingSession)
    session.timeout_seconds = 30.0
    session.factories = {}
    session.execution_fence_port = fences.DisabledExecutionFencePort()
    resource = {"runner": "codex", "model": "fixture-model", "provider_binding": None}
    node = {"budget": {"max_wall_seconds": 10}}
    detail: dict[str, Any] = {}
    session.execution_host_binding = {"schema": run.EXECUTION_HOST_SCHEMA, "host_id": "headless_cli",
                                      "adapter": "headless_cli"}
    try:
        model = session._model(resource, node)
        detail["adapter"] = list(fences.model_fence_identity(model))
        detail["fenced"] = fences.model_requires_fence(model)
    except Exception as exc:  # The exam records the refusal instead of crashing.
        detail["error"] = f"{type(exc).__name__}: {exc}"
    session.execution_host_binding = None
    try:
        session._model(resource, node)
        detail["unbound"] = "accepted"
    except ValueError as exc:
        detail["unbound"] = f"refused: {exc}"
    ok = (detail.get("adapter", [None])[0] == "codex" and detail.get("fenced") is True
          and str(detail.get("unbound", "")).startswith("refused"))
    return case("capability-production-is-headless", ok, detail)


def rollover_case(files: dict[str, str]) -> dict[str, Any]:
    import command_ingress

    supported = sorted(set(SESSION_ROLLOVER_EVENTS) & set(command_ingress.SUPPORTED_EVENT_TYPES))
    mentions = sorted(rel for rel, text in files.items()
                      if rel != SELF and any(name in text for name in SESSION_ROLLOVER_EVENTS))
    return case("no-session-rollover-protocol", not supported and not mentions,
                {"supported": supported, "mentioned_in": mentions})


def fence_case() -> dict[str, Any]:
    import execution_fence as fences
    import execution_fence_linux as linux

    present = [name for name, found in (
        ("CONTROL_LAUNCH_BUDGET", hasattr(fences, "CONTROL_LAUNCH_BUDGET")),
        ("CONTROL_OPS", hasattr(fences, "CONTROL_OPS")),
        ("ExecutionFencePort.launch_control", hasattr(fences.ExecutionFencePort, "launch_control")),
        ("compose_control_argv", hasattr(fences, "compose_control_argv") or hasattr(linux, "compose_control_argv")),
        ("load_egress_policy", hasattr(fences, "load_egress_policy")),
    ) if found]
    return case("fence-has-no-control-plane", not present, {"present": present})


def instance_case() -> dict[str, Any]:
    from instance_config import initialize_instance

    with tempfile.TemporaryDirectory(prefix="lh-pure-loop-") as raw:
        root = Path(raw)
        home = root / "home"
        fake_bin = root / "bin"
        fake_bin.mkdir(parents=True)
        codex = fake_bin / "codex"
        codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        codex.chmod(0o755)
        env = {"HOME": str(home), "PATH": str(fake_bin)}
        paths = {key: str(root / key) for key in ("repo", "state", "workspace", "cache", "logs")}
        config = initialize_instance(
            root / "config" / "instance.json",
            system="Linux",
            environ=env,
            home=home,
            cwd=root,
            overrides={"paths": paths, "cli": {"providers": {"codex": str(codex)}}},
        )
        policy = json.loads(config.egress_policy_path.read_text(encoding="utf-8"))
        cli_keys = sorted(config.data["cli"])
        policy_keys = sorted(policy)
    # bubblewrap is the Linux fence's own sandbox binary, pinned like a provider.
    control = [key for key in policy_keys if key.startswith("control") or key.endswith("_cli")]
    ok = "providers" in cli_keys and set(cli_keys) <= {"providers", "bubblewrap"} and not control
    return case("instance-has-no-host-product-entry", ok,
                {"cli_keys": cli_keys, "policy_keys": policy_keys, "control_entries": control})


def host_contract_case() -> dict[str, Any]:
    from host_ports import HostPortError, headless_contract, resolve_host_ports

    contract = headless_contract()
    try:
        resolve_host_ports({**contract, "selected_adapter": "any-adapter"})
        selection = "accepted"
    except HostPortError as exc:
        selection = f"refused: {exc}"
    binding = resolve_host_ports(contract)
    ok = ("optional_adapters" not in contract and selection.startswith("refused")
          and binding["interface"] == "headless_cli")
    return case("host-contract-is-headless-only", ok,
                {"contract_keys": sorted(contract), "selection": selection, "interface": binding["interface"]})


def main() -> int:
    cases: list[dict[str, Any]] = []
    try:
        files = _tracked_text()
        cases.append(vocabulary_case(files))
        cases.append(rollover_case(files))
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        cases.append(case("tracked-scan-crashed", False, f"{type(exc).__name__}: {exc}"))
    for build in (executor_registry_case, capability_headless_case, fence_case, instance_case, host_contract_case):
        try:
            cases.append(build())
        except Exception as exc:
            cases.append(case(build.__name__.replace("_", "-"), False, f"{type(exc).__name__}: {exc}"))
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

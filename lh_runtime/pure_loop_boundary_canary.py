#!/usr/bin/env python3
"""The engine is a pure goal loop: no environment, tool, or vendor dependency.

Goal, Run, Attempt, the disposable git clone, the executor, the verifier, and
the verdict are the whole architecture.  git is the one premise.  Nothing in
this repository may name a host product, a platform bridge, a model vendor, a
hosted VCS service, an OS sandbox tool, or an OS service manager, and no code
path may exist that only such a tool could exercise:

- no executor, judge, or evaluator is built in; every one is declared, runs
  from an absolute path, and the core never searches PATH for a provider;
- capability routing actuates its production model through a declaration;
- command ingress carries no agent-session rollover protocol;
- the execution fence is a port: no kernel backend ships, and the explicit
  local-process backend runs a declared command and says it contains nothing;
- instance init writes no tool entry into config or egress policy;
- the host contract is the headless baseline with no adapter catalogue;
- the lifecycle is the foreground process; no OS service adapter is described;
- cost comes only from declared pricing.
"""

from __future__ import annotations

import json
import re
import shutil
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
VOCABULARY = re.compile("|".join((
    "orca", "wsl", "kimi", "vscode", "openclaw",
    "codex", "claude", r"\bagy\b", "gemini", "anthropic", "openai",
    "github", "bwrap", "bubblewrap", "seccomp", "appcontainer", "systemd", "launchd",
)), re.IGNORECASE)
# The repository's own hosting configuration is not the engine.
HOSTING_PREFIXES = (".github/",)
# Lines that name where this repository lives, not something the engine uses.
HOSTING_LINES = ("github.com/justinyu73/loop-hybrid-2", ".github/workflows/ci.yml")
# Portability guards that forbid a platform token must name it.  Only these exact
# lines may carry it; any other line in the same file still fails.
GUARD_LINES = {
    "tools/portable_runtime_contract.py": (
        're.compile(r"(?<![a-z])wsl(?![a-z])", re.IGNORECASE)',
        're.compile(r"(?<![a-z])systemd(?![a-z])", re.IGNORECASE)',
    ),
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
        if rel == SELF or rel.startswith(HOSTING_PREFIXES):
            continue
        allowed = (*GUARD_LINES.get(rel, ()), *HOSTING_LINES)
        for number, line in enumerate(text.splitlines(), 1):
            if VOCABULARY.search(line) and not any(marker in line for marker in allowed):
                hits.append(f"{rel}:{number}")
    return case("no-tool-or-vendor-vocabulary", not hits, {"hits": len(hits), "first": hits[:25]})


def _recording_port() -> Any:
    """A fence port that admits one descriptor and records what it would launch."""
    import execution_fence as fences

    class RecordingPort(fences.ExecutionFencePort):
        def __init__(self) -> None:
            self.launched: list[dict[str, Any]] = []

        @staticmethod
        def descriptor() -> dict[str, Any]:
            return {"launch_descriptor_digest": "sha256:" + "0" * 64, "binding": {}}

        def prepare(self, binding: Any) -> dict[str, Any]:
            return self.descriptor()

        def launch(self, descriptor: Any, argv: Any, *, input_text: Any = None, timeout_seconds: float,
                   env_projection: Any = None, on_started: Any = None) -> subprocess.CompletedProcess[str]:
            self.launched.append({"argv": list(argv), "input_text": input_text})
            return subprocess.CompletedProcess(list(argv), 0, "{}\n", "")

        def receipt_projection(self, descriptor: Any) -> dict[str, Any]:
            return {}

    return RecordingPort()


def _no_path_search(record: list[str]):
    """Patch every PATH lookup the core could use; each call is recorded."""
    import instance_config

    saved = (shutil.which, instance_config.discover_executable)

    def which(name: Any, *args: Any, **kwargs: Any) -> None:
        record.append(f"which:{name}")
        return None

    def discover(command: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        record.append(f"discover:{command}")
        return {"path": None, "source": "missing"}

    shutil.which = which  # type: ignore[assignment]
    instance_config.discover_executable = discover  # type: ignore[assignment]

    def restore() -> None:
        shutil.which, instance_config.discover_executable = saved  # type: ignore[assignment]

    return restore


def declared_executor_case() -> dict[str, Any]:
    import cli_agent_executor as executors
    import goal_loop_run as run

    builtin = {name: sorted(getattr(run, name, {}) or ()) for name in (
        "EXECUTORS", "HOST_EXECUTORS", "JUDGE_EXECUTORS", "CAPABILITY_EVALUATION_EXECUTORS")}
    declarations = executors.validate_executor_declarations({
        "coder": {"argv": [sys.executable, "-c", "import sys; print(sys.argv[1])", "{prompt}"]},
    })
    detail: dict[str, Any] = {"builtin": builtin}
    try:
        executors.validate_executor_declarations({"relative": {"argv": ["agent", "{prompt}"]}})
        detail["relative_argv"] = "accepted"
    except ValueError as exc:
        detail["relative_argv"] = f"refused: {exc}"
    try:
        run.resolve_executor("undeclared", execute=True, declarations=declarations)
        detail["undeclared"] = "accepted"
    except ValueError as exc:
        detail["undeclared"] = f"refused: {exc}"
    port = _recording_port()
    lookups: list[str] = []
    restore = _no_path_search(lookups)
    try:
        agent = executors.make_declared_agent("coder", declarations, execution_fence_port=port)
        agent(REPO, {"run_id": "run-1", "attempt": 1, "goal": {"goal_id": "g"},
                     "base_revision": "0" * 40, "execution_fence": port.descriptor()})
    except Exception as exc:  # The exam records the refusal instead of crashing.
        detail["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        restore()
    launched = port.launched[0] if port.launched else {}
    detail.update(launched_argv0=(launched.get("argv") or [None])[0], lookups=lookups)
    ok = (not any(builtin.values())
          and str(detail["relative_argv"]).startswith("refused")
          and str(detail["undeclared"]).startswith("refused")
          and launched.get("argv", [None])[0] == sys.executable
          and "{prompt}" not in launched.get("argv", [])
          and not lookups and "error" not in detail)
    return case("executors-are-declared-and-never-searched", ok, detail)


def capability_declared_case() -> dict[str, Any]:
    import cli_agent_executor as executors
    import execution_fence as fences
    import goal_loop_run as run

    session = run.CapabilityRoutingSession.__new__(run.CapabilityRoutingSession)
    session.timeout_seconds = 30.0
    session.factories = {}
    session.execution_fence_port = fences.DisabledExecutionFencePort()
    session.executor_declarations = executors.validate_executor_declarations({
        # The resource pins a model, so the declaration carries the {model} slot.
        "coder": {"argv": [sys.executable, "-c", "pass", "{model}", "{prompt}"]},
    })
    resource = {"runner": "coder", "model": "fixture-model", "provider_binding": None}
    node = {"budget": {"max_wall_seconds": 10}}
    detail: dict[str, Any] = {}
    session.execution_host_binding = {"schema": run.EXECUTION_HOST_SCHEMA, "host_id": "headless_cli",
                                      "adapter": "headless_cli"}
    try:
        model = session._model(resource, node)
        detail["adapter"] = list(fences.model_fence_identity(model))
        detail["fenced"] = fences.model_requires_fence(model)
    except Exception as exc:
        detail["error"] = f"{type(exc).__name__}: {exc}"
    session.execution_host_binding = None
    try:
        session._model(resource, node)
        detail["unbound"] = "accepted"
    except ValueError as exc:
        detail["unbound"] = f"refused: {exc}"
    ok = (detail.get("adapter", [None])[0] == "declared-coder" and detail.get("fenced") is True
          and str(detail.get("unbound", "")).startswith("refused"))
    return case("capability-production-runs-a-declaration", ok, detail)


def rollover_case(files: dict[str, str]) -> dict[str, Any]:
    import command_ingress

    supported = sorted(set(SESSION_ROLLOVER_EVENTS) & set(command_ingress.SUPPORTED_EVENT_TYPES))
    mentions = sorted(rel for rel, text in files.items()
                      if rel != SELF and any(name in text for name in SESSION_ROLLOVER_EVENTS))
    return case("no-session-rollover-protocol", not supported and not mentions,
                {"supported": supported, "mentioned_in": mentions})


def fence_case() -> dict[str, Any]:
    import importlib.util

    import execution_fence as fences

    present = [name for name, found in (
        ("CONTROL_LAUNCH_BUDGET", hasattr(fences, "CONTROL_LAUNCH_BUDGET")),
        ("ExecutionFencePort.launch_control", hasattr(fences.ExecutionFencePort, "launch_control")),
        ("ExecutionFencePort.launch_provider", hasattr(fences.ExecutionFencePort, "launch_provider")),
        ("load_egress_policy", hasattr(fences, "load_egress_policy")),
        ("kernel backend: linux", importlib.util.find_spec("execution_fence_linux") is not None),
        ("kernel backend: windows", importlib.util.find_spec("execution_fence_windows") is not None),
    ) if found]
    detail: dict[str, Any] = {"present": present}
    with tempfile.TemporaryDirectory(prefix="lh-pure-loop-fence-") as raw:
        clone = Path(raw).resolve()
        try:
            port = fences.configured_execution_fence({"LH_EXECUTION_FENCE_BACKEND": "local-process"})
            binding = fences.build_attempt_binding(
                goal={"goal_id": "g"}, run_id="run-1", attempt=1, attempt_fence=1,
                base_revision="0" * 40, clone_root=clone, verifier_argv=[sys.executable],
                adapter_id="declared-coder", adapter_version="v1", timeout_seconds=30)
            descriptor = port.prepare(binding)
            proc = port.launch(descriptor, [sys.executable, "-c", "print('local-ok')"], timeout_seconds=30)
            receipt = port.receipt_projection(descriptor)
            detail.update(port=type(port).__name__, returncode=proc.returncode,
                          stdout=(proc.stdout or "").strip()[-40:],
                          kernel_containment=receipt.get("kernel_containment"))
        except Exception as exc:
            detail["error"] = f"{type(exc).__name__}: {exc}"
    ok = (not present and detail.get("returncode") == 0 and detail.get("stdout") == "local-ok"
          and detail.get("kernel_containment") is False)
    return case("fence-is-a-port-with-an-honest-local-backend", ok, detail)


def instance_case() -> dict[str, Any]:
    from instance_config import initialize_instance

    with tempfile.TemporaryDirectory(prefix="lh-pure-loop-") as raw:
        root = Path(raw)
        home = root / "home"
        paths = {key: str(root / key) for key in ("repo", "state", "workspace", "cache", "logs")}
        config = initialize_instance(root / "config" / "instance.json", system="Linux",
                                     environ={"HOME": str(home), "PATH": ""}, home=home, cwd=root,
                                     overrides={"paths": paths})
        cli_keys = sorted(config.data["cli"])
        policy_path = getattr(config, "egress_policy_path", None)
        policy = (json.loads(Path(policy_path).read_text(encoding="utf-8"))
                  if policy_path is not None and Path(policy_path).is_file() else {})
        tool_entries = sorted(key for key in policy if key.startswith("control") or key.endswith("_cli")
                              or key == "provider_sandbox_profile")
    return case("instance-has-no-tool-entry", cli_keys == ["providers"] and not tool_entries,
                {"cli_keys": cli_keys, "tool_entries": tool_entries})


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


def lifecycle_case() -> dict[str, Any]:
    import lifecycle

    present = [name for name in ("platform_adapter_kinds", "build_adapter_descriptor") if hasattr(lifecycle, name)]
    foreground = lifecycle.build_foreground_descriptor(("python", "-m", "lh_runtime.goal_loop_run"))
    return case("lifecycle-is-the-foreground-process", not present and foreground["platform"] == "any",
                {"present": present, "adapter": foreground.get("adapter")})


def pricing_case() -> dict[str, Any]:
    import token_cost

    usage = token_cost.measured_usage(model="any-model", input_tokens=1000, output_tokens=10)
    undeclared = token_cost.compute_cost(usage)
    declared = token_cost.compute_cost(
        usage, pricing={"any-model": {"input": 1.0, "output": 2.0, "cache_read": 0.1}})
    # A built-in table is any module-level mapping of model ids to rates.
    tables = sorted(name for name, value in vars(token_cost).items()
                    if isinstance(value, dict) and value
                    and all(isinstance(rates, dict) and {"input", "output"} <= set(rates) for rates in value.values()))
    ok = (not tables and undeclared.get("state") == token_cost.USAGE_UNKNOWN
          and declared.get("state") == token_cost.USAGE_MEASURED)
    return case("cost-comes-only-from-declared-pricing", ok,
                {"builtin_tables": tables, "undeclared": undeclared.get("state"), "declared": declared.get("state")})


def main() -> int:
    cases: list[dict[str, Any]] = []
    try:
        files = _tracked_text()
        cases.append(vocabulary_case(files))
        cases.append(rollover_case(files))
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        cases.append(case("tracked-scan-crashed", False, f"{type(exc).__name__}: {exc}"))
    for build in (declared_executor_case, capability_declared_case, fence_case, instance_case,
                  host_contract_case, lifecycle_case, pricing_case):
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

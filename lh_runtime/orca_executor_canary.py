#!/usr/bin/env python3
"""CUT-5 provider-free proof for the Orca-managed terminal executor.

The fake Orca CLI below exercises the public create/wait/read/close/stop
surface without starting Orca, a provider, or a real terminal.  The canary
proves that LH sends the provider command to the existing disposable path,
waits on an exit result, preserves the existing usage hook boundary, and
stops on an unsatisfied wait instead of declaring a completed attempt.
"""
from __future__ import annotations

import json
import hashlib
import os
import stat
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
import cli_agent_executor as executors
from cli_agent_executor import _orca_worktree_selector
from _fixture import (
    FixtureExecutionFencePort,
    capsule_with_fence,
    make_campaign,
    make_source_repo,
)
from capability_resolver_canary import graph
from executor_wiring_canary import _noop_sleep
from campaign_compiler import CampaignCompiler
from goal_loop_run import EXECUTORS, JUDGE_EXECUTORS, resolve_executor
from goal_store import GoalStore
from run_store import RunStore
import goal_loop_run as fixture_glr
from p7_native_runstore_fixture import explicit_runstore_factory
from native_delivery_fixture import make_native_bundle


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


FAKE_ORCA = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import sys

state = pathlib.Path(os.environ["FAKE_ORCA_STATE"])
args = sys.argv[1:]
with state.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args, ensure_ascii=False) + "\n")

if args[:2] == ["status", "--json"]:
    print(json.dumps({"capability": {
        "schema": "external-orca-cli-capability/v1",
        "protocol": "1",
        "version": "1.2.0",
        "operations": ["capability_probe", "repo_list", "repo_add", "project_setup_delete", "terminal_create", "terminal_wait", "terminal_read", "terminal_stop", "terminal_close"],
        "path_codecs": ["posix", "windows-drive", "windows-unc", "macos-posix"],
        "platforms": ["linux", "windows", "macos"],
    }}))
elif args[:2] == ["terminal", "create"]:
    print(json.dumps({"terminal": {"handle": "term-cut5", "title": "LH fixture"}}))
elif args[:2] == ["terminal", "wait"]:
    if os.environ.get("FAKE_ORCA_MODE") == "timeout":
        print(json.dumps({"wait": {"handle": "term-cut5", "condition": "exit", "satisfied": False, "status": "running", "exitCode": None}}))
        raise SystemExit(1)
    print(json.dumps({"wait": {"handle": "term-cut5", "condition": "exit", "satisfied": True, "status": "exited", "exitCode": 0}}))
elif args[:2] == ["terminal", "read"]:
    print(json.dumps({"terminal": {"handle": "term-cut5", "status": "exited", "tail": ["fixture output", "finished"]}}))
elif args[:2] == ["terminal", "close"]:
    print(json.dumps({"close": {"handle": "term-cut5"}}))
elif args[:2] == ["terminal", "stop"]:
    print(json.dumps({"stopped": 1}))
else:
    print(json.dumps({"error": "unexpected command", "args": args}))
    raise SystemExit(2)
'''


FAKE_ORCA_RUNTIME = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import subprocess
import sys

args = sys.argv[1:]
log = pathlib.Path(os.environ["FAKE_ORCA_STATE"])
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args, ensure_ascii=False) + "\n")

if args[:2] == ["status", "--json"]:
    print(json.dumps({"capability": {
        "schema": "external-orca-cli-capability/v1",
        "protocol": "1",
        "version": "1.2.0",
        "operations": ["capability_probe", "repo_list", "repo_add", "project_setup_delete", "terminal_create", "terminal_wait", "terminal_read", "terminal_stop", "terminal_close"],
        "path_codecs": ["posix", "windows-drive", "windows-unc", "macos-posix"],
        "platforms": ["linux", "windows", "macos"],
    }}))
elif args[:2] == ["terminal", "create"]:
    command = args[args.index("--command") + 1]
    completed = subprocess.run(["/bin/sh", "-lc", command], cwd=os.getcwd(), capture_output=True, text=True)
    pathlib.Path(os.environ["FAKE_ORCA_EXIT"]).write_text(str(completed.returncode), encoding="utf-8")
    pathlib.Path(os.environ["FAKE_ORCA_OUTPUT"]).write_text(completed.stdout + completed.stderr, encoding="utf-8")
    print(json.dumps({"terminal": {"handle": "term-runtime"}}))
elif args[:2] == ["terminal", "wait"]:
    code = int(pathlib.Path(os.environ["FAKE_ORCA_EXIT"]).read_text(encoding="utf-8"))
    print(json.dumps({"wait": {"handle": "term-runtime", "condition": "exit", "satisfied": True, "status": "exited", "exitCode": code}}))
elif args[:2] == ["terminal", "read"]:
    print(json.dumps({"terminal": {"handle": "term-runtime", "status": "exited", "tail": []}}))
elif args[:2] == ["terminal", "close"]:
    print(json.dumps({"close": {"handle": "term-runtime"}}))
else:
    print(json.dumps({"error": "unexpected command", "args": args}))
    raise SystemExit(2)
'''


FAKE_ORCA_IMPORT = r'''#!/usr/bin/env python3
import json
import os
import pathlib
import sys

args = sys.argv[1:]
state = pathlib.Path(os.environ["FAKE_ORCA_STATE"])
registered = pathlib.Path(os.environ["FAKE_ORCA_REGISTERED"])
with state.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args, ensure_ascii=False) + "\n")

if args[:2] == ["terminal", "create"]:
    if not registered.exists():
        print(json.dumps({"ok": False, "error": {"code": "selector_not_found", "message": "selector_not_found"}}))
        raise SystemExit(1)
    else:
        print(json.dumps({"terminal": {"handle": "term-import"}}))
elif args[:2] == ["repo", "list"]:
    print(json.dumps({"repos": []}))
elif args[:2] == ["repo", "add"]:
    registered.write_text("repo-cut5", encoding="utf-8")
    print(json.dumps({"repo": {"id": "repo-cut5", "path": args[args.index("--path") + 1]}}))
elif args[:2] == ["terminal", "wait"]:
    print(json.dumps({"wait": {"handle": "term-import", "satisfied": True, "status": "exited", "exitCode": 0}}))
elif args[:2] == ["terminal", "read"]:
    print(json.dumps({"terminal": {"handle": "term-import", "tail": ["registered output"]}}))
elif args[:2] == ["terminal", "close"]:
    print(json.dumps({"close": {"handle": "term-import"}}))
elif args[:2] == ["project", "setup-delete"]:
    pathlib.Path(os.environ["FAKE_ORCA_CLEANED"]).write_text(args[args.index("--setup") + 1], encoding="utf-8")
    print(json.dumps({"result": {"setup": {"id": "repo-cut5"}}}))
else:
    print(json.dumps({"error": "unexpected command", "args": args}))
    raise SystemExit(2)
'''


def _fake_orca(root: Path) -> tuple[Path, Path]:
    cli = root / "fake-orca"
    cli.write_text(FAKE_ORCA, encoding="utf-8")
    cli.chmod(cli.stat().st_mode | stat.S_IXUSR)
    state = root / "orca.log"
    os.environ["FAKE_ORCA_STATE"] = str(state)
    return cli, state


def _fake_runtime_tools(root: Path) -> tuple[Path, Path, Path, Path]:
    fake_bin = root / "fake-bin"
    fake_bin.mkdir()
    provider = fake_bin / "codex"
    provider.write_text("#!/bin/sh\nmkdir -p src\nprintf 'from-orca\\n' > src/from-orca.txt\nprintf '{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":12,\"cached_input_tokens\":4,\"output_tokens\":2}}\\n'\n", encoding="utf-8")
    provider.chmod(provider.stat().st_mode | stat.S_IXUSR)
    cli = root / "fake-orca-runtime"
    cli.write_text(FAKE_ORCA_RUNTIME, encoding="utf-8")
    cli.chmod(cli.stat().st_mode | stat.S_IXUSR)
    state, exit_file, output = root / "runtime-orca.log", root / "runtime-exit", root / "runtime-output"
    os.environ.update({"FAKE_ORCA_STATE": str(state), "FAKE_ORCA_EXIT": str(exit_file), "FAKE_ORCA_OUTPUT": str(output)})
    return cli, fake_bin, state, output


def _fake_import_tools(root: Path) -> tuple[Path, Path, Path, Path]:
    cli = root / "fake-orca-import"
    cli.write_text(FAKE_ORCA_IMPORT, encoding="utf-8")
    cli.chmod(cli.stat().st_mode | stat.S_IXUSR)
    state, registered, cleaned = root / "import-orca.log", root / "registered", root / "cleaned"
    os.environ.update({"FAKE_ORCA_STATE": str(state), "FAKE_ORCA_REGISTERED": str(registered), "FAKE_ORCA_CLEANED": str(cleaned)})
    return cli, state, registered, cleaned


def main() -> int:
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        cli, state = _fake_orca(root)
        workspace = root / "workspace"
        workspace.mkdir()
        seen: dict[str, object] = {}
        fixture_fence = FixtureExecutionFencePort()

        def collector(_proc: object, context: dict[str, object]) -> dict[str, object]:
            seen["context"] = context
            return {"state": "measured", "model": "fixture", "input_tokens": 3, "output_tokens": 2, "cache_read_tokens": 0}

        agent = executors.make_orca_agent(
            agent="codex",
            orca_cli=str(cli),
            provider_argv_builder=lambda prompt: ["/bin/echo", prompt],
            usage_collector=collector,
            snapshot_fn=lambda: {"path": "fixture-session", "usage": None},
            timeout_seconds=2,
            execution_fence_port=fixture_fence,
        )
        result = agent(workspace, capsule_with_fence(fixture_fence, workspace, {"run_id": "run-cut5", "attempt": 1, "goal": {"feature_contract": "x"}, "base_revision": "base"}))
        commands = [json.loads(line) for line in state.read_text(encoding="utf-8").splitlines()]
        create = next((args for args in commands if args[:2] == ["terminal", "create"]), [])

        binding = {"runner": "codex", "base_url": "http://127.0.0.1:8801/v1", "model": "fixture-codex"}
        binding_bin = root / "binding-bin"
        binding_bin.mkdir()
        binding_codex = binding_bin / "codex"
        binding_codex.write_text(
            "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":11,\"cached_input_tokens\":3,\"output_tokens\":2}}'\n",
            encoding="utf-8",
        )
        binding_codex.chmod(binding_codex.stat().st_mode | stat.S_IXUSR)
        bound_agent = executors.make_orca_agent(
            agent="codex", orca_cli=str(cli),
            provider_argv_builder=lambda prompt: [str(binding_codex), "exec", "--ephemeral", prompt],
            usage_collector=lambda _proc, _context: {
                "state": "measured", "model": "fixture-codex", "input_tokens": 11,
                "output_tokens": 2, "cache_read_tokens": 3,
            },
            provider_binding=binding, timeout_seconds=2,
            execution_fence_port=fixture_fence,
        )
        bound = bound_agent(workspace, capsule_with_fence(fixture_fence, workspace, {"run_id": "run-cut5-binding", "attempt": 1, "goal": {}, "base_revision": "base"}))
        commands = [json.loads(line) for line in state.read_text(encoding="utf-8").splitlines()]
        bound_create = [args for args in commands if args[:2] == ["terminal", "create"]][-1]
        bound_command = bound_create[bound_create.index("--command") + 1]
        try:
            executors._bind_orca_provider_argv(
                "claude", ["claude", "-p", "P"],
                {"runner": "claude", "base_url": "https://mock.example/v1", "model": "fixture-claude"},
            )
            retired_binding_rejected = False
        except ValueError:
            retired_binding_rejected = True
        try:
            executors.make_orca_agent(
                agent="kimi", orca_cli=str(cli), provider_argv_builder=lambda prompt: ["kimi", "-p", prompt],
                provider_binding={"runner": "kimi", "base_url": "https://mock.example/v1", "model": "fixture-kimi"}, timeout_seconds=2,
                execution_fence_port=fixture_fence,
            )(workspace, capsule_with_fence(fixture_fence, workspace, {"run_id": "run-cut5-kimi", "attempt": 1, "goal": {}, "base_revision": "base"}))
            kimi_rejected = False
        except ValueError:
            kimi_rejected = True
        try:
            executors._bind_orca_provider_argv(
                "codex", ["codex", "exec", "P"],
                {"runner": "kimi", "base_url": "https://mock.example/v1", "model": "fixture"},
            )
            mismatch_rejected = False
        except ValueError:
            mismatch_rejected = True

        os.environ["FAKE_ORCA_MODE"] = "timeout"
        timeout_agent = executors.make_orca_agent(
            agent="codex", orca_cli=str(cli), provider_argv_builder=lambda prompt: ["/bin/echo", prompt], timeout_seconds=1,
            execution_fence_port=fixture_fence,
        )
        try:
            timeout_agent(workspace, capsule_with_fence(fixture_fence, workspace, {"run_id": "run-cut5-timeout", "attempt": 1, "goal": {}, "base_revision": "base"}))
            timeout_raises = False
        except TimeoutError:
            timeout_raises = True
        finally:
            os.environ.pop("FAKE_ORCA_MODE", None)
        commands = [json.loads(line) for line in state.read_text(encoding="utf-8").splitlines()]

        import_root = root / "import-fixture"
        import_root.mkdir()
        import_cli, import_log, registered, cleaned = _fake_import_tools(import_root)
        (import_root / "workspace").mkdir()
        import_agent = executors.make_orca_agent(
            agent="codex", orca_cli=str(import_cli),
            provider_argv_builder=lambda prompt: ["/bin/echo", "registered output"], timeout_seconds=2,
            execution_fence_port=fixture_fence,
        )
        import_result = import_agent(import_root / "workspace", capsule_with_fence(fixture_fence, import_root / "workspace", {"run_id": "run-cut5-import", "attempt": 1, "goal": {}, "base_revision": "base"}))
        import_commands = [json.loads(line) for line in import_log.read_text(encoding="utf-8").splitlines()]

        runtime_root = root / "runtime-fixture"
        runtime_root.mkdir()
        trusted_bootstrap_root = runtime_root / "trusted-bootstrap"
        bootstrap_path = (
            trusted_bootstrap_root
            / "docs"
            / "bootstrap-authority.md"
        )
        bootstrap_path.parent.mkdir(parents=True)
        bootstrap_path.write_text(
            '<a id="lh-external-bootstrap-001"></a>\n'
            "### Unified bootstrap fixture\n",
            encoding="utf-8",
        )
        bootstrap_digest = (
            "sha256:" + hashlib.sha256(bootstrap_path.read_bytes()).hexdigest()
        )
        runtime_cli, fake_bin, runtime_log, _ = _fake_runtime_tools(runtime_root)
        source, base = make_source_repo(runtime_root)
        runtime_campaign = make_campaign("campaign-orca")
        envelope = CampaignCompiler(runtime_campaign).compile()["stages"]["stage-1"]
        runtime_binding = make_native_bundle(
            source,
            base,
            "campaign-orca:stage-1",
            "stage-1",
            [{
                "id": "orca-source-check",
                "commands": [{
                    "id": "diff-check",
                    "argv": ["git", "diff", "--cached", "--check"],
                    "cwd": "${WORKTREE}",
                    "expect_exit": 0,
                    "timeout_seconds": 10,
                }],
                "required_receipts": ["executor"],
            }],
            [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if Path('src/from-orca.txt').is_file() else 1)"],
            ["src/"],
            int(envelope["max_attempts"]),
            goal={"feature_contract": "stage-1", "admission_envelope": envelope},
        )
        GoalStore(runtime_root / "goals").record_event(
            event_id="orca-seed-1", idempotency_key="orca-seed-1", source="manual_intent", event_type="goal_candidate",
            payload={"candidate": {"goal_id": "campaign-orca:stage-1", "campaign_id": "campaign-orca", "stage_id": "stage-1", "goal": runtime_binding["goal"]}},
        )
        old_path = os.environ.get("PATH", "")
        old_orca_cli = os.environ.get("LH_ORCA_CLI")
        old_orca_agent = os.environ.get("LH_ORCA_AGENT")
        old_trusted_root = os.environ.get("LH_TRUSTED_BOOTSTRAP_ROOT")
        os.environ["PATH"] = f"{fake_bin}:{old_path}"
        os.environ["LH_ORCA_CLI"] = str(runtime_cli)
        os.environ["LH_ORCA_AGENT"] = "codex"
        os.environ["LH_TRUSTED_BOOTSTRAP_ROOT"] = str(trusted_bootstrap_root)
        try:
            from goal_loop_run import run
            runtime_graph = graph(evaluate=False)
            for resource, runner in zip(
                runtime_graph["registry"]["resources"],
                ("codex", "codex", "codex", "codex"),
            ):
                resource["runner"] = runner
                resource["model_family"] = f"ambient:{runner}"
                resource["endpoint_ref"] = f"ambient:{runner}"
                resource["network_access"] = "external"
                resource["data_boundary"] = "external"
                resource.pop("model", None)
                resource["trust_tier"] = "claimed"
            runtime_graph["nodes"][0]["permissions"]["network"] = "external"
            runtime_graph["nodes"][0]["data_boundary"] = "external"
            with explicit_runstore_factory(fixture_glr):
                runtime_result = run(
                    execute=True,
                    goal_store_root=runtime_root / "goals", run_store_root=runtime_root / "runs",
                    workspace_root=runtime_root / "workspaces", campaign=runtime_campaign,
                    source_repo=source, base_revision=base, max_cycles=30,
                    execution_graph=runtime_graph,
                    execution_host="external-orca",
                    bootstrap_authority={
                        "decision_id": "LH-EXTERNAL-BOOTSTRAP-001",
                        "authority_ref": "docs/bootstrap-authority.md#lh-external-bootstrap-001",
                        "authority_digest": bootstrap_digest,
                        "root": str(trusted_bootstrap_root),
                    },
                    sleep_fn=_noop_sleep,
                    execution_fence_port=fixture_fence,
                )
            goal = GoalStore(runtime_root / "goals").get_goal("campaign-orca:stage-1")
            receipt_meta = RunStore(runtime_root / "runs").latest_receipt(goal["run_id"])
            receipt = json.loads((runtime_root / "runs" / receipt_meta["receipt_ref"]).read_text(encoding="utf-8")) if receipt_meta else {}
        finally:
            os.environ["PATH"] = old_path
            for name, value in (
                ("LH_ORCA_CLI", old_orca_cli),
                ("LH_ORCA_AGENT", old_orca_agent),
                ("LH_TRUSTED_BOOTSTRAP_ROOT", old_trusted_root),
            ):
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
        runtime_commands = [json.loads(line) for line in runtime_log.read_text(encoding="utf-8").splitlines()]

        registry_ok = set(EXECUTORS) == {"codex", "orca"} and JUDGE_EXECUTORS == {"agy", "codex"}
        dry = resolve_executor("orca", execute=False)
        pinned_codex = executors.hosted_provider_argv("codex", "P", "gpt-code")
        try:
            executors.hosted_provider_argv("kimi", "P", "kimi-code/k3")
            pinned_kimi_retired = False
        except ValueError as exc:
            pinned_kimi_retired = "kimi_retired" in str(exc)
        canonical_echo = str(Path("/bin/echo").resolve(strict=False))
        cases = [
            case("registry-adds-orca-but-keeps-judge-direct", registry_ok, f"executors={sorted(EXECUTORS)} judges={sorted(JUDGE_EXECUTORS)}"),
            case(
                "host-runs-the-selected-model-without-becoming-the-model",
                pinned_codex[:4] == ["codex", "exec", "-m", "gpt-code"]
                and pinned_kimi_retired,
                json.dumps({"codex": pinned_codex, "kimi_retired": pinned_kimi_retired}),
            ),
            case("create-targets-existing-disposable-workspace", any(_orca_worktree_selector(workspace) in args for args in create), json.dumps(create)),
            case("provider-command-is-shell-quoted-and-bounded", any(f"exec {canonical_echo}" in args and ".lh-orca-provider-output.jsonl" in args for args in create) and "fixture output\nfinished" == result["stdout_tail"], json.dumps(result)),
            case(
                "codex-binding-is-applied-inside-one-orca-command",
                "model_providers.lh_terminal=" in bound_command
                and 'model_provider="lh_terminal"' in bound_command
                and "-m fixture-codex" in bound_command
                and bound["execution"]["provider_binding"] == {"runner": "codex", "model": "fixture-codex", "mode": "codex_argv"}
                and binding["base_url"] not in json.dumps(bound),
                json.dumps({"command": bound_command, "execution": bound["execution"]}),
            ),
            case(
                "redirected-binding-usage-is-unknown-not-mispriced",
                bound["usage"]["state"] == "unknown"
                and bound["usage"]["model"] == "fixture-codex"
                and "no verified usage/cost attribution" in bound["usage"]["reason"],
                json.dumps(bound["usage"]),
            ),
            case(
                "retired-provider-binding-fails-closed",
                retired_binding_rejected,
                "Claude provider binding is rejected without an adapter",
            ),
            case("unsupported-or-mismatched-binding-fails-closed", kimi_rejected and mismatch_rejected, f"kimi={kimi_rejected} mismatch={mismatch_rejected}"),
            case("existing-usage-hook-is-preserved", result["usage"]["state"] == "measured" and seen["context"]["snapshot"] == {"path": "fixture-session", "usage": None}, json.dumps(result["usage"])),
            case("unsatisfied-wait-stops-and-raises", timeout_raises and any(args[:2] == ["terminal", "stop"] for args in commands), json.dumps(commands[-4:])),
            case(
                "unregistered-disposable-clone-is-imported-and-cleaned",
                import_result["stdout_tail"] == "registered output"
                and any(args[:2] == ["repo", "add"] for args in import_commands)
                and any(args[:2] == ["project", "setup-delete"] for args in import_commands)
                and cleaned.read_text(encoding="utf-8") == "repo-cut5",
                json.dumps({"commands": import_commands, "result": import_result}, ensure_ascii=False),
            ),
            case("dry-run-does-not-construct-orca-process", dry is None, str(dry)),
            case(
                "capability-model-runs-through-separate-orca-host",
                runtime_result.get("invoked") is True
                and runtime_result.get("driver", {}).get("runs_dispatched") == 1
                and goal.get("state") == "completed"
                and receipt.get("binding", {}).get("runner") == "codex"
                and receipt.get("binding", {}).get("execution_host", {}).get(
                    "host_id"
                ) == "external-orca"
                and receipt.get("binding", {}).get("execution_host", {}).get(
                    "bootstrap_authority", {}
                ).get("decision_id") == "LH-EXTERNAL-BOOTSTRAP-001"
                and receipt.get("provider", {}).get("summary") == "codex executor completed via Orca terminal"
                and receipt.get("usage", {}).get("state") == "measured"
                and receipt.get("usage", {}).get("input_tokens") == 8
                and receipt.get("usage", {}).get("cache_read_tokens") == 4
                and receipt.get("usage", {}).get("output_tokens") == 2
                and any(
                    args[:2] == ["terminal", "create"]
                    and "LH-EXTERNAL-BOOTSTRAP-001" in " ".join(args)
                    for args in runtime_commands
                ),
                json.dumps({"runtime": runtime_result, "goal": goal, "receipt": receipt}, ensure_ascii=False),
            ),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    result = {
        "check_id": "lh-orca-executor-cut5",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "known_gaps_open": [
            "real Orca daemon and provider live smoke remains human-run",
            "Orca has no token-usage endpoint; usage must come from the underlying provider session log or remain unknown",
        ],
        "verification": {"command": "python3 -B lh_runtime/orca_executor_canary.py", "spec": "docs/contracts/autonomous-driver-v1.md"},
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

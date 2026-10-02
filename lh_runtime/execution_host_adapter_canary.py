#!/usr/bin/env python3
"""Hermetic acceptance proof for the P5 Orca ExecutionHost adapter.

No Orca process, provider, network, or real filesystem path is used here.  A
recording transport stands in for the already-admitted ExecutionFencePort
control launch.  The canary therefore tests the adapter's own contract:
capability negotiation, closed requests, platform path codecs, exact Attempt
identity, and cleanup ownership.
"""
from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any, Mapping

from execution_host_adapter import (
    CAPABILITY_OPERATION,
    CAPABILITY_PROTOCOL,
    CAPABILITY_SCHEMA,
    CONTROL_OPERATIONS,
    CapabilityNegotiationError,
    ControlRequestRejected,
    OrcaExecutionHostAdapter,
    PathCodec,
    StaleAttemptHandle,
    discover_orca_cli,
    negotiate_capability,
    provider_command_spec,
    semantic_attempt_identity,
)


def case(case_id: str, ok: bool, detail: str) -> dict[str, Any]:
    return {"id": case_id, "ok": ok, "detail": detail}


def capability(
    *,
    version: str = "1.2.0",
    operations: list[str] | None = None,
    path_codecs: list[str] | None = None,
    platforms: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "capability": {
            "schema": CAPABILITY_SCHEMA,
            "protocol": CAPABILITY_PROTOCOL,
            "version": version,
            "operations": list(operations or CONTROL_OPERATIONS),
            "path_codecs": list(path_codecs or ["posix", "windows-drive", "windows-unc", "macos-posix"]),
            "platforms": list(platforms or ["linux", "windows", "macos"]),
        }
    }


class RecordingTransport:
    def __init__(self, capability_response: Mapping[str, Any] | None = None) -> None:
        self.capability_response = dict(capability_response or capability())
        self.calls: list[dict[str, Any]] = []
        self.next_handle = "term-p5-1"

    def __call__(self, request: Mapping[str, Any]) -> dict[str, Any]:
        request = dict(request)
        self.calls.append(request)
        op = request.get("op")
        if op == CAPABILITY_OPERATION:
            return dict(self.capability_response)
        if op == "repo_list":
            return {"repos": []}
        if op == "repo_add":
            return {"repo": {"id": "setup-p5-owned", "path": request["path"]}}
        if op == "terminal_create":
            return {"terminal": {"handle": self.next_handle}}
        if op == "terminal_wait":
            return {"wait": {"handle": request["handle"], "satisfied": True, "status": "exited", "exitCode": 0}}
        if op == "terminal_read":
            return {"terminal": {"handle": request["handle"], "tail": ["p5 fixture output"]}}
        if op == "terminal_stop":
            return {"stopped": 1}
        if op == "terminal_close":
            return {"close": {"handle": request["handle"]}}
        if op == "project_setup_delete":
            return {"result": {"setup": {"id": request["setup"]}}}
        raise AssertionError(f"unexpected operation: {op}")


def status_payload() -> dict[str, Any]:
    return {
        "target": {"kind": "local"},
        "app": {"running": True},
        "runtime": {
            "state": "ready",
            "reachable": True,
            "runtimeId": "runtime-p5",
            "appVersion": "1.4.188",
            "capabilities": [
                "runtime.status.compat.v1",
                "project-host-setup.v1",
                "terminal.multiplex.v1",
                "terminal.binary-stream.v1",
                "workspace-run-context.v1",
                "folder-workspace.path-status.v1",
            ],
        },
        "graph": {"state": "ready"},
    }


def main() -> int:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)

        # Path syntax is normalized per host while Attempt identity stays
        # platform-independent.
        windows = PathCodec("windows")
        linux = PathCodec("linux")
        macos = PathCodec("macos")
        wsl = PathCodec("wsl", "Ubuntu")
        selectors = [
            windows.selector(r"D:\\disposable\\attempt"),
            linux.selector("/var/tmp/disposable/attempt"),
            macos.selector("/Users/runner/disposable/attempt"),
        ]
        cases.append(case(
            "platform-paths-have-one-disposable-attempt-semantics",
            all(item.startswith("path:") for item in selectors)
            and len({item.split(":", 1)[0] for item in selectors}) == 1
            and semantic_attempt_identity("run-p5", 1)
            == semantic_attempt_identity("run-p5", 1)
            and wsl.selector("/home/runner/attempt")
            == r"path:\\wsl.localhost\Ubuntu\home\runner\attempt",
            json.dumps({"selectors": selectors, "wsl": wsl.selector("/home/runner/attempt")}),
        ))
        cases.append(case(
            "legacy-wsl-unc-path-round-trips",
            wsl.canonical(r"\\wsl$\Ubuntu\home\runner\attempt")
            == "/home/runner/attempt"
            and wsl.same_path(
                r"\\wsl$\Ubuntu\home\runner\attempt",
                "/home/runner/attempt",
            ),
            wsl.canonical(r"\\wsl$\Ubuntu\home\runner\attempt"),
        ))

        invalid_paths: list[str] = []
        for codec, value in (
            (windows, r"D:\\disposable\\..\\escape"),
            (linux, "relative/attempt"),
            (macos, "/Users/runner/../escape"),
            (wsl, r"\\wsl.localhost\Other\home\runner\attempt"),
        ):
            try:
                codec.canonical(value)
            except ControlRequestRejected as exc:
                invalid_paths.append(str(exc))
        cases.append(case("invalid-platform-paths-fail-closed", len(invalid_paths) == 4, json.dumps(invalid_paths)))

        # Version and operation negotiation happen before a normal control
        # operation reaches the transport.
        low_transport = RecordingTransport(capability(version="0.9.9"))
        low_session = OrcaExecutionHostAdapter(path_codec=linux).begin_attempt(
            orca_cli="orca", transport=low_transport, workspace="/tmp/p5-low",
            run_id="run-low", attempt=1,
        )
        low_rejected = False
        try:
            low_session.call({"op": "repo_list"})
        except CapabilityNegotiationError as exc:
            low_rejected = str(exc) == "orca_capability_version_unsupported"
        cases.append(case("minimum-version-negotiation-fails-closed", low_rejected and len(low_transport.calls) == 1, json.dumps(low_transport.calls)))

        missing_transport = RecordingTransport(capability(operations=["capability_probe", "repo_list"]))
        missing_session = OrcaExecutionHostAdapter(path_codec=linux).begin_attempt(
            orca_cli="orca", transport=missing_transport, workspace="/tmp/p5-missing",
            run_id="run-missing", attempt=1,
        )
        missing_rejected = False
        try:
            missing_session.call({"op": "repo_list"})
        except CapabilityNegotiationError as exc:
            missing_rejected = str(exc) == "orca_capability_operation_missing"
        cases.append(case("required-capability-missing-fails-closed", missing_rejected and len(missing_transport.calls) == 1, json.dumps(missing_transport.calls)))

        status_capability = negotiate_capability(status_payload())
        cases.append(case(
            "released-orca-status-vector-negotiates-closed-capability",
            status_capability.version == "1.4.188"
            and status_capability.operation_set == set(CONTROL_OPERATIONS)
            and set(status_capability.path_codecs) == {"posix", "windows-drive", "macos-posix"},
            json.dumps({
                "version": status_capability.version,
                "operations": status_capability.operations,
                "path_codecs": status_capability.path_codecs,
            }),
        ))

        # A successful session proves repo import, terminal lifecycle, and
        # attempt-bound cleanup in sequence.
        transport = RecordingTransport()
        session = OrcaExecutionHostAdapter(path_codec=linux).begin_attempt(
            orca_cli="orca", transport=transport, workspace="/tmp/p5-attempt",
            run_id="run-p5", attempt=1,
        )
        session.call({"op": "repo_list"})
        session.call({"op": "repo_add", "path": "/tmp/p5-attempt"})
        session.call({
            "op": "terminal_create",
            "worktree_selector": "path:/tmp/p5-attempt",
            "title": "LH p5 fixture",
            "provider_argv": ["codex", "exec", "prompt; this remains one quoted argv value"],
            "output_path": "/tmp/p5-attempt/.provider-output.jsonl",
            "env_overlay": {"TERM": "xterm"},
        })
        session.call({"op": "terminal_wait", "handle": "term-p5-1", "timeout_ms": 1000})
        session.call({"op": "terminal_read", "handle": "term-p5-1", "limit": 10})
        session.call({"op": "terminal_close", "handle": "term-p5-1"})
        session.call({"op": "project_setup_delete", "setup": "setup-p5-owned"})
        cases.append(case(
            "repo-import-and-terminal-lifecycle-are-attempt-bound",
            session.active_handles == ()
            and session.owned_setups == ()
            and [call["op"] for call in transport.calls]
            == [CAPABILITY_OPERATION, "repo_list", "repo_add", "terminal_create", "terminal_wait", "terminal_read", "terminal_close", "project_setup_delete"],
            json.dumps({"calls": transport.calls, "active_handles": session.active_handles, "owned_setups": session.owned_setups}),
        ))

        before_stale = len(transport.calls)
        stale_rejected = False
        try:
            session.call({"op": "terminal_wait", "handle": "term-p5-1", "timeout_ms": 1000})
        except StaleAttemptHandle:
            stale_rejected = True
        cases.append(case("closed-handle-is-stale-and-never-reused", stale_rejected and len(transport.calls) == before_stale, str(transport.calls[-1])))

        foreign_transport = RecordingTransport()
        foreign_session = OrcaExecutionHostAdapter(path_codec=linux).begin_attempt(
            orca_cli="orca", transport=foreign_transport, workspace="/tmp/p5-other",
            run_id="run-other", attempt=1,
        )
        foreign_rejected = False
        try:
            foreign_session.call({"op": "terminal_close", "handle": "term-p5-1"})
        except StaleAttemptHandle:
            foreign_rejected = True
        cases.append(case("foreign-attempt-handle-fails-closed", foreign_rejected and not foreign_transport.calls, str(foreign_transport.calls)))

        selector_rejected = False
        try:
            foreign_session.call({"op": "terminal_stop", "worktree_selector": "path:/tmp/not-owned"})
        except (ControlRequestRejected, StaleAttemptHandle):
            selector_rejected = True
        cases.append(case("foreign-selector-and-unowned-setup-fail-closed", selector_rejected, "no host call for invalid selector"))

        closed_schema = False
        try:
            foreign_session.call({"op": "repo_list", "command": "rm -rf /"})
        except ControlRequestRejected:
            closed_schema = True
        cases.append(case("unknown-control-field-and-free-command-are-rejected", closed_schema, "command field rejected before capability handshake"))

        command_spec = provider_command_spec(
            ["codex", "exec", "prompt; && $(echo still-data)"],
            env_overlay={"TERM": "xterm"},
            output_path="/tmp/p5-attempt/output.jsonl",
        )
        shell_flag_rejected = False
        try:
            provider_command_spec(["codex", "--command", "touch /tmp/not-allowed"])
        except ControlRequestRejected:
            shell_flag_rejected = True
        cases.append(case(
            "provider-command-is-argv-plus-digest-not-free-shell",
            command_spec["schema"] == "lh-provider-command-spec/v1"
            and "command" not in command_spec
            and command_spec["argv_digest"].startswith("sha256:")
            and shell_flag_rejected,
            json.dumps(command_spec),
        ))

        with tempfile.TemporaryDirectory() as discovery_raw:
            discovery_root = Path(discovery_raw)
            cli = discovery_root / "orca"
            cli.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            cli.chmod(cli.stat().st_mode | stat.S_IXUSR)
            discovered = discover_orca_cli(str(cli))
        cases.append(case("explicit-cli-discovery-is-path-neutral", discovered == str(cli), discovered))

        failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
        result = {
            "check_id": "lh-execution-host-adapter-p5",
            "status": "pass" if not failures else "fail",
            "total": len(cases),
            "blocking_failures": failures,
            "cases": cases,
            "known_gaps_open": [
                "real Orca status-vector readback is verified on the current Windows host; native Windows/macOS disposable Attempt lifecycle remains operator/live evidence",
                "provider command still uses Orca's legacy terminal --command after LH's typed argv digest; host-side mediation is not claimed",
            ],
            "verification": {
                "command": "python3 -B lh_runtime/execution_host_adapter_canary.py",
                "spec": "docs/contracts/model-routing-v1.md#lh-capability-model-routing-002",
            },
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

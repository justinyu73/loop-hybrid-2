#!/usr/bin/env python3
"""Hermetic proof for the ``execution_host_port`` facade (no live Orca CLI).

Uses the same FAKE_ORCA stub fixture shape as ``orca_executor_canary.py`` /
``lh-orca-executor-cut5`` -- provider-free, no network, no real terminal --
to prove ``execution_host_port.make_execution_host_port`` (a) rejects a
binding that is not ``external-orca``, (b) delegates actuation to the unchanged,
already-covered ``cli_agent_executor.make_orca_agent``, and (c) writes and
reads back the durable request/receipt markers this repo's controller already
colocates with a disposable clone (see ``lh-disposable-workspace/v1`` in
``controller.py``).

The corresponding LIVE proof against a real Orca CLI and two real Goal stages
is ``execution_host_port_live_canary.py`` -- deliberately not part of this
hermetic gate, the same way ``live_smoke_run.sh --execute`` is opt-in and
separate from ``lh-b12-live-smoke --dry-run``.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import execution_host_port as ehp
from _fixture import FixtureExecutionFencePort, capsule_with_fence


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


class RecordingFencePort(FixtureExecutionFencePort):
    """Record control subprocess budgets for the host-adapter regression."""

    def __init__(self) -> None:
        self.control_calls: list[dict[str, object]] = []

    def launch_control(self, descriptor, request, *, timeout_seconds):
        self.control_calls.append({
            "op": request.get("op"),
            "timeout_ms": request.get("timeout_ms"),
            "timeout_seconds": timeout_seconds,
        })
        return super().launch_control(
            descriptor, request, timeout_seconds=timeout_seconds
        )


FAKE_ORCA = """#!/usr/bin/env python3
import json
import os
import pathlib
import sys

state = pathlib.Path(os.environ["FAKE_ORCA_STATE"])
args = sys.argv[1:]
with state.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(args, ensure_ascii=False) + "\\n")

if args[:2] == ["terminal", "create"]:
    print(json.dumps({"terminal": {"handle": "term-ehp-fixture", "title": "LH fixture"}}))
elif args[:2] == ["status", "--json"]:
    print(json.dumps({"capability": {
        "schema": "external-orca-cli-capability/v1",
        "protocol": "1",
        "version": "1.2.0",
        "operations": ["capability_probe", "repo_list", "repo_add", "project_setup_delete", "terminal_create", "terminal_wait", "terminal_read", "terminal_stop", "terminal_close"],
        "path_codecs": ["posix", "windows-drive", "windows-unc", "macos-posix"],
        "platforms": ["linux", "windows", "macos"],
    }}))
elif args[:2] == ["terminal", "wait"]:
    print(json.dumps({"wait": {"handle": "term-ehp-fixture", "condition": "exit", "satisfied": True, "status": "exited", "exitCode": 0}}))
elif args[:2] == ["terminal", "read"]:
    print(json.dumps({"terminal": {"handle": "term-ehp-fixture", "status": "exited", "tail": ["fixture output", "finished"]}}))
elif args[:2] == ["terminal", "close"]:
    print(json.dumps({"close": {"handle": "term-ehp-fixture"}}))
elif args[:2] == ["terminal", "stop"]:
    print(json.dumps({"stopped": 1}))
else:
    print(json.dumps({"error": "unexpected command", "args": args}))
    raise SystemExit(2)
"""


def main() -> int:
    cases: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        cli = root / "fake-orca"
        cli.write_text(FAKE_ORCA, encoding="utf-8")
        cli.chmod(cli.stat().st_mode | stat.S_IXUSR)
        state = root / "orca.log"
        os.environ["FAKE_ORCA_STATE"] = str(state)
        workspace = root / "workspace"
        workspace.mkdir()

        # (a) round-trip the durable markers directly, independent of Orca.
        binding = {
            "schema": "lh-execution-host-binding/v1",
            "host_id": "external-orca",
            "adapter": "orca-terminal",
            "bootstrap_authority": {
                "schema": "lh-bootstrap-authority/v1",
                "decision_id": "LH-EXTERNAL-BOOTSTRAP-001",
                "authority_ref": "docs/bootstrap-authority.md#lh-external-bootstrap-001",
                "authority_digest": "sha256:" + "0" * 64,
                "root": str(root),
            },
        }
        nested_clone = root / "nested-clone"
        nested_target = nested_clone / "loop-hybrid"
        nested_target.mkdir(parents=True)
        (nested_clone / ".git").mkdir()
        nested_request_path = ehp.write_execution_host_request(
            nested_target, run_id="run-nested", attempt=1, agent="codex",
            execution_host_binding=binding,
        )
        nested_request = ehp.read_execution_host_request(nested_target)
        cases.append(case(
            "nested-target-uses-clone-root-marker",
            nested_request_path.parent == nested_clone / ".git"
            and nested_request is not None
            and nested_request["run_id"] == "run-nested",
            json.dumps(nested_request),
        ))
        assert ehp.read_execution_host_request(workspace) is None
        request_path = ehp.write_execution_host_request(
            workspace, run_id="run-fixture", attempt=1, agent="codex",
            execution_host_binding=binding,
        )
        reread_request = ehp.read_execution_host_request(workspace)
        cases.append(case(
            "request-marker-round-trips",
            request_path.name == ehp.REQUEST_MARKER_NAME
            and request_path.parent == workspace / ".git"
            and reread_request is not None
            and reread_request["schema"] == ehp.REQUEST_SCHEMA
            and reread_request["run_id"] == "run-fixture"
            and reread_request["host_id"] == "external-orca",
            json.dumps(reread_request),
        ))
        assert ehp.read_execution_host_receipt(workspace) is None
        receipt_path = ehp.record_execution_host_receipt(
            workspace, run_id="run-fixture", attempt=1, agent="codex",
            result={
                "summary": "codex executor completed via Orca terminal",
                "usage": {"state": "measured"},
                "execution": {"backend": "orca", "terminal_handle": "term-ehp-fixture", "exit_code": 0},
            },
        )
        reread_receipt = ehp.read_execution_host_receipt(workspace)
        cases.append(case(
            "receipt-marker-round-trips",
            receipt_path.name == ehp.RECEIPT_MARKER_NAME
            and reread_receipt is not None
            and reread_receipt["schema"] == ehp.RECEIPT_SCHEMA
            and reread_receipt["terminal_handle"] == "term-ehp-fixture"
            and reread_receipt["backend"] == "orca",
            json.dumps(reread_receipt),
        ))

        # (b) a non-external-orca binding fails closed before any Orca CLI call.
        fixture_fence = RecordingFencePort()
        rejected = False
        try:
            ehp.make_execution_host_port(
                agent="codex",
                execution_host_binding={"host_id": "not-external-orca"},
                timeout_seconds=5,
                execution_fence_port=fixture_fence,
                orca_cli=str(cli),
            )
        except ValueError:
            rejected = True
        cases.append(case("non-external-orca-binding-fails-closed", rejected, f"rejected={rejected}"))

        # (c) end-to-end: the port delegates to the unchanged make_orca_agent
        # and writes both markers around the same call the mandatory
        # lh-orca-executor-cut5 gate already covers.
        port = ehp.make_execution_host_port(
            agent="codex",
            execution_host_binding=binding,
            timeout_seconds=5,
            execution_fence_port=fixture_fence,
            orca_cli=str(cli),
            provider_argv_builder=lambda prompt: ["/bin/echo", prompt],
        )
        capsule = capsule_with_fence(
            fixture_fence, workspace,
            {"run_id": "run-ehp-e2e", "attempt": 2, "goal": {}, "base_revision": "base"},
        )
        result = port(workspace, capsule)
        post_request = ehp.read_execution_host_request(workspace)
        post_receipt = ehp.read_execution_host_receipt(workspace)
        cases.append(case(
            "port-delegates-and-records-both-markers",
            result.get("summary") == "codex executor completed via Orca terminal"
            and result.get("execution", {}).get("terminal_handle") == "term-ehp-fixture"
            and post_request is not None
            and post_request["run_id"] == "run-ehp-e2e"
            and post_request["attempt"] == 2
            and post_receipt is not None
            and post_receipt["run_id"] == "run-ehp-e2e"
            and post_receipt["terminal_handle"] == "term-ehp-fixture"
            and post_receipt["backend"] == "orca",
            json.dumps({"result": result, "request": post_request, "receipt": post_receipt}),
        ))
        wait_calls = [
            call for call in fixture_fence.control_calls
            if call["op"] == "terminal_wait"
        ]
        cases.append(case(
            "terminal-wait-control-budget-includes-remote-wait-window",
            any(
                call["timeout_ms"] == 5000
                and call["timeout_seconds"] == 35.0
                for call in wait_calls
            ),
            json.dumps(wait_calls),
        ))
        requires_fence = getattr(port, "requires_execution_fence", False)
        adapter_id = getattr(port, "execution_fence_adapter_id", None)
        cases.append(case(
            "port-is-marked-a-mutation-adapter",
            requires_fence is True and adapter_id == "execution-host-port-codex",
            f"requires_execution_fence={requires_fence} adapter_id={adapter_id}",
        ))

    failures = [{"id": c["id"], "detail": c["detail"]} for c in cases if not c["ok"]]
    out = {
        "check_id": "lh-execution-host-port",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
        "cases": cases,
        "known_gaps_open": [
            "this canary is provider-free/Orca-free like lh-orca-executor-cut5; "
            "the live counterpart is execution_host_port_live_canary.py "
            "(LH_EXECUTION_HOST_PORT_LIVE=1, human/operator opt-in, not in gate-pack)",
        ],
        "verification": {
            "command": "python3 -B lh_runtime/execution_host_port_canary.py",
            "spec": "docs/contracts/model-routing-v1.md#lh-capability-model-routing-002",
        },
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

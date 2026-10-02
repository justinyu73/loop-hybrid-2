#!/usr/bin/env python3
"""Fence live activation smoke — the production dispatch chain crosses the
real kernel backend (council rulings D2=A, D4=b in
docs/active/lh-auto-runner-gap-review-plan.md).

Cross-process by construction: the original scheduler and goal-loop entry
functions each run in a fresh child process. The enabled arm explicitly
injects non-kernel RunStore commands only for post-worker checks/verifier;
the real worker kernel proof does not cover these later fixture commands.
Every assertion reads durable bytes the subprocess chain produced
(scheduler event log, run-store receipts, provider artifacts).

Arms:
  enabled   ``LH_EXECUTION_FENCE_BACKEND=linux-bubblewrap-seccomp`` -> the
            registered codex executor resolves to a single-process stub on
            PATH and launches inside real bubblewrap.  The stub attests the
            descriptor-digest env only bwrap sets, reports fork/socket EPERM
            observed from inside the fence, and its workspace write verifies
            the run.
  disabled  env absent -> ``execution_fence_unavailable`` before any child;
            the run parks ``human_required`` with ``provider_invocations`` 0.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

from campaign_compiler import CAMPAIGN_SCHEMA, CampaignCompiler  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from run_store import RunStore  # noqa: E402
from native_delivery_fixture import make_native_bundle  # noqa: E402

BACKEND_ID = "linux-bubblewrap-seccomp"
STUB_SOURCE = """#!/usr/bin/python3
import errno, json, os, socket
report = {
    "descriptor_digest_env": os.environ.get("LH_EXECUTION_FENCE_DESCRIPTOR_DIGEST"),
    "provider_control_channel": os.environ.get("LH_PROVIDER_CONTROL_CHANNEL"),
}
try:
    os.fork()
    report["fork"] = "allowed"
except OSError as exc:
    report["fork"] = "denied:" + (errno.errorcode.get(exc.errno) or str(exc.errno))
try:
    socket.socket()
    report["socket"] = "allowed"
except OSError as exc:
    report["socket"] = "denied:" + (errno.errorcode.get(exc.errno) or str(exc.errno))
os.makedirs("src", exist_ok=True)
with open("src/stub-effect.txt", "w") as fh:
    fh.write("fence-stub-effect\\n")
print("FENCE-STUB " + json.dumps(report, sort_keys=True))
"""


def _case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _git(*args: str) -> None:
    subprocess.run(["git", *args], check=True, capture_output=True, text=True)


def _make_source(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    _git("init", "-q", str(source))
    _git("-C", str(source), "config", "user.email", "fence@example.invalid")
    _git("-C", str(source), "config", "user.name", "Fence Smoke")
    (source / "src").mkdir()
    (source / "src" / "baseline.txt").write_text("baseline\n", encoding="utf-8")
    _git("-C", str(source), "add", "src/baseline.txt")
    _git("-C", str(source), "commit", "-qm", "baseline")
    base = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return source, base


def _campaign_dict() -> dict[str, Any]:
    return {
        "schema": CAMPAIGN_SCHEMA,
        "campaign_id": "campaign-fence",
        "stages": [{
            "stage_id": "stage-1",
            "goal": {"feature_contract": "fence-activation"},
            "allowed_paths": ["src/"],
            "allowed_side_effects": ["workspace", "artifact"],
            "acceptance_lamp": {
                "id": "fence-stub-effect-lamp",
                "smoke": "the stub's workspace write exists",
                "verification_argv": ["sh", "-c", "test -f src/stub-effect.txt"],
            },
            "max_attempts": 2,
            "next_stage_id": None,
        }],
    }


def _stub_bin(root: Path) -> Path:
    stub_dir = root / "stub-bin"
    stub_dir.mkdir()
    stub = stub_dir / "codex"
    stub.write_text(STUB_SOURCE, encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return stub_dir


def _fixture_scheduler_argv(original_argv: list[str]) -> list[str]:
    """Opt in post-worker fixture DI without replacing either CLI body."""

    import_roots = [str(HERE), str(HERE.parent / "tests")]
    goal_bootstrap = (
        "import sys\n"
        f"sys.path[:0] = {import_roots!r}\n"
        "import goal_loop_run as goal_loop\n"
        "from p7_native_runstore_fixture import explicit_runstore_factory\n"
        "with explicit_runstore_factory(goal_loop):\n"
        "    raise SystemExit(goal_loop.main(sys.argv[1:]))\n"
    )
    scheduler_bootstrap = (
        "import functools, subprocess, sys\n"
        "from unittest.mock import patch\n"
        f"sys.path[:0] = {import_roots!r}\n"
        "import scheduler_entrypoint as scheduler\n"
        "def fixture_goal_child(argv, **kwargs):\n"
        f"    return subprocess.run([*argv[:2], '-c', {goal_bootstrap!r}, *argv[3:]], **kwargs)\n"
        "original_execute = scheduler.execute\n"
        "with patch.object(scheduler, 'execute', new=functools.partial(original_execute, runner=fixture_goal_child)):\n"
        "    raise SystemExit(scheduler.main(sys.argv[1:]))\n"
    )
    return [*original_argv[:2], "-c", scheduler_bootstrap, *original_argv[3:]]


def _tick(root: Path, *, backend: str | None,
          post_worker_fixture: bool = False) -> subprocess.CompletedProcess[str]:
    source, base = _make_source(root)
    campaign = _campaign_dict()
    campaign_path = root / "campaign.json"
    campaign_path.write_text(json.dumps(campaign), encoding="utf-8")
    stub_dir = _stub_bin(root)
    # Seeding is a fixture act outside the mechanism under test (N15 precedent:
    # the injected source sits outside the accepted chain).
    envelope = CampaignCompiler(campaign).compile()["stages"]["stage-1"]
    binding = make_native_bundle(
        source,
        base,
        "campaign-fence:stage-1",
        "stage-1",
        [{
            "id": "fence-source-check",
            "commands": [{
                "id": "diff-check",
                "argv": ["git", "diff", "--cached", "--check"],
                "cwd": "${WORKTREE}",
                "expect_exit": 0,
                "timeout_seconds": 10,
            }],
            "required_receipts": ["executor"],
        }],
        [sys.executable, "-B", "-c", "from pathlib import Path; raise SystemExit(0 if Path('src/stub-effect.txt').is_file() else 1)"],
        ["src/"],
        int(envelope["max_attempts"]),
        goal={"feature_contract": "stage-1", "admission_envelope": envelope},
    )
    GoalStore(root / "goals").record_event(
        event_id="fence-smoke-seed",
        idempotency_key="fence-smoke-seed",
        source="manual_intent",
        event_type="goal_candidate",
        payload={"candidate": {
            "goal_id": "campaign-fence:stage-1",
            "campaign_id": "campaign-fence",
            "stage_id": "stage-1",
            "goal": binding["goal"],
        }},
    )
    env = dict(os.environ)
    env["PATH"] = f"{stub_dir}:{env.get('PATH', '')}"
    env["LH_SCHEDULER_OWNER_ID"] = "fence-smoke"
    env.pop("LH_EXECUTION_FENCE_BACKEND", None)
    if backend is not None:
        env["LH_EXECUTION_FENCE_BACKEND"] = backend
    scheduler_argv = [
        sys.executable, "-B", str(HERE / "scheduler_entrypoint.py"),
        "--owner-id", "fence-smoke",
        "--event-log", str(root / "events.jsonl"),
        "--",
        "--executor", "codex",
        "--execute",
        "--goal-store", str(root / "goals"),
        "--run-store", str(root / "runs"),
        "--workspace-root", str(root / "workspaces"),
        "--campaign", str(campaign_path),
        "--source-repo", str(source),
        "--base-revision", base,
        "--max-cycles", "6",
        "--idle-limit", "2",
        "--executor-timeout-seconds", "120",
    ]
    if post_worker_fixture:
        scheduler_argv = _fixture_scheduler_argv(scheduler_argv)
    return subprocess.run(
        scheduler_argv,
        capture_output=True, text=True, timeout=600, env=env,
    )


def _durable_facts(root: Path) -> dict[str, Any]:
    goal = GoalStore(root / "goals").get_goal("campaign-fence:stage-1")
    runs = RunStore(root / "runs")
    run = runs.get_run(goal["run_id"]) if goal and goal.get("run_id") else {}
    receipt_meta = runs.latest_receipt(goal["run_id"]) if goal and goal.get("run_id") else None
    receipt: dict[str, Any] = {}
    provider: dict[str, Any] = {}
    diff_text = ""
    if receipt_meta:
        receipt = json.loads((root / "runs" / receipt_meta["receipt_ref"]).read_text(encoding="utf-8"))
        provider_ref = receipt.get("provider", {}).get("artifact", {}).get("ref")
        if isinstance(provider_ref, str):
            provider = json.loads((root / "runs" / provider_ref).read_text(encoding="utf-8"))
        diff_ref = receipt.get("diff", {}).get("ref")
        if isinstance(diff_ref, str):
            diff_text = (root / "runs" / diff_ref).read_text(encoding="utf-8")
    events = [
        json.loads(line)
        for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return {"goal": goal or {}, "run": run, "receipt": receipt,
            "provider": provider, "diff_text": diff_text, "events": events}


def _stub_report(provider: dict[str, Any]) -> dict[str, Any]:
    tail = provider.get("stdout_tail")
    if not isinstance(tail, str):
        return {}
    for line in tail.splitlines():
        if line.startswith("FENCE-STUB "):
            try:
                return json.loads(line[len("FENCE-STUB "):])
            except json.JSONDecodeError:
                return {}
    return {}


def main() -> int:
    cases: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory() as raw:
        enabled_root = Path(raw) / "enabled"
        enabled_root.mkdir()
        enabled = _tick(enabled_root, backend=BACKEND_ID, post_worker_fixture=True)
        facts = _durable_facts(enabled_root)
        fence = facts["receipt"].get("execution_fence", {})
        report = _stub_report(facts["provider"])
        proofs = fence.get("proofs", {})
        cases.append(_case(
            "enabled-production-chain-dispatches-through-real-backend",
            enabled.returncode == 0
            and facts["run"].get("state") in {"verified", "completed"}
            and facts["receipt"].get("verification", {}).get("exit_code") == 0
            and fence.get("status") == "admitted"
            and fence.get("backend", {}).get("backend_id") == BACKEND_ID,
            {"exit": enabled.returncode, "run_state": facts["run"].get("state"),
             "fence_status": fence.get("status"),
             "backend": fence.get("backend", {}).get("backend_id"),
             "stderr": enabled.stderr[-400:]},
        ))
        cases.append(_case(
            "stub-attests-the-descriptor-digest-only-bwrap-injects",
            isinstance(fence.get("launch_descriptor_digest"), str)
            and report.get("descriptor_digest_env") == fence.get("launch_descriptor_digest")
            and report.get("provider_control_channel") == "stdio",
            {"report": report,
             "descriptor_digest": fence.get("launch_descriptor_digest")},
        ))
        cases.append(_case(
            "fork-and-socket-are-denied-inside-the-fence",
            report.get("fork") == "denied:EPERM"
            and report.get("socket") == "denied:EPERM",
            {"fork": report.get("fork"), "socket": report.get("socket")},
        ))
        cases.append(_case(
            "both-proof-tracks-are-admissible-on-the-receipt",
            set(proofs) == {
                "filesystem_effect_containment",
                "provider_control_egress",
                "provider_sandbox",
            }
            and proofs["filesystem_effect_containment"].get("result") == "admissible"
            and proofs["provider_control_egress"].get("result") == "admissible"
            # A mutation launch runs under this kernel fence itself; the
            # provider_sandbox track honestly reports there is nothing hosted
            # for a composed sandbox to apply to (host-bline-provider-sandbox).
            and proofs["provider_sandbox"].get("result") == "not_applicable",
            {track: row.get("result") for track, row in proofs.items()},
        ))
        cases.append(_case(
            "workspace-write-landed-in-the-receipt-diff",
            "src/stub-effect.txt" in facts["diff_text"],
            {"diff_bytes": len(facts["diff_text"])},
        ))
        cases.append(_case(
            "scheduler-event-log-records-the-dispatch",
            any(
                row.get("outcome") == "tick_completed"
                and (row.get("runs_dispatched") or 0) >= 1
                for row in facts["events"]
            ),
            [{"outcome": row.get("outcome"), "runs_dispatched": row.get("runs_dispatched")}
             for row in facts["events"]],
        ))

        disabled_root = Path(raw) / "disabled"
        disabled_root.mkdir()
        disabled = _tick(disabled_root, backend=None)
        off_facts = _durable_facts(disabled_root)
        off_fence = off_facts["receipt"].get("execution_fence", {})
        cases.append(_case(
            "absent-backend-parks-the-run-before-any-child",
            disabled.returncode == 0
            and off_facts["run"].get("state") == "human_required"
            and off_fence.get("error_code") == "execution_fence_unavailable"
            and off_facts["provider"].get("provider_invocations") == 0
            and "FENCE-STUB" not in (off_facts["provider"].get("stdout_tail") or ""),
            {"exit": disabled.returncode, "run_state": off_facts["run"].get("state"),
             "fence": off_fence, "provider_invocations": off_facts["provider"].get("provider_invocations"),
             "stderr": disabled.stderr[-400:]},
        ))

    failures = [{"id": row["id"], "detail": row["detail"]} for row in cases if not row["ok"]]
    payload = {
        "schema": "lh-fence-activation-smoke/v1",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "cases": cases,
        "failures": failures,
        "provider_invocations": 0,
        "verification": {
            "authority": "docs/contracts/goal-lifecycle-v1.md#lh-preventive-execution-fence-003",
            "command": "python3 -B lh_runtime/fence_activation_smoke.py",
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Local provider live smoke: one bounded Goal through the ``local`` executor.

``--dry-run`` is the gate.  It needs no provider account and never contacts a
model:

- the Codex provider-home rules (``auth.json`` required, ``config.toml``
  bound only when present);
- ``--execute`` refuses without the explicit opt-in and creates nothing;
- on Linux with bubblewrap, a rehearsal drives the chain the live run uses --
  instance init, manual intent, ``goal_loop_run.run(executor="local")``, the
  provider sandbox, the verifier, the receipt -- with a stand-in provider in
  place of Codex.

``--execute`` runs that chain once with the real ``codex`` on PATH and the
caller's Codex login.  It needs ``LH_LOCAL_PROVIDER_LIVE=1``, Linux,
bubblewrap and libseccomp; the stage allows one attempt, so it spends at most
one model call.

Delivery checks run through the explicit non-kernel fixture runner
(``tests/p7_fence_fixture``): the compatibility executor path has no
production delivery command runner, so a verified run here proves the provider
sandbox, the verifier and the receipt -- not kernel-contained delivery checks.
The report says so in ``known_gaps_open``.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import execution_fence as fences  # noqa: E402
import execution_fence_linux as linux_fence  # noqa: E402

CHECK_ID = "lh-local-provider-live-smoke"
LIVE_OPT_IN = "LH_LOCAL_PROVIDER_LIVE"
AGENT = "codex"
CAMPAIGN_ID = "local-provider-live-smoke"
STAGE_ID = "hello"
TARGET_FILE = "src/hello.txt"
TARGET_TEXT = "hello from the lh local provider"
FEATURE_CONTRACT = (
    f"Create the file {TARGET_FILE} containing exactly one line: {TARGET_TEXT}. "
    "Do not create, modify or delete any other file, and do not run git commands."
)
VERIFIER_SOURCE = (
    "import pathlib, sys; "
    f"path = pathlib.Path({TARGET_FILE!r}); "
    f"sys.exit(0 if path.is_file() and path.read_text(encoding='utf-8').strip() == {TARGET_TEXT!r} else 1)"
)
EXPECTED_PROOFS = {
    "filesystem_effect_containment": "applied_by_provider_sandbox",
    "provider_control_egress": "host_network_policy_preflight",
    "provider_sandbox": "applied",
}
KNOWN_GAPS = [
    "delivery checks run through the explicit non-kernel fixture runner; "
    "the compatibility executor path has no production delivery command runner",
    "provider network egress is host network by design; only argv is preflighted against the policy",
    "kernel-contained local provider runs are Linux-only",
]
# Stand-in for Codex in the rehearsal: writes the target and reports usage in
# the shape ``codex exec --json`` emits.  Plain Python, so no node runtime.
STAND_IN_SOURCE = f"""#!/usr/bin/python3
import json, pathlib
path = pathlib.Path({TARGET_FILE!r})
path.parent.mkdir(exist_ok=True)
path.write_text({TARGET_TEXT!r} + "\\n", encoding="utf-8")
print(json.dumps({{"type": "turn.completed",
                  "usage": {{"input_tokens": 12, "cached_input_tokens": 0, "output_tokens": 5}}}}))
"""


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def _git(*argv: str, cwd: Path) -> str:
    return subprocess.run(["git", *argv], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


# -- provider-home rules (platform-neutral) ---------------------------------

def _raw_profile(codex_home: Path) -> dict[str, Any]:
    return {
        "bubblewrap": {"path": "/usr/bin/bwrap", "sha256": "sha256:" + "0" * 64, "version": "0.9.0"},
        "network": "host",
        "ro_binds": ["/usr"],
        "provider_home_ro_binds": {AGENT: [str(codex_home)]},
        "env": {},
        "flags": [],
        "seccomp": {"default": "allow",
                    "denied_syscalls": list(fences.PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS)},
    }


def _home_files(root: Path, names: tuple[str, ...]) -> tuple[list[dict[str, str]] | None, str | None]:
    home = root / ("home-" + "-".join(name.split(".")[0] for name in names))
    home.mkdir()
    for name in names:
        (home / name).write_text("{}\n" if name == "auth.json" else "", encoding="utf-8")
    try:
        profile = linux_fence.normalize_sandbox_profile(_raw_profile(home), AGENT)
    except fences.ExecutionFenceUnavailable as exc:
        return None, exc.reason
    return profile["provider_home_ro_files"], None


def provider_home_cases() -> list[dict[str, Any]]:
    transient = linux_fence.CODEX_TRANSIENT_HOME
    with tempfile.TemporaryDirectory(prefix="lh-live-smoke-home-") as raw:
        root = Path(raw)
        auth_only, auth_only_refusal = _home_files(root, ("auth.json",))
        config_only, config_only_refusal = _home_files(root, ("config.toml",))
        both, both_refusal = _home_files(root, ("auth.json", "config.toml"))
        both_home = root / "home-auth-config"
    return [
        case("codex-home-without-config-toml-is-accepted",
             auth_only_refusal is None and [item["target"] for item in auth_only or []]
             == [f"{transient}/auth.json"],
             {"files": auth_only, "refusal": auth_only_refusal}),
        case("codex-home-without-auth-json-is-refused",
             config_only is None and config_only_refusal == "egress_policy_codex_provider_file_missing:auth.json",
             {"files": config_only, "refusal": config_only_refusal}),
        case("config-toml-still-bound-when-present",
             both_refusal is None and both == [
                 {"source": str(both_home / "auth.json"), "target": f"{transient}/auth.json"},
                 {"source": str(both_home / "config.toml"), "target": f"{transient}/config.toml"},
             ],
             {"files": both, "refusal": both_refusal}),
    ]


def opt_in_case() -> dict[str, Any]:
    saved = os.environ.pop(LIVE_OPT_IN, None)
    try:
        with tempfile.TemporaryDirectory(prefix="lh-live-smoke-optin-") as raw:
            work_root = Path(raw) / "never-created"
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = main(["--execute", "--work-root", str(work_root)])
            report = json.loads(out.getvalue() or "{}")
            created = work_root.exists()
    finally:
        if saved is not None:
            os.environ[LIVE_OPT_IN] = saved
    ok = code == 2 and report.get("reason") == "live_opt_in_missing" and not created
    return case("execute-requires-explicit-opt-in", ok,
                {"exit": code, "reason": report.get("reason"), "work_root_created": created})


# -- the bounded run (Linux + bubblewrap) -----------------------------------

def _file_state(path: Path) -> list[Any]:
    stat = path.lstat()
    return [stat.st_size, stat.st_mtime_ns, stat.st_mode]


def _tree_snapshot(root: Path) -> dict[str, list[Any]]:
    if not root.exists():
        return {}
    snapshot = {".": _file_state(root)}
    for path in sorted(root.rglob("*")):
        snapshot[path.relative_to(root).as_posix()] = _file_state(path)
    return snapshot


def _top_level(home: Path) -> list[str]:
    return sorted(entry.name for entry in home.iterdir())


def _processes_running(marker: str) -> list[str]:
    found: list[str] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or entry.name == str(os.getpid()):
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if marker in command:
            found.append(f"{entry.name}:{command[:120]}")
    return found


def _source_repo(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir()
    _git("init", "-q", cwd=source)
    (source / "README.md").write_text("local provider live smoke\n", encoding="utf-8")
    _git("add", "-A", cwd=source)
    _git("-c", "user.name=lh-live-smoke", "-c", "user.email=lh-live-smoke@example.invalid",
         "commit", "-qm", "base", cwd=source)
    return source, _git("rev-parse", "HEAD", cwd=source)


def _campaign(source: Path, base: str) -> dict[str, Any]:
    from campaign_compiler import CampaignCompiler
    from native_delivery_fixture import make_native_bundle

    verifier = [sys.executable, "-B", "-c", VERIFIER_SOURCE]
    stage = {
        "stage_id": STAGE_ID,
        "goal": {"feature_contract": FEATURE_CONTRACT},
        "allowed_paths": ["src/"],
        "allowed_side_effects": ["workspace", "artifact"],
        "acceptance_lamp": {"id": f"{STAGE_ID}-lamp", "smoke": f"{TARGET_FILE} holds the expected line",
                            "verification_argv": verifier},
        "max_attempts": 1,
        "next_stage_id": None,
    }
    campaign = {"schema": "lh-campaign/v1", "campaign_id": CAMPAIGN_ID, "stages": [stage]}
    envelope = CampaignCompiler(campaign).compile()["stages"][STAGE_ID]
    bundle = make_native_bundle(
        source, base, f"{CAMPAIGN_ID}:{STAGE_ID}", STAGE_ID,
        [{"id": "diff-check",
          "commands": [{"id": "diff-check", "argv": ["git", "diff", "--cached", "--check"],
                        "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}],
          "required_receipts": ["executor"]}],
        verifier, ["src/"], 1,
        goal={"feature_contract": stage["goal"], "admission_envelope": envelope},
    )
    stage["goal"] = {**stage["goal"], "delivery_required": True,
                     "delivery_contract": bundle["contract"], "delivery_plan": bundle["plan"],
                     "delivery_packet": bundle["packet"]}
    return campaign


def write_stand_in(bin_dir: Path) -> Path:
    """Put a stand-in ``codex`` first on PATH (a ``.cmd`` shim where PATHEXT applies)."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        stand_in = bin_dir / f"{AGENT}.cmd"
        stand_in.write_text("@echo off\r\n", encoding="utf-8")
    else:
        stand_in = bin_dir / AGENT
        stand_in.write_text(STAND_IN_SOURCE, encoding="utf-8")
        stand_in.chmod(0o755)
    return stand_in.resolve()


def prepare_instance(root: Path, *, codex_home: Path,
                     path_prefix: str | None) -> tuple[Any, dict[str, str], dict[str, Any]]:
    """Initialize a run-local instance that declares Codex as its provider.

    Instance init pins only declared providers; an undeclared provider gets no
    policy entry and the fence refuses at prepare.  The rehearsal and the live
    run therefore declare Codex the same way -- ``LH_PROVIDER_NAMES`` and
    PATH -- and differ only in which ``codex`` PATH finds first.
    """
    import instance_config

    environ = dict(os.environ)
    environ.update({"CODEX_HOME": str(codex_home), "LH_PROVIDER_NAMES": AGENT,
                    "LH_STATE_ROOT": str(root / "state"), "LH_WORKSPACE_ROOT": str(root / "instance-workspaces"),
                    "LH_CACHE_ROOT": str(root / "cache"), "LH_LOGS_ROOT": str(root / "logs")})
    if path_prefix is not None:
        environ["PATH"] = path_prefix + os.pathsep + environ.get("PATH", "")
    instance = instance_config.initialize_instance(
        root / "instance" / "instance.json", system=platform.system(), environ=environ,
        home=Path.home(), cwd=root)
    policy = json.loads(instance.egress_policy_path.read_text(encoding="utf-8"))
    return instance, environ, (policy.get("providers") or {}).get(AGENT) or {}


def instance_pin_case() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="lh-live-smoke-pin-") as raw:
        root = Path(raw).resolve()
        stand_in = write_stand_in(root / "bin")
        codex_home = root / "codex-home"
        codex_home.mkdir()
        (codex_home / "auth.json").write_text("{}\n", encoding="utf-8")
        try:
            _instance, _environ, pin = prepare_instance(root, codex_home=codex_home, path_prefix=str(root / "bin"))
        except Exception as exc:
            return case("instance-declares-and-pins-codex", False, f"{type(exc).__name__}: {exc}")
        pinned = pin.get("path")
        ok = (isinstance(pinned, str) and Path(pinned).is_file() and Path(pinned).samefile(stand_in)
              and str(pin.get("sha256", "")).startswith("sha256:"))
        return case("instance-declares-and-pins-codex", ok, {"pinned": pinned, "expected": str(stand_in)})


def bounded_run(root: Path, *, codex_home: Path, path_prefix: str | None,
                timeout_seconds: float) -> dict[str, Any]:
    """Run one Goal through ``goal_loop_run.run(executor="local")`` and grade it."""
    import goal_loop_run
    from command_ingress import submit_command
    from goal_store import GoalStore
    from p7_native_runstore_fixture import explicit_runstore_factory
    from run_store import RunStore

    home = Path.home()
    codex_before = _tree_snapshot(codex_home)
    home_before = _top_level(home)
    source, base = _source_repo(root)
    campaign = _campaign(source, base)
    instance, environ, provider_pin = prepare_instance(root, codex_home=codex_home, path_prefix=path_prefix)

    goals = GoalStore(root / "goals")
    submitted = submit_command(goals, source="local-provider-live-smoke", event_type="manual_intent",
                               event_id="live-smoke-1", payload={"campaign_id": CAMPAIGN_ID, "stage_id": STAGE_ID})
    # The run resolves the provider through the same instance the policy was
    # generated from, so the launch re-hash compares like with like.
    overlay = {**instance.environment_overlay(environ), "CODEX_HOME": str(codex_home),
               "LH_EXECUTION_FENCE_BACKEND": fences.LINUX_BACKEND_ID,
               "LH_LOCAL_PROVIDER_AGENT": AGENT}
    saved = {name: os.environ.get(name) for name in overlay}
    try:
        os.environ.update(overlay)
        with explicit_runstore_factory(goal_loop_run):
            result = goal_loop_run.run(
                executor="local", execute=True,
                goal_store_root=root / "goals", run_store_root=root / "runs",
                workspace_root=root / "workspaces", campaign=campaign,
                source_repo=source, base_revision=base, holder="local-provider-live-smoke",
                max_cycles=3, idle_limit=1, executor_timeout_seconds=timeout_seconds,
                sleep_fn=lambda _seconds: None)
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    runs = RunStore(root / "runs")
    with runs._connect() as conn:
        attempts = [dict(row) for row in conn.execute(
            "SELECT run_id, ordinal, state, receipt_ref FROM attempts ORDER BY run_id, ordinal").fetchall()]
    receipt: dict[str, Any] = {}
    files_touched: list[str] = []
    if len(attempts) == 1 and attempts[0].get("receipt_ref"):
        receipt = json.loads((runs.root / attempts[0]["receipt_ref"]).read_text(encoding="utf-8"))
        diff = runs.root / "artifacts" / attempts[0]["run_id"] / str(attempts[0]["ordinal"]) / "diff.patch"
        if diff.is_file():
            files_touched = sorted(line.split()[3][2:] for line in diff.read_text(encoding="utf-8").splitlines()
                                   if line.startswith("diff --git ") and len(line.split()) >= 4)
    provider_failure = None
    if len(attempts) == 1:
        provider_record = runs.root / "artifacts" / attempts[0]["run_id"] / str(attempts[0]["ordinal"]) / "provider.json"
        if provider_record.is_file():
            provider_failure = json.loads(provider_record.read_text(encoding="utf-8")).get("failure")
    fence = receipt.get("execution_fence") or {}
    proofs = {track: (value or {}).get("result") for track, value in (fence.get("proofs") or {}).items()}
    usage = runs.usage_records()
    measured = [row for row in usage if row.get("state") == "measured"
                and int(row.get("input_tokens") or 0) + int(row.get("output_tokens") or 0) > 0]
    leftovers = _processes_running(str(root))
    checks = {
        "provider_pinned": bool(provider_pin.get("path")),
        "intent_received": submitted.get("status") == "received",
        "run_verified": [row["state"] for row in attempts] == ["verified"],
        "diff_only_in_target": files_touched == [TARGET_FILE],
        "source_repo_untouched": _git("rev-parse", "HEAD", cwd=source) == base
        and _git("status", "--porcelain", cwd=source) == "",
        "local_provider_proofs": proofs == EXPECTED_PROOFS and bool(fence.get("local_provider")),
        "usage_measured": len(measured) == 1,
        "codex_home_unchanged": _tree_snapshot(codex_home) == codex_before,
        "home_has_no_new_entries": set(_top_level(home)) <= set(home_before),
        "no_leftover_processes": not leftovers,
    }
    driver = result.get("driver") or {}
    return {
        "checks": checks,
        "evidence": {
            "provider": {"path": provider_pin.get("path"), "sha256": provider_pin.get("sha256")},
            "fence": {"status": fence.get("status"), "reason": fence.get("reason")},
            "provider_failure": str(provider_failure)[:400] if provider_failure else None,
            "profile_digest": ((fence.get("local_provider") or {}).get("provider_sandbox") or {}).get("profile_digest"),
            "proofs": proofs,
            "attempts": [{key: row[key] for key in ("ordinal", "state")} for row in attempts],
            "files_touched": files_touched,
            "usage": [{key: row.get(key) for key in ("state", "model", "input_tokens", "output_tokens")}
                      for row in usage],
            "driver": {key: driver.get(key) for key in ("stop_reason", "cycles", "runs_dispatched")},
            "leftover_processes": leftovers,
            "delivery_command_runner": "non-kernel-fixture",
        },
    }


def _linux_bubblewrap() -> str | None:
    if not sys.platform.startswith("linux"):
        return None
    return shutil.which("bwrap")


def _work_root(parent: Path | None) -> Path:
    # bubblewrap mounts a fresh tmpfs on /tmp, so the clone and the provider
    # must live outside /tmp to stay visible inside the sandbox.
    base = parent if parent is not None else Path.home() / ".cache" / "lh-local-provider-live"
    base.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="run-", dir=base)).resolve()


def rehearsal_case() -> dict[str, Any]:
    if _linux_bubblewrap() is None:
        return case("rehearsal-run-verifies-through-local-provider", False,
                    "local_provider_fence_requires_linux_bubblewrap")
    root = _work_root(None)
    try:
        bin_dir = root / "bin"
        write_stand_in(bin_dir)
        codex_home = root / "codex-home"
        codex_home.mkdir()
        # A default Codex login: auth.json and no config.toml.
        (codex_home / "auth.json").write_text("{}\n", encoding="utf-8")
        graded = bounded_run(root, codex_home=codex_home, path_prefix=str(bin_dir), timeout_seconds=120)
    except Exception as exc:  # A crash is a failed exam, never a skipped one.
        return case("rehearsal-run-verifies-through-local-provider", False, f"{type(exc).__name__}: {exc}")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return case("rehearsal-run-verifies-through-local-provider", all(graded["checks"].values()), graded)


def dry_run() -> int:
    cases = [*provider_home_cases(), opt_in_case(), instance_pin_case(), rehearsal_case()]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "mode": "dry-run",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "results": [{"id": item["id"], "passed": item["ok"]} for item in cases],
        "blocking_failures": failures,
        "known_gaps_open": KNOWN_GAPS,
    }, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


def _refuse(reason: str, **detail: Any) -> int:
    print(json.dumps({"check_id": CHECK_ID, "mode": "execute", "status": "refused",
                      "reason": reason, **detail}, ensure_ascii=False, indent=2))
    return 2


def execute(work_root: Path | None, keep_root: bool) -> int:
    if os.environ.get(LIVE_OPT_IN) != "1":
        return _refuse("live_opt_in_missing", next=f"set {LIVE_OPT_IN}=1 to spend one model call")
    if _linux_bubblewrap() is None:
        return _refuse("local_provider_fence_requires_linux_bubblewrap")
    codex = shutil.which(AGENT)
    if codex is None:
        return _refuse("codex_not_on_path")
    configured = os.environ.get("CODEX_HOME", "").strip()
    codex_home = Path(configured).expanduser() if configured else Path.home() / ".codex"
    if not (codex_home / "auth.json").is_file():
        return _refuse("codex_login_missing", codex_home=str(codex_home))
    root = _work_root(work_root)
    try:
        graded = bounded_run(root, codex_home=codex_home.resolve(), path_prefix=None, timeout_seconds=600)
    except Exception as exc:
        graded = {"checks": {"completed_without_error": False}, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        if not keep_root:
            shutil.rmtree(root, ignore_errors=True)
    ok = all(graded["checks"].values())
    print(json.dumps({
        "check_id": CHECK_ID,
        "mode": "execute",
        "status": "pass" if ok else "fail",
        "blocking_failures": [name for name, passed in graded["checks"].items() if not passed],
        **graded,
        "work_root": str(root) if keep_root else None,
        "known_gaps_open": KNOWN_GAPS,
    }, ensure_ascii=False, indent=2))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local provider live smoke (dry-run gate or one live run)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="gate mode; no provider account, no model call")
    mode.add_argument("--execute", action="store_true",
                      help=f"one real Codex call; requires {LIVE_OPT_IN}=1, Linux and bubblewrap")
    parser.add_argument("--work-root", default=None, help="parent directory for the run (outside /tmp)")
    parser.add_argument("--keep-root", action="store_true", help="keep the run directory for inspection")
    args = parser.parse_args(argv)
    if not args.execute:
        return dry_run()
    return execute(Path(args.work_root).expanduser().resolve() if args.work_root else None, args.keep_root)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Onboarding: bring a target repository under the loop in three steps.

- ``init <target>`` writes a contract template (``project_runtime_contract.json``,
  with its ``executors`` declaration) and an acceptance lamp template
  (``checks/acceptance.py``).  An existing file is never overwritten.
- ``validate <target>`` reports shape errors and drift in the contract, such as
  a verifier inside ``allowed_paths`` or an executor that is not an absolute
  path.  It reads files and git objects only; nothing from the target runs.
- ``pilot <target> --executors <file> --executor <name>`` clones the target into
  a scratch directory and drives one full run with the declared stand-in
  executor, through the real entry points, until the run is verified.  The
  target repository is never changed: its HEAD and working tree are compared
  before and after.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cli_agent_executor as executors  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from project_binding import CONTRACT_SCHEMA, resolve_project  # noqa: E402
from run_store import RunStore  # noqa: E402

INIT_SCHEMA = "lh-onboarding-init/v1"
VALIDATE_SCHEMA = "lh-onboarding-validate/v1"
PILOT_SCHEMA = "lh-onboarding-pilot/v1"
CONTRACT_NAME = "project_runtime_contract.json"
LAMP_PATH = "checks/acceptance.py"
REQUIRED_FIELDS = ("project_id", "campaign", "source_repo", "base_revision", "runtime")
PLACEHOLDER_EXECUTOR = "/absolute/path/to/your-coding-agent"
LAMP_TEMPLATE = '''#!/usr/bin/env python3
"""Acceptance lamp: exit 0 only when this stage's goal is met.

Replace the check below with the real one.  It runs from the root of a
disposable clone of this repository, and it must stay outside the stage's
allowed_paths so that the executor cannot change its own exam.
"""
from pathlib import Path

raise SystemExit(0 if Path("src/ACCEPTED").is_file() else 1)
'''


def _git(target: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(target), *args], capture_output=True, text=True,
                          stdin=subprocess.DEVNULL)


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "project"


def contract_template(target: Path, project_id: str | None = None) -> dict[str, Any]:
    project = _slug(project_id or target.resolve().name)
    branch = _git(target, "symbolic-ref", "--short", "HEAD")
    return {
        "schema": CONTRACT_SCHEMA,
        "project_id": project,
        "campaign": {
            "schema": "lh-campaign/v1",
            "campaign_id": f"{project}-campaign",
            "stages": [{
                "stage_id": "feature",
                "goal": {"must_have": ["the change described for this stage, with its tests"],
                         "must_not": ["push", "publish", "merge"]},
                "allowed_paths": ["src/", "tests/"],
                "allowed_side_effects": ["workspace", "artifact"],
                "acceptance_lamp": {"id": "acceptance", "smoke": f"python {LAMP_PATH}",
                                    "verification_argv": [sys.executable, LAMP_PATH]},
                "delivery": {"derive": "acceptance_lamp",
                             "checks": [{"id": "diff-hygiene", "argv": ["git", "diff", "--cached", "--check"]}]},
                "max_attempts": 4,
                "next_stage_id": None,
            }],
        },
        "source_repo": ".",
        "base_revision": branch.stdout.strip() if branch.returncode == 0 and branch.stdout.strip() else "main",
        "runtime": {"goal_store": "runtime/goals", "run_store": "runtime/runs",
                    "workspace_root": "runtime/workspaces", "status_snapshot_out": "runtime/platform_status.json",
                    "pause_flag": "runtime/loop-pause"},
        "executors": {"coder": {"argv": [PLACEHOLDER_EXECUTOR, "{prompt}"], "usage": "none"}},
        "models": {"execute": "coder"},
    }


def init(target: Path, *, project_id: str | None = None) -> dict[str, Any]:
    target = Path(target).resolve()
    if not (target / ".git").exists():
        raise ValueError("onboarding_target_not_a_git_repository")
    files = {
        target / CONTRACT_NAME: json.dumps(contract_template(target, project_id), indent=2, ensure_ascii=False) + "\n",
        target / LAMP_PATH: LAMP_TEMPLATE,
    }
    written, kept = [], []
    for path, text in files.items():
        if path.exists() or path.is_symlink():
            kept.append(str(path))
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        written.append(str(path))
    return {"schema": INIT_SCHEMA, "target": str(target), "written": written, "kept": kept}


def _finding(code: str, where: str, detail: str) -> dict[str, str]:
    return {"code": code, "where": where, "detail": detail}


def _under(relative: str, prefix: str) -> bool:
    clean = prefix.strip().strip("/")
    return bool(clean) and (relative == clean or relative.startswith(clean + "/"))


def _verifier_files(argv: list[Any]) -> list[str]:
    """Relative file arguments of a verification command: the files the lamp runs from the repository."""
    files = []
    for item in argv:
        if not isinstance(item, str) or item.startswith("-") or Path(item).is_absolute():
            continue
        if "/" in item or item.endswith((".py", ".sh")):
            files.append(Path(item).as_posix())
    return files


def _shape_findings(target: Path, contract: Any) -> list[dict[str, str]]:
    if not isinstance(contract, dict):
        return [_finding("contract_unreadable", "contract", "the contract is not a JSON object")]
    findings = []
    if contract.get("schema") != CONTRACT_SCHEMA:
        findings.append(_finding("contract_schema_invalid", "schema", f"want {CONTRACT_SCHEMA}"))
    for field in REQUIRED_FIELDS:
        if not contract.get(field):
            findings.append(_finding("contract_field_missing", field, "required"))
    declared = contract.get("executors") if isinstance(contract.get("executors"), dict) else {}
    if not declared:
        findings.append(_finding("executor_undeclared", "executors", "declare at least one executor"))
    for name, declaration in declared.items():
        argv = declaration.get("argv") if isinstance(declaration, dict) else None
        if not isinstance(argv, list) or not argv or not all(isinstance(item, str) and item for item in argv):
            findings.append(_finding("executor_declaration_invalid", f"executors.{name}", "argv must be a list"))
            continue
        if not Path(argv[0]).is_absolute():
            findings.append(_finding("executor_not_absolute", f"executors.{name}.argv[0]", argv[0]))
        elif not Path(argv[0]).is_file():
            findings.append(_finding("executor_missing", f"executors.{name}.argv[0]", argv[0]))
        if argv.count(executors.PROMPT_SLOT) != 1:
            findings.append(_finding("executor_prompt_slot_missing", f"executors.{name}.argv",
                                     f"carry {executors.PROMPT_SLOT} exactly once"))
    models = contract.get("models")
    execute = models.get("execute") if isinstance(models, dict) else None
    if not isinstance(execute, str) or execute not in declared:
        findings.append(_finding("model_executor_undeclared", "models.execute", repr(execute)))
    base = contract.get("base_revision")
    if isinstance(base, str) and base and _git(target, "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}").returncode:
        findings.append(_finding("base_revision_unknown", "base_revision", base))
    stages = (contract.get("campaign") or {}).get("stages") if isinstance(contract.get("campaign"), dict) else None
    for index, stage in enumerate(stages if isinstance(stages, list) else []):
        where = f"campaign.stages[{index}]"
        if not isinstance(stage, dict):
            continue
        allowed = [item for item in stage.get("allowed_paths") or [] if isinstance(item, str)]
        lamp = stage.get("acceptance_lamp") if isinstance(stage.get("acceptance_lamp"), dict) else {}
        argv = lamp.get("verification_argv") if isinstance(lamp.get("verification_argv"), list) else []
        if not argv:
            findings.append(_finding("acceptance_lamp_missing", where, "acceptance_lamp.verification_argv"))
        for relative in _verifier_files(argv):
            if any(_under(relative, prefix) for prefix in allowed):
                findings.append(_finding("verifier_in_allowed_paths", f"{where}.acceptance_lamp", relative))
            if not (target / relative).is_file():
                findings.append(_finding("verifier_missing", f"{where}.acceptance_lamp", relative))
            elif _git(target, "cat-file", "-e", f"HEAD:{relative}").returncode:
                findings.append(_finding("verifier_not_committed", f"{where}.acceptance_lamp", relative))
    return findings


def validate(target: Path, *, contract_path: Path | None = None,
             contract: dict[str, Any] | None = None) -> dict[str, Any]:
    target = Path(target).resolve()
    path = Path(contract_path).resolve() if contract_path is not None else target / CONTRACT_NAME
    if contract is None:
        try:
            contract = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return {"schema": VALIDATE_SCHEMA, "ok": False, "target": str(target), "contract": str(path),
                    "findings": [_finding("contract_unreadable", str(path), str(exc)[:300])]}
    findings = _shape_findings(target, contract)
    if not findings:
        # Shape is sound: let the engine's own resolver bind it, exactly as a run would.
        with tempfile.TemporaryDirectory(prefix="lh-onboarding-validate-") as raw:
            probe = Path(raw) / CONTRACT_NAME
            bound = dict(contract)
            source = Path(str(contract["source_repo"]))
            bound["source_repo"] = str(source if source.is_absolute() else (path.parent / source).resolve())
            probe.write_text(json.dumps(bound), encoding="utf-8")
            try:
                resolve_project(probe)
            except (SystemExit, ValueError) as exc:
                findings.append(_finding("contract_unresolvable", "contract", str(exc)[:300]))
    return {"schema": VALIDATE_SCHEMA, "ok": not findings, "target": str(target), "contract": str(path),
            "findings": findings}


def _target_state(target: Path) -> dict[str, str]:
    return {"head": _git(target, "rev-parse", "HEAD").stdout.strip(),
            "status": _git(target, "status", "--porcelain", "--untracked-files=all").stdout}


def _goal_for_run(goals: GoalStore, run_id: str) -> dict[str, Any] | None:
    for state in goals.summary().get("goals_by_state", {}):
        for goal in goals.goals_in_state(state):
            if goal.get("run_id") == run_id:
                return goal
    return None


def pilot(target: Path, *, executors_file: Path, executor: str, stage_id: str | None = None,
          work_root: Path | None = None, max_cycles: int = 8) -> dict[str, Any]:
    target = Path(target).resolve()
    result: dict[str, Any] = {"schema": PILOT_SCHEMA, "target": str(target), "verified": False, "run_id": None}
    try:
        contract = json.loads((target / CONTRACT_NAME).read_text(encoding="utf-8"))
        stand_in = executors.validate_executor_declarations(json.loads(Path(executors_file).read_text(encoding="utf-8")))
    except (OSError, ValueError) as exc:
        return {**result, "refused": True, "findings": [_finding("pilot_input_invalid", "pilot", str(exc)[:300])]}
    if executor not in stand_in:
        return {**result, "refused": True,
                "findings": [_finding("model_executor_undeclared", "--executor", f"{executor!r} is not in {executors_file}")]}
    effective = {**contract, "executors": stand_in, "models": {"execute": executor}}
    checked = validate(target, contract=effective)
    if not checked["ok"]:
        return {**result, "refused": True, "findings": checked["findings"]}
    stages = contract["campaign"]["stages"]
    stage = next((item for item in stages if item.get("stage_id") == stage_id), None) if stage_id else stages[0]
    if stage is None:
        return {**result, "refused": True, "findings": [_finding("stage_unknown", "--stage", str(stage_id))]}
    before = _target_state(target)
    work = Path(work_root).resolve() if work_root is not None else Path(tempfile.mkdtemp(prefix="lh-onboarding-pilot-"))
    work.mkdir(parents=True, exist_ok=True)
    if any(work.iterdir()):
        return {**result, "refused": True, "findings": [_finding("work_root_not_empty", str(work), "use an empty directory")]}
    source = work / "source"
    clone = subprocess.run(["git", "clone", "--quiet", "--no-local", str(target), str(source)],
                           capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if clone.returncode:
        return {**result, "work_root": str(work), "findings": [_finding("pilot_clone_failed", str(target), clone.stderr[-300:])]}
    base = _git(target, "rev-parse", f"{contract['base_revision']}^{{commit}}").stdout.strip()
    pilot_contract = {**effective, "source_repo": str(source), "base_revision": base,
                      "runtime": {"goal_store": "runtime/goals", "run_store": "runtime/runs",
                                  "workspace_root": "runtime/workspaces",
                                  "status_snapshot_out": "runtime/platform_status.json"}}
    contract_path = work / CONTRACT_NAME
    contract_path.write_text(json.dumps(pilot_contract, indent=2, ensure_ascii=False), encoding="utf-8")
    env = dict(os.environ)
    # The pilot is a local trial: without a named backend it uses the local-process fence,
    # whose receipt states plainly that nothing was isolated.
    env.setdefault("LH_EXECUTION_FENCE_BACKEND", "local-process")
    campaign_id = contract["campaign"]["campaign_id"]
    ingress = subprocess.run(
        [sys.executable, "-B", str(HERE / "command_ingress.py"), "--goal-store", str(work / "runtime" / "goals"),
         "--source", "onboarding-pilot", "--event-type", "manual_intent", "--event-id", f"pilot-{stage['stage_id']}",
         "--payload", json.dumps({"campaign_id": campaign_id, "stage_id": stage["stage_id"], "intent": "onboarding pilot"})],
        capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL)
    run = subprocess.run(
        [sys.executable, "-B", str(HERE / "goal_loop_run.py"), "--contract", str(contract_path), "--execute",
         "--max-cycles", str(max_cycles), "--idle-limit", "2", "--executor-timeout-seconds", "300"],
        capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL)
    try:
        driver = json.loads(run.stdout).get("driver") or {}
    except ValueError:
        driver = {}
    runs, goals = RunStore(work / "runtime" / "runs"), GoalStore(work / "runtime" / "goals")
    terminal = runs.terminal_runs() or runs.runnable_runs()
    run_row = runs.get_run(terminal[0]["run_id"]) if terminal else {}
    run_id = run_row.get("run_id")
    receipt_meta = runs.latest_receipt(run_id) if run_id else None
    goal = _goal_for_run(goals, run_id) if run_id else None
    after = _target_state(target)
    verified = run_row.get("state") == "verified" and (goal or {}).get("state") == "completed"
    return {**result, "verified": verified and before == after, "run_id": run_id, "run_state": run_row.get("state"),
            "goal_state": (goal or {}).get("state"),
            "receipt": str(runs.root / receipt_meta["receipt_ref"]) if receipt_meta else None,
            "stop_reason": driver.get("stop_reason"), "target_unchanged": before == after,
            "fence_backend": env["LH_EXECUTION_FENCE_BACKEND"], "work_root": str(work),
            "ingress_exit": ingress.returncode, "run_exit": run.returncode,
            "errors": [text[-500:] for text in (ingress.stderr, run.stderr) if text.strip()]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bring a target repository under the loop: init, validate, pilot")
    commands = parser.add_subparsers(dest="command", required=True)
    p_init = commands.add_parser("init", help="write the contract and acceptance lamp templates")
    p_init.add_argument("target")
    p_init.add_argument("--project-id", default=None)
    p_validate = commands.add_parser("validate", help="report shape errors and drift; runs nothing from the target")
    p_validate.add_argument("target")
    p_validate.add_argument("--contract", default=None)
    p_pilot = commands.add_parser("pilot", help="one full run in a scratch clone with a stand-in executor")
    p_pilot.add_argument("target")
    p_pilot.add_argument("--executors", required=True, help="declaration file of the stand-in executor")
    p_pilot.add_argument("--executor", required=True, help="the declared stand-in executor's name")
    p_pilot.add_argument("--stage", default=None)
    p_pilot.add_argument("--work-root", default=None)
    p_pilot.add_argument("--max-cycles", type=int, default=8)
    args = parser.parse_args(argv)
    if args.command == "init":
        try:
            result = init(Path(args.target), project_id=args.project_id)
        except ValueError as exc:
            print(json.dumps({"schema": INIT_SCHEMA, "error": str(exc)}))
            return 2
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    if args.command == "validate":
        result = validate(Path(args.target), contract_path=Path(args.contract) if args.contract else None)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["ok"] else 1
    result = pilot(Path(args.target), executors_file=Path(args.executors), executor=args.executor,
                   stage_id=args.stage, work_root=Path(args.work_root) if args.work_root else None,
                   max_cycles=args.max_cycles)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

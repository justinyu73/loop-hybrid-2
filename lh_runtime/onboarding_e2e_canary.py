#!/usr/bin/env python3
"""Onboarding, end to end, as a clean user: init -> validate -> pilot -> read the receipt.

Every step runs the real command line in a clean environment: a fresh HOME,
PATH holding only the Python and git directories, and no git system or user
configuration.  The target repository must come out of the pilot exactly as it
went in, and a contract written wrong must be caught by ``validate`` without
running anything from the target.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
TOOL = HERE / "onboarding.py"
CHECK_ID = "lh-onboarding-e2e"
STUB = """from pathlib import Path
Path("src").mkdir(exist_ok=True)
# bytes, not text: on Windows text mode writes CRLF, which the template's `git diff --check` rightly rejects
Path("src/ACCEPTED").write_bytes(b"accepted by the stand-in executor\\n")
print("stand-in executor wrote src/ACCEPTED")
"""
SIDE_EFFECT = """from pathlib import Path
Path(__file__).resolve().parent.parent.joinpath("LAMP-WAS-RUN").write_text("ran\\n", encoding="utf-8")
raise SystemExit(0)
"""


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def clean_env(root: Path) -> dict[str, str]:
    home, tmp = root / "home", root / "tmp"
    home.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required")
    env = {
        "PATH": os.pathsep.join(dict.fromkeys([str(Path(sys.executable).parent), str(Path(git).parent)])),
        "HOME": str(home), "USERPROFILE": str(home), "XDG_CONFIG_HOME": str(home / ".config"),
        "TMPDIR": str(tmp), "TEMP": str(tmp), "TMP": str(tmp),
        "GIT_CONFIG_NOSYSTEM": "1", "PYTHONUTF8": "1",
    }
    if os.name == "nt":
        env["SYSTEMROOT"] = os.environ.get("SYSTEMROOT", r"C:\Windows")
    return env


def git(*args: str, cwd: Path, env: dict[str, str]) -> str:
    return subprocess.run(["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL).stdout.strip()


def tool(env: dict[str, str], *args: str) -> tuple[int, dict[str, Any]]:
    if not TOOL.is_file():
        raise FileNotFoundError("lh_runtime/onboarding.py is not provided")
    done = subprocess.run([sys.executable, "-B", str(TOOL), *args], env=env, capture_output=True, text=True,
                          encoding="utf-8", timeout=600, stdin=subprocess.DEVNULL)
    try:
        return done.returncode, json.loads(done.stdout)
    except ValueError:
        return done.returncode, {"unparsed": done.stdout[-500:], "stderr": done.stderr[-800:]}


def make_target(root: Path, env: dict[str, str]) -> Path:
    target = root / "target-project"
    target.mkdir(parents=True)
    git("init", "-q", "-b", "main", cwd=target, env=env)
    git("config", "user.email", "owner@example.invalid", cwd=target, env=env)
    git("config", "user.name", "Target Owner", cwd=target, env=env)
    (target / "src").mkdir()
    (target / "src" / "app.txt").write_text("the target project\n", encoding="utf-8")
    git("add", "-A", cwd=target, env=env)
    git("commit", "-qm", "target baseline", cwd=target, env=env)
    return target


def stand_in(root: Path) -> Path:
    script = root / "stand-in-executor.py"
    script.write_text(STUB, encoding="utf-8")
    declarations = root / "stand-in-executors.json"
    declarations.write_text(json.dumps({"stand-in": {"argv": [sys.executable, "-B", str(script), "{prompt}"],
                                                     "usage": "none"}}), encoding="utf-8")
    return declarations


def codes(result: dict[str, Any]) -> list[str]:
    return sorted({row.get("code") for row in result.get("findings", []) if isinstance(row, dict)})


def state(target: Path, env: dict[str, str]) -> dict[str, str]:
    return {"head": git("rev-parse", "HEAD", cwd=target, env=env),
            "status": git("status", "--porcelain", "--untracked-files=all", cwd=target, env=env)}


def journey(root: Path) -> list[dict[str, Any]]:
    env = clean_env(root)
    target = make_target(root, env)
    rows: list[dict[str, Any]] = []

    code, first = tool(env, "init", str(target))
    contract = target / "project_runtime_contract.json"
    lamp = target / "checks" / "acceptance.py"
    lamp_text = lamp.read_text(encoding="utf-8") if lamp.is_file() else ""
    lamp.write_text(lamp_text + "# edited by the owner\n", encoding="utf-8") if lamp.is_file() else None
    code2, second = tool(env, "init", str(target))
    rows.append(case("init-writes-templates-and-never-overwrites",
                     code == 0 and contract.is_file() and lamp.is_file()
                     and sorted(Path(p).name for p in first.get("written", [])) == ["acceptance.py", "project_runtime_contract.json"]
                     and code2 == 0 and not second.get("written") and len(second.get("kept", [])) == 2
                     and lamp.read_text(encoding="utf-8").endswith("# edited by the owner\n"),
                     {"first": first, "second": second}))

    code, uncommitted = tool(env, "validate", str(target))
    found = codes(uncommitted)
    rows.append(case("validate-reports-the-template-gaps",
                     code == 1 and uncommitted.get("ok") is False and "verifier_not_committed" in found
                     and any(item.startswith("executor_") for item in found)
                     and set(found) <= {"verifier_not_committed", "executor_not_absolute", "executor_missing"},
                     {"findings": uncommitted.get("findings")}))

    git("add", "-A", cwd=target, env=env)
    git("commit", "-qm", "adopt the loop", cwd=target, env=env)
    before = state(target, env)
    declarations = stand_in(root)
    code, pilot = tool(env, "pilot", str(target), "--executors", str(declarations), "--executor", "stand-in",
                       "--work-root", str(root / "pilot"))
    receipt: dict[str, Any] = {}
    receipt_path = Path(str(pilot.get("receipt") or ""))
    if pilot.get("receipt") and receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    rows.append(case("pilot-runs-to-verified-and-the-receipt-reads-back",
                     code == 0 and pilot.get("verified") is True and pilot.get("run_state") == "verified"
                     and pilot.get("goal_state") == "completed" and receipt.get("run_id") == pilot.get("run_id")
                     and (receipt.get("verification") or {}).get("exit_code") == 0,
                     {"pilot": pilot, "receipt_run": receipt.get("run_id"),
                      "verification": receipt.get("verification", {}).get("exit_code") if receipt else None}))
    after = state(target, env)
    rows.append(case("the-target-repository-is-unchanged",
                     before == after and pilot.get("target_unchanged") is True and not (target / "runtime").exists(),
                     {"before": before, "after": after}))
    return rows


def broken(root: Path) -> list[dict[str, Any]]:
    env = clean_env(root)
    target = make_target(root, env)
    tool(env, "init", str(target))
    (target / "checks" / "acceptance.py").write_text(SIDE_EFFECT, encoding="utf-8")
    git("add", "-A", cwd=target, env=env)
    git("commit", "-qm", "adopt the loop", cwd=target, env=env)
    contract_path = target / "project_runtime_contract.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    stage = contract["campaign"]["stages"][0]
    stage["allowed_paths"] = ["src/", "checks/"]           # the executor could rewrite its own verifier
    contract["executors"] = {"coder": {"argv": ["python", "agent.py", "{prompt}"], "usage": "none"}}  # not absolute
    contract["models"] = {"execute": "missing-agent"}      # names nothing declared
    contract_path.write_text(json.dumps(contract, indent=2), encoding="utf-8")
    code, result = tool(env, "validate", str(target))
    found = codes(result)
    expected = {"verifier_in_allowed_paths", "executor_not_absolute", "model_executor_undeclared"}
    declarations = stand_in(root)
    pcode, pilot = tool(env, "pilot", str(target), "--executors", str(declarations), "--executor", "stand-in",
                        "--work-root", str(root / "pilot"))
    return [
        case("validate-catches-a-contract-written-wrong",
             code == 1 and expected <= set(found) and not (target / "LAMP-WAS-RUN").exists(),
             {"findings": result.get("findings"), "lamp_ran": (target / "LAMP-WAS-RUN").exists()}),
        case("pilot-refuses-an-invalid-contract",
             pcode != 0 and pilot.get("verified") is False and "verifier_in_allowed_paths" in codes(pilot)
             and not pilot.get("run_id") and not (target / "LAMP-WAS-RUN").exists(),
             {"pilot": pilot}),
    ]


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-onboarding-") as raw:
        root = Path(raw).resolve()
        results: list[dict[str, Any]] = []
        groups: list[tuple[list[str], Callable[[], list[dict[str, Any]]]]] = [
            (["init-writes-templates-and-never-overwrites", "validate-reports-the-template-gaps",
              "pilot-runs-to-verified-and-the-receipt-reads-back", "the-target-repository-is-unchanged"],
             lambda: journey(root / "journey")),
            (["validate-catches-a-contract-written-wrong", "pilot-refuses-an-invalid-contract"],
             lambda: broken(root / "broken")),
        ]
        for names, action in groups:
            try:
                results.extend(action())
            except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
                detail = f"{type(exc).__name__}: {str(exc)[:300]}"
                done = {row["id"] for row in results}
                results.extend(case(name, False, detail) for name in names if name not in done)
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

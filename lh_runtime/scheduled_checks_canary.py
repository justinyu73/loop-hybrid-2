#!/usr/bin/env python3
"""Scheduled checks: an integrity check that only CI runs is not watching this machine.

CI grades committed bytes; it cannot say whether the seals on the machine that
runs the loop are intact right now.  ``scheduled_checks.run`` re-runs a closed
table of read-only checks against the target (the contract seal, the decision
registry) no more often than an interval, and leaves a verdict in the state
root.  The status snapshot carries that verdict and the one lamp lights
``integrity_check_red`` when it is red, missing or older than its maximum age
-- unknown is not healthy.  When the checks are not enabled the rule does not
apply and nothing changes.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "tests"))
from _fixture import make_campaign  # noqa: E402
from goal_store import GoalStore  # noqa: E402
import goal_loop_run as fixture_glr  # noqa: E402
from p7_native_runstore_fixture import explicit_runstore_factory  # noqa: E402
from run_store import RunStore  # noqa: E402
import status_lamp  # noqa: E402
import status_snapshot  # noqa: E402

CHECK_ID = "lh-scheduled-checks"
SEAL_TOOL = ROOT / "gate-pack" / "contract_seal" / "seal.py"
RULE = "integrity_check_red"
CONFIG = {"min_interval_seconds": 600.0, "max_age_seconds": 3600.0}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def checks():
    import scheduled_checks
    return scheduled_checks


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True).stdout.strip()


def sealed_target(root: Path) -> Path:
    target = root / "target"
    (target / "docs" / "contracts").mkdir(parents=True)
    git(target, "init", "-q")
    git(target, "config", "user.email", "checks@example.invalid")
    git(target, "config", "user.name", "Checks Canary")
    (target / "docs" / "contracts" / "alpha-v1.md").write_text("# alpha\n", encoding="utf-8")
    subprocess.run([sys.executable, "-B", str(SEAL_TOOL), "reseal", "--root", str(target), "--sealed-by", "canary",
                    "--reason", "fixture"], check=True, capture_output=True)
    git(target, "add", "-A")
    git(target, "commit", "-qm", "sealed baseline")
    return target


def state_path(root: Path) -> Path:
    return root / "goals" / "scheduled-checks.json"


def fired(root: Path, *, config: dict[str, Any] | None) -> tuple[list[str], Any]:
    snapshot = status_snapshot.build_snapshot(RunStore(root / "runs"), GoalStore(root / "goals"), generated_at="t",
                                              scheduled_checks=config)
    without = {key: value for key, value in snapshot.items() if key != "lamp"}
    result = status_lamp.lamp(without)
    return [row["id"] for row in result["fired"]], snapshot.get("scheduled_checks")


def c1_green(root: Path) -> dict[str, Any]:
    target = sealed_target(root)
    verdict = checks().run(target, state_path(root), now=time.time(), **CONFIG)
    rules, projection = fired(root, config=CONFIG)
    ok = (verdict.get("schema") == "lh-scheduled-checks/v1" and verdict.get("verdict") == "green"
          and [row["check"] for row in verdict.get("results", [])] == ["contract_seal"]
          and RULE not in rules and RULE in status_lamp.lamp({})["evaluated"])
    return case("intact-checks-leave-a-green-verdict-and-no-lamp", ok, {"verdict": verdict, "fired": rules,
                                                                        "projection": projection})


def c2_broken(root: Path) -> dict[str, Any]:
    target = sealed_target(root)
    (target / "docs" / "contracts" / "alpha-v1.md").write_text("# alpha, quietly rewritten\n", encoding="utf-8")
    verdict = checks().run(target, state_path(root), now=time.time(), **CONFIG)
    rules, _projection = fired(root, config=CONFIG)
    ok = verdict.get("verdict") == "red" and RULE in rules
    return case("a-broken-seal-lights-integrity-check-red", ok, {"verdict": verdict, "fired": rules})


def c3_stale_or_missing(root: Path) -> dict[str, Any]:
    target = sealed_target(root)
    checks().run(target, state_path(root), now=time.time(), **CONFIG)  # an old green verdict
    data = json.loads(state_path(root).read_text(encoding="utf-8"))
    data["checked_at"] = data["checked_at"] - 10 * CONFIG["max_age_seconds"]
    state_path(root).write_text(json.dumps(data), encoding="utf-8")
    stale_rules, stale = fired(root, config=CONFIG)
    state_path(root).unlink()
    missing_rules, missing = fired(root, config=CONFIG)
    state_path(root).write_text("{not json", encoding="utf-8")
    broken_rules, broken = fired(root, config=CONFIG)
    ok = RULE in stale_rules and RULE in missing_rules and RULE in broken_rules
    return case("a-stale-missing-or-unreadable-verdict-lights-the-lamp", ok,
                {"stale": stale, "missing": missing, "unreadable": broken})


def c4_disabled(root: Path) -> dict[str, Any]:
    rules, projection = fired(root, config=None)
    ok = RULE not in rules and projection is None
    return case("not-enabled-means-the-rule-does-not-apply", ok, {"fired": rules, "projection": projection})


def c5_read_only(root: Path) -> dict[str, Any]:
    target = sealed_target(root)
    head, status = git(target, "rev-parse", "HEAD"), git(target, "status", "--porcelain")
    files = sorted(p.relative_to(target).as_posix() for p in target.rglob("*") if ".git" not in p.parts)
    checks().run(target, state_path(root), now=1000.0, **CONFIG)
    after = sorted(p.relative_to(target).as_posix() for p in target.rglob("*") if ".git" not in p.parts)
    ok = (git(target, "rev-parse", "HEAD") == head and git(target, "status", "--porcelain") == status
          and after == files and state_path(root).is_file())
    return case("the-checks-never-write-to-the-target", ok, {"before": files, "after": after})


def c6_interval(root: Path) -> dict[str, Any]:
    target = sealed_target(root)
    first = checks().run(target, state_path(root), now=1000.0, **CONFIG)
    (target / "docs" / "contracts" / "alpha-v1.md").write_text("# changed between passes\n", encoding="utf-8")
    soon = checks().run(target, state_path(root), now=1000.0 + 60, **CONFIG)
    later = checks().run(target, state_path(root), now=1000.0 + CONFIG["min_interval_seconds"] + 1, **CONFIG)
    ok = (first.get("verdict") == "green" and soon.get("checked_at") == first.get("checked_at")
          and soon.get("verdict") == "green" and later.get("verdict") == "red")
    return case("checks-run-no-more-often-than-the-interval", ok, {"first": first.get("checked_at"),
                                                                    "soon": soon.get("checked_at"),
                                                                    "later": later.get("verdict")})


def c7_wiring(root: Path) -> dict[str, Any]:
    results = {}
    for name, config in (("off", None), ("on", CONFIG)):
        sub = root / name
        target = sealed_target(sub)
        base = git(target, "rev-parse", "HEAD")

        def fake_factory(*, timeout_seconds: float = 900):
            def model(_workspace: Path, _capsule: dict) -> dict:
                return {"summary": f"unused ({timeout_seconds})"}
            return model

        def idle_driver(_worker, **_kwargs):
            return {"stop_reason": "idle", "cycles": 0, "runs_dispatched": 0}

        kwargs: dict[str, Any] = {} if config is None else {"scheduled_checks": config}
        with explicit_runstore_factory(fixture_glr):
            out = fixture_glr.run(
                executor="fake", execute=True, goal_store_root=sub / "goals", run_store_root=sub / "runs",
                workspace_root=sub / "workspaces", campaign=make_campaign("campaign-checks"), source_repo=target,
                base_revision=base, factory_overrides={"fake": fake_factory}, driver_fn=idle_driver, **kwargs,
            )
        results[name] = {"key": "scheduled_checks" in out, "file": state_path(sub).is_file(),
                         "verdict": (out.get("scheduled_checks") or {}).get("verdict")}
    ok = results["off"] == {"key": False, "file": False, "verdict": None} and results["on"] == {
        "key": True, "file": True, "verdict": "green"}
    return case("goal-loop-run-checks-only-when-the-contract-enables-it", ok, results)


def main() -> int:
    builds = [
        ("intact-checks-leave-a-green-verdict-and-no-lamp", c1_green),
        ("a-broken-seal-lights-integrity-check-red", c2_broken),
        ("a-stale-missing-or-unreadable-verdict-lights-the-lamp", c3_stale_or_missing),
        ("not-enabled-means-the-rule-does-not-apply", c4_disabled),
        ("the-checks-never-write-to-the-target", c5_read_only),
        ("checks-run-no-more-often-than-the-interval", c6_interval),
        ("goal-loop-run-checks-only-when-the-contract-enables-it", c7_wiring),
    ]
    with tempfile.TemporaryDirectory(prefix="lh-scheduled-checks-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        results = []
        for name, build in builds:
            (root / name).mkdir()
            results.append(guarded(name, lambda build=build, name=name: build(root / name)))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

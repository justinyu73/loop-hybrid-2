#!/usr/bin/env python3
"""Regression watch: a completed goal is looked at again, and a regression is raised, not repaired.

A goal that reached ``completed`` used to be done for good: a later change that
broke its acceptance lamp went unnoticed.  ``regression_watch.sweep`` re-runs
the lamp of completed goals against the current source HEAD in a disposable
workspace, a bounded number per pass and no more often than an interval.

- green: only the last-checked state moves;
- red: one ``regression_detected`` event parked in ``human_required`` (an open
  question for the owner), keyed by goal, source HEAD and lamp; the goal stays
  ``completed`` and no run is created;
- a lamp that cannot be launched: ``unknown``, neither red nor green.

The watch is opt-in per project contract and off by default.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))
from _fixture import make_campaign, make_source_repo  # noqa: E402
from goal_store import GoalStore  # noqa: E402
import goal_loop_run as fixture_glr  # noqa: E402
import open_questions  # noqa: E402
from p7_native_runstore_fixture import explicit_runstore_factory  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-regression-watch"
LAMP = [sys.executable, "-c",
        "import pathlib, sys; sys.exit(0 if pathlib.Path('feature.txt').read_text().strip() == 'ok' else 1)"]
HOUR = 3600.0


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def watch():
    import regression_watch
    return regression_watch


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.strip()


def source_with_feature(root: Path) -> Path:
    source, _base = make_source_repo(root)
    (source / "feature.txt").write_text("ok\n", encoding="utf-8")
    git("-C", str(source), "add", "feature.txt")
    git("-C", str(source), "commit", "-qm", "feature")
    return source


def break_feature(source: Path) -> None:
    (source / "feature.txt").write_text("broken\n", encoding="utf-8")
    git("-C", str(source), "commit", "-qam", "regress")


def completed_goal(store: GoalStore, goal_id: str, *, lamp: list[str] | None = None) -> None:
    goal = {"feature_contract": goal_id,
            "admission_envelope": {"allowed_paths": ["src/"],
                                   "acceptance_lamp": {"id": f"{goal_id}-lamp", "smoke": "feature ok",
                                                       "verification_argv": lamp or LAMP}}}
    event_key = f"seed:{goal_id}"
    store.record_event(event_id=f"evt-{goal_id}", idempotency_key=event_key, source="manual",
                       event_type="manual_intent", payload={"goal_id": goal_id})
    store.create_candidate(event_key, goal_id=goal_id, campaign_id="campaign-watch", stage_id="stage-1", goal=goal)
    store.transition_goal(goal_id, "active")
    store.transition_goal(goal_id, "completed")


def regressions(store: GoalStore) -> list[dict[str, Any]]:
    return [row for row in store.events_from("regression_watch") if row.get("event_type") == "regression_detected"]


def sweep(store: GoalStore, source: Path, root: Path, *, now: float, **kwargs: Any) -> dict[str, Any]:
    kwargs.setdefault("max_goals", 5)
    kwargs.setdefault("min_interval_seconds", HOUR)
    return watch().sweep(store, source_repo=source, workspace_root=root / "watch-workspaces", now=now, **kwargs)


def outcomes(result: dict[str, Any]) -> dict[str, str]:
    return {row["goal_id"]: row["outcome"] for row in result.get("checked", [])}


def c1_green(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    completed_goal(store, "g-green")
    result = sweep(store, source, root, now=1000.0)
    ok = (result.get("schema") == "lh-regression-watch/v1" and outcomes(result) == {"g-green": "green"}
          and not regressions(store) and store.get_goal("g-green")["state"] == "completed")
    return case("a-completed-goal-that-is-still-green-raises-nothing", ok, result)


def c2_regression(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    completed_goal(store, "g-red")
    before = store.get_goal("g-red")
    break_feature(source)
    result = sweep(store, source, root, now=1000.0)
    events = regressions(store)
    after = store.get_goal("g-red")
    questions = open_questions.build_open_questions(RunStore(root / "runs"), store, now=1001.0)
    listed = [item for item in questions["items"] if item["source"] == "event"]
    ok = (outcomes(result) == {"g-red": "red"} and len(events) == 1 and events[0]["state"] == "human_required"
          and str((events[0].get("result") or {}).get("reason", "")).startswith("regression_detected")
          and after["state"] == "completed" and after.get("run_id") == before.get("run_id")
          and len(listed) == 1 and listed[0]["kind"] == open_questions.AWAITING_OWNER)
    return case("a-regression-is-raised-for-the-owner-and-nothing-is-reopened", ok,
                {"result": result, "events": events, "goal_state": after["state"], "open_questions": listed})


def c3_dedupe(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    completed_goal(store, "g-red")
    break_feature(source)
    sweep(store, source, root, now=1000.0)
    again = sweep(store, source, root, now=1000.0 + 2 * HOUR)
    ok = outcomes(again) == {"g-red": "red"} and len(regressions(store)) == 1
    return case("the-same-head-raises-the-same-regression-once", ok, {"again": again, "events": len(regressions(store))})


def c4_bounds(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    for goal_id in ("g-a", "g-b", "g-c"):
        completed_goal(store, goal_id)
    first = sweep(store, source, root, now=1000.0, max_goals=2)
    second = sweep(store, source, root, now=1000.0 + 60, max_goals=2)
    third = sweep(store, source, root, now=1000.0 + 2 * HOUR, max_goals=2)
    ok = (len(first.get("checked", [])) == 2
          and set(outcomes(second)) == {"g-a", "g-b", "g-c"} - set(outcomes(first))
          and len(third.get("checked", [])) == 2
          and set(outcomes(first)) | set(outcomes(second)) == {"g-a", "g-b", "g-c"})
    return case("each-pass-is-bounded-and-respects-the-interval", ok,
                {"first": outcomes(first), "second": outcomes(second), "third": outcomes(third)})


def c5_unknown(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    completed_goal(store, "g-unknown", lamp=[str(root / "missing" / "lamp-that-does-not-exist")])
    result = sweep(store, source, root, now=1000.0)
    ok = outcomes(result) == {"g-unknown": "unknown"} and not regressions(store)
    return case("a-lamp-that-cannot-run-is-unknown-not-a-regression", ok, result)


def _run_goal_loop(root: Path, source: Path, regression_watch: dict[str, Any] | None) -> dict[str, Any]:
    base = git("-C", str(source), "rev-parse", "HEAD")

    def fake_factory(*, timeout_seconds: float = 900):
        def model(_workspace: Path, _capsule: dict) -> dict:
            return {"summary": f"unused ({timeout_seconds})"}
        return model

    def idle_driver(_worker, **_kwargs):
        return {"stop_reason": "idle", "cycles": 0, "runs_dispatched": 0}

    kwargs: dict[str, Any] = {} if regression_watch is None else {"regression_watch": regression_watch}
    with explicit_runstore_factory(fixture_glr):
        return fixture_glr.run(
            executor="fake", execute=True, goal_store_root=root / "goals", run_store_root=root / "runs",
            workspace_root=root / "workspaces", campaign=make_campaign("campaign-watch"), source_repo=source,
            base_revision=base, factory_overrides={"fake": fake_factory}, driver_fn=idle_driver, **kwargs,
        )


def c6_opt_in(root: Path) -> dict[str, Any]:
    off_root, on_root = root / "off", root / "on"
    results = {}
    for name, sub, config in (("off", off_root, None),
                              ("on", on_root, {"max_goals": 5, "min_interval_seconds": HOUR})):
        source = source_with_feature(sub)
        store = GoalStore(sub / "goals")
        completed_goal(store, "g-red")
        break_feature(source)
        result = _run_goal_loop(sub, source, config)
        results[name] = {"has_watch": "regression_watch" in result, "events": len(regressions(GoalStore(sub / "goals"))),
                         "watch": result.get("regression_watch")}
    ok = (results["off"] == {"has_watch": False, "events": 0, "watch": None}
          and results["on"]["has_watch"] and results["on"]["events"] == 1)
    return case("the-watch-is-opt-in-and-off-by-default", ok, results)


def c7_source_untouched(root: Path) -> dict[str, Any]:
    source = source_with_feature(root)
    store = GoalStore(root / "goals")
    completed_goal(store, "g-green")
    completed_goal(store, "g-other")
    head = git("-C", str(source), "rev-parse", "HEAD")
    refs = git("-C", str(source), "for-each-ref")
    sweep(store, source, root, now=1000.0)
    leftover = [p for p in (root / "watch-workspaces").rglob("*")] if (root / "watch-workspaces").exists() else []
    ok = (git("-C", str(source), "rev-parse", "HEAD") == head and git("-C", str(source), "for-each-ref") == refs
          and git("-C", str(source), "status", "--porcelain") == "" and not leftover)
    return case("the-watch-never-writes-to-the-source-and-cleans-its-workspaces", ok,
                {"leftover": [str(p) for p in leftover[:5]]})


def main() -> int:
    builds = [
        ("a-completed-goal-that-is-still-green-raises-nothing", c1_green),
        ("a-regression-is-raised-for-the-owner-and-nothing-is-reopened", c2_regression),
        ("the-same-head-raises-the-same-regression-once", c3_dedupe),
        ("each-pass-is-bounded-and-respects-the-interval", c4_bounds),
        ("a-lamp-that-cannot-run-is-unknown-not-a-regression", c5_unknown),
        ("the-watch-is-opt-in-and-off-by-default", c6_opt_in),
        ("the-watch-never-writes-to-the-source-and-cleans-its-workspaces", c7_source_untouched),
    ]
    with tempfile.TemporaryDirectory(prefix="lh-regression-watch-", ignore_cleanup_errors=True) as raw:
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

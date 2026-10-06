#!/usr/bin/env python3
"""Open questions: what a human owes is typed, aged, and read from the store.

``human_required`` goals and events are projected into typed open questions
with their reason and waiting time.  The projection decides nothing and writes
nothing; it only makes visible which items are owed and which have gone quiet.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import status_snapshot  # noqa: E402
from controller import LoopController  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from native_delivery_fixture import make_native_run  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-open-questions"
CHECK = "from pathlib import Path; assert Path('src/out.txt').read_text(encoding='utf-8').strip() == 'done'"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing API is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def project(runs: RunStore, goals: GoalStore, *, now: float, quiet_after_seconds: float = 3600.0) -> dict[str, Any]:
    module = importlib.import_module("open_questions")
    return module.build_open_questions(runs, goals, now=now, quiet_after_seconds=quiet_after_seconds)


def git(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", check=check)


class World:
    def __init__(self, root: Path):
        root.mkdir(parents=True)
        self.root = root
        self.source = root / "source"
        self.source.mkdir()
        for args in (("init", "-q"), ("config", "user.email", "oq@example.invalid"), ("config", "user.name", "OQ")):
            git(*args, cwd=self.source)
        (self.source / "baseline.txt").write_text("baseline\n", encoding="utf-8")
        git("add", "baseline.txt", cwd=self.source)
        git("commit", "-qm", "baseline", cwd=self.source)
        self.base = git("rev-parse", "HEAD", cwd=self.source).stdout.strip()
        self.runs = RunStore(root / "runs", command_runner=fixture_command_runner)
        self.goals = GoalStore(root / "goals")

    def parked_goal(self, name: str, model: Callable[[Path, dict[str, Any]], dict[str, Any]]) -> str:
        checks = [{"id": "out", "commands": [{"id": "out", "argv": [sys.executable, "-B", "-c", CHECK],
                                              "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}],
                   "required_receipts": ["executor"]}]
        verifier = [sys.executable, "-B", "-c", CHECK]
        bundle = make_native_run(self.runs, self.source, self.base, name, "oq", checks, verifier, ["src/"], 1,
                                 goal={"goal_id": name}, run_id=name)
        event = self.goals.record_event(event_id=f"{name}-event", source="open-questions-canary",
                                        event_type="goal_candidate", payload={"goal_id": name})
        self.goals.create_candidate(event["event_key"], goal_id=name, campaign_id="oq", stage_id="oq",
                                    goal=self.runs.get_run(name)["goal"], revision=bundle["contract"]["goal"]["revision"])
        self.goals.activate_with_run(name, name, event_key=event["event_key"])
        LoopController(self.runs, self.root / "workspaces").tick(name, holder="oq", model=model, verifier_argv=verifier)
        # The worker parks a goal whose run needs a human; this fixture does the same.
        self.goals.transition_goal(name, "human_required", expected_state="active")
        return name

    def digests(self) -> dict[str, str]:
        return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted([*self.root.glob("runs/*.sqlite3"), *self.root.glob("goals/*.sqlite3")])}


def fence_model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
    return {"summary": "execution fence unavailable; model not invoked",
            "failure": "execution_fence_unavailable: backend_not_configured",
            "routing": {"route": "human_required", "reason": "execution_fence_unavailable"}}


def pushing_model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
    (workspace / "src").mkdir(exist_ok=True)
    (workspace / "src" / "out.txt").write_text("done\n", encoding="utf-8")
    git("config", "user.email", "agent@example.invalid", cwd=workspace)
    git("config", "user.name", "Agent", cwd=workspace)
    git("add", "-A", cwd=workspace)
    git("commit", "-qm", "candidate", cwd=workspace)
    git("push", "--no-verify", str(workspace.parents[2] / "source"), "HEAD:refs/heads/moved", cwd=workspace, check=False)
    return {"summary": "pushed past the boundary", "usage": {"state": "unknown"}}


def _items(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["subject"]: item for item in result.get("items", [])}


def c1_c2_kinds(root: Path) -> list[dict[str, Any]]:
    world = World(root)
    world.parked_goal("G-fence", fence_model)
    world.parked_goal("G-moved", pushing_model)
    items = _items(project(world.runs, world.goals, now=10**10))
    fence, moved = items.get("G-fence", {}), items.get("G-moved", {})
    return [
        case("a-missing-fence-awaits-the-owner",
             fence.get("kind") == "awaiting_owner" and "execution_fence_unavailable" in str(fence.get("reason")), fence),
        case("a-moved-source-branch-is-a-scope-escalation",
             moved.get("kind") == "scope_escalation" and "source_refs_mutated" in str(moved.get("reason")), moved),
    ]


def c3_event(root: Path) -> dict[str, Any]:
    world = World(root)
    event = world.goals.record_event(event_id="cmd-unknown-stage", source="operator", event_type="manual_intent",
                                     payload={"campaign_id": "oq", "stage_id": "missing"})
    world.goals.transition_event(event["event_key"], "human_required", result={"reason": "stage_unknown"})
    item = _items(project(world.runs, world.goals, now=10**10)).get(event["event_key"], {})
    return case("an-unmatched-command-awaits-the-owner",
                item.get("kind") == "awaiting_owner" and item.get("source") == "event"
                and "stage_unknown" in str(item.get("reason")), item)


def c4_quiet(root: Path) -> dict[str, Any]:
    world = World(root)
    world.parked_goal("G-quiet", fence_model)
    updated = world.goals.get_goal("G-quiet")["updated_at"]
    fresh = _items(project(world.runs, world.goals, now=updated + 10, quiet_after_seconds=3600)).get("G-quiet", {})
    old = _items(project(world.runs, world.goals, now=updated + 7200, quiet_after_seconds=3600)).get("G-quiet", {})
    ok = fresh.get("quiet") is False and old.get("quiet") is True and old.get("waiting_seconds", 0) >= 7200
    return case("an-item-left-waiting-goes-quiet", ok, {"fresh": fresh, "old": old})


def c5_resolved(root: Path) -> dict[str, Any]:
    world = World(root)
    world.parked_goal("G-resolved", fence_model)
    before = "G-resolved" in _items(project(world.runs, world.goals, now=10**10))
    world.goals.transition_goal("G-resolved", "candidate", expected_state="human_required")
    after = "G-resolved" in _items(project(world.runs, world.goals, now=10**10))
    return case("a-resolved-item-disappears", before and not after, {"listed_before": before, "listed_after": after})


def c6_snapshot(root: Path) -> dict[str, Any]:
    world = World(root)
    world.parked_goal("G-snap", fence_model)
    before = world.digests()
    snapshot = status_snapshot.build_snapshot(world.runs, world.goals, generated_at="2026-10-06T00:00:00Z")
    after = world.digests()
    listed = [item.get("subject") for item in (snapshot.get("open_questions") or {}).get("items", [])]
    ok = "G-snap" in listed and before == after and bool(before)
    return case("the-snapshot-carries-open-questions-and-writes-nothing", ok,
                {"listed": listed, "store_unchanged": before == after})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-open-questions-") as raw:
        root = Path(raw).resolve()
        try:
            results = c1_c2_kinds(root / "c1")
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:300]}"
            results = [case("a-missing-fence-awaits-the-owner", False, detail),
                       case("a-moved-source-branch-is-a-scope-escalation", False, detail)]
        results.extend([
            guarded("an-unmatched-command-awaits-the-owner", lambda: c3_event(root / "c3")),
            guarded("an-item-left-waiting-goes-quiet", lambda: c4_quiet(root / "c4")),
            guarded("a-resolved-item-disappears", lambda: c5_resolved(root / "c5")),
            guarded("the-snapshot-carries-open-questions-and-writes-nothing", lambda: c6_snapshot(root / "c6")),
        ])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

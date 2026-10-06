#!/usr/bin/env python3
"""Status trust: the snapshot says which code is running, and one pure lamp judges it.

A driver keeps running the code it loaded at start.  When the engine files on
disk change underneath it, the heartbeat and the snapshot must say so
(``code_identity.stale``) instead of looking healthy.  Whether the project is
degraded is decided in exactly one place, ``status_lamp.lamp``: a pure function
of the snapshot that names the rule behind its answer.
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from goal_store import GoalStore  # noqa: E402
from run_store import RunStore  # noqa: E402
import status_snapshot  # noqa: E402

CHECK_ID = "lh-status-trust"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def lamp_module():
    import status_lamp
    return status_lamp


DRIVER = textwrap.dedent('''
    import json, sys
    from pathlib import Path
    engine, store, edit = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] == "edit"
    sys.path.insert(0, str(engine))
    from goal_loop_driver import run_driver
    from goal_store import GoalStore
    from run_store import RunStore
    import status_snapshot

    class Worker:
        def __init__(self):
            self.run_store = RunStore(store / "runs")
            self.goal_store = GoalStore(store / "goals")
            self.ticks = 0
        def tick(self, **_kwargs):
            self.ticks += 1
            if edit and self.ticks == 1:  # the engine is updated on disk while the driver keeps running
                target = engine / "run_store.py"
                target.write_text(target.read_text(encoding="utf-8") + "\\n# updated on disk\\n", encoding="utf-8")
            return {"status": "idle", "run": None, "terminal_after": None}

    worker = Worker()
    run_driver(worker, holder="status-trust", model=lambda _w, _c: {"summary": "unused"},
               max_cycles=3, idle_limit=3, backoff_seconds=0, sleep_fn=lambda _s: None)
    heartbeat = status_snapshot.read_heartbeat(status_snapshot.default_heartbeat_path(worker.run_store.root))
    snapshot = status_snapshot.build_snapshot(worker.run_store, worker.goal_store, generated_at="t", heartbeat=heartbeat)
    print(json.dumps({"heartbeat": heartbeat, "snapshot_code_identity": snapshot.get("code_identity")}))
''')


def _drive(root: Path, *, edit: bool) -> dict[str, Any]:
    engine = root / "engine"
    shutil.copytree(HERE, engine, ignore=shutil.ignore_patterns("__pycache__"))
    script = root / "drive.py"
    script.write_text(DRIVER, encoding="utf-8")
    done = subprocess.run([sys.executable, "-B", str(script), str(engine), str(root / "store"), "edit" if edit else "keep"],
                          capture_output=True, text=True, encoding="utf-8", timeout=180, stdin=subprocess.DEVNULL)
    if done.returncode != 0:
        raise RuntimeError(done.stderr[-400:])
    return json.loads(done.stdout.strip().splitlines()[-1])


def c1_code_identity(root: Path) -> list[dict[str, Any]]:
    edited = _drive(root / "edited", edit=True)
    kept = _drive(root / "kept", edit=False)
    hb_edited = (edited["heartbeat"] or {}).get("code_identity") or {}
    hb_kept = (kept["heartbeat"] or {}).get("code_identity") or {}
    snap_edited = edited.get("snapshot_code_identity") or {}
    snap_kept = kept.get("snapshot_code_identity") or {}
    digests = all(str(value.get(key, "")).startswith("sha256:")
                  for value in (hb_edited, hb_kept, snap_edited, snap_kept) for key in ("loaded_digest", "disk_digest"))
    return [
        case("changed-code-without-restart-is-stale",
             digests and hb_edited.get("stale") is True and snap_edited.get("stale") is True
             and hb_edited["loaded_digest"] != hb_edited["disk_digest"]
             and snap_edited.get("loaded_digest") == hb_edited["loaded_digest"],
             {"heartbeat": hb_edited, "snapshot": snap_edited}),
        case("unchanged-code-is-not-stale",
             digests and hb_kept.get("stale") is False and snap_kept.get("stale") is False
             and hb_kept["loaded_digest"] == hb_kept["disk_digest"],
             {"heartbeat": hb_kept, "snapshot": snap_kept}),
    ]


def _healthy(root: Path) -> dict[str, Any]:
    runs, goals = RunStore(root / "runs"), GoalStore(root / "goals")
    identity = status_snapshot.engine_code_identity(status_snapshot.engine_digest())
    heartbeat = status_snapshot.build_heartbeat(holder="lamp", monotonic_ts=1.0,
                                                wall_ts=datetime.now(timezone.utc).isoformat(), phase="idle",
                                                cycles=1, code_identity=identity)
    return status_snapshot.build_snapshot(runs, goals, generated_at="t", heartbeat=heartbeat,
                                          dispatch_gate={"action": "allow", "reason_code": None})


def _without_lamp(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in snapshot.items() if key != "lamp"}


def c2_pure(root: Path) -> dict[str, Any]:
    lamp = lamp_module().lamp
    snapshot = _without_lamp(_healthy(root))
    frozen = copy.deepcopy(snapshot)
    first, second, third = lamp(snapshot), lamp(copy.deepcopy(snapshot)), lamp(snapshot)
    ok = first == second == third and snapshot == frozen and first.get("lamp") == "ok" and first.get("fired") == []
    return case("same-snapshot-same-lamp", ok, {"lamp": first, "input_unchanged": snapshot == frozen})


def c3_degraded(root: Path) -> list[dict[str, Any]]:
    lamp = lamp_module().lamp
    base = _without_lamp(_healthy(root))
    variants: dict[str, Callable[[dict[str, Any]], None]] = {
        "heartbeat_stale": lambda s: s.update(stale=True),
        "code_identity_stale": lambda s: s["code_identity"].update(stale=True),
        "needs_human": lambda s: s["status"]["headline"].update(needs_human=1),
        "dispatch_stopped": lambda s: s.update(dispatch_gate={"action": "stop", "reason_code": "daily_cost_hard"}),
    }
    rows = []
    for rule_id, mutate in variants.items():
        snapshot = copy.deepcopy(base)
        mutate(snapshot)
        result = lamp(snapshot)
        fired = {row.get("id"): row.get("rule") for row in result.get("fired", [])}
        ok = result.get("lamp") == "degraded" and list(fired) == [rule_id] and bool(fired[rule_id])
        rows.append(case(f"degraded-when-{rule_id.replace('_', '-')}", ok, result))
    unknown = copy.deepcopy(base)
    unknown["code_identity"] = {"loaded_digest": None, "disk_digest": unknown["code_identity"]["disk_digest"], "stale": None}
    result = lamp(unknown)
    rows.append(case("unknown-code-identity-is-degraded",
                     result.get("lamp") == "degraded" and "code_identity_stale" in {r.get("id") for r in result.get("fired", [])},
                     result))
    return rows


def _tree_digest(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob("*")) if path.is_file()}


def c4_projection(root: Path) -> list[dict[str, Any]]:
    runs, goals = RunStore(root / "runs"), GoalStore(root / "goals")
    before_runs, before_goals = _tree_digest(runs.root), _tree_digest(goals.root)
    snapshot = status_snapshot.build_snapshot(runs, goals, generated_at="t")
    lamp_module().lamp(_without_lamp(snapshot))
    after_runs, after_goals = _tree_digest(runs.root), _tree_digest(goals.root)
    carried = snapshot.get("lamp")
    return [
        case("projection-does-not-change-the-stores",
             before_runs == after_runs and before_goals == after_goals,
             {"runs_changed": sorted(set(before_runs.items()) ^ set(after_runs.items())),
              "goals_changed": sorted(set(before_goals.items()) ^ set(after_goals.items()))}),
        case("snapshot-carries-the-one-lamp",
             isinstance(carried, dict) and carried == lamp_module().lamp(_without_lamp(snapshot))
             and carried.get("lamp") == "degraded",  # no heartbeat at all: unknown is not healthy
             {"lamp": carried}),
    ]


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-status-trust-") as raw:
        root = Path(raw).resolve()
        results: list[dict[str, Any]] = []
        groups: list[tuple[list[str], Callable[[], list[dict[str, Any]]]]] = [
            (["changed-code-without-restart-is-stale", "unchanged-code-is-not-stale"],
             lambda: c1_code_identity(root / "c1")),
            (["same-snapshot-same-lamp"], lambda: [c2_pure(root / "c2")]),
            (["degraded-when-heartbeat-stale", "degraded-when-code-identity-stale", "degraded-when-needs-human",
              "degraded-when-dispatch-stopped", "unknown-code-identity-is-degraded"],
             lambda: c3_degraded(root / "c3")),
            (["projection-does-not-change-the-stores", "snapshot-carries-the-one-lamp"],
             lambda: c4_projection(root / "c4")),
        ]
        for names, action in groups:
            try:
                results.extend(action())
            except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
                detail = f"{type(exc).__name__}: {str(exc)[:300]}"
                results.extend(case(name, False, detail) for name in names)
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

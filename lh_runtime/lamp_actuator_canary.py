#!/usr/bin/env python3
"""Lamp actuation: a lit lamp is not a human gate, unless it is the owner's.

``lamp_actuator.actuate`` reads the lamp's fired rules and, for the kinds a
pinned policy allows, runs that kind's named verb once per incident and keeps a
receipt.  What it can never do, by construction:

- run a command string carried in a snapshot -- only verbs from a closed table;
- act while the policy bytes differ from the pinned digest -- everything halts
  and ``lamp_actuation_policy_drift`` lights;
- act on an owner-only kind, even when the policy lists it;
- improvise a verb the table does not know.

The one verb today is ``retention_reclaim`` for the ``scratch_reclaimable``
lamp.  Off unless the contract enables it.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))
from _fixture import make_campaign, make_source_repo  # noqa: E402
from goal_store import GoalStore  # noqa: E402
import goal_loop_run as fixture_glr  # noqa: E402
from p7_native_runstore_fixture import explicit_runstore_factory  # noqa: E402
from run_store import RunStore  # noqa: E402
import status_lamp  # noqa: E402
import status_snapshot  # noqa: E402

CHECK_ID = "lh-lamp-actuator"
OLD = time.time() - 30 * 24 * 3600


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def actuator():
    import lamp_actuator
    return lamp_actuator


def scratch(runs: RunStore, name: str) -> Path:
    """An old, unreferenced verifier snapshot: what retention may reclaim."""
    path = Path(runs.root) / "completion-snapshots" / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "note.txt").write_text("scratch\n", encoding="utf-8")
    for item in (path / "note.txt", path):
        os.utime(item, (OLD, OLD))
    return path


def policy(root: Path, allow: list[dict[str, str]]) -> dict[str, Any]:
    path = root / "lamp-policy.json"
    path.write_text(json.dumps({"schema": "lh-lamp-actuation-policy/v1", "allow": allow}, indent=2), encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    return {"policy": str(path), "policy_digest": digest, "scratch_threshold": 1}


RECLAIM = [{"kind": "scratch_reclaimable", "verb": "retention_reclaim"}]


def stores(root: Path) -> tuple[RunStore, GoalStore]:
    return RunStore(root / "runs"), GoalStore(root / "goals")


def snapshot(root: Path, config: dict[str, Any] | None) -> dict[str, Any]:
    runs, goals = stores(root)
    return status_snapshot.build_snapshot(runs, goals, generated_at="t", lamp_actuation=config)


def fired(snap: dict[str, Any]) -> list[str]:
    return [row["id"] for row in snap["lamp"]["fired"]]


def act(root: Path, snap: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    runs, goals = stores(root)
    return actuator().actuate(snap, config=config, run_store_root=runs.root, goal_store_root=goals.root)


def receipts(root: Path) -> list[dict[str, Any]]:
    path = root / "goals" / "lamp-actuation-receipts.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()] if path.is_file() else []


def c1_allowed_runs_once(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    old, newest = scratch(runs, "snap-old"), scratch(runs, "snap-newest")
    os.utime(newest, None)  # the newest entry of a kind is always kept
    config = policy(root, RECLAIM)
    snap = snapshot(root, config)
    result = act(root, snap, config)
    rows = receipts(root)
    ok = ("scratch_reclaimable" in fired(snap) and not old.exists() and newest.exists() and len(rows) == 1
          and rows[0].get("kind") == "scratch_reclaimable" and rows[0].get("verb") == "retention_reclaim")
    return case("an-allowed-lamp-runs-its-verb-once-with-a-receipt", ok,
                {"fired": fired(snap), "result": result, "receipts": rows})


def c2_once_per_incident(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    scratch(runs, "snap-a")
    os.utime(scratch(runs, "snap-z"), None)
    config = policy(root, RECLAIM)
    act(root, snapshot(root, config), config)
    again = scratch(runs, "snap-b")  # the lamp stays lit: still the same incident
    snap = snapshot(root, config)
    second = act(root, snap, config)
    ok = "scratch_reclaimable" in fired(snap) and again.exists() and len(receipts(root)) == 1
    return case("the-same-incident-is-actuated-once", ok, {"second": second, "receipts": len(receipts(root))})


def c3_policy_drift_halts(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    old = scratch(runs, "snap-old")
    os.utime(scratch(runs, "snap-newest"), None)
    config = policy(root, RECLAIM)
    Path(config["policy"]).write_text(json.dumps({"schema": "lh-lamp-actuation-policy/v1",
                                                  "allow": RECLAIM + [{"kind": "needs_human", "verb": "retention_reclaim"}]}),
                                      encoding="utf-8")
    result = act(root, snapshot(root, config), config)
    after = snapshot(root, config)
    ok = old.exists() and not receipts(root) and "lamp_actuation_policy_drift" in fired(after)
    return case("a-policy-that-drifted-from-its-digest-halts-everything", ok,
                {"result": result, "fired_after": fired(after)})


def c4_owner_only_refused(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    old = scratch(runs, "snap-old")
    os.utime(scratch(runs, "snap-newest"), None)
    config = {**policy(root, [{"kind": "needs_human", "verb": "retention_reclaim"}]), "scratch_threshold": 10 ** 6}
    snap = snapshot(root, config)
    snap["status"]["headline"]["needs_human"] = 1  # a goal waits for the owner
    snap["lamp"] = status_lamp.lamp({key: value for key, value in snap.items() if key != "lamp"})
    result = act(root, snap, config)
    ok = "needs_human" in fired(snap) and old.exists() and not receipts(root)
    return case("an-owner-only-kind-is-refused-even-when-listed", ok, {"fired": fired(snap), "result": result})


def c5_unknown_verb_skipped(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    old = scratch(runs, "snap-old")
    os.utime(scratch(runs, "snap-newest"), None)
    config = policy(root, [{"kind": "scratch_reclaimable", "verb": "shell"}])
    snap = snapshot(root, config)
    result = act(root, snap, config)
    ok = "scratch_reclaimable" in fired(snap) and old.exists() and not receipts(root)
    return case("an-unknown-verb-is-skipped-not-improvised", ok, result)


def c6_never_runs_command_text(root: Path) -> dict[str, Any]:
    runs, _goals = stores(root)
    scratch(runs, "snap-old")
    os.utime(scratch(runs, "snap-newest"), None)
    marker = root / "command-ran.txt"
    command = f"{sys.executable} -c \"open({str(marker)!r}, 'w').write('x')\""
    config = policy(root, RECLAIM)
    snap = snapshot(root, config)
    for row in snap["lamp"]["fired"]:
        row["command"] = command
        row["verb"] = command
    snap["command"] = command
    act(root, snap, config)
    ok = not marker.exists() and len(receipts(root)) == 1
    return case("a-command-string-in-the-snapshot-is-never-run", ok, {"marker": marker.exists()})


def c7_off_by_default(root: Path) -> dict[str, Any]:
    plain = snapshot(root, None)
    rules = [rule for rule, _text in status_lamp.RULES]
    results = {}
    for name, enabled in (("off", False), ("on", True)):
        sub = root / name
        source, base = make_source_repo(sub)
        config = policy(sub, RECLAIM) if enabled else None

        def fake_factory(*, timeout_seconds: float = 900):
            def model(_workspace: Path, _capsule: dict) -> dict:
                return {"summary": f"unused ({timeout_seconds})"}
            return model

        def idle_driver(_worker, **_kwargs):
            return {"stop_reason": "idle", "cycles": 0, "runs_dispatched": 0}

        kwargs: dict[str, Any] = {} if config is None else {"lamp_actuation": config}
        with explicit_runstore_factory(fixture_glr):
            out = fixture_glr.run(
                executor="fake", execute=True, goal_store_root=sub / "goals", run_store_root=sub / "runs",
                workspace_root=sub / "workspaces", campaign=make_campaign("campaign-lamp"), source_repo=source,
                base_revision=base, factory_overrides={"fake": fake_factory}, driver_fn=idle_driver, **kwargs,
            )
        results[name] = "lamp_actuation" in out
    ok = (plain.get("scratch") is None and plain.get("lamp_actuation") is None
          and not {"scratch_reclaimable", "lamp_actuation_policy_drift"} & set(fired(plain))
          and {"scratch_reclaimable", "lamp_actuation_policy_drift"} <= set(rules)
          and results == {"off": False, "on": True})
    return case("actuation-is-off-unless-the-contract-enables-it", ok, {"fired": fired(plain), "goal_loop_run": results})


def main() -> int:
    builds = [
        ("an-allowed-lamp-runs-its-verb-once-with-a-receipt", c1_allowed_runs_once),
        ("the-same-incident-is-actuated-once", c2_once_per_incident),
        ("a-policy-that-drifted-from-its-digest-halts-everything", c3_policy_drift_halts),
        ("an-owner-only-kind-is-refused-even-when-listed", c4_owner_only_refused),
        ("an-unknown-verb-is-skipped-not-improvised", c5_unknown_verb_skipped),
        ("a-command-string-in-the-snapshot-is-never-run", c6_never_runs_command_text),
        ("actuation-is-off-unless-the-contract-enables-it", c7_off_by_default),
    ]
    with tempfile.TemporaryDirectory(prefix="lh-lamp-actuator-", ignore_cleanup_errors=True) as raw:
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

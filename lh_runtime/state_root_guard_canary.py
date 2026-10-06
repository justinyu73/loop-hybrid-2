#!/usr/bin/env python3
"""State-root guard: no task writes inside the engine's own production roots.

The production roots are the ones the engine itself uses, from the platform
path rules (``PlatformPaths``): the default per-platform location, or the one
``LH_INSTANCE_ROOT`` / ``LH_STATE_ROOT`` and friends point to.  Every guarded
module refuses a state or scratch root inside them.  Task-owned scratch is
named ``LH_TASK_STATE_ROOT`` / ``LH_TASK_TMP_ROOT``; the earlier names are
refused outright, naming the new one, so a protection someone configured never
fails silently.
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterator

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))  # work_unit_completion imports as a package module

import platform_ports  # noqa: E402
from plan_node_controller import PlanNodeController, PlanNodeControllerError  # noqa: E402
from post_merge_resume import BoundedPostMergeResumePoller, PostMergeResumeController, PostMergeResumeError  # noqa: E402
from lh_runtime.work_unit_completion import task_owned_check_environment  # noqa: E402

CHECK_ID = "lh-state-root-guard"
OLD_STATE, OLD_TMP = "LH_HOST_STATE_ROOT", "LH_HOST_TMP_ROOT"
NEW_STATE, NEW_TMP = "LH_TASK_STATE_ROOT", "LH_TASK_TMP_ROOT"
ROOT_VARIABLES = ("LH_INSTANCE_ROOT", "LH_STATE_ROOT", "LH_RUN_ROOT", "LH_WORKSPACE_ROOT", "LH_CACHE_ROOT",
                  "LH_LOG_ROOT", "XDG_STATE_HOME", OLD_STATE, OLD_TMP, NEW_STATE, NEW_TMP)


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


@contextlib.contextmanager
def environment(home: Path, **values: str) -> Iterator[dict[str, str]]:
    """A process environment whose platform paths live under a fake home."""
    saved = dict(os.environ)
    try:
        for name in ROOT_VARIABLES:
            os.environ.pop(name, None)
        os.environ["HOME"] = str(home)
        os.environ["USERPROFILE"] = str(home)
        os.environ["LOCALAPPDATA"] = str(home / "AppData" / "Local")
        os.environ.update(values)
        yield dict(os.environ)
    finally:
        os.environ.clear()
        os.environ.update(saved)


def reason(action: Callable[[], Any]) -> str:
    try:
        result = action()
        if isinstance(result, contextlib.AbstractContextManager):
            with result:
                pass
        return "accepted"
    except (PlanNodeControllerError, PostMergeResumeError) as exc:
        return getattr(exc, "reason", str(exc))
    except ValueError as exc:
        return str(exc)


def guarded(target: Path, scratch: Path, env: dict[str, str]) -> dict[str, str]:
    return {
        "plan_node_controller": reason(lambda: PlanNodeController(target, goal_id="guard-goal")),
        "post_merge_resume": reason(lambda: PostMergeResumeController(target, expected_binding={})),
        "post_merge_poller": reason(lambda: BoundedPostMergeResumePoller(target, expected_binding={},
                                                                          idempotency_key="guard")),
        "completion_checks": reason(lambda: task_owned_check_environment(
            scratch / "store", str(scratch / "worktree"), base={**env, NEW_TMP: str(target)})),
    }


FORBIDDEN = {"plan_node_controller": "production_state_root_forbidden", "post_merge_resume": "production_state_root_forbidden",
             "post_merge_poller": "production_state_root_forbidden", "completion_checks": "completion_check_tmp_root_production"}


def c1_inside(root: Path) -> dict[str, Any]:
    wrong = {}
    variants = {"default": {}, "instance_root": {"LH_INSTANCE_ROOT": str(root / "instance")},
                "state_root": {"LH_STATE_ROOT": str(root / "state-override")}}
    for name, values in variants.items():
        with environment(root / f"home-{name}", **values) as env:
            paths = platform_ports.PlatformPaths.from_environment(env)
            for label, production in (("instance", paths.instance_root), ("state", paths.state_root)):
                got = guarded(Path(production) / "task", root / f"scratch-{name}-{label}", env)
                if got != FORBIDDEN:
                    wrong[f"{name}/{label}"] = got
    return case("every-guard-refuses-the-engines-own-production-roots", not wrong, wrong or "all refused")


def c2_outside(root: Path) -> dict[str, Any]:
    with environment(root / "home") as env:
        got = guarded(root / "elsewhere" / "task", root / "scratch", env)
    ok = all(value not in {"production_state_root_forbidden", "completion_check_tmp_root_production"}
             and not value.startswith("renamed_environment") for value in got.values())
    return case("roots-outside-production-are-accepted", ok, got)


def c3_renamed(root: Path) -> dict[str, Any]:
    results = {}
    with environment(root / "home", **{OLD_STATE: str(root / "old-state")}) as env:
        results["plan_node_controller"] = reason(lambda: PlanNodeController(root / "outside", goal_id="g"))
        results["post_merge_resume"] = reason(lambda: PostMergeResumeController(root / "outside", expected_binding={}))
        results["production_roots"] = reason(lambda: platform_ports.production_roots(env))
    with environment(root / "home") as env:
        results["completion_checks"] = reason(lambda: task_owned_check_environment(
            root / "store", str(root / "worktree"), base={**env, OLD_TMP: str(root / "old-tmp")}))
    expected_state = f"renamed_environment:{OLD_STATE}->{NEW_STATE}"
    expected_tmp = f"renamed_environment:{OLD_TMP}->{NEW_TMP}"
    ok = (all(results[key] == expected_state for key in ("plan_node_controller", "post_merge_resume", "production_roots"))
          and results["completion_checks"] == expected_tmp)
    return case("an-old-variable-name-is-refused-naming-the-new-one", ok, results)


def c4_child_environment(root: Path) -> dict[str, Any]:
    with environment(root / "home") as env:
        with task_owned_check_environment(root / "store", str(root / "worktree"),
                                          base={**env, NEW_TMP: str(root / "task-tmp")}) as (child, replacements):
            names = sorted(name for name in child if name.startswith("LH_"))
            ok = (NEW_STATE in child and NEW_TMP in child and OLD_STATE not in child and OLD_TMP not in child
                  and Path(child[NEW_STATE]).is_relative_to(root / "task-tmp")
                  and child[NEW_STATE] == replacements["${TASK_OWNED_STATE_ROOT}"])
    return case("check-commands-receive-only-the-new-names", ok, names)


def c5_residue() -> dict[str, Any]:
    found = {}
    for path in sorted(HERE.glob("*.py")):
        if path.name == Path(__file__).name:
            continue
        text = path.read_text(encoding="utf-8")
        hits = [token for token in ("external-host", OLD_STATE, OLD_TMP) if token in text]
        if hits and path.name != "platform_ports.py":
            found[path.name] = hits
    runner = (HERE / "runner_adapter.py").read_text(encoding="utf-8")
    table = (HERE / "platform_ports.py").read_text(encoding="utf-8")
    ok = not found and "P3B" not in runner and "external-host" not in table and OLD_STATE in table
    return case("old-names-survive-only-in-the-refusal-table", ok,
                {"elsewhere": found, "runner_adapter_P3B": "P3B" in runner})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-state-root-guard-") as raw:
        root = Path(raw).resolve()
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("every-guard-refuses-the-engines-own-production-roots", lambda: c1_inside(root / "c1")),
            ("roots-outside-production-are-accepted", lambda: c2_outside(root / "c2")),
            ("an-old-variable-name-is-refused-naming-the-new-one", lambda: c3_renamed(root / "c3")),
            ("check-commands-receive-only-the-new-names", lambda: c4_child_environment(root / "c4")),
            ("old-names-survive-only-in-the-refusal-table", c5_residue),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:  # a crash is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

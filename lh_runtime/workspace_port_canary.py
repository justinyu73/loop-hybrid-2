#!/usr/bin/env python3
"""Workspace port: how a disposable workspace is made may change; what it guarantees may not.

The controller builds each Attempt's workspace through a ``WorkspacePort``.  The
default backend is the existing ``git clone --no-local`` path, moved unchanged.
``workspace_port.check_backend`` runs the guarantees every backend must keep
against any backend; this exam runs it on the default (it must pass) and on a
deliberately unsound ``git worktree`` backend (it must fail), and checks the
controller really uses the injected backend and refuses a workspace outside its
root.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent / "tests"))

from controller import LoopController  # noqa: E402
from run_store import RunStore  # noqa: E402
import effect_guard_canary as runs_fixture  # noqa: E402

CHECK_ID = "lh-workspace-port"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def port():
    import workspace_port
    return workspace_port


class WorktreeBackend:
    """Unsound on purpose: a linked worktree shares the source's refs and object store."""

    def prepare(self, request):
        module = port()
        target = request.workspace_root / request.run_id / str(request.attempt)
        target.parent.mkdir(parents=True, exist_ok=True)
        done = request.run(["git", "-C", str(request.source_root), "worktree", "add", "--detach", str(target),
                            request.base_revision])
        if done.returncode != 0:
            raise RuntimeError(done.stderr)
        return module.PreparedWorkspace(workspace=target / request.target_subdir, git_root=target,
                                        ref=f"workspace://{request.run_id}/{request.attempt}")


class Spy:
    def __init__(self, inner):
        self.inner, self.calls = inner, 0

    def prepare(self, request):
        self.calls += 1
        return self.inner.prepare(request)


class Escaping:
    """Returns a workspace outside the controller's workspace root."""

    def __init__(self, outside: Path):
        self.outside = outside

    def prepare(self, request):
        module = port()
        prepared = module.GitCloneWorkspace().prepare(module.WorkspaceRequest(
            run_id=request.run_id, attempt=request.attempt, base_revision=request.base_revision,
            source_root=request.source_root, target_subdir=request.target_subdir,
            workspace_root=self.outside, run=request.run))
        return prepared


def c1_default(root: Path) -> dict[str, Any]:
    report = port().check_backend(port().GitCloneWorkspace(), root)
    failed = [name for name, row in report["checks"].items() if not row["ok"]]
    return case("the-default-backend-keeps-every-guarantee", report["verdict"] == "GREEN" and not failed,
                {"failed": failed, "checks": sorted(report["checks"])})


def c2_unsound(root: Path) -> dict[str, Any]:
    report = port().check_backend(WorktreeBackend(), root)
    failed = sorted(name for name, row in report["checks"].items() if not row["ok"])
    ok = report["verdict"] == "RED" and {"branches_stay_in_the_workspace", "objects_stay_in_the_workspace"} <= set(failed)
    return case("a-backend-that-shares-the-source-is-caught", ok, {"verdict": report["verdict"], "failed": failed})


def _diff(run: runs_fixture.Run) -> str:
    receipt = json.loads((run.runs.root / run.runs.latest_receipt(run.run_id)["receipt_ref"]).read_text(encoding="utf-8"))
    return (run.runs.root / receipt["diff"]["ref"]).read_text(encoding="utf-8")


def c3_injected(root: Path) -> dict[str, Any]:
    baseline = runs_fixture.Run(root / "baseline", files={"src/out.txt": "same change\n"}, allowed=["src/"])
    spied = runs_fixture.Run(root / "spied", files={"src/out.txt": "same change\n"}, allowed=["src/"], tick=False)
    spy = Spy(port().GitCloneWorkspace())

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        (workspace / "src").mkdir(exist_ok=True)
        (workspace / "src" / "out.txt").write_text("same change\n", encoding="utf-8")
        return {"summary": "same change", "usage": {"state": "unknown"}}

    result = LoopController(spied.runs, root / "spied" / "workspaces", workspace_port=spy).tick(
        spied.run_id, holder="workspace-port", model=model, verifier_argv=runs_fixture._exists(["src/out.txt"]))
    same = _diff(baseline) == _diff(spied)
    stored = spied.runs.get_run(spied.run_id)["state"]
    ok = (spy.calls == 1 and result.get("status") == "verified" and baseline.result.get("status") == "verified"
          and stored == "verified" and same)
    return case("the-controller-uses-the-injected-backend-and-the-run-is-unchanged", ok,
                {"calls": spy.calls, "status": result.get("status"), "stored": stored, "same_diff": same})


def c4_escape(root: Path) -> dict[str, Any]:
    run = runs_fixture.Run(root / "escape", files={"src/out.txt": "x\n"}, allowed=["src/"], tick=False)
    outside = root / "elsewhere"
    try:
        result = LoopController(run.runs, root / "escape" / "workspaces", workspace_port=Escaping(outside)).tick(
            run.run_id, holder="workspace-port", model=lambda _w, _c: {"summary": "x"},
            verifier_argv=runs_fixture._exists(["src/out.txt"]))
        outcome = f"{result.get('state')}:{(result.get('error') or '')[:120]}"
    except RuntimeError as exc:
        outcome = f"raised:{exc}"
    stored = RunStore(run.runs.root).get_run(run.run_id)
    ok = "workspace_outside_root" in outcome or "workspace_outside_root" in json.dumps(stored, default=str)
    return case("a-workspace-outside-the-root-is-refused", ok and stored.get("state") != "verified",
                {"outcome": outcome, "run_state": stored.get("state")})


def main() -> int:
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="lh-workspace-port-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        builds: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("the-default-backend-keeps-every-guarantee", lambda: c1_default(root / "c1")),
            ("a-backend-that-shares-the-source-is-caught", lambda: c2_unsound(root / "c2")),
            ("the-controller-uses-the-injected-backend-and-the-run-is-unchanged", lambda: c3_injected(root / "c3")),
            ("a-workspace-outside-the-root-is-refused", lambda: c4_escape(root / "c4")),
        ]
        for name, build in builds:
            try:
                results.append(build())
            except Exception as exc:  # a crash or a missing module is a failed exam, never a skip
                results.append(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Workspace port: how a disposable workspace is made, and what every backend must guarantee.

The controller asks a ``WorkspacePort`` for each Attempt's workspace.  The default
``GitCloneWorkspace`` is a full ``git clone --no-local`` of the source checkout,
detached at the base revision, with every remote sealed for push and a refusing
``pre-push`` hook.  A faster backend (snapshots, shared objects, a different
checkout mechanism) may replace it, but only if it keeps the guarantees that
``check_backend`` runs:

- the workspace lies inside the workspace root and is detached at the base;
- pushing through the clone's remotes, or through one the executor adds, does
  not change the source's refs;
- branches and tags made in the workspace do not appear in the source;
- damaging the workspace's object store does not damage the source;
- a target inside a larger checkout runs from the same subtree.

The controller additionally refuses a workspace outside its own root.
"""
from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

try:
    from .platform_ports import ProcessResult, remove_tree
except ImportError:  # direct execution keeps lh_runtime on sys.path
    from platform_ports import ProcessResult, remove_tree  # type: ignore

CONFORMANCE_SCHEMA = "lh-workspace-conformance/v1"
MARKER_SCHEMA = "loop-hybrid-disposable-workspace/v1"
Runner = Callable[[list[str]], ProcessResult]


@dataclass(frozen=True)
class WorkspaceRequest:
    run_id: str
    attempt: int
    base_revision: str
    source_root: Path       # the Git root of the source checkout
    target_subdir: Path     # the target inside it; Path(".") for the whole checkout
    workspace_root: Path
    run: Runner             # the controller's bounded command runner (deadlines, process port)


@dataclass(frozen=True)
class PreparedWorkspace:
    workspace: Path         # where the executor runs: git_root / target_subdir
    git_root: Path          # the workspace's own Git root; removed after the Attempt
    ref: str


class WorkspacePort(Protocol):
    def prepare(self, request: WorkspaceRequest) -> PreparedWorkspace:
        ...


class GitCloneWorkspace:
    """The default backend: a full, push-sealed clone per Attempt."""

    def prepare(self, request: WorkspaceRequest) -> PreparedWorkspace:
        clone_root = request.workspace_root / request.run_id / str(request.attempt)
        clone_root.parent.mkdir(parents=True, exist_ok=True)
        completed = request.run(["git", "clone", "--quiet", "--no-local", str(request.source_root), str(clone_root)])
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or "could not create disposable workspace")
        checkout = request.run(["git", "-C", str(clone_root), "checkout", "--quiet", "--detach", request.base_revision])
        if checkout.returncode != 0:
            raise RuntimeError(checkout.stderr.strip() or "could not checkout base revision")
        workspace = clone_root / request.target_subdir
        if not workspace.is_dir():
            raise RuntimeError("source target subtree is missing from disposable workspace")
        self._seal_push(clone_root, request.run)
        marker = {
            "schema": MARKER_SCHEMA,
            "run_id": request.run_id,
            "attempt": request.attempt,
            "base_revision": request.base_revision,
            "git_root": str(clone_root),
            "target_subdir": request.target_subdir.as_posix(),
            "push_sealed": True,
        }
        (clone_root / ".git" / "lh-disposable-workspace.json").write_text(
            json.dumps(marker, sort_keys=True), encoding="utf-8", newline="")
        return PreparedWorkspace(workspace=workspace, git_root=clone_root,
                                 ref=f"workspace://{request.run_id}/{request.attempt}")

    @staticmethod
    def _seal_push(clone_root: Path, run: Runner) -> None:
        """The clone's remotes point at the local source; nothing may push back through them.

        Every existing remote gets an unusable push URL, and a pre-push hook
        refuses any push, including one to a remote the executor adds later.
        A push that bypasses the hook is caught by the source-refs comparison
        around the executor call.
        """
        remotes = run(["git", "-C", str(clone_root), "remote"])
        for name in remotes.stdout.split():
            sealed = run(["git", "-C", str(clone_root), "remote", "set-url", "--push", name,
                          "lh-push-disabled://disposable-workspace"])
            if sealed.returncode != 0:
                raise RuntimeError("could not seal the disposable workspace remote")
        hook = clone_root / ".git" / "hooks" / "pre-push"
        hook.parent.mkdir(parents=True, exist_ok=True)
        hook.write_text("#!/bin/sh\necho 'lh: pushing from a disposable workspace is refused' >&2\nexit 1\n",
                        encoding="utf-8", newline="\n")
        hook.chmod(0o755)


# -- conformance --------------------------------------------------------------

def _runner(argv: list[str]) -> ProcessResult:
    done = subprocess.run(argv, capture_output=True, text=True, stdin=subprocess.DEVNULL)
    return ProcessResult(tuple(argv), done.returncode, done.stdout, done.stderr)


def _git(repo: Path, *args: str) -> ProcessResult:
    return _runner(["git", "-C", str(repo), *args])


def _source(root: Path, *, subdir: str | None = None) -> tuple[Path, str]:
    root.mkdir(parents=True)
    for args in (("init", "-q"), ("config", "user.email", "workspace@example.invalid"),
                 ("config", "user.name", "Workspace Conformance")):
        _git(root, *args)
    target = root / subdir if subdir else root
    target.mkdir(parents=True, exist_ok=True)
    (target / "README.md").write_text("workspace conformance\n", encoding="utf-8")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    _git(root, "tag", "base-tag")
    return root, _git(root, "rev-parse", "HEAD").stdout.strip()


def _refs(repo: Path) -> str:
    return _git(repo, "for-each-ref", "--format=%(refname) %(objectname)").stdout


def _row(ok: bool, detail: Any) -> dict[str, Any]:
    return {"ok": bool(ok), "detail": detail}


def check_backend(backend: WorkspacePort, root: str | Path) -> dict[str, Any]:
    """Run every guarantee against ``backend``; the object-store probe is destructive and runs last."""
    root = Path(root)
    source, base = _source(root / "source")
    request = WorkspaceRequest(run_id="conformance", attempt=1, base_revision=base, source_root=source,
                               target_subdir=Path("."), workspace_root=root / "workspaces", run=_runner)
    checks: dict[str, dict[str, Any]] = {}
    prepared = backend.prepare(request)
    git_root, workspace = Path(prepared.git_root).resolve(), Path(prepared.workspace).resolve()
    inside = git_root.is_relative_to(request.workspace_root.resolve()) and workspace == git_root
    checks["inside_the_workspace_root"] = _row(inside, {"git_root": str(git_root)})
    detached = _git(workspace, "symbolic-ref", "-q", "HEAD").returncode != 0
    checks["detached_at_the_base"] = _row(detached and _git(workspace, "rev-parse", "HEAD").stdout.strip() == base,
                                          {"detached": detached})
    before = _refs(source)
    (workspace / "change.txt").write_text("workspace change\n", encoding="utf-8")
    _git(workspace, "add", "-A")
    _git(workspace, "-c", "user.email=w@example.invalid", "-c", "user.name=W", "commit", "-qm", "workspace change")
    _git(workspace, "push", "--no-verify", "origin", "HEAD:refs/heads/from-workspace")
    checks["existing_remotes_refuse_push"] = _row(_refs(source) == before, "push through origin, hook bypassed")
    _git(workspace, "remote", "add", "lh-conformance-extra", str(source))
    _git(workspace, "push", "lh-conformance-extra", "HEAD:refs/heads/from-extra-remote")
    checks["added_remotes_refuse_push"] = _row(_refs(source) == before, "push through a remote added by the executor")
    _git(workspace, "branch", "made-in-workspace")
    _git(workspace, "tag", "made-in-workspace")
    checks["branches_stay_in_the_workspace"] = _row(_refs(source) == before, "branch and tag created in the workspace")
    mono, mono_base = _source(root / "monorepo", subdir="pkg/target")
    nested = backend.prepare(WorkspaceRequest(run_id="conformance-nested", attempt=1, base_revision=mono_base,
                                              source_root=mono, target_subdir=Path("pkg/target"),
                                              workspace_root=root / "workspaces", run=_runner))
    prefix = _git(Path(nested.workspace), "rev-parse", "--show-prefix").stdout.strip()
    checks["target_subtree_runs_from_the_same_subtree"] = _row(
        Path(nested.workspace).is_dir() and prefix == "pkg/target/", {"prefix": prefix})
    common = Path(_git(workspace, "rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip())
    for item in [common / "objects" / name for name in ("pack",)] + sorted((common / "objects").glob("[0-9a-f][0-9a-f]")):
        if item.is_dir():
            remove_tree(item)  # object files are read-only; a plain rmtree would silently keep them
    intact = _git(source, "fsck", "--no-progress").returncode == 0 and _git(source, "cat-file", "-e", f"{base}^{{tree}}").returncode == 0
    checks["objects_stay_in_the_workspace"] = _row(intact, {"common_dir": str(common)})
    verdict = "GREEN" if all(row["ok"] for row in checks.values()) else "RED"
    return {"schema": CONFORMANCE_SCHEMA, "verdict": verdict, "checks": checks}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a workspace backend against the guarantees every backend must keep")
    parser.add_argument("--backend", default="workspace_port:GitCloneWorkspace", help="module:Class to check")
    args = parser.parse_args(argv)
    module_name, _, class_name = args.backend.partition(":")
    backend = getattr(importlib.import_module(module_name), class_name)()
    with tempfile.TemporaryDirectory(prefix="lh-workspace-conformance-", ignore_cleanup_errors=True) as raw:
        report = check_backend(backend, Path(raw))
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0 if report["verdict"] == "GREEN" else 1


if __name__ == "__main__":
    raise SystemExit(main())

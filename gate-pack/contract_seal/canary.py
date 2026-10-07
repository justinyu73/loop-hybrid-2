#!/usr/bin/env python3
"""Contract seal canary: a sealed contract cannot change quietly.

Every case drives ``gate-pack/contract_seal/seal.py`` through its command line
against a throwaway git repository, the way a target project would use it.

The scope is read from the repository and the command line, never from the seal
file: a seal that decides what it covers cannot see itself shrink.
"""
from __future__ import annotations

import ast
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
TOOL = HERE / "seal.py"
CHECK_ID = "contract-seal-kit"
SEAL = "docs/contracts/seal.json"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def tool(*args: str, root: Path) -> tuple[int, dict[str, Any]]:
    if not TOOL.is_file():
        raise FileNotFoundError("gate-pack/contract_seal/seal.py is not provided")
    done = subprocess.run([sys.executable, "-B", str(TOOL), *args, "--root", str(root)],
                          capture_output=True, text=True, encoding="utf-8", timeout=120)
    try:
        payload = json.loads(done.stdout)
    except ValueError:
        payload = {"unparsed": done.stdout[-300:], "stderr": done.stderr[-300:]}
    return done.returncode, payload


class Repo:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True)
        self.git("init", "-q")
        self.git("config", "user.email", "seal@example.invalid")
        self.git("config", "user.name", "Seal Canary")
        self.git("config", "core.autocrlf", "false")
        self.write("docs/contracts/alpha-v1.md", "# alpha\n\nstatus: active\n")
        self.write("docs/contracts/beta-v1.md", "# beta\n\nstatus: active\n")
        self.write("docs/active/notes.md", "# notes are not sealed\n")
        self.git("add", "-A")
        self.git("commit", "-qm", "baseline")

    def git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True,
                              text=True, encoding="utf-8").stdout.strip()

    def write(self, rel: str, text: str, *, newline: str = "\n") -> None:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.replace("\n", newline).encode("utf-8"))

    def seal(self) -> tuple[int, dict[str, Any]]:
        return tool("reseal", "--sealed-by", "canary", "--reason", "fixture baseline", root=self.root)

    def verify(self) -> tuple[int, dict[str, Any]]:
        return tool("verify", root=self.root)


def _problems(payload: dict[str, Any]) -> list[str]:
    return sorted({str(row.get("kind")) for row in payload.get("problems", []) if isinstance(row, dict)})


def c1_intact(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    sealed = repo.seal()
    rc, payload = repo.verify()
    covered = sorted(payload.get("covered", []))
    ok = (sealed[0] == 0 and rc == 0 and payload.get("verdict") == "intact"
          and covered == ["docs/contracts/alpha-v1.md", "docs/contracts/beta-v1.md"])
    return case("an-intact-seal-is-green", ok, {"reseal": sealed, "verify": payload})


def c2_edited(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    repo.write("docs/contracts/alpha-v1.md", "# alpha\n\nstatus: retired\n")
    rc, payload = repo.verify()
    ok = rc != 0 and payload.get("verdict") == "broken" and "digest_mismatch" in _problems(payload)
    return case("an-edited-contract-without-a-reseal-is-red", ok, payload)


def c3_deleted(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    repo.git("rm", "-q", "docs/contracts/beta-v1.md")
    rc, payload = repo.verify()
    ok = rc != 0 and payload.get("verdict") == "broken" and "sealed_file_missing" in _problems(payload)
    return case("a-deleted-sealed-contract-is-red", ok, payload)


def c4_added(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    repo.write("docs/contracts/gamma-v1.md", "# gamma\n")  # untracked but not ignored still counts
    rc, payload = repo.verify()
    ok = rc != 0 and payload.get("verdict") == "broken" and "unsealed_file" in _problems(payload)
    return case("a-new-unsealed-contract-is-red", ok, payload)


def c5_shrunk(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    seal_path = repo.root / SEAL
    data = json.loads(seal_path.read_text(encoding="utf-8"))
    data["files"].pop("docs/contracts/beta-v1.md")
    seal_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    rc, payload = repo.verify()
    narrowed = json.loads(seal_path.read_text(encoding="utf-8"))
    narrowed["scope"] = ["docs/contracts/alpha-*.md"]
    narrowed["files"] = {"docs/contracts/alpha-v1.md": data["files"]["docs/contracts/alpha-v1.md"]}
    seal_path.write_text(json.dumps(narrowed, indent=2), encoding="utf-8")
    rc2, payload2 = repo.verify()
    ok = (rc != 0 and "unsealed_file" in _problems(payload)
          and rc2 != 0 and payload2.get("verdict") == "broken")
    return case("a-seal-that-shrinks-is-red", ok, {"dropped_entry": payload, "narrowed_scope": payload2})


def c6_reseal(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    repo.write("docs/contracts/alpha-v1.md", "# alpha\n\nstatus: retired\n")
    repo.write("docs/contracts/gamma-v1.md", "# gamma\n")
    broken = repo.verify()
    resealed = repo.seal()
    rc, payload = repo.verify()
    data = json.loads((repo.root / SEAL).read_text(encoding="utf-8"))
    ok = (broken[0] != 0 and resealed[0] == 0 and rc == 0 and payload.get("verdict") == "intact"
          and data.get("sealed_by") == "canary" and data.get("reason") == "fixture baseline"
          and "docs/contracts/gamma-v1.md" in data.get("files", {}))
    return case("a-reseal-is-recorded-and-turns-green", ok, {"verify": payload, "seal": data})


def c7_line_endings(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.seal()
    before = json.loads((repo.root / SEAL).read_text(encoding="utf-8"))["files"]["docs/contracts/alpha-v1.md"]
    repo.write("docs/contracts/alpha-v1.md", "# alpha\n\nstatus: active\n", newline="\r\n")
    rc, payload = repo.verify()
    ok = rc == 0 and payload.get("verdict") == "intact" and isinstance(before, str) and before.startswith("sha256:")
    return case("crlf-and-lf-checkouts-seal-alike", ok, payload)


def c8_stdlib_only(_root: Path) -> dict[str, Any]:
    if not TOOL.is_file():
        raise FileNotFoundError("gate-pack/contract_seal/seal.py is not provided")
    tree = ast.parse(TOOL.read_text(encoding="utf-8"))
    imported = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported |= {(node.module or "").split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    stdlib = set(sys.stdlib_module_names) | {"__future__"}
    extra = sorted(imported - stdlib)
    return case("the-tool-imports-only-the-standard-library", not extra, {"non_stdlib": extra})


def main() -> int:
    builds = [
        ("an-intact-seal-is-green", c1_intact),
        ("an-edited-contract-without-a-reseal-is-red", c2_edited),
        ("a-deleted-sealed-contract-is-red", c3_deleted),
        ("a-new-unsealed-contract-is-red", c4_added),
        ("a-seal-that-shrinks-is-red", c5_shrunk),
        ("a-reseal-is-recorded-and-turns-green", c6_reseal),
        ("crlf-and-lf-checkouts-seal-alike", c7_line_endings),
        ("the-tool-imports-only-the-standard-library", c8_stdlib_only),
    ]
    with tempfile.TemporaryDirectory(prefix="contract-seal-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        results = [guarded(name, lambda build=build, name=name: build(root / name)) for name, build in builds]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Decision registry canary: the exam paper exists before the answer.

Every case drives ``gate-pack/decision_registry/registry.py`` through its
command line against a throwaway git repository, the way a target project
would use it.  Nothing here touches this repository's own history.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
TOOL = HERE / "registry.py"
CHECK_ID = "decision-registry"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a crash or a missing tool is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {str(exc)[:300]}")


def tool(*args: str, root: Path) -> tuple[int, dict[str, Any]]:
    if not TOOL.is_file():
        raise FileNotFoundError("gate-pack/decision_registry/registry.py is not provided")
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
        self.git("config", "user.email", "registry@example.invalid")
        self.git("config", "user.name", "Registry Canary")
        self.write("decisions/policy.json", json.dumps(
            {"schema": "lh-decision-policy/v1", "guarded_prefixes": ["deploy/", "governance/"]}))
        self.write("README.md", "fixture\n")
        self.commit("chore: baseline")

    def git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True,
                              text=True, encoding="utf-8").stdout.strip()

    def write(self, relative: str, text: str) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
        return path

    def commit(self, message: str) -> str:
        self.git("add", "-A")
        self.git("commit", "-qm", message, "--allow-empty")
        return self.git("rev-parse", "HEAD")

    def row(self, decision_id: str, **overrides: Any) -> Path:
        value = {"decision_id": decision_id, "question": "how should the deploy config change",
                 "candidates": ["keep", "change"],
                 "acceptance": [{"id": "config-present", "argv": [sys.executable, "-B", "-c",
                                 "from pathlib import Path; assert Path('deploy/app.conf').read_text().strip() == 'ok'"],
                                 "expect_exit": 0}],
                 "surface": ["deploy/"], "artefacts": []}
        value.update(overrides)
        path = self.root.parent / f"{self.root.name}-{decision_id}.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def register(self, decision_id: str, **overrides: Any) -> tuple[int, dict[str, Any]]:
        return tool("register", "--row", str(self.row(decision_id, **overrides)), root=self.root)


def c1_refusals(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    observed = {
        "no-probes": repo.register("D-1", acceptance=[]),
        "no-surface": repo.register("D-2", surface=None),
        "one-candidate": repo.register("D-3", candidates=["only"]),
        "unknown-supersede": repo.register("D-4", supersedes="D-missing"),
        "missing-artefact": repo.register("D-5", artefacts=["docs/absent.md"]),
    }
    first = repo.register("D-6")
    duplicate = repo.register("D-6")
    expected = {"no-probes": "acceptance_probes_required", "no-surface": "surface_required",
                "one-candidate": "candidates_required", "unknown-supersede": "supersedes_unknown",
                "missing-artefact": "artefact_missing"}
    reasons = {name: payload.get("reason") for name, (_code, payload) in observed.items()}
    ok = (reasons == expected and all(code != 0 for code, _ in observed.values())
          and first[0] == 0 and duplicate[0] != 0 and duplicate[1].get("reason") == "decision_id_duplicate")
    return case("registration-needs-probes-and-surface", ok,
                {"reasons": reasons, "first": first[1].get("status"), "duplicate": duplicate[1].get("reason")})


def c2_artefacts(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.write("docs/brief.md", "approved brief\n")
    registered = repo.register("D-10", artefacts=["docs/brief.md"])
    clean = tool("verify", root=repo.root)
    repo.write("docs/brief.md", "edited after the vote\n")
    edited = tool("verify", root=repo.root)
    rebound = repo.register("D-11", artefacts=["docs/brief.md"], supersedes="D-10")
    after = tool("verify", root=repo.root)
    ok = (registered[0] == 0 and clean[0] == 0 and edited[0] != 0 and rebound[0] == 0 and after[0] == 0)
    return case("artefacts-are-bound-at-registration", ok,
                {"clean": clean[1].get("status"), "edited": edited[1], "after_supersede": after[1].get("status")})


def c3_append_only(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.register("D-20")
    repo.register("D-21")
    ledger = repo.root / "decisions" / "registrations.jsonl"
    lines = ledger.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    row["question"] = "rewritten after the fact"
    body = {key: value for key, value in row.items() if key != "row_digest"}
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    row["row_digest"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    lines[0] = json.dumps(row, ensure_ascii=False, sort_keys=True)
    ledger.write_text("\n".join(lines) + "\n", encoding="utf-8")
    edited = tool("verify", root=repo.root)
    return case("ledger-is-append-only", edited[0] != 0, edited[1])


def _orphan_shas(payload: dict[str, Any]) -> list[str]:
    return [row.get("sha") for row in payload.get("orphans", [])]


def c4_orphan(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    start = repo.git("rev-parse", "HEAD")
    repo.write("src/app.py", "print('benign')\n")
    benign = repo.commit("feat: benign change")
    repo.write("deploy/app.conf", "ok\n")
    guarded_sha = repo.commit("feat: change deploy config")
    code, payload = tool("orphans", "--range", f"{start}..HEAD", root=repo.root)
    ok = code != 0 and _orphan_shas(payload) == [guarded_sha] and benign not in _orphan_shas(payload)
    return case("guarded-commit-without-decision-is-an-orphan", ok, payload)


def c5_c6_wash(root: Path) -> list[dict[str, Any]]:
    repo = Repo(root)
    start = repo.git("rev-parse", "HEAD")
    repo.write("deploy/app.conf", "ok\n")
    early = repo.commit("feat: D-30 deploy config before registration")
    repo.register("D-30")
    repo.write("deploy/app.conf", "ok\n\n")
    late = repo.commit("feat: D-30 register and touch deploy again")
    code, payload = tool("orphans", "--range", f"{start}..HEAD", root=repo.root)
    shas = _orphan_shas(payload)
    fifth = case("a-later-commit-cannot-wash-an-earlier-orphan", code != 0 and early in shas and late not in shas,
                 {"orphans": shas, "early": early, "late": late})
    sixth = case("decision-must-be-registered-in-the-commits-own-tree", early in shas,
                 {"cited_before_registration": early, "orphans": shas})
    return [fifth, sixth]


def c7_squash(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.register("D-40")
    repo.register("D-41")
    repo.commit("chore: register D-40 and D-41")
    start = repo.git("rev-parse", "HEAD")
    repo.write("deploy/app.conf", "ok\n")
    squashed = repo.commit("Merge pull request #1\n\n* feat: D-40 change deploy config\n")
    repo.write("governance/rules.md", "rule\n")
    prose = repo.commit("feat: governance rules\n\nThis relates to D-41 but does not name it as a subject.\n")
    code, payload = tool("orphans", "--range", f"{start}..HEAD", root=repo.root)
    shas = _orphan_shas(payload)
    ok = squashed not in shas and prose in shas and code != 0
    return case("squash-subject-lines-count-prose-does-not", ok, {"orphans": shas, "squashed": squashed, "prose": prose})


def c8_c9_readback(root: Path) -> list[dict[str, Any]]:
    repo = Repo(root)
    repo.register("D-50")
    repo.commit("chore: register D-50")
    start = repo.git("rev-parse", "HEAD")
    repo.write("deploy/app.conf", "ok\n")
    repo.commit("feat: D-50 change deploy config")
    inside = tool("readback", "--decision", "D-50", "--range", f"{start}..HEAD", root=repo.root)
    ledger_before = (repo.root / "decisions" / "registrations.jsonl").read_bytes()
    repo.write("governance/rules.md", "unannounced\n")
    repo.commit("feat: D-50 also edit governance")
    drift = tool("readback", "--decision", "D-50", "--range", f"{start}..HEAD", root=repo.root)
    eighth = case("readback-surface-is-default-deny",
                  inside[1].get("drift") == [] and drift[0] != 0 and "governance/rules.md" in json.dumps(drift[1].get("drift")),
                  {"inside": inside[1].get("drift"), "drift": drift[1].get("drift")})
    repo.write("deploy/app.conf", "broken\n")
    failing = tool("readback", "--decision", "D-50", "--range", f"{start}..HEAD~1", root=repo.root)
    ledger_after = (repo.root / "decisions" / "registrations.jsonl").read_bytes()
    ninth = case("disposition-is-derived-by-running-probes",
                 inside[1].get("disposition") == "passing" and failing[1].get("disposition") == "failing"
                 and ledger_before == ledger_after,
                 {"passing": inside[1].get("disposition"), "failing": failing[1].get("disposition"),
                  "ledger_unchanged": ledger_before == ledger_after})
    return [eighth, ninth]


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="decision-registry-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("registration-needs-probes-and-surface", lambda: c1_refusals(root / "c1")),
            guarded("artefacts-are-bound-at-registration", lambda: c2_artefacts(root / "c2")),
            guarded("ledger-is-append-only", lambda: c3_append_only(root / "c3")),
            guarded("guarded-commit-without-decision-is-an-orphan", lambda: c4_orphan(root / "c4")),
        ]
        for builder, names, folder in ((c5_c6_wash, ("a-later-commit-cannot-wash-an-earlier-orphan",
                                                     "decision-must-be-registered-in-the-commits-own-tree"), "c5"),):
            try:
                results.extend(builder(root / folder))
            except Exception as exc:
                results.extend(case(name, False, f"{type(exc).__name__}: {str(exc)[:300]}") for name in names)
        results.append(guarded("squash-subject-lines-count-prose-does-not", lambda: c7_squash(root / "c7")))
        try:
            results.extend(c8_c9_readback(root / "c8"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {str(exc)[:300]}"
            results.extend([case("readback-surface-is-default-deny", False, detail),
                            case("disposition-is-derived-by-running-probes", False, detail)])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures],
                      "known_gaps_open": ["the ledger is writable by the agent it governs; this makes bypass "
                                          "visible in a diff, it does not make it impossible"]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

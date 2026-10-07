#!/usr/bin/env python3
"""Red proof canary: an exam that was never seen red proves nothing.

``registry.py red-proof --decision ID --at REV`` runs the decision's acceptance
probes in a disposable clone at REV and records, per probe, that it ran and
failed.  A probe that already passes at REV is not recorded as a proof.  A probe
covering behaviour that already exists may be exempted at registration, with a
reason that readback shows.  ``readback`` reports each probe as ``proven``,
``exempt`` or ``missing``; with ``require_red_proof`` in the policy, missing is
red.  Approval never requires green: the red is what makes the gap real.

Every case drives the command line against a throwaway git repository.
"""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from canary import Repo, case, guarded, tool  # noqa: E402

CHECK_ID = "decision-registry-red-proof"
PROOFS = "decisions/red-proofs.jsonl"


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _proof_rows(repo: Repo) -> list[dict[str, Any]]:
    path = repo.root / PROOFS
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()] if path.is_file() else []


def _red_states(payload: dict[str, Any]) -> dict[str, Any]:
    return {row.get("id"): row.get("red_proof") for row in payload.get("probes", [])}


def _baseline_with_decision(repo: Repo, decision_id: str, **overrides: Any) -> str:
    registered = repo.register(decision_id, **overrides)
    if registered[0] != 0:
        raise AssertionError(registered)
    return repo.commit(f"register {decision_id}")


def _implement(repo: Repo, decision_id: str) -> str:
    repo.write("deploy/app.conf", "ok\n")
    return repo.commit(f"deploy: {decision_id} config")


def c1_proven(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    exam = _baseline_with_decision(repo, "D-1")
    proved = tool("red-proof", "--decision", "D-1", "--at", exam, root=repo.root)
    implemented = _implement(repo, "D-1")
    back = tool("readback", "--decision", "D-1", "--range", f"{exam}..{implemented}", root=repo.root)
    rows = _proof_rows(repo)
    ok = (proved[0] == 0 and len(rows) == 1 and rows[0].get("verdict") == "red"
          and back[0] == 0 and _red_states(back[1]) == {"config-present": "proven"})
    return case("a-probe-red-at-the-exam-commit-is-proven", ok, {"proof": proved[1], "readback": back[1]})


def c2_green_refused(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    _baseline_with_decision(repo, "D-2")
    implemented = _implement(repo, "D-2")
    proved = tool("red-proof", "--decision", "D-2", "--at", implemented, root=repo.root)
    ok = proved[0] != 0 and proved[1].get("reason") == "probe_green_at_rev" and not _proof_rows(repo)
    return case("a-probe-already-green-is-not-a-proof", ok, proved[1])


def c3_tamper(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    exam = _baseline_with_decision(repo, "D-3")
    tool("red-proof", "--decision", "D-3", "--at", exam, root=repo.root)
    clean = tool("verify", root=repo.root)
    path = repo.root / PROOFS
    row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    row["exit"] = 0
    path.write_text(json.dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    tampered = tool("verify", root=repo.root)
    ok = clean[0] == 0 and tampered[0] != 0
    return case("a-tampered-proof-ledger-fails-verify", ok, {"clean": clean[1], "tampered": tampered[1]})


def c4_required(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    repo.write("decisions/policy.json", json.dumps({"schema": "lh-decision-policy/v1",
                                                     "guarded_prefixes": ["deploy/", "governance/"],
                                                     "require_red_proof": True}))
    repo.commit("policy: require red proof")
    exam = _baseline_with_decision(repo, "D-4")
    implemented = _implement(repo, "D-4")
    missing = tool("readback", "--decision", "D-4", "--range", f"{exam}..{implemented}", root=repo.root)
    tool("red-proof", "--decision", "D-4", "--at", exam, root=repo.root)
    proven = tool("readback", "--decision", "D-4", "--range", f"{exam}..{implemented}", root=repo.root)
    ok = (missing[0] != 0 and _red_states(missing[1]) == {"config-present": "missing"}
          and proven[0] == 0 and _red_states(proven[1]) == {"config-present": "proven"})
    return case("a-required-proof-that-is-missing-is-red", ok, {"missing": missing[1], "proven": proven[1]})


def c5_exempt(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    probe = {"id": "baseline-present", "argv": [sys.executable, "-B", "-c",
                                                "from pathlib import Path; assert Path('README.md').is_file()"],
             "expect_exit": 0}
    blank = repo.register("D-5", acceptance=[{**probe, "red_proof": {"exempt": "  "}}])
    reason = "covers behaviour that already exists; proven by mutation instead"
    exam = _baseline_with_decision(repo, "D-6", acceptance=[{**probe, "red_proof": {"exempt": reason}}])
    proved = tool("red-proof", "--decision", "D-6", "--at", exam, root=repo.root)
    back = tool("readback", "--decision", "D-6", "--range", f"{exam}..{exam}", root=repo.root)
    shown = [row.get("exempt_reason") for row in back[1].get("probes", [])]
    ok = (blank[0] != 0 and blank[1].get("reason") == "red_proof_exempt_reason_required"
          and proved[0] == 0 and not _proof_rows(repo)
          and _red_states(back[1]) == {"baseline-present": "exempt"} and shown == [reason])
    return case("an-exemption-needs-a-reason-and-is-shown", ok, {"blank": blank[1], "proof": proved[1], "readback": back[1]})


def c6_changed_probe(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    exam = _baseline_with_decision(repo, "D-7")
    tool("red-proof", "--decision", "D-7", "--at", exam, root=repo.root)
    ledger = repo.root / "decisions" / "registrations.jsonl"
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    previous = None
    for row in rows:  # rewrite the probe and recompute the chain, so only the proof binding can notice
        if row["decision_id"] == "D-7":
            row["acceptance"][0]["argv"] = [sys.executable, "-B", "-c", "pass"]
        row["prev_digest"] = previous
        body = {key: value for key, value in row.items() if key != "row_digest"}
        row["row_digest"] = _digest(body)
        previous = row["row_digest"]
    ledger.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    implemented = _implement(repo, "D-7")
    back = tool("readback", "--decision", "D-7", "--range", f"{exam}..{implemented}", root=repo.root)
    ok = _red_states(back[1]) == {"config-present": "missing"}
    return case("a-proof-does-not-survive-a-changed-probe", ok, back[1])


def c7_full_sha(root: Path) -> dict[str, Any]:
    repo = Repo(root)
    exam = _baseline_with_decision(repo, "D-8")
    _implement(repo, "D-8")
    proved = tool("red-proof", "--decision", "D-8", "--at", "HEAD~1", root=repo.root)
    rows = _proof_rows(repo)
    ok = proved[0] == 0 and len(rows) == 1 and rows[0].get("commit") == exam and len(exam) == 40
    return case("a-proof-binds-the-full-commit-sha", ok, {"exam": exam, "rows": rows})


def main() -> int:
    builds = [
        ("a-probe-red-at-the-exam-commit-is-proven", c1_proven),
        ("a-probe-already-green-is-not-a-proof", c2_green_refused),
        ("a-tampered-proof-ledger-fails-verify", c3_tamper),
        ("a-required-proof-that-is-missing-is-red", c4_required),
        ("an-exemption-needs-a-reason-and-is-shown", c5_exempt),
        ("a-proof-does-not-survive-a-changed-probe", c6_changed_probe),
        ("a-proof-binds-the-full-commit-sha", c7_full_sha),
    ]
    with tempfile.TemporaryDirectory(prefix="red-proof-", ignore_cleanup_errors=True) as raw:
        root = Path(raw).resolve()
        results = [guarded(name, lambda build=build, name=name: build(root / name)) for name, build in builds]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Candidate review v2: an independent verifier must review, not merely exit 0.

A delivery contract may carry a ``candidate_review`` policy: an approved spec,
named requirements, and optional caller context, all pinned by digest.  The
verifier then has to return a closed review bound to this exact candidate,
base, checks, scope, and identity.  The engine derives the verdict from the
review itself, seals the raw bytes, and re-verifies the proof whenever delivery
is read back.  Exit code 0 without a review never reaches the external action
port.

Every case runs offline in a tempdir with a counting action adapter; no model,
network, or credential is involved.
"""
from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "tests"))

import delivery_contract as engine  # noqa: E402
import external_action_port as eap  # noqa: E402
import native_delivery_fixture  # noqa: E402
from controller import LoopController  # noqa: E402
from external_verdict import VerdictStore  # noqa: E402
from goal_store import GoalStore  # noqa: E402
from p7_fence_fixture import fixture_command_runner  # noqa: E402
from run_store import RunStore  # noqa: E402

CHECK_ID = "lh-candidate-review"
SPEC_TEXT = "double(n) returns 2*n for every integer, including negative integers.\n"
CANDIDATE = "def double(n):\n    return n * 2\n"


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def guarded(case_id: str, action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:  # a missing API or a crash is a failed exam, never a skip
        return case(case_id, False, f"{type(exc).__name__}: {exc}")


def api(name: str) -> Callable[..., Any]:
    value = getattr(engine, name, None)
    if not callable(value):
        raise AssertionError(f"delivery_contract.{name} is not provided")
    return value


def reason_of(action: Callable[[], Any]) -> str | None:
    """The named refusal, or None when the action was accepted."""
    try:
        action()
    except engine.DeliveryUnitError as exc:
        return exc.reason
    except ValueError as exc:
        return f"untyped:{exc}"
    return None


def sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def ref(path: Path) -> dict[str, str]:
    return {"path": str(path.resolve()), "content_digest": sha256(path.read_bytes())}


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    return path


def policy(root: Path, *, requirements: list[dict[str, str]] | None = None) -> dict[str, Any]:
    spec = write(root / "approved" / "requirements.txt", SPEC_TEXT)
    return {
        "schema": "lh-candidate-review-contract/v2",
        "spec_ref": ref(spec),
        "requirements": requirements or [
            {"id": "double", "text": "double(n) = 2*n for positive, zero, and negative integers"},
        ],
        "caller_context_refs": [],
    }


IDENTITY = {"goal_id": "g", "goal_revision": 1, "node_id": "n", "unit_id": "u", "run_id": "r",
            "attempt": 1, "fence": 1, "base_sha": "b" * 40}


def context(pol: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    values = {"candidate_digest": "sha256:" + "c" * 64, "base_sha": "b" * 40,
              "checks_digest": "sha256:" + "d" * 64, "scope": {"allowed_paths": ["src/"]},
              "identity": dict(IDENTITY)}
    values.update(overrides)
    return api("candidate_review_context")(pol, **values)


def review(ctx: dict[str, Any], *, blocking: bool = False, suggestion: bool = False) -> dict[str, Any]:
    finding = {"id": "negative-double" if blocking else "double-style", "blocking": blocking,
               "requirement_id": "double",
               "condition": "negative integer input" if blocking else "optional readability",
               "location": "src/m1.py:double", "impact": "wrong result" if blocking else "style only",
               "evidence": ["double(-2) returned 0; expected -4" if blocking else "behavior is unchanged"]}
    return {
        "schema": "lh-candidate-review-result/v2",
        "status": "completed",
        "context_digest": ctx["context_digest"],
        "candidate_digest": ctx["candidate_digest"],
        "base_sha": ctx["base_sha"],
        "checks_digest": ctx["checks_digest"],
        "requirements": [{"id": "double", "verdict": "RED" if blocking else "GREEN",
                          "evidence": ["inspected positive, zero, and negative branches"]}],
        "scope": {"verdict": "GREEN", "evidence": ["only src/m1.py changed inside src/"]},
        "findings": [finding] if blocking or suggestion else [],
    }


# -- 1-4: the closed contract ------------------------------------------------

def c1_policy_is_closed(root: Path) -> dict[str, Any]:
    validate = api("validate_candidate_review_policy")
    base = policy(root)
    empty = write(root / "approved" / "empty.txt", "   \n")
    extra_refs = [ref(write(root / "approved" / f"ctx-{index}.txt", f"caller {index}\n")) for index in range(17)]

    def mutate(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        value = copy.deepcopy(base)
        change(value)
        return value

    expectations = {
        "extra-field": (mutate(lambda p: p.update(approved=True)), "candidate_review_policy_invalid"),
        "wrong-schema": (mutate(lambda p: p.update(schema="lh-candidate-review-contract/v1")),
                         "candidate_review_policy_invalid"),
        "spec-digest-mismatch": (mutate(lambda p: p["spec_ref"].update(content_digest="sha256:" + "0" * 64)),
                                 "candidate_review_ref_digest_mismatch"),
        "relative-spec-path": (mutate(lambda p: p["spec_ref"].update(path="approved/requirements.txt")),
                               "candidate_review_ref_unreadable"),
        "empty-spec": (mutate(lambda p: p.update(spec_ref=ref(empty))), "candidate_review_policy_ref_empty"),
        "too-many-context-refs": (mutate(lambda p: p.update(caller_context_refs=extra_refs)),
                                  "candidate_review_context_refs_invalid"),
        "no-requirements": (mutate(lambda p: p.update(requirements=[])), "candidate_review_requirements_missing"),
        "duplicate-requirement": (mutate(lambda p: p.update(requirements=[{"id": "a", "text": "x"}, {"id": "a", "text": "y"}])),
                                  "candidate_review_requirement_duplicate"),
        "reserved-scope-id": (mutate(lambda p: p.update(requirements=[{"id": "scope", "text": "x"}])),
                              "candidate_review_requirement_duplicate"),
    }
    observed = {name: reason_of(lambda value=value: validate(value)) for name, (value, _) in expectations.items()}
    symlink = "unavailable"
    link = root / "approved" / "linked.txt"
    try:
        link.symlink_to(root / "approved" / "requirements.txt")
    except OSError:
        pass
    else:
        symlink = reason_of(lambda: validate(mutate(lambda p: p.update(spec_ref=ref(link)))))
    accepted = reason_of(lambda: validate(base)) is None
    ok = (accepted and all(observed[name] == expected for name, (_, expected) in expectations.items())
          and symlink in {"unavailable", "candidate_review_ref_unreadable"})
    return case("review-policy-is-closed", ok, {"accepted": accepted, "observed": observed, "symlink": symlink})


def c2_context_binds_the_candidate(root: Path) -> dict[str, Any]:
    pol = policy(root)
    first = context(pol)
    variants = {
        "candidate": context(pol, candidate_digest="sha256:" + "e" * 64),
        "base": context(pol, base_sha="a" * 40),
        "checks": context(pol, checks_digest="sha256:" + "f" * 64),
        "scope": context(pol, scope={"allowed_paths": ["docs/"]}),
        "identity": context(pol, identity={**IDENTITY, "attempt": 2}),
    }
    other = root / "other"
    write(other / "approved" / "requirements.txt", SPEC_TEXT.replace("negative", "nonnegative"))
    moved = copy.deepcopy(pol)
    moved["spec_ref"] = ref(other / "approved" / "requirements.txt")
    variants["spec"] = context(moved)
    distinct = {name: value["context_digest"] != first["context_digest"] for name, value in variants.items()}
    recomputed = first["context_digest"] == engine.digest_json({k: v for k, v in first.items() if k != "context_digest"})
    big = write(root / "approved" / "big.txt", "x" * 300_000)
    oversized = copy.deepcopy(pol)
    oversized["caller_context_refs"] = [ref(big)]
    limit = reason_of(lambda: context(oversized))
    ok = all(distinct.values()) and recomputed and first.get("spec_text") == SPEC_TEXT and limit == "candidate_review_context_limit"
    return case("review-context-binds-the-candidate", ok,
                {"distinct": distinct, "recomputed": recomputed, "limit": limit})


def c3_result_is_closed_and_bound(root: Path) -> dict[str, Any]:
    validate = api("validate_candidate_review_result")
    load = api("load_candidate_review_json")
    pol = policy(root)
    ctx = context(pol)
    two = policy(root, requirements=[{"id": "double", "text": "doubles"}, {"id": "total", "text": "is total"}])
    two_ctx = context(two)

    def mutated(change: Callable[[dict[str, Any]], None], *, ctx_used: dict[str, Any] = ctx,
                blocking: bool = False, suggestion: bool = False) -> str | None:
        value = review(ctx_used, blocking=blocking, suggestion=suggestion)
        change(value)
        return reason_of(lambda: validate(value, ctx_used))

    def red_without_blocking(value: dict[str, Any]) -> None:
        value["findings"][0]["blocking"] = False

    observed = {
        "extra-field": mutated(lambda v: v.update(approved=True)),
        "missing-requirement": mutated(lambda v: None, ctx_used=two_ctx),
        "foreign-context": mutated(lambda v: v.update(context_digest="sha256:" + "9" * 64)),
        "candidate-mismatch": mutated(lambda v: v.update(candidate_digest="sha256:" + "8" * 64)),
        "base-mismatch": mutated(lambda v: v.update(base_sha="7" * 40)),
        "checks-mismatch": mutated(lambda v: v.update(checks_digest="sha256:" + "6" * 64)),
        "duplicate-finding": mutated(lambda v: v["findings"].append(copy.deepcopy(v["findings"][0])), suggestion=True),
        "unknown-requirement": mutated(lambda v: v["findings"][0].update(requirement_id="nope"), suggestion=True),
        "red-without-blocking-finding": mutated(red_without_blocking, blocking=True),
    }
    expected = {
        "extra-field": "candidate_review_content_invalid",
        "missing-requirement": "candidate_review_requirement_coverage_invalid",
        "foreign-context": "candidate_review_context_digest_mismatch",
        "candidate-mismatch": "candidate_review_candidate_digest_mismatch",
        "base-mismatch": "candidate_review_base_sha_mismatch",
        "checks-mismatch": "candidate_review_checks_digest_mismatch",
        "duplicate-finding": "candidate_review_finding_duplicate",
        "unknown-requirement": "candidate_review_finding_requirement_invalid",
        "red-without-blocking-finding": "candidate_review_blocking_finding_missing",
    }
    raw = json.dumps(review(ctx))
    duplicate_key = reason_of(lambda: load(raw[:-1] + ', "status": "completed"}'))
    ok = observed == expected and duplicate_key == "candidate_review_duplicate_key" and load(raw) == review(ctx)
    return case("review-result-is-closed-and-bound", ok, {"observed": observed, "duplicate_key": duplicate_key})


def c4_verdict_is_derived(root: Path) -> dict[str, Any]:
    validate = api("validate_candidate_review_result")
    ctx = context(policy(root))
    verdicts = {
        "clean": validate(review(ctx), ctx),
        "suggestion-only": validate(review(ctx, suggestion=True), ctx),
        "blocking": validate(review(ctx, blocking=True), ctx),
    }
    return case("review-verdict-is-derived-not-reported",
                verdicts == {"clean": "GREEN", "suggestion-only": "GREEN", "blocking": "RED"}, verdicts)


# -- 5-8, 10: the RunStore delivery path ---------------------------------------

class CountingAdapter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def perform(self, op_key: str, request: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(op_key)
        return {"operation_key": op_key, "external_id": f"action-{len(self.calls)}"}


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _repo(root: Path) -> tuple[Path, str]:
    source = root / "source"
    source.mkdir(parents=True)
    _git("init", "-q", cwd=source)
    _git("config", "user.email", "review@example.invalid", cwd=source)
    _git("config", "user.name", "Review Canary", cwd=source)
    write(source / "baseline.txt", "baseline\n")
    _git("add", "baseline.txt", cwd=source)
    _git("commit", "-qm", "baseline", cwd=source)
    return source, _git("rev-parse", "HEAD", cwd=source)


CHECK_CANDIDATE = ("from pathlib import Path; d = {}; exec(Path('src/m1.py').read_text(encoding='utf-8'), d); "
                   "assert d['double'](-2) == -4 and d['double'](0) == 0 and d['double'](2) == 4")


def _reviewer(mode: str) -> list[str]:
    """A verifier program; ``mode`` decides what it prints after checking the file."""
    builder = (
        "def doc(c, blocking):\n"
        "    f = {'id': 'negative-double', 'blocking': True, 'requirement_id': 'double',\n"
        "         'condition': 'negative integer input', 'location': 'src/m1.py:double',\n"
        "         'impact': 'wrong result', 'evidence': ['reviewer marked it blocking']}\n"
        "    return {'schema': 'lh-candidate-review-result/v2', 'status': 'completed',\n"
        "            'context_digest': c['context_digest'], 'candidate_digest': c['candidate_digest'],\n"
        "            'base_sha': c['base_sha'], 'checks_digest': c['checks_digest'],\n"
        "            'requirements': [{'id': 'double', 'verdict': 'RED' if blocking else 'GREEN',\n"
        "                              'evidence': ['inspected positive, zero, and negative branches']}],\n"
        "            'scope': {'verdict': 'GREEN', 'evidence': ['only src/m1.py changed inside src/']},\n"
        "            'findings': [f] if blocking else []}\n"
    )
    tail = {
        "silent": "",
        "green": "r = json.load(sys.stdin); print(json.dumps({'verdict': 'GREEN', 'review': doc(r['candidate_review_context'], False)}))\n",
        "blocking": "r = json.load(sys.stdin); print(json.dumps({'verdict': 'GREEN', 'review': doc(r['candidate_review_context'], True)}))\n",
    }[mode]
    program = "import json, sys\n" + CHECK_CANDIDATE.replace("; ", "\n") + "\n" + builder + tail
    return [sys.executable, "-B", "-c", program]


@contextlib.contextmanager
def _sealing_with(review_policy: dict[str, Any] | None):
    """Seal the fixture's contract with a review policy (fixture-only injection)."""
    original = native_delivery_fixture.contract_engine.seal_contract
    if review_policy is not None:
        native_delivery_fixture.contract_engine.seal_contract = (
            lambda body: original({**body, "candidate_review": copy.deepcopy(review_policy)}))
    try:
        yield
    finally:
        native_delivery_fixture.contract_engine.seal_contract = original


def _run(root: Path, name: str, *, review_policy: dict[str, Any] | None, mode: str) -> dict[str, Any]:
    source, base = _repo(root / name)
    runs = RunStore(root / name / "runs", command_runner=fixture_command_runner)
    goals = GoalStore(root / name / "goals")
    checks = [{"id": "source-check", "commands": [{
        "id": "candidate-behavior", "argv": [sys.executable, "-B", "-c", CHECK_CANDIDATE],
        "cwd": "${WORKTREE}", "expect_exit": 0, "timeout_seconds": 30}], "required_receipts": ["executor"]}]
    with _sealing_with(review_policy):
        bundle = native_delivery_fixture.make_native_run(
            runs, source, base, name, "review", checks, _reviewer(mode), ["src/"], 1, phase="async",
            goal={"goal_id": name, "feature_contract": "candidate review fixture"}, run_id=name)
    goal = runs.get_run(bundle["run_id"])["goal"]
    event = goals.record_event(event_id=f"{name}-event", source="candidate-review-canary",
                               event_type="goal_candidate",
                               payload={"goal_id": name, "revision": bundle["contract"]["goal"]["revision"]})
    goals.create_candidate(event["event_key"], goal_id=name, campaign_id="candidate-review",
                           stage_id="review", goal=goal, revision=bundle["contract"]["goal"]["revision"])
    goals.activate_with_run(name, bundle["run_id"], event_key=event["event_key"])

    def model(workspace: Path, _capsule: dict[str, Any]) -> dict[str, Any]:
        write(workspace / "src" / "m1.py", CANDIDATE)
        return {"summary": "bounded candidate", "usage": {"state": "unknown"}}

    adapter = CountingAdapter()
    result = LoopController(runs, root / name / "workspaces").tick_async(
        bundle["run_id"], holder=f"{name}-holder", model=model, verdict_store=VerdictStore(root / name / "verdicts"),
        action_ledger=eap.ActionLedger(root / name / "actions"), adapter=adapter, action_id="deliver")
    source_delivery = runs.verify_delivery(bundle["run_id"], phase="source")
    delivery = result.get("delivery") if isinstance(result.get("delivery"), dict) else {}
    return {"runs": runs, "run_id": bundle["run_id"], "result": result, "adapter_calls": len(adapter.calls),
            "source": source_delivery, "verifier_reason": str(delivery.get("reason"))}


def c5_exit_zero_without_review(root: Path) -> dict[str, Any]:
    run = _run(root, "silent", review_policy=policy(root / "silent-policy"), mode="silent")
    ok = (run["adapter_calls"] == 0 and run["source"]["verdict"] == "RED"
          and run["verifier_reason"].startswith("delivery_independent_review_invalid"))
    return case("exit-zero-without-review-never-reaches-the-action-port", ok,
                {"adapter_calls": run["adapter_calls"], "source": run["source"].get("reason"),
                 "verifier_reason": run["verifier_reason"], "status": run["result"].get("status")})


def c6_c7_sealed_review(root: Path) -> list[dict[str, Any]]:
    pol = policy(root / "green-policy")
    run = _run(root, "green", review_policy=pol, mode="green")
    proof = (run["runs"].delivery_evidence(run["run_id"], phase="source") or {}).get("verifier") or {}
    review_ref = proof.get("review_ref") or {}
    raw_path = Path(review_ref["path"]) if review_ref.get("path") else None
    raw_ok = raw_path is not None and raw_path.is_file() and sha256(raw_path.read_bytes()) == review_ref.get("content_digest")
    bound = ((proof.get("candidate_review_context") or {}).get("candidate_digest")
             == (proof.get("snapshot") or {}).get("source_before") is not None)
    sixth = case("sealed-review-reaches-the-action-port-once",
                 run["result"].get("status") == "awaiting_external_verdict" and run["adapter_calls"] == 1
                 and run["source"]["verdict"] == "GREEN" and raw_ok and bound,
                 {"status": run["result"].get("status"), "adapter_calls": run["adapter_calls"],
                  "source": run["source"].get("reason"), "raw_ok": raw_ok, "bound": bound})
    if raw_path is None:
        return [sixth, case("tampered-review-or-moved-spec-turns-delivery-red", False, "no sealed review to tamper")]
    original = raw_path.read_bytes()
    raw_path.write_bytes(original + b" ")
    tampered = run["runs"].verify_delivery(run["run_id"], phase="source")
    raw_path.write_bytes(original)
    restored = run["runs"].verify_delivery(run["run_id"], phase="source")
    # The approved spec is pinned by digest, so editing it invalidates the
    # contract binding itself before any proof is read.
    spec = Path(pol["spec_ref"]["path"])
    spec.write_bytes(SPEC_TEXT.replace("negative", "odd").encode("utf-8"))
    moved = run["runs"].verify_delivery(run["run_id"], phase="source")
    spec.write_bytes(SPEC_TEXT.encode("utf-8"))
    # A proof whose sealed context carries different approved text -- even with
    # a recomputed context digest -- must not verify against the contract.
    receipt = copy.deepcopy(proof.get("receipt") or {})
    forged_context = dict(receipt.get("candidate_review_context") or {})
    forged_context["spec_text"] = "forged requirements\n"
    forged_context["context_digest"] = engine.digest_json({k: v for k, v in forged_context.items() if k != "context_digest"})
    receipt["candidate_review_context"] = forged_context
    contract = run["runs"].delivery_binding(run["run_id"])["contract"]
    forged = reason_of(lambda: api("verify_candidate_review_proof")(contract, receipt))
    seventh = case("tampered-review-or-moved-spec-turns-delivery-red",
                   tampered["verdict"] == "RED" and "candidate_review" in json.dumps(tampered, default=str)
                   and restored["verdict"] == "GREEN" and moved["verdict"] == "RED"
                   and str(moved.get("reason", "")).startswith("delivery_binding_invalid")
                   and forged == "candidate_review_source_context_mismatch",
                   {"tampered": tampered.get("reason"), "restored": restored.get("verdict"),
                    "moved": moved.get("reason"), "forged_context": forged})
    return [sixth, seventh]


def c8_blocking_review(root: Path) -> dict[str, Any]:
    run = _run(root, "blocking", review_policy=policy(root / "blocking-policy"), mode="blocking")
    # The reviewer exits 0 and even claims GREEN; the engine derives RED from its findings.
    ok = (run["adapter_calls"] == 0 and run["source"]["verdict"] == "RED"
          and run["verifier_reason"] == "delivery_independent_review_invalid:candidate_review_not_green")
    return case("blocking-review-is-red-without-action", ok,
                {"adapter_calls": run["adapter_calls"], "source": run["source"].get("reason"),
                 "verifier_reason": run["verifier_reason"]})


def c9_artifact_bytes(root: Path) -> dict[str, Any]:
    store = RunStore(root / "artifact-runs")
    content = "review line 1\nreview line 2\n繁體中文\n"
    written = store.write_artifact("bytes", 1, "review.json", content)
    actual = (store.root / written["ref"]).read_bytes()
    ok = (actual == content.encode("utf-8") and written["digest"] == sha256(actual)
          and store.read_artifact("bytes", 1, "review.json") == content)
    return case("artifact-bytes-match-their-digest", ok, {"bytes": len(actual), "expected": len(content.encode("utf-8"))})


def c10_contract_without_review(root: Path) -> dict[str, Any]:
    run = _run(root, "legacy", review_policy=None, mode="silent")
    ok = run["adapter_calls"] == 1 and run["source"]["verdict"] == "GREEN"
    return case("contract-without-review-is-unchanged", ok,
                {"adapter_calls": run["adapter_calls"], "source": run["source"].get("reason"),
                 "status": run["result"].get("status")})


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-candidate-review-") as raw:
        root = Path(raw).resolve()
        results = [
            guarded("review-policy-is-closed", lambda: c1_policy_is_closed(root / "c1")),
            guarded("review-context-binds-the-candidate", lambda: c2_context_binds_the_candidate(root / "c2")),
            guarded("review-result-is-closed-and-bound", lambda: c3_result_is_closed_and_bound(root / "c3")),
            guarded("review-verdict-is-derived-not-reported", lambda: c4_verdict_is_derived(root / "c4")),
            guarded("exit-zero-without-review-never-reaches-the-action-port", lambda: c5_exit_zero_without_review(root / "c5")),
        ]
        try:
            results.extend(c6_c7_sealed_review(root / "c6"))
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            results.extend([case("sealed-review-reaches-the-action-port-once", False, detail),
                            case("tampered-review-or-moved-spec-turns-delivery-red", False, detail)])
        results.extend([
            guarded("blocking-review-is-red-without-action", lambda: c8_blocking_review(root / "c8")),
            guarded("artifact-bytes-match-their-digest", lambda: c9_artifact_bytes(root / "c9")),
            guarded("contract-without-review-is-unchanged", lambda: c10_contract_without_review(root / "c10")),
        ])
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({
        "check_id": CHECK_ID,
        "status": "pass" if not failures else "fail",
        "total": len(results),
        "results": results,
        "blocking_failures": [row["id"] for row in failures],
        "known_gaps_open": [
            "the work-unit completion path (related checks first, bounded rework, suggestions) is X1b",
            "the symlink-ref refusal is exercised only where the host lets an unprivileged user create symlinks",
        ],
    }, indent=2, ensure_ascii=False, default=str))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())

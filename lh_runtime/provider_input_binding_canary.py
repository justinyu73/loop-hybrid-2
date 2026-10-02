#!/usr/bin/env python3
"""Offline acceptance for provider-context projection and input binding.

Drives the pure functions with synthetic facts: the forward path, every
refusal goal-lifecycle-v1 §Provider-context projection and input binding
names (unknown field, oversize, reorder, omission, mismatch, stale, replay),
the raw-capsule bypass wall, attestation-before-launch ordering, and a fixed
corpus proving the empty-projection render reproduces the legacy
`build_prompt` byte for byte. The corpus digest is printed for VRP-14.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import provider_input_binding as pib  # noqa: E402
from cli_agent_executor import build_prompt  # noqa: E402


def _case(case_id: str, ok: bool, detail: Any = None) -> dict[str, Any]:
    row: dict[str, Any] = {"id": case_id, "ok": bool(ok)}
    if detail is not None:
        row["detail"] = detail
    return row


def _rejected(fn, reason_prefix: str) -> tuple[bool, str]:
    try:
        fn()
    except pib.ProviderInputRejected as exc:
        return exc.reason.startswith(reason_prefix), exc.reason
    return False, "no rejection"


CORPUS_CAPSULES: list[dict[str, Any]] = [
    {"attempt": 1, "base_revision": "a" * 40,
     "goal": {"goal_id": "g-1", "feature_contract": "add a health endpoint"}},
    {"attempt": 3, "base_revision": "fixture-base",
     "goal": {"goal_id": "g-2", "feature_contract": "修正 rollover 判定", "notes": ["非 ASCII", "第二行"]},
     "bootstrap_authority": {"authority": "docs/spec.md#anchor", "decision": "X-001"}},
    {"attempt": 2, "base_revision": "b" * 40, "goal": {},
     "bootstrap_authority": {"routing": {"nested": True, "port": 18080}}},
]


def main() -> int:
    cases: list[dict[str, Any]] = []

    # Projection: forward, absence, unknown, oversize.
    record, rendered = pib.project_context(
        {"knowledge_context": {"hits": ["h1"], "query": "q"}, "grill_note": "watch the fence"})
    round_trip = (
        record["schema"] == pib.PROJECTION_SCHEMA
        and record["field_names"] == ["knowledge_context", "grill_note"]
        and record["absent_fields"] == []
        and record["total_bytes"] == sum(record["field_byte_counts"].values())
        and record["projection_digest"] == pib.digest_json(rendered)
        and all(len(rendered[f].encode("utf-8")) == record["field_byte_counts"][f]
                for f in record["field_names"])
    )
    cases.append(_case("projection-round-trip-records-bytes-and-digests", round_trip, record))

    none_record, _ = pib.project_context({"knowledge_context": None})
    missing_record, _ = pib.project_context({})
    cases.append(_case(
        "absence-is-recorded-not-guessed",
        none_record == missing_record
        and none_record["absent_fields"] == list(pib.PROJECTION_FIELDS),
    ))

    ok, reason = _rejected(lambda: pib.project_context({"scope_note": "x"}), "unknown_field")
    cases.append(_case("unknown-context-field-is-rejected", ok, reason))

    ok, reason = _rejected(
        lambda: pib.project_context({"grill_note": "x" * (pib.FIELD_BYTE_LIMITS["grill_note"] + 1)}),
        "oversized_field")
    cases.append(_case("oversized-field-is-rejected", ok, reason))

    ok, reason = _rejected(
        lambda: pib.project_context({
            "knowledge_context": "k" * pib.FIELD_BYTE_LIMITS["knowledge_context"],
            "grill_note": "g" * 1500,
        }),
        "oversized_total")
    cases.append(_case("oversized-total-is-rejected", ok, reason))

    # Render: legacy byte equality on the corpus, advisory insertion, bypass.
    corpus_rows = []
    equal = True
    for capsule in CORPUS_CAPSULES:
        legacy = build_prompt(capsule)
        new = pib.render_prompt(capsule, {})
        equal = equal and legacy == new
        corpus_rows.append({"capsule": capsule, "prompt": new})
    corpus_digest = pib.digest_json(corpus_rows)
    cases.append(_case("empty-projection-renders-legacy-bytes", equal,
                       {"capsules": len(CORPUS_CAPSULES)}))
    # Pinned at the schema decision commit, where render_prompt was proven
    # byte-identical to the pre-delegation build_prompt. After the delegation
    # this pin is what turns silent render drift red.
    cases.append(_case(
        "corpus-digest-is-pinned",
        corpus_digest == "sha256:3e00da3503790c9ed0e0a925b4ca311b0bc06b15f2689480e17b53db6103db35",
        corpus_digest))

    capsule = CORPUS_CAPSULES[1]
    legacy = build_prompt(capsule)
    _, advisory = pib.project_context({"grill_note": "verifier flaked on attempt 2"})
    with_advisory = pib.render_prompt(capsule, advisory)
    marker = pib.ADVISORY_HEADER + "verifier flaked on attempt 2"
    expected = legacy.replace(
        "\n\nMake the minimal change", marker + "\n\nMake the minimal change", 1)
    cases.append(_case("advisory-renders-marked-and-preserves-base",
                       marker in with_advisory and with_advisory == expected))

    ok, reason = _rejected(
        lambda: pib.render_prompt({**capsule, "knowledge_context": {"raw": True}}, {}),
        "raw_advisory_in_capsule")
    cases.append(_case("raw-advisory-in-capsule-is-rejected", ok, reason))

    # Binding and attestation.
    context = {
        "goal_revision": "goal-rev-1", "run_id": "run-1", "attempt": 2,
        "adapter_id": "cli-claude", "adapter_version": "v1",
        "capability_digest": pib.digest_json({"cap": 1}),
        "authority_digest": pib.digest_json({"auth": 1}),
    }
    prompt = with_advisory
    argv_template = ["claude", "-p", "{prompt}", "--permission-mode", "bypassPermissions"]
    env_projection = {"LH_PROVIDER": pib.digest_text("claude")}
    descriptor_digest = pib.digest_json({"descriptor": "fixture"})
    segments = pib.build_segments(prompt, argv_template, env_projection)
    binding = pib.build_input_binding(
        binding_context=context, projection_record=record, segments=segments,
        launch_descriptor_digest=descriptor_digest, nonce="nonce-1", issued_at=1000.0)

    ok, reason = _rejected(
        lambda: pib.build_input_binding(
            binding_context={**context, "authority_digest": ""},
            projection_record=record, segments=segments,
            launch_descriptor_digest=descriptor_digest, nonce="n", issued_at=1000.0),
        "missing_binding_field")
    cases.append(_case("binding-requires-complete-context", ok, reason))

    ok, reason = _rejected(
        lambda: pib.build_input_binding(
            binding_context=context, projection_record=record,
            segments=segments + [dict(segments[0])],
            launch_descriptor_digest=descriptor_digest, nonce="n", issued_at=1000.0),
        "duplicate_segment_kind")
    cases.append(_case("duplicate-segment-kind-is-rejected", ok, reason))

    def attest(prompt_text=prompt, argv=argv_template, env=env_projection,
               descriptor=descriptor_digest, now=1001.0, nonces=None, bound=binding):
        return pib.attest_before_launch(
            bound, prompt=prompt_text, command_template=argv,
            environment_projection=env, launch_descriptor_digest=descriptor,
            now=now, seen_nonces=nonces if nonces is not None else set())

    attestation = attest()
    cases.append(_case(
        "forward-attestation-binds-the-manifest",
        attestation["attested_binding_digest"] == binding["provider_input_digest"]))

    ok, reason = _rejected(lambda: attest(prompt_text=prompt + " "), "segment_mismatch")
    cases.append(_case("segment-mismatch-is-rejected", ok, reason))

    reordered = {**binding, "segments": [binding["segments"][1], binding["segments"][0],
                                         binding["segments"][2]]}
    ok, reason = _rejected(lambda: attest(bound=reordered), "segments_reordered")
    cases.append(_case("reordered-segments-are-rejected", ok, reason))

    truncated = {**binding, "segments": binding["segments"][:2]}
    ok, reason = _rejected(lambda: attest(bound=truncated), "missing_segment")
    cases.append(_case("missing-segment-is-rejected", ok, reason))

    ok, reason = _rejected(
        lambda: attest(now=1000.0 + pib.BINDING_TTL_SECONDS + 1), "stale_binding")
    cases.append(_case("stale-binding-is-rejected", ok, reason))

    ok, reason = _rejected(lambda: attest(nonces={"nonce-1"}), "replayed_nonce")
    cases.append(_case("replayed-nonce-is-rejected", ok, reason))

    ok, reason = _rejected(
        lambda: attest(descriptor=pib.digest_json({"descriptor": "other"})),
        "launch_descriptor_mismatch")
    cases.append(_case("launch-descriptor-mismatch-is-rejected", ok, reason))

    launches: list[Any] = []
    try:
        pib.attested_launch(
            binding, prompt=prompt + "tampered", command_template=argv_template,
            environment_projection=env_projection,
            launch_descriptor_digest=descriptor_digest, now=1001.0,
            seen_nonces=set(), launcher=launches.append)
    except pib.ProviderInputRejected:
        pass
    outcome = pib.attested_launch(
        binding, prompt=prompt, command_template=argv_template,
        environment_projection=env_projection,
        launch_descriptor_digest=descriptor_digest, now=1001.0,
        seen_nonces=set(), launcher=lambda a: launches.append(a) or "launched")
    cases.append(_case(
        "rejection-precedes-launch",
        len(launches) == 1 and outcome == "launched"
        and launches[0]["attested_binding_digest"] == binding["provider_input_digest"]))

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        open_ok = not pib.admission_disabled(root)
        (root / pib.ADMISSION_SWITCH_NAME).write_text("")
        ok, reason = _rejected(lambda: pib.require_admission(root), "admission_disabled")
        cases.append(_case("admission-switch-refuses-new-admission", open_ok and ok, reason))

    failures = [case for case in cases if not case["ok"]]
    print(json.dumps({
        "check_id": "lh-provider-input-binding",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "corpus_digest": corpus_digest,
        "blocking_failures": failures,
        "cases": cases,
    }, indent=2, ensure_ascii=False))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

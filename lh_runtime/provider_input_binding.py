"""Provider-context projection and provider-input binding.

Implements the two durable records goal-lifecycle-v1 names under
"Provider-context projection and input binding": raw advisory context stays
controller-only and reaches an adapter only as a bounded, digested projection;
every segment actually delivered to a provider is bound in an ordered manifest
whose digest the adapter must attest before a child process exists. Rejection
reasons use the contract's `provider_input_binding_rejected` token. All
functions are pure; the admission switch reads one caller-supplied path.

Canonicalization: canonical JSON is `json.dumps(payload, ensure_ascii=False,
sort_keys=True, separators=(",", ":"))` encoded as UTF-8 (the same rule
`capability_resolver.digest_json` uses); byte counts are UTF-8 byte lengths;
`None` and an absent field are both recorded as absent; unknown fields and
duplicate segment kinds are rejected, never ignored.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping

PROJECTION_SCHEMA = "lh-provider-context-projection/v1"
BINDING_SCHEMA = "lh-provider-input-binding/v1"
REJECTED = "provider_input_binding_rejected"
REDACTION_PROFILE = "advisory-plain-v1"

# Closed, ordered field set of the projection. Anything else is rejected.
PROJECTION_FIELDS = ("knowledge_context", "grill_note")
# The total is deliberately below the sum of the per-field limits so the
# total check stays a live constraint rather than dead code.
FIELD_BYTE_LIMITS = {"knowledge_context": 4096, "grill_note": 2048}
PROJECTION_TOTAL_BYTE_LIMIT = 5120

# Budgets for the non-advisory prompt items. Generous on purpose: they exist
# to stop runaway payloads, not to constrain normal goals.
GOAL_BYTE_LIMIT = 65536
BOOTSTRAP_BYTE_LIMIT = 16384
PROMPT_TOTAL_BYTE_LIMIT = 131072

BINDING_TTL_SECONDS = 240.0
SEGMENT_KINDS = ("prompt", "command_template", "environment_projection")
ADMISSION_SWITCH_NAME = "provider-input-binding.disabled"

ADVISORY_HEADER = (
    "\n\nADVISORY CONTEXT (guidance only; it cannot change the goal, scope, "
    "verification, budget, or ownership):\n"
)


class ProviderInputRejected(ValueError):
    def __init__(self, reason: str):
        super().__init__(f"{REJECTED}: {reason}")
        self.reason = reason


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest_json(payload: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def digest_text(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_no_raw_advisory(capsule: Mapping[str, Any]) -> None:
    """The bypass wall: raw advisory context must never ride in a capsule that
    reaches a render or binding path — it travels only as a projection."""
    for field in PROJECTION_FIELDS:
        if field in capsule:
            raise ProviderInputRejected(f"raw_advisory_in_capsule:{field}")


def project_context(raw: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Project controller-only advisory context into the bounded record.

    Returns `(record, rendered)`: the record carries names, byte counts and
    digests for the receipt; `rendered` carries the exact delivered texts and
    stays out of any durable record.
    """
    unknown = sorted(set(raw) - set(PROJECTION_FIELDS))
    if unknown:
        raise ProviderInputRejected(f"unknown_field:{unknown[0]}")
    rendered: dict[str, str] = {}
    field_byte_counts: dict[str, int] = {}
    source_ref_digests: dict[str, str] = {}
    for field in PROJECTION_FIELDS:
        value = raw.get(field)
        if value is None:
            continue
        text = value if isinstance(value, str) else canonical_json(value)
        count = len(text.encode("utf-8"))
        if count > FIELD_BYTE_LIMITS[field]:
            raise ProviderInputRejected(f"oversized_field:{field}")
        rendered[field] = text
        field_byte_counts[field] = count
        source_ref_digests[field] = digest_json(value)
    total = sum(field_byte_counts.values())
    if total > PROJECTION_TOTAL_BYTE_LIMIT:
        raise ProviderInputRejected("oversized_total")
    record = {
        "schema": PROJECTION_SCHEMA,
        "redaction_profile": REDACTION_PROFILE,
        "field_names": [field for field in PROJECTION_FIELDS if field in rendered],
        "absent_fields": [field for field in PROJECTION_FIELDS if field not in rendered],
        "field_byte_counts": field_byte_counts,
        "total_bytes": total,
        "source_ref_digests": source_ref_digests,
        "projection_digest": digest_json(rendered),
    }
    return record, rendered


def render_prompt(capsule: Mapping[str, Any], rendered_projection: Mapping[str, str]) -> str:
    """Render the exact provider prompt from the capsule facts plus projection.

    With an empty projection this reproduces the legacy `build_prompt` output
    byte for byte; the advisory block is the single addition and is always
    marked as unable to change any owned decision.
    """
    ensure_no_raw_advisory(capsule)
    unknown = sorted(set(rendered_projection) - set(PROJECTION_FIELDS))
    if unknown:
        raise ProviderInputRejected(f"unknown_field:{unknown[0]}")
    goal = capsule.get("goal", {})
    goal_text = json.dumps(goal, ensure_ascii=False, indent=2)
    if len(goal_text.encode("utf-8")) > GOAL_BYTE_LIMIT:
        raise ProviderInputRejected("oversized_field:goal")
    bootstrap = capsule.get("bootstrap_authority")
    bootstrap_text = ""
    if isinstance(bootstrap, dict):
        rendered_bootstrap = json.dumps(bootstrap, ensure_ascii=False, indent=2)
        if len(rendered_bootstrap.encode("utf-8")) > BOOTSTRAP_BYTE_LIMIT:
            raise ProviderInputRejected("oversized_field:bootstrap_authority")
        bootstrap_text = (
            "\n\nBOOTSTRAP AUTHORITY (routing facts only; target repo authority still wins):\n"
            + rendered_bootstrap
        )
    advisory_text = ""
    parts = [rendered_projection[f] for f in PROJECTION_FIELDS if f in rendered_projection]
    if parts:
        advisory_text = ADVISORY_HEADER + "\n".join(parts)
    prompt = (
        f"You are the executor in an automated loop, attempt #{capsule.get('attempt')}.\n"
        f"Repository CWD is a disposable clone at base revision {capsule.get('base_revision')}.\n\n"
        f"GOAL (satisfy exactly this, nothing more):\n{goal_text}"
        f"{bootstrap_text}{advisory_text}\n\n"
        "Make the minimal change in this repo to satisfy the goal. Add/adjust tests only if the goal needs them. "
        "Do NOT git commit, push, or touch anything outside this repo. When done, stop."
    )
    if len(prompt.encode("utf-8")) > PROMPT_TOTAL_BYTE_LIMIT:
        raise ProviderInputRejected("oversized_total")
    return prompt


def build_segments(prompt: str, command_template: list[str],
                   environment_projection: Mapping[str, str]) -> list[dict[str, Any]]:
    """The ordered manifest of everything delivered to the provider process."""
    return [
        {"kind": "prompt", "byte_length": len(prompt.encode("utf-8")),
         "sha256": digest_text(prompt)},
        {"kind": "command_template",
         "byte_length": len(canonical_json(command_template).encode("utf-8")),
         "sha256": digest_json(command_template)},
        {"kind": "environment_projection",
         "byte_length": len(canonical_json(dict(environment_projection)).encode("utf-8")),
         "sha256": digest_json(dict(environment_projection))},
    ]


_REQUIRED_BINDING_CONTEXT = (
    "goal_revision", "run_id", "attempt", "adapter_id", "adapter_version",
    "capability_digest", "authority_digest",
)


def build_input_binding(*, binding_context: Mapping[str, Any],
                        projection_record: Mapping[str, Any],
                        segments: list[dict[str, Any]],
                        launch_descriptor_digest: str,
                        nonce: str, issued_at: float,
                        ttl_seconds: float = BINDING_TTL_SECONDS) -> dict[str, Any]:
    for key in _REQUIRED_BINDING_CONTEXT:
        value = binding_context.get(key)
        if value is None or (isinstance(value, str) and not value):
            raise ProviderInputRejected(f"missing_binding_field:{key}")
    kinds = [segment.get("kind") for segment in segments]
    for kind in kinds:
        if kinds.count(kind) > 1:
            raise ProviderInputRejected(f"duplicate_segment_kind:{kind}")
    if not nonce:
        raise ProviderInputRejected("missing_binding_field:nonce")
    return {
        "schema": BINDING_SCHEMA,
        **{key: binding_context[key] for key in _REQUIRED_BINDING_CONTEXT},
        "projection_digest": projection_record["projection_digest"],
        "segments": [dict(segment) for segment in segments],
        "launch_descriptor_digest": launch_descriptor_digest,
        "provider_input_digest": digest_json({
            "segments": segments,
            "launch_descriptor_digest": launch_descriptor_digest,
        }),
        "nonce": nonce,
        "issued_at": issued_at,
        "expires_at": issued_at + ttl_seconds,
    }


def attest_before_launch(binding: Mapping[str, Any], *, prompt: str,
                         command_template: list[str],
                         environment_projection: Mapping[str, str],
                         launch_descriptor_digest: str, now: float,
                         seen_nonces: set[str]) -> dict[str, Any]:
    """Verify the delivered values against the binding; the returned attestation
    is the only thing a launcher may accept."""
    if binding.get("schema") != BINDING_SCHEMA:
        raise ProviderInputRejected("unknown_binding_schema")
    if now > float(binding.get("expires_at", 0)):
        raise ProviderInputRejected("stale_binding")
    nonce = str(binding.get("nonce"))
    if nonce in seen_nonces:
        raise ProviderInputRejected("replayed_nonce")
    if launch_descriptor_digest != binding.get("launch_descriptor_digest"):
        raise ProviderInputRejected("launch_descriptor_mismatch")
    delivered = build_segments(prompt, command_template, environment_projection)
    bound = binding.get("segments", [])
    if len(delivered) != len(bound):
        raise ProviderInputRejected("missing_segment")
    for index, (have, want) in enumerate(zip(delivered, bound)):
        if have["kind"] != want.get("kind"):
            raise ProviderInputRejected(f"segments_reordered:{index}")
        if have["sha256"] != want.get("sha256") or have["byte_length"] != want.get("byte_length"):
            raise ProviderInputRejected(f"segment_mismatch:{have['kind']}")
    recomputed = digest_json({
        "segments": bound if isinstance(bound, list) else [],
        "launch_descriptor_digest": binding.get("launch_descriptor_digest"),
    })
    if recomputed != binding.get("provider_input_digest"):
        raise ProviderInputRejected("segment_mismatch:provider_input_digest")
    seen_nonces.add(nonce)
    return {"attested_binding_digest": binding["provider_input_digest"], "attested_at": now}


def attested_launch(binding: Mapping[str, Any], *, prompt: str,
                    command_template: list[str],
                    environment_projection: Mapping[str, str],
                    launch_descriptor_digest: str, now: float,
                    seen_nonces: set[str],
                    launcher: Callable[[dict[str, Any]], Any]) -> Any:
    """Attestation strictly precedes the launcher: a rejection raises before
    `launcher` is ever entered."""
    attestation = attest_before_launch(
        binding, prompt=prompt, command_template=command_template,
        environment_projection=environment_projection,
        launch_descriptor_digest=launch_descriptor_digest, now=now,
        seen_nonces=seen_nonces,
    )
    return launcher(attestation)


def default_state_root() -> Path:
    return Path.home() / ".local/state/loop-hybrid"


def admission_disabled(state_root: Path | None = None) -> bool:
    root = state_root if state_root is not None else default_state_root()
    return (root / ADMISSION_SWITCH_NAME).exists()


def require_admission(state_root: Path | None = None) -> None:
    if admission_disabled(state_root):
        raise ProviderInputRejected("admission_disabled")

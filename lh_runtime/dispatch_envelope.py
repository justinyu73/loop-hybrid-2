"""Validate the immutable external host -> LH project dispatch boundary."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA = "lh-external-dispatch/v1"
FIELDS = {
    "schema",
    "dispatch_id",
    "project_id",
    "owner_id",
    "contract_ref",
    "contract_digest",
    "desired_state",
    "desired_state_event_id",
    "desired_state_digest",
    "campaign_id",
    "base_revision",
    "issued_at",
    "source_invocation_id",
    "envelope_digest",
}


def digest_json(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def digest_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def load_and_validate(
    path: str | Path,
    *,
    project_id: str,
    owner_id: str,
    contract_path: str | Path,
) -> dict[str, Any]:
    envelope_path = Path(path).resolve()
    envelope = json.loads(envelope_path.read_text(encoding="utf-8"))
    if not isinstance(envelope, dict) or set(envelope) != FIELDS:
        raise ValueError("dispatch envelope fields do not match v1")
    if envelope.get("schema") != SCHEMA:
        raise ValueError("unsupported dispatch envelope schema")
    if envelope.get("project_id") != project_id or envelope.get("owner_id") != owner_id:
        raise ValueError("dispatch project or owner identity mismatch")
    if envelope.get("desired_state") != "enabled":
        raise ValueError("dispatch desired state is not enabled")
    for field in (
        "dispatch_id",
        "contract_ref",
        "contract_digest",
        "desired_state_event_id",
        "desired_state_digest",
        "campaign_id",
        "base_revision",
        "issued_at",
        "source_invocation_id",
        "envelope_digest",
    ):
        if not isinstance(envelope.get(field), str) or not envelope[field]:
            raise ValueError(f"dispatch {field} is required")
    for field in ("contract_digest", "desired_state_digest", "envelope_digest"):
        if not envelope[field].startswith("sha256:"):
            raise ValueError(f"dispatch {field} must be sha256")
    without_digest = {key: value for key, value in envelope.items() if key != "envelope_digest"}
    if digest_json(without_digest) != envelope["envelope_digest"]:
        raise ValueError("dispatch envelope digest mismatch")
    body = {
        key: value
        for key, value in envelope.items()
        if key not in {"dispatch_id", "envelope_digest"}
    }
    expected_id = "dispatch-" + digest_json(body).removeprefix("sha256:")[:32]
    if envelope["dispatch_id"] != expected_id:
        raise ValueError("dispatch_id does not match envelope body")

    resolved_contract = Path(contract_path).resolve()
    if Path(envelope["contract_ref"]).resolve() != resolved_contract:
        raise ValueError("dispatch contract_ref mismatch")
    if digest_file(resolved_contract) != envelope["contract_digest"]:
        raise ValueError("dispatch contract digest mismatch")
    contract = json.loads(resolved_contract.read_text(encoding="utf-8"))
    campaign = contract.get("campaign") if isinstance(contract.get("campaign"), dict) else {}
    if contract.get("schema") != "lh-project-runtime-contract/v1":
        raise ValueError("dispatch contract schema mismatch")
    if contract.get("project_id") != project_id:
        raise ValueError("dispatch contract project_id mismatch")
    if campaign.get("campaign_id") != envelope["campaign_id"]:
        raise ValueError("dispatch campaign_id mismatch")
    if contract.get("base_revision") != envelope["base_revision"]:
        raise ValueError("dispatch base_revision mismatch")
    return envelope


def receipt_binding(envelope: dict[str, Any]) -> dict[str, str]:
    return {
        key: str(envelope[key])
        for key in (
            "dispatch_id",
            "envelope_digest",
            "project_id",
            "owner_id",
            "contract_digest",
            "desired_state_event_id",
            "desired_state_digest",
        )
    }

"""Provider-neutral host port contract.

The headless interface is the whole host contract.  No worktree, executable,
or ambient host value is interpreted as a selection signal, and no host
adapter can be selected.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

HOST_PORT_SCHEMA = "host-host-interface-policy/v1"
HOST_BINDING_SCHEMA = "host-host-port-binding/v1"
CORE_PORTS = ("workspace", "terminal", "process", "ui_gateway")


class HostPortError(ValueError):
    """The explicit host-port contract cannot be admitted."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HostPortError(f"{name}_missing")
    if any(char in value for char in "\r\n\x00"):
        raise HostPortError(f"{name}_invalid")
    return value.strip()


def headless_contract() -> dict[str, Any]:
    """Return the immutable headless host contract."""
    return {
        "schema": HOST_PORT_SCHEMA,
        "revision": "1",
        "core_ports": list(CORE_PORTS),
        "required_default_interface": "headless_cli",
    }


def validate_host_contract(raw: Any) -> dict[str, Any]:
    """Validate and normalize a host contract without probing the host."""
    if not isinstance(raw, Mapping) or raw.get("schema") != HOST_PORT_SCHEMA:
        raise HostPortError("host_contract_schema_invalid")
    revision = _required_text("revision", raw.get("revision"))
    if raw.get("core_ports") != list(CORE_PORTS):
        raise HostPortError("core_ports_invalid")
    if raw.get("required_default_interface") != "headless_cli":
        raise HostPortError("headless_interface_required")
    if raw.get("selected_adapter") not in (None, "") or raw.get("optional_adapters"):
        raise HostPortError("host_adapter_unsupported")
    if raw.get("worktree") not in (None, ""):
        raise HostPortError("host_worktree_unsupported")
    return {
        "schema": HOST_PORT_SCHEMA,
        "revision": revision,
        "core_ports": list(CORE_PORTS),
        "required_default_interface": "headless_cli",
    }


def resolve_host_ports(raw: Any) -> dict[str, Any]:
    """Bind the headless ports."""
    contract = validate_host_contract(raw)
    binding = {
        "schema": HOST_BINDING_SCHEMA,
        "contract_digest": _digest(contract),
        "interface": "headless_cli",
        "ports": list(CORE_PORTS),
        "core_operational": True,
        "selection_inferred": False,
    }
    binding["binding_digest"] = _digest(binding)
    return binding


__all__ = [
    "CORE_PORTS",
    "HOST_BINDING_SCHEMA",
    "HOST_PORT_SCHEMA",
    "HostPortError",
    "headless_contract",
    "resolve_host_ports",
    "validate_host_contract",
]

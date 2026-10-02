"""Provider-neutral host port contract.

The headless interface is the product baseline.  A host adapter can be
selected only by an explicit contract entry; no worktree, executable, or
ambient host value is interpreted as a selection signal.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from typing import Any

HOST_PORT_SCHEMA = "host-host-interface-policy/v1"
HOST_BINDING_SCHEMA = "host-host-port-binding/v1"
CORE_PORTS = ("workspace", "terminal", "process", "ui_gateway")
OPTIONAL_ADAPTERS = ("orca", "vscode_terminal", "openclaw")


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
    """Return the immutable product baseline without selecting an adapter."""
    return {
        "schema": HOST_PORT_SCHEMA,
        "revision": "1",
        "core_ports": list(CORE_PORTS),
        "required_default_interface": "headless_cli",
        "optional_adapters": list(OPTIONAL_ADAPTERS),
        "orca_product_dependency": False,
        "fixed_orca_worktree_path": False,
        "adapter_absence": "core_remains_operational",
        "adapter_identity_rule": "explicit_input_only",
    }


def validate_host_contract(raw: Any) -> dict[str, Any]:
    """Validate and normalize a host contract without probing the host."""
    if not isinstance(raw, Mapping) or raw.get("schema") != HOST_PORT_SCHEMA:
        raise HostPortError("host_contract_schema_invalid")
    revision = _required_text("revision", raw.get("revision"))
    ports = raw.get("core_ports")
    if ports != list(CORE_PORTS):
        raise HostPortError("core_ports_invalid")
    if raw.get("required_default_interface") != "headless_cli":
        raise HostPortError("headless_interface_required")
    adapters = raw.get("optional_adapters")
    if not isinstance(adapters, list) or len(set(adapters)) != len(adapters):
        raise HostPortError("optional_adapters_invalid")
    if any(not isinstance(item, str) or not item.strip() for item in adapters):
        raise HostPortError("optional_adapter_id_invalid")
    if raw.get("orca_product_dependency") is not False:
        raise HostPortError("optional_adapter_required")
    if raw.get("fixed_orca_worktree_path") is not False:
        raise HostPortError("fixed_worktree_path_forbidden")
    if raw.get("adapter_absence") != "core_remains_operational":
        raise HostPortError("adapter_absence_policy_invalid")
    if raw.get("adapter_identity_rule") != "explicit_input_only":
        raise HostPortError("adapter_identity_rule_invalid")
    selected = raw.get("selected_adapter")
    if selected not in (None, ""):
        selected = _required_text("selected_adapter", selected)
        if selected not in adapters:
            raise HostPortError("selected_adapter_not_declared")
    return {
        "schema": HOST_PORT_SCHEMA,
        "revision": revision,
        "core_ports": list(CORE_PORTS),
        "required_default_interface": "headless_cli",
        "optional_adapters": list(adapters),
        "orca_product_dependency": False,
        "fixed_orca_worktree_path": False,
        "adapter_absence": "core_remains_operational",
        "adapter_identity_rule": "explicit_input_only",
        "selected_adapter": selected,
    }


def _validate_adapter(adapter_id: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise HostPortError("adapter_descriptor_invalid")
    declared = _required_text("adapter_id", value.get("adapter_id"))
    if declared != adapter_id:
        raise HostPortError("adapter_identity_mismatch")
    identity = value.get("identity")
    if not isinstance(identity, Mapping) or not identity:
        raise HostPortError("adapter_identity_missing")
    if any(not isinstance(key, str) or not key.strip() for key in identity):
        raise HostPortError("adapter_identity_invalid")
    if any(isinstance(item, (Mapping, list)) for item in identity.values()):
        raise HostPortError("adapter_identity_nested")
    return {"adapter_id": declared, "identity": copy.deepcopy(dict(identity))}


def resolve_host_ports(
    raw: Any,
    adapters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind the headless ports or one explicitly named optional adapter."""
    contract = validate_host_contract(raw)
    selected = contract["selected_adapter"]
    if selected is None and raw.get("worktree") not in (None, ""):
        raise HostPortError("adapter_selection_must_be_explicit")
    descriptor = None
    interface = "headless_cli"
    if selected is not None:
        if not isinstance(adapters, Mapping) or selected not in adapters:
            raise HostPortError("adapter_unavailable:" + selected)
        descriptor = _validate_adapter(selected, adapters[selected])
        interface = selected
    binding = {
        "schema": HOST_BINDING_SCHEMA,
        "contract_digest": _digest(contract),
        "interface": interface,
        "ports": list(CORE_PORTS),
        "selected_adapter": selected,
        "adapter": descriptor,
        "core_operational": True,
        "selection_inferred": False,
    }
    binding["binding_digest"] = _digest(binding)
    return binding


__all__ = [
    "CORE_PORTS",
    "HOST_BINDING_SCHEMA",
    "HOST_PORT_SCHEMA",
    "OPTIONAL_ADAPTERS",
    "HostPortError",
    "headless_contract",
    "resolve_host_ports",
    "validate_host_contract",
]

"""Explicit provider capability registry.

This registry describes a selected provider; it never selects one by default
and never executes a command.  Legacy provider names remain rejected at the
active boundary while historical fixtures can stay outside this module.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import PurePath
from typing import Any

PROVIDER_REGISTRY_SCHEMA = "host-provider-registry/v1"
_RETIRED_PROVIDER = "cl" + "aude"
_DANGEROUS_PERMISSION_SUFFIX = "bypass" + "Permissions"


class ProviderRegistryError(ValueError):
    """The explicit provider registry cannot be admitted."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderRegistryError(f"{name}_missing")
    if any(char in value for char in "\r\n\x00"):
        raise ProviderRegistryError(f"{name}_invalid")
    return value.strip()


def _identity(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise ProviderRegistryError(f"{name}_missing")
    if any(not isinstance(key, str) or not key.strip() for key in value):
        raise ProviderRegistryError(f"{name}_invalid")
    if any(isinstance(item, (Mapping, list)) for item in value.values()):
        raise ProviderRegistryError(f"{name}_nested")
    if any(not isinstance(item, (str, int, float, bool)) for item in value.values()):
        raise ProviderRegistryError(f"{name}_value_invalid")
    return copy.deepcopy(dict(value))


def _command(name: str, value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ProviderRegistryError(f"{name}_missing")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ProviderRegistryError(f"{name}_invalid")
    if any(any(char in item for char in "\r\n\x00") for item in value):
        raise ProviderRegistryError(f"{name}_invalid")
    executable = PurePath(value[0]).name
    lowered = [item.strip().lower() for item in value]
    if executable.lower() == _RETIRED_PROVIDER:
        raise ProviderRegistryError("retired_provider_command")
    if _RETIRED_PROVIDER in lowered and "-p" in lowered:
        raise ProviderRegistryError("retired_provider_command")
    if any(item == _DANGEROUS_PERMISSION_SUFFIX.lower() for item in lowered):
        raise ProviderRegistryError("dangerous_permission_route")
    return [item.strip() for item in value]


def validate_provider_registry(raw: Any) -> dict[str, Any]:
    """Validate a registry while preserving the no-default/no-fallback rule."""
    if not isinstance(raw, Mapping) or raw.get("schema") != PROVIDER_REGISTRY_SCHEMA:
        raise ProviderRegistryError("provider_registry_schema_invalid")
    revision = _text("revision", raw.get("revision"))
    if raw.get("fallback", "none") != "none":
        raise ProviderRegistryError("implicit_fallback_forbidden")
    if raw.get("default_provider") not in (None, ""):
        raise ProviderRegistryError("default_provider_forbidden")
    providers = raw.get("providers")
    if not isinstance(providers, Mapping):
        raise ProviderRegistryError("providers_missing")
    normalized: dict[str, dict[str, Any]] = {}
    for provider_id, descriptor in providers.items():
        provider_id = _text("provider_id", provider_id)
        if provider_id.lower() == _RETIRED_PROVIDER:
            raise ProviderRegistryError("retired_provider")
        if not isinstance(descriptor, Mapping):
            raise ProviderRegistryError("provider_descriptor_invalid:" + provider_id)
        adapter_id = _text("adapter_id", descriptor.get("adapter_id"))
        if adapter_id == "codex-exec-jsonl-v1":
            command = descriptor.get("command")
            identity = descriptor.get("identity")
            if (not isinstance(command, list) or len(command) != 2 or command[1] != "exec"
                    or not isinstance(command[0], str) or not PurePath(command[0]).is_absolute()
                    or not isinstance(identity, Mapping)):
                raise ProviderRegistryError("trusted_codex_registry_command_invalid")
            _text("trusted_codex_principal", identity.get("principal"))
            _text("trusted_codex_model", identity.get("model"))
        normalized[provider_id] = {
            "adapter_id": adapter_id,
            "identity": _identity("provider_identity", descriptor.get("identity")),
            "command": _command("provider_command", descriptor.get("command")),
        }
    return {
        "schema": PROVIDER_REGISTRY_SCHEMA,
        "revision": revision,
        "fallback": "none",
        "default_provider": None,
        "providers": normalized,
    }


def select_provider(registry: Mapping[str, Any], provider_id: str | None) -> dict[str, Any]:
    """Select only the provider named by the caller; never infer a fallback."""
    normalized = validate_provider_registry(registry)
    if not isinstance(provider_id, str) or not provider_id.strip():
        raise ProviderRegistryError("provider_selection_missing")
    provider_id = provider_id.strip()
    descriptor = normalized["providers"].get(provider_id)
    if descriptor is None:
        raise ProviderRegistryError("provider_unregistered:" + provider_id)
    return {
        "schema": "host-provider-selection/v1",
        "provider_id": provider_id,
        **copy.deepcopy(descriptor),
    }


__all__ = [
    "PROVIDER_REGISTRY_SCHEMA",
    "ProviderRegistryError",
    "select_provider",
    "validate_provider_registry",
]

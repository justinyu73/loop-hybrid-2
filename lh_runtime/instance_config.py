#!/usr/bin/env python3
"""Versioned, instance-owned paths and executable discovery for LH.

The project contract remains the authority for project intent.  This module
owns only installation-local placement, executable discovery, secret-store
names, and the generated client preflight policy.  It never stores credentials
or makes a project path relative to the operator's checkout.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterator, Mapping

try:
    from .execution_fence import PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS
except ImportError:
    from execution_fence import PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS


INSTANCE_CONFIG_SCHEMA = "lh-instance-config/v1"
LEGACY_INSTANCE_CONFIG_SCHEMA = "lh-instance-config/v0"
EGRESS_POLICY_SCHEMA = "host-execution-host-egress-policy/v1"
EGRESS_POLICY_ENFORCED_BY = "lh-client-preflight"
INSTANCE_CONFIG_ENV = "LH_INSTANCE_CONFIG"
INSTANCE_ROOT_ENV = "LH_INSTANCE_ROOT"
PATH_KEYS = ("repo", "state", "workspace", "cache", "logs")
PROVIDER_NAME_RE = re.compile(r"^[a-z][a-z0-9._-]{0,63}$")
OPTIONAL_PROVIDER_RULES: dict[str, dict[str, Any]] = {
    "codex": {
        "flags": ["exec", "--ephemeral", "--json", "--dangerously-bypass-approvals-and-sandbox"],
        "value_flags": ["-m"],
        "prompt_flags": [],
        "trailing_prompt": True,
    },
}
GENERIC_PROVIDER_RULES: dict[str, Any] = {
    "flags": [],
    "value_flags": [],
    "prompt_flags": [],
    "trailing_prompt": True,
}
# Read-only system roots of a generated provider-sandbox profile.  Missing
# entries are tolerated by the sandbox (bind-try), so one list serves distros
# with and without a merged /usr or a systemd stub resolver.
PROVIDER_SANDBOX_SYSTEM_RO_BINDS = ("/usr", "/bin", "/lib", "/lib64", "/etc", "/run/systemd/resolve")
PROVIDER_SANDBOX_FLAGS = ("--die-with-parent", "--new-session")


def _bubblewrap_version(path: Any) -> str | None:
    """The version bubblewrap reports, or None when it cannot be read."""
    if not isinstance(path, str) or not path:
        return None
    try:
        result = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    version = result.stdout.strip().removeprefix("bubblewrap ").strip()
    return version if result.returncode == 0 and version else None


def _provider_home_binds(name: str, *, environ: Mapping[str, str], home: Path) -> list[str]:
    """Provider homes the sandbox exposes read-only (credentials stay on disk)."""
    if name == "codex":
        configured = environ.get("CODEX_HOME", "").strip()
        return [str(Path(configured).expanduser().resolve(strict=False) if configured else home / ".codex")]
    return []


def _provider_ro_roots(path: Any) -> list[str]:
    """Read-only roots a provider needs: its own directory and, for a
    ``#!/usr/bin/env node`` script, the prefix holding the matching node."""
    if not isinstance(path, str) or not path:
        return []
    provider = Path(path).resolve(strict=False)
    roots = [str(provider.parent)]
    try:
        with provider.open("r", encoding="utf-8", errors="replace") as handle:
            first_line = handle.readline(256)
    except OSError:
        return roots
    if first_line[2:].strip().split() == ["/usr/bin/env", "node"]:
        for parent in provider.parents:
            runtime = parent / "bin" / "node"
            if runtime.is_file() and os.access(runtime, os.X_OK):
                roots.append(str(parent.resolve(strict=False)))
                break
    return roots


class InstanceConfigError(ValueError):
    """The instance file cannot be safely consumed."""


def _platform_name(system: str | None = None) -> str:
    value = (system or platform.system()).strip().lower()
    if value.startswith("win"):
        return "windows"
    if value in {"darwin", "mac", "macos"}:
        return "macos"
    if value == "linux":
        return "linux"
    return value or "unknown"


def _environment(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    return {str(key): str(value) for key, value in (os.environ if environ is None else environ).items()}


def _home_path(environ: Mapping[str, str], home: str | Path | None) -> Path:
    if home is not None:
        return Path(home).expanduser()
    candidate = environ.get("USERPROFILE") or environ.get("HOME")
    if candidate:
        return Path(candidate).expanduser()
    return Path.home()


def _absolute(value: str | Path, *, anchor: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = anchor / path
    return path.resolve(strict=False)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value)).hexdigest()


def digest_file(path: str | Path) -> str | None:
    candidate = Path(path)
    try:
        if not candidate.is_file():
            return None
        digest = hashlib.sha256()
        with candidate.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return "sha256:" + digest.hexdigest()
    except OSError:
        return None


def _is_executable(path: Path) -> bool:
    if not path.is_file():
        return False
    if _platform_name() == "windows":
        return True
    return os.access(path, os.X_OK)


def _candidate_path(command: str, *, home: Path, environ: Mapping[str, str], name: str) -> list[Path]:
    candidates: list[Path] = []
    local_app_data = environ.get("LOCALAPPDATA") or environ.get("APPDATA")
    if local_app_data:
        local = Path(local_app_data)
        candidates.extend(
            (
                local / "Programs" / name / f"{name}.exe",
                local / name / f"{name}.exe",
            )
        )
    candidates.extend(
        (
            home / ".local" / "bin" / command,
            home / ".codex" / "bin" / command,
        )
    )
    nvm_root = home / ".nvm" / "versions" / "node"
    if nvm_root.is_dir():
        try:
            versions = sorted(nvm_root.iterdir(), reverse=True)
        except OSError:
            versions = []
        candidates.extend(version / "bin" / command for version in versions)
    return candidates


def discover_executable(
    command: str,
    *,
    explicit: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    home: str | Path | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Resolve one executable and return path/digest provenance only."""
    env = _environment(environ)
    home_path = _home_path(env, home)
    label = name or command
    requested = str(explicit).strip() if explicit is not None else ""
    if requested:
        requested_path = Path(requested).expanduser()
        if requested_path.is_absolute() or requested_path.parent != Path("."):
            candidate = requested_path.resolve(strict=False)
            if _is_executable(candidate):
                return {"command": command, "path": str(candidate), "sha256": digest_file(candidate), "source": "explicit"}
            return {"command": command, "path": None, "sha256": None, "source": "explicit_missing"}
        found = shutil.which(requested, path=env.get("PATH"))
        if found and _is_executable(Path(found)):
            candidate = Path(found).resolve(strict=False)
            return {"command": command, "path": str(candidate), "sha256": digest_file(candidate), "source": "explicit_path"}
        return {"command": command, "path": None, "sha256": None, "source": "explicit_missing"}

    found = shutil.which(command, path=env.get("PATH"))
    if found and _is_executable(Path(found)):
        candidate = Path(found).resolve(strict=False)
        return {"command": command, "path": str(candidate), "sha256": digest_file(candidate), "source": "PATH"}
    for candidate in _candidate_path(command, home=home_path, environ=env, name=label):
        if _is_executable(candidate):
            resolved = candidate.resolve(strict=False)
            return {"command": command, "path": str(resolved), "sha256": digest_file(resolved), "source": "known_location"}
    return {"command": command, "path": None, "sha256": None, "source": "missing"}


def _default_roots(
    system: str,
    *,
    environ: Mapping[str, str],
    home: Path,
    cwd: Path,
) -> dict[str, Path]:
    """Return platform-native data roots.

    Windows values use the Known Folder environment contract when present;
    Linux follows XDG; macOS uses Application Support.  The fallback is still
    derived from the current user and never names a particular account.
    """
    normalized = _platform_name(system)
    if normalized == "windows":
        local = Path(environ.get("LOCALAPPDATA") or environ.get("APPDATA") or home / "AppData" / "Local")
        data_root = local / "lh-host"
        state_root = data_root / "state"
        cache_root = data_root / "cache"
    elif normalized == "macos":
        app_support = home / "Library" / "Application Support"
        data_root = app_support / "lh-host"
        state_root = data_root / "state"
        cache_root = data_root / "cache"
    else:
        data_root = Path(environ.get("XDG_DATA_HOME") or home / ".local" / "share") / "lh-host"
        state_root = Path(environ.get("XDG_STATE_HOME") or home / ".local" / "state") / "lh-host"
        cache_root = Path(environ.get("XDG_CACHE_HOME") or home / ".cache") / "lh-host"
    return {
        "repo": Path(environ.get("LH_REPO_ROOT") or cwd),
        "state": Path(environ.get("LH_STATE_ROOT") or state_root),
        "workspace": Path(environ.get("LH_WORKSPACE_ROOT") or data_root / "workspaces"),
        "cache": Path(environ.get("LH_CACHE_ROOT") or cache_root),
        "logs": Path(environ.get("LH_LOGS_ROOT") or state_root / "logs"),
    }


def default_instance_config_path(
    *,
    system: str | None = None,
    environ: Mapping[str, str] | None = None,
    home: str | Path | None = None,
) -> Path:
    env = _environment(environ)
    if env.get(INSTANCE_CONFIG_ENV, "").strip():
        return Path(env[INSTANCE_CONFIG_ENV]).expanduser().resolve(strict=False)
    if env.get(INSTANCE_ROOT_ENV, "").strip():
        return (Path(env[INSTANCE_ROOT_ENV]).expanduser() / "instance.json").resolve(strict=False)
    home_path = _home_path(env, home)
    normalized = _platform_name(system)
    if normalized == "windows":
        root = Path(env.get("LOCALAPPDATA") or env.get("APPDATA") or home_path / "AppData" / "Local") / "lh-host"
    elif normalized == "macos":
        root = home_path / "Library" / "Application Support" / "lh-host"
    else:
        root = Path(env.get("XDG_CONFIG_HOME") or home_path / ".config") / "lh-host"
    return (root / "instance.json").resolve(strict=False)


def discover_instance_config(
    explicit: str | Path | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    include_default: bool = True,
) -> Path | None:
    """Find an instance file without creating one."""
    env = _environment(environ)
    configured: Path | None = None
    if explicit is not None and str(explicit).strip():
        configured = Path(explicit).expanduser()
    elif env.get(INSTANCE_CONFIG_ENV, "").strip():
        configured = Path(env[INSTANCE_CONFIG_ENV]).expanduser()
    elif env.get(INSTANCE_ROOT_ENV, "").strip():
        configured = Path(env[INSTANCE_ROOT_ENV]).expanduser() / "instance.json"
    if configured is not None:
        resolved = configured.resolve(strict=False)
        if not resolved.is_file():
            raise InstanceConfigError(f"explicit instance config not found: {resolved}")
        return resolved
    if include_default:
        resolved = default_instance_config_path(environ=env)
        if resolved.is_file():
            return resolved
    return None


def _deep_update(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _cli_override(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Mapping) and isinstance(value.get("path"), str) and value["path"].strip():
        return value["path"].strip()
    return None


def _default_config_data(
    config_path: Path,
    *,
    system: str | None = None,
    environ: Mapping[str, str] | None = None,
    home: str | Path | None = None,
    cwd: str | Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    env = _environment(environ)
    home_path = _home_path(env, home)
    cwd_path = Path(cwd or Path.cwd()).expanduser().resolve(strict=False)
    normalized = _platform_name(system)
    override_data = dict(overrides or {})
    platform_override = override_data.get("platform")
    if isinstance(platform_override, str) and platform_override.strip():
        normalized = _platform_name(platform_override)
    default_paths = _default_roots(normalized, environ=env, home=home_path, cwd=cwd_path)
    paths = {key: str(value) for key, value in default_paths.items()}
    path_overrides = override_data.get("paths") if isinstance(override_data.get("paths"), Mapping) else {}
    for key in PATH_KEYS:
        value = path_overrides.get(key)
        if isinstance(value, str) and value.strip():
            paths[key] = value.strip()
        legacy_value = override_data.get(f"{key}_root")
        if isinstance(legacy_value, str) and legacy_value.strip():
            paths[key] = legacy_value.strip()

    cli_overrides = override_data.get("cli") if isinstance(override_data.get("cli"), Mapping) else {}
    cli: dict[str, Any] = {"providers": {}}
    provider_overrides = cli_overrides.get("providers") if isinstance(cli_overrides.get("providers"), Mapping) else {}
    configured_names = {
        str(name) for name in provider_overrides
        if isinstance(name, str) and name.strip()
    }
    declared_names = env.get("LH_PROVIDER_NAMES", "")
    configured_names.update(
        name.strip() for name in declared_names.split(",")
        if name.strip()
    )
    # Compatibility environment variables are opt-in declarations, not
    # defaults: an unset variable creates no provider entry.
    for name in ("codex", "claude"):
        if env.get(f"LH_{name.upper()}_CLI", "").strip():
            configured_names.add(name)
    for name in sorted(configured_names):
        if PROVIDER_NAME_RE.fullmatch(name) is None:
            raise InstanceConfigError(f"invalid provider adapter name: {name!r}")
        spec = provider_overrides.get(name)
        command = str(spec.get("command", name)) if isinstance(spec, Mapping) else name
        env_name = f"LH_{name.upper()}_CLI"
        explicit = _cli_override(spec) or env.get(env_name) or env.get(f"LH_PROVIDER_{name.upper()}_CLI") or None
        entry = discover_executable(command, explicit=explicit, environ=env, home=home_path, name=name)
        if isinstance(spec, Mapping) and isinstance(spec.get("policy"), Mapping):
            entry["policy"] = dict(spec["policy"])
        cli["providers"][name] = entry
    provider_sandbox: dict[str, Any] | None = None
    if normalized == "linux":
        # The local provider sandbox needs a pinned bubblewrap and the
        # read-only provider homes.
        bubblewrap_spec = cli_overrides.get("bubblewrap")
        bubblewrap_override = _cli_override(bubblewrap_spec) or env.get("LH_BUBBLEWRAP") or None
        bubblewrap = discover_executable(
            "bwrap", explicit=bubblewrap_override, environ=env, home=home_path, name="bubblewrap")
        bubblewrap["version"] = _bubblewrap_version(bubblewrap.get("path"))
        cli["bubblewrap"] = bubblewrap
        provider_sandbox = {
            "provider_home_ro_binds": {
                name: _provider_home_binds(name, environ=env, home=home_path)
                for name in cli["providers"]
            },
        }
        if isinstance(override_data.get("provider_sandbox"), Mapping):
            _deep_update(provider_sandbox, override_data["provider_sandbox"])

    instance_id = override_data.get("instance_id")
    if not isinstance(instance_id, str) or not instance_id.strip():
        instance_id = "instance-" + hashlib.sha256(str(config_path.resolve(strict=False)).encode("utf-8")).hexdigest()[:16]
    secret_store = {
        "backend": "os-native",
        "namespace": f"lh-host/{instance_id}",
    }
    if isinstance(override_data.get("secret_store"), Mapping):
        _deep_update(secret_store, override_data["secret_store"])
    egress = {"path": "egress-policy.json", "generated": True}
    if isinstance(override_data.get("egress_policy"), Mapping):
        _deep_update(egress, override_data["egress_policy"])
    data: dict[str, Any] = {
        "schema": INSTANCE_CONFIG_SCHEMA,
        "instance_id": instance_id,
        "platform": normalized,
        "paths": paths,
        "cli": cli,
        "secret_store": secret_store,
        "egress_policy": egress,
    }
    if provider_sandbox is not None:
        data["provider_sandbox"] = provider_sandbox
    for key in ("secret_store", "egress_policy"):
        if isinstance(override_data.get(key), Mapping):
            _deep_update(data[key], override_data[key])
    return data


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _backup_path(path: Path) -> Path:
    return path.with_name(path.name + ".bak")


def backup_config(path: str | Path) -> Path:
    source = Path(path).expanduser().resolve(strict=False)
    if not source.is_file():
        raise InstanceConfigError(f"instance config not found: {source}")
    target = _backup_path(source)
    _atomic_write(target, source.read_bytes())
    return target


class InstanceConfig:
    """Validated instance config with resolved path and policy projections."""

    def __init__(self, path: str | Path, data: Mapping[str, Any]):
        self.path = Path(path).expanduser().resolve(strict=False)
        self.data = json.loads(json.dumps(dict(data), ensure_ascii=False))
        self.validate()

    @classmethod
    def load(cls, path: str | Path) -> "InstanceConfig":
        config_path = Path(path).expanduser().resolve(strict=False)
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise InstanceConfigError(f"instance config unreadable: {config_path}") from exc
        if not isinstance(data, dict):
            raise InstanceConfigError("instance config must be a JSON object")
        return cls(config_path, data)

    @classmethod
    def from_data(cls, path: str | Path, data: Mapping[str, Any]) -> "InstanceConfig":
        return cls(path, data)

    def validate(self) -> None:
        if self.data.get("schema") != INSTANCE_CONFIG_SCHEMA:
            raise InstanceConfigError(f"unsupported instance config schema: {self.data.get('schema')!r}")
        for field in ("instance_id", "platform", "paths", "cli", "secret_store", "egress_policy"):
            if field not in self.data:
                raise InstanceConfigError(f"instance config missing required field: {field}")
        if not isinstance(self.data["instance_id"], str) or not self.data["instance_id"].strip():
            raise InstanceConfigError("instance_id must be a non-empty string")
        paths = self.data["paths"]
        if not isinstance(paths, dict) or any(not isinstance(paths.get(key), str) or not paths[key].strip() for key in PATH_KEYS):
            raise InstanceConfigError("paths must contain non-empty repo/state/workspace/cache/logs strings")
        cli = self.data["cli"]
        if not isinstance(cli, dict) or not isinstance(cli.get("providers"), dict):
            raise InstanceConfigError("cli must contain a providers object")
        for name, entry in cli["providers"].items():
            if not isinstance(name, str) or PROVIDER_NAME_RE.fullmatch(name) is None:
                raise InstanceConfigError(f"cli.providers has invalid provider name: {name!r}")
            if not isinstance(entry, dict) or not isinstance(entry.get("command"), str) or not entry["command"].strip():
                raise InstanceConfigError(f"cli.providers.{name} must contain a command")
            self._validate_cli_entry(entry, f"cli.providers.{name}")
        if "bubblewrap" in cli:
            bubblewrap = cli["bubblewrap"]
            if not isinstance(bubblewrap, dict) or not isinstance(bubblewrap.get("command"), str):
                raise InstanceConfigError("cli.bubblewrap must contain a command")
            self._validate_cli_entry(bubblewrap, "cli.bubblewrap")
            if bubblewrap.get("version") is not None and not isinstance(bubblewrap["version"], str):
                raise InstanceConfigError("cli.bubblewrap.version must be a string or null")
        sandbox = self.data.get("provider_sandbox")
        if sandbox is not None:
            homes = sandbox.get("provider_home_ro_binds") if isinstance(sandbox, dict) else None
            if not isinstance(homes, dict) or any(
                not isinstance(values, list) or any(not isinstance(item, str) for item in values)
                for values in homes.values()
            ):
                raise InstanceConfigError("provider_sandbox.provider_home_ro_binds must map names to path lists")
        secret = self.data["secret_store"]
        if not isinstance(secret, dict) or not isinstance(secret.get("backend"), str) or not isinstance(secret.get("namespace"), str):
            raise InstanceConfigError("secret_store must contain backend and namespace strings")
        if not secret["backend"].strip() or not secret["namespace"].strip():
            raise InstanceConfigError("secret_store backend and namespace must be non-empty")
        egress = self.data["egress_policy"]
        if not isinstance(egress, dict) or not isinstance(egress.get("path"), str) or not egress["path"].strip():
            raise InstanceConfigError("egress_policy.path must be a non-empty string")

    @staticmethod
    def _validate_cli_entry(entry: Mapping[str, Any], label: str) -> None:
        path = entry.get("path")
        digest = entry.get("sha256")
        if path is not None and not isinstance(path, str):
            raise InstanceConfigError(f"{label}.path must be a string or null")
        if digest is not None and (not isinstance(digest, str) or not digest.startswith("sha256:")):
            raise InstanceConfigError(f"{label}.sha256 must be a sha256 digest or null")
        for forbidden in ("token", "password", "credential", "secret"):
            if any(forbidden in str(key).lower() for key in entry):
                raise InstanceConfigError(f"{label} cannot contain credential fields")

    @property
    def config_digest(self) -> str:
        return digest_json(self.data)

    def resolved_paths(self) -> dict[str, str]:
        return {key: str(_absolute(self.data["paths"][key], anchor=self.path.parent)) for key in PATH_KEYS}

    def resolve_path(self, kind: str, value: str | Path) -> str:
        if kind not in PATH_KEYS:
            raise InstanceConfigError(f"unknown instance path kind: {kind}")
        candidate = Path(value).expanduser()
        if candidate.is_absolute():
            return str(candidate.resolve(strict=False))
        root = Path(self.resolved_paths()[kind])
        return str((root / candidate).resolve(strict=False))

    @property
    def egress_policy_path(self) -> Path:
        return Path(self.resolve_path("state", self.data["egress_policy"]["path"]))

    def build_egress_policy(self) -> dict[str, Any]:
        self.validate()
        def pinned(entry: Mapping[str, Any]) -> dict[str, Any]:
            return {
                "path": entry.get("path"),
                "sha256": entry.get("sha256"),
                "discovery": entry.get("source"),
            }

        providers: dict[str, Any] = {}
        for name, entry in self.data["cli"]["providers"].items():
            configured_rules = entry.get("policy") if isinstance(entry.get("policy"), Mapping) else {}
            rules = dict(OPTIONAL_PROVIDER_RULES.get(name, GENERIC_PROVIDER_RULES))
            rules.update(dict(configured_rules))
            providers[name] = {
                **pinned(entry),
                **rules,
            }
        policy: dict[str, Any] = {
            "schema": EGRESS_POLICY_SCHEMA,
            "issuer": "host-instance",
            "enforced_by": EGRESS_POLICY_ENFORCED_BY,
            "declared": "Per-installation client preflight policy; credentials and host mediation remain outside this artifact.",
            "agent_context_schema_version": 1,
            "providers": providers,
            "instance_binding": {
                "instance_id": self.data["instance_id"],
                "config_digest": self.config_digest,
                "platform": self.data["platform"],
            },
        }
        profile = self.provider_sandbox_profile()
        if profile is not None:
            policy["provider_sandbox_profile"] = profile
        return policy

    def provider_sandbox_profile(self) -> dict[str, Any] | None:
        """The generated local provider-sandbox profile (Linux with bubblewrap)."""
        bubblewrap = self.data["cli"].get("bubblewrap")
        sandbox = self.data.get("provider_sandbox")
        if (
            self.data.get("platform") != "linux"
            or not isinstance(bubblewrap, dict)
            or not bubblewrap.get("path")
            or not bubblewrap.get("sha256")
            or not bubblewrap.get("version")
            or not isinstance(sandbox, dict)
        ):
            return None
        provider_roots = [
            root
            for entry in self.data["cli"]["providers"].values()
            for root in _provider_ro_roots(entry.get("path"))
        ]
        homes = sandbox.get("provider_home_ro_binds") or {}
        return {
            "declared": "Generated per installation for local provider launches; the profile is signed into each launch descriptor.",
            "bubblewrap": {
                "path": bubblewrap["path"],
                "sha256": bubblewrap["sha256"],
                "version": bubblewrap["version"],
            },
            "network": "host",
            "ro_binds": sorted({*PROVIDER_SANDBOX_SYSTEM_RO_BINDS, *provider_roots}),
            "provider_home_ro_binds": {
                str(name): [str(item) for item in values]
                for name, values in sorted(homes.items())
                if isinstance(values, list)
            },
            "env": {"TERM": "xterm-256color"},
            "flags": list(PROVIDER_SANDBOX_FLAGS),
            "seccomp": {
                "default": "allow",
                "denied_syscalls": list(PROVIDER_SANDBOX_DEFAULT_DENIED_SYSCALLS),
            },
        }

    def write_egress_policy(self) -> Path:
        path = self.egress_policy_path
        raw = json.dumps(self.build_egress_policy(), ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        _atomic_write(path, raw)
        return path

    def policy_digest(self) -> str | None:
        return digest_file(self.egress_policy_path)

    def policy_binding_status(self) -> str:
        try:
            policy = json.loads(self.egress_policy_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return "missing_or_invalid"
        binding = policy.get("instance_binding") if isinstance(policy, dict) else None
        if (
            isinstance(policy, dict)
            and policy.get("schema") == EGRESS_POLICY_SCHEMA
            and isinstance(binding, dict)
            and binding.get("instance_id") == self.data["instance_id"]
            and binding.get("config_digest") == self.config_digest
        ):
            return "ok"
        return "config_digest_mismatch"

    def readback(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema": INSTANCE_CONFIG_SCHEMA,
            "config_path": str(self.path),
            "config_digest": self.config_digest,
            "instance_id": self.data["instance_id"],
            "platform": self.data["platform"],
            "paths": self.resolved_paths(),
            "cli": self.data["cli"],
            "secret_store": {
                "backend": self.data["secret_store"]["backend"],
                "namespace": self.data["secret_store"]["namespace"],
            },
            "egress_policy": {
                "path": str(self.egress_policy_path),
                "digest": self.policy_digest(),
                "config_digest": self.config_digest,
                "binding_status": self.policy_binding_status(),
            },
        }

    def environment_overlay(self, environ: Mapping[str, str] | None = None) -> dict[str, str]:
        self.validate()
        env = _environment(environ)
        overlay = {
            INSTANCE_CONFIG_ENV: str(self.path),
            INSTANCE_ROOT_ENV: str(self.path.parent),
            "LH_EGRESS_POLICY": str(self.egress_policy_path),
        }
        cli_dirs: list[str] = []
        for name, entry in self.data["cli"]["providers"].items():
            path = entry.get("path")
            if isinstance(path, str) and path.strip():
                overlay[f"LH_PROVIDER_{name.upper()}_CLI"] = path
                cli_dirs.append(str(Path(path).parent))
        if cli_dirs:
            current = env.get("PATH", "")
            overlay["PATH"] = os.pathsep.join(dict.fromkeys([*cli_dirs, current])) if current else os.pathsep.join(dict.fromkeys(cli_dirs))
        return overlay

    def save(self, *, backup: bool = True) -> Path:
        self.validate()
        if backup and self.path.is_file():
            backup_config(self.path)
        raw = json.dumps(self.data, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        _atomic_write(self.path, raw)
        return self.path


def initialize_instance(
    config_path: str | Path,
    *,
    system: str | None = None,
    environ: Mapping[str, str] | None = None,
    home: str | Path | None = None,
    cwd: str | Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> InstanceConfig:
    path = Path(config_path).expanduser().resolve(strict=False)
    data = _default_config_data(path, system=system, environ=environ, home=home, cwd=cwd, overrides=overrides)
    instance = InstanceConfig.from_data(path, data)
    instance.save(backup=path.is_file())
    instance.write_egress_policy()
    return InstanceConfig.load(path)


def migrate_config(
    config_path: str | Path,
    *,
    system: str | None = None,
    environ: Mapping[str, str] | None = None,
    home: str | Path | None = None,
    cwd: str | Path | None = None,
) -> InstanceConfig:
    path = Path(config_path).expanduser().resolve(strict=False)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InstanceConfigError(f"legacy instance config unreadable: {path}") from exc
    if isinstance(raw, dict) and raw.get("schema") == INSTANCE_CONFIG_SCHEMA:
        return InstanceConfig.load(path)
    if not isinstance(raw, dict) or raw.get("schema") not in {None, LEGACY_INSTANCE_CONFIG_SCHEMA}:
        raise InstanceConfigError(f"cannot migrate instance config schema: {raw.get('schema') if isinstance(raw, dict) else None!r}")
    legacy_paths = raw.get("paths") if isinstance(raw.get("paths"), Mapping) else {}
    overrides: dict[str, Any] = {"paths": {}}
    for key in PATH_KEYS:
        value = legacy_paths.get(key) or raw.get(f"{key}_root")
        if isinstance(value, str) and value.strip():
            overrides["paths"][key] = value
    for key in ("instance_id", "platform", "cli", "secret_store", "egress_policy"):
        if key in raw:
            overrides[key] = raw[key]
    data = _default_config_data(path, system=system, environ=environ, home=home, cwd=cwd, overrides=overrides)
    migrated = InstanceConfig.from_data(path, data)
    migrated.save(backup=True)
    migrated.write_egress_policy()
    return InstanceConfig.load(path)


def rollback_config(config_path: str | Path, backup_path: str | Path | None = None) -> InstanceConfig:
    path = Path(config_path).expanduser().resolve(strict=False)
    source = Path(backup_path).expanduser().resolve(strict=False) if backup_path is not None else _backup_path(path)
    if not source.is_file():
        raise InstanceConfigError(f"instance config backup not found: {source}")
    try:
        raw = source.read_bytes()
        data = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InstanceConfigError(f"instance config unreadable: {source}") from exc
    if not isinstance(data, dict):
        raise InstanceConfigError("instance config must be a JSON object")
    InstanceConfig.from_data(path, data)
    _atomic_write(path, raw)
    return InstanceConfig.load(path)


@contextlib.contextmanager
def temporary_environment(overlay: Mapping[str, str]) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in overlay}
    try:
        os.environ.update({str(key): str(value) for key, value in overlay.items()})
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage an LH instance config")
    subparsers = parser.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init")
    init.add_argument("--config", required=True)
    init.add_argument("--repo-root")
    init.add_argument("--state-root")
    init.add_argument("--workspace-root")
    init.add_argument("--cache-root")
    init.add_argument("--logs-root")
    readback = subparsers.add_parser("readback")
    readback.add_argument("--config", required=True)
    migrate = subparsers.add_parser("migrate")
    migrate.add_argument("--config", required=True)
    rollback = subparsers.add_parser("rollback")
    rollback.add_argument("--config", required=True)
    rollback.add_argument("--backup")
    args = parser.parse_args(argv)
    if args.command == "init":
        paths = {key: getattr(args, f"{key.replace('-', '_')}_root") for key in PATH_KEYS}
        paths = {key: value for key, value in paths.items() if value}
        instance = initialize_instance(args.config, overrides={"paths": paths})
    elif args.command == "readback":
        instance = InstanceConfig.load(args.config)
    elif args.command == "migrate":
        instance = migrate_config(args.config)
    else:
        instance = rollback_config(args.config, args.backup)
    print(json.dumps(instance.readback(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

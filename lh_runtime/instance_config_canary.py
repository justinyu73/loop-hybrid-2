#!/usr/bin/env python3
"""Provider-free checks for the instance config contract."""

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from instance_config import (  # noqa: E402
    INSTANCE_CONFIG_ENV,
    INSTANCE_CONFIG_SCHEMA,
    INSTANCE_ROOT_ENV,
    InstanceConfigError,
    InstanceConfig,
    default_instance_config_path,
    discover_instance_config,
    initialize_instance,
    migrate_config,
    rollback_config,
    temporary_environment,
)


def case(case_id: str, ok: bool, detail: str) -> dict[str, object]:
    return {"id": case_id, "ok": ok, "detail": detail}


def _fake_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":  # Windows runs a file by its PATHEXT suffix, not an execute bit
        path = path.with_name(path.name + ".cmd")
        path.write_text("@exit /b 0\r\n", encoding="utf-8")
        return path
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def _contains_credential_key(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            token in str(key).lower()
            for key in value
            for token in ("token", "password", "credential")
        ) or any(_contains_credential_key(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_credential_key(item) for item in value)
    return False


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lh-instance-config-") as raw:
        root = Path(raw)
        home = root / "clean user" / "使用者"
        fake_bin = root / "fake bin"
        fake_coder = _fake_executable(fake_bin / "coder")
        fake_reviewer = _fake_executable(fake_bin / "reviewer")
        fake_synthetic = _fake_executable(fake_bin / "synthetic")
        env = {
            "HOME": str(home),
            "PATH": str(fake_bin),
            "XDG_CONFIG_HOME": str(root / "xdg config"),
            "XDG_DATA_HOME": str(root / "xdg data"),
            "XDG_STATE_HOME": str(root / "xdg state"),
            "XDG_CACHE_HOME": str(root / "xdg cache"),
        }
        linux_path = default_instance_config_path(system="Linux", environ=env, home=home)
        windows_path = default_instance_config_path(
            system="Windows",
            environ={**env, "LOCALAPPDATA": str(root / "D drive" / "LocalAppData")},
            home=home,
        )
        macos_path = default_instance_config_path(system="Darwin", environ=env, home=home)
        platform_defaults = (
            str(root / "xdg config" / "lh-host" / "instance.json") == str(linux_path)
            and "LocalAppData" in str(windows_path)
            and Path(macos_path).as_posix().endswith("/Library/Application Support/lh-host/instance.json")
        )

        configured_paths = {
            "repo": str(root / "D drive" / "project repo"),
            "state": str(root / "D drive" / "instance state"),
            "workspace": str(root / "D drive" / "workspace with spaces"),
            "cache": str(root / "D drive" / "cache"),
            "logs": str(root / "D drive" / "logs"),
        }
        config_path = root / "config" / "instance.json"
        config = initialize_instance(
            config_path,
            system="Linux",
            environ=env,
            home=home,
            cwd=root,
            overrides={
                "paths": configured_paths,
                "cli": {
                    "providers": {
                        "coder": str(fake_coder),
                        "reviewer": str(fake_reviewer),
                        "synthetic": str(fake_synthetic),
                    },
                },
            },
        )
        readback = config.readback()
        policy_path = config.egress_policy_path
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        config_and_policy = (
            readback["schema"] == INSTANCE_CONFIG_SCHEMA
            and all(readback["paths"][key] == str(Path(value).resolve()) for key, value in configured_paths.items())
            and readback["egress_policy"]["path"] == str(policy_path)
            and isinstance(readback["egress_policy"]["digest"], str)
            and readback["egress_policy"]["binding_status"] == "ok"
            and policy["schema"] == "host-execution-host-egress-policy/v1"
            and policy["instance_binding"]["config_digest"] == readback["config_digest"]
            and policy["providers"]["coder"]["sha256"] is not None
        )
        cli_discovery = (
            config.data["cli"]["providers"]["coder"]["path"] == str(fake_coder.resolve())
            and config.data["cli"]["providers"]["reviewer"]["path"] == str(fake_reviewer.resolve())
            and config.data["cli"]["providers"]["synthetic"]["path"] == str(fake_synthetic.resolve())
        )
        secret_name_only = (
            set(config.data["secret_store"]) == {"backend", "namespace"}
            and not _contains_credential_key(config.data["secret_store"])
            and not _contains_credential_key(policy)
        )

        overlay = config.environment_overlay({"PATH": "base-path"})
        old_probe = os.environ.get("LH_INSTANCE_CANARY_PROBE")
        with temporary_environment({**overlay, "LH_INSTANCE_CANARY_PROBE": "inside"}):
            env_scoped = (
                os.environ.get("LH_INSTANCE_CANARY_PROBE") == "inside"
                and os.environ.get("LH_INSTANCE_CONFIG") == str(config.path)
                and os.environ.get("LH_EGRESS_POLICY") == str(policy_path)
                and str(fake_bin) in os.environ.get("PATH", "")
            )
        env_restored = os.environ.get("LH_INSTANCE_CANARY_PROBE") == old_probe

        discovered = discover_instance_config(
            environ={"LH_INSTANCE_CONFIG": str(config_path)},
            include_default=False,
        )
        discovery_ok = discovered == config.path

        missing_explicit = []
        for key, value in (
            (INSTANCE_CONFIG_ENV, root / "missing-config" / "instance.json"),
            (INSTANCE_ROOT_ENV, root / "missing-root"),
        ):
            try:
                discover_instance_config(environ={key: str(value)}, include_default=False)
            except InstanceConfigError:
                missing_explicit.append(True)
            else:
                missing_explicit.append(False)
        missing_explicit_fails_closed = all(missing_explicit)

        legacy_path = root / "legacy" / "instance.json"
        legacy_path.parent.mkdir(parents=True, exist_ok=True)
        legacy_path.write_text(
            json.dumps(
                {
                    "schema": "lh-instance-config/v0",
                    "instance_id": "legacy-instance",
                    "paths": {"repo": "legacy-repo", "state": "legacy-state", "workspace": "legacy-workspace", "cache": "legacy-cache", "logs": "legacy-logs"},
                    "secret_store": {"backend": "os-native", "namespace": "lh-host/legacy-instance"},
                }
            ),
            encoding="utf-8",
        )
        migrated = migrate_config(legacy_path, system="Linux", environ=env, home=home, cwd=root)
        migration_ok = (
            migrated.data["schema"] == INSTANCE_CONFIG_SCHEMA
            and _backup_path(legacy_path).is_file()
            and migrated.data["instance_id"] == "legacy-instance"
            and migrated.readback()["paths"]["state"] == str((legacy_path.parent / "legacy-state").resolve())
        )
        old_state = migrated.data["paths"]["state"]
        migrated.data["paths"]["state"] = str(root / "new-state")
        migrated.save()
        rolled_back = rollback_config(legacy_path)
        rollback_ok = rolled_back.data["paths"]["state"] == old_state

        cases = [
            case("platform-defaults-are-native-and-derived", platform_defaults, str({"linux": linux_path, "windows": windows_path, "macos": macos_path})),
            case("paths-cli-and-policy-are-instance-owned", config_and_policy, json.dumps(readback, ensure_ascii=False)),
            case("cli-discovery-records-absolute-digests", cli_discovery, json.dumps(config.data["cli"], ensure_ascii=False)),
            case("secret-store-is-name-only", secret_name_only, json.dumps(config.data["secret_store"], ensure_ascii=False)),
            case("runtime-environment-is-scoped-and-restored", env_scoped and env_restored, str({"scoped": env_scoped, "restored": env_restored})),
            case("explicit-instance-discovery-is-read-only", discovery_ok, str(discovered)),
            case("missing-explicit-instance-does-not-fallback", missing_explicit_fails_closed, str(missing_explicit)),
            case("legacy-config-migrates-with-backup", migration_ok, json.dumps(migrated.readback(), ensure_ascii=False)),
            case("config-rollback-restores-backup", rollback_ok, str(rolled_back.readback()["paths"]["state"])),
        ]
    failures = [{"id": item["id"], "detail": item["detail"]} for item in cases if not item["ok"]]
    print(
        json.dumps(
            {
                "check_id": "lh-instance-config",
                "status": "pass" if not failures else "fail",
                "total": len(cases),
                "blocking_failures": failures,
                "known_gaps_open": [
                    "OS keychain calls and platform service registration remain later installer/lifecycle adapter work",
                    "generated policy is an instance artifact; live execution-fence backend proof remains a separate P4 gate",
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not failures else 1


def _backup_path(path: Path) -> Path:
    return path.with_name(path.name + ".bak")


if __name__ == "__main__":
    raise SystemExit(main())

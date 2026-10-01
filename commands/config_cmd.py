from __future__ import annotations

from typing import Any

import yaml
from pathlib import Path

from mq import config as config_module
from mq import inventory
from mq.config import (
    DEFAULT_CONFIG,
    VALID_AUTH,
    ConfigError,
    load_config,
    save_config,
    set_profile,
)
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim

CONNECTION_KEYS = tuple(DEFAULT_CONFIG.keys())


def _config_file() -> Path:
    """Read the path from the module on every call.

    Binding ``CONFIG_FILE`` at import time makes the value go stale as soon as
    anything reassigns it, which is how a test or a future ``--config-dir`` flag
    would end up writing to the wrong place.
    """
    return config_module.CONFIG_FILE


def execute(
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    vhost: str | None = None,
    ssl: bool | None = None,
    ssl_verify: bool | None = None,
    ssl_cafile: str | None = None,
    heartbeat: int | None = None,
    connection_timeout: int | None = None,
    management_port: int | None = None,
    blocked_connection_timeout: int | None = None,
    connection_attempts: int | None = None,
    auth: str | None = None,
    token: str | None = None,
    password_command: str | None = None,
    connection_name: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    profile: str | None = None,
    target_profile: str | None = None,
    set_active: bool = False,
    show: bool = False,
    list_profiles: bool = False,
    unset: list[str] | None = None,
) -> None:
    if show:
        _show(show_profiles=list_profiles)
        return
    if list_profiles:
        _list_profiles()
        return

    current: dict[str, Any] = {}
    if _config_file().exists():
        try:
            with open(_config_file(), encoding="utf-8") as handle:
                current = yaml.safe_load(handle) or {}
        except (OSError, yaml.YAMLError) as e:
            handle_error(f"Cannot read {_config_file()}: {e}")
            return
    if not isinstance(current, dict):
        handle_error(f"Invalid config file (expected a mapping): {_config_file()}")
        return

    # Where do this run's settings go: a named profile, or the top level?
    destination = target_profile or profile
    if destination:
        profiles = current.get("profiles")
        if profiles is None:
            # Must be attached to `current` before it is written, or the new
            # profile is created, reported as saved, and then silently dropped.
            profiles = {}
            current["profiles"] = profiles
        if not isinstance(profiles, dict):
            handle_error("'profiles' in the config file must be a mapping.")
            return
        target = profiles.get(destination)
        if target is None:
            target = {}
            profiles[destination] = target
        if not isinstance(target, dict):
            handle_error(f"Profile '{destination}' must be a mapping.")
            return
    else:
        target = current

    overrides = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "vhost": vhost,
        "ssl": ssl,
        "ssl_verify": ssl_verify,
        "ssl_cafile": ssl_cafile,
        "heartbeat": heartbeat,
        "connection_timeout": connection_timeout,
        "management_port": management_port,
        "blocked_connection_timeout": blocked_connection_timeout,
        "connection_attempts": connection_attempts,
        "auth": auth,
        "token": token,
        "password_command": password_command,
        "connection_name": connection_name,
        "client_cert": client_cert,
        "client_key": client_key,
    }
    if auth and auth not in VALID_AUTH:
        handle_error(f"--auth must be one of: {', '.join(VALID_AUTH)}")
        return

    provided = {k: v for k, v in overrides.items() if v is not None}
    for key in unset or []:
        if key not in CONNECTION_KEYS:
            handle_error(
                f"Cannot unset '{key}'. Known settings: {', '.join(CONNECTION_KEYS)}"
            )
            return
        target.pop(key, None)
        provided[key] = None

    before = dict(target)
    target.update({k: v for k, v in provided.items() if v is not None})

    if set_active and destination:
        current["active_profile"] = destination

    # Validate the shape the config will actually have after saving.
    effective = {**DEFAULT_CONFIG, **current}
    if target is not current:
        effective = {
            **DEFAULT_CONFIG,
            **{k: v for k, v in current.items() if k not in ("profiles", "active_profile")},
            **target,
        }
    missing: list[str] = []
    if not effective.get("host"):
        missing.append("host")
    if effective.get("auth", "plain") == "plain" and not effective.get("username"):
        missing.append("username")
    if (
        effective.get("auth", "plain") == "plain"
        and not effective.get("password")
        and not effective.get("password_command")
    ):
        missing.append("password")

    if missing and target is current:
        handle_error(
            f"Cannot save an incomplete config: missing {', '.join(missing)}. "
            "Pass them as flags, or configure a profile that is completed later."
        )
        return

    save_config(current)

    if is_json_mode():
        print_json(
            {
                "file": str(_config_file()),
                "profile": destination,
                "changed": {
                    k: ("(removed)" if v is None else v)
                    for k, v in provided.items()
                    if before.get(k) != v
                },
            }
        )
        return

    changed = [(k, before.get(k), v) for k, v in provided.items() if before.get(k) != v]
    print_pairs(
        [
            ("Config file", str(_config_file())),
            ("Profile", destination or "(top level)"),
        ],
        title="Configuration saved",
    )
    if changed:
        print()
        for key, old, new in changed:
            if new is None:
                print(f"  {key}: (removed)")
            elif old is None:
                print(f"  {key}: {_redact(key, new)} (new)")
            else:
                print(f"  {key}: {_redact(key, new)} (was: {_redact(key, old)})")
    if destination:
        print()
        print(dim(f"  use it with: mq --profile {destination} <command>"))
    if not inventory.is_writable():
        print()
        print(dim(f"  note: {inventory.path()} is not writable; queue tracking is disabled."))


def _redact(key: str, value: Any) -> str:
    if key in ("password", "token") and value:
        return "****"
    return str(value)


def _show(show_profiles: bool = False) -> None:
    # Note: the active profile is left alone, so 'mq --profile X config --show'
    # reports X rather than the flat top level.
    try:
        config = load_config()
    except ConfigError as e:
        if is_json_mode():
            print_json({"config_file": str(_config_file()), "error": str(e)})
            return
        print_pairs(
            [("Config file", str(_config_file())), ("Status", f"incomplete: {e}")],
            title="Configuration",
        )
        raise SystemExit(1)

    data = {
        "host": config.host,
        "port": config.port,
        "username": config.username,
        "password": "****" if config.password else "(not set)",
        "vhost": config.vhost,
        "tls": config.ssl,
        "tls_verify": config.ssl_verify,
        "tls_cafile": config.ssl_cafile,
        "heartbeat": config.heartbeat,
        "connection_timeout": config.connection_timeout,
        "blocked_connection_timeout": config.blocked_connection_timeout,
        "connection_attempts": config.connection_attempts,
        "auth": config.auth,
        "management_port": config.management_port,
        "source": config.describe_source(),
        "config_file": str(_config_file()),
        "inventory_file": str(inventory.path()),
    }
    if is_json_mode():
        print_json(data)
        return

    pairs: list[tuple[str, object]] = [
        ("Host", config.host),
        ("Port", config.port),
        ("Username", config.username),
        ("Password", "****" if config.password else "(not set)"),
        ("VHost", config.vhost),
        ("TLS", "yes" if config.ssl else "no"),
    ]
    if config.ssl:
        pairs += [
            ("TLS verify", "yes" if config.ssl_verify else "no (INSECURE)"),
            ("TLS CA file", config.ssl_cafile or "(system store)"),
        ]
    if config.auth != "plain":
        pairs.append(("Auth", config.auth))
    if config.auth == "external":
        pairs.append(("Client cert", config.client_cert or "(from ssl context)"))
    if config.password_command:
        pairs.append(("Password from", config.password_command))
    pairs += [
        ("Heartbeat", f"{config.heartbeat}s"),
        ("Connect timeout", f"{config.connection_timeout}s"),
        ("Blocked timeout", f"{config.blocked_connection_timeout}s"),
        ("Connect attempts", config.connection_attempts),
        ("Management port", config.management_port or "(plugin API unused)"),
        ("Loaded from", config.describe_source()),
        ("Config file", _config_file()),
        ("Inventory file", inventory.path()),
    ]
    print_pairs(pairs, title="Configuration")

    if show_profiles:
        _print_profiles()


def _print_profiles() -> None:
    raw: dict[str, Any] = {}
    if _config_file().exists():
        try:
            with open(_config_file(), encoding="utf-8") as handle:
                raw = yaml.safe_load(handle) or {}
        except (OSError, yaml.YAMLError):
            raw = {}
    profiles = raw.get("profiles") or {}
    if not isinstance(profiles, dict) or not profiles:
        print()
        print(dim("  No profiles defined. Create one with: mq config --profile staging --host ..."))
        return
    active = raw.get("active_profile")
    print()
    print_pairs(
        [
            (name, "active" if name == active else (profile.get("host") if isinstance(profile, dict) else ""))
            for name, profile in sorted(profiles.items())
        ],
        title="Profiles",
    )


def _list_profiles() -> None:
    _print_profiles()

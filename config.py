from __future__ import annotations

import os
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlparse

import yaml

CONFIG_DIR = Path.home() / ".config" / "mq"
CONFIG_FILE = CONFIG_DIR / "config.yaml"

ENV_MAP = {
    "host": "MQ_HOST",
    "port": "MQ_PORT",
    "username": "MQ_USERNAME",
    "password": "MQ_PASSWORD",
    "vhost": "MQ_VHOST",
    "ssl": "MQ_SSL",
    "ssl_verify": "MQ_SSL_VERIFY",
    "ssl_cafile": "MQ_SSL_CAFILE",
    "heartbeat": "MQ_HEARTBEAT",
    "connection_timeout": "MQ_CONNECTION_TIMEOUT",
    "management_port": "MQ_MANAGEMENT_PORT",
    "blocked_connection_timeout": "MQ_BLOCKED_CONNECTION_TIMEOUT",
    "connection_attempts": "MQ_CONNECTION_ATTEMPTS",
    "auth": "MQ_AUTH",
    "token": "MQ_TOKEN",
    "password_command": "MQ_PASSWORD_COMMAND",
    "connection_name": "MQ_CONNECTION_NAME",
    "client_cert": "MQ_CLIENT_CERT",
    "client_key": "MQ_CLIENT_KEY",
}

INT_FIELDS = (
    "port",
    "heartbeat",
    "connection_timeout",
    "management_port",
    "blocked_connection_timeout",
    "connection_attempts",
)
BOOL_FIELDS = ("ssl", "ssl_verify")

# Keys that live only in the config file, not in the flat connection settings.
NON_CONNECTION_KEYS = ("profiles", "active_profile")

DEFAULT_CONFIG: dict[str, Any] = {
    "host": "",
    "port": 5672,
    "username": "",
    "password": "",
    "vhost": "/",
    "ssl": False,
    "ssl_verify": True,
    "ssl_cafile": None,
    "heartbeat": 60,
    "connection_timeout": 10,
    "management_port": None,
    "blocked_connection_timeout": 30,
    "connection_attempts": 1,
    "auth": "plain",
    "token": None,
    "password_command": None,
    "connection_name": None,
    "client_cert": None,
    "client_key": None,
}

VALID_AUTH = ("plain", "external", "oauth")


class ConfigError(Exception):
    """Raised when no usable connection configuration can be assembled."""


@dataclass
class MQConfig:
    host: str
    port: int
    username: str
    password: str
    vhost: str
    ssl: bool = False
    ssl_verify: bool = True
    ssl_cafile: str | None = None
    heartbeat: int = 60
    connection_timeout: int = 10
    management_port: int | None = None
    blocked_connection_timeout: int = 30
    connection_attempts: int = 1
    auth: str = "plain"
    token: str | None = None
    password_command: str | None = None
    connection_name: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    source: str = "config file"
    profile: str | None = None

    @property
    def redacted(self) -> "MQConfig":
        """A copy safe to print: password and token replaced with a placeholder."""
        clone = MQConfig(**{**self.__dict__})
        if clone.password:
            clone.password = "****"
        if clone.token:
            clone.token = "****"
        return clone

    def describe_source(self) -> str:
        parts = [self.source]
        if self.profile:
            parts.append(f"profile '{self.profile}'")
        return " + ".join(parts)


_overrides: dict[str, Any] = {}
_active_profile: str | None = None


def set_overrides(**kwargs: Any) -> None:
    """Record CLI flags. Only values explicitly provided are stored."""
    _overrides.clear()
    _overrides.update({k: v for k, v in kwargs.items() if v is not None})


def set_profile(name: str | None) -> None:
    global _active_profile
    _active_profile = name


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _as_int(value: Any, key: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigError(
            f"{key} must be a whole number, got {value!r}"
        ) from None


def _resolve_secret(config: dict[str, Any]) -> str:
    """Resolve the password, allowing an external command to supply it.

    On POSIX the command is split without a shell, so a value like
    ``pass show mq/prod`` cannot be turned into something else by shell
    metacharacters. Windows has no equivalent safe split for a bare command
    string, so it runs through the shell there.
    """
    command = config.get("password_command")
    if not command:
        return str(config.get("password") or "")

    import subprocess

    use_shell = os.name == "nt"
    argv: Any = command if use_shell else shlex.split(command)
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=15,
            shell=use_shell,
        )
    except (OSError, ValueError) as exc:
        raise ConfigError(f"password_command could not be executed: {exc}") from exc
    if result.returncode != 0:
        raise ConfigError(
            f"password_command exited with {result.returncode}: "
            f"{(result.stderr or '').strip()}"
        )
    return result.stdout.rstrip("\r\n")


def _from_url(url: str) -> dict[str, Any]:
    """Parse ``amqp://user:pass@host:port/vhost?heartbeat=..`` into config keys."""
    parsed = urlparse(url)
    if parsed.scheme not in ("amqp", "amqps"):
        raise ConfigError(
            f"Unsupported URL scheme '{parsed.scheme}' in MQ_URL. "
            "Expected amqp:// or amqps://."
        )
    if not parsed.hostname:
        raise ConfigError(f"MQ_URL has no host: {url!r}")

    result: dict[str, Any] = {
        "host": parsed.hostname,
        "port": 5671 if parsed.scheme == "amqps" else 5672,
        "ssl": parsed.scheme == "amqps",
    }
    if parsed.username:
        result["username"] = unquote(parsed.username)
    if parsed.password:
        result["password"] = unquote(parsed.password)

    # "/vhost" - an empty path means the default "/" vhost; "%2f" encodes "/".
    path = parsed.path
    if path.startswith("/") and len(path) > 1:
        result["vhost"] = unquote(path[1:])

    for key, value in parse_qsl(parsed.query):
        if key in INT_FIELDS:
            result[key] = _as_int(value, key)
        elif key in BOOL_FIELDS:
            result[key] = _as_bool(value)
        elif key in ("username", "password", "vhost", "auth", "ssl_cafile", "token"):
            result[key] = value
    return result


def _env_layer() -> dict[str, Any]:
    env_vals: dict[str, Any] = {}
    for key, var in ENV_MAP.items():
        value = os.environ.get(var)
        if value is None or value == "":
            continue
        if key in BOOL_FIELDS:
            env_vals[key] = _as_bool(value)
        elif key in INT_FIELDS:
            parsed = _as_int(value, key)
            if parsed is not None:
                env_vals[key] = parsed
        else:
            env_vals[key] = value

    url = os.environ.get("MQ_URL")
    if url:
        env_vals = {**_from_url(url), **env_vals}

    profile = os.environ.get("MQ_PROFILE")
    if profile:
        env_vals["_profile"] = profile
    return env_vals


def _file_layer() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        with open(CONFIG_FILE, encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError as exc:
        raise ConfigError(f"Cannot read {CONFIG_FILE}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {CONFIG_FILE}: {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"Invalid config file (expected a mapping): {CONFIG_FILE}")
    return data


def _apply_profile(base: dict[str, Any], raw: dict[str, Any], name: str | None) -> tuple[dict[str, Any], str | None]:
    """Merge a named profile over the flat top-level settings."""
    profiles = raw.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        return base, None
    selected = name or raw.get("active_profile")
    if not selected:
        return base, None
    if selected not in profiles:
        known = ", ".join(sorted(profiles)) or "(none)"
        raise ConfigError(
            f"Unknown profile '{selected}'. Available profiles: {known}"
        )
    profile_data = profiles[selected] or {}
    if not isinstance(profile_data, dict):
        raise ConfigError(f"Profile '{selected}' must be a mapping")
    merged = {**base, **{k: v for k, v in profile_data.items() if k not in NON_CONNECTION_KEYS}}
    return merged, selected


def _config_from(merged: dict[str, Any], source: str, profile: str | None) -> MQConfig:
    auth = str(merged.get("auth") or "plain").strip().lower()
    if auth not in VALID_AUTH:
        raise ConfigError(
            f"auth must be one of {', '.join(VALID_AUTH)}, got {auth!r}"
        )

    port = _as_int(merged.get("port", 5672), "port")
    management_port = merged.get("management_port")
    if management_port in ("", None):
        management_port = None
    else:
        management_port = _as_int(management_port, "management_port")

    return MQConfig(
        host=str(merged.get("host") or ""),
        port=port if port is not None else 5672,
        username=str(merged.get("username") or ""),
        password=_resolve_secret(merged),
        vhost=str(merged.get("vhost") or "/"),
        ssl=bool(merged.get("ssl", False)),
        ssl_verify=bool(merged.get("ssl_verify", True)),
        ssl_cafile=(str(merged["ssl_cafile"]) if merged.get("ssl_cafile") else None),
        heartbeat=_as_int(merged.get("heartbeat", 60), "heartbeat") or 0,
        connection_timeout=_as_int(merged.get("connection_timeout", 10), "connection_timeout") or 10,
        management_port=management_port,
        blocked_connection_timeout=(
            _as_int(merged.get("blocked_connection_timeout", 30), "blocked_connection_timeout") or 0
        ),
        connection_attempts=_as_int(merged.get("connection_attempts", 1), "connection_attempts") or 1,
        auth=auth,
        token=(str(merged["token"]) if merged.get("token") else None),
        password_command=(str(merged["password_command"]) if merged.get("password_command") else None),
        connection_name=(str(merged["connection_name"]) if merged.get("connection_name") else None),
        client_cert=(str(merged["client_cert"]) if merged.get("client_cert") else None),
        client_key=(str(merged["client_key"]) if merged.get("client_key") else None),
        source=source,
        profile=profile,
    )


def load_config() -> MQConfig:
    """Assemble the effective configuration.

    Precedence: CLI flags > environment > selected profile > flat config file >
    built-in defaults. A missing config file is not fatal as long as the
    environment supplies enough (this is what makes ``MQ_URL=... mq ping`` and
    CI usage work).
    """
    raw = _file_layer()
    flat = {k: v for k, v in raw.items() if k not in NON_CONNECTION_KEYS}

    env = _env_layer()
    env_profile = env.pop("_profile", None)

    selected = _active_profile or env_profile
    merged, profile = _apply_profile(flat, raw, selected)

    if raw:
        source = "config file"
    elif env:
        source = "environment"
    else:
        source = "defaults"

    merged = {**DEFAULT_CONFIG, **merged, **env, **_overrides}
    config = _config_from(merged, source, profile)

    missing = _missing_fields(config)
    if missing:
        raise ConfigError(_missing_message(missing))
    return config


def _missing_fields(config: MQConfig) -> list[str]:
    missing: list[str] = []
    if not config.host:
        missing.append("host")
    if config.auth == "plain" and not config.username:
        missing.append("username")
    if config.auth == "plain" and not config.password and not config.password_command:
        missing.append("password")
    if config.auth == "oauth" and not (config.token or config.password_command):
        missing.append("token (or password_command)")
    if config.auth == "external" and not config.ssl:
        # EXTERNAL without TLS has no way to present a certificate.
        raise ConfigError(
            "auth: external requires TLS. Set ssl: true (or use an amqps:// URL)."
        )
    return missing


def _missing_message(missing: list[str]) -> str:
    return (
        f"Missing required setting(s): {', '.join(missing)}.\n"
        "  Configure them with:  mq config --host ... --username ... --password ...\n"
        "  Or use the environment: MQ_URL=amqp://user:pass@host:5672/vhost mq ping\n"
        f"  Config file: {CONFIG_FILE}"
    )


def save_config(config: dict[str, Any]) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, default_flow_style=False, sort_keys=False)


def _fail(message: str) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_config_or_exit() -> MQConfig:
    """``load_config`` for the CLI layer: turns errors into a clean message."""
    try:
        return load_config()
    except ConfigError as exc:
        _fail(str(exc))
        raise  # unreachable, keeps type checkers happy


def load_local_config() -> MQConfig:
    """A config good enough for commands that never open a connection.

    ``mq inventory`` reads and writes a local file, so demanding a working
    broker connection to list it would be wrong.
    """
    try:
        return load_config()
    except ConfigError:
        pass

    raw = _file_layer()
    flat = {k: v for k, v in raw.items() if k not in NON_CONNECTION_KEYS}
    env = _env_layer()
    selected = _active_profile or env.pop("_profile", None)
    merged, profile = _apply_profile(flat, raw, selected)
    merged = {**DEFAULT_CONFIG, **merged, **env}
    return _config_from(merged, "local settings", profile)

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

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
    "heartbeat": "MQ_HEARTBEAT",
    "connection_timeout": "MQ_CONNECTION_TIMEOUT",
    "management_port": "MQ_MANAGEMENT_PORT",
}

DEFAULT_CONFIG = {
    "host": "",
    "port": 5672,
    "username": "",
    "password": "",
    "vhost": "/",
    "ssl": False,
    "heartbeat": 60,
    "connection_timeout": 10,
    "management_port": None,
}


@dataclass
class MQConfig:
    host: str
    port: int
    username: str
    password: str
    vhost: str
    ssl: bool = False
    heartbeat: int = 60
    connection_timeout: int = 10
    management_port: int | None = None


_overrides: dict = {}


def set_overrides(**kwargs: object) -> None:
    _overrides.clear()
    _overrides.update({k: v for k, v in kwargs.items() if v is not None})


def load_config() -> MQConfig:
    if not CONFIG_FILE.exists():
        print(
            "Error: Configuration not found. Run 'mq config' first to set up connection.",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(CONFIG_FILE) as f:
        data = yaml.safe_load(f)

    if not data or not isinstance(data, dict):
        print(
            f"Error: Invalid config file: {CONFIG_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    missing = [k for k in ("host", "port", "username", "password") if k not in data or not data.get(k)]
    if missing:
        print(
            f"Error: Missing required field(s) in config: {', '.join(missing)}",
            file=sys.stderr,
        )
        print("Run 'mq config' to update configuration.", file=sys.stderr)
        sys.exit(1)

    env_vals: dict[str, object] = {}
    for key, env in ENV_MAP.items():
        val = os.environ.get(env)
        if val is not None:
            if key in ("ssl",):
                env_vals[key] = val.lower() in ("1", "true", "yes")
            elif key in ("port", "heartbeat", "connection_timeout", "management_port"):
                try:
                    env_vals[key] = int(val)
                except ValueError:
                    pass
            else:
                env_vals[key] = val

    merged = {**DEFAULT_CONFIG, **data, **env_vals, **_overrides}

    return MQConfig(
        host=str(merged["host"]),
        port=int(merged["port"]),
        username=str(merged["username"]),
        password=str(merged["password"]),
        vhost=str(merged.get("vhost", "/")),
        ssl=bool(merged.get("ssl", False)),
        heartbeat=int(merged.get("heartbeat", 60)),
        connection_timeout=int(merged.get("connection_timeout", 10)),
        management_port=merged.get("management_port"),
    )


def save_config(config: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

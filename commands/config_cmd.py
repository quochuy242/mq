from __future__ import annotations

from mq.config import CONFIG_FILE, DEFAULT_CONFIG, save_config, load_config


def execute(
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    vhost: str | None = None,
    ssl: bool | None = None,
    heartbeat: int | None = None,
    connection_timeout: int | None = None,
    show: bool = False,
) -> None:
    if show:
        _show_config()
        return

    current = {}
    if CONFIG_FILE.exists():
        import yaml
        with open(CONFIG_FILE) as f:
            current = yaml.safe_load(f) or {}

    overrides = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "vhost": vhost,
        "ssl": ssl,
        "heartbeat": heartbeat,
        "connection_timeout": connection_timeout,
    }
    merged = {**DEFAULT_CONFIG, **current}
    for k, v in overrides.items():
        if v is not None:
            merged[k] = v

    missing = [k for k in ("host", "port", "username", "password") if not merged.get(k)]
    if missing:
        print(f"Missing required field(s): {', '.join(missing)}")
        print("Provide values via CLI options or ensure they exist in the config file.")
        return

    save_config(merged)
    print(f"Configuration saved to {CONFIG_FILE}")

    changed = [(k, current.get(k), merged[k]) for k in merged if k not in current or current.get(k) != merged[k]]
    if changed:
        print("  Updated fields:")
        for key, old, new in changed:
            if key == "password":
                print(f"    {key}: ****")
            elif old is None:
                print(f"    {key}: {new} (new)")
            else:
                print(f"    {key}: {new} (was: {old})")


def _show_config() -> None:
    try:
        config = load_config()
    except SystemExit:
        return

    print(f"Host:              {config.host}")
    print(f"Port:              {config.port}")
    print(f"Username:          {config.username}")
    print(f"Password:          {'****' if config.password else '(not set)'}")
    print(f"VHost:             {config.vhost}")
    print(f"TLS:               {'Yes' if config.ssl else 'No'}")
    print(f"Heartbeat:         {config.heartbeat}s")
    print(f"Connection Timeout: {config.connection_timeout}s")
    print(f"Config file:       {CONFIG_FILE}")

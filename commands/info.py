from __future__ import annotations

from mq.config import MQConfig
from mq.services.diagnostic_service import (
    broker_info,
    probe_permissions,
)
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def _permission_display(value: bool | None) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "n/a"


def execute(config: MQConfig, queue: str | None = None, probe: bool = True) -> None:
    if queue:
        from mq.commands.info_queue import execute as queue_info

        queue_info(config, queue)
        return

    local: list[tuple[str, object]] = [
        ("Host", config.host),
        ("Port", config.port),
        ("TLS", "yes" if config.ssl else "no"),
        ("TLS verify", "yes" if config.ssl_verify else "no (insecure)"),
        ("VHost", config.vhost),
        ("Username", config.username),
        ("Auth", config.auth),
        ("Config from", config.describe_source()),
        ("Heartbeat", f"{config.heartbeat}s"),
        ("Connect timeout", f"{config.connection_timeout}s"),
        ("Blocked timeout", f"{config.blocked_connection_timeout}s"),
    ]

    remote: dict[str, object] = {}
    error: str | None = None
    if probe:
        try:
            remote = broker_info(config)
        except Exception as e:
            error = str(e)
    else:
        error = "skipped (--no-probe)"

    permissions = None
    if probe:
        try:
            permissions = probe_permissions(config)
        except Exception:
            permissions = None

    if is_json_mode():
        payload: dict[str, object] = {
            "connection": {
                "host": config.host,
                "port": config.port,
                "tls": config.ssl,
                "vhost": config.vhost,
                "username": config.username,
                "auth": config.auth,
                "source": config.describe_source(),
            },
            "broker": remote,
            "permissions": permissions.to_dict() if permissions else None,
        }
        if error:
            payload["broker_error"] = error
        print_json(payload)
        return

    print_pairs(local, title="Connection")
    if remote:
        print()
        pairs = [
            ("Product", remote.get("product")),
            ("Version", remote.get("version")),
            ("Cluster", remote.get("cluster_name")),
            ("Information", remote.get("information")),
            ("Frame max", remote.get("frame_max")),
            ("Channel max", remote.get("channel_max")),
            ("Heartbeat", remote.get("heartbeat")),
        ]
        capabilities = remote.get("capabilities") or {}
        if isinstance(capabilities, dict):
            enabled = [name for name, on in capabilities.items() if on]
            pairs.append(("Capabilities", ", ".join(enabled) if enabled else "(none)"))
        print_pairs(pairs, title="Broker")

    if permissions:
        print()
        pairs = [
            ("Configure", _permission_display(permissions.configure)),
            ("Write", _permission_display(permissions.write)),
            ("Read", _permission_display(permissions.read)),
            ("Method", permissions.mode),
        ]
        for key, detail in (permissions.details or {}).items():
            pairs.append((f"{key} detail", detail))
        print_pairs(pairs, title="Permissions (AMQP probe)")
    elif error:
        print()
        print(f"  Broker probe skipped: {error}")

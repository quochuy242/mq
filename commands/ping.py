from __future__ import annotations

from mq.config import MQConfig
from mq.connection import close_quietly, create_connection, negotiated, server_properties
from mq.services.diagnostic_service import broker_info
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def execute(config: MQConfig) -> None:
    try:
        conn = create_connection(config)
    except Exception as e:
        handle_error("Cannot connect to RabbitMQ", e)
        return

    try:
        params = conn.params
        host = params.host or config.host
        port = params.port or config.port
        props = server_properties(conn)
        limits = negotiated(conn)
    finally:
        close_quietly(conn)

    if is_json_mode():
        print_json(
            {
                "reachable": True,
                "host": host,
                "port": port,
                "vhost": config.vhost,
                "tls": config.ssl,
                "username": config.username,
                "server": {
                    "product": props.get("product"),
                    "version": props.get("version"),
                    "cluster_name": props.get("cluster_name"),
                },
                "negotiated": {
                    "heartbeat": limits.get("heartbeat"),
                    "frame_max": limits.get("frame_max"),
                    "channel_max": limits.get("channel_max"),
                },
            }
        )
        return

    pairs: list[tuple[str, object]] = [
        ("Endpoint", f"{host}:{port}"),
        ("TLS", "yes" if config.ssl else "no"),
        ("VHost", config.vhost),
        ("User", config.username),
        ("Auth", config.auth),
        ("Server", f"{props.get('product', 'unknown')} {props.get('version', '')}".strip()),
        ("Cluster", props.get("cluster_name")),
        ("Heartbeat", f"{limits.get('heartbeat')}s"),
        ("Frame max", limits.get("frame_max")),
        ("Channel max", limits.get("channel_max")),
        ("Connect timeout", f"{config.connection_timeout}s"),
    ]
    print_pairs(pairs, title="RabbitMQ")


def execute_info(config: MQConfig) -> None:
    """Full broker self-report, read from the AMQP handshake."""
    try:
        info = broker_info(config)
    except Exception as e:
        handle_error("Cannot read broker information", e)
        return
    if is_json_mode():
        print_json(info)
        return
    pairs = [(key.replace("_", " ").capitalize(), value) for key, value in info.items()]
    print_pairs(pairs, title="Broker")

from __future__ import annotations

from uuid import uuid4

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_connection


def _probe_permission(
    config: MQConfig, method: str, queue: str
) -> bool | str | None:
    connection = create_connection(config)
    channel = connection.channel()
    try:
        if method == "configure":
            channel.queue_declare(queue=queue, auto_delete=False)
            return True
        elif method == "write":
            channel.basic_publish(exchange="", routing_key=queue, body=b"")
            return True
        elif method == "read":
            channel.basic_get(queue=queue, auto_ack=True)
            return True
        return None
    except ChannelClosedByBroker:
        return False
    except Exception as e:
        return f"error: {e}"
    finally:
        try:
            channel.close()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass


def _probe_permissions(config: MQConfig) -> dict[str, bool | str | None] | None:
    probe_queue = f"mq-perm-probe-{uuid4().hex[:8]}"
    perms: dict[str, bool | str | None] = {}

    perms["configure"] = _probe_permission(config, "configure", probe_queue)

    if perms.get("configure") is True:
        perms["write"] = _probe_permission(config, "write", probe_queue)
        perms["read"] = _probe_permission(config, "read", probe_queue)

        ch = None
        try:
            conn = create_connection(config)
            ch = conn.channel()
            ch.queue_delete(queue=probe_queue)
        except Exception:
            pass
        finally:
            if ch:
                try:
                    ch.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
    else:
        perms["write"] = None
        perms["read"] = None

    return perms


def execute(config: MQConfig, queue: str | None = None) -> None:
    if queue:
        from mq.commands.info_queue import execute as queue_info

        queue_info(config, queue)
        return
    print(f"Host:       {config.host}")
    print(f"Port:       {config.port}")
    print(f"TLS:        {'Yes' if config.ssl else 'No'}")
    print(f"VHost:      {config.vhost}")
    print(f"Username:   {config.username}")
    print(f"Timeout:    {config.connection_timeout}s")
    print(f"Heartbeat:  {config.heartbeat}s")

    perms = _probe_permissions(config)
    if perms:
        print("Permissions (AMQP probe):")
        for name in ("configure", "write", "read"):
            val = perms.get(name)
            if val is True:
                display = "Yes"
            elif val is False:
                display = "No (access refused)"
            elif val is None:
                display = "N/A"
            else:
                display = str(val)
            print(f"  {name:<12} {display}")

from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.connection import create_connection
from mq.utils import handle_error


def execute(config: MQConfig) -> None:
    try:
        conn = create_connection(config)
        params = conn.params
        conn.close()
        host = params.host or config.host
        port = params.port or config.port
        tls = "yes" if config.ssl else "no"
        print(f"Connected to {host}:{port} (TLS: {tls})")
        print(f"  Virtual host: {config.vhost}")
        print(f"  Heartbeat:    {config.heartbeat}s")
        print(f"  Timeout:      {config.connection_timeout}s")
    except pika.exceptions.ProbableAuthenticationError:
        handle_error("Authentication failed - check username and password")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

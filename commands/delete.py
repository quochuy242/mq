from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error


def execute(config: MQConfig, queue: str) -> None:
    try:
        conn, channel = create_channel(config)
        method = channel.queue_delete(queue=queue)
        conn.close()
        msg_count = method.method.message_count if method and hasattr(method.method, "message_count") else 0
        print(f"Queue deleted: '{queue}'")
        if msg_count:
            print(f"  Messages removed: {msg_count}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error


def execute(config: MQConfig, queue: str) -> None:
    try:
        conn, channel = create_channel(config)
        method = channel.queue_purge(queue=queue)
        conn.close()
        msg_count = method.method.message_count if method else 0
        print(f"Queue '{queue}' purged")
        print(f"  Removed messages: {msg_count}")
    except ChannelClosedByBroker as e:
        handle_error(f"Queue not found or channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

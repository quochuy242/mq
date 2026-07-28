from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error


def execute(config: MQConfig, queue: str) -> None:
    try:
        conn, channel = create_channel(config)
        method = channel.queue_declare(queue=queue, passive=True)
        conn.close()
        total = method.method.message_count
        consumers = method.method.consumer_count
        print(f"Queue:      {queue}")
        print(f"  Ready:          {total}")
        print(f"  Consumers:      {consumers}")
    except ChannelClosedByBroker as e:
        handle_error(f"Queue '{queue}' not found: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

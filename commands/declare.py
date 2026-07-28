from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error


def execute(
    config: MQConfig,
    queue: str,
    durable: bool = False,
    exclusive: bool = False,
    auto_delete: bool = False,
) -> None:
    try:
        conn, channel = create_channel(config)
        method = channel.queue_declare(
            queue=queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
        )
        conn.close()
        print("Queue declared successfully")
        print(f"  Name:           {queue}")
        print(f"  Durable:        {'yes' if durable else 'no'}")
        print(f"  Exclusive:      {'yes' if exclusive else 'no'}")
        print(f"  Auto-delete:    {'yes' if auto_delete else 'no'}")
        print(f"  Messages:       {method.method.message_count}")
        print(f"  Consumers:      {method.method.consumer_count}")
    except ChannelClosedByBroker as e:
        handle_error(f"Channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

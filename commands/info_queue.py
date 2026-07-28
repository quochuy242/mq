from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_connection
from mq.utils import handle_error


def execute(config: MQConfig, queue: str) -> None:
    try:
        connection = create_connection(config)
        channel = connection.channel()
        method = channel.queue_declare(queue=queue, passive=True)
        connection.close()

        ready = method.method.message_count
        consumers = method.method.consumer_count

        print(f"Queue:      {queue}")
        print(f"  Ready:      {ready}")
        if ready == 0:
            print(f"  Status:     empty")
        print(f"  Consumers:  {consumers}")
        print(f"  Active:     {'yes' if consumers > 0 else 'no'}")
    except ChannelClosedByBroker as e:
        handle_error(f"Queue '{queue}' not found: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

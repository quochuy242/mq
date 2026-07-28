from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.connection import create_connection
from mq.utils import handle_error


def execute(
    config: MQConfig,
    queue: str,
    limit: int = -1,
) -> None:
    try:
        connection = create_connection(config)
        channel = connection.channel()
        replayed = 0

        while limit < 0 or replayed < limit:
            mf, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if mf is None:
                break

            channel.basic_ack(delivery_tag=mf.delivery_tag)

            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
                properties=properties,
            )
            replayed += 1

        connection.close()

        if replayed == 0:
            print(f"No messages to replay in '{queue}'.")
            return

        print(f"Replayed {replayed} message(s) to '{queue}'")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

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
        retried = 0

        while limit < 0 or retried < limit:
            mf, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if mf is None:
                break

            routing_key = queue
            exchange = ""

            if properties and properties.headers:
                x_death = properties.headers.get("x-death")
                if x_death and isinstance(x_death, list) and len(x_death) > 0:
                    first = x_death[0]
                    routing_key = first.get("routing-keys", [queue])[0]
                    exchange = first.get("exchange", "")

            channel.basic_ack(delivery_tag=mf.delivery_tag)

            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
            )
            retried += 1

        connection.close()

        if retried == 0:
            print(f"No messages to retry in '{queue}'.")
            return

        print(f"Retried {retried} message(s) from '{queue}'")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

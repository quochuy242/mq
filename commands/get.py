from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_connection
from mq.utils import handle_error, pretty_print_message
from mq.utils.helpers import jq_filter, pretty_print_json


def execute(
    config: MQConfig,
    queue: str,
    index: int = 1,
    ack: bool = False,
    requeue: bool = False,
    pretty: bool = False,
    jq_expr: str | None = None,
) -> None:
    try:
        connection = create_connection(config)
        channel = connection.channel()

        if index < 1:
            channel.close()
            connection.close()
            handle_error("Index must be >= 1")

        for i in range(index - 1):
            method_frame, _, _ = channel.basic_get(queue=queue, auto_ack=False)
            if method_frame is None:
                channel.close()
                connection.close()
                handle_error(
                    f"Queue has only {i} message(s), cannot reach index {index}"
                )
            channel.basic_nack(
                delivery_tag=method_frame.delivery_tag, requeue=True
            )

        method_frame, properties, body = channel.basic_get(
            queue=queue, auto_ack=False
        )

        if method_frame is None:
            channel.close()
            connection.close()
            print("Queue is empty.")
            return

        delivery_tag = method_frame.delivery_tag

        if ack:
            channel.basic_ack(delivery_tag=delivery_tag)
            action = "acknowledged and removed"
        elif requeue:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            action = "requeued"
        else:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
            action = "peeked (left on queue)"

        channel.close()
        connection.close()

        print(f"Message #{index} fetched from '{queue}' ({action}):")
        headers = properties.headers if properties else None

        if jq_expr:
            body_text = body.decode("utf-8", errors="replace")
            result = jq_filter(body_text, jq_expr)
            print(result)
        elif pretty:
            body_text = body.decode("utf-8", errors="replace")
            print(pretty_print_json(body_text))
        else:
            pretty_print_message(body, properties=properties, headers=headers)

    except ChannelClosedByBroker as e:
        handle_error(f"Queue not found or channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

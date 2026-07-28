from __future__ import annotations

from typing import Any

import pika

from mq.config import MQConfig
from mq.connection import create_connection


def peek_message(
    config: MQConfig,
    queue: str,
    requeue: bool = True,
) -> tuple[
    pika.spec.Basic.GetOk | None,
    pika.spec.BasicProperties | None,
    bytes | None,
]:
    connection = create_connection(config)
    channel = connection.channel()
    method_frame, properties, body = channel.basic_get(
        queue=queue, auto_ack=False
    )
    if method_frame is not None:
        channel.basic_nack(
            delivery_tag=method_frame.delivery_tag, requeue=requeue
        )
    channel.close()
    connection.close()
    return method_frame, properties, body


def get_message(
    config: MQConfig,
    queue: str,
    index: int = 1,
    ack: bool = False,
    requeue: bool = False,
) -> tuple[
    pika.spec.Basic.GetOk | None,
    pika.spec.BasicProperties | None,
    bytes | None,
    str,
]:
    connection = create_connection(config)
    channel = connection.channel()

    if index < 1:
        channel.close()
        connection.close()
        raise ValueError("Index must be >= 1")

    for i in range(index - 1):
        mf, _, _ = channel.basic_get(queue=queue, auto_ack=False)
        if mf is None:
            channel.close()
            connection.close()
            raise IndexError(
                f"Queue has only {i} message(s), cannot reach index {index}"
            )
        channel.basic_nack(delivery_tag=mf.delivery_tag, requeue=True)

    method_frame, properties, body = channel.basic_get(
        queue=queue, auto_ack=False
    )

    if method_frame is None:
        channel.close()
        connection.close()
        return None, None, None, ""

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
    return method_frame, properties, body, action


def move_message(
    config: MQConfig,
    source: str,
    destination: str,
    ack_source: bool = True,
) -> bool:
    connection = create_connection(config)
    channel = connection.channel()

    mf, properties, body = channel.basic_get(
        queue=source, auto_ack=False
    )
    if mf is None:
        channel.close()
        connection.close()
        return False

    if ack_source:
        channel.basic_ack(delivery_tag=mf.delivery_tag)

    channel.basic_publish(
        exchange="",
        routing_key=destination,
        body=body,
        properties=properties,
    )

    channel.close()
    connection.close()
    return True


def extract_message_dict(
    method_frame: pika.spec.Basic.GetOk,
    properties: pika.spec.BasicProperties | None,
    body: bytes,
) -> dict[str, Any]:
    msg: dict[str, Any] = {
        "body": _decode_body(body),
        "body_raw": body.hex(),
    }
    if properties:
        props: dict[str, Any] = {}
        for attr in dir(properties):
            if not attr.startswith("_") and not callable(
                getattr(properties, attr)
            ):
                val = getattr(properties, attr)
                if val is not None:
                    props[attr] = val
        msg["properties"] = props
    if method_frame:
        msg["delivery_tag"] = method_frame.delivery_tag
        msg["redelivered"] = method_frame.redelivered
        msg["routing_key"] = method_frame.routing_key
        msg["exchange"] = method_frame.exchange
    return msg


def _decode_body(body: bytes) -> Any:
    import orjson

    try:
        return orjson.loads(body)
    except orjson.JSONDecodeError:
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError:
            return body.hex()

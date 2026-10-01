from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

from mq import inventory
from mq.config import MQConfig
from mq.connection import (
    BrokerBlockedError,
    close_quietly,
    create_channel,
)

Delivered = tuple[pika.spec.Basic.GetOk, pika.spec.BasicProperties, bytes]


class ReturnedMessage(Exception):
    """Raised when ``mandatory=True`` and the broker had nowhere to route."""

    def __init__(self, reply_code: int, reply_text: str, exchange: str, routing_key: str):
        self.reply_code = reply_code
        self.reply_text = reply_text
        self.exchange = exchange
        self.routing_key = routing_key
        super().__init__(
            f"The broker returned the message as unroutable "
            f"({reply_code} {reply_text}) for exchange '{exchange}' "
            f"routing key '{routing_key}'. No queue is bound to it, so the "
            f"message was not delivered."
        )


@dataclass
class ReturnCollector:
    """Collects ``basic.return`` frames so unroutable messages are not silent."""

    returned: list[dict[str, Any]] = field(default_factory=list)

    def callback(self, channel: BlockingChannel, method: Any, properties: Any, body: bytes) -> None:
        self.returned.append(
            {
                "reply_code": getattr(method, "reply_code", 0),
                "reply_text": getattr(method, "reply_text", ""),
                "exchange": getattr(method, "exchange", ""),
                "routing_key": getattr(method, "routing_key", ""),
                "size": len(body) if body else 0,
            }
        )

    def raise_if_returned(self) -> None:
        if not self.returned:
            return
        first = self.returned[0]
        raise ReturnedMessage(
            first["reply_code"], first["reply_text"], first["exchange"], first["routing_key"]
        )


def publish(
    config: MQConfig,
    connection: BlockingConnection,
    channel: BlockingChannel,
    exchange: str,
    routing_key: str,
    body: bytes,
    properties: pika.BasicProperties | None = None,
    mandatory: bool = False,
) -> None:
    """Publish one message and wait for the broker to confirm it.

    Publisher confirms are the only way to know the broker accepted the message.
    Without them a crash between "publish" and "ack" silently loses data, which is
    how move/retry/replay used to drop messages.
    """
    collector = ReturnCollector()
    if mandatory:
        channel.add_on_return_callback(collector.callback)

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=properties,
        mandatory=mandatory,
    )
    # Publisher confirms are asynchronous; the publish call only flushes.
    _pump(connection)
    collector.raise_if_returned()


def _pump(connection: BlockingConnection, rounds: int = 3) -> None:
    """Let the IOLoop deliver confirms and return frames."""
    for _ in range(rounds):
        try:
            connection.process_data_events(time_limit=0.05)
        except Exception:
            return


def publish_standalone(
    config: MQConfig,
    exchange: str,
    routing_key: str,
    body: bytes,
    properties: pika.BasicProperties | None = None,
    mandatory: bool = False,
) -> None:
    """Open a connection just to publish one message, with confirms on."""
    conn, channel, _ = create_channel(config, confirm=True)
    try:
        publish(
            config,
            conn,
            channel,
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
            mandatory=mandatory,
        )
    finally:
        close_quietly(channel, conn)


def peek_messages(config: MQConfig, queue: str, count: int) -> list[Delivered]:
    """Read up to ``count`` messages without removing them.

    All messages are held unacked and only requeued at the very end. Requeueing
    one at a time (the obvious implementation) is broken: RabbitMQ puts a
    requeued message back near its original position, so the next ``basic_get``
    hands back the *same* message -- ``peek --count 5`` used to print message
    #1 five times.
    """
    conn, channel, _ = create_channel(config)
    collected: list[Delivered] = []
    try:
        inventory.touch_queue(queue, config.vhost)
        for _ in range(max(1, count)):
            method_frame, properties, body = channel.basic_get(queue=queue, auto_ack=False)
            if method_frame is None:
                break
            collected.append((method_frame, properties, body))

        if collected:
            for method_frame, _, _ in collected:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
    finally:
        close_quietly(channel, conn)
    return collected


def get_message(
    config: MQConfig,
    queue: str,
    index: int = 1,
    ack: bool = False,
    requeue: bool = True,
) -> tuple[pika.spec.Basic.GetOk | None, pika.spec.BasicProperties | None, bytes | None, str]:
    """Fetch the message at 1-based ``index`` and act on it.

    The messages before the target are held unacked for the duration rather than
    requeued as they are passed, which is what makes ``--index`` return the
    message the user actually asked for.
    """
    if index < 1:
        raise ValueError("Index must be >= 1")

    conn, channel, _ = create_channel(config)
    skipped: list[pika.spec.Basic.GetOk] = []
    try:
        inventory.touch_queue(queue, config.vhost)

        available = 0
        for _ in range(index - 1):
            method_frame, _, _ = channel.basic_get(queue=queue, auto_ack=False)
            if method_frame is None:
                for pending in skipped:
                    channel.basic_nack(delivery_tag=pending.delivery_tag, requeue=True)
                raise IndexError(
                    f"Queue '{queue}' has only {available} message(s); "
                    f"cannot reach index {index}."
                )
            skipped.append(method_frame)
            available += 1

        method_frame, properties, body = channel.basic_get(queue=queue, auto_ack=False)

        if method_frame is None:
            for pending in skipped:
                channel.basic_nack(delivery_tag=pending.delivery_tag, requeue=True)
            return None, None, None, ""

        if ack:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            action = "acknowledged and removed"
        elif requeue:
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            action = "requeued"
        else:
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            action = "discarded"

        for pending in skipped:
            channel.basic_nack(delivery_tag=pending.delivery_tag, requeue=True)
        return method_frame, properties, body, action
    finally:
        close_quietly(channel, conn)


def move_messages(
    config: MQConfig,
    source: str,
    destination: str,
    count: int = -1,
    exchange: str = "",
    routing_key: str | None = None,
    persistent: bool | None = None,
    mandatory: bool = False,
    on_progress: Callable[[int], None] | None = None,
) -> int:
    """Move messages between queues on one connection, safely.

    Order matters: publish with confirms first, ack the source only afterwards.
    A crash between the two loses nothing -- the source message is still
    unacked and will be redelivered.
    """
    conn, channel, _ = create_channel(config, confirm=True)
    moved = 0
    target_routing = routing_key if routing_key is not None else destination
    try:
        inventory.touch_queue(source, config.vhost)
        inventory.touch_queue(destination, config.vhost)

        while count < 0 or moved < count:
            method_frame, properties, body = channel.basic_get(
                queue=source, auto_ack=False
            )
            if method_frame is None:
                break

            outgoing = properties
            if persistent is not None:
                outgoing = _with_delivery_mode(properties, persistent)

            try:
                publish(
                    config,
                    conn,
                    channel,
                    exchange=exchange,
                    routing_key=target_routing,
                    body=body,
                    properties=outgoing,
                    mandatory=mandatory,
                )
            except Exception:
                # Publish failed: put the message back and stop, rather than
                # dropping it on the floor.
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                raise

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            moved += 1
            if on_progress:
                on_progress(moved)
    finally:
        close_quietly(channel, conn)
    return moved


def _with_delivery_mode(
    properties: pika.BasicProperties | None, persistent: bool
) -> pika.BasicProperties:
    if properties is None:
        return pika.BasicProperties(delivery_mode=2 if persistent else 1)
    clone = pika.spec.BasicProperties(
        content_type=properties.content_type,
        content_encoding=properties.content_encoding,
        headers=properties.headers,
        delivery_mode=2 if persistent else 1,
        priority=properties.priority,
        correlation_id=properties.correlation_id,
        reply_to=properties.reply_to,
        message_id=properties.message_id,
        timestamp=properties.timestamp,
        type=properties.type,
        user_id=properties.user_id,
        app_id=properties.app_id,
        cluster_id=properties.cluster_id,
    )
    return clone


def consume(
    config: MQConfig,
    queue: str,
    auto_ack: bool = False,
    limit: int | None = None,
    prefetch: int = 1,
    on_message: Callable[[pika.spec.Basic.Deliver, pika.BasicProperties, bytes], None] | None = None,
    on_stop: Callable[[], bool] | None = None,
) -> int:
    """Consume from a queue, handling stop conditions from inside the callback.

    ``on_stop`` is polled after every delivery; returning True stops consuming.
    That keeps the limit/filter logic out of the callback and avoids the
    stop_consuming-inside-callback races this code used to have.
    """
    conn, channel, blocked = create_channel(config)
    count = 0

    def callback(
        ch: BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        nonlocal count
        if on_message is not None:
            on_message(method, properties, body)
        if not auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        count += 1
        if limit is not None and count >= limit:
            ch.stop_consuming()

    try:
        inventory.touch_queue(queue, config.vhost)
        channel.basic_qos(prefetch_count=max(1, prefetch))
        channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=auto_ack
        )
        channel.start_consuming()
    finally:
        close_quietly(channel, conn)
    return count


def extract_message_dict(
    method_frame: pika.spec.Basic.GetOk,
    properties: pika.spec.BasicProperties | None,
    body: bytes,
) -> dict[str, Any]:
    """A message as plain data: body, hex body, and the AMQP properties.

    ``body_raw`` keeps the exact bytes so an export/import round trip is
    lossless; the decoded ``body`` is what a human or jq filter wants.
    """
    msg: dict[str, Any] = {
        "body": decode_body(body),
        "body_raw": body.hex(),
        "size": len(body),
    }
    if properties:
        props = properties_to_dict(properties)
        if props:
            msg["properties"] = props
    if method_frame:
        msg["delivery_tag"] = method_frame.delivery_tag
        msg["redelivered"] = method_frame.redelivered
        msg["routing_key"] = method_frame.routing_key
        msg["exchange"] = method_frame.exchange
    return msg


# The AMQP basic properties worth exporting, in a stable order.
PROPERTY_FIELDS = (
    "content_type",
    "content_encoding",
    "headers",
    "delivery_mode",
    "priority",
    "correlation_id",
    "reply_to",
    "expiration",
    "message_id",
    "timestamp",
    "type",
    "user_id",
    "app_id",
    "cluster_id",
)


def properties_to_dict(properties: Any) -> dict[str, Any]:
    """The set property fields as a plain dict, ready for JSON."""
    if properties is None:
        return {}
    return {
        name: getattr(properties, name)
        for name in PROPERTY_FIELDS
        if getattr(properties, name, None) is not None
    }


def set_property_fields(properties: Any) -> list[str]:
    """Which property fields are actually set, in a stable order."""
    return [name for name in PROPERTY_FIELDS if getattr(properties, name, None) is not None]


def properties_from_dict(data: dict[str, Any]) -> pika.BasicProperties:
    return pika.BasicProperties(
        content_type=data.get("content_type"),
        content_encoding=data.get("content_encoding"),
        headers=data.get("headers"),
        delivery_mode=data.get("delivery_mode"),
        priority=data.get("priority"),
        correlation_id=data.get("correlation_id"),
        reply_to=data.get("reply_to"),
        expiration=data.get("expiration"),
        message_id=data.get("message_id"),
        timestamp=data.get("timestamp"),
        type=data.get("type"),
        user_id=data.get("user_id"),
        app_id=data.get("app_id"),
        cluster_id=data.get("cluster_id"),
    )


def decode_body(body: bytes) -> Any:
    """JSON if it parses, UTF-8 if it decodes, hex if it is binary."""
    import orjson

    if not body:
        return ""
    try:
        return orjson.loads(body)
    except (orjson.JSONDecodeError, ValueError):
        pass
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return body.hex()


__all__ = [
    "PROPERTY_FIELDS",
    "BrokerBlockedError",
    "Delivered",
    "ReturnCollector",
    "ReturnedMessage",
    "consume",
    "decode_body",
    "extract_message_dict",
    "get_message",
    "move_messages",
    "peek_messages",
    "properties_from_dict",
    "properties_to_dict",
    "publish",
    "publish_standalone",
    "set_property_fields",
]

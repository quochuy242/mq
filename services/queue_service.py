from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel, create_connection
from mq.utils import handle_error


def declare_queue(
    config: MQConfig,
    queue: str,
    durable: bool = False,
    exclusive: bool = False,
    auto_delete: bool = False,
) -> tuple[int, int]:
    conn, channel = create_channel(config)
    method = channel.queue_declare(
        queue=queue,
        durable=durable,
        exclusive=exclusive,
        auto_delete=auto_delete,
    )
    conn.close()
    return method.method.message_count, method.method.consumer_count


def delete_queue(config: MQConfig, queue: str) -> int:
    conn, channel = create_channel(config)
    method = channel.queue_delete(queue=queue)
    conn.close()
    return (
        method.method.message_count
        if method and hasattr(method.method, "message_count")
        else 0
    )


def purge_queue(config: MQConfig, queue: str) -> int:
    conn, channel = create_channel(config)
    method = channel.queue_purge(queue=queue)
    conn.close()
    return method.method.message_count if method else 0


def queue_stats(config: MQConfig, queue: str) -> tuple[int, int]:
    conn, channel = create_channel(config)
    method = channel.queue_declare(queue=queue, passive=True)
    conn.close()
    return method.method.message_count, method.method.consumer_count


def passive_declare(
    config: MQConfig, queue: str
) -> tuple[str, int, int] | None:
    try:
        conn, channel = create_channel(config)
        method = channel.queue_declare(queue=queue, passive=True)
        conn.close()
        return queue, method.method.message_count, method.method.consumer_count
    except ChannelClosedByBroker:
        return None

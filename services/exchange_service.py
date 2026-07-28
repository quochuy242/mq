from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel


def declare_exchange(
    config: MQConfig,
    exchange: str,
    exchange_type: str = "direct",
    durable: bool = False,
    auto_delete: bool = False,
) -> None:
    conn, channel = create_channel(config)
    channel.exchange_declare(
        exchange=exchange,
        exchange_type=exchange_type,
        durable=durable,
        auto_delete=auto_delete,
    )
    conn.close()


def delete_exchange(config: MQConfig, exchange: str) -> None:
    conn, channel = create_channel(config)
    channel.exchange_delete(exchange=exchange)
    conn.close()


def bind_queue(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
) -> None:
    conn, channel = create_channel(config)
    channel.queue_bind(
        queue=queue, exchange=exchange, routing_key=routing_key
    )
    conn.close()


def unbind_queue(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
) -> None:
    conn, channel = create_channel(config)
    channel.queue_unbind(
        queue=queue, exchange=exchange, routing_key=routing_key
    )
    conn.close()

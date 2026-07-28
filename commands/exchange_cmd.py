from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.services.exchange_service import (
    declare_exchange,
    delete_exchange,
)
from mq.utils import handle_error


def execute_list(config: MQConfig, pattern: str | None = None) -> None:
    handle_error(
        "Listing exchanges requires RabbitMQ Management HTTP API. "
        "Use 'mq exchange declare' or 'mq exchange delete' instead. "
        "For listing, install and enable the rabbitmq_management plugin.",
        exit_code=1,
    )


def execute_declare(
    config: MQConfig,
    exchange: str,
    exchange_type: str = "direct",
    durable: bool = False,
    auto_delete: bool = False,
) -> None:
    try:
        declare_exchange(
            config,
            exchange,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
        )
        print(f"Exchange declared: '{exchange}'")
        print(f"  Type:        {exchange_type}")
        print(f"  Durable:     {'yes' if durable else 'no'}")
        print(f"  Auto-delete: {'yes' if auto_delete else 'no'}")
    except ChannelClosedByBroker as e:
        handle_error(f"Channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)


def execute_delete(
    config: MQConfig,
    exchange: str,
    if_unused: bool = False,
) -> None:
    try:
        delete_exchange(config, exchange)
        print(f"Exchange deleted: '{exchange}'")
    except ChannelClosedByBroker as e:
        handle_error(f"Channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

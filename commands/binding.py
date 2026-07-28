from __future__ import annotations

import pika
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.services.exchange_service import bind_queue, unbind_queue
from mq.utils import handle_error


def execute_bind(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
) -> None:
    try:
        bind_queue(config, queue, exchange, routing_key)
        rk = routing_key or "(default)"
        print(f"Bound queue '{queue}' to exchange '{exchange}'")
        print(f"  Routing key: {rk}")
    except ChannelClosedByBroker as e:
        handle_error(f"Bind failed: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)


def execute_unbind(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
) -> None:
    try:
        unbind_queue(config, queue, exchange, routing_key)
        rk = routing_key or "(default)"
        print(f"Unbound queue '{queue}' from exchange '{exchange}'")
        print(f"  Routing key: {rk}")
    except ChannelClosedByBroker as e:
        handle_error(f"Unbind failed: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

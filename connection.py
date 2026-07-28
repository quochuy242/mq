from __future__ import annotations

import ssl
from typing import Tuple

import pika
from pika.adapters.blocking_connection import BlockingChannel

from mq.config import MQConfig


def create_connection(config: MQConfig) -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(config.username, config.password)

    ssl_options: pika.SSLOptions | None = None
    if config.ssl:
        context = ssl.create_default_context()
        ssl_options = pika.SSLOptions(context)

    parameters = pika.ConnectionParameters(
        host=config.host,
        port=config.port,
        virtual_host=config.vhost,
        credentials=credentials,
        ssl_options=ssl_options,
        heartbeat=config.heartbeat,
        socket_timeout=config.connection_timeout,
        connection_attempts=1,
        retry_delay=0,
    )
    return pika.BlockingConnection(parameters)


def create_channel(config: MQConfig) -> Tuple[pika.BlockingConnection, BlockingChannel]:
    connection = create_connection(config)
    channel = connection.channel()
    return connection, channel

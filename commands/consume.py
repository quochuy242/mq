from __future__ import annotations

import signal
import sys
from typing import Any, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error, pretty_print_message
from mq.utils.helpers import jq_filter, pretty_print_json


def _match_headers(
    msg_headers: dict | None, filters: list[str]
) -> bool:
    if not filters:
        return True
    if not msg_headers:
        return False
    for f in filters:
        if "=" not in f:
            continue
        key, _, val = f.partition("=")
        if msg_headers.get(key.strip()) != val.strip():
            return False
    return True


def execute(
    config: MQConfig,
    queue: str,
    auto_ack: bool = False,
    limit: Optional[int] = None,
    as_json: bool = False,
    pretty: bool = False,
    jq_expr: str | None = None,
    header_filter: Optional[list[str]] = None,
) -> None:
    try:
        conn, channel = create_channel(config)
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
        return
    except Exception as e:
        handle_error("Unexpected error", e)
        return

    count = 0
    should_stop = False

    mode = "auto-ack" if auto_ack else "manual ack"
    label = f"Consuming from '{queue}' (mode: {mode})"
    if limit:
        label += f", limit: {limit} messages"
    print(label, file=sys.stderr)

    def callback(
        ch: BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        nonlocal count, should_stop
        count += 1

        headers_dict = properties.headers if properties else {}
        if not _match_headers(headers_dict, header_filter or []):
            if not auto_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            if limit and count >= limit:
                should_stop = True
                ch.stop_consuming()
            return

        if jq_expr:
            body_text = body.decode("utf-8", errors="replace")
            print(jq_filter(body_text, jq_expr))
        elif pretty:
            body_text = body.decode("utf-8", errors="replace")
            print(pretty_print_json(body_text))
        else:
            pretty_print_message(
                body,
                properties=properties,
                headers=headers_dict,
                as_json=as_json,
            )

        if not auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)

        if limit and count >= limit:
            should_stop = True
            ch.stop_consuming()

    def signal_handler(sig: int, frame: Any) -> None:
        nonlocal should_stop
        if not should_stop:
            should_stop = True
            print("\nStopping consumer...", file=sys.stderr)
            try:
                channel.stop_consuming()
            except Exception:
                pass

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=auto_ack,
        )
        print("Press Ctrl+C to stop.", file=sys.stderr)
        channel.start_consuming()
    except KeyboardInterrupt:
        try:
            channel.stop_consuming()
        except Exception:
            pass
    except ChannelClosedByBroker as e:
        handle_error(f"Queue not found or channel error: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection error: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if count:
            print(f"Consumed {count} message(s).", file=sys.stderr)

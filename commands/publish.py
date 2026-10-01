from __future__ import annotations

from typing import Optional

import pika

from mq import inventory
from mq.config import MQConfig
from mq.services.message_service import ReturnedMessage, publish_standalone
from mq.utils import (
    handle_error,
    is_json,
    is_json_mode,
    parse_headers,
    pretty_print_json,
    print_json,
    print_pairs,
)


def build_properties(
    persistent: bool = False,
    content_type: str | None = None,
    headers: list[str] | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
    app_id: str | None = None,
    expiration: str | None = None,
    priority: int | None = None,
    reply_to: str | None = None,
    msg_type: str | None = None,
) -> pika.BasicProperties:
    parsed_headers = parse_headers(headers) if headers else {}
    return pika.BasicProperties(
        delivery_mode=2 if persistent else 1,
        content_type=content_type,
        headers=parsed_headers or None,
        correlation_id=correlation_id,
        message_id=message_id,
        app_id=app_id,
        expiration=str(expiration) if expiration is not None else None,
        priority=priority,
        reply_to=reply_to,
        type=msg_type,
    )


def execute(
    config: MQConfig,
    queue: str,
    file: Optional[str] = None,
    body: Optional[str] = None,
    exchange: str = "",
    routing_key: Optional[str] = None,
    persistent: bool = False,
    content_type: Optional[str] = None,
    headers: list[str] | None = None,
    mandatory: bool = False,
    correlation_id: Optional[str] = None,
    message_id: Optional[str] = None,
    app_id: Optional[str] = None,
    expiration: Optional[str] = None,
    priority: Optional[int] = None,
    reply_to: Optional[str] = None,
    msg_type: Optional[str] = None,
) -> None:
    if not file and body is None:
        handle_error("Either --body or --file must be specified")
        return

    if file:
        try:
            with open(file, "rb") as handle:
                body_bytes = handle.read()
        except FileNotFoundError:
            handle_error(f"File not found: {file}")
            return
        except OSError as e:
            handle_error(f"Cannot read {file}: {e}")
            return
    else:
        body_bytes = (body or "").encode("utf-8")

    auto_detect_json = not content_type and is_json(content_type, body_bytes)
    final_content_type = content_type or ("application/json" if auto_detect_json else None)

    properties = build_properties(
        persistent=persistent,
        content_type=final_content_type,
        headers=headers,
        correlation_id=correlation_id,
        message_id=message_id,
        app_id=app_id,
        expiration=expiration,
        priority=priority,
        reply_to=reply_to,
        msg_type=msg_type,
    )

    target_key = routing_key if routing_key is not None else queue

    try:
        publish_standalone(
            config,
            exchange=exchange,
            routing_key=target_key,
            body=body_bytes,
            properties=properties,
            mandatory=mandatory,
        )
    except ReturnedMessage as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error(
            f"Cannot publish to exchange '{exchange or '(default)'}' "
            f"with routing key '{target_key}'",
            e,
        )
        return

    if exchange:
        inventory.touch_exchange(exchange, config.vhost)
    if not exchange and queue:
        inventory.touch_queue(queue, config.vhost)

    if is_json_mode():
        print_json(
            {
                "exchange": exchange,
                "routing_key": target_key,
                "bytes": len(body_bytes),
                "delivery_mode": 2 if persistent else 1,
                "content_type": final_content_type,
                "confirmed": True,
            }
        )
        return

    pairs: list[tuple[str, object]] = [
        ("Exchange", exchange or "(default)"),
        ("Routing key", target_key),
        ("Size", f"{len(body_bytes)} bytes"),
        ("Delivery", "persistent" if persistent else "non-persistent"),
        ("Confirmed", "yes (publisher confirm)"),
    ]
    if final_content_type:
        pairs.append(("Content type", final_content_type))
    if properties.headers:
        pairs.append(("Headers", ", ".join(f"{k}={v}" for k, v in properties.headers.items())))
    print_pairs(pairs, title="Published")

    print()
    if auto_detect_json or final_content_type == "application/json":
        print(pretty_print_json(body_bytes))
    else:
        try:
            print(body_bytes.decode("utf-8"))
        except UnicodeDecodeError:
            print(body_bytes.hex())

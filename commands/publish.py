from __future__ import annotations

import sys
from typing import Optional

import pika

from mq.config import MQConfig
from mq.connection import create_channel
from mq.utils import handle_error, is_json, parse_headers, pretty_print_json


def execute(
    config: MQConfig,
    queue: str,
    file: Optional[str] = None,
    body: Optional[str] = None,
    persistent: bool = False,
    content_type: Optional[str] = None,
    headers: list[str] | None = None,
) -> None:
    if not file and not body:
        print("Error: Either --file or --body must be specified", file=sys.stderr)
        sys.exit(1)

    if file:
        try:
            with open(file, "rb") as f:
                body_bytes = f.read()
        except FileNotFoundError:
            handle_error(f"File not found: {file}")
            return
        except IOError as e:
            handle_error(f"Error reading file: {e}")
            return
    else:
        body_bytes = body.encode("utf-8")  # type: ignore[union-attr]

    auto_detect_json = not content_type and is_json(content_type, body_bytes)
    final_content_type = content_type or ("application/json" if auto_detect_json else None)

    parsed_headers = parse_headers(headers) if headers else {}

    properties = pika.BasicProperties(
        delivery_mode=2 if persistent else 1,
        content_type=final_content_type,
        headers=parsed_headers or None,
    )

    try:
        conn, channel = create_channel(config)
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=body_bytes,
            properties=properties,
        )
        conn.close()

        size = len(body_bytes)
        delivery = "persistent" if persistent else "non-persistent"
        print(f"Published to queue '{queue}'")
        print(f"  Size:      {size} bytes")
        print(f"  Delivery:  {delivery}")
        if final_content_type:
            print(f"  Type:      {final_content_type}")
        if parsed_headers:
            print(f"  Headers:   {parsed_headers}")
        print()
        if auto_detect_json or final_content_type == "application/json":
            print(pretty_print_json(body_bytes))
        else:
            print(body_bytes.decode("utf-8", errors="replace"))
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

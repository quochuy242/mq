from __future__ import annotations

import json
import sys
from typing import Any

import pika

from mq.config import MQConfig
from mq.connection import create_connection
from mq.services.message_service import extract_message_dict
from mq.utils import handle_error


def execute_export(
    config: MQConfig,
    queue: str,
    output: str | None = None,
    limit: int = -1,
    fmt: str = "json",
) -> None:
    try:
        connection = create_connection(config)
        channel = connection.channel()

        messages: list[dict[str, Any]] = []

        while limit < 0 or len(messages) < limit:
            mf, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if mf is None:
                break

            msg = extract_message_dict(mf, properties, body)
            channel.basic_nack(
                delivery_tag=mf.delivery_tag, requeue=False
            )
            messages.append(msg)

        connection.close()

        if not messages:
            print("Queue is empty, nothing to export.")
            return

        data = {
            "queue": queue,
            "count": len(messages),
            "format": fmt,
            "messages": messages,
        }

        output_json = json.dumps(data, indent=2, default=str)

        if output:
            with open(output, "w") as f:
                f.write(output_json)
            print(f"Exported {len(messages)} message(s) to {output}")
        else:
            print(output_json)

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)


def execute_import(
    config: MQConfig,
    queue: str,
    file: str,
) -> None:
    try:
        with open(file) as f:
            data = json.load(f)

        raw_messages: list[dict[str, Any]] = data.get("messages", [data])

        connection = create_connection(config)
        channel = connection.channel()
        imported = 0

        for msg in raw_messages:
            body_raw = msg.get("body_raw")
            body: bytes | None = None
            if body_raw:
                body = bytes.fromhex(body_raw)
            else:
                body_val = msg.get("body")
                if isinstance(body_val, str):
                    body = body_val.encode("utf-8")
                elif isinstance(body_val, dict | list):
                    body = json.dumps(body_val).encode("utf-8")
                else:
                    body = b""

            props = None
            raw_props = msg.get("properties")
            if raw_props:
                content_type = raw_props.get("content_type")
                delivery_mode = raw_props.get("delivery_mode")
                headers = raw_props.get("headers")
                props = pika.BasicProperties(
                    content_type=content_type,
                    delivery_mode=delivery_mode,
                    headers=headers,
                )

            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
                properties=props,
            )
            imported += 1

        connection.close()
        print(f"Imported {imported} message(s) to '{queue}'")

    except FileNotFoundError:
        handle_error(f"File not found: {file}")
    except json.JSONDecodeError as e:
        handle_error(f"Invalid JSON in {file}: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

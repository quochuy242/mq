from __future__ import annotations

import json
from typing import Any

import pika

from mq.config import MQConfig
from mq.connection import close_quietly, create_channel
from mq.services.message_service import (
    ReturnedMessage,
    extract_message_dict,
    properties_from_dict,
    publish,
)
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim, warn


def execute_export(
    config: MQConfig,
    queue: str,
    output: str | None = None,
    limit: int = -1,
    destructive: bool = False,
    pretty: bool = True,
) -> None:
    """Write a queue's messages to a JSON file.

    **Non-destructive by default.** The old implementation nacked every message
    with ``requeue=False``, which silently emptied the queue -- a read-only
    looking command that destroyed production data. Messages are now held
    unacked and returned to the queue, and only ``--destructive`` removes them.
    """
    conn, channel, _ = create_channel(config)
    messages: list[dict[str, Any]] = []
    held: list[pika.spec.Basic.GetOk] = []
    removed = 0

    try:
        while limit < 0 or len(messages) < limit:
            method_frame, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if method_frame is None:
                break
            messages.append(extract_message_dict(method_frame, properties, body))
            held.append(method_frame)
        if destructive:
            for method_frame in held:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
            removed = len(held)
        else:
            # Requeue the whole batch at the end. Requeueing inside the loop
            # would put each message back where the next basic_get looks, and
            # the same message would be exported repeatedly.
            for method_frame in held:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
    except Exception as e:
        handle_error(f"Cannot read queue '{queue}'", e)
        return
    finally:
        close_quietly(channel, conn)

    if not messages:
        if is_json_mode():
            print_json({"queue": queue, "count": 0, "messages": []})
        else:
            print(f"Queue '{queue}' is empty, nothing to export.")
        return

    payload = {
        "queue": queue,
        "vhost": config.vhost,
        "count": len(messages),
        "destructive": destructive,
        "messages": messages,
    }
    text = json.dumps(payload, indent=2, default=str) if pretty else json.dumps(
        payload, separators=(",", ":"), default=str
    )

    if output:
        try:
            with open(output, "w", encoding="utf-8") as handle:
                handle.write(text)
        except OSError as e:
            handle_error(f"Cannot write {output}: {e}")
            return
        report_export(queue, len(messages), output, removed)
    else:
        # No file requested: stream the JSON to stdout, like every other command.
        print(text)


def report_export(queue: str, count: int, output: str, removed: int) -> None:
    if is_json_mode():
        print_json(
            {"queue": queue, "count": count, "output": output, "messages_removed": removed}
        )
        return
    print_pairs(
        [
            ("Queue", queue),
            ("Exported", f"{count:,}"),
            ("File", output),
            ("Queue after export", "empty" if removed else f"{count:,} message(s) still waiting"),
        ],
        title="Export complete",
    )
    if removed:
        print()
        print(warn(f"  {removed:,} message(s) were consumed from '{queue}' (--destructive)."))
    else:
        print()
        print(dim("  The queue was left untouched. Use --destructive to consume the messages."))


def execute_import(
    config: MQConfig,
    queue: str,
    file: str,
    exchange: str = "",
    mandatory: bool = False,
) -> None:
    """Publish a previously exported JSON file back into a queue."""
    try:
        with open(file, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        handle_error(f"File not found: {file}")
        return
    except json.JSONDecodeError as e:
        handle_error(f"Invalid JSON in {file}: {e}")
        return
    except OSError as e:
        handle_error(f"Cannot read {file}: {e}")
        return

    raw_messages = data.get("messages") if isinstance(data, dict) else None
    if raw_messages is None:
        raw_messages = [data] if isinstance(data, dict) else data
    if not isinstance(raw_messages, list):
        handle_error(f"Unexpected export format in {file}: expected a 'messages' list.")
        return

    conn, channel, _ = create_channel(config, confirm=True)
    imported = 0
    try:
        for entry in raw_messages:
            if not isinstance(entry, dict):
                continue
            body = _body_of(entry)
            props = properties_from_dict(entry.get("properties") or {})
            try:
                publish(
                    config,
                    conn,
                    channel,
                    exchange=exchange,
                    routing_key=queue,
                    body=body,
                    properties=props,
                    mandatory=mandatory,
                )
            except ReturnedMessage as e:
                handle_error(str(e))
                return
            imported += 1
    except Exception as e:
        handle_error(f"Cannot import into '{queue}'", e)
        return
    finally:
        close_quietly(channel, conn)

    if is_json_mode():
        print_json({"queue": queue, "imported": imported, "source": file})
        return
    print_pairs(
        [("Target", queue), ("Source", file), ("Imported", f"{imported:,}")],
        title="Import complete",
    )


def _body_of(entry: dict[str, Any]) -> bytes:
    raw = entry.get("body_raw")
    if isinstance(raw, str) and raw:
        try:
            return bytes.fromhex(raw)
        except ValueError:
            pass
    value = entry.get("body")
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (dict, list)):
        return json.dumps(value).encode("utf-8")
    if value is None:
        return b""
    return str(value).encode("utf-8")

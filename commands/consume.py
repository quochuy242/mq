from __future__ import annotations

import signal
import sys
from typing import Any, Optional

import pika

from mq.config import MQConfig
from mq.connection import close_quietly, create_channel
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.helpers import jq_filter, parse_header_filters, pretty_print_json, pretty_print_message
from mq.utils.output import dim


def _matches(headers: dict | None, wanted: dict[str, str]) -> bool:
    if not wanted:
        return True
    if not headers:
        return False
    return all(str(headers.get(key)) == value for key, value in wanted.items())


def execute(
    config: MQConfig,
    queue: str,
    auto_ack: bool = False,
    limit: Optional[int] = None,
    as_json: bool = False,
    pretty: bool = False,
    jq_expr: Optional[str] = None,
    header_filter: Optional[list[str]] = None,
    prefetch: int = 1,
) -> None:
    json_mode = as_json or is_json_mode()
    wanted = parse_header_filters(header_filter)
    consumed = 0
    matched = 0
    skipped = 0

    conn, channel, blocked = create_channel(config)

    def on_message(
        method: pika.spec.Basic.Deliver, properties: pika.BasicProperties, body: bytes
    ) -> None:
        nonlocal consumed, matched, skipped
        consumed += 1
        headers = (properties.headers if properties else None) or {}

        if not _matches(headers, wanted):
            skipped += 1
            return

        matched += 1
        if jq_expr:
            print(jq_filter(body.decode("utf-8", errors="replace"), jq_expr))
        elif pretty:
            print(pretty_print_json(body.decode("utf-8", errors="replace")))
        else:
            pretty_print_message(
                body, properties=properties, headers=headers or None, as_json=json_mode
            )

    def callback(
        ch: Any, method: pika.spec.Basic.Deliver, properties: Any, body: bytes
    ) -> None:
        blocked.raise_if_blocked()
        on_message(method, properties, body)
        if not auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        if limit is not None and matched >= limit:
            ch.stop_consuming()

    def request_stop(signum: int, frame: Any) -> None:
        print("\nStopping...", file=sys.stderr)
        try:
            channel.stop_consuming()
        except Exception:
            pass

    previous = {
        sig: signal.signal(sig, request_stop)
        for sig in (signal.SIGINT, signal.SIGTERM)
        if hasattr(signal, sig)
    }

    try:
        channel.basic_qos(prefetch_count=max(1, prefetch))
        channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=auto_ack
        )
        if not json_mode:
            mode = "auto-ack" if auto_ack else "manual ack"
            print_pairs(
                [
                    ("Queue", queue),
                    ("Mode", mode),
                    ("Prefetch", max(1, prefetch)),
                    ("Limit", limit if limit is not None else "unlimited"),
                    ("Filter", ", ".join(f"{k}={v}" for k, v in wanted.items()) or "(none)"),
                ],
                title="Consuming",
            )
            print(dim("  Ctrl+C to stop."))
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        close_quietly(channel, conn)
        _restore(previous)
        handle_error(f"Cannot consume from queue '{queue}'", e)
        return
    finally:
        close_quietly(channel, conn)
        _restore(previous)

    if json_mode:
        print_json(
            {
                "queue": queue,
                "delivered": consumed,
                "matched": matched,
                "skipped": skipped,
            }
        )
    else:
        print()
        print_pairs(
            [
                ("Delivered", consumed),
                ("Matched", matched),
                ("Skipped", skipped),
            ],
            title="Done",
        )


def _restore(previous: dict[int, Any]) -> None:
    for sig, handler in previous.items():
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError, TypeError):
            pass

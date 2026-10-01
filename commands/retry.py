from __future__ import annotations

from typing import Any

import pika

from mq.config import MQConfig
from mq.connection import close_quietly, create_channel
from mq.services.message_service import ReturnedMessage, publish
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim

# Headers RabbitMQ owns; copying them onto a republished message makes it
# dead-letter again on the next hop.
DEATH_HEADERS = ("x-death", "x-first-death-queue", "x-first-death-reason",
                 "x-first-death-exchange", "x-first-death-routing-keys")


def _death_entry(headers: dict[str, Any], dlq: str) -> dict[str, Any] | None:
    """Pick the x-death record describing how the message reached this DLQ.

    x-death is a list ordered most-recent-first and holds one record per
    (queue, reason) pair, so the first record is not necessarily the original
    queue. The record whose ``queue`` is this DLQ is the right one to read.
    """
    deaths = headers.get("x-death")
    if not isinstance(deaths, list):
        return None
    for record in deaths:
        if isinstance(record, dict) and record.get("queue") == dlq:
            return record
    for record in deaths:
        if isinstance(record, dict):
            return record
    return None


def _target_from_death(
    record: dict[str, Any], dlq: str
) -> tuple[str, str]:
    keys = record.get("routing-keys")
    if isinstance(keys, (list, tuple)) and keys:
        routing_key = str(keys[0])
    else:
        routing_key = dlq
    exchange = str(record.get("exchange") or "")
    return exchange, routing_key


def execute(
    config: MQConfig,
    queue: str,
    limit: int = -1,
    target_queue: str | None = None,
    target_exchange: str | None = None,
    target_routing_key: str | None = None,
    mandatory: bool = True,
) -> None:
    """Re-publish messages from a dead-letter queue back to where they came from.

    Two things the previous version got wrong, both of them data problems:

    * it acked the message before publishing, so a crash lost it;
    * it forwarded the original ``x-death`` header, so the broker dead-lettered
      the message again immediately and the retry loop never made progress.
    """
    conn, channel, blocked = create_channel(config, confirm=True)
    retried = 0
    failed = 0
    unroutable: list[dict[str, Any]] = []

    try:
        while limit < 0 or retried + failed < limit:
            blocked.raise_if_blocked()
            method_frame, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if method_frame is None:
                break

            headers = dict(properties.headers or {}) if properties else {}
            record = _death_entry(headers, queue)

            if target_queue is not None:
                exchange = target_exchange or ""
                routing_key = target_routing_key or target_queue
            elif record is not None:
                exchange, routing_key = _target_from_death(record, queue)
            else:
                # No x-death at all: the only safe assumption is the DLQ itself.
                exchange, routing_key = "", queue

            for key in DEATH_HEADERS:
                headers.pop(key, None)
            headers["x-retry-count"] = int(headers.get("x-retry-count", 0)) + 1
            headers["x-retried-from"] = queue

            outgoing = _with_headers(properties, headers)

            try:
                publish(
                    config,
                    conn,
                    channel,
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                    properties=outgoing,
                    mandatory=mandatory,
                )
            except ReturnedMessage as e:
                unroutable.append(
                    {
                        "exchange": e.exchange,
                        "routing_key": e.routing_key,
                        "reply_code": e.reply_code,
                    }
                )
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                failed += 1
                if len(unroutable) >= 5:
                    break
                continue
            except Exception:
                # Keep the message on the DLQ rather than losing it.
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                raise

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            retried += 1
    except Exception as e:
        handle_error(f"Cannot retry messages from '{queue}'", e)
        return
    finally:
        close_quietly(channel, conn)

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "retried": retried,
                "unroutable": len(unroutable),
                "details": unroutable,
            }
        )
        raise SystemExit(1 if failed and not retried else 0)

    if retried == 0 and not unroutable:
        print(f"No messages to retry in '{queue}'.")
        return

    pairs: list[tuple[str, object]] = [
        ("DLQ", queue),
        ("Retried", f"{retried:,}"),
    ]
    if unroutable:
        pairs.append(("Unroutable", len(unroutable)))
    print_pairs(pairs, title="Retry")
    for item in unroutable:
        print(dim(
            f"    exchange '{item['exchange']}' routing key "
            f"'{item['routing_key']}' has no queue bound to it "
            f"({item['reply_code']})"
        ))
    if unroutable:
        print()
        print(dim("  Those messages were left on the DLQ. Bind a queue or pass "
                   "--target-queue to send them somewhere that exists."))


def _with_headers(
    properties: pika.BasicProperties | None, headers: dict[str, Any]
) -> pika.BasicProperties:
    from mq.services.message_service import properties_from_dict, properties_to_dict

    data = properties_to_dict(properties) if properties else {}
    data["headers"] = headers or None
    return properties_from_dict(data)

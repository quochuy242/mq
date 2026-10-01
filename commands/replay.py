from __future__ import annotations

from typing import Any

import pika

from mq.config import MQConfig
from mq.connection import close_quietly, create_channel
from mq.services.message_service import ReturnedMessage, publish
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def execute(
    config: MQConfig,
    queue: str,
    limit: int = -1,
    exchange: str = "",
    routing_key: str | None = None,
    mandatory: bool = True,
) -> None:
    """Re-publish every message in a queue back onto itself, in order.

    Useful to unblock a queue whose messages are all being dead-lettered, or to
    re-run a batch with a fixed payload. Messages are published with confirms
    and only acked afterwards, so an interrupted replay never loses any.
    """
    conn, channel, blocked = create_channel(config, confirm=True)
    replayed = 0
    target_key = routing_key if routing_key is not None else queue

    try:
        while limit < 0 or replayed < limit:
            blocked.raise_if_blocked()
            method_frame, properties, body = channel.basic_get(
                queue=queue, auto_ack=False
            )
            if method_frame is None:
                break

            outgoing = _stamp(properties, replayed)

            try:
                publish(
                    config,
                    conn,
                    channel,
                    exchange=exchange,
                    routing_key=target_key,
                    body=body,
                    properties=outgoing,
                    mandatory=mandatory,
                )
            except Exception:
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                raise

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            replayed += 1
    except ReturnedMessage as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error(f"Cannot replay queue '{queue}'", e)
        return
    finally:
        close_quietly(channel, conn)

    if is_json_mode():
        print_json({"queue": queue, "replayed": replayed, "exchange": exchange})
        return

    if replayed == 0:
        print(f"No messages to replay in '{queue}'.")
        return

    print_pairs(
        [
            ("Queue", queue),
            ("Exchange", exchange or "(default)"),
            ("Routing key", target_key),
            ("Replayed", f"{replayed:,}"),
            ("Safety", "published and confirmed, then acked"),
        ],
        title="Replay",
    )


def _stamp(
    properties: pika.BasicProperties | None, position: int
) -> pika.BasicProperties:
    from mq.services.message_service import properties_from_dict, properties_to_dict

    data: dict[str, Any] = properties_to_dict(properties) if properties else {}
    headers = dict(data.get("headers") or {})
    headers["x-replay-position"] = position
    data["headers"] = headers
    return properties_from_dict(data)

from __future__ import annotations

from mq.config import MQConfig
from mq.services.message_service import extract_message_dict, get_message
from mq.utils import handle_error, is_json_mode, jq_filter, print_json, print_pairs
from mq.utils.helpers import pretty_print_message


def execute(
    config: MQConfig,
    queue: str,
    index: int = 1,
    ack: bool = False,
    requeue: bool = True,
    pretty: bool = True,
    jq_expr: str | None = None,
) -> None:
    if ack and not requeue:
        # Both flags together are contradictory and used to silently favour ack.
        handle_error("--ack and --no-requeue cannot be combined: --ack removes the message.")
        return

    try:
        method_frame, properties, body, action = get_message(
            config, queue, index=index, ack=ack, requeue=requeue
        )
    except ValueError as e:
        handle_error(str(e))
        return
    except IndexError as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error(f"Cannot read from queue '{queue}'", e)
        return

    if method_frame is None or body is None:
        if is_json_mode():
            print_json({"queue": queue, "index": index, "message": None})
        else:
            print(f"Queue '{queue}' is empty.")
        return

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "index": index,
                "action": action,
                "message": extract_message_dict(method_frame, properties, body),
            }
        )
        return

    if jq_expr:
        print(jq_filter(body.decode("utf-8", errors="replace"), jq_expr))
        return

    print_pairs(
        [
            ("Queue", queue),
            ("Position", index),
            ("Action", action),
            ("Redelivered", "yes" if method_frame.redelivered else "no"),
        ],
        title=f"Message #{index}",
    )
    print()
    if pretty:
        from mq.utils.helpers import pretty_print_json

        print(pretty_print_json(body))
    else:
        pretty_print_message(body, properties=properties)

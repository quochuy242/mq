from __future__ import annotations

from mq.config import MQConfig
from mq.services.message_service import extract_message_dict, peek_messages
from mq.utils import handle_error, is_json_mode, print_json
from mq.utils.helpers import pretty_print_json, pretty_print_message


def execute(
    config: MQConfig,
    queue: str,
    count: int = 1,
    as_json: bool = False,
    pretty: bool = False,
) -> None:
    """Read messages without consuming them, on a single connection."""
    try:
        messages = peek_messages(config, queue, count)
    except Exception as e:
        handle_error(f"Cannot read from queue '{queue}'", e)
        return

    if not messages:
        if is_json_mode() or as_json:
            print_json([])
        else:
            print(f"Queue '{queue}' is empty.")
        return

    if as_json or is_json_mode():
        print_json([extract_message_dict(mf, props, body) for mf, props, body in messages])
        return

    for position, (method_frame, properties, body) in enumerate(messages, start=1):
        if count > 1:
            print(f"--- message {position} of {len(messages)} ---")
        if pretty:
            print(pretty_print_json(body))
        else:
            pretty_print_message(body, properties=properties)
        if count > 1:
            print()

    print(f"({len(messages)} message(s) read and left on the queue)")

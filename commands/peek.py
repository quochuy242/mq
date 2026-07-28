from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.services.message_service import extract_message_dict, peek_message
from mq.utils import handle_error, is_json_mode, print_json
from mq.utils.helpers import pretty_print_message


def execute(
    config: MQConfig,
    queue: str,
    count: int = 1,
    as_json: bool = False,
    pretty: bool = False,
) -> None:
    try:
        messages = []
        for i in range(count):
            mf, properties, body = peek_message(config, queue)
            if mf is None:
                if i == 0:
                    print("Queue is empty.")
                    return
                break
            messages.append((mf, properties, body))

        if as_json or is_json_mode():
            data = []
            for mf, properties, body in messages:
                msg = extract_message_dict(mf, properties, body)
                data.append(msg)
            print_json(data)
            return

        for i, (mf, properties, body) in enumerate(messages):
            if count > 1:
                print(f"--- Message {i + 1} ---")
            pretty_print_message(body, properties=properties)
            if count > 1 and i < len(messages) - 1:
                print()

        if len(messages) < count:
            print(
                f"\n(only {len(messages)} message(s) available)",
            )

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

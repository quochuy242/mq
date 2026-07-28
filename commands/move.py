from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.services.message_service import move_message
from mq.utils import handle_error


def execute(
    config: MQConfig,
    source: str,
    destination: str,
    count: int = 1,
) -> None:
    try:
        moved = 0
        for i in range(count):
            ok = move_message(config, source, destination)
            if not ok:
                if i == 0:
                    print(f"Source queue '{source}' is empty.")
                    return
                break
            moved += 1

        print(f"Moved {moved} message(s)")
        print(f"  From: {source}")
        print(f"  To:   {destination}")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

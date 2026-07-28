from __future__ import annotations

import time

from mq.config import MQConfig
from mq.services.management_service import (
    ManagementAPIError,
    get_queue_details,
)
from mq.utils import handle_error, is_json_mode, print_json
from mq.utils.output import colorize, dim, header


def execute(
    config: MQConfig,
    queue: str,
    interval: float = 1.0,
) -> None:
    try:
        while True:
            info = get_queue_details(config, queue)
            if info is None:
                handle_error(
                    f"Cannot fetch queue details. "
                    "The Management HTTP API is required for watch."
                )
                return

            if is_json_mode():
                print_json(
                    {
                        "queue": info.name,
                        "ready": info.ready,
                        "unacked": info.unacked,
                        "consumers": info.consumers,
                        "incoming_rate": info.incoming_rate,
                        "outgoing_rate": info.outgoing_rate,
                    }
                )
                return

            now = time.strftime("%H:%M:%S")
            print(f"\033[2J\033[H", end="")  # clear screen
            print(f"{header(f'mq watch {queue}')}  {dim(now)}")
            print(f"  {'Ready:':<16} {info.ready}")
            print(f"  {'Unacked:':<16} {info.unacked}")
            print(f"  {'Consumers:':<16} {info.consumers}")
            print(f"  {'Incoming/s:':<16} {info.incoming_rate:.1f}")
            print(f"  {'Outgoing/s:':<16} {info.outgoing_rate:.1f}")
            print(f"\n  {dim('Ctrl+C to stop')}")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopped.")
    except ManagementAPIError as e:
        handle_error(f"Watch failed: {e}")
    except Exception as e:
        handle_error(f"Unexpected error: {e}")

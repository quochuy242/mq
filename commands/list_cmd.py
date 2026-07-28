from __future__ import annotations

from mq.config import MQConfig
from mq.services.management_service import ManagementAPIError, list_queues
from mq.utils import handle_error, is_json_mode
from mq.utils.output import print_table


def execute(
    config: MQConfig,
    pattern: str | None = None,
) -> None:
    try:
        queues = list_queues(config, pattern=pattern)
    except ManagementAPIError as e:
        handle_error(
            f"Cannot list queues: {e}\n"
            "The Management HTTP API is required for listing. "
            "Enable the rabbitmq_management plugin or configure management_port."
        )
        return
    except Exception as e:
        handle_error(f"Unexpected error: {e}")
        return

    if not queues:
        print("No queues found.")
        return

    rows = [
        [q.name, str(q.ready), str(q.unacked), str(q.consumers)]
        for q in queues
    ]

    print_table(
        headers=["QUEUE", "READY", "UNACK", "CONSUMERS"],
        rows=rows,
        json_output=is_json_mode(),
        title="Queues",
    )

from __future__ import annotations

from mq.config import MQConfig
from mq.services.management_service import ManagementAPIError, list_queues
from mq.utils import handle_error, is_json_mode
from mq.utils.output import print_table


def execute(
    config: MQConfig,
    limit: int = 10,
    sort_by: str = "ready",
) -> None:
    try:
        queues = list_queues(config)
    except ManagementAPIError as e:
        handle_error(
            f"Cannot list queues: {e}\n"
            "The Management HTTP API is required for top. "
            "Enable the rabbitmq_management plugin or configure management_port."
        )
        return
    except Exception as e:
        handle_error(f"Unexpected error: {e}")
        return

    if not queues:
        print("No queues found.")
        return

    if sort_by == "unacked":
        queues.sort(key=lambda q: q.unacked, reverse=True)
    else:
        queues.sort(key=lambda q: q.ready, reverse=True)

    queues = queues[:limit]

    rows = [
        [str(i + 1), q.name, str(q.ready), str(q.unacked), str(q.consumers)]
        for i, q in enumerate(queues)
    ]

    print_table(
        headers=["#", "QUEUE", "READY", "UNACK", "CONSUMERS"],
        rows=rows,
        json_output=is_json_mode(),
        title=f"Top {len(queues)} queues (by {sort_by})",
    )

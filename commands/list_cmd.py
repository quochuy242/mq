from __future__ import annotations

from mq.config import MQConfig
from mq.services import discovery, management_service
from mq.utils import handle_error, is_json_mode, print_json, print_table
from mq.utils.output import dim


def execute(
    config: MQConfig,
    pattern: str | None = None,
    queues: list[str] | None = None,
    use_management: bool = False,
) -> None:
    """List queues using passive declares over AMQP.

    No management port is involved unless ``--use-management`` is passed, and
    even then only as a fallback for when the local inventory knows nothing.
    """
    patterns = [pattern] if pattern else None
    names = discovery.resolve_queue_names(config, patterns, queues)

    if not names and use_management:
        return _via_management(config, pattern)

    if not names:
        if is_json_mode():
            print_json({"queues": [], "source": "amqp", "note": discovery.mgmt_fallback_message(config)})
        else:
            print(dim(discovery.mgmt_fallback_message(config)))
        return

    try:
        rows = discovery.list_queues(config, patterns, queues)
    except Exception as e:
        handle_error("Cannot list queues", e)
        return

    if is_json_mode():
        print_json([row.to_dict() for row in rows])
        return

    if not rows:
        print("No queues found.")
        return

    print_table(
        headers=["QUEUE", "READY", "CONSUMERS", "STATUS"],
        rows=[row.table_row() for row in rows],
        title=f"Queues in '{config.vhost}' (AMQP passive declare)",
    )
    missing = [row for row in rows if row.status != "ok"]
    if missing:
        print()
        print(dim(f"  {len(missing)} name(s) are not reachable: "
                   f"{', '.join(r.name for r in missing)}"))


def _via_management(config: MQConfig, pattern: str | None) -> None:
    """Opt-in HTTP fallback, for when the inventory is empty."""
    try:
        queues = management_service.list_queues(config, pattern=pattern)
    except Exception as e:
        handle_error(
            "Cannot list queues over the management API",
            e,
        )
        return

    rows = [
        [q.name, f"{q.ready:,}", f"{q.unacked:,}", str(q.consumers)]
        for q in queues
    ]
    if is_json_mode():
        print_json([q.to_dict() for q in queues])
        return
    if not rows:
        print("No queues found.")
        return
    print_table(
        headers=["QUEUE", "READY", "UNACK", "CONSUMERS"],
        rows=rows,
        title=f"Queues in '{config.vhost}' (management API)",
    )

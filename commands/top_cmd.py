from __future__ import annotations

from mq.config import MQConfig
from mq.services import discovery
from mq.utils import handle_error, is_json_mode, print_json, print_table
from mq.utils.output import dim

SORT_KEYS = ("ready", "consumers", "name")


def execute(
    config: MQConfig,
    limit: int = 10,
    sort_by: str = "ready",
    pattern: str | None = None,
    queues: list[str] | None = None,
) -> None:
    if sort_by not in SORT_KEYS:
        handle_error(
            f"Unknown --sort-by '{sort_by}'. Choose one of: {', '.join(SORT_KEYS)}."
        )
        return

    patterns = [pattern] if pattern else None
    names = discovery.resolve_queue_names(config, patterns, queues)
    if not names:
        message = discovery.mgmt_fallback_message(config)
        if is_json_mode():
            print_json({"queues": [], "note": message})
        else:
            print(dim(message))
        return

    try:
        rows = [row for row in discovery.list_queues(config, patterns, queues) if row.status == "ok"]
    except Exception as e:
        handle_error("Cannot collect queue depths", e)
        return

    if sort_by == "ready":
        rows.sort(key=lambda r: r.ready or 0, reverse=True)
    elif sort_by == "consumers":
        rows.sort(key=lambda r: r.consumers or 0, reverse=True)
    else:
        rows.sort(key=lambda r: r.name)

    total = len(rows)
    rows = rows[: max(1, limit)]

    if is_json_mode():
        print_json([row.to_dict() for row in rows])
        return

    if not rows:
        print("No reachable queues found.")
        return

    print_table(
        headers=["#", "QUEUE", "READY", "CONSUMERS"],
        rows=[
            [str(i + 1), row.name, f"{row.ready:,}", str(row.consumers)]
            for i, row in enumerate(rows)
        ],
        title=f"Top {len(rows)} of {total} queue(s) by {sort_by}",
    )

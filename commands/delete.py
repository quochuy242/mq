from __future__ import annotations

import sys

from mq.config import MQConfig
from mq.services.queue_service import delete_queue
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import warn


def execute(
    config: MQConfig,
    queue: str,
    if_unused: bool = False,
    if_empty: bool = False,
    assume_yes: bool = False,
) -> None:
    if not assume_yes and sys.stdin.isatty() and not is_json_mode():
        if if_unused or if_empty:
            condition = " and ".join(
                part for part, on in (("unused", if_unused), ("empty", if_empty)) if on
            )
            prompt = f"Delete queue '{queue}' if it is {condition}? [y/N] "
        else:
            prompt = f"Delete queue '{queue}' and all of its messages? [y/N] "
        answer = input(prompt).strip().lower()
        if answer not in ("y", "yes"):
            print("Aborted.")
            return

    try:
        removed = delete_queue(config, queue, if_unused=if_unused, if_empty=if_empty)
    except Exception as e:
        handle_error(f"Cannot delete queue '{queue}'", e)
        return

    if is_json_mode():
        print_json({"queue": queue, "deleted": True, "messages_removed": removed})
        return

    pairs: list[tuple[str, object]] = [("Queue", queue), ("Deleted", "yes")]
    if removed:
        pairs.append(("Messages removed", f"{removed:,}"))
    if if_unused or if_empty:
        conditions = ", ".join(
            part for part, on in (("if-unused", if_unused), ("if-empty", if_empty)) if on
        )
        pairs.append(("Conditions", conditions))
    print_pairs(pairs, title="Queue deleted")

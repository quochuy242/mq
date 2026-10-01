from __future__ import annotations

import sys

from mq.config import MQConfig
from mq.services.queue_service import QueueProbe, passive_declare, purge_queue
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def execute(config: MQConfig, queue: str, assume_yes: bool = False, dry_run: bool = False) -> None:
    probe = passive_declare(config, queue)
    if not probe.exists:
        reason = probe.error.detail if probe.error else "it was not found"
        handle_error(f"Refusing to purge '{queue}': {reason}")
        return

    doomed = probe.ready or 0
    if not assume_yes and sys.stdin.isatty() and not is_json_mode():
        print(f"Queue '{queue}' holds {doomed:,} message(s).")
        answer = input(f"Purge them? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            print("Aborted.")
            return

    if dry_run:
        if is_json_mode():
            print_json({"queue": queue, "purged": False, "would_remove": doomed})
        else:
            print_pairs(
                [("Queue", queue), ("Would remove", f"{doomed:,}"), ("Purged", "no (--dry-run)")],
                title="Dry run",
            )
        return

    try:
        removed = purge_queue(config, queue)
    except Exception as e:
        handle_error(f"Cannot purge queue '{queue}'", e)
        return

    if is_json_mode():
        print_json({"queue": queue, "purged": True, "messages_removed": removed})
        return
    print_pairs(
        [("Queue", queue), ("Messages removed", f"{removed:,}")],
        title="Queue purged",
    )

from __future__ import annotations

from mq.config import MQConfig
from mq.services.queue_service import passive_declare
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim, warn


def execute(config: MQConfig, queue: str) -> None:
    """Depth and consumer count for one queue, from a passive declare."""
    probe = passive_declare(config, queue)

    if not probe.exists:
        if is_json_mode():
            print_json(
                {
                    "queue": queue,
                    "status": probe.status,
                    "error": probe.error.kind if probe.error else None,
                    "detail": probe.error.detail if probe.error else None,
                }
            )
        else:
            reason = probe.error.detail if probe.error else "queue not found"
            handle_error(f"Cannot read queue '{queue}': {reason}")
        raise SystemExit(1 if not is_json_mode() else 0)

    ready = probe.ready or 0
    consumers = probe.consumers or 0

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "status": probe.status,
                "ready": ready,
                "consumers": consumers,
            }
        )
        return

    pairs: list[tuple[str, object]] = [
        ("Queue", queue),
        ("Ready", f"{ready:,}"),
        ("Consumers", consumers),
    ]
    print_pairs(pairs, title="Queue stats")

    # A queue that keeps filling with nobody reading it is the failure mode
    # operators actually look for, and it is visible from these two numbers.
    if ready > 0 and consumers == 0:
        print()
        print(warn("  No consumer is attached and messages are waiting."))
        print(dim("  The backlog will grow until it hits a quota or runs the node out of disk."))
    if consumers > 1:
        print()
        print(dim(f"  {consumers} consumers are competing for this queue."))

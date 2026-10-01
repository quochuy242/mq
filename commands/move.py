from __future__ import annotations

from mq.config import MQConfig
from mq.services.message_service import ReturnedMessage, move_messages
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim


def execute(
    config: MQConfig,
    source: str,
    destination: str,
    count: int = 1,
    exchange: str = "",
    routing_key: str | None = None,
    persistent: bool | None = None,
    mandatory: bool = False,
) -> None:
    """Move messages between queues on one connection.

    Each message is published and confirmed *before* the source is acked, so an
    interrupted run redelivers rather than loses.
    """
    if source == destination and not exchange:
        handle_error("Source and destination are the same queue.")
        return

    try:
        moved = move_messages(
            config,
            source,
            destination,
            count=count,
            exchange=exchange,
            routing_key=routing_key,
            persistent=persistent,
            mandatory=mandatory,
        )
    except ReturnedMessage as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error(f"Cannot move messages from '{source}' to '{destination}'", e)
        return

    if is_json_mode():
        print_json(
            {
                "source": source,
                "destination": destination,
                "exchange": exchange,
                "moved": moved,
            }
        )
        return

    if moved == 0:
        print_pairs(
            [("Source", source), ("Moved", 0), ("Reason", "the source queue is empty")],
            title="Move",
        )
        return

    pairs: list[tuple[str, object]] = [
        ("Source", source),
        ("Destination", destination),
        ("Exchange", exchange or "(default)"),
        ("Moved", f"{moved:,}"),
        ("Safety", "published and confirmed, then acked"),
    ]
    if persistent is not None:
        pairs.append(("Delivery", "persistent" if persistent else "non-persistent"))
    print_pairs(pairs, title="Move")
    if count < 0:
        print()
        print(dim("  the source queue was drained"))

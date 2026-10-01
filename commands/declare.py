from __future__ import annotations

from mq.config import MQConfig
from mq.services.queue_service import declare_queue, parse_arguments
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim


def build_arguments(
    args: list[str] | None,
    queue_type: str | None = None,
    ttl: int | None = None,
    expires: int | None = None,
    max_length: int | None = None,
    max_length_bytes: int | None = None,
    dlx: str | None = None,
    dl_routing_key: str | None = None,
    overflow: str | None = None,
    max_priority: int | None = None,
    single_active_consumer: bool = False,
    lazy: bool = False,
) -> dict:
    """Merge the convenience flags into the raw ``--arg`` table.

    Explicit ``--arg`` entries win, so a hand-written x-argument is never
    silently overridden by a shorthand flag.
    """
    arguments = parse_arguments(args)

    def put(key: str, value: object) -> None:
        arguments.setdefault(key, value)

    if queue_type:
        put("x-queue-type", queue_type)
    if ttl is not None:
        put("x-message-ttl", ttl)
    if expires is not None:
        put("x-expires", expires)
    if max_length is not None:
        put("x-max-length", max_length)
    if max_length_bytes is not None:
        put("x-max-length-bytes", max_length_bytes)
    if max_priority is not None:
        put("x-max-priority", max_priority)
    if dlx:
        put("x-dead-letter-exchange", dlx)
    if dl_routing_key:
        put("x-dead-letter-routing-key", dl_routing_key)
    if overflow:
        put("x-overflow", overflow)
    if single_active_consumer:
        put("x-single-active-consumer", True)
    if lazy:
        put("x-queue-mode", "lazy")
    return arguments


def execute(
    config: MQConfig,
    queue: str,
    durable: bool = False,
    exclusive: bool = False,
    auto_delete: bool = False,
    arguments: dict | None = None,
) -> None:
    try:
        ready, consumers, name = declare_queue(
            config,
            queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )
    except ValueError as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error(f"Cannot declare queue '{queue}'", e)
        return

    if is_json_mode():
        print_json(
            {
                "queue": name,
                "durable": durable,
                "exclusive": exclusive,
                "auto_delete": auto_delete,
                "arguments": arguments or {},
                "ready": ready,
                "consumers": consumers,
            }
        )
        return

    pairs: list[tuple[str, object]] = [
        ("Name", name),
        ("Durable", "yes" if durable else "no"),
        ("Exclusive", "yes" if exclusive else "no"),
        ("Auto-delete", "yes" if auto_delete else "no"),
        ("Ready", f"{ready:,}"),
        ("Consumers", consumers),
    ]
    if arguments:
        pairs.append(("Arguments", ", ".join(f"{k}={v}" for k, v in sorted(arguments.items()))))
    print_pairs(pairs, title="Queue declared")
    if not name:
        print()
        print(dim("  (the broker generated the name)"))

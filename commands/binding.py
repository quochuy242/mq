from __future__ import annotations

from mq.config import MQConfig
from mq.services.exchange_service import bind_queue, unbind_queue
from mq.services.queue_service import parse_arguments
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def execute_bind(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
    args: list[str] | None = None,
) -> None:
    try:
        arguments = parse_arguments(args)
    except ValueError as e:
        handle_error(str(e))
        return
    try:
        bind_queue(config, queue, exchange, routing_key, arguments=arguments or None)
    except Exception as e:
        handle_error(f"Cannot bind '{queue}' to '{exchange}'", e)
        return

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "exchange": exchange,
                "routing_key": routing_key,
                "arguments": arguments,
                "bound": True,
            }
        )
        return
    pairs: list[tuple[str, object]] = [
        ("Queue", queue),
        ("Exchange", exchange),
        ("Routing key", routing_key or "(empty)"),
    ]
    if arguments:
        pairs.append(("Arguments", ", ".join(f"{k}={v}" for k, v in sorted(arguments.items()))))
    print_pairs(pairs, title="Bound")


def execute_unbind(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
    args: list[str] | None = None,
) -> None:
    try:
        arguments = parse_arguments(args)
    except ValueError as e:
        handle_error(str(e))
        return
    try:
        unbind_queue(config, queue, exchange, routing_key, arguments=arguments or None)
    except Exception as e:
        handle_error(f"Cannot unbind '{queue}' from '{exchange}'", e)
        return

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "exchange": exchange,
                "routing_key": routing_key,
                "arguments": arguments,
                "bound": False,
            }
        )
        return
    print_pairs(
        [
            ("Queue", queue),
            ("Exchange", exchange),
            ("Routing key", routing_key or "(empty)"),
        ],
        title="Unbound",
    )

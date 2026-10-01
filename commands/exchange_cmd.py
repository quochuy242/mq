from __future__ import annotations

from mq.config import MQConfig
from mq.services import discovery
from mq.services.exchange_service import (
    delete_exchange,
    declare_exchange,
    verify_exchange,
)
from mq.utils import handle_error, is_json_mode, print_json, print_table
from mq.utils.output import dim, print_pairs


def execute_list(
    config: MQConfig, pattern: str | None = None, exchanges: list[str] | None = None
) -> None:
    """List exchanges via passive declare -- no management port required."""
    patterns = [pattern] if pattern else None
    rows = discovery.list_exchanges(config, patterns, exchanges)

    if not rows:
        message = (
            f"No exchanges are tracked for vhost {config.vhost!r}.\n"
            "  AMQP cannot enumerate exchanges, so only known names are checked.\n"
            "  Track them with: mq inventory add-exchange amq.topic\n"
            "  or pass them per run: mq exchange list --exchanges amq.topic,amq.fanout"
        )
        if is_json_mode():
            print_json({"exchanges": [], "note": message})
        else:
            print(dim(message))
        return

    if is_json_mode():
        print_json([row.to_dict() for row in rows])
        return

    print_table(
        headers=["EXCHANGE", "STATUS"],
        rows=[row.table_row() for row in rows],
        title=f"Exchanges in '{config.vhost}' (AMQP passive declare)",
    )


def execute_declare(
    config: MQConfig,
    exchange: str,
    exchange_type: str = "direct",
    durable: bool = False,
    auto_delete: bool = False,
    internal: bool = False,
    arguments: dict | None = None,
) -> None:
    try:
        declare_exchange(
            config,
            exchange,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments,
        )
    except Exception as e:
        handle_error(f"Cannot declare exchange '{exchange}'", e)
        return

    if is_json_mode():
        print_json({"exchange": exchange, "type": exchange_type, "durable": durable})
        return
    pairs = [
        ("Exchange", exchange),
        ("Type", exchange_type),
        ("Durable", "yes" if durable else "no"),
        ("Auto-delete", "yes" if auto_delete else "no"),
        ("Internal", "yes" if internal else "no"),
    ]
    print_pairs(pairs, title="Exchange declared")


def execute_delete(
    config: MQConfig,
    exchange: str,
    if_unused: bool = False,
) -> None:
    try:
        delete_exchange(config, exchange, if_unused=if_unused)
    except Exception as e:
        handle_error(f"Cannot delete exchange '{exchange}'", e)
        return

    if is_json_mode():
        print_json({"exchange": exchange, "deleted": True})
        return
    print_pairs([("Exchange", exchange), ("Deleted", "yes")], title="Exchange deleted")


def execute_info(
    config: MQConfig,
    exchange: str,
    exchange_type: str | None = None,
    durable: bool | None = None,
    auto_delete: bool | None = None,
) -> None:
    """Assert what an exchange actually is, using 406 PRECONDITION_FAILED."""
    try:
        checks = verify_exchange(
            config, exchange,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
        )
    except Exception as e:
        handle_error(f"Cannot inspect exchange '{exchange}'", e)
        return

    if not checks:
        try:
            from mq.services.exchange_service import passive_exchange

            probe = passive_exchange(config, exchange)
        except Exception as e:
            handle_error(f"Cannot inspect exchange '{exchange}'", e)
            return
        checks = [("exists", True, probe.exists, None if probe.exists else "not found")]

    if is_json_mode():
        print_json(
            {
                "exchange": exchange,
                "checks": [
                    {"field": name, "expected": expected, "matches": matches, "detail": detail}
                    for name, expected, matches, detail in checks
                ],
            }
        )
        return

    print_pairs(
        [
            (name, _matches_label(matches) + (f"  (expected {expected})" if detail is None else detail))
            for name, expected, matches, detail in checks
        ],
        title=f"Exchange '{exchange}'",
    )


def _matches_label(matches: bool | None) -> str:
    return "match" if matches else ("mismatch" if matches is False else "unknown")

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import pika
import pika.exceptions
from pika.exceptions import ChannelClosedByBroker

from mq import inventory
from mq.config import MQConfig
from mq.connection import create_channel, close_quietly
from mq.errors import AmqpErrorInfo, classify

DEFAULT_EXCHANGE = ""


@dataclass
class ExchangeProbe:
    """Existence of an exchange, via a passive declare."""

    name: str
    exists: bool
    error: AmqpErrorInfo | None = None
    observed: bool = True

    @property
    def status(self) -> str:
        if not self.observed:
            return "unknown"
        if self.exists:
            return "ok"
        if self.error and self.error.kind in ("access-refused", "not-allowed"):
            return "forbidden"
        return "missing"

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"exchange": self.name, "status": self.status}
        if self.error:
            data["error"] = self.error.kind
            data["detail"] = self.error.detail
        return data


def declare_exchange(
    config: MQConfig,
    exchange: str,
    exchange_type: str = "direct",
    durable: bool = False,
    auto_delete: bool = False,
    internal: bool = False,
    arguments: dict[str, Any] | None = None,
) -> None:
    conn, channel, _ = create_channel(config)
    try:
        channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments or None,
        )
        if exchange:
            inventory.touch_exchange(exchange, config.vhost)
    finally:
        close_quietly(channel, conn)


def delete_exchange(config: MQConfig, exchange: str, if_unused: bool = False) -> None:
    conn, channel, _ = create_channel(config)
    try:
        channel.exchange_delete(exchange=exchange, if_unused=if_unused)
        if exchange:
            inventory.remove(exchange, config.vhost, kind="exchange")
    finally:
        close_quietly(channel, conn)


def bind_queue(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
    arguments: dict[str, Any] | None = None,
) -> None:
    conn, channel, _ = create_channel(config)
    try:
        channel.queue_bind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments or None,
        )
        # A binding implies both objects exist, so record both.
        inventory.touch_queue(queue, config.vhost)
        if exchange:
            inventory.touch_exchange(exchange, config.vhost)
    finally:
        close_quietly(channel, conn)


def unbind_queue(
    config: MQConfig,
    queue: str,
    exchange: str,
    routing_key: str = "",
    arguments: dict[str, Any] | None = None,
) -> None:
    conn, channel, _ = create_channel(config)
    try:
        channel.queue_unbind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments or None,
        )
    finally:
        close_quietly(channel, conn)


def passive_exchange(config: MQConfig, exchange: str) -> ExchangeProbe:
    """Check one exchange. A passive declare is 404 when it is not there."""
    conn, channel, _ = create_channel(config)
    try:
        channel.exchange_declare(
            exchange=exchange,
            passive=True,
        )
        return ExchangeProbe(name=exchange, exists=True)
    except ChannelClosedByBroker as e:
        return ExchangeProbe(name=exchange, exists=False, error=classify(e))
    finally:
        close_quietly(channel, conn)


def probe_exchanges(config: MQConfig, names: Sequence[str]) -> list[ExchangeProbe]:
    """Passive-declare many exchanges on one connection.

    A channel error closes the channel, so it is recreated per failure; see the
    identical handling in ``queue_service.probe_queues``.
    """
    results: list[ExchangeProbe] = []
    if not names:
        return results

    conn, channel, _ = create_channel(config)
    try:
        for name in names:
            try:
                channel.exchange_declare(exchange=name, passive=True)
            except ChannelClosedByBroker as e:
                results.append(ExchangeProbe(name=name, exists=False, error=classify(e)))
                close_quietly(channel)
                try:
                    channel = conn.channel()
                except pika.exceptions.AMQPError as reconnect_error:
                    for remaining in names[len(results):]:
                        results.append(
                            ExchangeProbe(
                                name=remaining,
                                exists=False,
                                observed=False,
                                error=classify(reconnect_error),
                            )
                        )
                    return results
            except pika.exceptions.AMQPError as e:
                for remaining in names[len(results):]:
                    results.append(
                        ExchangeProbe(
                            name=remaining, exists=False, observed=False, error=classify(e)
                        )
                    )
                return results
            else:
                results.append(ExchangeProbe(name=name, exists=True))
    finally:
        close_quietly(channel, conn)

    # No inventory write: listing is a read, not a declaration.
    return results


def verify_exchange(
    config: MQConfig,
    exchange: str,
    exchange_type: str | None = None,
    durable: bool | None = None,
    auto_delete: bool | None = None,
) -> list[tuple[str, Any, bool | None, str | None]]:
    """Assert an exchange's shape, the same 406-probe trick used for queues."""
    checks: list[tuple[str, Any, bool | None, str | None]] = []

    def probe(field_name: str, expect: Any, **kwargs: Any) -> None:
        conn, channel, _ = create_channel(config)
        try:
            channel.exchange_declare(exchange=exchange, **kwargs)
        except ChannelClosedByBroker as e:
            info = classify(e)
            if info.kind == "precondition-failed":
                checks.append((field_name, expect, False, info.detail))
            elif info.code == 404:
                checks.append((field_name, expect, None, "exchange does not exist"))
            else:
                checks.append((field_name, expect, None, info.detail))
        except pika.exceptions.AMQPError as e:
            checks.append((field_name, expect, None, classify(e).detail))
        else:
            checks.append((field_name, expect, True, None))
        finally:
            close_quietly(channel, conn)

    if exchange_type is not None:
        probe("type", exchange_type, exchange_type=exchange_type)
    if durable is not None:
        probe("durable", durable, durable=durable)
    if auto_delete is not None:
        probe("auto_delete", auto_delete, auto_delete=auto_delete)
    return checks

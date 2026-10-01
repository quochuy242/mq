from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import ChannelClosedByBroker

from mq import inventory
from mq.config import MQConfig
from mq.connection import create_channel, close_quietly
from mq.errors import AmqpErrorInfo, classify

# Argument names RabbitMQ understands natively, kept for validation and help text.
KNOWN_ARGUMENTS = (
    "x-queue-type",
    "x-message-ttl",
    "x-expires",
    "x-max-length",
    "x-max-length-bytes",
    "x-max-priority",
    "x-overflow",
    "x-dead-letter-exchange",
    "x-dead-letter-routing-key",
    "x-single-active-consumer",
    "x-queue-mode",
    "x-consumer-timeout",
    "x-delivery-limit",
    "x-queue-version",
    "x-consumer-timeout",
    "x-max-in-memory-length",
    "x-max-in-memory-bytes",
)


@dataclass
class QueueProbe:
    """Result of a passive declare: the cheapest way to ask "does this exist"."""

    name: str
    exists: bool
    ready: int | None = None
    consumers: int | None = None
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
        data: dict[str, Any] = {"queue": self.name, "status": self.status}
        if self.exists:
            data["ready"] = self.ready
            data["consumers"] = self.consumers
        if self.error:
            data["error"] = self.error.kind
            data["detail"] = self.error.detail
        return data


def declare_queue(
    config: MQConfig,
    queue: str,
    durable: bool = False,
    exclusive: bool = False,
    auto_delete: bool = False,
    arguments: dict[str, Any] | None = None,
) -> tuple[int, int, str]:
    """Declare a queue. Returns (ready, consumers, broker-assigned name).

    The returned name matters: passing ``queue=""`` makes the broker generate
    one, and callers need it for later operations.
    """
    conn, channel, _ = create_channel(config)
    try:
        method = channel.queue_declare(
            queue=queue,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments or None,
        )
        name = method.method.queue or queue
        if name:
            inventory.touch_queue(name, config.vhost)
        return method.method.message_count, method.method.consumer_count, name
    finally:
        close_quietly(channel, conn)


def delete_queue(
    config: MQConfig, queue: str, if_unused: bool = False, if_empty: bool = False
) -> int:
    conn, channel, _ = create_channel(config)
    try:
        method = channel.queue_delete(
            queue=queue, if_unused=if_unused, if_empty=if_empty
        )
        inventory.remove(queue, config.vhost, kind="queue")
        return (
            method.method.message_count
            if method and hasattr(method.method, "message_count")
            else 0
        )
    finally:
        close_quietly(channel, conn)


def purge_queue(config: MQConfig, queue: str) -> int:
    conn, channel, _ = create_channel(config)
    try:
        method = channel.queue_purge(queue=queue)
        return method.method.message_count if method else 0
    finally:
        close_quietly(channel, conn)


def queue_stats(config: MQConfig, queue: str) -> tuple[int, int]:
    conn, channel, _ = create_channel(config)
    try:
        method = channel.queue_declare(queue=queue, passive=True)
        inventory.touch_queue(queue, config.vhost)
        return method.method.message_count, method.method.consumer_count
    finally:
        close_quietly(channel, conn)


def passive_declare(config: MQConfig, queue: str) -> QueueProbe:
    """Check one queue. A 404/403 is an answer, not an error to propagate."""
    conn, channel, _ = create_channel(config)
    try:
        method = channel.queue_declare(queue=queue, passive=True)
        return QueueProbe(
            name=queue,
            exists=True,
            ready=method.method.message_count,
            consumers=method.method.consumer_count,
        )
    except ChannelClosedByBroker as e:
        return QueueProbe(name=queue, exists=False, error=classify(e))
    finally:
        close_quietly(channel, conn)


class PassiveChannel:
    """A long-lived connection for repeated passive declares.

    ``mq watch`` polls the same queue for minutes; opening a TCP + TLS + AMQP
    handshake per tick would make the tool the load. A broker error closes the
    channel (not the connection), so it is transparently recreated.
    """

    def __init__(self, config: MQConfig) -> None:
        self.config = config
        self.conn, self.channel, self.blocked = create_channel(config)

    def probe(self, queue: str) -> QueueProbe:
        try:
            method = self.channel.queue_declare(queue=queue, passive=True)
        except ChannelClosedByBroker as e:
            self._renew_channel()
            return QueueProbe(name=queue, exists=False, error=classify(e))
        except pika.exceptions.AMQPError as e:
            return QueueProbe(name=queue, exists=False, observed=False, error=classify(e))
        # Deliberately no inventory write: this is a read, and 'mq list' on a
        # throwaway name must not silently make that name permanent.
        return QueueProbe(
            name=queue,
            exists=True,
            ready=method.method.message_count,
            consumers=method.method.consumer_count,
        )

    def _renew_channel(self) -> None:
        close_quietly(self.channel)
        try:
            self.channel = self.conn.channel()
        except pika.exceptions.AMQPError:
            self.channel = None  # type: ignore[assignment]

    def close(self) -> None:
        close_quietly(self.channel, self.conn)

    def __enter__(self) -> "PassiveChannel":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def probe_queues(config: MQConfig, names: Sequence[str]) -> list[QueueProbe]:
    """Passive-declare many queues over a single connection.

    A broker channel error (404/403) kills the channel, so the channel is
    recreated after each failure -- otherwise one missing queue would abort the
    whole sweep and make the remaining queues look unreachable.
    """
    results: list[QueueProbe] = []
    if not names:
        return results

    session = PassiveChannel(config)
    try:
        for name in names:
            results.append(session.probe(name))
            if session.channel is None:
                for remaining in names[len(results):]:
                    results.append(
                        QueueProbe(
                            name=remaining,
                            exists=False,
                            observed=False,
                            error=AmqpErrorInfo(
                                kind="connection-lost",
                                detail="The connection dropped during the sweep.",
                            ),
                        )
                    )
                return results
    finally:
        session.close()

    return results


@dataclass
class DeclarationCheck:
    """One field of an expected queue declaration, checked against the broker."""

    field_name: str
    expected: Any
    matches: bool | None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"field": self.field_name, "expected": self.expected}
        data["matches"] = self.matches
        if self.detail:
            data["detail"] = self.detail
        return data


def verify_queue(
    config: MQConfig,
    queue: str,
    durable: bool | None = None,
    exclusive: bool | None = None,
    auto_delete: bool | None = None,
    arguments: dict[str, Any] | None = None,
) -> list[DeclarationCheck]:
    """Assert what a queue *is* using the protocol's own consistency check.

    AMQP has no "read queue arguments" call. But ``queue.declare`` against an
    existing queue with different parameters is rejected with 406
    PRECONDITION_FAILED, and each field can be probed one at a time. The first
    mismatch poisons the channel, so every field needs its own channel -- there
    is no cheaper way to read a declaration back over 0-9-1.
    """
    checks: list[DeclarationCheck] = []

    # A 406 assertion against a queue that does not exist is indistinguishable
    # from a genuine mismatch, so establish existence first.
    existing = passive_declare(config, queue)
    if not existing.exists:
        detail = "queue does not exist" if existing.error is None else existing.error.detail
        return [DeclarationCheck(field_name="exists", expected=True, matches=False, detail=detail)]
    checks.append(
        DeclarationCheck(
            field_name="exists",
            expected=True,
            matches=True,
            detail=f"ready={existing.ready} consumers={existing.consumers}",
        )
    )

    def probe(
        field_name: str, expect: Any, mismatch_kinds: tuple[str, ...] = ("precondition-failed",),
        **kwargs: Any,
    ) -> None:
        conn, channel, _ = create_channel(config)
        try:
            channel.queue_declare(queue=queue, **kwargs)
        except ChannelClosedByBroker as e:
            info = classify(e)
            if info.kind in mismatch_kinds:
                checks.append(
                    DeclarationCheck(
                        field_name=field_name, expected=expect, matches=False,
                        detail=info.detail,
                    )
                )
            elif info.code == 404:
                checks.append(
                    DeclarationCheck(
                        field_name=field_name, expected=expect, matches=None,
                        detail="queue does not exist",
                    )
                )
            elif info.kind == "access-refused":
                checks.append(
                    DeclarationCheck(
                        field_name=field_name, expected=expect, matches=None,
                        detail="no permission to inspect this queue",
                    )
                )
            else:
                checks.append(
                    DeclarationCheck(
                        field_name=field_name, expected=expect, matches=None,
                        detail=info.detail,
                    )
                )
        except pika.exceptions.AMQPError as e:
            checks.append(
                DeclarationCheck(
                    field_name=field_name, expected=expect, matches=None, detail=classify(e).detail
                )
            )
        else:
            checks.append(DeclarationCheck(field_name=field_name, expected=expect, matches=True))
        finally:
            close_quietly(channel, conn)

    # Layer the requested expectations one at a time. RabbitMQ compares exactly
    # the arguments it is given, so sending the whole table would let one
    # mismatch mask every other field.
    if durable is not None:
        probe("durable", durable, durable=durable)
    if auto_delete is not None:
        probe("auto_delete", auto_delete, auto_delete=auto_delete)
    if exclusive is not None:
        # A differing exclusive flag comes back as 405 RESOURCE_LOCKED rather
        # than 406. When we expect False, 405 only means someone else holds the
        # queue exclusively, which is not evidence either way.
        probe(
            "exclusive",
            exclusive,
            mismatch_kinds=("precondition-failed", "resource-locked")
            if exclusive
            else ("precondition-failed",),
            exclusive=exclusive,
        )
    for key in sorted(arguments or {}):
        probe(
            f"argument {key}",
            arguments[key],
            durable=bool(durable),
            arguments={key: arguments[key]},
        )
    return checks


def parse_arguments(pairs: Iterable[str] | None) -> dict[str, Any]:
    """Turn ``--arg x-queue-type=quorum`` style flags into an AMQP table.

    Values are coerced to int/bool where they clearly look like one, because
    RabbitMQ rejects ``"3600000"`` for x-message-ttl.
    """
    result: dict[str, Any] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(
                f"invalid argument '{pair}', expected key=value "
                f"(known: {', '.join(KNOWN_ARGUMENTS)})"
            )
        key, _, raw = pair.partition("=")
        key = key.strip()
        if not key:
            raise ValueError(f"invalid argument '{pair}': empty key")
        result[key] = _coerce_value(raw.strip())
    return result


def _coerce_value(raw: str) -> Any:
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(raw)
    except ValueError:
        return raw

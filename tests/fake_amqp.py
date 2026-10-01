"""An in-memory stand-in for a pika BlockingConnection.

Enough of the surface for the service layer: channel lifecycle, basic
get/publish/ack/nack with confirms, passive declares that can be made to fail
with a specific reply code, and publisher returns. Tests use it to assert the
ordering guarantees that matter (publish before ack, requeue only at the end).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pika
import pika.exceptions
from pika.exceptions import ChannelClosedByBroker


@dataclass
class FakeMessage:
    body: bytes
    properties: pika.BasicProperties | None = None
    exchange: str = ""
    routing_key: str = ""
    redelivered: bool = False
    queue: str = ""
    # Enqueue order. RabbitMQ restores a requeued message to (approximately) its
    # original position; sorting on this reproduces that.
    seq: int = 0


def _requeue(queue: list[FakeMessage], message: FakeMessage) -> None:
    for index, existing in enumerate(queue):
        if existing.seq > message.seq:
            queue.insert(index, message)
            return
    queue.append(message)


def _first_mismatch(existing: Declared, requested: dict[str, Any]) -> str | None:
    """The first inequivalent field, mirroring RabbitMQ's consistency check."""
    for field in ("durable", "auto_delete"):
        if field in requested and requested[field] != getattr(existing, field):
            return field
    arguments = requested.get("arguments") or {}
    for key, value in arguments.items():
        if (existing.arguments or {}).get(key) != value:
            return key
    return None


@dataclass
class Declared:
    name: str
    durable: bool = False
    exclusive: bool = False
    auto_delete: bool = False
    arguments: dict[str, Any] | None = None
    type: str = "direct"
    internal: bool = False
    consumer_count: int = 0
    bindings: list[tuple[str, str]] = field(default_factory=list)


def broker_error(code: int, text: str) -> ChannelClosedByBroker:
    return ChannelClosedByBroker(code, text)


class FakeBroker:
    """Holds the queues; shared by every channel created from one connection."""

    def __init__(self) -> None:
        self.queues: dict[str, list[FakeMessage]] = {}
        self.declared: dict[str, Declared] = {}
        self.exchanges: dict[str, Declared] = {"": Declared(name="", type="direct")}
        self.published: list[dict[str, Any]] = []
        self.acked: list[int] = []
        self.nacked: list[tuple[int, bool]] = []
        self.delivery_tags = itertools.count(1)
        self.unacked: dict[int, FakeMessage] = {}
        self.publish_failures: list[Exception] = []
        self.returned: list[tuple[int, str, str, str]] = []
        self.confirm_calls = 0
        self.seq = 0
        # name -> reply code forced for the next passive declare
        self.fail_next: dict[str, int] = {}

    # -- queue operations -------------------------------------------------
    def seed(self, queue: str, bodies, properties=None) -> list[FakeMessage]:
        """Fill a queue with messages, assigning a stable enqueue order."""
        self.declare(queue)
        messages: list[FakeMessage] = []
        for body in bodies:
            self.seq += 1
            messages.append(
                FakeMessage(
                    body=body if isinstance(body, bytes) else body.encode(),
                    properties=properties,
                    queue=queue,
                    seq=self.seq,
                )
            )
        self.queues[queue] = messages
        return messages

    def declare(self, name: str, **kwargs: Any) -> tuple[int, int, str]:
        if not name:
            name = f"amq.gen-{len(self.declared)}"
        if kwargs.get("passive"):
            if name in self.fail_next:
                code = self.fail_next.pop(name)
                raise broker_error(code, _text_for(code))
            if name not in self.declared:
                raise broker_error(404, f"NOT_FOUND - no queue '{name}' in vhost '/'")
            queue = self.queues.setdefault(name, [])
            return len(queue), self.declared[name].consumer_count, name

        # RabbitMQ refuses a redeclaration whose parameters differ. This is the
        # check verify_queue() relies on, so the fake has to model it.
        existing = self.declared.get(name)
        if existing is not None:
            if kwargs.get("exclusive"):
                raise broker_error(
                    405, "RESOURCE_LOCKED - cannot obtain exclusive access to queue"
                )
            mismatch = _first_mismatch(existing, kwargs)
            if mismatch:
                raise broker_error(
                    406, f"PRECONDITION_FAILED - inequivalent arg '{mismatch}'"
                )

        self.declared[name] = Declared(name=name, **{
            k: v for k, v in kwargs.items() if k in
            ("durable", "exclusive", "auto_delete", "arguments")
        })
        self.queues.setdefault(name, [])
        return len(self.queues[name]), 0, name

    def purge(self, name: str) -> int:
        queue = self.queues.get(name)
        if queue is None:
            raise broker_error(404, f"NOT_FOUND - no queue '{name}'")
        count = len(queue)
        queue.clear()
        return count

    def delete(self, name: str) -> int:
        queue = self.queues.pop(name, None)
        self.declared.pop(name, None)
        return len(queue or [])

    def basic_get(self, name: str) -> tuple[int | None, Any, bytes | None]:
        if name in self.fail_next:
            code = self.fail_next.pop(name)
            raise broker_error(code, _text_for(code))
        if name not in self.queues:
            raise broker_error(404, f"NOT_FOUND - no queue '{name}' in vhost '/'")
        queue = self.queues[name]
        if not queue:
            return None, None, None
        message = queue.pop(0)
        tag = next(self.delivery_tags)
        self.unacked[tag] = message
        return tag, message.properties, message.body

    def ack(self, tag: int, multiple: bool = False) -> None:
        self.acked.append(tag)
        self.unacked.pop(tag, None)

    def nack(self, tag: int, requeue: bool, multiple: bool = False) -> None:
        self.nacked.append((tag, requeue))
        message = self.unacked.pop(tag, None)
        if requeue and message is not None and message.queue:
            _requeue(self.queues.setdefault(message.queue, []), message)

    # -- exchange operations ---------------------------------------------
    def exchange_declare(self, name: str, **kwargs: Any) -> None:
        if kwargs.get("passive"):
            if name in self.fail_next:
                code = self.fail_next.pop(name)
                raise broker_error(code, _text_for(code))
            if name not in self.exchanges:
                raise broker_error(404, f"NOT_FOUND - no exchange '{name}' in vhost '/'")
            return
        self.exchanges[name] = Declared(
            name=name,
            type=kwargs.get("exchange_type", "direct"),
            durable=kwargs.get("durable", False),
            auto_delete=kwargs.get("auto_delete", False),
            internal=kwargs.get("internal", False),
            arguments=kwargs.get("arguments"),
        )

    def queue_bind(self, queue: str, exchange: str, routing_key: str, arguments=None) -> None:
        if exchange not in self.exchanges:
            raise broker_error(404, f"NOT_FOUND - no exchange '{exchange}'")
        if queue not in self.queues:
            raise broker_error(404, f"NOT_FOUND - no queue '{queue}'")
        self.exchanges[exchange].bindings.append((queue, routing_key))

    # -- publish ---------------------------------------------------------
    def publish(self, **kwargs: Any) -> None:
        if self.publish_failures:
            raise self.publish_failures.pop(0)
        self.published.append(dict(kwargs))
        targets = self._route(kwargs.get("exchange", ""), kwargs.get("routing_key", ""))
        if kwargs.get("mandatory") and not targets:
            self.returned.append(
                (312, "NO_ROUTE", kwargs.get("exchange", ""), kwargs.get("routing_key", ""))
            )
            return
        for queue in targets:
            self.seq += 1
            self.queues[queue].append(
                FakeMessage(
                    body=kwargs.get("body", b""),
                    properties=kwargs.get("properties"),
                    exchange=kwargs.get("exchange", ""),
                    routing_key=kwargs.get("routing_key", ""),
                    queue=queue,
                    seq=self.seq,
                )
            )

    def _route(self, exchange: str, routing_key: str) -> list[str]:
        """Queues a published message would actually reach."""
        if exchange == "":
            return [routing_key] if routing_key in self.queues else []
        declared = self.exchanges.get(exchange)
        if declared is None:
            return []
        return [
            queue
            for queue, key in declared.bindings
            if key == routing_key and queue in self.queues
        ]


def _text_for(code: int) -> str:
    return {
        403: "ACCESS_REFUSED - access to queue refused",
        404: "NOT_FOUND - no queue",
        405: "RESOURCE_LOCKED - cannot obtain exclusive access",
        406: "PRECONDITION_FAILED - inequivalent arg 'durable'",
        530: "NOT_ALLOWED - vhost does not exist",
    }.get(code, f"error {code}")


class FakeChannel:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.channel_number = 1
        self.closed = False
        self.consume_callback = None
        self._queue: str | None = None

    # -- lifecycle -------------------------------------------------------
    def confirm_delivery(self) -> None:
        self.broker.confirm_calls += 1

    def close(self) -> None:
        self.closed = True

    # -- queue -----------------------------------------------------------
    def queue_declare(self, queue: str, passive: bool = False, durable: bool = False,
                      exclusive: bool = False, auto_delete: bool = False,
                      arguments: dict | None = None):
        count, consumers, assigned = self.broker.declare(
            queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )
        return _frame(message_count=count, consumer_count=consumers, queue=assigned)

    def queue_purge(self, queue: str):
        return _frame(message_count=self.broker.purge(queue))

    def queue_delete(self, queue: str, if_unused: bool = False, if_empty: bool = False):
        return _frame(message_count=self.broker.delete(queue))

    def queue_bind(self, queue: str, exchange: str, routing_key: str = "", arguments=None):
        self.broker.queue_bind(queue, exchange, routing_key, arguments)

    def queue_unbind(self, queue: str, exchange: str, routing_key: str = "", arguments=None):
        pass

    # -- exchange --------------------------------------------------------
    def exchange_declare(self, exchange: str, exchange_type: str = "direct", passive: bool = False,
                         durable: bool = False, auto_delete: bool = False,
                         internal: bool = False, arguments=None):
        self.broker.exchange_declare(
            exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments,
        )

    # -- messages --------------------------------------------------------
    def basic_get(self, queue: str, auto_ack: bool = False):
        tag, properties, body = self.broker.basic_get(queue)
        if tag is None:
            return None, None, None
        message = self.broker.unacked[tag]
        message.queue = queue
        if auto_ack:
            self.broker.ack(tag)
        return _get_ok(tag, queue, message), properties, body

    def basic_ack(self, delivery_tag: int, multiple: bool = False) -> None:
        self.broker.ack(delivery_tag, multiple)

    def basic_nack(self, delivery_tag: int, requeue: bool = True, multiple: bool = False) -> None:
        self.broker.nack(delivery_tag, requeue, multiple)

    def basic_publish(self, exchange: str = "", routing_key: str = "", body: bytes = b"",
                      properties=None, mandatory: bool = False):
        self.broker.publish(
            exchange=exchange, routing_key=routing_key, body=body,
            properties=properties, mandatory=mandatory,
        )

    def add_on_return_callback(self, callback) -> None:
        self._return_callback = callback

    _return_callback = None

    def process_data_events(self, time_limit: float = 0) -> None:
        for code, text, exchange, key in list(self.broker.returned):
            self.broker.returned.remove((code, text, exchange, key))
            self._return_callback(
                self, _returned(code, text, exchange, key), None, b""
            )

    # -- consume ---------------------------------------------------------
    def basic_qos(self, prefetch_count: int = 0) -> None:
        self.prefetch = prefetch_count

    def basic_consume(self, queue: str, on_message_callback, auto_ack: bool = False):
        self.consume_callback = on_message_callback
        self._queue = queue

    def start_consuming(self) -> None:
        # Deliver everything currently queued, honouring stop_consuming.
        while True:
            if self.consume_callback is None or self._queue is None:
                return
            queue = self.queues.get(self._queue) or []
            if not queue:
                return
            tag, properties, body = self.broker.basic_get(self._queue)
            message = self.broker.unacked[tag]
            message.queue = self._queue
            self.consume_callback(self, _deliver(tag, self._queue, message), properties, body)

    def stop_consuming(self) -> None:
        self.consume_callback = None


@dataclass
class FakeParams:
    """Enough of ConnectionParameters for commands that echo the endpoint."""

    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    heartbeat: int = 60


class FakeConnection:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.closed = False
        self._blocked = []
        self._unblocked = []
        self._channels: list[FakeChannel] = []
        self.params = FakeParams()
        self._impl = SimpleNamespace(
            server_properties={
                "product": "RabbitMQ",
                "version": "4.0.5",
                "cluster_name": "test-cluster",
                "information": "Test Server",
                "capabilities": {
                    "publisher_confirms": True,
                    "consumer_cancel_notify": True,
                    "basic.nack": True,
                    "connection.blocked": True,
                    "exchange_exchange_bindings": True,
                    "per_consumer_qos": True,
                },
            },
            server_capabilities={"publisher_confirms": True},
            _heartbeat=30,
        )

    def channel(self) -> FakeChannel:
        channel = FakeChannel(self.broker)
        self._channels.append(channel)
        return channel

    def close(self) -> None:
        self.closed = True

    def add_on_connection_blocked_callback(self, callback) -> None:
        self._blocked.append(callback)

    def add_on_connection_unblocked_callback(self, callback) -> None:
        self._unblocked.append(callback)

    def process_data_events(self, time_limit: float = 0) -> None:
        # Real pika dispatches to the channels; returns and confirms surface here.
        for channel in list(self._channels):
            channel.process_data_events(time_limit)


def _frame(**kwargs: Any):
    return SimpleNamespace(method=SimpleNamespace(**kwargs))


def _get_ok(tag: int, routing_key: str, message: FakeMessage):
    # Real pika hands back the Basic.GetOk method object itself, so
    # ``method_frame.delivery_tag`` works without unwrapping.
    return SimpleNamespace(
        delivery_tag=tag,
        redelivered=message.redelivered,
        routing_key=routing_key,
        exchange=message.exchange,
    )


def _deliver(tag: int, routing_key: str, message: FakeMessage):
    return _get_ok(tag, routing_key, message)


def _returned(code: int, text: str, exchange: str, routing_key: str):
    return SimpleNamespace(
        method=SimpleNamespace(
            reply_code=code,
            reply_text=text,
            exchange=exchange,
            routing_key=routing_key,
        )
    )

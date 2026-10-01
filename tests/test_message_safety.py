"""Regression tests for the data-loss bugs this rewrite set out to fix.

Each test names the failure it prevents. They run against an in-memory fake
broker, so no RabbitMQ node is needed.
"""

from __future__ import annotations

import contextlib
import tempfile
import unittest
from pathlib import Path

import pika

from mq.commands import export_import
from mq.commands import replay as replay_cmd
from mq.commands import retry as retry_cmd
from mq.services import message_service
from tests.fake_amqp import FakeBroker
from tests.support import QuietTestCase, fake_connection, make_config


@contextlib.contextmanager
def export_target():
    with tempfile.TemporaryDirectory() as tmp:
        yield str(Path(tmp) / "export.json")


def seeded(broker: FakeBroker, queue: str, bodies: list[str]) -> None:
    broker.seed(queue, bodies)


def body_list(broker: FakeBroker, queue: str) -> list[bytes]:
    return [m.body for m in broker.queues.get(queue, [])]


class ExportIsNonDestructive(QuietTestCase):
    """``mq export`` used to nack(requeue=False) and silently empty the queue."""

    def test_messages_stay_on_the_queue(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["one", "two", "three"])
        config = make_config()

        with fake_connection(broker), export_target() as path:
            export_import.execute_export(config, "orders", output=path)

        self.assertEqual(3, len(broker.queues["orders"]), "queue must not be drained")
        self.assertEqual([], broker.published, "export must not publish")

    def test_no_message_is_discarded(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["one", "two", "three"])
        config = make_config()

        with fake_connection(broker), export_target() as path:
            export_import.execute_export(config, "orders", output=path)

        self.assertTrue(broker.nacked)
        self.assertTrue(
            all(requeue for _, requeue in broker.nacked),
            f"a message was dropped: {broker.nacked}",
        )

    def test_destructive_flag_consumes_the_queue(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["one", "two"])
        config = make_config()

        with fake_connection(broker), export_target() as path:
            export_import.execute_export(config, "orders", output=path, destructive=True)

        self.assertEqual([], broker.queues["orders"])

    def test_all_reads_happen_before_any_requeue(self):
        """Requeueing inside the read loop lets basic_get hand back the same message."""
        broker = FakeBroker()
        seeded(broker, "orders", ["a", "b", "c"])
        config = make_config()

        events: list[str] = []
        original_get, original_nack = broker.basic_get, broker.nack

        def traced_get(name):
            events.append("get")
            return original_get(name)

        def traced_nack(tag, requeue, multiple=False):
            events.append("nack")
            return original_nack(tag, requeue, multiple)

        broker.basic_get = traced_get  # type: ignore[method-assign]
        broker.nack = traced_nack  # type: ignore[method-assign]

        with fake_connection(broker), export_target() as path:
            export_import.execute_export(config, "orders", output=path)

        self.assertEqual(
            ["get", "get", "get", "get", "nack", "nack", "nack"],
            events,
        )


class MoveIsCrashSafe(QuietTestCase):
    """move/retry/replay used to ack the source before publishing."""

    def test_publish_happens_before_the_source_ack(self):
        broker = FakeBroker()
        seeded(broker, "src", ["payload"])
        broker.declare("dst")
        config = make_config()

        order: list[str] = []
        original_publish, original_ack = broker.publish, broker.ack

        def traced_publish(**kwargs):
            order.append("publish")
            return original_publish(**kwargs)

        def traced_ack(tag, multiple=False):
            order.append("ack")
            return original_ack(tag, multiple)

        broker.publish = traced_publish  # type: ignore[method-assign]
        broker.ack = traced_ack  # type: ignore[method-assign]

        with fake_connection(broker):
            moved = message_service.move_messages(config, "src", "dst", count=1)

        self.assertEqual(1, moved)
        self.assertEqual(["publish", "ack"], order)
        self.assertGreaterEqual(broker.confirm_calls, 1, "confirms must be enabled")

    def test_publish_failure_leaves_the_message_on_the_source(self):
        broker = FakeBroker()
        seeded(broker, "src", ["payload"])
        broker.declare("dst")
        broker.publish_failures.append(RuntimeError("broker went away"))
        config = make_config()

        with fake_connection(broker):
            with self.assertRaises(RuntimeError):
                message_service.move_messages(config, "src", "dst", count=1)

        self.assertEqual([b"payload"], body_list(broker, "src"))
        self.assertEqual([], broker.acked)

    def test_unroutable_destination_is_reported(self):
        broker = FakeBroker()
        seeded(broker, "src", ["payload"])
        # No "dst" queue exists, so a mandatory publish returns NO_ROUTE.
        config = make_config()

        with fake_connection(broker):
            with self.assertRaises(message_service.ReturnedMessage):
                message_service.move_messages(
                    config, "src", "dst", count=1, mandatory=True
                )

        self.assertEqual([b"payload"], body_list(broker, "src"))


def dlq_properties() -> pika.BasicProperties:
    return pika.BasicProperties(
        headers={
            "x-death": [
                {
                    "count": 1,
                    "queue": "orders.dlq",
                    "reason": "rejected",
                    "exchange": "orders.events",
                    "routing-keys": ["order.created"],
                }
            ]
        },
        content_type="application/json",
    )


class RetryStripsDeathHeaders(QuietTestCase):
    """Forwarding x-death made the broker dead-letter the message again."""

    def test_x_death_is_removed_and_the_route_recovered(self):
        broker = FakeBroker()
        broker.declare("orders.dlq")
        broker.declare("orders")
        broker.exchange_declare("orders.events", exchange_type="topic")
        broker.queue_bind("orders", "orders.events", "order.created")
        broker.seed("orders.dlq", [b"{}"], properties=dlq_properties())
        config = make_config()

        with fake_connection(broker):
            retry_cmd.execute(config, "orders.dlq", limit=1, mandatory=True)

        self.assertEqual(1, len(broker.published))
        sent = broker.published[0]
        self.assertEqual("orders.events", sent["exchange"])
        self.assertEqual("order.created", sent["routing_key"])

        headers = sent["properties"].headers
        self.assertNotIn("x-death", headers, "x-death must be stripped")
        self.assertEqual(1, headers["x-retry-count"])
        self.assertEqual("orders.dlq", headers["x-retried-from"])
        self.assertEqual("application/json", sent["properties"].content_type)

    def test_explicit_target_overrides_x_death(self):
        broker = FakeBroker()
        broker.declare("orders.dlq")
        broker.declare("orders")
        broker.seed("orders.dlq", [b"{}"], properties=dlq_properties())
        config = make_config()

        with fake_connection(broker):
            retry_cmd.execute(
                config, "orders.dlq", limit=1, target_queue="orders", mandatory=True
            )

        self.assertEqual("orders", broker.published[0]["routing_key"])

    def test_publish_failure_keeps_the_message_on_the_dlq(self):
        broker = FakeBroker()
        broker.declare("orders.dlq")
        broker.declare("orders")
        broker.seed("orders.dlq", [b"{}"], properties=dlq_properties())
        broker.publish_failures.append(RuntimeError("nope"))
        config = make_config()

        with fake_connection(broker):
            # retry surfaces the failure as a non-zero exit; the important part
            # is that the message was returned to the DLQ rather than acked.
            with self.assertRaises(SystemExit) as raised:
                retry_cmd.execute(config, "orders.dlq", limit=1, target_queue="orders")

        self.assertNotEqual(0, raised.exception.code)
        self.assertEqual(1, len(broker.queues["orders.dlq"]))
        self.assertEqual([], broker.acked)
        self.assertTrue(all(requeue for _, requeue in broker.nacked))


class ReplayIsCrashSafe(QuietTestCase):
    def test_publish_before_ack(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["a", "b"])
        config = make_config()

        order: list[str] = []
        original_publish, original_ack = broker.publish, broker.ack

        def traced_publish(**kwargs):
            order.append("publish")
            return original_publish(**kwargs)

        def traced_ack(tag, multiple=False):
            order.append("ack")
            return original_ack(tag, multiple)

        broker.publish = traced_publish  # type: ignore[method-assign]
        broker.ack = traced_ack  # type: ignore[method-assign]

        with fake_connection(broker):
            replay_cmd.execute(config, "orders", limit=2)

        self.assertEqual(["publish", "ack", "publish", "ack"], order)


class PeekDoesNotRepeatMessages(QuietTestCase):
    """Requeueing per message made ``peek --count 5`` print message #1 five times."""

    def test_distinct_messages_are_returned(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["a", "b", "c", "d", "e", "f"])
        config = make_config()

        with fake_connection(broker):
            messages = message_service.peek_messages(config, "orders", 5)

        self.assertEqual(
            [b"a", b"b", b"c", b"d", b"e"], [body for _, _, body in messages]
        )
        self.assertEqual(5, len(broker.nacked))
        self.assertTrue(all(requeue for _, requeue in broker.nacked))
        self.assertEqual(
            [b"a", b"b", b"c", b"d", b"e", b"f"], body_list(broker, "orders")
        )

    def test_empty_queue_returns_nothing(self):
        broker = FakeBroker()
        broker.declare("orders")
        config = make_config()

        with fake_connection(broker):
            self.assertEqual([], message_service.peek_messages(config, "orders", 5))


class GetByIndexIsAccurate(QuietTestCase):
    def test_index_reaches_the_right_message(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["a", "b", "c"])
        config = make_config()

        with fake_connection(broker):
            _, _, body, action = message_service.get_message(config, "orders", index=3)

        self.assertEqual(b"c", body)
        self.assertEqual("requeued", action)
        self.assertEqual([b"a", b"b", b"c"], body_list(broker, "orders"))

    def test_ack_removes_only_the_target(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["a", "b", "c"])
        config = make_config()

        with fake_connection(broker):
            _, _, body, action = message_service.get_message(
                config, "orders", index=2, ack=True
            )

        self.assertEqual(b"b", body)
        self.assertEqual("acknowledged and removed", action)
        self.assertEqual([b"a", b"c"], body_list(broker, "orders"))

    def test_index_past_the_end_raises_and_keeps_the_queue(self):
        broker = FakeBroker()
        seeded(broker, "orders", ["a"])
        config = make_config()

        with fake_connection(broker):
            with self.assertRaises(IndexError):
                message_service.get_message(config, "orders", index=5)

        self.assertEqual([b"a"], body_list(broker, "orders"))

    def test_index_must_be_positive(self):
        broker = FakeBroker()
        broker.declare("orders")
        config = make_config()

        with fake_connection(broker):
            with self.assertRaises(ValueError):
                message_service.get_message(config, "orders", index=0)


if __name__ == "__main__":
    unittest.main()

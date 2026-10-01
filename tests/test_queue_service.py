"""Passive declares, argument parsing and drift detection over AMQP."""

from __future__ import annotations

import unittest

from mq.services import discovery
from mq.services.exchange_service import bind_queue, declare_exchange
from mq.services.queue_service import (
    declare_queue,
    delete_queue,
    parse_arguments,
    probe_queues,
    purge_queue,
    verify_queue,
)
from tests.fake_amqp import FakeBroker
from tests.support import QuietTestCase, fake_connection, make_config, temp_config_dir


class ArgumentParsing(QuietTestCase):
    def test_coerces_integers(self):
        # RabbitMQ rejects a string TTL, so values must be typed.
        self.assertEqual({"x-message-ttl": 3600000}, parse_arguments(["x-message-ttl=3600000"]))

    def test_coerces_booleans(self):
        self.assertEqual({"x-single-active-consumer": True}, parse_arguments(["x-single-active-consumer=true"]))
        self.assertEqual({"x-single-active-consumer": False}, parse_arguments(["x-single-active-consumer=false"]))

    def test_keeps_strings(self):
        self.assertEqual({"x-queue-type": "quorum"}, parse_arguments(["x-queue-type=quorum"]))

    def test_several_pairs(self):
        result = parse_arguments(["x-queue-type=quorum", "x-max-length=100"])
        self.assertEqual({"x-queue-type": "quorum", "x-max-length": 100}, result)

    def test_value_containing_equals_is_preserved(self):
        self.assertEqual({"x-match": "a=b"}, parse_arguments(["x-match=a=b"]))

    def test_missing_equals_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_arguments(["x-queue-type"])

    def test_empty_key_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_arguments(["=quorum"])

    def test_none_is_empty(self):
        self.assertEqual({}, parse_arguments(None))


class QueueLifecycle(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.broker = FakeBroker()
        self.config = make_config()

    def test_declare_returns_broker_assigned_name(self):
        with fake_connection(self.broker):
            _, _, name = declare_queue(self.config, "")
        self.assertTrue(name.startswith("amq.gen-"))

    def test_declare_forwards_arguments(self):
        with fake_connection(self.broker):
            declare_queue(
                self.config, "orders", durable=True, arguments={"x-queue-type": "quorum"}
            )
        declared = self.broker.declared["orders"]
        self.assertTrue(declared.durable)
        self.assertEqual({"x-queue-type": "quorum"}, declared.arguments)

    def test_purge_empties_the_queue(self):
        self.broker.seed("orders", ["a", "b", "c"])
        with fake_connection(self.broker):
            self.assertEqual(3, purge_queue(self.config, "orders"))
        self.assertEqual([], self.broker.queues["orders"])

    def test_delete_removes_from_inventory(self):
        with temp_config_dir():
            self.broker.declare("orders")
            with fake_connection(self.broker):
                delete_queue(self.config, "orders")
            from mq import inventory

            self.assertEqual([], inventory.list_entries("queue", "/"))


class Probing(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.broker = FakeBroker()
        self.config = make_config()

    def test_reports_depth_and_consumers(self):
        self.broker.seed("orders", ["a", "b"])
        with fake_connection(self.broker):
            probes = probe_queues(self.config, ["orders"])
        self.assertTrue(probes[0].exists)
        self.assertEqual(2, probes[0].ready)

    def test_missing_queue_is_a_result_not_an_exception(self):
        with fake_connection(self.broker):
            probes = probe_queues(self.config, ["nope"])
        self.assertFalse(probes[0].exists)
        self.assertEqual("missing", probes[0].status)

    def test_forbidden_is_distinguished_from_missing(self):
        self.broker.declare("orders")
        self.broker.fail_next["orders"] = 403
        with fake_connection(self.broker):
            probes = probe_queues(self.config, ["orders"])
        self.assertEqual("forbidden", probes[0].status)

    def test_sweep_survives_a_missing_queue(self):
        """A 404 closes the channel; the rest of the sweep must still run."""
        self.broker.seed("a", ["1"])
        self.broker.seed("c", ["3"])
        with fake_connection(self.broker):
            probes = probe_queues(self.config, ["a", "b", "c"])
        self.assertEqual(["a", "b", "c"], [p.name for p in probes])
        self.assertEqual([True, False, True], [p.exists for p in probes])

    def test_sweep_survives_a_forbidden_queue(self):
        self.broker.seed("a", ["1"])
        self.broker.seed("c", ["3"])
        self.broker.fail_next["b"] = 403
        with fake_connection(self.broker):
            probes = probe_queues(self.config, ["a", "b", "c"])
        self.assertEqual([True, False, True], [p.exists for p in probes])

    def test_empty_input_opens_no_connection(self):
        with fake_connection(self.broker):
            self.assertEqual([], probe_queues(self.config, []))


class VerifyDeclarations(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.broker = FakeBroker()
        self.config = make_config()

    def test_matching_declaration_reports_match(self):
        self.broker.declare("orders", durable=True)
        with fake_connection(self.broker):
            checks = verify_queue(self.config, "orders", durable=True)
        by_field = {c.field_name: c for c in checks}
        self.assertTrue(by_field["exists"].matches)
        self.assertTrue(by_field["durable"].matches)

    def test_mismatched_durability_is_reported(self):
        self.broker.declare("orders", durable=False)
        with fake_connection(self.broker):
            checks = verify_queue(self.config, "orders", durable=True)
        by_field = {c.field_name: c for c in checks}
        self.assertFalse(by_field["durable"].matches)

    def test_missing_queue_short_circuits(self):
        with fake_connection(self.broker):
            checks = verify_queue(self.config, "ghost", durable=True)
        self.assertEqual(1, len(checks))
        self.assertFalse(checks[0].matches)

    def test_argument_is_probed_individually(self):
        self.broker.declare("orders", arguments={"x-queue-type": "quorum"})
        with fake_connection(self.broker):
            checks = verify_queue(
                self.config, "orders", arguments={"x-queue-type": "classic"}
            )
        mismatch = [c for c in checks if c.field_name.startswith("argument")]
        self.assertEqual(1, len(mismatch))
        self.assertFalse(mismatch[0].matches)

    def test_matching_argument_reports_match(self):
        self.broker.declare("orders", arguments={"x-queue-type": "quorum"})
        with fake_connection(self.broker):
            checks = verify_queue(
                self.config, "orders", arguments={"x-queue-type": "quorum"}
            )
        match = [c for c in checks if c.field_name.startswith("argument")]
        self.assertTrue(match[0].matches)


class ExchangeBinding(QuietTestCase):
    def test_bind_records_both_objects_in_the_inventory(self):
        broker = FakeBroker()
        config = make_config()
        broker.declare("orders")
        with temp_config_dir():
            with fake_connection(broker):
                declare_exchange(config, "orders.events", exchange_type="topic")
                bind_queue(config, "orders", "orders.events", "order.created")
            from mq import inventory

            self.assertEqual(["orders"], [e.name for e in inventory.list_entries("queue", "/")])
            self.assertEqual(
                ["orders.events"], [e.name for e in inventory.list_entries("exchange", "/")]
            )

    def test_default_exchange_routing(self):
        broker = FakeBroker()
        config = make_config()
        broker.declare("orders")
        with fake_connection(broker):
            bind_queue(config, "orders", "", "")
        self.assertEqual([("orders", "")], broker.exchanges[""].bindings)


class Discovery(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._ctx = temp_config_dir()
        self._ctx.__enter__()
        self.config = make_config()

    def tearDown(self) -> None:
        self._ctx.__exit__(None, None, None)
        super().tearDown()

    def test_resolve_merges_explicit_and_tracked_names(self):
        from mq import inventory

        inventory.add("tracked", "/", kind="queue")
        names = discovery.resolve_queue_names(self.config, None, ["explicit", "other"])
        self.assertEqual(["explicit", "other", "tracked"], names)

    def test_resolve_splits_comma_lists(self):
        names = discovery.resolve_queue_names(self.config, None, ["a,b , c"])
        self.assertEqual(["a", "b", "c"], names)

    def test_pattern_filters_tracked_names(self):
        from mq import inventory

        inventory.add("orders.eu", "/", kind="queue")
        inventory.add("payments", "/", kind="queue")
        names = discovery.resolve_queue_names(self.config, ["orders*"], None)
        self.assertEqual(["orders.eu"], names)

    def test_list_queues_reports_status_per_name(self):
        broker = FakeBroker()
        broker.seed("a", ["1"])
        with fake_connection(broker):
            rows = discovery.list_queues(self.config, None, ["a", "ghost"])
        self.assertEqual(["ok", "missing"], [r.status for r in rows])
        self.assertEqual(1, rows[0].ready)

    def test_list_exchanges_always_includes_the_default(self):
        broker = FakeBroker()
        with fake_connection(broker):
            rows = discovery.list_exchanges(self.config)
        self.assertEqual(["(default)"], [row.table_row()[0] for row in rows])
        self.assertEqual(["ok"], [row.status for row in rows])

    def test_list_exchanges_reports_missing_names(self):
        broker = FakeBroker()
        with fake_connection(broker):
            rows = discovery.list_exchanges(self.config, None, ["ghost"])
        self.assertEqual(["missing"], [row.status for row in rows])

    def test_watch_derives_a_rate_from_depth_deltas(self):
        broker = FakeBroker()
        broker.seed("orders", ["1", "2", "3"])
        config = make_config()
        ticks = iter([100.0, 102.0, 104.0, 106.0, 108.0])

        with fake_connection(broker):
            samples = discovery.watch_queue(
                config, "orders", iterations=3, sleep=lambda _: None, now=lambda: next(ticks)
            )

        self.assertEqual(3, len(samples))
        self.assertIsNone(samples[0].delta_ready, "the first sample has no delta")
        self.assertEqual(0, samples[1].delta_ready, "a static queue has no delta")
        self.assertEqual(0.0, samples[1].rate)
        self.assertEqual(3, samples[0].ready)
        self.assertEqual(0, samples[0].consumers)

    def test_watch_rate_reflects_a_backlog_growing(self):
        broker = FakeBroker()
        broker.seed("orders", ["1"])
        config = make_config()
        ticks = iter([0.0, 1.0, 2.0, 3.0])

        def grow(_seconds):
            """Two extra messages arrive between the first and second poll."""
            broker.queues["orders"].extend(_messages(broker, "orders", 2))

        with fake_connection(broker):
            samples = discovery.watch_queue(
                config, "orders", iterations=2, sleep=grow, now=lambda: next(ticks)
            )

        self.assertEqual(2, len(samples))
        self.assertEqual(2, samples[1].delta_ready)
        self.assertEqual(2.0, samples[1].rate)

    def test_watch_reports_a_deleted_queue(self):
        broker = FakeBroker()  # the queue does not exist at all
        config = make_config()

        with fake_connection(broker):
            samples = discovery.watch_queue(
                config, "orders", iterations=1, sleep=lambda _: None
            )

        self.assertEqual([], samples, "an unreachable queue yields no samples")

    def test_watch_still_returns_samples_when_the_queue_disappears(self):
        broker = FakeBroker()
        broker.seed("orders", ["1"])
        config = make_config()
        state = {"first": True}

        def drop_it(_seconds):
            if state["first"]:
                broker.queues.pop("orders", None)
                broker.declared.pop("orders", None)
                state["first"] = False

        with fake_connection(broker):
            samples = discovery.watch_queue(
                config, "orders", iterations=2, sleep=drop_it
            )

        self.assertEqual(1, len(samples))
        self.assertEqual(1, samples[0].ready)

    def test_watch_stops_after_the_requested_iterations(self):
        broker = FakeBroker()
        broker.seed("orders", ["1"])
        config = make_config()

        with fake_connection(broker):
            samples = discovery.watch_queue(
                config, "orders", iterations=2, sleep=lambda _: None
            )

        self.assertEqual(2, len(samples))


def _messages(broker: FakeBroker, queue: str, count: int):
    from tests.fake_amqp import FakeMessage

    produced = []
    for _ in range(count):
        broker.seq += 1
        produced.append(FakeMessage(body=b"x", queue=queue, seq=broker.seq))
    return produced


if __name__ == "__main__":
    unittest.main()

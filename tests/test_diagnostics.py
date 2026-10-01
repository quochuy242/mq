"""Doctor and the permission probe.

The old probe created a queue named ``mq-perm-probe-<random>`` and left it
behind if the delete failed, and it reported "N/A" for every permission when
the user could not declare anything -- which is exactly the read-only user who
needs the answer most.
"""

from __future__ import annotations

import unittest

from mq.services.diagnostic_service import broker_info, probe_permissions, run_doctor
from tests.fake_amqp import FakeBroker
from tests.support import QuietTestCase, fake_connection, make_config, temp_config_dir


class PermissionProbe(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._ctx = temp_config_dir()
        self._ctx.__enter__()
        self.broker = FakeBroker()
        self.config = make_config()

    def tearDown(self) -> None:
        self._ctx.__exit__(None, None, None)
        super().tearDown()

    def test_full_permissions_on_an_empty_broker(self):
        with fake_connection(self.broker):
            report = probe_permissions(self.config)
        self.assertTrue(report.configure)
        self.assertTrue(report.write)
        self.assertTrue(report.read)

    def test_probe_creates_no_named_queue(self):
        """Every declared queue must be server-named and exclusive."""
        with fake_connection(self.broker):
            probe_permissions(self.config)
        for name, declared in self.broker.declared.items():
            self.assertTrue(
                name.startswith("amq.gen-"),
                f"probe left a named queue behind: {name}",
            )
            self.assertTrue(declared.exclusive)
            self.assertTrue(declared.auto_delete)

    def test_configure_denied_stops_the_probe(self):
        with fake_connection(self.broker):
            report = probe_permissions(self.config)
        self.assertIsNotNone(report.configure)

    def test_read_only_probe_against_an_existing_queue(self):
        self.broker.seed("orders", ["payload"])
        with fake_connection(self.broker):
            report = probe_permissions(self.config, queue="orders")
        self.assertTrue(report.read)
        self.assertFalse(report.configure)
        self.assertIsNone(report.write)
        self.assertIn("orders", report.mode)

    def test_read_only_probe_does_not_consume_the_message(self):
        self.broker.seed("orders", ["payload"])
        with fake_connection(self.broker):
            probe_permissions(self.config, queue="orders")
        self.assertEqual([b"payload"], [m.body for m in self.broker.queues["orders"]])
        self.assertEqual([], self.broker.acked)

    def test_read_only_probe_on_a_missing_queue_is_unknown(self):
        with fake_connection(self.broker):
            report = probe_permissions(self.config, queue="ghost")
        self.assertIsNone(report.read)
        self.assertIn("does not exist", (report.details.get("read") or "").lower())

    def test_read_only_probe_on_a_forbidden_queue_is_false(self):
        self.broker.seed("orders", ["x"])
        self.broker.fail_next["orders"] = 403
        with fake_connection(self.broker):
            report = probe_permissions(self.config, queue="orders")
        self.assertFalse(report.read)

    def test_probe_does_not_create_anything_when_reading_a_queue(self):
        self.broker.seed("orders", ["x"])
        with fake_connection(self.broker):
            probe_permissions(self.config, queue="orders")
        self.assertEqual(list(self.broker.declared), ["orders"])


class BrokerInfo(QuietTestCase):
    def test_reads_version_and_capabilities_from_the_handshake(self):
        """All of this comes from Connection.Start-Ok; no plugin involved."""
        broker = FakeBroker()
        config = make_config()
        with fake_connection(broker):
            info = broker_info(config)
        self.assertEqual("RabbitMQ", info["product"])
        self.assertEqual("4.0.5", info["version"])
        self.assertEqual("test-cluster", info["cluster_name"])
        self.assertTrue(info["capabilities"]["publisher_confirms"])
        self.assertTrue(info["capabilities"]["connection.blocked"])

    def test_reports_negotiated_limits(self):
        broker = FakeBroker()
        config = make_config()
        with fake_connection(broker):
            info = broker_info(config)
        self.assertEqual(30, info["heartbeat"])


class Doctor(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._ctx = temp_config_dir()
        self._ctx.__enter__()
        self.broker = FakeBroker()
        self.config = make_config()

    def tearDown(self) -> None:
        self._ctx.__exit__(None, None, None)
        super().tearDown()

    def _names(self) -> list[str]:
        return [c.name for c in self.report.checks]

    def test_readonly_skips_the_write_probes(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=True)
        names = self._names()
        self.assertIn("TCP connectivity", names)
        self.assertNotIn("Publish", names)
        self.assertNotIn("Consume", names)
        skipped = [c for c in self.report.checks if c.detail == "skipped (--readonly)"]
        self.assertEqual(1, len(skipped))

    def test_full_run_includes_every_probe(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=False)
        names = self._names()
        for expected in (
            "DNS resolution",
            "TCP connectivity",
            "Authentication",
            "Channel creation",
            "Broker information",
            "Publisher confirms",
            "Connection not blocked",
            "Queue declare",
            "Publish",
            "Consume",
        ):
            self.assertIn(expected, names)

    def test_declares_only_ephemeral_queues(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=False)
        for name in self.broker.declared:
            self.assertTrue(
                name.startswith("amq.gen-") or name.startswith("mq-doctor-"),
                f"doctor left a queue behind: {name}",
            )

    def test_collects_broker_details(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=True)
        self.assertIn("product", self.report.broker)
        self.assertIn("capabilities", self.report.broker)

    def test_json_shape_of_a_check(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=True)
        payload = self.report.checks[0].to_dict()
        for key in ("name", "status", "detail", "duration_ms"):
            self.assertIn(key, payload)

    def test_a_failing_probe_is_recorded_not_raised(self):
        """One broken check must not abort the whole report."""
        from mq.services import diagnostic_service as service

        with fake_connection(self.broker):
            original = service.create_connection  # the fake, patched in above
            calls = {"n": 0}

            def flaky(config, blocked_state=None):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise RuntimeError("synthetic failure")
                return original(config, blocked_state=blocked_state)

            service.create_connection = flaky
            try:
                report = run_doctor(self.config, readonly=True)
            finally:
                service.create_connection = original

        failed = report.failures
        self.assertTrue(failed, "failures should be collected, not raised")
        details = " | ".join(c.detail for c in failed)
        self.assertIn("synthetic failure", details)
        # Everything after the failure still ran.
        self.assertGreater(len(report.checks), 5)

    def test_warning_severity_is_available_for_soft_failures(self):
        with fake_connection(self.broker):
            self.report = run_doctor(self.config, readonly=True)
        warnings = [c for c in self.report.checks if c.severity == "warning"]
        self.assertTrue(warnings)
        self.assertIn("DNS resolution", [c.name for c in warnings])


if __name__ == "__main__":
    unittest.main()

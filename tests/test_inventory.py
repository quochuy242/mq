"""The inventory store: what 'mq list' knows about, and what survives a round trip."""

from __future__ import annotations

import unittest

from mq import inventory
from tests.support import QuietTestCase, temp_config_dir


class InventoryStore(QuietTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._ctx = temp_config_dir()
        self._ctx.__enter__()

    def tearDown(self) -> None:
        self._ctx.__exit__(None, None, None)
        super().tearDown()

    def test_add_and_list(self):
        inventory.add("orders", "/", kind="queue")
        inventory.add("payments", "/", kind="queue")
        names = [e.name for e in inventory.list_entries("queue", "/")]
        self.assertEqual(["orders", "payments"], sorted(names))

    def test_add_is_idempotent(self):
        inventory.add("orders", "/", kind="queue")
        inventory.add("orders", "/", kind="queue")
        self.assertEqual(1, len(inventory.list_entries("queue", "/")))

    def test_add_preserves_existing_metadata(self):
        inventory.add("orders", "/", kind="queue", tags=["prod"], note="billing")
        inventory.add("orders", "/", kind="queue")
        entry = inventory.get("orders", "/", kind="queue")
        self.assertEqual(["prod"], entry.tags)
        self.assertEqual("billing", entry.note)

    def test_remove(self):
        inventory.add("orders", "/", kind="queue")
        self.assertTrue(inventory.remove("orders", "/", kind="queue"))
        self.assertFalse(inventory.remove("orders", "/", kind="queue"))
        self.assertEqual([], inventory.list_entries("queue", "/"))

    def test_queues_and_exchanges_coexist(self):
        """Regression: writing an exchange used to crash because the queue
        section still held raw dicts read from disk."""
        inventory.add("orders", "/", kind="queue")
        inventory.add("amq.topic", "/", kind="exchange")
        self.assertEqual(["orders"], [e.name for e in inventory.list_entries("queue", "/")])
        self.assertEqual(["amq.topic"], [e.name for e in inventory.list_entries("exchange", "/")])

    def test_vhost_scoping(self):
        inventory.add("orders", "/", kind="queue")
        inventory.add("orders", "/prod", kind="queue")
        self.assertEqual(1, len(inventory.list_entries("queue", "/")))
        self.assertEqual(1, len(inventory.list_entries("queue", "/prod")))

    def test_pattern_filter(self):
        inventory.add("orders", "/", kind="queue")
        inventory.add("orders.eu", "/", kind="queue")
        inventory.add("payments", "/", kind="queue")
        self.assertEqual(
            ["orders", "orders.eu"], inventory.queue_names("/", ["orders*"])
        )

    def test_default_exchange_is_always_listed(self):
        inventory.add("amq.topic", "/", kind="exchange")
        self.assertIn("", inventory.exchange_names("/"))

    def test_round_trip_through_yaml(self):
        inventory.add("orders", "/", kind="queue", tags=["a", "b"], note="note")
        entries = inventory.list_entries("queue", "/")
        entry = entries[0]
        self.assertEqual(["a", "b"], entry.tags)
        self.assertEqual("note", entry.note)
        self.assertIsNotNone(entry.last_seen)

    def test_touch_helpers_are_safe(self):
        inventory.touch_queue("", "/")  # empty name must be a no-op
        inventory.touch_exchange("", "/")
        self.assertEqual([], inventory.list_entries("queue", "/"))
        self.assertEqual([], inventory.list_entries("exchange", "/"))

    def test_corrupt_file_is_tolerated(self):
        inventory.path().parent.mkdir(parents=True, exist_ok=True)
        inventory.path().write_text("::: not yaml :::\n  - broken")
        self.assertEqual([], inventory.list_entries("queue", "/"))


if __name__ == "__main__":
    unittest.main()

"""End-to-end CLI checks with a fake broker injected.

These exercise what the unit tests skip: option names, JSON output shapes and
exit codes as a user actually meets them.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Iterator

from typer.testing import CliRunner

import mq.cli as cli_module
from mq.utils import set_json
from tests.fake_amqp import FakeBroker
from tests.support import clean_env, fake_connection, temp_config_dir

runner = CliRunner()


@contextlib.contextmanager
def tempfile_file(payload) -> Iterator[str]:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dump.json"
        path.write_text(json.dumps(payload))
        yield str(path)


@contextlib.contextmanager
def broker_env(broker: FakeBroker | None = None) -> Iterator[FakeBroker]:
    """A config in a temp dir, MQ_* cleared, optionally backed by a fake broker."""
    fake = broker if broker is not None else FakeBroker()
    with temp_config_dir(), clean_env():
        os.environ["MQ_URL"] = "amqp://mq:secret@localhost:5672/"
        set_json(False)
        if broker is not None:
            with fake_connection(fake):
                yield fake
        else:
            yield fake
    set_json(False)


def run(*args: str) -> tuple[int, str]:
    result = runner.invoke(cli_module.app, list(args))
    if result.exception and not isinstance(result.exception, SystemExit):
        raise result.exception
    return result.exit_code, result.output


class HelpAndDiscovery(unittest.TestCase):
    def test_every_command_has_help(self):
        commands = [
            "ping", "config", "declare", "delete", "purge", "stats", "publish",
            "get", "consume", "info", "info-queue", "verify-queue", "peek",
            "bind", "unbind", "whoami", "doctor", "move", "retry", "replay",
            "export", "import", "list", "top", "watch", "version",
        ]
        for name in commands:
            code, out = run(name, "--help")
            self.assertEqual(0, code, f"{name} --help failed")
            self.assertIn("Usage", out)

    def test_subgroups(self):
        for path in (["exchange", "--help"], ["inventory", "--help"], ["benchmark", "--help"]):
            code, out = run(*path)
            self.assertEqual(0, code, f"{' '.join(path)} failed")

    def test_version(self):
        code, out = run("version")
        self.assertEqual(0, code)
        self.assertIn("mq-cli", out)


class InventoryCommands(unittest.TestCase):
    """Inventory is local-only: it must work with no broker configured."""

    def test_add_list_remove(self):
        with broker_env():
            self.assertEqual(0, run("inventory", "add", "orders,payments")[0])
            self.assertEqual(0, run("inventory", "list")[0])

            code, out = run("--json", "inventory", "list")
            self.assertEqual(0, code)
            names = [e["name"] for e in json.loads(out)["entries"]]
            self.assertEqual(["orders", "payments"], names)

            self.assertEqual(0, run("inventory", "rm", "payments")[0])
            code, out = run("--json", "inventory", "list")
            self.assertEqual(["orders"], [e["name"] for e in json.loads(out)["entries"]])

    def test_exchange_inventory(self):
        with broker_env():
            run("inventory", "add", "amq.topic", "--exchange")
            code, out = run("--json", "inventory", "list", "--kind", "exchange")
            self.assertEqual(0, code)
            self.assertEqual(
                ["amq.topic"], [e["name"] for e in json.loads(out)["entries"]]
            )

    def test_empty_inventory_explains_itself(self):
        with broker_env():
            code, out = run("inventory", "list")
            self.assertEqual(0, code)
            self.assertIn("cannot enumerate", out)


class BrokerCommands(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = FakeBroker()

    def test_ping_reports_the_broker(self):
        with broker_env(self.broker):
            code, out = run("--json", "ping")
            self.assertEqual(0, code, out)
            payload = json.loads(out)
            self.assertTrue(payload["reachable"])
            self.assertEqual("localhost", payload["host"])

    def test_stats_reports_depth(self):
        self.broker.seed("orders", ["a", "b", "c"])
        with broker_env(self.broker):
            code, out = run("--json", "stats", "orders")
            self.assertEqual(0, code, out)
            self.assertEqual(3, json.loads(out)["ready"])

    def test_stats_on_a_missing_queue_exits_non_zero(self):
        with broker_env(self.broker):
            code, out = run("--json", "stats", "ghost")
            self.assertEqual(0, code, "JSON mode reports the error in the payload")
            self.assertEqual("missing", json.loads(out)["status"])

            code, out = run("stats", "ghost")
            self.assertEqual(1, code, "text mode exits non-zero")
            self.assertIn("Error", out)

    def test_list_reads_tracked_queues(self):
        with broker_env(self.broker):
            run("inventory", "add", "orders,payments,ghost")
            self.broker.seed("orders", ["a"])
            self.broker.declare("payments")

            code, out = run("--json", "list")
            self.assertEqual(0, code, out)
            rows = {r["queue"]: r for r in json.loads(out)}
            self.assertEqual({"orders", "payments", "ghost"}, set(rows))
            self.assertEqual("ok", rows["orders"]["status"])
            self.assertEqual(1, rows["orders"]["ready"])
            self.assertEqual("missing", rows["ghost"]["status"])

    def test_list_accepts_explicit_names_without_touching_the_inventory(self):
        self.broker.seed("orders", ["a"])
        with broker_env(self.broker):
            code, out = run("--json", "list", "--queues", "orders")
            self.assertEqual(0, code, out)
            self.assertEqual(["orders"], [r["queue"] for r in json.loads(out)])

            code, out = run("--json", "inventory", "list")
            self.assertEqual([], json.loads(out)["entries"])

    def test_top_sorts_by_depth(self):
        self.broker.seed("small", ["a"])
        self.broker.seed("big", ["a", "b", "c", "d"])
        with broker_env(self.broker):
            code, out = run("--json", "top", "--queues", "small,big", "--limit", "1")
            self.assertEqual(0, code, out)
            self.assertEqual(["big"], [r["queue"] for r in json.loads(out)])

    def test_top_rejects_an_unknown_sort_key(self):
        with broker_env(self.broker):
            code, out = run("top", "--sort-by", "colour")
            self.assertEqual(1, code)
            self.assertIn("sort-by", out)

    def test_watch_emits_json_samples(self):
        self.broker.seed("orders", ["a"])
        with broker_env(self.broker):
            code, out = run("--json", "watch", "orders", "--count", "2", "--interval", "0")
            self.assertEqual(0, code, out)
            lines = [line for line in out.strip().splitlines() if line]
            self.assertEqual(2, len(lines))
            self.assertEqual("orders", json.loads(lines[0])["queue"])

    def test_publish_confirms_and_records_the_inventory(self):
        self.broker.declare("orders")
        with broker_env(self.broker):
            code, out = run("--json", "publish", "orders", "--body", "hello", "--mandatory")
            self.assertEqual(0, code, out)
            self.assertTrue(json.loads(out)["confirmed"])

            code, out = run("--json", "inventory", "list")
            self.assertEqual(["orders"], [e["name"] for e in json.loads(out)["entries"]])

    def test_publish_to_a_bound_exchange(self):
        self.broker.declare("orders")
        self.broker.exchange_declare("events", exchange_type="topic")
        self.broker.queue_bind("orders", "events", "order.created")
        with broker_env(self.broker):
            code, out = run(
                "--json", "publish", "order.created",
                "--exchange", "events", "--body", "{}", "--mandatory",
            )
            self.assertEqual(0, code, out)
            self.assertEqual("events", json.loads(out)["exchange"])

    def test_publish_mandatory_detects_an_unroutable_key(self):
        self.broker.declare("orders")
        with broker_env(self.broker):
            code, out = run(
                "--json", "publish", "nowhere", "--body", "x", "--mandatory"
            )
            self.assertEqual(1, code)
            self.assertIn("unroutable", json.dumps(json.loads(out)))

    def test_peek_does_not_consume(self):
        self.broker.seed("orders", ["a", "b"])
        with broker_env(self.broker):
            code, out = run("--json", "peek", "orders", "--count", "2")
            self.assertEqual(0, code, out)
            self.assertEqual(["a", "b"], [m["body"] for m in json.loads(out)])
        self.assertEqual(2, len(self.broker.queues["orders"]))

    def test_declare_with_arguments(self):
        with broker_env(self.broker):
            code, out = run(
                "--json", "declare", "orders", "--durable", "--queue-type", "quorum"
            )
            self.assertEqual(0, code, out)
            payload = json.loads(out)
            self.assertEqual("orders", payload["queue"])
            self.assertEqual({"x-queue-type": "quorum"}, payload["arguments"])

    def test_declare_rejects_a_malformed_argument(self):
        with broker_env(self.broker):
            code, out = run("declare", "orders", "--arg", "nonsense")
            self.assertEqual(1, code)
            self.assertIn("key=value", out)

    def test_get_rejects_contradictory_flags(self):
        with broker_env(self.broker):
            code, out = run("get", "orders", "--ack", "--no-requeue")
            self.assertEqual(1, code)
            self.assertIn("cannot be combined", out)

    def test_move_moves_and_empties(self):
        self.broker.seed("src", ["a", "b"])
        self.broker.declare("dst")
        with broker_env(self.broker):
            code, out = run("--json", "move", "src", "dst", "--count", "-1")
            self.assertEqual(0, code, out)
            self.assertEqual(2, json.loads(out)["moved"])
        self.assertEqual([], self.broker.queues["src"])
        self.assertEqual(2, len(self.broker.queues["dst"]))

    def test_move_refuses_the_same_queue_twice(self):
        with broker_env(self.broker):
            code, out = run("move", "orders", "orders")
            self.assertEqual(1, code)
            self.assertIn("same queue", out)

    def test_export_leaves_the_queue_intact(self):
        self.broker.seed("orders", ["a", "b"])
        with broker_env(self.broker):
            code, out = run("--json", "export", "orders")
            self.assertEqual(0, code, out)
            self.assertEqual(2, len(json.loads(out)["messages"]))
        self.assertEqual(2, len(self.broker.queues["orders"]))

    def test_export_then_import_round_trips(self):
        self.broker.seed("orders", ["a", "b"])
        with broker_env(self.broker) as fake:
            code, out = run("--json", "export", "orders")
            exported = json.loads(out)
            self.broker.declare("restored")
            fake.published.clear()

            with tempfile_file(exported) as path:
                code, out = run("--json", "import", "restored", path)
                self.assertEqual(0, code, out)
                self.assertEqual(2, json.loads(out)["imported"])

            self.assertEqual(
                [b"a", b"b"], [p["body"] for p in fake.published]
            )

    def test_verify_queue_reports_drift_with_a_non_zero_exit(self):
        self.broker.declare("orders", durable=False)
        with broker_env(self.broker):
            code, out = run("--json", "verify-queue", "orders", "--expect-durable")
            self.assertEqual(1, code)
            self.assertTrue(json.loads(out)["drift"])

    def test_verify_queue_passes_when_the_declaration_matches(self):
        self.broker.declare("orders", durable=True)
        with broker_env(self.broker):
            code, out = run("--json", "verify-queue", "orders", "--expect-durable")
            self.assertEqual(0, code, out)
            self.assertFalse(json.loads(out)["drift"])

    def test_verify_queue_needs_something_to_check(self):
        self.broker.declare("orders")
        with broker_env(self.broker):
            code, out = run("verify-queue", "orders")
            self.assertEqual(1, code)
            self.assertIn("--arg", out)

    def test_whoami_against_a_read_only_queue(self):
        self.broker.seed("orders", ["a"])
        with broker_env(self.broker):
            code, out = run("--json", "whoami", "--queue", "orders")
            self.assertEqual(0, code, out)
            self.assertTrue(json.loads(out)["permissions"]["read"])

    def test_doctor_status_reports_a_health_verdict(self):
        """--status must give monitoring a verdict and a matching exit code.

        The DNS and TCP probes in the report are real socket calls and no broker
        is listening in the test environment, so the verdict here is 'unhealthy'
        -- which is exactly what the exit code has to agree with.
        """
        with broker_env(self.broker):
            code, out = run("--json", "doctor", "--status")
            payload = json.loads(out)
            self.assertFalse(payload["healthy"])
            self.assertTrue(payload["failures"])
            self.assertEqual(1, code, "unhealthy must exit non-zero")

            code, out = run("doctor", "--status")
            self.assertEqual(1, code)
            self.assertIn("TCP connectivity", out)

    def test_doctor_runs_every_check(self):
        with broker_env(self.broker):
            code, out = run("--json", "doctor", "--readonly")
            self.assertEqual(0, code, out)
            payload = json.loads(out)
            names = [c["name"] for c in payload["checks"]]
            for expected in ("Broker information", "Connection not blocked"):
                self.assertIn(expected, names)
            self.assertNotIn("Publish", names, "--readonly skips the writes")

    def test_errors_are_json_in_json_mode(self):
        with broker_env(self.broker):
            code, out = run("--json", "stats", "ghost")
            payload = json.loads(out)
            self.assertEqual("missing", payload["status"])
            self.assertIsNotNone(payload["error"])


@contextlib.contextmanager
def config_env() -> Iterator[None]:
    """No MQ_URL: config-file precedence cannot be observed through an env URL."""
    with temp_config_dir(), clean_env():
        set_json(False)
        yield
    set_json(False)


class ConfigCommands(unittest.TestCase):
    def test_show_reports_every_setting(self):
        with config_env():
            run("config", "--host", "h", "--username", "u", "--password", "pw")
            code, out = run("--json", "config", "--show")
            self.assertEqual(0, code, out)
            payload = json.loads(out)
            for key in (
                "host", "port", "username", "vhost", "tls", "heartbeat",
                "connection_timeout", "blocked_connection_timeout", "auth",
                "management_port", "inventory_file",
            ):
                self.assertIn(key, payload)

    def test_show_reports_an_incomplete_config_without_crashing(self):
        with config_env():
            code, out = run("--json", "config", "--show")
            self.assertEqual(0, code)
            self.assertIn("error", json.loads(out))
            self.assertIn("host", json.loads(out)["error"])

    def test_password_is_never_printed(self):
        with config_env():
            run("config", "--host", "h", "--username", "u", "--password", "hunter2")
            code, out = run("config", "--show")
            self.assertEqual(0, code)
            self.assertNotIn("hunter2", out)
            self.assertIn("****", out)

    def test_write_then_read(self):
        with config_env():
            code, _ = run(
                "config", "--host", "rabbit.example", "--username", "ops",
                "--password", "pw", "--vhost", "/prod", "--ssl",
            )
            self.assertEqual(0, code)
            code, out = run("--json", "config", "--show")
            payload = json.loads(out)
            self.assertEqual("rabbit.example", payload["host"])
            self.assertTrue(payload["tls"])

    def test_incomplete_write_is_refused(self):
        with config_env():
            run("config", "--host", "h", "--username", "u", "--password", "pw")
            code, out = run("config", "--unset", "host")
            self.assertEqual(1, code)
            self.assertIn("incomplete", out)

    def test_unset_removes_a_setting(self):
        with config_env():
            run("config", "--host", "h", "--username", "u", "--password", "pw",
                "--heartbeat", "90")
            self.assertEqual(0, run("config", "--unset", "heartbeat")[0])
            code, out = run("--json", "config", "--show")
            self.assertNotEqual(90, json.loads(out)["heartbeat"])

    def test_unset_rejects_an_unknown_key(self):
        with config_env():
            run("config", "--host", "h", "--username", "u", "--password", "pw")
            code, out = run("config", "--unset", "nonsense")
            self.assertEqual(1, code)
            self.assertIn("nonsense", out)

    def test_profile_creation_and_switch(self):
        with config_env():
            code, _ = run(
                "config", "--profile", "staging", "--host", "rabbit.staging",
                "--username", "ops", "--password", "pw",
            )
            self.assertEqual(0, code)
            code, out = run("--json", "--profile", "staging", "config", "--show")
            self.assertEqual(0, code)
            self.assertEqual("rabbit.staging", json.loads(out)["host"])

    def test_set_active_persists_the_profile_choice(self):
        with config_env():
            run("config", "--profile", "prod", "--host", "rabbit.prod",
                "--username", "ops", "--password", "pw", "--set-active")
            code, out = run("--json", "config", "--show")
            self.assertEqual("rabbit.prod", json.loads(out)["host"])

    def test_rejects_an_unknown_auth_mechanism(self):
        with config_env():
            code, out = run("config", "--auth", "kerberos")
            self.assertEqual(1, code)
            self.assertIn("auth", out)


if __name__ == "__main__":
    unittest.main()

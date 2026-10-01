"""Broker reply codes must become messages an operator can act on.

The old code collapsed every ChannelClosedByBroker into "Channel error: ...",
so "you lack permission" and "that queue does not exist" were indistinguishable.
"""

from __future__ import annotations

import unittest

from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from mq.errors import (
    classify,
    describe,
    is_access_refused,
    is_not_found,
    reply_code_of,
    to_json_dict,
)
from tests.support import QuietTestCase


def closed(code: int, text: str) -> ChannelClosedByBroker:
    return ChannelClosedByBroker(code, text)


class ChannelReplies(QuietTestCase):
    def test_403_is_a_permission_problem(self):
        info = classify(closed(403, "ACCESS_REFUSED - access to queue 'orders' refused"))
        self.assertEqual("access-refused", info.kind)
        self.assertEqual(403, info.code)
        self.assertIn("permitted", info.detail.lower())

    def test_404_is_a_missing_object(self):
        info = classify(closed(404, "NOT_FOUND - no queue 'orders' in vhost '/'"))
        self.assertEqual("not-found", info.kind)
        self.assertEqual(404, info.code)

    def test_404_mentions_that_permissions_look_the_same(self):
        info = classify(closed(404, "NOT_FOUND - no queue 'orders'"))
        self.assertIn("404", info.hint or "")

    def test_406_is_a_declaration_mismatch(self):
        info = classify(closed(406, "PRECONDITION_FAILED - inequivalent arg 'durable'"))
        self.assertEqual("precondition-failed", info.kind)
        self.assertIn("different arguments", info.hint or "")

    def test_405_is_a_lock(self):
        info = classify(closed(405, "RESOURCE_LOCKED - cannot obtain exclusive access"))
        self.assertEqual("resource-locked", info.kind)

    def test_530_is_vhost_or_access(self):
        info = classify(closed(530, "NOT_ALLOWED - vhost /prod not found"))
        self.assertEqual("not-allowed", info.kind)

    def test_413_overloaded_as_quota(self):
        info = classify(closed(413, "PRECONDITION_FAILED - memory quota exceeded"))
        self.assertEqual("quota-exceeded", info.kind)
        self.assertIn("alarm", info.hint or "")

    def test_320_is_a_forced_close(self):
        info = classify(ConnectionClosedByBroker(320, "CONNECTION_FORCED - broker shutdown"))
        self.assertEqual("connection-forced", info.kind)
        self.assertTrue(info.fatal)

    def test_predicates(self):
        self.assertTrue(is_not_found(closed(404, "NOT_FOUND")))
        self.assertFalse(is_not_found(closed(403, "ACCESS_REFUSED")))
        self.assertTrue(is_access_refused(closed(403, "ACCESS_REFUSED")))
        # An exception with no reply code is simply "not access refused".
        self.assertFalse(is_access_refused(RuntimeError("boom")))
        self.assertIsNone(reply_code_of(RuntimeError("boom")))


class ConnectionFailures(QuietTestCase):
    def test_authentication(self):
        info = classify(ProbableAuthenticationError(closed(403, "ACCESS_REFUSED")))
        self.assertEqual("auth-failed", info.kind)
        self.assertIn("--username", info.hint or "")

    def test_vhost_access_denied(self):
        info = classify(ProbableAccessDeniedError(closed(530, "NOT_ALLOWED")))
        self.assertEqual("auth-failed", info.kind)
        self.assertIn("vhost", (info.hint or "").lower())

    def test_connection_error_is_fatal(self):
        info = classify(AMQPConnectionError("Connection refused"))
        self.assertEqual("connection-failed", info.kind)
        self.assertTrue(info.fatal)

    def test_unknown_exception_falls_back_to_internal(self):
        info = classify(ValueError("something odd"))
        self.assertEqual("internal", info.kind)
        self.assertEqual("something odd", info.detail)


class Rendering(QuietTestCase):
    def test_describe_includes_context_and_hint(self):
        text = describe(closed(403, "ACCESS_REFUSED"), "Cannot purge 'orders'")
        self.assertIn("Cannot purge 'orders'", text)
        self.assertIn("->", text)

    def test_json_shape_is_stable(self):
        payload = to_json_dict(closed(404, "NOT_FOUND - no queue"))
        self.assertEqual(
            sorted(payload.keys()),
            ["detail", "error", "exception", "fatal", "hint", "reply_code"],
        )
        self.assertEqual(404, payload["reply_code"])


if __name__ == "__main__":
    unittest.main()

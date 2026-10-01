"""Output formatting and the small helpers commands rely on."""

from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout

from mq.services.message_service import decode_body, properties_from_dict, properties_to_dict
from mq.utils import jq_filter, parse_header_filters, pretty_print_json
from mq.utils.output import print_json, print_pairs, print_table, set_color
from tests.support import QuietTestCase


class Capture(QuietTestCase):
    """QuietTestCase already swallows output; this exposes it for assertions."""

    def render(self, func, *args, **kwargs) -> str:
        func(*args, **kwargs)
        return self.output


class Tables(Capture):
    def tearDown(self) -> None:
        set_color(None)
        super().tearDown()

    def test_json_output_is_a_flat_list_of_objects(self):
        out = self.render(
            print_table,
            ["QUEUE", "READY"],
            [["orders", "5"]],
            json_output=True,
            title="Queues",
        )
        payload = json.loads(out)
        # A flat array, not nested under the title, so jq pipelines stay simple.
        self.assertEqual([{"QUEUE": "orders", "READY": "5"}], payload)

    def test_text_output_includes_headers_and_rows(self):
        out = self.render(print_table, ["A", "B"], [["1", "2"]])
        self.assertIn("A", out)
        self.assertIn("1  2", out)

    def test_empty_rows_are_marked_not_silently_blank(self):
        out = self.render(print_table, ["A"], [])
        self.assertIn("no rows", out)

    def test_long_cells_are_truncated_to_the_terminal(self):
        rows = [["x" * 400, "y"]]
        out = self.render(
            print_table, ["A", "B"], rows, max_width=60
        )
        longest = max(len(line) for line in out.splitlines())
        self.assertLessEqual(longest, 62)
        self.assertIn("…", out)

    def test_pairs_render_aligned(self):
        out = self.render(print_pairs, [("Host", "h"), ("VHost", "/")])
        self.assertIn("Host:", out)
        self.assertIn("VHost:", out)

    def test_pairs_as_json(self):
        out = self.render(print_pairs, [("Host", "h")], json_output=True)
        self.assertEqual({"Host": "h"}, json.loads(out))

    def test_print_json_handles_unserialisable_values(self):
        out = self.render(print_json, {"path": io.StringIO("x")})
        self.assertIn("path", json.loads(out))


class Colour(Capture):
    def tearDown(self) -> None:
        set_color(None)
        super().tearDown()

    def test_no_colour_when_disabled(self):
        set_color(False)
        out = self.render(print_pairs, [("Host", "h")])
        self.assertNotIn("\033[", out)

    def test_colour_when_enabled(self):
        set_color(True)
        out = self.render(print_pairs, [("Host", "h")])
        self.assertIn("\033[", out)


class JqSubset(Capture):
    def test_dotted_path(self):
        self.assertEqual("42", jq_filter('{"a":{"b":42}}', ".a.b"))

    def test_array_index(self):
        self.assertEqual("2", jq_filter('{"a":[1,2,3]}', ".a.1"))

    def test_whole_document(self):
        # Rendered indented, so compare parsed content rather than exact bytes.
        self.assertEqual({"a": 1}, json.loads(jq_filter('{"a":1}', ".")))

    def test_missing_path_is_empty_not_a_crash(self):
        self.assertEqual("", jq_filter('{"a":1}', ".nope.deeper"))

    def test_non_json_passes_through(self):
        self.assertEqual("hello", jq_filter("hello", ".a"))

    def test_empty_string_field_is_preserved(self):
        """The old loop used '' as a 'missing' sentinel and stopped early."""
        self.assertEqual("", jq_filter('{"a":"","b":2}', ".a"))
        self.assertEqual("2", jq_filter('{"a":"","b":2}', ".b"))

    def test_null_renders_as_null(self):
        self.assertEqual("null", jq_filter('{"a":null}', ".a"))

    def test_bytes_input(self):
        self.assertEqual("1", jq_filter(b'{"a":1}', ".a"))


class HeaderParsing(Capture):
    def test_keeps_values_containing_equals(self):
        self.assertEqual({"k": "a=b"}, parse_header_filters(["k=a=b"]))

    def test_skips_malformed_filters(self):
        self.assertEqual({}, parse_header_filters(["broken"]))

    def test_none_is_empty(self):
        self.assertEqual({}, parse_header_filters(None))


class Properties(Capture):
    def test_round_trip_preserves_set_fields_only(self):
        import pika

        original = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            headers={"x-retry-count": 2},
            correlation_id="abc",
        )
        data = properties_to_dict(original)
        self.assertEqual("application/json", data["content_type"])
        self.assertNotIn("expiration", data)

        restored = properties_from_dict(data)
        self.assertEqual("application/json", restored.content_type)
        self.assertEqual({"x-retry-count": 2}, restored.headers)
        self.assertEqual(2, restored.delivery_mode)

    def test_none_properties(self):
        self.assertEqual({}, properties_to_dict(None))


class BodyDecoding(Capture):
    def test_json(self):
        self.assertEqual({"a": 1}, decode_body(b'{"a":1}'))

    def test_utf8(self):
        self.assertEqual("héllo", decode_body("héllo".encode()))

    def test_binary_falls_back_to_hex(self):
        self.assertEqual("00ff", decode_body(b"\x00\xff"))

    def test_empty(self):
        self.assertEqual("", decode_body(b""))

    def test_pretty_print_leaves_non_json_alone(self):
        self.assertEqual("plain text", pretty_print_json("plain text"))


if __name__ == "__main__":
    unittest.main()

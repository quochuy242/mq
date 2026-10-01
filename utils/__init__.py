from mq.utils.helpers import (
    handle_error,
    is_json,
    jq_filter,
    parse_header_filters,
    parse_headers,
    pretty_print_json,
    pretty_print_message,
    properties_to_dict,
    report,
    set_debug,
)
from mq.utils.output import (
    accent,
    dim,
    fail,
    header,
    ok,
    print_json,
    print_pairs,
    print_table,
    set_color,
    status_marker,
    warn,
)

_json: bool = False
_trace: bool = False


def set_json(enabled: bool) -> None:
    global _json
    _json = enabled


def is_json_mode() -> bool:
    return _json


def set_trace(enabled: bool) -> None:
    global _trace
    _trace = enabled


def is_trace() -> bool:
    return _trace


def echo_json(data: object) -> None:
    """Print a JSON document. In text mode it is still valid JSON, by design."""
    print_json(data)

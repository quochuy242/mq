from mq.utils.helpers import (
    _debug,
    handle_error,
    is_json,
    parse_headers,
    pretty_print_json,
    pretty_print_message,
    set_debug,
)
from mq.utils.output import print_json, print_table

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

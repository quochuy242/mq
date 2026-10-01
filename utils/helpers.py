from __future__ import annotations

import json
import sys
import traceback
from typing import Any

import orjson

from mq.errors import classify, to_json_dict

_debug: bool = False


def set_debug(enabled: bool) -> None:
    global _debug
    _debug = enabled


def is_json(content_type: str | None, body: bytes) -> bool:
    if content_type:
        ct = content_type.lower()
        if "json" in ct:
            return True
    try:
        orjson.loads(body)
        return True
    except (orjson.JSONDecodeError, ValueError, TypeError):
        return False


def parse_headers(headers: list[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for header in headers or []:
        if "=" not in header:
            print(
                f"Warning: invalid header format '{header}', expected key=value",
                file=sys.stderr,
            )
            continue
        key, _, value = header.partition("=")
        result[key.strip()] = value.strip()
    return result


def parse_header_filters(filters: list[str] | None) -> dict[str, str]:
    """Parse ``key=value`` filters, keeping values that contain '=' intact."""
    result: dict[str, str] = {}
    for item in filters or []:
        if "=" not in item:
            continue
        key, _, value = item.partition("=")
        result[key.strip()] = value.strip()
    return result


def pretty_print_json(data: str | bytes) -> str:
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    try:
        obj = orjson.loads(data)
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode()
    except (orjson.JSONDecodeError, ValueError, TypeError):
        return data


def properties_to_dict(properties: Any) -> dict[str, Any]:
    """Serialise AMQP basic properties, keeping only the ones that are set."""
    from mq.services.message_service import PROPERTY_FIELDS

    if properties is None:
        return {}
    return {
        name: getattr(properties, name, None)
        for name in PROPERTY_FIELDS
        if getattr(properties, name, None) is not None
    }


def pretty_print_message(
    body: bytes,
    properties: Any = None,
    headers: dict[str, str] | None = None,
    as_json: bool = False,
) -> None:
    if as_json:
        msg: dict[str, Any] = {}
        if headers:
            msg["headers"] = headers
        props = properties_to_dict(properties)
        if props:
            msg["properties"] = props
        try:
            msg["body"] = orjson.loads(body)
        except (orjson.JSONDecodeError, ValueError):
            msg["body"] = body.decode("utf-8", errors="replace")
        print(orjson.dumps(msg, option=orjson.OPT_INDENT_2).decode())
        return

    if headers:
        print("Headers:")
        for k, v in headers.items():
            print(f"  {k}: {v}")

    props = properties_to_dict(properties)
    if props:
        print("Properties:")
        for key, value in props.items():
            print(f"  {key}: {value}")

    print("Body:")
    try:
        parsed = orjson.loads(body)
        print(orjson.dumps(parsed, option=orjson.OPT_INDENT_2).decode())
    except (orjson.JSONDecodeError, ValueError):
        try:
            print(body.decode("utf-8"))
        except UnicodeDecodeError:
            print(repr(body))


def jq_filter(data: Any, expression: str) -> str:
    """A deliberately small subset of jq: dotted paths and array indices.

    This is not jq. It handles ``.a.b``, ``.a.0.b`` and ``.`` -- enough to pull
    an id out of a payload in a shell pipeline, which is what it is used for.
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    if isinstance(data, str):
        try:
            obj = orjson.loads(data)
        except (orjson.JSONDecodeError, ValueError):
            return data
    else:
        obj = data

    if not expression or expression == ".":
        return _dump(obj)

    parts = [p for p in expression.lstrip(".").split(".") if p]
    current: Any = obj
    missing = False
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                missing = True
                break
            current = current[part]
        elif isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                missing = True
                break
            if index >= len(current) or index < -len(current):
                missing = True
                break
            current = current[index]
        else:
            missing = True
            break

    if missing:
        return ""
    return _dump(current)


def _dump(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return orjson.dumps(value, option=orjson.OPT_INDENT_2).decode()
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def handle_error(msg: str, exc: Exception | None = None, exit_code: int = 1) -> None:
    """Print an error and exit.

    In ``--json`` mode the failure is emitted as a JSON object on stdout so a
    failing command is still machine-readable, instead of a bare sentence on
    stderr that scripts cannot parse.
    """
    from mq.utils import is_json_mode

    if is_json_mode():
        payload: dict[str, Any] = {"error": "error", "detail": msg}
        if exc is not None:
            payload = to_json_dict(exc)
            payload["context"] = msg
        print(json.dumps(payload, indent=2, default=str), file=sys.stdout)
        sys.exit(exit_code)

    if _debug and exc:
        print(f"Error: {msg}", file=sys.stderr)
        traceback.print_exc()
    elif exc:
        info = classify(exc)
        print(f"Error: {msg}", file=sys.stderr)
        if info.detail and info.detail != msg:
            print(f"  {info.detail}", file=sys.stderr)
        if info.hint:
            print(f"  Hint: {info.hint}", file=sys.stderr)
    else:
        print(f"Error: {msg}", file=sys.stderr)
    sys.exit(exit_code)


def report(exc: BaseException, context: str | None = None) -> None:
    """Exit with a classified message for ``exc``."""
    handle_error(context or classify(exc).detail, exc)


__all__ = [
    "handle_error",
    "is_json",
    "jq_filter",
    "parse_header_filters",
    "parse_headers",
    "pretty_print_json",
    "pretty_print_message",
    "properties_to_dict",
    "report",
    "set_debug",
]

from __future__ import annotations

import json
import sys
import traceback
from typing import Any

import orjson

_debug: bool = False


def set_debug(enabled: bool) -> None:
    global _debug
    _debug = enabled


def is_json(content_type: str | None, body: bytes) -> bool:
    if content_type:
        ct = content_type.lower()
        if ct in ("application/json", "text/json", "application/x-json"):
            return True
        if "json" in ct:
            return True
    try:
        orjson.loads(body)
        return True
    except (orjson.JSONDecodeError, ValueError, TypeError):
        return False


def parse_headers(headers: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for header in headers:
        if "=" not in header:
            print(
                f"Warning: invalid header format '{header}', expected key=value",
                file=sys.stderr,
            )
            continue
        key, _, value = header.partition("=")
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
        if properties:
            props: dict[str, Any] = {}
            for attr in dir(properties):
                if not attr.startswith("_") and not callable(
                    getattr(properties, attr)
                ):
                    val = getattr(properties, attr)
                    if val is not None:
                        props[attr] = val
            msg["properties"] = props
        try:
            msg["body"] = orjson.loads(body)
        except orjson.JSONDecodeError:
            msg["body"] = body.decode("utf-8", errors="replace")
        print(orjson.dumps(msg, option=orjson.OPT_INDENT_2).decode())
        return

    if headers:
        print("Headers:")
        for k, v in headers.items():
            print(f"  {k}: {v}")

    if properties:
        print("Properties:")
        for attr in dir(properties):
            if not attr.startswith("_") and not callable(
                getattr(properties, attr)
            ):
                val = getattr(properties, attr)
                if val is not None:
                    print(f"  {attr}: {val}")

    print("Body:")
    try:
        parsed = orjson.loads(body)
        print(orjson.dumps(parsed, option=orjson.OPT_INDENT_2).decode())
    except orjson.JSONDecodeError:
        try:
            print(body.decode("utf-8"))
        except UnicodeDecodeError:
            print(repr(body))


def jq_filter(data: str | dict | list, expression: str) -> str:
    if isinstance(data, (str, bytes)):
        import orjson

        try:
            obj = orjson.loads(data)
        except orjson.JSONDecodeError:
            return str(data)
    else:
        obj = data

    if not expression or expression == ".":
        import orjson

        return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode()

    parts = expression.lstrip(".").split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part, "")
        elif isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
            except (ValueError, IndexError):
                current = ""
        else:
            current = ""
        if current == "":
            break

    import orjson

    if isinstance(current, (dict, list)):
        return orjson.dumps(current, option=orjson.OPT_INDENT_2).decode()
    return str(current)


def handle_error(
    msg: str, exc: Exception | None = None, exit_code: int = 1
) -> None:
    from mq.utils import is_trace

    if _debug and exc:
        print(f"Error: {msg}", file=sys.stderr)
        traceback.print_exc()
    elif exc:
        exc_msg = str(exc)
        cleaned = exc_msg.split("\n")[0] if exc_msg else ""
        print(f"Error: {msg}", file=sys.stderr)
        if cleaned:
            print(f"  {cleaned}", file=sys.stderr)
    else:
        print(f"Error: {msg}", file=sys.stderr)
    sys.exit(exit_code)

from __future__ import annotations

import json
import sys
from typing import Any


GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"


def _supports_color() -> bool:
    return sys.stdout.isatty()


def colorize(text: str, color: str) -> str:
    if _supports_color():
        return f"{color}{text}{RESET}"
    return text


def ok(text: str) -> str:
    return colorize(f"{text}", GREEN)


def fail(text: str) -> str:
    return colorize(f"{text}", RED)


def warn(text: str) -> str:
    return colorize(f"{text}", YELLOW)


def header(text: str) -> str:
    return colorize(f"{text}", BOLD)


def dim(text: str) -> str:
    return colorize(f"{text}", DIM)


def print_table(
    headers: list[str],
    rows: list[list[str]],
    json_output: bool = False,
    title: str | None = None,
) -> None:
    if json_output:
        data = [dict(zip(headers, row)) for row in rows]
        obj: dict[str, Any] = {}
        if title:
            obj[title] = data
        else:
            obj = {"data": data}
        print(json.dumps(obj, indent=2, default=str))
        return

    if title:
        print(f"{header(title)}:")

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    sep = "  "
    hdr = sep.join(h.ljust(w) for h, w in zip(headers, col_widths))
    print(f"  {header(hdr)}")
    print(f"  {dim('-' * len(hdr))}")
    for row in rows:
        line = sep.join(str(c).ljust(w) for c, w in zip(row, col_widths))
        print(f"  {line}")


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str))

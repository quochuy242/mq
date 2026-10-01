from __future__ import annotations

import json
import os
import shutil
import sys
from typing import Any

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"

_color_enabled: bool | None = None


def set_color(enabled: bool | None) -> None:
    """Force colour on or off. ``None`` restores auto-detection."""
    global _color_enabled
    _color_enabled = enabled


def _supports_color() -> bool:
    if _color_enabled is not None:
        return _color_enabled
    if os.environ.get("NO_COLOR") is not None:
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    if os.environ.get("MQ_FORCE_COLOR"):
        return True
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


def accent(text: str) -> str:
    return colorize(f"{text}", MAGENTA)


def dim(text: str) -> str:
    return colorize(f"{text}", DIM)


def terminal_width(default: int = 100) -> int:
    try:
        return shutil.get_terminal_size((default, 24)).columns
    except OSError:
        return default


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def print_table(
    headers: list[str],
    rows: list[list[str]],
    json_output: bool = False,
    title: str | None = None,
    max_width: int | None = None,
) -> None:
    """Render an aligned table, or a flat JSON array in ``--json`` mode.

    The JSON shape is deliberately a plain list of objects rather than nesting
    under a title key: ``jq`` and shell pipelines should not have to know what
    the command is called.
    """
    if json_output:
        print(json.dumps([dict(zip(headers, row)) for row in rows], indent=2, default=str))
        return

    if not rows:
        print(dim("  (no rows)"))
        return

    if title:
        print(f"{header(title)}:")

    columns = len(headers)
    widths = [len(h) for h in headers]
    for row in rows:
        for i in range(columns):
            cell = str(row[i]) if i < len(row) else ""
            widths[i] = max(widths[i], len(cell))

    if max_width:
        widths = _fit_widths(widths, max_width)

    sep = "  "
    rendered_headers = [_truncate(h, widths[i]).ljust(widths[i]) for i, h in enumerate(headers)]
    header_line = sep.join(rendered_headers).rstrip()
    print(f"  {header(header_line)}")
    print(f"  {dim('-' * len(header_line))}")
    for row in rows:
        cells = []
        for i in range(columns):
            cell = str(row[i]) if i < len(row) else ""
            cells.append(_truncate(cell, widths[i]).ljust(widths[i]))
        print(f"  {sep.join(cells).rstrip()}")


def _fit_widths(widths: list[int], max_width: int) -> list[int]:
    """Shrink the widest column until the table fits the terminal."""
    widths = list(widths)
    while sum(widths) + 2 * (len(widths) - 1) + 2 > max_width and max(widths) > 12:
        widths[widths.index(max(widths))] -= 1
    return widths


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str))


def print_json_line(data: Any) -> None:
    """One compact JSON object per line, for streaming output.

    ``mq watch --json`` runs indefinitely; a multi-line document per sample
    would be unreadable and unpipeable. This is JSON Lines.
    """
    print(json.dumps(data, separators=(",", ":"), default=str))


def print_pairs(
    pairs: list[tuple[str, Any]],
    json_output: bool = False,
    title: str | None = None,
    indent: str = "  ",
) -> None:
    """Key/value output, as a table in text mode and an object in JSON mode."""
    if json_output:
        print_json({str(key): value for key, value in pairs})
        return
    if title:
        print(f"{header(title)}:")
    if not pairs:
        print(dim(f"{indent}(nothing to show)"))
        return
    width = max(len(str(key)) for key, _ in pairs)
    for key, value in pairs:
        label = dim(f"{str(key)}:".ljust(width + 1))
        text = "" if value is None else str(value)
        if "\n" in text:
            first, *rest = text.split("\n")
            print(f"{indent}{label} {first}")
            for line in rest:
                print(f"{indent}{' ' * (width + 2)}{line}")
        else:
            print(f"{indent}{label} {text}")


def status_marker(status: str) -> str:
    if status == "ok":
        return ok("ok")
    if status == "forbidden":
        return warn("forbidden")
    if status == "missing":
        return fail("missing")
    return warn(status)

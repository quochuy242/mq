"""A local, client-side inventory of the queues and exchanges a user cares about.

RabbitMQ's AMQP 0-9-1 protocol has no way to enumerate queues, exchanges or
bindings -- there is simply no such method, and the management plugin is the
only thing that adds one. So ``mq list`` used to require port 15672.

This module closes the gap the only way it can be closed without the plugin:
remember the names the client already knows about, then verify each one with a
*passive declare*, which is a legal AMQP round trip that reports the live
message and consumer counts.

The inventory is a convenience cache, not a source of truth. It is populated
from three places:

* explicitly, via ``mq inventory add``;
* automatically, whenever a command touches a queue or exchange;
* by hand, since the file is plain YAML and safe to edit.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

from mq import config as config_module

INVENTORY_FILE = config_module.CONFIG_DIR / "inventory.yaml"

_EMPTY: dict[str, Any] = {"queues": [], "exchanges": []}


def _inventory_file() -> Path:
    """The file's directory, read from the config module each time.

    Keeping the directory and the filename derived from one source means a
    relocated config directory cannot leave the inventory behind elsewhere.
    """
    return config_module.CONFIG_DIR / INVENTORY_FILE.name


@dataclass
class InventoryEntry:
    name: str
    vhost: str = "/"
    kind: str = "queue"
    note: str | None = None
    tags: list[str] = field(default_factory=list)
    last_seen: float | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"name": self.name, "vhost": self.vhost, "kind": self.kind}
        if self.note:
            data["note"] = self.note
        if self.tags:
            data["tags"] = sorted(self.tags)
        if self.last_seen is not None:
            data["last_seen"] = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.localtime(self.last_seen)
            )
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "InventoryEntry":
        seen = data.get("last_seen")
        try:
            last_seen: float | None = time.mktime(time.strptime(seen, "%Y-%m-%dT%H:%M:%S"))
        except (TypeError, ValueError):
            last_seen = None
        return InventoryEntry(
            name=str(data.get("name", "")),
            vhost=str(data.get("vhost", "/")),
            kind=str(data.get("kind", "queue")),
            note=data.get("note"),
            tags=list(data.get("tags") or []),
            last_seen=last_seen,
        )


def _key(name: str, vhost: str) -> tuple[str, str]:
    return (name, vhost)


def _read() -> dict[str, Any]:
    if not _inventory_file().exists():
        return {"queues": [], "exchanges": []}
    try:
        with open(_inventory_file(), encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return {"queues": [], "exchanges": []}
    if not isinstance(data, dict):
        return {"queues": [], "exchanges": []}
    data.setdefault("queues", [])
    data.setdefault("exchanges", [])
    return data


def _write(data: dict[str, Any]) -> None:
    config_module.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    for section in ("queues", "exchanges"):
        raw = data.get(section) or []
        # A section may still hold plain dicts read straight from disk, so
        # normalise both shapes before serialising.
        entries: list[InventoryEntry] = []
        for item in raw:
            if isinstance(item, InventoryEntry):
                entries.append(item)
            elif isinstance(item, dict) and item.get("name"):
                entries.append(InventoryEntry.from_dict(item))
        data[section] = sorted(
            (e.to_dict() for e in entries),
            key=lambda d: (d.get("vhost", "/"), d.get("name", "")),
        )
    with open(_inventory_file(), "w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, default_flow_style=False, sort_keys=False)


def _section(data: dict[str, Any], kind: str) -> list[InventoryEntry]:
    key = "exchanges" if kind == "exchange" else "queues"
    entries = []
    for raw in data.get(key) or []:
        if isinstance(raw, dict) and raw.get("name"):
            entries.append(InventoryEntry.from_dict(raw))
    return entries


def add(name: str, vhost: str, kind: str = "queue", note: str | None = None,
        tags: Iterable[str] | None = None) -> InventoryEntry:
    """Add or refresh an entry. Idempotent, so it is safe to call on every use."""
    data = _read()
    existing = _section(data, kind)
    for entry in existing:
        if _key(entry.name, entry.vhost) == _key(name, vhost):
            entry.last_seen = time.time()
            if note:
                entry.note = note
            if tags:
                entry.tags = sorted(set(entry.tags) | set(tags))
            _store(data, kind, existing)
            return entry
    entry = InventoryEntry(
        name=name,
        vhost=vhost,
        kind=kind,
        note=note,
        tags=sorted(set(tags or [])),
        last_seen=time.time(),
    )
    existing.append(entry)
    _store(data, kind, existing)
    return entry


def _store(data: dict[str, Any], kind: str, entries: list[InventoryEntry]) -> None:
    data["exchanges" if kind == "exchange" else "queues"] = entries
    _write(data)


def remove(name: str, vhost: str, kind: str = "queue") -> bool:
    data = _read()
    existing = _section(data, kind)
    kept = [e for e in existing if _key(e.name, e.vhost) != _key(name, vhost)]
    removed = len(kept) != len(existing)
    if removed:
        _store(data, kind, kept)
    return removed


def list_entries(kind: str = "queue", vhost: str | None = None) -> list[InventoryEntry]:
    entries = _section(_read(), kind)
    if vhost is None:
        return sorted(entries, key=lambda e: (e.vhost, e.name))
    return sorted((e for e in entries if e.vhost == vhost), key=lambda e: e.name)


def get(name: str, vhost: str, kind: str = "queue") -> InventoryEntry | None:
    for entry in list_entries(kind, vhost):
        if entry.name == name:
            return entry
    return None


def queue_names(vhost: str, patterns: Iterable[str] | None = None) -> list[str]:
    """Names of tracked queues, optionally filtered by shell-style globs."""
    import fnmatch

    names = [e.name for e in list_entries("queue", vhost)]
    if not patterns:
        return names
    return [n for n in names if any(fnmatch.fnmatch(n, p) for p in patterns)]


def exchange_names(vhost: str, patterns: Iterable[str] | None = None) -> list[str]:
    import fnmatch

    names = [e.name for e in list_entries("exchange", vhost)]
    if patterns:
        return [n for n in names if any(fnmatch.fnmatch(n, p) for p in patterns)]
    # The default exchange always exists and is always publishable.
    return [""] + names


def touch_queue(name: str, vhost: str) -> None:
    """Record a queue the tool just interacted with. Best effort, never raises."""
    if not name:
        return
    try:
        add(name, vhost, kind="queue")
    except OSError:
        pass


def touch_exchange(name: str, vhost: str) -> None:
    if not name:
        return
    try:
        add(name, vhost, kind="exchange")
    except OSError:
        pass


def path() -> Path:
    return _inventory_file()


def is_writable() -> bool:
    """Whether the inventory can be persisted on this machine."""
    target = _inventory_file()
    directory = target.parent
    if os.access(directory, os.W_OK):
        return True
    return target.exists() and os.access(target, os.W_OK)

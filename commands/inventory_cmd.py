from __future__ import annotations

from mq import inventory
from mq.config import MQConfig
from mq.utils import handle_error, is_json_mode, print_json, print_table
from mq.utils.output import dim, print_pairs


def execute_list(config: MQConfig, kind: str = "all") -> None:
    entries: list[inventory.InventoryEntry] = []
    if kind in ("all", "queue"):
        entries += inventory.list_entries("queue")
    if kind in ("all", "exchange"):
        entries += inventory.list_entries("exchange")

    if is_json_mode():
        print_json(
            {
                "file": str(inventory.path()),
                "entries": [e.to_dict() for e in entries],
            }
        )
        return

    if not entries:
        print("The inventory is empty.")
        print()
        print(dim("  AMQP cannot enumerate queues or exchanges, so this list is what\n"
                   "  'mq list' and 'mq exchange list' are able to check. Populate it with:\n"
                   "      mq inventory add orders payments\n"
                   "      mq inventory add-exchange amq.topic\n"
                   "  Queues and exchanges are also recorded automatically after\n"
                   "  declare, bind, publish, consume, move, retry and replay."))
        return

    rows = []
    for entry in entries:
        seen = entry.last_seen or 0
        rows.append(
            [
                entry.kind,
                entry.name or "(default)",
                entry.vhost,
                ",".join(entry.tags) if entry.tags else "-",
                _format_time(seen) if seen else "never",
                entry.note or "-",
            ]
        )
    print_table(
        headers=["KIND", "NAME", "VHOST", "TAGS", "LAST SEEN", "NOTE"],
        rows=rows,
        title="Tracked objects",
    )
    print()
    print(dim(f"  file: {inventory.path()}"))


def execute_add(
    config: MQConfig,
    names: list[str],
    kind: str = "queue",
    vhost: str | None = None,
    tag: list[str] | None = None,
    note: str | None = None,
) -> None:
    target_vhost = vhost or config.vhost
    added = []
    for name in names:
        for part in name.split(","):
            part = part.strip()
            if not part:
                continue
            inventory.add(part, target_vhost, kind=kind, note=note, tags=tag)
            added.append(part)

    if not added:
        handle_error("No names given. Usage: mq inventory add <name> [<name>...]")
        return

    if is_json_mode():
        print_json({"added": added, "vhost": target_vhost, "kind": kind})
        return
    print_pairs(
        [
            ("Added", ", ".join(added)),
            ("Kind", kind),
            ("VHost", target_vhost),
            ("File", str(inventory.path())),
        ],
        title="Inventory updated",
    )
    print()
    print(dim(f"  verify now with: mq list  (or: mq top --limit 5)"))


def execute_remove(
    config: MQConfig, names: list[str], kind: str = "queue", vhost: str | None = None
) -> None:
    target_vhost = vhost or config.vhost
    removed, missing = [], []
    for name in names:
        for part in name.split(","):
            part = part.strip()
            if not part:
                continue
            if inventory.remove(part, target_vhost, kind=kind):
                removed.append(part)
            else:
                missing.append(part)

    if is_json_mode():
        print_json({"removed": removed, "not_found": missing, "vhost": target_vhost})
        return
    if removed:
        print_pairs([("Removed", ", ".join(removed)), ("VHost", target_vhost)],
                    title="Inventory updated")
    if missing:
        print(dim(f"  not tracked: {', '.join(missing)}"))


def execute_purge(config: MQConfig) -> None:
    if not inventory.is_writable():
        handle_error(
            f"Cannot write {inventory.path()}. Check the permissions on the "
            f"config directory."
        )
        return
    if is_json_mode():
        print_json({"file": str(inventory.path()), "writable": True})
        return
    print_pairs([("File", str(inventory.path())), ("Writable", "yes")], title="Inventory")


def _format_time(timestamp: float) -> str:
    import time

    return time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp))

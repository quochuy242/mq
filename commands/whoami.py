from __future__ import annotations

from mq.config import MQConfig
from mq.services.diagnostic_service import probe_permissions
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def _display(value: bool | None) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "n/a"


def execute(config: MQConfig, queue: str | None = None) -> None:
    try:
        report = probe_permissions(config, queue=queue)
    except Exception as e:
        handle_error("Permission probe failed", e)
        return

    if is_json_mode():
        print_json(
            {
                "username": config.username,
                "vhost": config.vhost,
                "permissions": {
                    "configure": report.configure,
                    "write": report.write,
                    "read": report.read,
                },
                "mode": report.mode,
                "details": report.details,
            }
        )
        return

    pairs: list[tuple[str, object]] = [
        ("User", config.username),
        ("VHost", config.vhost),
        ("Configure", _display(report.configure)),
        ("Write", _display(report.write)),
        ("Read", _display(report.read)),
    ]
    if queue:
        pairs.append(("Probed queue", queue))
    else:
        pairs.append(("Method", report.mode))
    for key, detail in (report.details or {}).items():
        pairs.append((f"{key} detail", detail))
    print_pairs(pairs, title="Identity")

from __future__ import annotations

from mq.config import MQConfig
from mq.services.diagnostic_service import probe_permissions
from mq.utils import is_json_mode, print_json


def execute(config: MQConfig) -> None:
    perms = probe_permissions(config)

    if is_json_mode():
        print_json(
            {
                "username": config.username,
                "vhost": config.vhost,
                "permissions": {
                    k: (
                        "Yes"
                        if v is True
                        else "No" if v is False else "N/A"
                    )
                    for k, v in perms.items()
                },
            }
        )
        return

    print(f"User:       {config.username}")
    print(f"VHost:      {config.vhost}")
    print("Permissions:")
    for name in ("configure", "write", "read"):
        val = perms.get(name)
        if val is True:
            display = "Yes"
        elif val is False:
            display = "No (access refused)"
        elif val is None:
            display = "N/A"
        else:
            display = str(val)
        print(f"  {name:<12} {display}")

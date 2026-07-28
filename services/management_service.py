from __future__ import annotations

import json
import urllib.request
from base64 import b64encode
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from mq.config import MQConfig


@dataclass
class MgmtQueueInfo:
    name: str
    ready: int
    unacked: int
    consumers: int
    incoming_rate: float = 0.0
    outgoing_rate: float = 0.0


class ManagementAPIError(Exception):
    pass


def _mgmt_url(config: MQConfig, path: str) -> str:
    if config.ssl:
        scheme = "https"
        port = config.management_port or 15671
    else:
        scheme = "http"
        port = config.management_port or 15672

    if isinstance(port, str):
        port = int(port)
    return f"{scheme}://{config.host}:{port}{path}"


def _mgmt_request(config: MQConfig, path: str) -> Any:
    url = _mgmt_url(config, path)
    credentials = b64encode(
        f"{config.username}:{config.password}".encode()
    ).decode()

    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Basic {credentials}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise ManagementAPIError(
                "Authentication failed for Management API"
            )
        raise ManagementAPIError(
            f"Management API HTTP {e.code}: {e.reason}"
        )
    except urllib.error.URLError as e:
        raise ManagementAPIError(
            f"Cannot reach Management API: {e.reason}"
        )
    except Exception as e:
        raise ManagementAPIError(str(e))


def list_queues(
    config: MQConfig, pattern: str | None = None
) -> list[MgmtQueueInfo]:
    path = "/api/queues"
    if pattern:
        encoded = quote(pattern, safe="")
        path = f"/api/queues?page=1&page_size=100&name={encoded}"

    data = _mgmt_request(config, path)

    if pattern:
        import fnmatch

        all_queues = data if isinstance(data, list) else []
        data = [q for q in all_queues if fnmatch.fnmatch(q.get("name", ""), pattern)]

    queues: list[MgmtQueueInfo] = []
    for q in data if isinstance(data, list) else []:
        queues.append(
            MgmtQueueInfo(
                name=q.get("name", ""),
                ready=q.get("messages_ready", 0),
                unacked=q.get("messages_unacknowledged", 0),
                consumers=q.get("consumers", 0),
            )
        )

    queues.sort(key=lambda q: q.name)
    return queues


def get_queue_details(
    config: MQConfig, queue: str
) -> MgmtQueueInfo | None:
    encoded_vhost = quote(config.vhost, safe="")
    encoded_queue = quote(queue, safe="")
    path = f"/api/queues/{encoded_vhost}/{encoded_queue}"

    try:
        data = _mgmt_request(config, path)
    except ManagementAPIError:
        return None

    return MgmtQueueInfo(
        name=data.get("name", queue),
        ready=data.get("messages_ready", 0),
        unacked=data.get("messages_unacknowledged", 0),
        consumers=data.get("consumers", 0),
        incoming_rate=data.get("message_stats", {}).get("publish_details", {}).get("rate", 0.0),
        outgoing_rate=data.get("message_stats", {}).get("deliver_details", {}).get("rate", 0.0),
    )


def is_mgmt_available(config: MQConfig) -> bool:
    try:
        _mgmt_request(config, "/api/overview")
        return True
    except ManagementAPIError:
        return False

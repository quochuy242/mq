"""Optional HTTP client for RabbitMQ's management plugin.

Everything here is an *enhancement*, never a requirement. The tool works fully
over AMQP 0-9-1; this module is only consulted when a command asks for it
explicitly, or as a last-resort fallback for ``mq list`` when the local
inventory is empty. If port 15672/15671 is closed, nothing in the AMQP path
changes behaviour.
"""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from base64 import b64encode
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from mq.config import MQConfig
from mq.connection import build_ssl_context


@dataclass
class MgmtQueueInfo:
    name: str
    ready: int
    unacked: int
    consumers: int
    vhost: str = "/"
    durable: bool | None = None
    node: str | None = None
    incoming_rate: float = 0.0
    outgoing_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue": self.name,
            "vhost": self.vhost,
            "ready": self.ready,
            "unacked": self.unacked,
            "consumers": self.consumers,
            "durable": self.durable,
            "node": self.node,
            "incoming_rate": self.incoming_rate,
            "outgoing_rate": self.outgoing_rate,
        }


class ManagementAPIError(Exception):
    """The management API could not be reached, or refused the request."""


def default_port(config: MQConfig) -> int:
    return config.management_port or (15671 if config.ssl else 15672)


def base_url(config: MQConfig) -> str:
    scheme = "https" if config.ssl else "http"
    return f"{scheme}://{config.host}:{default_port(config)}"


def _mgmt_url(config: MQConfig, path: str) -> str:
    return f"{base_url(config)}{path}"


def _request(
    config: MQConfig, path: str, method: str = "GET", payload: Any = None
) -> Any:
    credentials = b64encode(f"{config.username}:{config.password}".encode()).decode()
    body = None
    headers = {"Authorization": f"Basic {credentials}", "Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(_mgmt_url(config, path), data=body, headers=headers, method=method)

    # Reuse the tool's TLS trust store, so a broker with an internal CA is not
    # reachable over HTTP but still is over https.
    if config.ssl:
        req_opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=build_ssl_context(config))
        )
    else:
        req_opener = urllib.request.build_opener()

    try:
        with req_opener.open(req, timeout=config.connection_timeout + 5) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise ManagementAPIError(
                f"Management API refused the credentials (HTTP {e.code}). "
                "The 'management' tag is required on the user."
            ) from e
        if e.code == 404:
            raise ManagementAPIError(
                f"Management API HTTP 404: no such endpoint '{path}'. "
                "Is the rabbitmq_management plugin enabled?"
            ) from e
        raise ManagementAPIError(f"Management API HTTP {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise ManagementAPIError(
            f"Cannot reach the management API at {base_url(config)}: {e.reason}"
        ) from e
    except ssl.SSLError as e:
        raise ManagementAPIError(f"TLS error talking to the management API: {e}") from e
    except json.JSONDecodeError as e:
        raise ManagementAPIError(f"Management API returned invalid JSON: {e}") from e


def is_available(config: MQConfig) -> bool:
    """Whether the management plugin answers. Never raises."""
    try:
        _request(config, "/api/overview")
        return True
    except ManagementAPIError:
        return False


def unreachable_reason(config: MQConfig) -> str:
    """A short explanation of why the plugin path is not usable."""
    try:
        _request(config, "/api/overview")
    except ManagementAPIError as e:
        return str(e)
    return ""


def list_queues(
    config: MQConfig, pattern: str | None = None, include_unacked: bool = True
) -> list[MgmtQueueInfo]:
    """List queues in the *current vhost* (not every vhost on the node)."""
    import fnmatch

    vhost = quote(config.vhost, safe="")
    results: list[MgmtQueueInfo] = []
    page = 1
    while True:
        try:
            data = _request(config, f"/api/queues/{vhost}?page={page}&page_size=500")
        except ManagementAPIError:
            raise
        if not isinstance(data, list) or not data:
            break
        for item in data:
            results.append(_to_queue_info(item, config.vhost))
        if len(data) < 500:
            break
        page += 1

    if pattern:
        results = [q for q in results if fnmatch.fnmatch(q.name, pattern)]
    results.sort(key=lambda q: q.name)
    return results


def _to_queue_info(item: dict[str, Any], vhost: str) -> MgmtQueueInfo:
    stats = item.get("message_stats") or {}
    return MgmtQueueInfo(
        name=item.get("name", ""),
        ready=item.get("messages_ready", 0),
        unacked=item.get("messages_unacknowledged", 0),
        consumers=item.get("consumers", 0),
        vhost=item.get("vhost", vhost),
        durable=item.get("durable"),
        node=item.get("node"),
        incoming_rate=(stats.get("publish_details") or {}).get("rate", 0.0),
        outgoing_rate=(stats.get("deliver_details") or {}).get("rate", 0.0),
    )


def get_queue_details(config: MQConfig, queue: str) -> MgmtQueueInfo | None:
    vhost = quote(config.vhost, safe="")
    encoded_queue = quote(queue, safe="")
    try:
        data = _request(config, f"/api/queues/{vhost}/{encoded_queue}")
    except ManagementAPIError:
        return None
    if not isinstance(data, dict):
        return None
    return _to_queue_info(data, config.vhost)


def list_exchanges(config: MQConfig) -> list[dict[str, Any]]:
    vhost = quote(config.vhost, safe="")
    data = _request(config, f"/api/exchanges/{vhost}?page_size=500")
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []

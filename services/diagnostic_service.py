from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import create_connection


@dataclass
class CheckResult:
    name: str
    status: bool
    detail: str = ""
    duration_ms: float = 0.0


@dataclass
class DoctorReport:
    checks: list[CheckResult] = field(default_factory=list)
    latency_ms: float = 0.0

    @property
    def all_ok(self) -> bool:
        return all(c.status for c in self.checks)


def _check(checks: list[CheckResult], name: str, fn: Any) -> None:
    start = time.perf_counter()
    try:
        fn()
        checks.append(
            CheckResult(
                name=name, status=True, duration_ms=(time.perf_counter() - start) * 1000
            )
        )
    except Exception as e:
        checks.append(
            CheckResult(
                name=name,
                status=False,
                detail=str(e).split("\n")[0],
                duration_ms=(time.perf_counter() - start) * 1000,
            )
        )


def run_doctor(config: MQConfig) -> DoctorReport:
    report = DoctorReport()
    checks = []

    def check_tcp():
        import socket

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(config.connection_timeout)
        s.connect((config.host, config.port))
        s.close()

    def check_tls():
        import ssl as sslmod

        ctx = sslmod.create_default_context()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(config.connection_timeout)
        sock.connect((config.host, config.port))
        ssl_sock = ctx.wrap_socket(
            sock, server_hostname=config.host
        )
        ssl_sock.close()
        sock.close()

    def check_auth():
        conn = create_connection(config)
        conn.close()

    def check_channel():
        conn = create_connection(config)
        ch = conn.channel()
        ch.close()
        conn.close()

    def check_declare():
        conn = create_connection(config)
        ch = conn.channel()
        ch.queue_declare(
            queue=f"mq-doctor-probe-{int(time.time())}",
            auto_delete=True,
        )
        ch.close()
        conn.close()

    def check_publish():
        import uuid

        q = f"mq-doctor-probe-{uuid.uuid4().hex[:8]}"
        conn = create_connection(config)
        ch = conn.channel()
        ch.queue_declare(queue=q, auto_delete=True)
        ch.basic_publish(exchange="", routing_key=q, body=b"doctor")
        ch.queue_delete(queue=q)
        ch.close()
        conn.close()

    def check_consume():
        import uuid

        q = f"mq-doctor-probe-{uuid.uuid4().hex[:8]}"
        conn = create_connection(config)
        ch = conn.channel()
        ch.queue_declare(queue=q, auto_delete=True)
        ch.basic_publish(exchange="", routing_key=q, body=b"doctor")
        mf, _, _ = ch.basic_get(queue=q, auto_ack=True)
        ch.queue_delete(queue=q)
        ch.close()
        conn.close()
        if mf is None:
            raise Exception("consume failed")

    _check(checks, "TCP connectivity", check_tcp)

    if config.ssl:
        _check(checks, "TLS handshake", check_tls)

    _check(checks, "Authentication", check_auth)
    _check(checks, "Channel creation", check_channel)
    _check(checks, "Queue declare", check_declare)
    _check(checks, "Publish", check_publish)
    _check(checks, "Consume", check_consume)

    # latency test
    latencies = []
    for _ in range(3):
        start = time.perf_counter()
        conn = create_connection(config)
        conn.close()
        latencies.append((time.perf_counter() - start) * 1000)
    report.latency_ms = sum(latencies) / len(latencies)

    report.checks = checks
    return report


def probe_permissions(config: MQConfig) -> dict[str, bool | str | None]:
    from uuid import uuid4

    perms: dict[str, bool | str | None] = {}

    probe_queue = f"mq-perm-probe-{uuid4().hex[:8]}"

    def _probe(method: str) -> bool | str | None:
        connection = create_connection(config)
        channel = connection.channel()
        try:
            if method == "configure":
                channel.queue_declare(queue=probe_queue, auto_delete=False)
                return True
            elif method == "write":
                channel.basic_publish(
                    exchange="", routing_key=probe_queue, body=b""
                )
                return True
            elif method == "read":
                channel.basic_get(queue=probe_queue, auto_ack=True)
                return True
            return None
        except ChannelClosedByBroker:
            return False
        except Exception as e:
            return f"error: {e}"
        finally:
            try:
                channel.close()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass

    perms["configure"] = _probe("configure")

    if perms.get("configure") is True:
        perms["write"] = _probe("write")
        perms["read"] = _probe("read")

        ch = None
        try:
            conn = create_connection(config)
            ch = conn.channel()
            ch.queue_delete(queue=probe_queue)
        except Exception:
            pass
        finally:
            if ch:
                try:
                    ch.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
    else:
        perms["write"] = None
        perms["read"] = None

    return perms

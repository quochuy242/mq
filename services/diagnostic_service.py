from __future__ import annotations

import socket
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

import pika
import pika.exceptions
from pika.exceptions import ChannelClosedByBroker

from mq.config import MQConfig
from mq.connection import (
    BlockedState,
    build_ssl_context,
    close_quietly,
    create_channel,
    create_connection,
    negotiated,
    server_properties,
)
from mq.errors import classify


@dataclass
class CheckResult:
    name: str
    status: bool
    detail: str = ""
    duration_ms: float = 0.0
    hint: str | None = None
    severity: str = "error"  # error | warning

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "duration_ms": round(self.duration_ms, 2),
        }
        if self.severity != "error":
            data["severity"] = self.severity
        if self.hint:
            data["hint"] = self.hint
        return data


@dataclass
class DoctorReport:
    checks: list[CheckResult] = field(default_factory=list)
    latency_ms: float = 0.0
    broker: dict[str, Any] = field(default_factory=dict)
    readonly: bool = False

    @property
    def all_ok(self) -> bool:
        return all(c.status for c in self.checks)

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.status]


def _check(
    checks: list[CheckResult],
    name: str,
    fn: Callable[[], str | None],
    severity: str = "error",
) -> None:
    """Run one check, recording the duration and the first line of any failure."""
    start = time.perf_counter()
    try:
        detail = fn()
    except Exception as e:
        info = classify(e)
        checks.append(
            CheckResult(
                name=name,
                status=False,
                detail=info.detail,
                hint=info.hint,
                duration_ms=(time.perf_counter() - start) * 1000,
                severity=severity,
            )
        )
    else:
        checks.append(
            CheckResult(
                name=name,
                status=True,
                detail=detail or "",
                duration_ms=(time.perf_counter() - start) * 1000,
                severity=severity,
            )
        )


def broker_info(config: MQConfig) -> dict[str, Any]:
    """Read the broker's own description of itself, straight from AMQP.

    Connection.Start-Ok carries product, version, cluster name and the
    capability table. This used to be the job of ``GET /api/overview`` on the
    management port, and it is available here without any plugin.
    """
    conn = create_connection(config)
    try:
        props = server_properties(conn)
        limits = negotiated(conn)
        capabilities = props.get("capabilities") or {}
        return {
            "product": props.get("product", "unknown"),
            "version": props.get("version", "unknown"),
            "cluster_name": props.get("cluster_name"),
            "information": props.get("information"),
            "copyright": props.get("copyright"),
            "auth_mechanisms": props.get("auth_mechanisms"),
            "capabilities": {
                "publisher_confirms": capabilities.get("publisher_confirms", False),
                "consumer_cancel_notify": capabilities.get("consumer_cancel_notify", False),
                "basic_nack": capabilities.get("basic.nack", False),
                "connection.blocked": capabilities.get("connection.blocked", False),
                "exchange_exchange_bindings": capabilities.get(
                    "exchange_exchange_bindings", False
                ),
                "per_consumer_qos": capabilities.get("per_consumer_qos", False),
            },
            "frame_max": limits.get("frame_max"),
            "channel_max": limits.get("channel_max"),
            "heartbeat": limits.get("heartbeat"),
        }
    finally:
        close_quietly(conn)


def run_doctor(config: MQConfig, readonly: bool = False) -> DoctorReport:
    """Connectivity, TLS, auth and broker self-report, in that order.

    With ``readonly`` the write probes are skipped, so the command is safe to
    run against production as a user who only holds read permissions.
    """
    report = DoctorReport(readonly=readonly)
    checks: list[CheckResult] = []

    def check_dns() -> str:
        infos = socket.getaddrinfo(config.host, config.port, proto=socket.IPPROTO_TCP)
        families = sorted({info[0] for info in infos})
        names = {socket.AF_INET: "ipv4", socket.AF_INET6: "ipv6"}
        found = ", ".join(names.get(f, str(f)) for f in families)
        return f"{config.host} resolves to {found}"

    def check_tcp() -> str:
        sock = socket.create_connection((config.host, config.port), timeout=config.connection_timeout)
        sock.close()
        return f"TCP connect to {config.host}:{config.port} succeeded"

    def check_tls() -> str:
        # Must use the tool's own context, otherwise a broker trusted via
        # ssl_cafile passes 'mq ping' but fails the doctor's TLS check.
        context = build_ssl_context(config)
        sock = socket.create_connection((config.host, config.port), timeout=config.connection_timeout)
        try:
            tls_sock = context.wrap_socket(sock, server_hostname=config.host)
        except Exception:
            sock.close()
            raise
        try:
            cipher = tls_sock.cipher()
            peer = tls_sock.getpeercert()
            subject = _certificate_subject(peer)
            return f"{tls_sock.version()} {cipher[0] if cipher else '?'} peer={subject}"
        finally:
            tls_sock.close()

    def check_auth() -> str:
        conn = create_connection(config)
        close_quietly(conn)
        return f"authenticated as {config.username} on vhost {config.vhost!r}"

    def check_channel() -> str:
        conn, channel, _ = create_channel(config)
        number = channel.channel_number
        close_quietly(channel, conn)
        return f"opened channel {number}"

    def check_broker_info() -> str:
        info = broker_info(config)
        report.broker = info
        return f"{info['product']} {info['version']} (cluster {info['cluster_name']})"

    def check_confirms() -> str:
        conn, channel, _ = create_channel(config, confirm=True)
        try:
            return "publisher confirms enabled"
        finally:
            close_quietly(channel, conn)

    def check_declare() -> str:
        # Server-named, exclusive, auto-delete: no named queue is created and
        # the broker removes it as soon as this channel closes.
        conn, channel, _ = create_channel(config)
        try:
            name = channel.queue_declare(
                queue="", exclusive=True, auto_delete=True
            ).method.queue
            return f"declared server-named queue {name}"
        finally:
            close_quietly(channel, conn)

    def check_publish() -> str:
        conn, channel, blocked = create_channel(config, confirm=True)
        probe = f"mq-doctor-{uuid4().hex[:8]}"
        try:
            channel.queue_declare(queue=probe, exclusive=True, auto_delete=True)
            channel.basic_publish(
                exchange="", routing_key=probe, body=b"doctor",
                properties=pika.BasicProperties(delivery_mode=1),
            )
            blocked.raise_if_blocked()
            return "published and confirmed on an ephemeral queue"
        finally:
            close_quietly(channel, conn)

    def check_consume() -> str:
        conn, channel, _ = create_channel(config)
        probe = f"mq-doctor-{uuid4().hex[:8]}"
        try:
            channel.queue_declare(queue=probe, exclusive=True, auto_delete=True)
            channel.basic_publish(
                exchange="", routing_key=probe, body=b"doctor",
                properties=pika.BasicProperties(delivery_mode=1),
            )
            method_frame, _, _ = channel.basic_get(queue=probe, auto_ack=True)
            if method_frame is None:
                raise RuntimeError(
                    "the message was published but basic.get returned nothing"
                )
            return "read back the probe message"
        finally:
            close_quietly(channel, conn)

    def check_blocked_channel() -> str:
        state = BlockedState()
        conn = create_connection(config, blocked_state=state)
        try:
            conn.channel()
            if state.blocked:
                raise RuntimeError(f"broker reports a resource alarm: {state.reason}")
            return "no resource alarm on this connection"
        finally:
            close_quietly(conn)

    _check(checks, "DNS resolution", check_dns, severity="warning")
    _check(checks, "TCP connectivity", check_tcp)
    if config.ssl:
        _check(checks, "TLS handshake", check_tls)
    _check(checks, "Authentication", check_auth)
    _check(checks, "Channel creation", check_channel)
    _check(checks, "Broker information", check_broker_info)
    _check(checks, "Publisher confirms", check_confirms)
    _check(checks, "Connection not blocked", check_blocked_channel, severity="warning")
    if not readonly:
        _check(checks, "Queue declare", check_declare)
        _check(checks, "Publish", check_publish)
        _check(checks, "Consume", check_consume)
    else:
        checks.append(
            CheckResult(
                name="Write probes",
                status=True,
                detail="skipped (--readonly)",
                severity="warning",
            )
        )

    report.latency_ms = _measure_handshake_latency(config)
    report.checks = checks
    return report


def _certificate_subject(peer: dict[str, Any] | None) -> str:
    if not peer:
        return "(no peer certificate)"
    for rdn in peer.get("subject", ()):
        for key, value in rdn:
            if key == "commonName":
                return str(value)
    return "(no CN)"


def _measure_handshake_latency(config: MQConfig, rounds: int = 3) -> float:
    latencies: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        try:
            conn = create_connection(config)
        except Exception:
            continue
        close_quietly(conn)
        latencies.append((time.perf_counter() - start) * 1000)
    if not latencies:
        return 0.0
    return sum(latencies) / len(latencies)


@dataclass
class PermissionReport:
    configure: bool | None = None
    write: bool | None = None
    read: bool | None = None
    mode: str = "probe queue"
    details: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "configure": self.configure,
            "write": self.write,
            "read": self.read,
            "mode": self.mode,
            "details": self.details,
        }


def probe_permissions(
    config: MQConfig, queue: str | None = None
) -> PermissionReport:
    """Determine configure/write/read the only way AMQP allows: by trying.

    With no ``queue`` this uses a server-named exclusive queue, so no named
    object is left behind and no existing queue is touched. Passing a queue
    name switches to a read-only check against an existing queue, which works
    for users who are not allowed to declare anything at all.
    """
    report = PermissionReport()

    if queue:
        report.mode = f"read-only probe on queue {queue!r}"
        report.read = _probe_read_existing(config, queue, report)
        report.configure = False
        report.write = None
        return report

    report.mode = "server-named exclusive queue (no named object created)"
    report.configure = _probe_configure(config, report)
    if report.configure is not True:
        report.write = None
        report.read = None
        return report
    report.write = _probe_write(config, report)
    report.read = _probe_read(config, report)
    return report


def _new_exclusive_queue(channel: Any) -> str:
    return channel.queue_declare(
        queue="", exclusive=True, auto_delete=True
    ).method.queue


def _probe_configure(config: MQConfig, report: PermissionReport) -> bool | None:
    conn, channel, _ = create_channel(config)
    try:
        _new_exclusive_queue(channel)
        return True
    except ChannelClosedByBroker as e:
        info = classify(e)
        report.details["configure"] = info.message()
        return False if info.kind != "connection-lost" else None
    except Exception as e:
        report.details["configure"] = classify(e).detail
        return None
    finally:
        close_quietly(channel, conn)


def _probe_write(config: MQConfig, report: PermissionReport) -> bool | None:
    conn, channel, _ = create_channel(config)
    try:
        name = _new_exclusive_queue(channel)
        channel.basic_publish(
            exchange="",
            routing_key=name,
            body=b"",
            properties=pika.BasicProperties(delivery_mode=1),
            mandatory=True,
        )
        return True
    except ChannelClosedByBroker as e:
        report.details["write"] = classify(e).message()
        return False
    except pika.exceptions.AMQPError as e:
        report.details["write"] = classify(e).detail
        return None
    finally:
        close_quietly(channel, conn)


def _probe_read(config: MQConfig, report: PermissionReport) -> bool | None:
    conn, channel, _ = create_channel(config)
    try:
        name = _new_exclusive_queue(channel)
        channel.basic_publish(exchange="", routing_key=name, body=b"probe")
        channel.basic_get(queue=name, auto_ack=True)
        return True
    except ChannelClosedByBroker as e:
        report.details["read"] = classify(e).message()
        return False
    except pika.exceptions.AMQPError as e:
        report.details["read"] = classify(e).detail
        return None
    finally:
        close_quietly(channel, conn)


def _probe_read_existing(
    config: MQConfig, queue: str, report: PermissionReport
) -> bool | None:
    conn, channel, _ = create_channel(config)
    try:
        method_frame, _, _ = channel.basic_get(queue=queue, auto_ack=False)
        if method_frame is not None:
            # Put it straight back; the tool must not consume a real message.
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
        else:
            channel.basic_get(queue=queue, auto_ack=True)
        return True
    except ChannelClosedByBroker as e:
        info = classify(e)
        report.details["read"] = info.message()
        if info.code == 404:
            return None
        return False
    except pika.exceptions.AMQPError as e:
        report.details["read"] = classify(e).detail
        return None
    finally:
        close_quietly(channel, conn)

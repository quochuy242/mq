from __future__ import annotations

import os
import socket
import ssl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Tuple

try:
    import certifi
except Exception:  # pragma: no cover - optional runtime dependency
    certifi = None

import pika
import pika.credentials
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel

from mq.config import MQConfig

CLIENT_NAME = "mq-cli"
CLIENT_VERSION = "1.0.0"


class SSLConfigurationError(Exception):
    """Raised when the TLS settings are unusable (bad CA path, etc.)."""


class BrokerBlockedError(Exception):
    """Raised when the broker blocks publishing because of a resource alarm."""


@dataclass
class BlockedState:
    """Tracks connection.blocked / connection.unblocked frames from the broker.

    RabbitMQ sends these when a node hits a memory or disk alarm. They arrive on
    the connection, not the channel, and they are the only way to observe the
    alarm state over AMQP -- so every long-running command needs to look here.
    """

    reason: str | None = None
    since: float | None = None
    events: list[tuple[str, float]] = field(default_factory=list)

    @property
    def blocked(self) -> bool:
        return self.reason is not None

    def blocked_callback(self, connection: Any, method: Any) -> None:
        self.reason = getattr(method, "reason", "resource alarm")
        self.since = time.time()
        self.events.append(("blocked", self.since))

    def unblocked_callback(self, connection: Any, method: Any) -> None:
        self.reason = None
        self.since = None
        self.events.append(("unblocked", time.time()))

    def raise_if_blocked(self) -> None:
        if self.reason:
            raise BrokerBlockedError(
                f"The broker is blocking publishers: {self.reason}. "
                "Publishing stopped because a node resource alarm is active."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "blocked": self.blocked,
            "reason": self.reason,
            "since": self.since,
        }


def _candidate_ca_files(config: MQConfig) -> list[tuple[str, Path]]:
    """CA bundle sources in priority order, as (origin, resolved path)."""
    candidates: list[tuple[str, Path]] = []

    if config.ssl_cafile:
        candidates.append(("ssl_cafile", Path(config.ssl_cafile).expanduser()))

    for env_var in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
        value = os.environ.get(env_var)
        if value:
            candidates.append((env_var, Path(value).expanduser()))

    if certifi is not None:
        try:
            candidates.append(("certifi", Path(certifi.where())))
        except Exception:
            pass

    return candidates


def _resolve_ca_file(config: MQConfig) -> Path | None:
    """Pick the first CA bundle that actually exists on disk."""
    for origin, path in _candidate_ca_files(config):
        if path.is_file():
            return path
        if origin == "ssl_cafile":
            # Explicitly configured but missing: this is a config mistake worth
            # reporting instead of silently falling back to another store.
            raise SSLConfigurationError(
                f"CA certificate file not found: {path}\n"
                f"  Configured via ssl_cafile. Fix the path, or unset it to use "
                f"the system trust store."
            )
    return None


def build_ssl_context(config: MQConfig) -> ssl.SSLContext:
    """Build the TLS context used for AMQP connections.

    Verification is on by default. Two ways to relax it:
      ssl_cafile  - trust a specific CA bundle (recommended)
      ssl_verify  - false disables verification entirely (insecure)

    With ``auth: external`` the context keeps the client certificate readable
    so SASL EXTERNAL can present it.
    """
    if not config.ssl_verify:
        # check_hostname must be cleared before verify_mode, not after.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    ca_file = _resolve_ca_file(config)
    context = ssl.create_default_context()
    if ca_file is not None:
        try:
            context.load_verify_locations(cafile=str(ca_file))
            # Keep the system/public CAs usable alongside the custom bundle.
            context.load_default_certs(ssl.Purpose.SERVER_AUTH)
        except (OSError, ssl.SSLError) as e:
            raise SSLConfigurationError(
                f"Failed to load CA certificate file {ca_file}: {e}"
            ) from e

    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True

    if config.auth == "external":
        # A client cert is useless unless it is actually loaded, and leaving
        # load_default_certs() in place is harmless for a CA-less client cert.
        _load_client_certificate(context, config)
    return context


def _load_client_certificate(context: ssl.SSLContext, config: MQConfig) -> None:
    if not config.client_cert:
        return
    try:
        context.load_cert_chain(certfile=config.client_cert, keyfile=config.client_key)
    except (OSError, ssl.SSLError) as exc:
        raise SSLConfigurationError(
            f"Failed to load the client certificate {config.client_cert}: {exc}"
        ) from exc


def _build_credentials(config: MQConfig) -> pika.credentials.Credentials:
    if config.auth == "external":
        return pika.credentials.ExternalCredentials()
    if config.auth == "oauth":
        token = config.token or config.password
        if not token:
            raise ValueError("auth: oauth requires a token (MQ_TOKEN).")
        return pika.credentials.PlainCredentials(config.username, token)
    return pika.PlainCredentials(config.username, config.password)


def _client_properties(config: MQConfig) -> dict[str, Any]:
    """Identify this client in the broker's connection list.

    Without this, connections show up as "Unknown" in the management UI, which
    makes it impossible to tell an ``mq`` session from an application.
    """
    name = config.connection_name or f"{CLIENT_NAME}:{os.getpid()}"
    props: dict[str, Any] = {
        "product": CLIENT_NAME,
        "version": CLIENT_VERSION,
        "platform": "Python",
        "information": "RabbitMQ CLI over AMQP 0-9-1",
    }
    if config.connection_name:
        props["connection_name"] = config.connection_name
    return props


def _parameters(config: MQConfig) -> pika.ConnectionParameters:
    ssl_options: pika.SSLOptions | None = None
    if config.ssl:
        context = build_ssl_context(config)
        ssl_options = pika.SSLOptions(context, config.host)

    return pika.ConnectionParameters(
        host=config.host,
        port=config.port,
        virtual_host=config.vhost,
        credentials=_build_credentials(config),
        ssl_options=ssl_options,
        heartbeat=config.heartbeat,
        socket_timeout=config.connection_timeout,
        stack_timeout=config.connection_timeout + 5,
        connection_attempts=max(1, config.connection_attempts),
        retry_delay=1,
        blocked_connection_timeout=config.blocked_connection_timeout or None,
        client_properties=_client_properties(config),
    )


_VERIFY_HINT = """\
The broker's TLS certificate is not trusted by this tool (self-signed or \
internal CA). Recommended fix: mq config --ssl-cafile /path/to/ca_certificate.pem
(run with --debug for the full certificate details, see README 'TLS / SSL')."""


def _is_cert_verify_error(exc: BaseException) -> bool:
    if isinstance(exc, ssl.SSLCertVerificationError):
        return True
    message = str(exc).lower()
    return "certificate_verify_failed" in message or "certificate verify failed" in message


def _describe_socket_error(config: MQConfig, exc: BaseException) -> str:
    """Turn a refused connection into something that says what to try next."""
    text = str(exc).lower()
    if isinstance(exc, socket.gaierror):
        return (
            f"Cannot resolve host '{config.host}'. "
            "Check DNS, or pass the address via MQ_HOST/--host."
        )
    if isinstance(exc, ConnectionRefusedError) or "refused" in text:
        if config.ssl and config.port == 5672:
            return (
                f"Connection refused on {config.host}:{config.port} while TLS is "
                "enabled. Port 5672 is usually plaintext; use 5671 for amqps."
            )
        return (
            f"Connection refused on {config.host}:{config.port}. "
            "Check that the broker is running and the port is reachable."
        )
    if isinstance(exc, socket.timeout) or "timed out" in text:
        return (
            f"Timed out connecting to {config.host}:{config.port} after "
            f"{config.connection_timeout}s. Check firewalls and routing."
        )
    return str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__


def create_connection(
    config: MQConfig, blocked_state: BlockedState | None = None
) -> pika.BlockingConnection:
    """Open a connection, translating low-level failures into useful text."""
    parameters = _parameters(config)
    try:
        connection = pika.BlockingConnection(parameters)
    except Exception as e:
        # pika may surface a TLS failure raw or wrapped in an AMQP error, so
        # match on the message rather than the exception type.
        if _is_cert_verify_error(e):
            raise SSLConfigurationError(
                f"TLS certificate verification failed for "
                f"{config.host}:{config.port} - {_VERIFY_HINT}"
            ) from e
        if isinstance(e, pika.exceptions.ProbableAuthenticationError):
            raise
        if isinstance(e, pika.exceptions.AMQPConnectionError):
            raise ConnectionError(_describe_socket_error(config, e)) from e
        raise

    if blocked_state is not None:
        connection.add_on_connection_blocked_callback(
            blocked_state.blocked_callback
        )
        connection.add_on_connection_unblocked_callback(
            blocked_state.unblocked_callback
        )
    return connection


def create_channel(
    config: MQConfig,
    blocked_state: BlockedState | None = None,
    confirm: bool = False,
) -> Tuple[pika.BlockingConnection, BlockingChannel, BlockedState]:
    """Open a connection and channel, plus the blocked-state tracker.

    ``confirm=True`` turns on publisher confirms. Every command that publishes
    should use it: without confirms the broker gives no guarantee that a
    message was accepted, and a "moved"/"retried" message can vanish.
    """
    state = blocked_state if blocked_state is not None else BlockedState()
    connection = create_connection(config, blocked_state=state)
    channel = connection.channel()
    if confirm:
        channel.confirm_delivery()
    return connection, channel, state


def close_quietly(*objects: Any) -> None:
    """Close connections/channels without masking the original failure."""
    for obj in objects:
        if obj is None:
            continue
        try:
            obj.close()
        except Exception:
            pass


def server_properties(connection: pika.BlockingConnection) -> dict[str, Any]:
    """The broker's Connection.Start-Ok properties.

    This is free AMQP information: product, version, cluster name, negotiated
    frame/channel limits and the capability table. No plugin required.
    """
    impl = getattr(connection, "_impl", None)
    props = getattr(impl, "server_properties", None)
    if isinstance(props, dict):
        return dict(props)
    return {}


def negotiated(connection: pika.BlockingConnection) -> dict[str, Any]:
    """Limits the broker actually agreed to, which can differ from the config."""
    impl = getattr(connection, "_impl", None)
    params = getattr(connection, "params", None)
    result: dict[str, Any] = {
        "heartbeat": getattr(impl, "_heartbeat", None)
        or getattr(params, "heartbeat", None),
        "frame_max": getattr(connection, "frame_max", None),
        "channel_max": getattr(connection, "channel_max", None),
    }
    capabilities = getattr(impl, "server_capabilities", None)
    if isinstance(capabilities, dict):
        result["capabilities"] = capabilities
    return result

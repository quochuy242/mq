"""Actionable classification of AMQP failures.

The tool talks to RabbitMQ over AMQP 0-9-1 only, so every failure a user can
hit surfaces as a broker reply code. Raw reply text ("PRECONDITION_FAILED -
inequivalent arg 'durable'") is cryptic; this module turns those codes into a
short kind, a human explanation and a suggested next step.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pika
from pika.exceptions import (
    AMQPChannelError,
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
    ProbableAuthenticationError,
    ProbableAccessDeniedError,
)


@dataclass
class AmqpErrorInfo:
    """A broker failure, decoded into something a human can act on."""

    kind: str
    detail: str
    hint: str | None = None
    code: int | None = None
    fatal: bool = False

    def message(self) -> str:
        parts = [self.detail]
        if self.hint:
            parts.append(f"  -> {self.hint}")
        return "\n".join(parts)


# Broker reply codes, most to least specific. Values are the AMQP reply codes
# RabbitMQ uses; several are overloaded (406 is also the quota error), so the
# text is matched as well.
_REPLY_CODES: dict[int, tuple[str, str, str | None]] = {
    311: (
        "content-too-large",
        "The message is larger than the broker's max frame size.",
        "Publish a smaller message, or negotiate a larger frame_max on a broker "
        "that allows it.",
    ),
    320: (
        "connection-forced",
        "The broker closed the connection (CONNECTION_FORCED).",
        "Usually a resource alarm, a node restart, or a duplicate client-properties "
        "name. Check broker logs.",
    ),
    402: (
        "invalid-path",
        "The vhost in the connection parameters does not exist (INVALID_PATH).",
        "Check the vhost name; note that '/' and '%2F' are different on the wire.",
    ),
    403: (
        "access-refused",
        "The user is not permitted to perform this operation (ACCESS_REFUSED).",
        "The object may not exist, or the user lacks the matching permission "
        "(configure/write/read) on this vhost.",
    ),
    404: (
        "not-found",
        "The queue or exchange does not exist (NOT_FOUND).",
        "RabbitMQ also returns 404 instead of 403 when you may not see the object, "
        "so a permission problem looks identical to a missing object.",
    ),
    405: (
        "resource-locked",
        "The resource is locked by another connection (RESOURCE_LOCKED).",
        "An exclusive consumer is holding it, or it is being redeclared elsewhere.",
    ),
    406: (
        "precondition-failed",
        "The declaration does not match the existing object (PRECONDITION_FAILED).",
        "An existing queue/exchange has different durable, auto-delete, type or "
        "arguments. Declare it passively (mq info-queue) to inspect it.",
    ),
    413: (
        "quota-exceeded",
        "A configured quota or limit was exceeded (quota exceeded).",
        "The vhost or node hit a queue/disk memory limit; messages are refused "
        "until the backlog drains.",
    ),
    504: (
        "channel-error",
        "The channel was closed by the broker (CHANNEL_ERROR).",
        "The frame or operation exceeded a broker-side limit on this channel.",
    ),
    530: (
        "not-allowed",
        "The operation is not allowed (NOT_ALLOWED).",
        "The vhost does not exist, access is refused, or a user-name collision "
        "was rejected.",
    ),
}

# Substrings that disambiguate overloaded reply codes.
_REPLY_TEXT_HINTS: tuple[tuple[str, str, str], ...] = (
    ("memory", "quota-exceeded", "A node memory/disk alarm is active; publishing is blocked."),
    ("disk", "quota-exceeded", "A node memory/disk alarm is active; publishing is blocked."),
    ("quota", "quota-exceeded", "A queue or vhost limit was exceeded."),
    ("inequivalent", "precondition-failed", "The existing object was declared with different arguments."),
    ("unknown", "precondition-failed", "Argument type/value is not supported by the broker."),
    ("access refused", "access-refused", "Permission is missing for this vhost or resource."),
)


def _classify_reply(code: int | None, text: str) -> tuple[str, str, str | None]:
    lowered = (text or "").lower()
    for needle, kind, hint in _REPLY_TEXT_HINTS:
        if needle in lowered:
            return kind, _first_line(text), hint
    if code is not None and code in _REPLY_CODES:
        kind, detail, hint = _REPLY_CODES[code]
        return kind, detail, hint
    return "amqp-error", _first_line(text) or "The broker rejected the operation.", None


def _first_line(text: str | None) -> str:
    if not text:
        return ""
    return text.strip().splitlines()[0]


def classify(exc: BaseException) -> AmqpErrorInfo:
    """Turn any exception raised by pika into an :class:`AmqpErrorInfo`."""
    if isinstance(exc, ProbableAuthenticationError):
        return AmqpErrorInfo(
            kind="auth-failed",
            detail="Authentication failed.",
            hint="Check --username/--password, the EXTERNAL client certificate, "
            "or the OAuth token.",
        )

    if isinstance(exc, ProbableAccessDeniedError):
        return AmqpErrorInfo(
            kind="auth-failed",
            detail="The broker refused the credentials for this vhost.",
            hint="The user exists but has no access to this vhost.",
        )

    if isinstance(exc, ChannelClosedByBroker):
        code = getattr(exc, "reply_code", None)
        text = getattr(exc, "reply_text", None)
        kind, detail, hint = _classify_reply(code, text or "")
        return AmqpErrorInfo(kind=kind, detail=detail, hint=hint, code=code)

    if isinstance(exc, ConnectionClosedByBroker):
        code = getattr(exc, "reply_code", None)
        text = getattr(exc, "reply_text", None)
        kind, detail, hint = _classify_reply(code, text or "")
        return AmqpErrorInfo(
            kind=kind,
            detail=detail or "The broker closed the connection.",
            hint=hint,
            code=code,
            fatal=True,
        )

    if isinstance(exc, AMQPChannelError):
        return AmqpErrorInfo(
            kind="channel-error",
            detail=_first_line(str(exc)) or "The channel was closed.",
        )

    if isinstance(exc, pika.exceptions.StreamLostError):
        return AmqpErrorInfo(
            kind="connection-lost",
            detail="The connection to the broker was lost.",
            hint="A heartbeat may have timed out, or the broker restarted. "
            "Long-running commands (consume, watch) are not resumed automatically.",
            fatal=True,
        )

    if isinstance(exc, pika.exceptions.ConnectionWrongStateError):
        return AmqpErrorInfo(
            kind="connection-state",
            detail=_first_line(str(exc)) or "The connection is no longer open.",
            fatal=True,
        )

    if isinstance(exc, pika.exceptions.AMQPHeartbeatTimeout):
        return AmqpErrorInfo(
            kind="heartbeat-timeout",
            detail="The AMQP heartbeat timed out.",
            hint="Raise --heartbeat or check for a stalled network path.",
            fatal=True,
        )

    if isinstance(exc, AMQPConnectionError):
        return AmqpErrorInfo(
            kind="connection-failed",
            detail=_first_line(str(exc)) or "Could not connect to the broker.",
            hint="Verify host, port and TLS settings with 'mq ping'.",
            fatal=True,
        )

    if isinstance(exc, OSError):
        return AmqpErrorInfo(
            kind="network",
            detail=_first_line(str(exc)) or "A network error occurred.",
            hint="Check DNS, routing, firewalls and the broker's port.",
            fatal=True,
        )

    return AmqpErrorInfo(
        kind="internal",
        detail=_first_line(str(exc)) or exc.__class__.__name__,
    )


def describe(exc: BaseException, context: str | None = None) -> str:
    """Render ``exc`` as a multi-line, actionable error message."""
    info = classify(exc)
    prefix = f"{context}: " if context else ""
    return prefix + info.message()


def reply_code_of(exc: BaseException) -> int | None:
    """Return the broker reply code when the exception carries one."""
    for attr in ("reply_code", "_reply_code"):
        code = getattr(exc, attr, None)
        if isinstance(code, int):
            return code
    return None


def is_not_found(exc: BaseException) -> bool:
    return reply_code_of(exc) == 404


def is_access_refused(exc: BaseException) -> bool:
    return reply_code_of(exc) == 403


def is_precondition_failed(exc: BaseException) -> bool:
    return reply_code_of(exc) == 406


def to_json_dict(exc: BaseException) -> dict[str, Any]:
    """Machine-readable form of a failure, for ``--json`` output."""
    info = classify(exc)
    return {
        "error": info.kind,
        "detail": info.detail,
        "hint": info.hint,
        "reply_code": info.code,
        "fatal": info.fatal,
        "exception": exc.__class__.__name__,
    }

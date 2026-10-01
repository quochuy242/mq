"""Discovery over AMQP only: the replacements for the management HTTP calls.

``mq list``/``top``/``watch`` and ``mq exchange list`` used to require the
management plugin because AMQP 0-9-1 cannot enumerate anything. The protocol
still cannot, so the names come from :mod:`mq.inventory` and each one is then
verified with a passive declare -- a legal round trip that returns the live
message and consumer counts.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Sequence

from mq import inventory
from mq.config import MQConfig
from mq.services.exchange_service import ExchangeProbe, probe_exchanges
from mq.services.queue_service import PassiveChannel, QueueProbe, probe_queues


@dataclass
class QueueRow:
    name: str
    vhost: str
    status: str
    ready: int | None = None
    consumers: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue": self.name,
            "vhost": self.vhost,
            "status": self.status,
            "ready": self.ready,
            "consumers": self.consumers,
            "error": self.error,
        }

    def table_row(self) -> list[str]:
        ready = "-" if self.ready is None else f"{self.ready:,}"
        consumers = "-" if self.consumers is None else str(self.consumers)
        return [self.name, ready, consumers, self.status]


@dataclass
class ExchangeRow:
    name: str
    vhost: str
    status: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.name or "(default)",
            "vhost": self.vhost,
            "status": self.status,
            "error": self.error,
        }

    def table_row(self) -> list[str]:
        return [self.name or "(default)", self.status]


@dataclass
class QueueSample:
    """One poll of a queue, used by ``mq watch``."""

    queue: str
    timestamp: float
    ready: int | None
    consumers: int | None
    delta_ready: int | None = None
    rate: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue": self.queue,
            "timestamp": self.timestamp,
            "ready": self.ready,
            "consumers": self.consumers,
            "delta": self.delta_ready,
            "rate_per_s": None if self.rate is None else round(self.rate, 2),
        }


def _split_names(values: Sequence[str] | None) -> list[str]:
    """Expand comma-separated --queues/--exchanges values into a flat list."""
    names: list[str] = []
    for item in values or []:
        for part in item.split(","):
            part = part.strip()
            if part and part not in names:
                names.append(part)
    return names


def resolve_queue_names(
    config: MQConfig,
    patterns: Sequence[str] | None = None,
    explicit: Sequence[str] | None = None,
) -> list[str]:
    """Names to probe, from --queues first, then the inventory plus --pattern."""
    names = _split_names(explicit)
    for name in inventory.queue_names(config.vhost, patterns or None):
        if name not in names:
            names.append(name)
    return names


def to_rows(config: MQConfig, probes: Iterable[QueueProbe]) -> list[QueueRow]:
    return [
        QueueRow(
            name=probe.name,
            vhost=config.vhost,
            status=probe.status,
            ready=probe.ready,
            consumers=probe.consumers,
            error=(probe.error.kind if probe.error else None),
        )
        for probe in probes
    ]


def list_queues(
    config: MQConfig,
    patterns: Sequence[str] | None = None,
    explicit: Sequence[str] | None = None,
) -> list[QueueRow]:
    names = resolve_queue_names(config, patterns, explicit)
    if not names:
        return []
    return to_rows(config, probe_queues(config, names))


def list_exchanges(
    config: MQConfig,
    patterns: Sequence[str] | None = None,
    explicit: Sequence[str] | None = None,
) -> list[ExchangeRow]:
    explicit_names = _split_names(explicit)
    if explicit_names:
        names = explicit_names
    else:
        names = inventory.exchange_names(config.vhost, patterns or None)
    if not names:
        return []
    return [
        ExchangeRow(
            name=probe.name,
            vhost=config.vhost,
            status=probe.status,
            error=(probe.error.kind if probe.error else None),
        )
        for probe in probe_exchanges(config, names)
    ]


def sample_queue(config: MQConfig, queue: str) -> QueueSample | None:
    probe = probe_queues(config, [queue])
    if not probe or not probe[0].exists:
        return None
    return QueueSample(
        queue=queue,
        timestamp=time.time(),
        ready=probe[0].ready,
        consumers=probe[0].consumers,
    )


def watch_queue(
    config: MQConfig,
    queue: str,
    interval: float = 1.0,
    iterations: int | None = None,
    on_sample: Callable[[QueueSample], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.time,
) -> list[QueueSample]:
    """Poll a queue and derive the publish rate from the ready-count delta.

    RabbitMQ exposes no rate over AMQP, but a ready-count delta divided by the
    elapsed time is a close approximation of it -- and it needs no plugin.
    The connection is opened once and reused for every tick.
    """
    samples: list[QueueSample] = []
    previous: QueueSample | None = None
    count = 0

    with PassiveChannel(config) as session:
        while iterations is None or count < iterations:
            probe = session.probe(queue)
            if probe.exists:
                current = QueueSample(
                    queue=queue,
                    timestamp=now(),
                    ready=probe.ready,
                    consumers=probe.consumers,
                )
                if previous is not None:
                    elapsed = current.timestamp - previous.timestamp
                    current.delta_ready = (current.ready or 0) - (previous.ready or 0)
                    if elapsed > 0:
                        current.rate = current.delta_ready / elapsed
                previous = current
                samples.append(current)
                if on_sample:
                    on_sample(current)
            else:
                previous = None
                if on_sample:
                    on_sample(
                        QueueSample(
                            queue=queue,
                            timestamp=now(),
                            ready=None,
                            consumers=None,
                        )
                    )
            count += 1
            if iterations is not None and count >= iterations:
                break
            sleep(interval)

    return samples


def mgmt_fallback_message(config: MQConfig) -> str:
    """Explains the one case where discovery needs the plugin."""
    return (
        f"No queues are tracked for vhost {config.vhost!r}.\n"
        "  AMQP cannot enumerate queues, so 'mq list' only shows what this tool\n"
        "  already knows about. Add names explicitly:\n"
        "      mq inventory add orders payments\n"
        "  or pass them per run:\n"
        "      mq list --queues orders,payments\n"
        "  They are also recorded automatically after declare/bind/publish.\n"
        "  Alternatively, enable the rabbitmq_management plugin and re-run with\n"
        "  --use-management to have the broker enumerate them over HTTP."
    )

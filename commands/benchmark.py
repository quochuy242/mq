from __future__ import annotations

import time
from statistics import mean, stdev

import pika

from mq.config import MQConfig
from mq.connection import close_quietly, create_channel
from mq.services.message_service import ReturnedMessage, publish
from mq.utils import handle_error, is_json_mode, print_json, print_pairs


def execute_publish(
    config: MQConfig,
    queue: str,
    count: int = 1000,
    size: int = 1024,
    exchange: str = "",
    routing_key: str | None = None,
    persistent: bool = False,
) -> None:
    """Measure publish throughput with confirms on, so the number is real.

    Without publisher confirms the measurement is meaningless: the client is
    only timing its own write to a socket buffer.
    """
    body = b"x" * max(1, size)
    target = routing_key if routing_key is not None else queue
    properties = pika.BasicProperties(delivery_mode=2 if persistent else 1)

    conn, channel, blocked = create_channel(config, confirm=True)
    latencies: list[float] = []
    try:
        # mandatory catches a mistyped queue name. Without it, publishing to
        # the default exchange with no such queue silently drops every message
        # and the benchmark reports a meaningless throughput.
        publish(
            config, conn, channel, exchange=exchange, routing_key=target,
            body=body, properties=properties, mandatory=True,
        )

        start = time.perf_counter()
        for _ in range(count):
            t0 = time.perf_counter()
            blocked.raise_if_blocked()
            publish(
                config, conn, channel, exchange=exchange, routing_key=target,
                body=body, properties=properties, mandatory=True,
            )
            latencies.append((time.perf_counter() - t0) * 1000)
        elapsed = time.perf_counter() - start
    except ReturnedMessage as e:
        handle_error(str(e))
        return
    except Exception as e:
        handle_error("Publish benchmark failed", e)
        return
    finally:
        close_quietly(channel, conn)

    _report(
        "Publish",
        queue=queue,
        count=count,
        size=size,
        elapsed=elapsed,
        latencies=latencies,
        extra=[("Confirms", "on"), ("Delivery", "persistent" if persistent else "non-persistent")],
    )


def execute_consume(
    config: MQConfig,
    queue: str,
    count: int = 1000,
    prefetch: int = 50,
) -> None:
    conn, channel, blocked = create_channel(config)
    latencies: list[float] = []
    received = 0
    start = time.perf_counter()

    def callback(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        nonlocal received
        blocked.raise_if_blocked()
        received += 1
        latencies.append((time.perf_counter() - start) * 1000)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if received >= count:
            ch.stop_consuming()

    try:
        channel.basic_qos(prefetch_count=max(1, prefetch))
        channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=False
        )
        channel.start_consuming()
        elapsed = time.perf_counter() - start
    except Exception as e:
        handle_error("Consume benchmark failed", e)
        return
    finally:
        close_quietly(channel, conn)

    if received < count:
        handle_error(
            f"Only {received} of {count} message(s) arrived before the queue ran dry. "
            f"Publish more first: mq benchmark publish {queue} --count {count}"
        )
        return

    _report(
        "Consume",
        queue=queue,
        count=received,
        size=0,
        elapsed=elapsed,
        latencies=latencies,
        extra=[("Prefetch", max(1, prefetch))],
    )


def _report(
    label: str,
    queue: str,
    count: int,
    size: int,
    elapsed: float,
    latencies: list[float],
    extra: list[tuple[str, object]],
) -> None:
    throughput = count / elapsed if elapsed > 0 else 0.0
    pairs: list[tuple[str, object]] = [
        ("Queue", queue),
        ("Messages", f"{count:,}"),
    ]
    if size:
        pairs.append(("Size", f"{size} bytes"))
    pairs += [
        ("Duration", f"{elapsed:.2f}s"),
        ("Throughput", f"{throughput:,.0f} msg/s"),
    ]
    if latencies:
        pairs += [
            ("Latency avg", f"{mean(latencies):.2f}ms"),
            ("Latency min", f"{min(latencies):.2f}ms"),
            ("Latency max", f"{max(latencies):.2f}ms"),
        ]
        if len(latencies) > 1:
            pairs.append(("Latency stdev", f"{stdev(latencies):.2f}ms"))
    pairs += extra

    if is_json_mode():
        print_json(
            {
                "operation": label.lower(),
                "queue": queue,
                "count": count,
                "size": size,
                "elapsed_s": round(elapsed, 3),
                "throughput_per_s": round(throughput, 1),
                "latency_ms": {
                    "avg": round(mean(latencies), 3) if latencies else None,
                    "min": round(min(latencies), 3) if latencies else None,
                    "max": round(max(latencies), 3) if latencies else None,
                    "stdev": round(stdev(latencies), 3) if len(latencies) > 1 else None,
                },
            }
        )
        return
    print_pairs(pairs, title=f"{label} benchmark")

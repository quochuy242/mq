from __future__ import annotations

import time
from statistics import mean, stdev

import pika

from mq.config import MQConfig
from mq.connection import create_channel, create_connection
from mq.utils import handle_error


def execute_publish(
    config: MQConfig,
    queue: str,
    count: int = 1000,
    size: int = 1024,
) -> None:
    try:
        body = b"x" * size
        conn, channel = create_channel(config)

        # warm up
        channel.basic_publish(exchange="", routing_key=queue, body=body)

        latencies: list[float] = []
        start = time.perf_counter()

        for i in range(count):
            t0 = time.perf_counter()
            channel.basic_publish(
                exchange="", routing_key=queue, body=body
            )
            latencies.append((time.perf_counter() - t0) * 1000)

        elapsed = time.perf_counter() - start
        conn.close()

        throughput = count / elapsed if elapsed > 0 else 0
        avg_lat = mean(latencies) if latencies else 0
        stdev_lat = stdev(latencies) if len(latencies) > 1 else 0

        print("Benchmark Publish Results:")
        print(f"  Queue:      {queue}")
        print(f"  Messages:   {count}")
        print(f"  Size:       {size} bytes")
        print(f"  Duration:   {elapsed:.2f}s")
        print(f"  Throughput: {throughput:.0f} msg/s")
        print(f"  Avg Latency: {avg_lat:.2f}ms")
        print(f"  Stdev Lat:   {stdev_lat:.2f}ms")
        print(f"  Min Latency: {min(latencies):.2f}ms")
        print(f"  Max Latency: {max(latencies):.2f}ms")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)


def execute_consume(
    config: MQConfig,
    queue: str,
    count: int = 1000,
) -> None:
    try:
        conn, channel = create_channel(config)
        latencies: list[float] = []
        received = 0
        start = time.perf_counter()

        def callback(
            ch: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            nonlocal received
            received += 1
            latencies.append((time.perf_counter() - start) * 1000)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if received >= count:
                ch.stop_consuming()

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=False,
        )

        channel.start_consuming()
        elapsed = time.perf_counter() - start
        conn.close()

        throughput = count / elapsed if elapsed > 0 else 0
        avg_lat = mean(latencies) if latencies else 0

        print("Benchmark Consume Results:")
        print(f"  Queue:      {queue}")
        print(f"  Messages:   {count}")
        print(f"  Duration:   {elapsed:.2f}s")
        print(f"  Throughput: {throughput:.0f} msg/s")
        print(f"  Avg Latency: {avg_lat:.2f}ms")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)

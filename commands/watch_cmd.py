from __future__ import annotations

import time

from mq.config import MQConfig
from mq.services import discovery
from mq.utils import handle_error, is_json_mode
from mq.utils.output import dim, header, print_json_line


def _render(sample: discovery.QueueSample, first: bool) -> None:
    stamp = time.strftime("%H:%M:%S", time.localtime(sample.timestamp))
    if not first:
        print("\033[2J\033[H", end="")

    print(f"{header('mq watch ' + sample.queue)}  {dim(stamp)}")

    if sample.ready is None:
        print(f"  {'Ready:':<18} {dim('unreachable (deleted, or no permission)')}")
        return

    pairs: list[tuple[str, str]] = [
        ("Ready:", f"{sample.ready:,}"),
        ("Consumers:", "-" if sample.consumers is None else str(sample.consumers)),
    ]
    if sample.delta_ready is not None:
        pairs.append(
            (
                "Delta:",
                f"{sample.delta_ready:+,}"
                f"  ({sample.rate:+.1f}/s)" if sample.rate is not None else f"{sample.delta_ready:+,}",
            )
        )
    else:
        pairs.append(("Delta:", dim("waiting for the second sample...")))

    for label, value in pairs:
        print(f"  {label:<18} {value}")
    print(f"\n  {dim('Ctrl+C to stop')}")


def execute(
    config: MQConfig,
    queue: str,
    interval: float = 1.0,
    count: int | None = None,
    as_json: bool = False,
) -> None:
    """Poll a queue over AMQP and show the depth trend.

    The previous implementation read rates from the management API. Over plain
    AMQP there is no rate field, so the rate shown here is derived from the
    change in the ready count between polls.
    """
    json_mode = as_json or is_json_mode()
    samples: list[discovery.QueueSample] = []

    def on_sample(sample: discovery.QueueSample) -> None:
        if json_mode:
            # JSON Lines: watch runs until interrupted, so one line per sample.
            print_json_line(sample.to_dict())
        else:
            _render(sample, not samples)
        samples.append(sample)

    try:
        discovery.watch_queue(
            config,
            queue,
            interval=interval,
            iterations=count,
            on_sample=on_sample,
        )
    except KeyboardInterrupt:
        if not json_mode:
            print("\nStopped.")
        return
    except Exception as e:
        handle_error("Watch failed", e)
        return

    if json_mode and samples and all(s.ready is None for s in samples):
        handle_error(
            f"Queue '{queue}' could not be read: it may not exist, or this user "
            f"has no permission to read it."
        )

from __future__ import annotations

from mq.config import MQConfig
from mq.services.queue_service import parse_arguments, passive_declare, verify_queue
from mq.utils import handle_error, is_json_mode, print_json, print_pairs
from mq.utils.output import dim, fail, ok, warn


def execute(
    config: MQConfig,
    queue: str,
    expect_durable: bool | None = None,
    expect_quorum: bool | None = None,
    expect_classic: bool | None = None,
    expect_ttl: int | None = None,
    expect_dlx: str | None = None,
    args: list[str] | None = None,
) -> None:
    """Show what a queue is, and optionally assert what it should be.

    AMQP cannot read a queue's arguments back, but it will reject a declare
    whose parameters differ (406 PRECONDITION_FAILED). Each expectation is
    therefore turned into a declaration probe -- the protocol's own consistency
    check -- which is the only way to do this without the management plugin.
    """
    probe = passive_declare(config, queue)
    if not probe.exists:
        if is_json_mode():
            print_json(
                {
                    "queue": queue,
                    "status": probe.status,
                    "error": probe.error.kind if probe.error else None,
                    "detail": probe.error.detail if probe.error else None,
                }
            )
        else:
            reason = probe.error.detail if probe.error else "queue not found"
            handle_error(f"Cannot read queue '{queue}': {reason}")
        raise SystemExit(1 if not is_json_mode() else 0)

    expectations: dict = {}
    if expect_durable is not None:
        expectations["durable"] = expect_durable
    if expect_quorum:
        expectations["x-queue-type"] = "quorum"
    if expect_classic:
        expectations["x-queue-type"] = "classic"
    if expect_ttl is not None:
        expectations["x-message-ttl"] = expect_ttl
    if expect_dlx is not None:
        expectations["x-dead-letter-exchange"] = expect_dlx
    try:
        expectations.update(parse_arguments(args))
    except ValueError as e:
        handle_error(str(e))
        return

    checks: list = []
    if expectations:
        try:
            checks = verify_queue(
                config,
                queue,
                durable=expectations.pop("durable", None),
                arguments=expectations or None,
            )
        except Exception as e:
            handle_error(f"Cannot verify queue '{queue}'", e)
            return

    mismatches = [c for c in checks if c.matches is False]

    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "ready": probe.ready,
                "consumers": probe.consumers,
                "status": probe.status,
                "checks": [c.to_dict() for c in checks],
                "matches": not mismatches,
            }
        )
        raise SystemExit(1 if mismatches else 0)

    ready = probe.ready or 0
    consumers = probe.consumers or 0
    print_pairs(
        [
            ("Queue", queue),
            ("Ready", f"{ready:,}"),
            ("Consumers", consumers),
            ("Status", "empty" if ready == 0 else "has messages"),
            ("Consuming", "yes" if consumers > 0 else "no"),
        ],
        title="Queue",
    )

    if ready > 0 and consumers == 0:
        print()
        print(warn("  No consumer attached while messages are waiting."))

    if checks:
        print()
        print("Declaration checks:")
        for check in checks:
            if check.matches is True:
                marker = ok("ok ")
            elif check.matches is False:
                marker = fail("X  ")
            else:
                marker = warn("?  ")
            line = f"    {marker} {check.field_name} == {check.expected}"
            if check.detail:
                line += dim(f"  ({check.detail})")
            print(line)
        if mismatches:
            print()
            print(fail(f"  {len(mismatches)} expectation(s) do not match the broker."))


def execute_drift(
    config: MQConfig,
    queue: str,
    expected: list[str] | None = None,
    args: list[str] | None = None,
    expect_durable: bool | None = None,
) -> None:
    """Compare a live queue against a declared expectation, exit non-zero on drift.

    Designed for CI: state the arguments your infrastructure is supposed to
    guarantee and let the exit code gate the pipeline.
    """
    try:
        parsed = parse_arguments(args or expected)
    except ValueError as e:
        handle_error(str(e))
        return

    if not parsed and expect_durable is None:
        handle_error(
            "Nothing to check. Pass at least one --arg key=value or --expect-durable."
        )
        return

    try:
        checks = verify_queue(
            config, queue, durable=expect_durable, arguments=parsed or None
        )
    except Exception as e:
        handle_error(f"Cannot verify queue '{queue}'", e)
        return

    mismatches = [c for c in checks if c.matches is False]
    unknown = [c for c in checks if c.matches is None]
    if is_json_mode():
        print_json(
            {
                "queue": queue,
                "drift": bool(mismatches),
                "indeterminate": bool(unknown),
                "checks": [c.to_dict() for c in checks],
            }
        )
    else:
        for check in checks:
            if check.matches is True:
                marker = ok("ok ")
            elif check.matches is False:
                marker = fail("X  ")
            else:
                marker = warn("?  ")
            line = f"  {marker} {check.field_name} == {check.expected}"
            if check.detail:
                line += dim(f"  ({check.detail})")
            print(line)
    raise SystemExit(1 if mismatches else 0)
